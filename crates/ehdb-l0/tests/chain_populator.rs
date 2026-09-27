//! **The populator and its watermark** — the thing that makes repointing at the
//! chain store safe.
//!
//! The property under test is not "the populator writes events". It is that an
//! **unpopulated store says so**, instead of answering `empty` and letting a
//! reader conclude a running execution has not started.

use std::sync::Arc;

use ehdb_l0::chain::{ChainError, ChainEvent};
use ehdb_l0::chain_populator::{populator_enabled, Authority, ChainPopulator, POPULATOR_ENV};
use ehdb_l0::chain_store_durable::DurableChainStore;
use ehdb_l0::substrate::{DurableSubstrate, LocalFsSubstrate};

struct Rig {
    _dir: tempfile::TempDir,
    store: DurableChainStore,
    substrate: Arc<dyn DurableSubstrate>,
}

fn rig() -> Rig {
    let dir = tempfile::tempdir().expect("tempdir");
    let fs = LocalFsSubstrate::new(dir.path()).expect("substrate");
    let substrate: Arc<dyn DurableSubstrate> = Arc::new(fs);
    Rig {
        _dir: dir,
        store: DurableChainStore::new(Arc::clone(&substrate)),
        substrate,
    }
}

impl Rig {
    fn pop(&self) -> ChainPopulator<'_> {
        ChainPopulator::new(&self.store, self.substrate.as_ref())
    }
}

// ---------------------------------------------------------------------------
// ⭐⭐ The data-integrity cliff this exists to prevent.
// ---------------------------------------------------------------------------

/// **RED, expressed as the hazard.** A store that was never populated must NOT
/// answer "no events" — it must say it cannot answer.
///
/// Without the watermark, `DurableChainStore::chain` returns an empty Vec here,
/// which downstream reads as "this execution has not started" for an execution
/// that may have been running for hours.
#[test]
fn an_unpopulated_execution_is_not_populated_not_empty() {
    let r = rig();
    let p = r.pop();

    // The raw store genuinely answers "empty" — that is the hazard.
    assert!(
        r.store.chain("exec-never-seen").expect("read").is_empty(),
        "precondition: the raw store returns an empty chain, which is the \
         ambiguity the watermark resolves"
    );

    // The guarded read refuses to answer.
    assert_eq!(
        p.authority("exec-never-seen").expect("authority"),
        Authority::NotPopulated
    );
    assert!(
        !p.authority("exec-never-seen").unwrap().is_trustworthy(),
        "an unpopulated execution must never be trusted"
    );
    assert_eq!(
        p.chain_if_authoritative("exec-never-seen")
            .expect("guarded read"),
        None,
        "the guarded read must return None (fall through), NEVER Some(vec![]) — \
         Some(empty) is what makes a running execution read as not-started"
    );
}

/// **GREEN.** After population the store is authoritative and answers.
#[test]
fn a_populated_execution_is_authoritative_and_answers() {
    let r = rig();
    let p = r.pop();
    p.populate("e1", "a", None, None, "{}").expect("populate");
    p.populate("e1", "b", Some("a"), None, "{}")
        .expect("populate");

    match p.authority("e1").expect("authority") {
        Authority::Authoritative {
            first_seq,
            through_seq,
        } => {
            assert_eq!(first_seq, 1);
            assert_eq!(through_seq, 2);
        }
        other => panic!("expected Authoritative, got {other:?}"),
    }

    let chain = p
        .chain_if_authoritative("e1")
        .expect("guarded read")
        .expect("authoritative");
    assert_eq!(chain.len(), 2);
    assert_eq!(chain[1].event_id, "b");
}

/// ⭐ **The third state, and the one a naive design gets wrong.** An execution
/// that WAS populated and genuinely holds no events must be answerable as
/// empty — otherwise the watermark would make every empty answer unreachable
/// and the store could never report a true negative.
#[test]
fn a_populated_but_eventless_execution_answers_empty_and_is_trusted() {
    let r = rig();
    let p = r.pop();
    // Populate then remove the event, leaving the marker — the shape a
    // retention sweep or a cancelled execution produces.
    p.populate("e1", "a", None, None, "{}").expect("populate");
    r.substrate.delete("chain/e1/ev/a").expect("delete event");
    r.substrate
        .delete("chain/e1/seq/00000000000000000001")
        .expect("delete pointer");

    assert!(p.authority("e1").unwrap().is_trustworthy());
    assert_eq!(
        p.chain_if_authoritative("e1")
            .expect("guarded")
            .map(|c| c.len()),
        Some(0),
        "a populated-but-eventless execution must answer Some(empty), which is a \
         TRUE negative — distinct from the None above"
    );
}

/// ⚠ The three states must be mutually distinguishable. If two collapse, the
/// watermark has bought nothing.
#[test]
fn the_three_states_are_distinguishable() {
    let r = rig();
    let p = r.pop();
    p.populate("populated", "a", None, None, "{}").unwrap();

    let never = p.chain_if_authoritative("never").unwrap();
    let full = p.chain_if_authoritative("populated").unwrap();

    assert_eq!(never, None, "never populated -> None");
    assert_eq!(full.map(|c| c.len()), Some(1), "populated -> Some(n)");
    assert_ne!(
        p.authority("never").unwrap().label(),
        p.authority("populated").unwrap().label()
    );
}

// ---------------------------------------------------------------------------
// Crash-ordering: the marker is claimed BEFORE the append.
// ---------------------------------------------------------------------------

/// ⭐ A crash between marker and append must leave a marker over a SHORT chain,
/// not events with no marker.
///
/// Events-without-marker is silently invisible: every reader falls through and
/// the population appears never to have happened. Marker-over-short-chain is
/// reported honestly by the chain's own gap detection. The failure mode is
/// chosen rather than inherited, and this test pins the choice.
#[test]
fn the_watermark_is_claimed_before_the_append() {
    let r = rig();
    let p = r.pop();
    p.populate("e1", "a", None, None, "{}").unwrap();

    // Simulate a crash after the marker was claimed for seq 2 but before the
    // event landed: write the marker forward, leave the chain short.
    r.substrate
        .put_overwrite("chain/e1/wm", b"1:2")
        .expect("advance marker");

    // The store still answers — and the answer is honest about what it holds.
    let chain = p
        .chain_if_authoritative("e1")
        .unwrap()
        .expect("authoritative");
    assert_eq!(chain.len(), 1, "the chain is short, and says so");
    match p.authority("e1").unwrap() {
        Authority::Authoritative { through_seq, .. } => assert_eq!(through_seq, 2),
        other => panic!("expected Authoritative, got {other:?}"),
    }
    // ⚠ The reverse ordering would have produced NO marker here, and
    // chain_if_authoritative would have returned None — the population
    // invisible.
}

// ---------------------------------------------------------------------------
// Population mechanics.
// ---------------------------------------------------------------------------

#[test]
fn populate_enforces_the_chain_head_like_a_direct_append() {
    let r = rig();
    let p = r.pop();
    p.populate("e1", "a", None, None, "{}").unwrap();
    p.populate("e1", "b", Some("a"), None, "{}").unwrap();
    assert!(matches!(
        p.populate("e1", "c", Some("a"), None, "{}"),
        Err(ChainError::NotHead { .. })
    ));
}

#[test]
fn the_watermark_widens_across_replicated_out_of_order_delivery() {
    let r = rig();
    let p = r.pop();
    let ev = |seq: u64, id: &str, prev: Option<&str>| ChainEvent {
        exec_seq: seq,
        event_id: id.to_string(),
        prev_event_id: prev.map(str::to_string),
        execution_id: "e1".to_string(),
        parent_execution_id: None,
        payload: "{}".to_string(),
    };
    p.populate_replicated(ev(3, "c", Some("b"))).unwrap();
    p.populate_replicated(ev(1, "a", None)).unwrap();

    match p.authority("e1").unwrap() {
        Authority::Authoritative {
            first_seq,
            through_seq,
        } => {
            assert_eq!(first_seq, 1, "widened DOWN to the earliest seen");
            assert_eq!(through_seq, 3, "and UP to the latest");
        }
        other => panic!("{other:?}"),
    }
}

#[test]
fn population_survives_reopening_the_store() {
    let dir = tempfile::tempdir().unwrap();
    {
        let fs = LocalFsSubstrate::new(dir.path()).unwrap();
        let sub: Arc<dyn DurableSubstrate> = Arc::new(fs);
        let store = DurableChainStore::new(Arc::clone(&sub));
        let p = ChainPopulator::new(&store, sub.as_ref());
        p.populate("e1", "a", None, None, "{}").unwrap();
    }
    let fs = LocalFsSubstrate::new(dir.path()).unwrap();
    let sub: Arc<dyn DurableSubstrate> = Arc::new(fs);
    let store = DurableChainStore::new(Arc::clone(&sub));
    let p = ChainPopulator::new(&store, sub.as_ref());
    assert!(
        p.authority("e1").unwrap().is_trustworthy(),
        "marker is durable"
    );
    assert_eq!(
        p.chain_if_authoritative("e1").unwrap().map(|c| c.len()),
        Some(1)
    );
}

// ---------------------------------------------------------------------------
// The arming flag.
// ---------------------------------------------------------------------------

#[test]
fn the_populator_flag_is_off_by_default_and_fails_safe() {
    let prev = std::env::var(POPULATOR_ENV).ok();
    unsafe { std::env::remove_var(POPULATOR_ENV) };
    assert!(!populator_enabled(), "default MUST be off");
    for junk in ["", " ", "off", "no", "0", "populate", "enabled"] {
        unsafe { std::env::set_var(POPULATOR_ENV, junk) };
        assert!(!populator_enabled(), "{junk:?} must not arm a WRITER");
    }
    unsafe { std::env::set_var(POPULATOR_ENV, "true") };
    assert!(populator_enabled());
    match prev {
        Some(v) => unsafe { std::env::set_var(POPULATOR_ENV, v) },
        None => unsafe { std::env::remove_var(POPULATOR_ENV) },
    }
}

#[test]
fn authority_labels_are_enumerated_for_metric_pinning() {
    assert_eq!(Authority::ALL_LABELS.len(), 2);
    assert!(Authority::ALL_LABELS.contains(&Authority::NotPopulated.label()));
    assert!(Authority::ALL_LABELS.contains(
        &Authority::Authoritative {
            first_seq: 1,
            through_seq: 1
        }
        .label()
    ));
}

/// ⚠ **The other direction.** The test above applies seq 3 then seq 1, so
/// `through_seq` is already at its maximum and the widening-UP branch is never
/// exercised — a mutant removing `.max()` survived on exactly that gap.
///
/// Both directions matter because replication delivers in any order: a low
/// sequence arriving first must let a later high one extend the watermark, or
/// the store under-reports what it holds and a reader stops short.
#[test]
fn the_watermark_widens_up_when_a_later_sequence_arrives_second() {
    let r = rig();
    let p = r.pop();
    let ev = |seq: u64, id: &str, prev: Option<&str>| ChainEvent {
        exec_seq: seq,
        event_id: id.to_string(),
        prev_event_id: prev.map(str::to_string),
        execution_id: "e1".to_string(),
        parent_execution_id: None,
        payload: "{}".to_string(),
    };

    p.populate_replicated(ev(1, "a", None)).unwrap();
    match p.authority("e1").unwrap() {
        Authority::Authoritative { through_seq, .. } => assert_eq!(through_seq, 1),
        other => panic!("{other:?}"),
    }

    p.populate_replicated(ev(3, "c", Some("b"))).unwrap();
    match p.authority("e1").unwrap() {
        Authority::Authoritative {
            first_seq,
            through_seq,
        } => {
            assert_eq!(first_seq, 1, "first stays at the earliest");
            assert_eq!(through_seq, 3, "through must widen UP to the latest seen");
        }
        other => panic!("{other:?}"),
    }
}
