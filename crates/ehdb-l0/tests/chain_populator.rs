//! **The populator and its watermark** — the thing that makes repointing at the
//! chain store safe.
//!
//! The property under test is not "the populator writes events". It is that an
//! **unpopulated store says so**, instead of answering `empty` and letting a
//! reader conclude a running execution has not started.

use std::sync::Arc;

use ehdb_l0::chain::{ChainError, ChainEvent};
use ehdb_l0::chain_populator::{
    populator_enabled, Authority, ChainPopulator, FromLog, LogEvent, POPULATOR_ENV,
};
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

/// Build a log slice from ids. ⚠ Deliberately carries NO `prev_event_id`: the
/// edge is recomputed from position, because the column is NULL on 99.65% of
/// prod-shaped rows.
fn log(ids: &[&str]) -> Vec<LogEvent> {
    ids.iter()
        .map(|id| LogEvent {
            event_id: (*id).to_string(),
            parent_execution_id: None,
            payload: "{}".to_string(),
        })
        .collect()
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
    // ⚠ Built through the LOG path, because that is the only source the guarded
    // read serves. The per-event `populate` path still writes, but it cannot know
    // whether it saw the execution's first event, so it records no coverage and is
    // never served — see `a_per_event_populated_partition_is_never_served`.
    p.populate_from_log("e1", &log(&["a", "b"]))
        .expect("populate from log");

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
    // An execution the authoritative log genuinely has no events for. The caller
    // read the log successfully and it was empty — which is NOT the same as a read
    // that failed, and the populator's doc makes that the caller's obligation.
    p.populate_from_log("e1", &[]).expect("populate empty");

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
    p.populate_from_log("populated", &log(&["a"])).unwrap();

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

/// ⭐ On an ALREADY-AUTHORITATIVE execution, a crash between marker and append
/// must leave a marker over a SHORT chain, not events with no marker.
///
/// ⚠ The ordering is asymmetric and this test covers only half of it. On a
/// NOT-yet-populated execution the append comes FIRST — see
/// `a_failed_first_populate_leaves_the_store_not_populated` for why marking
/// first there would create authority out of a failure.
///
/// Events-without-marker is silently invisible: every reader falls through and
/// the population appears never to have happened. Marker-over-short-chain is
/// reported honestly by the chain's own gap detection. The failure mode is
/// chosen rather than inherited, and this test pins the choice.
#[test]
fn the_watermark_is_claimed_before_the_append_once_authoritative() {
    let r = rig();
    let p = r.pop();
    p.populate("e1", "a", None, None, "{}").unwrap();

    // Simulate a crash after the marker was claimed for seq 2 but before the
    // event landed: write the marker forward, leave the chain short.
    r.substrate
        .put_overwrite("chain/e1/wm", b"1:2")
        .expect("advance marker");

    // The MARKER survives — that is what the pre-append ordering buys, and the
    // reverse ordering would have left no marker at all here.
    match p.authority("e1").unwrap() {
        Authority::Authoritative { through_seq, .. } => assert_eq!(through_seq, 2),
        other => panic!("expected Authoritative, got {other:?}"),
    }

    // ⚠⚠ But the GUARDED READ refuses while the marker is ahead of the content.
    //
    // This assertion was the other way round until 2026-09-28: it expected
    // `Some(1)` and called that "honest about what it holds". The kind proof
    // showed it is not honest in the way that matters — `chain_if_authoritative`
    // returns a bare `Vec`, so a caller has nowhere to see the shortfall, and a
    // short chain is indistinguishable from a complete one (contiguous seqs, and
    // complete by the chain's own gap check). Measured: Postgres 7 events, store
    // 6, watermark `1:7`, guarded read `Some(6)`.
    //
    // The gap is the only evidence the store has that the log moved on without
    // it, so it is read rather than ignored, and the caller is sent to the
    // authoritative log.
    assert_eq!(
        p.chain_if_authoritative("e1").unwrap().map(|c| c.len()),
        None,
        "a watermark ahead of the stored content must read as CANNOT ANSWER, not \
         as a short chain a caller cannot tell is short"
    );
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
        p.populate_from_log("e1", &log(&["a"])).unwrap();
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

// ---------------------------------------------------------------------------
// ⚠⚠ Never create authority out of a failure.
// ---------------------------------------------------------------------------

/// **The defect this fixes, in its realistic form.**
///
/// Arming the populator MID-FLIGHT is the normal way it gets switched on: the
/// first row seen for an already-running execution carries a `prev` the empty
/// store does not have, so `append` fails with `NotHead`. The first draft had
/// already claimed the watermark by then, leaving `Authoritative` over ZERO
/// events — so `chain_if_authoritative` returned `Some(vec![])` and a reader
/// concluded a running execution had no events.
///
/// That is the exact cliff the watermark exists to prevent, arriving through
/// the failure path instead of the empty-store path.
#[test]
fn a_failed_first_populate_leaves_the_store_not_populated() {
    let r = rig();
    let p = r.pop();

    let res = p.populate("e-inflight", "ev-500", Some("ev-499"), None, "{}");
    assert!(
        res.is_err(),
        "precondition: appending with a prev the empty store lacks must fail"
    );

    assert_eq!(
        p.authority("e-inflight").expect("authority"),
        Authority::NotPopulated,
        "a FAILED first populate must leave the store NOT populated — authority \
         must never be created out of a failure"
    );
    assert_eq!(
        p.chain_if_authoritative("e-inflight").expect("guarded"),
        None,
        "and the guarded read must still fall through, not answer Some(empty)"
    );
}

/// ⚠ Control: a SUCCEEDING first populate does create authority. Without this,
/// "a failure creates no authority" is satisfied by a populator that never
/// creates any.
#[test]
fn a_succeeding_first_populate_does_create_authority() {
    let r = rig();
    let p = r.pop();
    p.populate("e-fresh", "a", None, None, "{}")
        .expect("must succeed");
    assert!(
        p.authority("e-fresh").unwrap().is_trustworthy(),
        "a successful first populate MUST create authority"
    );
    // ⚠ Authority, but NOT served. The per-event path records no coverage, so the
    // guarded read refuses it — see `a_per_event_populated_partition_is_never_served`
    // for the argument. This test's own subject is the WATERMARK, which is created.
    assert_eq!(
        p.chain_if_authoritative("e-fresh").unwrap(),
        None,
        "a per-event-populated partition has no coverage record and is not served"
    );
}

/// A failure on an ALREADY-authoritative execution must not revoke authority —
/// there is real content behind it.
#[test]
fn a_failure_on_an_authoritative_execution_does_not_revoke_it() {
    let r = rig();
    let p = r.pop();
    p.populate("e1", "a", None, None, "{}").unwrap();
    assert!(p
        .populate("e1", "bad", Some("not-the-head"), None, "{}")
        .is_err());

    assert!(
        p.authority("e1").unwrap().is_trustworthy(),
        "existing authority survives a failed append"
    );

    // ⚠ The failed append widened the marker to 2 before it failed, so the
    // marker and the content now disagree and the guarded read refuses. Authority
    // is NOT revoked — the distinction matters: the marker records that this
    // execution was being populated, which is what a later repair pass needs,
    // while the read refuses because the prefix it holds may be stale.
    assert_eq!(
        p.chain_if_authoritative("e1").unwrap().map(|c| c.len()),
        None,
        "a failed append leaves marker and content disagreeing, so the guarded \
         read must refuse rather than serve a possibly-stale prefix"
    );

    // ⭐ And the content is still THERE — refusing to serve it is not losing it.
    assert_eq!(
        p.authority("e1").unwrap().label(),
        "authoritative",
        "the marker is intact"
    );
}

/// The same rule on the replicated path.
#[test]
fn a_failed_first_replicated_apply_creates_no_authority() {
    let r = rig();
    let p = r.pop();
    // An event whose id is empty is refused by the store.
    let bad = ChainEvent {
        exec_seq: 1,
        event_id: String::new(),
        prev_event_id: None,
        execution_id: "e-repl".to_string(),
        parent_execution_id: None,
        payload: "{}".to_string(),
    };
    assert!(
        p.populate_replicated(bad).is_err(),
        "precondition: apply fails"
    );
    assert_eq!(
        p.authority("e-repl").unwrap(),
        Authority::NotPopulated,
        "a failed first replicated apply must create no authority either"
    );
}

/// ⚠ **The doc comment must not describe the pre-fix ordering.**
///
/// Found while wiring the call site: after the asymmetry landed, `populate`'s
/// own doc still read *"the watermark is claimed before the append"* — the
/// claim whose unconditional form is the defect. A reader trusting it would
/// "restore" uniform ordering and reintroduce authoritative-and-empty.
///
/// A comment is not a guard (noetl/ai-meta#332) — but a comment that
/// contradicts its code is worse than none, so this pins the one sentence that
/// matters.
#[test]
fn the_populate_doc_does_not_claim_uniform_watermark_first_ordering() {
    let src = include_str!("../src/chain_populator.rs");
    let at = src
        .find("pub fn populate(")
        .expect("populate not found — the extraction broke, not the property");
    // The doc block immediately above the signature.
    let head = &src[..at];
    let doc_start = head
        .rfind("/// **Populate one event.**")
        .expect("populate's doc header not found — re-anchor this guard");
    let doc = &head[doc_start..];
    assert!(
        doc.len() > 200,
        "extracted {} bytes of doc — implausibly small; a guard measuring \
         nothing passes",
        doc.len()
    );
    assert!(
        doc.contains("asymmetric"),
        "populate's doc no longer calls the ordering asymmetric. If the \
         asymmetry was genuinely removed, `a_failed_first_populate_leaves_the_\
         store_not_populated` is the test that should be failing — check that \
         first.\n{doc}"
    );
    assert!(
        !doc.contains("claimed **before** the append"),
        "populate's doc claims the watermark is claimed before the append \
         unconditionally. That is true only when the execution is ALREADY \
         authoritative; doing it on a never-seen execution leaves \
         `Authoritative {{1,1}}` over zero events when the append fails, which \
         is the authoritative-and-empty cliff.\n{doc}"
    );
}

/// ⭐⭐ **The restart scenario, as measured in kind on 2026-09-28 — now fixed by
/// construction.**
///
/// The emit path stamps the chain edge from an in-memory head map that does not
/// survive a restart, so the next event for a still-running execution reaches the
/// log with `prev_event_id = NULL`. Under the per-event populator that row was
/// rejected as not-the-head and the partition then froze while the execution kept
/// emitting: Postgres 7 events, store 6, watermark `1:7`.
///
/// `populate_from_log` cannot exhibit that, because it never reads the column. The
/// log below is exactly the prod shape — a second null-prev "root" in the middle —
/// and the resulting chain is fully linked with no second root.
#[test]
fn a_post_restart_null_prev_row_does_not_create_a_pseudo_root() {
    let r = rig();
    let p = r.pop();

    // The log as Postgres holds it after a mid-flight restart. `log()` carries no
    // prev column at all, which IS the fix: position is the only input.
    let out = p
        .populate_from_log("e-restart", &log(&["e1", "e2", "e3", "e4", "e5"]))
        .expect("populate from log");
    assert_eq!(
        out,
        FromLog::InSync {
            total: 5,
            appended: 5
        }
    );

    let chain = p
        .chain_if_authoritative("e-restart")
        .expect("guarded read")
        .expect("a log-sourced partition is served");
    assert_eq!(
        chain.len(),
        5,
        "every event, including the post-restart ones"
    );

    // ⭐ Exactly ONE root, and every other event links to its predecessor.
    let roots = chain.iter().filter(|e| e.prev_event_id.is_none()).count();
    assert_eq!(
        roots, 1,
        "a log carrying a second null-prev row must still yield ONE chain root; \
         found {roots}. This is the 534-of-595 shape from the kind database."
    );
    for w in chain.windows(2) {
        assert_eq!(
            w[1].prev_event_id.as_deref(),
            Some(w[0].event_id.as_str()),
            "each event must link to its predecessor in log order"
        );
    }
}

/// ⭐⭐ **Mid-flight arming no longer truncates.**
///
/// Measured before the fix: an execution armed after it started held 1 event
/// against the log's 3, reported `authoritative first=1 through=1`, and the
/// guarded read returned a contiguous, gap-check-passing chain beginning at the
/// execution's THIRD event. `first=1` was true of the store and false of the
/// execution.
///
/// Arming mid-flight now populates from the log's first event, so the partition is
/// complete the moment it exists.
#[test]
fn arming_mid_flight_populates_from_the_logs_first_event() {
    let r = rig();
    let p = r.pop();

    // The execution has been running for a while; the populator is armed now.
    let full = log(&["a", "b", "c"]);
    p.populate_from_log("e-mid", &full).expect("populate");

    let chain = p.chain_if_authoritative("e-mid").unwrap().expect("served");
    assert_eq!(
        chain.len(),
        3,
        "the WHOLE log, not the tail from arming time"
    );
    assert_eq!(chain[0].event_id, "a", "rooted at the log's first event");
    assert!(
        chain[0].prev_event_id.is_none(),
        "and that root is the only event with no predecessor"
    );
}

/// ⚠⚠ A partition built by the **per-event** path is never served.
///
/// It cannot know whether it saw the execution's first event, so it records no
/// coverage. The watermark alone cannot cover for that: its `first_seq` is the
/// STORE's sequence, `1` for any fresh partition regardless of where the execution
/// began — which is precisely how the truncated read passed for authoritative.
#[test]
fn a_per_event_populated_partition_is_never_served() {
    let r = rig();
    let p = r.pop();
    p.populate("e-emit", "x", None, None, "{}").unwrap();
    p.populate("e-emit", "y", Some("x"), None, "{}").unwrap();

    assert!(
        p.authority("e-emit").unwrap().is_trustworthy(),
        "the watermark exists"
    );
    assert_eq!(
        p.coverage("e-emit").unwrap(),
        None,
        "but no coverage record — the per-event path cannot claim one"
    );
    assert_eq!(
        p.chain_if_authoritative("e-emit").unwrap(),
        None,
        "so the guarded read refuses, however complete the partition happens to be"
    );
}

/// ⚠ A crash part-way through extending leaves coverage claiming more than the
/// partition holds, and the read must refuse.
///
/// This is the realistic failure of the log path: coverage is claimed before the
/// appends (so a crash cannot make the store under-report), which means a crash
/// mid-extend leaves `cov_total` ahead of the stored length.
#[test]
fn coverage_claiming_more_than_is_stored_refuses() {
    let r = rig();
    let p = r.pop();
    p.populate_from_log("e-part", &log(&["a", "b"])).unwrap();
    assert!(p.chain_if_authoritative("e-part").unwrap().is_some());

    // Simulate the crash: coverage says 4, the partition holds 2.
    r.substrate
        .put_overwrite("chain/e-part/cov", b"a:4")
        .expect("advance coverage");

    assert_eq!(
        p.chain_if_authoritative("e-part").unwrap(),
        None,
        "coverage ahead of content must read as CANNOT ANSWER"
    );
}

/// ⚠ Coverage naming a different root than the partition holds must refuse.
///
/// Guards the case where a partition was rebuilt from a different starting point —
/// the truncation defect's signature — even if the counts happen to line up.
#[test]
fn coverage_naming_a_different_root_refuses() {
    let r = rig();
    let p = r.pop();
    p.populate_from_log("e-root", &log(&["a", "b"])).unwrap();

    r.substrate
        .put_overwrite("chain/e-root/cov", b"zzz:2")
        .expect("rewrite coverage root");

    assert_eq!(
        p.chain_if_authoritative("e-root").unwrap(),
        None,
        "a coverage root that is not the partition's root must read as CANNOT ANSWER"
    );
}

/// ⚠ The watermark tripwire must still be reachable on a **covered** partition.
///
/// Without this the tripwire would be dead code for every partition that can
/// actually be served: the coverage gate would refuse first in every realistic
/// case. Defence in depth is only defence if something exercises it.
#[test]
fn the_watermark_tripwire_still_fires_on_a_covered_partition() {
    let r = rig();
    let p = r.pop();
    p.populate_from_log("e-wm", &log(&["a", "b"])).unwrap();
    assert!(p.chain_if_authoritative("e-wm").unwrap().is_some());

    // Coverage stays correct; only the watermark runs ahead.
    r.substrate
        .put_overwrite("chain/e-wm/wm", b"1:9")
        .expect("advance watermark");

    assert_eq!(
        p.chain_if_authoritative("e-wm").unwrap(),
        None,
        "a watermark ahead of the stored head must refuse even when coverage agrees"
    );
}

/// ⚠ Re-running with the same log appends nothing; with a longer log, extends.
#[test]
fn population_from_the_log_is_idempotent_and_incremental() {
    let r = rig();
    let p = r.pop();

    assert_eq!(
        p.populate_from_log("e-inc", &log(&["a", "b"])).unwrap(),
        FromLog::InSync {
            total: 2,
            appended: 2
        }
    );
    assert_eq!(
        p.populate_from_log("e-inc", &log(&["a", "b"])).unwrap(),
        FromLog::InSync {
            total: 2,
            appended: 0
        },
        "re-running with the same log must append nothing, not fail"
    );
    assert_eq!(
        p.populate_from_log("e-inc", &log(&["a", "b", "c"]))
            .unwrap(),
        FromLog::InSync {
            total: 3,
            appended: 1
        },
        "a longer log extends the partition by exactly the remainder"
    );

    let chain = p.chain_if_authoritative("e-inc").unwrap().expect("served");
    assert_eq!(chain.len(), 3);
    assert_eq!(chain[2].prev_event_id.as_deref(), Some("b"));
}

/// ⚠ A log that disagrees with the stored prefix is REPORTED, not repaired.
///
/// Silently rewriting an immutable chain to match a new reading is how a store
/// stops being evidence.
#[test]
fn a_log_disagreeing_with_the_stored_prefix_is_reported_not_repaired() {
    let r = rig();
    let p = r.pop();
    p.populate_from_log("e-div", &log(&["a", "b"])).unwrap();

    let out = p
        .populate_from_log("e-div", &log(&["a", "DIFFERENT"]))
        .unwrap();
    match out {
        FromLog::Diverged {
            at_position,
            stored,
            log: l,
        } => {
            assert_eq!(at_position, 1);
            assert_eq!(stored, "b");
            assert_eq!(l, "DIFFERENT");
        }
        other => panic!("expected Diverged, got {other:?}"),
    }

    // ⭐ And nothing was REWRITTEN — the events are exactly as they were.
    //
    // ⚠ Read through the store, not through `chain_if_authoritative`: as of
    // noetl/ai-meta#360 a detected content conflict REVOKES serve-trust by
    // dropping the coverage record, so the guarded read now refuses. This test's
    // subject is "reported, not repaired", and that is still true — the chain is
    // intact, only the permission to serve it is withdrawn.
    let raw = r.store.chain("e-div").expect("raw chain");
    assert_eq!(raw[1].event_id, "b", "the stored chain is untouched");
    assert_eq!(raw.len(), 2, "and nothing was appended or removed");

    assert_eq!(
        p.chain_if_authoritative("e-div").unwrap(),
        None,
        "but a partition proven to disagree with the authoritative log must no \
         longer be served — Postgres is authoritative, so the STORE is the wrong \
         one here"
    );
}

/// Every `FromLog` label is enumerated, so none is an absent series.
#[test]
fn from_log_labels_are_enumerated_for_pinning() {
    for v in [
        FromLog::InSync {
            total: 1,
            appended: 0,
        },
        FromLog::InSync {
            total: 1,
            appended: 1,
        },
        FromLog::Diverged {
            at_position: 0,
            stored: String::new(),
            log: String::new(),
        },
        FromLog::StaleLog {
            stored_len: 2,
            log_len: 1,
        },
    ] {
        assert!(
            FromLog::ALL_LABELS.contains(&v.label()),
            "{} missing from ALL_LABELS",
            v.label()
        );
    }
    assert_eq!(FromLog::ALL_LABELS.len(), 4);

    // ⚠ Only Diverged is unhealthy. StaleLog reading as healthy IS the fix for
    // noetl/ai-meta#360 — if this flips, 6.3% false divergence returns.
    assert!(FromLog::StaleLog {
        stored_len: 2,
        log_len: 1
    }
    .is_healthy());
    assert!(FromLog::InSync {
        total: 1,
        appended: 0
    }
    .is_healthy());
    assert!(!FromLog::Diverged {
        at_position: 0,
        stored: String::new(),
        log: String::new()
    }
    .is_healthy());
}

/// ⚠⚠ A failed append part-way through `populate_from_log`.
///
/// Found by a surviving mutant: moving the watermark write to BEFORE the appends
/// changed no test, which meant nothing exercised the log path's failure case at
/// all. The population below fails on its second event (the store refuses an empty
/// id), leaving coverage claiming 2 over a partition holding 1.
///
/// The property is that the guarded read refuses afterwards **and** that the
/// watermark is not left claiming content that was never written.
#[test]
fn a_failed_append_mid_extend_leaves_nothing_servable() {
    let r = rig();
    let p = r.pop();

    let bad = vec![
        LogEvent {
            event_id: "a".to_string(),
            parent_execution_id: None,
            payload: "{}".to_string(),
        },
        LogEvent {
            event_id: String::new(), // the store refuses this
            parent_execution_id: None,
            payload: "{}".to_string(),
        },
    ];
    assert!(
        p.populate_from_log("e-fail", &bad).is_err(),
        "an append the store refuses must surface, not be swallowed"
    );

    assert_eq!(
        p.chain_if_authoritative("e-fail").unwrap(),
        None,
        "a partition whose population failed part-way must not be served"
    );

    // ⭐ The watermark must not claim events that were never appended. Writing it
    // before the appends is what the surviving mutant did; this is the assertion
    // that makes that ordering load-bearing rather than incidental.
    match p.authority("e-fail").unwrap() {
        Authority::NotPopulated => {}
        Authority::Authoritative { through_seq, .. } => panic!(
            "the watermark claims through_seq={through_seq} after a failed \
             mid-extend; it must not be written until the content is there"
        ),
    }
}

// ---------------------------------------------------------------------------
// noetl/ai-meta#360 — a stale snapshot is not a divergence.
// ---------------------------------------------------------------------------

/// ⭐⭐ **The prod failure, reproduced exactly.**
///
/// `LogSourcedChainSource::chain_for` reads the event log and THEN takes the
/// store lock — it cannot hold a `std::sync::Mutex` across the database `await`.
/// So under concurrent reads of one execution, the task that read the log FIRST
/// can reach the lock SECOND and find the store already ahead of its snapshot.
///
/// Classing that as `Diverged` produced **5 false divergences in 79 comparisons
/// (6.3%)** on the prod ramp of 2026-09-29, failing the comparator gate and
/// forcing a rollback — while nothing was wrong: 60 partitions with 0 persistent
/// mismatches and 24/24 executions completed.
///
/// The interleaving below is that race, made deterministic:
///   task B reads [a,b,c,d] and applies it;
///   task A, holding the older snapshot [a,b,c], reaches the store second.
#[test]
fn a360_a_snapshot_behind_the_store_is_stale_not_diverged() {
    let r = rig();
    let p = r.pop();
    let exec = "race";

    // Task B: the newer snapshot lands first.
    p.populate_from_log(exec, &log(&["a", "b", "c", "d"]))
        .expect("newer snapshot applies");

    // Task A: the OLDER snapshot arrives second. Every event it holds matches;
    // the store simply holds one more.
    let out = p
        .populate_from_log(exec, &log(&["a", "b", "c"]))
        .expect("a stale snapshot must not error");

    match out {
        FromLog::StaleLog {
            stored_len,
            log_len,
        } => {
            assert_eq!(stored_len, 4);
            assert_eq!(log_len, 3);
        }
        FromLog::Diverged { .. } => panic!(
            "a snapshot that is BEHIND the store was classed as Diverged. That is \
             the noetl/ai-meta#360 false alarm — 6.3% of prod comparisons — and it \
             fails the comparator gate while nothing is actually wrong."
        ),
        other => panic!("expected StaleLog, got {other:?}"),
    }
    assert!(out.is_healthy(), "StaleLog must count as healthy");

    // ⭐ And the partition stays SERVABLE with the FULL chain. The stale read
    // must not shrink it — the extra event is real and committed.
    let chain = p
        .chain_if_authoritative(exec)
        .expect("guarded read")
        .expect("a stale read must leave the partition servable");
    assert_eq!(
        chain.len(),
        4,
        "the stale snapshot must not truncate the partition; coverage is monotonic"
    );
}

/// ⚠⚠ **THE ALARM MUST STILL WORK.** A genuine content conflict — a position
/// both sides hold, filled with different events — is still `Diverged`.
///
/// Without this, the #360 fix could have been "stop reporting divergence", which
/// would pass the comparator gate by switching the detector off.
#[test]
fn a360_a_real_content_conflict_is_still_diverged() {
    let r = rig();
    let p = r.pop();
    let exec = "conflict";
    p.populate_from_log(exec, &log(&["a", "b", "c"]))
        .expect("initial");

    // Same length, DIFFERENT event at position 1. Not staleness — disagreement.
    let out = p
        .populate_from_log(exec, &log(&["a", "ZZZ", "c"]))
        .expect("no error");
    match out {
        FromLog::Diverged {
            at_position,
            ref stored,
            ref log,
        } => {
            assert_eq!(at_position, 1);
            assert_eq!(stored, "b");
            assert_eq!(log, "ZZZ");
        }
        other => panic!(
            "a CONTENT conflict must still be Diverged, got {other:?}. If this \
             reports StaleLog the #360 fix has disabled the detector instead of \
             classifying it."
        ),
    }
    assert!(!out.is_healthy());
    assert_eq!(
        p.chain_if_authoritative(exec).unwrap(),
        None,
        "and a genuinely diverged partition must still refuse to serve"
    );
}

/// ⚠ A conflict where the log is SHORTER but the shared prefix disagrees is a
/// conflict, not staleness. The length must not be what decides it.
#[test]
fn a360_shorter_log_with_a_conflicting_prefix_is_diverged_not_stale() {
    let r = rig();
    let p = r.pop();
    let exec = "short-conflict";
    p.populate_from_log(exec, &log(&["a", "b", "c", "d"]))
        .expect("initial");
    let out = p
        .populate_from_log(exec, &log(&["a", "XX"]))
        .expect("no error");
    assert!(
        matches!(out, FromLog::Diverged { at_position: 1, .. }),
        "a shorter log whose shared prefix CONFLICTS is divergence, got {out:?} — \
         classifying on length alone would mask a real conflict behind staleness"
    );
}

/// ⚠ A stale snapshot leaves coverage alone because it returns EARLY.
///
/// ⚠⚠ This test does NOT exercise the monotonic guard in `write_coverage` — I
/// first wrote it believing it did, and a mutant that deleted that guard passed.
/// The early `StaleLog` return happens before `write_coverage` is reached, so
/// coverage is untouched for a different reason than the guard. Saying so,
/// because a test whose name promises one property and checks another is worse
/// than no test. The guard's own coverage is
/// `a360_coverage_guard_rejects_a_shrinking_total` below.
#[test]
fn a360_a_stale_read_leaves_coverage_and_the_partition_alone() {
    let r = rig();
    let p = r.pop();
    let exec = "mono";
    p.populate_from_log(exec, &log(&["a", "b", "c", "d", "e"]))
        .expect("full");
    let (_, before) = p.coverage(exec).unwrap().expect("coverage");
    assert_eq!(before, 5);

    p.populate_from_log(exec, &log(&["a", "b"]))
        .expect("stale read");
    let (_, after) = p.coverage(exec).unwrap().expect("coverage");
    assert_eq!(after, 5, "coverage went {before} -> {after}");
    assert_eq!(
        p.chain_if_authoritative(exec).unwrap().map(|c| c.len()),
        Some(5),
        "and the partition is still fully servable"
    );
}

/// ⚠⚠ **The monotonic-coverage guard, actually exercised.**
///
/// Reaching it needs coverage to be AHEAD of the stored chain, which is the
/// crash-mid-extend shape: `populate_from_log` claims coverage before appending,
/// so a crash between the two leaves `cov_total` larger than the partition. A
/// later read with a log SHORTER than that recorded total then calls
/// `write_coverage` with a smaller value — the only path that reaches the guard.
///
/// Coverage records how much of the execution the store has SEEN. For an
/// append-only log that only grows, so a smaller value is always the older
/// observation and is dropped.
#[test]
fn a360_coverage_guard_rejects_a_shrinking_total() {
    let r = rig();
    let p = r.pop();
    let exec = "shrink";
    p.populate_from_log(exec, &log(&["a", "b", "c"]))
        .expect("initial");

    // Simulate the crash: coverage claims 9 over a 3-event partition.
    r.substrate
        .put_overwrite("chain/shrink/cov", b"a:9")
        .expect("advance coverage past the content");
    assert_eq!(p.coverage(exec).unwrap().unwrap().1, 9);

    // A later read whose log is longer than the store (so no StaleLog) but
    // SHORTER than the recorded coverage. This is the only path to the guard.
    p.populate_from_log(exec, &log(&["a", "b", "c", "d"]))
        .expect("extend");

    let (_, total) = p.coverage(exec).unwrap().expect("coverage");
    assert_eq!(
        total, 9,
        "coverage shrank to {total}. It must never move backwards: a smaller \
         total is always the older observation, and accepting it would let a \
         partition look complete when the log once held more."
    );
}
