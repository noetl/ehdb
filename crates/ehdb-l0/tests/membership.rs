//! **Membership adoption over D8** (multi-region spec M2a).
//! Policy layer only — no socket, no bring-up, no write path.
//!
//! The instrument here is the module's own view/counter output. ⛔ Explicitly
//! NOT the cross-store parity comparator and NOT
//! `/api/ehdb/projection-fold/diff`: both carry open asymmetries, and neither
//! is anywhere near this path.

use ehdb_l0::membership::{
    view_for, MemberEntry, MembershipMode, MembershipPolicy, TransitionCounts, TransitionKind,
    DEFAULT_STALL_LIMIT,
};
use ehdb_l0::runtime::{RuntimeStore, DATASET_D8_RUNTIME};
use ehdb_l0::substrate::LocalFsSubstrate;
use std::sync::Arc;

fn m(id: &str, hb: u64) -> MemberEntry {
    MemberEntry {
        worker_id: id.to_string(),
        heartbeat: hb,
    }
}

// ---------------------------------------------------------------------------
// The flag. Default off; a typo must not switch a subsystem on.
// ---------------------------------------------------------------------------

#[test]
fn default_mode_is_off_and_touches_nothing() {
    assert_eq!(MembershipMode::default(), MembershipMode::Off);
    assert!(!MembershipMode::Off.touches_d8());
    assert_eq!(MembershipMode::parse(None), MembershipMode::Off);
}

#[test]
fn modes_parse_by_name() {
    assert_eq!(MembershipMode::parse(Some("d8")), MembershipMode::D8);
    assert_eq!(
        MembershipMode::parse(Some(" GOSSIP ")),
        MembershipMode::Gossip
    );
    assert!(MembershipMode::D8.touches_d8());
    assert!(MembershipMode::Gossip.touches_d8());
}

#[test]
fn an_unrecognised_mode_falls_back_to_off() {
    for junk in ["", "  ", "on", "true", "1", "D-8", "swim", "foca"] {
        assert_eq!(
            MembershipMode::parse(Some(junk)),
            MembershipMode::Off,
            "unrecognised mode {junk:?} must fail safe to Off"
        );
    }
}

// ---------------------------------------------------------------------------
// E5 — every metric label pinned at 0, unconditionally.
// ---------------------------------------------------------------------------

/// ⭐ `Registry::gather` prunes families with no children, so a labelled
/// counter is ABSENT until it fires — indistinguishable from a broken
/// exporter or a pod predating the metric. Every label must exist at 0 from
/// construction.
#[test]
fn every_transition_label_is_present_at_zero_before_anything_happens() {
    let counts = TransitionCounts::default();
    let exported = counts.exported();
    assert_eq!(
        exported.len(),
        TransitionKind::ALL.len(),
        "every label must be exported, got {exported:?}"
    );
    for k in TransitionKind::ALL {
        assert_eq!(counts.get(k), 0, "label {} must be pinned at 0", k.label());
        assert!(
            exported.iter().any(|(l, v)| *l == k.label() && *v == 0),
            "label {} missing from export",
            k.label()
        );
    }
}

/// ⚠ The pin must not depend on the mode. A pin inside `if mode != Off` is
/// not a pin — it leaves counters absent on exactly the configuration whose
/// value someone would be reading.
#[test]
fn the_pin_does_not_depend_on_the_mode() {
    for mode in [
        MembershipMode::Off,
        MembershipMode::D8,
        MembershipMode::Gossip,
    ] {
        let _policy = MembershipPolicy::new(mode);
        let counts = TransitionCounts::default();
        assert_eq!(counts.exported().len(), TransitionKind::ALL.len());
    }
}

// ---------------------------------------------------------------------------
// E2 — the watermark evicts, and a stalled watermark is VISIBLE.
// ---------------------------------------------------------------------------

#[test]
fn a_node_below_the_watermark_is_evicted_and_counted() {
    let mut policy = MembershipPolicy::new(MembershipMode::D8);
    policy.advance(10);
    let mut counts = TransitionCounts::default();
    let view = view_for(&[m("a", 12), m("b", 3)], &policy, &mut counts);
    assert_eq!(view.live, vec![m("a", 12)]);
    assert_eq!(view.evicted, vec![m("b", 3)]);
    assert_eq!(counts.get(TransitionKind::Evict), 1);
}

/// ⭐ The test the spec asks for: it must FAIL if the watermark advance is
/// removed. Here the advance is what separates the two nodes; with the
/// watermark left at 0 nothing is ever evicted.
#[test]
fn without_advancing_the_watermark_nothing_is_ever_evicted() {
    let policy = MembershipPolicy::new(MembershipMode::D8); // never advanced
    let mut counts = TransitionCounts::default();
    let view = view_for(&[m("a", 12), m("b", 3)], &policy, &mut counts);
    assert_eq!(view.live.len(), 2, "a watermark of 0 evicts nobody");
    assert_eq!(counts.get(TransitionKind::Evict), 0);
}

/// ⚠ A watermark that moves backwards resurrects evicted nodes. Refuse it.
#[test]
fn the_watermark_is_monotone() {
    let mut policy = MembershipPolicy::new(MembershipMode::D8);
    assert!(policy.advance(10));
    assert!(!policy.advance(5), "a lower watermark must be refused");
    assert_eq!(policy.watermark(), 10);
    assert!(!policy.advance(10), "an equal watermark is not an advance");
}

/// ⭐⭐ The inert-gate guard. A policy that stops advancing evicts nobody, so
/// every node reads live and the subsystem looks healthy exactly when it has
/// stopped working. The view must say so rather than answer confidently.
#[test]
fn a_stalled_policy_reports_itself_untrustworthy() {
    let mut policy = MembershipPolicy::new(MembershipMode::D8);
    policy.advance(10);
    let mut counts = TransitionCounts::default();
    assert!(view_for(&[m("a", 12)], &policy, &mut counts).is_authoritative());

    for _ in 0..DEFAULT_STALL_LIMIT {
        policy.tick_without_advance();
    }
    let view = view_for(&[m("a", 12)], &policy, &mut counts);
    assert!(view.stalled, "a stalled policy must mark its view");
    assert!(
        !view.is_authoritative(),
        "a stalled view must not claim authority: live is 'everyone we ever saw'"
    );
}

/// ⚠ Control for the test above: advancing again clears the stall. Without
/// this, `stalled` could be stuck true and the guard would fire always,
/// which is as useless as firing never.
#[test]
fn advancing_again_clears_the_stall() {
    let mut policy = MembershipPolicy::new(MembershipMode::D8).with_stall_limit(2);
    policy.tick_without_advance();
    policy.tick_without_advance();
    assert!(policy.stalled());
    assert!(policy.advance(99));
    assert!(!policy.stalled(), "an advance must clear the stall");
}

/// A failed advance counts toward the stall — otherwise a caller looping on
/// a watermark that never grows would never be reported.
#[test]
fn a_refused_advance_counts_toward_the_stall() {
    let mut policy = MembershipPolicy::new(MembershipMode::D8).with_stall_limit(2);
    policy.advance(10);
    policy.advance(5); // refused
    policy.advance(5); // refused
    assert!(policy.stalled());
}

// ---------------------------------------------------------------------------
// E1 — a REAL consumer: the policy driven off an actual D8 store.
// ---------------------------------------------------------------------------

/// ⭐ The gap M2a exists to close. D8 had 12 running tests and **0 reachable
/// consumers** — the only two references outside `runtime.rs` were an inert
/// crate and a re-export. This exercises D8's real API end to end through the
/// policy layer.
#[test]
fn the_policy_consumes_a_real_d8_store() {
    let dir = tempfile::tempdir().expect("tempdir");
    let substrate = Arc::new(LocalFsSubstrate::new(dir.path()).expect("substrate"));
    let mut store =
        RuntimeStore::open(RuntimeStore::config(dir.path()), substrate).expect("open D8");

    store.register("worker-a", "pool=user,arch=arm64").unwrap();
    store
        .register("worker-b", "pool=system,arch=arm64")
        .unwrap();
    for _ in 0..5 {
        store.heartbeat("worker-a").unwrap();
    }
    store.heartbeat("worker-b").unwrap();

    let members: Vec<MemberEntry> = store
        .list_live()
        .unwrap()
        .into_iter()
        .map(|s| MemberEntry {
            worker_id: s.worker_id,
            heartbeat: s.heartbeat,
        })
        .collect();
    assert_eq!(members.len(), 2, "both workers registered");

    // worker-a beat 5 extra times; worker-b did not. A watermark between them
    // evicts b.
    let mut policy = MembershipPolicy::new(MembershipMode::D8);
    policy.advance(3);
    let mut counts = TransitionCounts::default();
    let view = view_for(&members, &policy, &mut counts);

    assert_eq!(view.live.len(), 1, "only the beating worker stays live");
    assert_eq!(view.live[0].worker_id, "worker-a");
    assert_eq!(counts.get(TransitionKind::Evict), 1);
    assert!(view.is_authoritative());

    // And D8's own watermark query agrees — the policy is not inventing a
    // second notion of liveness.
    let d8_live = store.list_live_since(3).unwrap();
    assert_eq!(d8_live.len(), 1);
    assert_eq!(d8_live[0].worker_id, "worker-a");

    // deregister is the other transition D8 owns. It removes exactly the
    // named worker -- b is untouched, which is the assertion worth making:
    // a deregister that emptied the roster would also pass an is_empty check.
    assert!(store.deregister("worker-a").unwrap());
    let after: Vec<String> = store
        .list_live()
        .unwrap()
        .into_iter()
        .map(|s| s.worker_id)
        .collect();
    assert_eq!(after, vec!["worker-b".to_string()]);
    // Deregistering an unknown worker reports false rather than erroring.
    assert!(!store.deregister("worker-zzz").unwrap());

    assert_eq!(DATASET_D8_RUNTIME, "d8_runtime");
}

/// ⚠ Added because a mutation battery found the gap: flipping the eviction
/// comparison from `>=` to `>` left every other test green, because none of
/// them placed a heartbeat exactly ON the watermark.
///
/// The boundary is inclusive and has to be, because it is what
/// `list_live_since(min_heartbeat)` means in D8 — a node at exactly the
/// watermark is live. An exclusive comparison would evict a node on every
/// tick where its heartbeat just caught up, producing a slow trickle of
/// spurious evictions that looks like real churn.
#[test]
fn the_eviction_boundary_is_inclusive_at_the_watermark() {
    let mut policy = MembershipPolicy::new(MembershipMode::D8);
    policy.advance(10);
    let mut counts = TransitionCounts::default();

    let view = view_for(
        &[m("exactly", 10), m("below", 9), m("above", 11)],
        &policy,
        &mut counts,
    );

    let live: Vec<&str> = view.live.iter().map(|e| e.worker_id.as_str()).collect();
    assert_eq!(
        live,
        vec!["exactly", "above"],
        "a heartbeat exactly at the watermark is LIVE, not evicted"
    );
    assert_eq!(view.evicted.len(), 1);
    assert_eq!(view.evicted[0].worker_id, "below");
    assert_eq!(counts.get(TransitionKind::Evict), 1);
}

/// And the same boundary as D8 itself draws it, so the policy layer and the
/// store cannot disagree about who is live.
#[test]
fn the_policy_boundary_matches_d8s_own_list_live_since() {
    let dir = tempfile::tempdir().expect("tempdir");
    let substrate = Arc::new(LocalFsSubstrate::new(dir.path()).expect("substrate"));
    let mut store =
        RuntimeStore::open(RuntimeStore::config(dir.path()), substrate).expect("open D8");
    store.register("w", "pool=user").unwrap();
    let hb = store.get("w").unwrap().expect("registered").heartbeat;

    // D8 says a node at exactly `hb` is live.
    assert_eq!(store.list_live_since(hb).unwrap().len(), 1);

    // So must the policy.
    let mut policy = MembershipPolicy::new(MembershipMode::D8);
    policy.advance(hb);
    let mut counts = TransitionCounts::default();
    let view = view_for(&[m("w", hb)], &policy, &mut counts);
    assert_eq!(
        view.live.len(),
        1,
        "policy must agree with D8 at the boundary"
    );
    assert_eq!(counts.get(TransitionKind::Evict), 0);
}
