//! **Execution-partitioned chain store** — MVP slice proofs.
//!
//! The RFC sets three proof obligations; these are P1 and P2 plus the
//! advance-where-it-stalls demonstration. Instrument is this module's own
//! counters and timings. ⛔ Not the cross-store parity comparator and not
//! `/api/ehdb/projection-fold/diff` — both carry open asymmetries and neither
//! is on this path.

use std::time::Instant;

use ehdb_l0::chain::{chain_store_enabled, ChainError, ChainStore, CHAIN_STORE_ENV};

fn store_with(executions: usize, per_execution: usize) -> ChainStore {
    let mut s = ChainStore::new();
    for e in 0..executions {
        let exec = format!("exec-{e}");
        let mut prev: Option<String> = None;
        for i in 0..per_execution {
            let ev = format!("{exec}-ev-{i}");
            s.append(&exec, &ev, prev.as_deref(), None, "{}")
                .expect("append");
            prev = Some(ev);
        }
    }
    s
}

// ---------------------------------------------------------------------------
// (a) append to an execution partition
// ---------------------------------------------------------------------------

#[test]
fn append_assigns_a_per_execution_sequence_starting_at_one() {
    let mut s = ChainStore::new();
    assert_eq!(s.append("e1", "a", None, None, "{}").unwrap(), 1);
    assert_eq!(s.append("e1", "b", Some("a"), None, "{}").unwrap(), 2);
    // A second execution starts its OWN sequence at 1 — the sequence is
    // per-partition, not global. This is the property `global_sequence` lacks.
    assert_eq!(s.append("e2", "x", None, None, "{}").unwrap(), 1);
    assert_eq!(s.partition("e1").unwrap().len(), 2);
    assert_eq!(s.partition("e2").unwrap().len(), 1);
}

/// I1: the chain is a path. An append that does not extend the head is refused,
/// which is what stops a second writer forking it.
#[test]
fn an_append_that_does_not_extend_the_head_is_refused() {
    let mut s = ChainStore::new();
    s.append("e1", "a", None, None, "{}").unwrap();
    s.append("e1", "b", Some("a"), None, "{}").unwrap();

    // A stale writer still thinks the head is `a`.
    let err = s
        .append("e1", "c", Some("a"), None, "{}")
        .expect_err("must refuse");
    assert!(matches!(err, ChainError::NotHead { .. }));
    assert!(err.message().contains("out of order"));

    // And a root append onto a non-empty partition is equally refused.
    assert!(matches!(
        s.append("e1", "d", None, None, "{}"),
        Err(ChainError::NotHead { .. })
    ));
}

#[test]
fn a_duplicate_event_id_is_refused() {
    let mut s = ChainStore::new();
    s.append("e1", "a", None, None, "{}").unwrap();
    assert!(s.append("e1", "a", Some("a"), None, "{}").is_err());
}

// ---------------------------------------------------------------------------
// (b) O(1) parent lookup — P1
// ---------------------------------------------------------------------------

#[test]
fn parent_of_resolves_the_predecessor_and_stops_at_the_root() {
    let mut s = ChainStore::new();
    s.append("e1", "a", None, None, "{}").unwrap();
    s.append("e1", "b", Some("a"), None, "{}").unwrap();
    let b = s.get("e1", "b").unwrap();
    let a = s.parent_of(b).unwrap().expect("b has a parent");
    assert_eq!(a.event_id, "a");
    assert!(s.parent_of(a).unwrap().is_none(), "the root has no parent");
}

/// ⭐ **P1 — the parent lookup does not depend on store size.**
///
/// Measured rather than asserted, and over a 100x range of `N` so a constant
/// factor cannot hide a linear term. The current store fails this in the
/// inverted direction: its own docs measure `read_execution` at 858 ms for a
/// 92 KB result, because "the latency does not track the result size... it is
/// the replay in front of it".
#[test]
fn parent_lookup_cost_is_independent_of_total_store_size() {
    let small = store_with(10, 10); // N = 100
    let large = store_with(300, 10); // N = 3_000, 30x
    assert_eq!(small.total_events(), 100);
    assert_eq!(large.total_events(), 3_000);

    let probe = |s: &ChainStore| {
        let ev = s.get("exec-5", "exec-5-ev-9").expect("present in both");
        let t = Instant::now();
        for _ in 0..2_000 {
            let _ = s.parent_of(ev).unwrap();
        }
        t.elapsed()
    };
    let (t_small, t_large) = (probe(&small), probe(&large));
    let ratio = t_large.as_secs_f64() / t_small.as_secs_f64().max(1e-9);

    // A linear term would show ~100x here. A tree probe over a 100x larger
    // keyspace costs at most a couple of extra comparisons.
    assert!(
        ratio < 5.0,
        "parent lookup scaled with store size: {ratio:.2}x for 30x the events \
         (small={t_small:?} large={t_large:?})"
    );
}

// ---------------------------------------------------------------------------
// (c) ordered chain read — P2
// ---------------------------------------------------------------------------

#[test]
fn chain_returns_the_execution_in_order_and_nothing_else() {
    let s = store_with(50, 8);
    let chain = s.chain("exec-7");
    assert_eq!(chain.len(), 8, "only this execution's events");
    let seqs: Vec<u64> = chain.iter().map(|e| e.exec_seq).collect();
    assert_eq!(seqs, (1..=8).collect::<Vec<_>>(), "ascending, gapless");
    assert!(
        chain.iter().all(|e| e.execution_id == "exec-7"),
        "no other execution's events leaked in"
    );
}

/// ⭐ **P2 — chain-read cost is a function of `k`, not `N`.**
///
/// `k` fixed at 10, `N` grown 100x. This is the exact measurement the current
/// store inverts.
#[test]
fn chain_read_cost_depends_on_k_not_on_n() {
    let small = store_with(10, 10);
    let large = store_with(300, 10);

    let probe = |s: &ChainStore| {
        let t = Instant::now();
        for _ in 0..500 {
            let c = s.chain("exec-5");
            assert_eq!(c.len(), 10);
        }
        t.elapsed()
    };
    let (t_small, t_large) = (probe(&small), probe(&large));
    let ratio = t_large.as_secs_f64() / t_small.as_secs_f64().max(1e-9);
    assert!(
        ratio < 5.0,
        "chain read scaled with N: {ratio:.2}x for 100x the events \
         (small={t_small:?} large={t_large:?})"
    );
}

/// ⚠ **Positive control for the two tests above.** If the harness cannot detect
/// a cost that genuinely IS linear in N, the flat readings prove nothing. This
/// scans every partition on purpose and asserts the ratio the others reject.
#[test]
fn the_scaling_harness_can_detect_a_linear_cost() {
    let small = store_with(10, 10);
    let large = store_with(300, 10);

    // A deliberate full scan — what the current store does per read.
    let linear_probe = |s: &ChainStore| {
        let t = Instant::now();
        for _ in 0..50 {
            let mut seen = 0usize;
            for e in 0..s.execution_count() {
                seen += s.chain(&format!("exec-{e}")).len();
            }
            assert_eq!(seen, s.total_events());
        }
        t.elapsed()
    };
    let (t_small, t_large) = (linear_probe(&small), linear_probe(&large));
    let ratio = t_large.as_secs_f64() / t_small.as_secs_f64().max(1e-9);
    assert!(
        ratio > 5.0,
        "the harness failed to detect a known-linear cost ({ratio:.2}x) — the flat \
         readings in the P1/P2 tests cannot be trusted until this one shows scaling"
    );
}

// ---------------------------------------------------------------------------
// (d) chain-following: advance where the scan stalls
// ---------------------------------------------------------------------------

#[test]
fn walk_from_head_returns_the_chain_newest_first() {
    let mut s = ChainStore::new();
    for (ev, prev) in [("a", None), ("b", Some("a")), ("c", Some("b"))] {
        s.append("e1", ev, prev, None, "{}").unwrap();
    }
    let walked: Vec<&str> = s
        .walk_from_head("e1")
        .unwrap()
        .iter()
        .map(|e| e.event_id.as_str())
        .collect();
    assert_eq!(walked, vec!["c", "b", "a"]);
    assert!(s.chain_is_complete("e1").unwrap());
}

/// ⭐⭐ **The whole point of the slice.** A gap is reported with the key it is
/// waiting for. The current store returns an empty/short result, which the
/// drive reads as "not ready" and re-drives forever — 53 executions did exactly
/// that, every 8 s, and nothing anywhere said why.
#[test]
fn a_gap_is_a_named_error_at_a_specific_key_never_an_empty_result() {
    let mut s = ChainStore::new();
    s.append("e1", "a", None, None, "{}").unwrap();
    s.append("e1", "b", Some("a"), None, "{}").unwrap();
    // A replica that has `b` but not yet `a` — the normal eventual-consistency
    // state, and also what a truncated log looks like.
    let mut partial = ChainStore::new();
    partial.append("e1", "b_orphan", None, None, "{}").unwrap();
    let orphan = partial.get("e1", "b_orphan").unwrap();
    assert!(partial.parent_of(orphan).unwrap().is_none(), "root, no gap");

    // Now a real gap: an event whose prev is absent.
    let mut gapped = ChainStore::new();
    gapped.append("e1", "a", None, None, "{}").unwrap();
    gapped.append("e1", "b", Some("a"), None, "{}").unwrap();
    let synthetic = ehdb_l0::chain::ChainEvent {
        exec_seq: 99,
        event_id: "z".into(),
        prev_event_id: Some("missing-link".into()),
        execution_id: "e1".into(),
        parent_execution_id: None,
        payload: "{}".into(),
    };
    let err = gapped.parent_of(&synthetic).expect_err("must name the gap");
    assert_eq!(
        err,
        ChainError::GapAt {
            execution_id: "e1".into(),
            event_id: "missing-link".into()
        }
    );
    let msg = err.message();
    assert!(msg.contains("missing-link"), "must name the KEY: {msg}");
    assert!(msg.contains("named, not empty"), "{msg}");
}

/// ⚠ Control for the test above: a complete chain must NOT report a gap.
/// Otherwise "reports gaps" could be satisfied by something that always does.
#[test]
fn a_complete_chain_reports_no_gap() {
    let s = store_with(5, 20);
    for e in 0..5 {
        let exec = format!("exec-{e}");
        assert!(
            s.chain_is_complete(&exec).unwrap(),
            "{exec} should be complete"
        );
        assert_eq!(s.walk_from_head(&exec).unwrap().len(), 20);
    }
}

/// An unknown execution is empty, not an error — an absent probe is not a gap.
#[test]
fn an_unknown_execution_is_empty_not_a_gap() {
    let s = store_with(2, 2);
    assert!(s.chain("nope").is_empty());
    assert!(s.walk_from_head("nope").unwrap().is_empty());
}

// ---------------------------------------------------------------------------
// Execution tree (pattern C) + the flag
// ---------------------------------------------------------------------------

#[test]
fn the_execution_tree_edge_is_one_hop() {
    let mut s = ChainStore::new();
    s.append("root", "r1", None, None, "{}").unwrap();
    s.append("child", "c1", None, Some("root"), "{}").unwrap();
    s.append("grandchild", "g1", None, Some("child"), "{}")
        .unwrap();
    assert_eq!(s.parent_execution_of("grandchild"), Some("child"));
    assert_eq!(s.parent_execution_of("child"), Some("root"));
    assert_eq!(s.parent_execution_of("root"), None);
}

#[test]
fn the_flag_is_off_by_default_and_fails_safe() {
    // Serialised by name: this test owns the env var.
    let prev = std::env::var(CHAIN_STORE_ENV).ok();
    unsafe { std::env::remove_var(CHAIN_STORE_ENV) };
    assert!(!chain_store_enabled(), "default must be OFF");
    for junk in ["", " ", "off", "no", "0", "enabled", "chain"] {
        unsafe { std::env::set_var(CHAIN_STORE_ENV, junk) };
        assert!(
            !chain_store_enabled(),
            "{junk:?} must not enable the chain path"
        );
    }
    unsafe { std::env::set_var(CHAIN_STORE_ENV, "true") };
    assert!(chain_store_enabled());
    match prev {
        Some(v) => unsafe { std::env::set_var(CHAIN_STORE_ENV, v) },
        None => unsafe { std::env::remove_var(CHAIN_STORE_ENV) },
    }
}

// ---------------------------------------------------------------------------
// Eventual replication — the follower path.
//
// ⚠ These tests exist because a mutation battery found a hole: K7 ("report a
// complete chain when there is a gap") SURVIVED, and it survived because the
// gap was unconstructible. `append` enforces the head, so no test could build
// a partition that actually had a hole in it — the assertion had nothing to
// bite on. Async replication delivers out of order, so a follower MUST hold
// e3 while waiting for e2; that path was missing from the slice entirely.
// The surviving mutant was a question about the design, not just the test.
// ---------------------------------------------------------------------------

use ehdb_l0::chain::ChainEvent;

fn ev(exec: &str, seq: u64, id: &str, prev: Option<&str>) -> ChainEvent {
    ChainEvent {
        exec_seq: seq,
        event_id: id.to_string(),
        prev_event_id: prev.map(str::to_string),
        execution_id: exec.to_string(),
        parent_execution_id: None,
        payload: "{}".to_string(),
    }
}

/// ⭐ Out-of-order delivery is accepted, and the resulting hole is reported as
/// INCOMPLETE — not as a finished chain.
#[test]
fn a_replica_missing_a_middle_event_reports_incomplete() {
    let mut r = ChainStore::new();
    r.apply_replicated(ev("e1", 1, "a", None)).unwrap();
    r.apply_replicated(ev("e1", 3, "c", Some("b"))).unwrap(); // b has not arrived
    assert_eq!(r.chain("e1").len(), 2, "holds what it has");
    assert!(
        !r.chain_is_complete("e1").unwrap(),
        "a hole must read as INCOMPLETE, never as a finished chain"
    );
    // And the gap names the key it is waiting for.
    let c = r.get("e1", "c").unwrap();
    assert_eq!(
        r.parent_of(c).expect_err("gap"),
        ChainError::GapAt {
            execution_id: "e1".into(),
            event_id: "b".into()
        }
    );
}

/// ⚠ Control: once the missing event arrives, the same partition reports
/// complete. Without this, "reports incomplete" could be satisfied by
/// something that always does.
#[test]
fn the_same_replica_reports_complete_once_the_gap_is_filled() {
    let mut r = ChainStore::new();
    r.apply_replicated(ev("e1", 1, "a", None)).unwrap();
    r.apply_replicated(ev("e1", 3, "c", Some("b"))).unwrap();
    assert!(!r.chain_is_complete("e1").unwrap());

    r.apply_replicated(ev("e1", 2, "b", Some("a"))).unwrap(); // catch-up
    assert!(
        r.chain_is_complete("e1").unwrap(),
        "catch-up must close the gap"
    );
    let walked: Vec<&str> = r
        .walk_from_head("e1")
        .unwrap()
        .iter()
        .map(|e| e.event_id.as_str())
        .collect();
    assert_eq!(walked, vec!["c", "b", "a"]);
}

/// ⭐ A shorter-but-contiguous prefix is COMPLETE. Eventual consistency means a
/// replica may simply be behind, and being behind is not being broken — this is
/// the distinction that lets a reader serve a bounded-stale prefix safely.
#[test]
fn a_contiguous_prefix_is_complete_not_incomplete() {
    let mut r = ChainStore::new();
    r.apply_replicated(ev("e1", 1, "a", None)).unwrap();
    r.apply_replicated(ev("e1", 2, "b", Some("a"))).unwrap();
    assert!(
        r.chain_is_complete("e1").unwrap(),
        "behind is not broken: a contiguous prefix is a valid complete chain"
    );
}

/// Replication retries must be idempotent.
#[test]
fn redelivering_an_identical_event_is_idempotent() {
    let mut r = ChainStore::new();
    r.apply_replicated(ev("e1", 1, "a", None)).unwrap();
    r.apply_replicated(ev("e1", 1, "a", None)).unwrap();
    r.apply_replicated(ev("e1", 1, "a", None)).unwrap();
    assert_eq!(r.chain("e1").len(), 1);
}

/// ⚠ Property N's detector. A *conflicting* event at a key we already hold is a
/// fork, which single-writer exclusion should make unreachable — so if this
/// ever fires in the field, exclusion was not real. It is the alarm, not the
/// guard.
#[test]
fn a_conflicting_event_at_a_known_key_is_reported_as_a_fork() {
    let mut r = ChainStore::new();
    r.apply_replicated(ev("e1", 1, "a", None)).unwrap();
    let conflicting = ChainEvent {
        payload: "{\"different\":true}".into(),
        ..ev("e1", 1, "a", None)
    };
    let err = r
        .apply_replicated(conflicting)
        .expect_err("must detect the fork");
    assert!(matches!(err, ChainError::Forked { .. }));
    assert!(err.message().contains("exclusion was not real"));
}

/// The writer path stays strict — `apply_replicated`'s laxity must not leak
/// into `append`, or the home partition could fork.
#[test]
fn the_writer_path_is_still_strict_after_replication_ingest() {
    let mut s = ChainStore::new();
    s.apply_replicated(ev("e1", 1, "a", None)).unwrap();
    // Appending must still have to extend the head.
    assert!(matches!(
        s.append("e1", "z", Some("not-the-head"), None, "{}"),
        Err(ChainError::NotHead { .. })
    ));
    assert_eq!(s.append("e1", "b", Some("a"), None, "{}").unwrap(), 2);
}
