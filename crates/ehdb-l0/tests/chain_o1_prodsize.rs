//! **O(1) predecessor lookup at prod-sized chain length.**
//!
//! The earlier P1 proof (`chain_durable_p1.rs`) held the chain SHORT and grew
//! the STORE, showing the read count does not depend on `N`. This one holds the
//! store modest and grows the CHAIN, showing it does not depend on `k` either.
//!
//! Both are needed, and neither implies the other: an implementation that walks
//! a partition's index from the start would pass the first and fail this one.

use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Instant;

use ehdb_l0::chain_store_durable::DurableChainStore;
use ehdb_l0::substrate::{CountingSubstrate, DurableSubstrate, LocalFsSubstrate};

/// A long chain. 2,000 events in ONE execution — two orders of magnitude past
/// the 7-event shape the stall was reported on, and past any execution length
/// seen in the incident write-ups.
const LONG_CHAIN: u64 = 2_000;
/// A short one, for the comparison.
const SHORT_CHAIN: u64 = 20;

fn store(
    dir: &std::path::Path,
) -> (
    DurableChainStore,
    Arc<ehdb_l0::substrate::SubstrateCounters>,
    Arc<std::sync::Mutex<Vec<String>>>,
) {
    let fs = LocalFsSubstrate::new(dir).expect("substrate");
    let counting = CountingSubstrate::new(fs);
    let counters = counting.counters();
    let keys = counting.read_keys();
    (
        DurableChainStore::new(Arc::new(counting) as Arc<dyn DurableSubstrate>),
        counters,
        keys,
    )
}

/// Build one execution with `len` chained events. Returns the head's id.
fn build_chain(store: &DurableChainStore, exec: &str, len: u64) -> String {
    let mut prev: Option<String> = None;
    for i in 0..len {
        let ev = format!("{exec}-ev-{i}");
        store
            .append(exec, &ev, prev.as_deref(), None, "{}")
            .expect("append");
        prev = Some(ev);
    }
    prev.expect("non-empty")
}

/// ⭐⭐ **The predecessor read costs exactly one substrate operation on a
/// 2,000-event chain — the same as on a 20-event chain.**
///
/// Asserted as an exact count on both, and on the exact key touched, so a
/// larger-but-still-constant cost (say, two reads) would fail rather than pass
/// as "still constant".
#[test]
fn the_predecessor_read_is_one_operation_at_prod_chain_length() {
    let mut observed = Vec::new();

    for (label, len) in [("short", SHORT_CHAIN), ("long", LONG_CHAIN)] {
        let dir = tempfile::tempdir().unwrap();
        let (s, counters, keys) = store(dir.path());
        let head_id = build_chain(&s, "exec-a", len);
        let head = s.get("exec-a", &head_id).expect("read").expect("present");

        counters.get_all_calls.store(0, Ordering::SeqCst);
        counters.get_range_calls.store(0, Ordering::SeqCst);
        counters.list_prefix_calls.store(0, Ordering::SeqCst);
        keys.lock().unwrap().clear();

        let t = Instant::now();
        let parent = s.parent_of(&head).expect("resolves").expect("has a parent");
        let elapsed = t.elapsed();

        let reads = counters.get_all_calls.load(Ordering::SeqCst);
        let ranges = counters.get_range_calls.load(Ordering::SeqCst);
        let lists = counters.list_prefix_calls.load(Ordering::SeqCst);
        let touched = keys.lock().unwrap().clone();

        assert_eq!(
            reads, 1,
            "{label}: expected exactly one get_all, got {reads}"
        );
        assert_eq!(ranges, 0, "{label}: no ranged reads");
        assert_eq!(
            lists, 0,
            "{label}: a predecessor fetch must not ENUMERATE anything — the key is \
             computed from ids the caller already holds, not searched for"
        );
        assert_eq!(
            touched,
            vec![format!("chain/exec-a/ev/exec-a-ev-{}", len - 2)],
            "{label}: must read exactly the computed key"
        );
        assert_eq!(
            parent.event_id,
            format!("exec-a-ev-{}", len - 2),
            "{label}: wrong predecessor"
        );

        observed.push((label, len, reads + ranges + lists, elapsed));
    }

    let (_, _, short_ops, short_t) = observed[0];
    let (_, _, long_ops, long_t) = observed[1];
    assert_eq!(
        short_ops, long_ops,
        "operation count changed with chain length: {observed:?}"
    );

    // ⚠ Wall time is reported, not asserted as a ratio: a single sub-millisecond
    // filesystem read is dominated by noise, and a flaky timing assertion would
    // be worse than none. The OPERATION COUNT is the load-bearing evidence —
    // it is exact, and it is what a linear term would move.
    eprintln!(
        "O(1) evidence: {}-event chain {short_ops} op(s) in {short_t:?}; \
         {}-event chain {long_ops} op(s) in {long_t:?}",
        SHORT_CHAIN, LONG_CHAIN
    );
}

/// ⚠ **Positive control.** If the counter could not register a cost that IS
/// proportional to chain length, the constant readings above would prove
/// nothing. A full chain read over the same partition must scale.
#[test]
fn the_counter_registers_a_cost_that_does_scale_with_chain_length() {
    let mut ops = Vec::new();
    for len in [SHORT_CHAIN, 200] {
        let dir = tempfile::tempdir().unwrap();
        let (s, counters, _k) = store(dir.path());
        build_chain(&s, "exec-a", len);
        counters.get_all_calls.store(0, Ordering::SeqCst);
        counters.list_prefix_calls.store(0, Ordering::SeqCst);
        let c = s.chain("exec-a").expect("chain");
        assert_eq!(c.len() as u64, len);
        ops.push(
            counters.get_all_calls.load(Ordering::SeqCst)
                + counters.list_prefix_calls.load(Ordering::SeqCst),
        );
    }
    assert!(
        ops[1] > ops[0] * 5,
        "a full chain read is O(k) and must scale; got {ops:?} — if it does not, \
         the counter is not observing and the constant readings are void"
    );
}

/// Walking the whole 2,000-event chain costs exactly one read per hop — O(k),
/// with no per-hop enumeration. This is the cost the drive actually pays.
#[test]
fn a_full_walk_costs_one_read_per_hop_and_no_enumeration() {
    let dir = tempfile::tempdir().unwrap();
    let (s, counters, _k) = store(dir.path());
    build_chain(&s, "exec-a", LONG_CHAIN);

    counters.get_all_calls.store(0, Ordering::SeqCst);
    counters.list_prefix_calls.store(0, Ordering::SeqCst);
    let walked = s.walk_from_head("exec-a").expect("walk");
    let reads = counters.get_all_calls.load(Ordering::SeqCst);
    let lists = counters.list_prefix_calls.load(Ordering::SeqCst);

    assert_eq!(walked.len() as u64, LONG_CHAIN, "the whole chain");
    assert_eq!(
        lists, 0,
        "a pointer walk must never enumerate — it follows keys it is given"
    );
    // head lookup + one per hop.
    assert_eq!(
        reads,
        LONG_CHAIN + 1,
        "expected exactly one read per hop plus the head lookup"
    );
}

/// And the chain is intact end to end at that length — an O(1) read of a
/// corrupt chain would be worthless.
#[test]
fn the_prod_sized_chain_is_complete_and_correctly_ordered() {
    let dir = tempfile::tempdir().unwrap();
    let (s, _c, _k) = store(dir.path());
    build_chain(&s, "exec-a", LONG_CHAIN);

    assert!(s.chain_is_complete("exec-a").expect("complete"));
    let chain = s.chain("exec-a").expect("chain");
    assert_eq!(chain.len() as u64, LONG_CHAIN);
    let seqs: Vec<u64> = chain.iter().map(|e| e.exec_seq).collect();
    assert_eq!(
        seqs,
        (1..=LONG_CHAIN).collect::<Vec<u64>>(),
        "ascending and gapless across the 3- and 4-digit padding boundaries"
    );
}
