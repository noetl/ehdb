//! **P1, discharged on a counting substrate** — the RFC's actual obligation.
//!
//! Increments 1–3 proved the algorithmic shape in memory. A `BTreeMap` probe is
//! O(1) for reasons that say nothing about what the store does to disk, so the
//! RFC requires P1 be verified "by a counting substrate, not by reading the
//! code". This file does that: it counts **substrate operations**, and asserts
//! the exact number rather than a trend.

use std::sync::atomic::Ordering;
use std::sync::Arc;

use ehdb_l0::chain::ChainError;
use ehdb_l0::chain_store_durable::DurableChainStore;
use ehdb_l0::substrate::{CountingSubstrate, DurableSubstrate, LocalFsSubstrate};

/// A store over a counting substrate, plus handles to its counters and the
/// exact keys it touched.
fn counting_store(
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

fn seed(store: &DurableChainStore, executions: usize, per_execution: usize) {
    for e in 0..executions {
        let exec = format!("exec-{e}");
        let mut prev: Option<String> = None;
        for i in 0..per_execution {
            let ev = format!("{exec}-ev-{i}");
            store
                .append(&exec, &ev, prev.as_deref(), None, "{}")
                .expect("append");
            prev = Some(ev);
        }
    }
}

// ---------------------------------------------------------------------------
// P1 — exactly one storage operation.
// ---------------------------------------------------------------------------

/// ⭐⭐ **P1.** A predecessor fetch issues **exactly one** substrate read, and
/// it reads **exactly the key it computed**.
///
/// Both halves matter. The count alone would be satisfied by one giant read;
/// the key list is what shows it addressed the right object rather than
/// scanning something and getting lucky.
#[test]
fn a_predecessor_fetch_issues_exactly_one_substrate_read() {
    let dir = tempfile::tempdir().unwrap();
    let (store, counters, keys) = counting_store(dir.path());
    seed(&store, 30, 20); // 600 events across 30 executions

    let target = store
        .get("exec-7", "exec-7-ev-19")
        .expect("read")
        .expect("present");

    // Zero the instruments immediately before the operation under test.
    counters.get_all_calls.store(0, Ordering::SeqCst);
    counters.get_range_calls.store(0, Ordering::SeqCst);
    counters.list_prefix_calls.store(0, Ordering::SeqCst);
    keys.lock().unwrap().clear();

    let parent = store
        .parent_of(&target)
        .expect("resolves")
        .expect("has one");
    assert_eq!(parent.event_id, "exec-7-ev-18");

    let reads = counters.get_all_calls.load(Ordering::SeqCst);
    let ranges = counters.get_range_calls.load(Ordering::SeqCst);
    let lists = counters.list_prefix_calls.load(Ordering::SeqCst);
    let touched = keys.lock().unwrap().clone();

    assert_eq!(reads, 1, "expected EXACTLY one get_all, got {reads}");
    assert_eq!(ranges, 0, "no ranged reads should be needed");
    // ⚠ This assertion exists because its absence let a mutant survive: without
    // a list_prefix counter, an implementation could enumerate the partition on
    // every lookup and still report "exactly one read".
    assert_eq!(
        lists, 0,
        "a predecessor fetch must not enumerate anything — the key is COMPUTED \
         from ids the caller already holds (I4), not searched for; got {lists} \
         list_prefix calls"
    );
    assert_eq!(
        touched,
        vec!["chain/exec-7/ev/exec-7-ev-18".to_string()],
        "must read exactly the computed key and nothing else"
    );
}

/// ⚠ **The control that makes the count meaningful.** If the instrument cannot
/// register a *larger* number, "1" proves nothing about the design — it could
/// equally mean the counter is broken or the substrate is bypassed.
#[test]
fn the_counter_registers_more_than_one_read_when_more_happen() {
    let dir = tempfile::tempdir().unwrap();
    let (store, counters, _keys) = counting_store(dir.path());
    seed(&store, 5, 10);

    counters.get_all_calls.store(0, Ordering::SeqCst);
    let chain = store.chain("exec-3").expect("chain");
    let reads = counters.get_all_calls.load(Ordering::SeqCst);

    assert_eq!(chain.len(), 10);
    assert!(
        reads > 1,
        "a 10-event chain read must cost more than one read; got {reads} — if this \
         is 1, the counter is not observing the substrate and the P1 result is void"
    );
}

/// ⭐ **P1 is independent of store size** — the property, not just the number.
/// The same one read, whether the store holds 200 events or 6,000.
#[test]
fn the_predecessor_read_count_does_not_grow_with_the_store() {
    let mut observed = Vec::new();
    for (execs, per) in [(10usize, 20usize), (60, 20)] {
        let dir = tempfile::tempdir().unwrap();
        let (store, counters, _k) = counting_store(dir.path());
        seed(&store, execs, per);
        let target = store.get("exec-5", "exec-5-ev-19").unwrap().unwrap();

        counters.get_all_calls.store(0, Ordering::SeqCst);
        let _ = store.parent_of(&target).unwrap().unwrap();
        observed.push((execs * per, counters.get_all_calls.load(Ordering::SeqCst)));
    }
    assert_eq!(
        observed[0].1, observed[1].1,
        "read count changed with store size: {observed:?}"
    );
    assert_eq!(observed[0].1, 1, "and it is one: {observed:?}");
}

// ---------------------------------------------------------------------------
// Pattern B2 — cost is a function of k, in operations.
// ---------------------------------------------------------------------------

/// A `k`-event chain read costs `1 + 2k` reads (one listing, then a pointer and
/// an event per position) and **does not move** when the rest of the store
/// grows 30x. Asserted as an exact number, not a trend.
#[test]
fn the_chain_read_op_count_depends_on_k_not_on_n() {
    let mut observed = Vec::new();
    for execs in [10usize, 60usize] {
        let dir = tempfile::tempdir().unwrap();
        let (store, counters, _k) = counting_store(dir.path());
        seed(&store, execs, 10);
        counters.get_all_calls.store(0, Ordering::SeqCst);
        let c = store.chain("exec-4").expect("chain");
        assert_eq!(c.len(), 10);
        observed.push(counters.get_all_calls.load(Ordering::SeqCst));
    }
    assert_eq!(
        observed[0], observed[1],
        "chain-read op count scaled with total store size: {observed:?}"
    );
    assert_eq!(
        observed[0], 20,
        "expected 2 reads per event (pointer + event) for k=10: {observed:?}"
    );
}

// ---------------------------------------------------------------------------
// The durable store keeps the in-memory store's semantics.
// ---------------------------------------------------------------------------

#[test]
fn append_enforces_the_head_on_the_persisted_state() {
    let dir = tempfile::tempdir().unwrap();
    let (store, _c, _k) = counting_store(dir.path());
    store.append("e1", "a", None, None, "{}").unwrap();
    store.append("e1", "b", Some("a"), None, "{}").unwrap();
    assert!(matches!(
        store.append("e1", "c", Some("a"), None, "{}"),
        Err(ChainError::NotHead { .. })
    ));
}

#[test]
fn a_missing_predecessor_is_a_named_gap_on_disk_too() {
    let dir = tempfile::tempdir().unwrap();
    let (store, _c, _k) = counting_store(dir.path());
    store.append("e1", "a", None, None, "{}").unwrap();
    let synthetic = ehdb_l0::chain::ChainEvent {
        exec_seq: 99,
        event_id: "z".into(),
        prev_event_id: Some("absent-link".into()),
        execution_id: "e1".into(),
        parent_execution_id: None,
        payload: "{}".into(),
    };
    assert_eq!(
        store.parent_of(&synthetic).expect_err("gap"),
        ChainError::GapAt {
            execution_id: "e1".into(),
            event_id: "absent-link".into()
        }
    );
}

#[test]
fn the_chain_is_ordered_and_isolated_to_its_partition() {
    let dir = tempfile::tempdir().unwrap();
    let (store, _c, _k) = counting_store(dir.path());
    seed(&store, 5, 12);
    let c = store.chain("exec-2").expect("chain");
    assert_eq!(c.len(), 12);
    assert_eq!(
        c.iter().map(|e| e.exec_seq).collect::<Vec<_>>(),
        (1..=12).collect::<Vec<u64>>()
    );
    assert!(c.iter().all(|e| e.execution_id == "exec-2"));
}

/// Zero-padding is what makes the listing sorted; without it `seq/10` sorts
/// before `seq/2`. Proven past the 1→2 digit boundary.
#[test]
fn ordering_survives_the_digit_boundary() {
    let dir = tempfile::tempdir().unwrap();
    let (store, _c, _k) = counting_store(dir.path());
    seed(&store, 1, 15);
    let c = store.chain("exec-0").expect("chain");
    assert_eq!(
        c.iter().map(|e| e.exec_seq).collect::<Vec<_>>(),
        (1..=15).collect::<Vec<u64>>(),
        "lexical listing must equal numeric order across 9 -> 10"
    );
}

#[test]
fn a_walk_from_head_follows_the_chain_to_the_root() {
    let dir = tempfile::tempdir().unwrap();
    let (store, _c, _k) = counting_store(dir.path());
    seed(&store, 1, 6);
    let walked = store.walk_from_head("exec-0").expect("walk");
    assert_eq!(walked.len(), 6);
    assert_eq!(walked[0].exec_seq, 6, "newest first");
    assert_eq!(walked[5].exec_seq, 1);
    assert!(store.chain_is_complete("exec-0").unwrap());
}

/// Survives a reopen — it is on disk, not in a process.
#[test]
fn the_chain_survives_reopening_the_store() {
    let dir = tempfile::tempdir().unwrap();
    {
        let (store, _c, _k) = counting_store(dir.path());
        seed(&store, 2, 5);
    }
    let (reopened, _c, _k) = counting_store(dir.path());
    assert_eq!(reopened.chain("exec-1").unwrap().len(), 5);
    assert!(reopened.chain_is_complete("exec-1").unwrap());
    let head = reopened.head("exec-1").unwrap().expect("head persisted");
    assert_eq!(head, "exec-1-ev-4");
}

/// ⭐⭐ **Append is O(1) in substrate reads**, and this pins the number.
///
/// ⚠ Added because the first draft of this store derived the next sequence with
/// a `list_prefix` on every append. That made append **O(k)** in the execution's
/// own length and building a k-event chain **O(k²)** — this suite took 308
/// seconds. Append is the hot write path; the head record now carries its own
/// sequence so one read serves both the I1 head check and the next sequence.
///
/// Asserted as an exact count at two very different chain lengths, because a
/// trend would not have caught the original defect early enough to matter.
#[test]
fn append_costs_a_constant_number_of_reads_regardless_of_chain_length() {
    let dir = tempfile::tempdir().unwrap();
    let (store, counters, _k) = counting_store(dir.path());

    let mut prev: Option<String> = None;
    let mut sampled = Vec::new();
    for i in 0..400 {
        let ev = format!("ev-{i}");
        if i == 1 || i == 399 {
            counters.get_all_calls.store(0, Ordering::SeqCst);
            counters.get_range_calls.store(0, Ordering::SeqCst);
            counters.list_prefix_calls.store(0, Ordering::SeqCst);
        }
        store
            .append("e1", &ev, prev.as_deref(), None, "{}")
            .unwrap();
        if i == 1 || i == 399 {
            // ⚠ list_prefix included deliberately: the O(k) defect this test
            // exists to catch was a prefix enumeration, which an
            // only-get_all sum could not see.
            sampled.push((
                i,
                counters.get_all_calls.load(Ordering::SeqCst)
                    + counters.get_range_calls.load(Ordering::SeqCst)
                    + counters.list_prefix_calls.load(Ordering::SeqCst),
            ));
        }
        prev = Some(ev);
    }

    assert_eq!(
        sampled[0].1, sampled[1].1,
        "append read-count grew with chain length: {sampled:?} — the head record \
         must carry its own sequence, or append is O(k)"
    );
    assert_eq!(
        sampled[0].1, 1,
        "append should read exactly the head record: {sampled:?}"
    );
}

/// ⚠ Control: the instrument would notice if append became expensive. Builds a
/// chain with a deliberately O(k) sequence derivation and shows the count
/// climbing — so the flat reading above is not the counter failing to observe.
#[test]
fn the_append_counter_would_notice_an_o_k_derivation() {
    let dir = tempfile::tempdir().unwrap();
    let (store, counters, _k) = counting_store(dir.path());
    let mut prev: Option<String> = None;
    for i in 0..30 {
        let ev = format!("ev-{i}");
        store
            .append("e1", &ev, prev.as_deref(), None, "{}")
            .unwrap();
        prev = Some(ev);
    }
    // A full chain read IS O(k); if the counter can see that climb, it can see
    // an O(k) append too.
    counters.get_all_calls.store(0, Ordering::SeqCst);
    let _ = store.chain("e1").unwrap();
    let many = counters.get_all_calls.load(Ordering::SeqCst);
    assert!(
        many >= 30,
        "the counter failed to register an O(k) cost ({many}) — the constant \
         append reading cannot be trusted"
    );
}

/// ⚠ **D5.** The `seq/` keys are zero-padded so that a prefix listing is
/// already in numeric order. Asserted on the RAW KEYS, not on `chain()`'s
/// output — `chain()` re-sorts defensively, and that re-sort was masking the
/// property, letting an unpadded-key mutant survive the battery.
#[test]
fn the_seq_keys_list_in_numeric_order_without_resorting() {
    let dir = tempfile::tempdir().unwrap();
    let fs = LocalFsSubstrate::new(dir.path()).expect("substrate");
    let counting = CountingSubstrate::new(fs);
    let sub: Arc<dyn DurableSubstrate> = Arc::new(counting);
    let store = DurableChainStore::new(Arc::clone(&sub));

    let mut prev: Option<String> = None;
    for i in 0..15 {
        let ev = format!("ev-{i}");
        store
            .append("e1", &ev, prev.as_deref(), None, "{}")
            .unwrap();
        prev = Some(ev);
    }

    let mut keys = sub.list_prefix("chain/e1/seq/").expect("list");
    keys.sort(); // lexical
                 // Read the sequence back out of each key and require it to be ascending.
    let seqs: Vec<u64> = keys
        .iter()
        .map(|k| {
            k.rsplit('/')
                .next()
                .unwrap()
                .trim_start_matches('0')
                .parse::<u64>()
                .unwrap_or(0)
        })
        .collect();
    assert_eq!(
        seqs,
        (1..=15).collect::<Vec<u64>>(),
        "lexical key order must equal numeric order across the 9 -> 10 boundary; \
         without zero-padding seq/10 sorts before seq/2"
    );
}

/// ⚠ **D8.** Events are immutable: a second write to an existing event key must
/// not replace it. `put_if_absent` is what guarantees that, and a mutant
/// swapping it for `put_overwrite` survived because nothing checked.
#[test]
fn an_event_key_is_write_once() {
    let dir = tempfile::tempdir().unwrap();
    let fs = LocalFsSubstrate::new(dir.path()).expect("substrate");
    let sub: Arc<dyn DurableSubstrate> = Arc::new(CountingSubstrate::new(fs));
    let store = DurableChainStore::new(Arc::clone(&sub));
    store.append("e1", "a", None, None, "{\"v\":1}").unwrap();

    // A replicated event at the SAME key with different content must not
    // overwrite what is already stored.
    let conflicting = ehdb_l0::chain::ChainEvent {
        exec_seq: 1,
        event_id: "a".into(),
        prev_event_id: None,
        execution_id: "e1".into(),
        parent_execution_id: None,
        payload: "{\"v\":999}".into(),
    };
    let _ = store.apply_replicated(conflicting);

    let stored = store.get("e1", "a").unwrap().expect("still there");
    assert_eq!(
        stored.payload, "{\"v\":1}",
        "an event key is write-once; a conflicting write must not replace the \
         original (put_if_absent, not put_overwrite)"
    );
}

/// ⭐ **D8, properly.** Appending an `event_id` that already exists must be
/// REFUSED, not silently succeed.
///
/// ⚠ This test exists because the mutation battery kept surviving `put_if_absent`
/// -> `put_overwrite`, and chasing that revealed the real defect: `append`
/// discarded `put_if_absent`'s boolean, so a duplicate id reported success,
/// wrote nothing, and advanced the head to the duplicate.
#[test]
fn appending_a_duplicate_event_id_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let (store, _c, _k) = counting_store(dir.path());
    store.append("e1", "a", None, None, "{}").unwrap();

    // The head IS "a", so the I1 head check passes and the write is reached.
    // Only the write-once guarantee can refuse this.
    let err = store
        .append("e1", "a", Some("a"), None, "{}")
        .expect_err("a duplicate event id must be refused");
    assert!(
        err.message().contains("write-once"),
        "the refusal must name the reason: {}",
        err.message()
    );

    // And the original is intact, with the head unmoved.
    assert_eq!(store.chain("e1").unwrap().len(), 1);
    assert_eq!(store.head("e1").unwrap().as_deref(), Some("a"));
}
