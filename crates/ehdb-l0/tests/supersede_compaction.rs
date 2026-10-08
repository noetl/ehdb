//! **Key-level compaction** (noetl/ehdb#391, noetl/ai-meta#455).
//!
//! # The defect this closes
//!
//! Measured in `benches/vector_query.rs`: query cost tracks **op-log depth, not live
//! points**. 500 points re-embedded 10x (5,000 ops, 500 live) cost **64.9 ms** — within 2%
//! of 5,000 points written once. So re-embedding was as expensive as growing.
//!
//! Compaction was the obvious remedy and it did not work: a merge made the 10x case **9%
//! worse**. The reason is structural rather than a bug —
//!
//! - `VectorStore::live_points` reads **every op ever written** then folds latest-wins;
//! - the `Dataset` trait exposed **no supersede hook**, only `dedupe_key`, which is an
//!   *append-time idempotency window* and says nothing about one record superseding another;
//! - `merge_once` combines **parts**, not keys.
//!
//! So merge had no way to know that a later `VectorOp` for the same `point_id` makes an
//! earlier one dead weight.
//!
//! # Why dropping records during a merge is safe
//!
//! A latest-wins fold answers, for each key, *the record with the maximum sort key*. A
//! compacting merge keeps, for each key, the maximum-sort-key record **among its own
//! sources**. Every record it drops therefore had a lower sort key than a record that is
//! kept, for the same key — so the global per-key maximum is unchanged, and the fold's
//! answer cannot move. This holds whether or not the merged span is contiguous (it is), and
//! regardless of what other parts hold.
//!
//! ⚠⚠ **It is only safe for a dataset where nothing reads history.** That is the whole
//! reason the hook is opt-in, and it is not a theoretical caveat: `RuntimeDataset` (D8)
//! must **not** opt in, because `RuntimeStore::watch_since` — added in the P2 registry work
//! — returns the **op log** after a cursor. Compacting D8 would silently delete the history
//! a watcher resumes from. Each dataset's eligibility is a semantic claim about its
//! readers, not a performance choice.
//!
//! # Tombstones
//!
//! A tombstone is the maximum-sort-key record for its key, so the rule above keeps it and a
//! delete cannot be resurrected. Tombstones therefore accumulate until retention drops
//! their part — deliberately, because dropping one early is data corruption rather than a
//! missed optimisation.

use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use std::sync::Arc;

use ehdb_l0::substrate::DurableSubstrate;
use ehdb_l0::{
    shard_for_execution, Dataset, L0Config, L0Engine, LocalFsSubstrate, MergePolicy, VectorStore,
};
use serde::{Deserialize, Serialize};

fn dir(tag: &str) -> PathBuf {
    static N: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let n = N.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    std::env::temp_dir().join(format!("ehdb-sup-{tag}-{}-{n}", std::process::id()))
}

const SEAL: u64 = 128;

fn vector_store(tag: &str) -> (VectorStore, PathBuf) {
    let root = dir(tag);
    let hot = root.join("hot");
    let obj = root.join("obj");
    std::fs::create_dir_all(&hot).unwrap();
    std::fs::create_dir_all(&obj).unwrap();
    let s: Arc<dyn DurableSubstrate> = Arc::new(LocalFsSubstrate::new(&obj).unwrap());
    let cfg = VectorStore::config(&hot)
        .with_shard_count(1)
        .with_seal_max_records(SEAL)
        // ⚠ `small_part_max_records` must sit at or above the expected live-set size, or
        // a compacted output exceeds the "small" bound, is never re-merged under the
        // one-level tiering, and compaction stalls after one pass per run. This is the
        // policy a compacting dataset wants, and it is stated here because the default
        // `d1(seal_max_records)` is NOT it.
        .with_merge_policy(MergePolicy {
            small_part_max_records: SEAL * 32,
            trigger_run_len: 4,
            max_merge_parts: 8,
        });
    (VectorStore::open(cfg, s).unwrap(), root)
}

fn emb(seed: usize, round: usize) -> Vec<f32> {
    (0..8)
        .map(|i| ((i * 13 + seed * 7 + round * 101) % 97) as f32 / 97.0)
        .collect()
}

/// Records physically stored, from the engine's own manifest.
fn stored_records(st: &VectorStore) -> u64 {
    st.engine()
        .manifest_snapshot()
        .parts
        .iter()
        .map(|p| p.record_count)
        .sum()
}

fn parts(st: &VectorStore) -> usize {
    st.engine().manifest_snapshot().parts.len()
}

/// Drive merges to a fixed point, so the measurement is of a settled store rather than
/// of however many merges one call happened to perform.
fn drain_merges(st: &mut VectorStore) -> usize {
    let mut total = 0;
    for _ in 0..64 {
        st.flush_and_wait().unwrap();
        let n = st.run_pending_merges().unwrap();
        total += n;
        if n == 0 {
            break;
        }
    }
    st.flush_and_wait().unwrap();
    total
}

fn live_set(st: &VectorStore, collection: &str) -> BTreeMap<String, Vec<f32>> {
    st.top_k(collection, &emb(0, 0), usize::MAX / 2)
        .unwrap()
        .into_iter()
        .map(|h| (h.point_id, h.embedding))
        .collect()
}

// ---------------------------------------------------------------------------
// 1. The headline: a compacting merge collapses superseded versions.
// ---------------------------------------------------------------------------

#[test]
fn merge_collapses_superseded_versions_for_an_opted_in_dataset() {
    const LIVE: usize = 300;
    const ROUNDS: usize = 8;
    let (mut st, root) = vector_store("collapse");
    let c = "playbook";

    for r in 0..ROUNDS {
        for i in 0..LIVE {
            st.upsert(c, &format!("p{i}"), emb(i, r)).unwrap();
        }
    }
    let ops = (LIVE * ROUNDS) as u64;
    st.flush_and_wait().unwrap();
    let before_records = stored_records(&st);
    let before_parts = parts(&st);
    let before_live = live_set(&st, c);

    let before_superseded = st.engine().metrics().snapshot().records_superseded;
    let merges = drain_merges(&mut st);
    let after_superseded = st.engine().metrics().snapshot().records_superseded;
    let after_records = stored_records(&st);
    let after_parts = parts(&st);
    let after_live = live_set(&st, c);

    println!(
        "\nops={ops} live={LIVE}\n  before: {before_records} records in {before_parts} parts\n  \
         after:  {after_records} records in {after_parts} parts  ({merges} merges)\n  \
         compaction ratio: {:.2}x",
        before_records as f64 / after_records.max(1) as f64
    );

    // The answer must not move — set equality on the whole live set, not a count.
    assert_eq!(
        before_live, after_live,
        "compaction changed the latest-wins answer: that is data loss, not optimisation"
    );
    assert_eq!(
        after_live.len(),
        LIVE,
        "the live set must still be exactly {LIVE} points"
    );

    // And the stored footprint must actually fall toward the live count.
    assert!(
        after_records < before_records,
        "compaction stored {after_records} records vs {before_records} before — a merge \
         that cannot collapse superseded keys is the #391 defect"
    );
    assert!(
        after_records <= (LIVE as u64) * 2,
        "after draining {merges} merges the store holds {after_records} records for {LIVE} \
         live points ({ops} ops written). Compaction is not converging — with one-level \
         tiering a compacted output stays 'small' only while its record_count is at or \
         below the policy's small_part_max_records, so if it exceeds that it is never \
         re-merged."
    );

    // The counter must MOVE, and must equal what was actually dropped. A metric that
    // is pinned at 0 and never proven to move is indistinguishable from a store with
    // nothing to collapse — which is exactly the state this feature exists to fix.
    let dropped = after_superseded - before_superseded;
    println!("  records_superseded: {before_superseded} -> {after_superseded}");
    assert!(
        dropped > 0,
        "records_superseded did not move ({before_superseded} -> {after_superseded}) \
         while {} records were collapsed — the counter is inert",
        before_records - after_records
    );
    assert_eq!(
        dropped,
        before_records - after_records,
        "the counter must equal the records actually dropped, or it is measuring \
         something other than compaction"
    );

    let _ = std::fs::remove_dir_all(&root);
}

// ---------------------------------------------------------------------------
// 2. THE CONTROL: a dataset that does not opt in must keep every version.
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct Plain {
    seq: u64,
    key: String,
}

/// Deliberately does NOT override `supersede_key`.
struct PlainDataset;
impl Dataset for PlainDataset {
    type Record = Plain;
    const NAME: &'static str = "supersede_control_d";
    fn sort_key(r: &Plain) -> u64 {
        r.seq
    }
    fn partition(r: &Plain, n: u32) -> u32 {
        shard_for_execution(&r.key, n)
    }
    fn index_key(r: &Plain) -> &str {
        &r.key
    }
    fn read_partition(k: &str, n: u32) -> u32 {
        shard_for_execution(k, n)
    }
}

#[test]
fn a_dataset_that_does_not_opt_in_keeps_every_version() {
    // The planted-defect control for the whole feature: if compaction ever applied by
    // default, or leaked across datasets, this fails. Without it, "compaction works" and
    // "compaction silently rewrites every dataset's history" look identical.
    let root = dir("control");
    let hot = root.join("hot");
    let obj = root.join("obj");
    std::fs::create_dir_all(&hot).unwrap();
    std::fs::create_dir_all(&obj).unwrap();
    let s: Arc<dyn DurableSubstrate> = Arc::new(LocalFsSubstrate::new(&obj).unwrap());
    let cfg = L0Config::for_dataset(PlainDataset::NAME, &hot)
        .with_shard_count(1)
        .with_seal_max_records(SEAL)
        .with_merge_policy(MergePolicy::d1(SEAL));
    let mut e = L0Engine::<PlainDataset>::open(cfg, s).unwrap();

    const KEYS: u64 = 100;
    const ROUNDS: u64 = 8;
    let mut seq = 0u64;
    for _ in 0..ROUNDS {
        for k in 0..KEYS {
            seq += 1;
            e.append_record(Plain {
                seq,
                key: format!("k{k}"),
            })
            .unwrap();
        }
    }
    let total = KEYS * ROUNDS;
    e.flush_and_wait_uploads().unwrap();
    let before: u64 = e
        .manifest_snapshot()
        .parts
        .iter()
        .map(|p| p.record_count)
        .sum();

    let mut merges = 0;
    for _ in 0..64 {
        e.flush_and_wait_uploads().unwrap();
        let n = e.run_pending_merges().unwrap();
        merges += n;
        if n == 0 {
            break;
        }
    }
    e.flush_and_wait_uploads().unwrap();
    let after: u64 = e
        .manifest_snapshot()
        .parts
        .iter()
        .map(|p| p.record_count)
        .sum();

    println!(
        "\ncontrol (no supersede_key): {total} appends -> {before} records before, \
         {after} after {merges} merges"
    );
    assert_eq!(
        after,
        total,
        "a dataset that does not opt in MUST keep every version; compaction collapsed \
         {} records it had no permission to touch",
        total - after
    );
    assert_eq!(
        e.metrics().snapshot().records_superseded,
        0,
        "records_superseded moved on a dataset that declares no supersede_key"
    );
    // And every record must still be replayable, in order.
    let all = e.replay_all().unwrap();
    assert_eq!(
        all.len() as u64,
        total,
        "replay must still see every record"
    );
    let mut prev = 0;
    for r in &all {
        assert!(r.seq > prev, "replay not ascending at {}", r.seq);
        prev = r.seq;
    }
    let _ = std::fs::remove_dir_all(&root);
}

// ---------------------------------------------------------------------------
// 3. A tombstone must survive, or a delete is resurrected.
// ---------------------------------------------------------------------------

#[test]
fn a_tombstone_survives_compaction_so_a_delete_is_not_resurrected() {
    let (mut st, root) = vector_store("tomb");
    let c = "mcp";
    // Enough rewrites that the deleted point has plenty of superseded versions
    // for compaction to collapse around.
    for r in 0..6 {
        for i in 0..80 {
            st.upsert(c, &format!("p{i}"), emb(i, r)).unwrap();
        }
    }
    st.delete(c, "p7").unwrap();
    st.flush_and_wait().unwrap();
    assert!(
        !live_set(&st, c).contains_key("p7"),
        "precondition: the delete must take effect before compaction"
    );

    let merges = drain_merges(&mut st);
    let after = live_set(&st, c);
    println!("\ntombstone: {merges} merges, live={} after", after.len());
    assert!(
        !after.contains_key("p7"),
        "p7 was deleted and came back after compaction — dropping a tombstone is data \
         corruption, not an optimisation"
    );
    assert_eq!(
        after.len(),
        79,
        "the other 79 must remain; got {}",
        after.len()
    );

    // And it must still be absent after a COLD reopen, which is where a dropped
    // tombstone would show up if the in-RAM fold happened to mask it.
    let hot = root.join("hot");
    let obj = root.join("obj");
    drop(st);
    let s: Arc<dyn DurableSubstrate> = Arc::new(LocalFsSubstrate::new(&obj).unwrap());
    let cold = VectorStore::open(
        VectorStore::config(&hot)
            .with_shard_count(1)
            .with_seal_max_records(SEAL),
        s,
    )
    .unwrap();
    let cold_live = live_set(&cold, c);
    assert!(
        !cold_live.contains_key("p7"),
        "p7 reappeared on a COLD reopen after compaction"
    );
    assert_eq!(
        cold_live.len(),
        79,
        "cold reopen must see the same 79 points"
    );
    println!("cold reopen agrees: {} live, p7 absent", cold_live.len());
    let _ = std::fs::remove_dir_all(&root);
}

// ---------------------------------------------------------------------------
// 4. Ordering and fold determinism must still hold (the invariant work).
// ---------------------------------------------------------------------------

#[test]
fn compaction_preserves_ordering_and_fold_determinism() {
    let (mut st, root) = vector_store("invariants");
    let c = "playbook";
    for r in 0..6 {
        for i in 0..100 {
            st.upsert(c, &format!("p{i}"), emb(i, r)).unwrap();
        }
    }
    st.flush_and_wait().unwrap();

    // A digest of the folded live set — order-insensitive by construction (BTreeMap),
    // so it tests the FOLD's answer, not the iteration order of one read.
    let digest = |st: &VectorStore| -> String {
        let m = live_set(st, c);
        let mut s = String::new();
        for (k, v) in &m {
            s.push_str(k);
            for x in v {
                s.push_str(&format!(":{x:.6}"));
            }
            s.push(';');
        }
        format!("{}:{}", m.len(), s.len())
    };

    let before = digest(&st);
    // Five reads before compaction must agree — otherwise the fold is already
    // non-deterministic and nothing below is attributable to compaction.
    let pre: BTreeSet<String> = (0..5).map(|_| digest(&st)).collect();
    assert_eq!(
        pre.len(),
        1,
        "the fold is non-deterministic BEFORE compaction, so this test cannot attribute \
         anything to compaction: {pre:?}"
    );

    let merges = drain_merges(&mut st);
    let post: BTreeSet<String> = (0..5).map(|_| digest(&st)).collect();
    assert_eq!(
        post.len(),
        1,
        "the fold became non-deterministic AFTER compaction: {post:?}"
    );
    assert_eq!(
        post.iter().next().unwrap(),
        &before,
        "compaction changed the fold's answer"
    );

    // Raw replay of the compacted log must still be strictly ascending in sort key.
    let ops = st.engine().replay_all().unwrap();
    let mut prev = 0u64;
    for op in &ops {
        let k = ehdb_l0::VectorDataset::sort_key(op);
        assert!(
            k > prev,
            "compacted replay is not ascending at {k} (prev {prev})"
        );
        prev = k;
    }
    println!(
        "\ninvariants: {merges} merges, {} ops remain, replay ascending, \
         fold digest stable across 5 reads before and after",
        ops.len()
    );
    let _ = std::fs::remove_dir_all(&root);
}

/// A compacted store must cold-reopen to the same live set — by set equality, with a
/// one-element drop proven to fail the check.
#[test]
fn a_compacted_store_cold_reopens_by_set_equality() {
    let (mut st, root) = vector_store("cold");
    let c = "playbook";
    for r in 0..5 {
        for i in 0..120 {
            st.upsert(c, &format!("p{i}"), emb(i, r)).unwrap();
        }
    }
    drain_merges(&mut st);
    let hot_live = live_set(&st, c);
    let hot = root.join("hot");
    let obj = root.join("obj");
    drop(st);

    let s: Arc<dyn DurableSubstrate> = Arc::new(LocalFsSubstrate::new(&obj).unwrap());
    let cold = VectorStore::open(
        VectorStore::config(&hot)
            .with_shard_count(1)
            .with_seal_max_records(SEAL),
        s,
    )
    .unwrap();
    let cold_live = live_set(&cold, c);
    assert_eq!(
        hot_live.keys().collect::<Vec<_>>(),
        cold_live.keys().collect::<Vec<_>>(),
        "cold reopen of a compacted store lost or gained points"
    );
    assert_eq!(hot_live, cold_live, "cold reopen changed an embedding");

    // The control: a one-element drop must fail the same comparison, so a
    // vacuous equality cannot pass for a real one.
    let mut damaged = cold_live.clone();
    let victim = damaged.keys().next().unwrap().clone();
    damaged.remove(&victim);
    assert_ne!(
        hot_live, damaged,
        "the set-equality check cannot detect a one-element drop, so its pass above is \
         not evidence"
    );
    println!(
        "\ncold reopen: {} points set-equal; a one-element drop is detected",
        cold_live.len()
    );
    let _ = std::fs::remove_dir_all(&root);
}
