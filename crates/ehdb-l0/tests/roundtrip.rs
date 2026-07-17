//! L0.1 end-to-end proof (RFC §5 exit criteria) — real behavior, not asserted:
//!
//! - **Round-trip:** write events → seal to immutable parts → manifest + sparse
//!   index recorded → a lookup prunes via the manifest, binary-searches the
//!   sparse index, ranged-GETs only the needed block, decodes correctly.
//! - **Hot vs durable split:** an origin node serves recent reads from local
//!   parts with **zero substrate I/O**; a cold-loaded node serves the same
//!   reads entirely from the substrate.
//! - **Zero-I/O pruning:** a lookup for execution E on shard s touches only
//!   shard-s part objects — parts of other shards are fetched **zero** times.
//! - **Only the needed block:** a lookup after a high sequence ranged-GETs fewer
//!   bytes than the whole part (the sparse-index skip).
//! - **Cold-load correctness:** a fresh node with an empty local dir reproduces
//!   the **exact** record set + global sequence of the origin (the fungible-
//!   writer property that retires the per-shard-Raft "T-RF" plan, RFC §2.7).
//! - **Hot-path isolation:** appends do not slow down when the substrate is
//!   slow (the async-durability §2.3 claim).

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use ehdb_l0::substrate::DurableSubstrate;
use ehdb_l0::{
    shard_for_execution, CountingSubstrate, EventRecord, L0Config, L0EventLogEngine,
    LocalFsSubstrate,
};

fn unique_dir(tag: &str) -> std::path::PathBuf {
    static N: AtomicU64 = AtomicU64::new(0);
    let n = N.fetch_add(1, Ordering::Relaxed);
    // PID makes the path unique across separate `cargo test` process runs (so a
    // prior run's leftover substrate is never resumed); the counter makes it
    // unique within a run.
    std::env::temp_dir().join(format!("ehdb-l0-it-{tag}-{}-{n}", std::process::id()))
}

/// A counting substrate handle: the shared store, its counters, its read-key
/// log.
type CountingStore = (
    Arc<dyn DurableSubstrate>,
    Arc<ehdb_l0::substrate::SubstrateCounters>,
    Arc<std::sync::Mutex<Vec<String>>>,
);

/// Build a counting substrate over a shared local dir, returning the Arc, its
/// counters, and its read-key log.
fn counting_store(object_dir: &std::path::Path) -> CountingStore {
    let inner = LocalFsSubstrate::new(object_dir).unwrap();
    let counting = CountingSubstrate::new(inner);
    let counters = counting.counters();
    let read_keys = counting.read_keys();
    (Arc::new(counting), counters, read_keys)
}

/// The workload: `executions` distinct executions, each with `events_each`
/// events, appended round-robin so shards interleave in the global sequence.
fn write_workload(
    engine: &mut L0EventLogEngine,
    executions: &[&str],
    events_each: u64,
) -> Vec<EventRecord> {
    let mut expected = Vec::new();
    for i in 0..events_each {
        for exec in executions {
            let seq = engine
                .append(
                    exec,
                    &format!("txn-{exec}-{i}"),
                    format!("payload-{exec}-{i}"),
                )
                .unwrap();
            expected.push(EventRecord::new(
                seq,
                *exec,
                format!("txn-{exec}-{i}"),
                format!("payload-{exec}-{i}"),
            ));
        }
    }
    expected
}

#[test]
fn round_trip_prune_sparse_ranged_get_and_cold_load() {
    let object_dir = unique_dir("obj");
    let origin_local = unique_dir("origin");
    let cold_local = unique_dir("cold");

    // 4 shards, small seal + granule sizes so we get multiple parts per shard
    // and multiple granules per part (exercises pruning + the sparse index).
    let make_config = |root: &std::path::Path| {
        L0Config::d1(root)
            .with_shard_count(4)
            .with_granule_size(4)
            .with_seal_max_records(8)
    };

    // Pick executions that land on several distinct shards.
    let executions = [
        "1001", "1002", "1003", "2007", "3011", "4013", "5019", "6023",
    ];
    let mut shards_used = executions
        .iter()
        .map(|e| shard_for_execution(e, 4))
        .collect::<Vec<_>>();
    shards_used.sort();
    shards_used.dedup();
    assert!(
        shards_used.len() >= 2,
        "test needs executions spread across >=2 shards, got {shards_used:?}"
    );

    // --- origin: write, prove a hot read, then flush to durable ---
    let (origin_store, origin_counters, _origin_reads) = counting_store(&object_dir);
    let mut origin = L0EventLogEngine::open(make_config(&origin_local), origin_store).unwrap();
    let mut expected = write_workload(&mut origin, &executions, 10); // 80 events
    expected.sort_by_key(|r| r.global_sequence);
    let origin_tip = origin.global_sequence();
    assert_eq!(origin_tip, expected.len() as u64);

    // A hot read on the origin (local parts resident) touches the substrate
    // ZERO times for range reads.
    let range_reads_before = origin_counters.get_range_calls.load(Ordering::Relaxed);
    let hot = origin.read_execution_after("1001", 0).unwrap();
    let range_reads_after = origin_counters.get_range_calls.load(Ordering::Relaxed);
    assert_eq!(
        range_reads_before, range_reads_after,
        "hot-path read must not issue any substrate range GETs"
    );
    let expected_1001: Vec<_> = expected
        .iter()
        .filter(|r| r.execution_id == "1001")
        .cloned()
        .collect();
    assert_eq!(
        hot, expected_1001,
        "hot read must return exactly execution 1001's events"
    );

    // Flush all sealed parts to the durable substrate.
    origin.flush_and_wait_uploads().unwrap();
    let m = origin.metrics().snapshot();
    assert!(
        m.seals >= 1 && m.uploads == m.seals,
        "every sealed part uploaded: {m:?}"
    );
    assert!(m.upload_bytes > 0);

    // --- cold node: fresh, empty local dir, only the substrate ---
    let (cold_store, _cold_counters, cold_read_keys) = counting_store(&object_dir);
    let cold = L0EventLogEngine::cold_load(make_config(&cold_local), cold_store).unwrap();

    // Cold-load correctness: exact record set + same global sequence tip.
    assert_eq!(
        cold.global_sequence(),
        origin_tip,
        "cold node resumes the same tip"
    );
    let cold_all = cold.replay_all().unwrap();
    assert_eq!(
        cold_all, expected,
        "cold node reproduces the exact record set"
    );

    // Cross-check the origin's full replay equals the cold node's.
    let origin_all = origin.replay_all().unwrap();
    assert_eq!(origin_all, expected);
    assert_eq!(origin_all, cold_all);

    // --- zero-I/O pruning: a cold read for exec E only touches shard_for(E) parts ---
    let target = "3011";
    let target_shard = shard_for_execution(target, 4);
    cold_read_keys.lock().unwrap().clear();
    let cold_hits = cold.read_execution_after(target, 0).unwrap();
    let expected_target: Vec<_> = expected
        .iter()
        .filter(|r| r.execution_id == target)
        .cloned()
        .collect();
    assert_eq!(
        cold_hits, expected_target,
        "cold read returns exactly {target}'s events"
    );
    assert!(!cold_hits.is_empty());

    let touched = cold_read_keys.lock().unwrap().clone();
    assert!(
        !touched.is_empty(),
        "cold read must fetch from the substrate"
    );
    let want_prefix = format!("shard-{target_shard}/");
    for key in &touched {
        assert!(
            key.contains(&want_prefix),
            "cold read for shard {target_shard} touched a non-matching part: {key}"
        );
    }
    // And the manifest actually held parts of OTHER shards (so pruning was
    // meaningful, not vacuous).
    let manifest = cold.manifest_snapshot();
    let other_shard_parts = manifest
        .parts
        .iter()
        .filter(|p| p.partition != target_shard)
        .count();
    assert!(
        other_shard_parts > 0,
        "expected parts in other shards for the prune to skip"
    );
    let read_metrics = cold.metrics().snapshot();
    assert!(
        read_metrics.parts_pruned > 0,
        "the prune must have skipped at least one part with zero I/O: {read_metrics:?}"
    );

    // (The "ranged GET fetches only the needed block" sparse-index proof is a
    // separate focused test with deterministic single-shard sizing:
    // `sparse_index_ranged_get_fetches_only_needed_block`.)

    // cleanup
    drop(cold);
    drop(origin);
    let _ = std::fs::remove_dir_all(&object_dir);
    let _ = std::fs::remove_dir_all(&origin_local);
    let _ = std::fs::remove_dir_all(&cold_local);
}

#[test]
fn hot_path_not_blocked_by_slow_substrate() {
    // Baseline: fast substrate.
    let base_obj = unique_dir("hp-base-obj");
    let base_local = unique_dir("hp-base-local");
    let base_store: Arc<dyn DurableSubstrate> = Arc::new(CountingSubstrate::new(
        LocalFsSubstrate::new(&base_obj).unwrap(),
    ));
    let cfg = |root: &std::path::Path| {
        L0Config::d1(root)
            .with_shard_count(1)
            .with_granule_size(4)
            .with_seal_max_records(8)
    };
    let mut base = L0EventLogEngine::open(cfg(&base_local), base_store).unwrap();
    let n = 200u64;
    let t0 = Instant::now();
    for i in 0..n {
        base.append("1001", &format!("t{i}"), format!("p{i}"))
            .unwrap();
    }
    let t_base = t0.elapsed();
    let base_uploads_expected = base.metrics().snapshot().seals;
    drop(base); // joins uploader (fast)

    // Slow substrate: 40ms injected latency per upload.
    let latency = Duration::from_millis(40);
    let slow_obj = unique_dir("hp-slow-obj");
    let slow_local = unique_dir("hp-slow-local");
    let slow_counting =
        CountingSubstrate::with_put_latency(LocalFsSubstrate::new(&slow_obj).unwrap(), latency);
    let slow_store: Arc<dyn DurableSubstrate> = Arc::new(slow_counting);
    let mut slow = L0EventLogEngine::open(cfg(&slow_local), slow_store).unwrap();
    let t1 = Instant::now();
    for i in 0..n {
        slow.append("1001", &format!("t{i}"), format!("p{i}"))
            .unwrap();
    }
    let t_slow = t1.elapsed();

    // If uploads blocked the append path, t_slow would exceed t_base by
    // ~(uploads * latency). Assert the append loop is NOT serialized behind the
    // slow uploads — the extra time is far below that serialized cost.
    let serialized_cost = Duration::from_millis(base_uploads_expected * latency.as_millis() as u64);
    assert!(
        base_uploads_expected >= 10,
        "expected many seals to make the test meaningful, got {base_uploads_expected}"
    );
    let extra = t_slow.saturating_sub(t_base);
    assert!(
        extra < serialized_cost / 2,
        "append hot path appears blocked by slow uploads: base={t_base:?} slow={t_slow:?} \
         extra={extra:?} serialized_cost={serialized_cost:?}"
    );

    // The uploads still land (durability preserved) once drained.
    slow.flush_and_wait_uploads().unwrap();
    let m = slow.metrics().snapshot();
    assert_eq!(m.uploads, m.seals, "all sealed parts eventually uploaded");
    assert!(m.mean_upload_lag_micros() > 0);

    drop(slow);
    let _ = std::fs::remove_dir_all(&base_obj);
    let _ = std::fs::remove_dir_all(&base_local);
    let _ = std::fs::remove_dir_all(&slow_obj);
    let _ = std::fs::remove_dir_all(&slow_local);
}

#[test]
fn owner_restart_resumes_catalog_from_substrate() {
    // An owner that restarts (same local dir gone, substrate intact) resumes
    // its catalog + global sequence from the durable manifest — the same
    // mechanism cold-load uses, but for the original writer.
    let object_dir = unique_dir("restart-obj");
    let local_a = unique_dir("restart-a");
    let cfg = |root: &std::path::Path| {
        L0Config::d1(root)
            .with_seal_max_records(8)
            .with_granule_size(4)
    };

    let (store1, _c1, _k1) = counting_store(&object_dir);
    let mut a = L0EventLogEngine::open(cfg(&local_a), store1).unwrap();
    for i in 0..20u64 {
        a.append("1001", &format!("t{i}"), format!("p{i}")).unwrap();
    }
    a.flush_and_wait_uploads().unwrap();
    let tip = a.global_sequence();
    let all_a = a.replay_all().unwrap();
    drop(a);

    // Restart as a fresh engine over the same substrate (new empty local dir).
    let local_b = unique_dir("restart-b");
    let (store2, _c2, _k2) = counting_store(&object_dir);
    let b = L0EventLogEngine::open(cfg(&local_b), store2).unwrap();
    assert_eq!(b.global_sequence(), tip, "restart resumes the same tip");
    assert_eq!(
        b.replay_all().unwrap(),
        all_a,
        "restart reproduces the record set"
    );

    drop(b);
    let _ = std::fs::remove_dir_all(&object_dir);
    let _ = std::fs::remove_dir_all(&local_a);
    let _ = std::fs::remove_dir_all(&local_b);
}

#[test]
fn sparse_index_ranged_get_fetches_only_needed_block() {
    // Deterministic single-shard sizing so parts are full and part boundaries
    // are predictable: 1 shard, 1 execution, seal at 8 records, granule 4 →
    // each full part is 8 records / 2 granules. 24 events → exactly 3 full parts.
    let object_dir = unique_dir("blk-obj");
    let origin_local = unique_dir("blk-origin");
    let cold_local = unique_dir("blk-cold");
    let cfg = |root: &std::path::Path| {
        L0Config::d1(root)
            .with_shard_count(1)
            .with_granule_size(4)
            .with_seal_max_records(8)
    };

    let (origin_store, _oc, _ok) = counting_store(&object_dir);
    let mut origin = L0EventLogEngine::open(cfg(&origin_local), origin_store).unwrap();
    for i in 0..24u64 {
        origin
            .append("1001", &format!("t{i}"), format!("payload-number-{i}"))
            .unwrap();
    }
    origin.flush_and_wait_uploads().unwrap();

    // Cold-load and inspect the manifest: 3 full parts, each with 2 granule marks.
    let (cold_store, cold_counters, _ck) = counting_store(&object_dir);
    let cold = L0EventLogEngine::cold_load(cfg(&cold_local), cold_store).unwrap();
    let manifest = cold.manifest_snapshot();
    assert_eq!(manifest.parts.len(), 3, "24 events / 8 per part = 3 parts");
    for p in &manifest.parts {
        assert_eq!(p.record_count, 8);
        assert_eq!(
            p.sparse_index.marks.len(),
            2,
            "8 records / granule 4 = 2 marks"
        );
    }

    // The LAST part: read after its last granule's first sequence - 1. Every
    // earlier part prunes (range wholly below the cursor); only this part is
    // read, from its last mark to the end — strictly fewer bytes than the whole
    // part.
    let last_part = manifest
        .parts
        .iter()
        .max_by_key(|p| p.min_sequence)
        .unwrap();
    let last_mark = last_part.sparse_index.marks.last().unwrap();
    assert!(
        last_mark.byte_offset > 0,
        "last granule is not at the part start"
    );
    let after = last_mark.first_sequence - 1;

    let before = cold_counters.get_range_bytes.load(Ordering::Relaxed);
    let hits = cold.read_execution_after("1001", after).unwrap();
    let block_bytes = cold_counters.get_range_bytes.load(Ordering::Relaxed) - before;

    // Correctness: the returned events are exactly those after the cursor.
    assert!(!hits.is_empty());
    assert!(hits.iter().all(|r| r.global_sequence > after));
    assert_eq!(
        hits.iter().map(|r| r.global_sequence).min().unwrap(),
        after + 1
    );

    // The sparse-index skip: fetched only the last granule's block, NOT the
    // whole part, and NOT any earlier part.
    assert_eq!(
        block_bytes,
        last_part.byte_size - last_mark.byte_offset,
        "ranged GET fetched exactly [last_mark, end) of the last part"
    );
    assert!(
        block_bytes < last_part.byte_size,
        "ranged GET ({block_bytes}) fetched fewer bytes than the whole part ({})",
        last_part.byte_size
    );
    // Total dataset bytes are much larger than this one block — the skip is real.
    let total_bytes: u64 = manifest.parts.iter().map(|p| p.byte_size).sum();
    assert!(
        block_bytes * 3 < total_bytes,
        "block {block_bytes} vs total {total_bytes}"
    );

    drop(cold);
    drop(origin);
    let _ = std::fs::remove_dir_all(&object_dir);
    let _ = std::fs::remove_dir_all(&origin_local);
    let _ = std::fs::remove_dir_all(&cold_local);
}
