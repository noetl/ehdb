//! L0.1 selfcheck — runs the hot-local/durable-async composite for D1 over a
//! local-filesystem substrate and prints the real proof numbers (append
//! latency hot vs slow-store, upload lag, prune counts, ranged-block bytes vs
//! whole-dataset bytes, cold-load equality). This is the runnable companion to
//! the `tests/roundtrip.rs` assertions — same behavior, human-readable output.
//!
//! Run: `cargo run -p ehdb-l0 --example l0_selfcheck`

use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::{Duration, Instant};

use ehdb_l0::substrate::{CountingSubstrate, DurableSubstrate};
use ehdb_l0::{shard_for_execution, L0Config, L0EventLogEngine, LocalFsSubstrate};

fn tmp(tag: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!("ehdb-l0-selfcheck-{}-{tag}", std::process::id()))
}

fn main() {
    let object_dir = tmp("obj");
    let origin_local = tmp("origin");
    let cold_local = tmp("cold");
    let _ = std::fs::remove_dir_all(&object_dir);
    let _ = std::fs::remove_dir_all(&origin_local);
    let _ = std::fs::remove_dir_all(&cold_local);

    let cfg = |root: &std::path::Path| {
        L0Config::d1(root)
            .with_shard_count(4)
            .with_granule_size(4)
            .with_seal_max_records(8)
    };

    println!("== EHDB L0.1 selfcheck (D1 event log, hot-local/durable-async) ==\n");

    // --- origin: write across 4 shards ---
    let inner = LocalFsSubstrate::new(&object_dir).unwrap();
    let counting = CountingSubstrate::new(inner);
    let origin_counters = counting.counters();
    let origin_store: Arc<dyn DurableSubstrate> = Arc::new(counting);
    let mut origin = L0EventLogEngine::open(cfg(&origin_local), origin_store).unwrap();

    let executions = [
        "1001", "1002", "1003", "2007", "3011", "4013", "5019", "6023",
    ];
    let events_each = 25u64;
    for i in 0..events_each {
        for e in &executions {
            origin
                .append(e, &format!("txn-{e}-{i}"), format!("payload-{e}-{i}"))
                .unwrap();
        }
    }
    let tip = origin.global_sequence();
    println!(
        "wrote {} events across {} executions / {} shards; global_sequence tip = {tip}",
        tip,
        executions.len(),
        4
    );

    // Hot read (local parts) — zero substrate range GETs.
    let before = origin_counters.get_range_calls.load(Ordering::Relaxed);
    let hot = origin.read_execution_after("1001", 0).unwrap();
    let after = origin_counters.get_range_calls.load(Ordering::Relaxed);
    println!(
        "hot read exec 1001: {} events, substrate range GETs during read = {} (expect 0)",
        hot.len(),
        after - before
    );

    origin.flush_and_wait_uploads().unwrap();
    let m = origin.metrics().snapshot();
    println!(
        "flushed: seals={} uploads={} upload_bytes={} mean_upload_lag={}us",
        m.seals,
        m.uploads,
        m.upload_bytes,
        m.mean_upload_lag_micros()
    );

    // --- cold node: reconstruct from the substrate only ---
    let cold_inner = LocalFsSubstrate::new(&object_dir).unwrap();
    let cold_counting = CountingSubstrate::new(cold_inner);
    let cold_counters = cold_counting.counters();
    let cold_read_keys = cold_counting.read_keys();
    let cold_store: Arc<dyn DurableSubstrate> = Arc::new(cold_counting);
    let cold = L0EventLogEngine::cold_load(cfg(&cold_local), cold_store).unwrap();

    let origin_all = origin.replay_all().unwrap();
    let cold_all = cold.replay_all().unwrap();
    println!(
        "\ncold-load: tip={} (origin {tip}), records={} (origin {}), byte-identical={}",
        cold.global_sequence(),
        cold_all.len(),
        origin_all.len(),
        cold_all == origin_all && cold.global_sequence() == tip
    );

    // Zero-I/O pruning: a cold read for one exec touches only its shard's parts.
    let target = "3011";
    let target_shard = shard_for_execution(target, 4);
    cold_read_keys.lock().unwrap().clear();
    let hits = cold.read_execution_after(target, 0).unwrap();
    let touched = cold_read_keys.lock().unwrap().clone();
    let read_metrics = cold.metrics().snapshot();
    let only_target_shard = touched
        .iter()
        .all(|k| k.contains(&format!("shard-{target_shard}/")));
    println!(
        "\ncold read exec {target} (shard {target_shard}): {} events; parts touched={}, \
         parts_pruned (zero I/O)={} (of which bloom-pruned={}), all-touched-are-target-shard={}",
        hits.len(),
        touched.len(),
        read_metrics.parts_pruned,
        read_metrics.parts_bloom_pruned,
        only_target_shard
    );

    // L0.2 index-first pruning in a SINGLE partition (worst case for the
    // partition/MinMax prune) — the bloom is the only mechanism.
    {
        let ii_obj = tmp("ii-obj");
        let ii_local = tmp("ii-local");
        let ii_cold = tmp("ii-cold");
        for d in [&ii_obj, &ii_local, &ii_cold] {
            let _ = std::fs::remove_dir_all(d);
        }
        let one_shard = |root: &std::path::Path| {
            L0Config::d1(root)
                .with_shard_count(1)
                .with_granule_size(4)
                .with_seal_max_records(8)
        };
        let store: Arc<dyn DurableSubstrate> = Arc::new(CountingSubstrate::new(
            LocalFsSubstrate::new(&ii_obj).unwrap(),
        ));
        let mut w = L0EventLogEngine::open(one_shard(&ii_local), store).unwrap();
        for e in 0..6 {
            for i in 0..16u64 {
                w.append(&format!("e{e}"), &format!("t{i}"), format!("p{i}"))
                    .unwrap();
            }
        }
        w.flush_and_wait_uploads().unwrap();
        let cold_store: Arc<dyn DurableSubstrate> = Arc::new(CountingSubstrate::new(
            LocalFsSubstrate::new(&ii_obj).unwrap(),
        ));
        let cold = L0EventLogEngine::cold_load(one_shard(&ii_cold), cold_store).unwrap();
        let parts = cold.manifest_snapshot().parts.len();
        let got = cold.read_execution_after("e3", 0).unwrap();
        let s = cold.metrics().snapshot();
        println!(
            "\nL0.2 single-partition read exec e3: {} events (correct); {} parts total, \
             bloom-pruned={} parts, scanned={} parts (partition prune powerless here)",
            got.len(),
            parts,
            s.parts_bloom_pruned,
            s.parts_scanned
        );
        drop(cold);
        drop(w);
        for d in [&ii_obj, &ii_local, &ii_cold] {
            let _ = std::fs::remove_dir_all(d);
        }
    }

    // Ranged-block skip: read after a high sequence fetches only the tail block.
    let manifest = cold.manifest_snapshot();
    let total_bytes: u64 = manifest.parts.iter().map(|p| p.byte_size).sum();
    let last_part = manifest
        .parts
        .iter()
        .filter(|p| p.partition == target_shard && p.sparse_index.marks.len() >= 2)
        .max_by_key(|p| p.min_sequence)
        .unwrap();
    let last_mark = last_part.sparse_index.marks.last().unwrap();
    let cursor = last_mark.first_sequence - 1;
    let b0 = cold_counters.get_range_bytes.load(Ordering::Relaxed);
    let _ = cold.read_execution_after(target, cursor).unwrap();
    let block = cold_counters.get_range_bytes.load(Ordering::Relaxed) - b0;
    println!(
        "ranged GET after seq {cursor}: fetched {block} bytes of last part ({} bytes); \
         whole dataset = {total_bytes} bytes  → sparse-index skip factor ~{}x",
        last_part.byte_size,
        total_bytes.checked_div(block).unwrap_or(0)
    );

    // --- hot-path isolation: append latency, fast store vs slow store ---
    println!("\n-- hot-path isolation (append not blocked by slow substrate) --");
    let latency = Duration::from_millis(40);
    let n = 200u64;

    let base_obj = tmp("hp-base-obj");
    let base_local = tmp("hp-base-local");
    let _ = std::fs::remove_dir_all(&base_obj);
    let _ = std::fs::remove_dir_all(&base_local);
    let base_store: Arc<dyn DurableSubstrate> = Arc::new(CountingSubstrate::new(
        LocalFsSubstrate::new(&base_obj).unwrap(),
    ));
    let mut base = L0EventLogEngine::open(
        L0Config::d1(&base_local)
            .with_shard_count(1)
            .with_granule_size(4)
            .with_seal_max_records(8),
        base_store,
    )
    .unwrap();
    let t0 = Instant::now();
    for i in 0..n {
        base.append("1001", &format!("t{i}"), format!("p{i}"))
            .unwrap();
    }
    let t_base = t0.elapsed();
    let seals = base.metrics().snapshot().seals;
    drop(base);

    let slow_obj = tmp("hp-slow-obj");
    let slow_local = tmp("hp-slow-local");
    let _ = std::fs::remove_dir_all(&slow_obj);
    let _ = std::fs::remove_dir_all(&slow_local);
    let slow_store: Arc<dyn DurableSubstrate> = Arc::new(CountingSubstrate::with_put_latency(
        LocalFsSubstrate::new(&slow_obj).unwrap(),
        latency,
    ));
    let mut slow = L0EventLogEngine::open(
        L0Config::d1(&slow_local)
            .with_shard_count(1)
            .with_granule_size(4)
            .with_seal_max_records(8),
        slow_store,
    )
    .unwrap();
    let t1 = Instant::now();
    for i in 0..n {
        slow.append("1001", &format!("t{i}"), format!("p{i}"))
            .unwrap();
    }
    let t_slow = t1.elapsed();

    let serialized = Duration::from_millis(seals * latency.as_millis() as u64);
    println!(
        "{n} appends: fast-store loop = {t_base:?}, slow-store loop = {t_slow:?}\n\
         (if uploads blocked the append path, slow would be ~+{serialized:?} for {seals} \
         {latency:?} uploads; instead extra = {:?})",
        t_slow.saturating_sub(t_base)
    );
    drop(slow);

    // cleanup
    for d in [
        &object_dir,
        &origin_local,
        &cold_local,
        &base_obj,
        &base_local,
        &slow_obj,
        &slow_local,
    ] {
        let _ = std::fs::remove_dir_all(d);
    }
    println!("\n== selfcheck complete ==");
}
