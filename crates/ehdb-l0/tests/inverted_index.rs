//! L0.2 proof — the fixed per-dataset inverted index (execution-id blooms) +
//! index-first pruning. Real behavior, not asserted:
//!
//! - **Part-level bloom prune:** with everything in ONE partition
//!   (`shard_count == 1`, the prod default) the partition/MinMax prune does
//!   nothing, yet a per-execution lookup opens only the parts that can hold that
//!   execution — the rest are skipped by the bloom with zero I/O.
//! - **Zero false negatives:** every event of the queried execution is still
//!   returned (the bloom never wrongly skips a matching part).
//! - **Granule-level narrowing:** within a part, a read fetches only the
//!   contiguous granule span whose blooms admit the execution — fewer bytes than
//!   the sparse-index-only block.

use std::sync::atomic::Ordering;
use std::sync::Arc;

use ehdb_l0::substrate::DurableSubstrate;
use ehdb_l0::{CountingSubstrate, L0Config, L0EventLogEngine, LocalFsSubstrate};

fn unique_dir(tag: &str) -> std::path::PathBuf {
    static N: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let n = N.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir().join(format!("ehdb-l0-ii-{tag}-{}-{n}", std::process::id()))
}

type CountingStore = (
    Arc<dyn DurableSubstrate>,
    Arc<ehdb_l0::substrate::SubstrateCounters>,
    Arc<std::sync::Mutex<Vec<String>>>,
);

fn counting_store(object_dir: &std::path::Path) -> CountingStore {
    let counting = CountingSubstrate::new(LocalFsSubstrate::new(object_dir).unwrap());
    let counters = counting.counters();
    let read_keys = counting.read_keys();
    (Arc::new(counting), counters, read_keys)
}

#[test]
fn part_level_bloom_skips_nonmatching_parts_in_one_partition() {
    let object_dir = unique_dir("obj");
    let origin_local = unique_dir("origin");
    let cold_local = unique_dir("cold");

    // ONE partition (shard_count = 1) — the partition/MinMax prune can't help;
    // the execution bloom is the ONLY pruning mechanism. granule 4, seal at 8.
    let cfg = |root: &std::path::Path| {
        L0Config::d1(root)
            .with_shard_count(1)
            .with_granule_size(4)
            .with_seal_max_records(8)
    };

    // Execution-homogeneous parts: append all of e0, then all of e1, ... so each
    // execution fills its own parts and every OTHER part's bloom rejects it.
    let execs: Vec<String> = (0..6).map(|i| format!("e{i}")).collect();
    let events_per_exec = 16u64; // 2 full parts each

    let (origin_store, _oc, _ok) = counting_store(&object_dir);
    let mut origin = L0EventLogEngine::open(cfg(&origin_local), origin_store).unwrap();
    for exec in &execs {
        for i in 0..events_per_exec {
            origin
                .append(exec, &format!("txn-{exec}-{i}"), format!("p-{exec}-{i}"))
                .unwrap();
        }
    }
    origin.flush_and_wait_uploads().unwrap();

    // Cold node reads from the substrate, so we can see exactly which parts
    // are fetched.
    let (cold_store, _cc, cold_read_keys) = counting_store(&object_dir);
    let cold = L0EventLogEngine::cold_load(cfg(&cold_local), cold_store).unwrap();

    let manifest = cold.manifest_snapshot();
    let total_parts = manifest.parts.len();
    assert_eq!(
        total_parts, 12,
        "6 execs * 16 events / 8 per part = 12 parts"
    );
    // All parts are in partition 0 (single owner) — so partition/MinMax pruning
    // is powerless; the bloom must do the work.
    assert!(manifest.parts.iter().all(|p| p.partition == 0));
    // Every part carries an execution bloom (L0.2), not an L0.1 empty one.
    assert!(manifest.parts.iter().all(|p| p.execution_bloom.is_some()));

    // Read exec e3: correctness first (zero false negatives).
    let target = "e3";
    cold_read_keys.lock().unwrap().clear();
    let hits = cold.read_execution_after(target, 0).unwrap();
    assert_eq!(
        hits.len(),
        events_per_exec as usize,
        "all {target} events returned"
    );
    assert!(hits.iter().all(|r| r.execution_id == target));
    assert_eq!(
        hits.iter().map(|r| r.global_sequence).min().unwrap(),
        hits[0].global_sequence
    );

    // Index-first pruning: the read opened only e3's parts (2), skipping the
    // other 10 via the bloom. Allow a tiny slack for a rare bloom false positive.
    let snap = cold.metrics().snapshot();
    assert!(
        snap.parts_bloom_pruned >= 9,
        "bloom should skip ~10 non-matching parts, skipped {}",
        snap.parts_bloom_pruned
    );
    assert!(
        snap.parts_scanned <= 4,
        "read should open only ~2 parts, opened {}",
        snap.parts_scanned
    );

    // The object-store keys touched are only e3's parts (their id encodes the
    // sequence range; correctness is the returned set, already asserted). The
    // count of distinct fetched keys must be small vs the 12 parts.
    let touched: std::collections::BTreeSet<String> =
        cold_read_keys.lock().unwrap().iter().cloned().collect();
    assert!(
        touched.len() <= 4 && !touched.is_empty(),
        "opened {} parts of {total_parts}; expected ~2",
        touched.len()
    );

    // Correctness for EVERY execution — the bloom never drops a real match.
    for exec in &execs {
        let got = cold.read_execution_after(exec, 0).unwrap();
        assert_eq!(got.len(), events_per_exec as usize, "exec {exec} full set");
        assert!(got.iter().all(|r| &r.execution_id == exec));
    }

    drop(cold);
    drop(origin);
    for d in [&object_dir, &origin_local, &cold_local] {
        let _ = std::fs::remove_dir_all(d);
    }
}

#[test]
fn granule_bloom_narrows_the_ranged_block() {
    let object_dir = unique_dir("g-obj");
    let origin_local = unique_dir("g-origin");
    let cold_local = unique_dir("g-cold");

    // One part, two granules of 4: granule 0 = filler exec "a", granule 1 =
    // "target". A read for target from seq 0 should skip granule 0 via its bloom
    // and fetch only granule 1's block.
    let cfg = |root: &std::path::Path| {
        L0Config::d1(root)
            .with_shard_count(1)
            .with_granule_size(4)
            .with_seal_max_records(8)
    };

    let (origin_store, _oc, _ok) = counting_store(&object_dir);
    let mut origin = L0EventLogEngine::open(cfg(&origin_local), origin_store).unwrap();
    for i in 0..4 {
        origin
            .append("a", &format!("ta{i}"), format!("filler-{i}"))
            .unwrap();
    }
    for i in 0..4 {
        origin
            .append("target", &format!("tt{i}"), format!("wanted-{i}"))
            .unwrap();
    }
    origin.flush_and_wait_uploads().unwrap();

    let (cold_store, cold_counters, _ck) = counting_store(&object_dir);
    let cold = L0EventLogEngine::cold_load(cfg(&cold_local), cold_store).unwrap();
    let manifest = cold.manifest_snapshot();
    assert_eq!(manifest.parts.len(), 1);
    let part = &manifest.parts[0];
    assert_eq!(part.sparse_index.marks.len(), 2);
    assert_eq!(part.granule_blooms.len(), 2, "per-granule blooms present");

    let before = cold_counters.get_range_bytes.load(Ordering::Relaxed);
    let hits = cold.read_execution_after("target", 0).unwrap();
    let block = cold_counters.get_range_bytes.load(Ordering::Relaxed) - before;

    // Correctness: exactly the 4 target events.
    assert_eq!(hits.len(), 4);
    assert!(hits.iter().all(|r| r.execution_id == "target"));

    // Granule narrowing: fetched only granule 1's block, not the whole part.
    let granule1_offset = part.sparse_index.marks[1].byte_offset;
    assert!(granule1_offset > 0);
    assert_eq!(
        block,
        part.byte_size - granule1_offset,
        "fetched only granule 1's block [{granule1_offset}, {})",
        part.byte_size
    );
    assert!(block < part.byte_size, "narrowed below the whole part");

    drop(cold);
    drop(origin);
    for d in [&object_dir, &origin_local, &cold_local] {
        let _ = std::fs::remove_dir_all(d);
    }
}
