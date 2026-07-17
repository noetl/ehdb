//! L0.5 proof — retention (drop-partition) + orphan reclaim/GC. Real behavior,
//! not asserted:
//!
//! - **Orphan reclaim after merge:** the superseded source parts a merge leaves
//!   behind are deleted from the object store, while the merged part + the
//!   referenced parts survive; cold-load stays correct after the vacuum.
//! - **Retention as drop-partition:** whole parts below a sort-key floor are
//!   dropped (never a row split), their objects reclaimed, `reclaimed_through`
//!   advanced; the retained record set is exactly the window, and a cold-load
//!   reflects it.

use std::sync::atomic::Ordering;
use std::sync::Arc;

use ehdb_l0::substrate::DurableSubstrate;
use ehdb_l0::{CountingSubstrate, L0Config, L0EventLogEngine, LocalFsSubstrate, MergePolicy};

fn unique_dir(tag: &str) -> std::path::PathBuf {
    static N: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let n = N.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir().join(format!("ehdb-l0-rt-{tag}-{}-{n}", std::process::id()))
}

fn store(object_dir: &std::path::Path) -> Arc<dyn DurableSubstrate> {
    Arc::new(CountingSubstrate::new(
        LocalFsSubstrate::new(object_dir).unwrap(),
    ))
}

fn part_object_count(store: &Arc<dyn DurableSubstrate>) -> usize {
    store.list_prefix("parts/d1_event_log/").unwrap().len()
}

#[test]
fn orphan_reclaim_after_merge_frees_source_objects() {
    let object_dir = unique_dir("obj");
    let origin_local = unique_dir("origin");
    let cold_local = unique_dir("cold");

    let cfg = |root: &std::path::Path| {
        L0Config::d1(root)
            .with_shard_count(1)
            .with_granule_size(4)
            .with_seal_max_records(8)
            .with_merge_policy(MergePolicy {
                small_part_max_records: 8,
                trigger_run_len: 3,
                max_merge_parts: 8,
            })
    };

    // A separate store handle for asserting object counts (shares the dir).
    let probe = store(&object_dir);

    let mut origin = L0EventLogEngine::open(cfg(&origin_local), store(&object_dir)).unwrap();
    for i in 0..48u64 {
        origin
            .append("1001", &format!("t{i}"), format!("payload-{i}"))
            .unwrap();
    }
    origin.flush_and_wait_uploads().unwrap();
    let expected = origin.replay_all().unwrap();

    let objects_before_merge = part_object_count(&probe);
    assert!(
        objects_before_merge >= 6,
        "expected the small part objects present"
    );

    let merges = origin.run_pending_merges().unwrap();
    assert!(merges >= 1);
    // After merge the source objects still physically exist (unreferenced) plus
    // the new merged object.
    let objects_after_merge = part_object_count(&probe);
    assert!(
        objects_after_merge > 1,
        "sources linger until GC: {objects_after_merge}"
    );

    // Orphan reclaim: delete the unreferenced source objects.
    let reclaimed = origin.reclaim_orphans().unwrap();
    assert!(reclaimed >= 1, "reclaimed some orphans");
    let objects_after_gc = part_object_count(&probe);
    // Only the parts the manifest references remain.
    let referenced = origin.manifest_snapshot().parts.len();
    assert_eq!(
        objects_after_gc, referenced,
        "GC leaves exactly the referenced parts"
    );
    assert!(objects_after_gc < objects_after_merge, "GC freed objects");

    let m = origin.metrics().snapshot();
    assert!(m.orphans_reclaimed >= 1 && m.orphan_bytes > 0, "{m:?}");

    // Correctness preserved: replay + cold-load identical after the vacuum.
    assert_eq!(origin.replay_all().unwrap(), expected);
    let cold = L0EventLogEngine::cold_load(cfg(&cold_local), store(&object_dir)).unwrap();
    assert_eq!(cold.replay_all().unwrap(), expected);

    drop(cold);
    drop(origin);
    for d in [&object_dir, &origin_local, &cold_local] {
        let _ = std::fs::remove_dir_all(d);
    }
}

#[test]
fn retention_drops_whole_parts_below_the_floor() {
    let object_dir = unique_dir("ret-obj");
    let origin_local = unique_dir("ret-origin");
    let cold_local = unique_dir("ret-cold");

    let cfg = |root: &std::path::Path| {
        L0Config::d1(root)
            .with_shard_count(1)
            .with_granule_size(4)
            .with_seal_max_records(8)
    };
    let probe = store(&object_dir);

    // 48 events (1 exec) → 6 parts covering seqs [1-8]..[41-48].
    let mut origin = L0EventLogEngine::open(cfg(&origin_local), store(&object_dir)).unwrap();
    for i in 0..48u64 {
        origin
            .append("1001", &format!("t{i}"), format!("payload-{i}"))
            .unwrap();
    }
    origin.flush_and_wait_uploads().unwrap();
    let parts_before = origin.manifest_snapshot().parts.len();
    let objects_before = part_object_count(&probe);

    // Retain from seq 25 → drop the three parts entirely below 25
    // ([1-8],[9-16],[17-24]); keep [25-48].
    let dropped = origin.apply_retention(25).unwrap();
    assert_eq!(dropped, 3, "three whole parts dropped");
    assert_eq!(
        origin.reclaimed_through(),
        24,
        "floor advanced to the highest dropped seq"
    );

    let parts_after = origin.manifest_snapshot().parts.len();
    assert_eq!(parts_after, parts_before - 3);
    // Dropped parts' objects reclaimed.
    let objects_after = part_object_count(&probe);
    assert!(objects_after < objects_before, "dropped objects reclaimed");
    assert_eq!(objects_after, parts_after, "only retained parts remain");

    // The retained record set is exactly seqs 25..=48.
    let retained = origin.replay_all().unwrap();
    assert_eq!(retained.len(), 24);
    assert_eq!(retained.first().unwrap().global_sequence, 25);
    assert_eq!(retained.last().unwrap().global_sequence, 48);

    // A read from before the floor finds only retained records (old ones gone),
    // never an error.
    let got = origin.read_execution_after("1001", 0).unwrap();
    assert_eq!(got.len(), 24);
    assert!(got.iter().all(|r| r.global_sequence >= 25));

    // Cold-load reflects retention: same reduced parts + floor + record set.
    let cold = L0EventLogEngine::cold_load(cfg(&cold_local), store(&object_dir)).unwrap();
    assert_eq!(cold.reclaimed_through(), 24);
    assert_eq!(cold.manifest_snapshot().parts.len(), parts_after);
    assert_eq!(cold.replay_all().unwrap(), retained);

    let m = origin.metrics().snapshot();
    assert_eq!(m.parts_dropped, 3);

    drop(cold);
    drop(origin);
    for d in [&object_dir, &origin_local, &cold_local] {
        let _ = std::fs::remove_dir_all(d);
    }
}
