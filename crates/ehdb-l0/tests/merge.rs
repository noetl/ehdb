//! L0.3 proof — the background small→big merge/compaction engine. Real behavior,
//! not asserted:
//!
//! - **Compaction:** many small parts merge into fewer bigger parts.
//! - **Correctness through merge:** `replay_all` after merge returns the exact
//!   same record set + order as before; per-execution reads stay correct (the
//!   rebuilt sparse index + blooms work on merged parts).
//! - **Cold-load after merge:** a fresh node built from the post-merge durable
//!   manifest reproduces the identical record set — the merge is durable.
//! - **No overlap / no double-count:** merged parts never straddle an un-merged
//!   part (contiguous-run planner), so no record appears twice.

use std::sync::atomic::Ordering;
use std::sync::Arc;

use ehdb_l0::substrate::DurableSubstrate;
use ehdb_l0::{
    CountingSubstrate, EventRecord, L0Config, L0EventLogEngine, LocalFsSubstrate, MergePolicy,
};

fn unique_dir(tag: &str) -> std::path::PathBuf {
    static N: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let n = N.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir().join(format!("ehdb-l0-mg-{tag}-{}-{n}", std::process::id()))
}

fn store(object_dir: &std::path::Path) -> Arc<dyn DurableSubstrate> {
    Arc::new(CountingSubstrate::new(
        LocalFsSubstrate::new(object_dir).unwrap(),
    ))
}

#[test]
fn merge_compacts_parts_preserving_records_and_cold_load() {
    let object_dir = unique_dir("obj");
    let origin_local = unique_dir("origin");
    let cold_local = unique_dir("cold");

    // 1 partition, seal at 8, merge a run of >= 3 small parts, up to 8 at once.
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

    // Interleave several executions so a merged part holds many executions —
    // exercises the rebuilt per-part + per-granule blooms.
    let execs = ["1001", "2002", "3003"];
    let mut expected: Vec<EventRecord> = Vec::new();
    let mut origin = L0EventLogEngine::open(cfg(&origin_local), store(&object_dir)).unwrap();
    for i in 0..16u64 {
        for e in &execs {
            let seq = origin
                .append(e, &format!("txn-{e}-{i}"), format!("payload-{e}-{i}"))
                .unwrap();
            expected.push(EventRecord::new(
                seq,
                *e,
                format!("txn-{e}-{i}"),
                format!("payload-{e}-{i}"),
            ));
        }
    }
    expected.sort_by_key(|r| r.global_sequence);
    origin.flush_and_wait_uploads().unwrap();

    let parts_before = origin.manifest_snapshot().parts.len();
    assert!(
        parts_before >= 6,
        "expected several small parts, got {parts_before}"
    );
    let replay_before = origin.replay_all().unwrap();
    assert_eq!(replay_before, expected);

    // Run the merge engine.
    let merges = origin.run_pending_merges().unwrap();
    assert!(merges >= 1, "expected at least one merge");
    let parts_after = origin.manifest_snapshot().parts.len();
    assert!(
        parts_after < parts_before,
        "merge must reduce part count: {parts_before} -> {parts_after}"
    );
    let m = origin.metrics().snapshot();
    assert_eq!(m.merges, merges as u64);
    assert!(m.parts_merged >= 3 && m.merged_bytes > 0, "{m:?}");

    // Correctness through merge: exact record set + order unchanged.
    let replay_after = origin.replay_all().unwrap();
    assert_eq!(
        replay_after, expected,
        "merge preserved the exact record set"
    );

    // No double-count: total records equal the append count (no record appears
    // in two parts).
    assert_eq!(replay_after.len(), expected.len());

    // Per-execution reads still correct after merge (rebuilt index + blooms).
    for e in &execs {
        let got = origin.read_execution_after(e, 0).unwrap();
        let want: Vec<_> = expected
            .iter()
            .filter(|r| &r.execution_id == e)
            .cloned()
            .collect();
        assert_eq!(got, want, "post-merge read for {e}");
    }

    // Cold-load from the POST-MERGE durable manifest reproduces the identical
    // record set + tip — the merge is durable and cold-load-consistent.
    let cold = L0EventLogEngine::cold_load(cfg(&cold_local), store(&object_dir)).unwrap();
    assert_eq!(cold.global_sequence(), origin.global_sequence());
    assert_eq!(cold.replay_all().unwrap(), expected);
    // The cold node sees the compacted manifest (same reduced part count).
    assert_eq!(cold.manifest_snapshot().parts.len(), parts_after);

    drop(cold);
    drop(origin);
    for d in [&object_dir, &origin_local, &cold_local] {
        let _ = std::fs::remove_dir_all(d);
    }
}

#[test]
fn merge_is_idempotent_when_nothing_qualifies() {
    let object_dir = unique_dir("noop-obj");
    let local = unique_dir("noop-local");
    let cfg = L0Config::d1(&local)
        .with_shard_count(1)
        .with_seal_max_records(8)
        .with_merge_policy(MergePolicy {
            small_part_max_records: 8,
            trigger_run_len: 10, // never reached
            max_merge_parts: 8,
        });
    let mut e = L0EventLogEngine::open(cfg, store(&object_dir)).unwrap();
    for i in 0..16u64 {
        e.append("1001", &format!("t{i}"), format!("p{i}")).unwrap();
    }
    e.flush_and_wait_uploads().unwrap();
    let before = e.manifest_snapshot().parts.len();
    assert_eq!(e.run_pending_merges().unwrap(), 0, "no merge should fire");
    assert_eq!(e.manifest_snapshot().parts.len(), before);

    drop(e);
    for d in [&object_dir, &local] {
        let _ = std::fs::remove_dir_all(d);
    }
}
