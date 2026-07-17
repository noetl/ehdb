//! L0.7 proof — the engine is **genuinely dataset-generic**, not just D1
//! renamed. Defines a *second, synthetic* dataset with a different record
//! schema, a different sort key, and a different index dimension, and runs the
//! SAME `L0Engine` over it end-to-end: append → seal → replicate → merge →
//! read-by-index → cold-load. If the engine still assumed D1's `EventRecord`,
//! none of this would compile or pass.

use std::sync::Arc;

use ehdb_l0::substrate::DurableSubstrate;
use ehdb_l0::{
    shard_for_execution, CountingSubstrate, Dataset, L0Config, L0Engine, LocalFsSubstrate,
    MergePolicy, ReplicaTarget,
};
use serde::{Deserialize, Serialize};

/// A synthetic dataset — a tiny "audit" record with fields that DON'T match D1.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct AuditRecord {
    /// The sort key (named nothing like `global_sequence`).
    tick: u64,
    /// The index dimension (named nothing like `execution_id`).
    actor: String,
    /// An opaque payload.
    action: String,
}

struct AuditDataset;

impl Dataset for AuditDataset {
    type Record = AuditRecord;
    const NAME: &'static str = "test_audit";

    fn sort_key(r: &AuditRecord) -> u64 {
        r.tick
    }
    fn partition(r: &AuditRecord, shard_count: u32) -> u32 {
        shard_for_execution(&r.actor, shard_count)
    }
    fn index_key(r: &AuditRecord) -> &str {
        &r.actor
    }
    fn read_partition(actor: &str, shard_count: u32) -> u32 {
        shard_for_execution(actor, shard_count)
    }
}

fn unique_dir(tag: &str) -> std::path::PathBuf {
    static N: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let n = N.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    std::env::temp_dir().join(format!("ehdb-l0-gen-{tag}-{}-{n}", std::process::id()))
}

fn targets(dirs: &[std::path::PathBuf]) -> Vec<ReplicaTarget> {
    dirs.iter()
        .enumerate()
        .map(|(i, d)| {
            let s: Arc<dyn DurableSubstrate> = Arc::new(LocalFsSubstrate::new(d).unwrap());
            ReplicaTarget::new(format!("replica-{i}"), s)
        })
        .collect()
}

#[test]
fn a_second_dataset_runs_on_the_same_engine() {
    let obj = unique_dir("obj");
    let local = unique_dir("local");
    let cold_local = unique_dir("cold");
    let cfg = |root: &std::path::Path| {
        L0Config::for_dataset(AuditDataset::NAME, root)
            .with_shard_count(1)
            .with_granule_size(4)
            .with_seal_max_records(8)
            .with_merge_policy(MergePolicy {
                small_part_max_records: 8,
                trigger_run_len: 3,
                max_merge_parts: 8,
            })
    };

    // Append audit records for two actors, interleaved.
    let store: Arc<dyn DurableSubstrate> =
        Arc::new(CountingSubstrate::new(LocalFsSubstrate::new(&obj).unwrap()));
    let mut engine = L0Engine::<AuditDataset>::open(cfg(&local), store).unwrap();
    let mut expected: Vec<AuditRecord> = Vec::new();
    for tick in 1..=24u64 {
        let actor = if tick % 2 == 0 { "alice" } else { "bob" };
        let rec = AuditRecord {
            tick,
            actor: actor.to_string(),
            action: format!("act-{tick}"),
        };
        let sk = engine.append_record(rec.clone()).unwrap();
        assert_eq!(sk, tick, "append_record returns the record's sort key");
        expected.push(rec);
    }
    engine.flush_and_wait_uploads().unwrap();

    // The dataset name propagated into the substrate keys (D::NAME, not d1).
    let manifest = engine.manifest_snapshot();
    assert_eq!(manifest.dataset, "test_audit");
    assert!(manifest
        .parts
        .iter()
        .all(|p| p.replicas.iter().all(|r| r.key.contains("test_audit"))));

    // Merge works on the generic dataset.
    let merges = engine.run_pending_merges().unwrap();
    assert!(merges >= 1, "merge ran on the audit dataset");

    // read_index_after filters by the dataset's index dim (`actor`), sorted by
    // its sort key (`tick`).
    let alice = engine.read_index_after("alice", 0).unwrap();
    let want_alice: Vec<_> = expected
        .iter()
        .filter(|r| r.actor == "alice")
        .cloned()
        .collect();
    assert_eq!(alice, want_alice, "read-by-index on a non-D1 dataset");
    assert!(alice.iter().all(|r| r.actor == "alice"));
    assert!(alice.windows(2).all(|w| w[0].tick < w[1].tick));

    // replay_all reproduces the whole (audit) record set in sort-key order.
    let all = engine.replay_all().unwrap();
    let mut want_all = expected.clone();
    want_all.sort_by_key(|r| r.tick);
    assert_eq!(all, want_all);

    // Cold-load a fresh node over the same substrate — reproduces the audit set.
    let cold_store: Arc<dyn DurableSubstrate> =
        Arc::new(CountingSubstrate::new(LocalFsSubstrate::new(&obj).unwrap()));
    let cold = L0Engine::<AuditDataset>::cold_load(cfg(&cold_local), cold_store).unwrap();
    assert_eq!(cold.replay_all().unwrap(), want_all);

    drop(cold);
    drop(engine);
    for d in [&obj, &local, &cold_local] {
        let _ = std::fs::remove_dir_all(d);
    }
}

#[test]
fn generic_nway_replication_on_second_dataset() {
    // N-way replication is dataset-agnostic too: 3 replicas, kill one, cold-load.
    let dirs: Vec<_> = (0..3).map(|i| unique_dir(&format!("r{i}"))).collect();
    let local = unique_dir("nlocal");
    let cold_local = unique_dir("ncold");
    let cfg = |root: &std::path::Path| {
        L0Config::for_dataset(AuditDataset::NAME, root)
            .with_shard_count(1)
            .with_seal_max_records(8)
    };

    let mut e = L0Engine::<AuditDataset>::open_replicated(cfg(&local), targets(&dirs)).unwrap();
    for tick in 1..=16u64 {
        e.append_record(AuditRecord {
            tick,
            actor: "svc".into(),
            action: format!("a{tick}"),
        })
        .unwrap();
    }
    e.flush_and_wait_uploads().unwrap();
    let expected = e.replay_all().unwrap();
    for p in &e.manifest_snapshot().parts {
        assert_eq!(p.replica_count(), 3);
    }
    drop(e);

    // Kill replica-0, cold-load from survivors.
    std::fs::remove_dir_all(&dirs[0]).unwrap();
    let cold =
        L0Engine::<AuditDataset>::cold_load_replicated(cfg(&cold_local), targets(&dirs)).unwrap();
    assert_eq!(cold.replay_all().unwrap(), expected);
    assert!(cold.metrics().snapshot().read_fallbacks > 0);

    drop(cold);
    for d in dirs.iter().chain([&local, &cold_local]) {
        let _ = std::fs::remove_dir_all(d);
    }
}
