//! L0.6 proof — **N-way replication of immutable parts**. Real behavior, not
//! asserted:
//!
//! - **N-way copy:** each sealed part is written to all N substrate replicas; the
//!   manifest lists a [`ReplicaLocation`] per replica; the part physically exists
//!   in every replica's store.
//! - **Manifest replicated too:** the durable manifest lands on every replica, so
//!   any one can serve a cold-load alone.
//! - **Read fallback (the durability payoff):** kill `replica-0` → a fresh node
//!   cold-loads from a surviving replica and reads still reproduce the exact
//!   record set, falling back past the dead replica. No consensus — parts are
//!   immutable.

use std::sync::Arc;

use ehdb_l0::substrate::DurableSubstrate;
use ehdb_l0::{EventRecord, L0Config, L0EventLogEngine, LocalFsSubstrate, ReplicaTarget};

fn unique_dir(tag: &str) -> std::path::PathBuf {
    static N: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let n = N.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    std::env::temp_dir().join(format!("ehdb-l0-nway-{tag}-{}-{n}", std::process::id()))
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

fn part_keys_in(dir: &std::path::Path) -> Vec<String> {
    LocalFsSubstrate::new(dir)
        .unwrap()
        .list_prefix("parts/d1_event_log/")
        .unwrap()
}

#[test]
fn nway_replicated_write_manifest_and_read_fallback() {
    // Three replica stores (r0, r1, r2) — distinct directories standing in for
    // distinct nodes/disks.
    let r0 = unique_dir("r0");
    let r1 = unique_dir("r1");
    let r2 = unique_dir("r2");
    let origin_local = unique_dir("origin");
    let cfg = |root: &std::path::Path| {
        L0Config::d1(root)
            .with_shard_count(1)
            .with_granule_size(4)
            .with_seal_max_records(8)
    };

    // --- origin: N-way replicated write ---
    let mut origin = L0EventLogEngine::open_replicated(
        cfg(&origin_local),
        targets(&[r0.clone(), r1.clone(), r2.clone()]),
    )
    .unwrap();
    let mut expected: Vec<EventRecord> = Vec::new();
    for i in 0..24u64 {
        let seq = origin
            .append("1001", &format!("t{i}"), format!("payload-{i}"))
            .unwrap();
        expected.push(EventRecord::new(
            seq,
            "1001",
            format!("t{i}"),
            format!("payload-{i}"),
        ));
    }
    origin.flush_and_wait_uploads().unwrap();

    // Manifest: 3 parts (24/8), each replicated to all 3 replicas.
    let manifest = origin.manifest_snapshot();
    assert_eq!(manifest.parts.len(), 3, "24 events / 8 per part");
    for p in &manifest.parts {
        assert_eq!(p.replica_count(), 3, "each part replicated 3-way");
        let ids: std::collections::BTreeSet<_> =
            p.replicas.iter().map(|r| r.replica.clone()).collect();
        assert_eq!(
            ids,
            ["replica-0", "replica-1", "replica-2"]
                .iter()
                .map(|s| s.to_string())
                .collect()
        );
        // All replicas of a part share the same (deterministic) key.
        assert!(p.replicas.iter().all(|r| r.key == p.replicas[0].key));
    }

    // Physically present in every replica store.
    for dir in [&r0, &r1, &r2] {
        assert_eq!(part_keys_in(dir).len(), 3, "all 3 parts present in {dir:?}");
    }
    // replica_writes == parts × N.
    let m = origin.metrics().snapshot();
    assert_eq!(m.replica_writes, 3 * 3, "3 parts × 3 replicas: {m:?}");

    // Cold-load from all three replicas reproduces the records.
    let cold_local = unique_dir("cold");
    let cold = L0EventLogEngine::cold_load_replicated(
        cfg(&cold_local),
        targets(&[r0.clone(), r1.clone(), r2.clone()]),
    )
    .unwrap();
    assert_eq!(cold.replay_all().unwrap(), expected);
    drop(cold);

    // --- kill replica-0 (delete its store) ---
    std::fs::remove_dir_all(&r0).unwrap();

    // A fresh node cold-loads with replica-0 DEAD: the manifest is served from a
    // surviving replica, and part reads fall back past the dead replica-0.
    let cold2_local = unique_dir("cold2");
    let cold2 = L0EventLogEngine::cold_load_replicated(
        cfg(&cold2_local),
        targets(&[r0.clone(), r1.clone(), r2.clone()]), // r0 now re-created empty
    )
    .unwrap();
    let got = cold2.replay_all().unwrap();
    assert_eq!(
        got, expected,
        "records reproduced despite replica-0 being gone"
    );
    let m2 = cold2.metrics().snapshot();
    assert!(
        m2.read_fallbacks > 0,
        "reads must have fallen back past the dead replica-0: {m2:?}"
    );

    // Sanity: a per-execution read also works over the survivors.
    let hits = cold2.read_execution_after("1001", 0).unwrap();
    assert_eq!(hits.len(), 24);
    assert!(hits.iter().all(|r| r.execution_id == "1001"));

    drop(cold2);
    drop(origin);
    for d in [&r0, &r1, &r2, &origin_local, &cold_local, &cold2_local] {
        let _ = std::fs::remove_dir_all(d);
    }
}

#[test]
fn single_replica_open_is_replica_zero() {
    // Back-compat: the single-substrate `open` yields exactly one replica.
    let obj = unique_dir("single-obj");
    let local = unique_dir("single-local");
    let store: Arc<dyn DurableSubstrate> = Arc::new(LocalFsSubstrate::new(&obj).unwrap());
    let mut e =
        L0EventLogEngine::open(L0Config::d1(&local).with_seal_max_records(8), store).unwrap();
    for i in 0..8u64 {
        e.append("1001", &format!("t{i}"), format!("p{i}")).unwrap();
    }
    e.flush_and_wait_uploads().unwrap();
    let manifest = e.manifest_snapshot();
    assert!(!manifest.parts.is_empty());
    for p in &manifest.parts {
        assert_eq!(p.replica_count(), 1);
        assert_eq!(p.replicas[0].replica, "replica-0");
    }
    drop(e);
    for d in [&obj, &local] {
        let _ = std::fs::remove_dir_all(d);
    }
}
