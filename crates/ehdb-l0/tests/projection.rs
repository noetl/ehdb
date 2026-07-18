//! D3 proof — execution/projection read-models over immutable parts.

use std::sync::Arc;

use ehdb_l0::substrate::DurableSubstrate;
use ehdb_l0::{LocalFsSubstrate, ProjectionStore, ReplicaTarget};

fn unique_dir(tag: &str) -> std::path::PathBuf {
    static N: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let n = N.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    std::env::temp_dir().join(format!("ehdb-l0-d3-{tag}-{}-{n}", std::process::id()))
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
fn get_state_is_the_latest_snapshot_and_survives_cold_load() {
    let obj = unique_dir("obj");
    let local = unique_dir("local");
    let cold_local = unique_dir("cold");
    let cfg = |root: &std::path::Path| ProjectionStore::config(root).with_seal_max_records(8);

    let store: Arc<dyn DurableSubstrate> = Arc::new(LocalFsSubstrate::new(&obj).unwrap());
    let mut p = ProjectionStore::open(cfg(&local), store).unwrap();

    // Two executions, each transitioning through several states.
    for exec in ["exec-1", "exec-2", "exec-3"] {
        p.record_state(exec, "queued", "{}").unwrap();
        p.record_state(exec, "running", "{\"step\":1}").unwrap();
    }
    // exec-1 completes; exec-2 gets more updates.
    p.record_state("exec-1", "completed", "{\"result\":\"ok\"}")
        .unwrap();
    p.record_state("exec-2", "running", "{\"step\":2}").unwrap();

    // get-state = the latest snapshot.
    let s1 = p.get_state("exec-1").unwrap().unwrap();
    assert_eq!(s1.status, "completed");
    assert_eq!(s1.data, "{\"result\":\"ok\"}");
    let s2 = p.get_state("exec-2").unwrap().unwrap();
    assert_eq!(s2.status, "running");
    assert_eq!(s2.data, "{\"step\":2}");
    assert!(p.get_state("nope").unwrap().is_none());

    // list-executions.
    assert_eq!(
        p.list_executions().unwrap(),
        vec!["exec-1".to_string(), "exec-2".into(), "exec-3".into()]
    );

    // Flush + merge (log compaction) preserves the read model.
    p.flush_and_wait().unwrap();
    let _ = p.run_pending_merges().unwrap();
    assert_eq!(p.get_state("exec-1").unwrap().unwrap().status, "completed");

    // Cold-load reconstructs the read model.
    let cold_store: Arc<dyn DurableSubstrate> = Arc::new(LocalFsSubstrate::new(&obj).unwrap());
    let cold = ProjectionStore::cold_load(cfg(&cold_local), cold_store).unwrap();
    assert_eq!(cold.get_state("exec-2").unwrap().unwrap().status, "running");
    assert_eq!(cold.list_executions().unwrap().len(), 3);

    drop(cold);
    drop(p);
    for d in [&obj, &local, &cold_local] {
        let _ = std::fs::remove_dir_all(d);
    }
}

#[test]
fn read_model_survives_a_dead_replica() {
    let dirs: Vec<_> = (0..3).map(|i| unique_dir(&format!("r{i}"))).collect();
    let local = unique_dir("nlocal");
    let cold_local = unique_dir("ncold");
    let cfg = |root: &std::path::Path| ProjectionStore::config(root).with_seal_max_records(8);

    let mut p = ProjectionStore::open_replicated(cfg(&local), targets(&dirs)).unwrap();
    for i in 0..10u64 {
        p.record_state(&format!("e{i}"), "done", "{}").unwrap();
    }
    p.flush_and_wait().unwrap();
    let want = p.list_executions().unwrap();
    drop(p);

    std::fs::remove_dir_all(&dirs[0]).unwrap();
    let cold = ProjectionStore::cold_load_replicated(cfg(&cold_local), targets(&dirs)).unwrap();
    assert_eq!(cold.list_executions().unwrap(), want);
    assert_eq!(cold.get_state("e5").unwrap().unwrap().status, "done");
    assert!(cold.engine().metrics().snapshot().read_fallbacks > 0);

    drop(cold);
    for d in dirs.iter().chain([&local, &cold_local]) {
        let _ = std::fs::remove_dir_all(d);
    }
}
