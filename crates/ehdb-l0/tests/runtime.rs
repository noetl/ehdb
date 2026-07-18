//! D8 proof — worker register / heartbeat / list-live / deregister over
//! immutable parts.

use std::sync::Arc;

use ehdb_l0::substrate::DurableSubstrate;
use ehdb_l0::{LocalFsSubstrate, ReplicaTarget, RuntimeStore};

fn unique_dir(tag: &str) -> std::path::PathBuf {
    static N: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let n = N.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    std::env::temp_dir().join(format!("ehdb-l0-d8-{tag}-{}-{n}", std::process::id()))
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
fn register_heartbeat_listlive_deregister_and_cold_load() {
    let obj = unique_dir("obj");
    let local = unique_dir("local");
    let cold_local = unique_dir("cold");
    let cfg = |root: &std::path::Path| RuntimeStore::config(root).with_seal_max_records(8);

    let store: Arc<dyn DurableSubstrate> = Arc::new(LocalFsSubstrate::new(&obj).unwrap());
    let mut r = RuntimeStore::open(cfg(&local), store).unwrap();

    // register two workers with their runtime contracts.
    assert_eq!(r.register("w-a", "pool=user;arch=arm64;cap=4").unwrap(), 1);
    assert_eq!(
        r.register("w-b", "pool=system;arch=amd64;cap=1").unwrap(),
        1
    );

    // heartbeat advances a worker's watermark; echoes its contract.
    assert_eq!(r.heartbeat("w-a").unwrap(), Some(2));
    assert_eq!(r.heartbeat("w-a").unwrap(), Some(3));
    let a = r.get("w-a").unwrap().unwrap();
    assert_eq!(
        (a.heartbeat, a.contract.as_str()),
        (3, "pool=user;arch=arm64;cap=4")
    );
    // heartbeat of an unknown worker is a no-op signal to register first.
    assert_eq!(r.heartbeat("ghost").unwrap(), None);

    // list-live has both; ordered by worker id.
    let live: Vec<_> = r
        .list_live()
        .unwrap()
        .into_iter()
        .map(|s| s.worker_id)
        .collect();
    assert_eq!(live, vec!["w-a", "w-b"]);

    // fresh-since watermark evicts a worker that stopped beating (w-b at 1).
    r.heartbeat("w-a").unwrap(); // w-a -> 4
    let fresh: Vec<_> = r
        .list_live_since(2)
        .unwrap()
        .into_iter()
        .map(|s| s.worker_id)
        .collect();
    assert_eq!(
        fresh,
        vec!["w-a"],
        "w-b (heartbeat=1) is below the watermark"
    );

    // deregister drops a worker out of list-live; re-register resets to 1.
    assert!(r.deregister("w-b").unwrap());
    assert!(r.get("w-b").unwrap().is_none());
    assert!(!r.deregister("w-b").unwrap(), "already gone");
    assert_eq!(
        r.list_live()
            .unwrap()
            .iter()
            .map(|s| s.worker_id.as_str())
            .collect::<Vec<_>>(),
        vec!["w-a"]
    );
    assert_eq!(
        r.register("w-b", "pool=system;arch=amd64;cap=2").unwrap(),
        1
    );
    assert_eq!(r.get("w-b").unwrap().unwrap().heartbeat, 1);

    // Flush + merge + cold-load: registrations survive.
    r.flush_and_wait().unwrap();
    let _ = r.run_pending_merges().unwrap();
    let cold_store: Arc<dyn DurableSubstrate> = Arc::new(LocalFsSubstrate::new(&obj).unwrap());
    let cold = RuntimeStore::cold_load(cfg(&cold_local), cold_store).unwrap();
    assert_eq!(cold.get("w-a").unwrap().unwrap().heartbeat, 4);
    assert_eq!(
        cold.get("w-b").unwrap().unwrap().contract,
        "pool=system;arch=amd64;cap=2"
    );
    assert_eq!(cold.list_live().unwrap().len(), 2);

    drop(cold);
    drop(r);
    for d in [&obj, &local, &cold_local] {
        let _ = std::fs::remove_dir_all(d);
    }
}

#[test]
fn runtime_survives_a_dead_replica() {
    let dirs: Vec<_> = (0..3).map(|i| unique_dir(&format!("r{i}"))).collect();
    let local = unique_dir("nlocal");
    let cold_local = unique_dir("ncold");
    let cfg = |root: &std::path::Path| RuntimeStore::config(root).with_seal_max_records(8);

    let mut r = RuntimeStore::open_replicated(cfg(&local), targets(&dirs)).unwrap();
    for i in 0..12u64 {
        r.register(&format!("w{i}"), format!("pool=user;cap={i}"))
            .unwrap();
        r.heartbeat(&format!("w{i}")).unwrap();
    }
    r.flush_and_wait().unwrap();
    drop(r);

    std::fs::remove_dir_all(&dirs[0]).unwrap();
    let cold = RuntimeStore::cold_load_replicated(cfg(&cold_local), targets(&dirs)).unwrap();
    assert_eq!(cold.get("w7").unwrap().unwrap().heartbeat, 2);
    assert_eq!(cold.list_live().unwrap().len(), 12);
    assert!(cold.engine().metrics().snapshot().read_fallbacks > 0);

    drop(cold);
    for d in dirs.iter().chain([&local, &cold_local]) {
        let _ = std::fs::remove_dir_all(d);
    }
}
