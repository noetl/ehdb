//! D4 proof — KV get/put-latest, CAS, prefix-scan, delete over immutable parts.

use std::sync::Arc;

use ehdb_l0::substrate::DurableSubstrate;
use ehdb_l0::{KvStore, LocalFsSubstrate, ReplicaTarget};

fn unique_dir(tag: &str) -> std::path::PathBuf {
    static N: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let n = N.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    std::env::temp_dir().join(format!("ehdb-l0-d4-{tag}-{}-{n}", std::process::id()))
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
fn put_get_cas_prefix_delete_and_cold_load() {
    let obj = unique_dir("obj");
    let local = unique_dir("local");
    let cold_local = unique_dir("cold");
    let cfg = |root: &std::path::Path| KvStore::config(root).with_seal_max_records(8);

    let store: Arc<dyn DurableSubstrate> = Arc::new(LocalFsSubstrate::new(&obj).unwrap());
    let mut kv = KvStore::open(cfg(&local), store).unwrap();

    // put / get-latest with versions.
    assert_eq!(kv.put("chain/a", "v1").unwrap(), 1);
    assert_eq!(kv.put("chain/a", "v2").unwrap(), 2);
    let e = kv.get("chain/a").unwrap().unwrap();
    assert_eq!((e.value.as_str(), e.version), ("v2", 2));
    assert!(kv.get("missing").unwrap().is_none());

    // CAS: succeeds at the current version, fails on a stale one.
    assert_eq!(kv.compare_and_set("chain/a", 2, "v3").unwrap(), Some(3));
    assert_eq!(kv.compare_and_set("chain/a", 2, "stale").unwrap(), None);
    assert_eq!(kv.get("chain/a").unwrap().unwrap().value, "v3");
    // CAS on a fresh key requires expected version 0.
    assert_eq!(kv.compare_and_set("chain/b", 0, "b1").unwrap(), Some(1));
    assert_eq!(kv.compare_and_set("chain/c", 5, "x").unwrap(), None);

    // prefix-scan.
    kv.put("lease/x", "held").unwrap();
    kv.put("lease/y", "held").unwrap();
    let chain: Vec<_> = kv.prefix_scan("chain/").unwrap();
    assert_eq!(
        chain,
        vec![
            ("chain/a".to_string(), "v3".to_string()),
            ("chain/b".into(), "b1".into())
        ]
    );

    // delete tombstones the key.
    assert_eq!(kv.delete("chain/b").unwrap(), Some(2));
    assert!(kv.get("chain/b").unwrap().is_none());
    assert_eq!(
        kv.prefix_scan("chain/").unwrap().len(),
        1,
        "deleted key gone"
    );

    // Flush + merge + cold-load: state is preserved.
    kv.flush_and_wait().unwrap();
    let _ = kv.run_pending_merges().unwrap();
    let cold_store: Arc<dyn DurableSubstrate> = Arc::new(LocalFsSubstrate::new(&obj).unwrap());
    let cold = KvStore::cold_load(cfg(&cold_local), cold_store).unwrap();
    assert_eq!(cold.get("chain/a").unwrap().unwrap().value, "v3");
    assert!(
        cold.get("chain/b").unwrap().is_none(),
        "tombstone survives cold-load"
    );
    assert_eq!(cold.prefix_scan("lease/").unwrap().len(), 2);

    drop(cold);
    drop(kv);
    for d in [&obj, &local, &cold_local] {
        let _ = std::fs::remove_dir_all(d);
    }
}

#[test]
fn kv_survives_a_dead_replica() {
    let dirs: Vec<_> = (0..3).map(|i| unique_dir(&format!("r{i}"))).collect();
    let local = unique_dir("nlocal");
    let cold_local = unique_dir("ncold");
    let cfg = |root: &std::path::Path| KvStore::config(root).with_seal_max_records(8);

    let mut kv = KvStore::open_replicated(cfg(&local), targets(&dirs)).unwrap();
    for i in 0..12u64 {
        kv.put(&format!("k{i}"), format!("val-{i}")).unwrap();
    }
    kv.flush_and_wait().unwrap();
    drop(kv);

    std::fs::remove_dir_all(&dirs[0]).unwrap();
    let cold = KvStore::cold_load_replicated(cfg(&cold_local), targets(&dirs)).unwrap();
    assert_eq!(cold.get("k7").unwrap().unwrap().value, "val-7");
    assert_eq!(cold.prefix_scan("k").unwrap().len(), 12);
    assert!(cold.engine().metrics().snapshot().read_fallbacks > 0);

    drop(cold);
    for d in dirs.iter().chain([&local, &cold_local]) {
        let _ = std::fs::remove_dir_all(d);
    }
}
