//! D9 proof — system-WASM publish / bind (rollback) / resolve / unpublish over
//! content-addressed bytes + an op-log on immutable parts.

use std::sync::Arc;

use ehdb_l0::substrate::DurableSubstrate;
use ehdb_l0::{LocalFsSubstrate, ReplicaTarget, WasmStore};

fn unique_dir(tag: &str) -> std::path::PathBuf {
    static N: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let n = N.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    std::env::temp_dir().join(format!("ehdb-l0-d9-{tag}-{}-{n}", std::process::id()))
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

// A stand-in for a compiled WASM module (real bytes, just not a valid module).
fn wasm_blob(tag: &str) -> Vec<u8> {
    let mut v = b"\0asm\x01\0\0\0".to_vec(); // wasm magic + version
    v.extend(tag.as_bytes().iter().cycle().take(500));
    v
}

#[test]
fn publish_bind_resolve_unpublish_and_cold_load() {
    let obj = unique_dir("obj");
    let local = unique_dir("local");
    let cold_local = unique_dir("cold");
    let cfg = |root: &std::path::Path| WasmStore::config(root).with_seal_max_records(8);

    let store: Arc<dyn DurableSubstrate> = Arc::new(LocalFsSubstrate::new(&obj).unwrap());
    let mut w = WasmStore::open(cfg(&local), store).unwrap();

    let v1 = wasm_blob("orchestrate-v1");
    let v2 = wasm_blob("orchestrate-v2");

    // publish assigns monotonic per-triple versions; each activates.
    assert_eq!(
        w.publish("orchestrate-core", "stable", "prod", &v1)
            .unwrap(),
        1
    );
    assert_eq!(
        w.publish("orchestrate-core", "stable", "prod", &v2)
            .unwrap(),
        2
    );
    // a different triple is independent.
    assert_eq!(
        w.publish("orchestrate-core", "canary", "prod", &v2)
            .unwrap(),
        1
    );

    // resolve returns the active version's bytes.
    let m = w
        .resolve("orchestrate-core", "stable", "prod")
        .unwrap()
        .unwrap();
    assert_eq!((m.version, m.bytes.as_slice()), (2, v2.as_slice()));
    assert!(w.resolve("nope", "stable", "prod").unwrap().is_none());

    // bind rolls the active binding back to an older published version.
    assert!(w.bind("orchestrate-core", "stable", "prod", 1).unwrap());
    let rolled = w
        .resolve("orchestrate-core", "stable", "prod")
        .unwrap()
        .unwrap();
    assert_eq!(
        (rolled.version, rolled.bytes.as_slice()),
        (1, v1.as_slice())
    );
    // binding to a never-published version is refused.
    assert!(!w.bind("orchestrate-core", "stable", "prod", 9).unwrap());

    // list shows live triples with their active binding.
    let live = w.list(None).unwrap();
    assert_eq!(live.len(), 2);
    let prod_stable = live
        .iter()
        .find(|b| b.channel == "stable")
        .expect("stable binding present");
    assert_eq!(prod_stable.version, 1, "reflects the rollback");
    // env filter.
    assert_eq!(w.list(Some("prod")).unwrap().len(), 2);
    assert_eq!(w.list(Some("dev")).unwrap().len(), 0);

    // unpublish tombstones a triple.
    assert!(w.unpublish("orchestrate-core", "canary", "prod").unwrap());
    assert!(w
        .resolve("orchestrate-core", "canary", "prod")
        .unwrap()
        .is_none());
    assert!(!w.unpublish("orchestrate-core", "canary", "prod").unwrap());
    assert_eq!(w.list(None).unwrap().len(), 1);

    // Flush + merge + cold-load: bytes + bindings survive (incl. the rollback).
    w.flush_and_wait().unwrap();
    let _ = w.run_pending_merges().unwrap();
    let cold_store: Arc<dyn DurableSubstrate> = Arc::new(LocalFsSubstrate::new(&obj).unwrap());
    let cold = WasmStore::cold_load(cfg(&cold_local), cold_store).unwrap();
    let cm = cold
        .resolve("orchestrate-core", "stable", "prod")
        .unwrap()
        .unwrap();
    assert_eq!((cm.version, cm.bytes.as_slice()), (1, v1.as_slice()));
    assert!(cold
        .resolve("orchestrate-core", "canary", "prod")
        .unwrap()
        .is_none());
    assert_eq!(cold.list(None).unwrap().len(), 1);

    drop(cold);
    drop(w);
    for d in [&obj, &local, &cold_local] {
        let _ = std::fs::remove_dir_all(d);
    }
}

#[test]
fn wasm_survives_a_dead_replica() {
    let dirs: Vec<_> = (0..3).map(|i| unique_dir(&format!("r{i}"))).collect();
    let local = unique_dir("nlocal");
    let cold_local = unique_dir("ncold");
    let cfg = |root: &std::path::Path| WasmStore::config(root).with_seal_max_records(8);

    let mut w = WasmStore::open_replicated(cfg(&local), targets(&dirs)).unwrap();
    for i in 0..10u64 {
        w.publish(
            "drive",
            "stable",
            &format!("env{i}"),
            &wasm_blob(&format!("d{i}")),
        )
        .unwrap();
    }
    w.flush_and_wait().unwrap();
    drop(w);

    // Kill replica-0 — both the op-log parts AND the content-addressed bytes
    // are gone from r0; the survivors must serve both.
    std::fs::remove_dir_all(&dirs[0]).unwrap();
    let cold = WasmStore::cold_load_replicated(cfg(&cold_local), targets(&dirs)).unwrap();
    let m = cold.resolve("drive", "stable", "env7").unwrap().unwrap();
    assert_eq!(m.bytes, wasm_blob("d7"));
    assert_eq!(cold.list(None).unwrap().len(), 10);
    assert!(cold.engine().metrics().snapshot().read_fallbacks > 0);

    drop(cold);
    for d in dirs.iter().chain([&local, &cold_local]) {
        let _ = std::fs::remove_dir_all(d);
    }
}
