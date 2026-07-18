//! D7 proof — catalog register / versioned get / snapshot / deregister over
//! immutable parts.

use std::sync::Arc;

use ehdb_l0::substrate::DurableSubstrate;
use ehdb_l0::{CatalogKind, CatalogStore, LocalFsSubstrate, ReplicaTarget};

fn unique_dir(tag: &str) -> std::path::PathBuf {
    static N: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let n = N.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    std::env::temp_dir().join(format!("ehdb-l0-d7-{tag}-{}-{n}", std::process::id()))
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
fn register_version_snapshot_deregister_and_cold_load() {
    let obj = unique_dir("obj");
    let local = unique_dir("local");
    let cold_local = unique_dir("cold");
    let cfg = |root: &std::path::Path| CatalogStore::config(root).with_seal_max_records(8);

    let store: Arc<dyn DurableSubstrate> = Arc::new(LocalFsSubstrate::new(&obj).unwrap());
    let mut c = CatalogStore::open(cfg(&local), store).unwrap();

    // register assigns monotonic per-path versions.
    assert_eq!(
        c.register("system/auth", CatalogKind::Playbook, "v1-yaml")
            .unwrap(),
        1
    );
    assert_eq!(
        c.register("system/auth", CatalogKind::Playbook, "v2-yaml")
            .unwrap(),
        2
    );
    assert_eq!(
        c.register("tool/http", CatalogKind::Tool, "http-spec")
            .unwrap(),
        1
    );
    assert_eq!(
        c.register("resource/pg", CatalogKind::Resource, "pg-dsn-shape")
            .unwrap(),
        1
    );

    // get-latest resolves the newest version.
    let auth = c.get("system/auth").unwrap().unwrap();
    assert_eq!((auth.version, auth.content.as_str()), (2, "v2-yaml"));
    assert!(c.get("missing").unwrap().is_none());

    // pinned get resolves an older version exactly.
    assert_eq!(
        c.get_version("system/auth", 1).unwrap().unwrap().content,
        "v1-yaml"
    );
    assert!(c.get_version("system/auth", 9).unwrap().is_none());

    // snapshot lists live latest-per-path; kind filter narrows it.
    let all = c.snapshot(None).unwrap();
    assert_eq!(
        all.iter().map(|e| e.path.as_str()).collect::<Vec<_>>(),
        vec!["resource/pg", "system/auth", "tool/http"]
    );
    let tools = c.snapshot(Some(CatalogKind::Tool)).unwrap();
    assert_eq!(tools.len(), 1);
    assert_eq!(tools[0].path, "tool/http");

    // deregister tombstones the path; pinned reads of prior versions still work.
    assert_eq!(c.deregister("tool/http").unwrap(), Some(2));
    assert!(c.get("tool/http").unwrap().is_none());
    assert_eq!(
        c.get_version("tool/http", 1).unwrap().unwrap().content,
        "http-spec"
    );
    assert_eq!(
        c.snapshot(None).unwrap().len(),
        2,
        "deregistered path dropped"
    );

    // re-register revives at the next version.
    assert_eq!(
        c.register("tool/http", CatalogKind::Tool, "http-v2")
            .unwrap(),
        3
    );
    assert_eq!(c.get("tool/http").unwrap().unwrap().version, 3);

    // Flush + merge + cold-load: the full registry survives.
    c.flush_and_wait().unwrap();
    let _ = c.run_pending_merges().unwrap();
    let cold_store: Arc<dyn DurableSubstrate> = Arc::new(LocalFsSubstrate::new(&obj).unwrap());
    let cold = CatalogStore::cold_load(cfg(&cold_local), cold_store).unwrap();
    assert_eq!(cold.get("system/auth").unwrap().unwrap().content, "v2-yaml");
    assert_eq!(cold.get("tool/http").unwrap().unwrap().version, 3);
    assert_eq!(
        cold.get_version("system/auth", 1).unwrap().unwrap().content,
        "v1-yaml"
    );
    assert_eq!(cold.snapshot(None).unwrap().len(), 3);

    drop(cold);
    drop(c);
    for d in [&obj, &local, &cold_local] {
        let _ = std::fs::remove_dir_all(d);
    }
}

#[test]
fn catalog_survives_a_dead_replica() {
    let dirs: Vec<_> = (0..3).map(|i| unique_dir(&format!("r{i}"))).collect();
    let local = unique_dir("nlocal");
    let cold_local = unique_dir("ncold");
    let cfg = |root: &std::path::Path| CatalogStore::config(root).with_seal_max_records(8);

    let mut c = CatalogStore::open_replicated(cfg(&local), targets(&dirs)).unwrap();
    for i in 0..12u64 {
        c.register(
            &format!("pb/p{i}"),
            CatalogKind::Playbook,
            format!("body-{i}"),
        )
        .unwrap();
    }
    c.flush_and_wait().unwrap();
    drop(c);

    std::fs::remove_dir_all(&dirs[0]).unwrap();
    let cold = CatalogStore::cold_load_replicated(cfg(&cold_local), targets(&dirs)).unwrap();
    assert_eq!(cold.get("pb/p7").unwrap().unwrap().content, "body-7");
    assert_eq!(cold.snapshot(None).unwrap().len(), 12);
    assert!(cold.engine().metrics().snapshot().read_fallbacks > 0);

    drop(cold);
    for d in dirs.iter().chain([&local, &cold_local]) {
        let _ = std::fs::remove_dir_all(d);
    }
}
