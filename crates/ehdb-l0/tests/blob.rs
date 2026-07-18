//! D5 proof — content-addressed blob store + registry over immutable parts.

use std::sync::Arc;

use ehdb_l0::substrate::DurableSubstrate;
use ehdb_l0::{content_digest, BlobStore, LocalFsSubstrate, ReplicaTarget};

fn unique_dir(tag: &str) -> std::path::PathBuf {
    static N: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let n = N.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    std::env::temp_dir().join(format!("ehdb-l0-d5-{tag}-{}-{n}", std::process::id()))
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
fn put_content_address_get_prefix_delete_and_cold_load() {
    let obj = unique_dir("obj");
    let local = unique_dir("local");
    let cold_local = unique_dir("cold");
    let cfg = |root: &std::path::Path| BlobStore::config(root).with_seal_max_records(8);

    let store: Arc<dyn DurableSubstrate> = Arc::new(LocalFsSubstrate::new(&obj).unwrap());
    let mut b = BlobStore::open(cfg(&local), store).unwrap();

    let arrow = b"ARROW-IPC-bytes-...........".repeat(20);
    let result = b"result-tier-payload".repeat(50);

    // put content-addresses the bytes; identical bytes → identical digest (dedup).
    let d1 = b.put("state/exec-1/shard-0", &arrow).unwrap();
    let d1b = b.put("state/exec-2/shard-0", &arrow).unwrap();
    assert_eq!(d1, d1b, "identical bytes share a content digest (dedup)");
    assert_eq!(d1, content_digest(&arrow));
    let d2 = b.put("result/exec-1", &result).unwrap();
    assert_ne!(d1, d2);

    // get returns the exact bytes.
    assert_eq!(b.get("state/exec-1/shard-0").unwrap().unwrap(), arrow);
    assert_eq!(b.get("result/exec-1").unwrap().unwrap(), result);
    assert!(b.get("missing").unwrap().is_none());

    // prefix-list.
    let state = b.prefix_list("state/").unwrap();
    assert_eq!(
        state.iter().map(|(k, _)| k.clone()).collect::<Vec<_>>(),
        vec!["state/exec-1/shard-0", "state/exec-2/shard-0"]
    );
    assert!(state.iter().all(|(_, dg)| dg == &d1));

    // overwrite a key with new bytes → new digest.
    let arrow2 = b"ARROW-v2".repeat(30);
    let d1v2 = b.put("state/exec-1/shard-0", &arrow2).unwrap();
    assert_ne!(d1v2, d1);
    assert_eq!(b.get("state/exec-1/shard-0").unwrap().unwrap(), arrow2);

    // delete unmaps the key.
    assert!(b.delete("result/exec-1").unwrap());
    assert!(b.get("result/exec-1").unwrap().is_none());

    // Flush + merge + cold-load: blobs + registry survive.
    b.flush_and_wait().unwrap();
    let _ = b.run_pending_merges().unwrap();
    let cold_store: Arc<dyn DurableSubstrate> = Arc::new(LocalFsSubstrate::new(&obj).unwrap());
    let cold = BlobStore::cold_load(cfg(&cold_local), cold_store).unwrap();
    assert_eq!(cold.get("state/exec-1/shard-0").unwrap().unwrap(), arrow2);
    assert!(cold.get("result/exec-1").unwrap().is_none());
    assert_eq!(cold.prefix_list("state/").unwrap().len(), 2);

    drop(cold);
    drop(b);
    for d in [&obj, &local, &cold_local] {
        let _ = std::fs::remove_dir_all(d);
    }
}

#[test]
fn blobs_survive_a_dead_replica() {
    let dirs: Vec<_> = (0..3).map(|i| unique_dir(&format!("r{i}"))).collect();
    let local = unique_dir("nlocal");
    let cold_local = unique_dir("ncold");
    let cfg = |root: &std::path::Path| BlobStore::config(root).with_seal_max_records(8);

    let mut b = BlobStore::open_replicated(cfg(&local), targets(&dirs)).unwrap();
    for i in 0..10u64 {
        b.put(&format!("k{i}"), format!("blob-{i}").repeat(20).as_bytes())
            .unwrap();
    }
    b.flush_and_wait().unwrap();
    drop(b);

    // Kill replica-0 — both the registry parts AND the content-addressed bytes
    // are gone from r0; the survivors must serve both.
    std::fs::remove_dir_all(&dirs[0]).unwrap();
    let cold = BlobStore::cold_load_replicated(cfg(&cold_local), targets(&dirs)).unwrap();
    assert_eq!(
        cold.get("k7").unwrap().unwrap(),
        "blob-7".repeat(20).as_bytes()
    );
    assert_eq!(cold.prefix_list("k").unwrap().len(), 10);
    assert!(cold.engine().metrics().snapshot().read_fallbacks > 0);

    drop(cold);
    for d in dirs.iter().chain([&local, &cold_local]) {
        let _ = std::fs::remove_dir_all(d);
    }
}
