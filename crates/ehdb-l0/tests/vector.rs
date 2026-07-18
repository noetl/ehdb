//! D6 proof — vector upsert / top-k cosine / delete over immutable parts.

use std::sync::Arc;

use ehdb_l0::substrate::DurableSubstrate;
use ehdb_l0::{LocalFsSubstrate, ReplicaTarget, VectorStore};

fn unique_dir(tag: &str) -> std::path::PathBuf {
    static N: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let n = N.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    std::env::temp_dir().join(format!("ehdb-l0-d6-{tag}-{}-{n}", std::process::id()))
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
fn upsert_topk_delete_and_cold_load() {
    let obj = unique_dir("obj");
    let local = unique_dir("local");
    let cold_local = unique_dir("cold");
    let cfg = |root: &std::path::Path| VectorStore::config(root).with_seal_max_records(8);

    let store: Arc<dyn DurableSubstrate> = Arc::new(LocalFsSubstrate::new(&obj).unwrap());
    let mut v = VectorStore::open(cfg(&local), store).unwrap();

    // Two collections; unit-ish vectors in 3-D.
    v.upsert("docs", "p-x", vec![1.0, 0.0, 0.0]).unwrap();
    v.upsert("docs", "p-y", vec![0.0, 1.0, 0.0]).unwrap();
    v.upsert("docs", "p-xy", vec![0.9, 0.1, 0.0]).unwrap();
    v.upsert("other", "z", vec![0.0, 0.0, 1.0]).unwrap();

    // top-k for a query near the x axis → p-x first, then p-xy.
    let hits = v.top_k("docs", &[1.0, 0.0, 0.0], 2).unwrap();
    assert_eq!(hits.len(), 2);
    assert_eq!(hits[0].point_id, "p-x");
    assert_eq!(hits[1].point_id, "p-xy");
    assert!(hits[0].score > hits[1].score);
    // Collection isolation: "other"'s point isn't ranked against "docs".
    assert!(!hits.iter().any(|h| h.point_id == "z"));

    // upsert overwrites (latest wins): move p-xy toward y. p-y is the exact
    // match for a y-query, but the overwritten p-xy must now outrank p-x.
    v.upsert("docs", "p-xy", vec![0.1, 0.9, 0.0]).unwrap();
    let hy = v.top_k("docs", &[0.0, 1.0, 0.0], 3).unwrap();
    assert_eq!(hy[0].point_id, "p-y", "exact match ranks first");
    let rank: Vec<_> = hy.iter().map(|h| h.point_id.as_str()).collect();
    let (pxy, px) = (
        rank.iter().position(|&p| p == "p-xy"),
        rank.iter().position(|&p| p == "p-x"),
    );
    assert!(pxy < px, "the overwrite moved p-xy above p-x for a y-query");
    assert_eq!(
        v.get_point("docs", "p-xy").unwrap().unwrap(),
        vec![0.1, 0.9, 0.0]
    );

    // delete removes it from results.
    v.delete("docs", "p-xy").unwrap();
    let after = v.top_k("docs", &[0.0, 1.0, 0.0], 5).unwrap();
    assert!(
        !after.iter().any(|h| h.point_id == "p-xy"),
        "deleted point gone"
    );
    assert!(v.get_point("docs", "p-xy").unwrap().is_none());

    // Flush + merge + cold-load: the vector index survives.
    v.flush_and_wait().unwrap();
    let _ = v.run_pending_merges().unwrap();
    let cold_store: Arc<dyn DurableSubstrate> = Arc::new(LocalFsSubstrate::new(&obj).unwrap());
    let cold = VectorStore::cold_load(cfg(&cold_local), cold_store).unwrap();
    let ch = cold.top_k("docs", &[1.0, 0.0, 0.0], 5).unwrap();
    assert_eq!(ch[0].point_id, "p-x");
    assert!(
        !ch.iter().any(|h| h.point_id == "p-xy"),
        "delete survives cold-load"
    );

    drop(cold);
    drop(v);
    for d in [&obj, &local, &cold_local] {
        let _ = std::fs::remove_dir_all(d);
    }
}

#[test]
fn vectors_survive_a_dead_replica() {
    let dirs: Vec<_> = (0..3).map(|i| unique_dir(&format!("r{i}"))).collect();
    let local = unique_dir("nlocal");
    let cold_local = unique_dir("ncold");
    let cfg = |root: &std::path::Path| VectorStore::config(root).with_seal_max_records(8);

    let mut v = VectorStore::open_replicated(cfg(&local), targets(&dirs)).unwrap();
    for i in 0..12u64 {
        v.upsert("c", &format!("p{i}"), vec![i as f32, 1.0, 0.0])
            .unwrap();
    }
    v.flush_and_wait().unwrap();
    drop(v);

    std::fs::remove_dir_all(&dirs[0]).unwrap();
    let cold = VectorStore::cold_load_replicated(cfg(&cold_local), targets(&dirs)).unwrap();
    let hits = cold.top_k("c", &[11.0, 1.0, 0.0], 3).unwrap();
    assert_eq!(
        hits[0].point_id, "p11",
        "nearest to the query served from survivors"
    );
    assert!(cold.engine().metrics().snapshot().read_fallbacks > 0);

    drop(cold);
    for d in dirs.iter().chain([&local, &cold_local]) {
        let _ = std::fs::remove_dir_all(d);
    }
}
