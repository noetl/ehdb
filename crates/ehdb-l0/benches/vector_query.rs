//! **P6: what a similarity query costs** (noetl/ai-meta#455).
//!
//! `VectorStore::top_k` is an **exact brute-force scan** — it materialises every live point
//! in the collection, scores all of them, sorts, truncates. So `recall@k` is 1.0 by
//! construction (see `tests/vector_recall.rs`), and **cost is the number that actually
//! bounds the design**: it is what decides the collection size at which an approximate
//! index stops being optional.
//!
//! Three axes, because they are independently controllable and one of them is not obvious:
//!
//! | axis | why |
//! | :-- | :-- |
//! | **live points** | the O(n) term everyone expects |
//! | **dimension** | the per-comparison term; `cosine` is O(d) |
//! | **op-log depth at constant live size** | `live_points` folds the **whole op log** for the collection, so an update-heavy collection costs more than its live size suggests |
//!
//! The third is the one worth measuring. A collection of 500 objects re-embedded ten times
//! holds 500 live points and 5,000 ops, and if cost tracks ops rather than live points then
//! **re-embedding is as expensive as growing** — which is a different operational story,
//! and the same shape as the manifest cost that grew with write count rather than data
//! size (noetl/ehdb#344).
//!
//! ⚠ Every case asserts the hit count **inside** the timed region. A query that returns
//! nothing is infinitely fast, and "fast" and "returned no work" are otherwise the same
//! number.
//!
//! Run: `cargo bench -p ehdb-l0 --bench vector_query`.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use ehdb_l0::substrate::DurableSubstrate;
use ehdb_l0::{LocalFsSubstrate, VectorStore};

fn dir(tag: &str) -> PathBuf {
    static N: AtomicU64 = AtomicU64::new(0);
    let n = N.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir().join(format!("ehdb-vbench-{tag}-{}-{n}", std::process::id()))
}

fn store(tag: &str) -> (VectorStore, PathBuf) {
    let root = dir(tag);
    let hot = root.join("hot");
    let obj = root.join("obj");
    std::fs::create_dir_all(&hot).unwrap();
    std::fs::create_dir_all(&obj).unwrap();
    let s: Arc<dyn DurableSubstrate> = Arc::new(LocalFsSubstrate::new(&obj).unwrap());
    let st = VectorStore::open(VectorStore::config(&hot).with_seal_max_records(512), s).unwrap();
    (st, root)
}

fn vec_for(seed: usize, dim: usize) -> Vec<f32> {
    (0..dim)
        .map(|i| (((i * 31 + seed * 17) % 101) as f32) / 101.0)
        .collect()
}

fn cleanup(p: &Path) {
    let _ = std::fs::remove_dir_all(p);
}

/// Cost vs **live points**, at a fixed dimension. Spans 50x.
fn query_vs_live_points(c: &mut Criterion) {
    const DIM: usize = 128;
    let mut g = c.benchmark_group("vector_query_vs_live_points");
    for n in [100usize, 1_000, 5_000] {
        let (mut st, root) = store(&format!("lp{n}"));
        for i in 0..n {
            st.upsert("c", &format!("p{i}"), vec_for(i, DIM)).unwrap();
        }
        let q = vec_for(7, DIM);
        // Elements = points scanned, so criterion reports per-point cost directly.
        g.throughput(Throughput::Elements(n as u64));
        g.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, _| {
            b.iter(|| {
                let hits = st.top_k("c", &q, 10).unwrap();
                assert_eq!(hits.len(), 10, "a query returning nothing is not fast");
                std::hint::black_box(hits.len())
            });
        });
        cleanup(&root);
    }
    g.finish();
}

/// Cost vs **dimension**, at a fixed live size. `cosine` is O(d), so this is the
/// per-comparison term. Spans 32x.
fn query_vs_dimension(c: &mut Criterion) {
    const N: usize = 1_000;
    let mut g = c.benchmark_group("vector_query_vs_dimension");
    for dim in [32usize, 128, 384, 1_024] {
        let (mut st, root) = store(&format!("d{dim}"));
        for i in 0..N {
            st.upsert("c", &format!("p{i}"), vec_for(i, dim)).unwrap();
        }
        let q = vec_for(7, dim);
        g.throughput(Throughput::Elements(N as u64));
        g.bench_with_input(BenchmarkId::from_parameter(dim), &dim, |b, _| {
            b.iter(|| {
                let hits = st.top_k("c", &q, 10).unwrap();
                assert_eq!(hits.len(), 10, "a query returning nothing is not fast");
                std::hint::black_box(hits.len())
            });
        });
        cleanup(&root);
    }
    g.finish();
}

/// ⭐ **The non-obvious axis: op-log depth at CONSTANT live size.**
///
/// 500 live points throughout; only the number of times each was re-upserted changes. If
/// cost tracks ops rather than live points, re-embedding a collection is as expensive as
/// growing it — and the fix is compaction, not an index.
fn query_vs_oplog_depth(c: &mut Criterion) {
    const DIM: usize = 128;
    const LIVE: usize = 500;
    let mut g = c.benchmark_group("vector_query_vs_oplog_depth");
    // The last row is the SAME 10x collection after `run_pending_merges`. If cost tracks
    // ops, compaction — not an approximate index — is the control, and this row is the
    // proof. If it does not move, compaction is not collapsing superseded ops and the
    // remedy lies elsewhere.
    for (label, rewrites, compact) in [
        ("1x500ops".to_string(), 1usize, false),
        ("4x500ops".to_string(), 4, false),
        ("10x500ops".to_string(), 10, false),
        ("10x500ops_merged".to_string(), 10, true),
    ] {
        let (mut st, root) = store(&format!("rw{label}"));
        for _ in 0..rewrites {
            for i in 0..LIVE {
                st.upsert("c", &format!("p{i}"), vec_for(i, DIM)).unwrap();
            }
        }
        if compact {
            st.flush_and_wait().unwrap();
            let merged = st.run_pending_merges().unwrap();
            println!("  (compaction ran {merged} merge(s) before the `{label}` row)");
        }
        let q = vec_for(7, DIM);
        // Elements = LIVE, held constant, so the reported per-element cost is
        // comparable across rows and any rise is the op-log term alone.
        g.throughput(Throughput::Elements(LIVE as u64));
        g.bench_with_input(
            BenchmarkId::from_parameter(label.clone()),
            &rewrites,
            |b, _| {
                b.iter(|| {
                    let hits = st.top_k("c", &q, 10).unwrap();
                    assert_eq!(hits.len(), 10, "a query returning nothing is not fast");
                    std::hint::black_box(hits.len())
                });
            },
        );
        cleanup(&root);
    }
    g.finish();
}

criterion_group!(
    benches,
    query_vs_live_points,
    query_vs_dimension,
    query_vs_oplog_depth
);
criterion_main!(benches);
