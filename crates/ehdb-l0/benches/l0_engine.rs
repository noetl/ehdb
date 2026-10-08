//! **The production L0 engine's benchmarks.**
//!
//! ⚠ WHY THIS FILE EXISTS. Measured on 2026-10-08, before it did:
//!
//! | crate | `.rs` files | bench files |
//! | :-- | --: | --: |
//! | **`ehdb-l0`** — the production engine | **88** | **0** |
//! | `ehdb-feed` | 36 | 0 |
//! | `ehdb-reference` — the *reference* model | 22 | 2 |
//! | `ehdb-service` / `-transaction` / `-catalog` / `-storage` | 3/3/2/2 | 1 each |
//!
//! **82% of the code had no benchmark, and the engine on the production path had none at
//! all, while the reference implementation had two.** That is the same shape as measuring
//! a tier that is not on the read path: the numbers were real and about the wrong thing.
//!
//! # Every instrument carries a resolution control
//!
//! A benchmark that cannot resolve the effect it is looking for reports a clean number and
//! a wrong conclusion. Each group below therefore includes a case with a **known, planted
//! difference** — a 10x payload, a 10x chain length — so the output itself shows whether
//! the instrument separates them. If the planted case does not differ, the measurement is
//! not evidence, whatever the headline figure says.
//!
//! Run: `cargo bench -p ehdb-l0`. Numbers and methodology live in
//! `docs/measures/l0-benchmarks.md`; re-measure rather than quoting that file.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use ehdb_l0::substrate::DurableSubstrate;
use ehdb_l0::{shard_for_execution, Dataset, FlushPolicy, L0Config, L0Engine, LocalFsSubstrate};
use serde::{Deserialize, Serialize};

/// A record whose payload size is a knob, so payload can be the planted effect.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct BenchRecord {
    seq: u64,
    key: String,
    payload: String,
}

struct BenchDataset;

impl Dataset for BenchDataset {
    type Record = BenchRecord;
    const NAME: &'static str = "bench_d";
    fn sort_key(r: &BenchRecord) -> u64 {
        r.seq
    }
    fn partition(r: &BenchRecord, shard_count: u32) -> u32 {
        shard_for_execution(&r.key, shard_count)
    }
    fn index_key(r: &BenchRecord) -> &str {
        &r.key
    }
    fn read_partition(k: &str, shard_count: u32) -> u32 {
        shard_for_execution(k, shard_count)
    }
}

fn unique_dir(tag: &str) -> PathBuf {
    static N: AtomicU64 = AtomicU64::new(0);
    let n = N.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir().join(format!("ehdb-l0-bench-{tag}-{}-{n}", std::process::id()))
}

fn engine(root: &Path) -> L0Engine<BenchDataset> {
    let sub: std::sync::Arc<dyn DurableSubstrate> =
        std::sync::Arc::new(LocalFsSubstrate::new(root).expect("substrate"));
    // One shard on purpose: the point is per-shard engine cost, not hash spreading.
    let cfg = L0Config::for_dataset(BenchDataset::NAME, root).with_shard_count(1);
    L0Engine::<BenchDataset>::open(cfg, sub).expect("engine")
}

fn rec(seq: u64, key: &str, payload_bytes: usize) -> BenchRecord {
    BenchRecord {
        seq,
        key: key.to_string(),
        payload: "x".repeat(payload_bytes),
    }
}

/// **Append throughput.** Reported as a RATE via `Throughput::Elements`, not a total —
/// a total over an unstated window is not a throughput.
///
/// ⚠ `iter_custom`, not `iter_batched`, and the reason is a measured failure. The first
/// version built a fresh engine and temp directory per batch, so SETUP dominated: it
/// reported **8.3 ms per append (~120/s)** for an in-memory append, and its own resolution
/// control FAILED — a 10x payload came out *faster* (7.79 ms vs 8.31 ms) with overlapping
/// confidence intervals. An instrument that cannot separate a 10x planted effect is not
/// measuring the thing it names, whatever figure it prints.
///
/// Here the engine is built ONCE outside the timer and `iters` appends are timed as a
/// block, so construction is amortised to nothing.
///
/// Resolution control, retained: `payload=64` vs `payload=640`. The 10x case must now cost
/// visibly more. If the two converge again, this instrument has stopped resolving payload
/// and no append figure from it is evidence.
fn append_throughput(c: &mut Criterion) {
    let mut g = c.benchmark_group("l0_append");
    for payload in [64usize, 640] {
        g.throughput(Throughput::Elements(1));
        g.bench_with_input(
            BenchmarkId::new("append_record", format!("payload_{payload}B")),
            &payload,
            |b, &payload| {
                b.iter_custom(|iters| {
                    let dir = unique_dir("app");
                    let mut e = engine(&dir);
                    // Warm the shard so the first-append path is not in the sample.
                    e.append_record(rec(0, "k", payload)).expect("warm");
                    let t0 = std::time::Instant::now();
                    for i in 1..=iters {
                        e.append_record(rec(i, "k", payload)).expect("append");
                    }
                    let d = t0.elapsed();
                    std::hint::black_box(&e);
                    let _ = std::fs::remove_dir_all(&dir);
                    d
                });
            },
        );
    }
    g.finish();
}

/// **Group commit** — the production write path, and the decomposition that makes the
/// per-append figure interpretable.
///
/// The engine is `fsync-per-append, posture A` by its own documentation, and `fault.rs`
/// cites "a ~4 ms `fsync`". The single-append measurement above lands at ~3.7 ms, which
/// independently confirms that figure rather than contradicting it — so posture A's
/// ~270 appends/s is the REAL number, not an instrument artifact.
///
/// `take_sync_handles` exists so a batch that arrived together pays **one** `fsync`
/// instead of one each. This measures that: N appends plus a single sync, reported
/// per-record. The ratio between this and the single-append case IS the fsync share, and
/// it is the number that decides whether a workload needs batching.
///
/// ⚠⚠ `FlushPolicy::CallerDriven` is REQUIRED and the first version of this bench omitted
/// it. Under the default `EveryAppend` every append already `fsync`s, so
/// `take_sync_handles` adds a SECOND sync rather than replacing N — per-record cost came
/// out FLAT across a 10x batch (192 → 184 elem/s) and I nearly published "EHDB's batching
/// does not amortise". The instrument was wrong, not the engine. `FeedWriter` sets this
/// posture; a bare `L0Engine` caller must too.
///
/// Resolution control: batch sizes spanning 10x (8 → 80). Per-record cost must FALL as the
/// batch grows, because the one fsync is amortised over more records. If it is flat, either
/// the posture was not set or the fsync is not where the cost is — and in both cases the
/// decomposition above would be wrong.
fn group_commit(c: &mut Criterion) {
    let mut g = c.benchmark_group("l0_group_commit");
    for batch in [8u64, 80] {
        g.throughput(Throughput::Elements(batch));
        g.bench_with_input(
            BenchmarkId::new("append_batch_then_one_sync", batch),
            &batch,
            |b, &batch| {
                b.iter_custom(|iters| {
                    let dir = unique_dir("gc");
                    let mut e = engine(&dir);
                    // Without this the default posture fsyncs every append and the batch sync
                    // is pure overhead — see the note above.
                    e.set_flush_policy(FlushPolicy::CallerDriven);
                    e.append_record(rec(0, "k", 64)).expect("warm");
                    for f in e.take_sync_handles().expect("warm handles") {
                        f.sync_data().expect("warm sync");
                    }
                    let mut seq = 0u64;
                    let t0 = std::time::Instant::now();
                    for _ in 0..iters {
                        for _ in 0..batch {
                            seq += 1;
                            e.append_record(rec(seq, "k", 64)).expect("append");
                        }
                        // ONE fsync for the whole batch — the point of the API.
                        for f in e.take_sync_handles().expect("handles") {
                            f.sync_data().expect("sync");
                        }
                    }
                    let d = t0.elapsed();
                    std::hint::black_box(&e);
                    let _ = std::fs::remove_dir_all(&dir);
                    d
                });
            },
        );
    }
    g.finish();
}

/// **Read-by-index cost against chain length.** `read_index_after(key, 0)` is the fold
/// input, so its cost IS the fold's input cost, and it is the number that decides whether
/// a long chain is affordable.
///
/// Resolution control: lengths span 10x twice (10 → 100 → 1000). The cost must be
/// monotonically increasing in length. A flat curve means the read is not touching the
/// records it claims to, and every absolute figure here would be meaningless.
fn read_index_vs_chain_length(c: &mut Criterion) {
    let mut g = c.benchmark_group("l0_read_index_vs_length");
    for len in [10u64, 100, 900, 1100, 4400] {
        let dir = unique_dir(&format!("read{len}"));
        let mut e = engine(&dir);
        for i in 1..=len {
            e.append_record(rec(i, "hot", 64)).expect("append");
        }
        g.throughput(Throughput::Elements(len));
        g.bench_with_input(BenchmarkId::new("read_index_after", len), &len, |b, _| {
            b.iter(|| {
                let got = e.read_index_after("hot", 0).expect("read");
                // Assert inside the measured closure: a read that returns nothing is fast
                // and worthless, and that is exactly how a benchmark reports a great
                // number for a broken path.
                assert_eq!(got.len() as u64, len);
                std::hint::black_box(got.len())
            });
        });
    }
    g.finish();
}

/// **Partition scan**, the follower/catch-up path, as a rate over records scanned.
fn partition_scan(c: &mut Criterion) {
    let mut g = c.benchmark_group("l0_partition_scan");
    for len in [100u64, 1000] {
        let dir = unique_dir(&format!("scan{len}"));
        let mut e = engine(&dir);
        for i in 1..=len {
            e.append_record(rec(i, &format!("k{}", i % 16), 64))
                .expect("append");
        }
        g.throughput(Throughput::Elements(len));
        g.bench_with_input(
            BenchmarkId::new("read_partition_after", len),
            &len,
            |b, _| {
                b.iter(|| {
                    let got = e.read_partition_after(0, 0).expect("scan");
                    assert_eq!(
                        got.len() as u64,
                        len,
                        "a scan that returns nothing is not fast"
                    );
                    std::hint::black_box(got.len())
                });
            },
        );
    }
    g.finish();
}

criterion_group!(
    benches,
    append_throughput,
    group_commit,
    read_index_vs_chain_length,
    partition_scan
);
criterion_main!(benches);
