//! **The mirror / drain path, measured** (noetl/ai-meta#455 B8).
//!
//! ⚠ WHY THIS FILE EXISTS. `ehdb-feed` is 36 `.rs` files and had **0 benchmarks**. The
//! networked publish path has an attribution harness (`examples/dispatch_bench.rs`, run by
//! hand), but the *drain* primitives every consumer runs in a hot loop had nothing — so
//! the cost of draining was unmeasured on the one crate whose job is delivery.
//!
//! # The batch axis is the one with prod history
//!
//! [ai-meta#344](https://github.com/noetl/ai-meta/issues/344) was **100% transport
//! timeouts** because the relay never batched: **0 of 80,264** fan-outs used a batch. The
//! fix was real, and the cost it avoided was never quantified. `poll_limited` takes the
//! batch limit as an argument, so the per-record cost at limit 1 versus 1024 is directly
//! measurable — and that ratio *is* what not batching costs.
//!
//! # Every instrument carries a resolution control
//!
//! `feed_poll_batch` sweeps the limit across three orders of magnitude. If per-record cost
//! comes out flat across that sweep, the instrument is not resolving batching at all and
//! no conclusion about #344's fix may be drawn from it — so the sweep is itself the
//! control, and the bench asserts inside the timed region that each poll returned the
//! number of records it claimed to.
//!
//! ⚠ A drain that returns nothing is infinitely fast. Every case below asserts the record
//! count it got, inside the measured closure, because "fast" and "returned no work" are the
//! same number otherwise.
//!
//! Run: `cargo bench -p ehdb-feed`. Numbers live in `docs/measures/l0-benchmarks.md`.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use ehdb_feed::{ShardConsumerGroup, SubjectConsumerGroup};
use ehdb_l0::substrate::DurableSubstrate;
use ehdb_l0::{ChangeFeed, D1EventLog, L0Config, L0Engine, LocalFsSubstrate};

fn dir(tag: &str) -> PathBuf {
    static N: AtomicU64 = AtomicU64::new(0);
    let n = N.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir().join(format!("ehdb-drain-bench-{tag}-{}-{n}", std::process::id()))
}

/// An engine with `n` records already in shard 0, ready to be drained.
fn seeded(tag: &str, n: u64) -> (L0Engine<D1EventLog>, PathBuf) {
    let root = dir(tag);
    let hot = root.join("hot");
    let obj = root.join("obj");
    std::fs::create_dir_all(&hot).unwrap();
    std::fs::create_dir_all(&obj).unwrap();
    let store: Arc<dyn DurableSubstrate> = Arc::new(LocalFsSubstrate::new(&obj).unwrap());
    let mut e =
        L0Engine::<D1EventLog>::open(L0Config::d1(&hot).with_shard_count(1), store).unwrap();
    for i in 0..n {
        e.append(&format!("k{i}"), "t", "cmd").unwrap();
    }
    (e, root)
}

fn cleanup(p: &Path) {
    let _ = std::fs::remove_dir_all(p);
}

/// **The #344 axis.** Per-record drain cost as a function of the batch limit.
///
/// Throughput is set to the record count so criterion reports per-record time directly —
/// the quantity that decides whether an un-batched relay can keep up.
fn feed_poll_batch(c: &mut Criterion) {
    const BACKLOG: u64 = 4_000;
    let mut g = c.benchmark_group("feed_poll_batch");
    let (engine, root) = seeded("batch", BACKLOG);

    // 4_000 == the whole backlog in ONE poll. It is the attribution control: if
    // per-poll cost were O(records returned), 4 polls of 1,024 would cost about the
    // same as 1 poll of 4,000. If instead each poll costs O(total backlog), the
    // single-poll case is several times cheaper and the batch curve above is
    // explained by poll COUNT rather than by batch size.
    for limit in [1usize, 16, 256, 1024, 4_000] {
        g.throughput(Throughput::Elements(BACKLOG));
        g.bench_with_input(BenchmarkId::from_parameter(limit), &limit, |b, &limit| {
            b.iter(|| {
                // Drain the whole backlog from cursor 0 in batches of `limit`.
                let mut feed = ChangeFeed::new(0, 0);
                let mut total = 0usize;
                loop {
                    let batch = feed.poll_limited(&engine, limit).unwrap();
                    if batch.is_empty() {
                        break;
                    }
                    assert!(
                        batch.len() <= limit,
                        "poll_limited returned {} for a limit of {limit}",
                        batch.len()
                    );
                    total += batch.len();
                }
                // ⚠ Inside the timed region: a drain that returns nothing is
                // infinitely fast and would read as the best possible result.
                assert_eq!(
                    total as u64, BACKLOG,
                    "the drain must deliver the whole backlog, not merely finish"
                );
                std::hint::black_box(total)
            });
        });
    }
    g.finish();
    cleanup(&root);
}

/// Drain cost as a function of backlog length — does it stay linear?
///
/// ⚠ **Drains in a loop, because one `poll` is capped.** `ChangeFeed::poll` uses
/// `default_batch_limit()` — **2,000** records (`EHDB_FEED_BATCH_LIMIT`), deliberately
/// bounded since unbounded is what caused noetl/ai-meta#298. A first version of this
/// function called `poll` once and asserted it got the whole backlog; at 10,000 it got
/// **2,000**, and the in-region assertion is the only reason that surfaced. Had it been
/// written without the check, the 10,000 case would have reported a comfortable time for
/// draining **20% of the backlog** — a short read is not a fast drain, and the two are the
/// same number.
fn feed_poll_vs_backlog(c: &mut Criterion) {
    let mut g = c.benchmark_group("feed_poll_vs_backlog");
    // Spans 100x, so a super-linear drain cannot hide inside the sweep.
    for n in [100u64, 1_000, 10_000] {
        let (engine, root) = seeded(&format!("bl{n}"), n);
        g.throughput(Throughput::Elements(n));
        g.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, &n| {
            b.iter(|| {
                let mut feed = ChangeFeed::new(0, 0);
                let mut total = 0u64;
                loop {
                    let batch = feed.poll(&engine).unwrap();
                    if batch.is_empty() {
                        break;
                    }
                    total += batch.len() as u64;
                }
                assert_eq!(total, n, "a short read is not a fast drain");
                std::hint::black_box(total)
            });
        });
        cleanup(&root);
    }
    g.finish();
}

/// The consumer-group hot loop: `poll_assign` + `ack`, per record.
///
/// This is what a worker actually runs. It is strictly more work than a raw feed poll —
/// at-least-once bookkeeping, an in-flight map insert and removal per record — so the
/// difference between this group and `feed_poll_vs_backlog` is the price of the delivery
/// guarantee.
fn group_drain(c: &mut Criterion) {
    const N: u64 = 2_000;
    let mut g = c.benchmark_group("group_drain");
    let (engine, root) = seeded("group", N);

    g.throughput(Throughput::Elements(N));
    g.bench_function("poll_assign_then_ack", |b| {
        b.iter(|| {
            let mut group = ShardConsumerGroup::<D1EventLog>::new(0, 1_000_000, 0);
            let mut drained = 0u64;
            while let Some(d) = group.poll_assign(&engine, 1, 0).unwrap() {
                assert!(group.ack(d.sort_key), "an assigned record must be ackable");
                drained += 1;
                if drained > N * 2 {
                    panic!("drain did not terminate: redelivery is firing with ack_wait set high");
                }
            }
            assert_eq!(drained, N, "the group must drain the whole backlog");
            assert_eq!(
                group.inflight_len(),
                0,
                "everything acked must leave nothing in flight"
            );
            std::hint::black_box(drained)
        });
    });

    // The same loop WITHOUT acking, to separate assignment cost from ack cost.
    g.throughput(Throughput::Elements(N));
    g.bench_function("poll_assign_only", |b| {
        b.iter(|| {
            let mut group = ShardConsumerGroup::<D1EventLog>::new(0, 1_000_000, 0);
            let mut drained = 0u64;
            while let Some(_d) = group.poll_assign(&engine, 1, 0).unwrap() {
                drained += 1;
                if drained >= N {
                    break;
                }
            }
            assert_eq!(drained, N, "assignment must cover the whole backlog");
            std::hint::black_box(drained)
        });
    });
    g.finish();
    cleanup(&root);
}

/// The subject-routed group: the same drain plus per-record subject matching.
fn subject_group_drain(c: &mut Criterion) {
    const N: u64 = 2_000;
    let mut g = c.benchmark_group("subject_group_drain");
    let (engine, root) = seeded("subject", N);

    g.throughput(Throughput::Elements(N));
    g.bench_function("drain_all_subjects", |b| {
        b.iter(|| {
            let mut group = SubjectConsumerGroup::<D1EventLog>::new(
                0,
                1_000_000,
                0,
                ehdb_feed::d1_command_subject(1),
            );
            let filter = ehdb_feed::SubjectFilter::all();
            let mut drained = 0u64;
            while let Some(d) = group.poll_assign(&engine, &filter, 1, 0).unwrap() {
                assert!(group.ack(d.sort_key));
                drained += 1;
                if drained > N * 2 {
                    panic!("drain did not terminate");
                }
            }
            assert_eq!(drained, N, "an All filter must deliver everything");
            std::hint::black_box(drained)
        });
    });
    g.finish();
    cleanup(&root);
}

/// **Attribution: is the cost of a tiny batch a FIXED per-poll overhead, or does each
/// poll cost O(backlog)?**
///
/// The two are indistinguishable from the batch sweep alone, and they imply different
/// fixes — a fixed overhead says "batch"; an O(backlog) poll says the read path
/// re-examines work it already delivered, which batching only hides.
///
/// This discriminates by holding the batch limit at **1** and varying the backlog. Cost is
/// reported **per poll** (throughput is set to the poll count), so:
///
/// * per-poll time ~flat across backlogs ⇒ fixed overhead per poll;
/// * per-poll time rising with backlog ⇒ each poll pays for the whole backlog.
fn feed_poll_per_poll_cost(c: &mut Criterion) {
    let mut g = c.benchmark_group("feed_poll_per_poll_cost");
    // 10x apart. Kept small because limit=1 makes this O(polls) calls.
    for n in [200u64, 2_000] {
        let (engine, root) = seeded(&format!("pp{n}"), n);
        // Elements = number of polls, so criterion reports time PER POLL.
        g.throughput(Throughput::Elements(n));
        g.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, &n| {
            b.iter(|| {
                let mut feed = ChangeFeed::new(0, 0);
                let mut polls = 0u64;
                let mut total = 0u64;
                loop {
                    let batch = feed.poll_limited(&engine, 1).unwrap();
                    if batch.is_empty() {
                        break;
                    }
                    polls += 1;
                    total += batch.len() as u64;
                }
                assert_eq!(total, n, "the drain must still deliver everything");
                assert_eq!(polls, n, "limit=1 must take exactly one poll per record");
                std::hint::black_box(polls)
            });
        });
        cleanup(&root);
    }
    g.finish();
}

criterion_group!(
    benches,
    feed_poll_batch,
    feed_poll_per_poll_cost,
    feed_poll_vs_backlog,
    group_drain,
    subject_group_drain
);
criterion_main!(benches);
