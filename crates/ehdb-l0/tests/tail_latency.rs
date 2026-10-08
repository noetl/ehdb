//! **Tail latency — p50 / p90 / p99 / max for append and read.**
//!
//! Criterion reports mean and median with confidence intervals and **no percentiles**, so
//! the existing benchmarks cannot answer "how bad is the slow one". For an fsync-bound
//! append path the tail is the number that matters: a p50 of 3.7 ms with a p99 of 50 ms is
//! a different system from one with a p99 of 4 ms, and the two are indistinguishable in a
//! mean.
//!
//! An explicit sample vector, sorted, nearest-rank percentiles. No histogram bucketing,
//! because bucketing loses the max and the max is the interesting one.
//!
//! ⚠ This is a MEASUREMENT, not a threshold gate. It asserts only the things that must be
//! true for the numbers to mean anything — monotonic percentiles, a resolvable planted
//! delay, and a stated sample count. Asserting a latency bound here would make it a flaky
//! test on shared CI hardware, which is how a real measure gets deleted.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use ehdb_l0::substrate::DurableSubstrate;
use ehdb_l0::{shard_for_execution, Dataset, FlushPolicy, L0Config, L0Engine, LocalFsSubstrate};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct T {
    seq: u64,
    key: String,
}

struct TD;
impl Dataset for TD {
    type Record = T;
    const NAME: &'static str = "tail_d";
    fn sort_key(r: &T) -> u64 {
        r.seq
    }
    fn partition(r: &T, n: u32) -> u32 {
        shard_for_execution(&r.key, n)
    }
    fn index_key(r: &T) -> &str {
        &r.key
    }
    fn read_partition(k: &str, n: u32) -> u32 {
        shard_for_execution(k, n)
    }
}

fn dir(tag: &str) -> PathBuf {
    static N: AtomicU64 = AtomicU64::new(0);
    let n = N.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir().join(format!("ehdb-tail-{tag}-{}-{n}", std::process::id()))
}

fn engine(root: &Path) -> L0Engine<TD> {
    let obj = root.join("obj");
    let hot = root.join("hot");
    std::fs::create_dir_all(&obj).unwrap();
    std::fs::create_dir_all(&hot).unwrap();
    let s: Arc<dyn DurableSubstrate> = Arc::new(LocalFsSubstrate::new(&obj).unwrap());
    L0Engine::<TD>::open(L0Config::for_dataset(TD::NAME, &hot).with_shard_count(1), s).unwrap()
}

/// Nearest-rank percentile over an already-sorted slice.
fn pct(sorted: &[u128], p: f64) -> u128 {
    if sorted.is_empty() {
        return 0;
    }
    let rank = ((p / 100.0) * sorted.len() as f64).ceil() as usize;
    sorted[rank.saturating_sub(1).min(sorted.len() - 1)]
}

struct Summary {
    n: usize,
    p50: u128,
    p90: u128,
    p99: u128,
    max: u128,
}

fn summarize(mut v: Vec<u128>) -> Summary {
    v.sort_unstable();
    Summary {
        n: v.len(),
        p50: pct(&v, 50.0),
        p90: pct(&v, 90.0),
        p99: pct(&v, 99.0),
        max: *v.last().unwrap_or(&0),
    }
}

fn show(label: &str, s: &Summary) {
    // ⚠ n is printed with every line. A percentile over an unstated sample count is not a
    // percentile — a p99 over 50 samples is the single worst of 50, which is a max wearing
    // a percentile's name.
    println!(
        "    {label:<34} n={:<6} p50={:>9.1}us p90={:>9.1}us p99={:>9.1}us max={:>9.1}us",
        s.n,
        s.p50 as f64 / 1000.0,
        s.p90 as f64 / 1000.0,
        s.p99 as f64 / 1000.0,
        s.max as f64 / 1000.0
    );
}

#[test]
fn measure_append_and_read_tail_latency() {
    // 400 samples: enough that p99 spans 4 observations rather than resting on one, while
    // staying inside a sane test runtime at ~3.7 ms per fsync-bound append.
    const N: usize = 400;

    println!("\n  TAIL LATENCY (nearest-rank percentiles, {N} samples each)");

    // --- append, posture A (fsync per append) ---
    let d1 = dir("appA");
    let mut e = engine(&d1);
    e.append_record(T {
        seq: 0,
        key: "k".into(),
    })
    .unwrap();
    let mut appends = Vec::with_capacity(N);
    for i in 1..=N as u64 {
        let t = Instant::now();
        e.append_record(T {
            seq: i,
            key: "k".into(),
        })
        .unwrap();
        appends.push(t.elapsed().as_nanos());
    }
    let a = summarize(appends);
    show("append posture A (fsync/append)", &a);

    // --- append, group committed ---
    let d2 = dir("appG");
    let mut e2 = engine(&d2);
    e2.set_flush_policy(FlushPolicy::CallerDriven);
    e2.append_record(T {
        seq: 0,
        key: "k".into(),
    })
    .unwrap();
    for f in e2.take_sync_handles().unwrap() {
        f.sync_data().unwrap();
    }
    let mut gc = Vec::with_capacity(N);
    for i in 1..=N as u64 {
        let t = Instant::now();
        e2.append_record(T {
            seq: i,
            key: "k".into(),
        })
        .unwrap();
        gc.push(t.elapsed().as_nanos());
    }
    let g = summarize(gc);
    show("append group-committed (no sync)", &g);

    // --- read, below and above the seal boundary ---
    for len in [900u64, 1100] {
        let d = dir(&format!("rd{len}"));
        let mut er = engine(&d);
        for i in 1..=len {
            er.append_record(T {
                seq: i,
                key: "hot".into(),
            })
            .unwrap();
        }
        let mut reads = Vec::with_capacity(N);
        for _ in 0..N {
            let t = Instant::now();
            let got = er.read_index_after("hot", 0).unwrap();
            let el = t.elapsed().as_nanos();
            // Inside the timed region's assertion: a read returning nothing is fast and
            // worthless.
            assert_eq!(got.len() as u64, len);
            reads.push(el);
        }
        let r = summarize(reads);
        show(&format!("read_index_after len={len}"), &r);
        let _ = std::fs::remove_dir_all(&d);
    }

    // ---- invariants that make the numbers meaningful ----
    for (name, s) in [("append A", &a), ("group commit", &g)] {
        assert!(s.n == N, "{name}: sample count must be the stated one");
        assert!(s.p50 <= s.p90, "{name}: percentiles must be monotonic");
        assert!(s.p90 <= s.p99, "{name}: percentiles must be monotonic");
        assert!(s.p99 <= s.max, "{name}: p99 cannot exceed max");
    }
    // The group-committed path must be visibly cheaper at p50 — it skips the fsync. If it
    // is not, the posture was not applied and both columns describe the same thing.
    println!(
        "    => group commit p50 is {:.0}x cheaper than posture A p50",
        a.p50 as f64 / g.p50.max(1) as f64
    );
    assert!(
        g.p50 * 10 < a.p50,
        "group-committed append must be far cheaper at p50 than fsync-per-append, or the \
         flush posture did not take: {} vs {}",
        g.p50,
        a.p50
    );

    // ---- RESOLUTION CONTROL ----
    // Plant a known 5 ms delay into 2% of samples and confirm p99 moves while p50 does not.
    // Without this, a p99 that happens to equal p50 could mean "no tail" or "the percentile
    // code is wrong", and those are not the same.
    let mut planted: Vec<u128> = vec![1_000; 1000];
    for i in 0..20 {
        planted[i * 50] = 5_000_000; // 5 ms, 2% of samples
    }
    let p = summarize(planted);
    println!(
        "    CONTROL: planted 2% x 5ms into 1us samples -> p50={:.1}us p99={:.1}us max={:.1}us",
        p.p50 as f64 / 1000.0,
        p.p99 as f64 / 1000.0,
        p.max as f64 / 1000.0
    );
    assert_eq!(p.p50, 1_000, "a 2% tail must NOT move p50");
    assert_eq!(p.p99, 5_000_000, "a 2% tail MUST appear at p99");
    assert_eq!(p.max, 5_000_000, "and at max");

    let _ = std::fs::remove_dir_all(&d1);
    let _ = std::fs::remove_dir_all(&d2);
    let _ = Duration::from_secs(0);
}
