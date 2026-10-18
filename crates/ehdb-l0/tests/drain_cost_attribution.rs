//! noetl/ehdb#389 — ATTRIBUTE the drain cost, deterministically.
//!
//! The issue measured three facts and could not explain them: per-poll cost is
//! not fixed (7.3 µs at a 200 backlog vs 232.8 µs at 2,000 — 32x for 10x),
//! poll count does not dominate at part-sized batches (4 polls and 1 poll differ
//! by 0.1%), and total drain goes ~n^2.5 at limit 1 while staying linear at
//! limit >= 256. It recorded a hypothesis — cost is governed by how far the
//! cursor advances per poll — and deliberately did not assert it.
//!
//! The mechanism is in `L0Engine::read_partition_after_limited`:
//!
//! ```ignore
//! let start_offset = part.sparse_index.locate(after_seq + 1);
//! let len = part.byte_size.saturating_sub(start_offset);   // to the PART END
//! ```
//!
//! `len` ignores `limit`. Every poll reads from the cursor to the end of the part
//! and decodes every frame in that span, then keeps `limit` records. So a poll
//! costs the SPAN, not the batch — and a drain at limit 1 re-reads the tail once
//! per record.
//!
//! **FIXED** by reading and decoding incrementally and stopping at `limit`
//! (noetl/ehdb#389). These tests now pin the fixed behaviour from both sides:
//! the records returned are unchanged (an oracle compares every `(after, limit)`
//! against the full tail), and the bytes read are bounded by the batch rather
//! than the backlog.
//!
//! Measured, same harness that attributed the bug:
//!
//!     backlog   before      after
//!     128        66.5x       3.7x
//!     256       131.0x       3.7x
//!
//! The ratio is FLAT instead of doubling — the quadratic is gone. The residual
//! 3.7x is the sparse index's granule backfill plus chunk headroom: bounded,
//! and independent of how deep the backlog is.
//!
//! Asserted in BYTES, not time: a counter is deterministic and a timing
//! assertion in CI is a flake waiting to happen.

use std::sync::Arc;

use ehdb_l0::substrate::DurableSubstrate;
use ehdb_l0::{
    ChangeFeed, CountingSubstrate, D1EventLog, Dataset, L0Config, L0Engine, LocalFsSubstrate,
};

fn unique_dir(tag: &str) -> std::path::PathBuf {
    static N: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let n = N.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    std::env::temp_dir().join(format!("ehdb-drain-attr-{tag}-{}-{n}", std::process::id()))
}

fn cfg(root: &std::path::Path) -> L0Config {
    L0Config::d1(root)
        .with_shard_count(1)
        .with_granule_size(4)
        .with_seal_max_records(4_096)
}

/// Seed `n` records into an object store, then return a COLD engine over it:
/// cold-load parts carry no `local_path`, so every read goes through the
/// substrate and is therefore visible to `CountingSubstrate`.
fn seeded_cold(
    tag: &str,
    n: u64,
) -> (
    L0Engine<D1EventLog>,
    Arc<ehdb_l0::substrate::SubstrateCounters>,
    Vec<std::path::PathBuf>,
) {
    let (obj, warm, cold) = (
        unique_dir(&format!("{tag}-obj")),
        unique_dir(&format!("{tag}-warm")),
        unique_dir(&format!("{tag}-cold")),
    );
    {
        let store: Arc<dyn DurableSubstrate> = Arc::new(LocalFsSubstrate::new(&obj).unwrap());
        let mut engine = L0Engine::<D1EventLog>::open(cfg(&warm), store).unwrap();
        for i in 0..n {
            engine
                .append("1001", &format!("t{i}"), format!("payload-{i}"))
                .unwrap();
        }
        engine.flush_and_wait_uploads().unwrap();
    }
    let counting = CountingSubstrate::new(LocalFsSubstrate::new(&obj).unwrap());
    let counters = counting.counters();
    let store: Arc<dyn DurableSubstrate> = Arc::new(counting);
    let engine = L0Engine::<D1EventLog>::open(cfg(&cold), store).unwrap();
    (engine, counters, vec![obj, warm, cold])
}

fn drain_bytes(
    engine: &L0Engine<D1EventLog>,
    counters: &ehdb_l0::substrate::SubstrateCounters,
    limit: usize,
    expect: u64,
) -> u64 {
    use std::sync::atomic::Ordering::Relaxed;
    let before = counters.get_range_bytes.load(Relaxed);
    let mut feed = ChangeFeed::new(0, 0);
    let mut total = 0u64;
    loop {
        let batch = feed.poll_limited(engine, limit).unwrap();
        if batch.is_empty() {
            break;
        }
        total += batch.len() as u64;
    }
    assert_eq!(total, expect, "the drain must deliver everything");
    counters.get_range_bytes.load(Relaxed) - before
}

/// ⭐ EQUIVALENCE. Stopping early must not change what a poll returns.
///
/// The oracle is the function itself with an unbounded limit: for any cursor, the
/// correct answer for `limit` is the first `limit` of the full tail. This covers
/// the cases a bounded read could plausibly get wrong — empty, partial, exactly
/// the limit, a limit past the end, and a cursor past the end — and it is a real
/// oracle rather than a restatement, because the unbounded path reads a single
/// span and never exercises the chunk loop.
#[test]
fn a_bounded_read_returns_exactly_what_an_unbounded_one_would() {
    let (engine, _counters, dirs) = seeded_cold("equiv", 64);

    let full = engine
        .read_partition_after_limited(0, 0, usize::MAX)
        .unwrap();
    assert_eq!(full.len(), 64, "the oracle must see the whole tail");

    for after in [0u64, 1, 7, 31, 63, 64, 65, 1_000] {
        let expected_tail: Vec<_> = full
            .iter()
            .filter(|r| D1EventLog::sort_key(r) > after)
            .cloned()
            .collect();
        for limit in [1usize, 2, 3, 7, 16, 63, 64, 65, 128] {
            let got = engine
                .read_partition_after_limited(0, after, limit)
                .unwrap();
            let want: Vec<_> = expected_tail.iter().take(limit).cloned().collect();
            assert_eq!(
                got.iter().map(D1EventLog::sort_key).collect::<Vec<_>>(),
                want.iter().map(D1EventLog::sort_key).collect::<Vec<_>>(),
                "after={after} limit={limit}: same records, same ascending order"
            );
        }
    }
    for d in &dirs {
        let _ = std::fs::remove_dir_all(d);
    }
}

/// A `limit` of 0 returns nothing and reads nothing — the early return is kept.
#[test]
fn a_zero_limit_reads_nothing() {
    use std::sync::atomic::Ordering::Relaxed;
    let (engine, counters, dirs) = seeded_cold("zero", 32);
    let before = counters.get_range_bytes.load(Relaxed);
    assert!(engine
        .read_partition_after_limited(0, 0, 0)
        .unwrap()
        .is_empty());
    assert_eq!(
        counters.get_range_bytes.load(Relaxed) - before,
        0,
        "a zero limit must not touch the substrate"
    );
    for d in &dirs {
        let _ = std::fs::remove_dir_all(d);
    }
}

/// ⭐⭐ THE FIX, measured: the per-poll cost no longer grows with the backlog.
///
/// Before, this ratio doubled as the backlog doubled (66.5x at 128, 131.0x at
/// 256) because every poll re-read the remaining tail. It is now flat, so a
/// poll costs the batch it returns plus bounded index backfill.
#[test]
fn the_per_poll_cost_is_flat_across_backlogs() {
    let mut ratios = Vec::new();
    let mut keep = Vec::new();
    for n in [128u64, 256, 512] {
        let (engine, counters, dirs) = seeded_cold(&format!("flat{n}"), n);
        let whole = drain_bytes(&engine, &counters, 8_192, n);
        assert!(whole > 0, "the cold engine must read through the substrate");
        let one = drain_bytes(&engine, &counters, 1, n);
        let ratio = one as f64 / whole as f64;
        eprintln!("  {n} records: one batch = {whole} B, limit 1 = {one} B ({ratio:.2}x)");
        ratios.push(ratio);
        keep.push(dirs);
    }
    let (lo, hi) = (
        ratios.iter().cloned().fold(f64::MAX, f64::min),
        ratios.iter().cloned().fold(0.0, f64::max),
    );
    // Flat means the spread does not grow with n. A doubling-per-step bug would
    // put 4x between the ends of this sweep.
    assert!(
        hi / lo < 1.5,
        "the ratio must be flat across a 4x backlog sweep, got {ratios:?} \
         (spread {:.2}x) — a ratio that still grows means the span is still \
         being re-read",
        hi / lo
    );
    for dirs in &keep {
        for d in dirs {
            let _ = std::fs::remove_dir_all(d);
        }
    }
}

/// And in absolute terms: a one-record poll reads ~one record's worth, not the
/// whole tail. Pinned as a multiple of the mean frame size so it does not
/// hard-code a byte count that a payload change would invalidate.
#[test]
fn a_single_record_poll_reads_about_one_record() {
    use std::sync::atomic::Ordering::Relaxed;
    let (engine, counters, dirs) = seeded_cold("single", 512);
    let whole = drain_bytes(&engine, &counters, 8_192, 512);
    let mean_frame = whole / 512;

    let before = counters.get_range_bytes.load(Relaxed);
    let got = engine.read_partition_after_limited(0, 0, 1).unwrap();
    let read = counters.get_range_bytes.load(Relaxed) - before;
    assert_eq!(got.len(), 1);
    eprintln!("  one poll at limit 1 read {read} B; mean frame {mean_frame} B");
    assert!(
        read < mean_frame * 16,
        "a one-record poll read {read} B against a {mean_frame} B mean frame — \
         that is the whole tail again, not a bounded read"
    );
    for d in &dirs {
        let _ = std::fs::remove_dir_all(d);
    }
}

/// ⭐ The chunk-growth branch.
///
/// ⚠ My first version of this test used uniform 4 KB records and did NOT reach
/// the branch — a mutation removing the growth left it passing. The chunk is
/// sized from the part's MEAN frame size, so uniform large records give a large
/// chunk and the first read already contains one. Growth is only needed when a
/// record is much larger than the part's mean, i.e. MIXED sizes: the 64 small
/// records here keep the mean near 200 B while the one 8 KB record cannot fit in
/// the first chunk, so the loop must double and retry. Without that it spins or
/// returns short.
#[test]
fn a_record_larger_than_the_chunk_is_still_returned() {
    let (obj, warm, cold) = (
        unique_dir("big-obj"),
        unique_dir("big-warm"),
        unique_dir("big-cold"),
    );
    let big = "x".repeat(8192);
    {
        let store: Arc<dyn DurableSubstrate> = Arc::new(LocalFsSubstrate::new(&obj).unwrap());
        let mut engine = L0Engine::<D1EventLog>::open(cfg(&warm), store).unwrap();
        // Many small records keep the part's MEAN frame size low...
        for i in 0..64u64 {
            engine
                .append("1001", &format!("t{i}"), format!("small-{i}"))
                .unwrap();
        }
        // ...and one record far above it cannot fit the first chunk.
        engine.append("1001", "big", big.clone()).unwrap();
        for i in 0..8u64 {
            engine
                .append("1001", &format!("u{i}"), format!("after-{i}"))
                .unwrap();
        }
        engine.flush_and_wait_uploads().unwrap();
    }
    let counting = CountingSubstrate::new(LocalFsSubstrate::new(&obj).unwrap());
    let engine = L0Engine::<D1EventLog>::open(cfg(&cold), Arc::new(counting)).unwrap();

    // One at a time, and every record must come back in order.
    let mut seen = Vec::new();
    let mut after = 0u64;
    loop {
        let batch = engine.read_partition_after_limited(0, after, 1).unwrap();
        if batch.is_empty() {
            break;
        }
        assert_eq!(batch.len(), 1, "limit 1 must return exactly one record");
        after = D1EventLog::sort_key(&batch[0]);
        seen.push(after);
    }
    assert_eq!(
        seen.len(),
        73,
        "every record must be delivered, oversized one included"
    );
    let mut sorted = seen.clone();
    sorted.sort_unstable();
    assert_eq!(seen, sorted, "and in ascending order");

    for d in [&obj, &warm, &cold] {
        let _ = std::fs::remove_dir_all(d);
    }
}
