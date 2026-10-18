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
//! These tests assert it with BYTES READ rather than with time: a counter is
//! deterministic, and a timing assertion in CI is a flake waiting to happen.
//! They are characterisation tests — they pin today's behaviour so the fix is
//! measurable, and they do not claim the behaviour is correct.

use std::sync::Arc;

use ehdb_l0::substrate::DurableSubstrate;
use ehdb_l0::{ChangeFeed, CountingSubstrate, D1EventLog, L0Config, L0Engine, LocalFsSubstrate};

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

/// ⭐ The attribution: a poll costs the SPAN it must touch, not the batch it
/// returns. Draining one record at a time therefore re-reads the tail once per
/// record, and the bytes read are quadratic in the backlog.
#[test]
fn a_poll_costs_the_span_it_touches_not_the_batch_it_returns() {
    let (engine, counters, dirs) = seeded_cold("span", 256);

    // Positive control: the cold engine really does read through the substrate.
    // Without this, a zero would mean "no substrate reads happen at all" and
    // every comparison below would be vacuous.
    let whole = drain_bytes(&engine, &counters, 4_096, 256);
    assert!(
        whole > 0,
        "the cold-load engine must read parts through the substrate"
    );

    let one_at_a_time = drain_bytes(&engine, &counters, 1, 256);
    // Print the measurement, not just the verdict.
    eprintln!(
        "256 records: one batch = {whole} B, limit 1 = {one_at_a_time} B ({:.1}x)",
        one_at_a_time as f64 / whole as f64
    );

    assert!(
        one_at_a_time > whole * 10,
        "limit 1 re-reads the tail per record: {one_at_a_time} bytes vs {whole} \
         for the same 256 records in one batch"
    );
    for d in &dirs {
        let _ = std::fs::remove_dir_all(d);
    }
}

/// ⭐⭐ The shape, not just the direction: the penalty GROWS with the backlog.
///
/// If a poll's cost were a fixed overhead, doubling the backlog would double the
/// limit-1 bytes. Because each poll re-reads the remaining span, doubling the
/// backlog roughly QUADRUPLES them — so the ratio against a single-batch drain
/// doubles. This is the fact the issue measured as "32x for a 10x backlog" and
/// could not attribute.
#[test]
fn the_per_poll_penalty_grows_with_the_backlog() {
    let (small, c_small, d_small) = seeded_cold("small", 128);
    let (large, c_large, d_large) = seeded_cold("large", 256);

    let small_whole = drain_bytes(&small, &c_small, 4_096, 128);
    let small_one = drain_bytes(&small, &c_small, 1, 128);
    let large_whole = drain_bytes(&large, &c_large, 4_096, 256);
    let large_one = drain_bytes(&large, &c_large, 1, 256);

    let small_ratio = small_one as f64 / small_whole as f64;
    let large_ratio = large_one as f64 / large_whole as f64;

    // Doubling the backlog must roughly double the re-read ratio. A fixed
    // per-poll overhead would leave it flat.
    eprintln!(
        "128 records: {small_one} B vs {small_whole} B = {small_ratio:.1}x; \
         256 records: {large_one} B vs {large_whole} B = {large_ratio:.1}x"
    );
    assert!(
        large_ratio > small_ratio * 1.5,
        "the re-read ratio must grow with the backlog: {small_ratio:.1}x at 128 \
         records vs {large_ratio:.1}x at 256 — a flat ratio would mean the cost \
         is a fixed per-poll overhead, which is the reading the issue refuted"
    );
    for d in d_small.iter().chain(d_large.iter()) {
        let _ = std::fs::remove_dir_all(d);
    }
}
