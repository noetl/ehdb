//! noetl/ai-meta#298 — the *measurement*: delivered bytes per poll, bounded vs not.
//!
//! The unit tests assert record counts.  This asserts the quantity that actually
//! killed prod: how many bytes one `poll` hands the consumer to hold at once.
//! Run with `--nocapture` to see the table.

use std::sync::Arc;

use ehdb_l0::substrate::DurableSubstrate;
use ehdb_l0::{ChangeFeed, D1EventLog, L0Config, L0Engine, LocalFsSubstrate};

fn unique_dir(tag: &str) -> std::path::PathBuf {
    static N: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let n = N.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    std::env::temp_dir().join(format!("ehdb-l0-bytes-{tag}-{}-{n}", std::process::id()))
}

/// Serialised size of the batch — exactly what `serve` puts in one frame and
/// what `read_frame` allocates on the other side.
fn frame_bytes(recs: &[ehdb_l0::EventRecord]) -> usize {
    serde_json::to_vec(recs).expect("serialise").len()
}

fn engine_with(n: u64, payload_bytes: usize, tag: &str) -> L0Engine<D1EventLog> {
    let obj = unique_dir(&format!("{tag}-obj"));
    let local = unique_dir(&format!("{tag}-local"));
    let cfg = L0Config::d1(&local).with_seal_max_records(256);
    let store: Arc<dyn DurableSubstrate> = Arc::new(LocalFsSubstrate::new(&obj).unwrap());
    let mut engine = L0Engine::<D1EventLog>::open(cfg, store).unwrap();
    let payload = "x".repeat(payload_bytes);
    for i in 0..n {
        engine.append(&format!("e{i}"), "t", payload.clone()).unwrap();
    }
    engine
}

/// The acceptance criterion, as a number: the delivered frame must be bounded by
/// the BATCH, not by the log.  Growing the log 4x must not grow the frame.
#[test]
fn the_delivered_frame_is_bounded_by_batch_not_by_log_size() {
    let limit = 500usize;
    let mut rows = Vec::new();

    for n in [1_000u64, 2_000, 4_000] {
        let engine = engine_with(n, 512, &format!("n{n}"));

        // Pre-#298 shape: one unbounded poll = the whole log in one frame.
        let unbounded = frame_bytes(&ChangeFeed::new(0, 0).poll_limited(&engine, usize::MAX).unwrap());

        // Post-fix: the largest single frame across a full bounded drain.
        let mut feed = ChangeFeed::new(0, 0);
        let mut worst = 0usize;
        let mut total = 0usize;
        loop {
            let b = feed.poll_limited(&engine, limit).unwrap();
            if b.is_empty() {
                break;
            }
            worst = worst.max(frame_bytes(&b));
            total += b.len();
        }
        assert_eq!(total as u64, n, "the bounded drain must deliver every record");
        rows.push((n, unbounded, worst));
    }

    println!("\n  log records | UNBOUNDED frame | BOUNDED worst frame | reduction");
    println!("  ------------+-----------------+---------------------+----------");
    for (n, u, w) in &rows {
        println!("  {:>11} | {:>13} B | {:>17} B | {:>6.1}x", n, u, w, *u as f64 / *w as f64);
    }
    println!();

    // The unbounded frame must grow with the log — that IS the bug.
    assert!(
        rows[2].1 > rows[0].1 * 3,
        "control: quadrupling the log must roughly quadruple the unbounded frame \
         (got {} -> {}); if this fails the fixture is not exercising the bug",
        rows[0].1,
        rows[2].1
    );

    // The bounded frame must NOT grow with the log — that is the fix.
    let (small, large) = (rows[0].2, rows[2].2);
    let drift = (large as f64 - small as f64).abs() / small as f64;
    assert!(
        drift < 0.05,
        "bounded frame must be independent of log size, but moved {:.1}% \
         ({} B at 1k records -> {} B at 4k)",
        drift * 100.0,
        small,
        large
    );
}
