//! noetl/ai-meta#298 — the feed delivers *bounded* batches.
//!
//! Before this, `ChangeFeed::poll` returned the whole tail after the cursor in
//! one `Vec`, which the feed then serialised into a single frame. At cursor 0 —
//! what every replay-from-0 boot asks for — that is the whole log. On prod it
//! reached 3313 MiB for 29,608 records against a 2 GiB limit and the system pool
//! OOM-crashlooped for twelve days.
//!
//! These are the assertions that would have caught it.

use std::sync::Arc;

use ehdb_l0::substrate::DurableSubstrate;
use ehdb_l0::{ChangeFeed, D1EventLog, L0Config, L0Engine, LocalFsSubstrate};

fn unique_dir(tag: &str) -> std::path::PathBuf {
    static N: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let n = N.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    std::env::temp_dir().join(format!("ehdb-l0-bounded-{tag}-{}-{n}", std::process::id()))
}

fn seqs(recs: &[ehdb_l0::EventRecord]) -> Vec<u64> {
    recs.iter().map(|r| r.global_sequence).collect()
}

/// Seal small so the records span many sealed parts plus the hot buffer — the
/// early-break path, not just the in-memory tail.
fn engine_with(n: u64, tag: &str) -> L0Engine<D1EventLog> {
    let obj = unique_dir(&format!("{tag}-obj"));
    let local = unique_dir(&format!("{tag}-local"));
    let cfg = L0Config::d1(&local).with_seal_max_records(7);
    let store: Arc<dyn DurableSubstrate> = Arc::new(LocalFsSubstrate::new(&obj).unwrap());
    let mut engine = L0Engine::<D1EventLog>::open(cfg, store).unwrap();
    for i in 0..n {
        engine
            .append(&format!("e{i}"), "t", format!("p{i}"))
            .unwrap();
    }
    engine
}

/// The regression itself: an unbounded poll hands back the entire log in one
/// batch. That batch is the buffer that OOMed prod.
#[test]
fn a_bounded_poll_does_not_deliver_the_whole_log() {
    let engine = engine_with(300, "whole-log");

    let unbounded = ChangeFeed::new(0, 0).poll(&engine).unwrap();
    assert_eq!(
        unbounded.len(),
        300,
        "control: the default limit is above this log, so it drains in one batch"
    );

    let mut feed = ChangeFeed::new(0, 0);
    let bounded = feed.poll_limited(&engine, 25).unwrap();
    assert_eq!(bounded.len(), 25, "a bounded poll must cap the batch");
    assert_eq!(
        feed.cursor(),
        bounded.last().map(|r| r.global_sequence).unwrap(),
        "the cursor must land on the last delivered record so the next poll resumes there"
    );
}

/// Bounding must be invisible to correctness: draining in bounded batches has to
/// reproduce the unbounded sequence exactly — no gaps, no repeats, no reorder.
/// This is the property that lets the batch limit be changed freely.
#[test]
fn a_bounded_drain_reproduces_the_unbounded_sequence_exactly() {
    let engine = engine_with(300, "equiv");

    let unbounded = seqs(&ChangeFeed::new(0, 0).poll(&engine).unwrap());

    let mut feed = ChangeFeed::new(0, 0);
    let mut drained: Vec<u64> = Vec::new();
    let mut batches = 0;
    loop {
        let b = feed.poll_limited(&engine, 13).unwrap();
        if b.is_empty() {
            break;
        }
        assert!(b.len() <= 13, "poll_limited returned {} records", b.len());
        batches += 1;
        drained.extend(seqs(&b));
    }

    assert_eq!(
        unbounded, drained,
        "bounded drain must equal the unbounded read"
    );
    assert!(
        batches > 1,
        "the fixture must actually span multiple batches"
    );
    assert_eq!(drained.len(), 300);
}

/// Resuming from a mid-log cursor stays bounded too — an outage backlog is
/// exactly when the old shape allocated most.
#[test]
fn resuming_from_a_cursor_is_also_bounded() {
    let engine = engine_with(300, "resume");
    let mut feed = ChangeFeed::new(0, 150);
    let b = feed.poll_limited(&engine, 20).unwrap();
    assert_eq!(b.len(), 20);
    assert!(
        b.iter().all(|r| r.global_sequence > 150),
        "a resumed poll must not redeliver below the cursor"
    );
}

/// A limit of 0 through the engine means "deliver nothing", and must not be
/// confused with the env-level 0 that means "unbounded".
#[test]
fn a_zero_limit_delivers_nothing_and_does_not_move_the_cursor() {
    let engine = engine_with(50, "zero");
    let mut feed = ChangeFeed::new(0, 0);
    assert!(feed.poll_limited(&engine, 0).unwrap().is_empty());
    assert_eq!(
        feed.cursor(),
        0,
        "an empty batch must not advance the cursor"
    );
}

/// If the effective default ever resolves to unbounded, the fix is inert and
/// #298 is back.  Asserted through `default_batch_limit()` rather than the
/// constant, so this is a real runtime check and not a constant-folded tautology
/// — and so it also covers the env-parse path landing on the bounded default.
#[test]
fn the_effective_default_is_bounded_and_actually_caps_a_poll() {
    let limit = ehdb_l0::feed::default_batch_limit();
    assert_ne!(
        limit,
        usize::MAX,
        "the shipped default must bound the batch"
    );
    assert!(limit > 0, "a default of 0 would wedge every consumer");

    // And prove the default is not merely a number: a log longer than it must
    // come back capped.
    let engine = engine_with((limit as u64) + 25, "default");
    let got = ChangeFeed::new(0, 0).poll(&engine).unwrap();
    assert_eq!(
        got.len(),
        limit,
        "poll() must apply the default limit, not deliver the whole log"
    );
}
