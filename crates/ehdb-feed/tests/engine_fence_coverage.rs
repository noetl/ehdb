//! The engine-chokepoint fence, end to end through `FeedWriter`.
//!
//! ⭐ These are the tests the previous fence could not have: `FencedSharedBackend`
//! decorated a trait the writer does not use, so no test through the real append
//! path could ever have exercised it. 624 appends and 290,440 commits went
//! through the prod writer with `writes_checked` at 0 (noetl/ai-meta#460).

use std::sync::Arc;

use ehdb_core::fence::{EpochFence, FenceLedger, FenceMetrics, FenceMode, MutationPath};
use ehdb_feed::FeedWriter;
use ehdb_l0::{D1EventLog, EventRecord, L0Config, L0Engine, LocalFsSubstrate};

fn tmp(name: &str) -> std::path::PathBuf {
    let p = std::env::temp_dir().join(format!(
        "ehdb-engine-fence-{name}-{}-{:?}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&p).unwrap();
    p
}

fn engine(dir: &std::path::Path) -> L0Engine<D1EventLog> {
    let substrate: Arc<dyn ehdb_l0::substrate::DurableSubstrate> =
        Arc::new(LocalFsSubstrate::new(dir).unwrap());
    L0Engine::<D1EventLog>::open(L0Config::d1(dir), substrate).unwrap()
}

fn record(id: u64) -> EventRecord {
    EventRecord::new(
        id,
        format!("exec-{id}"),
        "command",
        format!(r#"{{"id":{id}}}"#),
    )
}

/// ⭐ The denominator: a fenced writer taking real appends must move
/// `writes_checked`. This is what the old fence could never show.
#[test]
fn real_appends_are_actually_checked() {
    let dir = tmp("checked");
    let led = FenceLedger::new(dir.join(".fence")).unwrap();
    let metrics = FenceMetrics::new();
    let fence = EpochFence::new(FenceMode::Shadow, Some(led), Arc::clone(&metrics));
    fence.set_epoch(3);
    let w = Arc::new(FeedWriter::new_fenced(engine(&dir), Arc::clone(&fence), 0));

    assert_eq!(
        metrics
            .writes_checked
            .load(std::sync::atomic::Ordering::Relaxed),
        0
    );
    for i in 0..5 {
        w.append(record(i)).unwrap();
    }
    let checked = metrics
        .writes_checked
        .load(std::sync::atomic::Ordering::Relaxed);
    assert!(checked >= 5, "5 appends produced only {checked} checks");
    // ⚠ Per path, not in aggregate: a climbing total made only of appends would
    // hide seal and retention being unfenced.
    assert!(
        metrics.checked_for(MutationPath::AppendRecord) >= 5,
        "append_record path shows {}",
        metrics.checked_for(MutationPath::AppendRecord)
    );
    // ⭐ And the safety gate: the marker actually advanced, so the fence received
    // a real token rather than 0.
    assert_eq!(
        metrics
            .epoch_advances
            .load(std::sync::atomic::Ordering::Relaxed),
        1
    );
    std::fs::remove_dir_all(&dir).ok();
}

/// ⭐⭐ The positive control: a superseded writer's append is REFUSED, through
/// the real `FeedWriter::append` path.
#[test]
fn a_superseded_writer_is_refused_on_the_real_append_path() {
    let dir = tmp("refused");
    let ledger_root = dir.join(".fence");

    // A live writer at epoch 9 advances the marker.
    let live_fence = EpochFence::new(
        FenceMode::Enforce,
        Some(FenceLedger::new(&ledger_root).unwrap()),
        FenceMetrics::new(),
    );
    live_fence.set_epoch(9);
    let live = Arc::new(FeedWriter::new_fenced(engine(&dir), live_fence, 0));
    live.append(record(1)).expect("the holder may write");

    // The superseded writer: same shard, same ledger, lower epoch.
    let stale_dir = tmp("refused-stale");
    let metrics = FenceMetrics::new();
    let stale_fence = EpochFence::new(
        FenceMode::Enforce,
        Some(FenceLedger::new(&ledger_root).unwrap()),
        Arc::clone(&metrics),
    );
    stale_fence.set_epoch(8);
    let stale = Arc::new(FeedWriter::new_fenced(engine(&stale_dir), stale_fence, 0));

    let err = stale
        .append(record(2))
        .expect_err("a superseded writer must be refused");
    let msg = err.to_string();
    assert!(msg.contains("stale epoch"), "{msg}");
    assert!(
        msg.contains("append_record"),
        "the path must be named: {msg}"
    );
    assert_eq!(
        metrics
            .stale_refused
            .load(std::sync::atomic::Ordering::Relaxed),
        1
    );
    assert_eq!(metrics.refused_for(MutationPath::AppendRecord), 1);

    // ⚠ And the fence has been SEEN to refuse — a fence that has never once
    // refused is indistinguishable from one that cannot.
    std::fs::remove_dir_all(&dir).ok();
    std::fs::remove_dir_all(&stale_dir).ok();
}

/// ⚠ Shadow must let the same write through, or it would not be safe to arm
/// first.
#[test]
fn shadow_observes_the_same_write_without_refusing_it() {
    let dir = tmp("shadow");
    let ledger_root = dir.join(".fence");
    let hi = EpochFence::new(
        FenceMode::Shadow,
        Some(FenceLedger::new(&ledger_root).unwrap()),
        FenceMetrics::new(),
    );
    hi.set_epoch(9);
    let live = Arc::new(FeedWriter::new_fenced(engine(&dir), hi, 0));
    live.append(record(1)).unwrap();

    let stale_dir = tmp("shadow-stale");
    let metrics = FenceMetrics::new();
    let lo = EpochFence::new(
        FenceMode::Shadow,
        Some(FenceLedger::new(&ledger_root).unwrap()),
        Arc::clone(&metrics),
    );
    lo.set_epoch(8);
    let stale = Arc::new(FeedWriter::new_fenced(engine(&stale_dir), lo, 0));
    stale
        .append(record(2))
        .expect("shadow must NOT refuse — that is what makes it safe to arm");
    assert_eq!(
        metrics
            .stale_observed
            .load(std::sync::atomic::Ordering::Relaxed),
        1
    );
    assert_eq!(
        metrics
            .stale_refused
            .load(std::sync::atomic::Ordering::Relaxed),
        0
    );
    std::fs::remove_dir_all(&dir).ok();
    std::fs::remove_dir_all(&stale_dir).ok();
}

/// ⚠ An unfenced writer must be byte-identical to the old behaviour: no ledger,
/// no counters, nothing on disk.
#[test]
fn an_unfenced_writer_counts_nothing_and_writes_no_ledger() {
    let dir = tmp("unfenced");
    let w = Arc::new(FeedWriter::new(engine(&dir)));
    for i in 0..3 {
        w.append(record(i)).unwrap();
    }
    let eng = w.engine();
    let f = eng.fence();
    assert_eq!(f.mode(), FenceMode::Off);
    assert_eq!(
        f.metrics()
            .writes_checked
            .load(std::sync::atomic::Ordering::Relaxed),
        0
    );
    assert!(
        !dir.join(".fence").exists(),
        "an off fence must not create a ledger directory"
    );
    std::fs::remove_dir_all(&dir).ok();
}
