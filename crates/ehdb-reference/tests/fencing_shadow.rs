//! **Stale-writer fencing, shadow mode** (noetl/ehdb#330, F2).
//!
//! Invariant F says the *store* must refuse a write whose epoch is below the
//! highest it has accepted for that shard. This is that check, shipped in
//! **shadow**: a stale epoch is counted and logged and the write still lands, so
//! the live writer path is byte-for-byte unaffected while the counter is
//! observed.
//!
//! The three properties that matter, and each is pinned here:
//!   (a) a stale token is **detected and counted**,
//!   (b) in shadow mode the write **still succeeds**,
//!   (c) the enforce path **exists** and refuses — behind a flag that is off.

use std::sync::Arc;

use ehdb_reference::durable_eventlog_shared::{FilesystemSharedBackend, SharedSegmentBackend};
use ehdb_reference::fencing::{
    is_stale_epoch, FenceDecision, FencedSharedBackend, FencingLedger, FencingMetrics, FencingMode,
};

fn unique_dir(tag: &str) -> std::path::PathBuf {
    static N: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let n = N.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    std::env::temp_dir().join(format!(
        "ehdb-fence-{tag}-{}-{n}-{nanos}",
        std::process::id()
    ))
}

/// A store two writers share, each with its own epoch.
fn store(tag: &str) -> (std::path::PathBuf, std::path::PathBuf) {
    (
        unique_dir(&format!("{tag}-obj")),
        unique_dir(&format!("{tag}-fence")),
    )
}

fn writer(
    obj: &std::path::Path,
    fence: &std::path::Path,
    mode: FencingMode,
    epoch: u64,
    metrics: Arc<FencingMetrics>,
) -> FencedSharedBackend<FilesystemSharedBackend> {
    let inner = FilesystemSharedBackend::open(obj).unwrap();
    let ledger = FencingLedger::new(fence).unwrap();
    let b = FencedSharedBackend::new(inner, ledger)
        .with_mode(mode)
        .with_metrics(metrics);
    b.set_epoch(epoch);
    b
}

const SHARD: u32 = 0;

#[test]
fn a_fresh_store_accepts_any_epoch() {
    // Fencing must be introducible to a store that has never been fenced,
    // otherwise it could never be turned on for a live shard.
    let (obj, fence) = store("fresh");
    let m = FencingMetrics::new();
    let w = writer(&obj, &fence, FencingMode::Shadow, 7, m.clone());
    w.put_segment(SHARD, 1, b"hello").unwrap();
    assert_eq!(
        m.stale_observed.load(std::sync::atomic::Ordering::Relaxed),
        0
    );
    assert_eq!(
        m.epoch_advances.load(std::sync::atomic::Ordering::Relaxed),
        1
    );
}

#[test]
fn a_stale_epoch_is_detected_and_counted() {
    // (a) — the detection half.
    let (obj, fence) = store("detect");
    let m = FencingMetrics::new();

    // The new owner writes at epoch 5.
    let new_owner = writer(&obj, &fence, FencingMode::Shadow, 5, m.clone());
    new_owner.put_segment(SHARD, 1, b"from-epoch-5").unwrap();

    // The superseded writer, still at epoch 4, keeps going.
    let stale = writer(&obj, &fence, FencingMode::Shadow, 4, m.clone());
    stale.put_segment(SHARD, 2, b"from-epoch-4").unwrap();

    assert_eq!(
        m.stale_observed.load(std::sync::atomic::Ordering::Relaxed),
        1,
        "the store must notice it was written by a superseded epoch"
    );
}

#[test]
fn in_shadow_mode_the_stale_write_still_succeeds() {
    // (b) — the whole point of shadow. The live writer path is unaffected.
    let (obj, fence) = store("shadow-succeeds");
    let m = FencingMetrics::new();

    writer(&obj, &fence, FencingMode::Shadow, 5, m.clone())
        .put_segment(SHARD, 1, b"epoch-5")
        .unwrap();

    let stale = writer(&obj, &fence, FencingMode::Shadow, 4, m.clone());
    let outcome = stale.put_segment(SHARD, 2, b"epoch-4-payload");
    assert!(
        outcome.is_ok(),
        "shadow mode must NOT refuse: {:?}",
        outcome.err()
    );
    // And the bytes really landed — "succeeded" must mean written, not swallowed.
    assert_eq!(
        stale.get_segment(SHARD, 2).unwrap().as_deref(),
        Some(&b"epoch-4-payload"[..]),
        "the superseded writer's bytes are still in the store"
    );
    assert_eq!(
        m.stale_refused.load(std::sync::atomic::Ordering::Relaxed),
        0,
        "nothing is refused in shadow mode"
    );
    assert_eq!(
        m.stale_observed.load(std::sync::atomic::Ordering::Relaxed),
        1,
        "but it is counted — the gap between observed and refused is exactly \
         what flipping enforcement would change"
    );
}

#[test]
fn the_enforce_path_exists_and_refuses() {
    // (c) — the mechanism is real, not a stub that only counts.
    let (obj, fence) = store("enforce");
    let m = FencingMetrics::new();

    writer(&obj, &fence, FencingMode::Enforce, 5, m.clone())
        .put_segment(SHARD, 1, b"epoch-5")
        .unwrap();

    let stale = writer(&obj, &fence, FencingMode::Enforce, 4, m.clone());
    let err = stale
        .put_segment(SHARD, 2, b"epoch-4")
        .expect_err("enforce mode must refuse a stale epoch");
    assert!(is_stale_epoch(&err), "typed as a fencing refusal: {err}");
    assert!(
        stale.get_segment(SHARD, 2).unwrap().is_none(),
        "and the bytes must NOT have landed"
    );
    assert_eq!(
        m.stale_refused.load(std::sync::atomic::Ordering::Relaxed),
        1
    );
}

#[test]
fn enforce_still_accepts_the_current_epoch() {
    // ⚠ The positive control. Without it, a store that refused EVERYTHING would
    // pass the refusal test above and look correct.
    let (obj, fence) = store("enforce-positive");
    let m = FencingMetrics::new();

    let owner = writer(&obj, &fence, FencingMode::Enforce, 5, m.clone());
    owner.put_segment(SHARD, 1, b"first").unwrap();
    owner
        .put_segment(SHARD, 2, b"second")
        .expect("the current epoch must still be able to write");
    let newer = writer(&obj, &fence, FencingMode::Enforce, 6, m.clone());
    newer
        .put_segment(SHARD, 3, b"third")
        .expect("a NEWER epoch must be able to take over");
    assert_eq!(
        m.stale_refused.load(std::sync::atomic::Ordering::Relaxed),
        0
    );
}

#[test]
fn shadow_is_the_default_mode() {
    // ⚠ The safety claim of this whole change. Constructed WITHOUT calling
    // `.with_mode(..)`, so this is what a caller gets by default.
    let (obj, fence) = store("default");
    let inner = FilesystemSharedBackend::open(&obj).unwrap();
    let ledger = FencingLedger::new(&fence).unwrap();
    let b = FencedSharedBackend::new(inner, ledger);
    assert_eq!(b.mode(), FencingMode::Shadow);
    assert!(!b.mode().is_enforcing());
    assert_eq!(FencingMode::default(), FencingMode::Shadow);
    // Unrecognised config must fail SAFE (observe), never into refusing.
    assert_eq!(
        FencingMode::from_str_or_shadow("enforc"),
        FencingMode::Shadow
    );
    assert_eq!(FencingMode::from_str_or_shadow(""), FencingMode::Shadow);
    assert_eq!(
        FencingMode::from_str_or_shadow("enforce"),
        FencingMode::Enforce
    );
}

#[test]
fn every_mutating_method_is_fenced_not_just_the_two_named() {
    // ⚠ Fencing only put_segment/append_segment would leave a superseded writer
    // able to move the reclaim watermark or DELETE segments — strictly worse
    // than a stale append. This is the "two call sites is not coverage" check.
    let (obj, fence) = store("all-writes");
    let m = FencingMetrics::new();
    writer(&obj, &fence, FencingMode::Enforce, 5, m.clone())
        .put_segment(SHARD, 1, b"epoch-5")
        .unwrap();

    let stale = writer(&obj, &fence, FencingMode::Enforce, 4, m.clone());

    let put = stale.put_segment(SHARD, 9, b"x").unwrap_err();
    let app = stale.append_segment(SHARD, 1, 0, b"x").unwrap_err();
    let wm = stale.put_reclaim_watermark(SHARD, 1, 1).unwrap_err();
    let del = stale.delete_segment(SHARD, 1).unwrap_err();

    for (name, err) in [
        ("put_segment", put),
        ("append_segment", app),
        ("put_reclaim_watermark", wm),
        ("delete_segment", del),
    ] {
        assert!(is_stale_epoch(&err), "{name} was not fenced: {err}");
    }
    assert_eq!(
        m.stale_refused.load(std::sync::atomic::Ordering::Relaxed),
        4
    );
}

#[test]
fn reads_are_never_fenced() {
    // Fencing governs who may WRITE. A superseded node must still be able to
    // read — a non-owner cold-load is a legitimate, expected operation.
    let (obj, fence) = store("reads");
    let m = FencingMetrics::new();
    writer(&obj, &fence, FencingMode::Enforce, 5, m.clone())
        .put_segment(SHARD, 1, b"payload")
        .unwrap();

    let stale = writer(&obj, &fence, FencingMode::Enforce, 1, m.clone());
    assert_eq!(
        stale.get_segment(SHARD, 1).unwrap().as_deref(),
        Some(&b"payload"[..]),
        "a superseded node may still cold-load"
    );
    assert_eq!(stale.list_segment_ids(SHARD).unwrap(), vec![1]);
    assert_eq!(
        m.stale_observed.load(std::sync::atomic::Ordering::Relaxed),
        0,
        "reads must not even be counted as fencing events"
    );
}

#[test]
fn the_high_water_epoch_is_durable_across_instances() {
    // The ledger is the store's memory. If it lived only in RAM, restarting the
    // superseded writer would clear the very fact that fences it.
    let (obj, fence) = store("durable");
    let m = FencingMetrics::new();
    {
        let w = writer(&obj, &fence, FencingMode::Enforce, 9, m.clone());
        w.put_segment(SHARD, 1, b"a").unwrap();
    } // dropped — a process restart

    let ledger = FencingLedger::new(&fence).unwrap();
    assert_eq!(ledger.highest_epoch(SHARD).unwrap(), 9);

    let stale = writer(&obj, &fence, FencingMode::Enforce, 8, m.clone());
    assert!(is_stale_epoch(
        &stale.put_segment(SHARD, 2, b"b").unwrap_err()
    ));
}

#[test]
fn the_epoch_never_regresses() {
    let (_obj, fence) = store("monotonic");
    let ledger = FencingLedger::new(&fence).unwrap();

    assert_eq!(
        ledger.check_and_advance(SHARD, 3).unwrap(),
        FenceDecision::Fresh {
            epoch: 3,
            advanced: true
        }
    );
    // An equal epoch is legitimate (the same owner writing again) but does not
    // advance the marker.
    assert_eq!(
        ledger.check_and_advance(SHARD, 3).unwrap(),
        FenceDecision::Fresh {
            epoch: 3,
            advanced: false
        }
    );
    assert!(ledger.check_and_advance(SHARD, 2).unwrap().is_stale());
    assert_eq!(
        ledger.highest_epoch(SHARD).unwrap(),
        3,
        "a stale attempt must not lower the high-water mark"
    );
}

#[test]
fn shards_are_fenced_independently() {
    // A high epoch on shard 0 must not fence a legitimate writer on shard 1.
    let (_obj, fence) = store("per-shard");
    let ledger = FencingLedger::new(&fence).unwrap();
    ledger.check_and_advance(0, 10).unwrap();
    assert!(!ledger.check_and_advance(1, 2).unwrap().is_stale());
    assert_eq!(ledger.highest_epoch(0).unwrap(), 10);
    assert_eq!(ledger.highest_epoch(1).unwrap(), 2);
}

#[test]
fn the_metrics_are_present_at_zero() {
    // Prometheus prunes empty families, so an unpinned counter is absent until
    // it fires — "no stale writes" would then look like "no fencing at all".
    let m = FencingMetrics::new();
    let text = m.render_prometheus(FencingMode::Shadow);
    for series in [
        "ehdb_fencing_writes_checked_total 0",
        "ehdb_fencing_stale_observed_total 0",
        "ehdb_fencing_stale_refused_total 0",
        "ehdb_fencing_epoch_advances_total 0",
        "ehdb_fencing_enforcing 0",
    ] {
        assert!(text.contains(series), "missing `{series}` in:\n{text}");
    }
    assert!(m
        .render_prometheus(FencingMode::Enforce)
        .contains("ehdb_fencing_enforcing 1"));
}
