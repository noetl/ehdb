//! **The engine's metrics are scrapable, pinned at zero, and proven to move.**
//!
//! ⚠ WHAT WAS ACTUALLY WRONG. My first audit reported "`metrics.rs` exports 7 functions for
//! an 88-file engine" and filed that as the observability gap. **That count was wrong**: it
//! matched `pub fn` only, while **17 of the 20** incrementers are `pub(crate) fn`, and all
//! **27** metric fields are `pub`. A second pass also reported 4 fields as never written —
//! also wrong, a line-oriented grep missing `self.field\n    .fetch_add(..)`. Checked
//! multi-line-aware: **0 of 27 are unwritten.**
//!
//! The real gap was narrower and had nothing to do with counters missing: **27 good
//! counters that nothing could scrape.** `L0Metrics` was in-process only; the single
//! Prometheus exposition in the workspace lives in `ehdb-feed/src/scaler.rs` and covers
//! consumer lag, not the engine. So prod could not read any of it.
//!
//! This file guards the exposition three ways: the denominator is self-maintaining, every
//! series is pinned at 0, and the ones the lifecycle drives are proven to move.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use ehdb_l0::substrate::DurableSubstrate;
use ehdb_l0::{
    shard_for_execution, Dataset, L0Config, L0Engine, L0MetricsSnapshot, LocalFsSubstrate,
    MergePolicy,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct E {
    seq: u64,
    key: String,
}

struct ED;
impl Dataset for ED {
    type Record = E;
    const NAME: &'static str = "expo_d";
    fn sort_key(r: &E) -> u64 {
        r.seq
    }
    fn partition(r: &E, n: u32) -> u32 {
        shard_for_execution(&r.key, n)
    }
    fn index_key(r: &E) -> &str {
        &r.key
    }
    fn read_partition(k: &str, n: u32) -> u32 {
        shard_for_execution(k, n)
    }
}

fn dir(tag: &str) -> PathBuf {
    static N: AtomicU64 = AtomicU64::new(0);
    let n = N.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir().join(format!("ehdb-expo-{tag}-{}-{n}", std::process::id()))
}

fn engine(root: &Path) -> L0Engine<ED> {
    let obj = root.join("obj");
    let hot = root.join("hot");
    std::fs::create_dir_all(&obj).unwrap();
    std::fs::create_dir_all(&hot).unwrap();
    let s: Arc<dyn DurableSubstrate> = Arc::new(LocalFsSubstrate::new(&obj).unwrap());
    L0Engine::<ED>::open(
        L0Config::for_dataset(ED::NAME, &hot)
            .with_shard_count(1)
            .with_seal_max_records(16)
            .with_merge_policy(MergePolicy {
                small_part_max_records: 16,
                trigger_run_len: 4,
                max_merge_parts: 8,
            }),
        s,
    )
    .unwrap()
}

/// Parse the exposition into `name -> value`, ignoring `# HELP` / `# TYPE`.
fn parse(text: &str) -> std::collections::BTreeMap<String, f64> {
    let mut m = std::collections::BTreeMap::new();
    for line in text.lines() {
        if line.starts_with('#') || line.trim().is_empty() {
            continue;
        }
        let (lhs, rhs) = match line.rsplit_once(' ') {
            Some(x) => x,
            None => continue,
        };
        let name = lhs.split('{').next().unwrap_or(lhs).to_string();
        if let Ok(v) = rhs.parse::<f64>() {
            m.insert(name, v);
        }
    }
    m
}

// =========================================================================

/// ⚠⚠ **The denominator guard.** Adding a field to `L0MetricsSnapshot` without adding it to
/// `SERIES` must fail here. Without this the exposition silently stops covering the engine
/// as the engine grows, and a scrape that is missing a series looks exactly like a healthy
/// one.
///
/// The field list is read from the SOURCE rather than hard-coded, so the test cannot drift
/// from the struct it checks.
#[test]
fn every_snapshot_field_is_exported() {
    let src = include_str!("../src/metrics.rs");
    let start = src
        .find("pub struct L0MetricsSnapshot {")
        .expect("the snapshot struct must exist");
    let body = &src[start..];
    let end = body.find("\n}").expect("struct must terminate");
    let fields: Vec<&str> = body[..end]
        .lines()
        .filter_map(|l| {
            let l = l.trim();
            l.strip_prefix("pub ")
                .and_then(|r| r.split(':').next())
                .filter(|n| !n.is_empty() && n.chars().all(|c| c.is_ascii_lowercase() || c == '_'))
        })
        .collect();
    assert!(
        fields.len() >= 20,
        "parsed only {} fields from the struct — the parser is broken, and a parser that \
         finds nothing would make this test pass vacuously: {fields:?}",
        fields.len()
    );
    let exported: std::collections::BTreeSet<&str> =
        L0MetricsSnapshot::series_names().into_iter().collect();
    let declared: std::collections::BTreeSet<&str> = fields.iter().copied().collect();
    println!(
        "  snapshot fields={} exported series={}",
        declared.len(),
        exported.len()
    );
    let missing: Vec<_> = declared.difference(&exported).collect();
    let extra: Vec<_> = exported.difference(&declared).collect();
    assert!(
        missing.is_empty(),
        "these snapshot fields are NOT exported, so prod cannot read them: {missing:?}"
    );
    assert!(
        extra.is_empty(),
        "these series are exported but are not snapshot fields: {extra:?}"
    );
}

/// Pinned at zero: a fresh engine must emit **every** series, all at 0.
///
/// ⚠ This is the property `prometheus::Registry::gather` cannot give. It prunes metric
/// families with no children, so a labelled metric is *absent* until something increments
/// it — and an absent series and a healthy zero are indistinguishable to every alert. The
/// prod gateway once served a 200 with **zero bytes** for exactly that reason.
#[test]
fn a_fresh_engine_emits_every_series_pinned_at_zero() {
    let snap = L0MetricsSnapshot {
        appends: 0,
        dedupe_hits: 0,
        dedupe_window_evictions: 0,
        out_of_order_appends: 0,
        replica_domain_violations: 0,
        manifest_versions_pruned: 0,
        manifest_versions_retained: 0,
        ingest_append_failed: 0,
        ingest_decode_failed: 0,
        recovered_active_records: 0,
        seals: 0,
        uploads: 0,
        upload_bytes: 0,
        upload_lag_micros_total: 0,
        backfill_uploads: 0,
        backfill_upload_bytes: 0,
        tail_batches: 0,
        tail_records: 0,
        tail_bytes: 0,
        tail_replication_failed: 0,
        tail_recovered_records: 0,
        tail_objects_superseded: 0,
        tail_objects_reclaimed: 0,
        tail_reclaim_bytes: 0,
        tail_reclaim_failed: 0,
        tail_objects_retained: 0,
        merges: 0,
        parts_merged: 0,
        merged_bytes: 0,
        orphans_reclaimed: 0,
        orphan_bytes: 0,
        parts_dropped: 0,
        replica_writes: 0,
        read_fallbacks: 0,
        cold_loads: 0,
        reads: 0,
        parts_pruned: 0,
        parts_bloom_pruned: 0,
        parts_scanned: 0,
        // The four C6 state gauges. Listed explicitly rather than via
        // `..Default::default()` on purpose: adding a field must FORCE a decision here,
        // and a struct-update literal would silently default a new series to 0 and let
        // this test keep passing without anyone looking at it.
        manifest_parts: 0,
        parts_local_only: 0,
        parts_under_replicated: 0,
        dedupe_window_records: 0,
        records_superseded: 0,
    };
    let text = snap.render_prometheus("expo_d");
    let m = parse(&text);
    println!("  series emitted at zero: {}", m.len() - 2); // minus build_info + the derived mean
    for name in L0MetricsSnapshot::series_names() {
        let key = format!("ehdb_l0_{name}");
        let got = m.get(&key);
        assert_eq!(
            got,
            Some(&0.0),
            "{key} must be PRESENT and 0 on a fresh engine, not absent; got {got:?}"
        );
    }
    // build_info pinned at 1, so an absent series can be told from an old binary.
    assert_eq!(
        m.get("ehdb_l0_build_info"),
        Some(&1.0),
        "build_info must be pinned at 1"
    );
    assert!(
        text.contains("# TYPE ehdb_l0_appends counter"),
        "types must be declared: {text:.200}"
    );
    assert!(
        text.contains("dataset=\"expo_d\""),
        "the dataset label must be present so one process serving several is separable"
    );
}

/// RED-proven movement: drive real work and assert the series that must move DO.
///
/// ⚠ A series nothing increments is indistinguishable from a healthy system. A pin at 0 is
/// only trustworthy if something is known to move it off 0 — otherwise "pinned at zero" is
/// just "always zero".
#[test]
fn the_series_the_lifecycle_drives_are_proven_to_move() {
    let d = dir("move");
    let mut e = engine(&d);
    let before = e.metrics().snapshot();
    assert_eq!(
        before.appends, 0,
        "baseline must be zero, or 'moved' proves nothing"
    );

    for i in 1..=512u64 {
        e.append_record(E {
            seq: i,
            key: "k".into(),
        })
        .unwrap();
        if i % 16 == 0 {
            e.seal_aged_parts().ok();
            e.run_pending_merges().ok();
            e.reclaim_orphans().ok();
        }
    }
    for _ in 0..5 {
        e.read_index_after("k", 0).unwrap();
    }
    let after = e.metrics().snapshot();
    let text = after.render_prometheus(ED::NAME);
    let m = parse(&text);

    // Each of these is a claim that a specific code path ran. Naming them individually
    // rather than asserting "something moved" is the point: a single moving counter would
    // satisfy a vague assertion while 26 stayed inert.
    for (name, why) in [
        ("appends", "512 appends happened"),
        ("seals", "seal_max_records=16 over 512 appends must seal"),
        ("merges", "trigger_run_len=4 small parts must merge"),
        ("parts_merged", "a merge consumes parts"),
        ("reads", "5 reads happened"),
        (
            "manifest_versions_retained",
            "retention tracks the retained count",
        ),
    ] {
        let v = m.get(&format!("ehdb_l0_{name}")).copied().unwrap_or(-1.0);
        println!("    {name:<28} = {v}   ({why})");
        assert!(
            v > 0.0,
            "ehdb_l0_{name} must be > 0 after real work — {why}. A series that never moves \
             cannot be told from a healthy zero."
        );
    }

    // And the canary must NOT have moved: out-of-order appends are a defect signal, and a
    // defect signal that fires during a clean run is worse than one that never fires.
    let ooo = m
        .get("ehdb_l0_out_of_order_appends")
        .copied()
        .unwrap_or(-1.0);
    println!("    out_of_order_appends         = {ooo}   (must stay 0 on a clean run)");
    assert_eq!(
        ooo, 0.0,
        "the ascending-contract canary must not trip on an in-order run"
    );

    // ---- CONTROL: the parser must be able to see a non-zero ----
    // If `parse` were broken, every lookup above would read 0 or -1 and the test would fail
    // loudly rather than pass — but the reverse (a parser that returns a constant) would
    // pass. So assert the parser distinguishes two different values.
    assert!(
        m.get("ehdb_l0_appends").copied().unwrap_or(0.0) >= 512.0,
        "the parser must read the real value, not a constant: {:?}",
        m.get("ehdb_l0_appends")
    );
    assert_ne!(
        m.get("ehdb_l0_appends"),
        m.get("ehdb_l0_reads"),
        "two series with genuinely different values must parse differently, or the parser \
         is returning something constant"
    );
    let _ = std::fs::remove_dir_all(&d);
}
