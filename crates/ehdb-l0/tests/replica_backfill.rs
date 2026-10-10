//! **Attaching a replica to an existing store replicates nothing that already
//! exists** — and the backfill that fixes it (noetl/ehdb#400).
//!
//! Uploads are enqueued at exactly one site, on seal. So a store that has been
//! running single-replica and then gains an off-box replica carries two
//! populations: parts sealed after the attach, which replicate, and every part
//! of real history, which never will. The only signal was
//! `parts_under_replicated` holding at a non-zero constant — indistinguishable
//! from an upload backlog draining slowly.
//!
//! The headline proof here is the one that actually matters for durability:
//! after the backfill, a cold load **from the new replica alone** reproduces
//! every record, including the ones written before that replica existed.

use std::sync::Arc;

use ehdb_l0::substrate::DurableSubstrate;
use ehdb_l0::{EventRecord, L0Config, L0EventLogEngine, LocalFsSubstrate, ReplicaTarget};

fn unique_dir(tag: &str) -> std::path::PathBuf {
    static N: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let n = N.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    std::env::temp_dir().join(format!("ehdb-l0-backfill-{tag}-{}-{n}", std::process::id()))
}

fn target(id: &str, dir: &std::path::Path) -> ReplicaTarget {
    let s: Arc<dyn DurableSubstrate> = Arc::new(LocalFsSubstrate::new(dir).unwrap());
    ReplicaTarget::new(id.to_string(), s)
}

fn part_objects(dir: &std::path::Path) -> Vec<String> {
    LocalFsSubstrate::new(dir)
        .unwrap()
        .list_prefix("parts/d1_event_log/")
        .unwrap()
}

fn cfg(root: &std::path::Path) -> L0Config {
    L0Config::d1(root)
        .with_shard_count(1)
        .with_granule_size(4)
        .with_seal_max_records(8)
}

/// Write 24 records (3 parts) to a single-replica store, then reopen it with a
/// second replica attached. Returns (local_root, r0, r1, expected_payloads).
fn history_then_attach(
    tag: &str,
) -> (
    std::path::PathBuf,
    std::path::PathBuf,
    std::path::PathBuf,
    Vec<String>,
) {
    let local = unique_dir(&format!("{tag}-local"));
    let r0 = unique_dir(&format!("{tag}-r0"));
    let r1 = unique_dir(&format!("{tag}-r1"));

    let mut origin =
        L0EventLogEngine::open_replicated(cfg(&local), vec![target("replica-0", &r0)]).unwrap();
    let mut expected = Vec::new();
    for i in 0..24u64 {
        origin
            .append("1001", &format!("t{i}"), format!("payload-{i}"))
            .unwrap();
        expected.push(format!("payload-{i}"));
    }
    origin.flush_and_wait_uploads().unwrap();
    assert_eq!(origin.manifest_snapshot().parts.len(), 3, "24/8 = 3 parts");
    drop(origin);

    assert_eq!(part_objects(&r0).len(), 3, "history landed on replica-0");
    assert!(part_objects(&r1).is_empty(), "replica-1 starts empty");
    (local, r0, r1, expected)
}

/// The defect, characterised: a seal-only enqueue cannot repair history, so
/// flushing (or restarting, or waiting) leaves every pre-existing part short.
#[test]
fn attaching_a_replica_leaves_every_existing_part_short_forever() {
    let (local, r0, r1, _) = history_then_attach("gap");

    let mut engine = L0EventLogEngine::open_replicated(
        cfg(&local),
        vec![target("replica-0", &r0), target("replica-1", &r1)],
    )
    .unwrap();

    // A flush is the strongest thing a caller can do short of a backfill: it
    // seals pending parts and waits for every outstanding upload.
    engine.flush_and_wait_uploads().unwrap();
    engine.refresh_state_gauges();

    let manifest = engine.manifest_snapshot();
    assert_eq!(manifest.parts.len(), 3);
    for p in &manifest.parts {
        assert_eq!(
            p.replica_count(),
            1,
            "part {} still single-copy after attach + flush",
            p.part_id
        );
    }
    assert_eq!(
        engine.metrics().snapshot().parts_under_replicated,
        3,
        "the only signal the gap gets"
    );
    assert!(
        part_objects(&r1).is_empty(),
        "nothing reached the newly attached replica"
    );
    // And the reason it can never improve on its own: there is no upload work
    // outstanding. The engine considers itself finished.
    assert_eq!(
        engine.metrics().snapshot().uploads,
        0,
        "this process uploaded nothing — history is not enqueued anywhere"
    );
}

/// The fix, end to end — including the cold-load-from-the-new-replica-alone
/// proof.
#[test]
fn backfill_replicates_history_and_the_new_replica_alone_can_serve_it() {
    let (local, r0, r1, expected) = history_then_attach("fix");

    let mut engine = L0EventLogEngine::open_replicated(
        cfg(&local),
        vec![target("replica-0", &r0), target("replica-1", &r1)],
    )
    .unwrap();

    let enqueued = engine.backfill_under_replicated().unwrap();
    assert_eq!(enqueued, 3, "all three pre-existing parts enqueued");
    engine.flush_and_wait_uploads().unwrap();
    engine.refresh_state_gauges();

    // Every part now has both copies, and the manifest kept the original one
    // rather than replacing it (`put_if_absent` → Ok(false) still yields a
    // location).
    let manifest = engine.manifest_snapshot();
    assert_eq!(manifest.parts.len(), 3);
    for p in &manifest.parts {
        assert_eq!(p.replica_count(), 2, "part {} two-way", p.part_id);
        let ids: std::collections::BTreeSet<_> =
            p.replicas.iter().map(|r| r.replica.as_str()).collect();
        assert_eq!(
            ids,
            ["replica-0", "replica-1"].into_iter().collect(),
            "part {} names both replicas",
            p.part_id
        );
    }
    let snap = engine.metrics().snapshot();
    assert_eq!(snap.parts_under_replicated, 0, "the gauge closed");
    assert_eq!(snap.backfill_uploads, 3, "counted as backfills");
    assert!(snap.backfill_upload_bytes > 0);
    // ⚠ The backfill must NOT pollute the seal→durable lag statistics: those
    // parts were sealed in a previous process and have no such interval.
    assert_eq!(
        snap.uploads, 0,
        "backfills are not counted as seal-path uploads"
    );
    assert_eq!(
        snap.upload_lag_micros_total, 0,
        "no fabricated seal→durable lag samples"
    );

    // The object count matches, counted from the substrate rather than from the
    // manifest that the backfill just wrote.
    assert_eq!(
        part_objects(&r1).len(),
        3,
        "replica-1 physically holds all 3"
    );
    assert_eq!(part_objects(&r0).len(), 3);
    drop(engine);

    // ⭐ The headline: lose the original replica entirely and cold-load from the
    // newly attached one ALONE. Every record written before replica-1 existed
    // must come back.
    std::fs::remove_dir_all(&r0).unwrap();
    let fresh_local = unique_dir("fix-coldload");
    let revived =
        L0EventLogEngine::cold_load_replicated(cfg(&fresh_local), vec![target("replica-1", &r1)])
            .unwrap();
    let got: Vec<String> = revived
        .replay_all()
        .unwrap()
        .into_iter()
        .map(|r: EventRecord| r.payload)
        .collect();
    assert_eq!(
        got, expected,
        "the new replica alone reproduces the full history"
    );
}

/// A single-replica set makes no spreading claim, so there is nothing to
/// backfill — and the call must not invent work.
#[test]
fn a_single_replica_set_backfills_nothing() {
    let local = unique_dir("single-local");
    let r0 = unique_dir("single-r0");
    let mut engine =
        L0EventLogEngine::open_replicated(cfg(&local), vec![target("replica-0", &r0)]).unwrap();
    for i in 0..16u64 {
        engine
            .append("1001", &format!("t{i}"), format!("p-{i}"))
            .unwrap();
    }
    engine.flush_and_wait_uploads().unwrap();
    assert_eq!(engine.backfill_under_replicated().unwrap(), 0);
    assert_eq!(engine.metrics().snapshot().backfill_uploads, 0);
}
