//! **Bounded manifest retention** (noetl/ehdb#344).
//!
//! Every manifest write emits a *full* snapshot of the manifest under
//! `manifest/<dataset>/manifest-v<version>.json`. Nothing has ever read one —
//! `LATEST` is the only manifest the engine loads — and until this policy
//! existed nothing deleted one either. The cost is therefore the product of two
//! growing quantities: each snapshot lists every part, so snapshot *size* grows
//! with part count, while snapshot *count* grows with write count.
//!
//! On prod that reached 6,770 snapshots totalling **19.4 GB** sitting behind
//! **71.8 MB** of actual command data. The volume hit 100%, every
//! `append_batch` began failing, and the ingest face dropped each publish
//! without a log — so command dispatch stopped platform-wide while the writer
//! still reported `Ready`.
//!
//! These tests pin the bound, and — just as importantly — pin that the bound is
//! what does the work: the negative control runs the identical workload with
//! retention disabled and asserts the pile grows.

use std::sync::Arc;

use ehdb_l0::substrate::DurableSubstrate;
use ehdb_l0::{D1EventLog, EventRecord, L0Config, L0Engine, LocalFsSubstrate, ReplicaTarget};

fn unique_dir(tag: &str) -> std::path::PathBuf {
    static N: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let n = N.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    std::env::temp_dir().join(format!(
        "ehdb-l0-manifest-{tag}-{}-{n}-{nanos}",
        std::process::id()
    ))
}

fn rec(seq: u64) -> EventRecord {
    EventRecord::new(
        seq,
        "exec-retain",
        format!("txn-{seq}"),
        format!("payload-{seq}"),
    )
}

/// One seal (and therefore one manifest version) per appended record.
fn open_engine(
    local: &std::path::Path,
    sub: &std::path::Path,
    retain: usize,
) -> L0Engine<D1EventLog> {
    let s: Arc<dyn DurableSubstrate> = Arc::new(LocalFsSubstrate::new(sub).unwrap());
    let config = L0Config::d1(local)
        .with_shard_count(1)
        .with_seal_max_records(1)
        .with_manifest_retain(retain);
    L0Engine::<D1EventLog>::open_replicated(config, vec![ReplicaTarget::new("replica-0", s)])
        .unwrap()
}

/// Count the versioned snapshots actually on disk (excludes `LATEST`).
fn snapshot_files(sub: &std::path::Path) -> Vec<String> {
    let dir = sub.join("manifest").join("d1_event_log");
    let Ok(rd) = std::fs::read_dir(&dir) else {
        return Vec::new();
    };
    let mut out: Vec<String> = rd
        .filter_map(|e| e.ok())
        .map(|e| e.file_name().to_string_lossy().to_string())
        .filter(|n| n.starts_with("manifest-v") && n.ends_with(".json"))
        .collect();
    out.sort();
    out
}

fn drive_versions(engine: &mut L0Engine<D1EventLog>, n: u64) {
    for seq in 1..=n {
        engine.append_record(rec(seq)).unwrap();
    }
    engine.flush_and_wait_uploads().unwrap();
}

const RETAIN: usize = 4;
const WRITES: u64 = 40;

#[test]
fn retention_bounds_the_snapshot_count() {
    // "Bounded" means the pile does not grow with the number of writes. Asserting
    // a single count against a single workload would also pass for a policy that
    // merely slowed growth down, so this runs the workload at two very different
    // sizes and compares.
    let (local, sub) = (unique_dir("bound-local"), unique_dir("bound-sub"));
    let mut engine = open_engine(&local, &sub, RETAIN);
    drive_versions(&mut engine, WRITES);
    let small = snapshot_files(&sub).len();
    drop(engine);

    let (local2, sub2) = (unique_dir("bound2-local"), unique_dir("bound2-sub"));
    let mut engine2 = open_engine(&local2, &sub2, RETAIN);
    drive_versions(&mut engine2, WRITES * 5);
    let large = snapshot_files(&sub2).len();

    // The fast path is best-effort (a concurrent writer can land a file after the
    // delete looked for it), so allow the sweep's worst case rather than pinning
    // an exact number — what must hold is that 5x the writes is not 5x the pile.
    let ceiling = 2 * RETAIN + 2;
    assert!(
        small <= ceiling && large <= ceiling,
        "retention must bound the pile independently of write count: {small} snapshots after \
         {WRITES} writes, {large} after {} — ceiling {ceiling}. This is the growth that filled \
         the prod volume.",
        WRITES * 5
    );
    assert!(
        large <= small + RETAIN,
        "5x the writes produced {large} snapshots vs {small} — that is growth, not a bound"
    );
    assert!(
        small > 0,
        "pruning to zero would mean the bound is really 'delete everything', which is a \
         different (and wrong) behaviour"
    );
}

#[test]
fn positive_control_without_retention_the_pile_grows() {
    // ⚠ The control that makes the test above mean something. Same workload,
    // retention disabled. If this ALSO came back bounded, the assertion above
    // would be measuring something other than the retention policy — a workload
    // that simply never wrote many versions, say.
    let (local, sub) = (unique_dir("ctl-local"), unique_dir("ctl-sub"));
    let mut engine = open_engine(&local, &sub, 0);
    drive_versions(&mut engine, WRITES);

    let files = snapshot_files(&sub);
    assert!(
        files.len() > RETAIN + 1,
        "with retention off the pile must grow — this is the pre-fix behaviour, and if it \
         does not reproduce here the bounded test proves nothing. got {} files",
        files.len()
    );
}

#[test]
fn latest_survives_pruning_and_the_store_still_opens() {
    // The one file that must never be a prune candidate. `LATEST` is what the
    // engine actually loads; deleting it would turn a disk-space fix into data
    // loss, so this asserts both that it is there and that it still works.
    let (local, sub) = (unique_dir("latest-local"), unique_dir("latest-sub"));
    let mut engine = open_engine(&local, &sub, RETAIN);
    drive_versions(&mut engine, WRITES);
    let parts_before = engine.manifest_snapshot().parts.len();
    drop(engine);

    let latest = sub.join("manifest").join("d1_event_log").join("LATEST");
    assert!(latest.exists(), "LATEST must never be pruned");
    let bytes = std::fs::read(&latest).unwrap();
    assert!(
        !bytes.is_empty(),
        "LATEST must still be readable after pruning"
    );

    // The real safety property: a cold open off the pruned store sees everything.
    let reopened = open_engine(&local, &sub, RETAIN);
    assert_eq!(
        reopened.manifest_snapshot().parts.len(),
        parts_before,
        "pruning superseded snapshots must not lose a single part — the surviving LATEST \
         is a complete manifest, not a delta"
    );
}

#[test]
fn a_backlog_written_before_the_policy_existed_converges() {
    // ⚠ The case a sliding window cannot handle on its own. The O(1) per-write
    // delete only ever evicts the version that just fell out of the window, so
    // an engine upgraded onto a store that already holds thousands of snapshots
    // would keep them forever. That is precisely prod's situation, so it gets a
    // test rather than an assumption.
    let (local, sub) = (unique_dir("backlog-local"), unique_dir("backlog-sub"));
    let mut engine = open_engine(&local, &sub, RETAIN);
    drive_versions(&mut engine, 2);
    drop(engine);

    let dir = sub.join("manifest").join("d1_event_log");
    let seed = std::fs::read(dir.join("LATEST")).unwrap();
    // Versions far below anything the sliding window will ever address.
    for v in 900_000..900_050u64 {
        std::fs::write(dir.join(format!("manifest-v{v:020}.json")), &seed).unwrap();
    }
    // Decoys: neither is a versioned snapshot, so neither may be deleted.
    std::fs::write(dir.join("manifest-vNOTANUMBER.json"), b"decoy").unwrap();
    std::fs::write(dir.join("unrelated.json"), b"decoy").unwrap();
    assert!(
        snapshot_files(&sub).len() >= 50,
        "backlog must actually be seeded"
    );

    let mut engine = open_engine(&local, &sub, RETAIN);
    drive_versions(&mut engine, WRITES);

    assert!(
        snapshot_files(&sub).len() <= RETAIN + 1,
        "the periodic sweep must converge a pre-existing pile, not just bound new growth; \
         got {:?}",
        snapshot_files(&sub)
    );
    assert!(
        dir.join("manifest-vNOTANUMBER.json").exists(),
        "a key that does not parse as a version must never be deleted — prune candidates \
         have to round-trip through the version parser"
    );
    assert!(
        dir.join("unrelated.json").exists(),
        "the prune must not touch keys outside the versioned-snapshot naming"
    );
    assert!(dir.join("LATEST").exists(), "LATEST survives the sweep too");
}

#[test]
fn pruning_is_counted() {
    // Absence of a metric must not be the only evidence that pruning happened.
    let (local, sub) = (unique_dir("metric-local"), unique_dir("metric-sub"));
    let mut engine = open_engine(&local, &sub, RETAIN);
    drive_versions(&mut engine, WRITES);

    let snap = engine.metrics().snapshot();
    assert!(
        snap.manifest_versions_pruned > 0,
        "the prune counter must move, otherwise a future regression that silently stops \
         pruning would look exactly like a workload that had nothing to prune"
    );
}
