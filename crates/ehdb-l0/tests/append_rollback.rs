//! noetl/ehdb#426 — a failed append leaves no trace, and a durable append never
//! reports failure because of what happened to the seal after it.
//!
//! The fault seams are process-global, so every test here takes `SEAM` first:
//! cargo runs the tests of one binary on parallel threads.

use ehdb_l0::dataset::{D1EventLog, EventRecord};
use ehdb_l0::engine::{L0Config, L0Engine};
use ehdb_l0::fault;
use ehdb_l0::substrate::{DurableSubstrate, LocalFsSubstrate};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};

static SEAM: Mutex<()> = Mutex::new(());
static N: AtomicU32 = AtomicU32::new(0);

fn dir(tag: &str) -> PathBuf {
    let n = N.fetch_add(1, Ordering::SeqCst);
    std::env::temp_dir().join(format!("ehdb-426-rb-{tag}-{}-{n}", std::process::id()))
}

fn open(root: &Path, seal_max_records: u64) -> L0Engine<D1EventLog> {
    let store: Arc<dyn DurableSubstrate> =
        Arc::new(LocalFsSubstrate::new(root.join("obj")).unwrap());
    let mut cfg = L0Config::d1(root.join("local"));
    cfg.seal_max_records = seal_max_records;
    L0Engine::open(cfg, store).unwrap()
}

fn ev(seq: u64, id: &str) -> EventRecord {
    EventRecord::new(seq, "exec-1", "t", "p").with_event_id(id)
}

fn ids(e: &L0Engine<D1EventLog>) -> Vec<String> {
    e.read_index_after("exec-1", 0)
        .expect("read")
        .into_iter()
        .map(|r| r.event_id.unwrap_or_default())
        .collect()
}

/// A write that put half a frame on disk is cut back out. Without the rollback
/// the half frame sits between two good ones and the restart that replays the
/// active part reads it as corruption — the whole shard fails to open.
#[test]
fn a_partial_write_is_rolled_back_and_the_part_still_recovers() {
    let _g = SEAM.lock().unwrap_or_else(|p| p.into_inner());
    let root = dir("partial");
    {
        let mut e = open(&root, 1024);
        e.append_record(ev(1, "a")).unwrap();
        fault::fail_next_writes_partway(1);
        assert!(e.append_record(ev(2, "b")).is_err());
        assert_eq!(fault::pending_injected_io_failures(), 0, "seam must fire");
        e.append_record(ev(3, "c")).unwrap();
        assert_eq!(ids(&e), ["a", "c"]);
    }
    let e = open(&root, 1024);
    assert_eq!(
        ids(&e),
        ["a", "c"],
        "restart must replay exactly the acked records"
    );
    let _ = std::fs::remove_dir_all(&root);
}

/// A sync that failed after the whole frame was written: the caller was told
/// the append failed, so the record must not come back on restart.
#[test]
fn a_failed_sync_is_rolled_back_so_restart_does_not_resurrect_it() {
    let _g = SEAM.lock().unwrap_or_else(|p| p.into_inner());
    let root = dir("sync");
    {
        let mut e = open(&root, 1024);
        e.append_record(ev(1, "a")).unwrap();
        fault::fail_next_syncs(1);
        assert!(e.append_record(ev(2, "b")).is_err());
        assert_eq!(fault::pending_injected_io_failures(), 0, "seam must fire");
        e.append_record(ev(3, "c")).unwrap();
    }
    let e = open(&root, 1024);
    assert_eq!(ids(&e), ["a", "c"], "a record reported failed came back");
    let _ = std::fs::remove_dir_all(&root);
}

/// The rolled-back writer's offsets still match the file, so a part sealed
/// after a failure reads back correctly through its sparse index.
#[test]
fn a_part_sealed_after_a_rollback_reads_back_through_its_index() {
    let _g = SEAM.lock().unwrap_or_else(|p| p.into_inner());
    let root = dir("seal");
    let mut e = open(&root, 4);
    e.append_record(ev(1, "a")).unwrap();
    fault::fail_next_writes_partway(1);
    assert!(e.append_record(ev(2, "b")).is_err());
    for (s, id) in [(3, "c"), (4, "d"), (5, "e")] {
        e.append_record(ev(s, id)).unwrap();
    }
    e.flush_and_wait_uploads().unwrap();
    assert_eq!(ids(&e), ["a", "c", "d", "e"]);
    drop(e);
    let e = open(&root, 4);
    assert_eq!(ids(&e), ["a", "c", "d", "e"]);
    let _ = std::fs::remove_dir_all(&root);
}

/// A failed append must not advance the shard tail: its retry is the same
/// record, not an out-of-order one.
#[test]
fn a_retried_append_is_not_counted_out_of_order() {
    let _g = SEAM.lock().unwrap_or_else(|p| p.into_inner());
    let root = dir("canary");
    let mut e = open(&root, 1024);
    e.append_record(ev(1, "a")).unwrap();
    fault::fail_next_syncs(1);
    assert!(e.append_record(ev(2, "b")).is_err());
    e.append_record(ev(2, "b")).unwrap();
    assert_eq!(e.metrics().snapshot().out_of_order_appends, 0);
    let _ = std::fs::remove_dir_all(&root);
}

/// A seal that fails after a durable append: the append reports success, the
/// failure is counted, and the writer keeps working and seals later.
#[cfg(unix)]
#[test]
fn a_failed_seal_does_not_fail_the_durable_append() {
    use std::os::unix::fs::PermissionsExt;
    let _g = SEAM.lock().unwrap_or_else(|p| p.into_inner());
    let root = dir("sealfail");
    let mut e = open(&root, 2);
    e.append_record(ev(1, "a")).unwrap();
    let shard_dir = root.join("local/parts/d1_event_log/shard-0");
    assert!(shard_dir.is_dir(), "test assumes {shard_dir:?}");
    // A read-only directory refuses the seal's rename; the open fd still writes.
    std::fs::set_permissions(&shard_dir, std::fs::Permissions::from_mode(0o555)).unwrap();
    let probe = shard_dir.join("probe");
    if std::fs::write(&probe, b"x").is_ok() {
        // Running as root: permissions do not bind, so the fault cannot be made.
        let _ = std::fs::remove_file(&probe);
        std::fs::set_permissions(&shard_dir, std::fs::Permissions::from_mode(0o755)).unwrap();
        eprintln!("skipped: directory permissions are not enforced for this user");
        return;
    }
    let appended = e.append_record(ev(2, "b"));
    std::fs::set_permissions(&shard_dir, std::fs::Permissions::from_mode(0o755)).unwrap();
    assert!(
        appended.is_ok(),
        "a durable append reported failure: {appended:?}"
    );
    assert_eq!(
        e.metrics().snapshot().seal_failed,
        1,
        "the seal failure must be counted"
    );
    // The writer still has its file: the next append lands and seals all three.
    e.append_record(ev(3, "c")).unwrap();
    e.flush_and_wait_uploads().unwrap();
    assert_eq!(ids(&e), ["a", "b", "c"]);
    drop(e);
    let e = open(&root, 2);
    assert_eq!(ids(&e), ["a", "b", "c"]);
    let _ = std::fs::remove_dir_all(&root);
}

/// Chain state describes only what reached the file.
#[cfg(feature = "chain-cert")]
#[test]
fn a_failed_append_is_not_absorbed_into_the_chain() {
    let _g = SEAM.lock().unwrap_or_else(|p| p.into_inner());
    let root = dir("chain");
    let store: Arc<dyn DurableSubstrate> =
        Arc::new(LocalFsSubstrate::new(root.join("obj")).unwrap());
    let cfg = L0Config::d1(root.join("local")).with_chain_cert(true);
    let mut e: L0Engine<D1EventLog> = L0Engine::open(cfg, store).unwrap();
    e.append_record(ev(1, "a")).unwrap();
    fault::fail_next_syncs(1);
    assert!(e.append_record(ev(2, "b")).is_err());
    e.append_record(ev(3, "c")).unwrap();
    assert_eq!(e.chain_absorbed(), 2);
    let _ = std::fs::remove_dir_all(&root);
}
