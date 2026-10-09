//! Stranded active parts — [noetl/ehdb#396](https://github.com/noetl/ehdb/issues/396).
//!
//! A process appends records, acknowledges them, writes them into the active part file, and
//! exits before the part seals. The successor opens its own active path, so the abandoned
//! file is left behind: referenced by no manifest entry, reached by no read path, and — until
//! this change — deleted by nothing, because the orphan sweep collected `.eslog` only.
//!
//! Measured on a production volume before the fix: **9 orphans, 11.2 MiB, oldest 15 days
//! old**, accumulating one per restart.
//!
//! ⚠⚠⚠ The property that matters most here is NOT "stale files are deleted". It is **"the
//! live active file is not"** — deleting the file a writer is currently appending to would
//! destroy in-flight acknowledged records, which is strictly worse than the leak.

use std::sync::Arc;

use ehdb_l0::substrate::DurableSubstrate;
use ehdb_l0::{L0Config, L0EventLogEngine, LocalFsSubstrate};

fn unique_dir(tag: &str) -> std::path::PathBuf {
    static N: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let n = N.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    std::env::temp_dir().join(format!("ehdb-396-{tag}-{}-{n}", std::process::id()))
}

fn store(dir: &std::path::Path) -> Arc<dyn DurableSubstrate> {
    Arc::new(LocalFsSubstrate::new(dir).unwrap())
}

/// An engine with a live, un-sealed active part holding `n` records.
fn engine_with_active(
    local: &std::path::Path,
    objects: &std::path::Path,
    n: u64,
) -> L0EventLogEngine {
    // seal_max_records high enough that `n` appends do NOT seal — the point is to have a
    // live active part. A fixture that sealed would leave nothing to protect.
    let cfg = L0Config::d1(local).with_shard_count(1).with_seal_max_records(10_000);
    let mut e = L0EventLogEngine::open(cfg, store(objects)).unwrap();
    for i in 0..n {
        e.append("7001", &format!("t{i}"), format!("payload-{i}")).unwrap();
    }
    e
}

/// The shard directory the active files live in.
fn shard_dir(local: &std::path::Path) -> std::path::PathBuf {
    local.join("parts/d1_event_log/shard-0")
}

/// Plant a file that looks exactly like an abandoned active part from a dead process.
fn plant_stale(local: &std::path::Path, id: u32, bytes: usize) -> std::path::PathBuf {
    let dir = shard_dir(local);
    std::fs::create_dir_all(&dir).unwrap();
    let p = dir.join(format!("part-{id:06}.active"));
    std::fs::write(&p, vec![0xABu8; bytes]).unwrap();
    p
}

#[test]
fn a_stranded_active_part_is_surveyed_and_reclaimed() {
    let local = unique_dir("local");
    let objects = unique_dir("obj");
    let mut e = engine_with_active(&local, &objects, 5);

    let stale = plant_stale(&local, 19, 4096);
    assert!(stale.exists(), "fixture");

    // Survey first — it must SEE it, and must not delete it.
    let seen = e.stranded_active().unwrap();
    assert_eq!(seen.len(), 1, "expected exactly the planted stale file: {seen:?}");
    assert_eq!(seen[0].0, stale);
    assert_eq!(seen[0].1, 4096, "the survey must report the real byte size");
    assert!(stale.exists(), "the SURVEY must be read-only — it reported and deleted");

    // Then reclaim.
    let (files, bytes) = e.reclaim_stranded_active().unwrap();
    assert_eq!(files, 1);
    assert_eq!(bytes, 4096, "bytes freed must be the file's real size, not an estimate");
    assert!(!stale.exists(), "the stranded file was not reclaimed — the leak is still open");

    drop(e);
    let _ = std::fs::remove_dir_all(&local);
    let _ = std::fs::remove_dir_all(&objects);
}

/// ⚠⚠⚠ The safety invariant. If this ever fails, the fix is worse than the bug.
#[test]
fn the_live_active_part_is_never_reclaimed() {
    let local = unique_dir("local");
    let objects = unique_dir("obj");
    let mut e = engine_with_active(&local, &objects, 7);

    // Find the live active file: the one .active the engine itself is appending to.
    let live: Vec<_> = std::fs::read_dir(shard_dir(&local))
        .unwrap()
        .filter_map(|x| x.ok().map(|x| x.path()))
        .filter(|p| p.extension().map(|e| e == "active").unwrap_or(false))
        .collect();
    assert_eq!(live.len(), 1, "fixture: exactly one live active file, got {live:?}");
    let live = live[0].clone();
    let live_len = std::fs::metadata(&live).unwrap().len();
    assert!(live_len > 0, "fixture: the live active file must hold the appended records");

    let stale = plant_stale(&local, 44, 2048);

    let (files, bytes) = e.reclaim_stranded_active().unwrap();

    assert!(
        live.exists(),
        "⚠⚠⚠ THE LIVE ACTIVE PART WAS DELETED. That destroys acknowledged records that have \
         not sealed yet — strictly worse than the leak this function exists to fix."
    );
    assert_eq!(
        std::fs::metadata(&live).unwrap().len(),
        live_len,
        "the live active part was modified"
    );
    assert_eq!(files, 1, "only the stale file may be reclaimed");
    assert_eq!(bytes, 2048);
    assert!(!stale.exists());

    // And the engine still works afterwards: the records are readable and it can append on.
    let before = e.replay_all().unwrap().len();
    assert_eq!(before, 7, "the live records must survive the reclaim");
    e.append("7001", "after", "payload-after".to_string()).unwrap();
    assert_eq!(e.replay_all().unwrap().len(), 8, "the writer must still be usable");

    drop(e);
    let _ = std::fs::remove_dir_all(&local);
    let _ = std::fs::remove_dir_all(&objects);
}

/// Several orphans at once — the production shape (one per restart).
#[test]
fn many_stranded_parts_are_reclaimed_and_the_bytes_add_up() {
    let local = unique_dir("local");
    let objects = unique_dir("obj");
    let mut e = engine_with_active(&local, &objects, 3);

    let sizes = [1024usize, 2048, 4096, 8192];
    for (i, sz) in sizes.iter().enumerate() {
        plant_stale(&local, 10 + i as u32, *sz);
    }
    let expect: u64 = sizes.iter().map(|s| *s as u64).sum();

    let seen = e.stranded_active().unwrap();
    assert_eq!(seen.len(), 4, "{seen:?}");
    assert_eq!(seen.iter().map(|(_, b)| *b).sum::<u64>(), expect);

    let (files, bytes) = e.reclaim_stranded_active().unwrap();
    assert_eq!(files, 4);
    assert_eq!(bytes, expect, "the total must be the sum of the real file sizes");

    // Idempotent: nothing left, and a second call is not an error.
    assert_eq!(e.stranded_active().unwrap().len(), 0);
    assert_eq!(e.reclaim_stranded_active().unwrap(), (0, 0));

    drop(e);
    let _ = std::fs::remove_dir_all(&local);
    let _ = std::fs::remove_dir_all(&objects);
}

/// ⚠ A sealed `.eslog` must not be touched by this path — that is `reclaim_orphans`'
/// business, and it consults the manifest. Confusing the two would delete referenced data.
#[test]
fn sealed_parts_are_not_in_scope() {
    let local = unique_dir("local");
    let objects = unique_dir("obj");
    // Seal quickly so there IS a sealed part on disk.
    let cfg = L0Config::d1(&local).with_shard_count(1).with_seal_max_records(4);
    let mut e = L0EventLogEngine::open(cfg, store(&objects)).unwrap();
    for i in 0..12u64 {
        e.append("7002", &format!("t{i}"), format!("p-{i}")).unwrap();
    }
    e.flush_and_wait_uploads().unwrap();

    let eslogs: Vec<_> = std::fs::read_dir(shard_dir(&local))
        .unwrap()
        .filter_map(|x| x.ok().map(|x| x.path()))
        .filter(|p| p.extension().map(|x| x == "eslog").unwrap_or(false))
        .collect();
    assert!(!eslogs.is_empty(), "fixture: expected sealed parts on disk");

    let seen = e.stranded_active().unwrap();
    assert!(
        seen.iter().all(|(p, _)| p.extension().map(|x| x == "active").unwrap_or(false)),
        "the survey returned a non-.active path: {seen:?}"
    );
    e.reclaim_stranded_active().unwrap();
    for p in &eslogs {
        assert!(p.exists(), "a SEALED part was deleted by the active reclaim: {p:?}");
    }
    assert_eq!(e.replay_all().unwrap().len(), 12, "sealed records must survive");

    drop(e);
    let _ = std::fs::remove_dir_all(&local);
    let _ = std::fs::remove_dir_all(&objects);
}
