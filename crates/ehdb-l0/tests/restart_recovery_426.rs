//! noetl/ehdb#426 — acknowledged records must survive a restart.
//!
//! Three paths used to lose them:
//! - the active part after the first seal: the writer reopened `part-000000`,
//!   gone since that seal, and stranded the part holding the last records;
//! - a sealed part whose upload had not landed: the durable manifest lists only
//!   uploaded parts, so it vanished from the catalog;
//! - `reclaim_orphans` (run by every `apply_retention`), which then deleted
//!   that sealed part from disk.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;

use ehdb_core::{EhdbError, Result};
use ehdb_l0::dataset::{D1EventLog, EventRecord};
use ehdb_l0::engine::{L0Config, L0Engine};
use ehdb_l0::substrate::{DurableSubstrate, LocalFsSubstrate};

fn unique_dir(tag: &str) -> PathBuf {
    static N: AtomicU64 = AtomicU64::new(0);
    let n = N.fetch_add(1, Ordering::Relaxed);
    let d = std::env::temp_dir().join(format!("ehdb-426-restart-{tag}-{}-{n}", std::process::id()));
    let _ = std::fs::remove_dir_all(&d);
    d
}

/// A local substrate whose part uploads can be switched off — "the upload had
/// not landed when the process stopped".
struct Gated {
    inner: LocalFsSubstrate,
    uploads: AtomicBool,
}

impl DurableSubstrate for Gated {
    fn put_if_absent(&self, key: &str, bytes: &[u8]) -> Result<bool> {
        if !self.uploads.load(Ordering::SeqCst) && key.starts_with("parts/") {
            return Err(EhdbError::Storage("uploads off (test)".into()));
        }
        self.inner.put_if_absent(key, bytes)
    }
    fn put_overwrite(&self, key: &str, bytes: &[u8]) -> Result<()> {
        self.inner.put_overwrite(key, bytes)
    }
    fn get_range(&self, key: &str, offset: u64, len: u64) -> Result<Vec<u8>> {
        self.inner.get_range(key, offset, len)
    }
    fn get_all(&self, key: &str) -> Result<Vec<u8>> {
        self.inner.get_all(key)
    }
    fn exists(&self, key: &str) -> Result<bool> {
        self.inner.exists(key)
    }
    fn list_prefix(&self, prefix: &str) -> Result<Vec<String>> {
        self.inner.list_prefix(prefix)
    }
    fn delete(&self, key: &str) -> Result<()> {
        self.inner.delete(key)
    }
}

fn gated(root: &Path, uploads: bool) -> Arc<Gated> {
    Arc::new(Gated {
        inner: LocalFsSubstrate::new(root).unwrap(),
        uploads: AtomicBool::new(uploads),
    })
}

fn open(root: &Path, store: Arc<dyn DurableSubstrate>, seal_every: u64) -> L0Engine<D1EventLog> {
    let cfg = L0Config::d1(root.join("local"))
        .with_shard_count(1)
        .with_seal_max_records(seal_every);
    L0Engine::open(cfg, store).unwrap()
}

fn ev(seq: u64) -> EventRecord {
    EventRecord::new(seq, "exec-1", "t", "p").with_event_id(format!("e{seq}"))
}

fn seqs(e: &L0Engine<D1EventLog>) -> Vec<u64> {
    e.read_index_after("exec-1", 0)
        .unwrap()
        .into_iter()
        .map(|r| r.global_sequence)
        .collect()
}

fn shard_dir(root: &Path) -> PathBuf {
    root.join("local/parts/d1_event_log/shard-0")
}

#[test]
fn the_active_part_after_a_seal_is_recovered() {
    let root = unique_dir("active");
    let store: Arc<dyn DurableSubstrate> = gated(&root.join("obj"), true);
    {
        let mut e = open(&root, Arc::clone(&store), 2);
        for s in 1..=3 {
            e.append_record(ev(s)).unwrap();
        }
        // No flush: it would seal the active part. 1..=2 sealed; 3 sits in the active part, which is no longer id 0.
        assert!(shard_dir(&root).join("part-000001.active").exists());
    }
    let e = open(&root, store, 2);
    assert_eq!(
        seqs(&e),
        [1, 2, 3],
        "the record in part-000001.active was lost"
    );
    assert!(
        e.stranded_active().unwrap().is_empty(),
        "the live part must not read as stranded"
    );
}

#[test]
fn a_sealed_part_whose_upload_never_landed_survives_restart_and_retention() {
    let root = unique_dir("unuploaded");
    let off = gated(&root.join("obj"), false);
    {
        let mut e = open(&root, off.clone(), 2);
        for s in 1..=4 {
            e.append_record(ev(s)).unwrap();
        }
        e.flush_and_wait_uploads().unwrap();
    }
    // Uploads work again after the restart.
    off.uploads.store(true, Ordering::SeqCst);
    let mut e = open(&root, off.clone(), 2);
    assert_eq!(
        seqs(&e),
        [1, 2, 3, 4],
        "sealed-but-unuploaded parts vanished"
    );
    e.flush_and_wait_uploads().unwrap();
    // A retention pass that drops nothing of theirs must not delete them.
    e.apply_retention(1).unwrap();
    e.reclaim_orphans().unwrap();
    assert_eq!(seqs(&e), [1, 2, 3, 4]);
    drop(e);
    // And they are now durable: a cold load from the substrate alone sees them.
    let cold =
        L0Engine::<D1EventLog>::cold_load(L0Config::d1(root.join("cold")).with_shard_count(1), off)
            .unwrap();
    assert_eq!(
        seqs(&cold),
        [1, 2, 3, 4],
        "adopted parts were not re-uploaded"
    );
}

#[test]
fn reclaim_keeps_an_unreferenced_part_the_manifest_does_not_cover() {
    let root = unique_dir("reclaim-keep");
    let store = gated(&root.join("obj"), true);
    let mut e = open(&root, store.clone(), 2);
    for s in 1..=2 {
        e.append_record(ev(s)).unwrap();
    }
    e.flush_and_wait_uploads().unwrap();
    // A sealed part the manifest has never heard of, above everything it holds.
    let sealed = std::fs::read_dir(shard_dir(&root))
        .unwrap()
        .filter_map(|d| d.ok().map(|d| d.path()))
        .find(|p| p.extension().is_some_and(|x| x == "eslog"))
        .expect("a sealed part");
    let stray_local = shard_dir(&root).join(format!("shard-0-seq-{:020}-{:020}.eslog", 50, 60));
    std::fs::copy(&sealed, &stray_local).unwrap();
    let stray_key = format!(
        "parts/d1_event_log/shard-0/shard-0-seq-{:020}-{:020}.eslog",
        70, 80
    );
    store.put_if_absent(&stray_key, b"x").unwrap();
    // One the manifest DOES cover: a stale object inside the range [1, 2].
    let covered_key = format!(
        "parts/d1_event_log/shard-0/shard-0-seq-{:020}-{:020}.eslog",
        1, 1
    );
    store.put_if_absent(&covered_key, b"x").unwrap();
    // And a merge output that never made it into the manifest.
    let merged = shard_dir(&root).join("merged");
    std::fs::create_dir_all(&merged).unwrap();
    let merged_orphan = merged.join(format!("shard-0-seq-{:020}-{:020}.eslog", 1, 2));
    std::fs::write(&merged_orphan, b"x").unwrap();

    e.reclaim_orphans().unwrap();
    assert!(stray_local.exists(), "an uncovered local part was deleted");
    assert!(
        store.exists(&stray_key).unwrap(),
        "an uncovered object was deleted"
    );
    assert!(
        !store.exists(&covered_key).unwrap(),
        "a covered orphan object must still be reclaimed"
    );
    assert!(
        !merged_orphan.exists(),
        "an unswapped merge output must still be reclaimed"
    );
    assert_eq!(seqs(&e), [1, 2]);
}

/// Plant two active parts in `root`'s shard dir: an older incarnation's at id 5
/// holding `old_keys`, and the live one at id 2 holding `live_keys`. Ids are
/// chosen so the older part has the HIGHER id — ids restart per incarnation.
fn plant_two_incarnations(root: &Path, old_keys: &[u64], live_keys: &[u64]) {
    let dir = shard_dir(root);
    for (id, keys, tag) in [(5u32, old_keys, "old"), (2u32, live_keys, "live")] {
        let src = unique_dir(tag);
        {
            let mut e = open(&src, gated(&src.join("obj"), true), 1_000);
            for &k in keys {
                e.append_record(ev(k)).unwrap();
            }
        }
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::copy(
            shard_dir(&src).join("part-000000.active"),
            dir.join(format!("part-{id:06}.active")),
        )
        .unwrap();
    }
}

/// An older incarnation's active part whose range overlaps nothing is sealed
/// and recovered; the newest one stays live.
#[test]
fn an_older_active_part_that_overlaps_nothing_is_recovered() {
    let root = unique_dir("older-ok");
    let store: Arc<dyn DurableSubstrate> = gated(&root.join("obj"), true);
    plant_two_incarnations(&root, &[1, 2], &[10]);
    let mut e = open(&root, Arc::clone(&store), 2);
    assert_eq!(
        seqs(&e),
        [1, 2, 10],
        "the older incarnation's acked records were not recovered"
    );
    assert!(e.stranded_active().unwrap().is_empty());
    e.append_record(ev(11)).unwrap();
    e.flush_and_wait_uploads().unwrap();
    drop(e);
    let e = open(&root, store, 2);
    assert_eq!(seqs(&e), [1, 2, 10, 11]);
}

/// One that overlaps the live part is left exactly as it is, and no new
/// active part reuses its id.
#[test]
fn an_older_active_part_that_overlaps_is_left_alone() {
    let root = unique_dir("older-overlap");
    let store: Arc<dyn DurableSubstrate> = gated(&root.join("obj"), true);
    plant_two_incarnations(&root, &[12], &[10, 11, 13]);
    let dir = shard_dir(&root);
    let stale = std::fs::read(dir.join("part-000005.active")).unwrap();

    let mut e = open(&root, Arc::clone(&store), 2);
    assert_eq!(
        seqs(&e),
        [10, 11, 13],
        "the live part is the one with the newest records"
    );
    for s in 14..=18 {
        e.append_record(ev(s)).unwrap();
    }
    e.flush_and_wait_uploads().unwrap();
    assert_eq!(
        std::fs::read(dir.join("part-000005.active")).unwrap(),
        stale,
        "an overlapping older part was modified, or its id reused"
    );
    let stranded: Vec<PathBuf> = e
        .stranded_active()
        .unwrap()
        .into_iter()
        .map(|(p, _)| p)
        .collect();
    assert_eq!(stranded, vec![dir.join("part-000005.active")]);
}

/// `(min, max)` of every sealed part on local disk, from the `.eslog` names.
fn sealed_ranges(root: &Path) -> Vec<(u64, u64)> {
    let mut out: Vec<(u64, u64)> = std::fs::read_dir(shard_dir(root))
        .unwrap()
        .filter_map(|e| e.ok())
        .filter_map(|e| {
            let name = e.file_name().to_string_lossy().to_string();
            let stem = name.strip_suffix(".eslog")?;
            let mut it = stem.rsplit('-');
            let max: u64 = it.next()?.parse().ok()?;
            let min: u64 = it.next()?.parse().ok()?;
            Some((min, max))
        })
        .collect();
    out.sort();
    out
}

fn assert_disjoint(ranges: &[(u64, u64)]) {
    for w in ranges.windows(2) {
        assert!(
            w[0].1 < w[1].0,
            "sealed parts overlap: {:?} and {:?} (all: {ranges:?})",
            w[0],
            w[1]
        );
    }
}

/// The prod layout of 2026-11-04 (ai-meta#516 pre-flight): the live writer had
/// just sealed, so its active part was EMPTY, while an older incarnation's
/// active part — holding records below every sealed part — sat beside it.
///
/// An empty file carries no key, so "the active part with the newest records"
/// chose the stale one; the writer then appended today's records to a part that
/// began a day earlier, and its seal produced a part overlapping every part
/// sealed in between. A stranded part below the sealed range can never be the
/// live one.
#[test]
fn a_stale_active_part_below_the_sealed_range_is_never_resumed() {
    let root = unique_dir("stale-below");
    let store: Arc<dyn DurableSubstrate> = gated(&root.join("obj"), true);
    {
        let mut e = open(&root, Arc::clone(&store), 2);
        for s in 10..=13 {
            e.append_record(ev(s)).unwrap();
        }
        e.flush_and_wait_uploads().unwrap();
    }
    assert_eq!(sealed_ranges(&root), [(10, 11), (12, 13)]);
    // The older incarnation's part: higher id, records below the sealed range.
    let src = unique_dir("stale-below-src");
    {
        let mut e = open(&src, gated(&src.join("obj"), true), 1_000);
        for k in [5, 6] {
            e.append_record(ev(k)).unwrap();
        }
    }
    std::fs::copy(
        shard_dir(&src).join("part-000000.active"),
        shard_dir(&root).join("part-000007.active"),
    )
    .unwrap();

    let mut e = open(&root, Arc::clone(&store), 2);
    for s in 14..=16 {
        e.append_record(ev(s)).unwrap();
    }
    e.flush_and_wait_uploads().unwrap();
    assert_disjoint(&sealed_ranges(&root));
    assert_eq!(
        seqs(&e),
        [5, 6, 10, 11, 12, 13, 14, 15, 16],
        "acked records lost, or the stale part's records not recovered"
    );
    drop(e);
    let e = open(&root, store, 2);
    assert_eq!(seqs(&e), [5, 6, 10, 11, 12, 13, 14, 15, 16]);
    assert_disjoint(&sealed_ranges(&root));
}

/// Same layout, but the stale part overlaps a sealed part: it cannot be sealed
/// in place, so it must be left exactly as it is and reported — never resumed.
#[test]
fn a_stale_active_part_overlapping_a_sealed_part_is_left_and_reported() {
    let root = unique_dir("stale-overlap");
    let store: Arc<dyn DurableSubstrate> = gated(&root.join("obj"), true);
    {
        let mut e = open(&root, Arc::clone(&store), 2);
        for s in 10..=13 {
            e.append_record(ev(s)).unwrap();
        }
        e.flush_and_wait_uploads().unwrap();
    }
    let src = unique_dir("stale-overlap-src");
    {
        let mut e = open(&src, gated(&src.join("obj"), true), 1_000);
        e.append_record(ev(11)).unwrap();
    }
    let stale_path = shard_dir(&root).join("part-000007.active");
    std::fs::copy(shard_dir(&src).join("part-000000.active"), &stale_path).unwrap();
    let stale = std::fs::read(&stale_path).unwrap();

    let mut e = open(&root, Arc::clone(&store), 2);
    for s in 14..=16 {
        e.append_record(ev(s)).unwrap();
    }
    e.flush_and_wait_uploads().unwrap();
    assert_disjoint(&sealed_ranges(&root));
    assert_eq!(
        std::fs::read(&stale_path).unwrap(),
        stale,
        "the stale part was modified"
    );
    // Reported; anything else reported is an empty leftover from an earlier
    // seal (pre-existing, and harmless: it holds no record).
    let stranded = e.stranded_active().unwrap();
    assert!(stranded.iter().any(|(p, _)| *p == stale_path));
    assert!(
        stranded
            .iter()
            .all(|(p, len)| *p == stale_path || *len == 0),
        "{stranded:?}"
    );
    assert_eq!(seqs(&e), [10, 11, 12, 13, 14, 15, 16]);
}
