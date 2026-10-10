//! **D2 — deleting tail objects a durable part already covers**
//! (noetl/ehdb#394).
//!
//! Without this, tail objects accumulate forever. With it done wrong, it
//! deletes the only off-box copy of an event. So the load-bearing test in this
//! file is not the one that proves deletion happens — it is
//! `a_tail_object_is_retained_when_a_MIDDLE_part_is_still_local_only`, which
//! proves deletion is **refused** in the one case where a naive implementation
//! would destroy data.

use std::sync::Arc;

use ehdb_core::{EhdbError, Result};
use ehdb_l0::engine::reclaim_superseded_tail_objects;
use ehdb_l0::substrate::DurableSubstrate;
use ehdb_l0::{EventRecord, L0Config, L0EventLogEngine, LocalFsSubstrate, ReplicaTarget};

fn unique_dir(tag: &str) -> std::path::PathBuf {
    static N: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let n = N.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    std::env::temp_dir().join(format!("ehdb-l0-d2-{tag}-{}-{n}", std::process::id()))
}

fn substrate(dir: &std::path::Path) -> Arc<dyn DurableSubstrate> {
    Arc::new(LocalFsSubstrate::new(dir).unwrap())
}

fn cfg(root: &std::path::Path) -> L0Config {
    L0Config::d1(root)
        .with_shard_count(1)
        .with_granule_size(4)
        .with_seal_max_records(8)
        .with_tail_replication(true)
}

fn objects(dir: &std::path::Path, prefix: &str) -> Vec<String> {
    LocalFsSubstrate::new(dir)
        .unwrap()
        .list_prefix(prefix)
        .unwrap()
}

/// A substrate whose writes can be switched off, so a part can be made
/// local-only on purpose.
struct Switchable {
    inner: Arc<dyn DurableSubstrate>,
    refusing: std::sync::atomic::AtomicBool,
}
impl Switchable {
    fn new(inner: Arc<dyn DurableSubstrate>) -> Self {
        Self {
            inner,
            refusing: std::sync::atomic::AtomicBool::new(false),
        }
    }
    fn set(&self, v: bool) {
        self.refusing.store(v, std::sync::atomic::Ordering::SeqCst);
    }
    fn refusing(&self) -> bool {
        self.refusing.load(std::sync::atomic::Ordering::SeqCst)
    }
}
impl DurableSubstrate for Switchable {
    fn failure_domain(&self) -> ehdb_l0::failure_domain::FailureDomain {
        self.inner.failure_domain()
    }
    fn put_if_absent(&self, key: &str, bytes: &[u8]) -> Result<bool> {
        if self.refusing() {
            return Err(EhdbError::Storage("refusing".into()));
        }
        self.inner.put_if_absent(key, bytes)
    }
    fn put_overwrite(&self, key: &str, bytes: &[u8]) -> Result<()> {
        if self.refusing() {
            return Err(EhdbError::Storage("refusing".into()));
        }
        self.inner.put_overwrite(key, bytes)
    }
    fn get_range(&self, k: &str, o: u64, l: u64) -> Result<Vec<u8>> {
        self.inner.get_range(k, o, l)
    }
    fn get_all(&self, k: &str) -> Result<Vec<u8>> {
        self.inner.get_all(k)
    }
    fn exists(&self, k: &str) -> Result<bool> {
        self.inner.exists(k)
    }
    fn list_prefix(&self, p: &str) -> Result<Vec<String>> {
        self.inner.list_prefix(p)
    }
    fn delete(&self, k: &str) -> Result<()> {
        self.inner.delete(k)
    }
}

/// ⭐ The ordinary case: once a durable part covers the tail object's interval,
/// the object is redundant and goes away — and the records are still there.
#[test]
fn a_tail_object_is_reclaimed_once_a_durable_part_covers_it() {
    let local = unique_dir("ok-local");
    let remote = unique_dir("ok-remote");
    let mut e = L0EventLogEngine::open_replicated(
        cfg(&local),
        vec![ReplicaTarget::new("replica-0", substrate(&remote))],
    )
    .unwrap();

    let mut expected = Vec::new();
    for i in 0..5u64 {
        e.append("1001", &format!("t{i}"), format!("rec-{i}"))
            .unwrap();
        expected.push(format!("rec-{i}"));
    }
    // Replicate while unsealed, then seal so a durable part covers them.
    assert_eq!(e.replicate_tail().unwrap().records, 5);
    assert_eq!(objects(&remote, "tail/").len(), 1);
    e.flush_and_wait_uploads().unwrap();
    let m = e.manifest_snapshot();
    assert_eq!(m.parts.len(), 1);
    assert!(
        m.parts[0].is_durable(),
        "the part must be durable for D2 to fire"
    );

    let wm = e.contiguous_durable_watermarks();
    let replicas = e.replica_handles();
    let rep = reclaim_superseded_tail_objects(&replicas, "d1_event_log", &wm, e.metrics().as_ref());

    assert_eq!(rep.reclaimed, 1, "the redundant tail object is deleted");
    assert_eq!(rep.retained, 0);
    assert_eq!(rep.failed, 0);
    assert_eq!(rep.unparsed, 0);
    assert!(rep.bytes > 0, "reclaimed bytes are measured, not estimated");
    assert!(objects(&remote, "tail/").is_empty(), "gone from the bucket");
    drop(e);

    // ⚠ And the records survive the reclaim — the part has them.
    std::fs::remove_dir_all(&local).unwrap();
    let fresh = unique_dir("ok-cold");
    let revived = L0EventLogEngine::cold_load_replicated(
        cfg(&fresh),
        vec![ReplicaTarget::new("replica-0", substrate(&remote))],
    )
    .unwrap();
    let got: Vec<String> = revived
        .replay_all()
        .unwrap()
        .into_iter()
        .map(|r: EventRecord| r.payload)
        .collect();
    assert_eq!(got, expected, "reclaiming the tail object lost nothing");
}

/// ⚠⚠⚠ **THE SAFETY TEST.** A tail object must be retained when a part in the
/// MIDDLE of the shard's sequence is still local-only, even though a LATER part
/// is durable.
///
/// A naive watermark — `max(max_sequence)` over durable parts — would skip the
/// local-only part and authorise deleting a tail object whose records exist
/// nowhere off-box. That is not a cleanup; it is the destruction of the only
/// remote copy of those events.
#[test]
fn a_tail_object_is_retained_when_a_middle_part_is_still_local_only() {
    let local = unique_dir("gap-local");
    let remote = unique_dir("gap-remote");
    let facade = Arc::new(Switchable::new(substrate(&remote)));
    let sub: Arc<dyn DurableSubstrate> = facade.clone();
    let mut e =
        L0EventLogEngine::open_replicated(cfg(&local), vec![ReplicaTarget::new("replica-0", sub)])
            .unwrap();

    // Part A — uploads fine.
    for i in 0..8u64 {
        e.append("1001", &format!("a{i}"), format!("A-{i}"))
            .unwrap();
    }
    e.flush_and_wait_uploads().unwrap();

    // Part B — upload refused, so it stays LOCAL-ONLY.
    facade.set(true);
    for i in 0..8u64 {
        e.append("1001", &format!("b{i}"), format!("B-{i}"))
            .unwrap();
    }
    e.flush_and_wait_uploads().unwrap();
    facade.set(false);

    // Part C — uploads fine, and is AFTER the gap.
    for i in 0..8u64 {
        e.append("1001", &format!("c{i}"), format!("C-{i}"))
            .unwrap();
    }
    e.flush_and_wait_uploads().unwrap();

    let m = e.manifest_snapshot();
    assert_eq!(m.parts.len(), 3, "A, B, C");
    let durable: Vec<bool> = {
        let mut p: Vec<_> = m.parts.iter().collect();
        p.sort_by_key(|x| x.min_sequence);
        p.iter().map(|x| x.is_durable()).collect()
    };
    assert_eq!(
        durable,
        vec![true, false, true],
        "the scenario requires exactly a durable / local-only / durable run; got {durable:?}"
    );

    // Now some unsealed records, replicated as a tail object.
    for i in 0..3u64 {
        e.append("1001", &format!("d{i}"), format!("D-{i}"))
            .unwrap();
    }
    assert_eq!(e.replicate_tail().unwrap().records, 3);
    assert_eq!(objects(&remote, "tail/").len(), 1);

    // ⭐ The contiguous watermark must stop at A, NOT reach C.
    let wm = e.contiguous_durable_watermarks();
    let mut sorted: Vec<_> = m.parts.iter().collect();
    sorted.sort_by_key(|x| x.min_sequence);
    let a_max = sorted[0].max_sequence;
    let c_max = sorted[2].max_sequence;
    assert_eq!(
        wm,
        vec![(0, a_max)],
        "watermark must stop at the first non-durable part (A={a_max}), not run to C={c_max}"
    );

    let replicas = e.replica_handles();
    let rep = reclaim_superseded_tail_objects(&replicas, "d1_event_log", &wm, e.metrics().as_ref());
    assert_eq!(
        rep.reclaimed, 0,
        "NOTHING may be deleted while a middle part is local-only"
    );
    assert_eq!(rep.retained, 1, "and the decision to keep it is reported");
    assert_eq!(
        objects(&remote, "tail/").len(),
        1,
        "the tail object is still in the bucket"
    );
}

/// Objects this code does not understand are never deleted.
#[test]
fn unrecognised_objects_under_the_tail_prefix_are_never_deleted() {
    let local = unique_dir("odd-local");
    let remote = unique_dir("odd-remote");
    let mut e = L0EventLogEngine::open_replicated(
        cfg(&local),
        vec![ReplicaTarget::new("replica-0", substrate(&remote))],
    )
    .unwrap();
    for i in 0..5u64 {
        e.append("1001", &format!("t{i}"), format!("rec-{i}"))
            .unwrap();
    }
    e.flush_and_wait_uploads().unwrap();

    let s = substrate(&remote);
    // An unparseable key, and another dataset's tail object.
    s.put_if_absent("tail/d1_event_log/not-a-shard-dir/whatever.eslog", b"x")
        .unwrap();
    s.put_if_absent(
        "tail/d9_other/shard-0/seq-00000000000000000001-00000000000000000002.eslog",
        b"y",
    )
    .unwrap();

    let wm = e.contiguous_durable_watermarks();
    let replicas = e.replica_handles();
    let rep = reclaim_superseded_tail_objects(&replicas, "d1_event_log", &wm, e.metrics().as_ref());

    assert_eq!(rep.reclaimed, 0, "neither object may be deleted");
    assert_eq!(
        rep.unparsed, 1,
        "the malformed d1 key is counted as unparsed"
    );
    assert_eq!(
        objects(&remote, "tail/").len(),
        2,
        "both unrecognised objects remain"
    );
}

/// An empty prefix is not an error, and reports nothing rather than something.
#[test]
fn reclaiming_with_no_tail_objects_reports_zero() {
    let local = unique_dir("empty-local");
    let remote = unique_dir("empty-remote");
    let e = L0EventLogEngine::open_replicated(
        cfg(&local),
        vec![ReplicaTarget::new("replica-0", substrate(&remote))],
    )
    .unwrap();
    let wm = e.contiguous_durable_watermarks();
    let replicas = e.replica_handles();
    let rep = reclaim_superseded_tail_objects(&replicas, "d1_event_log", &wm, e.metrics().as_ref());
    assert_eq!(rep, Default::default());
}
