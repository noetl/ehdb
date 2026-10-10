//! **B3 — off-box replication of the UNSEALED tail** (noetl/ehdb#394).
//!
//! A sealed part is copied to every replica by the uploader, so A2/A3 made
//! *sealed* history survive node loss. Until a part seals it exists only on
//! local disk: `seal_max_age` bounds that window (B2) but cannot close it. This
//! file proves the tail itself reaches the replica set and can be replayed.
//!
//! # The RED control is the load-bearing test
//!
//! Every green result here has an innocent explanation — a cold load that
//! silently read a local cache, or a comparison of two vectors that are both
//! short — so the file starts by proving the loss is **real** with replication
//! off. Without that, nothing below is falsifiable.
//!
//! The shape of every case: seal some parts (so a durable manifest exists),
//! append more records that do **not** seal, destroy the local root, and
//! cold-load from the replica alone.

use std::sync::Arc;

use ehdb_l0::substrate::DurableSubstrate;
use ehdb_l0::{EventRecord, L0Config, L0EventLogEngine, LocalFsSubstrate, ReplicaTarget};

fn unique_dir(tag: &str) -> std::path::PathBuf {
    static N: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let n = N.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    std::env::temp_dir().join(format!("ehdb-l0-b3-{tag}-{}-{n}", std::process::id()))
}

fn substrate(dir: &std::path::Path) -> Arc<dyn DurableSubstrate> {
    Arc::new(LocalFsSubstrate::new(dir).unwrap())
}

fn cfg(root: &std::path::Path, tail: bool) -> L0Config {
    L0Config::d1(root)
        .with_shard_count(1)
        .with_granule_size(4)
        // 8 per part, so 16 appends seal exactly two and the next few do not.
        .with_seal_max_records(8)
        .with_tail_replication(tail)
}

/// Objects physically present under a prefix, counted from the substrate rather
/// than from any manifest.
fn objects_under(dir: &std::path::Path, prefix: &str) -> Vec<String> {
    LocalFsSubstrate::new(dir)
        .unwrap()
        .list_prefix(prefix)
        .unwrap()
}

struct Built {
    local: std::path::PathBuf,
    remote: std::path::PathBuf,
    sealed_payloads: Vec<String>,
    tail_payloads: Vec<String>,
}

/// Seal two parts, then append `tail_n` records that do not seal. Returns the
/// roots and the two expected payload sets. Replicates the tail iff `tail`.
fn build(tag: &str, tail: bool, tail_n: u64) -> Built {
    let local = unique_dir(&format!("{tag}-local"));
    let remote = unique_dir(&format!("{tag}-remote"));

    let mut e = L0EventLogEngine::open_replicated(
        cfg(&local, tail),
        vec![ReplicaTarget::new("replica-0", substrate(&remote))],
    )
    .unwrap();

    let mut sealed_payloads = Vec::new();
    for i in 0..16u64 {
        e.append("1001", &format!("t{i}"), format!("sealed-{i}"))
            .unwrap();
        sealed_payloads.push(format!("sealed-{i}"));
    }
    e.flush_and_wait_uploads().unwrap();
    // ⚠ The flush seals whatever was pending, so the tail must be appended
    // AFTER it — otherwise this test would be about sealed parts again.
    assert_eq!(
        e.manifest_snapshot().parts.len(),
        2,
        "16/8 = 2 sealed parts"
    );

    let mut tail_payloads = Vec::new();
    for i in 0..tail_n {
        e.append("1001", &format!("u{i}"), format!("tail-{i}"))
            .unwrap();
        tail_payloads.push(format!("tail-{i}"));
    }
    assert_eq!(
        e.manifest_snapshot().parts.len(),
        2,
        "the tail must NOT have sealed, or this proves nothing about unsealed records"
    );

    if tail {
        let rep = e.replicate_tail().unwrap();
        assert_eq!(rep.records, tail_n as usize, "every tail record replicated");
        assert_eq!(rep.batches, 1, "one batch for one shard");
        assert!(rep.failed_shards.is_empty(), "no replica refused");
    }
    drop(e);

    Built {
        local,
        remote,
        sealed_payloads,
        tail_payloads,
    }
}

/// Destroy the local root (disk loss) and cold-load from the replica alone.
fn recover_from_remote_alone(b: &Built) -> Vec<String> {
    std::fs::remove_dir_all(&b.local).unwrap();
    let fresh = unique_dir("recovered");
    let e = L0EventLogEngine::cold_load_replicated(
        cfg(&fresh, false),
        vec![ReplicaTarget::new("replica-0", substrate(&b.remote))],
    )
    .expect("the replica alone can serve a cold load");
    e.replay_all()
        .unwrap()
        .into_iter()
        .map(|r: EventRecord| r.payload)
        .collect()
}

/// ⚠⚠ **RED control.** With tail replication off, records appended into an
/// unsealed part are gone when the local disk is lost. If this ever passes with
/// the tail intact, every GREEN below is meaningless.
#[test]
fn without_tail_replication_the_unsealed_records_are_lost() {
    let b = build("red", false, 5);
    assert!(
        objects_under(&b.remote, "tail/").is_empty(),
        "nothing should have written a tail object with the flag off"
    );

    let got = recover_from_remote_alone(&b);

    assert_eq!(
        got, b.sealed_payloads,
        "exactly the sealed history comes back"
    );
    for p in &b.tail_payloads {
        assert!(
            !got.contains(p),
            "{p} survived with replication OFF — the loss this feature prevents is not real, \
             so the GREEN tests prove nothing"
        );
    }
    assert_eq!(got.len(), 16, "5 unsealed records lost, 16 sealed kept");
}

/// ⭐ **GREEN.** The same loss, with the tail replicated: every record comes
/// back from the replica alone, including the ones that never sealed.
#[test]
fn the_unsealed_tail_survives_and_cold_loads_from_the_replica_alone() {
    let b = build("green", true, 5);

    // Byte-level proof, from the substrate rather than from a gauge: the tail
    // objects physically exist, and they are NOT under `parts/`.
    let tails = objects_under(&b.remote, "tail/");
    assert_eq!(tails.len(), 1, "one tail object: {tails:?}");
    assert!(
        tails[0].starts_with("tail/d1_event_log/shard-0/"),
        "tail key shape: {tails:?}"
    );
    let parts = objects_under(&b.remote, "parts/");
    assert_eq!(
        parts.len(),
        2,
        "the two sealed parts, and no tail among them"
    );

    let got = recover_from_remote_alone(&b);

    let mut expected = b.sealed_payloads.clone();
    expected.extend(b.tail_payloads.clone());
    assert_eq!(
        got, expected,
        "all 21 records — the 16 sealed AND the 5 that never sealed"
    );
}

/// ⚠ A tail object is **not a part**: it must never enter the manifest, or
/// `plan_retention` could drop it as though it were one and a read would try to
/// use a sparse index it does not have.
#[test]
fn a_tail_object_never_enters_the_manifest() {
    let b = build("manifest", true, 5);
    let fresh = unique_dir("manifest-recovered");
    std::fs::remove_dir_all(&b.local).unwrap();
    let e = L0EventLogEngine::cold_load_replicated(
        cfg(&fresh, false),
        vec![ReplicaTarget::new("replica-0", substrate(&b.remote))],
    )
    .unwrap();
    let m = e.manifest_snapshot();
    for p in &m.parts {
        for r in &p.replicas {
            assert!(
                !r.key.starts_with("tail/"),
                "part {} references a tail object at {}",
                p.part_id,
                r.key
            );
        }
        assert!(
            !p.part_id.contains("tail"),
            "a tail object became part {}",
            p.part_id
        );
    }
}

/// The overlap is expected, not an error (the noetl/ai-meta#335 shape): a part
/// that seals *after* its tail objects were written makes them redundant, so the
/// same record is in both places. Recovery must not duplicate it.
#[test]
fn records_present_in_both_a_tail_object_and_a_sealed_part_are_not_duplicated() {
    let local = unique_dir("overlap-local");
    let remote = unique_dir("overlap-remote");
    let mut e = L0EventLogEngine::open_replicated(
        cfg(&local, true),
        vec![ReplicaTarget::new("replica-0", substrate(&remote))],
    )
    .unwrap();

    let mut expected = Vec::new();
    for i in 0..4u64 {
        e.append("1001", &format!("t{i}"), format!("both-{i}"))
            .unwrap();
        expected.push(format!("both-{i}"));
    }
    // Replicate while unsealed, THEN seal the very same records.
    let rep = e.replicate_tail().unwrap();
    assert_eq!(rep.records, 4);
    e.flush_and_wait_uploads().unwrap();
    assert_eq!(e.manifest_snapshot().parts.len(), 1, "those 4 now sealed");
    assert_eq!(
        objects_under(&remote, "tail/").len(),
        1,
        "the tail object remains"
    );
    drop(e);

    std::fs::remove_dir_all(&local).unwrap();
    let fresh = unique_dir("overlap-recovered");
    let e = L0EventLogEngine::cold_load_replicated(
        cfg(&fresh, false),
        vec![ReplicaTarget::new("replica-0", substrate(&remote))],
    )
    .unwrap();
    let got: Vec<String> = e
        .replay_all()
        .unwrap()
        .into_iter()
        .map(|r: EventRecord| r.payload)
        .collect();

    assert_eq!(got, expected, "each record exactly once, not twice");
    assert_eq!(
        e.metrics().snapshot().tail_objects_superseded,
        1,
        "the redundant tail object is counted as superseded, not silently skipped"
    );
    assert_eq!(
        e.metrics().snapshot().tail_recovered_records,
        0,
        "nothing needed recovering — the seal already covered it"
    );
}

/// The flag is off by default, so a driver can call the replicator
/// unconditionally without it doing anything.
#[test]
fn the_replicator_is_a_no_op_until_the_flag_is_set() {
    let local = unique_dir("off-local");
    let remote = unique_dir("off-remote");
    let mut e = L0EventLogEngine::open_replicated(
        cfg(&local, false),
        vec![ReplicaTarget::new("replica-0", substrate(&remote))],
    )
    .unwrap();
    for i in 0..4u64 {
        e.append("1001", &format!("t{i}"), format!("p-{i}"))
            .unwrap();
    }
    let rep = e.replicate_tail().unwrap();
    assert_eq!(rep, Default::default(), "nothing reported");
    assert!(!rep.replicated_anything());
    assert!(objects_under(&remote, "tail/").is_empty());
    assert_eq!(e.metrics().snapshot().tail_batches, 0);
}

/// The watermark advances only over what actually reached a replica, and a
/// second pass with no new records writes nothing.
#[test]
fn a_second_pass_with_no_new_records_writes_nothing() {
    let local = unique_dir("idem-local");
    let remote = unique_dir("idem-remote");
    let mut e = L0EventLogEngine::open_replicated(
        cfg(&local, true),
        vec![ReplicaTarget::new("replica-0", substrate(&remote))],
    )
    .unwrap();
    for i in 0..4u64 {
        e.append("1001", &format!("t{i}"), format!("p-{i}"))
            .unwrap();
    }
    let first = e.replicate_tail().unwrap();
    assert_eq!(first.records, 4);
    let wm = e.tail_watermarks();
    assert_eq!(wm.len(), 1, "one shard has a watermark");

    let second = e.replicate_tail().unwrap();
    assert_eq!(second.records, 0, "nothing new to send");
    assert_eq!(second.batches, 0);
    assert_eq!(e.tail_watermarks(), wm, "watermark unchanged");
    assert_eq!(objects_under(&remote, "tail/").len(), 1, "no second object");

    // One more record, and only that one moves.
    e.append("1001", "t9", "p-9").unwrap();
    let third = e.replicate_tail().unwrap();
    assert_eq!(third.records, 1, "only the new record");
    assert_eq!(objects_under(&remote, "tail/").len(), 2);
}

/// A substrate that can be switched to refuse writes while still serving reads
/// — a replica that was writable when the engine opened and became unwritable
/// afterwards.
///
/// ⚠ It starts permissive on purpose. Refusing from the very first write makes
/// `open_replicated` fail at the `FORMAT_VERSION` gate, which is correct
/// behaviour (the gate is checked on *every* replica) but tests the wrong
/// thing: the question here is what a running replicator does when a replica
/// goes away, not what an open does when one was never there.
struct RefusesWrites {
    inner: Arc<dyn DurableSubstrate>,
    refusing: std::sync::atomic::AtomicBool,
}

impl RefusesWrites {
    fn new(inner: Arc<dyn DurableSubstrate>) -> Self {
        Self {
            inner,
            refusing: std::sync::atomic::AtomicBool::new(false),
        }
    }
    fn refuse(&self) {
        self.refusing
            .store(true, std::sync::atomic::Ordering::SeqCst);
    }
    fn is_refusing(&self) -> bool {
        self.refusing.load(std::sync::atomic::Ordering::SeqCst)
    }
}

impl DurableSubstrate for RefusesWrites {
    fn failure_domain(&self) -> ehdb_l0::failure_domain::FailureDomain {
        self.inner.failure_domain()
    }
    fn put_if_absent(&self, key: &str, bytes: &[u8]) -> ehdb_core::Result<bool> {
        if self.is_refusing() {
            return Err(ehdb_core::EhdbError::Storage(
                "replica refuses writes".into(),
            ));
        }
        self.inner.put_if_absent(key, bytes)
    }
    fn put_overwrite(&self, key: &str, bytes: &[u8]) -> ehdb_core::Result<()> {
        if self.is_refusing() {
            return Err(ehdb_core::EhdbError::Storage(
                "replica refuses writes".into(),
            ));
        }
        self.inner.put_overwrite(key, bytes)
    }
    fn get_range(&self, key: &str, offset: u64, len: u64) -> ehdb_core::Result<Vec<u8>> {
        self.inner.get_range(key, offset, len)
    }
    fn get_all(&self, key: &str) -> ehdb_core::Result<Vec<u8>> {
        self.inner.get_all(key)
    }
    fn exists(&self, key: &str) -> ehdb_core::Result<bool> {
        self.inner.exists(key)
    }
    fn list_prefix(&self, prefix: &str) -> ehdb_core::Result<Vec<String>> {
        self.inner.list_prefix(prefix)
    }
    fn delete(&self, key: &str) -> ehdb_core::Result<()> {
        self.inner.delete(key)
    }
}

/// ⚠⚠ **D3: a failing tail put must not fail the append, must not advance the
/// cursor, and must not be silent.**
///
/// The record is already durable locally; failing an append because a *replica*
/// is unreachable converts a durability feature into an availability
/// regression — the trade A2 already made when it chose to open at RF=1 rather
/// than refuse.
///
/// And the silence matters as much as the behaviour: a replicator failing every
/// put leaves `tail_batches` flat, which is exactly what an idle one does. The
/// two are opposite conditions, so they get different counters.
#[test]
fn a_refusing_replica_is_counted_and_retried_not_silently_dropped() {
    let local = unique_dir("refuse-local");
    let remote = unique_dir("refuse-remote");
    // Create the dir so reads resolve, then wrap it in a write-refusing façade.
    let facade = Arc::new(RefusesWrites::new(substrate(&remote)));
    let refusing: Arc<dyn DurableSubstrate> = facade.clone();

    let mut e = L0EventLogEngine::open_replicated(
        cfg(&local, true),
        vec![ReplicaTarget::new("replica-0", refusing)],
    )
    .unwrap();
    // Now the replica goes away.
    facade.refuse();

    // The appends themselves must succeed.
    for i in 0..4u64 {
        e.append("1001", &format!("t{i}"), format!("p-{i}"))
            .expect("a refusing REPLICA must not fail a local append");
    }

    let rep = e.replicate_tail().unwrap();
    assert_eq!(rep.batches, 0, "nothing reached a replica");
    assert_eq!(rep.records, 0);
    assert_eq!(rep.failed_shards, vec![0], "the failure names its shard");
    assert!(
        !rep.replicated_anything(),
        "must not report success when every put failed"
    );

    let snap = e.metrics().snapshot();
    assert_eq!(
        snap.tail_replication_failed, 1,
        "the failure is counted — otherwise it is indistinguishable from an idle replicator"
    );
    assert_eq!(snap.tail_batches, 0);

    // ⭐ The cursor must NOT have advanced, so the next tick re-drives the same
    // records rather than skipping past them forever.
    assert!(
        e.tail_watermarks().is_empty(),
        "cursor advanced past records that never left the node: {:?}",
        e.tail_watermarks()
    );
    let again = e.replicate_tail().unwrap();
    assert_eq!(
        again.failed_shards,
        vec![0],
        "the same records are retried on the next pass"
    );
    assert_eq!(e.metrics().snapshot().tail_replication_failed, 2);
}

/// A negative control on the prefix: tail objects belonging to a different
/// dataset must not be picked up by this one's recovery.
#[test]
fn recovery_ignores_tail_objects_under_another_datasets_prefix() {
    let b = build("prefix", true, 5);
    // Plant a decoy under a neighbouring dataset prefix.
    let s = substrate(&b.remote);
    s.put_if_absent(
        "tail/d9_other_dataset/shard-0/seq-00000000000000000001-00000000000000000001.eslog",
        b"not-a-valid-frame-and-must-never-be-read",
    )
    .unwrap();

    let got = recover_from_remote_alone(&b);
    let mut expected = b.sealed_payloads.clone();
    expected.extend(b.tail_payloads.clone());
    assert_eq!(
        got, expected,
        "the decoy under another dataset's prefix must be invisible — if recovery read it, \
         this would have failed to decode rather than returning the right set"
    );
}

/// ⚠⚠ **The lock-released path.** `replicate_tail` is a convenience that does
/// prepare → upload → commit in one call, which means the remote put runs while
/// the caller holds the engine. For the server's embedded engine that lock is
/// also taken by the live append path, so a tick colliding with an append would
/// add the full remote-put latency to it — p50 77 ms, p99 234 ms against GCS.
///
/// These pin the split that lets a driver avoid that: **prepare does no I/O**,
/// so it is safe under the lock, and the upload is a free function that borrows
/// nothing from the engine.
#[test]
fn preparing_a_tail_batch_touches_no_substrate() {
    let local = unique_dir("split-local");
    let remote = unique_dir("split-remote");
    let facade = Arc::new(RefusesWrites::new(substrate(&remote)));
    let refusing: Arc<dyn DurableSubstrate> = facade.clone();

    let mut e = L0EventLogEngine::open_replicated(
        cfg(&local, true),
        vec![ReplicaTarget::new("replica-0", refusing)],
    )
    .unwrap();
    for i in 0..4u64 {
        e.append("1001", &format!("t{i}"), format!("p-{i}"))
            .unwrap();
    }

    // Every write now fails. `prepare` must still succeed — if it performed any
    // substrate I/O it could not.
    facade.refuse();
    let batches = e
        .prepare_tail_batches()
        .expect("prepare must not touch the substrate, so a refusing replica cannot fail it");
    assert_eq!(batches.len(), 1, "one shard, one batch");
    assert_eq!(batches[0].records, 4);
    assert_eq!(batches[0].shard, 0);
    assert!(!batches[0].bytes.is_empty());
    assert!(
        batches[0].key.starts_with("tail/d1_event_log/shard-0/"),
        "key: {}",
        batches[0].key
    );

    // And the upload — the part that does I/O — fails against the same replica,
    // which is what proves the preceding success was not an accident of the
    // façade being permissive.
    let replicas = e.replica_handles();
    let ok = ehdb_l0::engine::upload_tail_batch(&replicas, &batches[0], e.metrics().as_ref());
    assert!(!ok, "the upload must fail where prepare succeeded");

    // Nothing committed, so the watermark has not moved.
    assert!(e.tail_watermarks().is_empty());
}

/// The driver's shape, end to end: prepare (lock), upload (lock RELEASED),
/// commit (lock) — and the records recover from the replica alone.
#[test]
fn the_lock_released_driver_shape_replicates_and_recovers() {
    let local = unique_dir("driver-local");
    let remote = unique_dir("driver-remote");
    let mut e = L0EventLogEngine::open_replicated(
        cfg(&local, true),
        vec![ReplicaTarget::new("replica-0", substrate(&remote))],
    )
    .unwrap();

    let mut sealed = Vec::new();
    for i in 0..8u64 {
        e.append("1001", &format!("s{i}"), format!("sealed-{i}"))
            .unwrap();
        sealed.push(format!("sealed-{i}"));
    }
    e.flush_and_wait_uploads().unwrap();

    let mut tail = Vec::new();
    for i in 0..5u64 {
        e.append("1001", &format!("u{i}"), format!("unsealed-{i}"))
            .unwrap();
        tail.push(format!("unsealed-{i}"));
    }

    // --- exactly what the server driver does ---
    let (batches, replicas) = {
        // lock held
        (e.prepare_tail_batches().unwrap(), e.replica_handles())
    };
    // lock released here: the uploads below borrow nothing from the engine.
    let metrics = e.metrics();
    let results: Vec<(bool, &ehdb_l0::engine::TailBatch)> = batches
        .iter()
        .map(|b| (upload_tail_batch_helper(&replicas, b, metrics.as_ref()), b))
        .collect();
    // lock re-taken to commit
    let mut report = Default::default();
    for (ok, b) in results {
        if ok {
            e.commit_tail_batch(b, &mut report);
        } else {
            e.fail_tail_batch(b, &mut report);
        }
    }
    assert_eq!(report.records, 5, "all five unsealed records committed");
    assert!(report.failed_shards.is_empty());
    drop(e);

    std::fs::remove_dir_all(&local).unwrap();
    let fresh = unique_dir("driver-recovered");
    let revived = L0EventLogEngine::cold_load_replicated(
        cfg(&fresh, false),
        vec![ReplicaTarget::new("replica-0", substrate(&remote))],
    )
    .unwrap();
    let got: Vec<String> = revived
        .replay_all()
        .unwrap()
        .into_iter()
        .map(|r: EventRecord| r.payload)
        .collect();
    let mut expected = sealed;
    expected.extend(tail);
    assert_eq!(
        got, expected,
        "13 records via the lock-released driver shape"
    );
}

fn upload_tail_batch_helper(
    replicas: &[ReplicaTarget],
    batch: &ehdb_l0::engine::TailBatch,
    metrics: &ehdb_l0::metrics::L0Metrics,
) -> bool {
    ehdb_l0::engine::upload_tail_batch(replicas, batch, metrics)
}
