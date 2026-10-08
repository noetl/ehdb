//! **The four state gauges (noetl/ai-meta#455 C6) — pinned, and proven to move.**
//!
//! Everything else in `L0Metrics` is a cumulative counter. These four are the *current
//! depth* of something, which is a different kind of signal: a counter answers "how much
//! has ever happened", a gauge answers "how much is outstanding right now", and no amount
//! of the first gives you the second.
//!
//! | gauge | what is outstanding |
//! | :-- | :-- |
//! | `manifest_parts` | live parts — predicts memory (~1.8 KB each) and is what merge bounds |
//! | `parts_local_only` | sealed parts with **no** durable copy — the upload backlog |
//! | `parts_under_replicated` | parts with **some but not enough** copies — divergence from the declared RF |
//! | `dedupe_window_records` | records held in the idempotency window |
//!
//! # ⭐ Why `parts_under_replicated` is not redundant with the durability window
//!
//! The uploader calls `replicate_bytes` across the replica set and then assigns
//! `p.replicas = locations` — **the successful writes only**, in one shot. So a part whose
//! write landed on 1 of 2 replicas is recorded as durable, and
//! `unreplicated.on_upload_done(..)` **fires**, closing its durability window and recording
//! its append→durable latency.
//!
//! The age-based signal therefore reports that part as **done**, correctly, while it holds
//! half the copies it is supposed to. `replica_writes` cannot fill the gap either: it is
//! cumulative, so it keeps climbing while the deficit persists. Nothing in the engine
//! reported a standing replication deficit before this gauge — and *an RF of N achieved on
//! one replica is an RF of 1 wearing a larger number*, which is the same observation the
//! failure-domain guard was built on.
//!
//! # The proof obligation
//!
//! A pin at 0 is only trustworthy if something is known to move it off 0; otherwise
//! "pinned at zero" is just "always zero", which is the inert-metric defect wearing a
//! healthy face. So every one of the four is driven off zero here by real engine work, and
//! the two replica gauges are driven **deterministically** — via a substrate that refuses
//! writes, plus `flush_and_wait_uploads`, so nothing depends on racing the uploader thread.

use std::sync::Arc;

use ehdb_core::Result as EhdbResult;
use ehdb_l0::substrate::{DurableSubstrate, InMemorySubstrate, LocalFsSubstrate};
use ehdb_l0::{shard_for_execution, Dataset, L0Config, L0Engine, ReplicaTarget};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct R {
    seq: u64,
    key: String,
    /// A per-record unique id so the dataset can declare a `dedupe_key`.
    ///
    /// ⚠ Needed because `Dataset::dedupe_key` defaults to `None`, so a dataset that does
    /// not override it remembers nothing and `dedupe_window_records` is legitimately 0
    /// forever. The gauge was correct and the first version of this test was wrong: it
    /// asserted the gauge was inert using a dataset that cannot drive it.
    dk: String,
}

struct GD;
impl Dataset for GD {
    type Record = R;
    const NAME: &'static str = "gauge_d";
    fn sort_key(r: &R) -> u64 {
        r.seq
    }
    fn partition(r: &R, n: u32) -> u32 {
        shard_for_execution(&r.key, n)
    }
    fn index_key(r: &R) -> &str {
        &r.key
    }
    fn read_partition(k: &str, n: u32) -> u32 {
        shard_for_execution(k, n)
    }
    fn dedupe_key(r: &R) -> Option<&str> {
        Some(&r.dk)
    }
}

/// A substrate that **refuses every write** and serves reads from an inner store.
///
/// This is how a replication deficit is produced deterministically: `replicate_bytes`
/// collects only the locations that succeeded, so a refusing replica simply does not
/// appear in the part's `replicas` list.
///
/// ⚠ It must be **toggleable**, not permanently refusing: `open_replicated` writes the
/// initial manifest to every replica and propagates the failure, so an always-refusing
/// replica cannot be opened at all. The toggle flips after open — which is also the more
/// faithful scenario, since a replica that was never writable would never have been
/// configured.
#[derive(Debug)]
struct Refusing {
    inner: InMemorySubstrate,
    refusing: std::sync::atomic::AtomicBool,
}

impl Refusing {
    fn new(tag: &str) -> Self {
        Self {
            inner: InMemorySubstrate::new(tag),
            refusing: std::sync::atomic::AtomicBool::new(false),
        }
    }
    fn start_refusing(&self) {
        self.refusing
            .store(true, std::sync::atomic::Ordering::SeqCst);
    }
    fn is_refusing(&self) -> bool {
        self.refusing.load(std::sync::atomic::Ordering::SeqCst)
    }
}

impl DurableSubstrate for Refusing {
    fn put_if_absent(&self, key: &str, bytes: &[u8]) -> EhdbResult<bool> {
        if self.is_refusing() {
            return Err(ehdb_core::EhdbError::Storage("refusing substrate".into()));
        }
        self.inner.put_if_absent(key, bytes)
    }
    fn put_overwrite(&self, key: &str, bytes: &[u8]) -> EhdbResult<()> {
        if self.is_refusing() {
            return Err(ehdb_core::EhdbError::Storage("refusing substrate".into()));
        }
        self.inner.put_overwrite(key, bytes)
    }
    fn get_range(&self, key: &str, offset: u64, len: u64) -> EhdbResult<Vec<u8>> {
        self.inner.get_range(key, offset, len)
    }
    fn get_all(&self, key: &str) -> EhdbResult<Vec<u8>> {
        self.inner.get_all(key)
    }
    fn exists(&self, key: &str) -> EhdbResult<bool> {
        self.inner.exists(key)
    }
    fn list_prefix(&self, prefix: &str) -> EhdbResult<Vec<String>> {
        self.inner.list_prefix(prefix)
    }
    fn delete(&self, key: &str) -> EhdbResult<()> {
        self.inner.delete(key)
    }
}

fn dir(tag: &str) -> std::path::PathBuf {
    static N: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let n = N.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    std::env::temp_dir().join(format!("ehdb-gauge-{tag}-{}-{n}", std::process::id()))
}

fn cfg(root: &std::path::Path, seal_every: u64) -> L0Config {
    let hot = root.join("hot");
    std::fs::create_dir_all(&hot).unwrap();
    L0Config::for_dataset(GD::NAME, &hot)
        .with_shard_count(1)
        .with_seal_max_records(seal_every)
}

fn local(root: &std::path::Path, name: &str) -> Arc<dyn DurableSubstrate> {
    let p = root.join(name);
    std::fs::create_dir_all(&p).unwrap();
    Arc::new(LocalFsSubstrate::new(&p).unwrap())
}

fn append_n(e: &mut L0Engine<GD>, n: u64) {
    for i in 1..=n {
        e.append_record(R {
            seq: i,
            key: format!("k{}", i % 8),
            dk: format!("dk-{i}"),
        })
        .unwrap();
    }
}

// ---------------------------------------------------------------------------

#[test]
fn a_fresh_engine_pins_all_four_state_gauges_at_zero() {
    let root = dir("pin");
    let e = L0Engine::<GD>::open(cfg(&root, 64), local(&root, "obj")).unwrap();
    let s = e.metrics().snapshot();
    assert_eq!(
        (
            s.manifest_parts,
            s.parts_local_only,
            s.parts_under_replicated,
            s.dedupe_window_records
        ),
        (0, 0, 0, 0),
        "a fresh engine must PRESENT all four at 0, not leave them absent"
    );
    // And they must be in the rendered text, not merely in the snapshot — an absent
    // series and a healthy zero are indistinguishable to every alert.
    let text = s.render_prometheus(GD::NAME);
    for name in [
        "manifest_parts",
        "parts_local_only",
        "parts_under_replicated",
        "dedupe_window_records",
    ] {
        assert!(
            text.contains(&format!("ehdb_l0_{name}{{dataset=\"{}\"}} 0", GD::NAME)),
            "ehdb_l0_{name} must be emitted at 0; got:\n{text}"
        );
    }
    println!("all four pinned at 0 and present in the exposition");
}

#[test]
fn manifest_parts_and_dedupe_window_move_with_real_work() {
    let root = dir("move");
    let mut e = L0Engine::<GD>::open(cfg(&root, 32), local(&root, "obj")).unwrap();
    append_n(&mut e, 200);
    e.flush_and_wait_uploads().unwrap();
    e.refresh_state_gauges();
    let s = e.metrics().snapshot();

    // ⚠ Named arguments, deliberately. The first version of this line used positional
    // args whose ORDER did not match its labels, so it reported
    // `under_replicated=200` on a single healthy replica — an impossible value that was
    // really `dedupe_window_records`. The assertions below were correct throughout; only
    // the human-readable line was wrong, and that is the line a person reads. A
    // mislabelled diagnostic is a measurement defect even when every assert passes.
    println!(
        "after 200 appends at seal=32: manifest_parts={parts} parts_local_only={local} \
         parts_under_replicated={under} dedupe_window_records={dedupe}",
        parts = s.manifest_parts,
        local = s.parts_local_only,
        under = s.parts_under_replicated,
        dedupe = s.dedupe_window_records
    );

    assert!(
        s.manifest_parts > 0,
        "manifest_parts stayed 0 after 200 appends at seal_max_records=32 — the gauge is \
         inert, and an inert gauge pinned at 0 reads exactly like a healthy one"
    );
    assert_eq!(
        s.manifest_parts,
        e.manifest_snapshot().parts.len() as u64,
        "the gauge must equal the manifest it is derived from"
    );
    assert!(
        s.dedupe_window_records > 0,
        "dedupe_window_records stayed 0 after 200 appends — inert"
    );
    // Single healthy replica: nothing may be local-only or under-replicated.
    assert_eq!(
        (s.parts_local_only, s.parts_under_replicated),
        (0, 0),
        "a single healthy replica must leave both deficit gauges at 0 — otherwise they \
         are measuring something other than a deficit"
    );
}

#[test]
fn parts_local_only_counts_a_part_that_no_replica_accepted() {
    let root = dir("localonly");
    // The ONLY replica refuses every write, so no part can become durable.
    let refusing = Arc::new(Refusing::new("refuse"));
    let as_sub: Arc<dyn DurableSubstrate> = refusing.clone();
    let mut e = L0Engine::<GD>::open_replicated(
        cfg(&root, 16),
        vec![ReplicaTarget::new("replica-0", as_sub)],
    )
    .unwrap();
    // Open succeeded against a writable replica; now it goes away.
    refusing.start_refusing();

    append_n(&mut e, 100);
    e.flush_and_wait_uploads().unwrap();
    e.refresh_state_gauges();
    let s = e.metrics().snapshot();

    println!(
        "all replicas refusing: manifest_parts={} local_only={} under_replicated={}",
        s.manifest_parts, s.parts_local_only, s.parts_under_replicated
    );

    assert!(
        s.manifest_parts > 0,
        "parts must still be sealed and catalogued when upload fails"
    );
    assert_eq!(
        s.parts_local_only, s.manifest_parts,
        "every part must be local-only when no replica accepted a write"
    );
    // ⭐ The distinction that makes two gauges rather than one: a part with ZERO
    // copies is NOT under-replicated, it is un-replicated. Collapsing the two
    // would make a total upload outage look like a partial one.
    assert_eq!(
        s.parts_under_replicated, 0,
        "a part with zero copies must not also count as under-replicated — that would \
         make a total upload failure indistinguishable from a partial one"
    );
}

#[test]
fn parts_under_replicated_counts_a_standing_deficit_the_durability_window_calls_done() {
    let root = dir("under");
    // replica-0 works, replica-1 refuses: every part lands 1 of 2 copies.
    let good = local(&root, "obj0");
    let bad = Arc::new(Refusing::new("refuse1"));
    let bad_sub: Arc<dyn DurableSubstrate> = bad.clone();
    let mut e = L0Engine::<GD>::open_replicated(
        cfg(&root, 16),
        vec![
            ReplicaTarget::new("replica-0", good),
            ReplicaTarget::new("replica-1", bad_sub),
        ],
    )
    .unwrap();
    // replica-1 fails after open: every subsequent part lands 1 of 2 copies.
    bad.start_refusing();

    append_n(&mut e, 100);
    e.flush_and_wait_uploads().unwrap();
    e.refresh_state_gauges();
    let s = e.metrics().snapshot();

    println!(
        "1 of 2 replicas accepting: manifest_parts={} local_only={} under_replicated={} \
         replica_writes={}",
        s.manifest_parts, s.parts_local_only, s.parts_under_replicated, s.replica_writes
    );

    assert!(s.manifest_parts > 0, "parts must be sealed");
    assert_eq!(
        s.parts_under_replicated, s.manifest_parts,
        "every part landed 1 of 2 copies, so every part is under-replicated"
    );
    assert_eq!(
        s.parts_local_only, 0,
        "these parts ARE durable on one replica, so none is local-only — the two gauges \
         must not both fire for one condition"
    );

    // The point of the gauge, stated as an assertion: the engine considers these
    // parts uploaded. Every replica in the manifest row is a real copy, the
    // durability window closed, and the deficit is permanent until a retry
    // re-drives it. Only this gauge reports it.
    let m = e.manifest_snapshot();
    for p in &m.parts {
        assert_eq!(
            p.replica_count(),
            1,
            "part {} should hold exactly one copy of the two requested",
            p.part_id
        );
        assert!(
            p.is_durable(),
            "part {} is considered durable by the engine — which is exactly why a \
             standing deficit needs its own signal",
            p.part_id
        );
    }
    println!(
        "{} parts are `is_durable() == true` while holding 1 of 2 copies — \
         the age-based durability window reports them done",
        m.parts.len()
    );
}

#[test]
fn the_gauges_track_state_down_as_well_as_up() {
    // A gauge that only ever rises is a counter with a misleading name.
    let root = dir("down");
    let mut e = L0Engine::<GD>::open(cfg(&root, 16), local(&root, "obj")).unwrap();
    append_n(&mut e, 200);
    e.flush_and_wait_uploads().unwrap();
    e.refresh_state_gauges();
    let before = e.metrics().snapshot().manifest_parts;
    assert!(before > 1, "need several parts to merge; got {before}");

    // Merge consumes parts, so the live part count must FALL.
    let merged = e.run_pending_merges().unwrap();
    e.flush_and_wait_uploads().unwrap();
    e.refresh_state_gauges();
    let after = e.metrics().snapshot().manifest_parts;

    println!("manifest_parts {before} -> {after} after {merged} merge(s)");
    if merged > 0 {
        assert!(
            after < before,
            "manifest_parts did not fall after {merged} merges ({before} -> {after}); \
             a gauge that only rises is a counter with a misleading name"
        );
    } else {
        // Be explicit rather than passing vacuously.
        panic!(
            "no merge ran, so the downward direction was never exercised — this test \
             proves nothing as written (parts={before})"
        );
    }
    assert_eq!(
        after,
        e.manifest_snapshot().parts.len() as u64,
        "the gauge must still equal the manifest after a merge"
    );
}
