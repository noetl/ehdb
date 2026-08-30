//! **The election and the store, composed** (noetl/ehdb#331 × noetl/ehdb#330).
//!
//! `lease_election.rs` proves the election state machine and `fencing_shadow.rs`
//! proves the store refuses a stale epoch. Neither proves the two **fit
//! together** — and until this file, nothing did: the election was never handed
//! to a fenced store anywhere in the tree.
//!
//! That composition is the precondition for two gates:
//!
//! * **G3** (promote the election) is pointless unless the token it mints is the
//!   one the store checks.
//! * **G2** (flip fencing to `enforce`) is an **outage** unless real tokens are
//!   already being issued — with every epoch at `0` the first writer to advance
//!   the marker fences every other one.
//!
//! So the ordering hazard recorded in the four-gate plan is asserted here rather
//! than only written down.
//!
//! Runs entirely in-process against `InMemoryLeaseStore` + a filesystem shared
//! backend. No cluster, no prod.

use std::sync::Arc;

use ehdb_reference::durable_eventlog_shared::{FilesystemSharedBackend, SharedSegmentBackend};
use ehdb_reference::election::{ElectionOutcome, InMemoryLeaseStore, ManualClock, ShardElection};
use ehdb_reference::fencing::{
    is_stale_epoch, FencedSharedBackend, FencingLedger, FencingMetrics, FencingMode,
};

const SHARD: u32 = 0;
const DUR: u64 = 15;

fn unique_dir(tag: &str) -> std::path::PathBuf {
    static N: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let n = N.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    std::env::temp_dir().join(format!("ehdb-edf-{tag}-{}-{n}-{nanos}", std::process::id()))
}

/// One cluster: a shared lease store, a shared clock, and one shared object
/// store that every node writes through.
struct Cluster {
    leases: Arc<InMemoryLeaseStore>,
    clock: Arc<ManualClock>,
    objects: std::path::PathBuf,
    ledger_dir: std::path::PathBuf,
    metrics: Arc<FencingMetrics>,
}

impl Cluster {
    fn new(tag: &str) -> Self {
        let base = unique_dir(tag);
        let objects = base.join("objects");
        let ledger_dir = base.join("fencing");
        std::fs::create_dir_all(&objects).unwrap();
        Self {
            leases: Arc::new(InMemoryLeaseStore::new()),
            clock: Arc::new(ManualClock::new(1_000_000)),
            objects,
            ledger_dir,
            metrics: FencingMetrics::new(),
        }
    }

    fn election(&self, id: &str) -> ShardElection<Arc<InMemoryLeaseStore>, Arc<ManualClock>> {
        ShardElection::new(self.leases.clone(), self.clock.clone(), id, SHARD)
            .with_duration_secs(DUR)
    }

    fn store(&self, mode: FencingMode) -> FencedSharedBackend<FilesystemSharedBackend> {
        let inner = FilesystemSharedBackend::open(&self.objects).unwrap();
        let ledger = FencingLedger::new(&self.ledger_dir).unwrap();
        FencedSharedBackend::new(inner, ledger)
            .with_mode(mode)
            .with_metrics(Arc::clone(&self.metrics))
    }

    fn stale_refused(&self) -> u64 {
        self.metrics
            .stale_refused
            .load(std::sync::atomic::Ordering::Relaxed)
    }
    fn stale_observed(&self) -> u64 {
        self.metrics
            .stale_observed
            .load(std::sync::atomic::Ordering::Relaxed)
    }
}

/// Adopt the node's current token, or refuse to write at all without one.
fn adopt(
    store: &FencedSharedBackend<FilesystemSharedBackend>,
    election: &ShardElection<Arc<InMemoryLeaseStore>, Arc<ManualClock>>,
) -> bool {
    match election.epoch() {
        Some(e) => {
            store.set_epoch(e);
            true
        }
        None => false,
    }
}

#[test]
fn the_token_the_election_mints_is_the_one_the_store_checks() {
    let c = Cluster::new("compose");
    let a = c.election("node-a");
    assert_eq!(
        a.try_acquire().unwrap(),
        ElectionOutcome::Acquired { epoch: 1 }
    );

    let store = c.store(FencingMode::Enforce);
    assert!(adopt(&store, &a));
    assert_eq!(store.epoch(), 1, "the store carries the elected token");
    store.put_segment(SHARD, 1, b"from-a").unwrap();
}

#[test]
fn after_failover_the_superseded_writer_is_refused_by_the_store() {
    // ⚠⚠ The whole point. A holds epoch 1 and keeps writing; B takes over at
    // epoch 2; A's next write must be refused BY THE STORE, not by A's own
    // good behaviour — a partitioned node cannot be relied on to check itself.
    let c = Cluster::new("failover");
    let a = c.election("node-a");
    let b = c.election("node-b");

    a.try_acquire().unwrap();
    let a_store = c.store(FencingMode::Enforce);
    adopt(&a_store, &a);
    a_store.put_segment(SHARD, 1, b"a-before").unwrap();

    // A partitions; its lease lapses; B takes over.
    c.clock.advance_millis((DUR + 1) * 1000);
    assert_eq!(
        b.try_acquire().unwrap(),
        ElectionOutcome::Acquired { epoch: 2 }
    );
    let b_store = c.store(FencingMode::Enforce);
    adopt(&b_store, &b);
    b_store.put_segment(SHARD, 2, b"b-after").unwrap();

    // ⚠ A is still holding its old token in memory — exactly the paused-holder
    // case a lease cannot prevent, which is why fencing exists.
    let err = a_store
        .put_segment(SHARD, 3, b"a-after-supersede")
        .expect_err("the store must refuse the superseded epoch");
    assert!(is_stale_epoch(&err), "typed as a fencing refusal: {err}");
    assert!(
        a_store.get_segment(SHARD, 3).unwrap().is_none(),
        "and the bytes must not have landed"
    );
    assert_eq!(c.stale_refused(), 1);
}

#[test]
fn a_writer_that_lost_its_lease_presents_no_token_at_all() {
    // The election's half of the contract: losing the lease must stop token
    // issuance, so a well-behaved writer cannot even attempt a write.
    let c = Cluster::new("lost");
    let a = c.election("node-a");
    let b = c.election("node-b");

    a.try_acquire().unwrap();
    let a_store = c.store(FencingMode::Enforce);
    assert!(adopt(&a_store, &a));

    c.clock.advance_millis((DUR + 1) * 1000);
    b.try_acquire().unwrap();
    assert_eq!(a.renew().unwrap(), ElectionOutcome::Lost);

    assert!(
        !adopt(&a_store, &a),
        "a node with no lease must present no token, so it never writes at all"
    );
    assert_eq!(a.epoch(), None);
}

#[test]
fn the_new_owner_can_write_after_taking_over() {
    // ⚠ The positive control. Without it, a store that refused every write after
    // any failover would pass the refusal test above and look correct.
    let c = Cluster::new("positive");
    let a = c.election("node-a");
    let b = c.election("node-b");

    a.try_acquire().unwrap();
    let a_store = c.store(FencingMode::Enforce);
    adopt(&a_store, &a);
    a_store.put_segment(SHARD, 1, b"a").unwrap();

    c.clock.advance_millis((DUR + 1) * 1000);
    b.try_acquire().unwrap();
    let b_store = c.store(FencingMode::Enforce);
    adopt(&b_store, &b);

    for id in 2..=4 {
        b_store
            .put_segment(SHARD, id, b"b")
            .unwrap_or_else(|e| panic!("the new owner must be able to write: {e}"));
    }
    assert_eq!(c.stale_refused(), 0, "nothing legitimate was refused");
    // And A's earlier bytes survive — fencing refuses future writes, it does not
    // retract accepted ones.
    assert_eq!(
        b_store.get_segment(SHARD, 1).unwrap().as_deref(),
        Some(&b"a"[..])
    );
}

#[test]
fn in_shadow_the_same_failover_is_counted_but_not_refused() {
    // The shadow period's contract: identical scenario, nothing refused, and the
    // gap between observed and refused is exactly what enabling enforce changes.
    let c = Cluster::new("shadow");
    let a = c.election("node-a");
    let b = c.election("node-b");

    a.try_acquire().unwrap();
    let a_store = c.store(FencingMode::Shadow);
    adopt(&a_store, &a);
    a_store.put_segment(SHARD, 1, b"a").unwrap();

    c.clock.advance_millis((DUR + 1) * 1000);
    b.try_acquire().unwrap();
    let b_store = c.store(FencingMode::Shadow);
    adopt(&b_store, &b);
    b_store.put_segment(SHARD, 2, b"b").unwrap();

    a_store
        .put_segment(SHARD, 3, b"a-superseded")
        .expect("shadow refuses nothing");
    assert_eq!(c.stale_observed(), 1, "but it is counted");
    assert_eq!(c.stale_refused(), 0);
    assert_eq!(
        a_store.get_segment(SHARD, 3).unwrap().as_deref(),
        Some(&b"a-superseded"[..]),
        "the superseded writer's bytes really landed — that is what shadow means"
    );
}

#[test]
fn enforcing_before_any_election_fences_every_writer() {
    // ⚠⚠ THE ORDERING HAZARD, asserted rather than only documented.
    //
    // The four-gate plan says enforcing (G2) before the election issues real
    // tokens (G3) is an OUTAGE, not a degradation. Reproduced: with no election
    // every writer's epoch is 0, so the moment ONE node is elected and advances
    // the marker, every un-elected writer is refused.
    let c = Cluster::new("hazard");

    // Two writers, neither elected — epoch stays 0.
    let un_elected = c.store(FencingMode::Enforce);
    assert_eq!(un_elected.epoch(), 0);
    un_elected
        .put_segment(SHARD, 1, b"works-while-everyone-is-zero")
        .expect("all-zero is self-consistent, so writes succeed");

    // Now a single node gets elected and writes.
    let a = c.election("node-a");
    a.try_acquire().unwrap();
    let elected = c.store(FencingMode::Enforce);
    adopt(&elected, &a);
    elected.put_segment(SHARD, 2, b"from-epoch-1").unwrap();

    // ⚠ Every writer still on epoch 0 is now fenced — including ones that are
    // legitimately the single writer for their own shard.
    let err = un_elected
        .put_segment(SHARD, 3, b"now-refused")
        .expect_err("an un-elected writer is fenced once any epoch is minted");
    assert!(is_stale_epoch(&err), "{err}");

    // The claim in plain terms: enable enforce first and writes stop.
    assert!(
        c.stale_refused() >= 1,
        "this is the outage the gate ordering exists to prevent"
    );
}
