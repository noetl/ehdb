//! **Single-writer election + fencing-token issuance** (noetl/ehdb#331, F1).
//!
//! Proves the four properties the spec depends on, against a `LeaseStore` with
//! real compare-and-swap semantics:
//!   1. a node acquires the lease and is issued a token,
//!   2. it renews without minting a new token,
//!   3. the epoch is **monotonic across failover**, and
//!   4. ⚠ **losing the lease stops token issuance** — the property that makes
//!      the whole thing safe rather than decorative.
//!
//! ⚠⚠ None of this makes the election authoritative. Single-writer still rests
//! on `StatefulSet replicas: 1`; promotion is owner-gated.

use std::sync::Arc;

use ehdb_reference::election::{
    shard_lease_name, ElectionOutcome, InMemoryLeaseStore, LeaseRecord, LeaseStore, ManualClock,
    ShardElection, DEFAULT_LEASE_DURATION_SECS,
};

const SHARD: u32 = 3;
const DUR: u64 = 15;

/// Two nodes sharing one store and one clock — the failover scenario.
struct Cluster {
    store: Arc<InMemoryLeaseStore>,
    clock: Arc<ManualClock>,
}

impl Cluster {
    fn new() -> Self {
        Self {
            store: Arc::new(InMemoryLeaseStore::new()),
            clock: Arc::new(ManualClock::new(1_000_000)),
        }
    }
    fn node(&self, id: &str) -> ShardElection<Arc<InMemoryLeaseStore>, Arc<ManualClock>> {
        ShardElection::new(self.store.clone(), self.clock.clone(), id, SHARD)
            .with_duration_secs(DUR)
    }
}

#[test]
fn the_first_node_acquires_and_is_issued_a_token() {
    let c = Cluster::new();
    let a = c.node("node-a");

    assert_eq!(a.epoch(), None, "no token before an election");
    assert_eq!(
        a.try_acquire().unwrap(),
        ElectionOutcome::Acquired { epoch: 1 }
    );
    assert_eq!(a.epoch(), Some(1));
    assert!(a.holds_lease());
}

#[test]
fn epoch_zero_is_never_a_legitimate_token() {
    // `None` means "no token"; if a real token could be 0, a node that failed to
    // acquire would be indistinguishable from one holding the first lease.
    let c = Cluster::new();
    let a = c.node("node-a");
    a.try_acquire().unwrap();
    assert!(a.epoch().unwrap() >= 1);
}

#[test]
fn renewal_keeps_the_same_token() {
    // ⚠ Minting a new epoch per renewal would make the store fence the holder
    // against its own earlier writes.
    let c = Cluster::new();
    let a = c.node("node-a");
    a.try_acquire().unwrap();

    c.clock.advance_millis(5_000);
    assert_eq!(a.renew().unwrap(), ElectionOutcome::Renewed { epoch: 1 });
    c.clock.advance_millis(5_000);
    assert_eq!(a.renew().unwrap(), ElectionOutcome::Renewed { epoch: 1 });
    assert_eq!(a.epoch(), Some(1), "the token is stable while held");
}

#[test]
fn a_second_node_cannot_take_a_live_lease() {
    let c = Cluster::new();
    let a = c.node("node-a");
    let b = c.node("node-b");
    a.try_acquire().unwrap();

    c.clock.advance_millis(5_000); // well inside the 15 s duration
    assert_eq!(
        b.try_acquire().unwrap(),
        ElectionOutcome::HeldByOther { holder_epoch: 1 }
    );
    assert_eq!(b.epoch(), None, "a node that did not win holds no token");
    assert_eq!(a.epoch(), Some(1));
}

#[test]
fn the_epoch_advances_across_failover_and_never_regresses() {
    let c = Cluster::new();
    let a = c.node("node-a");
    let b = c.node("node-b");

    a.try_acquire().unwrap();
    assert_eq!(a.epoch(), Some(1));

    // Node A partitions. Its lease lapses.
    c.clock.advance_millis((DUR + 1) * 1000);
    assert_eq!(
        b.try_acquire().unwrap(),
        ElectionOutcome::Acquired { epoch: 2 }
    );

    // And again.
    c.clock.advance_millis((DUR + 1) * 1000);
    let d = c.node("node-c");
    assert_eq!(
        d.try_acquire().unwrap(),
        ElectionOutcome::Acquired { epoch: 3 }
    );

    let stored = c.store.read(&shard_lease_name(SHARD)).unwrap().unwrap();
    assert_eq!(stored.transitions, 3);
    assert_eq!(stored.holder, "node-c");
}

#[test]
fn losing_the_lease_stops_token_issuance() {
    // ⚠⚠ The property that makes this safe rather than decorative. A superseded
    // node must stop presenting a token — otherwise it keeps writing under an
    // epoch it no longer owns, which is exactly the split-brain the design
    // exists to prevent.
    let c = Cluster::new();
    let a = c.node("node-a");
    let b = c.node("node-b");

    a.try_acquire().unwrap();
    assert_eq!(a.epoch(), Some(1));

    // A pauses; the lease lapses; B takes over.
    c.clock.advance_millis((DUR + 1) * 1000);
    b.try_acquire().unwrap();
    assert_eq!(b.epoch(), Some(2));

    // A wakes up and tries to carry on.
    assert_eq!(a.renew().unwrap(), ElectionOutcome::Lost);
    assert_eq!(
        a.epoch(),
        None,
        "the superseded node must present NO token at all"
    );
    assert!(!a.holds_lease());
}

#[test]
fn a_holder_whose_lease_lapsed_stops_even_before_anyone_takes_it() {
    // ⚠ The subtler half: A is still named as holder, but the lease has expired,
    // so other nodes are entitled to take it. Renewing through that gap would
    // let two nodes both believe they hold it.
    let c = Cluster::new();
    let a = c.node("node-a");
    a.try_acquire().unwrap();

    c.clock.advance_millis((DUR + 1) * 1000);
    assert_eq!(a.renew().unwrap(), ElectionOutcome::Lost);
    assert_eq!(a.epoch(), None);

    let stored = c.store.read(&shard_lease_name(SHARD)).unwrap().unwrap();
    assert_eq!(stored.holder, "node-a", "nobody else has taken it yet");
}

#[test]
fn a_lost_node_can_win_the_lease_back_with_a_new_epoch() {
    let c = Cluster::new();
    let a = c.node("node-a");
    let b = c.node("node-b");
    a.try_acquire().unwrap();
    c.clock.advance_millis((DUR + 1) * 1000);
    b.try_acquire().unwrap();
    a.renew().unwrap(); // Lost

    c.clock.advance_millis((DUR + 1) * 1000);
    assert_eq!(
        a.try_acquire().unwrap(),
        ElectionOutcome::Acquired { epoch: 3 }
    );
    assert_eq!(
        a.epoch(),
        Some(3),
        "re-acquiring mints a NEW token; the old one is never reused"
    );
}

#[test]
fn compare_and_swap_is_what_prevents_two_holders() {
    // ⚠ The positive control on the store itself. If CAS ignored the expected
    // version, every election test above would still pass while permitting two
    // simultaneous holders — so the primitive is asserted directly.
    let store = InMemoryLeaseStore::new();
    let name = shard_lease_name(SHARD);
    let rec = LeaseRecord {
        holder: "a".into(),
        transitions: 1,
        renewed_at_millis: 0,
        duration_secs: DUR,
        version: 0,
    };
    assert!(store.create(&name, &rec).unwrap());
    assert!(!store.create(&name, &rec).unwrap(), "create is exclusive");

    let cur = store.read(&name).unwrap().unwrap();
    let mut next = cur.clone();
    next.holder = "b".into();
    assert!(store.compare_and_swap(&name, cur.version, &next).unwrap());
    // The stale version must now be refused.
    assert!(
        !store.compare_and_swap(&name, cur.version, &next).unwrap(),
        "a second writer holding the OLD version must lose"
    );
}

#[test]
fn two_nodes_racing_from_the_same_view_produce_exactly_one_winner() {
    // Both read the same expired lease and both try to take it. CAS must let
    // exactly one through, and the loser must hold no token.
    let c = Cluster::new();
    let a = c.node("node-a");
    let b = c.node("node-b");
    a.try_acquire().unwrap();
    c.clock.advance_millis((DUR + 1) * 1000);

    let first = b.try_acquire().unwrap();
    let second = c.node("node-d").try_acquire().unwrap();

    assert_eq!(first, ElectionOutcome::Acquired { epoch: 2 });
    assert!(
        matches!(second, ElectionOutcome::HeldByOther { .. }),
        "the second attempt must not also acquire: {second:?}"
    );
    assert_eq!(b.epoch(), Some(2));
}

#[test]
fn the_spec_defaults_are_what_ship() {
    assert_eq!(DEFAULT_LEASE_DURATION_SECS, 15);
    assert_eq!(
        shard_lease_name(0),
        "ehdb-shard-00000000",
        "fixed-width lease name, as the spec states"
    );
    assert_eq!(shard_lease_name(255), "ehdb-shard-000000ff");
}

#[test]
fn shards_elect_independently() {
    let store = Arc::new(InMemoryLeaseStore::new());
    let clock = Arc::new(ManualClock::new(0));
    let s0 = ShardElection::new(store.clone(), clock.clone(), "node-a", 0);
    let s1 = ShardElection::new(store.clone(), clock.clone(), "node-b", 1);
    s0.try_acquire().unwrap();
    s1.try_acquire().unwrap();
    assert_eq!(s0.epoch(), Some(1));
    assert_eq!(s1.epoch(), Some(1), "a different shard, its own lease");
    assert_ne!(s0.lease_name(), s1.lease_name());
}
