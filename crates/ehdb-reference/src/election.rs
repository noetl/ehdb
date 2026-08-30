//! **Single-writer election and fencing-token issuance** (noetl/ehdb#331, F1).
//! Implements `docs/spec/writer-election-and-fencing.md` §3 and §4.1.
//!
//! ## The gap this closes
//!
//! No epoch, fencing token, lease or election existed anywhere. Single-writer
//! per shard rested entirely on `StatefulSet replicas: 1` — an **orchestration
//! preference, not a mutual-exclusion primitive**. A partitioned node whose
//! kubelet is unreachable shows as `Terminating` while its process keeps
//! appending, and Kubernetes may schedule a replacement during that window.
//!
//! ⚠⚠ Not prospective: the event-log tier has been `primary` and serving on prod
//! since 2026-08-13, so the tier most dependent on single-writer ordering is the
//! one already running without enforcement.
//!
//! ## ⚠⚠ Wired, but NOT authoritative
//!
//! Nothing here decides who writes. The election runs alongside the existing
//! arrangement and **issues tokens**; single-writer still rests on
//! `replicas: 1` until the owner promotes it. Promotion is a separate,
//! owner-gated step.
//!
//! ## Why Leases and not Raft
//!
//! The API server's compare-and-swap on `resourceVersion` *is* the mutual
//! exclusion, and etcd behind it already is the Raft cluster. Running a second
//! consensus cluster adds operational surface without adding a guarantee.
//!
//! ⚠ **A Lease elects; it does not fence.** Expiry is decided by *clocks*, and a
//! paused holder can believe it still holds a lease that expired elsewhere. The
//! fencing half is Invariant F in [`crate::fencing`] — the *store* refusing a
//! stale epoch. Both are required; neither is sufficient.
//!
//! ## What is implemented here, and what is not
//!
//! The **election state machine** — acquire, renew, expiry, monotonic epoch,
//! and the loss of a lease stopping token issuance — is implemented and proven
//! against a [`LeaseStore`] with real compare-and-swap semantics.
//!
//! ⚠ The **Kubernetes adapter is not implemented**. This workspace has no HTTP
//! or `kube` client, and pulling one in is a dependency decision rather than a
//! detail. [`LeaseStore`] is the seam: a K8s implementation maps
//! `metadata.resourceVersion` → [`LeaseRecord::version`] and
//! `spec.leaseTransitions` → [`LeaseRecord::transitions`], and needs an RBAC
//! grant on `coordination.k8s.io/leases` — which is owner-run.

use std::collections::HashMap;
use std::sync::Mutex;

use ehdb_core::{EhdbError, Result};

/// Lease duration, seconds (spec §3).
pub const DEFAULT_LEASE_DURATION_SECS: u64 = 15;
/// Renewal interval, seconds. Three missed renewals lose the lease.
pub const DEFAULT_RENEW_INTERVAL_SECS: u64 = 5;

/// One shard's lease, as the store holds it.
///
/// Mirrors `coordination.k8s.io/v1` Lease: `holder` is `spec.holderIdentity`,
/// `transitions` is `spec.leaseTransitions`, `version` is
/// `metadata.resourceVersion`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LeaseRecord {
    pub holder: String,
    /// Incremented on **every holder change**. This is the fencing token: its
    /// monotonicity is guaranteed by the same compare-and-swap that granted the
    /// lease, not by anything this code does.
    pub transitions: u64,
    /// When the holder last renewed, in millis on the caller's clock.
    pub renewed_at_millis: u64,
    pub duration_secs: u64,
    /// Opaque CAS token. A write succeeds only if this still matches.
    pub version: u64,
}

impl LeaseRecord {
    /// Whether the lease has expired as of `now_millis`.
    pub fn is_expired(&self, now_millis: u64) -> bool {
        now_millis > self.renewed_at_millis + self.duration_secs * 1000
    }
}

/// The compare-and-swap primitive an election needs. One implementation is the
/// Kubernetes API server; [`InMemoryLeaseStore`] is the test/e2e one.
pub trait LeaseStore: Send + Sync {
    fn read(&self, name: &str) -> Result<Option<LeaseRecord>>;

    /// Create `record` only if `name` does not exist. `Ok(false)` on a race.
    fn create(&self, name: &str, record: &LeaseRecord) -> Result<bool>;

    /// Replace `name` only if its stored `version` equals `expected_version`.
    /// `Ok(false)` when it does not — someone else wrote first.
    ///
    /// ⚠ This is the mutual exclusion. An implementation that ignores
    /// `expected_version` silently permits two holders.
    fn compare_and_swap(
        &self,
        name: &str,
        expected_version: u64,
        record: &LeaseRecord,
    ) -> Result<bool>;
}

/// Sharing one store across every shard's election is the normal arrangement,
/// so `Arc<S>` is itself a store.
impl<T: LeaseStore + ?Sized> LeaseStore for std::sync::Arc<T> {
    fn read(&self, name: &str) -> Result<Option<LeaseRecord>> {
        (**self).read(name)
    }
    fn create(&self, name: &str, record: &LeaseRecord) -> Result<bool> {
        (**self).create(name, record)
    }
    fn compare_and_swap(
        &self,
        name: &str,
        expected_version: u64,
        record: &LeaseRecord,
    ) -> Result<bool> {
        (**self).compare_and_swap(name, expected_version, record)
    }
}

/// A monotonic millisecond clock, injectable so expiry is testable without
/// sleeping.
pub trait Clock: Send + Sync {
    fn now_millis(&self) -> u64;
}

/// Wall clock.
#[derive(Debug, Default)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now_millis(&self) -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0)
    }
}

/// A clock the caller advances by hand.
#[derive(Debug, Default)]
pub struct ManualClock(std::sync::atomic::AtomicU64);

impl ManualClock {
    pub fn new(start_millis: u64) -> Self {
        Self(std::sync::atomic::AtomicU64::new(start_millis))
    }
    pub fn advance_millis(&self, by: u64) {
        self.0.fetch_add(by, std::sync::atomic::Ordering::SeqCst);
    }
}

impl Clock for ManualClock {
    fn now_millis(&self) -> u64 {
        self.0.load(std::sync::atomic::Ordering::SeqCst)
    }
}

impl<T: Clock + ?Sized> Clock for std::sync::Arc<T> {
    fn now_millis(&self) -> u64 {
        (**self).now_millis()
    }
}

/// The lease name for a shard — fixed width, matching the spec.
pub fn shard_lease_name(shard: u32) -> String {
    format!("ehdb-shard-{shard:08x}")
}

/// What happened on a call to [`ShardElection::try_acquire`] or
/// [`ShardElection::renew`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ElectionOutcome {
    /// Took the lease; `epoch` is the newly-minted fencing token.
    Acquired { epoch: u64 },
    /// Still the holder; the lease was renewed.
    Renewed { epoch: u64 },
    /// Someone else holds a live lease.
    HeldByOther { holder_epoch: u64 },
    /// ⚠ We believed we held it and no longer do. Token issuance stops.
    Lost,
}

/// Per-shard leader election over a [`LeaseStore`].
///
/// ⚠⚠ This does not gate writes. It issues a token; whether anything acts on
/// that token is a separate, owner-gated decision.
pub struct ShardElection<S: LeaseStore, C: Clock> {
    store: S,
    clock: C,
    identity: String,
    shard: u32,
    duration_secs: u64,
    /// The token we currently hold, if any. `None` is the safe state: no lease,
    /// no token, and therefore nothing for the store to accept.
    held: Mutex<Option<u64>>,
}

impl<S: LeaseStore, C: Clock> ShardElection<S, C> {
    pub fn new(store: S, clock: C, identity: impl Into<String>, shard: u32) -> Self {
        Self {
            store,
            clock,
            identity: identity.into(),
            shard,
            duration_secs: DEFAULT_LEASE_DURATION_SECS,
            held: Mutex::new(None),
        }
    }

    pub fn with_duration_secs(mut self, secs: u64) -> Self {
        self.duration_secs = secs;
        self
    }

    pub fn lease_name(&self) -> String {
        shard_lease_name(self.shard)
    }

    /// The current fencing token, or `None` when this node does not hold the
    /// lease.
    ///
    /// ⚠ `None` is not an error state to be worked around. A node without a
    /// token has not been elected, and issuing one anyway is precisely the
    /// split-brain this exists to prevent.
    pub fn epoch(&self) -> Option<u64> {
        *self.held.lock().unwrap()
    }

    pub fn holds_lease(&self) -> bool {
        self.epoch().is_some()
    }

    /// Try to become the holder. Acquires only when the lease is absent or has
    /// expired; the compare-and-swap is what makes concurrent attempts safe.
    pub fn try_acquire(&self) -> Result<ElectionOutcome> {
        let now = self.clock.now_millis();
        match self.store.read(&self.lease_name())? {
            None => {
                // First ever holder. transitions starts at 1 so epoch 0 always
                // means "no token" and can never be a legitimate one.
                let record = LeaseRecord {
                    holder: self.identity.clone(),
                    transitions: 1,
                    renewed_at_millis: now,
                    duration_secs: self.duration_secs,
                    version: 0,
                };
                if self.store.create(&self.lease_name(), &record)? {
                    *self.held.lock().unwrap() = Some(1);
                    return Ok(ElectionOutcome::Acquired { epoch: 1 });
                }
                // Lost the create race; re-read on the next tick.
                self.drop_token();
                Ok(ElectionOutcome::HeldByOther { holder_epoch: 0 })
            }
            Some(existing) => {
                if existing.holder == self.identity && !existing.is_expired(now) {
                    return self.renew();
                }
                if !existing.is_expired(now) {
                    self.drop_token();
                    return Ok(ElectionOutcome::HeldByOther {
                        holder_epoch: existing.transitions,
                    });
                }
                let epoch = existing.transitions + 1;
                let record = LeaseRecord {
                    holder: self.identity.clone(),
                    transitions: epoch,
                    renewed_at_millis: now,
                    duration_secs: self.duration_secs,
                    version: existing.version,
                };
                if self
                    .store
                    .compare_and_swap(&self.lease_name(), existing.version, &record)?
                {
                    *self.held.lock().unwrap() = Some(epoch);
                    Ok(ElectionOutcome::Acquired { epoch })
                } else {
                    // Someone else CAS'd first — they are the new holder.
                    self.drop_token();
                    Ok(ElectionOutcome::HeldByOther {
                        holder_epoch: existing.transitions,
                    })
                }
            }
        }
    }

    /// Renew an already-held lease.
    ///
    /// ⚠ Renewal does **not** change `transitions`, so the epoch is stable while
    /// a holder keeps its lease. Minting a new token per renewal would make the
    /// store fence the holder against itself.
    pub fn renew(&self) -> Result<ElectionOutcome> {
        let now = self.clock.now_millis();
        let Some(existing) = self.store.read(&self.lease_name())? else {
            self.drop_token();
            return Ok(ElectionOutcome::Lost);
        };
        if existing.holder != self.identity {
            // Someone took it while we were away.
            self.drop_token();
            return Ok(ElectionOutcome::Lost);
        }
        if existing.is_expired(now) {
            // ⚠ We still name ourselves as holder, but the lease lapsed — other
            // nodes are entitled to take it. Treat that as lost rather than
            // renewing through the gap, or two nodes could both believe they
            // hold it.
            self.drop_token();
            return Ok(ElectionOutcome::Lost);
        }
        let record = LeaseRecord {
            renewed_at_millis: now,
            ..existing.clone()
        };
        if self
            .store
            .compare_and_swap(&self.lease_name(), existing.version, &record)?
        {
            *self.held.lock().unwrap() = Some(existing.transitions);
            Ok(ElectionOutcome::Renewed {
                epoch: existing.transitions,
            })
        } else {
            self.drop_token();
            Ok(ElectionOutcome::Lost)
        }
    }

    /// Voluntarily release. Best-effort: the lease also lapses on its own.
    pub fn release(&self) -> Result<()> {
        self.drop_token();
        Ok(())
    }

    fn drop_token(&self) {
        *self.held.lock().unwrap() = None;
    }
}

/// An in-process [`LeaseStore`] with real compare-and-swap semantics — the
/// non-prod store the election is proven against.
#[derive(Debug, Default)]
pub struct InMemoryLeaseStore {
    inner: Mutex<HashMap<String, LeaseRecord>>,
}

impl InMemoryLeaseStore {
    pub fn new() -> Self {
        Self::default()
    }
    /// Overwrite ignoring CAS — for arranging a test's starting state only.
    pub fn force_put(&self, name: &str, record: LeaseRecord) {
        self.inner.lock().unwrap().insert(name.to_string(), record);
    }
}

impl LeaseStore for InMemoryLeaseStore {
    fn read(&self, name: &str) -> Result<Option<LeaseRecord>> {
        Ok(self.inner.lock().unwrap().get(name).cloned())
    }

    fn create(&self, name: &str, record: &LeaseRecord) -> Result<bool> {
        let mut g = self.inner.lock().unwrap();
        if g.contains_key(name) {
            return Ok(false);
        }
        let mut r = record.clone();
        r.version = 1;
        g.insert(name.to_string(), r);
        Ok(true)
    }

    fn compare_and_swap(
        &self,
        name: &str,
        expected_version: u64,
        record: &LeaseRecord,
    ) -> Result<bool> {
        let mut g = self.inner.lock().unwrap();
        let Some(cur) = g.get(name) else {
            return Err(EhdbError::NotFound(format!("lease {name}")));
        };
        if cur.version != expected_version {
            return Ok(false);
        }
        let mut r = record.clone();
        r.version = cur.version + 1;
        g.insert(name.to_string(), r);
        Ok(true)
    }
}
