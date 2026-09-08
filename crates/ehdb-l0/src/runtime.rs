//! **D8 — runtime registration** (worker registration / heartbeat / runtime
//! contract, the `noetl.runtime` table), RFC §0.1.
//!
//! Worker lifecycle with **register**, **heartbeat**, **deregister**, and
//! **list-live** — over immutable parts. Modeled as an append-only log of
//! `RuntimeOp`; a worker's current state is its latest op (a fold that, since
//! [`read_index_after`] returns a worker's ops in order, is "the last one"). A
//! monotonic per-worker `heartbeat` rides each op, so liveness is a
//! wall-clock-free predicate: a worker is live if its latest op is not a
//! deregister, and `list_live_since(min)` evicts the stale by heartbeat
//! watermark (the caller advances the watermark on its own tick).
//!
//! Fixed shape: **sort key** = `op_seq`; **partition** + **index dim** =
//! `worker_id` (so a per-worker read touches exactly one worker's history).
//!
//! [`read_index_after`]: crate::engine::L0Engine::read_index_after

use std::collections::BTreeMap;
use std::sync::Arc;

use ehdb_core::{EhdbError, Result};
use serde::{Deserialize, Serialize};

use crate::dataset::{shard_for_execution, Dataset};
use crate::engine::{L0Config, L0Engine, ReplicaTarget};
use crate::substrate::DurableSubstrate;

/// The D8 dataset id.
pub const DATASET_D8_RUNTIME: &str = "d8_runtime";

/// A worker-lifecycle transition.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RuntimeEvent {
    Register,
    Heartbeat,
    Deregister,
}

/// One runtime lifecycle op in the log (the D8 record schema).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RuntimeOp {
    /// Op-log position (the fixed sort key).
    pub op_seq: u64,
    /// The worker id (partition + index dim).
    pub worker_id: String,
    /// The lifecycle transition.
    pub event: RuntimeEvent,
    /// Monotonic per-worker heartbeat counter (1 at register, +1 per beat).
    pub heartbeat: u64,
    /// The runtime contract this worker advertised (pool / arch / capacity
    /// descriptor). Carried on register, echoed on heartbeat, empty on
    /// deregister.
    pub contract: String,
}

/// **D8 runtime dataset.** Sort key `op_seq`; partition + index dim `worker_id`.
#[derive(Debug, Clone, Copy)]
pub struct RuntimeDataset;

impl Dataset for RuntimeDataset {
    type Record = RuntimeOp;
    const NAME: &'static str = DATASET_D8_RUNTIME;

    fn sort_key(r: &RuntimeOp) -> u64 {
        r.op_seq
    }
    fn partition(r: &RuntimeOp, shard_count: u32) -> u32 {
        shard_for_execution(&r.worker_id, shard_count)
    }
    fn index_key(r: &RuntimeOp) -> &str {
        &r.worker_id
    }
    fn read_partition(worker_id: &str, shard_count: u32) -> u32 {
        shard_for_execution(worker_id, shard_count)
    }
}

/// A live worker's current registration (the fold result).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeState {
    pub worker_id: String,
    pub contract: String,
    /// The worker's latest heartbeat watermark.
    pub heartbeat: u64,
}

/// Longest accepted `worker_id`. Bounded because it becomes a substrate key and
/// an index dimension; an unbounded id is an unbounded key.
pub const MAX_WORKER_ID_LEN: usize = 256;

/// Longest accepted `contract`. Bounded for the same reason the id is: a
/// membership op arriving over gossip must not be able to cost arbitrary bytes.
pub const MAX_CONTRACT_LEN: usize = 4096;

/// **Structural validation of a membership op, applied at APPEND time**
/// (noetl/ai-meta#332).
///
/// # Why append and not read
///
/// Membership here is *persisted*. A rejected-at-read op is still in the log and
/// every subsequent fold replays it, so a poisoned entry outlives the thing that
/// wrote it. Rejecting at append is the only point at which it does not become
/// permanent.
///
/// # What this does and does not cover
///
/// This is the half that is verifiable **without network trust**: the op is
/// well-formed, its identity is usable as a key, and its fields agree with its
/// event. It deliberately does **not** answer *"is this node allowed to say
/// this"* — that is the signature/authorisation half, and it plugs in at
/// [`OpOrigin`] once a signing layer exists.
///
/// Splitting them matters: structural validity is checkable today and blocks the
/// malformed-input class immediately, while waiting for the auth layer would
/// leave both classes open.
pub fn validate_op(op: &RuntimeOp) -> Result<()> {
    let id = op.worker_id.as_str();
    if id.is_empty() {
        return Err(EhdbError::InvalidState(
            "runtime op has an empty worker_id".into(),
        ));
    }
    if id.len() > MAX_WORKER_ID_LEN {
        return Err(EhdbError::InvalidState(format!(
            "worker_id is {} bytes, over the {MAX_WORKER_ID_LEN} limit",
            id.len()
        )));
    }
    // ⚠ The id becomes part of a substrate key. `LocalFsSubstrate::resolve`
    // already refuses traversal, but refusing here means a malicious id never
    // reaches the log at all rather than being caught per-read forever after.
    //
    // ⚠ Narrow on purpose: the charset rule below already rejects `/`,
    // whitespace and control characters, so the ONLY input this catches that the
    // charset does not is a dot-run like `..` — dots are legal in an id
    // (`shard.3`). Measured, not assumed: a mutation deleting this check passed
    // every test until `".."` was added to the fixture, because every other
    // traversal string was being caught by the charset rule instead. A check
    // whose unique contribution is unknown is a check nobody can maintain.
    if id.split('.').all(|seg| seg.is_empty()) {
        return Err(EhdbError::InvalidState(format!(
            "worker_id {id:?} is a dot-run and would resolve as a path segment"
        )));
    }
    if !id
        .chars()
        .all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.' | ':'))
    {
        return Err(EhdbError::InvalidState(format!(
            "worker_id {id:?} has characters outside [A-Za-z0-9-_.:]"
        )));
    }
    if op.contract.len() > MAX_CONTRACT_LEN {
        return Err(EhdbError::InvalidState(format!(
            "contract is {} bytes, over the {MAX_CONTRACT_LEN} limit",
            op.contract.len()
        )));
    }
    // Field/event agreement: the heartbeat watermark is 1-based and only a
    // register resets it. A zero watermark on a live event is malformed, and
    // would make `list_live_since(1)` silently drop a node that is beating.
    match op.event {
        RuntimeEvent::Register | RuntimeEvent::Heartbeat if op.heartbeat == 0 => {
            Err(EhdbError::InvalidState(format!(
                "{:?} op for {id} carries heartbeat 0; the watermark is 1-based",
                op.event
            )))
        }
        _ => Ok(()),
    }
}

/// **The seam for the network-trust half** (noetl/ai-meta#332).
///
/// Structural validity ([`validate_op`]) says the op is well-formed. It cannot
/// say whether the sender was *entitled* to write it — that needs a signed
/// gossip message and a cluster key, neither of which exists yet.
///
/// This trait is where that check plugs in. When foca and the signing layer land,
/// a `SignedGossipOrigin` implements it and verifies the op against the message
/// signature and the sender's identity, so an unauthenticated peer cannot append
/// a `Register` claiming to be another shard's instance.
///
/// ⚠ The default is [`TrustedLocalOrigin`], and it is named for what it assumes
/// rather than described as safe: every caller is in-process today, so there is
/// nothing to authenticate. **Wiring gossip without replacing it is the
/// routing-poisoning path**, and because membership is persisted the poisoning
/// would be durable.
pub trait OpOrigin: Send + Sync {
    /// Return `Err` to refuse the op. Called at append, after [`validate_op`].
    fn authorize(&self, op: &RuntimeOp) -> Result<()>;
}

/// The default origin: accepts every structurally-valid op.
///
/// Correct **only** while every caller is in-process. See [`OpOrigin`].
#[derive(Debug, Clone, Copy, Default)]
pub struct TrustedLocalOrigin;

impl OpOrigin for TrustedLocalOrigin {
    fn authorize(&self, _op: &RuntimeOp) -> Result<()> {
        Ok(())
    }
}

/// **The D8 runtime store** — register / heartbeat / deregister / list-live over
/// the generic engine.
pub struct RuntimeStore {
    engine: L0Engine<RuntimeDataset>,
    /// Who is allowed to write membership. See [`OpOrigin`].
    ///
    /// Boxed rather than generic so swapping in the signed origin later does not
    /// change `RuntimeStore`'s type and ripple through every caller.
    origin: Box<dyn OpOrigin>,
}

impl RuntimeStore {
    /// Config default for D8 rooted at `local_root`.
    pub fn config(local_root: impl Into<std::path::PathBuf>) -> L0Config {
        L0Config::for_dataset(DATASET_D8_RUNTIME, local_root)
    }

    /// Open a single-replica runtime store.
    pub fn open(config: L0Config, substrate: Arc<dyn DurableSubstrate>) -> Result<Self> {
        Ok(Self {
            engine: L0Engine::open(config, substrate)?,
            origin: Box::new(TrustedLocalOrigin),
        })
    }
    /// Open an N-way replicated runtime store.
    pub fn open_replicated(config: L0Config, replicas: Vec<ReplicaTarget>) -> Result<Self> {
        Ok(Self {
            engine: L0Engine::open_replicated(config, replicas)?,
            origin: Box::new(TrustedLocalOrigin),
        })
    }
    /// Cold-load a single-replica runtime store.
    pub fn cold_load(config: L0Config, substrate: Arc<dyn DurableSubstrate>) -> Result<Self> {
        Ok(Self {
            engine: L0Engine::cold_load(config, substrate)?,
            origin: Box::new(TrustedLocalOrigin),
        })
    }
    /// Cold-load an N-way replicated runtime store.
    pub fn cold_load_replicated(config: L0Config, replicas: Vec<ReplicaTarget>) -> Result<Self> {
        Ok(Self {
            engine: L0Engine::cold_load_replicated(config, replicas)?,
            origin: Box::new(TrustedLocalOrigin),
        })
    }

    /// The latest op for a worker (index-pruned read, last wins).
    fn latest(&self, worker_id: &str) -> Result<Option<RuntimeOp>> {
        Ok(self
            .engine
            .read_index_after(worker_id, 0)?
            .into_iter()
            .next_back())
    }

    /// Install the origin check used at append (noetl/ai-meta#332).
    ///
    /// This is how the signed-gossip authorisation is wired in once foca and the
    /// signing layer exist: build the store, then hand it an [`OpOrigin`] that
    /// verifies the message signature against the sender's identity.
    pub fn with_origin(mut self, origin: Box<dyn OpOrigin>) -> Self {
        self.origin = origin;
        self
    }

    /// **register** — register (or re-register) `worker_id` with its runtime
    /// `contract`, resetting its heartbeat watermark to 1. Returns the watermark.
    pub fn register(&mut self, worker_id: &str, contract: impl Into<String>) -> Result<u64> {
        self.append(worker_id, RuntimeEvent::Register, 1, contract.into())?;
        Ok(1)
    }

    /// **heartbeat** — advance `worker_id`'s heartbeat watermark by one, echoing
    /// its current contract. Returns the new watermark, or `None` if the worker
    /// is not currently live (never registered or deregistered) — the caller
    /// should `register` first.
    pub fn heartbeat(&mut self, worker_id: &str) -> Result<Option<u64>> {
        match self.latest(worker_id)? {
            Some(op) if op.event != RuntimeEvent::Deregister => {
                let next = op.heartbeat + 1;
                self.append(worker_id, RuntimeEvent::Heartbeat, next, op.contract)?;
                Ok(Some(next))
            }
            _ => Ok(None),
        }
    }

    /// **deregister** — mark `worker_id` gone (drops out of `list_live`).
    /// Returns `true` if it was live, `false` if already absent.
    pub fn deregister(&mut self, worker_id: &str) -> Result<bool> {
        match self.latest(worker_id)? {
            Some(op) if op.event != RuntimeEvent::Deregister => {
                self.append(
                    worker_id,
                    RuntimeEvent::Deregister,
                    op.heartbeat,
                    String::new(),
                )?;
                Ok(true)
            }
            _ => Ok(false),
        }
    }

    /// The current registration of one worker, or `None` if not live.
    pub fn get(&self, worker_id: &str) -> Result<Option<RuntimeState>> {
        Ok(self.latest(worker_id)?.and_then(|op| {
            if op.event == RuntimeEvent::Deregister {
                None
            } else {
                Some(RuntimeState {
                    worker_id: op.worker_id,
                    contract: op.contract,
                    heartbeat: op.heartbeat,
                })
            }
        }))
    }

    /// **list-live** — every currently-registered worker (latest op not a
    /// deregister), in worker-id order.
    pub fn list_live(&self) -> Result<Vec<RuntimeState>> {
        self.list_live_since(0)
    }

    /// **list-live (fresh)** — live workers whose heartbeat watermark is at least
    /// `min_heartbeat`, in worker-id order. A caller that tracks a rolling
    /// watermark uses this to drop workers that have stopped beating without an
    /// explicit deregister (crash), no wall-clock needed.
    pub fn list_live_since(&self, min_heartbeat: u64) -> Result<Vec<RuntimeState>> {
        let all = self.engine.replay_all()?; // sorted by op_seq
        let mut latest: BTreeMap<String, RuntimeOp> = BTreeMap::new();
        for op in all {
            latest.insert(op.worker_id.clone(), op); // last op_seq wins
        }
        Ok(latest
            .into_values()
            .filter(|op| op.event != RuntimeEvent::Deregister && op.heartbeat >= min_heartbeat)
            .map(|op| RuntimeState {
                worker_id: op.worker_id,
                contract: op.contract,
                heartbeat: op.heartbeat,
            })
            .collect())
    }

    fn append(
        &mut self,
        worker_id: &str,
        event: RuntimeEvent,
        heartbeat: u64,
        contract: String,
    ) -> Result<()> {
        let op_seq = self.engine.global_sequence() + 1;
        let op = RuntimeOp {
            op_seq,
            worker_id: worker_id.to_string(),
            event,
            heartbeat,
            contract,
        };
        // noetl/ai-meta#332 — validate BEFORE the append, not on read. A
        // persisted bad op is replayed by every subsequent fold; this is the only
        // point at which refusing it keeps it out of the log.
        validate_op(&op)?;
        self.origin.authorize(&op)?;
        self.engine.append_record(op)?;
        Ok(())
    }

    /// Flush all sealed parts to the durable replicas.
    pub fn flush_and_wait(&mut self) -> Result<()> {
        self.engine.flush_and_wait_uploads()
    }
    /// Run background merge/compaction over the runtime log.
    pub fn run_pending_merges(&mut self) -> Result<usize> {
        self.engine.run_pending_merges()
    }
    /// The underlying generic engine.
    pub fn engine(&self) -> &L0Engine<RuntimeDataset> {
        &self.engine
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::substrate::LocalFsSubstrate;

    fn store(dir: &std::path::Path) -> RuntimeStore {
        let sub: Arc<dyn DurableSubstrate> =
            Arc::new(LocalFsSubstrate::new(dir.join("substrate")).unwrap());
        RuntimeStore::open(RuntimeStore::config(dir.join("local")), sub).unwrap()
    }

    /// The three membership events fold to a current state — the model the
    /// topology design rests on (noetl/ai-meta#332).
    #[test]
    fn register_heartbeat_deregister_fold_to_current_state() {
        let tmp = tempfile::tempdir().unwrap();
        let mut s = store(tmp.path());

        assert_eq!(
            s.register("node-a", "shard=0").unwrap(),
            1,
            "register resets the watermark to 1"
        );
        let st = s
            .get("node-a")
            .unwrap()
            .expect("registered node must be present");
        assert_eq!(
            st.contract, "shard=0",
            "the contract carries the routing payload"
        );
        assert_eq!(st.heartbeat, 1);

        assert_eq!(s.heartbeat("node-a").unwrap(), Some(2));
        assert_eq!(s.heartbeat("node-a").unwrap(), Some(3));
        assert_eq!(
            s.get("node-a").unwrap().unwrap().heartbeat,
            3,
            "the fold is latest-op-wins"
        );

        assert!(
            s.deregister("node-a").unwrap(),
            "deregistering a live node returns true"
        );
        assert_eq!(
            s.get("node-a").unwrap(),
            None,
            "a deregistered node is not live"
        );
        assert!(
            !s.deregister("node-a").unwrap(),
            "deregistering twice is false, not an error"
        );
    }

    /// ⚠ A heartbeat from a node that is not live must NOT resurrect it. That is
    /// the append-time-validation hazard in miniature: a stale or forged tick for
    /// a node that was deregistered would otherwise re-add it to routing.
    #[test]
    fn a_heartbeat_for_an_unregistered_node_does_not_resurrect_it() {
        let tmp = tempfile::tempdir().unwrap();
        let mut s = store(tmp.path());
        assert_eq!(
            s.heartbeat("ghost").unwrap(),
            None,
            "never-registered must not become live"
        );
        assert_eq!(s.get("ghost").unwrap(), None);
        assert!(
            s.list_live().unwrap().is_empty(),
            "a ghost must not appear in routing"
        );

        s.register("node-a", "shard=0").unwrap();
        s.deregister("node-a").unwrap();
        assert_eq!(
            s.heartbeat("node-a").unwrap(),
            None,
            "a deregistered node must stay gone"
        );
        assert!(s.list_live().unwrap().is_empty());
    }

    /// The liveness predicate the design uses instead of a wall clock.
    #[test]
    fn list_live_since_evicts_by_watermark_without_a_clock() {
        let tmp = tempfile::tempdir().unwrap();
        let mut s = store(tmp.path());
        s.register("node-a", "shard=0").unwrap();
        s.register("node-b", "shard=1").unwrap();
        // a beats; b goes quiet.
        for _ in 0..4 {
            s.heartbeat("node-a").unwrap();
        }
        assert_eq!(
            s.list_live().unwrap().len(),
            2,
            "both are still 'live' without a watermark"
        );

        let fresh = s.list_live_since(3).unwrap();
        assert_eq!(fresh.len(), 1, "the watermark must drop the quiet node");
        assert_eq!(fresh[0].worker_id, "node-a");
        // The quiet node is still registered — it stopped beating, it did not leave.
        assert!(
            s.get("node-b").unwrap().is_some(),
            "quiet is not the same as deregistered"
        );
    }

    /// ⭐ Survives a restart — the same premise the projection engine had to meet.
    #[test]
    fn membership_survives_a_reopen() {
        let tmp = tempfile::tempdir().unwrap();
        {
            let mut s = store(tmp.path());
            s.register("node-a", "shard=0").unwrap();
            s.register("node-b", "shard=1").unwrap();
            s.heartbeat("node-a").unwrap();
            s.deregister("node-b").unwrap();
            s.flush_and_wait().unwrap();
        }
        let s2 = store(tmp.path());
        let live = s2.list_live().unwrap();
        assert_eq!(live.len(), 1, "membership must survive a restart");
        assert_eq!(live[0].worker_id, "node-a");
        assert_eq!(live[0].heartbeat, 2);
        assert_eq!(
            s2.get("node-b").unwrap(),
            None,
            "the deregister must survive too"
        );
    }

    /// ⭐ cold_load — another instance reads this cluster's membership from
    /// durable state. This is the cross-cluster read primitive.
    #[test]
    fn cold_load_reads_membership_written_by_another_instance() {
        let tmp = tempfile::tempdir().unwrap();
        {
            let mut s = store(tmp.path());
            s.register("node-a", "shard=0").unwrap();
            s.flush_and_wait().unwrap();
        }
        let sub: Arc<dyn DurableSubstrate> =
            Arc::new(LocalFsSubstrate::new(tmp.path().join("substrate")).unwrap());
        let cold =
            RuntimeStore::cold_load(RuntimeStore::config(tmp.path().join("local2")), sub).unwrap();
        let live = cold.list_live().unwrap();
        assert_eq!(
            live.len(),
            1,
            "cold_load must see another instance's membership"
        );
        assert_eq!(live[0].contract, "shard=0");
    }

    /// ⚠⚠ WAS a characterisation of the gap; is now the GATE (noetl/ai-meta#332).
    ///
    /// D8 previously appended whatever it was handed, for any id. Harmless while
    /// every caller was in-process; the routing-poisoning path the moment gossip
    /// feeds it from the network — and because membership is **persisted**, a
    /// poisoned entry does not vanish when the attacker leaves. It is in the log,
    /// and every fold replays it. That is why the check is at APPEND.
    #[test]
    fn a_structurally_invalid_op_is_refused_at_append() {
        let tmp = tempfile::tempdir().unwrap();
        let mut s = store(tmp.path());

        let bad: &[(&str, &str)] = &[
            ("", "empty id"),
            ("  ", "whitespace-only id"),
            // ⚠ The one input the charset rule does NOT catch: dots are legal in
            // an id (`shard.3`), so a dot-run needs its own check. Without this
            // case the dot-run branch is untested and a mutation deleting it
            // passes — which is exactly what happened before this line existed.
            ("..", "bare dot-run resolves as a path segment"),
            (".", "single dot likewise"),
            ("../../etc/passwd", "path traversal into a substrate key"),
            ("a/b", "separator in a key"),
            (" node-a", "leading whitespace"),
            ("node a", "space is outside the accepted set"),
            ("nöde", "non-ascii is outside the accepted set"),
        ];
        for (id, why) in bad {
            assert!(
                s.register(id, "shard=0").is_err(),
                "register({id:?}) must be REFUSED — {why}"
            );
            // And refusing must mean it never reached the log.
            assert!(
                s.list_live().unwrap().is_empty(),
                "a refused op must not be persisted ({why})"
            );
        }

        let long = "n".repeat(MAX_WORKER_ID_LEN + 1);
        assert!(
            s.register(&long, "shard=0").is_err(),
            "an unbounded id is an unbounded key"
        );
        assert!(
            s.register("node-a", "c".repeat(MAX_CONTRACT_LEN + 1))
                .is_err(),
            "an unbounded contract lets one gossip op cost arbitrary bytes"
        );
        assert!(
            s.list_live().unwrap().is_empty(),
            "nothing invalid was persisted"
        );
    }

    /// ⚠ NEGATIVE CONTROL — validation that rejects everything would pass the
    /// test above. These must still work.
    #[test]
    fn valid_ops_still_append_and_still_fold() {
        let tmp = tempfile::tempdir().unwrap();
        let mut s = store(tmp.path());

        for id in ["node-a", "noetl-server-rust-0", "shard.3", "a:b", "A_1-2.3"] {
            assert!(
                s.register(id, "shard=0").is_ok(),
                "{id} is a legal identity"
            );
        }
        assert_eq!(
            s.list_live().unwrap().len(),
            5,
            "every valid register must persist"
        );

        // ...and the full lifecycle still folds correctly through validation.
        assert_eq!(s.heartbeat("node-a").unwrap(), Some(2));
        assert_eq!(s.heartbeat("node-a").unwrap(), Some(3));
        assert_eq!(s.get("node-a").unwrap().unwrap().heartbeat, 3);
        assert!(s.deregister("node-a").unwrap());
        assert_eq!(s.get("node-a").unwrap(), None);
        assert_eq!(s.list_live().unwrap().len(), 4);
    }

    /// A malformed op cannot be smuggled past the store's API either: the
    /// watermark/event agreement is checked on the record itself.
    #[test]
    fn field_and_event_must_agree() {
        let ok = RuntimeOp {
            op_seq: 1,
            worker_id: "node-a".into(),
            event: RuntimeEvent::Register,
            heartbeat: 1,
            contract: "shard=0".into(),
        };
        assert!(validate_op(&ok).is_ok());

        // A live event with a zero watermark would make list_live_since(1)
        // silently drop a node that is beating.
        for ev in [RuntimeEvent::Register, RuntimeEvent::Heartbeat] {
            let bad = RuntimeOp {
                event: ev,
                heartbeat: 0,
                ..ok.clone()
            };
            assert!(
                validate_op(&bad).is_err(),
                "{ev:?} with heartbeat 0 must be refused"
            );
        }
        // Deregister legitimately carries no watermark.
        let dereg = RuntimeOp {
            event: RuntimeEvent::Deregister,
            heartbeat: 0,
            ..ok.clone()
        };
        assert!(
            validate_op(&dereg).is_ok(),
            "deregister may carry heartbeat 0"
        );
    }

    /// ⚠⚠ The seam must be WIRED, not merely present.
    ///
    /// The first version of this test called `RefuseAll.authorize(..)` directly
    /// and passed — while a mutation deleting `self.origin.authorize(&op)?` from
    /// the append path ALSO passed. A seam nothing calls is the "exists but does
    /// not fire" shape this codebase keeps producing. So this drives it through
    /// the store.
    #[test]
    fn the_origin_seam_is_called_on_the_append_path() {
        struct RefuseAll;
        impl OpOrigin for RefuseAll {
            fn authorize(&self, _op: &RuntimeOp) -> Result<()> {
                Err(EhdbError::InvalidState("not authorised".into()))
            }
        }
        let tmp = tempfile::tempdir().unwrap();
        let mut s = store(tmp.path()).with_origin(Box::new(RefuseAll));

        // Structurally valid — so only the origin can refuse it.
        let op = RuntimeOp {
            op_seq: 1,
            worker_id: "node-a".into(),
            event: RuntimeEvent::Register,
            heartbeat: 1,
            contract: "shard=0".into(),
        };
        assert!(
            validate_op(&op).is_ok(),
            "the op must be structurally fine, or this proves nothing"
        );

        assert!(
            s.register("node-a", "shard=0").is_err(),
            "the store must consult the origin on append — this is where the \
             signed-gossip check will refuse an unauthorised peer"
        );
        assert!(
            s.list_live().unwrap().is_empty(),
            "an unauthorised op must not be persisted; membership is replayed \
             forever, so refusing at read would be too late"
        );

        // Negative control: the default origin accepts the same op.
        let mut ok = store(tmp.path()).with_origin(Box::new(TrustedLocalOrigin));
        assert!(ok.register("node-a", "shard=0").is_ok());
    }

    /// The audit property the design claims is "free": the log is the history.
    #[test]
    fn the_log_retains_history_the_current_state_has_forgotten() {
        let tmp = tempfile::tempdir().unwrap();
        let mut s = store(tmp.path());
        s.register("node-a", "shard=0").unwrap();
        s.heartbeat("node-a").unwrap();
        s.deregister("node-a").unwrap();
        assert_eq!(
            s.get("node-a").unwrap(),
            None,
            "current state has forgotten it"
        );

        // But the ops are still there — this is what a heartbeat COLUMN cannot do.
        let ops = s.engine().read_index_after("node-a", 0).unwrap();
        assert_eq!(
            ops.len(),
            3,
            "register + heartbeat + deregister must all remain"
        );
        assert_eq!(ops[0].event, RuntimeEvent::Register);
        assert_eq!(ops[2].event, RuntimeEvent::Deregister);
    }
}
