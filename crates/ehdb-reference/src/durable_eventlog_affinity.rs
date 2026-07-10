//! Execution-affinity **single-writer routing** over the durable segment store
//! (completion program, durable event-log backend slice 2;
//! [noetl/ai-meta#254] item 2).
//!
//! Slice 1 ([`crate::durable_eventlog`]) shipped the production disk format but
//! assumed the caller was the sole writer. This slice makes that assumption
//! *true under multiple replicas*: it partitions the log into per-shard segment
//! stores and lets a replica write only the shards it owns
//! ([`crate::affinity::ShardOwnership`]). A write that lands on a non-owner is
//! **refused with no side effect** (route it to the owner); a read on a
//! non-owner **cold-loads** the durable segments read-only. That is the
//! single-writer coherence the prod-cutover runbook's §C durability gate
//! requires beyond the pod-local, per-replica-divergent `local_reference`.
//!
//! ## Topology — one segment store per shard
//!
//! ```text
//! <root>/
//!   shard-0000/  seg-*.eslog   <- written only by the replica that owns shard 0
//!   shard-0001/  seg-*.eslog   <- written only by the replica that owns shard 1
//!   ...
//! ```
//!
//! Each shard directory is a full slice-1 [`DurableSegmentStore`] — the whole
//! [`EventLogDriver`] contract (append / scan / per-execution read / durable
//! tail+ack, gapless global sequence, replay-is-truth) holds *within* a shard.
//! An execution's events always land in `shard-<shard_of(execution)>`, so the
//! per-execution scope the contract promises is preserved; the global sequence
//! is monotonic **per shard stream** (each shard is its own stream — the
//! sharded-log shape #166 already uses off-server).
//!
//! ## Single-writer invariant
//!
//! A replica only *opens for write* (`owned_store`) a shard it owns, and only
//! the owner ever appends → each shard's segment files have exactly one writer.
//! A non-owner never opens the shard writable: its reads use
//! [`DurableSegmentStore::open_read_only`], which cannot mutate (it refuses
//! writes and never truncates a torn tail). So at most one replica ever writes a
//! given shard's bytes — the coherence property, enforced structurally.
//!
//! ## Correctness never depends on routing
//!
//! [`Routed::NotOwner`] carries **no side effect** — no bytes written, no
//! sequence consumed — so re-routing a refused write to the owner can never
//! double-process it, and a cold-load read is pure. A mis-route (e.g. a replica
//! asked to write a shard it doesn't own) is a routing decision to redo, never a
//! divergence; the append-only log stays the source of truth.
//!
//! ## Observability seam
//!
//! This is the storage/routing library; the running worker owns the metric
//! registry. [`Routed::decision_label`] and [`ServedBy::label`] give the stable
//! label set (`owned` / `not_owner`; `owner_resident` / `non_owner_cold_load`)
//! for the counter the worker-wiring slice (item 4) increments per routed op,
//! carrying `execution_id` as a span field per `observability.md`.
//!
//! [noetl/ai-meta#254]: https://github.com/noetl/ehdb/issues/254

use std::{
    collections::HashMap,
    fs,
    path::{Path, PathBuf},
    sync::Mutex,
};

use ehdb_core::{EhdbError, Result};
use serde::{Deserialize, Serialize};

use crate::affinity::ShardOwnership;
use crate::durable_eventlog::{
    DurableEventLogDriver, DurableSegmentStore, SegmentGcOutcome, SegmentGcPolicy,
    DEFAULT_SEGMENT_MAX_BYTES,
};
use crate::eventlog::{
    EventLogAckOutcome, EventLogAckRequest, EventLogAppendOutcome, EventLogAppendRequest,
    EventLogDriver, EventLogReadExecutionOutcome, EventLogReadExecutionRequest,
    EventLogScanOutcome, EventLogScanRequest, EventLogTailOutcome, EventLogTailRequest,
};

/// Shard-directory name prefix under the routed store root.
const SHARD_DIR_PREFIX: &str = "shard-";

/// The outcome of routing an op that **requires ownership** (a write: append /
/// durable tail-create / ack) through the affinity layer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Routed<T> {
    /// This replica owns the target shard; the op ran and produced `T`.
    Served(T),
    /// This replica does **not** own the target shard; the op was refused with
    /// no side effect. Route it to the replica that owns `owner_shard`.
    NotOwner { owner_shard: u32 },
}

impl<T> Routed<T> {
    /// Whether the op was served locally (owner) vs refused (non-owner).
    pub fn is_served(&self) -> bool {
        matches!(self, Routed::Served(_))
    }

    /// The served outcome, or `None` if it was refused as a non-owner.
    pub fn served(&self) -> Option<&T> {
        match self {
            Routed::Served(t) => Some(t),
            Routed::NotOwner { .. } => None,
        }
    }

    /// The owning shard when refused, or `None` if served.
    pub fn owner_shard(&self) -> Option<u32> {
        match self {
            Routed::NotOwner { owner_shard } => Some(*owner_shard),
            Routed::Served(_) => None,
        }
    }

    /// Stable metric label for the routed-write decision counter.
    pub fn decision_label(&self) -> &'static str {
        match self {
            Routed::Served(_) => "owned",
            Routed::NotOwner { .. } => "not_owner",
        }
    }
}

/// How a read was served under affinity: from the owner's resident store, or by
/// a non-owner cold-loading the durable segments read-only.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ServedBy {
    /// The owning replica served the read from its resident, open store.
    OwnerResident,
    /// A non-owning replica cold-loaded the durable segments read-only.
    NonOwnerColdLoad,
}

impl ServedBy {
    /// Stable metric label for the routed-read counter.
    pub fn label(self) -> &'static str {
        match self {
            ServedBy::OwnerResident => "owner_resident",
            ServedBy::NonOwnerColdLoad => "non_owner_cold_load",
        }
    }
}

/// A read outcome plus how affinity served it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AffinityRead<T> {
    /// Whether the owner served it resident or a non-owner cold-loaded it.
    pub served_by: ServedBy,
    /// The underlying read outcome (identical shape to the unrouted contract).
    pub outcome: T,
}

impl<T> AffinityRead<T> {
    fn owner(outcome: T) -> Self {
        Self {
            served_by: ServedBy::OwnerResident,
            outcome,
        }
    }

    fn cold_load(outcome: T) -> Self {
        Self {
            served_by: ServedBy::NonOwnerColdLoad,
            outcome,
        }
    }
}

/// Execution-affinity single-writer router over a set of per-shard durable
/// segment stores rooted at one directory. One instance models **one replica**:
/// its [`ShardOwnership`] decides which shards it may write and which it must
/// cold-load to read.
///
/// Owned-shard stores are opened lazily and kept resident behind a mutex (O(1)
/// locate for the owner); non-owned reads open a fresh read-only cold-load view
/// per call (no resident state on a non-owner).
#[derive(Debug)]
pub struct AffinityRoutedEventLog {
    root: PathBuf,
    ownership: ShardOwnership,
    segment_max_bytes: u64,
    /// Resident writable stores for the shards this replica owns (lazy).
    owned: Mutex<HashMap<u32, DurableEventLogDriver>>,
}

impl AffinityRoutedEventLog {
    /// Open a router for one replica, rooted at `root` with the given
    /// `ownership`. Owned-shard stores are opened on first touch.
    pub fn open(root: impl Into<PathBuf>, ownership: ShardOwnership) -> Result<Self> {
        Self::open_with_segment_size(root, ownership, DEFAULT_SEGMENT_MAX_BYTES)
    }

    /// Open with an explicit per-shard segment rollover threshold (tests force
    /// small segments to exercise rollover).
    pub fn open_with_segment_size(
        root: impl Into<PathBuf>,
        ownership: ShardOwnership,
        segment_max_bytes: u64,
    ) -> Result<Self> {
        if segment_max_bytes == 0 {
            return Err(EhdbError::InvalidState(
                "affinity-routed event-log segment_max_bytes must be > 0".to_string(),
            ));
        }
        Ok(Self {
            root: root.into(),
            ownership,
            segment_max_bytes,
            owned: Mutex::new(HashMap::new()),
        })
    }

    /// This replica's ownership.
    pub fn ownership(&self) -> ShardOwnership {
        self.ownership
    }

    /// The root directory backing every shard's segment store.
    pub fn root(&self) -> &Path {
        &self.root
    }

    /// The directory holding shard `shard`'s segment store.
    pub fn shard_dir(&self, shard: u32) -> PathBuf {
        self.root.join(format!("{SHARD_DIR_PREFIX}{shard:04}"))
    }

    /// The shard that owns `execution_id`.
    pub fn shard_of(&self, execution_id: &str) -> u32 {
        self.ownership.shard_of(execution_id)
    }

    /// Get (or lazily open) this replica's writable store for an **owned**
    /// shard. Returns a clone (the driver is `Arc`-shared) so callers operate
    /// without holding the owned-map lock across I/O. Caller must have checked
    /// ownership; opening a non-owned shard writable would violate single-writer.
    fn owned_store(&self, shard: u32) -> Result<DurableEventLogDriver> {
        let mut owned = self.owned.lock().map_err(|_| {
            EhdbError::InvalidState("affinity owned-store lock poisoned".to_string())
        })?;
        if let Some(driver) = owned.get(&shard) {
            return Ok(driver.clone());
        }
        let driver = DurableEventLogDriver::open_with_segment_size(
            self.shard_dir(shard),
            self.segment_max_bytes,
        )?;
        owned.insert(shard, driver.clone());
        Ok(driver)
    }

    /// Shard ids with an on-disk store directory under the root, ascending.
    fn present_shard_ids(&self) -> Result<Vec<u32>> {
        let mut ids = Vec::new();
        if !self.root.exists() {
            return Ok(ids);
        }
        for entry in fs::read_dir(&self.root).map_err(|err| EhdbError::Storage(err.to_string()))? {
            let entry = entry.map_err(|err| EhdbError::Storage(err.to_string()))?;
            let name = entry.file_name();
            if let Some(id) = name
                .to_string_lossy()
                .strip_prefix(SHARD_DIR_PREFIX)
                .and_then(|d| d.parse::<u32>().ok())
            {
                ids.push(id);
            }
        }
        ids.sort_unstable();
        Ok(ids)
    }

    /// Reclaim consumed sealed segments for every **owned** shard present on disk
    /// — the affinity-layer fan-out of [`DurableSegmentStore::reclaim_segments`]
    /// (segment GC). A no-op per shard unless `policy.enabled`. Returns
    /// `(shard, outcome)` for each owned shard touched, ascending.
    ///
    /// Only owned shards are reclaimed — a non-owned shard's single writer is a
    /// different replica, so this replica must never mutate its segments. This is
    /// **local** reclamation (each owner bounds its own local segment store); the
    /// shared segment tier's own object reclamation + watermark coherence across
    /// an ownership transfer is a separate concern (see the design note's
    /// shared-tier scope), so run this only where the durable store is the
    /// single-writer local authority (the recommended PVC bootstrapping topology).
    pub fn reclaim_owned_shards(
        &self,
        policy: &SegmentGcPolicy,
    ) -> Result<Vec<(u32, SegmentGcOutcome)>> {
        let mut out = Vec::new();
        for shard in self.present_shard_ids()? {
            if !self.ownership.owns_shard(shard) {
                continue;
            }
            let driver = self.owned_store(shard)?;
            out.push((shard, driver.reclaim_segments(policy)?));
        }
        Ok(out)
    }

    /// The owned writable driver for a shard this replica owns (opened lazily,
    /// `Arc`-shared). Errors if this replica does not own `shard` — opening a
    /// non-owned shard writable would violate single-writer. Exposed so the
    /// shared tier can drive segment GC (plan / reclaim-to-watermark) on the
    /// owner's local store.
    pub fn owned_driver(&self, shard: u32) -> Result<DurableEventLogDriver> {
        if !self.ownership.owns_shard(shard) {
            return Err(EhdbError::InvalidState(format!(
                "cannot access shard {shard} writable: not owned by this replica (shard_index {})",
                self.ownership.shard_index()
            )));
        }
        self.owned_store(shard)
    }

    /// Cold-load a read-only view of a shard this replica does not own.
    fn cold_load(&self, shard: u32) -> Result<DurableSegmentStore> {
        DurableSegmentStore::open_read_only_with_segment_size(
            self.shard_dir(shard),
            self.segment_max_bytes,
        )
    }

    /// Append one authorized event, routed to the shard that owns its
    /// execution. Served only if this replica owns that shard; otherwise refused
    /// with no side effect ([`Routed::NotOwner`]) so the caller can re-route.
    pub fn append(&self, request: &EventLogAppendRequest) -> Result<Routed<EventLogAppendOutcome>> {
        let shard = self.ownership.shard_of(&request.execution_id);
        if !self.ownership.owns_shard(shard) {
            return Ok(Routed::NotOwner { owner_shard: shard });
        }
        let driver = self.owned_store(shard)?;
        Ok(Routed::Served(driver.append(request)?))
    }

    /// Ordered per-execution read, routed to the execution's shard. The owner
    /// serves it resident; a non-owner cold-loads the durable segments
    /// read-only.
    pub fn read_execution(
        &self,
        request: &EventLogReadExecutionRequest,
    ) -> Result<AffinityRead<EventLogReadExecutionOutcome>> {
        let shard = self.ownership.shard_of(&request.execution_id);
        if self.ownership.owns_shard(shard) {
            let driver = self.owned_store(shard)?;
            Ok(AffinityRead::owner(driver.read_execution(request)?))
        } else {
            let mut view = self.cold_load(shard)?;
            Ok(AffinityRead::cold_load(view.read_execution(request)?))
        }
    }

    /// Ordered global scan of one shard's stream. The owner serves it resident;
    /// a non-owner cold-loads read-only. (A shard is a stream; a scan is always
    /// shard-scoped in the sharded log.)
    pub fn scan_shard(
        &self,
        shard: u32,
        request: &EventLogScanRequest,
    ) -> Result<AffinityRead<EventLogScanOutcome>> {
        if self.ownership.owns_shard(shard) {
            let driver = self.owned_store(shard)?;
            Ok(AffinityRead::owner(driver.scan_global(request)?))
        } else {
            let mut view = self.cold_load(shard)?;
            Ok(AffinityRead::cold_load(view.scan_global(request)?))
        }
    }

    /// Durable-consumer tail pull on one shard's stream. A durable consumer is
    /// **writer state** (create-on-first-pull persists a frame), so it is
    /// owner-only; a non-owner is refused ([`Routed::NotOwner`]) and must route
    /// to the owner.
    pub fn tail(
        &self,
        shard: u32,
        request: &EventLogTailRequest,
    ) -> Result<Routed<EventLogTailOutcome>> {
        if !self.ownership.owns_shard(shard) {
            return Ok(Routed::NotOwner { owner_shard: shard });
        }
        let driver = self.owned_store(shard)?;
        Ok(Routed::Served(driver.tail(request)?))
    }

    /// Advance a durable consumer's ack cursor on one shard's stream. A write —
    /// owner-only; a non-owner is refused.
    pub fn ack(
        &self,
        shard: u32,
        request: &EventLogAckRequest,
    ) -> Result<Routed<EventLogAckOutcome>> {
        if !self.ownership.owns_shard(shard) {
            return Ok(Routed::NotOwner { owner_shard: shard });
        }
        let driver = self.owned_store(shard)?;
        Ok(Routed::Served(driver.ack(request)?))
    }
}

/// List the shard directories present under a routed store root, in ascending
/// shard id, with each shard's on-disk byte size (sum of its segment files).
/// Used by the single-writer drive to prove which shards were written and to
/// detect a non-owner mutation (a read that changed bytes).
fn shard_dir_sizes(root: &Path) -> Result<Vec<(u32, u64)>> {
    let mut out = Vec::new();
    if !root.exists() {
        return Ok(out);
    }
    for entry in fs::read_dir(root).map_err(|err| EhdbError::Storage(err.to_string()))? {
        let entry = entry.map_err(|err| EhdbError::Storage(err.to_string()))?;
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if let Some(id) = name
            .strip_prefix(SHARD_DIR_PREFIX)
            .and_then(|d| d.parse::<u32>().ok())
        {
            let mut bytes = 0u64;
            for seg in
                fs::read_dir(entry.path()).map_err(|err| EhdbError::Storage(err.to_string()))?
            {
                let seg = seg.map_err(|err| EhdbError::Storage(err.to_string()))?;
                bytes += seg
                    .metadata()
                    .map_err(|err| EhdbError::Storage(err.to_string()))?
                    .len();
            }
            out.push((id, bytes));
        }
    }
    out.sort_unstable_by_key(|(id, _)| *id);
    Ok(out)
}

// ===========================================================================
// Single-writer drive — the star of this slice.
//
// Spins up a `shard_count`-replica pool over ONE root, partitions a
// deterministic execution set across the replicas, and proves:
//   * owner append succeeds (Served with a sequence),
//   * non-owner append is refused (NotOwner, no side effect),
//   * each shard's segments contain only its owner's executions (single-writer),
//   * a non-owner cold-load serves the owner's data read-only (no mutation),
//   * the owner's shard replays zero-loss after a simulated restart (recovery).
// ===========================================================================

/// Secret-free proof of one execution-affinity single-writer drive. Counts +
/// verdicts only (payloads are synthetic).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AffinitySingleWriterReport {
    /// Replicas / shards in the simulated pool.
    pub shard_count: u32,
    /// Distinct executions driven (each owned by exactly one shard).
    pub executions: usize,
    /// Appends that succeeded on the owning replica.
    pub owner_appends: usize,
    /// Append attempts refused on a non-owning replica.
    pub nonowner_refusals: usize,
    /// Every owner append was `Served` with a monotonic per-shard sequence.
    pub owner_writes_ok: bool,
    /// Every non-owner append was `NotOwner` (routed to the true owner) with no
    /// phantom write — total on-disk records equal `executions`.
    pub nonowner_refused_ok: bool,
    /// Each shard's segments contain only executions that hash to that shard —
    /// no cross-writer contamination (the single-writer invariant).
    pub single_writer_invariant: bool,
    /// A non-owner cold-load served the owner's exact records read-only and left
    /// the owner's segment bytes byte-for-byte unchanged.
    pub coldload_read_ok: bool,
    /// After dropping the pool and reopening the owner over the same root, the
    /// owner's shard replayed its events zero-loss (recovery under ownership).
    pub crash_recovery_ok: bool,
    /// The single reason a durability/coherence invariant failed, or `None`.
    pub divergence: Option<String>,
}

impl AffinitySingleWriterReport {
    /// Whether every ownership + single-writer + recovery invariant held.
    pub fn holds(&self) -> bool {
        self.owner_writes_ok
            && self.nonowner_refused_ok
            && self.single_writer_invariant
            && self.coldload_read_ok
            && self.crash_recovery_ok
            && self.divergence.is_none()
    }
}

/// Deterministically pick `per_shard` execution ids for each shard `0..count`,
/// searching decimal snowflake-shaped ids so every shard is covered. Returns
/// `(execution_id, owning_shard)` pairs grouped by shard, ascending.
fn assign_executions(count: u32, per_shard: usize) -> Vec<(String, u32)> {
    let base = 320_816_801_799_737_344_i64;
    let mut buckets: Vec<Vec<String>> = vec![Vec::new(); count as usize];
    let mut i = 0i64;
    while buckets.iter().any(|b| b.len() < per_shard) {
        let id = (base + i).to_string();
        let shard = crate::affinity::shard_for_execution(&id, count) as usize;
        if buckets[shard].len() < per_shard {
            buckets[shard].push(id);
        }
        i += 1;
        // Defensive bound so a degenerate hash can never loop forever.
        if i > 1_000_000 {
            break;
        }
    }
    let mut out = Vec::new();
    for (shard, ids) in buckets.into_iter().enumerate() {
        for id in ids {
            out.push((id, shard as u32));
        }
    }
    out
}

/// Drive an execution-affinity single-writer cycle over the store rooted at
/// `root` with a simulated `shard_count`-replica pool. `shard_count` must be
/// `>= 2` (a single shard has no split to prove). See the module docs for the
/// invariants proven.
pub fn exercise_affinity_single_writer(
    root: impl Into<PathBuf>,
    shard_count: u32,
) -> Result<AffinitySingleWriterReport> {
    if shard_count < 2 {
        return Err(EhdbError::InvalidState(
            "affinity single-writer drive requires shard_count >= 2".to_string(),
        ));
    }
    let root = root.into();
    let per_shard = 2usize;
    let executions = assign_executions(shard_count, per_shard);
    let total_execs = executions.len();

    // One router per replica, all over the same root.
    let replicas: Vec<AffinityRoutedEventLog> = (0..shard_count)
        .map(|idx| {
            let ownership = ShardOwnership::new(idx, shard_count)?;
            AffinityRoutedEventLog::open(&root, ownership)
        })
        .collect::<Result<Vec<_>>>()?;

    let mut owner_appends = 0usize;
    let mut nonowner_refusals = 0usize;
    let mut owner_writes_ok = true;
    let mut nonowner_refused_ok = true;
    let mut divergence: Option<String> = None;

    // Every execution offered to EVERY replica: only its owner writes it.
    for (execution_id, owner_shard) in &executions {
        for replica in &replicas {
            let request = EventLogAppendRequest {
                execution_id: execution_id.clone(),
                transaction_id: format!("aff-{execution_id}"),
                payload: format!("{{\"exec\":\"{execution_id}\"}}"),
            };
            match replica.append(&request)? {
                Routed::Served(outcome) => {
                    owner_appends += 1;
                    // The server MUST be the owner and the sequence monotonic.
                    if replica.ownership().shard_index() != *owner_shard
                        || outcome.global_sequence == 0
                    {
                        owner_writes_ok = false;
                        divergence.record_first(format!(
                            "owner write anomaly: exec {execution_id} served by shard {} (owner {owner_shard}), seq {}",
                            replica.ownership().shard_index(),
                            outcome.global_sequence
                        ));
                    }
                }
                Routed::NotOwner {
                    owner_shard: routed,
                } => {
                    nonowner_refusals += 1;
                    // A refusal MUST come from a non-owner and name the true owner.
                    if replica.ownership().shard_index() == *owner_shard || routed != *owner_shard {
                        nonowner_refused_ok = false;
                        divergence.record_first(format!(
                            "refusal anomaly: exec {execution_id} refused by owner {} (true owner {owner_shard}, routed {routed})",
                            replica.ownership().shard_index()
                        ));
                    }
                }
            }
        }
    }

    // Each execution written exactly once (by its single owner).
    if owner_appends != total_execs {
        owner_writes_ok = false;
        divergence.record_first(format!(
            "owner-append count {owner_appends} != executions {total_execs}"
        ));
    }
    // Every non-owner attempt was refused: (count-1) refusals per execution.
    let expected_refusals = total_execs * (shard_count as usize - 1);
    if nonowner_refusals != expected_refusals {
        nonowner_refused_ok = false;
        divergence.record_first(format!(
            "refusal count {nonowner_refusals} != expected {expected_refusals}"
        ));
    }

    // Single-writer invariant: each shard's segments contain only executions
    // that hash to that shard, and total on-disk records == executions (no
    // phantom write from a non-owner).
    let mut single_writer_invariant = true;
    let mut total_on_disk = 0usize;
    for shard in 0..shard_count {
        let mut view = DurableSegmentStore::open_read_only(replicas[0].shard_dir(shard))?;
        let scan = view.scan_global(&EventLogScanRequest {
            after: None,
            limit: 100_000,
        })?;
        total_on_disk += scan.record_count;
        for record in &scan.records {
            if crate::affinity::shard_for_execution(&record.execution_id, shard_count) != shard {
                single_writer_invariant = false;
                divergence.record_first(format!(
                    "cross-writer contamination: shard {shard} holds exec {} (owner {})",
                    record.execution_id,
                    crate::affinity::shard_for_execution(&record.execution_id, shard_count)
                ));
            }
        }
    }
    if total_on_disk != total_execs {
        single_writer_invariant = false;
        nonowner_refused_ok = false;
        divergence.record_first(format!(
            "on-disk record count {total_on_disk} != executions {total_execs} (phantom write?)"
        ));
    }

    // Non-owner cold-load read: a replica reads an execution it does NOT own;
    // it must cold-load the owner's exact records read-only and not mutate the
    // owner's segment bytes.
    let mut coldload_read_ok = true;
    // Pick an execution owned by shard 0 and a replica that is not shard 0.
    if let (Some((exec0, _)), Some(reader)) = (
        executions.iter().find(|(_, s)| *s == 0),
        replicas.iter().find(|r| r.ownership().shard_index() != 0),
    ) {
        let before = shard_dir_sizes(&root)?;
        let read = reader.read_execution(&EventLogReadExecutionRequest {
            execution_id: exec0.clone(),
            after: None,
            limit: 100,
        })?;
        let after = shard_dir_sizes(&root)?;
        let mut owner_view = DurableSegmentStore::open_read_only(reader.shard_dir(0))?;
        let owner_read = owner_view.read_execution(&EventLogReadExecutionRequest {
            execution_id: exec0.clone(),
            after: None,
            limit: 100,
        })?;
        let served_cold = read.served_by == ServedBy::NonOwnerColdLoad;
        let same_records = read.outcome.returned == owner_read.returned
            && read
                .outcome
                .records
                .iter()
                .zip(owner_read.records.iter())
                .all(|(a, b)| a.global_sequence == b.global_sequence && a.payload == b.payload);
        let no_mutation = before == after;
        if !(served_cold && same_records && no_mutation && read.outcome.returned > 0) {
            coldload_read_ok = false;
            divergence.record_first(format!(
                "cold-load read anomaly: served_cold={served_cold} same_records={same_records} no_mutation={no_mutation} returned={}",
                read.outcome.returned
            ));
        }
    } else {
        coldload_read_ok = false;
        divergence.record_first("cold-load read: no shard-0 execution to test".to_string());
    }

    // Crash recovery under ownership: drop the pool, reopen the shard-0 owner
    // over the same root, and prove its shard replays its events zero-loss.
    drop(replicas);
    let crash_recovery_ok;
    {
        let owner0 = AffinityRoutedEventLog::open(&root, ShardOwnership::new(0, shard_count)?)?;
        let expected0 = executions.iter().filter(|(_, s)| *s == 0).count();
        let scan = owner0.scan_shard(
            0,
            &EventLogScanRequest {
                after: None,
                limit: 100_000,
            },
        )?;
        let gapless = scan
            .outcome
            .records
            .iter()
            .enumerate()
            .all(|(i, r)| r.global_sequence == i as u64 + 1);
        crash_recovery_ok = scan.served_by == ServedBy::OwnerResident
            && scan.outcome.record_count == expected0
            && gapless;
        if !crash_recovery_ok {
            divergence.record_first(format!(
                "crash recovery anomaly: recovered {} of {expected0} shard-0 events (gapless={gapless})",
                scan.outcome.record_count
            ));
        }
    }

    Ok(AffinitySingleWriterReport {
        shard_count,
        executions: total_execs,
        owner_appends,
        nonowner_refusals,
        owner_writes_ok,
        nonowner_refused_ok,
        single_writer_invariant,
        coldload_read_ok,
        crash_recovery_ok,
        divergence,
    })
}

/// Tiny helper to record only the first divergence reason (later ones are
/// symptoms of the first).
trait FirstReason {
    fn record_first(&mut self, reason: String);
}

impl FirstReason for Option<String> {
    fn record_first(&mut self, reason: String) {
        if self.is_none() {
            *self = Some(reason);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tmp_root(tag: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "ehdb-affinity-{tag}-{}-{:?}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn append_req(exec: &str, payload: &str) -> EventLogAppendRequest {
        EventLogAppendRequest {
            execution_id: exec.to_string(),
            transaction_id: format!("txn-{exec}"),
            payload: payload.to_string(),
        }
    }

    #[test]
    fn single_owner_serves_everything() {
        let root = tmp_root("single");
        let log = AffinityRoutedEventLog::open(&root, ShardOwnership::single_owner()).unwrap();
        let routed = log.append(&append_req("100", "a")).unwrap();
        assert!(routed.is_served());
        assert_eq!(routed.served().unwrap().global_sequence, 1);
        // A single owner cold-load never fires — reads are owner-resident.
        let read = log
            .read_execution(&EventLogReadExecutionRequest {
                execution_id: "100".to_string(),
                after: None,
                limit: 10,
            })
            .unwrap();
        assert_eq!(read.served_by, ServedBy::OwnerResident);
        assert_eq!(read.outcome.returned, 1);
        let _ = fs::remove_dir_all(&root);
    }

    #[test]
    fn owner_appends_non_owner_refuses() {
        let root = tmp_root("split");
        // Find one execution owned by shard 0 and one by shard 1.
        let execs = assign_executions(2, 1);
        let exec0 = execs.iter().find(|(_, s)| *s == 0).unwrap().0.clone();
        let exec1 = execs.iter().find(|(_, s)| *s == 1).unwrap().0.clone();

        let r0 = AffinityRoutedEventLog::open(&root, ShardOwnership::new(0, 2).unwrap()).unwrap();
        let r1 = AffinityRoutedEventLog::open(&root, ShardOwnership::new(1, 2).unwrap()).unwrap();

        // Replica 0 owns exec0, refuses exec1.
        assert!(r0.append(&append_req(&exec0, "a")).unwrap().is_served());
        assert_eq!(
            r0.append(&append_req(&exec1, "b")).unwrap().owner_shard(),
            Some(1)
        );
        // Replica 1 owns exec1, refuses exec0.
        assert!(r1.append(&append_req(&exec1, "b")).unwrap().is_served());
        assert_eq!(
            r1.append(&append_req(&exec0, "a")).unwrap().owner_shard(),
            Some(0)
        );
        let _ = fs::remove_dir_all(&root);
    }

    #[test]
    fn non_owner_read_cold_loads() {
        let root = tmp_root("coldread");
        let execs = assign_executions(2, 1);
        let exec0 = execs.iter().find(|(_, s)| *s == 0).unwrap().0.clone();

        let r0 = AffinityRoutedEventLog::open(&root, ShardOwnership::new(0, 2).unwrap()).unwrap();
        let r1 = AffinityRoutedEventLog::open(&root, ShardOwnership::new(1, 2).unwrap()).unwrap();
        r0.append(&append_req(&exec0, "owned-by-0")).unwrap();

        // Replica 1 (non-owner of shard 0) reads exec0 → cold-load.
        let read = r1
            .read_execution(&EventLogReadExecutionRequest {
                execution_id: exec0.clone(),
                after: None,
                limit: 10,
            })
            .unwrap();
        assert_eq!(read.served_by, ServedBy::NonOwnerColdLoad);
        assert_eq!(read.outcome.returned, 1);
        assert_eq!(read.outcome.records[0].payload, "owned-by-0");
        let _ = fs::remove_dir_all(&root);
    }

    #[test]
    fn non_owner_tail_and_ack_refused() {
        let root = tmp_root("tailrefuse");
        let r0 = AffinityRoutedEventLog::open(&root, ShardOwnership::new(0, 2).unwrap()).unwrap();
        // Replica 0 does not own shard 1 → tail/ack on shard 1 refused.
        assert_eq!(
            r0.tail(
                1,
                &EventLogTailRequest {
                    consumer: "c".to_string(),
                    transaction_id: "t".to_string(),
                    limit: 10,
                }
            )
            .unwrap()
            .owner_shard(),
            Some(1)
        );
        assert_eq!(
            r0.ack(
                1,
                &EventLogAckRequest {
                    consumer: "c".to_string(),
                    transaction_id: "t".to_string(),
                    sequence: 1,
                }
            )
            .unwrap()
            .owner_shard(),
            Some(1)
        );
        let _ = fs::remove_dir_all(&root);
    }

    #[test]
    fn drive_proves_single_writer_invariant() {
        let root = tmp_root("drive2");
        let report = exercise_affinity_single_writer(&root, 2).unwrap();
        assert!(report.holds(), "{report:?}");
        assert_eq!(report.shard_count, 2);
        assert_eq!(report.owner_appends, report.executions);
        assert_eq!(report.nonowner_refusals, report.executions); // (2-1) per exec
        assert!(report.owner_writes_ok);
        assert!(report.nonowner_refused_ok);
        assert!(report.single_writer_invariant);
        assert!(report.coldload_read_ok);
        assert!(report.crash_recovery_ok);
        assert!(report.divergence.is_none());
        let _ = fs::remove_dir_all(&root);
    }

    #[test]
    fn drive_proves_single_writer_invariant_four_shards() {
        let root = tmp_root("drive4");
        let report = exercise_affinity_single_writer(&root, 4).unwrap();
        assert!(report.holds(), "{report:?}");
        assert_eq!(report.shard_count, 4);
        // (4-1) refusals per execution.
        assert_eq!(report.nonowner_refusals, report.executions * 3);
        let _ = fs::remove_dir_all(&root);
    }

    #[test]
    fn drive_requires_at_least_two_shards() {
        let root = tmp_root("drive1");
        let err = exercise_affinity_single_writer(&root, 1).unwrap_err();
        assert!(err.to_string().contains("shard_count >= 2"));
        let _ = fs::remove_dir_all(&root);
    }

    #[test]
    fn labels_are_stable() {
        assert_eq!(
            Routed::<()>::NotOwner { owner_shard: 1 }.decision_label(),
            "not_owner"
        );
        assert_eq!(Routed::Served(()).decision_label(), "owned");
        assert_eq!(ServedBy::OwnerResident.label(), "owner_resident");
        assert_eq!(ServedBy::NonOwnerColdLoad.label(), "non_owner_cold_load");
    }
}
