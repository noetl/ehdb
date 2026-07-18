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

use ehdb_core::Result;
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

/// **The D8 runtime store** — register / heartbeat / deregister / list-live over
/// the generic engine.
pub struct RuntimeStore {
    engine: L0Engine<RuntimeDataset>,
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
        })
    }
    /// Open an N-way replicated runtime store.
    pub fn open_replicated(config: L0Config, replicas: Vec<ReplicaTarget>) -> Result<Self> {
        Ok(Self {
            engine: L0Engine::open_replicated(config, replicas)?,
        })
    }
    /// Cold-load a single-replica runtime store.
    pub fn cold_load(config: L0Config, substrate: Arc<dyn DurableSubstrate>) -> Result<Self> {
        Ok(Self {
            engine: L0Engine::cold_load(config, substrate)?,
        })
    }
    /// Cold-load an N-way replicated runtime store.
    pub fn cold_load_replicated(config: L0Config, replicas: Vec<ReplicaTarget>) -> Result<Self> {
        Ok(Self {
            engine: L0Engine::cold_load_replicated(config, replicas)?,
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
        self.engine.append_record(RuntimeOp {
            op_seq,
            worker_id: worker_id.to_string(),
            event,
            heartbeat,
            contract,
        })?;
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
