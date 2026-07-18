//! **D3 — execution / projection read-models** (`noetl.execution` +
//! `projection_snapshot`), RFC §0.1.
//!
//! A read-model is the *current* materialized state of an execution, derived
//! from the append-only event log. On immutable parts we model it the same way
//! the whole platform does: an **append-only log of projection snapshots**
//! (`ProjectionOp`, one per state transition), with the current state of an
//! execution being the **latest** snapshot — a fold that, because
//! [`read_index_after`](crate::engine::L0Engine::read_index_after) returns a
//! key's records already in sort-key order, is just "the last one".
//!
//! Fixed shape: **sort key** = `proj_seq` (snapshot order); **partition** +
//! **index dim** = `execution_id`. Access paths: **get-state-by-execution**
//! (latest snapshot), **list-executions** (distinct ids).

use std::collections::BTreeSet;
use std::sync::Arc;

use ehdb_core::Result;
use serde::{Deserialize, Serialize};

use crate::dataset::{shard_for_execution, Dataset};
use crate::engine::{L0Config, L0Engine, ReplicaTarget};
use crate::substrate::DurableSubstrate;

/// The D3 dataset id.
pub const DATASET_D3_PROJECTION: &str = "d3_projection";

/// One projection snapshot in the read-model op log (the D3 record schema).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProjectionOp {
    /// Snapshot order (the fixed sort key). The latest wins.
    pub proj_seq: u64,
    /// The execution this snapshot is for (partition + index dim).
    pub execution_id: String,
    /// The execution's status at this snapshot (e.g. `running`, `completed`).
    pub status: String,
    /// The materialized read-model data at this snapshot (opaque; noetl-internal).
    pub data: String,
}

/// **D3 projection read-model dataset.** Sort key `proj_seq`; partition + index
/// dim `execution_id`.
#[derive(Debug, Clone, Copy)]
pub struct ExecutionProjection;

impl Dataset for ExecutionProjection {
    type Record = ProjectionOp;
    const NAME: &'static str = DATASET_D3_PROJECTION;

    fn sort_key(r: &ProjectionOp) -> u64 {
        r.proj_seq
    }
    fn partition(r: &ProjectionOp, shard_count: u32) -> u32 {
        shard_for_execution(&r.execution_id, shard_count)
    }
    fn index_key(r: &ProjectionOp) -> &str {
        &r.execution_id
    }
    fn read_partition(execution_id: &str, shard_count: u32) -> u32 {
        shard_for_execution(execution_id, shard_count)
    }
}

/// The current read-model state of one execution.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecutionState {
    pub execution_id: String,
    pub status: String,
    pub data: String,
    pub proj_seq: u64,
}

/// **The D3 projection store** — a read-model projection over the generic engine.
pub struct ProjectionStore {
    engine: L0Engine<ExecutionProjection>,
}

impl ProjectionStore {
    /// Config default for D3 rooted at `local_root`.
    pub fn config(local_root: impl Into<std::path::PathBuf>) -> L0Config {
        L0Config::for_dataset(DATASET_D3_PROJECTION, local_root)
    }

    /// Open a single-replica projection store.
    pub fn open(config: L0Config, substrate: Arc<dyn DurableSubstrate>) -> Result<Self> {
        Ok(Self {
            engine: L0Engine::open(config, substrate)?,
        })
    }
    /// Open an N-way replicated projection store.
    pub fn open_replicated(config: L0Config, replicas: Vec<ReplicaTarget>) -> Result<Self> {
        Ok(Self {
            engine: L0Engine::open_replicated(config, replicas)?,
        })
    }
    /// Cold-load a single-replica projection store.
    pub fn cold_load(config: L0Config, substrate: Arc<dyn DurableSubstrate>) -> Result<Self> {
        Ok(Self {
            engine: L0Engine::cold_load(config, substrate)?,
        })
    }
    /// Cold-load an N-way replicated projection store.
    pub fn cold_load_replicated(config: L0Config, replicas: Vec<ReplicaTarget>) -> Result<Self> {
        Ok(Self {
            engine: L0Engine::cold_load_replicated(config, replicas)?,
        })
    }

    /// **Record a new projection snapshot** for an execution (a state
    /// transition). Returns its `proj_seq`.
    pub fn record_state(
        &mut self,
        execution_id: &str,
        status: impl Into<String>,
        data: impl Into<String>,
    ) -> Result<u64> {
        let proj_seq = self.engine.global_sequence() + 1;
        self.engine.append_record(ProjectionOp {
            proj_seq,
            execution_id: execution_id.to_string(),
            status: status.into(),
            data: data.into(),
        })
    }

    /// **Get-state-by-execution** — the latest snapshot for `execution_id`
    /// (index-pruned read, last wins), or `None` if never recorded.
    pub fn get_state(&self, execution_id: &str) -> Result<Option<ExecutionState>> {
        let ops = self.engine.read_index_after(execution_id, 0)?;
        Ok(ops.into_iter().next_back().map(|op| ExecutionState {
            execution_id: op.execution_id,
            status: op.status,
            data: op.data,
            proj_seq: op.proj_seq,
        }))
    }

    /// **List-executions** — the distinct execution ids known to the read model,
    /// in id order.
    pub fn list_executions(&self) -> Result<Vec<String>> {
        let all = self.engine.replay_all()?;
        let set: BTreeSet<String> = all.into_iter().map(|op| op.execution_id).collect();
        Ok(set.into_iter().collect())
    }

    /// Flush all sealed parts to the durable replicas.
    pub fn flush_and_wait(&mut self) -> Result<()> {
        self.engine.flush_and_wait_uploads()
    }
    /// Run background merge/compaction over the projection log.
    pub fn run_pending_merges(&mut self) -> Result<usize> {
        self.engine.run_pending_merges()
    }
    /// The underlying generic engine (metrics / manifest / retention).
    pub fn engine(&self) -> &L0Engine<ExecutionProjection> {
        &self.engine
    }
}
