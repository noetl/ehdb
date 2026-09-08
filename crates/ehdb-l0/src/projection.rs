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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::substrate::LocalFsSubstrate;
    use std::sync::Arc;

    fn store(dir: &std::path::Path) -> ProjectionStore {
        let sub: Arc<dyn DurableSubstrate> =
            Arc::new(LocalFsSubstrate::new(dir.join("substrate")).unwrap());
        ProjectionStore::open(ProjectionStore::config(dir.join("local")), sub).unwrap()
    }

    /// The basic contract the embedded design assumes: append snapshots, read
    /// back the LATEST per execution.
    #[test]
    fn append_then_read_returns_the_latest_snapshot() {
        let tmp = tempfile::tempdir().unwrap();
        let mut s = store(tmp.path());
        s.record_state("exec-a", "running", "{}").unwrap();
        s.record_state("exec-a", "completed", "{\"n\":2}").unwrap();
        s.record_state("exec-b", "running", "{}").unwrap();

        let a = s.get_state("exec-a").unwrap().expect("exec-a must exist");
        assert_eq!(
            a.status, "completed",
            "the fold must return the LATEST snapshot"
        );
        // ⚠ proj_seq is assigned by the ENGINE from a GLOBAL sequence, not per
        // execution — so it is 2 here only because exec-a wrote the first two.
        assert_eq!(a.proj_seq, 2);
        let b = s.get_state("exec-b").unwrap().expect("exec-b must exist");
        assert_eq!(b.status, "running");
        assert_eq!(s.get_state("exec-missing").unwrap(), None);
    }

    /// Enumeration — the fan-out surface.
    #[test]
    fn list_executions_enumerates_distinct_ids() {
        let tmp = tempfile::tempdir().unwrap();
        let mut s = store(tmp.path());
        for i in 1..=3u64 {
            s.record_state(&format!("exec-{i}"), "running", "{}")
                .unwrap();
        }
        s.record_state("exec-1", "completed", "{}").unwrap();
        let mut ids = s.list_executions().unwrap();
        ids.sort();
        assert_eq!(
            ids,
            vec!["exec-1", "exec-2", "exec-3"],
            "distinct ids, not snapshots"
        );
    }

    /// ⚠⚠ THE ONE THE RFC RESTS ON: recovery = reopen from durable state and see
    /// what was there. If this does not hold, "restore snapshot + replay tail" has
    /// no foundation and the embedded design needs a different story.
    #[test]
    fn state_survives_a_reopen() {
        let tmp = tempfile::tempdir().unwrap();
        {
            let mut s = store(tmp.path());
            s.record_state("exec-a", "running", "{}").unwrap();
            s.record_state("exec-a", "completed", "{\"done\":true}")
                .unwrap();
            s.flush_and_wait().unwrap();
        } // dropped — simulating a restart

        let s2 = store(tmp.path());
        let a = s2
            .get_state("exec-a")
            .unwrap()
            .expect("state must survive a restart — this is the recovery premise");
        assert_eq!(a.status, "completed");
        // ⚠ proj_seq is assigned by the ENGINE from a GLOBAL sequence, not per
        // execution — so it is 2 here only because exec-a wrote the first two.
        assert_eq!(a.proj_seq, 2);
    }

    /// `cold_load` is the RFC's "restore" primitive. Does it actually restore?
    #[test]
    fn cold_load_recovers_state_without_a_prior_open() {
        let tmp = tempfile::tempdir().unwrap();
        {
            let mut s = store(tmp.path());
            s.record_state("exec-c", "completed", "{}").unwrap();
            s.flush_and_wait().unwrap();
        }
        let sub: Arc<dyn DurableSubstrate> =
            Arc::new(LocalFsSubstrate::new(tmp.path().join("substrate")).unwrap());
        let cold =
            ProjectionStore::cold_load(ProjectionStore::config(tmp.path().join("local2")), sub)
                .unwrap();
        let c = cold
            .get_state("exec-c")
            .unwrap()
            .expect("cold_load must see durable state written by another process");
        // Engine-assigned: the first record in a fresh store is seq 1.
        assert_eq!(c.proj_seq, 1);
    }

    /// ⚠⚠ THE POINT OF STEP 3: recovery reads the TAIL, not the log.
    ///
    /// The old checkpoint answered "where am I" by replaying everything and
    /// taking the max. This proves the stored cursor removes that scan — by
    /// COUNTING the records recovery actually reads, because "it is faster" is
    /// not a property a test can assert and "it read fewer rows" is.
    #[test]
    fn recovery_from_a_stored_cursor_reads_only_the_tail() {
        use crate::cursor;
        let tmp = tempfile::tempdir().unwrap();
        let sub: Arc<dyn DurableSubstrate> =
            Arc::new(LocalFsSubstrate::new(tmp.path().join("substrate")).unwrap());

        let mut s = ProjectionStore::open(
            ProjectionStore::config(tmp.path().join("local")),
            sub.clone(),
        )
        .unwrap();
        for i in 0..20u64 {
            s.record_state(&format!("exec-{i}"), "running", "{}")
                .unwrap();
        }
        s.flush_and_wait().unwrap();
        let total = s.engine().global_sequence();
        assert_eq!(total, 20, "20 appends should reach sequence 20");

        // A consumer applied through 15 and stored it.
        cursor::advance(sub.as_ref(), 0, 15).unwrap();

        // Recovery: load the cursor, read only past it.
        let resume = cursor::load(sub.as_ref(), 0).unwrap();
        assert_eq!(resume, 15);
        let tail = s.engine().read_partition_after(0, resume).unwrap();
        let full = s.engine().read_partition_after(0, 0).unwrap();

        assert_eq!(full.len(), 20, "the whole log is 20 records");
        assert_eq!(tail.len(), 5, "the tail past 15 is 5 records");
        assert!(
            tail.len() < full.len(),
            "recovery must read strictly fewer records than a full scan — that is \
             the entire debt this step pays off"
        );
        assert!(
            tail.iter().all(|r| r.proj_seq > resume),
            "every replayed record must be past the cursor; re-applying at or \
             below it is the double-apply the cursor exists to prevent"
        );
    }

    /// ⚠⚠ THE CURSOR RECONCILIATION (noetl/ai-meta#332 step 2/3).
    ///
    /// `ProjectionStore` has **no** `checkpoint()`. The durable
    /// `ProjectionCheckpoint` lives on the OTHER lineage
    /// (`ehdb-reference::ProjectionDriver`), and it is derived by replaying the
    /// whole log.
    ///
    /// What `ehdb-l0` has instead is the engine's own `global_sequence()`. This
    /// pins whether it can serve as the cursor — i.e. whether it is monotonic
    /// AND survives a restart. If it does, a stored cursor is a small addition;
    /// if it does not, step 3 needs a different foundation.
    #[test]
    fn the_engine_sequence_is_monotonic_and_survives_a_restart() {
        let tmp = tempfile::tempdir().unwrap();
        let last = {
            let mut s = store(tmp.path());
            let a = s.record_state("exec-a", "running", "{}").unwrap();
            let b = s.record_state("exec-b", "running", "{}").unwrap();
            assert!(
                b > a,
                "proj_seq must be monotonic across executions: {a} then {b}"
            );
            s.flush_and_wait().unwrap();
            s.engine().global_sequence()
        };
        assert!(
            last >= 2,
            "expected at least 2 records, engine reports {last}"
        );

        let s2 = store(tmp.path());
        assert_eq!(
            s2.engine().global_sequence(),
            last,
            "the engine sequence must survive a restart to serve as a resume cursor"
        );
        // And the next append continues rather than restarting at 1.
        let mut s2 = s2;
        let next = s2.record_state("exec-c", "running", "{}").unwrap();
        assert!(
            next > last,
            "a restarted engine must not reissue sequences: {last} then {next}"
        );
    }

    /// The manifest snapshot is storage bookkeeping, not the read model. Pinned
    /// because the RFC initially conflated the two.
    #[test]
    fn the_manifest_snapshot_is_storage_layout_not_read_model() {
        let tmp = tempfile::tempdir().unwrap();
        let mut s = store(tmp.path());
        s.record_state("exec-a", "running", "{}").unwrap();
        s.flush_and_wait().unwrap();
        let m = s.engine().manifest_snapshot();
        assert_eq!(m.dataset, DATASET_D3_PROJECTION);
    }
}
