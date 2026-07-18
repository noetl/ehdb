//! **D2 — the command queue** (`noetl.command` / `noetl.outbox`), RFC §0.1.
//!
//! A queue has *mutable* per-command state (unclaimed → claimed), but L0 parts
//! are **immutable**. The CQRS resolution: model the queue as an **append-only
//! log of queue operations** (`Enqueue`, `Claim`) over immutable parts, and
//! derive the current state — "which commands are still unclaimed" — as a
//! **fold/projection** over that log. Parts stay immutable; nothing is ever
//! mutated in place; claim-state is a view.
//!
//! Fixed shape (compiled-in, no runtime schema):
//! - **sort key** = `op_seq`, the monotonic op-log position (the fold order).
//! - **partition** = `shard_for(command_id)`, so all ops for one command are
//!   co-located.
//! - **index dimension** = `command_id`, so `claim-by-id` reads just that
//!   command's ops (the L0.2 bloom prunes to it).
//!
//! Access paths ([`CommandQueue`]): **enqueue**, **claim-by-id** (append a claim
//! op), **command_state** (fold one command's ops), **unclaimed_scan** (fold all
//! ops → the still-unclaimed command ids). All run on the generic
//! [`L0Engine`]`<`[`D2CommandQueue`]`>` — the shared part / catalog / merge /
//! N-way-replication engine, unchanged.

use std::collections::BTreeMap;
use std::sync::Arc;

use ehdb_core::Result;
use serde::{Deserialize, Serialize};

use crate::dataset::{shard_for_execution, Dataset};
use crate::engine::{L0Config, L0Engine, ReplicaTarget};
use crate::substrate::DurableSubstrate;

/// The D2 dataset id.
pub const DATASET_D2_COMMAND_QUEUE: &str = "d2_command_queue";

/// One queue operation in the append-only op log (the D2 record schema).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CommandOpRecord {
    /// The op-log position (the fixed sort key / fold order).
    pub op_seq: u64,
    /// Which command this op concerns (the fixed index dimension).
    pub command_id: String,
    /// The kind of operation.
    pub kind: CommandOpKind,
    /// The command payload (set on `Enqueue`, empty otherwise).
    pub payload: String,
    /// The claimer (set on `Claim`, empty otherwise).
    pub claimer: String,
}

/// The two queue operations. `Complete`/`Nack` are later additions; L0 D2 proves
/// the enqueue→claim lifecycle over immutable parts.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub enum CommandOpKind {
    /// A command was placed on the queue.
    Enqueue,
    /// A command was claimed by a worker.
    Claim,
}

/// **D2 command-queue dataset.** Sort key `op_seq`; partition + index dim
/// `command_id`.
#[derive(Debug, Clone, Copy)]
pub struct D2CommandQueue;

impl Dataset for D2CommandQueue {
    type Record = CommandOpRecord;
    const NAME: &'static str = DATASET_D2_COMMAND_QUEUE;

    fn sort_key(record: &CommandOpRecord) -> u64 {
        record.op_seq
    }
    fn partition(record: &CommandOpRecord, shard_count: u32) -> u32 {
        shard_for_execution(&record.command_id, shard_count)
    }
    fn index_key(record: &CommandOpRecord) -> &str {
        &record.command_id
    }
    fn read_partition(command_id: &str, shard_count: u32) -> u32 {
        shard_for_execution(command_id, shard_count)
    }
}

/// The derived state of one command (the fold result).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CommandState {
    /// Enqueued and not yet claimed.
    Unclaimed,
    /// Claimed by `claimer`.
    Claimed { claimer: String },
}

/// Fold a single command's ops (already in `op_seq` order) into its current
/// state. `None` if the command was never enqueued.
fn fold_command(ops: &[CommandOpRecord]) -> Option<CommandState> {
    let mut state: Option<CommandState> = None;
    for op in ops {
        match op.kind {
            CommandOpKind::Enqueue => state = Some(CommandState::Unclaimed),
            CommandOpKind::Claim => {
                // A claim only takes effect on an enqueued command.
                if state.is_some() {
                    state = Some(CommandState::Claimed {
                        claimer: op.claimer.clone(),
                    });
                }
            }
        }
    }
    state
}

/// **The D2 command queue** — a thin projection API over the generic
/// [`L0Engine`]`<`[`D2CommandQueue`]`>`. Holds the op-log tip so `enqueue` /
/// `claim` assign monotonic `op_seq`s.
pub struct CommandQueue {
    engine: L0Engine<D2CommandQueue>,
}

impl CommandQueue {
    /// D1-style config default for D2 (single owner, posture A) rooted at
    /// `local_root`.
    pub fn config(local_root: impl Into<std::path::PathBuf>) -> L0Config {
        L0Config::for_dataset(DATASET_D2_COMMAND_QUEUE, local_root)
    }

    /// Open a single-replica command queue.
    pub fn open(config: L0Config, substrate: Arc<dyn DurableSubstrate>) -> Result<Self> {
        Ok(Self {
            engine: L0Engine::open(config, substrate)?,
        })
    }

    /// Open an N-way replicated command queue (L0.6 durability).
    pub fn open_replicated(config: L0Config, replicas: Vec<ReplicaTarget>) -> Result<Self> {
        Ok(Self {
            engine: L0Engine::open_replicated(config, replicas)?,
        })
    }

    /// Cold-load a single-replica queue (reconstructs op-log state).
    pub fn cold_load(config: L0Config, substrate: Arc<dyn DurableSubstrate>) -> Result<Self> {
        Ok(Self {
            engine: L0Engine::cold_load(config, substrate)?,
        })
    }

    /// Cold-load an N-way replicated queue (survives a dead replica).
    pub fn cold_load_replicated(config: L0Config, replicas: Vec<ReplicaTarget>) -> Result<Self> {
        Ok(Self {
            engine: L0Engine::cold_load_replicated(config, replicas)?,
        })
    }

    /// **Enqueue** a command — append an `Enqueue` op. Returns its `op_seq`.
    pub fn enqueue(&mut self, command_id: &str, payload: impl Into<String>) -> Result<u64> {
        let op_seq = self.engine.global_sequence() + 1;
        self.engine.append_record(CommandOpRecord {
            op_seq,
            command_id: command_id.to_string(),
            kind: CommandOpKind::Enqueue,
            payload: payload.into(),
            claimer: String::new(),
        })
    }

    /// **Claim** a command by id — append a `Claim` op. Returns its `op_seq`.
    /// (Contention/CAS is a higher-layer concern; L0 records the op.)
    pub fn claim(&mut self, command_id: &str, claimer: impl Into<String>) -> Result<u64> {
        let op_seq = self.engine.global_sequence() + 1;
        self.engine.append_record(CommandOpRecord {
            op_seq,
            command_id: command_id.to_string(),
            kind: CommandOpKind::Claim,
            payload: String::new(),
            claimer: claimer.into(),
        })
    }

    /// **Claim-by-id state** — fold just this command's ops (index-pruned read).
    /// `None` if the command was never enqueued.
    pub fn command_state(&self, command_id: &str) -> Result<Option<CommandState>> {
        let ops = self.engine.read_index_after(command_id, 0)?;
        Ok(fold_command(&ops))
    }

    /// **Unclaimed scan** — fold the whole op log and return the command ids that
    /// are enqueued but not yet claimed, in id order.
    pub fn unclaimed_scan(&self) -> Result<Vec<String>> {
        let ops = self.engine.replay_all()?; // sorted by op_seq
        let mut by_cmd: BTreeMap<String, Vec<CommandOpRecord>> = BTreeMap::new();
        for op in ops {
            by_cmd.entry(op.command_id.clone()).or_default().push(op);
        }
        let mut unclaimed: Vec<String> = by_cmd
            .into_iter()
            .filter_map(|(cmd, ops)| match fold_command(&ops) {
                Some(CommandState::Unclaimed) => Some(cmd),
                _ => None,
            })
            .collect();
        unclaimed.sort();
        Ok(unclaimed)
    }

    /// Flush all sealed parts to the durable replicas (durability barrier).
    pub fn flush_and_wait(&mut self) -> Result<()> {
        self.engine.flush_and_wait_uploads()
    }

    /// Run background merge/compaction (L0.3) over the op log.
    pub fn run_pending_merges(&mut self) -> Result<usize> {
        self.engine.run_pending_merges()
    }

    /// Access the underlying generic engine (for metrics / manifest / retention).
    pub fn engine(&self) -> &L0Engine<D2CommandQueue> {
        &self.engine
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fold_enqueue_then_claim() {
        let ops = vec![
            CommandOpRecord {
                op_seq: 1,
                command_id: "c1".into(),
                kind: CommandOpKind::Enqueue,
                payload: "p".into(),
                claimer: String::new(),
            },
            CommandOpRecord {
                op_seq: 5,
                command_id: "c1".into(),
                kind: CommandOpKind::Claim,
                payload: String::new(),
                claimer: "worker-a".into(),
            },
        ];
        assert_eq!(
            fold_command(&ops),
            Some(CommandState::Claimed {
                claimer: "worker-a".into()
            })
        );
        assert_eq!(fold_command(&ops[..1]), Some(CommandState::Unclaimed));
        assert_eq!(fold_command(&[]), None);
    }

    #[test]
    fn claim_before_enqueue_is_ignored() {
        let ops = vec![CommandOpRecord {
            op_seq: 1,
            command_id: "c1".into(),
            kind: CommandOpKind::Claim,
            payload: String::new(),
            claimer: "x".into(),
        }];
        assert_eq!(fold_command(&ops), None);
    }
}
