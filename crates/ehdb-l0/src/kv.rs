//! **D4 — KV / coherence state** (chain heads, exec descriptors,
//! subscription-circuit, leases, cursors), RFC §0.1.
//!
//! Low-latency key/value with **get/put-latest, CAS, and prefix-scan** — over
//! immutable parts. Modeled as an append-only log of `KvOp` puts; the current
//! value of a key is the **latest** put (a fold that, since
//! [`read_index_after`](crate::engine::L0Engine::read_index_after) returns a
//! key's ops in order, is just "the last one"). A monotonic per-key `version`
//! rides each put so **CAS** (compare-and-set) is a read-latest + conditional
//! append. A tombstone (`deleted`) models delete.
//!
//! Fixed shape: **sort key** = `op_seq`; **partition** + **index dim** = `key`.

use std::collections::BTreeMap;
use std::sync::Arc;

use ehdb_core::Result;
use serde::{Deserialize, Serialize};

use crate::dataset::{shard_for_execution, Dataset};
use crate::engine::{L0Config, L0Engine, ReplicaTarget};
use crate::substrate::DurableSubstrate;

/// The D4 dataset id.
pub const DATASET_D4_KV: &str = "d4_kv";

/// One KV put/delete in the op log (the D4 record schema).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct KvOp {
    /// Op-log position (the fixed sort key).
    pub op_seq: u64,
    /// The key (partition + index dim).
    pub key: String,
    /// The value at this put (empty when `deleted`).
    pub value: String,
    /// Monotonic per-key version — the CAS token (1 for the first put).
    pub version: u64,
    /// Tombstone: `true` marks a delete.
    pub deleted: bool,
}

/// **D4 KV dataset.** Sort key `op_seq`; partition + index dim `key`.
#[derive(Debug, Clone, Copy)]
pub struct KvDataset;

impl Dataset for KvDataset {
    type Record = KvOp;
    const NAME: &'static str = DATASET_D4_KV;

    fn sort_key(r: &KvOp) -> u64 {
        r.op_seq
    }
    fn partition(r: &KvOp, shard_count: u32) -> u32 {
        shard_for_execution(&r.key, shard_count)
    }
    fn index_key(r: &KvOp) -> &str {
        &r.key
    }
    fn read_partition(key: &str, shard_count: u32) -> u32 {
        shard_for_execution(key, shard_count)
    }
}

/// A key's current value + version (the fold result).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KvEntry {
    pub value: String,
    pub version: u64,
}

/// **The D4 KV store** — get/put-latest + CAS + prefix-scan over the generic
/// engine.
pub struct KvStore {
    engine: L0Engine<KvDataset>,
}

impl KvStore {
    /// Config default for D4 rooted at `local_root`.
    pub fn config(local_root: impl Into<std::path::PathBuf>) -> L0Config {
        L0Config::for_dataset(DATASET_D4_KV, local_root)
    }

    /// Open a single-replica KV store.
    pub fn open(config: L0Config, substrate: Arc<dyn DurableSubstrate>) -> Result<Self> {
        Ok(Self {
            engine: L0Engine::open(config, substrate)?,
        })
    }
    /// Open an N-way replicated KV store.
    pub fn open_replicated(config: L0Config, replicas: Vec<ReplicaTarget>) -> Result<Self> {
        Ok(Self {
            engine: L0Engine::open_replicated(config, replicas)?,
        })
    }
    /// Cold-load a single-replica KV store.
    pub fn cold_load(config: L0Config, substrate: Arc<dyn DurableSubstrate>) -> Result<Self> {
        Ok(Self {
            engine: L0Engine::cold_load(config, substrate)?,
        })
    }
    /// Cold-load an N-way replicated KV store.
    pub fn cold_load_replicated(config: L0Config, replicas: Vec<ReplicaTarget>) -> Result<Self> {
        Ok(Self {
            engine: L0Engine::cold_load_replicated(config, replicas)?,
        })
    }

    /// **get-latest** — the current value + version of `key`, or `None` if unset
    /// or deleted (index-pruned read, last wins).
    pub fn get(&self, key: &str) -> Result<Option<KvEntry>> {
        let ops = self.engine.read_index_after(key, 0)?;
        Ok(ops.into_iter().next_back().and_then(|op| {
            if op.deleted {
                None
            } else {
                Some(KvEntry {
                    value: op.value,
                    version: op.version,
                })
            }
        }))
    }

    /// **put** — unconditionally set `key` to `value`, returning the new version.
    pub fn put(&mut self, key: &str, value: impl Into<String>) -> Result<u64> {
        let next_version = self.get(key)?.map(|e| e.version).unwrap_or(0) + 1;
        self.append(key, value.into(), next_version, false)?;
        Ok(next_version)
    }

    /// **CAS (compare-and-set)** — set `key` to `value` only if its current
    /// version equals `expected_version` (use `0` for "must not exist / deleted").
    /// Returns the new version on success, `None` on a version mismatch.
    pub fn compare_and_set(
        &mut self,
        key: &str,
        expected_version: u64,
        value: impl Into<String>,
    ) -> Result<Option<u64>> {
        let current = self.get(key)?.map(|e| e.version).unwrap_or(0);
        if current != expected_version {
            return Ok(None);
        }
        let next = current + 1;
        self.append(key, value.into(), next, false)?;
        Ok(Some(next))
    }

    /// **delete** — tombstone `key` (a later `get` returns `None`). Returns the
    /// tombstone version, or `None` if the key was already absent.
    pub fn delete(&mut self, key: &str) -> Result<Option<u64>> {
        match self.get(key)? {
            None => Ok(None),
            Some(e) => {
                let next = e.version + 1;
                self.append(key, String::new(), next, true)?;
                Ok(Some(next))
            }
        }
    }

    /// **prefix-scan** — the live (key, value) pairs whose key starts with
    /// `prefix`, in key order. Folds the whole log to the latest per key, drops
    /// tombstoned keys.
    pub fn prefix_scan(&self, prefix: &str) -> Result<Vec<(String, String)>> {
        let all = self.engine.replay_all()?; // sorted by op_seq
        let mut latest: BTreeMap<String, KvOp> = BTreeMap::new();
        for op in all {
            latest.insert(op.key.clone(), op); // last op_seq wins
        }
        Ok(latest
            .into_iter()
            .filter(|(k, op)| k.starts_with(prefix) && !op.deleted)
            .map(|(k, op)| (k, op.value))
            .collect())
    }

    fn append(&mut self, key: &str, value: String, version: u64, deleted: bool) -> Result<()> {
        let op_seq = self.engine.global_sequence() + 1;
        self.engine.append_record(KvOp {
            op_seq,
            key: key.to_string(),
            value,
            version,
            deleted,
        })?;
        Ok(())
    }

    /// Flush all sealed parts to the durable replicas.
    pub fn flush_and_wait(&mut self) -> Result<()> {
        self.engine.flush_and_wait_uploads()
    }
    /// Run background merge/compaction over the KV log.
    pub fn run_pending_merges(&mut self) -> Result<usize> {
        self.engine.run_pending_merges()
    }
    /// The underlying generic engine.
    pub fn engine(&self) -> &L0Engine<KvDataset> {
        &self.engine
    }
}
