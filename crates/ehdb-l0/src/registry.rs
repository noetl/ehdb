//! **D7 — catalog** (playbook / tool / resource registry, the `noetl.catalog`
//! table), RFC §0.1.
//!
//! A versioned registry with **register**, **get-by-path** (latest or a pinned
//! version), **list / snapshot**, and **deregister** — over immutable parts.
//! Modeled as an append-only log of `CatalogOp`; each register appends a new
//! monotonic per-path `version`, so a path's history is preserved and a pinned
//! read (`get_version`) is exact. A path's current entry is its latest non-
//! deleted op (a fold that, since [`read_index_after`] returns a path's ops in
//! order, is "the last one"). A tombstone (`deleted`) models deregister.
//!
//! Fixed shape: **sort key** = `op_seq`; **partition** + **index dim** = `path`
//! (so a get-by-path reads exactly one path's version history).
//!
//! Named `registry` to avoid colliding with the internal [`crate::catalog`]
//! meta-catalog (the manifest + sparse index); the dataset id is `d7_catalog`.
//!
//! [`read_index_after`]: crate::engine::L0Engine::read_index_after

use std::collections::BTreeMap;
use std::sync::Arc;

use ehdb_core::Result;
use serde::{Deserialize, Serialize};

use crate::dataset::{shard_for_execution, Dataset};
use crate::engine::{L0Config, L0Engine, ReplicaTarget};
use crate::substrate::DurableSubstrate;

/// The D7 dataset id.
pub const DATASET_D7_CATALOG: &str = "d7_catalog";

/// What a catalog entry describes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CatalogKind {
    Playbook,
    Tool,
    Resource,
}

/// One catalog register/deregister in the op log (the D7 record schema).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CatalogOp {
    /// Op-log position (the fixed sort key).
    pub op_seq: u64,
    /// The catalog path (partition + index dim), e.g. `system/auth`.
    pub path: String,
    /// Monotonic per-path version (1 for the first register).
    pub version: u64,
    /// What this entry is.
    pub kind: CatalogKind,
    /// The entry content (playbook YAML, tool spec, resource descriptor).
    /// Empty on `deleted`.
    pub content: String,
    /// Tombstone: `true` deregisters the path.
    pub deleted: bool,
}

/// **D7 catalog dataset.** Sort key `op_seq`; partition + index dim `path`.
#[derive(Debug, Clone, Copy)]
pub struct CatalogDataset;

impl Dataset for CatalogDataset {
    type Record = CatalogOp;
    const NAME: &'static str = DATASET_D7_CATALOG;

    fn sort_key(r: &CatalogOp) -> u64 {
        r.op_seq
    }
    fn partition(r: &CatalogOp, shard_count: u32) -> u32 {
        shard_for_execution(&r.path, shard_count)
    }
    fn index_key(r: &CatalogOp) -> &str {
        &r.path
    }
    fn read_partition(path: &str, shard_count: u32) -> u32 {
        shard_for_execution(path, shard_count)
    }
}

/// A catalog entry at a specific version (the fold / pinned-read result).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CatalogEntry {
    pub path: String,
    pub version: u64,
    pub kind: CatalogKind,
    pub content: String,
}

/// **The D7 catalog store** — register / get / list / deregister over the
/// generic engine.
pub struct CatalogStore {
    engine: L0Engine<CatalogDataset>,
}

impl CatalogStore {
    /// Config default for D7 rooted at `local_root`.
    pub fn config(local_root: impl Into<std::path::PathBuf>) -> L0Config {
        L0Config::for_dataset(DATASET_D7_CATALOG, local_root)
    }

    /// Open a single-replica catalog store.
    pub fn open(config: L0Config, substrate: Arc<dyn DurableSubstrate>) -> Result<Self> {
        Ok(Self {
            engine: L0Engine::open(config, substrate)?,
        })
    }
    /// Open an N-way replicated catalog store.
    pub fn open_replicated(config: L0Config, replicas: Vec<ReplicaTarget>) -> Result<Self> {
        Ok(Self {
            engine: L0Engine::open_replicated(config, replicas)?,
        })
    }
    /// Cold-load a single-replica catalog store.
    pub fn cold_load(config: L0Config, substrate: Arc<dyn DurableSubstrate>) -> Result<Self> {
        Ok(Self {
            engine: L0Engine::cold_load(config, substrate)?,
        })
    }
    /// Cold-load an N-way replicated catalog store.
    pub fn cold_load_replicated(config: L0Config, replicas: Vec<ReplicaTarget>) -> Result<Self> {
        Ok(Self {
            engine: L0Engine::cold_load_replicated(config, replicas)?,
        })
    }

    /// The full version history of a path, oldest first (index-pruned read).
    fn history(&self, path: &str) -> Result<Vec<CatalogOp>> {
        self.engine.read_index_after(path, 0) // sorted by op_seq
    }

    /// **register** — publish a new version of `path` with `content`, returning
    /// the assigned version. Registering a previously-deregistered path revives
    /// it at the next version (history is preserved).
    pub fn register(
        &mut self,
        path: &str,
        kind: CatalogKind,
        content: impl Into<String>,
    ) -> Result<u64> {
        let next = self
            .history(path)?
            .into_iter()
            .next_back()
            .map(|op| op.version)
            .unwrap_or(0)
            + 1;
        let op_seq = self.engine.global_sequence() + 1;
        self.engine.append_record(CatalogOp {
            op_seq,
            path: path.to_string(),
            version: next,
            kind,
            content: content.into(),
            deleted: false,
        })?;
        Ok(next)
    }

    /// **get-by-path (latest)** — the current entry for `path`, or `None` if it
    /// was never registered or is currently deregistered.
    pub fn get(&self, path: &str) -> Result<Option<CatalogEntry>> {
        Ok(self.history(path)?.into_iter().next_back().and_then(|op| {
            if op.deleted {
                None
            } else {
                Some(CatalogEntry {
                    path: op.path,
                    version: op.version,
                    kind: op.kind,
                    content: op.content,
                })
            }
        }))
    }

    /// **get-by-path (pinned)** — a specific published `version` of `path`, even
    /// if a newer version or a deregister has since landed. `None` if that
    /// version was never published (or was a tombstone).
    pub fn get_version(&self, path: &str, version: u64) -> Result<Option<CatalogEntry>> {
        Ok(self
            .history(path)?
            .into_iter()
            .find(|op| op.version == version && !op.deleted)
            .map(|op| CatalogEntry {
                path: op.path,
                version: op.version,
                kind: op.kind,
                content: op.content,
            }))
    }

    /// **deregister** — tombstone `path` (a later `get` returns `None`; pinned
    /// reads of prior versions still resolve). Returns the tombstone version, or
    /// `None` if the path was already absent.
    pub fn deregister(&mut self, path: &str) -> Result<Option<u64>> {
        match self.get(path)? {
            None => Ok(None),
            Some(entry) => {
                let next = entry.version + 1;
                let op_seq = self.engine.global_sequence() + 1;
                self.engine.append_record(CatalogOp {
                    op_seq,
                    path: path.to_string(),
                    version: next,
                    kind: entry.kind,
                    content: String::new(),
                    deleted: true,
                })?;
                Ok(Some(next))
            }
        }
    }

    /// **snapshot / list** — the live entries (latest per path, deregistered
    /// paths dropped), in path order. `kind` filters to one entry kind when set.
    pub fn snapshot(&self, kind: Option<CatalogKind>) -> Result<Vec<CatalogEntry>> {
        let all = self.engine.replay_all()?; // sorted by op_seq
        let mut latest: BTreeMap<String, CatalogOp> = BTreeMap::new();
        for op in all {
            latest.insert(op.path.clone(), op); // last op_seq wins
        }
        Ok(latest
            .into_values()
            .filter(|op| !op.deleted && kind.is_none_or(|k| k == op.kind))
            .map(|op| CatalogEntry {
                path: op.path,
                version: op.version,
                kind: op.kind,
                content: op.content,
            })
            .collect())
    }

    /// Flush all sealed parts to the durable replicas.
    pub fn flush_and_wait(&mut self) -> Result<()> {
        self.engine.flush_and_wait_uploads()
    }
    /// Run background merge/compaction over the catalog log.
    pub fn run_pending_merges(&mut self) -> Result<usize> {
        self.engine.run_pending_merges()
    }
    /// The underlying generic engine.
    pub fn engine(&self) -> &L0Engine<CatalogDataset> {
        &self.engine
    }
}
