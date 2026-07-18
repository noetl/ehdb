//! **D5 — object / blob** (state shards #166, result tier #104, Arrow-IPC,
//! artifacts), RFC §0.1 + §2.4.
//!
//! Large opaque blobs do **not** fit the VM columnar/metrics layout (RFC §2.4),
//! so D5 has a **deliberately non-part shape**: the bytes are stored **whole and
//! content-addressed** on the durable substrate (`objects/<digest>`, written
//! once, deduped, replicated N-way); L0 holds only a **registry op-log**
//! (`BlobOp`: `logical_key → digest`) as a normal dataset on the generic engine.
//! `get_blob` = latest registry op (the pointer) → ranged/whole read of the
//! content-addressed bytes with replica fallback.
//!
//! Registry fixed shape: **sort key** = `op_seq`; **partition** + **index dim**
//! = `logical_key`. Access paths: **put** (content-addressed), **get-by-key**,
//! **prefix-list**.
//!
//! Content address: a 128-bit digest built from `twox-hash` (already a
//! dependency) rendered as 32 hex chars — enough for dedup + addressing in the
//! kind/local shadow tier. A cryptographic (sha256) digest is a drop-in swap for
//! prod and does not change this shape.

use std::collections::BTreeMap;
use std::hash::Hasher;
use std::sync::Arc;

use ehdb_core::{EhdbError, Result};
use serde::{Deserialize, Serialize};
use twox_hash::XxHash64;

use crate::dataset::{shard_for_execution, Dataset};
use crate::engine::{L0Config, L0Engine, ReplicaTarget};
use crate::substrate::DurableSubstrate;

/// The D5 dataset id (the registry op-log).
pub const DATASET_D5_BLOB: &str = "d5_blob";

/// One registry op mapping a logical key to a content-addressed blob (the D5
/// record schema — the pointer catalog, NOT the bytes).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BlobOp {
    /// Op-log position (the fixed sort key).
    pub op_seq: u64,
    /// The logical key (partition + index dim).
    pub logical_key: String,
    /// The content digest of the blob bytes (the substrate object address).
    pub digest: String,
    /// The blob size in bytes.
    pub size: u64,
    /// Tombstone: `true` unmaps the logical key (the content-addressed bytes are
    /// left for GC; other keys may still reference the digest).
    pub deleted: bool,
}

/// **D5 blob-registry dataset.** Sort key `op_seq`; partition + index dim
/// `logical_key`.
#[derive(Debug, Clone, Copy)]
pub struct BlobRegistry;

impl Dataset for BlobRegistry {
    type Record = BlobOp;
    const NAME: &'static str = DATASET_D5_BLOB;

    fn sort_key(r: &BlobOp) -> u64 {
        r.op_seq
    }
    fn partition(r: &BlobOp, shard_count: u32) -> u32 {
        shard_for_execution(&r.logical_key, shard_count)
    }
    fn index_key(r: &BlobOp) -> &str {
        &r.logical_key
    }
    fn read_partition(logical_key: &str, shard_count: u32) -> u32 {
        shard_for_execution(logical_key, shard_count)
    }
}

/// The 128-bit content digest of `bytes` (32 hex chars) — two seeded `XxHash64`
/// halves. Dedup + addressing for the kind/local shadow tier; a cryptographic
/// digest is a prod drop-in.
pub fn content_digest(bytes: &[u8]) -> String {
    let mut h1 = XxHash64::with_seed(0);
    h1.write(bytes);
    let mut h2 = XxHash64::with_seed(0x9E37_79B9_7F4A_7C15);
    h2.write(bytes);
    format!("{:016x}{:016x}", h1.finish(), h2.finish())
}

fn object_key(digest: &str) -> String {
    format!("objects/{DATASET_D5_BLOB}/{digest}")
}

/// **The D5 blob store** — content-addressed bytes on the substrate + a registry
/// op-log on the generic engine.
pub struct BlobStore {
    engine: L0Engine<BlobRegistry>,
    /// Where the content-addressed blob bytes live (the same replicas as the
    /// registry, so a blob is as durable as its pointer).
    blob_replicas: Vec<ReplicaTarget>,
}

impl BlobStore {
    /// Config default for D5 rooted at `local_root`.
    pub fn config(local_root: impl Into<std::path::PathBuf>) -> L0Config {
        L0Config::for_dataset(DATASET_D5_BLOB, local_root)
    }

    /// Open a single-replica blob store.
    pub fn open(config: L0Config, substrate: Arc<dyn DurableSubstrate>) -> Result<Self> {
        Self::open_replicated(config, vec![ReplicaTarget::new("replica-0", substrate)])
    }
    /// Open an N-way replicated blob store (bytes + registry both N-way).
    pub fn open_replicated(config: L0Config, replicas: Vec<ReplicaTarget>) -> Result<Self> {
        Ok(Self {
            blob_replicas: replicas.clone(),
            engine: L0Engine::open_replicated(config, replicas)?,
        })
    }
    /// Cold-load a single-replica blob store.
    pub fn cold_load(config: L0Config, substrate: Arc<dyn DurableSubstrate>) -> Result<Self> {
        Self::cold_load_replicated(config, vec![ReplicaTarget::new("replica-0", substrate)])
    }
    /// Cold-load an N-way replicated blob store.
    pub fn cold_load_replicated(config: L0Config, replicas: Vec<ReplicaTarget>) -> Result<Self> {
        Ok(Self {
            blob_replicas: replicas.clone(),
            engine: L0Engine::cold_load_replicated(config, replicas)?,
        })
    }

    /// **put** — content-address `bytes`, write them once to every replica
    /// (deduped), and append a registry op mapping `logical_key → digest`.
    /// Returns the content digest.
    pub fn put(&mut self, logical_key: &str, bytes: &[u8]) -> Result<String> {
        let digest = content_digest(bytes);
        let key = object_key(&digest);
        let mut wrote_any = false;
        for target in &self.blob_replicas {
            // Immutable + content-addressed → put_if_absent dedups.
            if target.substrate.put_if_absent(&key, bytes).is_ok() {
                wrote_any = true;
            }
        }
        if !wrote_any {
            return Err(EhdbError::Storage(
                "blob put: failed to write bytes to any replica".into(),
            ));
        }
        let op_seq = self.engine.global_sequence() + 1;
        self.engine.append_record(BlobOp {
            op_seq,
            logical_key: logical_key.to_string(),
            digest: digest.clone(),
            size: bytes.len() as u64,
            deleted: false,
        })?;
        Ok(digest)
    }

    /// **get-by-key** — the current blob bytes for `logical_key`, or `None` if
    /// unset/deleted. Resolves the latest registry op → digest → reads the
    /// content-addressed bytes with replica fallback.
    pub fn get(&self, logical_key: &str) -> Result<Option<Vec<u8>>> {
        let ops = self.engine.read_index_after(logical_key, 0)?;
        let Some(op) = ops.into_iter().next_back() else {
            return Ok(None);
        };
        if op.deleted {
            return Ok(None);
        }
        let key = object_key(&op.digest);
        for target in &self.blob_replicas {
            if let Ok(bytes) = target.substrate.get_all(&key) {
                return Ok(Some(bytes));
            }
        }
        Err(EhdbError::Storage(format!(
            "blob get: object {} unreachable on all replicas",
            op.digest
        )))
    }

    /// **prefix-list** — the live logical keys whose key starts with `prefix`,
    /// each with its content digest, in key order.
    pub fn prefix_list(&self, prefix: &str) -> Result<Vec<(String, String)>> {
        let all = self.engine.replay_all()?;
        let mut latest: BTreeMap<String, BlobOp> = BTreeMap::new();
        for op in all {
            latest.insert(op.logical_key.clone(), op);
        }
        Ok(latest
            .into_iter()
            .filter(|(k, op)| k.starts_with(prefix) && !op.deleted)
            .map(|(k, op)| (k, op.digest))
            .collect())
    }

    /// **delete** — unmap `logical_key` (tombstone the registry). Returns `true`
    /// if it was mapped.
    pub fn delete(&mut self, logical_key: &str) -> Result<bool> {
        let ops = self.engine.read_index_after(logical_key, 0)?;
        match ops.into_iter().next_back() {
            Some(op) if !op.deleted => {
                let op_seq = self.engine.global_sequence() + 1;
                self.engine.append_record(BlobOp {
                    op_seq,
                    logical_key: logical_key.to_string(),
                    digest: op.digest,
                    size: 0,
                    deleted: true,
                })?;
                Ok(true)
            }
            _ => Ok(false),
        }
    }

    /// Flush all sealed registry parts to the durable replicas.
    pub fn flush_and_wait(&mut self) -> Result<()> {
        self.engine.flush_and_wait_uploads()
    }
    /// Run background merge/compaction over the registry log.
    pub fn run_pending_merges(&mut self) -> Result<usize> {
        self.engine.run_pending_merges()
    }
    /// The underlying generic engine.
    pub fn engine(&self) -> &L0Engine<BlobRegistry> {
        &self.engine
    }
}
