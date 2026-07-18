//! **D6 — vector / RAG** (platform docs, chunks, embeddings), RFC §0.1.
//!
//! Fixed access: **upsert**, **top-k cosine** (within a collection), **delete**.
//! Over immutable parts: an append-only log of `VectorOp` (upsert / delete);
//! a point's current embedding is the latest op for it, and a collection's live
//! point set is the fold of its ops (last-wins per `point_id`, tombstones
//! dropped). `top_k` folds a collection (an index-pruned read on `collection`)
//! and ranks by cosine similarity.
//!
//! Fixed shape: **sort key** = `op_seq`; **partition** + **index dim** =
//! `collection` (so a top-k reads exactly one collection's ops).

use std::collections::BTreeMap;
use std::sync::Arc;

use ehdb_core::Result;
use serde::{Deserialize, Serialize};

use crate::dataset::{shard_for_execution, Dataset};
use crate::engine::{L0Config, L0Engine, ReplicaTarget};
use crate::substrate::DurableSubstrate;

/// The D6 dataset id.
pub const DATASET_D6_VECTOR: &str = "d6_vector";

/// One vector upsert/delete in the op log (the D6 record schema).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VectorOp {
    /// Op-log position (the fixed sort key).
    pub op_seq: u64,
    /// The collection (partition + index dim).
    pub collection: String,
    /// The point id within the collection.
    pub point_id: String,
    /// The embedding (empty on `deleted`).
    pub embedding: Vec<f32>,
    /// Tombstone: `true` removes the point.
    pub deleted: bool,
}

/// **D6 vector dataset.** Sort key `op_seq`; partition + index dim `collection`.
#[derive(Debug, Clone, Copy)]
pub struct VectorDataset;

impl Dataset for VectorDataset {
    type Record = VectorOp;
    const NAME: &'static str = DATASET_D6_VECTOR;

    fn sort_key(r: &VectorOp) -> u64 {
        r.op_seq
    }
    fn partition(r: &VectorOp, shard_count: u32) -> u32 {
        shard_for_execution(&r.collection, shard_count)
    }
    fn index_key(r: &VectorOp) -> &str {
        &r.collection
    }
    fn read_partition(collection: &str, shard_count: u32) -> u32 {
        shard_for_execution(collection, shard_count)
    }
}

/// One cosine-ranked hit.
#[derive(Debug, Clone, PartialEq)]
pub struct VectorHit {
    pub point_id: String,
    pub score: f32,
    pub embedding: Vec<f32>,
}

/// Cosine similarity of two equal-length vectors (0 if either is zero-norm or
/// lengths differ).
fn cosine(a: &[f32], b: &[f32]) -> f32 {
    if a.len() != b.len() {
        return 0.0;
    }
    let (mut dot, mut na, mut nb) = (0.0f32, 0.0f32, 0.0f32);
    for (x, y) in a.iter().zip(b.iter()) {
        dot += x * y;
        na += x * x;
        nb += y * y;
    }
    if na == 0.0 || nb == 0.0 {
        return 0.0;
    }
    dot / (na.sqrt() * nb.sqrt())
}

/// **The D6 vector store** — upsert / top-k cosine / delete over the generic
/// engine.
pub struct VectorStore {
    engine: L0Engine<VectorDataset>,
}

impl VectorStore {
    /// Config default for D6 rooted at `local_root`.
    pub fn config(local_root: impl Into<std::path::PathBuf>) -> L0Config {
        L0Config::for_dataset(DATASET_D6_VECTOR, local_root)
    }

    /// Open a single-replica vector store.
    pub fn open(config: L0Config, substrate: Arc<dyn DurableSubstrate>) -> Result<Self> {
        Ok(Self {
            engine: L0Engine::open(config, substrate)?,
        })
    }
    /// Open an N-way replicated vector store.
    pub fn open_replicated(config: L0Config, replicas: Vec<ReplicaTarget>) -> Result<Self> {
        Ok(Self {
            engine: L0Engine::open_replicated(config, replicas)?,
        })
    }
    /// Cold-load a single-replica vector store.
    pub fn cold_load(config: L0Config, substrate: Arc<dyn DurableSubstrate>) -> Result<Self> {
        Ok(Self {
            engine: L0Engine::cold_load(config, substrate)?,
        })
    }
    /// Cold-load an N-way replicated vector store.
    pub fn cold_load_replicated(config: L0Config, replicas: Vec<ReplicaTarget>) -> Result<Self> {
        Ok(Self {
            engine: L0Engine::cold_load_replicated(config, replicas)?,
        })
    }

    /// **upsert** a point's embedding into a collection.
    pub fn upsert(&mut self, collection: &str, point_id: &str, embedding: Vec<f32>) -> Result<u64> {
        let op_seq = self.engine.global_sequence() + 1;
        self.engine.append_record(VectorOp {
            op_seq,
            collection: collection.to_string(),
            point_id: point_id.to_string(),
            embedding,
            deleted: false,
        })
    }

    /// **delete** a point from a collection.
    pub fn delete(&mut self, collection: &str, point_id: &str) -> Result<u64> {
        let op_seq = self.engine.global_sequence() + 1;
        self.engine.append_record(VectorOp {
            op_seq,
            collection: collection.to_string(),
            point_id: point_id.to_string(),
            embedding: Vec::new(),
            deleted: true,
        })
    }

    /// The live points of a collection (latest op per `point_id`, tombstones
    /// dropped), in point-id order.
    fn live_points(&self, collection: &str) -> Result<Vec<VectorOp>> {
        let ops = self.engine.read_index_after(collection, 0)?; // sorted by op_seq
        let mut latest: BTreeMap<String, VectorOp> = BTreeMap::new();
        for op in ops {
            latest.insert(op.point_id.clone(), op);
        }
        Ok(latest.into_values().filter(|op| !op.deleted).collect())
    }

    /// **top-k cosine** — the `k` live points in `collection` most similar to
    /// `query`, highest score first.
    pub fn top_k(&self, collection: &str, query: &[f32], k: usize) -> Result<Vec<VectorHit>> {
        let mut hits: Vec<VectorHit> = self
            .live_points(collection)?
            .into_iter()
            .map(|op| VectorHit {
                score: cosine(query, &op.embedding),
                point_id: op.point_id,
                embedding: op.embedding,
            })
            .collect();
        hits.sort_by(|a, b| {
            b.score
                .partial_cmp(&a.score)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        hits.truncate(k);
        Ok(hits)
    }

    /// The current embedding of one point, or `None` if unset/deleted.
    pub fn get_point(&self, collection: &str, point_id: &str) -> Result<Option<Vec<f32>>> {
        Ok(self
            .live_points(collection)?
            .into_iter()
            .find(|op| op.point_id == point_id)
            .map(|op| op.embedding))
    }

    /// Flush all sealed parts to the durable replicas.
    pub fn flush_and_wait(&mut self) -> Result<()> {
        self.engine.flush_and_wait_uploads()
    }
    /// Run background merge/compaction over the vector log.
    pub fn run_pending_merges(&mut self) -> Result<usize> {
        self.engine.run_pending_merges()
    }
    /// The underlying generic engine.
    pub fn engine(&self) -> &L0Engine<VectorDataset> {
        &self.engine
    }
}
