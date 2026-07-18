//! # EHDB **L0** — replicated object-store layer (foundation of the layered platform)
//!
//! L0 is the bottom layer of the EHDB layered platform
//! ([`docs/rfc/ehdb-layered-platform.md`](https://github.com/noetl/ai-meta/blob/main/docs/rfc/ehdb-layered-platform.md)):
//! **L0 object store → L1 streaming/NATS-takeover → L2 KV → L3 fixed reads.**
//! It is the one place durability, replication, retention, compaction, and
//! indexing live; every layer above is a *view* over L0 that can be rebuilt by
//! re-reading L0.
//!
//! ## ⛔ PROGRAM INVARIANT — EHDB is NOT a general-purpose database
//!
//! EHDB is a **noetl-internal store over a FIXED set of predefined datasets**
//! (RFC §0.1: D1 event-log … D10 provider-facts). L0 is built to that:
//!
//! - **Fixed, compiled-in schemas + a fixed sort key + fixed access paths per
//!   dataset.** No arbitrary user schemas, no DDL, no cost-based query planner,
//!   no secondary indexing on arbitrary columns.
//! - **Business/domain data and secret values are NEVER in EHDB.** L0 holds only
//!   noetl-internal platform data; metrics are secret-free.
//! - The invariant is a **scope guardrail that SHRINKS the design** (RFC §2.6).
//!   When a module grows toward generality, that is the smell the invariant is
//!   being violated.
//!
//! ## What this crate implements (L0.1, the first build slice)
//!
//! The **hot-local / durable-async composite** (RFC §2.3) for **dataset D1, the
//! event log**, on the VictoriaMetrics/VictoriaLogs write-engine model plus a
//! ClickHouse-MergeTree-style meta-catalog (RFC §2.5):
//!
//! **The L0 object store is noetl-native — EHDB implements it itself** (the
//! parts, manifest, sparse index, and N-way replication all live in this crate,
//! extending the #254 native segment store). It writes its own immutable parts
//! onto a pluggable **durable substrate** (a raw byte-sink; the local filesystem
//! now — noetl's own store on disk / a PVC / a block device). It does **not**
//! delegate the object-store logic to MinIO or an external S3 server.
//!
//! ```text
//! append → in-memory buffer / active local part  (hot tier — served immediately, fsync per append)
//!        → sealed immutable part on LOCAL disk    (page-cache-friendly hot tier)
//!        → async replicator writes the sealed part to N durable-SUBSTRATE replicas (durable / replicated tier)
//! read   → merge across { active part, local sealed parts, substrate-replica parts }, pruned by the manifest
//! ```
//!
//! - [`substrate`] — the pluggable [`substrate::DurableSubstrate`] trait (the
//!   raw durable byte-sink UNDER noetl's object-store logic) with a
//!   local-filesystem impl ([`substrate::LocalFsSubstrate`], noetl's own on-disk
//!   store) for kind/dev and an instrumenting [`substrate::CountingSubstrate`]
//!   wrapper that records per-key I/O (the "zero-I/O on pruned parts" proof) and
//!   can inject latency (the "hot path never blocks on the substrate" proof). A
//!   raw cloud block/blob byte-sink could slot in UNDER this trait later
//!   (RFC §6.2); noetl's object-store logic above it is unchanged.
//! - [`frame`] — the immutable-part record codec, byte-identical to the #254
//!   durable-segment frame (`magic(4) + body_len(4) + crc32(4) + body`), so an
//!   L0 part is a #254 segment the meta-catalog can prune and range-read.
//! - [`catalog`] — the ClickHouse-style meta-catalog: a [`catalog::Manifest`]
//!   (one [`catalog::PartMeta`] row per immutable part) + a per-part
//!   [`catalog::SparseIndex`] (granule → byte offset) + per-part min/max sort-key
//!   pruning. Fixed, compiled-in for D1 (sort key = `global_sequence`, partition
//!   = `shard_for(execution_id)`); NOT a general IndexDB (RFC §2.6).
//! - [`engine`] — [`engine::L0EventLogEngine`]: the append path (hot local part,
//!   never blocks on the substrate), the background async replicator, the
//!   pruned/ranged read path, and **cold-load** (a fresh node with no local data
//!   reproduces the exact record set + global sequence from the durable substrate —
//!   the fungible-writer property that retires the per-shard-Raft "T-RF" plan,
//!   RFC §2.7).
//! - [`metrics`] — secret-free counters (appends, seals, uploads, upload lag,
//!   range gets/bytes, cold-loads) for the L0.1 instrumentation exit criterion.
//!
//! ## Durability-window posture (RFC §2.3 / §6.1 decision)
//!
//! D1 is the source-of-truth event log, so L0 defaults to **posture A —
//! fsync-per-append to the local part** ([`engine::FlushPolicy::EveryAppend`]):
//! the local part is durable before the append returns; the substrate replication
//! adds N-way durability asynchronously. This reuses #254's fsync-per-append
//! strength. Posture B (VM-style buffered flush, larger crash window —
//! [`engine::FlushPolicy::Buffered`]) is offered for derived/metrics tiers only,
//! never for the event log.
//!
//! ## Replication is noetl-native — N-way copy, no consensus (RFC §2.7)
//!
//! Replication is noetl's own, and it stays simple **because parts are
//! immutable**: an immutable part is written **write-once to N durable-substrate
//! replicas** and the [`catalog::PartMeta::replicas`] list records where each
//! copy lives. Immutable objects never conflict, so N-way copy needs **no
//! consensus / no Raft** — the HDFS / block-replication model, not a replicated
//! log. The manifest is the replica-location catalog. **L0.1 writes a single
//! replica** and designs the seam in (the `replicas` list + a per-part write
//! loop); N-way copy is the additive later step that appends more replica
//! entries. This is what lets a writer be fungible: on writer death another node
//! cold-loads the sealed parts from a surviving replica and resumes — retiring
//! the per-shard-Raft "T-RF" plan.
//!
//! ## Reuse of #254 (durable segments) and Phase-8 (object tier)
//!
//! - The **immutable-part frame format** is the #254 segment frame verbatim
//!   ([`frame`] carries a byte-identical-to-`durable_eventlog.rs` test).
//! - The **content-addressed durability seam** already exists
//!   (`durable_eventlog_shared.rs` ships sealed segment bytes to a shared store
//!   and cold-loads them); L0 generalizes it into the manifest-driven
//!   part-uploader + read-merge here.
//! - Where a dataset maps onto an existing tier (D5 blobs → Phase-8
//!   `ObjectBlobDriver`), L0 wires the tier rather than reinventing it (RFC §2.4).
//!
//! ## Slices implemented here (D1 only; all additive, kind/local shadow)
//!
//! - **L0.1** — part model + meta-catalog + hot-local/durable-async tiering +
//!   cold-load ([`engine`], [`catalog`], [`part`], [`substrate`]).
//! - **L0.2** — the fixed per-dataset inverted index: per-part + per-granule
//!   [`bloom`] over `execution_id`, with index-first pruning in the read path.
//! - **L0.3** — the background small→big [`merge`] engine (contiguous-run
//!   compaction, rebuilt sparse index + blooms, atomic manifest swap).
//! - **L0.4** — the [`columnar`] per-field codec for the event tier
//!   (VictoriaLogs-style per-field columns, the big payload field isolated,
//!   single-column projection). Provided as a codec, additive to the row-frame
//!   part format; wiring it in as the event-tier encoding is the follow-on.
//! - **L0.5** — [`retention`] as drop-partition + orphan reclaim/GC (vacuums the
//!   superseded merge sources + dropped parts).
//!
//! - **L0.6** — N-way replication of immutable parts (`ReplicaTarget`s +
//!   [`catalog::ReplicaLocation`]); write-once copy to N replicas, read fallback,
//!   no consensus.
//! - **L0.7** — the engine is **dataset-generic**: [`dataset::Dataset`] captures
//!   a dataset's fixed record schema + sort key + partition + index dimension,
//!   and [`engine::L0Engine`]`<D>` runs the shared part/catalog/merge/replication
//!   engine over any of them. [`engine::L0EventLogEngine`] is the D1 alias.
//! - **D2** — the [`command_queue`] (`noetl.command`): a second dataset on the
//!   generic engine. The mutable queue is modeled as an **append-only op log
//!   over immutable parts**; claim-state is a **fold/projection**
//!   ([`command_queue::CommandQueue`]: enqueue / claim-by-id / unclaimed-scan).
//!
//! Still OUT (later work): the D3…D10 dataset impls; wiring the columnar codec in
//! as the on-disk part encoding + the Phase-8 blob shape (D5); and **all** of
//! L1/L2/L3. This crate touches no NATS, cuts nothing over, and is kind/local
//! shadow only.

pub mod bloom;
pub mod catalog;
pub mod columnar;
pub mod command_queue;
pub mod dataset;
pub mod engine;
pub mod frame;
pub mod kv;
pub mod merge;
pub mod metrics;
pub mod part;
pub mod projection;
pub mod retention;
pub mod substrate;

pub use bloom::Bloom;
pub use catalog::{Manifest, PartMeta, ReplicaLocation, SparseIndex};
pub use columnar::{decode_columnar, encode_columnar, project_column, Column, Field};
pub use command_queue::{
    CommandOpKind, CommandOpRecord, CommandQueue, CommandState, D2CommandQueue,
    DATASET_D2_COMMAND_QUEUE,
};
pub use dataset::{
    shard_for_execution, D1EventLog, Dataset, EventRecord, DATASET_D1_EVENT_LOG,
    DEFAULT_SHARD_COUNT,
};
pub use engine::{L0Config, L0Engine, L0EventLogEngine, ReplicaTarget};
pub use kv::{KvDataset, KvEntry, KvOp, KvStore, DATASET_D4_KV};
pub use merge::{MergePlan, MergePolicy};
pub use metrics::{L0Metrics, L0MetricsSnapshot};
pub use part::{build_merged_part, FlushPolicy, PartWriter, SealedPart};
pub use projection::{
    ExecutionProjection, ExecutionState, ProjectionOp, ProjectionStore, DATASET_D3_PROJECTION,
};
pub use retention::{plan_keep_last, plan_retention, RetentionPlan};
pub use substrate::{CountingSubstrate, DurableSubstrate, LocalFsSubstrate};
