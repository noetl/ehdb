//! EHDB vector core engine (completion program Phase 8, slice 3).
//!
//! This is the durable vector engine that Phase 8 puts *underneath* NoETL's
//! internal **platform vector** tier — the RAG / retrieval embeddings the worker
//! already ingests + queries in-process via the Phase-E retrieval path
//! ([`crate::retrieve_local_reference_context`] over
//! [`ehdb_retrieval::InMemoryRetrievalCatalog`]).  The concrete internal
//! collections it serves are the **platform RAG collections**: playbook /
//! runtime-surface documents, chunks, and their embeddings, plus catalog
//! embeddings — scoped by tenant / namespace / model like the RAG helper.
//!
//! Tenant/domain business vector collections reached by playbook connectors
//! (e.g. a customer's own Qdrant) stay **external** and never move — that is the
//! hard boundary the RFC draws.  This engine holds **platform vectors only**.
//!
//! Unlike the KV / object slices, the worker has **no external Qdrant client**:
//! platform retrieval already runs in-process (Phase E).  So this slice is largely
//! a **formalization** — it lifts the Phase-E ad-hoc retrieval into a first-class
//! [`VectorDriver`] with the same driver + disabled-by-default shadow shape as the
//! other Phase-8 tiers, so the vector tier is driver-selectable (Phase 10) and the
//! cutover to serving from EHDB stays a later, separately-gated step (Phase 9).
//!
//! ## Boundary — this is the vector index engine, NOT an event author
//!
//! A vector point is a derived platform index entry, not an event.  This engine
//! never authors a `noetl.event`; it persists + serves content-derivable
//! embeddings only.  It is a platform engine for platform vectors only; **business
//! data never flows through it**.
//!
//! ## Semantics preserved from the Qdrant / RAG path
//!
//! * **Upsert** — a point (`collection`, `point_id`, `model_id`, `vector`, optional
//!   `payload`) is one append to a single canonical stream ([`VECTOR_INDEX_STREAM`]),
//!   scoped by a per-point subject `noetl.vec.<hex(collection)>.<hex(point_id)>`.
//!   Re-upserting the same point advances a monotonic per-point version; the latest
//!   record wins.  The collection + point id ride in the record envelope **verbatim**
//!   (hex-encoded into subject tokens only), so ids carrying `.` / `/` round-trip.
//! * **Query (top-k)** — a bounded cosine-similarity search over the collection's
//!   live points, filtered to the query's `model_id` + matching dimensionality, then
//!   ranked descending by score (ties broken by `point_id`) and truncated to `top_k`.
//!   The scoring is the same cosine + descending-rank shape as
//!   [`ehdb_retrieval::InMemoryRetrievalCatalog::search_similar`], so a shadow's
//!   top-k tracks the incumbent retrieval path.
//! * **Delete = tombstone** — a delete appends a tombstone record; a subsequent
//!   query no longer returns the point (idempotent).  Append-only + immutable +
//!   `KeepAll` retention keep the whole write history, so any past index state is a
//!   replay (replay-is-truth).
//!
//! ## Driver interface (Phase 10-ready)
//!
//! The engine is exposed behind [`VectorDriver`] so the vector tier is
//! driver-selectable: the EHDB engine here is [`LocalReferenceVectorDriver`]; a
//! Qdrant driver implementing the same trait keeps the tier selectable back to the
//! incumbent (Roadmap Phase 10).  Callers program against the trait.
//!
//! ## Shadow validation
//!
//! [`compare_vector_parity`] is the pure, secret-free comparison the worker's
//! disabled-by-default shadow mode uses to prove the EHDB engine tracks the
//! authoritative Qdrant retrieval without serving reads from it: **id-set parity**
//! (same top-k point ids), **rank-order parity** (same ordered id sequence), and
//! **score monotonicity** (the EHDB ranking is non-increasing within a tolerance,
//! since float scores differ across engines), with a single divergence reason when
//! they differ.

use std::collections::BTreeMap;
use std::path::PathBuf;

use ehdb_core::{EhdbError, NamespaceName, Result, StreamName, TenantId, TransactionId};
use ehdb_stream::{RetentionPolicy, StreamRecord, Subject, SubjectFilter};
use ehdb_transaction::{CommitTransaction, Mutation, StreamMutation};
use serde::{Deserialize, Serialize};

use crate::LocalReferenceRuntime;

/// The single canonical stream that carries every vector upsert / tombstone (the
/// point → embedding index).  One stream keeps its
/// [`ehdb_stream::StreamSequence`] the global write-order sequence for the whole
/// vector tier, so replay is deterministic.
pub const VECTOR_INDEX_STREAM: &str = "noetl_vector_index";

/// Subject prefix scoping an upsert to its `(collection, point)`.  A record's
/// subject is `noetl.vec.<hex(collection)>.<hex(point_id)>`, so a per-point read
/// is an exact subject-filtered replay and a collection query is a
/// `noetl.vec.<hex(collection)>.>` replay folded + filtered.  Both the collection
/// and the point id are hex-encoded into single subject tokens because ids carry
/// `.` / `/` which are not valid inside one subject token.
pub const VECTOR_SUBJECT_PREFIX: &str = "noetl.vec";

/// Hard ceiling on the dimensionality of one stored embedding (bounded like the
/// rest of the integration).  An over-cap vector is an [`EhdbError::InvalidState`]
/// whose message carries `exceeds bound`, so a caller mistake classifies as
/// *rejected*.
pub const MAX_VECTOR_DIMENSIONS: usize = 4_096;

/// Hard ceiling on a single query's requested `top_k`.  Over-cap ⇒ *rejected*
/// (matches the RAG retrieval top-k ceiling).
pub const MAX_VECTOR_QUERY_TOP_K: usize = 64;

/// Hard ceiling on a point's optional payload (secret-free metadata like a source
/// URI or chunk ordinal).  Over-cap ⇒ *rejected*.
pub const MAX_VECTOR_PAYLOAD_BYTES: usize = 16 * 1024;

fn hex_encode(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for &b in bytes {
        out.push(HEX[(b >> 4) as usize] as char);
        out.push(HEX[(b & 0x0f) as usize] as char);
    }
    out
}

/// Build the exact per-point subject `noetl.vec.<hex(collection)>.<hex(point_id)>`.
fn point_subject(collection: &str, point_id: &str) -> Result<Subject> {
    if collection.is_empty() {
        return Err(EhdbError::InvalidIdentifier(
            "vector collection: empty".to_string(),
        ));
    }
    if point_id.is_empty() {
        return Err(EhdbError::InvalidIdentifier(
            "vector point id: empty".to_string(),
        ));
    }
    let col = hex_encode(collection.as_bytes());
    let point = hex_encode(point_id.as_bytes());
    Subject::new(format!("{VECTOR_SUBJECT_PREFIX}.{col}.{point}"))
}

/// Build the collection-scoped query filter `noetl.vec.<hex(collection)>.>`.
fn collection_filter(collection: &str) -> Result<SubjectFilter> {
    if collection.is_empty() {
        return Err(EhdbError::InvalidIdentifier(
            "vector collection: empty".to_string(),
        ));
    }
    let col = hex_encode(collection.as_bytes());
    SubjectFilter::new(format!("{VECTOR_SUBJECT_PREFIX}.{col}.>"))
}

/// The stored envelope for one vector upsert / tombstone (the record payload).
/// Carries the original (un-encoded) collection + point id so a query reconstructs
/// real ids without decoding the subject, plus the model id, the embedding, the
/// optional payload, the monotonic per-point version, and the tombstone flag.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct VectorEnvelope {
    collection: String,
    point_id: String,
    model_id: String,
    /// The embedding.  Empty on a tombstone.
    vector: Vec<f32>,
    /// Optional secret-free metadata that rode with the point (source uri, ordinal).
    payload: Option<String>,
    version: u64,
    deleted: bool,
}

/// Upsert one point (embedding + optional payload) into a platform collection.
#[derive(Debug, Clone, PartialEq)]
pub struct VectorUpsertRequest {
    /// The platform collection (e.g. `playbook-surface`, `catalog-embeddings`).
    pub collection: String,
    /// The point identifier (Qdrant point id — preserved verbatim).
    pub point_id: String,
    /// The embedding model id; a query only ranks points of its own model.
    pub model_id: String,
    /// The embedding vector.  Over-cap dimensionality ⇒ rejected; empty / non-finite
    /// / zero-norm ⇒ invalid.
    pub vector: Vec<f32>,
    /// Optional secret-free payload metadata (source uri, ordinal, checksum).
    pub payload: Option<String>,
    pub transaction_id: String,
}

/// Secret-free result of an upsert (no collection / point id / vector / payload
/// ever reaches a metric label).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VectorUpsertOutcome {
    pub action: String,
    pub collection: String,
    pub point_id: String,
    pub dimensions: u32,
    /// The per-point index version after this write (== previous version + 1).
    pub version: u64,
    /// Whether an index record was appended (always true on a successful upsert).
    pub written: bool,
    /// Whether the canonical index stream was created on this write.
    pub created_stream: bool,
    /// The global write-order sequence assigned to this upsert.
    pub global_sequence: u64,
}

/// Bounded cosine top-k query over a collection's live points.
#[derive(Debug, Clone, PartialEq)]
pub struct VectorQueryRequest {
    pub collection: String,
    /// Only points of this model are candidates.
    pub model_id: String,
    /// The query embedding.  Points of a different dimensionality are skipped.
    pub query: Vec<f32>,
    /// How many top hits to return (over [`MAX_VECTOR_QUERY_TOP_K`] ⇒ rejected).
    pub top_k: usize,
}

/// One query hit — the point id and its cosine score (secret-free: an id + a float,
/// never the vector or payload).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VectorHit {
    pub point_id: String,
    pub score: f32,
}

/// Secret-free result of a top-k query.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VectorQueryOutcome {
    pub action: String,
    pub collection: String,
    /// Whether the index stream exists yet (false before the first write anywhere).
    pub exists: bool,
    /// Live candidate points (same model + dimensionality) before `top_k`.
    pub candidate_count: usize,
    pub returned: usize,
    /// Whether `top_k` truncated the candidate set.
    pub truncated_by_top_k: bool,
    /// Hits ordered by score descending (ties broken by point id).
    pub hits: Vec<VectorHit>,
}

/// Delete a point (append a tombstone).  Idempotent — deleting an absent point is a
/// no-op.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VectorDeleteRequest {
    pub collection: String,
    pub point_id: String,
    pub transaction_id: String,
}

/// Secret-free result of a delete.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VectorDeleteOutcome {
    pub action: String,
    pub collection: String,
    pub point_id: String,
    /// Whether a live point existed before this delete (false = idempotent no-op).
    pub existed: bool,
    /// The tombstone's version (== previous version + 1; 0 when nothing existed).
    pub version: u64,
    /// The global sequence assigned to the tombstone (0 on an idempotent no-op).
    pub global_sequence: u64,
}

/// The driver interface for the vector tier.  EHDB is one implementation
/// ([`LocalReferenceVectorDriver`]); a Qdrant driver implementing the same trait
/// keeps the tier selectable back to the incumbent (Phase 10).
///
/// All methods are `&self`: the durable state lives in the on-disk index log,
/// opened + dropped per op (bounded/stateless discipline).
pub trait VectorDriver {
    /// A stable, secret-free identifier for the backing engine.
    fn driver_name(&self) -> &'static str;
    /// Upsert one point (embedding + optional payload) into a collection.
    fn upsert(&self, request: &VectorUpsertRequest) -> Result<VectorUpsertOutcome>;
    /// Bounded cosine top-k query over a collection's live points.
    fn query(&self, request: &VectorQueryRequest) -> Result<VectorQueryOutcome>;
    /// Delete a point (tombstone; idempotent).
    fn delete(&self, request: &VectorDeleteRequest) -> Result<VectorDeleteOutcome>;
}

/// The EHDB vector engine over the bounded local-reference transaction log (the
/// point → embedding index).
#[derive(Debug, Clone)]
pub struct LocalReferenceVectorDriver {
    /// The index transaction log path (point → embedding records).
    pub log_path: PathBuf,
    pub tenant: String,
    pub namespace: String,
}

impl LocalReferenceVectorDriver {
    pub fn new(
        log_path: impl Into<PathBuf>,
        tenant: impl Into<String>,
        namespace: impl Into<String>,
    ) -> Self {
        Self {
            log_path: log_path.into(),
            tenant: tenant.into(),
            namespace: namespace.into(),
        }
    }

    fn coordinates(&self) -> Result<(TenantId, NamespaceName, StreamName)> {
        Ok((
            TenantId::new(self.tenant.clone())?,
            NamespaceName::new(self.namespace.clone())?,
            StreamName::new(VECTOR_INDEX_STREAM.to_string())?,
        ))
    }

    /// The latest record's envelope for one point (tombstone or live), or `None`
    /// when the point was never written / the stream does not exist yet.
    fn latest_envelope(
        &self,
        runtime: &LocalReferenceRuntime,
        tenant: &TenantId,
        namespace: &NamespaceName,
        stream: &StreamName,
        subject: &Subject,
    ) -> Option<VectorEnvelope> {
        let filter = SubjectFilter::new(subject.as_str().to_string()).ok()?;
        let records = runtime
            .state()
            .streams
            .replay_matching(tenant, namespace, stream, &filter, None)
            .ok()?;
        records.last().and_then(decode_envelope)
    }
}

impl VectorDriver for LocalReferenceVectorDriver {
    fn driver_name(&self) -> &'static str {
        "ehdb-local-reference"
    }

    fn upsert(&self, request: &VectorUpsertRequest) -> Result<VectorUpsertOutcome> {
        let (tenant, namespace, stream) = self.coordinates()?;
        let subject = point_subject(&request.collection, &request.point_id)?;

        if request.vector.len() > MAX_VECTOR_DIMENSIONS {
            return Err(EhdbError::InvalidState(format!(
                "vector {} dimensions exceeds bound {MAX_VECTOR_DIMENSIONS}",
                request.vector.len()
            )));
        }
        validate_vector("embedding vector", &request.vector)?;
        if let Some(payload) = &request.payload {
            if payload.len() > MAX_VECTOR_PAYLOAD_BYTES {
                return Err(EhdbError::InvalidState(format!(
                    "vector payload {} bytes exceeds bound {MAX_VECTOR_PAYLOAD_BYTES}",
                    payload.len()
                )));
            }
        }
        if request.model_id.trim().is_empty() {
            return Err(EhdbError::InvalidIdentifier(
                "vector model id: empty".to_string(),
            ));
        }
        let transaction_id = TransactionId::new(request.transaction_id.clone())?;

        let mut runtime = LocalReferenceRuntime::open(&self.log_path)?;
        let latest = self.latest_envelope(&runtime, &tenant, &namespace, &stream, &subject);
        // Monotonic per-point version — advances across tombstones too.
        let next_version = latest.as_ref().map(|env| env.version).unwrap_or(0) + 1;

        let (created_stream, next_sequence) =
            next_stream_write(&runtime, &tenant, &namespace, &stream);

        let dimensions = request.vector.len() as u32;
        let envelope = VectorEnvelope {
            collection: request.collection.clone(),
            point_id: request.point_id.clone(),
            model_id: request.model_id.clone(),
            vector: request.vector.clone(),
            payload: request.payload.clone(),
            version: next_version,
            deleted: false,
        };
        append_envelope(
            &mut runtime,
            &transaction_id,
            &tenant,
            &namespace,
            &stream,
            &subject,
            &envelope,
            created_stream,
            next_sequence,
        )?;

        Ok(VectorUpsertOutcome {
            action: "vector-upsert".to_string(),
            collection: request.collection.clone(),
            point_id: request.point_id.clone(),
            dimensions,
            version: next_version,
            written: true,
            created_stream,
            global_sequence: next_sequence,
        })
    }

    fn query(&self, request: &VectorQueryRequest) -> Result<VectorQueryOutcome> {
        let (tenant, namespace, stream) = self.coordinates()?;
        let filter = collection_filter(&request.collection)?;

        if request.top_k == 0 {
            return Err(EhdbError::InvalidState(
                "vector query top_k must be greater than zero".to_string(),
            ));
        }
        if request.top_k > MAX_VECTOR_QUERY_TOP_K {
            return Err(EhdbError::InvalidState(format!(
                "vector query top_k {} exceeds bound {MAX_VECTOR_QUERY_TOP_K}",
                request.top_k
            )));
        }
        validate_vector("query vector", &request.query)?;

        let runtime = LocalReferenceRuntime::open(&self.log_path)?;
        let records = match runtime
            .state()
            .streams
            .replay_matching(&tenant, &namespace, &stream, &filter, None)
        {
            Ok(records) => records,
            // A missing stream (nothing written anywhere yet) is an absent probe.
            Err(_) => {
                return Ok(VectorQueryOutcome {
                    action: "vector-query".to_string(),
                    collection: request.collection.clone(),
                    exists: false,
                    candidate_count: 0,
                    returned: 0,
                    truncated_by_top_k: false,
                    hits: Vec::new(),
                });
            }
        };

        // Fold to the latest envelope per point (records replay in sequence order,
        // so a later record overwrites an earlier one).
        let mut latest_by_point: BTreeMap<String, VectorEnvelope> = BTreeMap::new();
        for record in records {
            if let Some(env) = decode_envelope(&record) {
                latest_by_point.insert(env.point_id.clone(), env);
            }
        }

        // Candidates: live points of the query's model + matching dimensionality —
        // the same filter shape as `InMemoryRetrievalCatalog::search_similar`.
        let query_norm = vector_norm(&request.query);
        let mut hits: Vec<VectorHit> = latest_by_point
            .into_values()
            .filter(|env| !env.deleted)
            .filter(|env| env.model_id == request.model_id)
            .filter(|env| env.vector.len() == request.query.len())
            .map(|env| VectorHit {
                score: cosine_similarity(&request.query, &env.vector, query_norm),
                point_id: env.point_id,
            })
            .collect();

        // Rank descending by score, ties broken by point id (deterministic).
        hits.sort_by(|left, right| {
            right
                .score
                .total_cmp(&left.score)
                .then_with(|| left.point_id.cmp(&right.point_id))
        });

        let candidate_count = hits.len();
        let truncated_by_top_k = candidate_count > request.top_k;
        hits.truncate(request.top_k);

        Ok(VectorQueryOutcome {
            action: "vector-query".to_string(),
            collection: request.collection.clone(),
            exists: true,
            candidate_count,
            returned: hits.len(),
            truncated_by_top_k,
            hits,
        })
    }

    fn delete(&self, request: &VectorDeleteRequest) -> Result<VectorDeleteOutcome> {
        let (tenant, namespace, stream) = self.coordinates()?;
        let subject = point_subject(&request.collection, &request.point_id)?;
        let transaction_id = TransactionId::new(request.transaction_id.clone())?;

        let mut runtime = LocalReferenceRuntime::open(&self.log_path)?;
        let latest = self.latest_envelope(&runtime, &tenant, &namespace, &stream, &subject);

        // Idempotent: an absent point (never written, or already a tombstone) does
        // not append a second tombstone.
        let Some(current) = latest.as_ref().filter(|env| !env.deleted) else {
            return Ok(VectorDeleteOutcome {
                action: "vector-delete".to_string(),
                collection: request.collection.clone(),
                point_id: request.point_id.clone(),
                existed: false,
                version: 0,
                global_sequence: 0,
            });
        };

        let next_version = current.version + 1;
        let (created_stream, next_sequence) =
            next_stream_write(&runtime, &tenant, &namespace, &stream);
        let envelope = VectorEnvelope {
            collection: request.collection.clone(),
            point_id: request.point_id.clone(),
            model_id: current.model_id.clone(),
            vector: Vec::new(),
            payload: None,
            version: next_version,
            deleted: true,
        };
        append_envelope(
            &mut runtime,
            &transaction_id,
            &tenant,
            &namespace,
            &stream,
            &subject,
            &envelope,
            created_stream,
            next_sequence,
        )?;

        Ok(VectorDeleteOutcome {
            action: "vector-delete".to_string(),
            collection: request.collection.clone(),
            point_id: request.point_id.clone(),
            existed: true,
            version: next_version,
            global_sequence: next_sequence,
        })
    }
}

/// The next (created_stream, sequence) for an index write — matches the
/// event-log / KV / object engines: a missing stream replays as an error (the
/// create signal), and `next = count + 1` keeps the write-order sequence monotonic
/// + gapless.
fn next_stream_write(
    runtime: &LocalReferenceRuntime,
    tenant: &TenantId,
    namespace: &NamespaceName,
    stream: &StreamName,
) -> (bool, u64) {
    match runtime
        .state()
        .streams
        .replay(tenant, namespace, stream, None)
    {
        Ok(records) => (false, records.len() as u64 + 1),
        Err(_) => (true, ehdb_stream::StreamSequence::first().value()),
    }
}

#[allow(clippy::too_many_arguments)]
fn append_envelope(
    runtime: &mut LocalReferenceRuntime,
    transaction_id: &TransactionId,
    tenant: &TenantId,
    namespace: &NamespaceName,
    stream: &StreamName,
    subject: &Subject,
    envelope: &VectorEnvelope,
    created_stream: bool,
    sequence: u64,
) -> Result<()> {
    let payload = serde_json::to_vec(envelope)
        .map_err(|err| EhdbError::InvalidState(format!("vector envelope encode: {err}")))?;

    let mut mutations = Vec::with_capacity(2);
    if created_stream {
        mutations.push(Mutation::Stream(StreamMutation::CreateStream {
            stream: stream.clone(),
            retention: RetentionPolicy::KeepAll,
        }));
    }
    mutations.push(Mutation::Stream(StreamMutation::Publish {
        stream: stream.clone(),
        subject: subject.clone(),
        payload,
        sequence,
    }));

    runtime.append(CommitTransaction {
        transaction_id: transaction_id.clone(),
        tenant: tenant.clone(),
        namespace: namespace.clone(),
        mutations,
    })?;
    Ok(())
}

fn decode_envelope(record: &StreamRecord) -> Option<VectorEnvelope> {
    serde_json::from_slice(&record.payload).ok()
}

/// Validate an embedding / query vector: non-empty, all finite, non-zero norm.
/// Matches [`ehdb_retrieval`]'s embedding validation so the two paths agree.
fn validate_vector(label: &str, vector: &[f32]) -> Result<()> {
    if vector.is_empty() {
        return Err(EhdbError::InvalidState(format!(
            "{label} must not be empty"
        )));
    }
    if vector.iter().any(|value| !value.is_finite()) {
        return Err(EhdbError::InvalidState(format!(
            "{label} must contain only finite values"
        )));
    }
    if vector_norm_squared(vector) == 0.0 {
        return Err(EhdbError::InvalidState(format!(
            "{label} must not be the zero vector"
        )));
    }
    Ok(())
}

fn vector_norm_squared(vector: &[f32]) -> f32 {
    vector.iter().map(|value| value * value).sum()
}

fn vector_norm(vector: &[f32]) -> f32 {
    vector_norm_squared(vector).sqrt()
}

/// Cosine similarity, identical to [`ehdb_retrieval`]'s scoring so an EHDB top-k
/// tracks the incumbent retrieval path exactly.  `left_norm` is precomputed once
/// for the query.
fn cosine_similarity(left: &[f32], right: &[f32], left_norm: f32) -> f32 {
    let dot: f32 = left
        .iter()
        .zip(right.iter())
        .map(|(left, right)| left * right)
        .sum();
    dot / (left_norm * vector_norm(right))
}

/// One authoritative Qdrant top-k hit, for the shadow parity check.
#[derive(Debug, Clone, PartialEq)]
pub struct AuthoritativeVectorHit {
    pub point_id: String,
    pub score: f32,
}

/// The parity verdict of one shadow query: did the EHDB engine's top-k match the
/// authoritative Qdrant top-k?  Pure + secret-free so the engine tests and the
/// worker's disabled-by-default shadow mode share one comparison.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VectorParityReport {
    /// Both engines returned the same **set** of top-k point ids.
    pub ids_ok: bool,
    /// Both engines returned the same **ordered** id sequence (rank-for-rank).
    pub order_ok: bool,
    /// The EHDB ranking is non-increasing within `tolerance` (a valid ranking —
    /// float scores differ across engines so absolute equality is not required).
    pub monotonic_ok: bool,
    /// The single reason parity failed, or `None` when it holds.
    pub divergence: Option<String>,
}

impl VectorParityReport {
    /// Whether every parity check held.
    pub fn holds(&self) -> bool {
        self.ids_ok && self.order_ok && self.monotonic_ok && self.divergence.is_none()
    }
}

/// Compare the EHDB engine's top-k against the authoritative Qdrant top-k for the
/// same query.
///
/// * `authoritative` — the incumbent Qdrant top-k (ordered best-first).
/// * `ehdb` — the EHDB engine's [`VectorQueryOutcome`] for the same query.
/// * `tolerance` — the allowed float slack when checking that the EHDB ranking is
///   monotonically non-increasing (scores differ across engines).
///
/// Returns the first divergence found, or a clean report.
pub fn compare_vector_parity(
    authoritative: &[AuthoritativeVectorHit],
    ehdb: &VectorQueryOutcome,
    tolerance: f32,
) -> VectorParityReport {
    let auth_ids: Vec<&str> = authoritative.iter().map(|h| h.point_id.as_str()).collect();
    let ehdb_ids: Vec<&str> = ehdb.hits.iter().map(|h| h.point_id.as_str()).collect();

    // Set parity: same multiset of ids (top-k ids are unique per collection, so a
    // sorted-sequence compare is a set compare).
    let mut auth_sorted = auth_ids.clone();
    auth_sorted.sort_unstable();
    let mut ehdb_sorted = ehdb_ids.clone();
    ehdb_sorted.sort_unstable();
    let ids_ok = auth_sorted == ehdb_sorted;

    // Rank parity: same ordered id sequence.
    let order_ok = auth_ids == ehdb_ids;

    // Monotonic ranking: EHDB scores non-increasing within tolerance.
    let monotonic_ok = ehdb
        .hits
        .windows(2)
        .all(|w| w[0].score + tolerance >= w[1].score);

    let divergence = if !ids_ok {
        Some(format!(
            "id-set divergence: authoritative {} ids != ehdb {} ids",
            auth_ids.len(),
            ehdb_ids.len()
        ))
    } else if !order_ok {
        Some("rank-order divergence: same ids, different ranking".to_string())
    } else if !monotonic_ok {
        Some("score divergence: ehdb ranking is not monotonic within tolerance".to_string())
    } else {
        None
    };

    VectorParityReport {
        ids_ok,
        order_ok,
        monotonic_ok,
        divergence,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Fixture {
        driver: LocalReferenceVectorDriver,
        dir: PathBuf,
    }

    fn fixture(tag: &str) -> Fixture {
        let dir = std::env::temp_dir().join(format!(
            "ehdb-vector-{tag}-{}-{:?}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let driver = LocalReferenceVectorDriver::new(dir.join("index.jsonl"), "noetl", "default");
        Fixture { driver, dir }
    }

    impl Drop for Fixture {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.dir);
        }
    }

    const MODEL: &str = "text-embedding-3-small";
    const COLLECTION: &str = "playbook-surface";
    // Real platform point ids can carry `/` and `.`.
    const POINT_A: &str = "noetl/playbook/weather.example/chunk.0";

    fn upsert(
        d: &LocalReferenceVectorDriver,
        collection: &str,
        point: &str,
        vector: &[f32],
        n: u64,
    ) -> VectorUpsertOutcome {
        d.upsert(&VectorUpsertRequest {
            collection: collection.to_string(),
            point_id: point.to_string(),
            model_id: MODEL.to_string(),
            vector: vector.to_vec(),
            payload: Some(format!("src://{point}")),
            transaction_id: format!("txn-{n}"),
        })
        .unwrap()
    }

    fn query(
        d: &LocalReferenceVectorDriver,
        collection: &str,
        q: &[f32],
        top_k: usize,
    ) -> VectorQueryOutcome {
        d.query(&VectorQueryRequest {
            collection: collection.to_string(),
            model_id: MODEL.to_string(),
            query: q.to_vec(),
            top_k,
        })
        .unwrap()
    }

    #[test]
    fn upsert_query_ranks_by_cosine() {
        let f = fixture("rank");
        let d = &f.driver;
        // Query [1,0,0] — a is closest, then b, then c.
        let a = upsert(d, COLLECTION, POINT_A, &[1.0, 0.0, 0.0], 1);
        assert!(a.written);
        assert_eq!(a.version, 1);
        assert!(a.created_stream);
        assert_eq!(a.dimensions, 3);
        upsert(d, COLLECTION, "point-b", &[0.9, 0.1, 0.0], 2);
        upsert(d, COLLECTION, "point-c", &[0.0, 1.0, 0.0], 3);

        let out = query(d, COLLECTION, &[1.0, 0.0, 0.0], 10);
        assert!(out.exists);
        assert_eq!(out.candidate_count, 3);
        assert_eq!(out.returned, 3);
        let ids: Vec<&str> = out.hits.iter().map(|h| h.point_id.as_str()).collect();
        assert_eq!(ids, vec![POINT_A, "point-b", "point-c"]);
        // Scores strictly descending.
        assert!(out.hits[0].score >= out.hits[1].score);
        assert!(out.hits[1].score >= out.hits[2].score);
    }

    #[test]
    fn top_k_truncates_and_flags() {
        let f = fixture("topk");
        let d = &f.driver;
        for i in 0..5 {
            upsert(d, COLLECTION, &format!("p{i}"), &[i as f32 + 1.0, 1.0], i);
        }
        let out = query(d, COLLECTION, &[1.0, 0.0], 2);
        assert_eq!(out.candidate_count, 5);
        assert_eq!(out.returned, 2);
        assert!(out.truncated_by_top_k);
    }

    #[test]
    fn upsert_overwrites_with_monotonic_version() {
        let f = fixture("overwrite");
        let d = &f.driver;
        upsert(d, COLLECTION, POINT_A, &[1.0, 0.0], 1);
        let second = upsert(d, COLLECTION, POINT_A, &[0.0, 1.0], 2);
        assert_eq!(second.version, 2);
        assert!(!second.created_stream);
        // Latest embedding wins: querying [0,1] now ranks the point at the top.
        let out = query(d, COLLECTION, &[0.0, 1.0], 10);
        assert_eq!(out.candidate_count, 1);
        assert_eq!(out.hits[0].point_id, POINT_A);
        assert!(out.hits[0].score > 0.99);
    }

    #[test]
    fn delete_tombstones_then_query_absent_and_idempotent() {
        let f = fixture("delete");
        let d = &f.driver;
        upsert(d, COLLECTION, POINT_A, &[1.0, 0.0], 1);
        upsert(d, COLLECTION, "point-b", &[0.0, 1.0], 2);
        let del = d
            .delete(&VectorDeleteRequest {
                collection: COLLECTION.to_string(),
                point_id: POINT_A.to_string(),
                transaction_id: "txn-del-1".to_string(),
            })
            .unwrap();
        assert!(del.existed);
        assert_eq!(del.version, 2);
        let out = query(d, COLLECTION, &[1.0, 0.0], 10);
        let ids: Vec<&str> = out.hits.iter().map(|h| h.point_id.as_str()).collect();
        assert_eq!(ids, vec!["point-b"]);
        // Second delete is an idempotent no-op.
        let del2 = d
            .delete(&VectorDeleteRequest {
                collection: COLLECTION.to_string(),
                point_id: POINT_A.to_string(),
                transaction_id: "txn-del-2".to_string(),
            })
            .unwrap();
        assert!(!del2.existed);
        assert_eq!(del2.global_sequence, 0);
    }

    #[test]
    fn collections_are_scope_isolated() {
        let f = fixture("scope");
        let d = &f.driver;
        upsert(d, "collection-a", "shared-id", &[1.0, 0.0], 1);
        upsert(d, "collection-b", "shared-id", &[0.0, 1.0], 2);
        let a = query(d, "collection-a", &[1.0, 0.0], 10);
        assert_eq!(a.candidate_count, 1);
        assert!(a.hits[0].score > 0.99);
        let b = query(d, "collection-b", &[1.0, 0.0], 10);
        assert_eq!(b.candidate_count, 1);
        // The same point id in b holds the orthogonal vector → score ~0.
        assert!(b.hits[0].score < 0.01);
    }

    #[test]
    fn query_filters_by_model_and_dimensionality() {
        let f = fixture("model");
        let d = &f.driver;
        upsert(d, COLLECTION, "same-model", &[1.0, 0.0], 1);
        // Different model → not a candidate.
        d.upsert(&VectorUpsertRequest {
            collection: COLLECTION.to_string(),
            point_id: "other-model".to_string(),
            model_id: "other-model-id".to_string(),
            vector: vec![1.0, 0.0],
            payload: None,
            transaction_id: "txn-2".to_string(),
        })
        .unwrap();
        // Different dimensionality → not a candidate.
        upsert(d, COLLECTION, "other-dims", &[1.0, 0.0, 0.0], 3);
        let out = query(d, COLLECTION, &[1.0, 0.0], 10);
        assert_eq!(out.candidate_count, 1);
        assert_eq!(out.hits[0].point_id, "same-model");
    }

    #[test]
    fn query_of_absent_collection_is_empty_not_error() {
        let f = fixture("absent");
        let out = query(&f.driver, "never-written", &[1.0, 0.0], 5);
        assert!(!out.exists);
        assert_eq!(out.returned, 0);
        assert!(out.hits.is_empty());
    }

    #[test]
    fn oversized_vector_is_rejected_bound() {
        let f = fixture("oversize");
        let big = vec![0.1f32; MAX_VECTOR_DIMENSIONS + 1];
        let err = f
            .driver
            .upsert(&VectorUpsertRequest {
                collection: COLLECTION.to_string(),
                point_id: "p".to_string(),
                model_id: MODEL.to_string(),
                vector: big,
                payload: None,
                transaction_id: "txn-1".to_string(),
            })
            .unwrap_err();
        assert!(err.to_string().contains("exceeds bound"));
    }

    #[test]
    fn over_limit_top_k_is_rejected_bound() {
        let f = fixture("topk-reject");
        let d = &f.driver;
        upsert(d, COLLECTION, POINT_A, &[1.0, 0.0], 1);
        let err = d
            .query(&VectorQueryRequest {
                collection: COLLECTION.to_string(),
                model_id: MODEL.to_string(),
                query: vec![1.0, 0.0],
                top_k: MAX_VECTOR_QUERY_TOP_K + 1,
            })
            .unwrap_err();
        assert!(err.to_string().contains("exceeds bound"));
    }

    #[test]
    fn empty_and_zero_vectors_are_invalid() {
        let f = fixture("badvec");
        let d = &f.driver;
        let empty = d
            .upsert(&VectorUpsertRequest {
                collection: COLLECTION.to_string(),
                point_id: "p".to_string(),
                model_id: MODEL.to_string(),
                vector: Vec::new(),
                payload: None,
                transaction_id: "txn".to_string(),
            })
            .unwrap_err();
        assert!(empty.to_string().contains("must not be empty"));
        let zero = d
            .upsert(&VectorUpsertRequest {
                collection: COLLECTION.to_string(),
                point_id: "p".to_string(),
                model_id: MODEL.to_string(),
                vector: vec![0.0, 0.0],
                payload: None,
                transaction_id: "txn".to_string(),
            })
            .unwrap_err();
        assert!(zero.to_string().contains("zero vector"));
    }

    #[test]
    fn empty_ids_are_invalid_identifier() {
        let f = fixture("badid");
        let d = &f.driver;
        let no_collection = d
            .upsert(&VectorUpsertRequest {
                collection: String::new(),
                point_id: "p".to_string(),
                model_id: MODEL.to_string(),
                vector: vec![1.0, 0.0],
                payload: None,
                transaction_id: "txn".to_string(),
            })
            .unwrap_err();
        assert!(no_collection.to_string().starts_with("invalid identifier"));
        let no_point = d
            .upsert(&VectorUpsertRequest {
                collection: COLLECTION.to_string(),
                point_id: String::new(),
                model_id: MODEL.to_string(),
                vector: vec![1.0, 0.0],
                payload: None,
                transaction_id: "txn".to_string(),
            })
            .unwrap_err();
        assert!(no_point.to_string().starts_with("invalid identifier"));
    }

    #[test]
    fn ids_with_dots_and_slashes_round_trip() {
        let f = fixture("special");
        let d = &f.driver;
        for (n, point) in [POINT_A, "a.b.c/d", "runtime/surface.md#0"]
            .into_iter()
            .enumerate()
        {
            upsert(d, COLLECTION, point, &[1.0, 0.0], n as u64);
        }
        let out = query(d, COLLECTION, &[1.0, 0.0], 10);
        assert_eq!(out.candidate_count, 3);
    }

    #[test]
    fn replay_reconstructs_from_log() {
        let f = fixture("replay");
        upsert(&f.driver, COLLECTION, POINT_A, &[1.0, 0.0], 1);
        upsert(&f.driver, COLLECTION, "point-b", &[0.0, 1.0], 2);
        // A fresh driver over the same log replays the index.
        let d2 = LocalReferenceVectorDriver::new(f.driver.log_path.clone(), "noetl", "default");
        let out = query(&d2, COLLECTION, &[1.0, 0.0], 10);
        assert_eq!(out.candidate_count, 2);
        assert_eq!(out.hits[0].point_id, POINT_A);
    }

    #[test]
    fn parity_holds_when_engines_agree() {
        let f = fixture("parity-ok");
        let d = &f.driver;
        upsert(d, COLLECTION, POINT_A, &[1.0, 0.0, 0.0], 1);
        upsert(d, COLLECTION, "point-b", &[0.0, 1.0, 0.0], 2);
        let ehdb = query(d, COLLECTION, &[1.0, 0.0, 0.0], 10);
        // Authoritative Qdrant returns the same ranking with slightly different
        // absolute scores (a different engine).
        let auth = vec![
            AuthoritativeVectorHit {
                point_id: POINT_A.to_string(),
                score: 0.998,
            },
            AuthoritativeVectorHit {
                point_id: "point-b".to_string(),
                score: 0.001,
            },
        ];
        let report = compare_vector_parity(&auth, &ehdb, 1e-3);
        assert!(report.holds(), "{report:?}");
        // Both empty also holds.
        let empty = query(d, COLLECTION, &[0.0, 0.0, 1.0], 10);
        // (orthogonal query still returns candidates; use an absent collection for
        // the true-empty case)
        let _ = empty;
        let absent = query(d, "never", &[1.0, 0.0, 0.0], 10);
        assert!(compare_vector_parity(&[], &absent, 1e-3).holds());
    }

    #[test]
    fn parity_flags_id_and_order_divergence() {
        let f = fixture("parity-bad");
        let d = &f.driver;
        upsert(d, COLLECTION, POINT_A, &[1.0, 0.0, 0.0], 1);
        upsert(d, COLLECTION, "point-b", &[0.0, 1.0, 0.0], 2);
        let ehdb = query(d, COLLECTION, &[1.0, 0.0, 0.0], 10);

        // Authoritative has an id EHDB does not → id-set divergence.
        let extra = vec![
            AuthoritativeVectorHit {
                point_id: POINT_A.to_string(),
                score: 0.99,
            },
            AuthoritativeVectorHit {
                point_id: "point-b".to_string(),
                score: 0.5,
            },
            AuthoritativeVectorHit {
                point_id: "point-c".to_string(),
                score: 0.1,
            },
        ];
        let ids = compare_vector_parity(&extra, &ehdb, 1e-3);
        assert!(!ids.ids_ok);
        assert!(ids.divergence.unwrap().contains("id-set divergence"));

        // Same ids, reversed order → rank-order divergence.
        let reversed = vec![
            AuthoritativeVectorHit {
                point_id: "point-b".to_string(),
                score: 0.99,
            },
            AuthoritativeVectorHit {
                point_id: POINT_A.to_string(),
                score: 0.5,
            },
        ];
        let order = compare_vector_parity(&reversed, &ehdb, 1e-3);
        assert!(order.ids_ok);
        assert!(!order.order_ok);
        assert!(order.divergence.unwrap().contains("rank-order divergence"));
    }

    #[test]
    fn parity_flags_non_monotonic_ranking() {
        // A hand-built EHDB outcome whose scores ascend → monotonic check fails.
        let ehdb = VectorQueryOutcome {
            action: "vector-query".to_string(),
            collection: COLLECTION.to_string(),
            exists: true,
            candidate_count: 2,
            returned: 2,
            truncated_by_top_k: false,
            hits: vec![
                VectorHit {
                    point_id: "a".to_string(),
                    score: 0.1,
                },
                VectorHit {
                    point_id: "b".to_string(),
                    score: 0.9,
                },
            ],
        };
        let auth = vec![
            AuthoritativeVectorHit {
                point_id: "a".to_string(),
                score: 0.1,
            },
            AuthoritativeVectorHit {
                point_id: "b".to_string(),
                score: 0.9,
            },
        ];
        let report = compare_vector_parity(&auth, &ehdb, 1e-6);
        assert!(!report.monotonic_ok);
        assert!(report.divergence.unwrap().contains("score divergence"));
    }

    #[test]
    fn driver_name_is_stable() {
        let f = fixture("name");
        assert_eq!(f.driver.driver_name(), "ehdb-local-reference");
    }
}
