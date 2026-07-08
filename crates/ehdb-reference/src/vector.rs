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
//!   scoped by a per-point subject
//!   `noetl.vec.<sha256hex(collection)>.<sha256hex(point_id)>`.
//!   Re-upserting the same point advances a monotonic per-point version; the latest
//!   record wins.  The collection + point id ride in the record envelope **verbatim**
//!   (digested into subject tokens only), so ids carrying `.` / `/` round-trip and
//!   a long id can never overflow the 256-char subject cap.
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
use ehdb_storage::ObjectDigest;
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
/// subject is `noetl.vec.<sha256hex(collection)>.<sha256hex(point_id)>`, so a
/// per-point read is an exact subject-filtered replay and a collection query is a
/// `noetl.vec.<sha256hex(collection)>.>` replay folded + filtered.  Both the
/// collection and the point id are reduced to fixed-width SHA-256 digest tokens
/// (not their own hex) because ids carry `.` / `/` that would split a subject
/// token, and a long id would hex-encode (2 chars/byte) past the 256-char
/// [`Subject`] cap.  A digest keeps each token a constant 64 chars for an id of
/// any length; the full collection + point id live in the record payload so a
/// read never reverses the subject.
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

/// The fixed-width SHA-256 digest token addressing an id (collection or point)
/// inside a subject.  Hashing the id (rather than hex-encoding it whole) bounds
/// the token to a constant 64 hex chars regardless of id length; the former
/// hex-of-full-id form (2 chars/byte) would overflow the 256-char [`Subject`] cap
/// for a long id.  Deterministic (same id ⇒ same token, so the collection filter
/// stays consistent with the per-point subject) + collision-safe (SHA-256), and
/// the full ids are preserved in the record payload so a subject never needs
/// decoding.
fn digest_token(value: &str) -> Result<String> {
    let digest = ObjectDigest::sha256(value.as_bytes());
    digest
        .as_str()
        .strip_prefix("sha256:")
        .map(|hex| hex.to_string())
        .ok_or_else(|| EhdbError::Storage(format!("unexpected digest form: {}", digest.as_str())))
}

/// Build the exact per-point subject
/// `noetl.vec.<sha256hex(collection)>.<sha256hex(point_id)>`.
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
    let col = digest_token(collection)?;
    let point = digest_token(point_id)?;
    Subject::new(format!("{VECTOR_SUBJECT_PREFIX}.{col}.{point}"))
}

/// Build the collection-scoped query filter `noetl.vec.<sha256hex(collection)>.>`.
fn collection_filter(collection: &str) -> Result<SubjectFilter> {
    if collection.is_empty() {
        return Err(EhdbError::InvalidIdentifier(
            "vector collection: empty".to_string(),
        ));
    }
    let col = digest_token(collection)?;
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
        point: (&str, &str),
    ) -> Option<VectorEnvelope> {
        let (collection, point_id) = point;
        let filter = SubjectFilter::new(subject.as_str().to_string()).ok()?;
        let records = runtime
            .state()
            .streams
            .replay_matching(tenant, namespace, stream, &filter, None)
            .ok()?;
        // The subject tokens are SHA-256 digests of the collection + point id, so a
        // (cryptographically infeasible) digest collision would co-mingle two
        // points' records under one subject.  Filter the replay to this exact
        // (collection, point id) — both live in the record payload — so a per-point
        // read never resolves a colliding id's envelope.
        records.iter().rev().find_map(|record| {
            decode_envelope(record)
                .filter(|env| env.collection == collection && env.point_id == point_id)
        })
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
        let latest = self.latest_envelope(
            &runtime,
            &tenant,
            &namespace,
            &stream,
            &subject,
            (&request.collection, &request.point_id),
        );
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
        // so a later record overwrites an earlier one).  The collection filter
        // matches on the collection's SHA-256 digest, so guard against a
        // (cryptographically infeasible) digest collision by folding only records
        // whose payload collection is the exact one queried.
        let mut latest_by_point: BTreeMap<String, VectorEnvelope> = BTreeMap::new();
        for record in records {
            if let Some(env) = decode_envelope(&record) {
                if env.collection == request.collection {
                    latest_by_point.insert(env.point_id.clone(), env);
                }
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
        let latest = self.latest_envelope(
            &runtime,
            &tenant,
            &namespace,
            &stream,
            &subject,
            (&request.collection, &request.point_id),
        );

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

// ===========================================================================
// Primary-serve (completion program Phase 9, tier 5 — the final tier) — EHDB
// serves the platform vector tier authoritatively in place of the internal
// Qdrant retrieval path (the platform RAG collections reached in-process through
// the Phase-E retrieval helper, [`crate::retrieve_local_reference_context`]).
//
// Tiers 1 (event log), 2 (projection), 3 (KV/state), and 4 (object/blob) proved
// the per-tier cutover pattern: an authoritative serving cycle that drives every
// capability through the EHDB engine while dual-run parity-checking the served
// results against the incumbent, plus a fresh-engine replay proving the store
// stays whole (reversibility).  This is the vector mirror of that pattern — the
// serving legs are the vector capabilities (upsert → query-topk → delete) instead
// of the object put/get/list/locate/delete, and the incumbent is the internal
// Qdrant retrieval path.  Because top-k is a ranking (not a single value), each
// served query's dual-run parity is an id-set + rank-order + score-monotonicity
// match ([`compare_vector_parity`]) against an in-lockstep Qdrant mirror computed
// with the identical cosine ranking.
//
// ## Reversibility (the safety property the cutover is gated on)
//
// The cycle appends only to the EHDB vector index stream
// ([`RetentionPolicy::KeepAll`]) and never touches the incumbent Qdrant path.
// Flipping a caller back from `primary` to `shadow`/`off` therefore restores
// Qdrant as the authoritative vector path with zero data loss — the EHDB index
// stays intact on disk (a later re-enable replays it whole) and Qdrant was never
// written.  [`exercise_primary_serve`] proves the "EHDB index stays intact" half
// directly via the fresh-driver replay leg; the "Qdrant untouched" half is a
// structural property of the caller (the worker asserts it by never importing a
// Qdrant writer).
// ===========================================================================

/// The vector drive served authoritatively through one primary-serve cycle: the
/// collection, the shared embedding model, the seed point/embedding entries, and
/// the query + top_k the served reads rank against.
///
/// The cycle upserts every entry, serves a top-k query (dual-run parity-checked
/// against an in-lockstep Qdrant mirror), deletes the last point (tombstone),
/// serves the query again (the deleted point now absent), and finally replays a
/// fresh driver over the same index.
#[derive(Debug, Clone, PartialEq)]
pub struct VectorPrimaryInput {
    /// The platform collection served (e.g. `playbook-surface`).
    pub collection: String,
    /// The embedding model every seed point + the query share (a query only ranks
    /// points of its own model, so the whole drive is one model's ranking).
    pub model_id: String,
    /// Distinct point/embedding entries seeded into the tier.  At least two are
    /// required so the delete (on the last) leaves the rest live for the replay
    /// ([`EhdbError::InvalidState`] otherwise); every embedding must match the
    /// query's dimensionality to be a ranked candidate.
    pub entries: Vec<(String, Vec<f32>)>,
    /// The query embedding the served top-k ranks against.
    pub query: Vec<f32>,
    /// How many top hits each served query returns (over [`MAX_VECTOR_QUERY_TOP_K`]
    /// ⇒ rejected).
    pub top_k: usize,
}

/// The served-by-EHDB proof for one vector primary-serve cycle: every serving leg
/// ran through the engine and preserved the Qdrant retrieval semantics (bounded
/// cosine top-k ranking, tombstone delete), and each served query held dual-run
/// parity against the Qdrant mirror.  Secret-free (counts + verdicts; the parity
/// reports carry point ids + a monotonicity verdict, never vectors or payloads).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VectorPrimaryServeReport {
    /// The backing engine that served the cycle.
    pub driver_name: String,
    /// How many points the cycle wrote authoritatively.
    pub upsert_count: usize,
    /// Every seed upsert was written.
    pub upsert_ok: bool,
    /// The served top-k query ranked exactly the live Qdrant mirror (id set + rank
    /// order) and held score-monotonicity.
    pub query_ok: bool,
    pub query_returned: usize,
    /// A served tombstone dropped the last point out of the served ranking (parity
    /// vs the mirror with that point removed).
    pub delete_ok: bool,
    /// A fresh driver over the same on-disk index served the identical live ranking
    /// (replay-is-truth / durability — the reversibility half proven directly).
    pub replay_returned: usize,
    pub replay_matches: bool,
    /// Per-served-query dual-run parity verdicts against the Qdrant mirror (query,
    /// post-delete query, replay query).
    pub dual_run: Vec<VectorParityReport>,
    /// Every dual-run parity verdict held.
    pub dual_run_holds: bool,
    /// The single reason the cycle failed a served-by-EHDB invariant, or `None`.
    pub divergence: Option<String>,
}

impl VectorPrimaryServeReport {
    /// Whether the EHDB engine served the whole cycle with the Qdrant retrieval
    /// semantics preserved and dual-run parity intact.
    pub fn served_by_ehdb(&self) -> bool {
        self.upsert_ok
            && self.query_ok
            && self.delete_ok
            && self.replay_matches
            && self.dual_run_holds
            && self.divergence.is_none()
    }
}

/// The in-lockstep Qdrant mirror's top-k for one query — computed with the
/// identical cosine ranking as the engine, over the mirror's live points, so a
/// served query's dual-run parity is an exact id-set + rank-order match (the
/// mirror is an independent computation from the engine's log-backed query, not a
/// copy of its output).
fn mirror_top_k(
    mirror: &BTreeMap<String, Vec<f32>>,
    query: &[f32],
    top_k: usize,
) -> Vec<AuthoritativeVectorHit> {
    let query_norm = vector_norm(query);
    let mut hits: Vec<AuthoritativeVectorHit> = mirror
        .iter()
        .filter(|(_, vector)| vector.len() == query.len())
        .map(|(point_id, vector)| AuthoritativeVectorHit {
            score: cosine_similarity(query, vector, query_norm),
            point_id: point_id.clone(),
        })
        .collect();
    hits.sort_by(|left, right| {
        right
            .score
            .total_cmp(&left.score)
            .then_with(|| left.point_id.cmp(&right.point_id))
    });
    hits.truncate(top_k);
    hits
}

/// Run the authoritative vector primary-serve cycle over `driver`.
///
/// Drives every serving leg through the EHDB engine — upsert, served top-k
/// `query`, tombstone `delete`, and a fresh-driver replay — asserting the Qdrant
/// retrieval semantics are preserved and dual-run parity-checking each served
/// query against a Qdrant mirror computed in lockstep with the identical cosine
/// ranking.  Returns the [`VectorPrimaryServeReport`] served-by-EHDB proof.
///
/// Reversible + non-destructive toward the incumbent: appends only to the EHDB
/// vector index stream ([`RetentionPolicy::KeepAll`]); the replay leg proves the
/// index stays whole so a flip back to Qdrant loses nothing.
///
/// `input.entries` must hold at least two entries whose first and last point ids
/// differ ([`EhdbError::InvalidState`] otherwise).  An over-cap `top_k` is
/// rejected by the engine ([`EhdbError::InvalidState`] carrying `exceeds bound`).
/// `transaction_prefix` scopes the per-write transaction ids.
pub fn exercise_primary_serve(
    driver: &LocalReferenceVectorDriver,
    input: &VectorPrimaryInput,
    transaction_prefix: &str,
) -> Result<VectorPrimaryServeReport> {
    if input.entries.len() < 2 {
        return Err(EhdbError::InvalidState(
            "vector primary-serve requires at least two entries".to_string(),
        ));
    }
    let first_key = input.entries.first().unwrap().0.clone();
    let last_key = input.entries.last().unwrap().0.clone();
    if first_key == last_key {
        return Err(EhdbError::InvalidState(
            "vector primary-serve requires the first and last point ids to differ".to_string(),
        ));
    }

    // The authoritative Qdrant mirror the served queries are dual-run
    // parity-checked against — the live point → embedding map, ranked in lockstep
    // with the identical cosine scoring (an independent computation, not the
    // engine's own output).
    let mut mirror: BTreeMap<String, Vec<f32>> = BTreeMap::new();
    let mut dual_run: Vec<VectorParityReport> = Vec::new();
    let mut txn = 0u64;
    let mut next_txn = || {
        txn += 1;
        format!("{transaction_prefix}-{txn}")
    };

    // Float slack when checking the EHDB ranking is monotonically non-increasing.
    // Scores differ across engines in general; here the mirror shares the formula,
    // so the slack is only defensive.
    const PARITY_TOLERANCE: f32 = 1e-6;

    // --- Upsert leg: EHDB serves the authoritative index write. ---------------
    let mut upsert_ok = true;
    for (point_id, vector) in &input.entries {
        let out = driver.upsert(&VectorUpsertRequest {
            collection: input.collection.clone(),
            point_id: point_id.clone(),
            model_id: input.model_id.clone(),
            vector: vector.clone(),
            payload: None,
            transaction_id: next_txn(),
        })?;
        upsert_ok &= out.written;
        mirror.insert(point_id.clone(), vector.clone());
    }
    let upsert_count = input.entries.len();

    // --- Query leg: served top-k, dual-run parity vs the live mirror. ---------
    let query1 = driver.query(&VectorQueryRequest {
        collection: input.collection.clone(),
        model_id: input.model_id.clone(),
        query: input.query.clone(),
        top_k: input.top_k,
    })?;
    let mirror1 = mirror_top_k(&mirror, &input.query, input.top_k);
    let parity1 = compare_vector_parity(&mirror1, &query1, PARITY_TOLERANCE);
    let query_ok = query1.exists && query1.returned == mirror1.len() && parity1.holds();
    let query_returned = query1.returned;
    dual_run.push(parity1);

    // --- Delete leg: a served tombstone drops the last point from the ranking. -
    let del = driver.delete(&VectorDeleteRequest {
        collection: input.collection.clone(),
        point_id: last_key.clone(),
        transaction_id: next_txn(),
    })?;
    mirror.remove(&last_key);
    let query2 = driver.query(&VectorQueryRequest {
        collection: input.collection.clone(),
        model_id: input.model_id.clone(),
        query: input.query.clone(),
        top_k: input.top_k,
    })?;
    let mirror2 = mirror_top_k(&mirror, &input.query, input.top_k);
    let parity2 = compare_vector_parity(&mirror2, &query2, PARITY_TOLERANCE);
    let last_absent = query2.hits.iter().all(|hit| hit.point_id != last_key);
    let delete_ok = del.existed && last_absent && parity2.holds();
    dual_run.push(parity2);

    // --- Replay leg: a fresh driver over the same index serves the live ranking.
    let replay_driver = driver.clone();
    let replay = replay_driver.query(&VectorQueryRequest {
        collection: input.collection.clone(),
        model_id: input.model_id.clone(),
        query: input.query.clone(),
        top_k: input.top_k,
    })?;
    let mirror_replay = mirror_top_k(&mirror, &input.query, input.top_k);
    let replay_parity = compare_vector_parity(&mirror_replay, &replay, PARITY_TOLERANCE);
    let replay_returned = replay.returned;
    let replay_matches =
        replay.exists && replay.returned == mirror_replay.len() && replay_parity.holds();
    dual_run.push(replay_parity);

    let dual_run_holds = dual_run.iter().all(VectorParityReport::holds);

    let divergence = if !upsert_ok {
        Some("primary upsert leg did not write every point".to_string())
    } else if !query_ok {
        Some("primary query leg lost a point or diverged from the vector mirror".to_string())
    } else if !delete_ok {
        Some("primary delete leg did not tombstone the point".to_string())
    } else if !replay_matches {
        Some(format!(
            "primary replay lost the live ranking: replayed {replay_returned} hits"
        ))
    } else if !dual_run_holds {
        dual_run
            .iter()
            .find_map(|report| report.divergence.clone())
            .or_else(|| Some("primary dual-run parity diverged".to_string()))
    } else {
        None
    };

    Ok(VectorPrimaryServeReport {
        driver_name: driver.driver_name().to_string(),
        upsert_count,
        upsert_ok,
        query_ok,
        query_returned,
        delete_ok,
        replay_returned,
        replay_matches,
        dual_run,
        dual_run_holds,
        divergence,
    })
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

    // -----------------------------------------------------------------------
    // Primary-serve (Phase 9, tier 5) tests
    // -----------------------------------------------------------------------

    /// A three-point drive whose query [1,0,0] ranks a > b > c; the delete drops
    /// the last-listed point (c).
    fn primary_input() -> VectorPrimaryInput {
        VectorPrimaryInput {
            collection: COLLECTION.to_string(),
            model_id: MODEL.to_string(),
            entries: vec![
                (POINT_A.to_string(), vec![1.0, 0.0, 0.0]),
                ("point-b".to_string(), vec![0.9, 0.1, 0.0]),
                ("point-c".to_string(), vec![0.0, 1.0, 0.0]),
            ],
            query: vec![1.0, 0.0, 0.0],
            top_k: 10,
        }
    }

    #[test]
    fn primary_serve_reports_served_by_ehdb() {
        let f = fixture("primary-served");
        let report = exercise_primary_serve(&f.driver, &primary_input(), "primary-t5").unwrap();
        assert!(report.served_by_ehdb(), "{report:?}");
        assert_eq!(report.driver_name, "ehdb-local-reference");
        assert_eq!(report.upsert_count, 3);
        assert!(report.upsert_ok);
        assert!(report.query_ok);
        assert_eq!(report.query_returned, 3);
        assert!(report.delete_ok);
        // The last point was tombstoned → the live ranking is the remaining two.
        assert_eq!(report.replay_returned, 2);
        assert!(report.replay_matches);
        assert!(report.dual_run_holds);
        assert!(report.divergence.is_none());
        // query + post-delete query + replay query = three dual-run verdicts.
        assert_eq!(report.dual_run.len(), 3);
        assert!(report.dual_run.iter().all(VectorParityReport::holds));
    }

    #[test]
    fn primary_serve_is_reversible_index_intact() {
        // The cycle only appends to the EHDB index; a fresh driver over the same
        // log serves the same live ranking (the deleted point stays absent), so a
        // flip back to Qdrant loses nothing.
        let f = fixture("primary-reversible");
        let report = exercise_primary_serve(&f.driver, &primary_input(), "primary-rev").unwrap();
        assert!(report.served_by_ehdb());
        let fresh = LocalReferenceVectorDriver::new(f.driver.log_path.clone(), "noetl", "default");
        let out = query(&fresh, COLLECTION, &[1.0, 0.0, 0.0], 10);
        let ids: Vec<&str> = out.hits.iter().map(|h| h.point_id.as_str()).collect();
        // point-c was tombstoned; a and b survive, ranked a > b.
        assert_eq!(ids, vec![POINT_A, "point-b"]);
    }

    #[test]
    fn primary_serve_top_k_parity_under_truncation() {
        // A top_k below the candidate count still holds dual-run parity: both the
        // engine and the mirror truncate the identical ranking.
        let f = fixture("primary-topk");
        let mut input = primary_input();
        input.top_k = 2;
        let report = exercise_primary_serve(&f.driver, &input, "primary-topk").unwrap();
        assert!(report.served_by_ehdb(), "{report:?}");
        assert_eq!(report.query_returned, 2);
        // After the delete only two points remain, so the replay returns both.
        assert_eq!(report.replay_returned, 2);
    }

    #[test]
    fn primary_serve_requires_two_entries() {
        let f = fixture("primary-one");
        let input = VectorPrimaryInput {
            collection: COLLECTION.to_string(),
            model_id: MODEL.to_string(),
            entries: vec![(POINT_A.to_string(), vec![1.0, 0.0])],
            query: vec![1.0, 0.0],
            top_k: 10,
        };
        let err = exercise_primary_serve(&f.driver, &input, "primary-one").unwrap_err();
        assert!(err.to_string().contains("at least two entries"));
    }

    #[test]
    fn primary_serve_requires_distinct_first_last() {
        let f = fixture("primary-dup");
        let input = VectorPrimaryInput {
            collection: COLLECTION.to_string(),
            model_id: MODEL.to_string(),
            entries: vec![
                (POINT_A.to_string(), vec![1.0, 0.0]),
                (POINT_A.to_string(), vec![0.0, 1.0]),
            ],
            query: vec![1.0, 0.0],
            top_k: 10,
        };
        let err = exercise_primary_serve(&f.driver, &input, "primary-dup").unwrap_err();
        assert!(err
            .to_string()
            .contains("first and last point ids to differ"));
    }

    #[test]
    fn primary_serve_over_limit_top_k_rejected() {
        // An over-cap top_k surfaces from the engine's served query as a rejected
        // (bound) error, propagated out of the cycle.
        let f = fixture("primary-reject");
        let mut input = primary_input();
        input.top_k = MAX_VECTOR_QUERY_TOP_K + 1;
        let err = exercise_primary_serve(&f.driver, &input, "primary-reject").unwrap_err();
        assert!(err.to_string().contains("exceeds bound"));
        assert!(matches!(err, EhdbError::InvalidState(_)));
    }

    #[test]
    fn primary_serve_is_scope_isolated() {
        // Serving one collection's cycle never leaks into a sibling collection's
        // ranking (subject scoping holds under the primary path).
        let f = fixture("primary-scope");
        // Seed an unrelated collection first.
        upsert(
            &f.driver,
            "other-collection",
            "other-point",
            &[1.0, 0.0, 0.0],
            99,
        );
        let report = exercise_primary_serve(&f.driver, &primary_input(), "primary-scope").unwrap();
        assert!(report.served_by_ehdb(), "{report:?}");
        // The sibling collection is untouched by the delete leg.
        let other = query(&f.driver, "other-collection", &[1.0, 0.0, 0.0], 10);
        assert_eq!(other.candidate_count, 1);
        assert_eq!(other.hits[0].point_id, "other-point");
    }

    #[test]
    fn digest_token_produces_subject_safe_fixed_width_tokens() {
        for sample in [
            "playbook-surface",
            "noetl/playbook/weather.example/chunk.0",
            "ünïcöde",
        ] {
            let token = digest_token(sample).unwrap();
            // A SHA-256 digest token is a fixed 64 lowercase `[0-9a-f]` chars —
            // one subject-safe, constant-width token regardless of id length.
            assert_eq!(token.len(), 64);
            assert!(token
                .chars()
                .all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase()));
            assert!(Subject::new(format!("{VECTOR_SUBJECT_PREFIX}.{token}.{token}")).is_ok());
        }
    }

    // A platform RAG collection + point id can be long — a document URI, a chunk
    // coordinate, a per-execution namespace.  The former hex-of-full-id subject
    // (2 chars/byte for BOTH tokens) overflowed the 256-char `Subject` cap; the
    // digest tokens keep the subject bounded for ids of any length.
    const LONG_COLLECTION: &str =
        "noetl/rag/tenant=acme-corporation/env=production/region=us-central1/knowledge-base=support-articles-v3";
    const LONG_POINT: &str =
        "noetl/doc=https%3A%2F%2Fdocs.example.com%2Fguides%2Fonboarding%2Fpart-04.html/chunk=0042/model=text-embedding-3-small";

    #[test]
    fn long_vector_ids_subject_is_bounded_and_round_trip() {
        let f = fixture("long-ids");
        let d = &f.driver;
        // Both ids long enough that the old hex-of-id subject (each token 2
        // chars/byte) overflowed the 256-char cap.
        assert!(LONG_COLLECTION.len() + LONG_POINT.len() > 123);
        let subject = point_subject(LONG_COLLECTION, LONG_POINT).unwrap();
        assert!(subject.as_str().len() < 256);
        // `noetl.vec.` + 64 + `.` + 64 = a fixed 139 chars.
        assert_eq!(
            subject.as_str().len(),
            VECTOR_SUBJECT_PREFIX.len() + 1 + 64 + 1 + 64
        );
        assert!(collection_filter(LONG_COLLECTION).unwrap().as_str().len() < 256);
        // Upsert → query → delete round-trip surfaces the real long ids from the
        // record payload (never reversed out of the digest subject).
        let up = upsert(d, LONG_COLLECTION, LONG_POINT, &[1.0, 0.0, 0.0], 1);
        assert!(up.written);
        assert_eq!(up.version, 1);
        let out = query(d, LONG_COLLECTION, &[1.0, 0.0, 0.0], 10);
        assert!(out.exists);
        assert_eq!(out.candidate_count, 1);
        assert_eq!(out.hits[0].point_id, LONG_POINT);
        // Overwrite advances the per-point version on the long id.
        let up2 = upsert(d, LONG_COLLECTION, LONG_POINT, &[0.0, 1.0, 0.0], 2);
        assert_eq!(up2.version, 2);
        // Delete tombstones the long point.
        let del = d
            .delete(&VectorDeleteRequest {
                collection: LONG_COLLECTION.to_string(),
                point_id: LONG_POINT.to_string(),
                transaction_id: "txn-del-long".to_string(),
            })
            .unwrap();
        assert!(del.existed);
        let gone = query(d, LONG_COLLECTION, &[0.0, 1.0, 0.0], 10);
        assert_eq!(gone.candidate_count, 0);
    }

    #[test]
    fn distinct_vector_points_get_distinct_bounded_subjects() {
        // Digest subjects are deterministic, unique across distinct (collection,
        // point) pairs, and always bounded regardless of id length.
        let a = point_subject("col-x", "point-1").unwrap();
        let b = point_subject("col-x", "point-2").unwrap();
        let c = point_subject("col-y", "point-1").unwrap();
        assert_ne!(
            a.as_str(),
            b.as_str(),
            "distinct points ⇒ distinct subjects"
        );
        assert_ne!(
            a.as_str(),
            c.as_str(),
            "distinct collections ⇒ distinct subjects"
        );
        assert!(a.as_str().len() < 256 && b.as_str().len() < 256 && c.as_str().len() < 256);
        let a2 = point_subject("col-x", "point-1").unwrap();
        assert_eq!(
            a.as_str(),
            a2.as_str(),
            "same (collection, point) ⇒ same subject"
        );
        // Even a pathologically long pair stays under the cap.
        let long_col = format!("col/{}", "seg=abcdefgh/".repeat(40));
        let long_point = format!("pt/{}", "seg=ijklmnop/".repeat(40));
        assert!(long_col.len() > 400 && long_point.len() > 400);
        assert!(
            point_subject(&long_col, &long_point)
                .unwrap()
                .as_str()
                .len()
                < 256
        );
    }

    #[test]
    fn old_hex_of_id_subject_overflowed_the_cap_repro() {
        // Reproduction of the pre-fix defect (the object-tier bug ehdb#256, latent
        // in vector): the OLD subject hex-encoded BOTH the collection and the point
        // id (2 chars/byte each), so long RAG ids produced a >256-char subject that
        // `Subject::new` rejects.  The digest tokens (this fix) bound it instead.
        fn old_hex(bytes: &[u8]) -> String {
            bytes.iter().map(|b| format!("{b:02x}")).collect()
        }
        let old_subject = format!(
            "{VECTOR_SUBJECT_PREFIX}.{}.{}",
            old_hex(LONG_COLLECTION.as_bytes()),
            old_hex(LONG_POINT.as_bytes())
        );
        assert!(
            old_subject.len() > 256,
            "old hex-of-id subject len {} must exceed the 256-char cap",
            old_subject.len()
        );
        assert!(
            Subject::new(old_subject).is_err(),
            "old hex-of-id subject must be rejected by the Subject cap"
        );
        // The fix's digest subject for the SAME ids is accepted + bounded.
        assert!(point_subject(LONG_COLLECTION, LONG_POINT).is_ok());
    }
}
