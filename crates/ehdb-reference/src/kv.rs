//! EHDB KV / platform-state core engine (completion program Phase 8, slice 1).
//!
//! This is the durable key/value engine that Phase 8 puts *underneath* NoETL's
//! internal **NATS JetStream Key-Value** usage — the platform-state tier, NOT
//! business data.  The concrete internal keyspace it replaces today is the
//! worker's subscription circuit-breaker store (the single `noetl_subscription_circuit`
//! NATS-KV bucket: `circuit.<subscription_id>` → a JSON `CircuitRegistry` snapshot,
//! rehydrated on restart, overwritten on each breaker transition); the broader
//! platform KV surface (e.g. the #115 program-scale ChainHeads / ExecDescriptor
//! coherence keys) folds into the same tier as it moves off NATS-KV.
//!
//! ## Boundary — this is the KV storage engine, NOT an event author
//!
//! A KV entry is derived platform state, not an event.  This engine never
//! authors a `noetl.event`; it persists + serves key/value state only.  It is a
//! platform engine for platform KV state only; **business data never flows
//! through it** (tenant/domain KV stays external, reached by playbook connectors).
//!
//! ## Semantics preserved from the NATS-KV path
//!
//! * **Last-writer-wins per key** — a `get` returns the newest value for a key,
//!   the same latest-revision semantic a NATS-KV `get` gives (JetStream KV keeps
//!   `history` revisions; the incumbent bucket uses `history = 1`).  The engine
//!   models each write as an append to a single canonical stream
//!   ([`KV_STATE_STREAM`]) scoped by a per-key subject, and a `get` is the
//!   latest record of that key's subject-filtered replay.
//! * **Delete = tombstone** — a delete appends a tombstone record; a subsequent
//!   `get` sees the key as absent (the KV `purge`/`delete` twin), and the log
//!   still replays deterministically.
//! * **Scan by bucket** — a bucket is a subject token, so listing a bucket's live
//!   keys is a subject-filtered replay folded to the latest record per key, with
//!   tombstones + TTL-expired entries dropped.
//! * **CAS (compare-and-swap)** — an optimistic write conditioned on the caller's
//!   expected version (absent, or a specific version), the NATS-KV `update` with
//!   an expected revision twin.  A mismatch is a distinct *conflict* outcome, not
//!   an error, and does not append.
//! * **TTL** — an entry may carry an absolute `expires_at_ms`; a read past that
//!   instant treats the key as absent.  The engine is **clock-free** — the caller
//!   owns the clock and supplies both the absolute expiry at write and `now_ms`
//!   at read, so the engine stays deterministic + replay-reproducible.
//! * **Append-only + immutable + replay-is-truth** — records are never mutated;
//!   `KeepAll` retention keeps the whole write history so any past state is a
//!   replay.
//!
//! ## Driver interface (Phase 10-ready)
//!
//! The engine is exposed behind [`KvStateDriver`] so the KV tier is
//! driver-selectable: the EHDB engine here is [`LocalReferenceKvStateDriver`]; a
//! NATS-KV driver implementing the same trait keeps the tier selectable back to
//! the incumbent (Roadmap Phase 10).  Callers program against the trait.
//!
//! ## Shadow validation
//!
//! [`compare_kv_parity`] is the pure, secret-free comparison the worker's
//! disabled-by-default shadow mode uses to prove the EHDB engine tracks the
//! authoritative NATS-KV bucket without serving reads from it: presence parity,
//! value parity, and TTL parity, with a single divergence reason when they differ.

use std::collections::BTreeMap;
use std::path::PathBuf;

use ehdb_core::{EhdbError, NamespaceName, Result, StreamName, TenantId, TransactionId};
use ehdb_stream::{RetentionPolicy, StreamRecord, Subject, SubjectFilter};
use ehdb_transaction::{CommitTransaction, Mutation, StreamMutation};
use serde::{Deserialize, Serialize};

use crate::LocalReferenceRuntime;

/// The single canonical stream that carries every platform KV write.  One stream
/// keeps its [`ehdb_stream::StreamSequence`] the global write-order sequence for
/// the whole KV tier, so replay is deterministic.
pub const KV_STATE_STREAM: &str = "noetl_kv_state";

/// Subject prefix scoping a KV write to its bucket + key.  A record's subject is
/// `noetl.kv.<bucket>.<hex(key)>`, so a per-key read is an exact subject-filtered
/// replay and a bucket scan is a `noetl.kv.<bucket>.>` replay.  The key is
/// hex-encoded into a single subject token because NoETL KV keys carry `.` / `/`
/// (e.g. `circuit.12345`) which are not valid inside one subject token.
pub const KV_SUBJECT_PREFIX: &str = "noetl.kv";

/// Upper bound on one stored value (bounded like the rest of the integration).
/// An over-cap value is an [`EhdbError::InvalidState`] whose message carries
/// `exceeds bound`, so a caller mistake classifies as *rejected*, distinct from
/// an identifier mistake or an engine-unavailable error.
pub const MAX_KV_VALUE_BYTES: usize = 1_048_576;

/// Hard ceiling on a single scan's returned entries.
pub const MAX_KV_SCAN_LIMIT: usize = 4_096;

fn hex_encode(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for &b in bytes {
        out.push(HEX[(b >> 4) as usize] as char);
        out.push(HEX[(b & 0x0f) as usize] as char);
    }
    out
}

/// Validate a bucket name — a single non-empty subject token of `[A-Za-z0-9_-]`
/// (the incumbent `noetl_subscription_circuit` qualifies).  A `.` would split the
/// bucket across subject tokens, so it is rejected as an
/// [`EhdbError::InvalidIdentifier`].
fn validated_bucket(bucket: &str) -> Result<String> {
    let b = bucket.trim();
    if b.is_empty()
        || !b
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
    {
        return Err(EhdbError::InvalidIdentifier(format!(
            "kv bucket: {bucket:?}"
        )));
    }
    Ok(b.to_string())
}

/// Build the exact per-key subject `noetl.kv.<bucket>.<hex(key)>`.
fn key_subject(bucket: &str, key: &str) -> Result<Subject> {
    let bucket = validated_bucket(bucket)?;
    if key.is_empty() {
        return Err(EhdbError::InvalidIdentifier("kv key: empty".to_string()));
    }
    let token = hex_encode(key.as_bytes());
    Subject::new(format!("{KV_SUBJECT_PREFIX}.{bucket}.{token}"))
}

/// Build the bucket-scan subject filter `noetl.kv.<bucket>.>`.
fn bucket_filter(bucket: &str) -> Result<SubjectFilter> {
    let bucket = validated_bucket(bucket)?;
    SubjectFilter::new(format!("{KV_SUBJECT_PREFIX}.{bucket}.>"))
}

/// The stored envelope for one KV write (the record payload).  Carries the
/// original (un-encoded) key so a scan reconstructs real keys without decoding
/// the subject, plus the value, monotonic per-key version, tombstone flag, and
/// optional absolute TTL.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct KvEnvelope {
    bucket: String,
    key: String,
    value: String,
    version: u64,
    deleted: bool,
    expires_at_ms: Option<u64>,
}

/// The caller's compare-and-swap expectation for a conditional [`KvPutRequest`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KvCasExpectation {
    /// The key must currently be absent (no live entry) — create-only.
    Absent,
    /// The key's current live version must equal this value.
    Version(u64),
}

/// Write a key.  When `cas` is set the write is conditional (optimistic).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KvPutRequest {
    pub bucket: String,
    pub key: String,
    /// The value (UTF-8; platform KV values are JSON snapshots).
    pub value: String,
    /// Absolute expiry the caller computed from its own clock, or `None` for no
    /// TTL.  Read as expired once `now_ms >= expires_at_ms`.
    pub expires_at_ms: Option<u64>,
    /// Optional compare-and-swap expectation; `None` = unconditional write.
    pub cas: Option<KvCasExpectation>,
    pub transaction_id: String,
}

/// Secret-free result of a put (no bucket/key/value ever reaches a metric label).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct KvPutOutcome {
    pub action: String,
    pub bucket: String,
    pub key: String,
    /// The per-key version after this write (== previous version + 1).  On a CAS
    /// conflict this is the *current* live version (0 when the key is absent).
    pub version: u64,
    /// Whether a record was appended (false on a CAS conflict).
    pub written: bool,
    /// Whether the write was refused by the CAS expectation.
    pub cas_conflict: bool,
    /// On a conflict, the key's current live version (`None` when absent).
    pub current_version: Option<u64>,
    /// Whether the canonical KV stream was created on this write.
    pub created_stream: bool,
    /// The global write-order sequence assigned to this write (0 on conflict).
    pub global_sequence: u64,
    pub byte_len: usize,
}

/// Read one key's latest live value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KvGetRequest {
    pub bucket: String,
    pub key: String,
    /// The caller's current time for TTL evaluation; `None` disables TTL
    /// filtering (a never-expiring read).
    pub now_ms: Option<u64>,
}

/// One live KV entry.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct KvEntryView {
    pub bucket: String,
    pub key: String,
    pub value: String,
    pub version: u64,
    pub expires_at_ms: Option<u64>,
}

/// Secret-free result of a get.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct KvGetOutcome {
    pub action: String,
    pub bucket: String,
    pub key: String,
    /// Whether a live (non-deleted, non-expired) entry was found.
    pub found: bool,
    /// Whether a value existed but was dropped because its TTL had passed.
    pub expired: bool,
    pub entry: Option<KvEntryView>,
}

/// Delete a key (append a tombstone).  Idempotent — deleting an absent key is a
/// no-op.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KvDeleteRequest {
    pub bucket: String,
    pub key: String,
    pub transaction_id: String,
}

/// Secret-free result of a delete.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct KvDeleteOutcome {
    pub action: String,
    pub bucket: String,
    pub key: String,
    /// Whether a live entry existed before this delete (false = idempotent no-op).
    pub existed: bool,
    /// The tombstone's version (== previous version + 1; 0 when nothing existed).
    pub version: u64,
    /// The global sequence assigned to the tombstone (0 on an idempotent no-op).
    pub global_sequence: u64,
}

/// Bounded scan of a bucket's live keys.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KvScanRequest {
    pub bucket: String,
    /// Optional prefix on the original (decoded) key.
    pub prefix: Option<String>,
    pub limit: usize,
    /// The caller's current time for TTL evaluation; `None` disables TTL
    /// filtering.
    pub now_ms: Option<u64>,
}

/// Secret-free result of a bucket scan.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct KvScanOutcome {
    pub action: String,
    pub bucket: String,
    /// Whether the KV stream exists yet (false before the first write anywhere).
    pub exists: bool,
    /// Live matching entries before `limit` is applied.
    pub match_count: usize,
    pub returned: usize,
    /// Entries ordered by key.
    pub entries: Vec<KvEntryView>,
}

/// The driver interface for the KV/state tier.  EHDB is one implementation
/// ([`LocalReferenceKvStateDriver`]); a NATS-KV driver implementing the same
/// trait keeps the tier selectable back to the incumbent (Phase 10).
///
/// All methods are `&self`: the durable state lives in the on-disk transaction
/// log, opened + dropped per op (bounded/stateless discipline).
pub trait KvStateDriver {
    /// A stable, secret-free identifier for the backing engine.
    fn driver_name(&self) -> &'static str;
    /// Write a key (optionally CAS-conditioned / TTL-stamped).
    fn put(&self, request: &KvPutRequest) -> Result<KvPutOutcome>;
    /// Read a key's latest live value.
    fn get(&self, request: &KvGetRequest) -> Result<KvGetOutcome>;
    /// Delete a key (tombstone; idempotent).
    fn delete(&self, request: &KvDeleteRequest) -> Result<KvDeleteOutcome>;
    /// Scan a bucket's live keys (bounded, ordered by key).
    fn scan(&self, request: &KvScanRequest) -> Result<KvScanOutcome>;
}

/// The EHDB KV engine over the bounded local-reference transaction log.
#[derive(Debug, Clone)]
pub struct LocalReferenceKvStateDriver {
    pub log_path: PathBuf,
    pub tenant: String,
    pub namespace: String,
}

impl LocalReferenceKvStateDriver {
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
            StreamName::new(KV_STATE_STREAM.to_string())?,
        ))
    }

    /// The latest record's envelope for one key (tombstone or live), or `None`
    /// when the key was never written / the stream does not exist yet.
    fn latest_envelope(
        &self,
        runtime: &LocalReferenceRuntime,
        tenant: &TenantId,
        namespace: &NamespaceName,
        stream: &StreamName,
        subject: &Subject,
    ) -> Option<KvEnvelope> {
        let filter = SubjectFilter::new(subject.as_str().to_string()).ok()?;
        let records = runtime
            .state()
            .streams
            .replay_matching(tenant, namespace, stream, &filter, None)
            .ok()?;
        records.last().and_then(decode_envelope)
    }
}

impl KvStateDriver for LocalReferenceKvStateDriver {
    fn driver_name(&self) -> &'static str {
        "ehdb-local-reference"
    }

    fn put(&self, request: &KvPutRequest) -> Result<KvPutOutcome> {
        let (tenant, namespace, stream) = self.coordinates()?;
        let bucket = validated_bucket(&request.bucket)?;
        let subject = key_subject(&request.bucket, &request.key)?;
        let byte_len = request.value.len();
        if byte_len > MAX_KV_VALUE_BYTES {
            return Err(EhdbError::InvalidState(format!(
                "kv value {byte_len} bytes exceeds bound {MAX_KV_VALUE_BYTES}"
            )));
        }
        let transaction_id = TransactionId::new(request.transaction_id.clone())?;

        let mut runtime = LocalReferenceRuntime::open(&self.log_path)?;

        let latest = self.latest_envelope(&runtime, &tenant, &namespace, &stream, &subject);
        // The current *live* version — a tombstone / absent key reads as no live
        // version, so a CAS `Absent` succeeds after a delete.
        let live_version = latest
            .as_ref()
            .filter(|env| !env.deleted)
            .map(|env| env.version);

        // Evaluate the CAS expectation before touching the log.
        if let Some(expectation) = request.cas {
            let satisfied = match expectation {
                KvCasExpectation::Absent => live_version.is_none(),
                KvCasExpectation::Version(expected) => live_version == Some(expected),
            };
            if !satisfied {
                return Ok(KvPutOutcome {
                    action: "kv-put".to_string(),
                    bucket,
                    key: request.key.clone(),
                    version: live_version.unwrap_or(0),
                    written: false,
                    cas_conflict: true,
                    current_version: live_version,
                    created_stream: false,
                    global_sequence: 0,
                    byte_len,
                });
            }
        }

        // Monotonic per-key version — advances across tombstones too, so a
        // delete → recreate strictly increases the version.
        let next_version = latest.as_ref().map(|env| env.version).unwrap_or(0) + 1;

        let (created_stream, next_sequence) =
            next_stream_write(&runtime, &tenant, &namespace, &stream);

        let envelope = KvEnvelope {
            bucket: bucket.clone(),
            key: request.key.clone(),
            value: request.value.clone(),
            version: next_version,
            deleted: false,
            expires_at_ms: request.expires_at_ms,
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

        Ok(KvPutOutcome {
            action: "kv-put".to_string(),
            bucket,
            key: request.key.clone(),
            version: next_version,
            written: true,
            cas_conflict: false,
            current_version: Some(next_version),
            created_stream,
            global_sequence: next_sequence,
            byte_len,
        })
    }

    fn get(&self, request: &KvGetRequest) -> Result<KvGetOutcome> {
        let (tenant, namespace, stream) = self.coordinates()?;
        let subject = key_subject(&request.bucket, &request.key)?;
        let bucket = validated_bucket(&request.bucket)?;
        let runtime = LocalReferenceRuntime::open(&self.log_path)?;

        let latest = self.latest_envelope(&runtime, &tenant, &namespace, &stream, &subject);
        let absent = || KvGetOutcome {
            action: "kv-get".to_string(),
            bucket: bucket.clone(),
            key: request.key.clone(),
            found: false,
            expired: false,
            entry: None,
        };

        let Some(env) = latest else {
            return Ok(absent());
        };
        if env.deleted {
            return Ok(absent());
        }
        if is_expired(env.expires_at_ms, request.now_ms) {
            return Ok(KvGetOutcome {
                action: "kv-get".to_string(),
                bucket,
                key: request.key.clone(),
                found: false,
                expired: true,
                entry: None,
            });
        }

        Ok(KvGetOutcome {
            action: "kv-get".to_string(),
            bucket,
            key: request.key.clone(),
            found: true,
            expired: false,
            entry: Some(entry_view(env)),
        })
    }

    fn delete(&self, request: &KvDeleteRequest) -> Result<KvDeleteOutcome> {
        let (tenant, namespace, stream) = self.coordinates()?;
        let bucket = validated_bucket(&request.bucket)?;
        let subject = key_subject(&request.bucket, &request.key)?;
        let transaction_id = TransactionId::new(request.transaction_id.clone())?;

        let mut runtime = LocalReferenceRuntime::open(&self.log_path)?;
        let latest = self.latest_envelope(&runtime, &tenant, &namespace, &stream, &subject);

        // Idempotent: an absent key (never written, or already a tombstone) does
        // not append a second tombstone.
        let Some(current) = latest.as_ref().filter(|env| !env.deleted) else {
            return Ok(KvDeleteOutcome {
                action: "kv-delete".to_string(),
                bucket,
                key: request.key.clone(),
                existed: false,
                version: 0,
                global_sequence: 0,
            });
        };

        let next_version = current.version + 1;
        let (created_stream, next_sequence) =
            next_stream_write(&runtime, &tenant, &namespace, &stream);
        let envelope = KvEnvelope {
            bucket: bucket.clone(),
            key: request.key.clone(),
            value: String::new(),
            version: next_version,
            deleted: true,
            expires_at_ms: None,
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

        Ok(KvDeleteOutcome {
            action: "kv-delete".to_string(),
            bucket,
            key: request.key.clone(),
            existed: true,
            version: next_version,
            global_sequence: next_sequence,
        })
    }

    fn scan(&self, request: &KvScanRequest) -> Result<KvScanOutcome> {
        let (tenant, namespace, stream) = self.coordinates()?;
        let bucket = validated_bucket(&request.bucket)?;
        let filter = bucket_filter(&request.bucket)?;
        let limit = request.limit.min(MAX_KV_SCAN_LIMIT);
        let runtime = LocalReferenceRuntime::open(&self.log_path)?;

        let records = match runtime
            .state()
            .streams
            .replay_matching(&tenant, &namespace, &stream, &filter, None)
        {
            Ok(records) => records,
            // A missing stream (nothing written anywhere yet) is an absent probe.
            Err(_) => {
                return Ok(KvScanOutcome {
                    action: "kv-scan".to_string(),
                    bucket,
                    exists: false,
                    match_count: 0,
                    returned: 0,
                    entries: Vec::new(),
                });
            }
        };

        // Fold to the latest envelope per key (records replay in sequence order,
        // so a later record overwrites an earlier one).
        let mut latest_by_key: BTreeMap<String, KvEnvelope> = BTreeMap::new();
        for record in records {
            if let Some(env) = decode_envelope(&record) {
                latest_by_key.insert(env.key.clone(), env);
            }
        }

        let mut entries: Vec<KvEntryView> = latest_by_key
            .into_values()
            .filter(|env| !env.deleted)
            .filter(|env| !is_expired(env.expires_at_ms, request.now_ms))
            .filter(|env| match &request.prefix {
                Some(prefix) => env.key.starts_with(prefix),
                None => true,
            })
            .map(entry_view)
            .collect();
        entries.sort_by(|a, b| a.key.cmp(&b.key));

        let match_count = entries.len();
        entries.truncate(limit);

        Ok(KvScanOutcome {
            action: "kv-scan".to_string(),
            bucket,
            exists: true,
            match_count,
            returned: entries.len(),
            entries,
        })
    }
}

/// The next (created_stream, sequence) for a write to the KV stream — matches the
/// event-log engine: a missing stream replays as an error (the create signal),
/// and `next = count + 1` keeps the write-order sequence monotonic + gapless.
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
    envelope: &KvEnvelope,
    created_stream: bool,
    sequence: u64,
) -> Result<()> {
    let payload = serde_json::to_vec(envelope)
        .map_err(|err| EhdbError::InvalidState(format!("kv envelope encode: {err}")))?;

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

fn decode_envelope(record: &StreamRecord) -> Option<KvEnvelope> {
    serde_json::from_slice(&record.payload).ok()
}

fn entry_view(env: KvEnvelope) -> KvEntryView {
    KvEntryView {
        bucket: env.bucket,
        key: env.key,
        value: env.value,
        version: env.version,
        expires_at_ms: env.expires_at_ms,
    }
}

fn is_expired(expires_at_ms: Option<u64>, now_ms: Option<u64>) -> bool {
    match (expires_at_ms, now_ms) {
        (Some(expiry), Some(now)) => now >= expiry,
        _ => false,
    }
}

/// The authoritative NATS-KV view of one key, for the shadow parity check.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthoritativeKvEntry {
    pub value: String,
    pub expires_at_ms: Option<u64>,
}

/// The parity verdict of one shadow write: did the EHDB engine's view of a key
/// match the authoritative NATS-KV view?  Pure + secret-free so the engine tests
/// and the worker's disabled-by-default shadow mode share one comparison.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct KvParityReport {
    /// Both stores agree on whether the key is present.
    pub present_ok: bool,
    /// When both present, their values are byte-equal.
    pub value_ok: bool,
    /// When both present, their TTLs agree.
    pub ttl_ok: bool,
    /// The single reason parity failed, or `None` when it holds.
    pub divergence: Option<String>,
}

impl KvParityReport {
    /// Whether every parity check held.
    pub fn holds(&self) -> bool {
        self.present_ok && self.value_ok && self.ttl_ok && self.divergence.is_none()
    }
}

/// Compare the EHDB engine's `get` result against the authoritative NATS-KV view.
///
/// * `authoritative` — what the incumbent NATS-KV bucket holds for the key
///   (`None` = absent there).
/// * `ehdb` — the EHDB engine's [`KvGetOutcome`] for the same key.
///
/// Returns the first divergence found, or a clean report.
pub fn compare_kv_parity(
    authoritative: Option<&AuthoritativeKvEntry>,
    ehdb: &KvGetOutcome,
) -> KvParityReport {
    let present_ok = authoritative.is_some() == ehdb.found;

    let (value_ok, ttl_ok) = match (authoritative, ehdb.entry.as_ref()) {
        (Some(auth), Some(entry)) => (
            auth.value == entry.value,
            auth.expires_at_ms == entry.expires_at_ms,
        ),
        // When either side is absent there is no value/TTL to contradict; the
        // presence check carries the verdict.
        _ => (true, true),
    };

    let divergence = if !present_ok {
        Some(format!(
            "presence divergence: authoritative_present={} ehdb_found={}",
            authoritative.is_some(),
            ehdb.found
        ))
    } else if !value_ok {
        Some("value divergence: authoritative value != ehdb value".to_string())
    } else if !ttl_ok {
        Some("ttl divergence: authoritative expiry != ehdb expiry".to_string())
    } else {
        None
    };

    KvParityReport {
        present_ok,
        value_ok,
        ttl_ok,
        divergence,
    }
}

// ===========================================================================
// Primary-serve (completion program Phase 9, tier 3) — EHDB serves the platform
// KV/state tier authoritatively in place of the internal NATS-KV bucket.
//
// Tiers 1 (event log) and 2 (projection) proved the per-tier cutover pattern: an
// authoritative serving cycle that drives every capability through the EHDB
// engine while dual-run parity-checking the served results against the incumbent,
// plus a fresh-engine replay proving the store stays whole (reversibility).  This
// is the KV mirror of that pattern — the serving legs are the KV capabilities
// (put → get → scan → CAS → delete → TTL) instead of the event-log ack cursor or
// the projection read-models, and the incumbent is the internal NATS-KV bucket.
//
// ## Reversibility (the safety property the cutover is gated on)
//
// The cycle appends only to the EHDB KV stream ([`RetentionPolicy::KeepAll`]) and
// never touches the incumbent NATS-KV bucket.  Flipping a caller back from
// `primary` to `shadow`/`off` therefore restores NATS-KV as the authoritative KV
// path with zero data loss — the EHDB store stays intact on disk (a later
// re-enable replays it whole) and NATS-KV was never written.  [`exercise_primary_serve`]
// proves the "EHDB store stays intact" half directly via the fresh-driver replay
// leg; the "NATS-KV untouched" half is a structural property of the caller (the
// worker asserts it by never importing a NATS-KV writer).
// ===========================================================================

/// The KV drive served authoritatively through one primary-serve cycle: the
/// bucket, the seed key/value entries, and the caller's clock for the TTL leg.
///
/// The cycle puts every entry, serves a `get` of each (dual-run parity-checked
/// against an in-lockstep NATS-KV mirror), scans the bucket, CAS-swaps the first
/// key + refuses a create-only conflict on it, deletes the last key, exercises an
/// absolute-TTL lease, and finally replays a fresh driver over the same store.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KvPrimaryInput {
    pub bucket: String,
    /// Distinct key/value entries seeded into the tier.  At least two are
    /// required so CAS (on the first) and delete (on the last) act on distinct
    /// keys ([`EhdbError::InvalidState`] otherwise).
    pub entries: Vec<(String, String)>,
    /// The caller's clock for the TTL leg: a lease key is written with expiry
    /// `now_ms + 1`, read live at `now_ms`, and read expired at `now_ms + 1`.
    /// The final replay scan runs at `now_ms + 1` so the expired lease is
    /// filtered out, leaving exactly the durable live set.
    pub now_ms: u64,
}

/// The lease key the TTL leg writes + expires.  Never added to the NATS-KV mirror
/// (it is expired by the replay scan's clock), so it never affects parity.
const PRIMARY_SERVE_LEASE_KEY: &str = "__ehdb_primary_lease__";

/// The served-by-EHDB proof for one KV primary-serve cycle: every serving leg ran
/// through the engine and preserved the NATS-KV semantics (last-writer-wins get,
/// bucket scan, optimistic CAS, tombstone delete, absolute TTL), and each served
/// read held dual-run parity against the NATS-KV mirror.  Secret-free (counts +
/// verdicts; the parity reports carry no values).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct KvPrimaryServeReport {
    /// The backing engine that served the cycle.
    pub driver_name: String,
    /// How many entries the cycle wrote authoritatively.
    pub put_count: usize,
    /// Every seed put was written.
    pub put_ok: bool,
    /// Every served `get` found the key and held parity vs the NATS-KV mirror.
    pub get_ok: bool,
    /// The served bucket scan returned exactly the live NATS-KV mirror.
    pub scan_ok: bool,
    pub scan_live: usize,
    /// A versioned CAS swap was served + a stale create-only write was a conflict
    /// (no append), and the post-swap read held parity.
    pub cas_ok: bool,
    /// A served tombstone made the key read absent (parity vs the mirror's None).
    pub delete_ok: bool,
    /// A served absolute-TTL lease read live before expiry and absent after.
    pub ttl_ok: bool,
    /// A fresh driver over the same on-disk store served the identical live set
    /// (replay-is-truth / durability — the reversibility half proven directly).
    pub replay_live: usize,
    pub replay_matches: bool,
    /// Per-served-read dual-run parity verdicts against the NATS-KV mirror.
    pub dual_run: Vec<KvParityReport>,
    /// Every dual-run parity verdict held.
    pub dual_run_holds: bool,
    /// The single reason the cycle failed a served-by-EHDB invariant, or `None`.
    pub divergence: Option<String>,
}

impl KvPrimaryServeReport {
    /// Whether the EHDB engine served the whole cycle with the NATS-KV semantics
    /// preserved and dual-run parity intact.
    pub fn served_by_ehdb(&self) -> bool {
        self.put_ok
            && self.get_ok
            && self.scan_ok
            && self.cas_ok
            && self.delete_ok
            && self.ttl_ok
            && self.replay_matches
            && self.dual_run_holds
            && self.divergence.is_none()
    }
}

/// Run the authoritative KV primary-serve cycle over `driver`.
///
/// Drives every serving leg through the EHDB engine — put, per-key served `get`,
/// bucket `scan`, optimistic `CAS` (versioned swap + create-only conflict),
/// tombstone `delete`, absolute-TTL lease, and a fresh-driver replay — asserting
/// the NATS-KV semantics are preserved and dual-run parity-checking each served
/// read against a NATS-KV mirror applied in lockstep with identical
/// last-writer-wins semantics.  Returns the [`KvPrimaryServeReport`]
/// served-by-EHDB proof.
///
/// Reversible + non-destructive toward the incumbent: appends only to the EHDB KV
/// stream ([`RetentionPolicy::KeepAll`]); the replay leg proves the store stays
/// whole so a flip back to NATS-KV loses nothing.
///
/// `input.entries` must hold at least two entries whose first and last keys
/// differ ([`EhdbError::InvalidState`] otherwise).  `transaction_prefix` scopes
/// the per-write transaction ids.
pub fn exercise_primary_serve(
    driver: &LocalReferenceKvStateDriver,
    input: &KvPrimaryInput,
    transaction_prefix: &str,
) -> Result<KvPrimaryServeReport> {
    if input.entries.len() < 2 {
        return Err(EhdbError::InvalidState(
            "kv primary-serve requires at least two entries".to_string(),
        ));
    }
    let first_key = input.entries.first().unwrap().0.clone();
    let last_key = input.entries.last().unwrap().0.clone();
    if first_key == last_key {
        return Err(EhdbError::InvalidState(
            "kv primary-serve requires the first and last keys to differ".to_string(),
        ));
    }
    let bucket = input.bucket.clone();

    // The authoritative NATS-KV mirror the served reads are dual-run
    // parity-checked against — applied in lockstep with identical LWW semantics.
    let mut auth: BTreeMap<String, AuthoritativeKvEntry> = BTreeMap::new();
    let mut dual_run: Vec<KvParityReport> = Vec::new();
    let mut txn = 0u64;
    let mut next_txn = || {
        txn += 1;
        format!("{transaction_prefix}-{txn}")
    };

    // --- Put leg: EHDB serves the authoritative write. -----------------------
    let mut put_ok = true;
    for (k, v) in &input.entries {
        let out = driver.put(&KvPutRequest {
            bucket: bucket.clone(),
            key: k.clone(),
            value: v.clone(),
            expires_at_ms: None,
            cas: None,
            transaction_id: next_txn(),
        })?;
        put_ok &= out.written;
        auth.insert(
            k.clone(),
            AuthoritativeKvEntry {
                value: v.clone(),
                expires_at_ms: None,
            },
        );
    }
    let put_count = input.entries.len();

    // --- Get leg: served reads, dual-run parity per key vs the NATS-KV mirror.
    let mut get_ok = true;
    for (k, _) in &input.entries {
        let got = driver.get(&KvGetRequest {
            bucket: bucket.clone(),
            key: k.clone(),
            now_ms: None,
        })?;
        let report = compare_kv_parity(auth.get(k), &got);
        get_ok &= got.found && report.holds();
        dual_run.push(report);
    }

    // --- Scan leg: the served bucket scan returns exactly the live mirror. ----
    let scan = driver.scan(&KvScanRequest {
        bucket: bucket.clone(),
        prefix: None,
        limit: MAX_KV_SCAN_LIMIT,
        now_ms: None,
    })?;
    let scan_live = scan.match_count;
    let live_pairs = |m: &BTreeMap<String, AuthoritativeKvEntry>| -> BTreeMap<String, String> {
        m.iter()
            .map(|(k, e)| (k.clone(), e.value.clone()))
            .collect()
    };
    let scanned: BTreeMap<String, String> = scan
        .entries
        .iter()
        .map(|e| (e.key.clone(), e.value.clone()))
        .collect();
    let scan_ok = scan.exists && scanned == live_pairs(&auth);

    // --- CAS leg: served optimistic write — versioned swap succeeds, a stale
    //     create-only write is a conflict (no append).
    let current = driver.get(&KvGetRequest {
        bucket: bucket.clone(),
        key: first_key.clone(),
        now_ms: None,
    })?;
    let current_version = current.entry.as_ref().map(|e| e.version).unwrap_or(0);
    let swapped_value = format!(
        "{}::cas",
        auth.get(&first_key).map(|e| e.value.as_str()).unwrap_or("")
    );
    let swap = driver.put(&KvPutRequest {
        bucket: bucket.clone(),
        key: first_key.clone(),
        value: swapped_value.clone(),
        expires_at_ms: None,
        cas: Some(KvCasExpectation::Version(current_version)),
        transaction_id: next_txn(),
    })?;
    if swap.written {
        auth.insert(
            first_key.clone(),
            AuthoritativeKvEntry {
                value: swapped_value.clone(),
                expires_at_ms: None,
            },
        );
    }
    let conflict = driver.put(&KvPutRequest {
        bucket: bucket.clone(),
        key: first_key.clone(),
        value: "conflict".to_string(),
        expires_at_ms: None,
        cas: Some(KvCasExpectation::Absent),
        transaction_id: next_txn(),
    })?;
    let after_cas = driver.get(&KvGetRequest {
        bucket: bucket.clone(),
        key: first_key.clone(),
        now_ms: None,
    })?;
    let cas_parity = compare_kv_parity(auth.get(&first_key), &after_cas);
    let cas_ok = swap.written
        && swap.version == current_version + 1
        && conflict.cas_conflict
        && !conflict.written
        && cas_parity.holds();
    dual_run.push(cas_parity);

    // --- Delete leg: a served tombstone makes the key read absent. -----------
    let del = driver.delete(&KvDeleteRequest {
        bucket: bucket.clone(),
        key: last_key.clone(),
        transaction_id: next_txn(),
    })?;
    auth.remove(&last_key);
    let after_del = driver.get(&KvGetRequest {
        bucket: bucket.clone(),
        key: last_key.clone(),
        now_ms: None,
    })?;
    let del_parity = compare_kv_parity(auth.get(&last_key), &after_del);
    let delete_ok = del.existed && !after_del.found && del_parity.holds();
    dual_run.push(del_parity);

    // --- TTL leg: served absolute-expiry read — live before, absent after. ---
    driver.put(&KvPutRequest {
        bucket: bucket.clone(),
        key: PRIMARY_SERVE_LEASE_KEY.to_string(),
        value: "lease".to_string(),
        expires_at_ms: Some(input.now_ms + 1),
        cas: None,
        transaction_id: next_txn(),
    })?;
    let before = driver.get(&KvGetRequest {
        bucket: bucket.clone(),
        key: PRIMARY_SERVE_LEASE_KEY.to_string(),
        now_ms: Some(input.now_ms),
    })?;
    let after = driver.get(&KvGetRequest {
        bucket: bucket.clone(),
        key: PRIMARY_SERVE_LEASE_KEY.to_string(),
        now_ms: Some(input.now_ms + 1),
    })?;
    let ttl_ok = before.found && !after.found && after.expired;

    // --- Replay leg: a fresh driver over the same store reconstructs the live
    // set.  Scanned at `now_ms + 1` so the expired lease is filtered → exactly
    // the durable live mirror (the durability / reversibility half proven
    // directly).
    let replay_driver = driver.clone();
    let replay = replay_driver.scan(&KvScanRequest {
        bucket: bucket.clone(),
        prefix: None,
        limit: MAX_KV_SCAN_LIMIT,
        now_ms: Some(input.now_ms + 1),
    })?;
    let replay_live = replay.match_count;
    let replayed: BTreeMap<String, String> = replay
        .entries
        .iter()
        .map(|e| (e.key.clone(), e.value.clone()))
        .collect();
    let replay_matches = replay.exists && replayed == live_pairs(&auth);

    let dual_run_holds = dual_run.iter().all(KvParityReport::holds);

    let divergence = if !put_ok {
        Some("primary put leg did not write every entry".to_string())
    } else if !get_ok {
        Some("primary get leg lost a key or diverged from the NATS-KV mirror".to_string())
    } else if !scan_ok {
        Some("primary scan served the wrong live set".to_string())
    } else if !cas_ok {
        Some(format!(
            "primary CAS leg failed: swap_written={} swap_version={} expected={} conflict={}",
            swap.written,
            swap.version,
            current_version + 1,
            conflict.cas_conflict
        ))
    } else if !delete_ok {
        Some("primary delete leg did not tombstone the key".to_string())
    } else if !ttl_ok {
        Some("primary TTL leg did not expire the lease".to_string())
    } else if !replay_matches {
        Some(format!(
            "primary replay lost the live set: replayed {replay_live} keys"
        ))
    } else if !dual_run_holds {
        dual_run
            .iter()
            .find_map(|r| r.divergence.clone())
            .or_else(|| Some("primary dual-run parity diverged".to_string()))
    } else {
        None
    };

    Ok(KvPrimaryServeReport {
        driver_name: driver.driver_name().to_string(),
        put_count,
        put_ok,
        get_ok,
        scan_ok,
        scan_live,
        cas_ok,
        delete_ok,
        ttl_ok,
        replay_live,
        replay_matches,
        dual_run,
        dual_run_holds,
        divergence,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tmp_log(tag: &str) -> (PathBuf, PathBuf) {
        let dir = std::env::temp_dir().join(format!(
            "ehdb-kv-{tag}-{}-{:?}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        (dir.join("log.jsonl"), dir)
    }

    fn driver(log: &std::path::Path) -> LocalReferenceKvStateDriver {
        LocalReferenceKvStateDriver::new(log, "noetl", "default")
    }

    fn put(
        d: &LocalReferenceKvStateDriver,
        bucket: &str,
        key: &str,
        value: &str,
        n: u64,
    ) -> KvPutOutcome {
        d.put(&KvPutRequest {
            bucket: bucket.to_string(),
            key: key.to_string(),
            value: value.to_string(),
            expires_at_ms: None,
            cas: None,
            transaction_id: format!("txn-{n}"),
        })
        .unwrap()
    }

    fn get(d: &LocalReferenceKvStateDriver, bucket: &str, key: &str) -> KvGetOutcome {
        d.get(&KvGetRequest {
            bucket: bucket.to_string(),
            key: key.to_string(),
            now_ms: None,
        })
        .unwrap()
    }

    #[test]
    fn put_get_round_trips_latest_value() {
        let (log, dir) = tmp_log("put-get");
        let d = driver(&log);
        let bucket = "noetl_subscription_circuit";
        let first = put(&d, bucket, "circuit.12345", "{\"phase\":\"closed\"}", 1);
        assert!(first.written);
        assert_eq!(first.version, 1);
        assert!(first.created_stream);
        // Overwrite with a newer value → version advances, get returns latest.
        let second = put(&d, bucket, "circuit.12345", "{\"phase\":\"open\"}", 2);
        assert_eq!(second.version, 2);
        assert!(!second.created_stream);
        let got = get(&d, bucket, "circuit.12345");
        assert!(got.found);
        let entry = got.entry.unwrap();
        assert_eq!(entry.value, "{\"phase\":\"open\"}");
        assert_eq!(entry.version, 2);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn get_of_absent_key_is_not_found_not_error() {
        let (log, dir) = tmp_log("absent");
        let d = driver(&log);
        let got = get(&d, "noetl_subscription_circuit", "circuit.does-not-exist");
        assert!(!got.found);
        assert!(!got.expired);
        assert!(got.entry.is_none());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn delete_tombstones_then_get_is_absent_and_idempotent() {
        let (log, dir) = tmp_log("delete");
        let d = driver(&log);
        let bucket = "noetl_subscription_circuit";
        put(&d, bucket, "circuit.1", "v1", 1);
        let del = d
            .delete(&KvDeleteRequest {
                bucket: bucket.to_string(),
                key: "circuit.1".to_string(),
                transaction_id: "txn-del-1".to_string(),
            })
            .unwrap();
        assert!(del.existed);
        assert_eq!(del.version, 2);
        assert!(!get(&d, bucket, "circuit.1").found);
        // Second delete is an idempotent no-op (no new tombstone).
        let del2 = d
            .delete(&KvDeleteRequest {
                bucket: bucket.to_string(),
                key: "circuit.1".to_string(),
                transaction_id: "txn-del-2".to_string(),
            })
            .unwrap();
        assert!(!del2.existed);
        assert_eq!(del2.global_sequence, 0);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn put_after_delete_recreates_with_monotonic_version() {
        let (log, dir) = tmp_log("recreate");
        let d = driver(&log);
        let bucket = "b";
        put(&d, bucket, "k", "v1", 1); // version 1
        d.delete(&KvDeleteRequest {
            bucket: bucket.to_string(),
            key: "k".to_string(),
            transaction_id: "txn-del".to_string(),
        })
        .unwrap(); // tombstone version 2
        let recreated = put(&d, bucket, "k", "v2", 2);
        assert_eq!(recreated.version, 3);
        let got = get(&d, bucket, "k");
        assert!(got.found);
        assert_eq!(got.entry.unwrap().value, "v2");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn scan_returns_live_keys_ordered_dropping_tombstones() {
        let (log, dir) = tmp_log("scan");
        let d = driver(&log);
        let bucket = "b";
        put(&d, bucket, "circuit.a", "1", 1);
        put(&d, bucket, "circuit.b", "2", 2);
        put(&d, bucket, "other.c", "3", 3);
        d.delete(&KvDeleteRequest {
            bucket: bucket.to_string(),
            key: "circuit.b".to_string(),
            transaction_id: "txn-del".to_string(),
        })
        .unwrap();
        let scan = d
            .scan(&KvScanRequest {
                bucket: bucket.to_string(),
                prefix: None,
                limit: 100,
                now_ms: None,
            })
            .unwrap();
        assert!(scan.exists);
        // circuit.b tombstoned out; circuit.a + other.c live, ordered by key.
        let keys: Vec<&str> = scan.entries.iter().map(|e| e.key.as_str()).collect();
        assert_eq!(keys, vec!["circuit.a", "other.c"]);
        // Prefix scan narrows to the circuit.* live keys.
        let pref = d
            .scan(&KvScanRequest {
                bucket: bucket.to_string(),
                prefix: Some("circuit.".to_string()),
                limit: 100,
                now_ms: None,
            })
            .unwrap();
        let pkeys: Vec<&str> = pref.entries.iter().map(|e| e.key.as_str()).collect();
        assert_eq!(pkeys, vec!["circuit.a"]);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn scan_is_bounded_by_limit() {
        let (log, dir) = tmp_log("scan-limit");
        let d = driver(&log);
        let bucket = "b";
        for i in 0..5 {
            put(&d, bucket, &format!("k{i}"), "v", i);
        }
        let scan = d
            .scan(&KvScanRequest {
                bucket: bucket.to_string(),
                prefix: None,
                limit: 2,
                now_ms: None,
            })
            .unwrap();
        assert_eq!(scan.match_count, 5);
        assert_eq!(scan.returned, 2);
        // Ordered by key, so the first two are k0, k1.
        assert_eq!(scan.entries[0].key, "k0");
        assert_eq!(scan.entries[1].key, "k1");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn cas_absent_creates_only_when_absent() {
        let (log, dir) = tmp_log("cas-absent");
        let d = driver(&log);
        let bucket = "b";
        // Create-only when absent → succeeds.
        let created = d
            .put(&KvPutRequest {
                bucket: bucket.to_string(),
                key: "k".to_string(),
                value: "v1".to_string(),
                expires_at_ms: None,
                cas: Some(KvCasExpectation::Absent),
                transaction_id: "txn-1".to_string(),
            })
            .unwrap();
        assert!(created.written);
        // Create-only again → conflict (key now present).
        let conflict = d
            .put(&KvPutRequest {
                bucket: bucket.to_string(),
                key: "k".to_string(),
                value: "v2".to_string(),
                expires_at_ms: None,
                cas: Some(KvCasExpectation::Absent),
                transaction_id: "txn-2".to_string(),
            })
            .unwrap();
        assert!(!conflict.written);
        assert!(conflict.cas_conflict);
        assert_eq!(conflict.current_version, Some(1));
        // The value is unchanged.
        assert_eq!(get(&d, bucket, "k").entry.unwrap().value, "v1");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn cas_version_swaps_only_on_expected_version() {
        let (log, dir) = tmp_log("cas-version");
        let d = driver(&log);
        let bucket = "b";
        put(&d, bucket, "k", "v1", 1); // version 1
                                       // Expect version 1 → swap succeeds, becomes version 2.
        let ok = d
            .put(&KvPutRequest {
                bucket: bucket.to_string(),
                key: "k".to_string(),
                value: "v2".to_string(),
                expires_at_ms: None,
                cas: Some(KvCasExpectation::Version(1)),
                transaction_id: "txn-2".to_string(),
            })
            .unwrap();
        assert!(ok.written);
        assert_eq!(ok.version, 2);
        // Expect the stale version 1 again → conflict.
        let conflict = d
            .put(&KvPutRequest {
                bucket: bucket.to_string(),
                key: "k".to_string(),
                value: "v3".to_string(),
                expires_at_ms: None,
                cas: Some(KvCasExpectation::Version(1)),
                transaction_id: "txn-3".to_string(),
            })
            .unwrap();
        assert!(conflict.cas_conflict);
        assert_eq!(conflict.current_version, Some(2));
        assert_eq!(get(&d, bucket, "k").entry.unwrap().value, "v2");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn ttl_expired_key_reads_absent() {
        let (log, dir) = tmp_log("ttl");
        let d = driver(&log);
        let bucket = "b";
        d.put(&KvPutRequest {
            bucket: bucket.to_string(),
            key: "k".to_string(),
            value: "v".to_string(),
            expires_at_ms: Some(1_000),
            cas: None,
            transaction_id: "txn-1".to_string(),
        })
        .unwrap();
        // Before expiry → found.
        let before = d
            .get(&KvGetRequest {
                bucket: bucket.to_string(),
                key: "k".to_string(),
                now_ms: Some(999),
            })
            .unwrap();
        assert!(before.found);
        // At/after expiry → absent + expired flagged.
        let after = d
            .get(&KvGetRequest {
                bucket: bucket.to_string(),
                key: "k".to_string(),
                now_ms: Some(1_000),
            })
            .unwrap();
        assert!(!after.found);
        assert!(after.expired);
        // Scan with now past expiry drops it too.
        let scan = d
            .scan(&KvScanRequest {
                bucket: bucket.to_string(),
                prefix: None,
                limit: 100,
                now_ms: Some(2_000),
            })
            .unwrap();
        assert_eq!(scan.match_count, 0);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn buckets_and_keys_are_scope_isolated() {
        let (log, dir) = tmp_log("scope");
        let d = driver(&log);
        // Same key in two buckets does not collide.
        put(&d, "bucket_a", "k", "a-value", 1);
        put(&d, "bucket_b", "k", "b-value", 2);
        assert_eq!(get(&d, "bucket_a", "k").entry.unwrap().value, "a-value");
        assert_eq!(get(&d, "bucket_b", "k").entry.unwrap().value, "b-value");
        // A scan of one bucket never sees the other's keys.
        let scan_a = d
            .scan(&KvScanRequest {
                bucket: "bucket_a".to_string(),
                prefix: None,
                limit: 100,
                now_ms: None,
            })
            .unwrap();
        assert_eq!(scan_a.entries.len(), 1);
        assert_eq!(scan_a.entries[0].value, "a-value");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn replay_reconstructs_from_log_alone() {
        let (log, dir) = tmp_log("replay");
        {
            let d = driver(&log);
            put(&d, "b", "k1", "v1", 1);
            put(&d, "b", "k2", "v2", 2);
        }
        // A fresh driver over the same log path replays the same state.
        let d2 = driver(&log);
        assert_eq!(get(&d2, "b", "k1").entry.unwrap().value, "v1");
        let scan = d2
            .scan(&KvScanRequest {
                bucket: "b".to_string(),
                prefix: None,
                limit: 100,
                now_ms: None,
            })
            .unwrap();
        assert_eq!(scan.match_count, 2);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn oversized_value_is_rejected_bound() {
        let (log, dir) = tmp_log("oversize");
        let d = driver(&log);
        let big = "x".repeat(MAX_KV_VALUE_BYTES + 1);
        let err = d
            .put(&KvPutRequest {
                bucket: "b".to_string(),
                key: "k".to_string(),
                value: big,
                expires_at_ms: None,
                cas: None,
                transaction_id: "txn-1".to_string(),
            })
            .unwrap_err();
        assert!(err.to_string().contains("exceeds bound"));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn invalid_bucket_and_key_are_invalid_identifier() {
        let (log, dir) = tmp_log("badid");
        let d = driver(&log);
        // A bucket with a dot would split across subject tokens.
        let bad_bucket = d
            .put(&KvPutRequest {
                bucket: "bad.bucket".to_string(),
                key: "k".to_string(),
                value: "v".to_string(),
                expires_at_ms: None,
                cas: None,
                transaction_id: "txn".to_string(),
            })
            .unwrap_err();
        assert!(bad_bucket.to_string().starts_with("invalid identifier"));
        // An empty key is invalid.
        let bad_key = d
            .put(&KvPutRequest {
                bucket: "b".to_string(),
                key: String::new(),
                value: "v".to_string(),
                expires_at_ms: None,
                cas: None,
                transaction_id: "txn".to_string(),
            })
            .unwrap_err();
        assert!(bad_key.to_string().starts_with("invalid identifier"));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn keys_with_dots_and_slashes_round_trip() {
        let (log, dir) = tmp_log("hexkeys");
        let d = driver(&log);
        // Real NoETL KV keys carry `.` and `/`; the hex subject token round-trips.
        for (n, key) in ["circuit.12345", "chain/head/exec-9", "a.b.c/d_e-f"]
            .into_iter()
            .enumerate()
        {
            put(&d, "b", key, "v", n as u64);
            assert!(get(&d, "b", key).found, "key {key} should round-trip");
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn parity_holds_when_stores_agree() {
        let (log, dir) = tmp_log("parity-ok");
        let d = driver(&log);
        put(&d, "b", "k", "v", 1);
        let ehdb = get(&d, "b", "k");
        let auth = AuthoritativeKvEntry {
            value: "v".to_string(),
            expires_at_ms: None,
        };
        let report = compare_kv_parity(Some(&auth), &ehdb);
        assert!(report.holds(), "{report:?}");
        // Both absent also holds.
        let ehdb_absent = get(&d, "b", "missing");
        let report_absent = compare_kv_parity(None, &ehdb_absent);
        assert!(report_absent.holds());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn parity_flags_presence_value_and_ttl_divergence() {
        let (log, dir) = tmp_log("parity-bad");
        let d = driver(&log);
        put(&d, "b", "k", "v", 1);
        let ehdb = get(&d, "b", "k");

        // Authoritative absent but EHDB present → presence divergence.
        let present = compare_kv_parity(None, &ehdb);
        assert!(!present.holds());
        assert!(present.divergence.unwrap().contains("presence divergence"));

        // Value mismatch.
        let value = compare_kv_parity(
            Some(&AuthoritativeKvEntry {
                value: "other".to_string(),
                expires_at_ms: None,
            }),
            &ehdb,
        );
        assert!(!value.value_ok);
        assert!(value.divergence.unwrap().contains("value divergence"));

        // TTL mismatch.
        let ttl = compare_kv_parity(
            Some(&AuthoritativeKvEntry {
                value: "v".to_string(),
                expires_at_ms: Some(5_000),
            }),
            &ehdb,
        );
        assert!(!ttl.ttl_ok);
        assert!(ttl.divergence.unwrap().contains("ttl divergence"));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn driver_name_is_stable() {
        let (log, dir) = tmp_log("name");
        let d = driver(&log);
        assert_eq!(d.driver_name(), "ehdb-local-reference");
        let _ = std::fs::remove_dir_all(&dir);
    }

    fn primary_input() -> KvPrimaryInput {
        KvPrimaryInput {
            bucket: "noetl_kv_primary".to_string(),
            entries: vec![
                (
                    "circuit.1".to_string(),
                    "{\"phase\":\"closed\"}".to_string(),
                ),
                ("circuit.2".to_string(), "{\"phase\":\"open\"}".to_string()),
                ("circuit.3".to_string(), "{\"phase\":\"half\"}".to_string()),
            ],
            now_ms: 1_000,
        }
    }

    #[test]
    fn primary_serve_cycle_is_served_by_ehdb() {
        let (log, dir) = tmp_log("primary-served");
        let d = driver(&log);
        let report = exercise_primary_serve(&d, &primary_input(), "primary-t3").unwrap();
        assert!(report.served_by_ehdb(), "{report:?}");
        assert_eq!(report.put_count, 3);
        assert!(report.put_ok && report.get_ok && report.scan_ok);
        assert!(report.cas_ok && report.delete_ok && report.ttl_ok);
        assert!(report.replay_matches && report.dual_run_holds);
        assert!(report.divergence.is_none());
        // Live after the cycle: circuit.1 (CAS-swapped) + circuit.2; circuit.3
        // deleted, the lease expired at the replay clock.
        assert_eq!(report.scan_live, 3, "scan runs before CAS/delete");
        assert_eq!(report.replay_live, 2);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn primary_serve_reversible_restores_nats_kv_path() {
        // Reversibility half proven directly: after the cycle, a fresh driver over
        // the SAME log serves the identical durable live set with zero data loss —
        // a flip back to the NATS-KV path replays the store whole and NATS-KV was
        // never written by the engine.
        let (log, dir) = tmp_log("primary-reversible");
        let d = driver(&log);
        let report = exercise_primary_serve(&d, &primary_input(), "primary-t3").unwrap();
        assert!(report.replay_matches);
        let fresh = driver(&log);
        let scan = fresh
            .scan(&KvScanRequest {
                bucket: "noetl_kv_primary".to_string(),
                prefix: None,
                limit: 100,
                now_ms: Some(2_000),
            })
            .unwrap();
        let keys: Vec<&str> = scan.entries.iter().map(|e| e.key.as_str()).collect();
        assert_eq!(keys, vec!["circuit.1", "circuit.2"]);
        // The CAS-swapped value survives the replay.
        assert!(scan.entries[0].value.ends_with("::cas"));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn primary_serve_dual_run_parity_holds_across_reads() {
        let (log, dir) = tmp_log("primary-parity");
        let d = driver(&log);
        let report = exercise_primary_serve(&d, &primary_input(), "primary-t3").unwrap();
        // One parity per initial get (3) + post-CAS get (1) + delete get (1).
        assert_eq!(report.dual_run.len(), 5);
        assert!(report.dual_run.iter().all(|r| r.holds()), "{report:?}");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn primary_serve_requires_two_distinct_keys() {
        let (log, dir) = tmp_log("primary-badinput");
        let d = driver(&log);
        // Fewer than two entries → rejected as invalid state.
        let one = KvPrimaryInput {
            bucket: "b".to_string(),
            entries: vec![("k".to_string(), "v".to_string())],
            now_ms: 0,
        };
        assert!(exercise_primary_serve(&d, &one, "t").is_err());
        // First == last key → rejected.
        let same = KvPrimaryInput {
            bucket: "b".to_string(),
            entries: vec![
                ("k".to_string(), "v1".to_string()),
                ("k".to_string(), "v2".to_string()),
            ],
            now_ms: 0,
        };
        assert!(exercise_primary_serve(&d, &same, "t").is_err());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn primary_serve_scope_isolated_by_bucket() {
        // A pre-existing key in a DIFFERENT bucket never leaks into the served
        // scan / replay of the cycle's bucket.
        let (log, dir) = tmp_log("primary-scope");
        let d = driver(&log);
        put(&d, "other_bucket", "circuit.1", "leak", 99);
        let report = exercise_primary_serve(&d, &primary_input(), "primary-t3").unwrap();
        assert!(report.served_by_ehdb(), "{report:?}");
        // The other bucket's key never affects the cycle's live set.
        assert_eq!(report.replay_live, 2);
        assert!(get(&d, "other_bucket", "circuit.1").found);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn hex_encode_produces_subject_safe_lowercase_tokens() {
        for sample in ["circuit.12345", "a/b.c-d_e", "ünïcöde"] {
            let encoded = hex_encode(sample.as_bytes());
            // A hex token is always lowercase `[0-9a-f]` — a single, subject-safe
            // token with no `.` to split a key across subject levels.
            assert!(!encoded.is_empty());
            assert!(encoded
                .chars()
                .all(|c| c.is_ascii_hexdigit() && !c.is_ascii_uppercase()));
            assert!(Subject::new(format!("{KV_SUBJECT_PREFIX}.b.{encoded}")).is_ok());
        }
    }
}
