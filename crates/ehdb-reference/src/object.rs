//! EHDB object / blob core engine (completion program Phase 8, slice 2).
//!
//! This is the durable content-addressed object engine that Phase 8 puts
//! *underneath* NoETL's internal **external object store** (GCS / S3) usage — the
//! platform-artifact tier, NOT business data.  The concrete internal artifacts it
//! replaces today are:
//!
//! * **State shards** (`#166`) — the Arrow-IPC (Feather) slim-chain execution
//!   state written by `state_materializer.rs` and cold-loaded on a drive miss by
//!   `state_reader.rs`, keyed
//!   `noetl/env=…/region=…/cell=…/shard=s<NNNN>/…/execution=<eid>/state/<open|sealed>.feather`.
//! * **Result tier** (`#104`) — the Arrow-Feather / JSON per-step result frames
//!   written by `result_materializer.rs` and resolved by `result_resolver.rs`,
//!   keyed `noetl/…/execution=<eid>/results/<step>/<frame>/<row>/<attempt>.<feather|json>`.
//!
//! Both are **content-derivable** platform artifacts (rebuildable from the WAL)
//! that the worker already reaches through the server's
//! `/api/internal/objects/{key}` API, honoring the data-access boundary.  The
//! EHDB object engine sits **underneath** that endpoint (or behind the worker's
//! object client as a selectable driver), not as a new direct-store path.
//! Tenant/domain object buckets reached by playbook connectors stay **external**
//! and never move.
//!
//! ## Boundary — this is the object storage engine, NOT an event author
//!
//! An object is a derived platform artifact, not an event.  This engine never
//! authors a `noetl.event`; it persists + serves content-addressed bytes only.
//! It is a platform engine for platform artifacts only; **business data never
//! flows through it**.
//!
//! ## Semantics preserved from the external-store path
//!
//! * **Content-addressed + immutable** — a blob is stored once under its SHA-256
//!   digest ([`ehdb_storage::ObjectDigest::sha256`]); an identical write dedups to
//!   the same blob.  Reads are length + digest verified (the
//!   [`ehdb_storage::ImmutableObjectStore::get_verified`] twin), so a corrupt or
//!   truncated read is a hard error, never a silent wrong answer.
//! * **Logical key → digest registry** — the external key scheme
//!   (`noetl/env=…/execution=…/state|results/…`) is preserved **verbatim** as the
//!   logical key.  A per-key subject-scoped append to a single canonical stream
//!   ([`OBJECT_STORE_STREAM`]) records the key → digest mapping, so a `get`/`locate`
//!   is the latest record of that key's subject-filtered replay and a `list` is a
//!   prefix-filtered replay folded to the latest record per key.  The logical key
//!   is hex-encoded into one subject token because the external key carries
//!   `.` / `/` / `=` which are not valid inside one subject token (and `=` is not
//!   even a safe [`ehdb_storage::ObjectPath`] character), so the logical key never
//!   touches the physical object path — only its digest does.
//! * **Delete = tombstone** — a delete appends a tombstone registry record; a
//!   subsequent `get`/`locate` sees the key as absent (the GC twin for the
//!   GC-managed tiers).  The content-addressed blob is left in place because other
//!   keys may reference the same digest; physical blob GC is a separate concern.
//! * **List by prefix** — a bounded prefix scan of the live logical keys (folded
//!   to the latest record per key, tombstones dropped), ordered by key.
//! * **Locate** — returns an in-cluster object URI (the presign-equivalent handle;
//!   EHDB is in-cluster so it hands back an internal `ehdb-object://` URI, not a
//!   signed external URL).
//! * **Append-only + immutable + replay-is-truth** — registry records are never
//!   mutated; `KeepAll` retention keeps the whole write history so any past
//!   mapping is a replay.
//!
//! ## Driver interface (Phase 10-ready)
//!
//! The engine is exposed behind [`ObjectBlobDriver`] so the object tier is
//! driver-selectable: the EHDB engine here is [`LocalReferenceObjectBlobDriver`];
//! an external-store driver implementing the same trait keeps the tier selectable
//! back to the incumbent (Roadmap Phase 10).  Callers program against the trait.
//!
//! ## Shadow validation
//!
//! [`compare_object_parity`] is the pure, secret-free comparison the worker's
//! disabled-by-default shadow mode uses to prove the EHDB engine tracks the
//! authoritative external store without serving reads from it: presence parity,
//! digest parity, byte-length parity, and retrievability, with a single
//! divergence reason when they differ.  Because artifacts are content-addressed,
//! the digest check is a cheap, exact equality.

use std::collections::BTreeMap;
use std::path::PathBuf;

use ehdb_core::{EhdbError, NamespaceName, Result, StreamName, TenantId, TransactionId};
use ehdb_storage::{
    ImmutableObjectStore, LocalObjectStore, ObjectDigest, ObjectPath, ObjectPlacement, ObjectRef,
};
use ehdb_stream::{RetentionPolicy, StreamRecord, Subject, SubjectFilter};
use ehdb_transaction::{CommitTransaction, Mutation, StreamMutation};
use serde::{Deserialize, Serialize};

use crate::LocalReferenceRuntime;

/// The single canonical stream that carries every object registry write (the
/// logical key → digest mapping).  One stream keeps its
/// [`ehdb_stream::StreamSequence`] the global write-order sequence for the whole
/// object tier, so replay is deterministic.
pub const OBJECT_STORE_STREAM: &str = "noetl_object_store";

/// Subject prefix scoping a registry write to its logical key.  A record's subject
/// is `noetl.obj.<hex(key)>`, so a per-key read is an exact subject-filtered
/// replay and a prefix list is a `noetl.obj.>` replay folded + filtered.  The key
/// is hex-encoded into a single subject token because object keys carry `.` / `/`
/// / `=` which are not valid inside one subject token.
pub const OBJECT_SUBJECT_PREFIX: &str = "noetl.obj";

/// The content-addressed blob directory under the object store root.  A blob is
/// stored at `<root>/objects/sha256/<hex>` — a path built only from the SHA-256
/// hex digest, so it is always a safe [`ObjectPath`] regardless of the logical
/// key's characters.
pub const OBJECT_CONTENT_PREFIX: &str = "objects/sha256";

/// Upper bound on one stored blob (bounded like the rest of the integration).  An
/// over-cap blob is an [`EhdbError::InvalidState`] whose message carries `exceeds
/// bound`, so a caller mistake classifies as *rejected*, distinct from an
/// identifier mistake or an engine-unavailable error.
pub const MAX_OBJECT_BYTES: usize = 16 * 1024 * 1024;

/// Hard ceiling on a single list's returned entries.
pub const MAX_OBJECT_LIST_LIMIT: usize = 4_096;

fn hex_encode(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for &b in bytes {
        out.push(HEX[(b >> 4) as usize] as char);
        out.push(HEX[(b & 0x0f) as usize] as char);
    }
    out
}

/// Build the exact per-key registry subject `noetl.obj.<hex(key)>`.
fn key_subject(key: &str) -> Result<Subject> {
    if key.is_empty() {
        return Err(EhdbError::InvalidIdentifier(
            "object key: empty".to_string(),
        ));
    }
    let token = hex_encode(key.as_bytes());
    Subject::new(format!("{OBJECT_SUBJECT_PREFIX}.{token}"))
}

/// Build the all-keys registry subject filter `noetl.obj.>`.
fn all_keys_filter() -> Result<SubjectFilter> {
    SubjectFilter::new(format!("{OBJECT_SUBJECT_PREFIX}.>"))
}

/// Build the content-addressed blob path `objects/sha256/<hex>` from a digest.
/// The [`ObjectDigest`] renders as `sha256:<hex>`; the `:` is not an
/// [`ObjectPath`]-safe character, so the prefix is stripped and only the hex tail
/// forms the path token.
fn content_path(digest: &ObjectDigest) -> Result<ObjectPath> {
    let hex = digest.as_str().strip_prefix("sha256:").ok_or_else(|| {
        EhdbError::Storage(format!("unexpected digest form: {}", digest.as_str()))
    })?;
    ObjectPath::new(format!("{OBJECT_CONTENT_PREFIX}/{hex}"))
}

/// The in-cluster object URI for a stored blob (the presign-equivalent handle).
fn object_uri(namespace: &str, digest: &ObjectDigest) -> String {
    let hex = digest.as_str().strip_prefix("sha256:").unwrap_or("");
    format!("ehdb-object://{namespace}/{OBJECT_CONTENT_PREFIX}/{hex}")
}

/// The stored envelope for one object registry write (the record payload).  Carries
/// the original (un-encoded) logical key so a list reconstructs real keys without
/// decoding the subject, plus the content digest, byte length, monotonic per-key
/// version, and tombstone flag.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ObjectEnvelope {
    key: String,
    /// The content digest (`sha256:<hex>`); empty on a tombstone.
    digest: String,
    byte_len: u64,
    version: u64,
    deleted: bool,
}

/// Write a content-addressed object under a logical key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectPutRequest {
    /// The logical object-store key (the external key scheme, preserved verbatim).
    pub key: String,
    /// The blob bytes.  Content-addressed by SHA-256; over-cap ⇒ rejected.
    pub bytes: Vec<u8>,
    pub transaction_id: String,
}

/// Secret-free result of a put (no key/bytes ever reach a metric label; the digest
/// is a hash, not a secret).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ObjectPutOutcome {
    pub action: String,
    pub key: String,
    /// The content digest assigned to the stored bytes (`sha256:<hex>`).
    pub digest: String,
    pub byte_len: u64,
    /// The per-key registry version after this write (== previous version + 1).
    pub version: u64,
    /// Whether a registry record was appended (always true on a successful put).
    pub written: bool,
    /// Whether the content-addressed blob already existed (byte-level dedup) —
    /// `true` means no new bytes were written to the blob store.
    pub content_deduplicated: bool,
    /// Whether the canonical registry stream was created on this write.
    pub created_stream: bool,
    /// The global write-order sequence assigned to this registry write.
    pub global_sequence: u64,
}

/// Read one logical key's latest live object (digest-verified retrievability).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectGetRequest {
    pub key: String,
}

/// Secret-free result of a get.  The bytes themselves are NOT surfaced here — a
/// `get` proves the object is present + retrievable + digest-verified and reports
/// its metadata; byte retrieval for a served read is the Phase-9 primary path.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ObjectGetOutcome {
    pub action: String,
    pub key: String,
    /// Whether a live (non-deleted) object was found.
    pub found: bool,
    pub digest: Option<String>,
    pub byte_len: Option<u64>,
    /// Whether the stored bytes passed length + SHA-256 verification
    /// ([`ImmutableObjectStore::get_verified`]).  False when absent.
    pub verified: bool,
}

/// Delete a logical key (append a tombstone).  Idempotent — deleting an absent key
/// is a no-op.  The content-addressed blob is left in place.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectDeleteRequest {
    pub key: String,
    pub transaction_id: String,
}

/// Secret-free result of a delete.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ObjectDeleteOutcome {
    pub action: String,
    pub key: String,
    /// Whether a live object existed before this delete (false = idempotent no-op).
    pub existed: bool,
    /// The tombstone's version (== previous version + 1; 0 when nothing existed).
    pub version: u64,
    /// The global sequence assigned to the tombstone (0 on an idempotent no-op).
    pub global_sequence: u64,
}

/// Bounded prefix scan of the live logical keys.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectListRequest {
    /// Optional prefix on the logical key (`noetl/.../state/` etc.).
    pub prefix: Option<String>,
    pub limit: usize,
}

/// One live object entry (metadata only — never the bytes).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ObjectEntryView {
    pub key: String,
    pub digest: String,
    pub byte_len: u64,
    pub version: u64,
}

/// Secret-free result of a prefix list.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ObjectListOutcome {
    pub action: String,
    /// Whether the registry stream exists yet (false before the first write).
    pub exists: bool,
    /// Live matching entries before `limit` is applied.
    pub match_count: usize,
    pub returned: usize,
    /// Entries ordered by key.
    pub entries: Vec<ObjectEntryView>,
}

/// Locate a live object — the presign-equivalent read handle.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ObjectLocateRequest {
    pub key: String,
}

/// Secret-free result of a locate.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ObjectLocateOutcome {
    pub action: String,
    pub key: String,
    /// Whether a live object was found.
    pub found: bool,
    /// The in-cluster object URI handle (`ehdb-object://…`); `None` when absent.
    pub uri: Option<String>,
    pub digest: Option<String>,
    pub byte_len: Option<u64>,
}

/// The driver interface for the object/blob tier.  EHDB is one implementation
/// ([`LocalReferenceObjectBlobDriver`]); an external-store driver implementing the
/// same trait keeps the tier selectable back to the incumbent (Phase 10).
///
/// All methods are `&self`: the durable state lives in the on-disk registry log +
/// content-addressed blob store, opened + dropped per op (bounded/stateless
/// discipline).
pub trait ObjectBlobDriver {
    /// A stable, secret-free identifier for the backing engine.
    fn driver_name(&self) -> &'static str;
    /// Store a content-addressed blob under a logical key.
    fn put(&self, request: &ObjectPutRequest) -> Result<ObjectPutOutcome>;
    /// Read a logical key's latest live object (digest-verified retrievability).
    fn get(&self, request: &ObjectGetRequest) -> Result<ObjectGetOutcome>;
    /// List live logical keys under a prefix (bounded, ordered by key).
    fn list(&self, request: &ObjectListRequest) -> Result<ObjectListOutcome>;
    /// Delete a logical key (tombstone; idempotent).
    fn delete(&self, request: &ObjectDeleteRequest) -> Result<ObjectDeleteOutcome>;
    /// Locate a live object's in-cluster read handle (the presign-equivalent).
    fn locate(&self, request: &ObjectLocateRequest) -> Result<ObjectLocateOutcome>;
}

/// The EHDB object engine over the bounded local-reference transaction log (the
/// key → digest registry) and a content-addressed [`LocalObjectStore`] (the bytes).
#[derive(Debug, Clone)]
pub struct LocalReferenceObjectBlobDriver {
    /// The registry transaction log path (logical key → digest mappings).
    pub log_path: PathBuf,
    /// The content-addressed blob store root (`<root>/objects/sha256/<hex>`).
    pub object_root: PathBuf,
    pub tenant: String,
    pub namespace: String,
}

impl LocalReferenceObjectBlobDriver {
    pub fn new(
        log_path: impl Into<PathBuf>,
        object_root: impl Into<PathBuf>,
        tenant: impl Into<String>,
        namespace: impl Into<String>,
    ) -> Self {
        Self {
            log_path: log_path.into(),
            object_root: object_root.into(),
            tenant: tenant.into(),
            namespace: namespace.into(),
        }
    }

    fn coordinates(&self) -> Result<(TenantId, NamespaceName, StreamName)> {
        Ok((
            TenantId::new(self.tenant.clone())?,
            NamespaceName::new(self.namespace.clone())?,
            StreamName::new(OBJECT_STORE_STREAM.to_string())?,
        ))
    }

    fn blob_store(&self) -> LocalObjectStore {
        LocalObjectStore::new(&self.object_root)
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
    ) -> Option<ObjectEnvelope> {
        let filter = SubjectFilter::new(subject.as_str().to_string()).ok()?;
        let records = runtime
            .state()
            .streams
            .replay_matching(tenant, namespace, stream, &filter, None)
            .ok()?;
        records.last().and_then(decode_envelope)
    }

    /// Store the bytes under their content-addressed path, deduplicating when the
    /// blob already exists (identical content ⇒ identical digest ⇒ identical
    /// path).  Returns `(digest, newly_stored)`.
    fn store_content(&self, bytes: &[u8]) -> Result<(ObjectDigest, bool)> {
        let digest = ObjectDigest::sha256(bytes);
        let path = content_path(&digest)?;
        let store = self.blob_store();
        // Content-addressed: an existing blob at this digest path is the same
        // bytes, so a re-put dedups without rewriting.
        match store.get(&path) {
            Ok(_) => Ok((digest, false)),
            Err(_) => {
                store.put_if_absent(path, bytes)?;
                Ok((digest, true))
            }
        }
    }
}

impl ObjectBlobDriver for LocalReferenceObjectBlobDriver {
    fn driver_name(&self) -> &'static str {
        "ehdb-local-reference"
    }

    fn put(&self, request: &ObjectPutRequest) -> Result<ObjectPutOutcome> {
        let (tenant, namespace, stream) = self.coordinates()?;
        let subject = key_subject(&request.key)?;
        let byte_len = request.bytes.len();
        if byte_len > MAX_OBJECT_BYTES {
            return Err(EhdbError::InvalidState(format!(
                "object {byte_len} bytes exceeds bound {MAX_OBJECT_BYTES}"
            )));
        }
        let transaction_id = TransactionId::new(request.transaction_id.clone())?;

        // Store the bytes first (content-addressed, idempotent), then record the
        // key → digest mapping.
        let (digest, newly_stored) = self.store_content(&request.bytes)?;

        let mut runtime = LocalReferenceRuntime::open(&self.log_path)?;
        let latest = self.latest_envelope(&runtime, &tenant, &namespace, &stream, &subject);
        // Monotonic per-key version — advances across tombstones too, so a
        // delete → recreate strictly increases the version.
        let next_version = latest.as_ref().map(|env| env.version).unwrap_or(0) + 1;

        let (created_stream, next_sequence) =
            next_stream_write(&runtime, &tenant, &namespace, &stream);

        let envelope = ObjectEnvelope {
            key: request.key.clone(),
            digest: digest.as_str().to_string(),
            byte_len: byte_len as u64,
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

        Ok(ObjectPutOutcome {
            action: "object-put".to_string(),
            key: request.key.clone(),
            digest: digest.as_str().to_string(),
            byte_len: byte_len as u64,
            version: next_version,
            written: true,
            content_deduplicated: !newly_stored,
            created_stream,
            global_sequence: next_sequence,
        })
    }

    fn get(&self, request: &ObjectGetRequest) -> Result<ObjectGetOutcome> {
        let (tenant, namespace, stream) = self.coordinates()?;
        let subject = key_subject(&request.key)?;
        let runtime = LocalReferenceRuntime::open(&self.log_path)?;

        let latest = self.latest_envelope(&runtime, &tenant, &namespace, &stream, &subject);
        let absent = || ObjectGetOutcome {
            action: "object-get".to_string(),
            key: request.key.clone(),
            found: false,
            digest: None,
            byte_len: None,
            verified: false,
        };

        let Some(env) = latest else {
            return Ok(absent());
        };
        if env.deleted {
            return Ok(absent());
        }

        // Digest-verified retrievability: rebuild the ObjectRef from the registry
        // metadata and fetch + verify the content-addressed blob.
        let digest = ObjectDigest::new(env.digest.clone())?;
        let path = content_path(&digest)?;
        let object_ref = ObjectRef {
            path,
            len: env.byte_len,
            digest: digest.clone(),
            placement: ObjectPlacement::local_dev(),
        };
        let verified = self.blob_store().get_verified(&object_ref).is_ok();

        Ok(ObjectGetOutcome {
            action: "object-get".to_string(),
            key: request.key.clone(),
            found: true,
            digest: Some(env.digest),
            byte_len: Some(env.byte_len),
            verified,
        })
    }

    fn list(&self, request: &ObjectListRequest) -> Result<ObjectListOutcome> {
        let (tenant, namespace, stream) = self.coordinates()?;
        let filter = all_keys_filter()?;
        let limit = request.limit.min(MAX_OBJECT_LIST_LIMIT);
        let runtime = LocalReferenceRuntime::open(&self.log_path)?;

        let records = match runtime
            .state()
            .streams
            .replay_matching(&tenant, &namespace, &stream, &filter, None)
        {
            Ok(records) => records,
            // A missing stream (nothing written anywhere yet) is an absent probe.
            Err(_) => {
                return Ok(ObjectListOutcome {
                    action: "object-list".to_string(),
                    exists: false,
                    match_count: 0,
                    returned: 0,
                    entries: Vec::new(),
                });
            }
        };

        // Fold to the latest envelope per key (records replay in sequence order,
        // so a later record overwrites an earlier one).
        let mut latest_by_key: BTreeMap<String, ObjectEnvelope> = BTreeMap::new();
        for record in records {
            if let Some(env) = decode_envelope(&record) {
                latest_by_key.insert(env.key.clone(), env);
            }
        }

        let mut entries: Vec<ObjectEntryView> = latest_by_key
            .into_values()
            .filter(|env| !env.deleted)
            .filter(|env| match &request.prefix {
                Some(prefix) => env.key.starts_with(prefix),
                None => true,
            })
            .map(entry_view)
            .collect();
        entries.sort_by(|a, b| a.key.cmp(&b.key));

        let match_count = entries.len();
        entries.truncate(limit);

        Ok(ObjectListOutcome {
            action: "object-list".to_string(),
            exists: true,
            match_count,
            returned: entries.len(),
            entries,
        })
    }

    fn delete(&self, request: &ObjectDeleteRequest) -> Result<ObjectDeleteOutcome> {
        let (tenant, namespace, stream) = self.coordinates()?;
        let subject = key_subject(&request.key)?;
        let transaction_id = TransactionId::new(request.transaction_id.clone())?;

        let mut runtime = LocalReferenceRuntime::open(&self.log_path)?;
        let latest = self.latest_envelope(&runtime, &tenant, &namespace, &stream, &subject);

        // Idempotent: an absent key (never written, or already a tombstone) does
        // not append a second tombstone.
        let Some(current) = latest.as_ref().filter(|env| !env.deleted) else {
            return Ok(ObjectDeleteOutcome {
                action: "object-delete".to_string(),
                key: request.key.clone(),
                existed: false,
                version: 0,
                global_sequence: 0,
            });
        };

        let next_version = current.version + 1;
        let (created_stream, next_sequence) =
            next_stream_write(&runtime, &tenant, &namespace, &stream);
        let envelope = ObjectEnvelope {
            key: request.key.clone(),
            digest: String::new(),
            byte_len: 0,
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

        Ok(ObjectDeleteOutcome {
            action: "object-delete".to_string(),
            key: request.key.clone(),
            existed: true,
            version: next_version,
            global_sequence: next_sequence,
        })
    }

    fn locate(&self, request: &ObjectLocateRequest) -> Result<ObjectLocateOutcome> {
        let (tenant, namespace, stream) = self.coordinates()?;
        let subject = key_subject(&request.key)?;
        let runtime = LocalReferenceRuntime::open(&self.log_path)?;

        let latest = self.latest_envelope(&runtime, &tenant, &namespace, &stream, &subject);
        let Some(env) = latest.filter(|env| !env.deleted) else {
            return Ok(ObjectLocateOutcome {
                action: "object-locate".to_string(),
                key: request.key.clone(),
                found: false,
                uri: None,
                digest: None,
                byte_len: None,
            });
        };

        let digest = ObjectDigest::new(env.digest.clone())?;
        Ok(ObjectLocateOutcome {
            action: "object-locate".to_string(),
            key: request.key.clone(),
            found: true,
            uri: Some(object_uri(&self.namespace, &digest)),
            digest: Some(env.digest),
            byte_len: Some(env.byte_len),
        })
    }
}

/// The next (created_stream, sequence) for a registry write — matches the
/// event-log / KV engines: a missing stream replays as an error (the create
/// signal), and `next = count + 1` keeps the write-order sequence monotonic +
/// gapless.
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
    envelope: &ObjectEnvelope,
    created_stream: bool,
    sequence: u64,
) -> Result<()> {
    let payload = serde_json::to_vec(envelope)
        .map_err(|err| EhdbError::InvalidState(format!("object envelope encode: {err}")))?;

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

fn decode_envelope(record: &StreamRecord) -> Option<ObjectEnvelope> {
    serde_json::from_slice(&record.payload).ok()
}

fn entry_view(env: ObjectEnvelope) -> ObjectEntryView {
    ObjectEntryView {
        key: env.key,
        digest: env.digest,
        byte_len: env.byte_len,
        version: env.version,
    }
}

/// The authoritative external-store view of one object, for the shadow parity
/// check.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthoritativeObject {
    /// The external store's content digest (`sha256:<hex>`).
    pub digest: String,
    pub byte_len: u64,
}

/// The parity verdict of one shadow write: did the EHDB engine's view of an object
/// match the authoritative external-store view?  Pure + secret-free so the engine
/// tests and the worker's disabled-by-default shadow mode share one comparison.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ObjectParityReport {
    /// Both stores agree on whether the object is present.
    pub present_ok: bool,
    /// When both present, their content digests are equal.
    pub digest_ok: bool,
    /// When both present, their byte lengths are equal.
    pub length_ok: bool,
    /// When present in EHDB, the stored bytes passed length + digest verification.
    pub retrievable_ok: bool,
    /// The single reason parity failed, or `None` when it holds.
    pub divergence: Option<String>,
}

impl ObjectParityReport {
    /// Whether every parity check held.
    pub fn holds(&self) -> bool {
        self.present_ok
            && self.digest_ok
            && self.length_ok
            && self.retrievable_ok
            && self.divergence.is_none()
    }
}

/// Compare the EHDB engine's `get` result against the authoritative external-store
/// view.
///
/// * `authoritative` — what the incumbent external object store holds for the key
///   (`None` = absent there).
/// * `ehdb` — the EHDB engine's [`ObjectGetOutcome`] for the same key.
///
/// Returns the first divergence found, or a clean report.  Because artifacts are
/// content-addressed, the digest check is an exact equality.
pub fn compare_object_parity(
    authoritative: Option<&AuthoritativeObject>,
    ehdb: &ObjectGetOutcome,
) -> ObjectParityReport {
    let present_ok = authoritative.is_some() == ehdb.found;

    let (digest_ok, length_ok) = match (authoritative, ehdb.found) {
        (Some(auth), true) => (
            Some(&auth.digest) == ehdb.digest.as_ref(),
            Some(auth.byte_len) == ehdb.byte_len,
        ),
        // When either side is absent there is no digest/length to contradict; the
        // presence check carries the verdict.
        _ => (true, true),
    };

    // Retrievability only constrains the present case — an absent EHDB object has
    // nothing to retrieve.
    let retrievable_ok = if ehdb.found { ehdb.verified } else { true };

    let divergence = if !present_ok {
        Some(format!(
            "presence divergence: authoritative_present={} ehdb_found={}",
            authoritative.is_some(),
            ehdb.found
        ))
    } else if !digest_ok {
        Some("digest divergence: authoritative digest != ehdb digest".to_string())
    } else if !length_ok {
        Some("length divergence: authoritative byte_len != ehdb byte_len".to_string())
    } else if !retrievable_ok {
        Some("retrievability divergence: ehdb object failed digest verification".to_string())
    } else {
        None
    };

    ObjectParityReport {
        present_ok,
        digest_ok,
        length_ok,
        retrievable_ok,
        divergence,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Fixture {
        driver: LocalReferenceObjectBlobDriver,
        dir: PathBuf,
    }

    fn fixture(tag: &str) -> Fixture {
        let dir = std::env::temp_dir().join(format!(
            "ehdb-object-{tag}-{}-{:?}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let driver = LocalReferenceObjectBlobDriver::new(
            dir.join("registry.jsonl"),
            dir.join("blobs"),
            "noetl",
            "default",
        );
        Fixture { driver, dir }
    }

    impl Drop for Fixture {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.dir);
        }
    }

    fn put(
        d: &LocalReferenceObjectBlobDriver,
        key: &str,
        bytes: &[u8],
        n: u64,
    ) -> ObjectPutOutcome {
        d.put(&ObjectPutRequest {
            key: key.to_string(),
            bytes: bytes.to_vec(),
            transaction_id: format!("txn-{n}"),
        })
        .unwrap()
    }

    fn get(d: &LocalReferenceObjectBlobDriver, key: &str) -> ObjectGetOutcome {
        d.get(&ObjectGetRequest {
            key: key.to_string(),
        })
        .unwrap()
    }

    // The real state-shard key scheme carries `/`, `.`, and `=`.
    const STATE_KEY: &str =
        "noetl/env=prod/region=us/cell=a/shard=s0001/execution=exec-9/state/open.feather";
    const RESULT_KEY: &str = "noetl/execution=exec-9/results/step1/f0/r0/a0.feather";

    #[test]
    fn put_get_round_trips_and_verifies() {
        let f = fixture("put-get");
        let d = &f.driver;
        let first = put(d, STATE_KEY, b"arrow-ipc-bytes-v1", 1);
        assert!(first.written);
        assert_eq!(first.version, 1);
        assert!(first.created_stream);
        assert!(!first.content_deduplicated);
        assert_eq!(first.byte_len, 18);
        // Overwrite with newer bytes → version advances, get returns latest digest.
        let second = put(d, STATE_KEY, b"arrow-ipc-bytes-v2-longer", 2);
        assert_eq!(second.version, 2);
        assert!(!second.created_stream);
        assert_ne!(first.digest, second.digest);
        let got = get(d, STATE_KEY);
        assert!(got.found);
        assert!(got.verified);
        assert_eq!(got.digest.as_deref(), Some(second.digest.as_str()));
        assert_eq!(got.byte_len, Some(25));
    }

    #[test]
    fn get_of_absent_key_is_not_found_not_error() {
        let f = fixture("absent");
        let got = get(&f.driver, "noetl/execution=missing/state/open.feather");
        assert!(!got.found);
        assert!(!got.verified);
        assert!(got.digest.is_none());
    }

    #[test]
    fn delete_tombstones_then_get_is_absent_and_idempotent() {
        let f = fixture("delete");
        let d = &f.driver;
        put(d, RESULT_KEY, b"result-frame", 1);
        let del = d
            .delete(&ObjectDeleteRequest {
                key: RESULT_KEY.to_string(),
                transaction_id: "txn-del-1".to_string(),
            })
            .unwrap();
        assert!(del.existed);
        assert_eq!(del.version, 2);
        assert!(!get(d, RESULT_KEY).found);
        // Second delete is an idempotent no-op (no new tombstone).
        let del2 = d
            .delete(&ObjectDeleteRequest {
                key: RESULT_KEY.to_string(),
                transaction_id: "txn-del-2".to_string(),
            })
            .unwrap();
        assert!(!del2.existed);
        assert_eq!(del2.global_sequence, 0);
    }

    #[test]
    fn put_after_delete_recreates_with_monotonic_version() {
        let f = fixture("recreate");
        let d = &f.driver;
        put(d, "noetl/k/state/open.feather", b"v1", 1); // version 1
        d.delete(&ObjectDeleteRequest {
            key: "noetl/k/state/open.feather".to_string(),
            transaction_id: "txn-del".to_string(),
        })
        .unwrap(); // tombstone version 2
        let recreated = put(d, "noetl/k/state/open.feather", b"v2", 2);
        assert_eq!(recreated.version, 3);
        let got = get(d, "noetl/k/state/open.feather");
        assert!(got.found);
        assert_eq!(got.byte_len, Some(2));
    }

    #[test]
    fn list_returns_live_keys_ordered_dropping_tombstones() {
        let f = fixture("list");
        let d = &f.driver;
        put(d, "noetl/exec=1/state/open.feather", b"a", 1);
        put(d, "noetl/exec=1/results/s/f/r/a.feather", b"b", 2);
        put(d, "noetl/exec=2/state/open.feather", b"c", 3);
        d.delete(&ObjectDeleteRequest {
            key: "noetl/exec=1/results/s/f/r/a.feather".to_string(),
            transaction_id: "txn-del".to_string(),
        })
        .unwrap();
        let list = d
            .list(&ObjectListRequest {
                prefix: None,
                limit: 100,
            })
            .unwrap();
        assert!(list.exists);
        let keys: Vec<&str> = list.entries.iter().map(|e| e.key.as_str()).collect();
        assert_eq!(
            keys,
            vec![
                "noetl/exec=1/state/open.feather",
                "noetl/exec=2/state/open.feather"
            ]
        );
        // Prefix list narrows to one execution's keys.
        let pref = d
            .list(&ObjectListRequest {
                prefix: Some("noetl/exec=2/".to_string()),
                limit: 100,
            })
            .unwrap();
        let pkeys: Vec<&str> = pref.entries.iter().map(|e| e.key.as_str()).collect();
        assert_eq!(pkeys, vec!["noetl/exec=2/state/open.feather"]);
    }

    #[test]
    fn list_is_bounded_by_limit() {
        let f = fixture("list-limit");
        let d = &f.driver;
        for i in 0..5 {
            put(d, &format!("noetl/k{i}/state/open.feather"), b"v", i);
        }
        let list = d
            .list(&ObjectListRequest {
                prefix: None,
                limit: 2,
            })
            .unwrap();
        assert_eq!(list.match_count, 5);
        assert_eq!(list.returned, 2);
        // Ordered by key, so the first two are k0, k1.
        assert_eq!(list.entries[0].key, "noetl/k0/state/open.feather");
        assert_eq!(list.entries[1].key, "noetl/k1/state/open.feather");
    }

    #[test]
    fn content_addressed_dedup_across_keys() {
        let f = fixture("dedup");
        let d = &f.driver;
        // Two distinct keys with identical bytes → same digest, second dedups.
        let a = put(d, "noetl/a/state/open.feather", b"identical-shard", 1);
        let b = put(d, "noetl/b/state/open.feather", b"identical-shard", 2);
        assert_eq!(a.digest, b.digest);
        assert!(!a.content_deduplicated);
        assert!(b.content_deduplicated);
        // Both keys remain independently retrievable + verified.
        assert!(get(d, "noetl/a/state/open.feather").verified);
        assert!(get(d, "noetl/b/state/open.feather").verified);
    }

    #[test]
    fn locate_returns_internal_uri_for_live_object() {
        let f = fixture("locate");
        let d = &f.driver;
        let put = put(d, STATE_KEY, b"shard-bytes", 1);
        let loc = d
            .locate(&ObjectLocateRequest {
                key: STATE_KEY.to_string(),
            })
            .unwrap();
        assert!(loc.found);
        assert_eq!(loc.digest.as_deref(), Some(put.digest.as_str()));
        assert_eq!(loc.byte_len, Some(11));
        let uri = loc.uri.unwrap();
        assert!(uri.starts_with("ehdb-object://default/objects/sha256/"));
        // A deleted key does not locate.
        d.delete(&ObjectDeleteRequest {
            key: STATE_KEY.to_string(),
            transaction_id: "txn-del".to_string(),
        })
        .unwrap();
        assert!(
            !d.locate(&ObjectLocateRequest {
                key: STATE_KEY.to_string(),
            })
            .unwrap()
            .found
        );
    }

    #[test]
    fn keys_and_prefixes_are_scope_isolated() {
        let f = fixture("scope");
        let d = &f.driver;
        // State-shard and result-tier prefixes never collide.
        put(d, "noetl/exec=9/state/open.feather", b"state", 1);
        put(d, "noetl/exec=9/results/s/f/r/a.feather", b"result", 2);
        let states = d
            .list(&ObjectListRequest {
                prefix: Some("noetl/exec=9/state/".to_string()),
                limit: 100,
            })
            .unwrap();
        assert_eq!(states.entries.len(), 1);
        assert_eq!(states.entries[0].key, "noetl/exec=9/state/open.feather");
        let results = d
            .list(&ObjectListRequest {
                prefix: Some("noetl/exec=9/results/".to_string()),
                limit: 100,
            })
            .unwrap();
        assert_eq!(results.entries.len(), 1);
        assert_eq!(
            results.entries[0].key,
            "noetl/exec=9/results/s/f/r/a.feather"
        );
    }

    #[test]
    fn replay_reconstructs_from_log_and_store() {
        let f = fixture("replay");
        put(&f.driver, "noetl/k1/state/open.feather", b"v1", 1);
        put(&f.driver, "noetl/k2/state/open.feather", b"v2", 2);
        // A fresh driver over the same paths replays the registry + verifies blobs.
        let d2 = LocalReferenceObjectBlobDriver::new(
            f.driver.log_path.clone(),
            f.driver.object_root.clone(),
            "noetl",
            "default",
        );
        let got = get(&d2, "noetl/k1/state/open.feather");
        assert!(got.found);
        assert!(got.verified);
        let list = d2
            .list(&ObjectListRequest {
                prefix: None,
                limit: 100,
            })
            .unwrap();
        assert_eq!(list.match_count, 2);
    }

    #[test]
    fn oversized_object_is_rejected_bound() {
        let f = fixture("oversize");
        let big = vec![0u8; MAX_OBJECT_BYTES + 1];
        let err = f
            .driver
            .put(&ObjectPutRequest {
                key: "noetl/k/state/open.feather".to_string(),
                bytes: big,
                transaction_id: "txn-1".to_string(),
            })
            .unwrap_err();
        assert!(err.to_string().contains("exceeds bound"));
    }

    #[test]
    fn empty_key_is_invalid_identifier() {
        let f = fixture("badid");
        let err = f
            .driver
            .put(&ObjectPutRequest {
                key: String::new(),
                bytes: b"v".to_vec(),
                transaction_id: "txn".to_string(),
            })
            .unwrap_err();
        assert!(err.to_string().starts_with("invalid identifier"));
    }

    #[test]
    fn keys_with_dots_slashes_and_equals_round_trip() {
        let f = fixture("special-keys");
        let d = &f.driver;
        // Real NoETL object keys carry `.`, `/`, and `=`; the hex subject token
        // round-trips and the `=` never touches the physical object path.
        for (n, key) in [STATE_KEY, RESULT_KEY, "noetl/env=x/a.b.c/d_e-f.feather"]
            .into_iter()
            .enumerate()
        {
            put(d, key, format!("bytes-{n}").as_bytes(), n as u64);
            let got = get(d, key);
            assert!(got.found, "key {key} should round-trip");
            assert!(got.verified, "key {key} should verify");
        }
    }

    #[test]
    fn get_verifies_content_against_digest() {
        let f = fixture("verify");
        let d = &f.driver;
        put(d, STATE_KEY, b"exact-bytes", 1);
        let got = get(d, STATE_KEY);
        assert!(got.verified);
        // Corrupting the on-disk blob makes the next get fail verification while
        // the registry still reports the object present.
        let hex = got
            .digest
            .unwrap()
            .strip_prefix("sha256:")
            .unwrap()
            .to_string();
        let blob = f.driver.object_root.join(OBJECT_CONTENT_PREFIX).join(&hex);
        std::fs::write(&blob, b"tampered-different-length").unwrap();
        let after = get(d, STATE_KEY);
        assert!(after.found);
        assert!(!after.verified);
    }

    #[test]
    fn parity_holds_when_stores_agree() {
        let f = fixture("parity-ok");
        let d = &f.driver;
        let put = put(d, STATE_KEY, b"shard", 1);
        let ehdb = get(d, STATE_KEY);
        let auth = AuthoritativeObject {
            digest: put.digest.clone(),
            byte_len: put.byte_len,
        };
        let report = compare_object_parity(Some(&auth), &ehdb);
        assert!(report.holds(), "{report:?}");
        // Both absent also holds.
        let ehdb_absent = get(d, "noetl/exec=missing/state/open.feather");
        let report_absent = compare_object_parity(None, &ehdb_absent);
        assert!(report_absent.holds());
    }

    #[test]
    fn parity_flags_presence_digest_and_length_divergence() {
        let f = fixture("parity-bad");
        let d = &f.driver;
        let put = put(d, STATE_KEY, b"shard", 1);
        let ehdb = get(d, STATE_KEY);

        // Authoritative absent but EHDB present → presence divergence.
        let present = compare_object_parity(None, &ehdb);
        assert!(!present.holds());
        assert!(present.divergence.unwrap().contains("presence divergence"));

        // Digest mismatch.
        let digest = compare_object_parity(
            Some(&AuthoritativeObject {
                digest: "sha256:".to_string() + &"0".repeat(64),
                byte_len: put.byte_len,
            }),
            &ehdb,
        );
        assert!(!digest.digest_ok);
        assert!(digest.divergence.unwrap().contains("digest divergence"));

        // Length mismatch.
        let length = compare_object_parity(
            Some(&AuthoritativeObject {
                digest: put.digest.clone(),
                byte_len: put.byte_len + 1,
            }),
            &ehdb,
        );
        assert!(!length.length_ok);
        assert!(length.divergence.unwrap().contains("length divergence"));
    }

    #[test]
    fn driver_name_is_stable() {
        let f = fixture("name");
        assert_eq!(f.driver.driver_name(), "ehdb-local-reference");
    }

    #[test]
    fn content_path_strips_digest_prefix_into_safe_path() {
        let digest = ObjectDigest::sha256(b"anything");
        let path = content_path(&digest).unwrap();
        assert!(path.as_str().starts_with("objects/sha256/"));
        assert!(!path.as_str().contains(':'));
    }
}
