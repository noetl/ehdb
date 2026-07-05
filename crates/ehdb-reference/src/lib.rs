use std::{
    collections::BTreeSet,
    io::Cursor,
    path::{Path, PathBuf},
    sync::Arc,
    time::Instant,
};

use arrow_array::{Array, ArrayRef, Int64Array, RecordBatch, StringArray};
use arrow_ipc::{reader::FileReader, writer::FileWriter};
use arrow_schema::{Field, Schema};
use ehdb_catalog::{CommitSnapshot, CreateTable, GrantScan, InMemoryCatalog};
use ehdb_core::{
    ChunkId, ColumnSchema, ConsumerName, DataType, DocumentId, EhdbError, NamespaceName, Result,
    SnapshotId, StreamName, TableName, TableSchema, TenantId, TransactionId,
};
use ehdb_retrieval::{
    InMemoryRetrievalCatalog, RegisterChunk, RegisterDocument, RegisterEmbedding, TextSearch,
    TextSearchHit,
};
use ehdb_storage::{
    table_snapshot_object_path, ImmutableObjectStore, InMemoryObjectReplicaRegistry, ObjectPath,
    ObjectRef, ObjectReplica, ReplicationAction, ReplicationPlan,
};
use ehdb_stream::{InMemoryStreamLog, RetentionPolicy, StreamConfig, StreamSequence, Subject};
use ehdb_system::{
    BindSystemLibrary, EnvironmentName, InMemorySystemLibraryCatalog, ModuleDigest,
    PublishSystemLibrary, ReleaseChannel, ResolveSystemLibrary, SystemCapability,
    SystemLibraryPath, SystemLibraryRevision, WasmTarget,
};
use ehdb_transaction::{
    CatalogMutation, CommitTransaction, LocalJsonlTransactionLog, Mutation, RetrievalMutation,
    StorageMutation, StreamMutation, SystemMutation, TransactionRecord,
};
use serde::{Deserialize, Serialize};

/// EHDB event-log core engine (completion program Phase 6) — the durable
/// persistence + ordering + serving layer for NoETL's append-only event log,
/// exposed behind a driver interface.  See [`eventlog`].
pub mod eventlog;
pub use eventlog::{
    compare_shadow_parity, EventLogAckOutcome, EventLogAckRequest, EventLogAppendOutcome,
    EventLogAppendRequest, EventLogDriver, EventLogParityReport, EventLogReadExecutionOutcome,
    EventLogReadExecutionRequest, EventLogRecordView, EventLogScanOutcome, EventLogScanRequest,
    EventLogTailOutcome, EventLogTailRequest, LocalReferenceEventLogDriver, EVENT_LOG_STREAM,
    EVENT_LOG_SUBJECT_PREFIX,
};

/// EHDB projection / read-model core engine (completion program Phase 7) — builds
/// + serves the materialized read-models off the Phase-6 event-log tail, exposed
/// behind a driver interface, retiring the PostgreSQL materializer.  See
/// [`projection`].
pub mod projection;
pub use projection::{
    compare_projection_parity, AuthoritativeExecutionState, EventReadModelView, ExecutionStateView,
    LocalReferenceProjectionEngine, ProjectionApplyOutcome, ProjectionApplyRequest,
    ProjectionCheckpoint, ProjectionDriver, ProjectionEventInput, ProjectionListExecutionsOutcome,
    ProjectionParityReport, ProjectionReadEventOutcome, ProjectionReadExecutionOutcome,
    PROJECTION_STREAM, PROJECTION_SUBJECT_PREFIX,
};

/// EHDB KV / platform-state core engine (completion program Phase 8, slice 1) —
/// the durable key/value engine underneath NoETL's internal NATS-KV platform
/// state tier, exposed behind a driver interface.  See [`kv`].
pub mod kv;
pub use kv::{
    compare_kv_parity, AuthoritativeKvEntry, KvCasExpectation, KvDeleteOutcome, KvDeleteRequest,
    KvEntryView, KvGetOutcome, KvGetRequest, KvParityReport, KvPutOutcome, KvPutRequest,
    KvScanOutcome, KvScanRequest, KvStateDriver, LocalReferenceKvStateDriver, KV_STATE_STREAM,
    KV_SUBJECT_PREFIX, MAX_KV_SCAN_LIMIT, MAX_KV_VALUE_BYTES,
};

/// EHDB object / blob core engine (completion program Phase 8, slice 2) — the
/// durable content-addressed object engine underneath NoETL's internal external
/// object-store platform-artifact tier (state shards + result tier), exposed
/// behind a driver interface.  See [`object`].
pub mod object;
pub use object::{
    compare_object_parity, AuthoritativeObject, LocalReferenceObjectBlobDriver, ObjectBlobDriver,
    ObjectDeleteOutcome, ObjectDeleteRequest, ObjectEntryView, ObjectGetOutcome, ObjectGetRequest,
    ObjectListOutcome, ObjectListRequest, ObjectLocateOutcome, ObjectLocateRequest,
    ObjectParityReport, ObjectPutOutcome, ObjectPutRequest, MAX_OBJECT_BYTES,
    MAX_OBJECT_LIST_LIMIT, OBJECT_CONTENT_PREFIX, OBJECT_STORE_STREAM, OBJECT_SUBJECT_PREFIX,
};

#[derive(Debug, Clone, Default)]
pub struct ReferenceDatabase {
    pub catalog: InMemoryCatalog,
    pub streams: InMemoryStreamLog,
    pub retrieval: InMemoryRetrievalCatalog,
    pub system: InMemorySystemLibraryCatalog,
    pub storage: InMemoryObjectReplicaRegistry,
}

#[derive(Debug)]
pub struct LocalReferenceRuntime {
    log: LocalJsonlTransactionLog,
    state: ReferenceDatabase,
}

impl LocalReferenceRuntime {
    pub fn open(path: impl Into<PathBuf>) -> Result<Self> {
        let log = LocalJsonlTransactionLog::open(path)?;
        let mut state = ReferenceDatabase::default();
        let records = log.replay(None);
        state.apply_records(&records)?;
        Ok(Self { log, state })
    }

    pub fn append(&mut self, request: CommitTransaction) -> Result<TransactionRecord> {
        let mut next_state = self.state.clone();
        let preview = self.log.preview_record(request.clone())?;
        next_state.apply_record(&preview)?;

        let record = self.log.append(request)?;
        self.state = next_state;
        Ok(record)
    }

    pub fn replay(&self) -> Vec<TransactionRecord> {
        self.log.replay(None)
    }

    pub fn state(&self) -> &ReferenceDatabase {
        &self.state
    }

    pub fn path(&self) -> &Path {
        self.log.path()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LocalReferenceSummary {
    pub log_path: String,
    pub transaction_count: usize,
    pub table_count: usize,
    pub snapshot_count: usize,
    pub scan_grant_count: usize,
    pub stream_count: usize,
    pub stream_record_count: usize,
    pub stream_consumer_count: usize,
    pub retrieval_document_count: usize,
    pub retrieval_chunk_count: usize,
    pub retrieval_embedding_count: usize,
    pub system_library_count: usize,
    pub system_binding_count: usize,
    pub storage_object_count: usize,
    pub storage_replica_count: usize,
}

impl LocalReferenceSummary {
    pub fn from_runtime(runtime: &LocalReferenceRuntime) -> Self {
        let state = runtime.state();
        Self {
            log_path: runtime.path().display().to_string(),
            transaction_count: runtime.replay().len(),
            table_count: state.catalog.table_count(),
            snapshot_count: state.catalog.snapshot_count(),
            scan_grant_count: state.catalog.scan_grant_count(),
            stream_count: state.streams.stream_count(),
            stream_record_count: state.streams.record_count(),
            stream_consumer_count: state.streams.consumer_count(),
            retrieval_document_count: state.retrieval.document_count(),
            retrieval_chunk_count: state.retrieval.chunk_count(),
            retrieval_embedding_count: state.retrieval.embedding_count(),
            system_library_count: state.system.library_count(),
            system_binding_count: state.system.binding_count(),
            storage_object_count: state.storage.object_count(),
            storage_replica_count: state.storage.replica_count(),
        }
    }
}

pub fn summarize_local_reference(path: impl Into<PathBuf>) -> Result<LocalReferenceSummary> {
    let runtime = LocalReferenceRuntime::open(path)?;
    Ok(LocalReferenceSummary::from_runtime(&runtime))
}

pub fn summarize_local_reference_json(path: impl Into<PathBuf>) -> Result<String> {
    serde_json::to_string(&summarize_local_reference(path)?)
        .map_err(|err| EhdbError::InvalidState(format!("encode local reference summary: {err}")))
}

/// Default tenant for NoETL worker/playbook bounded domain-record operations.
pub const DEFAULT_LOCAL_REFERENCE_TENANT: &str = "noetl";
/// Default namespace for NoETL worker/playbook bounded domain-record operations.
pub const DEFAULT_LOCAL_REFERENCE_NAMESPACE: &str = "default";

/// Bounded append request for a single NoETL domain record.
///
/// A "domain record" is a UTF-8 text payload published to a named local
/// reference stream under a subject.  The operation is bounded (one commit,
/// one payload) and stateless (the runtime is opened, appended, and dropped).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AppendDomainRecordRequest {
    pub log_path: PathBuf,
    pub tenant: String,
    pub namespace: String,
    pub stream: String,
    pub subject: String,
    pub transaction_id: String,
    pub payload: String,
}

/// Secret-free result of appending a single domain record.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AppendDomainRecordOutcome {
    pub action: String,
    pub log_path: String,
    pub tenant: String,
    pub namespace: String,
    pub stream: String,
    pub subject: String,
    pub sequence: u64,
    pub byte_len: usize,
    pub created_stream: bool,
    pub stream_record_count: usize,
    pub transaction_count: usize,
}

/// Bounded read request for domain records on a single stream.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReadDomainRecordsRequest {
    pub log_path: PathBuf,
    pub tenant: String,
    pub namespace: String,
    pub stream: String,
    pub after: Option<u64>,
    pub limit: usize,
}

/// One replayed domain record projected for a bounded read.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DomainRecordView {
    pub sequence: u64,
    pub subject: String,
    pub transaction_id: String,
    pub byte_len: usize,
    pub payload: String,
}

/// Secret-free result of a bounded domain-record read.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReadDomainRecordsOutcome {
    pub action: String,
    pub log_path: String,
    pub tenant: String,
    pub namespace: String,
    pub stream: String,
    pub exists: bool,
    pub record_count: usize,
    pub returned: usize,
    pub records: Vec<DomainRecordView>,
}

/// Append one bounded domain record to a local reference stream.
///
/// The stream is created on first use with `KeepAll` retention, then the
/// payload is published under the given subject.  The operation is a single
/// atomic commit; on any validation failure nothing is written.
pub fn append_local_reference_domain_record(
    request: AppendDomainRecordRequest,
) -> Result<AppendDomainRecordOutcome> {
    let tenant = TenantId::new(request.tenant.clone())?;
    let namespace = NamespaceName::new(request.namespace.clone())?;
    let stream = StreamName::new(request.stream.clone())?;
    let subject = Subject::new(request.subject.clone())?;
    let transaction_id = TransactionId::new(request.transaction_id.clone())?;
    let payload = request.payload.into_bytes();
    let byte_len = payload.len();

    let mut runtime = LocalReferenceRuntime::open(&request.log_path)?;

    // Determine stream existence + next sequence from replayed state.  A
    // missing stream replays as an error; that is the create-on-first-use
    // signal, not a failure.
    let (created_stream, next_sequence) = match runtime
        .state()
        .streams
        .replay(&tenant, &namespace, &stream, None)
    {
        Ok(records) => (false, records.len() as u64 + 1),
        Err(_) => (true, StreamSequence::first().value()),
    };

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
        sequence: next_sequence,
    }));

    runtime.append(CommitTransaction {
        transaction_id,
        tenant: tenant.clone(),
        namespace: namespace.clone(),
        mutations,
    })?;

    let stream_record_count = runtime
        .state()
        .streams
        .replay(&tenant, &namespace, &stream, None)
        .map(|records| records.len())
        .unwrap_or(0);

    Ok(AppendDomainRecordOutcome {
        action: "append".to_string(),
        log_path: runtime.path().display().to_string(),
        tenant: tenant.to_string(),
        namespace: namespace.to_string(),
        stream: stream.to_string(),
        subject: subject.as_str().to_string(),
        sequence: next_sequence,
        byte_len,
        created_stream,
        stream_record_count,
        transaction_count: runtime.replay().len(),
    })
}

/// Read up to `limit` bounded domain records from a local reference stream.
///
/// A missing stream is reported as `exists: false` with an empty record set
/// rather than an error, so a reader can probe a stream that has never been
/// written.  Payloads are decoded as UTF-8 (lossily) for the projection.
pub fn read_local_reference_domain_records(
    request: ReadDomainRecordsRequest,
) -> Result<ReadDomainRecordsOutcome> {
    let tenant = TenantId::new(request.tenant.clone())?;
    let namespace = NamespaceName::new(request.namespace.clone())?;
    let stream = StreamName::new(request.stream.clone())?;
    let after = match request.after {
        Some(value) => Some(StreamSequence::new(value)?),
        None => None,
    };

    let runtime = LocalReferenceRuntime::open(&request.log_path)?;
    let log_path = runtime.path().display().to_string();

    match runtime
        .state()
        .streams
        .replay(&tenant, &namespace, &stream, after)
    {
        Ok(records) => {
            let record_count = records.len();
            let projected: Vec<DomainRecordView> = records
                .into_iter()
                .take(request.limit)
                .map(|record| DomainRecordView {
                    sequence: record.sequence.value(),
                    subject: record.subject.as_str().to_string(),
                    transaction_id: record.transaction_id.to_string(),
                    byte_len: record.payload.len(),
                    payload: String::from_utf8_lossy(&record.payload).into_owned(),
                })
                .collect();
            Ok(ReadDomainRecordsOutcome {
                action: "read".to_string(),
                log_path,
                tenant: tenant.to_string(),
                namespace: namespace.to_string(),
                stream: stream.to_string(),
                exists: true,
                record_count,
                returned: projected.len(),
                records: projected,
            })
        }
        Err(_) => Ok(ReadDomainRecordsOutcome {
            action: "read".to_string(),
            log_path,
            tenant: tenant.to_string(),
            namespace: namespace.to_string(),
            stream: stream.to_string(),
            exists: false,
            record_count: 0,
            returned: 0,
            records: Vec::new(),
        }),
    }
}

/// Append one domain record and return the outcome as a JSON string.
pub fn append_local_reference_domain_record_json(
    request: AppendDomainRecordRequest,
) -> Result<String> {
    serde_json::to_string(&append_local_reference_domain_record(request)?)
        .map_err(|err| EhdbError::InvalidState(format!("encode append outcome: {err}")))
}

/// Read domain records and return the outcome as a JSON string.
pub fn read_local_reference_domain_records_json(
    request: ReadDomainRecordsRequest,
) -> Result<String> {
    serde_json::to_string(&read_local_reference_domain_records(request)?)
        .map_err(|err| EhdbError::InvalidState(format!("encode read outcome: {err}")))
}

// ---------------------------------------------------------------------------
// Event-stream integration path (NoETL integration Phase D).
//
// The NoETL event log (Postgres `noetl.event` / NATS JetStream) remains the
// authoritative, append-only source of truth.  EHDB is a *derived, auxiliary*
// consumer of already-emitted NoETL events: a NoETL worker/playbook step
// projects an event payload into a local-reference EHDB stream (the existing
// `append` primitive), then drains it through a durable consumer with explicit
// ack-after-materialize semantics.  Nothing here writes back to the NoETL event
// log — the EHDB local-reference stream is a separate JSONL fabric, and these
// functions only ever touch that fabric.  Durable-consumer replay is bounded by
// an explicit `limit`; the ack cursor is never moved backwards.
// ---------------------------------------------------------------------------

/// Bounded pull request for a durable consumer over a local-reference stream.
///
/// A durable consumer models the NoETL command/event drain: it is created on
/// first pull (`transaction_id` names that create commit) and thereafter
/// remembers its ack cursor across process restarts (the cursor lives in the
/// transaction log).  The pull returns records *after* the cursor, bounded by
/// `limit`, without moving the cursor — advancing the cursor is a separate,
/// explicit `ack` after the caller has materialized the batch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConsumeEventRecordsRequest {
    pub log_path: PathBuf,
    pub tenant: String,
    pub namespace: String,
    pub stream: String,
    pub consumer: String,
    /// Transaction id for the create-consumer commit (used only on first pull).
    pub transaction_id: String,
    pub limit: usize,
}

/// Secret-free result of a bounded durable-consumer pull.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConsumeEventRecordsOutcome {
    pub action: String,
    pub log_path: String,
    pub tenant: String,
    pub namespace: String,
    pub stream: String,
    pub consumer: String,
    /// Whether the underlying stream exists yet.
    pub exists: bool,
    /// Whether the durable consumer was created on this pull.
    pub created_consumer: bool,
    /// The consumer ack cursor before this pull (`None` = nothing acked yet).
    pub acked_sequence: Option<u64>,
    /// Total records pending after the cursor (before `limit` is applied).
    pub pending_count: usize,
    /// Records actually returned (`min(pending_count, limit)`).
    pub returned: usize,
    pub records: Vec<DomainRecordView>,
    pub transaction_count: usize,
}

/// Bounded ack request advancing a durable consumer's cursor after materialize.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AckEventConsumerRequest {
    pub log_path: PathBuf,
    pub tenant: String,
    pub namespace: String,
    pub stream: String,
    pub consumer: String,
    /// Transaction id for the ack commit.
    pub transaction_id: String,
    /// The stream sequence being acked (must be a real record; nonzero).
    pub sequence: u64,
}

/// Secret-free result of advancing a durable consumer's ack cursor.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AckEventConsumerOutcome {
    pub action: String,
    pub log_path: String,
    pub tenant: String,
    pub namespace: String,
    pub stream: String,
    pub consumer: String,
    pub acked_sequence: u64,
    pub transaction_count: usize,
}

/// Pull up to `limit` records for a durable consumer over a local-reference
/// stream, creating the consumer on first use.
///
/// A missing stream is reported as `exists: false` with an empty batch (and no
/// consumer created) rather than an error, so a drain step can probe a stream
/// that has never received a projected event.  The pull does not move the ack
/// cursor; call [`ack_local_reference_event_consumer`] after materializing.
pub fn consume_local_reference_event_records(
    request: ConsumeEventRecordsRequest,
) -> Result<ConsumeEventRecordsOutcome> {
    let tenant = TenantId::new(request.tenant.clone())?;
    let namespace = NamespaceName::new(request.namespace.clone())?;
    let stream = StreamName::new(request.stream.clone())?;
    let consumer = ConsumerName::new(request.consumer.clone())?;

    let mut runtime = LocalReferenceRuntime::open(&request.log_path)?;
    let log_path = runtime.path().display().to_string();

    // A durable consumer over a stream that has never been written is a no-op
    // probe: report absent without creating consumer state.
    if runtime
        .state()
        .streams
        .replay(&tenant, &namespace, &stream, None)
        .is_err()
    {
        return Ok(ConsumeEventRecordsOutcome {
            action: "consume".to_string(),
            log_path,
            tenant: tenant.to_string(),
            namespace: namespace.to_string(),
            stream: stream.to_string(),
            consumer: consumer.to_string(),
            exists: false,
            created_consumer: false,
            acked_sequence: None,
            pending_count: 0,
            returned: 0,
            records: Vec::new(),
            transaction_count: runtime.replay().len(),
        });
    }

    // Create the durable consumer on first pull (JetStream-style durable).
    let created_consumer = runtime
        .state()
        .streams
        .consumer(&tenant, &namespace, &stream, &consumer)
        .is_err();
    if created_consumer {
        let transaction_id = TransactionId::new(request.transaction_id.clone())?;
        runtime.append(CommitTransaction {
            transaction_id,
            tenant: tenant.clone(),
            namespace: namespace.clone(),
            mutations: vec![Mutation::Stream(StreamMutation::CreateConsumer {
                stream: stream.clone(),
                consumer: consumer.clone(),
            })],
        })?;
    }

    let acked_sequence = runtime
        .state()
        .streams
        .consumer(&tenant, &namespace, &stream, &consumer)
        .ok()
        .and_then(|durable| durable.acked_sequence.map(|sequence| sequence.value()));

    let pending = runtime
        .state()
        .streams
        .replay_for_consumer(&tenant, &namespace, &stream, &consumer)?;
    let pending_count = pending.len();
    let records: Vec<DomainRecordView> = pending
        .into_iter()
        .take(request.limit)
        .map(|record| DomainRecordView {
            sequence: record.sequence.value(),
            subject: record.subject.as_str().to_string(),
            transaction_id: record.transaction_id.to_string(),
            byte_len: record.payload.len(),
            payload: String::from_utf8_lossy(&record.payload).into_owned(),
        })
        .collect();
    let returned = records.len();

    Ok(ConsumeEventRecordsOutcome {
        action: "consume".to_string(),
        log_path,
        tenant: tenant.to_string(),
        namespace: namespace.to_string(),
        stream: stream.to_string(),
        consumer: consumer.to_string(),
        exists: true,
        created_consumer,
        acked_sequence,
        pending_count,
        returned,
        records,
        transaction_count: runtime.replay().len(),
    })
}

/// Advance a durable consumer's ack cursor to `sequence` after materialize.
///
/// The commit is atomic: the sequence must reference a real record and may not
/// move the cursor backwards (both enforced during replay-preview before
/// anything is written).  This is the explicit ack-after-materialize step of
/// the drain contract.
pub fn ack_local_reference_event_consumer(
    request: AckEventConsumerRequest,
) -> Result<AckEventConsumerOutcome> {
    let tenant = TenantId::new(request.tenant.clone())?;
    let namespace = NamespaceName::new(request.namespace.clone())?;
    let stream = StreamName::new(request.stream.clone())?;
    let consumer = ConsumerName::new(request.consumer.clone())?;
    let transaction_id = TransactionId::new(request.transaction_id.clone())?;
    // Reject sequence 0 up front (StreamSequence is nonzero); a real ack always
    // names a published record.
    let sequence = StreamSequence::new(request.sequence)?;

    let mut runtime = LocalReferenceRuntime::open(&request.log_path)?;

    runtime.append(CommitTransaction {
        transaction_id,
        tenant: tenant.clone(),
        namespace: namespace.clone(),
        mutations: vec![Mutation::Stream(StreamMutation::Ack {
            stream: stream.clone(),
            consumer: consumer.clone(),
            sequence: sequence.value(),
        })],
    })?;

    Ok(AckEventConsumerOutcome {
        action: "ack".to_string(),
        log_path: runtime.path().display().to_string(),
        tenant: tenant.to_string(),
        namespace: namespace.to_string(),
        stream: stream.to_string(),
        consumer: consumer.to_string(),
        acked_sequence: sequence.value(),
        transaction_count: runtime.replay().len(),
    })
}

/// Pull durable-consumer records and return the outcome as a JSON string.
pub fn consume_local_reference_event_records_json(
    request: ConsumeEventRecordsRequest,
) -> Result<String> {
    serde_json::to_string(&consume_local_reference_event_records(request)?)
        .map_err(|err| EhdbError::InvalidState(format!("encode consume outcome: {err}")))
}

/// Advance a durable consumer cursor and return the outcome as a JSON string.
pub fn ack_local_reference_event_consumer_json(request: AckEventConsumerRequest) -> Result<String> {
    serde_json::to_string(&ack_local_reference_event_consumer(request)?)
        .map_err(|err| EhdbError::InvalidState(format!("encode ack outcome: {err}")))
}

// ---------------------------------------------------------------------------
// System WASM library store integration path (NoETL integration Phase E).
//
// EHDB owns the durable catalog side of NoETL's system WASM library model:
// immutable module manifests (path/revision/digest/entry/target/object/caps)
// and mutable environment/channel bindings that resolve a logical library to a
// concrete module for a tenant/namespace/environment/channel.  WASM *execution*
// stays in the worker/system-pool host; these bounded helpers only publish a
// manifest, (re)bind a channel, and resolve the active module ref that host
// then loads.  Each op is one atomic transaction-log commit (publish/bind) or a
// read-only replay (resolve), opened + dropped per call, so the runtime is
// bounded and stateless — the same discipline as the Phase C/D helpers.
//
// Rebinding a channel to a new revision/digest hot-replaces the active module
// while every previously-published immutable manifest is retained, so a
// resolve after a rebind returns the new module and the old manifests stay
// addressable in the log.

/// Parse a NoETL-style WASM target token (`wasm32-unknown-unknown` /
/// `wasm32-wasi-preview1`) into a [`WasmTarget`].
fn parse_wasm_target(raw: &str) -> Result<WasmTarget> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "wasm32-unknown-unknown" | "wasm32_unknown_unknown" => Ok(WasmTarget::Wasm32UnknownUnknown),
        "wasm32-wasi-preview1" | "wasm32_wasi_preview1" => Ok(WasmTarget::Wasm32WasiPreview1),
        other => Err(EhdbError::InvalidState(format!(
            "unsupported system library wasm target: {other}"
        ))),
    }
}

/// Parse a NoETL-style host-capability token into a [`SystemCapability`].
fn parse_system_capability(raw: &str) -> Result<SystemCapability> {
    match raw.trim().to_ascii_lowercase().as_str() {
        "event_publish" => Ok(SystemCapability::EventPublish),
        "object_put" => Ok(SystemCapability::ObjectPut),
        "result_put" => Ok(SystemCapability::ResultPut),
        "ehdb_catalog_read" => Ok(SystemCapability::EhdbCatalogRead),
        "ehdb_catalog_write" => Ok(SystemCapability::EhdbCatalogWrite),
        "ehdb_stream_publish" => Ok(SystemCapability::EhdbStreamPublish),
        "ehdb_retrieval_write" => Ok(SystemCapability::EhdbRetrievalWrite),
        other => Err(EhdbError::InvalidState(format!(
            "unsupported system library host capability: {other}"
        ))),
    }
}

fn wasm_target_str(target: &WasmTarget) -> &'static str {
    match target {
        WasmTarget::Wasm32UnknownUnknown => "wasm32-unknown-unknown",
        WasmTarget::Wasm32WasiPreview1 => "wasm32-wasi-preview1",
    }
}

fn system_capability_str(capability: &SystemCapability) -> &'static str {
    match capability {
        SystemCapability::EventPublish => "event_publish",
        SystemCapability::ObjectPut => "object_put",
        SystemCapability::ResultPut => "result_put",
        SystemCapability::EhdbCatalogRead => "ehdb_catalog_read",
        SystemCapability::EhdbCatalogWrite => "ehdb_catalog_write",
        SystemCapability::EhdbStreamPublish => "ehdb_stream_publish",
        SystemCapability::EhdbRetrievalWrite => "ehdb_retrieval_write",
    }
}

/// Bounded request to publish one immutable system WASM library manifest.
///
/// String/scalar fields are validated + parsed inside the helper (the caller —
/// worker-rust — stays decoupled from the `ehdb_system` typed identifiers).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublishSystemModuleRequest {
    pub log_path: PathBuf,
    pub tenant: String,
    pub namespace: String,
    pub path: String,
    pub revision: u32,
    pub digest: String,
    pub entry: String,
    pub target: String,
    pub object_path: String,
    pub byte_len: u64,
    pub capabilities: Vec<String>,
    pub transaction_id: String,
}

/// Secret-free result of publishing a system WASM library manifest.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PublishSystemModuleOutcome {
    pub action: String,
    pub log_path: String,
    pub tenant: String,
    pub namespace: String,
    pub path: String,
    pub revision: u32,
    pub digest: String,
    pub entry: String,
    pub target: String,
    pub byte_len: u64,
    pub capabilities: Vec<String>,
    pub library_count: usize,
    pub transaction_count: usize,
}

/// Bounded request to bind a release channel to a published module revision.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BindSystemChannelRequest {
    pub log_path: PathBuf,
    pub tenant: String,
    pub namespace: String,
    pub environment: String,
    pub channel: String,
    pub path: String,
    pub revision: u32,
    pub digest: String,
    pub transaction_id: String,
}

/// Secret-free result of binding a release channel.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BindSystemChannelOutcome {
    pub action: String,
    pub log_path: String,
    pub tenant: String,
    pub namespace: String,
    pub environment: String,
    pub channel: String,
    pub path: String,
    pub revision: u32,
    pub digest: String,
    pub binding_count: usize,
    pub transaction_count: usize,
}

/// Bounded request to resolve the active module for a channel binding.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolveSystemModuleRequest {
    pub log_path: PathBuf,
    pub tenant: String,
    pub namespace: String,
    pub environment: String,
    pub channel: String,
    pub path: String,
}

/// Secret-free result of resolving the active module for a channel binding.
///
/// A never-bound (or unpublished) channel resolves to `exists: false` with an
/// empty module ref rather than an error, mirroring the read/consume "absent
/// probe" contract so a caller can safely probe a channel it has not bound.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResolveSystemModuleOutcome {
    pub action: String,
    pub log_path: String,
    pub tenant: String,
    pub namespace: String,
    pub environment: String,
    pub channel: String,
    pub path: String,
    pub exists: bool,
    pub revision: Option<u32>,
    pub digest: Option<String>,
    pub entry: Option<String>,
    pub target: Option<String>,
    pub object_path: Option<String>,
    pub byte_len: Option<u64>,
    pub capabilities: Vec<String>,
}

/// Publish one immutable system WASM library manifest as a single atomic
/// transaction-log commit.  On any validation failure nothing is written.
/// Re-publishing an identical (path, revision, digest) is rejected by the
/// engine (`AlreadyExists`) — manifests are immutable.
pub fn publish_local_reference_system_module(
    request: PublishSystemModuleRequest,
) -> Result<PublishSystemModuleOutcome> {
    let tenant = TenantId::new(request.tenant.clone())?;
    let namespace = NamespaceName::new(request.namespace.clone())?;
    let path = SystemLibraryPath::new(request.path.clone())?;
    let revision = SystemLibraryRevision::new(request.revision)?;
    let digest = ModuleDigest::new(request.digest.clone())?;
    let target = parse_wasm_target(&request.target)?;
    let object_path = ObjectPath::new(request.object_path.clone())?;
    let transaction_id = TransactionId::new(request.transaction_id.clone())?;
    let capabilities = request
        .capabilities
        .iter()
        .map(|c| parse_system_capability(c))
        .collect::<Result<Vec<_>>>()?;

    let mut runtime = LocalReferenceRuntime::open(&request.log_path)?;

    runtime.append(CommitTransaction {
        transaction_id,
        tenant: tenant.clone(),
        namespace: namespace.clone(),
        mutations: vec![Mutation::System(SystemMutation::PublishLibrary {
            path: path.clone(),
            revision,
            digest: digest.clone(),
            entry: request.entry.clone(),
            target: target.clone(),
            object_path,
            byte_len: request.byte_len,
            capabilities: capabilities.clone(),
        })],
    })?;

    Ok(PublishSystemModuleOutcome {
        action: "publish".to_string(),
        log_path: runtime.path().display().to_string(),
        tenant: tenant.to_string(),
        namespace: namespace.to_string(),
        path: path.as_str().to_string(),
        revision: revision.value(),
        digest: digest.as_str().to_string(),
        entry: request.entry,
        target: wasm_target_str(&target).to_string(),
        byte_len: request.byte_len,
        capabilities: capabilities
            .iter()
            .map(|c| system_capability_str(c).to_string())
            .collect(),
        library_count: runtime.state().system.library_count(),
        transaction_count: runtime.replay().len(),
    })
}

/// Bind (or hot-rebind) a release channel to a published module revision as a
/// single atomic transaction-log commit.  The target revision/digest must
/// already be published (`NotFound` otherwise).  Rebinding an existing channel
/// replaces the active module while every prior immutable manifest is retained.
pub fn bind_local_reference_system_channel(
    request: BindSystemChannelRequest,
) -> Result<BindSystemChannelOutcome> {
    let tenant = TenantId::new(request.tenant.clone())?;
    let namespace = NamespaceName::new(request.namespace.clone())?;
    let environment = EnvironmentName::new(request.environment.clone())?;
    let channel = ReleaseChannel::new(request.channel.clone())?;
    let path = SystemLibraryPath::new(request.path.clone())?;
    let revision = SystemLibraryRevision::new(request.revision)?;
    let digest = ModuleDigest::new(request.digest.clone())?;
    let transaction_id = TransactionId::new(request.transaction_id.clone())?;

    let mut runtime = LocalReferenceRuntime::open(&request.log_path)?;

    // tenant/namespace ride the enclosing CommitTransaction (see apply_system).
    runtime.append(CommitTransaction {
        transaction_id,
        tenant: tenant.clone(),
        namespace: namespace.clone(),
        mutations: vec![Mutation::System(SystemMutation::BindLibrary {
            path: path.clone(),
            environment: environment.clone(),
            channel: channel.clone(),
            revision,
            digest: digest.clone(),
        })],
    })?;

    Ok(BindSystemChannelOutcome {
        action: "bind".to_string(),
        log_path: runtime.path().display().to_string(),
        tenant: tenant.to_string(),
        namespace: namespace.to_string(),
        environment: environment.as_str().to_string(),
        channel: channel.as_str().to_string(),
        path: path.as_str().to_string(),
        revision: revision.value(),
        digest: digest.as_str().to_string(),
        binding_count: runtime.state().system.binding_count(),
        transaction_count: runtime.replay().len(),
    })
}

/// Resolve the active module a channel binding points at (read-only replay).
///
/// A never-bound channel resolves to `exists: false` rather than an error, so a
/// caller can probe a channel it has not bound.  Any other error propagates.
pub fn resolve_local_reference_system_module(
    request: ResolveSystemModuleRequest,
) -> Result<ResolveSystemModuleOutcome> {
    let tenant = TenantId::new(request.tenant.clone())?;
    let namespace = NamespaceName::new(request.namespace.clone())?;
    let environment = EnvironmentName::new(request.environment.clone())?;
    let channel = ReleaseChannel::new(request.channel.clone())?;
    let path = SystemLibraryPath::new(request.path.clone())?;

    let runtime = LocalReferenceRuntime::open(&request.log_path)?;
    let log_path = runtime.path().display().to_string();

    let absent = |log_path: String| ResolveSystemModuleOutcome {
        action: "resolve".to_string(),
        log_path,
        tenant: tenant.to_string(),
        namespace: namespace.to_string(),
        environment: environment.as_str().to_string(),
        channel: channel.as_str().to_string(),
        path: path.as_str().to_string(),
        exists: false,
        revision: None,
        digest: None,
        entry: None,
        target: None,
        object_path: None,
        byte_len: None,
        capabilities: Vec::new(),
    };

    match runtime.state().system.resolve(ResolveSystemLibrary {
        tenant: tenant.clone(),
        namespace: namespace.clone(),
        environment: environment.clone(),
        channel: channel.clone(),
        path: path.clone(),
    }) {
        Ok(library) => Ok(ResolveSystemModuleOutcome {
            action: "resolve".to_string(),
            log_path,
            tenant: tenant.to_string(),
            namespace: namespace.to_string(),
            environment: environment.as_str().to_string(),
            channel: channel.as_str().to_string(),
            path: library.path.as_str().to_string(),
            exists: true,
            revision: Some(library.revision.value()),
            digest: Some(library.digest.as_str().to_string()),
            entry: Some(library.entry.clone()),
            target: Some(wasm_target_str(&library.target).to_string()),
            object_path: Some(library.object_path.as_str().to_string()),
            byte_len: Some(library.byte_len),
            capabilities: library
                .capabilities
                .iter()
                .map(|c| system_capability_str(c).to_string())
                .collect(),
        }),
        // An unbound channel (or a binding whose manifest is missing) is the
        // absent-probe signal, not a failure.
        Err(EhdbError::NotFound(_)) => Ok(absent(log_path)),
        Err(err) => Err(err),
    }
}

/// Publish a system WASM library manifest and return the outcome as JSON.
pub fn publish_local_reference_system_module_json(
    request: PublishSystemModuleRequest,
) -> Result<String> {
    serde_json::to_string(&publish_local_reference_system_module(request)?)
        .map_err(|err| EhdbError::InvalidState(format!("encode publish outcome: {err}")))
}

/// Bind a release channel and return the outcome as JSON.
pub fn bind_local_reference_system_channel_json(
    request: BindSystemChannelRequest,
) -> Result<String> {
    serde_json::to_string(&bind_local_reference_system_channel(request)?)
        .map_err(|err| EhdbError::InvalidState(format!("encode bind outcome: {err}")))
}

/// Resolve the active module for a channel binding and return the outcome as
/// JSON.
pub fn resolve_local_reference_system_module_json(
    request: ResolveSystemModuleRequest,
) -> Result<String> {
    serde_json::to_string(&resolve_local_reference_system_module(request)?)
        .map_err(|err| EhdbError::InvalidState(format!("encode resolve outcome: {err}")))
}

// ---------------------------------------------------------------------------
// Bounded RAG retrieval path (EHDB Phase E, noetl/ehdb#234).
//
// The system-WASM store slice (publish/bind/resolve above) is one half of the
// Phase E direction; bounded RAG retrieval is the other.  These helpers let a
// NoETL worker/playbook/system role ingest bounded retrieval documents into the
// derived local-reference EHDB fabric and run a bounded, read-only text search
// over it via the existing `ehdb_retrieval` search under the hood.
//
// Boundaries mirror the domain-record / event-stream / system-store helpers:
//
// * **Read-only retrieve** — `retrieve_local_reference_context` opens the log,
//   replays it, searches, and drops the runtime; it NEVER writes the log (and
//   never touches `noetl.event`, which stays the append-only source of truth —
//   this is a separate JSONL fabric).
// * **Bounded** — three caps enforced *inside* the retrieve helper: a top-k cap
//   (`MAX_RETRIEVAL_TOP_K`), a per-hit result-size cap
//   (`MAX_RETRIEVAL_MAX_CHUNK_BYTES`, chunk text truncated on a char boundary),
//   and a wall-clock budget (`MAX_RETRIEVAL_TIME_BUDGET_MS`, surfaced via
//   `time_capped`).  Over-ceiling caps are `Rejected`; an empty query or a bad
//   tenant/namespace id is `Invalid` — both classified without searching, so a
//   CLI can map the outcome to a distinct exit code.
// * **Secret-free** — the result carries retrieval content (the whole point of
//   RAG) but no credential/env values; the ingest side stores only what the
//   caller supplies.
// ---------------------------------------------------------------------------

/// Default top-k when a retrieve request leaves `top_k` at 0.
pub const DEFAULT_RETRIEVAL_TOP_K: usize = 8;
/// Hard ceiling on retrieve top-k; a request above this is `Rejected`.
pub const MAX_RETRIEVAL_TOP_K: usize = 64;
/// Default per-hit text byte cap when a retrieve request leaves it at 0.
pub const DEFAULT_RETRIEVAL_MAX_CHUNK_BYTES: usize = 4_096;
/// Hard ceiling on the per-hit text byte cap; a request above this is `Rejected`.
pub const MAX_RETRIEVAL_MAX_CHUNK_BYTES: usize = 65_536;
/// Default wall-clock budget (ms) when a retrieve request leaves it at 0.
pub const DEFAULT_RETRIEVAL_TIME_BUDGET_MS: u64 = 5_000;
/// Hard ceiling on the retrieve wall-clock budget (ms); above this is `Rejected`.
pub const MAX_RETRIEVAL_TIME_BUDGET_MS: u64 = 60_000;
/// Hard ceiling on the number of chunks accepted in one ingest commit.
pub const MAX_RETRIEVAL_INGEST_CHUNKS: usize = 256;
/// Hard ceiling on a single ingested chunk's UTF-8 byte length.
pub const MAX_RETRIEVAL_INGEST_CHUNK_BYTES: usize = 65_536;

/// The classified outcome of a bounded retrieval.
///
/// `Hit` / `Empty` are the ok cases (a search ran); `Rejected` (an over-ceiling
/// cap) and `Invalid` (an empty query / bad identifier) are classified without
/// searching so a caller can map them to distinct exit codes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RetrievalOutcome {
    Hit,
    Empty,
    Rejected,
    Invalid,
}

impl RetrievalOutcome {
    pub fn as_str(&self) -> &'static str {
        match self {
            RetrievalOutcome::Hit => "hit",
            RetrievalOutcome::Empty => "empty",
            RetrievalOutcome::Rejected => "rejected",
            RetrievalOutcome::Invalid => "invalid",
        }
    }

    /// Whether a search actually ran (a hit or a clean empty result).
    pub fn ok(&self) -> bool {
        matches!(self, RetrievalOutcome::Hit | RetrievalOutcome::Empty)
    }
}

/// One chunk of a document to ingest into the derived retrieval fabric.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IngestChunkInput {
    pub chunk_id: String,
    pub ordinal: u32,
    pub text: String,
    pub checksum: String,
}

/// Bounded request to ingest one document + its chunks into the derived
/// local-reference retrieval fabric as a single atomic commit.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IngestRetrievalDocumentRequest {
    pub log_path: PathBuf,
    pub tenant: String,
    pub namespace: String,
    pub document_id: String,
    pub source_uri: String,
    pub content_type: String,
    pub transaction_id: String,
    pub chunks: Vec<IngestChunkInput>,
}

/// Secret-free result of ingesting one retrieval document.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IngestRetrievalDocumentOutcome {
    pub action: String,
    pub log_path: String,
    pub tenant: String,
    pub namespace: String,
    pub document_id: String,
    pub chunk_count: usize,
    pub document_count: usize,
    pub retrieval_chunk_count: usize,
    pub transaction_count: usize,
}

/// Bounded, read-only retrieval request over the derived retrieval fabric.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RetrieveContextRequest {
    pub log_path: PathBuf,
    pub tenant: String,
    pub namespace: String,
    pub query: String,
    /// Max hits to return (0 ⇒ [`DEFAULT_RETRIEVAL_TOP_K`]; over
    /// [`MAX_RETRIEVAL_TOP_K`] ⇒ `Rejected`).
    pub top_k: usize,
    /// Per-hit chunk-text byte cap (0 ⇒ [`DEFAULT_RETRIEVAL_MAX_CHUNK_BYTES`];
    /// over [`MAX_RETRIEVAL_MAX_CHUNK_BYTES`] ⇒ `Rejected`).
    pub max_chunk_bytes: usize,
    /// Wall-clock budget in ms (0 ⇒ [`DEFAULT_RETRIEVAL_TIME_BUDGET_MS`]; over
    /// [`MAX_RETRIEVAL_TIME_BUDGET_MS`] ⇒ `Rejected`).  Surfaced via
    /// `time_capped` when the observed elapsed time exceeds it.
    pub time_budget_ms: u64,
}

/// One returned retrieval hit (chunk text truncated to the size cap).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RetrievalHitView {
    pub document_id: String,
    pub chunk_id: String,
    pub ordinal: u32,
    pub match_count: usize,
    /// Full chunk text byte length, before the size cap truncated it.
    pub byte_len: usize,
    /// Whether `text` was truncated to fit the per-hit size cap.
    pub truncated: bool,
    pub text: String,
}

/// Secret-free result of a bounded retrieval.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RetrieveContextOutcome {
    pub action: String,
    pub outcome: RetrievalOutcome,
    pub log_path: String,
    pub tenant: String,
    pub namespace: String,
    pub top_k: usize,
    pub max_chunk_bytes: usize,
    pub time_budget_ms: u64,
    /// Candidate hits found before the top-k cap truncated them (bounded by
    /// [`MAX_RETRIEVAL_TOP_K`]).
    pub candidate_count: usize,
    pub returned: usize,
    pub truncated_by_top_k: bool,
    pub elapsed_ms: u64,
    /// Whether the observed elapsed time exceeded the wall-clock budget.
    pub time_capped: bool,
    pub detail: Option<String>,
    pub hits: Vec<RetrievalHitView>,
}

/// Ingest one document + its chunks into the derived retrieval fabric as one
/// atomic transaction-log commit.  Bounded: at most
/// [`MAX_RETRIEVAL_INGEST_CHUNKS`] chunks, each at most
/// [`MAX_RETRIEVAL_INGEST_CHUNK_BYTES`] UTF-8 bytes.  Writes only this derived
/// JSONL fabric — never `noetl.event`.
pub fn ingest_local_reference_retrieval_document(
    request: IngestRetrievalDocumentRequest,
) -> Result<IngestRetrievalDocumentOutcome> {
    if request.chunks.is_empty() {
        return Err(EhdbError::InvalidState(
            "retrieval ingest requires at least one chunk".to_string(),
        ));
    }
    if request.chunks.len() > MAX_RETRIEVAL_INGEST_CHUNKS {
        return Err(EhdbError::InvalidState(format!(
            "retrieval ingest {} chunks exceeds bound {MAX_RETRIEVAL_INGEST_CHUNKS}",
            request.chunks.len()
        )));
    }
    for chunk in &request.chunks {
        if chunk.text.len() > MAX_RETRIEVAL_INGEST_CHUNK_BYTES {
            return Err(EhdbError::InvalidState(format!(
                "retrieval ingest chunk {} is {} bytes, exceeds bound {MAX_RETRIEVAL_INGEST_CHUNK_BYTES}",
                chunk.chunk_id,
                chunk.text.len()
            )));
        }
    }

    let tenant = TenantId::new(request.tenant.clone())?;
    let namespace = NamespaceName::new(request.namespace.clone())?;
    let document_id = DocumentId::new(request.document_id.clone())?;
    let transaction_id = TransactionId::new(request.transaction_id.clone())?;

    let mut mutations = Vec::with_capacity(1 + request.chunks.len());
    mutations.push(Mutation::Retrieval(RetrievalMutation::RegisterDocument {
        document_id: document_id.clone(),
        source_uri: request.source_uri.clone(),
        content_type: request.content_type.clone(),
    }));
    for chunk in &request.chunks {
        let chunk_id = ChunkId::new(chunk.chunk_id.clone())?;
        mutations.push(Mutation::Retrieval(RetrievalMutation::RegisterChunk {
            document_id: document_id.clone(),
            chunk_id,
            ordinal: chunk.ordinal,
            text: chunk.text.clone(),
            checksum: chunk.checksum.clone(),
        }));
    }

    let mut runtime = LocalReferenceRuntime::open(&request.log_path)?;
    runtime.append(CommitTransaction {
        transaction_id,
        tenant: tenant.clone(),
        namespace: namespace.clone(),
        mutations,
    })?;

    Ok(IngestRetrievalDocumentOutcome {
        action: "ingest".to_string(),
        log_path: runtime.path().display().to_string(),
        tenant: tenant.to_string(),
        namespace: namespace.to_string(),
        document_id: document_id.to_string(),
        chunk_count: request.chunks.len(),
        document_count: runtime.state().retrieval.document_count(),
        retrieval_chunk_count: runtime.state().retrieval.chunk_count(),
        transaction_count: runtime.replay().len(),
    })
}

/// Truncate `text` to at most `max_bytes` UTF-8 bytes on a char boundary,
/// returning the (possibly truncated) slice and whether truncation happened.
fn truncate_on_char_boundary(text: &str, max_bytes: usize) -> (String, bool) {
    if text.len() <= max_bytes {
        return (text.to_string(), false);
    }
    let mut end = max_bytes;
    while end > 0 && !text.is_char_boundary(end) {
        end -= 1;
    }
    (text[..end].to_string(), true)
}

/// Bounded, read-only retrieval over the derived retrieval fabric.
///
/// Runs a tenant/namespace-scoped text search under the hood ([`TextSearch`]),
/// applying the three caps (top-k / per-hit size / wall-clock budget) inside the
/// helper.  A missing/empty log yields `Empty` (no docs in scope), not an error.
/// The runtime is opened read-only and dropped; nothing is ever written.
pub fn retrieve_local_reference_context(
    request: RetrieveContextRequest,
) -> Result<RetrieveContextOutcome> {
    let started = Instant::now();

    // Classified early-return without searching: same shape, no hits.
    let classify = |outcome: RetrievalOutcome, detail: Option<String>| RetrieveContextOutcome {
        action: "retrieve".to_string(),
        outcome,
        log_path: request.log_path.display().to_string(),
        tenant: request.tenant.clone(),
        namespace: request.namespace.clone(),
        top_k: request.top_k,
        max_chunk_bytes: request.max_chunk_bytes,
        time_budget_ms: request.time_budget_ms,
        candidate_count: 0,
        returned: 0,
        truncated_by_top_k: false,
        elapsed_ms: started.elapsed().as_millis() as u64,
        time_capped: false,
        detail,
        hits: Vec::new(),
    };

    // Over-ceiling caps ⇒ Rejected (no search).
    if request.top_k > MAX_RETRIEVAL_TOP_K {
        return Ok(classify(
            RetrievalOutcome::Rejected,
            Some(format!(
                "top_k {} exceeds bound {MAX_RETRIEVAL_TOP_K}",
                request.top_k
            )),
        ));
    }
    if request.max_chunk_bytes > MAX_RETRIEVAL_MAX_CHUNK_BYTES {
        return Ok(classify(
            RetrievalOutcome::Rejected,
            Some(format!(
                "max_chunk_bytes {} exceeds bound {MAX_RETRIEVAL_MAX_CHUNK_BYTES}",
                request.max_chunk_bytes
            )),
        ));
    }
    if request.time_budget_ms > MAX_RETRIEVAL_TIME_BUDGET_MS {
        return Ok(classify(
            RetrievalOutcome::Rejected,
            Some(format!(
                "time_budget_ms {} exceeds bound {MAX_RETRIEVAL_TIME_BUDGET_MS}",
                request.time_budget_ms
            )),
        ));
    }

    // Invalid input ⇒ Invalid (no search).
    if request.query.trim().is_empty() {
        return Ok(classify(
            RetrievalOutcome::Invalid,
            Some("retrieval query must not be empty".to_string()),
        ));
    }
    let tenant = match TenantId::new(request.tenant.clone()) {
        Ok(t) => t,
        Err(err) => return Ok(classify(RetrievalOutcome::Invalid, Some(err.to_string()))),
    };
    let namespace = match NamespaceName::new(request.namespace.clone()) {
        Ok(n) => n,
        Err(err) => return Ok(classify(RetrievalOutcome::Invalid, Some(err.to_string()))),
    };

    // Resolve 0 ⇒ default for the effective caps.
    let top_k = if request.top_k == 0 {
        DEFAULT_RETRIEVAL_TOP_K
    } else {
        request.top_k
    };
    let max_chunk_bytes = if request.max_chunk_bytes == 0 {
        DEFAULT_RETRIEVAL_MAX_CHUNK_BYTES
    } else {
        request.max_chunk_bytes
    };
    let time_budget_ms = if request.time_budget_ms == 0 {
        DEFAULT_RETRIEVAL_TIME_BUDGET_MS
    } else {
        request.time_budget_ms
    };

    // The log open + replay is the expensive part.  A genuine IO/corruption
    // error propagates as `Err`; a fresh/missing log opens empty and searches to
    // an `Empty` result (no docs in scope).
    let runtime = LocalReferenceRuntime::open(&request.log_path)?;
    let log_path = runtime.path().display().to_string();

    // Search up to the ceiling to learn the candidate count, then truncate to
    // the effective top-k so `truncated_by_top_k` is exact.
    let candidates: Vec<TextSearchHit> = match runtime.state().retrieval.search_text(TextSearch {
        tenant: tenant.clone(),
        namespace: namespace.clone(),
        query: request.query.clone(),
        limit: MAX_RETRIEVAL_TOP_K,
    }) {
        Ok(hits) => hits,
        // The only in-scope failure is an empty query, already guarded above; any
        // other validation error is surfaced as Invalid rather than a hard error.
        Err(err) => {
            let mut out = classify(RetrievalOutcome::Invalid, Some(err.to_string()));
            out.log_path = log_path;
            return Ok(out);
        }
    };

    let candidate_count = candidates.len();
    let truncated_by_top_k = candidate_count > top_k;
    let hits: Vec<RetrievalHitView> = candidates
        .into_iter()
        .take(top_k)
        .map(|hit| {
            let byte_len = hit.chunk.text.len();
            let (text, truncated) = truncate_on_char_boundary(&hit.chunk.text, max_chunk_bytes);
            RetrievalHitView {
                document_id: hit.chunk.document_id.to_string(),
                chunk_id: hit.chunk.id.to_string(),
                ordinal: hit.chunk.ordinal,
                match_count: hit.match_count,
                byte_len,
                truncated,
                text,
            }
        })
        .collect();

    let returned = hits.len();
    let outcome = if returned == 0 {
        RetrievalOutcome::Empty
    } else {
        RetrievalOutcome::Hit
    };
    let elapsed_ms = started.elapsed().as_millis() as u64;

    Ok(RetrieveContextOutcome {
        action: "retrieve".to_string(),
        outcome,
        log_path,
        tenant: tenant.to_string(),
        namespace: namespace.to_string(),
        top_k,
        max_chunk_bytes,
        time_budget_ms,
        candidate_count,
        returned,
        truncated_by_top_k,
        elapsed_ms,
        time_capped: elapsed_ms > time_budget_ms,
        detail: None,
        hits,
    })
}

/// Ingest one retrieval document and return the outcome as JSON.
pub fn ingest_local_reference_retrieval_document_json(
    request: IngestRetrievalDocumentRequest,
) -> Result<String> {
    serde_json::to_string(&ingest_local_reference_retrieval_document(request)?)
        .map_err(|err| EhdbError::InvalidState(format!("encode ingest outcome: {err}")))
}

/// Run a bounded retrieval and return the outcome as JSON.
pub fn retrieve_local_reference_context_json(request: RetrieveContextRequest) -> Result<String> {
    serde_json::to_string(&retrieve_local_reference_context(request)?)
        .map_err(|err| EhdbError::InvalidState(format!("encode retrieve outcome: {err}")))
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExecuteReplication {
    pub tenant: TenantId,
    pub namespace: NamespaceName,
    pub transaction_id: TransactionId,
    pub source: ObjectRef,
    pub plan: ReplicationPlan,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ReplicationExecution {
    pub registered: Vec<ObjectReplica>,
    pub record: Option<TransactionRecord>,
}

#[derive(Debug, Default)]
pub struct LocalReplicationExecutor;

impl LocalReplicationExecutor {
    pub fn execute<S: ImmutableObjectStore>(
        &self,
        runtime: &mut LocalReferenceRuntime,
        store: &S,
        request: ExecuteReplication,
    ) -> Result<ReplicationExecution> {
        validate_plan_matches_source(&request.source, &request.plan)?;

        let replicas = replicas_to_register(&request.source, &request.plan)?;
        if replicas.is_empty() {
            return Ok(ReplicationExecution {
                registered: Vec::new(),
                record: None,
            });
        }

        store.get_verified(&request.source)?;
        let record = runtime.append(CommitTransaction {
            transaction_id: request.transaction_id,
            tenant: request.tenant,
            namespace: request.namespace,
            mutations: replicas
                .iter()
                .cloned()
                .map(|replica| Mutation::Storage(StorageMutation::RegisterReplica { replica }))
                .collect(),
        })?;

        Ok(ReplicationExecution {
            registered: replicas,
            record: Some(record),
        })
    }
}

#[derive(Debug, Clone)]
pub struct WriteArrowIpcTable {
    pub tenant: TenantId,
    pub namespace: NamespaceName,
    pub table_name: TableName,
    pub snapshot_id: SnapshotId,
    pub create_transaction_id: TransactionId,
    pub snapshot_transaction_id: TransactionId,
    pub file_name: String,
    pub batch: RecordBatch,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ArrowIpcTableSnapshot {
    pub table: ehdb_catalog::CatalogTable,
    pub snapshot: ehdb_catalog::CatalogSnapshot,
    pub object: ObjectRef,
    pub create_record: TransactionRecord,
    pub snapshot_record: TransactionRecord,
}

#[derive(Debug, Default)]
pub struct LocalArrowIpcTableStore;

impl LocalArrowIpcTableStore {
    pub fn write_batch<S: ImmutableObjectStore>(
        &self,
        runtime: &mut LocalReferenceRuntime,
        store: &S,
        request: WriteArrowIpcTable,
    ) -> Result<ArrowIpcTableSnapshot> {
        let table_id = ehdb_core::TableId::new(format!(
            "{}_{}_{}",
            request.tenant, request.namespace, request.table_name
        ))?;
        let object_path = table_snapshot_object_path(
            &request.tenant,
            &request.namespace,
            &table_id,
            &request.snapshot_id,
            &request.file_name,
        )?;
        let bytes = encode_record_batch(&request.batch)?;
        let object = store.put_if_absent(object_path, &bytes)?;
        let schema = table_schema_from_batch(&request.batch)?;

        let create_record = runtime.append(CommitTransaction {
            transaction_id: request.create_transaction_id,
            tenant: request.tenant.clone(),
            namespace: request.namespace.clone(),
            mutations: vec![Mutation::Catalog(CatalogMutation::CreateTable {
                table_id: table_id.clone(),
                table_name: request.table_name.clone(),
                schema,
            })],
        })?;

        let table = runtime
            .state()
            .catalog
            .get_table(&request.tenant, &request.namespace, &request.table_name)?
            .clone();

        let parent_snapshot = runtime
            .state()
            .catalog
            .latest_snapshot(&request.tenant, &request.namespace, &table.id)
            .ok()
            .map(|snapshot| snapshot.id.clone());
        let snapshot_record = runtime.append(CommitTransaction {
            transaction_id: request.snapshot_transaction_id,
            tenant: request.tenant.clone(),
            namespace: request.namespace.clone(),
            mutations: vec![Mutation::Catalog(CatalogMutation::CommitSnapshot {
                table_id: table.id.clone(),
                snapshot_id: request.snapshot_id.clone(),
                parent_snapshot,
                files: vec![object.clone()],
            })],
        })?;

        let snapshot = runtime
            .state()
            .catalog
            .latest_snapshot(&request.tenant, &request.namespace, &table.id)?
            .clone();

        Ok(ArrowIpcTableSnapshot {
            table,
            snapshot,
            object,
            create_record,
            snapshot_record,
        })
    }

    pub fn read_latest<S: ImmutableObjectStore>(
        &self,
        runtime: &LocalReferenceRuntime,
        store: &S,
        tenant: &TenantId,
        namespace: &NamespaceName,
        table_name: &TableName,
    ) -> Result<Vec<RecordBatch>> {
        LocalArrowSnapshotScanner.scan_latest(
            runtime,
            store,
            ScanArrowSnapshot {
                tenant: tenant.clone(),
                namespace: namespace.clone(),
                table_name: table_name.clone(),
                projection: None,
                predicate: None,
            },
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScanArrowSnapshot {
    pub tenant: TenantId,
    pub namespace: NamespaceName,
    pub table_name: TableName,
    pub projection: Option<Vec<String>>,
    pub predicate: Option<ArrowEqualityPredicate>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArrowEqualityPredicate {
    pub column: String,
    pub value: ArrowScalarValue,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ArrowScalarValue {
    Utf8(String),
    Int64(i64),
}

#[derive(Debug, Default)]
pub struct LocalArrowSnapshotScanner;

impl LocalArrowSnapshotScanner {
    pub fn scan_latest<S: ImmutableObjectStore>(
        &self,
        runtime: &LocalReferenceRuntime,
        store: &S,
        request: ScanArrowSnapshot,
    ) -> Result<Vec<RecordBatch>> {
        let table = runtime.state().catalog.get_table(
            &request.tenant,
            &request.namespace,
            &request.table_name,
        )?;
        let snapshot = runtime.state().catalog.latest_snapshot(
            &request.tenant,
            &request.namespace,
            &table.id,
        )?;
        let expected_schema = arrow_schema_from_table(&table.schema);
        let projection = request
            .projection
            .as_ref()
            .map(|columns| projection_indices(expected_schema.as_ref(), columns))
            .transpose()?;
        let predicate = request
            .predicate
            .as_ref()
            .map(|predicate| predicate_index(expected_schema.as_ref(), predicate))
            .transpose()?;

        let mut batches = Vec::new();
        for object in &snapshot.files {
            let bytes = store.get_verified(object)?;
            for batch in decode_record_batches(&bytes)? {
                if batch.schema().as_ref() != expected_schema.as_ref() {
                    return Err(EhdbError::InvalidState(format!(
                        "arrow ipc schema mismatch for {}",
                        object.path.as_str()
                    )));
                }
                let batch = filter_batch(batch, predicate.as_ref())?;
                batches.push(project_batch(batch, projection.as_deref())?);
            }
        }
        Ok(batches)
    }
}

impl ReferenceDatabase {
    pub fn apply_record(&mut self, record: &TransactionRecord) -> Result<()> {
        for mutation in &record.mutations {
            self.apply_mutation(record, mutation)?;
        }
        Ok(())
    }

    pub fn apply_records<'a>(
        &mut self,
        records: impl IntoIterator<Item = &'a TransactionRecord>,
    ) -> Result<()> {
        for record in records {
            self.apply_record(record)?;
        }
        Ok(())
    }

    fn apply_mutation(&mut self, record: &TransactionRecord, mutation: &Mutation) -> Result<()> {
        match mutation {
            Mutation::Catalog(mutation) => self.apply_catalog(record, mutation),
            Mutation::Stream(mutation) => self.apply_stream(record, mutation),
            Mutation::Retrieval(mutation) => self.apply_retrieval(record, mutation),
            Mutation::System(mutation) => self.apply_system(record, mutation),
            Mutation::Storage(mutation) => self.apply_storage(mutation),
        }
    }

    fn apply_catalog(
        &mut self,
        record: &TransactionRecord,
        mutation: &CatalogMutation,
    ) -> Result<()> {
        match mutation {
            CatalogMutation::CreateTable {
                table_id,
                table_name,
                schema,
            } => {
                let table = self.catalog.create_table(CreateTable {
                    tenant: record.tenant.clone(),
                    namespace: record.namespace.clone(),
                    name: table_name.clone(),
                    schema: schema.clone(),
                    transaction_id: record.transaction_id.clone(),
                })?;
                if &table.id != table_id {
                    return Err(EhdbError::InvalidState(format!(
                        "catalog replay table id mismatch: expected {}, got {}",
                        table_id, table.id
                    )));
                }
                Ok(())
            }
            CatalogMutation::CommitSnapshot {
                table_id,
                snapshot_id,
                parent_snapshot,
                files,
            } => self
                .catalog
                .commit_snapshot(CommitSnapshot {
                    tenant: record.tenant.clone(),
                    namespace: record.namespace.clone(),
                    table_id: table_id.clone(),
                    snapshot_id: snapshot_id.clone(),
                    parent_snapshot: parent_snapshot.clone(),
                    files: files.clone(),
                    transaction_id: record.transaction_id.clone(),
                })
                .map(|_| ()),
            CatalogMutation::GrantScan {
                table_id,
                principal,
            } => self
                .catalog
                .grant_scan(GrantScan {
                    tenant: record.tenant.clone(),
                    namespace: record.namespace.clone(),
                    table_id: table_id.clone(),
                    principal: principal.clone(),
                    transaction_id: record.transaction_id.clone(),
                })
                .map(|_| ()),
        }
    }

    fn apply_stream(
        &mut self,
        record: &TransactionRecord,
        mutation: &StreamMutation,
    ) -> Result<()> {
        match mutation {
            StreamMutation::CreateStream { stream, retention } => {
                self.streams.create_stream(StreamConfig {
                    tenant: record.tenant.clone(),
                    namespace: record.namespace.clone(),
                    name: stream.clone(),
                    retention: retention.clone(),
                })
            }
            StreamMutation::CreateConsumer { stream, consumer } => self
                .streams
                .create_consumer(&record.tenant, &record.namespace, stream, consumer.clone())
                .map(|_| ()),
            StreamMutation::Publish {
                stream,
                subject,
                payload,
                sequence,
            } => {
                let published = self.streams.publish(
                    &record.tenant,
                    &record.namespace,
                    stream,
                    subject.clone(),
                    payload.clone(),
                    record.transaction_id.clone(),
                )?;
                if published.sequence.value() != *sequence {
                    return Err(EhdbError::InvalidState(format!(
                        "stream replay sequence mismatch: expected {}, got {}",
                        sequence,
                        published.sequence.value()
                    )));
                }
                Ok(())
            }
            StreamMutation::Ack {
                stream,
                consumer,
                sequence,
            } => self
                .streams
                .ack(
                    &record.tenant,
                    &record.namespace,
                    stream,
                    consumer,
                    StreamSequence::new(*sequence)?,
                )
                .map(|_| ()),
        }
    }

    fn apply_retrieval(
        &mut self,
        record: &TransactionRecord,
        mutation: &RetrievalMutation,
    ) -> Result<()> {
        match mutation {
            RetrievalMutation::RegisterDocument {
                document_id,
                source_uri,
                content_type,
            } => self
                .retrieval
                .register_document(RegisterDocument {
                    id: document_id.clone(),
                    tenant: record.tenant.clone(),
                    namespace: record.namespace.clone(),
                    source_uri: source_uri.clone(),
                    content_type: content_type.clone(),
                    transaction_id: record.transaction_id.clone(),
                })
                .map(|_| ()),
            RetrievalMutation::RegisterChunk {
                document_id,
                chunk_id,
                ordinal,
                text,
                checksum,
            } => self
                .retrieval
                .register_chunk(RegisterChunk {
                    id: chunk_id.clone(),
                    document_id: document_id.clone(),
                    ordinal: *ordinal,
                    text: text.clone(),
                    checksum: checksum.clone(),
                    transaction_id: record.transaction_id.clone(),
                })
                .map(|_| ()),
            RetrievalMutation::RegisterEmbedding {
                chunk_id,
                model_id,
                dimensions,
                vector,
            } => self
                .retrieval
                .register_embedding(RegisterEmbedding {
                    chunk_id: chunk_id.clone(),
                    model_id: model_id.clone(),
                    dimensions: *dimensions,
                    vector: vector.clone(),
                    transaction_id: record.transaction_id.clone(),
                })
                .map(|_| ()),
        }
    }

    fn apply_system(
        &mut self,
        record: &TransactionRecord,
        mutation: &SystemMutation,
    ) -> Result<()> {
        match mutation {
            SystemMutation::PublishLibrary {
                path,
                revision,
                digest,
                entry,
                target,
                object_path,
                byte_len,
                capabilities,
            } => self
                .system
                .publish(PublishSystemLibrary {
                    path: path.clone(),
                    revision: *revision,
                    digest: digest.clone(),
                    entry: entry.clone(),
                    target: target.clone(),
                    object_path: object_path.clone(),
                    byte_len: *byte_len,
                    capabilities: capabilities.clone(),
                    transaction_id: record.transaction_id.clone(),
                })
                .map(|_| ()),
            SystemMutation::BindLibrary {
                path,
                environment,
                channel,
                revision,
                digest,
            } => self
                .system
                .bind(BindSystemLibrary {
                    tenant: record.tenant.clone(),
                    namespace: record.namespace.clone(),
                    environment: environment.clone(),
                    channel: channel.clone(),
                    path: path.clone(),
                    revision: *revision,
                    digest: digest.clone(),
                    transaction_id: record.transaction_id.clone(),
                })
                .map(|_| ()),
        }
    }

    fn apply_storage(&mut self, mutation: &StorageMutation) -> Result<()> {
        match mutation {
            StorageMutation::RegisterReplica { replica } => {
                self.storage.register(replica.clone()).map(|_| ())
            }
        }
    }
}

fn encode_record_batch(batch: &RecordBatch) -> Result<Vec<u8>> {
    let mut bytes = Vec::new();
    {
        let mut writer = FileWriter::try_new(&mut bytes, batch.schema().as_ref())
            .map_err(|err| EhdbError::Storage(format!("arrow ipc writer init failed: {err}")))?;
        writer
            .write(batch)
            .map_err(|err| EhdbError::Storage(format!("arrow ipc write failed: {err}")))?;
        writer
            .finish()
            .map_err(|err| EhdbError::Storage(format!("arrow ipc finish failed: {err}")))?;
    }
    Ok(bytes)
}

fn decode_record_batches(bytes: &[u8]) -> Result<Vec<RecordBatch>> {
    let reader = FileReader::try_new(Cursor::new(bytes), None)
        .map_err(|err| EhdbError::Storage(format!("arrow ipc reader init failed: {err}")))?;
    reader
        .map(|batch| {
            batch.map_err(|err| EhdbError::Storage(format!("arrow ipc read failed: {err}")))
        })
        .collect()
}

fn table_schema_from_batch(batch: &RecordBatch) -> Result<TableSchema> {
    let columns = batch
        .schema()
        .fields()
        .iter()
        .map(|field| {
            ColumnSchema::new(
                field.name().clone(),
                field.data_type().clone(),
                field.is_nullable(),
            )
        })
        .collect::<Result<Vec<_>>>()?;
    TableSchema::new(columns)
}

fn arrow_schema_from_table(schema: &TableSchema) -> Arc<Schema> {
    Arc::new(Schema::new(
        schema
            .columns()
            .iter()
            .map(|column| {
                Field::new(
                    column.name.clone(),
                    column.data_type.clone(),
                    column.nullable,
                )
            })
            .collect::<Vec<_>>(),
    ))
}

fn projection_indices(schema: &Schema, columns: &[String]) -> Result<Vec<usize>> {
    if columns.is_empty() {
        return Err(EhdbError::InvalidState(
            "arrow scan projection must contain at least one column".to_string(),
        ));
    }

    let mut seen = BTreeSet::new();
    for column in columns {
        validate_arrow_scan_selector(column)?;
        if !seen.insert(column.as_str()) {
            return Err(EhdbError::InvalidState(format!(
                "duplicate arrow scan projection column: {column}"
            )));
        }
    }

    columns
        .iter()
        .map(|column| {
            schema
                .fields()
                .iter()
                .position(|field| field.name() == column)
                .ok_or_else(|| EhdbError::NotFound(format!("projection column {column}")))
        })
        .collect()
}

fn predicate_index(
    schema: &Schema,
    predicate: &ArrowEqualityPredicate,
) -> Result<(usize, ArrowScalarValue)> {
    validate_arrow_scan_selector(&predicate.column)?;
    let index = schema
        .fields()
        .iter()
        .position(|field| field.name() == &predicate.column)
        .ok_or_else(|| EhdbError::NotFound(format!("predicate column {}", predicate.column)))?;
    Ok((index, predicate.value.clone()))
}

fn validate_arrow_scan_selector(column: &str) -> Result<()> {
    ColumnSchema::new(column, DataType::Utf8, true).map(|_| ())
}

fn filter_batch(
    batch: RecordBatch,
    predicate: Option<&(usize, ArrowScalarValue)>,
) -> Result<RecordBatch> {
    let Some((column_index, value)) = predicate else {
        return Ok(batch);
    };
    let keep = matching_row_indices(&batch, *column_index, value)?;
    if keep.len() == batch.num_rows() {
        return Ok(batch);
    }

    let arrays = batch
        .columns()
        .iter()
        .map(|array| take_rows(array, &keep))
        .collect::<Result<Vec<_>>>()?;
    RecordBatch::try_new(batch.schema(), arrays)
        .map_err(|err| EhdbError::InvalidState(format!("arrow filter failed: {err}")))
}

fn matching_row_indices(
    batch: &RecordBatch,
    column_index: usize,
    value: &ArrowScalarValue,
) -> Result<Vec<usize>> {
    let column = batch.column(column_index);
    match value {
        ArrowScalarValue::Utf8(expected) => {
            let strings = column
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| {
                    EhdbError::InvalidState(
                        "UTF-8 equality predicate requires a UTF-8 column".to_string(),
                    )
                })?;
            Ok((0..strings.len())
                .filter(|row| !strings.is_null(*row) && strings.value(*row) == expected)
                .collect())
        }
        ArrowScalarValue::Int64(expected) => {
            let ints = column
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| {
                    EhdbError::InvalidState(
                        "Int64 equality predicate requires an Int64 column".to_string(),
                    )
                })?;
            Ok((0..ints.len())
                .filter(|row| !ints.is_null(*row) && ints.value(*row) == *expected)
                .collect())
        }
    }
}

fn take_rows(array: &ArrayRef, rows: &[usize]) -> Result<ArrayRef> {
    if let Some(strings) = array.as_any().downcast_ref::<StringArray>() {
        let values = rows
            .iter()
            .map(|row| strings.value(*row))
            .collect::<Vec<_>>();
        return Ok(Arc::new(StringArray::from(values)));
    }
    if let Some(ints) = array.as_any().downcast_ref::<Int64Array>() {
        let values = rows.iter().map(|row| ints.value(*row)).collect::<Vec<_>>();
        return Ok(Arc::new(Int64Array::from(values)));
    }

    Err(EhdbError::InvalidState(
        "arrow filter supports UTF-8 and Int64 arrays only".to_string(),
    ))
}

fn project_batch(batch: RecordBatch, projection: Option<&[usize]>) -> Result<RecordBatch> {
    let Some(indices) = projection else {
        return Ok(batch);
    };

    let fields = indices
        .iter()
        .map(|index| batch.schema().field(*index).clone())
        .collect::<Vec<_>>();
    let arrays = indices
        .iter()
        .map(|index| batch.column(*index).clone() as ArrayRef)
        .collect::<Vec<_>>();
    RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays)
        .map_err(|err| EhdbError::InvalidState(format!("arrow projection failed: {err}")))
}

fn validate_plan_matches_source(source: &ObjectRef, plan: &ReplicationPlan) -> Result<()> {
    if plan.object_path != source.path {
        return Err(EhdbError::InvalidState(format!(
            "replication plan path mismatch: expected {}, got {}",
            source.path.as_str(),
            plan.object_path.as_str()
        )));
    }
    if plan.digest != source.digest {
        return Err(EhdbError::InvalidState(format!(
            "replication plan digest mismatch: expected {}, got {}",
            source.digest.as_str(),
            plan.digest.as_str()
        )));
    }
    Ok(())
}

fn replicas_to_register(source: &ObjectRef, plan: &ReplicationPlan) -> Result<Vec<ObjectReplica>> {
    let mut replicas = Vec::new();
    for action in &plan.actions {
        match action {
            ReplicationAction::AlreadySatisfied { .. } => {}
            ReplicationAction::CopyNeeded {
                source: action_source,
                target,
            } => {
                if action_source != &source.placement {
                    return Err(EhdbError::InvalidState(
                        "replication action source placement does not match object source"
                            .to_string(),
                    ));
                }
                replicas.push(ObjectReplica {
                    path: source.path.clone(),
                    len: source.len,
                    digest: source.digest.clone(),
                    placement: target.clone(),
                });
            }
        }
    }
    Ok(replicas)
}

#[cfg(test)]
mod tests {
    use std::{
        fs,
        sync::{
            atomic::{AtomicU64, Ordering},
            Arc,
        },
        time::{SystemTime, UNIX_EPOCH},
    };

    use arrow_array::{Array, Int64Array, RecordBatch, StringArray};
    use arrow_schema::{Field, Schema};
    use ehdb_core::{
        ChunkId, ColumnSchema, ConsumerName, DataType, DocumentId, EmbeddingModelId, NamespaceName,
        PrincipalId, SnapshotId, StreamName, TableId, TableName, TableSchema, TenantId,
        TransactionId,
    };
    use ehdb_storage::{
        plan_replication, CloudProvider, DataGravityShard, GeoLocation, ImmutableObjectStore,
        LocalObjectStore, ObjectDigest, ObjectPath, ObjectPlacement, ObjectRef, ObjectReplica,
        PlacementPolicy, PlacementTarget,
    };
    use ehdb_stream::{RetentionPolicy, Subject};
    use ehdb_system::{
        EnvironmentName, ModuleDigest, ReleaseChannel, SystemCapability, SystemLibraryPath,
        SystemLibraryRevision, WasmTarget,
    };
    use ehdb_transaction::{
        CatalogMutation, CommitTransaction, InMemoryTransactionLog, Mutation, RetrievalMutation,
        StorageMutation, StreamMutation, SystemMutation,
    };

    use super::*;

    fn ids() -> (TenantId, NamespaceName) {
        (
            TenantId::new("tenant-a").unwrap(),
            NamespaceName::new("system").unwrap(),
        )
    }

    fn temp_log_path(test_name: &str) -> PathBuf {
        let suffix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!(
            "ehdb-reference-{test_name}-{}-{suffix}.jsonl",
            std::process::id()
        ))
    }

    static TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);

    fn temp_object_root(test_name: &str) -> PathBuf {
        let suffix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let counter = TEMP_COUNTER.fetch_add(1, Ordering::Relaxed);
        std::env::temp_dir().join(format!(
            "ehdb-reference-objects-{test_name}-{}-{suffix}-{counter}",
            std::process::id()
        ))
    }

    fn gcp_local_shard_replica() -> ObjectPlacement {
        ObjectPlacement::new(
            GeoLocation::new(CloudProvider::Gcp, "us-central1", Some("us-central1-a")).unwrap(),
            DataGravityShard::local_dev(),
        )
    }

    fn local_plus_gcp_policy() -> PlacementPolicy {
        PlacementPolicy::new(
            2,
            vec![
                PlacementTarget::primary(ObjectPlacement::local_dev()),
                PlacementTarget::replica(gcp_local_shard_replica()),
            ],
        )
        .unwrap()
    }

    fn arrow_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("execution_id", DataType::Utf8, false),
            Field::new("attempt", DataType::Int64, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["exec-1", "exec-2"])),
                Arc::new(Int64Array::from(vec![1, 2])),
            ],
        )
        .unwrap()
    }

    fn create_table_commit(transaction_id: &str) -> CommitTransaction {
        let (tenant, namespace) = ids();
        CommitTransaction {
            transaction_id: TransactionId::new(transaction_id).unwrap(),
            tenant,
            namespace,
            mutations: vec![Mutation::Catalog(CatalogMutation::CreateTable {
                table_id: TableId::new("tenant-a_system_executions").unwrap(),
                table_name: TableName::new("executions").unwrap(),
                schema: TableSchema::new(vec![ColumnSchema::new(
                    "execution_id",
                    DataType::Utf8,
                    false,
                )
                .unwrap()])
                .unwrap(),
            })],
        }
    }

    fn commit_snapshot_commit(transaction_id: &str) -> CommitTransaction {
        let (tenant, namespace) = ids();
        CommitTransaction {
            transaction_id: TransactionId::new(transaction_id).unwrap(),
            tenant,
            namespace,
            mutations: vec![Mutation::Catalog(CatalogMutation::CommitSnapshot {
                table_id: TableId::new("tenant-a_system_executions").unwrap(),
                snapshot_id: SnapshotId::new("snapshot-0001").unwrap(),
                parent_snapshot: None,
                files: vec![object_ref(
                    "tenant-a/system/tables/tenant-a_system_executions/snapshots/snapshot-0001/part-000.arrow",
                )],
            })],
        }
    }

    fn grant_scan_commit(transaction_id: &str, principal: &str) -> CommitTransaction {
        let (tenant, namespace) = ids();
        CommitTransaction {
            transaction_id: TransactionId::new(transaction_id).unwrap(),
            tenant,
            namespace,
            mutations: vec![Mutation::Catalog(CatalogMutation::GrantScan {
                table_id: TableId::new("tenant-a_system_executions").unwrap(),
                principal: PrincipalId::new(principal).unwrap(),
            })],
        }
    }

    fn object_ref(path: &str) -> ObjectRef {
        ObjectRef {
            path: ObjectPath::new(path).unwrap(),
            len: 4096,
            digest: ObjectDigest::new(format!("sha256:{}", "a".repeat(64))).unwrap(),
            placement: ObjectPlacement::local_dev(),
        }
    }

    fn object_replica(path: &str) -> ObjectReplica {
        object_ref(path).into()
    }

    #[test]
    fn local_runtime_appends_and_rebuilds_state_after_reopen() {
        let path = temp_log_path("runtime-restart");
        let mut runtime = LocalReferenceRuntime::open(&path).unwrap();
        let record = runtime.append(create_table_commit("txn-0001")).unwrap();
        runtime.append(commit_snapshot_commit("txn-0002")).unwrap();
        runtime
            .append(grant_scan_commit("txn-0003", "worker-system"))
            .unwrap();

        assert_eq!(record.sequence.value(), 1);
        assert_eq!(runtime.state().catalog.table_count(), 1);
        assert_eq!(runtime.state().catalog.snapshot_count(), 1);
        assert_eq!(runtime.state().catalog.scan_grant_count(), 1);
        assert_eq!(runtime.replay().len(), 3);
        assert_eq!(runtime.path(), path.as_path());
        drop(runtime);

        let reopened = LocalReferenceRuntime::open(&path).unwrap();
        let (tenant, namespace) = ids();
        let table_id = TableId::new("tenant-a_system_executions").unwrap();
        let principal = PrincipalId::new("worker-system").unwrap();
        assert_eq!(reopened.state().catalog.table_count(), 1);
        assert_eq!(reopened.state().catalog.snapshot_count(), 1);
        assert_eq!(reopened.state().catalog.scan_grant_count(), 1);
        assert!(reopened
            .state()
            .catalog
            .can_scan(&tenant, &namespace, &table_id, &principal));
        assert_eq!(reopened.replay().len(), 3);

        fs::remove_file(path).unwrap();
    }

    #[test]
    fn local_reference_summary_reports_replayed_domain_counts() {
        let path = temp_log_path("runtime-summary");
        let (tenant, namespace) = ids();
        let table_id = TableId::new("tenant-a_system_executions").unwrap();
        let stream = StreamName::new("execution-events").unwrap();
        let consumer = ConsumerName::new("materializer").unwrap();
        let document = DocumentId::new("doc-001").unwrap();
        let chunk = ChunkId::new("chunk-001").unwrap();
        let model = EmbeddingModelId::new("embedding-model").unwrap();
        let library_path = SystemLibraryPath::new("system/catalog/bootstrap").unwrap();
        let library_revision = SystemLibraryRevision::new(1).unwrap();
        let library_digest = ModuleDigest::new(format!("sha256:{}1", "d".repeat(63))).unwrap();
        let mut runtime = LocalReferenceRuntime::open(&path).unwrap();

        runtime.append(create_table_commit("txn-0001")).unwrap();
        runtime.append(commit_snapshot_commit("txn-0002")).unwrap();
        runtime
            .append(grant_scan_commit("txn-0003", "worker-system"))
            .unwrap();
        runtime
            .append(CommitTransaction {
                transaction_id: TransactionId::new("txn-0004").unwrap(),
                tenant: tenant.clone(),
                namespace: namespace.clone(),
                mutations: vec![
                    Mutation::Stream(StreamMutation::CreateStream {
                        stream: stream.clone(),
                        retention: RetentionPolicy::KeepAll,
                    }),
                    Mutation::Stream(StreamMutation::CreateConsumer {
                        stream: stream.clone(),
                        consumer,
                    }),
                    Mutation::Stream(StreamMutation::Publish {
                        stream,
                        subject: Subject::new("noetl.execution.completed").unwrap(),
                        payload: b"{\"execution_id\":\"exec-1\"}".to_vec(),
                        sequence: 1,
                    }),
                ],
            })
            .unwrap();
        runtime
            .append(CommitTransaction {
                transaction_id: TransactionId::new("txn-0005").unwrap(),
                tenant: tenant.clone(),
                namespace: namespace.clone(),
                mutations: vec![
                    Mutation::Retrieval(RetrievalMutation::RegisterDocument {
                        document_id: document.clone(),
                        source_uri: "artifact://exec-1/result.md".to_string(),
                        content_type: "text/markdown".to_string(),
                    }),
                    Mutation::Retrieval(RetrievalMutation::RegisterChunk {
                        document_id: document.clone(),
                        chunk_id: chunk.clone(),
                        ordinal: 0,
                        text: "EHDB summary helper keeps replay counts visible.".to_string(),
                        checksum: "sha256-test".to_string(),
                    }),
                    Mutation::Retrieval(RetrievalMutation::RegisterEmbedding {
                        chunk_id: chunk,
                        model_id: model,
                        dimensions: 3,
                        vector: vec![0.1, 0.2, 0.3],
                    }),
                ],
            })
            .unwrap();
        runtime
            .append(CommitTransaction {
                transaction_id: TransactionId::new("txn-0006").unwrap(),
                tenant: tenant.clone(),
                namespace: namespace.clone(),
                mutations: vec![
                    Mutation::System(SystemMutation::PublishLibrary {
                        path: library_path.clone(),
                        revision: library_revision,
                        digest: library_digest.clone(),
                        entry: "run".to_string(),
                        target: WasmTarget::Wasm32UnknownUnknown,
                        object_path: ObjectPath::new(
                            "system-libraries/system/catalog/bootstrap/1/module.wasm",
                        )
                        .unwrap(),
                        byte_len: 512,
                        capabilities: vec![SystemCapability::EhdbCatalogWrite],
                    }),
                    Mutation::System(SystemMutation::BindLibrary {
                        path: library_path,
                        environment: EnvironmentName::new("kind").unwrap(),
                        channel: ReleaseChannel::stable(),
                        revision: library_revision,
                        digest: library_digest,
                    }),
                    Mutation::Storage(StorageMutation::RegisterReplica {
                        replica: object_replica(
                            "tenant-a/system/tables/tenant-a_system_executions/snapshots/snapshot-0001/part-000.arrow",
                        ),
                    }),
                ],
            })
            .unwrap();
        drop(runtime);

        let summary = summarize_local_reference(&path).unwrap();
        assert_eq!(summary.log_path, path.display().to_string());
        assert_eq!(summary.transaction_count, 6);
        assert_eq!(summary.table_count, 1);
        assert_eq!(summary.snapshot_count, 1);
        assert_eq!(summary.scan_grant_count, 1);
        assert_eq!(summary.stream_count, 1);
        assert_eq!(summary.stream_record_count, 1);
        assert_eq!(summary.stream_consumer_count, 1);
        assert_eq!(summary.retrieval_document_count, 1);
        assert_eq!(summary.retrieval_chunk_count, 1);
        assert_eq!(summary.retrieval_embedding_count, 1);
        assert_eq!(summary.system_library_count, 1);
        assert_eq!(summary.system_binding_count, 1);
        assert_eq!(summary.storage_object_count, 1);
        assert_eq!(summary.storage_replica_count, 1);

        let json = summarize_local_reference_json(&path).unwrap();
        let value: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(value["log_path"], path.display().to_string());
        assert_eq!(value["transaction_count"], 6);
        assert_eq!(value["storage_replica_count"], 1);
        assert_eq!(value["retrieval_embedding_count"], 1);
        assert_eq!(value.as_object().unwrap().len(), 15);

        let reopened = LocalReferenceRuntime::open(&path).unwrap();
        assert!(reopened.state().catalog.can_scan(
            &tenant,
            &namespace,
            &table_id,
            &PrincipalId::new("worker-system").unwrap()
        ));

        fs::remove_file(path).unwrap();
    }

    #[test]
    fn local_runtime_does_not_append_invalid_projected_commit() {
        let path = temp_log_path("runtime-invalid");
        let (tenant, namespace) = ids();
        let mut runtime = LocalReferenceRuntime::open(&path).unwrap();
        let error = runtime
            .append(CommitTransaction {
                transaction_id: TransactionId::new("txn-0001").unwrap(),
                tenant,
                namespace,
                mutations: vec![Mutation::Stream(StreamMutation::Publish {
                    stream: StreamName::new("missing-stream").unwrap(),
                    subject: Subject::new("noetl.event").unwrap(),
                    payload: b"payload".to_vec(),
                    sequence: 1,
                })],
            })
            .unwrap_err();

        assert!(matches!(error, EhdbError::NotFound(_)));
        assert!(runtime.replay().is_empty());
        assert!(!path.exists());
    }

    #[test]
    fn local_runtime_does_not_append_conflicting_replica_registration() {
        let path = temp_log_path("runtime-storage-invalid");
        let (tenant, namespace) = ids();
        let object_path = "tenant-a/system/table/part-000.arrow";
        let mut runtime = LocalReferenceRuntime::open(&path).unwrap();
        runtime
            .append(CommitTransaction {
                transaction_id: TransactionId::new("txn-storage-0001").unwrap(),
                tenant: tenant.clone(),
                namespace: namespace.clone(),
                mutations: vec![Mutation::Storage(StorageMutation::RegisterReplica {
                    replica: object_replica(object_path),
                })],
            })
            .unwrap();

        let mut conflicting = object_replica(object_path);
        conflicting.digest = ObjectDigest::new(format!("sha256:{}", "b".repeat(64))).unwrap();
        let error = runtime
            .append(CommitTransaction {
                transaction_id: TransactionId::new("txn-storage-0002").unwrap(),
                tenant,
                namespace,
                mutations: vec![Mutation::Storage(StorageMutation::RegisterReplica {
                    replica: conflicting,
                })],
            })
            .unwrap_err();

        assert!(matches!(error, EhdbError::Storage(_)));
        assert_eq!(runtime.replay().len(), 1);

        fs::remove_file(path).unwrap();
    }

    #[test]
    fn local_replication_executor_records_copy_needed_replicas() {
        let log_path = temp_log_path("replication-executor");
        let object_root = temp_object_root("replication-executor");
        let (tenant, namespace) = ids();
        let store = LocalObjectStore::new(&object_root);
        let source = store
            .put_if_absent(
                ObjectPath::new("tenant-a/system/table/part-000.arrow").unwrap(),
                b"arrow-ipc-placeholder",
            )
            .unwrap();
        let plan = plan_replication(&source, &[], &local_plus_gcp_policy()).unwrap();
        let mut runtime = LocalReferenceRuntime::open(&log_path).unwrap();

        let execution = LocalReplicationExecutor
            .execute(
                &mut runtime,
                &store,
                ExecuteReplication {
                    tenant: tenant.clone(),
                    namespace: namespace.clone(),
                    transaction_id: TransactionId::new("txn-replicate-0001").unwrap(),
                    source: source.clone(),
                    plan,
                },
            )
            .unwrap();

        assert_eq!(execution.registered.len(), 1);
        assert!(execution.record.is_some());
        assert_eq!(runtime.state().storage.replica_count(), 1);
        drop(runtime);

        let reopened = LocalReferenceRuntime::open(&log_path).unwrap();
        assert_eq!(reopened.state().storage.replica_count(), 1);
        assert_eq!(
            reopened
                .state()
                .storage
                .plan_replication(&source, &local_plus_gcp_policy())
                .unwrap()
                .copy_count(),
            0
        );

        fs::remove_file(log_path).unwrap();
        fs::remove_dir_all(object_root).unwrap();
    }

    #[test]
    fn local_replication_executor_noops_satisfied_plan() {
        let log_path = temp_log_path("replication-executor-noop");
        let (tenant, namespace) = ids();
        let source = object_ref("tenant-a/system/table/part-000.arrow");
        let plan = plan_replication(&source, &[], &PlacementPolicy::local_dev()).unwrap();
        let store = LocalObjectStore::new(temp_object_root("replication-executor-noop"));
        let mut runtime = LocalReferenceRuntime::open(&log_path).unwrap();

        let execution = LocalReplicationExecutor
            .execute(
                &mut runtime,
                &store,
                ExecuteReplication {
                    tenant,
                    namespace,
                    transaction_id: TransactionId::new("txn-replicate-0001").unwrap(),
                    source,
                    plan,
                },
            )
            .unwrap();

        assert!(execution.registered.is_empty());
        assert!(execution.record.is_none());
        assert!(runtime.replay().is_empty());
        assert!(!log_path.exists());
    }

    #[test]
    fn local_replication_executor_verifies_source_before_append() {
        let log_path = temp_log_path("replication-executor-corrupt");
        let object_root = temp_object_root("replication-executor-corrupt");
        let (tenant, namespace) = ids();
        let store = LocalObjectStore::new(&object_root);
        let source = store
            .put_if_absent(
                ObjectPath::new("tenant-a/system/table/part-000.arrow").unwrap(),
                b"arrow-ipc-placeholder",
            )
            .unwrap();
        fs::write(object_root.join(source.path.as_str()), b"corrupt").unwrap();
        let plan = plan_replication(&source, &[], &local_plus_gcp_policy()).unwrap();
        let mut runtime = LocalReferenceRuntime::open(&log_path).unwrap();

        let error = LocalReplicationExecutor
            .execute(
                &mut runtime,
                &store,
                ExecuteReplication {
                    tenant,
                    namespace,
                    transaction_id: TransactionId::new("txn-replicate-0001").unwrap(),
                    source,
                    plan,
                },
            )
            .unwrap_err();

        assert!(matches!(error, EhdbError::Storage(_)));
        assert!(runtime.replay().is_empty());
        assert!(!log_path.exists());

        fs::remove_dir_all(object_root).unwrap();
    }

    #[test]
    fn local_arrow_ipc_fixture_writes_snapshot_and_reads_batch() {
        let log_path = temp_log_path("arrow-ipc");
        let object_root = temp_object_root("arrow-ipc");
        let (tenant, namespace) = ids();
        let store = LocalObjectStore::new(&object_root);
        let mut runtime = LocalReferenceRuntime::open(&log_path).unwrap();

        let written = LocalArrowIpcTableStore
            .write_batch(
                &mut runtime,
                &store,
                WriteArrowIpcTable {
                    tenant: tenant.clone(),
                    namespace: namespace.clone(),
                    table_name: TableName::new("executions").unwrap(),
                    snapshot_id: SnapshotId::new("snapshot-0001").unwrap(),
                    create_transaction_id: TransactionId::new("txn-create-table").unwrap(),
                    snapshot_transaction_id: TransactionId::new("txn-commit-snapshot").unwrap(),
                    file_name: "part-000.arrow".to_string(),
                    batch: arrow_batch(),
                },
            )
            .unwrap();

        assert_eq!(written.table.id.as_str(), "tenant-a_system_executions");
        assert_eq!(written.snapshot.files, vec![written.object.clone()]);
        assert_eq!(runtime.state().catalog.table_count(), 1);
        assert_eq!(runtime.state().catalog.snapshot_count(), 1);

        let batches = LocalArrowIpcTableStore
            .read_latest(
                &runtime,
                &store,
                &tenant,
                &namespace,
                &TableName::new("executions").unwrap(),
            )
            .unwrap();

        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 2);
        let execution_ids = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let attempts = batches[0]
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(execution_ids.value(0), "exec-1");
        assert_eq!(execution_ids.value(1), "exec-2");
        assert_eq!(attempts.value(0), 1);
        assert_eq!(attempts.value(1), 2);
        drop(runtime);

        let reopened = LocalReferenceRuntime::open(&log_path).unwrap();
        assert_eq!(reopened.state().catalog.snapshot_count(), 1);
        assert_eq!(
            LocalArrowIpcTableStore
                .read_latest(
                    &reopened,
                    &store,
                    &tenant,
                    &namespace,
                    &TableName::new("executions").unwrap(),
                )
                .unwrap()[0]
                .num_rows(),
            2
        );

        fs::remove_file(log_path).unwrap();
        fs::remove_dir_all(object_root).unwrap();
    }

    #[test]
    fn local_arrow_scan_fixture_projects_columns_in_order() {
        let log_path = temp_log_path("arrow-scan-projection");
        let object_root = temp_object_root("arrow-scan-projection");
        let (tenant, namespace) = ids();
        let table_name = TableName::new("executions").unwrap();
        let store = LocalObjectStore::new(&object_root);
        let mut runtime = LocalReferenceRuntime::open(&log_path).unwrap();
        LocalArrowIpcTableStore
            .write_batch(
                &mut runtime,
                &store,
                WriteArrowIpcTable {
                    tenant: tenant.clone(),
                    namespace: namespace.clone(),
                    table_name: table_name.clone(),
                    snapshot_id: SnapshotId::new("snapshot-0001").unwrap(),
                    create_transaction_id: TransactionId::new("txn-create-table").unwrap(),
                    snapshot_transaction_id: TransactionId::new("txn-commit-snapshot").unwrap(),
                    file_name: "part-000.arrow".to_string(),
                    batch: arrow_batch(),
                },
            )
            .unwrap();

        let batches = LocalArrowSnapshotScanner
            .scan_latest(
                &runtime,
                &store,
                ScanArrowSnapshot {
                    tenant: tenant.clone(),
                    namespace: namespace.clone(),
                    table_name: table_name.clone(),
                    projection: Some(vec!["attempt".to_string(), "execution_id".to_string()]),
                    predicate: None,
                },
            )
            .unwrap();

        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].schema().field(0).name(), "attempt");
        assert_eq!(batches[0].schema().field(1).name(), "execution_id");
        let attempts = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let execution_ids = batches[0]
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(attempts.value(0), 1);
        assert_eq!(execution_ids.value(1), "exec-2");

        fs::remove_file(log_path).unwrap();
        fs::remove_dir_all(object_root).unwrap();
    }

    #[test]
    fn local_arrow_scan_fixture_rejects_missing_projection_columns() {
        let log_path = temp_log_path("arrow-scan-missing-projection");
        let object_root = temp_object_root("arrow-scan-missing-projection");
        let (tenant, namespace) = ids();
        let table_name = TableName::new("executions").unwrap();
        let store = LocalObjectStore::new(&object_root);
        let mut runtime = LocalReferenceRuntime::open(&log_path).unwrap();
        LocalArrowIpcTableStore
            .write_batch(
                &mut runtime,
                &store,
                WriteArrowIpcTable {
                    tenant: tenant.clone(),
                    namespace: namespace.clone(),
                    table_name: table_name.clone(),
                    snapshot_id: SnapshotId::new("snapshot-0001").unwrap(),
                    create_transaction_id: TransactionId::new("txn-create-table").unwrap(),
                    snapshot_transaction_id: TransactionId::new("txn-commit-snapshot").unwrap(),
                    file_name: "part-000.arrow".to_string(),
                    batch: arrow_batch(),
                },
            )
            .unwrap();

        let error = LocalArrowSnapshotScanner
            .scan_latest(
                &runtime,
                &store,
                ScanArrowSnapshot {
                    tenant,
                    namespace,
                    table_name,
                    projection: Some(vec!["missing".to_string()]),
                    predicate: None,
                },
            )
            .unwrap_err();

        assert!(matches!(error, EhdbError::NotFound(_)));

        fs::remove_file(log_path).unwrap();
        fs::remove_dir_all(object_root).unwrap();
    }

    #[test]
    fn local_arrow_scan_fixture_rejects_invalid_projection_shape() {
        let log_path = temp_log_path("arrow-scan-invalid-projection-shape");
        let object_root = temp_object_root("arrow-scan-invalid-projection-shape");
        let (tenant, namespace) = ids();
        let table_name = TableName::new("executions").unwrap();
        let store = LocalObjectStore::new(&object_root);
        let mut runtime = LocalReferenceRuntime::open(&log_path).unwrap();
        LocalArrowIpcTableStore
            .write_batch(
                &mut runtime,
                &store,
                WriteArrowIpcTable {
                    tenant: tenant.clone(),
                    namespace: namespace.clone(),
                    table_name: table_name.clone(),
                    snapshot_id: SnapshotId::new("snapshot-0001").unwrap(),
                    create_transaction_id: TransactionId::new("txn-create-table").unwrap(),
                    snapshot_transaction_id: TransactionId::new("txn-commit-snapshot").unwrap(),
                    file_name: "part-000.arrow".to_string(),
                    batch: arrow_batch(),
                },
            )
            .unwrap();

        for projection in [
            Vec::new(),
            vec!["execution_id".to_string(), "execution_id".to_string()],
        ] {
            let error = LocalArrowSnapshotScanner
                .scan_latest(
                    &runtime,
                    &store,
                    ScanArrowSnapshot {
                        tenant: tenant.clone(),
                        namespace: namespace.clone(),
                        table_name: table_name.clone(),
                        projection: Some(projection),
                        predicate: None,
                    },
                )
                .unwrap_err();

            assert!(matches!(error, EhdbError::InvalidState(_)));
        }

        fs::remove_file(log_path).unwrap();
        fs::remove_dir_all(object_root).unwrap();
    }

    #[test]
    fn local_arrow_scan_fixture_rejects_invalid_selector_identifiers() {
        let log_path = temp_log_path("arrow-scan-invalid-selector-identifiers");
        let object_root = temp_object_root("arrow-scan-invalid-selector-identifiers");
        let (tenant, namespace) = ids();
        let table_name = TableName::new("executions").unwrap();
        let store = LocalObjectStore::new(&object_root);
        let mut runtime = LocalReferenceRuntime::open(&log_path).unwrap();
        LocalArrowIpcTableStore
            .write_batch(
                &mut runtime,
                &store,
                WriteArrowIpcTable {
                    tenant: tenant.clone(),
                    namespace: namespace.clone(),
                    table_name: table_name.clone(),
                    snapshot_id: SnapshotId::new("snapshot-0001").unwrap(),
                    create_transaction_id: TransactionId::new("txn-create-table").unwrap(),
                    snapshot_transaction_id: TransactionId::new("txn-commit-snapshot").unwrap(),
                    file_name: "part-000.arrow".to_string(),
                    batch: arrow_batch(),
                },
            )
            .unwrap();

        let projection_error = LocalArrowSnapshotScanner
            .scan_latest(
                &runtime,
                &store,
                ScanArrowSnapshot {
                    tenant: tenant.clone(),
                    namespace: namespace.clone(),
                    table_name: table_name.clone(),
                    projection: Some(vec!["bad selector".to_string()]),
                    predicate: None,
                },
            )
            .unwrap_err();
        assert!(matches!(projection_error, EhdbError::InvalidIdentifier(_)));

        let predicate_error = LocalArrowSnapshotScanner
            .scan_latest(
                &runtime,
                &store,
                ScanArrowSnapshot {
                    tenant,
                    namespace,
                    table_name,
                    projection: None,
                    predicate: Some(ArrowEqualityPredicate {
                        column: "bad selector".to_string(),
                        value: ArrowScalarValue::Utf8("exec-1".to_string()),
                    }),
                },
            )
            .unwrap_err();
        assert!(matches!(predicate_error, EhdbError::InvalidIdentifier(_)));

        fs::remove_file(log_path).unwrap();
        fs::remove_dir_all(object_root).unwrap();
    }

    #[test]
    fn local_arrow_filter_fixture_filters_utf8_equality() {
        let log_path = temp_log_path("arrow-filter-utf8");
        let object_root = temp_object_root("arrow-filter-utf8");
        let (tenant, namespace) = ids();
        let table_name = TableName::new("executions").unwrap();
        let store = LocalObjectStore::new(&object_root);
        let mut runtime = LocalReferenceRuntime::open(&log_path).unwrap();
        LocalArrowIpcTableStore
            .write_batch(
                &mut runtime,
                &store,
                WriteArrowIpcTable {
                    tenant: tenant.clone(),
                    namespace: namespace.clone(),
                    table_name: table_name.clone(),
                    snapshot_id: SnapshotId::new("snapshot-0001").unwrap(),
                    create_transaction_id: TransactionId::new("txn-create-table").unwrap(),
                    snapshot_transaction_id: TransactionId::new("txn-commit-snapshot").unwrap(),
                    file_name: "part-000.arrow".to_string(),
                    batch: arrow_batch(),
                },
            )
            .unwrap();

        let batches = LocalArrowSnapshotScanner
            .scan_latest(
                &runtime,
                &store,
                ScanArrowSnapshot {
                    tenant: tenant.clone(),
                    namespace: namespace.clone(),
                    table_name: table_name.clone(),
                    projection: None,
                    predicate: Some(ArrowEqualityPredicate {
                        column: "execution_id".to_string(),
                        value: ArrowScalarValue::Utf8("exec-2".to_string()),
                    }),
                },
            )
            .unwrap();

        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 1);
        let execution_ids = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(execution_ids.value(0), "exec-2");

        fs::remove_file(log_path).unwrap();
        fs::remove_dir_all(object_root).unwrap();
    }

    #[test]
    fn local_arrow_filter_fixture_filters_int64_before_projection() {
        let log_path = temp_log_path("arrow-filter-int64");
        let object_root = temp_object_root("arrow-filter-int64");
        let (tenant, namespace) = ids();
        let table_name = TableName::new("executions").unwrap();
        let store = LocalObjectStore::new(&object_root);
        let mut runtime = LocalReferenceRuntime::open(&log_path).unwrap();
        LocalArrowIpcTableStore
            .write_batch(
                &mut runtime,
                &store,
                WriteArrowIpcTable {
                    tenant: tenant.clone(),
                    namespace: namespace.clone(),
                    table_name: table_name.clone(),
                    snapshot_id: SnapshotId::new("snapshot-0001").unwrap(),
                    create_transaction_id: TransactionId::new("txn-create-table").unwrap(),
                    snapshot_transaction_id: TransactionId::new("txn-commit-snapshot").unwrap(),
                    file_name: "part-000.arrow".to_string(),
                    batch: arrow_batch(),
                },
            )
            .unwrap();

        let batches = LocalArrowSnapshotScanner
            .scan_latest(
                &runtime,
                &store,
                ScanArrowSnapshot {
                    tenant: tenant.clone(),
                    namespace: namespace.clone(),
                    table_name: table_name.clone(),
                    projection: Some(vec!["execution_id".to_string()]),
                    predicate: Some(ArrowEqualityPredicate {
                        column: "attempt".to_string(),
                        value: ArrowScalarValue::Int64(1),
                    }),
                },
            )
            .unwrap();

        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_columns(), 1);
        assert_eq!(batches[0].schema().field(0).name(), "execution_id");
        let execution_ids = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(execution_ids.value(0), "exec-1");

        fs::remove_file(log_path).unwrap();
        fs::remove_dir_all(object_root).unwrap();
    }

    #[test]
    fn local_arrow_filter_fixture_rejects_missing_predicate_column() {
        let log_path = temp_log_path("arrow-filter-missing-column");
        let object_root = temp_object_root("arrow-filter-missing-column");
        let (tenant, namespace) = ids();
        let table_name = TableName::new("executions").unwrap();
        let store = LocalObjectStore::new(&object_root);
        let mut runtime = LocalReferenceRuntime::open(&log_path).unwrap();
        LocalArrowIpcTableStore
            .write_batch(
                &mut runtime,
                &store,
                WriteArrowIpcTable {
                    tenant: tenant.clone(),
                    namespace: namespace.clone(),
                    table_name: table_name.clone(),
                    snapshot_id: SnapshotId::new("snapshot-0001").unwrap(),
                    create_transaction_id: TransactionId::new("txn-create-table").unwrap(),
                    snapshot_transaction_id: TransactionId::new("txn-commit-snapshot").unwrap(),
                    file_name: "part-000.arrow".to_string(),
                    batch: arrow_batch(),
                },
            )
            .unwrap();

        let error = LocalArrowSnapshotScanner
            .scan_latest(
                &runtime,
                &store,
                ScanArrowSnapshot {
                    tenant,
                    namespace,
                    table_name,
                    projection: None,
                    predicate: Some(ArrowEqualityPredicate {
                        column: "missing".to_string(),
                        value: ArrowScalarValue::Utf8("value".to_string()),
                    }),
                },
            )
            .unwrap_err();

        assert!(matches!(error, EhdbError::NotFound(_)));

        fs::remove_file(log_path).unwrap();
        fs::remove_dir_all(object_root).unwrap();
    }

    #[test]
    fn local_arrow_filter_fixture_rejects_type_mismatch() {
        let log_path = temp_log_path("arrow-filter-type-mismatch");
        let object_root = temp_object_root("arrow-filter-type-mismatch");
        let (tenant, namespace) = ids();
        let table_name = TableName::new("executions").unwrap();
        let store = LocalObjectStore::new(&object_root);
        let mut runtime = LocalReferenceRuntime::open(&log_path).unwrap();
        LocalArrowIpcTableStore
            .write_batch(
                &mut runtime,
                &store,
                WriteArrowIpcTable {
                    tenant: tenant.clone(),
                    namespace: namespace.clone(),
                    table_name: table_name.clone(),
                    snapshot_id: SnapshotId::new("snapshot-0001").unwrap(),
                    create_transaction_id: TransactionId::new("txn-create-table").unwrap(),
                    snapshot_transaction_id: TransactionId::new("txn-commit-snapshot").unwrap(),
                    file_name: "part-000.arrow".to_string(),
                    batch: arrow_batch(),
                },
            )
            .unwrap();

        let error = LocalArrowSnapshotScanner
            .scan_latest(
                &runtime,
                &store,
                ScanArrowSnapshot {
                    tenant,
                    namespace,
                    table_name,
                    projection: None,
                    predicate: Some(ArrowEqualityPredicate {
                        column: "attempt".to_string(),
                        value: ArrowScalarValue::Utf8("1".to_string()),
                    }),
                },
            )
            .unwrap_err();

        assert!(matches!(error, EhdbError::InvalidState(_)));

        fs::remove_file(log_path).unwrap();
        fs::remove_dir_all(object_root).unwrap();
    }

    #[test]
    fn local_arrow_ipc_fixture_rejects_corrupt_object_before_decode() {
        let log_path = temp_log_path("arrow-ipc-corrupt");
        let object_root = temp_object_root("arrow-ipc-corrupt");
        let (tenant, namespace) = ids();
        let store = LocalObjectStore::new(&object_root);
        let mut runtime = LocalReferenceRuntime::open(&log_path).unwrap();

        let written = LocalArrowIpcTableStore
            .write_batch(
                &mut runtime,
                &store,
                WriteArrowIpcTable {
                    tenant: tenant.clone(),
                    namespace: namespace.clone(),
                    table_name: TableName::new("executions").unwrap(),
                    snapshot_id: SnapshotId::new("snapshot-0001").unwrap(),
                    create_transaction_id: TransactionId::new("txn-create-table").unwrap(),
                    snapshot_transaction_id: TransactionId::new("txn-commit-snapshot").unwrap(),
                    file_name: "part-000.arrow".to_string(),
                    batch: arrow_batch(),
                },
            )
            .unwrap();
        fs::write(object_root.join(written.object.path.as_str()), b"corrupt").unwrap();

        let error = LocalArrowIpcTableStore
            .read_latest(
                &runtime,
                &store,
                &tenant,
                &namespace,
                &TableName::new("executions").unwrap(),
            )
            .unwrap_err();

        assert!(matches!(error, EhdbError::Storage(_)));

        fs::remove_file(log_path).unwrap();
        fs::remove_dir_all(object_root).unwrap();
    }

    #[test]
    fn rebuilds_reference_state_from_transaction_replay() {
        let (tenant, namespace) = ids();
        let mut log = InMemoryTransactionLog::default();
        let schema = TableSchema::new(vec![ColumnSchema::new(
            "execution_id",
            DataType::Utf8,
            false,
        )
        .unwrap()])
        .unwrap();
        let table_id = TableId::new("tenant-a_system_executions").unwrap();
        let snapshot_id = SnapshotId::new("snapshot-0001").unwrap();
        let stream = StreamName::new("execution-events").unwrap();
        let consumer = ConsumerName::new("materializer").unwrap();
        let document = DocumentId::new("doc-001").unwrap();
        let chunk = ChunkId::new("chunk-001").unwrap();
        let model = EmbeddingModelId::new("embedding-model").unwrap();
        let library_path = SystemLibraryPath::new("system/catalog/bootstrap").unwrap();
        let library_revision = SystemLibraryRevision::new(1).unwrap();
        let library_digest = ModuleDigest::new(format!("sha256:{}1", "d".repeat(63))).unwrap();

        log.append(CommitTransaction {
            transaction_id: TransactionId::new("txn-0001").unwrap(),
            tenant: tenant.clone(),
            namespace: namespace.clone(),
            mutations: vec![Mutation::Catalog(CatalogMutation::CreateTable {
                table_id: table_id.clone(),
                table_name: TableName::new("executions").unwrap(),
                schema,
            })],
        })
        .unwrap();
        log.append(CommitTransaction {
            transaction_id: TransactionId::new("txn-0002").unwrap(),
            tenant: tenant.clone(),
            namespace: namespace.clone(),
            mutations: vec![Mutation::Catalog(CatalogMutation::CommitSnapshot {
                table_id: table_id.clone(),
                snapshot_id: snapshot_id.clone(),
                parent_snapshot: None,
                files: vec![object_ref(
                    "tenant-a/system/tables/tenant-a_system_executions/snapshots/snapshot-0001/part-000.arrow",
                )],
            })],
        })
        .unwrap();
        log.append(CommitTransaction {
            transaction_id: TransactionId::new("txn-0003").unwrap(),
            tenant: tenant.clone(),
            namespace: namespace.clone(),
            mutations: vec![
                Mutation::Stream(StreamMutation::CreateStream {
                    stream: stream.clone(),
                    retention: RetentionPolicy::KeepAll,
                }),
                Mutation::Stream(StreamMutation::CreateConsumer {
                    stream: stream.clone(),
                    consumer: consumer.clone(),
                }),
                Mutation::Stream(StreamMutation::Publish {
                    stream: stream.clone(),
                    subject: Subject::new("noetl.execution.completed").unwrap(),
                    payload: b"{\"execution_id\":\"exec-1\"}".to_vec(),
                    sequence: 1,
                }),
            ],
        })
        .unwrap();
        log.append(CommitTransaction {
            transaction_id: TransactionId::new("txn-0004").unwrap(),
            tenant: tenant.clone(),
            namespace: namespace.clone(),
            mutations: vec![
                Mutation::Retrieval(RetrievalMutation::RegisterDocument {
                    document_id: document.clone(),
                    source_uri: "artifact://exec-1/result.md".to_string(),
                    content_type: "text/markdown".to_string(),
                }),
                Mutation::Retrieval(RetrievalMutation::RegisterChunk {
                    document_id: document.clone(),
                    chunk_id: chunk.clone(),
                    ordinal: 0,
                    text: "EHDB transaction replay keeps lineage searchable.".to_string(),
                    checksum: "sha256-test".to_string(),
                }),
                Mutation::Retrieval(RetrievalMutation::RegisterEmbedding {
                    chunk_id: chunk,
                    model_id: model.clone(),
                    dimensions: 3,
                    vector: vec![0.1, 0.2, 0.3],
                }),
            ],
        })
        .unwrap();
        log.append(CommitTransaction {
            transaction_id: TransactionId::new("txn-0005").unwrap(),
            tenant: tenant.clone(),
            namespace: namespace.clone(),
            mutations: vec![
                Mutation::System(SystemMutation::PublishLibrary {
                    path: library_path.clone(),
                    revision: library_revision,
                    digest: library_digest.clone(),
                    entry: "run".to_string(),
                    target: WasmTarget::Wasm32UnknownUnknown,
                    object_path: ObjectPath::new(
                        "system-libraries/system/catalog/bootstrap/1/module.wasm",
                    )
                    .unwrap(),
                    byte_len: 512,
                    capabilities: vec![SystemCapability::EhdbCatalogWrite],
                }),
                Mutation::System(SystemMutation::BindLibrary {
                    path: library_path.clone(),
                    environment: EnvironmentName::new("kind").unwrap(),
                    channel: ReleaseChannel::stable(),
                    revision: library_revision,
                    digest: library_digest,
                }),
            ],
        })
        .unwrap();
        log.append(CommitTransaction {
            transaction_id: TransactionId::new("txn-0006").unwrap(),
            tenant: tenant.clone(),
            namespace: namespace.clone(),
            mutations: vec![Mutation::Storage(StorageMutation::RegisterReplica {
                replica: object_replica(
                    "tenant-a/system/tables/tenant-a_system_executions/snapshots/snapshot-0001/part-000.arrow",
                ),
            })],
        })
        .unwrap();

        let mut reference = ReferenceDatabase::default();
        let records = log.replay(None);
        reference.apply_records(&records).unwrap();

        assert_eq!(reference.catalog.table_count(), 1);
        assert_eq!(reference.catalog.snapshot_count(), 1);
        assert_eq!(
            reference
                .catalog
                .latest_snapshot(&tenant, &namespace, &table_id)
                .unwrap()
                .id,
            snapshot_id
        );
        assert_eq!(
            reference
                .streams
                .replay(&tenant, &namespace, &stream, None)
                .unwrap()
                .len(),
            1
        );
        assert_eq!(
            reference
                .retrieval
                .find_chunks_containing(&tenant, &namespace, "lineage")
                .len(),
            1
        );
        assert_eq!(
            reference
                .retrieval
                .embedding(&ChunkId::new("chunk-001").unwrap(), &model)
                .unwrap()
                .dimensions,
            3
        );
        assert_eq!(
            reference
                .system
                .resolve(ehdb_system::ResolveSystemLibrary {
                    tenant,
                    namespace,
                    environment: EnvironmentName::new("kind").unwrap(),
                    channel: ReleaseChannel::stable(),
                    path: library_path,
                })
                .unwrap()
                .revision
                .value(),
            1
        );
        assert_eq!(reference.storage.object_count(), 1);
        assert_eq!(reference.storage.replica_count(), 1);
    }

    #[test]
    fn rejects_replay_when_durable_stream_sequence_does_not_match_state() {
        let (tenant, namespace) = ids();
        let stream = StreamName::new("execution-events").unwrap();
        let record = TransactionRecord {
            sequence: ehdb_transaction::TransactionSequence::first(),
            transaction_id: TransactionId::new("txn-0001").unwrap(),
            tenant,
            namespace,
            mutations: vec![
                Mutation::Stream(StreamMutation::CreateStream {
                    stream: stream.clone(),
                    retention: RetentionPolicy::KeepAll,
                }),
                Mutation::Stream(StreamMutation::Publish {
                    stream,
                    subject: Subject::new("noetl.event").unwrap(),
                    payload: b"payload".to_vec(),
                    sequence: 2,
                }),
            ],
        };

        let mut reference = ReferenceDatabase::default();
        let error = reference.apply_record(&record).unwrap_err();

        assert!(matches!(error, EhdbError::InvalidState(_)));
    }

    fn append_request(
        path: &std::path::Path,
        stream: &str,
        subject: &str,
        transaction_id: &str,
        payload: &str,
    ) -> AppendDomainRecordRequest {
        AppendDomainRecordRequest {
            log_path: path.to_path_buf(),
            tenant: DEFAULT_LOCAL_REFERENCE_TENANT.to_string(),
            namespace: DEFAULT_LOCAL_REFERENCE_NAMESPACE.to_string(),
            stream: stream.to_string(),
            subject: subject.to_string(),
            transaction_id: transaction_id.to_string(),
            payload: payload.to_string(),
        }
    }

    #[test]
    fn append_domain_record_creates_stream_then_appends_and_reads_back() {
        let path = temp_log_path("domain-append-read");

        let first = append_local_reference_domain_record(append_request(
            &path,
            "muno-itinerary",
            "muno.itinerary.created",
            "txn-c-0001",
            "{\"trip\":\"paris\"}",
        ))
        .unwrap();
        assert!(first.created_stream);
        assert_eq!(first.sequence, 1);
        assert_eq!(first.stream_record_count, 1);
        assert_eq!(first.byte_len, "{\"trip\":\"paris\"}".len());

        let second = append_local_reference_domain_record(append_request(
            &path,
            "muno-itinerary",
            "muno.itinerary.updated",
            "txn-c-0002",
            "{\"trip\":\"rome\"}",
        ))
        .unwrap();
        assert!(!second.created_stream);
        assert_eq!(second.sequence, 2);
        assert_eq!(second.stream_record_count, 2);
        assert_eq!(second.transaction_count, 2);

        let read = read_local_reference_domain_records(ReadDomainRecordsRequest {
            log_path: path.clone(),
            tenant: DEFAULT_LOCAL_REFERENCE_TENANT.to_string(),
            namespace: DEFAULT_LOCAL_REFERENCE_NAMESPACE.to_string(),
            stream: "muno-itinerary".to_string(),
            after: None,
            limit: 100,
        })
        .unwrap();
        assert!(read.exists);
        assert_eq!(read.record_count, 2);
        assert_eq!(read.returned, 2);
        assert_eq!(read.records[0].sequence, 1);
        assert_eq!(read.records[0].payload, "{\"trip\":\"paris\"}");
        assert_eq!(read.records[1].subject, "muno.itinerary.updated");

        fs::remove_file(path).unwrap();
    }

    #[test]
    fn append_domain_record_survives_reopen() {
        let path = temp_log_path("domain-reopen");
        append_local_reference_domain_record(append_request(
            &path,
            "orders",
            "orders.placed",
            "txn-r-0001",
            "one",
        ))
        .unwrap();
        // A second call reopens the log from scratch (stateless), proving the
        // record is durable across independent bounded invocations.
        let outcome = append_local_reference_domain_record(append_request(
            &path,
            "orders",
            "orders.placed",
            "txn-r-0002",
            "two",
        ))
        .unwrap();
        assert_eq!(outcome.sequence, 2);
        assert_eq!(outcome.transaction_count, 2);
        fs::remove_file(path).unwrap();
    }

    #[test]
    fn read_missing_stream_reports_absent_without_error() {
        let path = temp_log_path("domain-missing");
        let read = read_local_reference_domain_records(ReadDomainRecordsRequest {
            log_path: path.clone(),
            tenant: DEFAULT_LOCAL_REFERENCE_TENANT.to_string(),
            namespace: DEFAULT_LOCAL_REFERENCE_NAMESPACE.to_string(),
            stream: "never-written".to_string(),
            after: None,
            limit: 10,
        })
        .unwrap();
        assert!(!read.exists);
        assert_eq!(read.record_count, 0);
        assert!(read.records.is_empty());
        // Probing a missing stream must not create the log file.
        assert!(!path.exists());
    }

    #[test]
    fn read_honors_limit_and_after_cursor() {
        let path = temp_log_path("domain-cursor");
        for index in 1..=5 {
            append_local_reference_domain_record(append_request(
                &path,
                "events",
                "events.tick",
                &format!("txn-cursor-{index:04}"),
                &format!("payload-{index}"),
            ))
            .unwrap();
        }

        let limited = read_local_reference_domain_records(ReadDomainRecordsRequest {
            log_path: path.clone(),
            tenant: DEFAULT_LOCAL_REFERENCE_TENANT.to_string(),
            namespace: DEFAULT_LOCAL_REFERENCE_NAMESPACE.to_string(),
            stream: "events".to_string(),
            after: None,
            limit: 2,
        })
        .unwrap();
        assert_eq!(limited.record_count, 5);
        assert_eq!(limited.returned, 2);
        assert_eq!(limited.records[0].sequence, 1);

        let after = read_local_reference_domain_records(ReadDomainRecordsRequest {
            log_path: path.clone(),
            tenant: DEFAULT_LOCAL_REFERENCE_TENANT.to_string(),
            namespace: DEFAULT_LOCAL_REFERENCE_NAMESPACE.to_string(),
            stream: "events".to_string(),
            after: Some(3),
            limit: 100,
        })
        .unwrap();
        assert_eq!(after.record_count, 2);
        assert_eq!(after.records[0].sequence, 4);
        assert_eq!(after.records[1].sequence, 5);

        fs::remove_file(path).unwrap();
    }

    #[test]
    fn append_rejects_invalid_identifiers() {
        let path = temp_log_path("domain-invalid");
        let error = append_local_reference_domain_record(append_request(
            &path,
            "bad stream name",
            "orders.placed",
            "txn-x-0001",
            "payload",
        ))
        .unwrap_err();
        assert!(matches!(error, EhdbError::InvalidIdentifier(_)));
    }

    // ---- Event-stream integration path (Phase D) ----

    fn consume_request(
        path: &std::path::Path,
        stream: &str,
        consumer: &str,
        transaction_id: &str,
        limit: usize,
    ) -> ConsumeEventRecordsRequest {
        ConsumeEventRecordsRequest {
            log_path: path.to_path_buf(),
            tenant: DEFAULT_LOCAL_REFERENCE_TENANT.to_string(),
            namespace: DEFAULT_LOCAL_REFERENCE_NAMESPACE.to_string(),
            stream: stream.to_string(),
            consumer: consumer.to_string(),
            transaction_id: transaction_id.to_string(),
            limit,
        }
    }

    fn ack_request(
        path: &std::path::Path,
        stream: &str,
        consumer: &str,
        transaction_id: &str,
        sequence: u64,
    ) -> AckEventConsumerRequest {
        AckEventConsumerRequest {
            log_path: path.to_path_buf(),
            tenant: DEFAULT_LOCAL_REFERENCE_TENANT.to_string(),
            namespace: DEFAULT_LOCAL_REFERENCE_NAMESPACE.to_string(),
            stream: stream.to_string(),
            consumer: consumer.to_string(),
            transaction_id: transaction_id.to_string(),
            sequence,
        }
    }

    #[test]
    fn event_stream_drain_projects_consumes_acks_and_survives_reopen() {
        let path = temp_log_path("eventstream-drain");

        // Project two already-emitted NoETL events into the derived EHDB stream
        // (the existing `append` primitive is the project leg).
        for (index, payload) in ["{\"n\":1}", "{\"n\":2}"].iter().enumerate() {
            append_local_reference_domain_record(append_request(
                &path,
                "noetl-events",
                "noetl.execution.completed",
                &format!("txn-proj-{:04}", index + 1),
                payload,
            ))
            .unwrap();
        }

        // First durable-consumer pull creates the consumer and delivers all
        // pending records without moving the cursor.
        let first = consume_local_reference_event_records(consume_request(
            &path,
            "noetl-events",
            "materializer",
            "txn-consume-0001",
            100,
        ))
        .unwrap();
        assert!(first.exists);
        assert!(first.created_consumer);
        assert_eq!(first.acked_sequence, None);
        assert_eq!(first.pending_count, 2);
        assert_eq!(first.returned, 2);
        assert_eq!(first.records[0].sequence, 1);
        assert_eq!(first.records[1].sequence, 2);

        // Ack-after-materialize: advance the cursor past the first record only.
        let acked = ack_local_reference_event_consumer(ack_request(
            &path,
            "noetl-events",
            "materializer",
            "txn-ack-0001",
            1,
        ))
        .unwrap();
        assert_eq!(acked.acked_sequence, 1);

        // Reopen from scratch (stateless): the durable cursor is restored from
        // the transaction log, the consumer is not recreated, and only the
        // unacked record is pending.
        let second = consume_local_reference_event_records(consume_request(
            &path,
            "noetl-events",
            "materializer",
            "txn-consume-0002",
            100,
        ))
        .unwrap();
        assert!(!second.created_consumer);
        assert_eq!(second.acked_sequence, Some(1));
        assert_eq!(second.pending_count, 1);
        assert_eq!(second.returned, 1);
        assert_eq!(second.records[0].sequence, 2);

        // Ack the tail; the drain is now empty.
        ack_local_reference_event_consumer(ack_request(
            &path,
            "noetl-events",
            "materializer",
            "txn-ack-0002",
            2,
        ))
        .unwrap();
        let drained = consume_local_reference_event_records(consume_request(
            &path,
            "noetl-events",
            "materializer",
            "txn-consume-0003",
            100,
        ))
        .unwrap();
        assert_eq!(drained.acked_sequence, Some(2));
        assert_eq!(drained.pending_count, 0);
        assert_eq!(drained.returned, 0);

        fs::remove_file(path).unwrap();
    }

    #[test]
    fn consume_missing_stream_reports_absent_without_creating() {
        let path = temp_log_path("eventstream-missing");
        let outcome = consume_local_reference_event_records(consume_request(
            &path,
            "never-projected",
            "materializer",
            "txn-consume-x",
            10,
        ))
        .unwrap();
        assert!(!outcome.exists);
        assert!(!outcome.created_consumer);
        assert_eq!(outcome.pending_count, 0);
        assert!(outcome.records.is_empty());
        // A probe over a never-written stream must not create the log file.
        assert!(!path.exists());
    }

    #[test]
    fn consume_honors_limit_without_advancing_cursor() {
        let path = temp_log_path("eventstream-limit");
        for index in 1..=5 {
            append_local_reference_domain_record(append_request(
                &path,
                "ticks",
                "noetl.tick",
                &format!("txn-tick-{index:04}"),
                &format!("payload-{index}"),
            ))
            .unwrap();
        }
        let outcome = consume_local_reference_event_records(consume_request(
            &path,
            "ticks",
            "reader",
            "txn-consume-lim",
            2,
        ))
        .unwrap();
        assert_eq!(outcome.pending_count, 5);
        assert_eq!(outcome.returned, 2);
        assert_eq!(outcome.records[0].sequence, 1);

        // A second pull (no ack in between) still sees all five pending — the
        // cursor did not move.
        let again = consume_local_reference_event_records(consume_request(
            &path,
            "ticks",
            "reader",
            "txn-consume-lim2",
            2,
        ))
        .unwrap();
        assert!(!again.created_consumer);
        assert_eq!(again.pending_count, 5);
        fs::remove_file(path).unwrap();
    }

    #[test]
    fn ack_rejects_backwards_cursor() {
        let path = temp_log_path("eventstream-backwards");
        for index in 1..=2 {
            append_local_reference_domain_record(append_request(
                &path,
                "orders",
                "orders.placed",
                &format!("txn-ord-{index:04}"),
                "x",
            ))
            .unwrap();
        }
        consume_local_reference_event_records(consume_request(
            &path,
            "orders",
            "c",
            "txn-consume-b",
            100,
        ))
        .unwrap();
        ack_local_reference_event_consumer(ack_request(&path, "orders", "c", "txn-ack-fwd", 2))
            .unwrap();
        let error =
            ack_local_reference_event_consumer(ack_request(&path, "orders", "c", "txn-ack-bwd", 1))
                .unwrap_err();
        assert!(matches!(error, EhdbError::InvalidState(_)));
        fs::remove_file(path).unwrap();
    }

    #[test]
    fn ack_rejects_unknown_sequence() {
        let path = temp_log_path("eventstream-unknown-seq");
        append_local_reference_domain_record(append_request(
            &path,
            "orders",
            "orders.placed",
            "txn-only",
            "x",
        ))
        .unwrap();
        consume_local_reference_event_records(consume_request(
            &path,
            "orders",
            "c",
            "txn-consume-u",
            100,
        ))
        .unwrap();
        let error =
            ack_local_reference_event_consumer(ack_request(&path, "orders", "c", "txn-ack-u", 9))
                .unwrap_err();
        assert!(matches!(error, EhdbError::NotFound(_)));
        fs::remove_file(path).unwrap();
    }

    #[test]
    fn ack_rejects_sequence_zero() {
        let path = temp_log_path("eventstream-zero-seq");
        let error =
            ack_local_reference_event_consumer(ack_request(&path, "orders", "c", "txn-ack-0", 0))
                .unwrap_err();
        // StreamSequence::new(0) is rejected before the log is even opened.
        assert!(matches!(error, EhdbError::InvalidState(_)));
        assert!(!path.exists());
    }

    // --- System WASM library store helpers (Phase E) -----------------------

    fn sys_digest(last: char) -> String {
        format!("sha256:{}{last}", "a".repeat(63))
    }

    fn publish_req(
        path: &Path,
        library_path: &str,
        revision: u32,
        digest: &str,
        txn: &str,
    ) -> PublishSystemModuleRequest {
        PublishSystemModuleRequest {
            log_path: path.to_path_buf(),
            tenant: "tenant-a".to_string(),
            namespace: "system".to_string(),
            path: library_path.to_string(),
            revision,
            digest: digest.to_string(),
            entry: "run".to_string(),
            target: "wasm32-unknown-unknown".to_string(),
            object_path: format!("system-libraries/{library_path}/{revision}/module.wasm"),
            byte_len: 512,
            capabilities: vec!["ehdb_catalog_write".to_string()],
            transaction_id: txn.to_string(),
        }
    }

    fn bind_req(
        path: &Path,
        library_path: &str,
        revision: u32,
        digest: &str,
        txn: &str,
    ) -> BindSystemChannelRequest {
        BindSystemChannelRequest {
            log_path: path.to_path_buf(),
            tenant: "tenant-a".to_string(),
            namespace: "system".to_string(),
            environment: "kind".to_string(),
            channel: "stable".to_string(),
            path: library_path.to_string(),
            revision,
            digest: digest.to_string(),
            transaction_id: txn.to_string(),
        }
    }

    fn resolve_req(path: &Path, library_path: &str) -> ResolveSystemModuleRequest {
        ResolveSystemModuleRequest {
            log_path: path.to_path_buf(),
            tenant: "tenant-a".to_string(),
            namespace: "system".to_string(),
            environment: "kind".to_string(),
            channel: "stable".to_string(),
            path: library_path.to_string(),
        }
    }

    #[test]
    fn system_publish_bind_resolve_roundtrip() {
        let path = temp_log_path("system-roundtrip");
        let d1 = sys_digest('1');
        let p = publish_local_reference_system_module(publish_req(
            &path,
            "system/catalog/bootstrap",
            1,
            &d1,
            "txn-sys-0",
        ))
        .unwrap();
        assert_eq!(p.library_count, 1);
        assert_eq!(p.revision, 1);
        assert_eq!(p.target, "wasm32-unknown-unknown");
        assert_eq!(p.capabilities, vec!["ehdb_catalog_write".to_string()]);

        let b = bind_local_reference_system_channel(bind_req(
            &path,
            "system/catalog/bootstrap",
            1,
            &d1,
            "txn-sys-1",
        ))
        .unwrap();
        assert_eq!(b.binding_count, 1);

        let r =
            resolve_local_reference_system_module(resolve_req(&path, "system/catalog/bootstrap"))
                .unwrap();
        assert!(r.exists);
        assert_eq!(r.revision, Some(1));
        assert_eq!(r.digest, Some(d1));
        assert_eq!(r.entry.as_deref(), Some("run"));
        assert_eq!(r.target.as_deref(), Some("wasm32-unknown-unknown"));
        assert_eq!(r.capabilities, vec!["ehdb_catalog_write".to_string()]);
        let _ = fs::remove_file(&path);
    }

    #[test]
    fn system_resolve_absent_when_unbound() {
        let path = temp_log_path("system-absent");
        // Fresh log, nothing published/bound: resolve is an absent probe.
        let r =
            resolve_local_reference_system_module(resolve_req(&path, "system/catalog/bootstrap"))
                .unwrap();
        assert!(!r.exists);
        assert_eq!(r.revision, None);

        // Published but not bound is still absent (the channel binding is what
        // resolve reads).
        publish_local_reference_system_module(publish_req(
            &path,
            "system/catalog/bootstrap",
            1,
            &sys_digest('1'),
            "txn-sys-0",
        ))
        .unwrap();
        let r2 =
            resolve_local_reference_system_module(resolve_req(&path, "system/catalog/bootstrap"))
                .unwrap();
        assert!(!r2.exists);
        let _ = fs::remove_file(&path);
    }

    #[test]
    fn system_rebind_hot_replaces_and_preserves_old_manifest() {
        let path = temp_log_path("system-rebind");
        let d1 = sys_digest('1');
        let d2 = sys_digest('2');
        // Publish two immutable manifests (rev1, rev2).
        publish_local_reference_system_module(publish_req(
            &path,
            "system/catalog/bootstrap",
            1,
            &d1,
            "txn-sys-0",
        ))
        .unwrap();
        let p2 = publish_local_reference_system_module(publish_req(
            &path,
            "system/catalog/bootstrap",
            2,
            &d2,
            "txn-sys-1",
        ))
        .unwrap();
        assert_eq!(p2.library_count, 2);

        // Bind stable→rev1, resolve→rev1.
        bind_local_reference_system_channel(bind_req(
            &path,
            "system/catalog/bootstrap",
            1,
            &d1,
            "txn-sys-2",
        ))
        .unwrap();
        let r1 =
            resolve_local_reference_system_module(resolve_req(&path, "system/catalog/bootstrap"))
                .unwrap();
        assert_eq!(r1.revision, Some(1));

        // Hot-rebind stable→rev2, resolve→rev2; both manifests still retained.
        let b2 = bind_local_reference_system_channel(bind_req(
            &path,
            "system/catalog/bootstrap",
            2,
            &d2,
            "txn-sys-3",
        ))
        .unwrap();
        assert_eq!(
            b2.binding_count, 1,
            "rebind replaces, not adds, the binding"
        );
        let r2 =
            resolve_local_reference_system_module(resolve_req(&path, "system/catalog/bootstrap"))
                .unwrap();
        assert_eq!(r2.revision, Some(2));
        assert_eq!(r2.digest, Some(d2));

        // The old immutable manifest is still addressable in the log.
        let summary = summarize_local_reference(&path).unwrap();
        assert_eq!(summary.system_library_count, 2);
        assert_eq!(summary.system_binding_count, 1);
        let _ = fs::remove_file(&path);
    }

    #[test]
    fn system_republish_identical_manifest_rejected() {
        let path = temp_log_path("system-republish");
        let d1 = sys_digest('1');
        publish_local_reference_system_module(publish_req(
            &path,
            "system/catalog/bootstrap",
            1,
            &d1,
            "txn-sys-0",
        ))
        .unwrap();
        let err = publish_local_reference_system_module(publish_req(
            &path,
            "system/catalog/bootstrap",
            1,
            &d1,
            "txn-sys-1",
        ))
        .unwrap_err();
        assert!(matches!(err, EhdbError::AlreadyExists(_)));
        let _ = fs::remove_file(&path);
    }

    #[test]
    fn system_bind_before_publish_not_found() {
        let path = temp_log_path("system-bind-first");
        let err = bind_local_reference_system_channel(bind_req(
            &path,
            "system/catalog/bootstrap",
            1,
            &sys_digest('1'),
            "txn-sys-0",
        ))
        .unwrap_err();
        assert!(matches!(err, EhdbError::NotFound(_)));
        let _ = fs::remove_file(&path);
    }

    #[test]
    fn system_publish_rejects_bad_target_and_capability() {
        let path = temp_log_path("system-bad-target");
        let mut bad_target = publish_req(&path, "system/x", 1, &sys_digest('1'), "txn-sys-0");
        bad_target.target = "wasm64-nope".to_string();
        assert!(matches!(
            publish_local_reference_system_module(bad_target).unwrap_err(),
            EhdbError::InvalidState(_)
        ));

        let mut bad_cap = publish_req(&path, "system/x", 1, &sys_digest('1'), "txn-sys-0");
        bad_cap.capabilities = vec!["not_a_capability".to_string()];
        assert!(matches!(
            publish_local_reference_system_module(bad_cap).unwrap_err(),
            EhdbError::InvalidState(_)
        ));
        assert!(!path.exists(), "no commit on validation failure");
        let _ = fs::remove_file(&path);
    }

    // --- bounded RAG retrieval (Phase E, noetl/ehdb#234) --------------------

    fn ingest_chunk(chunk_id: &str, ordinal: u32, text: &str) -> IngestChunkInput {
        IngestChunkInput {
            chunk_id: chunk_id.to_string(),
            ordinal,
            text: text.to_string(),
            checksum: format!("len-{}", text.len()),
        }
    }

    fn ingest_doc(
        path: &Path,
        document_id: &str,
        transaction_id: &str,
        chunks: Vec<IngestChunkInput>,
    ) -> IngestRetrievalDocumentOutcome {
        ingest_local_reference_retrieval_document(IngestRetrievalDocumentRequest {
            log_path: path.to_path_buf(),
            tenant: "noetl".to_string(),
            namespace: "default".to_string(),
            document_id: document_id.to_string(),
            source_uri: format!("artifact://{document_id}/source.md"),
            content_type: "text/markdown".to_string(),
            transaction_id: transaction_id.to_string(),
            chunks,
        })
        .unwrap()
    }

    fn retrieve(
        path: &Path,
        query: &str,
        top_k: usize,
        max_chunk_bytes: usize,
    ) -> RetrieveContextOutcome {
        retrieve_local_reference_context(RetrieveContextRequest {
            log_path: path.to_path_buf(),
            tenant: "noetl".to_string(),
            namespace: "default".to_string(),
            query: query.to_string(),
            top_k,
            max_chunk_bytes,
            time_budget_ms: 0,
        })
        .unwrap()
    }

    #[test]
    fn retrieval_ingest_and_bounded_hit() {
        let path = temp_log_path("rag-hit");
        let out = ingest_doc(
            &path,
            "doc-rag",
            "txn-rag-1",
            vec![
                ingest_chunk("doc-rag-0", 0, "NoETL retrieval lineage lives with EHDB"),
                ingest_chunk("doc-rag-1", 1, "unrelated content about weather"),
            ],
        );
        assert_eq!(out.chunk_count, 2);
        assert_eq!(out.document_count, 1);
        assert_eq!(out.retrieval_chunk_count, 2);

        let r = retrieve(&path, "retrieval", 8, 0);
        assert_eq!(r.outcome, RetrievalOutcome::Hit);
        assert_eq!(r.returned, 1);
        assert_eq!(r.hits[0].chunk_id, "doc-rag-0");
        assert!(r.hits[0].text.contains("retrieval"));
        assert!(!r.time_capped);
        let _ = fs::remove_file(&path);
    }

    #[test]
    fn retrieval_top_k_truncates() {
        let path = temp_log_path("rag-topk");
        ingest_doc(
            &path,
            "doc-topk",
            "txn-topk-1",
            vec![
                ingest_chunk("doc-topk-0", 0, "retrieval retrieval retrieval alpha"),
                ingest_chunk("doc-topk-1", 1, "retrieval retrieval beta"),
                ingest_chunk("doc-topk-2", 2, "retrieval gamma"),
            ],
        );
        let r = retrieve(&path, "retrieval", 2, 0);
        assert_eq!(r.outcome, RetrievalOutcome::Hit);
        assert_eq!(r.candidate_count, 3);
        assert_eq!(r.returned, 2);
        assert!(r.truncated_by_top_k);
        // Ranked by match_count desc: the 3-match chunk leads.
        assert_eq!(r.hits[0].chunk_id, "doc-topk-0");
        let _ = fs::remove_file(&path);
    }

    #[test]
    fn retrieval_size_cap_truncates_chunk_text() {
        let path = temp_log_path("rag-size");
        let long = format!("retrieval {}", "x".repeat(5_000));
        ingest_doc(
            &path,
            "doc-size",
            "txn-size-1",
            vec![ingest_chunk("doc-size-0", 0, &long)],
        );
        let r = retrieve(&path, "retrieval", 8, 64);
        assert_eq!(r.outcome, RetrievalOutcome::Hit);
        assert_eq!(r.returned, 1);
        assert!(r.hits[0].truncated);
        assert!(r.hits[0].text.len() <= 64);
        assert_eq!(r.hits[0].byte_len, long.len());
        let _ = fs::remove_file(&path);
    }

    #[test]
    fn retrieval_empty_when_no_match() {
        let path = temp_log_path("rag-empty");
        ingest_doc(
            &path,
            "doc-empty",
            "txn-empty-1",
            vec![ingest_chunk("doc-empty-0", 0, "only lineage here")],
        );
        let r = retrieve(&path, "nonexistentterm", 8, 0);
        assert_eq!(r.outcome, RetrievalOutcome::Empty);
        assert_eq!(r.returned, 0);
        assert!(r.hits.is_empty());
        let _ = fs::remove_file(&path);
    }

    #[test]
    fn retrieval_missing_log_is_empty_not_error() {
        let path = temp_log_path("rag-missing");
        // No ingest: the log does not exist yet.
        let r = retrieve(&path, "retrieval", 8, 0);
        assert_eq!(r.outcome, RetrievalOutcome::Empty);
        assert_eq!(r.returned, 0);
        let _ = fs::remove_file(&path);
    }

    #[test]
    fn retrieval_over_limit_rejected() {
        let path = temp_log_path("rag-reject");
        ingest_doc(
            &path,
            "doc-reject",
            "txn-reject-1",
            vec![ingest_chunk("doc-reject-0", 0, "retrieval content")],
        );
        let over_top_k = retrieve(&path, "retrieval", MAX_RETRIEVAL_TOP_K + 1, 0);
        assert_eq!(over_top_k.outcome, RetrievalOutcome::Rejected);
        assert_eq!(over_top_k.returned, 0);

        let over_bytes = retrieve(&path, "retrieval", 8, MAX_RETRIEVAL_MAX_CHUNK_BYTES + 1);
        assert_eq!(over_bytes.outcome, RetrievalOutcome::Rejected);

        let over_time = retrieve_local_reference_context(RetrieveContextRequest {
            log_path: path.clone(),
            tenant: "noetl".to_string(),
            namespace: "default".to_string(),
            query: "retrieval".to_string(),
            top_k: 8,
            max_chunk_bytes: 0,
            time_budget_ms: MAX_RETRIEVAL_TIME_BUDGET_MS + 1,
        })
        .unwrap();
        assert_eq!(over_time.outcome, RetrievalOutcome::Rejected);
        let _ = fs::remove_file(&path);
    }

    #[test]
    fn retrieval_empty_query_invalid() {
        let path = temp_log_path("rag-invalid");
        let r = retrieve(&path, "   ", 8, 0);
        assert_eq!(r.outcome, RetrievalOutcome::Invalid);
        assert_eq!(r.returned, 0);
        let _ = fs::remove_file(&path);
    }

    #[test]
    fn retrieval_is_read_only() {
        let path = temp_log_path("rag-readonly");
        ingest_doc(
            &path,
            "doc-ro",
            "txn-ro-1",
            vec![ingest_chunk("doc-ro-0", 0, "retrieval read only")],
        );
        let before = summarize_local_reference(&path).unwrap().transaction_count;
        // Several retrievals must not append anything.
        for _ in 0..3 {
            let r = retrieve(&path, "retrieval", 8, 0);
            assert_eq!(r.outcome, RetrievalOutcome::Hit);
        }
        let after = summarize_local_reference(&path).unwrap().transaction_count;
        assert_eq!(before, after, "retrieval must not write the log");
        let _ = fs::remove_file(&path);
    }

    #[test]
    fn ingest_rejects_empty_and_oversized_chunks() {
        let path = temp_log_path("rag-ingest-reject");
        let empty = ingest_local_reference_retrieval_document(IngestRetrievalDocumentRequest {
            log_path: path.clone(),
            tenant: "noetl".to_string(),
            namespace: "default".to_string(),
            document_id: "doc-none".to_string(),
            source_uri: "artifact://doc-none/source.md".to_string(),
            content_type: "text/markdown".to_string(),
            transaction_id: "txn-none".to_string(),
            chunks: Vec::new(),
        })
        .unwrap_err();
        assert!(matches!(empty, EhdbError::InvalidState(_)));

        let huge = "x".repeat(MAX_RETRIEVAL_INGEST_CHUNK_BYTES + 1);
        let oversized = ingest_local_reference_retrieval_document(IngestRetrievalDocumentRequest {
            log_path: path.clone(),
            tenant: "noetl".to_string(),
            namespace: "default".to_string(),
            document_id: "doc-huge".to_string(),
            source_uri: "artifact://doc-huge/source.md".to_string(),
            content_type: "text/markdown".to_string(),
            transaction_id: "txn-huge".to_string(),
            chunks: vec![ingest_chunk("doc-huge-0", 0, &huge)],
        })
        .unwrap_err();
        assert!(matches!(oversized, EhdbError::InvalidState(_)));
        let _ = fs::remove_file(&path);
    }
}
