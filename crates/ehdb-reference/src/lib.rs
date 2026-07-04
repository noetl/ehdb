use std::{
    collections::BTreeSet,
    io::Cursor,
    path::{Path, PathBuf},
    sync::Arc,
};

use arrow_array::{Array, ArrayRef, Int64Array, RecordBatch, StringArray};
use arrow_ipc::{reader::FileReader, writer::FileWriter};
use arrow_schema::{Field, Schema};
use ehdb_catalog::{CommitSnapshot, CreateTable, GrantScan, InMemoryCatalog};
use ehdb_core::{
    ColumnSchema, DataType, EhdbError, NamespaceName, Result, SnapshotId, TableName, TableSchema,
    TenantId, TransactionId,
};
use ehdb_retrieval::{
    InMemoryRetrievalCatalog, RegisterChunk, RegisterDocument, RegisterEmbedding,
};
use ehdb_storage::{
    table_snapshot_object_path, ImmutableObjectStore, InMemoryObjectReplicaRegistry, ObjectRef,
    ObjectReplica, ReplicationAction, ReplicationPlan,
};
use ehdb_stream::{InMemoryStreamLog, StreamConfig, StreamSequence};
use ehdb_system::{BindSystemLibrary, InMemorySystemLibraryCatalog, PublishSystemLibrary};
use ehdb_transaction::{
    CatalogMutation, CommitTransaction, LocalJsonlTransactionLog, Mutation, RetrievalMutation,
    StorageMutation, StreamMutation, SystemMutation, TransactionRecord,
};
use serde::{Deserialize, Serialize};

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
}
