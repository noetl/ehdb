use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{self, File, OpenOptions},
    io::{BufRead, BufReader, Write},
    path::{Path, PathBuf},
};

use ehdb_core::{
    ChunkId, ConsumerName, DocumentId, EhdbError, EmbeddingModelId, NamespaceName, PrincipalId,
    Result, SnapshotId, StreamName, TableId, TableName, TableSchema, TenantId, TransactionId,
};
use ehdb_storage::{
    DataGravityShard, GeoLocation, ObjectDigest, ObjectPath, ObjectRef, ObjectReplica,
};
use ehdb_stream::{RetentionPolicy, Subject};
use ehdb_system::{
    EnvironmentName, ModuleDigest, ReleaseChannel, SystemCapability, SystemLibraryPath,
    SystemLibraryRevision, WasmTarget,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct TransactionSequence(u64);

impl TransactionSequence {
    pub fn first() -> Self {
        Self(1)
    }

    pub fn next(self) -> Self {
        Self(self.0 + 1)
    }

    pub fn value(self) -> u64 {
        self.0
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub enum Mutation {
    Catalog(CatalogMutation),
    Stream(StreamMutation),
    Retrieval(RetrievalMutation),
    System(SystemMutation),
    Storage(StorageMutation),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub enum CatalogMutation {
    CreateTable {
        table_id: TableId,
        table_name: TableName,
        schema: TableSchema,
    },
    CommitSnapshot {
        table_id: TableId,
        snapshot_id: SnapshotId,
        parent_snapshot: Option<SnapshotId>,
        files: Vec<ObjectRef>,
    },
    GrantScan {
        table_id: TableId,
        principal: PrincipalId,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub enum StreamMutation {
    CreateStream {
        stream: StreamName,
        retention: RetentionPolicy,
    },
    CreateConsumer {
        stream: StreamName,
        consumer: ConsumerName,
    },
    Publish {
        stream: StreamName,
        subject: Subject,
        payload: Vec<u8>,
        sequence: u64,
    },
    Ack {
        stream: StreamName,
        consumer: ConsumerName,
        sequence: u64,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub enum RetrievalMutation {
    RegisterDocument {
        document_id: DocumentId,
        source_uri: String,
        content_type: String,
    },
    RegisterChunk {
        document_id: DocumentId,
        chunk_id: ChunkId,
        ordinal: u32,
        text: String,
        checksum: String,
    },
    RegisterEmbedding {
        chunk_id: ChunkId,
        model_id: EmbeddingModelId,
        dimensions: usize,
        vector: Vec<f32>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub enum SystemMutation {
    PublishLibrary {
        path: SystemLibraryPath,
        revision: SystemLibraryRevision,
        digest: ModuleDigest,
        entry: String,
        target: WasmTarget,
        object_path: ObjectPath,
        byte_len: u64,
        capabilities: Vec<SystemCapability>,
    },
    BindLibrary {
        path: SystemLibraryPath,
        environment: EnvironmentName,
        channel: ReleaseChannel,
        revision: SystemLibraryRevision,
        digest: ModuleDigest,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub enum StorageMutation {
    RegisterReplica { replica: ObjectReplica },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TransactionRecord {
    pub sequence: TransactionSequence,
    pub transaction_id: TransactionId,
    pub tenant: TenantId,
    pub namespace: NamespaceName,
    pub mutations: Vec<Mutation>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CommitTransaction {
    pub transaction_id: TransactionId,
    pub tenant: TenantId,
    pub namespace: NamespaceName,
    pub mutations: Vec<Mutation>,
}

#[derive(Debug, Default)]
pub struct InMemoryTransactionLog {
    next_sequence: Option<TransactionSequence>,
    records: BTreeMap<TransactionSequence, TransactionRecord>,
    transaction_ids: BTreeSet<TransactionId>,
}

impl InMemoryTransactionLog {
    pub fn append(&mut self, request: CommitTransaction) -> Result<TransactionRecord> {
        let record = self.build_record(request)?;
        self.insert_record(record.clone())?;
        Ok(record)
    }

    pub fn replay(&self, after: Option<TransactionSequence>) -> Vec<TransactionRecord> {
        self.records
            .iter()
            .filter(|(sequence, _)| after.is_none_or(|cursor| **sequence > cursor))
            .map(|(_, record)| record.clone())
            .collect()
    }

    pub fn len(&self) -> usize {
        self.records.len()
    }

    pub fn is_empty(&self) -> bool {
        self.records.is_empty()
    }

    fn build_record(&self, request: CommitTransaction) -> Result<TransactionRecord> {
        validate_transaction_id(&request.transaction_id)?;
        validate_tenant_namespace(&request.tenant, &request.namespace)?;
        validate_mutations(&request.mutations)?;
        if request.mutations.is_empty() {
            return Err(EhdbError::InvalidState(
                "transaction requires at least one mutation".to_string(),
            ));
        }
        if self.transaction_ids.contains(&request.transaction_id) {
            return Err(EhdbError::AlreadyExists(request.transaction_id.to_string()));
        }

        let sequence = self
            .next_sequence
            .unwrap_or_else(TransactionSequence::first);
        Ok(TransactionRecord {
            sequence,
            transaction_id: request.transaction_id,
            tenant: request.tenant,
            namespace: request.namespace,
            mutations: request.mutations,
        })
    }

    fn insert_record(&mut self, record: TransactionRecord) -> Result<()> {
        validate_transaction_record(&record)?;
        if record.mutations.is_empty() {
            return Err(EhdbError::InvalidState(
                "transaction requires at least one mutation".to_string(),
            ));
        }
        if !self.transaction_ids.insert(record.transaction_id.clone()) {
            return Err(EhdbError::AlreadyExists(record.transaction_id.to_string()));
        }

        let expected_sequence = self
            .next_sequence
            .unwrap_or_else(TransactionSequence::first);
        if record.sequence != expected_sequence {
            return Err(EhdbError::InvalidState(format!(
                "expected transaction sequence {}, got {}",
                expected_sequence.value(),
                record.sequence.value()
            )));
        }

        self.records.insert(record.sequence, record.clone());
        self.next_sequence = Some(record.sequence.next());
        Ok(())
    }
}

fn validate_transaction_record(record: &TransactionRecord) -> Result<()> {
    validate_transaction_id(&record.transaction_id)?;
    validate_tenant_namespace(&record.tenant, &record.namespace)?;
    validate_mutations(&record.mutations)
}

fn validate_transaction_id(transaction_id: &TransactionId) -> Result<()> {
    TransactionId::new(transaction_id.as_str()).map(|_| ())
}

fn validate_tenant_namespace(tenant: &TenantId, namespace: &NamespaceName) -> Result<()> {
    TenantId::new(tenant.as_str()).map(|_| ())?;
    NamespaceName::new(namespace.as_str()).map(|_| ())
}

fn validate_mutations(mutations: &[Mutation]) -> Result<()> {
    for mutation in mutations {
        validate_mutation(mutation)?;
    }
    Ok(())
}

fn validate_mutation(mutation: &Mutation) -> Result<()> {
    match mutation {
        Mutation::Catalog(mutation) => validate_catalog_mutation(mutation),
        Mutation::Stream(mutation) => validate_stream_mutation(mutation),
        Mutation::Retrieval(mutation) => validate_retrieval_mutation(mutation),
        Mutation::System(mutation) => validate_system_mutation(mutation),
        Mutation::Storage(mutation) => validate_storage_mutation(mutation),
    }
}

fn validate_catalog_mutation(mutation: &CatalogMutation) -> Result<()> {
    match mutation {
        CatalogMutation::CreateTable {
            table_id,
            table_name,
            schema: _,
        } => {
            TableId::new(table_id.as_str()).map(|_| ())?;
            TableName::new(table_name.as_str()).map(|_| ())
        }
        CatalogMutation::CommitSnapshot {
            table_id,
            snapshot_id,
            parent_snapshot,
            files,
        } => {
            TableId::new(table_id.as_str()).map(|_| ())?;
            SnapshotId::new(snapshot_id.as_str()).map(|_| ())?;
            if let Some(parent_snapshot) = parent_snapshot {
                SnapshotId::new(parent_snapshot.as_str()).map(|_| ())?;
            }
            for file in files {
                validate_object_ref(file)?;
            }
            Ok(())
        }
        CatalogMutation::GrantScan {
            table_id,
            principal,
        } => {
            TableId::new(table_id.as_str()).map(|_| ())?;
            PrincipalId::new(principal.as_str()).map(|_| ())
        }
    }
}

fn validate_stream_mutation(mutation: &StreamMutation) -> Result<()> {
    match mutation {
        StreamMutation::CreateStream {
            stream,
            retention: _,
        } => StreamName::new(stream.as_str()).map(|_| ()),
        StreamMutation::CreateConsumer { stream, consumer } => {
            StreamName::new(stream.as_str()).map(|_| ())?;
            ConsumerName::new(consumer.as_str()).map(|_| ())
        }
        StreamMutation::Publish {
            stream,
            subject,
            payload: _,
            sequence: _,
        } => {
            StreamName::new(stream.as_str()).map(|_| ())?;
            Subject::new(subject.as_str()).map(|_| ())
        }
        StreamMutation::Ack {
            stream,
            consumer,
            sequence: _,
        } => {
            StreamName::new(stream.as_str()).map(|_| ())?;
            ConsumerName::new(consumer.as_str()).map(|_| ())
        }
    }
}

fn validate_retrieval_mutation(mutation: &RetrievalMutation) -> Result<()> {
    match mutation {
        RetrievalMutation::RegisterDocument {
            document_id,
            source_uri: _,
            content_type: _,
        } => DocumentId::new(document_id.as_str()).map(|_| ()),
        RetrievalMutation::RegisterChunk {
            document_id,
            chunk_id,
            ordinal: _,
            text: _,
            checksum: _,
        } => {
            DocumentId::new(document_id.as_str()).map(|_| ())?;
            ChunkId::new(chunk_id.as_str()).map(|_| ())
        }
        RetrievalMutation::RegisterEmbedding {
            chunk_id,
            model_id,
            dimensions: _,
            vector: _,
        } => {
            ChunkId::new(chunk_id.as_str()).map(|_| ())?;
            EmbeddingModelId::new(model_id.as_str()).map(|_| ())
        }
    }
}

fn validate_system_mutation(mutation: &SystemMutation) -> Result<()> {
    match mutation {
        SystemMutation::PublishLibrary {
            path,
            revision,
            digest,
            entry: _,
            target: _,
            object_path,
            byte_len: _,
            capabilities: _,
        } => {
            SystemLibraryPath::new(path.as_str()).map(|_| ())?;
            SystemLibraryRevision::new(revision.value()).map(|_| ())?;
            ModuleDigest::new(digest.as_str()).map(|_| ())?;
            ObjectPath::new(object_path.as_str()).map(|_| ())
        }
        SystemMutation::BindLibrary {
            path,
            environment,
            channel,
            revision,
            digest,
        } => {
            SystemLibraryPath::new(path.as_str()).map(|_| ())?;
            EnvironmentName::new(environment.as_str()).map(|_| ())?;
            ReleaseChannel::new(channel.as_str()).map(|_| ())?;
            SystemLibraryRevision::new(revision.value()).map(|_| ())?;
            ModuleDigest::new(digest.as_str()).map(|_| ())
        }
    }
}

fn validate_storage_mutation(mutation: &StorageMutation) -> Result<()> {
    match mutation {
        StorageMutation::RegisterReplica { replica } => validate_object_replica(replica),
    }
}

fn validate_object_ref(object: &ObjectRef) -> Result<()> {
    ObjectPath::new(object.path.as_str()).map(|_| ())?;
    ObjectDigest::new(object.digest.as_str()).map(|_| ())?;
    validate_placement(&object.placement)
}

fn validate_object_replica(replica: &ObjectReplica) -> Result<()> {
    ObjectPath::new(replica.path.as_str()).map(|_| ())?;
    ObjectDigest::new(replica.digest.as_str()).map(|_| ())?;
    validate_placement(&replica.placement)
}

fn validate_placement(placement: &ehdb_storage::ObjectPlacement) -> Result<()> {
    GeoLocation::new(
        placement.geo.provider.clone(),
        placement.geo.region.as_str(),
        placement.geo.zone.as_deref(),
    )
    .map(|_| ())?;
    DataGravityShard::new(placement.data_gravity_shard.as_str()).map(|_| ())
}

#[derive(Debug)]
pub struct LocalJsonlTransactionLog {
    path: PathBuf,
    inner: InMemoryTransactionLog,
}

impl LocalJsonlTransactionLog {
    pub fn open(path: impl Into<PathBuf>) -> Result<Self> {
        let path = path.into();
        let mut inner = InMemoryTransactionLog::default();

        if path.exists() {
            let file = File::open(&path).map_err(|err| EhdbError::Storage(err.to_string()))?;
            for (index, line) in BufReader::new(file).lines().enumerate() {
                let line = line.map_err(|err| EhdbError::Storage(err.to_string()))?;
                if line.trim().is_empty() {
                    continue;
                }
                let record: TransactionRecord = serde_json::from_str(&line)
                    .map_err(|err| map_transaction_log_decode_error(index + 1, err))?;
                inner.insert_record(record)?;
            }
        }

        Ok(Self { path, inner })
    }

    pub fn append(&mut self, request: CommitTransaction) -> Result<TransactionRecord> {
        let record = self.inner.build_record(request)?;
        self.append_record_to_disk(&record)?;
        self.inner.insert_record(record.clone())?;
        Ok(record)
    }

    pub fn preview_record(&self, request: CommitTransaction) -> Result<TransactionRecord> {
        self.inner.build_record(request)
    }

    pub fn replay(&self, after: Option<TransactionSequence>) -> Vec<TransactionRecord> {
        self.inner.replay(after)
    }

    pub fn len(&self) -> usize {
        self.inner.len()
    }

    pub fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    fn append_record_to_disk(&self, record: &TransactionRecord) -> Result<()> {
        if let Some(parent) = self.path.parent() {
            fs::create_dir_all(parent).map_err(|err| EhdbError::Storage(err.to_string()))?;
        }

        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)
            .map_err(|err| EhdbError::Storage(err.to_string()))?;
        serde_json::to_writer(&mut file, record)
            .map_err(|err| EhdbError::Storage(err.to_string()))?;
        file.write_all(b"\n")
            .map_err(|err| EhdbError::Storage(err.to_string()))?;
        file.sync_data()
            .map_err(|err| EhdbError::Storage(err.to_string()))?;
        Ok(())
    }
}

fn map_transaction_log_decode_error(line: usize, err: serde_json::Error) -> EhdbError {
    let message = err.to_string();
    if let Some(value) = message.strip_prefix("invalid identifier: ") {
        let value = value
            .rsplit_once(" at line ")
            .map_or(value, |(identifier, _)| identifier);
        EhdbError::InvalidIdentifier(value.to_string())
    } else {
        EhdbError::Storage(format!(
            "invalid transaction log record at line {line}: {err}"
        ))
    }
}

#[cfg(test)]
mod tests {
    use std::{
        fs,
        time::{SystemTime, UNIX_EPOCH},
    };

    use ehdb_core::{ColumnSchema, DataType};
    use ehdb_storage::{ObjectDigest, ObjectPath, ObjectPlacement, ObjectReplica};
    use ehdb_system::{SystemCapability, WasmTarget};

    use super::*;

    fn ids() -> (TenantId, NamespaceName) {
        (
            TenantId::new("tenant-a").unwrap(),
            NamespaceName::new("system").unwrap(),
        )
    }

    fn stream_transaction(
        transaction_id: &str,
        tenant: TenantId,
        namespace: NamespaceName,
        stream: &str,
        sequence: u64,
    ) -> CommitTransaction {
        CommitTransaction {
            transaction_id: TransactionId::new(transaction_id).unwrap(),
            tenant,
            namespace,
            mutations: vec![Mutation::Stream(StreamMutation::Publish {
                stream: StreamName::new(stream).unwrap(),
                subject: Subject::new("noetl.event").unwrap(),
                payload: format!("event-{sequence}").into_bytes(),
                sequence,
            })],
        }
    }

    fn schema() -> TableSchema {
        TableSchema::new(vec![ColumnSchema::new(
            "execution_id",
            DataType::Utf8,
            false,
        )
        .unwrap()])
        .unwrap()
    }

    fn temp_log_path(test_name: &str) -> PathBuf {
        let suffix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!(
            "ehdb-transaction-{test_name}-{}-{suffix}.jsonl",
            std::process::id()
        ))
    }

    fn digest(suffix: char) -> ModuleDigest {
        ModuleDigest::new(format!("sha256:{}{}", "b".repeat(63), suffix)).unwrap()
    }

    fn write_raw_records(path: &Path, records: &[serde_json::Value]) {
        let text = records
            .iter()
            .map(serde_json::Value::to_string)
            .collect::<Vec<_>>()
            .join("\n");
        fs::write(path, format!("{text}\n")).unwrap();
    }

    fn stream_record_json() -> serde_json::Value {
        let (tenant, namespace) = ids();
        let mut log = InMemoryTransactionLog::default();
        let record = log
            .append(stream_transaction(
                "txn-0001",
                tenant,
                namespace,
                "execution-events",
                1,
            ))
            .unwrap();
        serde_json::to_value(record).unwrap()
    }

    fn catalog_record_json() -> serde_json::Value {
        let (tenant, namespace) = ids();
        let mut log = InMemoryTransactionLog::default();
        let record = log
            .append(CommitTransaction {
                transaction_id: TransactionId::new("txn-catalog-0001").unwrap(),
                tenant,
                namespace,
                mutations: vec![Mutation::Catalog(CatalogMutation::CreateTable {
                    table_id: TableId::new("tenant-a_system_executions").unwrap(),
                    table_name: TableName::new("executions").unwrap(),
                    schema: schema(),
                })],
            })
            .unwrap();
        serde_json::to_value(record).unwrap()
    }

    fn retrieval_record_json() -> serde_json::Value {
        let (tenant, namespace) = ids();
        let mut log = InMemoryTransactionLog::default();
        let record = log
            .append(CommitTransaction {
                transaction_id: TransactionId::new("txn-retrieval-0001").unwrap(),
                tenant,
                namespace,
                mutations: vec![Mutation::Retrieval(RetrievalMutation::RegisterChunk {
                    document_id: DocumentId::new("doc-0001").unwrap(),
                    chunk_id: ChunkId::new("chunk-0001").unwrap(),
                    ordinal: 0,
                    text: "NoETL retrieval context".to_string(),
                    checksum: "sha256:test".to_string(),
                })],
            })
            .unwrap();
        serde_json::to_value(record).unwrap()
    }

    fn system_record_json() -> serde_json::Value {
        let (tenant, namespace) = ids();
        let mut log = InMemoryTransactionLog::default();
        let record = log
            .append(CommitTransaction {
                transaction_id: TransactionId::new("txn-system-0001").unwrap(),
                tenant,
                namespace,
                mutations: vec![Mutation::System(SystemMutation::PublishLibrary {
                    path: SystemLibraryPath::new("system/catalog/bootstrap").unwrap(),
                    revision: SystemLibraryRevision::new(1).unwrap(),
                    digest: digest('1'),
                    entry: "run".to_string(),
                    target: WasmTarget::Wasm32UnknownUnknown,
                    object_path: ObjectPath::new(
                        "system-libraries/system/catalog/bootstrap/1/module.wasm",
                    )
                    .unwrap(),
                    byte_len: 512,
                    capabilities: vec![SystemCapability::EhdbCatalogWrite],
                })],
            })
            .unwrap();
        serde_json::to_value(record).unwrap()
    }

    fn storage_record_json() -> serde_json::Value {
        let (tenant, namespace) = ids();
        let mut log = InMemoryTransactionLog::default();
        let record = log
            .append(CommitTransaction {
                transaction_id: TransactionId::new("txn-storage-0001").unwrap(),
                tenant,
                namespace,
                mutations: vec![Mutation::Storage(StorageMutation::RegisterReplica {
                    replica: ObjectReplica {
                        path: ObjectPath::new("tenant-a/system/table/part-000.arrow").unwrap(),
                        len: 4096,
                        digest: ObjectDigest::new(format!("sha256:{}", "a".repeat(64))).unwrap(),
                        placement: ObjectPlacement::local_dev(),
                    },
                })],
            })
            .unwrap();
        serde_json::to_value(record).unwrap()
    }

    #[test]
    fn appends_and_replays_transactions_in_order() {
        let (tenant, namespace) = ids();
        let mut log = InMemoryTransactionLog::default();

        let first = log
            .append(CommitTransaction {
                transaction_id: TransactionId::new("txn-0001").unwrap(),
                tenant: tenant.clone(),
                namespace: namespace.clone(),
                mutations: vec![Mutation::Stream(StreamMutation::CreateStream {
                    stream: StreamName::new("execution-events").unwrap(),
                    retention: RetentionPolicy::KeepAll,
                })],
            })
            .unwrap();
        let second = log
            .append(CommitTransaction {
                transaction_id: TransactionId::new("txn-0002").unwrap(),
                tenant,
                namespace,
                mutations: vec![Mutation::Stream(StreamMutation::Publish {
                    stream: StreamName::new("execution-events").unwrap(),
                    subject: Subject::new("noetl.event").unwrap(),
                    payload: b"event-1".to_vec(),
                    sequence: 1,
                })],
            })
            .unwrap();

        assert_eq!(first.sequence.value(), 1);
        assert_eq!(second.sequence.value(), 2);
        assert_eq!(log.replay(Some(first.sequence)), vec![second]);
    }

    #[test]
    fn rejects_duplicate_transaction_id() {
        let (tenant, namespace) = ids();
        let mut log = InMemoryTransactionLog::default();

        let request = CommitTransaction {
            transaction_id: TransactionId::new("txn-0001").unwrap(),
            tenant,
            namespace,
            mutations: vec![Mutation::Catalog(CatalogMutation::CreateTable {
                table_id: TableId::new("tenant-a_system_executions").unwrap(),
                table_name: TableName::new("executions").unwrap(),
                schema: schema(),
            })],
        };

        log.append(request.clone()).unwrap();
        let error = log.append(request).unwrap_err();

        assert!(matches!(error, EhdbError::AlreadyExists(_)));
    }

    #[test]
    fn rejects_empty_transaction() {
        let (tenant, namespace) = ids();
        let mut log = InMemoryTransactionLog::default();

        let error = log
            .append(CommitTransaction {
                transaction_id: TransactionId::new("txn-0001").unwrap(),
                tenant,
                namespace,
                mutations: vec![],
            })
            .unwrap_err();

        assert!(matches!(error, EhdbError::InvalidState(_)));
    }

    #[test]
    fn records_system_library_publish_and_binding_mutations() {
        let (tenant, namespace) = ids();
        let mut log = InMemoryTransactionLog::default();
        let path = SystemLibraryPath::new("system/catalog/bootstrap").unwrap();
        let revision = SystemLibraryRevision::new(1).unwrap();
        let digest = digest('1');

        let record = log
            .append(CommitTransaction {
                transaction_id: TransactionId::new("txn-0001").unwrap(),
                tenant,
                namespace,
                mutations: vec![
                    Mutation::System(SystemMutation::PublishLibrary {
                        path: path.clone(),
                        revision,
                        digest: digest.clone(),
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
                        path,
                        environment: EnvironmentName::new("kind").unwrap(),
                        channel: ReleaseChannel::stable(),
                        revision,
                        digest,
                    }),
                ],
            })
            .unwrap();

        assert_eq!(log.replay(None), vec![record]);
    }

    #[test]
    fn records_storage_replica_registration_mutation() {
        let (tenant, namespace) = ids();
        let mut log = InMemoryTransactionLog::default();
        let replica = ObjectReplica {
            path: ObjectPath::new("tenant-a/system/table/part-000.arrow").unwrap(),
            len: 4096,
            digest: ObjectDigest::new(format!("sha256:{}", "a".repeat(64))).unwrap(),
            placement: ObjectPlacement::local_dev(),
        };

        let record = log
            .append(CommitTransaction {
                transaction_id: TransactionId::new("txn-storage-0001").unwrap(),
                tenant,
                namespace,
                mutations: vec![Mutation::Storage(StorageMutation::RegisterReplica {
                    replica,
                })],
            })
            .unwrap();

        assert_eq!(log.replay(None), vec![record]);
    }

    #[test]
    fn records_catalog_scan_grant_mutation() {
        let (tenant, namespace) = ids();
        let mut log = InMemoryTransactionLog::default();

        let record = log
            .append(CommitTransaction {
                transaction_id: TransactionId::new("txn-grant-scan-0001").unwrap(),
                tenant,
                namespace,
                mutations: vec![Mutation::Catalog(CatalogMutation::GrantScan {
                    table_id: TableId::new("tenant-a_system_executions").unwrap(),
                    principal: PrincipalId::new("worker-system").unwrap(),
                })],
            })
            .unwrap();

        assert_eq!(log.replay(None), vec![record]);
    }

    #[test]
    fn replay_after_latest_sequence_returns_empty() {
        let (tenant, namespace) = ids();
        let mut log = InMemoryTransactionLog::default();
        let record = log
            .append(CommitTransaction {
                transaction_id: TransactionId::new("txn-0001").unwrap(),
                tenant,
                namespace,
                mutations: vec![Mutation::Stream(StreamMutation::CreateStream {
                    stream: StreamName::new("execution-events").unwrap(),
                    retention: RetentionPolicy::KeepAll,
                })],
            })
            .unwrap();

        assert!(log.replay(Some(record.sequence)).is_empty());
    }

    #[test]
    fn local_jsonl_log_persists_and_replays_after_reopen() {
        let path = temp_log_path("restart");
        let (tenant, namespace) = ids();

        let mut log = LocalJsonlTransactionLog::open(&path).unwrap();
        let first = log
            .append(stream_transaction(
                "txn-0001",
                tenant.clone(),
                namespace.clone(),
                "execution-events",
                1,
            ))
            .unwrap();
        let second = log
            .append(stream_transaction(
                "txn-0002",
                tenant.clone(),
                namespace.clone(),
                "execution-events",
                2,
            ))
            .unwrap();
        assert_eq!(log.len(), 2);
        drop(log);

        let mut reopened = LocalJsonlTransactionLog::open(&path).unwrap();
        assert_eq!(reopened.replay(None), vec![first, second.clone()]);
        assert_eq!(reopened.path(), path.as_path());

        let third = reopened
            .append(stream_transaction(
                "txn-0003",
                tenant,
                namespace,
                "execution-events",
                3,
            ))
            .unwrap();
        assert_eq!(third.sequence.value(), 3);
        assert_eq!(reopened.replay(Some(second.sequence)), vec![third]);

        fs::remove_file(path).unwrap();
    }

    #[test]
    fn local_jsonl_log_rejects_duplicate_transaction_after_reopen() {
        let path = temp_log_path("duplicate");
        let (tenant, namespace) = ids();

        let mut log = LocalJsonlTransactionLog::open(&path).unwrap();
        log.append(stream_transaction(
            "txn-0001",
            tenant.clone(),
            namespace.clone(),
            "execution-events",
            1,
        ))
        .unwrap();
        drop(log);

        let mut reopened = LocalJsonlTransactionLog::open(&path).unwrap();
        let error = reopened
            .append(stream_transaction(
                "txn-0001",
                tenant,
                namespace,
                "execution-events",
                2,
            ))
            .unwrap_err();

        assert!(matches!(error, EhdbError::AlreadyExists(_)));

        fs::remove_file(path).unwrap();
    }

    #[test]
    fn local_jsonl_log_rejects_invalid_record_identifiers_on_open() {
        let path = temp_log_path("invalid-record-identifiers");
        let mut record = stream_record_json();
        record["transaction_id"] = serde_json::json!("txn bad");

        write_raw_records(&path, &[record]);

        let error = LocalJsonlTransactionLog::open(&path).unwrap_err();

        assert!(matches!(error, EhdbError::InvalidIdentifier(_)));

        fs::remove_file(path).unwrap();
    }

    #[test]
    fn local_jsonl_log_rejects_unknown_transaction_fields_on_open() {
        for (case, mut record, pointer) in [
            ("record", stream_record_json(), ""),
            (
                "catalog",
                catalog_record_json(),
                "/mutations/0/Catalog/CreateTable",
            ),
            (
                "stream",
                stream_record_json(),
                "/mutations/0/Stream/Publish",
            ),
            (
                "retrieval",
                retrieval_record_json(),
                "/mutations/0/Retrieval/RegisterChunk",
            ),
            (
                "system",
                system_record_json(),
                "/mutations/0/System/PublishLibrary",
            ),
            (
                "storage",
                storage_record_json(),
                "/mutations/0/Storage/RegisterReplica",
            ),
        ] {
            let path = temp_log_path(&format!("unknown-transaction-field-{case}"));
            let target = if pointer.is_empty() {
                &mut record
            } else {
                record.pointer_mut(pointer).unwrap()
            };
            target
                .as_object_mut()
                .unwrap()
                .insert("unexpected".to_string(), serde_json::json!("field"));
            write_raw_records(&path, &[record]);

            assert!(matches!(
                LocalJsonlTransactionLog::open(&path).unwrap_err(),
                EhdbError::Storage(_)
            ));

            fs::remove_file(path).unwrap();
        }
    }

    #[test]
    fn local_jsonl_log_rejects_invalid_catalog_mutation_identifiers_on_open() {
        let path = temp_log_path("invalid-catalog-mutation-identifiers");
        let mut record = catalog_record_json();
        *record
            .pointer_mut("/mutations/0/Catalog/CreateTable/table_id")
            .unwrap() = serde_json::json!("bad table");

        write_raw_records(&path, &[record]);

        let error = LocalJsonlTransactionLog::open(&path).unwrap_err();

        assert!(matches!(error, EhdbError::InvalidIdentifier(_)));

        fs::remove_file(path).unwrap();
    }

    #[test]
    fn local_jsonl_log_rejects_invalid_stream_mutation_identifiers_on_open() {
        let path = temp_log_path("invalid-stream-mutation-identifiers");
        let mut record = stream_record_json();
        *record
            .pointer_mut("/mutations/0/Stream/Publish/subject")
            .unwrap() = serde_json::json!("noetl.*");

        write_raw_records(&path, &[record]);

        let error = LocalJsonlTransactionLog::open(&path).unwrap_err();

        assert!(matches!(error, EhdbError::InvalidIdentifier(_)));

        fs::remove_file(path).unwrap();
    }

    #[test]
    fn local_jsonl_log_rejects_invalid_retrieval_mutation_identifiers_on_open() {
        let path = temp_log_path("invalid-retrieval-mutation-identifiers");
        let mut record = retrieval_record_json();
        *record
            .pointer_mut("/mutations/0/Retrieval/RegisterChunk/chunk_id")
            .unwrap() = serde_json::json!("bad chunk");

        write_raw_records(&path, &[record]);

        let error = LocalJsonlTransactionLog::open(&path).unwrap_err();

        assert!(matches!(error, EhdbError::InvalidIdentifier(_)));

        fs::remove_file(path).unwrap();
    }

    #[test]
    fn local_jsonl_log_rejects_invalid_system_mutation_identifiers_on_open() {
        let path = temp_log_path("invalid-system-mutation-identifiers");
        let mut record = system_record_json();
        *record
            .pointer_mut("/mutations/0/System/PublishLibrary/digest")
            .unwrap() = serde_json::json!("sha256:not-a-valid-digest");

        write_raw_records(&path, &[record]);

        let error = LocalJsonlTransactionLog::open(&path).unwrap_err();

        assert!(matches!(error, EhdbError::InvalidIdentifier(_)));

        fs::remove_file(path).unwrap();
    }

    #[test]
    fn local_jsonl_log_rejects_invalid_storage_mutation_identifiers_on_open() {
        let path = temp_log_path("invalid-storage-mutation-identifiers");
        let mut record = storage_record_json();
        *record
            .pointer_mut("/mutations/0/Storage/RegisterReplica/replica/path")
            .unwrap() = serde_json::json!("../unsafe");

        write_raw_records(&path, &[record]);

        let error = LocalJsonlTransactionLog::open(&path).unwrap_err();

        assert!(matches!(error, EhdbError::Storage(_)));

        fs::remove_file(path).unwrap();
    }

    #[test]
    fn local_jsonl_log_rejects_corrupt_records_on_open() {
        let path = temp_log_path("corrupt");
        fs::write(&path, b"not-json\n").unwrap();

        let error = LocalJsonlTransactionLog::open(&path).unwrap_err();

        assert!(matches!(error, EhdbError::Storage(_)));

        fs::remove_file(path).unwrap();
    }
}
