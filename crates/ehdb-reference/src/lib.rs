use ehdb_catalog::{CommitSnapshot, CreateTable, InMemoryCatalog};
use ehdb_core::{EhdbError, NamespaceName, Result, TenantId, TransactionId};
use ehdb_retrieval::{
    InMemoryRetrievalCatalog, RegisterChunk, RegisterDocument, RegisterEmbedding,
};
use ehdb_storage::{
    ImmutableObjectStore, InMemoryObjectReplicaRegistry, ObjectRef, ObjectReplica,
    ReplicationAction, ReplicationPlan,
};
use ehdb_stream::{InMemoryStreamLog, StreamConfig, StreamSequence};
use ehdb_system::{BindSystemLibrary, InMemorySystemLibraryCatalog, PublishSystemLibrary};
use ehdb_transaction::{
    CatalogMutation, CommitTransaction, LocalJsonlTransactionLog, Mutation, RetrievalMutation,
    StorageMutation, StreamMutation, SystemMutation, TransactionRecord,
};
use std::path::{Path, PathBuf};

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
        sync::atomic::{AtomicU64, Ordering},
        time::{SystemTime, UNIX_EPOCH},
    };

    use ehdb_core::{
        ChunkId, ColumnSchema, ConsumerName, DataType, DocumentId, EmbeddingModelId, NamespaceName,
        SnapshotId, StreamName, TableId, TableName, TableSchema, TenantId, TransactionId,
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

        assert_eq!(record.sequence.value(), 1);
        assert_eq!(runtime.state().catalog.table_count(), 1);
        assert_eq!(runtime.state().catalog.snapshot_count(), 1);
        assert_eq!(runtime.replay().len(), 2);
        assert_eq!(runtime.path(), path.as_path());
        drop(runtime);

        let reopened = LocalReferenceRuntime::open(&path).unwrap();
        assert_eq!(reopened.state().catalog.table_count(), 1);
        assert_eq!(reopened.state().catalog.snapshot_count(), 1);
        assert_eq!(reopened.replay().len(), 2);

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
