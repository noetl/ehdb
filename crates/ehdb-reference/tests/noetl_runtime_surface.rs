use std::{
    path::PathBuf,
    time::{SystemTime, UNIX_EPOCH},
};

use ehdb_core::{
    ChunkId, ColumnSchema, ConsumerName, DataType, DocumentId, EmbeddingModelId, NamespaceName,
    PrincipalId, SnapshotId, StreamName, TableId, TableName, TableSchema, TenantId, TransactionId,
};
use ehdb_reference::LocalReferenceRuntime;
use ehdb_storage::{ObjectDigest, ObjectPath, ObjectPlacement, ObjectRef, ObjectReplica};
use ehdb_stream::{RetentionPolicy, Subject};
use ehdb_system::{
    EnvironmentName, ModuleDigest, ReleaseChannel, ResolveSystemLibrary, SystemCapability,
    SystemLibraryPath, SystemLibraryRevision, WasmTarget,
};
use ehdb_transaction::{
    CatalogMutation, CommitTransaction, Mutation, RetrievalMutation, StorageMutation,
    StreamMutation, SystemMutation,
};

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

fn object_ref(path: &str) -> ObjectRef {
    ObjectRef {
        path: ObjectPath::new(path).unwrap(),
        len: 4096,
        digest: ObjectDigest::new(format!("sha256:{}", "a".repeat(64))).unwrap(),
        placement: ObjectPlacement::local_dev(),
    }
}

#[test]
fn noetl_runtime_surface_replays_worker_flow_from_event_log() {
    let path = temp_log_path("noetl-runtime-surface");
    let tenant = TenantId::new("tenant-a").unwrap();
    let namespace = NamespaceName::new("system").unwrap();
    let table_id = TableId::new("tenant-a_system_executions").unwrap();
    let table_name = TableName::new("executions").unwrap();
    let stream = StreamName::new("execution-events").unwrap();
    let consumer = ConsumerName::new("materializer").unwrap();
    let principal = PrincipalId::new("worker-system").unwrap();
    let document_id = DocumentId::new("doc-001").unwrap();
    let chunk_id = ChunkId::new("chunk-001").unwrap();
    let model_id = EmbeddingModelId::new("embedding-model").unwrap();
    let system_path = SystemLibraryPath::new("system/catalog/bootstrap").unwrap();
    let system_revision = SystemLibraryRevision::new(1).unwrap();
    let system_digest = ModuleDigest::new(format!("sha256:{}1", "c".repeat(63))).unwrap();
    let snapshot_object = object_ref(
        "tenant-a/system/tables/tenant-a_system_executions/snapshots/snapshot-0001/part-000.arrow",
    );

    let mut runtime = LocalReferenceRuntime::open(&path).unwrap();
    runtime
        .append(CommitTransaction {
            transaction_id: TransactionId::new("txn-create-table").unwrap(),
            tenant: tenant.clone(),
            namespace: namespace.clone(),
            mutations: vec![Mutation::Catalog(CatalogMutation::CreateTable {
                table_id: table_id.clone(),
                table_name: table_name.clone(),
                schema: TableSchema::new(vec![ColumnSchema::new(
                    "execution_id",
                    DataType::Utf8,
                    false,
                )
                .unwrap()])
                .unwrap(),
            })],
        })
        .unwrap();
    runtime
        .append(CommitTransaction {
            transaction_id: TransactionId::new("txn-commit-snapshot").unwrap(),
            tenant: tenant.clone(),
            namespace: namespace.clone(),
            mutations: vec![Mutation::Catalog(CatalogMutation::CommitSnapshot {
                table_id: table_id.clone(),
                snapshot_id: SnapshotId::new("snapshot-0001").unwrap(),
                parent_snapshot: None,
                files: vec![snapshot_object.clone()],
            })],
        })
        .unwrap();
    runtime
        .append(CommitTransaction {
            transaction_id: TransactionId::new("txn-grant-scan").unwrap(),
            tenant: tenant.clone(),
            namespace: namespace.clone(),
            mutations: vec![Mutation::Catalog(CatalogMutation::GrantScan {
                table_id: table_id.clone(),
                principal: principal.clone(),
            })],
        })
        .unwrap();
    runtime
        .append(CommitTransaction {
            transaction_id: TransactionId::new("txn-execution-event").unwrap(),
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
                    subject: Subject::new("noetl.execution.playbook.completed").unwrap(),
                    payload: b"{\"execution_id\":\"exec-1\"}".to_vec(),
                    sequence: 1,
                }),
            ],
        })
        .unwrap();
    runtime
        .append(CommitTransaction {
            transaction_id: TransactionId::new("txn-retrieval").unwrap(),
            tenant: tenant.clone(),
            namespace: namespace.clone(),
            mutations: vec![
                Mutation::Retrieval(RetrievalMutation::RegisterDocument {
                    document_id: document_id.clone(),
                    source_uri: "artifact://exec-1/result.md".to_string(),
                    content_type: "text/markdown".to_string(),
                }),
                Mutation::Retrieval(RetrievalMutation::RegisterChunk {
                    document_id: document_id.clone(),
                    chunk_id: chunk_id.clone(),
                    ordinal: 0,
                    text: "EHDB stores NoETL lineage with retrieval metadata.".to_string(),
                    checksum: "sha256-test".to_string(),
                }),
                Mutation::Retrieval(RetrievalMutation::RegisterEmbedding {
                    chunk_id: chunk_id.clone(),
                    model_id: model_id.clone(),
                    dimensions: 3,
                    vector: vec![0.1, 0.2, 0.3],
                }),
            ],
        })
        .unwrap();
    runtime
        .append(CommitTransaction {
            transaction_id: TransactionId::new("txn-system-library").unwrap(),
            tenant: tenant.clone(),
            namespace: namespace.clone(),
            mutations: vec![
                Mutation::System(SystemMutation::PublishLibrary {
                    path: system_path.clone(),
                    revision: system_revision,
                    digest: system_digest.clone(),
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
                    path: system_path.clone(),
                    environment: EnvironmentName::new("kind").unwrap(),
                    channel: ReleaseChannel::stable(),
                    revision: system_revision,
                    digest: system_digest.clone(),
                }),
            ],
        })
        .unwrap();
    runtime
        .append(CommitTransaction {
            transaction_id: TransactionId::new("txn-storage-replica").unwrap(),
            tenant: tenant.clone(),
            namespace: namespace.clone(),
            mutations: vec![Mutation::Storage(StorageMutation::RegisterReplica {
                replica: ObjectReplica::from(snapshot_object.clone()),
            })],
        })
        .unwrap();

    assert_eq!(runtime.replay().len(), 7);
    drop(runtime);

    let reopened = LocalReferenceRuntime::open(&path).unwrap();
    assert_eq!(reopened.replay().len(), 7);
    assert_eq!(reopened.state().catalog.table_count(), 1);
    assert_eq!(reopened.state().catalog.snapshot_count(), 1);
    assert!(reopened
        .state()
        .catalog
        .can_scan(&tenant, &namespace, &table_id, &principal));
    assert_eq!(
        reopened
            .state()
            .streams
            .replay_for_consumer(&tenant, &namespace, &stream, &consumer)
            .unwrap()
            .len(),
        1
    );
    assert_eq!(
        reopened
            .state()
            .retrieval
            .find_chunks_containing(&tenant, &namespace, "lineage")
            .len(),
        1
    );
    assert_eq!(
        reopened
            .state()
            .system
            .resolve(ResolveSystemLibrary {
                tenant: tenant.clone(),
                namespace: namespace.clone(),
                environment: EnvironmentName::new("kind").unwrap(),
                channel: ReleaseChannel::stable(),
                path: system_path,
            })
            .unwrap()
            .plugin_ref()
            .entry,
        "run"
    );
    assert_eq!(reopened.state().storage.replica_count(), 1);

    std::fs::remove_file(path).unwrap();
}
