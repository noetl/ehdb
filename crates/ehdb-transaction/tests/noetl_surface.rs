use ehdb_catalog::{CreateTable, InMemoryCatalog};
use ehdb_core::{
    ChunkId, ColumnSchema, ConsumerName, DataType, DocumentId, EmbeddingModelId, NamespaceName,
    StreamName, TableName, TableSchema, TenantId, TransactionId,
};
use ehdb_retrieval::{
    InMemoryRetrievalCatalog, RegisterChunk, RegisterDocument, RegisterEmbedding,
};
use ehdb_storage::ObjectPath;
use ehdb_stream::{InMemoryStreamLog, RetentionPolicy, StreamConfig, Subject};
use ehdb_system::{
    BindSystemLibrary, EnvironmentName, InMemorySystemLibraryCatalog, ModuleDigest,
    PublishSystemLibrary, ReleaseChannel, SystemCapability, SystemLibraryPath,
    SystemLibraryRevision, WasmTarget,
};
use ehdb_transaction::{
    CatalogMutation, CommitTransaction, InMemoryTransactionLog, Mutation, RetrievalMutation,
    StreamMutation, SystemMutation,
};

#[test]
fn records_noetl_catalog_stream_and_retrieval_mutations_in_one_replayable_log() {
    let tenant = TenantId::new("tenant-a").unwrap();
    let namespace = NamespaceName::new("system").unwrap();
    let mut catalog = InMemoryCatalog::default();
    let mut streams = InMemoryStreamLog::default();
    let mut retrieval = InMemoryRetrievalCatalog::default();
    let mut system = InMemorySystemLibraryCatalog::default();
    let mut transactions = InMemoryTransactionLog::default();

    let table = catalog
        .create_table(CreateTable {
            tenant: tenant.clone(),
            namespace: namespace.clone(),
            name: TableName::new("executions").unwrap(),
            schema: TableSchema::new(vec![ColumnSchema::new(
                "execution_id",
                DataType::Utf8,
                false,
            )
            .unwrap()])
            .unwrap(),
            transaction_id: TransactionId::new("txn-0001").unwrap(),
        })
        .unwrap();
    let catalog_tx = transactions
        .append(CommitTransaction {
            transaction_id: TransactionId::new("txn-0001").unwrap(),
            tenant: tenant.clone(),
            namespace: namespace.clone(),
            mutations: vec![Mutation::Catalog(CatalogMutation::CreateTable {
                table_id: table.id.clone(),
                table_name: table.name.clone(),
                schema: table.schema.clone(),
            })],
        })
        .unwrap();

    let stream_name = StreamName::new("execution-events").unwrap();
    streams
        .create_stream(StreamConfig {
            tenant: tenant.clone(),
            namespace: namespace.clone(),
            name: stream_name.clone(),
            retention: RetentionPolicy::KeepAll,
        })
        .unwrap();
    streams
        .create_consumer(
            &tenant,
            &namespace,
            &stream_name,
            ConsumerName::new("materializer").unwrap(),
        )
        .unwrap();
    let event = streams
        .publish(
            &tenant,
            &namespace,
            &stream_name,
            Subject::new("noetl.execution.playbook.completed").unwrap(),
            b"{\"execution_id\":\"exec-1\"}".to_vec(),
            TransactionId::new("txn-0002").unwrap(),
        )
        .unwrap();
    let stream_tx = transactions
        .append(CommitTransaction {
            transaction_id: TransactionId::new("txn-0002").unwrap(),
            tenant: tenant.clone(),
            namespace: namespace.clone(),
            mutations: vec![Mutation::Stream(StreamMutation::Publish {
                stream: stream_name.clone(),
                subject: event.subject.clone(),
                payload: event.payload.clone(),
                sequence: event.sequence.value(),
            })],
        })
        .unwrap();

    let document = retrieval
        .register_document(RegisterDocument {
            id: DocumentId::new("doc-001").unwrap(),
            tenant: tenant.clone(),
            namespace: namespace.clone(),
            source_uri: "artifact://exec-1/result.md".to_string(),
            content_type: "text/markdown".to_string(),
            transaction_id: TransactionId::new("txn-0003").unwrap(),
        })
        .unwrap();
    let chunk = retrieval
        .register_chunk(RegisterChunk {
            id: ChunkId::new("chunk-001").unwrap(),
            document_id: document.id.clone(),
            ordinal: 0,
            text: "EHDB stores NoETL lineage with retrieval metadata.".to_string(),
            checksum: "sha256-test".to_string(),
            transaction_id: TransactionId::new("txn-0003").unwrap(),
        })
        .unwrap();
    let embedding = retrieval
        .register_embedding(RegisterEmbedding {
            chunk_id: chunk.id.clone(),
            model_id: EmbeddingModelId::new("embedding-model").unwrap(),
            dimensions: 3,
            vector: vec![0.1, 0.2, 0.3],
            transaction_id: TransactionId::new("txn-0003").unwrap(),
        })
        .unwrap();
    let retrieval_tx = transactions
        .append(CommitTransaction {
            transaction_id: TransactionId::new("txn-0003").unwrap(),
            tenant: tenant.clone(),
            namespace: namespace.clone(),
            mutations: vec![
                Mutation::Retrieval(RetrievalMutation::RegisterDocument {
                    document_id: document.id.clone(),
                    source_uri: document.source_uri.clone(),
                    content_type: document.content_type.clone(),
                }),
                Mutation::Retrieval(RetrievalMutation::RegisterChunk {
                    document_id: chunk.document_id.clone(),
                    chunk_id: chunk.id.clone(),
                    ordinal: chunk.ordinal,
                    text: chunk.text.clone(),
                    checksum: chunk.checksum.clone(),
                }),
                Mutation::Retrieval(RetrievalMutation::RegisterEmbedding {
                    chunk_id: embedding.chunk_id.clone(),
                    model_id: embedding.model_id.clone(),
                    dimensions: embedding.dimensions,
                    vector: embedding.vector.clone(),
                }),
            ],
        })
        .unwrap();

    let system_path = SystemLibraryPath::new("system/catalog/bootstrap").unwrap();
    let system_revision = SystemLibraryRevision::new(1).unwrap();
    let system_digest = ModuleDigest::new(format!("sha256:{}1", "c".repeat(63))).unwrap();
    let system_library = system
        .publish(PublishSystemLibrary {
            path: system_path.clone(),
            revision: system_revision,
            digest: system_digest.clone(),
            entry: "run".to_string(),
            target: WasmTarget::Wasm32UnknownUnknown,
            object_path: ObjectPath::new("system-libraries/system/catalog/bootstrap/1/module.wasm")
                .unwrap(),
            byte_len: 512,
            capabilities: vec![SystemCapability::EhdbCatalogWrite],
            transaction_id: TransactionId::new("txn-0004").unwrap(),
        })
        .unwrap();
    system
        .bind(BindSystemLibrary {
            tenant: tenant.clone(),
            namespace: namespace.clone(),
            environment: EnvironmentName::new("kind").unwrap(),
            channel: ReleaseChannel::stable(),
            path: system_path.clone(),
            revision: system_revision,
            digest: system_digest.clone(),
            transaction_id: TransactionId::new("txn-0004").unwrap(),
        })
        .unwrap();
    let system_tx = transactions
        .append(CommitTransaction {
            transaction_id: TransactionId::new("txn-0004").unwrap(),
            tenant: tenant.clone(),
            namespace: namespace.clone(),
            mutations: vec![
                Mutation::System(SystemMutation::PublishLibrary {
                    path: system_path.clone(),
                    revision: system_revision,
                    digest: system_digest.clone(),
                    entry: system_library.entry.clone(),
                    target: system_library.target.clone(),
                    object_path: system_library.object_path.clone(),
                    byte_len: system_library.byte_len,
                    capabilities: system_library.capabilities.clone(),
                }),
                Mutation::System(SystemMutation::BindLibrary {
                    path: system_path,
                    environment: EnvironmentName::new("kind").unwrap(),
                    channel: ReleaseChannel::stable(),
                    revision: system_revision,
                    digest: system_digest,
                }),
            ],
        })
        .unwrap();

    assert_eq!(
        transactions.replay(None),
        vec![catalog_tx, stream_tx, retrieval_tx, system_tx]
    );
    assert_eq!(system_library.plugin_ref().entry, "run");
    assert_eq!(
        retrieval
            .find_chunks_containing(&tenant, &namespace, "lineage")
            .len(),
        1
    );
}
