use ehdb_catalog::{CreateTable, InMemoryCatalog};
use ehdb_core::{
    ChunkId, ColumnSchema, ConsumerName, DataType, DocumentId, EmbeddingModelId, NamespaceName,
    StreamName, TableName, TableSchema, TenantId, TransactionId,
};
use ehdb_retrieval::{
    InMemoryRetrievalCatalog, RegisterChunk, RegisterDocument, RegisterEmbedding,
};
use ehdb_stream::{InMemoryStreamLog, RetentionPolicy, StreamConfig, Subject};
use ehdb_transaction::{
    CatalogMutation, CommitTransaction, InMemoryTransactionLog, Mutation, RetrievalMutation,
    StreamMutation,
};

#[test]
fn records_noetl_catalog_stream_and_retrieval_mutations_in_one_replayable_log() {
    let tenant = TenantId::new("tenant-a").unwrap();
    let namespace = NamespaceName::new("system").unwrap();
    let mut catalog = InMemoryCatalog::default();
    let mut streams = InMemoryStreamLog::default();
    let mut retrieval = InMemoryRetrievalCatalog::default();
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
                table_id: table.id,
                table_name: table.name,
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
                stream: stream_name,
                subject: event.subject.as_str().to_string(),
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
                    document_id: document.id,
                }),
                Mutation::Retrieval(RetrievalMutation::RegisterChunk {
                    document_id: chunk.document_id,
                    chunk_id: chunk.id,
                }),
                Mutation::Retrieval(RetrievalMutation::RegisterEmbedding {
                    chunk_id: embedding.chunk_id,
                    model_id: embedding.model_id,
                    dimensions: embedding.dimensions,
                }),
            ],
        })
        .unwrap();

    assert_eq!(
        transactions.replay(None),
        vec![catalog_tx, stream_tx, retrieval_tx]
    );
    assert_eq!(
        retrieval
            .find_chunks_containing(&tenant, &namespace, "lineage")
            .len(),
        1
    );
}
