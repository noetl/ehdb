use ehdb_catalog::{CreateTable, InMemoryCatalog};
use ehdb_core::{EhdbError, Result};
use ehdb_retrieval::{
    InMemoryRetrievalCatalog, RegisterChunk, RegisterDocument, RegisterEmbedding,
};
use ehdb_stream::{InMemoryStreamLog, StreamConfig, StreamSequence};
use ehdb_system::{BindSystemLibrary, InMemorySystemLibraryCatalog, PublishSystemLibrary};
use ehdb_transaction::{
    CatalogMutation, Mutation, RetrievalMutation, StreamMutation, SystemMutation, TransactionRecord,
};

#[derive(Debug, Default)]
pub struct ReferenceDatabase {
    pub catalog: InMemoryCatalog,
    pub streams: InMemoryStreamLog,
    pub retrieval: InMemoryRetrievalCatalog,
    pub system: InMemorySystemLibraryCatalog,
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
}

#[cfg(test)]
mod tests {
    use ehdb_core::{
        ChunkId, ColumnSchema, ConsumerName, DataType, DocumentId, EmbeddingModelId, NamespaceName,
        StreamName, TableId, TableName, TableSchema, TenantId, TransactionId,
    };
    use ehdb_storage::ObjectPath;
    use ehdb_stream::{RetentionPolicy, Subject};
    use ehdb_system::{
        EnvironmentName, ModuleDigest, ReleaseChannel, SystemCapability, SystemLibraryPath,
        SystemLibraryRevision, WasmTarget,
    };
    use ehdb_transaction::{
        CatalogMutation, CommitTransaction, InMemoryTransactionLog, Mutation, RetrievalMutation,
        StreamMutation, SystemMutation,
    };

    use super::*;

    fn ids() -> (TenantId, NamespaceName) {
        (
            TenantId::new("tenant-a").unwrap(),
            NamespaceName::new("system").unwrap(),
        )
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
                table_id: TableId::new("tenant-a_system_executions").unwrap(),
                table_name: TableName::new("executions").unwrap(),
                schema,
            })],
        })
        .unwrap();
        log.append(CommitTransaction {
            transaction_id: TransactionId::new("txn-0002").unwrap(),
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
            transaction_id: TransactionId::new("txn-0003").unwrap(),
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
            transaction_id: TransactionId::new("txn-0004").unwrap(),
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

        let mut reference = ReferenceDatabase::default();
        let records = log.replay(None);
        reference.apply_records(&records).unwrap();

        assert_eq!(reference.catalog.table_count(), 1);
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
