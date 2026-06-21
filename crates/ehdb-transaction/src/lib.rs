use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{self, File, OpenOptions},
    io::{BufRead, BufReader, Write},
    path::{Path, PathBuf},
};

use ehdb_core::{
    ChunkId, ConsumerName, DocumentId, EhdbError, EmbeddingModelId, NamespaceName, Result,
    StreamName, TableId, TableName, TenantId, TransactionId,
};
use ehdb_system::{
    EnvironmentName, ModuleDigest, ReleaseChannel, SystemLibraryPath, SystemLibraryRevision,
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Mutation {
    Catalog(CatalogMutation),
    Stream(StreamMutation),
    Retrieval(RetrievalMutation),
    System(SystemMutation),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum CatalogMutation {
    CreateTable {
        table_id: TableId,
        table_name: TableName,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum StreamMutation {
    CreateStream {
        stream: StreamName,
    },
    CreateConsumer {
        stream: StreamName,
        consumer: ConsumerName,
    },
    Publish {
        stream: StreamName,
        subject: String,
        sequence: u64,
    },
    Ack {
        stream: StreamName,
        consumer: ConsumerName,
        sequence: u64,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum RetrievalMutation {
    RegisterDocument {
        document_id: DocumentId,
    },
    RegisterChunk {
        document_id: DocumentId,
        chunk_id: ChunkId,
    },
    RegisterEmbedding {
        chunk_id: ChunkId,
        model_id: EmbeddingModelId,
        dimensions: usize,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum SystemMutation {
    PublishLibrary {
        path: SystemLibraryPath,
        revision: SystemLibraryRevision,
        digest: ModuleDigest,
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
pub struct TransactionRecord {
    pub sequence: TransactionSequence,
    pub transaction_id: TransactionId,
    pub tenant: TenantId,
    pub namespace: NamespaceName,
    pub mutations: Vec<Mutation>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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
                let record: TransactionRecord = serde_json::from_str(&line).map_err(|err| {
                    EhdbError::Storage(format!(
                        "invalid transaction log record at line {}: {err}",
                        index + 1
                    ))
                })?;
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

#[cfg(test)]
mod tests {
    use std::{
        fs,
        time::{SystemTime, UNIX_EPOCH},
    };

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
                subject: "noetl.event".to_string(),
                sequence,
            })],
        }
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
                    subject: "noetl.event".to_string(),
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
    fn local_jsonl_log_rejects_corrupt_records_on_open() {
        let path = temp_log_path("corrupt");
        fs::write(&path, b"not-json\n").unwrap();

        let error = LocalJsonlTransactionLog::open(&path).unwrap_err();

        assert!(matches!(error, EhdbError::Storage(_)));

        fs::remove_file(path).unwrap();
    }
}
