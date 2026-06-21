use std::collections::{BTreeMap, BTreeSet};

use ehdb_core::{
    ChunkId, ConsumerName, DocumentId, EhdbError, EmbeddingModelId, NamespaceName, Result,
    StreamName, TableId, TableName, TenantId, TransactionId,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Mutation {
    Catalog(CatalogMutation),
    Stream(StreamMutation),
    Retrieval(RetrievalMutation),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CatalogMutation {
    CreateTable {
        table_id: TableId,
        table_name: TableName,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
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

#[derive(Debug, Clone, PartialEq, Eq)]
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TransactionRecord {
    pub sequence: TransactionSequence,
    pub transaction_id: TransactionId,
    pub tenant: TenantId,
    pub namespace: NamespaceName,
    pub mutations: Vec<Mutation>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
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
        let record = TransactionRecord {
            sequence,
            transaction_id: request.transaction_id,
            tenant: request.tenant,
            namespace: request.namespace,
            mutations: request.mutations,
        };

        self.transaction_ids.insert(record.transaction_id.clone());
        self.records.insert(sequence, record.clone());
        self.next_sequence = Some(sequence.next());
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
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ids() -> (TenantId, NamespaceName) {
        (
            TenantId::new("tenant-a").unwrap(),
            NamespaceName::new("system").unwrap(),
        )
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
}
