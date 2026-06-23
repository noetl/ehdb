use std::{
    collections::BTreeMap,
    fs::{self, File, OpenOptions},
    io::{BufRead, BufReader, Write},
    path::{Path, PathBuf},
};

use ehdb_core::{
    ConsumerName, EhdbError, NamespaceName, Result, StreamName, TenantId, TransactionId,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct StreamSequence(u64);

impl StreamSequence {
    pub fn new(value: u64) -> Result<Self> {
        if value == 0 {
            Err(EhdbError::InvalidState(
                "stream sequence must be greater than zero".to_string(),
            ))
        } else {
            Ok(Self(value))
        }
    }

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

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct Subject(String);

impl Subject {
    pub fn new(value: impl Into<String>) -> Result<Self> {
        let value = value.into();
        let valid = !value.is_empty()
            && value.len() <= 256
            && value
                .chars()
                .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '.' | '_' | '-' | '*' | '>'));

        if valid {
            Ok(Self(value))
        } else {
            Err(EhdbError::InvalidIdentifier(value))
        }
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn matches(&self, subject: &Subject) -> bool {
        let filter_tokens = self.0.split('.').collect::<Vec<_>>();
        let subject_tokens = subject.0.split('.').collect::<Vec<_>>();
        let mut filter_index = 0;
        let mut subject_index = 0;

        while filter_index < filter_tokens.len() && subject_index < subject_tokens.len() {
            match filter_tokens[filter_index] {
                ">" => {
                    return filter_index == filter_tokens.len() - 1
                        && subject_index < subject_tokens.len();
                }
                "*" => {
                    filter_index += 1;
                    subject_index += 1;
                }
                token if token == subject_tokens[subject_index] => {
                    filter_index += 1;
                    subject_index += 1;
                }
                _ => return false,
            }
        }

        filter_index == filter_tokens.len() && subject_index == subject_tokens.len()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum RetentionPolicy {
    KeepAll,
    MaxRecords(usize),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamConfig {
    pub tenant: TenantId,
    pub namespace: NamespaceName,
    pub name: StreamName,
    pub retention: RetentionPolicy,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamRecord {
    pub sequence: StreamSequence,
    pub subject: Subject,
    pub payload: Vec<u8>,
    pub transaction_id: TransactionId,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DurableConsumer {
    pub name: ConsumerName,
    pub acked_sequence: Option<StreamSequence>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct StreamKey {
    tenant: TenantId,
    namespace: NamespaceName,
    name: StreamName,
}

fn stream_key(tenant: &TenantId, namespace: &NamespaceName, stream: &StreamName) -> StreamKey {
    StreamKey {
        tenant: tenant.clone(),
        namespace: namespace.clone(),
        name: stream.clone(),
    }
}

fn validate_stream_config(config: &StreamConfig) -> Result<()> {
    if matches!(config.retention, RetentionPolicy::MaxRecords(0)) {
        return Err(EhdbError::InvalidState(
            "stream max-record retention must be greater than zero".to_string(),
        ));
    }
    Ok(())
}

#[derive(Debug, Clone)]
struct StreamState {
    config: StreamConfig,
    next_sequence: StreamSequence,
    records: BTreeMap<StreamSequence, StreamRecord>,
    consumers: BTreeMap<ConsumerName, DurableConsumer>,
}

impl StreamState {
    fn new(config: StreamConfig) -> Self {
        Self {
            config,
            next_sequence: StreamSequence::first(),
            records: BTreeMap::new(),
            consumers: BTreeMap::new(),
        }
    }

    fn enforce_retention(&mut self) {
        let RetentionPolicy::MaxRecords(limit) = self.config.retention else {
            return;
        };

        while self.records.len() > limit {
            if let Some(sequence) = self.records.keys().next().copied() {
                self.records.remove(&sequence);
            }
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct InMemoryStreamLog {
    streams: BTreeMap<StreamKey, StreamState>,
}

impl InMemoryStreamLog {
    pub fn create_stream(&mut self, config: StreamConfig) -> Result<()> {
        validate_stream_config(&config)?;
        self.ensure_stream_absent(&config)?;
        let key = stream_key(&config.tenant, &config.namespace, &config.name);
        self.streams.insert(key, StreamState::new(config));
        Ok(())
    }

    pub fn publish(
        &mut self,
        tenant: &TenantId,
        namespace: &NamespaceName,
        stream: &StreamName,
        subject: Subject,
        payload: impl Into<Vec<u8>>,
        transaction_id: TransactionId,
    ) -> Result<StreamRecord> {
        let record = StreamRecord {
            sequence: self.stream(tenant, namespace, stream)?.next_sequence,
            subject,
            payload: payload.into(),
            transaction_id,
        };

        self.insert_record(tenant, namespace, stream, record.clone())?;
        Ok(record)
    }

    fn insert_record(
        &mut self,
        tenant: &TenantId,
        namespace: &NamespaceName,
        stream: &StreamName,
        record: StreamRecord,
    ) -> Result<()> {
        let state = self.stream_mut(tenant, namespace, stream)?;
        if record.sequence != state.next_sequence {
            return Err(EhdbError::InvalidState(format!(
                "expected stream sequence {}, got {}",
                state.next_sequence.value(),
                record.sequence.value()
            )));
        }

        state.records.insert(record.sequence, record.clone());
        state.next_sequence = state.next_sequence.next();
        state.enforce_retention();
        Ok(())
    }

    pub fn create_consumer(
        &mut self,
        tenant: &TenantId,
        namespace: &NamespaceName,
        stream: &StreamName,
        consumer: ConsumerName,
    ) -> Result<DurableConsumer> {
        self.ensure_consumer_absent(tenant, namespace, stream, &consumer)?;
        let state = self.stream_mut(tenant, namespace, stream)?;

        let durable = DurableConsumer {
            name: consumer.clone(),
            acked_sequence: None,
        };
        state.consumers.insert(consumer, durable.clone());
        Ok(durable)
    }

    pub fn replay(
        &self,
        tenant: &TenantId,
        namespace: &NamespaceName,
        stream: &StreamName,
        after: Option<StreamSequence>,
    ) -> Result<Vec<StreamRecord>> {
        self.replay_records(tenant, namespace, stream, after, None)
    }

    pub fn replay_matching(
        &self,
        tenant: &TenantId,
        namespace: &NamespaceName,
        stream: &StreamName,
        subject_filter: &Subject,
        after: Option<StreamSequence>,
    ) -> Result<Vec<StreamRecord>> {
        self.replay_records(tenant, namespace, stream, after, Some(subject_filter))
    }

    fn replay_records(
        &self,
        tenant: &TenantId,
        namespace: &NamespaceName,
        stream: &StreamName,
        after: Option<StreamSequence>,
        subject_filter: Option<&Subject>,
    ) -> Result<Vec<StreamRecord>> {
        let state = self.stream(tenant, namespace, stream)?;
        Ok(state
            .records
            .iter()
            .filter(|(sequence, _)| after.is_none_or(|cursor| **sequence > cursor))
            .filter(|(_, record)| {
                subject_filter.is_none_or(|filter| filter.matches(&record.subject))
            })
            .map(|(_, record)| record.clone())
            .collect())
    }

    pub fn replay_for_consumer(
        &self,
        tenant: &TenantId,
        namespace: &NamespaceName,
        stream: &StreamName,
        consumer: &ConsumerName,
    ) -> Result<Vec<StreamRecord>> {
        let state = self.stream(tenant, namespace, stream)?;
        let consumer = state
            .consumers
            .get(consumer)
            .ok_or_else(|| EhdbError::NotFound(consumer.to_string()))?;
        self.replay(tenant, namespace, stream, consumer.acked_sequence)
    }

    pub fn replay_matching_for_consumer(
        &self,
        tenant: &TenantId,
        namespace: &NamespaceName,
        stream: &StreamName,
        consumer: &ConsumerName,
        subject_filter: &Subject,
    ) -> Result<Vec<StreamRecord>> {
        let state = self.stream(tenant, namespace, stream)?;
        let consumer = state
            .consumers
            .get(consumer)
            .ok_or_else(|| EhdbError::NotFound(consumer.to_string()))?;
        self.replay_matching(
            tenant,
            namespace,
            stream,
            subject_filter,
            consumer.acked_sequence,
        )
    }

    pub fn ack(
        &mut self,
        tenant: &TenantId,
        namespace: &NamespaceName,
        stream: &StreamName,
        consumer: &ConsumerName,
        sequence: StreamSequence,
    ) -> Result<DurableConsumer> {
        self.validate_ack(tenant, namespace, stream, consumer, sequence)?;
        let state = self.stream_mut(tenant, namespace, stream)?;
        let consumer_state = state
            .consumers
            .get_mut(consumer)
            .ok_or_else(|| EhdbError::NotFound(consumer.to_string()))?;

        consumer_state.acked_sequence = Some(sequence);
        Ok(consumer_state.clone())
    }

    fn ensure_stream_absent(&self, config: &StreamConfig) -> Result<()> {
        let key = stream_key(&config.tenant, &config.namespace, &config.name);
        if self.streams.contains_key(&key) {
            return Err(EhdbError::AlreadyExists(format!(
                "{}.{}.{}",
                key.tenant, key.namespace, key.name
            )));
        }

        Ok(())
    }

    fn ensure_consumer_absent(
        &self,
        tenant: &TenantId,
        namespace: &NamespaceName,
        stream: &StreamName,
        consumer: &ConsumerName,
    ) -> Result<()> {
        let state = self.stream(tenant, namespace, stream)?;
        if state.consumers.contains_key(consumer) {
            return Err(EhdbError::AlreadyExists(consumer.to_string()));
        }

        Ok(())
    }

    fn validate_ack(
        &self,
        tenant: &TenantId,
        namespace: &NamespaceName,
        stream: &StreamName,
        consumer: &ConsumerName,
        sequence: StreamSequence,
    ) -> Result<()> {
        let state = self.stream(tenant, namespace, stream)?;
        if !state.records.contains_key(&sequence) {
            return Err(EhdbError::NotFound(format!(
                "stream sequence {}",
                sequence.value()
            )));
        }

        let consumer_state = state
            .consumers
            .get(consumer)
            .ok_or_else(|| EhdbError::NotFound(consumer.to_string()))?;

        if consumer_state
            .acked_sequence
            .is_some_and(|acked| sequence < acked)
        {
            return Err(EhdbError::InvalidState(format!(
                "cannot move consumer {} cursor backwards from {} to {}",
                consumer,
                consumer_state.acked_sequence.unwrap().value(),
                sequence.value()
            )));
        }

        Ok(())
    }

    fn stream(
        &self,
        tenant: &TenantId,
        namespace: &NamespaceName,
        stream: &StreamName,
    ) -> Result<&StreamState> {
        let key = StreamKey {
            tenant: tenant.clone(),
            namespace: namespace.clone(),
            name: stream.clone(),
        };
        self.streams
            .get(&key)
            .ok_or_else(|| EhdbError::NotFound(format!("{tenant}.{namespace}.{stream}")))
    }

    fn stream_mut(
        &mut self,
        tenant: &TenantId,
        namespace: &NamespaceName,
        stream: &StreamName,
    ) -> Result<&mut StreamState> {
        let key = StreamKey {
            tenant: tenant.clone(),
            namespace: namespace.clone(),
            name: stream.clone(),
        };
        self.streams
            .get_mut(&key)
            .ok_or_else(|| EhdbError::NotFound(format!("{tenant}.{namespace}.{stream}")))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
enum StreamJournalEntry {
    CreateStream {
        config: StreamConfig,
    },
    CreateConsumer {
        tenant: TenantId,
        namespace: NamespaceName,
        stream: StreamName,
        consumer: ConsumerName,
    },
    Publish {
        tenant: TenantId,
        namespace: NamespaceName,
        stream: StreamName,
        record: StreamRecord,
    },
    Ack {
        tenant: TenantId,
        namespace: NamespaceName,
        stream: StreamName,
        consumer: ConsumerName,
        sequence: StreamSequence,
    },
}

#[derive(Debug)]
pub struct LocalJsonlStreamLog {
    path: PathBuf,
    inner: InMemoryStreamLog,
}

impl LocalJsonlStreamLog {
    pub fn open(path: impl Into<PathBuf>) -> Result<Self> {
        let path = path.into();
        let mut inner = InMemoryStreamLog::default();

        if path.exists() {
            let file = File::open(&path).map_err(|err| EhdbError::Storage(err.to_string()))?;
            for (index, line) in BufReader::new(file).lines().enumerate() {
                let line = line.map_err(|err| EhdbError::Storage(err.to_string()))?;
                if line.trim().is_empty() {
                    continue;
                }
                let entry: StreamJournalEntry = serde_json::from_str(&line).map_err(|err| {
                    EhdbError::Storage(format!(
                        "invalid stream log record at line {}: {err}",
                        index + 1
                    ))
                })?;
                apply_journal_entry(&mut inner, entry)?;
            }
        }

        Ok(Self { path, inner })
    }

    pub fn create_stream(&mut self, config: StreamConfig) -> Result<()> {
        validate_stream_config(&config)?;
        self.inner.ensure_stream_absent(&config)?;
        let entry = StreamJournalEntry::CreateStream {
            config: config.clone(),
        };
        self.append_entry_to_disk(&entry)?;
        self.inner.create_stream(config)
    }

    pub fn create_consumer(
        &mut self,
        tenant: &TenantId,
        namespace: &NamespaceName,
        stream: &StreamName,
        consumer: ConsumerName,
    ) -> Result<DurableConsumer> {
        self.inner
            .ensure_consumer_absent(tenant, namespace, stream, &consumer)?;
        let entry = StreamJournalEntry::CreateConsumer {
            tenant: tenant.clone(),
            namespace: namespace.clone(),
            stream: stream.clone(),
            consumer: consumer.clone(),
        };
        self.append_entry_to_disk(&entry)?;
        self.inner
            .create_consumer(tenant, namespace, stream, consumer)
    }

    pub fn publish(
        &mut self,
        tenant: &TenantId,
        namespace: &NamespaceName,
        stream: &StreamName,
        subject: Subject,
        payload: impl Into<Vec<u8>>,
        transaction_id: TransactionId,
    ) -> Result<StreamRecord> {
        let record = StreamRecord {
            sequence: self.inner.stream(tenant, namespace, stream)?.next_sequence,
            subject,
            payload: payload.into(),
            transaction_id,
        };
        let entry = StreamJournalEntry::Publish {
            tenant: tenant.clone(),
            namespace: namespace.clone(),
            stream: stream.clone(),
            record: record.clone(),
        };
        self.append_entry_to_disk(&entry)?;
        self.inner
            .insert_record(tenant, namespace, stream, record.clone())?;
        Ok(record)
    }

    pub fn replay(
        &self,
        tenant: &TenantId,
        namespace: &NamespaceName,
        stream: &StreamName,
        after: Option<StreamSequence>,
    ) -> Result<Vec<StreamRecord>> {
        self.inner.replay(tenant, namespace, stream, after)
    }

    pub fn replay_matching(
        &self,
        tenant: &TenantId,
        namespace: &NamespaceName,
        stream: &StreamName,
        subject_filter: &Subject,
        after: Option<StreamSequence>,
    ) -> Result<Vec<StreamRecord>> {
        self.inner
            .replay_matching(tenant, namespace, stream, subject_filter, after)
    }

    pub fn replay_for_consumer(
        &self,
        tenant: &TenantId,
        namespace: &NamespaceName,
        stream: &StreamName,
        consumer: &ConsumerName,
    ) -> Result<Vec<StreamRecord>> {
        self.inner
            .replay_for_consumer(tenant, namespace, stream, consumer)
    }

    pub fn replay_matching_for_consumer(
        &self,
        tenant: &TenantId,
        namespace: &NamespaceName,
        stream: &StreamName,
        consumer: &ConsumerName,
        subject_filter: &Subject,
    ) -> Result<Vec<StreamRecord>> {
        self.inner
            .replay_matching_for_consumer(tenant, namespace, stream, consumer, subject_filter)
    }

    pub fn ack(
        &mut self,
        tenant: &TenantId,
        namespace: &NamespaceName,
        stream: &StreamName,
        consumer: &ConsumerName,
        sequence: StreamSequence,
    ) -> Result<DurableConsumer> {
        self.inner
            .validate_ack(tenant, namespace, stream, consumer, sequence)?;
        let entry = StreamJournalEntry::Ack {
            tenant: tenant.clone(),
            namespace: namespace.clone(),
            stream: stream.clone(),
            consumer: consumer.clone(),
            sequence,
        };
        self.append_entry_to_disk(&entry)?;
        self.inner
            .ack(tenant, namespace, stream, consumer, sequence)
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    fn append_entry_to_disk(&self, entry: &StreamJournalEntry) -> Result<()> {
        if let Some(parent) = self.path.parent() {
            fs::create_dir_all(parent).map_err(|err| EhdbError::Storage(err.to_string()))?;
        }

        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)
            .map_err(|err| EhdbError::Storage(err.to_string()))?;
        serde_json::to_writer(&mut file, entry)
            .map_err(|err| EhdbError::Storage(err.to_string()))?;
        file.write_all(b"\n")
            .map_err(|err| EhdbError::Storage(err.to_string()))?;
        file.sync_data()
            .map_err(|err| EhdbError::Storage(err.to_string()))?;
        Ok(())
    }
}

fn apply_journal_entry(inner: &mut InMemoryStreamLog, entry: StreamJournalEntry) -> Result<()> {
    match entry {
        StreamJournalEntry::CreateStream { config } => inner.create_stream(config),
        StreamJournalEntry::CreateConsumer {
            tenant,
            namespace,
            stream,
            consumer,
        } => inner
            .create_consumer(&tenant, &namespace, &stream, consumer)
            .map(|_| ()),
        StreamJournalEntry::Publish {
            tenant,
            namespace,
            stream,
            record,
        } => inner.insert_record(&tenant, &namespace, &stream, record),
        StreamJournalEntry::Ack {
            tenant,
            namespace,
            stream,
            consumer,
            sequence,
        } => inner
            .ack(&tenant, &namespace, &stream, &consumer, sequence)
            .map(|_| ()),
    }
}

#[cfg(test)]
mod tests {
    use std::{
        fs,
        time::{SystemTime, UNIX_EPOCH},
    };

    use super::*;

    fn ids() -> (TenantId, NamespaceName, StreamName) {
        (
            TenantId::new("tenant-a").unwrap(),
            NamespaceName::new("system").unwrap(),
            StreamName::new("execution-events").unwrap(),
        )
    }

    fn temp_log_path(test_name: &str) -> PathBuf {
        let suffix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!(
            "ehdb-stream-{test_name}-{}-{suffix}.jsonl",
            std::process::id()
        ))
    }

    #[test]
    fn publishes_and_replays_noetl_subjects() {
        let (tenant, namespace, stream) = ids();
        let mut log = InMemoryStreamLog::default();
        log.create_stream(StreamConfig {
            tenant: tenant.clone(),
            namespace: namespace.clone(),
            name: stream.clone(),
            retention: RetentionPolicy::KeepAll,
        })
        .unwrap();

        let first = log
            .publish(
                &tenant,
                &namespace,
                &stream,
                Subject::new("noetl.execution.command.completed").unwrap(),
                b"{\"execution_id\":\"exec-1\"}".to_vec(),
                TransactionId::new("txn-0001").unwrap(),
            )
            .unwrap();
        let second = log
            .publish(
                &tenant,
                &namespace,
                &stream,
                Subject::new("noetl.execution.playbook.completed").unwrap(),
                b"{\"execution_id\":\"exec-1\"}".to_vec(),
                TransactionId::new("txn-0002").unwrap(),
            )
            .unwrap();

        assert_eq!(first.sequence.value(), 1);
        assert_eq!(second.sequence.value(), 2);
        assert_eq!(
            log.replay(&tenant, &namespace, &stream, Some(first.sequence))
                .unwrap(),
            vec![second]
        );
    }

    #[test]
    fn subject_filters_match_exact_single_token_and_tail_wildcards() {
        let command = Subject::new("noetl.execution.command.completed").unwrap();
        let command_prefix = Subject::new("noetl.execution.command").unwrap();
        let playbook = Subject::new("noetl.execution.playbook.completed").unwrap();
        let exact = Subject::new("noetl.execution.command.completed").unwrap();
        let single_token = Subject::new("noetl.execution.*.completed").unwrap();
        let tail = Subject::new("noetl.execution.>").unwrap();
        let zero_tail = Subject::new("noetl.execution.command.>").unwrap();
        let misplaced_tail = Subject::new("noetl.>.completed").unwrap();

        assert!(exact.matches(&command));
        assert!(!exact.matches(&playbook));
        assert!(single_token.matches(&command));
        assert!(single_token.matches(&playbook));
        assert!(tail.matches(&command));
        assert!(tail.matches(&playbook));
        assert!(zero_tail.matches(&command));
        assert!(!zero_tail.matches(&command_prefix));
        assert!(!misplaced_tail.matches(&command));
    }

    #[test]
    fn replays_records_matching_subject_filter_after_cursor() {
        let (tenant, namespace, stream) = ids();
        let mut log = InMemoryStreamLog::default();
        log.create_stream(StreamConfig {
            tenant: tenant.clone(),
            namespace: namespace.clone(),
            name: stream.clone(),
            retention: RetentionPolicy::KeepAll,
        })
        .unwrap();

        let first_command = log
            .publish(
                &tenant,
                &namespace,
                &stream,
                Subject::new("noetl.execution.command.started").unwrap(),
                b"command-started".to_vec(),
                TransactionId::new("txn-0001").unwrap(),
            )
            .unwrap();
        let playbook = log
            .publish(
                &tenant,
                &namespace,
                &stream,
                Subject::new("noetl.execution.playbook.completed").unwrap(),
                b"playbook-completed".to_vec(),
                TransactionId::new("txn-0002").unwrap(),
            )
            .unwrap();
        let second_command = log
            .publish(
                &tenant,
                &namespace,
                &stream,
                Subject::new("noetl.execution.command.completed").unwrap(),
                b"command-completed".to_vec(),
                TransactionId::new("txn-0003").unwrap(),
            )
            .unwrap();

        let command_filter = Subject::new("noetl.execution.command.*").unwrap();
        let execution_filter = Subject::new("noetl.execution.>").unwrap();

        assert_eq!(
            log.replay_matching(&tenant, &namespace, &stream, &command_filter, None)
                .unwrap(),
            vec![first_command.clone(), second_command.clone()]
        );
        assert_eq!(
            log.replay_matching(
                &tenant,
                &namespace,
                &stream,
                &execution_filter,
                Some(first_command.sequence),
            )
            .unwrap(),
            vec![playbook, second_command]
        );
    }

    #[test]
    fn durable_consumer_resumes_after_ack() {
        let (tenant, namespace, stream) = ids();
        let consumer = ConsumerName::new("materializer").unwrap();
        let mut log = InMemoryStreamLog::default();
        log.create_stream(StreamConfig {
            tenant: tenant.clone(),
            namespace: namespace.clone(),
            name: stream.clone(),
            retention: RetentionPolicy::KeepAll,
        })
        .unwrap();
        log.create_consumer(&tenant, &namespace, &stream, consumer.clone())
            .unwrap();

        let first = log
            .publish(
                &tenant,
                &namespace,
                &stream,
                Subject::new("noetl.command").unwrap(),
                b"command-1".to_vec(),
                TransactionId::new("txn-0001").unwrap(),
            )
            .unwrap();
        let second = log
            .publish(
                &tenant,
                &namespace,
                &stream,
                Subject::new("noetl.event").unwrap(),
                b"event-1".to_vec(),
                TransactionId::new("txn-0002").unwrap(),
            )
            .unwrap();

        log.ack(&tenant, &namespace, &stream, &consumer, first.sequence)
            .unwrap();

        assert_eq!(
            log.replay_for_consumer(&tenant, &namespace, &stream, &consumer)
                .unwrap(),
            vec![second]
        );
    }

    #[test]
    fn durable_consumer_replays_matching_subjects_after_ack() {
        let (tenant, namespace, stream) = ids();
        let consumer = ConsumerName::new("materializer").unwrap();
        let missing_consumer = ConsumerName::new("missing-materializer").unwrap();
        let mut log = InMemoryStreamLog::default();
        log.create_stream(StreamConfig {
            tenant: tenant.clone(),
            namespace: namespace.clone(),
            name: stream.clone(),
            retention: RetentionPolicy::KeepAll,
        })
        .unwrap();
        log.create_consumer(&tenant, &namespace, &stream, consumer.clone())
            .unwrap();

        let first_command = log
            .publish(
                &tenant,
                &namespace,
                &stream,
                Subject::new("noetl.execution.command.started").unwrap(),
                b"command-started".to_vec(),
                TransactionId::new("txn-0001").unwrap(),
            )
            .unwrap();
        log.publish(
            &tenant,
            &namespace,
            &stream,
            Subject::new("noetl.execution.playbook.completed").unwrap(),
            b"playbook-completed".to_vec(),
            TransactionId::new("txn-0002").unwrap(),
        )
        .unwrap();
        let second_command = log
            .publish(
                &tenant,
                &namespace,
                &stream,
                Subject::new("noetl.execution.command.completed").unwrap(),
                b"command-completed".to_vec(),
                TransactionId::new("txn-0003").unwrap(),
            )
            .unwrap();
        log.ack(
            &tenant,
            &namespace,
            &stream,
            &consumer,
            first_command.sequence,
        )
        .unwrap();

        let command_filter = Subject::new("noetl.execution.command.*").unwrap();
        let matched = log
            .replay_matching_for_consumer(&tenant, &namespace, &stream, &consumer, &command_filter)
            .unwrap();

        assert_eq!(matched, vec![second_command.clone()]);
        assert_eq!(
            log.replay_for_consumer(&tenant, &namespace, &stream, &consumer)
                .unwrap()
                .len(),
            2
        );
        assert!(matches!(
            log.replay_matching_for_consumer(
                &tenant,
                &namespace,
                &stream,
                &missing_consumer,
                &command_filter,
            )
            .unwrap_err(),
            EhdbError::NotFound(_)
        ));
    }

    #[test]
    fn retention_limits_records() {
        let (tenant, namespace, stream) = ids();
        let mut log = InMemoryStreamLog::default();
        log.create_stream(StreamConfig {
            tenant: tenant.clone(),
            namespace: namespace.clone(),
            name: stream.clone(),
            retention: RetentionPolicy::MaxRecords(1),
        })
        .unwrap();

        log.publish(
            &tenant,
            &namespace,
            &stream,
            Subject::new("noetl.event").unwrap(),
            b"first".to_vec(),
            TransactionId::new("txn-0001").unwrap(),
        )
        .unwrap();
        let retained = log
            .publish(
                &tenant,
                &namespace,
                &stream,
                Subject::new("noetl.event").unwrap(),
                b"second".to_vec(),
                TransactionId::new("txn-0002").unwrap(),
            )
            .unwrap();

        assert_eq!(
            log.replay(&tenant, &namespace, &stream, None).unwrap(),
            vec![retained]
        );
    }

    #[test]
    fn rejects_zero_max_record_retention() {
        let (tenant, namespace, stream) = ids();
        let mut log = InMemoryStreamLog::default();
        let error = log
            .create_stream(StreamConfig {
                tenant,
                namespace,
                name: stream,
                retention: RetentionPolicy::MaxRecords(0),
            })
            .unwrap_err();

        assert!(matches!(error, EhdbError::InvalidState(_)));
    }

    #[test]
    fn rejects_duplicate_stream_and_consumer() {
        let (tenant, namespace, stream) = ids();
        let consumer = ConsumerName::new("materializer").unwrap();
        let mut log = InMemoryStreamLog::default();
        let config = StreamConfig {
            tenant: tenant.clone(),
            namespace: namespace.clone(),
            name: stream.clone(),
            retention: RetentionPolicy::KeepAll,
        };

        log.create_stream(config.clone()).unwrap();
        assert!(matches!(
            log.create_stream(config).unwrap_err(),
            EhdbError::AlreadyExists(_)
        ));

        log.create_consumer(&tenant, &namespace, &stream, consumer.clone())
            .unwrap();
        assert!(matches!(
            log.create_consumer(&tenant, &namespace, &stream, consumer)
                .unwrap_err(),
            EhdbError::AlreadyExists(_)
        ));
    }

    #[test]
    fn ack_cannot_move_consumer_cursor_backwards() {
        let (tenant, namespace, stream) = ids();
        let consumer = ConsumerName::new("materializer").unwrap();
        let mut log = InMemoryStreamLog::default();
        log.create_stream(StreamConfig {
            tenant: tenant.clone(),
            namespace: namespace.clone(),
            name: stream.clone(),
            retention: RetentionPolicy::KeepAll,
        })
        .unwrap();
        log.create_consumer(&tenant, &namespace, &stream, consumer.clone())
            .unwrap();

        let first = log
            .publish(
                &tenant,
                &namespace,
                &stream,
                Subject::new("noetl.event").unwrap(),
                b"first".to_vec(),
                TransactionId::new("txn-0001").unwrap(),
            )
            .unwrap();
        let second = log
            .publish(
                &tenant,
                &namespace,
                &stream,
                Subject::new("noetl.event").unwrap(),
                b"second".to_vec(),
                TransactionId::new("txn-0002").unwrap(),
            )
            .unwrap();

        log.ack(&tenant, &namespace, &stream, &consumer, second.sequence)
            .unwrap();
        let error = log
            .ack(&tenant, &namespace, &stream, &consumer, first.sequence)
            .unwrap_err();

        assert!(matches!(error, EhdbError::InvalidState(_)));
    }

    #[test]
    fn rejects_invalid_subjects() {
        assert!(Subject::new("noetl.event").is_ok());
        assert!(Subject::new("noetl event").is_err());
        assert!(Subject::new("").is_err());
        assert!(StreamSequence::new(0).is_err());
        assert_eq!(StreamSequence::new(1).unwrap(), StreamSequence::first());
    }

    #[test]
    fn local_jsonl_log_replays_records_and_consumer_cursor_after_reopen() {
        let path = temp_log_path("restart");
        let (tenant, namespace, stream) = ids();
        let consumer = ConsumerName::new("materializer").unwrap();
        let mut log = LocalJsonlStreamLog::open(&path).unwrap();

        log.create_stream(StreamConfig {
            tenant: tenant.clone(),
            namespace: namespace.clone(),
            name: stream.clone(),
            retention: RetentionPolicy::KeepAll,
        })
        .unwrap();
        log.create_consumer(&tenant, &namespace, &stream, consumer.clone())
            .unwrap();
        let first = log
            .publish(
                &tenant,
                &namespace,
                &stream,
                Subject::new("noetl.command").unwrap(),
                b"command-1".to_vec(),
                TransactionId::new("txn-0001").unwrap(),
            )
            .unwrap();
        let second = log
            .publish(
                &tenant,
                &namespace,
                &stream,
                Subject::new("noetl.event").unwrap(),
                b"event-1".to_vec(),
                TransactionId::new("txn-0002").unwrap(),
            )
            .unwrap();
        log.ack(&tenant, &namespace, &stream, &consumer, first.sequence)
            .unwrap();
        drop(log);

        let mut reopened = LocalJsonlStreamLog::open(&path).unwrap();
        assert_eq!(
            reopened
                .replay_for_consumer(&tenant, &namespace, &stream, &consumer)
                .unwrap(),
            vec![second]
        );
        assert_eq!(reopened.path(), path.as_path());

        let third = reopened
            .publish(
                &tenant,
                &namespace,
                &stream,
                Subject::new("noetl.event").unwrap(),
                b"event-2".to_vec(),
                TransactionId::new("txn-0003").unwrap(),
            )
            .unwrap();
        assert_eq!(third.sequence.value(), 3);

        fs::remove_file(path).unwrap();
    }

    #[test]
    fn local_jsonl_log_replays_matching_consumer_subjects_after_reopen() {
        let path = temp_log_path("consumer-subject-filter-replay");
        let (tenant, namespace, stream) = ids();
        let consumer = ConsumerName::new("materializer").unwrap();
        let mut log = LocalJsonlStreamLog::open(&path).unwrap();

        log.create_stream(StreamConfig {
            tenant: tenant.clone(),
            namespace: namespace.clone(),
            name: stream.clone(),
            retention: RetentionPolicy::KeepAll,
        })
        .unwrap();
        log.create_consumer(&tenant, &namespace, &stream, consumer.clone())
            .unwrap();
        let first_command = log
            .publish(
                &tenant,
                &namespace,
                &stream,
                Subject::new("noetl.execution.command.started").unwrap(),
                b"command-started".to_vec(),
                TransactionId::new("txn-0001").unwrap(),
            )
            .unwrap();
        log.publish(
            &tenant,
            &namespace,
            &stream,
            Subject::new("noetl.execution.playbook.completed").unwrap(),
            b"playbook-completed".to_vec(),
            TransactionId::new("txn-0002").unwrap(),
        )
        .unwrap();
        let second_command = log
            .publish(
                &tenant,
                &namespace,
                &stream,
                Subject::new("noetl.execution.command.completed").unwrap(),
                b"command-completed".to_vec(),
                TransactionId::new("txn-0003").unwrap(),
            )
            .unwrap();
        log.ack(
            &tenant,
            &namespace,
            &stream,
            &consumer,
            first_command.sequence,
        )
        .unwrap();
        drop(log);

        let reopened = LocalJsonlStreamLog::open(&path).unwrap();
        let command_filter = Subject::new("noetl.execution.command.*").unwrap();

        assert_eq!(
            reopened
                .replay_matching_for_consumer(
                    &tenant,
                    &namespace,
                    &stream,
                    &consumer,
                    &command_filter,
                )
                .unwrap(),
            vec![second_command]
        );

        fs::remove_file(path).unwrap();
    }

    #[test]
    fn local_jsonl_log_replays_records_matching_subject_filter_after_reopen() {
        let path = temp_log_path("subject-filter-replay");
        let (tenant, namespace, stream) = ids();
        let mut log = LocalJsonlStreamLog::open(&path).unwrap();

        log.create_stream(StreamConfig {
            tenant: tenant.clone(),
            namespace: namespace.clone(),
            name: stream.clone(),
            retention: RetentionPolicy::KeepAll,
        })
        .unwrap();
        let first_command = log
            .publish(
                &tenant,
                &namespace,
                &stream,
                Subject::new("noetl.execution.command.started").unwrap(),
                b"command-started".to_vec(),
                TransactionId::new("txn-0001").unwrap(),
            )
            .unwrap();
        log.publish(
            &tenant,
            &namespace,
            &stream,
            Subject::new("noetl.execution.playbook.completed").unwrap(),
            b"playbook-completed".to_vec(),
            TransactionId::new("txn-0002").unwrap(),
        )
        .unwrap();
        let second_command = log
            .publish(
                &tenant,
                &namespace,
                &stream,
                Subject::new("noetl.execution.command.completed").unwrap(),
                b"command-completed".to_vec(),
                TransactionId::new("txn-0003").unwrap(),
            )
            .unwrap();
        drop(log);

        let reopened = LocalJsonlStreamLog::open(&path).unwrap();
        let command_filter = Subject::new("noetl.execution.command.*").unwrap();

        assert_eq!(
            reopened
                .replay_matching(
                    &tenant,
                    &namespace,
                    &stream,
                    &command_filter,
                    Some(first_command.sequence),
                )
                .unwrap(),
            vec![second_command]
        );

        fs::remove_file(path).unwrap();
    }

    #[test]
    fn local_jsonl_log_replays_retention_and_next_sequence_after_reopen() {
        let path = temp_log_path("retention");
        let (tenant, namespace, stream) = ids();
        let mut log = LocalJsonlStreamLog::open(&path).unwrap();

        log.create_stream(StreamConfig {
            tenant: tenant.clone(),
            namespace: namespace.clone(),
            name: stream.clone(),
            retention: RetentionPolicy::MaxRecords(1),
        })
        .unwrap();
        log.publish(
            &tenant,
            &namespace,
            &stream,
            Subject::new("noetl.event").unwrap(),
            b"first".to_vec(),
            TransactionId::new("txn-0001").unwrap(),
        )
        .unwrap();
        let retained = log
            .publish(
                &tenant,
                &namespace,
                &stream,
                Subject::new("noetl.event").unwrap(),
                b"second".to_vec(),
                TransactionId::new("txn-0002").unwrap(),
            )
            .unwrap();
        drop(log);

        let mut reopened = LocalJsonlStreamLog::open(&path).unwrap();
        assert_eq!(
            reopened.replay(&tenant, &namespace, &stream, None).unwrap(),
            vec![retained]
        );
        let third = reopened
            .publish(
                &tenant,
                &namespace,
                &stream,
                Subject::new("noetl.event").unwrap(),
                b"third".to_vec(),
                TransactionId::new("txn-0003").unwrap(),
            )
            .unwrap();

        assert_eq!(third.sequence.value(), 3);
        assert_eq!(
            reopened.replay(&tenant, &namespace, &stream, None).unwrap(),
            vec![third]
        );

        fs::remove_file(path).unwrap();
    }

    #[test]
    fn local_jsonl_log_rejects_zero_max_record_retention_before_journal_write() {
        let path = temp_log_path("zero-retention");
        let (tenant, namespace, stream) = ids();
        let mut log = LocalJsonlStreamLog::open(&path).unwrap();

        let error = log
            .create_stream(StreamConfig {
                tenant: tenant.clone(),
                namespace: namespace.clone(),
                name: stream.clone(),
                retention: RetentionPolicy::MaxRecords(0),
            })
            .unwrap_err();

        assert!(matches!(error, EhdbError::InvalidState(_)));
        assert!(!path.exists());
        drop(log);

        let mut reopened = LocalJsonlStreamLog::open(&path).unwrap();
        reopened
            .create_stream(StreamConfig {
                tenant,
                namespace,
                name: stream,
                retention: RetentionPolicy::KeepAll,
            })
            .unwrap();

        fs::remove_file(path).unwrap();
    }

    #[test]
    fn local_jsonl_log_rejects_corrupt_entries_on_open() {
        let path = temp_log_path("corrupt");
        fs::write(&path, b"not-json\n").unwrap();

        let error = LocalJsonlStreamLog::open(&path).unwrap_err();

        assert!(matches!(error, EhdbError::Storage(_)));

        fs::remove_file(path).unwrap();
    }
}
