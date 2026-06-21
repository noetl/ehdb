use std::collections::BTreeMap;

use ehdb_core::{
    ConsumerName, EhdbError, NamespaceName, Result, StreamName, TenantId, TransactionId,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct StreamSequence(u64);

impl StreamSequence {
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

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
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
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RetentionPolicy {
    KeepAll,
    MaxRecords(usize),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamConfig {
    pub tenant: TenantId,
    pub namespace: NamespaceName,
    pub name: StreamName,
    pub retention: RetentionPolicy,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamRecord {
    pub sequence: StreamSequence,
    pub subject: Subject,
    pub payload: Vec<u8>,
    pub transaction_id: TransactionId,
}

#[derive(Debug, Clone, PartialEq, Eq)]
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

#[derive(Debug, Default)]
pub struct InMemoryStreamLog {
    streams: BTreeMap<StreamKey, StreamState>,
}

impl InMemoryStreamLog {
    pub fn create_stream(&mut self, config: StreamConfig) -> Result<()> {
        let key = StreamKey {
            tenant: config.tenant.clone(),
            namespace: config.namespace.clone(),
            name: config.name.clone(),
        };

        if self.streams.contains_key(&key) {
            return Err(EhdbError::AlreadyExists(format!(
                "{}.{}.{}",
                key.tenant, key.namespace, key.name
            )));
        }

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
        let state = self.stream_mut(tenant, namespace, stream)?;
        let record = StreamRecord {
            sequence: state.next_sequence,
            subject,
            payload: payload.into(),
            transaction_id,
        };

        state.records.insert(record.sequence, record.clone());
        state.next_sequence = state.next_sequence.next();
        state.enforce_retention();
        Ok(record)
    }

    pub fn create_consumer(
        &mut self,
        tenant: &TenantId,
        namespace: &NamespaceName,
        stream: &StreamName,
        consumer: ConsumerName,
    ) -> Result<DurableConsumer> {
        let state = self.stream_mut(tenant, namespace, stream)?;
        if state.consumers.contains_key(&consumer) {
            return Err(EhdbError::AlreadyExists(consumer.to_string()));
        }

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
        let state = self.stream(tenant, namespace, stream)?;
        Ok(state
            .records
            .iter()
            .filter(|(sequence, _)| after.map_or(true, |cursor| **sequence > cursor))
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

    pub fn ack(
        &mut self,
        tenant: &TenantId,
        namespace: &NamespaceName,
        stream: &StreamName,
        consumer: &ConsumerName,
        sequence: StreamSequence,
    ) -> Result<DurableConsumer> {
        let state = self.stream_mut(tenant, namespace, stream)?;
        if !state.records.contains_key(&sequence) {
            return Err(EhdbError::NotFound(format!(
                "stream sequence {}",
                sequence.value()
            )));
        }

        let consumer_state = state
            .consumers
            .get_mut(consumer)
            .ok_or_else(|| EhdbError::NotFound(consumer.to_string()))?;

        if consumer_state
            .acked_sequence
            .map_or(false, |acked| sequence < acked)
        {
            return Err(EhdbError::InvalidState(format!(
                "cannot move consumer {} cursor backwards from {} to {}",
                consumer,
                consumer_state.acked_sequence.unwrap().value(),
                sequence.value()
            )));
        }

        consumer_state.acked_sequence = Some(sequence);
        Ok(consumer_state.clone())
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

#[cfg(test)]
mod tests {
    use super::*;

    fn ids() -> (TenantId, NamespaceName, StreamName) {
        (
            TenantId::new("tenant-a").unwrap(),
            NamespaceName::new("system").unwrap(),
            StreamName::new("execution-events").unwrap(),
        )
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
}
