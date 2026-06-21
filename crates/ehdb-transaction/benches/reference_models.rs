use criterion::{black_box, criterion_group, criterion_main, Criterion};
use ehdb_core::{NamespaceName, StreamName, TenantId, TransactionId};
use ehdb_stream::{InMemoryStreamLog, RetentionPolicy, StreamConfig, Subject};
use ehdb_transaction::{CommitTransaction, InMemoryTransactionLog, Mutation, StreamMutation};

fn bench_stream_publish_replay(c: &mut Criterion) {
    c.bench_function("stream_publish_replay_1000", |b| {
        b.iter(|| {
            let tenant = TenantId::new("tenant-a").unwrap();
            let namespace = NamespaceName::new("system").unwrap();
            let stream = StreamName::new("execution-events").unwrap();
            let mut log = InMemoryStreamLog::default();
            log.create_stream(StreamConfig {
                tenant: tenant.clone(),
                namespace: namespace.clone(),
                name: stream.clone(),
                retention: RetentionPolicy::KeepAll,
            })
            .unwrap();

            for index in 0..1000 {
                log.publish(
                    &tenant,
                    &namespace,
                    &stream,
                    Subject::new("noetl.event").unwrap(),
                    black_box(format!("event-{index}").into_bytes()),
                    TransactionId::new(format!("txn-{index}")).unwrap(),
                )
                .unwrap();
            }

            black_box(log.replay(&tenant, &namespace, &stream, None).unwrap());
        })
    });
}

fn bench_transaction_append_replay(c: &mut Criterion) {
    c.bench_function("transaction_append_replay_1000", |b| {
        b.iter(|| {
            let tenant = TenantId::new("tenant-a").unwrap();
            let namespace = NamespaceName::new("system").unwrap();
            let stream = StreamName::new("execution-events").unwrap();
            let mut log = InMemoryTransactionLog::default();

            for index in 0..1000 {
                log.append(CommitTransaction {
                    transaction_id: TransactionId::new(format!("txn-{index}")).unwrap(),
                    tenant: tenant.clone(),
                    namespace: namespace.clone(),
                    mutations: vec![Mutation::Stream(StreamMutation::Publish {
                        stream: stream.clone(),
                        subject: "noetl.event".to_string(),
                        sequence: index + 1,
                    })],
                })
                .unwrap();
            }

            black_box(log.replay(None));
        })
    });
}

criterion_group!(
    benches,
    bench_stream_publish_replay,
    bench_transaction_append_replay
);
criterion_main!(benches);
