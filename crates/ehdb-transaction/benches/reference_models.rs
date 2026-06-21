use criterion::{black_box, criterion_group, criterion_main, Criterion};
use ehdb_core::{NamespaceName, StreamName, TenantId, TransactionId};
use ehdb_stream::{InMemoryStreamLog, LocalJsonlStreamLog, RetentionPolicy, StreamConfig, Subject};
use ehdb_transaction::{
    CommitTransaction, InMemoryTransactionLog, LocalJsonlTransactionLog, Mutation, StreamMutation,
};
use std::time::{SystemTime, UNIX_EPOCH};

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

fn bench_local_transaction_jsonl_append_reopen(c: &mut Criterion) {
    let mut group = c.benchmark_group("local_transaction_jsonl");
    group.sample_size(10);
    group.bench_function("append_reopen_100", |b| {
        b.iter(|| {
            let tenant = TenantId::new("tenant-a").unwrap();
            let namespace = NamespaceName::new("system").unwrap();
            let stream = StreamName::new("execution-events").unwrap();
            let path = temp_log_path("local-transaction-jsonl");
            let mut log = LocalJsonlTransactionLog::open(&path).unwrap();

            for index in 0..100 {
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
            drop(log);

            let reopened = LocalJsonlTransactionLog::open(&path).unwrap();
            black_box(reopened.replay(None));

            std::fs::remove_file(path).unwrap();
        })
    });
    group.finish();
}

fn bench_local_stream_jsonl_publish_reopen(c: &mut Criterion) {
    let mut group = c.benchmark_group("local_stream_jsonl");
    group.sample_size(10);
    group.bench_function("publish_reopen_100", |b| {
        b.iter(|| {
            let tenant = TenantId::new("tenant-a").unwrap();
            let namespace = NamespaceName::new("system").unwrap();
            let stream = StreamName::new("execution-events").unwrap();
            let path = temp_log_path("local-stream-jsonl");
            let mut log = LocalJsonlStreamLog::open(&path).unwrap();
            log.create_stream(StreamConfig {
                tenant: tenant.clone(),
                namespace: namespace.clone(),
                name: stream.clone(),
                retention: RetentionPolicy::KeepAll,
            })
            .unwrap();

            for index in 0..100 {
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
            drop(log);

            let reopened = LocalJsonlStreamLog::open(&path).unwrap();
            black_box(reopened.replay(&tenant, &namespace, &stream, None).unwrap());

            std::fs::remove_file(path).unwrap();
        })
    });
    group.finish();
}

fn temp_log_path(name: &str) -> std::path::PathBuf {
    let suffix = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!(
        "ehdb-bench-{name}-{}-{suffix}.jsonl",
        std::process::id()
    ))
}

criterion_group!(
    benches,
    bench_stream_publish_replay,
    bench_transaction_append_replay,
    bench_local_transaction_jsonl_append_reopen,
    bench_local_stream_jsonl_publish_reopen
);
criterion_main!(benches);
