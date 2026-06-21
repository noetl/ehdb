use criterion::{black_box, criterion_group, criterion_main, Criterion};
use ehdb_core::{NamespaceName, StreamName, TenantId, TransactionId};
use ehdb_reference::LocalReferenceRuntime;
use ehdb_stream::{RetentionPolicy, Subject};
use ehdb_transaction::{CommitTransaction, Mutation, StreamMutation};
use std::{
    sync::atomic::{AtomicU64, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};

static TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);

fn bench_local_reference_runtime_append_reopen(c: &mut Criterion) {
    let mut group = c.benchmark_group("local_reference_runtime");
    group.sample_size(10);
    group.bench_function("append_reopen_100", |b| {
        b.iter(|| {
            let tenant = TenantId::new("tenant-a").unwrap();
            let namespace = NamespaceName::new("system").unwrap();
            let stream = StreamName::new("execution-events").unwrap();
            let path = temp_log_path("local-reference-runtime");
            let mut runtime = LocalReferenceRuntime::open(&path).unwrap();

            runtime
                .append(CommitTransaction {
                    transaction_id: TransactionId::new("txn-create-stream").unwrap(),
                    tenant: tenant.clone(),
                    namespace: namespace.clone(),
                    mutations: vec![Mutation::Stream(StreamMutation::CreateStream {
                        stream: stream.clone(),
                        retention: RetentionPolicy::KeepAll,
                    })],
                })
                .unwrap();

            for index in 0..100 {
                runtime
                    .append(CommitTransaction {
                        transaction_id: TransactionId::new(format!("txn-{index}")).unwrap(),
                        tenant: tenant.clone(),
                        namespace: namespace.clone(),
                        mutations: vec![Mutation::Stream(StreamMutation::Publish {
                            stream: stream.clone(),
                            subject: Subject::new("noetl.event").unwrap(),
                            payload: black_box(format!("event-{index}").into_bytes()),
                            sequence: index + 1,
                        })],
                    })
                    .unwrap();
            }
            drop(runtime);

            let reopened = LocalReferenceRuntime::open(&path).unwrap();
            black_box(
                reopened
                    .state()
                    .streams
                    .replay(&tenant, &namespace, &stream, None)
                    .unwrap(),
            );
            black_box(reopened.replay());

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
    let counter = TEMP_COUNTER.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir().join(format!(
        "ehdb-bench-{name}-{}-{suffix}-{counter}.jsonl",
        std::process::id()
    ))
}

criterion_group!(benches, bench_local_reference_runtime_append_reopen);
criterion_main!(benches);
