use criterion::{black_box, criterion_group, criterion_main, Criterion};
use ehdb_core::{NamespaceName, StreamName, TenantId, TransactionId};
use ehdb_reference::{ExecuteReplication, LocalReferenceRuntime, LocalReplicationExecutor};
use ehdb_storage::{
    plan_replication, CloudProvider, DataGravityShard, GeoLocation, ImmutableObjectStore,
    LocalObjectStore, ObjectPath, ObjectPlacement, PlacementPolicy, PlacementTarget,
};
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

fn bench_local_replication_executor(c: &mut Criterion) {
    let mut group = c.benchmark_group("local_replication_executor");
    group.sample_size(10);
    group.bench_function("register_25", |b| {
        b.iter(|| {
            let tenant = TenantId::new("tenant-a").unwrap();
            let namespace = NamespaceName::new("system").unwrap();
            let log_path = temp_log_path("local-replication-executor");
            let object_root = temp_object_root("local-replication-executor");
            let store = LocalObjectStore::new(&object_root);
            let mut runtime = LocalReferenceRuntime::open(&log_path).unwrap();
            let policy = local_plus_gcp_policy();

            for index in 0..25 {
                let source = store
                    .put_if_absent(
                        ObjectPath::new(format!("tenant-a/system/table/part-{index}.arrow"))
                            .unwrap(),
                        black_box(format!("payload-{index}").as_bytes()),
                    )
                    .unwrap();
                let plan = plan_replication(&source, &[], &policy).unwrap();
                LocalReplicationExecutor
                    .execute(
                        &mut runtime,
                        &store,
                        ExecuteReplication {
                            tenant: tenant.clone(),
                            namespace: namespace.clone(),
                            transaction_id: TransactionId::new(format!("txn-replicate-{index}"))
                                .unwrap(),
                            source,
                            plan,
                        },
                    )
                    .unwrap();
            }
            drop(runtime);

            let reopened = LocalReferenceRuntime::open(&log_path).unwrap();
            black_box(reopened.state().storage.replica_count());
            black_box(reopened.replay());

            std::fs::remove_file(log_path).unwrap();
            std::fs::remove_dir_all(object_root).unwrap();
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

fn temp_object_root(name: &str) -> std::path::PathBuf {
    let suffix = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let counter = TEMP_COUNTER.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir().join(format!(
        "ehdb-bench-objects-{name}-{}-{suffix}-{counter}",
        std::process::id()
    ))
}

fn local_plus_gcp_policy() -> PlacementPolicy {
    PlacementPolicy::new(
        2,
        vec![
            PlacementTarget::primary(ObjectPlacement::local_dev()),
            PlacementTarget::replica(ObjectPlacement::new(
                GeoLocation::new(CloudProvider::Gcp, "us-central1", Some("us-central1-a")).unwrap(),
                DataGravityShard::local_dev(),
            )),
        ],
    )
    .unwrap()
}

criterion_group!(
    benches,
    bench_local_reference_runtime_append_reopen,
    bench_local_replication_executor
);
criterion_main!(benches);
