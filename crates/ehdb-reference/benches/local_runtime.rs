use criterion::{black_box, criterion_group, criterion_main, Criterion};
use std::{
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::{SystemTime, UNIX_EPOCH},
};

use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use ehdb_core::{NamespaceName, SnapshotId, StreamName, TableName, TenantId, TransactionId};
use ehdb_reference::{
    ExecuteReplication, LocalArrowIpcTableStore, LocalReferenceRuntime, LocalReplicationExecutor,
    WriteArrowIpcTable,
};
use ehdb_storage::{
    plan_replication, CloudProvider, DataGravityShard, GeoLocation, ImmutableObjectStore,
    LocalObjectStore, ObjectPath, ObjectPlacement, PlacementPolicy, PlacementTarget,
};
use ehdb_stream::{RetentionPolicy, Subject};
use ehdb_transaction::{CommitTransaction, Mutation, StreamMutation};

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

fn bench_local_arrow_ipc_table(c: &mut Criterion) {
    let mut group = c.benchmark_group("local_arrow_ipc_table");
    group.sample_size(10);
    group.bench_function("write_read_10", |b| {
        b.iter(|| {
            let tenant = TenantId::new("tenant-a").unwrap();
            let namespace = NamespaceName::new("system").unwrap();
            let log_path = temp_log_path("local-arrow-ipc-table");
            let object_root = temp_object_root("local-arrow-ipc-table");
            let store = LocalObjectStore::new(&object_root);
            let mut runtime = LocalReferenceRuntime::open(&log_path).unwrap();

            for index in 0..10 {
                LocalArrowIpcTableStore
                    .write_batch(
                        &mut runtime,
                        &store,
                        WriteArrowIpcTable {
                            tenant: tenant.clone(),
                            namespace: namespace.clone(),
                            table_name: TableName::new(format!("executions-{index}")).unwrap(),
                            snapshot_id: SnapshotId::new("snapshot-0001").unwrap(),
                            create_transaction_id: TransactionId::new(format!(
                                "txn-create-table-{index}"
                            ))
                            .unwrap(),
                            snapshot_transaction_id: TransactionId::new(format!(
                                "txn-commit-snapshot-{index}"
                            ))
                            .unwrap(),
                            file_name: "part-000.arrow".to_string(),
                            batch: black_box(arrow_batch()),
                        },
                    )
                    .unwrap();
                black_box(
                    LocalArrowIpcTableStore
                        .read_latest(
                            &runtime,
                            &store,
                            &tenant,
                            &namespace,
                            &TableName::new(format!("executions-{index}")).unwrap(),
                        )
                        .unwrap(),
                );
            }
            drop(runtime);

            let reopened = LocalReferenceRuntime::open(&log_path).unwrap();
            black_box(reopened.state().catalog.snapshot_count());
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

fn arrow_batch() -> RecordBatch {
    let schema = Arc::new(Schema::new(vec![
        Field::new("execution_id", DataType::Utf8, false),
        Field::new("attempt", DataType::Int64, false),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec!["exec-1", "exec-2", "exec-3"])),
            Arc::new(Int64Array::from(vec![1, 2, 3])),
        ],
    )
    .unwrap()
}

criterion_group!(
    benches,
    bench_local_reference_runtime_append_reopen,
    bench_local_replication_executor,
    bench_local_arrow_ipc_table
);
criterion_main!(benches);
