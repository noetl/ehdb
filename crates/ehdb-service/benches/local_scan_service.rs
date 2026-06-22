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
use ehdb_core::{NamespaceName, SnapshotId, TableName, TenantId, TransactionId};
use ehdb_reference::{
    ArrowEqualityPredicate, ArrowScalarValue, LocalArrowIpcTableStore, LocalReferenceRuntime,
    WriteArrowIpcTable,
};
use ehdb_service::{LocalArrowScanService, ScanLatestTableRequest};
use ehdb_storage::LocalObjectStore;

static TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);

fn bench_local_scan_service(c: &mut Criterion) {
    let mut group = c.benchmark_group("local_arrow_scan_service");
    group.sample_size(10);
    group.bench_function("filter_project_latest_100", |b| {
        b.iter(|| {
            let tenant = TenantId::new("tenant-a").unwrap();
            let namespace = NamespaceName::new("system").unwrap();
            let table_name = TableName::new("executions").unwrap();
            let log_path = temp_log_path("local-arrow-scan-service");
            let object_root = temp_object_root("local-arrow-scan-service");
            let store = LocalObjectStore::new(&object_root);
            let mut runtime = LocalReferenceRuntime::open(&log_path).unwrap();

            LocalArrowIpcTableStore
                .write_batch(
                    &mut runtime,
                    &store,
                    WriteArrowIpcTable {
                        tenant: tenant.clone(),
                        namespace: namespace.clone(),
                        table_name: table_name.clone(),
                        snapshot_id: SnapshotId::new("snapshot-0001").unwrap(),
                        create_transaction_id: TransactionId::new("txn-create-table").unwrap(),
                        snapshot_transaction_id: TransactionId::new("txn-commit-snapshot").unwrap(),
                        file_name: "part-000.arrow".to_string(),
                        batch: arrow_batch(),
                    },
                )
                .unwrap();

            let service = LocalArrowScanService::default();
            for _ in 0..100 {
                black_box(
                    service
                        .scan_latest(
                            &runtime,
                            &store,
                            ScanLatestTableRequest {
                                tenant: tenant.clone(),
                                namespace: namespace.clone(),
                                table_name: table_name.clone(),
                                projection: Some(vec![
                                    "attempt".to_string(),
                                    "execution_id".to_string(),
                                ]),
                                predicate: Some(ArrowEqualityPredicate {
                                    column: "execution_id".to_string(),
                                    value: ArrowScalarValue::Utf8("exec-2".to_string()),
                                }),
                            },
                        )
                        .unwrap(),
                );
            }

            std::fs::remove_file(log_path).unwrap();
            std::fs::remove_dir_all(object_root).unwrap();
        })
    });
    group.finish();
}

fn temp_log_path(name: &str) -> std::path::PathBuf {
    let suffix = unique_suffix();
    std::env::temp_dir().join(format!("ehdb-bench-{name}-{suffix}.jsonl"))
}

fn temp_object_root(name: &str) -> std::path::PathBuf {
    let suffix = unique_suffix();
    std::env::temp_dir().join(format!("ehdb-bench-objects-{name}-{suffix}"))
}

fn unique_suffix() -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let counter = TEMP_COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("{}-{nanos}-{counter}", std::process::id())
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

criterion_group!(benches, bench_local_scan_service);
criterion_main!(benches);
