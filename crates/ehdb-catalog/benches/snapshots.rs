use criterion::{black_box, criterion_group, criterion_main, Criterion};
use ehdb_catalog::{CommitSnapshot, CreateTable, InMemoryCatalog};
use ehdb_core::{
    ColumnSchema, DataType, NamespaceName, SnapshotId, TableName, TableSchema, TenantId,
    TransactionId,
};
use ehdb_storage::{ObjectDigest, ObjectPath, ObjectRef};

fn bench_catalog_snapshot_commits(c: &mut Criterion) {
    c.bench_function("catalog_commit_snapshots_1000", |b| {
        b.iter(|| {
            let tenant = TenantId::new("tenant-a").unwrap();
            let namespace = NamespaceName::new("system").unwrap();
            let mut catalog = InMemoryCatalog::default();
            let table = catalog
                .create_table(CreateTable {
                    tenant: tenant.clone(),
                    namespace: namespace.clone(),
                    name: TableName::new("executions").unwrap(),
                    schema: TableSchema::new(vec![ColumnSchema::new(
                        "execution_id",
                        DataType::Utf8,
                        false,
                    )
                    .unwrap()])
                    .unwrap(),
                    transaction_id: TransactionId::new("txn-create").unwrap(),
                })
                .unwrap();
            let mut parent = None;

            for index in 0..1000 {
                let snapshot_id = SnapshotId::new(format!("snapshot-{index:04}")).unwrap();
                let snapshot = catalog
                    .commit_snapshot(CommitSnapshot {
                        tenant: tenant.clone(),
                        namespace: namespace.clone(),
                        table_id: table.id.clone(),
                        snapshot_id,
                        parent_snapshot: parent,
                        files: vec![object_ref(index)],
                        transaction_id: TransactionId::new(format!("txn-{index:04}")).unwrap(),
                    })
                    .unwrap();
                parent = Some(snapshot.id);
            }

            black_box(
                catalog
                    .latest_snapshot(&tenant, &namespace, &table.id)
                    .unwrap(),
            );
        })
    });
}

fn object_ref(index: usize) -> ObjectRef {
    ObjectRef {
        path: ObjectPath::new(format!(
            "tenant-a/system/tables/executions/snapshots/snapshot-{index:04}/part-000.arrow"
        ))
        .unwrap(),
        len: 4096,
        digest: ObjectDigest::new(format!("sha256:{}", "a".repeat(64))).unwrap(),
    }
}

criterion_group!(benches, bench_catalog_snapshot_commits);
criterion_main!(benches);
