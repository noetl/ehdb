use criterion::{black_box, criterion_group, criterion_main, Criterion};
use ehdb_storage::{
    plan_replication, CloudProvider, DataGravityShard, GeoLocation, ImmutableObjectStore,
    InMemoryObjectReplicaRegistry, LocalObjectStore, ObjectDigest, ObjectPath, ObjectPlacement,
    ObjectRef, ObjectReplica, PlacementPolicy, PlacementTarget,
};
use std::{
    sync::atomic::{AtomicU64, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};

static TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);

fn bench_local_store_put_verified_get(c: &mut Criterion) {
    let mut group = c.benchmark_group("local_object_store");
    group.sample_size(10);
    group.bench_function("put_get_verified_100", |b| {
        b.iter(|| {
            let root = temp_root("put-get-verified");
            let store = LocalObjectStore::new(&root);
            let payload = vec![42u8; 4096];
            let mut refs = Vec::new();

            for index in 0..100 {
                let path =
                    ObjectPath::new(format!("tenant-a/system/table/part-{index}.arrow")).unwrap();
                refs.push(store.put_if_absent(path, black_box(&payload)).unwrap());
            }

            for object in &refs {
                black_box(store.get_verified(object).unwrap());
            }

            std::fs::remove_dir_all(root).unwrap();
        })
    });
    group.finish();
}

fn bench_placement_policy_validate(c: &mut Criterion) {
    c.bench_function("placement_policy_validate_1000", |b| {
        b.iter(|| {
            for index in 0..1000 {
                let shard = DataGravityShard::new(format!("tenant-a-system-{index}")).unwrap();
                let policy = PlacementPolicy::new(
                    3,
                    vec![
                        PlacementTarget::primary(ObjectPlacement::new(
                            GeoLocation::new(CloudProvider::Aws, "us-east-1", Some("use1-az1"))
                                .unwrap(),
                            shard.clone(),
                        )),
                        PlacementTarget::replica(ObjectPlacement::new(
                            GeoLocation::new(
                                CloudProvider::Gcp,
                                "us-central1",
                                Some("us-central1-a"),
                            )
                            .unwrap(),
                            shard.clone(),
                        )),
                        PlacementTarget::replica(ObjectPlacement::new(
                            GeoLocation::new(CloudProvider::Azure, "eastus", Some("1")).unwrap(),
                            shard,
                        )),
                    ],
                )
                .unwrap();
                black_box(policy);
            }
        })
    });
}

fn bench_replication_plan(c: &mut Criterion) {
    c.bench_function("replication_plan_1000", |b| {
        b.iter(|| {
            for index in 0..1000 {
                let shard = DataGravityShard::new(format!("tenant-a-system-{index}")).unwrap();
                let source_placement = ObjectPlacement::new(
                    GeoLocation::new(CloudProvider::Aws, "us-east-1", Some("use1-az1")).unwrap(),
                    shard.clone(),
                );
                let policy = PlacementPolicy::new(
                    3,
                    vec![
                        PlacementTarget::primary(source_placement.clone()),
                        PlacementTarget::replica(ObjectPlacement::new(
                            GeoLocation::new(
                                CloudProvider::Gcp,
                                "us-central1",
                                Some("us-central1-a"),
                            )
                            .unwrap(),
                            shard.clone(),
                        )),
                        PlacementTarget::replica(ObjectPlacement::new(
                            GeoLocation::new(CloudProvider::Azure, "eastus", Some("1")).unwrap(),
                            shard,
                        )),
                    ],
                )
                .unwrap();
                let source = ehdb_storage::ObjectRef {
                    path: ObjectPath::new(format!("tenant-a/system/table/part-{index}.arrow"))
                        .unwrap(),
                    len: 4096,
                    digest: ehdb_storage::ObjectDigest::new(format!("sha256:{}", "b".repeat(64)))
                        .unwrap(),
                    placement: source_placement,
                };
                black_box(plan_replication(&source, &[], &policy).unwrap());
            }
        })
    });
}

fn bench_replica_registry_register(c: &mut Criterion) {
    c.bench_function("replica_registry_register_1000", |b| {
        b.iter(|| {
            let mut registry = InMemoryObjectReplicaRegistry::default();
            for index in 0..1000 {
                let shard = DataGravityShard::new(format!("tenant-a-system-{index}")).unwrap();
                let replica = ObjectReplica {
                    path: ObjectPath::new(format!("tenant-a/system/table/part-{index}.arrow"))
                        .unwrap(),
                    len: 4096,
                    digest: ObjectDigest::new(format!("sha256:{}", "b".repeat(64))).unwrap(),
                    placement: ObjectPlacement::new(
                        GeoLocation::new(CloudProvider::Aws, "us-east-1", Some("use1-az1"))
                            .unwrap(),
                        shard,
                    ),
                };
                black_box(registry.register(replica).unwrap());
            }
            black_box(registry.replica_count());
        })
    });
}

fn bench_replication_plan_from_registry(c: &mut Criterion) {
    c.bench_function("replication_plan_from_registry_1000", |b| {
        b.iter(|| {
            for index in 0..1000 {
                let shard = DataGravityShard::new(format!("tenant-a-system-{index}")).unwrap();
                let source_placement = ObjectPlacement::new(
                    GeoLocation::new(CloudProvider::Aws, "us-east-1", Some("use1-az1")).unwrap(),
                    shard.clone(),
                );
                let source = ObjectRef {
                    path: ObjectPath::new(format!("tenant-a/system/table/part-{index}.arrow"))
                        .unwrap(),
                    len: 4096,
                    digest: ObjectDigest::new(format!("sha256:{}", "b".repeat(64))).unwrap(),
                    placement: source_placement.clone(),
                };
                let gcp_placement = ObjectPlacement::new(
                    GeoLocation::new(CloudProvider::Gcp, "us-central1", Some("us-central1-a"))
                        .unwrap(),
                    shard.clone(),
                );
                let policy = PlacementPolicy::new(
                    3,
                    vec![
                        PlacementTarget::primary(source_placement),
                        PlacementTarget::replica(gcp_placement.clone()),
                        PlacementTarget::replica(ObjectPlacement::new(
                            GeoLocation::new(CloudProvider::Azure, "eastus", Some("1")).unwrap(),
                            shard,
                        )),
                    ],
                )
                .unwrap();
                let mut registry = InMemoryObjectReplicaRegistry::default();
                registry
                    .register(ObjectReplica {
                        path: source.path.clone(),
                        len: source.len,
                        digest: source.digest.clone(),
                        placement: gcp_placement,
                    })
                    .unwrap();
                black_box(registry.plan_replication(&source, &policy).unwrap());
            }
        })
    });
}

fn temp_root(name: &str) -> std::path::PathBuf {
    let suffix = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let counter = TEMP_COUNTER.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir().join(format!(
        "ehdb-storage-bench-{name}-{}-{suffix}-{counter}",
        std::process::id()
    ))
}

criterion_group!(
    benches,
    bench_local_store_put_verified_get,
    bench_placement_policy_validate,
    bench_replication_plan,
    bench_replica_registry_register,
    bench_replication_plan_from_registry
);
criterion_main!(benches);
