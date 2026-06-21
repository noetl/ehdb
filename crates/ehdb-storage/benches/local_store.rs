use criterion::{black_box, criterion_group, criterion_main, Criterion};
use ehdb_storage::{
    CloudProvider, DataGravityShard, GeoLocation, ImmutableObjectStore, LocalObjectStore,
    ObjectPath, ObjectPlacement, PlacementPolicy, PlacementTarget,
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
    bench_placement_policy_validate
);
criterion_main!(benches);
