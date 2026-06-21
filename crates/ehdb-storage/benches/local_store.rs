use criterion::{black_box, criterion_group, criterion_main, Criterion};
use ehdb_storage::{ImmutableObjectStore, LocalObjectStore, ObjectPath};
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

criterion_group!(benches, bench_local_store_put_verified_get);
criterion_main!(benches);
