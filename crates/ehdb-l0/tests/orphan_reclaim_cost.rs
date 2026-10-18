//! noetl/ehdb#395 — `reclaim_orphans` must size an orphan WITHOUT fetching it.
//!
//! It used to call `get_all` on every orphan purely to populate a byte counter,
//! immediately before `delete`ing it. On a local substrate that is a wasted file
//! read; with a remote substrate in the replica set (noetl/server's GCS
//! `DurableSubstrate`, noetl/ai-meta#460 A2) it is a **full download per orphan**
//! — measured GCS get p50 77 ms / p95 139 ms — of an object about to be thrown
//! away. `apply_retention` calls `reclaim_orphans` unconditionally, so it is on
//! the retention path rather than an opt-in one.
//!
//! The assertion that matters is a COUNT, not a duration: zero whole-object reads.

use std::sync::Arc;

use ehdb_l0::substrate::DurableSubstrate;
use ehdb_l0::{CountingSubstrate, D1EventLog, L0Config, L0Engine, LocalFsSubstrate, MergePolicy};

fn unique_dir(tag: &str) -> std::path::PathBuf {
    static N: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let n = N.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    std::env::temp_dir().join(format!("ehdb-orphan-cost-{tag}-{}-{n}", std::process::id()))
}

/// Total bytes of every file under `root` — the physical cost the reclaim frees.
fn bytes_on_disk(root: &std::path::Path) -> u64 {
    fn walk(p: &std::path::Path, acc: &mut u64) {
        let Ok(rd) = std::fs::read_dir(p) else { return };
        for e in rd.flatten() {
            let path = e.path();
            if path.is_dir() {
                walk(&path, acc);
            } else if let Ok(m) = std::fs::metadata(&path) {
                *acc += m.len();
            }
        }
    }
    let mut acc = 0;
    walk(root, &mut acc);
    acc
}

#[test]
fn reclaiming_orphans_issues_no_whole_object_reads() {
    let (obj, local) = (unique_dir("obj"), unique_dir("local"));
    let counting = CountingSubstrate::new(LocalFsSubstrate::new(&obj).unwrap());
    let counters = counting.counters();
    let store: Arc<dyn DurableSubstrate> = Arc::new(counting);

    let mut engine = L0Engine::<D1EventLog>::open(
        L0Config::d1(&local)
            .with_shard_count(1)
            .with_granule_size(4)
            .with_seal_max_records(8)
            .with_merge_policy(MergePolicy {
                small_part_max_records: 8,
                trigger_run_len: 3,
                max_merge_parts: 8,
            }),
        store,
    )
    .unwrap();
    for i in 0..48u64 {
        engine
            .append("1001", &format!("t{i}"), format!("payload-{i}"))
            .unwrap();
    }
    engine.flush_and_wait_uploads().unwrap();
    // Merging supersedes the source parts, leaving them unreferenced on the
    // substrate — the orphans the reclaim is about.
    assert!(
        engine.run_pending_merges().unwrap() >= 1,
        "the merge is what creates the orphans this test measures"
    );

    // ⚠ Both roots: `reclaim_orphans` reclaims OBJECT orphans on the substrate
    // AND local hot-tier part files, and `orphan_bytes` sums both. Walking only
    // the object dir made my first version of the byte assertion wrong.
    let disk_before = bytes_on_disk(&obj) + bytes_on_disk(&local);
    let get_all_before = counters
        .get_all_calls
        .load(std::sync::atomic::Ordering::Relaxed);
    let size_before = counters
        .object_size_calls
        .load(std::sync::atomic::Ordering::Relaxed);
    let delete_before = counters
        .delete_calls
        .load(std::sync::atomic::Ordering::Relaxed);
    let bytes_metric_before = engine.metrics().snapshot().orphan_bytes;

    let reclaimed = engine.reclaim_orphans().unwrap() as u64;
    assert!(
        reclaimed >= 1,
        "positive control: there were orphans to reclaim, so the counts below \
         are readings rather than the absence of work"
    );

    let get_all_delta = counters
        .get_all_calls
        .load(std::sync::atomic::Ordering::Relaxed)
        - get_all_before;
    let size_delta = counters
        .object_size_calls
        .load(std::sync::atomic::Ordering::Relaxed)
        - size_before;

    // ⭐ The point of the change.
    assert_eq!(
        get_all_delta, 0,
        "reclaiming {reclaimed} orphans must not fetch a single whole object"
    );
    // ⚠ Compared against the DELETE count, not `reclaimed`. `reclaimed` also
    // counts local hot-tier part files, which are sized with `fs::metadata` and
    // never touch the substrate — my first version asserted 12 == 6 and the test
    // was wrong, not the code. Every object that was deleted was sized cheaply:
    // one `object_size` per `delete`, nothing unmeasured standing in for the
    // download.
    let delete_delta = counters
        .delete_calls
        .load(std::sync::atomic::Ordering::Relaxed)
        - delete_before;
    assert!(delete_delta >= 1, "at least one OBJECT orphan was deleted");
    assert_eq!(
        size_delta, delete_delta,
        "exactly one cheap size per deleted object"
    );
    assert!(
        size_delta <= reclaimed,
        "object orphans are a subset of all reclaimed orphans"
    );

    // And the metric it populates is still correct: the bytes it recorded equal
    // the bytes the reclaim actually freed from disk.
    let freed = disk_before - (bytes_on_disk(&obj) + bytes_on_disk(&local));
    let bytes_metric_delta = engine.metrics().snapshot().orphan_bytes - bytes_metric_before;
    assert!(freed > 0, "the reclaim freed real bytes");
    assert_eq!(
        bytes_metric_delta, freed,
        "orphan_bytes must equal what was actually freed — a size of 0 would \
         keep the download count at 0 while making the metric useless"
    );

    for d in [&obj, &local] {
        let _ = std::fs::remove_dir_all(d);
    }
}
