//! **Engine-level proof that the D1 durability window is measured and moves**
//! (noetl/ehdb#328, F4).
//!
//! `src/unreplicated.rs` covers the tracker in isolation. This covers what the
//! unit tests cannot reach: that the tracker is actually **wired into the
//! engine's append, seal and upload-completion paths**, so the number a scrape
//! sees reflects real engine state rather than a struct nobody calls.
//!
//! That distinction is the whole point of the finding this closes. A recorder
//! that exists but is never called produces a permanently-zero metric, which is
//! indistinguishable from a healthy system — so "the gauge exists" is not the
//! property worth asserting. "The gauge rises when records are pending and falls
//! when they are durable" is.

use std::sync::Arc;

use ehdb_l0::substrate::DurableSubstrate;
use ehdb_l0::{D1EventLog, EventRecord, L0Config, L0Engine, LocalFsSubstrate, ReplicaTarget};

fn unique_dir(tag: &str) -> std::path::PathBuf {
    static N: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let n = N.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    std::env::temp_dir().join(format!(
        "ehdb-l0-window-{tag}-{}-{n}-{nanos}",
        std::process::id()
    ))
}

fn target(dir: &std::path::Path) -> Vec<ReplicaTarget> {
    let s: Arc<dyn DurableSubstrate> = Arc::new(LocalFsSubstrate::new(dir).unwrap());
    vec![ReplicaTarget::new("replica-0", s)]
}

fn rec(seq: u64, exec: &str) -> EventRecord {
    EventRecord::new(seq, exec, format!("txn-{seq}"), format!("payload-{seq}"))
}

fn open(tag: &str, shard_count: u32) -> (L0Engine<D1EventLog>, std::path::PathBuf) {
    let local = unique_dir(&format!("{tag}-local"));
    let sub = unique_dir(&format!("{tag}-sub"));
    let config = L0Config::d1(&local).with_shard_count(shard_count);
    let engine = L0Engine::<D1EventLog>::open_replicated(config, target(&sub)).unwrap();
    (engine, local)
}

fn row(engine: &L0Engine<D1EventLog>, shard: u32) -> ehdb_l0::ShardUnreplicated {
    engine
        .unreplicated_snapshot()
        .into_iter()
        .find(|s| s.shard == shard)
        .expect("every shard has a row")
}

#[test]
fn every_shard_reports_zero_before_anything_is_appended() {
    // The pinning property, at engine level: a fresh engine must publish a row
    // per shard reading 0, so a scrape can tell "nothing pending" from "this
    // binary has no such metric".
    let (engine, _local) = open("pinned", 4);
    let snap = engine.unreplicated_snapshot();
    assert_eq!(snap.len(), 4, "one row per shard: {snap:?}");
    assert!(snap
        .iter()
        .all(|s| s.records == 0 && s.oldest_age_millis == 0));
}

#[test]
fn the_window_opens_on_append_and_closes_when_the_part_is_durable() {
    let (mut engine, _local) = open("moves", 1);

    // Nothing pending yet — the reading that must NOT be confused with success.
    assert_eq!(row(&engine, 0).records, 0);

    for seq in 1..=5 {
        engine.append_record(rec(seq, "exec-a")).unwrap();
    }

    // ⚠ Five records are acked and on one disk. They have NOT sealed (well under
    // the 1024-record / 8 MiB triggers) and therefore have not been uploaded —
    // this is exactly the population `upload_lag_micros_total` cannot see.
    let pending = row(&engine, 0);
    assert_eq!(
        pending.records, 5,
        "acked-but-unsealed records must count as unreplicated"
    );
    assert_eq!(
        engine.metrics().snapshot().uploads,
        0,
        "precondition: nothing has been uploaded, so a seal-relative metric \
         would read as perfectly healthy here"
    );

    std::thread::sleep(std::time::Duration::from_millis(30));
    assert!(
        row(&engine, 0).oldest_age_millis >= 25,
        "the window must age while records sit unsealed; got {}",
        row(&engine, 0).oldest_age_millis
    );

    // Seal + ship everything.
    engine.flush_and_wait_uploads().unwrap();

    let after = row(&engine, 0);
    assert_eq!(after.records, 0, "durable ⇒ nothing pending");
    assert_eq!(after.oldest_age_millis, 0, "durable ⇒ the window closes");

    // The end-to-end histogram recorded the append→durable latency. Without
    // this the gauge could fall to 0 simply because the tracker forgot the part.
    let (_buckets, count, sum) = engine.metrics().replicated_lag.snapshot();
    assert!(count >= 1, "append→durable latency must be observed");
    assert!(sum > 0.0, "and it must be a real duration, not 0: {sum}");
}

#[test]
fn one_shards_pending_records_do_not_leak_into_another() {
    // D1 partitions by execution id, so two executions can land on different
    // shards. Whichever they land on, a shard with nothing pending must stay 0 —
    // a per-shard window that aggregated across shards would hide an idle
    // shard's unbounded age behind a busy one's healthy reading.
    let (mut engine, _local) = open("isolation", 8);
    engine.append_record(rec(1, "exec-only-one")).unwrap();

    let snap = engine.unreplicated_snapshot();
    let busy: Vec<_> = snap.iter().filter(|s| s.records > 0).collect();
    assert_eq!(
        busy.len(),
        1,
        "exactly one shard holds the record: {snap:?}"
    );
    assert!(
        snap.iter().filter(|s| s.records == 0).count() == 7,
        "the other seven shards stay at 0 rather than inheriting the window"
    );
}

#[test]
fn a_quiet_shard_keeps_aging_because_nothing_seals_it() {
    // ⚠ This is finding F3 (noetl/ehdb#329) observed through F4's instrument:
    // `should_seal()` triggers on size or record count and never on age, so a
    // shard that goes quiet holds its records indefinitely. The point of this
    // test is that the condition is now VISIBLE — it is not a claim that the
    // behaviour is acceptable.
    let (mut engine, _local) = open("quiet", 1);
    engine.append_record(rec(1, "exec-quiet")).unwrap();

    let first = row(&engine, 0).oldest_age_millis;
    std::thread::sleep(std::time::Duration::from_millis(60));
    let later = row(&engine, 0).oldest_age_millis;

    assert!(
        later > first && later >= 55,
        "an idle shard's window grows without bound: {first} -> {later}"
    );
    assert_eq!(
        engine.metrics().snapshot().seals,
        0,
        "and it grows precisely because nothing sealed it"
    );
}

#[test]
fn a_crash_recovered_active_part_still_reports_its_records_as_pending() {
    // ⚠⚠ The regression this pins. Dropping the engine without sealing is the
    // SIGKILL analogue: the active part is left on disk exactly as a crash
    // leaves it, and reopening replays those records. They are acked, `fsync`'d
    // and NOT on the substrate — so the window must still count them.
    //
    // Before this, the tracker was seeded only by `append_record`, so a
    // recovered shard reported 0 pending and an age of 0: the instrument was
    // quietest at the moment of greatest risk, which is the exact "absent reads
    // as healthy" failure it exists to close.
    let local = unique_dir("crash-local");
    let sub = unique_dir("crash-sub");
    let config = || L0Config::d1(&local).with_shard_count(1);

    {
        let mut engine = L0Engine::<D1EventLog>::open_replicated(config(), target(&sub)).unwrap();
        for seq in 1..=6 {
            engine.append_record(rec(seq, "exec-crash")).unwrap();
        }
        assert_eq!(row(&engine, 0).records, 6);
        // Drop without sealing — no seal hook runs, no manifest is written.
    }

    let engine = L0Engine::<D1EventLog>::open_replicated(config(), target(&sub)).unwrap();
    // Touch the shard so its writer is opened and recovery runs.
    let recovered = engine.metrics().snapshot().recovered_active_records;
    assert_eq!(
        recovered, 6,
        "precondition: the crash left 6 records to replay"
    );

    let after = row(&engine, 0);
    assert_eq!(
        after.records, 6,
        "recovered records are still unreplicated and must be counted"
    );
    assert!(
        after.oldest_age_millis < 60_000,
        "their age restarts at the reopen rather than being invented: {}",
        after.oldest_age_millis
    );

    let _ = std::fs::remove_dir_all(&local);
    let _ = std::fs::remove_dir_all(&sub);
}
