//! Proof for the writer-assigned append path + the ascending-contract canary
//! (noetl/ai-meta#203).
//!
//! `append_writer_assigned` makes the single writer the authority on the sort
//! key: whatever order records (with whatever pre-set keys) reach it, the shard
//! log comes out strictly ascending, so a follower cursor never skips one.
//! `append_record` (the intrinsic-key path) leaves keys untouched and counts any
//! non-advancing append via `L0Metrics::out_of_order_appends`, so the loss class
//! is observable rather than silent.

use std::sync::Arc;

use ehdb_l0::substrate::DurableSubstrate;
use ehdb_l0::{ChangeFeed, D1EventLog, EventRecord, L0Config, L0Engine, LocalFsSubstrate};

fn unique_dir(tag: &str) -> std::path::PathBuf {
    static N: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let n = N.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    std::env::temp_dir().join(format!("ehdb-l0-wa-{tag}-{}-{n}", std::process::id()))
}

fn engine(tag: &str) -> (L0Engine<D1EventLog>, std::path::PathBuf, std::path::PathBuf) {
    let (obj, local) = (
        unique_dir(&format!("{tag}-obj")),
        unique_dir(&format!("{tag}-local")),
    );
    let store: Arc<dyn DurableSubstrate> = Arc::new(LocalFsSubstrate::new(&obj).unwrap());
    let e = L0Engine::<D1EventLog>::open(L0Config::d1(&local), store).unwrap();
    (e, obj, local)
}

/// A record whose pre-set sort key (`global_sequence`) is `key` — the shape the
/// command feed's producer sends (a snowflake it assigned).
fn rec(key: u64) -> EventRecord {
    EventRecord::new(key, format!("exec-{key}"), "t", format!("payload-{key}"))
}

#[test]
fn writer_assigned_keys_are_ascending_regardless_of_input_order() {
    let (mut e, obj, local) = engine("asc");

    // Feed the writer records whose pre-set keys are wildly out of order — the
    // concurrent-publish reordering. The writer re-keys each to its next
    // monotonic sequence.
    let inputs = [900u64, 100, 100, 5, 4242, 1];
    let mut assigned = Vec::new();
    for k in inputs {
        assigned.push(e.append_writer_assigned(rec(k)).unwrap());
    }
    // Assigned keys are 1..=N, strictly ascending, gapless — the contract.
    assert_eq!(assigned, vec![1, 2, 3, 4, 5, 6]);

    // A follower reading from the beginning sees every record exactly once, in
    // ascending key order (no skips despite the out-of-order inputs).
    let mut feed = ChangeFeed::new(0, 0);
    let seen: Vec<u64> = feed
        .poll(&e)
        .unwrap()
        .iter()
        .map(|r| r.global_sequence)
        .collect();
    assert_eq!(
        seen,
        vec![1, 2, 3, 4, 5, 6],
        "every record delivered, ascending"
    );

    // The canary stays clean — the writer-assigned path never trips it.
    assert_eq!(e.metrics().snapshot().out_of_order_appends, 0);

    for d in [&obj, &local] {
        let _ = std::fs::remove_dir_all(d);
    }
}

#[test]
fn append_record_out_of_order_is_counted_by_the_canary() {
    let (mut e, obj, local) = engine("canary");

    // The intrinsic-key path trusts the caller. Ascending appends are clean...
    e.append_record(rec(10)).unwrap();
    e.append_record(rec(20)).unwrap();
    assert_eq!(e.metrics().snapshot().out_of_order_appends, 0);

    // ...but a key at/behind the shard tail (the #203 out-of-order arrival) is
    // counted, so the silent-loss class becomes observable.
    e.append_record(rec(15)).unwrap(); // < 20
    e.append_record(rec(20)).unwrap(); // == 20
    assert_eq!(
        e.metrics().snapshot().out_of_order_appends,
        2,
        "both non-advancing appends counted"
    );

    for d in [&obj, &local] {
        let _ = std::fs::remove_dir_all(d);
    }
}
