//! noetl/ehdb#388 — the redelivery scan is off the per-poll hot path.
//!
//! `poll_assign` used to scan EVERY in-flight entry on every poll looking for an
//! expired deadline, because `inflight` is keyed by sort key rather than by
//! deadline. A consumer holding k records paid O(k) per delivery, so a drain that
//! acks lazily was O(n²) — measured at **2.86x slower for doing strictly less
//! work** (2,000 records: 2.08 ms poll+ack vs 5.93 ms poll-only).
//!
//! The fix is an O(1) reject: a cached lower bound on the earliest in-flight
//! deadline. Its invariant is one-sided — it may be lower than the true minimum,
//! never higher — because too low only costs a scan while too high would skip a
//! record that is genuinely due and silently break at-least-once.
//!
//! These tests are behavioural on purpose. The bound is private, and asserting on
//! it would pin the implementation rather than the contract that matters: a record
//! that is due is delivered. Each test below fails if the bound is ever allowed to
//! sit above the true earliest deadline.

use std::sync::Arc;

use ehdb_feed::{ShardConsumerGroup, SubjectConsumerGroup, SubjectFilter};
use ehdb_l0::substrate::DurableSubstrate;
use ehdb_l0::{D1EventLog, EventRecord, L0Config, L0Engine, LocalFsSubstrate};

fn unique_dir(tag: &str) -> std::path::PathBuf {
    static N: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let n = N.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    std::env::temp_dir().join(format!("ehdb-feed-rfp-{tag}-{}-{n}", std::process::id()))
}

fn engine_at(local: &std::path::Path, obj: &std::path::Path) -> L0Engine<D1EventLog> {
    let store: Arc<dyn DurableSubstrate> = Arc::new(LocalFsSubstrate::new(obj).unwrap());
    L0Engine::<D1EventLog>::open(L0Config::d1(local), store).unwrap()
}

fn cmd(seq: u64, pool: &str) -> EventRecord {
    EventRecord::new(
        seq,
        format!("exec-{seq}"),
        "t",
        format!("{{\"execution_pool\":\"{pool}\"}}"),
    )
}

/// Control. Without this, "nothing was redelivered" in the tests below is also
/// what a fast path that suppresses everything would produce.
#[test]
fn with_nothing_expired_every_delivery_is_fresh() {
    let (obj, local) = (unique_dir("obj"), unique_dir("local"));
    let mut engine = engine_at(&local, &obj);
    for seq in 1..=5u64 {
        engine.append_record(cmd(seq, "shared")).unwrap();
    }
    let mut group = ShardConsumerGroup::<D1EventLog>::new(0, 100, 0);

    for _ in 0..5 {
        let d = group
            .poll_assign(&engine, 1, 0)
            .unwrap()
            .expect("available");
        assert!(!d.redelivered, "nothing is past its deadline at tick 0");
    }
    assert!(
        group.poll_assign(&engine, 1, 0).unwrap().is_none(),
        "the backlog is drained and nothing is due"
    );
    for d in [&obj, &local] {
        let _ = std::fs::remove_dir_all(d);
    }
}

/// ⭐ The hazard the O(1) reject introduces.
///
/// `expire_now` makes a record due immediately — it is how a departed member's
/// record is released without waiting out `ack_wait` (noetl/ai-meta#216). With a
/// huge `ack_wait` the cached bound sits far in the future, so unless
/// `expire_now` lowers it, the reject fires and the record is never handed out
/// again: an at-least-once violation that no existing test would catch.
#[test]
fn expire_now_is_redelivered_although_the_cached_bound_says_future() {
    let (obj, local) = (unique_dir("obj"), unique_dir("local"));
    let mut engine = engine_at(&local, &obj);
    for seq in 1..=3u64 {
        engine.append_record(cmd(seq, "shared")).unwrap();
    }
    // ack_wait is enormous, so every real deadline is ~1e6 ticks away.
    let mut group = ShardConsumerGroup::<D1EventLog>::new(0, 1_000_000, 0);

    let mut keys = Vec::new();
    for _ in 0..3 {
        keys.push(group.poll_assign(&engine, 1, 0).unwrap().unwrap().sort_key);
    }
    // Release the middle one as if its member's connection had closed.
    assert!(group.expire_now(keys[1]), "it is in flight");

    let d = group
        .poll_assign(&engine, 2, 0)
        .unwrap()
        .expect("the released record must be handed out again");
    assert!(d.redelivered, "and marked as a retry");
    assert_eq!(
        d.sort_key, keys[1],
        "specifically the record that was expired"
    );
    for d in [&obj, &local] {
        let _ = std::fs::remove_dir_all(d);
    }
}

/// Semantics preserved: among several expired records the LOWEST SORT KEY is
/// redelivered, not the earliest deadline. A deadline-ordered index would be
/// cheaper and would change this, and delivery order is observable.
///
/// ⚠ My first version of this test was VACUOUS and a mutation proved it: with a
/// single `ack_wait`, polling at ascending ticks gives deadlines that ascend
/// together WITH sort key, so "earliest deadline" and "lowest sort key" pick the
/// same record and swapping the rule broke nothing. The two only disagree when a
/// HIGH sort key carries an EARLIER deadline, which is exactly what `expire_now`
/// produces — so that is how this builds the divergence.
#[test]
fn a_passed_deadline_redelivers_the_lowest_sort_key() {
    let (obj, local) = (unique_dir("obj"), unique_dir("local"));
    let mut engine = engine_at(&local, &obj);
    for seq in 1..=3u64 {
        engine.append_record(cmd(seq, "shared")).unwrap();
    }
    let mut group = ShardConsumerGroup::<D1EventLog>::new(0, 100, 0);

    // All three delivered at tick 0, so all three share deadline 100.
    let mut keys = Vec::new();
    for _ in 0..3 {
        keys.push(group.poll_assign(&engine, 1, 0).unwrap().unwrap().sort_key);
    }
    keys.sort_unstable();

    // Force the HIGHEST sort key to deadline 0: now the earliest deadline and the
    // lowest sort key are different records.
    assert!(group.expire_now(keys[2]));

    let d = group.poll_assign(&engine, 2, 101).unwrap().unwrap();
    assert!(d.redelivered);
    assert_eq!(
        d.sort_key, keys[0],
        "the LOWEST SORT KEY among the expired — keys[2] has the earlier deadline, \
         so picking by deadline would return it instead"
    );
    for d in [&obj, &local] {
        let _ = std::fs::remove_dir_all(d);
    }
}

/// A scan that finds nothing expired raises the bound to the true minimum. This
/// pins that the refresh is not raised ABOVE it — a deadline that passes later
/// must still fire.
#[test]
fn a_refreshed_bound_still_lets_a_later_deadline_fire() {
    let (obj, local) = (unique_dir("obj"), unique_dir("local"));
    let mut engine = engine_at(&local, &obj);
    for seq in 1..=2u64 {
        engine.append_record(cmd(seq, "shared")).unwrap();
    }
    let mut group = ShardConsumerGroup::<D1EventLog>::new(0, 100, 0);

    let first = group.poll_assign(&engine, 1, 0).unwrap().unwrap().sort_key; // due at 100
    group.poll_assign(&engine, 1, 10).unwrap().unwrap(); // due at 110

    // A poll with nothing due: this is the scan that refreshes the bound.
    assert!(group.poll_assign(&engine, 1, 50).unwrap().is_none());

    let d = group
        .poll_assign(&engine, 2, 105)
        .unwrap()
        .expect("the first record is past its deadline");
    assert!(d.redelivered);
    assert_eq!(d.sort_key, first);
    for d in [&obj, &local] {
        let _ = std::fs::remove_dir_all(d);
    }
}

/// ⭐⭐ The subtler hazard, specific to the subject-routed group.
///
/// Its predicate is `deadline <= now && filter.matches(subject)`, so a poll can
/// walk past an entry that IS expired because this subscriber's filter does not
/// match it. Refreshing the bound from the minimum of the entries that were *not*
/// expired would then push it above the true minimum — and the pool whose filter
/// DOES match would take the O(1) reject and never see its own due record. So the
/// bound is refreshed only when nothing at all was expired.
#[test]
fn an_expired_record_unmatched_by_one_filter_still_reaches_another() {
    let (obj, local) = (unique_dir("obj"), unique_dir("local"));
    let mut engine = engine_at(&local, &obj);
    engine.append_record(cmd(1, "system")).unwrap();
    for seq in 2..=4u64 {
        engine.append_record(cmd(seq, "shared")).unwrap();
    }
    let mut group =
        SubjectConsumerGroup::<D1EventLog>::new(0, 100, 0, ehdb_feed::d1_command_subject(1));
    let system: SubjectFilter = SubjectFilter::parse("commands.system.>");
    let shared: SubjectFilter = SubjectFilter::parse("commands.shared.>");

    // The system pool claims its command and stalls (never acks). Due at 100.
    let stuck = group
        .poll_assign(&engine, &system, 9, 0)
        .unwrap()
        .unwrap()
        .sort_key;

    // Past the deadline, the SHARED pool polls. The stuck record is expired but
    // does not match `shared`, so this poll must not raise the bound.
    let fresh = group.poll_assign(&engine, &shared, 1, 101).unwrap();
    assert!(fresh.is_some(), "the shared pool still gets its own work");
    assert!(
        !fresh.unwrap().redelivered,
        "and it is a fresh record, not the system pool's"
    );

    // Now the system pool polls again. Its record is due and must come back.
    let d = group
        .poll_assign(&engine, &system, 9, 101)
        .unwrap()
        .expect("the stuck system command is past its deadline and must redeliver");
    assert!(d.redelivered);
    assert_eq!(d.sort_key, stuck);
    for d in [&obj, &local] {
        let _ = std::fs::remove_dir_all(d);
    }
}
