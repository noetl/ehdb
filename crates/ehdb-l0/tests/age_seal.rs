//! **The age-based seal trigger** (noetl/ehdb#329, F3).
//!
//! Sealing is triggered by `seal_max_bytes` (8 MiB) or `seal_max_records`
//! (1024) and, before this, by nothing else. A record reaches the durable
//! substrate only when its part seals — so a shard that appends a few records
//! and goes quiet holds them on one disk **indefinitely**. The durability window
//! is bounded by volume, not by time.
//!
//! That inverts the intuition anyone brings: busy shards seal constantly and
//! have a window of seconds, while **idle shards have an unbounded one**. The
//! system is least durable where it is least active.
//!
//! The trigger added here is **off by default**. These tests pin both halves —
//! that it fires when enabled, and that nothing changes when it is not.

use std::sync::Arc;
use std::time::Duration;

use ehdb_l0::substrate::DurableSubstrate;
use ehdb_l0::{D1EventLog, EventRecord, L0Config, L0Engine, LocalFsSubstrate, ReplicaTarget};

const AGE: Duration = Duration::from_millis(50);

fn unique_dir(tag: &str) -> std::path::PathBuf {
    static N: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let n = N.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    std::env::temp_dir().join(format!(
        "ehdb-l0-age-{tag}-{}-{n}-{nanos}",
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

/// `age = None` reproduces today's behavior exactly.
fn open(tag: &str, age: Option<Duration>) -> L0Engine<D1EventLog> {
    let local = unique_dir(&format!("{tag}-local"));
    let sub = unique_dir(&format!("{tag}-sub"));
    let config = L0Config::d1(&local)
        .with_shard_count(1)
        .with_seal_max_age(age);
    L0Engine::<D1EventLog>::open_replicated(config, target(&sub)).unwrap()
}

#[test]
fn an_idle_shard_seals_on_age_alone() {
    let mut engine = open("fires", Some(AGE));
    engine.append_record(rec(1, "exec-quiet")).unwrap();

    // Nowhere near the size or count triggers — only age can seal this.
    assert_eq!(engine.metrics().snapshot().seals, 0);

    std::thread::sleep(AGE + Duration::from_millis(20));
    let sealed = engine.seal_aged_parts().unwrap();

    assert_eq!(sealed, 1, "the aged part must seal with no further appends");
    assert_eq!(engine.metrics().snapshot().seals, 1);
}

#[test]
fn with_the_trigger_off_the_same_idle_shard_never_seals() {
    // ⚠ The negative control. Without it the test above could pass on an engine
    // that seals everything unconditionally, and would prove nothing about the
    // age trigger specifically.
    let mut engine = open("off", None);
    engine.append_record(rec(1, "exec-quiet")).unwrap();

    std::thread::sleep(AGE + Duration::from_millis(20));
    let sealed = engine.seal_aged_parts().unwrap();

    assert_eq!(sealed, 0, "default-off must reproduce today's behavior");
    assert_eq!(
        engine.metrics().snapshot().seals,
        0,
        "the record is still sitting in an unsealed active part — this is the \
         unbounded window, preserved deliberately until the flag is enabled"
    );
}

#[test]
fn the_trigger_is_inert_unless_something_drives_it() {
    // ⚠ The reachability trap this design has to avoid. `should_seal()` is only
    // consulted on append, and the shard the age trigger protects is by
    // definition the one taking no appends. Enabling the flag and never driving
    // `seal_aged_parts` leaves it inert on exactly that shard — config that
    // looks correct, a trigger that never fires.
    let mut engine = open("inert", Some(AGE));
    engine.append_record(rec(1, "exec-quiet")).unwrap();
    std::thread::sleep(AGE + Duration::from_millis(20));

    // Deliberately do NOT call seal_aged_parts().
    assert_eq!(
        engine.metrics().snapshot().seals,
        0,
        "the flag alone seals nothing; a timer must drive it"
    );

    // And it is genuinely aged — so the zero above is inertness, not a part
    // that simply had not aged yet.
    let ages = engine.active_ages();
    assert_eq!(ages.len(), 1);
    assert!(
        ages[0].1 >= AGE,
        "the part IS past the limit: {:?}",
        ages[0].1
    );
}

#[test]
fn a_later_append_also_notices_the_age() {
    // The append path must honour the trigger too, otherwise a shard that is
    // *nearly* idle keeps deferring its seal on every trickle of traffic.
    let mut engine = open("append-path", Some(AGE));
    engine.append_record(rec(1, "exec-a")).unwrap();
    std::thread::sleep(AGE + Duration::from_millis(20));

    engine.append_record(rec(2, "exec-a")).unwrap();
    assert_eq!(
        engine.metrics().snapshot().seals,
        1,
        "the append that arrives after the limit seals the aged part"
    );
}

#[test]
fn the_age_measures_the_oldest_record_not_the_newest() {
    // A steady trickle must still seal within the limit. If the clock restarted
    // on every append, a shard taking one record just under the limit forever
    // would never seal — the unbounded window would survive the fix.
    let mut engine = open("oldest", Some(AGE));
    for seq in 1..=4 {
        engine.append_record(rec(seq, "exec-a")).unwrap();
        std::thread::sleep(AGE / 3);
    }
    engine.seal_aged_parts().unwrap();
    // Either path may be the one that fires — whichever append or sweep first
    // observes the limit — and the property is that the part seals at all.
    // (Asserting on `seal_aged_parts`'s return alone is wrong: a trickle whose
    // fourth append lands past the limit seals on the APPEND path, leaving the
    // sweep correctly with nothing to do.)
    assert_eq!(
        engine.metrics().snapshot().seals,
        1,
        "the part ages from its FIRST record, so a trickle still seals"
    );

    // The negative control for this specific property: with the trigger off,
    // the identical trickle seals nothing.
    let mut off = open("oldest-off", None);
    for seq in 1..=4 {
        off.append_record(rec(seq, "exec-a")).unwrap();
        std::thread::sleep(AGE / 3);
    }
    off.seal_aged_parts().unwrap();
    assert_eq!(
        off.metrics().snapshot().seals,
        0,
        "and it is the age trigger doing it, not the trickle itself"
    );
}

#[test]
fn sealing_on_age_is_a_no_op_when_nothing_is_pending() {
    let mut engine = open("empty", Some(AGE));
    assert_eq!(engine.seal_aged_parts().unwrap(), 0);
    std::thread::sleep(AGE + Duration::from_millis(10));
    assert_eq!(
        engine.seal_aged_parts().unwrap(),
        0,
        "an empty active part must never seal — that would write empty parts \
         forever on an idle shard"
    );
    assert_eq!(engine.metrics().snapshot().seals, 0);
}

#[test]
fn the_part_count_cost_is_bounded_and_measured() {
    // ⚠ The honest cost: sealing on age produces small parts on idle shards,
    // raising part count and merge pressure. Measured rather than asserted away
    // — N quiet intervals produce at most N parts, not one per record.
    // ⚠ This test uses its OWN, much longer age than the rest of the file, and the
    // reason is a real CI failure rather than caution.
    //
    // With the shared 50 ms `AGE`, the five appends in a round must complete inside
    // 50 ms or the age trigger fires *during* the loop and that round produces two
    // parts instead of one. Each append `fsync`s under the default posture, so on a
    // loaded runner five of them exceed 50 ms easily — the assertion then reads 4+
    // seals and reports a defect that is not there. It passed locally and on `main`
    // and failed on CI, which is the signature of a timing race, not a regression.
    //
    // 750 ms is not a magic number: far longer than five `fsync`s can plausibly
    // take, far shorter than the explicit sleep, so the only thing that can cross
    // the threshold is the sleep. The property under test is unchanged — N quiet
    // intervals produce N parts, not one per record.
    const COST_AGE: Duration = Duration::from_millis(750);
    let mut engine = open("cost", Some(COST_AGE));
    for round in 0..3 {
        for seq in 0..5 {
            engine
                .append_record(rec(round * 10 + seq + 1, "exec-a"))
                .unwrap();
        }
        std::thread::sleep(COST_AGE + Duration::from_millis(20));
        engine.seal_aged_parts().unwrap();
    }
    let seals = engine.metrics().snapshot().seals;
    assert_eq!(
        seals, 3,
        "one part per quiet interval, not one per record (15 records -> {seals})"
    );
}

#[test]
fn the_trigger_is_off_in_the_default_config_not_merely_when_asked() {
    // ⚠⚠ This test exists because mutation testing found its absence. The other
    // "off" test calls `.with_seal_max_age(None)` explicitly, so flipping the
    // DEFAULT to `Some(..)` left every test passing — the one safety claim this
    // change rests on ("default off = today's behavior") was untested.
    let dir = unique_dir("default");
    assert!(
        L0Config::d1(&dir).seal_max_age.is_none(),
        "d1() must not enable the age trigger"
    );
    assert!(
        L0Config::for_dataset("whatever", &dir)
            .seal_max_age
            .is_none(),
        "for_dataset() inherits d1()'s defaults and must not enable it either"
    );
}

#[test]
fn an_engine_built_from_the_default_config_never_age_seals() {
    // The behavioural half of the check above: no `with_seal_max_age` call at
    // all, so this is exactly what an existing caller gets after this change.
    let local = unique_dir("default-engine-local");
    let sub = unique_dir("default-engine-sub");
    let config = L0Config::d1(&local).with_shard_count(1);
    let mut engine = L0Engine::<D1EventLog>::open_replicated(config, target(&sub)).unwrap();

    engine.append_record(rec(1, "exec-quiet")).unwrap();
    std::thread::sleep(AGE + Duration::from_millis(40));
    engine.seal_aged_parts().unwrap();
    engine.append_record(rec(2, "exec-quiet")).unwrap();

    assert_eq!(
        engine.metrics().snapshot().seals,
        0,
        "an untouched caller must see byte-for-byte today's behavior"
    );
}
