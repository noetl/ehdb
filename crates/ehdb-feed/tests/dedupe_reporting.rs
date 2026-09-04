//! The batch reporting variant: per-record verdicts, and the tip must not move
//! backwards on a duplicate (noetl/ai-meta#313).

use ehdb_feed::FeedWriter;
use ehdb_l0::dataset::{D1EventLog, EventRecord};
use ehdb_l0::engine::{L0Config, L0Engine};
use ehdb_l0::substrate::{DurableSubstrate, LocalFsSubstrate};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

static N: AtomicU32 = AtomicU32::new(0);

fn writer(tag: &str) -> Arc<FeedWriter<D1EventLog>> {
    let n = N.fetch_add(1, Ordering::SeqCst);
    let dir =
        std::env::temp_dir().join(format!("ehdb-dedupe-rep-{tag}-{}-{n}", std::process::id()));
    let store: Arc<dyn DurableSubstrate> =
        Arc::new(LocalFsSubstrate::new(dir.join("obj")).unwrap());
    let engine = L0Engine::<D1EventLog>::open(L0Config::d1(dir.join("local")), store).unwrap();
    Arc::new(FeedWriter::new(engine))
}

fn ev(exec: &str, id: &str) -> EventRecord {
    EventRecord::new(0, exec, "t", "p").with_event_id(id)
}

/// ⭐ A redelivered batch reports every record as not-written, and the log holds
/// one copy of each.
///
/// ⚠ Mutation verified: reporting `true` unconditionally fails the
/// `appended=false` assertion; dropping the `filter(|(_, w)| *w)` from the tip
/// calculation fails `the_tip_never_moves_backwards_on_a_duplicate`.
#[test]
fn a_redelivered_batch_reports_every_record_as_acknowledged() {
    let w = writer("batch");
    let batch: Vec<EventRecord> = (0..5).map(|i| ev("exec-b", &format!("e{i}"))).collect();

    let first = w.append_batch_reporting(batch.clone()).unwrap();
    assert_eq!(first.len(), 5);
    assert!(
        first.iter().all(|(_, wrote)| *wrote),
        "a first delivery must report every record as written"
    );

    let second = w.append_batch_reporting(batch.clone()).unwrap();
    assert!(
        second.iter().all(|(_, wrote)| !*wrote),
        "a full redelivery must report every record as an acknowledgement, or the \
         caller's monotonicity check reports a divergence for a working dedupe"
    );
    for (i, ((s1, _), (s2, _))) in first.iter().zip(second.iter()).enumerate() {
        assert_eq!(s1, s2, "record {i} must acknowledge its original position");
    }
}

/// ⚠ The advertised tip must never move BACKWARDS because of a duplicate.
///
/// The tip is what a follower's cursor chases. Publishing an older sequence
/// after newer records exist would make the feed appear to rewind, and a
/// consumer that trusts it would re-read or stall.
#[test]
fn the_tip_never_moves_backwards_on_a_duplicate() {
    let w = writer("tip");
    let tip = w.tip_receiver();
    let old = vec![ev("exec-t", "old-1")];
    let first = w.append_batch_reporting(old.clone()).unwrap();
    let old_seq = first[0].0;

    // Newer traffic advances the tip.
    let newer: Vec<EventRecord> = (0..3).map(|i| ev("exec-t", &format!("new-{i}"))).collect();
    let adv = w.append_batch_reporting(newer).unwrap();
    let high = adv.iter().map(|(s, _)| *s).max().unwrap();
    assert!(high > old_seq);

    // Redelivering the OLD record must not rewind the tip.
    let redeliver = w.append_batch_reporting(old).unwrap();
    assert!(!redeliver[0].1, "it is a duplicate");
    assert_eq!(
        redeliver[0].0, old_seq,
        "acknowledged at its original position"
    );
    // ⚠ Observe the ACTUAL advertised tip, not a proxy for it. The first version
    // of this test asserted only that an empty batch is a no-op, which is true of
    // every implementation — a mutation that published the last record's sequence
    // regardless of whether it was written passed it untouched. The tip is what a
    // follower's cursor chases, so it has to be read.
    let tip_now = *tip.borrow();
    assert_eq!(
        tip_now, high,
        "a batch of pure duplicates must leave the advertised tip at the newest \
         WRITTEN record ({high}), not rewind it to the acknowledged one ({old_seq}). \
         A rewound tip makes the feed appear to go backwards to every follower"
    );
}

/// A mixed batch — some new, some already present — reports per record.
#[test]
fn a_mixed_batch_reports_per_record() {
    let w = writer("mixed");
    w.append_batch_reporting(vec![ev("exec-m", "a"), ev("exec-m", "b")])
        .unwrap();
    let out = w
        .append_batch_reporting(vec![ev("exec-m", "b"), ev("exec-m", "c")])
        .unwrap();
    assert_eq!(
        out.iter().map(|(_, w)| *w).collect::<Vec<_>>(),
        vec![false, true],
        "the already-present record acknowledges, the new one appends — a batch \
         is not all-or-nothing"
    );
}
