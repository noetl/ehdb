//! **noetl/ai-meta#313** — append-time idempotency, end to end against a real
//! on-disk engine.
//!
//! Every test here is a property the *append path of a `primary`-serving tier*
//! now depends on, so each is paired with the mutation it was verified to catch.
//! A guard never shown to fail is indistinguishable from one that cannot fail.

use ehdb_l0::dataset::{D1EventLog, EventRecord};
use ehdb_l0::engine::{L0Config, L0Engine};
use ehdb_l0::substrate::{DurableSubstrate, LocalFsSubstrate};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

static N: AtomicU32 = AtomicU32::new(0);

/// A unique scratch dir per call — `cargo test` does not serialise tests within
/// a binary, so a shared path would race and the failure would be blamed on the
/// dedupe.
fn scratch(tag: &str) -> std::path::PathBuf {
    let n = N.fetch_add(1, Ordering::SeqCst);
    std::env::temp_dir().join(format!("ehdb-l0-dedupe-{tag}-{}-{n}", std::process::id()))
}

fn engine_at(dir: &std::path::Path, capacity: Option<usize>) -> L0Engine<D1EventLog> {
    let mut cfg = L0Config::d1(dir.join("local"));
    if let Some(c) = capacity {
        cfg = cfg.with_dedupe_capacity(c);
    }
    let store: Arc<dyn DurableSubstrate> =
        Arc::new(LocalFsSubstrate::new(dir.join("substrate")).unwrap());
    L0Engine::open(cfg, store).expect("engine opens")
}

fn engine(dir: &std::path::Path) -> L0Engine<D1EventLog> {
    engine_at(dir, None)
}

fn ev(exec: &str, event_id: &str, payload: &str) -> EventRecord {
    EventRecord::new(0, exec, "txn", payload).with_event_id(event_id)
}

/// Records currently readable for an execution, in order.
fn read_all(e: &L0Engine<D1EventLog>, exec: &str) -> Vec<EventRecord> {
    e.read_index_after(exec, 0).expect("read")
}

/// ⭐ GATE 1 — a redelivery is deduplicated, acknowledged, and NOT silently dropped.
///
/// The pre-#313 behaviour had two failure modes and no third: the redelivery
/// either became a visible duplicate, or reused a key that no longer advanced the
/// shard tail and "lands behind any follower cursor and is silently never
/// delivered". This asserts the third outcome now exists.
///
/// ⚠ Mutation verified: removing the `dedupe_hit` guard from
/// `append_writer_assigned` fails this on the record count (2, not 1).
#[test]
fn a_redelivered_event_is_deduplicated_and_acknowledged() {
    let tmp = scratch("g");
    let mut e = engine(&tmp);

    let first = e
        .append_writer_assigned(ev("exec-1", "evt-1", "body"))
        .expect("first append");
    let tail_after_first = e.global_sequence();

    let second = e
        .append_writer_assigned(ev("exec-1", "evt-1", "body"))
        .expect("redelivery must succeed, not error");

    assert_eq!(
        second, first,
        "the redelivery must be acknowledged AT THE EXISTING POSITION. Returning \
         a different key would make the caller believe a second record exists"
    );
    assert_eq!(
        read_all(&e, "exec-1").len(),
        1,
        "exactly one copy must be in the log — a second is the #313 duplicate"
    );
    assert_eq!(
        e.global_sequence(),
        tail_after_first,
        "a duplicate must not burn a global sequence. A gap here surfaces as a \
         parity divergence, because the tier-append reply is checked for \
         `log_record_count == global_sequence` gaplessness"
    );
    assert_eq!(
        e.metrics()
            .out_of_order_appends
            .load(std::sync::atomic::Ordering::Relaxed),
        0,
        "the redelivery must not trip the ascending-contract canary — tripping it \
         is the silent-drop path this exists to remove"
    );
    assert_eq!(
        e.metrics()
            .dedupe_hits
            .load(std::sync::atomic::Ordering::Relaxed),
        1,
        "the dedupe must be observable; an invisible one cannot be operated"
    );
}

/// ⚠⚠ GATE 2 — THE NEGATIVE CONTROL. A genuinely new event still appends.
///
/// This is the failure mode that would be worst and quietest: a dedupe that
/// swallows legitimate new events is silent data loss on the write path of the
/// serving tier. Without this test, an implementation that deduplicates
/// *everything* passes gate 1 perfectly.
///
/// ⚠ Mutation verified: making `dedupe_hit` return `Some(0)` unconditionally
/// passes gate 1 and fails here on the record count.
#[test]
fn a_new_event_still_appends_normally() {
    let tmp = scratch("g");
    let mut e = engine(&tmp);

    let a = e
        .append_writer_assigned(ev("exec-1", "evt-a", "A"))
        .unwrap();
    let b = e
        .append_writer_assigned(ev("exec-1", "evt-b", "B"))
        .unwrap();
    let c = e
        .append_writer_assigned(ev("exec-1", "evt-c", "C"))
        .unwrap();

    assert!(
        a < b && b < c,
        "distinct events must get ascending positions"
    );
    let all = read_all(&e, "exec-1");
    assert_eq!(
        all.len(),
        3,
        "three distinct events must yield three records"
    );
    assert_eq!(
        e.metrics()
            .dedupe_hits
            .load(std::sync::atomic::Ordering::Relaxed),
        0,
        "no dedupe may fire for distinct keys"
    );
    let bodies: Vec<&str> = all.iter().map(|r| r.payload.as_str()).collect();
    assert_eq!(bodies, vec!["A", "B", "C"], "content and order preserved");
}

/// ⭐ GATE 3 — the #313 scenario, reproduced and closed.
///
/// #313 observed "11 duplicates" from a sink mirror re-mirroring events. This is
/// that shape: a batch delivered, then the whole batch delivered again (a relay
/// retry after a partial-failure report), interleaved with new events.
#[test]
fn the_313_re_mirror_scenario_produces_no_duplicates() {
    let tmp = scratch("g");
    let mut e = engine(&tmp);

    let batch: Vec<EventRecord> = (0..11)
        .map(|i| ev("exec-313", &format!("evt-{i}"), &format!("p{i}")))
        .collect();

    for r in batch.iter().cloned() {
        e.append_writer_assigned(r).unwrap();
    }
    // The relay reports failure and redelivers the identical batch.
    for r in batch.iter().cloned() {
        e.append_writer_assigned(r).unwrap();
    }
    // New traffic continues during the retry.
    e.append_writer_assigned(ev("exec-313", "evt-new", "fresh"))
        .unwrap();
    // And the retry happens again, because relays do that.
    for r in batch.iter().cloned() {
        e.append_writer_assigned(r).unwrap();
    }

    let all = read_all(&e, "exec-313");
    assert_eq!(
        all.len(),
        12,
        "11 originals + 1 new. Before #313's fix this was 34 — every redelivery \
         appended again"
    );
    assert_eq!(
        e.metrics()
            .dedupe_hits
            .load(std::sync::atomic::Ordering::Relaxed),
        22,
        "both full redeliveries must be accounted, not silently absorbed"
    );
    assert_eq!(
        e.metrics()
            .out_of_order_appends
            .load(std::sync::atomic::Ordering::Relaxed),
        0,
        "no redelivery may land behind the follower cursor"
    );
}

/// ⭐ GATE 4a — MIGRATION: records written before the column exists still work.
///
/// A pre-#313 record is one with no `event_id`. It must read back, and it must
/// still be appendable — dedupe simply does not apply to it. An implementation
/// that required the key would break every existing producer at once.
///
/// ⚠ Mutation verified: making `D1EventLog::dedupe_key` return
/// `Some(&record.execution_id)` when `event_id` is `None` fails this — all three
/// same-execution records collapse to one.
#[test]
fn records_without_an_event_id_still_append_and_never_deduplicate() {
    let tmp = scratch("g");
    let mut e = engine(&tmp);

    // Exactly what a pre-column producer sends: no event_id at all.
    for i in 0..3 {
        e.append_writer_assigned(EventRecord::new(0, "exec-old", "txn", format!("p{i}")))
            .unwrap();
    }
    assert_eq!(
        read_all(&e, "exec-old").len(),
        3,
        "keyless records must never be collapsed — they are indistinguishable to \
         the dedupe and must therefore all land"
    );
    assert_eq!(
        e.metrics()
            .dedupe_hits
            .load(std::sync::atomic::Ordering::Relaxed),
        0
    );
}

/// ⭐ GATE 4b — MIGRATION: the on-disk form of a keyless record is unchanged.
///
/// This is what makes a rollback safe. A record with no `event_id` must
/// serialise **byte-identically to before the column existed**, so a binary that
/// predates the field can still read everything written while the producer has
/// not been switched on.
#[test]
fn a_keyless_record_serialises_exactly_as_before() {
    let old = EventRecord::new(7, "exec-1", "txn-1", "body");
    let json = serde_json::to_string(&old).unwrap();
    assert!(
        !json.contains("event_id"),
        "a record with no key must not emit the field, or every pre-existing \
         record changes shape on disk and a rollback cannot read them: {json}"
    );

    // And the reverse: the pre-column wire form still parses.
    let legacy = r#"{"global_sequence":7,"execution_id":"exec-1","transaction_id":"txn-1","payload":"body"}"#;
    let parsed: EventRecord = serde_json::from_str(legacy).expect("legacy record must parse");
    assert_eq!(parsed, old);
    assert_eq!(parsed.event_id, None);
}

/// ⚠ GATE 4c — a record carrying the key round-trips, and unknown fields are
/// tolerated.
///
/// `deny_unknown_fields` was removed for this: with it, a binary predating any
/// new column **errors** on a record carrying it, so a rollback could not read
/// what the newer binary wrote. Tolerating unknown fields has to ship before
/// anything writes one.
#[test]
fn a_keyed_record_round_trips_and_unknown_fields_are_tolerated() {
    let keyed = EventRecord::new(7, "exec-1", "txn-1", "body").with_event_id("evt-42");
    let json = serde_json::to_string(&keyed).unwrap();
    assert!(json.contains("evt-42"));
    assert_eq!(serde_json::from_str::<EventRecord>(&json).unwrap(), keyed);

    let from_future = r#"{"global_sequence":7,"execution_id":"e","transaction_id":"t","payload":"p","some_later_column":123}"#;
    serde_json::from_str::<EventRecord>(from_future).expect(
        "an unknown column must not fail the read, or the next additive change is a rollback trap",
    );
}

/// ⚠ The idempotency window is a CAPACITY, not a guarantee — pinned so the limit
/// is a known property rather than a surprise.
#[test]
fn the_window_is_bounded_and_says_so_when_it_forgets() {
    let tmp = scratch("window");
    let mut e = engine_at(&tmp, Some(4));

    for i in 0..6 {
        e.append_writer_assigned(ev("exec-w", &format!("evt-{i}"), "p"))
            .unwrap();
    }
    // evt-0 has been forgotten; its redelivery is NOT deduplicated.
    e.append_writer_assigned(ev("exec-w", "evt-0", "p"))
        .unwrap();
    assert_eq!(
        read_all(&e, "exec-w").len(),
        7,
        "a redelivery past the window is a duplicate — the honest limit"
    );
    assert!(
        e.metrics()
            .dedupe_window_evictions
            .load(std::sync::atomic::Ordering::Relaxed)
            > 0,
        "an undersized window must be visible, or it looks exactly like a working one"
    );
}

/// ⚠ Dedupe OFF must be byte-for-byte today's behaviour, or "off" is not a real
/// rollback.
#[test]
fn capacity_zero_restores_the_pre_change_behaviour_exactly() {
    let tmp = scratch("off");
    let mut e = engine_at(&tmp, Some(0));

    e.append_writer_assigned(ev("exec-z", "evt-1", "p"))
        .unwrap();
    e.append_writer_assigned(ev("exec-z", "evt-1", "p"))
        .unwrap();
    assert_eq!(
        read_all(&e, "exec-z").len(),
        2,
        "with dedupe disabled the redelivery must duplicate exactly as before"
    );
    assert_eq!(
        e.metrics()
            .dedupe_hits
            .load(std::sync::atomic::Ordering::Relaxed),
        0
    );
}

/// ⭐ GATE 1b — the OTHER public append path is deduplicated too.
///
/// `append_record` is public API for callers that own their sort keys, and the
/// worker calls it directly. It needs its own test because the two guards mask
/// each other: with only `append_writer_assigned` tests, removing the
/// `append_record` guard left every test green — each entry point was covered
/// only by the other one's protection.
///
/// ⚠ Mutation verified: removing the `dedupe_hit` guard from `append_record`
/// fails this (2 records, not 1).
#[test]
fn the_caller_supplied_key_path_is_deduplicated_too() {
    let tmp = scratch("direct");
    let mut e = engine(&tmp);

    let first = e
        .append_record(ev("exec-d", "evt-d", "body").tap_seq(10))
        .expect("first append");
    let second = e
        .append_record(ev("exec-d", "evt-d", "body").tap_seq(10))
        .expect("redelivery must be acknowledged, not error");

    assert_eq!(second, first, "must acknowledge the existing position");
    assert_eq!(
        read_all(&e, "exec-d").len(),
        1,
        "append_record must deduplicate too — it is public API and the worker \
         calls it directly"
    );
    assert_eq!(
        e.metrics()
            .out_of_order_appends
            .load(std::sync::atomic::Ordering::Relaxed),
        0,
        "the redelivery reuses its sort key, so without dedupe it would trip the \
         canary and land behind the follower cursor — the silent-drop path"
    );
}

// ⚠⚠ KNOWN COVERAGE GAP — the remember-AFTER-append ordering is NOT tested.
//
// `append_record` deliberately calls `dedupe.remember` only after
// `writer.append` succeeds: remembering first would let a FAILED append poison
// its key, so the retry that the failure exists to invite would be answered
// "already present" for a record that is not there — silent loss, the exact
// class this module removes.
//
// A mutation moving `remember` before the append leaves every test here green.
// The reason is structural rather than an oversight in the tests: the engine
// exposes no seam to make an append fail deterministically. `writer.append`
// fails only on a real I/O error, and the usual tricks do not reach it — on Unix
// a write to an already-open descriptor succeeds after the file or directory is
// made read-only or unlinked, and `ensure_writer` (the one step that can fail on
// permissions) runs BEFORE the point the mutation moves.
//
// So this property currently rests on the code and its comment, not on a test.
// Closing it needs a fault-injection seam on the write path — worth having
// anyway, because it would also make the `ingest_append_failed` path from
// ehdb#345 testable, which is likewise only reachable today by filling a disk.
// Recorded here rather than left as an implied guarantee.

/// Test-only helper: set an explicit sort key for the caller-supplied path.
trait TapSeq {
    fn tap_seq(self, seq: u64) -> Self;
}
impl TapSeq for EventRecord {
    fn tap_seq(mut self, seq: u64) -> Self {
        self.global_sequence = seq;
        self
    }
}
