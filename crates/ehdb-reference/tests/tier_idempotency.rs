//! **noetl/ai-meta#313 — idempotency on the store the duplicates were actually in.**
//!
//! The first attempt at this built dedupe into `ehdb-l0`'s `L0Engine`. That was
//! the wrong store: the event-log **tier** — where #313 observed 11 duplicates,
//! and which is configured `primary` — is served by
//! `LocalReferenceEventLogDriver`, which never constructs an `L0 EventRecord` at
//! all. These tests run against that driver.

use ehdb_reference::eventlog::{
    EventLogAppendRequest, EventLogDriver, EventLogScanRequest, LocalReferenceEventLogDriver,
};
use ehdb_reference::{DEFAULT_LOCAL_REFERENCE_NAMESPACE, DEFAULT_LOCAL_REFERENCE_TENANT};
use std::sync::atomic::{AtomicU32, Ordering};

static N: AtomicU32 = AtomicU32::new(0);

fn driver(tag: &str) -> LocalReferenceEventLogDriver {
    let n = N.fetch_add(1, Ordering::SeqCst);
    let dir =
        std::env::temp_dir().join(format!("ehdb-tier-dedupe-{tag}-{}-{n}", std::process::id()));
    LocalReferenceEventLogDriver::new(
        dir,
        DEFAULT_LOCAL_REFERENCE_TENANT.to_string(),
        DEFAULT_LOCAL_REFERENCE_NAMESPACE.to_string(),
    )
}

/// A payload in the shape the server's `mirror_payload` actually emits.
fn payload(event_id: &str, body: &str) -> String {
    format!(r#"{{"event_id":"{event_id}","event_type":"x","body":"{body}"}}"#)
}

fn req(exec: &str, event_id: Option<&str>, body: &str) -> EventLogAppendRequest {
    EventLogAppendRequest {
        execution_id: exec.to_string(),
        transaction_id: format!("txn-{body}"),
        // ⚠ A keyless request carries NO `event_id` in its payload either. A
        // pre-#313 record genuinely has none, and an earlier version of this
        // helper stamped a placeholder into the body — which made the keyless
        // tests model a shape that never occurs.
        payload: match event_id {
            Some(id) => payload(id, body),
            None => format!(r#"{{"event_type":"x","body":"{body}"}}"#),
        },
        event_id: event_id.map(str::to_string),
    }
}

fn count(d: &LocalReferenceEventLogDriver) -> usize {
    d.scan_global(&EventLogScanRequest {
        after: None,
        limit: 10_000,
    })
    .expect("scan")
    .record_count
}

/// ⭐ GATE 1 — a redelivery is deduplicated and ACKNOWLEDGED at the existing
/// position, never silently dropped.
///
/// ⚠ Mutation verified: removing the dedupe block from
/// `LocalReferenceEventLogDriver::append` fails this on the record count (2).
#[test]
fn a_redelivered_event_is_deduplicated_and_acknowledged() {
    let d = driver("dedupe");
    let first = d.append(&req("exec-1", Some("evt-1"), "a")).expect("first");
    assert!(!first.deduplicated, "a first delivery is a write");

    let second = d
        .append(&req("exec-1", Some("evt-1"), "a"))
        .expect("redelivery");
    assert!(
        second.deduplicated,
        "the redelivery must report deduplicated=true — without it the caller's \
         monotonicity check reports a divergence for a dedupe that worked"
    );
    assert_eq!(
        second.global_sequence, first.global_sequence,
        "it must acknowledge the EXISTING position"
    );
    assert_eq!(
        count(&d),
        1,
        "exactly one copy — a second is the #313 duplicate"
    );
    assert_eq!(
        second.log_record_count, first.log_record_count,
        "a dedupe must not advance the record count"
    );
}

/// ⚠⚠ GATE 2 — THE NEGATIVE CONTROL. A genuinely new event still appends.
///
/// The worst and quietest failure would be a dedupe that swallows legitimate new
/// events: silent write-loss on a `primary`-serving tier. Without this, an
/// implementation that deduplicates *everything* passes gate 1 perfectly.
///
/// ⚠ Mutation verified: making the key lookup return `Some(1)` unconditionally
/// passes gate 1 and fails here.
#[test]
fn a_new_event_still_appends() {
    let d = driver("new");
    let a = d.append(&req("exec-2", Some("evt-a"), "a")).unwrap();
    let b = d.append(&req("exec-2", Some("evt-b"), "b")).unwrap();
    let c = d.append(&req("exec-2", Some("evt-c"), "c")).unwrap();

    assert!(!a.deduplicated && !b.deduplicated && !c.deduplicated);
    assert!(a.global_sequence < b.global_sequence && b.global_sequence < c.global_sequence);
    assert_eq!(
        count(&d),
        3,
        "three distinct events must yield three records"
    );
}

/// ⭐ GATE 3 — the #313 scenario, reproduced and closed ON THE TIER.
#[test]
fn the_313_re_mirror_scenario_is_closed_on_the_tier() {
    let d = driver("s313");
    let batch: Vec<_> = (0..11).map(|i| format!("evt-{i}")).collect();

    for id in &batch {
        d.append(&req("exec-313", Some(id), id)).unwrap();
    }
    let dupes: usize = batch
        .iter()
        .map(|id| {
            usize::from(
                d.append(&req("exec-313", Some(id), id))
                    .unwrap()
                    .deduplicated,
            )
        })
        .sum();
    d.append(&req("exec-313", Some("evt-new"), "new")).unwrap();
    for id in &batch {
        d.append(&req("exec-313", Some(id), id)).unwrap();
    }

    assert_eq!(
        dupes, 11,
        "every record of the redelivered batch must dedupe"
    );
    assert_eq!(
        count(&d),
        12,
        "11 originals + 1 new. Before this fix two full redeliveries made it 34"
    );
}

/// ⭐ GATE 4 — MIGRATION: records written before the key existed still work.
///
/// A pre-#313 record carries no `event_id`, and a producer that has not been
/// updated sends `None`. Both must append normally and never collapse.
///
/// ⚠ Mutation verified by the "dedupe everything" mutation (see gate 2), which
/// fails this test. Note that merely substituting a WRONG key for `None` — e.g.
/// falling back to `execution_id` — is a no-op rather than a defect: the needle
/// is built from the key and simply matches nothing, so behaviour is unchanged.
/// The failure that matters is dedupe firing without a key at all.
#[test]
fn keyless_records_append_and_never_deduplicate() {
    let d = driver("legacy");
    // Pre-existing records, written the way every current caller writes them.
    for i in 0..3 {
        let out = d.append(&req("exec-old", None, &format!("p{i}"))).unwrap();
        assert!(!out.deduplicated);
    }
    assert_eq!(count(&d), 3, "keyless records must never be collapsed");

    // And a keyed append still works against a log that already holds keyless ones.
    let keyed = d.append(&req("exec-old", Some("evt-new"), "new")).unwrap();
    assert!(!keyed.deduplicated);
    assert_eq!(
        count(&d),
        4,
        "the tier reads and appends fine after the change"
    );
}

/// ⚠ The `deduplicated=false` outcome must serialise BYTE-IDENTICALLY to before
/// the field existed — that is what makes the pre-activation deploy
/// rollback-safe in both directions.
#[test]
fn a_non_deduplicated_outcome_is_wire_identical_to_before() {
    let d = driver("wire");
    let out = d.append(&req("exec-w", None, "a")).unwrap();
    let json = serde_json::to_string(&out).unwrap();
    assert!(
        !json.contains("deduplicated"),
        "a false outcome must not emit the field, or every existing reply changes \
         shape and a rollback cannot parse them: {json}"
    );

    // The pre-field wire form still parses, and an unknown future field does not
    // break the read — `deny_unknown_fields` had to go before anything wrote one.
    let legacy = r#"{"action":"eventlog-append","execution_id":"e","global_sequence":7,"byte_len":3,"created_stream":false,"log_record_count":7}"#;
    let parsed: ehdb_reference::eventlog::EventLogAppendOutcome =
        serde_json::from_str(legacy).expect("legacy reply must parse");
    assert!(!parsed.deduplicated, "absent means false");

    let from_future = r#"{"action":"eventlog-append","execution_id":"e","global_sequence":7,"byte_len":3,"created_stream":false,"log_record_count":7,"some_later_field":1}"#;
    serde_json::from_str::<ehdb_reference::eventlog::EventLogAppendOutcome>(from_future)
        .expect("an unknown field must not fail the read");
}
