//! The write path's FAILURE behaviour, which was previously untestable.
//!
//! Before the fault seam (`ehdb_l0::fault`) nothing could make
//! `PartWriter::append` fail on demand: on Unix a write to an already-open
//! descriptor survives `chmod` and `unlink`, and `ensure_writer` — the one step
//! that can fail on permissions — runs before the point under test. So the
//! properties below rested on code review alone.

use ehdb_l0::dataset::{D1EventLog, EventRecord};
use ehdb_l0::engine::{L0Config, L0Engine};
use ehdb_l0::fault;
use ehdb_l0::substrate::{DurableSubstrate, LocalFsSubstrate};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

static N: AtomicU32 = AtomicU32::new(0);

fn engine(tag: &str) -> L0Engine<D1EventLog> {
    let n = N.fetch_add(1, Ordering::SeqCst);
    let dir = std::env::temp_dir().join(format!("ehdb-fault-{tag}-{}-{n}", std::process::id()));
    let store: Arc<dyn DurableSubstrate> =
        Arc::new(LocalFsSubstrate::new(dir.join("obj")).unwrap());
    L0Engine::open(L0Config::d1(dir.join("local")), store).unwrap()
}

fn ev(exec: &str, id: &str) -> EventRecord {
    EventRecord::new(0, exec, "t", "p").with_event_id(id)
}

fn read_all(e: &L0Engine<D1EventLog>, exec: &str) -> Vec<EventRecord> {
    e.read_index_after(exec, 0).expect("read")
}

/// ⭐⭐ THE GATE THAT COULD NOT BE WRITTEN BEFORE.
///
/// `append_record` must remember a dedupe key only **after** the write succeeds.
/// Remembering first lets a FAILED append poison its key: the retry that the
/// failure exists to invite is then answered "already present" for a record that
/// is not there — silent loss, on the write path of a serving tier.
///
/// noetl/ai-meta#313 shipped with this property asserted only by a comment,
/// because a mutation moving `remember` earlier left every test green.
///
/// ⚠ Mutation verified: moving `dedupe.remember(..)` above `writer.append(..)`
/// in `append_record` fails this — the retry finds the poisoned key, appends
/// nothing, and the log ends up empty while the caller was told a position.
#[test]
fn a_failed_append_does_not_poison_its_dedupe_key() {
    // These tests share one process-global seam, so they must not run
    // concurrently with each other. `cargo test` does not serialise within a
    // binary, so this file keeps exactly one seam-arming test.
    let mut e = engine("poison");

    fault::fail_next_appends(1);
    let err = e.append_writer_assigned(ev("exec-p", "evt-p"));
    assert!(err.is_err(), "the seam must actually fail the append");
    assert_eq!(
        fault::pending_injected_failures(),
        0,
        "the injection must have been CONSUMED — an injection that silently did \
         nothing would make the assertions below pass for the wrong reason"
    );
    assert!(
        read_all(&e, "exec-p").is_empty(),
        "nothing was written, so nothing may be readable"
    );

    // The retry. This is the whole point: it must actually land.
    let seq = e
        .append_writer_assigned(ev("exec-p", "evt-p"))
        .expect("the retry must succeed");
    let all = read_all(&e, "exec-p");
    assert_eq!(
        all.len(),
        1,
        "the retry after a failed append must WRITE the record. If the key was \
         remembered before the failed write, this is 0 — the caller holds a \
         position for a record that does not exist"
    );
    assert_eq!(all[0].event_id.as_deref(), Some("evt-p"));
    assert!(seq > 0);

    // And the record is now genuinely deduplicated, so the seam did not disable
    // the mechanism it was testing.
    e.append_writer_assigned(ev("exec-p", "evt-p")).unwrap();
    assert_eq!(read_all(&e, "exec-p").len(), 1, "still exactly one copy");
    assert_eq!(
        e.metrics().snapshot().dedupe_hits,
        1,
        "the post-retry redelivery must be the ONLY dedupe hit — the failed \
         append must not have counted as one"
    );

    // Phase 2: the negative control, in the same function so it cannot race.
    the_seam_is_inert_when_nothing_arms_it();
}

// ⚠ The negative control is a PHASE of the test above, not its own `#[test]`.
// The seam is process-global and `cargo test` does not serialise within a
// binary, so a separate test asserting "the seam is disarmed" races the one that
// arms it. That is not hypothetical — it failed in the workspace run while
// passing under `--test-threads=1`.
fn the_seam_is_inert_when_nothing_arms_it() {
    let mut e = engine("inert");
    assert_eq!(
        fault::pending_injected_failures(),
        0,
        "the arming phase must have left the seam disarmed"
    );
    for i in 0..5 {
        e.append_writer_assigned(ev("exec-i", &format!("e{i}")))
            .expect("an unarmed seam must never fail an append");
    }
    assert_eq!(read_all(&e, "exec-i").len(), 5);
}
