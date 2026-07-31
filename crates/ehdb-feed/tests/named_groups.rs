//! **T3 named-group semantics** — the events feed's delivery contract.
//!
//! The events path stacks two relationships that the command bus never needed
//! together (noetl/ai-meta#212):
//!
//! - **between** groups: fan-out — `noetl_materializer`,
//!   `noetl_result_materializer` and `noetl_state_materializer` each see *every*
//!   event, on their own cursor;
//! - **within** a group: queue-group — the two system-pool replicas compete, and
//!   an unacked record redelivers.
//!
//! These tests pin exactly that, plus the durability seam (a restart resumes per
//! group rather than replaying) and the isolation guarantee (a subject filter is
//! honoured per group). They are the gate the design doc's "no drops, ordering
//! acceptable for each consumer" parity claim reduces to.

use std::sync::Arc;
use std::time::Duration;

use ehdb_feed::cursor::CursorFallback;
use ehdb_feed::{event_feed_subject, FeedWriter, GroupCoordinator};
use ehdb_l0::{D1EventLog, EventRecord, L0Config, L0Engine, LocalFsSubstrate};

const MATERIALIZER: &str = "noetl_materializer";
const RESULT_MATERIALIZER: &str = "noetl_result_materializer";
const STATE_MATERIALIZER: &str = "noetl_state_materializer";
const ALL_EVENTS: &str = "events.>";

fn tmpdir(tag: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "ehdb-t3-{tag}-{}-{:?}",
        std::process::id(),
        std::thread::current().id()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn writer(dir: &std::path::Path) -> Arc<FeedWriter<D1EventLog>> {
    let store = Arc::new(LocalFsSubstrate::new(dir).unwrap());
    let engine =
        L0Engine::<D1EventLog>::open(L0Config::d1(dir).with_shard_count(1), store).unwrap();
    Arc::new(FeedWriter::new(engine))
}

fn event(seq: u64, exec: &str, event_type: &str) -> EventRecord {
    EventRecord::new(
        seq,
        exec,
        format!("tx-{seq}"),
        format!(r#"{{"event_type":"{event_type}","execution_id":"{exec}"}}"#),
    )
}

fn coordinator(
    w: &Arc<FeedWriter<D1EventLog>>,
    dir: &std::path::Path,
) -> Arc<GroupCoordinator<D1EventLog>> {
    Arc::new(GroupCoordinator::new(
        Arc::clone(w),
        0,
        Duration::from_millis(200),
        event_feed_subject(),
        Some(dir.to_path_buf()),
        // Beginning: these tests append first, then open groups, and want the
        // appended records delivered. Prod uses Tail.
        CursorFallback::Beginning,
    ))
}

/// The headline contract: three groups over one log each receive **every**
/// record. This is what makes an EHDB events feed a drop-in for three JetStream
/// durable consumers on one stream.
#[tokio::test]
async fn every_group_sees_every_record() {
    let dir = tmpdir("fanout");
    let w = writer(&dir);
    for i in 1..=5 {
        w.append(event(i, "exec-a", "action_started")).unwrap();
    }
    let c = coordinator(&w, &dir);

    for group in [MATERIALIZER, RESULT_MATERIALIZER, STATE_MATERIALIZER] {
        let mut got = Vec::new();
        for _ in 0..5 {
            let d = c.claim_next(group, ALL_EVENTS, 1).await;
            c.ack(group, d.sort_key).await;
            got.push(d.sort_key);
        }
        assert_eq!(got.len(), 5, "{group} should see all 5 records");
        let mut sorted = got.clone();
        sorted.sort_unstable();
        assert_eq!(got, sorted, "{group} must receive records in log order");
    }

    std::fs::remove_dir_all(&dir).ok();
}

/// Within one group, two members compete: each record goes to exactly one of
/// them, and between them they see the whole log with no duplicates. This is the
/// two-system-pool-pods case.
#[tokio::test]
async fn members_of_one_group_compete_exactly_once() {
    let dir = tmpdir("compete");
    let w = writer(&dir);
    for i in 1..=10 {
        w.append(event(i, "exec-b", "action_completed")).unwrap();
    }
    let c = coordinator(&w, &dir);

    let mut seen = Vec::new();
    for i in 0..10 {
        // Alternate the claiming member, as two competing pods would.
        let member = if i % 2 == 0 { 1 } else { 2 };
        let d = c.claim_next(MATERIALIZER, ALL_EVENTS, member).await;
        c.ack(MATERIALIZER, d.sort_key).await;
        seen.push(d.sort_key);
    }

    let mut deduped = seen.clone();
    deduped.sort_unstable();
    deduped.dedup();
    assert_eq!(
        deduped.len(),
        10,
        "no record may be delivered twice: {seen:?}"
    );

    std::fs::remove_dir_all(&dir).ok();
}

/// A stalled group must not pin another group's progress. This is the failure
/// mode that makes a single shared cursor wrong for the events path: if the
/// result materializer wedges, the durable-log materializer must keep draining.
#[tokio::test]
async fn a_stalled_group_does_not_block_another() {
    let dir = tmpdir("stall");
    let w = writer(&dir);
    for i in 1..=4 {
        w.append(event(i, "exec-c", "action_started")).unwrap();
    }
    let c = coordinator(&w, &dir);

    // RESULT group claims one record and never acks it — wedged in flight.
    let stuck = c.claim_next(RESULT_MATERIALIZER, ALL_EVENTS, 9).await;
    assert_eq!(stuck.sort_key, 1);

    // The durable-log group drains everything regardless.
    for expected in 1..=4 {
        let d = c.claim_next(MATERIALIZER, ALL_EVENTS, 1).await;
        assert_eq!(d.sort_key, expected);
        c.ack(MATERIALIZER, d.sort_key).await;
    }
    assert_eq!(
        c.lag(MATERIALIZER).await,
        0,
        "drained group must report 0 lag"
    );
    assert!(
        c.lag(RESULT_MATERIALIZER).await > 0,
        "wedged group must still report backlog"
    );

    std::fs::remove_dir_all(&dir).ok();
}

/// An unacked record redelivers after `ack_wait` — at-least-once within a group,
/// so a pod that dies mid-materialize loses nothing.
#[tokio::test]
async fn unacked_record_redelivers_within_a_group() {
    let dir = tmpdir("redeliver");
    let w = writer(&dir);
    w.append(event(1, "exec-d", "action_started")).unwrap();
    let c = coordinator(&w, &dir);

    let first = c.claim_next(MATERIALIZER, ALL_EVENTS, 1).await;
    assert!(!first.redelivered);

    // Member 1 "dies" without acking; after ack_wait member 2 gets it.
    tokio::time::sleep(Duration::from_millis(250)).await;
    let again = c.claim_next(MATERIALIZER, ALL_EVENTS, 2).await;
    assert_eq!(again.sort_key, first.sort_key);
    assert!(
        again.redelivered,
        "an expired in-flight record must redeliver"
    );

    std::fs::remove_dir_all(&dir).ok();
}

/// Per-group cursors persist, so a writer restart resumes each group where it
/// was instead of replaying the retained log into every materializer.
#[tokio::test]
async fn each_group_resumes_from_its_own_persisted_cursor() {
    let dir = tmpdir("resume");
    let w = writer(&dir);
    for i in 1..=6 {
        w.append(event(i, "exec-e", "action_started")).unwrap();
    }

    {
        let c = coordinator(&w, &dir);
        // The durable-log group drains all 6; the state group drains 2.
        for _ in 0..6 {
            let d = c.claim_next(MATERIALIZER, ALL_EVENTS, 1).await;
            c.ack(MATERIALIZER, d.sort_key).await;
        }
        for _ in 0..2 {
            let d = c.claim_next(STATE_MATERIALIZER, ALL_EVENTS, 1).await;
            c.ack(STATE_MATERIALIZER, d.sort_key).await;
        }
        c.checkpoint().await.unwrap();
        assert_eq!(c.cursor_errors(), 0, "cursor persistence must not error");
    }

    // Rebuild the coordinator over the same log + cursor dir — the restart.
    let c2 = coordinator(&w, &dir);
    let drained = c2.open_group(MATERIALIZER).await;
    let partial = c2.open_group(STATE_MATERIALIZER).await;

    assert_eq!(
        drained.from_cursor, 6,
        "fully-drained group resumes at the tip"
    );
    assert!(
        !drained.replayed(),
        "fully-drained group must replay nothing"
    );
    assert_eq!(
        partial.from_cursor, 2,
        "partial group resumes at its own cursor"
    );
    assert_eq!(
        partial.replay_records(),
        4,
        "partial group re-serves exactly its undrained tail"
    );

    // And the undrained tail really is what arrives.
    let next = c2.claim_next(STATE_MATERIALIZER, ALL_EVENTS, 1).await;
    assert_eq!(next.sort_key, 3);

    std::fs::remove_dir_all(&dir).ok();
}

/// A subject filter scopes a group to a slice of the event stream, and a record
/// outside it is never handed over — the isolation guarantee, per group.
#[tokio::test]
async fn subject_filter_scopes_delivery_within_a_group() {
    let dir = tmpdir("filter");
    let w = writer(&dir);
    w.append(event(1, "x", "action_started")).unwrap();
    w.append(event(2, "x", "result_written")).unwrap();
    w.append(event(3, "x", "action_started")).unwrap();
    let c = coordinator(&w, &dir);

    // A group subscribed only to result_written sees exactly record 2.
    let d = c
        .claim_next(RESULT_MATERIALIZER, "events.result_written", 1)
        .await;
    assert_eq!(d.sort_key, 2);
    c.ack(RESULT_MATERIALIZER, d.sort_key).await;

    // A wildcard group still sees all three.
    let mut all = Vec::new();
    for _ in 0..3 {
        let d = c.claim_next(MATERIALIZER, ALL_EVENTS, 1).await;
        c.ack(MATERIALIZER, d.sort_key).await;
        all.push(d.sort_key);
    }
    assert_eq!(all, vec![1, 2, 3]);

    std::fs::remove_dir_all(&dir).ok();
}

/// A cursor persisted above the reopened log's tip is clamped down rather than
/// trusted — otherwise every record below it is silently skipped
/// (noetl/ai-meta#208, applied per group).
#[tokio::test]
async fn a_cursor_past_the_tip_is_clamped_not_trusted() {
    let dir = tmpdir("clamp");
    let w = writer(&dir);
    w.append(event(1, "y", "action_started")).unwrap();
    w.append(event(2, "y", "action_started")).unwrap();

    // Forge a cursor far past the tip, as a replaced volume would leave behind.
    let store = ehdb_feed::cursor::CursorStore::open_named(&dir, MATERIALIZER, 0).unwrap();
    store.store(9_999).unwrap();

    let c = coordinator(&w, &dir);
    let report = c.open_group(MATERIALIZER).await;

    assert_eq!(report.stored_cursor, Some(9_999));
    assert!(
        report.clamped(),
        "a cursor past the tip must report as clamped"
    );
    assert_eq!(
        report.from_cursor, report.tip,
        "clamped cursor must land on the tip, never above it"
    );

    std::fs::remove_dir_all(&dir).ok();
}
