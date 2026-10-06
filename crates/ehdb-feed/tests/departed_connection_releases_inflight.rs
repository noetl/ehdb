//! A record held by a connection that is GONE must not wait out `ack_wait`.
//!
//! The `ack_wait` timer bounds how long a **live** consumer may sit on a record.
//! When the consumer has vanished — pod killed, autoscale-down, SIGKILL, a rolled
//! deployment — waiting it out buys nothing and costs a full `ack_wait` of dead
//! time before anyone else can have the record.
//!
//! Measured on kind (2026-09-28) by killing the system-pool pod mid-flight:
//! executions stalled **30.14s / 71.27s / 45.52s / 30.29s** against a 0.49-0.74s
//! baseline, every one of them on `command.completed -> step.enter`, with claim
//! latency itself steady at 27ms p50. The stall is in ASSIGNMENT, not claiming:
//! the orchestrate command sat in the group's `inflight` map under a member whose
//! connection had already closed. Prod's user pool autoscales 2<->20, so this
//! fires on every scale event.
//!
//! The fix is `ClaimCoordinator::release_conn`, called on every exit path of the
//! per-connection loop. It is keyed by CONNECTION, not member, because
//! `EhdbCommandSource` opens two connections under one `MemberId` (one to claim,
//! one to ack) and releasing by member would yank records the claim connection is
//! legitimately working on.

use std::sync::Arc;
use std::time::Duration;

use ehdb_feed::{ClaimCoordinator, FeedWriter};
use ehdb_l0::substrate::DurableSubstrate;
use ehdb_l0::{D1EventLog, EventRecord, FlushPolicy, L0Config, L0Engine, LocalFsSubstrate};

fn unique_dir(tag: &str) -> std::path::PathBuf {
    static N: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let n = N.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    std::env::temp_dir().join(format!("ehdb-feed-depart-{tag}-{}-{n}", std::process::id()))
}

fn cmd(id: u64) -> EventRecord {
    let payload = serde_json::json!({
        "execution_id": id,
        "command_id": format!("cmd-{id}"),
        "step": "start",
        "execution_pool": "shared",
    })
    .to_string();
    EventRecord::new(id, format!("exec-{id}"), "t", payload)
}

async fn coord_with(
    ack_wait: Duration,
) -> (
    Arc<FeedWriter<D1EventLog>>,
    Arc<ClaimCoordinator<D1EventLog>>,
) {
    let (obj, local) = (unique_dir("obj"), unique_dir("local"));
    let store: Arc<dyn DurableSubstrate> = Arc::new(LocalFsSubstrate::new(&obj).unwrap());
    let engine = L0Engine::<D1EventLog>::open(
        L0Config::d1(&local).with_flush(FlushPolicy::Buffered { fsync_every: 64 }),
        store,
    )
    .unwrap();
    let writer = Arc::new(FeedWriter::new(engine));
    let coord = Arc::new(ClaimCoordinator::new(
        writer.clone(),
        0,
        ack_wait,
        0,
        ehdb_feed::d1_command_subject(1),
    ));
    (writer, coord)
}

const FILTER: &str = "commands.shared.>";

/// The defect and the fix, on the real prod `ack_wait` of 30s.
///
/// A 30s `ack_wait` is what prod runs (`NOETL_COMMAND_BUS_ACK_WAIT_SECS=30`), so
/// the test uses it: if the release does not happen, the second claim cannot
/// possibly succeed inside the 3s bound, which is the RED. It is a bound, not a
/// sleep — the GREEN path returns in microseconds.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_departed_connections_record_is_reclaimable_at_once() {
    let (writer, coord) = coord_with(Duration::from_secs(30)).await;
    writer.append(cmd(100)).unwrap();

    // Connection 7 claims it and then goes away without acking.
    // NB: sort keys are WRITER-assigned on append, not the payload id, so the
    // record is tracked by the key the coordinator actually handed out.
    let first = coord.claim_next_on(FILTER, 1, 7).await;
    assert!(!first.redelivered, "a first delivery is not a redelivery");

    // Nobody else can have it while connection 7 holds it — the exactly-once half.
    assert!(
        tokio::time::timeout(Duration::from_millis(300), coord.claim_next(FILTER, 2))
            .await
            .is_err(),
        "a record in flight under a LIVE holder must not be handed to anyone else"
    );

    // Connection 7 is gone.
    let released = coord.release_conn(7).await;
    assert_eq!(
        released, 1,
        "the departed connection's one record is released"
    );

    // THE POINT: immediately reclaimable, not after 30s.
    let second = tokio::time::timeout(Duration::from_secs(3), coord.claim_next(FILTER, 2))
        .await
        .expect("a released record must be reclaimable AT ONCE, not after ack_wait");
    assert_eq!(
        second.sort_key, first.sort_key,
        "the same record comes back"
    );
    assert!(
        second.redelivered,
        "it must be marked redelivered — a consumer has to be able to tell a \
         retry from a first delivery, so the release must not launder it into a \
         fresh delivery"
    );
}

/// Releasing a connection must not touch another connection's records.
///
/// This is the guard that keeps the fix from becoming a duplicate-work bug: one
/// `MemberId` legitimately holds two connections, so a release must be surgical.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn releasing_one_connection_leaves_the_others_alone() {
    let (writer, coord) = coord_with(Duration::from_secs(30)).await;
    writer.append(cmd(100)).unwrap();
    writer.append(cmd(200)).unwrap();

    // Same member, two connections — exactly the EhdbCommandSource shape.
    let a = coord.claim_next_on(FILTER, 1, 11).await;
    let b = coord.claim_next_on(FILTER, 1, 12).await;
    assert_ne!(a.sort_key, b.sort_key, "two records, two connections");

    // Connection 11 departs. Connection 12 is still working.
    assert_eq!(
        coord.release_conn(11).await,
        1,
        "only 11's record is released"
    );

    let back = tokio::time::timeout(Duration::from_secs(3), coord.claim_next(FILTER, 2))
        .await
        .expect("11's record is reclaimable");
    assert_eq!(back.sort_key, a.sort_key, "and it is 11's record, not 12's");

    // 12's record must still be exclusively 12's.
    assert!(
        tokio::time::timeout(Duration::from_millis(300), coord.claim_next(FILTER, 3))
            .await
            .is_err(),
        "releasing connection 11 must NOT free connection 12's record — one member \
         holds a claim connection AND an ack connection, and freeing the live one's \
         work would manufacture duplicate execution"
    );
}

/// An ack settles the lease, so a later release of that connection is a no-op.
///
/// Without this, a connection that acked cleanly and then closed would "release"
/// an already-acked sort key. `expire_now` makes that harmless, but the lease
/// must not leak either.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_acked_record_is_not_released_again() {
    let (writer, coord) = coord_with(Duration::from_secs(30)).await;
    writer.append(cmd(100)).unwrap();

    let d = coord.claim_next_on(FILTER, 1, 21).await;
    assert!(coord.ack(d.sort_key).await, "the ack lands");
    assert_eq!(
        coord.release_conn(21).await,
        0,
        "an acked record is settled; closing the connection releases nothing"
    );
}

/// Releasing a connection that never claimed anything is safe, and conn 0 means
/// "no lease tracking" so in-process callers keep today's behaviour exactly.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn release_is_safe_for_unknown_and_untracked_connections() {
    let (writer, coord) = coord_with(Duration::from_secs(30)).await;
    writer.append(cmd(100)).unwrap();

    assert_eq!(
        coord.release_conn(999).await,
        0,
        "unknown connection: no-op"
    );

    // conn 0 = untracked (the plain `claim_next` path).
    let _d = coord.claim_next_on(FILTER, 1, 0).await;
    assert_eq!(
        coord.release_conn(0).await,
        0,
        "conn 0 is the untracked sentinel and must never release anything"
    );
}
