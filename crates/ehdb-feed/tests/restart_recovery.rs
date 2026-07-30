//! **Writer-restart recovery proof (noetl/ai-meta#208).**
//!
//! Restarting only the per-shard writer — a node drain, an image bump, an OOM —
//! left the command bus not delivering, in two independent ways. T5 deletes NATS,
//! so there is no bus to fail back to; both have to be structurally fixed.
//!
//! **Defect 1 — claimers never noticed.** `claim_next` blocks until a command is
//! available, so when the writer's pod went away the socket was left half-open and
//! the read neither returned data nor errored. Every redial path in the crate and
//! its callers is downstream of an `Err` that never came, so workers parked
//! forever: `0 of 30` commands claimed, nothing in any log.
//! [`half_open_coordinator_wedges_without_heartbeats`] reproduces exactly that
//! against a relay that holds the client socket open and stops forwarding, and
//! [`half_open_coordinator_surfaces_as_a_read_error`] /
//! [`client_redials_a_restarted_coordinator_and_dispatch_resumes`] show the fixed
//! path: an error within a few heartbeats, a redial, and dispatch resuming with
//! nothing lost.
//!
//! **Defect 2 — the restarted writer replayed its whole shard.** The coordinator
//! was rebuilt at `from_cursor = 0`, so every record still in the log was
//! re-delivered (in kind: `ehdb_feed_shard_lag{shard="0"} 2738`, draining at
//! ~1 record/s because each stale record costs a control-plane round-trip to learn
//! it is already claimed). [`restart_resumes_from_the_committed_cursor`] asserts
//! the resumed coordinator starts at the persisted cursor with **zero** lag while
//! a `from_cursor = 0` coordinator over the same log still sees the full replay —
//! the fix and the defect measured side by side — and
//! [`restart_still_redelivers_everything_unacked`] pins the direction the cursor is
//! allowed to be wrong: unacked work always comes back.

use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use ehdb_feed::{
    ClaimClient, ClaimCoordinator, CursorFallback, CursorOrigin, CursorStore, FeedWriter,
};
use ehdb_l0::substrate::DurableSubstrate;
use ehdb_l0::{D1EventLog, EventRecord, L0Config, L0Engine, LocalFsSubstrate};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

const ACK_WAIT: Duration = Duration::from_secs(30);
const FILTER: &str = "commands.shared.>";

fn unique_dir(tag: &str) -> std::path::PathBuf {
    static N: AtomicU64 = AtomicU64::new(0);
    let n = N.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir().join(format!(
        "ehdb-feed-restart-{tag}-{}-{n}",
        std::process::id()
    ))
}

/// A command record. Its own `global_sequence` is *not* what identifies it after
/// the append: the writer re-keys every record it accepts (the #203 contract), so
/// `payload` carries the test's own id and assertions use the writer-assigned key.
fn ev(id: u64) -> EventRecord {
    EventRecord::new(
        id,
        format!("exec-{id}"),
        "command",
        format!(r#"{{"id":{id}}}"#),
    )
}

/// Open (or **re**open, after a "restart") the shard's durable log as a writer.
/// The same pair of directories is the writer's volume: reopening replays what the
/// previous process committed, which is what makes the restart scenarios real
/// rather than mocked.
fn writer_over(local: &Path, obj: &Path) -> Arc<FeedWriter<D1EventLog>> {
    let store: Arc<dyn DurableSubstrate> = Arc::new(LocalFsSubstrate::new(obj).unwrap());
    let engine = L0Engine::<D1EventLog>::open(L0Config::d1(local), store).unwrap();
    Arc::new(FeedWriter::new(engine))
}

/// A **graceful writer shutdown**: seal the active part and wait for it to reach
/// the substrate, so the reopened engine recovers everything appended so far.
///
/// This is not test scaffolding, it is the shape a restart has to take. The engine
/// resumes from its durable manifest, so records still sitting in an unsealed
/// active part are not visible after a reopen even though they were `fsync`ed —
/// which is why the worker host seals on SIGTERM. The crash path (SIGKILL / OOM,
/// no seal) is a separate durability gap, tracked outside this fix; the resume
/// cursor is clamped to the reopened tip so it degrades to a small redelivery
/// instead of a dark bus (see `resume_cursor_is_clamped_to_a_truncated_log`).
fn graceful_shutdown(writer: &Arc<FeedWriter<D1EventLog>>) {
    writer
        .engine()
        .lock()
        .unwrap()
        .flush_and_wait_uploads()
        .unwrap();
}

/// Serve `coordinator`'s claims on a fresh loopback port.
async fn serve(coordinator: Arc<ClaimCoordinator<D1EventLog>>) -> std::net::SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(ehdb_feed::serve_claims(listener, coordinator));
    addr
}

// ---------------------------------------------------------------------------
// Defect 1 — a writer that stops answering
// ---------------------------------------------------------------------------

/// A TCP relay standing in for the writer's Kubernetes Service: a **stable
/// client-facing address** in front of a backend that can be swapped (the pod
/// restarted) or made to go silent.
///
/// `stall` is the half-open simulation, and it has to be a relay rather than
/// simply dropping the server: killing a listener sends a FIN, which a blocking
/// read *does* surface. What wedged the bus in kind is the case where no FIN or
/// RST ever arrives — the pod's veth and conntrack entry disappear with the pod —
/// so the client's peer looks alive at the TCP level and only stops talking. Once
/// stalled, this relay parks holding both halves of the connection: nothing is
/// forwarded, nothing is closed, and the client's kernel is never told anything.
struct Relay {
    addr: std::net::SocketAddr,
    upstream: Arc<std::sync::Mutex<std::net::SocketAddr>>,
    stall: Arc<AtomicBool>,
}

impl Relay {
    async fn in_front_of(upstream: std::net::SocketAddr) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let upstream = Arc::new(std::sync::Mutex::new(upstream));
        let stall = Arc::new(AtomicBool::new(false));
        let (up, st) = (Arc::clone(&upstream), Arc::clone(&stall));
        tokio::spawn(async move {
            loop {
                let Ok((client, _)) = listener.accept().await else {
                    return;
                };
                let target = *up.lock().unwrap();
                let Ok(server) = TcpStream::connect(target).await else {
                    continue;
                };
                let (cr, cw) = client.into_split();
                let (sr, sw) = server.into_split();
                tokio::spawn(pump(cr, sw, Arc::clone(&st)));
                tokio::spawn(pump(sr, cw, Arc::clone(&st)));
            }
        });
        Self {
            addr,
            upstream,
            stall,
        }
    }

    /// Stop forwarding without closing anything — the writer "died" invisibly.
    fn stall(&self) {
        self.stall.store(true, Ordering::SeqCst);
    }

    /// Point new connections at a restarted backend and let them through.
    fn restarted_at(&self, upstream: std::net::SocketAddr) {
        *self.upstream.lock().unwrap() = upstream;
        self.stall.store(false, Ordering::SeqCst);
    }
}

/// Copy one direction, parking forever (holding both halves open, so no FIN is
/// ever sent) as soon as the relay is stalled.
async fn pump(
    mut from: tokio::net::tcp::OwnedReadHalf,
    mut to: tokio::net::tcp::OwnedWriteHalf,
    stall: Arc<AtomicBool>,
) {
    let mut buf = [0u8; 8192];
    loop {
        if stall.load(Ordering::SeqCst) {
            std::future::pending::<()>().await;
        }
        let n = match from.read(&mut buf).await {
            Ok(0) | Err(_) => return,
            Ok(n) => n,
        };
        if stall.load(Ordering::SeqCst) {
            // Bytes read from a peer that is now considered dead are dropped, and
            // this task keeps both halves alive: exactly a half-open socket.
            std::future::pending::<()>().await;
        }
        if to.write_all(&buf[..n]).await.is_err() {
            return;
        }
    }
}

/// **The #208 defect-1 repro.** With heartbeats opted out — the pre-fix wire
/// behaviour — a claim against a silently-dead coordinator never returns. This is
/// the wedge: no data, no error, no log line, forever.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn half_open_coordinator_wedges_without_heartbeats() {
    let (obj, local) = (unique_dir("obj"), unique_dir("local"));
    let writer = writer_over(&local, &obj);
    let coord = Arc::new(ClaimCoordinator::new(
        writer.clone(),
        0,
        ACK_WAIT,
        0,
        ehdb_feed::d1_command_subject(1),
    ));
    let relay = Relay::in_front_of(serve(coord).await).await;

    let mut client = ClaimClient::connect_with_heartbeat(relay.addr, 1, FILTER, None)
        .await
        .unwrap();
    writer.append(ev(1)).unwrap();
    assert_eq!(
        client.claim_next::<EventRecord>().await.unwrap().sort_key,
        1,
        "sanity: delivery works before the writer goes away"
    );
    client.ack(1).await.unwrap();

    relay.stall();
    // Nothing will ever come back, so the only way to observe the wedge is to
    // give up waiting for it ourselves.
    let parked = tokio::time::timeout(
        Duration::from_millis(1_500),
        client.claim_next::<EventRecord>(),
    )
    .await;
    assert!(
        parked.is_err(),
        "pre-#208 behaviour: a claim on a half-open socket neither returns nor errors"
    );
}

/// The fix: the same silently-dead coordinator becomes an ordinary read error
/// within a few heartbeat intervals, so the caller's redial path is reachable.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn half_open_coordinator_surfaces_as_a_read_error() {
    let (obj, local) = (unique_dir("obj"), unique_dir("local"));
    let writer = writer_over(&local, &obj);
    let coord = Arc::new(ClaimCoordinator::new(
        writer.clone(),
        0,
        ACK_WAIT,
        0,
        ehdb_feed::d1_command_subject(1),
    ));
    let relay = Relay::in_front_of(serve(coord).await).await;

    let beat = Duration::from_millis(150);
    let mut client = ClaimClient::connect_with_heartbeat(relay.addr, 1, FILTER, Some(beat))
        .await
        .unwrap();
    writer.append(ev(1)).unwrap();
    assert_eq!(
        client.claim_next::<EventRecord>().await.unwrap().sort_key,
        1
    );
    client.ack(1).await.unwrap();

    relay.stall();
    let started = Instant::now();
    let err = tokio::time::timeout(
        Duration::from_millis(5_000),
        client.claim_next::<EventRecord>(),
    )
    .await
    .expect("the claim must not park forever any more")
    .expect_err("a dead coordinator must surface as an error");
    assert!(
        err.to_string().contains("heartbeat"),
        "the error should name the missed heartbeats: {err}"
    );
    // ~3 beats, with generous slack for a loaded test machine.
    assert!(
        started.elapsed() < Duration::from_secs(3),
        "detection took {:?}",
        started.elapsed()
    );
}

/// A parked claim consumes heartbeats without disturbing delivery: the command
/// appended after several beats is still claimed exactly once, and the client can
/// see that this peer proved its liveness.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn heartbeats_do_not_disturb_a_parked_claim() {
    let (obj, local) = (unique_dir("obj"), unique_dir("local"));
    let writer = writer_over(&local, &obj);
    let coord = Arc::new(ClaimCoordinator::new(
        writer.clone(),
        0,
        ACK_WAIT,
        0,
        ehdb_feed::d1_command_subject(1),
    ));
    let addr = serve(coord).await;

    let mut client =
        ClaimClient::connect_with_heartbeat(addr, 1, FILTER, Some(Duration::from_millis(50)))
            .await
            .unwrap();
    let appender = writer.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(400)).await;
        appender.append(ev(7)).unwrap();
    });

    let claimed = tokio::time::timeout(Duration::from_secs(5), client.claim_next::<EventRecord>())
        .await
        .expect("a parked claim across many heartbeats still delivers")
        .unwrap();
    assert_eq!(claimed.sort_key, 1, "the only appended command");
    assert!(claimed.record.payload.contains("\"id\":7"));
    assert!(
        client.peer_heartbeats(),
        "the coordinator should have proven liveness while the claim parked"
    );
    client.ack(claimed.sort_key).await.unwrap();
}

/// End to end: the writer restarts, the claimer's read errors, it redials the same
/// address, and dispatch resumes — no worker restart, no command lost. This is the
/// scenario that needed three `kubectl rollout restart`s to recover.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn client_redials_a_restarted_coordinator_and_dispatch_resumes() {
    let (obj, local) = (unique_dir("obj"), unique_dir("local"));
    let cursor_dir = unique_dir("cursor");

    let writer = writer_over(&local, &obj);
    let coord = Arc::new(
        ClaimCoordinator::resume(
            writer.clone(),
            0,
            ACK_WAIT,
            ehdb_feed::d1_command_subject(1),
            CursorStore::open(&cursor_dir, 0).unwrap(),
            CursorFallback::Tail,
        )
        .unwrap(),
    );
    let relay = Relay::in_front_of(serve(coord.clone()).await).await;

    let beat = Duration::from_millis(150);
    let mut client = ClaimClient::connect_with_heartbeat(relay.addr, 1, FILTER, Some(beat))
        .await
        .unwrap();

    // Two commands claimed + acked before the restart.
    for seq in 1..=2 {
        writer.append(ev(seq)).unwrap();
        let c = client.claim_next::<EventRecord>().await.unwrap();
        client.ack(c.sort_key).await.unwrap();
    }
    coord.persist_cursor().await.unwrap();
    graceful_shutdown(&writer);

    // The writer pod goes away invisibly, then comes back over the same volume at
    // a new pod IP behind the same service address.
    relay.stall();
    assert!(
        client.claim_next::<EventRecord>().await.is_err(),
        "the claimer must learn the writer is gone"
    );
    drop(coord);

    let restarted_writer = writer_over(&local, &obj);
    let restarted = Arc::new(
        ClaimCoordinator::resume(
            restarted_writer.clone(),
            0,
            ACK_WAIT,
            ehdb_feed::d1_command_subject(1),
            CursorStore::open(&cursor_dir, 0).unwrap(),
            CursorFallback::Tail,
        )
        .unwrap(),
    );
    assert_eq!(restarted.started_from().1, CursorOrigin::Persisted);
    assert_eq!(
        restarted.lag().await,
        0,
        "the restarted writer must not re-serve the commands already acked"
    );
    relay.restarted_at(serve(restarted).await);

    // The worker's redial loop: reconnect to the same address and keep going.
    let mut client = ClaimClient::connect_with_heartbeat(relay.addr, 1, FILTER, Some(beat))
        .await
        .unwrap();
    restarted_writer.append(ev(3)).unwrap();
    let after = tokio::time::timeout(Duration::from_secs(5), client.claim_next::<EventRecord>())
        .await
        .expect("dispatch must resume after the redial")
        .unwrap();
    assert!(
        after.record.payload.contains("\"id\":3"),
        "the first command after the restart is the new one, not a replay: {}",
        after.record.payload
    );
    client.ack(after.sort_key).await.unwrap();
}

// ---------------------------------------------------------------------------
// Defect 2 — a restarted writer must not replay its shard
// ---------------------------------------------------------------------------

/// The test's own id for a claimed command, read back out of its payload (the
/// writer owns the sort key, so the payload is what carries identity).
fn payload_id(rec: &EventRecord) -> u64 {
    serde_json::from_str::<serde_json::Value>(&rec.payload)
        .ok()
        .and_then(|v| v.get("id").and_then(|i| i.as_u64()))
        .expect("test records carry their id in the payload")
}

/// Claim and ack everything currently available through a fresh client, returning
/// the ids in delivery order.
async fn drain(addr: std::net::SocketAddr, member: u32) -> Vec<u64> {
    let mut client = ClaimClient::connect(addr, member, FILTER).await.unwrap();
    let mut got = Vec::new();
    while let Ok(Ok(c)) = tokio::time::timeout(
        Duration::from_millis(500),
        client.claim_next::<EventRecord>(),
    )
    .await
    {
        got.push(payload_id(&c.record));
        client.ack(c.sort_key).await.unwrap();
    }
    got
}

/// **The #208 defect-2 fix, measured against the defect.** After a restart the
/// resumed coordinator starts at the persisted cursor and sees zero backlog, while
/// a `from_cursor = 0` coordinator over the very same log still reports the whole
/// shard as lag — the 2738-record replay that stalled dispatch in kind.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn restart_resumes_from_the_committed_cursor() {
    let (obj, local) = (unique_dir("obj"), unique_dir("local"));
    let cursor_dir = unique_dir("cursor");
    const N: u64 = 50;

    let writer = writer_over(&local, &obj);
    let coord = Arc::new(
        ClaimCoordinator::resume(
            writer.clone(),
            0,
            ACK_WAIT,
            ehdb_feed::d1_command_subject(1),
            CursorStore::open(&cursor_dir, 0).unwrap(),
            CursorFallback::Tail,
        )
        .unwrap(),
    );
    let addr = serve(coord.clone()).await;

    for seq in 1..=N {
        writer.append(ev(seq)).unwrap();
    }
    assert_eq!(drain(addr, 1).await.len() as u64, N);
    let persisted = coord.persist_cursor().await.unwrap().unwrap();
    assert_eq!(
        persisted, N,
        "everything is acked, so the cursor is the tip"
    );
    graceful_shutdown(&writer);
    drop(coord);

    // --- the restart: same volume, new process ---
    let restarted_writer = writer_over(&local, &obj);
    let resumed = ClaimCoordinator::resume(
        restarted_writer.clone(),
        0,
        ACK_WAIT,
        ehdb_feed::d1_command_subject(1),
        CursorStore::open(&cursor_dir, 0).unwrap(),
        CursorFallback::Tail,
    )
    .unwrap();
    assert_eq!(resumed.started_from(), (N, CursorOrigin::Persisted));
    assert_eq!(
        resumed.lag().await,
        0,
        "resumed from the committed cursor: nothing to re-serve"
    );

    // The pre-fix coordinator over the same reopened log, for contrast.
    let from_zero = ClaimCoordinator::new(
        restarted_writer.clone(),
        0,
        ACK_WAIT,
        0,
        ehdb_feed::d1_command_subject(1),
    );
    assert_eq!(
        from_zero.lag().await,
        N,
        "this is the defect: from_cursor = 0 re-serves the whole shard log"
    );

    // And the resumed coordinator delivers only what is genuinely new.
    let resumed_addr = serve(Arc::new(resumed)).await;
    for seq in N + 1..=N + 3 {
        restarted_writer.append(ev(seq)).unwrap();
    }
    assert_eq!(drain(resumed_addr, 2).await, vec![N + 1, N + 2, N + 3]);
}

/// The safety half of defect 2: resuming must never *skip* work. Everything that
/// was not acked before the restart — never delivered, or delivered and still in
/// flight — comes back after it, exactly once.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn restart_still_redelivers_everything_unacked() {
    let (obj, local) = (unique_dir("obj"), unique_dir("local"));
    let cursor_dir = unique_dir("cursor");

    let writer = writer_over(&local, &obj);
    let coord = Arc::new(
        ClaimCoordinator::resume(
            writer.clone(),
            0,
            ACK_WAIT,
            ehdb_feed::d1_command_subject(1),
            CursorStore::open(&cursor_dir, 0).unwrap(),
            CursorFallback::Tail,
        )
        .unwrap(),
    );
    let addr = serve(coord.clone()).await;

    for seq in 1..=10 {
        writer.append(ev(seq)).unwrap();
    }
    // Ack 1..=6, claim 7 and 8 without acking (in flight when the writer dies),
    // leave 9 and 10 never delivered.
    let mut client = ClaimClient::connect(addr, 1, FILTER).await.unwrap();
    for _ in 1..=6 {
        let c = client.claim_next::<EventRecord>().await.unwrap();
        client.ack(c.sort_key).await.unwrap();
    }
    for _ in 7..=8 {
        let _ = client.claim_next::<EventRecord>().await.unwrap();
    }
    assert_eq!(coord.persist_cursor().await.unwrap(), Some(6));
    graceful_shutdown(&writer);
    drop(client);
    drop(coord);

    let restarted_writer = writer_over(&local, &obj);
    let resumed = ClaimCoordinator::resume(
        restarted_writer.clone(),
        0,
        ACK_WAIT,
        ehdb_feed::d1_command_subject(1),
        CursorStore::open(&cursor_dir, 0).unwrap(),
        CursorFallback::Tail,
    )
    .unwrap();
    assert_eq!(resumed.started_from(), (6, CursorOrigin::Persisted));
    assert_eq!(resumed.lag().await, 4, "7..=10 are still owed");
    let resumed_addr = serve(Arc::new(resumed)).await;
    assert_eq!(
        drain(resumed_addr, 2).await,
        vec![7, 8, 9, 10],
        "every unacked command comes back, in order, exactly once"
    );
}

/// With no cursor stored yet — the first start after this fix ships, or a wiped
/// volume — the fallback decides. `Tail` serves only what arrives after the
/// restart; `Beginning` keeps the pre-fix full replay as an escape hatch.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_fresh_cursor_store_falls_back_as_configured() {
    let (obj, local) = (unique_dir("obj"), unique_dir("local"));
    let writer = writer_over(&local, &obj);
    for seq in 1..=20 {
        writer.append(ev(seq)).unwrap();
    }

    let tail = ClaimCoordinator::resume(
        writer.clone(),
        0,
        ACK_WAIT,
        ehdb_feed::d1_command_subject(1),
        CursorStore::open(unique_dir("cursor-tail"), 0).unwrap(),
        CursorFallback::Tail,
    )
    .unwrap();
    assert_eq!(tail.started_from(), (20, CursorOrigin::FallbackTail));
    assert_eq!(tail.lag().await, 0);

    let beginning = ClaimCoordinator::resume(
        writer.clone(),
        0,
        ACK_WAIT,
        ehdb_feed::d1_command_subject(1),
        CursorStore::open(unique_dir("cursor-begin"), 0).unwrap(),
        CursorFallback::Beginning,
    )
    .unwrap();
    assert_eq!(
        beginning.started_from(),
        (0, CursorOrigin::FallbackBeginning)
    );
    assert_eq!(beginning.lag().await, 20);
}

/// **The dark-bus hazard the resume introduces, closed.** A crash (no seal) can
/// reopen the log *behind* the persisted cursor, and the writer then re-issues keys
/// from its recovered sequence — so a cursor taken literally would filter out every
/// future record and the bus would never deliver again. The resume clamps to the
/// reopened tip: at worst a small redelivery, never silence.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn resume_cursor_is_clamped_to_a_truncated_log() {
    let (obj, local) = (unique_dir("obj"), unique_dir("local"));
    let cursor_dir = unique_dir("cursor");

    // A cursor far ahead of anything this log knows about — what a crash after
    // 5_000 unsealed appends would leave behind.
    CursorStore::open(&cursor_dir, 0)
        .unwrap()
        .store(5_000)
        .unwrap();

    let writer = writer_over(&local, &obj);
    let resumed = Arc::new(
        ClaimCoordinator::resume(
            writer.clone(),
            0,
            ACK_WAIT,
            ehdb_feed::d1_command_subject(1),
            CursorStore::open(&cursor_dir, 0).unwrap(),
            CursorFallback::Tail,
        )
        .unwrap(),
    );
    assert_eq!(
        resumed.started_from(),
        (0, CursorOrigin::Persisted),
        "clamped to the reopened tip, not the stored 5000"
    );

    let addr = serve(resumed).await;
    for id in 1..=3 {
        writer.append(ev(id)).unwrap();
    }
    assert_eq!(
        drain(addr, 1).await,
        vec![1, 2, 3],
        "records appended after the clamp must still be delivered"
    );
}

/// The background persister keeps the cursor durable without the host having to
/// call it, and never writes a cursor ahead of what is actually acked.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_persister_keeps_the_cursor_current() {
    let (obj, local) = (unique_dir("obj"), unique_dir("local"));
    let cursor_dir = unique_dir("cursor");
    let store_probe = CursorStore::open(&cursor_dir, 0).unwrap();

    let writer = writer_over(&local, &obj);
    let coord = Arc::new(
        ClaimCoordinator::resume(
            writer.clone(),
            0,
            ACK_WAIT,
            ehdb_feed::d1_command_subject(1),
            CursorStore::open(&cursor_dir, 0).unwrap(),
            CursorFallback::Tail,
        )
        .unwrap(),
    );
    let addr = serve(coord.clone()).await;
    coord
        .clone()
        .spawn_cursor_persister(Duration::from_millis(50));

    for seq in 1..=5 {
        writer.append(ev(seq)).unwrap();
    }
    // Claim all five, ack only the first three: the cursor may advance to 3 and no
    // further, because 4 is still in flight.
    let mut client = ClaimClient::connect(addr, 1, FILTER).await.unwrap();
    let mut keys = Vec::new();
    for _ in 1..=5 {
        keys.push(client.claim_next::<EventRecord>().await.unwrap().sort_key);
    }
    for key in &keys[..3] {
        client.ack(*key).await.unwrap();
    }

    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if store_probe.load().unwrap() == Some(3) {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "the persister should have stored cursor 3, saw {:?}",
            store_probe.load().unwrap()
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert_eq!(coord.committed_cursor().await, 3);
}
