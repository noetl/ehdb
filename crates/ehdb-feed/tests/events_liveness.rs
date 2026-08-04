//! **Events-face liveness proof (noetl/ai-meta#225).**
//!
//! noetl/ai-meta#208 gave the *command* claim face (`:9101`) TCP keepalive and a
//! negotiated coordinator heartbeat. The two **events** faces never got it:
//!
//! - the named-group claim face (`:9104`, [`ehdb_feed::serve_group_claims`]) set
//!   `TCP_NODELAY` and nothing else, and its wire had no heartbeat at all;
//! - the WAL fan-out face (`:9108`, [`ehdb_feed::serve`]) had keepalive but no
//!   application-level heartbeat, so it could not tell "writer alive but stuck"
//!   from "feed idle".
//!
//! In prod that produced a consumer wedge with every health signal green:
//! `noetl.event` — the sole durable event log — took no writes for 3h24m,
//! `/readyz` stayed `ready`, `cursor_errors` stayed 0, and the only symptom was a
//! group cursor that had stopped moving. The consumers held ESTABLISHED sockets
//! the replacement writer knew nothing about, and every redial path downstream
//! was unreachable because there was no `Err` to trigger it.
//!
//! Each fix is measured against its own **negative control** — the same scenario
//! with heartbeats disabled, asserting the wedge still reproduces — so a test
//! that passes because the scenario stopped exercising the bug is caught.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use ehdb_feed::{CursorFallback, FeedSubscription, FeedWriter, GroupClaimClient, GroupCoordinator};
use ehdb_l0::substrate::DurableSubstrate;
use ehdb_l0::{D1EventLog, EventRecord, L0Config, L0Engine, LocalFsSubstrate};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

const ACK_WAIT: Duration = Duration::from_secs(30);
const GROUP: &str = "noetl_materializer";
const ALL_EVENTS: &str = "events.>";

/// Short enough that a wedge is provable in a few seconds, long enough that a
/// loaded CI box is never mistaken for a dead peer.
const BEAT: Duration = Duration::from_millis(200);
/// `BEAT * HEARTBEAT_MISS_FACTOR` is the detection window; allow a few of them.
const DETECT_WINDOW: Duration = Duration::from_secs(3);

fn unique_dir(tag: &str) -> std::path::PathBuf {
    static N: AtomicU64 = AtomicU64::new(0);
    let n = N.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir().join(format!(
        "ehdb-feed-events-liveness-{tag}-{}-{n}",
        std::process::id()
    ))
}

fn ev(id: u64) -> EventRecord {
    EventRecord::new(
        id,
        format!("exec-{id}"),
        format!("tx-{id}"),
        format!(r#"{{"event_type":"action_started","seq":{id}}}"#),
    )
}

fn open_writer(dir: &std::path::Path) -> Arc<FeedWriter<D1EventLog>> {
    std::fs::create_dir_all(dir).unwrap();
    let store: Arc<dyn DurableSubstrate> = Arc::new(LocalFsSubstrate::new(dir).unwrap());
    let engine =
        L0Engine::<D1EventLog>::open(L0Config::d1(dir).with_shard_count(1), store).unwrap();
    Arc::new(FeedWriter::new(engine))
}

/// Stand up the events group-claim face (`:9104`'s shape) over `writer`.
async fn serve_group_face(
    writer: Arc<FeedWriter<D1EventLog>>,
    dir: &std::path::Path,
) -> std::net::SocketAddr {
    let coordinator = group_coordinator(writer, dir, ACK_WAIT).await;
    listen_group_claims(coordinator).await
}

async fn group_coordinator(
    writer: Arc<FeedWriter<D1EventLog>>,
    dir: &std::path::Path,
    ack_wait: Duration,
) -> Arc<GroupCoordinator<D1EventLog>> {
    let coordinator = Arc::new(GroupCoordinator::new(
        writer,
        0,
        ack_wait,
        ehdb_feed::event_feed_subject(),
        Some(dir.to_path_buf()),
        CursorFallback::default(),
    ));
    coordinator.open_group(GROUP).await;
    coordinator
}

/// Bind one more group-claim listener over an existing coordinator. Used to
/// model the replacement writer's face coming up: from the consumer's side that
/// is a fresh listener at the address it redials, which is the whole of what the
/// consumer can observe.
async fn listen_group_claims(
    coordinator: Arc<GroupCoordinator<D1EventLog>>,
) -> std::net::SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let _ = ehdb_feed::serve_group_claims(listener, coordinator).await;
    });
    addr
}

/// Stand up the WAL fan-out face (`:9108`'s shape) over `writer`.
async fn serve_wal_face(writer: Arc<FeedWriter<D1EventLog>>) -> std::net::SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let _ = ehdb_feed::serve(writer, listener).await;
    });
    addr
}

// ---------------------------------------------------------------------------
// A relay that can go half-open: the exact prod failure mode. It holds both
// halves of the client socket open and simply stops forwarding, so no FIN and no
// RST ever reaches the client — the socket stays ESTABLISHED client-side while
// the peer is, as far as the data path is concerned, gone.
// ---------------------------------------------------------------------------

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
            std::future::pending::<()>().await;
        }
        if to.write_all(&buf[..n]).await.is_err() {
            return;
        }
    }
}

// ---------------------------------------------------------------------------
// :9104 — the events named-group claim face.
// ---------------------------------------------------------------------------

/// **The fix.** A half-open group-claim connection surfaces as a read error
/// within a few missed heartbeats instead of parking forever.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn half_open_group_claim_surfaces_as_a_read_error() {
    let dir = unique_dir("group-halfopen");
    let writer = open_writer(&dir);
    let face = serve_group_face(Arc::clone(&writer), &dir).await;
    let relay = Relay::in_front_of(face).await;

    let mut client =
        GroupClaimClient::connect_with_heartbeat(relay.addr, GROUP, 0, ALL_EVENTS, Some(BEAT))
            .await
            .unwrap();

    // Drain one record so the connection is proven live and heartbeat-backed.
    writer.append(ev(1)).unwrap();
    let first = client.claim_next::<EventRecord>().await.unwrap();
    client.ack(first.sort_key).await.unwrap();
    assert!(
        client.peer_heartbeats(),
        "the coordinator must announce heartbeats up front, or the client can \
         never arm its read deadline"
    );

    // The writer "restarts": the socket stays ESTABLISHED, nothing is forwarded.
    relay.stall();
    let err = tokio::time::timeout(DETECT_WINDOW, client.claim_next::<EventRecord>())
        .await
        .expect("the claim must return, not park forever — that is the #225 wedge")
        .expect_err("a stalled coordinator must be an error, not a record");
    assert!(
        err.to_string().contains("stopped heartbeating"),
        "unexpected error: {err}"
    );
}

/// **Negative control for the above.** With heartbeats disabled the wedge still
/// reproduces: the claim parks indefinitely on a half-open socket with no error.
/// If this test ever starts failing, the scenario has stopped exercising the bug
/// and the test above is no longer proving anything.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn negative_control_group_claim_wedges_without_heartbeats() {
    let dir = unique_dir("group-halfopen-neg");
    let writer = open_writer(&dir);
    let face = serve_group_face(Arc::clone(&writer), &dir).await;
    let relay = Relay::in_front_of(face).await;

    let mut client =
        GroupClaimClient::connect_with_heartbeat(relay.addr, GROUP, 0, ALL_EVENTS, None)
            .await
            .unwrap();
    writer.append(ev(1)).unwrap();
    let first = client.claim_next::<EventRecord>().await.unwrap();
    client.ack(first.sort_key).await.unwrap();
    assert!(!client.peer_heartbeats(), "opted out — none should arrive");

    relay.stall();
    // Append work the consumer would drain if it were healthy. It never sees it.
    writer.append(ev(2)).unwrap();
    let parked = tokio::time::timeout(DETECT_WINDOW, client.claim_next::<EventRecord>()).await;
    assert!(
        parked.is_err(),
        "without heartbeats a half-open claim must still park silently — this is \
         the defect, and the positive test above is only meaningful while it holds"
    );
}

/// **End to end.** A consumer attached when the writer's face goes away detects
/// the dead connection, redials, and drains the whole backlog — the "no silent
/// park, backlog drains, nothing lost" bar.
///
/// A short `ack_wait` here is deliberate. Stalling the relay strands the
/// coordinator-side claim task mid-`claim_next`: it is still a member of the
/// group, so it can be *assigned* a record whose response then vanishes into the
/// stalled socket. That orphaned assignment is real — it is what a half-open
/// consumer does to a live coordinator — and requiring the drain to complete
/// anyway is what proves the redelivery path covers it. With the pre-#225 client
/// this cannot even be reached: the consumer never learns to redial.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn group_consumer_reattaches_after_a_writer_restart_and_drains() {
    const ORPHAN_ACK_WAIT: Duration = Duration::from_secs(2);
    let dir = unique_dir("group-restart");
    let writer = open_writer(&dir);
    let coordinator = group_coordinator(Arc::clone(&writer), &dir, ORPHAN_ACK_WAIT).await;
    let face = listen_group_claims(Arc::clone(&coordinator)).await;
    let relay = Relay::in_front_of(face).await;

    let mut client =
        GroupClaimClient::connect_with_heartbeat(relay.addr, GROUP, 0, ALL_EVENTS, Some(BEAT))
            .await
            .unwrap();
    writer.append(ev(1)).unwrap();
    let first = client.claim_next::<EventRecord>().await.unwrap();
    client.ack(first.sort_key).await.unwrap();

    // The writer's face goes away half-open. The consumer must notice.
    relay.stall();
    let err = tokio::time::timeout(DETECT_WINDOW, client.claim_next::<EventRecord>())
        .await
        .expect("the consumer must notice, not park — that is the wedge")
        .expect_err("a stalled coordinator must be an error");
    assert!(err.to_string().contains("stopped heartbeating"));
    drop(client);

    // Backlog lands while the consumer is away, then the replacement face is up
    // at the address it redials.
    for id in 2..=6 {
        writer.append(ev(id)).unwrap();
    }
    relay.restarted_at(listen_group_claims(Arc::clone(&coordinator)).await);

    // The redial is the thing under test: reconnect, then drain everything.
    let mut client =
        GroupClaimClient::connect_with_heartbeat(relay.addr, GROUP, 0, ALL_EVENTS, Some(BEAT))
            .await
            .unwrap();
    let mut drained = std::collections::BTreeSet::new();
    let deadline = std::time::Instant::now() + Duration::from_secs(20);
    while drained.len() < 5 && std::time::Instant::now() < deadline {
        let Ok(Ok(claimed)) =
            tokio::time::timeout(DETECT_WINDOW, client.claim_next::<EventRecord>()).await
        else {
            continue; // redelivery of the orphaned assignment is still pending
        };
        client.ack(claimed.sort_key).await.unwrap();
        drained.insert(claimed.record.execution_id.clone());
    }
    assert_eq!(
        drained.len(),
        5,
        "the whole backlog must drain after the reattach, including the record \
         stranded on the orphaned claim: {drained:?}"
    );
}

// ---------------------------------------------------------------------------
// :9108 — the WAL fan-out face.
// ---------------------------------------------------------------------------

/// **The fix.** The WAL subscription had keepalive but no heartbeat, so it could
/// not distinguish "writer alive but stuck" from "feed idle". It can now.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn half_open_wal_subscription_surfaces_as_a_read_error() {
    let dir = unique_dir("wal-halfopen");
    let writer = open_writer(&dir);
    let face = serve_wal_face(Arc::clone(&writer)).await;
    let relay = Relay::in_front_of(face).await;

    let mut sub = FeedSubscription::connect_with_heartbeat(relay.addr, 0, 0, Some(BEAT))
        .await
        .unwrap();
    writer.append(ev(1)).unwrap();
    let batch: Vec<EventRecord> = sub.recv_batch().await.unwrap();
    assert_eq!(batch.len(), 1);
    assert!(
        sub.peer_heartbeats(),
        "the writer must announce heartbeats up front on a subscribing client"
    );

    relay.stall();
    let err = tokio::time::timeout(DETECT_WINDOW, sub.recv_batch::<EventRecord>())
        .await
        .expect("the subscription must return, not park forever")
        .expect_err("a stalled writer must be an error, not a batch");
    assert!(
        err.to_string().contains("stopped heartbeating"),
        "unexpected error: {err}"
    );
}

/// **Negative control.** Heartbeats off → the half-open subscription still parks
/// silently, which is the pre-#225 behaviour.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn negative_control_wal_subscription_wedges_without_heartbeats() {
    let dir = unique_dir("wal-halfopen-neg");
    let writer = open_writer(&dir);
    let face = serve_wal_face(Arc::clone(&writer)).await;
    let relay = Relay::in_front_of(face).await;

    let mut sub = FeedSubscription::connect_with_heartbeat(relay.addr, 0, 0, None)
        .await
        .unwrap();
    writer.append(ev(1)).unwrap();
    let batch: Vec<EventRecord> = sub.recv_batch().await.unwrap();
    assert_eq!(batch.len(), 1);
    assert!(!sub.peer_heartbeats(), "opted out — none should arrive");

    relay.stall();
    writer.append(ev(2)).unwrap();
    let parked = tokio::time::timeout(DETECT_WINDOW, sub.recv_batch::<EventRecord>()).await;
    assert!(
        parked.is_err(),
        "without heartbeats a half-open WAL subscription must still park silently"
    );
}

/// An idle feed is **not** a dead one: a subscription that sees only heartbeats
/// for many detection windows must stay attached and then deliver.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_idle_feed_is_not_mistaken_for_a_dead_writer() {
    let dir = unique_dir("wal-idle");
    let writer = open_writer(&dir);
    let face = serve_wal_face(Arc::clone(&writer)).await;

    let mut sub = FeedSubscription::connect_with_heartbeat(face, 0, 0, Some(BEAT))
        .await
        .unwrap();
    // Quiet for well past the miss window — heartbeats alone must hold it open.
    tokio::time::sleep(BEAT * 12).await;
    writer.append(ev(1)).unwrap();
    let batch: Vec<EventRecord> = tokio::time::timeout(DETECT_WINDOW, sub.recv_batch())
        .await
        .expect("an idle feed must not be declared dead")
        .unwrap();
    assert_eq!(batch.len(), 1);
}

/// Backward compatibility: a pre-#225 client frame (`{shard, cursor}` with no
/// `heartbeat_ms`) still decodes and is served with no heartbeats.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_pre_225_subscribe_frame_still_decodes() {
    let legacy = br#"{"shard":0,"cursor":7}"#;
    let req: ehdb_feed::SubscribeReq = serde_json::from_slice(legacy).unwrap();
    assert_eq!(req.shard, 0);
    assert_eq!(req.cursor, 7);
    assert_eq!(
        req.heartbeat_ms, None,
        "an absent heartbeat_ms must mean opted out, not a decode failure"
    );
}
