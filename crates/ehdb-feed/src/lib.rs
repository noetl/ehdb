//! # ehdb-feed — L1 networked change-feed delivery (T0 shadow transport)
//!
//! The networked realisation of topology (c) (per-shard-writer-as-broker): the
//! per-shard writer owns the durable log (an [`L0Engine`]) **and** owns delivery
//! for its shard. This crate is that delivery face — it carries the L0
//! [`ChangeFeed`] (`Watch(shard, cursor)`) batches to subscribers over a real
//! socket, one delivery hop (writer→subscriber) = NATS parity. The control plane
//! (noetl-server) is **not** in this path: it publishes the next record to the
//! writer via [`FeedWriter::append`]; subscribers pull directly from the writer.
//!
//! **T0 posture:** this is the shadow transport — additive, kind/local,
//! comparison-only. NATS stays authoritative; this path only *observes* the same
//! records so their append→subscriber latency can be measured (see
//! `tests/latency.rs`) and compared against NATS before any cutover (T4, gated).
//!
//! Wire protocol (deliberately minimal for the shadow tier): length-prefixed
//! (`u32` big-endian) JSON frames. A subscriber opens a [`TcpStream`], writes one
//! [`SubscribeReq`] frame (`{shard, cursor}`), then reads a stream of batch
//! frames (`Vec<D::Record>`) as the writer appends. `TCP_NODELAY` is set on both
//! ends so a single record is delivered immediately, not Nagle-batched.
//!
//! Delivery is **push, not poll-spin:** the writer signals a [`watch`] channel on
//! each append; each subscriber task drains its feed, then parks on
//! `changed().await` until the next append — an append that races the park
//! advances the watch version, so `changed()` returns immediately (no lost
//! wakeup). Resume/reconnect is exact: reconnect with the last-received
//! `global_sequence` as the cursor (the ack watermark T1 builds on).

pub mod claim;
pub mod cursor;
pub mod group;
pub mod groups;
pub mod kv;
pub mod publish;
pub mod scaler;
pub mod sse;
pub mod subject;
pub use claim::{
    d1_command_subject, serve_claims, ClaimClient, ClaimCoordinator, Claimed, DEFAULT_POOL,
};
pub use cursor::{CursorFallback, CursorOrigin, CursorStore, ResumeReport};
pub use group::{Delivery, MemberId, ShardConsumerGroup, SubjectConsumerGroup};
pub use kv::{serve_kv, KvClient, KvCoordinator};
pub use groups::{
    event_feed_subject, serve_group_claims, GroupClaimClient, GroupClaimed, GroupCoordinator,
    EVENT_SUBJECT_ROOT, UNKNOWN_EVENT_TYPE,
};
pub use publish::{serve_ingest, PipelinedPublishClient, PublishClient, PublishRouter};
pub use scaler::{
    bind_and_serve_snapshot_with_resume, render_prometheus, render_resume, render_snapshot,
    LagSnapshot, ShardLag, SubjectLag,
};
pub use subject::{Subject, SubjectFilter, SubjectFn};

use std::io;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use ehdb_l0::{ChangeFeed, Dataset, FlushPolicy, L0Engine};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream, ToSocketAddrs};
use tokio::sync::watch;

/// A subscriber's request: the shard to follow and the resume cursor (sort key
/// of the last record it already has; `0` = from the beginning).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubscribeReq {
    pub shard: u32,
    pub cursor: u64,
    /// Opt this subscription into liveness heartbeats: while the feed is caught
    /// up and the push loop is parked, the writer sends a heartbeat frame every
    /// `heartbeat_ms` (noetl/ai-meta#225).
    ///
    /// `#[serde(default)]` so a pre-#225 client's `{shard, cursor}` frame still
    /// decodes — that client gets no heartbeats, exactly as before.
    #[serde(default)]
    pub heartbeat_ms: Option<u64>,
}

pub(crate) fn io_err<E: std::fmt::Display>(err: E) -> io::Error {
    io::Error::other(err.to_string())
}

/// How often a parked read proves the peer is still alive to a client that asked
/// for heartbeats (noetl/ai-meta#208 for the command claim face, #225 for the
/// events group-claim and WAL fan-out faces).
pub const DEFAULT_HEARTBEAT: Duration = Duration::from_secs(5);

/// How many consecutive heartbeats a client waits for before it calls the
/// connection dead and redials. Three gives a ~15 s detection window with the
/// default interval — slack enough that a busy peer is never mistaken for a dead
/// one, short enough that consumption resumes in seconds.
pub const HEARTBEAT_MISS_FACTOR: u32 = 3;

/// The heartbeat frame a parked face sends. A distinct frame rather than a
/// variant of any response type, so every response stays byte-identical on the
/// wire and a client that never asks for heartbeats — and so never receives one
/// — is unaffected.
pub(crate) const HEARTBEAT_FRAME: &[u8] = b"{\"heartbeat\":true}";

#[derive(Debug, Clone, Copy, Deserialize)]
struct HeartbeatFrame {
    heartbeat: bool,
}

/// Is this frame a liveness heartbeat rather than a payload? Unambiguous: no
/// response type in this crate carries a `heartbeat` field, so a real response
/// fails to decode here.
pub(crate) fn is_heartbeat(body: &[u8]) -> bool {
    serde_json::from_slice::<HeartbeatFrame>(body)
        .map(|h| h.heartbeat)
        .unwrap_or(false)
}

/// How long a connection may sit idle before the kernel starts probing the peer.
pub const KEEPALIVE_IDLE: Duration = Duration::from_secs(5);
/// The gap between keepalive probes once probing has started.
pub const KEEPALIVE_INTERVAL: Duration = Duration::from_secs(2);
/// How many unanswered probes declare the connection dead — so a dead peer
/// surfaces as a read/write error in roughly
/// `KEEPALIVE_IDLE + KEEPALIVE_RETRIES * KEEPALIVE_INTERVAL` ≈ 11 s.
pub const KEEPALIVE_RETRIES: u32 = 3;

/// The socket posture every ehdb-feed connection gets: `TCP_NODELAY` (a single
/// record is delivered immediately, not Nagle-batched) **plus TCP keepalive**.
///
/// Keepalive is the fix for the silent wedge in noetl/ai-meta#208. Every protocol
/// in this crate parks on a blocking read while it waits for the peer — a
/// claimer inside `claim_next`, a subscriber inside its push loop, a publisher
/// waiting for its durable ack. When the peer's pod dies, whether that read ever
/// returns depends on a FIN or RST actually arriving: under Kubernetes it often
/// does not (the veth and conntrack entry go away with the pod), so the socket is
/// left **half-open** and the read neither yields data nor errors. Without
/// keepalive the caller parks forever, no error is logged, and every redial path
/// in this crate and its callers is unreachable because there is no `Err` to
/// trigger it — which is exactly how a routine writer restart wedged dispatch
/// with `0 of 30` commands claimed and nothing in any log.
///
/// With keepalive armed, the kernel probes an idle connection and a dead peer
/// becomes an ordinary `io::Error` within ~11 s, so the existing
/// error-then-reconnect paths do their job unchanged.
pub(crate) fn configure_stream(sock: &TcpStream) -> io::Result<()> {
    sock.set_nodelay(true)?;
    let keepalive = socket2::TcpKeepalive::new()
        .with_time(KEEPALIVE_IDLE)
        .with_interval(KEEPALIVE_INTERVAL);
    // `with_retries` (TCP_KEEPCNT) is not portable to every target socket2
    // supports; the idle+interval pair alone still bounds detection everywhere.
    #[cfg(not(any(
        target_os = "openbsd",
        target_os = "redox",
        target_os = "solaris",
        target_os = "windows"
    )))]
    let keepalive = keepalive.with_retries(KEEPALIVE_RETRIES);
    socket2::SockRef::from(sock).set_tcp_keepalive(&keepalive)
}

/// Close the durability window over the handles taken from the engine, with the
/// engine lock **released** (noetl/ai-meta#205). `fsync` is a blocking,
/// millisecond-scale syscall and the consuming side needs the engine lock to poll
/// its feed, so syncing under the lock stalls every claimer for its duration.
fn commit(handles: &[std::fs::File]) -> io::Result<()> {
    for handle in handles {
        handle.sync_data()?;
    }
    Ok(())
}

pub(crate) async fn write_frame<W: AsyncWriteExt + Unpin>(
    w: &mut W,
    bytes: &[u8],
) -> io::Result<()> {
    let len = u32::try_from(bytes.len()).map_err(io_err)?;
    w.write_all(&len.to_be_bytes()).await?;
    w.write_all(bytes).await?;
    w.flush().await?;
    Ok(())
}

pub(crate) async fn read_frame<R: AsyncReadExt + Unpin>(r: &mut R) -> io::Result<Vec<u8>> {
    let mut len = [0u8; 4];
    r.read_exact(&mut len).await?;
    let n = u32::from_be_bytes(len) as usize;
    let mut buf = vec![0u8; n];
    r.read_exact(&mut buf).await?;
    Ok(buf)
}

/// The per-shard writer's networked face: owns the L0 engine (the durable log)
/// and signals followers on every append. Wrap in an [`Arc`] and share one clone
/// with [`serve`] and one with the appending control plane.
pub struct FeedWriter<D: Dataset> {
    engine: Arc<Mutex<L0Engine<D>>>,
    tip_tx: watch::Sender<u64>,
    /// Set by [`seal_and_close`](FeedWriter::seal_and_close): the log has been
    /// sealed for shutdown and no further append may be *acked*.
    ///
    /// This is the non-blocking half of the shutdown seal (noetl/ai-meta#226).
    /// The worker used to enforce "nothing appends after the seal" by leaking
    /// the engine's `MutexGuard` (`std::mem::forget`) and holding it through
    /// process exit. That does stop appends — by parking every appender on a
    /// mutex that is never released. Every append path in this crate runs
    /// **inside an async task** and takes this `std::sync::Mutex` *blocking*
    /// (`serve_ingest`'s committer, the claim/WAL readers), so each parked
    /// appender burns a whole tokio worker thread. With a backlog in flight at
    /// SIGTERM there are more parked appenders than worker threads, the runtime
    /// is starved, and the shutdown future that was supposed to go on and seal
    /// the *second* host is never polled again — not even its timeout fires.
    ///
    /// A flag costs nothing on the hot path (one relaxed-ordering load per
    /// batch, not per record) and fails the post-seal appender **loudly and
    /// immediately** instead of hanging it, which is also the better contract:
    /// the publisher gets an error and retries against the replacement writer
    /// rather than blocking until its own timeout.
    closed: Arc<AtomicBool>,
}

impl<D> FeedWriter<D>
where
    D: Dataset,
    D::Record: Serialize + DeserializeOwned + Clone + Send + 'static,
{
    /// Wrap an engine as a networked writer, seeding the tip signal at the
    /// engine's current global sequence.
    ///
    /// Takes ownership of the engine's **commit points**: the flush posture is
    /// switched to [`FlushPolicy::CallerDriven`] and every append path here
    /// `fsync`s before it returns. Durability is unchanged (a returned sort key
    /// is still durable), but a batch of records that arrive together shares one
    /// `fsync` instead of paying one each — the group-commit fix for the command
    /// bus's dispatch latency (noetl/ai-meta#205).
    pub fn new(mut engine: L0Engine<D>) -> Self {
        let tip = engine.global_sequence();
        engine.set_flush_policy(FlushPolicy::CallerDriven);
        let (tip_tx, _rx) = watch::channel(tip);
        Self {
            engine: Arc::new(Mutex::new(engine)),
            tip_tx,
            closed: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Seal the active part for shutdown, wait for its upload, and **close the
    /// writer to further appends** — without holding the engine lock afterwards
    /// (noetl/ai-meta#226).
    ///
    /// After this returns, every [`append`](Self::append) /
    /// [`append_batch`](Self::append_batch) fails with `feed writer sealed`
    /// rather than landing in a fresh active part that the next incarnation
    /// would never see (an acked-then-lost record — the hole the seal exists to
    /// close).
    ///
    /// # Ordering
    ///
    /// The flag is set **before** the lock is taken and re-checked **under** it
    /// by the append paths, which makes the three cases exhaustive:
    ///
    /// - An appender already holding the lock finishes and is acked. Its record
    ///   is in the part this call then seals — correct.
    /// - An appender already blocked *on* the lock wakes after the seal
    ///   releases it, sees `closed`, and errors without appending. Its
    ///   publisher retries. Correct, and it waited only for the flush, not
    ///   forever.
    /// - An appender arriving after the flag is set never touches the mutex at
    ///   all. Correct, and it costs no thread.
    ///
    /// Idempotent: a second call re-flushes (a no-op with nothing pending) and
    /// leaves the writer closed.
    pub fn seal_and_close(&self) -> io::Result<()> {
        // Set first: an appender that has not yet reached the lock now fails
        // fast instead of queueing behind the flush below.
        self.closed.store(true, Ordering::SeqCst);
        let mut engine = self
            .engine
            .lock()
            .map_err(|_| io_err("L0 engine mutex poisoned before the shutdown seal"))?;
        engine.flush_and_wait_uploads().map_err(io_err)
        // Lock released here — deliberately. `closed` is what holds the line
        // from this point on, and it does so without owning a thread.
    }

    /// Has this writer been sealed for shutdown ([`seal_and_close`](Self::seal_and_close))?
    pub fn is_closed(&self) -> bool {
        self.closed.load(Ordering::SeqCst)
    }

    /// Append one record to the durable log and wake followers. Returns the
    /// **writer-assigned** sort key. This is the server→writer publish seam (the
    /// control plane calls it).
    ///
    /// The key is assigned by the writer ([`L0Engine::append_writer_assigned`]),
    /// not taken from the incoming record. The producer (noetl-server) assigns a
    /// snowflake command id, but under concurrent publish a lower id can reach
    /// this single writer *after* a higher one; trusting it would append behind
    /// the shard tail, land behind every follower's cursor, and silently drop
    /// the record (noetl/ai-meta#203). Letting the serialized writer assign a
    /// strictly-increasing key keeps the shard log ascending, so the feed cursor
    /// never skips an ingested record. The command's identity stays in its
    /// payload; the returned key is the ack token followers commit against.
    pub fn append(&self, record: D::Record) -> io::Result<u64> {
        // Cheap pre-check: a sealed writer never even contends for the lock.
        self.ensure_open()?;
        let (seq, handles) = {
            let mut engine = self.engine.lock().unwrap();
            // Re-check under the lock: the seal may have completed while this
            // appender was blocked on it. Without this the pre-check is a plain
            // TOCTOU and a record could still be acked into a post-seal part.
            self.ensure_open()?;
            let seq = engine.append_writer_assigned(record).map_err(io_err)?;
            (seq, engine.take_sync_handles().map_err(io_err)?)
        };
        // Close the durability window with the engine lock released — the key is
        // a durable ack, but the `fsync` must not block readers to earn it.
        commit(&handles)?;
        // Ignore send errors: no live subscribers is fine (shadow tier).
        let _ = self.tip_tx.send(seq);
        Ok(seq)
    }

    /// **Group commit** — append a whole batch under **one** engine-lock
    /// acquisition and **one** `fsync`, returning each record's writer-assigned
    /// sort key in the order given. The fix for the command bus's dispatch
    /// latency (noetl/ai-meta#205): under posture A every append paid its own
    /// ~4 ms `sync_data()` while holding the engine lock, which capped the bus at
    /// ~230 commands/s and turned the control plane's publish path into a queue.
    /// N records that arrive together now share one `fsync`.
    ///
    /// Durability is **unchanged**: this returns only after the `fsync` that
    /// covers every record in the batch, so a returned key is as durable as one
    /// from [`append`](Self::append). Ordering is unchanged: the writer still
    /// assigns each key ([`L0Engine::append_writer_assigned`]) under the same
    /// serialized lock, strictly increasing across the batch, so the ascending
    /// shard-log contract the #203 fix restored holds exactly as before.
    ///
    /// Followers are woken **once**, at the batch tip — a [`watch`] signal
    /// carries the latest value, and a woken follower drains its feed to the tip
    /// before parking again, so one wake per batch delivers every record in it.
    pub fn append_batch(&self, records: Vec<D::Record>) -> io::Result<Vec<u64>> {
        if records.is_empty() {
            return Ok(Vec::new());
        }
        // Cheap pre-check: a sealed writer never even contends for the lock.
        self.ensure_open()?;
        let (seqs, handles) = {
            let mut engine = self.engine.lock().unwrap();
            // Re-check under the lock — see `append` for why the pre-check
            // alone is not enough.
            self.ensure_open()?;
            let mut seqs = Vec::with_capacity(records.len());
            for record in records {
                seqs.push(engine.append_writer_assigned(record).map_err(io_err)?);
            }
            (seqs, engine.take_sync_handles().map_err(io_err)?)
        };
        commit(&handles)?;
        if let Some(tip) = seqs.last() {
            let _ = self.tip_tx.send(*tip);
        }
        Ok(seqs)
    }

    /// `Err` once the writer has been sealed for shutdown. The error is
    /// deliberately an ordinary `io::Error`: every publish path in this crate
    /// and its callers already treats one as "drop the connection and redial",
    /// which is the correct response to "this writer is going away".
    fn ensure_open(&self) -> io::Result<()> {
        if self.closed.load(Ordering::SeqCst) {
            return Err(io_err(
                "feed writer sealed for shutdown — republish to the replacement writer",
            ));
        }
        Ok(())
    }

    /// A shared handle to the underlying engine — for flush / inspection in
    /// harnesses and (later) the writer's own compaction ticks.
    pub fn engine(&self) -> Arc<Mutex<L0Engine<D>>> {
        Arc::clone(&self.engine)
    }

    /// A watch receiver that fires whenever a record is appended (the tip
    /// advances). For an **in-process** consumer co-located with the writer —
    /// the system-pool worker consuming its own shard's commands without a
    /// network hop: await [`changed()`](watch::Receiver::changed) to block until
    /// new records land, then drain via a [`ChangeFeed`] / `ShardConsumerGroup`
    /// over [`engine`](Self::engine). Pairs the sync consumer model with an
    /// async, no-poll-spin wait (the same signal the networked delivery uses).
    pub fn tip_receiver(&self) -> watch::Receiver<u64> {
        self.tip_tx.subscribe()
    }

    pub(crate) fn subscriber_handle(&self) -> (Arc<Mutex<L0Engine<D>>>, watch::Receiver<u64>) {
        (Arc::clone(&self.engine), self.tip_tx.subscribe())
    }
}

/// Accept subscriber connections on `listener` and push each one its shard's
/// change-feed from the requested cursor. Runs until the listener errors; spawn
/// it as a task. Each connection gets its own task and independent cursor.
pub async fn serve<D>(writer: Arc<FeedWriter<D>>, listener: TcpListener) -> io::Result<()>
where
    D: Dataset,
    D::Record: Serialize + DeserializeOwned + Clone + Send + 'static,
{
    loop {
        let (mut sock, _peer) = listener.accept().await?;
        configure_stream(&sock)?;
        let req_bytes = read_frame(&mut sock).await?;
        let req: SubscribeReq = serde_json::from_slice(&req_bytes).map_err(io_err)?;
        let (engine, rx) = writer.subscriber_handle();
        tokio::spawn(async move {
            let _ = push_loop::<D>(engine, rx, sock, req).await;
        });
    }
}

async fn push_loop<D>(
    engine: Arc<Mutex<L0Engine<D>>>,
    mut rx: watch::Receiver<u64>,
    mut sock: TcpStream,
    req: SubscribeReq,
) -> io::Result<()>
where
    D: Dataset,
    D::Record: Serialize + DeserializeOwned + Clone,
{
    let mut feed = ChangeFeed::new(req.shard, req.cursor);
    // noetl/ai-meta#225: a subscriber that asked for heartbeats gets one up
    // front, so it learns *immediately* that this writer heartbeats and can arm
    // its read deadline for the whole connection — rather than only after the
    // feed happens to go quiet for a beat. A subscriber that did not ask (a
    // pre-#225 client) is served exactly as before.
    let beat = req
        .heartbeat_ms
        .filter(|ms| *ms > 0)
        .map(Duration::from_millis);
    if beat.is_some() {
        write_frame(&mut sock, HEARTBEAT_FRAME).await?;
    }
    loop {
        let batch = {
            let engine = engine.lock().unwrap();
            feed.poll(&engine).map_err(io_err)?
        };
        if !batch.is_empty() {
            let body = serde_json::to_vec(&batch).map_err(io_err)?;
            write_frame(&mut sock, &body).await?;
            // Drain fully before parking: re-poll for anything appended since.
            continue;
        }
        // Caught up — park until the next append advances the tip. A race (append
        // between poll and here) already bumped the watch version, so this
        // returns immediately rather than sleeping through it.
        match beat {
            None => {
                if rx.changed().await.is_err() {
                    return Ok(()); // the writer was dropped
                }
            }
            Some(beat) => {
                // A heartbeat only *pauses* the park, it never abandons it: the
                // `watch` receiver keeps its version across the timeout, so no
                // append can be missed by a heartbeat tick.
                loop {
                    match tokio::time::timeout(beat, rx.changed()).await {
                        Ok(Ok(())) => break,
                        Ok(Err(_)) => return Ok(()), // the writer was dropped
                        Err(_) => write_frame(&mut sock, HEARTBEAT_FRAME).await?,
                    }
                }
            }
        }
    }
}

/// A subscriber connection to a [`FeedWriter`]'s shard feed.
pub struct FeedSubscription {
    sock: TcpStream,
    /// How long a parked read may go quiet before the peer is declared dead.
    /// Cleared once a peer proves it does not heartbeat, so an older writer
    /// never triggers a redial loop on a genuinely idle feed.
    read_deadline: Option<Duration>,
    /// Has this connection ever seen a heartbeat? Only then is a missed one
    /// evidence of a dead peer.
    peer_heartbeats: bool,
}

impl FeedSubscription {
    /// Connect to a feed server at `addr` and subscribe to `shard` from `cursor`
    /// (`0` = from the beginning; the writer's current tip = only new records).
    ///
    /// `addr` accepts any [`ToSocketAddrs`] — including a `host:port` **DNS
    /// name**, resolved at connect time. A Kubernetes service name therefore
    /// works directly and a pod-IP change is followed on reconnect, the same fix
    /// [`ClaimClient::connect`](crate::claim::ClaimClient::connect) carries. The
    /// previous `SocketAddr`-only signature made this subscription unusable from
    /// another pod without resolving the IP by hand.
    pub async fn connect<A: ToSocketAddrs>(addr: A, shard: u32, cursor: u64) -> io::Result<Self> {
        Self::connect_with_heartbeat(addr, shard, cursor, Some(DEFAULT_HEARTBEAT)).await
    }

    /// [`connect`](Self::connect) with an explicit heartbeat interval — `None`
    /// opts out of heartbeats entirely (keepalive still applies), which is only
    /// wanted in tests that assert the pre-#225 wire shape.
    pub async fn connect_with_heartbeat<A: ToSocketAddrs>(
        addr: A,
        shard: u32,
        cursor: u64,
        heartbeat: Option<Duration>,
    ) -> io::Result<Self> {
        let mut sock = TcpStream::connect(addr).await?;
        configure_stream(&sock)?;
        let req = serde_json::to_vec(&SubscribeReq {
            shard,
            cursor,
            heartbeat_ms: heartbeat.map(|hb| hb.as_millis() as u64),
        })
        .map_err(io_err)?;
        write_frame(&mut sock, &req).await?;
        Ok(Self {
            sock,
            read_deadline: heartbeat.map(|hb| hb * HEARTBEAT_MISS_FACTOR),
            peer_heartbeats: false,
        })
    }

    /// Receive the next delivered batch (one or more records in sort-key order).
    ///
    /// Parking here is unbounded by design — the feed may legitimately be idle
    /// for hours. What is *not* unbounded is waiting on a **dead** writer: while
    /// parked this consumes the writer's heartbeat frames, and once the peer has
    /// proven it heartbeats, [`HEARTBEAT_MISS_FACTOR`] missed beats return an
    /// error so the caller resubscribes (noetl/ai-meta#225). A writer that never
    /// heartbeats (a pre-#225 build) disarms the deadline on the first miss and
    /// liveness falls back to TCP keepalive alone.
    pub async fn recv_batch<R: DeserializeOwned>(&mut self) -> io::Result<Vec<R>> {
        loop {
            let body = match self.read_deadline {
                None => read_frame(&mut self.sock).await?,
                Some(deadline) => {
                    match tokio::time::timeout(deadline, read_frame(&mut self.sock)).await {
                        Ok(body) => body?,
                        Err(_) if self.peer_heartbeats => {
                            return Err(io_err(format!(
                                "events-feed writer stopped heartbeating for {}ms",
                                deadline.as_millis()
                            )));
                        }
                        Err(_) => {
                            // Never heartbeated: treat the peer as heartbeat-unaware
                            // rather than dead, and let keepalive own liveness.
                            self.read_deadline = None;
                            continue;
                        }
                    }
                }
            };
            if is_heartbeat(&body) {
                self.peer_heartbeats = true;
                continue;
            }
            return serde_json::from_slice(&body).map_err(io_err);
        }
    }

    /// Has the writer on this connection proven it sends heartbeats? Used by
    /// tests (and useful in diagnostics) to distinguish keepalive-only liveness
    /// from heartbeat-backed liveness.
    pub fn peer_heartbeats(&self) -> bool {
        self.peer_heartbeats
    }
}
