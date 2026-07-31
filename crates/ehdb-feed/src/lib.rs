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
pub mod publish;
pub mod scaler;
pub mod sse;
pub mod subject;
pub use claim::{
    d1_command_subject, serve_claims, ClaimClient, ClaimCoordinator, Claimed, DEFAULT_POOL,
};
pub use cursor::{CursorFallback, CursorOrigin, CursorStore, ResumeReport};
pub use group::{Delivery, MemberId, ShardConsumerGroup, SubjectConsumerGroup};
pub use publish::{serve_ingest, PipelinedPublishClient, PublishClient, PublishRouter};
pub use scaler::{
    bind_and_serve_snapshot_with_resume, render_prometheus, render_resume, render_snapshot,
    LagSnapshot, ShardLag, SubjectLag,
};
pub use subject::{Subject, SubjectFilter, SubjectFn};

use std::io;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use ehdb_l0::{ChangeFeed, Dataset, FlushPolicy, L0Engine};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::watch;

/// A subscriber's request: the shard to follow and the resume cursor (sort key
/// of the last record it already has; `0` = from the beginning).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubscribeReq {
    pub shard: u32,
    pub cursor: u64,
}

pub(crate) fn io_err<E: std::fmt::Display>(err: E) -> io::Error {
    io::Error::other(err.to_string())
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
        }
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
        let (seq, handles) = {
            let mut engine = self.engine.lock().unwrap();
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
        let (seqs, handles) = {
            let mut engine = self.engine.lock().unwrap();
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
        if rx.changed().await.is_err() {
            return Ok(()); // the writer was dropped
        }
    }
}

/// A subscriber connection to a [`FeedWriter`]'s shard feed.
pub struct FeedSubscription {
    sock: TcpStream,
}

impl FeedSubscription {
    /// Connect to a feed server at `addr` and subscribe to `shard` from `cursor`
    /// (`0` = from the beginning; the writer's current tip = only new records).
    pub async fn connect(addr: SocketAddr, shard: u32, cursor: u64) -> io::Result<Self> {
        let mut sock = TcpStream::connect(addr).await?;
        configure_stream(&sock)?;
        let req = serde_json::to_vec(&SubscribeReq { shard, cursor }).map_err(io_err)?;
        write_frame(&mut sock, &req).await?;
        Ok(Self { sock })
    }

    /// Receive the next delivered batch (one or more records in sort-key order).
    pub async fn recv_batch<R: DeserializeOwned>(&mut self) -> io::Result<Vec<R>> {
        let body = read_frame(&mut self.sock).await?;
        serde_json::from_slice(&body).map_err(io_err)
    }
}
