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

pub mod group;
pub mod publish;
pub mod scaler;
pub mod sse;
pub use group::{Delivery, MemberId, ShardConsumerGroup};
pub use publish::{serve_ingest, PublishClient, PublishRouter};
pub use scaler::{render_prometheus, ShardLag};

use std::io;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use ehdb_l0::{ChangeFeed, Dataset, L0Engine};
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
    pub fn new(engine: L0Engine<D>) -> Self {
        let tip = engine.global_sequence();
        let (tip_tx, _rx) = watch::channel(tip);
        Self {
            engine: Arc::new(Mutex::new(engine)),
            tip_tx,
        }
    }

    /// Append one record to the durable log and wake followers. Returns the sort
    /// key. This is the server→writer publish seam (the control plane calls it).
    pub fn append(&self, record: D::Record) -> io::Result<u64> {
        let seq = {
            let mut engine = self.engine.lock().unwrap();
            engine.append_record(record).map_err(io_err)?
        };
        // Ignore send errors: no live subscribers is fine (shadow tier).
        let _ = self.tip_tx.send(seq);
        Ok(seq)
    }

    /// A shared handle to the underlying engine — for flush / inspection in
    /// harnesses and (later) the writer's own compaction ticks.
    pub fn engine(&self) -> Arc<Mutex<L0Engine<D>>> {
        Arc::clone(&self.engine)
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
        sock.set_nodelay(true)?;
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
        sock.set_nodelay(true)?;
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
