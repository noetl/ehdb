//! **L1 T4 — the networked publish path (server → writer).**
//!
//! The write half of topology (c). Today [`FeedWriter::append`] is an in-process
//! call; in the deployed shape the **stateless control plane** (noetl-server)
//! runs in a different process from the **per-shard writer** (co-located in the
//! system-pool worker). This module is the seam between them: the server opens a
//! [`PublishClient`] to the writer's ingest port and publishes each command
//! record; the writer's [`serve_ingest`] loop appends it to the durable log
//! (assigning it to the record's shard, signalling followers) and returns the
//! **assigned sort key** as a durable ack. This mirrors what publishing to NATS
//! does today — one network hop, server not in the delivery path.
//!
//! [`PublishRouter`] is the server's fan-out: given the shard writers' addresses,
//! it routes each record to the writer that owns the record's shard
//! ([`Dataset::partition`]) — the analog of `NOETL_SHARD_SUBJECT_ROUTE`.
//!
//! Wire protocol mirrors the delivery transport: a length-prefixed JSON record
//! frame in, an 8-byte big-endian sort-key ack out (request/response per record,
//! so the publisher has an at-least-once durable confirmation). `TCP_NODELAY` on
//! both ends.
//!
//! **Sort-key ownership:** the publisher sends a fully-formed record whose sort
//! key is already set — for the command bus that key is the command's identity
//! (its monotonic id), so the server assigns it, exactly as it assigns command
//! ids today. The writer enforces the single-writer ascending-sort-key contract
//! per shard on append.

use std::collections::{BTreeMap, VecDeque};
use std::io;
use std::marker::PhantomData;
use std::sync::{Arc, Mutex};

use ehdb_l0::Dataset;
use serde::de::DeserializeOwned;
use serde::Serialize;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream, ToSocketAddrs};
use tokio::sync::{mpsc, oneshot};

use crate::{io_err, read_frame, write_frame, FeedWriter};

/// The most records one group commit will cover. A cap, not a target: the
/// committer takes whatever has *already* arrived and never waits to fill a
/// batch, so latency is never traded for batching — under light load a batch is
/// one record and behaves exactly as before.
const MAX_COMMIT_BATCH: usize = 512;

/// Accept publisher connections on `listener` and append each published record
/// to `writer`, returning its assigned sort key. Runs until the listener errors;
/// spawn it as a task. A publisher holds one connection and streams records,
/// reading back one sort-key ack per record, **in request order**.
///
/// **Group commit (noetl/ai-meta#205).** Each connection is served by two tasks:
/// a reader that decodes frames as fast as they arrive, and a committer that
/// takes *everything already queued* and appends it via
/// [`FeedWriter::append_batch`] — one engine lock, one `fsync`, one follower
/// wake for the whole batch. The committer never waits to fill a batch, so a
/// lone record still commits immediately; under concurrent publish the ~4 ms
/// `sync_data()` that used to be paid per record is paid once per batch. Acks
/// are still written one per record in request order, only after the `fsync`
/// that covers them — the durable-ack contract is unchanged.
pub async fn serve_ingest<D>(listener: TcpListener, writer: Arc<FeedWriter<D>>) -> io::Result<()>
where
    D: Dataset,
    D::Record: Serialize + DeserializeOwned + Clone + Send + 'static,
{
    loop {
        let (sock, _peer) = listener.accept().await?;
        crate::configure_stream(&sock)?;
        let writer = Arc::clone(&writer);
        tokio::spawn(async move {
            let (mut rd, mut wr) = sock.into_split();
            let (tx, mut rx) = mpsc::channel::<D::Record>(MAX_COMMIT_BATCH);

            // Reader: decode frames off the wire as fast as they land.
            tokio::spawn(async move {
                loop {
                    let body = match read_frame(&mut rd).await {
                        Ok(b) => b,
                        Err(_) => return, // publisher disconnected
                    };
                    let Ok(record) = serde_json::from_slice::<D::Record>(&body) else {
                        return;
                    };
                    if tx.send(record).await.is_err() {
                        return;
                    }
                }
            });

            // Committer: one group commit per drain of whatever has arrived.
            let mut batch: Vec<D::Record> = Vec::with_capacity(MAX_COMMIT_BATCH);
            loop {
                match rx.recv().await {
                    Some(first) => batch.push(first),
                    None => return, // reader gone
                }
                // Take only what is already queued — never wait to batch.
                while batch.len() < MAX_COMMIT_BATCH {
                    match rx.try_recv() {
                        Ok(record) => batch.push(record),
                        Err(_) => break,
                    }
                }
                let seqs = match writer.append_batch(std::mem::take(&mut batch)) {
                    Ok(s) => s,
                    Err(_) => return,
                };
                for seq in seqs {
                    if wr.write_all(&seq.to_be_bytes()).await.is_err() {
                        return;
                    }
                }
                if wr.flush().await.is_err() {
                    return;
                }
            }
        });
    }
}

/// A single connection to one shard writer's ingest port.
pub struct PublishClient {
    sock: TcpStream,
}

impl PublishClient {
    /// Connect to a writer's ingest endpoint. `addr` accepts any
    /// [`ToSocketAddrs`] — including a `host:port` **DNS name** (a Kubernetes
    /// service name), resolved by `TcpStream::connect` (finding-#2 fix).
    pub async fn connect<A: ToSocketAddrs>(addr: A) -> io::Result<Self> {
        let sock = TcpStream::connect(addr).await?;
        crate::configure_stream(&sock)?;
        Ok(Self { sock })
    }

    /// Publish one record and await the writer-assigned sort key (durable ack).
    pub async fn publish<R: Serialize>(&mut self, record: &R) -> io::Result<u64> {
        let body = serde_json::to_vec(record).map_err(io_err)?;
        write_frame(&mut self.sock, &body).await?;
        let mut seq = [0u8; 8];
        self.sock.read_exact(&mut seq).await?;
        Ok(u64::from_be_bytes(seq))
    }
}

/// A **pipelined** connection to one shard writer's ingest port: many records
/// may be in flight at once on the single socket.
///
/// The serial [`PublishClient`] holds the socket for a whole round-trip, so a
/// control plane publishing concurrently serialises — every publisher waits a
/// full RTT (plus the writer's `fsync`) for the one in front of it, and the
/// writer never sees two records at once so it can never group-commit them
/// (noetl/ai-meta#205). This client decouples the two directions: a writer task
/// streams request frames out as they are submitted, and a reader task matches
/// each 8-byte sort-key ack back to its waiter. `serve_ingest` responds strictly
/// in request order on a connection, so FIFO matching is exact.
///
/// [`publish`](Self::publish) takes `&self`, so callers need no exclusive lock.
/// When the socket dies every outstanding and subsequent publish fails, so the
/// owner drops the client and redials (the control plane already does this).
pub struct PipelinedPublishClient {
    tx: mpsc::UnboundedSender<(Vec<u8>, oneshot::Sender<u64>)>,
}

impl PipelinedPublishClient {
    /// Connect to a writer's ingest endpoint. `addr` accepts any
    /// [`ToSocketAddrs`] — including a `host:port` **DNS name** (a Kubernetes
    /// service name), resolved by `TcpStream::connect` (finding-#2 fix).
    pub async fn connect<A: ToSocketAddrs>(addr: A) -> io::Result<Self> {
        let sock = TcpStream::connect(addr).await?;
        crate::configure_stream(&sock)?;
        let (mut rd, mut wr) = sock.into_split();
        let (tx, mut rx) = mpsc::unbounded_channel::<(Vec<u8>, oneshot::Sender<u64>)>();

        // Waiters in request order. The writer task pushes a waiter *before* the
        // frame it belongs to reaches the wire, so the reader can never see an
        // ack whose waiter is not yet queued.
        let waiters: Arc<Mutex<VecDeque<oneshot::Sender<u64>>>> =
            Arc::new(Mutex::new(VecDeque::new()));

        let wr_waiters = Arc::clone(&waiters);
        let writer_task = tokio::spawn(async move {
            while let Some((body, responder)) = rx.recv().await {
                wr_waiters.lock().unwrap().push_back(responder);
                if write_frame(&mut wr, &body).await.is_err() {
                    break;
                }
            }
            // Dropping the queue drops every waiter's sender: each pending
            // `publish` sees a closed oneshot and reports the connection dead.
            wr_waiters.lock().unwrap().clear();
        });

        let rd_waiters = Arc::clone(&waiters);
        tokio::spawn(async move {
            loop {
                let mut seq = [0u8; 8];
                if rd.read_exact(&mut seq).await.is_err() {
                    break;
                }
                let waiter = rd_waiters.lock().unwrap().pop_front();
                match waiter {
                    Some(w) => {
                        let _ = w.send(u64::from_be_bytes(seq));
                    }
                    None => break, // ack with no waiter — the stream is desynced
                }
            }
            // No more acks will ever arrive, so stop the writer *before* draining:
            // a half-closed socket (read side dead, write side still accepting)
            // would otherwise keep queueing waiters that nothing can ever resolve,
            // and those `publish` calls would hang instead of failing. Aborting
            // drops `rx`, so every later `publish` fails fast on a closed channel;
            // the writer can no longer enqueue a waiter because it only does so
            // after `rx.recv()`, an await point it cannot pass once cancelled.
            writer_task.abort();
            rd_waiters.lock().unwrap().clear();
        });

        Ok(Self { tx })
    }

    /// Publish one record and await the writer-assigned sort key (durable ack).
    /// Concurrent calls pipeline: each frame goes out as submitted rather than
    /// waiting for the previous round-trip.
    pub async fn publish<R: Serialize>(&self, record: &R) -> io::Result<u64> {
        let body = serde_json::to_vec(record).map_err(io_err)?;
        let (responder, response) = oneshot::channel();
        self.tx
            .send((body, responder))
            .map_err(|_| io_err("ehdb publish connection closed"))?;
        response
            .await
            .map_err(|_| io_err("ehdb publish connection closed before ack"))
    }
}

/// The control plane's shard-routing publisher: holds a
/// [`PipelinedPublishClient`] per shard writer and routes each record to the
/// writer that owns its shard. `&self` throughout — concurrent publishes share
/// the router without an exclusive lock, which is what lets the writer see (and
/// group-commit) more than one record at a time.
pub struct PublishRouter<D: Dataset> {
    shard_count: u32,
    clients: BTreeMap<u32, PipelinedPublishClient>,
    _marker: PhantomData<fn() -> D>,
}

impl<D> PublishRouter<D>
where
    D: Dataset,
    D::Record: Serialize,
{
    /// Connect to every shard writer. `addrs` maps shard → the writer's ingest
    /// address as a `host:port` string (a DNS name or `ip:port`, resolved at
    /// connect time — finding-#2 fix); `shard_count` is the routing modulus
    /// (must match the writers').
    pub async fn connect(shard_count: u32, addrs: BTreeMap<u32, String>) -> io::Result<Self> {
        let mut clients = BTreeMap::new();
        for (shard, addr) in addrs {
            clients.insert(shard, PipelinedPublishClient::connect(addr).await?);
        }
        Ok(Self {
            shard_count,
            clients,
            _marker: PhantomData,
        })
    }

    /// The shard a record routes to.
    pub fn shard_of(&self, record: &D::Record) -> u32 {
        D::partition(record, self.shard_count)
    }

    /// Publish `record` to the writer that owns its shard; returns the assigned
    /// sort key. Errors if no writer is configured for that shard.
    pub async fn publish(&self, record: &D::Record) -> io::Result<u64> {
        let shard = self.shard_of(record);
        let client = self
            .clients
            .get(&shard)
            .ok_or_else(|| io_err(format!("no writer configured for shard {shard}")))?;
        client.publish(record).await
    }
}
