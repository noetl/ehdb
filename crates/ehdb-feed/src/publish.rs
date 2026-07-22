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

use std::collections::BTreeMap;
use std::io;
use std::marker::PhantomData;
use std::sync::Arc;

use ehdb_l0::Dataset;
use serde::de::DeserializeOwned;
use serde::Serialize;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream, ToSocketAddrs};

use crate::{io_err, read_frame, write_frame, FeedWriter};

/// Accept publisher connections on `listener` and append each published record
/// to `writer`, returning its assigned sort key. Runs until the listener errors;
/// spawn it as a task. A publisher holds one connection and streams records,
/// reading back one sort-key ack per record.
pub async fn serve_ingest<D>(listener: TcpListener, writer: Arc<FeedWriter<D>>) -> io::Result<()>
where
    D: Dataset,
    D::Record: Serialize + DeserializeOwned + Clone + Send + 'static,
{
    loop {
        let (mut sock, _peer) = listener.accept().await?;
        sock.set_nodelay(true)?;
        let writer = Arc::clone(&writer);
        tokio::spawn(async move {
            loop {
                let body = match read_frame(&mut sock).await {
                    Ok(b) => b,
                    Err(_) => return, // publisher disconnected
                };
                let record: D::Record = match serde_json::from_slice(&body) {
                    Ok(r) => r,
                    Err(_) => return,
                };
                let seq = match writer.append(record) {
                    Ok(s) => s,
                    Err(_) => return,
                };
                if sock.write_all(&seq.to_be_bytes()).await.is_err() || sock.flush().await.is_err()
                {
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
        sock.set_nodelay(true)?;
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

/// The control plane's shard-routing publisher: holds a [`PublishClient`] per
/// shard writer and routes each record to the writer that owns its shard.
pub struct PublishRouter<D: Dataset> {
    shard_count: u32,
    clients: BTreeMap<u32, PublishClient>,
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
            clients.insert(shard, PublishClient::connect(addr).await?);
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
    pub async fn publish(&mut self, record: &D::Record) -> io::Result<u64> {
        let shard = self.shard_of(record);
        let client = self
            .clients
            .get_mut(&shard)
            .ok_or_else(|| io_err(format!("no writer configured for shard {shard}")))?;
        client.publish(record).await
    }
}
