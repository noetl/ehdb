//! **L1 T4 — the networked claim RPC (competing consumers across processes).**
//!
//! The write half (`publish`) and the broadcast delivery (`serve`) don't give
//! *competing* consumption: `serve` fans every record to every subscriber. NATS
//! today gives a pool's N worker replicas one shared durable consumer so each
//! command goes to exactly one worker. This module is that role for the EHDB bus.
//!
//! A [`ClaimCoordinator`] holds **one** [`ShardConsumerGroup`] per shard — the
//! shared coordinator. [`serve_claims`] exposes it over the network; every worker
//! replica opens a [`ClaimClient`] and loops `claim_next → process → ack`. The
//! coordinator hands each command to exactly one caller (competing consumers) and
//! **redelivers** an unacked command after `ack_wait` (member crash → 0 loss),
//! reusing the T1 group's ack/ack_wait semantics — now shared across processes.
//!
//! `claim_next` **blocks** until a command is available (like NATS receive): the
//! coordinator polls the shared group; when the shard is caught up it parks on the
//! writer's tip signal (bounded by a poll interval so `ack_wait` redeliveries
//! surface even with no new appends), then re-competes. Wire protocol mirrors
//! [`crate::publish`]: length-prefixed JSON request in, JSON/ok response out,
//! `TCP_NODELAY`.

use std::sync::Arc;
use std::time::{Duration, Instant};

use ehdb_l0::Dataset;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex;

use crate::group::{MemberId, ShardConsumerGroup};
use crate::{io_err, read_frame, write_frame, FeedWriter};

/// Default cap on how long `claim_next` parks before re-polling, so an
/// `ack_wait` redelivery surfaces even with no new appends.
const DEFAULT_POLL_INTERVAL_MS: u64 = 250;

/// The shared per-shard claim coordinator: one [`ShardConsumerGroup`] behind an
/// async mutex, over the co-located writer's engine. Every worker replica claims
/// through it, so a command is delivered to exactly one member.
pub struct ClaimCoordinator<D: Dataset> {
    writer: Arc<FeedWriter<D>>,
    group: Mutex<ShardConsumerGroup<D>>,
    clock: Instant,
    poll_interval: Duration,
}

impl<D> ClaimCoordinator<D>
where
    D: Dataset,
    D::Record: Serialize + DeserializeOwned + Clone + Send + 'static,
{
    /// A coordinator over `writer`'s shard, redelivering unacked commands after
    /// `ack_wait`. `from_cursor = 0` replays the shard's undelivered tail.
    pub fn new(
        writer: Arc<FeedWriter<D>>,
        shard: u32,
        ack_wait: Duration,
        from_cursor: u64,
    ) -> Self {
        let ack_wait_ticks = ack_wait.as_millis() as u64;
        let poll_interval =
            Duration::from_millis(DEFAULT_POLL_INTERVAL_MS.min(ack_wait_ticks.max(1)));
        Self {
            group: Mutex::new(ShardConsumerGroup::new(shard, ack_wait_ticks, from_cursor)),
            writer,
            clock: Instant::now(),
            poll_interval,
        }
    }

    fn now_ticks(&self) -> u64 {
        self.clock.elapsed().as_millis() as u64
    }

    /// Claim the next command for `member`, **blocking** until one is available
    /// (a fresh command or an `ack_wait`-expired redelivery). Exactly-one-member
    /// delivery is enforced by the shared group.
    pub async fn claim_next(&self, member: MemberId) -> crate::group::Delivery<D::Record> {
        let mut tip_rx = self.writer.tip_receiver();
        loop {
            let assigned = {
                // Async lock FIRST (may await), then the engine's sync lock — no
                // std guard is ever held across an await.
                let mut group = self.group.lock().await;
                let engine = self.writer.engine();
                let e = engine.lock().unwrap();
                group.poll_assign(&e, member, self.now_ticks())
            };
            match assigned {
                Ok(Some(delivery)) => return delivery,
                Ok(None) => {
                    // Caught up: park for a new append or the poll interval (so an
                    // expired in-flight record is re-competed even with no append).
                    let _ = tokio::time::timeout(self.poll_interval, tip_rx.changed()).await;
                }
                Err(_) => {
                    // A read error is transient here (the log is durable); back off
                    // a beat and retry rather than drop the member.
                    tokio::time::sleep(self.poll_interval).await;
                }
            }
        }
    }

    /// Ack a claimed command (commit; do not redeliver). Returns `true` if it was
    /// in flight.
    pub async fn ack(&self, sort_key: u64) -> bool {
        self.group.lock().await.ack(sort_key)
    }

    /// Nack a claimed command — leave it in flight so it redelivers to another
    /// member after `ack_wait` (the at-least-once path; the group's timer owns
    /// the redelivery, so this is a no-op beyond declining the ack).
    pub async fn nack(&self, _sort_key: u64) {}

    /// The shard's current backlog (undelivered + in-flight-unacked) — the KEDA
    /// lag value for this shard (see [`crate::scaler`]).
    pub async fn lag(&self) -> u64 {
        let group = self.group.lock().await;
        let engine = self.writer.engine();
        let e = engine.lock().unwrap();
        group.lag(&e).unwrap_or(0)
    }
}

/// A claim request on the wire.
#[derive(Debug, Clone, Serialize, Deserialize)]
enum ClaimReq {
    /// Block until a command is assigned to `member`.
    Next { member: MemberId },
    /// Ack a claimed command.
    Ack { sort_key: u64 },
    /// Nack a claimed command (redeliver after ack_wait).
    Nack { sort_key: u64 },
}

/// A claimed command on the wire.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct ClaimResp<R> {
    sort_key: u64,
    redelivered: bool,
    record: R,
}

/// Accept claim connections on `listener` and serve each from the shared
/// `coordinator`. Runs until the listener errors; spawn it as a task. Each
/// connection is one member looping `Next → (process) → Ack`.
pub async fn serve_claims<D>(
    listener: TcpListener,
    coordinator: Arc<ClaimCoordinator<D>>,
) -> std::io::Result<()>
where
    D: Dataset,
    D::Record: Serialize + DeserializeOwned + Clone + Send + 'static,
{
    loop {
        let (mut sock, _peer) = listener.accept().await?;
        sock.set_nodelay(true)?;
        let coordinator = Arc::clone(&coordinator);
        tokio::spawn(async move {
            loop {
                let body = match read_frame(&mut sock).await {
                    Ok(b) => b,
                    Err(_) => return,
                };
                let req: ClaimReq = match serde_json::from_slice(&body) {
                    Ok(r) => r,
                    Err(_) => return,
                };
                match req {
                    ClaimReq::Next { member } => {
                        let delivery = coordinator.claim_next(member).await;
                        let resp = ClaimResp {
                            sort_key: delivery.sort_key,
                            redelivered: delivery.redelivered,
                            record: delivery.record,
                        };
                        let bytes = match serde_json::to_vec(&resp) {
                            Ok(b) => b,
                            Err(_) => return,
                        };
                        if write_frame(&mut sock, &bytes).await.is_err() {
                            return;
                        }
                    }
                    ClaimReq::Ack { sort_key } => {
                        coordinator.ack(sort_key).await;
                        if write_frame(&mut sock, b"1").await.is_err() {
                            return;
                        }
                    }
                    ClaimReq::Nack { sort_key } => {
                        coordinator.nack(sort_key).await;
                        if write_frame(&mut sock, b"1").await.is_err() {
                            return;
                        }
                    }
                }
            }
        });
    }
}

/// One claimed command, delivered to a [`ClaimClient`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Claimed<R> {
    pub sort_key: u64,
    pub redelivered: bool,
    pub record: R,
}

/// A worker replica's connection to its shard's claim coordinator. One member
/// competing with the pool's other replicas.
pub struct ClaimClient {
    sock: TcpStream,
    member: MemberId,
}

impl ClaimClient {
    /// Connect to a claim server as `member`.
    pub async fn connect(addr: std::net::SocketAddr, member: MemberId) -> std::io::Result<Self> {
        let sock = TcpStream::connect(addr).await?;
        sock.set_nodelay(true)?;
        Ok(Self { sock, member })
    }

    /// Claim the next command (blocks until one is assigned to this member).
    pub async fn claim_next<R: DeserializeOwned>(&mut self) -> std::io::Result<Claimed<R>> {
        let req = serde_json::to_vec(&ClaimReq::Next {
            member: self.member,
        })
        .map_err(io_err)?;
        write_frame(&mut self.sock, &req).await?;
        let body = read_frame(&mut self.sock).await?;
        let resp: ClaimResp<R> = serde_json::from_slice(&body).map_err(io_err)?;
        Ok(Claimed {
            sort_key: resp.sort_key,
            redelivered: resp.redelivered,
            record: resp.record,
        })
    }

    /// Ack a claimed command by its sort key.
    pub async fn ack(&mut self, sort_key: u64) -> std::io::Result<()> {
        let req = serde_json::to_vec(&ClaimReq::Ack { sort_key }).map_err(io_err)?;
        write_frame(&mut self.sock, &req).await?;
        let _ = read_frame(&mut self.sock).await?;
        Ok(())
    }

    /// Nack a claimed command (redeliver after ack_wait).
    pub async fn nack(&mut self, sort_key: u64) -> std::io::Result<()> {
        let req = serde_json::to_vec(&ClaimReq::Nack { sort_key }).map_err(io_err)?;
        write_frame(&mut self.sock, &req).await?;
        let _ = read_frame(&mut self.sock).await?;
        Ok(())
    }
}
