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

use ehdb_l0::{Dataset, EventRecord};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use tokio::net::{TcpListener, TcpStream, ToSocketAddrs};
use tokio::sync::Mutex;

use crate::group::{MemberId, PoolTag, RouteFn, RoutedConsumerGroup};
use crate::{io_err, read_frame, write_frame, FeedWriter};

/// The default pool tag — matches the server's default `execution_pool`
/// (`shared`, the segment non-`system/`/`subscription/` playbooks land on).
/// A record whose `execution_pool` is absent/blank, and a worker that declares
/// no pool, both fall back here — never a wildcard, so isolation holds.
pub const DEFAULT_POOL: &str = "shared";

/// The D1 command-bus [`RouteFn`]: read `execution_pool` from the command
/// notification JSON in an [`EventRecord`]'s payload, defaulting to
/// [`DEFAULT_POOL`]. One source of truth for the routing key the server stamps
/// (`execute.rs` → `"execution_pool": pool_segment`) and the pool a worker
/// declares (its `NATS_FILTER_SUBJECT` segment) — the honest equivalent of the
/// NATS subject `noetl.commands.<pool>.>`.
pub fn d1_execution_pool_route() -> RouteFn<EventRecord> {
    Arc::new(|rec: &EventRecord| -> PoolTag {
        serde_json::from_str::<serde_json::Value>(&rec.payload)
            .ok()
            .and_then(|v| {
                v.get("execution_pool")
                    .and_then(|p| p.as_str())
                    .map(str::to_string)
            })
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| DEFAULT_POOL.to_string())
    })
}

/// Default cap on how long `claim_next` parks before re-polling, so an
/// `ack_wait` redelivery surfaces even with no new appends.
const DEFAULT_POLL_INTERVAL_MS: u64 = 250;

/// The shared per-shard claim coordinator: one [`ShardConsumerGroup`] behind an
/// async mutex, over the co-located writer's engine. Every worker replica claims
/// through it, so a command is delivered to exactly one member.
pub struct ClaimCoordinator<D: Dataset> {
    writer: Arc<FeedWriter<D>>,
    group: Mutex<RoutedConsumerGroup<D>>,
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
    /// `route` maps each record to its target pool so a member claims only
    /// within the pool it declares (`system` ⇄ `shared` isolation,
    /// noetl/ai-meta#194); use [`d1_execution_pool_route`] for the command bus.
    pub fn new(
        writer: Arc<FeedWriter<D>>,
        shard: u32,
        ack_wait: Duration,
        from_cursor: u64,
        route: RouteFn<D::Record>,
    ) -> Self {
        let ack_wait_ticks = ack_wait.as_millis() as u64;
        let poll_interval =
            Duration::from_millis(DEFAULT_POLL_INTERVAL_MS.min(ack_wait_ticks.max(1)));
        Self {
            group: Mutex::new(RoutedConsumerGroup::new(
                shard,
                ack_wait_ticks,
                from_cursor,
                route,
            )),
            writer,
            clock: Instant::now(),
            poll_interval,
        }
    }

    fn now_ticks(&self) -> u64 {
        self.clock.elapsed().as_millis() as u64
    }

    /// Claim the next command **for `pool`** for `member`, **blocking** until
    /// one is available (a fresh command or an `ack_wait`-expired redelivery).
    /// Exactly-one-member delivery within the pool is enforced by the shared
    /// group; a command for another pool is never assigned here (isolation).
    pub async fn claim_next(
        &self,
        pool: &str,
        member: MemberId,
    ) -> crate::group::Delivery<D::Record> {
        let mut tip_rx = self.writer.tip_receiver();
        loop {
            let assigned = {
                // Async lock FIRST (may await), then the engine's sync lock — no
                // std guard is ever held across an await.
                let mut group = self.group.lock().await;
                let engine = self.writer.engine();
                let e = engine.lock().unwrap();
                group.poll_assign(&e, pool, member, self.now_ticks())
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
    /// Block until a command **for `pool`** is assigned to `member`. `pool` is
    /// the member's declared pool segment; the coordinator only ever hands it a
    /// command whose `execution_pool` matches (strict isolation).
    Next { member: MemberId, pool: PoolTag },
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
                    ClaimReq::Next { member, pool } => {
                        let delivery = coordinator.claim_next(&pool, member).await;
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
    pool: PoolTag,
}

impl ClaimClient {
    /// Connect to a claim server as `member` claiming for `pool`.
    ///
    /// `addr` accepts any [`ToSocketAddrs`] — including a `host:port`
    /// **DNS name** (`noetl-cmdbus-writer.noetl.svc.cluster.local:9101`),
    /// which `TcpStream::connect` resolves at connect time. This is the
    /// finding-#2 fix: a Kubernetes service name works directly, so no
    /// ClusterIP-only workaround and pod-IP changes are followed on reconnect.
    pub async fn connect<A: ToSocketAddrs>(
        addr: A,
        member: MemberId,
        pool: impl Into<PoolTag>,
    ) -> std::io::Result<Self> {
        let sock = TcpStream::connect(addr).await?;
        sock.set_nodelay(true)?;
        Ok(Self {
            sock,
            member,
            pool: pool.into(),
        })
    }

    /// Claim the next command (blocks until one is assigned to this member).
    pub async fn claim_next<R: DeserializeOwned>(&mut self) -> std::io::Result<Claimed<R>> {
        let req = serde_json::to_vec(&ClaimReq::Next {
            member: self.member,
            pool: self.pool.clone(),
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
