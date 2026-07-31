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
//!
//! **Liveness (noetl/ai-meta#208).** A blocking-until-available read is exactly
//! the shape that cannot tell "the writer has nothing for me" from "the writer is
//! gone", so a claim connection needs its own liveness signal or a writer restart
//! wedges every claimer silently. Two independent mechanisms cover it:
//!
//! 1. **TCP keepalive** on both ends ([`crate::configure_stream`]) — a dead peer
//!    becomes an ordinary read error in ~11 s even when no FIN/RST ever arrives,
//!    which is the usual case when a pod is deleted under Kubernetes.
//! 2. **A coordinator heartbeat**, negotiated per connection: a client that asks
//!    for one (`heartbeat_ms` on [`ClaimReq::Next`]) gets one heartbeat frame up
//!    front — proving the peer speaks heartbeats, so the client's read deadline of
//!    [`HEARTBEAT_MISS_FACTOR`] missed beats is armed for the whole connection —
//!    and another whenever a claim has parked that long. This also catches a writer
//!    that is *alive but stuck*, which keepalive cannot: keepalive is answered by
//!    the peer's kernel.
//!
//! Both are backward compatible: heartbeats are only sent to a client that asked,
//! and a client whose peer never heartbeats disarms its deadline and falls back to
//! keepalive-only.

use std::sync::Arc;
use std::time::{Duration, Instant};

use ehdb_l0::{shard_for_execution, Dataset, EventRecord};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use tokio::net::{TcpListener, TcpStream, ToSocketAddrs};
use tokio::sync::Mutex;

use crate::cursor::{CursorFallback, CursorOrigin, CursorStore, ResumeReport};
use crate::group::{MemberId, SubjectConsumerGroup};
use crate::subject::{Subject, SubjectFn};
use crate::{io_err, read_frame, write_frame, FeedWriter};

/// The default pool token — matches the server's default `execution_pool`
/// (`shared`, the segment non-`system/`/`subscription/` playbooks land on). A
/// record whose `execution_pool` is absent/blank falls back here — never a
/// wildcard, so isolation holds.
pub const DEFAULT_POOL: &str = "shared";

/// The D1 command-bus [`SubjectFn`]: derive a record's routing
/// [`Subject`](crate::subject::Subject) — `commands.<pool>.shard.<n>` — from the
/// command notification. `<pool>` is `execution_pool` from the notification JSON
/// the server stamps (`execute.rs` → `"execution_pool": pool_segment`, default
/// [`DEFAULT_POOL`]); `<n>` is `shard_for_execution(execution_id, shard_count)`,
/// byte-identical to the server/worker `shard_for`. One source of truth for the
/// subject a worker's [`SubjectFilter`](crate::subject::SubjectFilter) matches —
/// the honest equivalent of the NATS subject `noetl.commands.<pool>.shard.<n>`.
pub fn d1_command_subject(shard_count: u32) -> SubjectFn<EventRecord> {
    let shard_count = shard_count.max(1);
    Arc::new(move |rec: &EventRecord| -> Subject {
        let pool = serde_json::from_str::<serde_json::Value>(&rec.payload)
            .ok()
            .and_then(|v| {
                v.get("execution_pool")
                    .and_then(|p| p.as_str())
                    .map(str::to_string)
            })
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| DEFAULT_POOL.to_string());
        let shard = shard_for_execution(&rec.execution_id, shard_count);
        Subject::command(&pool, shard)
    })
}

/// Default cap on how long `claim_next` parks before re-polling, so an
/// `ack_wait` redelivery surfaces even with no new appends.
const DEFAULT_POLL_INTERVAL_MS: u64 = 250;

/// How often a parked `claim_next` proves the coordinator is still alive to a
/// client that asked for heartbeats (noetl/ai-meta#208).
pub const DEFAULT_HEARTBEAT: Duration = Duration::from_secs(5);

/// How many consecutive heartbeats a client waits for before it calls the
/// connection dead and redials. Three gives a ~15 s detection window with the
/// default interval — slack enough that a busy coordinator is never mistaken for a
/// dead one, short enough that dispatch resumes in seconds.
pub const HEARTBEAT_MISS_FACTOR: u32 = 3;

/// The heartbeat frame the coordinator sends on a parked claim. A distinct frame
/// rather than a variant of [`ClaimResp`], so the claim response stays
/// byte-identical on the wire and a pre-#208 client — which never asks for
/// heartbeats and so never receives one — is unaffected.
const HEARTBEAT_FRAME: &[u8] = b"{\"heartbeat\":true}";

#[derive(Debug, Clone, Copy, Deserialize)]
struct HeartbeatFrame {
    heartbeat: bool,
}

/// Is this frame a coordinator heartbeat rather than a claim? Unambiguous: a
/// [`ClaimResp`] frame has no `heartbeat` field, so it fails to decode here.
fn is_heartbeat(body: &[u8]) -> bool {
    serde_json::from_slice::<HeartbeatFrame>(body)
        .map(|h| h.heartbeat)
        .unwrap_or(false)
}

/// The shared per-shard claim coordinator: one [`ShardConsumerGroup`] behind an
/// async mutex, over the co-located writer's engine. Every worker replica claims
/// through it, so a command is delivered to exactly one member.
pub struct ClaimCoordinator<D: Dataset> {
    writer: Arc<FeedWriter<D>>,
    group: Mutex<SubjectConsumerGroup<D>>,
    clock: Instant,
    poll_interval: Duration,
    /// Where this coordinator's committed cursor is persisted, when the host
    /// asked for durable progress ([`resume`](Self::resume)).
    cursor_store: Option<CursorStore>,
    /// The cursor this coordinator started from, and how it was chosen — for the
    /// host's one-line restart log.
    started_at: (u64, CursorOrigin),
    /// The full resume story, when this coordinator came up through
    /// [`resume`](Self::resume): what was stored, the reopened log's tip, and
    /// what was actually used (noetl/ai-meta#208).
    resume_report: Option<ResumeReport>,
}

impl<D> ClaimCoordinator<D>
where
    D: Dataset,
    D::Record: Serialize + DeserializeOwned + Clone + Send + 'static,
{
    /// A coordinator over `writer`'s shard, redelivering unacked commands after
    /// `ack_wait`. `from_cursor = 0` replays the shard's undelivered tail.
    /// `subject_of` maps each record to its routing subject so a member claims
    /// only within its subscribed subjects (pool + shard isolation,
    /// noetl/ai-meta#194); use [`d1_command_subject`] for the command bus.
    pub fn new(
        writer: Arc<FeedWriter<D>>,
        shard: u32,
        ack_wait: Duration,
        from_cursor: u64,
        subject_of: SubjectFn<D::Record>,
    ) -> Self {
        Self::build(
            writer,
            shard,
            ack_wait,
            from_cursor,
            subject_of,
            None,
            CursorOrigin::Persisted,
            None,
        )
    }

    /// A coordinator that **resumes where the last one left off** — the
    /// writer-restart fix (noetl/ai-meta#208).
    ///
    /// Reads the committed cursor back out of `store` and starts the group there,
    /// so a restarted writer re-serves only what was genuinely unacked. Without
    /// this, every restart rebuilt the group at `from_cursor = 0` and re-delivered
    /// the entire shard log: in kind a routine restart produced
    /// `ehdb_feed_shard_lag{shard="0"} 2738`, and because each stale record costs a
    /// full control-plane round-trip to learn it is already claimed, the replay
    /// drained at ~1 record/s with fresh commands queued behind it.
    ///
    /// With no cursor stored yet, `fallback` decides: [`CursorFallback::Tail`]
    /// (the default) starts at the shard's current tip;
    /// [`CursorFallback::Beginning`] keeps the pre-fix full replay.
    ///
    /// A stored cursor is **clamped to the reopened log's tip**: an engine that
    /// recovered less than the cursor covers (its active part had not been sealed)
    /// would otherwise sit behind a cursor no future key can exceed, and deliver
    /// nothing ever again. [`started_from`](Self::started_from) reports what was
    /// actually used so the host can log a clamp.
    ///
    /// Persisting is the caller's to schedule — either
    /// [`spawn_cursor_persister`](Self::spawn_cursor_persister) or explicit
    /// [`persist_cursor`](Self::persist_cursor) calls.
    pub fn resume(
        writer: Arc<FeedWriter<D>>,
        shard: u32,
        ack_wait: Duration,
        subject_of: SubjectFn<D::Record>,
        store: CursorStore,
        fallback: CursorFallback,
    ) -> std::io::Result<Self> {
        // The shard's current tip: the writer assigns strictly increasing keys, so
        // the global sequence is a sound "after now" watermark for any shard (same
        // basis as `ChangeFeed::tail`).
        let tip = writer.engine().lock().unwrap().global_sequence();
        let stored = store.load()?;
        let (from_cursor, origin) = match stored {
            // Clamped to the tip on purpose. The engine resumes from its durable
            // manifest, so a log that lost an unsealed tail can reopen *behind* the
            // cursor we persisted — and since the writer then re-issues keys from
            // its recovered sequence, a cursor ahead of the tip would filter out
            // every new record and the bus would go permanently dark. Clamping
            // costs at most a small redelivery; not clamping is an outage.
            Some(cursor) => (cursor.min(tip), CursorOrigin::Persisted),
            None => match fallback {
                CursorFallback::Tail => (tip, CursorOrigin::FallbackTail),
                CursorFallback::Beginning => (0, CursorOrigin::FallbackBeginning),
            },
        };
        let report = ResumeReport {
            shard,
            stored_cursor: stored,
            tip,
            from_cursor,
            origin,
        };
        Ok(Self::build(
            writer,
            shard,
            ack_wait,
            from_cursor,
            subject_of,
            Some(store),
            origin,
            Some(report),
        ))
    }

    // The private constructor both `new` and `resume` funnel through; the arity
    // is the union of their inputs, not an interface anyone calls.
    #[allow(clippy::too_many_arguments)]
    fn build(
        writer: Arc<FeedWriter<D>>,
        shard: u32,
        ack_wait: Duration,
        from_cursor: u64,
        subject_of: SubjectFn<D::Record>,
        cursor_store: Option<CursorStore>,
        origin: CursorOrigin,
        resume_report: Option<ResumeReport>,
    ) -> Self {
        let ack_wait_ticks = ack_wait.as_millis() as u64;
        let poll_interval =
            Duration::from_millis(DEFAULT_POLL_INTERVAL_MS.min(ack_wait_ticks.max(1)));
        Self {
            group: Mutex::new(SubjectConsumerGroup::new(
                shard,
                ack_wait_ticks,
                from_cursor,
                subject_of,
            )),
            writer,
            clock: Instant::now(),
            poll_interval,
            cursor_store,
            started_at: (from_cursor, origin),
            resume_report,
        }
    }

    /// The cursor this coordinator started from and how it was chosen — for the
    /// host's restart log line.
    pub fn started_from(&self) -> (u64, CursorOrigin) {
        self.started_at
    }

    /// The full resume story for the host's restart line and the resume gauges —
    /// what was stored, the reopened log's tip, what was actually used, whether
    /// the cursor was clamped, and whether anything replayed
    /// ([`ResumeReport`], noetl/ai-meta#208). `None` when this coordinator was
    /// built with [`new`](Self::new) rather than resumed, so a caller can never
    /// mistake "no durable progress configured" for "resumed at 0".
    pub fn resume_report(&self) -> Option<ResumeReport> {
        self.resume_report
    }

    fn now_ticks(&self) -> u64 {
        self.clock.elapsed().as_millis() as u64
    }

    /// Claim the next command **matching `filter`** for `member`, **blocking**
    /// until one is available (a fresh command or an `ack_wait`-expired
    /// redelivery). `filter` is the member's subscription (a
    /// [`SubjectFilter`](crate::subject::SubjectFilter) string, e.g.
    /// `commands.shared.>`); a command outside it is never assigned here — the
    /// isolation guarantee. Members sharing a filter compete exactly-once.
    pub async fn claim_next(
        &self,
        filter: &str,
        member: MemberId,
    ) -> crate::group::Delivery<D::Record> {
        let filter = crate::subject::SubjectFilter::parse(filter);
        let mut tip_rx = self.writer.tip_receiver();
        loop {
            let assigned = {
                // Async lock FIRST (may await), then the engine's sync lock — no
                // std guard is ever held across an await.
                let mut group = self.group.lock().await;
                let engine = self.writer.engine();
                let e = engine.lock().unwrap();
                group.poll_assign(&e, &filter, member, self.now_ticks())
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

    /// This shard's backlog split by routing subject — the **per-pool** KEDA lag
    /// value (see [`SubjectConsumerGroup::subject_lags`]). `lag()` is whole-shard
    /// and so mixes the pools that share the shard; a pool's ScaledObject wants
    /// only its own subject (noetl/ai-meta#194 T2).
    pub async fn subject_lags(&self) -> Vec<crate::scaler::SubjectLag> {
        let group = self.group.lock().await;
        let engine = self.writer.engine();
        let e = engine.lock().unwrap();
        group
            .subject_lags(&e)
            .unwrap_or_default()
            .into_iter()
            .map(|(subject, lag)| crate::scaler::SubjectLag { subject, lag })
            .collect()
    }

    /// Seed the reported subject label set from the shard's existing log, so a
    /// freshly-started writer publishes a `0` for every subject it has ever
    /// carried rather than an empty label set
    /// ([`SubjectConsumerGroup::seed_subjects`]). Best-effort: a read error
    /// leaves the set to fill in from live traffic.
    pub async fn seed_subjects(&self) {
        let mut group = self.group.lock().await;
        let engine = self.writer.engine();
        let e = engine.lock().unwrap();
        let _ = group.seed_subjects(&e);
    }

    /// The group's contiguous acked-through cursor — everything at or below it is
    /// acked and not in flight. The value a restart resumes from
    /// ([`resume`](Self::resume)) and the `committed` field of this shard's
    /// [`ShardLag`](crate::scaler::ShardLag).
    pub async fn committed_cursor(&self) -> u64 {
        self.group.lock().await.committed_cursor()
    }

    /// Persist the current committed cursor, if this coordinator was built with a
    /// [`CursorStore`]. Returns the cursor that is now durable (`None` when there
    /// is no store).
    ///
    /// Safe to call as often as wanted: the store is monotonic and skips a write
    /// when the cursor has not advanced, so an idle bus does no disk I/O.
    pub async fn persist_cursor(&self) -> std::io::Result<Option<u64>> {
        let Some(store) = self.cursor_store.as_ref() else {
            return Ok(None);
        };
        // Read the cursor under the group lock, write it outside: the store's
        // `fsync` must not stall claimers (the same discipline as the writer's
        // off-lock commit, noetl/ai-meta#205).
        let cursor = self.committed_cursor().await;
        store.store(cursor)?;
        Ok(Some(cursor))
    }

    /// Persist the committed cursor every `interval` in the background — the
    /// steady-state durable-progress ticker.
    ///
    /// The cursor may lag the truth by at most one interval, which is the safe
    /// direction: a restart then re-delivers a handful of already-acked commands
    /// and the control plane answers "already claimed" (the at-least-once shape the
    /// `ack_wait` path already has). A persist failure is not fatal — it is retried
    /// on the next tick, since falling back to an older cursor is always safe.
    pub fn spawn_cursor_persister(
        self: Arc<Self>,
        interval: Duration,
    ) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
            loop {
                ticker.tick().await;
                let _ = self.persist_cursor().await;
            }
        })
    }
}

/// A claim request on the wire.
#[derive(Debug, Clone, Serialize, Deserialize)]
enum ClaimReq {
    /// Block until a command **matching `filter`** is assigned to `member`.
    /// `filter` is the member's subscription (a `SubjectFilter` string, e.g.
    /// `commands.shared.>`); the coordinator only ever hands it a command whose
    /// subject matches (strict isolation).
    ///
    /// `heartbeat_ms` opts this connection into liveness heartbeats: while the
    /// claim is parked, the coordinator sends a heartbeat frame every
    /// `heartbeat_ms` so the client can tell an idle bus from a dead writer
    /// (noetl/ai-meta#208). Absent (a pre-#208 client) means no heartbeats, so the
    /// wire behaviour for old clients is byte-for-byte what it was.
    Next {
        member: MemberId,
        filter: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        heartbeat_ms: Option<u64>,
    },
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
///
/// Accepted sockets get keepalive ([`crate::configure_stream`]) so a member whose
/// pod vanished stops holding a connection, and a `Next` that asked for
/// heartbeats gets one every `heartbeat_ms` while it parks — the liveness half of
/// noetl/ai-meta#208.
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
        crate::configure_stream(&sock)?;
        let coordinator = Arc::clone(&coordinator);
        tokio::spawn(async move {
            // One heartbeat is sent up front on the first heartbeat-requesting
            // claim of a connection, so the client learns *immediately* that this
            // coordinator heartbeats and can arm its read deadline for the whole
            // connection — rather than only after its first claim happens to park
            // long enough. Once per connection, so the per-claim hot path
            // (noetl/ai-meta#205) pays nothing.
            let mut liveness_announced = false;
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
                    ClaimReq::Next {
                        member,
                        filter,
                        heartbeat_ms,
                    } => {
                        let claim = coordinator.claim_next(&filter, member);
                        let delivery = match heartbeat_ms.filter(|ms| *ms > 0) {
                            None => claim.await,
                            Some(ms) => {
                                if !liveness_announced {
                                    if write_frame(&mut sock, HEARTBEAT_FRAME).await.is_err() {
                                        return;
                                    }
                                    liveness_announced = true;
                                }
                                let beat = Duration::from_millis(ms);
                                // `&mut claim` inside the timeout: a heartbeat only
                                // *pauses* polling the claim, it never drops it, so
                                // no assignment can be lost to a heartbeat tick.
                                tokio::pin!(claim);
                                loop {
                                    match tokio::time::timeout(beat, &mut claim).await {
                                        Ok(delivery) => break delivery,
                                        Err(_) => {
                                            if write_frame(&mut sock, HEARTBEAT_FRAME)
                                                .await
                                                .is_err()
                                            {
                                                return;
                                            }
                                        }
                                    }
                                }
                            }
                        };
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
/// competing with the other replicas sharing its subject filter.
pub struct ClaimClient {
    sock: TcpStream,
    member: MemberId,
    filter: String,
    /// The heartbeat interval this client asked the coordinator for (`None` =
    /// opted out; the pre-#208 wire behaviour).
    heartbeat: Option<Duration>,
    /// How long a parked read may go quiet before the peer is declared dead.
    /// Cleared once a peer proves it does not heartbeat, so an older coordinator
    /// never triggers a redial loop on an idle bus.
    read_deadline: Option<Duration>,
    /// Has this connection ever seen a heartbeat? Only then is a missed one
    /// evidence of a dead peer.
    peer_heartbeats: bool,
}

impl ClaimClient {
    /// Connect to a claim server as `member` subscribing with `filter` (a
    /// `SubjectFilter` string, e.g. `commands.shared.>` for the shared pool on
    /// any shard, or `commands.system.shard.0` for the system pool on shard 0).
    ///
    /// `addr` accepts any [`ToSocketAddrs`] — including a `host:port`
    /// **DNS name** (`noetl-cmdbus-writer.noetl.svc.cluster.local:9101`),
    /// which `TcpStream::connect` resolves at connect time. This is the
    /// finding-#2 fix: a Kubernetes service name works directly, so no
    /// ClusterIP-only workaround and pod-IP changes are followed on reconnect.
    /// The socket carries keepalive and the claim asks for
    /// [`DEFAULT_HEARTBEAT`] liveness heartbeats, so a writer restart surfaces as
    /// a read error instead of an indefinite park (noetl/ai-meta#208).
    pub async fn connect<A: ToSocketAddrs>(
        addr: A,
        member: MemberId,
        filter: impl Into<String>,
    ) -> std::io::Result<Self> {
        Self::connect_with_heartbeat(addr, member, filter, Some(DEFAULT_HEARTBEAT)).await
    }

    /// [`connect`](Self::connect) with an explicit heartbeat interval — `None`
    /// opts out of heartbeats entirely (keepalive still applies), which is only
    /// wanted in tests that assert the pre-#208 wire shape.
    pub async fn connect_with_heartbeat<A: ToSocketAddrs>(
        addr: A,
        member: MemberId,
        filter: impl Into<String>,
        heartbeat: Option<Duration>,
    ) -> std::io::Result<Self> {
        let sock = TcpStream::connect(addr).await?;
        crate::configure_stream(&sock)?;
        Ok(Self {
            sock,
            member,
            filter: filter.into(),
            heartbeat,
            read_deadline: heartbeat.map(|hb| hb * HEARTBEAT_MISS_FACTOR),
            peer_heartbeats: false,
        })
    }

    /// Claim the next command (blocks until one matching this member's filter is
    /// assigned).
    ///
    /// Parking here is unbounded by design — the bus may legitimately be idle for
    /// hours. What is *not* unbounded is waiting on a **dead** coordinator: while
    /// parked this consumes the coordinator's heartbeat frames, and once the peer
    /// has proven it heartbeats, [`HEARTBEAT_MISS_FACTOR`] missed beats return an
    /// error so the caller redials (noetl/ai-meta#208 defect 1). A coordinator that
    /// never heartbeats (an older writer) disarms the deadline on the first miss
    /// and liveness falls back to TCP keepalive alone.
    pub async fn claim_next<R: DeserializeOwned>(&mut self) -> std::io::Result<Claimed<R>> {
        let req = serde_json::to_vec(&ClaimReq::Next {
            member: self.member,
            filter: self.filter.clone(),
            heartbeat_ms: self.heartbeat.map(|hb| hb.as_millis() as u64),
        })
        .map_err(io_err)?;
        write_frame(&mut self.sock, &req).await?;
        loop {
            let body = match self.read_deadline {
                None => read_frame(&mut self.sock).await?,
                Some(deadline) => {
                    match tokio::time::timeout(deadline, read_frame(&mut self.sock)).await {
                        Ok(body) => body?,
                        Err(_) if self.peer_heartbeats => {
                            return Err(io_err(format!(
                                "claim coordinator stopped heartbeating for {}ms",
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
            let resp: ClaimResp<R> = serde_json::from_slice(&body).map_err(io_err)?;
            return Ok(Claimed {
                sort_key: resp.sort_key,
                redelivered: resp.redelivered,
                record: resp.record,
            });
        }
    }

    /// Has the coordinator on this connection proven it sends heartbeats? Used by
    /// tests (and useful in diagnostics) to distinguish keepalive-only liveness
    /// from heartbeat-backed liveness.
    pub fn peer_heartbeats(&self) -> bool {
        self.peer_heartbeats
    }

    /// Ack a claimed command by its sort key.
    pub async fn ack(&mut self, sort_key: u64) -> std::io::Result<()> {
        let req = serde_json::to_vec(&ClaimReq::Ack { sort_key }).map_err(io_err)?;
        write_frame(&mut self.sock, &req).await?;
        self.read_reply().await
    }

    /// Nack a claimed command (redeliver after ack_wait).
    pub async fn nack(&mut self, sort_key: u64) -> std::io::Result<()> {
        let req = serde_json::to_vec(&ClaimReq::Nack { sort_key }).map_err(io_err)?;
        write_frame(&mut self.sock, &req).await?;
        self.read_reply().await
    }

    /// Read an ack/nack reply, skipping a heartbeat frame if one is still in
    /// flight from a claim this caller abandoned (the coordinator only heartbeats
    /// inside a parked claim, so at most a few can be queued).
    async fn read_reply(&mut self) -> std::io::Result<()> {
        loop {
            let body = read_frame(&mut self.sock).await?;
            if !is_heartbeat(&body) {
                return Ok(());
            }
        }
    }
}
