//! **L1 T3 — named durable groups (N independently-cursored consumer groups
//! over one shard log).**
//!
//! [`ClaimCoordinator`](crate::claim::ClaimCoordinator) holds **one**
//! [`SubjectConsumerGroup`] per shard: every member competes in the same group,
//! against one feed cursor and one committed cursor. That is exactly right for
//! the command bus, where a command must go to exactly one worker in the whole
//! fleet.
//!
//! The **events** path needs the other JetStream shape. On `noetl_events` three
//! durable consumers — `noetl_materializer`, `noetl_result_materializer`,
//! `noetl_state_materializer` — each drain the *same* stream on their *own* ack
//! cursor, and within each consumer the system-pool replicas compete. So the two
//! relationships are stacked:
//!
//! - **between** groups: fan-out — every record is delivered to every group,
//!   each at its own pace;
//! - **within** a group: queue-group — each record goes to exactly one member,
//!   with ack / `ack_wait` redelivery.
//!
//! A [`GroupCoordinator`] is that: a lazily-populated map of group name →
//! [`SubjectConsumerGroup`], each with its own [`ChangeFeed`](ehdb_l0::ChangeFeed)
//! cursor over the shared writer's engine. One log read per group, no
//! cross-group coupling — a stalled group can never pin another group's cursor,
//! which is the failure mode that makes a single shared cursor wrong here.
//!
//! **Durability.** Each group's committed cursor persists through its own
//! [`CursorStore`] (`CursorStore::open_named`), so a writer restart resumes each
//! group where it was rather than replaying the whole retained log into every
//! consumer. This reuses the [#208](https://github.com/noetl/ai-meta/issues/208)
//! machinery wholesale — the crash-safe temp+fsync+rename+dir-fsync write, the
//! monotonic guard, the clamp-to-reopened-tip, and the [`ResumeReport`] that
//! makes a restart line self-evident — rather than growing a second, weaker
//! store beside it.
//!
//! The command bus is untouched by this module: `ClaimCoordinator`,
//! `serve_claims`, and their wire frames are byte-identical. The events feed
//! runs its own coordinator on its own port.

use std::collections::BTreeMap;
use std::io;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use ehdb_l0::{Dataset, EventRecord};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use tokio::net::{TcpListener, TcpStream, ToSocketAddrs};
use tokio::sync::Mutex;

use crate::cursor::{CursorFallback, CursorOrigin, CursorStore, ResumeReport};
use crate::group::{Delivery, MemberId, SubjectConsumerGroup};
use crate::subject::{Subject, SubjectFn};
use crate::{io_err, read_frame, write_frame, FeedWriter};

/// Default cap on how long `claim_next` parks before re-polling, so an
/// `ack_wait` redelivery surfaces even with no new appends. Mirrors the command
/// bus's interval.
const DEFAULT_POLL_INTERVAL_MS: u64 = 250;

/// The subject-token root for the events feed — the honest analog of the NATS
/// subject prefix `noetl.events`.
pub const EVENT_SUBJECT_ROOT: &str = "events";

/// Fallback event-type token for a record whose payload carries no
/// `event_type`. Never a wildcard, so a filter can always name it explicitly.
pub const UNKNOWN_EVENT_TYPE: &str = "unknown";

/// The events [`SubjectFn`]: derive a record's routing [`Subject`] —
/// `events.<event_type>` — from the published event payload.
///
/// This mirrors the server's NATS subject `noetl.events.<event_type>`
/// (`event_publisher.rs:177`). Identity (`execution_id`) deliberately stays in
/// the payload and **not** in the subject: the live Rust publisher never put it
/// there, and the gateway already reads it from the payload with the
/// subject-derived value as a soft fallback. Encoding it here would invent a
/// contract nothing upstream honours.
pub fn event_feed_subject() -> SubjectFn<EventRecord> {
    Arc::new(|rec: &EventRecord| -> Subject {
        let event_type = serde_json::from_str::<serde_json::Value>(&rec.payload)
            .ok()
            .and_then(|v| {
                v.get("event_type")
                    .and_then(|t| t.as_str())
                    .map(str::to_string)
            })
            .filter(|s| !s.is_empty())
            .unwrap_or_else(|| UNKNOWN_EVENT_TYPE.to_string());
        // A subject token is dot-delimited by construction; an event type that
        // contained a dot would silently widen the caller's filter, so flatten
        // it to a single token.
        Subject::parse(&format!(
            "{EVENT_SUBJECT_ROOT}.{}",
            event_type.replace('.', "_")
        ))
    })
}

/// One open group: its consumer group, the durable cursor store backing it, and
/// the report describing where it resumed.
struct OpenGroup<D: Dataset> {
    group: Arc<Mutex<SubjectConsumerGroup<D>>>,
    cursor: Option<Arc<CursorStore>>,
    resume: ResumeReport,
}

impl<D: Dataset> Clone for OpenGroup<D> {
    fn clone(&self) -> Self {
        Self {
            group: Arc::clone(&self.group),
            cursor: self.cursor.clone(),
            resume: self.resume,
        }
    }
}

/// A coordinator over **named** consumer groups sharing one shard's log.
///
/// Groups are created lazily on first claim, resuming from their own
/// [`CursorStore`] when a `cursor_dir` is configured. Each group holds its own
/// feed cursor, so groups never block one another.
pub struct GroupCoordinator<D: Dataset> {
    writer: Arc<FeedWriter<D>>,
    shard: u32,
    ack_wait_ticks: u64,
    subject_of: SubjectFn<D::Record>,
    poll_interval: Duration,
    clock: Instant,
    groups: Mutex<BTreeMap<String, OpenGroup<D>>>,
    /// Directory the per-group cursor files live in (the writer's volume).
    /// `None` disables persistence — every group then starts at `fallback`.
    cursor_dir: Option<PathBuf>,
    fallback: CursorFallback,
    /// Count of failed cursor persists. Surfaced to the host rather than logged
    /// here: this crate carries no logging dependency, and a persist failure is
    /// a metric an operator should see, not a line that scrolls past.
    cursor_errors: AtomicU64,
    /// The most recent cursor-persist failure, as `(group, message)`.
    ///
    /// A bare counter was not diagnosable: it said 26 persists failed and gave
    /// no way to learn why, which is how noetl/ai-meta#216 sat unexplained. The
    /// crate still cannot log, so it keeps the last error for the host to read
    /// and log — one string, overwritten, no allocation on the success path.
    last_cursor_error: std::sync::Mutex<Option<(String, String)>>,
}

impl<D> GroupCoordinator<D>
where
    D: Dataset,
    D::Record: Serialize + DeserializeOwned + Clone + Send + 'static,
{
    /// A coordinator over `writer`'s shard. `subject_of` maps each record to its
    /// routing subject (use [`event_feed_subject`] for the events feed).
    ///
    /// `cursor_dir` is the writer's data directory; each group persists its
    /// committed cursor to `claim-cursor.<group>.shard-<n>.json` there. `None`
    /// disables persistence, so every group starts at `fallback` on each open —
    /// test-only posture.
    ///
    /// `fallback` decides where a group with no stored cursor begins. For the
    /// events feed the meaningful choice is
    /// [`CursorFallback::Tail`](crate::cursor::CursorFallback::Tail): a wiped
    /// volume has no log to replay, and replaying a retained log into the
    /// materializers re-projects events that are already durable.
    pub fn new(
        writer: Arc<FeedWriter<D>>,
        shard: u32,
        ack_wait: Duration,
        subject_of: SubjectFn<D::Record>,
        cursor_dir: Option<PathBuf>,
        fallback: CursorFallback,
    ) -> Self {
        let ack_wait_ticks = ack_wait.as_millis() as u64;
        let poll_interval =
            Duration::from_millis(DEFAULT_POLL_INTERVAL_MS.min(ack_wait_ticks.max(1)));
        Self {
            writer,
            shard,
            ack_wait_ticks,
            subject_of,
            poll_interval,
            clock: Instant::now(),
            groups: Mutex::new(BTreeMap::new()),
            cursor_dir,
            fallback,
            cursor_errors: AtomicU64::new(0),
            last_cursor_error: std::sync::Mutex::new(None),
        }
    }

    /// Force `group` open and return the [`ResumeReport`] describing where it
    /// started. The host calls this at startup for each group it intends to
    /// serve so the restart line is logged once, with the numbers that make a
    /// clamp or a replay self-evident (noetl/ai-meta#208).
    pub async fn open_group(&self, group: &str) -> ResumeReport {
        self.handle(group).await.resume
    }

    /// How many cursor persists have failed. Non-zero means group progress is
    /// not durable — a restart will replay from an older cursor. Records are
    /// never lost by this, only re-delivered.
    pub fn cursor_errors(&self) -> u64 {
        self.cursor_errors.load(Ordering::Relaxed)
    }

    /// The most recent cursor-persist failure as `(group, message)`, if any.
    /// The host logs this; the crate cannot (no logging dependency).
    pub fn last_cursor_error(&self) -> Option<(String, String)> {
        self.last_cursor_error
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }

    fn record_cursor_error(&self, group: &str, err: &std::io::Error) {
        self.cursor_errors.fetch_add(1, Ordering::Relaxed);
        if let Ok(mut slot) = self.last_cursor_error.lock() {
            *slot = Some((group.to_string(), err.to_string()));
        }
    }

    fn now_ticks(&self) -> u64 {
        self.clock.elapsed().as_millis() as u64
    }

    /// The shard's current tip, used to clamp a resumed cursor.
    fn tip(&self) -> u64 {
        let engine = self.writer.engine();
        let e = engine.lock().unwrap();
        e.global_sequence()
    }

    /// Get (or lazily create) the handle for `group`.
    async fn handle(&self, group: &str) -> OpenGroup<D> {
        let mut groups = self.groups.lock().await;
        if let Some(existing) = groups.get(group) {
            return existing.clone();
        }

        let cursor = self
            .cursor_dir
            .as_ref()
            .and_then(|dir| CursorStore::open_named(dir, group, self.shard).ok())
            .map(Arc::new);
        let stored_cursor = cursor.as_ref().and_then(|c| c.load().ok()).flatten();

        // Clamp a stored cursor to the reopened tip: one above the tip (log
        // recreated / volume replaced) would silently skip every record below
        // it (noetl/ai-meta#208).
        let tip = self.tip();
        let (from_cursor, origin) = match stored_cursor {
            Some(c) => (c.min(tip), CursorOrigin::Persisted),
            None => match self.fallback {
                CursorFallback::Tail => (tip, CursorOrigin::FallbackTail),
                CursorFallback::Beginning => (0, CursorOrigin::FallbackBeginning),
            },
        };
        let report = ResumeReport {
            shard: self.shard,
            stored_cursor,
            tip,
            from_cursor,
            origin,
        };

        let mut g = SubjectConsumerGroup::new(
            self.shard,
            self.ack_wait_ticks,
            from_cursor,
            Arc::clone(&self.subject_of),
        );
        {
            // Seed the reported subject set so per-subject lag has its full label
            // set from the first scrape rather than only after traffic arrives.
            let engine = self.writer.engine();
            let e = engine.lock().unwrap();
            let _ = g.seed_subjects(&e);
        }
        let handle = OpenGroup {
            group: Arc::new(Mutex::new(g)),
            cursor,
            resume: report,
        };
        groups.insert(group.to_string(), handle.clone());
        handle
    }

    /// Claim the next record for `member` in `group`, **blocking** until one
    /// matching `filter` is available (a fresh record or an `ack_wait`-expired
    /// redelivery). Members sharing a group compete exactly-once; members in
    /// different groups each see every record.
    pub async fn claim_next(
        &self,
        group: &str,
        filter: &str,
        member: MemberId,
    ) -> Delivery<D::Record> {
        let filter = crate::subject::SubjectFilter::parse(filter);
        let handle = self.handle(group).await;
        let mut tip_rx = self.writer.tip_receiver();
        loop {
            let assigned = {
                // Async lock FIRST (may await), then the engine's sync lock — no
                // std guard is ever held across an await.
                let mut g = handle.group.lock().await;
                let engine = self.writer.engine();
                let e = engine.lock().unwrap();
                g.poll_assign(&e, &filter, member, self.now_ticks())
            };
            match assigned {
                Ok(Some(delivery)) => return delivery,
                Ok(None) => {
                    let _ = tokio::time::timeout(self.poll_interval, tip_rx.changed()).await;
                }
                Err(_) => {
                    tokio::time::sleep(self.poll_interval).await;
                }
            }
        }
    }

    /// Ack a claimed record in `group`. Returns `true` if it was in flight.
    ///
    /// The group's committed cursor is persisted here, on the ack path, rather
    /// than only by [`checkpoint`](Self::checkpoint): an ack is exactly the
    /// moment progress becomes durable-worthy, and `CursorStore::store` is a
    /// no-op when the cursor has not advanced, so a busy group does not pay a
    /// write per ack. A persist failure is logged, not propagated — losing a
    /// cursor write costs replay, never records.
    pub async fn ack(&self, group: &str, sort_key: u64) -> bool {
        let handle = self.handle(group).await;
        let (acked, committed) = {
            let mut g = handle.group.lock().await;
            let acked = g.ack(sort_key);
            (acked, g.committed_cursor())
        };
        if let Some(cursor) = &handle.cursor {
            if let Err(e) = cursor.store(committed) {
                self.record_cursor_error(group, &e);
            }
        }
        acked
    }

    /// Nack a claimed record — decline the ack so the group's `ack_wait` timer
    /// redelivers it (at-least-once).
    pub async fn nack(&self, _group: &str, _sort_key: u64) {}

    /// `group`'s backlog past its committed cursor (undelivered + in-flight).
    pub async fn lag(&self, group: &str) -> u64 {
        let handle = self.handle(group).await;
        let g = handle.group.lock().await;
        let engine = self.writer.engine();
        let e = engine.lock().unwrap();
        g.lag(&e).unwrap_or(0)
    }

    /// `group`'s contiguous acked-through cursor.
    pub async fn committed_cursor(&self, group: &str) -> u64 {
        let handle = self.handle(group).await;
        let g = handle.group.lock().await;
        g.committed_cursor()
    }

    /// `(group, committed_cursor, lag)` for every **open** group — the metrics
    /// view. Does not create groups, so scraping never conjures a consumer that
    /// nothing subscribed to.
    pub async fn group_lags(&self) -> Vec<(String, u64, u64)> {
        let groups = {
            let g = self.groups.lock().await;
            g.iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect::<Vec<_>>()
        };
        let mut out = Vec::with_capacity(groups.len());
        for (name, handle) in groups {
            let g = handle.group.lock().await;
            let engine = self.writer.engine();
            let e = engine.lock().unwrap();
            out.push((name, g.committed_cursor(), g.lag(&e).unwrap_or(0)));
        }
        out
    }

    /// Persist every open group's committed cursor. Belt-and-braces beside the
    /// ack path: a group that goes idle mid-batch has already had its cursor
    /// stored on its last ack, so this mainly bounds the loss window for a group
    /// whose final ack raced a shutdown.
    /// The writer this coordinator serves from.
    ///
    /// The events host hands back only the coordinator, so without this its
    /// caller cannot reach the log to append to it or to seal it — which makes
    /// the two-host shutdown path untestable from outside the crate, and that
    /// path is exactly where noetl/ai-meta#226 lost records.
    pub fn writer(&self) -> Arc<FeedWriter<D>> {
        Arc::clone(&self.writer)
    }

    pub async fn checkpoint(&self) -> io::Result<()> {
        let groups = {
            let g = self.groups.lock().await;
            g.iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect::<Vec<_>>()
        };
        for (_name, handle) in groups {
            let Some(cursor) = &handle.cursor else {
                continue;
            };
            let committed = {
                let g = handle.group.lock().await;
                g.committed_cursor()
            };
            if let Err(e) = cursor.store(committed) {
                self.record_cursor_error(&_name, &e);
            }
        }
        Ok(())
    }
}

/// A named-group claim request on the wire.
///
/// Deliberately a **separate** frame type from
/// [`ClaimReq`](crate::claim) rather than an added field on it: the command bus
/// is live in production on that protocol, and the events feed runs on its own
/// port with its own coordinator. Sharing the enum would put the events cutover
/// in the command bus's blast radius for no benefit.
#[derive(Debug, Clone, Serialize, Deserialize)]
enum GroupClaimReq {
    /// Block until a record matching `filter` is assigned to `member` in `group`.
    Next {
        group: String,
        member: MemberId,
        filter: String,
        /// `heartbeat_ms` opts this connection into liveness heartbeats: while
        /// the claim is parked, the coordinator sends a heartbeat frame every
        /// `heartbeat_ms` so the client can tell an idle feed from a dead writer
        /// (noetl/ai-meta#225 — the events-face twin of #208's command face).
        ///
        /// `#[serde(default)]` so a pre-#225 client's frame still decodes; it
        /// means no heartbeats, so the wire shape for that client is unchanged.
        #[serde(default)]
        heartbeat_ms: Option<u64>,
    },
    /// Ack a claimed record in `group`.
    Ack { group: String, sort_key: u64 },
    /// Nack a claimed record in `group` (redeliver after ack_wait).
    Nack { group: String, sort_key: u64 },
}

/// A claimed record on the wire.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct GroupClaimResp<R> {
    sort_key: u64,
    redelivered: bool,
    record: R,
}

/// Accept named-group claim connections on `listener`, served from the shared
/// `coordinator`. Runs until the listener errors; spawn it as a task.
///
/// Accepted sockets get keepalive ([`crate::configure_stream`]) so a consumer
/// whose pod vanished stops holding a connection, and a `Next` that asked for
/// heartbeats gets one every `heartbeat_ms` while it parks — the liveness half
/// of noetl/ai-meta#225.
///
/// Before that this face set `TCP_NODELAY` and nothing else, which is precisely
/// how the events consumers wedged in prod: a writer restart left the client's
/// socket half-open (ESTABLISHED client-side, unknown to the replacement
/// writer), the parked `claim_next` read neither yielded nor errored, and every
/// redial path downstream was unreachable because there was no `Err` to trigger
/// it. `noetl.event` — the sole durable event log — took no writes for 3h24m
/// with every health signal green.
pub async fn serve_group_claims<D>(
    listener: TcpListener,
    coordinator: Arc<GroupCoordinator<D>>,
) -> io::Result<()>
where
    D: Dataset,
    D::Record: Serialize + DeserializeOwned + Clone + Send + 'static,
{
    loop {
        // noetl/ehdb#311 — per-connection failures must not drop the listener.
        // This face already spawns its handshake, so it could not be killed the
        // way the WAL fan-out face could; but an accept error and the socket
        // setup below both still `?`d out of the loop, and a peer that vanishes
        // between SYN and accept is a routine, per-connection event.
        let (mut sock, _peer) = match listener.accept().await {
            Ok(v) => v,
            Err(e) => {
                tracing::warn!(error = %e, "groups: accept failed; face stays up");
                continue;
            }
        };
        if let Err(e) = crate::configure_stream(&sock) {
            tracing::warn!(error = %e, "groups: rejecting connection (socket setup)");
            continue;
        }
        let coordinator = Arc::clone(&coordinator);
        tokio::spawn(async move {
            // One heartbeat up front on the first heartbeat-requesting claim of a
            // connection, so the client learns immediately that this coordinator
            // heartbeats and can arm its read deadline for the whole connection
            // — rather than only after its first claim happens to park long
            // enough. Once per connection, so the per-claim path pays nothing.
            let mut liveness_announced = false;
            loop {
                let body = match read_frame(&mut sock).await {
                    Ok(b) => b,
                    Err(_) => return,
                };
                let req: GroupClaimReq = match serde_json::from_slice(&body) {
                    Ok(r) => r,
                    Err(_) => return,
                };
                match req {
                    GroupClaimReq::Next {
                        group,
                        member,
                        filter,
                        heartbeat_ms,
                    } => {
                        let claim = coordinator.claim_next(&group, &filter, member);
                        let delivery = match heartbeat_ms.filter(|ms| *ms > 0) {
                            None => claim.await,
                            Some(ms) => {
                                if !liveness_announced {
                                    if write_frame(&mut sock, crate::HEARTBEAT_FRAME)
                                        .await
                                        .is_err()
                                    {
                                        return;
                                    }
                                    liveness_announced = true;
                                }
                                let beat = Duration::from_millis(ms);
                                // `&mut claim` inside the timeout: a heartbeat
                                // only *pauses* polling the claim, it never
                                // drops it, so no assignment can be lost to a
                                // heartbeat tick.
                                tokio::pin!(claim);
                                loop {
                                    match tokio::time::timeout(beat, &mut claim).await {
                                        Ok(delivery) => break delivery,
                                        Err(_) => {
                                            if write_frame(&mut sock, crate::HEARTBEAT_FRAME)
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
                        let resp = GroupClaimResp {
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
                    GroupClaimReq::Ack { group, sort_key } => {
                        coordinator.ack(&group, sort_key).await;
                        if write_frame(&mut sock, b"1").await.is_err() {
                            return;
                        }
                    }
                    GroupClaimReq::Nack { group, sort_key } => {
                        coordinator.nack(&group, sort_key).await;
                        if write_frame(&mut sock, b"1").await.is_err() {
                            return;
                        }
                    }
                }
            }
        });
    }
}

/// One claimed record delivered to a [`GroupClaimClient`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GroupClaimed<R> {
    pub sort_key: u64,
    pub redelivered: bool,
    pub record: R,
}

/// A consumer's connection to a named group on the events feed. One member
/// competing with the other replicas that share its group **and** filter.
pub struct GroupClaimClient {
    sock: TcpStream,
    group: String,
    member: MemberId,
    filter: String,
    /// The heartbeat interval this client asked the coordinator for (`None` =
    /// opted out; the pre-#225 wire behaviour).
    heartbeat: Option<Duration>,
    /// How long a parked read may go quiet before the peer is declared dead.
    /// Cleared once a peer proves it does not heartbeat, so an older coordinator
    /// never triggers a redial loop on a genuinely idle feed.
    read_deadline: Option<Duration>,
    /// Has this connection ever seen a heartbeat? Only then is a missed one
    /// evidence of a dead peer.
    peer_heartbeats: bool,
    /// Has a deadline miss already demoted this connection to the hard ceiling?
    /// A second miss at the ceiling is treated as a dead peer (noetl/ai-meta#297).
    deadline_demoted: bool,
}

impl GroupClaimClient {
    /// Connect as `member` of `group`, subscribing with `filter` (a
    /// `SubjectFilter` string, e.g. `events.>` for every event type).
    ///
    /// `addr` accepts a `host:port` DNS name, resolved at connect time, so a
    /// Kubernetes service name works directly and pod-IP changes are followed on
    /// reconnect.
    /// The socket carries keepalive and the claim asks for
    /// [`DEFAULT_HEARTBEAT`](crate::DEFAULT_HEARTBEAT) liveness heartbeats, so a
    /// writer restart surfaces as a read error instead of an indefinite park
    /// (noetl/ai-meta#225).
    pub async fn connect<A: ToSocketAddrs>(
        addr: A,
        group: impl Into<String>,
        member: MemberId,
        filter: impl Into<String>,
    ) -> io::Result<Self> {
        Self::connect_with_heartbeat(addr, group, member, filter, Some(crate::DEFAULT_HEARTBEAT))
            .await
    }

    /// [`connect`](Self::connect) with an explicit heartbeat interval — `None`
    /// opts out of heartbeats entirely (keepalive still applies), which is only
    /// wanted in tests that assert the pre-#225 wire shape.
    pub async fn connect_with_heartbeat<A: ToSocketAddrs>(
        addr: A,
        group: impl Into<String>,
        member: MemberId,
        filter: impl Into<String>,
        heartbeat: Option<Duration>,
    ) -> io::Result<Self> {
        let sock = TcpStream::connect(addr).await?;
        crate::configure_stream(&sock)?;
        Ok(Self {
            sock,
            group: group.into(),
            member,
            filter: filter.into(),
            heartbeat,
            read_deadline: heartbeat.map(|hb| hb * crate::HEARTBEAT_MISS_FACTOR),
            peer_heartbeats: false,
            deadline_demoted: false,
        })
    }

    /// The group this client drains.
    pub fn group(&self) -> &str {
        &self.group
    }

    /// Claim the next record (blocks until one matching the filter is assigned).
    ///
    /// Parking here is unbounded by design — the events feed may legitimately be
    /// idle for hours. What is *not* unbounded is waiting on a **dead**
    /// coordinator: while parked this consumes the coordinator's heartbeat
    /// frames, and once the peer has proven it heartbeats,
    /// [`HEARTBEAT_MISS_FACTOR`](crate::HEARTBEAT_MISS_FACTOR) missed beats
    /// return an error so the caller redials (noetl/ai-meta#225). A coordinator
    /// that never heartbeats (a pre-#225 writer) disarms the deadline on the
    /// first miss and liveness falls back to TCP keepalive alone.
    pub async fn claim_next<R: DeserializeOwned>(&mut self) -> io::Result<GroupClaimed<R>> {
        let req = serde_json::to_vec(&GroupClaimReq::Next {
            group: self.group.clone(),
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
                                "events-feed group coordinator stopped heartbeating for {}ms",
                                deadline.as_millis()
                            )));
                        }
                        Err(_) => {
                            // Never heartbeated. Do NOT disarm — that is an
                            // unbounded park, and keepalive cannot see a peer
                            // that is alive but stuck (noetl/ai-meta#297).
                            // Demote to the hard ceiling once; a miss at the
                            // ceiling means dead, so error and let the caller
                            // redial.
                            match crate::hard_read_ceiling() {
                                Some(ceiling) if !self.deadline_demoted => {
                                    self.deadline_demoted = true;
                                    self.read_deadline = Some(ceiling);
                                    continue;
                                }
                                Some(ceiling) => {
                                    return Err(io_err(format!(
                                        "group coordinator silent for {}ms with no heartbeat; \
                                         treating as dead (noetl/ai-meta#297)",
                                        ceiling.as_millis()
                                    )));
                                }
                                None => {
                                    // Explicitly disabled by the operator.
                                    self.read_deadline = None;
                                    continue;
                                }
                            }
                        }
                    }
                }
            };
            if crate::is_heartbeat(&body) {
                self.peer_heartbeats = true;
                continue;
            }
            let resp: GroupClaimResp<R> = serde_json::from_slice(&body).map_err(io_err)?;
            return Ok(GroupClaimed {
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

    /// Ack a claimed record by its sort key.
    pub async fn ack(&mut self, sort_key: u64) -> io::Result<()> {
        let req = serde_json::to_vec(&GroupClaimReq::Ack {
            group: self.group.clone(),
            sort_key,
        })
        .map_err(io_err)?;
        write_frame(&mut self.sock, &req).await?;
        let _ = read_frame(&mut self.sock).await?;
        Ok(())
    }

    /// Nack a claimed record (redeliver after ack_wait).
    pub async fn nack(&mut self, sort_key: u64) -> io::Result<()> {
        let req = serde_json::to_vec(&GroupClaimReq::Nack {
            group: self.group.clone(),
            sort_key,
        })
        .map_err(io_err)?;
        write_frame(&mut self.sock, &req).await?;
        let _ = read_frame(&mut self.sock).await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn event_subject_is_derived_from_payload_event_type() {
        let f = event_feed_subject();
        let rec = EventRecord::new(1, "exec-1", "tx-1", r#"{"event_type":"action_started"}"#);
        assert_eq!(f(&rec).as_str(), "events.action_started");
    }

    #[test]
    fn event_subject_falls_back_when_type_missing_or_blank() {
        let f = event_feed_subject();
        for payload in [r#"{}"#, r#"{"event_type":""}"#, "not json"] {
            let rec = EventRecord::new(1, "exec-1", "tx-1", payload);
            assert_eq!(
                f(&rec).as_str(),
                "events.unknown",
                "payload {payload:?} should fall back"
            );
        }
    }

    #[test]
    fn dotted_event_type_stays_one_token() {
        // A dot in the type would otherwise widen a caller's filter by adding a
        // subject level.
        let f = event_feed_subject();
        let rec = EventRecord::new(1, "e", "t", r#"{"event_type":"a.b"}"#);
        let subject = f(&rec);
        assert_eq!(subject.tokens().len(), 2);
        assert_eq!(subject.as_str(), "events.a_b");
    }

    /// Each named group gets its **own** cursor file, so one group's progress
    /// can never be read as another's — the whole point of named groups.
    #[test]
    fn named_cursor_stores_are_per_group_and_do_not_collide() {
        let dir = std::env::temp_dir().join(format!("ehdb-named-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);

        let a = CursorStore::open_named(&dir, "noetl_materializer", 0).unwrap();
        let b = CursorStore::open_named(&dir, "noetl_state_materializer", 0).unwrap();
        assert_ne!(a.path(), b.path());

        a.store(100).unwrap();
        b.store(7).unwrap();

        assert_eq!(
            CursorStore::open_named(&dir, "noetl_materializer", 0)
                .unwrap()
                .load()
                .unwrap(),
            Some(100)
        );
        assert_eq!(
            CursorStore::open_named(&dir, "noetl_state_materializer", 0)
                .unwrap()
                .load()
                .unwrap(),
            Some(7)
        );

        std::fs::remove_dir_all(&dir).ok();
    }

    /// A named store must not collide with the command bus's un-named shard
    /// file — the events feed and the command bus can share a volume.
    #[test]
    fn named_store_does_not_collide_with_the_unnamed_shard_store() {
        let dir = std::env::temp_dir().join(format!("ehdb-named-vs-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let unnamed = CursorStore::open(&dir, 0).unwrap();
        let named = CursorStore::open_named(&dir, "g", 0).unwrap();
        assert_ne!(unnamed.path(), named.path());
        std::fs::remove_dir_all(&dir).ok();
    }

    /// A group name with path separators must not escape the cursor directory.
    #[test]
    fn group_names_are_sanitised_into_the_cursor_dir() {
        let dir = std::env::temp_dir().join(format!("ehdb-named-esc-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let store = CursorStore::open_named(&dir, "../../etc/passwd", 0).unwrap();
        assert_eq!(store.path().parent().unwrap(), dir.as_path());
        assert!(!store.path().to_string_lossy().contains(".."));
        std::fs::remove_dir_all(&dir).ok();
    }
}
