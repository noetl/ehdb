//! **L1 T2 — the KEDA autoscaler lag signal (shadow).**
//!
//! Exposes each shard consumer group's **lag** (backlog past the committed
//! cursor — see [`ShardConsumerGroup::lag`](crate::ShardConsumerGroup::lag)) as a
//! Prometheus gauge on a scrapeable `/metrics` endpoint. KEDA's prometheus scaler
//! reads that gauge and scales the worker pool on backlog — so scaling has a real
//! signal **before** any command-bus cutover (the hard-ordering rule: T2 ready
//! before T4).
//!
//! **T2 posture:** shadow — the gauge is published and can be scraped/compared,
//! but nothing scales the live (NATS-authoritative) bus off it yet. The KEDA
//! `ScaledObject` that consumes this gauge is an ops-repo manifest; this crate
//! owns the *signal*.
//!
//! The exposition follows the Prometheus text format (v0.0.4): a `# HELP` / `#
//! TYPE gauge` header, one `ehdb_feed_shard_lag{shard="N"}` series per shard, and
//! an `ehdb_feed_total_lag` aggregate (a convenient single trigger for a
//! pool-wide `ScaledObject`).
//!
//! The **committed cursor** is exposed alongside it as
//! `ehdb_feed_shard_committed{shard="N"}` (noetl/ai-meta#208): lag alone cannot
//! distinguish "caught up" from "resumed at the wrong place", and the committed
//! cursor is exactly the value a restarted writer resumes from — so a restart
//! that starts replaying is visible as the cursor jumping backwards rather than
//! only as a lag spike.
//!
//! The committed cursor turned out not to be enough on its own: sort keys are
//! assigned from the engine's *recovered* sequence, so the cursor is relative to
//! what the manifest recovered and legitimately steps down across a restart. The
//! `ehdb_feed_shard_resume_*` family ([`render_resume`]) closes that: it states
//! what was stored, the reopened tip, what was used, and how many records the
//! restart actually replayed — so "the restart was invisible" is one equality
//! (`ehdb_feed_shard_resume_replay_records == 0`) rather than arithmetic across
//! scrapes.

use std::io;
use std::net::SocketAddr;
use std::sync::Arc;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

use crate::cursor::ResumeReport;

/// One shard consumer group's lag sample.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ShardLag {
    pub shard: u32,
    /// The group's committed-through cursor (acked prefix).
    pub committed: u64,
    /// Backlog: shard records past `committed` (undelivered + unacked).
    pub lag: u64,
}

/// One routing subject's backlog — the per-pool slice of a shard's lag.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubjectLag {
    /// The routing subject, e.g. `commands.shared.shard.0`. It already carries
    /// the shard, so this series needs no second label.
    pub subject: String,
    /// Backlog for this subject: records past the group's committed cursor whose
    /// subject matches (undelivered + unacked).
    pub lag: u64,
}

/// A full sample of the writer's lag surface: per-shard totals plus the
/// per-subject split.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct LagSnapshot {
    pub shards: Vec<ShardLag>,
    pub subjects: Vec<SubjectLag>,
}

impl LagSnapshot {
    /// A snapshot with no per-subject split — the pre-noetl/ai-meta#194 shape.
    pub fn shards_only(shards: Vec<ShardLag>) -> Self {
        Self {
            shards,
            subjects: Vec::new(),
        }
    }
}

const LAG_METRIC: &str = "ehdb_feed_shard_lag";
const TOTAL_METRIC: &str = "ehdb_feed_total_lag";
const COMMITTED_METRIC: &str = "ehdb_feed_shard_committed";
const SUBJECT_METRIC: &str = "ehdb_feed_subject_lag";

/// Render shard lags as Prometheus exposition text (v0.0.4).
pub fn render_prometheus(samples: &[ShardLag]) -> String {
    render_snapshot(&LagSnapshot::shards_only(samples.to_vec()))
}

/// Render a full lag snapshot (per-shard + per-subject) as Prometheus exposition
/// text (v0.0.4).
///
/// **The per-subject series is a scaler contract.** KEDA's `metrics-api` scaler
/// in `format: prometheus` matches `valueLocation` as a *prefix of the whole
/// `name{labels}` token* and takes the first matching line — it has no label
/// selector. So the rendered line must be byte-stable:
/// `ehdb_feed_subject_lag{subject="commands.shared.shard.0"} 12`, exactly one
/// label, subjects in sorted order. Changing the label set or its spacing breaks
/// every ScaledObject pointing at it (noetl/ai-meta#194).
pub fn render_snapshot(snapshot: &LagSnapshot) -> String {
    let samples = &snapshot.shards;
    let mut out = String::new();
    out.push_str(&format!(
        "# HELP {LAG_METRIC} Consumer-group backlog (undelivered + unacked records) per shard.\n"
    ));
    out.push_str(&format!("# TYPE {LAG_METRIC} gauge\n"));
    // Deterministic order for stable scrapes.
    let mut ordered = samples.to_vec();
    ordered.sort_by_key(|s| s.shard);
    for s in &ordered {
        out.push_str(&format!(
            "{LAG_METRIC}{{shard=\"{}\"}} {}\n",
            s.shard, s.lag
        ));
    }
    out.push_str(&format!(
        "# HELP {COMMITTED_METRIC} Consumer-group committed cursor (acked prefix) per shard — the sort key a restarted writer resumes from.\n"
    ));
    out.push_str(&format!("# TYPE {COMMITTED_METRIC} gauge\n"));
    for s in &ordered {
        out.push_str(&format!(
            "{COMMITTED_METRIC}{{shard=\"{}\"}} {}\n",
            s.shard, s.committed
        ));
    }
    let total: u64 = ordered.iter().map(|s| s.lag).sum();
    out.push_str(&format!(
        "# HELP {TOTAL_METRIC} Total consumer-group backlog across all shards.\n"
    ));
    out.push_str(&format!("# TYPE {TOTAL_METRIC} gauge\n"));
    out.push_str(&format!("{TOTAL_METRIC} {total}\n"));

    // Per-subject split — the per-pool trigger value. Always emitted (even with
    // no subjects known yet) so the family's HELP/TYPE headers are stable.
    out.push_str(&format!(
        "# HELP {SUBJECT_METRIC} Consumer-group backlog per routing subject (commands.<pool>.shard.<n>) — the per-pool autoscaler trigger.\n"
    ));
    out.push_str(&format!("# TYPE {SUBJECT_METRIC} gauge\n"));
    let mut subjects = snapshot.subjects.clone();
    subjects.sort_by(|a, b| a.subject.cmp(&b.subject));
    for s in &subjects {
        out.push_str(&format!(
            "{SUBJECT_METRIC}{{subject=\"{}\"}} {}\n",
            s.subject, s.lag
        ));
    }
    out
}

/// Serve a Prometheus `/metrics` endpoint. On each connection, `provider` is
/// called to sample the current lags (so the scrape always reflects live state),
/// and the rendered exposition is returned with a `200`. Runs until the listener
/// errors; spawn it as a task.
///
/// Deliberately minimal HTTP/1.1: any request gets the metrics body (KEDA/
/// Prometheus scrape `GET /metrics`; a health probe `GET /` gets the same 200).
pub async fn serve_metrics<F>(listener: TcpListener, provider: F) -> io::Result<()>
where
    F: Fn() -> Vec<ShardLag> + Send + Sync + 'static,
{
    serve_snapshot_metrics(listener, move || LagSnapshot::shards_only(provider())).await
}

/// [`serve_metrics`] over a full [`LagSnapshot`] — per-shard totals plus the
/// per-subject split.
pub async fn serve_snapshot_metrics<F>(listener: TcpListener, provider: F) -> io::Result<()>
where
    F: Fn() -> LagSnapshot + Send + Sync + 'static,
{
    let provider = Arc::new(provider);
    loop {
        let (mut sock, _peer) = listener.accept().await?;
        let provider = Arc::clone(&provider);
        tokio::spawn(async move {
            // Drain the request head (we don't route on it); tolerate a short read.
            let mut scratch = [0u8; 1024];
            let _ = sock.read(&mut scratch).await;
            let body = render_snapshot(&provider());
            let resp = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: text/plain; version=0.0.4\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                body.len(),
                body
            );
            let _ = sock.write_all(resp.as_bytes()).await;
            let _ = sock.flush().await;
        });
    }
}

/// Bind `addr` and serve metrics (convenience over [`serve_metrics`]).
pub async fn bind_and_serve<F>(addr: SocketAddr, provider: F) -> io::Result<()>
where
    F: Fn() -> Vec<ShardLag> + Send + Sync + 'static,
{
    let listener = TcpListener::bind(addr).await?;
    serve_metrics(listener, provider).await
}

const RESUME_FROM_METRIC: &str = "ehdb_feed_shard_resume_from";
const RESUME_TIP_METRIC: &str = "ehdb_feed_shard_resume_tip";
const RESUME_STORED_METRIC: &str = "ehdb_feed_shard_resume_stored";
const RESUME_REPLAY_METRIC: &str = "ehdb_feed_shard_resume_replay_records";

/// Render the **resume facts** as Prometheus exposition text — the startup half
/// of the restart signal (noetl/ai-meta#208).
///
/// These are constants for the life of the process: what a restart resumed from
/// and whether it replayed. They exist as gauges rather than only a log line so a
/// runbook can *assert* the outcome — `ehdb_feed_shard_resume_replay_records == 0`
/// is the check "the restart was invisible", answerable from one scrape with no
/// arithmetic across samples and no log scraping.
///
/// `ehdb_feed_shard_committed` cannot answer it: sort keys are assigned from the
/// engine's recovered sequence, so the committed cursor is relative to what the
/// manifest recovered and legitimately steps *down* across a restart. That is
/// precisely what made the first prod restart's `origin="persisted"
/// from_cursor=0` line unreadable.
///
/// `resume_stored` is emitted only when a cursor was actually read back, so
/// "nothing stored" is an absent series rather than an indistinguishable `0`.
pub fn render_resume(reports: &[ResumeReport]) -> String {
    let mut ordered = reports.to_vec();
    ordered.sort_by_key(|r| r.shard);

    let mut out = String::new();
    out.push_str(&format!(
        "# HELP {RESUME_FROM_METRIC} The cursor this shard's claim group started from at process start.\n"
    ));
    out.push_str(&format!("# TYPE {RESUME_FROM_METRIC} gauge\n"));
    for r in &ordered {
        out.push_str(&format!(
            "{RESUME_FROM_METRIC}{{shard=\"{}\",origin=\"{}\"}} {}\n",
            r.shard,
            r.origin.as_str(),
            r.from_cursor
        ));
    }
    out.push_str(&format!(
        "# HELP {RESUME_TIP_METRIC} The reopened log's tip at resume time — the ceiling the stored cursor is clamped to.\n"
    ));
    out.push_str(&format!("# TYPE {RESUME_TIP_METRIC} gauge\n"));
    for r in &ordered {
        out.push_str(&format!(
            "{RESUME_TIP_METRIC}{{shard=\"{}\"}} {}\n",
            r.shard, r.tip
        ));
    }
    out.push_str(&format!(
        "# HELP {RESUME_STORED_METRIC} The cursor read back from the durable store before clamping (absent when nothing was stored).\n"
    ));
    out.push_str(&format!("# TYPE {RESUME_STORED_METRIC} gauge\n"));
    for r in &ordered {
        if let Some(stored) = r.stored_cursor {
            out.push_str(&format!(
                "{RESUME_STORED_METRIC}{{shard=\"{}\",clamped=\"{}\"}} {}\n",
                r.shard,
                r.clamped(),
                stored
            ));
        }
    }
    out.push_str(&format!(
        "# HELP {RESUME_REPLAY_METRIC} Records re-served from the existing log at resume — 0 means the restart replayed nothing.\n"
    ));
    out.push_str(&format!("# TYPE {RESUME_REPLAY_METRIC} gauge\n"));
    for r in &ordered {
        out.push_str(&format!(
            "{RESUME_REPLAY_METRIC}{{shard=\"{}\"}} {}\n",
            r.shard,
            r.replay_records()
        ));
    }
    out
}

/// [`serve_metrics`] plus the process-constant resume facts
/// ([`render_resume`]). `reports` is captured once at startup — these values do
/// not change while the process runs.
pub async fn serve_metrics_with_resume<F>(
    listener: TcpListener,
    reports: Vec<ResumeReport>,
    provider: F,
) -> io::Result<()>
where
    F: Fn() -> Vec<ShardLag> + Send + Sync + 'static,
{
    let resume = Arc::new(render_resume(&reports));
    let provider = Arc::new(provider);
    loop {
        let (mut sock, _peer) = listener.accept().await?;
        let provider = Arc::clone(&provider);
        let resume = Arc::clone(&resume);
        tokio::spawn(async move {
            let mut scratch = [0u8; 1024];
            let _ = sock.read(&mut scratch).await;
            let body = format!("{}{}", render_prometheus(&provider()), resume);
            let resp = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: text/plain; version=0.0.4\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                body.len(),
                body
            );
            let _ = sock.write_all(resp.as_bytes()).await;
            let _ = sock.flush().await;
        });
    }
}

/// Bind `addr` and serve lag + resume facts (convenience over
/// [`serve_metrics_with_resume`]).
pub async fn bind_and_serve_with_resume<F>(
    addr: SocketAddr,
    reports: Vec<ResumeReport>,
    provider: F,
) -> io::Result<()>
where
    F: Fn() -> Vec<ShardLag> + Send + Sync + 'static,
{
    let listener = TcpListener::bind(addr).await?;
    serve_metrics_with_resume(listener, reports, provider).await
}

/// Bind `addr` and serve a full lag snapshot (convenience over
/// [`serve_snapshot_metrics`]).
pub async fn bind_and_serve_snapshot<F>(addr: SocketAddr, provider: F) -> io::Result<()>
where
    F: Fn() -> LagSnapshot + Send + Sync + 'static,
{
    let listener = TcpListener::bind(addr).await?;
    serve_snapshot_metrics(listener, provider).await
}

/// [`serve_snapshot_metrics`] plus the process-constant resume facts
/// ([`render_resume`]) — **the endpoint a deployed writer serves.**
///
/// The two halves answer different questions and a writer needs both from one
/// scrape: the snapshot carries `ehdb_feed_subject_lag`, which is the per-pool
/// autoscaler trigger (noetl/ai-meta#194), and the resume facts carry whether
/// the last restart replayed anything (noetl/ai-meta#208). Serving only one
/// silently drops the other — [`serve_metrics_with_resume`] renders lag without
/// the per-subject split, so it must not be what the writer binds.
pub async fn serve_snapshot_metrics_with_resume<F>(
    listener: TcpListener,
    reports: Vec<ResumeReport>,
    provider: F,
) -> io::Result<()>
where
    F: Fn() -> LagSnapshot + Send + Sync + 'static,
{
    let resume = Arc::new(render_resume(&reports));
    let provider = Arc::new(provider);
    loop {
        let (mut sock, _peer) = listener.accept().await?;
        let provider = Arc::clone(&provider);
        let resume = Arc::clone(&resume);
        tokio::spawn(async move {
            let mut scratch = [0u8; 1024];
            let _ = sock.read(&mut scratch).await;
            let body = format!("{}{}", render_snapshot(&provider()), resume);
            let resp = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: text/plain; version=0.0.4\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                body.len(),
                body
            );
            let _ = sock.write_all(resp.as_bytes()).await;
            let _ = sock.flush().await;
        });
    }
}

/// Bind `addr` and serve the full lag snapshot + resume facts (convenience over
/// [`serve_snapshot_metrics_with_resume`]).
pub async fn bind_and_serve_snapshot_with_resume<F>(
    addr: SocketAddr,
    reports: Vec<ResumeReport>,
    provider: F,
) -> io::Result<()>
where
    F: Fn() -> LagSnapshot + Send + Sync + 'static,
{
    let listener = TcpListener::bind(addr).await?;
    serve_snapshot_metrics_with_resume(listener, reports, provider).await
}
