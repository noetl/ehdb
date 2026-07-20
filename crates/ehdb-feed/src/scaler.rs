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

use std::io;
use std::net::SocketAddr;
use std::sync::Arc;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

/// One shard consumer group's lag sample.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ShardLag {
    pub shard: u32,
    /// The group's committed-through cursor (acked prefix).
    pub committed: u64,
    /// Backlog: shard records past `committed` (undelivered + unacked).
    pub lag: u64,
}

const LAG_METRIC: &str = "ehdb_feed_shard_lag";
const TOTAL_METRIC: &str = "ehdb_feed_total_lag";

/// Render shard lags as Prometheus exposition text (v0.0.4).
pub fn render_prometheus(samples: &[ShardLag]) -> String {
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
    let total: u64 = ordered.iter().map(|s| s.lag).sum();
    out.push_str(&format!(
        "# HELP {TOTAL_METRIC} Total consumer-group backlog across all shards.\n"
    ));
    out.push_str(&format!("# TYPE {TOTAL_METRIC} gauge\n"));
    out.push_str(&format!("{TOTAL_METRIC} {total}\n"));
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
    let provider = Arc::new(provider);
    loop {
        let (mut sock, _peer) = listener.accept().await?;
        let provider = Arc::clone(&provider);
        tokio::spawn(async move {
            // Drain the request head (we don't route on it); tolerate a short read.
            let mut scratch = [0u8; 1024];
            let _ = sock.read(&mut scratch).await;
            let body = render_prometheus(&provider());
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
