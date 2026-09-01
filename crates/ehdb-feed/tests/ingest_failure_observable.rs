//! **A failing ingest face must not look like a healthy one** (noetl/ehdb#345).
//!
//! `serve_ingest` answered both of its failure modes with a bare `return`: the
//! socket closed, the publisher saw `connection closed before ack`, and the
//! writer said nothing. On 2026-09-01 that hid a **100%-full `/data/cmdbus`**
//! for hours. Every `POST /api/execute` on the platform returned 500 while the
//! writer pod reported `Ready`, `restarts=0`, and **zero ERROR and zero WARN
//! lines**, with all nine listeners bound. Diagnosis burned several rounds
//! ruling out DNS, endpoints, selector drift, pod-IP staleness and image
//! mismatch — all of which were fine — because the one component that knew what
//! was wrong was silent.
//!
//! Worse, the two failure modes were *indistinguishable*. A full volume and a
//! publisher speaking an incompatible record shape produced byte-identical
//! symptoms, so the symptom could not point at a remedy. These tests pin that
//! each is counted, that they are counted **separately**, and — the control —
//! that a healthy publish counts neither.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use ehdb_feed::{serve_ingest, FeedWriter, PublishClient};
use ehdb_l0::substrate::DurableSubstrate;
use ehdb_l0::{D1EventLog, EventRecord, L0Config, L0Engine, LocalFsSubstrate};
use tokio::io::AsyncWriteExt;

fn unique_dir(tag: &str) -> std::path::PathBuf {
    static N: AtomicU64 = AtomicU64::new(0);
    let n = N.fetch_add(1, Ordering::Relaxed);
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    std::env::temp_dir().join(format!(
        "ehdb-feed-ingest-fail-{tag}-{}-{n}-{nanos}",
        std::process::id()
    ))
}

fn ev(id: u64) -> EventRecord {
    EventRecord::new(
        id,
        format!("exec-{id}"),
        format!("tx-{id}"),
        format!(r#"{{"event_type":"action_started","seq":{id}}}"#),
    )
}

fn open_writer(tag: &str) -> Arc<FeedWriter<D1EventLog>> {
    let dir = unique_dir(tag);
    std::fs::create_dir_all(&dir).unwrap();
    let store: Arc<dyn DurableSubstrate> = Arc::new(LocalFsSubstrate::new(&dir).unwrap());
    let engine =
        L0Engine::<D1EventLog>::open(L0Config::d1(&dir).with_shard_count(1), store).unwrap();
    Arc::new(FeedWriter::new(engine))
}

async fn bind_ingest(w: &Arc<FeedWriter<D1EventLog>>) -> std::net::SocketAddr {
    let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = l.local_addr().unwrap();
    tokio::spawn(serve_ingest(l, w.clone()));
    addr
}

/// The wire format `serve_ingest` reads: 4-byte big-endian length, then body.
async fn send_raw_frame(addr: std::net::SocketAddr, body: &[u8]) {
    let mut sock = tokio::net::TcpStream::connect(addr).await.unwrap();
    sock.write_all(&(body.len() as u32).to_be_bytes())
        .await
        .unwrap();
    sock.write_all(body).await.unwrap();
    sock.flush().await.unwrap();
    // Give the reader task a moment to decode and drop the connection.
    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_append_failure_is_counted() {
    // A sealed writer is the reachable stand-in for the full volume: both make
    // `append_batch` return `Err`, which is the branch that used to be silent.
    let w = open_writer("append");
    let addr = bind_ingest(&w).await;
    w.seal_and_close().unwrap();

    let mut client = PublishClient::connect(addr).await.unwrap();
    let result = client.publish(&ev(1)).await;

    assert!(
        result.is_err(),
        "the publish must still fail — this change makes the failure observable, \
         it does not make it succeed"
    );
    let snap = w.metrics().snapshot();
    assert!(
        snap.ingest_append_failed > 0,
        "an append the writer refused must be counted; before this it produced no log, \
         no metric, and a Ready-looking writer"
    );
    assert_eq!(
        snap.ingest_decode_failed, 0,
        "an append failure must NOT be attributed to decoding — the whole point is that \
         'volume is full' and 'record shape disagrees' stop being the same symptom"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_decode_failure_is_counted_separately() {
    let w = open_writer("decode");
    let addr = bind_ingest(&w).await;

    send_raw_frame(addr, b"{ this is not a valid record }").await;

    let snap = w.metrics().snapshot();
    assert!(
        snap.ingest_decode_failed > 0,
        "a frame that does not deserialize must be counted"
    );
    assert_eq!(
        snap.ingest_append_failed, 0,
        "a decode failure must not be attributed to the volume — it would send an \
         operator to check disk space on a healthy disk"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn positive_control_a_healthy_publish_counts_nothing() {
    // ⚠ Without this, both assertions above would still pass on an implementation
    // that incremented the counters unconditionally — which would page on every
    // healthy write and be worse than the silence it replaced.
    let w = open_writer("healthy");
    let addr = bind_ingest(&w).await;

    let mut client = PublishClient::connect(addr).await.unwrap();
    let seq = client.publish(&ev(1)).await.unwrap();
    assert!(seq > 0, "the healthy publish must actually have been acked");

    let snap = w.metrics().snapshot();
    assert_eq!(
        snap.ingest_append_failed, 0,
        "a successful publish must not count as an append failure"
    );
    assert_eq!(
        snap.ingest_decode_failed, 0,
        "a successful publish must not count as a decode failure"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_counter_is_not_throttled_even_though_the_log_is() {
    // The log throttles to 1-in-256 so an outage cannot flood; the metric must
    // still see every failure, or the throttle would silently become an
    // undercount and the alert would fire late or not at all.
    let w = open_writer("throttle");
    let addr = bind_ingest(&w).await;
    w.seal_and_close().unwrap();

    let attempts = 12u64;
    for _ in 0..attempts {
        if let Ok(mut c) = PublishClient::connect(addr).await {
            let _ = c.publish(&ev(1)).await;
        }
    }

    let snap = w.metrics().snapshot();
    assert_eq!(
        snap.ingest_append_failed, attempts,
        "every failed append must be counted, not one in 256 — the throttle governs \
         log volume only"
    );
}
