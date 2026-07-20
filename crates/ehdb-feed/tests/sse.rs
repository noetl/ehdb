//! L1 T3 proof — the gateway/SPA SSE live feed: event-stream framing, ordered
//! delivery, and Last-Event-ID reconnect resume.

use std::sync::Arc;
use std::time::Duration;

use ehdb_feed::sse::serve_sse;
use ehdb_feed::FeedWriter;
use ehdb_l0::substrate::DurableSubstrate;
use ehdb_l0::{D1EventLog, EventRecord, FlushPolicy, L0Config, L0Engine, LocalFsSubstrate};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

fn unique_dir(tag: &str) -> std::path::PathBuf {
    static N: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let n = N.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    std::env::temp_dir().join(format!("ehdb-feed-t3-{tag}-{}-{n}", std::process::id()))
}

fn ev(seq: u64) -> EventRecord {
    EventRecord::new(seq, format!("exec-{seq}"), "t", "command-payload")
}

/// Parse SSE `id:`/`data:` events out of a raw response body slice.
fn parse_events(raw: &str) -> Vec<(u64, String)> {
    let mut out = Vec::new();
    for block in raw.split("\n\n") {
        let mut id = None;
        let mut data = None;
        for line in block.lines() {
            if let Some(v) = line.strip_prefix("id: ") {
                id = v.trim().parse::<u64>().ok();
            } else if let Some(v) = line.strip_prefix("data: ") {
                data = Some(v.to_string());
            }
        }
        if let (Some(id), Some(data)) = (id, data) {
            out.push((id, data));
        }
    }
    out
}

/// Read from the socket until `want` events have been parsed (or timeout).
async fn read_until(sock: &mut TcpStream, want: usize) -> Vec<(u64, String)> {
    let mut acc = String::new();
    let mut tmp = [0u8; 2048];
    for _ in 0..200 {
        match tokio::time::timeout(Duration::from_secs(5), sock.read(&mut tmp)).await {
            Ok(Ok(0)) => break,
            Ok(Ok(n)) => {
                acc.push_str(&String::from_utf8_lossy(&tmp[..n]));
                if parse_events(&acc).len() >= want {
                    break;
                }
            }
            _ => break,
        }
    }
    assert!(
        acc.contains("Content-Type: text/event-stream"),
        "SSE header present: {acc:?}"
    );
    parse_events(&acc)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn sse_stream_and_last_event_id_resume() {
    let (obj, local) = (unique_dir("obj"), unique_dir("local"));
    let store: Arc<dyn DurableSubstrate> = Arc::new(LocalFsSubstrate::new(&obj).unwrap());
    let engine = L0Engine::<D1EventLog>::open(
        L0Config::d1(&local).with_flush(FlushPolicy::Buffered { fsync_every: 64 }),
        store,
    )
    .unwrap();
    let writer = Arc::new(FeedWriter::new(engine));

    // Seed 3 records before anyone connects.
    for seq in 1..=3u64 {
        writer.append(ev(seq)).unwrap();
    }

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(serve_sse(listener, writer.clone()));

    // Client A: subscribe from the beginning. Gets the seeded 1,2,3, and 4,5
    // appended live after it connected — proving both replay and live push.
    let mut a = TcpStream::connect(addr).await.unwrap();
    a.set_nodelay(true).unwrap();
    a.write_all(b"GET /feed?shard=0&cursor=0 HTTP/1.1\r\nHost: x\r\n\r\n")
        .await
        .unwrap();
    a.flush().await.unwrap();
    // A small settle so the subscription is established before the live appends.
    tokio::task::yield_now().await;
    writer.append(ev(4)).unwrap();
    writer.append(ev(5)).unwrap();

    let seen = read_until(&mut a, 5).await;
    assert_eq!(
        seen.iter().map(|(id, _)| *id).collect::<Vec<_>>(),
        vec![1, 2, 3, 4, 5],
        "seeded replay + live push, in order"
    );
    assert!(seen[0].1.contains("\"global_sequence\":1"));
    drop(a); // client A "drops" its connection

    // Client B reconnects with Last-Event-ID: 3 (as a browser EventSource would)
    // → resumes at 4,5 with no missed / no duplicate events.
    let mut b = TcpStream::connect(addr).await.unwrap();
    b.set_nodelay(true).unwrap();
    b.write_all(b"GET /feed?shard=0 HTTP/1.1\r\nLast-Event-ID: 3\r\nHost: x\r\n\r\n")
        .await
        .unwrap();
    b.flush().await.unwrap();
    let resumed = read_until(&mut b, 2).await;
    assert_eq!(
        resumed.iter().map(|(id, _)| *id).collect::<Vec<_>>(),
        vec![4, 5],
        "reconnect resumes exactly after Last-Event-ID, 0 missed / 0 dup"
    );

    for d in [&obj, &local] {
        let _ = std::fs::remove_dir_all(d);
    }
}
