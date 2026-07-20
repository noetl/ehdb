//! L1 T2 proof — the KEDA lag signal: lag reflects backlog, Prometheus
//! exposition renders correctly, and the /metrics endpoint is scrapeable.

use std::sync::Arc;

use ehdb_feed::scaler::{bind_and_serve, ShardLag};
use ehdb_feed::{render_prometheus, ShardConsumerGroup};
use ehdb_l0::substrate::DurableSubstrate;
use ehdb_l0::{D1EventLog, L0Config, L0Engine, LocalFsSubstrate};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

fn unique_dir(tag: &str) -> std::path::PathBuf {
    static N: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let n = N.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    std::env::temp_dir().join(format!("ehdb-feed-t2-{tag}-{}-{n}", std::process::id()))
}

#[test]
fn lag_tracks_backlog() {
    let (obj, local) = (unique_dir("obj"), unique_dir("local"));
    let store: Arc<dyn DurableSubstrate> = Arc::new(LocalFsSubstrate::new(&obj).unwrap());
    let mut engine = L0Engine::<D1EventLog>::open(L0Config::d1(&local), store).unwrap();
    for i in 0..10u64 {
        engine.append(&format!("e{i}"), "t", "cmd").unwrap();
    }

    let mut group = ShardConsumerGroup::<D1EventLog>::new(0, 100, 0);
    // Nothing consumed → lag == full backlog.
    assert_eq!(group.lag(&engine).unwrap(), 10);

    // Consume + ack 4 → lag drops to 6.
    for _ in 0..4 {
        let d = group.poll_assign(&engine, 1, 0).unwrap().unwrap();
        group.ack(d.sort_key);
    }
    assert_eq!(group.lag(&engine).unwrap(), 6);

    // Deliver-but-don't-ack 2 more: still backlog (unacked counts as lag).
    group.poll_assign(&engine, 1, 0).unwrap().unwrap();
    group.poll_assign(&engine, 1, 0).unwrap().unwrap();
    assert_eq!(
        group.lag(&engine).unwrap(),
        6,
        "in-flight unacked still counts as lag"
    );

    // New appends grow the lag.
    engine.append("e-new", "t", "cmd").unwrap();
    assert_eq!(group.lag(&engine).unwrap(), 7);

    for d in [&obj, &local] {
        let _ = std::fs::remove_dir_all(d);
    }
}

#[test]
fn prometheus_exposition_is_well_formed() {
    let samples = vec![
        ShardLag {
            shard: 2,
            committed: 5,
            lag: 3,
        },
        ShardLag {
            shard: 0,
            committed: 9,
            lag: 0,
        },
        ShardLag {
            shard: 1,
            committed: 4,
            lag: 12,
        },
    ];
    let text = render_prometheus(&samples);

    // Header lines present.
    assert!(text.contains("# TYPE ehdb_feed_shard_lag gauge"));
    assert!(text.contains("# TYPE ehdb_feed_total_lag gauge"));
    // One series per shard, in shard order, with the right values.
    assert!(text.contains("ehdb_feed_shard_lag{shard=\"0\"} 0\n"));
    assert!(text.contains("ehdb_feed_shard_lag{shard=\"1\"} 12\n"));
    assert!(text.contains("ehdb_feed_shard_lag{shard=\"2\"} 3\n"));
    // Deterministic ordering: shard 0 before 1 before 2.
    let p0 = text.find("shard=\"0\"").unwrap();
    let p1 = text.find("shard=\"1\"").unwrap();
    let p2 = text.find("shard=\"2\"").unwrap();
    assert!(p0 < p1 && p1 < p2);
    // Aggregate = sum.
    assert!(text.contains("ehdb_feed_total_lag 15\n"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn metrics_endpoint_is_scrapeable() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener); // free the port for bind_and_serve

    tokio::spawn(bind_and_serve(addr, || {
        vec![
            ShardLag {
                shard: 0,
                committed: 3,
                lag: 7,
            },
            ShardLag {
                shard: 1,
                committed: 0,
                lag: 2,
            },
        ]
    }));

    // Give the server a moment to bind (retry connect).
    let mut sock = {
        let mut attempt = None;
        for _ in 0..50 {
            match TcpStream::connect(addr).await {
                Ok(s) => {
                    attempt = Some(s);
                    break;
                }
                Err(_) => tokio::task::yield_now().await,
            }
        }
        attempt.expect("metrics server accepted a connection")
    };
    sock.write_all(b"GET /metrics HTTP/1.1\r\nHost: x\r\n\r\n")
        .await
        .unwrap();
    sock.flush().await.unwrap();

    let mut resp = String::new();
    sock.read_to_string(&mut resp).await.unwrap();

    assert!(resp.starts_with("HTTP/1.1 200 OK"), "got: {resp}");
    assert!(resp.contains("text/plain; version=0.0.4"));
    assert!(resp.contains("ehdb_feed_shard_lag{shard=\"0\"} 7\n"));
    assert!(resp.contains("ehdb_feed_shard_lag{shard=\"1\"} 2\n"));
    assert!(resp.contains("ehdb_feed_total_lag 9\n"));
}
