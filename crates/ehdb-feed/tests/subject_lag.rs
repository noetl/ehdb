//! noetl/ai-meta#194 T2 proof — the **per-pool** autoscaler lag signal.
//!
//! Whole-shard lag (`ehdb_feed_total_lag`) is the wrong trigger for scaling one
//! pool: the pools share a shard, so a system-pool backlog would scale the user
//! pool, and a single stuck system-pool command pins the global committed cursor
//! and therefore pins whole-shard lag high forever. These tests pin the
//! per-subject split that a pool's ScaledObject reads instead, plus the
//! byte-stable exposition line KEDA's `metrics-api` scaler prefix-matches.

use std::sync::Arc;
use std::time::Duration;

use ehdb_feed::scaler::{bind_and_serve_snapshot, LagSnapshot, SubjectLag};
use ehdb_feed::{
    render_snapshot, ClaimCoordinator, FeedWriter, ShardLag, SubjectConsumerGroup, SubjectFilter,
};
use ehdb_l0::substrate::DurableSubstrate;
use ehdb_l0::{D1EventLog, EventRecord, L0Config, L0Engine, LocalFsSubstrate};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

fn unique_dir(tag: &str) -> std::path::PathBuf {
    static N: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let n = N.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    std::env::temp_dir().join(format!(
        "ehdb-feed-subjlag-{tag}-{}-{n}",
        std::process::id()
    ))
}

/// A command notification for `pool` — the shape the server stamps
/// (`execution_pool`), which `d1_command_subject` routes on.
fn cmd(seq: u64, pool: &str) -> EventRecord {
    EventRecord::new(
        seq,
        format!("exec-{seq}"),
        "t",
        format!("{{\"execution_pool\":\"{pool}\"}}"),
    )
}

fn engine_at(local: &std::path::Path, obj: &std::path::Path) -> L0Engine<D1EventLog> {
    let store: Arc<dyn DurableSubstrate> = Arc::new(LocalFsSubstrate::new(obj).unwrap());
    L0Engine::<D1EventLog>::open(L0Config::d1(local), store).unwrap()
}

#[test]
fn subject_lags_split_the_shard_backlog_by_pool() {
    let (obj, local) = (unique_dir("obj"), unique_dir("local"));
    let mut engine = engine_at(&local, &obj);

    // 6 shared-pool + 4 system-pool commands, interleaved, on the one shard.
    for seq in 1..=10u64 {
        let pool = if seq % 5 == 0 { "system" } else { "shared" };
        engine.append_record(cmd(seq, pool)).unwrap();
    }

    let mut group =
        SubjectConsumerGroup::<D1EventLog>::new(0, 1_000, 0, ehdb_feed::d1_command_subject(1));
    // Pull everything onto the group so subjects are known, without acking.
    let shared: SubjectFilter = SubjectFilter::parse("commands.shared.>");
    while group.poll_assign(&engine, &shared, 1, 0).unwrap().is_some() {}

    let lags = group.subject_lags(&engine).unwrap();
    let shared_lag = lags
        .iter()
        .find(|(s, _)| s == "commands.shared.shard.0")
        .map(|(_, n)| *n);
    let system_lag = lags
        .iter()
        .find(|(s, _)| s == "commands.system.shard.0")
        .map(|(_, n)| *n);
    assert_eq!(shared_lag, Some(8), "shared pool's own backlog");
    assert_eq!(system_lag, Some(2), "system pool's own backlog");
    // With nothing acked, the split is exactly the whole-shard number.
    assert_eq!(
        lags.iter().map(|(_, n)| *n).sum::<u64>(),
        group.lag(&engine).unwrap(),
        "per-subject lags sum to whole-shard lag while nothing is acked"
    );

    for d in [&obj, &local] {
        let _ = std::fs::remove_dir_all(d);
    }
}

#[test]
fn a_stuck_system_command_does_not_inflate_the_shared_pools_lag() {
    // The failure this metric exists to prevent: the global committed cursor
    // cannot advance past an unacked system-pool command, so whole-shard lag
    // stays high even with the shared pool fully drained — which would pin the
    // user pool at maxReplicas forever.
    let (obj, local) = (unique_dir("obj"), unique_dir("local"));
    let mut engine = engine_at(&local, &obj);
    engine.append_record(cmd(1, "system")).unwrap();
    for seq in 2..=6u64 {
        engine.append_record(cmd(seq, "shared")).unwrap();
    }

    let mut group =
        SubjectConsumerGroup::<D1EventLog>::new(0, 1_000_000, 0, ehdb_feed::d1_command_subject(1));
    // The system command is claimed and never acked (the stuck worker).
    let system: SubjectFilter = SubjectFilter::parse("commands.system.>");
    group.poll_assign(&engine, &system, 9, 0).unwrap().unwrap();
    // The shared pool drains its own queue completely.
    let shared: SubjectFilter = SubjectFilter::parse("commands.shared.>");
    while let Some(d) = group.poll_assign(&engine, &shared, 1, 0).unwrap() {
        assert!(group.ack(d.sort_key));
    }

    assert_eq!(
        group.lag(&engine).unwrap(),
        6,
        "whole-shard lag is pinned by the stuck system command"
    );
    let lags = group.subject_lags(&engine).unwrap();
    assert_eq!(
        lags.iter()
            .find(|(s, _)| s == "commands.shared.shard.0")
            .map(|(_, n)| *n),
        Some(0),
        "the shared pool's own lag is 0 — the trigger the user pool must read"
    );
    assert_eq!(
        lags.iter()
            .find(|(s, _)| s == "commands.system.shard.0")
            .map(|(_, n)| *n),
        Some(1)
    );

    for d in [&obj, &local] {
        let _ = std::fs::remove_dir_all(d);
    }
}

#[test]
fn seeding_reports_a_drained_subject_as_zero_rather_than_absent() {
    // A group resumed at the tail has seen no records, so without seeding its
    // label set is empty — and a KEDA trigger whose series has vanished is a
    // scaler error, not a scale-to-min.
    let (obj, local) = (unique_dir("obj"), unique_dir("local"));
    let mut engine = engine_at(&local, &obj);
    for seq in 1..=4u64 {
        engine.append_record(cmd(seq, "shared")).unwrap();
    }
    engine.append_record(cmd(5, "system")).unwrap();
    let tip = engine.global_sequence();

    // Resume at the tail: nothing to deliver.
    let mut group =
        SubjectConsumerGroup::<D1EventLog>::new(0, 1_000, tip, ehdb_feed::d1_command_subject(1));
    assert!(
        group.subject_lags(&engine).unwrap().is_empty(),
        "unseeded: no label set at all"
    );

    group.seed_subjects(&engine).unwrap();
    let lags = group.subject_lags(&engine).unwrap();
    assert_eq!(
        lags,
        vec![
            ("commands.shared.shard.0".to_string(), 0),
            ("commands.system.shard.0".to_string(), 0),
        ],
        "every subject the shard has carried reports 0 after seeding"
    );

    for d in [&obj, &local] {
        let _ = std::fs::remove_dir_all(d);
    }
}

#[test]
fn the_exposition_line_is_byte_stable_for_kedas_prefix_match() {
    // KEDA's metrics-api scaler (format: prometheus) has no label selector: it
    // prefix-matches `valueLocation` against the whole `name{labels}` token and
    // takes the first hit. So this exact line shape is a contract.
    let text = render_snapshot(&LagSnapshot {
        shards: vec![ShardLag {
            shard: 0,
            committed: 41,
            lag: 12,
        }],
        subjects: vec![
            SubjectLag {
                subject: "commands.system.shard.0".to_string(),
                lag: 3,
            },
            SubjectLag {
                subject: "commands.shared.shard.0".to_string(),
                lag: 9,
            },
        ],
    });

    assert!(text.contains("# TYPE ehdb_feed_subject_lag gauge"));
    assert!(
        text.contains("ehdb_feed_subject_lag{subject=\"commands.shared.shard.0\"} 9\n"),
        "exactly one label, no spaces — got:\n{text}"
    );
    assert!(text.contains("ehdb_feed_subject_lag{subject=\"commands.system.shard.0\"} 3\n"));
    // Sorted regardless of input order, so a scrape diff is stable.
    let shared = text.find("commands.shared.shard.0").unwrap();
    let system = text.find("commands.system.shard.0").unwrap();
    assert!(shared < system);
    // The pre-existing families are untouched (the worker still renders them).
    assert!(text.contains("ehdb_feed_total_lag 12\n"));
    assert!(text.contains("ehdb_feed_shard_committed{shard=\"0\"} 41\n"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_metrics_endpoint_serves_the_per_subject_series() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);

    tokio::spawn(bind_and_serve_snapshot(addr, || LagSnapshot {
        shards: vec![ShardLag {
            shard: 0,
            committed: 5,
            lag: 4,
        }],
        subjects: vec![SubjectLag {
            subject: "commands.shared.shard.0".to_string(),
            lag: 4,
        }],
    }));

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
    assert!(resp.contains("ehdb_feed_subject_lag{subject=\"commands.shared.shard.0\"} 4\n"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_coordinator_exposes_its_subject_split() {
    // The wiring the writer host actually uses.
    let (obj, local) = (unique_dir("coord-obj"), unique_dir("coord-local"));
    let engine = engine_at(&local, &obj);
    let writer = Arc::new(FeedWriter::new(engine));
    let coord = ClaimCoordinator::new(
        writer.clone(),
        0,
        Duration::from_secs(30),
        0,
        ehdb_feed::d1_command_subject(1),
    );

    for seq in 1..=3u64 {
        writer.append(cmd(seq, "shared")).unwrap();
    }
    writer.append(cmd(4, "system")).unwrap();

    let lags = coord.subject_lags().await;
    assert_eq!(
        lags,
        vec![
            SubjectLag {
                subject: "commands.shared.shard.0".to_string(),
                lag: 3
            },
            SubjectLag {
                subject: "commands.system.shard.0".to_string(),
                lag: 1
            },
        ]
    );
    assert_eq!(coord.lag().await, 4, "and it still sums to whole-shard lag");

    for d in [&obj, &local] {
        let _ = std::fs::remove_dir_all(d);
    }
}
