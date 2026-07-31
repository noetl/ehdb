//! noetl/ai-meta#208 follow-up — the restart signal must be readable on its own.
//!
//! The writer-restart fix works; its *signal* did not. The first prod restart on
//! the EHDB bus logged `origin="persisted" from_cursor=0`, which is exactly what
//! a replay-from-the-beginning looks like. It was not one — the reopened log's
//! tip was itself low, the stored cursor was clamped down to it, and nothing was
//! re-served — but proving that took arithmetic across two scrapes. These tests
//! pin the report + gauges that make the outcome self-evident from one look.

use std::sync::Arc;
use std::time::Duration;

use ehdb_feed::scaler::render_resume;
use ehdb_feed::{
    ClaimCoordinator, CursorFallback, CursorOrigin, CursorStore, FeedWriter, ResumeReport,
};
use ehdb_l0::substrate::DurableSubstrate;
use ehdb_l0::{D1EventLog, EventRecord, L0Config, L0Engine, LocalFsSubstrate};

fn unique_dir(tag: &str) -> std::path::PathBuf {
    static N: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let n = N.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    std::env::temp_dir().join(format!("ehdb-feed-resume-{tag}-{}-{n}", std::process::id()))
}

fn writer_at(local: &std::path::Path, obj: &std::path::Path) -> Arc<FeedWriter<D1EventLog>> {
    let store: Arc<dyn DurableSubstrate> = Arc::new(LocalFsSubstrate::new(obj).unwrap());
    let engine = L0Engine::<D1EventLog>::open(L0Config::d1(local), store).unwrap();
    Arc::new(FeedWriter::new(engine))
}

fn report(stored: Option<u64>, tip: u64, from_cursor: u64, origin: CursorOrigin) -> ResumeReport {
    ResumeReport {
        shard: 0,
        stored_cursor: stored,
        tip,
        from_cursor,
        origin,
    }
}

#[test]
fn the_prod_line_that_read_like_a_replay_is_now_unambiguous() {
    // The observed shape: a persisted resume whose stored cursor (408) was above
    // the reopened log's tip (165), so it clamped down. The old line said
    // `origin=persisted from_cursor=<low>` and stopped there.
    let clamped = report(Some(408), 165, 165, CursorOrigin::Persisted);
    assert!(clamped.clamped());
    assert!(!clamped.replayed(), "clamping to the tip replays nothing");
    assert_eq!(clamped.replay_records(), 0);
    assert_eq!(
        clamped.to_string(),
        "shard=0 origin=persisted stored_cursor=408 tip=165 from_cursor=165 \
         clamped=true replay=false replay_records=0"
    );

    // The failure it must be distinguishable from: a real replay of the log.
    let replaying = report(None, 165, 0, CursorOrigin::FallbackBeginning);
    assert!(replaying.replayed());
    assert_eq!(replaying.replay_records(), 165);
    assert_eq!(
        replaying.to_string(),
        "shard=0 origin=fallback_beginning stored_cursor=none tip=165 from_cursor=0 \
         clamped=false replay=true replay_records=165"
    );

    // …and from an ordinary clean resume with nothing outstanding.
    let clean = report(Some(165), 165, 165, CursorOrigin::Persisted);
    assert!(!clean.clamped());
    assert!(!clean.replayed());
}

#[test]
fn an_uncommitted_tail_shows_up_as_replay_records_not_as_a_guess() {
    // Stored cursor behind the tip: the genuinely-unacked tail is re-served, and
    // the count says exactly how much.
    let r = report(Some(100), 112, 100, CursorOrigin::Persisted);
    assert!(!r.clamped());
    assert!(r.replayed());
    assert_eq!(r.replay_records(), 12);
}

#[test]
fn resume_gauges_answer_the_runbook_question_from_one_scrape() {
    let text = render_resume(&[
        report(Some(408), 165, 165, CursorOrigin::Persisted),
        ResumeReport {
            shard: 1,
            stored_cursor: None,
            tip: 20,
            from_cursor: 20,
            origin: CursorOrigin::FallbackTail,
        },
    ]);

    assert!(text.contains("# TYPE ehdb_feed_shard_resume_replay_records gauge"));
    // "The restart was invisible" is a single equality, no cross-sample math.
    assert!(text.contains("ehdb_feed_shard_resume_replay_records{shard=\"0\"} 0\n"));
    assert!(text.contains("ehdb_feed_shard_resume_replay_records{shard=\"1\"} 0\n"));
    // The three inputs are all present, so the outcome can be re-derived.
    assert!(text.contains("ehdb_feed_shard_resume_from{shard=\"0\",origin=\"persisted\"} 165\n"));
    assert!(text.contains("ehdb_feed_shard_resume_tip{shard=\"0\"} 165\n"));
    assert!(text.contains("ehdb_feed_shard_resume_stored{shard=\"0\",clamped=\"true\"} 408\n"));
    // Shard 1 stored nothing: an absent series, not a `0` that reads like a
    // cursor pointing at the start of the log.
    assert!(
        !text.contains("ehdb_feed_shard_resume_stored{shard=\"1\""),
        "nothing stored must be absent, not 0 — got:\n{text}"
    );
    assert!(text.contains("ehdb_feed_shard_resume_from{shard=\"1\",origin=\"fallback_tail\"} 20\n"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_resumed_coordinator_reports_its_own_restart() {
    let (obj, local) = (unique_dir("obj"), unique_dir("local"));
    let writer = writer_at(&local, &obj);
    for seq in 1..=6u64 {
        writer
            .append(EventRecord::new(seq, format!("e{seq}"), "t", "cmd"))
            .unwrap();
    }
    let tip = writer.engine().lock().unwrap().global_sequence();

    // Nothing persisted yet → tail fallback, and nothing replays.
    let store = CursorStore::open(&local, 0).unwrap();
    let fresh = ClaimCoordinator::resume(
        writer.clone(),
        0,
        Duration::from_secs(30),
        ehdb_feed::d1_command_subject(1),
        store,
        CursorFallback::Tail,
    )
    .unwrap();
    let r = fresh
        .resume_report()
        .expect("a resumed coordinator reports");
    assert_eq!(r.origin, CursorOrigin::FallbackTail);
    assert_eq!(r.stored_cursor, None);
    assert_eq!((r.tip, r.from_cursor), (tip, tip));
    assert!(!r.replayed(), "tail fallback replays nothing: {r}");

    // A coordinator built without durable progress has no report at all — so a
    // caller can never read "no store configured" as "resumed at 0".
    let plain = ClaimCoordinator::new(
        writer.clone(),
        0,
        Duration::from_secs(30),
        0,
        ehdb_feed::d1_command_subject(1),
    );
    assert!(plain.resume_report().is_none());

    for d in [&obj, &local] {
        let _ = std::fs::remove_dir_all(d);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_replaying_restart_is_reported_as_a_replay() {
    // The explicit escape hatch (`CursorFallback::Beginning`) is the one posture
    // that does replay — and the report must say so rather than looking like the
    // clamped case.
    let (obj, local) = (unique_dir("replay-obj"), unique_dir("replay-local"));
    let writer = writer_at(&local, &obj);
    for seq in 1..=9u64 {
        writer
            .append(EventRecord::new(seq, format!("e{seq}"), "t", "cmd"))
            .unwrap();
    }
    let tip = writer.engine().lock().unwrap().global_sequence();

    let coord = ClaimCoordinator::resume(
        writer.clone(),
        0,
        Duration::from_secs(30),
        ehdb_feed::d1_command_subject(1),
        CursorStore::open(&local, 0).unwrap(),
        CursorFallback::Beginning,
    )
    .unwrap();
    let r = coord.resume_report().unwrap();
    assert_eq!(r.origin, CursorOrigin::FallbackBeginning);
    assert_eq!(r.from_cursor, 0);
    assert!(r.replayed());
    assert_eq!(r.replay_records(), tip);

    for d in [&obj, &local] {
        let _ = std::fs::remove_dir_all(d);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_writers_endpoint_serves_the_resume_facts_and_the_per_subject_lag_together() {
    // The two families answer different questions and the writer needs both from
    // one scrape: `ehdb_feed_subject_lag` is the per-pool autoscaler trigger
    // (noetl/ai-meta#194), the resume gauges say whether the last restart
    // replayed (noetl/ai-meta#208). Binding a server that renders only one drops
    // the other silently — nothing errors, the series is just absent, and for the
    // lag family an absent series is a KEDA *scaler error*. This pins the
    // combined endpoint so that regression cannot ship.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);

    let reports = vec![ResumeReport {
        shard: 0,
        from_cursor: 41,
        tip: 41,
        stored_cursor: Some(41),
        origin: CursorOrigin::Persisted,
    }];

    tokio::spawn(ehdb_feed::bind_and_serve_snapshot_with_resume(
        addr,
        reports,
        || ehdb_feed::LagSnapshot {
            shards: vec![ehdb_feed::ShardLag {
                shard: 0,
                committed: 41,
                lag: 6,
            }],
            subjects: vec![ehdb_feed::SubjectLag {
                subject: "commands.shared.shard.0".to_string(),
                lag: 6,
            }],
        },
    ));

    let mut sock = {
        let mut attempt = None;
        for _ in 0..50 {
            match tokio::net::TcpStream::connect(addr).await {
                Ok(s) => {
                    attempt = Some(s);
                    break;
                }
                Err(_) => tokio::task::yield_now().await,
            }
        }
        attempt.expect("metrics server accepted a connection")
    };
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    sock.write_all(b"GET /metrics HTTP/1.1\r\nHost: x\r\n\r\n")
        .await
        .unwrap();
    sock.flush().await.unwrap();
    let mut resp = String::new();
    sock.read_to_string(&mut resp).await.unwrap();

    assert!(resp.starts_with("HTTP/1.1 200 OK"), "got: {resp}");
    // The autoscaler trigger, byte-stable for KEDA's prefix match.
    assert!(
        resp.contains("ehdb_feed_subject_lag{subject=\"commands.shared.shard.0\"} 6\n"),
        "per-subject lag missing: {resp}"
    );
    // The restart verdict, answerable without arithmetic across scrapes.
    assert!(
        resp.contains("ehdb_feed_shard_resume_replay_records{shard=\"0\"} 0\n"),
        "resume facts missing: {resp}"
    );
    // And the pre-existing families the runbooks already read.
    assert!(resp.contains("ehdb_feed_total_lag 6\n"), "{resp}");
    assert!(
        resp.contains("ehdb_feed_shard_committed{shard=\"0\"} 41\n"),
        "{resp}"
    );
}
