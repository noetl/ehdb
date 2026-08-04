//! **The shutdown seal must not own a thread (noetl/ai-meta#226).**
//!
//! The worker's graceful SIGTERM sealed the command-bus host and then never
//! reached the events host. Not a budget problem — idle, the same code sealed
//! both in 568 ms with 90 s of grace left. The mechanism was the seal's *hold*:
//!
//! ```text
//!   let guard = engine.lock();          // std::sync::Mutex
//!   guard.flush_and_wait_uploads();
//!   std::mem::forget(guard);            // hold it through process exit
//! ```
//!
//! That does stop post-seal appends — by parking every appender on a mutex that
//! is never released. Every append path in this crate runs **inside an async
//! task** and takes that mutex *blocking* (`serve_ingest`'s committer, and the
//! claim/WAL readers), so each parked appender burns a whole tokio worker
//! thread. With a backlog in flight at SIGTERM there are more parked appenders
//! than worker threads, so the runtime is starved and the shutdown future that
//! was going to seal the *second* host is never polled again — not even its own
//! `tokio::time::timeout` fires. Prod's reopened events log came back 390
//! records below its persisted cursor.
//!
//! [`FeedWriter::seal_and_close`] replaces the hold with a flag: appends after
//! the seal fail immediately, so the "nothing is acked after the seal" contract
//! is kept **without** owning a thread.
//!
//! [`a_sealed_writer_does_not_starve_the_runtime`] is the load-holding
//! regression, and [`negative_control_the_leaked_guard_starves_the_runtime`]
//! reproduces the original defect with the old `mem::forget` shape so the
//! positive test cannot pass vacuously.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use ehdb_feed::FeedWriter;
use ehdb_l0::substrate::DurableSubstrate;
use ehdb_l0::{D1EventLog, EventRecord, L0Config, L0Engine, LocalFsSubstrate};

fn unique_dir(tag: &str) -> std::path::PathBuf {
    static N: AtomicU64 = AtomicU64::new(0);
    let n = N.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir().join(format!(
        "ehdb-feed-seal-close-{tag}-{}-{n}",
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

fn open_writer(dir: &std::path::Path) -> Arc<FeedWriter<D1EventLog>> {
    std::fs::create_dir_all(dir).unwrap();
    let store: Arc<dyn DurableSubstrate> = Arc::new(LocalFsSubstrate::new(dir).unwrap());
    let engine =
        L0Engine::<D1EventLog>::open(L0Config::d1(dir).with_shard_count(1), store).unwrap();
    Arc::new(FeedWriter::new(engine))
}

/// Can `rt` still run a spawned task?
///
/// Observed from **outside** the runtime, over a `std` channel with a `std`
/// timeout, because a starved runtime cannot be trusted to time its own
/// starvation. That is not a theoretical nicety: in a multi-threaded runtime the
/// worker threads also drive the timer, so worker threads blocked in `lock()`
/// stop `tokio::time::timeout` from ever firing — including one wrapped around
/// the shutdown sequence from `block_on`. That is precisely why the worker's
/// `tokio::time::timeout(15s, worker.shutdown())` never logged its expiry in
/// prod: the budget could not fire because the thing it was budgeting had eaten
/// the clock.
fn schedules_a_task(rt: &tokio::runtime::Runtime, within: Duration) -> bool {
    let (tx, rx) = std::sync::mpsc::channel::<()>();
    rt.spawn(async move {
        let _ = tx.send(());
    });
    rx.recv_timeout(within).is_ok()
}

/// Build the two-worker runtime both halves of the experiment share. Two is
/// enough to make the starvation deterministic rather than load-dependent, and
/// it matches the prod worker's CPU limit.
fn two_worker_runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap()
}

/// Saturate the runtime with appenders looping on `writer` from async tasks —
/// exactly the shape of `serve_ingest`'s committer, which calls the blocking
/// `append_batch` from inside a spawned task. More appenders than worker
/// threads, so every worker is certain to be running one.
fn hold_a_backlog_in_flight(writer: &Arc<FeedWriter<D1EventLog>>) {
    for w in 0..16u64 {
        let writer = Arc::clone(writer);
        tokio::spawn(async move {
            let mut id = w * 10_000;
            loop {
                id += 1;
                if writer.append(ev(id)).is_err() {
                    return; // sealed — the correct way for an appender to stop
                }
                tokio::task::yield_now().await;
            }
        });
    }
}

/// The number of records a reopened log holds — what a resuming consumer's
/// cursor is compared against.
fn reopened_tip(dir: &std::path::Path) -> u64 {
    let store: Arc<dyn DurableSubstrate> = Arc::new(LocalFsSubstrate::new(dir).unwrap());
    let engine =
        L0Engine::<D1EventLog>::open(L0Config::d1(dir).with_shard_count(1), store).unwrap();
    engine.global_sequence()
}

/// An append after the seal fails rather than landing in a fresh active part the
/// next incarnation would never see — the acked-then-lost hole.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_append_after_the_seal_is_refused_not_lost() {
    let dir = unique_dir("refuse");
    let writer = open_writer(&dir);
    for id in 1..=8 {
        writer.append(ev(id)).unwrap();
    }
    let sealed_at = writer.engine().lock().unwrap().global_sequence();

    writer.seal_and_close().unwrap();
    assert!(writer.is_closed());

    let err = writer
        .append(ev(9))
        .expect_err("a post-seal append must be refused, never silently accepted");
    assert!(err.to_string().contains("sealed"), "unexpected: {err}");
    let err = writer
        .append_batch(vec![ev(10), ev(11)])
        .expect_err("a post-seal batch must be refused too");
    assert!(err.to_string().contains("sealed"), "unexpected: {err}");

    drop(writer);
    assert_eq!(
        reopened_tip(&dir),
        sealed_at,
        "the reopened log must hold exactly what was acked before the seal — no \
         more (a refused append must not have landed) and no less (the seal must \
         have flushed everything acked)"
    );
}

/// **The load-holding regression.** Hold a real backlog in flight, seal
/// underneath it, and require the runtime to keep scheduling — which is exactly
/// what the leaked guard destroyed, and therefore exactly what stopped the
/// worker from going on to seal its second host.
///
/// The negative control below is byte-for-byte this test with one line changed:
/// `seal_and_close()` becomes `lock()` + `flush` + `mem::forget(guard)`.
#[test]
fn a_sealed_writer_does_not_starve_the_runtime() {
    let rt = two_worker_runtime();
    let dir = unique_dir("no-starve");
    let writer = rt.block_on(async {
        let writer = open_writer(&dir);
        hold_a_backlog_in_flight(&writer);
        // Let a real backlog build before the seal.
        tokio::time::sleep(Duration::from_millis(150)).await;
        writer
    });

    // THE ONE LINE THE EXPERIMENT TURNS ON. Sealed from a plain thread, as the
    // worker's shutdown does from its blocking pool.
    let w = Arc::clone(&writer);
    std::thread::spawn(move || w.seal_and_close())
        .join()
        .unwrap()
        .unwrap();

    assert!(
        schedules_a_task(&rt, Duration::from_secs(5)),
        "the runtime must still schedule work after a seal — a seal that owns \
         every worker thread is the noetl/ai-meta#226 defect, and the task that \
         could not be scheduled in prod was the events host's seal"
    );

    // The log must reopen at exactly what was acked: nothing below the tip.
    let sealed_at = writer.engine().lock().unwrap().global_sequence();
    drop(writer);
    rt.shutdown_timeout(Duration::from_secs(5));
    assert_eq!(
        reopened_tip(&dir),
        sealed_at,
        "the reopened log must come back at exactly its sealed tip — a log below \
         its own tip is what forces the resume clamp"
    );
}

/// **Negative control.** The pre-#226 seal — `lock()` + `flush` +
/// `mem::forget(guard)` — against the same harness: the appenders park forever
/// and the runtime stops scheduling anything on its worker threads. If this ever
/// stops reproducing, the harness has stopped exercising the defect and the test
/// above proves nothing.
///
/// Deliberately **not** a `#[tokio::test]`. The point is to leave worker threads
/// blocked inside `lock()` — not at an await point, so nothing can cancel or
/// abort them — and dropping a runtime waits for its workers. This builds its
/// own runtime and leaks it, keeping the wedge contained to this test rather
/// than hanging the binary. That the wedge is *unrecoverable* once entered is
/// why #226 lost data rather than merely running slowly.
#[test]
fn negative_control_the_leaked_guard_starves_the_runtime() {
    let rt = two_worker_runtime();
    let dir = unique_dir("starve");
    let writer = rt.block_on(async {
        let writer = open_writer(&dir);
        hold_a_backlog_in_flight(&writer);
        tokio::time::sleep(Duration::from_millis(150)).await;
        writer
    });

    // THE ONE LINE. The pre-#226 seal: take the engine lock and hold it forever.
    let engine = writer.engine();
    std::thread::spawn(move || {
        let mut guard = engine.lock().unwrap();
        guard.flush_and_wait_uploads().unwrap();
        std::mem::forget(guard);
    })
    .join()
    .unwrap();

    assert!(
        !schedules_a_task(&rt, Duration::from_secs(3)),
        "the leaked guard must still starve the runtime — this is the defect the \
         positive test is measured against"
    );

    // Never drop it: the worker threads are blocked in `lock()` forever.
    std::mem::forget(rt);
    std::mem::forget(writer);
}
