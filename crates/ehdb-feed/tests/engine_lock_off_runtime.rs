//! noetl/ai-meta#501 — no async face may park a Tokio worker on the engine lock.
//!
//! The engine sits behind a `std::sync::Mutex`. Every async face of the writer
//! used to take it inline — `engine.read()` / `append_batch` on a runtime worker
//! — so for as long as *anything else* held it, each waiting face pinned one
//! worker. The command-bus writer runs a few workers on a 2-CPU quota; on prod
//! (v6.10.0, 2026-10-26) a startup scan holding the lock in 20k-record chunks
//! was enough that the writer answered only 12/25 1 Hz probes on :9090.
//!
//! Every test here runs on that shape — a multi-thread runtime with TWO workers
//! — holds the engine lock from a plain thread for seconds, drives one face into
//! it, and asks a rendezvous probe whether both workers can still run. Each
//! fails against the revert of its own face's fix:
//!
//! | test                          | face                                   |
//! |-------------------------------|----------------------------------------|
//! | `claim_*`                     | `ClaimCoordinator::claim_next` (:9101) |
//! | `command_lag_*`               | `lag` / `subject_lags` (2 s sampler)   |
//! | `feed_push_loop_*`            | `serve` → `push_loop` (:9108)          |
//! | `group_claim_*`, `group_lags*`| `GroupCoordinator` (:9104, :9106)      |
//! | `ingest_*`                    | `serve_ingest` (:9100 / :9105)         |
//! | `sse_*`                       | `serve_sse`                            |
//! | `http_probe_*`                | all of the above at once, vs. a probe  |
//! | `lag_count_holds_*`           | the sampler's read is bounded per hold |

use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{mpsc, Arc};
use std::time::{Duration, Instant};

use ehdb_feed::scaler::{bind_and_serve_snapshot, LagSnapshot};
use ehdb_feed::{
    d1_command_subject, serve_claims, serve_group_claims, serve_ingest, ClaimClient,
    ClaimCoordinator, CursorFallback, FeedSubscription, FeedWriter, GroupClaimClient,
    GroupCoordinator, PublishClient,
};
use ehdb_l0::substrate::DurableSubstrate;
use ehdb_l0::{D1EventLog, EventRecord, L0Config, L0Engine, LocalFsSubstrate};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

type Writer = Arc<FeedWriter<D1EventLog>>;

/// How long the test holder keeps the engine lock.
const HOLD: Duration = Duration::from_millis(2_500);

fn unique_dir(tag: &str) -> std::path::PathBuf {
    static N: AtomicUsize = AtomicUsize::new(0);
    let n = N.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir().join(format!("ehdb-feed-501-{tag}-{}-{n}", std::process::id()))
}

fn cmd(seq: u64, pool: &str) -> EventRecord {
    EventRecord::new(
        seq,
        format!("exec-{seq}"),
        "command.issued",
        format!("{{\"execution_pool\":\"{pool}\"}}"),
    )
}

/// A writer over a fresh log holding `n` commands for pool `p`.
fn writer_with(tag: &str, n: u64) -> Writer {
    let dir = unique_dir(tag);
    std::fs::create_dir_all(&dir).unwrap();
    let store: Arc<dyn DurableSubstrate> = Arc::new(LocalFsSubstrate::new(&dir).unwrap());
    let engine =
        L0Engine::<D1EventLog>::open(L0Config::d1(&dir).with_shard_count(1), store).unwrap();
    let w = Arc::new(FeedWriter::new(engine));
    let mut batch = Vec::new();
    for i in 1..=n {
        batch.push(cmd(i, "p"));
        if batch.len() == 5_000 || i == n {
            w.append_batch(std::mem::take(&mut batch)).unwrap();
        }
    }
    w
}

/// Take the engine lock on a plain thread and keep it for `hold`. Returns once
/// the lock is actually held.
fn hold_engine(w: &Writer, hold: Duration) -> std::thread::JoinHandle<()> {
    let engine = w.engine();
    let (tx, rx) = mpsc::channel();
    let t = std::thread::spawn(move || {
        let _g = engine.read();
        tx.send(()).unwrap();
        std::thread::sleep(hold);
    });
    rx.recv().unwrap();
    t
}

/// Can BOTH runtime workers run a task at the same time, right now?
///
/// Two tasks rendezvous: each checks in, then spins without yielding until the
/// other has. A worker parked on the engine `std` Mutex cannot pick up the
/// second task, and the first times out.
async fn both_workers_free(timeout: Duration) -> bool {
    let arrived = Arc::new(AtomicUsize::new(0));
    let spawn = |arrived: Arc<AtomicUsize>| {
        tokio::spawn(async move {
            arrived.fetch_add(1, Ordering::SeqCst);
            let t = Instant::now();
            while arrived.load(Ordering::SeqCst) < 2 {
                if t.elapsed() > timeout {
                    return false;
                }
                std::hint::spin_loop();
            }
            true
        })
    };
    let a = spawn(arrived.clone());
    let b = spawn(arrived.clone());
    a.await.unwrap_or(false) & b.await.unwrap_or(false)
}

/// Probe the runtime until the holder lets go; `(probes, held)`.
async fn probe_while(holder: &std::thread::JoinHandle<()>) -> (u32, u32) {
    let (mut probes, mut held) = (0, 0);
    while !holder.is_finished() {
        if !both_workers_free(Duration::from_millis(400)).await {
            held += 1;
        }
        probes += 1;
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    (probes, held)
}

/// The shared body: hold the lock, let `face` run into it, probe, release, and
/// require the face to finish afterwards (a face that simply never ran would
/// also leave the workers free).
async fn assert_face_parks_no_worker<T: Send + 'static>(
    what: &str,
    w: &Writer,
    face: impl std::future::Future<Output = T> + Send + 'static,
) -> T {
    let holder = hold_engine(w, HOLD);
    let task = tokio::spawn(face);
    // Let the face reach the lock.
    tokio::time::sleep(Duration::from_millis(150)).await;
    let (probes, held) = probe_while(&holder).await;
    holder.join().unwrap();
    let out = tokio::time::timeout(Duration::from_secs(10), task)
        .await
        .unwrap_or_else(|_| panic!("{what}: never finished once the lock was released"))
        .unwrap();
    eprintln!("  {what}: {probes} runtime probes during a {HOLD:?} hold, {held} held");
    assert!(
        probes >= 3,
        "{what}: too few probes ({probes}) to mean anything"
    );
    assert_eq!(
        held, 0,
        "{what}: {held}/{probes} probes found a runtime worker parked while another \
         thread held the engine lock — this face takes the std Mutex on a Tokio worker"
    );
    out
}

async fn listener() -> (TcpListener, SocketAddr) {
    let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let a = l.local_addr().unwrap();
    (l, a)
}

fn coordinator(w: &Writer) -> Arc<ClaimCoordinator<D1EventLog>> {
    Arc::new(ClaimCoordinator::new(
        Arc::clone(w),
        0,
        Duration::from_secs(30),
        0,
        d1_command_subject(1),
    ))
}

fn groups(w: &Writer) -> Arc<GroupCoordinator<D1EventLog>> {
    Arc::new(GroupCoordinator::new(
        Arc::clone(w),
        0,
        Duration::from_secs(30),
        d1_command_subject(1),
        None,
        CursorFallback::Beginning,
    ))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn claim_waits_for_a_held_engine_lock_off_the_runtime() {
    let w = writer_with("claim", 3);
    let c = coordinator(&w);
    let d = assert_face_parks_no_worker("claim_next", &w, async move {
        c.claim_next("commands.p.>", 1).await
    })
    .await;
    assert_eq!(d.sort_key, 1, "the first command is still the one claimed");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn command_lag_and_subject_lags_wait_off_the_runtime() {
    let w = writer_with("lag", 7);
    let c = coordinator(&w);
    let c2 = Arc::clone(&c);
    let lag = assert_face_parks_no_worker("lag", &w, async move { c2.lag().await }).await;
    assert_eq!(lag, 7);
    let split =
        assert_face_parks_no_worker("subject_lags", &w, async move { c.subject_lags().await })
            .await;
    assert_eq!(split.len(), 1, "{split:?}");
    assert_eq!(split[0].subject, "commands.p.shard.0");
    assert_eq!(split[0].lag, 7);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn feed_push_loop_waits_off_the_runtime() {
    let w = writer_with("feed", 4);
    let (l, addr) = listener().await;
    tokio::spawn(ehdb_feed::serve(Arc::clone(&w), l));
    let got = assert_face_parks_no_worker("push_loop", &w, async move {
        let mut sub = FeedSubscription::connect(addr, 0, 0).await.unwrap();
        sub.recv_batch::<EventRecord>().await.unwrap()
    })
    .await;
    assert_eq!(got.len(), 4);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn group_claim_waits_off_the_runtime() {
    let w = writer_with("gclaim", 2);
    let g = groups(&w);
    // Opened BEFORE the hold, so the claim loop's own poll is what meets it.
    g.open_group("open").await;
    let (l, addr) = listener().await;
    tokio::spawn(serve_group_claims(l, g));
    let got = assert_face_parks_no_worker("group claim_next", &w, async move {
        let mut c = GroupClaimClient::connect(addr, "open", 1, "commands.>")
            .await
            .unwrap();
        c.claim_next::<EventRecord>().await.unwrap()
    })
    .await;
    assert_eq!(got.sort_key, 1);
    // Opened INSIDE the hold: the tip read that clamps a new group's cursor.
    let got = assert_face_parks_no_worker("group open (tip read)", &w, async move {
        let mut c = GroupClaimClient::connect(addr, "fresh", 2, "commands.>")
            .await
            .unwrap();
        c.claim_next::<EventRecord>().await.unwrap()
    })
    .await;
    assert_eq!(got.sort_key, 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn group_lags_wait_off_the_runtime() {
    let w = writer_with("glags", 5);
    let g = groups(&w);
    g.open_group("g").await;
    let lags =
        assert_face_parks_no_worker("group_lags", &w, async move { g.group_lags().await }).await;
    assert_eq!(lags, vec![("g".to_string(), 0, 5)]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ingest_append_waits_off_the_runtime() {
    let w = writer_with("ingest", 1);
    let (l, addr) = listener().await;
    tokio::spawn(serve_ingest(l, Arc::clone(&w)));
    let mut client = PublishClient::connect(addr).await.unwrap();
    let seq = assert_face_parks_no_worker("serve_ingest", &w, async move {
        client.publish(&cmd(2, "p")).await.unwrap()
    })
    .await;
    assert!(
        seq > 1,
        "the publish was appended after the hold (seq {seq})"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sse_stream_waits_off_the_runtime() {
    let w = writer_with("sse", 2);
    let (l, addr) = listener().await;
    tokio::spawn(ehdb_feed::sse::serve_sse(l, Arc::clone(&w)));
    let body = assert_face_parks_no_worker("serve_sse", &w, async move {
        let mut s = TcpStream::connect(addr).await.unwrap();
        s.write_all(b"GET /feed?shard=0&cursor=0 HTTP/1.1\r\nHost: x\r\n\r\n")
            .await
            .unwrap();
        let mut out = Vec::new();
        let mut buf = [0u8; 4096];
        while !String::from_utf8_lossy(&out).contains("id: 2") {
            let n = s.read(&mut buf).await.unwrap();
            assert!(n > 0, "sse closed early");
            out.extend_from_slice(&buf[..n]);
        }
        String::from_utf8_lossy(&out).into_owned()
    })
    .await;
    assert!(body.contains("id: 1"), "{body}");
}

/// One HTTP GET with a hard 1 s budget, from outside the runtime — the shape of
/// the prod `wget -T1` probe.
fn http_probe(addr: SocketAddr) -> bool {
    use std::io::{Read, Write};
    let Ok(mut s) = std::net::TcpStream::connect_timeout(&addr, Duration::from_secs(1)) else {
        return false;
    };
    let _ = s.set_read_timeout(Some(Duration::from_secs(1)));
    let _ = s.set_write_timeout(Some(Duration::from_secs(1)));
    if s.write_all(b"GET /metrics HTTP/1.1\r\nHost: x\r\n\r\n")
        .is_err()
    {
        return false;
    }
    let mut buf = [0u8; 64];
    matches!(s.read(&mut buf), Ok(n) if n > 0 && buf.starts_with(b"HTTP/1.1 200"))
}

/// ⭐ The prod symptom, end to end: with EVERY face waiting on a held engine lock
/// at once — claims from several members, the lag sampler, a feed subscriber, a
/// group claimer, publishers — an unrelated HTTP face on the same 2-worker
/// runtime must keep answering inside 1 s.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn http_probe_answers_while_every_face_waits_on_the_engine_lock() {
    let w = writer_with("http", 3);
    let c = coordinator(&w);
    let g = groups(&w);
    let (cl, claim_addr) = listener().await;
    let (fl, feed_addr) = listener().await;
    let (gl, group_addr) = listener().await;
    let (il, ingest_addr) = listener().await;
    tokio::spawn(serve_claims(cl, Arc::clone(&c)));
    tokio::spawn(ehdb_feed::serve(Arc::clone(&w), fl));
    tokio::spawn(serve_group_claims(gl, g));
    tokio::spawn(serve_ingest(il, Arc::clone(&w)));
    let metrics_addr = {
        let (l, a) = listener().await;
        drop(l);
        a
    };
    tokio::spawn(bind_and_serve_snapshot(metrics_addr, || {
        LagSnapshot::shards_only(Vec::new())
    }));
    let deadline = Instant::now() + Duration::from_secs(5);
    while !http_probe(metrics_addr) {
        assert!(Instant::now() < deadline, "metrics face never came up");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    // Publishers connect before the hold; they publish during it.
    let mut pubs = Vec::new();
    for _ in 0..3 {
        pubs.push(PublishClient::connect(ingest_addr).await.unwrap());
    }

    let holder = hold_engine(&w, Duration::from_millis(3_000));
    let mut tasks: Vec<tokio::task::JoinHandle<()>> = Vec::new();
    for m in 1..=3u32 {
        tasks.push(tokio::spawn(async move {
            let mut cc = ClaimClient::connect(claim_addr, m, "commands.>")
                .await
                .unwrap();
            cc.claim_next::<EventRecord>().await.unwrap();
        }));
    }
    {
        let c = Arc::clone(&c);
        tasks.push(tokio::spawn(async move {
            c.lag().await;
            c.subject_lags().await;
        }));
    }
    tasks.push(tokio::spawn(async move {
        let mut s = FeedSubscription::connect(feed_addr, 0, 0).await.unwrap();
        s.recv_batch::<EventRecord>().await.unwrap();
    }));
    tasks.push(tokio::spawn(async move {
        let mut gc = GroupClaimClient::connect(group_addr, "g", 9, "commands.>")
            .await
            .unwrap();
        gc.claim_next::<EventRecord>().await.unwrap();
    }));
    for (i, mut p) in pubs.into_iter().enumerate() {
        tasks.push(tokio::spawn(async move {
            p.publish(&cmd(100 + i as u64, "p")).await.unwrap();
        }));
    }

    // Probe from a plain thread, at 10 Hz, for the whole hold.
    let answered = Arc::new(AtomicU64::new(0));
    let total = Arc::new(AtomicU64::new(0));
    let stop = Arc::new(AtomicBool::new(false));
    let prober = {
        let (answered, total, stop) = (answered.clone(), total.clone(), stop.clone());
        std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(200));
            while !stop.load(Ordering::SeqCst) {
                total.fetch_add(1, Ordering::SeqCst);
                if http_probe(metrics_addr) {
                    answered.fetch_add(1, Ordering::SeqCst);
                }
                std::thread::sleep(Duration::from_millis(100));
            }
        })
    };
    while !holder.is_finished() {
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    stop.store(true, Ordering::SeqCst);
    prober.join().unwrap();
    holder.join().unwrap();
    for t in tasks {
        tokio::time::timeout(Duration::from_secs(10), t)
            .await
            .expect("every face finishes once the lock is released")
            .unwrap();
    }

    let (a, n) = (
        answered.load(Ordering::SeqCst),
        total.load(Ordering::SeqCst),
    );
    eprintln!("  http probe answered {a}/{n} during a 3 s engine-lock hold");
    assert!(n >= 10, "too few probes ({n})");
    assert_eq!(
        a,
        n,
        "the HTTP face missed {} of {n} 1 s probes while the engine lock was held",
        n - a
    );
}

/// The 2 s sampler's count no longer holds the engine lock for the whole
/// backlog: another reader waits at most one bounded chunk, not the count.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lag_count_holds_the_engine_lock_one_bounded_chunk_at_a_time() {
    let n = 200_000u64;
    let w = writer_with("bounded", n);
    let c = coordinator(&w);

    // A competing reader takes the lock in a tight loop and records its worst wait.
    let stop = Arc::new(AtomicBool::new(false));
    let worst_us = Arc::new(AtomicU64::new(0));
    let reader = {
        let (stop, worst_us, engine) = (stop.clone(), worst_us.clone(), w.engine());
        std::thread::spawn(move || {
            while !stop.load(Ordering::SeqCst) {
                let t = Instant::now();
                drop(engine.read());
                worst_us.fetch_max(t.elapsed().as_micros() as u64, Ordering::SeqCst);
                std::thread::sleep(Duration::from_micros(200));
            }
        })
    };

    let t = Instant::now();
    let lag = c.lag().await;
    let split = c.subject_lags().await;
    let cost = t.elapsed();
    stop.store(true, Ordering::SeqCst);
    reader.join().unwrap();
    let worst = Duration::from_micros(worst_us.load(Ordering::SeqCst));
    eprintln!("  lag+subject_lags over {n} records: {cost:?}; worst competing wait {worst:?}");

    assert_eq!(lag, n);
    assert_eq!(split[0].lag, n);
    // ⚠ The denominator: the count must genuinely cost something.
    assert!(
        cost >= Duration::from_millis(100),
        "the count must be slow enough to measure (took {cost:?})"
    );
    assert!(
        worst * 5 < cost,
        "a competing reader waited {worst:?} during a {cost:?} count — the sampler \
         holds the engine lock for the whole backlog"
    );
}

/// A claim abandoned while its poll waits on the blocking pool (the claimer's
/// connection went away) must not strand the record it ends up assigning: the
/// next claimer gets that record at once, not after `ack_wait` (30 s here).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_abandoned_claim_gives_its_record_straight_back() {
    let w = writer_with("orphan", 2);
    let c = coordinator(&w);
    let holder = hold_engine(&w, Duration::from_millis(800));
    let abandoned = {
        let c = Arc::clone(&c);
        tokio::spawn(async move { c.claim_next("commands.p.>", 1).await })
    };
    tokio::time::sleep(Duration::from_millis(200)).await;
    abandoned.abort();
    holder.join().unwrap();

    let next = tokio::time::timeout(Duration::from_secs(5), c.claim_next("commands.p.>", 2))
        .await
        .expect("the next claim is served promptly");
    assert_eq!(
        next.sort_key, 1,
        "record 1 was assigned to an abandoned claim and left in flight for the \
         whole ack_wait; the next claimer got {} instead",
        next.sort_key
    );
    assert!(
        next.redelivered,
        "it is a redelivery of the orphaned assignment"
    );
}
