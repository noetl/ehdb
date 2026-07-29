//! **Command-bus dispatch-latency attribution harness (noetl/ai-meta#205).**
//!
//! Reproduces the deployed L1 T4 topology in one process on loopback — the same
//! component graph the prod flip runs — and breaks the `publish → claimed` wall
//! time into the hops that make it up, so the optimisation targets the measured
//! cost rather than a guess:
//!
//! ```text
//!   noetl-server                    noetl-cmdbus-writer            noetl-worker xN
//!   EhdbCommandPublisher            serve_ingest                   ClaimClient
//!   (one Mutex<PublishRouter>)  →   FeedWriter::append         →   claim_next
//!        │                             │  (engine Mutex + fsync)        │
//!        ├── A: publisher-mutex wait   ├── C: append (fsync) cost       │
//!        └── B: publish RTT ───────────┘                               │
//!                                      └── D: append → claim delivered ┘
//! ```
//!
//! - **A — publisher-mutex wait.** `EhdbCommandPublisher::publish` holds one
//!   `Mutex<PublishRouter>` across the whole request/response, so concurrent
//!   server publishes serialise behind a full network round-trip each. This is
//!   the queue NATS does not have (its client pipelines over one connection).
//! - **B — publish RTT.** Mutex acquired → writer's sort-key ack: wire + the
//!   writer's engine-lock wait + the append itself.
//! - **C — append cost.** Isolated by appending directly with no claimers:
//!   posture-A (`FlushPolicy::EveryAppend`) `sync_data()` per record, taken
//!   while holding the engine `std::sync::Mutex` on a tokio worker thread.
//! - **D — delivery.** Writer ack → the claiming member has the record: the tip
//!   signal, the coordinator's `poll_assign` (which needs the same engine lock
//!   every claimer contends for), and the claim-response wire hop.
//!
//! Each `(publishers, claimers)` shape runs in **both** publish modes back to
//! back — `before` (the mutex held across the round-trip) and `after` (the
//! pipelined router) — so the pair is measured on the same machine under the
//! same load. Read A down the publisher axis for the serialisation cost, D down
//! the claimer axis for claim-side contention, and C for the durability floor.
//!
//! ```console
//! cargo run --release -p ehdb-feed --example dispatch_bench
//! cargo run --release -p ehdb-feed --example dispatch_bench -- --records 400
//! ```
//!
//! Measurement-only: builds nothing the runtime links, asserts nothing, and
//! writes only to a temp dir it removes.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use ehdb_feed::{ClaimClient, ClaimCoordinator, FeedWriter, PublishRouter};
use ehdb_l0::substrate::DurableSubstrate;
use ehdb_l0::{D1EventLog, EventRecord, FlushPolicy, L0Config, L0Engine, LocalFsSubstrate};
use tokio::net::TcpListener;
use tokio::sync::Mutex;

/// How the control plane drives its publish connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PublishMode {
    /// **Before** — `noetl-server`'s `EhdbCommandPublisher` held one
    /// `Mutex<PublishRouter>` across the whole publish round-trip. Concurrent
    /// publishes queue behind a full RTT each, and because only one record is
    /// ever in flight the writer sees them one at a time — so every record pays
    /// its own `fsync`. Reproduced faithfully here by taking the mutex across
    /// the call.
    Serialized,
    /// **After** — the router pipelines (`&self`), so frames stream out as
    /// submitted and the writer group-commits whatever arrived together.
    Pipelined,
}

/// The control plane's publisher under either mode.
struct ServerPublisher {
    router: PublishRouter<D1EventLog>,
    serial: Mutex<()>,
    mode: PublishMode,
}

impl ServerPublisher {
    async fn connect(shard_count: u32, addrs: BTreeMap<u32, String>, mode: PublishMode) -> Self {
        Self {
            router: PublishRouter::<D1EventLog>::connect(shard_count, addrs)
                .await
                .expect("connect publish router"),
            serial: Mutex::new(()),
            mode,
        }
    }

    /// Publish one command, returning `(publisher_queue_wait, publish_rtt)`.
    async fn publish(&self, record: &EventRecord) -> (Duration, Duration) {
        let t0 = Instant::now();
        match self.mode {
            PublishMode::Serialized => {
                let _guard = self.serial.lock().await;
                let acquired = Instant::now();
                self.router.publish(record).await.expect("publish");
                (acquired - t0, acquired.elapsed())
            }
            PublishMode::Pipelined => {
                self.router.publish(record).await.expect("publish");
                (Duration::ZERO, t0.elapsed())
            }
        }
    }
}

fn percentile(sorted: &[u128], p: f64) -> u128 {
    if sorted.is_empty() {
        return 0;
    }
    let idx = ((sorted.len() as f64 * p).ceil() as usize).saturating_sub(1);
    sorted[idx.min(sorted.len() - 1)]
}

struct Stats {
    p50: u128,
    p99: u128,
    max: u128,
}

fn stats(mut samples: Vec<u128>) -> Stats {
    samples.sort_unstable();
    Stats {
        p50: percentile(&samples, 0.50),
        p99: percentile(&samples, 0.99),
        max: samples.last().copied().unwrap_or(0),
    }
}

fn ev(id: u64) -> EventRecord {
    EventRecord::new(
        id,
        format!("exec-{id}"),
        "command",
        // Shape-matched to a real command notification so subject derivation and
        // frame size are representative.
        format!(
            r#"{{"command_id":{id},"execution_id":"exec-{id}","execution_pool":"shared","kind":"task"}}"#
        ),
    )
}

/// One matrix cell: `publishers` concurrent server publishes over `claimers`
/// competing worker members, in `mode`.
struct Cell {
    publishers: usize,
    claimers: usize,
    mode: PublishMode,
}

impl Cell {
    fn label(&self) -> &'static str {
        match self.mode {
            PublishMode::Serialized => "before",
            PublishMode::Pipelined => "after",
        }
    }
}

async fn run_cell(cell: &Cell, records: usize, root: &std::path::Path) {
    let dir = root.join(format!(
        "{}-p{}-c{}",
        cell.label(),
        cell.publishers,
        cell.claimers
    ));
    std::fs::create_dir_all(&dir).expect("cell dir");
    let obj = dir.join("obj");
    std::fs::create_dir_all(&obj).expect("obj dir");

    let store: Arc<dyn DurableSubstrate> = Arc::new(LocalFsSubstrate::new(&obj).unwrap());
    let engine = L0Engine::<D1EventLog>::open(
        L0Config::d1(&dir)
            .with_shard_count(1)
            .with_flush(FlushPolicy::EveryAppend),
        store,
    )
    .expect("open engine");
    let writer = Arc::new(FeedWriter::new(engine));

    // --- hop C: the append cost alone (no claimers attached yet, no network).
    let append_samples: Vec<u128> = (0..64)
        .map(|i| {
            let t = Instant::now();
            writer.append(ev(9_000_000 + i)).expect("append");
            t.elapsed().as_micros()
        })
        .collect();
    let append = stats(append_samples);

    // --- the writer's two networked faces, exactly as `spawn_writer_host` wires
    //     them.
    let ingest = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let ingest_addr = ingest.local_addr().unwrap();
    tokio::spawn(ehdb_feed::serve_ingest(ingest, writer.clone()));

    let coordinator = Arc::new(ClaimCoordinator::new(
        writer.clone(),
        0,
        Duration::from_secs(15),
        writer.engine().lock().unwrap().global_sequence(),
        ehdb_feed::d1_command_subject(1),
    ));
    let claim = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let claim_addr = claim.local_addr().unwrap();
    tokio::spawn(ehdb_feed::serve_claims(claim, coordinator.clone()));

    // --- the claiming members. Each stamps the arrival of every record it
    //     claims into a shared slot keyed by the record's command id, so hop D
    //     is (claim arrival - publish ack) for that record.
    let arrivals: Arc<Mutex<BTreeMap<u64, Instant>>> = Arc::new(Mutex::new(BTreeMap::new()));
    let claimed_count = Arc::new(AtomicU64::new(0));
    let mut members = Vec::new();
    for m in 0..cell.claimers {
        let addr = format!("127.0.0.1:{}", claim_addr.port());
        let arrivals = arrivals.clone();
        let claimed_count = claimed_count.clone();
        members.push(tokio::spawn(async move {
            let mut client = ClaimClient::connect(addr, (m as u32) | 1, "commands.shared.>")
                .await
                .expect("claim connect");
            loop {
                let Ok(c) = client.claim_next::<EventRecord>().await else {
                    return;
                };
                let now = Instant::now();
                let id: u64 = serde_json::from_str::<serde_json::Value>(&c.record.payload)
                    .ok()
                    .and_then(|v| v.get("command_id").and_then(|x| x.as_u64()))
                    .unwrap_or(0);
                arrivals.lock().await.insert(id, now);
                claimed_count.fetch_add(1, Ordering::Relaxed);
                let _ = client.ack(c.sort_key).await;
            }
        }));
    }
    // Let every member reach its first blocking claim before publishing.
    tokio::time::sleep(Duration::from_millis(150)).await;

    // --- the publishing side: `publishers` concurrent tasks sharing the one
    //     server publisher, exactly as the server's axum handlers do.
    let publisher = Arc::new(
        ServerPublisher::connect(
            1,
            BTreeMap::from([(0u32, format!("127.0.0.1:{}", ingest_addr.port()))]),
            cell.mode,
        )
        .await,
    );

    let waits = Arc::new(Mutex::new(Vec::<u128>::new()));
    let rtts = Arc::new(Mutex::new(Vec::<u128>::new()));
    let acks = Arc::new(Mutex::new(BTreeMap::<u64, Instant>::new()));
    // When the control plane *submitted* each publish — the `issued` analog, so
    // (arrival - submitted) is the harness's `issued → claimed`.
    let submits = Arc::new(Mutex::new(BTreeMap::<u64, Instant>::new()));
    let next = Arc::new(AtomicU64::new(0));

    let t_start = Instant::now();
    let mut pubs = Vec::new();
    for _ in 0..cell.publishers {
        let publisher = publisher.clone();
        let (waits, rtts, acks, next) = (waits.clone(), rtts.clone(), acks.clone(), next.clone());
        let submits = submits.clone();
        pubs.push(tokio::spawn(async move {
            loop {
                let id = next.fetch_add(1, Ordering::Relaxed);
                if id >= records as u64 {
                    return;
                }
                let submitted = Instant::now();
                let (wait, rtt) = publisher.publish(&ev(id)).await;
                let acked = Instant::now();
                waits.lock().await.push(wait.as_micros());
                rtts.lock().await.push(rtt.as_micros());
                submits.lock().await.insert(id, submitted);
                acks.lock().await.insert(id, acked);
            }
        }));
    }
    for p in pubs {
        let _ = p.await;
    }
    let publish_wall = t_start.elapsed();

    // Drain: wait for every published record to be claimed (bounded).
    let deadline = Instant::now() + Duration::from_secs(30);
    while (claimed_count.load(Ordering::Relaxed) as usize) < records && Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    for m in members {
        m.abort();
    }

    // --- hop D per record: claim arrival - publish ack.
    let acks = acks.lock().await;
    let submits = submits.lock().await;
    let arrivals = arrivals.lock().await;
    let delivery: Vec<u128> = acks
        .iter()
        .filter_map(|(id, acked)| {
            arrivals
                .get(id)
                .map(|got| got.saturating_duration_since(*acked).as_micros())
        })
        .collect();
    // --- end to end: submit → the member holds the command (the `issued →
    //     claimed` analog).
    let e2e: Vec<u128> = submits
        .iter()
        .filter_map(|(id, sent)| {
            arrivals
                .get(id)
                .map(|got| got.saturating_duration_since(*sent).as_micros())
        })
        .collect();
    let delivered = delivery.len();

    let wait = stats(waits.lock().await.clone());
    let rtt = stats(rtts.lock().await.clone());
    let deliv = stats(delivery);
    let total = stats(e2e);

    println!(
        "{:<7} pub={:<3} claim={:<3} | A wait p50 {:>7} p99 {:>7} | B rtt p50 {:>6} p99 {:>7} | C append p50 {:>5} | D deliver p50 {:>6} p99 {:>7} | E2E p50 {:>7} p99 {:>8} | {}/{} claimed, {:>5.0} cmd/s",
        cell.label(),
        cell.publishers,
        cell.claimers,
        us(wait.p50),
        us(wait.p99),
        us(rtt.p50),
        us(rtt.p99),
        us(append.p50),
        us(deliv.p50),
        us(deliv.p99),
        us(total.p50),
        us(total.p99),
        delivered,
        records,
        records as f64 / publish_wall.as_secs_f64(),
    );
    let _ = (append.max, wait.max, rtt.max, deliv.max, total.max);
}

/// Microseconds rendered in whichever unit reads without counting digits.
fn us(v: u128) -> String {
    if v >= 10_000 {
        format!("{:.1}ms", v as f64 / 1000.0)
    } else {
        format!("{v}µs")
    }
}

#[tokio::main(flavor = "multi_thread", worker_threads = 8)]
async fn main() {
    let records: usize = std::env::args()
        .collect::<Vec<_>>()
        .windows(2)
        .find(|w| w[0] == "--records")
        .and_then(|w| w[1].parse().ok())
        .unwrap_or(300);

    let root = std::env::temp_dir().join(format!("ehdb-dispatch-bench-{}", std::process::id()));
    std::fs::create_dir_all(&root).expect("bench root");

    println!(
        "dispatch-latency attribution — {records} records/cell, single shard, loopback\n\
         A = server publisher-mutex wait   B = publish round-trip (wire + engine lock + append)\n\
         C = raw append cost (fsync posture)   D = writer ack → member holds the command\n"
    );

    // Each (publishers, claimers) shape is run in both modes, back to back, so
    // the before/after pair is measured on the same machine under the same load.
    let shapes = [(1, 3), (16, 3), (16, 8), (64, 8), (64, 24)];

    for (publishers, claimers) in shapes {
        for mode in [PublishMode::Serialized, PublishMode::Pipelined] {
            run_cell(
                &Cell {
                    publishers,
                    claimers,
                    mode,
                },
                records,
                &root,
            )
            .await;
        }
        println!();
    }

    let _ = std::fs::remove_dir_all(&root);
}
