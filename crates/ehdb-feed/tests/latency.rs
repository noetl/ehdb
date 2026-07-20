//! L1 T0 latency + parity harness — the go/no-go measurement.
//!
//! Drives a [`FeedWriter`] over an L0 engine and a networked [`FeedSubscription`]
//! on loopback in a **closed-loop, one-in-flight** measurement (append the next
//! record only after the previous one lands), so the reported **append→subscriber
//! bus latency** has no timer-pacing granularity and no queue backlog. It also
//! breaks the number into per-hop components — hop A (in-process feed read) and
//! hop B (raw loopback transport RTT) — and reports the posture-A fsync-per-append
//! **durability** cost *separately* (it is the durable-log write cost, not the
//! bus; NATS-JetStream shares it, it is sub-ms on NVMe, and group-commit amortizes
//! it). Asserts **parity**: every record delivered exactly once, in sort-key order
//! (0 missed, 0 spurious) — the shadow-feed correctness bar vs NATS.
//!
//! Run `cargo test -p ehdb-feed -- --nocapture` to see the reported numbers; the
//! assertion is a loose regression backstop, the real go/no-go is the reported bus
//! p99 read against NATS's achievable latency.

use std::sync::Arc;
use std::time::{Duration, Instant};

use ehdb_feed::{FeedSubscription, FeedWriter};
use ehdb_l0::substrate::DurableSubstrate;
use ehdb_l0::{D1EventLog, EventRecord, FlushPolicy, L0Config, L0Engine, LocalFsSubstrate};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

fn unique_dir(tag: &str) -> std::path::PathBuf {
    static N: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let n = N.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    std::env::temp_dir().join(format!("ehdb-feed-{tag}-{}-{n}", std::process::id()))
}

fn percentile(sorted: &[u128], p: f64) -> u128 {
    if sorted.is_empty() {
        return 0;
    }
    let idx = ((sorted.len() as f64 * p).ceil() as usize).saturating_sub(1);
    sorted[idx.min(sorted.len() - 1)]
}

/// A raw loopback frame round-trip baseline — the transport-hop floor the feed's
/// end-to-end number sits on top of. One-way ≈ RTT / 2.
async fn transport_floor_rtt_us(samples: usize) -> Vec<u128> {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    // Echo server: read 4-byte-prefixed frame, write it straight back.
    tokio::spawn(async move {
        let (mut sock, _) = listener.accept().await.unwrap();
        sock.set_nodelay(true).unwrap();
        loop {
            let mut len = [0u8; 4];
            if sock.read_exact(&mut len).await.is_err() {
                break;
            }
            let n = u32::from_be_bytes(len) as usize;
            let mut buf = vec![0u8; n];
            if sock.read_exact(&mut buf).await.is_err() {
                break;
            }
            let _ = sock.write_all(&len).await;
            let _ = sock.write_all(&buf).await;
            let _ = sock.flush().await;
        }
    });
    let mut client = TcpStream::connect(addr).await.unwrap();
    client.set_nodelay(true).unwrap();
    let payload = vec![7u8; 200];
    let mut out = Vec::with_capacity(samples);
    for i in 0..samples {
        let t0 = Instant::now();
        client
            .write_all(&(payload.len() as u32).to_be_bytes())
            .await
            .unwrap();
        client.write_all(&payload).await.unwrap();
        client.flush().await.unwrap();
        let mut len = [0u8; 4];
        client.read_exact(&mut len).await.unwrap();
        let n = u32::from_be_bytes(len) as usize;
        let mut buf = vec![0u8; n];
        client.read_exact(&mut buf).await.unwrap();
        if i >= 50 {
            out.push(t0.elapsed().as_micros());
        }
    }
    out
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn append_to_subscriber_latency_and_parity() {
    const WARMUP: u64 = 200;
    const MEASURED: u64 = 2000;
    let total = WARMUP + MEASURED;

    // Buffered flush isolates the *bus* (change-feed + transport) from the
    // *durability* cost: fsync-per-append (posture A) is a separate, tunable
    // dimension NATS-JetStream shares, and it dominates on a slow filesystem, so
    // measuring it into the bus number would be measuring the disk, not the bus.
    // We measure the bus with buffered flush, and report the posture-A fsync cost
    // separately below.
    let buffered = || FlushPolicy::Buffered { fsync_every: 256 };

    // Hop A — in-process append→feed-visible (the L0 change-feed read cost, no
    // socket, no task hop, no fsync). Isolates the storage read tier.
    let mut hop_a_us: Vec<u128> = Vec::new();
    {
        let a_obj = unique_dir("aobj");
        let a_local = unique_dir("alocal");
        let a_store: Arc<dyn DurableSubstrate> = Arc::new(LocalFsSubstrate::new(&a_obj).unwrap());
        let mut eng =
            L0Engine::<D1EventLog>::open(L0Config::d1(&a_local).with_flush(buffered()), a_store)
                .unwrap();
        let mut feed = ehdb_l0::ChangeFeed::new(0, 0);
        for i in 0..1000u64 {
            let seq = i + 1;
            let t0 = Instant::now();
            eng.append(&format!("exec-{i}"), "t", "command-payload")
                .unwrap();
            let batch = feed.poll(&eng).unwrap();
            let dt = t0.elapsed().as_micros();
            assert_eq!(batch.len(), 1, "one record appended, one visible");
            if seq > 100 {
                hop_a_us.push(dt);
            }
        }
        hop_a_us.sort_unstable();
        for d in [&a_obj, &a_local] {
            let _ = std::fs::remove_dir_all(d);
        }
    }

    // The durability dimension — posture-A fsync-per-append cost, append only (no
    // read, no socket). Reported so nothing is hidden: this is the durable-log
    // write cost, NOT the bus. On this sandbox filesystem fsync is milliseconds;
    // on production NVMe it is sub-millisecond, and group-commit (fsync_every)
    // amortizes it under concurrent load.
    let mut fsync_us: Vec<u128> = Vec::new();
    {
        let f_obj = unique_dir("fobj");
        let f_local = unique_dir("flocal");
        let f_store: Arc<dyn DurableSubstrate> = Arc::new(LocalFsSubstrate::new(&f_obj).unwrap());
        let mut eng = L0Engine::<D1EventLog>::open(L0Config::d1(&f_local), f_store).unwrap();
        for i in 0..500u64 {
            let t0 = Instant::now();
            eng.append(&format!("exec-{i}"), "t", "command-payload")
                .unwrap();
            if i >= 50 {
                fsync_us.push(t0.elapsed().as_micros());
            }
        }
        fsync_us.sort_unstable();
        for d in [&f_obj, &f_local] {
            let _ = std::fs::remove_dir_all(d);
        }
    }

    let obj = unique_dir("obj");
    let local = unique_dir("local");
    let store: Arc<dyn DurableSubstrate> = Arc::new(LocalFsSubstrate::new(&obj).unwrap());
    let engine =
        L0Engine::<D1EventLog>::open(L0Config::d1(&local).with_flush(buffered()), store).unwrap();
    let writer = Arc::new(FeedWriter::new(engine));

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(ehdb_feed::serve(writer.clone(), listener));

    // Subscribe (shard 0, from the beginning — engine starts empty).
    let mut sub = FeedSubscription::connect(addr, 0, 0).await.unwrap();

    // Closed-loop, one-in-flight measurement: the receiver acks each delivered
    // record's (seq, recv-instant) back to the appender, which appends the next
    // record only after the previous one lands. This measures true unsaturated
    // append→subscriber latency — no timer-pacing granularity, no queue backlog.
    let (ack_tx, mut ack_rx) = tokio::sync::mpsc::unbounded_channel::<(u64, Instant)>();
    let recv_task = tokio::spawn(async move {
        loop {
            let batch = match tokio::time::timeout(
                Duration::from_secs(10),
                sub.recv_batch::<EventRecord>(),
            )
            .await
            {
                Ok(Ok(b)) => b,
                _ => break, // stall or writer gone
            };
            let now = Instant::now();
            for rec in batch {
                if ack_tx.send((rec.global_sequence, now)).is_err() {
                    return;
                }
            }
        }
    });

    let mut lat_us: Vec<u128> = Vec::with_capacity(MEASURED as usize);
    let mut delivered: Vec<u64> = Vec::with_capacity(total as usize);
    for i in 0..total {
        let seq = i + 1;
        let t0 = Instant::now();
        writer
            .append(EventRecord::new(
                seq,
                format!("exec-{i}"),
                "t",
                "command-payload",
            ))
            .unwrap();
        // One in flight: the next ack is this record.
        let (got, t_recv) = tokio::time::timeout(Duration::from_secs(10), ack_rx.recv())
            .await
            .expect("feed delivery stalled")
            .expect("receiver closed");
        delivered.push(got);
        if seq > WARMUP {
            lat_us.push(t_recv.duration_since(t0).as_micros());
        }
    }
    drop(writer); // let the receiver task wind down
    let _ = recv_task.await;

    // Parity: exactly `total` records, in strict sort-key order, none missed /
    // none spurious.
    assert_eq!(
        delivered.len() as u64,
        total,
        "every record delivered exactly once"
    );
    assert_eq!(
        delivered,
        (1..=total).collect::<Vec<_>>(),
        "in sort-key order, 0 missed / 0 spurious"
    );

    lat_us.sort_unstable();

    let mut floor = transport_floor_rtt_us(400).await;
    floor.sort_unstable();

    eprintln!("\n=== L1 T0 shadow-feed latency (loopback; buffered flush) ===");
    eprintln!("  BUS: append -> networked subscriber (the append->worker number)");
    eprintln!("    samples        : {}", lat_us.len());
    eprintln!("    p50            : {} us", percentile(&lat_us, 0.50));
    eprintln!("    p90            : {} us", percentile(&lat_us, 0.90));
    eprintln!("    p99            : {} us", percentile(&lat_us, 0.99));
    eprintln!(
        "    max            : {} us",
        lat_us.last().copied().unwrap_or(0)
    );
    eprintln!("  per-hop:");
    eprintln!(
        "    hop A feed-read (in-proc) p50/p99 : {} / {} us",
        percentile(&hop_a_us, 0.50),
        percentile(&hop_a_us, 0.99)
    );
    eprintln!(
        "    hop B transport  (loopback RTT/2) p50 : {} us",
        percentile(&floor, 0.50) / 2
    );
    eprintln!("  DURABILITY (separate dimension, NOT the bus):");
    eprintln!(
        "    posture-A fsync-per-append p50/p99 : {} / {} us  (this sandbox FS; sub-ms on NVMe; group-commit amortizes)",
        percentile(&fsync_us, 0.50),
        percentile(&fsync_us, 0.99)
    );
    eprintln!("============================================================\n");

    // Loose regression backstop only — the real go/no-go is the reported bus p99
    // read against NATS. Generous so a loaded CI runner doesn't flake.
    let p99 = percentile(&lat_us, 0.99);
    assert!(
        p99 < 50_000,
        "bus p99 {p99} us is implausibly high — investigate before proceeding"
    );

    for d in [&obj, &local] {
        let _ = std::fs::remove_dir_all(d);
    }
}
