//! L1 T4 proof — the networked publish path: server→writer publish RPC, the
//! end-to-end server→writer→subscriber path over sockets, and shard routing.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use ehdb_feed::{serve_ingest, FeedSubscription, FeedWriter, PublishClient, PublishRouter};
use ehdb_l0::substrate::DurableSubstrate;
use ehdb_l0::{D1EventLog, EventRecord, FlushPolicy, L0Config, L0Engine, LocalFsSubstrate};
use tokio::net::TcpListener;

fn unique_dir(tag: &str) -> std::path::PathBuf {
    static N: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let n = N.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    std::env::temp_dir().join(format!("ehdb-feed-t4-{tag}-{}-{n}", std::process::id()))
}

fn ev(seq: u64) -> EventRecord {
    EventRecord::new(seq, format!("exec-{seq}"), "t", "command-payload")
}

fn writer(
    dir: &std::path::Path,
    obj: &std::path::Path,
    shards: u32,
) -> Arc<FeedWriter<D1EventLog>> {
    let store: Arc<dyn DurableSubstrate> = Arc::new(LocalFsSubstrate::new(obj).unwrap());
    let engine = L0Engine::<D1EventLog>::open(
        L0Config::d1(dir)
            .with_shard_count(shards)
            .with_flush(FlushPolicy::Buffered { fsync_every: 64 }),
        store,
    )
    .unwrap();
    Arc::new(FeedWriter::new(engine))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn publish_rpc_returns_the_assigned_sort_key() {
    let (obj, local) = (unique_dir("obj"), unique_dir("local"));
    let w = writer(&local, &obj, 1);

    let ingest = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = ingest.local_addr().unwrap();
    tokio::spawn(serve_ingest(ingest, w.clone()));

    let mut pubc = PublishClient::connect(addr).await.unwrap();
    for seq in 1..=5u64 {
        let assigned = pubc.publish(&ev(seq)).await.unwrap();
        assert_eq!(
            assigned, seq,
            "writer returns the record's assigned sort key"
        );
    }
    // The publishes are durable in the writer's log.
    assert_eq!(w.engine().lock().unwrap().global_sequence(), 5);

    for d in [&obj, &local] {
        let _ = std::fs::remove_dir_all(d);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn server_to_writer_to_subscriber_end_to_end() {
    let (obj, local) = (unique_dir("obj"), unique_dir("local"));
    let w = writer(&local, &obj, 1);

    // The writer exposes BOTH an ingest port (publish in) and a delivery port
    // (feed out) — the co-located per-shard writer's two faces.
    let ingest = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let ingest_addr = ingest.local_addr().unwrap();
    tokio::spawn(serve_ingest(ingest, w.clone()));

    let delivery = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let delivery_addr = delivery.local_addr().unwrap();
    tokio::spawn(ehdb_feed::serve(w.clone(), delivery));

    // Worker subscribes to the delivery feed.
    let mut sub = FeedSubscription::connect(delivery_addr, 0, 0)
        .await
        .unwrap();

    // Stateless "server" publishes commands to the writer's ingest port.
    let mut server = PublishClient::connect(ingest_addr).await.unwrap();
    for seq in 1..=6u64 {
        server.publish(&ev(seq)).await.unwrap();
    }

    // The worker receives every published command over the EHDB bus, in order.
    let mut got: Vec<u64> = Vec::new();
    while got.len() < 6 {
        let batch = tokio::time::timeout(Duration::from_secs(10), sub.recv_batch::<EventRecord>())
            .await
            .expect("delivery stalled")
            .expect("decode");
        got.extend(batch.iter().map(|r| r.global_sequence));
    }
    assert_eq!(
        got,
        (1..=6).collect::<Vec<_>>(),
        "server→writer→worker over EHDB, in order"
    );

    for d in [&obj, &local] {
        let _ = std::fs::remove_dir_all(d);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn router_publishes_to_the_owning_shard() {
    // Two shard writers (shard 0 and shard 1), each its own engine + ingest port.
    let dirs: Vec<_> = (0..2)
        .map(|s| {
            (
                unique_dir(&format!("obj{s}")),
                unique_dir(&format!("local{s}")),
            )
        })
        .collect();
    let shard_count = 2u32;

    let mut writers = Vec::new();
    let mut addrs: BTreeMap<u32, std::net::SocketAddr> = BTreeMap::new();
    for (shard, (obj, local)) in dirs.iter().enumerate() {
        let w = writer(local, obj, shard_count);
        let ingest = TcpListener::bind("127.0.0.1:0").await.unwrap();
        addrs.insert(shard as u32, ingest.local_addr().unwrap());
        tokio::spawn(serve_ingest(ingest, w.clone()));
        writers.push(w);
    }

    let mut router = PublishRouter::<D1EventLog>::connect(shard_count, addrs)
        .await
        .unwrap();

    // Publish records for many executions; each routes to the writer that owns
    // its shard. Track the expected per-shard set.
    let mut want: BTreeMap<u32, u64> = BTreeMap::new();
    for seq in 1..=20u64 {
        let rec = ev(seq);
        let shard = router.shard_of(&rec);
        router.publish(&rec).await.unwrap();
        *want.entry(shard).or_default() += 1;
    }
    assert!(want.len() == 2, "records spread across both shards");

    // Each writer holds exactly the records routed to its shard (exact count via
    // a feed read of that shard).
    for (shard, w) in writers.iter().enumerate() {
        let n = w
            .engine()
            .lock()
            .unwrap()
            .read_partition_after(shard as u32, 0)
            .unwrap()
            .len() as u64;
        assert_eq!(
            n,
            want[&(shard as u32)],
            "shard {shard} writer holds its routed records"
        );
    }

    for (obj, local) in &dirs {
        for d in [obj, local] {
            let _ = std::fs::remove_dir_all(d);
        }
    }
}
