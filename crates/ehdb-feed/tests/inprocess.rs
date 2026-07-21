//! In-process consumer support — the co-located writer's `tip_receiver()`
//! blocks a consumer until new records land, then a ChangeFeed drains them
//! without a network hop (the system-pool worker consuming its own shard).

use std::sync::Arc;
use std::time::Duration;

use ehdb_l0::substrate::DurableSubstrate;
use ehdb_l0::{
    ChangeFeed, D1EventLog, EventRecord, FlushPolicy, L0Config, L0Engine, LocalFsSubstrate,
};

use ehdb_feed::FeedWriter;

fn unique_dir(tag: &str) -> std::path::PathBuf {
    static N: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let n = N.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    std::env::temp_dir().join(format!("ehdb-feed-ip-{tag}-{}-{n}", std::process::id()))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn tip_receiver_wakes_an_in_process_consumer() {
    let (obj, local) = (unique_dir("obj"), unique_dir("local"));
    let store: Arc<dyn DurableSubstrate> = Arc::new(LocalFsSubstrate::new(&obj).unwrap());
    let engine = L0Engine::<D1EventLog>::open(
        L0Config::d1(&local).with_flush(FlushPolicy::Buffered { fsync_every: 64 }),
        store,
    )
    .unwrap();
    let writer = Arc::new(FeedWriter::new(engine));

    let engine_arc = writer.engine();
    let mut rx = writer.tip_receiver();

    // Consumer task: block on the tip signal, then drain the feed in-process.
    let consumer = tokio::spawn(async move {
        let mut feed = ChangeFeed::new(0, 0);
        let mut got: Vec<u64> = Vec::new();
        while got.len() < 5 {
            let batch = {
                let e = engine_arc.lock().unwrap();
                feed.poll(&e).unwrap()
            };
            if batch.is_empty() {
                if rx.changed().await.is_err() {
                    break;
                }
                continue;
            }
            got.extend(batch.iter().map(|r| r.global_sequence));
        }
        got
    });

    // Producer: append after a beat so the consumer is parked on `changed()`.
    tokio::task::yield_now().await;
    for seq in 1..=5u64 {
        writer
            .append(EventRecord::new(seq, format!("exec-{seq}"), "t", "cmd"))
            .unwrap();
    }

    let got = tokio::time::timeout(Duration::from_secs(10), consumer)
        .await
        .expect("consumer did not wake")
        .unwrap();
    assert_eq!(
        got,
        (1..=5).collect::<Vec<_>>(),
        "in-process consumer drained all records"
    );

    for d in [&obj, &local] {
        let _ = std::fs::remove_dir_all(d);
    }
}
