//! **Group-commit + pipelined-publish proof (noetl/ai-meta#205).**
//!
//! The dispatch-latency fix changed *when* the writer `fsync`s and *how many*
//! records the control plane may have in flight. Neither may weaken what T4
//! shipped, so this asserts the three contracts the change is allowed to touch:
//!
//! 1. **Ordering** — a batch is still keyed by the serialized writer, strictly
//!    ascending, so the shard log stays ascending and no ingested record lands
//!    behind a follower's cursor (the #203 guarantee).
//! 2. **Durable ack** — a returned sort key means the record is on disk. Group
//!    commit amortises the `fsync`; it does not skip it or move it after the ack.
//! 3. **Pipelined identity** — with many publishes in flight on one connection,
//!    every caller gets back *its own* record's key (the ack FIFO cannot drift).

use std::collections::BTreeMap;
use std::sync::Arc;

use ehdb_feed::{serve_ingest, FeedWriter, PipelinedPublishClient};
use ehdb_l0::substrate::DurableSubstrate;
use ehdb_l0::{D1EventLog, EventRecord, FlushPolicy, L0Config, L0Engine, LocalFsSubstrate};
use tokio::net::TcpListener;

fn unique_dir(tag: &str) -> std::path::PathBuf {
    static N: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let n = N.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    std::env::temp_dir().join(format!("ehdb-feed-gc-{tag}-{}-{n}", std::process::id()))
}

/// A record carrying its own identity in the payload, so a publisher can prove
/// the key it got back belongs to the record it sent.
fn ev(id: u64) -> EventRecord {
    EventRecord::new(
        id,
        format!("exec-{id}"),
        "command",
        format!(r#"{{"id":{id}}}"#),
    )
}

fn writer(dir: &std::path::Path, obj: &std::path::Path) -> Arc<FeedWriter<D1EventLog>> {
    let store: Arc<dyn DurableSubstrate> = Arc::new(LocalFsSubstrate::new(obj).unwrap());
    // Opened under posture A — `FeedWriter` takes over the commit points itself,
    // which is exactly what the deployed writer host does.
    let engine = L0Engine::<D1EventLog>::open(
        L0Config::d1(dir)
            .with_shard_count(1)
            .with_flush(FlushPolicy::EveryAppend),
        store,
    )
    .unwrap();
    Arc::new(FeedWriter::new(engine))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn append_batch_keys_ascending_and_matches_one_at_a_time() {
    let (obj, local) = (unique_dir("obj"), unique_dir("local"));
    let w = writer(&local, &obj);

    // A batch whose incoming ids are deliberately out of order — the producer's
    // ids may reach the single writer in any order (#203). The writer's assigned
    // keys must still ascend.
    let batch: Vec<EventRecord> = [90u64, 12, 45, 7, 60].iter().map(|i| ev(*i)).collect();
    let keys = w.append_batch(batch).unwrap();

    assert_eq!(
        keys,
        vec![1, 2, 3, 4, 5],
        "writer assigns 1..n in batch order"
    );
    assert!(
        keys.windows(2).all(|p| p[1] > p[0]),
        "assigned keys strictly ascend across a batch: {keys:?}"
    );
    assert_eq!(
        w.engine().lock().unwrap().global_sequence(),
        5,
        "the shard tip advanced by exactly the batch size"
    );

    // A one-at-a-time append continues the same sequence — batching changes the
    // cost, not the numbering.
    assert_eq!(w.append(ev(1000)).unwrap(), 6);

    for d in [&obj, &local] {
        let _ = std::fs::remove_dir_all(d);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 3)]
async fn batched_records_are_on_disk_when_the_keys_return() {
    let (obj, local) = (unique_dir("obj"), unique_dir("local"));
    let w = writer(&local, &obj);

    let batch: Vec<EventRecord> = (1..=200u64).map(ev).collect();
    let keys = w.append_batch(batch).unwrap();
    assert_eq!(keys.len(), 200);

    // Durable ack: the bytes are readable from the shard's log the instant the
    // keys come back — group commit `fsync`s *before* returning, it does not
    // defer the sync past the acknowledgement.
    let engine = w.engine();
    let records = engine.lock().unwrap().read_partition_after(0, 0).unwrap();
    assert_eq!(
        records.len(),
        200,
        "every batched record is readable once its key is returned"
    );
    let seqs: Vec<u64> = records.iter().map(|r| r.global_sequence).collect();
    let mut sorted = seqs.clone();
    sorted.sort_unstable();
    assert_eq!(seqs, sorted, "the shard log reads back ascending");

    for d in [&obj, &local] {
        let _ = std::fs::remove_dir_all(d);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pipelined_publish_returns_each_callers_own_key() {
    let (obj, local) = (unique_dir("obj"), unique_dir("local"));
    let w = writer(&local, &obj);

    let ingest = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = ingest.local_addr().unwrap();
    tokio::spawn(serve_ingest(ingest, w.clone()));

    // Many publishes in flight at once on ONE connection — the shape the fix
    // introduces. Each must get back the key of the record it sent, so the ack
    // FIFO must not drift by even one slot.
    let client = Arc::new(PipelinedPublishClient::connect(addr).await.unwrap());
    const N: u64 = 400;
    let mut tasks = Vec::new();
    for id in 1..=N {
        let client = client.clone();
        tasks.push(tokio::spawn(async move {
            (id, client.publish(&ev(id)).await.unwrap())
        }));
    }
    let mut by_id: BTreeMap<u64, u64> = BTreeMap::new();
    for t in tasks {
        let (id, key) = t.await.unwrap();
        assert!(by_id.insert(id, key).is_none(), "id {id} acked twice");
    }

    assert_eq!(
        by_id.len() as u64,
        N,
        "every publish was acked exactly once"
    );
    let mut keys: Vec<u64> = by_id.values().copied().collect();
    keys.sort_unstable();
    keys.dedup();
    assert_eq!(
        keys.len() as u64,
        N,
        "no two publishes were handed the same sort key"
    );
    assert_eq!(
        keys,
        (1..=N).collect::<Vec<u64>>(),
        "the assigned keys are exactly 1..=N — no gaps, so no record was dropped"
    );

    // And the log itself holds all N, ascending.
    let engine = w.engine();
    let stored = engine.lock().unwrap().read_partition_after(0, 0).unwrap();
    assert_eq!(
        stored.len() as u64,
        N,
        "every published record is in the log"
    );

    // The ascending-contract canary never tripped: no append landed at or below
    // its shard's tail, which is the loss class #203 fixed and the one a batched
    // or pipelined write path could plausibly reintroduce.
    assert_eq!(
        engine
            .lock()
            .unwrap()
            .metrics()
            .snapshot()
            .out_of_order_appends,
        0,
        "no out-of-order append under concurrent pipelined publish"
    );

    for d in [&obj, &local] {
        let _ = std::fs::remove_dir_all(d);
    }
}
