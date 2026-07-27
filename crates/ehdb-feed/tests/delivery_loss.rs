//! Regression proof for noetl/ai-meta#203 — records ingested into the writer
//! feed but never delivered to a claimer (~10% loss under load, feed lag=0).
//!
//! Root cause: the command feed's producer (noetl-server) assigns each record's
//! sort key (the command's snowflake id) itself, not the writer. Under
//! concurrent publish a lower sort key can be appended to the single-writer
//! shard log *after* a higher one — violating the [`Dataset`] contract
//! (*"appended in ascending sort_key order within a partition"*). The feed
//! cursor advances to the max sort key it has read and never looks back, so the
//! late-arriving lower key is filtered out (`read_partition_after` returns only
//! `> cursor`): it is ingested (a sort key is returned to the server) but never
//! claimed, and `lag()` (also cursor-relative) never counts it. NATS is
//! unaffected because it delivers in arrival order, not id order.
//!
//! The fix makes the serialized writer assign the ordering key on append
//! (`FeedWriter::append` → `append_writer_assigned`), so the shard log is
//! ascending by construction and no ingested record can land behind the cursor.
//! Identity (`command_id` / `execution_id`) rides the payload, so these tests
//! track delivery by `command_id`, independent of the writer-assigned key.

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use ehdb_feed::{ClaimCoordinator, FeedWriter};
use ehdb_l0::substrate::DurableSubstrate;
use ehdb_l0::{D1EventLog, EventRecord, FlushPolicy, L0Config, L0Engine, LocalFsSubstrate};

fn unique_dir(tag: &str) -> std::path::PathBuf {
    static N: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let n = N.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    std::env::temp_dir().join(format!("ehdb-feed-loss-{tag}-{}-{n}", std::process::id()))
}

/// A shared-pool command notification whose server-assigned id is `id` (carried
/// in the payload as `command_id`, the delivery identity) and whose incoming
/// sort key is that same id (what noetl-server pre-assigns as the sort key).
fn cmd(id: u64) -> EventRecord {
    let payload = serde_json::json!({
        "execution_id": id,
        "command_id": format!("cmd-{id}"),
        "step": "start",
        "execution_pool": "shared",
    })
    .to_string();
    EventRecord::new(id, format!("exec-{id}"), "t", payload)
}

/// The delivery identity we track: the `command_id` from the payload.
fn command_id(rec: &EventRecord) -> String {
    serde_json::from_str::<serde_json::Value>(&rec.payload)
        .unwrap()
        .get("command_id")
        .and_then(|v| v.as_str())
        .unwrap()
        .to_string()
}

async fn writer_at(dir: &std::path::Path, obj: &std::path::Path) -> Arc<FeedWriter<D1EventLog>> {
    let store: Arc<dyn DurableSubstrate> = Arc::new(LocalFsSubstrate::new(obj).unwrap());
    let engine = L0Engine::<D1EventLog>::open(
        L0Config::d1(dir).with_flush(FlushPolicy::Buffered { fsync_every: 64 }),
        store,
    )
    .unwrap();
    Arc::new(FeedWriter::new(engine))
}

/// Deterministic core repro: a command whose id is *lower* than one already
/// ingested — published later under concurrency — must still be delivered.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn late_lower_id_command_is_still_delivered() {
    let (obj, local) = (unique_dir("obj"), unique_dir("local"));
    let writer = writer_at(&local, &obj).await;
    let coord = Arc::new(ClaimCoordinator::new(
        writer.clone(),
        0,
        Duration::from_secs(30),
        0,
        ehdb_feed::d1_command_subject(1),
    ));

    // 1. A higher-id command is published + ingested first.
    writer.append(cmd(100)).unwrap();

    // 2. A claimer takes it and acks — advancing its feed cursor past it.
    let first = tokio::time::timeout(
        Duration::from_secs(2),
        coord.claim_next("commands.shared.>", 1),
    )
    .await
    .expect("first command not delivered");
    assert_eq!(command_id(&first.record), "cmd-100");
    coord.ack(first.sort_key).await;

    // 3. A *lower*-id command (assigned earlier at the server but published later
    //    under concurrent load) is now ingested. The writer accepts it and
    //    returns a sort key — exactly the "smoking gun" from #203.
    let _sk = writer.append(cmd(50)).unwrap();

    // 4. It MUST still be claimable. Pre-fix, the feed cursor sat at 100 and a
    //    read of `> 100` never surfaced the lower key — this claim timed out.
    let late = tokio::time::timeout(
        Duration::from_secs(2),
        coord.claim_next("commands.shared.>", 1),
    )
    .await
    .expect("REGRESSION #203: ingested command cmd-50 never delivered");
    assert_eq!(command_id(&late.record), "cmd-50");
    coord.ack(late.sort_key).await;

    // 5. lag is honest: the shard is truly drained.
    assert_eq!(coord.lag().await, 0, "drained shard reports 0 lag");

    for dd in [&obj, &local] {
        let _ = std::fs::remove_dir_all(dd);
    }
}

/// Load repro: publish concurrently in an order that does not match id order
/// (the steady-state pattern that produced ~10% loss in prod). Every ingested
/// command must be claimed exactly once, and lag must reach 0 only when the
/// shard is truly drained.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_out_of_order_publish_loses_nothing() {
    let (obj, local) = (unique_dir("obj"), unique_dir("local"));
    let writer = writer_at(&local, &obj).await;
    let coord = Arc::new(ClaimCoordinator::new(
        writer.clone(),
        0,
        Duration::from_secs(30),
        0,
        ehdb_feed::d1_command_subject(1),
    ));

    const N: u64 = 200;
    // Publish adjacent pairs swapped (2,1,4,3,6,5,…) — a minimal deterministic
    // model of two racing publishers reordering by one slot — yielding between
    // the two appends so a claimer can advance its cursor in the gap (the exact
    // interleaving that dropped the lower id pre-fix). Every id 1..=N is
    // ingested exactly once.
    let w = writer.clone();
    let pubr = tokio::spawn(async move {
        let mut ingested = BTreeSet::new();
        let mut base = 1u64;
        while base <= N {
            let hi = (base + 1).min(N);
            for id in [hi, base] {
                if ingested.insert(id) {
                    w.append(cmd(id)).unwrap();
                    tokio::task::yield_now().await;
                }
            }
            base += 2;
        }
        ingested
    });

    // One claimer draining concurrently with the publisher.
    let c = coord.clone();
    let claimer = tokio::spawn(async move {
        let mut got: BTreeSet<String> = BTreeSet::new();
        // Ends on the first timeout — caught up long enough that nothing more is
        // coming.
        while let Ok(d) = tokio::time::timeout(
            Duration::from_millis(500),
            c.claim_next("commands.shared.>", 1),
        )
        .await
        {
            got.insert(command_id(&d.record));
            c.ack(d.sort_key).await;
        }
        got
    });

    let ingested = pubr.await.unwrap();
    let got = claimer.await.unwrap();

    let want: BTreeSet<String> = ingested.iter().map(|id| format!("cmd-{id}")).collect();
    let missing: Vec<&String> = want.difference(&got).collect();
    assert!(
        missing.is_empty(),
        "REGRESSION #203: {} ingested commands never delivered: {:?}",
        missing.len(),
        missing
    );
    assert_eq!(
        got, want,
        "delivered set == ingested set (0 loss, 0 phantom)"
    );
    assert_eq!(coord.lag().await, 0, "drained shard reports 0 lag");

    for dd in [&obj, &local] {
        let _ = std::fs::remove_dir_all(dd);
    }
}
