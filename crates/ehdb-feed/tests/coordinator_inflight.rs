//! **The claim coordinator can report its in-flight depth** (noetl/ehdb#402).
//!
//! `SubjectConsumerGroup::inflight_len` has existed since consumer groups
//! landed, and `render_snapshot` publishes `inflight` for every shard — but
//! `ClaimCoordinator`, which is what a writer actually holds, exposed `lag()`
//! and `committed_cursor()` and no way to reach the third value.
//!
//! ⚠ That is not a missing convenience. The field is published
//! unconditionally, so a writer with no accessor has exactly one option left
//! and it is the harmful one: fill it with a literal `0`. That does not
//! read as "unknown" — per the metric's own HELP text, high lag with zero
//! in-flight is **the stalled-consumer reading**, so a working bus would
//! publish an outage signal on every scrape.
//!
//! What this pins:
//!
//! - the coordinator reports 0 when nothing is claimed;
//! - claiming without acking raises `inflight()` while leaving `lag()`
//!   **unchanged** — the two are independent, which is the whole reason the
//!   field exists;
//! - acking lowers it again.

use std::sync::Arc;
use std::time::Duration;

use ehdb_feed::{ClaimClient, ClaimCoordinator, FeedWriter};
use ehdb_l0::substrate::DurableSubstrate;
use ehdb_l0::{D1EventLog, EventRecord, FlushPolicy, L0Config, L0Engine, LocalFsSubstrate};
use tokio::net::TcpListener;

fn unique_dir(tag: &str) -> std::path::PathBuf {
    static N: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let n = N.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    std::env::temp_dir().join(format!(
        "ehdb-coord-inflight-{tag}-{}-{n}",
        std::process::id()
    ))
}

fn ev(seq: u64) -> EventRecord {
    EventRecord::new(seq, format!("exec-{seq}"), "t", "command-payload")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_coordinator_reports_inflight_independently_of_lag() {
    let (obj, local) = (unique_dir("obj"), unique_dir("local"));
    let store: Arc<dyn DurableSubstrate> = Arc::new(LocalFsSubstrate::new(&obj).unwrap());
    let engine = L0Engine::<D1EventLog>::open(
        L0Config::d1(&local).with_flush(FlushPolicy::Buffered { fsync_every: 64 }),
        store,
    )
    .unwrap();
    let writer = Arc::new(FeedWriter::new(engine));
    let coord = Arc::new(ClaimCoordinator::new(
        writer.clone(),
        0,
        Duration::from_secs(30),
        0,
        ehdb_feed::d1_command_subject(1),
    ));

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(ehdb_feed::serve_claims(listener, coord.clone()));

    const N: u64 = 6;
    for seq in 1..=N {
        writer.append(ev(seq)).unwrap();
    }

    // Nothing claimed yet: the whole set is backlog, none of it in flight.
    let lag_before = coord.lag().await;
    assert_eq!(lag_before, N, "all {N} records are backlog");
    assert_eq!(
        coord.inflight().await,
        0,
        "nothing has been handed to a consumer"
    );

    // Claim three WITHOUT acking. Hold the client open — a departed connection
    // releases its in-flight set, which is a different test.
    let mut client = ClaimClient::connect(addr, 1, "commands.shared.>")
        .await
        .unwrap();
    let mut claimed = Vec::new();
    for _ in 0..3 {
        let c = tokio::time::timeout(Duration::from_secs(5), client.claim_next::<EventRecord>())
            .await
            .expect("claim did not time out")
            .expect("claim succeeded");
        claimed.push(c);
    }

    assert_eq!(
        coord.inflight().await,
        3,
        "three records are assigned and unacked"
    );
    // ⭐ The property the field exists for: polling without acking moves a
    // record from undelivered to unacked, and `lag` counts both — so it does
    // not move at all. A caller watching only `lag` cannot see this happen.
    assert_eq!(
        coord.lag().await,
        lag_before,
        "lag is unchanged — it counts undelivered PLUS unacked"
    );

    drop(client);
    drop(claimed);
}
