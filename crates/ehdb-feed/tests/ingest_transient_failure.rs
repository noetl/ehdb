//! A **transient** append failure: counted, and the face recovers.
//!
//! ⚠ Its own test binary, deliberately. The fault seam (`ehdb_l0::fault`) is
//! process-global and `cargo test` does not serialise tests within a binary, so
//! an arming test beside non-arming ones makes the non-arming ones fail
//! intermittently — which is exactly what happened when this lived in
//! `ingest_failure_observable.rs`: it passed under `--test-threads=1` and failed
//! in the plain workspace run. One arming test per binary is the rule.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use ehdb_feed::{serve_ingest, FeedWriter, PublishClient};
use ehdb_l0::substrate::DurableSubstrate;
use ehdb_l0::{D1EventLog, EventRecord, L0Config, L0Engine, LocalFsSubstrate};

fn unique_dir(tag: &str) -> std::path::PathBuf {
    static N: AtomicU64 = AtomicU64::new(0);
    let n = N.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir().join(format!("ehdb-transient-{tag}-{}-{n}", std::process::id()))
}

fn open_writer(tag: &str) -> Arc<FeedWriter<D1EventLog>> {
    let dir = unique_dir(tag);
    let store: Arc<dyn DurableSubstrate> =
        Arc::new(LocalFsSubstrate::new(dir.join("obj")).unwrap());
    let engine = L0Engine::<D1EventLog>::open(L0Config::d1(dir.join("local")), store).unwrap();
    Arc::new(FeedWriter::new(engine))
}

async fn bind_ingest(w: &Arc<FeedWriter<D1EventLog>>) -> std::net::SocketAddr {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let w2 = Arc::clone(w);
    tokio::spawn(async move {
        let _ = serve_ingest(listener, w2).await;
    });
    addr
}

fn ev(seq: u64) -> EventRecord {
    EventRecord::new(seq, format!("exec-{seq}"), "t", "payload")
}

/// ⭐ A TRANSIENT append failure is counted, and the face RECOVERS.
///
/// The `an_append_failure_is_counted` test above uses a sealed writer as its
/// stand-in for a full volume, and its own comment says so. That is the right
/// reachable proxy for "the append returns `Err`", but it cannot test what
/// happens *next*, because a sealed writer never accepts anything again.
///
/// The real incident shape is transient: `/data/cmdbus` fills, appends fail,
/// space is freed, and writes must resume. This uses the fault seam
/// (`ehdb_l0::fault`) to fail exactly one append on an otherwise **healthy, open**
/// writer, then publishes again on a fresh connection and requires it to land.
///
/// ⚠ Without this, "the counter increments" and "the writer is permanently dead"
/// are the same test result.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_transient_append_failure_is_counted_and_the_face_recovers() {
    let w = open_writer("transient");
    let addr = bind_ingest(&w).await;

    ehdb_l0::fault::fail_next_appends(1);
    let mut client = PublishClient::connect(addr).await.unwrap();
    let failed = client.publish(&ev(1)).await;
    assert!(
        failed.is_err(),
        "the injected failure must fail the publish"
    );
    assert_eq!(
        ehdb_l0::fault::pending_injected_failures(),
        0,
        "the injection must have been CONSUMED — otherwise the assertions below \
         pass for the wrong reason and the next test inherits an armed seam"
    );

    let snap = w.metrics().snapshot();
    assert!(
        snap.ingest_append_failed > 0,
        "a transient append failure must be counted exactly like a permanent one"
    );
    assert_eq!(
        snap.ingest_decode_failed, 0,
        "the record decoded fine; conflating the two sends an operator to check \
         disk space on a healthy disk, which is the confusion ehdb#345 removed"
    );

    // ⭐ The part a sealed writer cannot test: the face is still alive.
    let mut client2 = PublishClient::connect(addr)
        .await
        .expect("the ingest face must still accept connections after a failed append");
    let seq = client2.publish(&ev(2)).await.expect(
        "a publish after a transient failure must LAND — the writer was \
                 never unhealthy, only the one append was",
    );
    assert!(seq > 0);
}
