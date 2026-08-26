//! noetl/ai-meta#297 — a peer that is alive but stuck must not park a reader forever.
//!
//! The failure this reproduces: a coordinator that accepts the connection and
//! then sends **nothing** — no data, no heartbeat. Before the fix the first
//! deadline miss set `read_deadline = None`, and every read after that awaited
//! with no timeout. TCP keepalive is answered by a live-but-stuck kernel, so
//! nothing ever fired. The claim loop went silent, the pod stayed Running 1/1
//! with green probes, and dispatch stopped.

use std::time::Duration;

use tokio::io::AsyncReadExt;
use tokio::net::TcpListener;

/// A peer that accepts, reads the request, and then goes silent forever —
/// alive at the TCP layer, stuck at the application layer.
async fn silent_peer() -> (String, tokio::task::JoinHandle<()>) {
    let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = l.local_addr().unwrap().to_string();
    let h = tokio::spawn(async move {
        if let Ok((mut sock, _)) = l.accept().await {
            // Consume whatever it sends, then never reply. Hold the socket open
            // so this is a stuck peer, not a closed one.
            let mut buf = [0u8; 1024];
            let _ = sock.read(&mut buf).await;
            std::future::pending::<()>().await;
        }
    });
    (addr, h)
}

/// The claim loop must return an error within a bounded time instead of parking.
#[tokio::test(flavor = "multi_thread")]
async fn a_silent_coordinator_surfaces_as_an_error_instead_of_parking() {
    // SAFETY: values are restored below; this test owns them for its duration.
    unsafe { std::env::set_var("EHDB_READ_HARD_CEILING_MS", "600") };

    let (addr, _h) = silent_peer().await;
    let mut c = ehdb_feed::claim::ClaimClient::connect_with_heartbeat(
        &addr,
        0,
        "commands.>".to_string(),
        Some(Duration::from_millis(200)),
    )
    .await
    .expect("connect");

    // Generous outer bound: the point is that it returns AT ALL. Pre-fix this
    // future never resolves and the test hangs until the harness kills it.
    let got = tokio::time::timeout(
        Duration::from_secs(20),
        c.claim_next::<ehdb_l0::EventRecord>(),
    )
    .await;

    unsafe { std::env::remove_var("EHDB_READ_HARD_CEILING_MS") };

    let inner = got.expect(
        "claim_next parked past the outer bound — the hard ceiling did not fire (ai-meta#297)",
    );
    let err = inner.expect_err("a silent coordinator must not look like a successful claim");
    let msg = err.to_string();
    assert!(
        msg.contains("297") || msg.to_lowercase().contains("dead") || msg.contains("silent"),
        "the error must name the stall so an operator can find it, got: {msg}"
    );
}

/// The ceiling must be armed by default. If this reads None the fix is inert.
#[test]
fn the_ceiling_is_armed_by_default() {
    let prev = std::env::var("EHDB_READ_HARD_CEILING_MS").ok();
    // SAFETY: restored below.
    unsafe { std::env::remove_var("EHDB_READ_HARD_CEILING_MS") };
    assert!(
        ehdb_feed::hard_read_ceiling().is_some(),
        "the hard read ceiling must be on by default; an unbounded park is the bug"
    );
    unsafe { std::env::set_var("EHDB_READ_HARD_CEILING_MS", "not-a-number") };
    assert_eq!(
        ehdb_feed::hard_read_ceiling(),
        Some(ehdb_feed::DEFAULT_READ_HARD_CEILING),
        "a typo must land on the armed default, never on unbounded"
    );
    unsafe { std::env::set_var("EHDB_READ_HARD_CEILING_MS", "0") };
    assert_eq!(
        ehdb_feed::hard_read_ceiling(),
        None,
        "0 is the deliberate operator opt-out"
    );
    match prev {
        Some(v) => unsafe { std::env::set_var("EHDB_READ_HARD_CEILING_MS", v) },
        None => unsafe { std::env::remove_var("EHDB_READ_HARD_CEILING_MS") },
    }
}
