//! noetl/ehdb#311 — a malformed connection must not kill the face.
//!
//! `ehdb_feed::serve` performed its subscribe handshake INSIDE the accept loop,
//! so a per-connection error propagated out of the whole function, ended the
//! loop and dropped the listener. One bad client killed the WAL fan-out face for
//! the remaining life of the process:
//!
//! ```text
//! $ netstat -tln | grep -c ':9108'
//! 1
//! $ printf 'GET / HTTP/1.1\r\n\r\n' | nc 127.0.0.1 9108
//! $ netstat -tln | grep -c ':9108'
//! 0
//! ```
//!
//! No panic, no restart, no log line — and the symptom surfaced in a DIFFERENT
//! component (the off-server state builder retrying `Connection refused`) while
//! the writer still reported the face up from its startup line.
//!
//! Anything that opens the port without completing the handshake does it: a port
//! scan, an HTTP probe aimed at the wrong port, a load balancer's TCP check, a
//! client that dies mid-handshake — **including any Kubernetes `tcpSocket`
//! probe**, which is why the operating rule for this cluster has been "never
//! bare-connect :9104/:9107/:9108".
//!
//! Each test here abuses the face and then requires it to still serve a GOOD
//! client. Asserting only "the port is still bound" would be weaker: the
//! listener can outlive a loop that is no longer accepting.

use std::sync::Arc;
use std::time::Duration;

use ehdb_feed::{FeedSubscription, FeedWriter};
use ehdb_l0::substrate::DurableSubstrate;
use ehdb_l0::{D1EventLog, EventRecord, L0Config, L0Engine, LocalFsSubstrate};
use tokio::io::AsyncWriteExt;
use tokio::net::{TcpListener, TcpStream};

fn unique_dir(tag: &str) -> std::path::PathBuf {
    let n = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!("ehdb311-{tag}-{n}"))
}

fn open_writer(dir: &std::path::Path) -> Arc<FeedWriter<D1EventLog>> {
    std::fs::create_dir_all(dir).unwrap();
    let store: Arc<dyn DurableSubstrate> = Arc::new(LocalFsSubstrate::new(dir).unwrap());
    let engine =
        L0Engine::<D1EventLog>::open(L0Config::d1(dir).with_shard_count(1), store).unwrap();
    Arc::new(FeedWriter::new(engine))
}

fn ev(id: u64) -> EventRecord {
    EventRecord::new(
        id,
        format!("exec-{id}"),
        format!("tx-{id}"),
        format!(r#"{{"event_type":"action_started","seq":{id}}}"#),
    )
}

async fn wal_face(writer: Arc<FeedWriter<D1EventLog>>) -> std::net::SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let _ = ehdb_feed::serve(writer, listener).await;
    });
    tokio::time::sleep(Duration::from_millis(50)).await;
    addr
}

/// A good client must still be able to subscribe and receive.  This is the
/// "face is alive" predicate the whole file rests on.
async fn face_still_serves(addr: std::net::SocketAddr, writer: &FeedWriter<D1EventLog>) -> bool {
    let Ok(mut sub) = FeedSubscription::connect(addr, 0, 0).await else {
        return false;
    };
    writer.append(ev(99)).unwrap();
    matches!(
        tokio::time::timeout(Duration::from_secs(5), sub.recv_batch::<EventRecord>()).await,
        Ok(Ok(batch)) if !batch.is_empty()
    )
}

#[tokio::test]
async fn the_exact_issue_repro_does_not_kill_the_face() {
    let writer = open_writer(&unique_dir("http"));
    let addr = wal_face(Arc::clone(&writer)).await;

    // Verbatim from the issue.
    let mut bad = TcpStream::connect(addr).await.unwrap();
    bad.write_all(b"GET / HTTP/1.1\r\n\r\n").await.unwrap();
    bad.flush().await.unwrap();
    drop(bad);
    tokio::time::sleep(Duration::from_millis(150)).await;

    assert!(
        face_still_serves(addr, &writer).await,
        "an HTTP request to the WAL face killed it — this is ehdb#311"
    );
}

/// The `tcpSocket`-probe shape: connect, send nothing, close.  Called out
/// separately because it is what a Kubernetes probe or `nc -z` does, and it
/// exercises the socket-setup path rather than the frame parser.
#[tokio::test]
async fn a_bare_connect_and_close_does_not_kill_the_face() {
    let writer = open_writer(&unique_dir("probe"));
    let addr = wal_face(Arc::clone(&writer)).await;

    for _ in 0..5 {
        let s = TcpStream::connect(addr).await.unwrap();
        drop(s);
    }
    tokio::time::sleep(Duration::from_millis(150)).await;

    assert!(
        face_still_serves(addr, &writer).await,
        "a bare connect-and-close killed the face — this is what any tcpSocket probe does"
    );
}

/// A well-framed but semantically wrong handshake: the frame reads fine and the
/// JSON parse fails.  Distinct from the two above, which fail earlier.
#[tokio::test]
async fn a_malformed_subscribe_request_does_not_kill_the_face() {
    let writer = open_writer(&unique_dir("badjson"));
    let addr = wal_face(Arc::clone(&writer)).await;

    let junk = b"{\"not\":\"a subscribe req\"}";
    let mut bad = TcpStream::connect(addr).await.unwrap();
    bad.write_all(&(junk.len() as u32).to_be_bytes())
        .await
        .unwrap();
    bad.write_all(junk).await.unwrap();
    bad.flush().await.unwrap();
    drop(bad);
    tokio::time::sleep(Duration::from_millis(150)).await;

    assert!(
        face_still_serves(addr, &writer).await,
        "a malformed SubscribeReq killed the face"
    );
}

/// The control: a good client works on a face nobody has abused. If this ever
/// fails the other three prove nothing, because `face_still_serves` would be
/// returning false for an unrelated reason.
#[tokio::test]
async fn the_liveness_predicate_itself_is_sound() {
    let writer = open_writer(&unique_dir("control"));
    let addr = wal_face(Arc::clone(&writer)).await;
    assert!(
        face_still_serves(addr, &writer).await,
        "the liveness predicate must pass on an unabused face"
    );
}
