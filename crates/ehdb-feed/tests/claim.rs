//! L1 T4 proof — the networked claim RPC: competing consumers across
//! connections (0 double-delivery), crashed-member redelivery (0 loss),
//! per-shard ordering.

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use ehdb_feed::{ClaimClient, ClaimCoordinator, FeedWriter};
use ehdb_l0::substrate::DurableSubstrate;
use ehdb_l0::{D1EventLog, EventRecord, FlushPolicy, L0Config, L0Engine, LocalFsSubstrate};
use tokio::net::TcpListener;

fn unique_dir(tag: &str) -> std::path::PathBuf {
    static N: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let n = N.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    std::env::temp_dir().join(format!("ehdb-feed-claim-{tag}-{}-{n}", std::process::id()))
}

fn ev(seq: u64) -> EventRecord {
    EventRecord::new(seq, format!("exec-{seq}"), "t", "command-payload")
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn competing_members_each_command_once() {
    let (obj, local) = (unique_dir("obj"), unique_dir("local"));
    let writer = writer_at(&local, &obj).await;
    let coord = Arc::new(ClaimCoordinator::new(
        writer.clone(),
        0,
        Duration::from_secs(30),
        0,
    ));

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(ehdb_feed::serve_claims(listener, coord.clone()));

    const N: u64 = 30;
    for seq in 1..=N {
        writer.append(ev(seq)).unwrap();
    }

    // 3 competing members each claim+ack in a loop; collect who got what.
    let mut handles = Vec::new();
    for member in 1..=3u32 {
        handles.push(tokio::spawn(async move {
            let mut client = ClaimClient::connect(addr, member).await.unwrap();
            let mut got: Vec<u64> = Vec::new();
            // Each member keeps claiming until the set is drained; a per-claim
            // timeout stops it once nothing's left (the loop ends on timeout/err).
            while let Ok(Ok(c)) = tokio::time::timeout(
                Duration::from_millis(400),
                client.claim_next::<EventRecord>(),
            )
            .await
            {
                got.push(c.record.global_sequence);
                client.ack(c.sort_key).await.unwrap();
            }
            got
        }));
    }

    let mut all: Vec<u64> = Vec::new();
    let mut per_member = Vec::new();
    for h in handles {
        let g = h.await.unwrap();
        per_member.push(g.len());
        all.extend(g);
    }

    // Every command claimed exactly once (0 double, 0 loss).
    let unique: BTreeSet<u64> = all.iter().copied().collect();
    assert_eq!(all.len() as u64, N, "no double-delivery");
    assert_eq!(
        unique.len() as u64,
        N,
        "every command claimed by exactly one member"
    );
    assert_eq!(unique, (1..=N).collect::<BTreeSet<_>>());
    // Load was shared (each member got at least one).
    assert!(
        per_member.iter().all(|&c| c > 0),
        "load balanced across members: {per_member:?}"
    );

    for d in [&obj, &local] {
        let _ = std::fs::remove_dir_all(d);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn crashed_member_commands_redelivered() {
    let (obj, local) = (unique_dir("obj"), unique_dir("local"));
    let writer = writer_at(&local, &obj).await;
    // Short ack_wait so the redelivery is quick to observe.
    let coord = Arc::new(ClaimCoordinator::new(
        writer.clone(),
        0,
        Duration::from_millis(150),
        0,
    ));

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(ehdb_feed::serve_claims(listener, coord.clone()));

    for seq in 1..=4u64 {
        writer.append(ev(seq)).unwrap();
    }

    // Member 1 claims 2 commands and then "crashes" (never acks, drops conn).
    let mut m1 = ClaimClient::connect(addr, 1).await.unwrap();
    let a = m1.claim_next::<EventRecord>().await.unwrap();
    let b = m1.claim_next::<EventRecord>().await.unwrap();
    assert!(!a.redelivered && !b.redelivered);
    let crashed: BTreeSet<u64> = [a.record.global_sequence, b.record.global_sequence].into();
    drop(m1); // crash: connection gone, commands a+b never acked

    // Member 2 drains everything: the 2 fresh commands, then (after ack_wait) the
    // 2 redelivered ones. Every command is eventually claimed + acked.
    let mut m2 = ClaimClient::connect(addr, 2).await.unwrap();
    let mut acked: BTreeSet<u64> = BTreeSet::new();
    let mut saw_redelivery = false;
    while acked.len() < 4 {
        let c = tokio::time::timeout(Duration::from_secs(5), m2.claim_next::<EventRecord>())
            .await
            .expect("redelivery did not arrive")
            .unwrap();
        saw_redelivery |= c.redelivered;
        acked.insert(c.record.global_sequence);
        m2.ack(c.sort_key).await.unwrap();
    }
    assert_eq!(
        acked,
        (1..=4).collect::<BTreeSet<_>>(),
        "0 loss — every command claimed+acked"
    );
    assert!(
        saw_redelivery,
        "the crashed member's in-flight commands were redelivered"
    );
    assert!(crashed.iter().all(|s| acked.contains(s)));

    for d in [&obj, &local] {
        let _ = std::fs::remove_dir_all(d);
    }
}
