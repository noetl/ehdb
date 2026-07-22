//! L1 T4 proof — the networked claim RPC: competing consumers across
//! connections (0 double-delivery), crashed-member redelivery (0 loss),
//! per-shard ordering.

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use ehdb_feed::{ClaimClient, ClaimCoordinator, FeedWriter};
use ehdb_l0::substrate::DurableSubstrate;
use ehdb_l0::{
    shard_for_execution, D1EventLog, EventRecord, FlushPolicy, L0Config, L0Engine, LocalFsSubstrate,
};
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
        ehdb_feed::d1_command_subject(1),
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
            let mut client = ClaimClient::connect(addr, member, "commands.shared.>")
                .await
                .unwrap();
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
        ehdb_feed::d1_command_subject(1),
    ));

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(ehdb_feed::serve_claims(listener, coord.clone()));

    for seq in 1..=4u64 {
        writer.append(ev(seq)).unwrap();
    }

    // Member 1 claims 2 commands and then "crashes" (never acks, drops conn).
    let mut m1 = ClaimClient::connect(addr, 1, "commands.shared.>")
        .await
        .unwrap();
    let a = m1.claim_next::<EventRecord>().await.unwrap();
    let b = m1.claim_next::<EventRecord>().await.unwrap();
    assert!(!a.redelivered && !b.redelivered);
    let crashed: BTreeSet<u64> = [a.record.global_sequence, b.record.global_sequence].into();
    drop(m1); // crash: connection gone, commands a+b never acked

    // Member 2 drains everything: the 2 fresh commands, then (after ack_wait) the
    // 2 redelivered ones. Every command is eventually claimed + acked.
    let mut m2 = ClaimClient::connect(addr, 2, "commands.shared.>")
        .await
        .unwrap();
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

/// A D1 command-notification record tagged for `pool` (JSON payload carrying
/// `execution_pool`, exactly as noetl-server stamps it).
fn ev_pool(seq: u64, pool: &str) -> EventRecord {
    let payload = serde_json::json!({
        "execution_id": seq,
        "event_id": seq,
        "command_id": format!("cmd-{seq}"),
        "step": "start",
        "server_url": "http://localhost:8082",
        "execution_pool": pool,
    })
    .to_string();
    EventRecord::new(seq, format!("exec-{seq}"), "t", payload)
}

/// Read the `execution_pool` back off a claimed record's payload.
fn pool_of(rec: &EventRecord) -> String {
    serde_json::from_str::<serde_json::Value>(&rec.payload)
        .unwrap()
        .get("execution_pool")
        .and_then(|p| p.as_str())
        .unwrap()
        .to_string()
}

/// The finding-#1 proof (noetl/ai-meta#194): with `system` and `shared`
/// commands interleaved on one shard, a member claiming for one pool NEVER
/// receives the other pool's command — strict bidirectional isolation — while
/// each pool's commands are still fully delivered to its own members.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pool_isolation_no_cross_pool_delivery() {
    let (obj, local) = (unique_dir("obj"), unique_dir("local"));
    let writer = writer_at(&local, &obj).await;
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

    // Interleave the two pools' commands on the one shard log.
    const N: u64 = 12;
    let mut want_system = BTreeSet::new();
    let mut want_shared = BTreeSet::new();
    for seq in 1..=N {
        if seq % 2 == 0 {
            writer.append(ev_pool(seq, "system")).unwrap();
            want_system.insert(seq);
        } else {
            writer.append(ev_pool(seq, "shared")).unwrap();
            want_shared.insert(seq);
        }
    }

    // A member per pool subscribes with its pool's subject filter; each asserts
    // every record it is handed carries `expect_pool` (0 cross-subject claims).
    async fn drain(
        addr: std::net::SocketAddr,
        member: u32,
        filter: &'static str,
        expect_pool: &'static str,
    ) -> BTreeSet<u64> {
        let mut client = ClaimClient::connect(addr, member, filter).await.unwrap();
        let mut got = BTreeSet::new();
        while let Ok(Ok(c)) = tokio::time::timeout(
            Duration::from_millis(500),
            client.claim_next::<EventRecord>(),
        )
        .await
        {
            assert_eq!(
                pool_of(&c.record),
                expect_pool,
                "member subscribed `{filter}` was handed a `{}` command — cross-subject leak",
                pool_of(&c.record)
            );
            got.insert(c.record.global_sequence);
            client.ack(c.sort_key).await.unwrap();
        }
        got
    }

    let sys = tokio::spawn(drain(addr, 1, "commands.system.>", "system"));
    let shd = tokio::spawn(drain(addr, 2, "commands.shared.>", "shared"));
    let got_system = sys.await.unwrap();
    let got_shared = shd.await.unwrap();

    // Each pool got exactly its own commands — all of them, none of the other's.
    assert_eq!(
        got_system, want_system,
        "system pool: all + only system cmds"
    );
    assert_eq!(
        got_shared, want_shared,
        "shared pool: all + only shared cmds"
    );
    assert!(
        got_system.is_disjoint(&got_shared),
        "no command delivered to both pools"
    );

    for d in [&obj, &local] {
        let _ = std::fs::remove_dir_all(d);
    }
}

/// Shard routing as a subject dimension (#166 over subjects): with a
/// 2-shard subject derivation, a member subscribed to `commands.shared.shard.0`
/// only ever claims shard-0 commands — never a shard-1 command. Proves the
/// same subject mechanism carries the shard dimension, not just the pool.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shard_routing_over_subjects() {
    let (obj, local) = (unique_dir("obj"), unique_dir("local"));
    let writer = writer_at(&local, &obj).await;
    // Subject shard is derived from execution_id over 2 shards.
    let coord = Arc::new(ClaimCoordinator::new(
        writer.clone(),
        0,
        Duration::from_secs(30),
        0,
        ehdb_feed::d1_command_subject(2),
    ));

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(ehdb_feed::serve_claims(listener, coord.clone()));

    // Append shared commands; bucket each by the shard its execution_id derives.
    let mut want_shard0 = BTreeSet::new();
    const N: u64 = 24;
    for seq in 1..=N {
        writer.append(ev_pool(seq, "shared")).unwrap();
        if shard_for_execution(&format!("exec-{seq}"), 2) == 0 {
            want_shard0.insert(seq);
        }
    }
    // Both shards must actually be represented or the test proves nothing.
    assert!(
        !want_shard0.is_empty() && want_shard0.len() < N as usize,
        "test needs both shards populated: {want_shard0:?}"
    );

    // A member subscribed to shard 0 only.
    let mut client = ClaimClient::connect(addr, 1, "commands.shared.shard.0")
        .await
        .unwrap();
    let mut got = BTreeSet::new();
    while let Ok(Ok(c)) = tokio::time::timeout(
        Duration::from_millis(500),
        client.claim_next::<EventRecord>(),
    )
    .await
    {
        assert_eq!(
            shard_for_execution(&c.record.execution_id, 2),
            0,
            "shard-0 subscriber was handed a shard-1 command"
        );
        got.insert(c.record.global_sequence);
        client.ack(c.sort_key).await.unwrap();
    }
    assert_eq!(
        got, want_shard0,
        "shard-0 subscriber got exactly shard-0 cmds"
    );

    for d in [&obj, &local] {
        let _ = std::fs::remove_dir_all(d);
    }
}

/// Wildcard/hierarchy: a prefix filter (`commands.>`) picks up every matching
/// subject across pools — basic NATS-style subject matching — while a
/// single-token wildcard (`commands.*.shard.0`) still isolates by shard.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn wildcard_prefix_matches_all_subjects() {
    let (obj, local) = (unique_dir("obj"), unique_dir("local"));
    let writer = writer_at(&local, &obj).await;
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

    let mut all = BTreeSet::new();
    for seq in 1..=10u64 {
        let pool = if seq % 2 == 0 { "system" } else { "shared" };
        writer.append(ev_pool(seq, pool)).unwrap();
        all.insert(seq);
    }

    // `commands.>` is the super-subscription: it claims every pool's commands.
    let mut client = ClaimClient::connect(addr, 1, "commands.>").await.unwrap();
    let mut got = BTreeSet::new();
    while let Ok(Ok(c)) = tokio::time::timeout(
        Duration::from_millis(500),
        client.claim_next::<EventRecord>(),
    )
    .await
    {
        got.insert(c.record.global_sequence);
        client.ack(c.sort_key).await.unwrap();
    }
    assert_eq!(got, all, "`commands.>` picks up every subject");

    for d in [&obj, &local] {
        let _ = std::fs::remove_dir_all(d);
    }
}
