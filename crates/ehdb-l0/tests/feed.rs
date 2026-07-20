//! L1 T0 proof — the `Watch(shard, cursor)` change-feed primitive: follow /
//! resume / tail / shard-isolation / parity-vs-replay / replica-kill, over the
//! D1 event-log on the L0 engine.

use std::collections::BTreeMap;
use std::sync::Arc;

use ehdb_l0::substrate::DurableSubstrate;
use ehdb_l0::{ChangeFeed, D1EventLog, L0Config, L0Engine, LocalFsSubstrate, ReplicaTarget};

fn unique_dir(tag: &str) -> std::path::PathBuf {
    static N: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let n = N.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    std::env::temp_dir().join(format!("ehdb-l0-feed-{tag}-{}-{n}", std::process::id()))
}

fn targets(dirs: &[std::path::PathBuf]) -> Vec<ReplicaTarget> {
    dirs.iter()
        .enumerate()
        .map(|(i, d)| {
            let s: Arc<dyn DurableSubstrate> = Arc::new(LocalFsSubstrate::new(d).unwrap());
            ReplicaTarget::new(format!("replica-{i}"), s)
        })
        .collect()
}

fn seqs(recs: &[ehdb_l0::EventRecord]) -> Vec<u64> {
    recs.iter().map(|r| r.global_sequence).collect()
}

#[test]
fn follow_resume_and_parity_across_seal() {
    let obj = unique_dir("obj");
    let local = unique_dir("local");
    // Small seal so records cross the durable-part boundary, exercising the
    // ranged-read path, not just the hot buffer.
    let cfg = L0Config::d1(&local).with_seal_max_records(4);
    let store: Arc<dyn DurableSubstrate> = Arc::new(LocalFsSubstrate::new(&obj).unwrap());
    let mut engine = L0Engine::<D1EventLog>::open(cfg, store).unwrap();

    let mut feed = ChangeFeed::new(0, 0);
    // Nothing appended yet → empty, cursor stays 0.
    assert!(feed.poll(&engine).unwrap().is_empty());
    assert_eq!(feed.cursor(), 0);

    for i in 0..5u64 {
        engine
            .append(&format!("e{i}"), "t", format!("p{i}"))
            .unwrap();
    }
    let batch = feed.poll(&engine).unwrap();
    assert_eq!(
        seqs(&batch),
        vec![1, 2, 3, 4, 5],
        "delivered in sort-key order"
    );
    assert_eq!(feed.cursor(), 5);
    // Idempotent at a fixed cursor: re-poll with no new appends → nothing.
    assert!(feed.poll(&engine).unwrap().is_empty());

    // Append more → only the new records, cursor advances.
    for i in 5..8u64 {
        engine
            .append(&format!("e{i}"), "t", format!("p{i}"))
            .unwrap();
    }
    assert_eq!(seqs(&feed.poll(&engine).unwrap()), vec![6, 7, 8]);
    assert_eq!(feed.cursor(), 8);

    // Parity: the union of everything the feed delivered == a full shard replay,
    // 0 missed / 0 spurious.
    engine.flush_and_wait_uploads().unwrap();
    let full = engine.read_partition_after(0, 0).unwrap();
    assert_eq!(seqs(&full), (1..=8).collect::<Vec<_>>());

    // Resume / reconnect: a fresh feed from cursor 5 delivers exactly the tail.
    let mut resumed = ChangeFeed::new(0, 5);
    assert_eq!(seqs(&resumed.poll(&engine).unwrap()), vec![6, 7, 8]);

    // seek rewinds to redeliver from an earlier watermark (ack-redelivery seam).
    resumed.seek(2);
    assert_eq!(
        seqs(&resumed.poll(&engine).unwrap()),
        vec![3, 4, 5, 6, 7, 8]
    );

    // A brand-new follower from 0 still sees all 8 across the sealed-part
    // boundary + hot buffer.
    let mut fresh = ChangeFeed::new(0, 0);
    assert_eq!(
        seqs(&fresh.poll(&engine).unwrap()),
        (1..=8).collect::<Vec<_>>()
    );

    drop(engine);
    for d in [&obj, &local] {
        let _ = std::fs::remove_dir_all(d);
    }
}

#[test]
fn tail_sees_only_records_after_now() {
    let obj = unique_dir("obj");
    let local = unique_dir("local");
    let store: Arc<dyn DurableSubstrate> = Arc::new(LocalFsSubstrate::new(&obj).unwrap());
    let mut engine = L0Engine::<D1EventLog>::open(L0Config::d1(&local), store).unwrap();

    for i in 0..4u64 {
        engine.append(&format!("e{i}"), "t", "old").unwrap();
    }
    // Shadow posture: tail from the current tip → sees only new commands.
    let mut feed = ChangeFeed::tail(&engine, 0);
    assert_eq!(feed.cursor(), 4);
    assert!(
        feed.poll(&engine).unwrap().is_empty(),
        "no records after the tip yet"
    );

    engine.append("e-new-1", "t", "new").unwrap();
    engine.append("e-new-2", "t", "new").unwrap();
    assert_eq!(seqs(&feed.poll(&engine).unwrap()), vec![5, 6]);

    drop(engine);
    for d in [&obj, &local] {
        let _ = std::fs::remove_dir_all(d);
    }
}

#[test]
fn shards_are_isolated() {
    let obj = unique_dir("obj");
    let local = unique_dir("local");
    let cfg = L0Config::d1(&local).with_shard_count(4);
    let store: Arc<dyn DurableSubstrate> = Arc::new(LocalFsSubstrate::new(&obj).unwrap());
    let mut engine = L0Engine::<D1EventLog>::open(cfg, store).unwrap();

    // Append across many executions; record which shard each landed in.
    let mut want: BTreeMap<u32, Vec<u64>> = BTreeMap::new();
    for i in 0..24u64 {
        let exec = format!("exec-{i}");
        let shard = engine.shard_for(&exec);
        let seq = engine.append(&exec, "t", "x").unwrap();
        want.entry(shard).or_default().push(seq);
    }
    // At least two shards populated, else the isolation claim is vacuous.
    assert!(want.len() >= 2, "test needs records in multiple shards");

    let mut union: Vec<u64> = Vec::new();
    for shard in 0..engine.shard_count() {
        let got = seqs(&ChangeFeed::new(shard, 0).poll(&engine).unwrap());
        let expected = want.get(&shard).cloned().unwrap_or_default();
        assert_eq!(
            got, expected,
            "shard {shard} feed sees exactly its own records"
        );
        // Monotonic within the shard.
        assert!(got.windows(2).all(|w| w[0] < w[1]));
        union.extend(got);
    }
    union.sort();
    assert_eq!(
        union,
        (1..=24).collect::<Vec<_>>(),
        "every record surfaced on exactly one shard"
    );

    drop(engine);
    for d in [&obj, &local] {
        let _ = std::fs::remove_dir_all(d);
    }
}

#[test]
fn feed_survives_a_dead_replica() {
    let dirs: Vec<_> = (0..3).map(|i| unique_dir(&format!("r{i}"))).collect();
    let local = unique_dir("nlocal");
    let cold_local = unique_dir("ncold");
    let cfg = |root: &std::path::Path| L0Config::d1(root).with_seal_max_records(4);

    let mut engine = L0Engine::<D1EventLog>::open_replicated(cfg(&local), targets(&dirs)).unwrap();
    for i in 0..12u64 {
        engine
            .append(&format!("e{i}"), "t", format!("p{i}"))
            .unwrap();
    }
    engine.flush_and_wait_uploads().unwrap();
    drop(engine);

    // Kill replica-0 — the feed must still deliver the shard's tail from the
    // survivors.
    std::fs::remove_dir_all(&dirs[0]).unwrap();
    let cold =
        L0Engine::<D1EventLog>::cold_load_replicated(cfg(&cold_local), targets(&dirs)).unwrap();
    let mut feed = ChangeFeed::new(0, 0);
    assert_eq!(
        seqs(&feed.poll(&cold).unwrap()),
        (1..=12).collect::<Vec<_>>()
    );
    assert!(cold.engine_read_fallbacks() > 0);

    drop(cold);
    for d in dirs.iter().chain([&local, &cold_local]) {
        let _ = std::fs::remove_dir_all(d);
    }
}

// Small helper trait so the replica-kill assertion reads cleanly.
trait ReadFallbacks {
    fn engine_read_fallbacks(&self) -> u64;
}
impl ReadFallbacks for L0Engine<D1EventLog> {
    fn engine_read_fallbacks(&self) -> u64 {
        self.metrics().snapshot().read_fallbacks
    }
}
