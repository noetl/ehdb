//! L1 T1 proof — consumer groups: competing consumers / ack / ack_wait
//! redelivery / crash-redelivery / committed-cursor resume / shard routing,
//! over the L0 change-feed. Clock is logical (explicit ticks) for determinism.

use std::collections::BTreeSet;
use std::sync::Arc;

use ehdb_feed::ShardConsumerGroup;
use ehdb_l0::substrate::DurableSubstrate;
use ehdb_l0::{D1EventLog, L0Config, L0Engine, LocalFsSubstrate, ReplicaTarget};

fn unique_dir(tag: &str) -> std::path::PathBuf {
    static N: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let n = N.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    std::env::temp_dir().join(format!("ehdb-feed-t1-{tag}-{}-{n}", std::process::id()))
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

fn open(dir: &std::path::Path, obj: &std::path::Path) -> L0Engine<D1EventLog> {
    let store: Arc<dyn DurableSubstrate> = Arc::new(LocalFsSubstrate::new(obj).unwrap());
    L0Engine::<D1EventLog>::open(L0Config::d1(dir).with_shard_count(4), store).unwrap()
}

#[test]
fn competing_consumers_each_record_once() {
    let (obj, local) = (unique_dir("obj"), unique_dir("local"));
    let mut engine = open(&local, &obj);
    // 10 records into shard 0's key space (use keys that route to shard 0).
    let shard0: Vec<String> = (0..)
        .map(|i| format!("k{i}"))
        .filter(|k| engine.shard_for(k) == 0)
        .take(10)
        .collect();
    for k in &shard0 {
        engine.append(k, "t", "cmd").unwrap();
    }

    let mut group = ShardConsumerGroup::<D1EventLog>::new(0, 100, 0);
    // Two members alternate pulling; each record goes to exactly one member.
    let mut seen: BTreeSet<u64> = BTreeSet::new();
    let mut by_member = [0u32, 0u32];
    for round in 0..10u64 {
        let member = (round % 2) as u32 + 1;
        let d = group
            .poll_assign(&engine, member, 0)
            .unwrap()
            .expect("a record is available");
        assert!(!d.redelivered);
        assert_eq!(d.member, member);
        assert!(seen.insert(d.sort_key), "each record assigned exactly once");
        by_member[(member - 1) as usize] += 1;
        group.ack(d.sort_key);
    }
    // All 10 delivered, both members shared the load, none left in flight.
    assert_eq!(seen.len(), 10);
    assert_eq!(by_member, [5, 5]);
    assert_eq!(group.inflight_len(), 0);
    // Caught up: nothing more to assign.
    assert!(group.poll_assign(&engine, 1, 0).unwrap().is_none());
    // Committed through the last record.
    assert_eq!(group.committed_cursor(), engine.global_sequence());

    for d in [&obj, &local] {
        let _ = std::fs::remove_dir_all(d);
    }
}

#[test]
fn ack_wait_redelivers_then_ack_stops_it() {
    let (obj, local) = (unique_dir("obj"), unique_dir("local"));
    let mut engine = open(&local, &obj);
    let shard0: Vec<String> = (0..)
        .map(|i| format!("k{i}"))
        .filter(|k| engine.shard_for(k) == 0)
        .take(3)
        .collect();
    for k in &shard0 {
        engine.append(k, "t", "cmd").unwrap();
    }

    let ack_wait = 10;
    let mut group = ShardConsumerGroup::<D1EventLog>::new(0, ack_wait, 0);

    // Member 1 takes the first record at t=0 (deadline 10), does not ack.
    let a = group.poll_assign(&engine, 1, 0).unwrap().unwrap();
    assert!(!a.redelivered);
    let stuck = a.sort_key;

    // Member 2 takes the next fresh record at t=1.
    let b = group.poll_assign(&engine, 2, 1).unwrap().unwrap();
    assert_ne!(b.sort_key, stuck);
    group.ack(b.sort_key);

    // Before the deadline, member 2 gets the *third* fresh record, not a redeliver.
    let c = group.poll_assign(&engine, 2, 5).unwrap().unwrap();
    assert!(!c.redelivered);
    group.ack(c.sort_key);

    // Past member 1's deadline, the stuck record is redelivered (to whoever polls).
    let re = group.poll_assign(&engine, 2, 15).unwrap().unwrap();
    assert!(re.redelivered, "expired in-flight record is redelivered");
    assert_eq!(re.sort_key, stuck);
    assert_eq!(re.member, 2, "reassigned to the polling member");

    // Committed is still stuck below the unacked record.
    assert_eq!(group.committed_cursor(), stuck - 1);

    // Ack it → nothing left, committed advances to the end.
    group.ack(stuck);
    assert_eq!(group.inflight_len(), 0);
    assert!(
        group.poll_assign(&engine, 1, 100).unwrap().is_none(),
        "no more redeliveries after ack"
    );
    assert_eq!(group.committed_cursor(), engine.global_sequence());

    for d in [&obj, &local] {
        let _ = std::fs::remove_dir_all(d);
    }
}

#[test]
fn crashed_member_work_is_fully_redelivered() {
    let (obj, local) = (unique_dir("obj"), unique_dir("local"));
    let mut engine = open(&local, &obj);
    let shard0: Vec<String> = (0..)
        .map(|i| format!("k{i}"))
        .filter(|k| engine.shard_for(k) == 0)
        .take(8)
        .collect();
    for k in &shard0 {
        engine.append(k, "t", "cmd").unwrap();
    }

    let ack_wait = 10;
    let mut group = ShardConsumerGroup::<D1EventLog>::new(0, ack_wait, 0);

    // Member 1 pulls everything at t=0 and then "crashes" (never acks).
    let mut held = Vec::new();
    while let Some(d) = group.poll_assign(&engine, 1, 0).unwrap() {
        held.push(d.sort_key);
    }
    assert_eq!(held.len(), 8);
    assert_eq!(group.committed_cursor(), 0, "nothing acked yet");

    // After ack_wait, member 2 drains all of member 1's in-flight work (each a
    // redelivery) and acks it. Every record surfaces at least once, is acked once.
    let mut acked: BTreeSet<u64> = BTreeSet::new();
    let mut now = 20;
    while let Some(d) = group.poll_assign(&engine, 2, now).unwrap() {
        assert!(d.redelivered);
        assert_eq!(d.member, 2);
        group.ack(d.sort_key);
        acked.insert(d.sort_key);
        now += 1;
    }
    assert_eq!(
        acked.len(),
        8,
        "every crashed-member record redelivered + acked"
    );
    assert_eq!(group.inflight_len(), 0);
    assert_eq!(group.committed_cursor(), engine.global_sequence());

    for d in [&obj, &local] {
        let _ = std::fs::remove_dir_all(d);
    }
}

#[test]
fn committed_cursor_resumes_a_fresh_group() {
    let (obj, local) = (unique_dir("obj"), unique_dir("local"));
    let mut engine = open(&local, &obj);
    let shard0: Vec<String> = (0..)
        .map(|i| format!("k{i}"))
        .filter(|k| engine.shard_for(k) == 0)
        .take(6)
        .collect();
    for k in &shard0 {
        engine.append(k, "t", "cmd").unwrap();
    }

    let mut group = ShardConsumerGroup::<D1EventLog>::new(0, 100, 0);
    // Ack the first 3 in order, then persist the committed cursor.
    let mut acked_seqs = Vec::new();
    for _ in 0..3 {
        let d = group.poll_assign(&engine, 1, 0).unwrap().unwrap();
        group.ack(d.sort_key);
        acked_seqs.push(d.sort_key);
    }
    let resume = group.committed_cursor();
    assert_eq!(resume, acked_seqs[2], "committed through the 3rd ack");

    // A fresh group resumed from the committed cursor delivers only the remaining
    // records — no re-delivery of the acked prefix.
    let mut resumed = ShardConsumerGroup::<D1EventLog>::new(0, 100, resume);
    let mut got = Vec::new();
    while let Some(d) = resumed.poll_assign(&engine, 9, 0).unwrap() {
        resumed.ack(d.sort_key);
        got.push(d.sort_key);
    }
    assert_eq!(got.len(), 3, "only the unacked tail resumes");
    assert!(got.iter().all(|s| *s > resume));

    for d in [&obj, &local] {
        let _ = std::fs::remove_dir_all(d);
    }
}

#[test]
fn shard_routing_isolates_groups() {
    let (obj, local) = (unique_dir("obj"), unique_dir("local"));
    let mut engine = open(&local, &obj);
    // Append across executions spanning multiple shards.
    let mut per_shard: std::collections::BTreeMap<u32, Vec<u64>> = Default::default();
    for i in 0..40u64 {
        let exec = format!("exec-{i}");
        let shard = engine.shard_for(&exec);
        let seq = engine.append(&exec, "t", "cmd").unwrap();
        per_shard.entry(shard).or_default().push(seq);
    }
    assert!(per_shard.len() >= 2, "need multiple shards populated");

    // One group per shard sees exactly its shard's records; no cross-shard leak.
    for (&shard, want) in &per_shard {
        let mut group = ShardConsumerGroup::<D1EventLog>::new(shard, 100, 0);
        let mut got = Vec::new();
        while let Some(d) = group.poll_assign(&engine, 1, 0).unwrap() {
            group.ack(d.sort_key);
            got.push(d.sort_key);
        }
        assert_eq!(
            &got, want,
            "group on shard {shard} sees exactly its records"
        );
    }

    for d in [&obj, &local] {
        let _ = std::fs::remove_dir_all(d);
    }
}

#[test]
fn group_survives_a_dead_replica() {
    let dirs: Vec<_> = (0..3).map(|i| unique_dir(&format!("r{i}"))).collect();
    let (local, cold_local) = (unique_dir("nlocal"), unique_dir("ncold"));
    let cfg = |root: &std::path::Path| L0Config::d1(root).with_seal_max_records(4);

    let mut engine = L0Engine::<D1EventLog>::open_replicated(cfg(&local), targets(&dirs)).unwrap();
    let mut want = Vec::new();
    for i in 0..12u64 {
        // shard_count default 1 → all shard 0.
        want.push(engine.append(&format!("e{i}"), "t", "cmd").unwrap());
    }
    engine.flush_and_wait_uploads().unwrap();
    drop(engine);

    // Kill a replica; a fresh group over the cold-loaded engine still delivers
    // and acks the whole shard from the survivors.
    std::fs::remove_dir_all(&dirs[0]).unwrap();
    let cold =
        L0Engine::<D1EventLog>::cold_load_replicated(cfg(&cold_local), targets(&dirs)).unwrap();
    let mut group = ShardConsumerGroup::<D1EventLog>::new(0, 100, 0);
    let mut got = Vec::new();
    while let Some(d) = group.poll_assign(&cold, 1, 0).unwrap() {
        group.ack(d.sort_key);
        got.push(d.sort_key);
    }
    assert_eq!(got, want, "whole shard delivered from surviving replicas");
    assert!(cold.metrics().snapshot().read_fallbacks > 0);

    drop(cold);
    for d in dirs.iter().chain([&local, &cold_local]) {
        let _ = std::fs::remove_dir_all(d);
    }
}
