//! noetl/ai-meta#209 defect 2 — engine-level proof that a hard kill no longer
//! loses (or destroys) the unsealed active part.
//!
//! `crates/ehdb-l0/src/part.rs` covers the writer in isolation. This covers the
//! part the writer tests cannot reach: the **engine's** reopen, where the
//! catalog resumes from a durable manifest that lists sealed parts only, and the
//! recovered records therefore sit outside everything the engine thinks it has.
//!
//! Dropping the engine without sealing is the SIGKILL analogue — no seal hook
//! runs, no manifest is written, and the active part is left on disk exactly as
//! a crash leaves it.

use std::sync::Arc;

use ehdb_l0::substrate::DurableSubstrate;
use ehdb_l0::{D1EventLog, EventRecord, L0Config, L0Engine, LocalFsSubstrate, ReplicaTarget};

fn unique_dir(tag: &str) -> std::path::PathBuf {
    static N: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let n = N.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    std::env::temp_dir().join(format!(
        "ehdb-l0-crash-{tag}-{}-{n}-{nanos}",
        std::process::id()
    ))
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

fn rec(seq: u64, exec: &str) -> EventRecord {
    EventRecord::new(seq, exec, format!("txn-{seq}"), format!("payload-{seq}"))
}

/// Records appended and `fsync`ed into an unsealed part, then lost to a hard
/// kill, are present after the engine reopens.
///
/// Before the fix the engine resumed from the manifest (which never saw them)
/// **and** truncated the active file on open, so they were destroyed rather than
/// merely invisible.
#[test]
fn unsealed_records_survive_an_engine_reopen() {
    let obj = unique_dir("obj");
    let local = unique_dir("local");
    let cfg = || L0Config::d1(&local).with_shard_count(1);

    {
        let mut engine =
            L0Engine::<D1EventLog>::open_replicated(cfg(), targets(&[obj.clone()])).unwrap();
        for seq in 1..=5 {
            engine.append_record(rec(seq, "exec-a")).unwrap();
        }
        // No seal, no flush — drop is the SIGKILL analogue.
    }

    let engine = L0Engine::<D1EventLog>::open_replicated(cfg(), targets(&[obj])).unwrap();
    let got = engine.read_index_after("exec-a", 0).unwrap();
    assert_eq!(
        got.len(),
        5,
        "all 5 fsync'ed-and-acked records must survive the crash"
    );
    assert_eq!(
        got.iter().map(|r| r.global_sequence).collect::<Vec<_>>(),
        vec![1, 2, 3, 4, 5]
    );
    assert_eq!(
        engine.metrics().snapshot().recovered_active_records,
        5,
        "the recovery must be reported, not silent"
    );
}

/// The reconciliation that makes recovery safe rather than merely non-lossy.
///
/// The engine's `global_sequence` comes from the manifest, which is behind the
/// recovered tail. If it is not lifted, the next writer-assigned append mints a
/// key at or below the recovered tip — an append that lands behind every
/// follower cursor and is never delivered, i.e. the silent drop of
/// noetl/ai-meta#203. So the post-recovery append must both advance and stay in
/// order.
#[test]
fn a_post_recovery_append_does_not_land_behind_the_recovered_tail() {
    let obj = unique_dir("obj");
    let local = unique_dir("local");
    let cfg = || L0Config::d1(&local).with_shard_count(1);

    {
        let mut engine =
            L0Engine::<D1EventLog>::open_replicated(cfg(), targets(&[obj.clone()])).unwrap();
        for seq in 1..=4 {
            engine.append_record(rec(seq, "exec-a")).unwrap();
        }
    }

    let mut engine = L0Engine::<D1EventLog>::open_replicated(cfg(), targets(&[obj])).unwrap();
    let assigned = engine
        .append_writer_assigned(rec(0, "exec-a"))
        .expect("writer assigns the key");
    assert!(
        assigned > 4,
        "writer-assigned key {assigned} must advance past the recovered tip 4"
    );
    assert_eq!(
        engine.metrics().snapshot().out_of_order_appends,
        0,
        "recovering a crash must not introduce the #203 silent drop"
    );

    let got = engine.read_index_after("exec-a", 0).unwrap();
    assert_eq!(got.len(), 5, "4 recovered + 1 new");
}

/// A clean seal leaves nothing to replay, so a normal restart must not
/// double-count records that are already durable in a sealed part.
#[test]
fn a_sealed_engine_recovers_nothing_on_reopen() {
    let obj = unique_dir("obj");
    let local = unique_dir("local");
    let cfg = || L0Config::d1(&local).with_shard_count(1);

    {
        let mut engine =
            L0Engine::<D1EventLog>::open_replicated(cfg(), targets(&[obj.clone()])).unwrap();
        for seq in 1..=3 {
            engine.append_record(rec(seq, "exec-a")).unwrap();
        }
        engine.flush_and_wait_uploads().unwrap();
    }

    let engine = L0Engine::<D1EventLog>::open_replicated(cfg(), targets(&[obj])).unwrap();
    let got = engine.read_index_after("exec-a", 0).unwrap();
    assert_eq!(got.len(), 3, "sealed records are read once, not twice");
    assert_eq!(
        engine.metrics().snapshot().recovered_active_records,
        0,
        "a clean shutdown leaves nothing to recover"
    );
}
