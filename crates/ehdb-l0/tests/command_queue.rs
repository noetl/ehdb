//! D2 proof — the **command queue** over immutable parts. Real behavior:
//!
//! - **enqueue / claim** append ops to the log (parts stay immutable).
//! - **claim-by-id** folds one command's ops → its current state.
//! - **unclaimed-scan** folds the whole log → only the still-unclaimed commands.
//! - **cold-load** reconstructs the exact queue state from the substrate.
//! - **replica-kill fallback** (L0.6): a dead replica does not lose the queue.

use std::sync::Arc;

use ehdb_l0::substrate::DurableSubstrate;
use ehdb_l0::{CommandQueue, CommandState, LocalFsSubstrate, ReplicaTarget};

fn unique_dir(tag: &str) -> std::path::PathBuf {
    static N: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let n = N.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    std::env::temp_dir().join(format!("ehdb-l0-d2-{tag}-{}-{n}", std::process::id()))
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

#[test]
fn enqueue_claim_unclaimed_scan_and_cold_load() {
    let obj = unique_dir("obj");
    let local = unique_dir("local");
    let cold_local = unique_dir("cold");
    let cfg = |root: &std::path::Path| CommandQueue::config(root).with_seal_max_records(8);

    let store: Arc<dyn DurableSubstrate> = Arc::new(LocalFsSubstrate::new(&obj).unwrap());
    let mut q = CommandQueue::open(cfg(&local), store).unwrap();

    // Enqueue commands 1..=12.
    for id in 1..=12u64 {
        q.enqueue(&id.to_string(), format!("payload-{id}")).unwrap();
    }
    // Claim the even ones.
    for id in (2..=12u64).step_by(2) {
        q.claim(&id.to_string(), format!("worker-{id}")).unwrap();
    }

    // claim-by-id state.
    assert_eq!(q.command_state("1").unwrap(), Some(CommandState::Unclaimed));
    assert_eq!(
        q.command_state("2").unwrap(),
        Some(CommandState::Claimed {
            claimer: "worker-2".into()
        })
    );
    assert_eq!(q.command_state("999").unwrap(), None, "never enqueued");

    // unclaimed-scan → only the odd ids (string-sorted, since ids are opaque).
    let unclaimed = q.unclaimed_scan().unwrap();
    let mut want: Vec<String> = (1..=12u64).step_by(2).map(|i| i.to_string()).collect();
    want.sort();
    assert_eq!(
        unclaimed, want,
        "unclaimed scan returns exactly the odd commands"
    );

    // Flush + run a merge (compaction of the op log) — queue state is unchanged.
    q.flush_and_wait().unwrap();
    let _ = q.run_pending_merges().unwrap();
    assert_eq!(
        q.unclaimed_scan().unwrap(),
        want,
        "merge preserves queue state"
    );
    assert_eq!(
        q.command_state("2").unwrap(),
        Some(CommandState::Claimed {
            claimer: "worker-2".into()
        })
    );

    // Cold-load a fresh node from the substrate → reconstructs the queue state.
    let cold_store: Arc<dyn DurableSubstrate> = Arc::new(LocalFsSubstrate::new(&obj).unwrap());
    let cold = CommandQueue::cold_load(cfg(&cold_local), cold_store).unwrap();
    assert_eq!(
        cold.unclaimed_scan().unwrap(),
        want,
        "cold-load reproduces the queue"
    );
    assert_eq!(
        cold.command_state("4").unwrap(),
        Some(CommandState::Claimed {
            claimer: "worker-4".into()
        })
    );
    assert_eq!(
        cold.command_state("5").unwrap(),
        Some(CommandState::Unclaimed)
    );

    drop(cold);
    drop(q);
    for d in [&obj, &local, &cold_local] {
        let _ = std::fs::remove_dir_all(d);
    }
}

#[test]
fn queue_survives_a_dead_replica() {
    // 3-way replicated queue; kill replica-0; cold-load still serves the queue.
    let dirs: Vec<_> = (0..3).map(|i| unique_dir(&format!("r{i}"))).collect();
    let local = unique_dir("nlocal");
    let cold_local = unique_dir("ncold");
    let cfg = |root: &std::path::Path| CommandQueue::config(root).with_seal_max_records(8);

    let mut q = CommandQueue::open_replicated(cfg(&local), targets(&dirs)).unwrap();
    for id in 1..=10u64 {
        q.enqueue(&id.to_string(), format!("p{id}")).unwrap();
    }
    q.claim("3", "w3").unwrap();
    q.claim("7", "w7").unwrap();
    q.flush_and_wait().unwrap();
    let want = q.unclaimed_scan().unwrap(); // 1,2,4,5,6,8,9,10
    assert_eq!(want.len(), 8);
    drop(q);

    // Kill replica-0.
    std::fs::remove_dir_all(&dirs[0]).unwrap();
    let cold = CommandQueue::cold_load_replicated(cfg(&cold_local), targets(&dirs)).unwrap();
    assert_eq!(
        cold.unclaimed_scan().unwrap(),
        want,
        "queue served from survivors"
    );
    assert_eq!(
        cold.command_state("3").unwrap(),
        Some(CommandState::Claimed {
            claimer: "w3".into()
        })
    );
    assert!(
        cold.engine().metrics().snapshot().read_fallbacks > 0,
        "read fell back past dead replica-0"
    );

    drop(cold);
    for d in dirs.iter().chain([&local, &cold_local]) {
        let _ = std::fs::remove_dir_all(d);
    }
}

// ---------------------------------------------------------------------------
// noetl/ai-meta#454 — the fold counter.
// ---------------------------------------------------------------------------

/// ⭐ Existence is not reachability: a counter that no path increments reads
/// exactly like a path that never ran. This pins that both fold call sites
/// actually record, and that the cost dimension sums op records rather than
/// counting folds twice.
#[test]
fn folds_are_counted_at_both_call_sites() {
    let obj = unique_dir("fold-obj");
    let local = unique_dir("fold-local");
    let store: Arc<dyn DurableSubstrate> = Arc::new(LocalFsSubstrate::new(&obj).unwrap());
    let mut q = CommandQueue::open(CommandQueue::config(&local), store).unwrap();

    let m = q.engine().metrics();
    assert_eq!(m.snapshot().folds, 0, "fresh queue has folded nothing");

    // 3 commands, 2 ops each (enqueue + claim) on two of them.
    for id in 1..=3u64 {
        q.enqueue(&id.to_string(), format!("p-{id}")).unwrap();
    }
    for id in 1..=2u64 {
        q.claim(&id.to_string(), "worker-a").unwrap();
    }
    assert_eq!(
        m.snapshot().folds,
        0,
        "writes must not fold — only the read paths do"
    );

    // Call site 1: command_state folds exactly one command.
    let before = m.snapshot();
    let st = q.command_state("1").unwrap();
    assert!(
        matches!(st, Some(CommandState::Claimed { ref claimer }) if claimer == "worker-a"),
        "fixture must have folded to Claimed, got {st:?}"
    );
    let after = m.snapshot();
    assert_eq!(after.folds, before.folds + 1, "command_state folds once");
    assert_eq!(
        after.ops_folded,
        before.ops_folded + 2,
        "and consumed that command's 2 ops (enqueue + claim)"
    );

    // Call site 2: unclaimed_scan folds EVERY command, one fold each.
    let before = m.snapshot();
    let unclaimed = q.unclaimed_scan().unwrap();
    assert_eq!(unclaimed, vec!["3".to_string()]);
    let after = m.snapshot();
    assert_eq!(
        after.folds,
        before.folds + 3,
        "a scan over 3 commands records 3 folds, not 1"
    );
    assert_eq!(
        after.ops_folded,
        before.ops_folded + 5,
        "summing their ops: 2 + 2 + 1"
    );

    // ⭐ The ratio the pair exists for.
    let s = m.snapshot();
    assert!(
        s.ops_folded > s.folds,
        "ops_folded/folds is the mean ops per fold and must be derivable: {} / {}",
        s.ops_folded,
        s.folds
    );
}

/// A command that was never enqueued still folds — the fold is what *decides*
/// it is absent, so not counting it would understate the read cost of a miss.
#[test]
fn a_miss_still_counts_as_a_fold() {
    let obj = unique_dir("fold-miss-obj");
    let local = unique_dir("fold-miss-local");
    let store: Arc<dyn DurableSubstrate> = Arc::new(LocalFsSubstrate::new(&obj).unwrap());
    let q = CommandQueue::open(CommandQueue::config(&local), store).unwrap();
    let m = q.engine().metrics();

    assert_eq!(q.command_state("nope").unwrap(), None);
    assert_eq!(m.snapshot().folds, 1, "a miss is still a fold");
    assert_eq!(m.snapshot().ops_folded, 0, "over zero ops");
}
