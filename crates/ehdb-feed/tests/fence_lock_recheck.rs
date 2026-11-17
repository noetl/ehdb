//! noetl/ehdb#426: `FencedEngine::write` checks the fence before taking the
//! engine lock, so a write can be overtaken by a newer epoch while it waits.
//! It must re-check once it holds the lock.

use std::sync::Arc;
use std::time::Duration;

use ehdb_core::fence::{EpochFence, FenceLedger, FenceMetrics, FenceMode, MutationPath};
use ehdb_feed::fenced_engine::FencedEngine;
use ehdb_l0::dataset::D1EventLog;
use ehdb_l0::engine::{L0Config, L0Engine};
use ehdb_l0::substrate::{DurableSubstrate, LocalFsSubstrate};

#[test]
fn a_write_overtaken_while_waiting_for_the_lock_is_refused() {
    let root = std::env::temp_dir().join(format!("ehdb-426-fence-lock-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    let store: Arc<dyn DurableSubstrate> =
        Arc::new(LocalFsSubstrate::new(root.join("obj")).unwrap());
    let engine: L0Engine<D1EventLog> =
        L0Engine::open(L0Config::d1(root.join("local")), store).unwrap();
    let fence = EpochFence::new(
        FenceMode::Enforce,
        Some(FenceLedger::new(root.join("fence")).unwrap()),
        FenceMetrics::new(),
    );
    fence.set_epoch(5);
    let fenced = Arc::new(FencedEngine::new(engine, Arc::clone(&fence), 0));

    // Hold the engine lock so the next write checks the fence, then waits.
    let held = fenced.read();
    let writer = {
        let fenced = Arc::clone(&fenced);
        std::thread::spawn(move || fenced.write(MutationPath::AppendRecord).map(|_| ()))
    };
    std::thread::sleep(Duration::from_millis(200));
    // A newer writer is accepted on the same marker while it waits.
    let newer = EpochFence::new(
        FenceMode::Enforce,
        Some(FenceLedger::new(root.join("fence")).unwrap()),
        FenceMetrics::new(),
    );
    newer.set_epoch(7);
    newer.check(0, MutationPath::AppendRecord).unwrap();
    drop(held);

    let result = writer.join().unwrap();
    assert!(result.is_err(), "a superseded write got the engine lock");
    assert_eq!(
        fence
            .metrics()
            .stale_refused
            .load(std::sync::atomic::Ordering::Relaxed),
        1
    );
    let _ = std::fs::remove_dir_all(&root);
}
