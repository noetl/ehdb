//! `active_age` must not report an age for an EMPTY active part.
//!
//! ⚠⚠ Diagnosed from production gauges that disagreed with each other. The server publishes
//! both `noetl_ehdb_oldest_unsealed_age_seconds` (from `L0Engine::active_ages`) and
//! `noetl_ehdb_unreplicated_records` (from `unreplicated_snapshot`). Across 15-minute
//! stretches the first climbed 59 → 779 s while the second read **0**:
//!
//! ```text
//! 21:32  unsealed_age=59   unrep_recs=0
//! 21:35  unsealed_age=239  unrep_recs=0
//! 21:38  unsealed_age=419  unrep_recs=0
//! 21:41  unsealed_age=599  unrep_recs=0
//! 21:44  unsealed_age=779  unrep_recs=0
//! ```
//!
//! ⚠⚠ RED CONTROL RESULT: removing the `record_count` check from `active_age` leaves every
//! test in this file PASSING. So these tests do NOT reproduce the production divergence, and
//! the cause of it is still unknown — the seal path clears `first_append_at` in every
//! sequence these tests can drive. They pin a correct invariant (an empty active part has no
//! age) and nothing more. Do not read them as evidence that the divergence is fixed.
//!
//! What IS established: `aged_out()` consults `record_count` and `active_age()` did not, so
//! the two disagreed about the same writer; and the seal trigger was never at risk. `active_age()` read `first_append_at` without consulting `record_count`, while
//! `aged_out()` does consult it, so the seal trigger was never fooled; only the reported age
//! was. Publishing the two side by side is what made it visible at all.

use std::sync::Arc;

use ehdb_l0::substrate::DurableSubstrate;
use ehdb_l0::{L0Config, L0EventLogEngine, LocalFsSubstrate};

fn unique_dir(tag: &str) -> std::path::PathBuf {
    static N: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let n = N.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    std::env::temp_dir().join(format!("ehdb-age-{tag}-{}-{n}", std::process::id()))
}

fn store(dir: &std::path::Path) -> Arc<dyn DurableSubstrate> {
    Arc::new(LocalFsSubstrate::new(dir).unwrap())
}

/// ⭐ The regression. After a seal the active part is empty, so there is no unsealed record
/// and no age to report — reporting one invents a tail that does not exist.
#[test]
fn after_a_seal_the_active_age_is_absent_not_climbing() {
    let local = unique_dir("local");
    let objects = unique_dir("obj");
    let cfg = L0Config::d1(&local).with_shard_count(1).with_seal_max_records(10_000);
    let mut e = L0EventLogEngine::open(cfg, store(&objects)).unwrap();

    for i in 0..6u64 {
        e.append("8001", &format!("t{i}"), format!("p-{i}")).unwrap();
    }

    // With records pending there IS an age — the positive control, without which this test
    // passes for a function that always returns None.
    let pending = e.active_ages();
    assert_eq!(pending.len(), 1, "expected one shard with a pending age: {pending:?}");

    // Seal everything. The active part is now empty.
    e.flush_and_wait_uploads().unwrap();

    let after = e.active_ages();
    assert!(
        after.is_empty(),
        "⚠⚠ an EMPTY active part reported an age of {:?}. That is a phantom unsealed tail: \
         nothing is waiting, yet the gauge climbs — which is exactly what made the prod \
         durability gauges contradict each other.",
        after
    );

    // And the records really did seal, so the fixture proved what it claims.
    assert_eq!(e.replay_all().unwrap().len(), 6);

    drop(e);
    let _ = std::fs::remove_dir_all(&local);
    let _ = std::fs::remove_dir_all(&objects);
}

/// The age returns once appending resumes — so the fix suppresses a phantom, not the signal.
#[test]
fn the_age_returns_when_appending_resumes() {
    let local = unique_dir("local");
    let objects = unique_dir("obj");
    let cfg = L0Config::d1(&local).with_shard_count(1).with_seal_max_records(10_000);
    let mut e = L0EventLogEngine::open(cfg, store(&objects)).unwrap();

    e.append("8002", "t0", "p0".to_string()).unwrap();
    e.flush_and_wait_uploads().unwrap();
    assert!(e.active_ages().is_empty(), "empty after the seal");

    e.append("8002", "t1", "p1".to_string()).unwrap();
    assert_eq!(
        e.active_ages().len(),
        1,
        "a real unsealed record must still produce an age — the fix must not silence the \
         signal it exists to report"
    );

    drop(e);
    let _ = std::fs::remove_dir_all(&local);
    let _ = std::fs::remove_dir_all(&objects);
}

/// ⚠ `aged_out` was always correct, and the seal trigger therefore never fired on a phantom.
/// Pinned so a future "simplification" does not make `active_age` and `aged_out` agree by
/// removing the `record_count` check from the wrong one.
#[test]
fn an_empty_part_never_ages_out_however_long_it_sits() {
    let local = unique_dir("local");
    let objects = unique_dir("obj");
    let cfg = L0Config::d1(&local)
        .with_shard_count(1)
        .with_seal_max_records(10_000)
        // A limit so small that any non-zero age exceeds it immediately.
        .with_seal_max_age(Some(std::time::Duration::from_nanos(1)));
    let mut e = L0EventLogEngine::open(cfg, store(&objects)).unwrap();

    e.append("8003", "t0", "p0".to_string()).unwrap();
    // ⚠ Deliberately sealed via `flush_and_wait_uploads`, not by asserting that
    // `seal_aged_parts` fires here. With a 1 ns limit the APPEND path's own `should_seal`
    // may already have sealed the part, so `seal_aged_parts` legitimately returns 0 — my
    // first draft asserted 1 and failed for that reason. The property under test is what
    // happens to an EMPTY part, so the seal just needs to be deterministic.
    e.flush_and_wait_uploads().unwrap();
    assert_eq!(e.replay_all().unwrap().len(), 1, "fixture: the record did seal");

    // Now empty. However long it sits, there is nothing to seal.
    assert_eq!(
        e.seal_aged_parts().unwrap(),
        0,
        "an EMPTY active part must never age out — sealing nothing would create empty parts \
         on a timer and grow the manifest without bound"
    );
    assert!(e.active_ages().is_empty());

    drop(e);
    let _ = std::fs::remove_dir_all(&local);
    let _ = std::fs::remove_dir_all(&objects);
}
