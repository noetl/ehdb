//! `forget_runtime` must actually release the replayed state, not just look like it.
//!
//! The cache is what makes the tier's memory permanent: `with_runtime` inserts a
//! runtime per log path and never removed one until this existed. A caller that
//! rotates a log needs the old state gone, or the file shrinks and the RSS does
//! not — which is the shape that OOM-killed the production writer twice.

use std::path::PathBuf;

fn tmp(tag: &str) -> PathBuf {
    let p = std::env::temp_dir().join(format!(
        "ehdb-forget-{tag}-{}-{:?}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
    ));
    std::fs::create_dir_all(&p).unwrap();
    p.join("log.jsonl")
}

#[test]
fn forget_runtime_evicts_a_cached_runtime_and_reports_it() {
    // ⚠ The cache is only consulted when enabled; with it off there is nothing
    // to evict and this test would pass vacuously. Assert the precondition.
    unsafe { std::env::set_var("NOETL_EHDB_REFERENCE_RUNTIME_CACHE", "true") };
    assert!(
        ehdb_reference::runtime_cache_enabled(),
        "the cache must be ON, or eviction is a no-op and this proves nothing"
    );

    let path = tmp("evict");
    let before = ehdb_reference::cached_runtime_count();
    ehdb_reference::with_runtime(&path, |_rt| Ok(())).expect("open");
    let after_open = ehdb_reference::cached_runtime_count();
    assert_eq!(
        after_open,
        before + 1,
        "opening must have cached exactly one runtime (before={before} after={after_open})"
    );

    assert!(
        ehdb_reference::forget_runtime(&path),
        "forget_runtime must report that it removed the entry it just cached"
    );
    assert_eq!(
        ehdb_reference::cached_runtime_count(),
        before,
        "the cache must be back to its prior size — the state is what holds the memory"
    );

    // ⭐ NEGATIVE CONTROL: a second forget must report false. Without this,
    // an implementation that always returns `true` passes the assertion above.
    assert!(
        !ehdb_reference::forget_runtime(&path),
        "forgetting an absent path must report false, not a blanket true"
    );

    let _ = std::fs::remove_dir_all(path.parent().unwrap());
}

#[test]
fn forgetting_a_path_that_was_never_opened_is_safe_and_false() {
    let path = tmp("never");
    assert!(
        !ehdb_reference::forget_runtime(&path),
        "an unopened path was reported as evicted"
    );
    let _ = std::fs::remove_dir_all(path.parent().unwrap());
}
