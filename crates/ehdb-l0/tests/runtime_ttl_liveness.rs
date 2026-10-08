//! **P1 of the north star: wall-clock TTL liveness on the D8 runtime registry.**
//!
//! D8 already registers, heartbeats, deregisters and lists
//! (`ehdb-l0/src/runtime.rs`). Its liveness primitive is `list_live_since(min_heartbeat)`
//! — a **monotonic counter**, which makes the CALLER decide what "stale" means in units of
//! an opaque logical clock. A registry's liveness question is `now - last_seen < ttl`, and
//! that is what makes "service registry" true rather than "worker table".
//!
//! See `design/north-star-distributed-registry.md` §B2.
//!
//! ⚠⚠ **The upgrade hazard this file exists to pin.** Adding a timestamp means records
//! written before the upgrade have none. If "no timestamp" were treated as "not live", the
//! first read after an upgrade would evict **the entire fleet** — a silent, total, and
//! instantaneous outage caused by a liveness improvement. So an absent timestamp is
//! **unknown, not dead**: those records stay live and are *counted* so the condition is
//! visible rather than inferred.

use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use ehdb_l0::substrate::DurableSubstrate;
use ehdb_l0::{LocalFsSubstrate, RuntimeStore};

fn dir(tag: &str) -> PathBuf {
    static N: AtomicU64 = AtomicU64::new(0);
    let n = N.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir().join(format!("ehdb-ttl-{tag}-{}-{n}", std::process::id()))
}

fn store(root: &PathBuf) -> RuntimeStore {
    let obj = root.join("obj");
    let hot = root.join("hot");
    std::fs::create_dir_all(&obj).unwrap();
    std::fs::create_dir_all(&hot).unwrap();
    let s: Arc<dyn DurableSubstrate> = Arc::new(LocalFsSubstrate::new(&obj).unwrap());
    RuntimeStore::open(RuntimeStore::config(&hot), s).unwrap()
}

const SEC: u64 = 1_000_000; // micros

#[test]
fn a_registration_is_live_until_its_ttl_elapses() {
    let d = dir("basic");
    let mut st = store(&d);

    // Register three services of different kinds — the registry is the fleet's, not just
    // the worker pool's.
    st.register_at("server-0", "noetl-server/api", 1_000 * SEC)
        .unwrap();
    st.register_at("gateway-0", "noetl-gateway/sse", 1_000 * SEC)
        .unwrap();
    st.register_at("worker-7", "pool=user,arch=arm64", 1_000 * SEC)
        .unwrap();

    let ttl = 30 * SEC;

    // At registration time, all three are live.
    let live = st.list_live_at(1_000 * SEC, ttl).unwrap();
    let ids: Vec<&str> = live.iter().map(|s| s.worker_id.as_str()).collect();
    println!("  at t=1000s ttl=30s -> {ids:?}");
    assert_eq!(
        ids,
        vec!["gateway-0", "server-0", "worker-7"],
        "all three live at t0"
    );

    // 29s later: still inside the TTL.
    assert_eq!(
        st.list_live_at(1_029 * SEC, ttl).unwrap().len(),
        3,
        "29s < 30s ttl"
    );

    // 31s later: all expired, with no deregister and no reaper.
    let expired = st.list_live_at(1_031 * SEC, ttl).unwrap();
    println!("  at t=1031s ttl=30s -> {} live", expired.len());
    assert!(
        expired.is_empty(),
        "31s > 30s ttl must expire every record: {expired:?}"
    );

    // A heartbeat renews exactly one of them.
    st.heartbeat_at("worker-7", 1_030 * SEC).unwrap();
    let after = st.list_live_at(1_031 * SEC, ttl).unwrap();
    let ids: Vec<&str> = after.iter().map(|s| s.worker_id.as_str()).collect();
    println!("  after renewing worker-7 -> {ids:?}");
    assert_eq!(ids, vec!["worker-7"], "only the renewed record is live");

    // ⚠ The boundary is exclusive: exactly at the TTL a record is NOT live. Stated because
    // an off-by-one here is the difference between a flapping registry and a stable one,
    // and `<` vs `<=` is invisible in any test that only probes 29s and 31s.
    assert!(
        st.list_live_at(1_030 * SEC + ttl, ttl).unwrap().is_empty(),
        "now - last_seen == ttl must be EXPIRED, not live"
    );
    assert_eq!(
        st.list_live_at(1_030 * SEC + ttl - 1, ttl).unwrap().len(),
        1,
        "one microsecond inside the ttl must still be live"
    );
    let _ = std::fs::remove_dir_all(&d);
}

#[test]
fn deregister_beats_a_fresh_timestamp() {
    let d = dir("dereg");
    let mut st = store(&d);
    st.register_at("w", "c", 100 * SEC).unwrap();
    st.deregister("w").unwrap();
    // An explicit deregister must win regardless of the clock: a service that said goodbye
    // is gone even if its last timestamp is inside the TTL.
    let live = st.list_live_at(100 * SEC, 30 * SEC).unwrap();
    println!("  deregistered at a fresh timestamp -> {} live", live.len());
    assert!(
        live.is_empty(),
        "deregister must beat a fresh timestamp: {live:?}"
    );
    let _ = std::fs::remove_dir_all(&d);
}

/// ⚠⚠ The upgrade hazard, pinned.
#[test]
fn a_record_with_no_timestamp_is_unknown_not_dead() {
    let d = dir("legacy");
    let mut st = store(&d);
    // `register` (the pre-TTL API) writes no timestamp — exactly what a record persisted
    // before this change looks like on decode.
    st.register("legacy-worker", "c").unwrap();
    st.register_at("modern-worker", "c", 1_000 * SEC).unwrap();

    let live = st.list_live_at(9_999 * SEC, 30 * SEC).unwrap();
    let ids: Vec<&str> = live.iter().map(|s| s.worker_id.as_str()).collect();
    println!("  far past every ttl -> {ids:?}");
    assert_eq!(
        ids,
        vec!["legacy-worker"],
        "a timestamp-less record must survive a TTL sweep (unknown, not dead) while a \
         stamped-but-stale one is evicted"
    );

    // And the condition must be COUNTED, not inferred: an operator has to be able to see
    // that liveness is being decided on incomplete information.
    let (live_n, unknown_n) = st.liveness_coverage(9_999 * SEC, 30 * SEC).unwrap();
    println!("  coverage: live={live_n} of which timestamp-unknown={unknown_n}");
    assert_eq!(unknown_n, 1, "the unknown-timestamp count must be reported");
    assert_eq!(live_n, 1, "and counted among the live");
    let _ = std::fs::remove_dir_all(&d);
}

/// The old counter-based API must keep working — this is additive.
#[test]
fn the_heartbeat_counter_api_is_unchanged() {
    let d = dir("compat");
    let mut st = store(&d);
    st.register("w1", "c").unwrap();
    st.register("w2", "c").unwrap();
    st.heartbeat("w1").unwrap();
    st.heartbeat("w1").unwrap();
    // w1 at heartbeat 3, w2 at 1.
    assert_eq!(
        st.list_live().unwrap().len(),
        2,
        "both live with no watermark"
    );
    let fresh = st.list_live_since(2).unwrap();
    let ids: Vec<&str> = fresh.iter().map(|s| s.worker_id.as_str()).collect();
    println!("  list_live_since(2) -> {ids:?}");
    assert_eq!(
        ids,
        vec!["w1"],
        "the counter path must behave exactly as before"
    );
    let _ = std::fs::remove_dir_all(&d);
}
