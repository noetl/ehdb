//! **M1 + M4 wired** — locality on a replica target, and the region survival
//! goal consulted at the `check_replica_domains` call site.
//!
//! ## The gap these close
//!
//! `Locality` (M1) and `RegionPlacement` / `check_region_survival` (M4) were
//! both merged and **consulted by nothing** — the same shape as the failure
//! domain guard before `replica_domain_gate.rs` gave it a call site. A survival
//! goal nothing reads is a setting an operator believes is applied.
//!
//! ## Two different questions, deliberately not merged
//!
//! `check_replica_domains` asks "are these distinct devices"; this asks "are
//! these distinct **declared regions**". No device-id comparison can answer the
//! second: two disks in one region are two domains and one region. `Zone` is
//! the default and the region check is a strict no-op under it, so an existing
//! deployment sees byte-identical behaviour.

use std::sync::Arc;

use ehdb_l0::failure_domain::SurvivalGoal;
use ehdb_l0::placement::Locality;
use ehdb_l0::substrate::{DurableSubstrate, InMemorySubstrate};
use ehdb_l0::{L0Config, L0EventLogEngine, ReplicaTarget};

fn unique_dir(tag: &str) -> std::path::PathBuf {
    static N: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let n = N.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    std::env::temp_dir().join(format!("ehdb-rsg-{tag}-{}-{n}-{nanos}", std::process::id()))
}

fn cfg(root: &std::path::Path) -> L0Config {
    L0Config::d1(root).with_shard_count(1).with_granule_size(4)
}

/// Distinct failure domains (so the device check always passes here) with the
/// caller-chosen regions, so each test isolates the REGION axis.
fn targets(regions: &[Option<&str>]) -> Vec<ReplicaTarget> {
    regions
        .iter()
        .enumerate()
        .map(|(i, r)| {
            let s: Arc<dyn DurableSubstrate> =
                Arc::new(InMemorySubstrate::new(format!("instance-{i}")));
            let t = ReplicaTarget::new(format!("replica-{i}"), s);
            match r {
                Some(region) => t.with_locality(Locality::new(*region, format!("{region}-a"))),
                None => t,
            }
        })
        .collect()
}

#[test]
fn a_replica_target_is_undeclared_unless_told_otherwise() {
    // M1's safe direction: `new` must not invent a locality. An assumed region
    // would let an undeclared set *pass* the region check, which is the one
    // outcome that must never happen silently.
    let s: Arc<dyn DurableSubstrate> = Arc::new(InMemorySubstrate::new("x"));
    let t = ReplicaTarget::new("replica-0", s);
    assert!(t.locality.is_undeclared());
    assert_eq!(t.locality.region, None);

    let s2: Arc<dyn DurableSubstrate> = Arc::new(InMemorySubstrate::new("y"));
    let t2 = ReplicaTarget::new("replica-1", s2).with_locality(Locality::new("us-central1", "a"));
    assert_eq!(t2.locality.region.as_deref(), Some("us-central1"));
}

#[test]
fn the_default_goal_leaves_the_region_check_inert() {
    // ⚠ The load-bearing default. Every deployment today declares no locality;
    // if `Zone` consulted the region check they would all fail to open.
    let engine =
        L0EventLogEngine::open_replicated(cfg(&unique_dir("zone-default")), targets(&[None, None]));
    assert!(
        engine.is_ok(),
        "an undeclared replica set must open unchanged under the default goal"
    );
}

#[test]
fn region_goal_refuses_two_replicas_in_one_region() {
    let msg = match L0EventLogEngine::open_replicated(
        cfg(&unique_dir("shared-region")).with_survival_goal(SurvivalGoal::Region),
        targets(&[Some("us-central1"), Some("us-central1")]),
    ) {
        Ok(_) => panic!("two replicas in one region must be refused under the Region goal"),
        Err(e) => format!("{e}"),
    };
    assert!(
        msg.contains("survival goal 'region'"),
        "the refusal must name the goal it could not meet, got: {msg}"
    );
}

#[test]
fn region_goal_refuses_an_undeclared_replica() {
    // Silence is not independence: an undeclared replica cannot be SHOWN to be
    // in another region, so it must not be counted as spread.
    let msg = match L0EventLogEngine::open_replicated(
        cfg(&unique_dir("undeclared-region")).with_survival_goal(SurvivalGoal::Region),
        targets(&[Some("us-central1"), None]),
    ) {
        Ok(_) => panic!("an undeclared replica must be refused under the Region goal"),
        Err(e) => format!("{e}"),
    };
    assert!(msg.contains("survival goal 'region'"), "got: {msg}");
}

#[test]
fn region_goal_accepts_a_genuinely_cross_region_set() {
    // THE POSITIVE CONTROL. Without it every assertion above is satisfied by a
    // check that refuses unconditionally — which would make the Region goal
    // impossible to use while looking rigorous.
    let engine = L0EventLogEngine::open_replicated(
        cfg(&unique_dir("spread-region")).with_survival_goal(SurvivalGoal::Region),
        targets(&[Some("us-central1"), Some("europe-west1")]),
    );
    assert!(
        engine.is_ok(),
        "two replicas in distinct declared regions must be accepted under the Region goal"
    );
}

#[test]
fn the_region_check_is_reached_from_the_engine_not_only_from_its_own_tests() {
    // The reachability property this file exists for. `check_region_survival`
    // was correct and called by nothing; the proof that it is now WIRED is that
    // flipping only the goal — with the replica set held constant — changes the
    // engine's answer.
    let set = |r: &[Option<&str>]| targets(r);
    let shared = [Some("us-central1"), Some("us-central1")];

    let inert = L0EventLogEngine::open_replicated(cfg(&unique_dir("reach-zone")), set(&shared));
    let enforced = L0EventLogEngine::open_replicated(
        cfg(&unique_dir("reach-region")).with_survival_goal(SurvivalGoal::Region),
        set(&shared),
    );
    assert!(inert.is_ok(), "Zone must not consult the region check");
    assert!(
        enforced.is_err(),
        "Region must consult it — if both arms agree, the goal reaches nothing"
    );
}
