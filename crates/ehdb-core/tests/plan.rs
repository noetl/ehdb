//! **M0 — the three resolvers are identity functions under today's config.**
//!
//! This is M0's whole exit criterion. Everything later is "give a resolver a
//! non-default input", so if the identity is not proven here, no later
//! phase's rollback is trustworthy.

use ehdb_core::plan::{
    resolve_placement, resolve_route, resolve_visibility, AxisConfig, Enforcement, Locality,
    PlacementPlan, ReadConsistency, ReadLocality, ReplicaSpec, RequestContext, RoutePlan,
    RouteTarget, SurvivalGoal, VisibilityPlan,
};

const NOW: u64 = 1_789_000_000_000;

/// Today's replica set, as `engine.rs:300 open()` constructs it.
fn todays_replicas() -> Vec<ReplicaSpec> {
    vec![ReplicaSpec::new("replica-0")]
}

// ---------------------------------------------------------------------------
// E2 — THE identity property.
// ---------------------------------------------------------------------------

/// ⭐ Default config in, today's hard-coded values out. All three, together,
/// because they are only useful as a set.
#[test]
fn all_three_resolvers_are_identity_under_the_default_config() {
    let cfg = AxisConfig::default();

    assert_eq!(
        resolve_placement(&cfg, todays_replicas()),
        PlacementPlan::today(),
        "placement must emit exactly what engine.rs hard-codes"
    );
    assert_eq!(
        resolve_route(&cfg, RequestContext::default()),
        RoutePlan::today(),
        "routing must emit owner / no follower reads"
    );
    assert_eq!(
        resolve_visibility(&cfg, NOW),
        VisibilityPlan::today(),
        "visibility must gate nothing"
    );
}

/// The baselines must themselves match the values in the tree, or the test
/// above is asserting a constant against itself.
#[test]
fn the_baselines_spell_out_todays_actual_values() {
    let p = PlacementPlan::today();
    assert_eq!(p.replicas.len(), 1);
    assert_eq!(p.replicas[0].id, "replica-0", "engine.rs:300");
    assert!(p.replicas[0].locality.is_undeclared());
    assert_eq!(p.survive, SurvivalGoal::Zone);

    // ⚠ The spec said Zone-enforced. engine.rs:133 has
    // require_distinct_domains: false, and its doc calls false "today's
    // behaviour: violations are counted and logged but the open succeeds".
    assert_eq!(
        p.enforcement,
        Enforcement::Shadow,
        "today is a goal held in SHADOW, not an enforced one"
    );

    assert_eq!(RoutePlan::today().target, RouteTarget::Owner);
    assert!(!RoutePlan::today().may_follower_read);
    assert!(!VisibilityPlan::today().gates_anything());
}

/// ⚠ `Default` for every axis must BE today. If a default ever drifts, the
/// identity above would still pass while meaning something else.
#[test]
fn every_axis_default_is_todays_value() {
    let d = AxisConfig::default();
    assert!(d.locality.is_undeclared());
    assert_eq!(d.survival, SurvivalGoal::Zone);
    assert_eq!(d.enforcement, Enforcement::Shadow);
    assert_eq!(d.read_locality, ReadLocality::Owner);
    assert_eq!(d.consistency, ReadConsistency::Strong);
}

/// The identity must not depend on the clock. A resolver that read time
/// directly would make this flap.
#[test]
fn the_identity_holds_at_every_instant() {
    let cfg = AxisConfig::default();
    for now in [0, 1, NOW, u64::MAX] {
        assert_eq!(resolve_visibility(&cfg, now), VisibilityPlan::today());
    }
}

/// And it must not depend on the request.
#[test]
fn the_identity_holds_for_every_shard() {
    let cfg = AxisConfig::default();
    for shard in [0, 1, 7, u32::MAX] {
        assert_eq!(
            resolve_route(&cfg, RequestContext { shard }),
            RoutePlan::today()
        );
    }
}

// ---------------------------------------------------------------------------
// Non-default arms actually differ — or "identity" is trivially true.
// ---------------------------------------------------------------------------

/// ⚠ **The control that makes the identity meaningful.** If the resolvers
/// returned today's plan for *every* input, the tests above would pass and
/// prove nothing. Each axis must be shown to move its plan.
#[test]
fn every_axis_changes_its_plan_when_set() {
    let base = AxisConfig::default();

    let region = AxisConfig {
        survival: SurvivalGoal::Region,
        ..base.clone()
    };
    assert_ne!(
        resolve_placement(&region, todays_replicas()),
        PlacementPlan::today(),
        "survival goal must reach the placement plan"
    );

    let enforce = AxisConfig {
        enforcement: Enforcement::Enforce,
        ..base.clone()
    };
    assert_ne!(
        resolve_placement(&enforce, todays_replicas()),
        PlacementPlan::today(),
        "enforcement must reach the placement plan"
    );

    let bounded = AxisConfig {
        consistency: ReadConsistency::Bounded {
            max_staleness_millis: 5_000,
        },
        ..base.clone()
    };
    assert_ne!(
        resolve_visibility(&bounded, NOW),
        VisibilityPlan::today(),
        "consistency must reach the visibility plan"
    );

    let nearest = AxisConfig {
        read_locality: ReadLocality::Nearest,
        consistency: ReadConsistency::Bounded {
            max_staleness_millis: 1,
        },
        ..base.clone()
    };
    assert_ne!(
        resolve_route(&nearest, RequestContext::default()),
        RoutePlan::today(),
        "read locality must reach the route plan"
    );

    let placed = AxisConfig {
        locality: Locality::new("us-central1", "us-central1-a"),
        ..base
    };
    let with_locality = vec![ReplicaSpec {
        id: "replica-0".into(),
        locality: placed.locality.clone(),
    }];
    assert_ne!(
        resolve_placement(&placed, with_locality),
        PlacementPlan::today(),
        "locality must reach the placement plan"
    );
}

// ---------------------------------------------------------------------------
// Composition rules.
// ---------------------------------------------------------------------------

/// ⭐ `Strong` forbids a follower read however the locality is set. A strong
/// read served from a merely-nearby replica is a wrong answer, not a fast
/// one — so the two axes compose with an AND, not an OR.
#[test]
fn strong_never_permits_a_follower_read_whatever_the_locality() {
    let cfg = AxisConfig {
        read_locality: ReadLocality::Nearest,
        consistency: ReadConsistency::Strong,
        ..Default::default()
    };
    let plan = resolve_route(&cfg, RequestContext::default());
    assert!(
        !plan.may_follower_read,
        "Strong + Nearest must not permit a follower read"
    );
    assert_eq!(plan, RoutePlan::today());
}

/// And the mirror: a stale-tolerant read still routes to the owner while the
/// locality says owner.
#[test]
fn bounded_alone_does_not_permit_a_follower_read() {
    let cfg = AxisConfig {
        read_locality: ReadLocality::Owner,
        consistency: ReadConsistency::Bounded {
            max_staleness_millis: 5_000,
        },
        ..Default::default()
    };
    assert!(!resolve_route(&cfg, RequestContext::default()).may_follower_read);
}

#[test]
fn nearest_plus_bounded_permits_a_follower_read() {
    let cfg = AxisConfig {
        read_locality: ReadLocality::Nearest,
        consistency: ReadConsistency::Bounded {
            max_staleness_millis: 5_000,
        },
        ..Default::default()
    };
    assert!(resolve_route(&cfg, RequestContext::default()).may_follower_read);
}

// ---------------------------------------------------------------------------
// Visibility arithmetic.
// ---------------------------------------------------------------------------

#[test]
fn bounded_requires_a_closed_timestamp_at_now_minus_the_allowance() {
    let cfg = AxisConfig {
        consistency: ReadConsistency::Bounded {
            max_staleness_millis: 5_000,
        },
        ..Default::default()
    };
    let v = resolve_visibility(&cfg, NOW);
    assert_eq!(v.require_closed_ts, Some(NOW - 5_000));
    assert_eq!(v.floor_hlc, None, "bounded pins no floor");
}

/// An allowance larger than the clock saturates to 0 rather than wrapping to
/// a colossal requirement that would refuse every read.
#[test]
fn an_allowance_past_the_epoch_saturates() {
    let cfg = AxisConfig {
        consistency: ReadConsistency::Bounded {
            max_staleness_millis: u64::MAX,
        },
        ..Default::default()
    };
    assert_eq!(resolve_visibility(&cfg, 1_000).require_closed_ts, Some(0));
}

#[test]
fn exact_pins_both_the_floor_and_the_requirement() {
    let cfg = AxisConfig {
        consistency: ReadConsistency::Exact { at_millis: 12_345 },
        ..Default::default()
    };
    let v = resolve_visibility(&cfg, NOW);
    assert_eq!(v.floor_hlc, Some(12_345));
    assert_eq!(v.require_closed_ts, Some(12_345));
    assert!(v.gates_anything());
}

// ---------------------------------------------------------------------------
// Placement passes the caller's replica set through untouched.
// ---------------------------------------------------------------------------

/// ⚠ An empty replica set is passed through, not silently defaulted.
/// `engine.rs:329` already refuses an empty set; inventing one here would
/// hide a misconfiguration behind a plausible-looking plan.
#[test]
fn an_empty_replica_set_is_passed_through_not_invented() {
    let plan = resolve_placement(&AxisConfig::default(), vec![]);
    assert!(plan.replicas.is_empty(), "must not fabricate replica-0");
}

#[test]
fn the_replica_set_is_carried_verbatim() {
    let replicas = vec![
        ReplicaSpec {
            id: "replica-0".into(),
            locality: Locality::new("us-central1", "a"),
        },
        ReplicaSpec {
            id: "replica-1".into(),
            locality: Locality::new("europe-west1", "b"),
        },
    ];
    let plan = resolve_placement(&AxisConfig::default(), replicas.clone());
    assert_eq!(plan.replicas, replicas);
}

#[test]
fn consistency_labels_are_stable_for_metric_pinning() {
    assert_eq!(ReadConsistency::Strong.label(), "strong");
    assert_eq!(
        ReadConsistency::Bounded {
            max_staleness_millis: 1
        }
        .label(),
        "bounded"
    );
    assert_eq!(ReadConsistency::Exact { at_millis: 1 }.label(), "exact");
}

/// E3 — no on-disk format is touched. `Locality` is the only serialisable
/// type here, and an undeclared one must serialise to nothing.
#[test]
fn an_undeclared_locality_serialises_to_nothing() {
    assert_eq!(
        serde_json::to_string(&Locality::undeclared()).unwrap(),
        "{}"
    );
}
