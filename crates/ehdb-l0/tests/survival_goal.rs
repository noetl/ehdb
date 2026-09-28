//! **Survival goals** (multi-region spec M4). Validation only — nothing here is
//! wired into the engine, and nothing touches the write path.
//!
//! The property under test is the one `failure_domain.rs`'s module header
//! already names for domains, restated for regions: *a region goal over one
//! region is a zone goal wearing a larger name.*

use ehdb_l0::failure_domain::{
    check_region_survival, validate_region_survival, RegionPlacement, RegionViolation, SurvivalGoal,
};

fn r(replica: &str, region: Option<&str>) -> RegionPlacement {
    RegionPlacement {
        replica: replica.to_string(),
        region: region.map(str::to_string),
    }
}

// ---------------------------------------------------------------------------
// E1 — `zone` is today's behaviour: this check contributes nothing.
// ---------------------------------------------------------------------------

/// ⭐ The identity property. Under the default goal the region check is inert,
/// so adding it cannot change any existing deployment.
#[test]
fn zone_goal_never_reports_a_region_violation() {
    // Deliberately the worst possible input for a region goal: one replica,
    // no declared region. Under `Zone` it must still be silent.
    let one = vec![r("replica-0", None)];
    assert!(check_region_survival(&one, SurvivalGoal::Zone).is_empty());

    let same_region = vec![
        r("replica-0", Some("us-central1")),
        r("replica-1", Some("us-central1")),
    ];
    assert!(check_region_survival(&same_region, SurvivalGoal::Zone).is_empty());
    assert!(validate_region_survival(&same_region, SurvivalGoal::Zone).is_ok());
}

/// `Zone` is the default, so a caller that never mentions a goal gets today.
#[test]
fn default_goal_is_zone() {
    assert_eq!(SurvivalGoal::default(), SurvivalGoal::Zone);
}

// ---------------------------------------------------------------------------
// E2 — under `region`, a same-region set is REFUSED.
// ---------------------------------------------------------------------------

#[test]
fn region_goal_refuses_two_replicas_in_one_region() {
    let v = check_region_survival(
        &[
            r("replica-0", Some("us-central1")),
            r("replica-1", Some("us-central1")),
        ],
        SurvivalGoal::Region,
    );
    assert_eq!(v.len(), 1, "expected exactly one violation, got {v:?}");
    assert!(matches!(v[0], RegionViolation::SharedRegion { .. }));
    assert!(v[0].message().contains("us-central1"));
    assert!(validate_region_survival(
        &[r("a", Some("us-central1")), r("b", Some("us-central1"))],
        SurvivalGoal::Region
    )
    .is_err());
}

#[test]
fn region_goal_accepts_two_replicas_in_different_regions() {
    let v = check_region_survival(
        &[
            r("replica-0", Some("us-central1")),
            r("replica-1", Some("europe-west1")),
        ],
        SurvivalGoal::Region,
    );
    assert!(v.is_empty(), "distinct regions must pass, got {v:?}");
}

/// Fail-closed, matching `FailureDomain::Undeclared`: silence is not spread.
#[test]
fn region_goal_refuses_an_undeclared_region() {
    let v = check_region_survival(
        &[r("replica-0", Some("us-central1")), r("replica-1", None)],
        SurvivalGoal::Region,
    );
    assert!(v
        .iter()
        .any(|x| matches!(x, RegionViolation::UndeclaredRegion { .. })));
}

/// ⚠ The production shape today: **one** replica. A region goal is unmeetable
/// by construction and must say so rather than passing vacuously.
///
/// This is the test that separates "no violations because it is fine" from
/// "no violations because there was nothing to compare" — the vacuous-pass
/// class that has bitten this program before.
#[test]
fn region_goal_refuses_a_single_replica_rather_than_passing_vacuously() {
    let v = check_region_survival(&[r("replica-0", Some("us-central1"))], SurvivalGoal::Region);
    assert_eq!(v, vec![RegionViolation::NotEnoughReplicas { have: 1 }]);

    let empty = check_region_survival(&[], SurvivalGoal::Region);
    assert_eq!(empty, vec![RegionViolation::NotEnoughReplicas { have: 0 }]);
}

/// Every violation, not the first — same contract as `check_replica_domains`.
#[test]
fn all_violations_are_reported_not_just_the_first() {
    let v = check_region_survival(
        &[
            r("a", Some("us-central1")),
            r("b", Some("us-central1")),
            r("c", None),
            r("d", Some("us-central1")),
        ],
        SurvivalGoal::Region,
    );
    assert_eq!(
        v.len(),
        3,
        "expected 2 shared-region + 1 undeclared, got {v:?}"
    );
}

// ---------------------------------------------------------------------------
// Config parsing — fail-safe direction.
// ---------------------------------------------------------------------------

/// ⚠ A typo must narrow, never widen. `SurvivalGoal::parse("regoin")` claiming
/// region survival would be a config error that reads as a stronger guarantee.
#[test]
fn an_unrecognised_goal_parses_to_zone_not_region() {
    assert_eq!(SurvivalGoal::parse("region"), SurvivalGoal::Region);
    assert_eq!(SurvivalGoal::parse("  REGION "), SurvivalGoal::Region);
    assert_eq!(SurvivalGoal::parse("zone"), SurvivalGoal::Zone);
    for typo in ["regoin", "", "  ", "rgion", "true", "1", "Region!"] {
        assert_eq!(
            SurvivalGoal::parse(typo),
            SurvivalGoal::Zone,
            "unrecognised goal {typo:?} must fail safe to Zone"
        );
    }
}

/// The error message has to name the goal, or an operator reading a failed
/// engine open cannot tell which policy refused them.
#[test]
fn the_error_names_the_goal_and_the_reason() {
    let err = validate_region_survival(
        &[r("a", Some("us-central1")), r("b", Some("us-central1"))],
        SurvivalGoal::Region,
    )
    .expect_err("must refuse");
    let msg = err.to_string();
    assert!(msg.contains("region"), "should name the goal: {msg}");
    assert!(
        msg.contains('a') && msg.contains('b'),
        "should name the replicas: {msg}"
    );
}
