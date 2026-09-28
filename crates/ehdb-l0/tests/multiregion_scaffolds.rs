//! **M6 / M7 / M8 scaffolds** — flags defined off, activation refused.
//!
//! These capabilities are gated behind M5 (fencing enforcement). The property
//! under test is not what they do — they do nothing — but that they **say so**
//! rather than silently behaving like today.

use ehdb_l0::closed_timestamp::ClosedTimestamp;
use ehdb_l0::region_routing::{
    resolve_route, ReadLocality, ReplicaCandidate, RouteRefusal, RouteTarget,
};
use ehdb_l0::replica_targets::{parse_targets, ReplicaTargetSpec, TargetParseError};
use ehdb_l0::write_failover::{activate, FailoverMode, FailoverRefusal};

// ---------------------------------------------------------------------------
// M6 — read locality.
// ---------------------------------------------------------------------------

#[test]
fn read_locality_defaults_to_owner_which_is_today() {
    assert_eq!(ReadLocality::default(), ReadLocality::Owner);
    assert_eq!(ReadLocality::parse(None), ReadLocality::Owner);
    assert_eq!(
        resolve_route(ReadLocality::Owner, &[]),
        Ok(RouteTarget::Owner)
    );
}

#[test]
fn an_unrecognised_locality_falls_back_to_owner() {
    for junk in ["", " ", "near", "closest", "true", "1", "follower"] {
        assert_eq!(ReadLocality::parse(Some(junk)), ReadLocality::Owner);
    }
    assert_eq!(
        ReadLocality::parse(Some(" NEAREST ")),
        ReadLocality::Nearest
    );
}

/// ⭐ The scaffold property. `nearest` must refuse, not quietly serve from
/// the owner. A silent fallback is indistinguishable from a working feature
/// that happens to pick the owner every time — which is exactly the false
/// clean M6's own exit criterion E4 is written against.
#[test]
fn nearest_refuses_instead_of_silently_serving_from_the_owner() {
    let err = resolve_route(ReadLocality::Nearest, &[])
        .expect_err("nearest must not resolve while gated behind M5");
    assert_eq!(
        err,
        RouteRefusal::NotActivated {
            requested: "nearest"
        }
    );
    let msg = err.message();
    assert!(msg.contains("not implemented"), "{msg}");
    assert!(
        msg.contains("look taken"),
        "must explain the refusal: {msg}"
    );
}

/// ⚠ Unknown freshness is never near. A candidate with no closed timestamp
/// must not be eligible — same fail-closed posture as an undeclared failure
/// domain.
#[test]
fn a_candidate_with_unknown_freshness_is_not_eligible() {
    let unknown = ReplicaCandidate {
        id: "replica-1".into(),
        region: Some("europe-west1".into()),
        closed: None,
    };
    assert!(!unknown.freshness_is_known());

    let known = ReplicaCandidate {
        id: "replica-2".into(),
        region: Some("europe-west1".into()),
        closed: Some(ClosedTimestamp {
            shard: 0,
            closed_millis: 1_000,
            as_of_millis: 1_000,
        }),
    };
    assert!(known.freshness_is_known());
}

// ---------------------------------------------------------------------------
// M7 — replica target parsing.
// ---------------------------------------------------------------------------

#[test]
fn no_targets_configured_means_single_local_replica() {
    assert_eq!(parse_targets(None), Ok(vec![]));
    assert_eq!(parse_targets(Some("")), Ok(vec![]));
    assert_eq!(parse_targets(Some("   ")), Ok(vec![]));
}

#[test]
fn a_two_region_list_parses() {
    let got = parse_targets(Some(
        "id=replica-0,region=us-central1,zone=us-central1-a; \
         id=replica-1,region=europe-west1,uri=file:///mnt/eu",
    ))
    .expect("valid list");
    assert_eq!(
        got,
        vec![
            ReplicaTargetSpec {
                id: "replica-0".into(),
                region: Some("us-central1".into()),
                zone: Some("us-central1-a".into()),
                uri: None,
            },
            ReplicaTargetSpec {
                id: "replica-1".into(),
                region: Some("europe-west1".into()),
                zone: None,
                uri: Some("file:///mnt/eu".into()),
            },
        ]
    );
}

/// ⚠ An unknown key is refused, not ignored. A silently-dropped key is a
/// setting the operator believes is applied.
#[test]
fn an_unknown_key_is_refused_rather_than_dropped() {
    let err = parse_targets(Some("id=a,regoin=us-central1")).expect_err("typo must be refused");
    assert!(matches!(err, TargetParseError::UnknownKey { .. }));
    assert!(err.message().contains("regoin"));
    assert!(err.message().contains("believes is applied"));
}

#[test]
fn duplicate_and_missing_ids_are_refused() {
    assert!(matches!(
        parse_targets(Some("id=a,region=r1;id=a,region=r2")),
        Err(TargetParseError::DuplicateId { .. })
    ));
    assert!(matches!(
        parse_targets(Some("region=us-central1")),
        Err(TargetParseError::MissingId { .. })
    ));
    assert!(matches!(
        parse_targets(Some("id=,region=us-central1")),
        Err(TargetParseError::EmptyId { .. })
    ));
}

/// Empty values read as absent rather than as an empty string, so
/// `region=` does not produce a region literally named "".
#[test]
fn an_empty_value_is_absent_not_empty_string() {
    let got = parse_targets(Some("id=a,region=,zone=")).expect("valid");
    assert_eq!(got[0].region, None);
    assert_eq!(got[0].zone, None);
}

// ---------------------------------------------------------------------------
// M8 — write failover.
// ---------------------------------------------------------------------------

#[test]
fn failover_defaults_to_off_and_off_is_the_only_mode_that_arms() {
    assert_eq!(FailoverMode::default(), FailoverMode::Off);
    assert_eq!(FailoverMode::parse(None), FailoverMode::Off);
    assert_eq!(activate(FailoverMode::Off), Ok(()));
}

#[test]
fn an_unrecognised_failover_mode_falls_back_to_off() {
    for junk in ["", " ", "on", "true", "1", "automatic", "man"] {
        assert_eq!(
            FailoverMode::parse(Some(junk)),
            FailoverMode::Off,
            "unrecognised mode {junk:?} must not arm a failover"
        );
    }
}

/// ⭐ Both non-off modes refuse, and the message names the actual blocker —
/// a per-cluster lease authority — rather than a generic "unimplemented".
/// Arming this would ship a failover that cannot fire in the one scenario it
/// exists for.
#[test]
fn manual_and_auto_both_refuse_naming_the_lease_authority() {
    for mode in [FailoverMode::Manual, FailoverMode::Auto] {
        let err = activate(mode).expect_err("must refuse while the fork is open");
        assert_eq!(
            err,
            FailoverRefusal::BlockedOnLeaseAuthority {
                requested: mode.as_str()
            }
        );
        let msg = err.message();
        assert!(msg.contains("per-cluster"), "must name the blocker: {msg}");
        assert!(msg.contains("cannot fire"), "{msg}");
    }
}

/// ⚠ Control across all three scaffolds: the DEFAULT of each is the only
/// value that succeeds. If a scaffold ever starts accepting its active mode
/// without the gate being lifted, exactly one of these flips.
#[test]
fn every_scaffold_admits_only_its_default() {
    assert!(resolve_route(ReadLocality::Owner, &[]).is_ok());
    assert!(resolve_route(ReadLocality::Nearest, &[]).is_err());
    assert!(activate(FailoverMode::Off).is_ok());
    assert!(activate(FailoverMode::Manual).is_err());
    assert!(activate(FailoverMode::Auto).is_err());
    // M7 is config parsing, not an activation gate: an empty list is today.
    assert_eq!(parse_targets(None).map(|v| v.len()), Ok(0));
}
