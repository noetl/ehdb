//! **Replica locality** (multi-region spec M1). Type + serde compat only —
//! nothing places, nothing routes, nothing is on the write path.

use ehdb_l0::placement::Locality;

#[test]
fn undeclared_is_the_default_and_knows_it() {
    let l = Locality::default();
    assert!(l.is_undeclared());
    assert_eq!(l, Locality::undeclared());
}

/// ⭐ The compat property. An undeclared locality must serialise to `{}` —
/// nothing at all — so embedding one in a manifest cannot change the bytes a
/// rollback binary reads.
#[test]
fn an_undeclared_locality_serialises_to_nothing() {
    assert_eq!(
        serde_json::to_string(&Locality::undeclared()).unwrap(),
        "{}"
    );
    // And a half-declared one omits only the absent half.
    let half = Locality {
        region: Some("us-central1".into()),
        zone: None,
    };
    assert_eq!(
        serde_json::to_string(&half).unwrap(),
        r#"{"region":"us-central1"}"#
    );
}

#[test]
fn a_declared_locality_round_trips() {
    let l = Locality::new("us-central1", "us-central1-a");
    let back: Locality = serde_json::from_str(&serde_json::to_string(&l).unwrap()).unwrap();
    assert_eq!(l, back);
    assert!(!l.is_undeclared());
}

#[test]
fn missing_fields_deserialise_as_undeclared() {
    assert!(serde_json::from_str::<Locality>("{}")
        .unwrap()
        .is_undeclared());
}

#[test]
fn parse_reads_region_and_zone() {
    let l = Locality::parse("region=us-central1,zone=us-central1-a").unwrap();
    assert_eq!(l, Locality::new("us-central1", "us-central1-a"));
    assert_eq!(
        Locality::parse("  REGION = europe-west1 ")
            .unwrap()
            .region
            .as_deref(),
        Some("europe-west1")
    );
    assert!(Locality::parse("").unwrap().is_undeclared());
}

/// ⚠ An unknown key is refused. A dropped key is a setting the operator
/// believes is applied.
#[test]
fn parse_refuses_an_unknown_key() {
    let err = Locality::parse("region=us-central1,regoin=eu").expect_err("typo must be refused");
    assert!(err.contains("regoin"), "{err}");
    assert!(err.contains("believes is applied"), "{err}");
    assert!(
        Locality::parse("us-central1").is_err(),
        "a bare value is not key=value"
    );
}

#[test]
fn an_empty_value_is_absent_not_empty_string() {
    let l = Locality::parse("region=,zone=").unwrap();
    assert!(
        l.is_undeclared(),
        "region= must mean absent, not a region named \"\""
    );
}

// ---------------------------------------------------------------------------
// provably_different_region — the fail-closed direction.
// ---------------------------------------------------------------------------

#[test]
fn two_declared_distinct_regions_are_provably_different() {
    let a = Locality::new("us-central1", "a");
    let b = Locality::new("europe-west1", "b");
    assert!(a.provably_different_region(&b));
    assert!(b.provably_different_region(&a));
}

#[test]
fn the_same_region_is_not_different_even_in_different_zones() {
    let a = Locality::new("us-central1", "us-central1-a");
    let b = Locality::new("us-central1", "us-central1-b");
    assert!(!a.provably_different_region(&b));
}

/// ⭐ The fail-closed case, and the one that matters. An undeclared side
/// cannot be *shown* different, and for a placement decision that must be
/// treated the same as "not different" — acting on an unproven difference is
/// how an RF of N over one domain gets called an RF of N.
#[test]
fn an_undeclared_side_is_never_provably_different() {
    let declared = Locality::new("us-central1", "a");
    let nothing = Locality::undeclared();
    assert!(!declared.provably_different_region(&nothing));
    assert!(!nothing.provably_different_region(&declared));
    assert!(!nothing.provably_different_region(&Locality::undeclared()));

    // Even a declared ZONE does not make the region provable.
    let zone_only = Locality {
        region: None,
        zone: Some("us-central1-b".into()),
    };
    assert!(!declared.provably_different_region(&zone_only));
}
