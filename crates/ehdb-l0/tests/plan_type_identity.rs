//! The M0/M1 convergence, held by the compiler rather than by an audit.
//!
//! M0 (`ehdb-core::plan`) and M1/M3/M4 (`ehdb-l0`) were written in parallel and
//! each declared `Locality`, `SurvivalGoal`, `ReadConsistency`, `ReadLocality`
//! and `RouteTarget`. Five pairs of structurally identical types that must stay
//! identical is a representation that drifts — and two of them carry serde
//! shapes a rollback binary depends on, so a divergence would be a wire
//! incompatibility rather than a tidiness problem.
//!
//! The l0 names are now re-exports. These assertions fail to COMPILE if anyone
//! reintroduces a local definition, which is the only form of this check that
//! cannot itself go stale: a grep-based guard would keep passing against a
//! renamed module, and an audit only runs when someone remembers to run it.
//!
//! ⚠ A same-shaped-but-distinct type would satisfy a `PartialEq`-style test and
//! fail these, which is the point: this is assignment across the two paths, so
//! only genuine type identity passes.

use ehdb_core::plan;

#[test]
fn the_l0_names_are_the_core_types_not_copies_of_them() {
    // Assignment in both directions. One direction alone would pass for a type
    // that merely derefs or converts.
    let a: plan::Locality = ehdb_l0::placement::Locality::new("us-central1", "us-central1-a");
    let b: ehdb_l0::placement::Locality = a.clone();
    assert_eq!(a, b);

    let c: plan::SurvivalGoal = ehdb_l0::failure_domain::SurvivalGoal::Region;
    let d: ehdb_l0::failure_domain::SurvivalGoal = c;
    assert_eq!(c, d);

    let e: plan::ReadConsistency = ehdb_l0::closed_timestamp::ReadConsistency::Strong;
    let f: ehdb_l0::closed_timestamp::ReadConsistency = e;
    assert_eq!(e, f);

    let g: plan::ReadLocality = ehdb_l0::region_routing::ReadLocality::Owner;
    let h: ehdb_l0::region_routing::ReadLocality = g;
    assert_eq!(g, h);

    let i: plan::RouteTarget = ehdb_l0::region_routing::RouteTarget::Owner;
    let j: ehdb_l0::region_routing::RouteTarget = i.clone();
    assert_eq!(i, j);
}

/// The methods that moved from l0 to core moved with their BEHAVIOUR, not just
/// their names. Each of these encodes a refusal the l0 version documented.
#[test]
fn the_moved_methods_kept_their_documented_refusals() {
    // Locality::parse refuses an unknown key rather than dropping it.
    assert!(plan::Locality::parse("region=a,zone=b").is_ok());
    let err = plan::Locality::parse("regionn=a").unwrap_err();
    assert!(
        err.contains("unknown locality key"),
        "an unknown key must be REFUSED, not ignored: {err}"
    );

    // SurvivalGoal::parse fails safe to Zone, never widening the claim.
    assert_eq!(
        plan::SurvivalGoal::parse("region"),
        plan::SurvivalGoal::Region
    );
    assert_eq!(
        plan::SurvivalGoal::parse("REGION"),
        plan::SurvivalGoal::Region
    );
    assert_eq!(
        plan::SurvivalGoal::parse("regionn"),
        plan::SurvivalGoal::Zone,
        "a typo must not silently widen what we claim to survive"
    );

    // ReadLocality::parse fails safe to Owner.
    assert_eq!(
        plan::ReadLocality::parse(Some("nearest")),
        plan::ReadLocality::Nearest
    );
    assert_eq!(
        plan::ReadLocality::parse(Some("neerest")),
        plan::ReadLocality::Owner,
        "a typo must not move the read path"
    );
    assert_eq!(plan::ReadLocality::parse(None), plan::ReadLocality::Owner);

    // provably_different_region returns FALSE on undeclared — "cannot be shown
    // different" is not "is different".
    let declared = plan::Locality::new("r1", "z1");
    let other = plan::Locality::new("r2", "z1");
    let undeclared = plan::Locality::undeclared();
    assert!(declared.provably_different_region(&other));
    assert!(
        !declared.provably_different_region(&undeclared),
        "an undeclared region must never count as provably different"
    );
}

/// The env readers stayed in l0 as free functions, and still work.
///
/// ⚠ Not run with a set variable: `cargo test` does not serialise tests, so a
/// `set_var` here would race every sibling. The unset path is the one that
/// matters anyway — it is what every deployment runs today.
#[test]
fn the_env_readers_default_to_todays_behaviour() {
    if std::env::var(ehdb_l0::placement::LOCALITY_ENV).is_err() {
        assert!(ehdb_l0::placement::locality_from_env()
            .unwrap()
            .is_undeclared());
    }
    if std::env::var(ehdb_l0::region_routing::READ_LOCALITY_ENV).is_err() {
        assert_eq!(
            ehdb_l0::region_routing::read_locality_from_env(),
            plan::ReadLocality::Owner
        );
    }
}
