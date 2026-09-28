//! **Closed timestamps and read freshness** (multi-region spec M3).
//! Predicate only — nothing here is on a serving path or the append path.

use ehdb_l0::closed_timestamp::{
    admits, closed_timestamp_for, ClosedTimestamp, FreshnessRefusal, ReadConsistency,
    DEFAULT_MAX_READING_AGE_MILLIS,
};
use ehdb_l0::unreplicated::ShardUnreplicated;

const NOW: u64 = 1_789_000_000_000;

fn ct(closed: u64, as_of: u64) -> ClosedTimestamp {
    ClosedTimestamp {
        shard: 0,
        closed_millis: closed,
        as_of_millis: as_of,
    }
}

// ---------------------------------------------------------------------------
// Derivation from the unreplicated window.
// ---------------------------------------------------------------------------

#[test]
fn a_shard_with_nothing_pending_is_closed_up_to_now() {
    // `unreplicated.rs` is explicit that 0 here is "a real reading and not an
    // absence", so this must mean fully closed, not unknown.
    let s = ShardUnreplicated {
        shard: 3,
        oldest_age_millis: 0,
        ..Default::default()
    };
    let c = closed_timestamp_for(&s, NOW);
    assert_eq!(c.shard, 3);
    assert_eq!(c.closed_millis, NOW);
    assert_eq!(c.as_of_millis, NOW);
}

#[test]
fn the_oldest_pending_record_sets_the_closed_point() {
    let s = ShardUnreplicated {
        shard: 0,
        oldest_age_millis: 2_500,
        ..Default::default()
    };
    assert_eq!(closed_timestamp_for(&s, NOW).closed_millis, NOW - 2_500);
}

/// ⚠ `as_of` must always be populated. A closed timestamp without one is a
/// number a consumer cannot age-check — the `noetl.execution.status` shape.
#[test]
fn every_derived_timestamp_carries_its_as_of() {
    for age in [0, 1, 5_000, u64::MAX] {
        let s = ShardUnreplicated {
            shard: 1,
            oldest_age_millis: age,
            ..Default::default()
        };
        assert_eq!(closed_timestamp_for(&s, NOW).as_of_millis, NOW);
    }
}

/// An absurd age saturates rather than wrapping to a huge closed point — the
/// failure direction matters: wrapping would claim freshness far in the future.
#[test]
fn an_absurd_age_saturates_toward_zero_not_into_the_future() {
    let s = ShardUnreplicated {
        shard: 0,
        oldest_age_millis: u64::MAX,
        ..Default::default()
    };
    assert_eq!(closed_timestamp_for(&s, NOW).closed_millis, 0);
}

// ---------------------------------------------------------------------------
// E4 — `strong` is today and consults nothing.
// ---------------------------------------------------------------------------

/// ⭐ The identity property. Under the default, the gate never refuses —
/// including on inputs that would refuse every other mode. Today's read path
/// cannot acquire a dependency on a number it has never needed.
#[test]
fn strong_is_always_admitted_even_on_hopeless_inputs() {
    assert_eq!(ReadConsistency::default(), ReadConsistency::Strong);
    let hopeless = ct(0, 0); // closed at epoch, read taken at epoch
    assert!(admits(
        &hopeless,
        ReadConsistency::Strong,
        NOW,
        DEFAULT_MAX_READING_AGE_MILLIS
    )
    .is_ok());
}

// ---------------------------------------------------------------------------
// E3 — an unsatisfiable request is REFUSED, never silently served stale.
// ---------------------------------------------------------------------------

#[test]
fn bounded_admits_a_replica_inside_the_allowance() {
    let c = ct(NOW - 1_000, NOW);
    assert!(admits(
        &c,
        ReadConsistency::Bounded {
            max_staleness_millis: 5_000
        },
        NOW,
        DEFAULT_MAX_READING_AGE_MILLIS
    )
    .is_ok());
}

#[test]
fn bounded_refuses_a_replica_outside_the_allowance() {
    let c = ct(NOW - 9_000, NOW);
    let err = admits(
        &c,
        ReadConsistency::Bounded {
            max_staleness_millis: 5_000,
        },
        NOW,
        DEFAULT_MAX_READING_AGE_MILLIS,
    )
    .expect_err("must refuse, not serve stale");
    assert!(matches!(err, FreshnessRefusal::TooStale { .. }));
    assert!(err.message().contains("9000"));
}

/// The boundary is inclusive: exactly at the allowance is still admitted.
#[test]
fn the_staleness_boundary_is_inclusive() {
    let c = ct(NOW - 5_000, NOW);
    let cons = ReadConsistency::Bounded {
        max_staleness_millis: 5_000,
    };
    assert!(admits(&c, cons, NOW, DEFAULT_MAX_READING_AGE_MILLIS).is_ok());
    let c_over = ct(NOW - 5_001, NOW);
    assert!(admits(&c_over, cons, NOW, DEFAULT_MAX_READING_AGE_MILLIS).is_err());
}

#[test]
fn exact_admits_a_point_already_closed() {
    let c = ct(NOW, NOW);
    assert!(admits(
        &c,
        ReadConsistency::Exact {
            at_millis: NOW - 100
        },
        NOW,
        DEFAULT_MAX_READING_AGE_MILLIS
    )
    .is_ok());
}

#[test]
fn exact_refuses_a_point_not_yet_closed() {
    let c = ct(NOW - 1_000, NOW);
    let err = admits(
        &c,
        ReadConsistency::Exact { at_millis: NOW },
        NOW,
        DEFAULT_MAX_READING_AGE_MILLIS,
    )
    .expect_err("must refuse a point the replica has not closed");
    assert!(matches!(err, FreshnessRefusal::NotYetClosed { .. }));
}

// ---------------------------------------------------------------------------
// The reading's own age — "unknown" must not read as "fresh".
// ---------------------------------------------------------------------------

/// ⭐ The subtle one. The closed timestamp says the replica was fresh — but
/// the reading is ancient, so we do not actually know anything current about
/// it. Admitting here would treat *unknown* as *fresh*, which is the whole
/// class of defect this module exists to prevent.
#[test]
fn a_stale_reading_is_refused_even_when_it_claims_freshness() {
    // Claims closed-up-to-now, but was computed a minute ago.
    let c = ct(NOW, NOW - 60_000);
    let err = admits(
        &c,
        ReadConsistency::Bounded {
            max_staleness_millis: u64::MAX, // caller would accept ANY staleness
        },
        NOW,
        DEFAULT_MAX_READING_AGE_MILLIS,
    )
    .expect_err("an ancient reading is not evidence of freshness");
    assert!(matches!(err, FreshnessRefusal::ReadingTooOld { .. }));
    assert!(err.message().contains("unknown, not proven"));
}

/// ⚠ Control for the test above: with a *fresh* reading and the same
/// permissive staleness, the read IS admitted. Without this, the refusal
/// above could be coming from something other than the reading's age.
#[test]
fn a_fresh_reading_with_the_same_allowance_is_admitted() {
    let c = ct(NOW - 100_000, NOW); // very stale DATA, fresh READING
    assert!(admits(
        &c,
        ReadConsistency::Bounded {
            max_staleness_millis: u64::MAX
        },
        NOW,
        DEFAULT_MAX_READING_AGE_MILLIS
    )
    .is_ok());
}

/// The reading-age gate applies to exact reads too.
#[test]
fn the_reading_age_gate_applies_to_exact_reads() {
    let c = ct(NOW, NOW - 60_000);
    assert!(matches!(
        admits(
            &c,
            ReadConsistency::Exact { at_millis: 0 },
            NOW,
            DEFAULT_MAX_READING_AGE_MILLIS
        ),
        Err(FreshnessRefusal::ReadingTooOld { .. })
    ));
}

/// A clock that moved backwards between computation and interrogation must
/// report 0, not wrap to ~584 million years and refuse everything.
#[test]
fn a_backwards_clock_saturates_rather_than_wrapping() {
    let c = ct(NOW, NOW);
    assert_eq!(c.staleness_millis(NOW - 5_000), 0);
    assert_eq!(c.reading_age_millis(NOW - 5_000), 0);
    assert!(admits(
        &c,
        ReadConsistency::Bounded {
            max_staleness_millis: 0
        },
        NOW - 5_000,
        DEFAULT_MAX_READING_AGE_MILLIS
    )
    .is_ok());
}

#[test]
fn labels_are_stable_for_metric_pinning() {
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
