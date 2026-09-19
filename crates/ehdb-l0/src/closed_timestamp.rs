//! **Closed timestamps and read freshness** (multi-region spec M3).
//!
//! Read-side only. Nothing here is on the append path and nothing here is
//! wired into a serving path yet — this is the predicate a read routes on.
//!
//! ## What a closed timestamp is
//!
//! The point up to which a replica's contents are known complete: no record
//! with a position at or below it can still arrive. A read asking for data "as
//! of T" is safe on this replica exactly when `T <= closed`.
//!
//! ## Where the number comes from
//!
//! [`UnreplicatedTracker`](crate::unreplicated::UnreplicatedTracker) already
//! computes the input: per shard, the age of the **oldest acknowledged record
//! not yet durable on the substrate**, measured from the *append*.
//!
//! ⚠ Deliberately **not** `L0Metrics::upload_lag_micros_total`. That is
//! accumulated as `job.sealed_at.elapsed()` — measured from **seal** — so a
//! record sitting in an unsealed active part contributes nothing to it. On a
//! quiet shard the pre-seal term dominates, which is precisely the case a
//! seal-based number is blind to. `unreplicated.rs` says it in its own words:
//! *"A dashboard built on it reads healthy in precisely the scenario where
//! events sit unreplicated."* A closed timestamp built on that metric would
//! claim freshness exactly when it is most wrong.
//!
//! ## ⚠ `as_of` is not decoration
//!
//! Every [`ClosedTimestamp`] carries when it was computed. A number with no
//! staleness signal is not evidence — `noetl.execution.status` read exactly
//! like live data for months after its writer was retired. A consumer that
//! cannot tell a fresh closed timestamp from a stale one has learned nothing.
//!
//! ## Units
//!
//! Positions are **milliseconds since the Unix epoch**, matching the
//! `UnreplicatedTracker` age input. When the HLC lands (M2, `ehdb-core`), a
//! closed timestamp becomes `Hlc::physical_millis()` and this module's
//! arithmetic is unchanged — the conversion is one call at the boundary.

use crate::unreplicated::ShardUnreplicated;

/// How fresh a read requires its data to be.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ReadConsistency {
    /// Today: serve from the owner, no freshness predicate. **Default**, so
    /// adding this type changes nothing by itself.
    #[default]
    Strong,
    /// Serve from any replica whose closed timestamp is no older than
    /// `max_staleness_millis`.
    Bounded { max_staleness_millis: u64 },
    /// Serve a snapshot at exactly `at_millis`. Requires `closed >= at_millis`.
    Exact { at_millis: u64 },
}

impl ReadConsistency {
    pub fn label(&self) -> &'static str {
        match self {
            Self::Strong => "strong",
            Self::Bounded { .. } => "bounded",
            Self::Exact { .. } => "exact",
        }
    }
}

/// One shard's closed timestamp, with the instant it was computed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ClosedTimestamp {
    pub shard: u32,
    /// Everything at or below this position is complete here.
    pub closed_millis: u64,
    /// ⚠ When this was computed. Never drop it: see the module note.
    pub as_of_millis: u64,
}

impl ClosedTimestamp {
    /// How stale this reading is, relative to `now_millis`.
    ///
    /// Saturating, so a clock that moved backwards between computation and
    /// interrogation reports `0` rather than wrapping to an enormous age.
    pub fn staleness_millis(&self, now_millis: u64) -> u64 {
        now_millis.saturating_sub(self.closed_millis)
    }

    /// How old the *reading itself* is — distinct from how stale the data is.
    /// A recent reading of an old closed timestamp is trustworthy; an ancient
    /// reading of a recent one is not.
    pub fn reading_age_millis(&self, now_millis: u64) -> u64 {
        now_millis.saturating_sub(self.as_of_millis)
    }
}

/// Derive a shard's closed timestamp from its unreplicated window.
///
/// `now_millis` minus the age of the oldest not-yet-durable record is the
/// point below which nothing is outstanding. A shard with nothing pending
/// (`oldest_age_millis == 0`) is closed **up to now** — and `unreplicated.rs`
/// is explicit that a `0` there is *"a real reading and not an absence"*.
pub fn closed_timestamp_for(shard: &ShardUnreplicated, now_millis: u64) -> ClosedTimestamp {
    ClosedTimestamp {
        shard: shard.shard,
        closed_millis: now_millis.saturating_sub(shard.oldest_age_millis),
        as_of_millis: now_millis,
    }
}

/// Why a read could not be served at the requested freshness.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FreshnessRefusal {
    /// The replica is further behind than the caller allows.
    TooStale {
        shard: u32,
        requested_max_staleness_millis: u64,
        actual_staleness_millis: u64,
    },
    /// An exact-staleness read asked for a point this replica has not closed.
    NotYetClosed {
        shard: u32,
        requested_millis: u64,
        closed_millis: u64,
    },
    /// The closed-timestamp reading is itself too old to act on.
    ///
    /// ⚠ Distinct from `TooStale`, and the distinction is the point: a stale
    /// *reading* means we do not know how fresh the replica is. Treating
    /// "unknown" as "fresh" is the failure this whole module exists to avoid.
    ReadingTooOld {
        shard: u32,
        reading_age_millis: u64,
        max_reading_age_millis: u64,
    },
}

impl FreshnessRefusal {
    pub fn message(&self) -> String {
        match self {
            Self::TooStale {
                shard,
                requested_max_staleness_millis,
                actual_staleness_millis,
            } => format!(
                "shard {shard} is {actual_staleness_millis}ms behind, caller allows \
                 {requested_max_staleness_millis}ms"
            ),
            Self::NotYetClosed {
                shard,
                requested_millis,
                closed_millis,
            } => format!(
                "shard {shard} has closed only through {closed_millis}, read asked for \
                 {requested_millis}"
            ),
            Self::ReadingTooOld {
                shard,
                reading_age_millis,
                max_reading_age_millis,
            } => format!(
                "shard {shard}'s closed-timestamp reading is {reading_age_millis}ms old \
                 (limit {max_reading_age_millis}ms): freshness is unknown, not proven"
            ),
        }
    }
}

/// How old a closed-timestamp reading may be before it stops counting as
/// evidence. Deliberately conservative.
pub const DEFAULT_MAX_READING_AGE_MILLIS: u64 = 10_000;

/// ⭐ **The gate.** Whether `closed` permits serving a read at `consistency`.
///
/// `Ok(())` means serve. `Err` means **refuse** — never "serve it anyway and
/// hope". A silently-stale read is a wrong answer, not a slow one, and it is
/// indistinguishable from a correct one at the call site.
pub fn admits(
    closed: &ClosedTimestamp,
    consistency: ReadConsistency,
    now_millis: u64,
    max_reading_age_millis: u64,
) -> Result<(), FreshnessRefusal> {
    // `Strong` does not consult a closed timestamp at all: it is served by the
    // owner, which is complete by construction. Checking it here would make
    // today's read path depend on a number it has never needed.
    if matches!(consistency, ReadConsistency::Strong) {
        return Ok(());
    }

    let reading_age = closed.reading_age_millis(now_millis);
    if reading_age > max_reading_age_millis {
        return Err(FreshnessRefusal::ReadingTooOld {
            shard: closed.shard,
            reading_age_millis: reading_age,
            max_reading_age_millis,
        });
    }

    match consistency {
        ReadConsistency::Strong => unreachable!("handled above"),
        ReadConsistency::Bounded {
            max_staleness_millis,
        } => {
            let actual = closed.staleness_millis(now_millis);
            if actual > max_staleness_millis {
                Err(FreshnessRefusal::TooStale {
                    shard: closed.shard,
                    requested_max_staleness_millis: max_staleness_millis,
                    actual_staleness_millis: actual,
                })
            } else {
                Ok(())
            }
        }
        ReadConsistency::Exact { at_millis } => {
            if at_millis > closed.closed_millis {
                Err(FreshnessRefusal::NotYetClosed {
                    shard: closed.shard,
                    requested_millis: at_millis,
                    closed_millis: closed.closed_millis,
                })
            } else {
                Ok(())
            }
        }
    }
}
