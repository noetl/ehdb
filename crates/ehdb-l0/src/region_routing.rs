//! **Read-locality routing / follower reads** (multi-region spec M6).
//!
//! ## ⛔ SCAFFOLD — gated behind M5, not activated
//!
//! Flag defined, types conforming to the spec, **no routing performed**.
//! [`resolve_route`] refuses [`ReadLocality::Nearest`] rather than quietly
//! serving from the owner, because a scaffold that silently does today's
//! thing is indistinguishable from a working feature that happens to pick the
//! owner every time — and that is the failure M6's own exit criterion E4 is
//! written against ("a run where `candidates=0` on every read is a FAILED
//! exit, not a passing one").
//!
//! ⚠ **Why M5 gates this.** Serving a read from a non-owner replica is only
//! sound while exactly one writer owns the shard. Until fencing is enforcing
//! (M5), single-writer rests on `replicas: 1` — an orchestration preference,
//! not a mutual-exclusion primitive. A follower read under two writers can
//! return a prefix of a log that another node is concurrently diverging from.
//!
//! The freshness half already exists and is real: see
//! [`crate::closed_timestamp`] (M3). This module is only the *selection*.

use crate::closed_timestamp::ClosedTimestamp;

/// Env var selecting read routing.
pub const READ_LOCALITY_ENV: &str = "NOETL_EHDB_READ_LOCALITY";

/// Where a read may be served from.
/// Which replica a read may be served from.
///
/// ⭐ Re-exported from [`ehdb_core::plan`], not redefined — see the note on
/// [`crate::placement::Locality`]. `Owner` is today's behaviour and the
/// default; `Nearest` is ⛔ not implemented.
pub use ehdb_core::plan::ReadLocality;

/// Read [`ReadLocality`] from [`READ_LOCALITY_ENV`].
///
/// ⚠ A free function for the same reason as [`crate::placement::locality_from_env`].
pub fn read_locality_from_env() -> ReadLocality {
    ReadLocality::parse(std::env::var(READ_LOCALITY_ENV).ok().as_deref())
}

/// A replica a read could be served from (spec M6 shape).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicaCandidate {
    pub id: String,
    /// Declared region. `None` is never treated as near.
    pub region: Option<String>,
    /// ⚠ `None` means **freshness unknown**, and must never be treated as
    /// fresh — same fail-closed posture as `FailureDomain::Undeclared`.
    pub closed: Option<ClosedTimestamp>,
}

impl ReplicaCandidate {
    /// Eligible only when freshness is *known*. Unknown is not a soft yes.
    pub fn freshness_is_known(&self) -> bool {
        self.closed.is_some()
    }
}

/// Where a read was routed.
/// Where a read is routed.
///
/// ⭐ Re-exported from [`ehdb_core::plan`], not redefined.
pub use ehdb_core::plan::RouteTarget;

/// Why routing did not produce a non-owner target.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RouteRefusal {
    /// The capability is defined but not built.
    NotActivated { requested: &'static str },
}

impl RouteRefusal {
    pub fn message(&self) -> String {
        match self {
            Self::NotActivated { requested } => format!(
                "read locality '{requested}' is selected but not implemented: follower \
                 reads are gated behind fencing enforcement (M5), and falling back to \
                 'owner' would make the flag look taken while changing nothing"
            ),
        }
    }
}

/// Resolve a read's target.
///
/// `Owner` succeeds — that is today. `Nearest` refuses. See the module note
/// for why a silent fallback is worse than an error.
pub fn resolve_route(
    locality: ReadLocality,
    _candidates: &[ReplicaCandidate],
) -> Result<RouteTarget, RouteRefusal> {
    match locality {
        ReadLocality::Owner => Ok(RouteTarget::Owner),
        ReadLocality::Nearest => Err(RouteRefusal::NotActivated {
            requested: ReadLocality::Nearest.as_str(),
        }),
    }
}
