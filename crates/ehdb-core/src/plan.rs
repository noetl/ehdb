//! **The three resolvers** (multi-region spec M0).
//!
//! Multi-dimensionality is only affordable if the axes compose **in data**
//! rather than multiplying in code. The rule this module exists to enforce:
//!
//! > ⛔ **No axis may introduce a branch inside a tier driver.** The number of
//! > code paths stays equal to the number of tiers, forever.
//!
//! Four new axes (region, consistency level, staleness, survival goal) feed
//! **three pure functions**, each emitting a plain data plan that drivers
//! consume. `4 axes x 6 tiers` would be 24 branching paths; this is 3
//! resolvers plus 6 unchanged drivers.
//!
//! | resolver | bound at | consumes |
//! | :-- | :-- | :-- |
//! | [`resolve_placement`] | engine **open** | region, survival goal |
//! | [`resolve_route`] | per **request** | region, shard, consistency |
//! | [`resolve_visibility`] | per **read** | consistency, staleness |
//!
//! ## ⭐ The identity property
//!
//! Under today's configuration each resolver must emit **exactly what the
//! code hard-codes now**. That is M0's whole exit criterion, and it is what
//! makes every later phase's rollback trustworthy: a phase is then "give a
//! resolver a non-default input", and reverting it is "stop doing that".
//!
//! [`PlacementPlan::today`], [`RoutePlan::today`] and
//! [`VisibilityPlan::today`] name that baseline so a test can assert against
//! it rather than restating literals.
//!
//! ## ⚠ Correction to the spec, found while building this
//!
//! The M0 spec says the degenerate placement plan carries
//! `min_distinct = SurvivalGoal::Zone`, and M4's spec says *"today's `true`
//! maps exactly to `SurvivalGoal::Zone`"*. **Both overstate what today does.**
//! `L0Config::require_distinct_domains` defaults to **`false`**
//! (`ehdb-l0/src/engine.rs:133`), and its own doc says: *"`false` — the
//! default — is today's behaviour: violations are counted and logged but the
//! open succeeds."*
//!
//! So today is goal `Zone` at enforcement **`Shadow`**, not enforced `Zone`.
//! Collapsing those into one value would make the identity plan claim an
//! enforcement that does not exist — the plan would be wrong in the direction
//! that reads as safer. Hence [`Enforcement`] is a separate axis.
//!
//! ## Where this lives, and why
//!
//! `ehdb-core`, not `ehdb-l0`. `ehdb-reference` — which is what actually
//! serves the production event-log tier — does **not** depend on `ehdb-l0`,
//! so a plan type defined there would be unreachable from the tier. Core is
//! the one crate both stacks share.
//!
//! ## No flag
//!
//! M0 deliberately introduces none. A flag with a single legal value is a
//! representation that drifts; the resolvers simply have no non-default input
//! until a later phase supplies one.

use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// Axis values
// ---------------------------------------------------------------------------

/// Where a replica physically lives.
///
/// Both fields `Option` + `skip_serializing_if`, so an undeclared locality
/// serialises to nothing and cannot change bytes a rollback binary reads.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Locality {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub region: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub zone: Option<String>,
}

impl Locality {
    pub fn new(region: impl Into<String>, zone: impl Into<String>) -> Self {
        Self {
            region: Some(region.into()),
            zone: Some(zone.into()),
        }
    }
    pub fn undeclared() -> Self {
        Self::default()
    }
    pub fn is_undeclared(&self) -> bool {
        self.region.is_none() && self.zone.is_none()
    }
}

/// How much loss a replica set must survive.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SurvivalGoal {
    /// **Default and today.** Spread across zones / devices.
    #[default]
    Zone,
    /// Spread across regions.
    Region,
}

/// Whether a placement violation **refuses** or is merely counted.
///
/// ⚠ Separate from [`SurvivalGoal`] because today's default is a goal held in
/// shadow, not an enforced one — see the module note.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Enforcement {
    /// **Default and today.** Count and log; the open succeeds.
    #[default]
    Shadow,
    /// Refuse.
    Enforce,
}

/// How fresh a read requires its data to be.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ReadConsistency {
    /// **Default and today.** Served by the owner; no freshness predicate.
    #[default]
    Strong,
    Bounded {
        max_staleness_millis: u64,
    },
    Exact {
        at_millis: u64,
    },
}

impl ReadConsistency {
    /// Stable label, for pinning metric label values.
    pub fn label(&self) -> &'static str {
        match self {
            Self::Strong => "strong",
            Self::Bounded { .. } => "bounded",
            Self::Exact { .. } => "exact",
        }
    }
}

/// Where a read may be served from.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ReadLocality {
    /// **Default and today.**
    #[default]
    Owner,
    Nearest,
}

// ---------------------------------------------------------------------------
// Inputs
// ---------------------------------------------------------------------------

/// Everything the resolvers may read. One struct so a caller cannot forget an
/// axis, and so `Default` *is* today.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct AxisConfig {
    pub locality: Locality,
    pub survival: SurvivalGoal,
    pub enforcement: Enforcement,
    pub read_locality: ReadLocality,
    pub consistency: ReadConsistency,
}

/// One replica as placement sees it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicaSpec {
    pub id: String,
    pub locality: Locality,
}

impl ReplicaSpec {
    pub fn new(id: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            locality: Locality::undeclared(),
        }
    }
}

/// A read request's shape, for [`resolve_route`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct RequestContext {
    pub shard: u32,
}

// ---------------------------------------------------------------------------
// Plans
// ---------------------------------------------------------------------------

/// Resolved at engine **open**.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlacementPlan {
    pub replicas: Vec<ReplicaSpec>,
    pub survive: SurvivalGoal,
    pub enforcement: Enforcement,
}

impl PlacementPlan {
    /// ⭐ Exactly what the code hard-codes now: one replica named
    /// `replica-0` (`engine.rs:300`), goal `Zone`, held in `Shadow`
    /// (`require_distinct_domains: false`, `engine.rs:133`).
    pub fn today() -> Self {
        Self {
            replicas: vec![ReplicaSpec::new("replica-0")],
            survive: SurvivalGoal::Zone,
            enforcement: Enforcement::Shadow,
        }
    }
}

/// Where a request is served from.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RouteTarget {
    Owner,
    Replica(String),
}

/// Resolved per **request**.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RoutePlan {
    pub target: RouteTarget,
    pub may_follower_read: bool,
}

impl RoutePlan {
    /// ⭐ Today: the owner, no follower reads.
    pub fn today() -> Self {
        Self {
            target: RouteTarget::Owner,
            may_follower_read: false,
        }
    }
}

/// Resolved per **read**.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VisibilityPlan {
    pub floor_hlc: Option<u64>,
    pub require_closed_ts: Option<u64>,
}

impl VisibilityPlan {
    /// ⭐ Today: no freshness gate of any kind.
    pub fn today() -> Self {
        Self {
            floor_hlc: None,
            require_closed_ts: None,
        }
    }

    /// Whether this plan gates anything. `false` is today.
    pub fn gates_anything(&self) -> bool {
        self.floor_hlc.is_some() || self.require_closed_ts.is_some()
    }
}

// ---------------------------------------------------------------------------
// The three resolvers — pure functions, no I/O, no env reads
// ---------------------------------------------------------------------------

/// Resolve placement at engine open.
///
/// `replicas` is what the caller was going to construct anyway; the resolver
/// attaches the policy. An empty set is **not** silently replaced with a
/// default — `engine.rs:329` already refuses an empty replica set, and
/// inventing one here would hide a misconfiguration behind a plausible plan.
pub fn resolve_placement(cfg: &AxisConfig, replicas: Vec<ReplicaSpec>) -> PlacementPlan {
    PlacementPlan {
        replicas,
        survive: cfg.survival,
        enforcement: cfg.enforcement,
    }
}

/// Resolve routing for one request.
///
/// `may_follower_read` requires **both** a non-owner locality and a
/// consistency level that tolerates staleness. `Strong` never permits a
/// follower read however the locality is set — a strong read from a replica
/// that is merely nearby is a wrong answer, not a fast one.
pub fn resolve_route(cfg: &AxisConfig, _req: RequestContext) -> RoutePlan {
    let tolerates_staleness = !matches!(cfg.consistency, ReadConsistency::Strong);
    let may_follower_read =
        matches!(cfg.read_locality, ReadLocality::Nearest) && tolerates_staleness;
    RoutePlan {
        // Target selection among replicas is M6's job and is gated behind M5.
        // Until then every route is the owner, and the flag above records
        // whether a non-owner *would* be permissible.
        target: RouteTarget::Owner,
        may_follower_read,
    }
}

/// Resolve what a read is allowed to see.
///
/// ⚠ `now_millis` is passed in rather than read from a clock: a resolver that
/// reads the clock is not a pure function, cannot be table-tested, and would
/// make the identity property depend on when the test ran.
pub fn resolve_visibility(cfg: &AxisConfig, now_millis: u64) -> VisibilityPlan {
    match cfg.consistency {
        ReadConsistency::Strong => VisibilityPlan::today(),
        ReadConsistency::Bounded {
            max_staleness_millis,
        } => VisibilityPlan {
            floor_hlc: None,
            require_closed_ts: Some(now_millis.saturating_sub(max_staleness_millis)),
        },
        ReadConsistency::Exact { at_millis } => VisibilityPlan {
            floor_hlc: Some(at_millis),
            require_closed_ts: Some(at_millis),
        },
    }
}
