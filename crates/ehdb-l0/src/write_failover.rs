//! **Region-survivable writes** (multi-region spec M8).
//!
//! ## ⛔ SCAFFOLD — flag only, no leadership movement
//!
//! Nothing here moves a writer. [`FailoverMode`] exists so the flag has one
//! definition rather than three spellings later, and so the honest state is
//! recorded in code rather than only in a document.
//!
//! ## ⚠⚠ This is blocked on a real, open design decision
//!
//! **Reads reach REGION at M6/M7; writes stay ZONE.** The Kubernetes Lease
//! CAS that elects a writer is **per-cluster**: losing the region hosting
//! that API server means no writer can be elected anywhere. Three options
//! were considered and only one is closed:
//!
//! * home-cluster API server as the lease authority — workable, but that
//!   cluster becomes a cross-region dependency;
//! * leases in EHDB over a KV CAS — **never**. A CAS over a store with no
//!   agreement underneath is not a CAS;
//! * embedded consensus scoped to lease records only — its own RFC.
//!
//! Until one is chosen in writing, activating failover would ship a mechanism
//! that cannot fire in the one scenario it exists for. [`activate`] therefore
//! refuses everything except `Off`.

/// Env var selecting write failover.
pub const WRITE_FAILOVER_ENV: &str = "NOETL_EHDB_WRITE_FAILOVER";

/// How a shard's write leadership may move.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum FailoverMode {
    /// **Default.** Leadership never moves. Today.
    #[default]
    Off,
    /// A human moves leadership. ⛔ Not implemented.
    Manual,
    /// A failure detector moves leadership. ⛔ Not implemented, and **not
    /// recommended**: a wrong "the region is gone" decision is a split-brain
    /// on a tier serving `primary`, and C2 says gaplessness depends on there
    /// being exactly one writer. A failover that leaves two is not a degraded
    /// mode, it is divergence.
    Auto,
}

impl FailoverMode {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Off => "off",
            Self::Manual => "manual",
            Self::Auto => "auto",
        }
    }

    /// ⚠ Unrecognised ⇒ `Off`. A typo must not arm a failover.
    pub fn parse(raw: Option<&str>) -> Self {
        match raw.map(|v| v.trim().to_ascii_lowercase()).as_deref() {
            Some("manual") => Self::Manual,
            Some("auto") => Self::Auto,
            _ => Self::Off,
        }
    }

    pub fn from_env() -> Self {
        Self::parse(std::env::var(WRITE_FAILOVER_ENV).ok().as_deref())
    }
}

/// Why failover could not be armed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FailoverRefusal {
    /// The mode is defined but the mechanism is not built, and the design
    /// fork it depends on is unresolved.
    BlockedOnLeaseAuthority { requested: &'static str },
}

impl FailoverRefusal {
    pub fn message(&self) -> String {
        match self {
            Self::BlockedOnLeaseAuthority { requested } => format!(
                "write failover '{requested}' is selected but not implemented: the \
                 lease authority is per-cluster, so it cannot elect a writer after \
                 losing the region that hosts it. Arming this would ship a failover \
                 that cannot fire in the one scenario it exists for"
            ),
        }
    }
}

/// Arm failover. Only `Off` succeeds.
pub fn activate(mode: FailoverMode) -> Result<(), FailoverRefusal> {
    match mode {
        FailoverMode::Off => Ok(()),
        other => Err(FailoverRefusal::BlockedOnLeaseAuthority {
            requested: other.as_str(),
        }),
    }
}
