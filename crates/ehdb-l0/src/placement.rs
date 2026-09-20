//! **Replica locality** (multi-region spec M1).
//!
//! The typed coordinate a replica declares about where it physically is.
//!
//! ## ⚠ This is NOT the `region=` already in the key strings
//!
//! `region` already appears inside KV and object logical keys —
//! `noetl/env=…/region=us-central1/cell=…/shard=s0042/tenant=…`. That is a
//! **naming convention inside an opaque key** and nothing parses it: those
//! keys are addressed through a SHA-256 subject digest, and the full key is
//! carried in the record payload purely as data. Routing or placing on it
//! would be reading structure into a string that no code maintains.
//!
//! Those key strings stay exactly as they are. This type is separate, typed,
//! and the only thing placement decisions may consult.
//!
//! ## Undeclared fails closed
//!
//! A [`Locality`] with no region cannot be *shown* independent of any other,
//! so it is never assumed to be. Same posture as
//! [`FailureDomain::Undeclared`](crate::failure_domain::FailureDomain::Undeclared),
//! whose own doc puts it: silence is not independence.

use serde::{Deserialize, Serialize};

/// Env var declaring this node's locality, e.g.
/// `region=us-central1,zone=us-central1-a`.
pub const LOCALITY_ENV: &str = "NOETL_EHDB_LOCALITY";

/// Where a replica physically lives.
///
/// ⭐ Both fields are `Option` with `skip_serializing_if`, and that is
/// mandatory rather than stylistic: a replica with no locality must serialise
/// **byte-identically to today**, so a rollback binary keeps reading every
/// manifest written while the flag is unset. Same precedent as `event_id` on
/// `EventRecord`.
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

    /// Nothing declared.
    pub fn undeclared() -> Self {
        Self::default()
    }

    pub fn is_undeclared(&self) -> bool {
        self.region.is_none() && self.zone.is_none()
    }

    /// Parse `region=<r>,zone=<z>`. Unknown keys are **refused**, not ignored
    /// — a dropped key is a setting the operator believes is applied.
    pub fn parse(raw: &str) -> Result<Self, String> {
        let mut out = Self::default();
        for pair in raw.split(',').map(str::trim).filter(|p| !p.is_empty()) {
            let (k, v) = pair
                .split_once('=')
                .ok_or_else(|| format!("locality fragment '{pair}' is not key=value"))?;
            let v = v.trim();
            match k.trim().to_ascii_lowercase().as_str() {
                "region" => out.region = (!v.is_empty()).then(|| v.to_string()),
                "zone" => out.zone = (!v.is_empty()).then(|| v.to_string()),
                other => {
                    return Err(format!(
                        "unknown locality key '{other}': refused rather than ignored, \
                         because a silently-dropped key is a setting the operator \
                         believes is applied"
                    ))
                }
            }
        }
        Ok(out)
    }

    pub fn from_env() -> Result<Self, String> {
        match std::env::var(LOCALITY_ENV) {
            Ok(raw) => Self::parse(&raw),
            Err(_) => Ok(Self::undeclared()),
        }
    }

    /// Whether these two are provably in **different** regions.
    ///
    /// ⚠ Returns `false` when either side is undeclared. "Cannot be shown
    /// different" is not "is the same", but for a placement decision the two
    /// must be treated alike: acting on an unproven difference is how an RF
    /// of N over one domain gets called an RF of N.
    pub fn provably_different_region(&self, other: &Self) -> bool {
        match (&self.region, &other.region) {
            (Some(a), Some(b)) => a != b,
            _ => false,
        }
    }
}
