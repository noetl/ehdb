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
/// Where a replica physically lives.
///
/// ⭐ Re-exported from [`ehdb_core::plan`], not redefined. M0 and M1 were
/// written in parallel and each declared this type; two structurally identical
/// types that must stay identical is a representation that drifts, and the
/// serde shape here is load-bearing (a replica with no locality must serialise
/// byte-identically to today so a rollback binary keeps reading every manifest
/// written while the flag is unset — same precedent as `event_id` on
/// `EventRecord`). One definition cannot disagree with itself.
pub use ehdb_core::plan::Locality;

/// Read [`Locality`] from [`LOCALITY_ENV`], defaulting to undeclared.
///
/// ⚠ A free function rather than `Locality::from_env`, because the type now
/// lives in `ehdb-core::plan`, whose module note states that M0 deliberately
/// introduces no flags. The parsing is a property of the type and moved with
/// it; reading the environment is a property of a deployment and stays here.
pub fn locality_from_env() -> Result<Locality, String> {
    match std::env::var(LOCALITY_ENV) {
        Ok(raw) => Locality::parse(&raw),
        Err(_) => Ok(Locality::undeclared()),
    }
}
