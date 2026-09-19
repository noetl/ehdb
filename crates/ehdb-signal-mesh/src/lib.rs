//! POC — a tiered A2A/ReAct agent mesh over the EHDB event log.
//!
//! Branch `design/a2a-react-signal-mesh`. Design doc:
//! `docs/spec/a2a-react-signal-mesh.md`.
//!
//! ⚠ Flag-gated and additive. Nothing here is wired into any binary that runs
//! in production, and no generated code is executed anywhere in this crate.

pub mod a2a;
pub mod event;
pub mod fold;
pub mod mesh;
pub mod react;

/// Master flag. Everything in the mesh stays off unless this is exactly
/// `"true"` — the house convention (`seal_max_age`, fencing, the repair sweep
/// all use it), so arming is deliberate and reversible.
pub const MESH_ENABLED_ENV: &str = "NOETL_SIGNAL_MESH";
/// Opt in to a model-backed reasoner. Default off keeps every test
/// deterministic.
pub const MESH_REASONER_ENV: &str = "NOETL_SIGNAL_MESH_REASONER";

/// Is the mesh armed? Pure over the raw value so the default is testable
/// without touching process env.
pub fn mesh_armed(raw: Option<&str>) -> bool {
    matches!(raw.map(str::trim), Some("true"))
}
