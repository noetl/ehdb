//! Phase 10 — the tunable per-tier backend-selection config surface.
//!
//! Phases 6–9 built and (per tier) activated an EHDB engine underneath each of
//! NoETL's five internal **platform tiers** — event log, projection/read-model,
//! KV/state, object/blob, and vector.  Each tier reads its own runtime flag
//! (`NOETL_EHDB_<TIER>` ∈ `off|shadow|primary`) under the umbrella enable
//! (`NOETL_EHDB_ENABLED`).  That is a working but *scattered* surface: an
//! operator has to reason about five independent flags and their interaction
//! with the umbrella enable to answer "which engine serves each tier?".
//!
//! Phase 10 consolidates that scatter into **one coherent, documented schema**
//! without a breaking rename.  The existing `NOETL_EHDB_*` env vars remain the
//! source of truth; this module is the *vocabulary + resolution + validation*
//! layer on top of them:
//!
//! * [`PlatformTier`] — the fixed set of five internal platform tiers, each with
//!   its runtime env-var name and the **incumbent** (external) engine it can be
//!   pinned back to.
//! * [`TierMode`] — the operational mode of a tier's EHDB engine
//!   (`off`/`shadow`/`primary`), mirroring the worker's per-tier enums.
//! * [`Backend`] — which engine actually *serves* the tier: [`Backend::Ehdb`]
//!   iff the tier is in `primary`, otherwise the [`Backend::External`] incumbent
//!   (`shadow` dual-writes but the incumbent still serves; `off` is a strict
//!   no-op).  See [`backend_for_mode`].
//! * [`BackendMatrix`] — the resolved umbrella-enable + per-tier
//!   selection + backend matrix, with [`BackendMatrix::validate`] rejecting
//!   incoherent combos (a tier requesting `shadow`/`primary` while the umbrella
//!   is disabled, or a control-plane role trying to serve a data-plane tier) and
//!   a secret-free JSON render ([`BackendMatrix::to_json`]).
//!
//! ## EHDB is the default, not a lock-in
//!
//! This schema is deliberately symmetric.  EHDB is the *default target* only at
//! program end; every tier stays first-class selectable back to its incumbent by
//! keeping its flag at `off`/`shadow`.  The matrix reports the incumbent for each
//! tier so a deployment can run any per-tier mix (e.g. JetStream+Postgres for the
//! log, EHDB for vectors) and see exactly what is selected.
//!
//! ## Boundaries
//!
//! * **Platform-only.**  These five tiers are NoETL's *internal* platform
//!   storage uses.  Business/tenant data is never in EHDB — it stays in external
//!   business systems reached via playbook connectors, entirely outside this
//!   surface.
//! * **Pure data.**  This module reads no environment and opens no engine.  The
//!   worker's `src/ehdb/backends.rs` resolves the process env (through the same
//!   per-tier parsers the runtime dispatch uses) into a [`BackendMatrix`]; this
//!   crate only defines the schema, the backend derivation, and the coherence
//!   rules so they are shared and testable.

use serde::{Deserialize, Serialize};

/// One of NoETL's five internal platform tiers that EHDB can back.
///
/// The order is stable (`ALL` iterates event-log → projection → KV → object →
/// vector) so a rendered matrix is deterministic.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PlatformTier {
    /// Append-only platform event log (Phase 6 / Phase 9 tier 1).
    EventLog,
    /// Materialized read-model / projection tier (Phase 7 / Phase 9 tier 2).
    Projection,
    /// Platform KV / state tier (Phase 8 slice 1 / Phase 9 tier 3).
    Kv,
    /// Content-addressed object / blob tier (Phase 8 slice 2 / Phase 9 tier 4).
    Object,
    /// Platform vector / RAG-retrieval tier (Phase 8 slice 3 / Phase 9 tier 5).
    Vector,
}

impl PlatformTier {
    /// Every platform tier, in stable render order.
    pub const ALL: [PlatformTier; 5] = [
        PlatformTier::EventLog,
        PlatformTier::Projection,
        PlatformTier::Kv,
        PlatformTier::Object,
        PlatformTier::Vector,
    ];

    /// The tier's short stable key (matches the `ehdb-selfcheck` verb prefixes
    /// and the `noetl_ehdb_<key>_*` metric namespace).
    pub fn as_str(&self) -> &'static str {
        match self {
            PlatformTier::EventLog => "eventlog",
            PlatformTier::Projection => "projection",
            PlatformTier::Kv => "kv",
            PlatformTier::Object => "object",
            PlatformTier::Vector => "vector",
        }
    }

    /// The runtime env var that selects this tier's mode
    /// (`off`/`shadow`/`primary`).
    pub fn env_var(&self) -> &'static str {
        match self {
            PlatformTier::EventLog => "NOETL_EHDB_EVENTLOG",
            PlatformTier::Projection => "NOETL_EHDB_PROJECTION",
            PlatformTier::Kv => "NOETL_EHDB_KV",
            PlatformTier::Object => "NOETL_EHDB_OBJECT",
            PlatformTier::Vector => "NOETL_EHDB_VECTOR",
        }
    }

    /// The external incumbent engine this tier stays selectable back to when its
    /// flag is `off`/`shadow` (EHDB is the default, not a lock-in).
    pub fn incumbent(&self) -> &'static str {
        match self {
            PlatformTier::EventLog => "NATS JetStream + Postgres",
            PlatformTier::Projection => "Postgres materializer",
            PlatformTier::Kv => "NATS KV",
            PlatformTier::Object => "external object store (GCS/S3) / Postgres",
            PlatformTier::Vector => "Qdrant",
        }
    }
}

/// The operational mode of a tier's EHDB engine.  Mirrors the worker's per-tier
/// `<Tier>Mode` enums (`off` default, `shadow` dual-write + compare, `primary`
/// serve).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum TierMode {
    /// No EHDB engine; the incumbent is authoritative (strict no-op).
    Off,
    /// Dual-write into EHDB + compare parity; the incumbent still serves.
    Shadow,
    /// EHDB serves the tier authoritatively; the incumbent is dual-run
    /// parity-checked and retired for that tier.
    Primary,
}

impl TierMode {
    /// The mode's stable lowercase token.
    pub fn as_str(&self) -> &'static str {
        match self {
            TierMode::Off => "off",
            TierMode::Shadow => "shadow",
            TierMode::Primary => "primary",
        }
    }

    /// Fail-safe parse mirroring the worker's per-tier `from_env`: only the exact
    /// tokens `shadow`/`primary` (case-insensitive, trimmed) select those modes;
    /// everything else — unset, empty, or unrecognised — is `Off` so an unknown
    /// value never mirrors or serves.
    pub fn from_raw(raw: Option<&str>) -> Self {
        match raw.map(|v| v.trim().to_ascii_lowercase()).as_deref() {
            Some("shadow") => TierMode::Shadow,
            Some("primary") => TierMode::Primary,
            _ => TierMode::Off,
        }
    }
}

/// Which engine actually serves a tier.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Backend {
    /// The EHDB engine serves the tier authoritatively.
    Ehdb,
    /// The external incumbent engine serves the tier.
    External,
}

impl Backend {
    /// The backend's stable lowercase token.
    pub fn as_str(&self) -> &'static str {
        match self {
            Backend::Ehdb => "ehdb",
            Backend::External => "external",
        }
    }
}

/// Derive the serving backend from a tier's mode: EHDB serves only in `primary`;
/// `shadow` (dual-write, incumbent serves) and `off` (no-op) keep the external
/// incumbent authoritative.
pub fn backend_for_mode(mode: TierMode) -> Backend {
    match mode {
        TierMode::Primary => Backend::Ehdb,
        TierMode::Off | TierMode::Shadow => Backend::External,
    }
}

/// One tier's resolved selection: its mode and the backend that serves it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct TierSelection {
    pub tier: PlatformTier,
    pub mode: TierMode,
    pub backend: Backend,
}

impl TierSelection {
    /// Build a selection, deriving the backend from the mode.
    pub fn new(tier: PlatformTier, mode: TierMode) -> Self {
        TierSelection {
            tier,
            mode,
            backend: backend_for_mode(mode),
        }
    }
}

/// A coherence violation of the backend-config surface.  Carries the tier it
/// concerns (`None` for an umbrella-level violation) and a clear operator
/// message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BackendConfigError {
    pub tier: Option<PlatformTier>,
    pub message: String,
}

impl std::fmt::Display for BackendConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.tier {
            Some(t) => write!(f, "[{}] {}", t.as_str(), self.message),
            None => write!(f, "{}", self.message),
        }
    }
}

impl std::error::Error for BackendConfigError {}

/// The resolved per-tier backend-selection matrix — the consolidated Phase-10
/// view of the scattered `NOETL_EHDB_*` env.
///
/// Precedence the matrix encodes: **umbrella enable** (`enabled`) →
/// **per-tier mode** (from `NOETL_EHDB_<TIER>`) → **derived backend**
/// (`primary` ⇒ EHDB, else the incumbent).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BackendMatrix {
    /// The umbrella enable (`NOETL_EHDB_ENABLED` truthy).
    pub enabled: bool,
    /// The resolved client role (`worker`/`playbook`/`system`/`gateway`/…, or
    /// `unknown`).  A fixed enum token — never a secret.
    pub role: String,
    /// Whether the role is a control-plane gatekeeper (gateway/api/server),
    /// which must never serve a data-plane tier.
    pub role_is_control_plane: bool,
    /// The umbrella integration mode string
    /// (`disabled`/`control_plane`/`local_reference`).
    pub integration_mode: String,
    /// The five tier selections, in [`PlatformTier::ALL`] order.
    pub tiers: Vec<TierSelection>,
}

impl BackendMatrix {
    /// Look up a tier's resolved selection.
    pub fn selection(&self, tier: PlatformTier) -> Option<&TierSelection> {
        self.tiers.iter().find(|s| s.tier == tier)
    }

    /// The backend serving a tier (defaults to [`Backend::External`] if the tier
    /// is somehow absent).
    pub fn backend_for(&self, tier: PlatformTier) -> Backend {
        self.selection(tier)
            .map(|s| s.backend)
            .unwrap_or(Backend::External)
    }

    /// Every coherence violation (empty ⇒ coherent).  The rules:
    ///
    /// * A tier in `shadow` or `primary` while the umbrella is **disabled** is
    ///   incoherent — a tier cannot shadow-mirror or serve while EHDB is off.
    /// * A tier in `shadow` or `primary` on a **control-plane** role is
    ///   incoherent — gateway/api/server are gatekeepers and never touch a
    ///   data-plane tier.
    pub fn validate(&self) -> Vec<BackendConfigError> {
        let mut errors = Vec::new();
        for sel in &self.tiers {
            if sel.mode == TierMode::Off {
                continue;
            }
            if !self.enabled {
                errors.push(BackendConfigError {
                    tier: Some(sel.tier),
                    message: format!(
                        "{} requests '{}' but NOETL_EHDB_ENABLED is not set — a tier cannot \
                         {} while the umbrella EHDB integration is disabled (set \
                         NOETL_EHDB_ENABLED or return {} to 'off')",
                        sel.tier.env_var(),
                        sel.mode.as_str(),
                        if sel.mode == TierMode::Primary {
                            "serve from EHDB"
                        } else {
                            "shadow-mirror into EHDB"
                        },
                        sel.tier.env_var(),
                    ),
                });
            }
            if self.role_is_control_plane {
                errors.push(BackendConfigError {
                    tier: Some(sel.tier),
                    message: format!(
                        "{} requests '{}' on control-plane role '{}' — gateway/api/server are \
                         gatekeepers and never serve a data-plane tier (run EHDB tiers on \
                         worker/playbook/system roles only)",
                        sel.tier.env_var(),
                        sel.mode.as_str(),
                        self.role,
                    ),
                });
            }
        }
        errors
    }

    /// Whether the matrix is coherent (no [`validate`](Self::validate)
    /// violations).
    pub fn is_coherent(&self) -> bool {
        self.validate().is_empty()
    }

    /// A secret-free JSON render of the resolved matrix.  By construction this
    /// emits only the umbrella facts, the role token, and each tier's
    /// key/mode/backend/incumbent/env-var **names** — never any env *value*, so
    /// nothing sensitive can leak.
    pub fn to_json(&self) -> serde_json::Value {
        let errors = self.validate();
        serde_json::json!({
            "enabled": self.enabled,
            "role": self.role,
            "control_plane": self.role_is_control_plane,
            "integration_mode": self.integration_mode,
            "coherent": errors.is_empty(),
            "errors": errors.iter().map(|e| e.to_string()).collect::<Vec<_>>(),
            "tiers": self.tiers.iter().map(|s| serde_json::json!({
                "tier": s.tier.as_str(),
                "env_var": s.tier.env_var(),
                "mode": s.mode.as_str(),
                "backend": s.backend.as_str(),
                "incumbent": s.tier.incumbent(),
            })).collect::<Vec<_>>(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn matrix(
        enabled: bool,
        role: &str,
        control_plane: bool,
        modes: [TierMode; 5],
    ) -> BackendMatrix {
        BackendMatrix {
            enabled,
            role: role.to_string(),
            role_is_control_plane: control_plane,
            integration_mode: if enabled {
                "local_reference"
            } else {
                "disabled"
            }
            .to_string(),
            tiers: PlatformTier::ALL
                .iter()
                .zip(modes)
                .map(|(&t, m)| TierSelection::new(t, m))
                .collect(),
        }
    }

    #[test]
    fn backend_derivation_primary_is_ehdb_else_external() {
        assert_eq!(backend_for_mode(TierMode::Off), Backend::External);
        assert_eq!(backend_for_mode(TierMode::Shadow), Backend::External);
        assert_eq!(backend_for_mode(TierMode::Primary), Backend::Ehdb);
    }

    #[test]
    fn tier_mode_fail_safe_parse() {
        assert_eq!(TierMode::from_raw(None), TierMode::Off);
        assert_eq!(TierMode::from_raw(Some("")), TierMode::Off);
        assert_eq!(TierMode::from_raw(Some("garbage")), TierMode::Off);
        assert_eq!(TierMode::from_raw(Some(" SHADOW ")), TierMode::Shadow);
        assert_eq!(TierMode::from_raw(Some("Primary")), TierMode::Primary);
    }

    #[test]
    fn all_five_tiers_have_distinct_env_vars_and_keys() {
        let keys: Vec<&str> = PlatformTier::ALL.iter().map(|t| t.as_str()).collect();
        let envs: Vec<&str> = PlatformTier::ALL.iter().map(|t| t.env_var()).collect();
        assert_eq!(keys.len(), 5);
        for i in 0..5 {
            for j in (i + 1)..5 {
                assert_ne!(keys[i], keys[j]);
                assert_ne!(envs[i], envs[j]);
            }
        }
    }

    #[test]
    fn all_external_default_is_coherent() {
        // Disabled umbrella, every tier off → every backend external, coherent.
        let m = matrix(false, "worker", false, [TierMode::Off; 5]);
        assert!(m.is_coherent());
        for t in PlatformTier::ALL {
            assert_eq!(m.backend_for(t), Backend::External);
        }
    }

    #[test]
    fn all_ehdb_default_is_coherent_when_enabled() {
        let m = matrix(true, "worker", false, [TierMode::Primary; 5]);
        assert!(m.is_coherent());
        for t in PlatformTier::ALL {
            assert_eq!(m.backend_for(t), Backend::Ehdb);
        }
    }

    #[test]
    fn mixed_selection_resolves_per_tier() {
        // Log + vector on EHDB; projection/kv/object stay external — the "any
        // per-tier mix" the RFC calls out.
        let m = matrix(
            true,
            "worker",
            false,
            [
                TierMode::Primary, // eventlog → ehdb
                TierMode::Shadow,  // projection → external (dual-write)
                TierMode::Off,     // kv → external
                TierMode::Off,     // object → external
                TierMode::Primary, // vector → ehdb
            ],
        );
        assert!(m.is_coherent());
        assert_eq!(m.backend_for(PlatformTier::EventLog), Backend::Ehdb);
        assert_eq!(m.backend_for(PlatformTier::Projection), Backend::External);
        assert_eq!(m.backend_for(PlatformTier::Kv), Backend::External);
        assert_eq!(m.backend_for(PlatformTier::Object), Backend::External);
        assert_eq!(m.backend_for(PlatformTier::Vector), Backend::Ehdb);
    }

    #[test]
    fn primary_without_enable_is_incoherent() {
        let m = matrix(
            false,
            "worker",
            false,
            [
                TierMode::Primary,
                TierMode::Off,
                TierMode::Off,
                TierMode::Off,
                TierMode::Off,
            ],
        );
        assert!(!m.is_coherent());
        let errs = m.validate();
        assert_eq!(errs.len(), 1);
        assert_eq!(errs[0].tier, Some(PlatformTier::EventLog));
        assert!(errs[0].message.contains("NOETL_EHDB_ENABLED"));
    }

    #[test]
    fn shadow_without_enable_is_incoherent() {
        let m = matrix(
            false,
            "worker",
            false,
            [
                TierMode::Off,
                TierMode::Shadow,
                TierMode::Off,
                TierMode::Off,
                TierMode::Off,
            ],
        );
        assert!(!m.is_coherent());
        assert_eq!(m.validate()[0].tier, Some(PlatformTier::Projection));
    }

    #[test]
    fn primary_on_control_plane_role_is_incoherent() {
        let m = matrix(
            true,
            "gateway",
            true,
            [
                TierMode::Primary,
                TierMode::Off,
                TierMode::Off,
                TierMode::Off,
                TierMode::Off,
            ],
        );
        assert!(!m.is_coherent());
        let errs = m.validate();
        assert_eq!(errs.len(), 1);
        assert!(errs[0].message.contains("control-plane"));
    }

    #[test]
    fn json_render_is_secret_free_and_complete() {
        let m = matrix(
            true,
            "worker",
            false,
            [
                TierMode::Primary,
                TierMode::Shadow,
                TierMode::Off,
                TierMode::Primary,
                TierMode::Off,
            ],
        );
        let json = m.to_json();
        // Renders all five tiers with the derived backend + incumbent.
        let tiers = json["tiers"].as_array().unwrap();
        assert_eq!(tiers.len(), 5);
        assert_eq!(tiers[0]["tier"], "eventlog");
        assert_eq!(tiers[0]["backend"], "ehdb");
        assert_eq!(tiers[0]["incumbent"], "NATS JetStream + Postgres");
        assert_eq!(tiers[1]["backend"], "external"); // shadow → incumbent serves
        assert_eq!(tiers[4]["tier"], "vector");
        assert_eq!(json["coherent"], true);
        // Serialized form carries only names/enums — no way for a secret value
        // to appear because none is ever read into the matrix.
        let s = serde_json::to_string(&json).unwrap();
        assert!(s.contains("\"enabled\":true"));
        assert!(!s.contains("password"));
    }
}
