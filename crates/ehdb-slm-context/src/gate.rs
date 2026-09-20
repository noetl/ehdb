//! S3 — the admission gate, in **propose-only** mode.
//!
//! A model proposes a step spec; this module decides whether it may become a
//! step. It never runs one.
//!
//! # ⛔ Nothing executes in this build
//!
//! There is **no execution path in this crate at all** — no function here runs
//! a step, registers a catalog entry, or performs I/O of any kind. That is not
//! a flag default that could be flipped by configuration; it is the absence of
//! the code. [`Admission::executed`] exists so the wiring phase has somewhere to
//! record the flip and so a test can assert it never moved.
//!
//! [`StepGenMode::Execute`] parses, and in this build behaves exactly as
//! [`StepGenMode::Propose`]. The owner gates the actual flip.
//!
//! # The gates, in order
//!
//! Order matters and is fixed, so the *reported* rule is deterministic when a
//! spec would fail several:
//!
//! 1. **Malformed** — the proposal is not a JSON object.
//! 2. **Budget** — the fold's bounds are already spent (steps, then depth).
//! 3. **Schema** — the real DSL validator, **injected** via [`DslValidator`].
//!    ⭐ This crate never validates DSL itself. Two validators that disagree is
//!    worse than one that is strict, and the parser
//!    (`noetl-server`'s `playbook::parser::{parse_playbook, validate_playbook}`,
//!    both `pub`) is the one that admits real executions.
//! 4. **Credential reach** — no `auth:`, no keychain alias off the allowlist.
//! 5. **Tool-kind deny list** — `python`. Terminal: no human may approve it.
//! 6. **Read-shape** — for kinds that need it (`http`).
//! 7. **Tool-kind allowlist** — then the human gate for anything off it.
//!
//! # Fork F4, as decided by the owner (2026-09-19)
//!
//! ⛔ **`python` is DENIED, not merely un-allowlisted.** It sits on a deny list
//! checked before the allowlist and before the human gate, so it cannot be
//! approved into existence. "Not on the allowlist" would leave it reachable by
//! a human clicking approve; a deny list makes the decision structural.
//!
//! ⛔ **`http` is not on the default allowlist either, and the reason is the
//! URL.** Method and body are mechanically checkable — `HttpConfig` exposes
//! `method`, `body`, `json`, `form` (`noetl/tools` `http.rs:47-77`) — but the
//! URL is not. A side-effect-free GET still permits:
//!
//! - **exfiltration** — arbitrary data in the query string to an arbitrary host;
//! - **SSRF** — `169.254.169.254` and any in-cluster service;
//! - and GET-safety is a **server-side convention**, not a property this gate
//!   can verify.
//!
//! Constraining that needs a host allowlist, which would default to empty and
//! so admit nothing anyway. The honest answer to "can `http` be cleanly
//! constrained to side-effect-free GETs" is **no**, so the default allowlist is
//! **`noop` alone**.
//!
//! The read-shape machinery is nonetheless implemented and tested, as
//! defence-in-depth for an operator who explicitly opts `http` in: it then
//! additionally requires GET/HEAD, no body/json/form, and a **non-empty** host
//! allowlist. With the default empty host list, an allowlisted `http` still
//! cannot pass.
//!
//! # Carrier (fork F2, approved)
//!
//! An admitted spec is carried as a **catalog entry** under a dedicated path
//! prefix, content-addressed by digest. The catalog is the only carrier with
//! existing versioning, ACLs and soft-delete/restore — and routing through it
//! means the parser sees the spec, which is the whole safety story.

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::event::AdmitGate;
use crate::fold::WorkingContext;

/// `NOETL_SLM_STEPGEN`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum StepGenMode {
    /// No proposal is considered. The default.
    #[default]
    Off,
    /// Validate and decide; execute nothing. The active mode.
    Propose,
    /// ⛔ Reserved. Parses, but behaves as [`StepGenMode::Propose`] in this
    /// build because no execution path exists. Owner-gated.
    Execute,
}

impl StepGenMode {
    pub fn parse(raw: &str) -> Self {
        match raw.trim().to_ascii_lowercase().as_str() {
            "propose" => StepGenMode::Propose,
            "on" | "execute" => StepGenMode::Execute,
            _ => StepGenMode::Off,
        }
    }

    pub fn from_env() -> Self {
        std::env::var("NOETL_SLM_STEPGEN")
            .map(|v| StepGenMode::parse(&v))
            .unwrap_or_default()
    }
}

/// `NOETL_SLM_HUMAN_GATE`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum HumanGate {
    /// Side-effectful kinds need an explicit approval event. The default.
    #[default]
    Required,
    Off,
}

impl HumanGate {
    pub fn parse(raw: &str) -> Self {
        match raw.trim().to_ascii_lowercase().as_str() {
            "off" | "false" | "0" => HumanGate::Off,
            _ => HumanGate::Required,
        }
    }
}

/// Every way a proposal can be refused.
///
/// ⭐ [`RejectionRule::ALL`] is the **complete** set, so a consumer can pin a
/// counter series at 0 for every label. A pinned set that omits one value
/// reintroduces the absent-series bug on exactly that value while the rest read
/// 0 and look complete.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RejectionRule {
    Malformed,
    BudgetExhausted,
    DepthExceeded,
    SchemaInvalid,
    CredentialReach,
    KeychainAliasNotAllowed,
    MissingToolKind,
    ToolKindDenied,
    HttpNotReadShaped,
    ToolKindNotAllowed,
}

impl RejectionRule {
    pub const ALL: &'static [RejectionRule] = &[
        RejectionRule::Malformed,
        RejectionRule::BudgetExhausted,
        RejectionRule::DepthExceeded,
        RejectionRule::SchemaInvalid,
        RejectionRule::CredentialReach,
        RejectionRule::KeychainAliasNotAllowed,
        RejectionRule::MissingToolKind,
        RejectionRule::ToolKindDenied,
        RejectionRule::HttpNotReadShaped,
        RejectionRule::ToolKindNotAllowed,
    ];

    pub fn label(self) -> &'static str {
        match self {
            RejectionRule::Malformed => "malformed",
            RejectionRule::BudgetExhausted => "budget_exhausted",
            RejectionRule::DepthExceeded => "depth_exceeded",
            RejectionRule::SchemaInvalid => "schema_invalid",
            RejectionRule::CredentialReach => "credential_reach",
            RejectionRule::KeychainAliasNotAllowed => "keychain_alias_not_allowed",
            RejectionRule::MissingToolKind => "missing_tool_kind",
            RejectionRule::ToolKindDenied => "tool_kind_denied",
            RejectionRule::HttpNotReadShaped => "http_not_read_shaped",
            RejectionRule::ToolKindNotAllowed => "tool_kind_not_allowed",
        }
    }
}

/// The real DSL validator, supplied by the caller.
///
/// ⭐ Implemented in `noetl-server` over `playbook::parser::parse_playbook` +
/// `validate_playbook` — both **VERIFIED `pub`** (`parser.rs:15`, `:164`), so
/// the gate reuses the parser that admits real executions rather than growing a
/// second opinion.
pub trait DslValidator {
    fn validate(&self, spec: &serde_json::Value) -> Result<(), String>;
    /// Recorded on every admission, so a later replay knows which validator
    /// admitted a step.
    fn version(&self) -> String;
}

/// Policy inputs. Defaults are the plan's §10 recommendations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Policy {
    /// `NOETL_SLM_ALLOWED_TOOL_KINDS`. Kinds off this list need the human gate.
    pub allowed_tool_kinds: Vec<String>,
    /// `NOETL_SLM_DENIED_TOOL_KINDS`. ⛔ Terminal — a denied kind is rejected
    /// before the allowlist and before the human gate, so it cannot be approved
    /// into existence. Deny beats allow if a kind appears on both.
    pub denied_tool_kinds: Vec<String>,
    /// `NOETL_SLM_HTTP_ALLOWED_HOSTS`. Empty by default, which means an
    /// allowlisted `http` step still cannot pass the read-shape check.
    pub http_allowed_hosts: Vec<String>,
    /// `NOETL_SLM_HUMAN_GATE`.
    pub human_gate: HumanGate,
    /// Keychain aliases a generated step may name. Empty = none.
    pub allowed_keychain_aliases: Vec<String>,
    /// `NOETL_SLM_CATALOG_PREFIX` — F2's dedicated namespace, so generated
    /// entries are identifiable and bulk-reversible by soft delete.
    pub catalog_prefix: String,
}

impl Default for Policy {
    fn default() -> Self {
        Self {
            // Owner decision 2026-09-19: no python, and http is not cleanly
            // constrainable (see the module doc). `noop` alone.
            allowed_tool_kinds: vec!["noop".into()],
            denied_tool_kinds: vec!["python".into()],
            http_allowed_hosts: Vec::new(),
            human_gate: HumanGate::Required,
            allowed_keychain_aliases: Vec::new(),
            catalog_prefix: "generated/slm/".into(),
        }
    }
}

impl Policy {
    pub fn from_env() -> Self {
        let d = Policy::default();
        Self {
            allowed_tool_kinds: std::env::var("NOETL_SLM_ALLOWED_TOOL_KINDS")
                .ok()
                .map(|v| {
                    v.split(',')
                        .map(|s| s.trim().to_ascii_lowercase())
                        .filter(|s| !s.is_empty())
                        .collect()
                })
                .unwrap_or(d.allowed_tool_kinds),
            denied_tool_kinds: std::env::var("NOETL_SLM_DENIED_TOOL_KINDS")
                .ok()
                .map(|v| {
                    v.split(',')
                        .map(|s| s.trim().to_ascii_lowercase())
                        .filter(|s| !s.is_empty())
                        .collect()
                })
                .unwrap_or(d.denied_tool_kinds),
            http_allowed_hosts: std::env::var("NOETL_SLM_HTTP_ALLOWED_HOSTS")
                .ok()
                .map(|v| {
                    v.split(',')
                        .map(|s| s.trim().to_ascii_lowercase())
                        .filter(|s| !s.is_empty())
                        .collect()
                })
                .unwrap_or(d.http_allowed_hosts),
            human_gate: std::env::var("NOETL_SLM_HUMAN_GATE")
                .map(|v| HumanGate::parse(&v))
                .unwrap_or_default(),
            allowed_keychain_aliases: std::env::var("NOETL_SLM_ALLOWED_KEYCHAIN_ALIASES")
                .ok()
                .map(|v| {
                    v.split(',')
                        .map(|s| s.trim().to_string())
                        .filter(|s| !s.is_empty())
                        .collect()
                })
                .unwrap_or(d.allowed_keychain_aliases),
            catalog_prefix: std::env::var("NOETL_SLM_CATALOG_PREFIX").unwrap_or(d.catalog_prefix),
        }
    }
}

/// Where an admitted spec would live (F2). Computed, never written here.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CatalogCarrier {
    pub path: String,
    pub content_digest: String,
}

/// An admitted proposal.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Admission {
    pub carrier: CatalogCarrier,
    pub gate: AdmitGate,
    pub validator_version: String,
    pub depth: u32,
    /// ⛔ **Always `false` in this build.** Propose-only: this crate contains no
    /// execution path. The field exists so the wiring phase has somewhere to
    /// record the flip, and so a test can assert it has not moved.
    pub executed: bool,
}

/// Exactly one outcome per proposal. Never both, never neither.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Decision {
    /// Mode is `off` — the proposal was not considered at all.
    NotConsidered,
    Rejected {
        rule: RejectionRule,
        detail: String,
    },
    /// Passed every automatic gate but needs a human. **Not** admitted.
    AwaitingApproval {
        carrier: CatalogCarrier,
        tool_kind: String,
    },
    Admitted(Admission),
}

impl Decision {
    pub fn rejection_rule(&self) -> Option<RejectionRule> {
        match self {
            Decision::Rejected { rule, .. } => Some(*rule),
            _ => None,
        }
    }

    pub fn is_admitted(&self) -> bool {
        matches!(self, Decision::Admitted(_))
    }
}

/// `sha256:<hex>` over the canonical JSON form.
///
/// The prefix format is deliberately identical to `ehdb-storage`'s
/// `ObjectDigest::sha256`, pinned by a known-answer test so the two cannot
/// drift apart silently — the same discipline S0 applied to the frame header.
pub fn content_digest(spec: &serde_json::Value) -> String {
    // serde_json's Map is a BTreeMap by default, so `to_string` emits keys in
    // sorted order and the digest is stable across construction order.
    let canonical = spec.to_string();
    let mut h = Sha256::new();
    h.update(canonical.as_bytes());
    format!("sha256:{:x}", h.finalize())
}

fn carrier(spec: &serde_json::Value, policy: &Policy) -> CatalogCarrier {
    let digest = content_digest(spec);
    let short = digest.strip_prefix("sha256:").unwrap_or(&digest);
    CatalogCarrier {
        path: format!("{}{}", policy.catalog_prefix, &short[..16.min(short.len())]),
        content_digest: digest,
    }
}

/// Walk the spec for anything that reaches for a credential.
fn credential_reach(spec: &serde_json::Value, policy: &Policy) -> Option<(RejectionRule, String)> {
    fn walk(v: &serde_json::Value, policy: &Policy, found: &mut Option<(RejectionRule, String)>) {
        if found.is_some() {
            return;
        }
        match v {
            serde_json::Value::Object(map) => {
                for (k, val) in map {
                    if found.is_some() {
                        return;
                    }
                    let key = k.to_ascii_lowercase();
                    if key == "auth" {
                        *found = Some((
                            RejectionRule::CredentialReach,
                            "a generated step may not carry an `auth:` block".to_string(),
                        ));
                        return;
                    }
                    if key == "credential" || key == "keychain" || key == "secret" {
                        let alias = val.as_str().unwrap_or_default().to_string();
                        if !policy.allowed_keychain_aliases.iter().any(|a| a == &alias) {
                            *found = Some((
                                RejectionRule::KeychainAliasNotAllowed,
                                format!("alias {alias:?} is not on the allowlist"),
                            ));
                            return;
                        }
                    }
                    walk(val, policy, found);
                }
            }
            serde_json::Value::Array(items) => {
                for item in items {
                    walk(item, policy, found);
                }
            }
            _ => {}
        }
    }
    let mut found = None;
    walk(spec, policy, &mut found);
    found
}

/// Read-shape check for `http`. Returns the failing reason, if any.
///
/// ⚠ This is defence-in-depth for an explicit opt-in, **not** a claim that a
/// constrained `http` is safe. The URL remains the unconstrainable part; the
/// host allowlist is what bounds it, and it is empty by default.
fn http_not_read_shaped(spec: &serde_json::Value, policy: &Policy) -> Option<String> {
    let tool = spec.get("tool")?;

    for body_field in ["body", "json", "form"] {
        if tool.get(body_field).map(|v| !v.is_null()).unwrap_or(false) {
            return Some(format!("`{body_field}` present: not a read-shaped request"));
        }
    }

    let method = tool
        .get("method")
        .and_then(|m| m.as_str())
        .unwrap_or("GET")
        .trim()
        .to_ascii_uppercase();
    if method != "GET" && method != "HEAD" {
        return Some(format!("method {method} is not GET or HEAD"));
    }

    let url = match tool.get("url").and_then(|u| u.as_str()) {
        Some(u) => u,
        None => return Some("no url".to_string()),
    };
    if policy.http_allowed_hosts.is_empty() {
        return Some(
            "NOETL_SLM_HTTP_ALLOWED_HOSTS is empty: an arbitrary-host GET still \
             permits exfiltration and SSRF, so no host is admissible"
                .to_string(),
        );
    }
    let host = url
        .split("://")
        .nth(1)
        .unwrap_or(url)
        .split('/')
        .next()
        .unwrap_or("")
        .split('@')
        .next_back()
        .unwrap_or("")
        .split(':')
        .next()
        .unwrap_or("")
        .to_ascii_lowercase();
    if !policy.http_allowed_hosts.iter().any(|h| h == &host) {
        return Some(format!(
            "host {host:?} is not on NOETL_SLM_HTTP_ALLOWED_HOSTS"
        ));
    }
    None
}

fn tool_kind_of(spec: &serde_json::Value) -> Option<String> {
    spec.get("tool")
        .and_then(|t| t.get("kind"))
        .or_else(|| spec.get("kind"))
        .and_then(|k| k.as_str())
        .map(|s| s.trim().to_ascii_lowercase())
}

/// Decide one proposal. Pure: no I/O, no clock, no execution.
pub fn admit(
    spec: &serde_json::Value,
    ctx: &WorkingContext,
    policy: &Policy,
    validator: &dyn DslValidator,
    mode: StepGenMode,
) -> Decision {
    if mode == StepGenMode::Off {
        return Decision::NotConsidered;
    }

    let reject = |rule: RejectionRule, detail: String| Decision::Rejected { rule, detail };

    // 1 — shape
    if !spec.is_object() {
        return reject(
            RejectionRule::Malformed,
            "proposal is not a JSON object".into(),
        );
    }

    // 2 — budget, before anything expensive
    if let Some(limit) = ctx.budget.exhausted {
        return reject(
            match limit {
                crate::fold::BudgetLimit::Depth => RejectionRule::DepthExceeded,
                _ => RejectionRule::BudgetExhausted,
            },
            format!("budget spent: {limit:?}"),
        );
    }

    // 3 — the real DSL validator, injected
    if let Err(detail) = validator.validate(spec) {
        return reject(RejectionRule::SchemaInvalid, detail);
    }

    // 4 — credential reach
    if let Some((rule, detail)) = credential_reach(spec, policy) {
        return reject(rule, detail);
    }

    // 5 — tool kind, then the human gate
    let Some(kind) = tool_kind_of(spec) else {
        return reject(
            RejectionRule::MissingToolKind,
            "no tool.kind on the proposal".into(),
        );
    };
    // 5 — deny list. Terminal: NOT routed to the human gate, because a denied
    // kind must not be approvable into existence.
    if policy.denied_tool_kinds.iter().any(|k| k == &kind) {
        return reject(
            RejectionRule::ToolKindDenied,
            format!("tool kind {kind:?} is denied and cannot be approved"),
        );
    }

    // 6 — read-shape, for kinds that need it
    if kind == "http" {
        if let Some(why) = http_not_read_shaped(spec, policy) {
            return reject(RejectionRule::HttpNotReadShaped, why);
        }
    }

    let allowed = policy.allowed_tool_kinds.iter().any(|k| k == &kind);
    let carrier = carrier(spec, policy);

    if !allowed {
        return match policy.human_gate {
            HumanGate::Required => Decision::AwaitingApproval {
                carrier,
                tool_kind: kind,
            },
            HumanGate::Off => reject(
                RejectionRule::ToolKindNotAllowed,
                format!("tool kind {kind:?} is not allowed and the human gate is off"),
            ),
        };
    }

    Decision::Admitted(Admission {
        carrier,
        gate: AdmitGate::Auto,
        validator_version: validator.version(),
        depth: ctx.budget.max_depth_seen.saturating_add(1),
        // ⛔ propose-only. No code path in this crate sets this true.
        executed: false,
    })
}
