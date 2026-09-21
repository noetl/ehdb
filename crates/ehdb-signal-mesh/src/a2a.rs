//! A2A subset — Agent Card + Task, as the mesh uses them.
//!
//! Grounded in the A2A specification at <https://a2a-protocol.org/latest/specification/>
//! (latest release **1.0.0**; `Major.Minor` is what version negotiation uses,
//! patch excluded).
//!
//! ⚠ Three corrections to commonly-repeated summaries, worth stating because
//! each one would produce a subtly wrong implementation:
//!
//! 1. The well-known path is **`/.well-known/agent-card.json`**. It was
//!    `agent.json` until v0.3 (2025-07-30) and was registered as a well-known
//!    URI (RFC 8615) in v1.0.
//! 2. `TaskState` has **eight** states, not five. The commonly-quoted
//!    submitted/working/completed/failed/canceled omits `input-required`,
//!    `rejected` and `auth-required` — and two of those three are *interrupted*
//!    rather than terminal, which is exactly the distinction a dispatcher has
//!    to branch on.
//! 3. Extensions are declared as `AgentExtension` objects in the card and
//!    echoed per-message; an extension may be `required: true`.
//!
//! This POC implements the **data model only** — no HTTP, no JSON-RPC. The
//! transport is deliberately out of scope; see the design doc's
//! "what the POC does NOT prove".

use serde::{Deserialize, Serialize};

/// RFC 8615 well-known location for an Agent Card.
pub const AGENT_CARD_WELL_KNOWN_PATH: &str = "/.well-known/agent-card.json";

/// The A2A protocol version this subset models.
pub const A2A_PROTOCOL_VERSION: &str = "1.0";

/// One advertised capability. In the mesh, a skill is "reduce tier N-1 signals
/// of class X".
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Skill {
    pub id: String,
    pub name: String,
    pub description: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tags: Vec<String>,
}

/// Declared extension (A2A `AgentExtension`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentExtension {
    pub uri: String,
    #[serde(default)]
    pub required: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct Capabilities {
    #[serde(default)]
    pub streaming: bool,
    #[serde(default)]
    pub push_notifications: bool,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub extensions: Vec<AgentExtension>,
}

/// The Agent Card: identity, skills, endpoint, auth.
///
/// ⚠ No `deny_unknown_fields`: a card from a newer agent must still parse.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentCard {
    pub name: String,
    pub description: String,
    /// Service endpoint (the `interfaces` binding, flattened for the POC).
    pub url: String,
    pub version: String,
    pub protocol_version: String,
    #[serde(default)]
    pub capabilities: Capabilities,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub skills: Vec<Skill>,
    /// Named security schemes the endpoint requires. Empty = none declared,
    /// which the mesh treats as "not publishable outside the cluster".
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub security_schemes: Vec<String>,
}

impl AgentCard {
    /// Canonical bytes for digesting — sorted keys, no whitespace.
    ///
    /// Mirrors `WorkingContext::canonical_bytes` on the SLM branch: a digest is
    /// only comparable if the serialisation is order-stable.
    pub fn canonical_bytes(&self) -> Vec<u8> {
        let v = serde_json::to_value(self).expect("card serialises");
        canonical_json(&v).into_bytes()
    }

    pub fn digest(&self) -> String {
        fnv1a_hex(&self.canonical_bytes())
    }
}

/// A2A `TaskState`. All eight.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum TaskState {
    Submitted,
    Working,
    InputRequired,
    AuthRequired,
    Completed,
    Failed,
    Canceled,
    Rejected,
}

impl TaskState {
    pub fn as_str(self) -> &'static str {
        match self {
            TaskState::Submitted => "submitted",
            TaskState::Working => "working",
            TaskState::InputRequired => "input-required",
            TaskState::AuthRequired => "auth-required",
            TaskState::Completed => "completed",
            TaskState::Failed => "failed",
            TaskState::Canceled => "canceled",
            TaskState::Rejected => "rejected",
        }
    }

    /// Terminal states admit no further transition.
    pub fn is_terminal(self) -> bool {
        matches!(
            self,
            TaskState::Completed | TaskState::Failed | TaskState::Canceled | TaskState::Rejected
        )
    }

    /// Interrupted states are **not** terminal — the task resumes when the
    /// input or credential arrives. Collapsing these into "failed" is the
    /// single most common way a dispatcher mis-handles A2A.
    pub fn is_interrupted(self) -> bool {
        matches!(self, TaskState::InputRequired | TaskState::AuthRequired)
    }

    pub const ALL: [TaskState; 8] = [
        TaskState::Submitted,
        TaskState::Working,
        TaskState::InputRequired,
        TaskState::AuthRequired,
        TaskState::Completed,
        TaskState::Failed,
        TaskState::Canceled,
        TaskState::Rejected,
    ];
}

/// A stateful unit of work between two agents.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Task {
    pub id: String,
    pub from_agent: String,
    pub to_agent: String,
    pub state: TaskState,
    /// The bounded-read watermark the caller is asking to be answered at.
    pub up_to_seq: u64,
}

impl Task {
    pub fn submit(id: &str, from: &str, to: &str, up_to_seq: u64) -> Self {
        Self {
            id: id.to_string(),
            from_agent: from.to_string(),
            to_agent: to.to_string(),
            state: TaskState::Submitted,
            up_to_seq,
        }
    }

    /// Advance the task. Refuses to move out of a terminal state.
    pub fn transition(&mut self, next: TaskState) -> Result<(), String> {
        if self.state.is_terminal() {
            return Err(format!(
                "task {} is terminal in {}; refusing transition to {}",
                self.id,
                self.state.as_str(),
                next.as_str()
            ));
        }
        self.state = next;
        Ok(())
    }
}

/// Deterministic, dependency-free digest. FNV-1a 64, hex.
///
/// ⚠ Not a cryptographic hash and not claimed to be one — it exists so the POC
/// has a stable content digest with no new dependency. Production would use the
/// platform's existing canonical digest.
pub fn fnv1a_hex(bytes: &[u8]) -> String {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for b in bytes {
        h ^= *b as u64;
        h = h.wrapping_mul(0x1000_0000_01b3);
    }
    format!("{h:016x}")
}

/// Key-sorted, whitespace-free JSON.
pub fn canonical_json(v: &serde_json::Value) -> String {
    match v {
        serde_json::Value::Object(m) => {
            let mut keys: Vec<&String> = m.keys().collect();
            keys.sort();
            let inner: Vec<String> = keys
                .iter()
                .map(|k| {
                    format!(
                        "{}:{}",
                        serde_json::to_string(k).unwrap(),
                        canonical_json(&m[*k])
                    )
                })
                .collect();
            format!("{{{}}}", inner.join(","))
        }
        serde_json::Value::Array(a) => {
            let inner: Vec<String> = a.iter().map(canonical_json).collect();
            format!("[{}]", inner.join(","))
        }
        other => serde_json::to_string(other).unwrap(),
    }
}
