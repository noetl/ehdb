//! Agent/tier event payloads — **additive** alongside `slm.*`.
//!
//! Modelled directly on `crates/ehdb-slm-context/src/event.rs`
//! (branch `feat/slm-context-s1-s2-events-fold`): a `#[serde(tag = "kind")]`
//! union, no `deny_unknown_fields` anywhere, and a `#[serde(other)] Unknown`
//! arm so a build predating a new kind can still fold a newer log.
//!
//! ⚠ That forward-compatibility rule is copied on purpose, not by habit. A tier
//! mesh is upgraded tier-by-tier, so a lower tier *will* emit kinds an upper
//! tier does not know yet. Failing the fold there would make a rolling upgrade
//! an outage.

use serde::{Deserialize, Serialize};

/// Bumped only on a **breaking** payload change; additive optional fields do not.
pub const SIGNAL_MESH_PAYLOAD_VERSION: u32 = 1;

/// A device reading as it enters the mesh.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SignalObserved {
    pub device_id: String,
    pub signal_class: String,
    pub value: f64,
    /// Collector-assigned monotonic sequence, per device.
    pub device_seq: u64,
}

/// One ReAct turn: what the agent saw, concluded, and did.
///
/// The trace is an event, so a decision is replayable rather than merely logged.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AgentReasoned {
    pub agent_id: String,
    pub tier: u8,
    /// `observe` / `reason` / `act` — the ReAct loop phase this records.
    pub phase: String,
    pub thought: String,
    /// Inputs this turn consumed, by event sequence. Makes the turn auditable
    /// against the exact prefix it saw.
    pub observed_seqs: Vec<u64>,
}

/// A tier's reduced value, emitted for the tier above to read.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AggregateEmitted {
    pub agent_id: String,
    pub tier: u8,
    pub value: f64,
    /// How many tier-below inputs this reduced.
    pub input_count: u32,
    /// The highest input sequence folded. This is the **bounded-read
    /// watermark** the tier above quotes when it reads this aggregate.
    pub up_to_seq: u64,
}

/// The top of the mesh: one number and one boolean.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct VerdictSynthesised {
    pub agent_id: String,
    /// The numerical definition-function of the accumulated reasoning.
    pub value: f64,
    /// The boolean the number is thresholded into.
    pub decision: bool,
    pub threshold: f64,
    pub up_to_seq: u64,
}

/// An A2A Task state transition, recorded as an event.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TaskTransitioned {
    pub task_id: String,
    pub from_agent: String,
    pub to_agent: String,
    /// One of [`crate::task::TaskState`]'s wire names.
    pub state: String,
}

/// An agent published its Agent Card (A2A discovery), recorded so the mesh's
/// topology is replayable too.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AgentCardPublished {
    pub agent_id: String,
    pub tier: u8,
    /// Digest of the card's canonical bytes — the card itself lives in the
    /// catalog, per the F2 "catalog as carrier" pattern.
    pub card_digest: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind")]
pub enum MeshEvent {
    #[serde(rename = "mesh.signal.observed")]
    SignalObserved(SignalObserved),
    #[serde(rename = "mesh.agent.reasoned")]
    AgentReasoned(AgentReasoned),
    #[serde(rename = "mesh.aggregate.emitted")]
    AggregateEmitted(AggregateEmitted),
    #[serde(rename = "mesh.verdict.synthesised")]
    VerdictSynthesised(VerdictSynthesised),
    #[serde(rename = "mesh.task.transitioned")]
    TaskTransitioned(TaskTransitioned),
    #[serde(rename = "mesh.agent.card_published")]
    AgentCardPublished(AgentCardPublished),
    #[serde(other)]
    Unknown,
}

impl MeshEvent {
    pub fn kind(&self) -> Option<&'static str> {
        Some(match self {
            MeshEvent::SignalObserved(_) => "mesh.signal.observed",
            MeshEvent::AgentReasoned(_) => "mesh.agent.reasoned",
            MeshEvent::AggregateEmitted(_) => "mesh.aggregate.emitted",
            MeshEvent::VerdictSynthesised(_) => "mesh.verdict.synthesised",
            MeshEvent::TaskTransitioned(_) => "mesh.task.transitioned",
            MeshEvent::AgentCardPublished(_) => "mesh.agent.card_published",
            MeshEvent::Unknown => return None,
        })
    }

    pub fn from_payload(bytes: &[u8]) -> Result<Self, serde_json::Error> {
        serde_json::from_slice(bytes)
    }
}

/// One record in the log: an envelope plus the payload.
///
/// `seq` stands in for EHDB's `global_sequence`; `stream` for `execution_id`.
/// ⚠ Per the SLM fold's C5 note, `seq` is **per-stream**, not a global order.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Record {
    pub seq: u64,
    pub stream: String,
    pub payload: MeshEvent,
}
