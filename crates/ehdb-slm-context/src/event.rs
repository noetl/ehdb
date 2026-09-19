//! S1 — the SLM context event payloads.
//!
//! Six kinds, all carried **inside** the record payload (S0's rule), all
//! versioned, all tolerant of unknown keys. `slm.step.*` are declared here and
//! emitted by S3, so the envelope and compatibility story is settled once.

use serde::{Deserialize, Serialize};

/// Payload schema version. Bumped only on a **breaking** payload change;
/// additive optional fields do not bump it.
pub const SLM_CONTEXT_PAYLOAD_VERSION: u32 = 1;

/// A reference to a payload that is too large to inline, per the plan's size
/// floor. Mirrors the platform's existing reference-first result model.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResultRefStr(pub String);

/// Inline bytes or a reference. Never both, never neither.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Body {
    Inline(String),
    Ref(ResultRefStr),
}

impl Body {
    /// Bytes a digest is taken over, when inline. A reference has none here —
    /// its digest travels in the sibling `*_digest` field.
    pub fn inline_str(&self) -> Option<&str> {
        match self {
            Body::Inline(s) => Some(s),
            Body::Ref(_) => None,
        }
    }
}

/// Which model answered. A bare name is not a pin — see the plan §8.1.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModelRef {
    pub family: String,
    pub variant: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub digest: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub server: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub api: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Sampling {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub temperature: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub top_p: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub seed: Option<i64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_tokens: Option<u32>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TurnPrompted {
    pub v: u32,
    pub execution_id: String,
    pub turn: u32,
    pub model: ModelRef,
    pub prompt: Body,
    pub prompt_digest: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sampling: Option<Sampling>,
    /// The event ids this prompt was folded from — the provenance edge that
    /// makes the fold auditable rather than merely deterministic.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub source_event_ids: Vec<u64>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TurnCompleted {
    pub v: u32,
    pub execution_id: String,
    pub turn: u32,
    pub completion: Body,
    pub completion_digest: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prompt_tokens: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub completion_tokens: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub latency_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub finish_reason: Option<String>,
}

/// Why a turn did not produce a usable completion, and what was done instead.
///
/// Mirrors the degrade-to-escalate behaviour already in the tree
/// (`diagnose_execution.yaml:432` forces `confidence = 0.0` on a parse
/// failure). Degrading is recorded, never inferred from absence.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TurnDegraded {
    pub v: u32,
    pub execution_id: String,
    pub turn: u32,
    pub reason: DegradeReason,
    pub fallback: Fallback,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DegradeReason {
    ParseFailure,
    Timeout,
    Refusal,
    Transport,
    Other,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Fallback {
    Escalate,
    Abort,
    ComputedFindings,
}

/// A step the model proposed. Declared in S1, emitted by S3.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StepProposed {
    pub v: u32,
    pub execution_id: String,
    pub turn: u32,
    pub spec: serde_json::Value,
    pub content_digest: String,
}

/// A proposal that passed every gate. Declared in S1, emitted by S3.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StepAdmitted {
    pub v: u32,
    pub execution_id: String,
    pub content_digest: String,
    pub validator_version: String,
    pub gate: AdmitGate,
    #[serde(default)]
    pub depth: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AdmitGate {
    Auto,
    Human,
}

/// A proposal that failed a gate. Declared in S1, emitted by S3.
///
/// ⭐ Not bookkeeping — the instrument. A phase that can only observe
/// admissions cannot distinguish "the model proposes nothing invalid" from
/// "the validator never runs".
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StepRejected {
    pub v: u32,
    pub execution_id: String,
    pub content_digest: String,
    /// The rule that refused it. A label, so rejections are countable by cause.
    pub rule: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
}

/// Compaction output (S5). Declared here so the fold can already skip it
/// forward-compatibly rather than counting it as unknown.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContextSummarised {
    pub v: u32,
    pub execution_id: String,
    pub summary: String,
    pub replaces_from_event_id: u64,
    pub replaces_to_event_id: u64,
}

/// The tagged payload union written into `StreamRecord.payload`.
///
/// ⚠ No `deny_unknown_fields` anywhere in this file, deliberately: forward
/// compatibility is the whole point of the payload path (S0). An unrecognised
/// `kind` deserialises to [`SlmContextEvent::Unknown`] instead of failing, so a
/// build predating a new event type can still fold a newer log.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind")]
pub enum SlmContextEvent {
    #[serde(rename = "slm.turn.prompted")]
    TurnPrompted(TurnPrompted),
    #[serde(rename = "slm.turn.completed")]
    TurnCompleted(TurnCompleted),
    #[serde(rename = "slm.turn.degraded")]
    TurnDegraded(TurnDegraded),
    #[serde(rename = "slm.step.proposed")]
    StepProposed(StepProposed),
    #[serde(rename = "slm.step.admitted")]
    StepAdmitted(StepAdmitted),
    #[serde(rename = "slm.step.rejected")]
    StepRejected(StepRejected),
    #[serde(rename = "slm.context.summarised")]
    ContextSummarised(ContextSummarised),
    #[serde(other)]
    Unknown,
}

impl SlmContextEvent {
    /// The wire `kind`, or `None` for an unrecognised one.
    pub fn kind(&self) -> Option<&'static str> {
        Some(match self {
            SlmContextEvent::TurnPrompted(_) => "slm.turn.prompted",
            SlmContextEvent::TurnCompleted(_) => "slm.turn.completed",
            SlmContextEvent::TurnDegraded(_) => "slm.turn.degraded",
            SlmContextEvent::StepProposed(_) => "slm.step.proposed",
            SlmContextEvent::StepAdmitted(_) => "slm.step.admitted",
            SlmContextEvent::StepRejected(_) => "slm.step.rejected",
            SlmContextEvent::ContextSummarised(_) => "slm.context.summarised",
            SlmContextEvent::Unknown => return None,
        })
    }

    /// Parse one payload. An unrecognised `kind` is `Unknown`, not an error;
    /// malformed JSON, or a recognised kind with a broken body, still errors.
    pub fn from_payload(bytes: &[u8]) -> Result<Self, serde_json::Error> {
        serde_json::from_slice(bytes)
    }
}

/// A proposal reference as the fold surfaces it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StepSpecRef {
    pub content_digest: String,
    pub gate: AdmitGate,
    pub depth: u32,
}

/// A rejection as the fold surfaces it — fed back to the model so it stops
/// repeating the same invalid proposal.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Rejection {
    pub content_digest: String,
    pub rule: String,
}

/// A summary as the fold surfaces it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Summary {
    pub summary: String,
    pub replaces_from_event_id: u64,
    pub replaces_to_event_id: u64,
}
