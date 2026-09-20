//! S2 — the deterministic fold from an event prefix to a working context.
//!
//! `fold()` is a pure function of (events, up_to_seq, version). No clock, no
//! I/O, no interior mutability, no hash-ordered output. Those are not stylistic
//! preferences — each is a way the same input could produce a different
//! `WorkingContext`, which is the one property this phase exists to guarantee.
//!
//! # Scope (C5)
//!
//! `global_sequence` is **per-engine**, not a global order. The fold is scoped
//! to one `execution_id` on one engine and **asserts** it: events for another
//! execution are an error, not a silent skip, because silently folding a
//! neighbour's context is worse than refusing.

use serde::{Deserialize, Serialize};

use crate::event::{
    AdmitGate, Rejection, SlmContextEvent, StepSpecRef, Summary, TurnCompleted, TurnDegraded,
    TurnPrompted,
};

/// Bumped on ANY semantic change to the fold. A context is only comparable to
/// another produced at the same version.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct FoldVersion(pub u32);

pub const CURRENT_FOLD_VERSION: FoldVersion = FoldVersion(1);

#[derive(Debug, Clone, PartialEq)]
pub enum FoldError {
    /// Input was not in ascending sequence order. Sorting it here would hide a
    /// caller bug and make the fold's output depend on the caller's ordering.
    UnsortedInput { at: usize, prev: u64, got: u64 },
    /// An event belonging to a different execution. See C5.
    ForeignExecution {
        at: usize,
        expected: String,
        got: String,
    },
    /// A recognised payload that would not parse.
    Malformed { at: usize, detail: String },
}

impl std::fmt::Display for FoldError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FoldError::UnsortedInput { at, prev, got } => {
                write!(f, "events not ascending at index {at}: {prev} then {got}")
            }
            FoldError::ForeignExecution { at, expected, got } => {
                write!(
                    f,
                    "event {at} belongs to execution {got}, folding {expected}"
                )
            }
            FoldError::Malformed { at, detail } => write!(f, "event {at} malformed: {detail}"),
        }
    }
}

impl std::error::Error for FoldError {}

/// One model turn, paired from its prompted/completed/degraded events.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Turn {
    pub turn: u32,
    pub prompted: Option<TurnPrompted>,
    pub completed: Option<TurnCompleted>,
    pub degraded: Option<TurnDegraded>,
}

impl Turn {
    /// A turn that produced no usable completion. Used by the budget: a
    /// degraded turn still costs a turn, which is what stops a degrade loop
    /// from being free.
    pub fn is_degraded(&self) -> bool {
        self.degraded.is_some()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BudgetLimit {
    GeneratedSteps,
    Depth,
    Turns,
}

/// The bounds that stop a generate loop expanding forever, and how much of each
/// is spent. Exhaustion is a **recorded terminal state**, never a silent stop.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Budget {
    pub max_generated_steps: u32,
    pub max_depth: u32,
    pub max_turns: u32,
    pub steps_admitted: u32,
    pub max_depth_seen: u32,
    pub turns_used: u32,
    pub exhausted: Option<BudgetLimit>,
}

impl Default for Budget {
    /// The plan's recommended defaults (§10 gate 4).
    fn default() -> Self {
        Self {
            max_generated_steps: 8,
            max_depth: 2,
            max_turns: 16,
            steps_admitted: 0,
            max_depth_seen: 0,
            turns_used: 0,
            exhausted: None,
        }
    }
}

impl Budget {
    fn recompute_exhaustion(&mut self) {
        // Checked in a fixed order so the reported limit is deterministic when
        // more than one is spent.
        self.exhausted = if self.steps_admitted >= self.max_generated_steps {
            Some(BudgetLimit::GeneratedSteps)
        } else if self.max_depth_seen >= self.max_depth {
            Some(BudgetLimit::Depth)
        } else if self.turns_used >= self.max_turns {
            Some(BudgetLimit::Turns)
        } else {
            None
        };
    }

    pub fn is_exhausted(&self) -> bool {
        self.exhausted.is_some()
    }
}

/// The working context for the next model call.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WorkingContext {
    pub execution_id: String,
    pub up_to_seq: u64,
    pub fold_version: FoldVersion,
    pub turns: Vec<Turn>,
    pub admitted: Vec<StepSpecRef>,
    pub rejected: Vec<Rejection>,
    pub summaries: Vec<Summary>,
    pub budget: Budget,
    /// Events whose `kind` this build does not recognise. ⭐ Surfaced rather
    /// than dropped: an old build folding a newer log degrades **visibly**.
    pub skipped_unknown: u32,
}

impl WorkingContext {
    fn empty(execution_id: String, up_to_seq: u64, version: FoldVersion, budget: Budget) -> Self {
        Self {
            execution_id,
            up_to_seq,
            fold_version: version,
            turns: Vec::new(),
            admitted: Vec::new(),
            rejected: Vec::new(),
            summaries: Vec::new(),
            budget,
            skipped_unknown: 0,
        }
    }

    /// A stable byte form for comparing two folds. Field order is the struct's
    /// declaration order and every collection is a `Vec`, so this is
    /// deterministic by construction — there is no map to iterate.
    pub fn canonical_bytes(&self) -> Vec<u8> {
        serde_json::to_vec(self).expect("WorkingContext is plain data and always serialises")
    }
}

/// Fold an ascending prefix of `(sequence, payload)` into a working context.
///
/// `events` must be sorted ascending by sequence and belong to one execution;
/// both are checked, not assumed (C5). Events with `sequence > up_to_seq` are
/// ignored, which is what makes a prefix fold a prefix.
pub fn fold(
    execution_id: &str,
    events: &[(u64, Vec<u8>)],
    up_to_seq: u64,
    version: FoldVersion,
    budget: Budget,
) -> Result<WorkingContext, FoldError> {
    let mut ctx = WorkingContext::empty(execution_id.to_string(), up_to_seq, version, budget);
    let mut prev_seq: Option<u64> = None;

    for (idx, (seq, payload)) in events.iter().enumerate() {
        if let Some(p) = prev_seq {
            if *seq <= p {
                return Err(FoldError::UnsortedInput {
                    at: idx,
                    prev: p,
                    got: *seq,
                });
            }
        }
        prev_seq = Some(*seq);

        if *seq > up_to_seq {
            continue;
        }

        let parsed = match SlmContextEvent::from_payload(payload) {
            Ok(e) => e,
            Err(err) => {
                return Err(FoldError::Malformed {
                    at: idx,
                    detail: err.to_string(),
                });
            }
        };

        // Execution scoping, checked per event.
        let owner = match &parsed {
            SlmContextEvent::TurnPrompted(e) => Some(&e.execution_id),
            SlmContextEvent::TurnCompleted(e) => Some(&e.execution_id),
            SlmContextEvent::TurnDegraded(e) => Some(&e.execution_id),
            SlmContextEvent::StepProposed(e) => Some(&e.execution_id),
            SlmContextEvent::StepAdmitted(e) => Some(&e.execution_id),
            SlmContextEvent::StepRejected(e) => Some(&e.execution_id),
            SlmContextEvent::ContextSummarised(e) => Some(&e.execution_id),
            SlmContextEvent::Unknown => None,
        };
        if let Some(owner) = owner {
            if owner != execution_id {
                return Err(FoldError::ForeignExecution {
                    at: idx,
                    expected: execution_id.to_string(),
                    got: owner.clone(),
                });
            }
        }

        match parsed {
            SlmContextEvent::TurnPrompted(e) => {
                let t = e.turn;
                turn_slot(&mut ctx.turns, t).prompted = Some(e);
            }
            SlmContextEvent::TurnCompleted(e) => {
                let t = e.turn;
                turn_slot(&mut ctx.turns, t).completed = Some(e);
            }
            SlmContextEvent::TurnDegraded(e) => {
                let t = e.turn;
                turn_slot(&mut ctx.turns, t).degraded = Some(e);
            }
            SlmContextEvent::StepProposed(_) => {
                // A proposal alone changes nothing: only admission spends
                // budget, and only rejection teaches the model.
            }
            SlmContextEvent::StepAdmitted(e) => {
                ctx.admitted.push(StepSpecRef {
                    content_digest: e.content_digest,
                    gate: e.gate,
                    depth: e.depth,
                });
                ctx.budget.steps_admitted = ctx.budget.steps_admitted.saturating_add(1);
                ctx.budget.max_depth_seen = ctx.budget.max_depth_seen.max(e.depth);
            }
            SlmContextEvent::StepRejected(e) => {
                ctx.rejected.push(Rejection {
                    content_digest: e.content_digest,
                    rule: e.rule,
                });
            }
            SlmContextEvent::ContextSummarised(e) => {
                ctx.summaries.push(Summary {
                    summary: e.summary,
                    replaces_from_event_id: e.replaces_from_event_id,
                    replaces_to_event_id: e.replaces_to_event_id,
                });
            }
            SlmContextEvent::Unknown => {
                ctx.skipped_unknown = ctx.skipped_unknown.saturating_add(1);
            }
        }
    }

    // Turns are appended in first-seen order; sort so the context is ordered by
    // turn number regardless of the order the events arrived in.
    ctx.turns.sort_by_key(|t| t.turn);
    ctx.budget.turns_used = ctx.turns.len() as u32;
    ctx.budget.recompute_exhaustion();
    Ok(ctx)
}

/// Find or create the slot for a turn. Linear over a small vector — a map would
/// introduce iteration-order nondeterminism for no measurable gain at these
/// sizes, and determinism is the property being bought.
fn turn_slot(turns: &mut Vec<Turn>, turn: u32) -> &mut Turn {
    if let Some(pos) = turns.iter().position(|t| t.turn == turn) {
        return &mut turns[pos];
    }
    turns.push(Turn {
        turn,
        prompted: None,
        completed: None,
        degraded: None,
    });
    let last = turns.len() - 1;
    &mut turns[last]
}

/// Re-exported for callers that want the gate type without reaching into
/// `event`.
pub use crate::event::AdmitGate as Gate;

const _: () = {
    // Compile-time reminder that AdmitGate is part of the fold's output shape.
    fn _assert(_: AdmitGate) {}
};
