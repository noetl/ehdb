//! The deterministic fold: event prefix → a tier's working context.
//!
//! Contract copied from `crates/ehdb-slm-context/src/fold.rs`
//! (branch `feat/slm-context-s1-s2-events-fold`): a **pure** function of
//! `(records, up_to_seq)`. No clock, no I/O, no interior mutability, no
//! hash-ordered output. Each of those is a way the same input could produce a
//! different context, which is the property the whole replay story rests on.
//!
//! # `up_to_seq` is the bounded read
//!
//! A tier never reads "the latest" from the tier below — it reads *as of* a
//! watermark it names. That is what makes a cascade reproducible: re-running
//! tier N against the same watermark yields the same aggregate, whatever tier
//! N-1 has appended since. It is the same shape as the closed-timestamp
//! bounded-staleness read in the multi-region work (`feat/mr-cluster-a`:
//! `crates/ehdb-core/src/hlc.rs`), reduced to a single-engine sequence.

use ehdb_core::plan::ReadConsistency;

use crate::event::{MeshEvent, Record};

#[derive(Debug, Clone, PartialEq)]
pub enum FoldError {
    /// Input was not in ascending sequence order. Sorting here would hide a
    /// caller bug and make the output depend on the caller's ordering.
    UnsortedInput { at: usize, prev: u64, got: u64 },
    /// A record from a different stream. Folding a neighbour's signals
    /// silently is worse than refusing.
    ForeignStream {
        at: usize,
        expected: String,
        got: String,
    },
}

impl std::fmt::Display for FoldError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FoldError::UnsortedInput { at, prev, got } => {
                write!(f, "unsorted input at {at}: {prev} then {got}")
            }
            FoldError::ForeignStream { at, expected, got } => {
                write!(f, "foreign stream at {at}: expected {expected}, got {got}")
            }
        }
    }
}

/// What one tier has observed, as of a watermark.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct TierContext {
    /// Raw signal values visible in the prefix (tier 0 input).
    pub signals: Vec<f64>,
    /// Aggregates emitted by the tier below, most recent per agent.
    pub inputs: Vec<TierInput>,
    /// Highest sequence actually folded. ⚠ May be **less** than the requested
    /// `up_to_seq` when the log has not reached it yet — that gap is the
    /// staleness, and it is reported rather than hidden.
    pub folded_through: u64,
    /// The watermark that was requested.
    pub requested_up_to: u64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct TierInput {
    pub agent_id: String,
    pub value: f64,
    pub input_count: u32,
    pub up_to_seq: u64,
}

impl TierContext {
    /// How far behind the requested watermark this context is.
    ///
    /// ⚠ Exposed deliberately. A tier that cannot tell "I read everything" from
    /// "I read what existed" will report a confident aggregate over a partial
    /// prefix — the same shape as serving a projection that is behind without
    /// knowing it.
    pub fn staleness(&self) -> u64 {
        self.requested_up_to.saturating_sub(self.folded_through)
    }

    pub fn is_complete(&self) -> bool {
        self.staleness() == 0
    }
}

/// Fold a record prefix into one tier's context.
///
/// Pure. `records` must be ascending by `seq` and all from `stream`.
pub fn fold(records: &[Record], stream: &str, up_to_seq: u64) -> Result<TierContext, FoldError> {
    let mut ctx = TierContext {
        requested_up_to: up_to_seq,
        ..Default::default()
    };
    let mut prev: Option<u64> = None;

    for (i, r) in records.iter().enumerate() {
        if let Some(p) = prev {
            if r.seq <= p {
                return Err(FoldError::UnsortedInput {
                    at: i,
                    prev: p,
                    got: r.seq,
                });
            }
        }
        prev = Some(r.seq);

        if r.stream != stream {
            return Err(FoldError::ForeignStream {
                at: i,
                expected: stream.to_string(),
                got: r.stream.clone(),
            });
        }

        // The bound. Everything after the watermark is invisible to this read.
        if r.seq > up_to_seq {
            continue;
        }
        ctx.folded_through = r.seq;

        match &r.payload {
            MeshEvent::SignalObserved(s) => ctx.signals.push(s.value),
            MeshEvent::AggregateEmitted(a) => {
                // Last-writer-wins per agent, in sequence order. Keeps the
                // result independent of how many times a tier re-emitted.
                if let Some(slot) = ctx.inputs.iter_mut().find(|x| x.agent_id == a.agent_id) {
                    slot.value = a.value;
                    slot.input_count = a.input_count;
                    slot.up_to_seq = a.up_to_seq;
                } else {
                    ctx.inputs.push(TierInput {
                        agent_id: a.agent_id.clone(),
                        value: a.value,
                        input_count: a.input_count,
                        up_to_seq: a.up_to_seq,
                    });
                }
            }
            // Traces, task transitions and card publications are provenance,
            // not inputs to the numeric reduction.
            _ => {}
        }
    }
    Ok(ctx)
}

/// Whether a context is fresh enough to act on, under the platform's real
/// read-consistency policy.
///
/// ⚠⚠ **The units differ, and that is stated rather than papered over.**
/// [`ReadConsistency::Bounded`] carries `max_staleness_millis` — a wall-clock
/// budget, because M3's closed timestamp is a time. This POC has **no clock**;
/// its bound is a *sequence gap*. Reusing the millisecond field as if it were a
/// sequence count would typecheck and be wrong.
///
/// So the conversion is explicit and one-way: the caller states how many
/// sequence units it is willing to treat as one millisecond of budget. A real
/// implementation would carry an HLC and call
/// [`ehdb_l0::closed_timestamp::admits`] directly instead.
///
/// The enum itself is reused rather than re-declared, so the mesh speaks the
/// platform's vocabulary — `strong` / `bounded` / `exact` — and a reader does
/// not have to learn a second one.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FreshnessRefusal {
    /// The fold is further behind its watermark than the policy allows.
    TooStale { staleness: u64, allowed: u64 },
    /// `Exact` asks for a specific point in time, which a clockless POC cannot
    /// evaluate. Refused rather than approximated.
    ExactUnsupported,
}

impl std::fmt::Display for FreshnessRefusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FreshnessRefusal::TooStale { staleness, allowed } => {
                write!(f, "staleness {staleness} exceeds the allowed {allowed}")
            }
            FreshnessRefusal::ExactUnsupported => {
                write!(
                    f,
                    "ReadConsistency::Exact needs a clock this POC does not have"
                )
            }
        }
    }
}

/// Apply the platform's read-consistency policy to a folded context.
///
/// `seq_per_milli` is the caller's declared exchange rate between the POC's
/// sequence gap and M3's millisecond budget. There is no correct universal
/// value — which is the point of making it an argument rather than a constant.
pub fn admits(
    ctx: &TierContext,
    consistency: ReadConsistency,
    seq_per_milli: u64,
) -> Result<(), FreshnessRefusal> {
    match consistency {
        // Strong does not consult a freshness budget at all — it is served by
        // the owner and complete by construction. Same reasoning as
        // `ehdb_l0::closed_timestamp::admits`.
        ReadConsistency::Strong => {
            if ctx.staleness() == 0 {
                Ok(())
            } else {
                Err(FreshnessRefusal::TooStale {
                    staleness: ctx.staleness(),
                    allowed: 0,
                })
            }
        }
        ReadConsistency::Bounded {
            max_staleness_millis,
        } => {
            let allowed = max_staleness_millis.saturating_mul(seq_per_milli);
            if ctx.staleness() <= allowed {
                Ok(())
            } else {
                Err(FreshnessRefusal::TooStale {
                    staleness: ctx.staleness(),
                    allowed,
                })
            }
        }
        ReadConsistency::Exact { .. } => Err(FreshnessRefusal::ExactUnsupported),
    }
}

/// How a tier reduces what it sees to one number.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Reduction {
    /// Sum of inputs. Used where magnitudes add (counts, energy).
    Sum,
    /// Count-weighted mean. Used where inputs are themselves averages over
    /// different population sizes.
    ///
    /// ⚠ This is the one that is easy to get wrong: a plain mean of means
    /// silently discards the weights, and the error is invisible whenever the
    /// weights happen to be equal — which is exactly what a uniform test
    /// fixture makes them.
    WeightedMean,
    /// Largest input. Used for worst-case escalation.
    Max,
}

/// Reduce a tier context to a single value.
///
/// Tier 0 reduces raw `signals`; higher tiers reduce `inputs` from below.
pub fn reduce(ctx: &TierContext, how: Reduction) -> f64 {
    if !ctx.inputs.is_empty() {
        return match how {
            Reduction::Sum => ctx.inputs.iter().map(|i| i.value).sum(),
            Reduction::Max => ctx
                .inputs
                .iter()
                .map(|i| i.value)
                .fold(f64::NEG_INFINITY, f64::max),
            Reduction::WeightedMean => {
                let total_w: f64 = ctx.inputs.iter().map(|i| i.input_count as f64).sum();
                if total_w == 0.0 {
                    return 0.0;
                }
                let acc: f64 = ctx
                    .inputs
                    .iter()
                    .map(|i| i.value * i.input_count as f64)
                    .sum();
                acc / total_w
            }
        };
    }
    if ctx.signals.is_empty() {
        return 0.0;
    }
    match how {
        Reduction::Sum => ctx.signals.iter().sum(),
        Reduction::Max => ctx
            .signals
            .iter()
            .copied()
            .fold(f64::NEG_INFINITY, f64::max),
        Reduction::WeightedMean => {
            // Raw signals carry weight 1 each, so this is the plain mean.
            ctx.signals.iter().sum::<f64>() / ctx.signals.len() as f64
        }
    }
}

/// Total inputs behind a tier's value — the weight it passes upward.
///
/// ⚠ A tier must pass the **population size**, not the number of children.
/// Passing the child count is what turns a weighted mean into a plain one, one
/// tier up, with no visible symptom.
pub fn population(ctx: &TierContext) -> u32 {
    if !ctx.inputs.is_empty() {
        return ctx.inputs.iter().map(|i| i.input_count).sum();
    }
    ctx.signals.len() as u32
}
