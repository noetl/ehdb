//! The ReAct loop, one per agent — and the HAMMR shape one tier up.
//!
//! **ReAct** (Yao et al., *Synergizing Reasoning and Acting in Language
//! Models*, arXiv:2210.03629, ICLR 2023) interleaves a thought with an action
//! and an observation, rather than reasoning to completion and then acting.
//!
//! **HAMMR** (Castrejon et al., *HierArchical MultiModal React agents for
//! generic VQA*, arXiv:2404.05465, NeurIPS 2024 Workshop) makes that
//! hierarchical: a dispatcher ReAct agent whose **actions are themselves ReAct
//! agents**. That is precisely the tier relationship here — a tier-N agent's
//! `Act` is "send an A2A Task to a tier-(N-1) agent", and the sub-agent runs
//! its own loop.
//!
//! # Why the reasoner is stubbed by default
//!
//! The POC's claim is about the **cascade and its replayability**, not about
//! model quality. A deterministic reasoner makes the whole run reproducible, so
//! a failing assertion is a defect in the mesh rather than sampling noise. The
//! model-backed reasoner sits behind a flag and is not what any test asserts.

use crate::fold::{population, reduce, Reduction, TierContext};

/// One step of the loop.
#[derive(Debug, Clone, PartialEq)]
pub enum Phase {
    Observe,
    Reason,
    Act,
}

impl Phase {
    pub fn as_str(&self) -> &'static str {
        match self {
            Phase::Observe => "observe",
            Phase::Reason => "reason",
            Phase::Act => "act",
        }
    }
}

/// What a turn concluded.
#[derive(Debug, Clone, PartialEq)]
pub struct Turn {
    pub phase: Phase,
    pub thought: String,
    /// Present on `Act`: the value this agent will emit upward.
    pub value: Option<f64>,
    /// Present on `Act`: the population weight travelling with it.
    pub population: Option<u32>,
}

/// A reasoner turns a context into a thought + a value.
///
/// Trait-shaped so the deterministic and model-backed implementations are
/// interchangeable and only one of them is ever on a test path.
pub trait Reasoner {
    fn name(&self) -> &'static str;
    fn reason(&self, agent_id: &str, tier: u8, ctx: &TierContext, how: Reduction) -> Turn;
}

/// The default. Pure arithmetic over the folded context — no model, no clock,
/// no randomness.
#[derive(Debug, Clone, Copy, Default)]
pub struct DeterministicReasoner;

impl Reasoner for DeterministicReasoner {
    fn name(&self) -> &'static str {
        "deterministic"
    }

    fn reason(&self, agent_id: &str, tier: u8, ctx: &TierContext, how: Reduction) -> Turn {
        let value = reduce(ctx, how);
        let pop = population(ctx);
        // ⚠ The thought names the staleness. An agent that reasons over a
        // partial prefix and does not say so is indistinguishable from one that
        // read everything.
        let thought = format!(
            "agent={agent_id} tier={tier} reduced {} input(s) (population {pop}) to {value:.4} \
             as of seq {} (requested {}, staleness {})",
            ctx.inputs.len().max(ctx.signals.len()),
            ctx.folded_through,
            ctx.requested_up_to,
            ctx.staleness()
        );
        Turn {
            phase: Phase::Act,
            thought,
            value: Some(value),
            population: Some(pop),
        }
    }
}

/// Run one observe→reason→act cycle.
///
/// Returns the three turns in order, so the caller records each as an event and
/// the trace is replayable.
pub fn run_cycle(
    agent_id: &str,
    tier: u8,
    ctx: &TierContext,
    how: Reduction,
    reasoner: &dyn Reasoner,
) -> Vec<Turn> {
    let observe = Turn {
        phase: Phase::Observe,
        thought: format!(
            "agent={agent_id} tier={tier} observed {} signal(s) and {} lower-tier aggregate(s) \
             through seq {}",
            ctx.signals.len(),
            ctx.inputs.len(),
            ctx.folded_through
        ),
        value: None,
        population: None,
    };
    let reason = Turn {
        phase: Phase::Reason,
        thought: format!(
            "agent={agent_id} tier={tier} applying {:?} via {}",
            how,
            reasoner.name()
        ),
        value: None,
        population: None,
    };
    let act = reasoner.reason(agent_id, tier, ctx, how);
    vec![observe, reason, act]
}
