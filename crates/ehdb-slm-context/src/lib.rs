//! SLM execution context — event payloads (S1) and the deterministic fold (S2).
//!
//! Implements phases S1 and S2 of ai-meta
//! `specs/active/2026-09-19-slm-ehdb-context/`. Design source:
//! `loops/active/2026-09-11-ehdb-resilient-core-phases/handover/SLM-EHDB-CONTEXT-PLAN.md`.
//!
//! # What this crate is
//!
//! The context of an AI-driven noetl run, expressed as **append-only payloads
//! on the existing event log (D1)** plus a **pure fold** that assembles the
//! working context for the next model call. It adds no dataset, no tool kind
//! and no execution primitive — see the plan's §2.3 for why none is needed.
//!
//! # What this crate is NOT
//!
//! It calls no model, emits no event and reads no store. It is the substrate:
//! types plus a pure function. Emission and fold call sites are wiring in the
//! server, gated by [`SlmContextFlags`], and land in a later phase. **With no
//! caller, this crate is inert by construction** — which is a stronger
//! statement than a flag default, and is why S1/S2 can land before S3's open
//! forks are settled.
//!
//! # The compatibility rule this crate obeys (S0, proven)
//!
//! ⛔ Never add a field to the record envelope. `StreamRecord` is
//! `#[serde(deny_unknown_fields)]`, so an older reader **rejects** an added
//! envelope field.
//! ✅ Context data lives **inside the opaque payload**, versioned, with
//! `#[serde(default)]` + `skip_serializing_if` on every added field.
//! Pinned by `ehdb-stream/tests/record_envelope_additivity.rs`.
//!
//! An **unknown event kind** is skipped by the fold rather than failing it, and
//! counted in [`WorkingContext::skipped_unknown`] — so an old build reading a
//! newer log degrades visibly instead of silently.

#![forbid(unsafe_code)]

pub mod event;
pub mod fold;

pub use event::{
    Rejection, SlmContextEvent, StepSpecRef, Summary, TurnCompleted, TurnDegraded, TurnPrompted,
    SLM_CONTEXT_PAYLOAD_VERSION,
};
pub use fold::{fold, Budget, BudgetLimit, FoldError, FoldVersion, Turn, WorkingContext};

/// Tri-state gate shared by every SLM-context capability.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Mode {
    /// Feature absent. The default everywhere.
    #[default]
    Off,
    /// Compute and record, but let nothing act on the result.
    Shadow,
    /// Live.
    On,
}

impl Mode {
    /// Parse a flag value. Anything unrecognised is [`Mode::Off`] — a
    /// misspelled flag must not silently arm a feature.
    pub fn parse(raw: &str) -> Self {
        match raw.trim().to_ascii_lowercase().as_str() {
            "shadow" => Mode::Shadow,
            "on" | "true" | "1" => Mode::On,
            _ => Mode::Off,
        }
    }

    pub fn is_off(self) -> bool {
        matches!(self, Mode::Off)
    }
}

/// The flags gating SLM context, read in one place so there is one answer.
///
/// Names are spelled literally in [`SlmContextFlags::from_env`] via
/// `std::env::var`, so a `grep NOETL_SLM` finds every read. A helper wrapper
/// would hide them from exactly that scan.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct SlmContextFlags {
    /// `NOETL_SLM_CONTEXT_EVENTS` — emit the S1 payloads. Default `off`.
    pub events: Mode,
    /// `NOETL_SLM_CONTEXT_FOLD` — compute the S2 working context. Default `off`.
    pub fold: Mode,
}

impl SlmContextFlags {
    pub fn from_env() -> Self {
        Self {
            events: std::env::var("NOETL_SLM_CONTEXT_EVENTS")
                .map(|v| Mode::parse(&v))
                .unwrap_or_default(),
            fold: std::env::var("NOETL_SLM_CONTEXT_FOLD")
                .map(|v| Mode::parse(&v))
                .unwrap_or_default(),
        }
    }

    /// True when nothing is armed — the default, and the state in which this
    /// crate's presence is unobservable.
    pub fn all_off(self) -> bool {
        self.events.is_off() && self.fold.is_off()
    }
}

#[cfg(test)]
mod flag_tests {
    use super::*;

    #[test]
    fn default_is_off() {
        let f = SlmContextFlags::default();
        assert!(f.all_off());
    }

    #[test]
    fn unrecognised_values_are_off_not_on() {
        for raw in ["", "yes", "enabled", "ON!", "shadowy", "0", "off"] {
            assert_eq!(Mode::parse(raw), Mode::Off, "{raw:?} must not arm anything");
        }
    }

    #[test]
    fn recognised_values_parse() {
        assert_eq!(Mode::parse("shadow"), Mode::Shadow);
        assert_eq!(Mode::parse("SHADOW"), Mode::Shadow);
        assert_eq!(Mode::parse(" on "), Mode::On);
        assert_eq!(Mode::parse("true"), Mode::On);
        assert_eq!(Mode::parse("1"), Mode::On);
    }
}
