//! **SWIM gossip transport for NoETL membership** (noetl/ai-meta#332).
//!
//! # The split
//!
//! **foca observes; EHDB records.** foca carries transport and failure detection
//! only — every membership transition it reports is appended to EHDB's D8
//! runtime log, and all state, history, recovery and query stay there.
//!
//! This is what `agents/rules/self-sufficiency.md` prescribes: no external
//! discovery *service* (nothing to deploy, operate or quorum), but a proven
//! *library* for the part that fails silently when hand-rolled — SWIM's indirect
//! probing, which distinguishes "I cannot reach X" from "X is down".
//!
//! # Status: INERT
//!
//! ⚠ Nothing here is wired to a running system. There is no socket, no runtime
//! and no bring-up: this crate provides the identity, the notification→D8
//! mapping, and the authorisation seam. Wiring it to a live cluster is a separate,
//! gated step, and [`origin::GossipOrigin`] is deliberately impossible to
//! construct without supplying a verifier so that step cannot be taken by
//! accident.

pub mod identity;
pub mod origin;
pub mod sink;

pub use identity::ShardIdentity;
pub use origin::{GossipOrigin, MembershipVerifier};
pub use sink::{MembershipSink, NotificationKind, SinkOutcome};
