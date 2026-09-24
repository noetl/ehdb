//! **Alternative EventStore backends — sketches, deliberately incomplete.**
//!
//! These exist to prove two things about §12's seam, both of which are claims
//! that are easy to make and easy to get wrong:
//!
//! 1. **The abstraction is real** — a second, structurally different backend
//!    implements [`EventStore`](crate::store_role::EventStore) without the
//!    trait bending to fit EHDB.
//! 2. **The conformance suite discriminates** — it *rejects* a backend that
//!    cannot provide a clause. A suite that passes everything is decoration.
//!
//! ⛔ Neither is production code. [`JetStreamSketchEventStore`] does no
//! networking and embeds no NATS client; it models the *shape* of a
//! subject-per-execution store in memory so the conformance verdict is about
//! the **model**, not about a connection.

use std::collections::BTreeMap;

use crate::chain::{ChainError, ChainEvent, ExecSeq};
use crate::store_role::EventStore;

/// **Sketch: one JetStream subject per `execution_id`.**
///
/// Models what a subject-per-execution store naturally gives you: an ordered
/// append log per subject, with the stream sequence as the position. That much
/// is genuinely good, and this sketch reproduces it faithfully.
///
/// ⚠ What it does **not** naturally give you, and why that is the informative
/// part of the sketch:
///
/// * **No addressed predecessor lookup.** A subject replay yields messages in
///   order; there is no `(subject, message_id) → offset` index. Resolving a
///   predecessor means replaying the subject and scanning — which is the exact
///   cost the restructure exists to remove. Closing this needs a *side* index
///   (JetStream KV), i.e. a second store to keep consistent with the first.
/// * **No notion of a named missing link.** A replay returns what the stream
///   has. A consumer that is behind sees a shorter list, indistinguishable from
///   a complete one — the false-complete that makes a timed-out read look like a
///   finished execution.
///
/// So this sketch **fails conformance**, and it fails on precisely the two
/// clauses the RFC identifies as the point of the redesign. That is the sketch
/// doing its job: it shows the contract has teeth and is not merely a
/// restatement of EHDB's method signatures.
#[derive(Debug, Default)]
pub struct JetStreamSketchEventStore {
    /// subject (`execution_id`) → ordered messages. The stream sequence is the
    /// key, exactly as JetStream would have it.
    subjects: BTreeMap<String, BTreeMap<u64, ChainEvent>>,
}

impl JetStreamSketchEventStore {
    pub fn new() -> Self {
        Self::default()
    }
    fn next_seq(&self, subject: &str) -> u64 {
        self.subjects
            .get(subject)
            .and_then(|m| m.keys().next_back().copied())
            .unwrap_or(0)
            + 1
    }
}

impl EventStore for JetStreamSketchEventStore {
    fn backend_name(&self) -> &'static str {
        "jetstream-sketch"
    }

    /// A publish to the subject. ⚠ JetStream's own dedupe is by `Nats-Msg-Id`
    /// within a window; it has no concept of "must extend the chain head", so
    /// nothing here refuses a stale-head append. Expected-sequence publish
    /// (`expected_last_subject_sequence`) could approximate it — which is worth
    /// noting as the concrete thing a real implementation would have to add.
    fn append(
        &mut self,
        execution_id: &str,
        event_id: &str,
        prev_event_id: Option<&str>,
        parent_execution_id: Option<&str>,
        payload: &str,
    ) -> Result<ExecSeq, ChainError> {
        let seq = self.next_seq(execution_id);
        self.subjects
            .entry(execution_id.to_string())
            .or_default()
            .insert(
                seq,
                ChainEvent {
                    exec_seq: seq,
                    event_id: event_id.to_string(),
                    prev_event_id: prev_event_id.map(str::to_string),
                    execution_id: execution_id.to_string(),
                    parent_execution_id: parent_execution_id.map(str::to_string),
                    payload: payload.to_string(),
                },
            );
        Ok(seq)
    }

    /// ⚠ A replay-and-scan. No index exists to do better.
    fn get(&self, execution_id: &str, event_id: &str) -> Result<Option<ChainEvent>, ChainError> {
        Ok(self
            .subjects
            .get(execution_id)
            .and_then(|m| m.values().find(|e| e.event_id == event_id))
            .cloned())
    }

    /// ⚠ Returns `None` for an absent predecessor — the stream simply does not
    /// have it, and a replay cannot distinguish "not yet" from "not a thing".
    fn parent_of(&self, event: &ChainEvent) -> Result<Option<ChainEvent>, ChainError> {
        let Some(prev) = event.prev_event_id.as_deref() else {
            return Ok(None);
        };
        self.get(&event.execution_id, prev)
    }

    fn chain(&self, execution_id: &str) -> Result<Vec<ChainEvent>, ChainError> {
        Ok(self
            .subjects
            .get(execution_id)
            .map(|m| m.values().cloned().collect())
            .unwrap_or_default())
    }

    fn walk_from_head(&self, execution_id: &str) -> Result<Vec<ChainEvent>, ChainError> {
        let mut v = self.chain(execution_id)?;
        v.reverse();
        Ok(v)
    }

    /// ⚠ "Everything in the subject" always looks complete.
    fn chain_is_complete(&self, _execution_id: &str) -> Result<bool, ChainError> {
        Ok(true)
    }

    fn parent_execution_of(&self, execution_id: &str) -> Result<Option<String>, ChainError> {
        Ok(self
            .subjects
            .get(execution_id)
            .and_then(|m| m.values().next())
            .and_then(|e| e.parent_execution_id.clone()))
    }

    fn apply_replicated(&mut self, event: ChainEvent) -> Result<(), ChainError> {
        self.subjects
            .entry(event.execution_id.clone())
            .or_default()
            .insert(event.exec_seq, event);
        Ok(())
    }
}

/// **A deliberately broken backend**, mirroring ops#311's `vertex-stub`.
///
/// Exists so the conformance suite can be shown to reject something. It accepts
/// everything, remembers nothing, and reports every chain complete — the
/// maximally-wrong shape.
#[derive(Debug, Default)]
pub struct StubEventStore;

impl EventStore for StubEventStore {
    fn backend_name(&self) -> &'static str {
        "stub"
    }
    fn append(
        &mut self,
        _e: &str,
        _ev: &str,
        _p: Option<&str>,
        _pe: Option<&str>,
        _pl: &str,
    ) -> Result<ExecSeq, ChainError> {
        Ok(1)
    }
    fn get(&self, _e: &str, _ev: &str) -> Result<Option<ChainEvent>, ChainError> {
        Ok(None)
    }
    fn parent_of(&self, _event: &ChainEvent) -> Result<Option<ChainEvent>, ChainError> {
        Ok(None)
    }
    fn chain(&self, _e: &str) -> Result<Vec<ChainEvent>, ChainError> {
        Ok(Vec::new())
    }
    fn walk_from_head(&self, _e: &str) -> Result<Vec<ChainEvent>, ChainError> {
        Ok(Vec::new())
    }
    fn chain_is_complete(&self, _e: &str) -> Result<bool, ChainError> {
        Ok(true)
    }
    fn parent_execution_of(&self, _e: &str) -> Result<Option<String>, ChainError> {
        Ok(None)
    }
    fn apply_replicated(&mut self, _event: ChainEvent) -> Result<(), ChainError> {
        Ok(())
    }
}
