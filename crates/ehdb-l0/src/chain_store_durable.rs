//! **The chain store on a durable substrate** — the RFC's P1 obligation.
//!
//! Increments 1–3 proved the *algorithmic* shape in memory. That is necessary
//! and not sufficient: an in-memory `BTreeMap` probe is O(1) for reasons that
//! say nothing about what the store does to disk. **P1 is a claim about I/O**,
//! and the RFC requires it be verified "by a counting substrate, not by reading
//! the code".
//!
//! ## The key layout, chosen to make P1 true rather than to make it provable
//!
//! ```text
//! chain/<execution_id>/ev/<event_id>          -> the serialised ChainEvent
//! chain/<execution_id>/seq/<020-pad exec_seq> -> that event's id (a pointer)
//! ```
//!
//! * **Pattern A (predecessor)** — the caller already holds
//!   `(execution_id, prev_event_id)` (invariant I4), so the key is *computed*,
//!   not searched. **Exactly one `get_all`.** No list, no scan, no index walk.
//! * **Pattern B2 (ordered chain)** — one `list_prefix` on the `seq/` space,
//!   then one `get_all` per event: `k + 1` operations for a `k`-event
//!   execution, **independent of how many other executions exist**. Zero-padding
//!   makes the lexical order the numeric order, so the listing is already
//!   sorted.
//!
//! The `seq/` pointer records are deliberately tiny (an event id), so the
//! ordering index costs bytes rather than a second copy of the payload.
//!
//! ⚠ This is the **storage** layer. It holds no in-memory index, on purpose:
//! an index would make the counters measure the cache rather than the layout,
//! and P1 would pass for the wrong reason.

use std::sync::Arc;

use ehdb_core::{EhdbError, Result};

use crate::chain::{ChainError, ChainEvent, ExecSeq};
use crate::substrate::DurableSubstrate;

fn ev_key(execution_id: &str, event_id: &str) -> String {
    format!("chain/{execution_id}/ev/{event_id}")
}

fn seq_key(execution_id: &str, seq: ExecSeq) -> String {
    // Zero-padded so lexical order == numeric order in `list_prefix`.
    format!("chain/{execution_id}/seq/{seq:020}")
}

fn seq_prefix(execution_id: &str) -> String {
    format!("chain/{execution_id}/seq/")
}

fn head_key(execution_id: &str) -> String {
    format!("chain/{execution_id}/head")
}

/// A chain store backed by a [`DurableSubstrate`].
pub struct DurableChainStore {
    substrate: Arc<dyn DurableSubstrate>,
}

impl DurableChainStore {
    pub fn new(substrate: Arc<dyn DurableSubstrate>) -> Self {
        Self { substrate }
    }

    /// ⭐ **Pattern A.** Exactly one substrate read.
    ///
    /// The key is computed from ids the caller already carries, so there is
    /// nothing to search. This is the method the P1 proof counts.
    pub fn get(&self, execution_id: &str, event_id: &str) -> Result<Option<ChainEvent>> {
        match self.substrate.get_all(&ev_key(execution_id, event_id)) {
            Ok(bytes) => {
                let ev: ChainEvent = serde_json::from_slice(&bytes)
                    .map_err(|e| EhdbError::Storage(format!("decode chain event: {e}")))?;
                Ok(Some(ev))
            }
            // An absent key is a miss, not an error: the substrate cannot
            // distinguish "never written" from "not replicated yet", and that
            // distinction is the caller's (see `parent_of`).
            Err(_) => Ok(None),
        }
    }

    /// ⭐ **Pattern A, the predecessor.** One `get` ⇒ one substrate op.
    ///
    /// A missing non-root predecessor is [`ChainError::GapAt`], naming the key
    /// — never `None`, which would be indistinguishable from a chain root.
    pub fn parent_of(
        &self,
        event: &ChainEvent,
    ) -> std::result::Result<Option<ChainEvent>, ChainError> {
        let Some(prev) = event.prev_event_id.as_deref() else {
            return Ok(None);
        };
        match self.get(&event.execution_id, prev) {
            Ok(Some(e)) => Ok(Some(e)),
            Ok(None) => Err(ChainError::GapAt {
                execution_id: event.execution_id.clone(),
                event_id: prev.to_string(),
            }),
            Err(e) => Err(ChainError::Invalid(e.to_string())),
        }
    }

    /// The partition's head as `(exec_seq, event_id)`, if any. **One read.**
    ///
    /// ⚠ The head record carries the sequence as well as the id, deliberately.
    /// A first draft derived the next sequence with `list_prefix` on every
    /// append, which made append **O(k)** in the execution's own length and
    /// building a k-event chain **O(k²)** — measured at 308 s for the P1 suite.
    /// Append is the hot write path; it must be O(1) reads, and the only way to
    /// get that is for the head to already know where it is.
    pub fn head_entry(&self, execution_id: &str) -> Result<Option<(ExecSeq, String)>> {
        match self.substrate.get_all(&head_key(execution_id)) {
            Ok(b) => {
                let raw = String::from_utf8_lossy(&b).to_string();
                let (seq, id) = raw
                    .split_once(':')
                    .ok_or_else(|| EhdbError::Storage(format!("malformed head record: {raw:?}")))?;
                let seq: ExecSeq = seq
                    .parse()
                    .map_err(|e| EhdbError::Storage(format!("head seq: {e}")))?;
                Ok(Some((seq, id.to_string())))
            }
            Err(_) => Ok(None),
        }
    }

    /// The head event id. One read.
    pub fn head(&self, execution_id: &str) -> Result<Option<String>> {
        Ok(self.head_entry(execution_id)?.map(|(_, id)| id))
    }

    /// **Pattern B — append.** Enforces I1 against the persisted head.
    pub fn append(
        &self,
        execution_id: &str,
        event_id: &str,
        prev_event_id: Option<&str>,
        parent_execution_id: Option<&str>,
        payload: &str,
    ) -> std::result::Result<ExecSeq, ChainError> {
        if execution_id.trim().is_empty() || event_id.trim().is_empty() {
            return Err(ChainError::Invalid(
                "execution_id and event_id are required".into(),
            ));
        }
        // ⭐ ONE read serves both the I1 check and the next sequence.
        let head = self
            .head_entry(execution_id)
            .map_err(|e| ChainError::Invalid(e.to_string()))?;
        if head.as_ref().map(|(_, id)| id.as_str()) != prev_event_id {
            return Err(ChainError::NotHead {
                execution_id: execution_id.to_string(),
                expected: head.map(|(_, id)| id),
                got: prev_event_id.map(str::to_string),
            });
        }
        let seq = head.map(|(s, _)| s).unwrap_or(0) + 1;
        let event = ChainEvent {
            exec_seq: seq,
            event_id: event_id.to_string(),
            prev_event_id: prev_event_id.map(str::to_string),
            execution_id: execution_id.to_string(),
            parent_execution_id: parent_execution_id.map(str::to_string),
            payload: payload.to_string(),
        };
        let bytes = serde_json::to_vec(&event)
            .map_err(|e| ChainError::Invalid(format!("encode chain event: {e}")))?;

        // Events are immutable: `put_if_absent` reports whether it actually
        // wrote, and that result is load-bearing.
        //
        // ⚠ A first draft DISCARDED it. `put_if_absent` returns `Ok(false)` for
        // an existing key, so re-appending an `event_id` that already existed
        // reported success while writing nothing — and then advanced the head to
        // it. The in-memory store catches duplicates with an explicit index
        // check; this one relied on a return value it threw away. Found by a
        // mutation battery: swapping in `put_overwrite` changed no test, which
        // is what exposed that nothing was reading the result.
        let newly_written = self
            .substrate
            .put_if_absent(&ev_key(execution_id, event_id), &bytes)
            .map_err(|e| ChainError::Invalid(e.to_string()))?;
        if !newly_written {
            return Err(ChainError::Invalid(format!(
                "event {event_id} already exists in execution {execution_id}: an \
                 event key is write-once"
            )));
        }
        self.substrate
            .put_if_absent(&seq_key(execution_id, seq), event_id.as_bytes())
            .map_err(|e| ChainError::Invalid(e.to_string()))?;
        // The head is a pointer and legitimately advances.
        self.substrate
            .put_overwrite(
                &head_key(execution_id),
                format!("{seq}:{event_id}").as_bytes(),
            )
            .map_err(|e| ChainError::Invalid(e.to_string()))?;
        Ok(seq)
    }

    /// **Pattern B2 — the ordered chain.** `1 + k` substrate reads for a
    /// `k`-event execution, independent of total store size.
    pub fn chain(&self, execution_id: &str) -> Result<Vec<ChainEvent>> {
        let mut keys = self.substrate.list_prefix(&seq_prefix(execution_id))?;
        keys.sort(); // zero-padded ⇒ lexical order is numeric order
        let mut out = Vec::with_capacity(keys.len());
        for k in keys {
            let id_bytes = self.substrate.get_all(&k)?;
            let event_id = String::from_utf8_lossy(&id_bytes).to_string();
            if let Some(ev) = self.get(execution_id, &event_id)? {
                out.push(ev);
            }
        }
        out.sort_by_key(|e| e.exec_seq);
        Ok(out)
    }

    /// Head→root walk by following `prev_event_id`. One read per hop.
    pub fn walk_from_head(
        &self,
        execution_id: &str,
    ) -> std::result::Result<Vec<ChainEvent>, ChainError> {
        let head = match self.head(execution_id) {
            Ok(Some(h)) => h,
            Ok(None) => return Ok(Vec::new()),
            Err(e) => return Err(ChainError::Invalid(e.to_string())),
        };
        let mut cursor = match self.get(execution_id, &head) {
            Ok(Some(e)) => e,
            Ok(None) => {
                return Err(ChainError::GapAt {
                    execution_id: execution_id.to_string(),
                    event_id: head,
                })
            }
            Err(e) => return Err(ChainError::Invalid(e.to_string())),
        };
        let mut out = Vec::new();
        loop {
            out.push(cursor.clone());
            match self.parent_of(&cursor)? {
                Some(p) => cursor = p,
                None => break,
            }
        }
        Ok(out)
    }

    /// Complete, or incomplete at a named key.
    pub fn chain_is_complete(&self, execution_id: &str) -> std::result::Result<bool, ChainError> {
        let span = self
            .chain(execution_id)
            .map_err(|e| ChainError::Invalid(e.to_string()))?
            .last()
            .map(|e| e.exec_seq as usize)
            .unwrap_or(0);
        match self.walk_from_head(execution_id) {
            Ok(w) => Ok(w.len() == span),
            Err(ChainError::GapAt { .. }) => Ok(false),
            Err(e) => Err(e),
        }
    }

    /// Follower ingest — accepts out-of-order delivery.
    pub fn apply_replicated(&self, event: ChainEvent) -> std::result::Result<(), ChainError> {
        let bytes =
            serde_json::to_vec(&event).map_err(|e| ChainError::Invalid(format!("encode: {e}")))?;
        self.substrate
            .put_if_absent(&ev_key(&event.execution_id, &event.event_id), &bytes)
            .map_err(|e| ChainError::Invalid(e.to_string()))?;
        self.substrate
            .put_if_absent(
                &seq_key(&event.execution_id, event.exec_seq),
                event.event_id.as_bytes(),
            )
            .map_err(|e| ChainError::Invalid(e.to_string()))?;
        // Advance the head only if this is the newest we hold — one read, not
        // a chain scan.
        let current = self
            .head_entry(&event.execution_id)
            .map_err(|e| ChainError::Invalid(e.to_string()))?;
        if current.as_ref().map(|(s, _)| *s).unwrap_or(0) < event.exec_seq {
            self.substrate
                .put_overwrite(
                    &head_key(&event.execution_id),
                    format!("{}:{}", event.exec_seq, event.event_id).as_bytes(),
                )
                .map_err(|e| ChainError::Invalid(e.to_string()))?;
        }
        Ok(())
    }
}
