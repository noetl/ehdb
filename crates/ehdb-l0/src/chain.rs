//! **The execution-partitioned event chain** — MVP slice of
//! `docs/rfc/ehdb-execution-partitioned-event-store.md` (ai-meta).
//!
//! ## What this is for
//!
//! Today "give me this execution's events" is a **linear filter over every
//! event ever written**: `ehdb-stream` keys records by a global
//! `StreamSequence` and `replay_records` filters the whole `BTreeMap` by
//! subject. So the cost of reading one execution is proportional to everything
//! *other* executions wrote, chain reads time out, a timed-out read is
//! indistinguishable from a short chain, the drive re-drives, and the re-drive
//! adds load to the same store. This module is the storage shape that removes
//! the first link.
//!
//! ## The model (RFC §2)
//!
//! Four ids, and the fourth is what makes this work:
//!
//! ```text
//! event_id, prev_event_id, execution_id, parent_execution_id
//! ```
//!
//! Because a reader is **told** `(execution_id, prev_event_id)`, finding the
//! predecessor is an **address**, not a search. That is the whole trick:
//!
//! | pattern | key | cost |
//! | :-- | :-- | :-- |
//! | **A** predecessor of an event | `(execution_id, prev_event_id)` | **one** index probe |
//! | **B** append | `(execution_id, exec_seq+1)` | partition tail |
//! | **B2** ordered chain read | range `(execution_id, ·)` | `O(k)`, contiguous |
//!
//! ## ⚠ `prev_event_id`, NOT `parent_event_id`
//!
//! The RFC's fork F1, and it is load-bearing. In this codebase
//! `parent_event_id` is a *causal trigger* pointer that is known to **dangle**
//! under the off-server drive (RFC #115 Phase 2 records this explicitly); the
//! walkable chain pointer is `prev_event_id`. Building chain-following on
//! `parent_event_id` would walk into a dangling pointer on exactly the path
//! that is currently failing. This module therefore names its edge
//! `prev_event_id` and treats causal provenance as an unrelated attribute.
//!
//! ## Status: INERT
//!
//! Nothing calls this. It is not wired into the tier, the engine, the drive or
//! any write path. It is the primitive, with its proofs, so the wiring can be a
//! separate reviewable step.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use ehdb_core::EhdbError;

/// Per-execution sequence. Starts at 1; `0` is reserved for "no events".
///
/// ⚠ Distinct from `global_sequence`. `global_sequence` is the append order of
/// the whole store and is assigned per engine; `exec_seq` is the order *within
/// one execution*. Both exist and neither substitutes for the other — see the
/// RFC on why `global_sequence` must not be read as a global order.
pub type ExecSeq = u64;

/// One event as the chain stores it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChainEvent {
    /// Position within this execution. Assigned by the partition's writer.
    pub exec_seq: ExecSeq,
    pub event_id: String,
    /// The chain edge (I1). `None` only at the chain root.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prev_event_id: Option<String>,
    pub execution_id: String,
    /// The execution-tree edge (I3). `None` only at a tree root.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parent_execution_id: Option<String>,
    pub payload: String,
}

/// Why a chain operation failed.
///
/// ⭐ `GapAt` is the point of the whole design. A chain read that cannot
/// resolve a link reports **which key it is waiting for**, never an empty
/// result. An empty result is what makes a timed-out read look like a finished
/// execution — the condition the re-drive loop exists to paper over, and the
/// reason 53 executions could re-drive forever with nothing saying why.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ChainError {
    /// A link is missing. Under eventual replication this is the normal
    /// "not here yet" and is safe to retry or serve short; under a truncated
    /// log it is real loss. Either way it is **named**.
    GapAt {
        execution_id: String,
        event_id: String,
    },
    /// Two events claim the same predecessor — the fork RFC §2.5 Property N
    /// says a single writer makes impossible. If this ever fires, exclusion
    /// was not real.
    Forked {
        execution_id: String,
        prev_event_id: String,
        first: String,
        second: String,
    },
    /// An append whose `prev` is not the current head.
    NotHead {
        execution_id: String,
        expected: Option<String>,
        got: Option<String>,
    },
    Invalid(String),
}

impl ChainError {
    pub fn message(&self) -> String {
        match self {
            Self::GapAt {
                execution_id,
                event_id,
            } => format!(
                "chain gap in execution {execution_id}: event {event_id} is not present \
                 (named, not empty: a reader may wait, serve a shorter prefix, or report it)"
            ),
            Self::Forked {
                execution_id,
                prev_event_id,
                first,
                second,
            } => format!(
                "chain FORK in execution {execution_id}: events {first} and {second} both \
                 claim predecessor {prev_event_id} — single-writer exclusion was not real"
            ),
            Self::NotHead {
                execution_id,
                expected,
                got,
            } => format!(
                "append to execution {execution_id} out of order: head is {expected:?}, \
                 append claims predecessor {got:?}"
            ),
            Self::Invalid(m) => m.clone(),
        }
    }
}

impl From<ChainError> for EhdbError {
    fn from(e: ChainError) -> Self {
        EhdbError::InvalidState(e.message())
    }
}

/// One execution's partition: the ordered chain plus the point index.
///
/// Two maps, and the second is what makes pattern A one probe rather than a
/// walk:
///
/// * `by_seq: exec_seq -> ChainEvent` — ordered, so B2 is a contiguous range.
/// * `by_event: event_id -> exec_seq` — so `(execution_id, event_id)` resolves
///   without touching any other event.
#[derive(Debug, Default, Clone)]
pub struct ExecutionPartition {
    by_seq: BTreeMap<ExecSeq, ChainEvent>,
    by_event: BTreeMap<String, ExecSeq>,
    head: Option<String>,
}

impl ExecutionPartition {
    pub fn len(&self) -> usize {
        self.by_seq.len()
    }
    pub fn is_empty(&self) -> bool {
        self.by_seq.is_empty()
    }
    /// The newest event's id, or `None` for an empty partition.
    pub fn head(&self) -> Option<&str> {
        self.head.as_deref()
    }
    pub fn next_seq(&self) -> ExecSeq {
        self.by_seq.keys().next_back().copied().unwrap_or(0) + 1
    }
}

/// The execution-partitioned chain store.
///
/// Partitioned by `execution_id`, so one execution's events are grouped and a
/// read never consults another execution's data. That is the property the
/// current store lacks and the reason it cannot answer B2 in bounded time.
#[derive(Debug, Default)]
pub struct ChainStore {
    partitions: BTreeMap<String, ExecutionPartition>,
}

impl ChainStore {
    pub fn new() -> Self {
        Self::default()
    }

    /// **Pattern B — append the next event to an execution.**
    ///
    /// Enforces I1 by refusing an append whose `prev` is not the current head.
    /// That refusal is what makes the chain a path rather than a tree: a
    /// second writer appending to a stale head is rejected here, so
    /// [`ChainError::Forked`] should be unreachable — it exists to *detect*
    /// exclusion having failed elsewhere, not to be relied on.
    pub fn append(
        &mut self,
        execution_id: &str,
        event_id: &str,
        prev_event_id: Option<&str>,
        parent_execution_id: Option<&str>,
        payload: &str,
    ) -> std::result::Result<ExecSeq, ChainError> {
        if execution_id.trim().is_empty() {
            return Err(ChainError::Invalid("execution_id is empty".into()));
        }
        if event_id.trim().is_empty() {
            return Err(ChainError::Invalid("event_id is empty".into()));
        }
        let part = self.partitions.entry(execution_id.to_string()).or_default();

        // I1: the chain is a path. An append must extend the head.
        if part.head.as_deref() != prev_event_id {
            return Err(ChainError::NotHead {
                execution_id: execution_id.to_string(),
                expected: part.head.clone(),
                got: prev_event_id.map(str::to_string),
            });
        }
        if part.by_event.contains_key(event_id) {
            return Err(ChainError::Invalid(format!(
                "event {event_id} already present in execution {execution_id}"
            )));
        }

        let seq = part.next_seq();
        part.by_seq.insert(
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
        part.by_event.insert(event_id.to_string(), seq);
        part.head = Some(event_id.to_string());
        Ok(seq)
    }

    /// **The follower ingest path** — apply an event received by replication.
    ///
    /// ⚠ Deliberately does **not** enforce the head, and that asymmetry is the
    /// design, not laxity:
    ///
    /// * [`append`](Self::append) is the **writer** path. It enforces I1, so the
    ///   home partition's chain is a path and cannot fork.
    /// * `apply_replicated` is the **replica** path. Async replication may
    ///   deliver out of order (RFC §2.5 Property P is conditional on ordered
    ///   delivery and is *not* relied on for safety), so a follower must be able
    ///   to hold `e₃` while still waiting for `e₂`.
    ///
    /// Safety is unaffected: RFC §2.5 Property N holds from single-writer plus
    /// immutability, so what arrives here can only ever be a **subset of one
    /// fixed path**. A hole is a *gap*, never a *fork* — and
    /// [`chain_is_complete`](Self::chain_is_complete) reports it as such.
    ///
    /// ⚠ Re-applying an identical event is idempotent (replication retries);
    /// a *conflicting* event at the same key is [`ChainError::Forked`], which
    /// under Property N should be unreachable and exists to detect exclusion
    /// having failed upstream.
    pub fn apply_replicated(&mut self, event: ChainEvent) -> std::result::Result<(), ChainError> {
        if event.execution_id.trim().is_empty() || event.event_id.trim().is_empty() {
            return Err(ChainError::Invalid(
                "replicated event needs execution_id and event_id".into(),
            ));
        }
        let part = self
            .partitions
            .entry(event.execution_id.clone())
            .or_default();

        if let Some(existing_seq) = part.by_event.get(&event.event_id) {
            let existing = part.by_seq.get(existing_seq).expect("index/seq agree");
            if existing == &event {
                return Ok(()); // idempotent redelivery
            }
            return Err(ChainError::Forked {
                execution_id: event.execution_id.clone(),
                prev_event_id: event
                    .prev_event_id
                    .clone()
                    .unwrap_or_else(|| "<root>".into()),
                first: existing.event_id.clone(),
                second: event.event_id.clone(),
            });
        }

        let seq = event.exec_seq;
        let is_newest = part.by_seq.keys().next_back().is_none_or(|top| seq > *top);
        part.by_event.insert(event.event_id.clone(), seq);
        if is_newest {
            part.head = Some(event.event_id.clone());
        }
        part.by_seq.insert(seq, event);
        Ok(())
    }

    /// **Pattern A — one event, by its key.** One index probe plus one fetch,
    /// independent of how many events exist anywhere.
    pub fn get(&self, execution_id: &str, event_id: &str) -> Option<&ChainEvent> {
        let part = self.partitions.get(execution_id)?;
        let seq = part.by_event.get(event_id)?;
        part.by_seq.get(seq)
    }

    /// **Pattern A — the predecessor of an event.**
    ///
    /// ⭐ The operation the whole RFC is about. Because the caller holds
    /// `(execution_id, prev_event_id)` already, this is an address, not a
    /// search — one probe, no scan, no dependence on store size.
    ///
    /// `Ok(None)` means "this is the chain root". A missing predecessor that
    /// *should* exist is [`ChainError::GapAt`] — the distinction between
    /// "finished" and "not here yet" that the current store cannot make.
    pub fn parent_of(
        &self,
        event: &ChainEvent,
    ) -> std::result::Result<Option<&ChainEvent>, ChainError> {
        let Some(prev) = event.prev_event_id.as_deref() else {
            return Ok(None); // chain root
        };
        match self.get(&event.execution_id, prev) {
            Some(e) => Ok(Some(e)),
            None => Err(ChainError::GapAt {
                execution_id: event.execution_id.clone(),
                event_id: prev.to_string(),
            }),
        }
    }

    /// **Pattern B2 — the execution's chain in order.** Contiguous over this
    /// partition only; no other execution's records are touched.
    pub fn chain(&self, execution_id: &str) -> Vec<&ChainEvent> {
        match self.partitions.get(execution_id) {
            Some(p) => p.by_seq.values().collect(),
            None => Vec::new(),
        }
    }

    /// Walk head→root by following `prev_event_id`, newest first.
    ///
    /// This is chain-**following**, the replacement for chain-*reconstruction*.
    /// A gap is reported with the key it is waiting for rather than returning a
    /// short list that a caller would read as a complete one.
    pub fn walk_from_head(
        &self,
        execution_id: &str,
    ) -> std::result::Result<Vec<&ChainEvent>, ChainError> {
        let Some(part) = self.partitions.get(execution_id) else {
            return Ok(Vec::new());
        };
        let Some(head) = part.head.as_deref() else {
            return Ok(Vec::new());
        };
        let mut out = Vec::new();
        let mut cursor = self
            .get(execution_id, head)
            .ok_or_else(|| ChainError::GapAt {
                execution_id: execution_id.to_string(),
                event_id: head.to_string(),
            })?;
        loop {
            out.push(cursor);
            match self.parent_of(cursor)? {
                Some(p) => cursor = p,
                None => break,
            }
        }
        Ok(out)
    }

    /// Whether the partition's chain is complete from head to root.
    ///
    /// ⭐ This is the predicate the drive needs, and it is **three-valued in
    /// effect**: complete, or incomplete *at a named key*. The current store
    /// can only offer "I returned some events", which is why a timeout and a
    /// finished execution look identical to it.
    pub fn chain_is_complete(&self, execution_id: &str) -> std::result::Result<bool, ChainError> {
        // ⚠ Compared against the partition's SPAN (highest exec_seq), not against
        // how many records happen to be present. A count comparison is
        // satisfied by "I hold 2 of 3 and walked 2", which is exactly the
        // false-complete the design exists to prevent.
        let Some(part) = self.partitions.get(execution_id) else {
            return Ok(true); // nothing to be incomplete about
        };
        let span = part.by_seq.keys().next_back().copied().unwrap_or(0) as usize;
        match self.walk_from_head(execution_id) {
            Ok(walked) => Ok(walked.len() == span),
            Err(ChainError::GapAt { .. }) => Ok(false),
            Err(e) => Err(e),
        }
    }

    /// **Pattern C — the execution tree edge.** One hop up, O(1) per hop.
    pub fn parent_execution_of(&self, execution_id: &str) -> Option<&str> {
        self.partitions
            .get(execution_id)?
            .by_seq
            .values()
            .next()?
            .parent_execution_id
            .as_deref()
    }

    pub fn partition(&self, execution_id: &str) -> Option<&ExecutionPartition> {
        self.partitions.get(execution_id)
    }
    pub fn execution_count(&self) -> usize {
        self.partitions.len()
    }
    /// Total events across all partitions — the `N` that pattern A and B2 must
    /// be independent of.
    pub fn total_events(&self) -> usize {
        self.partitions.values().map(|p| p.len()).sum()
    }
}

/// Flag gating the chain store. Default **off**; nothing reads it yet.
pub const CHAIN_STORE_ENV: &str = "NOETL_EHDB_EXEC_CHAIN";

/// Whether chain-following is enabled. ⚠ Fail-safe: anything unrecognised is
/// `false`, matching `EventLogMode::from_env` ("an unknown driver never
/// mirrors"). A typo must not move the read path.
pub fn chain_store_enabled() -> bool {
    matches!(
        std::env::var(CHAIN_STORE_ENV)
            .unwrap_or_default()
            .trim()
            .to_ascii_lowercase()
            .as_str(),
        "1" | "true" | "yes" | "on"
    )
}
