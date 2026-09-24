//! **Pluggable storage roles** — the runtime backend seam
//! (ai-meta `docs/rfc/ehdb-execution-partitioned-event-store.md` §12).
//!
//! EHDB is the **default implementation**, not a hard dependency. An operator
//! selects, per **storage role**, which backend a noetl internal workload uses.
//!
//! ## Precedent, mirrored deliberately
//!
//! `noetl/ops#311` did this for models:
//!
//! ```text
//! NOETL_SLM_BACKEND = ollama | vertex | vertex-stub | vllm     # default: ollama
//! ```
//!
//! with precedence **explicit call-site argument → flag → default**, and the
//! property that *"a call site that passes nothing and runs with no flag behaves
//! exactly as it does today."* This module is the storage analogue, role by role.
//! The `-stub` idiom is carried over too (see [`Backend::Stub`]) — a backend that
//! exists to be *rejected* by conformance is how you prove conformance
//! discriminates.
//!
//! ## ⚠ Where the seam is, and why that keeps it thin
//!
//! The hard requirement is that the O(1) predecessor fetch and the
//! per-execution append are **not slowed by indirection**. That is a
//! *granularity* decision, not an optimisation:
//!
//! > **The seam is at the workload boundary, not inside the chain walk.**
//!
//! [`EventStore`] is deliberately **coarse-grained**: one call per *logical
//! operation* (`walk_from_head` returns the whole chain), never one call per
//! event. So a 1,000-event walk crosses the seam **once**, and the inner loop
//! stays inside the implementation where it is monomorphised and borrow-based.
//! A fine-grained trait — `next_event()` per step — would put a virtual call and
//! an allocation in the inner loop, which is the shape this note exists to
//! forbid.
//!
//! ⚠ Trait methods return **owned** values because a remote backend has no
//! borrow to hand back. In-process callers that want the borrow keep using the
//! concrete [`ChainStore`](crate::chain::ChainStore) directly; the trait is the
//! *configuration* seam.

use crate::chain::{ChainError, ChainEvent, ChainStore, ExecSeq};

// ---------------------------------------------------------------------------
// Roles
// ---------------------------------------------------------------------------

/// A storage role noetl's internal processing needs. Each is selected
/// independently at runtime.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum StorageRole {
    /// Event sourcing. Contract = the 4-id chain model ([`EventStore`]).
    EventLog,
    /// The serving / projection tier.
    Projection,
    /// Internal noetl execution-context management.
    Context,
    Kv,
    Object,
    Vector,
}

impl StorageRole {
    /// Every role. Closed array so a new role fails to compile here rather than
    /// silently going unconfigurable — and so the metric that labels by role
    /// can pin every value at 0.
    pub const ALL: [StorageRole; 6] = [
        StorageRole::EventLog,
        StorageRole::Projection,
        StorageRole::Context,
        StorageRole::Kv,
        StorageRole::Object,
        StorageRole::Vector,
    ];

    pub fn label(&self) -> &'static str {
        match self {
            Self::EventLog => "eventlog",
            Self::Projection => "projection",
            Self::Context => "context",
            Self::Kv => "kv",
            Self::Object => "object",
            Self::Vector => "vector",
        }
    }

    /// The env var selecting this role's backend.
    pub fn env_var(&self) -> &'static str {
        match self {
            Self::EventLog => "NOETL_STORE_EVENTLOG",
            Self::Projection => "NOETL_STORE_PROJECTION",
            Self::Context => "NOETL_STORE_CONTEXT",
            Self::Kv => "NOETL_STORE_KV",
            Self::Object => "NOETL_STORE_OBJECT",
            Self::Vector => "NOETL_STORE_VECTOR",
        }
    }
}

/// A backend implementation choice.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Backend {
    /// **The default for every role.** Nothing changes unless configured.
    Ehdb,
    /// Sketched, not built — see [`crate::chain_jetstream`].
    JetStream,
    /// Named for completeness; rejected in the RFC (over-buys consistency).
    CockroachKv,
    Postgres,
    Redis,
    /// ⚠ A deliberately non-conforming backend, mirroring ops#311's
    /// `vertex-stub`. It exists so the conformance suite can be shown to
    /// **reject** something. A suite that passes everything proves nothing.
    Stub,
}

impl Backend {
    pub fn label(&self) -> &'static str {
        match self {
            Self::Ehdb => "ehdb",
            Self::JetStream => "jetstream",
            Self::CockroachKv => "cockroach-kv",
            Self::Postgres => "postgres",
            Self::Redis => "redis",
            Self::Stub => "stub",
        }
    }

    /// ⚠ Unrecognised ⇒ [`Backend::Ehdb`], the default. Matching ops#311's
    /// "no flag behaves exactly as today" and `EventLogMode::from_env`'s
    /// fail-safe: a typo must never move a workload onto a different store.
    pub fn parse(raw: Option<&str>) -> Self {
        match raw.map(|v| v.trim().to_ascii_lowercase()).as_deref() {
            Some("jetstream") => Self::JetStream,
            Some("cockroach-kv") | Some("cockroach_kv") => Self::CockroachKv,
            Some("postgres") => Self::Postgres,
            Some("redis") => Self::Redis,
            Some("stub") => Self::Stub,
            _ => Self::Ehdb,
        }
    }
}

/// Resolve a role's backend. Precedence, exactly ops#311's:
/// **explicit argument → env flag → `Ehdb`.**
pub fn resolve_backend(role: StorageRole, explicit: Option<Backend>) -> Backend {
    if let Some(b) = explicit {
        return b;
    }
    Backend::parse(std::env::var(role.env_var()).ok().as_deref())
}

/// The full resolved selection, for logging and for a `*_info` gauge.
///
/// ⭐ Reported for **every** role including the defaulted ones, so a scrape
/// says what is actually in use rather than only what was overridden. An
/// absent label reads identically to a broken exporter.
pub fn resolved_selection() -> Vec<(StorageRole, Backend)> {
    StorageRole::ALL
        .iter()
        .map(|r| (*r, resolve_backend(*r, None)))
        .collect()
}

// ---------------------------------------------------------------------------
// The EventStore contract — the 4-id chain model
// ---------------------------------------------------------------------------

/// **The conformance contract for any event-sourcing backend.**
///
/// This is the 4-id model as an interface. A backend is usable for
/// [`StorageRole::EventLog`] only if it passes
/// [`conformance::run`](crate::store_role::conformance::run).
///
/// Coarse-grained on purpose — see the module note on where the seam is.
pub trait EventStore: Send {
    fn backend_name(&self) -> &'static str;

    /// **(a)** Append to an `execution_id` partition. MUST enforce that the
    /// append extends the current head (invariant I1), so the chain is a path.
    fn append(
        &mut self,
        execution_id: &str,
        event_id: &str,
        prev_event_id: Option<&str>,
        parent_execution_id: Option<&str>,
        payload: &str,
    ) -> Result<ExecSeq, ChainError>;

    /// One event by its key.
    fn get(&self, execution_id: &str, event_id: &str) -> Result<Option<ChainEvent>, ChainError>;

    /// **(b)** The predecessor. MUST be one addressed lookup — no scan — and
    /// MUST return [`ChainError::GapAt`] naming the missing key rather than
    /// `None` when a non-root predecessor is absent.
    fn parent_of(&self, event: &ChainEvent) -> Result<Option<ChainEvent>, ChainError>;

    /// **(c)** The execution's chain, ascending, this partition only.
    fn chain(&self, execution_id: &str) -> Result<Vec<ChainEvent>, ChainError>;

    /// Head→root walk. One seam crossing for the whole walk.
    fn walk_from_head(&self, execution_id: &str) -> Result<Vec<ChainEvent>, ChainError>;

    /// **(d)** Complete, or incomplete. MUST be false when a hole exists and
    /// true for a contiguous prefix (behind is not broken).
    fn chain_is_complete(&self, execution_id: &str) -> Result<bool, ChainError>;

    /// Execution-tree edge, one hop.
    fn parent_execution_of(&self, execution_id: &str) -> Result<Option<String>, ChainError>;

    /// Follower ingest. MUST accept out-of-order delivery (eventual
    /// consistency) and MUST be idempotent on identical redelivery.
    fn apply_replicated(&mut self, event: ChainEvent) -> Result<(), ChainError>;
}

/// EHDB's implementation — the default.
impl EventStore for ChainStore {
    fn backend_name(&self) -> &'static str {
        "ehdb"
    }
    fn append(
        &mut self,
        execution_id: &str,
        event_id: &str,
        prev_event_id: Option<&str>,
        parent_execution_id: Option<&str>,
        payload: &str,
    ) -> Result<ExecSeq, ChainError> {
        ChainStore::append(
            self,
            execution_id,
            event_id,
            prev_event_id,
            parent_execution_id,
            payload,
        )
    }
    fn get(&self, execution_id: &str, event_id: &str) -> Result<Option<ChainEvent>, ChainError> {
        Ok(ChainStore::get(self, execution_id, event_id).cloned())
    }
    fn parent_of(&self, event: &ChainEvent) -> Result<Option<ChainEvent>, ChainError> {
        ChainStore::parent_of(self, event).map(|o| o.cloned())
    }
    fn chain(&self, execution_id: &str) -> Result<Vec<ChainEvent>, ChainError> {
        Ok(ChainStore::chain(self, execution_id)
            .into_iter()
            .cloned()
            .collect())
    }
    fn walk_from_head(&self, execution_id: &str) -> Result<Vec<ChainEvent>, ChainError> {
        ChainStore::walk_from_head(self, execution_id).map(|v| v.into_iter().cloned().collect())
    }
    fn chain_is_complete(&self, execution_id: &str) -> Result<bool, ChainError> {
        ChainStore::chain_is_complete(self, execution_id)
    }
    fn parent_execution_of(&self, execution_id: &str) -> Result<Option<String>, ChainError> {
        Ok(ChainStore::parent_execution_of(self, execution_id).map(str::to_string))
    }
    fn apply_replicated(&mut self, event: ChainEvent) -> Result<(), ChainError> {
        ChainStore::apply_replicated(self, event)
    }
}

// ---------------------------------------------------------------------------
// Conformance
// ---------------------------------------------------------------------------

pub mod conformance {
    //! **The EventStore conformance suite.** A backend is usable for
    //! [`StorageRole::EventLog`](super::StorageRole::EventLog) only if
    //! [`run`] returns no failures.
    //!
    //! ⭐ The suite is only meaningful if it **rejects** something. `Stub`
    //! exists for that, and `stub_is_rejected` in the tests is the suite's own
    //! positive control.

    use super::{ChainError, ChainEvent, EventStore};

    /// One contract clause that failed, named so an operator can see *which*
    /// guarantee a candidate backend does not provide.
    #[derive(Debug, Clone, PartialEq, Eq)]
    pub struct Violation {
        pub clause: &'static str,
        pub detail: String,
    }

    fn ev(exec: &str, seq: u64, id: &str, prev: Option<&str>) -> ChainEvent {
        ChainEvent {
            exec_seq: seq,
            event_id: id.to_string(),
            prev_event_id: prev.map(str::to_string),
            execution_id: exec.to_string(),
            parent_execution_id: None,
            payload: "{}".to_string(),
        }
    }

    /// Run the contract against `store`. Returns every violation, not the
    /// first — a backend failing three clauses should report three.
    pub fn run(store: &mut dyn EventStore) -> Vec<Violation> {
        let mut v = Vec::new();
        let e = "conformance-exec";

        // (a) append assigns a per-execution sequence starting at 1.
        match store.append(e, "a", None, None, "{}") {
            Ok(1) => {}
            Ok(other) => v.push(Violation {
                clause: "append/per-execution-seq-starts-at-1",
                detail: format!("first append returned {other}, expected 1"),
            }),
            Err(err) => v.push(Violation {
                clause: "append/accepts-root",
                detail: err.message(),
            }),
        }
        let _ = store.append(e, "b", Some("a"), None, "{}");

        // (a) I1 — an append that does not extend the head is refused.
        if store.append(e, "c", Some("a"), None, "{}").is_ok() {
            v.push(Violation {
                clause: "append/enforces-head (I1)",
                detail: "an append on a stale head was ACCEPTED — the chain can fork".into(),
            });
        }

        // (c) the chain is this execution's, ascending.
        match store.chain(e) {
            Ok(c) => {
                if c.iter().any(|x| x.execution_id != e) {
                    v.push(Violation {
                        clause: "chain/partition-isolation",
                        detail: "chain() returned another execution's events".into(),
                    });
                }
                if c.windows(2).any(|w| w[0].exec_seq >= w[1].exec_seq) {
                    v.push(Violation {
                        clause: "chain/ascending",
                        detail: "chain() is not ascending by exec_seq".into(),
                    });
                }
            }
            Err(err) => v.push(Violation {
                clause: "chain/readable",
                detail: err.message(),
            }),
        }

        // (b) parent_of resolves, and stops at the root.
        match store.get(e, "b") {
            Ok(Some(b)) => match store.parent_of(&b) {
                Ok(Some(p)) if p.event_id == "a" => {}
                Ok(other) => v.push(Violation {
                    clause: "parent_of/resolves-predecessor",
                    detail: format!("expected 'a', got {:?}", other.map(|x| x.event_id)),
                }),
                Err(err) => v.push(Violation {
                    clause: "parent_of/resolves-predecessor",
                    detail: err.message(),
                }),
            },
            other => v.push(Violation {
                clause: "get/by-key",
                detail: format!("get(b) returned {other:?}"),
            }),
        }

        // (b) ⭐ a missing predecessor is a NAMED gap, never None.
        let orphan = ev(e, 999, "orphan", Some("never-written"));
        match store.parent_of(&orphan) {
            Err(ChainError::GapAt { event_id, .. }) if event_id == "never-written" => {}
            Ok(None) => v.push(Violation {
                clause: "parent_of/names-the-gap",
                detail: "a missing predecessor returned None (indistinguishable from a chain \
                         root) instead of GapAt{event_id} — this is what makes a timed-out \
                         read look like a finished execution"
                    .into(),
            }),
            other => v.push(Violation {
                clause: "parent_of/names-the-gap",
                detail: format!("expected GapAt, got {other:?}"),
            }),
        }

        // Follower ingest: out-of-order accepted, and the hole is visible.
        let f = "conformance-follower";
        if let Err(err) = store.apply_replicated(ev(f, 1, "f1", None)) {
            v.push(Violation {
                clause: "apply_replicated/accepts",
                detail: err.message(),
            });
        }
        if let Err(err) = store.apply_replicated(ev(f, 3, "f3", Some("f2"))) {
            v.push(Violation {
                clause: "apply_replicated/accepts-out-of-order",
                detail: format!(
                    "a follower refused out-of-order delivery: {}",
                    err.message()
                ),
            });
        }
        // Idempotent redelivery.
        if store.apply_replicated(ev(f, 1, "f1", None)).is_err() {
            v.push(Violation {
                clause: "apply_replicated/idempotent",
                detail: "identical redelivery was rejected".into(),
            });
        }
        // (d) a hole reads incomplete.
        match store.chain_is_complete(f) {
            Ok(true) => v.push(Violation {
                clause: "chain_is_complete/hole-is-incomplete",
                detail: "a partition missing a middle event reported COMPLETE — the \
                         false-complete this contract exists to prevent"
                    .into(),
            }),
            Ok(false) => {}
            Err(err) => v.push(Violation {
                clause: "chain_is_complete/readable",
                detail: err.message(),
            }),
        }
        // (d) control: a contiguous prefix reads complete (behind is not broken).
        let g = "conformance-prefix";
        let _ = store.apply_replicated(ev(g, 1, "g1", None));
        let _ = store.apply_replicated(ev(g, 2, "g2", Some("g1")));
        if store.chain_is_complete(g) == Ok(false) {
            v.push(Violation {
                clause: "chain_is_complete/prefix-is-complete",
                detail: "a contiguous prefix reported INCOMPLETE — being behind is not \
                         being broken, and treating it as broken makes every replica unusable"
                    .into(),
            });
        }

        v
    }

    /// Whether `store` may serve the EventLog role.
    pub fn passes(store: &mut dyn EventStore) -> bool {
        run(store).is_empty()
    }
}
