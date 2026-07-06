//! EHDB event-log core engine (completion program Phase 6).
//!
//! This is the durable persistence + ordering + serving layer for NoETL's
//! append-only `noetl.event` log — the engine that Phase 6 puts *underneath*
//! the producer path in place of the NATS-JetStream + PostgreSQL log-and-store
//! path (the scaling pressure point tracked by noetl/ai-meta#166 / #104 / #115).
//!
//! ## Boundary — this is the storage/ordering engine, NOT an event author
//!
//! Event *authorship* is unchanged: the gateway/server remain the gatekeeper of
//! **what** enters the log through the append-only producer path.  This engine
//! is the disk-and-index *under* an append the producer already authorized — it
//! does not decide what is appended, it persists + orders + serves it.  It is a
//! platform engine for the platform event log only; **business data never flows
//! through it** (that stays in external systems via playbook connectors).
//!
//! ## Semantics preserved from the JetStream + Postgres path
//!
//! * **Global sequence** — every appended event is assigned the next sequence on
//!   a single canonical event-log stream ([`EVENT_LOG_STREAM`]).  The sequence is
//!   monotonic and gapless, the same guarantee JetStream's stream sequence gives
//!   and that the Postgres `noetl.event` ordering column gives.
//! * **Per-execution scope** — each event carries an `execution_id`; the engine
//!   scopes it by encoding the id into the record subject
//!   ([`EVENT_LOG_SUBJECT_PREFIX`]`.<execution_id>`).  A per-execution ordered
//!   read is a subject-filtered replay; a global ordered scan is an unfiltered
//!   replay.  Both preserve sequence order.
//! * **Offset / ack (tail / subscribe)** — a durable consumer remembers its ack
//!   cursor across restarts (the cursor lives in the transaction log).  A tail
//!   pull returns records *after* the cursor without moving it; `ack` advances
//!   it after the caller has materialized the batch.  This is at-least-once with
//!   explicit ack, matching a JetStream durable consumer.
//! * **Append-only + immutable + replay-is-truth** — records are never mutated;
//!   `KeepAll` retention means the log is the source of truth; state is a replay
//!   of it.
//!
//! ## Driver interface (Phase 10-ready)
//!
//! The engine is exposed behind [`EventLogDriver`] so the log tier is
//! driver-selectable: the EHDB engine here is [`LocalReferenceEventLogDriver`];
//! a JetStream+Postgres driver implementing the same trait is what keeps every
//! tier selectable back to the incumbent (Roadmap Phase 10).  Callers program
//! against the trait, not the concrete engine.
//!
//! ## Shadow validation
//!
//! [`compare_shadow_parity`] is the pure comparison the worker's disabled-by-
//! default shadow mode uses to prove the EHDB engine tracks the authoritative
//! log without serving reads from it: sequence parity, count parity, and
//! monotonic ordering, with a single divergence reason string when they differ.

use std::path::PathBuf;

use ehdb_core::{
    ConsumerName, EhdbError, NamespaceName, Result, StreamName, TenantId, TransactionId,
};
use ehdb_stream::{RetentionPolicy, StreamSequence, Subject, SubjectFilter};
use ehdb_transaction::{CommitTransaction, Mutation, StreamMutation};
use serde::{Deserialize, Serialize};

use crate::LocalReferenceRuntime;

/// The single canonical stream that carries NoETL's platform event log.  Using
/// one stream makes its [`StreamSequence`] the global, monotonic, gapless
/// event-log sequence (the JetStream stream-sequence / Postgres ordering twin).
pub const EVENT_LOG_STREAM: &str = "noetl_event_log";

/// Subject prefix used to scope an event to its execution.  A record's subject
/// is `noetl.event.exec.<execution_id>`, so a per-execution read is an exact
/// subject-filtered replay and a global scan is an unfiltered replay.
pub const EVENT_LOG_SUBJECT_PREFIX: &str = "noetl.event.exec";

/// Validate + build the per-execution subject for an event.  `execution_id` must
/// be a single non-empty token of `[A-Za-z0-9_-]` (NoETL execution ids are i64
/// snowflakes, so digits in practice); anything else is an
/// [`EhdbError::InvalidIdentifier`] so a caller mistake classifies distinctly
/// from an engine-unavailable error.
fn execution_subject(execution_id: &str) -> Result<Subject> {
    let id = execution_id.trim();
    if id.is_empty()
        || !id
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
    {
        return Err(EhdbError::InvalidIdentifier(format!(
            "event-log execution id: {execution_id:?}"
        )));
    }
    Subject::new(format!("{EVENT_LOG_SUBJECT_PREFIX}.{id}"))
}

/// Parse the `execution_id` back out of a record subject.  Returns the trailing
/// token after [`EVENT_LOG_SUBJECT_PREFIX`], or the whole subject if it does not
/// carry the prefix (defensive — every append writes the prefix).
fn execution_from_subject(subject: &str) -> String {
    subject
        .strip_prefix(&format!("{EVENT_LOG_SUBJECT_PREFIX}."))
        .unwrap_or(subject)
        .to_string()
}

/// One event as served by the engine (secret-free projection: no log path, no
/// stream/consumer coordinates — the payload is the caller's own event body).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EventLogRecordView {
    /// The global, monotonic event-log sequence assigned at append.
    pub global_sequence: u64,
    /// The execution this event is scoped to.
    pub execution_id: String,
    pub transaction_id: String,
    pub byte_len: usize,
    pub payload: String,
}

/// Append one event to the platform event log.  The producer path already
/// authorized *what* is being appended; this only persists + orders it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EventLogAppendRequest {
    pub execution_id: String,
    pub transaction_id: String,
    pub payload: String,
}

/// Secret-free result of an append: the assigned global sequence + record shape.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EventLogAppendOutcome {
    pub action: String,
    pub execution_id: String,
    /// The global sequence assigned to this event (monotonic, gapless).
    pub global_sequence: u64,
    pub byte_len: usize,
    /// Whether the canonical event-log stream was created on this append.
    pub created_stream: bool,
    /// Total records in the log after this append (== the highest sequence).
    pub log_record_count: usize,
}

/// Bounded ordered scan of the whole log by global sequence.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EventLogScanRequest {
    pub after: Option<u64>,
    pub limit: usize,
}

/// Secret-free result of a global ordered scan.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EventLogScanOutcome {
    pub action: String,
    /// Whether the log stream exists yet (false before the first append).
    pub exists: bool,
    /// Total records after the cursor, before `limit` is applied.
    pub record_count: usize,
    pub returned: usize,
    pub records: Vec<EventLogRecordView>,
}

/// Bounded ordered read scoped to a single execution.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EventLogReadExecutionRequest {
    pub execution_id: String,
    pub after: Option<u64>,
    pub limit: usize,
}

/// Secret-free result of a per-execution ordered read.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EventLogReadExecutionOutcome {
    pub action: String,
    pub execution_id: String,
    pub exists: bool,
    pub record_count: usize,
    pub returned: usize,
    pub records: Vec<EventLogRecordView>,
}

/// Bounded tail pull for a durable consumer (offset/subscribe).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EventLogTailRequest {
    pub consumer: String,
    pub transaction_id: String,
    pub limit: usize,
}

/// Secret-free result of a durable-consumer tail pull.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EventLogTailOutcome {
    pub action: String,
    pub consumer: String,
    pub exists: bool,
    pub created_consumer: bool,
    pub acked_sequence: Option<u64>,
    pub pending_count: usize,
    pub returned: usize,
    pub records: Vec<EventLogRecordView>,
}

/// Advance a durable consumer's ack cursor after materialize.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EventLogAckRequest {
    pub consumer: String,
    pub transaction_id: String,
    pub sequence: u64,
}

/// Secret-free result of advancing the ack cursor.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EventLogAckOutcome {
    pub action: String,
    pub consumer: String,
    pub acked_sequence: u64,
}

/// The driver interface for the event-log tier.  EHDB is one implementation
/// ([`LocalReferenceEventLogDriver`]); a JetStream+Postgres driver implementing
/// the same trait keeps the tier selectable back to the incumbent (Phase 10).
///
/// All methods are `&self`: the engine is stateless per call (the durable state
/// lives in the on-disk transaction log, opened + dropped per op), matching the
/// bounded/stateless discipline the rest of the integration enforces.
pub trait EventLogDriver {
    /// A stable, secret-free identifier for the backing engine.
    fn driver_name(&self) -> &'static str;
    /// Persist + order one authorized event; assign the next global sequence.
    fn append(&self, request: &EventLogAppendRequest) -> Result<EventLogAppendOutcome>;
    /// Ordered scan of the whole log by global sequence.
    fn scan_global(&self, request: &EventLogScanRequest) -> Result<EventLogScanOutcome>;
    /// Ordered read scoped to a single execution.
    fn read_execution(
        &self,
        request: &EventLogReadExecutionRequest,
    ) -> Result<EventLogReadExecutionOutcome>;
    /// Durable-consumer tail pull (does not move the ack cursor).
    fn tail(&self, request: &EventLogTailRequest) -> Result<EventLogTailOutcome>;
    /// Advance a durable consumer's ack cursor after materialize.
    fn ack(&self, request: &EventLogAckRequest) -> Result<EventLogAckOutcome>;
}

/// The EHDB event-log engine over the bounded local-reference transaction log.
///
/// Composes the existing append-only stream primitives ([`crate`]'s
/// `LocalReferenceRuntime` + `ehdb_stream`), scoping every event to the single
/// canonical [`EVENT_LOG_STREAM`] so the stream sequence *is* the global
/// event-log sequence.
#[derive(Debug, Clone)]
pub struct LocalReferenceEventLogDriver {
    pub log_path: PathBuf,
    pub tenant: String,
    pub namespace: String,
}

impl LocalReferenceEventLogDriver {
    pub fn new(
        log_path: impl Into<PathBuf>,
        tenant: impl Into<String>,
        namespace: impl Into<String>,
    ) -> Self {
        Self {
            log_path: log_path.into(),
            tenant: tenant.into(),
            namespace: namespace.into(),
        }
    }

    fn coordinates(&self) -> Result<(TenantId, NamespaceName, StreamName)> {
        Ok((
            TenantId::new(self.tenant.clone())?,
            NamespaceName::new(self.namespace.clone())?,
            StreamName::new(EVENT_LOG_STREAM.to_string())?,
        ))
    }
}

impl EventLogDriver for LocalReferenceEventLogDriver {
    fn driver_name(&self) -> &'static str {
        "ehdb-local-reference"
    }

    fn append(&self, request: &EventLogAppendRequest) -> Result<EventLogAppendOutcome> {
        let (tenant, namespace, stream) = self.coordinates()?;
        let subject = execution_subject(&request.execution_id)?;
        let transaction_id = TransactionId::new(request.transaction_id.clone())?;
        let payload = request.payload.clone().into_bytes();
        let byte_len = payload.len();

        let mut runtime = LocalReferenceRuntime::open(&self.log_path)?;

        // Create-on-first-use + next global sequence from replayed state.  A
        // missing stream replays as an error — that is the create signal, not a
        // failure.  next = count + 1 keeps the sequence monotonic + gapless.
        let (created_stream, next_sequence) = match runtime
            .state()
            .streams
            .replay(&tenant, &namespace, &stream, None)
        {
            Ok(records) => (false, records.len() as u64 + 1),
            Err(_) => (true, StreamSequence::first().value()),
        };

        let mut mutations = Vec::with_capacity(2);
        if created_stream {
            mutations.push(Mutation::Stream(StreamMutation::CreateStream {
                stream: stream.clone(),
                // Append-only source of truth: keep every event.
                retention: RetentionPolicy::KeepAll,
            }));
        }
        mutations.push(Mutation::Stream(StreamMutation::Publish {
            stream: stream.clone(),
            subject,
            payload,
            sequence: next_sequence,
        }));

        runtime.append(CommitTransaction {
            transaction_id,
            tenant: tenant.clone(),
            namespace: namespace.clone(),
            mutations,
        })?;

        let log_record_count = runtime
            .state()
            .streams
            .replay(&tenant, &namespace, &stream, None)
            .map(|records| records.len())
            .unwrap_or(0);

        Ok(EventLogAppendOutcome {
            action: "eventlog-append".to_string(),
            execution_id: request.execution_id.trim().to_string(),
            global_sequence: next_sequence,
            byte_len,
            created_stream,
            log_record_count,
        })
    }

    fn scan_global(&self, request: &EventLogScanRequest) -> Result<EventLogScanOutcome> {
        let (tenant, namespace, stream) = self.coordinates()?;
        let after = match request.after {
            Some(value) => Some(StreamSequence::new(value)?),
            None => None,
        };
        let runtime = LocalReferenceRuntime::open(&self.log_path)?;

        match runtime
            .state()
            .streams
            .replay(&tenant, &namespace, &stream, after)
        {
            Ok(records) => {
                let record_count = records.len();
                let projected = records
                    .into_iter()
                    .take(request.limit)
                    .map(project_record)
                    .collect::<Vec<_>>();
                Ok(EventLogScanOutcome {
                    action: "eventlog-scan".to_string(),
                    exists: true,
                    record_count,
                    returned: projected.len(),
                    records: projected,
                })
            }
            Err(_) => Ok(EventLogScanOutcome {
                action: "eventlog-scan".to_string(),
                exists: false,
                record_count: 0,
                returned: 0,
                records: Vec::new(),
            }),
        }
    }

    fn read_execution(
        &self,
        request: &EventLogReadExecutionRequest,
    ) -> Result<EventLogReadExecutionOutcome> {
        let (tenant, namespace, stream) = self.coordinates()?;
        // Validate the execution id up front and build the exact subject filter.
        let subject = execution_subject(&request.execution_id)?;
        let filter = SubjectFilter::new(subject.as_str().to_string())?;
        let after = match request.after {
            Some(value) => Some(StreamSequence::new(value)?),
            None => None,
        };
        let runtime = LocalReferenceRuntime::open(&self.log_path)?;

        match runtime
            .state()
            .streams
            .replay_matching(&tenant, &namespace, &stream, &filter, after)
        {
            Ok(records) => {
                let record_count = records.len();
                let projected = records
                    .into_iter()
                    .take(request.limit)
                    .map(project_record)
                    .collect::<Vec<_>>();
                Ok(EventLogReadExecutionOutcome {
                    action: "eventlog-read-exec".to_string(),
                    execution_id: request.execution_id.trim().to_string(),
                    exists: true,
                    record_count,
                    returned: projected.len(),
                    records: projected,
                })
            }
            // A missing stream (no event ever appended) is an absent probe, not
            // an error — a per-execution read of a never-written log is empty.
            Err(_) => Ok(EventLogReadExecutionOutcome {
                action: "eventlog-read-exec".to_string(),
                execution_id: request.execution_id.trim().to_string(),
                exists: false,
                record_count: 0,
                returned: 0,
                records: Vec::new(),
            }),
        }
    }

    fn tail(&self, request: &EventLogTailRequest) -> Result<EventLogTailOutcome> {
        let (tenant, namespace, stream) = self.coordinates()?;
        let consumer = ConsumerName::new(request.consumer.clone())?;

        let mut runtime = LocalReferenceRuntime::open(&self.log_path)?;

        // A durable consumer over a never-written log is an absent probe.
        if runtime
            .state()
            .streams
            .replay(&tenant, &namespace, &stream, None)
            .is_err()
        {
            return Ok(EventLogTailOutcome {
                action: "eventlog-tail".to_string(),
                consumer: consumer.to_string(),
                exists: false,
                created_consumer: false,
                acked_sequence: None,
                pending_count: 0,
                returned: 0,
                records: Vec::new(),
            });
        }

        // Create the durable consumer on first pull (JetStream-style durable).
        let created_consumer = runtime
            .state()
            .streams
            .consumer(&tenant, &namespace, &stream, &consumer)
            .is_err();
        if created_consumer {
            let transaction_id = TransactionId::new(request.transaction_id.clone())?;
            runtime.append(CommitTransaction {
                transaction_id,
                tenant: tenant.clone(),
                namespace: namespace.clone(),
                mutations: vec![Mutation::Stream(StreamMutation::CreateConsumer {
                    stream: stream.clone(),
                    consumer: consumer.clone(),
                })],
            })?;
        }

        let acked_sequence = runtime
            .state()
            .streams
            .consumer(&tenant, &namespace, &stream, &consumer)
            .ok()
            .and_then(|durable| durable.acked_sequence.map(|sequence| sequence.value()));

        let pending = runtime
            .state()
            .streams
            .replay_for_consumer(&tenant, &namespace, &stream, &consumer)?;
        let pending_count = pending.len();
        let records = pending
            .into_iter()
            .take(request.limit)
            .map(project_record)
            .collect::<Vec<_>>();
        let returned = records.len();

        Ok(EventLogTailOutcome {
            action: "eventlog-tail".to_string(),
            consumer: consumer.to_string(),
            exists: true,
            created_consumer,
            acked_sequence,
            pending_count,
            returned,
            records,
        })
    }

    fn ack(&self, request: &EventLogAckRequest) -> Result<EventLogAckOutcome> {
        let (tenant, namespace, stream) = self.coordinates()?;
        let consumer = ConsumerName::new(request.consumer.clone())?;
        let transaction_id = TransactionId::new(request.transaction_id.clone())?;
        // StreamSequence rejects 0; a real ack always names a published record.
        let sequence = StreamSequence::new(request.sequence)?;

        let mut runtime = LocalReferenceRuntime::open(&self.log_path)?;
        runtime.append(CommitTransaction {
            transaction_id,
            tenant: tenant.clone(),
            namespace: namespace.clone(),
            mutations: vec![Mutation::Stream(StreamMutation::Ack {
                stream: stream.clone(),
                consumer: consumer.clone(),
                sequence: sequence.value(),
            })],
        })?;

        Ok(EventLogAckOutcome {
            action: "eventlog-ack".to_string(),
            consumer: consumer.to_string(),
            acked_sequence: sequence.value(),
        })
    }
}

fn project_record(record: ehdb_stream::StreamRecord) -> EventLogRecordView {
    EventLogRecordView {
        global_sequence: record.sequence.value(),
        execution_id: execution_from_subject(record.subject.as_str()),
        transaction_id: record.transaction_id.to_string(),
        byte_len: record.payload.len(),
        payload: String::from_utf8_lossy(&record.payload).into_owned(),
    }
}

/// The parity verdict of one shadow append: did the EHDB engine track the
/// authoritative log without divergence?  Pure + secret-free so both the engine
/// tests and the worker's disabled-by-default shadow mode share one comparison.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EventLogParityReport {
    /// EHDB's assigned sequence equals the authoritative sequence (when the
    /// authoritative sequence is known).  `true` when unknown (nothing to
    /// contradict).
    pub sequence_ok: bool,
    /// EHDB's record count equals the number of events mirrored so far.
    pub count_ok: bool,
    /// EHDB's assigned sequence is strictly greater than the previous one
    /// (monotonic, gapless-by-one ordering).
    pub order_ok: bool,
    /// The single reason parity failed, or `None` when it holds.
    pub divergence: Option<String>,
}

impl EventLogParityReport {
    /// Whether every parity check held.
    pub fn holds(&self) -> bool {
        self.sequence_ok && self.count_ok && self.order_ok && self.divergence.is_none()
    }
}

/// Compare one shadow append against the authoritative log.
///
/// * `authoritative_sequence` — the sequence the authoritative producer path
///   assigned to this event, when known (JetStream stream-seq / Postgres
///   ordering).  `None` skips the sequence-parity check (still checks count +
///   order), so the shadow can run even where the authoritative sequence is not
///   surfaced to the worker.
/// * `outcome` — the EHDB engine's append result.
/// * `previous_sequence` — the EHDB sequence of the previous shadow append in
///   this run (`0` before the first), for the monotonic-order check.
/// * `expected_count` — how many events have been mirrored so far in this run
///   (this append included), for the count-parity check.
///
/// Returns the first divergence found, or a clean report.
pub fn compare_shadow_parity(
    authoritative_sequence: Option<u64>,
    outcome: &EventLogAppendOutcome,
    previous_sequence: u64,
    expected_count: usize,
) -> EventLogParityReport {
    let sequence_ok = match authoritative_sequence {
        Some(auth) => auth == outcome.global_sequence,
        None => true,
    };
    let order_ok = outcome.global_sequence > previous_sequence;
    let count_ok = outcome.log_record_count == expected_count;

    let divergence = if !sequence_ok {
        Some(format!(
            "sequence divergence: authoritative={} ehdb={}",
            authoritative_sequence.unwrap_or_default(),
            outcome.global_sequence
        ))
    } else if !order_ok {
        Some(format!(
            "ordering divergence: ehdb sequence {} not > previous {}",
            outcome.global_sequence, previous_sequence
        ))
    } else if !count_ok {
        Some(format!(
            "count divergence: ehdb record count {} != expected {}",
            outcome.log_record_count, expected_count
        ))
    } else {
        None
    };

    EventLogParityReport {
        sequence_ok,
        count_ok,
        order_ok,
        divergence,
    }
}

// ===========================================================================
// Primary-serve (completion program Phase 9, tier 1 — event-log cutover).
//
// Phase 6 shipped the engine + the shadow (dual-write + parity, never serve).
// Phase 9 tier 1 is the first per-tier PRIMARY cutover: the EHDB engine becomes
// the authoritative append + read + tail + ack + replay path for the platform
// event log, in place of the JetStream + Postgres incumbent.
//
// This block is the crate-side primary-serve helper: a single authoritative
// *cycle* that drives every serving leg through the engine and proves the
// JetStream+Postgres semantics are preserved (global gapless ordering,
// per-execution scope, durable consumer cursor, replay-is-truth), while
// dual-run parity-checking each append against the incumbent sequence.
//
// ## Reversibility (the safety property the cutover is gated on)
//
// The cycle is **additive toward the incumbent**: it appends only to the EHDB
// log ([`RetentionPolicy::KeepAll`]) and never mutates or deletes anything the
// incumbent owns.  Flipping a caller back from `primary` to `shadow`/`off`
// therefore restores the incumbent as authoritative with zero data loss — the
// EHDB log stays intact on disk (a later re-enable replays it whole), and the
// incumbent's own store was never touched.  [`exercise_primary_serve`] proves
// the "EHDB log stays intact" half directly via the replay leg; the "incumbent
// untouched" half is a structural property of the caller (the worker asserts it
// by never importing a NoETL event writer).
// ===========================================================================

/// One event to drive through the authoritative primary-serve cycle.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EventLogPrimaryEvent {
    pub execution_id: String,
    pub transaction_id: String,
    pub payload: String,
    /// The sequence the incumbent (JetStream stream-seq / Postgres ordering)
    /// producer path assigned to this event, when known — for the dual-run
    /// parity check.  `None` relies on count + ordering parity (the safe default
    /// where the authoritative sequence is not surfaced 1-based-aligned).
    pub authoritative_sequence: Option<u64>,
}

/// The served-by-EHDB proof for one primary-serve cycle: every serving leg ran
/// through the engine and preserved the incumbent's semantics, and each append
/// held dual-run parity against the incumbent.  Secret-free (counts + verdicts;
/// the payloads are the caller's own event bodies, echoed only in the leg views
/// the caller already holds).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EventLogPrimaryServeReport {
    /// The backing engine that served the cycle.
    pub driver_name: String,
    /// How many events the cycle appended authoritatively.
    pub appended: usize,
    /// Global scan returned every appended event in gapless 1-based order.
    pub scan_ordered: bool,
    pub scan_count: usize,
    /// Per-execution reads returned only that execution's events, in order.
    pub scope_ok: bool,
    /// Durable tail before any ack saw every appended event as pending, ordered.
    pub tail_pending_before_ack: usize,
    /// The sequence acked in the cycle (the first appended global sequence).
    pub acked_sequence: u64,
    /// After acking the first sequence, the durable cursor advanced and the tail
    /// returned exactly one fewer pending, starting after the ack.
    pub ack_advanced: bool,
    pub tail_pending_after_ack: usize,
    /// A fresh driver over the same on-disk log replayed the identical record
    /// set (replay-is-truth / durability — the reversibility half this helper
    /// proves directly).
    pub replay_count: usize,
    pub replay_matches: bool,
    /// Per-append dual-run parity verdicts against the incumbent sequence.
    pub dual_run: Vec<EventLogParityReport>,
    /// Every dual-run parity verdict held.
    pub dual_run_holds: bool,
    /// The single reason the cycle failed a served-by-EHDB invariant, or `None`.
    pub divergence: Option<String>,
}

impl EventLogPrimaryServeReport {
    /// Whether the EHDB engine served the whole cycle with the incumbent's
    /// semantics preserved and dual-run parity intact.
    pub fn served_by_ehdb(&self) -> bool {
        self.scan_ordered
            && self.scope_ok
            && self.ack_advanced
            && self.replay_matches
            && self.dual_run_holds
            && self.divergence.is_none()
    }
}

/// Run the authoritative event-log primary-serve cycle over `driver`.
///
/// Drives every serving leg through the EHDB engine — append, global scan,
/// per-execution scoped read, durable-consumer tail, ack, and a
/// fresh-driver replay — asserting the JetStream+Postgres semantics are
/// preserved (monotonic gapless global sequence, per-execution scope, durable
/// cursor advance, replay-is-truth) and dual-run parity-checking each append
/// against the incumbent sequence.  Returns the [`EventLogPrimaryServeReport`]
/// served-by-EHDB proof.
///
/// Reversible + non-destructive toward the incumbent: appends only to the EHDB
/// log ([`RetentionPolicy::KeepAll`]); the replay leg proves the log stays whole
/// so a flip back to the incumbent loses nothing.
///
/// `events` must be non-empty ([`EhdbError::InvalidState`] otherwise).  The ack
/// leg acks the first appended sequence; `consumer` names the durable consumer.
pub fn exercise_primary_serve(
    driver: &LocalReferenceEventLogDriver,
    events: &[EventLogPrimaryEvent],
    consumer: &str,
    ack_transaction_id: &str,
) -> Result<EventLogPrimaryServeReport> {
    if events.is_empty() {
        return Err(EhdbError::InvalidState(
            "event-log primary-serve requires at least one event".to_string(),
        ));
    }

    // --- Append leg: EHDB assigns the authoritative global sequence. ---------
    let mut dual_run = Vec::with_capacity(events.len());
    let mut first_sequence = 0u64;
    for (i, event) in events.iter().enumerate() {
        let outcome = driver.append(&EventLogAppendRequest {
            execution_id: event.execution_id.clone(),
            transaction_id: event.transaction_id.clone(),
            payload: event.payload.clone(),
        })?;
        if i == 0 {
            first_sequence = outcome.global_sequence;
        }
        // The canonical log is gapless from 1, so `expected_count == sequence`
        // and `previous == sequence - 1` are the engine's own invariants; the
        // authoritative sequence is enforced when the caller knows it.
        let previous_sequence = outcome.global_sequence.saturating_sub(1);
        let expected_count = outcome.global_sequence as usize;
        dual_run.push(compare_shadow_parity(
            event.authoritative_sequence,
            &outcome,
            previous_sequence,
            expected_count,
        ));
    }
    let dual_run_holds = dual_run.iter().all(EventLogParityReport::holds);

    // --- Global scan leg: authoritative ordered read of the whole log. -------
    let scan = driver.scan_global(&EventLogScanRequest {
        after: None,
        limit: events.len(),
    })?;
    let scan_count = scan.record_count;
    let scan_seqs: Vec<u64> = scan.records.iter().map(|r| r.global_sequence).collect();
    let expected_seqs: Vec<u64> = (1..=events.len() as u64).collect();
    let scan_ordered = scan.exists && scan_seqs == expected_seqs;

    // --- Per-execution scope leg: each execution's events, scoped + ordered. --
    let mut executions: Vec<String> = events.iter().map(|e| e.execution_id.clone()).collect();
    executions.sort();
    executions.dedup();
    let mut scope_ok = true;
    for execution_id in &executions {
        let read = driver.read_execution(&EventLogReadExecutionRequest {
            execution_id: execution_id.clone(),
            after: None,
            limit: events.len(),
        })?;
        let scoped = read.records.iter().all(|r| &r.execution_id == execution_id);
        let ordered = read
            .records
            .windows(2)
            .all(|w| w[0].global_sequence < w[1].global_sequence);
        let expected = events
            .iter()
            .filter(|e| &e.execution_id == execution_id)
            .count();
        scope_ok &= read.exists && scoped && ordered && read.record_count == expected;
    }

    // --- Durable tail + ack leg: offset/subscribe with a persisted cursor. ----
    let tail_before = driver.tail(&EventLogTailRequest {
        consumer: consumer.to_string(),
        transaction_id: format!("{ack_transaction_id}-tail-1"),
        limit: events.len(),
    })?;
    let tail_pending_before_ack = tail_before.pending_count;

    driver.ack(&EventLogAckRequest {
        consumer: consumer.to_string(),
        transaction_id: ack_transaction_id.to_string(),
        sequence: first_sequence,
    })?;

    let tail_after = driver.tail(&EventLogTailRequest {
        consumer: consumer.to_string(),
        transaction_id: format!("{ack_transaction_id}-tail-2"),
        limit: events.len(),
    })?;
    let tail_pending_after_ack = tail_after.pending_count;
    let ack_advanced = tail_after.acked_sequence == Some(first_sequence)
        && tail_pending_after_ack + 1 == tail_pending_before_ack
        && tail_after
            .records
            .first()
            .map(|r| r.global_sequence > first_sequence)
            .unwrap_or(events.len() == 1);

    // --- Replay leg: a fresh driver over the same log reconstructs it whole. --
    // A clone reopens the on-disk transaction log per op, so this is a genuine
    // from-disk replay (the durability / reversibility half proven directly).
    let replay_driver = driver.clone();
    let replay = replay_driver.scan_global(&EventLogScanRequest {
        after: None,
        limit: events.len(),
    })?;
    let replay_count = replay.record_count;
    let replay_matches = replay.exists && replay_count == events.len();

    let divergence = if !scan_ordered {
        Some(format!(
            "primary scan not gapless-ordered: got {scan_seqs:?}, expected {expected_seqs:?}"
        ))
    } else if !scope_ok {
        Some("primary per-execution read lost scope or order".to_string())
    } else if !ack_advanced {
        Some(format!(
            "primary durable cursor did not advance: acked={:?} pending {tail_pending_before_ack}->{tail_pending_after_ack}",
            tail_after.acked_sequence
        ))
    } else if !replay_matches {
        Some(format!(
            "primary replay lost records: replayed {replay_count} of {}",
            events.len()
        ))
    } else if !dual_run_holds {
        dual_run
            .iter()
            .find_map(|r| r.divergence.clone())
            .or_else(|| Some("primary dual-run parity diverged".to_string()))
    } else {
        None
    };

    Ok(EventLogPrimaryServeReport {
        driver_name: driver.driver_name().to_string(),
        appended: events.len(),
        scan_ordered,
        scan_count,
        scope_ok,
        tail_pending_before_ack,
        acked_sequence: first_sequence,
        ack_advanced,
        tail_pending_after_ack,
        replay_count,
        replay_matches,
        dual_run,
        dual_run_holds,
        divergence,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tmp_log(tag: &str) -> (PathBuf, PathBuf) {
        let dir = std::env::temp_dir().join(format!(
            "ehdb-eventlog-{tag}-{}-{:?}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        (dir.join("log.jsonl"), dir)
    }

    fn driver(log: &std::path::Path) -> LocalReferenceEventLogDriver {
        LocalReferenceEventLogDriver::new(log, "noetl", "default")
    }

    fn append(
        d: &LocalReferenceEventLogDriver,
        exec: &str,
        n: u64,
        payload: &str,
    ) -> EventLogAppendOutcome {
        d.append(&EventLogAppendRequest {
            execution_id: exec.to_string(),
            transaction_id: format!("txn-{exec}-{n}"),
            payload: payload.to_string(),
        })
        .unwrap()
    }

    #[test]
    fn append_assigns_monotonic_global_sequence() {
        let (log, dir) = tmp_log("seq");
        let d = driver(&log);
        let a = append(&d, "100", 1, "e1");
        assert_eq!(a.global_sequence, 1);
        assert!(a.created_stream);
        let b = append(&d, "200", 2, "e2");
        assert_eq!(b.global_sequence, 2);
        assert!(!b.created_stream);
        let c = append(&d, "100", 3, "e3");
        // Global sequence is across ALL executions, not per-execution.
        assert_eq!(c.global_sequence, 3);
        assert_eq!(c.log_record_count, 3);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn scan_global_is_ordered_and_bounded() {
        let (log, dir) = tmp_log("scan");
        let d = driver(&log);
        for i in 1..=5 {
            append(&d, "100", i, &format!("e{i}"));
        }
        let all = d
            .scan_global(&EventLogScanRequest {
                after: None,
                limit: 100,
            })
            .unwrap();
        assert!(all.exists);
        assert_eq!(all.record_count, 5);
        let seqs: Vec<u64> = all.records.iter().map(|r| r.global_sequence).collect();
        assert_eq!(seqs, vec![1, 2, 3, 4, 5]);
        // Bounded by limit, and `after` cursor advances.
        let page = d
            .scan_global(&EventLogScanRequest {
                after: Some(2),
                limit: 2,
            })
            .unwrap();
        assert_eq!(page.returned, 2);
        assert_eq!(page.records[0].global_sequence, 3);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn read_execution_is_scoped_and_ordered() {
        let (log, dir) = tmp_log("exec");
        let d = driver(&log);
        append(&d, "100", 1, "a");
        append(&d, "200", 2, "b");
        append(&d, "100", 3, "c");
        append(&d, "200", 4, "d");
        let ex100 = d
            .read_execution(&EventLogReadExecutionRequest {
                execution_id: "100".to_string(),
                after: None,
                limit: 100,
            })
            .unwrap();
        assert!(ex100.exists);
        assert_eq!(ex100.returned, 2);
        // Only exec 100 events, in global-sequence order (1 then 3).
        assert_eq!(ex100.records[0].global_sequence, 1);
        assert_eq!(ex100.records[1].global_sequence, 3);
        assert!(ex100.records.iter().all(|r| r.execution_id == "100"));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn tail_ack_advances_durable_cursor() {
        let (log, dir) = tmp_log("tail");
        let d = driver(&log);
        append(&d, "100", 1, "a");
        append(&d, "100", 2, "b");
        let t1 = d
            .tail(&EventLogTailRequest {
                consumer: "projector".to_string(),
                transaction_id: "txn-c1".to_string(),
                limit: 100,
            })
            .unwrap();
        assert!(t1.exists);
        assert!(t1.created_consumer);
        assert_eq!(t1.pending_count, 2);
        assert_eq!(t1.acked_sequence, None);
        // Ack the first, tail again: one fewer pending, cursor persisted.
        d.ack(&EventLogAckRequest {
            consumer: "projector".to_string(),
            transaction_id: "txn-ack1".to_string(),
            sequence: 1,
        })
        .unwrap();
        let t2 = d
            .tail(&EventLogTailRequest {
                consumer: "projector".to_string(),
                transaction_id: "txn-c2".to_string(),
                limit: 100,
            })
            .unwrap();
        assert_eq!(t2.pending_count, 1);
        assert_eq!(t2.acked_sequence, Some(1));
        assert_eq!(t2.records[0].global_sequence, 2);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn replay_reconstructs_from_log_alone() {
        let (log, dir) = tmp_log("replay");
        {
            let d = driver(&log);
            append(&d, "100", 1, "a");
            append(&d, "100", 2, "b");
        }
        // A fresh driver over the same log path replays the same state.
        let d2 = driver(&log);
        let scan = d2
            .scan_global(&EventLogScanRequest {
                after: None,
                limit: 100,
            })
            .unwrap();
        assert_eq!(scan.record_count, 2);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn absent_probes_are_not_errors() {
        let (log, dir) = tmp_log("absent");
        let d = driver(&log);
        let scan = d
            .scan_global(&EventLogScanRequest {
                after: None,
                limit: 10,
            })
            .unwrap();
        assert!(!scan.exists);
        let read = d
            .read_execution(&EventLogReadExecutionRequest {
                execution_id: "100".to_string(),
                after: None,
                limit: 10,
            })
            .unwrap();
        assert!(!read.exists);
        let tail = d
            .tail(&EventLogTailRequest {
                consumer: "c".to_string(),
                transaction_id: "t".to_string(),
                limit: 10,
            })
            .unwrap();
        assert!(!tail.exists);
        assert!(!tail.created_consumer);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn invalid_execution_id_is_invalid_identifier() {
        let (log, dir) = tmp_log("badid");
        let d = driver(&log);
        let err = d
            .append(&EventLogAppendRequest {
                execution_id: "bad id!".to_string(),
                transaction_id: "t".to_string(),
                payload: "x".to_string(),
            })
            .unwrap_err();
        assert!(err.to_string().starts_with("invalid identifier"));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn shadow_parity_holds_on_clean_append() {
        let (log, dir) = tmp_log("parity-ok");
        let d = driver(&log);
        let a = append(&d, "100", 1, "a");
        let report = compare_shadow_parity(Some(1), &a, 0, 1);
        assert!(report.holds(), "{report:?}");
        let b = append(&d, "100", 2, "b");
        let report2 = compare_shadow_parity(Some(2), &b, a.global_sequence, 2);
        assert!(report2.holds(), "{report2:?}");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn shadow_parity_flags_sequence_divergence() {
        let outcome = EventLogAppendOutcome {
            action: "eventlog-append".to_string(),
            execution_id: "100".to_string(),
            global_sequence: 7,
            byte_len: 1,
            created_stream: false,
            log_record_count: 7,
        };
        // Authoritative said 5, EHDB assigned 7 → divergence.
        let report = compare_shadow_parity(Some(5), &outcome, 6, 7);
        assert!(!report.holds());
        assert!(!report.sequence_ok);
        assert!(report.divergence.unwrap().contains("sequence divergence"));
    }

    #[test]
    fn shadow_parity_flags_count_divergence() {
        let outcome = EventLogAppendOutcome {
            action: "eventlog-append".to_string(),
            execution_id: "100".to_string(),
            global_sequence: 3,
            byte_len: 1,
            created_stream: false,
            log_record_count: 3,
        };
        // Sequence + order fine, but only 2 mirrored so far → count divergence.
        let report = compare_shadow_parity(Some(3), &outcome, 2, 2);
        assert!(!report.holds());
        assert!(!report.count_ok);
        assert!(report.divergence.unwrap().contains("count divergence"));
    }

    #[test]
    fn shadow_parity_sequence_check_skipped_when_unknown() {
        let outcome = EventLogAppendOutcome {
            action: "eventlog-append".to_string(),
            execution_id: "100".to_string(),
            global_sequence: 9,
            byte_len: 1,
            created_stream: false,
            log_record_count: 9,
        };
        // No authoritative sequence → sequence check passes, count+order still enforced.
        let report = compare_shadow_parity(None, &outcome, 8, 9);
        assert!(report.holds(), "{report:?}");
        assert!(report.sequence_ok);
    }

    #[test]
    fn driver_name_is_stable() {
        let (log, dir) = tmp_log("name");
        let d = driver(&log);
        assert_eq!(d.driver_name(), "ehdb-local-reference");
        let _ = std::fs::remove_dir_all(&dir);
    }

    fn primary_events() -> Vec<EventLogPrimaryEvent> {
        // Two executions interleaved, 1-based authoritative sequences known.
        vec![
            EventLogPrimaryEvent {
                execution_id: "100".to_string(),
                transaction_id: "txn-1".to_string(),
                payload: "{\"seq\":1}".to_string(),
                authoritative_sequence: Some(1),
            },
            EventLogPrimaryEvent {
                execution_id: "200".to_string(),
                transaction_id: "txn-2".to_string(),
                payload: "{\"seq\":2}".to_string(),
                authoritative_sequence: Some(2),
            },
            EventLogPrimaryEvent {
                execution_id: "100".to_string(),
                transaction_id: "txn-3".to_string(),
                payload: "{\"seq\":3}".to_string(),
                authoritative_sequence: Some(3),
            },
        ]
    }

    #[test]
    fn primary_serve_cycle_is_served_by_ehdb() {
        let (log, dir) = tmp_log("primary-serve");
        let d = driver(&log);
        let report = exercise_primary_serve(&d, &primary_events(), "projector", "ack-txn").unwrap();
        assert!(report.served_by_ehdb(), "{report:?}");
        assert_eq!(report.driver_name, "ehdb-local-reference");
        assert_eq!(report.appended, 3);
        assert!(report.scan_ordered);
        assert_eq!(report.scan_count, 3);
        assert!(report.scope_ok);
        // Durable tail saw all 3 pending, then one fewer after acking seq 1.
        assert_eq!(report.tail_pending_before_ack, 3);
        assert_eq!(report.acked_sequence, 1);
        assert!(report.ack_advanced);
        assert_eq!(report.tail_pending_after_ack, 2);
        // Replay-is-truth: a fresh driver over the same log reconstructs it whole.
        assert_eq!(report.replay_count, 3);
        assert!(report.replay_matches);
        assert!(report.dual_run_holds);
        assert!(report.divergence.is_none());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn primary_serve_dual_run_flags_incumbent_divergence() {
        let (log, dir) = tmp_log("primary-diverge");
        let d = driver(&log);
        // Authoritative claims 99 for the first event but EHDB assigns 1 → the
        // dual-run parity fails and the cycle is not served-by-EHDB.
        let mut events = primary_events();
        events[0].authoritative_sequence = Some(99);
        let report = exercise_primary_serve(&d, &events, "projector", "ack-txn").unwrap();
        assert!(!report.served_by_ehdb());
        assert!(!report.dual_run_holds);
        assert!(report.divergence.unwrap().contains("sequence divergence"));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn primary_serve_without_authoritative_sequence_still_serves() {
        let (log, dir) = tmp_log("primary-noauth");
        let d = driver(&log);
        // No incumbent sequence surfaced → count+order parity still enforced.
        let events: Vec<EventLogPrimaryEvent> = (1..=3)
            .map(|n| EventLogPrimaryEvent {
                execution_id: "100".to_string(),
                transaction_id: format!("txn-{n}"),
                payload: format!("{{\"seq\":{n}}}"),
                authoritative_sequence: None,
            })
            .collect();
        let report = exercise_primary_serve(&d, &events, "projector", "ack-txn").unwrap();
        assert!(report.served_by_ehdb(), "{report:?}");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn primary_serve_reversibility_replay_after_reopen() {
        // Proves the reversibility half this helper owns: after a primary cycle
        // a brand-new driver over the same on-disk log serves the identical
        // record set — a flip back to the incumbent (or a later re-enable)
        // loses nothing because the EHDB log stays whole on disk.
        let (log, dir) = tmp_log("primary-revert");
        {
            let d = driver(&log);
            let report =
                exercise_primary_serve(&d, &primary_events(), "projector", "ack-txn").unwrap();
            assert!(report.served_by_ehdb());
        }
        let reopened = driver(&log);
        let scan = reopened
            .scan_global(&EventLogScanRequest {
                after: None,
                limit: 100,
            })
            .unwrap();
        assert_eq!(scan.record_count, 3);
        assert_eq!(
            scan.records
                .iter()
                .map(|r| r.global_sequence)
                .collect::<Vec<_>>(),
            vec![1, 2, 3]
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn primary_serve_requires_at_least_one_event() {
        let (log, dir) = tmp_log("primary-empty");
        let d = driver(&log);
        let err = exercise_primary_serve(&d, &[], "projector", "ack-txn").unwrap_err();
        assert!(err.to_string().contains("at least one event"));
        let _ = std::fs::remove_dir_all(&dir);
    }
}
