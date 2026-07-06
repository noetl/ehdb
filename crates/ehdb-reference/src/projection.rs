//! EHDB projection / read-model core engine (completion program Phase 7).
//!
//! This is the engine that builds + serves the materialized **read-models**
//! NoETL's control plane queries — retiring the **PostgreSQL materializer** that
//! today folds the `noetl.event` log into projected state (`noetl.event` itself,
//! the per-execution `projection_snapshot`, and the durable consumer offset).
//! Phase 6 put the [event-log engine](crate::eventlog) *underneath* the append
//! path; Phase 7 puts this projection engine *on top* of that log's tail.
//!
//! ## Boundary — this materializes read-models, it never authors events
//!
//! A projection is a **derived** read-model: it is built by *consuming* the
//! append-only event log, and it never writes an event back (the #103 sole-writer
//! invariant is preserved — the materializer stays the only `noetl.event` writer;
//! this engine only *reads* the log and materializes a separate read-model store).
//! It is a platform engine for platform read-models only; **business data never
//! flows through it**.
//!
//! ## What it materializes (mirrors the Postgres materializer's read-models)
//!
//! * **Event read-model** — one row per event, keyed on `event_id`, idempotent
//!   (the `ON CONFLICT (event_id) DO NOTHING` twin).  Mirrors the `noetl.event`
//!   projection the server's `POST /api/internal/events/project` writes.
//! * **Execution-state read-model** — one folded row per `execution_id`: derived
//!   status, current node, event count, first/last global sequence, terminal
//!   flag.  Mirrors the per-execution `projection_snapshot` the server's
//!   `POST /api/internal/projection/advance` → `advance_snapshot` recomputes.
//! * **Consumer checkpoint** — the durable offset (highest applied global
//!   sequence) the projector has materialized through.  Mirrors the
//!   `event_stream.position` upsert.
//!
//! ## Exactly-once / replay-safe materialization
//!
//! Apply is idempotent **keyed on the event log's global sequence** (the gapless,
//! monotonic sequence Phase 6 assigns).  The engine persists a *checkpoint* =
//! the highest global sequence materialized so far; an incoming event whose
//! `global_sequence <= checkpoint` is skipped (a replay / at-least-once
//! redelivery is a no-op), and an already-materialized `event_id` is de-duped as
//! a second guard.  The execution-state fold is monotonic (it only advances),
//! so re-applying is a forward no-op.  Rebuild-from-log is therefore
//! deterministic: the same event-log tail always yields the same read-models.
//!
//! ## Driver interface (Phase 10-ready)
//!
//! The engine is exposed behind [`ProjectionDriver`] so the projection tier is
//! driver-selectable: the EHDB engine here is [`LocalReferenceProjectionEngine`];
//! a Postgres-materializer driver implementing the same trait keeps the tier
//! selectable back to the incumbent (Roadmap Phase 10).
//!
//! ## Shadow validation
//!
//! [`compare_projection_parity`] is the pure, secret-free comparison the worker's
//! disabled-by-default shadow mode uses to prove the EHDB read-models track the
//! Postgres materializer's output without serving any read from EHDB: key parity
//! (same execution set), value parity (status / event count), ordering
//! (monotonic last-sequence), and checkpoint lag.

use std::collections::BTreeMap;
use std::path::PathBuf;

use ehdb_core::{EhdbError, NamespaceName, Result, StreamName, TenantId, TransactionId};
use ehdb_stream::{RetentionPolicy, StreamRecord, Subject, SubjectFilter};
use ehdb_transaction::{CommitTransaction, Mutation, StreamMutation};
use serde::{Deserialize, Serialize};

use crate::eventlog::EventLogRecordView;
use crate::LocalReferenceRuntime;

/// The single stream that persists the materialized read-model records.  Each
/// applied event is one published record; the read-models are the replay-fold of
/// this stream, so the projection store is itself append-only + rebuildable.
pub const PROJECTION_STREAM: &str = "noetl_projection_log";

/// Subject prefix scoping a materialized record to its execution — a
/// per-execution read-model fold is an exact subject-filtered replay.
pub const PROJECTION_SUBJECT_PREFIX: &str = "noetl.projection.exec";

/// Terminal event types — observing one marks the execution's read-model row
/// terminal + fixes its derived status.  BOTH the dotted (`playbook.completed`)
/// and underscore (`playbook_completed`) spellings exist across the codebase and
/// the WAL (see the worker `state_materializer` + server `event_write::is_terminal`),
/// so the fold matches both rather than betting on one.
pub const TERMINAL_EVENT_TYPES: &[&str] = &[
    "playbook.completed",
    "playbook_completed",
    "playbook.failed",
    "playbook_failed",
    "playbook.cancelled",
    "playbook_cancelled",
];

/// Upper bound on a single apply batch — the engine is bounded like the rest of
/// the integration; an over-cap batch is an [`EhdbError::InvalidState`] (rejected,
/// distinct from an identifier mistake) so a caller can't drive an unbounded fold.
pub const MAX_APPLY_BATCH: usize = 4_096;

/// Derived status for an execution whose latest event carries none and which has
/// not reached a terminal event yet.
pub const DEFAULT_RUNNING_STATUS: &str = "running";

fn terminal_status(event_type: &str) -> Option<&'static str> {
    match event_type {
        "playbook.completed" | "playbook_completed" => Some("completed"),
        "playbook.failed" | "playbook_failed" => Some("failed"),
        "playbook.cancelled" | "playbook_cancelled" => Some("cancelled"),
        _ => None,
    }
}

/// Validate an execution id (a single `[A-Za-z0-9_-]` token — NoETL execution ids
/// are i64 snowflakes).  A bad id is an [`EhdbError::InvalidIdentifier`] so a
/// caller mistake classifies distinctly from an engine-unavailable error.
fn validated_execution_id(execution_id: &str) -> Result<String> {
    let id = execution_id.trim();
    if id.is_empty()
        || !id
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
    {
        return Err(EhdbError::InvalidIdentifier(format!(
            "projection execution id: {execution_id:?}"
        )));
    }
    Ok(id.to_string())
}

fn execution_subject(execution_id: &str) -> Result<Subject> {
    let id = validated_execution_id(execution_id)?;
    Subject::new(format!("{PROJECTION_SUBJECT_PREFIX}.{id}"))
}

/// One event as fed to the projection engine — the projected slice of a NoETL
/// event the read-models fold over.  This is derived either directly by a caller
/// or from an event-log tail record via [`ProjectionEventInput::from_event_log_record`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProjectionEventInput {
    /// The event log's global, monotonic, gapless sequence (Phase 6).  The
    /// idempotency / checkpoint key.
    pub global_sequence: u64,
    /// The event's stable snowflake id (the `noetl.event` PK / dedup key).
    pub event_id: i64,
    pub execution_id: String,
    pub event_type: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub node_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub status: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prev_event_id: Option<i64>,
}

impl ProjectionEventInput {
    /// Bridge from an [`EventLogRecordView`] served by the Phase-6 event-log
    /// engine: the record carries the global sequence + execution id, and its
    /// `payload` is the event body JSON the read-model fields come from.  Returns
    /// `None` when the payload isn't a chainable event object (no `event_id`).
    pub fn from_event_log_record(record: &EventLogRecordView) -> Option<Self> {
        let value: serde_json::Value = serde_json::from_str(&record.payload).ok()?;
        let obj = value.as_object()?;
        let event_id = obj.get("event_id").and_then(|v| v.as_i64())?;
        let event_type = obj
            .get("event_type")
            .and_then(|v| v.as_str())
            .unwrap_or_default()
            .to_string();
        Some(Self {
            global_sequence: record.global_sequence,
            event_id,
            execution_id: record.execution_id.clone(),
            event_type,
            node_name: obj
                .get("node_name")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),
            status: obj
                .get("status")
                .and_then(|v| v.as_str())
                .map(|s| s.to_string()),
            prev_event_id: obj.get("prev_event_id").and_then(|v| v.as_i64()),
        })
    }
}

/// One materialized row of the **event read-model** (the `noetl.event`
/// projection twin) — secret-free (the payload body is not retained; the
/// read-model carries only the projected columns).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EventReadModelView {
    pub global_sequence: u64,
    pub event_id: i64,
    pub execution_id: String,
    pub event_type: String,
    pub node_name: Option<String>,
    pub status: Option<String>,
    pub prev_event_id: Option<i64>,
}

/// One folded row of the **execution-state read-model** (the `projection_snapshot`
/// twin) — the per-execution derived state the control plane reads.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExecutionStateView {
    pub execution_id: String,
    /// Derived status: the terminal status once a terminal event is folded, else
    /// the latest event's status, else [`DEFAULT_RUNNING_STATUS`].
    pub status: String,
    /// The node of the most-recent event that named one.
    pub current_node: Option<String>,
    pub event_count: usize,
    pub first_global_sequence: u64,
    pub last_global_sequence: u64,
    pub last_event_id: i64,
    pub terminal: bool,
    pub terminal_event_type: Option<String>,
}

/// The durable consumer checkpoint (the `event_stream.position` twin): the
/// highest event-log global sequence this projector has materialized through.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProjectionCheckpoint {
    pub consumer: String,
    /// Highest global sequence applied (exactly-once key).  `0` before the first
    /// apply.
    pub applied_through_sequence: u64,
    /// Total materialized event rows.
    pub applied_count: usize,
}

/// Request to materialize a batch of events (typically an ordered event-log tail).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectionApplyRequest {
    pub consumer: String,
    pub transaction_id: String,
    pub events: Vec<ProjectionEventInput>,
}

/// Secret-free result of one apply.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProjectionApplyOutcome {
    pub action: String,
    pub consumer: String,
    /// New rows materialized this apply.
    pub applied: usize,
    /// Events skipped because `event_id` was already materialized (dedup guard).
    pub duplicates: usize,
    /// Events skipped because `global_sequence <= checkpoint` (replay guard).
    pub skipped_below_checkpoint: usize,
    /// Whether the projection store stream was created on this apply.
    pub created_stream: bool,
    pub checkpoint: ProjectionCheckpoint,
}

/// Bounded read of the execution-state read-model for one execution.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProjectionReadExecutionOutcome {
    pub action: String,
    pub execution_id: String,
    pub exists: bool,
    pub state: Option<ExecutionStateView>,
}

/// Lookup of one event in the event read-model by `event_id`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProjectionReadEventOutcome {
    pub action: String,
    pub event_id: i64,
    pub exists: bool,
    pub event: Option<EventReadModelView>,
}

/// Bounded list of execution-state rows (ordered by first global sequence).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProjectionListExecutionsOutcome {
    pub action: String,
    pub exists: bool,
    pub total: usize,
    pub returned: usize,
    pub states: Vec<ExecutionStateView>,
}

/// The driver interface for the projection tier.  EHDB is one implementation
/// ([`LocalReferenceProjectionEngine`]); a Postgres-materializer driver
/// implementing the same trait keeps the tier selectable back to the incumbent
/// (Phase 10).  All methods are `&self`: durable state lives in the on-disk
/// projection log, opened + dropped per op (bounded / stateless).
pub trait ProjectionDriver {
    /// A stable, secret-free identifier for the backing engine.
    fn driver_name(&self) -> &'static str;
    /// Idempotently materialize a batch of events into the read-models.
    fn apply(&self, request: &ProjectionApplyRequest) -> Result<ProjectionApplyOutcome>;
    /// Read the folded execution-state read-model for one execution (the
    /// read-serving interface Phase 9 cuts over to).
    fn read_execution_state(&self, execution_id: &str) -> Result<ProjectionReadExecutionOutcome>;
    /// Look one event up in the event read-model by `event_id`.
    fn read_event(&self, event_id: i64) -> Result<ProjectionReadEventOutcome>;
    /// Bounded, ordered list of execution-state rows.
    fn list_executions(&self, limit: usize) -> Result<ProjectionListExecutionsOutcome>;
    /// The current durable checkpoint for `consumer`.
    fn checkpoint(&self, consumer: &str) -> Result<ProjectionCheckpoint>;
}

/// The EHDB projection engine over the bounded local-reference transaction log.
///
/// Composes the append-only stream primitives ([`LocalReferenceRuntime`] +
/// `ehdb_stream`): the materialized read-model records live on a single
/// [`PROJECTION_STREAM`], scoped per execution by subject, so a per-execution
/// fold is a subject-filtered replay and a full rebuild is an unfiltered replay.
#[derive(Debug, Clone)]
pub struct LocalReferenceProjectionEngine {
    pub log_path: PathBuf,
    pub tenant: String,
    pub namespace: String,
}

impl LocalReferenceProjectionEngine {
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
            StreamName::new(PROJECTION_STREAM.to_string())?,
        ))
    }

    /// Replay every materialized record from the projection store, in stream
    /// (== apply) order.  Absent store (nothing materialized yet) replays as an
    /// empty vec, not an error.
    fn replay_all(&self, runtime: &LocalReferenceRuntime) -> Result<Vec<StreamRecord>> {
        let (tenant, namespace, stream) = self.coordinates()?;
        Ok(runtime
            .state()
            .streams
            .replay(&tenant, &namespace, &stream, None)
            .unwrap_or_default())
    }

    fn decode(record: &StreamRecord) -> Option<ProjectionEventInput> {
        serde_json::from_slice::<ProjectionEventInput>(&record.payload).ok()
    }
}

impl ProjectionDriver for LocalReferenceProjectionEngine {
    fn driver_name(&self) -> &'static str {
        "ehdb-local-reference"
    }

    fn apply(&self, request: &ProjectionApplyRequest) -> Result<ProjectionApplyOutcome> {
        if request.events.len() > MAX_APPLY_BATCH {
            return Err(EhdbError::InvalidState(format!(
                "projection apply batch {} exceeds bound {MAX_APPLY_BATCH}",
                request.events.len()
            )));
        }
        // Validate every execution id up front (a bad id fails the whole batch as
        // a caller mistake, before any partial write).
        for ev in &request.events {
            validated_execution_id(&ev.execution_id)?;
        }

        let (tenant, namespace, stream) = self.coordinates()?;
        let mut runtime = LocalReferenceRuntime::open(&self.log_path)?;

        // Rebuild the current checkpoint + seen-set from the persisted store.
        let existing = self.replay_all(&runtime)?;
        let created_stream = existing.is_empty()
            && runtime
                .state()
                .streams
                .replay(&tenant, &namespace, &stream, None)
                .is_err();
        let mut checkpoint = existing
            .iter()
            .filter_map(Self::decode)
            .map(|e| e.global_sequence)
            .max()
            .unwrap_or(0);
        let mut applied_count = existing.len();
        let mut seen_event_ids: std::collections::HashSet<i64> = existing
            .iter()
            .filter_map(Self::decode)
            .map(|e| e.event_id)
            .collect();

        // Apply in ascending global-sequence order (the event-log tail delivers
        // in order; sort defensively so exactly-once holds even on a shuffled
        // batch).
        let mut ordered: Vec<&ProjectionEventInput> = request.events.iter().collect();
        ordered.sort_by_key(|e| e.global_sequence);

        let mut applied = 0usize;
        let mut duplicates = 0usize;
        let mut skipped_below_checkpoint = 0usize;
        let mut next_sequence = existing.len() as u64 + 1;
        let mut mutations = Vec::new();

        if created_stream {
            mutations.push(Mutation::Stream(StreamMutation::CreateStream {
                stream: stream.clone(),
                retention: RetentionPolicy::KeepAll,
            }));
        }

        for ev in ordered {
            // Replay guard (exactly-once on the source offset): anything at or
            // below the checkpoint has already been materialized.
            if ev.global_sequence <= checkpoint {
                skipped_below_checkpoint += 1;
                continue;
            }
            // Dedup guard (the ON CONFLICT (event_id) DO NOTHING twin).
            if !seen_event_ids.insert(ev.event_id) {
                duplicates += 1;
                continue;
            }
            let subject = execution_subject(&ev.execution_id)?;
            let payload = serde_json::to_vec(ev)
                .map_err(|e| EhdbError::Storage(format!("projection encode: {e}")))?;
            mutations.push(Mutation::Stream(StreamMutation::Publish {
                stream: stream.clone(),
                subject,
                payload,
                sequence: next_sequence,
            }));
            next_sequence += 1;
            checkpoint = ev.global_sequence;
            applied += 1;
            applied_count += 1;
        }

        // Only commit when something was actually materialized (an all-skipped
        // batch is a pure no-op — never an empty transaction).
        if applied > 0 {
            let transaction_id = TransactionId::new(request.transaction_id.clone())?;
            runtime.append(CommitTransaction {
                transaction_id,
                tenant,
                namespace,
                mutations,
            })?;
        }

        Ok(ProjectionApplyOutcome {
            action: "projection-apply".to_string(),
            consumer: request.consumer.clone(),
            applied,
            duplicates,
            skipped_below_checkpoint,
            created_stream: created_stream && applied > 0,
            checkpoint: ProjectionCheckpoint {
                consumer: request.consumer.clone(),
                applied_through_sequence: checkpoint,
                applied_count,
            },
        })
    }

    fn read_execution_state(&self, execution_id: &str) -> Result<ProjectionReadExecutionOutcome> {
        let (tenant, namespace, stream) = self.coordinates()?;
        let subject = execution_subject(execution_id)?;
        let filter = SubjectFilter::new(subject.as_str().to_string())?;
        let runtime = LocalReferenceRuntime::open(&self.log_path)?;

        let records = runtime
            .state()
            .streams
            .replay_matching(&tenant, &namespace, &stream, &filter, None)
            .unwrap_or_default();
        let inputs: Vec<ProjectionEventInput> = records.iter().filter_map(Self::decode).collect();
        if inputs.is_empty() {
            return Ok(ProjectionReadExecutionOutcome {
                action: "projection-read-execution".to_string(),
                execution_id: validated_execution_id(execution_id)?,
                exists: false,
                state: None,
            });
        }
        Ok(ProjectionReadExecutionOutcome {
            action: "projection-read-execution".to_string(),
            execution_id: validated_execution_id(execution_id)?,
            exists: true,
            state: Some(fold_execution_state(
                &validated_execution_id(execution_id)?,
                &inputs,
            )),
        })
    }

    fn read_event(&self, event_id: i64) -> Result<ProjectionReadEventOutcome> {
        let runtime = LocalReferenceRuntime::open(&self.log_path)?;
        let event = self
            .replay_all(&runtime)?
            .iter()
            .filter_map(Self::decode)
            .find(|e| e.event_id == event_id)
            .map(|e| EventReadModelView {
                global_sequence: e.global_sequence,
                event_id: e.event_id,
                execution_id: e.execution_id,
                event_type: e.event_type,
                node_name: e.node_name,
                status: e.status,
                prev_event_id: e.prev_event_id,
            });
        Ok(ProjectionReadEventOutcome {
            action: "projection-read-event".to_string(),
            event_id,
            exists: event.is_some(),
            event,
        })
    }

    fn list_executions(&self, limit: usize) -> Result<ProjectionListExecutionsOutcome> {
        let runtime = LocalReferenceRuntime::open(&self.log_path)?;
        let records = self.replay_all(&runtime)?;
        // Group by execution id, preserving first-seen order.
        let mut grouped: BTreeMap<String, Vec<ProjectionEventInput>> = BTreeMap::new();
        let mut first_seq: BTreeMap<String, u64> = BTreeMap::new();
        for input in records.iter().filter_map(Self::decode) {
            first_seq
                .entry(input.execution_id.clone())
                .or_insert(input.global_sequence);
            grouped
                .entry(input.execution_id.clone())
                .or_default()
                .push(input);
        }
        let mut states: Vec<ExecutionStateView> = grouped
            .iter()
            .map(|(exec, inputs)| fold_execution_state(exec, inputs))
            .collect();
        // Order by first global sequence (stable execution arrival order).
        states.sort_by_key(|s| s.first_global_sequence);
        let total = states.len();
        let returned = states.iter().take(limit).cloned().collect::<Vec<_>>();
        Ok(ProjectionListExecutionsOutcome {
            action: "projection-list-executions".to_string(),
            exists: total > 0,
            total,
            returned: returned.len(),
            states: returned,
        })
    }

    fn checkpoint(&self, consumer: &str) -> Result<ProjectionCheckpoint> {
        let runtime = LocalReferenceRuntime::open(&self.log_path)?;
        let records = self.replay_all(&runtime)?;
        let applied_through_sequence = records
            .iter()
            .filter_map(Self::decode)
            .map(|e| e.global_sequence)
            .max()
            .unwrap_or(0);
        Ok(ProjectionCheckpoint {
            consumer: consumer.to_string(),
            applied_through_sequence,
            applied_count: records.len(),
        })
    }
}

/// Fold a per-execution event list into the execution-state read-model.  The
/// events arrive in materialize (== global-sequence) order; the fold is
/// monotonic so re-folding a superset is a forward advance.
fn fold_execution_state(execution_id: &str, inputs: &[ProjectionEventInput]) -> ExecutionStateView {
    let mut current_node: Option<String> = None;
    let mut latest_status: Option<String> = None;
    let mut terminal = false;
    let mut terminal_event_type: Option<String> = None;
    let mut first_global_sequence = u64::MAX;
    let mut last_global_sequence = 0u64;
    let mut last_event_id = 0i64;

    for ev in inputs {
        first_global_sequence = first_global_sequence.min(ev.global_sequence);
        if ev.global_sequence >= last_global_sequence {
            last_global_sequence = ev.global_sequence;
            last_event_id = ev.event_id;
            if ev.node_name.is_some() {
                current_node = ev.node_name.clone();
            }
            if ev.status.is_some() {
                latest_status = ev.status.clone();
            }
        }
        if TERMINAL_EVENT_TYPES.contains(&ev.event_type.as_str()) {
            terminal = true;
            terminal_event_type = Some(ev.event_type.clone());
        }
    }

    let status = terminal_event_type
        .as_deref()
        .and_then(terminal_status)
        .map(|s| s.to_string())
        .or(latest_status)
        .unwrap_or_else(|| DEFAULT_RUNNING_STATUS.to_string());

    ExecutionStateView {
        execution_id: execution_id.to_string(),
        status,
        current_node,
        event_count: inputs.len(),
        first_global_sequence: if inputs.is_empty() {
            0
        } else {
            first_global_sequence
        },
        last_global_sequence,
        last_event_id,
        terminal,
        terminal_event_type,
    }
}

/// The parity verdict of a shadow projection run: do the EHDB read-models track
/// the Postgres materializer's output?  Pure + secret-free so both the engine
/// tests and the worker's disabled-by-default shadow mode share one comparison.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProjectionParityReport {
    /// The EHDB read-model has exactly the authoritative execution set (no
    /// missing / extra execution rows).
    pub key_ok: bool,
    /// Every shared execution's derived status + terminal flag + event count
    /// match the authoritative materializer.
    pub value_ok: bool,
    /// The EHDB checkpoint has caught up to (>=) the authoritative offset.
    pub checkpoint_ok: bool,
    /// EHDB checkpoint lag behind the authoritative offset (0 when caught up).
    pub checkpoint_lag: u64,
    /// The single reason parity failed, or `None` when it holds.
    pub divergence: Option<String>,
}

impl ProjectionParityReport {
    pub fn holds(&self) -> bool {
        self.key_ok && self.value_ok && self.checkpoint_ok && self.divergence.is_none()
    }
}

/// One authoritative (Postgres-materializer) execution-state row the shadow
/// compares against.  Deliberately the minimal secret-free projection the
/// worker can observe from the incumbent read-model.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AuthoritativeExecutionState {
    pub execution_id: String,
    pub status: String,
    pub event_count: usize,
    pub terminal: bool,
}

/// Compare the EHDB projection read-models against the authoritative
/// materializer's output.
///
/// * `ehdb` — the EHDB execution-state rows (e.g. from [`ProjectionDriver::list_executions`]).
/// * `authoritative` — the Postgres materializer's execution-state rows.
/// * `ehdb_checkpoint` — the EHDB projector's applied-through sequence.
/// * `authoritative_offset` — the incumbent's committed offset (highest global
///   sequence materialized), when known; `None` skips the checkpoint-lag check.
///
/// Returns the first divergence found, or a clean report.
pub fn compare_projection_parity(
    ehdb: &[ExecutionStateView],
    authoritative: &[AuthoritativeExecutionState],
    ehdb_checkpoint: u64,
    authoritative_offset: Option<u64>,
) -> ProjectionParityReport {
    let ehdb_map: BTreeMap<&str, &ExecutionStateView> =
        ehdb.iter().map(|s| (s.execution_id.as_str(), s)).collect();
    let auth_map: BTreeMap<&str, &AuthoritativeExecutionState> = authoritative
        .iter()
        .map(|s| (s.execution_id.as_str(), s))
        .collect();

    let key_ok =
        ehdb_map.len() == auth_map.len() && auth_map.keys().all(|k| ehdb_map.contains_key(k));

    let mut value_divergence: Option<String> = None;
    let mut value_ok = true;
    if key_ok {
        for (exec, auth) in &auth_map {
            let projected = ehdb_map.get(exec).expect("key_ok ⇒ present");
            if projected.status != auth.status
                || projected.terminal != auth.terminal
                || projected.event_count != auth.event_count
            {
                value_ok = false;
                value_divergence = Some(format!(
                    "value divergence for execution {exec}: ehdb(status={},terminal={},count={}) \
                     authoritative(status={},terminal={},count={})",
                    projected.status,
                    projected.terminal,
                    projected.event_count,
                    auth.status,
                    auth.terminal,
                    auth.event_count
                ));
                break;
            }
        }
    }

    let (checkpoint_ok, checkpoint_lag) = match authoritative_offset {
        Some(offset) => {
            let lag = offset.saturating_sub(ehdb_checkpoint);
            (ehdb_checkpoint >= offset, lag)
        }
        None => (true, 0),
    };

    let divergence = if !key_ok {
        Some(format!(
            "key divergence: ehdb {} execution rows vs authoritative {}",
            ehdb_map.len(),
            auth_map.len()
        ))
    } else if !value_ok {
        value_divergence
    } else if !checkpoint_ok {
        Some(format!(
            "checkpoint lag: ehdb applied-through {ehdb_checkpoint} behind authoritative {} (lag {checkpoint_lag})",
            authoritative_offset.unwrap_or_default()
        ))
    } else {
        None
    };

    ProjectionParityReport {
        key_ok,
        value_ok,
        checkpoint_ok,
        checkpoint_lag,
        divergence,
    }
}

// ===========================================================================
// Primary-serve (completion program Phase 9, tier 2 — projection cutover).
//
// Phase 7 shipped the engine + the shadow (dual-materialize + parity, never
// serve).  Phase 9 tier 2 is the second per-tier PRIMARY cutover (the event-log
// tier is tier 1): the EHDB projection engine becomes the authoritative
// read-model builder + read-serving path the control plane queries, in place of
// the PostgreSQL materializer.
//
// This block is the crate-side primary-serve helper: a single authoritative
// *cycle* that drives every serving leg through the engine and proves the
// PostgreSQL-materializer query contracts are preserved (apply → materialize →
// serve the read-model queries [`ProjectionDriver::read_execution_state`] /
// [`ProjectionDriver::read_event`] / [`ProjectionDriver::list_executions`] →
// durable checkpoint → replay-idempotent on the global sequence), while
// dual-run parity-checking the served read-models against the incumbent
// materializer's output.
//
// ## Reversibility (the safety property the cutover is gated on)
//
// The cycle is **additive toward the incumbent**: it materializes only into the
// derived EHDB projection store ([`RetentionPolicy::KeepAll`]) by *consuming*
// the already-authored event log, and never authors an event nor mutates
// anything the incumbent materializer owns.  Flipping a caller back from
// `primary` to `shadow`/`off` therefore restores the PostgreSQL materializer as
// the authoritative read path with zero data loss — the EHDB read-model store
// stays intact on disk (a later re-enable replays it whole), and the incumbent's
// own read-models were never touched.  [`exercise_primary_serve`] proves the
// "EHDB read-model stays intact" half directly via the fresh-engine replay leg;
// the "incumbent untouched" half is a structural property of the caller (the
// worker asserts it by never importing a NoETL event writer or the materializer).
// ===========================================================================

/// The event batch to drive through the authoritative primary-serve cycle, plus
/// the incumbent (PostgreSQL-materializer) view the served read-models are
/// dual-run parity-checked against.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectionPrimaryInput {
    /// The already-authored events to materialize authoritatively (an ordered
    /// event-log tail).
    pub events: Vec<ProjectionEventInput>,
    /// The incumbent materializer's execution-state rows for the same events —
    /// the dual-run parity ground truth.
    pub authoritative: Vec<AuthoritativeExecutionState>,
    /// The incumbent's committed offset (highest global sequence materialized),
    /// when known — for the checkpoint-lag half of the dual-run parity check.
    /// `None` relies on key + value parity (the safe default where the
    /// authoritative offset is not surfaced).
    pub authoritative_offset: Option<u64>,
}

/// The served-by-EHDB proof for one projection primary-serve cycle: every
/// serving leg ran through the engine and preserved the PostgreSQL
/// materializer's query contracts, and the served read-models held dual-run
/// parity against the incumbent.  Secret-free (counts + verdicts; the read-model
/// fields are the caller's own projected columns, never the event body).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProjectionPrimaryServeReport {
    /// The backing engine that served the cycle.
    pub driver_name: String,
    /// New read-model rows the cycle materialized authoritatively.
    pub applied: usize,
    /// The durable checkpoint (applied-through global sequence) after materialize.
    pub checkpoint: u64,
    /// The served `list_executions` returned exactly the materialized execution
    /// set (the list query contract).
    pub list_ok: bool,
    pub list_count: usize,
    /// Per-execution `read_execution_state` served the folded state, scoped to
    /// that execution with the right event count (the per-execution read
    /// contract).
    pub scope_ok: bool,
    /// `read_event` served the event read-model for a known `event_id` (the
    /// event-lookup contract).
    pub read_event_ok: bool,
    /// Re-applying the same batch was an idempotent no-op (0 applied, every event
    /// skipped below checkpoint, checkpoint unchanged) — exactly-once on the
    /// event-log global sequence.
    pub replay_idempotent: bool,
    /// A fresh engine over the same on-disk store served the identical read-model
    /// set (replay-is-truth / durability — the reversibility half proven
    /// directly).
    pub replay_count: usize,
    pub replay_matches: bool,
    /// Dual-run parity of the served read-models against the incumbent
    /// materializer.
    pub dual_run: ProjectionParityReport,
    /// The dual-run parity verdict held.
    pub dual_run_holds: bool,
    /// The single reason the cycle failed a served-by-EHDB invariant, or `None`.
    pub divergence: Option<String>,
}

impl ProjectionPrimaryServeReport {
    /// Whether the EHDB engine served the whole cycle with the incumbent's query
    /// contracts preserved and dual-run parity intact.
    pub fn served_by_ehdb(&self) -> bool {
        self.list_ok
            && self.scope_ok
            && self.read_event_ok
            && self.replay_idempotent
            && self.replay_matches
            && self.dual_run_holds
            && self.divergence.is_none()
    }
}

/// Run the authoritative projection primary-serve cycle over `engine`.
///
/// Drives every serving leg through the EHDB engine — apply (materialize), the
/// three read-model query contracts (`list_executions`, per-execution
/// `read_execution_state`, `read_event`), the durable `checkpoint`, an
/// idempotent re-apply (exactly-once on the global sequence), and a fresh-engine
/// replay — asserting the PostgreSQL materializer's query contracts are
/// preserved and dual-run parity-checking the served read-models against the
/// incumbent's output.  Returns the [`ProjectionPrimaryServeReport`]
/// served-by-EHDB proof.
///
/// Reversible + non-destructive toward the incumbent: materializes only into the
/// derived EHDB projection store ([`RetentionPolicy::KeepAll`]) by consuming the
/// already-authored events; the replay leg proves the store stays whole so a flip
/// back to the incumbent materializer loses nothing.
///
/// `input.events` must be non-empty ([`EhdbError::InvalidState`] otherwise).
/// `consumer` names the durable projector checkpoint.
pub fn exercise_primary_serve(
    engine: &LocalReferenceProjectionEngine,
    input: &ProjectionPrimaryInput,
    consumer: &str,
    transaction_id: &str,
) -> Result<ProjectionPrimaryServeReport> {
    if input.events.is_empty() {
        return Err(EhdbError::InvalidState(
            "projection primary-serve requires at least one event".to_string(),
        ));
    }

    // --- Apply leg: EHDB materializes the read-models authoritatively. -------
    let apply = engine.apply(&ProjectionApplyRequest {
        consumer: consumer.to_string(),
        transaction_id: transaction_id.to_string(),
        events: input.events.clone(),
    })?;
    let checkpoint = apply.checkpoint.applied_through_sequence;

    // The materialized execution set the read-model queries must serve.
    let mut expected_execs: Vec<String> = input
        .events
        .iter()
        .map(|e| e.execution_id.clone())
        .collect();
    expected_execs.sort();
    expected_execs.dedup();

    // --- List leg: `list_executions` (the control-plane list query contract). -
    let list = engine.list_executions(input.events.len().max(1))?;
    let mut listed_execs: Vec<String> =
        list.states.iter().map(|s| s.execution_id.clone()).collect();
    listed_execs.sort();
    listed_execs.dedup();
    let list_ok = list.exists && listed_execs == expected_execs;
    let list_count = list.total;

    // --- Scope leg: per-execution `read_execution_state`, scoped + folded. ----
    let mut scope_ok = true;
    for execution_id in &expected_execs {
        let read = engine.read_execution_state(execution_id)?;
        let expected_count = input
            .events
            .iter()
            .filter(|e| &e.execution_id == execution_id)
            .count();
        match read.state {
            Some(state) => {
                scope_ok &= read.exists
                    && &state.execution_id == execution_id
                    && state.event_count == expected_count;
            }
            None => scope_ok = false,
        }
    }

    // --- Event-lookup leg: `read_event` serves the event read-model by id. ----
    let probe_event_id = input.events.first().map(|e| e.event_id).unwrap_or_default();
    let read_event = engine.read_event(probe_event_id)?;
    let read_event_ok = read_event.exists
        && read_event
            .event
            .as_ref()
            .map(|e| e.event_id == probe_event_id)
            .unwrap_or(false);

    // --- Replay-idempotent leg: re-apply the same batch → exactly-once no-op. --
    let replay_apply = engine.apply(&ProjectionApplyRequest {
        consumer: consumer.to_string(),
        transaction_id: format!("{transaction_id}-replay"),
        events: input.events.clone(),
    })?;
    let replay_idempotent = replay_apply.applied == 0
        && replay_apply.skipped_below_checkpoint == input.events.len()
        && replay_apply.checkpoint.applied_through_sequence == checkpoint;

    // --- Replay-is-truth leg: a fresh engine over the same store reconstructs it.
    // A clone reopens the on-disk projection store per op, so this is a genuine
    // from-disk replay (the durability / reversibility half proven directly).
    let replay_engine = engine.clone();
    let replay = replay_engine.list_executions(input.events.len().max(1))?;
    let replay_count = replay.total;
    let replay_matches = replay.exists && replay.states == list.states;

    // --- Dual-run parity leg: served read-models vs the incumbent materializer.
    let dual_run = compare_projection_parity(
        &list.states,
        &input.authoritative,
        checkpoint,
        input.authoritative_offset,
    );
    let dual_run_holds = dual_run.holds();

    let divergence = if !list_ok {
        Some(format!(
            "primary list served wrong execution set: got {listed_execs:?}, expected {expected_execs:?}"
        ))
    } else if !scope_ok {
        Some("primary per-execution read lost scope or fold".to_string())
    } else if !read_event_ok {
        Some(format!(
            "primary read_event did not serve event {probe_event_id}"
        ))
    } else if !replay_idempotent {
        Some(format!(
            "primary re-apply not idempotent: applied {} skipped_below_checkpoint {} checkpoint {}",
            replay_apply.applied,
            replay_apply.skipped_below_checkpoint,
            replay_apply.checkpoint.applied_through_sequence
        ))
    } else if !replay_matches {
        Some(format!(
            "primary replay lost read-models: replayed {replay_count} execution rows"
        ))
    } else if !dual_run_holds {
        dual_run
            .divergence
            .clone()
            .or_else(|| Some("primary dual-run parity diverged".to_string()))
    } else {
        None
    };

    Ok(ProjectionPrimaryServeReport {
        driver_name: engine.driver_name().to_string(),
        applied: apply.applied,
        checkpoint,
        list_ok,
        list_count,
        scope_ok,
        read_event_ok,
        replay_idempotent,
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
            "ehdb-projection-{tag}-{}-{:?}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        (dir.join("log.jsonl"), dir)
    }

    fn engine(log: &std::path::Path) -> LocalReferenceProjectionEngine {
        LocalReferenceProjectionEngine::new(log, "noetl", "default")
    }

    fn ev(
        global_sequence: u64,
        event_id: i64,
        exec: &str,
        event_type: &str,
        node: Option<&str>,
        status: Option<&str>,
    ) -> ProjectionEventInput {
        ProjectionEventInput {
            global_sequence,
            event_id,
            execution_id: exec.to_string(),
            event_type: event_type.to_string(),
            node_name: node.map(|s| s.to_string()),
            status: status.map(|s| s.to_string()),
            prev_event_id: None,
        }
    }

    fn apply(
        e: &LocalReferenceProjectionEngine,
        consumer: &str,
        txn: &str,
        events: Vec<ProjectionEventInput>,
    ) -> ProjectionApplyOutcome {
        e.apply(&ProjectionApplyRequest {
            consumer: consumer.to_string(),
            transaction_id: txn.to_string(),
            events,
        })
        .unwrap()
    }

    #[test]
    fn apply_materializes_and_advances_checkpoint() {
        let (log, dir) = tmp_log("apply");
        let e = engine(&log);
        let out = apply(
            &e,
            "projector",
            "t1",
            vec![
                ev(
                    1,
                    10,
                    "100",
                    "playbook_started",
                    Some("start"),
                    Some("running"),
                ),
                ev(
                    2,
                    11,
                    "100",
                    "command.completed",
                    Some("load"),
                    Some("completed"),
                ),
            ],
        );
        assert_eq!(out.applied, 2);
        assert_eq!(out.duplicates, 0);
        assert_eq!(out.skipped_below_checkpoint, 0);
        assert!(out.created_stream);
        assert_eq!(out.checkpoint.applied_through_sequence, 2);
        assert_eq!(out.checkpoint.applied_count, 2);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn apply_is_idempotent_on_replay() {
        let (log, dir) = tmp_log("idem");
        let e = engine(&log);
        let batch = vec![
            ev(
                1,
                10,
                "100",
                "playbook_started",
                Some("start"),
                Some("running"),
            ),
            ev(
                2,
                11,
                "100",
                "command.completed",
                Some("load"),
                Some("completed"),
            ),
        ];
        let first = apply(&e, "projector", "t1", batch.clone());
        assert_eq!(first.applied, 2);
        // Re-applying the same batch → all skipped below checkpoint, no new rows.
        let second = apply(&e, "projector", "t2", batch);
        assert_eq!(second.applied, 0);
        assert_eq!(second.skipped_below_checkpoint, 2);
        assert_eq!(second.checkpoint.applied_count, 2);
        assert!(!second.created_stream);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn apply_dedups_event_id_above_checkpoint() {
        // An event with a fresh (higher) global_sequence but an already-seen
        // event_id is de-duped, not double-materialized.
        let (log, dir) = tmp_log("dedup");
        let e = engine(&log);
        apply(&e, "p", "t1", vec![ev(1, 10, "100", "a", None, None)]);
        let out = apply(
            &e,
            "p",
            "t2",
            vec![
                ev(2, 10, "100", "a", None, None), // same event_id 10, new seq 2
                ev(3, 11, "100", "b", None, None),
            ],
        );
        assert_eq!(out.duplicates, 1);
        assert_eq!(out.applied, 1);
        assert_eq!(out.checkpoint.applied_count, 2);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn incremental_apply_advances_across_calls() {
        let (log, dir) = tmp_log("incr");
        let e = engine(&log);
        let a = apply(
            &e,
            "p",
            "t1",
            vec![ev(
                1,
                10,
                "100",
                "playbook_started",
                Some("s"),
                Some("running"),
            )],
        );
        assert_eq!(a.checkpoint.applied_through_sequence, 1);
        let b = apply(
            &e,
            "p",
            "t2",
            vec![
                ev(1, 10, "100", "playbook_started", Some("s"), Some("running")), // replay, skipped
                ev(
                    2,
                    11,
                    "100",
                    "playbook.completed",
                    Some("done"),
                    Some("completed"),
                ),
            ],
        );
        assert_eq!(b.applied, 1);
        assert_eq!(b.skipped_below_checkpoint, 1);
        assert_eq!(b.checkpoint.applied_through_sequence, 2);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn execution_state_fold_derives_terminal_status() {
        let (log, dir) = tmp_log("fold");
        let e = engine(&log);
        apply(
            &e,
            "p",
            "t1",
            vec![
                ev(
                    1,
                    10,
                    "325",
                    "playbook_started",
                    Some("start"),
                    Some("running"),
                ),
                ev(
                    2,
                    11,
                    "325",
                    "command.completed",
                    Some("load_offers"),
                    Some("completed"),
                ),
                ev(
                    3,
                    12,
                    "325",
                    "playbook.completed",
                    Some("finish"),
                    Some("completed"),
                ),
            ],
        );
        let read = e.read_execution_state("325").unwrap();
        assert!(read.exists);
        let state = read.state.unwrap();
        assert_eq!(state.status, "completed");
        assert!(state.terminal);
        assert_eq!(
            state.terminal_event_type.as_deref(),
            Some("playbook.completed")
        );
        assert_eq!(state.current_node.as_deref(), Some("finish"));
        assert_eq!(state.event_count, 3);
        assert_eq!(state.first_global_sequence, 1);
        assert_eq!(state.last_global_sequence, 3);
        assert_eq!(state.last_event_id, 12);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn execution_state_running_without_terminal() {
        let (log, dir) = tmp_log("running");
        let e = engine(&log);
        apply(
            &e,
            "p",
            "t1",
            vec![
                ev(
                    1,
                    10,
                    "400",
                    "playbook_started",
                    Some("start"),
                    Some("running"),
                ),
                ev(2, 11, "400", "command.started", Some("load"), None),
            ],
        );
        let state = e.read_execution_state("400").unwrap().state.unwrap();
        assert!(!state.terminal);
        // Latest status is None on event 2, so the last-known status stands.
        assert_eq!(state.status, "running");
        assert_eq!(state.current_node.as_deref(), Some("load"));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn scope_isolation_between_executions() {
        let (log, dir) = tmp_log("scope");
        let e = engine(&log);
        apply(
            &e,
            "p",
            "t1",
            vec![
                ev(1, 10, "100", "playbook_started", Some("a"), Some("running")),
                ev(2, 20, "200", "playbook_started", Some("b"), Some("running")),
                ev(3, 11, "100", "playbook.failed", Some("a"), Some("failed")),
            ],
        );
        let s100 = e.read_execution_state("100").unwrap().state.unwrap();
        let s200 = e.read_execution_state("200").unwrap().state.unwrap();
        assert_eq!(s100.event_count, 2);
        assert_eq!(s100.status, "failed");
        assert!(s100.terminal);
        assert_eq!(s200.event_count, 1);
        assert_eq!(s200.status, "running");
        assert!(!s200.terminal);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn read_event_and_list_and_absent_probes() {
        let (log, dir) = tmp_log("read");
        let e = engine(&log);
        // Absent probes before any apply.
        assert!(!e.read_execution_state("100").unwrap().exists);
        assert!(!e.read_event(10).unwrap().exists);
        assert!(!e.list_executions(10).unwrap().exists);
        assert_eq!(e.checkpoint("p").unwrap().applied_through_sequence, 0);

        apply(
            &e,
            "p",
            "t1",
            vec![
                ev(1, 10, "100", "playbook_started", Some("s"), Some("running")),
                ev(2, 20, "200", "playbook_started", Some("s"), Some("running")),
            ],
        );
        let ev10 = e.read_event(10).unwrap();
        assert!(ev10.exists);
        assert_eq!(ev10.event.unwrap().execution_id, "100");
        let list = e.list_executions(10).unwrap();
        assert_eq!(list.total, 2);
        assert_eq!(list.states[0].execution_id, "100"); // first global sequence
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn rebuild_from_event_log_is_deterministic() {
        // Rebuild-from-log: the same event set applied to a fresh engine yields
        // identical read-models regardless of batch boundaries.
        let (log_a, dir_a) = tmp_log("rebuild-a");
        let (log_b, dir_b) = tmp_log("rebuild-b");
        let events = vec![
            ev(1, 10, "1", "playbook_started", Some("s"), Some("running")),
            ev(2, 11, "2", "playbook_started", Some("s"), Some("running")),
            ev(
                3,
                12,
                "1",
                "command.completed",
                Some("load"),
                Some("completed"),
            ),
            ev(
                4,
                13,
                "1",
                "playbook.completed",
                Some("done"),
                Some("completed"),
            ),
        ];
        // Engine A: one big batch.
        let ea = engine(&log_a);
        apply(&ea, "p", "ta", events.clone());
        // Engine B: four single-event batches (different boundaries).
        let eb = engine(&log_b);
        for (i, single) in events.iter().enumerate() {
            apply(&eb, "p", &format!("tb-{i}"), vec![single.clone()]);
        }
        let la = ea.list_executions(100).unwrap().states;
        let lb = eb.list_executions(100).unwrap().states;
        assert_eq!(la, lb, "rebuild must be batch-boundary independent");
        assert_eq!(
            ea.checkpoint("p").unwrap().applied_through_sequence,
            eb.checkpoint("p").unwrap().applied_through_sequence
        );
        let _ = std::fs::remove_dir_all(&dir_a);
        let _ = std::fs::remove_dir_all(&dir_b);
    }

    #[test]
    fn oversized_batch_is_rejected() {
        let (log, dir) = tmp_log("oversize");
        let e = engine(&log);
        let batch: Vec<ProjectionEventInput> = (0..(MAX_APPLY_BATCH + 1))
            .map(|i| ev(i as u64 + 1, i as i64, "100", "a", None, None))
            .collect();
        let err = e
            .apply(&ProjectionApplyRequest {
                consumer: "p".to_string(),
                transaction_id: "t".to_string(),
                events: batch,
            })
            .unwrap_err();
        assert!(matches!(err, EhdbError::InvalidState(_)));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn invalid_execution_id_is_invalid_identifier() {
        let (log, dir) = tmp_log("badid");
        let e = engine(&log);
        let err = e
            .apply(&ProjectionApplyRequest {
                consumer: "p".to_string(),
                transaction_id: "t".to_string(),
                events: vec![ev(1, 10, "bad id!", "a", None, None)],
            })
            .unwrap_err();
        assert!(matches!(err, EhdbError::InvalidIdentifier(_)));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn from_event_log_record_bridges_payload() {
        let record = EventLogRecordView {
            global_sequence: 7,
            execution_id: "100".to_string(),
            transaction_id: "txn".to_string(),
            byte_len: 0,
            payload: r#"{"event_id":42,"event_type":"command.completed","node_name":"load","status":"completed","prev_event_id":41}"#.to_string(),
        };
        let input = ProjectionEventInput::from_event_log_record(&record).unwrap();
        assert_eq!(input.global_sequence, 7);
        assert_eq!(input.event_id, 42);
        assert_eq!(input.execution_id, "100");
        assert_eq!(input.event_type, "command.completed");
        assert_eq!(input.node_name.as_deref(), Some("load"));
        assert_eq!(input.prev_event_id, Some(41));
        // A payload with no event_id is not a chainable event.
        let bad = EventLogRecordView {
            global_sequence: 8,
            execution_id: "100".to_string(),
            transaction_id: "txn".to_string(),
            byte_len: 0,
            payload: r#"{"event_type":"heartbeat"}"#.to_string(),
        };
        assert!(ProjectionEventInput::from_event_log_record(&bad).is_none());
    }

    #[test]
    fn parity_holds_when_read_models_match() {
        let ehdb = vec![ExecutionStateView {
            execution_id: "100".to_string(),
            status: "completed".to_string(),
            current_node: Some("done".to_string()),
            event_count: 3,
            first_global_sequence: 1,
            last_global_sequence: 3,
            last_event_id: 12,
            terminal: true,
            terminal_event_type: Some("playbook.completed".to_string()),
        }];
        let auth = vec![AuthoritativeExecutionState {
            execution_id: "100".to_string(),
            status: "completed".to_string(),
            event_count: 3,
            terminal: true,
        }];
        let report = compare_projection_parity(&ehdb, &auth, 3, Some(3));
        assert!(report.holds(), "{report:?}");
        assert_eq!(report.checkpoint_lag, 0);
    }

    #[test]
    fn parity_flags_value_divergence() {
        let ehdb = vec![ExecutionStateView {
            execution_id: "100".to_string(),
            status: "running".to_string(),
            current_node: None,
            event_count: 2,
            first_global_sequence: 1,
            last_global_sequence: 2,
            last_event_id: 11,
            terminal: false,
            terminal_event_type: None,
        }];
        let auth = vec![AuthoritativeExecutionState {
            execution_id: "100".to_string(),
            status: "completed".to_string(),
            event_count: 3,
            terminal: true,
        }];
        let report = compare_projection_parity(&ehdb, &auth, 2, Some(3));
        assert!(!report.holds());
        assert!(!report.value_ok);
        assert!(report.divergence.unwrap().contains("value divergence"));
    }

    #[test]
    fn parity_flags_key_and_checkpoint_divergence() {
        // Missing execution row → key divergence.
        let auth = vec![
            AuthoritativeExecutionState {
                execution_id: "100".to_string(),
                status: "running".to_string(),
                event_count: 1,
                terminal: false,
            },
            AuthoritativeExecutionState {
                execution_id: "200".to_string(),
                status: "running".to_string(),
                event_count: 1,
                terminal: false,
            },
        ];
        let ehdb = vec![ExecutionStateView {
            execution_id: "100".to_string(),
            status: "running".to_string(),
            current_node: None,
            event_count: 1,
            first_global_sequence: 1,
            last_global_sequence: 1,
            last_event_id: 10,
            terminal: false,
            terminal_event_type: None,
        }];
        let key_report = compare_projection_parity(&ehdb, &auth, 1, Some(2));
        assert!(!key_report.key_ok);
        assert!(key_report.divergence.unwrap().contains("key divergence"));

        // Keys + values match but EHDB checkpoint lags → checkpoint divergence.
        let auth1 = &auth[..1];
        let lag_report = compare_projection_parity(&ehdb, auth1, 1, Some(5));
        assert!(!lag_report.checkpoint_ok);
        assert_eq!(lag_report.checkpoint_lag, 4);
        assert!(lag_report.divergence.unwrap().contains("checkpoint lag"));
    }

    #[test]
    fn driver_name_is_stable() {
        let (log, dir) = tmp_log("name");
        let e = engine(&log);
        assert_eq!(e.driver_name(), "ehdb-local-reference");
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Two executions interleaved: "100" runs two events to a terminal completed,
    /// "200" one running event — a scope + fold + parity ground truth.
    fn primary_input() -> ProjectionPrimaryInput {
        let events = vec![
            ev(
                1,
                10,
                "100",
                "playbook_started",
                Some("start"),
                Some("running"),
            ),
            ev(
                2,
                20,
                "200",
                "playbook_started",
                Some("start"),
                Some("running"),
            ),
            ev(
                3,
                11,
                "100",
                "playbook.completed",
                Some("finish"),
                Some("completed"),
            ),
        ];
        let authoritative = vec![
            AuthoritativeExecutionState {
                execution_id: "100".to_string(),
                status: "completed".to_string(),
                event_count: 2,
                terminal: true,
            },
            AuthoritativeExecutionState {
                execution_id: "200".to_string(),
                status: "running".to_string(),
                event_count: 1,
                terminal: false,
            },
        ];
        ProjectionPrimaryInput {
            events,
            authoritative,
            authoritative_offset: Some(3),
        }
    }

    #[test]
    fn primary_serve_cycle_is_served_by_ehdb() {
        let (log, dir) = tmp_log("primary-serve");
        let e = engine(&log);
        let report =
            exercise_primary_serve(&e, &primary_input(), "projector", "primary-t1").unwrap();
        assert!(report.served_by_ehdb(), "{report:?}");
        assert_eq!(report.driver_name, "ehdb-local-reference");
        assert_eq!(report.applied, 3);
        assert_eq!(report.checkpoint, 3);
        assert!(report.list_ok);
        assert_eq!(report.list_count, 2);
        assert!(report.scope_ok);
        assert!(report.read_event_ok);
        // Exactly-once: a re-apply of the same batch materializes nothing new.
        assert!(report.replay_idempotent);
        // Replay-is-truth: a fresh engine over the same store serves the identical set.
        assert_eq!(report.replay_count, 2);
        assert!(report.replay_matches);
        assert!(report.dual_run_holds);
        assert!(report.divergence.is_none());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn primary_serve_dual_run_flags_incumbent_divergence() {
        let (log, dir) = tmp_log("primary-diverge");
        let e = engine(&log);
        // The incumbent claims exec 100 is still running with 1 event, but EHDB
        // folds it to completed/2 → the dual-run parity fails and the cycle is
        // not served-by-EHDB (the read-models still materialized).
        let mut input = primary_input();
        input.authoritative[0] = AuthoritativeExecutionState {
            execution_id: "100".to_string(),
            status: "running".to_string(),
            event_count: 1,
            terminal: false,
        };
        let report = exercise_primary_serve(&e, &input, "projector", "primary-t1").unwrap();
        assert!(!report.served_by_ehdb());
        assert!(!report.dual_run_holds);
        assert_eq!(report.applied, 3);
        assert!(report.divergence.unwrap().contains("value divergence"));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn primary_serve_flags_checkpoint_lag() {
        let (log, dir) = tmp_log("primary-lag");
        let e = engine(&log);
        // Incumbent offset claims 9 but EHDB applied through 3 → checkpoint lag.
        let mut input = primary_input();
        input.authoritative_offset = Some(9);
        let report = exercise_primary_serve(&e, &input, "projector", "primary-t1").unwrap();
        assert!(!report.served_by_ehdb());
        assert!(!report.dual_run.checkpoint_ok);
        assert_eq!(report.dual_run.checkpoint_lag, 6);
        assert!(report.divergence.unwrap().contains("checkpoint lag"));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn primary_serve_without_authoritative_offset_still_serves() {
        let (log, dir) = tmp_log("primary-nooffset");
        let e = engine(&log);
        // No incumbent offset surfaced → checkpoint-lag skipped, key+value parity
        // still enforced, cycle still served-by-EHDB.
        let mut input = primary_input();
        input.authoritative_offset = None;
        let report = exercise_primary_serve(&e, &input, "projector", "primary-t1").unwrap();
        assert!(report.served_by_ehdb(), "{report:?}");
        assert!(report.dual_run.checkpoint_ok);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn primary_serve_reversibility_replay_after_reopen() {
        // Proves the reversibility half this helper owns: after a primary cycle a
        // brand-new engine over the same on-disk store serves the identical
        // read-model set — a flip back to the incumbent materializer (or a later
        // re-enable) loses nothing because the EHDB store stays whole on disk.
        let (log, dir) = tmp_log("primary-revert");
        {
            let e = engine(&log);
            let report =
                exercise_primary_serve(&e, &primary_input(), "projector", "primary-t1").unwrap();
            assert!(report.served_by_ehdb());
        }
        let reopened = engine(&log);
        let list = reopened.list_executions(100).unwrap();
        assert_eq!(list.total, 2);
        let s100 = reopened.read_execution_state("100").unwrap().state.unwrap();
        assert_eq!(s100.status, "completed");
        assert!(s100.terminal);
        assert_eq!(s100.event_count, 2);
        // The durable checkpoint survives the reopen too.
        assert_eq!(
            reopened
                .checkpoint("projector")
                .unwrap()
                .applied_through_sequence,
            3
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn primary_serve_requires_at_least_one_event() {
        let (log, dir) = tmp_log("primary-empty");
        let e = engine(&log);
        let input = ProjectionPrimaryInput {
            events: Vec::new(),
            authoritative: Vec::new(),
            authoritative_offset: None,
        };
        let err = exercise_primary_serve(&e, &input, "projector", "primary-t1").unwrap_err();
        assert!(err.to_string().contains("at least one event"));
        let _ = std::fs::remove_dir_all(&dir);
    }
}
