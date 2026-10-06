//! **The chain-store populator and its watermark.**
//!
//! ## The problem this exists to solve
//!
//! [`DurableChainStore`](crate::chain_store_durable::DurableChainStore) had
//! **zero callers** outside its own module and tests, across `ehdb`, `server`
//! and `worker`. An empty store is therefore indistinguishable from an
//! execution that has no events — and a reader that cannot tell them apart will
//! report a running execution as not-started.
//!
//! That is not a theoretical tidiness point. The advance decision downstream
//! reads an empty chain as `Empty`, which for a running execution is a
//! **decision made on an unpopulated store** — the same shape as returning
//! `Some(empty)` on a failed read, which a mutation battery already caught once
//! in this program.
//!
//! ## ⭐ The watermark, and why it is per-execution
//!
//! A **watermark** records the point from which the store is authoritative.
//! This one is per-execution and deliberately minimal:
//!
//! ```text
//! chain/<execution_id>/wm   ->  "<first_exec_seq>:<populated_through_seq>"
//! ```
//!
//! | marker | partition | meaning | a reader should |
//! | :-- | :-- | :-- | :-- |
//! | absent | — | **never populated** | **fall through** — the store knows nothing |
//! | present | empty | populated, genuinely no events | trust the empty answer |
//! | present | non-empty | populated | read the chain |
//!
//! A *global* watermark was considered and rejected: executions are populated
//! independently and out of order, so one global "authoritative from" point
//! would either exclude executions that ARE populated or include ones that are
//! not. The partition is already the unit of ownership; the watermark belongs on
//! it.
//!
//! ⚠⚠ **The marker ordering is ASYMMETRIC.** A never-seen execution is appended
//! FIRST and marked after; an already-authoritative one is marked FIRST and
//! appended after. The rule in one line: **never create authority out of a
//! failure.** The argument, and the defect that produced it, are in the comment
//! inside [`ChainPopulator::populate`]. (This paragraph asserted the uniform
//! "marker before the append" ordering until 2026-09-29, after the asymmetry had
//! already landed — the same doc drift the function-level guard now pins, one
//! level up.)
//!
//! ## ⭐⭐ The source of truth is the LOG, not the emit path
//!
//! [`ChainPopulator::populate_from_log`] is the entry point a reader should be
//! built on. It takes an execution's events **as the authoritative log ordered
//! them** and **recomputes the chain edges from that order**, rather than
//! trusting whatever `prev_event_id` the rows carry.
//!
//! That is not defensiveness, it is the measured reality: in the kind database
//! **643,420 of 645,677 rows carry `prev_event_id IS NULL`**, and **534 of 595
//! executions carry more than one null-prev root**, because the emit path stamps
//! the edge from an in-memory head map that does not survive a restart. A
//! populator that trusted the column would build a partition rooted wherever the
//! server last restarted.
//!
//! Recomputing from order also makes population **idempotent and restart-proof by
//! construction**: the stored chain must be a prefix of the log, and anything
//! beyond that prefix is appended. There is no in-memory state to lose.

use ehdb_core::{EhdbError, Result};

use crate::chain::{ChainError, ChainEvent, ExecSeq};
use crate::chain_store_durable::DurableChainStore;

/// Env var arming the populator. **Default off.**
pub const POPULATOR_ENV: &str = "NOETL_CHAIN_POPULATE";

/// Whether population is armed.
///
/// ⚠ Fail-safe: anything unrecognised is `false`. Arming a writer by typo is
/// worse than leaving it off.
pub fn populator_enabled() -> bool {
    matches!(
        std::env::var(POPULATOR_ENV)
            .unwrap_or_default()
            .trim()
            .to_ascii_lowercase()
            .as_str(),
        "1" | "true" | "yes" | "on"
    )
}

fn wm_key(execution_id: &str) -> String {
    format!("chain/{execution_id}/wm")
}

/// The **coverage** record: proof that this partition was built from the
/// authoritative log, and from that log's first event.
///
/// ```text
/// chain/<execution_id>/cov  ->  "<root_event_id>:<total_in_log>"
/// ```
///
/// The watermark answers *"was this populated?"*. It cannot answer *"was it
/// populated from the BEGINNING?"* — its `first_seq` is the **store's** sequence,
/// which is `1` for any fresh partition regardless of where the execution
/// actually started. Measured 2026-09-28: an execution with 3 events in the log
/// and 1 in the store reported `authoritative first=1 through=1`, and the guarded
/// read returned a confident, contiguous, gap-check-passing chain that began at
/// the execution's third event.
///
/// So coverage is recorded separately, and [`ChainPopulator::chain_if_authoritative`]
/// refuses any partition that lacks it. A partition written by the per-event
/// [`ChainPopulator::populate`] path has no coverage record and is therefore never
/// served — which is deliberate: that path cannot know whether it saw the
/// execution's first event.
fn cov_key(execution_id: &str) -> String {
    format!("chain/{execution_id}/cov")
}

/// One event as the authoritative log gives it, in log order.
///
/// ⚠ There is no `prev_event_id` field, and its absence is the point. The edge is
/// recomputed from the position in this slice; a column that is NULL on 99.65% of
/// prod-shaped rows is not an input worth having.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogEvent {
    pub event_id: String,
    pub parent_execution_id: Option<String>,
    pub payload: String,
    /// The chain link as the LOG records it: this event's predecessor.
    ///
    /// ⚠⚠ THIS FIELD WAS DELIBERATELY ABSENT UNTIL noetl/ai-meta#362, and the
    /// reason it is here now is the whole point of that issue.
    ///
    /// noetl/ai-meta#357 left it out on the evidence that it was NULL on 643,420
    /// of 645,677 rows, and rebuilt the order from the log's `ORDER BY event_id`
    /// sequence instead. That measurement was real but it was an AGGREGATE, and
    /// the aggregate hid the shape: a NULL prev is the *genesis* of an execution,
    /// so one per execution is correct by design. Measured PER EXECUTION, the
    /// linkage is populated on 61 of 63 recent executions — the 643k NULLs are one
    /// dead pre-feature era, not a live defect.
    ///
    /// And id-order is not a substitute, because `event_id` is a snowflake minted
    /// BEFORE the insert: commit order is not id order, so an event can commit
    /// into the MIDDLE of an id-ordered read and shift every position after it.
    /// That is the #362 divergence, measured on prod.
    ///
    /// `None` means "this is the root". Exactly one event per execution may say so.
    pub prev_event_id: Option<String>,
}

/// How a set of log events ordered by FOLLOWING ITS LINKS, per noetl/ai-meta#362.
///
/// Every non-`Ordered` variant is a condition the caller must REFUSE on, and each
/// one is distinct because they need different responses and different alarms.
/// Collapsing them into a bare `None` is how the previous two divergence classes
/// stayed unexplained for a whole session.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LinkOrder {
    /// Exactly one root, every event reached once, no fork, no cycle.
    /// Carries indices into the caller's slice, in chain order.
    Ordered(Vec<usize>),
    /// ⚠⚠ THE RE-ROOTING DEFECT. More than one event claims to be the genesis.
    ///
    /// This is what a lost in-memory head map produces: a mid-flight execution's
    /// next event is stamped `prev = NULL` and becomes a second false root. It is
    /// the defect the per-execution invariant measures, and the one thing that
    /// makes a link-defined chain unbuildable — there is no way to tell which root
    /// is the real one from the links alone.
    MultipleRoots { roots: Vec<String> },
    /// No event has a NULL prev: every event points at something. Either the true
    /// root is outside this set (a truncated read) or the links form a cycle.
    NoRoot,
    /// Two or more events claim the SAME predecessor.
    ///
    /// The model permits predecessors plural, and the measured data has fanout
    /// exactly 1 on all 2,262 edges — so rather than silently pick a branch, this
    /// reports. A fork that is later found legitimate can be built on from here;
    /// a fork silently resolved is a chain that quietly disagrees with the log.
    Fork { at: String, successors: Vec<String> },
    /// An event's `prev_event_id` names an id that is not in this set.
    Dangling { event: String, prev: String },
    /// The root was found and the walk terminated, but not every event was
    /// reached — so the set is not one chain.
    Unreachable { reached: usize, total: usize },
}

impl LinkOrder {
    pub fn label(&self) -> &'static str {
        match self {
            Self::Ordered(_) => "ordered",
            Self::MultipleRoots { .. } => "multiple_roots",
            Self::NoRoot => "no_root",
            Self::Fork { .. } => "fork",
            Self::Dangling { .. } => "dangling_prev",
            Self::Unreachable { .. } => "unreachable",
        }
    }
    /// Every label, so a metric can pin all values at 0 unconditionally — an
    /// unpinned label is an absent series and reads like a build without it.
    pub const ALL_LABELS: [&'static str; 6] = [
        "ordered",
        "multiple_roots",
        "no_root",
        "fork",
        "dangling_prev",
        "unreachable",
    ];
}

/// Order events by following `prev_event_id`, NOT by sorting on `event_id`.
///
/// ⭐⭐ This is the noetl/ai-meta#362 fix, and the property that matters is what it
/// does NOT do: it never compares two `event_id`s. A snowflake id is minted before
/// the insert, so an event with a lower id can commit later and land in the middle
/// of an id-ordered read — which shifted every subsequent position and read as a
/// content conflict. Under link traversal that same event is simply a link, and the
/// interior insert is a non-event.
///
/// Pure and allocation-light: one pass to index, one pass to walk.
pub fn order_by_links(events: &[LogEvent]) -> LinkOrder {
    use std::collections::HashMap;

    if events.is_empty() {
        return LinkOrder::Ordered(Vec::new());
    }

    // id -> index, and prev -> the events claiming it (to detect forks).
    let mut by_id: HashMap<&str, usize> = HashMap::with_capacity(events.len());
    for (i, e) in events.iter().enumerate() {
        by_id.insert(e.event_id.as_str(), i);
    }

    let mut roots: Vec<String> = Vec::new();
    let mut succ: HashMap<&str, Vec<usize>> = HashMap::with_capacity(events.len());
    for (i, e) in events.iter().enumerate() {
        match e.prev_event_id.as_deref() {
            None => roots.push(e.event_id.clone()),
            Some(prev) => {
                if !by_id.contains_key(prev) {
                    return LinkOrder::Dangling {
                        event: e.event_id.clone(),
                        prev: prev.to_string(),
                    };
                }
                succ.entry(prev).or_default().push(i);
            }
        }
    }

    // ⚠ Check the root count BEFORE walking. A walk from an arbitrary root would
    // succeed and return a plausible-looking prefix, which is exactly the kind of
    // confident wrong answer this module exists to avoid.
    match roots.len() {
        1 => {}
        0 => return LinkOrder::NoRoot,
        _ => {
            roots.sort();
            return LinkOrder::MultipleRoots { roots };
        }
    }

    for (prev, ss) in succ.iter() {
        if ss.len() > 1 {
            let mut names: Vec<String> = ss.iter().map(|&i| events[i].event_id.clone()).collect();
            names.sort();
            return LinkOrder::Fork {
                at: prev.to_string(),
                successors: names,
            };
        }
    }

    let root_idx = by_id[roots[0].as_str()];
    let mut order = Vec::with_capacity(events.len());
    let mut cur = root_idx;
    // The fork check above means each event has at most one successor, so the walk
    // is linear and bounded by `events.len()` — a cycle cannot spin forever, it
    // simply fails the reached-count check below.
    loop {
        order.push(cur);
        match succ.get(events[cur].event_id.as_str()) {
            Some(next) if order.len() <= events.len() => cur = next[0],
            _ => break,
        }
        if order.len() > events.len() {
            break;
        }
    }

    if order.len() != events.len() {
        return LinkOrder::Unreachable {
            reached: order.len(),
            total: events.len(),
        };
    }
    LinkOrder::Ordered(order)
}

/// What [`ChainPopulator::populate_from_log`] did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FromLog {
    /// The partition now matches the log exactly.
    InSync { total: usize, appended: usize },
    /// ⚠ The stored chain is **not a prefix** of the log, so the two disagree
    /// about history and the partition must not be served.
    ///
    /// Reported rather than repaired: silently rewriting an immutable chain to
    /// match a new reading is how a store stops being evidence. `at_position` is
    /// 0-based in log order.
    ///
    /// ⚠⚠ This means a **content conflict** — a position both sides hold, filled
    /// with different events. It does NOT mean "the caller's log snapshot was
    /// older than the store", which is [`FromLog::StaleLog`] and is benign.
    /// Conflating the two cost a prod ramp: see that variant.
    Diverged {
        at_position: usize,
        stored: String,
        log: String,
    },
    /// ⭐ The caller's log snapshot is **behind** the store: every event the
    /// snapshot holds matches, and the store simply holds more.
    ///
    /// # Why this is benign, and why it is not `Diverged`
    ///
    /// `populate_from_log` is the sole writer, so every stored event arrived from
    /// *some* read of this same log. A store that is longer than the caller's
    /// snapshot therefore means only that **a newer snapshot was already
    /// applied** — the extra events are real and committed. Nothing disagrees.
    ///
    /// It happens because the caller reads the log and *then* takes the store
    /// lock (the lock is a `std::sync::Mutex`, so it cannot be held across the
    /// database `await`). Under concurrent reads of one execution, the task that
    /// read first can reach the lock second and find the store already ahead.
    ///
    /// ⚠ Classing that as `Diverged` produced **5 false divergences in 79
    /// comparisons (6.3%)** on the prod ramp of 2026-09-29, which failed the
    /// comparator gate and forced a rollback — while nothing was actually wrong
    /// (60 partitions, 0 persistent mismatches; 24/24 executions completed).
    /// noetl/ai-meta#360.
    ///
    /// The partition stays **servable**: the stored chain is a superset of the
    /// caller's snapshot and is the fresher answer.
    StaleLog { stored_len: usize, log_len: usize },
}

impl FromLog {
    pub fn label(&self) -> &'static str {
        match self {
            Self::InSync { appended: 0, .. } => "in_sync",
            Self::InSync { .. } => "extended",
            Self::Diverged { .. } => "diverged",
            Self::StaleLog { .. } => "stale_log",
        }
    }

    /// Is this a healthy outcome? ⚠ `StaleLog` IS healthy — it is the caller
    /// being behind, not the store being wrong.
    pub fn is_healthy(&self) -> bool {
        !matches!(self, Self::Diverged { .. })
    }

    /// Every label, for pinning at 0 — absence is the default for a labelled
    /// series, so an unpinned family is indistinguishable from a build that
    /// predates it.
    pub const ALL_LABELS: [&'static str; 4] = ["in_sync", "extended", "diverged", "stale_log"];
}

/// What the store knows about an execution.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Authority {
    /// No marker: the store has never been told about this execution. A reader
    /// MUST fall through rather than trust an empty chain.
    NotPopulated,
    /// Marker present: the store is authoritative from `first_seq` through
    /// `through_seq`.
    Authoritative {
        first_seq: ExecSeq,
        through_seq: ExecSeq,
    },
}

impl Authority {
    /// Whether a reader may trust this store's answer for the execution —
    /// **including an empty one**.
    pub fn is_trustworthy(&self) -> bool {
        matches!(self, Self::Authoritative { .. })
    }

    pub fn label(&self) -> &'static str {
        match self {
            Self::NotPopulated => "not_populated",
            Self::Authoritative { .. } => "authoritative",
        }
    }

    /// Every label, so a metric can pin all values at 0 unconditionally.
    pub const ALL_LABELS: [&'static str; 2] = ["not_populated", "authoritative"];
}

/// Writes executions into the chain store and maintains their watermarks.
pub struct ChainPopulator<'a> {
    store: &'a DurableChainStore,
    substrate: &'a dyn crate::substrate::DurableSubstrate,
}

impl<'a> ChainPopulator<'a> {
    pub fn new(
        store: &'a DurableChainStore,
        substrate: &'a dyn crate::substrate::DurableSubstrate,
    ) -> Self {
        Self { store, substrate }
    }

    /// Read an execution's watermark.
    pub fn authority(&self, execution_id: &str) -> Result<Authority> {
        match self.substrate.get_all(&wm_key(execution_id)) {
            Ok(bytes) => {
                let raw = String::from_utf8_lossy(&bytes).to_string();
                let (a, b) = raw
                    .split_once(':')
                    .ok_or_else(|| EhdbError::Storage(format!("malformed watermark: {raw:?}")))?;
                Ok(Authority::Authoritative {
                    first_seq: a
                        .parse()
                        .map_err(|e| EhdbError::Storage(format!("watermark first_seq: {e}")))?,
                    through_seq: b
                        .parse()
                        .map_err(|e| EhdbError::Storage(format!("watermark through_seq: {e}")))?,
                })
            }
            Err(_) => Ok(Authority::NotPopulated),
        }
    }

    fn write_watermark(&self, execution_id: &str, first: ExecSeq, through: ExecSeq) -> Result<()> {
        self.substrate.put_overwrite(
            &wm_key(execution_id),
            format!("{first}:{through}").as_bytes(),
        )
    }

    /// **Populate one event.**
    ///
    /// ⭐ The watermark ordering is **asymmetric**, and deliberately so: an
    /// execution the store has never seen is appended FIRST and marked after,
    /// while one that is already authoritative is marked FIRST and appended
    /// after. The rule in one line: **never create authority out of a failure.**
    /// The long-form argument — and the defect that produced it — is in the
    /// comment on the ordering inside the body; read that before changing either
    /// branch.
    pub fn populate(
        &self,
        execution_id: &str,
        event_id: &str,
        prev_event_id: Option<&str>,
        parent_execution_id: Option<&str>,
        payload: &str,
    ) -> std::result::Result<ExecSeq, ChainError> {
        let before = self
            .authority(execution_id)
            .map_err(|e| ChainError::Invalid(e.to_string()))?;

        let next = self
            .store
            .head_entry(execution_id)
            .map_err(|e| ChainError::Invalid(e.to_string()))?
            .map(|(s, _)| s)
            .unwrap_or(0)
            + 1;

        // ⚠⚠ THE ORDERING IS NOT UNIFORM, and the asymmetry is the fix for a
        // real defect found while wiring this to the emit chokepoint.
        //
        // The first draft claimed the marker before EVERY append. On an
        // execution the store has never seen, an append that then FAILS leaves a
        // marker over zero events — `Authoritative { 1, 1 }` with an empty
        // partition — so `chain_if_authoritative` returns `Some(vec![])` and a
        // reader concludes a RUNNING execution has no events. That is exactly
        // the cliff the watermark exists to prevent, reintroduced through the
        // failure path.
        //
        // And it is the COMMON case, not an edge one: arming the populator
        // mid-flight means the first row seen for an already-running execution
        // carries a `prev` the empty store does not have, so `append` fails with
        // `NotHead` on essentially every in-flight execution.
        //
        // So:
        //   NotPopulated  -> append FIRST, then mark. A crash between them
        //                    leaves events with no marker, which reads as
        //                    NotPopulated — invisible, but SAFE (falls through).
        //                    Nothing is lost, because Postgres is still
        //                    authoritative.
        //   Authoritative -> mark FIRST, then append. There is already content,
        //                    so a crash must not make the store under-report it;
        //                    a marker over a short chain is reported honestly by
        //                    the chain's own gap detection.
        //
        // The rule in one line: **never create authority out of a failure.**
        let already_authoritative = matches!(before, Authority::Authoritative { .. });
        let first = match before {
            Authority::Authoritative { first_seq, .. } => first_seq,
            Authority::NotPopulated => next,
        };

        if already_authoritative {
            self.write_watermark(execution_id, first, next)
                .map_err(|e| ChainError::Invalid(e.to_string()))?;
        }

        let seq = self.store.append(
            execution_id,
            event_id,
            prev_event_id,
            parent_execution_id,
            payload,
        )?;

        if !already_authoritative {
            // The append succeeded, so authority is now justified by content.
            self.write_watermark(execution_id, first, seq)
                .map_err(|e| ChainError::Invalid(e.to_string()))?;
        }

        // The append assigns its own sequence; reconcile if they disagree
        // (a concurrent writer, which single-writer ownership forbids — so this
        // is a correction, not an expected path).
        if seq != next {
            self.write_watermark(execution_id, first, seq)
                .map_err(|e| ChainError::Invalid(e.to_string()))?;
        }
        Ok(seq)
    }

    /// Populate a replicated event (follower path — does not enforce the head).
    pub fn populate_replicated(&self, event: ChainEvent) -> std::result::Result<(), ChainError> {
        let exec = event.execution_id.clone();
        let seq = event.exec_seq;
        let before = self
            .authority(&exec)
            .map_err(|e| ChainError::Invalid(e.to_string()))?;
        let (first, through) = match before {
            Authority::Authoritative {
                first_seq,
                through_seq,
            } => (first_seq.min(seq), through_seq.max(seq)),
            Authority::NotPopulated => (seq, seq),
        };
        let already_authoritative = matches!(before, Authority::Authoritative { .. });

        // Same asymmetry, same reason — see `populate`.
        if already_authoritative {
            self.write_watermark(&exec, first, through)
                .map_err(|e| ChainError::Invalid(e.to_string()))?;
        }
        self.store.apply_replicated(event)?;
        if !already_authoritative {
            self.write_watermark(&exec, first, through)
                .map_err(|e| ChainError::Invalid(e.to_string()))?;
        }
        Ok(())
    }

    /// Read the coverage record: `(root_event_id, total_in_log)`.
    pub fn coverage(&self, execution_id: &str) -> Result<Option<(String, usize)>> {
        match self.substrate.get_all(&cov_key(execution_id)) {
            Ok(bytes) => {
                let raw = String::from_utf8_lossy(&bytes).to_string();
                let (root, total) = raw
                    .rsplit_once(':')
                    .ok_or_else(|| EhdbError::Storage(format!("malformed coverage: {raw:?}")))?;
                let total = total
                    .parse()
                    .map_err(|e| EhdbError::Storage(format!("coverage total: {e}")))?;
                Ok(Some((root.to_string(), total)))
            }
            Err(_) => Ok(None),
        }
    }

    /// Write the coverage record. ⚠⚠ **MONOTONIC — coverage never shrinks.**
    ///
    /// A caller whose log snapshot is behind would otherwise record a SMALLER
    /// total than the partition holds, and `chain_if_authoritative` would then
    /// refuse on `chain.len() != cov_total`. That turns a benign stale read into
    /// an unservable partition — the same false alarm as
    /// [`FromLog::StaleLog`], one layer down.
    ///
    /// Coverage describes how much of the execution the store has seen. That
    /// quantity only grows, so a smaller value is always the older observation
    /// and is dropped.
    fn write_coverage(&self, execution_id: &str, root: &str, total: usize) -> Result<()> {
        if let Some((existing_root, existing_total)) = self.coverage(execution_id)? {
            if existing_total > total && existing_root == root {
                // An older observation. Keep what we have.
                return Ok(());
            }
        }
        self.substrate
            .put_overwrite(&cov_key(execution_id), format!("{root}:{total}").as_bytes())
    }

    /// ⭐⭐ **Populate from the authoritative log.** This is the entry point a
    /// reader should be built on.
    ///
    /// `events` is the execution's events **in the order the authoritative log
    /// gives them** (for `noetl.event`, ascending `event_id` — a per-execution
    /// monotonic snowflake). The chain edge is **recomputed from that order**: the
    /// first event is the root and each subsequent event links to its predecessor.
    ///
    /// ⚠⚠ The log's own `prev_event_id` column is deliberately not an input. It is
    /// NULL on 643,420 of 645,677 kind rows, and 534 of 595 executions carry more
    /// than one null-prev root, because the emit path stamps the edge from an
    /// in-memory head map that does not survive a restart. Trusting the column
    /// would root the partition wherever the server last happened to restart.
    ///
    /// Idempotent and incremental: the stored chain must be a **prefix** of
    /// `events`, and only the remainder is appended. Re-running with a longer log
    /// extends the partition; re-running with the same log appends nothing.
    ///
    /// A disagreement with the stored prefix returns [`FromLog::Diverged`] and
    /// writes nothing — an immutable chain is not silently rewritten to match a
    /// new reading.
    pub fn populate_from_log(&self, execution_id: &str, events: &[LogEvent]) -> Result<FromLog> {
        // An execution the log genuinely has nothing for: mark it covered so the
        // empty answer is trusted rather than mistaken for "never populated".
        //
        // ⚠ The CALLER must not pass an empty slice for a failed read. A read
        // failure is "cannot answer" and has to stay distinguishable from "the log
        // has no events"; collapsing the two here is the exact bug this module
        // exists to prevent, moved up one level.
        if events.is_empty() {
            self.write_watermark(execution_id, 0, 0)?;
            self.write_coverage(execution_id, "", 0)?;
            return Ok(FromLog::InSync {
                total: 0,
                appended: 0,
            });
        }

        let stored = self.store.chain(execution_id)?;

        // The stored chain must be a prefix of the log, position for position.
        for (i, have) in stored.iter().enumerate() {
            match events.get(i) {
                Some(want) if want.event_id == have.event_id => {}
                Some(want) => {
                    // ⚠⚠ REVOKE SERVE-TRUST. A content conflict means the stored
                    // chain disagrees with the authoritative log at a position
                    // both hold — and Postgres is authoritative, so the STORE is
                    // the wrong one. Dropping the coverage record makes
                    // `chain_if_authoritative` refuse until a clean repopulate.
                    //
                    // Without this the partition stayed servable after a detected
                    // conflict: this function reports rather than repairs, so it
                    // wrote nothing, and the previously-healthy coverage still
                    // matched. The CALLING source refuses on `Diverged`, but any
                    // other reader calling `chain_if_authoritative` alone would be
                    // handed the chain we just proved wrong. Found by the test
                    // `a360_a_real_content_conflict_is_still_diverged`.
                    //
                    // ⚠ The EVENTS and the watermark are left intact — this
                    // revokes trust, it does not destroy evidence. A conflict is
                    // something to investigate, and deleting the diverging chain
                    // would remove the only copy of what disagreed.
                    self.substrate.delete(&cov_key(execution_id)).ok();
                    return Ok(FromLog::Diverged {
                        at_position: i,
                        stored: have.event_id.clone(),
                        log: want.event_id.clone(),
                    });
                }
                // ⭐ The store holds MORE than the caller's snapshot, and
                // everything the snapshot holds matched. That is the CALLER being
                // behind, not a disagreement — see `FromLog::StaleLog`. Returning
                // `Diverged` here is what produced 6.3% false divergence on the
                // prod ramp (noetl/ai-meta#360).
                //
                // ⚠ Return WITHOUT writing coverage: a stale snapshot would record
                // a SMALLER total than the partition holds, and the guarded read
                // would then refuse on `chain.len() != cov_total`. Fixing the
                // classification without this would only convert a false
                // divergence into a false refusal.
                None => {
                    return Ok(FromLog::StaleLog {
                        stored_len: stored.len(),
                        log_len: events.len(),
                    })
                }
            }
        }

        // Coverage is claimed BEFORE extending, for the same reason the watermark
        // is on the already-authoritative path: the root is already decided by the
        // log's first event, and a crash mid-extend must not leave a partition that
        // under-reports what it holds. It is not creating authority out of a
        // failure — the root it records is read from the log, not from a write that
        // might not have happened.
        self.write_coverage(execution_id, &events[0].event_id, events.len())?;

        let mut appended = 0usize;
        for (i, ev) in events.iter().enumerate().skip(stored.len()) {
            let prev = if i == 0 {
                None
            } else {
                Some(events[i - 1].event_id.as_str())
            };
            self.store
                .append(
                    execution_id,
                    &ev.event_id,
                    prev,
                    ev.parent_execution_id.as_deref(),
                    &ev.payload,
                )
                .map_err(|e| EhdbError::Storage(e.to_string()))?;
            appended += 1;
        }

        // The watermark last, and only once the content is actually there, so it
        // never claims more than the partition holds.
        self.write_watermark(execution_id, 1, events.len() as ExecSeq)?;

        Ok(FromLog::InSync {
            total: events.len(),
            appended,
        })
    }

    /// ⭐ **The guarded read.** `None` means *the store cannot answer*, which a
    /// caller must treat as "fall through", never as "no events".
    ///
    /// This is the function a chain source should call instead of
    /// `DurableChainStore::chain` directly — it is where the watermark does its
    /// job.
    pub fn chain_if_authoritative(&self, execution_id: &str) -> Result<Option<Vec<ChainEvent>>> {
        let Authority::Authoritative { through_seq, .. } = self.authority(execution_id)? else {
            return Ok(None);
        };

        // ⚠⚠ THE WATERMARK IS ALSO A TRIPWIRE, not only a "populated" flag.
        //
        // On the already-authoritative path `populate` widens the watermark
        // BEFORE the append. That is deliberate (a crash between the two must
        // not under-report existing content), and it means a *failed* append
        // leaves `through_seq` one ahead of what is stored. Measured in kind
        // 2026-09-28: Postgres 7 events, store 6, watermark `1:7`.
        //
        // That gap is the only evidence the store has that the log moved on
        // without it, so it must be read rather than ignored. It happens for a
        // mundane and recurring reason: the chain edge is stamped from the
        // server's IN-MEMORY head map, which does not survive a restart, so the
        // next event for a still-running execution carries `prev = NULL`, the
        // store rejects it as not-the-head, and the partition then stops growing
        // while the execution continues.
        //
        // Returning the stored prefix there would hand a caller a chain that is
        // silently stale — authoritative-looking, contiguous, complete by its own
        // gap check, and missing every event after the restart. `None` sends the
        // caller to the authoritative log instead, which is the safe direction
        // and the whole reason this function exists.
        let stored_head = self
            .store
            .head_entry(execution_id)?
            .map(|(s, _)| s)
            .unwrap_or(0);
        if stored_head != through_seq {
            return Ok(None);
        }

        // ⭐⭐ COVERAGE: was this partition built from the log, and from the log's
        // FIRST event?
        //
        // The watermark cannot answer that. Its `first_seq` is the STORE's
        // sequence, which is 1 for any fresh partition no matter where the
        // execution actually began. Measured 2026-09-28: an execution with 3 events
        // in the log and 1 in the store read `authoritative first=1 through=1`, and
        // this function returned a contiguous, gap-check-passing chain that started
        // at the execution's third event — a reader seeing a running execution
        // whose history begins wherever the populator happened to be armed.
        //
        // Only `populate_from_log` writes the coverage record, so a partition built
        // by the per-event path is never served. That is deliberate: the per-event
        // path cannot know whether it saw the execution's first event.
        let Some((cov_root, cov_total)) = self.coverage(execution_id)? else {
            return Ok(None);
        };

        let chain = self.store.chain(execution_id)?;

        // The recorded root must still be the root the partition holds, and the
        // recorded total must match what is stored. Either disagreement means the
        // partition is not the one the coverage describes.
        if chain.len() != cov_total {
            return Ok(None);
        }
        match chain.first() {
            None => {
                if !cov_root.is_empty() {
                    return Ok(None);
                }
            }
            Some(first) if first.event_id == cov_root => {}
            Some(_) => return Ok(None),
        }

        Ok(Some(chain))
    }
}
