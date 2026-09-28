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
//! ⚠ **The marker is written BEFORE the first event, not after.** Written after,
//! a crash mid-populate leaves events present with no marker — a reader falls
//! through and the population is invisible. Written before, a crash leaves a
//! marker with a short chain, which the chain's own gap detection already
//! reports honestly. The failure mode is chosen, not inherited.

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

    /// ⭐ **The guarded read.** `None` means *the store cannot answer*, which a
    /// caller must treat as "fall through", never as "no events".
    ///
    /// This is the function a chain source should call instead of
    /// `DurableChainStore::chain` directly — it is where the watermark does its
    /// job.
    pub fn chain_if_authoritative(&self, execution_id: &str) -> Result<Option<Vec<ChainEvent>>> {
        match self.authority(execution_id)? {
            Authority::NotPopulated => Ok(None),
            Authority::Authoritative { .. } => Ok(Some(self.store.chain(execution_id)?)),
        }
    }
}
