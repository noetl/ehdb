//! The epoch fence — a stale writer's durable mutations, refused.
//!
//! ## Why this exists rather than reusing `ehdb-reference`'s fence
//!
//! `ehdb_reference::fencing::FencedSharedBackend` decorates
//! `SharedSegmentBackend`, which is reached only through the
//! `durable_segment` event-log backend. Measured on the prod cmdbus-writer
//! (noetl/ai-meta#460): `NOETL_EHDB_EVENTLOG_BACKEND` is **unset**, and
//! `command_bus.rs` / `event_bus.rs` contain **zero** references to that trait.
//! They write through `L0Engine` over `LocalFsSubstrate`.
//!
//! So that fence was armed on a path the writer does not use. With 624 real L0
//! appends and 290,440 commits through the live writer, `writes_checked` was
//! **0** — a fence that would have reported `active 1` and refused nothing.
//! ⚠⚠ That is the M5 lesson one level up: M5 was *"the guard is on
//! `append_segment` and the publish path skips it"*; this was *"the guard is on
//! a backend this deployment does not use"*.
//!
//! `ehdb-reference` depends on neither `ehdb-l0` nor `ehdb-feed`, so its fence
//! is not reachable from the engine. The primitive lives here, in the crate both
//! sides already depend on.
//!
//! ## ⭐ Coverage is a type-system property, not a grep
//!
//! Every durable mutation on `L0Engine` requires `&mut`, and `ehdb-feed`'s
//! `FencedEngine` is the only owner of the engine — its `write()` is the only
//! way to obtain `&mut L0Engine`, and it cannot return without consulting this
//! fence. So the enumeration of fenced paths is *"all twelve `&mut self`
//! methods"* as established by the borrow checker.
//!
//! ⚠ **Be precise about what that does and does not guarantee.** The compiler
//! guarantees a fence check happened before any mutation. It does **not**
//! guarantee the [`MutationPath`] label a caller passes is the right one — the
//! label makes coverage *observable*, which is a weaker claim than enforcing it.
//! Conflating the two would be exactly the overclaim this whole programme keeps
//! finding, so the per-path numbers are evidence for a human reading a scrape,
//! not a proof.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use crate::{EhdbError, Result};

/// What the fence does with a stale write.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum FenceMode {
    /// No ledger, no checks, no counters. Byte-identical to not having a fence.
    #[default]
    Off,
    /// Check and count, but let the write through. The only safe way to learn
    /// whether the fence is on the path at all before it can refuse anything.
    Shadow,
    /// Refuse a stale write.
    Enforce,
}

impl FenceMode {
    pub fn from_str_or_off(s: &str) -> Self {
        match s.trim().to_ascii_lowercase().as_str() {
            "shadow" => Self::Shadow,
            "enforce" => Self::Enforce,
            _ => Self::Off,
        }
    }
    pub fn is_enforcing(self) -> bool {
        matches!(self, Self::Enforce)
    }
    pub fn is_active(self) -> bool {
        !matches!(self, Self::Off)
    }
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Off => "off",
            Self::Shadow => "shadow",
            Self::Enforce => "enforce",
        }
    }
}

/// The durable-mutation paths: one per `&mut self` method on `L0Engine`, plus
/// `PersistCursor`, which is durable writer state that is not on the engine.
///
/// ⚠ This list is a **label set**, not the coverage guarantee. Coverage comes
/// from `&mut` being unobtainable without a fence check. What this buys is a
/// per-path counter, so an operator can see that `writes_checked` climbing is
/// appends *and* seals *and* retention — rather than appends alone while the
/// rest stay unfenced, which is the partial-coverage failure that makes a fence
/// read complete when it is not.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MutationPath {
    AppendRecord,
    AppendRecordReporting,
    AppendWriterAssigned,
    AppendWriterAssignedReporting,
    TakeSyncHandles,
    SetFlushPolicy,
    FlushAndWaitUploads,
    SealAgedParts,
    RunPendingMerges,
    ReclaimOrphans,
    ApplyRetention,
    ApplyRetentionKeepLast,
    /// Persisting a claim group's committed cursor (`CursorStore::store`).
    ///
    /// ⚠ Not an L0-engine path — the only one here that is not. It is included
    /// because the cursor file is durable state on the writer's own volume, in
    /// the same durability domain as the log, and a superseded writer can rewind
    /// it. `CursorStore` guards `store` with a per-process `AtomicU64`
    /// high-water mark that starts at 0 and is never seeded from the file, so its
    /// documented invariant — "a late/duplicate call can never rewind durable
    /// progress" — holds only within one process's lifetime. A second instance
    /// begins at 0 and will happily write a lower cursor over a higher one.
    ///
    /// Rewinding a committed cursor re-delivers commands that were already
    /// acked, which is the failure the fence exists to prevent.
    PersistCursor,
}

impl MutationPath {
    /// Every path, in a fixed order. ⭐ The denominator the kind validation
    /// prints: a coverage claim without the population it measured is not a
    /// finding.
    pub const ALL: [MutationPath; 13] = [
        Self::AppendRecord,
        Self::AppendRecordReporting,
        Self::AppendWriterAssigned,
        Self::AppendWriterAssignedReporting,
        Self::TakeSyncHandles,
        Self::SetFlushPolicy,
        Self::FlushAndWaitUploads,
        Self::SealAgedParts,
        Self::RunPendingMerges,
        Self::ReclaimOrphans,
        Self::ApplyRetention,
        Self::ApplyRetentionKeepLast,
        Self::PersistCursor,
    ];

    /// The subset that is an `L0Engine` `&mut self` method.
    ///
    /// ⚠ Kept separate from [`ALL`](Self::ALL) because the engine-coverage gate
    /// exercises each path through `FencedEngine::write`, and a path that is not
    /// an engine method cannot be reached that way. Splitting the population is
    /// the honest fix; widening the gate to "iterate ALL and skip what does not
    /// fit" would make it pass for a path nothing covers.
    ///
    /// `ALL.len() == ENGINE.len() + 1` is asserted in the coverage test, so
    /// adding a path forces an explicit decision about which set it joins.
    pub const ENGINE: [MutationPath; 12] = [
        Self::AppendRecord,
        Self::AppendRecordReporting,
        Self::AppendWriterAssigned,
        Self::AppendWriterAssignedReporting,
        Self::TakeSyncHandles,
        Self::SetFlushPolicy,
        Self::FlushAndWaitUploads,
        Self::SealAgedParts,
        Self::RunPendingMerges,
        Self::ReclaimOrphans,
        Self::ApplyRetention,
        Self::ApplyRetentionKeepLast,
    ];

    pub fn index(self) -> usize {
        Self::ALL.iter().position(|p| *p == self).unwrap_or(0)
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::AppendRecord => "append_record",
            Self::AppendRecordReporting => "append_record_reporting",
            Self::AppendWriterAssigned => "append_writer_assigned",
            Self::AppendWriterAssignedReporting => "append_writer_assigned_reporting",
            Self::TakeSyncHandles => "take_sync_handles",
            Self::SetFlushPolicy => "set_flush_policy",
            Self::FlushAndWaitUploads => "flush_and_wait_uploads",
            Self::SealAgedParts => "seal_aged_parts",
            Self::RunPendingMerges => "run_pending_merges",
            Self::ReclaimOrphans => "reclaim_orphans",
            Self::ApplyRetention => "apply_retention",
            Self::ApplyRetentionKeepLast => "apply_retention_keep_last",
            Self::PersistCursor => "persist_cursor",
        }
    }
}

/// What a check concluded.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FenceDecision {
    /// This writer's epoch is at least the highest the store has accepted.
    Fresh { epoch: u64, advanced: bool },
    /// ⚠ A higher epoch has already written. This writer has been superseded.
    Stale { observed: u64, highest: u64 },
}

/// Counters, including per-path.
#[derive(Debug, Default)]
pub struct FenceMetrics {
    pub writes_checked: AtomicU64,
    pub stale_observed: AtomicU64,
    pub stale_refused: AtomicU64,
    pub epoch_advances: AtomicU64,
    checked_by_path: [AtomicU64; 13],
    refused_by_path: [AtomicU64; 13],
}

impl FenceMetrics {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    pub fn checked_for(&self, path: MutationPath) -> u64 {
        self.checked_by_path[path.index()].load(Ordering::Relaxed)
    }

    pub fn refused_for(&self, path: MutationPath) -> u64 {
        self.refused_by_path[path.index()].load(Ordering::Relaxed)
    }

    /// ⚠ Every path label is pinned at 0, unconditionally. A labelled metric is
    /// absent from a Prometheus scrape until something increments it, so an
    /// unpinned `path="seal_aged_parts"` would be missing both when seals are
    /// fenced-and-idle and when this build does not fence them — two states an
    /// operator has to tell apart.
    pub fn render_prometheus(&self, mode: FenceMode) -> String {
        let mut out = String::new();
        out.push_str("# HELP ehdb_engine_fence_mode Fence mode on the engine chokepoint: 0 off, 1 shadow, 2 enforce.\n");
        out.push_str("# TYPE ehdb_engine_fence_mode gauge\n");
        out.push_str(&format!(
            "ehdb_engine_fence_mode {}\n",
            match mode {
                FenceMode::Off => 0,
                FenceMode::Shadow => 1,
                FenceMode::Enforce => 2,
            }
        ));
        for (name, v) in [
            (
                "writes_checked",
                self.writes_checked.load(Ordering::Relaxed),
            ),
            (
                "stale_observed",
                self.stale_observed.load(Ordering::Relaxed),
            ),
            ("stale_refused", self.stale_refused.load(Ordering::Relaxed)),
            (
                "epoch_advances",
                self.epoch_advances.load(Ordering::Relaxed),
            ),
        ] {
            out.push_str(&format!(
                "# HELP ehdb_engine_fence_{name}_total Engine-chokepoint fence: {name}.\n"
            ));
            out.push_str(&format!("# TYPE ehdb_engine_fence_{name}_total counter\n"));
            out.push_str(&format!("ehdb_engine_fence_{name}_total {v}\n"));
        }
        out.push_str("# HELP ehdb_engine_fence_checked_total Fence checks by durable-mutation path. ⚠ Read these TOGETHER: a climbing total with only `append_*` moving means seal and retention are unfenced.\n");
        out.push_str("# TYPE ehdb_engine_fence_checked_total counter\n");
        for p in MutationPath::ALL {
            out.push_str(&format!(
                "ehdb_engine_fence_checked_total{{path=\"{}\"}} {}\n",
                p.as_str(),
                self.checked_for(p)
            ));
        }
        out.push_str(
            "# HELP ehdb_engine_fence_refused_total Writes refused by path (enforce only).\n",
        );
        out.push_str("# TYPE ehdb_engine_fence_refused_total counter\n");
        for p in MutationPath::ALL {
            out.push_str(&format!(
                "ehdb_engine_fence_refused_total{{path=\"{}\"}} {}\n",
                p.as_str(),
                self.refused_for(p)
            ));
        }
        out
    }
}

/// The monotonic high-water epoch per shard, on disk beside the log.
///
/// ⚠⚠ There is deliberately **no delete**. The marker is the fencing token's
/// high-water mark; removing it lets a superseded epoch be accepted as fresh,
/// which is strictly worse than having no fence — the same reason the
/// Kubernetes election Role carries no `delete` verb on Leases.
#[derive(Debug, Clone)]
pub struct FenceLedger {
    root: PathBuf,
}

impl FenceLedger {
    pub fn new(root: impl Into<PathBuf>) -> Result<Self> {
        let root = root.into();
        std::fs::create_dir_all(&root)
            .map_err(|e| EhdbError::Storage(format!("fence ledger {}: {e}", root.display())))?;
        Ok(Self { root })
    }

    fn marker(&self, shard: u32) -> PathBuf {
        self.root.join(format!("shard-{shard:08x}.epoch"))
    }

    pub fn highest_epoch(&self, shard: u32) -> Result<u64> {
        let path = self.marker(shard);
        match std::fs::read_to_string(&path) {
            Ok(s) => s.trim().parse::<u64>().map_err(|_| {
                // ⚠ Loud, not 0. A corrupt marker read as 0 would make every
                // epoch look fresh and the fence would silently stop fencing.
                EhdbError::Storage(format!(
                    "fence marker {} is not an integer: {:?}",
                    path.display(),
                    s.chars().take(32).collect::<String>()
                ))
            }),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(0),
            Err(e) => Err(EhdbError::Storage(format!(
                "read fence marker {}: {e}",
                path.display()
            ))),
        }
    }

    /// Compare `epoch` against the high-water mark, advancing it when higher.
    pub fn check_and_advance(&self, shard: u32, epoch: u64) -> Result<FenceDecision> {
        let highest = self.highest_epoch(shard)?;
        if epoch < highest {
            return Ok(FenceDecision::Stale {
                observed: epoch,
                highest,
            });
        }
        let advanced = epoch > highest;
        if advanced {
            self.write_marker(shard, epoch)?;
        }
        Ok(FenceDecision::Fresh { epoch, advanced })
    }

    fn write_marker(&self, shard: u32, epoch: u64) -> Result<()> {
        let path = self.marker(shard);
        let tmp = path.with_extension("epoch.tmp");
        std::fs::write(&tmp, epoch.to_string())
            .map_err(|e| EhdbError::Storage(format!("write fence marker: {e}")))?;
        std::fs::rename(&tmp, &path)
            .map_err(|e| EhdbError::Storage(format!("commit fence marker: {e}")))?;
        Ok(())
    }

    pub fn root(&self) -> &Path {
        &self.root
    }
}

/// The fence itself: a mode, a token, a ledger and counters.
#[derive(Debug)]
pub struct EpochFence {
    mode: FenceMode,
    /// The token this writer currently holds. `0` means **no token**, and is
    /// never a legitimate epoch — the election mints from 1.
    epoch: AtomicU64,
    ledger: Option<FenceLedger>,
    metrics: Arc<FenceMetrics>,
}

impl EpochFence {
    /// An inert fence. No ledger, no checks, no counters.
    pub fn off() -> Arc<Self> {
        Arc::new(Self {
            mode: FenceMode::Off,
            epoch: AtomicU64::new(0),
            ledger: None,
            metrics: FenceMetrics::new(),
        })
    }

    pub fn new(
        mode: FenceMode,
        ledger: Option<FenceLedger>,
        metrics: Arc<FenceMetrics>,
    ) -> Arc<Self> {
        Arc::new(Self {
            mode,
            epoch: AtomicU64::new(0),
            ledger,
            metrics,
        })
    }

    pub fn mode(&self) -> FenceMode {
        self.mode
    }

    pub fn metrics(&self) -> &Arc<FenceMetrics> {
        &self.metrics
    }

    /// Install the token the election minted.
    ///
    /// ⚠ Pass `0` when the token can no longer be defended — a holder whose
    /// lease contact is older than the lease duration must stop asserting it.
    /// `0` is refused under enforce once any real epoch has advanced the marker,
    /// and is inert against an empty ledger.
    pub fn set_epoch(&self, epoch: u64) {
        self.epoch.store(epoch, Ordering::SeqCst);
    }

    pub fn epoch(&self) -> u64 {
        self.epoch.load(Ordering::SeqCst)
    }

    /// The check every durable mutation runs. `Err` only when enforcing a stale
    /// write.
    pub fn check(&self, shard: u32, path: MutationPath) -> Result<FenceDecision> {
        let Some(ledger) = self.ledger.as_ref() else {
            // Off: no ledger, so nothing to compare against and nothing counted.
            return Ok(FenceDecision::Fresh {
                epoch: 0,
                advanced: false,
            });
        };
        let epoch = self.epoch();
        let decision = ledger.check_and_advance(shard, epoch)?;
        self.metrics.writes_checked.fetch_add(1, Ordering::Relaxed);
        self.metrics.checked_by_path[path.index()].fetch_add(1, Ordering::Relaxed);
        match decision {
            FenceDecision::Fresh { advanced, .. } => {
                if advanced {
                    self.metrics.epoch_advances.fetch_add(1, Ordering::Relaxed);
                }
                Ok(decision)
            }
            FenceDecision::Stale { observed, highest } => {
                self.metrics.stale_observed.fetch_add(1, Ordering::Relaxed);
                if self.mode.is_enforcing() {
                    self.metrics.stale_refused.fetch_add(1, Ordering::Relaxed);
                    self.metrics.refused_by_path[path.index()].fetch_add(1, Ordering::Relaxed);
                    return Err(EhdbError::InvalidState(format!(
                        "stale epoch: shard {shard} {} at epoch {observed}, store has accepted {highest}",
                        path.as_str()
                    )));
                }
                Ok(decision)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tmp(name: &str) -> PathBuf {
        let p = std::env::temp_dir().join(format!(
            "ehdb-fence-{name}-{}-{:?}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&p).unwrap();
        p
    }

    #[test]
    fn off_checks_nothing_and_counts_nothing() {
        let f = EpochFence::off();
        f.set_epoch(7);
        for p in MutationPath::ALL {
            assert!(matches!(
                f.check(0, p).unwrap(),
                FenceDecision::Fresh { .. }
            ));
        }
        // ⚠ Byte-identical to not having a fence: nothing counted at all, so an
        // operator cannot mistake "off" for "on and quiet".
        assert_eq!(f.metrics().writes_checked.load(Ordering::Relaxed), 0);
        assert_eq!(f.metrics().epoch_advances.load(Ordering::Relaxed), 0);
    }

    #[test]
    fn a_fresh_epoch_advances_the_marker_exactly_once() {
        let root = tmp("advance");
        let led = FenceLedger::new(&root).unwrap();
        let f = EpochFence::new(FenceMode::Shadow, Some(led), FenceMetrics::new());
        f.set_epoch(4);

        assert!(matches!(
            f.check(0, MutationPath::AppendRecord).unwrap(),
            FenceDecision::Fresh {
                epoch: 4,
                advanced: true
            }
        ));
        // Second check at the same epoch is fresh but does NOT advance — an
        // `epoch_advances` that climbed per write would be useless as the
        // "did the fence ever receive a real token" discriminator.
        assert!(matches!(
            f.check(0, MutationPath::AppendRecord).unwrap(),
            FenceDecision::Fresh {
                epoch: 4,
                advanced: false
            }
        ));
        assert_eq!(f.metrics().epoch_advances.load(Ordering::Relaxed), 1);
        assert_eq!(f.metrics().writes_checked.load(Ordering::Relaxed), 2);
        std::fs::remove_dir_all(&root).ok();
    }

    /// ⭐ The positive control: the fence must be SEEN to refuse.
    #[test]
    fn a_stale_epoch_is_observed_in_shadow_and_refused_in_enforce() {
        let root = tmp("refuse");
        // A live writer at epoch 9 advances the marker.
        let led = FenceLedger::new(&root).unwrap();
        let live = EpochFence::new(FenceMode::Shadow, Some(led.clone()), FenceMetrics::new());
        live.set_epoch(9);
        live.check(0, MutationPath::AppendRecord).unwrap();
        assert_eq!(led.highest_epoch(0).unwrap(), 9);

        // A superseded writer at epoch 8, in SHADOW: observed, not refused.
        let shadow = EpochFence::new(FenceMode::Shadow, Some(led.clone()), FenceMetrics::new());
        shadow.set_epoch(8);
        let d = shadow.check(0, MutationPath::AppendRecord).unwrap();
        assert!(matches!(
            d,
            FenceDecision::Stale {
                observed: 8,
                highest: 9
            }
        ));
        assert_eq!(shadow.metrics().stale_observed.load(Ordering::Relaxed), 1);
        assert_eq!(
            shadow.metrics().stale_refused.load(Ordering::Relaxed),
            0,
            "shadow must let it through — that is what makes it safe to arm first"
        );

        // The same writer in ENFORCE: refused, with the epochs in the message.
        let enforce = EpochFence::new(FenceMode::Enforce, Some(led.clone()), FenceMetrics::new());
        enforce.set_epoch(8);
        let err = enforce
            .check(0, MutationPath::AppendRecord)
            .expect_err("enforce must refuse a stale write");
        let msg = err.to_string();
        assert!(msg.contains("stale epoch"), "{msg}");
        assert!(msg.contains("append_record"), "path must be named: {msg}");
        assert_eq!(enforce.metrics().stale_refused.load(Ordering::Relaxed), 1);
        assert_eq!(enforce.metrics().refused_for(MutationPath::AppendRecord), 1);
        std::fs::remove_dir_all(&root).ok();
    }

    /// ⚠⚠ Epoch 0 — "no token" — against a marker that has seen a real epoch.
    /// This is the case a writer degrades into when its lease contact goes
    /// stale, and it must be refused.
    #[test]
    fn epoch_zero_is_refused_once_any_real_epoch_has_advanced() {
        let root = tmp("zero");
        let led = FenceLedger::new(&root).unwrap();
        let live = EpochFence::new(FenceMode::Enforce, Some(led.clone()), FenceMetrics::new());
        live.set_epoch(1);
        live.check(0, MutationPath::AppendRecord).unwrap();

        let untokened = EpochFence::new(FenceMode::Enforce, Some(led.clone()), FenceMetrics::new());
        untokened.set_epoch(0);
        assert!(untokened.check(0, MutationPath::AppendRecord).is_err());

        // ⚠ And the companion: against an EMPTY ledger, epoch 0 is NOT refused.
        // 0 is not less than 0, so it reads Fresh and never advances the marker
        // — an enforcing fence that cannot refuse anything. That is why
        // `epoch_advances >= 1` is a SAFETY gate before arming enforce, not a
        // liveness nicety.
        let empty_root = tmp("empty");
        let empty = EpochFence::new(
            FenceMode::Enforce,
            Some(FenceLedger::new(&empty_root).unwrap()),
            FenceMetrics::new(),
        );
        empty.set_epoch(0);
        assert!(matches!(
            empty.check(0, MutationPath::AppendRecord).unwrap(),
            FenceDecision::Fresh {
                advanced: false,
                ..
            }
        ));
        assert_eq!(empty.metrics().epoch_advances.load(Ordering::Relaxed), 0);
        std::fs::remove_dir_all(&root).ok();
        std::fs::remove_dir_all(&empty_root).ok();
    }

    #[test]
    fn a_corrupt_marker_fails_loudly_rather_than_reading_zero() {
        let root = tmp("corrupt");
        let led = FenceLedger::new(&root).unwrap();
        std::fs::write(root.join("shard-00000000.epoch"), "not-a-number").unwrap();
        // ⚠ 0 here would make every epoch look fresh and the fence would stop
        // fencing silently — the worst possible failure for this component.
        let err = led.highest_epoch(0).expect_err("must not read as 0");
        assert!(err.to_string().contains("not an integer"), "{err}");
        std::fs::remove_dir_all(&root).ok();
    }

    #[test]
    fn per_path_counters_are_independent_and_all_labels_are_pinned() {
        let root = tmp("paths");
        let led = FenceLedger::new(&root).unwrap();
        let f = EpochFence::new(FenceMode::Shadow, Some(led), FenceMetrics::new());
        f.set_epoch(1);
        f.check(0, MutationPath::SealAgedParts).unwrap();
        f.check(0, MutationPath::SealAgedParts).unwrap();
        f.check(0, MutationPath::ApplyRetention).unwrap();

        assert_eq!(f.metrics().checked_for(MutationPath::SealAgedParts), 2);
        assert_eq!(f.metrics().checked_for(MutationPath::ApplyRetention), 1);
        assert_eq!(
            f.metrics().checked_for(MutationPath::AppendRecord),
            0,
            "an untouched path must read 0, not inherit the total"
        );

        // Every label pinned, so absence never masquerades as zero.
        let text = f.metrics().render_prometheus(FenceMode::Shadow);
        // The denominator is printed, and the ENGINE/ALL split is asserted here
        // too so a new path cannot land in neither set: 12 engine paths plus the
        // non-engine `persist_cursor`.
        assert_eq!(MutationPath::ENGINE.len(), 12, "engine paths");
        assert_eq!(
            MutationPath::ALL.len(),
            MutationPath::ENGINE.len() + 1,
            "the denominator is every labelled path, engine or not"
        );
        for p in MutationPath::ALL {
            assert!(
                text.contains(&format!(
                    "ehdb_engine_fence_checked_total{{path=\"{}\"}}",
                    p.as_str()
                )),
                "path {} not pinned in:\n{text}",
                p.as_str()
            );
            assert!(
                text.contains(&format!(
                    "ehdb_engine_fence_refused_total{{path=\"{}\"}}",
                    p.as_str()
                )),
                "refused label for {} not pinned",
                p.as_str()
            );
        }
        assert!(text.contains("ehdb_engine_fence_mode 1"), "{text}");
        std::fs::remove_dir_all(&root).ok();
    }

    #[test]
    fn every_path_has_a_distinct_index_and_name() {
        let mut names: Vec<&str> = MutationPath::ALL.iter().map(|p| p.as_str()).collect();
        names.sort_unstable();
        let before = names.len();
        names.dedup();
        assert_eq!(
            before,
            names.len(),
            "duplicate path names collapse counters"
        );
        let mut idx: Vec<usize> = MutationPath::ALL.iter().map(|p| p.index()).collect();
        idx.sort_unstable();
        // Derived from the population rather than restated as a literal: a
        // hardcoded bound makes this fail on every new path for the wrong
        // reason, which trains the next reader to just bump the number.
        assert_eq!(
            idx,
            (0..MutationPath::ALL.len()).collect::<Vec<_>>(),
            "indices must be a bijection over ALL"
        );
    }
}
