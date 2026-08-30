//! **Stale-writer fencing for the shared segment store** (noetl/ehdb#330, F2).
//! Implements Invariant F from `docs/spec/writer-election-and-fencing.md` §4.2.
//!
//! ## The gap this closes
//!
//! The shared-store contract refuses nothing. [`put_segment`] "overwrites
//! atomically", and [`append_segment`] appends at a `committed_len` the
//! **caller** supplies. So two nodes that both believe they own a shard either
//! overwrite each other's segment or append at the same offset and corrupt the
//! prefix — and nothing anywhere detects it.
//!
//! ## ⭐ Why the token alone is not the fix
//!
//! A fencing token is **data**. Unless something *rejects* on it, a stale writer
//! stamps `epoch=4` into a log that has already accepted `epoch=5` and the write
//! lands anyway. So the enforcement belongs in the **storage contract**:
//!
//! > **Invariant F.** For each shard, the durable store must refuse any append
//! > whose epoch is lower than the highest epoch it has already durably accepted
//! > for that shard.
//!
//! ⚠ And the store must *refuse*, not be *asked*. A writer that checks its own
//! epoch and then writes has a race between the two calls; only a check the
//! store performs as part of the write closes it.
//!
//! ## ⚠⚠ Shipped in SHADOW mode — this refuses nothing yet
//!
//! [`FencingMode::Shadow`] is the default: a stale epoch is **counted and
//! logged, and the write still succeeds**. The live writer path is byte-for-byte
//! unaffected. [`FencingMode::Enforce`] is the real Invariant F and is
//! **owner-gated** — flipping it makes the store start rejecting writes.
//!
//! ## ⚠ Deviation from the spec, and why
//!
//! §4.1 says the epoch is "stamped into every segment frame". It is not, and
//! doing so would be wrong right now: `FRAME_HEADER_LEN` is a fixed 12 bytes and
//! the frame format is **shared byte-identically with `durable_eventlog.rs`**, so
//! widening it is a format break that makes every existing segment unreadable.
//!
//! The epoch lives in a **per-shard fencing marker** instead, which is what
//! Invariant F actually requires — the invariant is about the *highest epoch the
//! store has accepted for a shard*, not about any individual frame. Per-frame
//! attribution ("which epoch wrote this specific record") is a different, weaker
//! property and is deferred to a frame-version bump.
//!
//! [`put_segment`]: crate::durable_eventlog_shared::SharedSegmentBackend::put_segment
//! [`append_segment`]: crate::durable_eventlog_shared::SharedSegmentBackend::append_segment

use std::fs;
use std::io::Write;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use ehdb_core::{EhdbError, Result};

use crate::durable_eventlog_shared::{SharedSegmentBackend, SharedSegmentPutOutcome};

/// Greppable prefix on every refusal, so an operator can find them in logs
/// without knowing the surrounding message.
pub const STALE_EPOCH_PREFIX: &str = "stale_epoch";

/// How the store treats a write whose epoch is behind the highest it has
/// accepted for that shard.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum FencingMode {
    /// **Default.** Count and log a stale epoch; **let the write through**. The
    /// live writer path is unchanged, so this is safe to run in production while
    /// the counter is observed.
    #[default]
    Shadow,
    /// Invariant F for real: refuse the write. ⚠ Owner-gated — this changes what
    /// the store does, not just what it reports.
    Enforce,
}

impl FencingMode {
    /// Parse from configuration. Anything unrecognised is [`Self::Shadow`],
    /// because the fail-safe direction here is "observe", not "refuse".
    pub fn from_str_or_shadow(s: &str) -> Self {
        match s.trim().to_ascii_lowercase().as_str() {
            "enforce" => Self::Enforce,
            _ => Self::Shadow,
        }
    }
    pub fn is_enforcing(self) -> bool {
        matches!(self, Self::Enforce)
    }
}

/// What the ledger concluded about one write.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FenceDecision {
    /// The writer's epoch is at or above the highest accepted — the write is
    /// legitimate. `advanced` is true when this write raised the shard's
    /// high-water epoch.
    Fresh { epoch: u64, advanced: bool },
    /// ⚠ The writer has been superseded: some other epoch has already been
    /// accepted for this shard.
    Stale { observed: u64, highest: u64 },
}

impl FenceDecision {
    pub fn is_stale(self) -> bool {
        matches!(self, Self::Stale { .. })
    }
}

/// Build the error a stale write is refused with under [`FencingMode::Enforce`].
pub fn stale_epoch_error(shard: u32, observed: u64, highest: u64) -> EhdbError {
    EhdbError::InvalidState(format!(
        "{STALE_EPOCH_PREFIX}: shard {shard} write at epoch {observed} refused; \
         store has already accepted epoch {highest}"
    ))
}

/// Whether an error is a fencing refusal. Lets a caller distinguish "I have been
/// superseded" (stop, do not retry) from a transient storage failure (retry).
pub fn is_stale_epoch(err: &EhdbError) -> bool {
    matches!(err, EhdbError::InvalidState(m) if m.starts_with(STALE_EPOCH_PREFIX))
}

/// Secret-free fencing counters.
///
/// ⚠ Every counter is rendered even at zero. Prometheus prunes empty families,
/// so an unpinned counter is absent until it first fires — and "no stale writes
/// have happened" would then look exactly like "this binary has no fencing".
#[derive(Debug, Default)]
pub struct FencingMetrics {
    /// Writes the ledger checked.
    pub writes_checked: AtomicU64,
    /// **Stale writes observed.** In shadow mode these still succeeded.
    pub stale_observed: AtomicU64,
    /// Stale writes actually refused — always 0 in shadow mode. The gap between
    /// this and `stale_observed` is exactly what enforcement would have changed.
    pub stale_refused: AtomicU64,
    /// Writes that raised a shard's high-water epoch.
    pub epoch_advances: AtomicU64,
}

impl FencingMetrics {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Prometheus exposition. All four families are always present.
    pub fn render_prometheus(&self, mode: FencingMode) -> String {
        let g = |a: &AtomicU64| a.load(Ordering::Relaxed);
        let mut out = String::new();
        out.push_str("# HELP ehdb_fencing_writes_checked_total Shared-store writes checked against the shard's highest accepted epoch.\n");
        out.push_str("# TYPE ehdb_fencing_writes_checked_total counter\n");
        out.push_str(&format!(
            "ehdb_fencing_writes_checked_total {}\n",
            g(&self.writes_checked)
        ));
        out.push_str("# HELP ehdb_fencing_stale_observed_total Writes seen from an epoch below the shard's highest accepted epoch. In shadow mode these still succeeded.\n");
        out.push_str("# TYPE ehdb_fencing_stale_observed_total counter\n");
        out.push_str(&format!(
            "ehdb_fencing_stale_observed_total {}\n",
            g(&self.stale_observed)
        ));
        out.push_str("# HELP ehdb_fencing_stale_refused_total Stale writes actually refused. Always 0 in shadow mode; the gap from stale_observed is what enforcement would change.\n");
        out.push_str("# TYPE ehdb_fencing_stale_refused_total counter\n");
        out.push_str(&format!(
            "ehdb_fencing_stale_refused_total {}\n",
            g(&self.stale_refused)
        ));
        out.push_str("# HELP ehdb_fencing_epoch_advances_total Writes that raised a shard's high-water epoch.\n");
        out.push_str("# TYPE ehdb_fencing_epoch_advances_total counter\n");
        out.push_str(&format!(
            "ehdb_fencing_epoch_advances_total {}\n",
            g(&self.epoch_advances)
        ));
        // The mode itself, so a scrape says whether refusals are even possible.
        out.push_str("# HELP ehdb_fencing_enforcing Whether the store refuses stale-epoch writes (1) or only counts them (0).\n");
        out.push_str("# TYPE ehdb_fencing_enforcing gauge\n");
        out.push_str(&format!(
            "ehdb_fencing_enforcing {}\n",
            u8::from(mode.is_enforcing())
        ));
        out
    }
}

/// Durable per-shard high-water epoch.
///
/// One small file per shard under `root`. Reads tolerate a missing or unparsable
/// marker as epoch 0 — a store that has never been fenced accepts any epoch,
/// which is the only behavior that lets fencing be introduced to a live store.
#[derive(Debug)]
pub struct FencingLedger {
    root: PathBuf,
}

impl FencingLedger {
    pub fn new(root: impl Into<PathBuf>) -> Result<Self> {
        let root = root.into();
        fs::create_dir_all(&root).map_err(|err| EhdbError::Storage(err.to_string()))?;
        Ok(Self { root })
    }

    fn marker_path(&self, shard: u32) -> PathBuf {
        self.root.join(format!("shard-{shard:08x}.epoch"))
    }

    /// The highest epoch this store has accepted for `shard`; `0` when never
    /// fenced.
    pub fn highest_epoch(&self, shard: u32) -> Result<u64> {
        match fs::read_to_string(self.marker_path(shard)) {
            Ok(s) => Ok(s.trim().parse::<u64>().unwrap_or(0)),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(0),
            Err(err) => Err(EhdbError::Storage(err.to_string())),
        }
    }

    /// Decide whether a write at `epoch` may proceed, advancing the marker when
    /// it may.
    ///
    /// ⚠ The marker is advanced **before** the caller writes bytes. A crash
    /// between the two leaves the store claiming a higher epoch than it holds
    /// bytes for, which refuses *more* than strictly necessary. That is the
    /// fail-closed direction: the opposite ordering would leave a window in
    /// which bytes are committed under an epoch the store has not recorded, and
    /// a stale writer could then be accepted.
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
        let path = self.marker_path(shard);
        let tmp = path.with_extension("epoch.tmp");
        {
            let mut f =
                fs::File::create(&tmp).map_err(|err| EhdbError::Storage(err.to_string()))?;
            f.write_all(epoch.to_string().as_bytes())
                .map_err(|err| EhdbError::Storage(err.to_string()))?;
            f.sync_all()
                .map_err(|err| EhdbError::Storage(err.to_string()))?;
        }
        fs::rename(&tmp, &path).map_err(|err| EhdbError::Storage(err.to_string()))?;
        Ok(())
    }
}

/// Wraps any [`SharedSegmentBackend`] with Invariant F.
///
/// Every mutating call is checked against the shard's high-water epoch. Reads
/// pass straight through — fencing is about who may *write*.
#[derive(Debug)]
pub struct FencedSharedBackend<B: SharedSegmentBackend> {
    inner: B,
    ledger: FencingLedger,
    mode: FencingMode,
    /// This writer's current epoch. Issued by the election (noetl/ehdb#331);
    /// `0` until one is.
    epoch: AtomicU64,
    metrics: Arc<FencingMetrics>,
}

impl<B: SharedSegmentBackend> FencedSharedBackend<B> {
    /// Wrap `inner`. **Shadow by default** — nothing is refused.
    pub fn new(inner: B, ledger: FencingLedger) -> Self {
        Self {
            inner,
            ledger,
            mode: FencingMode::Shadow,
            epoch: AtomicU64::new(0),
            metrics: FencingMetrics::new(),
        }
    }

    /// ⚠ Owner-gated. Switching to [`FencingMode::Enforce`] makes the store
    /// start refusing writes.
    pub fn with_mode(mut self, mode: FencingMode) -> Self {
        self.mode = mode;
        self
    }

    pub fn with_metrics(mut self, metrics: Arc<FencingMetrics>) -> Self {
        self.metrics = metrics;
        self
    }

    pub fn mode(&self) -> FencingMode {
        self.mode
    }

    pub fn metrics(&self) -> &Arc<FencingMetrics> {
        &self.metrics
    }

    /// Adopt a fencing token. Called by the election on acquiring a lease.
    pub fn set_epoch(&self, epoch: u64) {
        self.epoch.store(epoch, Ordering::SeqCst);
    }

    pub fn epoch(&self) -> u64 {
        self.epoch.load(Ordering::SeqCst)
    }

    /// The check every mutating call runs. Returns `Ok(decision)` when the write
    /// may proceed; `Err` only when enforcing and the write is stale.
    fn guard(&self, shard: u32) -> Result<FenceDecision> {
        let epoch = self.epoch();
        let decision = self.ledger.check_and_advance(shard, epoch)?;
        self.metrics.writes_checked.fetch_add(1, Ordering::Relaxed);
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
                    return Err(stale_epoch_error(shard, observed, highest));
                }
                // ⚠ Shadow: the write proceeds. This is a superseded writer
                // being allowed to continue, deliberately, so the counter can be
                // observed before anything starts refusing.
                eprintln!(
                    "{STALE_EPOCH_PREFIX} (shadow, NOT refused): shard {shard} \
                     write at epoch {observed}, store has accepted {highest}"
                );
                Ok(decision)
            }
        }
    }
}

impl<B: SharedSegmentBackend> SharedSegmentBackend for FencedSharedBackend<B> {
    fn backend_name(&self) -> &'static str {
        self.inner.backend_name()
    }

    fn put_segment(
        &self,
        shard: u32,
        segment_id: u64,
        bytes: &[u8],
    ) -> Result<SharedSegmentPutOutcome> {
        self.guard(shard)?;
        self.inner.put_segment(shard, segment_id, bytes)
    }

    fn append_segment(
        &self,
        shard: u32,
        segment_id: u64,
        committed_len: u64,
        delta: &[u8],
    ) -> Result<SharedSegmentPutOutcome> {
        self.guard(shard)?;
        self.inner
            .append_segment(shard, segment_id, committed_len, delta)
    }

    /// ⚠ Also fenced. Moving the reclaim watermark is a **mutation**, and a
    /// superseded writer advancing it would make readers skip segments a live
    /// writer still owns. Fencing only `put_segment` / `append_segment` — the
    /// two the finding named — would have left this and `delete_segment` open.
    fn put_reclaim_watermark(&self, shard: u32, seq: u64, segment_id: u64) -> Result<()> {
        self.guard(shard)?;
        self.inner.put_reclaim_watermark(shard, seq, segment_id)
    }

    /// ⚠ Also fenced, and the most destructive of the four: a stale writer
    /// deleting segments is strictly worse than one appending to them.
    fn delete_segment(&self, shard: u32, segment_id: u64) -> Result<()> {
        self.guard(shard)?;
        self.inner.delete_segment(shard, segment_id)
    }

    // --- reads and metadata pass through: fencing governs writes only --------

    fn reclaim_watermark(&self, shard: u32) -> Result<(u64, u64)> {
        self.inner.reclaim_watermark(shard)
    }

    fn get_segment(&self, shard: u32, segment_id: u64) -> Result<Option<Vec<u8>>> {
        self.inner.get_segment(shard, segment_id)
    }

    fn list_segment_ids(&self, shard: u32) -> Result<Vec<u64>> {
        self.inner.list_segment_ids(shard)
    }

    fn committed_len(&self, shard: u32, segment_id: u64) -> Result<Option<u64>> {
        self.inner.committed_len(shard, segment_id)
    }
}
