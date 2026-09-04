//! Secret-free L0 instrumentation (RFC §5 exit criterion: "secret-free
//! metrics"). Plain atomic counters — no payloads, no execution ids, no keys.
//!
//! The append counters show the hot path; the upload counters show the
//! durable-async tier and its lag (seal → object-store durable); the read
//! counters show pruning effectiveness. A monitoring layer (a later slice) maps
//! these onto Prometheus gauges; here they are the observable surface the L0.1
//! proofs assert against.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// Upper bounds, in **seconds**, for [`LagHistogram`]. Chosen around the
/// durability SLO in `docs/spec/durability-window.md` §5 (p99 ≤ 10 s, max
/// unreplicated age ≤ 30 s) so the buckets straddle the thresholds an alert
/// actually reads, rather than being evenly spaced.
pub const REPLICATED_LAG_BUCKETS_SECONDS: [f64; 9] =
    [0.1, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0, 60.0, 300.0];

/// A fixed-bucket histogram of **append → substrate-durable** latency.
///
/// ⚠ A histogram rather than a mean, deliberately. The pre-existing
/// [`L0MetricsSnapshot::mean_upload_lag_micros`] is an average, and a durability
/// window is bounded by its **maximum** — a mean of 50 ms is entirely consistent
/// with a p99 of 30 s. Quantiles are the only useful shape here.
#[derive(Debug)]
pub struct LagHistogram {
    /// Cumulative counts, one per bound in [`REPLICATED_LAG_BUCKETS_SECONDS`],
    /// plus a final `+Inf` slot.
    buckets: [AtomicU64; REPLICATED_LAG_BUCKETS_SECONDS.len() + 1],
    count: AtomicU64,
    sum_micros: AtomicU64,
}

impl Default for LagHistogram {
    fn default() -> Self {
        Self {
            buckets: std::array::from_fn(|_| AtomicU64::new(0)),
            count: AtomicU64::new(0),
            sum_micros: AtomicU64::new(0),
        }
    }
}

impl LagHistogram {
    fn observe(&self, micros: u64) {
        let secs = micros as f64 / 1_000_000.0;
        let idx = REPLICATED_LAG_BUCKETS_SECONDS
            .iter()
            .position(|&b| secs <= b)
            .unwrap_or(REPLICATED_LAG_BUCKETS_SECONDS.len());
        self.buckets[idx].fetch_add(1, Ordering::Relaxed);
        self.count.fetch_add(1, Ordering::Relaxed);
        self.sum_micros.fetch_add(micros, Ordering::Relaxed);
    }

    /// `(cumulative_bucket_counts, count, sum_seconds)` — bucket counts are
    /// already cumulative, as the Prometheus histogram exposition requires.
    pub fn snapshot(&self) -> (Vec<u64>, u64, f64) {
        let mut running = 0u64;
        let cumulative = self
            .buckets
            .iter()
            .map(|b| {
                running += b.load(Ordering::Relaxed);
                running
            })
            .collect();
        (
            cumulative,
            self.count.load(Ordering::Relaxed),
            self.sum_micros.load(Ordering::Relaxed) as f64 / 1_000_000.0,
        )
    }
}

/// Shared L0 engine counters. Cloneable handle (`Arc`) so the append thread and
/// the uploader thread bump the same counters.
#[derive(Debug, Default)]
pub struct L0Metrics {
    /// Records appended to the hot tier.
    pub appends: AtomicU64,
    /// Appends whose sort key did **not** advance the shard's tail — i.e. a
    /// record inserted at/behind the current maximum sort key for its shard,
    /// violating the ascending-within-a-partition contract the feed cursor and
    /// range pruning depend on. Under the plain [`append_record`] path such a
    /// record lands behind any follower cursor and is silently never delivered
    /// (noetl/ai-meta#203). Writer-assigned appends
    /// ([`append_writer_assigned`]) keep this at 0 by construction; a non-zero
    /// value is the canary that some producer is appending out of order again,
    /// so the loss class is observable instead of silent.
    ///
    /// [`append_record`]: crate::engine::L0Engine::append_record
    /// [`append_writer_assigned`]: crate::engine::L0Engine::append_writer_assigned
    pub out_of_order_appends: AtomicU64,
    /// Versioned manifest snapshots deleted by the retention policy
    /// (noetl/ehdb#344).  Every manifest write emits a *full* snapshot under
    /// `manifest/<dataset>/manifest-v<version>.json`; nothing ever read them, and
    /// nothing pruned them, so cost grew **quadratically** in part count — 71.8 MB
    /// of command data produced 19.4 GB of manifest and filled the volume, which
    /// stopped every append and took prod dispatch down.
    /// Appends answered from the idempotency window instead of being written
    /// (noetl/ai-meta#313). A redelivery lands here rather than becoming a
    /// duplicate in the log or a record silently behind the follower cursor.
    pub dedupe_hits: AtomicU64,
    /// Keys the idempotency window has forgotten because it reached capacity.
    ///
    /// ⚠ The window is a capacity, not a guarantee: a redelivery arriving after
    /// this many intervening appends on its shard is NOT deduplicated. Non-zero
    /// means the window is undersized for the redelivery pattern — without this
    /// an undersized window is indistinguishable from a working one.
    pub dedupe_window_evictions: AtomicU64,
    pub manifest_versions_pruned: AtomicU64,
    /// Versioned manifest snapshots left on the substrate after the most recent
    /// prune — the bound actually being enforced, as opposed to the one
    /// configured.  A gauge: it is `store`d, not accumulated.
    pub manifest_versions_retained: AtomicU64,
    /// Ingest batches the writer refused to append (noetl/ehdb#345).
    ///
    /// `serve_ingest` responds to an `append_batch` error by dropping the
    /// connection, which the publisher sees only as `connection closed before
    /// ack`. Before this counter existed that error was discarded, so a **full
    /// volume** and a **serde-incompatible record** produced a byte-identical
    /// symptom at the publisher and no signal whatsoever at the writer. A prod
    /// writer sat at `Ready`, 0 restarts, 0 ERROR and 0 WARN lines while every
    /// command publish on the platform failed.
    ///
    /// Non-zero means the writer is refusing writes. It is never expected.
    pub ingest_append_failed: AtomicU64,
    /// Ingest frames that did not deserialize into the dataset's record type
    /// (noetl/ehdb#345). Same silent-drop path as
    /// [`ingest_append_failed`](Self::ingest_append_failed); counted separately
    /// because the remedy is completely different — a version/schema mismatch
    /// between publisher and writer, not a sick volume.
    pub ingest_decode_failed: AtomicU64,
    /// Records recovered by replaying an unsealed active part left behind by a
    /// crash (noetl/ai-meta#209 defect 2). These were `fsync`ed and acked to a
    /// publisher but sat outside the durable manifest, which lists sealed parts
    /// only — before recovery existed they were destroyed on the next open.
    ///
    /// A non-zero value means the process did **not** exit cleanly: a clean
    /// shutdown seals, so there is no active part left to replay. Treat a rising
    /// count as a report of hard kills (SIGKILL / OOM / node loss), not as an
    /// error in itself — the records were saved, and the number is how many
    /// would previously have been lost.
    pub recovered_active_records: AtomicU64,
    /// Parts sealed (active → immutable).
    pub seals: AtomicU64,
    /// Parts durably uploaded to the object store.
    pub uploads: AtomicU64,
    /// Bytes uploaded to the object store.
    pub upload_bytes: AtomicU64,
    /// Cumulative upload lag in **microseconds** (seal → object-store durable),
    /// summed across uploads. Mean lag = `upload_lag_micros_total / uploads`.
    pub upload_lag_micros_total: AtomicU64,
    /// **Append → substrate-durable latency** (noetl/ehdb#328) — the D1
    /// durability window, end to end.
    ///
    /// ⚠ Distinct from [`Self::upload_lag_micros_total`], which starts at the
    /// **seal** and therefore cannot see a record waiting in an unsealed active
    /// part. On a quiet shard that pre-seal term is the dominant one, so the two
    /// numbers can disagree by an unbounded amount and only this one answers
    /// "how much would we lose".
    pub replicated_lag: LagHistogram,
    /// Merge/compaction operations performed (L0.3).
    pub merges: AtomicU64,
    /// Source parts consumed by merges (their count summed).
    pub parts_merged: AtomicU64,
    /// Bytes written by merges (merged-part sizes summed).
    pub merged_bytes: AtomicU64,
    /// Orphan objects/files reclaimed by GC (L0.5) — superseded merge sources +
    /// dropped-partition parts.
    pub orphans_reclaimed: AtomicU64,
    /// Bytes freed by orphan reclaim.
    pub orphan_bytes: AtomicU64,
    /// Whole parts dropped by retention (L0.5).
    pub parts_dropped: AtomicU64,
    /// Immutable-part copies written to durable replicas (L0.6). With
    /// replication factor N, `replica_writes ≈ N × parts_sealed`.
    pub replica_writes: AtomicU64,
    /// Reads that fell back to a non-primary replica because an earlier replica
    /// was unreachable (L0.6) — the durability payoff in action.
    pub read_fallbacks: AtomicU64,
    /// Cold-load operations (a fresh node reconstructing from the object store).
    pub cold_loads: AtomicU64,
    /// Read lookups served.
    pub reads: AtomicU64,
    /// Parts pruned away across all reads (partition + MinMax + L0.2 bloom) — the
    /// "zero I/O on non-matching parts" measure.
    pub parts_pruned: AtomicU64,
    /// Of `parts_pruned`, those skipped specifically by the L0.2 execution-id
    /// bloom (survived the partition/MinMax prune, then the bloom rejected them).
    pub parts_bloom_pruned: AtomicU64,
    /// Parts actually opened (local or object-store) across all reads.
    pub parts_scanned: AtomicU64,
}

impl L0Metrics {
    /// A fresh shared metrics handle.
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    pub(crate) fn incr_appends(&self) {
        self.appends.fetch_add(1, Ordering::Relaxed);
    }
    pub(crate) fn incr_dedupe_hits(&self) {
        self.dedupe_hits.fetch_add(1, Ordering::Relaxed);
    }
    pub(crate) fn set_dedupe_window_evictions(&self, n: u64) {
        self.dedupe_window_evictions.store(n, Ordering::Relaxed);
    }
    pub(crate) fn incr_out_of_order_appends(&self) {
        self.out_of_order_appends.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn add_manifest_versions_pruned(&self, n: u64) {
        self.manifest_versions_pruned
            .fetch_add(n, Ordering::Relaxed);
    }
    pub(crate) fn set_manifest_versions_retained(&self, n: u64) {
        self.manifest_versions_retained.store(n, Ordering::Relaxed);
    }
    pub fn incr_ingest_append_failed(&self) {
        self.ingest_append_failed.fetch_add(1, Ordering::Relaxed);
    }
    pub fn incr_ingest_decode_failed(&self) {
        self.ingest_decode_failed.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn add_recovered_active_records(&self, n: u64) {
        self.recovered_active_records
            .fetch_add(n, Ordering::Relaxed);
    }
    pub(crate) fn incr_seals(&self) {
        self.seals.fetch_add(1, Ordering::Relaxed);
    }
    pub(crate) fn record_upload(&self, bytes: u64, lag_micros: u64) {
        self.uploads.fetch_add(1, Ordering::Relaxed);
        self.upload_bytes.fetch_add(bytes, Ordering::Relaxed);
        self.upload_lag_micros_total
            .fetch_add(lag_micros, Ordering::Relaxed);
    }
    /// Record one **append → substrate-durable** latency.
    pub(crate) fn record_replicated_lag(&self, micros: u64) {
        self.replicated_lag.observe(micros);
    }
    pub(crate) fn incr_cold_loads(&self) {
        self.cold_loads.fetch_add(1, Ordering::Relaxed);
    }
    pub(crate) fn record_replica_write(&self) {
        self.replica_writes.fetch_add(1, Ordering::Relaxed);
    }
    pub(crate) fn record_read_fallback(&self) {
        self.read_fallbacks.fetch_add(1, Ordering::Relaxed);
    }
    pub(crate) fn record_merge(&self, source_parts: u64, merged_bytes: u64) {
        self.merges.fetch_add(1, Ordering::Relaxed);
        self.parts_merged.fetch_add(source_parts, Ordering::Relaxed);
        self.merged_bytes.fetch_add(merged_bytes, Ordering::Relaxed);
    }
    pub(crate) fn record_orphan_reclaim(&self, bytes: u64) {
        self.orphans_reclaimed.fetch_add(1, Ordering::Relaxed);
        self.orphan_bytes.fetch_add(bytes, Ordering::Relaxed);
    }
    pub(crate) fn record_parts_dropped(&self, parts: u64) {
        self.parts_dropped.fetch_add(parts, Ordering::Relaxed);
    }
    pub(crate) fn record_read(&self, pruned: u64, bloom_pruned: u64, scanned: u64) {
        self.reads.fetch_add(1, Ordering::Relaxed);
        self.parts_pruned.fetch_add(pruned, Ordering::Relaxed);
        self.parts_bloom_pruned
            .fetch_add(bloom_pruned, Ordering::Relaxed);
        self.parts_scanned.fetch_add(scanned, Ordering::Relaxed);
    }

    /// A point-in-time snapshot (for assertions / reporting).
    pub fn snapshot(&self) -> L0MetricsSnapshot {
        L0MetricsSnapshot {
            appends: self.appends.load(Ordering::Relaxed),
            out_of_order_appends: self.out_of_order_appends.load(Ordering::Relaxed),
            manifest_versions_pruned: self.manifest_versions_pruned.load(Ordering::Relaxed),
            manifest_versions_retained: self.manifest_versions_retained.load(Ordering::Relaxed),
            ingest_append_failed: self.ingest_append_failed.load(Ordering::Relaxed),
            ingest_decode_failed: self.ingest_decode_failed.load(Ordering::Relaxed),
            recovered_active_records: self.recovered_active_records.load(Ordering::Relaxed),
            seals: self.seals.load(Ordering::Relaxed),
            uploads: self.uploads.load(Ordering::Relaxed),
            upload_bytes: self.upload_bytes.load(Ordering::Relaxed),
            upload_lag_micros_total: self.upload_lag_micros_total.load(Ordering::Relaxed),
            merges: self.merges.load(Ordering::Relaxed),
            parts_merged: self.parts_merged.load(Ordering::Relaxed),
            merged_bytes: self.merged_bytes.load(Ordering::Relaxed),
            orphans_reclaimed: self.orphans_reclaimed.load(Ordering::Relaxed),
            orphan_bytes: self.orphan_bytes.load(Ordering::Relaxed),
            parts_dropped: self.parts_dropped.load(Ordering::Relaxed),
            replica_writes: self.replica_writes.load(Ordering::Relaxed),
            read_fallbacks: self.read_fallbacks.load(Ordering::Relaxed),
            cold_loads: self.cold_loads.load(Ordering::Relaxed),
            reads: self.reads.load(Ordering::Relaxed),
            parts_pruned: self.parts_pruned.load(Ordering::Relaxed),
            parts_bloom_pruned: self.parts_bloom_pruned.load(Ordering::Relaxed),
            parts_scanned: self.parts_scanned.load(Ordering::Relaxed),
        }
    }
}

/// A plain-value copy of [`L0Metrics`] at one instant.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct L0MetricsSnapshot {
    pub appends: u64,
    pub out_of_order_appends: u64,
    pub manifest_versions_pruned: u64,
    pub manifest_versions_retained: u64,
    pub ingest_append_failed: u64,
    pub ingest_decode_failed: u64,
    pub recovered_active_records: u64,
    pub seals: u64,
    pub uploads: u64,
    pub upload_bytes: u64,
    pub upload_lag_micros_total: u64,
    pub merges: u64,
    pub parts_merged: u64,
    pub merged_bytes: u64,
    pub orphans_reclaimed: u64,
    pub orphan_bytes: u64,
    pub parts_dropped: u64,
    pub replica_writes: u64,
    pub read_fallbacks: u64,
    pub cold_loads: u64,
    pub reads: u64,
    pub parts_pruned: u64,
    pub parts_bloom_pruned: u64,
    pub parts_scanned: u64,
}

impl L0MetricsSnapshot {
    /// Mean upload lag in microseconds (0 if no uploads yet).
    pub fn mean_upload_lag_micros(&self) -> u64 {
        // `checked_div` returns `None` on a zero divisor (no uploads yet) → 0.
        self.upload_lag_micros_total
            .checked_div(self.uploads)
            .unwrap_or(0)
    }
}
