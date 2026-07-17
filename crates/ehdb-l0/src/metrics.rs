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

/// Shared L0 engine counters. Cloneable handle (`Arc`) so the append thread and
/// the uploader thread bump the same counters.
#[derive(Debug, Default)]
pub struct L0Metrics {
    /// Records appended to the hot tier.
    pub appends: AtomicU64,
    /// Parts sealed (active → immutable).
    pub seals: AtomicU64,
    /// Parts durably uploaded to the object store.
    pub uploads: AtomicU64,
    /// Bytes uploaded to the object store.
    pub upload_bytes: AtomicU64,
    /// Cumulative upload lag in **microseconds** (seal → object-store durable),
    /// summed across uploads. Mean lag = `upload_lag_micros_total / uploads`.
    pub upload_lag_micros_total: AtomicU64,
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
    pub(crate) fn incr_seals(&self) {
        self.seals.fetch_add(1, Ordering::Relaxed);
    }
    pub(crate) fn record_upload(&self, bytes: u64, lag_micros: u64) {
        self.uploads.fetch_add(1, Ordering::Relaxed);
        self.upload_bytes.fetch_add(bytes, Ordering::Relaxed);
        self.upload_lag_micros_total
            .fetch_add(lag_micros, Ordering::Relaxed);
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
