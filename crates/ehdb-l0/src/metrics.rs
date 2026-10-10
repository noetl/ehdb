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
    /// Failure-domain violations found in the replica set at open (#332 F5).
    ///
    /// ⚠ **Set unconditionally at every 2+-replica open, including to 0**, so a
    /// healthy set reads `0` rather than being absent. An absent series and a
    /// zero are different readings, and the absent one is indistinguishable from
    /// a build that cannot report at all.
    pub replica_domain_violations: AtomicU64,
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
    /// Parts uploaded by a **backfill** — a part that was already sealed when
    /// the replica it is now being copied to was attached.
    ///
    /// ⚠ Counted separately from [`Self::uploads`] on purpose. Uploads are
    /// enqueued on seal and nowhere else, so a substrate attached to an
    /// existing store leaves every already-sealed part at its old replica
    /// count forever; the backfill is what closes that, and it has no
    /// meaningful seal→durable lag because the seal happened in an earlier
    /// process. Folding these into `uploads` would divide the real lag total
    /// by a larger denominator and quietly deflate the mean
    /// (noetl/ehdb#400).
    pub backfill_uploads: AtomicU64,
    /// Bytes uploaded by a backfill.
    pub backfill_upload_bytes: AtomicU64,
    /// **Tail batches replicated off-box** (B3). One per shard per driver tick
    /// that had new unsealed records.
    pub tail_batches: AtomicU64,
    /// Records carried by those batches.
    pub tail_records: AtomicU64,
    /// Bytes carried by those batches.
    pub tail_bytes: AtomicU64,
    /// Tail batches where **every** replica write failed, so the records stayed
    /// local-only and the cursor was not advanced.
    ///
    /// ⚠ Its own counter rather than silence: a tail replicator that is failing
    /// every put looks exactly like one that has nothing to do, because both
    /// leave `tail_batches` flat.
    pub tail_replication_failed: AtomicU64,
    /// Records recovered from tail objects on a cold load — the number that
    /// would otherwise have been lost.
    pub tail_recovered_records: AtomicU64,
    /// Tail objects whose records were ALL already covered by the durable
    /// manifest, so they contributed nothing.
    ///
    /// ⚠ Expected to be non-zero in steady state: a part that sealed after its
    /// tail objects were written makes them redundant by design. A permanent
    /// zero alongside non-zero `tail_batches` means the overlap dedup is not
    /// being exercised, not that it is working.
    pub tail_objects_superseded: AtomicU64,
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

    // ---------------------------------------------------------------
    // State GAUGES (noetl/ai-meta#455 C6). Everything above is a
    // cumulative counter; these four are the *current* depth of
    // something, and all four are computed from in-RAM state with no
    // substrate I/O (`Manifest` + the dedupe window).
    // ---------------------------------------------------------------
    /// Live parts in the manifest.
    ///
    /// ⭐ This is the gauge that makes the engine's **memory bound observable**:
    /// `tests/memory_measures.rs` measures RAM at ~16.6 KB fixed + ~1.8 KB per
    /// part, so part count — not record count — is what predicts footprint. It is
    /// also what merge exists to bound, so a rising value with a flat `merges`
    /// counter is the manifest-growth shape that filled the prod volume
    /// (noetl/ehdb#344).
    pub manifest_parts: AtomicU64,
    /// Parts that are sealed but have **no durable replica yet** — the depth of
    /// the upload backlog.
    ///
    /// This is the durability window expressed as a queue rather than as an age.
    /// A part here exists only in the hot tier: losing the node loses it.
    pub parts_local_only: AtomicU64,
    /// **Divergence from the declared replication factor:** parts with at least
    /// one durable copy but *fewer than the configured replica count*.
    ///
    /// ⚠ Distinct from `parts_local_only` on purpose. A part with 0 copies has
    /// not been uploaded yet, which is latency. A part with 1 of 3 copies has
    /// been uploaded and is **still under-replicated**, which is a durability
    /// deficit that no age-based signal reports. `replica_writes` cannot answer
    /// it either: it is cumulative, so it keeps climbing while the deficit
    /// persists.
    pub parts_under_replicated: AtomicU64,
    /// Records currently held in the append-time idempotency window, summed over
    /// shards.
    ///
    /// `dedupe_window_evictions` says the window is *undersized*; this says how
    /// full it is before that happens.
    pub dedupe_window_records: AtomicU64,
    /// Records dropped by **key-level compaction** during a merge (noetl/ehdb#391).
    ///
    /// Cumulative. A dataset that declares no `supersede_key` can never move this, so a
    /// permanent 0 on a compacting dataset means merges are not collapsing anything — which
    /// is the #391 defect, and reads exactly like a store that has no superseded records.
    pub records_superseded: AtomicU64,
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
    /// Record the replica-set domain-violation count observed at open.
    pub fn set_replica_domain_violations(&self, n: u64) {
        self.replica_domain_violations.store(n, Ordering::Relaxed);
    }

    pub(crate) fn incr_out_of_order_appends(&self) {
        self.out_of_order_appends.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn add_manifest_versions_pruned(&self, n: u64) {
        self.manifest_versions_pruned
            .fetch_add(n, Ordering::Relaxed);
    }
    /// Store the four state gauges in one call.
    ///
    /// ⚠ **Called unconditionally at open, including with all-zero values.** A pin
    /// placed inside a config branch is not a pin — server#315 pinned a reason set
    /// inside `if event_bus_mode.publishes_ehdb()` and left it absent on exactly the
    /// configuration whose value someone would be reading.
    /// Record `n` superseded records dropped by a compacting merge.
    pub(crate) fn add_records_superseded(&self, n: u64) {
        self.records_superseded.fetch_add(n, Ordering::Relaxed);
    }

    pub(crate) fn set_state_gauges(
        &self,
        manifest_parts: u64,
        parts_local_only: u64,
        parts_under_replicated: u64,
        dedupe_window_records: u64,
    ) {
        self.manifest_parts.store(manifest_parts, Ordering::Relaxed);
        self.parts_local_only
            .store(parts_local_only, Ordering::Relaxed);
        self.parts_under_replicated
            .store(parts_under_replicated, Ordering::Relaxed);
        self.dedupe_window_records
            .store(dedupe_window_records, Ordering::Relaxed);
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
    /// Record one **backfill** upload. Deliberately does not touch
    /// `uploads` / `upload_bytes` / `upload_lag_micros_total` — see
    /// [`Self::backfill_uploads`].
    pub(crate) fn record_backfill_upload(&self, bytes: u64) {
        self.backfill_uploads.fetch_add(1, Ordering::Relaxed);
        self.backfill_upload_bytes
            .fetch_add(bytes, Ordering::Relaxed);
    }
    /// Record one tail batch that reached at least one replica.
    pub(crate) fn record_tail_batch(&self, records: u64, bytes: u64) {
        self.tail_batches.fetch_add(1, Ordering::Relaxed);
        self.tail_records.fetch_add(records, Ordering::Relaxed);
        self.tail_bytes.fetch_add(bytes, Ordering::Relaxed);
    }
    pub(crate) fn incr_tail_replication_failed(&self) {
        self.tail_replication_failed.fetch_add(1, Ordering::Relaxed);
    }
    pub(crate) fn add_tail_recovered(&self, n: u64) {
        self.tail_recovered_records.fetch_add(n, Ordering::Relaxed);
    }
    pub(crate) fn add_tail_objects_superseded(&self, n: u64) {
        self.tail_objects_superseded.fetch_add(n, Ordering::Relaxed);
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
            dedupe_hits: self.dedupe_hits.load(Ordering::Relaxed),
            dedupe_window_evictions: self.dedupe_window_evictions.load(Ordering::Relaxed),
            out_of_order_appends: self.out_of_order_appends.load(Ordering::Relaxed),
            replica_domain_violations: self.replica_domain_violations.load(Ordering::Relaxed),
            manifest_versions_pruned: self.manifest_versions_pruned.load(Ordering::Relaxed),
            manifest_versions_retained: self.manifest_versions_retained.load(Ordering::Relaxed),
            ingest_append_failed: self.ingest_append_failed.load(Ordering::Relaxed),
            ingest_decode_failed: self.ingest_decode_failed.load(Ordering::Relaxed),
            recovered_active_records: self.recovered_active_records.load(Ordering::Relaxed),
            seals: self.seals.load(Ordering::Relaxed),
            uploads: self.uploads.load(Ordering::Relaxed),
            upload_bytes: self.upload_bytes.load(Ordering::Relaxed),
            upload_lag_micros_total: self.upload_lag_micros_total.load(Ordering::Relaxed),
            backfill_uploads: self.backfill_uploads.load(Ordering::Relaxed),
            backfill_upload_bytes: self.backfill_upload_bytes.load(Ordering::Relaxed),
            tail_batches: self.tail_batches.load(Ordering::Relaxed),
            tail_records: self.tail_records.load(Ordering::Relaxed),
            tail_bytes: self.tail_bytes.load(Ordering::Relaxed),
            tail_replication_failed: self.tail_replication_failed.load(Ordering::Relaxed),
            tail_recovered_records: self.tail_recovered_records.load(Ordering::Relaxed),
            tail_objects_superseded: self.tail_objects_superseded.load(Ordering::Relaxed),
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
            manifest_parts: self.manifest_parts.load(Ordering::Relaxed),
            parts_local_only: self.parts_local_only.load(Ordering::Relaxed),
            parts_under_replicated: self.parts_under_replicated.load(Ordering::Relaxed),
            dedupe_window_records: self.dedupe_window_records.load(Ordering::Relaxed),
            records_superseded: self.records_superseded.load(Ordering::Relaxed),
        }
    }
}

/// A plain-value copy of [`L0Metrics`] at one instant.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct L0MetricsSnapshot {
    pub appends: u64,
    /// Appends answered from the idempotency window (noetl/ai-meta#313).
    ///
    /// ⚠ On the snapshot because a counter the engine keeps but nothing can read
    /// is not observability. Without this the dedupe is invisible in production:
    /// "working" and "never firing" produce identical scrapes.
    pub dedupe_hits: u64,
    /// Keys the idempotency window has forgotten at capacity (noetl/ai-meta#313).
    /// Non-zero means the window is undersized for the redelivery pattern.
    pub dedupe_window_evictions: u64,
    pub out_of_order_appends: u64,
    /// Failure-domain violations in the replica set at open (#332 F5).
    pub replica_domain_violations: u64,
    pub manifest_versions_pruned: u64,
    pub manifest_versions_retained: u64,
    pub ingest_append_failed: u64,
    pub ingest_decode_failed: u64,
    pub recovered_active_records: u64,
    pub seals: u64,
    pub uploads: u64,
    pub upload_bytes: u64,
    pub upload_lag_micros_total: u64,
    pub backfill_uploads: u64,
    pub backfill_upload_bytes: u64,
    pub tail_batches: u64,
    pub tail_records: u64,
    pub tail_bytes: u64,
    pub tail_replication_failed: u64,
    pub tail_recovered_records: u64,
    pub tail_objects_superseded: u64,
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
    /// Live parts in the manifest (gauge).
    pub manifest_parts: u64,
    /// Sealed parts with no durable replica yet — upload backlog depth (gauge).
    pub parts_local_only: u64,
    /// Parts with 1..N-1 durable copies — divergence from the declared RF (gauge).
    pub parts_under_replicated: u64,
    /// Records held in the idempotency window, summed over shards (gauge).
    pub dedupe_window_records: u64,
    /// Records dropped by key-level compaction during merges (counter).
    pub records_superseded: u64,
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

// ===========================================================================
// Prometheus exposition
// ===========================================================================

/// The engine's version, stamped at build time.
///
/// ⚠ Emitted as a `*_build_info{version}` gauge **pinned at 1**. Without it, a metric's
/// absence from a scrape has two indistinguishable causes: the series never fired, or the
/// running binary predates it. `build_info` separates them, and that ambiguity has cost
/// this fleet a 25-day monitoring outage.
pub const EHDB_L0_VERSION: &str = env!("CARGO_PKG_VERSION");

/// Every series this engine exports, as `(name, help, kind)`.
///
/// ⚠⚠ **This list is the exposition's denominator**, and
/// `every_snapshot_field_is_exported` asserts it covers every field of
/// [`L0MetricsSnapshot`]. Adding a counter to the struct without adding it here fails that
/// test — which is the point. A metric the engine keeps and nothing can scrape is not
/// observability, and that was the state of all 27 of these before this exposition existed.
const SERIES: &[(&str, &str, &str)] = &[
    ("appends", "Records appended to the engine.", "counter"),
    (
        "dedupe_hits",
        "Appends answered from the idempotency window.",
        "counter",
    ),
    (
        "dedupe_window_evictions",
        "Keys the idempotency window forgot at capacity; non-zero means it is undersized.",
        "counter",
    ),
    (
        "out_of_order_appends",
        "Appends that did not advance their shard tail — the ascending-contract canary.",
        "counter",
    ),
    (
        "replica_domain_violations",
        "Failure-domain violations in the replica set at open.",
        "gauge",
    ),
    (
        "manifest_versions_pruned",
        "Manifest snapshots deleted by retention.",
        "counter",
    ),
    (
        "manifest_versions_retained",
        "Manifest snapshots currently retained.",
        "gauge",
    ),
    (
        "ingest_append_failed",
        "Ingest appends that failed.",
        "counter",
    ),
    (
        "ingest_decode_failed",
        "Ingest frames that failed to decode.",
        "counter",
    ),
    (
        "recovered_active_records",
        "Records recovered from an active part at open.",
        "counter",
    ),
    ("seals", "Parts sealed.", "counter"),
    (
        "uploads",
        "Parts uploaded to the durable substrate.",
        "counter",
    ),
    ("upload_bytes", "Bytes uploaded.", "counter"),
    (
        "upload_lag_micros_total",
        "Summed upload lag in microseconds; divide by uploads for the mean.",
        "counter",
    ),
    ("merges", "Merge operations performed.", "counter"),
    ("parts_merged", "Parts consumed by merges.", "counter"),
    ("merged_bytes", "Bytes rewritten by merges.", "counter"),
    ("orphans_reclaimed", "Orphaned parts reclaimed.", "counter"),
    ("orphan_bytes", "Bytes reclaimed from orphans.", "counter"),
    (
        "parts_dropped",
        "Parts dropped after a merge superseded them.",
        "counter",
    ),
    (
        "replica_writes",
        "Writes issued to replica substrates.",
        "counter",
    ),
    (
        "read_fallbacks",
        "Reads that fell back to another replica.",
        "counter",
    ),
    ("cold_loads", "Cold manifest loads.", "counter"),
    ("reads", "Read operations served.", "counter"),
    (
        "parts_pruned",
        "Parts pruned by manifest metadata before any I/O.",
        "counter",
    ),
    (
        "parts_bloom_pruned",
        "Parts pruned by a bloom filter.",
        "counter",
    ),
    (
        "parts_scanned",
        "Parts actually scanned by reads.",
        "counter",
    ),
    (
        "manifest_parts",
        "Live parts in the manifest. Predicts engine memory (~1.8 KB each) and is what merge bounds.",
        "gauge",
    ),
    (
        "parts_local_only",
        "Sealed parts with no durable replica yet: the upload backlog depth.",
        "gauge",
    ),
    (
        "parts_under_replicated",
        "Parts with at least one copy but fewer than the configured replica count: divergence from the declared replication factor.",
        "gauge",
    ),
    (
        "backfill_uploads",
        "Parts copied to a replica by a backfill (already sealed when that replica was attached). A permanent 0 while parts_under_replicated is non-zero means existing history is not being repaired.",
        "counter",
    ),
    (
        "backfill_upload_bytes",
        "Bytes copied by backfill uploads.",
        "counter",
    ),
    (
        "tail_batches",
        "Unsealed-tail batches replicated off-box (B3). A permanent 0 with a non-zero tail_replication_failed means every put is failing; a permanent 0 with both at 0 means no driver is calling replicate_tail.",
        "counter",
    ),
    (
        "tail_records",
        "Records carried by replicated tail batches — the loss that off-box tail replication is preventing.",
        "counter",
    ),
    (
        "tail_bytes",
        "Bytes carried by replicated tail batches.",
        "counter",
    ),
    (
        "tail_replication_failed",
        "Tail batches where every replica write failed, so the records stayed local-only and the cursor did not advance. Distinct from silence: a failing replicator and an idle one both leave tail_batches flat.",
        "counter",
    ),
    (
        "tail_recovered_records",
        "Records recovered from tail objects on a cold load — what would otherwise have been lost with the primary gone.",
        "counter",
    ),
    (
        "tail_objects_superseded",
        "Tail objects whose records were already covered by the durable manifest. Non-zero is expected in steady state; a permanent 0 alongside non-zero tail_batches means the overlap dedup is not being exercised.",
        "counter",
    ),
    (
        "dedupe_window_records",
        "Records held in the append-time idempotency window, summed over shards.",
        "gauge",
    ),
    (
        "records_superseded",
        "Records dropped by key-level compaction during a merge. A permanent 0 on a dataset that declares a supersede_key means merges are not collapsing anything.",
        "counter",
    ),
];

impl L0MetricsSnapshot {
    /// Read one series by name. Kept beside [`SERIES`] so the two cannot drift: a name in
    /// the list with no arm here fails to compile.
    fn value(&self, name: &str) -> u64 {
        match name {
            "appends" => self.appends,
            "dedupe_hits" => self.dedupe_hits,
            "dedupe_window_evictions" => self.dedupe_window_evictions,
            "out_of_order_appends" => self.out_of_order_appends,
            "replica_domain_violations" => self.replica_domain_violations,
            "manifest_versions_pruned" => self.manifest_versions_pruned,
            "manifest_versions_retained" => self.manifest_versions_retained,
            "ingest_append_failed" => self.ingest_append_failed,
            "ingest_decode_failed" => self.ingest_decode_failed,
            "recovered_active_records" => self.recovered_active_records,
            "seals" => self.seals,
            "uploads" => self.uploads,
            "upload_bytes" => self.upload_bytes,
            "upload_lag_micros_total" => self.upload_lag_micros_total,
            "merges" => self.merges,
            "parts_merged" => self.parts_merged,
            "merged_bytes" => self.merged_bytes,
            "orphans_reclaimed" => self.orphans_reclaimed,
            "orphan_bytes" => self.orphan_bytes,
            "parts_dropped" => self.parts_dropped,
            "replica_writes" => self.replica_writes,
            "read_fallbacks" => self.read_fallbacks,
            "cold_loads" => self.cold_loads,
            "reads" => self.reads,
            "parts_pruned" => self.parts_pruned,
            "parts_bloom_pruned" => self.parts_bloom_pruned,
            "parts_scanned" => self.parts_scanned,
            "manifest_parts" => self.manifest_parts,
            "parts_local_only" => self.parts_local_only,
            "parts_under_replicated" => self.parts_under_replicated,
            "backfill_uploads" => self.backfill_uploads,
            "backfill_upload_bytes" => self.backfill_upload_bytes,
            "tail_batches" => self.tail_batches,
            "tail_records" => self.tail_records,
            "tail_bytes" => self.tail_bytes,
            "tail_replication_failed" => self.tail_replication_failed,
            "tail_recovered_records" => self.tail_recovered_records,
            "tail_objects_superseded" => self.tail_objects_superseded,
            "dedupe_window_records" => self.dedupe_window_records,
            "records_superseded" => self.records_superseded,
            other => unreachable!("SERIES names a metric with no accessor: {other}"),
        }
    }

    /// Render the Prometheus text exposition (format v0.0.4).
    ///
    /// ⚠ **Every series is emitted unconditionally, including at 0.** That is the whole
    /// design. `prometheus::Registry::gather` prunes metric families with no children, so a
    /// labelled metric is *absent* until something increments it — and an absent series and
    /// a healthy zero look identical to every alert. Emitting from a plain snapshot avoids
    /// that by construction: there are no label children to be empty.
    ///
    /// `dataset` becomes a label so one process serving several datasets is separable.
    pub fn render_prometheus(&self, dataset: &str) -> String {
        let ds = escape_label(dataset);
        let mut out = String::with_capacity(SERIES.len() * 160);
        out.push_str("# HELP ehdb_l0_build_info Engine version, pinned at 1 so an absent series can be told from an old binary.\n");
        out.push_str("# TYPE ehdb_l0_build_info gauge\n");
        out.push_str(&format!(
            "ehdb_l0_build_info{{version=\"{}\"}} 1\n",
            escape_label(EHDB_L0_VERSION)
        ));
        for (name, help, kind) in SERIES {
            out.push_str(&format!("# HELP ehdb_l0_{name} {help}\n"));
            out.push_str(&format!("# TYPE ehdb_l0_{name} {kind}\n"));
            out.push_str(&format!(
                "ehdb_l0_{name}{{dataset=\"{ds}\"}} {}\n",
                self.value(name)
            ));
        }
        // Derived, because a mean is what an operator reads and `total/count` invites the
        // divide-by-zero that reports 0 lag for "no uploads yet".
        out.push_str("# HELP ehdb_l0_upload_lag_micros_mean Mean upload lag; 0 when no uploads have happened.\n");
        out.push_str("# TYPE ehdb_l0_upload_lag_micros_mean gauge\n");
        out.push_str(&format!(
            "ehdb_l0_upload_lag_micros_mean{{dataset=\"{ds}\"}} {}\n",
            self.mean_upload_lag_micros()
        ));
        out
    }

    /// The series names this exposition emits, for tests and for a scrape-coverage check.
    pub fn series_names() -> Vec<&'static str> {
        SERIES.iter().map(|(n, _, _)| *n).collect()
    }
}

/// Escape a Prometheus label value (backslash, quote, newline).
fn escape_label(v: &str) -> String {
    v.replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
}
