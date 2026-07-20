//! **L1 T0 — the change-feed / `Watch(shard, cursor)` primitive.**
//!
//! The core of topology (c) (per-shard-writer-as-broker, RFC L1): the per-shard
//! writer already **owns** the durable log for its shard; here it **grows a
//! change-feed** so subscribers (workers, the gateway feed) can follow new
//! records appended after a cursor — the primitive the writer→worker delivery
//! path is built on. This slice is the in-process primitive; the networked
//! transport that carries a feed batch to a remote subscriber and the
//! append→worker latency measurement land in the following T0 slices.
//!
//! A [`ChangeFeed`] is a cursor over one shard's append-only log. Because the
//! log is immutable and sort-key-ordered, following it is exact and cheap:
//!
//! - **Follow** — [`poll`](ChangeFeed::poll) returns every record appended to
//!   the shard after the cursor, in sort-key order, and advances the cursor past
//!   the batch. Re-polling with no new appends returns nothing (idempotent at a
//!   fixed cursor) — **0 missed, 0 spurious** vs a full shard replay.
//! - **Resume / reconnect** — a feed reconstructed with the last delivered (or
//!   acked) cursor resumes exactly at the tail after it; [`seek`](ChangeFeed::seek)
//!   rewinds to redeliver from an earlier watermark (the ack-redelivery seam T1
//!   builds on).
//! - **Tail** — [`tail`](ChangeFeed::tail) starts at the engine's current tip so
//!   the subscriber sees only records appended *after now* (the shadow-feed
//!   posture: NATS stays authoritative, the feed observes new commands only).
//! - **Shard isolation** — a feed on one shard never sees another shard's
//!   records (the #166 subject-sharding equivalent).
//! - **Durability** — the feed reads the same replicated immutable parts as
//!   every other L0 read, so it survives seal/flush across the part boundary and
//!   an N-way replica-kill (reads fall back to a surviving replica).

use ehdb_core::Result;

use crate::dataset::Dataset;
use crate::engine::L0Engine;

/// A cursor-based follower over one shard's append-only log — the
/// `Watch(shard, cursor)` primitive.
///
/// The cursor is the sort key of the last delivered record (the resume point /
/// ack watermark). Cloning a feed clones its cursor, so two followers of the
/// same shard advance independently.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChangeFeed {
    shard: u32,
    cursor: u64,
}

impl ChangeFeed {
    /// Watch `shard` for records with sort key `> from_cursor`. `from_cursor = 0`
    /// replays the shard from the beginning; passing the engine's current tip
    /// tails only new records (see [`tail`](Self::tail)).
    pub fn new(shard: u32, from_cursor: u64) -> Self {
        Self {
            shard,
            cursor: from_cursor,
        }
    }

    /// Watch `shard` for records appended *after now* — starts at the engine's
    /// current global-sequence tip (the shadow-feed posture).
    pub fn tail<D: Dataset>(engine: &L0Engine<D>, shard: u32) -> Self {
        Self::new(shard, engine.global_sequence())
    }

    /// The shard this feed follows.
    pub fn shard(&self) -> u32 {
        self.shard
    }

    /// The cursor position — the sort key of the last delivered record, i.e. the
    /// resume point a reconnecting subscriber restarts from.
    pub fn cursor(&self) -> u64 {
        self.cursor
    }

    /// Poll `engine` for records after the cursor and advance the cursor past the
    /// returned batch. Returns the new records in sort-key order (empty once
    /// caught up). Idempotent at a fixed cursor: polling again without a new
    /// append returns nothing.
    pub fn poll<D: Dataset>(&mut self, engine: &L0Engine<D>) -> Result<Vec<D::Record>> {
        let batch = engine.read_partition_after(self.shard, self.cursor)?;
        if let Some(last) = batch.last() {
            self.cursor = D::sort_key(last);
        }
        Ok(batch)
    }

    /// Rewind (or fast-forward) the cursor to `cursor` — a subscriber whose ack
    /// was lost resumes redelivery from its last acked watermark (T1's
    /// ack/ack_wait seam).
    pub fn seek(&mut self, cursor: u64) {
        self.cursor = cursor;
    }
}
