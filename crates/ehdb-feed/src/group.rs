//! **L1 T1 — consumer groups + ack / ack_wait redelivery + shard routing.**
//!
//! The NATS consumer-group / queue-group equivalent over the T0 change-feed. A
//! [`ShardConsumerGroup`] follows one shard's log and hands each record to
//! **exactly one** group member (competing consumers / load balancing), tracks
//! it **in-flight** until the member **acks** it, and **redelivers** it if the
//! member fails to ack within `ack_wait` (at-least-once delivery — the #166
//! subject-sharding equivalent, one group per shard).
//!
//! **Clock-free.** Like the L0 engine, this carries no wall-clock: `ack_wait`
//! and deadlines are **logical ticks** the caller supplies to
//! [`poll_assign`](ShardConsumerGroup::poll_assign). The networked wiring (a
//! following slice) maps `Instant` → tick; here the coordinator is deterministic
//! and unit-testable. Ordering of the underlying feed and the resume/cursor
//! semantics are inherited from [`crate::ChangeFeed`] / the L0 log, so the group
//! is as durable and replica-fault-tolerant as any other L0 read.
//!
//! **Committed cursor.** [`committed_cursor`](ShardConsumerGroup::committed_cursor)
//! is the contiguous acked-through sort key — every record at or below it is
//! acked and no longer in flight. Persisting it and reconstructing a group from
//! it resumes exactly where the group left off (the durable-progress seam).

use std::collections::{BTreeMap, VecDeque};

use ehdb_core::Result;
use ehdb_l0::{ChangeFeed, Dataset, L0Engine};

/// A consumer-group member id (a worker instance in the group).
pub type MemberId = u32;

/// One assignment of a record to a member.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Delivery<R> {
    pub record: R,
    /// The record's sort key — the ack token.
    pub sort_key: u64,
    /// The member this delivery was assigned to.
    pub member: MemberId,
    /// `true` if this is a redelivery after an `ack_wait` expiry.
    pub redelivered: bool,
}

struct InFlight<R> {
    record: R,
    member: MemberId,
    deadline: u64,
    redelivered: bool,
}

/// A competing-consumer group over one shard's change-feed.
pub struct ShardConsumerGroup<D: Dataset> {
    shard: u32,
    ack_wait_ticks: u64,
    feed: ChangeFeed,
    /// Pulled from the feed, not yet assigned to a member (ascending sort key).
    pending: VecDeque<D::Record>,
    /// Delivered-but-unacked, keyed by sort key (ascending).
    inflight: BTreeMap<u64, InFlight<D::Record>>,
    /// Highest sort key ever pulled from the feed.
    delivered_frontier: u64,
}

impl<D> ShardConsumerGroup<D>
where
    D: Dataset,
    D::Record: Clone,
{
    /// A group over `shard`, redelivering records unacked after `ack_wait_ticks`,
    /// resuming from `from_cursor` (`0` = the shard from the beginning; a prior
    /// group's [`committed_cursor`](Self::committed_cursor) = resume).
    pub fn new(shard: u32, ack_wait_ticks: u64, from_cursor: u64) -> Self {
        Self {
            shard,
            ack_wait_ticks,
            feed: ChangeFeed::new(shard, from_cursor),
            pending: VecDeque::new(),
            inflight: BTreeMap::new(),
            delivered_frontier: from_cursor,
        }
    }

    /// The shard this group serves.
    pub fn shard(&self) -> u32 {
        self.shard
    }

    /// Pull any newly-appended records off the feed into the pending queue.
    fn refill(&mut self, engine: &L0Engine<D>) -> Result<()> {
        for rec in self.feed.poll(engine)? {
            self.delivered_frontier = self.delivered_frontier.max(D::sort_key(&rec));
            self.pending.push_back(rec);
        }
        Ok(())
    }

    /// Assign the next record to `member` at logical time `now`.
    ///
    /// A record whose `ack_wait` deadline has passed without an ack is redelivered
    /// **before** any fresh record (at-least-once must not starve retries). If no
    /// in-flight record has expired, the next never-delivered record is assigned.
    /// Returns `None` when the shard is fully caught up and nothing is due for
    /// redelivery.
    pub fn poll_assign(
        &mut self,
        engine: &L0Engine<D>,
        member: MemberId,
        now: u64,
    ) -> Result<Option<Delivery<D::Record>>> {
        self.refill(engine)?;

        // 1. Redelivery: the lowest-sort-key in-flight record past its deadline.
        let expired = self
            .inflight
            .iter()
            .find(|(_, f)| f.deadline <= now)
            .map(|(&sk, _)| sk);
        if let Some(sk) = expired {
            let f = self.inflight.get_mut(&sk).unwrap();
            f.member = member;
            f.deadline = now.saturating_add(self.ack_wait_ticks);
            f.redelivered = true;
            return Ok(Some(Delivery {
                record: f.record.clone(),
                sort_key: sk,
                member,
                redelivered: true,
            }));
        }

        // 2. Fresh: the next never-delivered record.
        if let Some(record) = self.pending.pop_front() {
            let sort_key = D::sort_key(&record);
            self.inflight.insert(
                sort_key,
                InFlight {
                    record: record.clone(),
                    member,
                    deadline: now.saturating_add(self.ack_wait_ticks),
                    redelivered: false,
                },
            );
            return Ok(Some(Delivery {
                record,
                sort_key,
                member,
                redelivered: false,
            }));
        }

        Ok(None)
    }

    /// Ack a delivered record by its sort key, removing it from in-flight.
    /// Returns `true` if it was in flight (a duplicate/late ack returns `false`).
    pub fn ack(&mut self, sort_key: u64) -> bool {
        self.inflight.remove(&sort_key).is_some()
    }

    /// The contiguous acked-through cursor: every record at/below it is acked and
    /// not in flight. Safe to persist and resume a fresh group from.
    pub fn committed_cursor(&self) -> u64 {
        // The lowest sort key that is still unacked — either in flight or pulled
        // but not yet assigned — bounds the committed prefix.
        let lowest_open = match (
            self.inflight.keys().next().copied(),
            self.pending.front().map(D::sort_key),
        ) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (Some(a), None) => Some(a),
            (None, Some(b)) => Some(b),
            (None, None) => None,
        };
        match lowest_open {
            Some(open) => open.saturating_sub(1),
            None => self.delivered_frontier,
        }
    }

    /// How many records are currently delivered-but-unacked.
    pub fn inflight_len(&self) -> usize {
        self.inflight.len()
    }

    /// **Consumer lag** — the number of shard records past the committed cursor
    /// (undelivered + delivered-but-unacked). This is the KEDA autoscaler trigger
    /// value (T2): the group's backlog, the analog of NATS JetStream consumer
    /// `num_pending`. Reads the shard tail from the committed cursor, so it costs
    /// O(backlog) — which is exactly the quantity being reported.
    pub fn lag(&self, engine: &L0Engine<D>) -> Result<u64> {
        Ok(engine
            .read_partition_after(self.shard, self.committed_cursor())?
            .len() as u64)
    }
}
