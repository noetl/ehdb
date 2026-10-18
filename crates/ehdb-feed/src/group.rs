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

use std::collections::{BTreeMap, BTreeSet, VecDeque};

use ehdb_core::Result;
use ehdb_l0::{ChangeFeed, Dataset, L0Engine};

use crate::subject::{Subject, SubjectFilter, SubjectFn};

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
    /// Lower bound on the earliest `deadline` in [`Self::inflight`] — the O(1)
    /// reject that keeps redelivery off the per-poll hot path.
    ///
    /// ⚠ The invariant is deliberately one-sided: this may be LOWER than the true
    /// minimum, never higher. Too low only costs an unnecessary scan; too high
    /// would skip a record that is genuinely due and break at-least-once. So it is
    /// lowered on insert and on `expire_now`, and never raised except by a scan
    /// that walked every entry and found nothing expired — which has, by then,
    /// computed the true minimum for free. Forgetting to update it cannot make it
    /// unsafe, which is why this is a cached bound rather than a second
    /// deadline-ordered index that a missed call site could desynchronise
    /// (noetl/ehdb#388).
    earliest_deadline: u64,
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
            earliest_deadline: u64::MAX,
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
        //
        // ⚠ Still the LOWEST SORT KEY among expired, not the earliest deadline.
        // A deadline-ordered index would be cheaper but would change which record
        // redelivers when several are due, and delivery order is observable.
        // `earliest_deadline` only decides whether the scan can be SKIPPED; when
        // it cannot, the scan below is byte-for-byte the original one.
        let expired = if self.earliest_deadline > now {
            // Nothing can be due, so the common case costs one comparison rather
            // than a walk of every in-flight entry (noetl/ehdb#388: a consumer
            // holding k records paid O(k) per delivery, making a lazily-acking
            // drain O(n^2) — measured at 2.86x SLOWER for doing strictly less
            // work).
            None
        } else {
            let mut found = None;
            let mut min_deadline = u64::MAX;
            for (&sk, f) in &self.inflight {
                if f.deadline <= now {
                    found = Some(sk);
                    break;
                }
                min_deadline = min_deadline.min(f.deadline);
            }
            if found.is_none() {
                // This scan visited every entry without short-circuiting, so
                // `min_deadline` is the true minimum: safe to raise the bound,
                // and it restores the fast path for subsequent polls.
                self.earliest_deadline = min_deadline;
            }
            found
        };
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
            let deadline = now.saturating_add(self.ack_wait_ticks);
            // Lower the bound; never raise it here.
            self.earliest_deadline = self.earliest_deadline.min(deadline);
            self.inflight.insert(
                sort_key,
                InFlight {
                    record: record.clone(),
                    member,
                    deadline,
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

    /// Make an in-flight record due for redelivery **immediately**, instead of
    /// waiting out the rest of its `ack_wait`.
    ///
    /// This exists for the one case the `ack_wait` timer serves badly: the member
    /// that holds the record is *gone*. The timer is a bound on how long a LIVE
    /// consumer may sit on a record; when the consumer has vanished, waiting it
    /// out buys nothing and costs a full `ack_wait` of dead time. Measured on
    /// kind: killing the system-pool pod mid-flight stalled executions for
    /// 30.1-71.3s against a 0.5-0.7s baseline, because the orchestrate command
    /// sat in `inflight` under a member whose connection had already closed.
    ///
    /// Returns `false` when `sort_key` is not in flight — a late release racing a
    /// real ack is a no-op, not an error.
    ///
    /// ⚠ Deliberately keeps the record IN FLIGHT rather than pushing it back to
    /// `pending`: `poll_assign` must still hand it out ahead of fresh records
    /// (at-least-once must not starve a retry) and must still mark it
    /// `redelivered`, so a consumer can tell a retry from a first delivery.
    pub fn expire_now(&mut self, sort_key: u64) -> bool {
        match self.inflight.get_mut(&sort_key) {
            Some(f) => {
                f.deadline = 0;
                // The record is due NOW, so the bound must admit it.
                self.earliest_deadline = 0;
                true
            }
            None => false,
        }
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

/// A delivered-but-unacked record in a [`SubjectConsumerGroup`], carrying its
/// subject so redelivery can be scoped to a member's filter.
struct SubjectInFlight<R> {
    record: R,
    subject: Subject,
    member: MemberId,
    deadline: u64,
    redelivered: bool,
}

/// A **subject-routed** competing-consumer group over one shard's change-feed —
/// the general NATS-subject subscription mechanism on the EHDB command path
/// (noetl/ai-meta#194, the RFC's G4 gap).
///
/// A single feed cursor reads the shard once; each record is tagged with its
/// [`Subject`] by [`SubjectFn`] (`commands.<pool>.shard.<n>`). A member claims
/// with a [`SubjectFilter`] ([`poll_assign`](Self::poll_assign) takes it) and is
/// **only ever handed a record whose subject matches** — so pool isolation
/// (`commands.system.>` vs `commands.shared.>`) and #166 shard routing
/// (`…​.shard.0`) are just filter dimensions of one primitive, and a worker can
/// never claim a command outside its subscribed subjects (the hard isolation
/// guarantee). Members sharing a filter still compete exactly-once (a claimed
/// record leaves `pending`); ack / `ack_wait` redelivery is unchanged and
/// scoped to a matching filter. The single global committed cursor (lowest open
/// across every subject) keeps whole-shard lag + resume correct.
pub struct SubjectConsumerGroup<D: Dataset> {
    shard: u32,
    ack_wait_ticks: u64,
    feed: ChangeFeed,
    /// Highest sort key ever pulled from the feed.
    delivered_frontier: u64,
    subject_of: SubjectFn<D::Record>,
    /// `(subject, record)` pulled from the feed, not yet assigned — ascending
    /// sort key (append order preserved through mid-queue removals).
    pending: VecDeque<(Subject, D::Record)>,
    /// Delivered-but-unacked, keyed by sort key (ascending).
    inflight: BTreeMap<u64, SubjectInFlight<D::Record>>,
    /// Lower bound on the earliest in-flight `deadline`; see the identically
    /// named field on [`ShardConsumerGroup`] for the one-sided invariant.
    earliest_deadline: u64,
    /// Every subject this group has ever routed a record to — the label set
    /// [`subject_lags`](Self::subject_lags) reports, so a drained subject keeps
    /// publishing `0` instead of its series vanishing (noetl/ai-meta#194: a KEDA
    /// trigger reading a series that disappears at lag 0 is a scaler error, not
    /// a scale-to-min). Bounded by pools × shards — a handful of entries.
    seen_subjects: BTreeSet<String>,
}

impl<D> SubjectConsumerGroup<D>
where
    D: Dataset,
    D::Record: Clone,
{
    /// A subject-routed group over `shard`, redelivering records unacked after
    /// `ack_wait_ticks`, resuming from `from_cursor`. `subject_of` maps each
    /// record to its routing subject (total; the D1 route is
    /// [`crate::claim::d1_command_subject`]).
    pub fn new(
        shard: u32,
        ack_wait_ticks: u64,
        from_cursor: u64,
        subject_of: SubjectFn<D::Record>,
    ) -> Self {
        Self {
            shard,
            ack_wait_ticks,
            feed: ChangeFeed::new(shard, from_cursor),
            delivered_frontier: from_cursor,
            subject_of,
            pending: VecDeque::new(),
            inflight: BTreeMap::new(),
            earliest_deadline: u64::MAX,
            seen_subjects: BTreeSet::new(),
        }
    }

    /// The shard this group serves.
    pub fn shard(&self) -> u32 {
        self.shard
    }

    /// Pull newly-appended records off the feed, tagging each with its subject.
    fn refill(&mut self, engine: &L0Engine<D>) -> Result<()> {
        for rec in self.feed.poll(engine)? {
            self.delivered_frontier = self.delivered_frontier.max(D::sort_key(&rec));
            let subject = (self.subject_of)(&rec);
            self.seen_subjects.insert(subject.as_str());
            self.pending.push_back((subject, rec));
        }
        Ok(())
    }

    /// Seed the reported subject set from the shard's existing log, without
    /// delivering anything.
    ///
    /// A group resumed at the tail has seen no records yet, so
    /// [`subject_lags`](Self::subject_lags) would report an empty label set until
    /// the first command of each pool arrives — exactly the window after a writer
    /// restart when an autoscaler most needs a reading. Scanning the retained log
    /// once at construction closes that hole: every subject the shard has ever
    /// carried is reported from the first scrape, at `0` when drained. Costs one
    /// partition read at startup and never moves the feed cursor, so it cannot
    /// affect delivery.
    pub fn seed_subjects(&mut self, engine: &L0Engine<D>) -> Result<()> {
        self.seed_subjects_from(Self::scan_subjects(self.shard, &self.subject_of, engine)?);
        Ok(())
    }

    /// The subject set of the shard's retained log, as a standalone scan.
    ///
    /// Separated from [`seed_subjects`] so a caller that opens SEVERAL groups on
    /// the same shard can pay for the scan once. Every group on a shard derives
    /// the same set — same records, same `subject_of` — so repeating the scan per
    /// group is redundant work, and on a large log it is not cheap: on the prod
    /// event bus this read takes **~123 s**, and it ran three times in series
    /// before the claim listener bound, which is ~376 s of the ~456 s an event-bus
    /// rebind cost after a writer restart (noetl/ai-meta#482).
    pub fn scan_subjects(
        shard: u32,
        subject_of: &SubjectFn<D::Record>,
        engine: &L0Engine<D>,
    ) -> Result<BTreeSet<String>> {
        let mut out = BTreeSet::new();
        for rec in engine.read_partition_after(shard, 0)? {
            out.insert((subject_of)(&rec).as_str().to_string());
        }
        Ok(out)
    }

    /// Seed the reported subject set from an already-computed scan.
    ///
    /// Union rather than replace: a group constructed after traffic has arrived
    /// may already have observed subjects the scan did not cover, and dropping
    /// them would shrink the reported label set — the opposite of what seeding is
    /// for.
    pub fn seed_subjects_from(&mut self, subjects: BTreeSet<String>) {
        self.seen_subjects.extend(subjects);
    }

    /// Every subject this group reports a lag for — the seeded/seen label set.
    pub fn seen_subjects(&self) -> impl Iterator<Item = &str> {
        self.seen_subjects.iter().map(String::as_str)
    }

    /// **Per-subject consumer lag** — the shard's backlog past the global
    /// committed cursor, split by routing subject (`commands.<pool>.shard.<n>`).
    ///
    /// [`lag`](Self::lag) is whole-shard by construction: one cursor, every
    /// subject. That is the right number for "is this shard behind", but it is
    /// the wrong trigger for scaling **one pool** — the pools share a shard, so a
    /// system-pool backlog would scale the user pool, and a single stuck
    /// system-pool command pins the global committed cursor and therefore pins
    /// whole-shard lag high forever. This splits the same backlog by the subject a
    /// worker actually subscribes to, so a pool's ScaledObject reads its own
    /// queue (noetl/ai-meta#194 T2).
    ///
    /// Every subject in [`seen_subjects`](Self::seen_subjects) is present in the
    /// result, at `0` when it has no backlog.
    ///
    /// Counted as *undelivered + delivered-but-unacked*, per subject: the group's
    /// own `pending` + `inflight`, plus the not-yet-pulled tail past
    /// `delivered_frontier`. It deliberately does **not** re-derive the count from
    /// the committed cursor the way [`lag`](Self::lag) does — records above a
    /// pinned cursor that this group has already acked are not backlog for
    /// anyone, and attributing them to a pool is exactly the false signal this
    /// split exists to remove. So the per-subject values sum to `lag` only when
    /// nothing above the cursor has been acked; otherwise they sum to less, which
    /// is the honest number.
    pub fn subject_lags(&self, engine: &L0Engine<D>) -> Result<Vec<(String, u64)>> {
        let mut counts: BTreeMap<String, u64> = self
            .seen_subjects
            .iter()
            .map(|s| (s.clone(), 0u64))
            .collect();
        let mut bump = |subject: String| *counts.entry(subject).or_insert(0) += 1;
        for (subject, _) in &self.pending {
            bump(subject.as_str());
        }
        for f in self.inflight.values() {
            bump(f.subject.as_str());
        }
        // The tail the group has not pulled yet — real backlog, not yet tagged.
        for rec in engine.read_partition_after(self.shard, self.delivered_frontier)? {
            bump((self.subject_of)(&rec).as_str());
        }
        Ok(counts.into_iter().collect())
    }

    /// Assign the next record **matching `filter`** to `member` at logical time
    /// `now`. A `filter`-matching in-flight record past its `ack_wait` deadline
    /// is redelivered before any fresh one (at-least-once must not starve
    /// retries). Returns `None` when nothing matching is pending or due —
    /// records for other subjects are invisible to this member (isolation).
    pub fn poll_assign(
        &mut self,
        engine: &L0Engine<D>,
        filter: &SubjectFilter,
        member: MemberId,
        now: u64,
    ) -> Result<Option<Delivery<D::Record>>> {
        self.refill(engine)?;

        // 1. Redelivery: the lowest-sort-key matching in-flight record past its
        //    deadline.
        let expired = if self.earliest_deadline > now {
            None
        } else {
            // ⚠ The filter makes the refresh conditional in a way the shard
            // group's is not: an entry can be EXPIRED but not match this
            // subscriber's filter, leaving `found` empty while a record really is
            // due. Raising the bound from a minimum that skipped those entries
            // would push it ABOVE the true minimum and a later poll with a
            // different filter would never see them. So the bound is only
            // refreshed when nothing at all was expired.
            let mut found = None;
            let mut min_deadline = u64::MAX;
            let mut any_expired = false;
            for (&sk, f) in &self.inflight {
                if f.deadline <= now {
                    any_expired = true;
                    if filter.matches(&f.subject) {
                        found = Some(sk);
                        break;
                    }
                } else {
                    min_deadline = min_deadline.min(f.deadline);
                }
            }
            if !any_expired {
                self.earliest_deadline = min_deadline;
            }
            found
        };
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

        // 2. Fresh: the first (lowest-sort-key) pending record matching `filter`.
        if let Some(pos) = self
            .pending
            .iter()
            .position(|(subj, _)| filter.matches(subj))
        {
            let (subject, record) = self.pending.remove(pos).expect("position in bounds");
            let sort_key = D::sort_key(&record);
            let deadline = now.saturating_add(self.ack_wait_ticks);
            // Lower the bound; never raise it here.
            self.earliest_deadline = self.earliest_deadline.min(deadline);
            self.inflight.insert(
                sort_key,
                SubjectInFlight {
                    record: record.clone(),
                    subject,
                    member,
                    deadline,
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

    /// Ack a delivered record by its sort key. Returns `true` if it was
    /// in flight.
    pub fn ack(&mut self, sort_key: u64) -> bool {
        self.inflight.remove(&sort_key).is_some()
    }

    /// Make an in-flight record due for redelivery **immediately**, instead of
    /// waiting out the rest of its `ack_wait`.
    ///
    /// This exists for the one case the `ack_wait` timer serves badly: the member
    /// that holds the record is *gone*. The timer is a bound on how long a LIVE
    /// consumer may sit on a record; when the consumer has vanished, waiting it
    /// out buys nothing and costs a full `ack_wait` of dead time. Measured on
    /// kind: killing the system-pool pod mid-flight stalled executions for
    /// 30.1-71.3s against a 0.5-0.7s baseline, because the orchestrate command
    /// sat in `inflight` under a member whose connection had already closed.
    ///
    /// Returns `false` when `sort_key` is not in flight — a late release racing a
    /// real ack is a no-op, not an error.
    ///
    /// ⚠ Deliberately keeps the record IN FLIGHT rather than pushing it back to
    /// `pending`: `poll_assign` must still hand it out ahead of fresh records
    /// (at-least-once must not starve a retry) and must still mark it
    /// `redelivered`, so a consumer can tell a retry from a first delivery.
    pub fn expire_now(&mut self, sort_key: u64) -> bool {
        match self.inflight.get_mut(&sort_key) {
            Some(f) => {
                f.deadline = 0;
                // The record is due NOW, so the bound must admit it.
                self.earliest_deadline = 0;
                true
            }
            None => false,
        }
    }

    /// The **global** contiguous acked-through cursor: the lowest still-open
    /// (pending or in-flight) sort key across *all* subjects, minus one; the
    /// `delivered_frontier` when caught up. Advances only when every subject has
    /// acked through it, so resume + lag stay whole-shard correct.
    pub fn committed_cursor(&self) -> u64 {
        // `pending` stays ascending (append-ascending; mid-queue removal keeps
        // order), so its front is the lowest pending sort key.
        let lowest_pending = self.pending.front().map(|(_, r)| D::sort_key(r));
        let lowest_inflight = self.inflight.keys().next().copied();
        let lowest_open = match (lowest_inflight, lowest_pending) {
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

    /// Total delivered-but-unacked records across all subjects.
    pub fn inflight_len(&self) -> usize {
        self.inflight.len()
    }

    /// **Consumer lag** — the whole shard's backlog past the global committed
    /// cursor (undelivered + delivered-but-unacked, every subject). The KEDA
    /// trigger value (T2); counts records for a subject with no subscriber yet.
    pub fn lag(&self, engine: &L0Engine<D>) -> Result<u64> {
        Ok(engine
            .read_partition_after(self.shard, self.committed_cursor())?
            .len() as u64)
    }
}
