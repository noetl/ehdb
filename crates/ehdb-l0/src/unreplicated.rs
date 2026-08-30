//! **The D1 durability window, measured from the append** (noetl/ehdb#328, F4).
//!
//! An acknowledged append is durable on **one** disk immediately (the local part
//! is `fsync`'d before the ack) and reaches the durable substrate only when its
//! part **seals** and the background uploader ships it. The interval between
//! those two points is the durability window: the records that would not come
//! back if the node were lost.
//!
//! ## ⚠ Why the pre-existing metric cannot measure it
//!
//! [`L0Metrics::upload_lag_micros_total`](crate::metrics::L0Metrics) is
//! accumulated as `job.sealed_at.elapsed()` — measured from **seal**. A record
//! waiting in an unsealed active part contributes **nothing** to it. Since
//! sealing is triggered only by size or record count and never by age
//! (noetl/ehdb#329), the pre-seal term is the *dominant* one on a quiet shard —
//! and it is exactly the term that metric is blind to. A dashboard built on it
//! reads healthy in precisely the scenario where events sit unreplicated.
//!
//! This tracker measures from the **append** instead, so the number answers the
//! question an operator is actually asking.
//!
//! ## What it tracks
//!
//! Per shard, the instant of the **oldest** acknowledged record that is not yet
//! durable on the substrate, across two populations:
//!
//! * the **active** (unsealed) part — records acked but not yet even sealed, and
//! * **sealed parts still in flight** to the substrate.
//!
//! The window is `now - min(first_append_at)` over both. When a shard has
//! nothing pending the age is **0**, which is a real reading and not an absence
//! — see [`UnreplicatedTracker::snapshot`].

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::Instant;

/// One shard's unreplicated position, sampled at a point in time.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ShardUnreplicated {
    pub shard: u32,
    /// Age of the **oldest** acked-but-not-yet-durable record, in milliseconds.
    /// `0` when the shard has nothing pending.
    pub oldest_age_millis: u64,
    /// How many acked records are not yet durable on the substrate.
    pub records: u64,
}

#[derive(Debug, Default)]
struct ShardPending {
    /// When the *first* record of the current active part was appended. `None`
    /// while the active part is empty.
    active_first_append: Option<Instant>,
    active_records: u64,
    /// Sealed parts handed to the uploader but not yet durable, keyed by
    /// `part_id` → (first append in that part, record count).
    sealed: HashMap<String, (Instant, u64)>,
}

impl ShardPending {
    fn oldest(&self) -> Option<Instant> {
        self.sealed
            .values()
            .map(|(at, _)| *at)
            .chain(self.active_first_append)
            .min()
    }

    fn records(&self) -> u64 {
        self.active_records + self.sealed.values().map(|(_, n)| *n).sum::<u64>()
    }
}

/// Tracks the durability window per shard.
///
/// Shared with the uploader thread, which calls [`Self::on_upload_done`] when a
/// part becomes durable.
#[derive(Debug)]
pub struct UnreplicatedTracker {
    shards: Mutex<HashMap<u32, ShardPending>>,
    /// Every shard this process may own. Held so [`Self::snapshot`] can emit a
    /// row for a shard that has never appended — see the pinning note there.
    pinned: Vec<u32>,
}

impl UnreplicatedTracker {
    /// A tracker that always reports on shards `0..shard_count`.
    pub fn new(shard_count: u32) -> Self {
        Self {
            shards: Mutex::new(HashMap::new()),
            pinned: (0..shard_count).collect(),
        }
    }

    /// One record was appended to `shard`'s active part and acked.
    pub fn on_append(&self, shard: u32) {
        let mut g = self.shards.lock().unwrap();
        let e = g.entry(shard).or_default();
        if e.active_first_append.is_none() {
            e.active_first_append = Some(Instant::now());
        }
        e.active_records += 1;
    }

    /// `shard`'s active part sealed as `part_id` with `record_count` records and
    /// was handed to the uploader. The part inherits the active part's
    /// first-append instant, so the window keeps measuring from the **append**
    /// and not from the seal.
    pub fn on_seal(&self, shard: u32, part_id: &str, record_count: u64) {
        let mut g = self.shards.lock().unwrap();
        let e = g.entry(shard).or_default();
        // A part recovered on open, or sealed by a path that never appended
        // through this tracker, has no recorded append instant. Falling back to
        // `now` under-reports rather than inventing an age.
        let first = e.active_first_append.take().unwrap_or_else(Instant::now);
        e.active_records = 0;
        e.sealed.insert(part_id.to_string(), (first, record_count));
    }

    /// `part_id` is durable on the substrate. Returns the append→durable latency
    /// so the caller can record it as the end-to-end replication lag.
    pub fn on_upload_done(&self, shard: u32, part_id: &str) -> Option<std::time::Duration> {
        let mut g = self.shards.lock().unwrap();
        let e = g.get_mut(&shard)?;
        let (first, _) = e.sealed.remove(part_id)?;
        Some(first.elapsed())
    }

    /// Sample every pinned shard.
    ///
    /// ⚠ **Every pinned shard gets a row, including one that has never
    /// appended.** Prometheus prunes empty metric families, so an unpinned
    /// labelled gauge is absent until it first fires — making "nothing pending"
    /// and "this binary has no such metric" indistinguishable on a scrape. A
    /// shard with nothing pending must read `0`, not vanish.
    pub fn snapshot(&self) -> Vec<ShardUnreplicated> {
        let g = self.shards.lock().unwrap();
        let mut out: Vec<ShardUnreplicated> = self
            .pinned
            .iter()
            .map(|&shard| match g.get(&shard) {
                Some(p) => ShardUnreplicated {
                    shard,
                    oldest_age_millis: p
                        .oldest()
                        .map(|at| at.elapsed().as_millis() as u64)
                        .unwrap_or(0),
                    records: p.records(),
                },
                None => ShardUnreplicated {
                    shard,
                    oldest_age_millis: 0,
                    records: 0,
                },
            })
            .collect();
        // Any shard outside the pinned range that has nonetheless seen traffic
        // is still reported — a missing row would hide a real backlog.
        let mut extra: Vec<u32> = g
            .keys()
            .copied()
            .filter(|s| !self.pinned.contains(s))
            .collect();
        extra.sort_unstable();
        for shard in extra {
            let p = &g[&shard];
            out.push(ShardUnreplicated {
                shard,
                oldest_age_millis: p
                    .oldest()
                    .map(|at| at.elapsed().as_millis() as u64)
                    .unwrap_or(0),
                records: p.records(),
            });
        }
        out.sort_by_key(|s| s.shard);
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn age(t: &UnreplicatedTracker, shard: u32) -> u64 {
        t.snapshot()
            .into_iter()
            .find(|s| s.shard == shard)
            .expect("shard row present")
            .oldest_age_millis
    }

    fn records(t: &UnreplicatedTracker, shard: u32) -> u64 {
        t.snapshot()
            .into_iter()
            .find(|s| s.shard == shard)
            .expect("shard row present")
            .records
    }

    #[test]
    fn a_shard_that_never_appended_reads_zero_rather_than_vanishing() {
        let t = UnreplicatedTracker::new(3);
        let snap = t.snapshot();
        assert_eq!(snap.len(), 3, "every pinned shard emits a row");
        assert!(
            snap.iter()
                .all(|s| s.oldest_age_millis == 0 && s.records == 0),
            "an idle shard reads 0, so absent and zero stay distinguishable"
        );
    }

    #[test]
    fn the_window_opens_on_append_not_on_seal() {
        let t = UnreplicatedTracker::new(1);
        t.on_append(0);
        std::thread::sleep(std::time::Duration::from_millis(30));
        // Still unsealed — the pre-seal term is exactly what the seal-relative
        // metric cannot see, so it must be visible here.
        assert!(
            age(&t, 0) >= 25,
            "an unsealed record's age must already count; got {}",
            age(&t, 0)
        );
        assert_eq!(records(&t, 0), 1);
    }

    #[test]
    fn sealing_does_not_reset_the_age() {
        let t = UnreplicatedTracker::new(1);
        t.on_append(0);
        std::thread::sleep(std::time::Duration::from_millis(30));
        let before = age(&t, 0);
        t.on_seal(0, "part-a", 1);
        let after = age(&t, 0);
        assert!(
            after >= before,
            "seal must inherit the append instant, else the window restarts at 0 \
             (before={before} after={after})"
        );
        assert_eq!(
            records(&t, 0),
            1,
            "a sealed-but-unshipped record still counts"
        );
    }

    #[test]
    fn the_window_closes_only_when_the_part_is_durable() {
        let t = UnreplicatedTracker::new(1);
        t.on_append(0);
        t.on_seal(0, "part-a", 1);
        std::thread::sleep(std::time::Duration::from_millis(20));
        assert!(
            age(&t, 0) > 0,
            "still pending while the upload is in flight"
        );

        let latency = t.on_upload_done(0, "part-a").expect("part was pending");
        assert!(
            latency.as_millis() >= 20,
            "end-to-end latency is measured from the APPEND: {latency:?}"
        );
        // The positive control's other half: it must fall back to 0.
        assert_eq!(age(&t, 0), 0, "durable ⇒ the window closes");
        assert_eq!(records(&t, 0), 0);
    }

    #[test]
    fn the_oldest_record_sets_the_window_not_the_newest() {
        let t = UnreplicatedTracker::new(1);
        t.on_append(0);
        t.on_seal(0, "old", 1);
        std::thread::sleep(std::time::Duration::from_millis(40));
        t.on_append(0); // a fresh record in a new active part
        let a = age(&t, 0);
        assert!(
            a >= 35,
            "the window is bounded by the OLDEST pending record, not the newest; got {a}"
        );
        assert_eq!(records(&t, 0), 2);
    }

    #[test]
    fn shipping_the_old_part_leaves_the_newer_one_pending() {
        let t = UnreplicatedTracker::new(1);
        t.on_append(0);
        t.on_seal(0, "old", 1);
        t.on_append(0);
        t.on_upload_done(0, "old");
        assert_eq!(records(&t, 0), 1, "the still-active record remains pending");
        assert!(age(&t, 0) < 1_000);
    }

    #[test]
    fn a_shard_outside_the_pinned_range_is_still_reported() {
        // A missing row would hide a real backlog, which is the failure mode
        // pinning exists to prevent — so it must not reappear at the edge.
        let t = UnreplicatedTracker::new(1);
        t.on_append(7);
        let snap = t.snapshot();
        assert!(snap.iter().any(|s| s.shard == 7 && s.records == 1));
        assert!(snap.windows(2).all(|w| w[0].shard <= w[1].shard), "sorted");
    }

    #[test]
    fn upload_done_for_an_unknown_part_is_none_not_a_panic() {
        let t = UnreplicatedTracker::new(1);
        assert!(t.on_upload_done(0, "nope").is_none());
        t.on_append(0);
        assert!(t.on_upload_done(0, "still-nope").is_none());
    }
}
