//! Append-time idempotency: a redelivered record is acknowledged, not appended.
//!
//! **noetl/ai-meta#313.** Redelivery was never a no-op. A re-appended record
//! either minted a fresh sort key (a duplicate in the log) or reused one that no
//! longer advanced the shard tail — and the engine's own comment on that case
//! says such a record "lands behind any follower cursor and is silently never
//! delivered". So the two outcomes of a retry were a visible duplicate or an
//! invisible loss, and there was no third.
//!
//! # Bounded, and the bound is the honest part
//!
//! This is a **recent-window** index, not a complete one. It remembers the last
//! `capacity` keys per shard and forgets older ones, so a redelivery that
//! arrives after `capacity` intervening appends on that shard is NOT deduped and
//! becomes a duplicate exactly as today.
//!
//! That is a deliberate trade, and it is sized against the thing it exists for:
//! a relay retry arrives seconds later, not days. A complete index would have to
//! be durable and unbounded — it would grow without limit and would need its own
//! recovery path, which is a larger change than the defect warrants.
//!
//! ⚠ The window is therefore a **capacity**, not a guarantee, and
//! `dedupe_window_evictions` counts every key it forgets. A non-zero eviction
//! rate is the signal that the window is too small for the redelivery pattern —
//! without it, an undersized window would look exactly like a working one.

use std::collections::{HashMap, VecDeque};

/// Default keys remembered per shard. ~65k covers a relay retry storm several
/// orders of magnitude larger than any observed one while costing a few MB.
pub const DEFAULT_DEDUPE_CAPACITY: usize = 65_536;

/// What an append decided about a record's idempotency key.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DedupeVerdict {
    /// Not seen in the window — append it.
    Fresh,
    /// Already present at this sort key — acknowledge that position, append
    /// nothing.
    Duplicate(u64),
}

/// Per-shard bounded recent-key index.
#[derive(Debug, Default)]
pub struct DedupeIndex {
    capacity: usize,
    shards: HashMap<u32, ShardWindow>,
    evictions: u64,
}

#[derive(Debug, Default)]
struct ShardWindow {
    /// key -> the sort key the record was appended at.
    seen: HashMap<String, u64>,
    /// Insertion order, for eviction. Holds the same keys as `seen`.
    order: VecDeque<String>,
}

impl DedupeIndex {
    /// A window remembering `capacity` keys per shard. `capacity == 0` disables
    /// dedupe entirely — every record reads as `Fresh`, which is byte-for-byte
    /// today's behaviour and is what makes this safe to ship switched off.
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            capacity,
            shards: HashMap::new(),
            evictions: 0,
        }
    }

    pub fn enabled(&self) -> bool {
        self.capacity > 0
    }

    pub fn evictions(&self) -> u64 {
        self.evictions
    }

    /// Number of keys currently remembered for a shard. Test/observability only.
    pub fn len(&self, shard: u32) -> usize {
        self.shards.get(&shard).map_or(0, |w| w.seen.len())
    }

    pub fn is_empty(&self) -> bool {
        self.shards.values().all(|w| w.seen.is_empty())
    }

    /// Has this key been seen on this shard, and at what position?
    pub fn check(&self, shard: u32, key: &str) -> DedupeVerdict {
        if !self.enabled() {
            return DedupeVerdict::Fresh;
        }
        match self.shards.get(&shard).and_then(|w| w.seen.get(key)) {
            Some(&seq) => DedupeVerdict::Duplicate(seq),
            None => DedupeVerdict::Fresh,
        }
    }

    /// Record that `key` landed at `sort_key` on `shard`.
    ///
    /// ⚠ Called only after the append actually happened. Recording before would
    /// make a failed append poison the key: the retry that the failure exists to
    /// invite would then be answered "already present" for a record that is not
    /// there — silent loss, which is the exact class this module removes.
    pub fn remember(&mut self, shard: u32, key: &str, sort_key: u64) {
        if !self.enabled() {
            return;
        }
        let cap = self.capacity;
        let w = self.shards.entry(shard).or_default();
        if w.seen.insert(key.to_string(), sort_key).is_none() {
            w.order.push_back(key.to_string());
        }
        while w.order.len() > cap {
            if let Some(old) = w.order.pop_front() {
                // Only evict if it is still the same entry we queued.
                if w.seen.remove(&old).is_some() {
                    self.evictions += 1;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_disabled_window_never_deduplicates() {
        let mut d = DedupeIndex::with_capacity(0);
        d.remember(0, "e1", 7);
        assert_eq!(
            d.check(0, "e1"),
            DedupeVerdict::Fresh,
            "capacity 0 must be byte-for-byte today's behaviour, or 'off' is not \
             a real rollback"
        );
    }

    #[test]
    fn a_remembered_key_reports_its_original_position() {
        let mut d = DedupeIndex::with_capacity(8);
        d.remember(3, "e1", 42);
        assert_eq!(d.check(3, "e1"), DedupeVerdict::Duplicate(42));
        assert_eq!(
            d.check(4, "e1"),
            DedupeVerdict::Fresh,
            "shards are independent; a key on one must not mask the other"
        );
    }

    #[test]
    fn the_window_evicts_in_insertion_order_and_counts_it() {
        let mut d = DedupeIndex::with_capacity(2);
        d.remember(0, "a", 1);
        d.remember(0, "b", 2);
        d.remember(0, "c", 3);
        assert_eq!(
            d.check(0, "a"),
            DedupeVerdict::Fresh,
            "oldest must be evicted"
        );
        assert_eq!(d.check(0, "b"), DedupeVerdict::Duplicate(2));
        assert_eq!(d.check(0, "c"), DedupeVerdict::Duplicate(3));
        assert_eq!(
            d.evictions(),
            1,
            "an undersized window must be visible; without this counter it looks \
             exactly like a working one"
        );
        assert_eq!(d.len(0), 2, "the window must not grow past its capacity");
    }

    #[test]
    fn re_remembering_a_key_does_not_double_queue_it() {
        let mut d = DedupeIndex::with_capacity(4);
        d.remember(0, "a", 1);
        d.remember(0, "a", 1);
        assert_eq!(d.len(0), 1);
        assert_eq!(d.evictions(), 0);
    }
}
