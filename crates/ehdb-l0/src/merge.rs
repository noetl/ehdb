//! The **background small→big merge/compaction engine** (RFC §2.1 "background
//! tiered merge"; L0.3). VM's core: as small immutable parts accumulate, merge
//! adjacent ones into a bigger immutable part so read fan-out and per-part
//! overhead stay bounded.
//!
//! Scoped by the noetl-only invariant (RFC §2.6 "per-dataset fixed merge
//! policy"): a **fixed, compiled-in** policy — merge a contiguous run of small
//! parts within a partition when the run gets long — not a general
//! cost-based compactor.
//!
//! **Contiguity is load-bearing.** Parts within a partition are non-overlapping,
//! contiguous sort-key ranges. The planner only ever merges a *contiguous run of
//! small parts* (never skipping an already-big part between them), so the merged
//! part's range stays contiguous and never overlaps a neighbor — the read-path
//! invariant holds after merge exactly as before.

use crate::catalog::{Manifest, PartMeta};

/// The fixed D1 merge policy.
#[derive(Debug, Clone, Copy)]
pub struct MergePolicy {
    /// A part is a merge candidate ("small") when its `record_count` is at most
    /// this — i.e. an un-merged sealed part. Merge outputs exceed it and are not
    /// re-merged, giving a clean one-level small→big tier (multi-level tiering is
    /// a later refinement).
    pub small_part_max_records: u64,
    /// Merge a partition's contiguous small run once it reaches this length.
    pub trigger_run_len: usize,
    /// Merge at most this many adjacent small parts at once (VM's merge
    /// multiplier analog).
    pub max_merge_parts: usize,
}

impl MergePolicy {
    /// D1 defaults: small = a single sealed part (≤ `seal_max_records`), trigger
    /// at 4 consecutive small parts, merge up to 8 at once.
    pub fn d1(seal_max_records: u64) -> Self {
        Self {
            small_part_max_records: seal_max_records,
            trigger_run_len: 4,
            max_merge_parts: 8,
        }
    }

    /// Whether a part is a "small" merge candidate under this policy. A
    /// local-only part (not yet uploaded) is excluded — merge operates on durable
    /// parts so the post-merge manifest swap is cold-load-consistent.
    fn is_small_candidate(&self, part: &PartMeta) -> bool {
        part.is_durable() && part.record_count <= self.small_part_max_records
    }
}

/// One merge to perform: the partition and the adjacent source part ids.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MergePlan {
    /// The partition being compacted.
    pub partition: u32,
    /// The contiguous source part ids to merge (≥ 2), in sort-key order.
    pub source_ids: Vec<String>,
}

/// Plan the next merge, or `None` if no partition has a long-enough contiguous
/// run of small durable parts. Deterministic: scans partitions in ascending id,
/// and within a partition the first qualifying contiguous small run.
pub fn plan_next_merge(manifest: &Manifest, policy: &MergePolicy) -> Option<MergePlan> {
    // Group part ids by partition, sorted by sort key.
    let mut partitions: Vec<u32> = manifest.parts.iter().map(|p| p.partition).collect();
    partitions.sort_unstable();
    partitions.dedup();

    for partition in partitions {
        let mut parts: Vec<&PartMeta> = manifest
            .parts
            .iter()
            .filter(|p| p.partition == partition)
            .collect();
        parts.sort_by_key(|p| p.min_sequence);

        // Find the first maximal run of consecutive small candidates.
        let mut i = 0;
        while i < parts.len() {
            if !policy.is_small_candidate(parts[i]) {
                i += 1;
                continue;
            }
            let run_start = i;
            while i < parts.len() && policy.is_small_candidate(parts[i]) {
                i += 1;
            }
            let run_len = i - run_start;
            if run_len >= policy.trigger_run_len {
                let take = run_len.min(policy.max_merge_parts);
                let source_ids = parts[run_start..run_start + take]
                    .iter()
                    .map(|p| p.part_id.clone())
                    .collect();
                return Some(MergePlan {
                    partition,
                    source_ids,
                });
            }
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::{PartMeta, ReplicaLocation, SparseIndex};

    fn small(partition: u32, min: u64, max: u64) -> PartMeta {
        PartMeta {
            part_id: format!("shard-{partition}-seq-{min:020}-{max:020}"),
            partition,
            min_sequence: min,
            max_sequence: max,
            record_count: max - min + 1,
            byte_size: 100,
            replicas: vec![ReplicaLocation {
                replica: "replica-0".into(),
                key: "parts/x".into(),
            }],
            local_path: None,
            sparse_index: SparseIndex {
                granule_size: 4,
                marks: vec![],
            },
            execution_bloom: None,
            granule_blooms: vec![],
        }
    }

    fn big(partition: u32, min: u64, max: u64) -> PartMeta {
        let mut p = small(partition, min, max);
        p.record_count = 1000; // above small_part_max_records
        p
    }

    #[test]
    fn plans_a_contiguous_small_run_when_long_enough() {
        let policy = MergePolicy {
            small_part_max_records: 8,
            trigger_run_len: 4,
            max_merge_parts: 8,
        };
        let mut m = Manifest::empty("d1_event_log");
        for k in 0..5u64 {
            m.push_part(small(0, k * 8 + 1, k * 8 + 8)); // 5 small parts, contiguous
        }
        let plan = plan_next_merge(&m, &policy).expect("a merge is planned");
        assert_eq!(plan.partition, 0);
        assert_eq!(plan.source_ids.len(), 5); // all 5 (< max 8)
    }

    #[test]
    fn does_not_plan_below_trigger() {
        let policy = MergePolicy {
            small_part_max_records: 8,
            trigger_run_len: 4,
            max_merge_parts: 8,
        };
        let mut m = Manifest::empty("d1_event_log");
        for k in 0..3u64 {
            m.push_part(small(0, k * 8 + 1, k * 8 + 8)); // only 3 < trigger 4
        }
        assert!(plan_next_merge(&m, &policy).is_none());
    }

    #[test]
    fn never_merges_across_a_big_part() {
        // small,small,BIG,small,small,small,small — the two runs are [0,1] (len 2)
        // and [3..7] (len 4). Only the second run reaches the trigger; the big
        // part is never in a plan, so no merged range straddles it.
        let policy = MergePolicy {
            small_part_max_records: 8,
            trigger_run_len: 4,
            max_merge_parts: 8,
        };
        let mut m = Manifest::empty("d1_event_log");
        m.push_part(small(0, 1, 8));
        m.push_part(small(0, 9, 16));
        m.push_part(big(0, 17, 200));
        m.push_part(small(0, 201, 208));
        m.push_part(small(0, 209, 216));
        m.push_part(small(0, 217, 224));
        m.push_part(small(0, 225, 232));
        let plan = plan_next_merge(&m, &policy).expect("second run qualifies");
        assert_eq!(plan.source_ids.len(), 4);
        // All sources are from the post-big run (min_sequence >= 201).
        assert!(plan
            .source_ids
            .iter()
            .all(|id| id.contains("00000000000000000201")
                || id.contains("00000000000000000209")
                || id.contains("00000000000000000217")
                || id.contains("00000000000000000225")));
    }

    #[test]
    fn local_only_parts_are_not_merge_candidates() {
        let policy = MergePolicy {
            small_part_max_records: 8,
            trigger_run_len: 2,
            max_merge_parts: 8,
        };
        let mut m = Manifest::empty("d1_event_log");
        for k in 0..4u64 {
            let mut p = small(0, k * 8 + 1, k * 8 + 8);
            p.replicas = Vec::new(); // local-only → not durable → not a candidate
            p.local_path = Some("/x".into());
            m.push_part(p);
        }
        assert!(plan_next_merge(&m, &policy).is_none());
    }
}
