//! **Retention as drop-partition** (RFC §2.1 "retention = drop whole partitions,
//! not row deletes"; L0.5) — the pure planning half.
//!
//! For D1 the retention unit is a whole **part**: a part is retained or dropped
//! as a unit (never a row-level delete), matching VM's drop-partition model. The
//! planner picks the parts entirely below a sort-key floor; the engine executes
//! the drop (manifest swap) + reclaims their objects.
//!
//! Separately, **orphan reclaim** (in the engine) vacuums part objects the
//! manifest no longer references — chiefly the superseded source parts a merge
//! (L0.3) leaves behind.

use crate::catalog::Manifest;

/// A retention plan: the parts to drop and the resulting reclamation floor.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RetentionPlan {
    /// Ids of parts to drop (entirely below the floor).
    pub drop_ids: Vec<String>,
    /// The new `reclaimed_through` (highest dropped sort-key value) — never
    /// decreases.
    pub reclaimed_through: u64,
}

impl RetentionPlan {
    /// Whether the plan drops anything.
    pub fn is_empty(&self) -> bool {
        self.drop_ids.is_empty()
    }
}

/// Plan retention that drops every part whose `max_sequence < keep_from_sequence`
/// (i.e. entirely below the floor). A part straddling the floor (min < floor <=
/// max) is **kept** whole — drop-partition never splits a part. The new
/// `reclaimed_through` is the highest sort-key value that leaves the log.
pub fn plan_retention(manifest: &Manifest, keep_from_sequence: u64) -> RetentionPlan {
    let mut drop_ids = Vec::new();
    let mut dropped_through = manifest.reclaimed_through;
    for part in &manifest.parts {
        if part.max_sequence < keep_from_sequence {
            drop_ids.push(part.part_id.clone());
            dropped_through = dropped_through.max(part.max_sequence);
        }
    }
    RetentionPlan {
        drop_ids,
        reclaimed_through: dropped_through,
    }
}

/// Convenience: plan retention that keeps at least the last `keep_last_records`
/// sort-key values, dropping whole parts below that window. Returns an empty plan
/// if the log is smaller than the window.
pub fn plan_keep_last(manifest: &Manifest, keep_last_records: u64) -> RetentionPlan {
    let tip = manifest.max_sequence();
    let keep_from = tip.saturating_sub(keep_last_records).saturating_add(1);
    plan_retention(manifest, keep_from)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::{Manifest, PartMeta, ReplicaLocation, SparseIndex};

    fn part(id: &str, min: u64, max: u64) -> PartMeta {
        PartMeta {
            part_id: id.to_string(),
            partition: 0,
            min_sequence: min,
            max_sequence: max,
            record_count: max - min + 1,
            byte_size: 100,
            replicas: vec![ReplicaLocation {
                replica: "replica-0".into(),
                key: format!("parts/{id}"),
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

    fn manifest(ranges: &[(&str, u64, u64)]) -> Manifest {
        let mut m = Manifest::empty("d1_event_log");
        for (id, min, max) in ranges {
            m.push_part(part(id, *min, *max));
        }
        m
    }

    #[test]
    fn drops_parts_entirely_below_the_floor() {
        let m = manifest(&[("a", 1, 8), ("b", 9, 16), ("c", 17, 24)]);
        // keep_from = 17 → parts a,b (max 8,16 < 17) drop; c kept.
        let plan = plan_retention(&m, 17);
        assert_eq!(plan.drop_ids, vec!["a".to_string(), "b".to_string()]);
        assert_eq!(plan.reclaimed_through, 16);
    }

    #[test]
    fn keeps_a_part_straddling_the_floor() {
        let m = manifest(&[("a", 1, 8), ("b", 9, 16)]);
        // keep_from = 12 → part b straddles (9 <= 12 <= 16) so it is KEPT whole;
        // only a (max 8 < 12) drops.
        let plan = plan_retention(&m, 12);
        assert_eq!(plan.drop_ids, vec!["a".to_string()]);
        assert_eq!(plan.reclaimed_through, 8);
    }

    #[test]
    fn keep_last_window() {
        let m = manifest(&[("a", 1, 8), ("b", 9, 16), ("c", 17, 24)]);
        // tip = 24, keep last 8 → keep_from = 17 → drop a,b.
        let plan = plan_keep_last(&m, 8);
        assert_eq!(plan.drop_ids, vec!["a".to_string(), "b".to_string()]);
        assert_eq!(plan.reclaimed_through, 16);
    }

    #[test]
    fn nothing_dropped_when_window_covers_all() {
        let m = manifest(&[("a", 1, 8), ("b", 9, 16)]);
        assert!(plan_keep_last(&m, 100).is_empty());
    }
}
