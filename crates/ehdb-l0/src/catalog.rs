//! The **ClickHouse-MergeTree-style meta-catalog** (RFC §2.5) — the "table
//! format over object storage" VictoriaMetrics' engine lacks.
//!
//! Because noetl's datasets are predefined (RFC §0.1), the catalog is
//! purpose-built and **fixed per dataset**: no DDL, no discovered schema, no
//! cost-based planner. Two small structures, both cached in RAM and themselves
//! durable objects in the object store:
//!
//! 1. [`Manifest`] — one [`PartMeta`] row per immutable part: where it is
//!    (`object_uri` / `local_path`), its partition, its `[min_sequence,
//!    max_sequence]` sort-key range (ClickHouse MinMax skip index), and its
//!    record count + byte size. The Iceberg-manifest / ClickHouse `system.parts`
//!    analog — the pointer catalog VM does not provide. Versioned so readers see
//!    a consistent snapshot.
//! 2. [`SparseIndex`] — one [`GranuleMark`] per **granule** (block of frames)
//!    over the dataset's fixed sort key → the granule's byte offset (its "mark",
//!    ClickHouse `primary.idx` + `.mrk`). Lets a lookup binary-search to the
//!    granule containing the target sequence and ranged-GET only that block.
//!
//! For D1 the fixed sort key is `global_sequence` and the fixed partition is
//! `shard_for(execution_id)`; these structures are **generated for D1**, not
//! discovered at runtime.

use serde::{Deserialize, Serialize};

/// One entry in a part's [`SparseIndex`]: the start of a granule (a block of
/// consecutive frames) — the granule's first sort-key value and the byte offset
/// (the "mark") of that frame's magic within the part.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GranuleMark {
    /// The sort-key value (`global_sequence` for D1) of the first record in this
    /// granule.
    pub first_sequence: u64,
    /// Byte offset of that first record's frame within the part — the mark a
    /// ranged GET seeks to.
    pub byte_offset: u64,
    /// Number of records in this granule (the last granule may be short).
    pub record_count: u32,
}

/// A part's sparse primary index over the fixed sort key. `marks` is ascending
/// by `first_sequence`; a lookup binary-searches it to the granule that may
/// contain the target, then the reader ranged-GETs from that mark.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SparseIndex {
    /// Target records per granule (the last granule may hold fewer). One index
    /// entry per granule keeps the index `O(records / granule_size)`.
    pub granule_size: u32,
    /// One mark per granule, ascending by `first_sequence`.
    pub marks: Vec<GranuleMark>,
}

impl SparseIndex {
    /// Byte offset to begin a ranged read for a lookup targeting `target_seq`
    /// (inclusive). Returns the mark of the **last granule whose
    /// `first_sequence <= target_seq`** — the granule that may contain
    /// `target_seq` (the preceding granules are entirely below it and are
    /// skipped). If `target_seq` precedes the first granule, returns `0` (read
    /// from the start). If the index is empty, returns `0`.
    ///
    /// This is the ClickHouse primary-index binary search: `O(log granules)`, no
    /// scan. The caller reads from the returned offset to the part's end and
    /// filters the (few) records below `target_seq` out of the first granule.
    pub fn locate(&self, target_seq: u64) -> u64 {
        if self.marks.is_empty() {
            return 0;
        }
        // Binary search for the rightmost mark with first_sequence <= target_seq.
        let mut lo = 0usize;
        let mut hi = self.marks.len(); // exclusive
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            if self.marks[mid].first_sequence <= target_seq {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        if lo == 0 {
            // target precedes the first granule → read from the start.
            0
        } else {
            self.marks[lo - 1].byte_offset
        }
    }
}

/// One immutable part's metadata — a row in the [`Manifest`]. A part is durable
/// once it has an `object_uri`; before the async upload lands it is local-only
/// (`local_path` set, `object_uri` `None`), served from the hot tier.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PartMeta {
    /// Deterministic part id: `shard-<partition>-seq-<min>-<max>`. Stable from
    /// content, so a re-upload lands the same object key (idempotent).
    pub part_id: String,
    /// The partition this part belongs to (D1: `shard_for(execution_id)`).
    pub partition: u32,
    /// Lowest sort-key value in the part (D1: min `global_sequence`).
    pub min_sequence: u64,
    /// Highest sort-key value in the part (D1: max `global_sequence`) — the
    /// MinMax skip index used for range pruning.
    pub max_sequence: u64,
    /// Number of records in the part.
    pub record_count: u64,
    /// On-disk byte size of the part (frame bytes).
    pub byte_size: u64,
    /// Object-store key once the async uploader has shipped the part
    /// (`None` while local-only). A cold-load reads only parts with this set.
    pub object_uri: Option<String>,
    /// Local hot-tier file path while the part is resident on this node
    /// (`None` on a cold-loaded node). Reads prefer this (no object-store I/O).
    pub local_path: Option<String>,
    /// The part's sparse primary index (granule marks).
    pub sparse_index: SparseIndex,
}

impl PartMeta {
    /// Whether this part's `[min, max]` sort-key range can contain any record
    /// strictly greater than `after_seq` — the MinMax skip-index prune. `false`
    /// means the whole part is below the cursor and is skipped with zero I/O.
    pub fn overlaps_after(&self, after_seq: u64) -> bool {
        self.max_sequence > after_seq
    }
}

/// The per-dataset manifest: the list of parts that exist and where. Versioned
/// (a new version on each seal/upload) so a reader sees a consistent snapshot.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct Manifest {
    /// The dataset this manifest catalogs (e.g. `d1_event_log`).
    pub dataset: String,
    /// Monotonic manifest version, bumped on each mutation.
    pub version: u64,
    /// One row per immutable part, insertion-ordered.
    pub parts: Vec<PartMeta>,
}

impl Manifest {
    /// A fresh, empty manifest for `dataset`.
    pub fn empty(dataset: impl Into<String>) -> Self {
        Self {
            dataset: dataset.into(),
            version: 0,
            parts: Vec::new(),
        }
    }

    /// Add a part row and bump the version.
    pub fn push_part(&mut self, part: PartMeta) {
        self.parts.push(part);
        self.version += 1;
    }

    /// **Manifest prune** (RFC §2.5 step 1): the parts of `partition` whose
    /// sort-key range can hold a record after `after_seq`. Parts in other
    /// partitions and parts entirely at/below `after_seq` are skipped here —
    /// **before any part I/O** — so a lookup never touches a non-matching part.
    pub fn prune(&self, partition: u32, after_seq: u64) -> Vec<&PartMeta> {
        let mut hits: Vec<&PartMeta> = self
            .parts
            .iter()
            .filter(|p| p.partition == partition && p.overlaps_after(after_seq))
            .collect();
        // Serve parts in sort-key order.
        hits.sort_by_key(|p| p.min_sequence);
        hits
    }

    /// All parts of `partition`, sorted by sort key — used by cold-load full
    /// replay (no cursor).
    pub fn parts_in_partition(&self, partition: u32) -> Vec<&PartMeta> {
        let mut hits: Vec<&PartMeta> = self
            .parts
            .iter()
            .filter(|p| p.partition == partition)
            .collect();
        hits.sort_by_key(|p| p.min_sequence);
        hits
    }

    /// The **durable view** written to the object store: only parts that have an
    /// `object_uri` (a cold-load must never point at a part that isn't uploaded
    /// yet), with `local_path` cleared (it is meaningless on another node).
    pub fn durable_view(&self) -> Manifest {
        Manifest {
            dataset: self.dataset.clone(),
            version: self.version,
            parts: self
                .parts
                .iter()
                .filter(|p| p.object_uri.is_some())
                .map(|p| PartMeta {
                    local_path: None,
                    ..p.clone()
                })
                .collect(),
        }
    }

    /// Highest sort-key value across all parts (0 if empty) — the global
    /// sequence tip a cold-loaded node resumes from.
    pub fn max_sequence(&self) -> u64 {
        self.parts.iter().map(|p| p.max_sequence).max().unwrap_or(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn idx(marks: &[(u64, u64)], gsize: u32) -> SparseIndex {
        SparseIndex {
            granule_size: gsize,
            marks: marks
                .iter()
                .map(|&(first_sequence, byte_offset)| GranuleMark {
                    first_sequence,
                    byte_offset,
                    record_count: gsize,
                })
                .collect(),
        }
    }

    #[test]
    fn locate_binary_searches_to_containing_granule() {
        // Granules starting at seq 1, 9, 17, 25 at byte offsets 0, 400, 800, 1200.
        let index = idx(&[(1, 0), (9, 400), (17, 800), (25, 1200)], 8);
        // target within the first granule
        assert_eq!(index.locate(1), 0);
        assert_eq!(index.locate(5), 0);
        // target at a granule boundary → that granule
        assert_eq!(index.locate(9), 400);
        // target inside the third granule
        assert_eq!(index.locate(20), 800);
        // target beyond the last mark → the last granule (may contain it)
        assert_eq!(index.locate(999), 1200);
        // target below the first granule → start
        assert_eq!(index.locate(0), 0);
    }

    #[test]
    fn locate_empty_index_is_zero() {
        assert_eq!(idx(&[], 8).locate(42), 0);
    }

    #[test]
    fn prune_skips_other_partitions_and_below_cursor() {
        let mut m = Manifest::empty("d1_event_log");
        let mk = |part_id: &str, partition, min_sequence, max_sequence| PartMeta {
            part_id: part_id.to_string(),
            partition,
            min_sequence,
            max_sequence,
            record_count: (max_sequence - min_sequence + 1),
            byte_size: 100,
            object_uri: Some(format!("parts/{part_id}")),
            local_path: None,
            sparse_index: idx(&[(min_sequence, 0)], 8),
        };
        m.push_part(mk("p0a", 0, 1, 10));
        m.push_part(mk("p0b", 0, 11, 20));
        m.push_part(mk("p1a", 1, 3, 15));

        // partition 0, after seq 12 → only p0b (p0a is entirely <= 12; p1a is
        // another partition).
        let hits = m.prune(0, 12);
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].part_id, "p0b");

        // partition 1 lookup never touches partition 0's parts.
        let hits = m.prune(1, 0);
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].part_id, "p1a");
    }

    #[test]
    fn durable_view_drops_local_only_parts() {
        let mut m = Manifest::empty("d1_event_log");
        m.push_part(PartMeta {
            part_id: "uploaded".into(),
            partition: 0,
            min_sequence: 1,
            max_sequence: 5,
            record_count: 5,
            byte_size: 50,
            object_uri: Some("parts/uploaded".into()),
            local_path: Some("/local/uploaded".into()),
            sparse_index: idx(&[(1, 0)], 8),
        });
        m.push_part(PartMeta {
            part_id: "local_only".into(),
            partition: 0,
            min_sequence: 6,
            max_sequence: 9,
            record_count: 4,
            byte_size: 40,
            object_uri: None,
            local_path: Some("/local/local_only".into()),
            sparse_index: idx(&[(6, 0)], 8),
        });
        let durable = m.durable_view();
        assert_eq!(durable.parts.len(), 1);
        assert_eq!(durable.parts[0].part_id, "uploaded");
        assert!(durable.parts[0].local_path.is_none());
    }
}
