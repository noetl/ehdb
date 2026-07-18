//! Dataset definitions for L0 — the FIXED, compiled-in schemas (RFC §0.1).
//!
//! L0.1 implements **D1, the event log**. Each dataset has a fixed schema, a
//! fixed sort key, and a fixed partition function — the properties that let L0
//! be purpose-built (no runtime DDL, no discovered schema). Adding a dataset is
//! a deliberate compiled-in change here, never a runtime operation.

use std::fmt::Debug;
use std::hash::Hasher;

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use twox_hash::XxHash64;

/// The **fixed, compiled-in shape of one L0 dataset** (RFC §0.1). The shared
/// part / catalog / merge / replication engine ([`crate::engine::L0Engine`]) is
/// generic over this trait: a dataset supplies its record schema, its fixed sort
/// key, its fixed partition function, and its fixed inverted-index dimension —
/// nothing else. There is no runtime schema, no DDL, no arbitrary index; a new
/// dataset is a new `impl Dataset`, a deliberate compiled-in change.
///
/// **Contract:** records are appended in **ascending [`sort_key`](Dataset::sort_key)
/// order within a partition** (the single writer guarantees this) — the sparse
/// index, MinMax pruning, and merge all rely on it.
pub trait Dataset: 'static {
    /// The dataset's fixed record schema.
    type Record: Serialize + DeserializeOwned + Clone + PartialEq + Debug + Send + 'static;

    /// The dataset id, used in substrate keys + the manifest (e.g.
    /// `d1_event_log`).
    const NAME: &'static str;

    /// The record's fixed sort key (D1: `global_sequence`). Ascending within a
    /// partition.
    fn sort_key(record: &Self::Record) -> u64;

    /// The partition (shard) a record belongs to (D1: `shard_for(execution_id)`).
    fn partition(record: &Self::Record, shard_count: u32) -> u32;

    /// The record's fixed inverted-index dimension value — the key the per-part /
    /// per-granule blooms filter on (D1: `execution_id`). Return `""` to opt out
    /// of bloom indexing for the dataset.
    fn index_key(record: &Self::Record) -> &str;

    /// The partition a **read** targets, given the index value it filters on
    /// (D1: `shard_for(execution_id)` — the read dimension is the partition
    /// dimension). This is what lets a per-index lookup prune to one partition.
    fn read_partition(index_value: &str, shard_count: u32) -> u32;
}

/// **D1 — the event log** (`noetl.event`). Sort key = `global_sequence`;
/// partition = `shard_for(execution_id)`; index dim = `execution_id`.
#[derive(Debug, Clone, Copy)]
pub struct D1EventLog;

impl Dataset for D1EventLog {
    type Record = EventRecord;
    const NAME: &'static str = DATASET_D1_EVENT_LOG;

    fn sort_key(record: &EventRecord) -> u64 {
        record.global_sequence
    }
    fn partition(record: &EventRecord, shard_count: u32) -> u32 {
        shard_for_execution(&record.execution_id, shard_count)
    }
    fn index_key(record: &EventRecord) -> &str {
        &record.execution_id
    }
    fn read_partition(execution_id: &str, shard_count: u32) -> u32 {
        shard_for_execution(execution_id, shard_count)
    }
}

/// Dataset id for D1, the append-only execution event log (`noetl.event`). Sort
/// key = `global_sequence`; partition = `shard_for(execution_id)`; access
/// patterns = append / range-scan-after-seq / per-execution replay.
pub const DATASET_D1_EVENT_LOG: &str = "d1_event_log";

/// Default partition (shard) count for D1. `1` = single owner (single-writer
/// default). Kept configurable so the pruning proof can spread executions
/// across shards and show non-matching partitions are skipped with zero I/O.
pub const DEFAULT_SHARD_COUNT: u32 = 1;

/// Fixed seed for the partition hash — `0`, byte-identical to
/// `noetl-worker` / `noetl-server` `sharding::shard_for` and
/// `ehdb-reference::affinity::shard_for_i64`. The two MUST agree on which shard
/// owns an execution or single-writer coherence breaks.
const SHARD_HASH_SEED: u64 = 0;

/// One D1 event-log record — the fixed schema. Mirrors the #254
/// `EventLogRecordView` / `SegmentFrame::Event` fields so an L0 part is a
/// pruneable, range-readable #254 segment.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EventRecord {
    /// The monotonic gapless global sequence assigned at append time (the D1
    /// sort key). Ascending within a partition (a single writer serializes
    /// appends), so a part's `[min, max]` sequence range is contiguous-enough
    /// for range pruning.
    pub global_sequence: u64,
    /// The execution this event belongs to (the scoped-replay dimension and the
    /// partition input).
    pub execution_id: String,
    /// The transaction id carried through the append contract.
    pub transaction_id: String,
    /// The opaque event payload (noetl-internal; never a secret value).
    pub payload: String,
}

impl EventRecord {
    /// Construct a D1 record.
    pub fn new(
        global_sequence: u64,
        execution_id: impl Into<String>,
        transaction_id: impl Into<String>,
        payload: impl Into<String>,
    ) -> Self {
        Self {
            global_sequence,
            execution_id: execution_id.into(),
            transaction_id: transaction_id.into(),
            payload: payload.into(),
        }
    }
}

/// Compute the partition (shard) that owns an execution id, byte-identical to
/// `noetl-server`/`noetl-worker`/`ehdb-reference::affinity::shard_for_execution`:
/// `XxHash64(seed=0)` over the id — the decimal `i64` snowflake as 8 explicit
/// little-endian bytes when numeric, else the raw UTF-8 bytes — taken
/// `% shard_count`. `shard_count <= 1` short-circuits to `0` (single-owner
/// default).
pub fn shard_for_execution(execution_id: &str, shard_count: u32) -> u32 {
    if shard_count <= 1 {
        return 0;
    }
    let trimmed = execution_id.trim();
    match trimmed.parse::<i64>() {
        Ok(id) => {
            let mut h = XxHash64::with_seed(SHARD_HASH_SEED);
            h.write(&id.to_le_bytes());
            (h.finish() % shard_count as u64) as u32
        }
        Err(_) => {
            let mut h = XxHash64::with_seed(SHARD_HASH_SEED);
            h.write(trimmed.as_bytes());
            (h.finish() % shard_count as u64) as u32
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn single_owner_short_circuits_to_zero() {
        assert_eq!(shard_for_execution("12345", 1), 0);
        assert_eq!(shard_for_execution("anything", 0), 0);
    }

    #[test]
    fn partitioning_is_deterministic_and_bounded() {
        for count in [2u32, 4, 8] {
            for id in 0..200i64 {
                let s = shard_for_execution(&id.to_string(), count);
                assert!(s < count, "shard {s} out of range for count {count}");
                // Deterministic: same id → same shard every call.
                assert_eq!(s, shard_for_execution(&id.to_string(), count));
            }
        }
    }

    #[test]
    fn numeric_ids_route_by_i64_le_bytes() {
        // A numeric id routes through the i64-LE-bytes path (matches worker);
        // a non-numeric id routes through the raw-bytes path. Both are stable.
        let a = shard_for_execution("1001", 4);
        let b = shard_for_execution("  1001  ", 4); // trimmed → same
        assert_eq!(a, b);
    }
}
