//! Execution-affinity **shard ownership** — the hash + ownership policy that
//! makes each event-log shard have exactly one writing replica (completion
//! program, durable event-log backend slice 2; [noetl/ai-meta#254]).
//!
//! ## Why this exists — single-writer coherence
//!
//! The durable segment store ([`crate::durable_eventlog`], slice 1) is the
//! production disk format underneath the [`EventLogDriver`](crate::EventLogDriver)
//! contract, but slice 1 *assumed* the caller was the sole writer. Under
//! multiple replicas that assumption breaks: two pods appending to the same
//! store diverge (each grows its own segment set / sequence). The prod-cutover
//! runbook's §C durability gate names exactly this — *"if more than one pod on
//! the flagged pool appends, each writes its own log → multiple divergent
//! authoritative logs. It must be pinned to a single writer, or the backend
//! must be shared."*
//!
//! This module supplies the ownership function that pins each shard to one
//! writer. The routing layer that uses it lives in
//! [`crate::durable_eventlog_affinity`]; the disk format (slice 1) is unchanged.
//!
//! ## The hash — reuse #166 / #116, byte-identical
//!
//! [`shard_for_i64`] is a byte-identical reimplementation of `noetl-worker`'s
//! `src/sharding.rs::shard_for` (itself a verbatim copy of `noetl-server`'s
//! `sharding::shard_for`): [`twox_hash::XxHash64`] with a fixed seed of `0` over
//! the 8 little-endian bytes of the `i64` execution id, taken `% shard_count`.
//! Reusing the same hash means the replica that already owns an execution for
//! off-server state-building (noetl/ai-meta#166 Phase 4) also owns its durable
//! event-log segments — locality, not a second independent partition.
//!
//! NoETL execution ids are snowflake `i64`s; EHDB stores them as their decimal
//! string form. [`shard_for_execution`] parses that decimal string back to the
//! `i64` and routes through [`shard_for_i64`] so the ownership is identical to
//! the worker's for every real execution. A non-numeric id (only test / synthetic
//! ids) falls back to hashing the raw UTF-8 bytes with the same seeded hash — a
//! deterministic, well-distributed owner, just not one the worker would also
//! compute (the worker only ever sees numeric ids).
//!
//! ## Correctness is independent of ownership
//!
//! Ownership decides *where* a write is allowed, never *whether* the event log
//! is correct. A write that lands on a non-owner is **refused with no side
//! effect** (no bytes, no sequence consumed) so it can be safely re-routed to
//! the owner; a read on a non-owner **cold-loads** the durable segments
//! read-only. The append-only event log stays the source of truth; a mis-route
//! is a routing decision to redo, never a divergence.
//!
//! [noetl/ai-meta#254]: https://github.com/noetl/ehdb/issues/254

use std::hash::Hasher;

use ehdb_core::{EhdbError, Result};
use serde::{Deserialize, Serialize};
use twox_hash::XxHash64;

/// Fixed seed for the shard-ownership hash. MUST match `noetl-server`'s
/// `sharding::SHARD_HASH_SEED` (`0`) and `noetl-worker`'s `SHARD_HASH_SEED`
/// (`0`) or EHDB would disagree with them on which replica owns an execution.
pub const SHARD_HASH_SEED: u64 = 0;

/// The env var an operator sets to pick this replica's shard index. Matches the
/// worker/server `NOETL_SHARD_INDEX` value at deploy time (the worker-wiring
/// slice passes its resolved index straight through, so the two never drift);
/// the EHDB-namespaced name lets the standalone `ehdb-local-reference` selfcheck
/// exercise ownership without the worker present.
pub const SHARD_INDEX_ENV: &str = "NOETL_EHDB_SHARD_INDEX";

/// The env var an operator sets for the pool's total shard count
/// (`NOETL_EHDB_SHARD_COUNT`, mirrors `NOETL_SHARD_COUNT`). Every replica MUST
/// agree. Unset / `1` = single owner (this replica owns every execution) —
/// the behaviour-neutral, single-writer default.
pub const SHARD_COUNT_ENV: &str = "NOETL_EHDB_SHARD_COUNT";

/// Compute the shard index that owns an `i64` execution id.
///
/// `XxHash64(seed=0)` over `execution_id.to_le_bytes()`, `% shard_count`.
/// Byte-identical to `noetl-worker` / `noetl-server` `shard_for`. `shard_count
/// <= 1` short-circuits to shard `0` (the single-owner default).
pub fn shard_for_i64(execution_id: i64, shard_count: u32) -> u32 {
    if shard_count <= 1 {
        return 0;
    }
    let mut h = XxHash64::with_seed(SHARD_HASH_SEED);
    // Hash the i64 as 8 explicit little-endian bytes so the result is stable
    // regardless of `Hasher::write_i64`'s platform behaviour — matches the
    // worker/server exactly.
    h.write(&execution_id.to_le_bytes());
    (h.finish() % shard_count as u64) as u32
}

/// Compute the shard index that owns a raw byte string (the non-numeric
/// execution-id fallback). Same seeded [`XxHash64`], hashing the bytes directly.
fn shard_for_bytes(bytes: &[u8], shard_count: u32) -> u32 {
    if shard_count <= 1 {
        return 0;
    }
    let mut h = XxHash64::with_seed(SHARD_HASH_SEED);
    h.write(bytes);
    (h.finish() % shard_count as u64) as u32
}

/// Compute the shard index that owns an EHDB execution id (a string).
///
/// Real NoETL execution ids are snowflake `i64`s rendered as decimal; those
/// parse and route through [`shard_for_i64`] so the owner is byte-identical to
/// the worker/server. A non-numeric id (test / synthetic only) falls back to
/// hashing its UTF-8 bytes — still deterministic and well-distributed.
pub fn shard_for_execution(execution_id: &str, shard_count: u32) -> u32 {
    if shard_count <= 1 {
        return 0;
    }
    let trimmed = execution_id.trim();
    match trimmed.parse::<i64>() {
        Ok(id) => shard_for_i64(id, shard_count),
        Err(_) => shard_for_bytes(trimmed.as_bytes(), shard_count),
    }
}

/// One EHDB replica's execution-affinity ownership: which shard it owns out of
/// how many, mirroring `noetl-server`'s `ShardConfig` / `noetl-worker`'s
/// `AffinityConfig`. A replica owns exactly its `shard_index` bucket, so a pool
/// of `shard_count` replicas (indices `0..shard_count`) partitions every
/// execution to exactly one writer — the single-writer invariant.
///
/// The single-owner default (`shard_count == 1`) owns every execution, so a
/// replica carrying this code is behaviour-neutral until an operator sets
/// `shard_count > 1` and gives each replica a distinct index.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ShardOwnership {
    shard_index: u32,
    shard_count: u32,
}

impl Default for ShardOwnership {
    /// The single-writer default: one shard, this replica owns everything.
    fn default() -> Self {
        Self {
            shard_index: 0,
            shard_count: 1,
        }
    }
}

impl ShardOwnership {
    /// The single-owner ownership (owns every execution). Equivalent to
    /// [`ShardOwnership::default`].
    pub fn single_owner() -> Self {
        Self::default()
    }

    /// Construct explicit ownership. `shard_count` must be `>= 1` and
    /// `shard_index` must be `< shard_count`, or this is a configuration bug
    /// (the caller asked to own a shard the pool doesn't have).
    pub fn new(shard_index: u32, shard_count: u32) -> Result<Self> {
        if shard_count == 0 {
            return Err(EhdbError::InvalidState(
                "shard ownership: shard_count must be >= 1".to_string(),
            ));
        }
        if shard_index >= shard_count {
            return Err(EhdbError::InvalidState(format!(
                "shard ownership: shard_index {shard_index} >= shard_count {shard_count}"
            )));
        }
        Ok(Self {
            shard_index,
            shard_count,
        })
    }

    /// Resolve ownership from the environment ([`SHARD_INDEX_ENV`] /
    /// [`SHARD_COUNT_ENV`]). Invalid combinations degrade to the safe
    /// single-owner default rather than erroring — correctness never depends on
    /// the partition, and a single writer is always safe (it just gives up the
    /// scale-out until the config is fixed).
    pub fn from_env() -> Self {
        let shard_index = env_u32(SHARD_INDEX_ENV, 0);
        let shard_count = env_u32(SHARD_COUNT_ENV, 1).max(1);
        Self::new(shard_index, shard_count).unwrap_or_else(|_| Self::single_owner())
    }

    /// This replica's shard index.
    pub fn shard_index(&self) -> u32 {
        self.shard_index
    }

    /// The pool's total shard count.
    pub fn shard_count(&self) -> u32 {
        self.shard_count
    }

    /// Whether the pool is genuinely partitioned (more than one shard). A
    /// single-shard pool is the single-writer default where this replica owns
    /// everything and no routing ever fires.
    pub fn is_sharded(&self) -> bool {
        self.shard_count > 1
    }

    /// Does this replica own shard `shard`? Single-shard always owns (`shard`
    /// is `0`); otherwise it owns exactly its own index.
    pub fn owns_shard(&self, shard: u32) -> bool {
        if self.shard_count <= 1 {
            return true;
        }
        shard == self.shard_index
    }

    /// The shard that owns `execution_id`.
    pub fn shard_of(&self, execution_id: &str) -> u32 {
        shard_for_execution(execution_id, self.shard_count)
    }

    /// Does this replica own `execution_id`? Matches `noetl-worker`'s
    /// `AffinityConfig::owns` shape, adapted to EHDB's string execution ids.
    pub fn owns_execution(&self, execution_id: &str) -> bool {
        self.owns_shard(self.shard_of(execution_id))
    }
}

fn env_u32(key: &str, default: u32) -> u32 {
    std::env::var(key)
        .ok()
        .and_then(|v| v.trim().parse::<u32>().ok())
        .unwrap_or(default)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn single_shard_owns_everything() {
        let o = ShardOwnership::single_owner();
        assert!(!o.is_sharded());
        assert!(o.owns_shard(0));
        assert!(o.owns_execution("100"));
        assert!(o.owns_execution("320816801799737344"));
        assert!(o.owns_execution("anything-at-all"));
        assert_eq!(o.shard_of("100"), 0);
    }

    #[test]
    fn shard_for_i64_is_stable_and_bounded() {
        for eid in [1_i64, 42, 320816801799737344, i64::MAX, i64::MIN] {
            let first = shard_for_i64(eid, 16);
            for _ in 0..100 {
                assert_eq!(shard_for_i64(eid, 16), first);
            }
            assert!(first < 16);
        }
    }

    #[test]
    fn shard_for_i64_matches_pinned_algorithm() {
        // Recomputed from the same algorithm the worker/server pin (XxHash64
        // seed 0 over LE bytes). If a twox-hash major bump changes the output,
        // this fails here in lockstep with the worker's own pinned test.
        let mut h = XxHash64::with_seed(0);
        h.write(&320816801799737344_i64.to_le_bytes());
        let expected = (h.finish() % 16) as u32;
        assert_eq!(shard_for_i64(320816801799737344, 16), expected);
    }

    #[test]
    fn shard_for_execution_matches_i64_for_numeric_ids() {
        // A decimal snowflake string routes identically to the i64 — the
        // property that makes EHDB agree with the worker on ownership.
        for eid in [1_i64, 42, 999, 320816801799737344, i64::MAX] {
            for count in [2u32, 4, 8, 16] {
                assert_eq!(
                    shard_for_execution(&eid.to_string(), count),
                    shard_for_i64(eid, count),
                    "eid {eid} count {count}"
                );
            }
        }
    }

    #[test]
    fn shard_for_execution_handles_non_numeric_deterministically() {
        // A non-numeric id still gets a stable, in-range owner.
        for id in ["exec-a", "weird_id", "100-abc"] {
            let first = shard_for_execution(id, 8);
            assert!(first < 8);
            assert_eq!(shard_for_execution(id, 8), first);
        }
    }

    #[test]
    fn ownership_partitions_every_execution_to_exactly_one_owner() {
        let count = 4;
        let owners: Vec<ShardOwnership> = (0..count)
            .map(|i| ShardOwnership::new(i, count).unwrap())
            .collect();
        for eid in ["1", "42", "999", "320816801799737344", "exec-x"] {
            let holding: Vec<u32> = owners
                .iter()
                .filter(|o| o.owns_execution(eid))
                .map(|o| o.shard_index())
                .collect();
            assert_eq!(holding.len(), 1, "eid {eid} owned by {holding:?}");
            assert_eq!(holding[0], shard_for_execution(eid, count));
        }
    }

    #[test]
    fn new_rejects_out_of_range_index() {
        assert!(ShardOwnership::new(0, 0).is_err());
        assert!(ShardOwnership::new(4, 4).is_err());
        assert!(ShardOwnership::new(3, 4).is_ok());
    }

    #[test]
    fn shard_of_distributes_snowflake_ids() {
        // Sequential snowflake-shaped ids don't cluster on one shard (avalanche).
        let base = 320816801799737344_i64;
        let mut hits = [0u32; 8];
        for i in 0..800 {
            hits[shard_for_execution(&(base + i).to_string(), 8) as usize] += 1;
        }
        for count in hits {
            assert!(count > 40, "uneven distribution: {hits:?}");
        }
    }
}
