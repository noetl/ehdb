//! **Stored per-shard apply cursor** (noetl/ai-meta#332 step 3).
//!
//! # What this replaces
//!
//! The only cursor in the codebase is `ehdb-reference`'s `ProjectionCheckpoint`,
//! and it is **derived by replaying the whole log**:
//!
//! ```text
//! let records = self.replay_all(&runtime)?;   // O(log length)
//! records.iter().map(|e| e.global_sequence).max()
//! ```
//!
//! Correct, durable, exactly-once-safe — and it makes recovery pay a full scan
//! *before* it can start replaying the tail. Fine at today's volume; the first
//! thing to stop being fine.
//!
//! # Why a stored pointer is a small addition here
//!
//! `ehdb-l0`'s engine sequence is monotonic across executions, survives a
//! restart, and does not reissue after one (pinned in `projection::tests`). And
//! `L0Engine::read_partition_after(shard, after_seq)` is already the tail read.
//! So the cursor is a durable `u64` per shard and recovery becomes
//! `load` + `read_partition_after` — no scan.
//!
//! # The fallback is preserved on purpose
//!
//! An absent cursor reads as `0`, which replays everything. Losing the pointer
//! costs time, never correctness — and that asymmetry is what makes it safe to
//! store a hint about durable data.

use ehdb_core::{EhdbError, Result};

use crate::substrate::DurableSubstrate;

/// Substrate key for one shard's cursor. A mutable pointer, so
/// [`DurableSubstrate::put_overwrite`] — not `put_if_absent`, which is for the
/// immutable parts.
pub fn cursor_key(shard: u32) -> String {
    format!("CURSOR/shard-{shard}")
}

/// The highest sequence applied for `shard`. `0` when none is stored.
///
/// ⚠ Absence is asked for explicitly and every other error propagates. A blanket
/// `Err(_) => Ok(0)` would turn an unreadable substrate into "replay from the
/// beginning", which is *safe* here — but it would also hide the substrate
/// failure, and the same shape in the format guard nearly shipped as a silent
/// overwrite of someone else's layout.
pub fn load(substrate: &dyn DurableSubstrate, shard: u32) -> Result<u64> {
    let key = cursor_key(shard);
    if !substrate.exists(&key)? {
        return Ok(0);
    }
    let bytes = substrate.get_all(&key)?;
    let text = String::from_utf8_lossy(&bytes);
    text.trim().parse::<u64>().map_err(|_| {
        EhdbError::Storage(format!(
            "cursor for shard {shard} is unreadable ({:?}): refusing to guess",
            text.trim()
        ))
    })
}

/// Advance the stored cursor.
///
/// ⚠ **Monotonic.** A lower value is ignored rather than written. Two workers
/// racing, or a retry carrying a stale value, must never move the cursor
/// backwards — that would silently re-apply a window that was already applied,
/// and the reason a stored cursor is safe at all is that it only ever
/// under-claims progress.
pub fn advance(substrate: &dyn DurableSubstrate, shard: u32, applied_through: u64) -> Result<u64> {
    let current = load(substrate, shard)?;
    if applied_through <= current {
        return Ok(current);
    }
    substrate.put_overwrite(&cursor_key(shard), applied_through.to_string().as_bytes())?;
    Ok(applied_through)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::substrate::InMemorySubstrate;

    fn sub() -> InMemorySubstrate {
        InMemorySubstrate::new("cursor-test")
    }

    /// The fallback the design deliberately keeps: no cursor means replay
    /// everything. Costs time, never correctness.
    #[test]
    fn an_absent_cursor_reads_as_zero_and_replays_everything() {
        assert_eq!(load(&sub(), 0).unwrap(), 0);
        assert_eq!(load(&sub(), 7).unwrap(), 0);
    }

    #[test]
    fn advance_then_load_round_trips_per_shard() {
        let s = sub();
        advance(&s, 0, 42).unwrap();
        advance(&s, 1, 7).unwrap();
        assert_eq!(load(&s, 0).unwrap(), 42);
        assert_eq!(load(&s, 1).unwrap(), 7, "cursors must be keyed BY SHARD");
        assert_eq!(load(&s, 2).unwrap(), 0, "an untouched shard is unaffected");
    }

    /// ⚠⚠ Monotonicity is the exactly-once property. A cursor that moves
    /// backwards re-applies an already-applied window; one that moves forward
    /// past what was applied SKIPS events, which is worse. Only forward, and
    /// only to a value the caller has actually applied through.
    #[test]
    fn the_cursor_never_moves_backwards() {
        let s = sub();
        advance(&s, 0, 100).unwrap();
        let after_stale = advance(&s, 0, 50).unwrap();
        assert_eq!(
            after_stale, 100,
            "a stale advance must be ignored, not written"
        );
        assert_eq!(load(&s, 0).unwrap(), 100);
        // Equal is also a no-op, not a rewrite.
        assert_eq!(advance(&s, 0, 100).unwrap(), 100);
        // And forward still works.
        assert_eq!(advance(&s, 0, 101).unwrap(), 101);
    }

    /// ⚠ An unreadable cursor must refuse, not silently read as 0. Reading as 0
    /// is *safe* (it replays everything) but it hides a substrate failure — and
    /// the identical shape in the format guard nearly shipped as a silent
    /// overwrite of another build's layout.
    #[test]
    fn an_unreadable_cursor_refuses_rather_than_guessing() {
        let s = sub();
        s.put_overwrite(&cursor_key(0), b"not-a-number").unwrap();
        assert!(load(&s, 0).is_err());
    }

    #[test]
    fn shards_do_not_share_a_key() {
        assert_ne!(cursor_key(0), cursor_key(1));
        assert!(cursor_key(3).contains('3'));
    }
}
