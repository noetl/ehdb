//! A deterministic fault-injection seam for the write path.
//!
//! # Why this exists
//!
//! Two real properties of this crate were **untestable**, and both are on the
//! path that matters most:
//!
//! 1. **`append_record` remembers a dedupe key only AFTER the write succeeds**
//!    (noetl/ai-meta#313). Remembering first would let a failed append poison its
//!    key, so the retry the failure exists to invite would be answered "already
//!    present" for a record that is not there — silent loss. A mutation moving
//!    `remember` earlier left every test green, because nothing could make an
//!    append fail.
//! 2. **`ingest_append_failed` / the ingest failure path** (ehdb#345). That
//!    counter exists because a full volume and a serde-incompatible record
//!    produced byte-identical symptoms and no signal at all — a production writer
//!    sat `Ready`, 0 restarts, 0 ERROR lines while every command publish failed.
//!    Reaching it in a test previously required actually filling a disk.
//!
//! The usual tricks do not work here: on Unix a write to an already-open
//! descriptor survives `chmod` and `unlink`, and the one step that *can* fail on
//! permissions (`ensure_writer`) runs before the point under test.
//!
//! # Why it is always compiled rather than feature-gated
//!
//! A `required-features` test target is skipped by a plain `cargo test
//! --workspace`, which is what CI runs — so the tests would exist and never
//! execute. That is the same defect class this seam was built to close, one level
//! up, so it is not an acceptable trade.
//!
//! The cost is **one relaxed atomic load per append**, against a ~4 ms `fsync`.
//! That is not a measurable trade; guaranteed coverage is.
//!
//! # Why it is inert
//!
//! The counter starts at 0 and only [`fail_next_appends`] raises it. Nothing in
//! any binary calls that — it is reachable from tests alone — so a production
//! process can never arm it. Same argument as `Dataset::dedupe_key` returning
//! `None`: inert by construction, not by configuration.

use std::sync::atomic::{AtomicUsize, Ordering};

static FAIL_NEXT_APPENDS: AtomicUsize = AtomicUsize::new(0);

/// Make the next `n` `PartWriter::append` calls fail with a storage error.
///
/// Each failure consumes one. Call with `0` to disarm.
pub fn fail_next_appends(n: usize) {
    FAIL_NEXT_APPENDS.store(n, Ordering::SeqCst);
}

/// How many injected failures remain. For a test's own positive control: a test
/// that arms the seam should assert it was actually consumed, or an injection
/// that silently did nothing reads as the code being correct.
pub fn pending_injected_failures() -> usize {
    FAIL_NEXT_APPENDS.load(Ordering::SeqCst)
}

/// Consume one injected failure if any are armed.
pub(crate) fn should_fail_append() -> bool {
    // Fast path: one relaxed load, and it is 0 in every process that has not
    // explicitly armed the seam.
    if FAIL_NEXT_APPENDS.load(Ordering::Relaxed) == 0 {
        return false;
    }
    // Armed — take one, without going below zero under concurrency.
    FAIL_NEXT_APPENDS
        .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| {
            if n == 0 {
                None
            } else {
                Some(n - 1)
            }
        })
        .is_ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_seam_is_disarmed_by_default_and_consumes_exactly_what_it_is_given() {
        // Not parallel-safe with other tests in this module by construction, so
        // this is the only test here and it drives the whole lifecycle.
        assert_eq!(pending_injected_failures(), 0, "must start disarmed");
        assert!(
            !should_fail_append(),
            "a disarmed seam never fails an append"
        );

        fail_next_appends(2);
        assert!(should_fail_append());
        assert!(should_fail_append());
        assert!(
            !should_fail_append(),
            "the seam must not fail a third append — an injection that outlives \
             its count would make an unrelated later test fail mysteriously"
        );
        assert_eq!(pending_injected_failures(), 0);

        fail_next_appends(1);
        fail_next_appends(0);
        assert!(!should_fail_append(), "0 must disarm");
    }
}
