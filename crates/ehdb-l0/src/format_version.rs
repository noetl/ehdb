//! **On-disk format version** — the guard that stops two builds opening the same
//! layout at incompatible versions (noetl/ai-meta#332).
//!
//! # Why this has to exist before the engine is embedded
//!
//! Today the engine runs in one process and its consumers talk to it over a wire
//! protocol, so a version difference between them is a protocol question and the
//! wire either works or visibly does not.
//!
//! Embedding removes that. The server and the worker would each `open()` an
//! engine directly, and they are pinned to the EHDB repo by **git rev** — 70
//! commits apart, measured 2026-09-08. Two processes writing the same on-disk
//! layout at 70 commits of divergence is a corruption vector, and nothing in a
//! git-rev pin would even notice.
//!
//! So the substrate carries a version marker, and `open` refuses a mismatch.
//!
//! # What this is NOT
//!
//! ⚠ Not `manifest_version`, which already exists and is a *monotonic sequence
//! number* for manifest snapshots — "which snapshot", not "which layout". The two
//! are unrelated and conflating them would make this guard read as already-solved.

use crate::substrate::DurableSubstrate;
use ehdb_core::{EhdbError, Result};

/// The layout version this build reads and writes.
///
/// ⚠ Bump this in the **same change** as any incompatible change to how bytes
/// are laid out on the substrate. A bump makes older builds refuse the data
/// rather than misread it, which is the entire point: refusing is recoverable,
/// misreading is not.
pub const FORMAT_VERSION: u32 = 1;

/// The substrate key holding the marker. A plain, boring name, because an
/// operator staring at a directory should be able to tell what it is.
pub const FORMAT_VERSION_KEY: &str = "FORMAT_VERSION";

/// What `verify_or_initialise` concluded. Returned rather than swallowed so a
/// caller can log which case it hit — "created" and "matched" are both success
/// and they mean very different things operationally.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FormatCheck {
    /// No marker present: this substrate is new, and the marker was written.
    Initialised,
    /// A marker was present and matches this build.
    Matched,
}

/// Decide what an observed marker means. Pure, so the policy is testable without
/// a filesystem.
///
/// `observed` is `None` when the substrate carries no marker yet.
pub fn evaluate(observed: Option<u32>, ours: u32) -> Result<FormatCheck> {
    match observed {
        None => Ok(FormatCheck::Initialised),
        Some(v) if v == ours => Ok(FormatCheck::Matched),
        Some(v) => Err(EhdbError::Storage(format!(
            "on-disk format version {v} does not match this build's {ours}: \
             refusing to open. Two builds writing one layout at different \
             versions corrupts it silently; refusing is the recoverable outcome. \
             (noetl/ai-meta#332)"
        ))),
    }
}

/// Read the marker, and write it if absent. Refuses on mismatch.
///
/// ⚠ Idempotent, and it does NOT overwrite a marker it disagrees with — that
/// would turn the guard into a rubber stamp.
pub fn verify_or_initialise(substrate: &dyn DurableSubstrate) -> Result<FormatCheck> {
    let observed = read_marker(substrate)?;
    let outcome = evaluate(observed, FORMAT_VERSION)?;
    if outcome == FormatCheck::Initialised {
        substrate.put_if_absent(FORMAT_VERSION_KEY, FORMAT_VERSION.to_string().as_bytes())?;
    }
    Ok(outcome)
}

fn read_marker(substrate: &dyn DurableSubstrate) -> Result<Option<u32>> {
    // ⚠ `exists` first, and `get_all` rather than `get_range`.
    //
    // The first version of this read did `get_range(key, 0, 32)` with
    // `Err(_) => Ok(None)`. Two things were wrong with it, and the tests caught
    // both:
    //
    //   * `get_range` treats a range past the end as an ERROR, not a short read
    //     (deliberately — see its doc comment), and the marker is 1-3 bytes. So
    //     the read failed every time.
    //   * the blanket `Err(_) => Ok(None)` then turned that — and a genuinely
    //     corrupt marker, and a substrate I/O failure — into "absent", which
    //     re-initialises. A guard that reports "no marker" whenever it cannot
    //     read one is not a guard.
    //
    // Absence is now asked for explicitly; every other error propagates.
    if !substrate.exists(FORMAT_VERSION_KEY)? {
        return Ok(None);
    }
    let bytes = substrate.get_all(FORMAT_VERSION_KEY)?;
    if bytes.is_empty() {
        return Err(EhdbError::Storage(
            "on-disk format marker is present but empty: refusing to open rather \
             than assuming it is ours (noetl/ai-meta#332)"
                .to_string(),
        ));
    }
    let text = String::from_utf8_lossy(&bytes);
    let trimmed = text.trim();
    trimmed.parse::<u32>().map(Some).map_err(|_| {
        // ⚠ Unparseable is NOT treated as absent. A corrupt marker would then be
        // silently overwritten with ours, which is the failure this guards.
        EhdbError::Storage(format!(
            "on-disk format marker is unreadable ({trimmed:?}): refusing to open \
             rather than assuming it is ours (noetl/ai-meta#332)"
        ))
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::substrate::InMemorySubstrate;
    use std::sync::Arc;

    #[test]
    fn a_matching_version_opens() {
        assert_eq!(
            evaluate(Some(FORMAT_VERSION), FORMAT_VERSION),
            Ok(FormatCheck::Matched)
        );
    }

    #[test]
    fn a_mismatched_version_refuses() {
        let err = evaluate(Some(FORMAT_VERSION + 1), FORMAT_VERSION).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("does not match"), "unhelpful message: {msg}");
        // ⚠ And it must refuse in BOTH directions — an older on-disk version is
        // just as unsafe for a newer build as the reverse.
        assert!(evaluate(Some(0), FORMAT_VERSION).is_err());
    }

    #[test]
    fn an_absent_marker_initialises() {
        assert_eq!(evaluate(None, FORMAT_VERSION), Ok(FormatCheck::Initialised));
    }

    /// End-to-end over a real substrate: first open writes, second open matches,
    /// and a substrate carrying someone else's version refuses.
    #[test]
    fn round_trip_over_a_substrate_then_refuses_a_foreign_marker() {
        let s: Arc<dyn DurableSubstrate> = Arc::new(InMemorySubstrate::new("fmt-test"));
        assert_eq!(
            verify_or_initialise(s.as_ref()).unwrap(),
            FormatCheck::Initialised
        );
        // Second open sees its own marker.
        assert_eq!(
            verify_or_initialise(s.as_ref()).unwrap(),
            FormatCheck::Matched
        );

        // A substrate written by a different build.
        let other: Arc<dyn DurableSubstrate> = Arc::new(InMemorySubstrate::new("fmt-test"));
        other.put_if_absent(FORMAT_VERSION_KEY, b"999").unwrap();
        let err = verify_or_initialise(other.as_ref()).unwrap_err();
        assert!(
            err.to_string().contains("999"),
            "must name the version it found"
        );
    }

    /// A substrate whose reads fail — the case that distinguishes "there is no
    /// marker" from "I could not find out".
    #[derive(Debug)]
    struct UnreadableSubstrate;

    impl DurableSubstrate for UnreadableSubstrate {
        fn put_if_absent(&self, _k: &str, _b: &[u8]) -> Result<bool> {
            Ok(true)
        }
        fn put_overwrite(&self, _k: &str, _b: &[u8]) -> Result<()> {
            Ok(())
        }
        fn get_range(&self, _k: &str, _o: u64, _l: u64) -> Result<Vec<u8>> {
            Err(EhdbError::Storage("disk on fire".into()))
        }
        fn get_all(&self, _k: &str) -> Result<Vec<u8>> {
            Err(EhdbError::Storage("disk on fire".into()))
        }
        fn exists(&self, _k: &str) -> Result<bool> {
            // The object IS there; we simply cannot read it.
            Ok(true)
        }
        fn list_prefix(&self, _p: &str) -> Result<Vec<String>> {
            Ok(Vec::new())
        }
        fn delete(&self, _k: &str) -> Result<()> {
            Ok(())
        }
    }

    /// ⚠⚠ A read failure must PROPAGATE, never read as "no marker".
    ///
    /// The first version of `read_marker` had a blanket `Err(_) => Ok(None)`.
    /// That is the same shape as the bare `return` in `serve_ingest` that hid a
    /// full disk for hours: an error swallowed into a benign-looking value. Here
    /// it would mean a substrate we cannot read gets INITIALISED as if it were
    /// new — writing our marker over someone else's layout, which is precisely
    /// the corruption this module exists to prevent.
    ///
    /// This test exists because the mutation restoring that swallow left every
    /// other test in this module green.
    #[test]
    fn a_read_failure_propagates_and_never_reads_as_absent() {
        let s = UnreadableSubstrate;
        let err = verify_or_initialise(&s)
            .expect_err("an unreadable substrate must not be treated as new");
        assert!(
            err.to_string().contains("disk on fire"),
            "the underlying failure must survive, not be replaced: {err}"
        );
    }

    /// ⚠⚠ The gate must be ON THE CHOKEPOINT, not merely available.
    ///
    /// Every `L0Engine::open*` funnels into `open_replicated_with_metrics`, so a
    /// check placed anywhere else is one an `open` variant can miss. This is the
    /// reachability question, not the existence question: the module compiling is
    /// not the module running.
    #[test]
    fn the_gate_is_wired_into_the_open_chokepoint() {
        let src = include_str!("engine.rs");
        let at = src
            .find("pub fn open_replicated_with_metrics")
            .expect("chokepoint not found — the extraction broke");
        // ⚠ Window widened 2000 → 5000 (resilient-core Phase 3), then
        // 5000 → 9000 (M4 wiring, 2026-09-19). The chokepoint keeps growing:
        // the failure-domain check, and now the M4 region-survival check, sit
        // between the format gate and the manifest load, which pushed
        // `load_durable_manifest` out of the old windows and tripped this
        // guard's own "widen it" branch each time.
        //
        // Widening preserves the property — gate present, and BEFORE the
        // manifest read — and does not relax it: the ORDER assertion below is
        // what carries the meaning, and a window that merely contains both
        // still fails if the gate moved after the load. Measured at the time of
        // widening: gate at +849, manifest load at +5787.
        //
        // ⚠ A character window over source is a proximity proxy, not the
        // property. It survives because the order check is the real assertion;
        // if this needs widening a fourth time, replace it with a parse rather
        // than a larger number.
        let body: String = src[at..].chars().take(9000).collect();
        assert!(
            body.contains("verify_or_initialise"),
            "the format gate is not called from the open chokepoint; an engine \
             could open a layout it cannot read (noetl/ai-meta#332)"
        );
        // And it must run before the manifest is loaded — reading a foreign
        // layout is the thing being prevented, so order matters.
        let gate = body.find("verify_or_initialise").unwrap();
        let load = body
            .find("load_durable_manifest")
            .expect("manifest load not found in the window — widen it");
        assert!(
            gate < load,
            "the gate must run BEFORE the manifest is read, or it validates a \
             layout that has already been parsed"
        );
    }

    /// ⚠ A corrupt marker must refuse, not be treated as absent — otherwise the
    /// guard silently overwrites it with ours, which is the failure it exists to
    /// prevent.
    #[test]
    fn an_unreadable_marker_refuses_rather_than_reinitialising() {
        let s: Arc<dyn DurableSubstrate> = Arc::new(InMemorySubstrate::new("fmt-test"));
        s.put_if_absent(FORMAT_VERSION_KEY, b"not-a-number")
            .unwrap();
        assert!(verify_or_initialise(s.as_ref()).is_err());
    }
}
