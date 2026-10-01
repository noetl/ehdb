//! Chain-certified projections — the in-engine certificate (noetl/ai-meta#366).
//!
//! A rolling SHA-256 over each execution's single-root chain, so a projection's
//! validity is an O(1) digest comparison instead of re-folding the chain.
//!
//! **Feature-gated (`chain-cert`), off by default.** With the feature off this
//! module does not compile in and `sha2` is not a dependency.
//!
//! # Why the digest is taken here
//!
//! [`crate::part::PartWriter::append`] already serialises each record to the
//! exact bytes it writes. Digesting *those* bytes costs only SHA-256 block
//! processing — no second serialisation. noetl/server#480 measured the
//! re-serialising variant at -17% append throughput; #481 measured digesting
//! stored bytes at 10x cheaper. This module is the latter.
//!
//! # Why chunking is by chain position and not by commit batch
//!
//! Rolling up whatever arrived in one commit batch is **boundary-dependent**:
//! `H(TAG‖prev‖b1‖b2) != H(TAG‖H(TAG‖prev‖b1)‖b2)`. Commit boundaries are a
//! function of arrival timing and load, so two replicas that batched
//! differently would disagree on the digest for the *same* chain. Chunking by
//! chain position ([`CHUNK_EVENTS`]) is derived from the chain index, so every
//! producer derives the same boundaries. See
//! `rolling_up_by_commit_batch_is_not_reproducible` in noetl/server#481.
//!
//! # What this depends on upstream
//!
//! Digesting stored bytes is only reproducible if the stored bytes are
//! deterministic. noetl/server's `Event` serialises its timestamp truncated to
//! whole microseconds (`serialize_timestamp_micros`) for exactly this reason —
//! without it, two producers rendering the same instant at different
//! sub-microsecond precision store different bytes and derive different
//! digests (spec guardrail A8).

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};

use sha2::{Digest, Sha256};

/// Domain separation for the chain hash. Shared with noetl/server's
/// `chain_cert::DOMAIN_TAG`; changing it forks every chain.
pub const DOMAIN_TAG: &[u8] = b"noetl.ehdb.chain-cert.v1";

/// Events per digest advance.
///
/// Chosen against the recorded corpus (chains of ~36-174 events): a chunk
/// larger than a typical chain leaves short executions with nothing sealed and
/// therefore nothing to compare in O(1), which makes the whole path inert —
/// the "exists but never runs" failure. Guarded by
/// `a_short_chain_still_seals_at_least_one_chunk`.
pub const CHUNK_EVENTS: u32 = 8;

/// **Calibration seam — benchmark only.** How many times each record's bytes
/// are absorbed.
///
/// `1` in every real configuration. A benchmark sets it to `k` to plant a
/// regression of exactly `k` times the certificate's cost, which is how the
/// instrument proves it can resolve a signal of that magnitude before its
/// numbers are trusted: a single A/B difference could not (noetl/ehdb#378).
///
/// Follows the same shape as [`crate::fault`]'s append seam — disarmed unless
/// something explicitly arms it, one relaxed load otherwise, and that load is
/// inside every measurement rather than excluded from it.
static CALIBRATION_DOSE: AtomicUsize = AtomicUsize::new(1);

/// Arm the calibration seam. Benchmarks only; `1` restores normal behaviour.
pub fn set_calibration_dose(k: usize) {
    CALIBRATION_DOSE.store(k, Ordering::Relaxed);
}

/// Current calibration dose.
pub fn calibration_dose() -> usize {
    CALIBRATION_DOSE.load(Ordering::Relaxed)
}

/// The 44-byte certificate: `(chain_len, chain_digest)` for one execution.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ChainCert {
    pub chain_len: u32,
    pub chain_digest: [u8; 32],
}

impl ChainCert {
    /// `chain_len` (4) + `chain_digest` (32); the execution id (8) is the key.
    pub const WIRE_BYTES: usize = 4 + 32;
}

/// One execution's rolling chain state.
#[derive(Debug, Clone)]
struct ChunkRoller {
    hasher: Sha256,
    len: u32,
    sealed_len: u32,
    sealed_digest: Option<[u8; 32]>,
    open: bool,
}

impl ChunkRoller {
    fn new() -> Self {
        Self {
            hasher: Sha256::new(),
            len: 0,
            sealed_len: 0,
            sealed_digest: None,
            open: false,
        }
    }

    #[inline]
    fn absorb(&mut self, body: &[u8]) {
        if !self.open {
            self.hasher = Sha256::new();
            self.hasher.update(DOMAIN_TAG);
            if let Some(p) = self.sealed_digest {
                self.hasher.update(p);
            }
            self.open = true;
        }
        self.hasher.update(body);
        self.len += 1;
        if self.len % CHUNK_EVENTS == 0 {
            // `finalize_fixed_reset` reuses the hasher in place. Constructing a
            // fresh Sha256 and moving the old one out copies ~100 bytes of
            // state per seal, which on a hardware-SHA host costs more than the
            // init+finalize the chunking exists to save.
            let d: [u8; 32] =
                sha2::digest::FixedOutputReset::finalize_fixed_reset(&mut self.hasher).into();
            self.sealed_digest = Some(d);
            self.sealed_len = self.len;
            self.open = false;
        }
    }

    fn certificate(&self) -> Option<ChainCert> {
        self.sealed_digest.map(|d| ChainCert {
            chain_len: self.sealed_len,
            chain_digest: d,
        })
    }
}

/// Per-execution chain state for one partition's writer.
///
/// A `PartWriter` serialises records from many executions, and the certificate
/// is a **per-execution** quantity, so the roller is keyed by the dataset's
/// index dimension (`execution_id` for D1). The map lookup is the honest cost
/// of per-execution chains and is included in every measurement.
#[derive(Debug, Default)]
pub struct ChainCertRegistry {
    rollers: HashMap<String, ChunkRoller>,
    absorbed: u64,
}

impl ChainCertRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Absorb the bytes just written for `key`'s record.
    ///
    /// ⚠ ONE map lookup on the hot path. The obvious
    /// `contains_key` + `get_mut` spelling hashes the key twice per append,
    /// which measured as a fixed per-append cost on top of the hashing and made
    /// the dose-response affine instead of linear — i.e. it showed up as the
    /// instrument misbehaving before it showed up as a cost.
    #[inline]
    pub fn absorb(&mut self, key: &str, body: &[u8]) {
        let dose = calibration_dose();
        if let Some(roller) = self.rollers.get_mut(key) {
            for _ in 0..dose {
                roller.absorb(body);
            }
        } else {
            let mut roller = ChunkRoller::new();
            for _ in 0..dose {
                roller.absorb(body);
            }
            self.rollers.insert(key.to_string(), roller);
        }
        self.absorbed += dose as u64;
    }

    /// The sealed certificate for one execution, if any chunk has sealed.
    pub fn certificate(&self, key: &str) -> Option<ChainCert> {
        self.rollers.get(key).and_then(|r| r.certificate())
    }

    /// Events not yet covered by a sealed certificate, for `key`.
    pub fn uncertified_tail(&self, key: &str) -> u32 {
        self.rollers.get(key).map_or(0, |r| r.len - r.sealed_len)
    }

    /// How many records this registry has absorbed.
    ///
    /// Discriminates "the path ran and did not help" from "the path never ran"
    /// — the distinction a zero on a single counter cannot make. Guards against
    /// shipping an inert integration (spec guardrail A9).
    pub fn absorbed(&self) -> u64 {
        self.absorbed
    }

    /// Number of executions with live chain state.
    pub fn tracked_executions(&self) -> usize {
        self.rollers.len()
    }
}

/// Verdict of comparing a stored certificate against a current one.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CertVerdict {
    /// Same length, same digest — the projection is valid, no refold.
    Valid,
    /// The stored certificate is a prefix of the current chain.
    StaleButPrefix,
    /// The stored certificate is longer than the current chain.
    StoredAhead,
    /// Same length, different digest — the chains diverged.
    DigestMismatch,
}

/// O(1) validity check: an i64/u32 compare and a 32-byte memcmp.
#[inline]
pub fn validate(stored: &ChainCert, current: &ChainCert) -> CertVerdict {
    if stored.chain_len == current.chain_len {
        if stored.chain_digest == current.chain_digest {
            CertVerdict::Valid
        } else {
            CertVerdict::DigestMismatch
        }
    } else if stored.chain_len < current.chain_len {
        CertVerdict::StaleButPrefix
    } else {
        CertVerdict::StoredAhead
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn body(i: u32) -> Vec<u8> {
        format!("ehdb-cert-vector-{i:04}").into_bytes()
    }

    #[test]
    fn the_digest_matches_an_independent_implementation() {
        // Golden vector from a THIRD implementation (Python/hashlib) that
        // shares no code with this one or with noetl/server's. The server-side
        // test `the_chunked_digest_matches_an_independent_implementation`
        // asserts the same value, so the two implementations are pinned to the
        // third rather than to each other — a transcription drift in either
        // fails loudly instead of quietly measuring a different construction.
        assert_eq!(CHUNK_EVENTS, 8, "the golden vector below assumes 8");
        let mut reg = ChainCertRegistry::new();
        for i in 0..24u32 {
            reg.absorb("exec-1", &body(i));
        }
        let cert = reg.certificate("exec-1").expect("3 sealed chunks");
        assert_eq!(cert.chain_len, 24);
        assert_eq!(
            hex::encode(cert.chain_digest),
            "c7d3e1fff420f51f0458bcf4f3fe5a3cbefe65aa6879d036197352d8b70a1f44",
            "this implementation has drifted from the pinned construction"
        );
    }

    #[test]
    fn the_certificate_is_44_bytes_on_the_wire() {
        assert_eq!(ChainCert::WIRE_BYTES + 8, 44);
    }

    #[test]
    fn chains_are_kept_per_execution() {
        // A PartWriter serialises many executions. If the registry mixed them
        // the digest would depend on interleaving, which is arrival-timing
        // dependent — the same hazard as commit-batch rollup.
        let mut interleaved = ChainCertRegistry::new();
        for i in 0..CHUNK_EVENTS {
            interleaved.absorb("exec-a", &body(i));
            interleaved.absorb("exec-b", &body(100 + i));
        }
        let mut separate = ChainCertRegistry::new();
        for i in 0..CHUNK_EVENTS {
            separate.absorb("exec-a", &body(i));
        }
        for i in 0..CHUNK_EVENTS {
            separate.absorb("exec-b", &body(100 + i));
        }
        assert_eq!(
            interleaved.certificate("exec-a"),
            separate.certificate("exec-a"),
            "exec-a's chain must not depend on exec-b's records being interleaved"
        );
        assert_eq!(
            interleaved.certificate("exec-b"),
            separate.certificate("exec-b"),
            "exec-b's chain must not depend on exec-a's records being interleaved"
        );
        assert_ne!(
            interleaved.certificate("exec-a"),
            interleaved.certificate("exec-b"),
            "different executions must not share a digest"
        );
    }

    #[test]
    fn a_divergent_early_chunk_changes_every_later_certificate() {
        // Chunks must be CHAINED, not independent digests. If chunk N+1 did not
        // absorb chunk N's digest, a chain that diverged early but converged
        // later would certify as valid — the certificate would check only the
        // tail.
        let n = CHUNK_EVENTS * 3;
        let mut clean = ChainCertRegistry::new();
        let mut tampered = ChainCertRegistry::new();
        let mut seen = 0;
        for i in 0..n {
            clean.absorb("e", &body(i));
            tampered.absorb("e", &if i == 0 { body(999) } else { body(i) });
            if let (Some(c), Some(t)) = (clean.certificate("e"), tampered.certificate("e")) {
                assert_eq!(c.chain_len, t.chain_len);
                assert_ne!(
                    c.chain_digest, t.chain_digest,
                    "divergence in chunk 1 must still be visible at chain_len {}",
                    c.chain_len
                );
                seen += 1;
            }
        }
        assert!(seen >= 3, "expected at least 3 sealed chunks, saw {seen}");
    }

    #[test]
    fn the_uncertified_tail_never_exceeds_one_chunk() {
        let mut reg = ChainCertRegistry::new();
        for i in 0..(CHUNK_EVENTS * 4 + 3) {
            reg.absorb("e", &body(i));
            assert!(
                reg.uncertified_tail("e") < CHUNK_EVENTS,
                "tail {} must stay under one chunk",
                reg.uncertified_tail("e")
            );
        }
    }

    #[test]
    fn a_short_chain_still_seals_at_least_one_chunk() {
        // Guards the choice of CHUNK_EVENTS: set above typical chain length,
        // short executions would certify nothing and the path would be inert.
        let mut reg = ChainCertRegistry::new();
        for i in 0..36u32 {
            reg.absorb("e", &body(i));
        }
        assert!(
            reg.certificate("e").is_some(),
            "a 36-event chain (the corpus mean) must seal a chunk; CHUNK_EVENTS={CHUNK_EVENTS} too large"
        );
    }

    #[test]
    fn validate_covers_every_verdict() {
        let a = ChainCert {
            chain_len: 8,
            chain_digest: [1u8; 32],
        };
        let b = ChainCert {
            chain_len: 8,
            chain_digest: [2u8; 32],
        };
        let longer = ChainCert {
            chain_len: 16,
            chain_digest: [1u8; 32],
        };
        assert_eq!(validate(&a, &a), CertVerdict::Valid);
        assert_eq!(validate(&a, &b), CertVerdict::DigestMismatch);
        assert_eq!(validate(&a, &longer), CertVerdict::StaleButPrefix);
        assert_eq!(validate(&longer, &a), CertVerdict::StoredAhead);
    }

    #[test]
    fn the_registry_records_that_it_ran() {
        // A9: distinguishes "ran and did not help" from "never ran". A clean
        // result from an unwired check is the failure mode being ruled out.
        let mut reg = ChainCertRegistry::new();
        assert_eq!(reg.absorbed(), 0);
        for i in 0..CHUNK_EVENTS {
            reg.absorb("e", &body(i));
        }
        assert_eq!(
            reg.absorbed(),
            CHUNK_EVENTS as u64,
            "the path must record that it RAN"
        );
        assert_eq!(reg.tracked_executions(), 1);
    }
}
