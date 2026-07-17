//! A compact **bloom filter** — the fixed-dimension inverted-index primitive
//! (RFC §2.1 "bloom filters as a cheap pre-filter"; §2.6 "blooms only on the
//! specific fields noetl filters on").
//!
//! This is deliberately **not** a general inverted `value → id` IndexDB. For D1
//! the only prunable dimension is `execution_id` (the per-execution replay
//! filter). A part (and each granule within it) carries a small bloom over its
//! execution ids; a lookup for execution E consults the bloom first and skips
//! any part/granule whose bloom says E is **definitely absent** — a
//! zero-false-negative pre-filter that turns a per-execution read from "scan
//! every part of the shard" into "open only the parts that can hold E". This is
//! the §2.6 shrink of the "biggest gap": a handful of fixed blooms, not a
//! general index.
//!
//! Dependency-free: `k` hash functions are derived by double-hashing two
//! `XxHash64` base hashes (Kirsch–Mitzenmacher), the same `twox-hash` already
//! pinned for partition routing.

use std::hash::Hasher;

use serde::{Deserialize, Serialize};
use twox_hash::XxHash64;

/// A fixed-size bloom filter, serialized into the manifest alongside its part
/// (small — a few hundred bytes — so it is cached in RAM with the catalog and
/// needs no extra fetch).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Bloom {
    /// Bit array, packed into `u64` words.
    words: Vec<u64>,
    /// Number of bits (`words.len() * 64`, but stored to avoid rounding
    /// ambiguity).
    m_bits: u32,
    /// Number of hash probes.
    k: u32,
}

impl Bloom {
    /// Build a bloom sized for `expected_items` distinct keys at ~1% false-
    /// positive rate. Clamped to sane bounds so an empty or huge part still gets
    /// a small, bounded structure.
    pub fn for_expected(expected_items: usize) -> Self {
        let n = expected_items.max(1) as f64;
        // Optimal bits: m = -n ln p / (ln2)^2, p = 0.01.
        let p = 0.01_f64;
        let m = (-(n * p.ln()) / (std::f64::consts::LN_2 * std::f64::consts::LN_2)).ceil();
        let m_bits = (m as u32).clamp(64, 1 << 16); // [64 bits, 8 KiB]
                                                    // Round up to a whole word.
        let words_len = m_bits.div_ceil(64) as usize;
        let m_bits = (words_len as u32) * 64;
        // Optimal k = m/n ln2, clamped.
        let k = ((m_bits as f64 / n) * std::f64::consts::LN_2).round() as u32;
        let k = k.clamp(1, 12);
        Self {
            words: vec![0u64; words_len],
            m_bits,
            k,
        }
    }

    /// Two independent 64-bit base hashes of `key` (seeds 0 and a fixed
    /// non-zero), combined by double-hashing for the `k` probes.
    fn base_hashes(key: &str) -> (u64, u64) {
        let mut h1 = XxHash64::with_seed(0);
        h1.write(key.as_bytes());
        let mut h2 = XxHash64::with_seed(0x9E37_79B9_7F4A_7C15);
        h2.write(key.as_bytes());
        (h1.finish(), h2.finish())
    }

    fn bit_indices(&self, key: &str) -> impl Iterator<Item = usize> + '_ {
        let (h1, h2) = Self::base_hashes(key);
        let m = self.m_bits as u64;
        (0..self.k).map(move |i| {
            // Kirsch–Mitzenmacher: g_i = h1 + i*h2.
            let combined = h1.wrapping_add((i as u64).wrapping_mul(h2));
            (combined % m) as usize
        })
    }

    /// Insert a key.
    pub fn insert(&mut self, key: &str) {
        for bit in self.bit_indices(key).collect::<Vec<_>>() {
            self.words[bit / 64] |= 1u64 << (bit % 64);
        }
    }

    /// Whether `key` **may** be present. `false` is definitive (the key was
    /// never inserted); `true` may be a false positive. Never a false negative —
    /// the correctness property the read path relies on.
    pub fn maybe_contains(&self, key: &str) -> bool {
        self.bit_indices(key).all(|bit| {
            let word = self.words[bit / 64];
            word & (1u64 << (bit % 64)) != 0
        })
    }

    /// The bloom's bit size (for reporting).
    pub fn size_bits(&self) -> u32 {
        self.m_bits
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_false_negatives() {
        let mut b = Bloom::for_expected(100);
        let keys: Vec<String> = (0..100).map(|i| format!("exec-{i}")).collect();
        for k in &keys {
            b.insert(k);
        }
        // Every inserted key MUST report present (zero false negatives).
        for k in &keys {
            assert!(b.maybe_contains(k), "false negative for {k}");
        }
    }

    #[test]
    fn absent_keys_mostly_pruned() {
        let mut b = Bloom::for_expected(100);
        for i in 0..100 {
            b.insert(&format!("present-{i}"));
        }
        // Absent keys should mostly be pruned (false-positive rate ~1%).
        let mut false_positives = 0;
        let trials = 2000;
        for i in 0..trials {
            if b.maybe_contains(&format!("absent-{i}")) {
                false_positives += 1;
            }
        }
        // Generous bound: well under 10% even with sizing slack.
        assert!(
            false_positives * 10 < trials,
            "false positives {false_positives}/{trials} too high"
        );
    }

    #[test]
    fn empty_expected_is_bounded_and_usable() {
        let mut b = Bloom::for_expected(0);
        assert!(b.size_bits() >= 64);
        b.insert("x");
        assert!(b.maybe_contains("x"));
        assert!(!b.maybe_contains("y") || b.maybe_contains("y")); // just no panic
    }

    #[test]
    fn serde_round_trip() {
        let mut b = Bloom::for_expected(10);
        b.insert("a");
        b.insert("b");
        let json = serde_json::to_string(&b).unwrap();
        let back: Bloom = serde_json::from_str(&json).unwrap();
        assert_eq!(b, back);
        assert!(back.maybe_contains("a"));
        assert!(back.maybe_contains("b"));
    }
}
