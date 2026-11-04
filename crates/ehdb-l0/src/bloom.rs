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
//! Dependency-free hashing: `k` probes are derived by double-hashing two
//! `XxHash64` base hashes (Kirsch–Mitzenmacher), the same `twox-hash` already
//! pinned for partition routing.
//!
//! # Two probe schemes (noetl/ai-meta#517, noetl/ehdb#426)
//!
//! ⚠⚠ **The legacy scheme degenerates.** Probes are `(h1 + i·h2) mod m` with
//! `m` a multiple of 64 and `h2` unconstrained, so a key visits only
//! `m / gcd(h2, m)` distinct bits. One key in 2^j collapses by a factor of
//! 2^j; on a 128-bit filter about one key in 128 tests a SINGLE bit. Measured
//! on prod 2026-11-04: the retention floor was pinned by key
//! `375912612040286208`, whose `h2 ≡ 0 (mod 128)` sent all nine probes to bit
//! 117 of a part holding one other execution — a "false positive" the 1%
//! sizing said was a ~10⁻¹¹ event. On the read path that only costs an extra
//! part read; the retention floor reuses these blooms and stalls on it.
//!
//! [`BloomScheme::Mixed`] derives every probe independently — a SplitMix64 mix
//! of `(h1, h2, i)` — so the false-positive rate follows the textbook
//! `(1 - e^{-kn/m})^k` at every size. Forcing `h2` odd alone is not enough: at
//! small `m` double hashing has only ~`m²/2` distinct probe sequences, and a
//! key matching a member's `(h1, h2) mod m` shares its whole probe set — about
//! 1 in 8,192 on a 128-bit filter, measured (58 in 200,000) before this was
//! changed. It is also sized from the number of DISTINCT keys with a higher
//! cap, and serializes its bits as base64 rather than one JSON number per word.
//!
//! **Rollout is expand-first.** A filter's meaning depends on its scheme, so the
//! scheme is stored with it and a legacy filter is always read with legacy
//! probes — reading it any other way would produce false NEGATIVES, which the
//! read path cannot survive. This release teaches every reader both schemes and
//! keeps writing legacy filters byte-for-byte; writers switch with
//! [`BLOOM_SCHEME_ENV`]`=2` only once every reader runs this release, because an
//! older binary rejects the new fields and would fail to open the manifest.

use std::hash::Hasher;

use base64::Engine as _;
use serde::{Deserialize, Serialize};
use twox_hash::XxHash64;

/// `EHDB_BLOOM_SCHEME` — which scheme NEW filters are written with: unset or
/// `1` = [`BloomScheme::Legacy`] (today's bytes), `2` = [`BloomScheme::Mixed`].
/// Unrecognised values read as legacy: a typo must never write a filter an
/// older reader cannot open.
pub const BLOOM_SCHEME_ENV: &str = "EHDB_BLOOM_SCHEME";

/// Largest legacy filter: 64 Ki bits (8 KiB). Holds ~1% only up to ~6,800 keys.
const LEGACY_MAX_BITS: u32 = 1 << 16;
/// Largest mixed-scheme filter: 256 Ki bits (32 KiB, ~44 KiB as base64).
/// Holds ~1% up to ~27,000 distinct keys — a merged part's worth.
const DISTINCT_MAX_BITS: u32 = 1 << 18;

/// How a filter's probes are derived. Stored with the filter: it is part of
/// what the bits mean.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum BloomScheme {
    /// `(h1 + i·h2) mod m`, `m` a multiple of 64, `h2` unconstrained. Kept
    /// bit-for-bit so every filter already written reads exactly as before.
    #[default]
    Legacy,
    /// Each probe is an independent SplitMix64 mix of `(h1, h2, i)`, mod `m`.
    Mixed,
}

impl BloomScheme {
    /// The scheme new filters are written with ([`BLOOM_SCHEME_ENV`]).
    pub fn for_writes() -> Self {
        Self::parse(std::env::var(BLOOM_SCHEME_ENV).ok().as_deref())
    }

    pub fn parse(v: Option<&str>) -> Self {
        match v.map(str::trim) {
            Some("2") => Self::Mixed,
            _ => Self::Legacy,
        }
    }

    fn wire(self) -> u8 {
        match self {
            Self::Legacy => 0,
            Self::Mixed => 2,
        }
    }
}

/// A fixed-size bloom filter, serialized into the manifest alongside its part
/// (small — a few hundred bytes — so it is cached in RAM with the catalog and
/// needs no extra fetch).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "BloomWire", into = "BloomWire")]
pub struct Bloom {
    /// Bit array, packed into `u64` words.
    words: Vec<u64>,
    /// Number of bits (`words.len() * 64`, but stored to avoid rounding
    /// ambiguity).
    m_bits: u32,
    /// Number of hash probes.
    k: u32,
    /// How the probes are derived (see the module docs).
    scheme: BloomScheme,
}

/// The serialized shape. A legacy filter serializes exactly as before this type
/// existed — `{"words":[…],"m_bits":…,"k":…}` — and a distinct-scheme filter as
/// `{"m_bits":…,"k":…,"scheme":2,"b64":"…"}`.
///
/// ⚠ Unknown fields are tolerated (no `deny_unknown_fields`): the filter is
/// nested in every `PartMeta` and in the columnar header, and rejecting a field
/// a newer binary wrote would make the whole manifest unreadable on rollback.
#[derive(Serialize, Deserialize)]
struct BloomWire {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    words: Vec<u64>,
    m_bits: u32,
    k: u32,
    #[serde(default, skip_serializing_if = "is_legacy")]
    scheme: u8,
    /// The words as little-endian bytes, base64 (distinct scheme only).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    b64: Option<String>,
}

fn is_legacy(s: &u8) -> bool {
    *s == 0
}

impl From<Bloom> for BloomWire {
    fn from(b: Bloom) -> Self {
        match b.scheme {
            BloomScheme::Legacy => BloomWire {
                words: b.words,
                m_bits: b.m_bits,
                k: b.k,
                scheme: 0,
                b64: None,
            },
            BloomScheme::Mixed => {
                let bytes: Vec<u8> = b.words.iter().flat_map(|w| w.to_le_bytes()).collect();
                BloomWire {
                    words: Vec::new(),
                    m_bits: b.m_bits,
                    k: b.k,
                    scheme: b.scheme.wire(),
                    b64: Some(base64::engine::general_purpose::STANDARD.encode(bytes)),
                }
            }
        }
    }
}

impl TryFrom<BloomWire> for Bloom {
    type Error = String;

    /// Every check here guards the read path against a filter that would
    /// otherwise index past its bit array or probe a different bit set than it
    /// was written with.
    fn try_from(w: BloomWire) -> Result<Self, String> {
        let (scheme, words) = match w.scheme {
            0 => (BloomScheme::Legacy, w.words),
            2 => {
                let raw = w.b64.ok_or("bloom scheme 2 without b64 bits")?;
                let bytes = base64::engine::general_purpose::STANDARD
                    .decode(raw)
                    .map_err(|e| format!("bloom b64: {e}"))?;
                if bytes.len() % 8 != 0 {
                    return Err(format!(
                        "bloom b64 length {} is not whole words",
                        bytes.len()
                    ));
                }
                let words = bytes
                    .chunks_exact(8)
                    .map(|c| u64::from_le_bytes(c.try_into().expect("8-byte chunk")))
                    .collect();
                (BloomScheme::Mixed, words)
            }
            // A scheme this binary does not know cannot be probed correctly,
            // and guessing would risk false negatives. Refuse loudly.
            other => return Err(format!("unknown bloom scheme {other}")),
        };
        if w.m_bits == 0 || w.k == 0 {
            return Err(format!("bloom with m_bits={} k={}", w.m_bits, w.k));
        }
        if (words.len() as u64) * 64 < u64::from(w.m_bits) {
            return Err(format!(
                "bloom m_bits={} exceeds its {} words",
                w.m_bits,
                words.len()
            ));
        }
        Ok(Bloom {
            words,
            m_bits: w.m_bits,
            k: w.k,
            scheme,
        })
    }
}

impl Bloom {
    /// Build a bloom sized for `expected_items` distinct keys at ~1% false-
    /// positive rate, in the scheme [`BLOOM_SCHEME_ENV`] selects. Clamped to sane
    /// bounds so an empty or huge part still gets a small, bounded structure.
    pub fn for_expected(expected_items: usize) -> Self {
        Self::for_expected_in(expected_items, BloomScheme::for_writes())
    }

    /// [`for_expected`](Self::for_expected) in an explicit scheme.
    pub fn for_expected_in(expected_items: usize, scheme: BloomScheme) -> Self {
        let n = expected_items.max(1) as f64;
        // Optimal bits: m = -n ln p / (ln2)^2, p = 0.01.
        let p = 0.01_f64;
        let m = (-(n * p.ln()) / (std::f64::consts::LN_2 * std::f64::consts::LN_2)).ceil();
        let m_bits = match scheme {
            BloomScheme::Legacy => {
                let m_bits = (m as u32).clamp(64, LEGACY_MAX_BITS); // [64 bits, 8 KiB]
                                                                    // Round up to a whole word.
                m_bits.div_ceil(64) * 64
            }
            BloomScheme::Mixed => (m as u32).clamp(64, DISTINCT_MAX_BITS).div_ceil(64) * 64,
        };
        let words_len = m_bits.div_ceil(64) as usize;
        // Optimal k = m/n ln2, clamped.
        let k = ((m_bits as f64 / n) * std::f64::consts::LN_2).round() as u32;
        let k = k.clamp(1, 12);
        Self {
            words: vec![0u64; words_len],
            m_bits,
            k,
            scheme,
        }
    }

    /// Which probe scheme this filter was written with.
    pub fn scheme(&self) -> BloomScheme {
        self.scheme
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
        let scheme = self.scheme;
        (0..self.k).map(move |i| {
            let i = i as u64;
            let combined = match scheme {
                // Kirsch–Mitzenmacher: g_i = h1 + i*h2. Kept exactly: the bits of
                // every legacy filter already written were set this way.
                BloomScheme::Legacy => h1.wrapping_add(i.wrapping_mul(h2)),
                // An independent probe per i, so no two keys share a probe
                // sequence by construction and no key's probes collapse.
                BloomScheme::Mixed => {
                    splitmix64(h1 ^ h2.rotate_left(32) ^ i.wrapping_mul(0x9E37_79B9_7F4A_7C15))
                }
            };
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

/// SplitMix64's finaliser: a fast, well-distributed 64-bit mix.
fn splitmix64(mut z: u64) -> u64 {
    z = z.wrapping_add(0x9E37_79B9_7F4A_7C15);
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_false_negatives() {
        for scheme in [BloomScheme::Legacy, BloomScheme::Mixed] {
            for n in [1usize, 10, 100, 1000] {
                let mut b = Bloom::for_expected_in(n, scheme);
                let keys: Vec<String> = (0..n).map(|i| format!("exec-{i}")).collect();
                for k in &keys {
                    b.insert(k);
                }
                // Every inserted key MUST report present (zero false negatives).
                for k in &keys {
                    assert!(
                        b.maybe_contains(k),
                        "false negative for {k} ({scheme:?}, n={n})"
                    );
                }
            }
        }
    }

    #[test]
    fn absent_keys_mostly_pruned() {
        let mut b = Bloom::for_expected_in(100, BloomScheme::Legacy);
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
        for scheme in [BloomScheme::Legacy, BloomScheme::Mixed] {
            let mut b = Bloom::for_expected_in(10, scheme);
            b.insert("a");
            b.insert("b");
            let json = serde_json::to_string(&b).unwrap();
            let back: Bloom = serde_json::from_str(&json).unwrap();
            assert_eq!(b, back);
            assert!(back.maybe_contains("a"));
            assert!(back.maybe_contains("b"));
        }
    }

    /// The real key from prod (noetl/ai-meta#517), and the real filter it pinned
    /// the floor on: a 10-record part holding one execution.
    const PROD_MEMBER: &str = "374482572496871424";
    const PROD_FALSE_HIT: &str = "375912612040286208";
    const PROD_BLOOM: &str = r#"{"words":[73201365303689216,9150170126090240],"m_bits":128,"k":9}"#;

    /// ⭐ Release A writes exactly today's bytes: an older reader (which rejects
    /// unknown fields on this struct) must still open everything this one writes
    /// while the switch is off.
    #[test]
    fn a_legacy_filter_serializes_exactly_as_before() {
        let b: Bloom = serde_json::from_str(PROD_BLOOM).unwrap();
        assert_eq!(b.scheme(), BloomScheme::Legacy);
        assert_eq!(serde_json::to_string(&b).unwrap(), PROD_BLOOM);
    }

    /// ⭐ Legacy filters keep their legacy probes: the real prod filter still
    /// says "maybe" to its real member — a different reading would be a false
    /// negative on the read path — and still reproduces the prod false positive.
    #[test]
    fn a_legacy_filter_reads_exactly_as_before() {
        let b: Bloom = serde_json::from_str(PROD_BLOOM).unwrap();
        assert!(
            b.maybe_contains(PROD_MEMBER),
            "legacy reading changed: false negative"
        );
        assert!(
            b.maybe_contains(PROD_FALSE_HIT),
            "legacy reading changed: the prod false positive no longer reproduces"
        );
    }

    /// ⭐⭐ The defect, on the real key: its h2 is ≡ 0 mod 128, so the legacy
    /// scheme sends all nine probes to ONE bit. The mixed scheme spreads them.
    #[test]
    fn the_mixed_scheme_does_not_collapse_the_prod_key() {
        let distinct_bits = |scheme, key: &str| {
            let b = Bloom::for_expected_in(10, scheme);
            assert_eq!(b.size_bits(), 128);
            let mut bits: Vec<usize> = b.bit_indices(key).collect();
            bits.sort_unstable();
            bits.dedup();
            bits.len()
        };
        assert_eq!(
            distinct_bits(BloomScheme::Legacy, PROD_FALSE_HIT),
            1,
            "the prod key's legacy probes collapse"
        );
        assert!(distinct_bits(BloomScheme::Mixed, PROD_FALSE_HIT) >= 7);
        // Across many keys, no mixed probe set degenerates to a couple of bits.
        for i in 0..20_000 {
            let key = format!("k-{i}");
            assert!(
                distinct_bits(BloomScheme::Mixed, &key) >= 4,
                "degenerate probes for {key}"
            );
        }
    }

    /// The prod shape — one execution in a small part, tested against many
    /// absent keys — measured against theory. For one key in a 128-bit, k=9
    /// filter the expected rate is ~(9/128)^9 ≈ 4e-11, i.e. zero in any trial
    /// we can run. The legacy scheme does not get there; the mixed one must.
    #[test]
    fn the_false_positive_rate_matches_theory_for_the_mixed_scheme() {
        let trials = 200_000;
        let rate = |scheme| {
            let mut b = Bloom::for_expected_in(10, scheme);
            b.insert(PROD_MEMBER);
            (0..trials)
                .filter(|i| b.maybe_contains(&format!("absent-{i}")))
                .count()
        };
        let legacy = rate(BloomScheme::Legacy);
        let mixed = rate(BloomScheme::Mixed);
        assert_eq!(mixed, 0, "mixed scheme: {mixed}/{trials} false positives");
        assert!(
            legacy > 0,
            "the legacy defect should be measurable here ({legacy}/{trials}); if this \
             reads 0 the test has lost its power"
        );
    }

    /// The rate also holds at the designed load: ~1% when full.
    #[test]
    fn a_full_mixed_filter_holds_about_one_percent() {
        for n in [100usize, 7_000, 20_000] {
            let mut b = Bloom::for_expected_in(n, BloomScheme::Mixed);
            for i in 0..n {
                b.insert(&format!("present-{i}"));
            }
            let trials = 50_000;
            let fp = (0..trials)
                .filter(|i| b.maybe_contains(&format!("absent-{i}")))
                .count();
            // 1% target, with sampling slack.
            assert!(
                fp * 100 <= trials * 2,
                "n={n}: {fp}/{trials} false positives"
            );
        }
    }

    /// #426 sizing: the legacy cap (64 Ki bits) loses the 1% target past ~6,800
    /// keys; the mixed scheme keeps growing to its higher cap.
    #[test]
    fn the_mixed_scheme_sizes_past_the_legacy_cap() {
        assert_eq!(
            Bloom::for_expected_in(20_000, BloomScheme::Legacy).size_bits(),
            LEGACY_MAX_BITS
        );
        assert!(Bloom::for_expected_in(20_000, BloomScheme::Mixed).size_bits() > LEGACY_MAX_BITS);
        assert!(
            Bloom::for_expected_in(10_000_000, BloomScheme::Mixed).size_bits() <= DISTINCT_MAX_BITS
        );
    }

    /// #426 encoding: base64 bits are much smaller than one JSON number per word,
    /// for filters of the same size.
    #[test]
    fn the_mixed_encoding_is_compact() {
        let fill = |scheme| {
            let mut b = Bloom::for_expected_in(5_000, scheme);
            for i in 0..5_000 {
                b.insert(&format!("e-{i}"));
            }
            serde_json::to_string(&b).unwrap().len()
        };
        assert_eq!(
            Bloom::for_expected_in(5_000, BloomScheme::Legacy).size_bits(),
            Bloom::for_expected_in(5_000, BloomScheme::Mixed).size_bits(),
            "the comparison must be like for like"
        );
        let (legacy, mixed) = (fill(BloomScheme::Legacy), fill(BloomScheme::Mixed));
        // base64 is ~10.7 chars per 64-bit word against ~20.5 for a JSON decimal
        // and its comma: about half the size (measured 8,034 vs 15,328 B here).
        assert!(
            mixed * 10 < legacy * 6,
            "legacy {legacy} B vs mixed {mixed} B"
        );
    }

    /// Expand-first: a field a newer binary adds must not fail the decode.
    #[test]
    fn unknown_fields_are_tolerated() {
        let json = r#"{"words":[1,2],"m_bits":128,"k":9,"future":"x"}"#;
        let b: Bloom = serde_json::from_str(json).expect("unknown field must not fail");
        assert_eq!(b.scheme(), BloomScheme::Legacy);
    }

    /// A scheme this binary does not know is refused rather than guessed: probing
    /// it the wrong way could produce false negatives.
    #[test]
    fn malformed_filters_are_refused() {
        for bad in [
            r#"{"m_bits":128,"k":9,"scheme":7,"b64":"AAAAAAAAAAAAAAAAAAAAAA=="}"#,
            r#"{"m_bits":128,"k":9,"scheme":2}"#,
            r#"{"m_bits":200,"k":9,"scheme":2,"b64":"AAAAAAAAAAAAAAAAAAAAAA=="}"#,
            r#"{"words":[1],"m_bits":128,"k":9}"#,
            r#"{"words":[1,2],"m_bits":128,"k":0}"#,
        ] {
            assert!(
                serde_json::from_str::<Bloom>(bad).is_err(),
                "accepted {bad}"
            );
        }
    }

    #[test]
    fn the_write_scheme_parses_conservatively() {
        assert_eq!(BloomScheme::parse(None), BloomScheme::Legacy);
        assert_eq!(BloomScheme::parse(Some("1")), BloomScheme::Legacy);
        assert_eq!(BloomScheme::parse(Some("two")), BloomScheme::Legacy);
        assert_eq!(BloomScheme::parse(Some(" 2 ")), BloomScheme::Mixed);
    }
}
