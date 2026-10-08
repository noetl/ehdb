//! **Quantified invariant measures, each with a planted-defect control.**
//!
//! These are not "does it work" tests — the suite has 1120 of those. They are *measures*:
//! each one computes a number over a realistic population, asserts the number, AND plants
//! a defect to prove the measure can see it.
//!
//! ⚠ WHY THE CONTROL IS THE POINT. A check that has never produced a positive result is
//! indistinguishable from a check that cannot. Every measure below therefore runs twice:
//! once over a healthy population, and once over a population with a known injected
//! violation, asserting the measure reports exactly that violation and no more. A measure
//! that passes the healthy case and also passes the planted case is decorative, and this
//! file fails if that happens.
//!
//! Each measure prints `population=N`, because a count of zero violations over zero
//! records is what a broken measure looks like.

use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use ehdb_l0::substrate::DurableSubstrate;
use ehdb_l0::{shard_for_execution, Dataset, L0Config, L0Engine, LocalFsSubstrate};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct M {
    seq: u64,
    key: String,
}

struct MD;
impl Dataset for MD {
    type Record = M;
    const NAME: &'static str = "measure_d";
    fn sort_key(r: &M) -> u64 {
        r.seq
    }
    fn partition(r: &M, n: u32) -> u32 {
        shard_for_execution(&r.key, n)
    }
    fn index_key(r: &M) -> &str {
        &r.key
    }
    fn read_partition(k: &str, n: u32) -> u32 {
        shard_for_execution(k, n)
    }
}

fn dir(tag: &str) -> PathBuf {
    static N: AtomicU64 = AtomicU64::new(0);
    let n = N.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir().join(format!("ehdb-measure-{tag}-{}-{n}", std::process::id()))
}

fn engine(root: &PathBuf) -> L0Engine<MD> {
    let s: Arc<dyn DurableSubstrate> = Arc::new(LocalFsSubstrate::new(root).unwrap());
    L0Engine::<MD>::open(L0Config::for_dataset(MD::NAME, root).with_shard_count(4), s).unwrap()
}

// =========================================================================
// MEASURE 1 — ordering: every index read is ascending in sort_key.
// =========================================================================

/// Count the descending adjacent pairs in a sequence. **Zero is healthy.**
fn descending_pairs(seqs: &[u64]) -> usize {
    seqs.windows(2).filter(|w| w[1] <= w[0]).count()
}

#[test]
fn measure_ordering_is_ascending_and_the_measure_can_see_a_violation() {
    let d = dir("ord");
    let mut e = engine(&d);
    let keys = ["a", "b", "c", "d", "e"];
    let mut population = 0usize;
    for i in 1..=500u64 {
        e.append_record(M {
            seq: i,
            key: keys[(i % 5) as usize].to_string(),
        })
        .unwrap();
        population += 1;
    }

    let mut violations = 0usize;
    let mut compared = 0usize;
    for k in keys {
        let got: Vec<u64> = e
            .read_index_after(k, 0)
            .unwrap()
            .iter()
            .map(|r| r.seq)
            .collect();
        assert!(
            !got.is_empty(),
            "key {k} returned nothing — a measure over an empty population is not a measure"
        );
        compared += got.len();
        violations += descending_pairs(&got);
    }
    println!("  ordering: population={population} compared={compared} violations={violations}");
    assert_eq!(
        compared, population,
        "every appended record must appear in exactly one index read"
    );
    assert_eq!(violations, 0, "an index read must be ascending in sort_key");

    // ---- PLANTED CONTROL ----
    // Reverse one read and confirm the measure reports violations. Without this, `0`
    // above could mean "ascending" or "descending_pairs is broken".
    let mut planted: Vec<u64> = e
        .read_index_after("a", 0)
        .unwrap()
        .iter()
        .map(|r| r.seq)
        .collect();
    planted.reverse();
    let seen = descending_pairs(&planted);
    println!(
        "  ordering CONTROL: reversed a {}-record read -> violations={seen}",
        planted.len()
    );
    assert_eq!(
        seen,
        planted.len() - 1,
        "the measure must flag EVERY adjacent pair of a fully reversed read"
    );

    // And a single swap must be seen as exactly one violation, not zero and not many.
    let mut one = planted.clone();
    one.reverse();
    one.swap(3, 4);
    assert_eq!(
        descending_pairs(&one),
        1,
        "a single inversion must count as exactly one"
    );
    let _ = std::fs::remove_dir_all(&d);
}

// =========================================================================
// MEASURE 2 — fold determinism: the same input folds to the same output.
// =========================================================================

/// A trivial deterministic fold: sum of seqs, in read order.
fn fold(records: &[M]) -> u64 {
    records
        .iter()
        .fold(0u64, |a, r| a.wrapping_mul(31).wrapping_add(r.seq))
}

#[test]
fn measure_fold_is_deterministic_across_repeated_reads_and_a_reopen() {
    let d = dir("fold");
    let mut e = engine(&d);
    for i in 1..=300u64 {
        e.append_record(M {
            seq: i,
            key: format!("k{}", i % 7),
        })
        .unwrap();
    }

    // Same engine, repeated reads.
    let mut digests = Vec::new();
    for _ in 0..5 {
        let mut all = Vec::new();
        for s in 0..7 {
            all.extend(e.read_index_after(&format!("k{s}"), 0).unwrap());
        }
        assert_eq!(
            all.len(),
            300,
            "population check: the fold input must be the whole log"
        );
        digests.push(fold(&all));
    }
    let distinct: std::collections::BTreeSet<u64> = digests.iter().copied().collect();
    println!(
        "  fold determinism: reads=5 population=300 distinct_digests={}",
        distinct.len()
    );
    assert_eq!(
        distinct.len(),
        1,
        "repeated reads of an unchanged log must fold identically"
    );

    // ---- PLANTED CONTROL ----
    // The fold MUST be sensitive to order, or "identical digests" proves nothing: a fold
    // that ignores order would report determinism even if the read order were random.
    let mut all: Vec<M> = Vec::new();
    for s in 0..7 {
        all.extend(e.read_index_after(&format!("k{s}"), 0).unwrap());
    }
    let base = fold(&all);
    let mut swapped = all.clone();
    swapped.swap(0, 1);
    println!(
        "  fold CONTROL: one swap changes the digest? {}",
        fold(&swapped) != base
    );
    assert_ne!(
        fold(&swapped),
        base,
        "the fold must be order-sensitive, or determinism over it is vacuous"
    );
    // And dropping one record must change it, or the fold is not reading everything.
    let mut dropped = all.clone();
    dropped.pop();
    assert_ne!(
        fold(&dropped),
        base,
        "the fold must be sensitive to a MISSING record"
    );
    let _ = std::fs::remove_dir_all(&d);
}

// =========================================================================
// MEASURE 3 — no divergence: a cold reopen reproduces the same set, exactly.
// =========================================================================

#[test]
fn measure_a_reopen_reproduces_the_set_and_the_measure_can_see_a_drop() {
    let d = dir("reopen");
    let mut want = Vec::new();
    {
        let mut e = engine(&d);
        for i in 1..=400u64 {
            let r = M {
                seq: i,
                key: format!("k{}", i % 11),
            };
            e.append_record(r.clone()).unwrap();
            want.push(r);
        }
        // Close the durability window before dropping, or the measure is about buffering.
        for f in e.take_sync_handles().unwrap() {
            f.sync_data().unwrap();
        }
    }
    let e2 = engine(&d);
    let mut got = Vec::new();
    for s in 0..11 {
        got.extend(e2.read_index_after(&format!("k{s}"), 0).unwrap());
    }
    let ws: std::collections::BTreeSet<u64> = want.iter().map(|r| r.seq).collect();
    let gs: std::collections::BTreeSet<u64> = got.iter().map(|r| r.seq).collect();
    println!(
        "  reopen parity: population={} recovered={} missing={} extra={}",
        ws.len(),
        gs.len(),
        ws.difference(&gs).count(),
        gs.difference(&ws).count()
    );
    assert_eq!(
        ws, gs,
        "a reopen must reproduce the set EXACTLY — set equality, not counts"
    );

    // ---- PLANTED CONTROL ----
    // Drop one record from the recovered set and confirm the comparison flags it. A
    // set-equality assertion that cannot fail on a single missing element is decorative.
    let mut holed = gs.clone();
    let victim = *holed.iter().next().unwrap();
    holed.remove(&victim);
    println!(
        "  reopen CONTROL: removed seq={victim} -> equal? {}",
        holed == ws
    );
    assert_ne!(holed, ws, "the comparison must fail on ONE missing record");
    let _ = std::fs::remove_dir_all(&d);
}

// =========================================================================
// ⚠ MEASURE 4 (read cost vs chain length) DELIBERATELY LIVES IN THE BENCHMARK,
// NOT HERE.
// =========================================================================
//
// It was written here first and removed. `cargo test` builds DEBUG, and a timing
// assertion in a debug build is noise wearing a measure's clothes. The debug numbers
// were:
//
//   len=  50  median=  102us  per_record=2049ns
//   len= 500  median=  143us  per_record= 287ns
//   len=2000  median= 4512us  per_record=2256ns
//
// A 10x length step cost only 1.4x (a ~100us fixed floor dominates at small N), which
// failed the resolution control — correctly. The same measurement in `--release` via
// `cargo bench -p ehdb-l0` resolves cleanly and is linear:
//
//   len=  10  726ns   72.6 ns/record
//   len= 100  5.67us  56.7 ns/record
//   len=1000  55.1us  55.1 ns/record
//
// So the measure is kept where it can resolve its signal, and `benches/l0_engine.rs`
// carries it with the control attached. ⚠ The debug 2000-record point hinted at
// super-linear growth (31x cost for 4x length); the benchmark now probes to 5000 to
// settle whether that is real or a debug artifact.
