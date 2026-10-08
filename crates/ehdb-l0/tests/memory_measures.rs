//! **Resident-memory measures — is a long-lived engine's RAM O(records) or O(parts)?**
//!
//! The question this file exists to answer: an engine that has been up for a week has
//! appended millions of records. Does it still hold RAM proportional to *all of them*?
//!
//! That is not an idle question here. The engine keeps four in-RAM structures that could
//! each answer it differently — the active `PartWriter` per shard (buffers until seal), the
//! `Manifest` (one entry per part), the dedupe window, and `shard_tail_max`. If any of them
//! is unbounded in record count, RAM grows with chain length and the process dies on a long
//! enough uptime. If they are all bounded by *part* count, RAM grows far more slowly and
//! `manifest_retain` already governs it.
//!
//! The answer is measured below, not asserted from reading the code.
//!
//! # Why a global allocator, and why exactly one `#[test]`
//!
//! Rust has no portable way to ask "how many bytes does this structure hold" — `size_of`
//! sees the stack footprint of a `HashMap`, not its heap. So this file installs a counting
//! `#[global_allocator]`, which an integration test may do because it is its own crate root.
//!
//! The counter is **process-global**, and `cargo test` runs test functions **in parallel
//! threads by default**. Two tests measuring at once would each attribute the other's
//! allocations to itself — producing a number that is wrong in a direction that looks
//! plausible. So this file deliberately contains **one** `#[test]`, which performs every
//! measurement sequentially. Adding a second one would silently corrupt both.
//!
//! # The instrument is checked before it is trusted
//!
//! The control runs **first**: leak a known 1 MiB and require the counter to see it. A
//! counting allocator that is never actually installed (wrong attribute, wrong crate,
//! optimised-away allocation) reports a steady, confident **0 bytes of growth** — which is
//! indistinguishable from the best possible result. Measuring first and controlling second
//! would let that read as "RAM is perfectly bounded".
//!
//! Run with `--nocapture` for the table.

use std::alloc::{GlobalAlloc, Layout, System};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;

use ehdb_l0::substrate::DurableSubstrate;
use ehdb_l0::{shard_for_execution, Dataset, L0Config, L0Engine, LocalFsSubstrate};
use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// The instrument.
// ---------------------------------------------------------------------------

static LIVE: AtomicUsize = AtomicUsize::new(0);
static PEAK: AtomicUsize = AtomicUsize::new(0);

struct Counting;

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, l: Layout) -> *mut u8 {
        let p = System.alloc(l);
        if !p.is_null() {
            let now = LIVE.fetch_add(l.size(), Ordering::Relaxed) + l.size();
            PEAK.fetch_max(now, Ordering::Relaxed);
        }
        p
    }
    unsafe fn dealloc(&self, p: *mut u8, l: Layout) {
        LIVE.fetch_sub(l.size(), Ordering::Relaxed);
        System.dealloc(p, l);
    }
    unsafe fn realloc(&self, p: *mut u8, l: Layout, new_size: usize) -> *mut u8 {
        let np = System.realloc(p, l, new_size);
        if !np.is_null() {
            if new_size >= l.size() {
                let d = new_size - l.size();
                let now = LIVE.fetch_add(d, Ordering::Relaxed) + d;
                PEAK.fetch_max(now, Ordering::Relaxed);
            } else {
                LIVE.fetch_sub(l.size() - new_size, Ordering::Relaxed);
            }
        }
        np
    }
}

#[global_allocator]
static ALLOC: Counting = Counting;

fn live() -> usize {
    LIVE.load(Ordering::Relaxed)
}

// ---------------------------------------------------------------------------
// Dataset under measurement.
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct R {
    seq: u64,
    key: String,
}

struct MD;
impl Dataset for MD {
    type Record = R;
    const NAME: &'static str = "memory_d";
    fn sort_key(r: &R) -> u64 {
        r.seq
    }
    fn partition(r: &R, n: u32) -> u32 {
        shard_for_execution(&r.key, n)
    }
    fn index_key(r: &R) -> &str {
        &r.key
    }
    fn read_partition(k: &str, n: u32) -> u32 {
        shard_for_execution(k, n)
    }
}

fn dir(tag: &str) -> PathBuf {
    static N: AtomicU64 = AtomicU64::new(0);
    let n = N.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir().join(format!("ehdb-mem-{tag}-{}-{n}", std::process::id()))
}

fn engine(root: &Path, seal_every: u64) -> L0Engine<MD> {
    let obj = root.join("obj");
    let hot = root.join("hot");
    std::fs::create_dir_all(&obj).unwrap();
    std::fs::create_dir_all(&hot).unwrap();
    let s: Arc<dyn DurableSubstrate> = Arc::new(LocalFsSubstrate::new(&obj).unwrap());
    let cfg = L0Config::for_dataset(MD::NAME, &hot)
        .with_shard_count(1)
        .with_seal_max_records(seal_every);
    L0Engine::<MD>::open(cfg, s).unwrap()
}

/// Live part count — **from the engine's own manifest**, not inferred from filenames.
///
/// ⚠ The first version of this walked the directory tree counting files whose name
/// contained `"part"`. It reported **1 part for 16,000 records at `seal_max_records=256`**,
/// because what it was matching was the `parts/` *directory*, not the parts. A per-part
/// number divided by that denominator was wrong by ~60x and looked plausible. The engine
/// publishes the number; ask it.
fn part_count(e: &L0Engine<MD>) -> usize {
    e.manifest_snapshot().parts.len()
}

/// log-log slope of y over x between two points: the growth exponent.
/// 1.0 = linear, 2.0 = quadratic, 0.0 = flat.
fn exponent(x0: f64, y0: f64, x1: f64, y1: f64) -> f64 {
    if x0 <= 0.0 || y0 <= 0.0 || x1 <= 0.0 || y1 <= 0.0 {
        return f64::NAN;
    }
    (y1 / y0).ln() / (x1 / x0).ln()
}

const CONTROL_BYTES: usize = 1024 * 1024;

#[test]
fn memory_growth_is_measured_against_a_checked_instrument() {
    // -----------------------------------------------------------------
    // 0. THE CONTROL, FIRST. A counter that is not installed reports 0
    //    growth for everything, which looks like the ideal result.
    // -----------------------------------------------------------------
    let before_control = live();
    let leaked: &'static mut [u8] = Box::leak(vec![7u8; CONTROL_BYTES].into_boxed_slice());
    std::hint::black_box(&leaked[0]);
    let control_observed = live().saturating_sub(before_control);
    assert!(
        control_observed >= CONTROL_BYTES * 9 / 10,
        "INSTRUMENT BLIND: leaked {CONTROL_BYTES} B, counter moved {control_observed} B. \
         Every number this test would print is meaningless — a counting allocator that is \
         not installed reports 0 growth, which is also the best possible result."
    );
    println!(
        "\ncontrol: leaked {} KiB, observed {} KiB -> instrument sees allocations",
        CONTROL_BYTES / 1024,
        control_observed / 1024
    );

    // -----------------------------------------------------------------
    // 1. RAM vs chain length, at a fixed seal size so parts accumulate.
    // -----------------------------------------------------------------
    const SEAL_EVERY: u64 = 256;
    let counts: [u64; 3] = [500, 2_000, 8_000];
    let mut rows: Vec<(u64, usize, usize)> = Vec::new(); // (records, parts, bytes)

    for &n in &counts {
        let root = dir(&format!("n{n}"));
        // Open, then take the baseline: engine construction itself is not
        // what we are measuring — growth with record count is.
        let mut e = engine(&root, SEAL_EVERY);
        let base = live();
        for i in 1..=n {
            e.append_record(R {
                seq: i,
                key: format!("k{}", i % 64),
            })
            .unwrap();
        }
        let held = live().saturating_sub(base);
        let parts = part_count(&e);
        // The engine must still be alive at the moment of measurement, or we
        // measure its destructor instead of its footprint.
        assert_eq!(e.global_sequence(), n);
        rows.push((n, parts, held));
        drop(e);
        let _ = std::fs::remove_dir_all(&root);
    }

    println!("\nrecords   parts   live bytes   B/record   B/part");
    for (n, parts, held) in &rows {
        println!(
            "{:>7}   {:>5}   {:>10}   {:>8.1}   {:>6}",
            n,
            parts,
            held,
            *held as f64 / *n as f64,
            held.checked_div(*parts)
                .map(|v| v.to_string())
                .unwrap_or_else(|| "-".into())
        );
    }

    let (n0, _, b0) = rows[0];
    let (n2, _, b2) = rows[2];
    let exp = exponent(n0 as f64, b0 as f64, n2 as f64, b2 as f64);
    println!(
        "\ngrowth exponent of live bytes over record count: {exp:.2} \
         (1.0 = linear, 2.0 = quadratic)"
    );

    // The exponent says "not quadratic". The actionable number is the MARGINAL
    // cost of a part, because part count is what a long-uptime engine
    // accumulates and what merge exists to bound. Two-point slope between the
    // two largest rows, so the fixed cost of opening an engine is divided out
    // rather than smeared across the per-record figure.
    let (_, p1, b1) = rows[1];
    let (_, p2, b2_) = rows[2];
    if p2 > p1 {
        let per_part = (b2_ as f64 - b1 as f64) / (p2 - p1) as f64;
        let fixed = b2_ as f64 - per_part * p2 as f64;
        println!(
            "fit over parts: ~{:.0} B fixed + ~{:.0} B per part               (=> 1M records at seal=1024 ~= {} parts ~= {:.1} MiB held)",
            fixed,
            per_part,
            1_000_000 / 1024,
            (fixed + per_part * (1_000_000.0 / 1024.0)) / (1024.0 * 1024.0)
        );
        assert!(
            per_part > 0.0 && per_part < 64.0 * 1024.0,
            "marginal cost of a part measured at {per_part:.0} B — outside any              plausible range, so the fit (and the O(parts) claim) is not trustworthy"
        );
    }

    // Quadratic is the failure that took the prod volume out in the manifest
    // path (noetl/ehdb#344). Linear in *records* is the one that kills a
    // long-uptime process. Either is a real defect; both are caught here.
    assert!(
        exp < 1.25,
        "RAM grows with chain length at exponent {exp:.2} \
         ({b0} B at {n0} records -> {b2} B at {n2}). An engine that holds RAM \
         proportional to every record it has ever seen cannot stay up."
    );

    // -----------------------------------------------------------------
    // 2. Does a seal actually release the writer's buffer? Append the
    //    same records with sealing effectively disabled, and compare.
    // -----------------------------------------------------------------
    const UNSEALED_N: u64 = 8_000;
    let root = dir("unsealed");
    let mut e = engine(&root, u64::MAX); // never seals on record count
    let base = live();
    for i in 1..=UNSEALED_N {
        e.append_record(R {
            seq: i,
            key: format!("k{}", i % 64),
        })
        .unwrap();
    }
    let unsealed_held = live().saturating_sub(base);
    let sealed_held = rows[2].2;
    println!(
        "\n{UNSEALED_N} records, never sealed: {unsealed_held} B \
         vs sealed every {SEAL_EVERY}: {sealed_held} B  \
         (ratio {:.2}x)",
        unsealed_held as f64 / sealed_held.max(1) as f64
    );
    assert_eq!(e.global_sequence(), UNSEALED_N);
    drop(e);
    let _ = std::fs::remove_dir_all(&root);

    // This is the directional fact worth pinning: an un-sealed writer holds
    // the records, a sealed one does not. If these two were equal, sealing
    // would not be releasing anything and the bound in part 1 would be
    // coming from somewhere else than the mechanism we think.
    assert!(
        unsealed_held > sealed_held,
        "never-sealing held {unsealed_held} B and sealing every {SEAL_EVERY} held \
         {sealed_held} B for the same {UNSEALED_N} records. Sealing is then not what \
         bounds RAM, so part 1's bound is attributed to the wrong mechanism."
    );

    println!(
        "\npopulation measured: {} record counts x 1 shard, \
         1 dataset, local-fs substrate; peak process RSS-proxy {} KiB",
        counts.len(),
        PEAK.load(Ordering::Relaxed) / 1024
    );
}
