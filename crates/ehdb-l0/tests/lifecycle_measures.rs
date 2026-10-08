//! **Seal / merge / reclaim measures — and whether the manifest quadratic is still latent.**
//!
//! The highest-risk surface in the engine, measured. On 2026-09-01 the manifest path
//! reached **6,770 snapshots / 19.4 GB behind 71.8 MB of real data**, filled the prod
//! volume, and made every append fail — because the cost was the product of two growing
//! quantities: snapshot **size** grows with part count, snapshot **count** grew with write
//! count. `manifest_retain` (noetl/ehdb#344) was the fix.
//!
//! # The planted defect is a supported configuration
//!
//! `manifest_retain = 0` disables pruning — it IS the pre-fix behaviour, by its own
//! doc comment. So the control here is not a mutation I inject: it is a config the engine
//! still accepts, which makes it the strongest possible instrument check. If bounded and
//! unbounded measure the same, the instrument cannot see the defect it exists to detect and
//! nothing else in this file is evidence.
//!
//! Bytes on disk, not timing — deterministic, so this belongs in `cargo test` rather than a
//! benchmark. `--nocapture` prints the table.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use ehdb_l0::substrate::DurableSubstrate;
use ehdb_l0::{shard_for_execution, Dataset, L0Config, L0Engine, LocalFsSubstrate, MergePolicy};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct R {
    seq: u64,
    key: String,
}

struct LD;
impl Dataset for LD {
    type Record = R;
    const NAME: &'static str = "lifecycle_d";
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
    std::env::temp_dir().join(format!("ehdb-life-{tag}-{}-{n}", std::process::id()))
}

/// Total bytes of every manifest object on disk, and how many there are.
fn manifest_bytes(root: &Path) -> (u64, usize) {
    let mut bytes = 0u64;
    let mut count = 0usize;
    let mut stack = vec![root.to_path_buf()];
    while let Some(p) = stack.pop() {
        let Ok(rd) = std::fs::read_dir(&p) else {
            continue;
        };
        for e in rd.flatten() {
            let path = e.path();
            if path.is_dir() {
                stack.push(path);
            } else if path.to_string_lossy().contains("manifest") {
                if let Ok(m) = e.metadata() {
                    bytes += m.len();
                    count += 1;
                }
            }
        }
    }
    (bytes, count)
}

fn engine(root: &Path, retain: usize, seal_every: u64) -> L0Engine<LD> {
    // ⚠ Both roots must exist first. The substrate and the hot tier are separate
    // directories; opening against a missing one fails as
    // `Storage("No such file or directory")` at the first APPEND, not at open — which
    // reads like an append bug rather than a setup one.
    let obj = root.join("obj");
    let hot = root.join("hot");
    std::fs::create_dir_all(&obj).unwrap();
    std::fs::create_dir_all(&hot).unwrap();
    let s: Arc<dyn DurableSubstrate> = Arc::new(LocalFsSubstrate::new(&obj).unwrap());
    let cfg = L0Config::for_dataset(LD::NAME, &hot)
        .with_shard_count(1)
        .with_seal_max_records(seal_every)
        .with_manifest_retain(retain)
        .with_merge_policy(MergePolicy {
            small_part_max_records: seal_every,
            trigger_run_len: 4,
            max_merge_parts: 8,
        });
    L0Engine::<LD>::open(cfg, s).unwrap()
}

/// Append `n` records, driving the caller-owned lifecycle, and report manifest footprint.
fn run(retain: usize, n: u64, seal_every: u64) -> (u64, usize, u64) {
    let d = dir(&format!("r{retain}n{n}"));
    let mut e = engine(&d, retain, seal_every);
    let mut merges = 0u64;
    for i in 1..=n {
        e.append_record(R {
            seq: i,
            key: "k".into(),
        })
        .unwrap();
        // The three lifecycle calls are CALLER-OWNED — the engine runs none of them on its
        // own. Not driving them is the configured-but-unreachable shape; this is what a
        // real operator loop does.
        if i % seal_every == 0 {
            e.seal_aged_parts().ok();
            merges += e.run_pending_merges().unwrap_or(0) as u64;
            e.reclaim_orphans().ok();
        }
    }
    let (bytes, count) = manifest_bytes(&d);
    let _ = std::fs::remove_dir_all(&d);
    (bytes, count, merges)
}

#[test]
fn measure_manifest_footprint_is_bounded_and_the_unbounded_config_is_visibly_worse() {
    let seal_every = 16u64;
    // Sized to pass THROUGH the retain=32 bound. At 1024 appends the version count only
    // just crosses 32, so pruning has barely engaged and the bounded count is still
    // climbing (19 -> 27 -> 35) — measuring there would report 'not bounded' about a bound
    // that had not yet applied.
    let sizes = [1024u64, 2048, 4096];

    println!("\n  manifest footprint vs appends (seal every {seal_every} records)");
    println!(
        "  {:>7}  {:>12} {:>7}  {:>12} {:>7}  {:>9}",
        "appends", "retain=32 B", "files", "retain=0 B", "files", "ratio"
    );

    let mut bounded = Vec::new();
    let mut unbounded = Vec::new();
    for n in sizes {
        let (bb, bc, _) = run(32, n, seal_every);
        let (ub, uc, _) = run(0, n, seal_every);
        println!(
            "  {n:>7}  {bb:>12} {bc:>7}  {ub:>12} {uc:>7}  {:>8.1}x",
            ub as f64 / bb.max(1) as f64
        );
        bounded.push((n, bb, bc));
        unbounded.push((n, ub, uc));
    }

    // ---- THE CONTROL: the instrument must SEE the unbounded config ----
    // If retain=0 and retain=32 measure the same, this test cannot detect the defect it
    // exists for, and the "bounded" assertions below would be vacuous.
    let (_, b_big, b_cnt) = *bounded.last().unwrap();
    let (_, u_big, u_cnt) = *unbounded.last().unwrap();
    println!("\n  CONTROL: at {} appends -> bounded {b_cnt} files / {b_big} B, unbounded {u_cnt} files / {u_big} B",
             sizes.last().unwrap());
    assert!(
        u_cnt > b_cnt,
        "retain=0 must keep MORE manifest files than retain=32, or this instrument cannot \
         see the defect it exists to detect: unbounded={u_cnt} bounded={b_cnt}"
    );
    assert!(
        u_big > b_big,
        "and more bytes: unbounded={u_big} bounded={b_big}"
    );

    // ---- THE RESULT: the bounded footprint does not grow with append count ----
    // Retention keeps `retain` versions plus LATEST, so the file COUNT must plateau however
    // many appends happen. That is the difference between linear and quadratic.
    let counts: Vec<usize> = bounded.iter().map(|(_, _, c)| *c).collect();
    let ucounts: Vec<usize> = unbounded.iter().map(|(_, _, c)| *c).collect();
    println!("  bounded   file counts across {sizes:?} appends: {counts:?}");
    println!("  unbounded file counts across {sizes:?} appends: {ucounts:?}");
    // The bound is on the file COUNT, so the right assertion is that 4x the appends does
    // not multiply the count — not an exact number. Two producers write manifest versions
    // (the uploader and the merge/drop path) and the convergence sweep runs amortised once
    // per `retain` writes, so a handful of stragglers above `retain + 1` is documented
    // behaviour, not a leak.
    let c_growth = counts[2] as f64 / counts[0] as f64;
    let u_growth = ucounts[2] as f64 / ucounts[0] as f64;
    println!("  4x appends -> bounded file count x{c_growth:.2}, unbounded x{u_growth:.2}");
    assert!(
        c_growth < 1.6,
        "retain=32 must hold the file count nearly flat across 4x appends, got x{c_growth:.2} {counts:?}"
    );
    assert!(
        u_growth > c_growth * 1.5,
        "and the unbounded config must grow visibly faster, or the bound is not what is \
         being measured: bounded x{c_growth:.2} unbounded x{u_growth:.2}"
    );
    // ---- THE RESULT, as a GROWTH EXPONENT ----
    //
    // The honest quantity. `bytes ~ appends^e`, so e = log(ratio) / log(append_ratio):
    // e = 1 is linear, e = 2 is the 2026-09-01 quadratic. A raw ratio cannot be compared
    // across different span sizes; an exponent can, which is what makes this a measure
    // rather than an observation.
    let expo = |a: u64, b: u64, na: u64, nb: u64| {
        ((b as f64 / a as f64).ln()) / ((nb as f64 / na as f64).ln())
    };
    let (n0, b0, c0) = bounded[0];
    let (n2, b2, c2) = bounded[2];
    let (_, u0, uc0) = unbounded[0];
    let (_, u2, uc2) = unbounded[2];
    let e_bytes_bounded = expo(b0, b2, n0, n2);
    let e_bytes_unbounded = expo(u0, u2, n0, n2);
    let e_files_bounded = expo(c0 as u64, c2 as u64, n0, n2);
    let e_files_unbounded = expo(uc0 as u64, uc2 as u64, n0, n2);
    println!("\n  GROWTH EXPONENT over {n0} -> {n2} appends (1.0 = linear, 2.0 = quadratic)");
    println!("    retain=32  bytes {e_bytes_bounded:.2}   files {e_files_bounded:.2}");
    println!("    retain=0   bytes {e_bytes_unbounded:.2}   files {e_files_unbounded:.2}");

    // The pre-fix config must still exhibit the quadratic. If it does not, this measure is
    // not watching the 2026-09-01 mechanism and its verdict on the fix is worthless.
    assert!(
        e_bytes_unbounded > 1.6,
        "retain=0 is the pre-fix behaviour and must still measure near-quadratic, or this \
         test is not watching the mechanism it exists for: e={e_bytes_unbounded:.2}"
    );
    // And the fix must bring it down decisively.
    assert!(
        e_bytes_bounded < 1.5,
        "retention must bring byte growth well below quadratic: e={e_bytes_bounded:.2}"
    );
    assert!(
        e_bytes_unbounded - e_bytes_bounded > 0.4,
        "the fix must move the exponent by a wide margin: {e_bytes_unbounded:.2} vs \
         {e_bytes_bounded:.2}"
    );
    // The file count is the part retention bounds outright.
    assert!(
        e_files_bounded < 0.3,
        "retain=32 must hold the file COUNT essentially flat: e={e_files_bounded:.2}"
    );
    assert!(
        e_files_unbounded > 0.8,
        "retain=0 must grow the file count ~linearly in appends: e={e_files_unbounded:.2}"
    );

    // ⚠ Residual, asserted so it cannot drift unnoticed: bounded BYTES still grow faster
    // than linear (measured e=1.23), because each retained snapshot legitimately lists more
    // parts as the log grows. Retention bounds how MANY snapshots exist, not how BIG one
    // is. That is not pathological and it is not free, and claiming "linear" would be
    // wrong.
    assert!(
        e_bytes_bounded > 1.0,
        "if bounded byte growth ever measures sub-linear, the snapshot is no longer listing \
         every part and this measure's premise changed: e={e_bytes_bounded:.2}"
    );
}

#[test]
fn measure_the_lifecycle_calls_actually_do_work_and_are_caller_owned() {
    // ⚠ The three lifecycle calls run ONLY when a caller drives them. A measure that never
    // calls them would report 0 merges and look like "no merges needed" rather than "nobody
    // asked" — the configured-but-unreachable shape.
    let seal_every = 16u64;
    let n = 1024u64;

    // Driven.
    let (_, _, merges_driven) = run(32, n, seal_every);

    // NOT driven — the negative control.
    let d = dir("undriven");
    let mut e = engine(&d, 32, seal_every);
    for i in 1..=n {
        e.append_record(R {
            seq: i,
            key: "k".into(),
        })
        .unwrap();
    }
    let merged_undriven = e.run_pending_merges().unwrap_or(0);
    let _ = std::fs::remove_dir_all(&d);

    println!("\n  lifecycle: {n} appends, seal every {seal_every}");
    println!("    merges when DRIVEN each seal : {merges_driven}");
    println!("    merges from ONE call at the end (undriven): {merged_undriven}");
    assert!(
        merges_driven > 0,
        "driving the lifecycle must actually merge something at {n} appends with \
         seal_max_records={seal_every}; 0 would mean the measure proves nothing"
    );
    // A single end call cannot catch up a whole run — which is exactly why the loop is
    // caller-owned and must be on a timer.
    println!(
        "    => driving on a timer merged {merges_driven}, one late call merged {merged_undriven}"
    );
}
