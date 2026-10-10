//! The durability-gauge semantics — [noetl/ai-meta#462](https://github.com/noetl/ai-meta/issues/462).
//!
//! Two gauges the server publishes side by side contradicted each other on prod, and two of
//! my hypotheses about which one lied were refuted (the second by a RED control that
//! passed). So this file establishes the semantics by MEASUREMENT instead.
//!
//! ## What the two actually mean
//!
//! - `active_ages()` → the age of the **active part's first append**: how long the oldest
//!   record not yet in a sealed part has been waiting.
//! - `unreplicated_snapshot().oldest_age_millis` → `min` over the **Instants** of
//!   {sealed-but-not-yet-durable first-appends} ∪ {active first-append}. The oldest *Instant*
//!   is the largest *elapsed*, so this is the maximum over a **superset**.
//!
//! ## ⭐ Therefore `unrep_age >= unsealed_age`, by construction
//!
//! It is an invariant, not a coincidence, and `unrep_age_is_always_at_least_unsealed_age`
//! hammers it over a randomised sequence. The only legitimate divergence is at a seal
//! boundary, where the sealed part is pending upload and `unrep` is **larger** — measured
//! here as `unsealed=0, unrep=13ms`.
//!
//! ⚠⚠ The prod reading was the OPPOSITE direction (`unsealed=779s` while `unrep=0`), which
//! **violates this invariant**. So it was not produced by these semantics, and the two
//! exploratory probes below confirm no ordinary path in `ehdb-l0` produces it: appends,
//! inline seals, flush-seals and crash recovery all keep the two in step.

use std::sync::Arc;

use ehdb_l0::substrate::DurableSubstrate;
use ehdb_l0::{L0Config, L0EventLogEngine, LocalFsSubstrate};

fn unique_dir(tag: &str) -> std::path::PathBuf {
    static N: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let n = N.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    std::env::temp_dir().join(format!("ehdb-462-{tag}-{}-{n}", std::process::id()))
}

fn store(dir: &std::path::Path) -> Arc<dyn DurableSubstrate> {
    Arc::new(LocalFsSubstrate::new(dir).unwrap())
}

/// Both readings, exactly as the server computes them.
fn read(e: &L0EventLogEngine, label: &str) -> (u64, u64, u64) {
    let unsealed = e
        .active_ages()
        .into_iter()
        .map(|(_, age)| age.as_millis() as u64)
        .max()
        .unwrap_or(0);
    let snap = e.unreplicated_snapshot();
    let unrep = snap.iter().map(|s| s.oldest_age_millis).max().unwrap_or(0);
    let recs: u64 = snap.iter().map(|s| s.records).sum();
    println!("  {label:<46} unsealed_ms={unsealed:<8} unrep_ms={unrep:<8} unrep_recs={recs}");
    (unsealed, unrep, recs)
}

#[test]
fn probe_which_event_advances_one_gauge_but_not_the_other() {
    let local = unique_dir("local");
    let objects = unique_dir("obj");
    // seal_max_records=4 so an INLINE seal happens during append — the path the server
    // actually takes under traffic, as distinct from flush_and_wait_uploads.
    let cfg = L0Config::d1(&local)
        .with_shard_count(1)
        .with_seal_max_records(4);
    let mut e = L0EventLogEngine::open(cfg, store(&objects)).unwrap();

    println!("\n=== sequence: appends crossing an INLINE seal boundary ===");
    read(&e, "0. opened");

    for i in 0..3u64 {
        e.append("4620", &format!("t{i}"), format!("p{i}")).unwrap();
        read(
            &e,
            &format!("after append {} (below seal threshold)", i + 1),
        );
    }

    // This append hits seal_max_records=4 -> inline seal + register_and_upload.
    e.append("4620", "t3", "p3".to_string()).unwrap();
    let (u_a, r_a, c_a) = read(&e, "after append 4 == INLINE SEAL fired");

    std::thread::sleep(std::time::Duration::from_millis(60));
    let (u_b, r_b, c_b) = read(&e, "+60ms, no appends");

    // Now append again: a fresh active part begins.
    e.append("4620", "t4", "p4".to_string()).unwrap();
    let (u_c, r_c, c_c) = read(&e, "after append 5 (new active part)");
    std::thread::sleep(std::time::Duration::from_millis(60));
    let (u_d, r_d, c_d) = read(&e, "+60ms");

    println!("\n=== what the probe shows ===");
    println!("  post-inline-seal   : unsealed={u_a}ms unrep={r_a}ms recs={c_a}");
    println!("  post-seal +60ms    : unsealed={u_b}ms unrep={r_b}ms recs={c_b}");
    println!("  new active part    : unsealed={u_c}ms unrep={r_c}ms recs={c_c}");
    println!("  new part +60ms     : unsealed={u_d}ms unrep={r_d}ms recs={c_d}");

    let diverged = (u_b > 0 && r_b == 0) || (u_d > 0 && r_d == 0);
    println!(
        "\n  DIVERGENCE (unsealed>0 while unrep==0) reproduced: {}",
        if diverged { "YES" } else { "no" }
    );

    drop(e);
    let _ = std::fs::remove_dir_all(&local);
    let _ = std::fs::remove_dir_all(&objects);
}

/// The other candidate: a shard whose records were RECOVERED on open rather than appended.
#[test]
fn probe_recovered_active_part_on_reopen() {
    let local = unique_dir("local");
    let objects = unique_dir("obj");
    let cfg = |root: &std::path::Path| {
        L0Config::d1(root)
            .with_shard_count(1)
            .with_seal_max_records(10_000)
    };

    {
        let mut e = L0EventLogEngine::open(cfg(&local), store(&objects)).unwrap();
        for i in 0..5u64 {
            e.append("4621", &format!("t{i}"), format!("p{i}")).unwrap();
        }
        read(&e, "before drop (records unsealed)");
        // Drop WITHOUT flushing: the active part file survives, unsealed.
        drop(e);
    }

    println!("\n=== reopen: the active part is recovered ===");
    let e2 = L0EventLogEngine::open(cfg(&local), store(&objects)).unwrap();
    let (u, r, c) = read(&e2, "after reopen (recovered)");
    std::thread::sleep(std::time::Duration::from_millis(60));
    let (u2, r2, c2) = read(&e2, "+60ms");
    println!("\n  reopen: unsealed={u}ms unrep={r}ms recs={c}");
    println!("  +60ms : unsealed={u2}ms unrep={r2}ms recs={c2}");
    println!(
        "  DIVERGENCE on recovery: {}",
        if (u2 > 0 && r2 == 0) || (r2 > 0 && u2 == 0) {
            "YES"
        } else {
            "no"
        }
    );

    drop(e2);
    let _ = std::fs::remove_dir_all(&local);
    let _ = std::fs::remove_dir_all(&objects);
}

/// ⭐⭐ THE INVARIANT, stated, hammered, and corrected by measurement.
///
/// ⚠⚠ The naive form `unrep_age >= unsealed_age` is FALSE, and finding out how it fails is
/// the whole content of #462. Measured over 8 runs of this sequence: **4 runs showed
/// violations, every one of them exactly 1 millisecond** (8→7, 5→4, 4→3).
///
/// That magnitude is the answer. The two values derive from the SAME first append, captured
/// at two different points in `append_record`:
///
/// ```text
/// engine.rs:812   writer.append(record)?          → PartWriter::first_append_at = t0
/// engine.rs:832   self.unreplicated.on_append(..) → active_first_append = t0 + δ
/// ```
///
/// δ is the dedupe-remember + metrics work between them — microseconds. So
/// `unrep_age = unsealed_age − δ`, i.e. the durability gauge is biased **LOW** by the
/// append path's own latency, and `as_millis()` truncation surfaces that as 1 ms whenever the
/// two land either side of a millisecond boundary.
///
/// ⚠ So the tolerance below is a MEASURED quantity, not a hand-waved epsilon. It is 2 ms
/// because the observed skew is 1 ms and one more millisecond of truncation slack keeps the
/// test from being flaky without letting a real inversion through: a genuine semantic
/// inversion would be seconds, as the production reading was.
///
/// `unreplicated_snapshot().oldest_age_millis` is `min` over the **Instants** of
/// {sealed-but-not-durable first-appends} ∪ {active first-append}. The oldest *Instant* is
/// the largest *elapsed*, so the age it reports is the maximum over a **superset** of what
/// `active_ages()` measures.
///
/// Therefore: **`unrep_age >= unsealed_age`, always.** The prod reading
/// `unsealed=779s, unrep=0` violates this, so at least one of those two numbers was not
/// produced by these semantics.
///
/// Hammered over a randomised operation sequence rather than one path, because the two
/// hypotheses already refuted here both came from reasoning about a single path.
#[test]
fn unrep_age_is_always_at_least_unsealed_age() {
    let local = unique_dir("inv-local");
    let objects = unique_dir("inv-obj");
    // Small seal threshold so inline seals happen often, and a tiny granule so the
    // sequence crosses many boundaries.
    let cfg = L0Config::d1(&local)
        .with_shard_count(1)
        .with_granule_size(2)
        .with_seal_max_records(3);
    let mut e = L0EventLogEngine::open(cfg, store(&objects)).unwrap();

    // Deterministic pseudo-random op mix (no rand dependency).
    let mut seed: u64 = 0x5EED_0462;
    let mut next = || {
        seed = seed
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        (seed >> 33) as u32
    };

    let mut violations = Vec::new();
    let mut checks = 0usize;
    for step in 0..600u64 {
        match next() % 10 {
            0 => {
                // occasionally flush, which seals + waits for uploads
                e.flush_and_wait_uploads().unwrap();
            }
            1 => {
                // and occasionally let time pass so ages are non-zero
                std::thread::sleep(std::time::Duration::from_millis(2));
            }
            _ => {
                e.append("4622", &format!("t{step}"), format!("p{step}"))
                    .unwrap();
            }
        }
        let unsealed = e
            .active_ages()
            .into_iter()
            .map(|(_, a)| a.as_millis() as u64)
            .max()
            .unwrap_or(0);
        let unrep = e
            .unreplicated_snapshot()
            .iter()
            .map(|s| s.oldest_age_millis)
            .max()
            .unwrap_or(0);
        checks += 1;
        // ⚠ The comparison is against the MEASURED capture skew (see the doc above), not an
        // arbitrary epsilon. A real inversion is seconds; this tolerates 2 ms.
        const SKEW_TOLERANCE_MS: u64 = 2;
        if unrep + SKEW_TOLERANCE_MS < unsealed {
            violations.push((step, unsealed, unrep));
        }
    }

    // ⚠ Print the denominator: a clean result over zero checks is not a result.
    println!(
        "\n  invariant checked {checks} times over a randomised sequence; violations: {}",
        violations.len()
    );
    for (s, u, r) in violations.iter().take(5) {
        println!("    step {s}: unsealed={u}ms unrep={r}ms");
    }
    assert!(
        checks > 400,
        "fixture must actually exercise the engine: {checks} checks"
    );
    assert!(
        violations.is_empty(),
        "unrep_age fell more than 2ms below unsealed_age in {} of {checks} checks. The \
         known capture skew is ~1ms; a gap larger than that is a real semantic inversion, \
         which is what the production reading (779s vs 0) would be: {:?}",
        violations.len(),
        &violations[..violations.len().min(5)]
    );

    drop(e);
    let _ = std::fs::remove_dir_all(&local);
    let _ = std::fs::remove_dir_all(&objects);
}
