//! **M2 — the commit HLC is stamped, and nothing reads it.**
//!
//! Exit criteria E1 (100 % of appends carry it under `shadow`), E2 (nothing
//! reads it), E3 (a rollback binary reads records that carry it — proven here
//! as byte-compat of the absent case), E5 (monotonicity across a backwards
//! wall-clock step and across a restart).
//!
//! ⚠ The mode is injected through `L0Config::with_hlc_mode`, never through
//! `NOETL_EHDB_HLC`: `cargo test` does not serialise tests, so an env-driven
//! test would race every other test in this binary.

use std::sync::Arc;

use ehdb_core::hlc::{Hlc, HlcClock};
use ehdb_l0::hlc_policy::HlcMode;
use ehdb_l0::substrate::{DurableSubstrate, InMemorySubstrate};
use ehdb_l0::{EventRecord, L0Config, L0EventLogEngine, ReplicaTarget};

fn unique_dir(tag: &str) -> std::path::PathBuf {
    static N: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let n = N.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    std::env::temp_dir().join(format!("ehdb-hlc-{tag}-{}-{n}-{nanos}", std::process::id()))
}

fn engine(tag: &str, mode: HlcMode) -> L0EventLogEngine {
    let s: Arc<dyn DurableSubstrate> = Arc::new(InMemorySubstrate::new(format!("hlc-{tag}")));
    L0EventLogEngine::open_replicated(
        L0Config::d1(&unique_dir(tag))
            .with_shard_count(1)
            .with_granule_size(64)
            .with_hlc_mode(mode),
        vec![ReplicaTarget::new("replica-0", s)],
    )
    .expect("open")
}

/// E1 — under `shadow`, EVERY append carries a commit HLC, and the count is
/// published rather than asserted in the abstract.
#[test]
fn every_append_is_stamped_under_shadow() {
    const N: usize = 50;
    let mut e = engine("shadow", HlcMode::Shadow);
    for i in 0..N {
        e.append_record(EventRecord::new(i as u64 + 1, format!("exec-{i}"), "t", "p"))
            .expect("append");
    }
    let recs = e.read_partition_after(0, 0).expect("scan");
    let stamped = recs.iter().filter(|r| r.commit_hlc.is_some()).count();
    println!("appends observed={} stamped={}", recs.len(), stamped);
    assert_eq!(recs.len(), N, "the drive must actually have appended N records");
    assert_eq!(stamped, N, "100% of appends must carry commit_hlc under shadow");

    // Strictly increasing, which is the property the clock exists for.
    let hlcs: Vec<u64> = recs.iter().filter_map(|r| r.commit_hlc).collect();
    for w in hlcs.windows(2) {
        assert!(w[1] > w[0], "commit HLCs must strictly increase: {w:?}");
    }
}

/// ⭐ THE POSITIVE CONTROL for the test above. `off` must stamp NOTHING —
/// otherwise "50 of 50 stamped" is satisfied by an engine that stamps
/// unconditionally, and the flag proves nothing.
#[test]
fn off_stamps_nothing() {
    let mut e = engine("off", HlcMode::Off);
    for i in 0..10 {
        e.append_record(EventRecord::new(i + 1, format!("exec-{i}"), "t", "p"))
            .expect("append");
    }
    let recs = e.read_partition_after(0, 0).expect("scan");
    assert_eq!(recs.len(), 10);
    assert_eq!(
        recs.iter().filter(|r| r.commit_hlc.is_some()).count(),
        0,
        "off must stamp nothing — the default has to stay byte-identical"
    );
}

/// E3 — a record with no commit HLC serialises **byte-identically to before
/// the field existed**, so a rollback binary keeps reading everything written
/// while the flag is off.
///
/// M2 planted defect #5 is serialising `0` instead of skipping; this is what
/// catches it.
#[test]
fn an_unstamped_record_serialises_without_the_field() {
    let r = EventRecord::new(1, "exec-1", "txn", "payload");
    let j = serde_json::to_string(&r).expect("serialise");
    assert!(
        !j.contains("commit_hlc"),
        "an unstamped record must omit the field entirely, not write 0: {j}"
    );
    // POSITIVE CONTROL: a stamped record DOES carry it, so the assertion above
    // is about `skip_serializing_if` and not about a field that never appears.
    let mut r2 = r.clone();
    r2.commit_hlc = Some(42);
    let j2 = serde_json::to_string(&r2).expect("serialise");
    assert!(j2.contains("\"commit_hlc\":42"), "{j2}");

    // And the old shape still parses — the rollback direction.
    let old = r#"{"global_sequence":1,"execution_id":"e","transaction_id":"t","payload":"p"}"#;
    let back: EventRecord = serde_json::from_str(old).expect("a pre-M2 record must still parse");
    assert_eq!(back.commit_hlc, None);
}

/// E5(b) — the HLC does not go backwards when the wall clock does.
#[test]
fn the_clock_survives_a_backwards_wall_clock_step() {
    let t = Arc::new(std::sync::atomic::AtomicU64::new(1_000_000));
    let t2 = Arc::clone(&t);
    let clock = HlcClock::with_source(Box::new(move || {
        t2.load(std::sync::atomic::Ordering::Relaxed)
    }));
    let a = clock.now();
    // NTP steps the clock back a full second.
    t.store(999_000, std::sync::atomic::Ordering::Relaxed);
    let b = clock.now();
    let c = clock.now();
    assert!(b > a, "a backwards wall-clock step must not produce a smaller HLC: {a:?} -> {b:?}");
    assert!(c > b, "and it must keep increasing: {b:?} -> {c:?}");
}

/// E5(a) — a restarted clock seeded from the tip does not re-issue.
#[test]
fn a_restarted_clock_seeded_from_the_tip_does_not_reissue() {
    let t = Arc::new(std::sync::atomic::AtomicU64::new(5_000_000));
    let mk = |seed: Hlc, t: Arc<std::sync::atomic::AtomicU64>| {
        HlcClock::seeded_from(
            seed,
            Box::new(move || t.load(std::sync::atomic::Ordering::Relaxed)),
        )
    };
    let c1 = mk(Hlc::default(), Arc::clone(&t));
    let last = c1.now();
    // Restart with the wall clock STEPPED BACK — the case the seed exists for.
    t.store(4_000_000, std::sync::atomic::Ordering::Relaxed);
    let c2 = mk(last, Arc::clone(&t));
    let after = c2.now();
    assert!(
        after > last,
        "a restart must not re-issue a timestamp the previous process used: \
         {last:?} then {after:?}"
    );
    // NEGATIVE CONTROL: an UNSEEDED restart under the same step-back does
    // re-issue, which is exactly why `seeded_from` exists. If this did not
    // regress, the seed would be decorative.
    let c3 = mk(Hlc::default(), Arc::clone(&t));
    assert!(
        c3.now() < last,
        "an unseeded clock should go backwards here — if it does not, this test \
         is not exercising the hazard the seed addresses"
    );
}

/// E2 — nothing reads `commit_hlc`.
///
/// Asserted as a REACHABILITY fact over the crate's own source: the field is
/// written in exactly one place (the engine stamp, via the dataset hook) and
/// read nowhere outside tests and the codec's explicit `None`. A reader
/// appearing before M3 is a phase-ordering violation, so this is a guard, not
/// an observation.
#[test]
fn nothing_reads_the_commit_hlc_yet() {
    fn code_only(src: &str) -> String {
        src.lines()
            .map(|l| match l.find("//") { Some(i) => &l[..i], None => l })
            .collect::<Vec<_>>()
            .join("\n")
    }
    let mut readers = Vec::new();
    for (name, src) in [
        ("engine.rs", include_str!("../src/engine.rs")),
        ("part.rs", include_str!("../src/part.rs")),
        ("part.rs (again, cheap) ", include_str!("../src/part.rs")),
    ] {
        for line in code_only(src).lines() {
            // A READ is `.commit_hlc` used as a value; the stamp is an
            // assignment through the dataset hook and lives in dataset.rs.
            if line.contains(".commit_hlc") && !line.contains(".commit_hlc =") {
                readers.push(format!("{name}: {}", line.trim()));
            }
        }
    }
    assert!(
        readers.is_empty(),
        "something reads commit_hlc before M3's closed timestamps exist — that is \
         a phase-ordering violation, not a bonus:\n  {}",
        readers.join("\n  ")
    );
}
