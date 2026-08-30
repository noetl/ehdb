//! **The D1 durability-window soak** (noetl/ehdb#261, design in
//! `playbooks/261-durability-soak/`).
//!
//! Produces the measured number that
//! `docs/spec/durability-window.md` §7 puts on the primary-serve gate: how long
//! an acknowledged record sits on one disk before reaching the substrate.
//!
//! ## ⚠⚠ Why the load shape matters more than the rate
//!
//! Sealing triggers on `seal_max_bytes` (8 MiB) or `seal_max_records` (1024) and
//! — unless `seal_max_age` is set (noetl/ehdb#329) — **never on age**. A shard
//! under sustained load seals constantly and shows a window of seconds, while an
//! idle shard's grows without bound.
//!
//! So a uniform saturating soak **cannot detect the defect it is being run to
//! measure**. It would report an excellent p99 and confirm health that is not
//! there. This harness therefore drives four deliberately different arms and
//! **reports per arm**, never pooled.
//!
//! ## Runs entirely in-process
//!
//! Temp dirs, a `LocalFsSubstrate`, no cluster, no prod. Safe in CI.
//!
//! ```text
//! cargo run -p ehdb-l0 --example durability_soak -- --seconds 30
//! cargo run -p ehdb-l0 --example durability_soak -- --seconds 30 --seal-max-age-ms 5000
//! ```
//!
//! The second form is the **after** measurement for noetl/ehdb#329: the same
//! four arms with the age trigger on. Nothing here touches prod, and the flag is
//! local to this process.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use ehdb_l0::substrate::DurableSubstrate;
use ehdb_l0::{
    shard_for_execution, D1EventLog, EventRecord, L0Config, L0Engine, LocalFsSubstrate,
    ReplicaTarget,
};

/// One load shape. `label` names it in the report; the arm is pinned to its own
/// shard so the arms cannot contaminate each other's window.
struct Arm {
    label: &'static str,
    shard: u32,
    execution_id: String,
    /// Append every `period`; `None` means "append once at the start, then never
    /// again" — the quiet arm.
    period: Option<Duration>,
    /// Burst/idle cycle: append only while inside the burst.
    duty: Option<(Duration, Duration)>,
    next_seq: u64,
    appended: u64,
}

/// Find an execution id that lands on `shard`. D1 partitions by a hash of the
/// id, so the arms are pinned by search rather than by assumption.
fn exec_id_for_shard(shard: u32, shard_count: u32) -> String {
    for n in 0..200_000u64 {
        let candidate = format!("exec-soak-{n}");
        if shard_for_execution(&candidate, shard_count) == shard {
            return candidate;
        }
    }
    panic!("no execution id maps to shard {shard} of {shard_count}");
}

fn arg(name: &str, default: u64) -> u64 {
    let args: Vec<String> = std::env::args().collect();
    args.iter()
        .position(|a| a == name)
        .and_then(|i| args.get(i + 1))
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn main() {
    let seconds = arg("--seconds", 30);
    let seal_max_age_ms = arg("--seal-max-age-ms", 0);
    let shard_count = 4u32;

    let root = std::env::temp_dir().join(format!(
        "ehdb-soak-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0)
    ));
    let local = root.join("local");
    let sub = root.join("substrate");
    std::fs::create_dir_all(&sub).unwrap();

    let mut config = L0Config::d1(&local).with_shard_count(shard_count);
    if seal_max_age_ms > 0 {
        config = config.with_seal_max_age(Some(Duration::from_millis(seal_max_age_ms)));
    }
    let substrate: Arc<dyn DurableSubstrate> = Arc::new(LocalFsSubstrate::new(&sub).unwrap());
    let mut engine = L0Engine::<D1EventLog>::open_replicated(
        config,
        vec![ReplicaTarget::new("replica-0", substrate)],
    )
    .unwrap();

    let mut arms = vec![
        // A — saturating. Seals constantly; the arm a naive soak runs alone.
        Arm {
            label: "A saturating",
            shard: 0,
            execution_id: exec_id_for_shard(0, shard_count),
            period: Some(Duration::from_millis(2)),
            duty: None,
            next_seq: 1,
            appended: 0,
        },
        // B — quiet. ⚠ THE POINT OF THE SOAK. Three records, then silence.
        Arm {
            label: "B quiet",
            shard: 1,
            execution_id: exec_id_for_shard(1, shard_count),
            period: None,
            duty: None,
            next_seq: 1,
            appended: 0,
        },
        // C — trickle. Neither busy nor idle, and the shape most production
        // shards actually have. The one most likely to be left out.
        Arm {
            label: "C trickle",
            shard: 2,
            execution_id: exec_id_for_shard(2, shard_count),
            period: Some(Duration::from_millis(3_000)),
            duty: None,
            next_seq: 1,
            appended: 0,
        },
        // D — bursty. Does a burst's tail get stranded when load stops?
        Arm {
            label: "D bursty",
            shard: 3,
            execution_id: exec_id_for_shard(3, shard_count),
            period: Some(Duration::from_millis(5)),
            duty: Some((Duration::from_secs(3), Duration::from_secs(7))),
            next_seq: 1,
            appended: 0,
        },
    ];

    println!(
        "soak: {seconds}s, {shard_count} shards, seal_max_age={}\n",
        if seal_max_age_ms > 0 {
            format!("{seal_max_age_ms}ms")
        } else {
            "OFF (today's prod behaviour)".to_string()
        }
    );
    for a in &arms {
        println!("  {:<14} shard {}  ({})", a.label, a.shard, a.execution_id);
    }
    println!();

    // max unreplicated age + max pending records, per shard.
    let mut peak: BTreeMap<u32, (u64, u64)> = BTreeMap::new();
    let start = Instant::now();
    let mut last_append: BTreeMap<u32, Instant> = BTreeMap::new();
    let mut last_sample = Instant::now();

    while start.elapsed() < Duration::from_secs(seconds) {
        let now = Instant::now();
        for a in arms.iter_mut() {
            let due = match (a.period, a.duty) {
                (None, _) => a.appended < 3, // quiet: 3 records then silence
                (Some(p), None) => last_append
                    .get(&a.shard)
                    .map(|t| now.duration_since(*t) >= p)
                    .unwrap_or(true),
                (Some(p), Some((burst, idle))) => {
                    let cycle = (burst + idle).as_millis() as u64;
                    let pos = (start.elapsed().as_millis() as u64) % cycle;
                    pos < burst.as_millis() as u64
                        && last_append
                            .get(&a.shard)
                            .map(|t| now.duration_since(*t) >= p)
                            .unwrap_or(true)
                }
            };
            if due {
                let r = EventRecord::new(
                    a.next_seq,
                    &a.execution_id,
                    format!("txn-{}", a.next_seq),
                    format!("payload-{}-{}", a.label, a.next_seq),
                );
                // append_writer_assigned keeps the shard log ascending by
                // construction, matching how the writer appends.
                engine.append_writer_assigned(r).unwrap();
                a.next_seq += 1;
                a.appended += 1;
                last_append.insert(a.shard, now);
            }
        }

        if last_sample.elapsed() >= Duration::from_millis(500) {
            if seal_max_age_ms > 0 {
                // ⚠ The age trigger is inert without something driving it: an
                // idle shard takes no appends, so should_seal() is never
                // consulted for it. This is the timer the deployment would need.
                engine.seal_aged_parts().unwrap();
            }
            for row in engine.unreplicated_snapshot() {
                let e = peak.entry(row.shard).or_insert((0, 0));
                e.0 = e.0.max(row.oldest_age_millis);
                e.1 = e.1.max(row.records);
            }
            last_sample = Instant::now();
        }
    }

    // Final sample before the report.
    for row in engine.unreplicated_snapshot() {
        let e = peak.entry(row.shard).or_insert((0, 0));
        e.0 = e.0.max(row.oldest_age_millis);
        e.1 = e.1.max(row.records);
    }

    let m = engine.metrics().snapshot();
    let (_buckets, lag_count, lag_sum) = engine.metrics().replicated_lag.snapshot();

    println!("── per arm ───────────────────────────────────────────────");
    println!(
        "{:<14} {:>9} {:>12} {:>10}",
        "arm", "appends", "max age (s)", "max pend"
    );
    for a in &arms {
        let (age, pend) = peak.get(&a.shard).copied().unwrap_or((0, 0));
        println!(
            "{:<14} {:>9} {:>12.3} {:>10}",
            a.label,
            a.appended,
            age as f64 / 1000.0,
            pend
        );
    }

    println!("\n── engine ────────────────────────────────────────────────");
    println!("  appends            {}", m.appends);
    println!("  seals              {}", m.seals);
    println!("  uploads            {}", m.uploads);
    println!("  replicated samples {lag_count}");
    println!(
        "  mean append→durable {:.3}s",
        if lag_count > 0 {
            lag_sum / lag_count as f64
        } else {
            0.0
        }
    );
    println!(
        "  ⚠ seal-relative mean (upload_lag) {:.3}s  ← blind to the pre-seal term",
        m.mean_upload_lag_micros() as f64 / 1_000_000.0
    );

    // ---- the soak's own positive control ---------------------------------
    let quiet = arms.iter().find(|a| a.label == "B quiet").unwrap();
    let (quiet_age, quiet_pend) = peak.get(&quiet.shard).copied().unwrap_or((0, 0));
    println!("\n── verdict ───────────────────────────────────────────────");
    if seal_max_age_ms == 0 {
        // ⚠ The quiet arm MUST have grown. If it did not, the fault is in the
        // soak — the shard took traffic it should not have, or the metric is not
        // wired — not in the system. A soak that cannot show the defect proves
        // nothing about its absence.
        assert!(
            quiet_pend > 0,
            "soak self-check FAILED: the quiet arm holds no pending records, so \
             this run measured nothing"
        );
        let floor = (seconds * 1000).saturating_sub(3_000);
        assert!(
            quiet_age >= floor,
            "soak self-check FAILED: the quiet arm's window reached only {quiet_age}ms \
             over a {seconds}s run. Expected it to grow unbounded — check that the \
             shard is genuinely idle and that the metric is wired."
        );
        println!(
            "  ✅ reproduced: the quiet arm's window grew to {:.1}s and never sealed.",
            quiet_age as f64 / 1000.0
        );
        println!("     A saturating-only soak would have reported health here.");
    } else {
        let bound = seal_max_age_ms * 3;
        assert!(
            quiet_age <= bound,
            "age trigger FAILED to bound the quiet arm: {quiet_age}ms > {bound}ms"
        );
        println!(
            "  ✅ bounded: the quiet arm's window peaked at {:.1}s under a {}ms trigger.",
            quiet_age as f64 / 1000.0,
            seal_max_age_ms
        );
        println!(
            "     seals={} — measure this against the run above for part-count cost.",
            m.seals
        );
    }

    let _ = std::fs::remove_dir_all(&root);
}
