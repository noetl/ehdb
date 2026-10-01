//! In-engine `FeedWriter` A/B: what does the chain certificate cost the REAL
//! append/commit path once it is wired into the engine?
//!
//! Lineage: noetl/server#480 measured a per-event re-serialising prototype at
//! -17%; #481 modelled an amortized variant on a stand-in loop; noetl/ehdb#378
//! measured the real `FeedWriter` but with the digest computed *beside* the
//! engine (absorbing each record's payload from the example). This measures the
//! integration itself — `L0Config::chain_cert` on vs off, with the digest taken
//! inside `PartWriter::append` over the bytes it writes.
//!
//! Run (the feature is off by default):
//!   cargo run --release -p ehdb-feed --features chain-cert     --example append_cert_bench
//!   cargo run --release -p ehdb-feed --features chain-cert-asm --example append_cert_bench
//!
//! ⚠ `MAX_COMMIT_BATCH = 512` (publish.rs) is a private const used only by
//! `serve_ingest`, and `EHDB_FEED_BATCH_LIMIT` (2000) is a READ limit consumed
//! by `ChangeFeed::poll`. So 512 is the real deployed group-commit cap; 2000 is
//! reported for continuity with #481 but is not something the deployed
//! networked path produces today.

#[cfg(not(feature = "chain-cert"))]
fn main() {
    eprintln!("this example needs the certificate compiled in:");
    eprintln!(
        "  cargo run --release -p ehdb-feed --features chain-cert --example append_cert_bench"
    );
    std::process::exit(2);
}

#[cfg(feature = "chain-cert")]
fn main() {
    imp::run();
}

#[cfg(feature = "chain-cert")]
mod imp {
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use ehdb_feed::FeedWriter;
    use ehdb_l0::substrate::DurableSubstrate;
    use ehdb_l0::{D1EventLog, EventRecord, FlushPolicy, L0Config, L0Engine, LocalFsSubstrate};

    /// Payload sized to the recorded corpus mean (371 B) so SHA-256 block
    /// processing — which is proportional to bytes — is representative.
    fn payload(i: u64) -> String {
        let filler = "abcdefghijklmnopqrstuvwxyz0123456789";
        let mut s = format!(r#"{{"event_id":{i},"worker":"noetl-worker-rust-ff446f587","ctx":""#);
        while s.len() < 355 {
            s.push_str(filler);
        }
        s.truncate(355);
        s.push_str(r#""}"#);
        s
    }

    fn ev(id: u64) -> EventRecord {
        EventRecord::new(id, format!("exec-{}", id % 64), "command", payload(id))
    }

    fn unique_dir(tag: &str) -> std::path::PathBuf {
        static N: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let n = N.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        std::env::temp_dir().join(format!("ehdb-cert-ab-{tag}-{}-{n}", std::process::id()))
    }

    fn pct(mut v: Vec<Duration>, p: f64) -> Duration {
        v.sort_unstable();
        if v.is_empty() {
            return Duration::ZERO;
        }
        v[(((v.len() - 1) as f64) * p).round() as usize]
    }

    fn us(d: Duration) -> f64 {
        d.as_secs_f64() * 1e6
    }

    fn median(v: &mut [f64]) -> f64 {
        v.sort_by(|a, b| a.partial_cmp(b).unwrap());
        v[v.len() / 2]
    }

    struct Run {
        events_per_sec: f64,
        per_event_p99_us: f64,
        absorbed: u64,
        seals: u64,
        uploads: u64,
    }

    /// One arm. `dose == 0` means the runtime flag is OFF (the baseline);
    /// `dose >= 1` turns it on and arms the calibration seam to absorb each
    /// record's bytes `dose` times, planting a regression of exactly `dose`
    /// times the certificate's cost.
    fn run_arm(dose: usize, batch_size: usize, batches: usize) -> Run {
        let local = unique_dir("local");
        let obj = unique_dir("obj");
        let store: Arc<dyn DurableSubstrate> = Arc::new(LocalFsSubstrate::new(&obj).unwrap());
        // Seal thresholds raised past the run: DEFAULT_SEAL_MAX_RECORDS is 1024,
        // so at batch=2000 a seal plus synchronous substrate upload would fire
        // mid-batch and land in only one cell. seals/uploads are reported to
        // prove it did not happen.
        let engine = L0Engine::<D1EventLog>::open(
            L0Config::d1(&local)
                .with_shard_count(1)
                .with_seal_max_records(u64::MAX / 2)
                .with_seal_max_bytes(u64::MAX / 2)
                .with_dedupe_capacity(0)
                .with_flush(FlushPolicy::CallerDriven)
                .with_chain_cert(dose > 0),
            store,
        )
        .unwrap();
        let w = Arc::new(FeedWriter::new(engine));
        ehdb_l0::chain_cert::set_calibration_dose(dose.max(1));

        let mut id = 1u64;
        // Warm-up: the first append creates the part dir + file.
        let warm: Vec<EventRecord> = (0..batch_size)
            .map(|_| {
                id += 1;
                ev(id)
            })
            .collect();
        w.append_batch(warm).expect("warm append");

        let mut samples = Vec::with_capacity(batches);
        let t_all = Instant::now();
        for _ in 0..batches {
            let batch: Vec<EventRecord> = (0..batch_size)
                .map(|_| {
                    id += 1;
                    ev(id)
                })
                .collect();
            let t0 = Instant::now();
            w.append_batch(batch).expect("append_batch");
            samples.push(t0.elapsed());
        }
        let total = t_all.elapsed();

        let snap = w.metrics().snapshot();
        let absorbed = w.engine().lock().unwrap().chain_absorbed();
        let out = Run {
            events_per_sec: (batches * batch_size) as f64 / total.as_secs_f64(),
            per_event_p99_us: us(pct(samples, 0.99)) / batch_size as f64,
            absorbed,
            seals: snap.seals,
            uploads: snap.uploads,
        };
        ehdb_l0::chain_cert::set_calibration_dose(1);
        let _ = w.seal_and_close();
        let _ = std::fs::remove_dir_all(&local);
        let _ = std::fs::remove_dir_all(&obj);
        out
    }

    pub fn run() {
        println!("in-engine FeedWriter A/B — chain certificate inside PartWriter::append");
        println!(
            "host: {} cores, release build, sha2 asm: {}",
            std::thread::available_parallelism()
                .map(|n| n.get())
                .unwrap_or(0),
            if cfg!(feature = "chain-cert-asm") {
                "ON (hardware SHA-256)"
            } else {
                "OFF (portable sha2)"
            }
        );
        println!("  payload: {} B (corpus mean 371 B)\n", payload(1).len());

        // Wiring check BEFORE any timing. An unwired integration would produce
        // a clean "no regression" result, which is the failure mode to rule out
        // rather than celebrate.
        {
            let off = run_arm(0, 64, 2);
            let on = run_arm(1, 64, 2);
            assert_eq!(off.absorbed, 0, "flag off must absorb nothing");
            assert!(
                on.absorbed > 0,
                "flag on absorbed nothing — path is UNWIRED"
            );
            println!(
                "  WIRING CHECK: flag off absorbed {} / flag on absorbed {} -> wired\n",
                off.absorbed, on.absorbed
            );
        }

        // Overridable so a cell that fails its own linearity check can be
        // re-measured with more points instead of being published anyway.
        let reps: usize = std::env::var("CC_REPS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(13);
        let doses: Vec<usize> = std::env::var("CC_DOSES")
            .ok()
            .map(|v| v.split(',').filter_map(|x| x.trim().parse().ok()).collect())
            .unwrap_or_else(|| vec![0, 1, 2, 4, 8]);
        let sizes: Vec<usize> = std::env::var("CC_BATCHES")
            .ok()
            .map(|v| v.split(',').filter_map(|x| x.trim().parse().ok()).collect())
            .unwrap_or_else(|| vec![512, 2000]);
        for &bs in &sizes {
            let batches = (120_000 / bs).max(8);
            println!(
                "== batch_size={bs}  ({batches} batches/rep x {reps} reps, {} events per dose, doses {:?})",
                batches * bs * reps, doses
            );

            let mut tps: Vec<Vec<f64>> = vec![Vec::new(); doses.len()];
            let mut p99s: Vec<Vec<f64>> = vec![Vec::new(); doses.len()];
            let mut seal_total = 0u64;
            let mut upload_total = 0u64;
            for _ in 0..reps {
                for (i, &k) in doses.iter().enumerate() {
                    let r = run_arm(k, bs, batches);
                    seal_total += r.seals;
                    upload_total += r.uploads;
                    tps[i].push(r.events_per_sec);
                    p99s[i].push(r.per_event_p99_us);
                }
            }

            let base = median(&mut tps[0].clone());
            let per_event_us = 1e6 / base;
            println!(
                "  baseline (flag OFF): {:>10.0} events/s = {:.3}us/event   per-event p99 {:.3}us",
                base,
                per_event_us,
                median(&mut p99s[0].clone())
            );
            println!(
                "  {:>5} {:>13} {:>11} {:>10} {:>18}",
                "dose", "events/s", "delta", "p99 us", "range"
            );
            let mut deltas = vec![0.0f64; doses.len()];
            for (i, &k) in doses.iter().enumerate() {
                let m = median(&mut tps[i].clone());
                let d = (m - base) / base * 100.0;
                deltas[i] = d;
                let lo = tps[i]
                    .iter()
                    .map(|t| (t - base) / base * 100.0)
                    .fold(f64::INFINITY, f64::min);
                let hi = tps[i]
                    .iter()
                    .map(|t| (t - base) / base * 100.0)
                    .fold(f64::NEG_INFINITY, f64::max);
                println!(
                    "  {:>5} {:>13.0} {:>+10.2}% {:>10.3} {:>18}",
                    k,
                    m,
                    d,
                    median(&mut p99s[i].clone()),
                    format!("[{:+.2}..{:+.2}]", lo, hi)
                );
            }

            // ⚠ WHICH NUMBER IS THE CERTIFICATE'S COST, AND WHAT IS THE DOSE FOR?
            //
            // The deployed certificate hashes each record ONCE, so its cost is
            // the k=1 reading — measured directly, not extrapolated.
            //
            // The dose is here for RESOLUTION, not estimation. A lone k=1
            // difference cannot be trusted at this magnitude (noetl/ehdb#378
            // measured -0.86% with a +-3.6pp spread and its 1x-vs-2x control
            // could not separate the arms). k=2 plants exactly one additional
            // hash — a known size — so separating k=1 from k=2 demonstrates the
            // instrument resolves a signal of about that size.
            //
            // A linear fit over the whole dose range is reported only as a
            // cross-check, and at batch=512 it FAILS: the response saturates
            // (per-unit cost runs -1.03, -1.52, -1.98, -2.18 then falls back to
            // -1.39 by k=8), so extrapolating from high doses to k=1 would be
            // the wrong model. Where it fails it is not used, rather than the
            // cell being discarded or the number published anyway.
            let k1 = deltas[1];
            let k2 = deltas[2];
            let separation = k1 - k2;
            let k1_lo = tps[1]
                .iter()
                .map(|t| (t - base) / base * 100.0)
                .fold(f64::INFINITY, f64::min);
            let k2_hi = tps[2]
                .iter()
                .map(|t| (t - base) / base * 100.0)
                .fold(f64::NEG_INFINITY, f64::max);

            let pts: Vec<(f64, f64)> = doses
                .iter()
                .zip(deltas.iter())
                .filter(|(&k, _)| k >= 1)
                .map(|(&k, &d)| (k as f64, d))
                .collect();
            let n = pts.len() as f64;
            let mk = pts.iter().map(|p| p.0).sum::<f64>() / n;
            let md = pts.iter().map(|p| p.1).sum::<f64>() / n;
            let sxy: f64 = pts.iter().map(|p| (p.0 - mk) * (p.1 - md)).sum();
            let sxx: f64 = pts.iter().map(|p| (p.0 - mk).powi(2)).sum();
            let b = sxy / sxx;
            let a = md - b * mk;
            let ss_res: f64 = pts.iter().map(|p| (p.1 - (a + b * p.0)).powi(2)).sum();
            let ss_tot: f64 = pts.iter().map(|p| (p.1 - md).powi(2)).sum();
            let r2 = if ss_tot > 0.0 {
                1.0 - ss_res / ss_tot
            } else {
                0.0
            };

            println!(
                "  RESOLUTION CONTROL: k=1 {:+.2}% vs k=2 {:+.2}% -> separation {:.2}pp                  from one known extra hash",
                k1, k2, separation
            );
            let resolves = separation > 1.0;
            println!(
                "    per-rep ranges {}overlap (k=1 min {:+.2}%, k=2 max {:+.2}%); medians over                  {} paired reps",
                if k1_lo > k2_hi { "do NOT " } else { "" },
                k1_lo,
                k2_hi,
                reps
            );
            println!(
                "    => instrument {} a ~{:.1}pp signal",
                if resolves {
                    "RESOLVES"
                } else {
                    "CANNOT RESOLVE"
                },
                separation.abs()
            );
            println!(
                "  linear cross-check (k>=1): fixed {:+.3}% + {:+.3}%/hash, R^2={:.4} -> {}",
                a,
                b,
                r2,
                if r2 >= 0.90 {
                    "LINEAR, so extrapolation agrees"
                } else {
                    "NON-LINEAR (saturating), so NOT used"
                }
            );
            if r2 >= 0.90 {
                println!("    extrapolated k=1 would be {:+.2}%", a + b);
            }

            if !resolves {
                println!(
                    "  => VERDICT at batch={bs}: UNPROVEN — instrument cannot resolve this magnitude\n"
                );
                continue;
            }
            println!(
                "  => VERDICT at batch={bs}: certificate costs {:+.2}% (direct k=1) — {} the +-2% gate",
                k1,
                if k1.abs() <= 2.0 { "WITHIN" } else { "OUTSIDE" }
            );
            println!("     implied {:.3}us/event", -k1 / 100.0 * per_event_us);
            println!(
                "  comparability: seals={seal_total} uploads={upload_total} across all doses \
                 (0 = no mid-batch seal polluted a cell)\n"
            );
        }
    }
}
