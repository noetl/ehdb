//! End-to-end `FeedWriter` A/B: what does a chain certificate cost the REAL
//! append/commit path?
//!
//! Settles the open question from noetl/server#480 and #481. Those measured a
//! stand-in append loop (serialise + one `write()` syscall per event, ~4.9us)
//! and modelled the delta. The modelled cell that passed the +-2% gate
//! depended on that generous denominator, so #481 said an end-to-end
//! `FeedWriter` A/B was the missing evidence. This is it.
//!
//! Run: `cargo run --release -p ehdb-feed --example append_cert_bench`
//!
//! ⚠ TWO CORRECTIONS to #481's framing, verified in this tree:
//!
//! * `MAX_COMMIT_BATCH = 512` (publish.rs:46) is a PRIVATE const used only by
//!   `serve_ingest`, the networked path. It does not bound `append_batch`.
//! * `EHDB_FEED_BATCH_LIMIT` (default 2000) is READ-side only -- it is
//!   consumed by `ChangeFeed::poll` (l0 feed.rs:107) and has ZERO effect on
//!   append throughput. #481 presented it as a commit batch size. It is not.
//!
//! So 512 is the real deployed group-commit cap; 2000 is a hypothetical here,
//! reachable only by editing that private const.
//!
//! ⚠ WHAT THE CERT ARM MEASURES. An integrated implementation would digest the
//! bytes the engine already serialised, adding only SHA-256 block processing
//! plus chunk bookkeeping. Reaching into the engine's frame buffer from an
//! example is not possible, so the arm absorbs each record's `payload` bytes --
//! sized to the recorded corpus mean (371 B) -- which is that same added cost
//! and does NOT double-serialise. The baseline arm runs the identical
//! `append_batch`; the arms differ only in the hashing.

use std::sync::Arc;
use std::time::{Duration, Instant};

use ehdb_feed::FeedWriter;
use ehdb_l0::substrate::DurableSubstrate;
use ehdb_l0::{D1EventLog, EventRecord, FlushPolicy, L0Config, L0Engine, LocalFsSubstrate};
use sha2::{Digest, Sha256};

// --- the certificate under test -------------------------------------------
// A faithful copy of `ChainCert`'s `ChunkRoller` from noetl/server#481. Pinned
// to that implementation by a golden vector computed by a THIRD, independent
// implementation (Python/hashlib) which shares no code with either -- see
// `assert_matches_independent_implementation` below. A copy that silently
// drifted would compute a different digest and fail that check, which is the
// whole point of asserting it here rather than trusting the transcription.

const DOMAIN_TAG: &[u8] = b"noetl.ehdb.chain-cert.v1";
const CHUNK_EVENTS: u32 = 8;

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
            let d: [u8; 32] =
                sha2::digest::FixedOutputReset::finalize_fixed_reset(&mut self.hasher).into();
            self.sealed_digest = Some(d);
            self.sealed_len = self.len;
            self.open = false;
        }
    }

    fn sealed(&self) -> Option<([u8; 32], u32)> {
        self.sealed_digest.map(|d| (d, self.sealed_len))
    }
}

fn assert_matches_independent_implementation() {
    let mut r = ChunkRoller::new();
    for i in 0..24u32 {
        r.absorb(format!("ehdb-cert-vector-{i:04}").as_bytes());
    }
    let (d, len) = r.sealed().expect("3 sealed chunks");
    assert_eq!(len, 24);
    let got = hex::encode(d);
    assert_eq!(
        got, "c7d3e1fff420f51f0458bcf4f3fe5a3cbefe65aa6879d036197352d8b70a1f44",
        "this copy of ChunkRoller has drifted from the pinned construction"
    );
    println!(
        "  cert construction pinned to independent vector: OK ({}…)",
        &got[..16]
    );
}

// --- harness ---------------------------------------------------------------

/// How many times each event's stored bytes are absorbed. `0` is the untouched
/// baseline, `1` is the certificate under test, and `k > 1` plants a regression
/// of exactly `k` times its cost -- the dose in a dose-response measurement.
type Dose = usize;

/// Payload sized to the recorded corpus mean (371 B), so SHA-256 block
/// processing -- which is proportional to bytes -- is representative.
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
    seals: u64,
    uploads: u64,
}

/// One A/B arm: `batches` calls of `append_batch(batch_size)` against a fresh
/// writer, timing each batch.
fn run_arm(dose: Dose, batch_size: usize, batches: usize) -> Run {
    let local = unique_dir("local");
    let obj = unique_dir("obj");
    let store: Arc<dyn DurableSubstrate> = Arc::new(LocalFsSubstrate::new(&obj).unwrap());
    // Seal thresholds raised well past the run: DEFAULT_SEAL_MAX_RECORDS is
    // 1024, so at batch=2000 a seal + synchronous substrate upload would fire
    // MID-BATCH and land in only one of the two cells, making 512 and 2000
    // incomparable. Seal/upload counts are reported to prove it did not happen.
    let engine = L0Engine::<D1EventLog>::open(
        L0Config::d1(&local)
            .with_shard_count(1)
            .with_seal_max_records(u64::MAX / 2)
            .with_seal_max_bytes(u64::MAX / 2)
            .with_dedupe_capacity(0)
            .with_flush(FlushPolicy::CallerDriven),
        store,
    )
    .unwrap();
    let w = Arc::new(FeedWriter::new(engine));

    let mut roller = ChunkRoller::new();
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
        for r in &batch {
            for _ in 0..dose {
                roller.absorb(r.payload.as_bytes());
            }
        }
        w.append_batch(batch).expect("append_batch");
        samples.push(t0.elapsed());
    }
    let total = t_all.elapsed();
    std::hint::black_box(roller.sealed());

    let snap = w.metrics().snapshot();
    let out = Run {
        events_per_sec: (batches * batch_size) as f64 / total.as_secs_f64(),

        per_event_p99_us: us(pct(samples.clone(), 0.99)) / batch_size as f64,
        seals: snap.seals,
        uploads: snap.uploads,
    };
    let _ = w.seal_and_close();
    let _ = std::fs::remove_dir_all(&local);
    let _ = std::fs::remove_dir_all(&obj);
    out
}

fn main() {
    println!("FeedWriter append A/B with chain certificate — settles noetl/server#481");
    println!(
        "host: {} cores, release build, sha2 `asm` (hardware SHA-256)\n",
        std::thread::available_parallelism()
            .map(|n| n.get())
            .unwrap_or(0)
    );
    assert_matches_independent_implementation();
    println!(
        "  payload size: {} B (corpus mean 371 B)\n",
        payload(1).len()
    );

    // A single A/B difference cannot resolve this: the first run measured
    // -0.86% at batch=512 with a [-2.49..+1.16] spread, and its 1x-vs-2x
    // control could not separate the arms. So measure a DOSE-RESPONSE instead:
    // absorb each body k times for k in {0,1,2,4,8} and fit delta against k.
    // The slope estimates one hash's cost using every point, the intercept
    // absorbs systematic offset, and LINEARITY is the instrument's own
    // self-check -- a non-linear response would mean the harness, not the
    // hashing, is driving the number.
    let reps = 9usize;
    let doses: [Dose; 5] = [0, 1, 2, 4, 8];
    for &bs in &[512usize, 2000] {
        let batches = (60_000 / bs).max(8);
        println!(
            "== batch_size={bs}  ({batches} batches/rep x {reps} reps, {} events per dose)",
            batches * bs * reps
        );

        let mut tps: Vec<Vec<f64>> = vec![Vec::new(); doses.len()];
        let mut p99s: Vec<Vec<f64>> = vec![Vec::new(); doses.len()];
        let mut seal_total = 0u64;
        let mut upload_total = 0u64;
        for _ in 0..reps {
            // Interleaved within a rep so slow drift hits every dose alike.
            for (i, &k) in doses.iter().enumerate() {
                let r = run_arm(k, bs, batches);
                seal_total += r.seals;
                upload_total += r.uploads;
                tps[i].push(r.events_per_sec);
                p99s[i].push(r.per_event_p99_us);
            }
        }

        let base = median(&mut tps[0].clone());
        let base_p99 = median(&mut p99s[0].clone());
        let per_event_us = 1e6 / base;
        println!(
            "  baseline: {:>10.0} events/s  = {:.3}us/event   per-event p99 {:.3}us",
            base, per_event_us, base_p99
        );
        println!(
            "  {:>5} {:>13} {:>11} {:>22}",
            "dose", "events/s", "delta", "range"
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
                "  {:>5} {:>13.0} {:>+10.2}% {:>14}",
                k,
                m,
                d,
                format!("[{:+.2}..{:+.2}]", lo, hi)
            );
        }

        // Least-squares slope with the intercept forced through the k=0 point
        // (which is 0 by construction): slope = sum(k*d) / sum(k^2).
        let sum_kd: f64 = doses
            .iter()
            .zip(deltas.iter())
            .map(|(&k, &d)| k as f64 * d)
            .sum();
        let sum_kk: f64 = doses.iter().map(|&k| (k as f64) * (k as f64)).sum();
        let slope = sum_kd / sum_kk;
        // R^2 of the forced-through-origin fit, as the linearity self-check.
        let ss_res: f64 = doses
            .iter()
            .zip(deltas.iter())
            .map(|(&k, &d)| (d - slope * k as f64).powi(2))
            .sum();
        let mean_d: f64 = deltas.iter().sum::<f64>() / deltas.len() as f64;
        let ss_tot: f64 = deltas.iter().map(|&d| (d - mean_d).powi(2)).sum();
        let r2 = if ss_tot > 0.0 {
            1.0 - ss_res / ss_tot
        } else {
            0.0
        };

        println!(
            "  FIT: {:+.3}% per hash/event   R^2={:.4}   (implied cost {:.3}us/event)",
            slope,
            r2,
            -slope / 100.0 * per_event_us
        );
        let linear = r2 >= 0.90;
        println!(
            "  instrument self-check: response is {} (R^2 {:.4} {} 0.90)",
            if linear {
                "LINEAR — dose-response is trustworthy"
            } else {
                "NON-LINEAR — harness-driven, UNPROVEN"
            },
            r2,
            if linear { ">=" } else { "<" }
        );
        if !linear {
            println!("  => VERDICT at batch={bs}: UNPROVEN\n");
            continue;
        }
        let one = slope;
        println!(
            "  => VERDICT at batch={bs}: certificate costs {:+.2}% — {} the +-2% gate",
            one,
            if one.abs() <= 2.0 {
                "WITHIN"
            } else {
                "OUTSIDE"
            }
        );
        println!(
            "  comparability: seals={seal_total} uploads={upload_total} across all doses \
             (0 = no mid-batch seal polluted a cell)\n"
        );
    }
}
