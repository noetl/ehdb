//! O(1) validation, against certificates produced by the REAL append path.
//!
//! noetl/server#480 measured this on synthetic certificates. Here the
//! certificates come out of `FeedWriter`/`PartWriter::append` itself, so the
//! O(1) claim is checked on the integrated path rather than on a stand-in.
//!
//! Run: cargo run --release -p ehdb-feed --features chain-cert --example chain_cert_validate

#[cfg(not(feature = "chain-cert"))]
fn main() {
    eprintln!("needs --features chain-cert");
    std::process::exit(2);
}

#[cfg(feature = "chain-cert")]
fn main() {
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    use ehdb_feed::FeedWriter;
    use ehdb_l0::chain_cert::{self, CertVerdict};
    use ehdb_l0::substrate::DurableSubstrate;
    use ehdb_l0::{D1EventLog, EventRecord, FlushPolicy, L0Config, L0Engine, LocalFsSubstrate};

    fn unique_dir(tag: &str) -> std::path::PathBuf {
        static N: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let n = N.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        std::env::temp_dir().join(format!("ehdb-ccv-{tag}-{}-{n}", std::process::id()))
    }
    fn pct(mut v: Vec<Duration>, p: f64) -> Duration {
        v.sort_unstable();
        v[(((v.len() - 1) as f64) * p).round() as usize]
    }
    fn us(d: Duration) -> f64 {
        d.as_secs_f64() * 1e6
    }

    println!("O(1) validation on certificates from the real append path");
    println!("  (chain_len | uncertified tail | validate p50/p99 | 1000-compare batch)\n");

    // Measurement floor first: the compare is an integer compare plus a 32-byte
    // memcmp. If that is cheaper than two `Instant::now()` calls, the per-op
    // column is reporting the CLOCK, not the work — so a batched figure is
    // printed alongside and the per-op column is labelled floor-bound.
    let floor = {
        let (mut s, t0) = (Vec::new(), Instant::now());
        for _ in 0..50_000 {
            let t = Instant::now();
            std::hint::black_box(0u8);
            s.push(t.elapsed());
        }
        let _ = t0;
        pct(s, 0.50)
    };
    println!("  timer floor (empty closure p50): {:.4}us\n", us(floor));

    for &n in &[36usize, 174, 1000, 10000] {
        let local = unique_dir("local");
        let obj = unique_dir("obj");
        let store: Arc<dyn DurableSubstrate> = Arc::new(LocalFsSubstrate::new(&obj).unwrap());
        let engine = L0Engine::<D1EventLog>::open(
            L0Config::d1(&local)
                .with_shard_count(1)
                .with_seal_max_records(u64::MAX / 2)
                .with_seal_max_bytes(u64::MAX / 2)
                .with_flush(FlushPolicy::CallerDriven)
                .with_chain_cert(true),
            store,
        )
        .unwrap();
        let w = Arc::new(FeedWriter::new(engine));
        let batch: Vec<EventRecord> = (1..=n as u64)
            .map(|i| EventRecord::new(i, "exec-1", "command", format!(r#"{{"i":{i}}}"#)))
            .collect();
        w.append_batch(batch).expect("append_batch");

        let eng = w.engine();
        let guard = eng.lock().unwrap();
        let cert = guard.chain_certificate("exec-1").expect("sealed");
        let absorbed = guard.chain_absorbed();
        drop(guard);

        let mut s = Vec::with_capacity(20_000);
        for _ in 0..20_000 {
            let t = Instant::now();
            std::hint::black_box(chain_cert::validate(
                std::hint::black_box(&cert),
                std::hint::black_box(&cert),
            ));
            s.push(t.elapsed());
        }
        let p50 = pct(s.clone(), 0.50);
        let p99 = pct(s, 0.99);

        // Batched: 1000 compares per sample, to get under the timer floor.
        let mut b = Vec::with_capacity(2_000);
        for _ in 0..2_000 {
            let t = Instant::now();
            for _ in 0..1000 {
                std::hint::black_box(chain_cert::validate(&cert, &cert));
            }
            b.push(t.elapsed());
        }
        let per = pct(b, 0.50).as_secs_f64() * 1e9 / 1000.0;

        assert_eq!(
            chain_cert::validate(&cert, &cert),
            CertVerdict::Valid,
            "a certificate must validate against itself"
        );
        assert_eq!(
            absorbed, n as u64,
            "the path must have absorbed every record"
        );

        println!(
            "  n={:<6} chain_len={:<6} tail={:<2} validate p50 {:.4}us p99 {:.4}us   batched {:.2}ns/compare",
            n,
            cert.chain_len,
            n as u32 - cert.chain_len,
            us(p50),
            us(p99),
            per
        );

        let _ = w.seal_and_close();
        let _ = std::fs::remove_dir_all(&local);
        let _ = std::fs::remove_dir_all(&obj);
    }
    println!("\n  The per-op columns are FLOOR-BOUND (one timer tick). The batched");
    println!("  ns/compare figure is the trustworthy one, and it is flat in n —");
    println!("  which is the O(1) claim. A refold is O(n) by construction.");
}
