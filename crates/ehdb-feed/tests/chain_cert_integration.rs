//! The certificate must actually fire through the REAL `FeedWriter` path — and
//! must be genuinely inert when the flag is off (noetl/ai-meta#366).
//!
//! The unit tests in `ehdb_l0::chain_cert` prove the construction is correct.
//! They cannot prove it is *wired*: a registry that is never called passes
//! every one of them. This file is the A9 guard at the integration level, which
//! is the failure this codebase keeps producing — a mechanism that exists and
//! does not run.
#![cfg(feature = "chain-cert")]

use std::sync::Arc;

use ehdb_feed::FeedWriter;
use ehdb_l0::substrate::DurableSubstrate;
use ehdb_l0::{D1EventLog, EventRecord, FlushPolicy, L0Config, L0Engine, LocalFsSubstrate};

fn unique_dir(tag: &str) -> std::path::PathBuf {
    static N: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let n = N.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    std::env::temp_dir().join(format!("ehdb-cc-int-{tag}-{}-{n}", std::process::id()))
}

fn ev(id: u64, exec: &str) -> EventRecord {
    EventRecord::new(id, exec, "command", format!(r#"{{"id":{id}}}"#))
}

fn writer(
    on: bool,
) -> (
    Arc<FeedWriter<D1EventLog>>,
    std::path::PathBuf,
    std::path::PathBuf,
) {
    let local = unique_dir("local");
    let obj = unique_dir("obj");
    let store: Arc<dyn DurableSubstrate> = Arc::new(LocalFsSubstrate::new(&obj).unwrap());
    let engine = L0Engine::<D1EventLog>::open(
        L0Config::d1(&local)
            .with_shard_count(1)
            .with_flush(FlushPolicy::CallerDriven)
            .with_chain_cert(on),
        store,
    )
    .unwrap();
    (Arc::new(FeedWriter::new(engine)), local, obj)
}

#[test]
fn appending_through_the_feed_writer_produces_a_certificate() {
    let (w, local, obj) = writer(true);
    let batch: Vec<EventRecord> = (1..=24u64).map(|i| ev(i, "exec-1")).collect();
    w.append_batch(batch).expect("append_batch");

    let engine = w.engine();
    let guard = engine.lock().unwrap();
    assert_eq!(
        guard.chain_absorbed(),
        24,
        "the certificate path must have RUN for every appended record"
    );
    let cert = guard
        .chain_certificate("exec-1")
        .expect("24 records at CHUNK_EVENTS=8 must seal 3 chunks");
    assert_eq!(cert.chain_len, 24, "all 24 records must be covered");
    assert_ne!(cert.chain_digest, [0u8; 32], "a zero digest means unwired");
    drop(guard);

    let _ = std::fs::remove_dir_all(&local);
    let _ = std::fs::remove_dir_all(&obj);
}

#[test]
fn the_flag_off_leaves_the_path_completely_inert() {
    // The other half of the guard: with the runtime flag off, nothing is
    // absorbed and no certificate exists. If this ever returns a certificate,
    // the flag is not actually gating anything.
    let (w, local, obj) = writer(false);
    let batch: Vec<EventRecord> = (1..=24u64).map(|i| ev(i, "exec-1")).collect();
    w.append_batch(batch).expect("append_batch");

    let engine = w.engine();
    let guard = engine.lock().unwrap();
    assert_eq!(
        guard.chain_absorbed(),
        0,
        "flag off must absorb nothing at all"
    );
    assert!(
        guard.chain_certificate("exec-1").is_none(),
        "flag off must produce no certificate"
    );
    drop(guard);

    let _ = std::fs::remove_dir_all(&local);
    let _ = std::fs::remove_dir_all(&obj);
}

#[test]
fn the_flag_does_not_change_the_bytes_written() {
    // Load-bearing: the digest covers the STORED bytes, so if turning the
    // certificate on altered them, flipping the flag would fork every chain.
    // Append identical records both ways and compare the part files byte for
    // byte.
    fn bytes_for(on: bool) -> Vec<u8> {
        let (w, local, obj) = writer(on);
        let batch: Vec<EventRecord> = (1..=24u64).map(|i| ev(i, "exec-1")).collect();
        w.append_batch(batch).expect("append_batch");
        let mut found = Vec::new();
        for entry in walk(&local) {
            if entry.is_file() {
                found.extend(std::fs::read(&entry).unwrap_or_default());
            }
        }
        let _ = std::fs::remove_dir_all(&local);
        let _ = std::fs::remove_dir_all(&obj);
        found
    }
    fn walk(dir: &std::path::Path) -> Vec<std::path::PathBuf> {
        let mut out = Vec::new();
        if let Ok(rd) = std::fs::read_dir(dir) {
            let mut entries: Vec<_> = rd.flatten().map(|e| e.path()).collect();
            entries.sort();
            for p in entries {
                if p.is_dir() {
                    out.extend(walk(&p));
                } else {
                    out.push(p);
                }
            }
        }
        out
    }
    let off = bytes_for(false);
    let on = bytes_for(true);
    assert!(
        !off.is_empty(),
        "the baseline must actually write something"
    );
    assert_eq!(
        off, on,
        "enabling the certificate must not change a single stored byte"
    );
}

#[test]
fn separate_executions_get_separate_chains_through_the_writer() {
    let (w, local, obj) = writer(true);
    let mut batch = Vec::new();
    for i in 1..=16u64 {
        batch.push(ev(i, "exec-a"));
        batch.push(ev(1000 + i, "exec-b"));
    }
    w.append_batch(batch).expect("append_batch");

    let engine = w.engine();
    let guard = engine.lock().unwrap();
    let a = guard.chain_certificate("exec-a").expect("exec-a sealed");
    let b = guard.chain_certificate("exec-b").expect("exec-b sealed");
    assert_eq!(a.chain_len, 16);
    assert_eq!(b.chain_len, 16);
    assert_ne!(
        a.chain_digest, b.chain_digest,
        "interleaved executions must not share a digest"
    );
    drop(guard);

    let _ = std::fs::remove_dir_all(&local);
    let _ = std::fs::remove_dir_all(&obj);
}
