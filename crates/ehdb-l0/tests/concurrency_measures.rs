//! **Concurrent writers, multi-shard contention, and which invariants survive them.**
//!
//! Before this file, **no test in `ehdb-l0` had ever spawned a thread** (`git grep -l
//! 'thread::spawn' crates/ehdb-l0/tests/` returned nothing). The engine's entire test
//! corpus exercised it from one thread, so every concurrency property it has was
//! un-measured — including the ones a distributed registry depends on.
//!
//! # The first finding is in the signature
//!
//! `append_record(&mut self)`. A single [`L0Engine`] is a **single writer by
//! construction** — concurrency is a property of whatever wraps it, not of the engine.
//! That is a design fact, not a defect, and it bounds everything below: the two honest
//! shapes are *one engine behind a lock* (writers serialise) and *N engines on disjoint
//! substrates* (shared-nothing). Both are measured.
//!
//! # ⚠⚠ The second finding reproduces prod's #362 in a unit test
//!
//! An `AtomicU64` sequencer plus a `Mutex` **is not enough**, and the first version of this
//! file asserted that it was — the canary fired 3 times with 2 writers and 32 times across
//! 8 shards. The reason is the window:
//!
//! ```text
//! let s = seq.fetch_add(1);        // thread A gets 5, thread B gets 6
//! let mut g = engine.lock();       // <-- B wins the race
//! g.append_record(..s..);          // 6 is appended before 5
//! ```
//!
//! **The sequence must be minted while holding the append lock**, not before it. Minted
//! outside, allocation order and append order are independent, and the log is no longer
//! ascending in its own sort key.
//!
//! That is precisely the mechanism behind noetl/ai-meta#362 on prod — *snowflake ids are
//! minted before insert, so commit order is not id order, and an id-ordered read is not an
//! append-only prefix*. It reopened #360 after a green re-ramp. Here it is reproducible in
//! 20 ms, which is the point of writing it down.
//!
//! # ⚠ The third finding is a related trap for callers
//!
//! `sort_key` is **supplied by the caller**, and the engine's ascending-tail canary
//! ([`L0Metrics::out_of_order_appends`], noetl/ai-meta#203) fires when an append does not
//! advance its shard's tail. So the obvious way to parallelise — give each thread its own
//! range of sequence numbers — **violates the engine's contract**, because thread 2's
//! sequence 10,000 lands before thread 1's sequence 5. Nothing rejects the append; a
//! counter moves. The fix is a shared sequencer, and the difference between the two is
//! measured below rather than asserted.
//!
//! # Interleaving is proved before a clean result is believed
//!
//! The reader-invariant test can pass for two entirely different reasons: the readers
//! interleaved with the writer and found no broken prefix, or **the writer finished before
//! any reader looked**, so nothing concurrent was ever observed. Those are
//! indistinguishable in the result and opposite in what they prove. So the test counts the
//! reads that landed strictly mid-write and **fails when that count is zero** — the same
//! "coverage was ~0 by construction" shape as noetl/ai-meta#307.
//!
//! Timings are printed, not asserted: a loaded CI box makes a throughput ratio a coin
//! flip. The invariants are asserted; the numbers are for the measures doc.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::time::Instant;

use ehdb_l0::substrate::DurableSubstrate;
use ehdb_l0::{shard_for_execution, Dataset, L0Config, L0Engine, LocalFsSubstrate};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct R {
    seq: u64,
    key: String,
}

struct CD;
impl Dataset for CD {
    type Record = R;
    const NAME: &'static str = "concurrency_d";
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
    std::env::temp_dir().join(format!("ehdb-conc-{tag}-{}-{n}", std::process::id()))
}

fn engine(root: &Path, shards: u32) -> L0Engine<CD> {
    let obj = root.join("obj");
    let hot = root.join("hot");
    std::fs::create_dir_all(&obj).unwrap();
    std::fs::create_dir_all(&hot).unwrap();
    let s: Arc<dyn DurableSubstrate> = Arc::new(LocalFsSubstrate::new(&obj).unwrap());
    let cfg = L0Config::for_dataset(CD::NAME, &hot)
        .with_shard_count(shards)
        .with_seal_max_records(512);
    L0Engine::<CD>::open(cfg, s).unwrap()
}

// ---------------------------------------------------------------------------
// 1. One engine behind a lock: writers serialise. Nothing may be lost.
// ---------------------------------------------------------------------------

#[test]
fn a_shared_engine_serialises_writers_and_loses_nothing() {
    const PER_THREAD: u64 = 500;
    let mut table: Vec<(usize, f64, u64)> = Vec::new();

    for threads in [1usize, 2, 4, 8] {
        let root = dir(&format!("shared{threads}"));
        let e = Arc::new(Mutex::new(engine(&root, 4)));
        // A shared sequencer: sort keys monotonic in APPEND order, which is
        // what the engine's ascending-tail contract actually requires.
        let seq = Arc::new(AtomicU64::new(0));
        let total = PER_THREAD * threads as u64;

        let t0 = Instant::now();
        let hs: Vec<_> = (0..threads)
            .map(|t| {
                let e = Arc::clone(&e);
                let seq = Arc::clone(&seq);
                std::thread::spawn(move || {
                    for i in 0..PER_THREAD {
                        // ⚠ The sequence is minted **under** the lock. Minting it
                        // before acquiring the lock is a different program with a
                        // different outcome — see
                        // `minting_a_sequence_before_the_lock_reorders_the_log`.
                        let mut g = e.lock().unwrap();
                        let s = seq.fetch_add(1, Ordering::SeqCst) + 1;
                        g.append_record(R {
                            seq: s,
                            key: format!("t{t}k{}", i % 16),
                        })
                        .unwrap();
                    }
                })
            })
            .collect();
        for h in hs {
            h.join().unwrap();
        }
        let secs = t0.elapsed().as_secs_f64();

        let g = e.lock().unwrap();
        let all = g.replay_all().unwrap();
        let ooo = g.metrics().snapshot().out_of_order_appends;

        // Set equality on the sequence space, not a count: a count is equal
        // for "lost 3, duplicated 3" and for "lost nothing".
        let seen: BTreeSet<u64> = all.iter().map(|r| r.seq).collect();
        let expected: BTreeSet<u64> = (1..=total).collect();
        assert_eq!(
            seen, expected,
            "{threads} concurrent writers through one lock: replay is not exactly \
             the sequence space that was appended"
        );
        assert_eq!(
            all.len() as u64,
            total,
            "{threads} writers: replay returned {} records for {total} appends \
             (set-equal but mis-counted means duplicates)",
            all.len()
        );
        assert_eq!(
            ooo, 0,
            "a shared sequencer must keep every append tail-advancing, but the \
             out-of-order canary fired {ooo} times with {threads} writers"
        );

        table.push((threads, total as f64 / secs, ooo));
        drop(g);
        drop(e);
        let _ = std::fs::remove_dir_all(&root);
    }

    println!("\nshared engine behind one Mutex (sort keys from a shared sequencer)");
    println!("threads   appends/s   out_of_order");
    for (t, rate, ooo) in &table {
        println!("{t:>7}   {rate:>9.0}   {ooo:>12}");
    }
    println!(
        "n={} configurations x {PER_THREAD} appends/thread. Throughput is reported, not \
         asserted: the write path is serialised by `&mut self`, so more threads cannot \
         raise it, and on a loaded box the ratio is noise.",
        table.len()
    );
}

// ---------------------------------------------------------------------------
// 2. ⚠ The caller trap: per-thread sequence ranges silently break the contract.
// ---------------------------------------------------------------------------

#[test]
fn per_thread_sequence_ranges_trip_the_ascending_tail_canary() {
    const PER_THREAD: u64 = 300;
    const THREADS: u64 = 4;
    const STRIDE: u64 = 1_000_000;

    let root = dir("ranges");
    let e = Arc::new(Mutex::new(engine(&root, 1)));

    let hs: Vec<_> = (0..THREADS)
        .map(|t| {
            let e = Arc::clone(&e);
            std::thread::spawn(move || {
                // The "obvious" parallelisation: disjoint ranges, no shared state.
                // Disjoint guarantees uniqueness; it does NOT guarantee that an
                // append advances its shard's tail.
                for i in 1..=PER_THREAD {
                    let mut g = e.lock().unwrap();
                    g.append_record(R {
                        seq: t * STRIDE + i,
                        key: format!("r{}", i % 8),
                    })
                    .unwrap();
                }
            })
        })
        .collect();
    for h in hs {
        h.join().unwrap();
    }

    let g = e.lock().unwrap();
    let ooo = g.metrics().snapshot().out_of_order_appends;
    let all = g.replay_all().unwrap();

    // Every record is still durable and readable — the engine did not reject
    // or drop anything. That is exactly why this is dangerous.
    let seen: BTreeSet<u64> = all.iter().map(|r| r.seq).collect();
    assert_eq!(
        seen.len() as u64,
        THREADS * PER_THREAD,
        "nothing should be lost: the contract violation is silent, not lossy"
    );

    println!(
        "\nper-thread ranges, {THREADS} writers x {PER_THREAD}: \
         out_of_order_appends = {ooo}, records durable = {}, lost = 0",
        all.len()
    );

    // This is the measurement: the canary MUST fire. If it does not, then
    // either the canary is inert or this test is not reproducing the hazard,
    // and in both cases the clean reading in test 1 means nothing.
    assert!(
        ooo > 0,
        "the ascending-tail canary did not fire for {THREADS} threads writing \
         disjoint interleaved ranges. Either `out_of_order_appends` is inert, or \
         this test no longer reproduces the hazard — and then test 1's `ooo == 0` \
         is not evidence that a shared sequencer is doing anything."
    );

    // Also assert replay is still globally ordered: the engine sorts on read,
    // so a contract violation degrades the TAIL canary, not read order.
    let mut prev = 0u64;
    for r in &all {
        assert!(
            r.seq > prev,
            "replay is not strictly ascending at seq {} (prev {prev})",
            r.seq
        );
        prev = r.seq;
    }
    println!("replay remains strictly ascending: the damage is to the tail canary, not read order");
}

// ---------------------------------------------------------------------------
// 3. Readers concurrent with a writer — and proof they actually interleaved.
// ---------------------------------------------------------------------------

#[test]
fn readers_interleave_with_a_writer_and_never_see_a_broken_prefix() {
    const N: u64 = 400;
    const READERS: usize = 3;

    let root = dir("rw");
    let e = Arc::new(RwLock::new(engine(&root, 2)));
    let done = Arc::new(std::sync::atomic::AtomicBool::new(false));

    let writer = {
        let e = Arc::clone(&e);
        let done = Arc::clone(&done);
        std::thread::spawn(move || {
            for i in 1..=N {
                e.write()
                    .unwrap()
                    .append_record(R {
                        seq: i,
                        key: format!("w{}", i % 8),
                    })
                    .unwrap();
                // Deliberate pacing so readers have a window to observe
                // intermediate states. Without it this test can pass while
                // proving nothing (see the coverage assertion below).
                std::thread::sleep(std::time::Duration::from_micros(80));
            }
            done.store(true, Ordering::SeqCst);
        })
    };

    let readers: Vec<_> = (0..READERS)
        .map(|_| {
            let e = Arc::clone(&e);
            let done = Arc::clone(&done);
            std::thread::spawn(move || {
                let mut reads = 0u64;
                let mut midflight = 0u64;
                let mut violations: Vec<String> = Vec::new();
                while !done.load(Ordering::SeqCst) {
                    let g = e.read().unwrap();
                    let got = g.replay_all().unwrap();
                    drop(g);
                    reads += 1;
                    let len = got.len() as u64;
                    if len > 0 && len < N {
                        midflight += 1;
                    }
                    // The invariant: whatever a reader sees must be a valid
                    // strictly-ascending prefix with no duplicates — never a
                    // torn or out-of-order view.
                    let mut prev = 0u64;
                    for r in &got {
                        if r.seq <= prev {
                            violations.push(format!("seq {} after {prev}", r.seq));
                            break;
                        }
                        prev = r.seq;
                    }
                }
                (reads, midflight, violations)
            })
        })
        .collect();

    writer.join().unwrap();
    let mut total_reads = 0u64;
    let mut total_mid = 0u64;
    let mut all_violations: Vec<String> = Vec::new();
    for h in readers {
        let (reads, mid, v) = h.join().unwrap();
        total_reads += reads;
        total_mid += mid;
        all_violations.extend(v);
    }

    println!(
        "\n{READERS} readers vs 1 writer over {N} appends: {total_reads} reads, \
         {total_mid} landed strictly mid-write, {} invariant violations",
        all_violations.len()
    );

    // THE COVERAGE CONTROL. A clean result with zero mid-write reads means the
    // writer finished first and nothing concurrent was ever observed — which
    // reads identically to "concurrency is safe".
    assert!(
        total_mid > 0,
        "COVERAGE ZERO: {total_reads} reads, none of them strictly mid-write. \
         The readers never observed a partial state, so this test's clean result \
         is not evidence about concurrent reads at all."
    );
    assert!(
        all_violations.is_empty(),
        "readers observed non-ascending views: {:?}",
        &all_violations[..all_violations.len().min(5)]
    );

    let g = e.read().unwrap();
    assert_eq!(g.global_sequence(), N);
    assert_eq!(g.replay_all().unwrap().len() as u64, N);
    drop(g);
    drop(e);
    let _ = std::fs::remove_dir_all(&root);
}

// ---------------------------------------------------------------------------
// 4. Multi-shard: does concurrency corrupt which shard a record lands in?
// ---------------------------------------------------------------------------

#[test]
fn concurrent_writers_do_not_corrupt_shard_partitioning() {
    const SHARDS: u32 = 8;
    const THREADS: u64 = 4;
    const PER_THREAD: u64 = 400;

    let root = dir("shards");
    let e = Arc::new(Mutex::new(engine(&root, SHARDS)));
    let seq = Arc::new(AtomicU64::new(0));

    let hs: Vec<_> = (0..THREADS)
        .map(|t| {
            let e = Arc::clone(&e);
            let seq = Arc::clone(&seq);
            std::thread::spawn(move || {
                let mut written: Vec<(u64, String)> = Vec::new();
                for i in 0..PER_THREAD {
                    // Keys deliberately SHARED across threads, so every shard
                    // is written by every thread — the contended case.
                    let key = format!("k{}", i % 64);
                    let mut g = e.lock().unwrap();
                    let s = seq.fetch_add(1, Ordering::SeqCst) + 1;
                    g.append_record(R {
                        seq: s,
                        key: key.clone(),
                    })
                    .unwrap();
                    drop(g);
                    written.push((s, key));
                    let _ = t;
                }
                written
            })
        })
        .collect();

    let mut expected: Vec<(u64, String)> = Vec::new();
    for h in hs {
        expected.extend(h.join().unwrap());
    }

    let g = e.lock().unwrap();

    // Set equality per shard against where each key SHOULD hash — not a count.
    let mut union: BTreeSet<u64> = BTreeSet::new();
    let mut per_shard: Vec<usize> = Vec::new();
    for shard in 0..SHARDS {
        let got = g.read_partition_after(shard, 0).unwrap();
        let got_seqs: BTreeSet<u64> = got.iter().map(|r| r.seq).collect();
        let want_seqs: BTreeSet<u64> = expected
            .iter()
            .filter(|(_, k)| shard_for_execution(k, SHARDS) == shard)
            .map(|(s, _)| *s)
            .collect();
        assert_eq!(
            got_seqs, want_seqs,
            "shard {shard} does not hold exactly the records whose keys hash to it \
             after {THREADS} concurrent writers"
        );
        for r in &got {
            assert_eq!(
                shard_for_execution(&r.key, SHARDS),
                shard,
                "record {} with key {} was read from shard {shard}",
                r.seq,
                r.key
            );
        }
        per_shard.push(got.len());
        union.extend(got_seqs);
    }

    let want_all: BTreeSet<u64> = expected.iter().map(|(s, _)| *s).collect();
    assert_eq!(
        union, want_all,
        "the union over {SHARDS} shards is not the full set that was appended — \
         a record landed in no shard, or in two"
    );
    assert_eq!(
        g.metrics().snapshot().out_of_order_appends,
        0,
        "shared sequencer across shards must keep every shard tail-advancing"
    );

    println!(
        "\n{SHARDS} shards, {THREADS} concurrent writers, shared keys: \
         {} records, per-shard {:?}, union set-equal to appended",
        want_all.len(),
        per_shard
    );
    drop(g);
    drop(e);
    let _ = std::fs::remove_dir_all(&root);
}

// ---------------------------------------------------------------------------
// 5. ⚠⚠ The prod mechanism (noetl/ai-meta#362), reproduced: a shared sequencer
//    minted OUTSIDE the append lock. This is what the first draft of test 1
//    asserted was safe.
// ---------------------------------------------------------------------------

#[test]
fn minting_a_sequence_before_the_lock_reorders_the_log() {
    const PER_THREAD: u64 = 400;
    const THREADS: u64 = 4;

    let root = dir("mintfirst");
    let e = Arc::new(Mutex::new(engine(&root, 1)));
    let seq = Arc::new(AtomicU64::new(0));

    let hs: Vec<_> = (0..THREADS)
        .map(|_| {
            let e = Arc::clone(&e);
            let seq = Arc::clone(&seq);
            std::thread::spawn(move || {
                for i in 0..PER_THREAD {
                    // The defect, verbatim: mint, THEN contend for the lock.
                    // Every id is unique and every id is ordered; the order in
                    // which they reach the log is not.
                    let s = seq.fetch_add(1, Ordering::SeqCst) + 1;
                    std::thread::yield_now(); // widen the window deterministically
                    let mut g = e.lock().unwrap();
                    g.append_record(R {
                        seq: s,
                        key: format!("m{}", i % 8),
                    })
                    .unwrap();
                }
            })
        })
        .collect();
    for h in hs {
        h.join().unwrap();
    }

    let g = e.lock().unwrap();
    let ooo = g.metrics().snapshot().out_of_order_appends;
    let all = g.replay_all().unwrap();
    let total = THREADS * PER_THREAD;

    // Nothing is lost and nothing is duplicated. The ids are perfectly unique
    // and perfectly dense. That is why this is hard to see in production.
    let seen: BTreeSet<u64> = all.iter().map(|r| r.seq).collect();
    assert_eq!(
        seen,
        (1..=total).collect::<BTreeSet<u64>>(),
        "mint-before-lock is a REORDERING defect, not a loss defect — the \
         sequence space must still be exactly complete"
    );

    println!(
        "\nmint-before-lock, {THREADS} writers x {PER_THREAD}: \
         out_of_order_appends = {ooo} of {total} appends ({:.1}%), lost = 0, duplicated = 0",
        100.0 * ooo as f64 / total as f64
    );

    assert!(
        ooo > 0,
        "minting {total} sequence numbers outside the append lock produced ZERO \
         out-of-order appends. Either the canary is inert or this no longer \
         reproduces noetl/ai-meta#362 — and then test 1's `ooo == 0` proves nothing, \
         because it would read the same for a sequencer that does not work."
    );

    // And the contrast that makes test 1 meaningful: the ONLY difference
    // between the two programs is where the mint happens.
    println!(
        "contrast: the same sequencer minted UNDER the lock yields \
         out_of_order_appends = 0 (see a_shared_engine_serialises_writers_and_loses_nothing)"
    );
    drop(g);
    drop(e);
    let _ = std::fs::remove_dir_all(&root);
}
