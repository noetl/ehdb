//! Engine-level micro-benchmarks for the five EHDB platform-tier reference
//! drivers plus the durable segment event-log backend.
//!
//! These are the **Phase 1** perf-testing deliverable: deterministic, in-process
//! Rust benchmarks over realistic platform-shaped payloads, run with
//! `cargo bench -p ehdb-reference --bench engine_micro`.  They are the reliable
//! signal for engine throughput/latency; the in-cluster end-to-end load layer
//! (driving real traffic through the worker and reading `noetl_ehdb_*` metrics)
//! and the EHDB-vs-incumbent (Postgres + NATS JetStream) head-to-head are a
//! later phase.  See the ehdb-wiki page
//! `Design-Performance-and-Load-Testing` for goals, metrics, and SLO strawman.
//!
//! ## What is measured, and an important caveat about the reference drivers
//!
//! Every `LocalReference*` driver is the **correctness reference implementation
//! of its driver contract**, not a production-serving engine.  Each operation
//! reopens the pod-local JSONL log and **replays it in full** to rebuild state
//! (`LocalReferenceRuntime::open` + `.replay()`), so every op is `O(n)` in the
//! current log size and a sequence of `N` ops is `O(N^2)`.  That is correct for
//! `shadow` mode (a derived, disposable mirror the incumbent still fronts) but
//! it means the reference throughput numbers **degrade with store size** — a
//! property these benches surface on purpose rather than hide.
//!
//! The `durable_segment` event-log backend (`DurableEventLogDriver`) is the
//! opposite: a bounded in-memory offset index over CRC-framed append segments,
//! `fsync`-per-append, `O(1)` amortized append.  The headline event-log
//! comparison here is therefore reference-JSONL vs durable-segment on the *same*
//! `EventLogDriver` contract — and the durable backend is expected to be both
//! safer *and* faster at sustained append, with the fsync cost visible in
//! single-append latency.  Measure what is real; report honestly.
//!
//! Payloads are seeded from a fixed constant so runs are reproducible.  Nothing
//! here changes engine semantics; all helpers are bench-local.

use std::hint::black_box;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use criterion::{criterion_group, criterion_main, BatchSize, BenchmarkId, Criterion, Throughput};

use ehdb_reference::durable_eventlog::{DurableEventLogDriver, DEFAULT_SEGMENT_MAX_BYTES};
use ehdb_reference::{
    EventLogAppendRequest, EventLogDriver, EventLogScanRequest, KvCasExpectation, KvGetRequest,
    KvPutRequest, KvScanRequest, KvStateDriver, LocalReferenceEventLogDriver,
    LocalReferenceKvStateDriver, LocalReferenceObjectBlobDriver, LocalReferenceProjectionEngine,
    LocalReferenceVectorDriver, ObjectBlobDriver, ObjectGetRequest, ObjectListRequest,
    ObjectLocateRequest, ObjectPutRequest, ProjectionApplyRequest, ProjectionDriver,
    ProjectionEventInput, VectorDriver, VectorQueryRequest, VectorUpsertRequest,
};

// ---------------------------------------------------------------------------
// Bench-local infra: deterministic RNG, unique temp dirs, realistic payloads.
// ---------------------------------------------------------------------------

/// A tiny deterministic xorshift64* PRNG so payload shapes are reproducible
/// across runs without pulling in a `rand` dependency.
struct Rng(u64);

impl Rng {
    fn new(seed: u64) -> Self {
        // Avoid the zero fixed-point.
        Rng(seed ^ 0x9E37_79B9_7F4A_7C15)
    }

    fn next_u64(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        x.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }

    /// A unit-ish f32 in roughly [-1, 1], never exactly zero-vectored in
    /// aggregate (the vector engine rejects zero-norm vectors).
    fn next_f32(&mut self) -> f32 {
        let bits = (self.next_u64() >> 40) as f32; // 24-bit mantissa range
        (bits / (1u32 << 24) as f32) * 2.0 - 1.0
    }
}

static DIR_COUNTER: AtomicU64 = AtomicU64::new(0);

/// A unique temp directory under a single bench root so a whole group can be
/// cleaned up with one `remove_dir_all`.  `Math.random`/wall-clock are avoided
/// deliberately; a process-scoped atomic counter keeps names unique and stable.
fn bench_root() -> PathBuf {
    std::env::temp_dir().join("ehdb-engine-micro-bench")
}

fn unique_dir(tag: &str) -> PathBuf {
    let n = DIR_COUNTER.fetch_add(1, Ordering::Relaxed);
    let dir = bench_root().join(format!("{tag}-{}-{n}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn cleanup(tag_dir: &PathBuf) {
    let _ = std::fs::remove_dir_all(tag_dir);
}

/// A realistic ~400-byte platform event envelope (secret-free, platform-only).
fn event_payload(seq: u64, exec: &str) -> String {
    format!(
        "{{\"event_type\":\"action_completed\",\"execution_id\":\"{exec}\",\
\"global_sequence\":{seq},\"node_name\":\"step-{n}\",\"status\":\"COMPLETED\",\
\"attempt\":1,\"worker\":\"noetl-worker-rust-0\",\"pool\":\"user\",\
\"duration_ms\":{d},\"result_ref\":\"ehdb-object://noetl/objects/sha256/\
{pad}\",\"trace_id\":\"{seq:016x}\",\"emitted_at_ms\":{ts}}}",
        n = seq % 32,
        d = 10 + (seq % 900),
        pad = "ab".repeat(24),
        ts = 1_720_000_000_000u64 + seq,
    )
}

const TENANT: &str = "noetl";
const NAMESPACE: &str = "default";

// ---------------------------------------------------------------------------
// Event-log tier — the headline.  local_reference (JSONL, O(n) replay per op)
// vs durable_segment (bounded index, fsync per append).
// ---------------------------------------------------------------------------

fn bench_eventlog(c: &mut Criterion) {
    let mut group = c.benchmark_group("eventlog");
    group.sample_size(10);
    group.warm_up_time(Duration::from_millis(300));
    group.measurement_time(Duration::from_secs(3));

    // --- Sustained append throughput: append K events into a FRESH store. ---
    // local_reference degrades with the growing log (O(N^2) over the run);
    // durable stays ~flat.  Throughput reported as events/sec.
    for &k in &[200u64, 1000u64] {
        group.throughput(Throughput::Elements(k));
        group.bench_with_input(
            BenchmarkId::new("local_reference/append_sustained", k),
            &k,
            |b, &k| {
                b.iter_batched(
                    || unique_dir("el-local-sustain"),
                    |dir| {
                        let driver = LocalReferenceEventLogDriver::new(
                            dir.join("log.jsonl"),
                            TENANT,
                            NAMESPACE,
                        );
                        for i in 0..k {
                            driver
                                .append(&EventLogAppendRequest {
                                    execution_id: format!("{}", 300_000_000_000u64 + i),
                                    transaction_id: format!("txn-{i}"),
                                    payload: event_payload(i, "exec-a"),
                                })
                                .unwrap();
                        }
                        cleanup(&dir);
                    },
                    BatchSize::PerIteration,
                )
            },
        );

        group.bench_with_input(
            BenchmarkId::new("durable_segment/append_sustained", k),
            &k,
            |b, &k| {
                b.iter_batched(
                    || unique_dir("el-dur-sustain"),
                    |dir| {
                        let driver = DurableEventLogDriver::open(&dir).unwrap();
                        for i in 0..k {
                            driver
                                .append(&EventLogAppendRequest {
                                    execution_id: format!("{}", 300_000_000_000u64 + i),
                                    transaction_id: format!("txn-{i}"),
                                    payload: event_payload(i, "exec-a"),
                                })
                                .unwrap();
                        }
                        cleanup(&dir);
                    },
                    BatchSize::PerIteration,
                )
            },
        );
    }

    // --- Single-append latency at a pre-warmed store size S. ---
    // Shows local_reference latency rising with S; durable staying flat (the
    // durable numbers include the per-append fsync).
    group.throughput(Throughput::Elements(1));
    for &s in &[100u64, 1000u64] {
        let dir = unique_dir("el-local-atsize");
        let driver = LocalReferenceEventLogDriver::new(dir.join("log.jsonl"), TENANT, NAMESPACE);
        for i in 0..s {
            driver
                .append(&EventLogAppendRequest {
                    execution_id: format!("{}", 400_000_000_000u64 + i),
                    transaction_id: format!("seed-{i}"),
                    payload: event_payload(i, "exec-seed"),
                })
                .unwrap();
        }
        let ctr = AtomicU64::new(s);
        group.bench_with_input(
            BenchmarkId::new("local_reference/append_at_size", s),
            &s,
            |b, _| {
                b.iter(|| {
                    let i = ctr.fetch_add(1, Ordering::Relaxed);
                    black_box(
                        driver
                            .append(&EventLogAppendRequest {
                                execution_id: format!("{}", 400_000_000_000u64 + i),
                                transaction_id: format!("hot-{i}"),
                                payload: event_payload(i, "exec-seed"),
                            })
                            .unwrap(),
                    );
                })
            },
        );
        cleanup(&dir);
    }
    for &s in &[1000u64, 5000u64] {
        let dir = unique_dir("el-dur-atsize");
        let driver = DurableEventLogDriver::open(&dir).unwrap();
        for i in 0..s {
            driver
                .append(&EventLogAppendRequest {
                    execution_id: format!("{}", 400_000_000_000u64 + i),
                    transaction_id: format!("seed-{i}"),
                    payload: event_payload(i, "exec-seed"),
                })
                .unwrap();
        }
        let ctr = AtomicU64::new(s);
        group.bench_with_input(
            BenchmarkId::new("durable_segment/append_at_size", s),
            &s,
            |b, _| {
                b.iter(|| {
                    let i = ctr.fetch_add(1, Ordering::Relaxed);
                    black_box(
                        driver
                            .append(&EventLogAppendRequest {
                                execution_id: format!("{}", 400_000_000_000u64 + i),
                                transaction_id: format!("hot-{i}"),
                                payload: event_payload(i, "exec-seed"),
                            })
                            .unwrap(),
                    );
                })
            },
        );
        cleanup(&dir);
    }

    // --- Segment-rotation overhead: append K into durable with the default
    // 8 MiB segment (≈no rotation over K) vs a tiny 16 KiB segment (many
    // rotations).  Delta ≈ rotation cost. ---
    let k = 1000u64;
    for (label, seg) in [
        ("seg_8MiB", DEFAULT_SEGMENT_MAX_BYTES),
        ("seg_16KiB", 16 * 1024),
    ] {
        group.throughput(Throughput::Elements(k));
        group.bench_function(
            BenchmarkId::new("durable_segment/append_rotation", label),
            |b| {
                b.iter_batched(
                    || unique_dir("el-dur-rot"),
                    |dir| {
                        let driver =
                            DurableEventLogDriver::open_with_segment_size(&dir, seg).unwrap();
                        for i in 0..k {
                            driver
                                .append(&EventLogAppendRequest {
                                    execution_id: format!("{}", 500_000_000_000u64 + i),
                                    transaction_id: format!("txn-{i}"),
                                    payload: event_payload(i, "exec-rot"),
                                })
                                .unwrap();
                        }
                        cleanup(&dir);
                    },
                    BatchSize::PerIteration,
                )
            },
        );
    }

    // --- Cold-load / replay throughput: build a durable store of N events,
    // drop it, then measure a fresh `open` (segment scan + CRC + index
    // rebuild).  Reported as events/sec (replay rate). ---
    let n = 5000u64;
    let replay_dir = unique_dir("el-dur-replay");
    {
        let driver = DurableEventLogDriver::open(&replay_dir).unwrap();
        for i in 0..n {
            driver
                .append(&EventLogAppendRequest {
                    execution_id: format!("{}", 600_000_000_000u64 + (i % 500)),
                    transaction_id: format!("txn-{i}"),
                    payload: event_payload(i, "exec-replay"),
                })
                .unwrap();
        }
    }
    group.throughput(Throughput::Elements(n));
    group.bench_function(
        BenchmarkId::new("durable_segment/cold_replay_open", n),
        |b| {
            b.iter(|| {
                let reopened = DurableEventLogDriver::open(&replay_dir).unwrap();
                // Touch the store so the reopen isn't optimized away.
                let scan = reopened
                    .scan_global(&EventLogScanRequest {
                        after: Some(n - 1),
                        limit: 4,
                    })
                    .unwrap();
                black_box(scan.record_count);
            })
        },
    );
    cleanup(&replay_dir);

    // --- Per-op-open append latency at a pre-warmed store size S — the
    // noetl/ehdb#267 signal. The worker rebuilds the durable stack PER OP (a
    // stateless boundary), so every mirrored append pays a fresh `open`. Before
    // the checkpoint sidecar, `open` replayed every segment (O(segment)) and this
    // curve rose with S (the deployed ~1.3 append/s cap); with the checkpoint,
    // open-for-append is O(1) and the curve is FLAT across S — a rising curve
    // here would be the O(segment) regression returning. Each iteration opens a
    // fresh driver over the pre-warmed dir and appends one event (no read, so the
    // offset index is never materialized — the O(1) append path). ---
    group.throughput(Throughput::Elements(1));
    for &s in &[100u64, 2_000u64, 10_000u64] {
        let dir = unique_dir("el-dur-peropopen");
        {
            let driver = DurableEventLogDriver::open(&dir).unwrap();
            for i in 0..s {
                driver
                    .append(&EventLogAppendRequest {
                        execution_id: format!("{}", 400_000_000_000u64 + i),
                        transaction_id: format!("seed-{i}"),
                        payload: event_payload(i, "exec-seed"),
                    })
                    .unwrap();
            }
        }
        let ctr = AtomicU64::new(s);
        group.bench_with_input(
            BenchmarkId::new("durable_segment/per_op_open_append_at_size", s),
            &s,
            |b, _| {
                b.iter(|| {
                    let i = ctr.fetch_add(1, Ordering::Relaxed);
                    // Reconstruct the driver per op (the worker's stateless
                    // boundary): a fresh open must be O(1) via the checkpoint.
                    let driver = DurableEventLogDriver::open(&dir).unwrap();
                    black_box(
                        driver
                            .append(&EventLogAppendRequest {
                                execution_id: format!("{}", 400_000_000_000u64 + i),
                                transaction_id: format!("hot-{i}"),
                                payload: event_payload(i, "exec-seed"),
                            })
                            .unwrap(),
                    );
                })
            },
        );
        cleanup(&dir);
    }

    group.finish();
    cleanup(&bench_root());
}

// ---------------------------------------------------------------------------
// Projection tier — fold/materialize rate + checkpoint read.
// ---------------------------------------------------------------------------

fn bench_projection(c: &mut Criterion) {
    let mut group = c.benchmark_group("projection");
    group.sample_size(10);
    group.warm_up_time(Duration::from_millis(300));
    group.measurement_time(Duration::from_secs(3));

    // Fold a synthetic event stream of K events in one `apply` call (one JSONL
    // reopen), reported as events/sec.
    for &k in &[500u64, 2000u64] {
        group.throughput(Throughput::Elements(k));
        group.bench_with_input(BenchmarkId::new("apply_fold", k), &k, |b, &k| {
            b.iter_batched(
                || {
                    let dir = unique_dir("proj-fold");
                    let events: Vec<ProjectionEventInput> = (1..=k)
                        .map(|i| ProjectionEventInput {
                            global_sequence: i,
                            event_id: i as i64,
                            execution_id: format!("{}", 700_000_000_000u64 + (i % 200)),
                            event_type: if i % 3 == 0 {
                                "action_completed".to_string()
                            } else {
                                "action_started".to_string()
                            },
                            node_name: Some(format!("step-{}", i % 16)),
                            status: Some("running".to_string()),
                            prev_event_id: if i > 1 { Some((i - 1) as i64) } else { None },
                        })
                        .collect();
                    (dir, events)
                },
                |(dir, events)| {
                    let engine = LocalReferenceProjectionEngine::new(
                        dir.join("proj.jsonl"),
                        TENANT,
                        NAMESPACE,
                    );
                    let out = engine
                        .apply(&ProjectionApplyRequest {
                            consumer: "projector".to_string(),
                            transaction_id: "bench-fold".to_string(),
                            events,
                        })
                        .unwrap();
                    black_box(out.applied);
                    cleanup(&dir);
                },
                BatchSize::PerIteration,
            )
        });
    }

    // Checkpoint read after a materialized stream (cursor query cost).
    let cp_dir = unique_dir("proj-cp");
    let cp_engine =
        LocalReferenceProjectionEngine::new(cp_dir.join("proj.jsonl"), TENANT, NAMESPACE);
    let seed_events: Vec<ProjectionEventInput> = (1..=1000u64)
        .map(|i| ProjectionEventInput {
            global_sequence: i,
            event_id: i as i64,
            execution_id: format!("{}", 700_000_000_000u64 + (i % 200)),
            event_type: "action_completed".to_string(),
            node_name: Some(format!("step-{}", i % 16)),
            status: Some("running".to_string()),
            prev_event_id: if i > 1 { Some((i - 1) as i64) } else { None },
        })
        .collect();
    cp_engine
        .apply(&ProjectionApplyRequest {
            consumer: "projector".to_string(),
            transaction_id: "cp-seed".to_string(),
            events: seed_events,
        })
        .unwrap();
    group.throughput(Throughput::Elements(1));
    group.bench_function("checkpoint_read/at_1000", |b| {
        b.iter(|| {
            let cp = cp_engine.checkpoint("projector").unwrap();
            black_box(cp.applied_through_sequence);
        })
    });
    cleanup(&cp_dir);

    group.finish();
    cleanup(&bench_root());
}

// ---------------------------------------------------------------------------
// KV tier — put / get / bucket-scan / CAS.
// ---------------------------------------------------------------------------

const KV_BUCKET: &str = "noetl_subscription_circuit";

fn bench_kv(c: &mut Criterion) {
    let mut group = c.benchmark_group("kv");
    group.sample_size(10);
    group.warm_up_time(Duration::from_millis(300));
    group.measurement_time(Duration::from_secs(3));

    // Sustained put throughput into a fresh bucket (put/sec, includes the
    // reference driver's growing-log cost).
    for &k in &[200u64, 1000u64] {
        group.throughput(Throughput::Elements(k));
        group.bench_with_input(BenchmarkId::new("put_sustained", k), &k, |b, &k| {
            b.iter_batched(
                || unique_dir("kv-put"),
                |dir| {
                    let driver =
                        LocalReferenceKvStateDriver::new(dir.join("kv.jsonl"), TENANT, NAMESPACE);
                    for i in 0..k {
                        driver
                            .put(&KvPutRequest {
                                bucket: KV_BUCKET.to_string(),
                                key: format!("circuit.{}", 800_000_000_000u64 + i),
                                value: format!(
                                    "{{\"phase\":\"closed\",\"failures\":{},\"probe_ms\":2000}}",
                                    i % 5
                                ),
                                expires_at_ms: None,
                                cas: None,
                                transaction_id: format!("txn-{i}"),
                            })
                            .unwrap();
                    }
                    cleanup(&dir);
                },
                BatchSize::PerIteration,
            )
        });
    }

    // Read-side ops at a fixed pre-loaded store size S (get/scan/CAS-check).
    let s = 1000u64;
    let dir = unique_dir("kv-read");
    let driver = LocalReferenceKvStateDriver::new(dir.join("kv.jsonl"), TENANT, NAMESPACE);
    for i in 0..s {
        driver
            .put(&KvPutRequest {
                bucket: KV_BUCKET.to_string(),
                key: format!("circuit.{}", 800_000_000_000u64 + i),
                value: format!("{{\"phase\":\"closed\",\"n\":{i}}}"),
                expires_at_ms: None,
                cas: None,
                transaction_id: format!("seed-{i}"),
            })
            .unwrap();
    }
    let get_ctr = AtomicU64::new(0);
    group.throughput(Throughput::Elements(1));
    group.bench_function(BenchmarkId::new("get/at_size", s), |b| {
        b.iter(|| {
            let i = get_ctr.fetch_add(1, Ordering::Relaxed) % s;
            let out = driver
                .get(&KvGetRequest {
                    bucket: KV_BUCKET.to_string(),
                    key: format!("circuit.{}", 800_000_000_000u64 + i),
                    now_ms: None,
                })
                .unwrap();
            black_box(out.found);
        })
    });
    group.bench_function(BenchmarkId::new("scan_bucket/at_size", s), |b| {
        b.iter(|| {
            let out = driver
                .scan(&KvScanRequest {
                    bucket: KV_BUCKET.to_string(),
                    prefix: Some("circuit.".to_string()),
                    limit: 128,
                    now_ms: None,
                })
                .unwrap();
            black_box(out.returned);
        })
    });
    // CAS check path: attempt a create-only put on an existing key (always a
    // deterministic conflict) — measures the reopen+replay+compare cost.
    group.bench_function(BenchmarkId::new("cas_check/at_size", s), |b| {
        b.iter(|| {
            let out = driver
                .put(&KvPutRequest {
                    bucket: KV_BUCKET.to_string(),
                    key: format!("circuit.{}", 800_000_000_000u64),
                    value: "{\"phase\":\"open\"}".to_string(),
                    expires_at_ms: None,
                    cas: Some(KvCasExpectation::Absent),
                    transaction_id: "cas-probe".to_string(),
                })
                .unwrap();
            black_box(out.cas_conflict);
        })
    });
    cleanup(&dir);

    group.finish();
    cleanup(&bench_root());
}

// ---------------------------------------------------------------------------
// Object tier — content-addressed put (small + large), get (verify), locate.
// ---------------------------------------------------------------------------

fn bench_object(c: &mut Criterion) {
    let mut group = c.benchmark_group("object");
    group.sample_size(10);
    group.warm_up_time(Duration::from_millis(300));
    group.measurement_time(Duration::from_secs(3));

    let small = 4 * 1024usize; // ~state-shard sized
    let large = 1024 * 1024usize; // ~1 MiB result-tier feather
    let mut rng = Rng::new(0x0E4D_B007);
    let small_blob: Vec<u8> = (0..small).map(|_| (rng.next_u64() & 0xff) as u8).collect();
    let large_blob: Vec<u8> = (0..large).map(|_| (rng.next_u64() & 0xff) as u8).collect();

    // Single put latency at a modest pre-loaded registry size (blob write +
    // SHA-256 digest dominate; registry reopen adds the reference cost).
    for (label, blob) in [
        ("put_small_4KiB", &small_blob),
        ("put_large_1MiB", &large_blob),
    ] {
        group.throughput(Throughput::Bytes(blob.len() as u64));
        group.bench_function(label, |b| {
            b.iter_batched(
                || unique_dir("obj-put"),
                |dir| {
                    let driver = LocalReferenceObjectBlobDriver::new(
                        dir.join("registry.jsonl"),
                        dir.join("blobs"),
                        TENANT,
                        NAMESPACE,
                    );
                    // A handful of distinct keys so content-dedup doesn't mask
                    // the write; unique key per put keeps each a real write.
                    let out = driver
                        .put(&ObjectPutRequest {
                            key: "noetl/execution=e1/results/s0/f0/r0.feather".to_string(),
                            bytes: blob.clone(),
                            transaction_id: "txn-obj".to_string(),
                        })
                        .unwrap();
                    black_box(out.digest);
                    cleanup(&dir);
                },
                BatchSize::PerIteration,
            )
        });
    }

    // get (digest-verified read: reads blob back + re-hashes) + locate
    // (registry lookup only) at a fixed store size.
    let dir = unique_dir("obj-read");
    let driver = LocalReferenceObjectBlobDriver::new(
        dir.join("registry.jsonl"),
        dir.join("blobs"),
        TENANT,
        NAMESPACE,
    );
    let keys: Vec<String> = (0..200u64)
        .map(|i| {
            format!(
                "noetl/execution=e{}/state/open.feather",
                900_000_000_000u64 + i
            )
        })
        .collect();
    for (i, key) in keys.iter().enumerate() {
        let mut kb = small_blob.clone();
        kb[0] = (i & 0xff) as u8; // perturb so digests differ (no dedup)
        driver
            .put(&ObjectPutRequest {
                key: key.clone(),
                bytes: kb,
                transaction_id: format!("seed-{i}"),
            })
            .unwrap();
    }
    let rc = AtomicU64::new(0);
    group.throughput(Throughput::Elements(1));
    group.bench_function("get_verify_small/at_size_200", |b| {
        b.iter(|| {
            let i = (rc.fetch_add(1, Ordering::Relaxed) % keys.len() as u64) as usize;
            let out = driver
                .get(&ObjectGetRequest {
                    key: keys[i].clone(),
                })
                .unwrap();
            black_box(out.verified);
        })
    });
    group.bench_function("locate/at_size_200", |b| {
        b.iter(|| {
            let i = (rc.fetch_add(1, Ordering::Relaxed) % keys.len() as u64) as usize;
            let out = driver
                .locate(&ObjectLocateRequest {
                    key: keys[i].clone(),
                })
                .unwrap();
            black_box(out.found);
        })
    });
    group.bench_function("list_prefix/at_size_200", |b| {
        b.iter(|| {
            let out = driver
                .list(&ObjectListRequest {
                    prefix: Some("noetl/execution=".to_string()),
                    limit: 128,
                })
                .unwrap();
            black_box(out.returned);
        })
    });
    cleanup(&dir);

    group.finish();
    cleanup(&bench_root());
}

// ---------------------------------------------------------------------------
// Vector tier — upsert rate + cosine top-k at a few catalog sizes.
// ---------------------------------------------------------------------------

const VEC_COLLECTION: &str = "playbook-surface";
const VEC_MODEL: &str = "text-embedding-3-small";
const VEC_DIM: usize = 1536;

fn random_vec(rng: &mut Rng, dim: usize) -> Vec<f32> {
    (0..dim).map(|_| rng.next_f32()).collect()
}

fn bench_vector(c: &mut Criterion) {
    let mut group = c.benchmark_group("vector");
    group.sample_size(10);
    group.warm_up_time(Duration::from_millis(300));
    group.measurement_time(Duration::from_secs(2));

    // Single-upsert latency at a fixed pre-loaded catalog size (dim=1536).
    // The reference driver reopens + replays the whole vector log per op, so a
    // sustained loop would be O(n^2) over 1536-float records; a fixed-catalog
    // single-op probe keeps this tractable and gives a clean per-upsert latency.
    let s = 256usize;
    let dir = unique_dir("vec-upsert");
    let driver = LocalReferenceVectorDriver::new(dir.join("vec.jsonl"), TENANT, NAMESPACE);
    let mut seed_rng = Rng::new(0x5EED_1234);
    for i in 0..s {
        driver
            .upsert(&VectorUpsertRequest {
                collection: VEC_COLLECTION.to_string(),
                point_id: format!("point-{i}"),
                model_id: VEC_MODEL.to_string(),
                vector: random_vec(&mut seed_rng, VEC_DIM),
                payload: Some(format!("src://catalog/playbook/{i}")),
                transaction_id: format!("seed-{i}"),
            })
            .unwrap();
    }
    let up_ctr = AtomicU64::new(s as u64);
    let up_vec = random_vec(&mut seed_rng, VEC_DIM);
    group.throughput(Throughput::Elements(1));
    group.bench_function(BenchmarkId::new("upsert/at_catalog", s), |b| {
        b.iter(|| {
            let i = up_ctr.fetch_add(1, Ordering::Relaxed);
            let out = driver
                .upsert(&VectorUpsertRequest {
                    collection: VEC_COLLECTION.to_string(),
                    point_id: format!("point-{i}"),
                    model_id: VEC_MODEL.to_string(),
                    vector: up_vec.clone(),
                    payload: Some("src://catalog/playbook/hot".to_string()),
                    transaction_id: format!("hot-{i}"),
                })
                .unwrap();
            black_box(out.dimensions);
        })
    });
    cleanup(&dir);

    // Cosine top-k query latency at a few catalog sizes.
    for &catalog in &[128usize, 512usize] {
        let dir = unique_dir("vec-query");
        let driver = LocalReferenceVectorDriver::new(dir.join("vec.jsonl"), TENANT, NAMESPACE);
        let mut rng = Rng::new(0xC0FFEE ^ catalog as u64);
        for i in 0..catalog {
            driver
                .upsert(&VectorUpsertRequest {
                    collection: VEC_COLLECTION.to_string(),
                    point_id: format!("point-{i}"),
                    model_id: VEC_MODEL.to_string(),
                    vector: random_vec(&mut rng, VEC_DIM),
                    payload: None,
                    transaction_id: format!("seed-{i}"),
                })
                .unwrap();
        }
        let query = random_vec(&mut rng, VEC_DIM);
        group.throughput(Throughput::Elements(1));
        group.bench_with_input(
            BenchmarkId::new("query_topk10", catalog),
            &catalog,
            |b, _| {
                b.iter(|| {
                    let out = driver
                        .query(&VectorQueryRequest {
                            collection: VEC_COLLECTION.to_string(),
                            model_id: VEC_MODEL.to_string(),
                            query: query.clone(),
                            top_k: 10,
                        })
                        .unwrap();
                    black_box(out.returned);
                })
            },
        );
        cleanup(&dir);
    }

    group.finish();
    cleanup(&bench_root());
}

// ---------------------------------------------------------------------------
// Shared-tier event-log append — the deployed durable path (noetl/ehdb#264).
//
// The worker's `durable_segment` backend is always the composed shared tier
// (`SharedTierEventLog`), which publishes each append to a shared store. This
// bench measures single-append latency at growing pre-warmed active-segment
// sizes S: with the O(delta) incremental publish it stays FLAT, where the
// earlier whole-segment re-publish rose ~linearly with S and throttled the
// deployed path to ~1-2 append/s in kind.
// ---------------------------------------------------------------------------
fn bench_shared_tier_append(c: &mut Criterion) {
    use std::sync::Arc;

    use ehdb_reference::affinity::ShardOwnership;
    use ehdb_reference::durable_eventlog_shared::{
        FilesystemSharedBackend, SharedSegmentBackend, SharedTierEventLog,
    };

    let mut group = c.benchmark_group("eventlog_shared_tier");
    group.sample_size(10);
    group.warm_up_time(Duration::from_millis(300));
    group.measurement_time(Duration::from_secs(3));
    group.throughput(Throughput::Elements(1));

    // Single-append latency at pre-warmed active-segment sizes S. The #264 fix
    // makes this flat (publish is O(delta)); flatness across S is the property
    // under test — a rising curve would be the O(segment) regression returning.
    for &s in &[100u64, 2_000u64, 10_000u64] {
        let dir = unique_dir("el-shared-atsize");
        let shared: Arc<dyn SharedSegmentBackend> =
            Arc::new(FilesystemSharedBackend::open(dir.join("shared")).unwrap());
        let log = SharedTierEventLog::open(
            dir.join("local"),
            ShardOwnership::new(0, 1).unwrap(),
            Arc::clone(&shared),
            dir.join("coldload"),
        )
        .unwrap();
        let exec = format!("{}", 500_000_000_000u64);
        for i in 0..s {
            log.append(&EventLogAppendRequest {
                execution_id: exec.clone(),
                transaction_id: format!("seed-{i}"),
                payload: event_payload(i, "exec-shared"),
            })
            .unwrap();
        }
        let ctr = AtomicU64::new(s);
        group.bench_with_input(
            BenchmarkId::new("durable_segment_shared/append_at_size", s),
            &s,
            |b, _| {
                b.iter(|| {
                    let i = ctr.fetch_add(1, Ordering::Relaxed);
                    black_box(
                        log.append(&EventLogAppendRequest {
                            execution_id: exec.clone(),
                            transaction_id: format!("hot-{i}"),
                            payload: event_payload(i, "exec-shared"),
                        })
                        .unwrap(),
                    );
                })
            },
        );
        cleanup(&dir);
    }

    // Per-op-open variant — the exact deployed worker shape (noetl/ehdb#267): the
    // worker rebuilds the WHOLE `SharedTierEventLog` stack per append, so each op
    // pays a fresh local `open` (now O(1) via the checkpoint, #267) plus the
    // O(delta) shared publish (#266). Held-open `append_at_size` above never
    // re-opened the local store, hiding the O(segment) replay; this reconstructs
    // the stack every iteration so flatness across S proves the deployed per-op
    // cost is now flat, not just the engine primitive.
    for &s in &[100u64, 2_000u64, 10_000u64] {
        let dir = unique_dir("el-shared-peropopen");
        let local_root = dir.join("local");
        let shared_root = dir.join("shared");
        let coldload_root = dir.join("coldload");
        let exec = format!("{}", 500_000_000_000u64);
        {
            let shared: Arc<dyn SharedSegmentBackend> =
                Arc::new(FilesystemSharedBackend::open(&shared_root).unwrap());
            let log = SharedTierEventLog::open(
                &local_root,
                ShardOwnership::new(0, 1).unwrap(),
                Arc::clone(&shared),
                &coldload_root,
            )
            .unwrap();
            for i in 0..s {
                log.append(&EventLogAppendRequest {
                    execution_id: exec.clone(),
                    transaction_id: format!("seed-{i}"),
                    payload: event_payload(i, "exec-shared"),
                })
                .unwrap();
            }
        }
        let ctr = AtomicU64::new(s);
        group.bench_with_input(
            BenchmarkId::new("durable_segment_shared/per_op_open_append_at_size", s),
            &s,
            |b, _| {
                b.iter(|| {
                    let i = ctr.fetch_add(1, Ordering::Relaxed);
                    // Rebuild the full stack per op, as `build_durable_stack` does.
                    let shared: Arc<dyn SharedSegmentBackend> =
                        Arc::new(FilesystemSharedBackend::open(&shared_root).unwrap());
                    let log = SharedTierEventLog::open(
                        &local_root,
                        ShardOwnership::new(0, 1).unwrap(),
                        Arc::clone(&shared),
                        &coldload_root,
                    )
                    .unwrap();
                    black_box(
                        log.append(&EventLogAppendRequest {
                            execution_id: exec.clone(),
                            transaction_id: format!("hot-{i}"),
                            payload: event_payload(i, "exec-shared"),
                        })
                        .unwrap(),
                    );
                })
            },
        );
        cleanup(&dir);
    }
    group.finish();
}

criterion_group!(
    benches,
    bench_eventlog,
    bench_shared_tier_append,
    bench_projection,
    bench_kv,
    bench_object,
    bench_vector
);
criterion_main!(benches);
