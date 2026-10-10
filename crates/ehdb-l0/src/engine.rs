//! [`L0EventLogEngine`] — the hot-local / durable-async composite (RFC §2.3) for
//! dataset D1, tying [`crate::part`] (the write engine), [`crate::catalog`] (the
//! meta-catalog), and [`crate::substrate`] (the durability tier) together.
//!
//! ```text
//!   append(exec, txn, payload)
//!     └─ hot: route to shard_for(exec) → PartWriter.append (fsync-per-append, posture A)
//!           └─ on seal trigger → immutable part + manifest row (local-only) + enqueue upload
//!   [background uploader thread]
//!     └─ read sealed part bytes → substrate.put_if_absent → record a replica → rewrite durable manifest
//!   read_execution_after(exec, seq)
//!     └─ manifest.prune(shard, seq)  ← MinMax skip: non-matching parts = zero I/O
//!        └─ per part: sparse_index.locate(seq) → ranged read [mark, end)  ← only the needed block
//!           └─ prefer local_path (hot); else substrate.get_range (durable)
//!        └─ + the active (unsealed) hot buffer
//!   cold_load(substrate)  ← fresh node, empty local dir
//!     └─ read durable manifest → serve reads entirely from the substrate
//!        (reproduces the exact record set + global sequence — the fungible-writer property, RFC §2.7)
//! ```
//!
//! The append path **never** calls the substrate; only the background
//! uploader does. That is the §2.3 claim the L0.1 proof exercises with an
//! injected substrate latency: appends do not regress when uploads are slow.

use std::collections::HashMap;
use std::fs::{self, File};
use std::io::{Read, Seek, SeekFrom};
use std::path::PathBuf;
use std::sync::mpsc::{self, Sender};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use ehdb_core::{EhdbError, Result};

use crate::catalog::{Manifest, PartMeta, ReplicaLocation};
use crate::dataset::{D1EventLog, Dataset, EventRecord, DATASET_D1_EVENT_LOG, DEFAULT_SHARD_COUNT};
use crate::frame::iter_frames_from;
use crate::merge::{plan_next_merge, MergePlan, MergePolicy};
use crate::metrics::L0Metrics;
use crate::part::{build_merged_part, substrate_key_for, FlushPolicy, PartWriter, SealedPart};
use crate::substrate::DurableSubstrate;
use crate::unreplicated::{ShardUnreplicated, UnreplicatedTracker};

/// Back-compat alias: the D1 event-log engine is the generic [`L0Engine`]
/// specialized to [`D1EventLog`]. It carries the D1 convenience API
/// (`append(exec, txn, payload)` / `read_execution_after`).
pub type L0EventLogEngine = L0Engine<D1EventLog>;

/// Default granule size (records per sparse-index entry).
pub const DEFAULT_GRANULE_SIZE: u32 = 16;
/// Default seal threshold by record count.
pub const DEFAULT_SEAL_MAX_RECORDS: u64 = 1024;
/// Default seal threshold by byte size (8 MiB — the #254 `DEFAULT_SEGMENT_MAX_BYTES`).
pub const DEFAULT_SEAL_MAX_BYTES: u64 = 8 * 1024 * 1024;
/// Default number of **versioned manifest snapshots** kept on the substrate,
/// besides `LATEST` (noetl/ehdb#344).
///
/// Each manifest write emits a full snapshot listing every part, so snapshot
/// size grows with part count while the number of snapshots grows with write
/// count — retaining all of them costs **O(parts x writes)**. On prod that was
/// 6,770 snapshots totalling 19.4 GB behind 71.8 MB of actual data, which
/// filled the volume and stopped every append.
///
/// Nothing reads these files; `LATEST` is the only manifest the engine loads.
/// They are kept purely so a human can inspect recent manifest history, so the
/// bound is small on purpose.
pub const DEFAULT_MANIFEST_RETAIN: usize = 32;

/// L0 engine configuration for one dataset.
#[derive(Debug, Clone)]
pub struct L0Config {
    /// The dataset id (L0.1: [`DATASET_D1_EVENT_LOG`]).
    pub dataset: String,
    /// Local hot-tier root directory (parts live under `parts/<dataset>/shard-*`).
    pub local_root: PathBuf,
    /// Partition (shard) count. `1` = single owner (single-writer default).
    pub shard_count: u32,
    /// Records per sparse-index granule.
    pub granule_size: u32,
    /// Seal a part once it reaches this byte size.
    pub seal_max_bytes: u64,
    /// Seal a part once it reaches this record count.
    pub seal_max_records: u64,
    /// **Age-based seal trigger** (noetl/ehdb#329). Seal an active part once its
    /// oldest record is this old, whatever its size or count.
    ///
    /// `None` — the default — is today's behavior, and today's behavior leaves
    /// the durability window **unbounded in time**: a shard that appends a few
    /// records and goes quiet never seals, so those records never reach the
    /// substrate. Off by default so enabling it is deliberate and reversible.
    ///
    /// ⚠ Setting this is necessary but not sufficient. An idle shard takes no
    /// appends, so something must drive [`L0Engine::seal_aged_parts`] on a
    /// timer; the flag alone is inert on exactly the shard it protects.
    pub seal_max_age: Option<Duration>,
    /// **Replicate the UNSEALED tail off-box** (B3, noetl/ehdb#394).
    ///
    /// A sealed part is copied to every replica by the uploader. Until a part
    /// seals, its records exist **only on local disk** — so the loss window is
    /// the seal interval, which `seal_max_age` bounds but does not close. With
    /// this on, [`L0Engine::replicate_tail`] copies records that are still in an
    /// active writer to the replica set as their own write-once objects, and
    /// a cold load reads them back.
    ///
    /// `false` — the default — is today's behaviour: the tail stays RF=1.
    ///
    /// ⚠ Necessary but not sufficient, the same shape as `seal_max_age`: an
    /// active writer takes no action on its own, so something must drive
    /// [`L0Engine::replicate_tail`] on a timer. The flag alone replicates
    /// nothing.
    ///
    /// ⚠⚠ This does not make durability synchronous. Loss becomes bounded by
    /// the **driver's tick interval** instead of the seal interval; it is not
    /// eliminated, and no remote ack gates the local append.
    pub tail_replication: bool,
    /// Durability-window posture (D1 default = [`FlushPolicy::EveryAppend`]).
    pub flush: FlushPolicy,
    /// Maintain per-execution chain certificates (noetl/ai-meta#366).
    ///
    /// **Off by default**, and additionally gated at compile time by the
    /// `chain-cert` feature — with the feature off this field is inert. Two
    /// gates because the compile-time one keeps `sha2` out of a default build
    /// while the runtime one lets the certificate be turned on in a deployed
    /// binary without a rebuild (and A/B'd in one process).
    ///
    /// ⚠ Turning this on does NOT change the bytes written. The digest covers
    /// the stored bytes, so if the flag altered them, flipping it would fork
    /// every chain.
    pub chain_cert: bool,
    /// Keys remembered per shard for append-time idempotency (noetl/ai-meta#313).
    /// `0` disables dedupe entirely.
    pub dedupe_capacity: usize,
    /// **Refuse a replica set that does not spread failure domains** (#332 F5).
    ///
    /// `false` — the default — is today's behaviour: violations are *counted and
    /// logged* but the open succeeds. Shadow first, enforce deliberately, the
    /// same shape `seal_max_age` and the fencing work use.
    ///
    /// ⚠ Only consulted for a set of **two or more** replicas. A single replica
    /// makes no spreading claim, so there is nothing to falsify; enforcing there
    /// would only reject substrates that decline to declare a domain.
    pub require_distinct_domains: bool,
    /// How much loss the replica set must survive (M4).
    ///
    /// [`SurvivalGoal::Zone`] — the default — is exactly today's behaviour:
    /// `check_region_survival` returns no violations for it, so the region
    /// check below is a strict no-op unless an operator asks for `Region`.
    pub survival_goal: crate::failure_domain::SurvivalGoal,
    /// M2 clock mode. `None` — the default — reads `NOETL_EHDB_HLC`.
    ///
    /// Injectable so tests choose the mode WITHOUT touching the process env:
    /// `cargo test` does not serialise tests, so an env-driven test would race
    /// every other test in the binary.
    pub hlc_mode: Option<crate::hlc_policy::HlcMode>,
    /// L0.3 background merge/compaction policy.
    pub merge_policy: MergePolicy,
    /// How many versioned manifest snapshots to keep besides `LATEST`
    /// (noetl/ehdb#344). `0` disables pruning entirely — the pre-fix behaviour,
    /// which is unbounded and is what filled the prod volume. See
    /// [`DEFAULT_MANIFEST_RETAIN`].
    pub manifest_retain: usize,
}

impl L0Config {
    /// D1 defaults rooted at `local_root`: single owner, posture A, 8 MiB /
    /// 1024-record seal, 16-record granules, D1 merge policy.
    pub fn d1(local_root: impl Into<PathBuf>) -> Self {
        Self {
            dataset: DATASET_D1_EVENT_LOG.to_string(),
            local_root: local_root.into(),
            shard_count: DEFAULT_SHARD_COUNT,
            granule_size: DEFAULT_GRANULE_SIZE,
            seal_max_bytes: DEFAULT_SEAL_MAX_BYTES,
            seal_max_records: DEFAULT_SEAL_MAX_RECORDS,
            seal_max_age: None,
            tail_replication: false,
            require_distinct_domains: false,
            survival_goal: crate::failure_domain::SurvivalGoal::Zone,
            hlc_mode: None,
            flush: FlushPolicy::EveryAppend,
            chain_cert: false,
            dedupe_capacity: crate::dedupe::DEFAULT_DEDUPE_CAPACITY,
            merge_policy: MergePolicy::d1(DEFAULT_SEAL_MAX_RECORDS),
            manifest_retain: DEFAULT_MANIFEST_RETAIN,
        }
    }

    /// Config for an arbitrary dataset id (used by non-D1 datasets). The engine
    /// overrides `dataset` from `D::NAME` on open regardless, so this is mainly
    /// for the merge policy defaults; the tuning knobs match [`d1`](Self::d1).
    pub fn for_dataset(dataset: impl Into<String>, local_root: impl Into<PathBuf>) -> Self {
        Self {
            dataset: dataset.into(),
            ..Self::d1(local_root)
        }
    }

    /// Set the partition (shard) count.
    /// Keys remembered per shard for append-time idempotency (#313).
    ///
    /// `0` disables dedupe and is byte-for-byte today's behaviour. Default is
    /// [`crate::dedupe::DEFAULT_DEDUPE_CAPACITY`]; it is inert regardless until a
    /// record actually carries a [`crate::dataset::Dataset::dedupe_key`], which
    /// no producer sends yet.
    pub fn with_dedupe_capacity(mut self, capacity: usize) -> Self {
        self.dedupe_capacity = capacity;
        self
    }

    pub fn with_shard_count(mut self, shard_count: u32) -> Self {
        self.shard_count = shard_count;
        self
    }
    /// Set the granule size.
    pub fn with_granule_size(mut self, granule_size: u32) -> Self {
        self.granule_size = granule_size;
        self
    }
    /// Set the record-count seal threshold. Also updates the merge policy's
    /// "small part" threshold to match, so a freshly-sealed part is a merge
    /// candidate and a merge output is not.
    pub fn with_seal_max_records(mut self, seal_max_records: u64) -> Self {
        self.seal_max_records = seal_max_records;
        self.merge_policy.small_part_max_records = seal_max_records;
        self
    }

    /// Set the L0.3 merge policy explicitly.
    pub fn with_merge_policy(mut self, merge_policy: MergePolicy) -> Self {
        self.merge_policy = merge_policy;
        self
    }

    /// Set how many versioned manifest snapshots to retain besides `LATEST`
    /// (noetl/ehdb#344). `0` restores the unbounded pre-fix behaviour and is
    /// only useful for proving, in a test, that the bound is what does the work.
    pub fn with_manifest_retain(mut self, manifest_retain: usize) -> Self {
        self.manifest_retain = manifest_retain;
        self
    }
    /// Set the byte-size seal threshold.
    /// Enable the **age-based seal trigger** (noetl/ehdb#329). `None` restores
    /// the size/count-only default.
    pub fn with_seal_max_age(mut self, seal_max_age: Option<Duration>) -> Self {
        self.seal_max_age = seal_max_age;
        self
    }

    /// Enable off-box replication of the unsealed tail (B3). See
    /// [`Self::tail_replication`] — the flag is inert without a driver calling
    /// [`L0Engine::replicate_tail`].
    pub fn with_tail_replication(mut self, on: bool) -> Self {
        self.tail_replication = on;
        self
    }

    /// Enforce failure-domain spread across the replica set (#332 F5).
    pub fn with_require_distinct_domains(mut self, require: bool) -> Self {
        self.require_distinct_domains = require;
        self
    }

    /// Set the survival goal (M4). `Zone` (default) leaves the region check
    /// inert; `Region` requires copies in distinct **declared** regions.
    pub fn with_survival_goal(mut self, goal: crate::failure_domain::SurvivalGoal) -> Self {
        self.survival_goal = goal;
        self
    }

    /// Set the M2 clock mode explicitly, bypassing `NOETL_EHDB_HLC`.
    pub fn with_hlc_mode(mut self, mode: crate::hlc_policy::HlcMode) -> Self {
        self.hlc_mode = Some(mode);
        self
    }

    pub fn with_seal_max_bytes(mut self, seal_max_bytes: u64) -> Self {
        self.seal_max_bytes = seal_max_bytes;
        self
    }
    /// Set the durability-window posture.
    /// Enable per-execution chain certificates (noetl/ai-meta#366).
    pub fn with_chain_cert(mut self, on: bool) -> Self {
        self.chain_cert = on;
        self
    }

    pub fn with_flush(mut self, flush: FlushPolicy) -> Self {
        self.flush = flush;
        self
    }

    fn part_dir(&self, shard: u32) -> PathBuf {
        self.local_root
            .join(format!("parts/{}/shard-{}", self.dataset, shard))
    }
}

/// One durable-substrate replica the engine writes copies to (L0.6). `id` names
/// the replica (recorded in [`ReplicaLocation::replica`]); `substrate` is its
/// byte-sink handle. In kind/dev each replica is a distinct
/// [`crate::substrate::LocalFsSubstrate`] directory; conceptually each is a
/// distinct node/disk.
#[derive(Clone)]
pub struct ReplicaTarget {
    /// Stable replica id (e.g. `replica-0`).
    pub id: String,
    /// The replica's durable byte-sink.
    pub substrate: Arc<dyn DurableSubstrate>,
    /// Where this replica physically lives (M1).
    ///
    /// ⭐ **Undeclared by default, and that is the safe direction.** A replica
    /// that declares nothing is never *assumed* to be in a distinct region —
    /// `check_region_survival` refuses an undeclared replica under
    /// [`SurvivalGoal::Region`] rather than counting it as spread. Same posture
    /// as `FailureDomain::Undeclared`: silence is not independence.
    ///
    /// Consulted only by the [`SurvivalGoal::Region`] check, which is a strict
    /// no-op under the default `Zone` goal — so a caller that never sets this
    /// gets byte-identical behaviour.
    pub locality: crate::placement::Locality,
}

impl ReplicaTarget {
    /// Construct a replica target with **undeclared** locality.
    ///
    /// The signature is unchanged on purpose: every existing call site keeps
    /// compiling and keeps its current behaviour. Locality is opt-in through
    /// [`ReplicaTarget::with_locality`].
    pub fn new(id: impl Into<String>, substrate: Arc<dyn DurableSubstrate>) -> Self {
        Self {
            id: id.into(),
            substrate,
            locality: crate::placement::Locality::undeclared(),
        }
    }

    /// Declare where this replica lives (M1).
    pub fn with_locality(mut self, locality: crate::placement::Locality) -> Self {
        self.locality = locality;
        self
    }
}

/// Where the uploader reads a part's bytes from.
///
/// ⚠ Two cases, because a backfilled part has **no local file to read**.
/// [`Manifest::durable_view`] stores `local_path: None`, and the durable view is
/// what [`load_durable_manifest`] seeds the in-RAM manifest from at open — so
/// after any restart every recovered part's `local_path` is gone, and an
/// uploader that only knew how to `fs::read` could never repair one.
enum UploadSource {
    /// A part sealed in this process: its bytes are still on local disk.
    LocalFile(String),
    /// A **backfill**: read the part back out of a replica that already holds
    /// it, and copy it to the ones that do not.
    Replica { replica: String, key: String },
}

/// A unit of upload work handed to the background uploader thread.
struct UploadJob {
    substrate_key: String,
    source: UploadSource,
    part_id: String,
    /// The part's shard — needed to close its durability window on the
    /// [`UnreplicatedTracker`] when the upload lands (noetl/ehdb#328).
    shard: u32,
    /// When the part was sealed — `None` for a backfill, whose seal happened in
    /// an earlier process. A fabricated near-zero lag here would be added to
    /// `upload_lag_micros_total` and deflate the reported mean seal→durable
    /// latency, so the backfill path records its own counters instead.
    sealed_at: Option<Instant>,
}

/// The generic L0 storage engine for one [`Dataset`] `D` (single writer per
/// partition — the caller is the shard owner, matching #254's single-writer
/// assumption). Parts / catalog / merge / replication are all `D`-agnostic; `D`
/// supplies only the record schema + fixed sort key + fixed partition + fixed
/// index dimension.
pub struct L0Engine<D: Dataset> {
    config: L0Config,
    /// The **N-way replica set** (L0.6): every immutable part + the durable
    /// manifest is written to all of these; reads try them in order with
    /// fallback. A single-substrate `open` yields one replica (`replica-0`).
    replicas: Vec<ReplicaTarget>,
    metrics: Arc<L0Metrics>,
    /// In-RAM catalog — local-only + durable parts. Shared with the uploader
    /// thread, which records a replica on upload.
    manifest: Arc<Mutex<Manifest>>,
    /// Per-shard active writers (engine-owned single writer).
    writers: HashMap<u32, PartWriter<D>>,
    /// Highest sort key seen (D1: the global-sequence tip).
    global_sequence: u64,
    /// Per-shard highest sort key ever appended (survives seals, unlike the
    /// active writer's `max_sequence`). Only used to spot an append that does
    /// not advance its shard's tail — the ascending-contract-violation canary
    /// behind [`L0Metrics::out_of_order_appends`] (noetl/ai-meta#203).
    shard_tail_max: HashMap<u32, u64>,
    /// Per-shard highest sort key already copied off-box as a **tail** object
    /// (B3). Monotonic and deliberately NOT reset on seal: the next active
    /// part's records carry strictly higher keys, so the same watermark keeps
    /// selecting only what has not been replicated.
    tail_cursor: HashMap<u32, u64>,
    /// Append-time idempotency window (noetl/ai-meta#313).
    dedupe: crate::dedupe::DedupeIndex,
    /// The M2 commit clock, and the mode that decides whether it is consulted.
    ///
    /// Held on the engine rather than created per append because the whole
    /// point of `HlcClock` is that it never returns a value less than or equal
    /// to one it already returned — a fresh clock per append would reset that
    /// guarantee every time.
    hlc_mode: crate::hlc_policy::HlcMode,
    hlc: ehdb_core::hlc::HlcClock,
    /// Sender to the uploader thread (dropped on close to stop it).
    upload_tx: Option<Sender<UploadJob>>,
    upload_handle: Option<JoinHandle<()>>,
    /// Outstanding upload count + condvar for `flush_and_wait_uploads`.
    outstanding: Arc<(Mutex<usize>, Condvar)>,
    /// **The D1 durability window, measured from the append** (noetl/ehdb#328).
    /// Shared with the uploader thread, which closes a part's window when it
    /// becomes durable. Observability only — nothing here gates an append.
    unreplicated: Arc<UnreplicatedTracker>,
}

impl<D: Dataset> L0Engine<D> {
    /// Open a **single-replica** writer engine over `substrate` (`replica-0`),
    /// reusing any manifest already there. The local hot tier is
    /// `config.local_root`.
    pub fn open(config: L0Config, substrate: Arc<dyn DurableSubstrate>) -> Result<Self> {
        Self::open_replicated(config, vec![ReplicaTarget::new("replica-0", substrate)])
    }

    /// Open sharing an existing [`L0Metrics`] handle (single replica).
    pub fn open_with_metrics(
        config: L0Config,
        substrate: Arc<dyn DurableSubstrate>,
        metrics: Arc<L0Metrics>,
    ) -> Result<Self> {
        Self::open_replicated_with_metrics(
            config,
            vec![ReplicaTarget::new("replica-0", substrate)],
            metrics,
        )
    }

    /// **Open an N-way replicated writer engine** (L0.6). Every immutable part +
    /// the durable manifest is written to all `replicas`; reads fall back across
    /// them. `replicas` must be non-empty.
    pub fn open_replicated(config: L0Config, replicas: Vec<ReplicaTarget>) -> Result<Self> {
        Self::open_replicated_with_metrics(config, replicas, L0Metrics::new())
    }

    /// N-way open sharing a metrics handle.
    pub fn open_replicated_with_metrics(
        mut config: L0Config,
        replicas: Vec<ReplicaTarget>,
        metrics: Arc<L0Metrics>,
    ) -> Result<Self> {
        if replicas.is_empty() {
            return Err(EhdbError::InvalidState(
                "L0 engine needs at least one replica target".into(),
            ));
        }
        // noetl/ai-meta#332 — the on-disk format gate, BEFORE anything is read
        // or written.  Every `open*` funnels here, so this is the one place it
        // can be enforced.
        //
        // ⚠ Checked on EVERY replica, not just the first: each is an independent
        // substrate with its own layout, and a replica written by a different
        // build is exactly the case a single-replica check would wave through.
        for replica in &replicas {
            crate::format_version::verify_or_initialise(replica.substrate.as_ref())
                .map_err(|err| EhdbError::Storage(format!("replica {}: {err}", replica.id)))?;
        }
        // noetl/ehdb#332 F5 — the failure-domain check, wired.
        //
        // ⚠ This guard existed and had NO production caller: `validate_replica_domains`
        // was referenced only from its own tests, so a replica set that shared one
        // disk was refused by nothing. An RF of N over one domain is an RF of 1
        // wearing a larger number, and until this call site existed the larger
        // number is all anyone could see.
        //
        // Only for a set of 2+: one replica makes no spreading claim.
        if replicas.len() >= 2 {
            let domains: Vec<crate::failure_domain::ReplicaDomain> = replicas
                .iter()
                .map(|r| {
                    let domain = r.substrate.failure_domain();
                    // The nesting check needs the path; only LocalDevice carries
                    // one. A Remote replica has no local root and cannot nest.
                    let root = match &domain {
                        crate::failure_domain::FailureDomain::LocalDevice { root, .. } => {
                            Some(root.clone())
                        }
                        _ => None,
                    };
                    crate::failure_domain::ReplicaDomain {
                        replica: r.id.clone(),
                        domain,
                        root,
                    }
                })
                .collect();
            // M4 — the REGION survival check, at the same call site as the
            // device/zone one.
            //
            // Deliberately here rather than inside `check_replica_domains`:
            // the two ask different questions and `failure_domain`'s module
            // note is explicit that "the two are combined by the caller, not by
            // this module". A device-id comparison cannot establish region
            // spread — two disks in one region are two domains and one region.
            //
            // ⭐ Inert by default. `check_region_survival` returns no
            // violations for `SurvivalGoal::Zone`, so a deployment that has not
            // asked for `Region` sees byte-identical behaviour and an
            // undeclared `locality` costs nothing.
            let placements: Vec<crate::failure_domain::RegionPlacement> = replicas
                .iter()
                .map(|r| crate::failure_domain::RegionPlacement {
                    replica: r.id.clone(),
                    region: r.locality.region.clone(),
                })
                .collect();
            // Enforced, not shadowed, and that asymmetry with the domain check
            // below is intentional: the domain check defaults to ON for everyone
            // and so needs a shadow rung, whereas `Region` is never reached
            // unless an operator explicitly asked for it. Asking for a survival
            // goal and silently not getting it is the failure this phase exists
            // to prevent.
            crate::failure_domain::validate_region_survival(&placements, config.survival_goal)?;

            let violations = crate::failure_domain::check_replica_domains(&domains);
            if !violations.is_empty() {
                metrics.set_replica_domain_violations(violations.len() as u64);
                if config.require_distinct_domains {
                    let joined = violations
                        .iter()
                        .map(|v| v.message())
                        .collect::<Vec<_>>()
                        .join("; ");
                    return Err(EhdbError::InvalidState(format!(
                        "replica set does not spread failure domains: {joined}"
                    )));
                }
                // Shadow: observable, not fatal. ⚠ The signal is the COUNTER,
                // not a log line — this crate takes no `tracing` dependency and
                // adding one for a warning is a dependency decision, not a
                // detail. The counter is pinned at 0 below on the healthy path
                // so absence and zero stay distinguishable.
            } else {
                metrics.set_replica_domain_violations(0);
            }
        }
        // The dataset id is authoritative from the type — keep the config in sync
        // so a generic dataset's substrate keys / manifest keys are correct.
        config.dataset = D::NAME.to_string();
        fs::create_dir_all(&config.local_root)
            .map_err(|err| EhdbError::Storage(err.to_string()))?;
        // Resume the durable catalog if present (owner restart) — from any
        // surviving replica, else start empty.
        let manifest = load_durable_manifest(&replicas, &config.dataset)?
            .unwrap_or_else(|| Manifest::empty(&config.dataset));
        let global_sequence = manifest.max_sequence();
        let mut engine = Self::assemble(config, replicas, metrics, manifest, global_sequence);
        // noetl/ai-meta#209 defect 2 — recover BEFORE anything reads or appends.
        //
        // Writers are opened lazily by `ensure_writer`, and recovery rides that
        // open.  Lazy is too late on both paths: `append_writer_assigned` reads
        // `global_sequence` to mint its key *before* calling into the append that
        // would open the writer, so the first post-crash append lands at or below
        // the recovered tip (the #203 silent drop); and a read never opens a
        // writer at all, so recovered records stay invisible until something
        // happens to append.  Both were caught by the engine-level tests and
        // neither is visible from the writer's own unit tests.
        engine.recover_active_parts()?;
        engine.start_uploader();
        // ⚠ Unconditional, including on a fresh engine where every value is 0. A pin
        // inside a config branch is not a pin (server#315): it leaves the series absent
        // on exactly the configuration whose value someone would be reading, and an
        // absent series is indistinguishable from a healthy zero to every alert.
        engine.refresh_state_gauges();
        Ok(engine)
    }

    /// Open every shard's writer once at startup, so an active part left by a
    /// crash is replayed (and the sequence reconciled) before the engine serves
    /// anything.  A shard with no active part opens an empty writer, which is
    /// what `ensure_writer` would have done on first append anyway.
    fn recover_active_parts(&mut self) -> Result<()> {
        for shard in 0..self.config.shard_count.max(1) {
            self.ensure_writer(shard)?;
        }
        Ok(())
    }

    /// **Cold-load** a fresh node (empty local dir) from a single substrate:
    /// read the durable manifest and serve reads from substrate-replica parts.
    /// Reproduces the exact record set + global sequence of the origin — the
    /// fungible-writer property that retires the per-shard-Raft "T-RF" plan
    /// (RFC §2.7).
    pub fn cold_load(config: L0Config, substrate: Arc<dyn DurableSubstrate>) -> Result<Self> {
        Self::cold_load_replicated(config, vec![ReplicaTarget::new("replica-0", substrate)])
    }

    /// Cold-load (single replica) sharing a metrics handle.
    pub fn cold_load_with_metrics(
        config: L0Config,
        substrate: Arc<dyn DurableSubstrate>,
        metrics: Arc<L0Metrics>,
    ) -> Result<Self> {
        Self::cold_load_replicated_with_metrics(
            config,
            vec![ReplicaTarget::new("replica-0", substrate)],
            metrics,
        )
    }

    /// **Cold-load from an N-way replica set** (L0.6). A fresh node reads the
    /// durable manifest from any surviving replica and serves reads across all of
    /// them with fallback — the durability payoff: one dead replica does not
    /// stop recovery.
    pub fn cold_load_replicated(config: L0Config, replicas: Vec<ReplicaTarget>) -> Result<Self> {
        Self::cold_load_replicated_with_metrics(config, replicas, L0Metrics::new())
    }

    /// N-way cold-load sharing a metrics handle.
    pub fn cold_load_replicated_with_metrics(
        mut config: L0Config,
        replicas: Vec<ReplicaTarget>,
        metrics: Arc<L0Metrics>,
    ) -> Result<Self> {
        config.dataset = D::NAME.to_string();
        if replicas.is_empty() {
            return Err(EhdbError::InvalidState(
                "L0 cold-load needs at least one replica target".into(),
            ));
        }
        fs::create_dir_all(&config.local_root)
            .map_err(|err| EhdbError::Storage(err.to_string()))?;
        let manifest = load_durable_manifest(&replicas, &config.dataset)?.ok_or_else(|| {
            EhdbError::InvalidState(format!(
                "cold-load: no durable manifest for dataset {} on any replica",
                config.dataset
            ))
        })?;
        let global_sequence = manifest.max_sequence();
        metrics.incr_cold_loads();
        let mut engine = Self::assemble(config, replicas, metrics, manifest, global_sequence);
        engine.start_uploader();
        // B3 — replay the unsealed tail (noetl/ehdb#394).
        //
        // ⚠ AFTER `start_uploader`, because the recovered records are appended
        // through the ordinary path and the part they land in must be able to
        // seal and upload like any other.
        //
        // ⚠⚠ Unconditional, and deliberately not gated on `tail_replication`.
        // The flag governs whether this node *writes* tail objects; whether
        // there are any to read is a property of the replica set, which may
        // have been written by a node whose flag was on. Gating recovery on the
        // reader's own config is how a durability feature silently fails to
        // deliver on exactly the node doing the recovering.
        let recovered = engine.recover_tail_from_replicas(global_sequence)?;
        if recovered > 0 {
            engine.refresh_state_gauges();
        }
        Ok(engine)
    }

    fn assemble(
        config: L0Config,
        replicas: Vec<ReplicaTarget>,
        metrics: Arc<L0Metrics>,
        manifest: Manifest,
        global_sequence: u64,
    ) -> Self {
        let shard_count = config.shard_count;
        let dedupe_capacity_init =
            crate::dedupe::DedupeIndex::with_capacity(config.dedupe_capacity);
        let hlc_mode_resolved = config
            .hlc_mode
            .unwrap_or_else(crate::hlc_policy::HlcMode::from_env);
        Self {
            config,
            replicas,
            metrics,
            manifest: Arc::new(Mutex::new(manifest)),
            writers: HashMap::new(),
            tail_cursor: HashMap::new(),
            global_sequence,
            shard_tail_max: HashMap::new(),
            dedupe: dedupe_capacity_init,
            // Resolved ONCE at open: a per-append env read would put a syscall
            // on the commit path, and the mode cannot change under a running
            // process anyway.
            hlc_mode: hlc_mode_resolved,
            // ⚠ Seeded from the tip the engine just recovered, not from
            // `default()`. `HlcClock::seeded_from` exists precisely because a
            // fresh process must not re-issue timestamps a previous one used,
            // and the wall clock alone does not guarantee that across a restart
            // during which it stepped back.
            hlc: ehdb_core::hlc::HlcClock::seeded_from(
                ehdb_core::hlc::Hlc::from_parts(0, 0),
                Box::new(|| {
                    std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .map(|d| d.as_millis() as u64)
                        .unwrap_or(0)
                }),
            ),
            upload_tx: None,
            upload_handle: None,
            outstanding: Arc::new((Mutex::new(0), Condvar::new())),
            unreplicated: Arc::new(UnreplicatedTracker::new(shard_count)),
        }
    }

    fn start_uploader(&mut self) {
        let (tx, rx) = mpsc::channel::<UploadJob>();
        let replicas = self.replicas.clone();
        let manifest = Arc::clone(&self.manifest);
        let metrics = Arc::clone(&self.metrics);
        let outstanding = Arc::clone(&self.outstanding);
        let unreplicated = Arc::clone(&self.unreplicated);
        let dataset = self.config.dataset.clone();
        let manifest_retain = self.config.manifest_retain;
        let handle = std::thread::Builder::new()
            .name("ehdb-l0-uploader".to_string())
            .spawn(move || {
                while let Ok(job) = rx.recv() {
                    // Read the sealed part bytes and **write-once copy to every
                    // replica** (N-way, L0.6). The append path never does this —
                    // durability is asynchronous (RFC §2.3). Parts are immutable,
                    // so each copy is byte-identical and no consensus is needed.
                    let read_result = match &job.source {
                        UploadSource::LocalFile(path) => {
                            fs::read(path).map_err(|err| EhdbError::Storage(err.to_string()))
                        }
                        UploadSource::Replica { replica, key } => replicas
                            .iter()
                            .find(|t| &t.id == replica)
                            .ok_or_else(|| {
                                EhdbError::InvalidState(format!(
                                    "backfill source replica {replica} is not in the replica set"
                                ))
                            })
                            .and_then(|t| t.substrate.get_all(key)),
                    };
                    let bytes = match read_result {
                        Ok(b) => b,
                        Err(_) => {
                            decrement(&outstanding);
                            continue;
                        }
                    };
                    let locations =
                        replicate_bytes(&replicas, &job.substrate_key, &bytes, &metrics);
                    if locations.is_empty() {
                        // Every replica write failed → part stays local-only (its
                        // manifest row keeps `replicas` empty); a retry slice
                        // re-drives it. Decrement so a waiter isn't wedged.
                        decrement(&outstanding);
                        continue;
                    }

                    // Record the replica locations on the part and snapshot the
                    // durable view — under the lock; the substrate writes happen
                    // OUTSIDE the lock so a slow store never blocks appends/reads.
                    let durable = {
                        let mut m = manifest.lock().unwrap();
                        if let Some(p) = m.parts.iter_mut().find(|p| p.part_id == job.part_id) {
                            p.replicas = locations.clone();
                        }
                        m.version += 1;
                        m.durable_view()
                    };
                    // The durable manifest must exist on EVERY replica so any one
                    // of them can serve a cold-load alone.
                    write_manifest_to_all(&replicas, &dataset, &durable, manifest_retain, &metrics);

                    match job.sealed_at {
                        Some(sealed_at) => {
                            let lag = sealed_at.elapsed().as_micros() as u64;
                            metrics.record_upload(bytes.len() as u64, lag);
                        }
                        // A backfill has no seal→durable interval to report.
                        None => metrics.record_backfill_upload(bytes.len() as u64),
                    }
                    // Close this part's durability window and record the
                    // **append→durable** latency. ⚠ `lag` above is measured from
                    // the SEAL and cannot see the pre-seal term; this one can.
                    // The failure paths above deliberately do NOT reach here —
                    // a part that did not replicate is still pending, which is
                    // what the window should keep reporting.
                    if let Some(end_to_end) = unreplicated.on_upload_done(job.shard, &job.part_id) {
                        metrics.record_replicated_lag(end_to_end.as_micros() as u64);
                    }
                    decrement(&outstanding);
                }
            })
            .expect("spawn ehdb-l0 uploader thread");
        self.upload_tx = Some(tx);
        self.upload_handle = Some(handle);
    }

    /// **Append one record to the hot tier** (dataset-generic). Routes to the
    /// record's partition ([`Dataset::partition`]), writes it, advances the
    /// sort-key tip, and seals if the active part is full. The caller supplies a
    /// fully-formed record whose sort key is `>=` every prior record in its
    /// partition (the single-writer ascending-sort-key contract). Never touches
    /// the substrate. Returns the record's sort key.
    /// Has this record's idempotency key already landed? Returns its position.
    ///
    /// `None` whenever the dataset has no key for the record — which is every
    /// dataset except D1, and every D1 record whose producer has not been updated
    /// to send one. So this is a no-op until something actually sends an
    /// `event_id`, which is what makes the change inert on arrival.
    fn dedupe_hit(&self, record: &D::Record) -> Option<u64> {
        let key = D::dedupe_key(record)?;
        let shard = D::partition(record, self.config.shard_count);
        match self.dedupe.check(shard, key) {
            crate::dedupe::DedupeVerdict::Duplicate(seq) => Some(seq),
            crate::dedupe::DedupeVerdict::Fresh => None,
        }
    }

    /// [`append_record`](Self::append_record), reporting **whether the record was
    /// actually written**.
    ///
    /// `(sort_key, appended)`. `appended == false` means the key was already
    /// present and the returned position is the existing record's — the caller
    /// must treat it as an acknowledgement, not a new append.
    ///
    /// ⚠ This exists because the plain `append_record` return is indistinguishable
    /// between the two cases, and the difference is load-bearing upstream: the
    /// tier-append reply is checked for **strictly increasing** sequences and for
    /// `log_record_count == global_sequence`. A deduplicated redelivery returns an
    /// OLDER position and does not advance the count, so a caller that cannot tell
    /// the two apart reports a parity **divergence** for a working dedupe
    /// (noetl/ai-meta#313).
    pub fn append_record_reporting(&mut self, record: D::Record) -> Result<(u64, bool)> {
        if let Some(existing) = self.dedupe_hit(&record) {
            self.metrics.incr_dedupe_hits();
            return Ok((existing, false));
        }
        self.append_record(record).map(|seq| (seq, true))
    }

    /// [`append_writer_assigned`](Self::append_writer_assigned), reporting whether
    /// the record was actually written. See [`Self::append_record_reporting`].
    pub fn append_writer_assigned_reporting(&mut self, record: D::Record) -> Result<(u64, bool)> {
        if let Some(existing) = self.dedupe_hit(&record) {
            self.metrics.incr_dedupe_hits();
            return Ok((existing, false));
        }
        self.append_writer_assigned(record).map(|seq| (seq, true))
    }

    pub fn append_record(&mut self, record: D::Record) -> Result<u64> {
        // ⚠ Before anything else. A duplicate must not touch the shard tail, must
        // not trip the ascending canary, must not seal, and must not advance the
        // durability window — it is not an append, it is an acknowledgement of one
        // that already happened.
        if let Some(existing) = self.dedupe_hit(&record) {
            self.metrics.incr_dedupe_hits();
            return Ok(existing);
        }
        // M2 — stamp the commit HLC.
        //
        // ⚠ AFTER the dedupe check above, never before: a duplicate is an
        // acknowledgement of an append that already happened, not an append, so
        // it must not burn a timestamp or overwrite the one the original
        // carries.
        //
        // ⚠⚠ Nothing reads `commit_hlc`, and that is the M2 exit criterion
        // rather than a gap — a reader appearing before M3's closed timestamps
        // would be a phase-ordering violation. Stamping is write-only here on
        // purpose, which is also why `off` (the default) is byte-identical:
        // `skip_serializing_if` omits the field entirely.
        let mut record = record;
        if self.hlc_mode.stamps() {
            D::stamp_commit_hlc(&mut record, self.hlc.now().as_u64());
        }
        let sort_key = D::sort_key(&record);
        let shard = D::partition(&record, self.config.shard_count);
        // Ascending-contract canary: an append that does not advance its shard's
        // tail lands behind any follower cursor and is silently never delivered
        // (noetl/ai-meta#203). Count it so the loss class is observable rather
        // than silent. `append_writer_assigned` assigns a strictly-advancing key
        // so it never trips this.
        match self.shard_tail_max.get(&shard) {
            Some(&prev) if sort_key <= prev => self.metrics.incr_out_of_order_appends(),
            _ => {
                self.shard_tail_max.insert(shard, sort_key);
            }
        }
        self.ensure_writer(shard)?;
        let dedupe_key = D::dedupe_key(&record).map(str::to_string);
        {
            let writer = self.writers.get_mut(&shard).unwrap();
            writer.append(record)?;
        }
        // ⚠ AFTER the append, never before. Remembering first would make a failed
        // append poison its key: the retry that the failure exists to invite would
        // then be answered "already present" for a record that is not there —
        // silent loss, the exact class this removes.
        if let Some(key) = dedupe_key {
            self.dedupe.remember(shard, &key, sort_key);
            self.metrics
                .set_dedupe_window_evictions(self.dedupe.evictions());
        }
        if sort_key > self.global_sequence {
            self.global_sequence = sort_key;
        }
        self.metrics.incr_appends();
        // Open the record's durability window. ⚠ Under `FlushPolicy::CallerDriven`
        // the ack lands after the caller's `fsync`, so counting here can include
        // a record for the few microseconds before it is acked. That
        // over-reports the window and never under-reports it — the safe
        // direction for a durability signal.
        self.unreplicated.on_append(shard);

        let sealed = {
            let writer = self.writers.get_mut(&shard).unwrap();
            if writer.should_seal() {
                writer.seal()?
            } else {
                None
            }
        };
        if let Some(sealed) = sealed {
            self.register_and_upload(sealed)?;
        }
        Ok(sort_key)
    }

    /// **Append letting the writer assign the sort key** — the next monotonic
    /// `global_sequence + 1` — instead of trusting the caller's. Restores the
    /// [`Dataset`] contract (*"appended in ascending sort_key order within a
    /// partition"*) for a feed whose producer assigns keys that may reach the
    /// single writer out of order, so every ingested record stays claimable
    /// (noetl/ai-meta#203). The re-key is dataset-defined
    /// ([`Dataset::assign_sort_key`]); datasets that keep the intrinsic key get
    /// the default no-op re-key, making this identical to [`append_record`] for
    /// them. Returns the assigned sort key.
    ///
    /// Because the single writer serializes appends and the assigned key strictly
    /// increases, the shard log is ascending by construction: a follower cursor
    /// never advances past an un-read record, and no append can land behind it.
    pub fn append_writer_assigned(&mut self, record: D::Record) -> Result<u64> {
        // An early-out, NOT the enforcement point — and the distinction is
        // recorded because the comment here first claimed otherwise.
        //
        // It originally said this check was needed so a duplicate would not burn
        // a global sequence and leave a gap. A mutation removing it left every
        // test green, which is how the claim was found to be false: `seq` here is
        // only a candidate, and `self.global_sequence` advances *inside*
        // `append_record` after its own dedupe check. So gaplessness is enforced
        // there, and this guard only saves the `assign_sort_key` work.
        //
        // Both guards are kept because both entry points are public and reached:
        // `append_record` is called directly by callers that own their sort keys.
        // Each is covered by its own test — see `event_id_idempotency.rs`.
        if let Some(existing) = self.dedupe_hit(&record) {
            self.metrics.incr_dedupe_hits();
            return Ok(existing);
        }
        let seq = self.global_sequence + 1;
        self.append_record(D::assign_sort_key(record, seq))
    }

    /// **Take the commit handles** for every shard with outstanding appends — the
    /// group-commit seam under [`FlushPolicy::CallerDriven`] (noetl/ai-meta#205).
    ///
    /// Each handle is a duplicated descriptor onto a shard's active part
    /// ([`PartWriter::take_sync_handle`]); calling `sync_data()` on it closes the
    /// durability window for every append that shard has taken since the last
    /// take, so a batch of records that arrived together pays **one** `fsync`
    /// instead of one each. Returning handles rather than doing the `fsync` here
    /// lets the caller drop the engine lock first, so the blocking `fsync` does
    /// not stall readers holding up the consuming side.
    ///
    /// The caller **must** complete the `sync_data()` before acknowledging any
    /// record in the batch — that is what keeps the crash window identical to
    /// posture A.
    pub fn take_sync_handles(&mut self) -> Result<Vec<std::fs::File>> {
        let mut handles = Vec::new();
        for writer in self.writers.values_mut() {
            if let Some(h) = writer.take_sync_handle()? {
                handles.push(h);
            }
        }
        Ok(handles)
    }

    /// Switch the flush posture for this engine, propagating it to the already-open
    /// active part writers. Used by [`crate::FeedWriter`] to take ownership of its
    /// own commit points (group commit); durability is unchanged because the
    /// writer syncs before it returns.
    pub fn set_flush_policy(&mut self, policy: FlushPolicy) {
        self.config.flush = policy;
        for writer in self.writers.values_mut() {
            writer.set_flush_policy(policy);
        }
    }

    /// This execution's chain certificate from the shard that owns it
    /// (noetl/ai-meta#366). `None` until a chunk seals.
    #[cfg(feature = "chain-cert")]
    pub fn chain_certificate(&self, execution_id: &str) -> Option<crate::chain_cert::ChainCert> {
        let shard = D::read_partition(execution_id, self.config.shard_count);
        self.writers
            .get(&shard)
            .and_then(|w| w.chain_certificate(execution_id))
    }

    /// Records absorbed into chain state across every open writer — proves the
    /// path ran rather than merely existing (A9).
    #[cfg(feature = "chain-cert")]
    pub fn chain_absorbed(&self) -> u64 {
        self.writers.values().map(|w| w.chain_absorbed()).sum()
    }

    fn ensure_writer(&mut self, shard: u32) -> Result<()> {
        if !self.writers.contains_key(&shard) {
            let writer = PartWriter::<D>::open(
                self.config.dataset.clone(),
                shard,
                self.config.part_dir(shard),
                self.config.granule_size,
                self.config.seal_max_bytes,
                self.config.seal_max_records,
                self.config.flush,
            )?;
            let mut writer = writer;
            writer.set_seal_max_age(self.config.seal_max_age);
            #[cfg(feature = "chain-cert")]
            writer.set_chain_cert(self.config.chain_cert);
            // noetl/ai-meta#209 defect 2 — a writer that just recovered an
            // active part left by a crash holds records the manifest does not
            // know about, because the manifest lists sealed parts only. The
            // engine's `global_sequence` came from that manifest, so it is
            // *behind* the recovered tail; lift both it and the shard tail above
            // the recovered records before anything is appended.
            //
            // Skipping this would let the next `append_writer_assigned` mint a
            // key at or below the recovered tail. That append lands behind every
            // follower cursor and is never delivered — the silent-drop class of
            // noetl/ai-meta#203 — so recovery without this would trade a crash
            // loss for a quieter one.
            if let Some(recovered_tip) = writer.max_sequence() {
                if recovered_tip > self.global_sequence {
                    self.global_sequence = recovered_tip;
                }
                let tail = self.shard_tail_max.entry(shard).or_insert(recovered_tip);
                if recovered_tip > *tail {
                    *tail = recovered_tip;
                }
                let recovered = writer.pending_records().len() as u64;
                self.metrics.add_recovered_active_records(recovered);
                // ⚠⚠ Re-seed the idempotency window from the recovered records
                // (noetl/ai-meta#313). Without this, a crash makes every record in
                // the active part deduplicable-no-more: the retry that a crash
                // most reliably produces would be answered "fresh" and appended a
                // second time. The window would be emptiest at exactly the moment
                // redelivery is most likely — the same "quietest when the most is
                // at risk" shape the durability window was fixed for.
                let seeds: Vec<(String, u64)> = writer
                    .pending_records()
                    .iter()
                    .filter_map(|r| D::dedupe_key(r).map(|k| (k.to_string(), D::sort_key(r))))
                    .collect();
                for (key, seq) in seeds {
                    self.dedupe.remember(shard, &key, seq);
                }
                // ⚠⚠ Seed the durability window too. These records are acked and
                // `fsync`'d but not on the substrate, so they are pending by
                // every definition the gauge uses — and without this the window
                // reads 0 immediately after a crash, i.e. it is quietest exactly
                // when the most is at risk.
                self.unreplicated.on_recovered(shard, recovered);
            }
            self.writers.insert(shard, writer);
        }
        Ok(())
    }

    /// Register a sealed part in the manifest (local-only) and enqueue its async
    /// upload.
    fn register_and_upload(&mut self, sealed: SealedPart<D>) -> Result<()> {
        let substrate_key = substrate_key_for(
            &self.config.dataset,
            sealed.meta.partition,
            &sealed.meta.part_id,
        );
        let local_path = sealed
            .meta
            .local_path
            .clone()
            .ok_or_else(|| EhdbError::InvalidState("sealed part missing local_path".into()))?;
        let part_id = sealed.meta.part_id.clone();
        let shard = sealed.meta.partition;
        let record_count = sealed.meta.record_count;

        {
            let mut m = self.manifest.lock().unwrap();
            m.push_part(sealed.meta);
        }
        self.metrics.incr_seals();
        // The sealed part inherits the active part's first-append instant, so
        // the window keeps measuring from the append rather than restarting.
        self.unreplicated.on_seal(shard, &part_id, record_count);

        // Bump outstanding BEFORE sending so flush_and_wait never races a job.
        {
            let (lock, _) = &*self.outstanding;
            *lock.lock().unwrap() += 1;
        }
        if let Some(tx) = &self.upload_tx {
            let job = UploadJob {
                substrate_key,
                source: UploadSource::LocalFile(local_path),
                part_id,
                shard,
                sealed_at: Some(Instant::now()),
            };
            if tx.send(job).is_err() {
                // Uploader gone (engine closing) — undo the outstanding bump.
                decrement(&self.outstanding);
            }
        } else {
            decrement(&self.outstanding);
        }
        Ok(())
    }

    /// **Copy every already-sealed part that is short of replicas to the
    /// replicas it is missing.** Returns the number of parts enqueued; pair it
    /// with [`Self::flush_and_wait_uploads`] to wait for them to land.
    ///
    /// ⚠⚠ **Why this has to exist.** Uploads are enqueued at exactly one site —
    /// [`Self::register_and_upload`], on seal — so attaching a new replica to an
    /// *existing* store replicates nothing that already exists. Every part
    /// sealed before the attach keeps its old replica count **forever**, and the
    /// only thing that ever said so was `parts_under_replicated` sitting at a
    /// non-zero constant, which reads exactly like a transient upload backlog
    /// draining slowly. On prod this was 39 parts: an off-box replica was armed,
    /// every domain gauge flipped to "survives node loss", and all 39 parts of
    /// actual history were still single-copy (noetl/ehdb#400).
    ///
    /// Safe to call repeatedly. The substrate write is `put_if_absent`, and
    /// [`replicate_bytes`] records a location for an already-present object
    /// (`Ok(false)`) as well as a freshly-written one — so a re-drive re-states
    /// the copies that exist instead of dropping them from the manifest.
    ///
    /// Returns `0` when the replica set has one member: a single replica makes
    /// no spreading claim, so nothing is under-replicated by definition.
    pub fn backfill_under_replicated(&mut self) -> Result<usize> {
        let want = self.replicas.len();
        if want < 2 {
            return Ok(0);
        }
        let jobs: Vec<UploadJob> = {
            let m = self.manifest.lock().unwrap_or_else(|e| e.into_inner());
            m.parts
                .iter()
                .filter(|p| p.replica_count() < want)
                .filter_map(|p| {
                    let source = match (&p.local_path, p.primary_replica()) {
                        // Sealed in this process and not yet reclaimed.
                        (Some(path), _) => UploadSource::LocalFile(path.clone()),
                        // Recovered from the durable manifest: read it back out
                        // of a replica that has it.
                        (None, Some(loc)) => UploadSource::Replica {
                            replica: loc.replica.clone(),
                            key: loc.key.clone(),
                        },
                        // No local file and no replica — there is no copy of
                        // this part anywhere to read from, so there is nothing
                        // a backfill can do. `parts_local_only` is the gauge
                        // that reports these.
                        (None, None) => return None,
                    };
                    Some(UploadJob {
                        substrate_key: substrate_key_for(
                            &self.config.dataset,
                            p.partition,
                            &p.part_id,
                        ),
                        source,
                        part_id: p.part_id.clone(),
                        shard: p.partition,
                        sealed_at: None,
                    })
                })
                .collect()
        };
        let enqueued = jobs.len();
        for job in jobs {
            // Bump outstanding BEFORE sending so `flush_and_wait_uploads`
            // never races a job, exactly as the seal path does.
            {
                let (lock, _) = &*self.outstanding;
                *lock.lock().unwrap() += 1;
            }
            match &self.upload_tx {
                Some(tx) => {
                    if tx.send(job).is_err() {
                        decrement(&self.outstanding);
                    }
                }
                None => decrement(&self.outstanding),
            }
        }
        Ok(enqueued)
    }

    /// Seal every pending active part and block until the uploader has shipped
    /// all outstanding parts to the substrate (a graceful handoff / durability
    /// barrier — used before a cold-load equality check).
    pub fn flush_and_wait_uploads(&mut self) -> Result<()> {
        let shards: Vec<u32> = self.writers.keys().copied().collect();
        let mut sealed_parts = Vec::new();
        for shard in shards {
            let writer = self.writers.get_mut(&shard).unwrap();
            if writer.has_pending() {
                if let Some(sp) = writer.seal()? {
                    sealed_parts.push(sp);
                }
            }
        }
        for sp in sealed_parts {
            self.register_and_upload(sp)?;
        }
        let (lock, cvar) = &*self.outstanding;
        let mut n = lock.lock().unwrap();
        while *n > 0 {
            n = cvar.wait(n).unwrap();
        }
        Ok(())
    }

    /// **L0.3 background merge/compaction.** Repeatedly plan + perform merges
    /// until no partition has a long-enough contiguous run of small durable parts
    /// ([`crate::merge`]). Returns the number of merges performed. Each merge
    /// reads a contiguous run of small parts, writes one bigger immutable part
    /// (rebuilt sparse index + blooms), uploads it, and atomically swaps the
    /// manifest (remove sources, add merged) — so a cold-load after a merge sees
    /// the compacted catalog and reproduces the identical record set.
    ///
    /// The superseded source objects are left in place for the retention/GC slice
    /// (L0.5) to reclaim; the manifest no longer references them, so reads never
    /// touch them.
    /// **Seal every active part that has aged out** and enqueue its upload.
    /// Returns how many parts were sealed.
    ///
    /// ⚠ This exists because [`PartWriter::should_seal`] is only consulted on
    /// append, and the shard the age trigger protects is by definition the one
    /// taking no appends. Setting `seal_max_age` without driving this on a timer
    /// leaves the trigger **inert on exactly the shard it was added for** — the
    /// flag would be present, the config would look correct, and nothing would
    /// ever fire.
    ///
    /// A no-op when `seal_max_age` is `None`, so a caller can drive it
    /// unconditionally.
    pub fn seal_aged_parts(&mut self) -> Result<usize> {
        // ⚠ A cheap short-circuit, NOT the enforcement point. Removing it
        // changes no behavior — `PartWriter::aged_out` already returns false
        // with no configured limit — and mutation testing confirms that. It is
        // kept to avoid walking every writer on the default path; do not read it
        // as the thing that keeps the trigger off.
        if self.config.seal_max_age.is_none() {
            return Ok(0);
        }
        let aged: Vec<u32> = self
            .writers
            .iter()
            .filter(|(_, w)| w.aged_out())
            .map(|(shard, _)| *shard)
            .collect();
        let mut sealed_count = 0;
        for shard in aged {
            let sealed = match self.writers.get_mut(&shard) {
                Some(w) => w.seal()?,
                None => None,
            };
            if let Some(sealed) = sealed {
                self.register_and_upload(sealed)?;
                sealed_count += 1;
            }
        }
        self.refresh_state_gauges();
        Ok(sealed_count)
    }

    /// **Copy the unsealed tail off-box** (B3, noetl/ehdb#394). Returns what
    /// moved.
    ///
    /// Until a part seals it exists only on local disk, so the loss window is
    /// the seal interval — `seal_max_age` bounds that window but cannot close
    /// it. This copies records still held in an active writer to the replica
    /// set as their own **write-once** objects, so a cold load can replay them
    /// ([`Self::recover_tail_from_replicas`]).
    ///
    /// ⚠ Why per-batch objects and not a re-upload of the active part: the part
    /// grows, so re-uploading it every tick is O(size) per tick and therefore
    /// quadratic over the part's life — a 7 MiB part re-uploaded every 60 s for
    /// 7 h is ~1.5 GB of puts for ~7 MiB of data. A batch per tick writes ~the
    /// data volume, once.
    ///
    /// A no-op when `tail_replication` is false, so a driver can call it
    /// unconditionally.
    ///
    /// ⚠⚠ **A failing tail put does not fail anything** (noetl/ehdb#394 D3).
    /// The record is already durable locally; failing an append because a
    /// *replica* is unreachable would convert a durability feature into an
    /// availability regression — the same trade A2 made when it chose to open
    /// at RF=1 rather than refuse. The cursor is left where it was, so the next
    /// tick re-drives the same records, and the failure is counted rather than
    /// silent.
    ///
    /// ⚠ Honest bound: this makes loss bounded by **the driver's tick
    /// interval**, not zero. No remote ack gates the local append, so a loss
    /// between a tick and the next one is still a loss. It is not consensus and
    /// not a quorum.
    pub fn replicate_tail(&mut self) -> Result<TailReplicationReport> {
        let batches = self.prepare_tail_batches()?;
        let replicas = self.replicas.clone();
        let mut report = TailReplicationReport::default();
        for batch in &batches {
            if upload_tail_batch(&replicas, batch, &self.metrics) {
                self.commit_tail_batch(batch, &mut report);
            } else {
                self.fail_tail_batch(batch, &mut report);
            }
        }
        Ok(report)
    }

    /// **Prepare** the tail batches without touching any substrate — the
    /// lock-held half.
    ///
    /// Cheap by construction: it clones and serializes records, which is CPU,
    /// and performs **no I/O**. A caller holding the engine lock can do this
    /// and release before [`upload_tail_batch`].
    ///
    /// Empty when `tail_replication` is false, so a driver can call it
    /// unconditionally.
    pub fn prepare_tail_batches(&self) -> Result<Vec<TailBatch>> {
        if !self.config.tail_replication {
            return Ok(Vec::new());
        }
        let mut shards: Vec<u32> = self.writers.keys().copied().collect();
        shards.sort_unstable();
        let mut out = Vec::new();
        for shard in shards {
            let cursor = self.tail_cursor.get(&shard).copied().unwrap_or(0);
            let pending: Vec<&D::Record> = match self.writers.get(&shard) {
                Some(w) => w
                    .pending_records()
                    .iter()
                    .filter(|r| D::sort_key(r) > cursor)
                    .collect(),
                None => continue,
            };
            if pending.is_empty() {
                continue;
            }
            let mut bytes: Vec<u8> = Vec::new();
            let mut lo = u64::MAX;
            let mut hi = 0u64;
            for rec in &pending {
                let body = serde_json::to_vec(rec)
                    .map_err(|err| EhdbError::Storage(format!("encode tail record: {err}")))?;
                bytes.extend_from_slice(&crate::frame::encode_frame(&body)?);
                let k = D::sort_key(rec);
                lo = lo.min(k);
                hi = hi.max(k);
            }
            out.push(TailBatch {
                key: tail_key(&self.config.dataset, shard, lo, hi),
                bytes,
                shard,
                high_sort_key: hi,
                records: pending.len(),
            });
        }
        Ok(out)
    }

    /// The replica set, for a driver that uploads with the lock released.
    /// Cloning is `Arc` clones.
    pub fn replica_handles(&self) -> Vec<ReplicaTarget> {
        self.replicas.clone()
    }

    /// **Commit** a batch that reached at least one replica — advance the
    /// watermark. The other lock-held half, and it does no I/O.
    pub fn commit_tail_batch(&mut self, batch: &TailBatch, report: &mut TailReplicationReport) {
        self.tail_cursor.insert(batch.shard, batch.high_sort_key);
        self.metrics
            .record_tail_batch(batch.records as u64, batch.bytes.len() as u64);
        report.batches += 1;
        report.records += batch.records;
        report.bytes += batch.bytes.len();
        report.shards.push(batch.shard);
    }

    /// **Record a batch that reached no replica.** The watermark is left where
    /// it was, so the next pass re-drives exactly these records.
    pub fn fail_tail_batch(&mut self, batch: &TailBatch, report: &mut TailReplicationReport) {
        self.metrics.incr_tail_replication_failed();
        report.failed_shards.push(batch.shard);
    }

    /// **The highest sort key per shard covered by a CONTIGUOUS run of durable
    /// parts** — what makes a tail object safe to delete (D2).
    ///
    /// Walks each shard's parts in ascending `min_sequence` and advances the
    /// watermark only while they are durable, stopping at the **first**
    /// non-durable part.
    ///
    /// ⚠⚠ Stopping is the point. Taking `max(max_sequence)` over durable parts
    /// would skip a local-only part in the middle and authorise deleting a tail
    /// object whose records exist nowhere off-box — turning a cleanup into the
    /// destruction of the only remote copy of those events. The conservative
    /// answer costs some retained objects; the other answer costs data.
    ///
    /// No I/O: reads the in-RAM manifest only, so it is safe under the lock.
    pub fn contiguous_durable_watermarks(&self) -> Vec<(u32, u64)> {
        let m = self.manifest.lock().unwrap_or_else(|e| e.into_inner());
        let mut by_shard: std::collections::BTreeMap<u32, Vec<&crate::catalog::PartMeta>> =
            std::collections::BTreeMap::new();
        for p in &m.parts {
            by_shard.entry(p.partition).or_default().push(p);
        }
        let mut out = Vec::new();
        for (shard, mut parts) in by_shard {
            parts.sort_by_key(|p| p.min_sequence);
            let mut watermark = 0u64;
            for p in parts {
                if !p.is_durable() {
                    break;
                }
                watermark = watermark.max(p.max_sequence);
            }
            out.push((shard, watermark));
        }
        out
    }

    /// The per-shard tail watermark — the highest sort key already copied
    /// off-box. Exposed so a test or an operator can see the replicator's
    /// progress without inferring it from a counter.
    pub fn tail_watermarks(&self) -> Vec<(u32, u64)> {
        let mut out: Vec<(u32, u64)> = self
            .tail_cursor
            .iter()
            .map(|(shard, seq)| (*shard, *seq))
            .collect();
        out.sort_by_key(|(shard, _)| *shard);
        out
    }

    /// **Replay tail objects from the replica set** (B3 recovery). Returns how
    /// many records were recovered — records that existed nowhere but the dead
    /// node's local disk and the replicas' tail objects.
    ///
    /// `after_seq` is the durable manifest's `max_sequence`: everything at or
    /// below it is already covered by a sealed part.
    ///
    /// ⚠⚠ The overlap is real and expected, not an error (the noetl/ai-meta#335
    /// shape): a part that sealed *after* its tail objects were written makes
    /// them redundant, so the same record legitimately exists in both a tail
    /// object and a sealed part. Dedup is by sort key against `after_seq` and
    /// then across objects, which is why this cannot simply append everything
    /// it finds.
    ///
    /// ⚠ Recovered records are appended through the ordinary append path, so
    /// they land in a fresh active part, become readable through the existing
    /// tail read in [`Self::read_partition_after_limited`], and will seal and
    /// upload normally. B3 needs no new read path — that is the point.
    /// Appending in ascending sort-key order is required by the
    /// ascending-contract canary.
    fn recover_tail_from_replicas(&mut self, after_seq: u64) -> Result<usize> {
        let prefix = tail_prefix(&self.config.dataset);
        let mut recovered: std::collections::BTreeMap<u64, D::Record> =
            std::collections::BTreeMap::new();
        let mut superseded_objects = 0u64;
        for target in &self.replicas {
            let keys = match target.substrate.list_prefix(&prefix) {
                Ok(k) => k,
                // A replica that cannot be listed is skipped; the survivors
                // carry the tail, exactly as they carry parts.
                Err(_) => continue,
            };
            for key in keys {
                let bytes = match target.substrate.get_all(&key) {
                    Ok(b) => b,
                    Err(_) => continue,
                };
                let frames = crate::frame::iter_frames_from(&bytes, 0)?;
                let mut contributed = false;
                for frame in frames {
                    let rec: D::Record = serde_json::from_slice(frame.body).map_err(|err| {
                        EhdbError::Storage(format!("decode tail record in {key}: {err}"))
                    })?;
                    let k = D::sort_key(&rec);
                    if k > after_seq {
                        recovered.entry(k).or_insert(rec);
                        contributed = true;
                    }
                }
                if !contributed {
                    superseded_objects += 1;
                }
            }
        }
        if superseded_objects > 0 {
            self.metrics.add_tail_objects_superseded(superseded_objects);
        }
        let n = recovered.len();
        // BTreeMap iteration is ascending by sort key, which the append path
        // requires.
        for (_, rec) in recovered {
            self.append_record(rec)?;
        }
        if n > 0 {
            self.metrics.add_tail_recovered(n as u64);
        }
        Ok(n)
    }

    /// The age of the oldest un-sealed record per shard — what the age trigger
    /// is comparing against.
    pub fn active_ages(&self) -> Vec<(u32, Duration)> {
        let mut out: Vec<(u32, Duration)> = self
            .writers
            .iter()
            .filter_map(|(shard, w)| w.active_age().map(|age| (*shard, age)))
            .collect();
        out.sort_by_key(|(shard, _)| *shard);
        out
    }

    pub fn run_pending_merges(&mut self) -> Result<usize> {
        let mut count = 0;
        loop {
            let plan = {
                let m = self.manifest.lock().unwrap();
                plan_next_merge(&m, &self.config.merge_policy)
            };
            let Some(plan) = plan else { break };
            self.merge_once(plan)?;
            count += 1;
        }
        self.refresh_state_gauges();
        Ok(count)
    }

    fn merge_once(&mut self, plan: MergePlan) -> Result<()> {
        // Snapshot the source part metas (clone) so we drop the lock before I/O.
        let sources: Vec<PartMeta> = {
            let m = self.manifest.lock().unwrap();
            plan.source_ids
                .iter()
                .filter_map(|id| m.parts.iter().find(|p| &p.part_id == id).cloned())
                .collect()
        };
        if sources.len() < 2 {
            return Ok(()); // nothing to merge (raced away)
        }

        // Read every source part's records (local hot tier if resident, else the
        // object store) and order them by the sort key.
        let mut records: Vec<D::Record> = Vec::new();
        for src in &sources {
            let bytes = self.read_whole_part(src)?;
            for frame in iter_frames_from(&bytes, 0)? {
                let rec: D::Record = serde_json::from_slice(frame.body).map_err(|err| {
                    EhdbError::Storage(format!("decode l0 record on merge: {err}"))
                })?;
                records.push(rec);
            }
        }
        records.sort_by_key(D::sort_key);

        // **Key-level compaction** (noetl/ehdb#391). For a dataset that declares a
        // `supersede_key`, keep only the highest-sort-key record per key among THESE
        // sources. Every dropped record already lost a latest-wins fold to a record that
        // is kept, so the global per-key maximum — and therefore the fold's answer — is
        // unchanged. A dataset that declares nothing is untouched.
        let superseded = compact_superseded::<D>(&mut records);
        if superseded > 0 {
            self.metrics.add_records_superseded(superseded);
        }

        // Build the merged immutable part (in a per-partition `merged/` subdir so
        // its active-file name can never collide with the append-path writer).
        let merged_dir = self.config.local_root.join(format!(
            "parts/{}/shard-{}/merged",
            self.config.dataset, plan.partition
        ));
        let sealed = build_merged_part::<D>(
            plan.partition,
            self.config.granule_size,
            &merged_dir,
            &records,
        )?;

        // Write the merged part synchronously to ALL replicas so the manifest
        // swap that removes the sources is durable-consistent for a cold-load.
        let substrate_key =
            substrate_key_for(&self.config.dataset, plan.partition, &sealed.meta.part_id);
        let local_path = sealed
            .meta
            .local_path
            .clone()
            .ok_or_else(|| EhdbError::InvalidState("merged part missing local_path".into()))?;
        let bytes = fs::read(&local_path).map_err(|err| EhdbError::Storage(err.to_string()))?;
        let locations = replicate_bytes(&self.replicas, &substrate_key, &bytes, &self.metrics);
        if locations.is_empty() {
            return Err(EhdbError::Storage(
                "merge: merged part failed to write to any replica".into(),
            ));
        }

        // Atomic manifest swap: remove the sources, add the merged part (durable).
        let durable = {
            let mut m = self.manifest.lock().unwrap();
            let source_set: std::collections::HashSet<&String> = plan.source_ids.iter().collect();
            m.parts.retain(|p| !source_set.contains(&p.part_id));
            let mut merged = sealed.meta.clone();
            merged.replicas = locations;
            m.parts.push(merged);
            m.version += 1;
            m.durable_view()
        };
        write_manifest_to_all(
            &self.replicas,
            &self.config.dataset,
            &durable,
            self.config.manifest_retain,
            &self.metrics,
        );

        self.metrics
            .record_merge(sources.len() as u64, bytes.len() as u64);
        Ok(())
    }

    /// **L0.5 orphan reclaim (GC).** Delete every part object + local part file
    /// the current manifest no longer references — chiefly the superseded source
    /// parts a merge (L0.3) leaves behind, and parts dropped by
    /// [`apply_retention`](Self::apply_retention). Idempotent (deleting a missing
    /// object is a no-op). Returns the number of objects/files reclaimed.
    ///
    /// Single-writer assumption: the caller is the shard owner, so no concurrent
    /// appender is racing an object into existence as GC lists.
    /// **Stranded active parts** — [noetl/ehdb#396](https://github.com/noetl/ehdb/issues/396).
    ///
    /// Every `*.active` file under this dataset's local parts root that is **not** the active
    /// path of a live writer. Read-only: it surveys, it does not delete.
    ///
    /// How these arise: a process appends records, acknowledges them, writes them into the
    /// active part file — and then exits before the part seals. The successor opens its own
    /// active path (`part-{next_local_id:06}.active`, and `next_local_id` restarts at 0), so
    /// the abandoned file is left behind. It is referenced by no manifest entry, so no read
    /// path reaches it, and until this change nothing deleted it either: the orphan sweep
    /// collected `.eslog` only. They accumulated **one per restart, forever** — measured at
    /// 11.2 MiB across 9 files on a production volume, the oldest 15 days old.
    ///
    /// ⚠⚠ The records in them were **acknowledged to a caller** and never made it into the
    /// manifest. Reclaiming them is therefore a data-disposition decision, not housekeeping,
    /// which is why this survey exists separately from the reclaim and why neither is wired
    /// into [`Self::apply_retention`]: a retention pass must not silently delete acked data
    /// as a side effect.
    pub fn stranded_active(&self) -> Result<Vec<(PathBuf, u64)>> {
        let live = self.live_active_paths();
        let parts_root = self
            .config
            .local_root
            .join(format!("parts/{}", self.config.dataset));
        let mut files = Vec::new();
        collect_files_with_extension(&parts_root, "active", &mut files)?;
        let mut out = Vec::new();
        for path in files {
            if live.contains(&path) {
                continue;
            }
            let bytes = fs::metadata(&path).map(|m| m.len()).unwrap_or(0);
            out.push((path, bytes));
        }
        out.sort();
        Ok(out)
    }

    /// The active path of every live writer — the files being appended to **right now**.
    ///
    /// ⚠⚠⚠ This is the single safety invariant of the reclaim below. Deleting one of these
    /// destroys in-flight acknowledged records that have not sealed yet, which is strictly
    /// worse than the leak it is meant to fix. It is excluded by exact path, taken from the
    /// writers themselves rather than reconstructed from a naming convention — a convention
    /// can drift from what the writer actually opened.
    fn live_active_paths(&self) -> std::collections::HashSet<PathBuf> {
        self.writers
            .values()
            .map(|w| w.active_path().to_path_buf())
            .collect()
    }

    /// Delete the stranded active parts [`Self::stranded_active`] reports.
    ///
    /// Returns `(files_deleted, bytes_freed)`. Idempotent: a second call finds nothing.
    ///
    /// ⚠ Deliberately **not** called from [`Self::apply_retention`] or
    /// [`Self::reclaim_orphans`]. The caller decides, because the records being discarded
    /// were acked. See [`Self::stranded_active`] for the full reasoning.
    pub fn reclaim_stranded_active(&mut self) -> Result<(usize, u64)> {
        let stranded = self.stranded_active()?;
        let mut files = 0usize;
        let mut bytes = 0u64;
        for (path, size) in stranded {
            fs::remove_file(&path).map_err(|err| EhdbError::Storage(err.to_string()))?;
            self.metrics.record_orphan_reclaim(size);
            files += 1;
            bytes += size;
        }
        Ok((files, bytes))
    }

    pub fn reclaim_orphans(&mut self) -> Result<usize> {
        let (referenced_objects, referenced_locals) = {
            let m = self.manifest.lock().unwrap();
            // Every substrate key referenced by any part's replica list — a
            // part may have N replicas (same key on different substrates), all of
            // which must be kept.
            let objs: std::collections::HashSet<String> = m
                .parts
                .iter()
                .flat_map(|p| p.replicas.iter().map(|r| r.key.clone()))
                .collect();
            let locals: std::collections::HashSet<String> = m
                .parts
                .iter()
                .filter_map(|p| p.local_path.clone())
                .collect();
            (objs, locals)
        };

        let mut reclaimed = 0usize;

        // Object orphans on EVERY replica substrate under this dataset's prefix.
        let prefix = format!("parts/{}/", self.config.dataset);
        for target in &self.replicas {
            for key in target.substrate.list_prefix(&prefix)? {
                if !referenced_objects.contains(&key) {
                    let bytes = target
                        .substrate
                        .get_all(&key)
                        .map(|b| b.len() as u64)
                        .unwrap_or(0);
                    target.substrate.delete(&key)?;
                    self.metrics.record_orphan_reclaim(bytes);
                    reclaimed += 1;
                }
            }
        }

        // Local hot-tier orphan part files.
        let parts_root = self
            .config
            .local_root
            .join(format!("parts/{}", self.config.dataset));
        let mut local_files = Vec::new();
        collect_eslog_files(&parts_root, &mut local_files)?;
        for path in local_files {
            let path_str = path.to_string_lossy().to_string();
            if !referenced_locals.contains(&path_str) {
                let bytes = fs::metadata(&path).map(|m| m.len()).unwrap_or(0);
                fs::remove_file(&path).map_err(|err| EhdbError::Storage(err.to_string()))?;
                self.metrics.record_orphan_reclaim(bytes);
                reclaimed += 1;
            }
        }

        self.refresh_state_gauges();
        Ok(reclaimed)
    }

    /// **L0.5 retention (drop-partition).** Drop every part entirely below
    /// `keep_from_sequence` (a part straddling the floor is kept whole — never a
    /// row-level delete), advance the manifest's `reclaimed_through`, rewrite the
    /// durable manifest, and reclaim the dropped parts' objects. A read below the
    /// floor afterward simply finds nothing (the records are gone), never an
    /// error. Returns the number of parts dropped.
    pub fn apply_retention(&mut self, keep_from_sequence: u64) -> Result<usize> {
        let plan = {
            let m = self.manifest.lock().unwrap();
            crate::retention::plan_retention(&m, keep_from_sequence)
        };
        if plan.is_empty() {
            return Ok(0);
        }

        // Manifest swap: drop the parts, advance the floor.
        let durable = {
            let mut m = self.manifest.lock().unwrap();
            let drop_set: std::collections::HashSet<&String> = plan.drop_ids.iter().collect();
            m.parts.retain(|p| !drop_set.contains(&p.part_id));
            if plan.reclaimed_through > m.reclaimed_through {
                m.reclaimed_through = plan.reclaimed_through;
            }
            m.version += 1;
            m.durable_view()
        };
        write_manifest_to_all(
            &self.replicas,
            &self.config.dataset,
            &durable,
            self.config.manifest_retain,
            &self.metrics,
        );

        let dropped = plan.drop_ids.len();
        self.metrics.record_parts_dropped(dropped as u64);
        // Reclaim the now-unreferenced dropped part objects + files.
        self.reclaim_orphans()?;
        Ok(dropped)
    }

    /// Convenience: retain at least the last `keep_last_records` sort-key values,
    /// dropping whole parts below that window.
    pub fn apply_retention_keep_last(&mut self, keep_last_records: u64) -> Result<usize> {
        let keep_from = {
            let m = self.manifest.lock().unwrap();
            m.max_sequence()
                .saturating_sub(keep_last_records)
                .saturating_add(1)
        };
        self.apply_retention(keep_from)
    }

    /// The retention floor — the highest sort-key value dropped by retention
    /// (`0` if nothing reclaimed). Reads below it find nothing.
    pub fn reclaimed_through(&self) -> u64 {
        self.manifest.lock().unwrap().reclaimed_through
    }

    /// Read a whole part's bytes — local hot tier if resident, else across the
    /// durable replicas with fallback.
    fn read_whole_part(&self, part: &PartMeta) -> Result<Vec<u8>> {
        if let Some(local_path) = &part.local_path {
            return fs::read(local_path).map_err(|err| EhdbError::Storage(err.to_string()));
        }
        self.read_across_replicas(part, |substrate, key| substrate.get_all(key))
    }

    /// Read a part from its durable replicas **with fallback** (L0.6): try each
    /// [`ReplicaLocation`] in order, resolving its `replica` id to a substrate
    /// handle; on failure move to the next. A dead `replica-0` is served from
    /// `replica-1`. Records a `read_fallbacks` metric whenever a non-primary
    /// replica is used. Errors only if the part is local-only or *every* replica
    /// fails.
    fn read_across_replicas<F>(&self, part: &PartMeta, read: F) -> Result<Vec<u8>>
    where
        F: Fn(&Arc<dyn DurableSubstrate>, &str) -> Result<Vec<u8>>,
    {
        if part.replicas.is_empty() {
            return Err(EhdbError::InvalidState(format!(
                "part {} has neither a local_path nor a durable replica",
                part.part_id
            )));
        }
        let mut last_err: Option<EhdbError> = None;
        for (attempt, loc) in part.replicas.iter().enumerate() {
            let Some(target) = self.replicas.iter().find(|t| t.id == loc.replica) else {
                // A replica id the current engine doesn't know (e.g. cold-loaded
                // with a different replica set) — skip it.
                continue;
            };
            match read(&target.substrate, &loc.key) {
                Ok(bytes) => {
                    if attempt > 0 {
                        self.metrics.record_read_fallback();
                    }
                    return Ok(bytes);
                }
                Err(err) => last_err = Some(err),
            }
        }
        Err(last_err.unwrap_or_else(|| {
            EhdbError::Storage(format!(
                "part {}: no reachable replica ({} listed)",
                part.part_id,
                part.replicas.len()
            ))
        }))
    }

    /// **The dataset-generic read path** (RFC §2.5 worked example + L0.2
    /// index-first pruning): records whose [`Dataset::index_key`] equals
    /// `index_value` with sort key `> after_seq`, in sort-key order.
    ///
    /// 1. Manifest prune (MinMax + partition skip): only parts of
    ///    [`Dataset::read_partition`]`(index_value)` whose range can hold a record
    ///    after `after_seq`. Non-matching parts are skipped with **zero I/O**.
    /// 2. **L0.2 bloom prune (index-first):** among the surviving parts, skip any
    ///    whose per-part index bloom says `index_value` is definitely absent.
    /// 3. Sparse index binary search: locate the granule containing `after_seq+1`.
    /// 4. **L0.2 granule bloom narrowing:** trim the ranged block to the
    ///    contiguous granule span whose blooms admit `index_value`.
    /// 5. Ranged read of only that block (local hot tier if resident, else a
    ///    ranged GET across replicas with fallback); decode + filter + the active
    ///    hot buffer.
    pub fn read_index_after(&self, index_value: &str, after_seq: u64) -> Result<Vec<D::Record>> {
        let shard = D::read_partition(index_value, self.config.shard_count);
        let mut out: Vec<D::Record> = Vec::new();

        let (candidate_parts, pruned_count, bloom_pruned_count) = {
            let m = self.manifest.lock().unwrap();
            let total_parts = m.parts.len();
            // Step 1: partition/MinMax prune. Clone the matched PartMeta so we
            // drop the manifest lock before any (possibly slow) substrate read.
            let partition_survivors: Vec<_> =
                m.prune(shard, after_seq).into_iter().cloned().collect();
            let after_partition = partition_survivors.len();
            // Step 2 (L0.2, index-first): the execution bloom rejects parts the
            // execution is definitely absent from — skipped with ZERO part I/O.
            let hits: Vec<_> = partition_survivors
                .into_iter()
                .filter(|p| p.execution_maybe_present(index_value))
                .collect();
            let bloom_pruned = after_partition - hits.len();
            // Every skipped part (wrong partition, below cursor, or bloom-
            // rejected) costs zero part I/O — pointer catalog + bloom only.
            let pruned = total_parts - hits.len();
            (hits, pruned, bloom_pruned)
        };

        for part in &candidate_parts {
            // Sparse-index start granule for the cursor.
            let start_offset = part.sparse_index.locate(after_seq + 1);
            let start_granule = part
                .sparse_index
                .marks
                .partition_point(|mark| mark.byte_offset < start_offset);
            // Granule-bloom narrowing: the contiguous granule span that may hold
            // the execution, starting no earlier than the cursor's granule.
            let (block_start, block_end) = match part.granule_span_for(index_value, start_granule) {
                Some((lo, hi)) => {
                    let block_start = part.granule_offset(lo);
                    // End = the next granule's mark, or the part end for the last.
                    let block_end = if hi < part.sparse_index.marks.len() {
                        part.granule_offset(hi)
                    } else {
                        part.byte_size
                    };
                    (block_start, block_end)
                }
                // No granule in range admits the execution (all bloom-rejected).
                None => continue,
            };
            let len = block_end.saturating_sub(block_start);
            if len == 0 {
                continue;
            }
            // Prefer the local hot tier (no substrate I/O); else a ranged GET
            // across the durable replicas with fallback (L0.6).
            let block = if let Some(local_path) = &part.local_path {
                read_local_range(local_path, block_start, len)?
            } else {
                self.read_across_replicas(part, |substrate, key| {
                    substrate.get_range(key, block_start, len)
                })?
            };
            for frame in iter_frames_from(&block, 0)? {
                let rec: D::Record = serde_json::from_slice(frame.body).map_err(|err| {
                    EhdbError::Storage(format!("decode l0 record in part {}: {err}", part.part_id))
                })?;
                if D::sort_key(&rec) > after_seq && D::index_key(&rec) == index_value {
                    out.push(rec);
                }
            }
        }

        // The active (unsealed) hot buffer for this shard.
        if let Some(writer) = self.writers.get(&shard) {
            for rec in writer.pending_records() {
                if D::sort_key(rec) > after_seq && D::index_key(rec) == index_value {
                    out.push(rec.clone());
                }
            }
        }

        out.sort_by_key(D::sort_key);
        self.metrics.record_read(
            pruned_count as u64,
            bloom_pruned_count as u64,
            candidate_parts.len() as u64,
        );
        Ok(out)
    }

    /// **L1 shard-scoped change-feed read** — every record in `shard` with sort
    /// key `> after_seq`, in sort-key order. Unlike [`read_index_after`], this is
    /// **not** index-filtered: it returns the shard's whole tail past the cursor,
    /// the read behind the `Watch(shard, cursor)` primitive ([`crate::feed`]).
    ///
    /// Manifest prune skips parts in other partitions or entirely at/below the
    /// cursor with **zero I/O**; each surviving part is read **ranged from the
    /// cursor's granule to the part end** (local hot tier if resident, else a
    /// ranged GET across replicas with fallback, L0.6), so a repeatedly-polling
    /// follower never re-reads the parts it has already drained. The active
    /// (unsealed) hot buffer for the shard is included, so a just-appended record
    /// is visible to a follower immediately (the shadow-feed / delivery path).
    pub fn read_partition_after(&self, shard: u32, after_seq: u64) -> Result<Vec<D::Record>> {
        self.read_partition_after_limited(shard, after_seq, usize::MAX)
    }

    /// `read_partition_after`, but delivering at most `limit` records.
    ///
    /// The unlimited form reads *the whole tail into one `Vec`*, and the feed
    /// then serialises that into a single frame. At `after_seq = 0` — which is
    /// what every replay-from-0 boot asks for — "the whole tail" is the whole
    /// log, so the delivery buffer is O(log). On prod that reached **3313 MiB**
    /// for 29,608 records against a 2 GiB container limit, and the system pool
    /// OOM-crashlooped for twelve days (noetl/ai-meta#298).
    ///
    /// Note what was *not* wrong: the consumer's resident index was correctly
    /// bounded the whole time (`NOETL_STATE_INDEX_MAX_BYTES`, 112 MiB resident
    /// against a 256 MiB ceiling). The configured limit bounded the resting
    /// index; nothing bounded the buffer the records arrive in. That is why
    /// every limit read healthy while the pod died.
    ///
    /// The early break is sound because [`Manifest::prune`] returns parts sorted
    /// by `min_sequence`: once `limit` records are in hand, every remaining part
    /// holds strictly higher sort keys, so the lowest `limit` records after the
    /// cursor are already collected. The hot buffer is the tail and is only read
    /// when the limit has not been reached. Peak becomes O(one part + limit)
    /// rather than O(log).
    ///
    /// Callers keep their cursor semantics unchanged: a short batch is exactly
    /// what a caught-up feed already returns, and the caller re-polls from the
    /// new cursor. No cursor is persisted and no ack is introduced, so the #119
    /// self-rehydrating shape is untouched.
    pub fn read_partition_after_limited(
        &self,
        shard: u32,
        after_seq: u64,
        limit: usize,
    ) -> Result<Vec<D::Record>> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let mut out: Vec<D::Record> = Vec::new();

        let candidate_parts: Vec<_> = {
            let m = self.manifest.lock().unwrap();
            // Clone the matched PartMeta so the manifest lock drops before any
            // (possibly slow) substrate read.
            m.prune(shard, after_seq).into_iter().cloned().collect()
        };

        for part in &candidate_parts {
            // Parts arrive in ascending `min_sequence`, so anything past this
            // point is strictly higher than what is already collected.
            if out.len() >= limit {
                break;
            }
            // Sparse-index start offset for the cursor; read from there to the
            // part end (the feed wants the shard's whole tail, no index narrowing).
            let start_offset = part.sparse_index.locate(after_seq + 1);
            let len = part.byte_size.saturating_sub(start_offset);
            if len == 0 {
                continue;
            }
            let block = if let Some(local_path) = &part.local_path {
                read_local_range(local_path, start_offset, len)?
            } else {
                self.read_across_replicas(part, |substrate, key| {
                    substrate.get_range(key, start_offset, len)
                })?
            };
            for frame in iter_frames_from(&block, 0)? {
                let rec: D::Record = serde_json::from_slice(frame.body).map_err(|err| {
                    EhdbError::Storage(format!("decode l0 record in part {}: {err}", part.part_id))
                })?;
                if D::sort_key(&rec) > after_seq {
                    out.push(rec);
                }
            }
        }

        // The active (unsealed) hot buffer for this shard — the tail, so it is
        // only worth reading when the limit has not already been reached.
        if out.len() < limit {
            if let Some(writer) = self.writers.get(&shard) {
                for rec in writer.pending_records() {
                    if D::sort_key(rec) > after_seq {
                        out.push(rec.clone());
                    }
                }
            }
        }

        out.sort_by_key(D::sort_key);
        out.truncate(limit);
        Ok(out)
    }

    /// The configured shard (partition) count.
    /// Sample the **D1 durability window** per shard — the age of the oldest
    /// acknowledged record not yet durable on the substrate, and how many such
    /// records there are (noetl/ehdb#328).
    ///
    /// ⚠ Every shard gets a row even when nothing is pending, so `0` is
    /// distinguishable from "this binary has no such metric".
    pub fn unreplicated_snapshot(&self) -> Vec<ShardUnreplicated> {
        self.unreplicated.snapshot()
    }

    pub fn shard_count(&self) -> u32 {
        self.config.shard_count
    }

    /// The shard a dataset key routes to (delegates to [`Dataset::read_partition`]).
    pub fn shard_for(&self, index_value: &str) -> u32 {
        D::read_partition(index_value, self.config.shard_count)
    }

    /// Reproduce the **entire** record set across all partitions in global-
    /// sequence order — the cold-load correctness helper. Reads each part fully
    /// (local if resident, else the substrate) plus the active hot buffers.
    pub fn replay_all(&self) -> Result<Vec<D::Record>> {
        let mut out: Vec<D::Record> = Vec::new();
        let parts: Vec<_> = {
            let m = self.manifest.lock().unwrap();
            let mut ps: Vec<_> = m.parts.to_vec();
            ps.sort_by_key(|p| (p.partition, p.min_sequence));
            ps
        };
        for part in &parts {
            let bytes = self.read_whole_part(part)?;
            for frame in iter_frames_from(&bytes, 0)? {
                let rec: D::Record = serde_json::from_slice(frame.body)
                    .map_err(|err| EhdbError::Storage(format!("decode l0 record: {err}")))?;
                out.push(rec);
            }
        }
        for writer in self.writers.values() {
            out.extend(writer.pending_records().iter().cloned());
        }
        out.sort_by_key(D::sort_key);
        Ok(out)
    }

    /// The current global-sequence tip.
    pub fn global_sequence(&self) -> u64 {
        self.global_sequence
    }

    /// The shared metrics handle.
    pub fn metrics(&self) -> Arc<L0Metrics> {
        Arc::clone(&self.metrics)
    }

    /// A snapshot clone of the in-RAM manifest.
    /// Recompute the four state gauges from in-RAM state (noetl/ai-meta#455 C6).
    ///
    /// Cheap but **not free**: it walks the manifest once, so it is O(parts). That is
    /// why it is called on manifest *mutations* (open, seal, merge, reclaim) and not on
    /// every append — a per-append O(parts) scan would make append cost grow with part
    /// count, which is precisely the quadratic shape `manifest_parts` exists to detect.
    /// Instrumenting a thing must not reproduce the defect it measures.
    ///
    /// Public so a scrape handler can force a refresh: between mutations the part
    /// gauges cannot change, but `dedupe_window_records` moves on every append, so its
    /// staleness is bounded by the seal interval unless a scraper calls this.
    pub fn refresh_state_gauges(&self) {
        let want_replicas = self.replicas.len();
        let (parts, local_only, under) = {
            let m = self.manifest.lock().unwrap_or_else(|e| e.into_inner());
            let mut local_only = 0u64;
            let mut under = 0u64;
            for part in &m.parts {
                let n = part.replica_count();
                if n == 0 {
                    local_only += 1;
                } else if n < want_replicas {
                    under += 1;
                }
            }
            (m.parts.len() as u64, local_only, under)
        };
        let dedupe: u64 = (0..self.config.shard_count)
            .map(|shard| self.dedupe.len(shard) as u64)
            .sum();
        self.metrics
            .set_state_gauges(parts, local_only, under, dedupe);
    }

    pub fn manifest_snapshot(&self) -> Manifest {
        self.manifest.lock().unwrap().clone()
    }
}

/// **D1 (event-log) convenience API** — the original ergonomic surface, now a
/// thin wrapper over the generic engine so every existing caller is unchanged.
impl L0Engine<D1EventLog> {
    /// Append one D1 event, assigning the next `global_sequence`. Returns it.
    pub fn append(
        &mut self,
        execution_id: &str,
        transaction_id: &str,
        payload: impl Into<String>,
    ) -> Result<u64> {
        let seq = self.global_sequence + 1;
        self.append_record(EventRecord::new(seq, execution_id, transaction_id, payload))
    }

    /// Events for `execution_id` with `global_sequence > after_seq` (the D1 read
    /// path; a thin alias for [`read_index_after`](Self::read_index_after)).
    pub fn read_execution_after(
        &self,
        execution_id: &str,
        after_seq: u64,
    ) -> Result<Vec<EventRecord>> {
        self.read_index_after(execution_id, after_seq)
    }
}

impl<D: Dataset> Drop for L0Engine<D> {
    fn drop(&mut self) {
        // Close the channel so the uploader thread exits, then join it.
        self.upload_tx = None;
        if let Some(handle) = self.upload_handle.take() {
            let _ = handle.join();
        }
    }
}

/// Drop records superseded by a later record with the same [`Dataset::supersede_key`].
///
/// `records` must already be sorted ascending by sort key. Walks backwards keeping the
/// first occurrence of each key — which, in ascending order, is the **last**, i.e. the
/// maximum-sort-key record. A record whose `supersede_key` is `None` is always kept, so a
/// dataset that does not opt in loses nothing. Returns how many were dropped.
fn compact_superseded<D: Dataset>(records: &mut Vec<D::Record>) -> u64 {
    let before = records.len();
    let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();
    let mut keep = vec![true; before];
    for i in (0..before).rev() {
        if let Some(key) = D::supersede_key(&records[i]) {
            if !seen.insert(key.to_string()) {
                keep[i] = false;
            }
        }
    }
    if keep.iter().all(|k| *k) {
        return 0;
    }
    let mut idx = 0usize;
    records.retain(|_| {
        let k = keep[idx];
        idx += 1;
        k
    });
    (before - records.len()) as u64
}

fn decrement(outstanding: &Arc<(Mutex<usize>, Condvar)>) {
    let (lock, cvar) = &**outstanding;
    let mut n = lock.lock().unwrap();
    if *n > 0 {
        *n -= 1;
    }
    cvar.notify_all();
}

/// Recursively collect `*.eslog` part files under `dir` (for orphan reclaim).
/// Collect files under `dir` whose extension is `ext`, recursively.
///
/// ⚠ `collect_eslog_files` below is the `"eslog"` case of this, kept as its own function
/// because callers read better that way. The extension filter is the whole reason
/// noetl/ehdb#396 existed: the orphan sweep collected **only** `.eslog`, so abandoned
/// `.active` parts were never seen by anything — not the manifest, not a read path, and not
/// the reclaimer.
fn collect_files_with_extension(
    dir: &std::path::Path,
    ext: &str,
    out: &mut Vec<PathBuf>,
) -> Result<()> {
    if !dir.exists() {
        return Ok(());
    }
    for entry in fs::read_dir(dir).map_err(|err| EhdbError::Storage(err.to_string()))? {
        let entry = entry.map_err(|err| EhdbError::Storage(err.to_string()))?;
        let path = entry.path();
        if path.is_dir() {
            collect_files_with_extension(&path, ext, out)?;
        } else if path.extension().map(|e| e == ext).unwrap_or(false) {
            out.push(path);
        }
    }
    Ok(())
}

fn collect_eslog_files(dir: &std::path::Path, out: &mut Vec<PathBuf>) -> Result<()> {
    if !dir.exists() {
        return Ok(());
    }
    for entry in fs::read_dir(dir).map_err(|err| EhdbError::Storage(err.to_string()))? {
        let entry = entry.map_err(|err| EhdbError::Storage(err.to_string()))?;
        let path = entry.path();
        if path.is_dir() {
            collect_eslog_files(&path, out)?;
        } else if path.extension().map(|e| e == "eslog").unwrap_or(false) {
            out.push(path);
        }
    }
    Ok(())
}

fn read_local_range(path: &str, offset: u64, len: u64) -> Result<Vec<u8>> {
    let mut f = File::open(path).map_err(|err| EhdbError::Storage(err.to_string()))?;
    f.seek(SeekFrom::Start(offset))
        .map_err(|err| EhdbError::Storage(err.to_string()))?;
    let mut buf = vec![0u8; len as usize];
    f.read_exact(&mut buf)
        .map_err(|err| EhdbError::Storage(err.to_string()))?;
    Ok(buf)
}

/// Prefix under which **tail** objects live.
///
/// ⚠⚠ Deliberately NOT under `parts/`. A tail object is not a part — it is a
/// copy of records still held in an active writer, with no `PartMeta`, no
/// sparse index and no manifest row. If one were ever treated as a part,
/// `plan_retention` could drop it as though it were one, and
/// `read_partition_after_limited` would try to read it through a sparse index
/// it does not have (noetl/ehdb#394).
fn tail_prefix(dataset: &str) -> String {
    format!("tail/{dataset}/")
}

/// One tail object's key. Zero-padded so a `list_prefix` returns them in
/// sequence order, matching how part keys are built.
fn tail_key(dataset: &str, shard: u32, from_seq: u64, to_seq: u64) -> String {
    format!("tail/{dataset}/shard-{shard}/seq-{from_seq:020}-{to_seq:020}.eslog")
}

/// One prepared tail batch: the bytes to write and where.
///
/// ⚠⚠ This type exists so the **substrate write can happen with the engine
/// lock released**. [`L0Engine::replicate_tail`] is a convenience that does
/// prepare → upload → commit in one call, which means the upload runs while
/// the caller holds the engine. For an embedded engine whose lock is also taken
/// by the live append path, that injects the full remote-put latency into every
/// append that collides with a tick — measured at p50 77 ms and p99 234 ms
/// against GCS, once per tick. The uploader thread has always avoided this (see
/// `start_uploader`: "the substrate writes happen OUTSIDE the lock so a slow
/// store never blocks appends/reads") and the tail replicator must too.
///
/// So a latency-sensitive driver uses
/// [`L0Engine::prepare_tail_batches`] → [`upload_tail_batch`] →
/// [`L0Engine::commit_tail_batch`], locking only for the first and last.
#[derive(Debug, Clone)]
pub struct TailBatch {
    /// Substrate key this batch is written to.
    pub key: String,
    /// Framed record bytes.
    pub bytes: Vec<u8>,
    /// The shard the records came from.
    pub shard: u32,
    /// Highest sort key in the batch — the watermark to commit on success.
    pub high_sort_key: u64,
    /// How many records the batch carries.
    pub records: usize,
}

/// What one [`L0Engine::replicate_tail`] pass did.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct TailReplicationReport {
    /// Objects written — one per shard that had new unsealed records.
    pub batches: usize,
    /// Records those objects carry.
    pub records: usize,
    /// Bytes those objects carry.
    pub bytes: usize,
    /// Shards whose tail advanced.
    pub shards: Vec<u32>,
    /// Shards where **every** replica write failed, so the cursor did not move
    /// and the records are still local-only.
    ///
    /// ⚠ Reported separately rather than folded into `batches == 0`: "nothing
    /// to do" and "could not do it" are opposite conditions and a caller that
    /// cannot tell them apart will read a failing replicator as an idle one.
    pub failed_shards: Vec<u32>,
}

impl TailReplicationReport {
    /// Whether anything reached a replica.
    pub fn replicated_anything(&self) -> bool {
        self.batches > 0
    }
}

fn manifest_latest_key(dataset: &str) -> String {
    format!("manifest/{dataset}/LATEST")
}

fn manifest_version_key(dataset: &str, version: u64) -> String {
    format!("manifest/{dataset}/manifest-v{version:020}.json")
}

/// **Write-once copy immutable `bytes` to every replica** under `key` (L0.6),
/// returning a [`ReplicaLocation`] for each replica that accepted the write.
/// Parts are immutable so `put_if_absent` is idempotent — a replica that already
/// holds the part still counts (it is durable there). Records a `replica_write`
/// metric per successful copy.
/// What one tail-reclaim pass did.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct TailReclaimReport {
    /// Objects deleted because a durable part already covers them.
    pub reclaimed: usize,
    /// Bytes those objects occupied, as the manifest-free `get_all` saw them.
    pub bytes: u64,
    /// Objects kept because their interval is NOT yet covered by a contiguous
    /// run of durable parts. **Deliberately reported**: a reclaimer that is
    /// deleting nothing and one that has nothing to delete both leave
    /// `reclaimed` at 0.
    pub retained: usize,
    /// Objects whose key could not be parsed, so they were left alone.
    ///
    /// ⚠ Never deleted on a parse failure. An unparseable key is a thing this
    /// code does not understand, and the safe response to not understanding an
    /// object in an event-log bucket is to leave it there.
    pub unparsed: usize,
    /// Delete calls that failed. Retried next pass; the object is still there.
    pub failed: usize,
}

/// The `(shard, high_sort_key)` a tail object's key encodes, or `None` when the
/// key is not one this code wrote.
fn parse_tail_key(dataset: &str, key: &str) -> Option<(u32, u64)> {
    let rest = key.strip_prefix(&format!("tail/{dataset}/shard-"))?;
    let (shard, rest) = rest.split_once("/seq-")?;
    let shard: u32 = shard.parse().ok()?;
    let core = rest.strip_suffix(".eslog")?;
    let (_lo, hi) = core.split_once('-')?;
    Some((shard, hi.parse().ok()?))
}

/// **Delete tail objects a durable part already covers** (noetl/ehdb#394 D2).
///
/// A free function, so every byte of I/O here happens with the engine lock
/// released — the same rule `upload_tail_batch` follows and for the same
/// measured reason. It takes the watermarks as data
/// ([`L0Engine::contiguous_durable_watermarks`]) rather than borrowing the
/// engine.
///
/// ⚠⚠ **Safety is the whole design.** An object is deleted only when its
/// `high_sort_key` is at or below a **contiguous** run of durable parts for its
/// shard. "Contiguous" is load-bearing: a later part being durable does not
/// mean a middle one is, and a durable part beyond the gap would otherwise
/// authorise deleting a tail object whose records are still single-copy in that
/// gap. That is the difference between reclaiming a redundant copy and
/// destroying the only off-box copy of an event.
///
/// Anything not understood is kept — unparseable keys, other datasets, other
/// prefixes.
pub fn reclaim_superseded_tail_objects(
    replicas: &[ReplicaTarget],
    dataset: &str,
    watermarks: &[(u32, u64)],
    metrics: &L0Metrics,
) -> TailReclaimReport {
    let mut report = TailReclaimReport::default();
    let wm: std::collections::HashMap<u32, u64> = watermarks.iter().copied().collect();
    let prefix = tail_prefix(dataset);
    for target in replicas {
        let keys = match target.substrate.list_prefix(&prefix) {
            Ok(k) => k,
            Err(_) => continue,
        };
        for key in keys {
            let Some((shard, hi)) = parse_tail_key(dataset, &key) else {
                report.unparsed += 1;
                continue;
            };
            let covered = wm.get(&shard).copied().unwrap_or(0);
            if hi > covered {
                report.retained += 1;
                continue;
            }
            // Size before deleting, so the reclaimed-bytes figure is real
            // rather than estimated. A failure to size is not a reason to keep
            // the object, only a reason not to count its bytes.
            let size = target
                .substrate
                .get_all(&key)
                .map(|b| b.len() as u64)
                .unwrap_or(0);
            match target.substrate.delete(&key) {
                Ok(()) => {
                    report.reclaimed += 1;
                    report.bytes += size;
                }
                Err(_) => report.failed += 1,
            }
        }
    }
    if report.reclaimed > 0 {
        metrics.add_tail_reclaimed(report.reclaimed as u64, report.bytes);
    }
    if report.failed > 0 {
        metrics.add_tail_reclaim_failed(report.failed as u64);
    }
    metrics.set_tail_objects_retained(report.retained as u64);
    report
}

/// Write one prepared tail batch to the replica set. `true` when at least one
/// replica took it.
///
/// ⚠ A free function on purpose: it borrows nothing from the engine, so a
/// driver can call it **with the engine lock released**. That is the whole
/// reason [`TailBatch`] exists.
pub fn upload_tail_batch(
    replicas: &[ReplicaTarget],
    batch: &TailBatch,
    metrics: &L0Metrics,
) -> bool {
    !replicate_bytes(replicas, &batch.key, &batch.bytes, metrics).is_empty()
}

fn replicate_bytes(
    replicas: &[ReplicaTarget],
    key: &str,
    bytes: &[u8],
    metrics: &L0Metrics,
) -> Vec<ReplicaLocation> {
    let mut locations = Vec::with_capacity(replicas.len());
    for target in replicas {
        match target.substrate.put_if_absent(key, bytes) {
            Ok(_) => {
                metrics.record_replica_write();
                locations.push(ReplicaLocation {
                    replica: target.id.clone(),
                    key: key.to_string(),
                });
            }
            Err(_) => { /* this replica is down; the others still give durability */ }
        }
    }
    locations
}

/// Write the durable manifest (LATEST pointer + versioned snapshot) to **every**
/// replica, so any one of them can serve a cold-load alone. Best-effort per
/// replica (a down replica is skipped; the survivors carry the manifest).
fn write_manifest_to_all(
    replicas: &[ReplicaTarget],
    dataset: &str,
    durable: &Manifest,
    manifest_retain: usize,
    metrics: &L0Metrics,
) {
    let Ok(ser) = serde_json::to_vec(durable) else {
        return;
    };
    for target in replicas {
        let _ = target
            .substrate
            .put_overwrite(&manifest_latest_key(dataset), &ser);
        let _ = target
            .substrate
            .put_if_absent(&manifest_version_key(dataset, durable.version), &ser);
    }
    prune_manifest_versions(replicas, dataset, durable.version, manifest_retain, metrics);
}

/// Delete versioned manifest snapshots older than the retention bound
/// (noetl/ehdb#344).
///
/// **Why this exists.** Every manifest write emits a *full* snapshot listing
/// every part. Nothing has ever read one — [`manifest_latest_key`] is the only
/// manifest the engine loads — and before this nothing deleted one, so the cost
/// was the product of two growing quantities: snapshot size grows with part
/// count, snapshot count grows with write count. On prod that reached 6,770
/// snapshots / 19.4 GB behind 71.8 MB of real data, filled the volume, and made
/// every append fail. Retention turns that from quadratic into linear.
///
/// **Two paths, and the sweep is the one that guarantees the bound.** The
/// per-write delete is an O(1) fast path: version `v - retain` is the one that
/// just fell out of the window. It is *best-effort only*. Manifest versions are
/// allocated under the manifest lock, so every integer is produced exactly once,
/// but the substrate writes are issued by two producers — the uploader thread
/// and the merge/drop path — so a version's file can land **after** the delete
/// that was meant to evict it already looked and found nothing. Those stragglers
/// then survive forever. Removing the sweep and keeping only the fast path
/// leaves 6 snapshots instead of 4 after 40 writes at `retain = 4`, and the
/// stragglers are arbitrarily old (`v6` and `v49` among `v77..v80`) — so the
/// fast path alone does not bound anything.
///
/// The periodic sweep is therefore the actual guarantee, and it doubles as the
/// only way to converge a backlog that predates this policy: a sliding window
/// can never reach below its own lower edge, so without the sweep an engine
/// upgraded onto an existing store would keep its 19.4 GB forever. It is
/// throttled to one pass per `retain` writes because
/// [`DurableSubstrate::list_prefix`] walks the whole substrate root, not just
/// the prefix. Every multiple of `retain` is produced exactly once, so the
/// throttle cannot be skipped by the same race.
///
/// `LATEST` is never a candidate: it does not carry the `manifest-v` prefix that
/// [`parse_manifest_version`] requires, and it is filtered again by name.
fn prune_manifest_versions(
    replicas: &[ReplicaTarget],
    dataset: &str,
    latest_version: u64,
    manifest_retain: usize,
    metrics: &L0Metrics,
) {
    if manifest_retain == 0 {
        // Retention disabled — the unbounded pre-fix behaviour. Kept reachable so
        // a test can prove the bound is what does the work.
        return;
    }
    let retain = manifest_retain as u64;
    let mut pruned = 0u64;

    // O(1) steady-state path: the version that just fell out of the window.
    if let Some(evicted) = latest_version.checked_sub(retain) {
        if evicted > 0 {
            let key = manifest_version_key(dataset, evicted);
            for target in replicas {
                if target.substrate.exists(&key).unwrap_or(false)
                    && target.substrate.delete(&key).is_ok()
                {
                    pruned += 1;
                }
            }
        }
    }

    // Backlog-convergence sweep, amortised one pass per `retain` writes.
    if latest_version % retain == 0 {
        let prefix = format!("manifest/{dataset}/");
        let latest_name = manifest_latest_key(dataset);
        for target in replicas {
            let Ok(keys) = target.substrate.list_prefix(&prefix) else {
                continue;
            };
            let mut versions: Vec<(u64, String)> = keys
                .into_iter()
                .filter(|k| k != &latest_name)
                .filter_map(|k| parse_manifest_version(&k).map(|v| (v, k)))
                .collect();
            metrics.set_manifest_versions_retained(versions.len() as u64);
            if versions.len() <= manifest_retain {
                continue;
            }
            versions.sort_unstable_by_key(|(v, _)| *v);
            let drop_count = versions.len() - manifest_retain;
            for (_, key) in versions.into_iter().take(drop_count) {
                if target.substrate.delete(&key).is_ok() {
                    pruned += 1;
                }
            }
            metrics.set_manifest_versions_retained(manifest_retain as u64);
        }
    }

    if pruned > 0 {
        metrics.add_manifest_versions_pruned(pruned);
    }
}

/// Parse the version out of a `manifest/<dataset>/manifest-v<020>.json` key.
///
/// Returns `None` for anything that is not a versioned snapshot — `LATEST` most
/// importantly, but also any unrelated key the substrate walk turns up. A prune
/// candidate must round-trip through here, so a key that does not parse can
/// never be deleted.
fn parse_manifest_version(key: &str) -> Option<u64> {
    let name = key.rsplit('/').next()?;
    let digits = name.strip_prefix("manifest-v")?.strip_suffix(".json")?;
    if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    digits.parse::<u64>().ok()
}

/// Load the durable manifest from the **first replica that has it** (L0.6): a
/// dead `replica-0` does not stop recovery — the manifest is replicated, so any
/// survivor serves it.
fn load_durable_manifest(replicas: &[ReplicaTarget], dataset: &str) -> Result<Option<Manifest>> {
    let key = manifest_latest_key(dataset);
    for target in replicas {
        match target.substrate.exists(&key) {
            Ok(true) => {}
            _ => continue,
        }
        let Ok(bytes) = target.substrate.get_all(&key) else {
            continue;
        };
        let manifest: Manifest = serde_json::from_slice(&bytes)
            .map_err(|err| EhdbError::Storage(format!("decode durable manifest: {err}")))?;
        return Ok(Some(manifest));
    }
    Ok(None)
}
