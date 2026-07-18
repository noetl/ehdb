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
use std::time::Instant;

use ehdb_core::{EhdbError, Result};

use crate::catalog::{Manifest, PartMeta, ReplicaLocation};
use crate::dataset::{D1EventLog, Dataset, EventRecord, DATASET_D1_EVENT_LOG, DEFAULT_SHARD_COUNT};
use crate::frame::iter_frames_from;
use crate::merge::{plan_next_merge, MergePlan, MergePolicy};
use crate::metrics::L0Metrics;
use crate::part::{build_merged_part, substrate_key_for, FlushPolicy, PartWriter, SealedPart};
use crate::substrate::DurableSubstrate;

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
    /// Durability-window posture (D1 default = [`FlushPolicy::EveryAppend`]).
    pub flush: FlushPolicy,
    /// L0.3 background merge/compaction policy.
    pub merge_policy: MergePolicy,
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
            flush: FlushPolicy::EveryAppend,
            merge_policy: MergePolicy::d1(DEFAULT_SEAL_MAX_RECORDS),
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
    /// Set the byte-size seal threshold.
    pub fn with_seal_max_bytes(mut self, seal_max_bytes: u64) -> Self {
        self.seal_max_bytes = seal_max_bytes;
        self
    }
    /// Set the durability-window posture.
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
}

impl ReplicaTarget {
    /// Construct a replica target.
    pub fn new(id: impl Into<String>, substrate: Arc<dyn DurableSubstrate>) -> Self {
        Self {
            id: id.into(),
            substrate,
        }
    }
}

/// A unit of upload work handed to the background uploader thread.
struct UploadJob {
    substrate_key: String,
    local_path: String,
    part_id: String,
    sealed_at: Instant,
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
    /// Sender to the uploader thread (dropped on close to stop it).
    upload_tx: Option<Sender<UploadJob>>,
    upload_handle: Option<JoinHandle<()>>,
    /// Outstanding upload count + condvar for `flush_and_wait_uploads`.
    outstanding: Arc<(Mutex<usize>, Condvar)>,
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
        engine.start_uploader();
        Ok(engine)
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
        Ok(engine)
    }

    fn assemble(
        config: L0Config,
        replicas: Vec<ReplicaTarget>,
        metrics: Arc<L0Metrics>,
        manifest: Manifest,
        global_sequence: u64,
    ) -> Self {
        Self {
            config,
            replicas,
            metrics,
            manifest: Arc::new(Mutex::new(manifest)),
            writers: HashMap::new(),
            global_sequence,
            upload_tx: None,
            upload_handle: None,
            outstanding: Arc::new((Mutex::new(0), Condvar::new())),
        }
    }

    fn start_uploader(&mut self) {
        let (tx, rx) = mpsc::channel::<UploadJob>();
        let replicas = self.replicas.clone();
        let manifest = Arc::clone(&self.manifest);
        let metrics = Arc::clone(&self.metrics);
        let outstanding = Arc::clone(&self.outstanding);
        let dataset = self.config.dataset.clone();
        let handle = std::thread::Builder::new()
            .name("ehdb-l0-uploader".to_string())
            .spawn(move || {
                while let Ok(job) = rx.recv() {
                    // Read the sealed part bytes and **write-once copy to every
                    // replica** (N-way, L0.6). The append path never does this —
                    // durability is asynchronous (RFC §2.3). Parts are immutable,
                    // so each copy is byte-identical and no consensus is needed.
                    let read_result = fs::read(&job.local_path)
                        .map_err(|err| EhdbError::Storage(err.to_string()));
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
                    write_manifest_to_all(&replicas, &dataset, &durable);

                    let lag = job.sealed_at.elapsed().as_micros() as u64;
                    metrics.record_upload(bytes.len() as u64, lag);
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
    pub fn append_record(&mut self, record: D::Record) -> Result<u64> {
        let sort_key = D::sort_key(&record);
        let shard = D::partition(&record, self.config.shard_count);
        self.ensure_writer(shard)?;
        {
            let writer = self.writers.get_mut(&shard).unwrap();
            writer.append(record)?;
        }
        if sort_key > self.global_sequence {
            self.global_sequence = sort_key;
        }
        self.metrics.incr_appends();

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

        {
            let mut m = self.manifest.lock().unwrap();
            m.push_part(sealed.meta);
        }
        self.metrics.incr_seals();

        // Bump outstanding BEFORE sending so flush_and_wait never races a job.
        {
            let (lock, _) = &*self.outstanding;
            *lock.lock().unwrap() += 1;
        }
        if let Some(tx) = &self.upload_tx {
            let job = UploadJob {
                substrate_key,
                local_path,
                part_id,
                sealed_at: Instant::now(),
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
        write_manifest_to_all(&self.replicas, &self.config.dataset, &durable);

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
        write_manifest_to_all(&self.replicas, &self.config.dataset, &durable);

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

fn decrement(outstanding: &Arc<(Mutex<usize>, Condvar)>) {
    let (lock, cvar) = &**outstanding;
    let mut n = lock.lock().unwrap();
    if *n > 0 {
        *n -= 1;
    }
    cvar.notify_all();
}

/// Recursively collect `*.eslog` part files under `dir` (for orphan reclaim).
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
fn write_manifest_to_all(replicas: &[ReplicaTarget], dataset: &str, durable: &Manifest) {
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
