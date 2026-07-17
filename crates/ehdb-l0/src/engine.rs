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

use crate::catalog::{Manifest, PartMeta};
use crate::dataset::{shard_for_execution, EventRecord, DATASET_D1_EVENT_LOG, DEFAULT_SHARD_COUNT};
use crate::frame::iter_frames_from;
use crate::merge::{plan_next_merge, MergePlan, MergePolicy};
use crate::metrics::L0Metrics;
use crate::part::{build_merged_part, substrate_key_for, FlushPolicy, PartWriter, SealedPart};
use crate::substrate::DurableSubstrate;

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

/// A unit of upload work handed to the background uploader thread.
struct UploadJob {
    substrate_key: String,
    local_path: String,
    part_id: String,
    sealed_at: Instant,
}

/// The L0 event-log engine (single writer per partition — the caller is the
/// shard owner, matching #254's single-writer assumption).
pub struct L0EventLogEngine {
    config: L0Config,
    substrate: Arc<dyn DurableSubstrate>,
    metrics: Arc<L0Metrics>,
    /// In-RAM catalog — local-only + durable parts. Shared with the uploader
    /// thread, which records a replica on upload.
    manifest: Arc<Mutex<Manifest>>,
    /// Per-shard active writers (engine-owned single writer).
    writers: HashMap<u32, PartWriter>,
    /// Monotonic global sequence assigned at append (the D1 sort key).
    global_sequence: u64,
    /// Sender to the uploader thread (dropped on close to stop it).
    upload_tx: Option<Sender<UploadJob>>,
    upload_handle: Option<JoinHandle<()>>,
    /// Outstanding upload count + condvar for `flush_and_wait_uploads`.
    outstanding: Arc<(Mutex<usize>, Condvar)>,
}

impl L0EventLogEngine {
    /// Open a writer engine over `substrate`, reusing any manifest already in
    /// the substrate (so a restart of the owner resumes its catalog). The
    /// local hot tier is `config.local_root`.
    pub fn open(config: L0Config, substrate: Arc<dyn DurableSubstrate>) -> Result<Self> {
        let metrics = L0Metrics::new();
        Self::open_with_metrics(config, substrate, metrics)
    }

    /// Open sharing an existing [`L0Metrics`] handle (so a caller can read
    /// counters).
    pub fn open_with_metrics(
        config: L0Config,
        substrate: Arc<dyn DurableSubstrate>,
        metrics: Arc<L0Metrics>,
    ) -> Result<Self> {
        fs::create_dir_all(&config.local_root)
            .map_err(|err| EhdbError::Storage(err.to_string()))?;
        // Resume the durable catalog if present (owner restart), else start empty.
        let manifest = load_durable_manifest(&*substrate, &config.dataset)?
            .unwrap_or_else(|| Manifest::empty(&config.dataset));
        let global_sequence = manifest.max_sequence();
        let mut engine = Self::assemble(config, substrate, metrics, manifest, global_sequence);
        engine.start_uploader();
        Ok(engine)
    }

    /// **Cold-load** a fresh node (empty local dir) from the substrate: read
    /// the durable manifest and serve reads entirely from substrate-replica parts.
    /// Reproduces the exact record set + global sequence of the origin — the
    /// fungible-writer property that retires the per-shard-Raft "T-RF" plan
    /// (RFC §2.7). The returned engine can also *resume writing* (new parts
    /// continue from the recovered `global_sequence`).
    pub fn cold_load(config: L0Config, substrate: Arc<dyn DurableSubstrate>) -> Result<Self> {
        let metrics = L0Metrics::new();
        Self::cold_load_with_metrics(config, substrate, metrics)
    }

    /// Cold-load sharing a metrics handle.
    pub fn cold_load_with_metrics(
        config: L0Config,
        substrate: Arc<dyn DurableSubstrate>,
        metrics: Arc<L0Metrics>,
    ) -> Result<Self> {
        fs::create_dir_all(&config.local_root)
            .map_err(|err| EhdbError::Storage(err.to_string()))?;
        let manifest = load_durable_manifest(&*substrate, &config.dataset)?.ok_or_else(|| {
            EhdbError::InvalidState(format!(
                "cold-load: no durable manifest for dataset {}",
                config.dataset
            ))
        })?;
        let global_sequence = manifest.max_sequence();
        metrics.incr_cold_loads();
        let mut engine = Self::assemble(config, substrate, metrics, manifest, global_sequence);
        engine.start_uploader();
        Ok(engine)
    }

    fn assemble(
        config: L0Config,
        substrate: Arc<dyn DurableSubstrate>,
        metrics: Arc<L0Metrics>,
        manifest: Manifest,
        global_sequence: u64,
    ) -> Self {
        Self {
            config,
            substrate,
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
        let substrate = Arc::clone(&self.substrate);
        let manifest = Arc::clone(&self.manifest);
        let metrics = Arc::clone(&self.metrics);
        let outstanding = Arc::clone(&self.outstanding);
        let dataset = self.config.dataset.clone();
        let handle = std::thread::Builder::new()
            .name("ehdb-l0-uploader".to_string())
            .spawn(move || {
                while let Ok(job) = rx.recv() {
                    // Read the sealed part bytes and ship them to the object
                    // store. The append path never does this — durability is
                    // asynchronous (RFC §2.3).
                    let upload_result = (|| -> Result<u64> {
                        let bytes = fs::read(&job.local_path)
                            .map_err(|err| EhdbError::Storage(err.to_string()))?;
                        substrate.put_if_absent(&job.substrate_key, &bytes)?;
                        Ok(bytes.len() as u64)
                    })();

                    let bytes_len = match upload_result {
                        Ok(n) => n,
                        Err(_) => {
                            // On upload failure the part stays local-only (its
                            // manifest row keeps `replicas` empty); a later
                            // retry slice re-drives it. Still decrement so a
                            // waiter isn't wedged.
                            decrement(&outstanding);
                            continue;
                        }
                    };

                    // Record the replica on the part and snapshot the durable view
                    // — under the lock, but the substrate write happens
                    // OUTSIDE the lock so a slow store never blocks appends/reads.
                    let durable = {
                        let mut m = manifest.lock().unwrap();
                        if let Some(p) = m.parts.iter_mut().find(|p| p.part_id == job.part_id) {
                            // Single-replica write now; N-way copy appends more
                            // keys here (the replication seam — parts are
                            // immutable, so no consensus is needed).
                            p.replicas = vec![job.substrate_key.clone()];
                        }
                        m.version += 1;
                        m.durable_view()
                    };
                    if let Ok(ser) = serde_json::to_vec(&durable) {
                        let _ = substrate.put_overwrite(&manifest_latest_key(&dataset), &ser);
                        let _ = substrate
                            .put_if_absent(&manifest_version_key(&dataset, durable.version), &ser);
                    }

                    let lag = job.sealed_at.elapsed().as_micros() as u64;
                    metrics.record_upload(bytes_len, lag);
                    decrement(&outstanding);
                }
            })
            .expect("spawn ehdb-l0 uploader thread");
        self.upload_tx = Some(tx);
        self.upload_handle = Some(handle);
    }

    /// Append one D1 event to the hot tier, assigning the next global sequence.
    /// Never touches the substrate. Returns the assigned global sequence.
    pub fn append(
        &mut self,
        execution_id: &str,
        transaction_id: &str,
        payload: impl Into<String>,
    ) -> Result<u64> {
        let seq = self.global_sequence + 1;
        let shard = shard_for_execution(execution_id, self.config.shard_count);
        self.ensure_writer(shard)?;
        {
            let writer = self.writers.get_mut(&shard).unwrap();
            writer.append(EventRecord::new(seq, execution_id, transaction_id, payload))?;
        }
        self.global_sequence = seq;
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
        Ok(seq)
    }

    fn ensure_writer(&mut self, shard: u32) -> Result<()> {
        if !self.writers.contains_key(&shard) {
            let writer = PartWriter::open(
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
    fn register_and_upload(&mut self, sealed: SealedPart) -> Result<()> {
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
        let mut records: Vec<EventRecord> = Vec::new();
        for src in &sources {
            let bytes = self.read_whole_part(src)?;
            for frame in iter_frames_from(&bytes, 0)? {
                let rec: EventRecord = serde_json::from_slice(frame.body).map_err(|err| {
                    EhdbError::Storage(format!("decode l0 record on merge: {err}"))
                })?;
                records.push(rec);
            }
        }
        records.sort_by_key(|r| r.global_sequence);

        // Build the merged immutable part (in a per-partition `merged/` subdir so
        // its active-file name can never collide with the append-path writer).
        let merged_dir = self.config.local_root.join(format!(
            "parts/{}/shard-{}/merged",
            self.config.dataset, plan.partition
        ));
        let sealed = build_merged_part(
            plan.partition,
            self.config.granule_size,
            &merged_dir,
            &records,
        )?;

        // Upload the merged part synchronously so the manifest swap that removes
        // the sources is durable-consistent for a cold-load.
        let substrate_key =
            substrate_key_for(&self.config.dataset, plan.partition, &sealed.meta.part_id);
        let local_path = sealed
            .meta
            .local_path
            .clone()
            .ok_or_else(|| EhdbError::InvalidState("merged part missing local_path".into()))?;
        let bytes = fs::read(&local_path).map_err(|err| EhdbError::Storage(err.to_string()))?;
        self.substrate.put_if_absent(&substrate_key, &bytes)?;

        // Atomic manifest swap: remove the sources, add the merged part (durable).
        let durable = {
            let mut m = self.manifest.lock().unwrap();
            let source_set: std::collections::HashSet<&String> = plan.source_ids.iter().collect();
            m.parts.retain(|p| !source_set.contains(&p.part_id));
            let mut merged = sealed.meta.clone();
            merged.replicas = vec![substrate_key.clone()];
            m.parts.push(merged);
            m.version += 1;
            m.durable_view()
        };
        if let Ok(ser) = serde_json::to_vec(&durable) {
            self.substrate
                .put_overwrite(&manifest_latest_key(&self.config.dataset), &ser)?;
            let _ = self.substrate.put_if_absent(
                &manifest_version_key(&self.config.dataset, durable.version),
                &ser,
            );
        }

        self.metrics
            .record_merge(sources.len() as u64, bytes.len() as u64);
        Ok(())
    }

    /// Read a whole part's bytes — local hot tier if resident, else the durable
    /// substrate (primary replica).
    fn read_whole_part(&self, part: &PartMeta) -> Result<Vec<u8>> {
        if let Some(local_path) = &part.local_path {
            fs::read(local_path).map_err(|err| EhdbError::Storage(err.to_string()))
        } else if let Some(replica) = part.primary_replica() {
            self.substrate.get_all(replica)
        } else {
            Err(EhdbError::InvalidState(format!(
                "part {} has no location",
                part.part_id
            )))
        }
    }

    /// **The D1 read path** (RFC §2.5 worked example + L0.2 index-first pruning):
    /// events for `execution_id` with `global_sequence > after_seq`, in sequence
    /// order.
    ///
    /// 1. Manifest prune (MinMax + partition skip): only parts of
    ///    `shard_for(execution_id)` whose range can hold a record after
    ///    `after_seq`. Non-matching parts are skipped with **zero I/O**.
    /// 2. **L0.2 bloom prune (index-first):** among the surviving parts, skip any
    ///    whose per-part `execution_id` bloom says the execution is definitely
    ///    absent — the primary prune when everything is in one partition.
    /// 3. Sparse index binary search: locate the granule containing `after_seq+1`.
    /// 4. **L0.2 granule bloom narrowing:** trim the ranged block to the
    ///    contiguous granule span whose blooms admit the execution.
    /// 5. Ranged read of only that block, from the local hot tier if resident,
    ///    else a ranged GET against the durable substrate; decode + filter + the
    ///    active hot buffer.
    pub fn read_execution_after(
        &self,
        execution_id: &str,
        after_seq: u64,
    ) -> Result<Vec<EventRecord>> {
        let shard = shard_for_execution(execution_id, self.config.shard_count);
        let mut out = Vec::new();

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
                .filter(|p| p.execution_maybe_present(execution_id))
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
            let (block_start, block_end) = match part.granule_span_for(execution_id, start_granule)
            {
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
            // Prefer the local hot tier (no substrate I/O); fall back to a
            // ranged GET against the durable substrate (the primary replica).
            let block = if let Some(local_path) = &part.local_path {
                read_local_range(local_path, block_start, len)?
            } else if let Some(replica) = part.primary_replica() {
                self.substrate.get_range(replica, block_start, len)?
            } else {
                return Err(EhdbError::InvalidState(format!(
                    "part {} has neither a local_path nor a durable replica",
                    part.part_id
                )));
            };
            for frame in iter_frames_from(&block, 0)? {
                let rec: EventRecord = serde_json::from_slice(frame.body).map_err(|err| {
                    EhdbError::Storage(format!("decode l0 record in part {}: {err}", part.part_id))
                })?;
                if rec.global_sequence > after_seq && rec.execution_id == execution_id {
                    out.push(rec);
                }
            }
        }

        // The active (unsealed) hot buffer for this shard.
        if let Some(writer) = self.writers.get(&shard) {
            for rec in writer.pending_records() {
                if rec.global_sequence > after_seq && rec.execution_id == execution_id {
                    out.push(rec.clone());
                }
            }
        }

        out.sort_by_key(|r| r.global_sequence);
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
    pub fn replay_all(&self) -> Result<Vec<EventRecord>> {
        let mut out = Vec::new();
        let parts: Vec<_> = {
            let m = self.manifest.lock().unwrap();
            let mut ps: Vec<_> = m.parts.to_vec();
            ps.sort_by_key(|p| (p.partition, p.min_sequence));
            ps
        };
        for part in &parts {
            let bytes = if let Some(local_path) = &part.local_path {
                fs::read(local_path).map_err(|err| EhdbError::Storage(err.to_string()))?
            } else if let Some(replica) = part.primary_replica() {
                self.substrate.get_all(replica)?
            } else {
                return Err(EhdbError::InvalidState(format!(
                    "replay_all: part {} has no location",
                    part.part_id
                )));
            };
            for frame in iter_frames_from(&bytes, 0)? {
                let rec: EventRecord = serde_json::from_slice(frame.body)
                    .map_err(|err| EhdbError::Storage(format!("decode l0 record: {err}")))?;
                out.push(rec);
            }
        }
        for writer in self.writers.values() {
            out.extend(writer.pending_records().iter().cloned());
        }
        out.sort_by_key(|r| r.global_sequence);
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

impl Drop for L0EventLogEngine {
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

fn load_durable_manifest(
    substrate: &dyn DurableSubstrate,
    dataset: &str,
) -> Result<Option<Manifest>> {
    let key = manifest_latest_key(dataset);
    if !substrate.exists(&key)? {
        return Ok(None);
    }
    let bytes = substrate.get_all(&key)?;
    let manifest: Manifest = serde_json::from_slice(&bytes)
        .map_err(|err| EhdbError::Storage(format!("decode durable manifest: {err}")))?;
    Ok(Some(manifest))
}
