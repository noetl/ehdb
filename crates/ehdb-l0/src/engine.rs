//! [`L0EventLogEngine`] — the hot-local / durable-async composite (RFC §2.3) for
//! dataset D1, tying [`crate::part`] (the write engine), [`crate::catalog`] (the
//! meta-catalog), and [`crate::object_store`] (the durability tier) together.
//!
//! ```text
//!   append(exec, txn, payload)
//!     └─ hot: route to shard_for(exec) → PartWriter.append (fsync-per-append, posture A)
//!           └─ on seal trigger → immutable part + manifest row (local-only) + enqueue upload
//!   [background uploader thread]
//!     └─ read sealed part bytes → object_store.put_if_absent → set object_uri → rewrite durable manifest
//!   read_execution_after(exec, seq)
//!     └─ manifest.prune(shard, seq)  ← MinMax skip: non-matching parts = zero I/O
//!        └─ per part: sparse_index.locate(seq) → ranged read [mark, end)  ← only the needed block
//!           └─ prefer local_path (hot); else object_store.get_range (durable)
//!        └─ + the active (unsealed) hot buffer
//!   cold_load(object_store)  ← fresh node, empty local dir
//!     └─ read durable manifest → serve reads entirely from the object store
//!        (reproduces the exact record set + global sequence — the fungible-writer property, RFC §2.7)
//! ```
//!
//! The append path **never** calls the object store; only the background
//! uploader does. That is the §2.3 claim the L0.1 proof exercises with an
//! injected object-store latency: appends do not regress when uploads are slow.

use std::collections::HashMap;
use std::fs::{self, File};
use std::io::{Read, Seek, SeekFrom};
use std::path::PathBuf;
use std::sync::mpsc::{self, Sender};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::JoinHandle;
use std::time::Instant;

use ehdb_core::{EhdbError, Result};

use crate::catalog::Manifest;
use crate::dataset::{shard_for_execution, EventRecord, DATASET_D1_EVENT_LOG, DEFAULT_SHARD_COUNT};
use crate::frame::iter_frames_from;
use crate::metrics::L0Metrics;
use crate::object_store::L0ObjectStore;
use crate::part::{object_key_for, FlushPolicy, PartWriter, SealedPart};

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
}

impl L0Config {
    /// D1 defaults rooted at `local_root`: single owner, posture A, 8 MiB /
    /// 1024-record seal, 16-record granules.
    pub fn d1(local_root: impl Into<PathBuf>) -> Self {
        Self {
            dataset: DATASET_D1_EVENT_LOG.to_string(),
            local_root: local_root.into(),
            shard_count: DEFAULT_SHARD_COUNT,
            granule_size: DEFAULT_GRANULE_SIZE,
            seal_max_bytes: DEFAULT_SEAL_MAX_BYTES,
            seal_max_records: DEFAULT_SEAL_MAX_RECORDS,
            flush: FlushPolicy::EveryAppend,
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
    /// Set the record-count seal threshold.
    pub fn with_seal_max_records(mut self, seal_max_records: u64) -> Self {
        self.seal_max_records = seal_max_records;
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
    object_key: String,
    local_path: String,
    part_id: String,
    sealed_at: Instant,
}

/// The L0 event-log engine (single writer per partition — the caller is the
/// shard owner, matching #254's single-writer assumption).
pub struct L0EventLogEngine {
    config: L0Config,
    object_store: Arc<dyn L0ObjectStore>,
    metrics: Arc<L0Metrics>,
    /// In-RAM catalog — local-only + durable parts. Shared with the uploader
    /// thread, which sets `object_uri` on upload.
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
    /// Open a writer engine over `object_store`, reusing any manifest already in
    /// the object store (so a restart of the owner resumes its catalog). The
    /// local hot tier is `config.local_root`.
    pub fn open(config: L0Config, object_store: Arc<dyn L0ObjectStore>) -> Result<Self> {
        let metrics = L0Metrics::new();
        Self::open_with_metrics(config, object_store, metrics)
    }

    /// Open sharing an existing [`L0Metrics`] handle (so a caller can read
    /// counters).
    pub fn open_with_metrics(
        config: L0Config,
        object_store: Arc<dyn L0ObjectStore>,
        metrics: Arc<L0Metrics>,
    ) -> Result<Self> {
        fs::create_dir_all(&config.local_root)
            .map_err(|err| EhdbError::Storage(err.to_string()))?;
        // Resume the durable catalog if present (owner restart), else start empty.
        let manifest = load_durable_manifest(&*object_store, &config.dataset)?
            .unwrap_or_else(|| Manifest::empty(&config.dataset));
        let global_sequence = manifest.max_sequence();
        let mut engine = Self::assemble(config, object_store, metrics, manifest, global_sequence);
        engine.start_uploader();
        Ok(engine)
    }

    /// **Cold-load** a fresh node (empty local dir) from the object store: read
    /// the durable manifest and serve reads entirely from object-store parts.
    /// Reproduces the exact record set + global sequence of the origin — the
    /// fungible-writer property that retires the per-shard-Raft "T-RF" plan
    /// (RFC §2.7). The returned engine can also *resume writing* (new parts
    /// continue from the recovered `global_sequence`).
    pub fn cold_load(config: L0Config, object_store: Arc<dyn L0ObjectStore>) -> Result<Self> {
        let metrics = L0Metrics::new();
        Self::cold_load_with_metrics(config, object_store, metrics)
    }

    /// Cold-load sharing a metrics handle.
    pub fn cold_load_with_metrics(
        config: L0Config,
        object_store: Arc<dyn L0ObjectStore>,
        metrics: Arc<L0Metrics>,
    ) -> Result<Self> {
        fs::create_dir_all(&config.local_root)
            .map_err(|err| EhdbError::Storage(err.to_string()))?;
        let manifest =
            load_durable_manifest(&*object_store, &config.dataset)?.ok_or_else(|| {
                EhdbError::InvalidState(format!(
                    "cold-load: no durable manifest for dataset {}",
                    config.dataset
                ))
            })?;
        let global_sequence = manifest.max_sequence();
        metrics.incr_cold_loads();
        let mut engine = Self::assemble(config, object_store, metrics, manifest, global_sequence);
        engine.start_uploader();
        Ok(engine)
    }

    fn assemble(
        config: L0Config,
        object_store: Arc<dyn L0ObjectStore>,
        metrics: Arc<L0Metrics>,
        manifest: Manifest,
        global_sequence: u64,
    ) -> Self {
        Self {
            config,
            object_store,
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
        let object_store = Arc::clone(&self.object_store);
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
                        object_store.put_if_absent(&job.object_key, &bytes)?;
                        Ok(bytes.len() as u64)
                    })();

                    let bytes_len = match upload_result {
                        Ok(n) => n,
                        Err(_) => {
                            // On upload failure the part stays local-only (its
                            // manifest row keeps object_uri = None); a later
                            // retry slice re-drives it. Still decrement so a
                            // waiter isn't wedged.
                            decrement(&outstanding);
                            continue;
                        }
                    };

                    // Set object_uri on the part and snapshot the durable view
                    // — under the lock, but the object-store write happens
                    // OUTSIDE the lock so a slow store never blocks appends/reads.
                    let durable = {
                        let mut m = manifest.lock().unwrap();
                        if let Some(p) = m.parts.iter_mut().find(|p| p.part_id == job.part_id) {
                            p.object_uri = Some(job.object_key.clone());
                        }
                        m.version += 1;
                        m.durable_view()
                    };
                    if let Ok(ser) = serde_json::to_vec(&durable) {
                        let _ = object_store.put_overwrite(&manifest_latest_key(&dataset), &ser);
                        let _ = object_store
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
    /// Never touches the object store. Returns the assigned global sequence.
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
        let object_key = object_key_for(
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
                object_key,
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
    /// all outstanding parts to the object store (a graceful handoff / durability
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

    /// **The D1 read path** (RFC §2.5 worked example): events for `execution_id`
    /// with `global_sequence > after_seq`, in sequence order.
    ///
    /// 1. Manifest prune (MinMax skip): only parts of `shard_for(execution_id)`
    ///    whose range can hold a record after `after_seq`. Non-matching parts are
    ///    skipped with **zero I/O**.
    /// 2. Sparse index binary search: locate the granule containing `after_seq+1`.
    /// 3. Ranged read from that granule's mark to the part's end — **only the
    ///    needed block**, from the local hot tier if resident, else a ranged GET
    ///    against the object store.
    /// 4. Decode + filter (`> after_seq`, matching execution) + the active hot
    ///    buffer.
    pub fn read_execution_after(
        &self,
        execution_id: &str,
        after_seq: u64,
    ) -> Result<Vec<EventRecord>> {
        let shard = shard_for_execution(execution_id, self.config.shard_count);
        let mut out = Vec::new();

        let (hits_meta, pruned_count) = {
            let m = self.manifest.lock().unwrap();
            let total_parts = m.parts.len();
            // Clone the matched PartMeta so we drop the manifest lock before any
            // (possibly slow) object-store read.
            let hits: Vec<_> = m.prune(shard, after_seq).into_iter().cloned().collect();
            // Every non-matching part — a different partition, or a range wholly
            // at/below the cursor — is skipped here with ZERO part I/O (pointer
            // catalog only). This is the full RFC §2.5 manifest prune.
            let pruned = total_parts - hits.len();
            (hits, pruned)
        };

        for part in &hits_meta {
            let start = part.sparse_index.locate(after_seq + 1);
            let len = part.byte_size.saturating_sub(start);
            if len == 0 {
                continue;
            }
            // Prefer the local hot tier (no object-store I/O); fall back to a
            // ranged GET against the durable tier.
            let block = if let Some(local_path) = &part.local_path {
                read_local_range(local_path, start, len)?
            } else if let Some(uri) = &part.object_uri {
                self.object_store.get_range(uri, start, len)?
            } else {
                return Err(EhdbError::InvalidState(format!(
                    "part {} has neither local_path nor object_uri",
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
        self.metrics
            .record_read(pruned_count as u64, hits_meta.len() as u64);
        Ok(out)
    }

    /// Reproduce the **entire** record set across all partitions in global-
    /// sequence order — the cold-load correctness helper. Reads each part fully
    /// (local if resident, else the object store) plus the active hot buffers.
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
            } else if let Some(uri) = &part.object_uri {
                self.object_store.get_all(uri)?
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
    object_store: &dyn L0ObjectStore,
    dataset: &str,
) -> Result<Option<Manifest>> {
    let key = manifest_latest_key(dataset);
    if !object_store.exists(&key)? {
        return Ok(None);
    }
    let bytes = object_store.get_all(&key)?;
    let manifest: Manifest = serde_json::from_slice(&bytes)
        .map_err(|err| EhdbError::Storage(format!("decode durable manifest: {err}")))?;
    Ok(Some(manifest))
}
