//! The VictoriaMetrics/VictoriaLogs-style **write engine** for one partition:
//! an in-memory buffer + an active local part → sealed **immutable part** (RFC
//! §2.1, §2.3).
//!
//! - Appends land in an active local part file (the hot tier), framed with the
//!   #254 codec ([`crate::frame`]).
//! - Under **posture A** ([`FlushPolicy::EveryAppend`], the D1 default) each
//!   append `fsync`s before returning — the local part is durable before ack,
//!   reusing #254's fsync-per-append strength.
//! - Under **posture B** ([`FlushPolicy::Buffered`]) appends batch and `fsync` on
//!   a threshold (and always on seal) — VM's larger-crash-window / higher-
//!   throughput posture, for derived tiers only (RFC §2.3 / §6.1).
//! - On the seal trigger (size or record count) the active part becomes an
//!   immutable `.eslog` file and a [`SealedPart`] carrying its [`PartMeta`]
//!   (partition, min/max sort key, sparse index) — ready for the manifest + the
//!   async upload.
//!
//! The active (unsealed) part's records are also held in memory so a read sees
//! the hot buffer regardless of flush posture — read-your-writes for the tail.

use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use ehdb_core::{EhdbError, Result};

use crate::bloom::Bloom;
use crate::catalog::{GranuleMark, PartMeta, SparseIndex};
use crate::dataset::Dataset;
use crate::frame::{encode_frame, iter_frames_from};

/// Durability-window posture (RFC §2.3). D1's event log uses [`Self::EveryAppend`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FlushPolicy {
    /// Posture A — `fsync` after every append. The local part is durable before
    /// the append returns. The recommended (and D1 default) posture for the
    /// source-of-truth event log.
    EveryAppend,
    /// Posture B — batch appends, `fsync` after `fsync_every` records (and always
    /// on seal). Faster, larger crash window; derived/metrics tiers only.
    Buffered { fsync_every: u32 },
    /// Posture A, **group-committed** — an append never `fsync`s on its own; the
    /// caller calls [`PartWriter::sync`] to close the durability window over a
    /// whole batch. The crash window is *not* widened: the caller is required to
    /// `sync` before it acknowledges any record in the batch, so a record is
    /// still durable before its writer confirms it. What changes is the *cost* —
    /// N records that arrive together share one `sync_data()` instead of paying
    /// N (noetl/ai-meta#205).
    ///
    /// Only for a writer that owns its commit points. [`crate::FeedWriter`] does
    /// (it drives `sync` on every append path); a bare `L0Engine` caller does
    /// not — use [`Self::EveryAppend`] there.
    CallerDriven,
}

/// A sealed, immutable part ready for the manifest + the async uploader.
/// Generic over the [`Dataset`] whose records it holds.
pub struct SealedPart<D: Dataset> {
    /// The catalog row for this part (`local_path` set, `replicas` still empty
    /// until the upload lands).
    pub meta: PartMeta,
    /// The exact records this part holds, in sort-key order — returned so the
    /// engine can serve reads of a just-sealed part without re-reading disk, and
    /// so the proof can assert the sealed content.
    pub records: Vec<D::Record>,
}

/// The active-part writer for one partition of one [`Dataset`]. One per shard in
/// the engine.
pub struct PartWriter<D: Dataset> {
    dataset: String,
    partition: u32,
    /// Directory holding this partition's part files.
    part_dir: PathBuf,
    /// Sealed-part id counter within this partition (for the active file name;
    /// the durable part id is derived from the sort-key range on seal).
    next_local_id: u64,
    granule_size: u32,
    seal_max_bytes: u64,
    seal_max_records: u64,
    /// **Age-based seal trigger** (noetl/ehdb#329) — seal an active part once
    /// its oldest record is this old, regardless of size or count.
    ///
    /// `None` (the default) is today's behavior: size and count only, which
    /// makes the durability window **unbounded in time** on a shard that goes
    /// quiet. Off by default so enabling it is a deliberate, reversible act.
    seal_max_age: Option<Duration>,
    flush: FlushPolicy,

    // --- active part state ---
    active_path: PathBuf,
    file: Option<File>,
    records: Vec<D::Record>,
    marks: Vec<GranuleMark>,
    min_sequence: u64,
    max_sequence: u64,
    byte_len: u64,
    record_count: u64,
    /// When the **first** record of the current active part was appended. The
    /// age trigger measures from here, so it bounds the wait of the *oldest*
    /// record rather than the newest.
    first_append_at: Option<Instant>,
    unflushed_since_fsync: u32,
}

impl<D: Dataset> PartWriter<D> {
    /// Open a writer for `partition` under `part_dir`
    /// (`.../parts/<dataset>/shard-<partition>/`).
    pub fn open(
        dataset: impl Into<String>,
        partition: u32,
        part_dir: impl Into<PathBuf>,
        granule_size: u32,
        seal_max_bytes: u64,
        seal_max_records: u64,
        flush: FlushPolicy,
    ) -> Result<Self> {
        let dataset = dataset.into();
        let part_dir = part_dir.into();
        fs::create_dir_all(&part_dir).map_err(|err| EhdbError::Storage(err.to_string()))?;
        let mut w = Self {
            dataset,
            partition,
            part_dir,
            next_local_id: 0,
            granule_size: granule_size.max(1),
            seal_max_bytes,
            seal_max_records,
            seal_max_age: None,
            flush,
            active_path: PathBuf::new(),
            file: None,
            records: Vec::new(),
            marks: Vec::new(),
            min_sequence: 0,
            max_sequence: 0,
            byte_len: 0,
            first_append_at: None,
            record_count: 0,
            unflushed_since_fsync: 0,
        };
        w.open_active()?;
        Ok(w)
    }

    fn open_active(&mut self) -> Result<()> {
        self.active_path = self
            .part_dir
            .join(format!("part-{:06}.active", self.next_local_id));
        self.records.clear();
        self.marks.clear();
        self.min_sequence = 0;
        self.max_sequence = 0;
        self.byte_len = 0;
        self.record_count = 0;
        self.first_append_at = None;
        self.unflushed_since_fsync = 0;

        // noetl/ai-meta#209 defect 2 — recover, do not truncate.
        //
        // This used to open with `.truncate(true)`, which made a hard kill
        // unrecoverable *by construction*: the engine resumes its catalog from
        // the durable manifest, which only lists SEALED parts, so records in the
        // active part were already invisible after a restart — and truncating
        // then destroyed the one copy of them that existed. Up to
        // `seal_max_records` (1024) records per shard, every one of them
        // `fsync`ed and acked to a publisher, gone on SIGKILL / OOM / node loss.
        // A durable ack a restart can lose is a contract violation, so the
        // active part is now replayed instead.
        //
        // The frame codec already carries the recovery contract this needs
        // (`iter_frames_from`, byte-identical to the #254 segment format): it
        // stops at a torn tail — a half-written header or body at EOF, which is
        // exactly what a crash mid-append leaves — and returns the intact
        // prefix. Bit-rot (a *complete* frame with bad magic or a CRC mismatch)
        // surfaces as an error and is never silently repaired.
        let recovered = self.recover_active()?;
        let mut opts = OpenOptions::new();
        opts.create(true).write(true);
        if recovered {
            // Append, so the recovered prefix survives and the next append lands
            // after it.
            opts.append(true);
        } else {
            opts.truncate(true);
        }
        let file = opts
            .open(&self.active_path)
            .map_err(|err| EhdbError::Storage(err.to_string()))?;
        self.file = Some(file);
        Ok(())
    }

    /// Replay an active part left behind by a crash, rebuilding the in-memory
    /// state an orderly open would have had. Returns whether anything was
    /// recovered.
    ///
    /// The file is truncated to the end of the last intact frame. That is the
    /// only mutation, and it is required: appending after a torn tail would
    /// leave a permanently unparseable byte range in the middle of the part,
    /// turning a recoverable crash into corruption on the *next* restart.
    fn recover_active(&mut self) -> Result<bool> {
        let bytes = match fs::read(&self.active_path) {
            Ok(b) => b,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(false),
            Err(err) => return Err(EhdbError::Storage(err.to_string())),
        };
        if bytes.is_empty() {
            return Ok(false);
        }
        // Propagates on bit-rot — see `read_frame_at`.
        let frames = iter_frames_from(&bytes, 0)?;
        if frames.is_empty() {
            // Nothing intact: the whole file is a torn tail (a crash between
            // create and the first complete frame). Start clean.
            return Ok(false);
        }
        let mut intact_len = 0u64;
        for frame in &frames {
            let record: D::Record = serde_json::from_slice(frame.body)
                .map_err(|err| EhdbError::Storage(format!("decode recovered l0 record: {err}")))?;
            let sort_key = D::sort_key(&record);
            if self.record_count % self.granule_size as u64 == 0 {
                self.marks.push(GranuleMark {
                    first_sequence: sort_key,
                    byte_offset: frame.offset,
                    record_count: 0,
                });
            }
            if self.record_count == 0 {
                self.min_sequence = sort_key;
            }
            self.max_sequence = sort_key;
            self.record_count += 1;
            if let Some(last) = self.marks.last_mut() {
                last.record_count += 1;
            }
            self.records.push(record);
            intact_len = frame.offset + frame.frame_len;
        }
        self.byte_len = intact_len;
        if self.record_count > 0 && self.first_append_at.is_none() {
            // Recovered from a crashed active part. The records' true append
            // instants are not knowable, so the window restarts now — which
            // understates their age rather than inventing one.
            self.first_append_at = Some(Instant::now());
        }
        // Drop a torn tail so the part stays parseable from here on.
        if intact_len < bytes.len() as u64 {
            let file = OpenOptions::new()
                .write(true)
                .open(&self.active_path)
                .map_err(|err| EhdbError::Storage(err.to_string()))?;
            file.set_len(intact_len)
                .map_err(|err| EhdbError::Storage(err.to_string()))?;
            file.sync_all()
                .map_err(|err| EhdbError::Storage(err.to_string()))?;
        }
        Ok(true)
    }

    /// Highest sort key held in the active (unsealed) part, or `None` when it is
    /// empty. The engine uses this after a recovering open to lift its global
    /// sequence and shard tail above the recovered records — without it, the
    /// next writer-assigned key could land at or below the recovered tail, which
    /// is the silent-drop class of noetl/ai-meta#203.
    pub fn max_sequence(&self) -> Option<u64> {
        (self.record_count > 0).then_some(self.max_sequence)
    }

    /// Append one record to the active part (hot tier). Never touches the object
    /// store — durability rides the async uploader on seal. Returns the byte
    /// offset the frame was written at (its mark).
    pub fn append(&mut self, record: D::Record) -> Result<u64> {
        let sort_key = D::sort_key(&record);
        let body = serde_json::to_vec(&record)
            .map_err(|err| EhdbError::Storage(format!("encode l0 record: {err}")))?;
        let frame = encode_frame(&body)?;
        let mark_offset = self.byte_len;

        // Start-of-granule mark: first record of each granule.
        if self.record_count % self.granule_size as u64 == 0 {
            self.marks.push(GranuleMark {
                first_sequence: sort_key,
                byte_offset: mark_offset,
                record_count: 0,
            });
        }

        let file = self
            .file
            .as_mut()
            .ok_or_else(|| EhdbError::InvalidState("l0 part writer has no active file".into()))?;
        file.write_all(&frame)
            .map_err(|err| EhdbError::Storage(err.to_string()))?;

        match self.flush {
            FlushPolicy::EveryAppend => {
                file.sync_data()
                    .map_err(|err| EhdbError::Storage(err.to_string()))?;
            }
            FlushPolicy::Buffered { fsync_every } => {
                self.unflushed_since_fsync += 1;
                if self.unflushed_since_fsync >= fsync_every.max(1) {
                    file.sync_data()
                        .map_err(|err| EhdbError::Storage(err.to_string()))?;
                    self.unflushed_since_fsync = 0;
                }
            }
            // The caller closes the durability window (group commit) — see
            // `sync`. Track the debt so `sync` can skip a no-op fsync.
            FlushPolicy::CallerDriven => {
                self.unflushed_since_fsync += 1;
            }
        }

        if self.record_count == 0 {
            self.min_sequence = sort_key;
        }
        self.max_sequence = sort_key;
        self.byte_len += frame.len() as u64;
        if self.record_count == 0 {
            self.first_append_at = Some(Instant::now());
        }
        self.record_count += 1;
        // Grow the current granule's count.
        if let Some(last) = self.marks.last_mut() {
            last.record_count += 1;
        }
        self.records.push(record);
        Ok(mark_offset)
    }

    /// Switch this writer's flush posture (see [`crate::L0Engine::set_flush_policy`]).
    /// Any fsync debt already accrued carries over to the next [`sync`](Self::sync)
    /// or [`seal`](Self::seal), so no append is left unflushed by the switch.
    pub fn set_flush_policy(&mut self, policy: FlushPolicy) {
        self.flush = policy;
    }

    /// **Take the commit handle** for everything appended since the last take —
    /// the group-commit seam ([`FlushPolicy::CallerDriven`]). Returns a
    /// *duplicated* file descriptor (`try_clone`) onto the active part, and
    /// clears the outstanding fsync debt; `None` when nothing is owed.
    ///
    /// The duplicate is the point: `sync_data()` on it flushes the same file, so
    /// the caller can run the (millisecond-scale, blocking) `fsync` **after**
    /// releasing the engine lock. Holding that lock across the `fsync` is what
    /// stalled every reader — a claiming consumer needs the same lock to poll its
    /// feed, so an in-lock `fsync` blocked the whole consuming side for its
    /// duration (noetl/ai-meta#205).
    ///
    /// Safe against a concurrent take: an append that lands between one caller's
    /// take and its `fsync` may or may not be covered by it, but it raises the
    /// debt again and so is covered by its own commit. `fsync` is monotone —
    /// covering more than asked is never wrong. Safe against a concurrent
    /// [`seal`](Self::seal) too: seal `fsync`s before it renames, so data reached
    /// through a handle taken earlier is already durable.
    pub fn take_sync_handle(&mut self) -> Result<Option<File>> {
        if self.unflushed_since_fsync == 0 {
            return Ok(None);
        }
        let Some(file) = self.file.as_ref() else {
            return Ok(None);
        };
        let dup = file
            .try_clone()
            .map_err(|err| EhdbError::Storage(err.to_string()))?;
        self.unflushed_since_fsync = 0;
        Ok(Some(dup))
    }

    /// Whether the active part has hit a seal trigger (size, record count, or
    /// — when configured — **age**).
    ///
    /// ⚠ Size and count alone leave the durability window **unbounded in time**:
    /// a shard that appends a few records and goes quiet never seals, never
    /// uploads, and never replicates. The age trigger is what bounds it, and it
    /// is off unless `seal_max_age` is set (noetl/ehdb#329).
    pub fn should_seal(&self) -> bool {
        self.record_count > 0
            && (self.byte_len >= self.seal_max_bytes
                || self.record_count >= self.seal_max_records
                || self.aged_out())
    }

    /// Whether the age trigger alone would seal this part.
    ///
    /// ⚠ Separate from [`Self::should_seal`] on purpose: an idle shard takes no
    /// appends, so nothing calls `should_seal` for it. Something must drive the
    /// age check on a timer — see `L0Engine::seal_aged_parts`. A trigger that is
    /// only consulted on append cannot fire on the shard it exists to protect.
    pub fn aged_out(&self) -> bool {
        match (self.seal_max_age, self.first_append_at) {
            (Some(limit), Some(since)) => self.record_count > 0 && since.elapsed() >= limit,
            _ => false,
        }
    }

    /// Configure the age trigger. `None` restores today's size/count-only
    /// behavior.
    pub fn set_seal_max_age(&mut self, age: Option<Duration>) {
        self.seal_max_age = age;
    }

    /// How long the oldest record in the active part has been waiting, if any.
    pub fn active_age(&self) -> Option<Duration> {
        self.first_append_at.map(|t| t.elapsed())
    }

    /// Whether the active part holds any un-sealed records.
    pub fn has_pending(&self) -> bool {
        self.record_count > 0
    }

    /// The active (unsealed) records, for serving the hot tail.
    pub fn pending_records(&self) -> &[D::Record] {
        &self.records
    }

    /// The active part's local file path (for durability / recovery inspection).
    pub fn active_path(&self) -> &Path {
        &self.active_path
    }

    /// The dataset this writer's parts belong to (for object-key computation).
    pub fn dataset(&self) -> &str {
        &self.dataset
    }

    /// This writer's partition (shard) id.
    pub fn partition(&self) -> u32 {
        self.partition
    }

    /// Seal the active part into an immutable `.eslog` file and return its
    /// [`SealedPart`]. `fsync`s, renames the active file to its durable name, and
    /// opens a fresh active part. Returns `None` if there is nothing to seal.
    pub fn seal(&mut self) -> Result<Option<SealedPart<D>>> {
        if self.record_count == 0 {
            return Ok(None);
        }
        // Ensure everything is durable before we treat the part as immutable.
        if let Some(file) = self.file.as_mut() {
            file.sync_data()
                .map_err(|err| EhdbError::Storage(err.to_string()))?;
        }
        self.file = None; // close the handle before rename

        let part_id = format!(
            "shard-{}-seq-{:020}-{:020}",
            self.partition, self.min_sequence, self.max_sequence
        );
        let final_name = format!("{part_id}.eslog");
        let final_path = self.part_dir.join(&final_name);
        fs::rename(&self.active_path, &final_path)
            .map_err(|err| EhdbError::Storage(err.to_string()))?;

        let marks = std::mem::take(&mut self.marks);
        let records = std::mem::take(&mut self.records);

        // L0.2 fixed inverted index: build the per-part + per-granule blooms over
        // the dataset's index dimension (D1: `execution_id`) from the sealed
        // records. A record's granule index is `record_position / granule_size`.
        let (execution_bloom, granule_blooms) =
            build_index_blooms::<D>(&records, &marks, self.granule_size);

        let meta = PartMeta {
            part_id: part_id.clone(),
            partition: self.partition,
            min_sequence: self.min_sequence,
            max_sequence: self.max_sequence,
            record_count: self.record_count,
            byte_size: self.byte_len,
            replicas: Vec::new(),
            local_path: Some(final_path.to_string_lossy().to_string()),
            sparse_index: SparseIndex {
                granule_size: self.granule_size,
                marks,
            },
            execution_bloom: Some(execution_bloom),
            granule_blooms,
        };

        self.next_local_id += 1;
        self.open_active()?;

        // The part is local-only (`replicas` empty) until the async uploader
        // ships it; the destination key is deterministic
        // ([`substrate_key_for`]), so the engine/uploader/cold-load all agree
        // without threading it through state.
        Ok(Some(SealedPart { meta, records }))
    }
}

/// The deterministic object-store key for a part: recomputed anywhere from
/// `(dataset, partition, part_id)`, so the writer, the uploader, and a cold-load
/// all agree without threading the key through state.
pub fn substrate_key_for(dataset: &str, partition: u32, part_id: &str) -> String {
    format!("parts/{dataset}/shard-{partition}/{part_id}.eslog")
}

/// Build the L0.2 fixed inverted index for a sealed part: one part-level bloom
/// over every record's index key (D1: `execution_id`), and one bloom per granule
/// (parallel to `marks`). Record `i` belongs to granule `i / granule_size`.
fn build_index_blooms<D: Dataset>(
    records: &[D::Record],
    marks: &[GranuleMark],
    granule_size: u32,
) -> (Bloom, Vec<Bloom>) {
    let mut part_bloom = Bloom::for_expected(records.len());
    let mut granule_blooms: Vec<Bloom> = marks
        .iter()
        .map(|m| Bloom::for_expected(m.record_count as usize))
        .collect();
    let gsize = granule_size.max(1) as usize;
    for (i, record) in records.iter().enumerate() {
        let key = D::index_key(record);
        part_bloom.insert(key);
        let g = i / gsize;
        if let Some(bloom) = granule_blooms.get_mut(g) {
            bloom.insert(key);
        }
    }
    (part_bloom, granule_blooms)
}

/// Build one immutable **merged** part (the L0.3 compaction output) from an
/// already-sorted (ascending `global_sequence`) record set, writing it into
/// `dest_dir` as a `<part_id>.eslog` file. Produces the same immutable-part
/// shape a fresh seal would — CRC-framed records, sparse index, per-part +
/// per-granule blooms — so a merged part is indistinguishable from a sealed one
/// to the read path. The records preserve their original global sequences, so
/// the merged part covers the contiguous sort-key range of its inputs.
///
/// `records` must be non-empty and ascending by `global_sequence` (the caller —
/// the merge engine — sorts the source parts' records before calling).
pub fn build_merged_part<D: Dataset>(
    partition: u32,
    granule_size: u32,
    dest_dir: &Path,
    records: &[D::Record],
) -> Result<SealedPart<D>> {
    if records.is_empty() {
        return Err(EhdbError::InvalidState(
            "build_merged_part: empty record set".into(),
        ));
    }
    let gsize = granule_size.max(1);
    fs::create_dir_all(dest_dir).map_err(|err| EhdbError::Storage(err.to_string()))?;

    let min_sequence = D::sort_key(&records[0]);
    let max_sequence = D::sort_key(&records[records.len() - 1]);
    let part_id = format!("shard-{partition}-seq-{min_sequence:020}-{max_sequence:020}");
    let tmp_path = dest_dir.join(format!("{part_id}.merge-tmp"));
    let final_path = dest_dir.join(format!("{part_id}.eslog"));

    let mut marks: Vec<GranuleMark> = Vec::new();
    let mut byte_len: u64 = 0;
    {
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&tmp_path)
            .map_err(|err| EhdbError::Storage(err.to_string()))?;
        for (i, record) in records.iter().enumerate() {
            let body = serde_json::to_vec(record)
                .map_err(|err| EhdbError::Storage(format!("encode l0 record: {err}")))?;
            let frame = encode_frame(&body)?;
            if i as u64 % gsize as u64 == 0 {
                marks.push(GranuleMark {
                    first_sequence: D::sort_key(record),
                    byte_offset: byte_len,
                    record_count: 0,
                });
            }
            file.write_all(&frame)
                .map_err(|err| EhdbError::Storage(err.to_string()))?;
            byte_len += frame.len() as u64;
            if let Some(last) = marks.last_mut() {
                last.record_count += 1;
            }
        }
        file.sync_data()
            .map_err(|err| EhdbError::Storage(err.to_string()))?;
    }
    fs::rename(&tmp_path, &final_path).map_err(|err| EhdbError::Storage(err.to_string()))?;

    let (execution_bloom, granule_blooms) = build_index_blooms::<D>(records, &marks, gsize);
    let meta = PartMeta {
        part_id,
        partition,
        min_sequence,
        max_sequence,
        record_count: records.len() as u64,
        byte_size: byte_len,
        replicas: Vec::new(),
        local_path: Some(final_path.to_string_lossy().to_string()),
        sparse_index: SparseIndex {
            granule_size: gsize,
            marks,
        },
        execution_bloom: Some(execution_bloom),
        granule_blooms,
    };
    Ok(SealedPart {
        meta,
        records: records.to_vec(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dataset::{D1EventLog, EventRecord};
    use std::sync::atomic::{AtomicU64, Ordering};

    /// A directory no other test — and no *previous run* — can be using.
    ///
    /// The old version keyed on a per-process counter plus the thread id, which
    /// repeat across test binaries and across runs. That was survivable only
    /// while `open_active` truncated: leftovers were wiped on open. Now that an
    /// existing active part is recovered, a reused directory makes a test read a
    /// previous run's records and fail with an inflated count. Pid + a
    /// monotonic-clock nanos reading makes the path unique per run.
    fn tmp() -> PathBuf {
        static N: AtomicU64 = AtomicU64::new(0);
        let n = N.fetch_add(1, Ordering::Relaxed);
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        std::env::temp_dir().join(format!(
            "ehdb-l0-part-{}-{n}-{nanos}-{:?}",
            std::process::id(),
            std::thread::current().id()
        ))
    }

    fn rec(seq: u64, exec: &str) -> EventRecord {
        EventRecord::new(seq, exec, format!("txn-{seq}"), format!("payload-{seq}"))
    }

    /// A writer for `part_dir` with the small-part settings the recovery tests
    /// share, so each test differs only in what it does to the file.
    fn writer(dir: &PathBuf) -> PartWriter<D1EventLog> {
        PartWriter::<D1EventLog>::open(
            "d1_event_log",
            0,
            dir,
            2,
            1 << 20,
            1024,
            FlushPolicy::EveryAppend,
        )
        .expect("open part writer")
    }

    /// noetl/ai-meta#209 defect 2 — the core contract: records `fsync`ed into an
    /// unsealed active part survive a process that never got to seal.
    ///
    /// Before this fix `open_active` opened with `.truncate(true)`, so reopening
    /// destroyed them — up to 1024 acked records per shard on any SIGKILL.
    #[test]
    fn unsealed_active_part_survives_a_crash() {
        let dir = tmp();
        {
            let mut w = writer(&dir);
            for seq in 1..=5 {
                w.append(rec(seq, "exec-a")).unwrap();
            }
            // No seal, no clean close — drop is what a SIGKILL leaves behind.
        }
        let w = writer(&dir);
        assert_eq!(
            w.pending_records().len(),
            5,
            "all 5 acked records recovered"
        );
        assert_eq!(w.max_sequence(), Some(5));
        assert_eq!(
            w.pending_records()
                .iter()
                .map(|r| r.global_sequence)
                .collect::<Vec<_>>(),
            vec![1, 2, 3, 4, 5],
            "recovered in order"
        );
    }

    /// Recovery must leave the writer *appendable*, not merely readable: the
    /// next append has to land after the recovered prefix, and a second crash
    /// has to recover the union of both.
    #[test]
    fn appends_after_recovery_extend_the_recovered_part() {
        let dir = tmp();
        {
            let mut w = writer(&dir);
            w.append(rec(1, "exec-a")).unwrap();
            w.append(rec(2, "exec-a")).unwrap();
        }
        {
            let mut w = writer(&dir);
            w.append(rec(3, "exec-a")).unwrap();
        }
        let w = writer(&dir);
        assert_eq!(
            w.pending_records()
                .iter()
                .map(|r| r.global_sequence)
                .collect::<Vec<_>>(),
            vec![1, 2, 3],
            "the post-recovery append survived the second crash too"
        );
    }

    /// A crash mid-`write_all` leaves a partial frame at EOF. Recovery keeps the
    /// intact prefix and drops the torn tail — the #254 contract the frame codec
    /// already implements.
    #[test]
    fn torn_tail_is_dropped_and_the_prefix_is_kept() {
        let dir = tmp();
        let path = {
            let mut w = writer(&dir);
            for seq in 1..=3 {
                w.append(rec(seq, "exec-a")).unwrap();
            }
            w.active_path().to_path_buf()
        };
        // Simulate the interrupted write: lop off part of the last frame.
        let bytes = fs::read(&path).unwrap();
        let torn = &bytes[..bytes.len() - 5];
        fs::write(&path, torn).unwrap();

        let mut w = writer(&dir);
        assert_eq!(w.pending_records().len(), 2, "intact prefix kept");
        assert_eq!(w.max_sequence(), Some(2));
        // And the file was truncated to the intact boundary, so appending after
        // recovery cannot bury an unparseable range mid-file.
        w.append(rec(4, "exec-a")).unwrap();
        let reread = writer(&dir);
        assert_eq!(
            reread
                .pending_records()
                .iter()
                .map(|r| r.global_sequence)
                .collect::<Vec<_>>(),
            vec![1, 2, 4],
            "the part is still fully parseable after appending over a torn tail"
        );
    }

    /// Bit-rot is not a torn tail. A *complete* frame whose CRC does not match
    /// must surface as an error, never be silently dropped or repaired.
    #[test]
    fn bit_rot_in_a_complete_frame_is_an_error() {
        let dir = tmp();
        let path = {
            let mut w = writer(&dir);
            w.append(rec(1, "exec-a")).unwrap();
            w.append(rec(2, "exec-a")).unwrap();
            w.active_path().to_path_buf()
        };
        let mut bytes = fs::read(&path).unwrap();
        // Corrupt a body byte of the FIRST frame, leaving its header intact.
        let body_start = crate::frame::FRAME_HEADER_LEN;
        bytes[body_start] ^= 0xFF;
        fs::write(&path, &bytes).unwrap();

        let err = PartWriter::<D1EventLog>::open(
            "d1_event_log",
            0,
            &dir,
            2,
            1 << 20,
            1024,
            FlushPolicy::EveryAppend,
        )
        .err()
        .expect("corrupt frame must not open silently");
        assert!(
            format!("{err}").contains("CRC"),
            "expected a CRC error, got: {err}"
        );
    }

    /// An empty or absent active part is the ordinary first-open case and must
    /// not be mistaken for recovery.
    #[test]
    fn absent_or_empty_active_part_opens_clean() {
        let dir = tmp();
        let w = writer(&dir);
        assert_eq!(w.pending_records().len(), 0);
        assert_eq!(w.max_sequence(), None);
        let path = w.active_path().to_path_buf();
        drop(w);
        // A zero-byte file (crashed between create and first append).
        fs::write(&path, b"").unwrap();
        let w = writer(&dir);
        assert_eq!(w.pending_records().len(), 0);
        assert_eq!(w.max_sequence(), None);
    }

    /// A clean seal leaves nothing to recover — the recovery path must not
    /// resurrect records that are already durable in a sealed part, which would
    /// double them.
    #[test]
    fn a_sealed_part_leaves_nothing_to_recover() {
        let dir = tmp();
        {
            let mut w = writer(&dir);
            w.append(rec(1, "exec-a")).unwrap();
            w.append(rec(2, "exec-a")).unwrap();
            let sealed = w.seal().unwrap().expect("sealed");
            assert_eq!(sealed.records.len(), 2);
        }
        let w = writer(&dir);
        assert_eq!(
            w.pending_records().len(),
            0,
            "sealed records must not be replayed as pending"
        );
    }

    #[test]
    fn seals_on_record_count_and_builds_sparse_index() {
        let dir = tmp();
        let mut w = PartWriter::<D1EventLog>::open(
            "d1_event_log",
            0,
            dir.join("parts/d1/shard-0"),
            4, // granule_size
            1 << 30,
            8, // seal at 8 records
            FlushPolicy::EveryAppend,
        )
        .unwrap();

        for seq in 1..=8 {
            w.append(rec(seq, "100")).unwrap();
        }
        assert!(w.should_seal());
        let sealed = w.seal().unwrap().expect("sealed a part");
        assert_eq!(sealed.meta.record_count, 8);
        assert_eq!(sealed.meta.min_sequence, 1);
        assert_eq!(sealed.meta.max_sequence, 8);
        assert_eq!(sealed.meta.partition, 0);
        // 8 records / granule 4 → 2 granule marks at seq 1 and seq 5.
        let marks = &sealed.meta.sparse_index.marks;
        assert_eq!(marks.len(), 2);
        assert_eq!(marks[0].first_sequence, 1);
        assert_eq!(marks[0].record_count, 4);
        assert_eq!(marks[1].first_sequence, 5);
        assert_eq!(marks[1].record_count, 4);
        // The sealed file decodes back to the same 8 records.
        let path = sealed.meta.local_path.as_ref().unwrap();
        let bytes = std::fs::read(path).unwrap();
        let frames = iter_frames_from(&bytes, 0).unwrap();
        assert_eq!(frames.len(), 8);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn sparse_index_mark_offsets_point_at_frame_starts() {
        let dir = tmp();
        let mut w = PartWriter::<D1EventLog>::open(
            "d1_event_log",
            2,
            dir.join("parts/d1/shard-2"),
            2, // granule of 2
            1 << 30,
            6,
            FlushPolicy::EveryAppend,
        )
        .unwrap();
        for seq in 10..=15 {
            w.append(rec(seq, "abc")).unwrap();
        }
        let sealed = w.seal().unwrap().unwrap();
        let bytes = std::fs::read(sealed.meta.local_path.as_ref().unwrap()).unwrap();
        // Every granule mark's byte_offset must land on a real frame whose first
        // record has the mark's first_sequence.
        for mark in &sealed.meta.sparse_index.marks {
            let frame = crate::frame::read_frame_at(&bytes, mark.byte_offset)
                .unwrap()
                .expect("mark points at a frame");
            let record: EventRecord = serde_json::from_slice(frame.body).unwrap();
            assert_eq!(record.global_sequence, mark.first_sequence);
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn buffered_posture_still_durable_on_seal() {
        let dir = tmp();
        let mut w = PartWriter::<D1EventLog>::open(
            "d1_event_log",
            0,
            dir.join("parts/d1/shard-0"),
            4,
            1 << 30,
            10,
            FlushPolicy::Buffered { fsync_every: 100 }, // won't fsync mid-part
        )
        .unwrap();
        for seq in 1..=5 {
            w.append(rec(seq, "100")).unwrap();
        }
        let sealed = w.seal().unwrap().unwrap();
        // seal fsyncs → the file holds all 5 records on disk.
        let bytes = std::fs::read(sealed.meta.local_path.as_ref().unwrap()).unwrap();
        assert_eq!(iter_frames_from(&bytes, 0).unwrap().len(), 5);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
