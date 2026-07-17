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

use ehdb_core::{EhdbError, Result};

use crate::catalog::{GranuleMark, PartMeta, SparseIndex};
use crate::dataset::EventRecord;
use crate::frame::encode_frame;

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
}

/// A sealed, immutable part ready for the manifest + the async uploader.
#[derive(Debug, Clone)]
pub struct SealedPart {
    /// The catalog row for this part (`local_path` set, `replicas` still empty
    /// `None` until the upload lands).
    pub meta: PartMeta,
    /// The exact records this part holds, in sort-key order — returned so the
    /// engine can serve reads of a just-sealed part without re-reading disk, and
    /// so the proof can assert the sealed content.
    pub records: Vec<EventRecord>,
}

/// The active-part writer for one partition. One per shard in the engine.
#[derive(Debug)]
pub struct PartWriter {
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
    flush: FlushPolicy,

    // --- active part state ---
    active_path: PathBuf,
    file: Option<File>,
    records: Vec<EventRecord>,
    marks: Vec<GranuleMark>,
    min_sequence: u64,
    max_sequence: u64,
    byte_len: u64,
    record_count: u64,
    unflushed_since_fsync: u32,
}

impl PartWriter {
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
            flush,
            active_path: PathBuf::new(),
            file: None,
            records: Vec::new(),
            marks: Vec::new(),
            min_sequence: 0,
            max_sequence: 0,
            byte_len: 0,
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
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&self.active_path)
            .map_err(|err| EhdbError::Storage(err.to_string()))?;
        self.file = Some(file);
        self.records.clear();
        self.marks.clear();
        self.min_sequence = 0;
        self.max_sequence = 0;
        self.byte_len = 0;
        self.record_count = 0;
        self.unflushed_since_fsync = 0;
        Ok(())
    }

    /// Append one record to the active part (hot tier). Never touches the object
    /// store — durability rides the async uploader on seal. Returns the byte
    /// offset the frame was written at (its mark).
    pub fn append(&mut self, record: EventRecord) -> Result<u64> {
        let body = serde_json::to_vec(&record)
            .map_err(|err| EhdbError::Storage(format!("encode l0 record: {err}")))?;
        let frame = encode_frame(&body)?;
        let mark_offset = self.byte_len;

        // Start-of-granule mark: first record of each granule.
        if self.record_count % self.granule_size as u64 == 0 {
            self.marks.push(GranuleMark {
                first_sequence: record.global_sequence,
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
        }

        if self.record_count == 0 {
            self.min_sequence = record.global_sequence;
        }
        self.max_sequence = record.global_sequence;
        self.byte_len += frame.len() as u64;
        self.record_count += 1;
        // Grow the current granule's count.
        if let Some(last) = self.marks.last_mut() {
            last.record_count += 1;
        }
        self.records.push(record);
        Ok(mark_offset)
    }

    /// Whether the active part has hit a seal trigger (size or record count).
    pub fn should_seal(&self) -> bool {
        self.record_count > 0
            && (self.byte_len >= self.seal_max_bytes || self.record_count >= self.seal_max_records)
    }

    /// Whether the active part holds any un-sealed records.
    pub fn has_pending(&self) -> bool {
        self.record_count > 0
    }

    /// The active (unsealed) records, for serving the hot tail.
    pub fn pending_records(&self) -> &[EventRecord] {
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
    pub fn seal(&mut self) -> Result<Option<SealedPart>> {
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
                marks: std::mem::take(&mut self.marks),
            },
        };
        let records = std::mem::take(&mut self.records);

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::frame::iter_frames_from;
    use std::sync::atomic::{AtomicU64, Ordering};

    fn tmp() -> PathBuf {
        static N: AtomicU64 = AtomicU64::new(0);
        let n = N.fetch_add(1, Ordering::Relaxed);
        std::env::temp_dir().join(format!(
            "ehdb-l0-part-{n}-{:?}",
            std::thread::current().id()
        ))
    }

    fn rec(seq: u64, exec: &str) -> EventRecord {
        EventRecord::new(seq, exec, format!("txn-{seq}"), format!("payload-{seq}"))
    }

    #[test]
    fn seals_on_record_count_and_builds_sparse_index() {
        let dir = tmp();
        let mut w = PartWriter::open(
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
        let mut w = PartWriter::open(
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
        let mut w = PartWriter::open(
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
