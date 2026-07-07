//! EHDB **durable** event-log backend (completion program, Phase 6 → Phase 9
//! primary-serve prerequisite).
//!
//! This is the production disk format the Phase-6 design note
//! ([Design-Event-Log-Core-Engine]) deferred: *"segmented append files + a
//! sparse offset index for O(log n) sequence seeks … deferred to the
//! primary-serve phase; the local-reference substrate is the reference
//! implementation of the contract, not the production disk format."* It is the
//! hard blocker the prod-cutover runbook's §C durability gate names — the
//! `local_reference` JSONL backend is pod-local and lost on restart, so it is
//! not production-durable as the **authoritative** store under `primary`.
//!
//! ## What this backend adds over `local_reference`
//!
//! * **Segmented append files.** Events land in append-only segment files
//!   (`seg-<id>.eslog`) that roll over at a size threshold instead of one
//!   ever-growing JSONL file, so a segment can be archived / GC'd / replicated
//!   as a unit without rewriting the whole log.
//! * **CRC-framed records + torn-tail recovery.** Each record is a
//!   length+CRC32-framed frame.  A crash mid-append leaves a truncated tail
//!   frame; recovery discards exactly that torn tail and keeps every frame that
//!   an [`append`](DurableEventLogDriver::append) call `fsync`'d before it
//!   returned — the zero-loss-on-restart bar.  A *complete* frame with a bad
//!   CRC or bad magic is bit-rot and is a hard error (matching the incumbent
//!   JSONL log's reject-corrupt-records stance), never silently repaired.
//! * **In-memory offset index (bounded).** The store keeps an offset index
//!   (`global_sequence → (segment, byte offset)`) plus a per-execution sequence
//!   index and durable-consumer ack cursors.  It does **not** keep event
//!   payloads resident — a read locates the frame via the index and cold-loads
//!   the payload bytes from the segment file on demand.  Index memory is
//!   `O(events)` in small fixed-size entries, not `O(total payload bytes)`, the
//!   bounded-WAL-index property [noetl/ai-meta#166] chases.
//! * **`fsync` durability + explicit crash recovery.** Every append `fsync`s
//!   the segment before returning; reopening the store replays the segment
//!   files to rebuild the in-memory index — replay-is-truth, from disk alone.
//!
//! ## Single-writer-per-shard (execution-affinity — a later slice)
//!
//! Coherence under multiple replicas needs exactly one writer per shard.  The
//! plan (per the runbook §C and [noetl/ai-meta#166] execution-affinity work) is
//! to reuse the XxHash64 execution-affinity ownership so each shard is owned by
//! exactly one replica = its sole writer, with reads routed to the owner or
//! cold-loaded from the durable segments.  **This slice assumes the caller is
//! the shard owner** (single writer) and does not wire affinity routing — that
//! is the next slice.  The segment format + recovery here is what a shard owner
//! writes and what a cold-load reads.
//!
//! ## Contract parity with [`EventLogDriver`]
//!
//! [`DurableEventLogDriver`] implements the same [`EventLogDriver`] trait as
//! [`LocalReferenceEventLogDriver`](crate::LocalReferenceEventLogDriver): a
//! monotonic gapless global sequence from 1, per-execution scoped ordered
//! reads, durable-consumer tail/ack, and replay-is-truth.  It is selectable via
//! [`EventLogStorageBackend`] with `local_reference` staying the default, so
//! nothing changes until an operator opts in.
//!
//! [Design-Event-Log-Core-Engine]: https://github.com/noetl/ehdb/wiki/Design-Event-Log-Core-Engine
//! [noetl/ai-meta#166]: https://github.com/noetl/ai-meta/issues/166

use std::{
    collections::{BTreeSet, HashMap},
    fs::{self, File, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

use ehdb_core::{EhdbError, Result};
use serde::{Deserialize, Serialize};

use crate::eventlog::{
    EventLogAckOutcome, EventLogAckRequest, EventLogAppendOutcome, EventLogAppendRequest,
    EventLogDriver, EventLogReadExecutionOutcome, EventLogReadExecutionRequest, EventLogRecordView,
    EventLogScanOutcome, EventLogScanRequest, EventLogTailOutcome, EventLogTailRequest,
};

/// Frame magic — a fixed sentinel prefixing every on-disk frame so a mid-file
/// byte that is present but wrong classifies as corruption rather than a torn
/// tail.
const FRAME_MAGIC: u32 = 0xE5DB_0001;
/// Fixed frame header: `magic(4) + body_len(4) + crc32(4)`.
const FRAME_HEADER_LEN: usize = 12;
/// Default segment rollover threshold (8 MiB).  A new segment is started once
/// the active one would exceed this; a single frame never spans two segments.
pub const DEFAULT_SEGMENT_MAX_BYTES: u64 = 8 * 1024 * 1024;
/// Sanity ceiling on a single frame body — guards recovery against a corrupt
/// length header demanding an absurd allocation.
const MAX_FRAME_BODY_BYTES: usize = 64 * 1024 * 1024;
/// Segment file name prefix.
const SEGMENT_PREFIX: &str = "seg-";
/// Segment file name suffix.
const SEGMENT_SUFFIX: &str = ".eslog";

/// One durable frame as serialized into a segment file.  Events and consumer
/// state (create + ack) share the segment stream so a single replay rebuilds
/// the whole in-memory index — including durable-consumer cursors — from disk.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
enum SegmentFrame {
    /// An appended platform event, carrying the global sequence assigned at
    /// append time.
    Event {
        global_sequence: u64,
        execution_id: String,
        transaction_id: String,
        payload: String,
    },
    /// A durable consumer first seen (JetStream-style create-on-first-pull), so
    /// `created_consumer` is accurate across a restart.
    ConsumerCreate { consumer: String },
    /// A durable-consumer ack advancing its cursor.  Replaying acks in order
    /// reconstructs each consumer's persisted cursor.
    Ack { consumer: String, sequence: u64 },
}

/// Location of an event frame within the segment set: which segment file and
/// the byte offset of the frame's magic.  16 bytes — the payload is *not* held
/// resident; a read cold-loads it from `(segment_id, offset)`.
#[derive(Debug, Clone, Copy)]
struct EventLoc {
    segment_id: u64,
    offset: u64,
}

/// A durable, append-only, segmented event-log store: the production disk
/// format underneath the [`EventLogDriver`] contract.
///
/// Single-writer: the caller is assumed to be the shard owner (see the module
/// docs).  All mutating ops `fsync` before returning; the in-memory index is
/// rebuilt from the segment files on [`open`](Self::open) (replay-is-truth).
#[derive(Debug)]
pub struct DurableSegmentStore {
    root: PathBuf,
    segment_max_bytes: u64,
    /// Offset index: `events[i]` locates the frame for global sequence `i + 1`.
    events: Vec<EventLoc>,
    /// Per-execution ordered global sequences (append-ordered, so ascending).
    by_execution: HashMap<String, Vec<u64>>,
    /// Durable consumers ever created (for accurate `created_consumer`).
    consumers_seen: BTreeSet<String>,
    /// Durable-consumer ack cursors (`consumer → highest acked global seq`).
    consumer_acks: HashMap<String, u64>,
    /// Highest segment id in use (the active, appended-to segment).
    active_segment_id: u64,
    /// Byte length of the active segment file.
    active_len: u64,
    /// A **cold-load** view opened by a non-owner replica: reads only, never
    /// mutates the segment files.  When set, [`write_frame`](Self::write_frame)
    /// refuses (so `append`/`tail`-create/`ack` cannot write another owner's
    /// shard) and [`replay`](Self::replay) does **not** truncate a recovered
    /// torn tail (truncation is a write; only the shard's single owner repairs
    /// its own tail on its own writable open).  See the execution-affinity
    /// single-writer routing slice ([noetl/ai-meta#166]).
    read_only: bool,
}

impl DurableSegmentStore {
    /// Open (or create) a durable segment store rooted at `root`, rebuilding the
    /// in-memory index by replaying the segment files from disk.
    pub fn open(root: impl Into<PathBuf>) -> Result<Self> {
        Self::open_with_segment_size(root, DEFAULT_SEGMENT_MAX_BYTES)
    }

    /// Open with an explicit segment rollover threshold (used by tests to force
    /// multi-segment rollover cheaply).
    pub fn open_with_segment_size(
        root: impl Into<PathBuf>,
        segment_max_bytes: u64,
    ) -> Result<Self> {
        Self::open_inner(root, segment_max_bytes, false)
    }

    /// Open a **read-only cold-load view** of the store — what a non-owner
    /// replica does to serve a read of a shard it does not own (the
    /// execution-affinity single-writer routing slice, [noetl/ai-meta#166]).
    /// The view replays the durable segments to rebuild the index but never
    /// mutates them: a recovered torn tail is *not* truncated (only the shard's
    /// single owner repairs its own tail on its own writable open) and every
    /// mutating op refuses.  A never-written shard (no directory) opens as an
    /// empty log without creating the directory.
    pub fn open_read_only(root: impl Into<PathBuf>) -> Result<Self> {
        Self::open_inner(root, DEFAULT_SEGMENT_MAX_BYTES, true)
    }

    /// Read-only cold-load view with an explicit segment rollover threshold
    /// (matches the writable store's size so replay classifies frames the same;
    /// the threshold is unused for a read-only view since it never appends).
    pub fn open_read_only_with_segment_size(
        root: impl Into<PathBuf>,
        segment_max_bytes: u64,
    ) -> Result<Self> {
        Self::open_inner(root, segment_max_bytes, true)
    }

    fn open_inner(
        root: impl Into<PathBuf>,
        segment_max_bytes: u64,
        read_only: bool,
    ) -> Result<Self> {
        let root = root.into();
        if segment_max_bytes == 0 {
            return Err(EhdbError::InvalidState(
                "durable event-log segment_max_bytes must be > 0".to_string(),
            ));
        }
        let mut store = Self {
            root,
            segment_max_bytes,
            events: Vec::new(),
            by_execution: HashMap::new(),
            consumers_seen: BTreeSet::new(),
            consumer_acks: HashMap::new(),
            active_segment_id: 0,
            active_len: 0,
            read_only,
        };
        if read_only {
            // A cold-load of a never-written shard is an empty log; do not
            // create the directory (a write) to serve an empty read.
            if !store.root.exists() {
                return Ok(store);
            }
        } else {
            fs::create_dir_all(&store.root).map_err(|err| EhdbError::Storage(err.to_string()))?;
        }
        store.replay()?;
        Ok(store)
    }

    /// Whether this is a read-only cold-load view (a non-owner's read of a shard
    /// it does not own).
    pub fn is_read_only(&self) -> bool {
        self.read_only
    }

    /// The directory backing the store.
    pub fn root(&self) -> &Path {
        &self.root
    }

    /// Total events durably appended (== the highest global sequence).
    pub fn len(&self) -> usize {
        self.events.len()
    }

    /// Whether the log is empty (no event ever appended).
    pub fn is_empty(&self) -> bool {
        self.events.is_empty()
    }

    /// Enumerate the store's segment files in ascending id order.
    fn segment_ids(&self) -> Result<Vec<u64>> {
        let mut ids = Vec::new();
        for entry in fs::read_dir(&self.root).map_err(|err| EhdbError::Storage(err.to_string()))? {
            let entry = entry.map_err(|err| EhdbError::Storage(err.to_string()))?;
            let name = entry.file_name();
            let name = name.to_string_lossy();
            if let Some(id) = name
                .strip_prefix(SEGMENT_PREFIX)
                .and_then(|rest| rest.strip_suffix(SEGMENT_SUFFIX))
                .and_then(|digits| digits.parse::<u64>().ok())
            {
                ids.push(id);
            }
        }
        ids.sort_unstable();
        Ok(ids)
    }

    fn segment_path(&self, id: u64) -> PathBuf {
        self.root
            .join(format!("{SEGMENT_PREFIX}{id:016}{SEGMENT_SUFFIX}"))
    }

    /// Rebuild the in-memory index from the segment files.  Torn-tail frames
    /// (truncated by a crash mid-append, before `fsync` returned) are discarded;
    /// a complete-but-corrupt frame is a hard error.
    fn replay(&mut self) -> Result<()> {
        self.events.clear();
        self.by_execution.clear();
        self.consumers_seen.clear();
        self.consumer_acks.clear();
        self.active_segment_id = 0;
        self.active_len = 0;

        let ids = self.segment_ids()?;
        for id in &ids {
            let path = self.segment_path(*id);
            let bytes = fs::read(&path).map_err(|err| EhdbError::Storage(err.to_string()))?;
            let good_len = self.replay_segment(*id, &bytes)?;
            // A torn tail leaves `good_len < bytes.len()`.  Truncate the file to
            // the last intact frame so subsequent appends never sit behind
            // garbage (idempotent recovery: a second reopen sees a clean file).
            // A read-only cold-load view never truncates — truncation is a
            // write, and only the shard's single owner repairs its own tail on
            // its own writable open.  The in-memory index already stops at the
            // last intact frame, so the read view still serves the exact
            // recovered prefix.
            if good_len < bytes.len() as u64 && !self.read_only {
                truncate_segment(&path, good_len)?;
            }
            self.active_segment_id = *id;
            self.active_len = good_len;
        }
        Ok(())
    }

    /// Replay one segment's bytes into the index, returning the byte length of
    /// the intact (non-torn) prefix.
    fn replay_segment(&mut self, segment_id: u64, bytes: &[u8]) -> Result<u64> {
        let mut offset: u64 = 0;
        loop {
            let start = offset as usize;
            // A truncated header at EOF is a torn tail — stop, keep the prefix.
            if start + FRAME_HEADER_LEN > bytes.len() {
                break;
            }
            let magic = u32::from_le_bytes(bytes[start..start + 4].try_into().unwrap());
            let body_len =
                u32::from_le_bytes(bytes[start + 4..start + 8].try_into().unwrap()) as usize;
            let crc = u32::from_le_bytes(bytes[start + 8..start + 12].try_into().unwrap());
            // Magic present but wrong == bit-rot, not a torn tail.
            if magic != FRAME_MAGIC {
                return Err(EhdbError::Storage(format!(
                    "durable event-log segment {segment_id}: bad frame magic at offset {offset}"
                )));
            }
            if body_len > MAX_FRAME_BODY_BYTES {
                return Err(EhdbError::Storage(format!(
                    "durable event-log segment {segment_id}: frame body {body_len} exceeds cap at offset {offset}"
                )));
            }
            let body_start = start + FRAME_HEADER_LEN;
            let body_end = body_start + body_len;
            // A truncated body at EOF is a torn tail — stop, keep the prefix.
            if body_end > bytes.len() {
                break;
            }
            let body = &bytes[body_start..body_end];
            if crc32(body) != crc {
                return Err(EhdbError::Storage(format!(
                    "durable event-log segment {segment_id}: frame CRC mismatch at offset {offset}"
                )));
            }
            let frame: SegmentFrame = serde_json::from_slice(body).map_err(|err| {
                EhdbError::Storage(format!(
                    "durable event-log segment {segment_id}: decode frame at offset {offset}: {err}"
                ))
            })?;
            self.apply_frame(segment_id, offset, frame)?;
            offset = body_end as u64;
        }
        Ok(offset)
    }

    /// Apply one decoded frame to the in-memory index during replay.
    fn apply_frame(&mut self, segment_id: u64, offset: u64, frame: SegmentFrame) -> Result<()> {
        match frame {
            SegmentFrame::Event {
                global_sequence,
                execution_id,
                ..
            } => {
                let expected = self.events.len() as u64 + 1;
                if global_sequence != expected {
                    return Err(EhdbError::Storage(format!(
                        "durable event-log: replay sequence gap, expected {expected} got {global_sequence}"
                    )));
                }
                self.events.push(EventLoc { segment_id, offset });
                self.by_execution
                    .entry(execution_id)
                    .or_default()
                    .push(global_sequence);
            }
            SegmentFrame::ConsumerCreate { consumer } => {
                self.consumers_seen.insert(consumer);
            }
            SegmentFrame::Ack { consumer, sequence } => {
                self.consumers_seen.insert(consumer.clone());
                let cursor = self.consumer_acks.entry(consumer).or_insert(0);
                // Cursor never moves backward.
                if sequence > *cursor {
                    *cursor = sequence;
                }
            }
        }
        Ok(())
    }

    /// Append one frame to the active segment (rolling over first if it would
    /// exceed the size threshold) and `fsync` it.  Returns the location the
    /// frame was written at.
    fn write_frame(&mut self, frame: &SegmentFrame) -> Result<EventLoc> {
        // A read-only cold-load view (a non-owner replica) must never mutate
        // another owner's shard — refuse every write centrally here so append /
        // tail-create / ack all fail closed rather than corrupting the segment.
        if self.read_only {
            return Err(EhdbError::InvalidState(
                "durable event-log: read-only cold-load view cannot write (not the shard owner)"
                    .to_string(),
            ));
        }
        let body = serde_json::to_vec(frame)
            .map_err(|err| EhdbError::Storage(format!("encode durable frame: {err}")))?;
        if body.len() > MAX_FRAME_BODY_BYTES {
            return Err(EhdbError::InvalidState(format!(
                "durable event-log frame body {} exceeds cap {MAX_FRAME_BODY_BYTES}",
                body.len()
            )));
        }
        let frame_len = (FRAME_HEADER_LEN + body.len()) as u64;

        // First append ever, or the active segment is full → start a new one.
        // A frame never spans two segments (the offset index stays valid).
        if self.active_segment_id == 0 {
            self.active_segment_id = 1;
            self.active_len = 0;
        } else if self.active_len > 0 && self.active_len + frame_len > self.segment_max_bytes {
            self.active_segment_id += 1;
            self.active_len = 0;
        }

        let path = self.segment_path(self.active_segment_id);
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .map_err(|err| EhdbError::Storage(err.to_string()))?;

        let mut header = [0u8; FRAME_HEADER_LEN];
        header[0..4].copy_from_slice(&FRAME_MAGIC.to_le_bytes());
        header[4..8].copy_from_slice(&(body.len() as u32).to_le_bytes());
        header[8..12].copy_from_slice(&crc32(&body).to_le_bytes());
        file.write_all(&header)
            .map_err(|err| EhdbError::Storage(err.to_string()))?;
        file.write_all(&body)
            .map_err(|err| EhdbError::Storage(err.to_string()))?;
        // Durability: the write is not acknowledged until it is on stable
        // storage.  A crash after this returns keeps the frame; a crash before
        // leaves a torn tail recovery discards.
        file.sync_data()
            .map_err(|err| EhdbError::Storage(err.to_string()))?;

        let loc = EventLoc {
            segment_id: self.active_segment_id,
            offset: self.active_len,
        };
        self.active_len += frame_len;
        Ok(loc)
    }

    /// Cold-load one event frame's body from its segment file and project it.
    fn read_event(&self, global_sequence: u64) -> Result<EventLogRecordView> {
        let loc = self
            .events
            .get((global_sequence - 1) as usize)
            .copied()
            .ok_or_else(|| {
                EhdbError::InvalidState(format!(
                    "durable event-log: no event at global sequence {global_sequence}"
                ))
            })?;
        let path = self.segment_path(loc.segment_id);
        let mut file = File::open(&path).map_err(|err| EhdbError::Storage(err.to_string()))?;
        file.seek(SeekFrom::Start(loc.offset))
            .map_err(|err| EhdbError::Storage(err.to_string()))?;
        let mut header = [0u8; FRAME_HEADER_LEN];
        file.read_exact(&mut header)
            .map_err(|err| EhdbError::Storage(err.to_string()))?;
        let magic = u32::from_le_bytes(header[0..4].try_into().unwrap());
        let body_len = u32::from_le_bytes(header[4..8].try_into().unwrap()) as usize;
        let crc = u32::from_le_bytes(header[8..12].try_into().unwrap());
        if magic != FRAME_MAGIC {
            return Err(EhdbError::Storage(format!(
                "durable event-log: bad frame magic reading sequence {global_sequence}"
            )));
        }
        let mut body = vec![0u8; body_len];
        file.read_exact(&mut body)
            .map_err(|err| EhdbError::Storage(err.to_string()))?;
        if crc32(&body) != crc {
            return Err(EhdbError::Storage(format!(
                "durable event-log: CRC mismatch reading sequence {global_sequence}"
            )));
        }
        let frame: SegmentFrame = serde_json::from_slice(&body)
            .map_err(|err| EhdbError::Storage(format!("decode durable frame: {err}")))?;
        match frame {
            SegmentFrame::Event {
                global_sequence: seq,
                execution_id,
                transaction_id,
                payload,
            } => Ok(EventLogRecordView {
                global_sequence: seq,
                execution_id,
                transaction_id,
                byte_len: payload.len(),
                payload,
            }),
            _ => Err(EhdbError::Storage(format!(
                "durable event-log: sequence {global_sequence} indexed a non-event frame"
            ))),
        }
    }

    /// Append one authorized event, assigning the next gapless global sequence.
    pub fn append(&mut self, request: &EventLogAppendRequest) -> Result<EventLogAppendOutcome> {
        validate_execution_id(&request.execution_id)?;
        if request.transaction_id.trim().is_empty() {
            return Err(EhdbError::InvalidIdentifier(format!(
                "event-log transaction id: {:?}",
                request.transaction_id
            )));
        }
        let execution_id = request.execution_id.trim().to_string();
        let global_sequence = self.events.len() as u64 + 1;
        let created_stream = self.events.is_empty();
        let byte_len = request.payload.len();

        let frame = SegmentFrame::Event {
            global_sequence,
            execution_id: execution_id.clone(),
            transaction_id: request.transaction_id.clone(),
            payload: request.payload.clone(),
        };
        let loc = self.write_frame(&frame)?;
        self.events.push(loc);
        self.by_execution
            .entry(execution_id.clone())
            .or_default()
            .push(global_sequence);

        Ok(EventLogAppendOutcome {
            action: "eventlog-append".to_string(),
            execution_id,
            global_sequence,
            byte_len,
            created_stream,
            log_record_count: self.events.len(),
        })
    }

    /// Ordered scan of the whole log by global sequence.
    pub fn scan_global(&self, request: &EventLogScanRequest) -> Result<EventLogScanOutcome> {
        if self.events.is_empty() {
            return Ok(EventLogScanOutcome {
                action: "eventlog-scan".to_string(),
                exists: false,
                record_count: 0,
                returned: 0,
                records: Vec::new(),
            });
        }
        let after = request.after.unwrap_or(0);
        let total = self.events.len() as u64;
        let mut records = Vec::new();
        let mut record_count = 0usize;
        let mut seq = after + 1;
        while seq <= total {
            record_count += 1;
            if records.len() < request.limit {
                records.push(self.read_event(seq)?);
            }
            seq += 1;
        }
        Ok(EventLogScanOutcome {
            action: "eventlog-scan".to_string(),
            exists: true,
            record_count,
            returned: records.len(),
            records,
        })
    }

    /// Ordered read scoped to a single execution.
    pub fn read_execution(
        &self,
        request: &EventLogReadExecutionRequest,
    ) -> Result<EventLogReadExecutionOutcome> {
        validate_execution_id(&request.execution_id)?;
        let execution_id = request.execution_id.trim().to_string();
        if self.events.is_empty() {
            return Ok(EventLogReadExecutionOutcome {
                action: "eventlog-read-exec".to_string(),
                execution_id,
                exists: false,
                record_count: 0,
                returned: 0,
                records: Vec::new(),
            });
        }
        let after = request.after.unwrap_or(0);
        let seqs = self.by_execution.get(&execution_id);
        let mut records = Vec::new();
        let mut record_count = 0usize;
        if let Some(seqs) = seqs {
            for &seq in seqs.iter().filter(|&&s| s > after) {
                record_count += 1;
                if records.len() < request.limit {
                    records.push(self.read_event(seq)?);
                }
            }
        }
        Ok(EventLogReadExecutionOutcome {
            action: "eventlog-read-exec".to_string(),
            execution_id,
            // The stream exists (some event appended) even if this execution has
            // none — mirrors the local-reference driver's exists contract.
            exists: true,
            record_count,
            returned: records.len(),
            records,
        })
    }

    /// Durable-consumer tail pull (creates the consumer on first pull; does not
    /// move the ack cursor).
    pub fn tail(&mut self, request: &EventLogTailRequest) -> Result<EventLogTailOutcome> {
        validate_consumer(&request.consumer)?;
        let consumer = request.consumer.trim().to_string();
        if self.events.is_empty() {
            return Ok(EventLogTailOutcome {
                action: "eventlog-tail".to_string(),
                consumer,
                exists: false,
                created_consumer: false,
                acked_sequence: None,
                pending_count: 0,
                returned: 0,
                records: Vec::new(),
            });
        }
        let created_consumer = !self.consumers_seen.contains(&consumer);
        if created_consumer {
            self.write_frame(&SegmentFrame::ConsumerCreate {
                consumer: consumer.clone(),
            })?;
            self.consumers_seen.insert(consumer.clone());
        }
        let acked = self.consumer_acks.get(&consumer).copied();
        let cursor = acked.unwrap_or(0);
        let total = self.events.len() as u64;
        let mut records = Vec::new();
        let mut pending_count = 0usize;
        let mut seq = cursor + 1;
        while seq <= total {
            pending_count += 1;
            if records.len() < request.limit {
                records.push(self.read_event(seq)?);
            }
            seq += 1;
        }
        Ok(EventLogTailOutcome {
            action: "eventlog-tail".to_string(),
            consumer,
            exists: true,
            created_consumer,
            acked_sequence: acked,
            pending_count,
            returned: records.len(),
            records,
        })
    }

    /// Advance a durable consumer's ack cursor after materialize (durably, via a
    /// persisted `Ack` frame).
    pub fn ack(&mut self, request: &EventLogAckRequest) -> Result<EventLogAckOutcome> {
        validate_consumer(&request.consumer)?;
        let consumer = request.consumer.trim().to_string();
        if request.sequence == 0 {
            return Err(EhdbError::InvalidState(
                "durable event-log ack sequence must be >= 1".to_string(),
            ));
        }
        if request.sequence > self.events.len() as u64 {
            return Err(EhdbError::InvalidState(format!(
                "durable event-log ack sequence {} exceeds log length {}",
                request.sequence,
                self.events.len()
            )));
        }
        self.write_frame(&SegmentFrame::Ack {
            consumer: consumer.clone(),
            sequence: request.sequence,
        })?;
        self.consumers_seen.insert(consumer.clone());
        let cursor = self.consumer_acks.entry(consumer.clone()).or_insert(0);
        if request.sequence > *cursor {
            *cursor = request.sequence;
        }
        Ok(EventLogAckOutcome {
            action: "eventlog-ack".to_string(),
            consumer,
            acked_sequence: request.sequence,
        })
    }
}

/// Truncate a segment file to `len` bytes (drops a recovered torn tail).
fn truncate_segment(path: &Path, len: u64) -> Result<()> {
    let file = OpenOptions::new()
        .write(true)
        .open(path)
        .map_err(|err| EhdbError::Storage(err.to_string()))?;
    file.set_len(len)
        .map_err(|err| EhdbError::Storage(err.to_string()))?;
    file.sync_all()
        .map_err(|err| EhdbError::Storage(err.to_string()))?;
    Ok(())
}

/// Validate + normalize an execution id (same rule as the local-reference
/// engine's subject builder — a single `[A-Za-z0-9_-]` token).
fn validate_execution_id(execution_id: &str) -> Result<()> {
    let id = execution_id.trim();
    if id.is_empty()
        || !id
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
    {
        return Err(EhdbError::InvalidIdentifier(format!(
            "event-log execution id: {execution_id:?}"
        )));
    }
    Ok(())
}

/// Validate a durable-consumer name (non-empty `[A-Za-z0-9_-]` token).
fn validate_consumer(consumer: &str) -> Result<()> {
    let name = consumer.trim();
    if name.is_empty()
        || !name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
    {
        return Err(EhdbError::InvalidIdentifier(format!(
            "event-log consumer: {consumer:?}"
        )));
    }
    Ok(())
}

/// CRC32 (IEEE 802.3, reflected) over `data` — dependency-free integrity check
/// for on-disk frames.
fn crc32(data: &[u8]) -> u32 {
    let mut crc: u32 = 0xFFFF_FFFF;
    for &byte in data {
        crc ^= byte as u32;
        for _ in 0..8 {
            let mask = (crc & 1).wrapping_neg();
            crc = (crc >> 1) ^ (0xEDB8_8320 & mask);
        }
    }
    !crc
}

/// The EHDB **durable segment** event-log driver — the production disk backend
/// behind the [`EventLogDriver`] contract.
///
/// Holds the [`DurableSegmentStore`] open across calls behind a mutex (the
/// single shard owner keeps its index resident for O(1) locate), unlike the
/// local-reference driver which reopens the JSONL log per op.  Cloning shares
/// the same open store (`Arc`); a genuine from-disk replay is a *fresh*
/// [`DurableEventLogDriver::open`] over the same root — that is what a pod
/// restart or a cold-load on a new owner does, and what the crash-recovery
/// tests exercise.
#[derive(Debug, Clone)]
pub struct DurableEventLogDriver {
    store: Arc<Mutex<DurableSegmentStore>>,
}

impl DurableEventLogDriver {
    /// Open (or create) a durable driver rooted at `root`, replaying the segment
    /// files to rebuild the index.
    pub fn open(root: impl Into<PathBuf>) -> Result<Self> {
        Ok(Self {
            store: Arc::new(Mutex::new(DurableSegmentStore::open(root)?)),
        })
    }

    /// Open with an explicit segment rollover threshold.
    pub fn open_with_segment_size(
        root: impl Into<PathBuf>,
        segment_max_bytes: u64,
    ) -> Result<Self> {
        Ok(Self {
            store: Arc::new(Mutex::new(DurableSegmentStore::open_with_segment_size(
                root,
                segment_max_bytes,
            )?)),
        })
    }

    fn lock(&self) -> Result<std::sync::MutexGuard<'_, DurableSegmentStore>> {
        self.store.lock().map_err(|_| {
            EhdbError::InvalidState("durable event-log store lock poisoned".to_string())
        })
    }
}

impl EventLogDriver for DurableEventLogDriver {
    fn driver_name(&self) -> &'static str {
        "ehdb-durable-segment"
    }

    fn append(&self, request: &EventLogAppendRequest) -> Result<EventLogAppendOutcome> {
        self.lock()?.append(request)
    }

    fn scan_global(&self, request: &EventLogScanRequest) -> Result<EventLogScanOutcome> {
        self.lock()?.scan_global(request)
    }

    fn read_execution(
        &self,
        request: &EventLogReadExecutionRequest,
    ) -> Result<EventLogReadExecutionOutcome> {
        self.lock()?.read_execution(request)
    }

    fn tail(&self, request: &EventLogTailRequest) -> Result<EventLogTailOutcome> {
        self.lock()?.tail(request)
    }

    fn ack(&self, request: &EventLogAckRequest) -> Result<EventLogAckOutcome> {
        self.lock()?.ack(request)
    }
}

/// Which durable medium backs the event-log tier when EHDB serves it.  This is
/// a *storage-backend* axis, orthogonal to the Phase-10
/// [`TierMode`](crate::backends::TierMode) (`off`/`shadow`/`primary`) axis:
/// mode decides *whether* EHDB serves, this decides *which durable engine* does
/// the serving.
///
/// `local_reference` (pod-local JSONL) stays the default so nothing changes
/// until an operator opts into `durable_segment` — the production-durable
/// segment store this module implements.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EventLogStorageBackend {
    /// The pod-local JSONL reference backend
    /// ([`LocalReferenceEventLogDriver`](crate::LocalReferenceEventLogDriver)).
    /// Default; correct for `shadow`, not production-durable under `primary`.
    #[default]
    LocalReference,
    /// The durable, segmented, crash-recoverable backend
    /// ([`DurableEventLogDriver`]) — the production disk format for `primary`.
    DurableSegment,
}

impl EventLogStorageBackend {
    /// The backend's stable snake_case token (matches the env-var value + the
    /// selfcheck verb naming).
    pub fn as_str(&self) -> &'static str {
        match self {
            EventLogStorageBackend::LocalReference => "local_reference",
            EventLogStorageBackend::DurableSegment => "durable_segment",
        }
    }

    /// The env var an operator sets to pick the backend.
    pub const ENV_VAR: &'static str = "NOETL_EHDB_EVENTLOG_BACKEND";

    /// Fail-safe parse: only the exact token `durable_segment`
    /// (case-insensitive, trimmed) selects the durable backend; everything else
    /// — unset, empty, or unrecognised — is `local_reference` so an unknown
    /// value never silently changes the authoritative store.
    pub fn from_raw(raw: Option<&str>) -> Self {
        match raw.map(|v| v.trim().to_ascii_lowercase()).as_deref() {
            Some("durable_segment") => EventLogStorageBackend::DurableSegment,
            _ => EventLogStorageBackend::LocalReference,
        }
    }
}

// ===========================================================================
// Crash-recovery drive — the star of this slice.
//
// Appends a set of events through a durable driver, ACKs a durable-consumer
// cursor, then **reopens a fresh driver over the same root** (a simulated pod
// restart / cold-load) and proves the reopened store serves the identical
// record set with zero loss, gapless ordering, per-execution scope, and the
// durable cursor intact — replay-is-truth, from the durable segments alone.
// This is the property the prod-cutover runbook's §C durability gate demands
// that `local_reference` (pod-local, lost on restart) cannot give.
// ===========================================================================

/// Secret-free proof of one durable crash-recovery cycle: what a reopened
/// store served after a simulated restart.  Counts + verdicts only (the
/// payloads are the caller's own event bodies).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DurableRecoveryReport {
    /// The backing driver name.
    pub driver_name: String,
    /// How many events were appended before the simulated restart.
    pub appended: usize,
    /// After reopening from disk, the scan returned exactly `appended` events.
    pub zero_loss: bool,
    /// The reopened scan was gapless-ordered 1..=appended.
    pub ordering_ok: bool,
    /// Per-execution reads after reopen returned only that execution's events,
    /// in order, with the same counts as before the restart.
    pub scope_ok: bool,
    /// Every reopened payload byte-for-byte matched what was appended.
    pub payloads_match: bool,
    /// The durable consumer's ack cursor survived the restart (still points at
    /// the acked sequence) and the pending set advanced past it.
    pub cursor_survived: bool,
    /// Pending count the reopened durable consumer reported (== appended - 1
    /// after acking the first sequence).
    pub pending_after_restart: usize,
    /// The single reason recovery failed a durability invariant, or `None`.
    pub divergence: Option<String>,
}

impl DurableRecoveryReport {
    /// Whether the reopened store recovered the whole log durably with every
    /// invariant intact.
    pub fn recovered(&self) -> bool {
        self.zero_loss
            && self.ordering_ok
            && self.scope_ok
            && self.payloads_match
            && self.cursor_survived
            && self.divergence.is_none()
    }
}

/// Drive a durable crash-recovery cycle over the store rooted at `root`.
///
/// Appends `events` through a durable driver, acks `consumer` at the first
/// appended sequence, drops that driver, then **reopens a fresh driver over the
/// same root** (the simulated restart) and verifies zero-loss + gapless
/// ordering + per-execution scope + payload fidelity + durable-cursor survival
/// against what was appended.  `events` must be non-empty.
pub fn exercise_durable_recovery(
    root: impl Into<PathBuf>,
    events: &[EventLogAppendRequest],
    consumer: &str,
) -> Result<DurableRecoveryReport> {
    if events.is_empty() {
        return Err(EhdbError::InvalidState(
            "durable recovery drive requires at least one event".to_string(),
        ));
    }
    let root = root.into();

    // --- Pre-restart: append + ack a durable cursor. -----------------------
    let mut executions: Vec<String> = Vec::new();
    let first_sequence;
    {
        let driver = DurableEventLogDriver::open(&root)?;
        for event in events {
            let outcome = driver.append(event)?;
            if !executions.contains(&outcome.execution_id) {
                executions.push(outcome.execution_id.clone());
            }
        }
        first_sequence = 1u64;
        driver.tail(&EventLogTailRequest {
            consumer: consumer.to_string(),
            transaction_id: "recovery-tail".to_string(),
            limit: events.len(),
        })?;
        driver.ack(&EventLogAckRequest {
            consumer: consumer.to_string(),
            transaction_id: "recovery-ack".to_string(),
            sequence: first_sequence,
        })?;
        // driver dropped here — no buffered state (every op fsync'd).
    }

    // --- Restart: a fresh driver replays the durable segments from disk. ---
    let reopened = DurableEventLogDriver::open(&root)?;
    let scan = reopened.scan_global(&EventLogScanRequest {
        after: None,
        limit: events.len(),
    })?;
    let zero_loss = scan.record_count == events.len() && scan.returned == events.len();
    let expected_seqs: Vec<u64> = (1..=events.len() as u64).collect();
    let got_seqs: Vec<u64> = scan.records.iter().map(|r| r.global_sequence).collect();
    let ordering_ok = scan.exists && got_seqs == expected_seqs;
    let payloads_match = scan.records.iter().zip(events.iter()).all(|(got, want)| {
        got.payload == want.payload && got.execution_id == want.execution_id.trim()
    });

    let mut scope_ok = true;
    for execution_id in &executions {
        let read = reopened.read_execution(&EventLogReadExecutionRequest {
            execution_id: execution_id.clone(),
            after: None,
            limit: events.len(),
        })?;
        let expected = events
            .iter()
            .filter(|e| e.execution_id.trim() == execution_id)
            .count();
        let scoped = read.records.iter().all(|r| &r.execution_id == execution_id);
        let ordered = read
            .records
            .windows(2)
            .all(|w| w[0].global_sequence < w[1].global_sequence);
        scope_ok &= read.exists && scoped && ordered && read.record_count == expected;
    }

    let tail = reopened.tail(&EventLogTailRequest {
        consumer: consumer.to_string(),
        transaction_id: "recovery-tail-2".to_string(),
        limit: events.len(),
    })?;
    let pending_after_restart = tail.pending_count;
    let cursor_survived = tail.acked_sequence == Some(first_sequence)
        && !tail.created_consumer
        && pending_after_restart + 1 == events.len();

    let divergence = if !zero_loss {
        Some(format!(
            "zero-loss failed: reopened {} of {} events",
            scan.record_count,
            events.len()
        ))
    } else if !ordering_ok {
        Some(format!(
            "ordering failed: got {got_seqs:?}, expected {expected_seqs:?}"
        ))
    } else if !payloads_match {
        Some("payload/execution fidelity lost across restart".to_string())
    } else if !scope_ok {
        Some("per-execution scope lost across restart".to_string())
    } else if !cursor_survived {
        Some(format!(
            "durable cursor did not survive restart: acked={:?} created={} pending={pending_after_restart}",
            tail.acked_sequence, tail.created_consumer
        ))
    } else {
        None
    };

    Ok(DurableRecoveryReport {
        driver_name: reopened.driver_name().to_string(),
        appended: events.len(),
        zero_loss,
        ordering_ok,
        scope_ok,
        payloads_match,
        cursor_survived,
        pending_after_restart,
        divergence,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::LocalReferenceEventLogDriver;

    fn tmp_root(tag: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "ehdb-durable-{tag}-{}-{:?}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn append(store: &mut DurableSegmentStore, exec: &str, n: u64, payload: &str) -> u64 {
        store
            .append(&EventLogAppendRequest {
                execution_id: exec.to_string(),
                transaction_id: format!("txn-{exec}-{n}"),
                payload: payload.to_string(),
            })
            .unwrap()
            .global_sequence
    }

    #[test]
    fn append_assigns_monotonic_gapless_global_sequence() {
        let root = tmp_root("seq");
        let mut store = DurableSegmentStore::open(&root).unwrap();
        assert_eq!(append(&mut store, "100", 1, "e1"), 1);
        assert_eq!(append(&mut store, "200", 2, "e2"), 2);
        assert_eq!(append(&mut store, "100", 3, "e3"), 3);
        assert_eq!(store.len(), 3);
        let _ = fs::remove_dir_all(&root);
    }

    #[test]
    fn scan_global_ordered_and_bounded_with_cursor() {
        let root = tmp_root("scan");
        let mut store = DurableSegmentStore::open(&root).unwrap();
        for i in 1..=5 {
            append(&mut store, "100", i, &format!("e{i}"));
        }
        let all = store
            .scan_global(&EventLogScanRequest {
                after: None,
                limit: 100,
            })
            .unwrap();
        assert!(all.exists);
        assert_eq!(all.record_count, 5);
        let seqs: Vec<u64> = all.records.iter().map(|r| r.global_sequence).collect();
        assert_eq!(seqs, vec![1, 2, 3, 4, 5]);
        let page = store
            .scan_global(&EventLogScanRequest {
                after: Some(2),
                limit: 2,
            })
            .unwrap();
        assert_eq!(page.returned, 2);
        assert_eq!(page.records[0].global_sequence, 3);
        // record_count reflects everything after the cursor, before limit.
        assert_eq!(page.record_count, 3);
        let _ = fs::remove_dir_all(&root);
    }

    #[test]
    fn read_execution_is_scoped_and_ordered() {
        let root = tmp_root("exec");
        let mut store = DurableSegmentStore::open(&root).unwrap();
        append(&mut store, "100", 1, "a");
        append(&mut store, "200", 2, "b");
        append(&mut store, "100", 3, "c");
        append(&mut store, "200", 4, "d");
        let ex100 = store
            .read_execution(&EventLogReadExecutionRequest {
                execution_id: "100".to_string(),
                after: None,
                limit: 100,
            })
            .unwrap();
        assert!(ex100.exists);
        assert_eq!(ex100.returned, 2);
        assert_eq!(ex100.records[0].global_sequence, 1);
        assert_eq!(ex100.records[1].global_sequence, 3);
        assert!(ex100.records.iter().all(|r| r.execution_id == "100"));
        let _ = fs::remove_dir_all(&root);
    }

    #[test]
    fn tail_ack_advances_durable_cursor() {
        let root = tmp_root("tail");
        let mut store = DurableSegmentStore::open(&root).unwrap();
        append(&mut store, "100", 1, "a");
        append(&mut store, "100", 2, "b");
        let t1 = store
            .tail(&EventLogTailRequest {
                consumer: "projector".to_string(),
                transaction_id: "c1".to_string(),
                limit: 100,
            })
            .unwrap();
        assert!(t1.exists);
        assert!(t1.created_consumer);
        assert_eq!(t1.pending_count, 2);
        assert_eq!(t1.acked_sequence, None);
        store
            .ack(&EventLogAckRequest {
                consumer: "projector".to_string(),
                transaction_id: "ack1".to_string(),
                sequence: 1,
            })
            .unwrap();
        let t2 = store
            .tail(&EventLogTailRequest {
                consumer: "projector".to_string(),
                transaction_id: "c2".to_string(),
                limit: 100,
            })
            .unwrap();
        assert!(!t2.created_consumer);
        assert_eq!(t2.pending_count, 1);
        assert_eq!(t2.acked_sequence, Some(1));
        assert_eq!(t2.records[0].global_sequence, 2);
        let _ = fs::remove_dir_all(&root);
    }

    #[test]
    fn crash_recovery_replays_identical_state_from_disk() {
        let root = tmp_root("recovery");
        {
            let mut store = DurableSegmentStore::open(&root).unwrap();
            append(&mut store, "100", 1, "a");
            append(&mut store, "200", 2, "b");
            append(&mut store, "100", 3, "c");
            store
                .ack(&EventLogAckRequest {
                    consumer: "projector".to_string(),
                    transaction_id: "ack".to_string(),
                    sequence: 1,
                })
                .unwrap();
        }
        // Simulate a restart: a brand-new store over the same root replays disk.
        let mut reopened = DurableSegmentStore::open(&root).unwrap();
        assert_eq!(reopened.len(), 3);
        let scan = reopened
            .scan_global(&EventLogScanRequest {
                after: None,
                limit: 100,
            })
            .unwrap();
        assert_eq!(
            scan.records
                .iter()
                .map(|r| r.global_sequence)
                .collect::<Vec<_>>(),
            vec![1, 2, 3]
        );
        // Payload survives verbatim.
        assert_eq!(scan.records[0].payload, "a");
        // Durable cursor survived the restart.
        let tail = reopened
            .tail(&EventLogTailRequest {
                consumer: "projector".to_string(),
                transaction_id: "c".to_string(),
                limit: 100,
            })
            .unwrap();
        assert_eq!(tail.acked_sequence, Some(1));
        assert_eq!(tail.pending_count, 2);
        assert!(!tail.created_consumer, "consumer create survived restart");
        // Next append continues the sequence without a gap.
        assert_eq!(append(&mut reopened, "300", 4, "d"), 4);
        let _ = fs::remove_dir_all(&root);
    }

    #[test]
    fn segment_rollover_spans_multiple_files_and_replays() {
        let root = tmp_root("rollover");
        // Tiny segment size forces a new file every few frames.
        {
            let mut store = DurableSegmentStore::open_with_segment_size(&root, 128).unwrap();
            for i in 1..=20 {
                append(&mut store, "100", i, &format!("payload-{i:03}"));
            }
        }
        // More than one segment file was produced.
        let segs: Vec<_> = fs::read_dir(&root)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.file_name().to_string_lossy().starts_with(SEGMENT_PREFIX))
            .collect();
        assert!(
            segs.len() > 1,
            "expected rollover, got {} segment(s)",
            segs.len()
        );
        // Replay across all segments reconstructs the whole gapless log.
        let reopened = DurableSegmentStore::open_with_segment_size(&root, 128).unwrap();
        let scan = reopened
            .scan_global(&EventLogScanRequest {
                after: None,
                limit: 1000,
            })
            .unwrap();
        assert_eq!(scan.record_count, 20);
        assert_eq!(
            scan.records
                .iter()
                .map(|r| r.global_sequence)
                .collect::<Vec<_>>(),
            (1..=20).collect::<Vec<_>>()
        );
        assert_eq!(scan.records[19].payload, "payload-020");
        let _ = fs::remove_dir_all(&root);
    }

    #[test]
    fn torn_tail_is_discarded_and_prior_events_survive() {
        let root = tmp_root("torn");
        {
            let mut store = DurableSegmentStore::open(&root).unwrap();
            append(&mut store, "100", 1, "a");
            append(&mut store, "100", 2, "b");
        }
        // Simulate a crash mid-append: append a garbage partial frame (a header
        // claiming a 100-byte body but only 3 bytes present) to the active seg.
        let seg = fs::read_dir(&root)
            .unwrap()
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .find(|p| {
                p.file_name()
                    .unwrap()
                    .to_string_lossy()
                    .starts_with(SEGMENT_PREFIX)
            })
            .unwrap();
        {
            let mut f = OpenOptions::new().append(true).open(&seg).unwrap();
            let mut torn = [0u8; FRAME_HEADER_LEN];
            torn[0..4].copy_from_slice(&FRAME_MAGIC.to_le_bytes());
            torn[4..8].copy_from_slice(&100u32.to_le_bytes()); // claims 100-byte body
            torn[8..12].copy_from_slice(&0u32.to_le_bytes());
            f.write_all(&torn).unwrap();
            f.write_all(b"xyz").unwrap(); // only 3 of the 100 body bytes
            f.sync_data().unwrap();
        }
        // Recovery discards the torn tail, keeps the two fsync'd events, and can
        // append seq 3 cleanly.
        let mut reopened = DurableSegmentStore::open(&root).unwrap();
        assert_eq!(reopened.len(), 2);
        let scan = reopened
            .scan_global(&EventLogScanRequest {
                after: None,
                limit: 10,
            })
            .unwrap();
        assert_eq!(scan.record_count, 2);
        assert_eq!(append(&mut reopened, "100", 3, "c"), 3);
        // A second reopen sees a clean file (torn tail was truncated).
        let twice = DurableSegmentStore::open(&root).unwrap();
        assert_eq!(twice.len(), 3);
        let _ = fs::remove_dir_all(&root);
    }

    #[test]
    fn complete_frame_with_bad_crc_is_hard_error() {
        let root = tmp_root("corrupt");
        {
            let mut store = DurableSegmentStore::open(&root).unwrap();
            append(&mut store, "100", 1, "a");
        }
        let seg = fs::read_dir(&root)
            .unwrap()
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .find(|p| {
                p.file_name()
                    .unwrap()
                    .to_string_lossy()
                    .starts_with(SEGMENT_PREFIX)
            })
            .unwrap();
        // Flip a byte in the body (offset past the 12-byte header) so the frame
        // is complete but its CRC no longer matches — bit-rot, not a torn tail.
        let mut bytes = fs::read(&seg).unwrap();
        let body_byte = FRAME_HEADER_LEN + 2;
        bytes[body_byte] ^= 0xFF;
        fs::write(&seg, &bytes).unwrap();
        let err = DurableSegmentStore::open(&root).unwrap_err();
        assert!(matches!(err, EhdbError::Storage(_)), "{err:?}");
        assert!(err.to_string().contains("CRC mismatch"));
        let _ = fs::remove_dir_all(&root);
    }

    #[test]
    fn absent_probes_are_not_errors() {
        let root = tmp_root("absent");
        let mut store = DurableSegmentStore::open(&root).unwrap();
        assert!(
            !store
                .scan_global(&EventLogScanRequest {
                    after: None,
                    limit: 10
                })
                .unwrap()
                .exists
        );
        assert!(
            !store
                .read_execution(&EventLogReadExecutionRequest {
                    execution_id: "100".to_string(),
                    after: None,
                    limit: 10,
                })
                .unwrap()
                .exists
        );
        assert!(
            !store
                .tail(&EventLogTailRequest {
                    consumer: "c".to_string(),
                    transaction_id: "t".to_string(),
                    limit: 10,
                })
                .unwrap()
                .exists
        );
        let _ = fs::remove_dir_all(&root);
    }

    #[test]
    fn invalid_ids_classify_distinctly() {
        let root = tmp_root("badid");
        let mut store = DurableSegmentStore::open(&root).unwrap();
        let err = store
            .append(&EventLogAppendRequest {
                execution_id: "bad id!".to_string(),
                transaction_id: "t".to_string(),
                payload: "x".to_string(),
            })
            .unwrap_err();
        assert!(matches!(err, EhdbError::InvalidIdentifier(_)));
        // ack of a never-written sequence is InvalidState (bound), not identifier.
        append(&mut store, "100", 1, "a");
        let err = store
            .ack(&EventLogAckRequest {
                consumer: "c".to_string(),
                transaction_id: "t".to_string(),
                sequence: 99,
            })
            .unwrap_err();
        assert!(matches!(err, EhdbError::InvalidState(_)));
        let _ = fs::remove_dir_all(&root);
    }

    #[test]
    fn parity_with_local_reference_over_identical_ops() {
        // The durable backend must produce identical observable results to the
        // local-reference backend for the same op sequence (contract parity).
        let durable_root = tmp_root("parity-durable");
        let local_dir = tmp_root("parity-local");
        let local_log = local_dir.join("log.jsonl");

        let durable = DurableEventLogDriver::open(&durable_root).unwrap();
        let local = LocalReferenceEventLogDriver::new(&local_log, "noetl", "default");

        let ops = [("100", "a"), ("200", "b"), ("100", "c"), ("300", "d")];
        for (i, (exec, payload)) in ops.iter().enumerate() {
            let d = durable
                .append(&EventLogAppendRequest {
                    execution_id: exec.to_string(),
                    transaction_id: format!("txn-{i}"),
                    payload: payload.to_string(),
                })
                .unwrap();
            let l = local
                .append(&EventLogAppendRequest {
                    execution_id: exec.to_string(),
                    transaction_id: format!("txn-{i}"),
                    payload: payload.to_string(),
                })
                .unwrap();
            assert_eq!(d.global_sequence, l.global_sequence);
            assert_eq!(d.created_stream, l.created_stream);
            assert_eq!(d.log_record_count, l.log_record_count);
        }

        // Global scan parity.
        let ds = durable
            .scan_global(&EventLogScanRequest {
                after: None,
                limit: 100,
            })
            .unwrap();
        let ls = local
            .scan_global(&EventLogScanRequest {
                after: None,
                limit: 100,
            })
            .unwrap();
        assert_eq!(ds.record_count, ls.record_count);
        assert_eq!(
            ds.records
                .iter()
                .map(|r| (r.global_sequence, r.execution_id.clone(), r.payload.clone()))
                .collect::<Vec<_>>(),
            ls.records
                .iter()
                .map(|r| (r.global_sequence, r.execution_id.clone(), r.payload.clone()))
                .collect::<Vec<_>>(),
        );

        // Per-execution scope parity.
        let dr = durable
            .read_execution(&EventLogReadExecutionRequest {
                execution_id: "100".to_string(),
                after: None,
                limit: 100,
            })
            .unwrap();
        let lr = local
            .read_execution(&EventLogReadExecutionRequest {
                execution_id: "100".to_string(),
                after: None,
                limit: 100,
            })
            .unwrap();
        assert_eq!(dr.returned, lr.returned);
        assert_eq!(
            dr.records
                .iter()
                .map(|r| r.global_sequence)
                .collect::<Vec<_>>(),
            lr.records
                .iter()
                .map(|r| r.global_sequence)
                .collect::<Vec<_>>(),
        );
        let _ = fs::remove_dir_all(&durable_root);
        let _ = fs::remove_dir_all(&local_dir);
    }

    #[test]
    fn backend_selector_defaults_to_local_reference() {
        assert_eq!(
            EventLogStorageBackend::default(),
            EventLogStorageBackend::LocalReference
        );
        assert_eq!(
            EventLogStorageBackend::from_raw(None),
            EventLogStorageBackend::LocalReference
        );
        assert_eq!(
            EventLogStorageBackend::from_raw(Some("")),
            EventLogStorageBackend::LocalReference
        );
        assert_eq!(
            EventLogStorageBackend::from_raw(Some("garbage")),
            EventLogStorageBackend::LocalReference
        );
        assert_eq!(
            EventLogStorageBackend::from_raw(Some(" Durable_Segment ")),
            EventLogStorageBackend::DurableSegment
        );
        assert_eq!(
            EventLogStorageBackend::DurableSegment.as_str(),
            "durable_segment"
        );
    }

    #[test]
    fn exercise_durable_recovery_proves_zero_loss() {
        let root = tmp_root("recovery-drive");
        let events: Vec<EventLogAppendRequest> = [("100", "e1"), ("200", "e2"), ("100", "e3")]
            .iter()
            .enumerate()
            .map(|(i, (exec, payload))| EventLogAppendRequest {
                execution_id: exec.to_string(),
                transaction_id: format!("txn-{i}"),
                payload: payload.to_string(),
            })
            .collect();
        let report = exercise_durable_recovery(&root, &events, "projector").unwrap();
        assert!(report.recovered(), "{report:?}");
        assert_eq!(report.driver_name, "ehdb-durable-segment");
        assert_eq!(report.appended, 3);
        assert!(report.zero_loss);
        assert!(report.ordering_ok);
        assert!(report.scope_ok);
        assert!(report.payloads_match);
        assert!(report.cursor_survived);
        assert_eq!(report.pending_after_restart, 2);
        assert!(report.divergence.is_none());
        let _ = fs::remove_dir_all(&root);
    }

    #[test]
    fn exercise_durable_recovery_requires_events() {
        let root = tmp_root("recovery-empty");
        let err = exercise_durable_recovery(&root, &[], "projector").unwrap_err();
        assert!(err.to_string().contains("at least one event"));
        let _ = fs::remove_dir_all(&root);
    }

    #[test]
    fn crc32_matches_known_vector() {
        // CRC32/IEEE of "123456789" is 0xCBF43926 (the standard check value).
        assert_eq!(crc32(b"123456789"), 0xCBF4_3926);
    }

    #[test]
    fn read_only_view_serves_reads_without_mutating() {
        let root = tmp_root("ro-reads");
        {
            let mut store = DurableSegmentStore::open(&root).unwrap();
            append(&mut store, "100", 1, "a");
            append(&mut store, "200", 2, "b");
        }
        // A read-only cold-load view (what a non-owner replica opens) serves the
        // same records the owner wrote.
        let view = DurableSegmentStore::open_read_only(&root).unwrap();
        assert!(view.is_read_only());
        let scan = view
            .scan_global(&EventLogScanRequest {
                after: None,
                limit: 100,
            })
            .unwrap();
        assert_eq!(scan.record_count, 2);
        assert_eq!(scan.records[0].payload, "a");
        let ex = view
            .read_execution(&EventLogReadExecutionRequest {
                execution_id: "200".to_string(),
                after: None,
                limit: 100,
            })
            .unwrap();
        assert_eq!(ex.returned, 1);
        assert_eq!(ex.records[0].global_sequence, 2);
        let _ = fs::remove_dir_all(&root);
    }

    #[test]
    fn read_only_view_refuses_writes() {
        let root = tmp_root("ro-write");
        {
            let mut store = DurableSegmentStore::open(&root).unwrap();
            append(&mut store, "100", 1, "a");
        }
        let mut view = DurableSegmentStore::open_read_only(&root).unwrap();
        // append refuses.
        let err = view
            .append(&EventLogAppendRequest {
                execution_id: "100".to_string(),
                transaction_id: "t".to_string(),
                payload: "nope".to_string(),
            })
            .unwrap_err();
        assert!(matches!(err, EhdbError::InvalidState(_)));
        assert!(err.to_string().contains("read-only"));
        // tail (which would create-on-first-pull → a write) also refuses.
        let err = view
            .tail(&EventLogTailRequest {
                consumer: "c".to_string(),
                transaction_id: "t".to_string(),
                limit: 10,
            })
            .unwrap_err();
        assert!(matches!(err, EhdbError::InvalidState(_)));
        let _ = fs::remove_dir_all(&root);
    }

    #[test]
    fn read_only_view_does_not_truncate_torn_tail() {
        let root = tmp_root("ro-torn");
        {
            let mut store = DurableSegmentStore::open(&root).unwrap();
            append(&mut store, "100", 1, "a");
            append(&mut store, "100", 2, "b");
        }
        // Append a torn (partial) frame simulating a crash mid-append.
        let seg = fs::read_dir(&root)
            .unwrap()
            .filter_map(|e| e.ok())
            .map(|e| e.path())
            .find(|p| {
                p.file_name()
                    .unwrap()
                    .to_string_lossy()
                    .starts_with(SEGMENT_PREFIX)
            })
            .unwrap();
        {
            let mut f = OpenOptions::new().append(true).open(&seg).unwrap();
            let mut torn = [0u8; FRAME_HEADER_LEN];
            torn[0..4].copy_from_slice(&FRAME_MAGIC.to_le_bytes());
            torn[4..8].copy_from_slice(&100u32.to_le_bytes());
            torn[8..12].copy_from_slice(&0u32.to_le_bytes());
            f.write_all(&torn).unwrap();
            f.write_all(b"xyz").unwrap();
            f.sync_data().unwrap();
        }
        let len_with_torn = fs::metadata(&seg).unwrap().len();
        // A read-only view recovers the two fsync'd events (index stops at the
        // last intact frame) but leaves the torn bytes on disk untouched.
        let view = DurableSegmentStore::open_read_only(&root).unwrap();
        assert_eq!(view.len(), 2);
        assert_eq!(
            fs::metadata(&seg).unwrap().len(),
            len_with_torn,
            "read-only view must not truncate the torn tail"
        );
        // A writable owner open, by contrast, does truncate (idempotent repair).
        let owner = DurableSegmentStore::open(&root).unwrap();
        assert_eq!(owner.len(), 2);
        assert!(fs::metadata(&seg).unwrap().len() < len_with_torn);
        let _ = fs::remove_dir_all(&root);
    }

    #[test]
    fn read_only_view_of_never_written_shard_is_empty() {
        let root = tmp_root("ro-absent").join("shard-does-not-exist");
        let view = DurableSegmentStore::open_read_only(&root).unwrap();
        assert!(view.is_empty());
        // Opening a read-only view of a missing shard must not create it.
        assert!(!root.exists());
    }
}
