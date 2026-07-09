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
//! * **O(1) open-for-append via a checkpoint sidecar.** The worker constructs
//!   this stack **per op** (a stateless boundary), so every mirrored append pays
//!   a fresh [`open`](DurableSegmentStore::open) — and a full replay is
//!   O(segment), which dominated the deployed durable append rate
//!   ([noetl/ehdb#267]). A small [`StoreCheckpoint`] sidecar (`event_count`,
//!   active-segment id + length, durable-consumer cursors) is rewritten after
//!   each mutating op so open-for-append loads it in O(1) and skips the replay;
//!   the offset index is materialized lazily on the first *read*
//!   ([`ensure_index_loaded`](DurableSegmentStore::ensure_index_loaded)). It is
//!   an optimization only — a missing / stale / inconsistent checkpoint falls
//!   back to a full replay (replay-is-truth), and the checkpoint can never name
//!   more durable data than the segments hold (it is rewritten strictly *after*
//!   the frame `fsync`), so recovery is never wrong. This mirrors the shared
//!   tier's resumable-digest sidecar ([noetl/ehdb#266]).
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
//! [noetl/ehdb#266]: https://github.com/noetl/ehdb/issues/266
//! [noetl/ehdb#267]: https://github.com/noetl/ehdb/issues/267

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
/// Sidecar checkpoint file name — a small per-store snapshot (`event_count`,
/// active-segment id + length, durable-consumer cursors) rewritten after each
/// mutating op so a subsequent open-for-append loads it in O(1) instead of
/// replaying every segment to rebuild the offset index. Not a segment file, so
/// [`DurableSegmentStore::segment_ids`] ignores it.
const CHECKPOINT_FILE: &str = "checkpoint.json";

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

/// The per-store checkpoint sidecar ([`CHECKPOINT_FILE`]) that lets
/// open-for-append skip the O(segment) replay (the [noetl/ehdb#267] fix).
///
/// It carries exactly the O(1) state the append / ack hot path needs — the
/// event count (for the next global sequence), the active-segment position (for
/// where to write), and the durable-consumer cursors — but **not** the offset
/// index (materialized lazily on the first read). It is written strictly
/// *after* the frame it describes is `fsync`'d, so it can never name more
/// durable data than the segments hold; a crash between the frame `fsync` and
/// the checkpoint rewrite leaves a segment *longer* than `active_len`, detected
/// as inconsistent on the next open → full replay recovers the extra frame(s).
/// Replay-is-truth remains the authoritative recovery path; this is a cache.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct StoreCheckpoint {
    /// Total events durably appended (the highest global sequence).
    event_count: u64,
    /// The active (appended-to) segment id at the time of the snapshot.
    active_segment_id: u64,
    /// The active segment file's byte length at the time of the snapshot — the
    /// consistency anchor: a trusted checkpoint requires the on-disk active
    /// segment to be exactly this long.
    active_len: u64,
    /// Durable consumers ever created (for accurate `created_consumer`).
    consumers_seen: Vec<String>,
    /// Durable-consumer ack cursors (`consumer → highest acked global seq`).
    consumer_acks: HashMap<String, u64>,
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
    /// Total events durably appended (the highest global sequence). Tracked
    /// explicitly so the append hot path stays O(1) even when the offset index
    /// is not materialized — a checkpoint-trust open ([noetl/ehdb#267]) leaves
    /// `events` empty but still knows the count from the checkpoint.
    event_count: u64,
    /// Whether the full offset index (`events` + `by_execution`) is
    /// materialized. A checkpoint-trust open leaves it `false` and lazily
    /// replays on the first read ([`ensure_index_loaded`](Self::ensure_index_loaded));
    /// a full-replay open (fallback) or a read-only cold-load sets it `true`.
    index_loaded: bool,
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
            event_count: 0,
            index_loaded: false,
        };
        if read_only {
            // A cold-load of a never-written shard is an empty log; do not
            // create the directory (a write) to serve an empty read. Read views
            // need the full offset index for reads (the checkpoint carries no
            // payload index), so they always replay — replay-is-truth.
            if !store.root.exists() {
                store.index_loaded = true;
                return Ok(store);
            }
            store.replay()?;
            store.index_loaded = true;
            return Ok(store);
        }
        fs::create_dir_all(&store.root).map_err(|err| EhdbError::Storage(err.to_string()))?;
        // Fast path ([noetl/ehdb#267]): a consistent checkpoint lets
        // open-for-append skip the O(segment) replay — load counts + active
        // position + consumer cursors in O(1); the offset index is materialized
        // lazily on the first read. A missing / stale / inconsistent checkpoint
        // falls back to a full replay (replay-is-truth) and rewrites it.
        if let Some(checkpoint) = store.load_checkpoint()? {
            if store.checkpoint_consistent(&checkpoint)? {
                store.apply_checkpoint(checkpoint);
                store.index_loaded = false;
                return Ok(store);
            }
        }
        store.replay()?;
        store.index_loaded = true;
        store.persist_checkpoint()?;
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

    /// Total events durably appended (== the highest global sequence). Reads the
    /// explicit counter, which is authoritative even when a checkpoint-trust open
    /// has left the offset index (`events`) unmaterialized.
    pub fn len(&self) -> usize {
        self.event_count as usize
    }

    /// Whether the log is empty (no event ever appended).
    pub fn is_empty(&self) -> bool {
        self.event_count == 0
    }

    /// The checkpoint sidecar path under the store root.
    fn checkpoint_path(&self) -> PathBuf {
        self.root.join(CHECKPOINT_FILE)
    }

    /// Load the checkpoint sidecar if present + decodable. A decode error is
    /// treated as *absent* (fall back to replay) rather than failing the open —
    /// the checkpoint is an optimization, never the source of truth.
    fn load_checkpoint(&self) -> Result<Option<StoreCheckpoint>> {
        match fs::read(self.checkpoint_path()) {
            Ok(bytes) => Ok(serde_json::from_slice(&bytes).ok()),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(err) => Err(EhdbError::Storage(err.to_string())),
        }
    }

    /// Whether a checkpoint faithfully describes the segments on disk, so
    /// open-for-append can trust it and skip the replay. Deliberately strict: the
    /// active segment must be the highest id present and its on-disk length must
    /// equal `active_len` exactly. Because a frame is counted in the checkpoint
    /// only *after* it is `fsync`'d (append fsyncs the frame, bumps counts, then
    /// rewrites the checkpoint), a crash between the frame `fsync` and the
    /// checkpoint rewrite leaves the segment *longer* than `active_len` — caught
    /// here as inconsistent → full replay recovers the extra fsync'd frame(s).
    /// The checkpoint can never name *more* durable data than the segments hold,
    /// so a length match guarantees the counts are exact.
    fn checkpoint_consistent(&self, checkpoint: &StoreCheckpoint) -> Result<bool> {
        let ids = self.segment_ids()?;
        if checkpoint.event_count == 0 {
            // An empty log never writes a frame (tail/ack refuse on an empty
            // log, append is the only first writer), so a zero-count checkpoint
            // must correspond to no segments at all.
            return Ok(ids.is_empty()
                && checkpoint.active_segment_id == 0
                && checkpoint.active_len == 0);
        }
        match ids.last().copied() {
            Some(highest) if highest == checkpoint.active_segment_id => {
                let actual_len = fs::metadata(self.segment_path(highest))
                    .map_err(|err| EhdbError::Storage(err.to_string()))?
                    .len();
                Ok(actual_len == checkpoint.active_len)
            }
            // No segments, or a segment newer than the checkpoint knows about
            // (a crash after rotation but before the checkpoint rewrite): stale.
            _ => Ok(false),
        }
    }

    /// Adopt a trusted checkpoint's counts / active position / cursors without
    /// materializing the offset index (left empty; the first read replays to
    /// build it). The append + ack hot paths need only these O(1) fields.
    fn apply_checkpoint(&mut self, checkpoint: StoreCheckpoint) {
        self.event_count = checkpoint.event_count;
        self.active_segment_id = checkpoint.active_segment_id;
        self.active_len = checkpoint.active_len;
        self.consumers_seen = checkpoint.consumers_seen.into_iter().collect();
        self.consumer_acks = checkpoint.consumer_acks;
        self.events.clear();
        self.by_execution.clear();
    }

    /// Persist the checkpoint sidecar (atomic temp-file + rename) after a
    /// mutating op. **No `fsync`**: the checkpoint is an optimization, so a crash
    /// that loses the latest rewrite simply falls back to a one-time replay on
    /// the next open (replay-is-truth). Correctness never depends on the
    /// checkpoint being durable — only on it never describing *more* than the
    /// fsync'd segments, guaranteed by the caller writing it strictly after the
    /// frame `fsync`. A read-only cold-load view never writes one.
    fn persist_checkpoint(&self) -> Result<()> {
        if self.read_only {
            return Ok(());
        }
        let checkpoint = StoreCheckpoint {
            event_count: self.event_count,
            active_segment_id: self.active_segment_id,
            active_len: self.active_len,
            consumers_seen: self.consumers_seen.iter().cloned().collect(),
            consumer_acks: self.consumer_acks.clone(),
        };
        let bytes = serde_json::to_vec(&checkpoint)
            .map_err(|err| EhdbError::Storage(format!("encode durable checkpoint: {err}")))?;
        let path = self.checkpoint_path();
        let tmp = self.root.join(format!("{CHECKPOINT_FILE}.tmp"));
        {
            let mut file = OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .open(&tmp)
                .map_err(|err| EhdbError::Storage(err.to_string()))?;
            file.write_all(&bytes)
                .map_err(|err| EhdbError::Storage(err.to_string()))?;
        }
        fs::rename(&tmp, &path).map_err(|err| EhdbError::Storage(err.to_string()))?;
        Ok(())
    }

    /// Materialize the full offset index if a checkpoint-trust open left it lazy.
    /// This is where the replay-is-truth integrity check (CRC + gapless sequence)
    /// runs for a checkpoint-opened store — corruption is caught on the first
    /// read, never silently served (the append path never reads event bodies).
    /// Idempotent; a no-op once loaded (a read-only cold-load loads eagerly at
    /// open, so this never fires for it).
    fn ensure_index_loaded(&mut self) -> Result<()> {
        if self.index_loaded {
            return Ok(());
        }
        self.replay()?;
        self.index_loaded = true;
        Ok(())
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
        // The offset index now holds every durable event; keep the explicit
        // counter in lockstep (it is authoritative for the append hot path).
        self.event_count = self.events.len() as u64;
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
        let global_sequence = self.event_count + 1;
        let created_stream = self.event_count == 0;
        let byte_len = request.payload.len();

        let frame = SegmentFrame::Event {
            global_sequence,
            execution_id: execution_id.clone(),
            transaction_id: request.transaction_id.clone(),
            payload: request.payload.clone(),
        };
        let loc = self.write_frame(&frame)?;
        self.event_count += 1;
        // Keep the resident offset index current only when it is materialized; a
        // checkpoint-trust open leaves it lazy and rebuilds it from disk on the
        // first read (which sees this fsync'd frame).
        if self.index_loaded {
            self.events.push(loc);
            self.by_execution
                .entry(execution_id.clone())
                .or_default()
                .push(global_sequence);
        }
        // Rewrite the checkpoint AFTER the frame is `fsync`'d (in `write_frame`)
        // so it never names more durable data than the segments hold.
        self.persist_checkpoint()?;

        Ok(EventLogAppendOutcome {
            action: "eventlog-append".to_string(),
            execution_id,
            global_sequence,
            byte_len,
            created_stream,
            log_record_count: self.event_count as usize,
        })
    }

    /// Ordered scan of the whole log by global sequence. Takes `&mut self`
    /// because a checkpoint-trust open defers the offset-index rebuild to the
    /// first read ([`ensure_index_loaded`](Self::ensure_index_loaded)).
    pub fn scan_global(&mut self, request: &EventLogScanRequest) -> Result<EventLogScanOutcome> {
        self.ensure_index_loaded()?;
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

    /// Ordered read scoped to a single execution. Takes `&mut self` because a
    /// checkpoint-trust open defers the offset-index rebuild to the first read
    /// ([`ensure_index_loaded`](Self::ensure_index_loaded)).
    pub fn read_execution(
        &mut self,
        request: &EventLogReadExecutionRequest,
    ) -> Result<EventLogReadExecutionOutcome> {
        validate_execution_id(&request.execution_id)?;
        self.ensure_index_loaded()?;
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
        // A tail pull reads pending event bodies (and may create-on-first-pull,
        // a write); it needs the full offset index.
        self.ensure_index_loaded()?;
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
            // The create advanced the active segment (a persisted frame) and the
            // consumer set — checkpoint after the `fsync` so a subsequent
            // checkpoint-trust open sees the consumer and the new active length.
            self.persist_checkpoint()?;
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
        // Bound against the explicit counter (authoritative even on a
        // checkpoint-trust open where the offset index is not materialized); an
        // ack needs no offset index, so it stays O(1) — no `ensure_index_loaded`.
        if request.sequence > self.event_count {
            return Err(EhdbError::InvalidState(format!(
                "durable event-log ack sequence {} exceeds log length {}",
                request.sequence, self.event_count
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
        // Checkpoint after the ack frame `fsync` so the persisted cursor + new
        // active length survive to the next checkpoint-trust open.
        self.persist_checkpoint()?;
        Ok(EventLogAckOutcome {
            action: "eventlog-ack".to_string(),
            consumer,
            acked_sequence: request.sequence,
        })
    }
}

/// The on-disk file name for segment `id` (`seg-<016>.eslog`).
///
/// Exposed so the shared/object-store segment tier
/// ([`crate::durable_eventlog_shared`]) can enumerate + publish a shard's
/// segments by id — and materialize cold-loaded segments back — without
/// re-deriving the naming that the store's own [`DurableSegmentStore::replay`]
/// depends on.
pub fn segment_file_name(id: u64) -> String {
    format!("{SEGMENT_PREFIX}{id:016}{SEGMENT_SUFFIX}")
}

/// Parse a segment file name back to its id, or `None` when `name` is not a
/// segment file. Inverse of [`segment_file_name`].
pub fn parse_segment_file_name(name: &str) -> Option<u64> {
    name.strip_prefix(SEGMENT_PREFIX)
        .and_then(|rest| rest.strip_suffix(SEGMENT_SUFFIX))
        .and_then(|digits| digits.parse::<u64>().ok())
}

/// List a segment-store directory's segment files in ascending id order as
/// `(segment_id, path)`. A missing directory is an empty list (not an error) —
/// a shard never written yet. Used by the shared tier to publish an owner's
/// segments and to detect which ids a cold-load must fetch.
pub fn list_segment_files(dir: &Path) -> Result<Vec<(u64, PathBuf)>> {
    let mut out = Vec::new();
    if !dir.exists() {
        return Ok(out);
    }
    for entry in fs::read_dir(dir).map_err(|err| EhdbError::Storage(err.to_string()))? {
        let entry = entry.map_err(|err| EhdbError::Storage(err.to_string()))?;
        let name = entry.file_name();
        if let Some(id) = parse_segment_file_name(&name.to_string_lossy()) {
            out.push((id, entry.path()));
        }
    }
    out.sort_unstable_by_key(|(id, _)| *id);
    Ok(out)
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
        let mut reopened = DurableSegmentStore::open_with_segment_size(&root, 128).unwrap();
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
        // The flip does NOT change the file length, so the checkpoint stays
        // "consistent" (length-anchored) and a checkpoint-trust open succeeds
        // WITHOUT scanning the bodies — that is the O(1) append path, which never
        // reads event bodies. Integrity is enforced by replay-is-truth on the
        // first READ: corruption is caught there, never silently served.
        let mut bytes = fs::read(&seg).unwrap();
        let body_byte = FRAME_HEADER_LEN + 2;
        bytes[body_byte] ^= 0xFF;
        fs::write(&seg, &bytes).unwrap();
        // (a) With the checkpoint present, open-for-append does not touch bodies;
        // the CRC error surfaces on the first read (which replays with full CRC).
        let mut trusted = DurableSegmentStore::open(&root).unwrap();
        let err = trusted
            .scan_global(&EventLogScanRequest {
                after: None,
                limit: 10,
            })
            .unwrap_err();
        assert!(matches!(err, EhdbError::Storage(_)), "{err:?}");
        assert!(err.to_string().contains("CRC mismatch"));
        // (b) With no checkpoint (legacy dir / lost sidecar), open falls back to a
        // full replay and the corrupt frame is a hard error at open time.
        fs::remove_file(root.join(CHECKPOINT_FILE)).unwrap();
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
        let mut view = DurableSegmentStore::open_read_only(&root).unwrap();
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

    // -----------------------------------------------------------------------
    // Checkpoint sidecar — O(1) open-for-append (noetl/ehdb#267).
    // -----------------------------------------------------------------------

    #[test]
    fn checkpoint_trust_open_skips_replay_until_first_read() {
        let root = tmp_root("ckpt-lazy");
        {
            let mut store = DurableSegmentStore::open(&root).unwrap();
            append(&mut store, "100", 1, "a");
            append(&mut store, "200", 2, "b");
            append(&mut store, "100", 3, "c");
        }
        // A checkpoint sidecar was written.
        assert!(root.join(CHECKPOINT_FILE).exists());
        // Reopen: the checkpoint is trusted, so the offset index is NOT
        // materialized (the O(1) open — no replay) yet the count is exact.
        let mut reopened = DurableSegmentStore::open(&root).unwrap();
        assert!(
            !reopened.index_loaded,
            "checkpoint-trust open must not replay the offset index"
        );
        assert_eq!(reopened.len(), 3);
        // Appending needs no index and continues the sequence gaplessly.
        assert_eq!(append(&mut reopened, "300", 4, "d"), 4);
        assert!(
            !reopened.index_loaded,
            "append must not force an index rebuild"
        );
        // The first READ lazily materializes the index (replay-is-truth) and
        // sees every event, including the ones appended while lazy.
        let scan = reopened
            .scan_global(&EventLogScanRequest {
                after: None,
                limit: 100,
            })
            .unwrap();
        assert!(reopened.index_loaded);
        assert_eq!(scan.record_count, 4);
        assert_eq!(
            scan.records
                .iter()
                .map(|r| r.global_sequence)
                .collect::<Vec<_>>(),
            vec![1, 2, 3, 4]
        );
        assert_eq!(scan.records[3].payload, "d");
        let _ = fs::remove_dir_all(&root);
    }

    #[test]
    fn checkpoint_open_matches_full_replay_across_many_reopens() {
        // Reopening repeatedly (the per-op worker shape) must never lose or
        // duplicate an event, whether it trusts the checkpoint or replays.
        let root = tmp_root("ckpt-idempotent");
        let n = 50u64;
        for i in 1..=n {
            let mut store = DurableSegmentStore::open(&root).unwrap();
            assert_eq!(store.len() as u64, i - 1);
            append(&mut store, "100", i, &format!("p{i}"));
            // store dropped → next iteration opens fresh (per-op boundary).
        }
        let mut reopened = DurableSegmentStore::open(&root).unwrap();
        let scan = reopened
            .scan_global(&EventLogScanRequest {
                after: None,
                limit: 1000,
            })
            .unwrap();
        assert_eq!(scan.record_count, n as usize);
        // Gapless, no duplicates.
        assert_eq!(
            scan.records
                .iter()
                .map(|r| r.global_sequence)
                .collect::<Vec<_>>(),
            (1..=n).collect::<Vec<_>>()
        );
        let _ = fs::remove_dir_all(&root);
    }

    #[test]
    fn checkpoint_missing_falls_back_to_replay_and_recreates_it() {
        let root = tmp_root("ckpt-missing");
        {
            let mut store = DurableSegmentStore::open(&root).unwrap();
            append(&mut store, "100", 1, "a");
            append(&mut store, "200", 2, "b");
            append(&mut store, "100", 3, "c");
        }
        // Legacy dir / lost sidecar: delete the checkpoint.
        fs::remove_file(root.join(CHECKPOINT_FILE)).unwrap();
        let mut reopened = DurableSegmentStore::open(&root).unwrap();
        // Fell back to a full replay: the index is eagerly loaded and correct.
        assert!(reopened.index_loaded);
        assert_eq!(reopened.len(), 3);
        // And the checkpoint was recreated so the NEXT open is O(1) again.
        assert!(root.join(CHECKPOINT_FILE).exists());
        let scan = reopened
            .scan_global(&EventLogScanRequest {
                after: None,
                limit: 100,
            })
            .unwrap();
        assert_eq!(scan.record_count, 3);
        let _ = fs::remove_dir_all(&root);
    }

    #[test]
    fn stale_checkpoint_is_ignored_in_favor_of_replay() {
        let root = tmp_root("ckpt-stale");
        {
            let mut store = DurableSegmentStore::open(&root).unwrap();
            append(&mut store, "100", 1, "a");
            append(&mut store, "100", 2, "b");
            append(&mut store, "100", 3, "c");
            append(&mut store, "100", 4, "d");
        }
        // Hand-write a STALE checkpoint under-counting the log and naming a wrong
        // active length (simulating a crash between a frame `fsync` and the
        // checkpoint rewrite). The length anchor no longer matches the segment.
        let stale = StoreCheckpoint {
            event_count: 2,
            active_segment_id: 1,
            active_len: 1, // deliberately wrong
            consumers_seen: Vec::new(),
            consumer_acks: HashMap::new(),
        };
        fs::write(
            root.join(CHECKPOINT_FILE),
            serde_json::to_vec(&stale).unwrap(),
        )
        .unwrap();
        // Reopen: consistency check fails (active_len mismatch) → full replay
        // recovers the true count of 4, not the stale 2. Replay-is-truth.
        let mut reopened = DurableSegmentStore::open(&root).unwrap();
        assert!(reopened.index_loaded);
        assert_eq!(reopened.len(), 4);
        // Next append continues from the true tip, no gap, no double-count.
        assert_eq!(append(&mut reopened, "100", 5, "e"), 5);
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
            vec![1, 2, 3, 4, 5]
        );
        let _ = fs::remove_dir_all(&root);
    }

    #[test]
    fn checkpoint_survives_rotation_and_consumer_state() {
        let root = tmp_root("ckpt-rotation");
        // Tiny segments force rotation; the checkpoint's active-segment anchor
        // must track the rotated (highest) segment, and consumer cursors must
        // ride the sidecar so a checkpoint-trust open recovers them.
        {
            let mut store = DurableSegmentStore::open_with_segment_size(&root, 128).unwrap();
            for i in 1..=20 {
                append(&mut store, "100", i, &format!("payload-{i:03}"));
            }
            store
                .ack(&EventLogAckRequest {
                    consumer: "projector".to_string(),
                    transaction_id: "ack".to_string(),
                    sequence: 5,
                })
                .unwrap();
        }
        // Multiple segments exist (rotation happened).
        let segs = fs::read_dir(&root)
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.file_name().to_string_lossy().starts_with(SEGMENT_PREFIX))
            .count();
        assert!(segs > 1, "expected rotation, got {segs} segment(s)");
        // Checkpoint-trust reopen: count + durable cursor survive without replay.
        let mut reopened = DurableSegmentStore::open_with_segment_size(&root, 128).unwrap();
        assert!(!reopened.index_loaded);
        assert_eq!(reopened.len(), 20);
        let tail = reopened
            .tail(&EventLogTailRequest {
                consumer: "projector".to_string(),
                transaction_id: "t".to_string(),
                limit: 100,
            })
            .unwrap();
        assert_eq!(tail.acked_sequence, Some(5));
        assert!(!tail.created_consumer, "consumer-create survived reopen");
        assert_eq!(tail.pending_count, 15);
        let _ = fs::remove_dir_all(&root);
    }
}
