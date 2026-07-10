//! Shared / object-store **segment tier** over the durable event-log backend
//! (completion program, durable event-log backend slice 3;
//! [noetl/ai-meta#254] item 3).
//!
//! Slice 1 ([`crate::durable_eventlog`]) shipped the production disk format and
//! slice 2 ([`crate::durable_eventlog_affinity`]) pinned each shard to a single
//! writer with a **local** cold-load for a non-owner read. But a non-owner's
//! cold-load in slice 2 reads the *non-owner's own local disk* — which, on a
//! different pod with different local storage, is empty. This slice makes the
//! cold-load pull from a **shared durable medium** instead: the owner publishes
//! its segments to a shared store, and a non-owner (or a **new owner that
//! inherited a shard with an empty local disk**) cold-loads those segments from
//! the shared store. That is the runbook §C durability gate's *"durable/shared
//! EHDB log backend beyond `local_reference`"* resolution — a shard survives the
//! loss of the writer's pod-local disk because its segments live on a shared
//! medium.
//!
//! ## Owner publishes; non-owner (and new owner) cold-loads from shared
//!
//! ```text
//!  Replica A (owner shard 0)                 Replica B (owner shard 1)
//!    local <root-a>/shard-0000/  --publish-->  [ shared store ]  <--cold-load--  read of shard 0
//!    (fast path, slice 1 writes)               seg objects, keyed             (B has NO local
//!                                              by (shard, segment_id)          shard-0 bytes)
//! ```
//!
//! * **Owner append** — writes locally (slice-1 fast path, `fsync`'d) *and then*
//!   publishes only the **newly-appended bytes** of the shard's active segment
//!   to the [`SharedSegmentBackend`] (an append-delta, not a whole-segment
//!   re-publish). The local write remains the authority for the owner's own
//!   reads; the shared copy is the durable, cross-replica-readable authority.
//!
//! ## Incremental append-delta publish — O(new-frame-bytes), not O(segment)
//!
//! Publishing is on the **synchronous append path** (a cross-replica reader must
//! see a just-appended event, so publish cannot lag the append), so its per-call
//! cost has to stay flat regardless of how large the active segment has grown.
//! [`SharedTierEventLog::publish_shard`] therefore tracks, per segment, the byte
//! length it has already published and sends the backend only the delta
//! `[published_len .. current_len]` via
//! [`SharedSegmentBackend::append_segment`]. The backend appends those bytes to
//! its shared object in place and re-commits the integrity marker — an O(delta)
//! write, **not** an O(active-segment-size) read-modify-write of the whole
//! segment on every append. (An earlier revision re-read + re-wrote the entire
//! active segment per append, which is O(segment) climbing to the 8 MiB rotation
//! boundary and throttled the durable-backend event path to ~1–2 append/s; see
//! [noetl/ehdb#264]. The reference-driver fallback in the default
//! [`SharedSegmentBackend::append_segment`] is still O(size), but
//! [`FilesystemSharedBackend`] overrides it with the incremental write.)
//!
//! Because the worker constructs this stack **per op** (a stateless boundary — no
//! in-memory state survives between appends), the running content digest is
//! carried on the integrity sidecar itself: each publish persists the resumable
//! [`XxHash64`] state over the committed prefix, and the next publish resumes it
//! and folds in only the delta. Without that persisted state the digest would
//! need an O(committed) re-read of the whole prefix on **every** append — the same
//! O(segment) cost this fix removes. The prefix is re-read exactly once, for a
//! segment written before the state existed. The whole-prefix `digest` string is
//! unchanged, so cold-load verification is identical.
//!
//! Crash-safety across the incremental write is preserved: the segment bytes are
//! appended + `fsync`'d **before** the integrity marker (byte-length + digest +
//! resumable state) is atomically re-committed, so a crash between them leaves an
//! uncommitted tail that a reader ignores (it reads exactly the committed prefix)
//! and the next publish drops + re-appends. The integrity guarantee over the full
//! committed prefix is unchanged.
//! * **Non-owner read** — cold-loads the shard's segments **from the shared
//!   store** into a scratch directory and opens a slice-1
//!   [`DurableSegmentStore::open_read_only`] over them. Replay is byte-identical
//!   and sequence-preserving — the full [`crate::eventlog::EventLogDriver`]
//!   contract holds over the materialized segments because they *are* the
//!   owner's segments.
//! * **New owner inheriting a shard** — [`SharedTierEventLog::hydrate_owned_shard`]
//!   pulls the shard's segments from shared into the owner's *own* local dir
//!   before it serves/appends, so a pod that never held the shard locally
//!   recovers it zero-loss from shared and continues the sequence. This is the
//!   crash-recovery-from-shared path (a pod restart / reschedule onto a fresh
//!   node).
//!
//! ## Digest-addressed, fixed-width segment keys (subject-length-trap-safe)
//!
//! A segment is addressed by **position** — `(shard, segment_id)`, both bounded
//! integers — so [`shared_segment_key`] is a **constant 40-char** key
//! (`noetl.ehdb.seg.<shard:08x>.<segment_id:016x>`). This deliberately avoids
//! the trap the object tier hit ([noetl/ai-meta#234]: hex-encoding an arbitrary
//! platform key into a NATS subject blew the 256-char subject cap) — the key
//! width here is independent of any payload and can never approach a subject
//! limit on whatever medium backs the trait. Each published object carries a
//! byte-length + [`XxHash64`] content digest so a truncated or corrupted shared
//! object is a **hard error** on read, not a silently-shortened replay.
//!
//! Segments are addressed by position rather than by content hash because a
//! segment is **append-mutable** (the active segment grows on every append); a
//! pure content-addressed key would change every append and break listing. The
//! digest is carried as integrity metadata, not as the key.
//!
//! ## Pluggable medium — PVC now, EHDB object tier later
//!
//! [`SharedSegmentBackend`] is the pluggable seam. [`FilesystemSharedBackend`]
//! (a shared directory — a `ReadWriteMany`/`ReadWriteOnce` PVC on kind) is the
//! bootstrapping medium, matching the design note's recommendation. The
//! self-sufficiency end-state routes the same trait to EHDB's own durable object
//! tier (Phase 8 object engine) so the segments live on EHDB itself; that is a
//! later slice and a different backend impl behind this same trait — no change to
//! the routing/hydrate logic here.
//!
//! [noetl/ai-meta#254]: https://github.com/noetl/ehdb/issues/254
//! [noetl/ai-meta#234]: https://github.com/noetl/ai-meta/issues/234
//! [noetl/ehdb#264]: https://github.com/noetl/ehdb/issues/264

use std::{
    collections::HashMap,
    fs::{self, File, OpenOptions},
    hash::Hasher,
    io::{Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

use ehdb_core::{EhdbError, Result};
use serde::{Deserialize, Serialize};
use twox_hash::XxHash64;

use crate::affinity::ShardOwnership;
use crate::durable_eventlog::{
    list_segment_files, segment_file_name, DurableSegmentStore, SegmentGcPolicy,
    DEFAULT_SEGMENT_MAX_BYTES,
};
use crate::durable_eventlog_affinity::{AffinityRead, AffinityRoutedEventLog, Routed, ServedBy};
use crate::eventlog::{
    EventLogAckOutcome, EventLogAckRequest, EventLogAppendOutcome, EventLogAppendRequest,
    EventLogReadExecutionOutcome, EventLogReadExecutionRequest, EventLogScanOutcome,
    EventLogScanRequest, EventLogTailOutcome, EventLogTailRequest,
};

/// Fixed seed for the shared-object content digest (integrity only, not crypto).
const DIGEST_SEED: u64 = 0;

/// The **fixed-width, bounded, secret-free** shared-store object key for one
/// segment. A constant 40 chars — `noetl.ehdb.seg.` (15) + `shard` (8 hex) +
/// `.` (1) + `segment_id` (16 hex) — independent of any payload, so it never
/// approaches a subject-length cap on whatever medium backs
/// [`SharedSegmentBackend`]. See the module docs on why this avoids the object
/// tier's subject-length trap.
pub fn shared_segment_key(shard: u32, segment_id: u64) -> String {
    format!("noetl.ehdb.seg.{shard:08x}.{segment_id:016x}")
}

/// XxHash64 content digest of a segment's bytes, hex-formatted (16 chars). An
/// integrity check for the shared medium (detects bit-rot / partial upload),
/// not a cryptographic guarantee.
pub fn segment_digest(bytes: &[u8]) -> String {
    let mut h = XxHash64::with_seed(DIGEST_SEED);
    h.write(bytes);
    format!("{:016x}", h.finish())
}

/// Outcome of publishing one segment to the shared store: the fixed-width key it
/// landed under, its byte length, and its content digest. Secret-free.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SharedSegmentPutOutcome {
    /// The fixed-width shared-store key ([`shared_segment_key`]).
    pub key: String,
    /// Bytes written.
    pub byte_len: u64,
    /// Content digest ([`segment_digest`]).
    pub digest: String,
    /// Whether the object was newly written vs already present at this
    /// `(len, digest)` (idempotent re-publish of an unchanged segment).
    pub newly_written: bool,
}

/// A pluggable **shared durable medium** the owner publishes segment bytes to
/// and a non-owner (or a new owner with an empty local disk) cold-loads them
/// from. Bootstraps on a filesystem-backed shared directory
/// ([`FilesystemSharedBackend`], a PVC on kind); the same trait plugs to EHDB's
/// own object tier later (the self-sufficiency end-state) — the routing/hydrate
/// logic in [`SharedTierEventLog`] is backend-agnostic.
///
/// Contract:
/// * `put_segment` is **idempotent** — re-publishing the same `(shard,
///   segment_id)` bytes is a no-op write; publishing a grown active segment
///   overwrites atomically (a reader never sees a partial object).
/// * `get_segment` returns **integrity-verified** bytes (byte-length + digest
///   checked) or a hard error on a truncated/corrupt object; `None` for an
///   object never (fully) published.
/// * `list_segment_ids` returns only **committed** segment ids for a shard,
///   ascending.
pub trait SharedSegmentBackend: Send + Sync {
    /// A stable, secret-free identifier for the backing medium.
    fn backend_name(&self) -> &'static str;

    /// Publish (idempotently) one shard segment's bytes under its fixed-width
    /// key. Overwrites the prior object for the same `(shard, segment_id)` when
    /// the active segment has grown.
    fn put_segment(
        &self,
        shard: u32,
        segment_id: u64,
        bytes: &[u8],
    ) -> Result<SharedSegmentPutOutcome>;

    /// Fetch one shard segment's integrity-verified bytes, or `None` when the
    /// object was never (fully) published. A present-but-corrupt/truncated
    /// object is a hard error.
    fn get_segment(&self, shard: u32, segment_id: u64) -> Result<Option<Vec<u8>>>;

    /// List a shard's committed segment ids in ascending order (empty for a
    /// shard never published).
    fn list_segment_ids(&self, shard: u32) -> Result<Vec<u64>>;

    /// Publish only the **newly-appended** bytes of a segment — the append-delta
    /// hot-path publish. `committed_len` is the byte length the caller has
    /// already published (the shared object's current committed length); `delta`
    /// is the new bytes `[committed_len .. committed_len + delta.len()]` from the
    /// owner's local segment. The backend appends `delta` to its shared object
    /// and re-commits the integrity marker for the full prefix
    /// `[0 .. committed_len + delta.len()]`.
    ///
    /// This is the O(delta) path that keeps append flat regardless of the active
    /// segment's size (see the module docs on the O(segment) trap this replaces,
    /// [noetl/ehdb#264]). The default implementation is a **correctness-only,
    /// O(size) fallback** (read the committed prefix, append the delta, republish
    /// the whole object via [`SharedSegmentBackend::put_segment`]); an efficient
    /// backend ([`FilesystemSharedBackend`]) overrides it with an in-place append.
    ///
    /// Contract: appending a `delta` whose `committed_len` does not match the
    /// object's current committed length is a hard error (a lost/torn publish),
    /// not a silent overwrite.
    ///
    /// [noetl/ehdb#264]: https://github.com/noetl/ehdb/issues/264
    fn append_segment(
        &self,
        shard: u32,
        segment_id: u64,
        committed_len: u64,
        delta: &[u8],
    ) -> Result<SharedSegmentPutOutcome> {
        // Generic O(size) fallback: reconstruct the whole object, then republish.
        // Efficient backends override this with an in-place O(delta) append.
        let mut bytes = self.get_segment(shard, segment_id)?.unwrap_or_default();
        if bytes.len() as u64 != committed_len {
            return Err(EhdbError::Storage(format!(
                "append_segment {}: committed_len {committed_len} != current object length {}",
                shared_segment_key(shard, segment_id),
                bytes.len()
            )));
        }
        bytes.extend_from_slice(delta);
        self.put_segment(shard, segment_id, &bytes)
    }

    /// The durably-committed byte length of a segment in the shared store, or
    /// `None` when the segment was never published. The caller uses this to
    /// reconcile its publish cursor after a restart (its in-memory
    /// already-published length is lost). The default reads the whole object;
    /// an efficient backend reads only the committed length from its marker.
    fn committed_len(&self, shard: u32, segment_id: u64) -> Result<Option<u64>> {
        Ok(self
            .get_segment(shard, segment_id)?
            .map(|bytes| bytes.len() as u64))
    }

    /// Publish the shard's **cross-replica reclaim watermark** — the highest
    /// reclaimed `(seq, segment_id)`. Idempotent + **monotonic**: never lowers a
    /// higher existing watermark. Committed *before* any shared segment object is
    /// deleted, so a non-owner cold-load / new-owner hydrate that reads the
    /// watermark skips segments with id `<= segment_id` and therefore never
    /// re-pulls a reclaimed segment or diverges from the owner — even mid-GC.
    ///
    /// The default errors: a backend that cannot durably persist a watermark
    /// cannot support shared-tier GC (the shared tier refuses to reclaim on it),
    /// which is safer than silently losing coherence. The filesystem/PVC backend
    /// overrides it.
    fn put_reclaim_watermark(&self, shard: u32, seq: u64, segment_id: u64) -> Result<()> {
        let _ = (shard, seq, segment_id);
        Err(EhdbError::InvalidState(
            "shared backend does not support a reclaim watermark (shared-tier GC unavailable)"
                .to_string(),
        ))
    }

    /// The shard's cross-replica reclaim watermark `(seq, segment_id)`, or
    /// `(0, 0)` when none was published. The default is `(0, 0)` — a backend
    /// with no watermark support reports "nothing reclaimed", so readers skip
    /// nothing and behave exactly as the pre-GC shared tier.
    fn reclaim_watermark(&self, shard: u32) -> Result<(u64, u64)> {
        let _ = shard;
        Ok((0, 0))
    }

    /// Delete one shared segment object (its bytes + integrity marker).
    /// Idempotent — an absent object is not an error. Used to reclaim shared disk
    /// **after** the watermark is committed; a crash mid-delete leaves an orphan
    /// object readers already skip (a space leak the next GC re-attempts), never
    /// a correctness bug. The default errors (see [`Self::put_reclaim_watermark`]).
    fn delete_segment(&self, shard: u32, segment_id: u64) -> Result<()> {
        let _ = (shard, segment_id);
        Err(EhdbError::InvalidState(
            "shared backend does not support segment deletion (shared-tier GC unavailable)"
                .to_string(),
        ))
    }
}

/// On-disk integrity sidecar committed *after* the segment bytes — its presence
/// marks the object committed (a crash between the bytes rename and the meta
/// rename leaves an uncommitted object the reader treats as absent).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct SharedSegmentMeta {
    byte_len: u64,
    digest: String,
    /// The **resumable** running-digest state over the committed prefix
    /// `[0..byte_len]`. Persisting it is what makes an append-delta publish
    /// O(delta): the worker constructs the shared-tier stack **per op** (a
    /// stateless boundary — no in-memory state survives between appends), so an
    /// [`XxHash64`] carried on the sidecar lets the next publish resume the
    /// digest from `byte_len` and fold in only the new bytes, instead of
    /// re-reading + re-hashing the whole committed prefix every append. Absent on
    /// segments written before this field existed; the first incremental publish
    /// over such a segment re-seeds from the committed prefix once, then persists
    /// the state. Not read on the cold-load path ([`get_segment`] verifies the
    /// whole-prefix `digest` string instead), so it never affects correctness.
    #[serde(default)]
    hasher_state: Option<XxHash64>,
}

/// The shard's cross-replica reclaim watermark object — the highest reclaimed
/// `(seq, segment)`. A non-owner cold-load / new-owner hydrate skips shared
/// segments with id `<= segment`, so this object (committed before any shared
/// segment is deleted) is what keeps readers coherent with the owner through a
/// reclamation. Monotonic: never lowered.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(deny_unknown_fields)]
struct SharedReclaimWatermark {
    seq: u64,
    segment: u64,
}

/// A [`SharedSegmentBackend`] over a shared filesystem directory — the
/// bootstrapping medium (a PVC on kind, per the design note). Segment objects
/// live flat under `<root>/<key>` with a `<key>.meta` integrity sidecar written
/// last (the commit marker). Publishing is atomic via temp-file + rename so a
/// concurrent reader never observes a partial object.
#[derive(Debug, Clone)]
pub struct FilesystemSharedBackend {
    root: PathBuf,
}

impl FilesystemSharedBackend {
    /// Open (creating the directory) a filesystem-backed shared store rooted at
    /// `root`.
    pub fn open(root: impl Into<PathBuf>) -> Result<Self> {
        let root = root.into();
        fs::create_dir_all(&root).map_err(|err| EhdbError::Storage(err.to_string()))?;
        Ok(Self { root })
    }

    /// The directory backing the shared store.
    pub fn root(&self) -> &Path {
        &self.root
    }

    fn object_path(&self, shard: u32, segment_id: u64) -> PathBuf {
        self.root.join(shared_segment_key(shard, segment_id))
    }

    fn meta_path(&self, shard: u32, segment_id: u64) -> PathBuf {
        self.root
            .join(format!("{}.meta", shared_segment_key(shard, segment_id)))
    }

    /// The shard's reclaim-watermark object path (a fixed-width, bounded key like
    /// the segment keys). One per shard.
    fn watermark_path(&self, shard: u32) -> PathBuf {
        self.root.join(format!("noetl.ehdb.rw.{shard:08x}"))
    }

    /// Write `bytes` to `path` via a sibling temp file + rename (atomic publish).
    fn atomic_write(path: &Path, bytes: &[u8]) -> Result<()> {
        let tmp = path.with_extension("tmp");
        {
            let mut file = OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .open(&tmp)
                .map_err(|err| EhdbError::Storage(err.to_string()))?;
            file.write_all(bytes)
                .map_err(|err| EhdbError::Storage(err.to_string()))?;
            file.sync_all()
                .map_err(|err| EhdbError::Storage(err.to_string()))?;
        }
        fs::rename(&tmp, path).map_err(|err| EhdbError::Storage(err.to_string()))?;
        Ok(())
    }

    /// Append only `delta` (the newly-appended bytes) to the shared object for
    /// `(shard, segment_id)` in place, re-committing the integrity marker for the
    /// full prefix — the O(delta) incremental publish backing
    /// [`SharedSegmentBackend::append_segment`]. `committed_len` is the object's
    /// current committed length (the caller's already-published cursor); an
    /// uncommitted tail from a prior interrupted append is dropped first.
    ///
    /// The running digest is **resumed from the persisted sidecar state**
    /// ([`SharedSegmentMeta::hasher_state`]) — the worker builds this stack per
    /// op, so nothing survives in memory between appends; without the persisted
    /// state the digest would need an O(committed) re-read of the whole prefix
    /// every append (the very cost this fix removes). The prefix is re-read only
    /// when the state is absent (a segment written before it was persisted) —
    /// once, after which the state carries forward.
    ///
    /// Crash-safety: the delta bytes are appended + `fsync`'d, then the marker
    /// (byte-length + digest + state) is atomically re-committed. A crash between
    /// leaves an uncommitted tail a reader ignores; the next call drops +
    /// re-appends it.
    fn append_segment_impl(
        &self,
        shard: u32,
        segment_id: u64,
        committed_len: u64,
        delta: &[u8],
    ) -> Result<SharedSegmentPutOutcome> {
        let key = shared_segment_key(shard, segment_id);
        let object_path = self.object_path(shard, segment_id);
        let meta_path = self.meta_path(shard, segment_id);
        let total_len = committed_len + delta.len() as u64;

        // --- Resume the running digest from the persisted sidecar state. ------
        // Use the persisted hasher when it matches our publish cursor; otherwise
        // (absent state on a pre-existing segment, or a cursor mismatch) re-seed
        // once by hashing the committed prefix — an O(committed) cost paid at
        // most once per such segment, then carried forward on the sidecar.
        let prev_meta = read_meta(&meta_path)?;
        let resumable = prev_meta
            .as_ref()
            .filter(|m| m.byte_len == committed_len)
            .and_then(|m| m.hasher_state);
        let mut hasher = match resumable {
            Some(state) => state,
            None => {
                let mut h = XxHash64::with_seed(DIGEST_SEED);
                if committed_len > 0 {
                    let prefix = read_object_prefix(&object_path, committed_len)?;
                    h.write(&prefix);
                }
                h
            }
        };

        // --- Reconcile the object length to `committed_len`, then append. -----
        let cur_obj_len = object_len(&object_path)?;
        if committed_len == 0 {
            // Fresh (or reset): (re)create the object with just the delta bytes.
            let mut file = OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .open(&object_path)
                .map_err(|err| EhdbError::Storage(err.to_string()))?;
            file.write_all(delta)
                .map_err(|err| EhdbError::Storage(err.to_string()))?;
            file.sync_data()
                .map_err(|err| EhdbError::Storage(err.to_string()))?;
        } else {
            if cur_obj_len < committed_len {
                return Err(EhdbError::Storage(format!(
                    "append_segment {key}: object length {cur_obj_len} < committed {committed_len} (lost/torn shared object)"
                )));
            }
            if cur_obj_len > committed_len {
                // Drop an uncommitted tail from a prior interrupted append.
                truncate_object(&object_path, committed_len)?;
            }
            if !delta.is_empty() {
                let mut file = OpenOptions::new()
                    .append(true)
                    .open(&object_path)
                    .map_err(|err| EhdbError::Storage(err.to_string()))?;
                file.write_all(delta)
                    .map_err(|err| EhdbError::Storage(err.to_string()))?;
                file.sync_data()
                    .map_err(|err| EhdbError::Storage(err.to_string()))?;
            }
        }

        // Fold the delta into the digest (`finish` does not consume the hasher,
        // so it carries forward as the resumable state), then commit last.
        hasher.write(delta);
        let digest = format!("{:016x}", hasher.finish());
        let meta = serde_json::to_vec(&SharedSegmentMeta {
            byte_len: total_len,
            digest: digest.clone(),
            hasher_state: Some(hasher),
        })
        .map_err(|err| EhdbError::Storage(format!("encode shared segment meta: {err}")))?;
        Self::atomic_write(&meta_path, &meta)?;

        Ok(SharedSegmentPutOutcome {
            key,
            byte_len: total_len,
            digest,
            newly_written: !delta.is_empty(),
        })
    }
}

impl SharedSegmentBackend for FilesystemSharedBackend {
    fn backend_name(&self) -> &'static str {
        "ehdb-shared-filesystem"
    }

    fn put_segment(
        &self,
        shard: u32,
        segment_id: u64,
        bytes: &[u8],
    ) -> Result<SharedSegmentPutOutcome> {
        let key = shared_segment_key(shard, segment_id);
        let byte_len = bytes.len() as u64;
        // Compute the digest via the running hasher so the resumable state can be
        // persisted alongside it (a later incremental append resumes from here).
        let mut hasher = XxHash64::with_seed(DIGEST_SEED);
        hasher.write(bytes);
        let digest = format!("{:016x}", hasher.finish());

        // Idempotent skip: if a committed object at this key already matches this
        // (len, digest), the segment is unchanged — no rewrite.
        let meta_path = self.meta_path(shard, segment_id);
        if let Some(existing) = read_meta(&meta_path)? {
            if existing.byte_len == byte_len && existing.digest == digest {
                return Ok(SharedSegmentPutOutcome {
                    key,
                    byte_len,
                    digest,
                    newly_written: false,
                });
            }
        }

        // Bytes first, meta (the commit marker) last.
        Self::atomic_write(&self.object_path(shard, segment_id), bytes)?;
        let meta = serde_json::to_vec(&SharedSegmentMeta {
            byte_len,
            digest: digest.clone(),
            hasher_state: Some(hasher),
        })
        .map_err(|err| EhdbError::Storage(format!("encode shared segment meta: {err}")))?;
        Self::atomic_write(&meta_path, &meta)?;

        Ok(SharedSegmentPutOutcome {
            key,
            byte_len,
            digest,
            newly_written: true,
        })
    }

    fn get_segment(&self, shard: u32, segment_id: u64) -> Result<Option<Vec<u8>>> {
        let meta_path = self.meta_path(shard, segment_id);
        // No meta == not (fully) committed == absent.
        let Some(meta) = read_meta(&meta_path)? else {
            return Ok(None);
        };
        let object_path = self.object_path(shard, segment_id);
        // Read exactly the committed prefix `[0 .. meta.byte_len]`. The object may
        // carry an uncommitted tail (a crash mid-incremental-append wrote delta
        // bytes but the marker still names the prior length) — that tail is not
        // yet committed, so a reader ignores it. An object *shorter* than the
        // committed length is genuine truncation/corruption of the shared medium.
        let mut file = match File::open(&object_path) {
            Ok(file) => file,
            // Meta present but bytes gone is corruption of the shared medium.
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                return Err(EhdbError::Storage(format!(
                    "shared segment {}: meta present but object bytes missing",
                    shared_segment_key(shard, segment_id)
                )));
            }
            Err(err) => return Err(EhdbError::Storage(err.to_string())),
        };
        let obj_len = file
            .metadata()
            .map_err(|err| EhdbError::Storage(err.to_string()))?
            .len();
        if obj_len < meta.byte_len {
            return Err(EhdbError::Storage(format!(
                "shared segment {}: length {} < committed {} (truncated/corrupt)",
                shared_segment_key(shard, segment_id),
                obj_len,
                meta.byte_len
            )));
        }
        let mut bytes = vec![0u8; meta.byte_len as usize];
        file.read_exact(&mut bytes)
            .map_err(|err| EhdbError::Storage(err.to_string()))?;
        if segment_digest(&bytes) != meta.digest {
            return Err(EhdbError::Storage(format!(
                "shared segment {}: digest mismatch (bit-rot)",
                shared_segment_key(shard, segment_id)
            )));
        }
        Ok(Some(bytes))
    }

    fn list_segment_ids(&self, shard: u32) -> Result<Vec<u64>> {
        let mut ids = Vec::new();
        if !self.root.exists() {
            return Ok(ids);
        }
        let prefix = format!("noetl.ehdb.seg.{shard:08x}.");
        for entry in fs::read_dir(&self.root).map_err(|err| EhdbError::Storage(err.to_string()))? {
            let entry = entry.map_err(|err| EhdbError::Storage(err.to_string()))?;
            let name = entry.file_name();
            let name = name.to_string_lossy();
            // A committed segment is the `.meta` sidecar (written last); the bare
            // object without meta is an in-progress publish, skipped.
            if let Some(rest) = name.strip_suffix(".meta") {
                if let Some(hex) = rest.strip_prefix(&prefix) {
                    if let Ok(id) = u64::from_str_radix(hex, 16) {
                        ids.push(id);
                    }
                }
            }
        }
        ids.sort_unstable();
        Ok(ids)
    }

    /// Efficient O(delta) override — append only the new bytes in place instead
    /// of the O(size) reconstruct-and-republish default.
    fn append_segment(
        &self,
        shard: u32,
        segment_id: u64,
        committed_len: u64,
        delta: &[u8],
    ) -> Result<SharedSegmentPutOutcome> {
        self.append_segment_impl(shard, segment_id, committed_len, delta)
    }

    /// Read the committed length straight from the integrity marker (no object
    /// read), so a caller can reconcile its publish cursor cheaply after restart.
    fn committed_len(&self, shard: u32, segment_id: u64) -> Result<Option<u64>> {
        Ok(read_meta(&self.meta_path(shard, segment_id))?.map(|meta| meta.byte_len))
    }

    fn put_reclaim_watermark(&self, shard: u32, seq: u64, segment_id: u64) -> Result<()> {
        let path = self.watermark_path(shard);
        // Monotonic: never lower an existing higher watermark (a stale / retried
        // GC must not resurrect already-skipped segments for readers).
        let current = read_watermark(&path)?.unwrap_or_default();
        if segment_id <= current.segment && seq <= current.seq {
            return Ok(());
        }
        let next = SharedReclaimWatermark {
            seq: seq.max(current.seq),
            segment: segment_id.max(current.segment),
        };
        let bytes = serde_json::to_vec(&next)
            .map_err(|err| EhdbError::Storage(format!("encode reclaim watermark: {err}")))?;
        Self::atomic_write(&path, &bytes)
    }

    fn reclaim_watermark(&self, shard: u32) -> Result<(u64, u64)> {
        Ok(read_watermark(&self.watermark_path(shard))?
            .map(|w| (w.seq, w.segment))
            .unwrap_or((0, 0)))
    }

    fn delete_segment(&self, shard: u32, segment_id: u64) -> Result<()> {
        // Remove the integrity marker FIRST so a crash between the two unlinks
        // leaves an object with no `.meta` — which `list_segment_ids` / `get_segment`
        // already treat as absent (an in-progress / removed object), never a
        // half-present segment.
        for path in [
            self.meta_path(shard, segment_id),
            self.object_path(shard, segment_id),
        ] {
            match fs::remove_file(&path) {
                Ok(()) => {}
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
                Err(err) => return Err(EhdbError::Storage(err.to_string())),
            }
        }
        Ok(())
    }
}

/// The current on-disk length of a shared object, or `0` when it does not exist.
fn object_len(path: &Path) -> Result<u64> {
    match fs::metadata(path) {
        Ok(meta) => Ok(meta.len()),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(0),
        Err(err) => Err(EhdbError::Storage(err.to_string())),
    }
}

/// Read exactly the first `len` bytes of a shared object (the committed prefix),
/// used to re-seed the running digest after a cache miss.
fn read_object_prefix(path: &Path, len: u64) -> Result<Vec<u8>> {
    let mut file = File::open(path).map_err(|err| EhdbError::Storage(err.to_string()))?;
    let mut buf = vec![0u8; len as usize];
    file.read_exact(&mut buf)
        .map_err(|err| EhdbError::Storage(err.to_string()))?;
    Ok(buf)
}

/// Truncate a shared object to `len` bytes (drops an uncommitted tail from a
/// prior interrupted incremental append) and `fsync`.
fn truncate_object(path: &Path, len: u64) -> Result<()> {
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

/// Read `len` bytes starting at `from` from a local segment file — the
/// append-delta the incremental publish sends to the shared store. Reads only
/// the delta window, never the whole segment (the O(segment) trap this avoids).
fn read_segment_delta(path: &Path, from: u64, len: u64) -> Result<Vec<u8>> {
    let mut file = File::open(path).map_err(|err| EhdbError::Storage(err.to_string()))?;
    file.seek(SeekFrom::Start(from))
        .map_err(|err| EhdbError::Storage(err.to_string()))?;
    let mut buf = vec![0u8; len as usize];
    file.read_exact(&mut buf)
        .map_err(|err| EhdbError::Storage(err.to_string()))?;
    Ok(buf)
}

/// Read + decode a shard's reclaim-watermark object, or `None` when absent. A
/// decode error is treated as absent (fail-safe: no watermark ⇒ readers skip
/// nothing ⇒ pre-GC behavior), never a hard error.
fn read_watermark(path: &Path) -> Result<Option<SharedReclaimWatermark>> {
    match fs::read(path) {
        Ok(bytes) => Ok(serde_json::from_slice(&bytes).ok()),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(err) => Err(EhdbError::Storage(err.to_string())),
    }
}

/// Read + decode a segment's integrity sidecar, or `None` when absent.
fn read_meta(meta_path: &Path) -> Result<Option<SharedSegmentMeta>> {
    match fs::read(meta_path) {
        Ok(bytes) => {
            let meta: SharedSegmentMeta = serde_json::from_slice(&bytes)
                .map_err(|err| EhdbError::Storage(format!("decode shared segment meta: {err}")))?;
            Ok(Some(meta))
        }
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(err) => Err(EhdbError::Storage(err.to_string())),
    }
}

/// Outcome of publishing an owned shard's segments to the shared store.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ShardPublishOutcome {
    /// The shard published.
    pub shard: u32,
    /// Segments present locally for the shard.
    pub local_segments: usize,
    /// Segments actually (re-)written to the shared store this call (unchanged
    /// sealed segments are skipped).
    pub published: usize,
}

/// Outcome of hydrating an owned shard's segments from the shared store into the
/// owner's local dir (the new-owner crash-recovery-from-shared path).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ShardHydrateOutcome {
    /// The shard hydrated.
    pub shard: u32,
    /// Segments in the shared store for the shard.
    pub shared_segments: usize,
    /// Segments materialized into the local dir this call (0 when the local dir
    /// already held segments — hydrate only fills an empty local shard).
    pub materialized: usize,
    /// Whether the local shard dir was empty before hydrate (a genuine
    /// inherit-from-shared vs a no-op on an already-resident owner).
    pub was_empty: bool,
}

/// Execution-affinity single-writer router with a **shared segment tier**: the
/// owner writes locally (slice-1 fast path via the slice-2
/// [`AffinityRoutedEventLog`]) *and* publishes its segments to a
/// [`SharedSegmentBackend`]; a non-owner (or a new owner inheriting a shard)
/// cold-loads / hydrates the segments **from the shared store**.
///
/// One instance models **one replica**. Its [`ShardOwnership`] decides which
/// shards it writes (and publishes) and which it must cold-load from shared.
pub struct SharedTierEventLog {
    /// Local owner fast path (slice 2) — reused verbatim for owned writes/reads.
    local: AffinityRoutedEventLog,
    /// The shared durable medium every owner publishes to and non-owners read.
    shared: Arc<dyn SharedSegmentBackend>,
    /// Scratch root under which non-owner cold-loads materialize shared
    /// segments (per-shard subdirs, rebuilt each cold-load).
    coldload_root: PathBuf,
    /// Per-shard segment rollover threshold (matches the local stores so replay
    /// classifies frames identically).
    segment_max_bytes: u64,
    /// Per-segment already-published byte length: `(shard, segment_id) ->
    /// published_len`. The publish cursor for the **incremental** append-delta
    /// path — each publish sends the backend only `[published_len .. current_len]`
    /// and advances the cursor, so an unchanged sealed segment is skipped and the
    /// growing active segment costs O(delta), not O(segment). Rebuilt from the
    /// backend's committed length on a cache miss (a restart clears it).
    published: Mutex<HashMap<(u32, u64), u64>>,
}

impl SharedTierEventLog {
    /// Open a shared-tier router for one replica.
    ///
    /// * `local_root` — this replica's local per-shard store root (owned-shard
    ///   fast path + hydrate target).
    /// * `ownership` — which shards this replica writes.
    /// * `shared` — the shared durable medium.
    /// * `coldload_root` — scratch root for materializing non-owner cold-loads
    ///   (kept separate from `local_root` so a cold-load never pollutes an owned
    ///   store).
    pub fn open(
        local_root: impl Into<PathBuf>,
        ownership: ShardOwnership,
        shared: Arc<dyn SharedSegmentBackend>,
        coldload_root: impl Into<PathBuf>,
    ) -> Result<Self> {
        Self::open_with_segment_size(
            local_root,
            ownership,
            shared,
            coldload_root,
            DEFAULT_SEGMENT_MAX_BYTES,
        )
    }

    /// Open with an explicit per-shard segment rollover threshold (tests force
    /// small segments to exercise multi-segment publish/cold-load).
    pub fn open_with_segment_size(
        local_root: impl Into<PathBuf>,
        ownership: ShardOwnership,
        shared: Arc<dyn SharedSegmentBackend>,
        coldload_root: impl Into<PathBuf>,
        segment_max_bytes: u64,
    ) -> Result<Self> {
        let local = AffinityRoutedEventLog::open_with_segment_size(
            local_root,
            ownership,
            segment_max_bytes,
        )?;
        Ok(Self {
            local,
            shared,
            coldload_root: coldload_root.into(),
            segment_max_bytes,
            published: Mutex::new(HashMap::new()),
        })
    }

    /// This replica's ownership.
    pub fn ownership(&self) -> ShardOwnership {
        self.local.ownership()
    }

    /// The underlying affinity-routed local store (owned-shard fast path). Exposed
    /// so a caller can drive per-owned-shard GC planning / reconciliation.
    pub fn local(&self) -> &AffinityRoutedEventLog {
        &self.local
    }

    /// The shared medium's name.
    pub fn shared_backend_name(&self) -> &'static str {
        self.shared.backend_name()
    }

    /// The shard that owns `execution_id`.
    pub fn shard_of(&self, execution_id: &str) -> u32 {
        self.local.shard_of(execution_id)
    }

    /// Publish an owned shard's local segments to the shared store
    /// **incrementally** — each segment sends only the bytes appended since the
    /// last publish (`[published_len .. current_len]`), not the whole segment. A
    /// sealed segment already published at its final length is skipped; the
    /// active (growing) segment costs O(delta), so this stays flat regardless of
    /// how large the active segment has grown (the fix for [noetl/ehdb#264]).
    /// Caller must own `shard`.
    ///
    /// The per-segment length is compared via a cheap `fs::metadata` — the whole
    /// segment is never read on the append hot path.
    pub fn publish_shard(&self, shard: u32) -> Result<ShardPublishOutcome> {
        let local_dir = self.local.shard_dir(shard);
        let segments = list_segment_files(&local_dir)?;
        let local_segments = segments.len();
        let mut published = 0usize;
        let mut map = self.published.lock().map_err(|_| {
            EhdbError::InvalidState("shared-tier published lock poisoned".to_string())
        })?;
        for (segment_id, path) in segments {
            let cur_len = fs::metadata(&path)
                .map_err(|err| EhdbError::Storage(err.to_string()))?
                .len();
            // Where we last left off. On a cache miss (restart) reconcile against
            // the shared store's own committed length so we never double-write.
            let published_len = match map.get(&(shard, segment_id)) {
                Some(&len) => len,
                None => self.shared.committed_len(shard, segment_id)?.unwrap_or(0),
            };
            if cur_len <= published_len {
                // Sealed + unchanged (or already fully published) — nothing new.
                map.insert((shard, segment_id), published_len);
                continue;
            }
            // Read only the delta `[published_len .. cur_len]` — O(delta), not
            // O(segment) — and hand it to the backend's incremental append.
            let delta = read_segment_delta(&path, published_len, cur_len - published_len)?;
            self.shared
                .append_segment(shard, segment_id, published_len, &delta)?;
            map.insert((shard, segment_id), cur_len);
            published += 1;
        }
        Ok(ShardPublishOutcome {
            shard,
            local_segments,
            published,
        })
    }

    /// The shard's shared segment ids that are still **retained** — i.e. above
    /// the cross-replica reclaim watermark (`> watermark.segment`). A cold-load /
    /// hydrate materializes only these, so a reader never re-pulls a reclaimed
    /// segment and diverges from the owner — even in the crash window where the
    /// watermark is committed but the shared objects are not yet deleted. The
    /// base offset falls out of the first surviving frame on replay.
    fn retained_shared_ids(&self, shard: u32) -> Result<Vec<u64>> {
        let (_, wm_segment) = self.shared.reclaim_watermark(shard)?;
        let mut ids = self.shared.list_segment_ids(shard)?;
        if wm_segment > 0 {
            ids.retain(|&id| id > wm_segment);
        }
        Ok(ids)
    }

    /// Materialize a shard's shared segments into a fresh directory and return a
    /// read-only [`DurableSegmentStore`] over them — the cold-load-from-shared
    /// primitive. `dest` is cleared first so a stale prior materialization can
    /// never leak. A shard with no (retained) shared segments yields an empty
    /// read-only store (the shared-store-miss case — not an error). Segments at
    /// or below the reclaim watermark are skipped ([`Self::retained_shared_ids`]).
    fn materialize_from_shared(&self, shard: u32, dest: &Path) -> Result<DurableSegmentStore> {
        // Clear + recreate the destination so we never mix a stale cold-load.
        if dest.exists() {
            fs::remove_dir_all(dest).map_err(|err| EhdbError::Storage(err.to_string()))?;
        }
        let ids = self.retained_shared_ids(shard)?;
        if ids.is_empty() {
            // Shared-store miss: no segments for this shard. Open read-only over
            // the (absent) dir → an empty log, not an error.
            return DurableSegmentStore::open_read_only_with_segment_size(
                dest.to_path_buf(),
                self.segment_max_bytes,
            );
        }
        fs::create_dir_all(dest).map_err(|err| EhdbError::Storage(err.to_string()))?;
        for id in ids {
            let bytes = self.shared.get_segment(shard, id)?.ok_or_else(|| {
                EhdbError::Storage(format!(
                    "shared segment {} listed but absent on fetch",
                    shared_segment_key(shard, id)
                ))
            })?;
            let path = dest.join(segment_file_name(id));
            fs::write(&path, &bytes).map_err(|err| EhdbError::Storage(err.to_string()))?;
        }
        DurableSegmentStore::open_read_only_with_segment_size(
            dest.to_path_buf(),
            self.segment_max_bytes,
        )
    }

    /// Cold-load a read-only view of a shard from the shared store (a non-owner
    /// read). Materializes into `<coldload_root>/shard-<NNNN>/`.
    fn cold_load(&self, shard: u32) -> Result<DurableSegmentStore> {
        let dest = self.coldload_root.join(format!("shard-{shard:04}"));
        self.materialize_from_shared(shard, &dest)
    }

    /// Hydrate an **owned** shard from the shared store into this replica's local
    /// dir — the new-owner crash-recovery-from-shared path. When the local shard
    /// dir is empty (a pod that never held the shard), the shared segments are
    /// materialized into it so the owner opens a fully-recovered writable store
    /// and continues the sequence. When the local dir already holds segments,
    /// this is a no-op (the resident owner is authoritative for its own writes;
    /// full reconciliation of a divergent local vs shared is out of scope for
    /// this slice — see the module docs / issue #254).
    ///
    /// Must be called **before** the first owned read/append for the shard (so
    /// the lazy owned-store open replays the hydrated segments). Errors if this
    /// replica does not own `shard`.
    pub fn hydrate_owned_shard(&self, shard: u32) -> Result<ShardHydrateOutcome> {
        if !self.ownership().owns_shard(shard) {
            return Err(EhdbError::InvalidState(format!(
                "cannot hydrate shard {shard}: not owned by this replica (shard_index {})",
                self.ownership().shard_index()
            )));
        }
        let local_dir = self.local.shard_dir(shard);
        let local_segments = list_segment_files(&local_dir)?;
        let was_empty = local_segments.is_empty();
        // Hydrate only the RETAINED shared segments (above the reclaim watermark);
        // a reclaimed segment is never pulled back, so the inheriting owner's base
        // offset matches the reclaiming owner's — no un-reclaim on transfer.
        let shared_ids = self.retained_shared_ids(shard)?;
        let mut materialized = 0usize;
        if was_empty && !shared_ids.is_empty() {
            fs::create_dir_all(&local_dir).map_err(|err| EhdbError::Storage(err.to_string()))?;
            for id in &shared_ids {
                let bytes = self.shared.get_segment(shard, *id)?.ok_or_else(|| {
                    EhdbError::Storage(format!(
                        "shared segment {} listed but absent on hydrate",
                        shared_segment_key(shard, *id)
                    ))
                })?;
                let path = local_dir.join(segment_file_name(*id));
                fs::write(&path, &bytes).map_err(|err| EhdbError::Storage(err.to_string()))?;
                // Seed the published map so we don't redundantly re-publish
                // exactly what we just pulled (idempotent even if we did).
                self.published
                    .lock()
                    .map_err(|_| {
                        EhdbError::InvalidState("shared-tier published lock poisoned".to_string())
                    })?
                    .insert((shard, *id), bytes.len() as u64);
                materialized += 1;
            }
        }
        Ok(ShardHydrateOutcome {
            shard,
            shared_segments: shared_ids.len(),
            materialized,
            was_empty,
        })
    }

    /// Append one authorized event, routed to its owning shard. The owner writes
    /// locally then publishes the shard's **newly-appended bytes** to shared
    /// (incremental append-delta, O(delta) — see [`Self::publish_shard`]); a
    /// non-owner is refused with no side effect ([`Routed::NotOwner`]) so the
    /// caller re-routes to the owner.
    pub fn append(&self, request: &EventLogAppendRequest) -> Result<Routed<EventLogAppendOutcome>> {
        match self.local.append(request)? {
            Routed::Served(outcome) => {
                let shard = self.shard_of(&request.execution_id);
                self.publish_shard(shard)?;
                Ok(Routed::Served(outcome))
            }
            Routed::NotOwner { owner_shard } => Ok(Routed::NotOwner { owner_shard }),
        }
    }

    /// Ordered per-execution read, routed to the execution's shard. The owner
    /// serves it resident from local; a non-owner cold-loads the shard's
    /// segments **from the shared store** read-only.
    pub fn read_execution(
        &self,
        request: &EventLogReadExecutionRequest,
    ) -> Result<AffinityRead<EventLogReadExecutionOutcome>> {
        let shard = self.shard_of(&request.execution_id);
        if self.ownership().owns_shard(shard) {
            self.local.read_execution(request)
        } else {
            let mut view = self.cold_load(shard)?;
            Ok(AffinityRead {
                served_by: ServedBy::NonOwnerColdLoad,
                outcome: view.read_execution(request)?,
            })
        }
    }

    /// Ordered global scan of one shard's stream. The owner serves it resident;
    /// a non-owner cold-loads read-only from the shared store.
    pub fn scan_shard(
        &self,
        shard: u32,
        request: &EventLogScanRequest,
    ) -> Result<AffinityRead<EventLogScanOutcome>> {
        if self.ownership().owns_shard(shard) {
            self.local.scan_shard(shard, request)
        } else {
            let mut view = self.cold_load(shard)?;
            Ok(AffinityRead {
                served_by: ServedBy::NonOwnerColdLoad,
                outcome: view.scan_global(request)?,
            })
        }
    }

    /// Durable-consumer tail pull on one shard's stream. Owner-only writer state
    /// (create-on-first-pull persists a frame + is published to shared so a new
    /// owner inherits the cursor); a non-owner is refused.
    pub fn tail(
        &self,
        shard: u32,
        request: &EventLogTailRequest,
    ) -> Result<Routed<EventLogTailOutcome>> {
        match self.local.tail(shard, request)? {
            Routed::Served(outcome) => {
                // A first-pull consumer-create wrote a frame — publish so the
                // durable cursor survives cross-replica.
                if outcome.created_consumer {
                    self.publish_shard(shard)?;
                }
                Ok(Routed::Served(outcome))
            }
            Routed::NotOwner { owner_shard } => Ok(Routed::NotOwner { owner_shard }),
        }
    }

    /// Advance a durable consumer's ack cursor on one shard's stream, then
    /// publish so the persisted cursor survives cross-replica. Owner-only; a
    /// non-owner is refused.
    pub fn ack(
        &self,
        shard: u32,
        request: &EventLogAckRequest,
    ) -> Result<Routed<EventLogAckOutcome>> {
        match self.local.ack(shard, request)? {
            Routed::Served(outcome) => {
                self.publish_shard(shard)?;
                Ok(Routed::Served(outcome))
            }
            Routed::NotOwner { owner_shard } => Ok(Routed::NotOwner { owner_shard }),
        }
    }

    /// Adopt the shard's published cross-replica reclaim watermark into this
    /// **owned** replica's local store — reclaiming any local segment at/below it.
    /// This is the crash-window recovery: if a prior `reclaim_shard` committed the
    /// shared watermark but crashed before the local unlink (or before this owner
    /// restarted), the owner's local segments are ahead of the watermark and it
    /// would over-serve reclaimed sequences that non-owners already skip. Calling
    /// this on bring-up realigns the owner with the shared watermark so all
    /// replicas agree. Idempotent; a no-op when there is no watermark or the local
    /// store is already at/below it. Errors if this replica does not own `shard`.
    pub fn reconcile_owned_shard(&self, shard: u32) -> Result<u64> {
        let driver = self.local.owned_driver(shard)?;
        let (_, wm_segment) = self.shared.reclaim_watermark(shard)?;
        if wm_segment > 0 {
            driver.reclaim_to_segment(wm_segment)?;
        }
        Ok(wm_segment)
    }

    /// Reclaim consumed sealed segments for an **owned** shard across BOTH the
    /// local store AND the shared tier — coherently (segment GC for the
    /// shared-medium topology). Owner-only; a non-owner is refused
    /// ([`Routed::NotOwner`]).
    ///
    /// The order is the crux (watermark-first):
    ///
    /// 1. **Reconcile** — adopt any already-published watermark locally first, so
    ///    the plan is computed against a store aligned with the shared state.
    /// 2. **Plan** the reclaim boundary from the owner's local consumer cursors
    ///    ([`DurableEventLogDriver::plan_reclaim_boundary`]) — no mutation yet.
    /// 3. **Commit the shared watermark** ([`SharedSegmentBackend::put_reclaim_watermark`])
    ///    — the cross-replica point of no return. From here every cold-load /
    ///    hydrate skips segments `<= watermark.segment`, so a reader can never
    ///    re-pull a segment this reclamation is about to remove.
    /// 4. **Reclaim locally** to the boundary ([`DurableEventLogDriver::reclaim_to_segment`]),
    ///    which write-forwards consumer state + `fsync`s the local manifest +
    ///    unlinks the local segments.
    /// 5. **Delete the shared objects** `<= watermark.segment`.
    ///
    /// A crash after (3) leaves readers skipping the watermark while some shared /
    /// local objects linger — a bounded space leak the next `reclaim_shard`
    /// re-attempts, never a divergence (readers already skip them; the owner
    /// realigns via step 1 on its next run / bring-up).
    pub fn reclaim_shard(
        &self,
        shard: u32,
        policy: &SegmentGcPolicy,
    ) -> Result<Routed<SharedShardGcOutcome>> {
        if !self.ownership().owns_shard(shard) {
            return Ok(Routed::NotOwner { owner_shard: shard });
        }
        let mut outcome = SharedShardGcOutcome {
            shard,
            reclaim_watermark_seq: 0,
            reclaimed_through_segment: 0,
            local_segments_reclaimed: 0,
            shared_objects_deleted: 0,
            note: None,
        };
        if !policy.enabled {
            outcome.note = Some("segment GC disabled".to_string());
            return Ok(Routed::Served(outcome));
        }
        let driver = self.local.owned_driver(shard)?;

        // (1) Reconcile local to any already-committed watermark.
        self.reconcile_owned_shard(shard)?;

        // (2) Plan the (new) boundary from the owner's local consumers.
        let Some((seq, segment)) = driver.plan_reclaim_boundary(policy)? else {
            outcome.note =
                Some("nothing reclaimable (no consumer interest past the floor)".to_string());
            return Ok(Routed::Served(outcome));
        };

        // (3) COMMIT the shared watermark FIRST — readers now skip <= segment.
        self.shared.put_reclaim_watermark(shard, seq, segment)?;
        outcome.reclaim_watermark_seq = seq;
        outcome.reclaimed_through_segment = segment;

        // (4) Reclaim locally to the boundary (write-forward + manifest + unlink).
        let local = driver.reclaim_to_segment(segment)?;
        outcome.local_segments_reclaimed = local.segments_reclaimed;

        // (5) Delete the shared objects at/below the watermark (idempotent; a
        //     crash here leaves orphans readers already skip).
        let mut deleted = 0usize;
        for id in self.shared.list_segment_ids(shard)? {
            if id <= segment {
                self.shared.delete_segment(shard, id)?;
                deleted += 1;
            }
        }
        outcome.shared_objects_deleted = deleted;
        Ok(Routed::Served(outcome))
    }
}

/// Secret-free outcome of a shared-tier reclamation for one owned shard — counts
/// + the committed watermark, no payloads.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SharedShardGcOutcome {
    /// The shard reclaimed.
    pub shard: u32,
    /// The committed cross-replica reclaim watermark sequence (highest reclaimed
    /// global sequence). 0 when nothing was reclaimed.
    pub reclaim_watermark_seq: u64,
    /// The committed watermark segment id (readers skip shared segments `<=` this).
    pub reclaimed_through_segment: u64,
    /// Local segment files unlinked this call.
    pub local_segments_reclaimed: usize,
    /// Shared segment objects deleted this call.
    pub shared_objects_deleted: usize,
    /// Why nothing (more) was reclaimed, or `None`. Secret-free.
    pub note: Option<String>,
}

// ===========================================================================
// Shared-tier segment-GC drive — proves coherent reclamation across the shared
// medium: the owner reclaims local + shared, and BOTH a non-owner cold-load and
// a NEW owner hydrating from shared see exactly the owner's retained set (no
// re-pull of reclaimed segments, no un-reclaim on transfer, shared disk bounded).
// ===========================================================================

/// Secret-free proof of one shared-tier segment-GC cycle. Counts + verdicts only.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SharedTierGcReport {
    /// Replicas / shards in the simulated pool.
    pub shard_count: u32,
    /// Events appended to the reclaimed shard before GC.
    pub appended: usize,
    /// The durable-consumer cursor acked before GC (the interest watermark).
    pub acked_through: u64,
    /// The committed cross-replica reclaim watermark sequence.
    pub reclaim_watermark_seq: u64,
    /// Shared segment objects for the shard before / after GC (fewer ⇒ shared
    /// disk was bounded, not just local).
    pub shared_segments_before: usize,
    pub shared_segments_after: usize,
    /// The owner's retained log is gapless from the reclaimed base to the tip.
    pub owner_retained_gapless: bool,
    /// A non-owner cold-load from shared returns EXACTLY the owner's retained
    /// records (same sequences + payloads) — it did not re-pull reclaimed
    /// segments and diverge.
    pub nonowner_coldload_coherent: bool,
    /// A NEW owner with an empty local disk hydrates from shared, recovers the
    /// same retained set with the base offset intact, and continues the sequence
    /// (reclamation stuck across the ownership transfer — no un-reclaim).
    pub new_owner_hydrate_coherent: bool,
    /// The shared objects at/below the watermark were pruned (shared disk bounded).
    pub shared_pruned: bool,
    /// The single reason an invariant failed, or `None`.
    pub divergence: Option<String>,
}

impl SharedTierGcReport {
    /// Whether reclamation was coherent across the shared medium AND bounded disk.
    pub fn holds(&self) -> bool {
        self.reclaim_watermark_seq > 0
            && self.owner_retained_gapless
            && self.nonowner_coldload_coherent
            && self.new_owner_hydrate_coherent
            && self.shared_pruned
            && self.divergence.is_none()
    }
}

/// Drive a coherent shared-tier segment-GC cycle under `root` with a
/// `shard_count`-replica pool (each replica its own local disk, one shared
/// store). Appends many events to a shard-0 execution under small segments (to
/// force rollover), acks a durable consumer to ~3/4, reclaims shard 0 via the
/// owner, then proves a non-owner cold-load and a fresh new-owner hydrate both
/// see exactly the owner's retained set — no re-pull of reclaimed segments, no
/// un-reclaim on transfer, shared disk bounded. `shard_count` must be `>= 2`.
pub fn exercise_shared_tier_gc(
    root: impl Into<PathBuf>,
    shard_count: u32,
) -> Result<SharedTierGcReport> {
    if shard_count < 2 {
        return Err(EhdbError::InvalidState(
            "shared-tier GC drive requires shard_count >= 2".to_string(),
        ));
    }
    let root = root.into();
    let shared: Arc<dyn SharedSegmentBackend> =
        Arc::new(FilesystemSharedBackend::open(root.join("shared"))?);
    // Small segments so a modest event count rolls over into many segments.
    let seg = 300u64;
    let appended = 40usize;
    let acked_through = (appended as u64 * 3 / 4).max(1);
    let consumer = "projector";

    // The shard-0 owner (replica 0) drives the whole cycle.
    let owner = SharedTierEventLog::open_with_segment_size(
        root.join("local-0"),
        ShardOwnership::new(0, shard_count)?,
        Arc::clone(&shared),
        root.join("coldload-0"),
        seg,
    )?;
    // A deterministic execution owned by shard 0.
    let exec0 = one_execution_per_shard(shard_count)
        .into_iter()
        .find(|(_, s)| *s == 0)
        .map(|(id, _)| id)
        .ok_or_else(|| EhdbError::InvalidState("no shard-0 execution".to_string()))?;

    let mut divergence: Option<String> = None;
    for i in 1..=appended {
        let served = owner.append(&EventLogAppendRequest {
            execution_id: exec0.clone(),
            transaction_id: format!("gc-{i:05}"),
            payload: format!("payload-{i:05}-0123456789abcdef0123456789abcdef"),
        })?;
        if !served.is_served() {
            record_first(&mut divergence, format!("owner did not serve append {i}"));
        }
    }
    // Durable consumer acks ~3/4 (the interest watermark).
    owner.tail(
        0,
        &EventLogTailRequest {
            consumer: consumer.to_string(),
            transaction_id: "gc-tail".to_string(),
            limit: appended,
        },
    )?;
    owner.ack(
        0,
        &EventLogAckRequest {
            consumer: consumer.to_string(),
            transaction_id: "gc-ack".to_string(),
            sequence: acked_through,
        },
    )?;

    let shared_segments_before = shared.list_segment_ids(0)?.len();

    // --- Reclaim shard 0 (local + shared, watermark-first). ----------------
    let gc = match owner.reclaim_shard(0, &SegmentGcPolicy::enabled(2))? {
        Routed::Served(o) => o,
        Routed::NotOwner { .. } => {
            return Err(EhdbError::InvalidState(
                "owner refused its own shard".to_string(),
            ))
        }
    };
    let reclaim_watermark_seq = gc.reclaim_watermark_seq;
    let shared_segments_after = shared.list_segment_ids(0)?.len();
    let shared_pruned = shared_segments_after < shared_segments_before;

    // The owner's retained log: gapless from the base to the tip.
    let owner_scan = owner.scan_shard(
        0,
        &EventLogScanRequest {
            after: None,
            limit: appended * 2,
        },
    )?;
    let owner_seqs: Vec<u64> = owner_scan
        .outcome
        .records
        .iter()
        .map(|r| r.global_sequence)
        .collect();
    let expected: Vec<u64> = (reclaim_watermark_seq + 1..=appended as u64).collect();
    let owner_retained_gapless = owner_seqs == expected;
    if !owner_retained_gapless {
        record_first(
            &mut divergence,
            format!("owner retained not gapless-from-base: {owner_seqs:?} != {expected:?}"),
        );
    }

    // --- Non-owner cold-load from shared must equal the owner's retained set. --
    let nonowner = SharedTierEventLog::open_with_segment_size(
        root.join("local-nonowner"),
        // Owns a different shard, so shard 0 is a cold-load from shared.
        ShardOwnership::new(1, shard_count)?,
        Arc::clone(&shared),
        root.join("coldload-nonowner"),
        seg,
    )?;
    let cold = nonowner.read_execution(&EventLogReadExecutionRequest {
        execution_id: exec0.clone(),
        after: None,
        limit: appended * 2,
    })?;
    let cold_seqs: Vec<u64> = cold
        .outcome
        .records
        .iter()
        .map(|r| r.global_sequence)
        .collect();
    let owner_read = owner.read_execution(&EventLogReadExecutionRequest {
        execution_id: exec0.clone(),
        after: None,
        limit: appended * 2,
    })?;
    let owner_read_seqs: Vec<u64> = owner_read
        .outcome
        .records
        .iter()
        .map(|r| r.global_sequence)
        .collect();
    let nonowner_coldload_coherent = cold.served_by == ServedBy::NonOwnerColdLoad
        && cold_seqs == owner_read_seqs
        && !cold_seqs.contains(&1)
        && cold
            .outcome
            .records
            .iter()
            .zip(owner_read.outcome.records.iter())
            .all(|(a, b)| a.global_sequence == b.global_sequence && a.payload == b.payload);
    if !nonowner_coldload_coherent {
        record_first(
            &mut divergence,
            format!("non-owner cold-load diverged: {cold_seqs:?} vs owner {owner_read_seqs:?}"),
        );
    }

    // --- New owner (empty local disk) hydrates + continues, no un-reclaim. --
    drop(owner);
    let new_owner = SharedTierEventLog::open_with_segment_size(
        root.join("local-newowner"),
        ShardOwnership::new(0, shard_count)?,
        Arc::clone(&shared),
        root.join("coldload-newowner"),
        seg,
    )?;
    new_owner.hydrate_owned_shard(0)?;
    let hydrated = new_owner.scan_shard(
        0,
        &EventLogScanRequest {
            after: None,
            limit: appended * 2,
        },
    )?;
    let hydrated_seqs: Vec<u64> = hydrated
        .outcome
        .records
        .iter()
        .map(|r| r.global_sequence)
        .collect();
    // Continue the sequence — the next append must be appended+1 (no un-reclaim
    // would reset the base / duplicate).
    let next = new_owner.append(&EventLogAppendRequest {
        execution_id: exec0.clone(),
        transaction_id: "gc-post".to_string(),
        payload: "post-hydrate".to_string(),
    })?;
    let next_seq = next.served().map(|o| o.global_sequence).unwrap_or(0);
    let new_owner_hydrate_coherent = hydrated.served_by == ServedBy::OwnerResident
        && hydrated_seqs == expected
        && next_seq == appended as u64 + 1;
    if !new_owner_hydrate_coherent {
        record_first(
            &mut divergence,
            format!(
                "new-owner hydrate diverged: seqs {hydrated_seqs:?} (want {expected:?}), next {next_seq}"
            ),
        );
    }

    Ok(SharedTierGcReport {
        shard_count,
        appended,
        acked_through,
        reclaim_watermark_seq,
        shared_segments_before,
        shared_segments_after,
        owner_retained_gapless,
        nonowner_coldload_coherent,
        new_owner_hydrate_coherent,
        shared_pruned,
        divergence,
    })
}

// ===========================================================================
// Shared-tier drive — the star of this slice.
//
// Spins up a two-replica pool with SEPARATE local disks over ONE shared store,
// and proves:
//   * owner append publishes its segments to the shared store,
//   * a non-owner (separate local disk, no local copy) cold-loads the owner's
//     exact records FROM THE SHARED STORE (slice-2 local cold-load would be
//     empty here — this is the shared tier's whole point),
//   * a NEW owner with an EMPTY local disk hydrates the shard from shared and
//     replays it zero-loss + gapless (crash-recovery-from-shared), then
//     continues the sequence,
//   * a shared-store miss (shard never published) reads as empty, not an error,
//   * parity: cold-load-from-shared records == the owner's local records.
// ===========================================================================

/// Secret-free proof of one shared-segment-tier drive. Counts + verdicts only
/// (payloads are synthetic).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SharedTierReport {
    /// Replicas / shards in the simulated pool.
    pub shard_count: u32,
    /// Distinct executions driven (each owned by exactly one shard).
    pub executions: usize,
    /// Total committed segments across all shards in the shared store after the
    /// owner appends + publishes.
    pub shared_segments: usize,
    /// Every owner append published its shard's segments to the shared store.
    pub owner_published_ok: bool,
    /// A non-owner with a separate (empty) local disk cold-loaded the owner's
    /// exact records from the shared store.
    pub nonowner_coldload_from_shared_ok: bool,
    /// A new owner with an empty local disk hydrated the shard from shared and
    /// replayed it zero-loss + gapless, then appended the next sequence.
    pub crash_recovery_from_shared_ok: bool,
    /// A shared-store miss (shard never published) read as empty, not an error.
    pub shared_miss_ok: bool,
    /// Cold-load-from-shared records matched the owner's local records
    /// (sequence + payload).
    pub parity_ok: bool,
    /// The single reason a durability/coherence invariant failed, or `None`.
    pub divergence: Option<String>,
}

impl SharedTierReport {
    /// Whether every shared-tier invariant held.
    pub fn holds(&self) -> bool {
        self.owner_published_ok
            && self.nonowner_coldload_from_shared_ok
            && self.crash_recovery_from_shared_ok
            && self.shared_miss_ok
            && self.parity_ok
            && self.divergence.is_none()
    }
}

/// Deterministically pick one execution id per shard `0..count`, searching
/// decimal snowflake-shaped ids so every shard is covered. Returns
/// `(execution_id, owning_shard)` pairs, one per shard, ascending by shard.
fn one_execution_per_shard(count: u32) -> Vec<(String, u32)> {
    let base = 320_816_801_799_737_344_i64;
    let mut found: HashMap<u32, String> = HashMap::new();
    let mut i = 0i64;
    while (found.len() as u32) < count {
        let id = (base + i).to_string();
        let shard = crate::affinity::shard_for_execution(&id, count);
        found.entry(shard).or_insert(id);
        i += 1;
        if i > 1_000_000 {
            break;
        }
    }
    let mut out: Vec<(String, u32)> = found.into_iter().map(|(s, id)| (id, s)).collect();
    out.sort_unstable_by_key(|(_, s)| *s);
    out
}

/// Record only the first divergence reason (later ones are symptoms).
fn record_first(slot: &mut Option<String>, reason: String) {
    if slot.is_none() {
        *slot = Some(reason);
    }
}

/// Drive a shared-segment-tier cycle under `root` with a `shard_count`-replica
/// pool sharing one shared store. `shard_count` must be `>= 2` (a single shard
/// has no owner/non-owner split to prove). See the module docs for the
/// invariants proven.
pub fn exercise_shared_tier(
    root: impl Into<PathBuf>,
    shard_count: u32,
) -> Result<SharedTierReport> {
    if shard_count < 2 {
        return Err(EhdbError::InvalidState(
            "shared-tier drive requires shard_count >= 2".to_string(),
        ));
    }
    let root = root.into();
    let shared: Arc<dyn SharedSegmentBackend> =
        Arc::new(FilesystemSharedBackend::open(root.join("shared"))?);
    let executions = one_execution_per_shard(shard_count);
    let total_execs = executions.len();
    let mut divergence: Option<String> = None;

    // One replica per shard, each with its OWN local disk, all sharing `shared`.
    let replicas: Vec<SharedTierEventLog> = (0..shard_count)
        .map(|idx| {
            let ownership = ShardOwnership::new(idx, shard_count)?;
            SharedTierEventLog::open(
                root.join(format!("local-{idx}")),
                ownership,
                Arc::clone(&shared),
                root.join(format!("coldload-{idx}")),
            )
        })
        .collect::<Result<Vec<_>>>()?;

    // --- Owner appends + publishes to shared. ------------------------------
    let mut owner_published_ok = true;
    for (execution_id, owner_shard) in &executions {
        let replica = &replicas[*owner_shard as usize];
        let served = replica.append(&EventLogAppendRequest {
            execution_id: execution_id.clone(),
            transaction_id: format!("shared-{execution_id}"),
            payload: format!("{{\"exec\":\"{execution_id}\"}}"),
        })?;
        if !served.is_served() {
            owner_published_ok = false;
            record_first(
                &mut divergence,
                format!("owner {owner_shard} did not serve its own exec {execution_id}"),
            );
        }
    }
    // Every owned shard has at least one committed segment in shared.
    let mut shared_segments = 0usize;
    for (_, owner_shard) in &executions {
        let ids = shared.list_segment_ids(*owner_shard)?;
        shared_segments += ids.len();
        if ids.is_empty() {
            owner_published_ok = false;
            record_first(
                &mut divergence,
                format!("shard {owner_shard} has no segments in the shared store after publish"),
            );
        }
    }

    // --- Non-owner cold-load FROM SHARED + parity. -------------------------
    // A replica that does NOT own shard 0 reads a shard-0 execution. Its local
    // disk has no shard-0 bytes, so a correct read proves it came from shared.
    let mut nonowner_coldload_from_shared_ok = true;
    let mut parity_ok = true;
    let exec0 = executions
        .iter()
        .find(|(_, s)| *s == 0)
        .map(|(id, _)| id.clone());
    if let Some(exec0) = exec0 {
        // A non-owner of shard 0.
        let reader = replicas
            .iter()
            .find(|r| r.ownership().shard_index() != 0)
            .expect("shard_count >= 2 guarantees a non-owner of shard 0");
        let read = reader.read_execution(&EventLogReadExecutionRequest {
            execution_id: exec0.clone(),
            after: None,
            limit: 100,
        })?;
        // The owner's local view of the same execution (parity reference).
        let owner_read = replicas[0].read_execution(&EventLogReadExecutionRequest {
            execution_id: exec0.clone(),
            after: None,
            limit: 100,
        })?;
        if read.served_by != ServedBy::NonOwnerColdLoad || read.outcome.returned == 0 {
            nonowner_coldload_from_shared_ok = false;
            record_first(
                &mut divergence,
                format!(
                    "non-owner cold-load anomaly: served_by={:?} returned={}",
                    read.served_by, read.outcome.returned
                ),
            );
        }
        let same = read.outcome.returned == owner_read.outcome.returned
            && read
                .outcome
                .records
                .iter()
                .zip(owner_read.outcome.records.iter())
                .all(|(a, b)| a.global_sequence == b.global_sequence && a.payload == b.payload);
        if !same {
            parity_ok = false;
            record_first(
                &mut divergence,
                "cold-load-from-shared records != owner local records".to_string(),
            );
        }
    } else {
        nonowner_coldload_from_shared_ok = false;
        parity_ok = false;
        record_first(&mut divergence, "no shard-0 execution to test".to_string());
    }

    // --- Shared-store miss: a shard never published reads empty (not error). --
    // Use a fresh, empty shared store + a replica reading a shard-0 exec.
    let mut shared_miss_ok = true;
    {
        let empty_shared: Arc<dyn SharedSegmentBackend> =
            Arc::new(FilesystemSharedBackend::open(root.join("shared-empty"))?);
        // A replica that owns shard 1 (so shard 0 is a non-owner cold-load) over
        // the EMPTY shared store.
        let miss_replica = SharedTierEventLog::open(
            root.join("local-miss"),
            ShardOwnership::new(1, shard_count)?,
            Arc::clone(&empty_shared),
            root.join("coldload-miss"),
        )?;
        let exec0 = executions
            .iter()
            .find(|(_, s)| *s == 0)
            .map(|(id, _)| id.clone())
            .unwrap_or_else(|| "320816801799737344".to_string());
        let read = miss_replica.read_execution(&EventLogReadExecutionRequest {
            execution_id: exec0,
            after: None,
            limit: 10,
        })?;
        // Empty shared → cold-load an empty read-only store → exists=false, no
        // records, and NOT an error (we got here).
        if read.served_by != ServedBy::NonOwnerColdLoad
            || read.outcome.exists
            || read.outcome.returned != 0
        {
            shared_miss_ok = false;
            record_first(
                &mut divergence,
                format!(
                    "shared-miss anomaly: served_by={:?} exists={} returned={}",
                    read.served_by, read.outcome.exists, read.outcome.returned
                ),
            );
        }
        // Backend-level miss probes.
        if empty_shared.get_segment(0, 0)?.is_some()
            || !empty_shared.list_segment_ids(0)?.is_empty()
        {
            shared_miss_ok = false;
            record_first(
                &mut divergence,
                "empty shared store returned a segment for an unpublished shard".to_string(),
            );
        }
    }

    // --- Crash-recovery-from-shared: NEW owner, EMPTY local disk. ----------
    // Drop the original pool, then a fresh replica becomes owner of shard 0 with
    // a brand-new empty local dir. It hydrates shard 0 from shared and must
    // recover zero-loss + gapless, then continue the sequence.
    drop(replicas);
    let mut crash_recovery_from_shared_ok = true;
    {
        let expected0 = executions.iter().filter(|(_, s)| *s == 0).count();
        let new_owner = SharedTierEventLog::open(
            root.join("local-recover"),
            ShardOwnership::new(0, shard_count)?,
            Arc::clone(&shared),
            root.join("coldload-recover"),
        )?;
        let hydrate = new_owner.hydrate_owned_shard(0)?;
        // The new owner's local disk was empty and shared had segments to pull.
        if !hydrate.was_empty || hydrate.materialized == 0 {
            crash_recovery_from_shared_ok = false;
            record_first(
                &mut divergence,
                format!(
                    "hydrate anomaly: was_empty={} materialized={}",
                    hydrate.was_empty, hydrate.materialized
                ),
            );
        }
        // Owner-resident scan after hydrate replays the shard zero-loss + gapless.
        let scan = new_owner.scan_shard(
            0,
            &EventLogScanRequest {
                after: None,
                limit: 100_000,
            },
        )?;
        let gapless = scan
            .outcome
            .records
            .iter()
            .enumerate()
            .all(|(i, r)| r.global_sequence == i as u64 + 1);
        if scan.served_by != ServedBy::OwnerResident
            || scan.outcome.record_count != expected0
            || !gapless
        {
            crash_recovery_from_shared_ok = false;
            record_first(
                &mut divergence,
                format!(
                    "recover-from-shared anomaly: served_by={:?} recovered {} of {expected0} (gapless={gapless})",
                    scan.served_by, scan.outcome.record_count
                ),
            );
        }
        // Continue the sequence: the next append lands at expected0 + 1.
        let cont = new_owner.append(&EventLogAppendRequest {
            execution_id: executions
                .iter()
                .find(|(_, s)| *s == 0)
                .map(|(id, _)| id.clone())
                .unwrap_or_default(),
            transaction_id: "shared-continue".to_string(),
            payload: "{\"cont\":true}".to_string(),
        })?;
        match cont.served() {
            Some(outcome) if outcome.global_sequence == expected0 as u64 + 1 => {}
            other => {
                crash_recovery_from_shared_ok = false;
                record_first(
                    &mut divergence,
                    format!("post-recovery append did not continue sequence: {other:?}"),
                );
            }
        }
    }

    Ok(SharedTierReport {
        shard_count,
        executions: total_execs,
        shared_segments,
        owner_published_ok,
        nonowner_coldload_from_shared_ok,
        crash_recovery_from_shared_ok,
        shared_miss_ok,
        parity_ok,
        divergence,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tmp_root(tag: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "ehdb-shared-{tag}-{}-{:?}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn append_req(exec: &str, payload: &str) -> EventLogAppendRequest {
        EventLogAppendRequest {
            execution_id: exec.to_string(),
            transaction_id: format!("txn-{exec}"),
            payload: payload.to_string(),
        }
    }

    #[test]
    fn shared_segment_key_is_fixed_width_and_bounded() {
        // Constant 40 chars regardless of the shard / segment magnitude, well
        // under any subject-length cap (the object-tier trap this slice avoids).
        let small = shared_segment_key(0, 1);
        let large = shared_segment_key(u32::MAX, u64::MAX);
        assert_eq!(small.len(), 40);
        assert_eq!(large.len(), 40);
        assert!(large.len() < 256);
        assert_ne!(small, large);
    }

    #[test]
    fn filesystem_backend_put_get_list_roundtrip() {
        let root = tmp_root("fsbackend");
        let backend = FilesystemSharedBackend::open(&root).unwrap();
        assert!(backend.get_segment(0, 1).unwrap().is_none());
        assert!(backend.list_segment_ids(0).unwrap().is_empty());

        let put = backend.put_segment(0, 1, b"hello").unwrap();
        assert!(put.newly_written);
        assert_eq!(put.byte_len, 5);
        // Idempotent re-put of identical bytes is a no-op write.
        let put2 = backend.put_segment(0, 1, b"hello").unwrap();
        assert!(!put2.newly_written);

        assert_eq!(backend.get_segment(0, 1).unwrap().unwrap(), b"hello");
        assert_eq!(backend.list_segment_ids(0).unwrap(), vec![1]);
        // A different shard is isolated.
        assert!(backend.list_segment_ids(1).unwrap().is_empty());
        let _ = fs::remove_dir_all(&root);
    }

    #[test]
    fn filesystem_backend_detects_truncation_and_bitrot() {
        let root = tmp_root("fscorrupt");
        let backend = FilesystemSharedBackend::open(&root).unwrap();
        backend.put_segment(3, 7, b"abcdefgh").unwrap();
        let object = root.join(shared_segment_key(3, 7));

        // Truncate the object → length mismatch → hard error.
        fs::write(&object, b"abc").unwrap();
        let err = backend.get_segment(3, 7).unwrap_err();
        assert!(err.to_string().contains("truncated/corrupt"), "{err}");

        // Bit-flip at the right length → digest mismatch → hard error.
        fs::write(&object, b"Xbcdefgh").unwrap();
        let err = backend.get_segment(3, 7).unwrap_err();
        assert!(err.to_string().contains("bit-rot"), "{err}");
        let _ = fs::remove_dir_all(&root);
    }

    #[test]
    fn owner_publishes_nonowner_cold_loads_from_shared() {
        let root = tmp_root("crossreplica");
        let shared: Arc<dyn SharedSegmentBackend> =
            Arc::new(FilesystemSharedBackend::open(root.join("shared")).unwrap());
        // One exec owned by shard 0, one by shard 1.
        let execs = one_execution_per_shard(2);
        let exec0 = execs.iter().find(|(_, s)| *s == 0).unwrap().0.clone();

        // Replica A owns shard 0 (its own local disk); replica B owns shard 1
        // (a SEPARATE local disk). B has no shard-0 bytes locally.
        let a = SharedTierEventLog::open(
            root.join("local-a"),
            ShardOwnership::new(0, 2).unwrap(),
            Arc::clone(&shared),
            root.join("coldload-a"),
        )
        .unwrap();
        let b = SharedTierEventLog::open(
            root.join("local-b"),
            ShardOwnership::new(1, 2).unwrap(),
            Arc::clone(&shared),
            root.join("coldload-b"),
        )
        .unwrap();

        assert!(a
            .append(&append_req(&exec0, "owned-by-0"))
            .unwrap()
            .is_served());

        // B (non-owner of shard 0, separate empty local disk) reads exec0 →
        // cold-load FROM SHARED yields A's record.
        let read = b
            .read_execution(&EventLogReadExecutionRequest {
                execution_id: exec0.clone(),
                after: None,
                limit: 10,
            })
            .unwrap();
        assert_eq!(read.served_by, ServedBy::NonOwnerColdLoad);
        assert_eq!(read.outcome.returned, 1);
        assert_eq!(read.outcome.records[0].payload, "owned-by-0");
        let _ = fs::remove_dir_all(&root);
    }

    #[test]
    fn new_owner_hydrates_from_shared_and_continues() {
        let root = tmp_root("hydrate");
        let shared: Arc<dyn SharedSegmentBackend> =
            Arc::new(FilesystemSharedBackend::open(root.join("shared")).unwrap());
        let execs = one_execution_per_shard(2);
        let exec0 = execs.iter().find(|(_, s)| *s == 0).unwrap().0.clone();

        // Original owner writes + publishes, then goes away.
        {
            let a = SharedTierEventLog::open(
                root.join("local-a"),
                ShardOwnership::new(0, 2).unwrap(),
                Arc::clone(&shared),
                root.join("coldload-a"),
            )
            .unwrap();
            a.append(&append_req(&exec0, "e1")).unwrap();
        }

        // New owner of shard 0 with a BRAND-NEW empty local disk.
        let c = SharedTierEventLog::open(
            root.join("local-c"),
            ShardOwnership::new(0, 2).unwrap(),
            Arc::clone(&shared),
            root.join("coldload-c"),
        )
        .unwrap();
        let hy = c.hydrate_owned_shard(0).unwrap();
        assert!(hy.was_empty);
        assert!(hy.materialized >= 1);

        // Owner-resident scan recovers the record; next append continues seq.
        let scan = c
            .scan_shard(
                0,
                &EventLogScanRequest {
                    after: None,
                    limit: 100,
                },
            )
            .unwrap();
        assert_eq!(scan.served_by, ServedBy::OwnerResident);
        assert_eq!(scan.outcome.record_count, 1);
        assert_eq!(scan.outcome.records[0].payload, "e1");
        let next = c.append(&append_req(&exec0, "e2")).unwrap();
        assert_eq!(next.served().unwrap().global_sequence, 2);
        let _ = fs::remove_dir_all(&root);
    }

    #[test]
    fn hydrate_refuses_non_owned_shard() {
        let root = tmp_root("hydraterefuse");
        let shared: Arc<dyn SharedSegmentBackend> =
            Arc::new(FilesystemSharedBackend::open(root.join("shared")).unwrap());
        let r = SharedTierEventLog::open(
            root.join("local"),
            ShardOwnership::new(0, 2).unwrap(),
            shared,
            root.join("coldload"),
        )
        .unwrap();
        // Replica owns shard 0, not shard 1 → hydrating shard 1 is an error.
        let err = r.hydrate_owned_shard(1).unwrap_err();
        assert!(matches!(err, EhdbError::InvalidState(_)));
        let _ = fs::remove_dir_all(&root);
    }

    #[test]
    fn shared_miss_reads_empty_not_error() {
        let root = tmp_root("miss");
        let shared: Arc<dyn SharedSegmentBackend> =
            Arc::new(FilesystemSharedBackend::open(root.join("shared")).unwrap());
        let execs = one_execution_per_shard(2);
        let exec0 = execs.iter().find(|(_, s)| *s == 0).unwrap().0.clone();
        // Replica owns shard 1; nothing ever published → cold-load of shard 0 is
        // an empty read, not an error.
        let r = SharedTierEventLog::open(
            root.join("local"),
            ShardOwnership::new(1, 2).unwrap(),
            shared,
            root.join("coldload"),
        )
        .unwrap();
        let read = r
            .read_execution(&EventLogReadExecutionRequest {
                execution_id: exec0,
                after: None,
                limit: 10,
            })
            .unwrap();
        assert_eq!(read.served_by, ServedBy::NonOwnerColdLoad);
        assert!(!read.outcome.exists);
        assert_eq!(read.outcome.returned, 0);
        let _ = fs::remove_dir_all(&root);
    }

    #[test]
    fn multi_segment_publish_and_cold_load_across_rollover() {
        let root = tmp_root("rollover");
        let shared: Arc<dyn SharedSegmentBackend> =
            Arc::new(FilesystemSharedBackend::open(root.join("shared")).unwrap());
        let execs = one_execution_per_shard(2);
        let exec0 = execs.iter().find(|(_, s)| *s == 0).unwrap().0.clone();

        // Tiny segment size forces multiple segments for shard 0.
        let a = SharedTierEventLog::open_with_segment_size(
            root.join("local-a"),
            ShardOwnership::new(0, 2).unwrap(),
            Arc::clone(&shared),
            root.join("coldload-a"),
            128,
        )
        .unwrap();
        for i in 1..=15 {
            a.append(&append_req(&exec0, &format!("payload-{i:03}")))
                .unwrap();
        }
        // More than one segment published.
        assert!(
            shared.list_segment_ids(0).unwrap().len() > 1,
            "expected multi-segment rollover in the shared store"
        );

        // Non-owner cold-loads the whole multi-segment stream in order.
        let b = SharedTierEventLog::open_with_segment_size(
            root.join("local-b"),
            ShardOwnership::new(1, 2).unwrap(),
            Arc::clone(&shared),
            root.join("coldload-b"),
            128,
        )
        .unwrap();
        let read = b
            .read_execution(&EventLogReadExecutionRequest {
                execution_id: exec0,
                after: None,
                limit: 100,
            })
            .unwrap();
        assert_eq!(read.served_by, ServedBy::NonOwnerColdLoad);
        assert_eq!(read.outcome.returned, 15);
        let seqs: Vec<u64> = read
            .outcome
            .records
            .iter()
            .map(|r| r.global_sequence)
            .collect();
        assert_eq!(seqs, (1..=15).collect::<Vec<_>>());
        assert_eq!(read.outcome.records[14].payload, "payload-015");
        let _ = fs::remove_dir_all(&root);
    }

    #[test]
    fn drive_proves_shared_tier_two_shards() {
        let root = tmp_root("drive2");
        let report = exercise_shared_tier(&root, 2).unwrap();
        assert!(report.holds(), "{report:?}");
        assert_eq!(report.shard_count, 2);
        assert!(report.owner_published_ok);
        assert!(report.nonowner_coldload_from_shared_ok);
        assert!(report.crash_recovery_from_shared_ok);
        assert!(report.shared_miss_ok);
        assert!(report.parity_ok);
        assert!(report.divergence.is_none());
        let _ = fs::remove_dir_all(&root);
    }

    #[test]
    fn drive_proves_shared_tier_four_shards() {
        let root = tmp_root("drive4");
        let report = exercise_shared_tier(&root, 4).unwrap();
        assert!(report.holds(), "{report:?}");
        assert_eq!(report.shard_count, 4);
        let _ = fs::remove_dir_all(&root);
    }

    #[test]
    fn drive_requires_at_least_two_shards() {
        let root = tmp_root("drive1");
        let err = exercise_shared_tier(&root, 1).unwrap_err();
        assert!(err.to_string().contains("shard_count >= 2"));
        let _ = fs::remove_dir_all(&root);
    }

    // -----------------------------------------------------------------------
    // Incremental append-delta publish ([noetl/ehdb#264]) — the O(delta) fix.
    // -----------------------------------------------------------------------

    /// A [`SharedSegmentBackend`] wrapper that tallies the bytes handed to
    /// `append_segment` (the delta) so a test can prove the publish hot path is
    /// O(delta), not O(active-segment-size). Delegates everything to an inner
    /// [`FilesystemSharedBackend`].
    struct RecordingBackend {
        inner: FilesystemSharedBackend,
        append_delta_bytes: Mutex<u64>,
        append_calls: Mutex<u64>,
    }

    impl RecordingBackend {
        fn new(inner: FilesystemSharedBackend) -> Self {
            Self {
                inner,
                append_delta_bytes: Mutex::new(0),
                append_calls: Mutex::new(0),
            }
        }
    }

    impl SharedSegmentBackend for RecordingBackend {
        fn backend_name(&self) -> &'static str {
            "recording"
        }
        fn put_segment(
            &self,
            shard: u32,
            segment_id: u64,
            bytes: &[u8],
        ) -> Result<SharedSegmentPutOutcome> {
            self.inner.put_segment(shard, segment_id, bytes)
        }
        fn get_segment(&self, shard: u32, segment_id: u64) -> Result<Option<Vec<u8>>> {
            self.inner.get_segment(shard, segment_id)
        }
        fn list_segment_ids(&self, shard: u32) -> Result<Vec<u64>> {
            self.inner.list_segment_ids(shard)
        }
        fn append_segment(
            &self,
            shard: u32,
            segment_id: u64,
            committed_len: u64,
            delta: &[u8],
        ) -> Result<SharedSegmentPutOutcome> {
            *self.append_delta_bytes.lock().unwrap() += delta.len() as u64;
            *self.append_calls.lock().unwrap() += 1;
            self.inner
                .append_segment(shard, segment_id, committed_len, delta)
        }
        fn committed_len(&self, shard: u32, segment_id: u64) -> Result<Option<u64>> {
            self.inner.committed_len(shard, segment_id)
        }
    }

    #[test]
    fn incremental_publish_sends_each_byte_exactly_once() {
        // The core regression guard for #264: with a whole-segment re-publish the
        // total bytes handed to the backend across N appends grows ~O(N^2) (each
        // append re-sends the whole growing segment). With incremental publish
        // each byte is sent exactly once, so the delta total equals the final
        // committed segment length.
        let root = tmp_root("incremental-once");
        let recording = Arc::new(RecordingBackend::new(
            FilesystemSharedBackend::open(root.join("shared")).unwrap(),
        ));
        let execs = one_execution_per_shard(2);
        let exec0 = execs.iter().find(|(_, s)| *s == 0).unwrap().0.clone();
        // Default 8 MiB segment → all appends stay in one active segment (no
        // rotation), so this is the exact scenario the O(segment) bug punished.
        let a = SharedTierEventLog::open(
            root.join("local-a"),
            ShardOwnership::new(0, 2).unwrap(),
            Arc::clone(&recording) as Arc<dyn SharedSegmentBackend>,
            root.join("coldload-a"),
        )
        .unwrap();

        let n = 64u64;
        let payload = "x".repeat(256);
        for _ in 0..n {
            a.append(&append_req(&exec0, &payload)).unwrap();
        }

        let delta_total = *recording.append_delta_bytes.lock().unwrap();
        let calls = *recording.append_calls.lock().unwrap();
        let committed = recording.committed_len(0, 1).unwrap().unwrap();

        // One publish per append, and every published byte is a first-time byte.
        assert_eq!(calls, n, "one incremental publish per append");
        assert_eq!(
            delta_total, committed,
            "each byte published exactly once (no whole-segment re-publish): \
             delta_total={delta_total} committed={committed}"
        );
        // Sanity: the O(segment) bug would have sent far more than one segment's
        // worth (~n/2 segments). Assert we are nowhere near that.
        assert!(
            delta_total < committed * 2,
            "publish volume must be O(segment), not O(n*segment)"
        );

        // Cold-load parity still holds over the incrementally-published bytes.
        let b = SharedTierEventLog::open(
            root.join("local-b"),
            ShardOwnership::new(1, 2).unwrap(),
            Arc::clone(&recording) as Arc<dyn SharedSegmentBackend>,
            root.join("coldload-b"),
        )
        .unwrap();
        let read = b
            .read_execution(&EventLogReadExecutionRequest {
                execution_id: exec0,
                after: None,
                limit: 1000,
            })
            .unwrap();
        assert_eq!(read.served_by, ServedBy::NonOwnerColdLoad);
        assert_eq!(read.outcome.returned as u64, n);
        let _ = fs::remove_dir_all(&root);
    }

    #[test]
    fn get_segment_reads_committed_prefix_ignoring_uncommitted_tail() {
        // Crash-safety of the incremental append: a crash between the delta
        // fsync and the marker re-commit leaves an uncommitted tail. A reader
        // must see only the committed prefix, and the next publish drops the tail.
        let root = tmp_root("uncommitted-tail");
        let backend = FilesystemSharedBackend::open(&root).unwrap();
        backend.append_segment(0, 1, 0, b"hello").unwrap();
        assert_eq!(backend.get_segment(0, 1).unwrap().unwrap(), b"hello");

        // Simulate the interrupted append: extra bytes on the object, marker NOT
        // advanced (bypass the backend to write straight to the object file).
        let object = root.join(shared_segment_key(0, 1));
        {
            let mut f = OpenOptions::new().append(true).open(&object).unwrap();
            f.write_all(b"WORLD-uncommitted").unwrap();
        }
        // Reader still sees only the committed prefix — the tail is invisible.
        assert_eq!(backend.get_segment(0, 1).unwrap().unwrap(), b"hello");

        // The next incremental append drops the uncommitted tail and continues.
        let out = backend.append_segment(0, 1, 5, b" world").unwrap();
        assert_eq!(out.byte_len, 11);
        assert_eq!(backend.get_segment(0, 1).unwrap().unwrap(), b"hello world");
        assert_eq!(out.digest, segment_digest(b"hello world"));
        let _ = fs::remove_dir_all(&root);
    }

    #[test]
    fn get_segment_errors_when_object_shorter_than_committed() {
        // The opposite of an uncommitted tail: an object *shorter* than the
        // committed length is genuine truncation/corruption → hard error.
        let root = tmp_root("short-object");
        let backend = FilesystemSharedBackend::open(&root).unwrap();
        backend.append_segment(0, 1, 0, b"abcdefgh").unwrap();
        let object = root.join(shared_segment_key(0, 1));
        fs::write(&object, b"abc").unwrap();
        let err = backend.get_segment(0, 1).unwrap_err();
        assert!(err.to_string().contains("truncated/corrupt"), "{err}");
        let _ = fs::remove_dir_all(&root);
    }

    #[test]
    fn append_segment_resumes_digest_from_sidecar_across_instances() {
        // A brand-new backend instance (the worker's per-op boundary) carries no
        // in-memory state — it must resume the running digest from the persisted
        // sidecar state so the full digest still matches a whole-object hash,
        // WITHOUT re-reading the committed prefix.
        let root = tmp_root("resume-sidecar");
        {
            let b = FilesystemSharedBackend::open(&root).unwrap();
            b.append_segment(0, 1, 0, b"aaaabbbb").unwrap();
        }
        // A brand-new instance over the same root = no in-memory carry-over.
        let b2 = FilesystemSharedBackend::open(&root).unwrap();
        assert_eq!(b2.committed_len(0, 1).unwrap(), Some(8));
        let out = b2.append_segment(0, 1, 8, b"cccc").unwrap();
        assert_eq!(out.byte_len, 12);
        // Resumed digest equals the whole-object hash.
        assert_eq!(out.digest, segment_digest(b"aaaabbbbcccc"));
        assert_eq!(b2.get_segment(0, 1).unwrap().unwrap(), b"aaaabbbbcccc");
        let _ = fs::remove_dir_all(&root);
    }

    #[test]
    fn per_op_construction_resumes_digest_from_sidecar() {
        // The real deployment shape: the worker builds a FRESH shared-tier stack
        // for every append (stateless boundary). A fresh backend per append over
        // a growing segment must produce a correct, integrity-verifiable object —
        // proving the resumable digest state on the sidecar carries the hash
        // across constructions with no in-memory state and no whole-prefix re-read.
        let root = tmp_root("perop");
        let mut committed = 0u64;
        let mut expected = Vec::new();
        for i in 0..40u64 {
            let backend = FilesystemSharedBackend::open(root.join("shared")).unwrap();
            let frame = format!("frame-{i:04}|").into_bytes();
            backend.append_segment(0, 1, committed, &frame).unwrap();
            committed += frame.len() as u64;
            expected.extend_from_slice(&frame);
        }
        // A final fresh backend cold-reads the whole object, integrity-verified
        // against the digest that was extended incrementally across 40 instances.
        let reader = FilesystemSharedBackend::open(root.join("shared")).unwrap();
        assert_eq!(reader.committed_len(0, 1).unwrap(), Some(committed));
        assert_eq!(reader.get_segment(0, 1).unwrap().unwrap(), expected);
        let _ = fs::remove_dir_all(&root);
    }

    #[test]
    fn append_segment_reseeds_from_prefix_when_state_absent() {
        // Backward-compat: a segment whose sidecar predates the persisted state
        // (no `hasher_state`) must re-seed the digest from the committed prefix
        // exactly once, producing the correct whole-object digest, then carry the
        // state forward.
        let root = tmp_root("legacy-meta");
        let backend = FilesystemSharedBackend::open(&root).unwrap();
        // Write an object + a legacy sidecar (byte_len + digest, NO hasher_state).
        let object = root.join(shared_segment_key(0, 1));
        fs::write(&object, b"legacy-prefix").unwrap();
        let legacy = format!(
            "{{\"byte_len\":13,\"digest\":\"{}\"}}",
            segment_digest(b"legacy-prefix")
        );
        fs::write(
            root.join(format!("{}.meta", shared_segment_key(0, 1))),
            legacy,
        )
        .unwrap();
        // Incremental append over the legacy segment re-seeds from the prefix.
        let out = backend.append_segment(0, 1, 13, b"-more").unwrap();
        assert_eq!(out.byte_len, 18);
        assert_eq!(out.digest, segment_digest(b"legacy-prefix-more"));
        assert_eq!(
            backend.get_segment(0, 1).unwrap().unwrap(),
            b"legacy-prefix-more"
        );
        // And the state now carries forward (a further append resumes, no re-seed).
        let out2 = backend.append_segment(0, 1, 18, b"!").unwrap();
        assert_eq!(out2.digest, segment_digest(b"legacy-prefix-more!"));
        let _ = fs::remove_dir_all(&root);
    }

    #[test]
    fn default_append_segment_fallback_matches_incremental() {
        // The trait's default (O(size)) append_segment must produce byte- and
        // digest-identical results to the efficient override — a backend that
        // does not override still stores the same committed prefix.
        struct DefaultOnly(FilesystemSharedBackend);
        impl SharedSegmentBackend for DefaultOnly {
            fn backend_name(&self) -> &'static str {
                "default-only"
            }
            fn put_segment(
                &self,
                shard: u32,
                segment_id: u64,
                bytes: &[u8],
            ) -> Result<SharedSegmentPutOutcome> {
                self.0.put_segment(shard, segment_id, bytes)
            }
            fn get_segment(&self, shard: u32, segment_id: u64) -> Result<Option<Vec<u8>>> {
                self.0.get_segment(shard, segment_id)
            }
            fn list_segment_ids(&self, shard: u32) -> Result<Vec<u64>> {
                self.0.list_segment_ids(shard)
            }
            // Deliberately NOT overriding append_segment / committed_len — use the
            // trait defaults.
        }
        let root = tmp_root("default-fallback");
        let d = DefaultOnly(FilesystemSharedBackend::open(&root).unwrap());
        d.append_segment(0, 1, 0, b"hello").unwrap();
        assert_eq!(d.committed_len(0, 1).unwrap(), Some(5));
        let out = d.append_segment(0, 1, 5, b" world").unwrap();
        assert_eq!(out.byte_len, 11);
        assert_eq!(out.digest, segment_digest(b"hello world"));
        assert_eq!(d.get_segment(0, 1).unwrap().unwrap(), b"hello world");
        // A mismatched committed_len is a hard error, not a silent overwrite.
        let err = d.append_segment(0, 1, 999, b"x").unwrap_err();
        assert!(err.to_string().contains("committed_len"), "{err}");
        let _ = fs::remove_dir_all(&root);
    }

    // -----------------------------------------------------------------------
    // Shared-tier segment GC — coherent reclamation across the shared medium.
    // -----------------------------------------------------------------------

    #[test]
    fn shared_backend_watermark_is_monotonic_and_delete_is_idempotent() {
        let root = tmp_root("wm");
        let backend = FilesystemSharedBackend::open(root.join("shared")).unwrap();
        assert_eq!(backend.reclaim_watermark(0).unwrap(), (0, 0));
        backend.put_reclaim_watermark(0, 100, 3).unwrap();
        assert_eq!(backend.reclaim_watermark(0).unwrap(), (100, 3));
        // A lower watermark never lowers the committed one (monotonic).
        backend.put_reclaim_watermark(0, 50, 1).unwrap();
        assert_eq!(backend.reclaim_watermark(0).unwrap(), (100, 3));
        // A higher one advances.
        backend.put_reclaim_watermark(0, 200, 6).unwrap();
        assert_eq!(backend.reclaim_watermark(0).unwrap(), (200, 6));
        // delete_segment is idempotent (absent = ok).
        backend.delete_segment(0, 999).unwrap();
        let _ = fs::remove_dir_all(&root);
    }

    #[test]
    fn shared_tier_gc_drive_holds_two_shards() {
        let root = tmp_root("gc-drive2");
        let report = exercise_shared_tier_gc(&root, 2).unwrap();
        assert!(report.holds(), "{report:?}");
        assert!(report.reclaim_watermark_seq > 0);
        assert!(report.shared_segments_after < report.shared_segments_before);
        assert!(report.owner_retained_gapless);
        assert!(report.nonowner_coldload_coherent);
        assert!(report.new_owner_hydrate_coherent);
        assert!(report.shared_pruned);
        assert!(report.divergence.is_none());
        let _ = fs::remove_dir_all(&root);
    }

    #[test]
    fn shared_tier_gc_drive_holds_four_shards() {
        let root = tmp_root("gc-drive4");
        let report = exercise_shared_tier_gc(&root, 4).unwrap();
        assert!(report.holds(), "{report:?}");
        let _ = fs::remove_dir_all(&root);
    }

    #[test]
    fn shared_tier_gc_requires_two_shards() {
        let root = tmp_root("gc-drive1");
        let err = exercise_shared_tier_gc(&root, 1).unwrap_err();
        assert!(err.to_string().contains("shard_count >= 2"));
        let _ = fs::remove_dir_all(&root);
    }

    #[test]
    fn reclaim_shard_refused_on_non_owner() {
        let root = tmp_root("gc-nonowner");
        let shared: Arc<dyn SharedSegmentBackend> =
            Arc::new(FilesystemSharedBackend::open(root.join("shared")).unwrap());
        // A replica that owns shard 1 cannot reclaim shard 0.
        let replica = SharedTierEventLog::open(
            root.join("local"),
            ShardOwnership::new(1, 2).unwrap(),
            Arc::clone(&shared),
            root.join("coldload"),
        )
        .unwrap();
        let routed = replica
            .reclaim_shard(0, &SegmentGcPolicy::enabled(2))
            .unwrap();
        assert_eq!(routed.owner_shard(), Some(0));
        let _ = fs::remove_dir_all(&root);
    }

    #[test]
    fn crash_window_reconcile_realigns_owner_to_shared_watermark() {
        // Simulate a crash AFTER the shared watermark was committed but BEFORE the
        // owner reclaimed its local segments: the owner's local store is ahead of
        // the shared watermark and would over-serve reclaimed sequences that a
        // non-owner already skips. `reconcile_owned_shard` must realign it.
        let root = tmp_root("gc-crashwindow");
        let shared: Arc<dyn SharedSegmentBackend> =
            Arc::new(FilesystemSharedBackend::open(root.join("shared")).unwrap());
        let seg = 300u64;
        let owner = SharedTierEventLog::open_with_segment_size(
            root.join("local-0"),
            ShardOwnership::new(0, 2).unwrap(),
            Arc::clone(&shared),
            root.join("coldload-0"),
            seg,
        )
        .unwrap();
        let exec0 = one_execution_per_shard(2)
            .into_iter()
            .find(|(_, s)| *s == 0)
            .map(|(id, _)| id)
            .unwrap();
        for i in 1..=30 {
            owner
                .append(&EventLogAppendRequest {
                    execution_id: exec0.clone(),
                    transaction_id: format!("c-{i:05}"),
                    payload: format!("payload-{i:05}-0123456789abcdef0123456789abcdef"),
                })
                .unwrap();
        }
        // Determine a mid segment id + its last seq from the owner's own view, then
        // publish ONLY the shared watermark (simulating a crash before local +
        // shared-object reclamation).
        let driver = owner.local().owned_driver(0).unwrap();
        let boundary = driver
            .plan_reclaim_boundary(&SegmentGcPolicy::enabled(2))
            .unwrap();
        // Give the plan an interest watermark first: ack ~3/4.
        owner
            .tail(
                0,
                &EventLogTailRequest {
                    consumer: "projector".to_string(),
                    transaction_id: "t".to_string(),
                    limit: 30,
                },
            )
            .unwrap();
        owner
            .ack(
                0,
                &EventLogAckRequest {
                    consumer: "projector".to_string(),
                    transaction_id: "a".to_string(),
                    sequence: 22,
                },
            )
            .unwrap();
        assert!(boundary.is_none(), "no interest before the ack");
        let (seq, segment) = driver
            .plan_reclaim_boundary(&SegmentGcPolicy::enabled(2))
            .unwrap()
            .expect("a boundary once a consumer has acked");
        // Commit ONLY the shared watermark — local segments still present (crash).
        shared.put_reclaim_watermark(0, seq, segment).unwrap();

        // A non-owner cold-load already skips <= watermark (coherent immediately).
        let nonowner = SharedTierEventLog::open_with_segment_size(
            root.join("local-nonowner"),
            ShardOwnership::new(1, 2).unwrap(),
            Arc::clone(&shared),
            root.join("coldload-nonowner"),
            seg,
        )
        .unwrap();
        let cold = nonowner
            .read_execution(&EventLogReadExecutionRequest {
                execution_id: exec0.clone(),
                after: None,
                limit: 100,
            })
            .unwrap();
        let cold_min = cold.outcome.records.first().map(|r| r.global_sequence);
        assert_eq!(
            cold_min,
            Some(seq + 1),
            "non-owner skips reclaimed via watermark"
        );

        // The owner, still holding local segments, realigns on reconcile.
        owner.reconcile_owned_shard(0).unwrap();
        let owner_scan = owner
            .scan_shard(
                0,
                &EventLogScanRequest {
                    after: None,
                    limit: 100,
                },
            )
            .unwrap();
        let owner_min = owner_scan
            .outcome
            .records
            .first()
            .map(|r| r.global_sequence);
        assert_eq!(
            owner_min,
            Some(seq + 1),
            "owner realigned to the shared watermark"
        );
        // Owner + non-owner now agree.
        let owner_seqs: Vec<u64> = owner_scan
            .outcome
            .records
            .iter()
            .map(|r| r.global_sequence)
            .collect();
        let cold_seqs: Vec<u64> = cold
            .outcome
            .records
            .iter()
            .map(|r| r.global_sequence)
            .collect();
        assert_eq!(owner_seqs, cold_seqs);
        let _ = fs::remove_dir_all(&root);
    }
}
