//! **Durable claim progress — the writer-restart resume seam (noetl/ai-meta#208).**
//!
//! [`SubjectConsumerGroup::committed_cursor`](crate::group::SubjectConsumerGroup::committed_cursor)
//! is the contiguous acked-through sort key: every record at or below it is acked
//! and no longer in flight. It has always been *documented* as the
//! durable-progress seam, but nothing persisted it — so a
//! [`ClaimCoordinator`](crate::claim::ClaimCoordinator) rebuilt after a writer
//! restart started from `from_cursor = 0` and **re-delivered the whole shard
//! log**, including long-completed commands. In kind that showed as
//! `ehdb_feed_shard_lag{shard="0"} 2738` immediately after a routine restart,
//! draining at ~1 record/s because every stale record costs a full round-trip to
//! the control plane to learn it is already claimed. Dispatch is effectively
//! stalled for the length of the replay, and the replay grows with the log.
//!
//! A [`CursorStore`] is the missing half: a small file next to the shard's log
//! (so it lives on the writer's own volume, the same durability domain as the
//! log itself) holding the last-persisted committed cursor.
//!
//! **Crash-safe write.** [`store`](CursorStore::store) writes a temp file,
//! `fsync`s it, renames it over the live path, then `fsync`s the directory — so a
//! crash mid-write leaves either the previous cursor or the new one, never a torn
//! file. A truncated or unparsable file is treated as *absent* rather than fatal:
//! resuming from the fallback is always safe, a hard failure to open the bus is
//! not.
//!
//! **Which way it is allowed to be wrong.** The cursor is persisted
//! periodically, so after a crash it can lag the true committed position by up to
//! one persist interval. That direction is safe: the group re-delivers a handful
//! of already-acked commands, the control plane answers "already claimed", and
//! nothing is lost — the same at-least-once shape the `ack_wait` redelivery path
//! already has. The store therefore **never** moves the cursor backwards
//! ([`store`](CursorStore::store) is monotonic per process) and callers must
//! never persist a value ahead of `committed_cursor()`.

use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use serde::{Deserialize, Serialize};

/// The on-disk shape. Versioned so a later slice can extend it (per-subject
/// cursors, a member table) without a silent misread.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct CursorFile {
    version: u32,
    shard: u32,
    committed_cursor: u64,
}

const CURSOR_FILE_VERSION: u32 = 1;

/// The durable home of one shard's claim-group committed cursor.
///
/// Construct with [`open`](Self::open) against the writer's data directory; the
/// file is `claim-cursor.shard-<n>.json` beside the shard's log.
#[derive(Debug)]
pub struct CursorStore {
    path: PathBuf,
    shard: u32,
    /// Highest value written by this process — the monotonic guard.
    written: AtomicU64,
}

impl CursorStore {
    /// The store for `shard` under `dir` (the writer's data directory, i.e. its
    /// volume). Creates `dir` if it does not exist; does not create the cursor
    /// file until the first [`store`](Self::store).
    pub fn open(dir: impl AsRef<Path>, shard: u32) -> io::Result<Self> {
        let dir = dir.as_ref();
        std::fs::create_dir_all(dir)?;
        Ok(Self {
            path: dir.join(format!("claim-cursor.shard-{shard}.json")),
            shard,
            written: AtomicU64::new(0),
        })
    }

    /// The file this store persists to.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// The shard this store tracks.
    pub fn shard(&self) -> u32 {
        self.shard
    }

    /// The persisted cursor, or `None` when nothing has been stored yet.
    ///
    /// A missing, empty, unparsable, wrong-version or wrong-shard file all read
    /// as `None`: the caller then resumes from its fallback (the shard tail),
    /// which is strictly better than refusing to serve the bus.
    pub fn load(&self) -> io::Result<Option<u64>> {
        let bytes = match std::fs::read(&self.path) {
            Ok(b) => b,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(e),
        };
        let parsed: Option<CursorFile> = serde_json::from_slice(&bytes).ok();
        Ok(parsed
            .filter(|f| f.version == CURSOR_FILE_VERSION && f.shard == self.shard)
            .map(|f| f.committed_cursor))
    }

    /// Persist `cursor` atomically. A no-op when `cursor` is not ahead of the
    /// highest value this process already wrote (monotonic: the cursor may only
    /// advance, so a late/duplicate call can never rewind durable progress).
    /// Returns `true` when the file was rewritten.
    pub fn store(&self, cursor: u64) -> io::Result<bool> {
        // `fetch_max` returns the previous value; skip the write when it was
        // already at or past `cursor`.
        if self.written.fetch_max(cursor, Ordering::Relaxed) >= cursor {
            return Ok(false);
        }
        let body = serde_json::to_vec(&CursorFile {
            version: CURSOR_FILE_VERSION,
            shard: self.shard,
            committed_cursor: cursor,
        })
        .map_err(crate::io_err)?;

        let tmp = self.path.with_extension("json.tmp");
        {
            let mut f = std::fs::File::create(&tmp)?;
            io::Write::write_all(&mut f, &body)?;
            f.sync_all()?;
        }
        std::fs::rename(&tmp, &self.path)?;
        // Durable rename: without the directory `fsync` the new name can be lost
        // on a host crash even though the file contents were synced.
        if let Some(parent) = self.path.parent() {
            if let Ok(dir) = std::fs::File::open(parent) {
                let _ = dir.sync_all();
            }
        }
        Ok(true)
    }
}

/// Where a rebuilt claim group starts when no cursor has been persisted yet —
/// the **first** start after this fix ships, or a wiped volume.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum CursorFallback {
    /// Start at the shard's current tip: only commands appended *after* the
    /// restart are delivered. The default, and the right posture for a bus whose
    /// producer keeps its own record of outstanding work — the control plane's
    /// orphaned-command guardrail (noetl/ai-meta#171) re-issues a command whose
    /// claim never landed, so a command skipped by this one-time gap is
    /// recovered, whereas replaying the whole log stalls dispatch every restart.
    #[default]
    Tail,
    /// Replay the shard from the beginning — the pre-#208 behaviour, kept as an
    /// explicit escape hatch (a deployment that would rather pay the replay than
    /// lean on the producer's guardrail).
    Beginning,
}

impl CursorFallback {
    /// Parse an env-style value (`tail` / `beginning`); anything else is
    /// [`Tail`](Self::Tail).
    pub fn from_env_value(value: &str) -> Self {
        match value.trim().to_ascii_lowercase().as_str() {
            "beginning" | "begin" | "zero" | "0" => Self::Beginning,
            _ => Self::Tail,
        }
    }
}

/// How a coordinator's start cursor was chosen — logged by the host so a restart
/// says in one line whether it resumed or fell back.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CursorOrigin {
    /// Read back from the [`CursorStore`] — a real resume.
    Persisted,
    /// No stored cursor: started at the shard tip.
    FallbackTail,
    /// No stored cursor: replaying the shard from 0.
    FallbackBeginning,
}

impl CursorOrigin {
    /// A short, stable label for logs / metrics.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Persisted => "persisted",
            Self::FallbackTail => "fallback_tail",
            Self::FallbackBeginning => "fallback_beginning",
        }
    }
}

/// Everything a restart needs to say about where it resumed — the
/// unambiguous form of the one-line restart signal (noetl/ai-meta#208).
///
/// **Why the origin alone is not enough.** The first prod writer restart on the
/// EHDB bus logged `origin="persisted" from_cursor=0`, which reads exactly like a
/// replay from the beginning of the shard — the failure the resume fix exists to
/// prevent. It was not one: the reopened log's tip was itself low (sort keys are
/// assigned from the engine's recovered sequence, so the number is relative to
/// what the manifest recovered, not an absolute all-time count), the stored
/// cursor was clamped down to it, and nothing was re-served. Proving that took
/// arithmetic across two scrapes of `ehdb_feed_shard_committed`. A runbook cannot
/// rely on a signal that needs arithmetic to disambiguate.
///
/// So the report carries the three numbers that make the outcome self-evident —
/// what was stored, what the reopened log's tip was, what was actually used — and
/// derives the two questions an operator asks: was the cursor clamped, and did
/// anything replay.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ResumeReport {
    /// The shard this coordinator serves.
    pub shard: u32,
    /// The cursor read back from the [`CursorStore`], before clamping. `None`
    /// when nothing was stored (a first start or a wiped volume).
    pub stored_cursor: Option<u64>,
    /// The reopened log's tip (`global_sequence`) at resume time — the ceiling
    /// the stored cursor is clamped to.
    pub tip: u64,
    /// The cursor the group actually started from, after clamping / fallback.
    pub from_cursor: u64,
    /// How `from_cursor` was chosen.
    pub origin: CursorOrigin,
}

impl ResumeReport {
    /// Was a stored cursor clamped down to the reopened log's tip? True means
    /// the log recovered less than the cursor covered — expected after an
    /// unsealed-tail loss, and the reason a "persisted" resume can start at a
    /// number lower than the one last persisted.
    pub fn clamped(&self) -> bool {
        matches!(self.stored_cursor, Some(c) if c > self.tip)
    }

    /// How many records the group will re-serve from the existing log — the
    /// replay this fix exists to keep at zero. `tip - from_cursor`.
    pub fn replay_records(&self) -> u64 {
        self.tip.saturating_sub(self.from_cursor)
    }

    /// Did this start replay anything already in the log?
    pub fn replayed(&self) -> bool {
        self.replay_records() > 0
    }
}

impl std::fmt::Display for ResumeReport {
    /// The restart line, unambiguous on its own:
    /// `shard=0 origin=persisted stored_cursor=408 tip=165 from_cursor=165
    /// clamped=true replay=false replay_records=0`.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "shard={} origin={}", self.shard, self.origin.as_str())?;
        match self.stored_cursor {
            Some(c) => write!(f, " stored_cursor={c}")?,
            None => write!(f, " stored_cursor=none")?,
        }
        write!(
            f,
            " tip={} from_cursor={} clamped={} replay={} replay_records={}",
            self.tip,
            self.from_cursor,
            self.clamped(),
            self.replayed(),
            self.replay_records()
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dir(tag: &str) -> PathBuf {
        static N: AtomicU64 = AtomicU64::new(0);
        let n = N.fetch_add(1, Ordering::Relaxed);
        std::env::temp_dir().join(format!("ehdb-cursor-{tag}-{}-{n}", std::process::id()))
    }

    #[test]
    fn absent_then_round_trips() {
        let d = dir("roundtrip");
        let store = CursorStore::open(&d, 3).unwrap();
        assert_eq!(store.load().unwrap(), None);
        assert!(store.store(41).unwrap());
        assert_eq!(store.load().unwrap(), Some(41));

        // A fresh handle (the restarted process) reads the same value back.
        let reopened = CursorStore::open(&d, 3).unwrap();
        assert_eq!(reopened.load().unwrap(), Some(41));
    }

    #[test]
    fn store_is_monotonic() {
        let store = CursorStore::open(dir("monotonic"), 0).unwrap();
        assert!(store.store(100).unwrap());
        // A stale/duplicate persist must not rewind durable progress.
        assert!(!store.store(50).unwrap());
        assert!(!store.store(100).unwrap());
        assert_eq!(store.load().unwrap(), Some(100));
        assert!(store.store(101).unwrap());
        assert_eq!(store.load().unwrap(), Some(101));
    }

    #[test]
    fn a_torn_file_reads_as_absent() {
        let d = dir("torn");
        let store = CursorStore::open(&d, 0).unwrap();
        store.store(7).unwrap();
        std::fs::write(store.path(), b"{\"version\":1,\"shard\":0,\"comm").unwrap();
        assert_eq!(store.load().unwrap(), None);
    }

    #[test]
    fn another_shards_file_is_not_adopted() {
        let d = dir("shard-mismatch");
        CursorStore::open(&d, 0).unwrap().store(9).unwrap();
        // Same directory, different shard: distinct file, so nothing is read.
        assert_eq!(CursorStore::open(&d, 1).unwrap().load().unwrap(), None);
        // And a file whose recorded shard disagrees is ignored rather than trusted.
        let store = CursorStore::open(&d, 1).unwrap();
        std::fs::write(
            store.path(),
            b"{\"version\":1,\"shard\":0,\"committed_cursor\":9}",
        )
        .unwrap();
        assert_eq!(store.load().unwrap(), None);
    }

    #[test]
    fn fallback_parses_from_env_value() {
        assert_eq!(CursorFallback::from_env_value("tail"), CursorFallback::Tail);
        assert_eq!(CursorFallback::from_env_value(""), CursorFallback::Tail);
        assert_eq!(
            CursorFallback::from_env_value(" Beginning "),
            CursorFallback::Beginning
        );
        assert_eq!(CursorFallback::default(), CursorFallback::Tail);
    }
}
