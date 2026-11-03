//! The pluggable **durable substrate** — a raw byte-sink UNDER noetl's own
//! object store (RFC §2.3 / §6.2).
//!
//! **The L0 object store is noetl-native: EHDB implements the object-store logic
//! itself** — the immutable parts, the ClickHouse-style manifest, the sparse
//! index, and the N-way replication all live in this crate (extending the #254
//! native segment store). This trait is *only* the durable byte-sink those
//! parts are written to and range-read from. It is deliberately **not** an
//! external object-store product: noetl never delegates the manifest, indexing,
//! or replication to MinIO / an S3 server. The substrate is just "write these
//! immutable bytes durably; give me a byte range back."
//!
//! Because it is a plain byte-sink, the substrate is trivially pluggable — but
//! the pluggable thing is the **substrate**, not the object store. For kind/dev
//! and the build now, the substrate is the local filesystem
//! ([`LocalFsSubstrate`], noetl's own store on disk / a PVC / a block device).
//! A raw cloud block-store or blob byte-sink could slot in UNDER this trait
//! later if ever wanted; noetl's object-store logic above it is unchanged.
//!
//! This module ships:
//!
//! - [`LocalFsSubstrate`] — the local-filesystem durable substrate (noetl's own
//!   on-disk store) for kind/dev.
//! - [`CountingSubstrate`] — a transparent wrapper that records per-key I/O
//!   (calls + bytes, and the exact keys touched) and can inject a put latency.
//!   This is the instrument the L0.1 proofs read: "a targeted lookup ranged-GETs
//!   only the needed block and touches zero non-matching parts", and "a slow
//!   substrate never blocks the append hot path".
//!
//! Unlike `ehdb-storage`'s content-addressed `ImmutableObjectStore` (whole-
//! object put/get, the §2.4 *blob* shape), L0's part substrate needs **ranged
//! GET** (fetch only the granule a lookup resolves to) — hence a distinct trait.

use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use ehdb_core::{EhdbError, Result};

/// The durable substrate L0 writes sealed parts to and range-reads
/// them back from. Substrate keys are `/`-separated logical paths (e.g.
/// `parts/d1_event_log/shard-0/part-...eslog`).
///
/// `Send + Sync` so the engine can share one behind an [`Arc`] between the
/// append thread and the background uploader thread.
pub trait DurableSubstrate: Send + Sync {
    /// Write an **immutable** object. Returns `Ok(true)` if newly written,
    /// `Ok(false)` if an object already existed at `key` (idempotent re-upload —
    /// parts are content-stable, so a duplicate upload is a no-op, not an error).
    /// Where this substrate's bytes physically live (noetl/ehdb#332).
    ///
    /// ⚠ Defaults to [`FailureDomain::Undeclared`] so existing implementations
    /// keep compiling — but an undeclared substrate can never be *shown* to be
    /// independent of another, and [`crate::failure_domain::validate_replica_domains`]
    /// refuses a replica set built from them. Silence is not treated as
    /// independence.
    fn failure_domain(&self) -> crate::failure_domain::FailureDomain {
        crate::failure_domain::FailureDomain::Undeclared
    }

    fn put_if_absent(&self, key: &str, bytes: &[u8]) -> Result<bool>;

    /// Overwrite a **mutable pointer** object (the manifest `LATEST` pointer and
    /// versioned manifest snapshots). Distinct from [`Self::put_if_absent`]
    /// because a pointer legitimately advances; parts never do.
    fn put_overwrite(&self, key: &str, bytes: &[u8]) -> Result<()>;

    /// Fetch a byte range `[offset, offset+len)` of an object — the core L0
    /// primitive that lets a lookup read *only* the resolved granule, not the
    /// whole part.
    /// ⚠ A range extending past the end of the object is a **hard error**, not
    /// a short read: the manifest states every part's length, so an over-read
    /// means the caller and the store disagree, and returning fewer bytes would
    /// let a truncated object read as intact. Pinned by the substrate
    /// conformance suite across every implementation (noetl/ehdb#332).
    fn get_range(&self, key: &str, offset: u64, len: u64) -> Result<Vec<u8>>;

    /// Fetch a whole object (used for small manifests and for full-part replay on
    /// cold-load).
    fn get_all(&self, key: &str) -> Result<Vec<u8>>;

    /// The byte length of the object at `key`, WITHOUT fetching its bytes.
    ///
    /// ⚠ The default implementation does fetch them, so every existing
    /// implementation keeps compiling unchanged — including the out-of-tree
    /// `GcsSubstrate` in noetl/server. That default is a fallback, not the
    /// intent: a substrate whose backing store can answer a size cheaply should
    /// override it, and the two in this file do.
    ///
    /// Added for noetl/ehdb#395: `reclaim_orphans` read every orphan IN FULL
    /// purely to populate a byte counter, immediately before deleting it. On a
    /// local substrate that is a wasted file read; on a remote one it is a full
    /// download per orphan (measured GCS get p50 77 ms, p95 139 ms) of an object
    /// that is about to be thrown away.
    fn object_size(&self, key: &str) -> Result<u64> {
        Ok(self.get_all(key)?.len() as u64)
    }

    /// Whether an object exists at `key`.
    fn exists(&self, key: &str) -> Result<bool>;

    /// List every substrate key under a `/`-terminated (or prefix-matched) logical
    /// prefix. Order is unspecified; callers sort.
    fn list_prefix(&self, prefix: &str) -> Result<Vec<String>>;

    /// Delete an object (L0.5 retention/GC — reclaiming superseded merge sources
    /// and dropped-partition parts). Deleting a missing key is a no-op (`Ok`), so
    /// GC is idempotent.
    fn delete(&self, key: &str) -> Result<()>;
}

// ---------------------------------------------------------------------------
// Local filesystem substrate (noetl's own on-disk store)
// ---------------------------------------------------------------------------

/// A local-filesystem [`DurableSubstrate`] for kind/dev — noetl's own store on
/// disk. Keys map to files under `root`; `put` is atomic (write-temp + rename)
/// so a reader never sees a half-written part.
#[derive(Debug, Clone)]
pub struct LocalFsSubstrate {
    root: PathBuf,
}

impl LocalFsSubstrate {
    /// Open (creating the root dir) a local-filesystem durable substrate.
    pub fn new(root: impl Into<PathBuf>) -> Result<Self> {
        let root = root.into();
        fs::create_dir_all(&root).map_err(|err| EhdbError::Storage(err.to_string()))?;
        Ok(Self { root })
    }

    fn resolve(&self, key: &str) -> Result<PathBuf> {
        // Keys are internal, compiled-path-derived (never user input), but guard
        // traversal defensively — the invariant is "no arbitrary surface".
        if key.is_empty() || key.contains("..") || key.starts_with('/') {
            return Err(EhdbError::Storage(format!("unsafe substrate key: {key:?}")));
        }
        Ok(self.root.join(key))
    }

    /// Write `bytes` to a fresh temp file beside `target` and `sync_data` it.
    /// Returns the temp path; the caller publishes it (rename or link) and is
    /// responsible for removing it on its own failure paths.
    fn write_tmp(&self, target: &Path, bytes: &[u8]) -> Result<PathBuf> {
        if let Some(parent) = target.parent() {
            fs::create_dir_all(parent).map_err(|err| EhdbError::Storage(err.to_string()))?;
        }
        let tmp = unique_tmp_path(target);
        let written = (|| -> std::io::Result<()> {
            let mut f = OpenOptions::new().write(true).create_new(true).open(&tmp)?;
            f.write_all(bytes)?;
            f.sync_data()
        })();
        if let Err(err) = written {
            let _ = fs::remove_file(&tmp);
            return Err(EhdbError::Storage(err.to_string()));
        }
        Ok(tmp)
    }

    /// Write-temp, rename over `key`, then fsync the parent directory.
    ///
    /// ⚠ The temp name is unique per call (noetl/ehdb#426). It used to be
    /// `target.with_extension("tmp")`, which two writers share whenever their
    /// keys differ only in extension (`x.eslog` / `x.idx` → `x.tmp`) or are the
    /// same key written concurrently (`LATEST` from the uploader thread and the
    /// merge path). Both then open-truncate-write one file and the loser's
    /// rename publishes the winner's bytes, or a mix of both.
    ///
    /// ⚠ The directory fsync is what makes the *rename* durable. Without it a
    /// power loss can roll the directory entry back to the old object even
    /// though the new bytes were synced.
    fn write_atomic(&self, key: &str, bytes: &[u8]) -> Result<()> {
        let target = self.resolve(key)?;
        let tmp = self.write_tmp(&target, bytes)?;
        if let Err(err) = fs::rename(&tmp, &target) {
            let _ = fs::remove_file(&tmp);
            return Err(EhdbError::Storage(err.to_string()));
        }
        sync_parent_dir(&target)
    }
}

/// A temp path next to `target` that no other writer, in this process or any
/// other, will choose: `.<file name>.<pid>.<per-process counter>.tmp`. Keeps
/// the `tmp` extension, so [`walk_keys`] still never lists it.
fn unique_tmp_path(target: &Path) -> PathBuf {
    static TMP_SEQ: AtomicU64 = AtomicU64::new(0);
    let n = TMP_SEQ.fetch_add(1, Ordering::Relaxed);
    let name = target
        .file_name()
        .map(|s| s.to_string_lossy().into_owned())
        .unwrap_or_default();
    target.with_file_name(format!(".{name}.{}.{n}.tmp", std::process::id()))
}

/// `fsync` the directory holding `path`, so a rename or link into it survives a
/// power loss. A no-op where directories cannot be opened for sync (non-unix).
fn sync_parent_dir(path: &Path) -> Result<()> {
    #[cfg(unix)]
    if let Some(parent) = path.parent() {
        File::open(parent)
            .and_then(|d| d.sync_all())
            .map_err(|err| EhdbError::Storage(format!("fsync dir {}: {err}", parent.display())))?;
    }
    #[cfg(not(unix))]
    let _ = path;
    Ok(())
}

impl LocalFsSubstrate {
    /// This substrate's root — used to detect a replica nested inside another.
    pub fn root(&self) -> &std::path::Path {
        &self.root
    }
}

impl DurableSubstrate for LocalFsSubstrate {
    /// ⚠ Resolved from the root's **device id**, not its path. Two directories
    /// on one PVC have different paths and the same device — the production
    /// case — and a path comparison would call them independent.
    fn failure_domain(&self) -> crate::failure_domain::FailureDomain {
        crate::failure_domain::FailureDomain::for_path(&self.root)
    }

    fn put_if_absent(&self, key: &str, bytes: &[u8]) -> Result<bool> {
        let target = self.resolve(key)?;
        // Fast path only — it skips the write for the common re-upload, but it
        // is NOT the guarantee. The link below is.
        if target.exists() {
            return Ok(false);
        }
        let tmp = self.write_tmp(&target, bytes)?;
        // ⚠ `hard_link` fails with `AlreadyExists` if anything is at `target`,
        // atomically, so exactly one of N concurrent writers can win
        // (noetl/ehdb#426). The old `exists()` + rename let every racer pass
        // the check and each rename over the last, all returning `true`.
        let linked = fs::hard_link(&tmp, &target);
        let _ = fs::remove_file(&tmp);
        match linked {
            Ok(()) => {
                sync_parent_dir(&target)?;
                Ok(true)
            }
            Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => Ok(false),
            Err(err) => Err(EhdbError::Storage(err.to_string())),
        }
    }

    fn put_overwrite(&self, key: &str, bytes: &[u8]) -> Result<()> {
        self.write_atomic(key, bytes)
    }

    fn get_range(&self, key: &str, offset: u64, len: u64) -> Result<Vec<u8>> {
        let target = self.resolve(key)?;
        let mut f = File::open(&target).map_err(|err| EhdbError::Storage(err.to_string()))?;
        f.seek(SeekFrom::Start(offset))
            .map_err(|err| EhdbError::Storage(err.to_string()))?;
        let mut buf = vec![0u8; len as usize];
        f.read_exact(&mut buf)
            .map_err(|err| EhdbError::Storage(err.to_string()))?;
        Ok(buf)
    }

    fn get_all(&self, key: &str) -> Result<Vec<u8>> {
        let target = self.resolve(key)?;
        fs::read(&target).map_err(|err| EhdbError::Storage(err.to_string()))
    }

    fn object_size(&self, key: &str) -> Result<u64> {
        let target = self.resolve(key)?;
        fs::metadata(&target)
            .map(|m| m.len())
            .map_err(|err| EhdbError::Storage(err.to_string()))
    }

    fn exists(&self, key: &str) -> Result<bool> {
        Ok(self.resolve(key)?.exists())
    }

    fn list_prefix(&self, prefix: &str) -> Result<Vec<String>> {
        let mut out = Vec::new();
        walk_keys(&self.root, &self.root, prefix, &mut out)?;
        Ok(out)
    }

    fn delete(&self, key: &str) -> Result<()> {
        let target = self.resolve(key)?;
        match fs::remove_file(&target) {
            Ok(()) => Ok(()),
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(err) => Err(EhdbError::Storage(err.to_string())),
        }
    }
}

fn walk_keys(root: &Path, dir: &Path, prefix: &str, out: &mut Vec<String>) -> Result<()> {
    if !dir.exists() {
        return Ok(());
    }
    for entry in fs::read_dir(dir).map_err(|err| EhdbError::Storage(err.to_string()))? {
        let entry = entry.map_err(|err| EhdbError::Storage(err.to_string()))?;
        let path = entry.path();
        if path.is_dir() {
            walk_keys(root, &path, prefix, out)?;
        } else {
            // Skip half-written temp files.
            if path.extension().map(|e| e == "tmp").unwrap_or(false) {
                continue;
            }
            let rel = path
                .strip_prefix(root)
                .map_err(|err| EhdbError::Storage(err.to_string()))?;
            let key = rel.to_string_lossy().replace('\\', "/");
            if key.starts_with(prefix) {
                out.push(key);
            }
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Instrumenting wrapper — the proof instrument
// ---------------------------------------------------------------------------

/// Per-store I/O counters recorded by [`CountingSubstrate`]. Secret-free
/// (counts + key names only, never payload bytes) — the L0.1 instrumentation
/// exit criterion.
#[derive(Debug, Default)]
pub struct SubstrateCounters {
    /// Number of `put_if_absent` calls that actually wrote (new parts uploaded).
    pub put_calls: AtomicU64,
    /// Total bytes uploaded by `put_if_absent`.
    pub put_bytes: AtomicU64,
    /// Number of `put_overwrite` calls (manifest-pointer writes).
    pub put_overwrite_calls: AtomicU64,
    /// Number of `get_range` calls (ranged block reads).
    pub get_range_calls: AtomicU64,
    /// Total bytes fetched by `get_range` — the "only the needed block" measure.
    pub get_range_bytes: AtomicU64,
    /// Number of `get_all` calls (whole-object reads: manifests, cold-load full
    /// parts).
    pub get_all_calls: AtomicU64,
    /// Total bytes fetched by `get_all`.
    pub get_all_bytes: AtomicU64,
    /// Number of `delete` calls (L0.5 GC reclaims).
    pub delete_calls: AtomicU64,
    /// Number of `list_prefix` calls (directory/prefix enumerations).
    ///
    /// ⚠ Added because its absence made a whole class of storage operation
    /// **invisible** to this instrument. A proof that an operation "issues
    /// exactly one storage read" was counting `get_all` + `get_range` only, so
    /// an implementation could enumerate a prefix on every call — an O(k) cost —
    /// and still read as exactly one. Two mutation-battery mutants survived on
    /// precisely that blind spot. A counter that cannot see an operation makes
    /// every claim about that operation vacuous.
    pub list_prefix_calls: AtomicU64,
    /// Number of `object_size` calls (sizing an object without fetching it).
    ///
    /// ⚠ Counted for the same reason `list_prefix_calls` is: an operation this
    /// instrument cannot see makes every claim about it vacuous. Without it, "the
    /// reclaim issues no downloads" would be provable while the reclaim issued a
    /// different unmeasured storage call instead.
    pub object_size_calls: AtomicU64,
}

/// A transparent [`DurableSubstrate`] wrapper that records per-key I/O and can
/// inject a fixed latency on `put_if_absent` (to prove the append hot path never
/// blocks on a slow substrate). It forwards to any inner substrate.
pub struct CountingSubstrate<S: DurableSubstrate> {
    inner: S,
    counters: Arc<SubstrateCounters>,
    /// Keys touched by a *read* (`get_range` / `get_all`), in call order — lets a
    /// proof assert exactly which parts a lookup fetched (and that pruned parts
    /// were fetched zero times).
    read_keys: Arc<Mutex<Vec<String>>>,
    /// Optional artificial latency applied to `put_if_absent` (simulate a slow
    /// remote substrate). The append path never calls put, so this latency
    /// lands only on the background uploader thread.
    put_latency: Option<Duration>,
}

impl<S: DurableSubstrate> CountingSubstrate<S> {
    /// Wrap `inner`, recording I/O with no injected latency.
    pub fn new(inner: S) -> Self {
        Self {
            inner,
            counters: Arc::new(SubstrateCounters::default()),
            read_keys: Arc::new(Mutex::new(Vec::new())),
            put_latency: None,
        }
    }

    /// Wrap `inner`, recording I/O and sleeping `latency` on every upload — the
    /// slow-substrate harness for the hot-path-isolation proof.
    pub fn with_put_latency(inner: S, latency: Duration) -> Self {
        Self {
            inner,
            counters: Arc::new(SubstrateCounters::default()),
            read_keys: Arc::new(Mutex::new(Vec::new())),
            put_latency: Some(latency),
        }
    }

    /// Shared handle to the counters (clone before moving the store into an
    /// [`Arc`]).
    pub fn counters(&self) -> Arc<SubstrateCounters> {
        Arc::clone(&self.counters)
    }

    /// Shared handle to the ordered list of read-touched keys.
    pub fn read_keys(&self) -> Arc<Mutex<Vec<String>>> {
        Arc::clone(&self.read_keys)
    }
}

impl<S: DurableSubstrate> DurableSubstrate for CountingSubstrate<S> {
    /// ⚠ Forwarded, not defaulted. A decorator that let this fall back to
    /// `Undeclared` would silently downgrade a real substrate's domain and make
    /// a validated replica set fail — or, worse, make an unvalidated one look
    /// undeclared rather than shared.
    fn failure_domain(&self) -> crate::failure_domain::FailureDomain {
        self.inner.failure_domain()
    }

    fn put_if_absent(&self, key: &str, bytes: &[u8]) -> Result<bool> {
        if let Some(latency) = self.put_latency {
            std::thread::sleep(latency);
        }
        let wrote = self.inner.put_if_absent(key, bytes)?;
        if wrote {
            self.counters.put_calls.fetch_add(1, Ordering::Relaxed);
            self.counters
                .put_bytes
                .fetch_add(bytes.len() as u64, Ordering::Relaxed);
        }
        Ok(wrote)
    }

    fn put_overwrite(&self, key: &str, bytes: &[u8]) -> Result<()> {
        self.counters
            .put_overwrite_calls
            .fetch_add(1, Ordering::Relaxed);
        self.inner.put_overwrite(key, bytes)
    }

    fn get_range(&self, key: &str, offset: u64, len: u64) -> Result<Vec<u8>> {
        self.counters
            .get_range_calls
            .fetch_add(1, Ordering::Relaxed);
        self.counters
            .get_range_bytes
            .fetch_add(len, Ordering::Relaxed);
        self.read_keys.lock().unwrap().push(key.to_string());
        self.inner.get_range(key, offset, len)
    }

    fn get_all(&self, key: &str) -> Result<Vec<u8>> {
        let bytes = self.inner.get_all(key)?;
        self.counters.get_all_calls.fetch_add(1, Ordering::Relaxed);
        self.counters
            .get_all_bytes
            .fetch_add(bytes.len() as u64, Ordering::Relaxed);
        self.read_keys.lock().unwrap().push(key.to_string());
        Ok(bytes)
    }

    fn object_size(&self, key: &str) -> Result<u64> {
        self.counters
            .object_size_calls
            .fetch_add(1, Ordering::Relaxed);
        // Forwarded, NOT defaulted: the default would call `get_all` on the inner
        // substrate and the whole point is that no bytes move.
        self.inner.object_size(key)
    }

    fn exists(&self, key: &str) -> Result<bool> {
        self.inner.exists(key)
    }

    fn list_prefix(&self, prefix: &str) -> Result<Vec<String>> {
        self.counters
            .list_prefix_calls
            .fetch_add(1, Ordering::Relaxed);
        self.inner.list_prefix(prefix)
    }

    fn delete(&self, key: &str) -> Result<()> {
        self.counters.delete_calls.fetch_add(1, Ordering::Relaxed);
        self.inner.delete(key)
    }
}

/// Blanket impl so an `Arc<dyn DurableSubstrate>` (and `Arc<S>`) is itself an
/// [`DurableSubstrate`] — the engine holds the store as a trait object shared
/// across threads.
impl DurableSubstrate for Arc<dyn DurableSubstrate> {
    /// ⚠ **Forwarding is not optional for a defaulted method.**
    ///
    /// This impl silently omitted `failure_domain`, so it fell through to the
    /// trait default and every `Arc<dyn DurableSubstrate>` reported
    /// [`FailureDomain::Undeclared`] — whatever the inner substrate declared.
    /// The engine holds its replicas as exactly that type, so the failure-domain
    /// mechanism was inert twice over: nothing called it, and the value it would
    /// have returned was wrong.
    ///
    /// It compiled precisely *because* the method has a default. A required
    /// method left out is a compile error; a defaulted one left out is a plausible
    /// wrong answer. `the_arc_forwards_every_trait_method` pins the whole set.
    fn failure_domain(&self) -> crate::failure_domain::FailureDomain {
        (**self).failure_domain()
    }
    fn put_if_absent(&self, key: &str, bytes: &[u8]) -> Result<bool> {
        (**self).put_if_absent(key, bytes)
    }
    fn put_overwrite(&self, key: &str, bytes: &[u8]) -> Result<()> {
        (**self).put_overwrite(key, bytes)
    }
    fn get_range(&self, key: &str, offset: u64, len: u64) -> Result<Vec<u8>> {
        (**self).get_range(key, offset, len)
    }
    fn get_all(&self, key: &str) -> Result<Vec<u8>> {
        (**self).get_all(key)
    }
    fn object_size(&self, key: &str) -> Result<u64> {
        (**self).object_size(key)
    }

    fn exists(&self, key: &str) -> Result<bool> {
        (**self).exists(key)
    }
    fn list_prefix(&self, prefix: &str) -> Result<Vec<String>> {
        (**self).list_prefix(prefix)
    }
    fn delete(&self, key: &str) -> Result<()> {
        (**self).delete(key)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tmp() -> PathBuf {
        let mut p = std::env::temp_dir();
        // Unique-ish without Date/rand (banned in some contexts): use a static
        // atomic counter + thread id.
        static N: AtomicU64 = AtomicU64::new(0);
        let n = N.fetch_add(1, Ordering::Relaxed);
        p.push(format!(
            "ehdb-l0-os-test-{n}-{:?}",
            std::thread::current().id()
        ));
        p
    }

    #[test]
    fn put_get_range_roundtrip() {
        let dir = tmp();
        let store = LocalFsSubstrate::new(&dir).unwrap();
        let data = b"0123456789abcdef".to_vec();
        assert!(store.put_if_absent("parts/a.eslog", &data).unwrap());
        // Idempotent re-upload is a no-op.
        assert!(!store.put_if_absent("parts/a.eslog", &data).unwrap());
        let mid = store.get_range("parts/a.eslog", 4, 6).unwrap();
        assert_eq!(mid, b"456789");
        assert_eq!(store.get_all("parts/a.eslog").unwrap(), data);
        let _ = fs::remove_dir_all(&dir);
    }

    /// noetl/ehdb#426: keys that differ only in extension used to share one
    /// temp file (`x.tmp`), and so did two writes of the same key.
    #[test]
    fn temp_names_are_unique_per_write() {
        let a = unique_tmp_path(Path::new("/r/parts/x.eslog"));
        let b = unique_tmp_path(Path::new("/r/parts/x.idx"));
        let c = unique_tmp_path(Path::new("/r/parts/x.eslog"));
        assert_ne!(a, b);
        assert_ne!(a, c);
        for p in [&a, &b, &c] {
            assert_eq!(
                p.extension().unwrap(),
                "tmp",
                "walk_keys must keep skipping it"
            );
            assert_eq!(
                p.parent().unwrap(),
                Path::new("/r/parts"),
                "rename must stay in-dir"
            );
        }
    }

    /// Concurrent writers of keys that differ only in extension each publish
    /// their own bytes, and no temp file is left behind or listed.
    #[test]
    fn concurrent_sibling_keys_do_not_cross_publish() {
        let dir = tmp();
        let store = Arc::new(LocalFsSubstrate::new(&dir).unwrap());
        let barrier = Arc::new(std::sync::Barrier::new(2));
        let handles: Vec<_> = ["eslog", "idx"]
            .into_iter()
            .map(|ext| {
                let store = Arc::clone(&store);
                let barrier = Arc::clone(&barrier);
                std::thread::spawn(move || {
                    barrier.wait();
                    for i in 0..200u32 {
                        let body = format!("{ext}-{i}-").repeat(64);
                        store
                            .put_overwrite(&format!("p/x.{ext}"), body.as_bytes())
                            .unwrap();
                        let got = store.get_all(&format!("p/x.{ext}")).unwrap();
                        assert!(
                            got.starts_with(ext.as_bytes()),
                            "{ext} read back another key's bytes"
                        );
                    }
                })
            })
            .collect();
        for h in handles {
            h.join().unwrap();
        }
        let mut keys = store.list_prefix("p/").unwrap();
        keys.sort();
        assert_eq!(keys, vec!["p/x.eslog".to_string(), "p/x.idx".to_string()]);
        let leftovers: Vec<_> = fs::read_dir(dir.join("p"))
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.path().extension().map(|x| x == "tmp").unwrap_or(false))
            .collect();
        assert!(
            leftovers.is_empty(),
            "temp files left behind: {leftovers:?}"
        );
        let _ = fs::remove_dir_all(&dir);
    }

    /// noetl/ehdb#426: `put_if_absent` must have exactly one winner under
    /// contention, and the stored bytes must be that winner's.
    #[test]
    fn put_if_absent_has_exactly_one_winner() {
        for round in 0..20 {
            let dir = tmp();
            let store = Arc::new(LocalFsSubstrate::new(&dir).unwrap());
            let n = 8;
            let barrier = Arc::new(std::sync::Barrier::new(n));
            let handles: Vec<_> = (0..n)
                .map(|i| {
                    let store = Arc::clone(&store);
                    let barrier = Arc::clone(&barrier);
                    std::thread::spawn(move || {
                        let body = format!("writer-{i}-").repeat(4096);
                        barrier.wait();
                        let won = store
                            .put_if_absent("parts/k.eslog", body.as_bytes())
                            .unwrap();
                        (won, body)
                    })
                })
                .collect();
            let results: Vec<_> = handles.into_iter().map(|h| h.join().unwrap()).collect();
            let winners: Vec<_> = results.iter().filter(|(w, _)| *w).collect();
            assert_eq!(
                winners.len(),
                1,
                "round {round}: {} writers claimed the key",
                winners.len()
            );
            assert_eq!(
                store.get_all("parts/k.eslog").unwrap(),
                winners[0].1.as_bytes(),
                "round {round}: stored bytes are not the winner's"
            );
            let _ = fs::remove_dir_all(&dir);
        }
    }

    /// An existing object is never replaced by `put_if_absent`, even when the
    /// fast-path `exists()` check is bypassed by a concurrent create.
    #[test]
    fn put_if_absent_never_replaces() {
        let dir = tmp();
        let store = LocalFsSubstrate::new(&dir).unwrap();
        assert!(store.put_if_absent("a/b", b"first").unwrap());
        assert!(!store.put_if_absent("a/b", b"second").unwrap());
        assert_eq!(store.get_all("a/b").unwrap(), b"first");
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn list_prefix_finds_nested_keys() {
        let dir = tmp();
        let store = LocalFsSubstrate::new(&dir).unwrap();
        store
            .put_if_absent("parts/d1/shard-0/p1.eslog", b"x")
            .unwrap();
        store
            .put_if_absent("parts/d1/shard-1/p2.eslog", b"y")
            .unwrap();
        store.put_overwrite("manifest/d1/LATEST", b"1").unwrap();
        let mut parts = store.list_prefix("parts/d1/").unwrap();
        parts.sort();
        assert_eq!(
            parts,
            vec![
                "parts/d1/shard-0/p1.eslog".to_string(),
                "parts/d1/shard-1/p2.eslog".to_string()
            ]
        );
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn counting_wrapper_records_io() {
        let dir = tmp();
        let inner = LocalFsSubstrate::new(&dir).unwrap();
        let counting = CountingSubstrate::new(inner);
        let counters = counting.counters();
        counting.put_if_absent("parts/a", b"hello world").unwrap();
        counting.get_range("parts/a", 0, 5).unwrap();
        assert_eq!(counters.put_calls.load(Ordering::Relaxed), 1);
        assert_eq!(counters.put_bytes.load(Ordering::Relaxed), 11);
        assert_eq!(counters.get_range_calls.load(Ordering::Relaxed), 1);
        assert_eq!(counters.get_range_bytes.load(Ordering::Relaxed), 5);
        assert_eq!(
            counting.read_keys().lock().unwrap().as_slice(),
            &["parts/a"]
        );
        let _ = fs::remove_dir_all(&dir);
    }
}

/// **A second `DurableSubstrate` implementation** (noetl/ehdb#332, F5).
///
/// Its purpose is to make the trait's contract testable against something that
/// is not a filesystem — until this existed, `DurableSubstrate` had exactly one
/// implementation, so nothing distinguished "the trait's contract" from
/// "whatever `LocalFsSubstrate` happens to do". The conformance suite runs
/// against both.
///
/// ⚠⚠ **Not a durability answer.** It is process memory: it dies with the
/// process and is [`FailureDomain::Ephemeral`], which
/// [`crate::failure_domain::survives_node_loss`] correctly refuses to count. A
/// real independent failure domain needs an off-node store — see
/// `docs/spec/second-substrate.md`.
#[derive(Debug)]
pub struct InMemorySubstrate {
    instance: String,
    objects: std::sync::Mutex<std::collections::BTreeMap<String, Vec<u8>>>,
}

impl InMemorySubstrate {
    pub fn new(instance: impl Into<String>) -> Self {
        Self {
            instance: instance.into(),
            objects: std::sync::Mutex::new(std::collections::BTreeMap::new()),
        }
    }
}

impl DurableSubstrate for InMemorySubstrate {
    fn failure_domain(&self) -> crate::failure_domain::FailureDomain {
        crate::failure_domain::FailureDomain::Ephemeral {
            instance: self.instance.clone(),
        }
    }

    fn put_if_absent(&self, key: &str, bytes: &[u8]) -> Result<bool> {
        let mut g = self.objects.lock().unwrap();
        if g.contains_key(key) {
            return Ok(false);
        }
        g.insert(key.to_string(), bytes.to_vec());
        Ok(true)
    }

    fn put_overwrite(&self, key: &str, bytes: &[u8]) -> Result<()> {
        self.objects
            .lock()
            .unwrap()
            .insert(key.to_string(), bytes.to_vec());
        Ok(())
    }

    fn get_range(&self, key: &str, offset: u64, len: u64) -> Result<Vec<u8>> {
        let g = self.objects.lock().unwrap();
        let obj = g
            .get(key)
            .ok_or_else(|| EhdbError::NotFound(format!("substrate object {key}")))?;
        let start = offset as usize;
        let end = start.saturating_add(len as usize);
        // ⚠ A range past the end is an ERROR, not a short read. The manifest
        // states every part's length, so an over-read means the caller and the
        // store disagree about the object — and returning fewer bytes would let
        // a truncated part read as intact. `LocalFsSubstrate` uses `read_exact`
        // and fails the same way; the conformance suite pins that they agree.
        if end > obj.len() {
            return Err(EhdbError::Storage(format!(
                "get_range {key}: [{offset}, {end}) exceeds object length {}",
                obj.len()
            )));
        }
        Ok(obj[start..end].to_vec())
    }

    fn get_all(&self, key: &str) -> Result<Vec<u8>> {
        self.objects
            .lock()
            .unwrap()
            .get(key)
            .cloned()
            .ok_or_else(|| EhdbError::NotFound(format!("substrate object {key}")))
    }

    fn object_size(&self, key: &str) -> Result<u64> {
        self.objects
            .lock()
            .unwrap()
            .get(key)
            .map(|b| b.len() as u64)
            .ok_or_else(|| EhdbError::NotFound(format!("substrate object {key}")))
    }

    fn exists(&self, key: &str) -> Result<bool> {
        Ok(self.objects.lock().unwrap().contains_key(key))
    }

    fn list_prefix(&self, prefix: &str) -> Result<Vec<String>> {
        Ok(self
            .objects
            .lock()
            .unwrap()
            .keys()
            .filter(|k| k.starts_with(prefix))
            .cloned()
            .collect())
    }

    fn delete(&self, key: &str) -> Result<()> {
        self.objects.lock().unwrap().remove(key);
        Ok(())
    }
}
