//! The pluggable **object-store** abstraction L0 codes against (RFC §2.3 / §6.2).
//!
//! L0's durability tier is a *replicated object store*. The prod backend choice
//! (real S3 vs GCS vs in-cluster MinIO/Ceph) is deferred (RFC §6.2) and does
//! **not** block L0 — every L0 path targets the [`L0ObjectStore`] trait. This
//! module ships:
//!
//! - [`LocalFsObjectStore`] — a local-filesystem backend for kind/dev. Enough to
//!   prove the whole hot-local/durable-async composite; a MinIO/S3 process on
//!   `localhost` is API-compatible with this trait (same `put`/`get_range`/
//!   `list`), so the prod adapter is a drop-in later slice, not a rewrite.
//! - [`CountingObjectStore`] — a transparent wrapper that records per-key I/O
//!   (calls + bytes, and the exact keys touched) and can inject a put latency.
//!   This is the instrument the L0.1 proofs read: "a targeted lookup ranged-GETs
//!   only the needed block and touches zero non-matching parts", and "a slow
//!   object store never blocks the append hot path".
//!
//! Unlike `ehdb-storage`'s content-addressed [`ImmutableObjectStore`] (whole-
//! object put/get, the §2.4 *blob* shape), L0's part store needs **ranged GET**
//! (fetch only the granule a lookup resolves to) — hence a distinct trait.

use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use ehdb_core::{EhdbError, Result};

/// The pluggable object-store backend L0 uploads sealed parts to and range-reads
/// them back from. Object keys are `/`-separated logical paths (e.g.
/// `parts/d1_event_log/shard-0/part-...eslog`).
///
/// `Send + Sync` so the engine can share one behind an [`Arc`] between the
/// append thread and the background uploader thread.
pub trait L0ObjectStore: Send + Sync {
    /// Write an **immutable** object. Returns `Ok(true)` if newly written,
    /// `Ok(false)` if an object already existed at `key` (idempotent re-upload —
    /// parts are content-stable, so a duplicate upload is a no-op, not an error).
    fn put_if_absent(&self, key: &str, bytes: &[u8]) -> Result<bool>;

    /// Overwrite a **mutable pointer** object (the manifest `LATEST` pointer and
    /// versioned manifest snapshots). Distinct from [`Self::put_if_absent`]
    /// because a pointer legitimately advances; parts never do.
    fn put_overwrite(&self, key: &str, bytes: &[u8]) -> Result<()>;

    /// Fetch a byte range `[offset, offset+len)` of an object — the core L0
    /// primitive that lets a lookup read *only* the resolved granule, not the
    /// whole part.
    fn get_range(&self, key: &str, offset: u64, len: u64) -> Result<Vec<u8>>;

    /// Fetch a whole object (used for small manifests and for full-part replay on
    /// cold-load).
    fn get_all(&self, key: &str) -> Result<Vec<u8>>;

    /// Whether an object exists at `key`.
    fn exists(&self, key: &str) -> Result<bool>;

    /// List every object key under a `/`-terminated (or prefix-matched) logical
    /// prefix. Order is unspecified; callers sort.
    fn list_prefix(&self, prefix: &str) -> Result<Vec<String>>;
}

// ---------------------------------------------------------------------------
// Local filesystem backend
// ---------------------------------------------------------------------------

/// A local-filesystem [`L0ObjectStore`] for kind/dev. Object keys map to files
/// under `root`; `put` is atomic (write-temp + rename) so a reader never sees a
/// half-written object. A MinIO/S3 process is API-compatible with this trait, so
/// swapping it in later is a config change, not a code change.
#[derive(Debug, Clone)]
pub struct LocalFsObjectStore {
    root: PathBuf,
}

impl LocalFsObjectStore {
    /// Open (creating the root dir) a local-filesystem object store.
    pub fn new(root: impl Into<PathBuf>) -> Result<Self> {
        let root = root.into();
        fs::create_dir_all(&root).map_err(|err| EhdbError::Storage(err.to_string()))?;
        Ok(Self { root })
    }

    fn resolve(&self, key: &str) -> Result<PathBuf> {
        // Keys are internal, compiled-path-derived (never user input), but guard
        // traversal defensively — the invariant is "no arbitrary surface".
        if key.is_empty() || key.contains("..") || key.starts_with('/') {
            return Err(EhdbError::Storage(format!("unsafe object key: {key:?}")));
        }
        Ok(self.root.join(key))
    }

    fn write_atomic(&self, key: &str, bytes: &[u8]) -> Result<()> {
        let target = self.resolve(key)?;
        if let Some(parent) = target.parent() {
            fs::create_dir_all(parent).map_err(|err| EhdbError::Storage(err.to_string()))?;
        }
        let tmp = target.with_extension("tmp");
        {
            let mut f = OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .open(&tmp)
                .map_err(|err| EhdbError::Storage(err.to_string()))?;
            f.write_all(bytes)
                .map_err(|err| EhdbError::Storage(err.to_string()))?;
            f.sync_data()
                .map_err(|err| EhdbError::Storage(err.to_string()))?;
        }
        fs::rename(&tmp, &target).map_err(|err| EhdbError::Storage(err.to_string()))?;
        Ok(())
    }
}

impl L0ObjectStore for LocalFsObjectStore {
    fn put_if_absent(&self, key: &str, bytes: &[u8]) -> Result<bool> {
        let target = self.resolve(key)?;
        if target.exists() {
            return Ok(false);
        }
        self.write_atomic(key, bytes)?;
        Ok(true)
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

    fn exists(&self, key: &str) -> Result<bool> {
        Ok(self.resolve(key)?.exists())
    }

    fn list_prefix(&self, prefix: &str) -> Result<Vec<String>> {
        let mut out = Vec::new();
        walk_keys(&self.root, &self.root, prefix, &mut out)?;
        Ok(out)
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

/// Per-store I/O counters recorded by [`CountingObjectStore`]. Secret-free
/// (counts + key names only, never payload bytes) — the L0.1 instrumentation
/// exit criterion.
#[derive(Debug, Default)]
pub struct ObjectStoreCounters {
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
}

/// A transparent [`L0ObjectStore`] wrapper that records per-key I/O and can
/// inject a fixed latency on `put_if_absent` (to prove the append hot path never
/// blocks on a slow object store). It forwards to any inner store.
pub struct CountingObjectStore<S: L0ObjectStore> {
    inner: S,
    counters: Arc<ObjectStoreCounters>,
    /// Keys touched by a *read* (`get_range` / `get_all`), in call order — lets a
    /// proof assert exactly which parts a lookup fetched (and that pruned parts
    /// were fetched zero times).
    read_keys: Arc<Mutex<Vec<String>>>,
    /// Optional artificial latency applied to `put_if_absent` (simulate a slow
    /// remote object store). The append path never calls put, so this latency
    /// lands only on the background uploader thread.
    put_latency: Option<Duration>,
}

impl<S: L0ObjectStore> CountingObjectStore<S> {
    /// Wrap `inner`, recording I/O with no injected latency.
    pub fn new(inner: S) -> Self {
        Self {
            inner,
            counters: Arc::new(ObjectStoreCounters::default()),
            read_keys: Arc::new(Mutex::new(Vec::new())),
            put_latency: None,
        }
    }

    /// Wrap `inner`, recording I/O and sleeping `latency` on every upload — the
    /// slow-object-store harness for the hot-path-isolation proof.
    pub fn with_put_latency(inner: S, latency: Duration) -> Self {
        Self {
            inner,
            counters: Arc::new(ObjectStoreCounters::default()),
            read_keys: Arc::new(Mutex::new(Vec::new())),
            put_latency: Some(latency),
        }
    }

    /// Shared handle to the counters (clone before moving the store into an
    /// [`Arc`]).
    pub fn counters(&self) -> Arc<ObjectStoreCounters> {
        Arc::clone(&self.counters)
    }

    /// Shared handle to the ordered list of read-touched keys.
    pub fn read_keys(&self) -> Arc<Mutex<Vec<String>>> {
        Arc::clone(&self.read_keys)
    }
}

impl<S: L0ObjectStore> L0ObjectStore for CountingObjectStore<S> {
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

    fn exists(&self, key: &str) -> Result<bool> {
        self.inner.exists(key)
    }

    fn list_prefix(&self, prefix: &str) -> Result<Vec<String>> {
        self.inner.list_prefix(prefix)
    }
}

/// Blanket impl so an `Arc<dyn L0ObjectStore>` (and `Arc<S>`) is itself an
/// [`L0ObjectStore`] — the engine holds the store as a trait object shared
/// across threads.
impl L0ObjectStore for Arc<dyn L0ObjectStore> {
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
    fn exists(&self, key: &str) -> Result<bool> {
        (**self).exists(key)
    }
    fn list_prefix(&self, prefix: &str) -> Result<Vec<String>> {
        (**self).list_prefix(prefix)
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
        let store = LocalFsObjectStore::new(&dir).unwrap();
        let data = b"0123456789abcdef".to_vec();
        assert!(store.put_if_absent("parts/a.eslog", &data).unwrap());
        // Idempotent re-upload is a no-op.
        assert!(!store.put_if_absent("parts/a.eslog", &data).unwrap());
        let mid = store.get_range("parts/a.eslog", 4, 6).unwrap();
        assert_eq!(mid, b"456789");
        assert_eq!(store.get_all("parts/a.eslog").unwrap(), data);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn list_prefix_finds_nested_keys() {
        let dir = tmp();
        let store = LocalFsObjectStore::new(&dir).unwrap();
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
        let inner = LocalFsObjectStore::new(&dir).unwrap();
        let counting = CountingObjectStore::new(inner);
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
