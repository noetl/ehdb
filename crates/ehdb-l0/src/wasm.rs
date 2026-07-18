//! **D9 — system-WASM store** (orchestrate-core + drive modules: the compiled
//! WASM the worker runs), RFC §0.1.
//!
//! A versioned, channel-bound module store with **publish**, **bind**,
//! **resolve**, and **list** — over immutable parts. A module is addressed by a
//! `(path, channel, env)` triple (e.g. `orchestrate-core / stable / prod`).
//!
//! Like [D5](crate::blob), the WASM bytes are large and opaque, so they are
//! stored **whole and content-addressed** on the durable substrate
//! (`objects/d9_wasm/<digest>`, written once, deduped, replicated N-way); L0
//! holds a **publish/bind op-log** (`WasmOp`) as a normal dataset on the generic
//! engine. The **active** version of a triple is the version named by its latest
//! non-tombstone op (publish activates the new version; bind re-points to an
//! already-published one — rollback / pin). `resolve` = active version → its
//! publish digest → whole read of the content-addressed bytes with replica
//! fallback.
//!
//! Fixed shape: **sort key** = `op_seq`; **partition** + **index dim** = the
//! encoded triple `key` (so a resolve reads exactly one triple's history).

use std::collections::BTreeMap;
use std::sync::Arc;

use ehdb_core::{EhdbError, Result};
use serde::{Deserialize, Serialize};

use crate::blob::content_digest;
use crate::dataset::{shard_for_execution, Dataset};
use crate::engine::{L0Config, L0Engine, ReplicaTarget};
use crate::substrate::DurableSubstrate;

/// The D9 dataset id (the publish/bind op-log).
pub const DATASET_D9_WASM: &str = "d9_wasm";

/// Unit-separator joining the `(path, channel, env)` triple into the index key.
/// Chosen because it does not appear in module paths / channel / env names.
const SEP: char = '\u{1f}';

/// Encode a module triple into its index key.
pub fn wasm_key(path: &str, channel: &str, env: &str) -> String {
    format!("{path}{SEP}{channel}{SEP}{env}")
}

/// What a WASM op does.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WasmOpKind {
    /// Publish new bytes at the next version; the triple's active version
    /// becomes this one.
    Publish,
    /// Re-point the triple's active version at an already-published version
    /// (rollback / pin).
    Bind,
    /// Tombstone the triple (resolve returns `None`).
    Unpublish,
}

/// One publish/bind/unpublish in the op log (the D9 record schema).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WasmOp {
    /// Op-log position (the fixed sort key).
    pub op_seq: u64,
    /// The encoded `(path, channel, env)` triple (partition + index dim).
    pub key: String,
    /// The module path (denormalized for readable output).
    pub path: String,
    /// The release channel.
    pub channel: String,
    /// The deploy environment.
    pub env: String,
    /// What this op does.
    pub kind: WasmOpKind,
    /// The version this op activates (publish: the new version; bind: the target
    /// version; unpublish: the last-active version, informational).
    pub version: u64,
    /// The content digest of the published bytes (set on `Publish`, else empty).
    pub digest: String,
    /// The module size in bytes (set on `Publish`, else 0).
    pub size: u64,
}

/// **D9 system-WASM dataset.** Sort key `op_seq`; partition + index dim = the
/// encoded triple `key`.
#[derive(Debug, Clone, Copy)]
pub struct WasmDataset;

impl Dataset for WasmDataset {
    type Record = WasmOp;
    const NAME: &'static str = DATASET_D9_WASM;

    fn sort_key(r: &WasmOp) -> u64 {
        r.op_seq
    }
    fn partition(r: &WasmOp, shard_count: u32) -> u32 {
        shard_for_execution(&r.key, shard_count)
    }
    fn index_key(r: &WasmOp) -> &str {
        &r.key
    }
    fn read_partition(key: &str, shard_count: u32) -> u32 {
        shard_for_execution(key, shard_count)
    }
}

/// A resolved module (the active binding + its bytes).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WasmModule {
    pub path: String,
    pub channel: String,
    pub env: String,
    pub version: u64,
    pub digest: String,
    pub bytes: Vec<u8>,
}

/// A live triple's active binding (no bytes — the `list` projection).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WasmBinding {
    pub path: String,
    pub channel: String,
    pub env: String,
    pub version: u64,
    pub digest: String,
}

fn object_key(digest: &str) -> String {
    format!("objects/{DATASET_D9_WASM}/{digest}")
}

/// **The D9 system-WASM store** — content-addressed module bytes on the
/// substrate + a publish/bind op-log on the generic engine.
pub struct WasmStore {
    engine: L0Engine<WasmDataset>,
    /// Where the content-addressed WASM bytes live (same replicas as the log).
    blob_replicas: Vec<ReplicaTarget>,
}

impl WasmStore {
    /// Config default for D9 rooted at `local_root`.
    pub fn config(local_root: impl Into<std::path::PathBuf>) -> L0Config {
        L0Config::for_dataset(DATASET_D9_WASM, local_root)
    }

    /// Open a single-replica WASM store.
    pub fn open(config: L0Config, substrate: Arc<dyn DurableSubstrate>) -> Result<Self> {
        Self::open_replicated(config, vec![ReplicaTarget::new("replica-0", substrate)])
    }
    /// Open an N-way replicated WASM store (bytes + log both N-way).
    pub fn open_replicated(config: L0Config, replicas: Vec<ReplicaTarget>) -> Result<Self> {
        Ok(Self {
            blob_replicas: replicas.clone(),
            engine: L0Engine::open_replicated(config, replicas)?,
        })
    }
    /// Cold-load a single-replica WASM store.
    pub fn cold_load(config: L0Config, substrate: Arc<dyn DurableSubstrate>) -> Result<Self> {
        Self::cold_load_replicated(config, vec![ReplicaTarget::new("replica-0", substrate)])
    }
    /// Cold-load an N-way replicated WASM store.
    pub fn cold_load_replicated(config: L0Config, replicas: Vec<ReplicaTarget>) -> Result<Self> {
        Ok(Self {
            blob_replicas: replicas.clone(),
            engine: L0Engine::cold_load_replicated(config, replicas)?,
        })
    }

    /// The full op history of a triple, oldest first (index-pruned read).
    fn history(&self, key: &str) -> Result<Vec<WasmOp>> {
        self.engine.read_index_after(key, 0) // sorted by op_seq
    }

    /// **publish** — store `bytes` content-addressed and record a new version of
    /// `(path, channel, env)`, which becomes the active binding. Returns the
    /// assigned version.
    pub fn publish(&mut self, path: &str, channel: &str, env: &str, bytes: &[u8]) -> Result<u64> {
        let key = wasm_key(path, channel, env);
        let digest = content_digest(bytes);
        let object = object_key(&digest);
        let mut wrote_any = false;
        for target in &self.blob_replicas {
            if target.substrate.put_if_absent(&object, bytes).is_ok() {
                wrote_any = true;
            }
        }
        if !wrote_any {
            return Err(EhdbError::Storage(
                "wasm publish: failed to write bytes to any replica".into(),
            ));
        }
        let next = self
            .history(&key)?
            .into_iter()
            .map(|op| op.version)
            .max()
            .unwrap_or(0)
            + 1;
        self.append(
            &key,
            path,
            channel,
            env,
            WasmOpKind::Publish,
            next,
            &digest,
            bytes.len() as u64,
        )?;
        Ok(next)
    }

    /// **bind** — re-point the active binding of `(path, channel, env)` at an
    /// already-published `version` (rollback / pin). Returns `false` if that
    /// version was never published for this triple.
    pub fn bind(&mut self, path: &str, channel: &str, env: &str, version: u64) -> Result<bool> {
        let key = wasm_key(path, channel, env);
        let published = self
            .history(&key)?
            .into_iter()
            .any(|op| op.kind == WasmOpKind::Publish && op.version == version);
        if !published {
            return Ok(false);
        }
        self.append(&key, path, channel, env, WasmOpKind::Bind, version, "", 0)?;
        Ok(true)
    }

    /// **unpublish** — tombstone `(path, channel, env)` (resolve returns `None`;
    /// the content-addressed bytes are left for GC). Returns `false` if already
    /// absent.
    pub fn unpublish(&mut self, path: &str, channel: &str, env: &str) -> Result<bool> {
        let key = wasm_key(path, channel, env);
        match self.active_op(&key)? {
            Some(op) => {
                self.append(
                    &key,
                    path,
                    channel,
                    env,
                    WasmOpKind::Unpublish,
                    op.version,
                    "",
                    0,
                )?;
                Ok(true)
            }
            None => Ok(false),
        }
    }

    /// The latest non-tombstone op for a triple (the active binding), or `None`
    /// if never published or currently unpublished.
    fn active_op(&self, key: &str) -> Result<Option<WasmOp>> {
        Ok(self
            .history(key)?
            .into_iter()
            .next_back()
            .filter(|op| op.kind != WasmOpKind::Unpublish))
    }

    /// **resolve** — the active module bytes for `(path, channel, env)`, or
    /// `None` if never published / unpublished. Resolves the active version → its
    /// publish digest → reads the content-addressed bytes with replica fallback.
    pub fn resolve(&self, path: &str, channel: &str, env: &str) -> Result<Option<WasmModule>> {
        let key = wasm_key(path, channel, env);
        let Some(active) = self.active_op(&key)? else {
            return Ok(None);
        };
        // A bind op carries no digest — find the publish op that put this version.
        let digest = self
            .history(&key)?
            .into_iter()
            .find(|op| op.kind == WasmOpKind::Publish && op.version == active.version)
            .map(|op| op.digest)
            .ok_or_else(|| {
                EhdbError::Storage(format!(
                    "wasm resolve: bound version {} of {path}/{channel}/{env} was never published",
                    active.version
                ))
            })?;
        let object = object_key(&digest);
        for target in &self.blob_replicas {
            if let Ok(bytes) = target.substrate.get_all(&object) {
                return Ok(Some(WasmModule {
                    path: path.to_string(),
                    channel: channel.to_string(),
                    env: env.to_string(),
                    version: active.version,
                    digest,
                    bytes,
                }));
            }
        }
        Err(EhdbError::Storage(format!(
            "wasm resolve: object {digest} unreachable on all replicas"
        )))
    }

    /// **list** — the live triples with their active binding (no bytes), in key
    /// order. `env` filters to one environment when set.
    pub fn list(&self, env: Option<&str>) -> Result<Vec<WasmBinding>> {
        let all = self.engine.replay_all()?; // sorted by op_seq
        let mut latest: BTreeMap<String, WasmOp> = BTreeMap::new();
        for op in all {
            latest.insert(op.key.clone(), op); // last op_seq wins
        }
        // The active op names the version; the digest may live on an earlier
        // publish op (if the active op is a bind), so resolve it per triple.
        let mut out = Vec::new();
        for (key, active) in latest {
            if active.kind == WasmOpKind::Unpublish {
                continue;
            }
            if let Some(e) = env {
                if active.env != e {
                    continue;
                }
            }
            let digest = if active.kind == WasmOpKind::Publish {
                active.digest.clone()
            } else {
                self.history(&key)?
                    .into_iter()
                    .find(|op| op.kind == WasmOpKind::Publish && op.version == active.version)
                    .map(|op| op.digest)
                    .unwrap_or_default()
            };
            out.push(WasmBinding {
                path: active.path,
                channel: active.channel,
                env: active.env,
                version: active.version,
                digest,
            });
        }
        Ok(out)
    }

    #[allow(clippy::too_many_arguments)]
    fn append(
        &mut self,
        key: &str,
        path: &str,
        channel: &str,
        env: &str,
        kind: WasmOpKind,
        version: u64,
        digest: &str,
        size: u64,
    ) -> Result<()> {
        let op_seq = self.engine.global_sequence() + 1;
        self.engine.append_record(WasmOp {
            op_seq,
            key: key.to_string(),
            path: path.to_string(),
            channel: channel.to_string(),
            env: env.to_string(),
            kind,
            version,
            digest: digest.to_string(),
            size,
        })?;
        Ok(())
    }

    /// Flush all sealed op-log parts to the durable replicas.
    pub fn flush_and_wait(&mut self) -> Result<()> {
        self.engine.flush_and_wait_uploads()
    }
    /// Run background merge/compaction over the op-log.
    pub fn run_pending_merges(&mut self) -> Result<usize> {
        self.engine.run_pending_merges()
    }
    /// The underlying generic engine.
    pub fn engine(&self) -> &L0Engine<WasmDataset> {
        &self.engine
    }
}
