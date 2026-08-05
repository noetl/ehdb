//! **The networked KV face — the NATS-KV replacement (noetl/ai-meta#214, #215).**
//!
//! [`KvStore`](ehdb_l0::KvStore) is an *in-process* D4 store: `put` takes
//! `&mut self`, so it lives inside whichever process owns the engine. The
//! gateway is a different process, and the two buckets it used
//! (`sessions`, `requests`) were NATS KV precisely because they needed a
//! networked store.
//!
//! This is that face, in the same shape as the feed's other faces
//! ([`serve_ingest`](crate::publish::serve_ingest),
//! [`serve_claims`](crate::claim::serve_claims)): length-prefixed JSON frames
//! over TCP, `TCP_NODELAY`, one shared store behind an async mutex.
//!
//! ## TTL
//!
//! The NATS buckets carried `max_age = 300s` with `max_msgs_per_subject = 1`,
//! i.e. per-key expiry. The D4 store has no notion of time — it is a
//! clock-free fold, deliberately (see [`ehdb_l0::kv`]).
//!
//! So TTL is implemented **here**, at the boundary, by storing an expiry
//! alongside the value and filtering on read:
//!
//! - [`put`](KvClient::put) stamps `expires_at_ms` (0 = never).
//! - [`get`](KvClient::get) returns `None` for an entry past its expiry, so an
//!   expired key is invisible the instant it lapses, without waiting for a sweep.
//! - A background sweep ([`KvCoordinator::sweep_expired`]) deletes lapsed keys so
//!   the log does not grow without bound.
//!
//! Read-side filtering is the load-bearing half: a sweep alone would leave a
//! window where an expired session still validates, which is exactly the bug a
//! session cache must not have.
//!
//! ## Bucket namespacing
//!
//! One store serves many logical buckets by prefixing keys `<bucket>/<key>`,
//! mirroring NATS's `$KV.<bucket>.>` subject space. [`prefix_scan`] over
//! `<bucket>/` is how the gateway enumerates a bucket, which is what its
//! `get_by_execution` routing lookup needs.

use std::io;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use ehdb_l0::KvStore;
use serde::{Deserialize, Serialize};
use tokio::net::{TcpListener, TcpStream, ToSocketAddrs};
use tokio::sync::Mutex;

use crate::{io_err, read_frame, write_frame};

/// Wall-clock milliseconds. The one place this crate consults a clock — the D4
/// store below it stays clock-free.
fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// The stored envelope: the caller's value plus its expiry.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct Envelope {
    v: String,
    /// Unix ms after which this entry is invisible. `0` = never expires.
    #[serde(default)]
    exp: u64,
}

impl Envelope {
    fn expired(&self, now: u64) -> bool {
        self.exp != 0 && now >= self.exp
    }
}

/// Compose the physical key for `(bucket, key)`.
fn physical(bucket: &str, key: &str) -> String {
    format!("{bucket}/{key}")
}

/// The shared KV store behind an async mutex — one per writer process.
pub struct KvCoordinator {
    store: Mutex<KvStore>,
}

impl KvCoordinator {
    pub fn new(store: KvStore) -> Self {
        Self {
            store: Mutex::new(store),
        }
    }

    /// Get `key` from `bucket`, honouring TTL. An expired entry reads as absent
    /// **immediately**, without waiting for the sweep.
    pub async fn get(&self, bucket: &str, key: &str) -> io::Result<Option<String>> {
        let store = self.store.lock().await;
        let entry = store.get(&physical(bucket, key)).map_err(io_err)?;
        let Some(entry) = entry else { return Ok(None) };
        let env: Envelope = serde_json::from_str(&entry.value).map_err(io_err)?;
        if env.expired(now_ms()) {
            return Ok(None);
        }
        Ok(Some(env.v))
    }

    /// Put `key` in `bucket` with an optional TTL (`0` = no expiry).
    pub async fn put(
        &self,
        bucket: &str,
        key: &str,
        value: String,
        ttl_ms: u64,
    ) -> io::Result<u64> {
        let exp = if ttl_ms == 0 { 0 } else { now_ms() + ttl_ms };
        let body = serde_json::to_string(&Envelope { v: value, exp }).map_err(io_err)?;
        let mut store = self.store.lock().await;
        store.put(&physical(bucket, key), body).map_err(io_err)
    }

    /// Delete `key` from `bucket`.
    pub async fn delete(&self, bucket: &str, key: &str) -> io::Result<bool> {
        let mut store = self.store.lock().await;
        Ok(store
            .delete(&physical(bucket, key))
            .map_err(io_err)?
            .is_some())
    }

    /// Every live `(key, value)` in `bucket`, expired entries filtered out.
    pub async fn scan(&self, bucket: &str) -> io::Result<Vec<(String, String)>> {
        let prefix = format!("{bucket}/");
        let store = self.store.lock().await;
        let raw = store.prefix_scan(&prefix).map_err(io_err)?;
        let now = now_ms();
        let mut out = Vec::with_capacity(raw.len());
        for (k, v) in raw {
            let Ok(env) = serde_json::from_str::<Envelope>(&v) else {
                continue;
            };
            if env.expired(now) {
                continue;
            }
            out.push((k.trim_start_matches(&prefix).to_string(), env.v));
        }
        Ok(out)
    }

    /// Delete every expired entry in `bucket`. Returns how many were removed.
    ///
    /// Purely a space reclaim — correctness already comes from the read-side
    /// filter, so a sweep that never runs makes the log grow but can never
    /// resurrect an expired key.
    pub async fn sweep_expired(&self, bucket: &str) -> io::Result<usize> {
        let prefix = format!("{bucket}/");
        let now = now_ms();
        let doomed: Vec<String> = {
            let store = self.store.lock().await;
            store
                .prefix_scan(&prefix)
                .map_err(io_err)?
                .into_iter()
                .filter(|(_, v)| {
                    serde_json::from_str::<Envelope>(v)
                        .map(|e| e.expired(now))
                        .unwrap_or(false)
                })
                .map(|(k, _)| k)
                .collect()
        };
        let mut store = self.store.lock().await;
        let mut n = 0;
        for k in doomed {
            if store.delete(&k).map_err(io_err)?.is_some() {
                n += 1;
            }
        }
        Ok(n)
    }

    /// Seal the KV store's active parts and wait for their uploads —
    /// the shutdown seam (noetl/ai-meta#209).
    ///
    /// Without this the coordinator kept its `KvStore` private with no way to
    /// flush it, so a host could seal its feed writers on SIGTERM and still
    /// leave the KV face unsealed. Everything written to the active part since
    /// the last seal — sessions, request state — sat outside the durable
    /// manifest.
    ///
    /// Since the L0 active-part recovery in the same issue, an unsealed KV part
    /// is *replayed* on the next open rather than destroyed, so this is no
    /// longer the difference between "kept" and "lost". It is still the
    /// difference between "durable in the object store, recoverable by any
    /// replica" and "local-only on a volume that may not come back", which is
    /// the guarantee a graceful shutdown is supposed to provide.
    ///
    /// Terminal path only: like every seal, do not call this on a coordinator
    /// that intends to keep serving.
    pub async fn flush_and_wait(&self) -> io::Result<()> {
        self.store.lock().await.flush_and_wait().map_err(io_err)
    }

    /// Spawn a periodic sweep over `buckets`.
    pub fn spawn_sweeper(self: Arc<Self>, buckets: Vec<String>, every: Duration) {
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(every).await;
                for b in &buckets {
                    let _ = self.sweep_expired(b).await;
                }
            }
        });
    }
}

/// A KV request on the wire.
#[derive(Debug, Clone, Serialize, Deserialize)]
enum KvReq {
    Get {
        bucket: String,
        key: String,
    },
    Put {
        bucket: String,
        key: String,
        value: String,
        #[serde(default)]
        ttl_ms: u64,
    },
    Delete {
        bucket: String,
        key: String,
    },
    Scan {
        bucket: String,
    },
}

/// A KV response on the wire. `ok=false` carries `err` so a client can tell a
/// missing key (`ok=true, value=None`) from a failed call — conflating them is
/// how a store silently starts losing writes.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct KvResp {
    ok: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    value: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    version: Option<u64>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    entries: Vec<(String, String)>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    err: Option<String>,
}

impl KvResp {
    fn err(e: impl std::fmt::Display) -> Self {
        Self {
            ok: false,
            value: None,
            version: None,
            entries: Vec::new(),
            err: Some(e.to_string()),
        }
    }
}

/// Serve the KV face on `listener` from the shared `coordinator`.
pub async fn serve_kv(listener: TcpListener, coordinator: Arc<KvCoordinator>) -> io::Result<()> {
    loop {
        // noetl/ehdb#311 — per-connection failures must not drop the listener.
        // This face already spawns its handshake, so it could not be killed the
        // way the WAL fan-out face could; but an accept error and the socket
        // setup below both still `?`d out of the loop, and a peer that vanishes
        // between SYN and accept is a routine, per-connection event.
        let (mut sock, _peer) = match listener.accept().await {
            Ok(v) => v,
            Err(e) => {
                tracing::warn!(error = %e, "kv: accept failed; face stays up");
                continue;
            }
        };
        if let Err(e) = sock.set_nodelay(true) {
            tracing::warn!(error = %e, "kv: rejecting connection (socket setup)");
            continue;
        }
        let coordinator = Arc::clone(&coordinator);
        tokio::spawn(async move {
            loop {
                let body = match read_frame(&mut sock).await {
                    Ok(b) => b,
                    Err(_) => return,
                };
                let resp = match serde_json::from_slice::<KvReq>(&body) {
                    Ok(KvReq::Get { bucket, key }) => match coordinator.get(&bucket, &key).await {
                        Ok(v) => KvResp {
                            ok: true,
                            value: v,
                            version: None,
                            entries: Vec::new(),
                            err: None,
                        },
                        Err(e) => KvResp::err(e),
                    },
                    Ok(KvReq::Put {
                        bucket,
                        key,
                        value,
                        ttl_ms,
                    }) => match coordinator.put(&bucket, &key, value, ttl_ms).await {
                        Ok(ver) => KvResp {
                            ok: true,
                            value: None,
                            version: Some(ver),
                            entries: Vec::new(),
                            err: None,
                        },
                        Err(e) => KvResp::err(e),
                    },
                    Ok(KvReq::Delete { bucket, key }) => {
                        match coordinator.delete(&bucket, &key).await {
                            Ok(_) => KvResp {
                                ok: true,
                                value: None,
                                version: None,
                                entries: Vec::new(),
                                err: None,
                            },
                            Err(e) => KvResp::err(e),
                        }
                    }
                    Ok(KvReq::Scan { bucket }) => match coordinator.scan(&bucket).await {
                        Ok(entries) => KvResp {
                            ok: true,
                            value: None,
                            version: None,
                            entries,
                            err: None,
                        },
                        Err(e) => KvResp::err(e),
                    },
                    Err(e) => KvResp::err(e),
                };
                let bytes = match serde_json::to_vec(&resp) {
                    Ok(b) => b,
                    Err(_) => return,
                };
                if write_frame(&mut sock, &bytes).await.is_err() {
                    return;
                }
            }
        });
    }
}

/// A client of the networked KV face.
///
/// Lazily connected and **self-healing**: a call that fails on a dead socket
/// drops the connection so the next one redials. The gateway holds this for the
/// lifetime of the process and must survive a writer restart without a restart
/// of its own.
pub struct KvClient {
    addr: String,
    sock: Mutex<Option<TcpStream>>,
}

impl KvClient {
    /// A client for the KV face at `addr` (`host:port`; a DNS name is resolved
    /// at connect time, so a Kubernetes service name works and pod-IP changes
    /// are followed on reconnect).
    pub fn new(addr: impl Into<String>) -> Self {
        Self {
            addr: addr.into(),
            sock: Mutex::new(None),
        }
    }

    /// Eagerly connect, so a misconfigured address fails loudly at startup
    /// instead of on the first user request.
    pub async fn connect<A: ToSocketAddrs + Clone + Into<String>>(addr: A) -> io::Result<Self> {
        let client = Self::new(addr.clone().into());
        let sock = TcpStream::connect(addr).await?;
        sock.set_nodelay(true)?;
        *client.sock.lock().await = Some(sock);
        Ok(client)
    }

    async fn call(&self, req: &KvReq) -> io::Result<KvResp> {
        let body = serde_json::to_vec(req).map_err(io_err)?;
        let mut guard = self.sock.lock().await;
        if guard.is_none() {
            let sock = TcpStream::connect(self.addr.as_str()).await?;
            sock.set_nodelay(true)?;
            *guard = Some(sock);
        }
        let sock = guard.as_mut().expect("just connected");
        let result = async {
            write_frame(sock, &body).await?;
            let resp = read_frame(sock).await?;
            serde_json::from_slice::<KvResp>(&resp).map_err(io_err)
        }
        .await;
        if result.is_err() {
            // Drop the socket so the next call redials.
            *guard = None;
        }
        result
    }

    pub async fn get(&self, bucket: &str, key: &str) -> io::Result<Option<String>> {
        let r = self
            .call(&KvReq::Get {
                bucket: bucket.into(),
                key: key.into(),
            })
            .await?;
        if !r.ok {
            return Err(io::Error::other(r.err.unwrap_or_default()));
        }
        Ok(r.value)
    }

    pub async fn put(&self, bucket: &str, key: &str, value: &str, ttl_ms: u64) -> io::Result<u64> {
        let r = self
            .call(&KvReq::Put {
                bucket: bucket.into(),
                key: key.into(),
                value: value.into(),
                ttl_ms,
            })
            .await?;
        if !r.ok {
            return Err(io::Error::other(r.err.unwrap_or_default()));
        }
        Ok(r.version.unwrap_or(0))
    }

    pub async fn delete(&self, bucket: &str, key: &str) -> io::Result<()> {
        let r = self
            .call(&KvReq::Delete {
                bucket: bucket.into(),
                key: key.into(),
            })
            .await?;
        if !r.ok {
            return Err(io::Error::other(r.err.unwrap_or_default()));
        }
        Ok(())
    }

    pub async fn scan(&self, bucket: &str) -> io::Result<Vec<(String, String)>> {
        let r = self
            .call(&KvReq::Scan {
                bucket: bucket.into(),
            })
            .await?;
        if !r.ok {
            return Err(io::Error::other(r.err.unwrap_or_default()));
        }
        Ok(r.entries)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use ehdb_l0::LocalFsSubstrate;

    fn coordinator(tag: &str) -> Arc<KvCoordinator> {
        let dir = std::env::temp_dir().join(format!("ehdb-kv-{tag}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let substrate = Arc::new(LocalFsSubstrate::new(&dir).unwrap());
        let store = KvStore::open(KvStore::config(&dir), substrate).unwrap();
        Arc::new(KvCoordinator::new(store))
    }

    /// noetl/ai-meta#209 — the shutdown seam the worker could not reach.
    ///
    /// `KvCoordinator` owned its `KvStore` privately with no flush, so a host
    /// sealing its feed writers on SIGTERM still left the KV face unsealed.
    /// Sealing must be callable, must be safe with nothing pending, and must
    /// leave already-written entries readable afterwards.
    #[tokio::test]
    async fn flush_and_wait_seals_without_losing_entries() {
        let c = coordinator("seal");
        // Safe on an empty store — a host seals unconditionally on shutdown.
        c.flush_and_wait().await.unwrap();

        c.put("sessions", "a", "token-1".into(), 0).await.unwrap();
        c.put("requests", "r", "state-1".into(), 0).await.unwrap();
        c.flush_and_wait().await.unwrap();

        assert_eq!(
            c.get("sessions", "a").await.unwrap(),
            Some("token-1".into()),
            "sealing must not lose what it made durable"
        );
        assert_eq!(
            c.get("requests", "r").await.unwrap(),
            Some("state-1".into())
        );
    }

    #[tokio::test]
    async fn put_get_delete_roundtrip() {
        let c = coordinator("rt");
        assert_eq!(c.get("sessions", "a").await.unwrap(), None);
        c.put("sessions", "a", "token-1".into(), 0).await.unwrap();
        assert_eq!(
            c.get("sessions", "a").await.unwrap(),
            Some("token-1".into())
        );
        assert!(c.delete("sessions", "a").await.unwrap());
        assert_eq!(c.get("sessions", "a").await.unwrap(), None);
    }

    /// An expired entry must read as absent **immediately**, not after a sweep.
    /// A sweep-only design leaves a window where an expired session still
    /// validates — the one bug a session cache must not have.
    #[tokio::test]
    async fn expired_entries_are_invisible_before_any_sweep() {
        let c = coordinator("ttl");
        c.put("sessions", "k", "v".into(), 1).await.unwrap(); // 1 ms
        tokio::time::sleep(Duration::from_millis(25)).await;
        assert_eq!(
            c.get("sessions", "k").await.unwrap(),
            None,
            "expired key must be invisible without a sweep"
        );
        // Still physically present until swept — the read filter is what protects us.
        assert_eq!(c.sweep_expired("sessions").await.unwrap(), 1);
        assert_eq!(c.sweep_expired("sessions").await.unwrap(), 0);
    }

    /// ttl_ms = 0 means never expires — the session cache and the request store
    /// both rely on an explicit TTL, so the "no TTL" path must not silently
    /// expire immediately.
    #[tokio::test]
    async fn zero_ttl_never_expires() {
        let c = coordinator("nottl");
        c.put("requests", "k", "v".into(), 0).await.unwrap();
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert_eq!(c.get("requests", "k").await.unwrap(), Some("v".into()));
    }

    /// Buckets share one store, so a key in one must never be visible in another.
    #[tokio::test]
    async fn buckets_are_isolated() {
        let c = coordinator("iso");
        c.put("sessions", "k", "s".into(), 0).await.unwrap();
        c.put("requests", "k", "r".into(), 0).await.unwrap();
        assert_eq!(c.get("sessions", "k").await.unwrap(), Some("s".into()));
        assert_eq!(c.get("requests", "k").await.unwrap(), Some("r".into()));
        let s = c.scan("sessions").await.unwrap();
        assert_eq!(s, vec![("k".to_string(), "s".to_string())]);
    }

    /// `scan` is what the gateway's SSE routing lookup uses, so it must return
    /// bucket-relative keys and skip expired entries.
    #[tokio::test]
    async fn scan_returns_relative_keys_and_skips_expired() {
        let c = coordinator("scan");
        c.put("requests", "live", "1".into(), 0).await.unwrap();
        c.put("requests", "dead", "2".into(), 1).await.unwrap();
        tokio::time::sleep(Duration::from_millis(25)).await;
        let got = c.scan("requests").await.unwrap();
        assert_eq!(got, vec![("live".to_string(), "1".to_string())]);
    }

    /// End-to-end over the wire, including that a client survives losing its
    /// connection — the gateway must not need a restart when the writer rolls.
    #[tokio::test]
    async fn client_roundtrips_over_the_wire_and_redials() {
        let c = coordinator("wire");
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        tokio::spawn(serve_kv(listener, c.clone()));

        let client = KvClient::new(addr.clone());
        client.put("sessions", "a", "v1", 0).await.unwrap();
        assert_eq!(
            client.get("sessions", "a").await.unwrap(),
            Some("v1".into())
        );
        assert_eq!(client.get("sessions", "missing").await.unwrap(), None);

        // Force the socket shut; the next call must redial rather than fail.
        *client.sock.lock().await = None;
        assert_eq!(
            client.get("sessions", "a").await.unwrap(),
            Some("v1".into()),
            "client must redial transparently"
        );

        client.delete("sessions", "a").await.unwrap();
        assert_eq!(client.get("sessions", "a").await.unwrap(), None);
    }
}
