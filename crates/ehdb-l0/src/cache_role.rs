//! **The Cache role** and two conforming backends (RFC §12).
//!
//! The Cache contract is deliberately the **weakest** of the roles, and that is
//! what makes Cloudflare KV a genuinely good fit rather than a compromise:
//!
//! * **no ordering**;
//! * **no read-your-writes** — a `get` after a `put` may legitimately miss;
//! * **a miss is never a fault** — losing an entry is a cache miss, and every
//!   caller must already have a path that recomputes.
//!
//! ⭐ Those are Cloudflare KV's actual semantics (eventually consistent,
//! sub-20ms *cached* reads), so a KV backend satisfies this contract **without
//! the contract being weakened to admit it**. That direction matters: a
//! contract bent to fit a candidate proves nothing about the candidate.
//!
//! ⚠ From GKE, KV is reached over the **REST API** — the same path the noetl.ai
//! waitlist already uses — so every operation is an internet round trip. That is
//! acceptable here precisely because a cache is off the per-event hot path, and
//! [`AccessMode`](crate::store_role::AccessMode) is what stops the same backend
//! being selected for the EventLog role.

use std::collections::BTreeMap;

/// The Cache contract. Any backend satisfying it may serve
/// [`StorageRole::Cache`](crate::store_role::StorageRole::Cache).
pub trait CacheStore: Send {
    fn backend_name(&self) -> &'static str;
    /// Store a value. MAY be lost, MAY not be immediately visible.
    fn put(&mut self, key: &str, value: &str);
    /// Fetch. `None` is always a legal answer — a miss is not a fault.
    fn get(&self, key: &str) -> Option<String>;
    fn delete(&mut self, key: &str);
    /// Whether this backend guarantees read-your-writes. Cache callers MUST NOT
    /// require it; this exists so a caller that *benefits* from it can tell.
    fn read_your_writes(&self) -> bool;
}

/// EHDB's in-region cache — the default. Strongly consistent locally, which is
/// **stronger than the contract requires** and therefore still conforming.
#[derive(Debug, Default)]
pub struct EhdbCache {
    map: BTreeMap<String, String>,
}

impl CacheStore for EhdbCache {
    fn backend_name(&self) -> &'static str {
        "ehdb"
    }
    fn put(&mut self, key: &str, value: &str) {
        self.map.insert(key.to_string(), value.to_string());
    }
    fn get(&self, key: &str) -> Option<String> {
        self.map.get(key).cloned()
    }
    fn delete(&mut self, key: &str) {
        self.map.remove(key);
    }
    fn read_your_writes(&self) -> bool {
        true
    }
}

/// **Cloudflare KV over the REST API — sketch.**
///
/// Models KV's semantics rather than its transport: no networking here. The one
/// behaviour worth modelling faithfully is **eventual consistency** — a write
/// is not immediately visible at the edge that serves the next read. This sketch
/// makes the first read after a write miss, then converge.
///
/// ⚠ Not production code: no account id, no namespace binding, no auth, no
/// retry/backoff, no TTL. Those are what a real implementation adds; none of
/// them changes the conformance verdict, which is about the *semantics*.
#[derive(Debug, Default)]
pub struct CloudflareKvCacheSketch {
    /// Authoritative store.
    map: BTreeMap<String, String>,
    /// Keys written but not yet "propagated" — the eventual-consistency window.
    pending: BTreeMap<String, u32>,
    /// How many reads a key stays invisible for.
    propagation_reads: u32,
}

impl CloudflareKvCacheSketch {
    /// `propagation_reads = 0` behaves like a strongly-consistent cache; `1`
    /// models "the first read after a write may miss".
    pub fn new(propagation_reads: u32) -> Self {
        Self {
            map: BTreeMap::new(),
            pending: BTreeMap::new(),
            propagation_reads,
        }
    }
    /// Force convergence — what waiting would do.
    pub fn converge(&mut self) {
        self.pending.clear();
    }
}

impl CacheStore for CloudflareKvCacheSketch {
    fn backend_name(&self) -> &'static str {
        "cloudflare-kv-sketch"
    }
    fn put(&mut self, key: &str, value: &str) {
        self.map.insert(key.to_string(), value.to_string());
        if self.propagation_reads > 0 {
            self.pending.insert(key.to_string(), self.propagation_reads);
        }
    }
    fn get(&self, key: &str) -> Option<String> {
        if self.pending.contains_key(key) {
            return None; // not propagated yet — a legal miss
        }
        self.map.get(key).cloned()
    }
    fn delete(&mut self, key: &str) {
        self.map.remove(key);
        self.pending.remove(key);
    }
    fn read_your_writes(&self) -> bool {
        self.propagation_reads == 0
    }
}

/// A cache that loses everything. ⚠ **Conforming**, and deliberately so: the
/// contract says a miss is never a fault, so a cache that always misses is
/// *useless but correct*.
///
/// ⭐ This is the honest boundary of what a Cache conformance suite can check,
/// and stating it is more useful than pretending otherwise: **correctness and
/// usefulness are different properties here**, and only the first is testable
/// from semantics. Hit rate is an operational metric, not a contract clause.
#[derive(Debug, Default)]
pub struct AlwaysMissCache;

impl CacheStore for AlwaysMissCache {
    fn backend_name(&self) -> &'static str {
        "always-miss"
    }
    fn put(&mut self, _k: &str, _v: &str) {}
    fn get(&self, _k: &str) -> Option<String> {
        None
    }
    fn delete(&mut self, _k: &str) {}
    fn read_your_writes(&self) -> bool {
        false
    }
}

/// A backend that violates the contract by **erroring instead of missing** —
/// modelled as a panic-on-get. Used to show the suite is not vacuous.
#[derive(Debug, Default)]
pub struct BrokenCache;

impl CacheStore for BrokenCache {
    fn backend_name(&self) -> &'static str {
        "broken"
    }
    fn put(&mut self, _k: &str, _v: &str) {}
    /// ⚠ Returns a value for a key that was never written — fabrication, which
    /// is the one thing a cache must never do. A miss is fine; a wrong hit is
    /// not.
    fn get(&self, _k: &str) -> Option<String> {
        Some("fabricated".to_string())
    }
    fn delete(&mut self, _k: &str) {}
    fn read_your_writes(&self) -> bool {
        true
    }
}

pub mod conformance {
    //! Cache-role conformance. Weaker than EventStore's, deliberately.

    use super::CacheStore;

    #[derive(Debug, Clone, PartialEq, Eq)]
    pub struct Violation {
        pub clause: &'static str,
        pub detail: String,
    }

    /// Run the Cache contract. Note what is **not** asserted: read-your-writes,
    /// ordering, and durability. A backend that lacks them still conforms.
    pub fn run(store: &mut dyn CacheStore) -> Vec<Violation> {
        let mut v = Vec::new();

        // ⭐ The only hard clause: never fabricate. A miss is legal; a wrong
        // hit is a correctness bug that silently poisons every caller.
        if let Some(found) = store.get("cache-conformance-never-written") {
            v.push(Violation {
                clause: "get/never-fabricates",
                detail: format!(
                    "a key that was never written returned {found:?} — a miss is always \
                     legal, a fabricated hit never is"
                ),
            });
        }

        // A put must not make an UNRELATED key appear.
        store.put("cache-conformance-k1", "v1");
        if store.get("cache-conformance-k2").is_some() {
            v.push(Violation {
                clause: "put/does-not-affect-other-keys",
                detail: "writing k1 made k2 readable".into(),
            });
        }

        // If the backend claims read-your-writes, it must actually have it.
        if store.read_your_writes() {
            match store.get("cache-conformance-k1") {
                Some(ref got) if got == "v1" => {}
                other => v.push(Violation {
                    clause: "read_your_writes/is-truthful",
                    detail: format!(
                        "backend advertises read-your-writes but a read returned {other:?}"
                    ),
                }),
            }
        }

        // Delete must not resurrect or fabricate.
        store.delete("cache-conformance-k1");
        if store.read_your_writes() && store.get("cache-conformance-k1").is_some() {
            v.push(Violation {
                clause: "delete/removes",
                detail: "a deleted key is still readable on a read-your-writes backend".into(),
            });
        }

        v
    }

    pub fn passes(store: &mut dyn CacheStore) -> bool {
        run(store).is_empty()
    }
}
