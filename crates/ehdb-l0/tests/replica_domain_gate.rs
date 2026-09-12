//! **The failure-domain guard, wired** (noetl/ehdb#332 F5, resilient-core Phase 3).
//!
//! ## The gap this closes
//!
//! `validate_replica_domains` / `check_replica_domains` existed and had **no
//! production caller** — they were referenced only from their own test file. So
//! a replica set sharing one disk was refused by nothing, and *an RF of N over
//! one domain is an RF of 1 wearing a larger number*. Until a call site existed,
//! the larger number was all anyone could see.
//!
//! The check now runs inside `open_replicated*`, the one place every replicated
//! open funnels through.
//!
//! ## Shadow first
//!
//! `require_distinct_domains` defaults to **false**: violations are counted, the
//! open succeeds. Same shape as `seal_max_age` and the fencing work — enabling
//! enforcement is deliberate and reversible. These tests pin **both** modes,
//! because a guard that only ever refuses is as untrustworthy as one that never
//! does.

use std::sync::Arc;

use ehdb_core::Result as EhdbResult;
use ehdb_l0::substrate::{DurableSubstrate, InMemorySubstrate, LocalFsSubstrate};
use ehdb_l0::{L0Config, L0EventLogEngine, ReplicaTarget};

fn unique_dir(tag: &str) -> std::path::PathBuf {
    static N: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let n = N.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    std::env::temp_dir().join(format!("ehdb-rdg-{tag}-{}-{n}-{nanos}", std::process::id()))
}

/// Two local dirs — different paths, **same device**. This is the prod shape:
/// `/data/eventbus` and `/data/eventbus/ehdb-tier` on one PVC.
fn same_device_targets() -> Vec<ReplicaTarget> {
    (0..2)
        .map(|i| {
            let d = unique_dir(&format!("same{i}"));
            let s: Arc<dyn DurableSubstrate> = Arc::new(LocalFsSubstrate::new(&d).unwrap());
            ReplicaTarget::new(format!("replica-{i}"), s)
        })
        .collect()
}

/// Two substrates in genuinely distinct domains.
fn spread_targets() -> Vec<ReplicaTarget> {
    (0..2)
        .map(|i| {
            let s: Arc<dyn DurableSubstrate> =
                Arc::new(InMemorySubstrate::new(format!("instance-{i}")));
            ReplicaTarget::new(format!("replica-{i}"), s)
        })
        .collect()
}

fn cfg(root: &std::path::Path) -> L0Config {
    L0Config::d1(root).with_shard_count(1).with_granule_size(4)
}

#[test]
fn enforcing_refuses_a_replica_set_that_shares_one_device() {
    // `L0Engine` is not `Debug`, so match rather than `expect_err`.
    let msg = match L0EventLogEngine::open_replicated(
        cfg(&unique_dir("enforce-local")).with_require_distinct_domains(true),
        same_device_targets(),
    ) {
        Ok(_) => panic!("two replicas on one device must be refused when enforcing"),
        Err(e) => format!("{e}"),
    };
    assert!(
        msg.contains("does not spread failure domains"),
        "the refusal must say WHY, got: {msg}"
    );
}

#[test]
fn shadow_allows_the_same_set_but_makes_it_observable() {
    // ⚠ The point of shadow: the open succeeds, so nothing breaks — but the
    // violation is a number someone can read, not a silence.
    let engine =
        L0EventLogEngine::open_replicated(cfg(&unique_dir("shadow-local")), same_device_targets())
            .expect("shadow mode must not refuse");
    let snap = engine.metrics().snapshot();
    assert!(
        snap.replica_domain_violations > 0,
        "a same-device replica set must register a violation even in shadow, \
         or shadow mode is indistinguishable from no check at all"
    );
}

#[test]
fn enforcing_accepts_a_genuinely_spread_set() {
    // The positive control. Without this, a guard that refused unconditionally
    // would satisfy every other assertion in this file while making N-way
    // replication impossible to use.
    // ⚠ Pre-poison the counter. Reading 0 from a fresh handle proves nothing:
    // an AtomicU64 nobody wrote is also 0, so "pinned to 0" and "never touched"
    // are indistinguishable. A mutation removing the pin survived this test
    // until the counter started non-zero. Absent-vs-zero, in miniature.
    let metrics = ehdb_l0::metrics::L0Metrics::new();
    metrics.set_replica_domain_violations(99);

    let engine = L0EventLogEngine::open_replicated_with_metrics(
        cfg(&unique_dir("enforce-spread")).with_require_distinct_domains(true),
        spread_targets(),
        Arc::clone(&metrics),
    )
    .expect("distinct domains must be accepted while enforcing");
    let snap = engine.metrics().snapshot();
    assert_eq!(
        snap.replica_domain_violations, 0,
        "a healthy set must PIN the counter at 0 — it started at 99, so a 0 \
         here means the open actually wrote it rather than leaving it untouched"
    );
}

/// A substrate that does NOT declare a failure domain — i.e. any implementation
/// that takes the trait default. Backed by an in-memory store so it is a real
/// substrate, not a stub that would fail the open for unrelated reasons.
struct UndeclaredSubstrate(InMemorySubstrate);

impl DurableSubstrate for UndeclaredSubstrate {
    // Deliberately no `failure_domain` — that is the whole point.
    fn put_if_absent(&self, key: &str, bytes: &[u8]) -> EhdbResult<bool> {
        self.0.put_if_absent(key, bytes)
    }
    fn put_overwrite(&self, key: &str, bytes: &[u8]) -> EhdbResult<()> {
        self.0.put_overwrite(key, bytes)
    }
    fn get_range(&self, key: &str, offset: u64, len: u64) -> EhdbResult<Vec<u8>> {
        self.0.get_range(key, offset, len)
    }
    fn get_all(&self, key: &str) -> EhdbResult<Vec<u8>> {
        self.0.get_all(key)
    }
    fn exists(&self, key: &str) -> EhdbResult<bool> {
        self.0.exists(key)
    }
    fn list_prefix(&self, prefix: &str) -> EhdbResult<Vec<String>> {
        self.0.list_prefix(prefix)
    }
    fn delete(&self, key: &str) -> EhdbResult<()> {
        self.0.delete(key)
    }
}

#[test]
fn a_single_replica_is_never_refused() {
    // RF=1 makes no spreading claim, so there is nothing to falsify.
    //
    // ⚠ The substrate here declares **no** domain. A first draft used
    // `LocalFsSubstrate`, which declares `LocalDevice` and therefore produces no
    // violation even when checked — so the test passed whether or not the
    // single-replica bypass existed, and a mutation widening the check to
    // `len() >= 1` survived it. The undeclared case is the one that breaks.
    let s: Arc<dyn DurableSubstrate> =
        Arc::new(UndeclaredSubstrate(InMemorySubstrate::new("solo")));
    L0EventLogEngine::open_replicated(
        cfg(&unique_dir("single-local")).with_require_distinct_domains(true),
        vec![ReplicaTarget::new("replica-0", s)],
    )
    .expect(
        "a single replica must open even while enforcing, even when it declares \
         no failure domain",
    );
}

#[test]
fn the_default_is_shadow_not_enforce() {
    // Pins the rollout posture itself: a future change flipping the default to
    // enforce would refuse existing single-domain deployments at open, which is
    // an outage rather than a tightening.
    assert!(
        !cfg(&unique_dir("default")).require_distinct_domains,
        "enforcement must be opt-in"
    );
}

/// The `Arc<dyn DurableSubstrate>` forwarding impl must forward **every** trait
/// method.
///
/// ⚠ Written because it did not. `failure_domain` was omitted and fell through
/// to the trait default, so every `Arc<dyn DurableSubstrate>` — which is what
/// the engine holds — reported `Undeclared` no matter what the inner substrate
/// said. A direct call returned `Ephemeral`; the same value behind the `Arc`
/// returned `Undeclared`.
///
/// It compiled **because the method has a default body**. A required method left
/// out of an impl is a compile error; a defaulted one left out is a plausible
/// wrong answer that nothing detects. So the check has to be structural: compare
/// the two method sets rather than trusting review.
#[test]
fn the_arc_forwards_every_trait_method() {
    let src = include_str!("../src/substrate.rs");

    fn block_end(lines: &[&str], start: usize) -> usize {
        let mut depth = 0i32;
        for (i, l) in lines.iter().enumerate().skip(start) {
            depth += l.matches('{').count() as i32 - l.matches('}').count() as i32;
            if i > start && depth == 0 {
                return i;
            }
        }
        lines.len() - 1
    }
    fn methods(lines: &[&str], start: usize, end: usize) -> Vec<String> {
        lines[start..=end]
            .iter()
            .filter(|l| l.starts_with("    fn "))
            .map(|l| {
                l.trim()
                    .trim_start_matches("fn ")
                    .split('(')
                    .next()
                    .unwrap_or("")
                    .to_string()
            })
            .collect()
    }

    let lines: Vec<&str> = src.lines().collect();
    let ti = lines
        .iter()
        .position(|l| l.starts_with("pub trait DurableSubstrate"))
        .expect("trait not found — extraction broke");
    let fi = lines
        .iter()
        .position(|l| l.starts_with("impl DurableSubstrate for Arc<dyn DurableSubstrate>"))
        .expect("forwarding impl not found — extraction broke");

    let trait_methods = methods(&lines, ti, block_end(&lines, ti));
    let fwd_methods = methods(&lines, fi, block_end(&lines, fi));

    // ⚠ Assert the extraction before asserting about it. An empty slice would
    // satisfy the subset check below while measuring nothing.
    assert!(
        trait_methods.len() >= 5,
        "implausibly few trait methods ({}) — the slice is wrong, not the code",
        trait_methods.len()
    );

    let missing: Vec<&String> = trait_methods
        .iter()
        .filter(|m| !fwd_methods.contains(m))
        .collect();
    assert!(
        missing.is_empty(),
        "the Arc forwarding impl does not forward {missing:?} — a defaulted \
         method left unforwarded returns the DEFAULT for every trait object the \
         engine holds, silently. trait={trait_methods:?} forwarded={fwd_methods:?}"
    );
}
