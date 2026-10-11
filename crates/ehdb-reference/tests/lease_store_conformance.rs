//! **The `LeaseStore` conformance suite, and proof that it discriminates.**
//!
//! `docs/spec/lease-election-k8s-binding.md` requires before promotion:
//!
//! > Adapter conformance: the same suite `InMemoryLeaseStore` passes, run
//! > against a real API server in kind.
//!
//! This file establishes the first half — that the extracted suite passes for
//! the reference store — and, more importantly, that it **rejects** a store
//! which does not satisfy the contract. A suite that has never rejected
//! anything is indistinguishable from a suite that cannot, and this codebase
//! has shipped several of those.

use ehdb_reference::election::conformance::{self, IgnoresVersionStore};
use ehdb_reference::election::{InMemoryLeaseStore, LeaseStore};

fn prefix(tag: &str) -> String {
    static N: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let n = N.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    format!("conf-{tag}-{}-{n}", std::process::id())
}

#[test]
fn the_reference_store_satisfies_every_clause() {
    let store = InMemoryLeaseStore::new();
    let violations = conformance::run(&store, &prefix("inmem"));
    assert!(
        violations.is_empty(),
        "InMemoryLeaseStore is the contract's reference implementation and must pass: {violations:#?}"
    );
}

/// ⭐ The suite's positive control.
#[test]
fn the_suite_rejects_a_store_that_ignores_cas() {
    let store = IgnoresVersionStore::default();
    let violations = conformance::run(&store, &prefix("ignores"));
    assert!(
        !violations.is_empty(),
        "a store that ignores expected_version MUST be rejected, or the suite proves nothing"
    );
    let clauses: Vec<&str> = violations.iter().map(|v| v.clause).collect();
    assert!(
        clauses.contains(&"cas_stale_version_refused"),
        "the rejection must name the mutual-exclusion clause specifically, not merely fail \
         somewhere; got {clauses:?}"
    );
}

/// ⚠ The suite must be re-runnable against a persistent store. Two runs with
/// different prefixes on the SAME store instance must both pass — otherwise it
/// would pass once against a real API server and fail forever after on the
/// create-on-absent clause, which reads like an adapter bug.
#[test]
fn the_suite_is_rerunnable_against_a_persistent_store() {
    let store = InMemoryLeaseStore::new();
    for i in 0..3 {
        let v = conformance::run(&store, &prefix(&format!("rerun{i}")));
        assert!(v.is_empty(), "run {i} failed: {v:#?}");
    }
}

/// ⚠ And it must NOT be re-runnable with the same prefix — i.e. the prefix is
/// doing real work. If this passed, the uniqueness requirement would be
/// decorative and a caller could safely ignore it.
#[test]
fn reusing_a_prefix_on_a_persistent_store_is_caught() {
    let store = InMemoryLeaseStore::new();
    let p = prefix("sameprefix");
    assert!(conformance::run(&store, &p).is_empty(), "first run");
    let second = conformance::run(&store, &p);
    assert!(
        !second.is_empty(),
        "reusing a prefix must be caught — otherwise the per-run uniqueness the suite \
         documents is not actually required and a real-API-server caller would be misled"
    );
}

/// A sanity check on the stub itself: it is only wrong in the CAS clause, which
/// is what makes it a fair control rather than a strawman that fails everything.
#[test]
fn the_stub_is_wrong_only_where_intended() {
    let store = IgnoresVersionStore::default();
    let p = prefix("stubshape");
    // The non-CAS parts behave: create is conditional, reads work.
    let r = ehdb_reference::election::LeaseRecord {
        holder: "n".into(),
        transitions: 1,
        renewed_at_millis: 1,
        duration_secs: 15,
        version: 0,
    };
    assert!(store.create(&p, &r).unwrap(), "create on absent");
    assert!(
        !store.create(&p, &r).unwrap(),
        "create on existing is still conditional"
    );
    assert!(store.read(&p).unwrap().is_some(), "read works");
    // But it accepts a stale version.
    assert!(
        store.compare_and_swap(&p, 999_999, &r).unwrap(),
        "the stub's defect: it accepts any expected_version"
    );
}
