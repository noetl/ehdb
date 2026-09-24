//! **Pluggable storage roles + EventStore conformance** (RFC §12).
//!
//! The load-bearing test here is not "EHDB passes". It is
//! `the_suite_rejects_the_stub` and `the_suite_rejects_the_jetstream_sketch`:
//! a conformance suite that passes everything discriminates nothing and is
//! worse than no suite, because it licenses a backend it never checked.

use std::time::Instant;

use ehdb_l0::chain::ChainStore;
use ehdb_l0::chain_alt::{JetStreamSketchEventStore, StubEventStore};
use ehdb_l0::store_role::{
    conformance, resolve_backend, resolved_selection, Backend, EventStore, StorageRole,
};

// ---------------------------------------------------------------------------
// Config: every role defaults to EHDB; a typo never moves a workload.
// ---------------------------------------------------------------------------

#[test]
fn every_role_defaults_to_ehdb() {
    for role in StorageRole::ALL {
        // No explicit choice, and (in a clean env) no flag.
        assert_eq!(
            Backend::parse(None),
            Backend::Ehdb,
            "role {} must default to ehdb",
            role.label()
        );
    }
    assert_eq!(resolved_selection().len(), StorageRole::ALL.len());
}

#[test]
fn backends_parse_by_name() {
    assert_eq!(Backend::parse(Some("jetstream")), Backend::JetStream);
    assert_eq!(Backend::parse(Some(" COCKROACH-KV ")), Backend::CockroachKv);
    assert_eq!(Backend::parse(Some("cockroach_kv")), Backend::CockroachKv);
    assert_eq!(Backend::parse(Some("postgres")), Backend::Postgres);
    assert_eq!(Backend::parse(Some("redis")), Backend::Redis);
    assert_eq!(Backend::parse(Some("stub")), Backend::Stub);
}

/// ⚠ ops#311's property: "a call site that passes nothing and runs with no flag
/// behaves exactly as it does today." A typo must not silently move a workload
/// onto a different store.
#[test]
fn an_unrecognised_backend_falls_back_to_ehdb() {
    for junk in ["", " ", "jetsream", "nats", "cockroach", "sql", "true", "1"] {
        assert_eq!(
            Backend::parse(Some(junk)),
            Backend::Ehdb,
            "unrecognised backend {junk:?} must fail safe to ehdb"
        );
    }
}

/// Precedence: explicit argument wins over the flag.
#[test]
fn an_explicit_choice_overrides_the_flag() {
    let role = StorageRole::EventLog;
    let prev = std::env::var(role.env_var()).ok();
    unsafe { std::env::set_var(role.env_var(), "jetstream") };
    assert_eq!(
        resolve_backend(role, None),
        Backend::JetStream,
        "flag applies"
    );
    assert_eq!(
        resolve_backend(role, Some(Backend::Ehdb)),
        Backend::Ehdb,
        "an explicit argument must win over the flag"
    );
    match prev {
        Some(v) => unsafe { std::env::set_var(role.env_var(), v) },
        None => unsafe { std::env::remove_var(role.env_var()) },
    }
}

/// Roles are selected independently — setting one must not move another.
#[test]
fn roles_are_selected_independently() {
    let prev = std::env::var(StorageRole::Context.env_var()).ok();
    unsafe { std::env::set_var(StorageRole::Context.env_var(), "redis") };
    assert_eq!(resolve_backend(StorageRole::Context, None), Backend::Redis);
    assert_eq!(
        resolve_backend(StorageRole::Projection, None),
        Backend::Ehdb,
        "selecting Context must not move Projection"
    );
    match prev {
        Some(v) => unsafe { std::env::set_var(StorageRole::Context.env_var(), v) },
        None => unsafe { std::env::remove_var(StorageRole::Context.env_var()) },
    }
}

#[test]
fn every_role_has_a_distinct_env_var_and_label() {
    let mut vars: Vec<&str> = StorageRole::ALL.iter().map(|r| r.env_var()).collect();
    let n = vars.len();
    vars.sort();
    vars.dedup();
    assert_eq!(vars.len(), n, "two roles share an env var");
    assert!(vars.iter().all(|v| v.starts_with("NOETL_STORE_")));
}

// ---------------------------------------------------------------------------
// Conformance: EHDB passes, and the suite REJECTS what cannot conform.
// ---------------------------------------------------------------------------

#[test]
fn ehdb_passes_the_eventstore_contract() {
    let mut s = ChainStore::new();
    let v = conformance::run(&mut s);
    assert!(v.is_empty(), "EHDB must conform, got violations: {v:#?}");
    assert!(conformance::passes(&mut ChainStore::new()));
    assert_eq!(EventStore::backend_name(&ChainStore::new()), "ehdb");
}

/// ⭐⭐ **The suite's own positive control.** If a backend that accepts
/// everything and remembers nothing were to PASS, the suite would be licensing
/// backends it never checked.
#[test]
fn the_suite_rejects_the_stub() {
    let mut stub = StubEventStore;
    let v = conformance::run(&mut stub);
    assert!(!v.is_empty(), "the stub must be REJECTED");
    assert!(!conformance::passes(&mut StubEventStore));

    // And it must fail on the clauses that matter, named.
    let clauses: Vec<&str> = v.iter().map(|x| x.clause).collect();
    for expected in [
        "append/enforces-head (I1)",
        "parent_of/names-the-gap",
        "chain_is_complete/hole-is-incomplete",
    ] {
        assert!(
            clauses.contains(&expected),
            "the stub should have failed {expected}; failed: {clauses:?}"
        );
    }
}

/// ⭐⭐ **The sketch proves the contract has teeth.** A subject-per-execution
/// store gives ordered append and ordered replay — genuinely — but no addressed
/// predecessor lookup and no notion of a named missing link. It fails on exactly
/// the two clauses that are the point of the redesign, which is what shows the
/// contract is a real specification rather than a restatement of EHDB's
/// signatures.
#[test]
fn the_suite_rejects_the_jetstream_sketch_on_the_clauses_that_matter() {
    let mut js = JetStreamSketchEventStore::new();
    let v = conformance::run(&mut js);
    assert!(!v.is_empty(), "the sketch must be REJECTED");

    let clauses: Vec<&str> = v.iter().map(|x| x.clause).collect();
    assert!(
        clauses.contains(&"parent_of/names-the-gap"),
        "a replay cannot distinguish 'not yet' from 'not a thing'; failed: {clauses:?}"
    );
    assert!(
        clauses.contains(&"chain_is_complete/hole-is-incomplete"),
        "'everything in the subject' always looks complete; failed: {clauses:?}"
    );
    assert!(
        clauses.contains(&"append/enforces-head (I1)"),
        "nothing in a plain publish refuses a stale-head append; failed: {clauses:?}"
    );

    // ⚠ And it must PASS the clauses it genuinely satisfies — otherwise the
    // rejection above could be the suite failing everything indiscriminately.
    assert!(
        !clauses.contains(&"chain/ascending"),
        "the sketch DOES give ordered replay and must pass that clause; failed: {clauses:?}"
    );
    assert!(
        !clauses.contains(&"chain/partition-isolation"),
        "one subject per execution IS partition isolation; failed: {clauses:?}"
    );
}

/// The suite reports every violation, not the first — a backend failing three
/// clauses should show three.
#[test]
fn the_suite_reports_all_violations_not_just_the_first() {
    let mut stub = StubEventStore;
    assert!(
        conformance::run(&mut stub).len() >= 3,
        "expected multiple violations from a maximally-wrong backend"
    );
}

// ---------------------------------------------------------------------------
// The thin-seam requirement, measured.
// ---------------------------------------------------------------------------

/// ⭐ **The seam must not slow the hot path.** Measured through `dyn EventStore`
/// against the concrete type over the same work.
///
/// The reason this can hold at all is granularity: `walk_from_head` crosses the
/// seam **once** for the whole chain, so a 200-event walk pays one virtual call,
/// not 200. A fine-grained trait — `next_event()` per step — would put a
/// virtual call and an allocation in the inner loop, and this test is what
/// would catch that regression.
#[test]
fn the_trait_seam_does_not_slow_the_hot_path() {
    let mut concrete = ChainStore::new();
    let mut prev: Option<String> = None;
    for i in 0..100 {
        let ev = format!("ev-{i}");
        concrete
            .append("e1", &ev, prev.as_deref(), None, "{}")
            .unwrap();
        prev = Some(ev);
    }

    let direct = {
        let t = Instant::now();
        for _ in 0..500 {
            let w = ehdb_l0::chain::ChainStore::walk_from_head(&concrete, "e1").unwrap();
            assert_eq!(w.len(), 100);
        }
        t.elapsed()
    };
    let through_trait = {
        let dynref: &dyn EventStore = &concrete;
        let t = Instant::now();
        for _ in 0..500 {
            let w = dynref.walk_from_head("e1").unwrap();
            assert_eq!(w.len(), 100);
        }
        t.elapsed()
    };

    // The trait returns owned events, so it pays 200 clones per walk against the
    // direct path's borrows. That is a real and bounded cost, not a per-event
    // virtual-call cost. Generous bound: the seam must not be an order of
    // magnitude.
    let ratio = through_trait.as_secs_f64() / direct.as_secs_f64().max(1e-9);
    assert!(
        ratio < 10.0,
        "the seam cost {ratio:.2}x on a 100-event walk (direct={direct:?} \
         trait={through_trait:?}) — if this fails, check whether the trait became \
         fine-grained enough to put a virtual call in the inner loop"
    );
}

/// Two structurally different backends behind one `dyn` — the abstraction is
/// real, not EHDB with extra steps.
#[test]
fn both_backends_are_usable_through_the_same_trait_object() {
    let mut a = ChainStore::new();
    let mut b = JetStreamSketchEventStore::new();
    let stores: Vec<&mut dyn EventStore> = vec![&mut a, &mut b];
    let mut names = Vec::new();
    for s in stores {
        s.append("e1", "x", None, None, "{}").unwrap();
        assert_eq!(s.chain("e1").unwrap().len(), 1);
        names.push(s.backend_name());
    }
    assert_eq!(names, vec!["ehdb", "jetstream-sketch"]);
}
