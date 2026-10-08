//! **P2 of the north star: generic registration, watch, ephemeral units, secret refs.**
//!
//! P1 gave D8 wall-clock TTL liveness. P2 makes the registry the *fleet's* rather than the
//! worker pool's, and makes discovery pushable rather than only pollable.
//!
//! Spec: `design/north-star-distributed-registry.md` §B.
//!
//! ⚠⚠ **A format-compatibility fact this file pins, which my P1 commit got half right.**
//! `RuntimeOp` carries `#[serde(deny_unknown_fields)]`. The repo already documents the
//! consequence in `ehdb-stream/tests/record_envelope_additivity.rs`: *"a reader built
//! before a field was added REJECTS a record that carries it — it does not ignore it.
//! Forward compatibility at the envelope is not 'lossy', it is an error."*
//!
//! So adding a field is **backward compatible** (new reader, old record: `serde(default)`
//! fills it) and **forward INCOMPATIBLE** (old reader, new record: decode error). My P1
//! commit claimed compatibility and only established the backward half. The practical
//! consequence is an **upgrade ordering requirement — readers before writers** — and
//! `the_compatibility_asymmetry_is_explicit` below pins it so it is documented rather than
//! discovered during a rolling upgrade.

use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use ehdb_l0::substrate::DurableSubstrate;
use ehdb_l0::{LocalFsSubstrate, RuntimeKind, RuntimeStore, SecretRef};

fn dir(tag: &str) -> std::path::PathBuf {
    static N: AtomicU64 = AtomicU64::new(0);
    let n = N.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir().join(format!("ehdb-p2-{tag}-{}-{n}", std::process::id()))
}

fn store(root: &Path) -> RuntimeStore {
    let obj = root.join("obj");
    let hot = root.join("hot");
    std::fs::create_dir_all(&obj).unwrap();
    std::fs::create_dir_all(&hot).unwrap();
    let s: Arc<dyn DurableSubstrate> = Arc::new(LocalFsSubstrate::new(&obj).unwrap());
    RuntimeStore::open(RuntimeStore::config(&hot), s).unwrap()
}

const SEC: u64 = 1_000_000;

// =========================================================================
// B1 — generic (kind, id) registration + discovery BY KIND
// =========================================================================

#[test]
fn every_kind_of_object_registers_and_is_discoverable_by_kind() {
    let d = dir("kind");
    let mut st = store(&d);
    let t = 1_000 * SEC;

    st.register_kind(RuntimeKind::Server, "server-0", "api;addr=10.0.0.1:8082", t)
        .unwrap();
    st.register_kind(RuntimeKind::Gateway, "gw-0", "sse;addr=10.0.0.2:8090", t)
        .unwrap();
    st.register_kind(
        RuntimeKind::Ehdb,
        "ehdb-0",
        "tier=events;addr=10.0.0.3:9110",
        t,
    )
    .unwrap();
    st.register_kind(RuntimeKind::Worker, "w-1", "pool=user;arch=arm64", t)
        .unwrap();
    st.register_kind(RuntimeKind::Worker, "w-2", "pool=system;arch=amd64", t)
        .unwrap();
    st.register_kind(RuntimeKind::Playbook, "muno:playbooks:profile", "v6", t)
        .unwrap();

    let ttl = 30 * SEC;
    let all = st.list_live_at(t, ttl).unwrap();
    assert_eq!(all.len(), 6, "all six registered: {all:?}");

    // Discovery BY KIND is the point — a caller asking "where are the gateways" must not
    // have to filter a fleet-wide list and guess which ids are gateways from their names.
    let workers = st.discover(RuntimeKind::Worker, t, ttl).unwrap();
    let ids: Vec<&str> = workers.iter().map(|s| s.id()).collect();
    println!("  discover(worker) -> {ids:?}");
    assert_eq!(ids, vec!["w-1", "w-2"]);

    for (k, want) in [
        (RuntimeKind::Server, vec!["server-0"]),
        (RuntimeKind::Gateway, vec!["gw-0"]),
        (RuntimeKind::Ehdb, vec!["ehdb-0"]),
        (RuntimeKind::Playbook, vec!["muno:playbooks:profile"]),
    ] {
        let got: Vec<String> = st
            .discover(k, t, ttl)
            .unwrap()
            .into_iter()
            .map(|s| s.worker_id)
            .collect();
        println!("  discover({k:?}) -> {got:?}");
        assert_eq!(got, want, "discovery by {k:?}");
    }

    // A kind with nothing registered returns empty, not everything.
    assert!(
        st.discover(RuntimeKind::Execution, t, ttl)
            .unwrap()
            .is_empty(),
        "an unused kind must be empty, not a fleet-wide list"
    );
    let _ = std::fs::remove_dir_all(&d);
}

/// A record written by the pre-P2 API has no kind. It must default to `Worker` — the only
/// thing D8 ever held — rather than vanishing from every `discover` call.
#[test]
fn a_record_with_no_kind_defaults_to_worker_not_to_nothing() {
    let d = dir("legacykind");
    let mut st = store(&d);
    st.register("legacy", "c").unwrap(); // pre-P2 API: no kind, no timestamp
    let live = st.list_live_at(9_999 * SEC, 30 * SEC).unwrap();
    assert_eq!(live.len(), 1, "the legacy record survives");
    assert_eq!(
        live[0].kind,
        RuntimeKind::Worker,
        "it must read as a worker"
    );
    let found = st
        .discover(RuntimeKind::Worker, 9_999 * SEC, 30 * SEC)
        .unwrap();
    println!("  legacy record discovered as worker: {}", found.len());
    assert_eq!(found.len(), 1, "and be discoverable under that kind");
    let _ = std::fs::remove_dir_all(&d);
}

// =========================================================================
// B3 — watch
// =========================================================================

#[test]
fn watch_returns_only_what_is_new_since_a_cursor() {
    let d = dir("watch");
    let mut st = store(&d);
    let t = 100 * SEC;

    // Nothing yet: a watch from 0 is empty, and the cursor stays 0.
    let (ops, cursor) = st.watch_since(0).unwrap();
    assert!(ops.is_empty(), "no ops yet");
    assert_eq!(cursor, 0, "an empty watch must not advance the cursor");

    st.register_kind(RuntimeKind::Worker, "w-1", "c", t)
        .unwrap();
    st.register_kind(RuntimeKind::Worker, "w-2", "c", t)
        .unwrap();

    let (ops, c1) = st.watch_since(0).unwrap();
    println!("  watch_since(0) -> {} ops, cursor {c1}", ops.len());
    assert_eq!(ops.len(), 2, "both registrations");
    assert!(c1 > 0, "the cursor must advance");

    // ⚠ The property that makes a watch a watch: resuming from the cursor returns NOTHING
    // when nothing changed. A watch that re-delivers the whole log every poll is a scan,
    // and it would still look "working" in a test that only ever polls from 0.
    let (ops, c2) = st.watch_since(c1).unwrap();
    println!("  watch_since({c1}) with no change -> {} ops", ops.len());
    assert!(
        ops.is_empty(),
        "resuming from the cursor must yield nothing new: {ops:?}"
    );
    assert_eq!(c2, c1, "and must not move the cursor");

    // One more change, and only that one comes back.
    st.heartbeat_at("w-1", t + SEC).unwrap();
    let (ops, c3) = st.watch_since(c1).unwrap();
    println!("  after one heartbeat -> {} ops", ops.len());
    assert_eq!(ops.len(), 1, "exactly the new op: {ops:?}");
    assert_eq!(ops[0].worker_id, "w-1");
    assert!(c3 > c1);

    // Deregistration is observable too — a discovery consumer must learn about departure,
    // not just arrival.
    st.deregister("w-2").unwrap();
    let (ops, _) = st.watch_since(c3).unwrap();
    assert_eq!(ops.len(), 1, "the deregister is delivered");
    assert_eq!(ops[0].worker_id, "w-2");
    println!("  departure delivered: {:?}", ops[0].event);
    let _ = std::fs::remove_dir_all(&d);
}

// =========================================================================
// B4 — ephemeral runtime units (live executions) with a short TTL
// =========================================================================

#[test]
fn an_execution_registers_as_an_ephemeral_unit_and_expires_without_a_reaper() {
    let d = dir("exec");
    let mut st = store(&d);
    let t0 = 500 * SEC;
    let ttl = 10 * SEC;

    st.register_kind(
        RuntimeKind::Execution,
        "366376937288900608",
        "playbook=muno/profile",
        t0,
    )
    .unwrap();
    st.register_kind(
        RuntimeKind::Execution,
        "366376945371324416",
        "playbook=muno/itinerary",
        t0,
    )
    .unwrap();
    st.register_kind(RuntimeKind::Worker, "w-1", "pool=user", t0)
        .unwrap();

    let execs = st.discover(RuntimeKind::Execution, t0, ttl).unwrap();
    println!("  live executions at t0: {}", execs.len());
    assert_eq!(execs.len(), 2);

    // One keeps running; the other finishes and simply stops renewing.
    st.heartbeat_at("366376937288900608", t0 + 8 * SEC).unwrap();

    let after = st
        .discover(RuntimeKind::Execution, t0 + 12 * SEC, ttl)
        .unwrap();
    let ids: Vec<&str> = after.iter().map(|s| s.id()).collect();
    println!("  at t0+12s (ttl 10s) -> {ids:?}");
    assert_eq!(
        ids,
        vec!["366376937288900608"],
        "the renewed execution survives; the finished one expires with NO tombstone and NO \
         reaper — that is why TTL is the right primitive for ephemera"
    );

    // And the long-lived worker is unaffected by the executions' short TTL.
    assert_eq!(
        st.discover(RuntimeKind::Worker, t0 + 12 * SEC, 30 * SEC)
            .unwrap()
            .len(),
        1,
        "a different kind's TTL must not evict the worker"
    );
    let _ = std::fs::remove_dir_all(&d);
}

// =========================================================================
// B5 — secret REFERENCES, never material
// =========================================================================

#[test]
fn a_secret_reference_is_a_pointer_and_material_is_refused() {
    // The platform rule already exists (keychain alias, resolved at step-execution time).
    // EHDB's job is to hold the POINTER and refuse anything that looks like the value.
    let ok = SecretRef::parse("gsm://projects/noetl/secrets/adiona_actor#3").unwrap();
    println!(
        "  parsed: provider={} path={} version={:?}",
        ok.provider, ok.path, ok.version
    );
    assert_eq!(ok.provider, "gsm");
    assert_eq!(ok.path, "projects/noetl/secrets/adiona_actor");
    assert_eq!(ok.version.as_deref(), Some("3"));

    // A version is optional — "latest" is a legitimate reference.
    let nover = SecretRef::parse("keychain://adiona_actor").unwrap();
    assert_eq!(nover.version, None);

    // ⚠ Everything below must be REFUSED. A catalog that accepts material becomes a second
    // secret store, which is the failure this type exists to prevent — and the same rule
    // `catalog-extract` already applies when it refuses to catalogue an inline credential
    // mapping.
    for (bad, why) in [
        ("", "empty"),
        (
            "adiona_actor",
            "no provider scheme — ambiguous between a ref and a literal",
        ),
        ("gsm://", "no path"),
        (
            "{\"user\":\"u\",\"password\":\"p\"}",
            "an inline credential mapping",
        ),
        (
            "postgres://user:hunter2@host/db",
            "a DSN with embedded credentials",
        ),
        ("-----BEGIN PRIVATE KEY-----\nMIIE", "key material"),
        (
            "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxIn0.sig",
            "a JWT",
        ),
        // ⚠⚠ These three are the cases ONLY the material-marker list can catch: material
        // smuggled inside an otherwise-VALID reference. Without them the marker list was
        // inert — a mutation disabling it entirely left every test green, because each of
        // the cases above is refused by a different rule (no scheme, or credentials in the
        // userinfo). An inert guard is indistinguishable from a working one until the thing
        // it guards against actually arrives.
        (
            "gsm://-----BEGIN PRIVATE KEY-----MIIEvQ",
            "key material behind a valid scheme",
        ),
        (
            "vault://secret/{\"password\":\"hunter2\"}",
            "a credential mapping behind a valid scheme",
        ),
        (
            "keychain://x#-----BEGIN PRIVATE KEY-----",
            "key material in the version slot",
        ),
    ] {
        let got = SecretRef::parse(bad);
        println!(
            "  refuse {why:<52} -> {}",
            if got.is_err() { "ok" } else { "ACCEPTED" }
        );
        assert!(got.is_err(), "must refuse {why}: {bad:?}");
    }

    // And a reference must never render its target — only the pointer.
    let shown = format!("{ok}");
    println!("  display: {shown}");
    assert!(!shown.contains("hunter2"));
    assert_eq!(shown, "gsm://projects/noetl/secrets/adiona_actor#3");
}

/// ⚠⚠ The compatibility asymmetry, pinned.
#[test]
fn the_compatibility_asymmetry_is_explicit() {
    // BACKWARD (new reader, old record): a record missing the new fields decodes, because
    // both carry `#[serde(default)]`.
    let old_json =
        r#"{"op_seq":1,"worker_id":"w","event":"register","heartbeat":1,"contract":"c"}"#;
    let decoded: Result<ehdb_l0::RuntimeOp, _> = serde_json::from_str(old_json);
    println!(
        "  new reader + old record: {}",
        if decoded.is_ok() { "decodes" } else { "FAILS" }
    );
    assert!(
        decoded.is_ok(),
        "backward compatibility must hold: {decoded:?}"
    );
    let op = decoded.unwrap();
    assert_eq!(
        op.last_seen_micros, 0,
        "absent timestamp defaults to unknown"
    );
    assert_eq!(
        op.kind,
        RuntimeKind::Worker,
        "absent kind defaults to worker"
    );

    // FORWARD (old reader, new record): `deny_unknown_fields` makes this an ERROR, not a
    // lossy read. Simulated with a struct that lacks the new fields, which is exactly what
    // an older binary's type looks like.
    #[derive(serde::Deserialize, Debug)]
    #[serde(deny_unknown_fields)]
    #[allow(dead_code)]
    struct OldRuntimeOp {
        op_seq: u64,
        worker_id: String,
        event: String,
        heartbeat: u64,
        contract: String,
    }
    let new_json = r#"{"op_seq":1,"worker_id":"w","event":"register","heartbeat":1,"contract":"c","last_seen_micros":123,"kind":"Server"}"#;
    let old_read: Result<OldRuntimeOp, _> = serde_json::from_str(new_json);
    println!(
        "  old reader + new record: {}",
        if old_read.is_err() {
            "REJECTS (as designed)"
        } else {
            "accepted"
        }
    );
    assert!(
        old_read.is_err(),
        "an old reader MUST reject a new record — this is the documented envelope design \
         (ehdb-stream/tests/record_envelope_additivity.rs), and it means rolling upgrades \
         must deploy READERS BEFORE WRITERS"
    );
}
