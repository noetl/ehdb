//! **Failure domains + a second substrate implementation** (noetl/ehdb#332, F5).
//!
//! Two things were missing and they compound:
//!
//! 1. `LocalFsSubstrate` was the only *storage-backing* `DurableSubstrate`
//!    (`CountingSubstrate` is a transparent decorator), so nothing separated
//!    "the trait's contract" from "whatever the filesystem impl happens to do",
//!    and RF > 1 had nowhere to replicate **to**.
//! 2. Nothing checked whether two replicas were actually independent. In prod
//!    the substrate root sits *inside* the writer's own data dir, on one PVC —
//!    so replication buys no independent failure domain at all.
//!
//! The conformance suite runs against **both** implementations, so the contract
//! is pinned rather than assumed.

use std::path::PathBuf;

use ehdb_l0::substrate::{DurableSubstrate, InMemorySubstrate, LocalFsSubstrate};
use ehdb_l0::{
    check_replica_domains, survives_node_loss, validate_replica_domains, DomainViolation,
    FailureDomain, ReplicaDomain,
};

fn unique_dir(tag: &str) -> PathBuf {
    static N: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let n = N.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    std::env::temp_dir().join(format!("ehdb-fd-{tag}-{}-{n}-{nanos}", std::process::id()))
}

// ---------------------------------------------------------------------------
// Conformance: the same suite against both implementations.
// ---------------------------------------------------------------------------

fn conformance(s: &dyn DurableSubstrate, name: &str) {
    // put_if_absent is idempotent and does NOT overwrite.
    assert!(s.put_if_absent("parts/a", b"first").unwrap(), "{name}: new");
    assert!(
        !s.put_if_absent("parts/a", b"second").unwrap(),
        "{name}: a duplicate upload is a no-op, not an error"
    );
    assert_eq!(
        s.get_all("parts/a").unwrap(),
        b"first",
        "{name}: an immutable object must NOT be replaced by a re-put"
    );

    // put_overwrite is the mutable-pointer path and DOES replace.
    s.put_overwrite("manifest/LATEST", b"v1").unwrap();
    s.put_overwrite("manifest/LATEST", b"v2").unwrap();
    assert_eq!(s.get_all("manifest/LATEST").unwrap(), b"v2", "{name}");

    // Ranged reads.
    s.put_if_absent("parts/b", b"0123456789").unwrap();
    assert_eq!(s.get_range("parts/b", 2, 3).unwrap(), b"234", "{name}");
    // ⭐ This divergence is what the second implementation found: the
    // filesystem impl used `read_exact` and errored, the first in-memory draft
    // clamped, and the trait said nothing. Resolved fail-closed — a short read
    // would let a truncated part look intact.
    assert!(
        s.get_range("parts/b", 8, 99).is_err(),
        "{name}: a range past the end is a hard error, not a short read"
    );
    assert_eq!(
        s.get_range("parts/b", 8, 2).unwrap(),
        b"89",
        "{name}: an exact range at the end still reads"
    );

    assert!(s.exists("parts/b").unwrap(), "{name}");
    assert!(!s.exists("parts/nope").unwrap(), "{name}");
    assert!(
        s.get_all("parts/nope").is_err(),
        "{name}: a missing object is an error, not empty bytes"
    );

    let mut listed = s.list_prefix("parts/").unwrap();
    listed.sort();
    assert_eq!(listed, vec!["parts/a", "parts/b"], "{name}");

    s.delete("parts/a").unwrap();
    assert!(!s.exists("parts/a").unwrap(), "{name}");
    s.delete("parts/a").unwrap(); // idempotent
}

/// Every `impl DurableSubstrate` in the workspace, run through the same suite.
///
/// ⚠ Decorators are included deliberately. `CountingSubstrate` already broke a
/// contract once — it took the default `failure_domain()` instead of forwarding,
/// silently downgrading a real substrate to `Undeclared`. A wrapper that
/// forwards six methods correctly and the seventh wrongly is exactly as broken
/// as a bad store, and only running it through the suite catches that.
#[test]
fn every_substrate_implementation_satisfies_the_same_contract() {
    use ehdb_l0::substrate::CountingSubstrate;
    use std::sync::Arc;

    // 1. the storage-backing filesystem impl
    conformance(
        &LocalFsSubstrate::new(unique_dir("conf-fs")).unwrap(),
        "LocalFsSubstrate",
    );

    // 2. the second storage-backing impl
    conformance(&InMemorySubstrate::new("test"), "InMemorySubstrate");

    // 3. the decorator, over each of them
    conformance(
        &CountingSubstrate::new(LocalFsSubstrate::new(unique_dir("conf-count-fs")).unwrap()),
        "CountingSubstrate<LocalFsSubstrate>",
    );
    conformance(
        &CountingSubstrate::new(InMemorySubstrate::new("counted")),
        "CountingSubstrate<InMemorySubstrate>",
    );

    // 4. the `Arc<dyn ..>` blanket forward
    let arced: Arc<dyn DurableSubstrate> = Arc::new(InMemorySubstrate::new("arced"));
    conformance(&arced, "Arc<dyn DurableSubstrate>");
}

/// How many distinct `impl DurableSubstrate for` blocks the suite above covers.
///
/// ⚠ Keep this in step with that test. It is asserted against the source below,
/// so adding an implementation without conformance-testing it fails CI rather
/// than passing silently.
const CONFORMANCE_COVERED_IMPLS: usize = 4;

#[test]
fn a_new_substrate_implementation_cannot_skip_the_conformance_suite() {
    // ⚠⚠ The suite existing is not the same as the suite being APPLIED. Before
    // this guard, a fifth `DurableSubstrate` could land fully untested and CI
    // would stay green — the contract would be pinned for the impls someone
    // remembered and unpinned for the one they added.
    //
    // Counts CODE, not prose: a doc comment naming the trait has cleared this
    // kind of check before.
    let src = include_str!("../src/substrate.rs");
    let found = src
        .lines()
        .map(str::trim_start)
        .filter(|l| !l.starts_with("//") && !l.starts_with("/*") && !l.starts_with('*'))
        .filter(|l| l.contains("impl") && l.contains("DurableSubstrate for"))
        .count();

    assert_eq!(
        found, CONFORMANCE_COVERED_IMPLS,
        "found {found} `impl DurableSubstrate for` blocks in substrate.rs but the \
         conformance suite covers {CONFORMANCE_COVERED_IMPLS}. Add the new \
         implementation to `every_substrate_implementation_satisfies_the_same_contract` \
         and bump CONFORMANCE_COVERED_IMPLS — a substrate that skips the suite is \
         a contract nobody checked."
    );
}

#[test]
fn the_impl_counter_would_notice_a_new_implementation() {
    // The positive control for the guard above. If the counting logic were
    // broken — matching nothing, or matching a fixed number — the assertion
    // would pass forever and pin nothing at all.
    let src = include_str!("../src/substrate.rs");
    let synthetic = format!("{src}\nimpl DurableSubstrate for Bogus {{}}\n");
    let found = synthetic
        .lines()
        .map(str::trim_start)
        .filter(|l| !l.starts_with("//") && !l.starts_with("/*") && !l.starts_with('*'))
        .filter(|l| l.contains("impl") && l.contains("DurableSubstrate for"))
        .count();
    assert_eq!(
        found,
        CONFORMANCE_COVERED_IMPLS + 1,
        "the counter must actually see a newly added implementation"
    );

    // And it must NOT be fooled by a doc comment that merely names the trait.
    let commented = format!("{src}\n/// impl DurableSubstrate for NotReal\n");
    let found_c = commented
        .lines()
        .map(str::trim_start)
        .filter(|l| !l.starts_with("//") && !l.starts_with("/*") && !l.starts_with('*'))
        .filter(|l| l.contains("impl") && l.contains("DurableSubstrate for"))
        .count();
    assert_eq!(
        found_c, CONFORMANCE_COVERED_IMPLS,
        "a doc comment naming the trait must not count as an implementation"
    );
}

// ---------------------------------------------------------------------------
// Failure domains.
// ---------------------------------------------------------------------------

#[test]
fn two_dirs_on_one_device_are_one_failure_domain() {
    // ⚠⚠ The production shape. Different paths, same PVC. A path comparison
    // would call these independent; the device id does not.
    let root = unique_dir("same-dev");
    std::fs::create_dir_all(root.join("primary")).unwrap();
    std::fs::create_dir_all(root.join("substrate")).unwrap();

    let a = LocalFsSubstrate::new(root.join("primary")).unwrap();
    let b = LocalFsSubstrate::new(root.join("substrate")).unwrap();

    assert_eq!(
        a.failure_domain().label(),
        b.failure_domain().label(),
        "two directories on one device must resolve to ONE domain"
    );

    let violations = check_replica_domains(&[
        ReplicaDomain {
            replica: "replica-0".into(),
            domain: a.failure_domain(),
            root: Some(root.join("primary")),
        },
        ReplicaDomain {
            replica: "replica-1".into(),
            domain: b.failure_domain(),
            root: Some(root.join("substrate")),
        },
    ]);
    assert!(
        violations
            .iter()
            .any(|v| matches!(v, DomainViolation::SharedDomain { .. })),
        "must be refused: {violations:?}"
    );
}

#[test]
fn a_substrate_nested_inside_the_primary_is_refused() {
    // ⚠⚠ Prod exactly: NOETL_EHDB_TIER_SERVICE_DIR=/data/eventbus/ehdb-tier
    // inside NOETL_EVENT_BUS_WRITER_DIR=/data/eventbus.
    let outer = PathBuf::from("/data/eventbus");
    let inner = PathBuf::from("/data/eventbus/ehdb-tier");
    let violations = check_replica_domains(&[
        ReplicaDomain {
            replica: "writer-data".into(),
            domain: FailureDomain::LocalDevice {
                device_id: 1,
                root: outer.clone(),
            },
            root: Some(outer.clone()),
        },
        ReplicaDomain {
            replica: "ehdb-tier".into(),
            domain: FailureDomain::LocalDevice {
                device_id: 2, // pretend a different device: nesting is still wrong
                root: inner.clone(),
            },
            root: Some(inner.clone()),
        },
    ]);
    assert!(
        violations.iter().any(|v| matches!(
            v,
            DomainViolation::NestedPath { inner: i, outer: o }
                if i == "ehdb-tier" && o == "writer-data"
        )),
        "the nested shape must be caught even on different devices: {violations:?}"
    );
}

#[test]
fn genuinely_separate_domains_are_accepted() {
    // ⚠ The positive control. Without it a validator that refused EVERY replica
    // set would pass both tests above and look correct.
    let ok = validate_replica_domains(&[
        ReplicaDomain {
            replica: "replica-0".into(),
            domain: FailureDomain::LocalDevice {
                device_id: 1,
                root: PathBuf::from("/data/a"),
            },
            root: Some(PathBuf::from("/data/a")),
        },
        ReplicaDomain {
            replica: "replica-1".into(),
            domain: FailureDomain::Remote {
                provider: "gcs".into(),
                bucket: "noetl-parts".into(),
            },
            root: None,
        },
    ]);
    assert!(ok.is_ok(), "a real spread must be accepted: {ok:?}");
}

#[test]
fn an_undeclared_domain_is_never_assumed_independent() {
    // Silence must not read as independence.
    let v = check_replica_domains(&[
        ReplicaDomain {
            replica: "mystery".into(),
            domain: FailureDomain::Undeclared,
            root: None,
        },
        ReplicaDomain {
            replica: "replica-0".into(),
            domain: FailureDomain::LocalDevice {
                device_id: 1,
                root: PathBuf::from("/data/a"),
            },
            root: Some(PathBuf::from("/data/a")),
        },
    ]);
    assert!(v
        .iter()
        .any(|x| matches!(x, DomainViolation::Undeclared { .. })));
}

#[test]
fn two_local_disks_are_distinct_domains_but_still_die_with_the_node() {
    // ⚠ The distinction that matters for RF > 1: passing domain validation is
    // NOT the same as surviving node loss. Two disks on one machine are two
    // domains and one node.
    let replicas = [
        ReplicaDomain {
            replica: "disk-a".into(),
            domain: FailureDomain::LocalDevice {
                device_id: 1,
                root: PathBuf::from("/mnt/a"),
            },
            root: Some(PathBuf::from("/mnt/a")),
        },
        ReplicaDomain {
            replica: "disk-b".into(),
            domain: FailureDomain::LocalDevice {
                device_id: 2,
                root: PathBuf::from("/mnt/b"),
            },
            root: Some(PathBuf::from("/mnt/b")),
        },
    ];
    assert!(validate_replica_domains(&replicas).is_ok());
    assert!(
        !survives_node_loss(&replicas),
        "distinct local disks still share the node"
    );

    let with_remote = [
        replicas[0].clone(),
        ReplicaDomain {
            replica: "gcs".into(),
            domain: FailureDomain::Remote {
                provider: "gcs".into(),
                bucket: "b".into(),
            },
            root: None,
        },
    ];
    assert!(survives_node_loss(&with_remote));
}

#[test]
fn the_in_memory_substrate_is_not_counted_as_durable() {
    let s = InMemorySubstrate::new("proto");
    assert!(matches!(
        s.failure_domain(),
        FailureDomain::Ephemeral { .. }
    ));
    assert!(
        !s.failure_domain().is_independent_of_node(),
        "a prototype must never be mistaken for a durability answer"
    );
}

#[test]
fn domain_labels_leak_no_paths_or_buckets() {
    // Labels reach metrics and logs; this repo is public and prod is real.
    let remote = FailureDomain::Remote {
        provider: "gcs".into(),
        bucket: "super-secret-bucket".into(),
    };
    assert_eq!(remote.label(), "remote-gcs");
    let local = FailureDomain::LocalDevice {
        device_id: 42,
        root: PathBuf::from("/data/eventbus/private"),
    };
    assert_eq!(local.label(), "local-device-42");
    for l in [remote.label(), local.label()] {
        assert!(!l.contains('/'), "no paths in a label: {l}");
        assert!(!l.contains("secret"), "no bucket names in a label: {l}");
    }
}

#[test]
fn a_decorator_forwards_the_domain_rather_than_defaulting() {
    // ⚠ A wrapper that let this fall back to Undeclared would silently downgrade
    // a real substrate — validation would then report "undeclared" instead of
    // the actual violation.
    use ehdb_l0::substrate::CountingSubstrate;
    let dir = unique_dir("decorator");
    let inner = LocalFsSubstrate::new(&dir).unwrap();
    let expected = inner.failure_domain();
    let wrapped = CountingSubstrate::new(inner);
    assert_eq!(wrapped.failure_domain(), expected);
    assert!(!matches!(
        wrapped.failure_domain(),
        FailureDomain::Undeclared
    ));
}
