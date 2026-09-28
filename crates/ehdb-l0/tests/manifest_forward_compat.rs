//! **Manifest forward-compatibility — the M1 expand-first gate**
//! (multi-region spec M1, `specs/active/2026-09-18-multiregion-ehdb/M1-locality.md`).
//!
//! ## What this protects
//!
//! M1 adds a `Locality` to [`ReplicaLocation`]. Manifests persist as JSON and a
//! rollback binary must keep reading manifests a newer binary wrote — on a tier
//! that serves `primary`. The precedent is recorded verbatim in
//! `dataset.rs:154`, where `deny_unknown_fields` was removed from `EventRecord`
//! *before* `event_id` was written:
//!
//! > *"Tolerating unknown fields must therefore ship and be deployed BEFORE
//! > anything writes one."*
//!
//! ## The audit that produced this file
//!
//! Denominator: **5 of 5** serialisable types in `catalog.rs` carried
//! `#[serde(deny_unknown_fields)]` — `ReplicaLocation`, `GranuleMark`,
//! `SparseIndex`, `PartMeta`, `Manifest`. So the whole manifest path was
//! closed to additive evolution, not just the one struct M1 touches.
//!
//! ⚠ These tests are the **Release A** half (tolerate). Nothing here writes a
//! locality; the field itself is a later, separately-shippable step.

use ehdb_l0::catalog::{Manifest, PartMeta, ReplicaLocation, SparseIndex};

/// A manifest exactly as today's binary writes one.
fn manifest_today() -> Manifest {
    Manifest {
        dataset: "d1_event_log".to_string(),
        version: 7,
        parts: vec![PartMeta {
            part_id: "shard-0-seq-1-10".to_string(),
            partition: 0,
            min_sequence: 1,
            max_sequence: 10,
            record_count: 10,
            byte_size: 2048,
            replicas: vec![ReplicaLocation {
                replica: "replica-0".to_string(),
                key: "parts/d1_event_log/shard-0/shard-0-seq-1-10.eslog".to_string(),
            }],
            local_path: None,
            sparse_index: SparseIndex {
                granule_size: 128,
                marks: vec![],
            },
            execution_bloom: None,
            granule_blooms: vec![],
        }],
        reclaimed_through: 0,
    }
}

/// ⭐ **The gate.** A newer binary writes `locality` into a `ReplicaLocation`;
/// this binary must still read the manifest.
///
/// This is the test that was RED before `deny_unknown_fields` came off
/// `ReplicaLocation` — and red in the exact way that matters: `serde` reports
/// `unknown field 'locality'` and the whole manifest fails to parse, so the
/// node cannot open the dataset at all.
#[test]
fn replica_location_tolerates_a_locality_written_by_a_newer_binary() {
    let json = r#"{
        "replica": "replica-0",
        "key": "parts/d1_event_log/shard-0/p.eslog",
        "locality": {"region": "us-central1", "zone": "us-central1-a"}
    }"#;
    let got: ReplicaLocation =
        serde_json::from_str(json).expect("a newer binary's replica entry must still parse");
    assert_eq!(got.replica, "replica-0");
}

/// The same hazard one level up: the unknown field arrives nested inside a
/// whole manifest, which is how it actually reaches a rollback binary.
#[test]
fn whole_manifest_tolerates_a_nested_unknown_field() {
    let json = r#"{
        "dataset": "d1_event_log",
        "version": 7,
        "parts": [{
            "part_id": "shard-0-seq-1-10", "partition": 0,
            "min_sequence": 1, "max_sequence": 10,
            "record_count": 10, "byte_size": 2048,
            "replicas": [{"replica": "replica-0", "key": "k",
                          "locality": {"region": "us-central1"}}],
            "local_path": null,
            "sparse_index": {"granule_size": 128, "marks": []},
            "execution_bloom": null,
            "granule_blooms": []
        }],
        "reclaimed_through": 0
    }"#;
    let got: Manifest = serde_json::from_str(json).expect("nested unknown field must not fail");
    assert_eq!(got.parts[0].replicas[0].replica, "replica-0");
}

/// ⭐ **The other half, and the one a tolerance change could silently break:**
/// removing `deny_unknown_fields` must not change what this binary *writes*.
/// A manifest with no locality has to serialise byte-identically to today, or
/// the rollback story runs the other way.
#[test]
fn todays_manifest_serialises_byte_identically() {
    const EXPECTED: &str = r#"{"dataset":"d1_event_log","version":7,"parts":[{"part_id":"shard-0-seq-1-10","partition":0,"min_sequence":1,"max_sequence":10,"record_count":10,"byte_size":2048,"replicas":[{"replica":"replica-0","key":"parts/d1_event_log/shard-0/shard-0-seq-1-10.eslog"}],"local_path":null,"sparse_index":{"granule_size":128,"marks":[]},"execution_bloom":null,"granule_blooms":[]}],"reclaimed_through":0}"#;
    let got = serde_json::to_string(&manifest_today()).expect("serialise");
    assert_eq!(
        got, EXPECTED,
        "manifest bytes changed — rollback compat broken"
    );
}

/// Round-trip, so tolerance is not mistaken for lossy parsing.
#[test]
fn round_trip_is_lossless() {
    let m = manifest_today();
    let back: Manifest = serde_json::from_str(&serde_json::to_string(&m).unwrap()).unwrap();
    assert_eq!(m, back);
}

/// ⚠ **Negative control.** A field that is genuinely required must still be
/// required. Without this, "tolerates unknown fields" could be satisfied by a
/// deserialiser that tolerates *everything*, including a missing `replica` —
/// which would turn a corrupt manifest into a silently empty one.
#[test]
fn a_missing_required_field_is_still_an_error() {
    let json = r#"{"key": "k", "locality": {"region": "us-central1"}}"#;
    let err = serde_json::from_str::<ReplicaLocation>(json)
        .expect_err("a manifest missing `replica` must still be refused");
    assert!(
        err.to_string().contains("replica"),
        "error should name the missing field, got: {err}"
    );
}
