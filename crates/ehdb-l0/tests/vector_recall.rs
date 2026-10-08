//! **P6: recall@k, and what the number can honestly mean** (noetl/ai-meta#455).
//!
//! The north-star spec asks for "an **honest recall measure** — cosine over a brute-force
//! scan is exact and O(n); an ANN index is not, and *semantic search works* is
//! unfalsifiable without a recall@k number against a known-answer set."
//!
//! # ⭐ Measuring first changed what the number means
//!
//! `VectorStore::top_k` (`vector.rs:164`) materialises **every live point** in the
//! collection, scores all of them, sorts, and truncates. There is no approximation
//! anywhere in it. So:
//!
//! > **recall@k is 1.0 by construction, and a reported 1.0 is therefore not evidence
//! > that search works.**
//!
//! It is evidence only that the exhaustive scan was exhaustive. Recall becomes a real,
//! falsifiable number the moment an ANN index exists — and **not before**. Publishing
//! "recall@10 = 1.00" today would be a vanity metric: unfalsifiable in exactly the way
//! the spec warned about, one level up.
//!
//! So this file does two different jobs, and keeps them apart:
//!
//! 1. **recall@k as a correctness guard.** Against a known-answer set it *must* be 1.0 —
//!    anything less is a defect in the scan or the fold, not an index tradeoff. And the
//!    measure carries a control proving it **can** report less than 1.0, because a recall
//!    function that always returns 1.0 is indistinguishable from a perfect search.
//! 2. **The cost that actually bounds the design** — see `benches/vector_query.rs`. Cost,
//!    not recall, is what decides when an ANN index is needed.
//!
//! # The catalog attachment
//!
//! P6 asks for embeddings "attached to catalog objects". No new dataset is needed:
//! `collection` is the partition **and** the index dimension, and `point_id` is free-form,
//! so a catalog object's `(resource_type, path)` identity maps onto `(collection,
//! point_id)` directly. That convention is pinned below so it cannot drift silently.

use std::collections::BTreeSet;
use std::sync::Arc;

use ehdb_l0::substrate::DurableSubstrate;
use ehdb_l0::{LocalFsSubstrate, VectorStore};

fn dir(tag: &str) -> std::path::PathBuf {
    static N: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let n = N.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    std::env::temp_dir().join(format!("ehdb-vrecall-{tag}-{}-{n}", std::process::id()))
}

fn store(tag: &str) -> (VectorStore, std::path::PathBuf) {
    let root = dir(tag);
    let hot = root.join("hot");
    let obj = root.join("obj");
    std::fs::create_dir_all(&hot).unwrap();
    std::fs::create_dir_all(&obj).unwrap();
    let s: Arc<dyn DurableSubstrate> = Arc::new(LocalFsSubstrate::new(&obj).unwrap());
    let st = VectorStore::open(VectorStore::config(&hot).with_seal_max_records(64), s).unwrap();
    (st, root)
}

/// A deterministic unit-ish vector: one dominant axis plus small deterministic noise, so
/// nearest-neighbour answers are known in advance rather than asserted from the output.
///
/// ⚠ **`axis` must be `< dim`, and this panics otherwise.** The first version of this file
/// built 64 points over 16 dimensions, so every point with `axis >= 16` had **no dominant
/// component at all** — and at `jitter = 0.0` the query vector for such an axis is the
/// **zero vector**, against which cosine is undefined. The resulting ranking was arbitrary
/// and `recall@1` read **0.0**, which looked like an engine defect and was a defect in the
/// known-answer set. A fixture that cannot express the answer it claims to know makes every
/// assertion about it meaningless, so the precondition is now enforced rather than assumed.
fn planted(axis: usize, dim: usize, jitter: f32) -> Vec<f32> {
    assert!(
        axis < dim,
        "planted({axis}, {dim}) has no axis {axis}: the vector would carry no dominant \
         component, and at jitter 0 it is the zero vector"
    );
    (0..dim)
        .map(|i| {
            if i == axis {
                1.0
            } else {
                jitter * (((i * 7 + axis * 13) % 11) as f32 / 11.0)
            }
        })
        .collect()
}

/// `recall@k` = |returned ∩ expected| / |expected|, over point ids.
///
/// Takes the candidate set as an argument rather than calling `top_k` itself, so the
/// control below can feed it a deliberately damaged set.
fn recall_at_k(returned: &[String], expected: &BTreeSet<String>) -> f64 {
    if expected.is_empty() {
        return f64::NAN; // an empty expected set makes recall meaningless, not 1.0
    }
    let got: BTreeSet<&str> = returned.iter().map(|s| s.as_str()).collect();
    let hit = expected.iter().filter(|e| got.contains(e.as_str())).count();
    hit as f64 / expected.len() as f64
}

/// Dimension == population, so every planted point is a distinct near-one-hot vector and
/// the nearest neighbour of axis `a`'s query really is point `a`.
const DIM: usize = 64;

/// Fail loudly on a known-answer vector that cannot express its own answer.
fn assert_non_degenerate(v: &[f32], axis: usize) {
    let norm: f32 = v.iter().map(|x| x * x).sum::<f32>().sqrt();
    assert!(
        norm > 0.1,
        "planted vector for axis {axis} has norm {norm}: cosine against it is undefined \
         or unstable, so any ranking it produces is arbitrary"
    );
    let (best, _) = v
        .iter()
        .enumerate()
        .max_by(|a, b| a.1.partial_cmp(b.1).unwrap())
        .unwrap();
    assert_eq!(
        best, axis,
        "planted vector for axis {axis} is dominated by axis {best} instead — the \
         known-answer set does not know the answer it claims"
    );
}

#[test]
fn recall_at_k_is_one_by_construction_and_the_measure_can_detect_a_miss() {
    let (mut st, root) = store("recall");
    let collection = "playbook";

    // A known-answer set: one point per axis, so the nearest neighbour of axis-a's
    // query vector IS point-a. The answer is known before any search runs.
    const N: usize = DIM;
    for a in 0..N {
        let v = planted(a, DIM, 0.05);
        // The fixture is checked BEFORE it is used as ground truth.
        assert_non_degenerate(&v, a);
        st.upsert(collection, &format!("p{a}"), v).unwrap();
    }

    // For each of several queries, the expected top-1 is the matching planted point.
    let mut recalls = Vec::new();
    for a in [0usize, 7, 31, 63] {
        let q = planted(a, DIM, 0.0);
        assert_non_degenerate(&q, a);
        let hits = st.top_k(collection, &q, 1).unwrap();
        let returned: Vec<String> = hits.iter().map(|h| h.point_id.clone()).collect();
        let expected: BTreeSet<String> = [format!("p{a}")].into_iter().collect();
        let r = recall_at_k(&returned, &expected);
        recalls.push(r);
        assert_eq!(
            r, 1.0,
            "recall@1 for axis {a} was {r}, returned {returned:?}. The scan is exhaustive, \
             so anything below 1.0 is a defect in the scan or the fold — NOT an index \
             tradeoff, because there is no index."
        );
    }

    // recall@k for a larger k, where the expected set is the k nearest axes.
    let q = planted(10, DIM, 0.0);
    let hits = st.top_k(collection, &q, 5).unwrap();
    let returned: Vec<String> = hits.iter().map(|h| h.point_id.clone()).collect();
    assert_eq!(
        returned.len(),
        5,
        "top_k must return k when k <= live points"
    );
    assert_eq!(
        returned[0], "p10",
        "the planted nearest neighbour must rank first; got {returned:?}"
    );
    // Scores must be non-increasing — a ranking that is not ordered is not a ranking.
    let scores: Vec<f32> = hits.iter().map(|h| h.score).collect();
    for w in scores.windows(2) {
        assert!(
            w[0] >= w[1],
            "top_k returned scores out of order: {scores:?}"
        );
    }

    println!(
        "recall@1 over {} known-answer queries: {:?} (population {N} points, dim {DIM})",
        recalls.len(),
        recalls
    );

    // ---------------------------------------------------------------
    // THE CONTROL. A recall function that always returns 1.0 is
    // indistinguishable from a perfect search. Feed it a damaged
    // candidate set and require it to say so.
    // ---------------------------------------------------------------
    let expected3: BTreeSet<String> = ["p10", "p11", "p12"]
        .iter()
        .map(|s| s.to_string())
        .collect();
    let complete = vec!["p10".to_string(), "p11".to_string(), "p12".to_string()];
    let damaged = vec!["p10".to_string(), "p99".to_string()]; // one hit, one miss
    let r_complete = recall_at_k(&complete, &expected3);
    let r_damaged = recall_at_k(&damaged, &expected3);
    println!("control: complete set -> {r_complete:.3}, damaged set -> {r_damaged:.3}");
    assert_eq!(r_complete, 1.0, "a complete candidate set must score 1.0");
    assert!(
        (r_damaged - 1.0 / 3.0).abs() < 1e-9,
        "the damaged set must score 1/3, got {r_damaged} — a recall function that cannot \
         report less than 1.0 makes every 1.0 above meaningless"
    );
    assert!(
        r_damaged < r_complete,
        "the measure must separate a complete set from a damaged one"
    );

    // And an empty expected set must NOT read as success.
    assert!(
        recall_at_k(&complete, &BTreeSet::new()).is_nan(),
        "recall over an empty expected set must be undefined, not 1.0 — otherwise a \
         known-answer set that failed to load reports a perfect score"
    );

    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn an_exact_scan_means_recall_cannot_be_the_headline_number() {
    // This test states the structural fact in a form that fails if it ever stops being
    // true — which is exactly when a recall number starts being worth publishing.
    let (mut st, root) = store("exact");
    let collection = "mcp";
    const N: usize = 50;
    for a in 0..N {
        st.upsert(collection, &format!("q{a}"), planted(a % DIM, DIM, 0.3))
            .unwrap();
    }

    // Asking for more than exist returns everything that exists — an approximate index
    // would be free to return fewer. If this ever starts under-returning, the scan has
    // stopped being exhaustive and recall@k becomes a real measurement.
    let hits = st.top_k(collection, &planted(0, DIM, 0.0), N * 2).unwrap();
    assert_eq!(
        hits.len(),
        N,
        "an exhaustive scan must return every live point when k exceeds the population; \
         returning fewer would mean an approximation has been introduced, and THEN a \
         recall@k number becomes meaningful"
    );

    // Every live point must appear exactly once — set equality, not a count.
    let got: BTreeSet<String> = hits.iter().map(|h| h.point_id.clone()).collect();
    let want: BTreeSet<String> = (0..N).map(|a| format!("q{a}")).collect();
    assert_eq!(got, want, "the scan must cover the live set exactly");
    println!(
        "exhaustive: k={} over {N} live points returned {} distinct ids",
        N * 2,
        got.len()
    );
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn a_catalog_objects_identity_maps_onto_collection_and_point_id() {
    // P6's "embeddings attached to catalog objects", pinned as a convention so it cannot
    // drift: `collection` = the catalog `resource_type`, `point_id` = the object `path`.
    // No new dataset is required — `collection` is already both the partition and the
    // index dimension, and `point_id` is free-form.
    let (mut st, root) = store("catalog");

    let objects = [
        ("playbook", "system/auth"),
        ("playbook", "travel/hotel_cards"),
        ("playbook", "adiona/localize"),
        ("mcp", "firestore"),
        ("mcp", "duffel"),
    ];
    for (i, (rtype, path)) in objects.iter().enumerate() {
        st.upsert(rtype, path, planted(i, DIM, 0.1)).unwrap();
    }

    // A query against one resource_type must not see another's objects. This is the
    // property that makes `collection` the right place for the type: isolation is
    // structural, not filtered after the fact.
    let hits = st.top_k("playbook", &planted(0, DIM, 0.0), 10).unwrap();
    let got: BTreeSet<String> = hits.iter().map(|h| h.point_id.clone()).collect();
    let want: BTreeSet<String> = ["system/auth", "travel/hotel_cards", "adiona/localize"]
        .iter()
        .map(|s| s.to_string())
        .collect();
    assert_eq!(
        got, want,
        "a query on resource_type `playbook` must return exactly the playbook paths — \
         cross-type leakage would mean the type is not actually partitioning"
    );

    // ⚠ A path contains `/`. That is fine HERE because `point_id` is a payload field,
    // not a substrate key — unlike the runtime registry's id, whose charset is
    // deliberately narrow because it becomes a key. Pinned so the two are not conflated.
    assert!(
        hits.iter().any(|h| h.point_id.contains('/')),
        "a catalog path contains `/` and must round-trip as a point_id"
    );

    // The nearest neighbour of object 0's own vector is object 0.
    assert_eq!(
        hits[0].point_id,
        "system/auth",
        "a catalog object must be its own nearest neighbour; got {:?}",
        hits.iter().map(|h| &h.point_id).collect::<Vec<_>>()
    );

    // And the convention survives a delete: a retracted object leaves the collection.
    st.delete("playbook", "adiona/localize").unwrap();
    let after: BTreeSet<String> = st
        .top_k("playbook", &planted(0, DIM, 0.0), 10)
        .unwrap()
        .iter()
        .map(|h| h.point_id.clone())
        .collect();
    assert!(
        !after.contains("adiona/localize"),
        "a deleted catalog object must leave the similarity results"
    );
    assert_eq!(after.len(), 2, "the other two must remain; got {after:?}");

    println!(
        "catalog convention: (resource_type, path) -> (collection, point_id); \
         {} types, {} objects, isolation and delete both hold",
        2,
        objects.len()
    );
    let _ = std::fs::remove_dir_all(&root);
}
