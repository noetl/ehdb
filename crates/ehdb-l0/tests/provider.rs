//! D10 proof — provider-fact fold (desired/observed carry-forward) /
//! get-latest-fact / drift-scan over immutable parts.

use std::sync::Arc;

use ehdb_l0::substrate::DurableSubstrate;
use ehdb_l0::{LocalFsSubstrate, ProviderStore, ReplicaTarget};

fn unique_dir(tag: &str) -> std::path::PathBuf {
    static N: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let n = N.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    std::env::temp_dir().join(format!("ehdb-l0-d10-{tag}-{}-{n}", std::process::id()))
}

fn targets(dirs: &[std::path::PathBuf]) -> Vec<ReplicaTarget> {
    dirs.iter()
        .enumerate()
        .map(|(i, d)| {
            let s: Arc<dyn DurableSubstrate> = Arc::new(LocalFsSubstrate::new(d).unwrap());
            ReplicaTarget::new(format!("replica-{i}"), s)
        })
        .collect()
}

#[test]
fn fold_get_latest_drift_scan_and_cold_load() {
    let obj = unique_dir("obj");
    let local = unique_dir("local");
    let cold_local = unique_dir("cold");
    let cfg = |root: &std::path::Path| ProviderStore::config(root).with_seal_max_records(8);

    let store: Arc<dyn DurableSubstrate> = Arc::new(LocalFsSubstrate::new(&obj).unwrap());
    let mut p = ProviderStore::open(cfg(&local), store).unwrap();

    // Plan sets desired for two resources in stack "prod".
    p.set_desired("prod", "gcp:bucket/data", "location=US;class=STANDARD")
        .unwrap();
    p.set_desired("prod", "gcp:sa/runner", "roles=run.invoker")
        .unwrap();
    // A resource in another stack is isolated.
    p.set_desired("dev", "gcp:bucket/scratch", "location=EU")
        .unwrap();

    // Before any refresh, desired != observed("") → everything is in drift.
    let pre = p.drift_scan("prod").unwrap();
    assert_eq!(
        pre.len(),
        2,
        "planned-but-unobserved resources read as drift"
    );

    // Refresh: observed carries desired forward and lands the observed state.
    p.set_observed("prod", "gcp:bucket/data", "location=US;class=STANDARD")
        .unwrap();
    p.set_observed("prod", "gcp:sa/runner", "roles=viewer") // diverged!
        .unwrap();

    // fold: the latest fact merges the separately-arrived desired + observed.
    let bucket = p
        .get_latest_fact("prod", "gcp:bucket/data")
        .unwrap()
        .unwrap();
    assert_eq!(bucket.desired, "location=US;class=STANDARD");
    assert_eq!(bucket.observed, "location=US;class=STANDARD");
    assert!(!bucket.in_drift(), "converged");
    let sa = p.get_latest_fact("prod", "gcp:sa/runner").unwrap().unwrap();
    assert!(
        sa.in_drift(),
        "roles diverged: run.invoker desired, viewer observed"
    );
    assert!(p.get_latest_fact("prod", "missing").unwrap().is_none());

    // drift-scan now returns only the diverged SA.
    let drift = p.drift_scan("prod").unwrap();
    assert_eq!(
        drift
            .iter()
            .map(|f| f.provider_urn.as_str())
            .collect::<Vec<_>>(),
        vec!["gcp:sa/runner"]
    );
    // Stacks are isolated: dev's drift is separate.
    assert_eq!(p.drift_scan("dev").unwrap().len(), 1);
    assert_eq!(p.list("prod").unwrap().len(), 2);

    // forget drops a resource out of drift + present list; get still resolves it.
    assert!(p.forget("prod", "gcp:sa/runner").unwrap());
    assert!(
        !p.forget("prod", "gcp:sa/runner").unwrap(),
        "already forgotten"
    );
    assert_eq!(
        p.drift_scan("prod").unwrap().len(),
        0,
        "forgotten drift gone"
    );
    assert_eq!(p.list("prod").unwrap().len(), 1);
    let gone = p.get_latest_fact("prod", "gcp:sa/runner").unwrap().unwrap();
    assert!(
        !gone.present,
        "resolves as not-present (deleted vs never-seen)"
    );

    // Flush + merge + cold-load: the folded facts survive.
    p.flush_and_wait().unwrap();
    let _ = p.run_pending_merges().unwrap();
    let cold_store: Arc<dyn DurableSubstrate> = Arc::new(LocalFsSubstrate::new(&obj).unwrap());
    let cold = ProviderStore::cold_load(cfg(&cold_local), cold_store).unwrap();
    let cb = cold
        .get_latest_fact("prod", "gcp:bucket/data")
        .unwrap()
        .unwrap();
    assert_eq!(cb.observed, "location=US;class=STANDARD");
    assert!(
        !cold
            .get_latest_fact("prod", "gcp:sa/runner")
            .unwrap()
            .unwrap()
            .present
    );
    assert_eq!(cold.list("prod").unwrap().len(), 1);
    assert_eq!(cold.drift_scan("prod").unwrap().len(), 0);

    drop(cold);
    drop(p);
    for d in [&obj, &local, &cold_local] {
        let _ = std::fs::remove_dir_all(d);
    }
}

#[test]
fn provider_facts_survive_a_dead_replica() {
    let dirs: Vec<_> = (0..3).map(|i| unique_dir(&format!("r{i}"))).collect();
    let local = unique_dir("nlocal");
    let cold_local = unique_dir("ncold");
    let cfg = |root: &std::path::Path| ProviderStore::config(root).with_seal_max_records(8);

    let mut p = ProviderStore::open_replicated(cfg(&local), targets(&dirs)).unwrap();
    for i in 0..12u64 {
        let urn = format!("gcp:res/{i}");
        p.set_desired("prod", &urn, format!("want-{i}")).unwrap();
        // Even indices converge; odd indices drift.
        let observed = if i % 2 == 0 {
            format!("want-{i}")
        } else {
            format!("got-{i}")
        };
        p.set_observed("prod", &urn, observed).unwrap();
    }
    p.flush_and_wait().unwrap();
    drop(p);

    std::fs::remove_dir_all(&dirs[0]).unwrap();
    let cold = ProviderStore::cold_load_replicated(cfg(&cold_local), targets(&dirs)).unwrap();
    assert_eq!(cold.list("prod").unwrap().len(), 12);
    assert_eq!(
        cold.drift_scan("prod").unwrap().len(),
        6,
        "the 6 odd resources drift"
    );
    assert_eq!(
        cold.get_latest_fact("prod", "gcp:res/7")
            .unwrap()
            .unwrap()
            .observed,
        "got-7"
    );
    assert!(cold.engine().metrics().snapshot().read_fallbacks > 0);

    drop(cold);
    for d in dirs.iter().chain([&local, &cold_local]) {
        let _ = std::fs::remove_dir_all(d);
    }
}
