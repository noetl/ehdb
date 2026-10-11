//! **The `LeaseStore` seam must be implementable from outside this workspace.**
//!
//! C2 places the Kubernetes adapter in `noetl/worker` so ehdb needs no HTTP or
//! `kube` client (`docs/spec/lease-election-k8s-binding.md`). That only works
//! if an external crate can actually write `impl LeaseStore for MyStore`.
//!
//! ⚠ It could not. `LeaseStore`'s methods return `ehdb_core::Result`, and
//! `ehdb-reference` imported that name with a **private** `use` — so the error
//! type was unnameable from a dependent crate and the trait was public in name
//! only.
//!
//! ⚠⚠ And the obvious workaround is a trap: a consumer adding its own
//! `ehdb-core` dependency works until the two versions diverge, at which point
//! `EhdbError` is two distinct types and the `impl` silently stops applying.
//! That is the same two-versions-in-one-graph shape that broke
//! `noetl/worker`'s release twice on `async-nats`. Hence the re-export, and
//! hence this guard.

use ehdb_reference::election::{LeaseRecord, LeaseStore};
use ehdb_reference::{EhdbError, Result};

/// Stands in for the out-of-workspace adapter. If this file compiles, an
/// external crate can name everything it needs through `ehdb_reference` alone.
struct ExternalStore;

impl LeaseStore for ExternalStore {
    fn read(&self, _name: &str) -> Result<Option<LeaseRecord>> {
        Ok(None)
    }
    fn create(&self, _name: &str, _record: &LeaseRecord) -> Result<bool> {
        Err(EhdbError::InvalidState("not a real store".into()))
    }
    fn compare_and_swap(
        &self,
        _name: &str,
        _expected_version: u64,
        _record: &LeaseRecord,
    ) -> Result<bool> {
        Ok(false)
    }
}

#[test]
fn an_external_crate_can_name_the_trait_and_its_error_type() {
    // The assertion is that this compiles at all; the call keeps it honest.
    let s = ExternalStore;
    assert!(s.read("x").unwrap().is_none());
    assert!(matches!(
        s.create("x", &LeaseRecord {
            holder: "h".into(),
            transitions: 0,
            renewed_at_millis: 0,
            duration_secs: 1,
            version: 0,
        }),
        Err(EhdbError::InvalidState(_))
    ));
}
