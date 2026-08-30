//! **Failure domains for durable substrates** (noetl/ehdb#332, F5).
//!
//! ## The gap this closes
//!
//! [`LocalFsSubstrate`](crate::substrate::LocalFsSubstrate) is the only
//! *storage-backing* `DurableSubstrate` in the tree —
//! [`CountingSubstrate`](crate::substrate::CountingSubstrate) is a transparent
//! decorator over another, not an independent store. So "the durable substrate"
//! is a filesystem path, and whether that path is a *distinct failure domain* is
//! decided entirely by what is mounted there. **Nothing checked.**
//!
//! In production nothing was:
//!
//! ```text
//! NOETL_EVENT_BUS_WRITER_DIR   = /data/eventbus
//! NOETL_EHDB_TIER_SERVICE_DIR  = /data/eventbus/ehdb-tier
//! ```
//!
//! `/data/eventbus` is one PVC. The substrate copy lives in a **subdirectory of
//! the same volume as the part it is a copy of**, so replication buys no
//! independent failure domain: the upload turns an unsealed local part into a
//! sealed local part beside it.
//!
//! ⚠ This is not a claim that loss is likely — GCE persistent disks are
//! replicated within a zone by the platform. It is a claim that **the durability
//! story the code tells and the one the deployment implements are different**,
//! and only the deployment is load-bearing.
//!
//! ## What this module adds
//!
//! A substrate now *declares* its failure domain, and
//! [`validate_replica_domains`] refuses a replica set whose members share one.
//! An RF of N over one domain is an RF of 1 wearing a larger number.

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use ehdb_core::{EhdbError, Result};

/// Where a substrate's bytes physically live, at the granularity that fails
/// together.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum FailureDomain {
    /// A local block device, identified by its device id. Two substrates with
    /// the same id are **the same disk**, whatever their paths suggest.
    LocalDevice { device_id: u64, root: PathBuf },
    /// An external store addressed over a network — a genuinely separate domain
    /// from the writer's node.
    Remote { provider: String, bucket: String },
    /// Process memory. Never durable; useful only in tests.
    Ephemeral { instance: String },
    /// The substrate did not declare one.
    ///
    /// ⚠ Treated as **its own unique domain per call**, so an undeclared
    /// substrate is never *silently* assumed to be independent — it fails the
    /// nesting check below but cannot be proven distinct.
    Undeclared,
}

impl FailureDomain {
    /// The domain of a filesystem path: its device id where the OS reports one.
    ///
    /// ⚠ Device id, not the path. Two directories on one PVC have different
    /// paths and the same device — which is precisely the production case, and
    /// a path comparison would have called it independent.
    pub fn for_path(path: &Path) -> Self {
        let root = path.to_path_buf();
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            // Walk up to the nearest existing ancestor: the leaf may not exist
            // yet, but its device is the one that matters.
            let mut probe: Option<&Path> = Some(path);
            while let Some(p) = probe {
                if let Ok(md) = std::fs::metadata(p) {
                    return Self::LocalDevice {
                        device_id: md.dev(),
                        root,
                    };
                }
                probe = p.parent();
            }
        }
        Self::Undeclared
    }

    /// A stable, secret-free label for metrics and logs. **Never** includes a
    /// bucket path or a full filesystem path beyond its device.
    pub fn label(&self) -> String {
        match self {
            Self::LocalDevice { device_id, .. } => format!("local-device-{device_id}"),
            Self::Remote { provider, .. } => format!("remote-{provider}"),
            Self::Ephemeral { instance } => format!("ephemeral-{instance}"),
            Self::Undeclared => "undeclared".to_string(),
        }
    }

    /// Whether this domain can genuinely survive the loss of the writer's node.
    pub fn is_independent_of_node(&self) -> bool {
        matches!(self, Self::Remote { .. })
    }
}

/// Why a replica set was refused.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DomainViolation {
    /// Two replicas resolve to the same failure domain.
    SharedDomain {
        replica_a: String,
        replica_b: String,
        domain: String,
    },
    /// One replica's root is inside another's — the production shape, where a
    /// copy lives in a subdirectory of the thing it copies.
    NestedPath { inner: String, outer: String },
    /// A replica did not declare a domain, so independence cannot be shown.
    Undeclared { replica: String },
}

impl DomainViolation {
    pub fn message(&self) -> String {
        match self {
            Self::SharedDomain {
                replica_a,
                replica_b,
                domain,
            } => format!(
                "replicas '{replica_a}' and '{replica_b}' share failure domain {domain}: \
                 an RF of 2 over one domain is an RF of 1"
            ),
            Self::NestedPath { inner, outer } => format!(
                "replica '{inner}' is nested inside replica '{outer}': a copy stored \
                 under the thing it copies dies with it"
            ),
            Self::Undeclared { replica } => format!(
                "replica '{replica}' declares no failure domain, so independence \
                 cannot be demonstrated"
            ),
        }
    }
}

/// One replica's identity for validation: its name and declared domain, plus its
/// root path when it has one.
#[derive(Debug, Clone)]
pub struct ReplicaDomain {
    pub replica: String,
    pub domain: FailureDomain,
    pub root: Option<PathBuf>,
}

/// Refuse a replica set that does not actually spread risk.
///
/// ⚠ Returns **every** violation rather than the first. A set with three
/// problems should report three; fixing them one round-trip at a time is how a
/// misconfiguration survives review.
pub fn check_replica_domains(replicas: &[ReplicaDomain]) -> Vec<DomainViolation> {
    let mut out = Vec::new();

    let mut seen: HashMap<String, &str> = HashMap::new();
    for r in replicas {
        if matches!(r.domain, FailureDomain::Undeclared) {
            out.push(DomainViolation::Undeclared {
                replica: r.replica.clone(),
            });
            continue;
        }
        let label = r.domain.label();
        if let Some(prev) = seen.get(&label) {
            out.push(DomainViolation::SharedDomain {
                replica_a: (*prev).to_string(),
                replica_b: r.replica.clone(),
                domain: label,
            });
        } else {
            seen.insert(label, &r.replica);
        }
    }

    // Nesting is checked independently of device id: two paths can sit on
    // different devices and still be nested via a bind mount, and the nested
    // shape is wrong on its own.
    for a in replicas {
        for b in replicas {
            if a.replica == b.replica {
                continue;
            }
            if let (Some(ra), Some(rb)) = (&a.root, &b.root) {
                if ra != rb && ra.starts_with(rb) {
                    out.push(DomainViolation::NestedPath {
                        inner: a.replica.clone(),
                        outer: b.replica.clone(),
                    });
                }
            }
        }
    }
    out
}

/// [`check_replica_domains`], as a hard error.
pub fn validate_replica_domains(replicas: &[ReplicaDomain]) -> Result<()> {
    let violations = check_replica_domains(replicas);
    if violations.is_empty() {
        return Ok(());
    }
    let joined = violations
        .iter()
        .map(|v| v.message())
        .collect::<Vec<_>>()
        .join("; ");
    Err(EhdbError::InvalidState(format!(
        "replica set does not spread failure domains: {joined}"
    )))
}

/// Whether a replica set can survive losing the writer's node at all.
///
/// ⚠ Distinct from [`validate_replica_domains`]: two separate local disks on one
/// node are different domains and still both die with the node. Node-loss
/// survival needs at least one [`FailureDomain::Remote`].
pub fn survives_node_loss(replicas: &[ReplicaDomain]) -> bool {
    replicas.iter().any(|r| r.domain.is_independent_of_node())
}
