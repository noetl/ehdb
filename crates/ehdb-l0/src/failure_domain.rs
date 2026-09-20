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

// ---------------------------------------------------------------------------
// Survival goals (multi-region spec M4) — region-aware placement.
//
// READ-SIDE / VALIDATION ONLY. Nothing here is called from the engine yet and
// nothing here touches the write path. The wiring — mapping the engine's
// replica set into `RegionPlacement`s at the existing `check_replica_domains`
// call site — is deliberately NOT done here: that call site lives in
// `engine.rs`, which this change does not own.
//
// ⚠ Why a parallel input type rather than a `region` field on `ReplicaDomain`:
// `ReplicaDomain` is built with a struct literal in `engine.rs`, so adding a
// field there would stop the crate compiling until that file changed. A
// separate type keeps this shippable on its own.
// ---------------------------------------------------------------------------

/// How much a replica set is required to survive.
///
/// ⚠ `Zone` is today's behaviour exactly — [`check_replica_domains`] unchanged.
/// It is the default so that adding this type changes nothing by itself.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SurvivalGoal {
    /// Survive losing one zone / disk / node. Distinct failure domains, which
    /// is what [`check_replica_domains`] already enforces.
    #[default]
    Zone,
    /// Survive losing a whole region. Requires copies in **different declared
    /// regions** — a property no device-id comparison can establish, because
    /// two disks in one region are two domains and one region.
    Region,
}

impl SurvivalGoal {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Zone => "zone",
            Self::Region => "region",
        }
    }

    /// Parse from configuration. An unrecognised value is **`Zone`**, matching
    /// the fail-safe precedent in `EventLogMode::from_env` ("an unknown driver
    /// never mirrors"): a typo must not silently widen what we claim to
    /// survive.
    pub fn parse(raw: &str) -> Self {
        match raw.trim().to_ascii_lowercase().as_str() {
            "region" => Self::Region,
            _ => Self::Zone,
        }
    }
}

/// A replica's declared region, for the [`SurvivalGoal::Region`] check.
///
/// Separate from [`ReplicaDomain`] on purpose (see the module note above). The
/// two are combined by the caller, not by this module.
#[derive(Debug, Clone)]
pub struct RegionPlacement {
    pub replica: String,
    /// The declared region. `None` means **undeclared**, which is refused under
    /// [`SurvivalGoal::Region`] — never assumed distinct. Same posture as
    /// [`FailureDomain::Undeclared`]: silence is not independence.
    pub region: Option<String>,
}

/// Why a replica set cannot meet a [`SurvivalGoal::Region`] goal.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RegionViolation {
    /// Two replicas sit in the same declared region.
    SharedRegion {
        replica_a: String,
        replica_b: String,
        region: String,
    },
    /// A replica declares no region, so cross-region spread cannot be shown.
    UndeclaredRegion { replica: String },
    /// Fewer than two replicas: a region goal is unmeetable by construction.
    NotEnoughReplicas { have: usize },
}

impl RegionViolation {
    pub fn message(&self) -> String {
        match self {
            Self::SharedRegion {
                replica_a,
                replica_b,
                region,
            } => format!(
                "replicas '{replica_a}' and '{replica_b}' are both in region '{region}': \
                 a region goal over one region is a zone goal wearing a larger name"
            ),
            Self::UndeclaredRegion { replica } => format!(
                "replica '{replica}' declares no region, so cross-region spread \
                 cannot be demonstrated"
            ),
            Self::NotEnoughReplicas { have } => {
                format!("a region survival goal needs at least 2 replicas, found {have}")
            }
        }
    }
}

/// Check a replica set against a survival goal.
///
/// Under [`SurvivalGoal::Zone`] this returns **empty, always** — the zone
/// property is [`check_replica_domains`]'s job and is not duplicated here. A
/// caller runs both.
///
/// ⚠ Returns **every** violation, not the first, for the same reason
/// [`check_replica_domains`] does: fixing a misconfiguration one round-trip at
/// a time is how the rest of it survives review.
pub fn check_region_survival(
    replicas: &[RegionPlacement],
    goal: SurvivalGoal,
) -> Vec<RegionViolation> {
    if goal == SurvivalGoal::Zone {
        return Vec::new();
    }
    let mut out = Vec::new();
    if replicas.len() < 2 {
        out.push(RegionViolation::NotEnoughReplicas {
            have: replicas.len(),
        });
        return out;
    }
    let mut seen: HashMap<String, &str> = HashMap::new();
    for r in replicas {
        let Some(region) = r.region.as_deref() else {
            out.push(RegionViolation::UndeclaredRegion {
                replica: r.replica.clone(),
            });
            continue;
        };
        if let Some(prev) = seen.get(region) {
            out.push(RegionViolation::SharedRegion {
                replica_a: (*prev).to_string(),
                replica_b: r.replica.clone(),
                region: region.to_string(),
            });
        } else {
            seen.insert(region.to_string(), &r.replica);
        }
    }
    out
}

/// [`check_region_survival`], as a hard error. The **enforce** half; callers
/// wanting shadow behaviour use `check_region_survival` and only count.
pub fn validate_region_survival(replicas: &[RegionPlacement], goal: SurvivalGoal) -> Result<()> {
    let violations = check_region_survival(replicas, goal);
    if violations.is_empty() {
        return Ok(());
    }
    let joined = violations
        .iter()
        .map(|v| v.message())
        .collect::<Vec<_>>()
        .join("; ");
    Err(EhdbError::InvalidState(format!(
        "replica set cannot meet survival goal '{}': {joined}",
        goal.as_str()
    )))
}
