//! **Cross-region replica target configuration** (multi-region spec M7).
//!
//! ## ⛔ SCAFFOLD — parsing only, no replication
//!
//! This parses the declarative target list and validates it. It does **not**
//! open a substrate, copy a part, or touch the engine. M7 is gated behind M4
//! and M6, and ultimately behind M5.
//!
//! ⚠ Parsing is included rather than stubbed because a config format that is
//! only exercised on the day it is switched on is a config format that fails
//! on the day it is switched on. The spec's own M7 proof warns that a
//! recorded `ReplicaLocation` with no bytes behind it is the
//! representation-drift shape — the same applies one level up to a target
//! that parses into something nobody validated.

/// Env var carrying the declarative target list.
pub const REPLICA_TARGETS_ENV: &str = "NOETL_EHDB_REPLICA_TARGETS";

/// One declared replica target.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicaTargetSpec {
    pub id: String,
    pub region: Option<String>,
    pub zone: Option<String>,
    pub uri: Option<String>,
}

/// Why a target list was rejected.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TargetParseError {
    EmptyId { entry: String },
    DuplicateId { id: String },
    UnknownKey { key: String, entry: String },
    MissingId { entry: String },
}

impl TargetParseError {
    pub fn message(&self) -> String {
        match self {
            Self::EmptyId { entry } => format!("target '{entry}' has an empty id"),
            Self::DuplicateId { id } => {
                format!("target id '{id}' appears twice: replica ids must be unique")
            }
            Self::UnknownKey { key, entry } => format!(
                "target '{entry}' carries unknown key '{key}': refused rather than \
                 ignored, because a silently-dropped key is a setting the operator \
                 believes is applied"
            ),
            Self::MissingId { entry } => format!("target '{entry}' declares no id"),
        }
    }
}

/// Parse `id=a,region=r,zone=z,uri=u;id=b,...`.
///
/// An empty or unset value yields an empty list, which the caller reads as
/// "single local replica, today".
///
/// ⚠ An unknown key is an **error**, not a shrug. A dropped key is a setting
/// the operator believes is applied — the class of defect this program keeps
/// finding, one config layer up.
pub fn parse_targets(raw: Option<&str>) -> Result<Vec<ReplicaTargetSpec>, TargetParseError> {
    let Some(raw) = raw else {
        return Ok(Vec::new());
    };
    let raw = raw.trim();
    if raw.is_empty() {
        return Ok(Vec::new());
    }
    let mut out: Vec<ReplicaTargetSpec> = Vec::new();
    for entry in raw.split(';').map(str::trim).filter(|e| !e.is_empty()) {
        let mut spec = ReplicaTargetSpec {
            id: String::new(),
            region: None,
            zone: None,
            uri: None,
        };
        let mut saw_id = false;
        for pair in entry.split(',').map(str::trim).filter(|p| !p.is_empty()) {
            let (k, v) = pair.split_once('=').unwrap_or((pair, ""));
            let v = v.trim().to_string();
            match k.trim().to_ascii_lowercase().as_str() {
                "id" => {
                    saw_id = true;
                    spec.id = v;
                }
                "region" => spec.region = (!v.is_empty()).then_some(v),
                "zone" => spec.zone = (!v.is_empty()).then_some(v),
                "uri" => spec.uri = (!v.is_empty()).then_some(v),
                other => {
                    return Err(TargetParseError::UnknownKey {
                        key: other.to_string(),
                        entry: entry.to_string(),
                    })
                }
            }
        }
        if !saw_id {
            return Err(TargetParseError::MissingId {
                entry: entry.to_string(),
            });
        }
        if spec.id.is_empty() {
            return Err(TargetParseError::EmptyId {
                entry: entry.to_string(),
            });
        }
        if out.iter().any(|t| t.id == spec.id) {
            return Err(TargetParseError::DuplicateId { id: spec.id });
        }
        out.push(spec);
    }
    Ok(out)
}

pub fn targets_from_env() -> Result<Vec<ReplicaTargetSpec>, TargetParseError> {
    parse_targets(std::env::var(REPLICA_TARGETS_ENV).ok().as_deref())
}
