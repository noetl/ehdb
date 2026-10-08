//! **Secret REFERENCES — a pointer, never the material** (north-star P5).
//!
//! `agents/rules/execution-model.md` already settles the policy: a playbook references a
//! credential **by alias**, and the keychain resolves it at step-execution time. EHDB's job
//! is to hold the pointer so a catalog object can declare what it needs, and to **refuse
//! anything that looks like the value** — because a store that accepts material becomes a
//! second secret store, which is precisely what the alias indirection exists to prevent.
//!
//! `catalog-extract` already applies the same rule from the other side: it refuses to
//! catalogue a non-scalar `auth:` because an inline credential mapping would copy a secret
//! into a second store.

use std::fmt;

/// A pointer to a secret held somewhere else.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SecretRef {
    /// The resolving provider — `gsm`, `keychain`, `vault`, `aws`, …
    pub provider: String,
    /// The provider-relative path.
    pub path: String,
    /// An optional pinned version. `None` means "whatever the provider calls latest",
    /// which is a legitimate reference and not an omission.
    pub version: Option<String>,
}

/// Longest accepted reference. Bounded for the same reason a worker id is: it becomes a
/// stored field, and an unbounded one is an unbounded cost.
pub const MAX_SECRET_REF_LEN: usize = 512;

/// Markers that mean the caller passed **material** rather than a reference.
///
/// ⚠ This list is deliberately about SHAPE, not entropy. "Looks random" is unfalsifiable
/// and would reject legitimate opaque paths; "contains a PEM header", "has a password in
/// the userinfo of a URL", "has three base64 segments separated by dots" are decidable.
const MATERIAL_MARKERS: &[(&str, &str)] = &[
    ("-----BEGIN", "PEM key or certificate material"),
    ("PRIVATE KEY", "private key material"),
    ("\"password\"", "an inline credential mapping"),
    ("'password'", "an inline credential mapping"),
    ("\"secret\"", "an inline credential mapping"),
    ("\"token\"", "an inline credential mapping"),
];

impl SecretRef {
    /// Parse `provider://path[#version]`.
    ///
    /// ⚠ A bare alias with no scheme is **refused**. `adiona_actor` is ambiguous between a
    /// reference and a literal, and a store that guesses will eventually guess that a
    /// password is an alias. The scheme is what makes the intent explicit.
    pub fn parse(s: &str) -> Result<Self, String> {
        let s = s.trim();
        if s.is_empty() {
            return Err("a secret reference must not be empty".into());
        }
        if s.len() > MAX_SECRET_REF_LEN {
            return Err(format!(
                "a secret reference must be at most {MAX_SECRET_REF_LEN} bytes, got {}",
                s.len()
            ));
        }
        for (marker, what) in MATERIAL_MARKERS {
            if s.contains(marker) {
                return Err(format!(
                    "refusing {what}: a secret reference is a POINTER, and storing material \
                     would make this a second secret store"
                ));
            }
        }
        // A JWT: three dot-separated base64url segments. Checked before the scheme test,
        // because a JWT has no scheme and would otherwise be refused with a misleading
        // "no provider" message.
        let segs: Vec<&str> = s.split('.').collect();
        if segs.len() == 3
            && segs.iter().all(|p| {
                p.len() > 8
                    && p.chars()
                        .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
            })
        {
            return Err("refusing what looks like a JWT: that is material, not a reference".into());
        }
        let Some((provider, rest)) = s.split_once("://") else {
            return Err(format!(
                "a secret reference needs an explicit provider scheme — got {s:?}. A bare \
                 alias is ambiguous between a reference and a literal, and a store that \
                 guesses will eventually guess that a password is an alias."
            ));
        };
        if provider.is_empty() || !provider.chars().all(|c| c.is_ascii_lowercase() || c == '-') {
            return Err(format!(
                "provider must be lowercase ascii, got {provider:?}"
            ));
        }
        let (path, version) = match rest.split_once('#') {
            Some((p, v)) if !v.is_empty() => (p, Some(v.to_string())),
            Some((p, _)) => (p, None),
            None => (rest, None),
        };
        if path.is_empty() {
            return Err("a secret reference needs a path".into());
        }
        // userinfo credentials: `scheme://user:pass@host`
        if let Some((userinfo, _)) = path.split_once('@') {
            if userinfo.contains(':') {
                return Err(
                    "refusing a URL with credentials in its userinfo: that embeds material".into(),
                );
            }
        }
        Ok(Self {
            provider: provider.to_string(),
            path: path.to_string(),
            version,
        })
    }
}

impl fmt::Display for SecretRef {
    /// ⚠ Renders the POINTER only. There is nothing else to render — the type never holds
    /// the value — and this impl exists so that is true by construction rather than by
    /// discipline at every call site.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}://{}", self.provider, self.path)?;
        if let Some(v) = &self.version {
            write!(f, "#{v}")?;
        }
        Ok(())
    }
}
