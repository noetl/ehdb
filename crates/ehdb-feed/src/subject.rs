//! **L1 T4 — the routing subject + subject-filter primitive (the NATS-subject
//! equivalent on the EHDB command path).**
//!
//! A command is routed by a hierarchical dot-delimited [`Subject`]
//! (`commands.<pool>.shard.<n>`), the honest analog of the NATS subject
//! `noetl.commands.<pool>.shard.<n>` (#166). A worker subscribes with a
//! [`SubjectFilter`] over its pool + the shard(s) it owns, and the coordinator
//! only ever hands it a command whose subject matches — so pool isolation and
//! #166 shard routing (and any future dimension, e.g. #116 affinity) all fall
//! out as **subject dimensions** of one mechanism. This is the G4 gap the RFC
//! flagged.
//!
//! Matching is NATS-style: `*` matches exactly one token, `>` matches the
//! remaining tokens (only valid as the last filter token). Literal tokens match
//! themselves. A worker can therefore never claim a command outside its
//! subscribed subjects — the isolation guarantee (noetl/ai-meta#194).

use std::sync::Arc;

/// Single-token wildcard (matches exactly one subject token).
pub const TOKEN_WILDCARD: &str = "*";
/// Multi-token tail wildcard (matches the remaining tokens; last filter token).
pub const TAIL_WILDCARD: &str = ">";

/// A concrete routing subject: an ordered list of dot-delimited tokens, e.g.
/// `commands.shared.shard.0`. Never contains wildcards.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Subject(Vec<String>);

impl Subject {
    /// The command-bus subject for `(pool, shard)`:
    /// `commands.<pool>.shard.<shard>`.
    pub fn command(pool: &str, shard: u32) -> Self {
        Subject(vec![
            "commands".to_string(),
            pool.to_string(),
            "shard".to_string(),
            shard.to_string(),
        ])
    }

    /// Parse a dotted subject string into tokens. Empty tokens are dropped so
    /// `commands..shard` never yields a blank token.
    pub fn parse(s: &str) -> Self {
        Subject(
            s.split('.')
                .filter(|t| !t.is_empty())
                .map(String::from)
                .collect(),
        )
    }

    /// The subject's tokens.
    pub fn tokens(&self) -> &[String] {
        &self.0
    }

    /// Render the subject back to its dotted string form.
    pub fn as_str(&self) -> String {
        self.0.join(".")
    }
}

impl std::fmt::Display for Subject {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.as_str())
    }
}

/// A subscription filter over subjects, with NATS wildcards (`*` one token,
/// `>` the tail). A worker subscribes with one; the coordinator scopes its
/// claims to matching subjects.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubjectFilter(Vec<String>);

impl SubjectFilter {
    /// Parse a dotted filter string (may contain `*` / `>`).
    pub fn parse(s: &str) -> Self {
        SubjectFilter(
            s.split('.')
                .filter(|t| !t.is_empty())
                .map(String::from)
                .collect(),
        )
    }

    /// A filter that matches every subject (`>`).
    pub fn all() -> Self {
        SubjectFilter(vec![TAIL_WILDCARD.to_string()])
    }

    /// Does this filter match `subject`? NATS token semantics:
    /// - `>` (last token) matches one-or-more remaining subject tokens;
    /// - `*` matches exactly one token;
    /// - a literal token matches itself;
    /// - lengths must otherwise line up exactly.
    pub fn matches(&self, subject: &Subject) -> bool {
        let f = &self.0;
        let s = &subject.0;
        let mut i = 0;
        while i < f.len() {
            if f[i] == TAIL_WILDCARD {
                // `>` must be the final filter token and needs ≥1 subject token
                // remaining (NATS: `a.>` does not match `a`).
                return i + 1 == f.len() && i < s.len();
            }
            if i >= s.len() {
                return false;
            }
            if f[i] != TOKEN_WILDCARD && f[i] != s[i] {
                return false;
            }
            i += 1;
        }
        i == s.len()
    }
}

impl std::fmt::Display for SubjectFilter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0.join("."))
    }
}

/// Extracts the routing [`Subject`] from a record — injected into the
/// coordinator so `ehdb-feed` stays dataset-agnostic. MUST be total (every
/// record maps to exactly one subject), so isolation holds. The D1 command-bus
/// route is [`crate::claim::d1_command_subject`].
pub type SubjectFn<R> = Arc<dyn Fn(&R) -> Subject + Send + Sync>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn literal_and_wildcards() {
        let s = Subject::command("shared", 0);
        assert_eq!(s.as_str(), "commands.shared.shard.0");

        // exact
        assert!(SubjectFilter::parse("commands.shared.shard.0").matches(&s));
        // single-token wildcard on the shard
        assert!(SubjectFilter::parse("commands.shared.shard.*").matches(&s));
        // tail wildcard
        assert!(SubjectFilter::parse("commands.shared.>").matches(&s));
        assert!(SubjectFilter::all().matches(&s));
        // pool mismatch — the isolation case
        assert!(!SubjectFilter::parse("commands.system.>").matches(&s));
        assert!(!SubjectFilter::parse("commands.system.shard.0").matches(&s));
        // wrong shard
        assert!(!SubjectFilter::parse("commands.shared.shard.1").matches(&s));
        // wildcard pool still isolates by shard
        assert!(SubjectFilter::parse("commands.*.shard.0").matches(&s));
        assert!(!SubjectFilter::parse("commands.*.shard.1").matches(&s));
    }

    #[test]
    fn tail_requires_a_remaining_token() {
        let s = Subject::parse("commands.shared");
        // `commands.shared.>` needs a token after `shared`
        assert!(!SubjectFilter::parse("commands.shared.>").matches(&s));
        assert!(SubjectFilter::parse("commands.>").matches(&s));
        // exact-length still matches
        assert!(SubjectFilter::parse("commands.shared").matches(&s));
        // longer subject than a non-tail filter does not match
        assert!(!SubjectFilter::parse("commands.shared").matches(&Subject::command("shared", 0)));
    }

    #[test]
    fn shard_dimension_isolation() {
        let s0 = Subject::command("shared", 0);
        let s1 = Subject::command("shared", 1);
        let f0 = SubjectFilter::parse("commands.shared.shard.0");
        assert!(f0.matches(&s0));
        assert!(!f0.matches(&s1));
    }
}
