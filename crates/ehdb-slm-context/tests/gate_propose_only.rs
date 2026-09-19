//! S3 — the admission gate, propose-only.
//!
//! The headline assertions: exactly one outcome per proposal, every rejection
//! carries a countable rule, and **nothing executes** even in `Execute` mode.

use ehdb_slm_context::event::AdmitGate;
use ehdb_slm_context::fold::{fold, Budget, CURRENT_FOLD_VERSION};
use ehdb_slm_context::gate::{
    admit, content_digest, Decision, DslValidator, HumanGate, Policy, RejectionRule, StepGenMode,
};
use ehdb_slm_context::WorkingContext;

const EXEC: &str = "exec-1";

/// Stands in for `noetl-server`'s parser. The real one is injected in wiring;
/// this crate must never grow its own DSL opinion.
struct StubValidator {
    accept: bool,
}
impl DslValidator for StubValidator {
    fn validate(&self, spec: &serde_json::Value) -> Result<(), String> {
        if !self.accept {
            return Err("stub: rejected".into());
        }
        if spec.get("step").is_none() {
            return Err("stub: a step must have a `step` label".into());
        }
        Ok(())
    }
    fn version(&self) -> String {
        "stub-v1".into()
    }
}

fn ok() -> StubValidator {
    StubValidator { accept: true }
}

fn empty_ctx() -> WorkingContext {
    fold(EXEC, &[], u64::MAX, CURRENT_FOLD_VERSION, Budget::default()).expect("fold")
}

fn ctx_with(budget: Budget) -> WorkingContext {
    fold(EXEC, &[], u64::MAX, CURRENT_FOLD_VERSION, budget).expect("fold")
}

fn spec(kind: &str) -> serde_json::Value {
    serde_json::json!({ "step": "generated", "tool": { "kind": kind } })
}

// --- the headline guarantees ---------------------------------------------

#[test]
fn nothing_executes_even_in_execute_mode() {
    // ⛔ The owner gate. `Execute` parses and behaves as `Propose` because this
    // crate contains no execution path at all.
    let d = admit(&spec("noop"), &empty_ctx(), &Policy::default(), &ok(), StepGenMode::Execute);
    match d {
        Decision::Admitted(a) => assert!(
            !a.executed,
            "propose-only: `executed` must be false in every build that has no execution path"
        ),
        other => panic!("expected Admitted, got {other:?}"),
    }
}

#[test]
fn off_considers_nothing() {
    let d = admit(&spec("noop"), &empty_ctx(), &Policy::default(), &ok(), StepGenMode::Off);
    assert_eq!(d, Decision::NotConsidered);
}

#[test]
fn exactly_one_outcome_per_proposal() {
    // A2: never both, never neither. Exercised across every gate.
    let ctx = empty_ctx();
    let p = Policy::default();
    let cases: Vec<serde_json::Value> = vec![
        spec("python"),
        spec("postgres"),
        serde_json::json!("not an object"),
        serde_json::json!({ "step": "x" }),
        serde_json::json!({ "step": "x", "tool": { "kind": "http" }, "auth": { "type": "gcp" } }),
    ];
    for case in cases {
        let d = admit(&case, &ctx, &p, &ok(), StepGenMode::Propose);
        let n = [
            matches!(d, Decision::NotConsidered),
            matches!(d, Decision::Rejected { .. }),
            matches!(d, Decision::AwaitingApproval { .. }),
            matches!(d, Decision::Admitted(_)),
        ]
        .iter()
        .filter(|b| **b)
        .count();
        assert_eq!(n, 1, "exactly one outcome for {case}, got {d:?}");
    }
}

// --- the gates ------------------------------------------------------------

#[test]
fn an_allowed_kind_is_admitted() {
    for kind in ["noop"] {
        let d = admit(&spec(kind), &empty_ctx(), &Policy::default(), &ok(), StepGenMode::Propose);
        match d {
            Decision::Admitted(a) => {
                assert_eq!(a.gate, AdmitGate::Auto);
                assert_eq!(a.validator_version, "stub-v1");
                assert!(a.carrier.path.starts_with("generated/slm/"));
                assert!(a.carrier.content_digest.starts_with("sha256:"));
            }
            other => panic!("{kind} should admit, got {other:?}"),
        }
    }
}

#[test]
fn a_side_effectful_kind_awaits_a_human_and_is_not_admitted() {
    for kind in ["postgres", "shell", "provider", "container", "playbook", "transfer"] {
        let d = admit(&spec(kind), &empty_ctx(), &Policy::default(), &ok(), StepGenMode::Propose);
        assert!(!d.is_admitted(), "{kind} must not be admitted");
        match &d {
            Decision::AwaitingApproval { tool_kind, .. } => assert_eq!(tool_kind, kind),
            other => panic!("{kind} must await approval, got {other:?}"),
        }
    }
}

#[test]
fn with_the_human_gate_off_a_disallowed_kind_is_rejected_not_admitted() {
    let p = Policy { human_gate: HumanGate::Off, ..Policy::default() };
    let d = admit(&spec("postgres"), &empty_ctx(), &p, &ok(), StepGenMode::Propose);
    assert_eq!(d.rejection_rule(), Some(RejectionRule::ToolKindNotAllowed));
}

#[test]
fn a_schema_failure_is_rejected_with_the_schema_rule() {
    let bad = StubValidator { accept: false };
    let d = admit(&spec("noop"), &empty_ctx(), &Policy::default(), &bad, StepGenMode::Propose);
    assert_eq!(d.rejection_rule(), Some(RejectionRule::SchemaInvalid));
}

#[test]
fn an_auth_block_is_refused_however_it_is_nested() {
    let nested = serde_json::json!({
        "step": "x",
        "tool": { "kind": "noop", "request": { "auth": { "type": "bearer" } } }
    });
    let d = admit(&nested, &empty_ctx(), &Policy::default(), &ok(), StepGenMode::Propose);
    assert_eq!(d.rejection_rule(), Some(RejectionRule::CredentialReach));
}

#[test]
fn a_keychain_alias_off_the_allowlist_is_refused() {
    let s = serde_json::json!({ "step": "x", "tool": { "kind": "noop" }, "credential": "pg_k8s" });
    let d = admit(&s, &empty_ctx(), &Policy::default(), &ok(), StepGenMode::Propose);
    assert_eq!(d.rejection_rule(), Some(RejectionRule::KeychainAliasNotAllowed));

    let p = Policy { allowed_keychain_aliases: vec!["pg_k8s".into()], ..Policy::default() };
    let d2 = admit(&s, &empty_ctx(), &p, &ok(), StepGenMode::Propose);
    assert!(d2.is_admitted(), "an allowlisted alias may pass: {d2:?}");
}

#[test]
fn a_missing_tool_kind_is_rejected() {
    let d = admit(
        &serde_json::json!({ "step": "x" }),
        &empty_ctx(),
        &Policy::default(),
        &ok(),
        StepGenMode::Propose,
    );
    assert_eq!(d.rejection_rule(), Some(RejectionRule::MissingToolKind));
}

#[test]
fn a_non_object_proposal_is_malformed() {
    for v in [serde_json::json!("string"), serde_json::json!([1, 2]), serde_json::json!(7)] {
        let d = admit(&v, &empty_ctx(), &Policy::default(), &ok(), StepGenMode::Propose);
        assert_eq!(d.rejection_rule(), Some(RejectionRule::Malformed), "for {v}");
    }
}

#[test]
fn an_exhausted_budget_refuses_before_validating() {
    let spent = Budget { steps_admitted: 8, ..Budget::default() };
    let d = admit(&spec("noop"), &ctx_with(spent), &Policy::default(), &ok(), StepGenMode::Propose);
    assert_eq!(d.rejection_rule(), Some(RejectionRule::BudgetExhausted));
}

#[test]
fn depth_exhaustion_reports_the_depth_rule() {
    let deep = Budget { max_depth_seen: 2, ..Budget::default() };
    let d = admit(&spec("noop"), &ctx_with(deep), &Policy::default(), &ok(), StepGenMode::Propose);
    assert_eq!(d.rejection_rule(), Some(RejectionRule::DepthExceeded));
}

// --- the instrument -------------------------------------------------------

#[test]
fn every_rejection_rule_is_enumerable_and_labelled() {
    // ⭐ A consumer pins a counter at 0 for each of these. A set that omits one
    // value reintroduces the absent-series bug on exactly that value while the
    // rest read 0 and look complete.
    let mut labels: Vec<&str> = RejectionRule::ALL.iter().map(|r| r.label()).collect();
    let before = labels.len();
    labels.sort_unstable();
    labels.dedup();
    assert_eq!(before, labels.len(), "duplicate labels: {labels:?}");
    assert_eq!(before, 10, "ALL must stay exhaustive when a rule is added");
}

#[test]
fn all_covers_every_rule_the_gate_can_actually_emit() {
    // Guards the other direction: a rule the gate emits but ALL omits would be
    // an unpinned series, invisible until it fired.
    let ctx = empty_ctx();
    let p = Policy::default();
    let spent = Budget { steps_admitted: 8, ..Budget::default() };
    let deep = Budget { max_depth_seen: 2, ..Budget::default() };
    let p_off = Policy { human_gate: HumanGate::Off, ..Policy::default() };
    let p_http = Policy { allowed_tool_kinds: vec!["http".into()], ..Policy::default() };

    let emitted: Vec<RejectionRule> = vec![
        admit(&serde_json::json!("x"), &ctx, &p, &ok(), StepGenMode::Propose),
        admit(&spec("noop"), &ctx_with(spent), &p, &ok(), StepGenMode::Propose),
        admit(&spec("noop"), &ctx_with(deep), &p, &ok(), StepGenMode::Propose),
        admit(&spec("noop"), &ctx, &p, &StubValidator { accept: false }, StepGenMode::Propose),
        admit(
            &serde_json::json!({"step":"x","tool":{"kind":"noop"},"auth":{}}),
            &ctx, &p, &ok(), StepGenMode::Propose,
        ),
        admit(
            &serde_json::json!({"step":"x","tool":{"kind":"noop"},"credential":"nope"}),
            &ctx, &p, &ok(), StepGenMode::Propose,
        ),
        admit(&serde_json::json!({"step":"x"}), &ctx, &p, &ok(), StepGenMode::Propose),
        admit(&spec("python"), &ctx, &p, &ok(), StepGenMode::Propose),
        admit(&spec("http"), &ctx, &p_http, &ok(), StepGenMode::Propose),
        admit(&spec("postgres"), &ctx, &p_off, &ok(), StepGenMode::Propose),
    ]
    .into_iter()
    .filter_map(|d| d.rejection_rule())
    .collect();

    assert_eq!(emitted.len(), 10, "one rejection per case: {emitted:?}");
    for rule in &emitted {
        assert!(RejectionRule::ALL.contains(rule), "{rule:?} is emitted but missing from ALL");
    }
}

// --- the carrier (F2) -----------------------------------------------------

#[test]
fn the_digest_is_content_addressed_and_key_order_independent() {
    let a = serde_json::json!({ "step": "x", "tool": { "kind": "noop" } });
    let b = serde_json::json!({ "tool": { "kind": "noop" }, "step": "x" });
    assert_eq!(content_digest(&a), content_digest(&b), "key order must not change the digest");

    let c = serde_json::json!({ "step": "y", "tool": { "kind": "noop" } });
    assert_ne!(content_digest(&a), content_digest(&c));
}

#[test]
fn the_digest_format_matches_ehdb_storage_object_digest() {
    // Pinned to the standard NIST vector for sha256("abc") so this crate and
    // ehdb-storage's ObjectDigest cannot drift apart silently.
    let d = content_digest(&serde_json::Value::String("abc".into()));
    assert!(d.starts_with("sha256:"), "format must match ObjectDigest: {d}");
    // serde_json renders the string WITH quotes, so pin the shape not the value.
    assert_eq!(d.len(), "sha256:".len() + 64, "hex sha256 is 64 chars: {d}");
    assert!(d[7..].chars().all(|c| c.is_ascii_hexdigit() && !c.is_uppercase()));
}

#[test]
fn the_carrier_path_is_namespaced_for_bulk_rollback() {
    // F2: a dedicated prefix is what makes generated entries identifiable and
    // reversible by soft delete.
    let d = admit(&spec("noop"), &empty_ctx(), &Policy::default(), &ok(), StepGenMode::Propose);
    match d {
        Decision::Admitted(a) => {
            assert!(a.carrier.path.starts_with("generated/slm/"), "{}", a.carrier.path);
            assert!(a.carrier.path.len() > "generated/slm/".len());
        }
        other => panic!("expected Admitted, got {other:?}"),
    }
}

// --- flag parsing ---------------------------------------------------------

#[test]
fn stepgen_defaults_off_and_unknown_values_are_off() {
    assert_eq!(StepGenMode::default(), StepGenMode::Off);
    for raw in ["", "yes", "enabled", "propose!", "1", "true"] {
        assert_eq!(StepGenMode::parse(raw), StepGenMode::Off, "{raw:?} must not arm the gate");
    }
    assert_eq!(StepGenMode::parse("propose"), StepGenMode::Propose);
    assert_eq!(StepGenMode::parse("on"), StepGenMode::Execute);
}

#[test]
fn the_human_gate_defaults_to_required() {
    assert_eq!(HumanGate::default(), HumanGate::Required);
    for raw in ["", "required", "yes", "anything"] {
        assert_eq!(HumanGate::parse(raw), HumanGate::Required, "{raw:?} must not disarm the gate");
    }
    assert_eq!(HumanGate::parse("off"), HumanGate::Off);
}

#[test]
fn the_default_allowlist_is_noop_only() {
    assert_eq!(
        Policy::default().allowed_tool_kinds,
        vec!["noop"],
        "owner decision 2026-09-19: no python, and http is not cleanly constrainable"
    );
    assert_eq!(Policy::default().denied_tool_kinds, vec!["python"]);
    assert!(Policy::default().http_allowed_hosts.is_empty());
    assert!(Policy::default().allowed_keychain_aliases.is_empty());
}

// --- fork F4 as decided by the owner, 2026-09-19 --------------------------

#[test]
fn python_is_denied_and_cannot_be_approved() {
    // ⛔ Not merely off the allowlist: a denied kind is terminal, so it never
    // reaches the human gate. "Not allowlisted" would leave it one approval
    // click away from running.
    let d = admit(&spec("python"), &empty_ctx(), &Policy::default(), &ok(), StepGenMode::Propose);
    assert_eq!(d.rejection_rule(), Some(RejectionRule::ToolKindDenied));
    assert!(!matches!(d, Decision::AwaitingApproval { .. }), "python must not be approvable");
    assert!(!d.is_admitted());
}

#[test]
fn deny_beats_allow_even_if_an_operator_allowlists_python() {
    let p = Policy {
        allowed_tool_kinds: vec!["python".into(), "noop".into()],
        ..Policy::default()
    };
    let d = admit(&spec("python"), &empty_ctx(), &p, &ok(), StepGenMode::Propose);
    assert_eq!(
        d.rejection_rule(),
        Some(RejectionRule::ToolKindDenied),
        "the deny list must win over an allowlist entry"
    );
}

#[test]
fn http_is_not_on_the_default_allowlist() {
    // Not constrainable: the URL permits exfiltration and SSRF regardless of
    // method, and GET-safety is a server-side convention.
    assert!(!Policy::default().allowed_tool_kinds.iter().any(|k| k == "http"));
    let d = admit(&spec("http"), &empty_ctx(), &Policy::default(), &ok(), StepGenMode::Propose);
    assert!(!d.is_admitted(), "http must not admit by default: {d:?}");
}

#[test]
fn an_opted_in_http_still_fails_without_a_host_allowlist() {
    // Defence in depth: even an explicit opt-in cannot pass while the host
    // allowlist is empty, which is the default.
    let p = Policy { allowed_tool_kinds: vec!["http".into()], ..Policy::default() };
    let s = serde_json::json!({ "step": "x", "tool": { "kind": "http", "url": "https://example.com/x" } });
    let d = admit(&s, &empty_ctx(), &p, &ok(), StepGenMode::Propose);
    assert_eq!(d.rejection_rule(), Some(RejectionRule::HttpNotReadShaped));

    // Pin the DETAIL, not just the rule. Without this the empty-allowlist early
    // return is an equivalent mutant: removing it still rejects, because an
    // empty allowlist fails the membership check anyway. The message is the
    // operator-facing explanation and is worth distinguishing.
    match d {
        Decision::Rejected { detail, .. } => assert!(
            detail.contains("NOETL_SLM_HTTP_ALLOWED_HOSTS is empty"),
            "the empty-allowlist case must say so explicitly; got: {detail}"
        ),
        other => panic!("expected Rejected, got {other:?}"),
    }
}

#[test]
fn an_opted_in_http_refuses_non_read_shapes() {
    let p = Policy {
        allowed_tool_kinds: vec!["http".into()],
        http_allowed_hosts: vec!["example.com".into()],
        ..Policy::default()
    };
    let cases = vec![
        serde_json::json!({"step":"x","tool":{"kind":"http","url":"https://example.com/x","method":"POST"}}),
        serde_json::json!({"step":"x","tool":{"kind":"http","url":"https://example.com/x","body":{"a":1}}}),
        serde_json::json!({"step":"x","tool":{"kind":"http","url":"https://example.com/x","json":{"a":1}}}),
        serde_json::json!({"step":"x","tool":{"kind":"http","url":"https://example.com/x","form":{"a":"1"}}}),
        serde_json::json!({"step":"x","tool":{"kind":"http","url":"https://evil.test/x"}}),
        serde_json::json!({"step":"x","tool":{"kind":"http","url":"http://169.254.169.254/latest/meta-data"}}),
    ];
    for c in cases {
        let d = admit(&c, &empty_ctx(), &p, &ok(), StepGenMode::Propose);
        assert_eq!(d.rejection_rule(), Some(RejectionRule::HttpNotReadShaped), "for {c}");
    }
}

#[test]
fn an_opted_in_get_to_an_allowlisted_host_can_pass() {
    // The positive control. Without it, the previous test could be passing
    // because http is rejected unconditionally rather than by read-shape.
    let p = Policy {
        allowed_tool_kinds: vec!["http".into()],
        http_allowed_hosts: vec!["example.com".into()],
        ..Policy::default()
    };
    let s = serde_json::json!({"step":"x","tool":{"kind":"http","url":"https://example.com/x","method":"GET"}});
    let d = admit(&s, &empty_ctx(), &p, &ok(), StepGenMode::Propose);
    assert!(d.is_admitted(), "an explicitly opted-in, host-allowlisted GET should pass: {d:?}");
}
