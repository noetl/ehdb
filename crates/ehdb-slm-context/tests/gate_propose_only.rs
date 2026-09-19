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
    let d = admit(&spec("python"), &empty_ctx(), &Policy::default(), &ok(), StepGenMode::Execute);
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
    let d = admit(&spec("python"), &empty_ctx(), &Policy::default(), &ok(), StepGenMode::Off);
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
    for kind in ["python", "http", "noop"] {
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
    let d = admit(&spec("python"), &empty_ctx(), &Policy::default(), &bad, StepGenMode::Propose);
    assert_eq!(d.rejection_rule(), Some(RejectionRule::SchemaInvalid));
}

#[test]
fn an_auth_block_is_refused_however_it_is_nested() {
    let nested = serde_json::json!({
        "step": "x",
        "tool": { "kind": "http", "request": { "auth": { "type": "bearer" } } }
    });
    let d = admit(&nested, &empty_ctx(), &Policy::default(), &ok(), StepGenMode::Propose);
    assert_eq!(d.rejection_rule(), Some(RejectionRule::CredentialReach));
}

#[test]
fn a_keychain_alias_off_the_allowlist_is_refused() {
    let s = serde_json::json!({ "step": "x", "tool": { "kind": "http" }, "credential": "pg_k8s" });
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
    let d = admit(&spec("python"), &ctx_with(spent), &Policy::default(), &ok(), StepGenMode::Propose);
    assert_eq!(d.rejection_rule(), Some(RejectionRule::BudgetExhausted));
}

#[test]
fn depth_exhaustion_reports_the_depth_rule() {
    let deep = Budget { max_depth_seen: 2, ..Budget::default() };
    let d = admit(&spec("python"), &ctx_with(deep), &Policy::default(), &ok(), StepGenMode::Propose);
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
    assert_eq!(before, 8, "ALL must stay exhaustive when a rule is added");
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

    let emitted: Vec<RejectionRule> = vec![
        admit(&serde_json::json!("x"), &ctx, &p, &ok(), StepGenMode::Propose),
        admit(&spec("python"), &ctx_with(spent), &p, &ok(), StepGenMode::Propose),
        admit(&spec("python"), &ctx_with(deep), &p, &ok(), StepGenMode::Propose),
        admit(&spec("python"), &ctx, &p, &StubValidator { accept: false }, StepGenMode::Propose),
        admit(
            &serde_json::json!({"step":"x","tool":{"kind":"http"},"auth":{}}),
            &ctx, &p, &ok(), StepGenMode::Propose,
        ),
        admit(
            &serde_json::json!({"step":"x","tool":{"kind":"http"},"credential":"nope"}),
            &ctx, &p, &ok(), StepGenMode::Propose,
        ),
        admit(&serde_json::json!({"step":"x"}), &ctx, &p, &ok(), StepGenMode::Propose),
        admit(&spec("postgres"), &ctx, &p_off, &ok(), StepGenMode::Propose),
    ]
    .into_iter()
    .filter_map(|d| d.rejection_rule())
    .collect();

    assert_eq!(emitted.len(), 8, "one rejection per case: {emitted:?}");
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
fn the_default_allowlist_is_the_approved_three() {
    assert_eq!(Policy::default().allowed_tool_kinds, vec!["python", "http", "noop"]);
    assert!(Policy::default().allowed_keychain_aliases.is_empty());
}
