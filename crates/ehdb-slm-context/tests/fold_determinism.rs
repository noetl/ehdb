//! S2 — the fold is deterministic, prefix-correct and execution-scoped.
//!
//! The determinism checks run 64 iterations, not one. A property that can fail
//! intermittently and is asserted once is a decorative check: it agrees with a
//! `HashMap` about half the time.

use ehdb_slm_context::event::{AdmitGate, Body, ModelRef, SlmContextEvent, TurnCompleted, TurnPrompted};
use ehdb_slm_context::fold::{fold, Budget, BudgetLimit, FoldError, CURRENT_FOLD_VERSION};

const ITERS: usize = 64;
const EXEC: &str = "exec-1";

fn payload(v: serde_json::Value) -> Vec<u8> {
    serde_json::to_vec(&v).unwrap()
}

fn prompted(exec: &str, turn: u32) -> Vec<u8> {
    serde_json::to_vec(&SlmContextEvent::TurnPrompted(TurnPrompted {
        v: 1,
        execution_id: exec.into(),
        turn,
        model: ModelRef {
            family: "gemma-4".into(),
            variant: "31b-it".into(),
            digest: None,
            server: None,
            api: None,
        },
        prompt: Body::Inline(format!("prompt {turn}")),
        prompt_digest: format!("sha256:p{turn}"),
        sampling: None,
        source_event_ids: vec![],
    }))
    .unwrap()
}

fn completed(exec: &str, turn: u32) -> Vec<u8> {
    serde_json::to_vec(&SlmContextEvent::TurnCompleted(TurnCompleted {
        v: 1,
        execution_id: exec.into(),
        turn,
        completion: Body::Inline(format!("completion {turn}")),
        completion_digest: format!("sha256:c{turn}"),
        prompt_tokens: None,
        completion_tokens: None,
        latency_ms: None,
        finish_reason: None,
    }))
    .unwrap()
}

fn admitted(exec: &str, digest: &str, depth: u32) -> Vec<u8> {
    payload(serde_json::json!({
        "kind": "slm.step.admitted", "v": 1, "execution_id": exec,
        "content_digest": digest, "validator_version": "parser-v1",
        "gate": "auto", "depth": depth
    }))
}

fn rejected(exec: &str, digest: &str, rule: &str) -> Vec<u8> {
    payload(serde_json::json!({
        "kind": "slm.step.rejected", "v": 1, "execution_id": exec,
        "content_digest": digest, "rule": rule
    }))
}

fn sample_log() -> Vec<(u64, Vec<u8>)> {
    vec![
        (10, prompted(EXEC, 0)),
        (11, completed(EXEC, 0)),
        (12, rejected(EXEC, "sha256:s1", "tool_kind_not_allowed")),
        (13, prompted(EXEC, 1)),
        (14, completed(EXEC, 1)),
        (15, admitted(EXEC, "sha256:s2", 0)),
        (16, payload(serde_json::json!({"kind":"slm.turn.reflected","v":1}))), // unknown
    ]
}

#[test]
fn same_prefix_same_context_across_many_runs() {
    let events = sample_log();
    let first = fold(EXEC, &events, u64::MAX, CURRENT_FOLD_VERSION, Budget::default())
        .expect("fold")
        .canonical_bytes();
    for i in 0..ITERS {
        let again = fold(EXEC, &events, u64::MAX, CURRENT_FOLD_VERSION, Budget::default())
            .expect("fold")
            .canonical_bytes();
        assert_eq!(first, again, "fold differed on iteration {i}");
    }
}

#[test]
fn empty_prefix_is_an_empty_context_not_an_error() {
    let ctx = fold(EXEC, &[], u64::MAX, CURRENT_FOLD_VERSION, Budget::default()).expect("fold");
    assert!(ctx.turns.is_empty());
    assert!(ctx.admitted.is_empty());
    assert!(ctx.rejected.is_empty());
    assert_eq!(ctx.skipped_unknown, 0);
    assert!(!ctx.budget.is_exhausted());
}

#[test]
fn up_to_seq_makes_it_a_prefix_fold() {
    let events = sample_log();
    let early = fold(EXEC, &events, 11, CURRENT_FOLD_VERSION, Budget::default()).expect("fold");
    assert_eq!(early.turns.len(), 1, "only turn 0 is within seq<=11");
    assert!(early.rejected.is_empty(), "the rejection at seq 12 is beyond the prefix");

    let all = fold(EXEC, &events, u64::MAX, CURRENT_FOLD_VERSION, Budget::default()).expect("fold");
    assert_eq!(all.turns.len(), 2);
    assert_eq!(all.rejected.len(), 1);
}

#[test]
fn rejections_are_retained_for_feedback() {
    let ctx = fold(EXEC, &sample_log(), u64::MAX, CURRENT_FOLD_VERSION, Budget::default())
        .expect("fold");
    assert_eq!(ctx.rejected.len(), 1);
    assert_eq!(ctx.rejected[0].rule, "tool_kind_not_allowed");
}

#[test]
fn unknown_kinds_are_counted_not_dropped_silently() {
    let ctx = fold(EXEC, &sample_log(), u64::MAX, CURRENT_FOLD_VERSION, Budget::default())
        .expect("fold");
    assert_eq!(ctx.skipped_unknown, 1, "the future event kind must be visible as skipped");
}

#[test]
fn turns_are_ordered_by_turn_number_not_arrival() {
    // Events deliberately out of turn order (but in sequence order).
    let events = vec![
        (1, prompted(EXEC, 2)),
        (2, prompted(EXEC, 0)),
        (3, prompted(EXEC, 1)),
    ];
    let ctx = fold(EXEC, &events, u64::MAX, CURRENT_FOLD_VERSION, Budget::default()).expect("fold");
    let order: Vec<u32> = ctx.turns.iter().map(|t| t.turn).collect();
    assert_eq!(order, vec![0, 1, 2]);
}

#[test]
fn unsorted_input_is_refused_not_silently_sorted() {
    let events = vec![(5, prompted(EXEC, 0)), (4, prompted(EXEC, 1))];
    match fold(EXEC, &events, u64::MAX, CURRENT_FOLD_VERSION, Budget::default()) {
        Err(FoldError::UnsortedInput { at, prev, got }) => {
            assert_eq!((at, prev, got), (1, 5, 4));
        }
        other => panic!("expected UnsortedInput, got {other:?}"),
    }
}

#[test]
fn a_foreign_execution_is_refused() {
    // C5: global_sequence is per-engine. Folding a neighbour's context silently
    // would be worse than refusing.
    let events = vec![(1, prompted(EXEC, 0)), (2, prompted("exec-2", 0))];
    match fold(EXEC, &events, u64::MAX, CURRENT_FOLD_VERSION, Budget::default()) {
        Err(FoldError::ForeignExecution { at, expected, got }) => {
            assert_eq!(at, 1);
            assert_eq!(expected, EXEC);
            assert_eq!(got, "exec-2");
        }
        other => panic!("expected ForeignExecution, got {other:?}"),
    }
}

#[test]
fn budget_exhausts_on_generated_steps() {
    let mut events = vec![];
    for i in 0..8u32 {
        events.push((i as u64 + 1, admitted(EXEC, &format!("sha256:{i}"), 0)));
    }
    let ctx = fold(EXEC, &events, u64::MAX, CURRENT_FOLD_VERSION, Budget::default()).expect("fold");
    assert_eq!(ctx.budget.steps_admitted, 8);
    assert_eq!(ctx.budget.exhausted, Some(BudgetLimit::GeneratedSteps));
}

#[test]
fn budget_exhausts_on_depth() {
    let events = vec![(1, admitted(EXEC, "sha256:a", 2))];
    let ctx = fold(EXEC, &events, u64::MAX, CURRENT_FOLD_VERSION, Budget::default()).expect("fold");
    assert_eq!(ctx.budget.max_depth_seen, 2);
    assert_eq!(ctx.budget.exhausted, Some(BudgetLimit::Depth));
}

#[test]
fn budget_exhausts_on_turns() {
    let mut events = vec![];
    for t in 0..16u32 {
        events.push((t as u64 + 1, prompted(EXEC, t)));
    }
    let ctx = fold(EXEC, &events, u64::MAX, CURRENT_FOLD_VERSION, Budget::default()).expect("fold");
    assert_eq!(ctx.budget.turns_used, 16);
    assert_eq!(ctx.budget.exhausted, Some(BudgetLimit::Turns));
}

#[test]
fn admitted_gate_and_depth_survive_the_fold() {
    let events = vec![(1, admitted(EXEC, "sha256:a", 1))];
    let ctx = fold(EXEC, &events, u64::MAX, CURRENT_FOLD_VERSION, Budget::default()).expect("fold");
    assert_eq!(ctx.admitted.len(), 1);
    assert_eq!(ctx.admitted[0].gate, AdmitGate::Auto);
    assert_eq!(ctx.admitted[0].depth, 1);
}

#[test]
fn the_fold_reads_no_clock() {
    // A time-dependent fold would differ across a slow run. Folding the same
    // input with a delay between must still match byte-for-byte.
    let events = sample_log();
    let a = fold(EXEC, &events, u64::MAX, CURRENT_FOLD_VERSION, Budget::default())
        .unwrap()
        .canonical_bytes();
    std::thread::sleep(std::time::Duration::from_millis(25));
    let b = fold(EXEC, &events, u64::MAX, CURRENT_FOLD_VERSION, Budget::default())
        .unwrap()
        .canonical_bytes();
    assert_eq!(a, b);
}
