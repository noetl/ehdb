//! Behavioural tests for the tiered mesh.
//!
//! Every one of these is deterministic: no clock, no network, no model.

use ehdb_signal_mesh::a2a::{Task, TaskState};
use ehdb_signal_mesh::event::*;
use ehdb_signal_mesh::fold::*;
use ehdb_signal_mesh::mesh::{Agent, Mesh};
use ehdb_signal_mesh::mesh_armed;
use ehdb_signal_mesh::react::DeterministicReasoner;

fn rec(seq: u64, stream: &str, payload: MeshEvent) -> Record {
    Record {
        seq,
        stream: stream.into(),
        payload,
    }
}

fn agg(agent: &str, value: f64, count: u32) -> MeshEvent {
    MeshEvent::AggregateEmitted(AggregateEmitted {
        agent_id: agent.into(),
        tier: 0,
        value,
        input_count: count,
        up_to_seq: 0,
    })
}

/// ⚠⚠ THE defect this suite exists to catch.
///
/// A weighted mean that drops its weights is a plain mean of means. The failure
/// is **invisible under equal weights** — which is the shape a tidy fixture
/// naturally has — so this test uses deliberately UNEQUAL populations, and the
/// second half proves the equal-weight case cannot discriminate.
#[test]
fn weighted_mean_respects_population_not_child_count() {
    let recs = vec![
        rec(1, "s", agg("a", 10.0, 1)),  // one device reading 10
        rec(2, "s", agg("b", 20.0, 99)), // ninety-nine reading 20
    ];
    let ctx = fold(&recs, "s", 2).expect("folds");
    let got = reduce(&ctx, Reduction::WeightedMean);

    // Correct: (10*1 + 20*99) / 100 = 19.9
    assert!(
        (got - 19.9).abs() < 1e-9,
        "weighted mean must weight by population, got {got}"
    );
    // A plain mean of means would be 15.0. Name it, so a future reader sees
    // exactly which wrong answer this guards against.
    assert!(
        (got - 15.0).abs() > 1e-9,
        "got the unweighted mean of means — the weights were discarded"
    );

    // The control that makes the fixture choice load-bearing: with equal
    // weights, weighted and unweighted agree, so an equal-weight fixture would
    // have passed against the broken implementation.
    let equal = vec![
        rec(1, "s", agg("a", 10.0, 3)),
        rec(2, "s", agg("b", 20.0, 3)),
    ];
    let ectx = fold(&equal, "s", 2).expect("folds");
    let w = reduce(&ectx, Reduction::WeightedMean);
    assert!(
        (w - 15.0).abs() < 1e-9,
        "equal weights collapse to the plain mean — this is why the fixture above is uneven"
    );
}

/// The population travelling upward is the summed population, not the number
/// of children.
#[test]
fn population_is_summed_not_counted() {
    let recs = vec![rec(1, "s", agg("a", 1.0, 4)), rec(2, "s", agg("b", 2.0, 6))];
    let ctx = fold(&recs, "s", 2).expect("folds");
    assert_eq!(population(&ctx), 10, "must sum child populations");
    assert_ne!(population(&ctx), 2, "must not be the child COUNT");
}

/// The bound is the whole determinism story.
#[test]
fn the_bounded_read_excludes_everything_after_the_watermark() {
    let recs = vec![
        rec(1, "s", agg("a", 1.0, 1)),
        rec(2, "s", agg("b", 2.0, 1)),
        rec(3, "s", agg("c", 3.0, 1)),
    ];
    let at2 = fold(&recs, "s", 2).expect("folds");
    assert_eq!(
        at2.inputs.len(),
        2,
        "seq 3 must be invisible at watermark 2"
    );
    assert_eq!(at2.folded_through, 2);
    assert!(at2.is_complete());

    // A watermark beyond the log is honest about the gap rather than silently
    // returning what exists.
    let ahead = fold(&recs, "s", 9).expect("folds");
    assert_eq!(ahead.folded_through, 3);
    assert_eq!(ahead.staleness(), 6, "the gap must be reported");
    assert!(!ahead.is_complete());
}

/// Unsorted input and a foreign stream are errors, not silent skips.
#[test]
fn the_fold_refuses_rather_than_guessing() {
    let unsorted = vec![rec(2, "s", agg("a", 1.0, 1)), rec(1, "s", agg("b", 2.0, 1))];
    assert!(matches!(
        fold(&unsorted, "s", 9),
        Err(FoldError::UnsortedInput { .. })
    ));

    let foreign = vec![
        rec(1, "s", agg("a", 1.0, 1)),
        rec(2, "other", agg("b", 2.0, 1)),
    ];
    assert!(matches!(
        fold(&foreign, "s", 9),
        Err(FoldError::ForeignStream { .. })
    ));

    // Two-sided: a well-formed prefix still folds, so the refusals above are
    // not a fold that refuses everything.
    let ok = vec![rec(1, "s", agg("a", 1.0, 1))];
    assert!(fold(&ok, "s", 9).is_ok());
}

/// An unrecognised kind must fold, not fail — rolling upgrades depend on it.
#[test]
fn an_unknown_event_kind_folds_instead_of_failing() {
    let raw = br#"{"kind":"mesh.some.future.kind","whatever":1}"#;
    let parsed = MeshEvent::from_payload(raw).expect("unknown kind still parses");
    assert_eq!(parsed, MeshEvent::Unknown);
    assert_eq!(parsed.kind(), None);

    let recs = vec![
        rec(1, "s", agg("a", 5.0, 1)),
        rec(2, "s", MeshEvent::Unknown),
    ];
    let ctx = fold(&recs, "s", 2).expect("an unknown kind must not break the fold");
    assert_eq!(ctx.inputs.len(), 1);
    assert_eq!(
        ctx.folded_through, 2,
        "the unknown record still advances the watermark"
    );
}

/// All eight A2A states, and terminal really is terminal.
#[test]
fn task_states_are_complete_and_terminal_is_enforced() {
    assert_eq!(TaskState::ALL.len(), 8, "A2A defines eight task states");
    let terminal: Vec<_> = TaskState::ALL.iter().filter(|s| s.is_terminal()).collect();
    assert_eq!(terminal.len(), 4, "completed/failed/canceled/rejected");
    let interrupted: Vec<_> = TaskState::ALL
        .iter()
        .filter(|s| s.is_interrupted())
        .collect();
    assert_eq!(interrupted.len(), 2, "input-required/auth-required");
    // ⚠ Interrupted must NOT be terminal — collapsing them is the common bug.
    for s in interrupted {
        assert!(!s.is_terminal(), "{} must be resumable", s.as_str());
    }

    let mut t = Task::submit("t1", "a", "b", 7);
    t.transition(TaskState::Working)
        .expect("submitted -> working");
    t.transition(TaskState::Completed)
        .expect("working -> completed");
    assert!(
        t.transition(TaskState::Working).is_err(),
        "terminal must refuse"
    );
}

/// The cascade is replayable: same log, same verdict.
#[test]
fn the_cascade_is_deterministic_and_replayable() {
    let build = || {
        vec![
            Agent {
                id: "t0-a".into(),
                tier: 0,
                how: Reduction::WeightedMean,
                children: vec![],
                signal_class: Some("temp".into()),
            },
            Agent {
                id: "t1".into(),
                tier: 1,
                how: Reduction::WeightedMean,
                children: vec!["t0-a".into()],
                signal_class: None,
            },
        ]
    };
    let run = || {
        let mut m = Mesh::new("s", build(), 10.0);
        m.observe_signal("d1", "temp", 12.0, 1);
        m.observe_signal("d2", "temp", 18.0, 2);
        let head = m.log.head();
        let v = m.cascade(head, &DeterministicReasoner).expect("cascade");
        (v, m.log.records.len())
    };
    let (a, na) = run();
    let (b, nb) = run();
    assert_eq!(a, b, "two runs of the same input must agree");
    assert_eq!(na, nb, "and append the same number of events");
    assert!((a.value - 15.0).abs() < 1e-9, "mean of 12 and 18");
    assert!(a.decision, "15 >= 10");
}

/// The cascade records its own provenance.
#[test]
fn every_decision_leaves_a_replayable_trace() {
    let agents = vec![Agent {
        id: "t0".into(),
        tier: 0,
        how: Reduction::Sum,
        children: vec![],
        signal_class: Some("c".into()),
    }];
    let mut m = Mesh::new("s", agents, 1.0);
    m.publish_cards();
    m.observe_signal("d1", "c", 4.0, 1);
    let head = m.log.head();
    m.cascade(head, &DeterministicReasoner).expect("cascade");

    let kinds: Vec<&str> = m
        .log
        .records
        .iter()
        .filter_map(|r| r.payload.kind())
        .collect();
    for required in [
        "mesh.agent.card_published",
        "mesh.signal.observed",
        "mesh.task.transitioned",
        "mesh.agent.reasoned",
        "mesh.aggregate.emitted",
        "mesh.verdict.synthesised",
    ] {
        assert!(
            kinds.contains(&required),
            "missing {required} from the trace"
        );
    }
    // Three ReAct phases per agent turn.
    let phases: Vec<String> = m
        .log
        .records
        .iter()
        .filter_map(|r| match &r.payload {
            MeshEvent::AgentReasoned(a) => Some(a.phase.clone()),
            _ => None,
        })
        .collect();
    assert_eq!(phases, vec!["observe", "reason", "act"]);
}

/// Off unless explicitly armed.
#[test]
fn the_mesh_is_off_unless_explicitly_armed() {
    assert!(!mesh_armed(None), "unset must not arm the mesh");
    assert!(mesh_armed(Some("true")));
    assert!(mesh_armed(Some("  true ")));
    for v in ["1", "yes", "TRUE", "on", "", "false"] {
        assert!(!mesh_armed(Some(v)), "{v:?} must not arm");
    }
}

/// The mesh speaks the platform's read-consistency vocabulary, and refuses
/// rather than approximating what it cannot evaluate.
///
/// ⚠ `ReadConsistency::Bounded` is in **milliseconds** and this POC's staleness
/// is a **sequence gap**. The conversion is an explicit argument precisely
/// because the units are not the same; a test that assumed they were would be
/// asserting a type error into correctness.
#[test]
fn freshness_uses_the_platforms_read_consistency() {
    use ehdb_core::plan::ReadConsistency;

    let recs = vec![rec(1, "s", agg("a", 1.0, 1))];
    // Requested watermark 5, log only reaches 1 => staleness 4.
    let stale = fold(&recs, "s", 5).expect("folds");
    assert_eq!(stale.staleness(), 4);

    // Strong admits only a complete read.
    assert!(matches!(
        admits(&stale, ReadConsistency::Strong, 1),
        Err(FreshnessRefusal::TooStale {
            staleness: 4,
            allowed: 0
        })
    ));
    let fresh = fold(&recs, "s", 1).expect("folds");
    assert!(
        admits(&fresh, ReadConsistency::Strong, 1).is_ok(),
        "complete read must pass"
    );

    // Bounded admits within budget and refuses beyond it — both sides, or a
    // policy that always refused would satisfy the negative case alone.
    let within = ReadConsistency::Bounded {
        max_staleness_millis: 4,
    };
    assert!(admits(&stale, within, 1).is_ok());
    let beyond = ReadConsistency::Bounded {
        max_staleness_millis: 3,
    };
    assert!(matches!(
        admits(&stale, beyond, 1),
        Err(FreshnessRefusal::TooStale { allowed: 3, .. })
    ));
    // The exchange rate is load-bearing: the same budget at a different rate
    // changes the verdict, which is why it is an argument and not a constant.
    assert!(admits(&stale, beyond, 2).is_ok(), "3ms x 2 seq/ms = 6 >= 4");

    // Exact needs a clock this POC does not have. Refused, not approximated.
    assert_eq!(
        admits(&stale, ReadConsistency::Exact { at_millis: 1 }, 1),
        Err(FreshnessRefusal::ExactUnsupported)
    );
}
