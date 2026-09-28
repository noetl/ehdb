//! S1 — the event payloads are additive and forward-compatible.
//!
//! Pins the rule S0 established: growth happens inside the payload, and a build
//! that predates a field — or a whole event kind — still reads the log.

use ehdb_slm_context::event::{
    Body, DegradeReason, Fallback, ModelRef, ResultRefStr, Sampling, SlmContextEvent, TurnDegraded,
    TurnPrompted,
};
use ehdb_slm_context::SLM_CONTEXT_PAYLOAD_VERSION;

fn model() -> ModelRef {
    ModelRef {
        family: "gemma-4".into(),
        variant: "31b-it".into(),
        digest: None,
        server: Some("ollama".into()),
        api: Some("openai-chat".into()),
    }
}

#[test]
fn payload_version_is_one() {
    assert_eq!(SLM_CONTEXT_PAYLOAD_VERSION, 1);
}

#[test]
fn a_known_kind_round_trips() {
    let ev = SlmContextEvent::TurnPrompted(TurnPrompted {
        v: SLM_CONTEXT_PAYLOAD_VERSION,
        execution_id: "exec-1".into(),
        turn: 0,
        model: model(),
        prompt: Body::Inline("hello".into()),
        prompt_digest: "sha256:aa".into(),
        sampling: Some(Sampling {
            temperature: Some(0.0),
            top_p: None,
            seed: Some(7),
            max_tokens: None,
        }),
        source_event_ids: vec![1, 2, 3],
    });
    let bytes = serde_json::to_vec(&ev).expect("serialize");
    let back = SlmContextEvent::from_payload(&bytes).expect("parse");
    assert_eq!(back, ev);
    assert_eq!(back.kind(), Some("slm.turn.prompted"));
}

#[test]
fn an_unknown_kind_is_skipped_not_fatal() {
    // The property that lets an OLD build fold a NEWER log.
    let future = br#"{"kind":"slm.turn.reflected","v":1,"execution_id":"exec-1"}"#;
    let parsed = SlmContextEvent::from_payload(future).expect("unknown kind must NOT be an error");
    assert_eq!(parsed, SlmContextEvent::Unknown);
    assert_eq!(parsed.kind(), None);
}

#[test]
fn unknown_optional_fields_on_a_known_kind_are_tolerated() {
    // A NEWER writer adds a field to an existing kind; an OLD reader ignores it.
    let newer = br#"{
        "kind": "slm.turn.degraded",
        "v": 1,
        "execution_id": "exec-1",
        "turn": 2,
        "reason": "parse_failure",
        "fallback": "escalate",
        "reflection_score": 0.4
    }"#;
    let parsed = SlmContextEvent::from_payload(newer).expect("added field must be tolerated");
    match parsed {
        SlmContextEvent::TurnDegraded(TurnDegraded {
            reason,
            fallback,
            turn,
            ..
        }) => {
            assert_eq!(reason, DegradeReason::ParseFailure);
            assert_eq!(fallback, Fallback::Escalate);
            assert_eq!(turn, 2);
        }
        other => panic!("expected TurnDegraded, got {other:?}"),
    }
}

#[test]
fn absent_optionals_do_not_reach_the_wire() {
    let ev = SlmContextEvent::TurnPrompted(TurnPrompted {
        v: 1,
        execution_id: "exec-1".into(),
        turn: 0,
        model: ModelRef {
            family: "gemma-4".into(),
            variant: "e4b".into(),
            digest: None,
            server: None,
            api: None,
        },
        prompt: Body::Ref(ResultRefStr("noetl://blob/sha256:bb".into())),
        prompt_digest: "sha256:bb".into(),
        sampling: None,
        source_event_ids: vec![],
    });
    let value: serde_json::Value = serde_json::to_value(&ev).unwrap();
    let text = value.to_string();

    // Scoped to the `model` object: a bare substring check is a false positive
    // here, because "prompt_digest" contains "digest".
    let model_obj = value
        .get("model")
        .and_then(|m| m.as_object())
        .expect("model object");
    for absent in ["digest", "server", "api"] {
        assert!(
            !model_obj.contains_key(absent),
            "model.{absent} leaked: {text}"
        );
    }
    assert_eq!(
        model_obj.len(),
        2,
        "only family + variant should survive: {text}"
    );

    // Top-level optionals must vanish entirely, not serialise as null.
    for absent in ["sampling", "source_event_ids"] {
        assert!(
            value.get(absent).is_none(),
            "{absent} must be absent, not null: {text}"
        );
    }
}

#[test]
fn a_malformed_known_kind_is_an_error_not_a_silent_skip() {
    // Tolerating unknown kinds must not become tolerating broken ones.
    let broken = br#"{"kind":"slm.turn.completed","v":1}"#; // missing required fields
    assert!(
        SlmContextEvent::from_payload(broken).is_err(),
        "a recognised kind with a broken body must fail loudly"
    );
}
