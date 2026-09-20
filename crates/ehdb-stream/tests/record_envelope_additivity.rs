//! S0 — where additive growth is legal, and where it is a format break.
//!
//! ## The finding this file records
//!
//! The ai-meta S0 spec assumed the event record body would take new
//! `Option<T>` fields additively, and set out to prove it. **It does not.**
//! `StreamRecord` carries `#[serde(deny_unknown_fields)]`
//! (`ehdb-stream/src/lib.rs:156`), so a reader built before a field was added
//! **rejects** a record that carries it — it does not ignore it. Forward
//! compatibility at the envelope is not "lossy", it is an error.
//!
//! That is the right design for an envelope and it is left alone. It just means
//! the additive path is somewhere else, and the somewhere else is already there:
//! `StreamRecord.payload: Vec<u8>` is **opaque bytes**. The envelope's shape is
//! fixed; the payload's shape is the payload's business.
//!
//! So the rule every later SLM-context phase inherits:
//!
//! ⛔ **Never add a field to `StreamRecord`.** It is a format break, and
//!    `deny_unknown_fields` makes it a loud one on the read side.
//! ✅ **Add SLM context data inside `payload`**, as a versioned, tagged,
//!    `#[serde(default)]`-tolerant structure.
//!
//! These tests pin both halves, so a future session that reaches for the
//! envelope gets a failing test instead of an unreadable log.

use ehdb_stream::StreamRecord;

#[test]
fn envelope_rejects_unknown_fields_and_that_is_deliberate() {
    // Exactly the shape of StreamRecord, plus one field a "newer writer" added.
    let with_extra = r#"{
        "sequence": 1,
        "subject": "noetl.event",
        "payload": [1, 2, 3],
        "transaction_id": "00000000-0000-0000-0000-000000000000",
        "slm_context_ref": "sha256:deadbeef"
    }"#;

    let parsed = serde_json::from_str::<StreamRecord>(with_extra);
    assert!(
        parsed.is_err(),
        "StreamRecord is deny_unknown_fields: an added envelope field must be \
         REJECTED, not ignored. If this test starts passing, someone removed \
         the attribute and forward-compat for the envelope changed meaning."
    );
    let err = parsed.unwrap_err().to_string();
    assert!(
        err.contains("slm_context_ref") || err.contains("unknown field"),
        "the rejection must name the offending field; got: {err}"
    );
}

// --- the legal path: versioned payload structures -------------------------

// A payload as an OLD build sees it. No deny_unknown_fields; unknown keys are
// tolerated and dropped. This is the shape S1's event types must use.
#[derive(Debug, serde::Serialize, serde::Deserialize, PartialEq)]
struct TurnPayloadV1 {
    v: u32,
    kind: String,
}

// The same payload after a later phase adds two optional fields.
#[derive(Debug, serde::Serialize, serde::Deserialize, PartialEq)]
struct TurnPayloadV1Plus {
    v: u32,
    kind: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    prompt_digest: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    model_ref: Option<String>,
}

#[test]
fn payload_grows_forward_compatibly() {
    // NEW writer -> OLD reader. The old reader must tolerate the new keys.
    let new_written = serde_json::to_vec(&TurnPayloadV1Plus {
        v: 1,
        kind: "slm.turn.prompted".into(),
        prompt_digest: Some("sha256:ab".into()),
        model_ref: Some("gemma-4-31b-it".into()),
    })
    .expect("serialize new");

    let old_read: TurnPayloadV1 =
        serde_json::from_slice(&new_written).expect("an OLD reader must tolerate NEW payload keys");
    assert_eq!(old_read.v, 1);
    assert_eq!(old_read.kind, "slm.turn.prompted");
}

#[test]
fn payload_grows_backward_compatibly() {
    // OLD writer -> NEW reader. The new reader must default the absent fields.
    let old_written = serde_json::to_vec(&TurnPayloadV1 {
        v: 1,
        kind: "slm.turn.prompted".into(),
    })
    .expect("serialize old");

    let new_read: TurnPayloadV1Plus =
        serde_json::from_slice(&old_written).expect("a NEW reader must accept an OLD payload");
    assert_eq!(new_read.prompt_digest, None);
    assert_eq!(new_read.model_ref, None);
}

#[test]
fn absent_optional_fields_do_not_appear_on_the_wire() {
    // skip_serializing_if keeps the log lean: an unset field costs zero bytes,
    // which is what makes "additive" affordable on an append-only log.
    let bytes = serde_json::to_vec(&TurnPayloadV1Plus {
        v: 1,
        kind: "slm.turn.degraded".into(),
        prompt_digest: None,
        model_ref: None,
    })
    .expect("serialize");
    let text = String::from_utf8(bytes).expect("utf8");
    assert!(
        !text.contains("prompt_digest"),
        "absent field leaked: {text}"
    );
    assert!(!text.contains("model_ref"), "absent field leaked: {text}");
}
