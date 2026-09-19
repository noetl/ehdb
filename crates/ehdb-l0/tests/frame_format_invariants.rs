//! S0 — frame-format invariants, pinned against the literal on-disk spec.
//!
//! Spec (`ehdb-l0/src/frame.rs`): a frame is `magic(4) | body_len(4) | crc32(4)`
//! little-endian, followed by the body. Twelve bytes of header, fixed.
//!
//! ## Why this file exists
//!
//! ⚠ **The frame format is implemented TWICE and the two copies are not linked.**
//! `ehdb-l0::frame` has `pub const FRAME_HEADER_LEN: usize = 12` and
//! `pub const FRAME_MAGIC: u32 = 0xE5DB_0001`; `ehdb-reference::durable_eventlog`
//! declares its own **private** constants with the same values and builds the
//! same header by hand (`durable_eventlog.rs:841-843`). `ehdb-reference` does
//! **not** depend on `ehdb-l0` — verified from its `Cargo.toml` — so nothing in
//! the type system, and no test before this one, made the two agree.
//!
//! `fencing.rs`'s module doc calls the format *"shared byte-identically with
//! `durable_eventlog.rs`"*. That is true of the **values** today and false of the
//! **mechanism**: they are two independent literals. A change to one is silent in
//! the other, and the failure mode is unreadable segments, not a compile error.
//!
//! So each crate pins itself to the **same written-down literal** instead. This
//! file is `ehdb-l0`'s half; `durable_eventlog.rs`'s `frame_format_invariants`
//! unit module is the other. If either drifts, that crate's own test fails.
//!
//! This matters for the SLM-context work (ai-meta `specs/active/2026-09-19-slm-ehdb-context/S0`)
//! because every later phase assumes new context data is additive **inside the
//! body**, never a header change. These tests are what make that assumption
//! checkable rather than asserted.

use ehdb_l0::frame::{
    crc32, encode_frame, read_frame_at, FRAME_HEADER_LEN, FRAME_MAGIC, MAX_FRAME_BODY_BYTES,
};

/// The canonical header, built from the literal spec rather than from the
/// constants under test — otherwise the test would agree with any change.
fn canonical_header(body: &[u8]) -> [u8; 12] {
    let mut h = [0u8; 12];
    h[0..4].copy_from_slice(&0xE5DB_0001u32.to_le_bytes());
    h[4..8].copy_from_slice(&(body.len() as u32).to_le_bytes());
    h[8..12].copy_from_slice(&crc32(body).to_le_bytes());
    h
}

#[test]
fn header_length_is_twelve() {
    assert_eq!(
        FRAME_HEADER_LEN, 12,
        "C3: the frame header is a fixed 12 bytes shared with \
         ehdb-reference::durable_eventlog. Widening it makes every existing \
         segment unreadable. New fields go in the record BODY."
    );
}

#[test]
fn magic_is_the_literal() {
    assert_eq!(
        FRAME_MAGIC, 0xE5DB_0001,
        "the magic is duplicated in durable_eventlog.rs; both must stay this value"
    );
}

#[test]
fn encoded_frame_matches_the_canonical_layout() {
    for body in [b"".as_slice(), b"x".as_slice(), b"{\"a\":1}".as_slice()] {
        let framed = encode_frame(body).expect("encode");
        assert_eq!(
            framed.len(),
            12 + body.len(),
            "frame length must be exactly header + body"
        );
        assert_eq!(
            &framed[..12],
            &canonical_header(body),
            "header bytes must match the literal spec for body {body:?}"
        );
        assert_eq!(&framed[12..], body, "body must follow the header verbatim");
    }
}

#[test]
fn decoder_accepts_a_hand_built_canonical_frame() {
    let body = b"{\"slm\":\"payload\"}";
    let mut framed = canonical_header(body).to_vec();
    framed.extend_from_slice(body);

    let decoded = read_frame_at(&framed, 0)
        .expect("a canonical frame must decode")
        .expect("a complete frame must not read as a torn tail");
    assert_eq!(decoded.body, body);
    assert_eq!(decoded.frame_len, (12 + body.len()) as u64);
    assert_eq!(decoded.offset, 0);
}

#[test]
fn a_body_grown_by_an_additive_field_still_frames() {
    // The S0 claim in one test: growing the BODY is ordinary. A body is opaque
    // bytes to the frame layer, so an added payload field changes only body_len.
    let before = br#"{"kind":"slm.turn.prompted","v":1}"#.as_slice();
    let after = br#"{"kind":"slm.turn.prompted","v":1,"prompt_digest":"sha256:ab"}"#.as_slice();

    let f_before = encode_frame(before).expect("encode before");
    let f_after = encode_frame(after).expect("encode after");

    assert_eq!(f_before.len(), 12 + before.len());
    assert_eq!(f_after.len(), 12 + after.len());
    assert_eq!(
        &f_before[..4],
        &f_after[..4],
        "the magic does not move when the body grows"
    );

    for framed in [&f_before, &f_after] {
        let d = read_frame_at(framed, 0).expect("decode").expect("complete");
        assert_eq!(d.frame_len as usize, framed.len());
    }
}

#[test]
fn truncated_header_is_a_torn_tail_not_an_error() {
    // Guards the recovery contract the additive claim leans on: a short read at
    // EOF keeps the prefix rather than failing the whole segment.
    let body = b"abc";
    let mut framed = canonical_header(body).to_vec();
    framed.extend_from_slice(body);
    for cut in 1..12usize {
        let short = &framed[..cut];
        assert!(
            matches!(read_frame_at(short, 0), Ok(None)),
            "a {cut}-byte prefix must read as a torn tail"
        );
    }
}

#[test]
fn body_cap_is_enforced() {
    assert_eq!(MAX_FRAME_BODY_BYTES, 64 * 1024 * 1024);
    let too_big = vec![0u8; MAX_FRAME_BODY_BYTES + 1];
    assert!(
        encode_frame(&too_big).is_err(),
        "an over-cap body must be refused at encode, not truncated"
    );
}
