//! Immutable-part record codec — **byte-identical to the #254 durable segment
//! frame** (`crates/ehdb-reference/src/durable_eventlog.rs`).
//!
//! A frame is `magic(4 LE) + body_len(4 LE) + crc32(4 LE) + body`, `body` =
//! `serde_json` of the record. Reusing this exact format (rather than inventing
//! a new one) means an L0 part **is** a #254 segment: the same torn-tail
//! recovery, the same CRC bit-rot detection, and the same on-disk bytes a #254
//! cold-load already knows how to read. The [`tests`] carry a
//! byte-for-byte-identical assertion against the #254 header layout + a shared
//! CRC known-answer vector so the two can never silently diverge.
//!
//! What L0 adds on top (in [`crate::catalog`]) is the ClickHouse-style *sparse
//! index over granules of frames* + the *manifest of parts* — the pruning
//! catalog #254's per-record offset index is not (RFC §2.5, §3).

use ehdb_core::{EhdbError, Result};

/// Frame magic — the #254 `FRAME_MAGIC` sentinel, so a mid-file byte that is
/// present but wrong classifies as corruption rather than a torn tail.
pub const FRAME_MAGIC: u32 = 0xE5DB_0001;
/// Fixed frame header length: `magic(4) + body_len(4) + crc32(4)`.
pub const FRAME_HEADER_LEN: usize = 12;
/// Sanity ceiling on a single frame body (#254 `MAX_FRAME_BODY_BYTES`) — guards
/// decode against a corrupt length header demanding an absurd allocation.
pub const MAX_FRAME_BODY_BYTES: usize = 64 * 1024 * 1024;

/// CRC-32 (IEEE, reflected, `0xEDB88320`) — byte-identical to the #254
/// `crc32` (same init/xor-out, same polynomial). A shared known-answer vector in
/// [`tests`] pins it to #254's `crc32_matches_known_vector`.
pub fn crc32(data: &[u8]) -> u32 {
    let mut crc: u32 = 0xFFFF_FFFF;
    for &byte in data {
        crc ^= byte as u32;
        for _ in 0..8 {
            let mask = (crc & 1).wrapping_neg();
            crc = (crc >> 1) ^ (0xEDB8_8320 & mask);
        }
    }
    !crc
}

/// Encode one record body into a framed byte vector (`header + body`). The body
/// is the caller's already-serialized record bytes (JSON).
pub fn encode_frame(body: &[u8]) -> Result<Vec<u8>> {
    if body.len() > MAX_FRAME_BODY_BYTES {
        return Err(EhdbError::InvalidState(format!(
            "l0 frame body {} exceeds cap {MAX_FRAME_BODY_BYTES}",
            body.len()
        )));
    }
    let mut out = Vec::with_capacity(FRAME_HEADER_LEN + body.len());
    out.extend_from_slice(&FRAME_MAGIC.to_le_bytes());
    out.extend_from_slice(&(body.len() as u32).to_le_bytes());
    out.extend_from_slice(&crc32(body).to_le_bytes());
    out.extend_from_slice(body);
    Ok(out)
}

/// A decoded frame's location + body slice within a part buffer.
#[derive(Debug, Clone, Copy)]
pub struct DecodedFrame<'a> {
    /// Byte offset of this frame's magic within the part buffer (its "mark").
    pub offset: u64,
    /// The frame's body bytes (the serialized record).
    pub body: &'a [u8],
    /// Total on-disk length of this frame (`FRAME_HEADER_LEN + body.len()`).
    pub frame_len: u64,
}

/// Read one frame starting at `offset` in `bytes`.
///
/// Returns `Ok(Some(frame))` for an intact frame, `Ok(None)` for a torn tail (a
/// truncated header/body at EOF — the #254 recovery contract: keep the prefix),
/// and `Err` for a *complete* frame with bad magic or a CRC mismatch (bit-rot,
/// never silently repaired — matches #254).
pub fn read_frame_at(bytes: &[u8], offset: u64) -> Result<Option<DecodedFrame<'_>>> {
    let start = offset as usize;
    // A truncated header at EOF is a torn tail — stop, keep the prefix.
    if start + FRAME_HEADER_LEN > bytes.len() {
        return Ok(None);
    }
    let magic = u32::from_le_bytes(bytes[start..start + 4].try_into().unwrap());
    let body_len = u32::from_le_bytes(bytes[start + 4..start + 8].try_into().unwrap()) as usize;
    let crc = u32::from_le_bytes(bytes[start + 8..start + 12].try_into().unwrap());
    if magic != FRAME_MAGIC {
        return Err(EhdbError::Storage(format!(
            "l0 part: bad frame magic at offset {offset}"
        )));
    }
    if body_len > MAX_FRAME_BODY_BYTES {
        return Err(EhdbError::Storage(format!(
            "l0 part: frame body {body_len} exceeds cap at offset {offset}"
        )));
    }
    let body_start = start + FRAME_HEADER_LEN;
    let body_end = body_start + body_len;
    // A truncated body at EOF is a torn tail — stop, keep the prefix.
    if body_end > bytes.len() {
        return Ok(None);
    }
    let body = &bytes[body_start..body_end];
    if crc32(body) != crc {
        return Err(EhdbError::Storage(format!(
            "l0 part: frame CRC mismatch at offset {offset}"
        )));
    }
    Ok(Some(DecodedFrame {
        offset,
        body,
        frame_len: (FRAME_HEADER_LEN + body_len) as u64,
    }))
}

/// Iterate every intact frame in a part buffer, from `start_offset`. Stops at a
/// torn tail (returns the intact prefix's frames); surfaces bit-rot as an error.
pub fn iter_frames_from(bytes: &[u8], start_offset: u64) -> Result<Vec<DecodedFrame<'_>>> {
    let mut frames = Vec::new();
    let mut offset = start_offset;
    while let Some(frame) = read_frame_at(bytes, offset)? {
        offset = frame.offset + frame.frame_len;
        frames.push(frame);
    }
    Ok(frames)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The shared CRC known-answer vector: `crc32(b"123456789") == 0xCBF43926`
    /// (the standard CRC-32/ISO-HDLC check value). #254's
    /// `crc32_matches_known_vector` pins the identical value — the two codecs
    /// agree bit-for-bit.
    #[test]
    fn crc32_matches_known_vector() {
        assert_eq!(crc32(b"123456789"), 0xCBF4_3926);
    }

    /// The header layout is byte-identical to #254: `magic(4 LE) + len(4 LE) +
    /// crc(4 LE) + body`, with the exact `0xE5DB0001` magic.
    #[test]
    fn frame_header_layout_is_254_identical() {
        let body = b"hello";
        let framed = encode_frame(body).unwrap();
        assert_eq!(&framed[0..4], &0xE5DB_0001u32.to_le_bytes());
        assert_eq!(&framed[4..8], &(body.len() as u32).to_le_bytes());
        assert_eq!(&framed[8..12], &crc32(body).to_le_bytes());
        assert_eq!(&framed[12..], body);
    }

    #[test]
    fn round_trip_frames() {
        let mut buf = Vec::new();
        for i in 0..5u32 {
            buf.extend_from_slice(&encode_frame(format!("body-{i}").as_bytes()).unwrap());
        }
        let frames = iter_frames_from(&buf, 0).unwrap();
        assert_eq!(frames.len(), 5);
        for (i, f) in frames.iter().enumerate() {
            assert_eq!(f.body, format!("body-{i}").as_bytes());
        }
    }

    #[test]
    fn torn_tail_keeps_prefix() {
        let mut buf = encode_frame(b"one").unwrap();
        buf.extend_from_slice(&encode_frame(b"two").unwrap());
        // Truncate mid-second-frame body.
        buf.truncate(buf.len() - 2);
        let frames = iter_frames_from(&buf, 0).unwrap();
        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0].body, b"one");
    }

    #[test]
    fn bad_magic_is_error_not_torn_tail() {
        let mut buf = encode_frame(b"one").unwrap();
        buf[0] ^= 0xFF; // corrupt the magic
        assert!(read_frame_at(&buf, 0).is_err());
    }

    #[test]
    fn crc_mismatch_is_error() {
        let mut buf = encode_frame(b"one").unwrap();
        let last = buf.len() - 1;
        buf[last] ^= 0xFF; // corrupt the body without fixing the CRC
        assert!(read_frame_at(&buf, 0).is_err());
    }
}
