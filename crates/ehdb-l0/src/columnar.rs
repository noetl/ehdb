//! **Columnar-per-field part codec** for the D1 event tier (RFC §2.1
//! "columnar on-disk"; §2.4 "generalize by data type — the big field isolated";
//! L0.4).
//!
//! VictoriaLogs stores each field in its own on-disk column and **isolates its
//! one big field (`_msg`) into dedicated files** so a query that filters on a
//! small field (a stream label) never pays to read the big message column. L0.4
//! brings that layout to D1: a columnar part lays the four D1 fields out as
//! separate column sections —
//!
//! ```text
//! [ header: record_count + per-column (offset,len) + execution bloom ]
//! [ column: global_sequence  (u64 delta-varint) ]
//! [ column: execution_id     (len-prefixed utf8) ]
//! [ column: transaction_id   (len-prefixed utf8) ]
//! [ column: payload          (len-prefixed utf8) ]   <- the BIG field, isolated last
//! ```
//!
//! — so a reader that only needs, say, the `execution_id`/`global_sequence`
//! columns (a "which executions / which sequences are in this part" scan)
//! **ranged-reads only those columns and never touches the payload column's
//! bytes**. That is the load-bearing L0.4 property, proven in [`tests`] and in
//! the crate's `columnar` integration test.
//!
//! This is a **codec** (bytes ⇆ records + single-column projection), additive to
//! the row-framed part format ([`crate::frame`]) the engine writes today; wiring
//! it in as the event-tier part encoding is the follow-on. It stays within the
//! fixed D1 schema (RFC §0.1) — four known columns, no arbitrary/dynamic columns.

use ehdb_core::{EhdbError, Result};

use crate::bloom::Bloom;
use crate::dataset::EventRecord;

/// Magic prefixing a columnar part, distinct from the row-frame magic.
const COLUMNAR_MAGIC: u32 = 0xE5DB_C010;
/// Codec version.
const COLUMNAR_VERSION: u16 = 1;

/// The four fixed D1 columns, in on-disk order. `Payload` is last and isolated
/// (VictoriaLogs `_msg`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Field {
    GlobalSequence,
    ExecutionId,
    TransactionId,
    Payload,
}

impl Field {
    fn index(self) -> usize {
        match self {
            Field::GlobalSequence => 0,
            Field::ExecutionId => 1,
            Field::TransactionId => 2,
            Field::Payload => 3,
        }
    }
}

const COLUMN_COUNT: usize = 4;

/// A `(offset, len)` byte span of one column within the encoded part.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ColumnSpan {
    pub offset: u64,
    pub len: u64,
}

/// The decoded fixed-size header of a columnar part: the record count, the four
/// column spans, and the per-part execution-id bloom (L0.2 reused).
#[derive(Debug, Clone)]
pub struct ColumnarHeader {
    pub record_count: u64,
    pub columns: [ColumnSpan; COLUMN_COUNT],
    pub execution_bloom: Bloom,
}

impl ColumnarHeader {
    /// The byte span of one column — what a projection ranged-reads. Reading a
    /// small column never requires the (isolated, last) payload column's bytes.
    pub fn column_span(&self, field: Field) -> ColumnSpan {
        self.columns[field.index()]
    }
}

// --- varint (LEB128, unsigned) ---

fn write_uvarint(out: &mut Vec<u8>, mut v: u64) {
    loop {
        let mut byte = (v & 0x7f) as u8;
        v >>= 7;
        if v != 0 {
            byte |= 0x80;
        }
        out.push(byte);
        if v == 0 {
            break;
        }
    }
}

fn read_uvarint(bytes: &[u8], pos: &mut usize) -> Result<u64> {
    let mut result: u64 = 0;
    let mut shift = 0u32;
    loop {
        let byte = *bytes
            .get(*pos)
            .ok_or_else(|| EhdbError::Storage("columnar: truncated varint".into()))?;
        *pos += 1;
        result |= ((byte & 0x7f) as u64) << shift;
        if byte & 0x80 == 0 {
            break;
        }
        shift += 7;
        if shift >= 64 {
            return Err(EhdbError::Storage("columnar: varint overflow".into()));
        }
    }
    Ok(result)
}

fn encode_seq_column(records: &[EventRecord]) -> Vec<u8> {
    // Delta-varint: sequences are ascending within a part, so deltas are small.
    let mut out = Vec::new();
    let mut prev = 0u64;
    for r in records {
        write_uvarint(&mut out, r.global_sequence - prev);
        prev = r.global_sequence;
    }
    out
}

fn decode_seq_column(bytes: &[u8], count: usize) -> Result<Vec<u64>> {
    let mut pos = 0usize;
    let mut out = Vec::with_capacity(count);
    let mut acc = 0u64;
    for _ in 0..count {
        acc += read_uvarint(bytes, &mut pos)?;
        out.push(acc);
    }
    Ok(out)
}

fn encode_str_column<'a>(values: impl Iterator<Item = &'a str>) -> Vec<u8> {
    let mut out = Vec::new();
    for v in values {
        write_uvarint(&mut out, v.len() as u64);
        out.extend_from_slice(v.as_bytes());
    }
    out
}

fn decode_str_column(bytes: &[u8], count: usize) -> Result<Vec<String>> {
    let mut pos = 0usize;
    let mut out = Vec::with_capacity(count);
    for _ in 0..count {
        let len = read_uvarint(bytes, &mut pos)? as usize;
        let end = pos + len;
        let slice = bytes
            .get(pos..end)
            .ok_or_else(|| EhdbError::Storage("columnar: truncated string column".into()))?;
        out.push(
            std::str::from_utf8(slice)
                .map_err(|err| EhdbError::Storage(format!("columnar: utf8: {err}")))?
                .to_string(),
        );
        pos = end;
    }
    Ok(out)
}

/// Fixed header byte length: magic(4) + version(2) + record_count(8) +
/// 4×(offset u64 + len u64) + bloom_len(4). The bloom bytes follow the fixed
/// part, before the columns.
const FIXED_HEADER_LEN: usize = 4 + 2 + 8 + COLUMN_COUNT * 16 + 4;

/// Encode records into a columnar part. Records should be ascending by
/// `global_sequence` (the seq column is delta-encoded). The payload column is
/// written last so it can be skipped by a small-field projection.
pub fn encode_columnar(records: &[EventRecord]) -> Result<Vec<u8>> {
    let count = records.len() as u64;

    let seq_col = encode_seq_column(records);
    let exec_col = encode_str_column(records.iter().map(|r| r.execution_id.as_str()));
    let txn_col = encode_str_column(records.iter().map(|r| r.transaction_id.as_str()));
    let payload_col = encode_str_column(records.iter().map(|r| r.payload.as_str()));

    let mut bloom = Bloom::for_expected(records.len());
    for r in records {
        bloom.insert(&r.execution_id);
    }
    let bloom_bytes = serde_json::to_vec(&bloom)
        .map_err(|err| EhdbError::Storage(format!("columnar: encode bloom: {err}")))?;

    // Columns begin right after the header (fixed part + bloom bytes).
    let columns_start = (FIXED_HEADER_LEN + bloom_bytes.len()) as u64;
    let seq_off = columns_start;
    let exec_off = seq_off + seq_col.len() as u64;
    let txn_off = exec_off + exec_col.len() as u64;
    let payload_off = txn_off + txn_col.len() as u64;
    let spans = [
        (seq_off, seq_col.len() as u64),
        (exec_off, exec_col.len() as u64),
        (txn_off, txn_col.len() as u64),
        (payload_off, payload_col.len() as u64),
    ];

    let mut out = Vec::with_capacity(payload_off as usize + payload_col.len());
    out.extend_from_slice(&COLUMNAR_MAGIC.to_le_bytes());
    out.extend_from_slice(&COLUMNAR_VERSION.to_le_bytes());
    out.extend_from_slice(&count.to_le_bytes());
    for (off, len) in spans {
        out.extend_from_slice(&off.to_le_bytes());
        out.extend_from_slice(&len.to_le_bytes());
    }
    out.extend_from_slice(&(bloom_bytes.len() as u32).to_le_bytes());
    out.extend_from_slice(&bloom_bytes);
    debug_assert_eq!(out.len() as u64, columns_start);
    out.extend_from_slice(&seq_col);
    out.extend_from_slice(&exec_col);
    out.extend_from_slice(&txn_col);
    out.extend_from_slice(&payload_col);
    Ok(out)
}

/// Decode just the header (record count + column spans + bloom) — small and at
/// the front, so a reader learns the layout without reading any column.
pub fn decode_header(bytes: &[u8]) -> Result<ColumnarHeader> {
    if bytes.len() < FIXED_HEADER_LEN {
        return Err(EhdbError::Storage("columnar: truncated header".into()));
    }
    let magic = u32::from_le_bytes(bytes[0..4].try_into().unwrap());
    if magic != COLUMNAR_MAGIC {
        return Err(EhdbError::Storage("columnar: bad magic".into()));
    }
    let version = u16::from_le_bytes(bytes[4..6].try_into().unwrap());
    if version != COLUMNAR_VERSION {
        return Err(EhdbError::Storage(format!(
            "columnar: unsupported version {version}"
        )));
    }
    let record_count = u64::from_le_bytes(bytes[6..14].try_into().unwrap());
    let mut columns = [ColumnSpan { offset: 0, len: 0 }; COLUMN_COUNT];
    let mut p = 14usize;
    for col in columns.iter_mut() {
        let offset = u64::from_le_bytes(bytes[p..p + 8].try_into().unwrap());
        let len = u64::from_le_bytes(bytes[p + 8..p + 16].try_into().unwrap());
        *col = ColumnSpan { offset, len };
        p += 16;
    }
    let bloom_len = u32::from_le_bytes(bytes[p..p + 4].try_into().unwrap()) as usize;
    p += 4;
    let bloom_bytes = bytes
        .get(p..p + bloom_len)
        .ok_or_else(|| EhdbError::Storage("columnar: truncated bloom".into()))?;
    let execution_bloom: Bloom = serde_json::from_slice(bloom_bytes)
        .map_err(|err| EhdbError::Storage(format!("columnar: decode bloom: {err}")))?;
    Ok(ColumnarHeader {
        record_count,
        columns,
        execution_bloom,
    })
}

/// Project one column's decoded values, reading only that column's bytes plus
/// the header. The returned enum carries the right value type per field.
pub fn project_column(bytes: &[u8], field: Field) -> Result<Column> {
    let header = decode_header(bytes)?;
    let span = header.column_span(field);
    let col = bytes
        .get(span.offset as usize..(span.offset + span.len) as usize)
        .ok_or_else(|| EhdbError::Storage("columnar: column span out of range".into()))?;
    let count = header.record_count as usize;
    match field {
        Field::GlobalSequence => Ok(Column::Sequences(decode_seq_column(col, count)?)),
        Field::ExecutionId | Field::TransactionId | Field::Payload => {
            Ok(Column::Strings(decode_str_column(col, count)?))
        }
    }
}

/// A projected column's values.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Column {
    Sequences(Vec<u64>),
    Strings(Vec<String>),
}

/// Decode a whole columnar part back into records (all four columns).
pub fn decode_columnar(bytes: &[u8]) -> Result<Vec<EventRecord>> {
    let header = decode_header(bytes)?;
    let count = header.record_count as usize;
    let read = |field: Field| -> Result<&[u8]> {
        let s = header.column_span(field);
        bytes
            .get(s.offset as usize..(s.offset + s.len) as usize)
            .ok_or_else(|| EhdbError::Storage("columnar: column span out of range".into()))
    };
    let seqs = decode_seq_column(read(Field::GlobalSequence)?, count)?;
    let execs = decode_str_column(read(Field::ExecutionId)?, count)?;
    let txns = decode_str_column(read(Field::TransactionId)?, count)?;
    let payloads = decode_str_column(read(Field::Payload)?, count)?;
    let mut out = Vec::with_capacity(count);
    for i in 0..count {
        out.push(EventRecord {
            global_sequence: seqs[i],
            execution_id: execs[i].clone(),
            transaction_id: txns[i].clone(),
            payload: payloads[i].clone(),
        });
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn recs(n: u64, payload_size: usize) -> Vec<EventRecord> {
        (1..=n)
            .map(|s| {
                EventRecord::new(
                    s,
                    format!("exec-{}", s % 3),
                    format!("txn-{s}"),
                    "x".repeat(payload_size),
                )
            })
            .collect()
    }

    #[test]
    fn round_trip_all_fields() {
        let records = recs(50, 20);
        let bytes = encode_columnar(&records).unwrap();
        let back = decode_columnar(&bytes).unwrap();
        assert_eq!(back, records);
    }

    #[test]
    fn payload_column_is_isolated_and_largest() {
        // Big payloads → the payload column dominates the part; the small columns
        // are a tiny fraction, and the payload column is LAST (isolated).
        let records = recs(100, 500);
        let bytes = encode_columnar(&records).unwrap();
        let h = decode_header(&bytes).unwrap();
        let payload = h.column_span(Field::Payload);
        let exec = h.column_span(Field::ExecutionId);
        let seq = h.column_span(Field::GlobalSequence);
        // Payload column is the last column in the file.
        assert!(payload.offset > exec.offset && payload.offset > seq.offset);
        // Payload column dwarfs the small columns.
        assert!(payload.len > 10 * (exec.len + seq.len));
    }

    #[test]
    fn projecting_a_small_field_reads_no_payload_bytes() {
        let records = recs(100, 500);
        let bytes = encode_columnar(&records).unwrap();
        let h = decode_header(&bytes).unwrap();
        let payload = h.column_span(Field::Payload);

        // Project execution_id: the bytes we must touch are [0, exec.end) — the
        // header + seq + exec columns — which ends at the payload column's start.
        // We never read into the payload column.
        let exec = h.column_span(Field::ExecutionId);
        let bytes_touched_end = exec.offset + exec.len;
        assert!(
            bytes_touched_end <= payload.offset,
            "reading execution_id must not require payload bytes"
        );

        let col = project_column(&bytes, Field::ExecutionId).unwrap();
        match col {
            Column::Strings(v) => {
                assert_eq!(v.len(), 100);
                assert_eq!(v[0], "exec-1"); // 1 % 3
            }
            _ => panic!("wrong column type"),
        }
    }

    #[test]
    fn sequence_column_projects_without_strings() {
        let records = recs(30, 10);
        let bytes = encode_columnar(&records).unwrap();
        let col = project_column(&bytes, Field::GlobalSequence).unwrap();
        match col {
            Column::Sequences(v) => assert_eq!(v, (1..=30).collect::<Vec<_>>()),
            _ => panic!("wrong column type"),
        }
    }

    #[test]
    fn header_bloom_prunes_absent_executions() {
        let records = recs(30, 10); // execs exec-0, exec-1, exec-2
        let bytes = encode_columnar(&records).unwrap();
        let h = decode_header(&bytes).unwrap();
        assert!(h.execution_bloom.maybe_contains("exec-1"));
        // An absent execution is (almost surely) pruned.
        assert!(!h.execution_bloom.maybe_contains("exec-999"));
    }

    #[test]
    fn empty_part_round_trips() {
        let records: Vec<EventRecord> = vec![];
        let bytes = encode_columnar(&records).unwrap();
        assert_eq!(decode_columnar(&bytes).unwrap(), records);
        assert_eq!(decode_header(&bytes).unwrap().record_count, 0);
    }
}
