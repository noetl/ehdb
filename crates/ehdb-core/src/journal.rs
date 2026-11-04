//! Reading an append-only JSONL journal back at open (noetl/ehdb#262, #426).
//!
//! Every local journal in this workspace (transaction log, stream log, system
//! library catalog) appends one record as `<json>\n` with `O_APPEND`, then
//! `sync_data`. A crash can therefore leave exactly two kinds of damage at the
//! end of the file, and [`read_jsonl`] handles both:
//!
//! - **A torn final line** — a record cut off part-way. Its `sync_data` never
//!   completed, so no caller was told it was durable. It is skipped, counted,
//!   and **cut off the file**, because leaving it there glues the next append
//!   onto it and turns a recoverable tail into an unreadable middle.
//! - **A complete final record with no newline** — it is read normally and its
//!   newline is restored, so the next append starts a new line.
//!
//! Anything else that fails to decode — a truncated record with records after
//! it, or a complete record with bad content — is an error. Those records were
//! acknowledged, and serving the journal without them would be serving an
//! edited history.

use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::Path;

use serde::de::DeserializeOwned;

use crate::{EhdbError, Result};

/// What [`read_jsonl`] read.
#[derive(Debug)]
pub struct JournalRead<T> {
    /// Decoded records with their 1-based line numbers, in file order.
    pub entries: Vec<(usize, T)>,
    /// Torn tail records skipped (0 or 1). Callers must surface this: a read
    /// that silently returns n-1 records looks exactly like a complete one.
    pub torn_tail_skipped: usize,
}

/// Read every record of the JSONL journal at `path`, repairing a torn tail on
/// disk so the journal is safe to append to. A missing file reads as empty.
///
/// `decode_error` maps a record that fails to decode (and is not a torn tail)
/// to the caller's error, given its 1-based line number.
pub fn read_jsonl<T: DeserializeOwned>(
    path: &Path,
    decode_error: impl Fn(usize, serde_json::Error) -> EhdbError,
) -> Result<JournalRead<T>> {
    let mut out = JournalRead {
        entries: Vec::new(),
        torn_tail_skipped: 0,
    };
    let file = match File::open(path) {
        Ok(f) => f,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(out),
        Err(err) => return Err(storage(err)),
    };
    let mut reader = BufReader::new(file);
    let mut offset = 0u64;
    let mut last_byte: Option<u8> = None;
    let mut index = 0usize;
    let mut torn: Option<(usize, u64, serde_json::Error)> = None;
    let mut buf = Vec::new();
    loop {
        buf.clear();
        let n = reader.read_until(b'\n', &mut buf).map_err(storage)?;
        if n == 0 {
            break;
        }
        let start = offset;
        offset += n as u64;
        last_byte = buf.last().copied();
        index += 1;
        let line = trim_newline(&buf);
        if line.iter().all(u8::is_ascii_whitespace) {
            continue;
        }
        if let Some((torn_index, _, err)) = torn.take() {
            // A non-blank line after an undecodable one: that record was not
            // the tail, so it is not torn — it is lost history.
            return Err(decode_error(torn_index, err));
        }
        // Raw bytes, not `str`: a crash can tear a multi-byte UTF-8 character,
        // and that must read as a truncated record, not as invalid text.
        match serde_json::from_slice::<T>(line) {
            Ok(entry) => out.entries.push((index, entry)),
            // Only a TRUNCATED record can be a torn tail. A complete record that
            // fails validation is bad content, never an interrupted append.
            Err(err) if err.is_eof() => torn = Some((index, start, err)),
            Err(err) => return Err(decode_error(index, err)),
        }
    }
    if let Some((_, start, _)) = torn {
        out.torn_tail_skipped = 1;
        // A line starts right after a `\n`, so the cut leaves a clean end.
        let file = OpenOptions::new().write(true).open(path).map_err(storage)?;
        file.set_len(start).map_err(storage)?;
        file.sync_data().map_err(storage)?;
    } else if last_byte.is_some_and(|b| b != b'\n') {
        let mut file = OpenOptions::new()
            .append(true)
            .open(path)
            .map_err(storage)?;
        file.write_all(b"\n").map_err(storage)?;
        file.sync_data().map_err(storage)?;
    }
    Ok(out)
}

fn trim_newline(buf: &[u8]) -> &[u8] {
    let line = buf.strip_suffix(b"\n").unwrap_or(buf);
    line.strip_suffix(b"\r").unwrap_or(line)
}

fn storage(err: std::io::Error) -> EhdbError {
    EhdbError::Storage(err.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::path::PathBuf;

    #[derive(serde::Deserialize, Debug, PartialEq)]
    struct Rec {
        n: u32,
    }

    fn tmp(tag: &str) -> PathBuf {
        use std::sync::atomic::{AtomicU32, Ordering};
        static N: AtomicU32 = AtomicU32::new(0);
        let n = N.fetch_add(1, Ordering::SeqCst);
        let d = std::env::temp_dir().join(format!("ehdb-journal-{tag}-{}-{n}", std::process::id()));
        fs::create_dir_all(&d).unwrap();
        d.join("j.jsonl")
    }

    fn read(p: &Path) -> Result<JournalRead<Rec>> {
        read_jsonl(p, |line, err| {
            EhdbError::Storage(format!("line {line}: {err}"))
        })
    }

    fn ns(r: &JournalRead<Rec>) -> Vec<u32> {
        r.entries.iter().map(|(_, e)| e.n).collect()
    }

    fn append(p: &Path, bytes: &[u8]) {
        OpenOptions::new()
            .create(true)
            .append(true)
            .open(p)
            .unwrap()
            .write_all(bytes)
            .unwrap();
    }

    #[test]
    fn a_missing_file_reads_empty() {
        let r = read(&tmp("missing")).unwrap();
        assert!(r.entries.is_empty());
        assert_eq!(r.torn_tail_skipped, 0);
    }

    #[test]
    fn a_torn_tail_is_skipped_and_cut_so_the_next_append_reopens() {
        let p = tmp("torn");
        append(&p, b"{\"n\":1}\n{\"n\":2}\n{\"n\":");
        let r = read(&p).unwrap();
        assert_eq!((ns(&r), r.torn_tail_skipped), (vec![1, 2], 1));
        append(&p, b"{\"n\":3}\n");
        let r = read(&p).expect("one crash + one append must reopen");
        assert_eq!((ns(&r), r.torn_tail_skipped), (vec![1, 2, 3], 0));
    }

    #[test]
    fn a_final_record_missing_its_newline_is_kept_and_terminated() {
        let p = tmp("nonl");
        append(&p, b"{\"n\":1}\n{\"n\":2}");
        assert_eq!(ns(&read(&p).unwrap()), vec![1, 2]);
        append(&p, b"{\"n\":3}\n");
        assert_eq!(ns(&read(&p).unwrap()), vec![1, 2, 3]);
    }

    #[test]
    fn a_torn_record_with_records_after_it_fails() {
        let p = tmp("middle");
        append(&p, b"{\"n\":1}\n{\"n\":\n{\"n\":3}\n");
        let err = read(&p).unwrap_err();
        assert!(format!("{err:?}").contains("line 2"), "{err:?}");
    }

    #[test]
    fn a_complete_but_invalid_tail_fails() {
        let p = tmp("invalid");
        append(&p, b"{\"n\":1}\n{\"n\":\"x\"}\n");
        assert!(read(&p).is_err());
    }

    #[test]
    fn blank_lines_after_a_torn_tail_still_count_as_the_tail() {
        let p = tmp("blank");
        append(&p, b"{\"n\":1}\n{\"n\":\n\n  \n");
        let r = read(&p).unwrap();
        assert_eq!((ns(&r), r.torn_tail_skipped), (vec![1], 1));
    }

    #[test]
    fn a_tail_torn_inside_a_utf8_character_is_a_torn_tail() {
        let p = tmp("utf8");
        append(&p, b"{\"n\":1}\n{\"s\":\"\xE2");
        let r = read(&p).unwrap();
        assert_eq!((ns(&r), r.torn_tail_skipped), (vec![1], 1));
    }
}
