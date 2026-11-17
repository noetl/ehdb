//! Reading an append-only JSONL journal back at open (noetl/ehdb#262, #426).
//!
//! Every local journal in this workspace (transaction log, stream log, system
//! library catalog) appends one record as `<json>\n` with `O_APPEND`, then
//! `sync_data`. A crash can therefore leave exactly two kinds of damage at the
//! end of the file, and [`read_jsonl`] handles both:
//!
//! - **A torn final line** — a record cut off part-way. Its `sync_data` never
//!   completed, so no caller was told it was durable. It is skipped and counted.
//! - **A complete final record with no newline** — it is read normally.
//!
//! Either one must be repaired **before the next append**, or the append is
//! glued onto it and a recoverable tail becomes an unreadable middle. That
//! repair is returned as a [`TailRepair`] and applied by the journal's append
//! path ([`TailRepair::apply`]) — **never by the read**.
//!
//! ⚠ Reading must not mutate the file (noetl/ehdb#426). The first version of this
//! module repaired the tail during open. Open is also a read path — including
//! reads of SEALED segments that are never appended to again — so the first
//! read truncated the torn bytes and every later read reported a clean segment:
//! the `torn_tail_skipped` signal callers surface (#262) was erased by the act of
//! looking. Caught by noetl/worker's
//! `a_torn_sealed_segment_is_reported_by_every_read_path`.
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
    /// What must be done to the file before anything is appended to it.
    pub repair: TailRepair,
}

/// The fix a journal needs before its next append — computed by [`read_jsonl`],
/// applied by the append path, never by a read.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct TailRepair {
    /// Cut the file to this length (the start of a torn final line).
    truncate_to: Option<u64>,
    /// Append a newline (a complete final record lost only its newline).
    append_newline: bool,
}

impl TailRepair {
    /// Whether anything needs doing.
    pub fn is_needed(&self) -> bool {
        self.truncate_to.is_some() || self.append_newline
    }

    /// Apply the repair to `path`, `sync_data` it, and mark it done. Idempotent:
    /// a second call does nothing. Call it immediately before appending.
    pub fn apply(&mut self, path: &Path) -> Result<()> {
        if let Some(len) = self.truncate_to {
            let file = OpenOptions::new().write(true).open(path).map_err(storage)?;
            file.set_len(len).map_err(storage)?;
            file.sync_data().map_err(storage)?;
        } else if self.append_newline {
            let mut file = OpenOptions::new()
                .append(true)
                .open(path)
                .map_err(storage)?;
            file.write_all(b"\n").map_err(storage)?;
            file.sync_data().map_err(storage)?;
        }
        *self = Self::default();
        Ok(())
    }
}

/// Read every record of the JSONL journal at `path`. **Read-only**: a torn tail
/// is reported in [`JournalRead::repair`], not fixed. A missing file reads as
/// empty.
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
        repair: TailRepair::default(),
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
        // A line starts right after a newline, so the cut leaves a clean end.
        out.repair.truncate_to = Some(start);
    } else if last_byte.is_some_and(|b| b != b'\n') {
        out.repair.append_newline = true;
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

    /// The writer's sequence: read, repair, then append.
    fn repair_then_append(p: &Path, r: &mut JournalRead<Rec>, bytes: &[u8]) {
        r.repair.apply(p).unwrap();
        append(p, bytes);
    }

    /// noetl/ehdb#426: reading never changes the file, so a torn tail is
    /// reported on EVERY read until a writer repairs it — the signal callers
    /// surface must not be erased by the act of reading.
    #[test]
    fn reading_never_mutates_the_file() {
        let cases: [(&str, &[u8]); 2] = [
            ("ro-torn", b"{\"n\":1}\n{\"n\":"),
            ("ro-nonl", b"{\"n\":1}"),
        ];
        for (tag, bytes) in cases {
            let p = tmp(tag);
            append(&p, bytes);
            for _ in 0..3 {
                let r = read(&p).unwrap();
                assert!(r.repair.is_needed(), "{tag}");
                assert_eq!(
                    std::fs::read(&p).unwrap(),
                    bytes,
                    "{tag}: a read changed the file"
                );
            }
        }
        let p = tmp("ro-torn-count");
        append(&p, b"{\"n\":1}\n{\"n\":");
        assert_eq!(read(&p).unwrap().torn_tail_skipped, 1);
        assert_eq!(
            read(&p).unwrap().torn_tail_skipped,
            1,
            "the second read lost the signal"
        );
    }

    #[test]
    fn a_torn_tail_is_skipped_and_cut_so_the_next_append_reopens() {
        let p = tmp("torn");
        append(&p, b"{\"n\":1}\n{\"n\":2}\n{\"n\":");
        let mut r = read(&p).unwrap();
        assert_eq!((ns(&r), r.torn_tail_skipped), (vec![1, 2], 1));
        repair_then_append(&p, &mut r, b"{\"n\":3}\n");
        let r = read(&p).expect("one crash + one append must reopen");
        assert_eq!((ns(&r), r.torn_tail_skipped), (vec![1, 2, 3], 0));
    }

    #[test]
    fn a_final_record_missing_its_newline_is_kept_and_terminated() {
        let p = tmp("nonl");
        append(&p, b"{\"n\":1}\n{\"n\":2}");
        let mut r = read(&p).unwrap();
        assert_eq!(ns(&r), vec![1, 2]);
        repair_then_append(&p, &mut r, b"{\"n\":3}\n");
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
