//! The journal of a role's actions: what it began, what it saw committed, what it found
//! wrong, and how long things took. One file a process, written a line at a time with
//! `O_APPEND`, so what is written survives the process being killed; a line cut short
//! by the kill is left out when the journals are read.
//!
//! Lines:
//! - `begin <ms> <op> <kind> <args…>`: an operation is about to commit;
//! - `ack <ms> <op>`: its commit returned, or a read after a lost answer found it;
//! - `skip <ms> <op>` and `fail <ms> <op> <reason…>`: it wrote nothing, or gave up;
//! - `seen <ms> <event> <value>`: a hook found an event answered already;
//! - `violation <ms> <text…>`: a check during the run failed;
//! - `time <name> <micros> [<extra>]` and `count <name>`: what the report sums up.

use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{self, Write as _};
use std::path::{Path, PathBuf};

use crate::keys::{OpId, now_ms};

pub struct Journal {
    file: File,
    role: char,
    pid: u32,
    next: u64,
}

/// What sets apart the numbers a reused pid takes in one run.
const REUSED: u32 = 10_000_000;

impl Journal {
    /// The journal of this process as `role`, in `<run>/journal/`, under the process's
    /// number in the run. The number is its pid, unless the journal of that pid is there
    /// already: the system gives the pids of processes that ended to new ones, and a long
    /// run starts more processes than there are pids. Then it is `pid + REUSED × k`, for
    /// the first `k` whose journal is not there; two processes with one pid never live
    /// at once, so no other process takes the same number meanwhile.
    pub fn open(run: &Path, name: &str, role: char) -> io::Result<Journal> {
        let dir = run.join("journal");
        std::fs::create_dir_all(&dir)?;
        let pid = std::process::id();
        // At most 400 lives of one pid, so that the number fits a u32 for any pid.
        for k in 0..400 {
            let number = pid + REUSED * k;
            let opened = OpenOptions::new()
                .create_new(true)
                .append(true)
                .open(dir.join(format!("{name}-{number}.log")));
            match opened {
                Ok(file) => {
                    return Ok(Journal {
                        file,
                        role,
                        pid: number,
                        next: 0,
                    });
                }
                Err(error) if error.kind() == io::ErrorKind::AlreadyExists => continue,
                Err(error) => return Err(error),
            }
        }
        Err(io::Error::other(format!("pid {pid} was used 400 times")))
    }

    /// The process's number in the run, see [`Journal::open`].
    pub fn number(&self) -> u32 {
        self.pid
    }

    pub fn next_op(&mut self) -> OpId {
        self.next += 1;
        OpId {
            role: self.role,
            pid: self.pid,
            n: self.next,
        }
    }

    /// Write `text` as one line, in one write.
    fn line(&mut self, text: String) {
        let mut line = text;
        line.push('\n');
        self.file
            .write_all(line.as_bytes())
            .expect("the journal is writable");
    }

    pub fn begin(&mut self, op: OpId, what: &str) {
        self.line(format!("begin {} {op} {what}", now_ms()));
    }

    pub fn ack(&mut self, op: OpId) {
        self.line(format!("ack {} {op}", now_ms()));
    }

    pub fn skip(&mut self, op: OpId) {
        self.line(format!("skip {} {op}", now_ms()));
    }

    pub fn fail(&mut self, op: OpId, reason: &str) {
        self.line(format!("fail {} {op} {}", now_ms(), one_line(reason)));
    }

    pub fn seen(&mut self, event: u64, value: u64) {
        self.line(format!("seen {} {event} {value}", now_ms()));
    }

    pub fn violation(&mut self, text: &str) {
        self.line(format!("violation {} {}", now_ms(), one_line(text)));
    }

    pub fn time(&mut self, name: &str, micros: u128, extra: &str) {
        self.line(format!("time {name} {micros} {}", one_line(extra)));
    }

    pub fn count(&mut self, name: &str) {
        self.line(format!("count {name}"));
    }
}

fn one_line(text: &str) -> String {
    text.replace('\n', " ")
}

/// Where an operation got to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum State {
    Begun,
    Acked,
    Skipped,
    Failed,
}

/// An operation as the journals tell it.
#[derive(Debug, Clone)]
pub struct Record {
    pub ms: u64,
    pub op: OpId,
    pub kind: String,
    pub args: Vec<String>,
    pub state: State,
}

impl Record {
    pub fn arg(&self, n: usize) -> &str {
        self.args.get(n).map_or("", String::as_str)
    }

    pub fn number(&self, n: usize) -> u64 {
        self.arg(n).parse().unwrap_or(u64::MAX)
    }
}

/// Every journal of a run, read together.
#[derive(Debug, Default)]
pub struct Journals {
    pub ops: HashMap<OpId, Record>,
    pub seen: Vec<(u64, u64)>,
    pub violations: Vec<String>,
    pub times: HashMap<String, Vec<(u64, String)>>,
    pub counts: HashMap<String, u64>,
}

impl Journals {
    pub fn read(run: &Path) -> io::Result<Journals> {
        let mut journals = Journals::default();
        let dir = run.join("journal");
        let mut files: Vec<PathBuf> = match std::fs::read_dir(&dir) {
            Ok(entries) => entries.filter_map(|e| e.ok().map(|e| e.path())).collect(),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Vec::new(),
            Err(error) => return Err(error),
        };
        files.sort();
        for path in files {
            let text = std::fs::read_to_string(&path)?;
            // The last line of a killed process may be cut short; a full line ends
            // with a newline.
            let complete = text.rfind('\n').map_or("", |end| &text[..end]);
            for line in complete.lines() {
                journals.take(line);
            }
        }
        Ok(journals)
    }

    fn take(&mut self, line: &str) {
        let words: Vec<&str> = line.split(' ').collect();
        let op = || words.get(2).and_then(|op| OpId::parse(op));
        let ms = || words.get(1).and_then(|ms| ms.parse().ok());
        match words.first().copied() {
            Some("begin") => {
                if let (Some(ms), Some(op), Some(kind)) = (ms(), op(), words.get(3)) {
                    self.ops.insert(
                        op,
                        Record {
                            ms,
                            op,
                            kind: kind.to_string(),
                            args: words[4..].iter().map(|w| w.to_string()).collect(),
                            state: State::Begun,
                        },
                    );
                }
            }
            Some(state @ ("ack" | "skip" | "fail")) => {
                if let Some(record) = op().and_then(|op| self.ops.get_mut(&op)) {
                    record.state = match state {
                        "ack" => State::Acked,
                        "skip" => State::Skipped,
                        _ => State::Failed,
                    };
                }
            }
            Some("seen") => {
                if let (Some(event), Some(value)) = (
                    words.get(2).and_then(|w| w.parse().ok()),
                    words.get(3).and_then(|w| w.parse().ok()),
                ) {
                    self.seen.push((event, value));
                }
            }
            Some("violation") => self.violations.push(words[2.min(words.len())..].join(" ")),
            Some("time") => {
                if let (Some(name), Some(micros)) =
                    (words.get(1), words.get(2).and_then(|w| w.parse().ok()))
                {
                    let extra = words.get(3..).map(|w| w.join(" ")).unwrap_or_default();
                    self.times
                        .entry(name.to_string())
                        .or_default()
                        .push((micros, extra));
                }
            }
            Some("count") => {
                if let Some(name) = words.get(1) {
                    *self.counts.entry(name.to_string()).or_default() += 1;
                }
            }
            _ => {}
        }
    }

    /// The operations of `kind`.
    pub fn of(&self, kind: &str) -> impl Iterator<Item = &Record> {
        self.ops.values().filter(move |record| record.kind == kind)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A pid used again in one run gets a number and a journal of its own, so that the
    /// operations of the two processes keep apart.
    #[test]
    fn a_reused_pid_gets_a_number_of_its_own() {
        let run = std::env::temp_dir().join(format!("kvsim-journal-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&run);
        let mut first = Journal::open(&run, "hook", 'h').expect("the first journal");
        let mut second = Journal::open(&run, "hook", 'h').expect("the second journal");
        let pid = std::process::id();
        assert_eq!((first.number(), second.number()), (pid, pid + REUSED));
        let (one, two) = (first.next_op(), second.next_op());
        first.begin(one, "answer 1 1");
        second.begin(two, "answer 2 2");
        let journals = Journals::read(&run).expect("the journals");
        assert_eq!(
            journals.ops.len(),
            2,
            "two operations, not one under one id"
        );
        let _ = std::fs::remove_dir_all(&run);
    }
}
