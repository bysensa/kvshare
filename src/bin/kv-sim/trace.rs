//! What kvshare says through `tracing` while a process of a run works.
//!
//! Every event is kept in memory with its time since the process started: a hook that
//! could not open the database writes them out, so that a run tells what the hook was
//! waiting for. What a process says at `info` and `warn`, who owns the database, since
//! when, and what became of an owner, is also appended at once to `logs/events.log`,
//! which all the processes of a run share: the story of the database's owners.

use std::fmt::Debug;
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use tracing::field::{Field, Visit};
use tracing::{Event, Level, Metadata, Subscriber, span};

/// kvshare's events since the process started: when, in milliseconds, and what.
#[derive(Clone)]
pub struct Trace {
    started: Instant,
    role: &'static str,
    logs: PathBuf,
    events: Arc<Mutex<Vec<(u128, String)>>>,
}

/// The trace of this process.
static TRACE: OnceLock<Trace> = OnceLock::new();

/// Keep the events of this process, which plays `role` in `run`, from now on.
pub fn install(run: &Path, role: &'static str) {
    let trace = TRACE.get_or_init(|| Trace {
        started: Instant::now(),
        role,
        logs: run.join("logs"),
        events: Arc::default(),
    });
    let _ = tracing::subscriber::set_global_default(trace.clone());
}

/// The trace of this process, if it keeps one.
pub fn installed() -> Option<&'static Trace> {
    TRACE.get()
}

impl Trace {
    /// How many events hold `words`.
    pub fn count(&self, words: &str) -> usize {
        let events = self.events.lock().expect("no panic holding it");
        events
            .iter()
            .filter(|(_, event)| event.contains(words))
            .count()
    }

    /// Write every event to `logs/trace-<pid>.log`.
    pub fn write(&self) {
        let events = self.events.lock().expect("no panic holding it");
        let lines: String = events
            .iter()
            .map(|(at, event)| format!("{at:>6} ms {event}\n"))
            .collect();
        let file = format!("trace-{}.log", std::process::id());
        let _ = std::fs::write(self.logs.join(file), lines);
    }

    /// Append one line to the log the processes of the run share. One write, so that
    /// the lines of several processes stay whole.
    fn tell(&self, event: &str) {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |since| since.as_millis());
        let line = format!("{now} {} {} {event}\n", self.role, std::process::id());
        if let Ok(mut log) = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(self.logs.join("events.log"))
        {
            let _ = log.write_all(line.as_bytes());
        }
    }
}

impl Subscriber for Trace {
    fn enabled(&self, metadata: &Metadata<'_>) -> bool {
        metadata.target().starts_with("kvshare")
    }

    fn new_span(&self, _: &span::Attributes<'_>) -> span::Id {
        span::Id::from_u64(1)
    }

    fn record(&self, _: &span::Id, _: &span::Record<'_>) {}

    fn record_follows_from(&self, _: &span::Id, _: &span::Id) {}

    fn event(&self, event: &Event<'_>) {
        struct Words(String, String);
        impl Visit for Words {
            fn record_debug(&mut self, field: &Field, value: &dyn Debug) {
                if field.name() == "message" {
                    self.0 = format!("{value:?}");
                } else {
                    self.1.push_str(&format!(" {}={value:?}", field.name()));
                }
            }
        }
        let mut words = Words(String::new(), String::new());
        event.record(&mut words);
        let level = *event.metadata().level();
        let line = format!("{level} {}{}", words.0, words.1);
        if level <= Level::INFO {
            self.tell(&line);
        }
        let at = self.started.elapsed().as_millis();
        self.events
            .lock()
            .expect("no panic holding it")
            .push((at, line));
    }

    fn enter(&self, _: &span::Id) {}

    fn exit(&self, _: &span::Id) {}
}

/// What `logs/events.log` tells of a run's owners.
pub struct Owners {
    /// How many processes of each role came to own the database.
    pub by_role: Vec<(String, usize)>,
    /// How long the open that made each of them the owner took, in microseconds.
    pub opens: Vec<u128>,
    /// How long the database had no owner that served between one owner and the next,
    /// in microseconds: from the loss or the leaving of the one to the other's start.
    pub gaps: Vec<u128>,
    /// The rows of hooks that owned the database one after another until a long-lived
    /// process did: how long each row lasted, in milliseconds, and how many hooks.
    pub rows: Vec<(u128, usize)>,
}

/// A `Duration` as `{:?}` writes it (`37.5ms`, `1.2s`, `850µs`), in microseconds.
fn micros(text: &str) -> Option<u128> {
    let digits = text.trim_end_matches(|c: char| c.is_alphabetic() || c == 'µ');
    let number: f64 = digits.parse().ok()?;
    let scale = match &text[digits.len()..] {
        "ns" => 0.001,
        "µs" => 1.0,
        "ms" => 1_000.0,
        "s" => 1_000_000.0,
        _ => return None,
    };
    Some((number * scale) as u128)
}

/// The value of `name=` in an event's text.
fn field<'t>(text: &'t str, name: &str) -> Option<&'t str> {
    let from = text.find(&format!("{name}="))? + name.len() + 1;
    text[from..].split_whitespace().next()
}

/// Read the story of the owners of the run in `run`, if its processes left one.
pub fn owners(run: &Path) -> Option<Owners> {
    let log = std::fs::read_to_string(run.join("logs").join("events.log")).ok()?;
    // (when, role, pid, the open's time), then for each pid when it stopped serving.
    let mut owned: Vec<(u128, String, u32, u128)> = Vec::new();
    let mut ended: std::collections::HashMap<u32, u128> = std::collections::HashMap::new();
    for line in log.lines() {
        let mut words = line.splitn(5, ' ');
        let (Some(at), Some(role), Some(pid), Some(_level), Some(text)) = (
            words.next().and_then(|at| at.parse::<u128>().ok()),
            words.next(),
            words.next().and_then(|pid| pid.parse::<u32>().ok()),
            words.next(),
            words.next(),
        ) else {
            continue;
        };
        if text.starts_with("owns the database") {
            let opened = field(text, "opened").and_then(micros).unwrap_or(0);
            owned.push((at, role.to_string(), pid, opened));
        } else if text.starts_with("the owner is lost") {
            if let Some(owner) = field(text, "owner").and_then(|pid| pid.parse().ok()) {
                let first = ended.entry(owner).or_insert(at);
                *first = (*first).min(at);
            }
        } else if text.starts_with("handed the database over") {
            // It served until it hung up, `silent` before the close ended.
            let silent = field(text, "silent").and_then(micros).unwrap_or(0);
            ended.insert(pid, at.saturating_sub(silent / 1000));
        }
    }
    owned.sort();
    let mut by_role: Vec<(String, usize)> = Vec::new();
    for (_, role, _, _) in &owned {
        match by_role.iter_mut().find(|(name, _)| name == role) {
            Some((_, count)) => *count += 1,
            None => by_role.push((role.clone(), 1)),
        }
    }
    let gaps = owned
        .windows(2)
        .filter_map(|pair| {
            let end = ended.get(&pair[0].2)?;
            Some(pair[1].0.saturating_sub(*end) * 1000)
        })
        .collect();
    let (mut rows, mut row) = (Vec::new(), None::<(u128, usize)>);
    for (at, role, _, _) in &owned {
        match (&mut row, role == "hook") {
            (Some((_, hooks)), true) => *hooks += 1,
            (None, true) => row = Some((*at, 1)),
            (Some((since, hooks)), false) => {
                rows.push((at - *since, *hooks));
                row = None;
            }
            (None, false) => {}
        }
    }
    Some(Owners {
        by_role,
        opens: owned.iter().map(|(_, _, _, opened)| *opened).collect(),
        gaps,
        rows,
    })
}
