//! The check of a run, once every process has stopped: the database, opened with
//! SurrealKV itself (which also proves it recovers after its last owner was killed),
//! against the roles' journals.
//!
//! Structure, which must hold in any snapshot, the newest checkpoint's too:
//! - threads: journals from 1 to the head without a gap, epochs that never go back and
//!   one process an epoch, an operation's entries together and as many as it wrote, and
//!   nothing of a thread without its record;
//! - tasks: transitions a path through the flow from `new`, the task its last step, its
//!   versions its steps;
//! - index: every count of references the number of files that refer, every fact
//!   referred to, nothing referred to missing;
//! - metrics: the total the sum of the records.
//!
//! Against the journals, for the database only:
//! - nothing acknowledged is lost, unless a later operation replaced or removed it;
//! - nothing is applied twice, and nothing is there that no journal began;
//! - a removed thread is gone whole;
//! - an event has one answer, the one its hooks saw.

use std::collections::{BTreeMap, HashMap};
use std::path::Path;

use surrealkv::{LSMIterator, Mode, TreeBuilder};

use crate::journal::{Journals, State};
use crate::keys::{
    self, Answer, Entry, Fact, FileFacts, Lease, Meta, Metric, OpId, Status, Task, Total,
    Transition,
};
use crate::roles::keeper::last_checkpoint;

/// Something wrong the check found.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Finding {
    pub check: &'static str,
    pub text: String,
}

fn finding(check: &'static str, text: String) -> Finding {
    Finding { check, text }
}

pub async fn run(dir: &Path) -> Result<Vec<Finding>, String> {
    let journals = Journals::read(dir).map_err(|e| e.to_string())?;
    let mut findings: Vec<Finding> = journals
        .violations
        .iter()
        .map(|text| finding("online", text.clone()))
        .collect();
    let model = Model::read(&dir.join("db")).await?;
    findings.extend(model.structure());
    findings.extend(model.against(&journals));
    if let Some(n) = last_checkpoint(dir) {
        let copy = dir.join("ckpt-check");
        let _ = std::fs::remove_dir_all(&copy);
        copy_dir(&dir.join("ckpt").join(n.to_string()), &copy).map_err(|e| e.to_string())?;
        match Model::read(&copy).await {
            Ok(checkpoint) => findings.extend(checkpoint.structure().into_iter().map(|f| {
                finding(
                    "checkpoint",
                    format!("checkpoint {n}: {}: {}", f.check, f.text),
                )
            })),
            Err(error) => findings.push(finding(
                "checkpoint",
                format!("checkpoint {n} does not open: {error}"),
            )),
        }
        let _ = std::fs::remove_dir_all(&copy);
    }
    Ok(findings)
}

fn copy_dir(from: &Path, to: &Path) -> std::io::Result<()> {
    std::fs::create_dir_all(to)?;
    for entry in std::fs::read_dir(from)? {
        let entry = entry?;
        let target = to.join(entry.file_name());
        if entry.file_type()?.is_dir() {
            copy_dir(&entry.path(), &target)?;
        } else {
            std::fs::copy(entry.path(), target)?;
        }
    }
    Ok(())
}

/// What the database holds, as the check needs it; a fact's body is left out.
#[derive(Debug, Default)]
pub struct Model {
    pub metas: BTreeMap<String, Meta>,
    pub leases: BTreeMap<String, Lease>,
    pub entries: BTreeMap<String, Vec<(u64, Entry)>>,
    pub tasks: BTreeMap<u64, Task>,
    pub versions: BTreeMap<u64, Vec<Status>>,
    pub transitions: BTreeMap<u64, Vec<(u64, Transition)>>,
    pub facts: BTreeMap<String, OpId>,
    pub references: BTreeMap<String, u64>,
    pub files: BTreeMap<String, FileFacts>,
    pub metrics: BTreeMap<String, Vec<(u64, Metric)>>,
    pub total: Option<Total>,
    pub answers: BTreeMap<u64, Answer>,
    pub unreadable: Vec<String>,
}

/// Every key and value of `[start, end)`.
fn scan(
    tx: &surrealkv::Transaction,
    start: &[u8],
    end: &[u8],
    mut each: impl FnMut(&[u8], Vec<u8>),
) -> surrealkv::Result<()> {
    let mut cursor = tx.range(start, end)?;
    let mut valid = cursor.seek_first()?;
    while valid {
        let key = cursor.key().user_key().to_vec();
        each(&key, cursor.value()?);
        valid = cursor.next()?;
    }
    Ok(())
}

impl Model {
    /// Read the database at `path` with SurrealKV itself.
    pub async fn read(path: &Path) -> Result<Model, String> {
        let tree = TreeBuilder::new()
            .with_path(path.to_path_buf())
            .with_versioning(true, 0)
            .with_enable_vlog(true)
            .build()
            .map_err(|e| e.to_string())?;
        let model = Model::from_tree(&tree).map_err(|e| e.to_string());
        tree.close().await.map_err(|e| e.to_string())?;
        model
    }

    fn from_tree(tree: &surrealkv::Tree) -> surrealkv::Result<Model> {
        let tx = tree.begin_with_mode(Mode::ReadOnly)?;
        let mut model = Model::default();
        let mut bad = Vec::new();
        macro_rules! decoded {
            ($key:expr, $value:expr) => {
                match keys::decode(&$value) {
                    Ok(value) => Some(value),
                    Err(_) => {
                        bad.push(String::from_utf8_lossy($key).into_owned());
                        None
                    }
                }
            };
        }
        scan(&tx, b"tm/", b"tm0", |key, value| {
            if let (Some(thread), Some(meta)) = (keys::thread_of(key, "tm/"), decoded!(key, value))
            {
                model.metas.insert(thread, meta);
            }
        })?;
        scan(&tx, b"tl/", b"tl0", |key, value| {
            if let (Some(thread), Some(lease)) = (keys::thread_of(key, "tl/"), decoded!(key, value))
            {
                model.leases.insert(thread, lease);
            }
        })?;
        scan(&tx, b"t/", b"t0", |key, value| {
            if let (Some(thread), Some(seq), Some(entry)) = (
                keys::thread_of(key, "t/"),
                keys::number_of(key),
                decoded!(key, value),
            ) {
                model.entries.entry(thread).or_default().push((seq, entry));
            }
        })?;
        scan(&tx, b"tk/", b"tk0", |key, value| {
            if let (Some(id), Some(task)) = (keys::number_of(key), decoded!(key, value)) {
                model.tasks.insert(id, task);
            }
        })?;
        scan(&tx, b"tx/", b"tx0", |key, value| {
            let id = keys::thread_of(key, "tx/").and_then(|id| id.parse().ok());
            if let (Some(id), Some(n), Some(step)) =
                (id, keys::number_of(key), decoded!(key, value))
            {
                model.transitions.entry(id).or_default().push((n, step));
            }
        })?;
        scan(&tx, b"ix/f/", b"ix/f0", |key, value| {
            let hash = String::from_utf8_lossy(&key[5..]).into_owned();
            if let Some(fact) = decoded!(key, value) {
                let fact: Fact = fact;
                model.facts.insert(hash, fact.op);
            }
        })?;
        scan(&tx, b"ix/r/", b"ix/r0", |key, value| {
            let hash = String::from_utf8_lossy(&key[5..]).into_owned();
            if let Some(count) = decoded!(key, value) {
                model.references.insert(hash, count);
            }
        })?;
        scan(&tx, b"ix/p/", b"ix/p0", |key, value| {
            let path = String::from_utf8_lossy(&key[5..]).into_owned();
            if let Some(file) = decoded!(key, value) {
                model.files.insert(path, file);
            }
        })?;
        scan(&tx, b"m/r/", b"m/r0", |key, value| {
            if let (Some(thread), Some(request), Some(metric)) = (
                keys::thread_of(key, "m/r/"),
                keys::number_of(key),
                decoded!(key, value),
            ) {
                model
                    .metrics
                    .entry(thread)
                    .or_default()
                    .push((request, metric));
            }
        })?;
        if let Some(value) = tx.get(keys::TOTAL)? {
            model.total = decoded!(keys::TOTAL, value);
        }
        scan(&tx, b"hk/", b"hk0", |key, value| {
            if let (Some(event), Some(answer)) = (keys::number_of(key), decoded!(key, value)) {
                model.answers.insert(event, answer);
            }
        })?;
        for &id in model.tasks.keys() {
            let key = keys::task(id);
            let mut past = key.clone();
            past.push(0);
            let mut cursor = tx.history(key.as_slice(), past.as_slice())?;
            let mut valid = cursor.seek_first()?;
            let mut versions = Vec::new();
            while valid {
                if let Ok(task) = keys::decode::<Task>(&cursor.value()?) {
                    versions.push(task.status);
                }
                valid = cursor.next()?;
            }
            model.versions.insert(id, versions);
        }
        model.unreadable = bad;
        Ok(model)
    }

    /// What must hold in any snapshot.
    pub fn structure(&self) -> Vec<Finding> {
        let mut found = Vec::new();
        for key in &self.unreadable {
            found.push(finding("unreadable", format!("{key} does not decode")));
        }
        self.threads(&mut found);
        self.tasks(&mut found);
        self.index(&mut found);
        let summed = self
            .metrics
            .values()
            .flatten()
            .fold(Total::default(), |mut sum, (_, m)| {
                sum.requests += 1;
                sum.tokens += m.tokens;
                sum
            });
        if self.total.unwrap_or_default() != summed {
            found.push(finding(
                "metrics",
                format!(
                    "the total is {:?} but the records sum to {summed:?}",
                    self.total
                ),
            ));
        }
        found
    }

    fn threads(&self, found: &mut Vec<Finding>) {
        for (thread, entries) in &self.entries {
            let Some(meta) = self.metas.get(thread) else {
                found.push(finding(
                    "removed",
                    format!(
                        "thread {thread} has {} entries but no record",
                        entries.len()
                    ),
                ));
                continue;
            };
            let (mut last, mut epoch, mut pid) = (0, 0, 0);
            for (seq, entry) in entries {
                if *seq != last + 1 {
                    found.push(finding(
                        "thread",
                        format!("thread {thread}: entry {seq} follows {last}"),
                    ));
                }
                if entry.epoch < epoch || (entry.epoch == epoch && entry.pid != pid) {
                    found.push(finding(
                        "thread",
                        format!(
                            "thread {thread}: entry {seq} of epoch {} by {} follows epoch {epoch} by {pid}",
                            entry.epoch, entry.pid
                        ),
                    ));
                }
                (last, epoch, pid) = (*seq, entry.epoch, entry.pid);
            }
            if meta.head != last {
                found.push(finding(
                    "thread",
                    format!(
                        "thread {thread}: its head is {} but its last entry is {last}",
                        meta.head
                    ),
                ));
            }
            // An operation's entries lie together, as many as it wrote.
            let mut batches: BTreeMap<OpId, (u64, u64, u32)> = BTreeMap::new();
            for (seq, entry) in entries {
                let batch = batches.entry(entry.op).or_insert((*seq, 0, entry.batch));
                batch.1 += 1;
            }
            for (op, (first, count, batch)) in batches {
                if count != u64::from(batch) {
                    found.push(finding(
                        "twice",
                        format!("thread {thread}: operation {op} wrote {count} entries from {first}, not {batch}"),
                    ));
                }
            }
        }
        for thread in self.metas.keys() {
            if !self.entries.contains_key(thread) && self.metas[thread].head != 0 {
                found.push(finding(
                    "thread",
                    format!(
                        "thread {thread}: its head is {} but it has no entries",
                        self.metas[thread].head
                    ),
                ));
            }
        }
        for thread in self.leases.keys().chain(self.metrics.keys()) {
            if !self.metas.contains_key(thread) {
                found.push(finding(
                    "removed",
                    format!("thread {thread} left keys without its record"),
                ));
            }
        }
    }

    fn tasks(&self, found: &mut Vec<Finding>) {
        for (id, steps) in &self.transitions {
            let mut previous = None;
            for (at, (n, step)) in steps.iter().enumerate() {
                if *n != at as u64 + 1 {
                    found.push(finding(
                        "task",
                        format!("task {id}: transition {n} is number {}", at + 1),
                    ));
                }
                if step.from != previous || !Status::allowed(step.from, step.to) {
                    found.push(finding(
                        "task",
                        format!(
                            "task {id}: transition {n} goes {:?} → {:?} after {previous:?}",
                            step.from, step.to
                        ),
                    ));
                }
                previous = Some(step.to);
            }
            match self.tasks.get(id) {
                Some(task) if Some(task.status) == previous && task.n == steps.len() as u64 => {}
                other => found.push(finding(
                    "task",
                    format!(
                        "task {id} is {other:?} after {} transitions to {previous:?}",
                        steps.len()
                    ),
                )),
            }
            let mut tos: Vec<Status> = steps.iter().map(|(_, step)| step.to).collect();
            tos.reverse();
            if self
                .versions
                .get(id)
                .is_some_and(|versions| *versions != tos)
            {
                found.push(finding(
                    "task",
                    format!(
                        "task {id}: its versions are {:?} but its steps went {tos:?}",
                        self.versions[id]
                    ),
                ));
            }
        }
        for id in self.tasks.keys() {
            if !self.transitions.contains_key(id) {
                found.push(finding("task", format!("task {id} has no transitions")));
            }
        }
    }

    fn index(&self, found: &mut Vec<Finding>) {
        let mut counted: BTreeMap<&str, u64> = BTreeMap::new();
        for file in self.files.values() {
            for hash in &file.hashes {
                *counted.entry(hash).or_default() += 1;
            }
        }
        for (hash, count) in &counted {
            if self.references.get(*hash) != Some(count) {
                found.push(finding(
                    "index",
                    format!(
                        "fact {hash}: {count} files refer to it but its count is {:?}",
                        self.references.get(*hash)
                    ),
                ));
            }
            if !self.facts.contains_key(*hash) {
                found.push(finding(
                    "index",
                    format!("fact {hash} is referred to but missing"),
                ));
            }
        }
        for hash in self.references.keys().chain(self.facts.keys()) {
            if !counted.contains_key(hash.as_str()) {
                found.push(finding(
                    "index",
                    format!("fact {hash} is kept with no file referring to it"),
                ));
            }
        }
    }

    /// What must hold against the journals.
    pub fn against(&self, journals: &Journals) -> Vec<Finding> {
        let mut found = Vec::new();
        let dropped: std::collections::HashSet<&str> =
            journals.of("drop").map(|record| record.arg(0)).collect();
        let gone = |thread: &str| !self.metas.contains_key(thread) && dropped.contains(thread);

        // Every operation in the database was begun by some journal.
        let mut ops: HashMap<OpId, u64> = HashMap::new();
        for entries in self.entries.values() {
            for (_, entry) in entries {
                ops.entry(entry.op).or_insert(0);
            }
        }
        for steps in self.transitions.values() {
            for (_, step) in steps {
                *ops.entry(step.op).or_insert(0) += 1;
            }
        }
        for metrics in self.metrics.values() {
            for (_, metric) in metrics {
                *ops.entry(metric.op).or_insert(0) += 1;
            }
        }
        for answer in self.answers.values() {
            *ops.entry(answer.op).or_insert(0) += 1;
        }
        for (op, count) in &ops {
            if !journals.ops.contains_key(op) {
                found.push(finding(
                    "unknown_op",
                    format!("operation {op} is in the database but no journal began it"),
                ));
            }
            if *count > 1 {
                found.push(finding(
                    "twice",
                    format!("operation {op} is in the database {count} times"),
                ));
            }
        }

        for record in journals.ops.values().filter(|r| r.state == State::Acked) {
            let op = record.op;
            match record.kind.as_str() {
                "append" => {
                    let thread = record.arg(0);
                    if gone(thread) {
                        continue;
                    }
                    let written = self.entries.get(thread).map_or(0, |entries| {
                        entries.iter().filter(|(_, e)| e.op == op).count()
                    });
                    if written as u64 != record.number(1) {
                        found.push(finding(
                            "lost",
                            format!("append {op} to {thread} was acknowledged with {} entries; {written} are there", record.number(1)),
                        ));
                    }
                }
                "metric" => {
                    let thread = record.arg(0);
                    let there = self
                        .metrics
                        .get(thread)
                        .is_some_and(|metrics| metrics.iter().any(|(_, m)| m.op == op));
                    if !there && !gone(thread) {
                        found.push(finding(
                            "lost",
                            format!("metric {op} of {thread} was acknowledged but is not there"),
                        ));
                    }
                }
                "transition" => {
                    let (task, n) = (record.number(0), record.number(3));
                    let there = self
                        .transitions
                        .get(&task)
                        .is_some_and(|steps| steps.iter().any(|(at, s)| *at == n && s.op == op));
                    if !there {
                        found.push(finding(
                            "lost",
                            format!(
                                "transition {op} of task {task} was acknowledged but is not there"
                            ),
                        ));
                    }
                }
                "answer" => {
                    let event = record.number(0);
                    match self.answers.get(&event) {
                        Some(answer) if answer.op == op && answer.value == record.number(1) => {}
                        other => found.push(finding(
                            "hook",
                            format!("answer {op} to event {event} was acknowledged but the event holds {other:?}"),
                        )),
                    }
                }
                "index" => {
                    let path = record.arg(0);
                    match self.files.get(path) {
                        Some(file) if file.op == op => {}
                        Some(file) => {
                            let later = journals.ops.get(&file.op).is_some_and(|r| {
                                r.kind == "index" && r.arg(0) == path && r.ms >= record.ms
                            });
                            if !later {
                                found.push(finding(
                                    "lost",
                                    format!(
                                        "index {op} of {path} was acknowledged but {path} holds {}",
                                        file.op
                                    ),
                                ));
                            }
                        }
                        None => found.push(finding(
                            "lost",
                            format!("index {op} of {path} was acknowledged but {path} is missing"),
                        )),
                    }
                }
                "close" => {
                    let thread = record.arg(0);
                    if !self.metas.get(thread).is_some_and(|m| m.closed) && !gone(thread) {
                        found.push(finding(
                            "lost",
                            format!("close {op} of {thread} was acknowledged but it is open"),
                        ));
                    }
                }
                "drop" => {
                    let thread = record.arg(0);
                    if self.metas.contains_key(thread)
                        || self.entries.contains_key(thread)
                        || self.metrics.contains_key(thread)
                        || self.leases.contains_key(thread)
                    {
                        found.push(finding("removed", format!("drop {op} of {thread} was acknowledged but the thread is still there")));
                    }
                }
                _ => {}
            }
        }

        // An event has one answer, the one every hook that found it saw.
        for (event, value) in &journals.seen {
            match self.answers.get(event) {
                Some(answer) if answer.value == *value => {}
                other => found.push(finding(
                    "hook",
                    format!("a hook saw {value} for event {event}, which holds {other:?}"),
                )),
            }
        }
        found
    }
}
