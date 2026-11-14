//! The keys and the values of the simulated Milou, shared by the roles and the check.
//!
//! Numbers in keys are padded to 20 digits so that keys sort as the numbers do. Every
//! value a role writes carries the id of the operation that wrote it, which the check
//! finds again from the roles' journals.

use std::fmt;

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

/// An operation of a role: who began it and its number in that process.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct OpId {
    pub role: char,
    /// The process's number in the run: its pid, made unique when the pid is reused,
    /// see `Journal::open`. Leases and journal entries name processes by it too.
    pub pid: u32,
    pub n: u64,
}

impl fmt::Display for OpId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}{}.{}", self.role, self.pid, self.n)
    }
}

impl OpId {
    /// The id `Display` wrote.
    pub fn parse(text: &str) -> Option<OpId> {
        let mut chars = text.chars();
        let role = chars.next()?;
        let (pid, n) = chars.as_str().split_once('.')?;
        Some(OpId {
            role,
            pid: pid.parse().ok()?,
            n: n.parse().ok()?,
        })
    }
}

/// A thread's record: the number of its last journal entry, and whether it is closed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct Meta {
    pub head: u64,
    pub closed: bool,
}

/// Who drives a thread, under which epoch, and until when (unix milliseconds).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Lease {
    pub pid: u32,
    pub epoch: u64,
    pub until_ms: u64,
}

/// One entry of a thread's journal, written by `op` as one of `batch` entries.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Entry {
    pub op: OpId,
    pub batch: u32,
    pub pid: u32,
    pub epoch: u64,
    pub body: Vec<u8>,
}

/// The statuses of a task's flow.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum Status {
    New,
    Triaged,
    Working,
    Review,
    Done,
    Blocked,
}

impl Status {
    /// Whether the flow lets a task go from `from` (`None` for a task not yet created)
    /// to `to`.
    pub fn allowed(from: Option<Status>, to: Status) -> bool {
        use Status::*;
        match (from, to) {
            (None, New) => true,
            (Some(New), Triaged) | (Some(Triaged), Working) | (Some(Working), Review) => true,
            (Some(Review), Done) | (Some(Blocked), Working) => true,
            (Some(Done | Blocked), Blocked) => false,
            (Some(_), Blocked) => true,
            _ => false,
        }
    }
}

/// A task: its status and how many transitions brought it there.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Task {
    pub status: Status,
    pub n: u64,
    pub op: OpId,
}

/// One transition of a task.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Transition {
    pub from: Option<Status>,
    pub to: Status,
    pub op: OpId,
}

/// A fact of the code index, by the hash of its content.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Fact {
    pub op: OpId,
    pub body: Vec<u8>,
}

/// The facts a file holds.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FileFacts {
    pub op: OpId,
    pub hashes: Vec<String>,
}

/// One request of a thread to the model.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Metric {
    pub op: OpId,
    pub tokens: u64,
}

/// The sum of every request's metric.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct Total {
    pub requests: u64,
    pub tokens: u64,
}

/// A hook's answer to an event.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct Answer {
    pub op: OpId,
    pub value: u64,
}

pub fn encode<T: Serialize>(value: &T) -> Vec<u8> {
    postcard::to_allocvec(value).expect("a value of the simulation encodes")
}

pub fn decode<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, String> {
    postcard::from_bytes(bytes).map_err(|error| error.to_string())
}

pub fn meta(thread: &str) -> Vec<u8> {
    format!("tm/{thread}").into_bytes()
}

pub fn lease(thread: &str) -> Vec<u8> {
    format!("tl/{thread}").into_bytes()
}

pub fn entry(thread: &str, seq: u64) -> Vec<u8> {
    format!("t/{thread}/{seq:020}").into_bytes()
}

/// The range of a thread's journal.
pub fn entries(thread: &str) -> (Vec<u8>, Vec<u8>) {
    (
        format!("t/{thread}/").into_bytes(),
        format!("t/{thread}0").into_bytes(),
    )
}

pub fn task(id: u64) -> Vec<u8> {
    format!("tk/{id:06}").into_bytes()
}

pub fn transition(id: u64, n: u64) -> Vec<u8> {
    format!("tx/{id:06}/{n:020}").into_bytes()
}

/// The range of a task's transitions.
pub fn transitions(id: u64) -> (Vec<u8>, Vec<u8>) {
    (
        format!("tx/{id:06}/").into_bytes(),
        format!("tx/{id:06}0").into_bytes(),
    )
}

pub fn fact(hash: &str) -> Vec<u8> {
    format!("ix/f/{hash}").into_bytes()
}

pub fn references(hash: &str) -> Vec<u8> {
    format!("ix/r/{hash}").into_bytes()
}

pub fn file(path: &str) -> Vec<u8> {
    format!("ix/p/{path}").into_bytes()
}

pub fn metric(thread: &str, request: u64) -> Vec<u8> {
    format!("m/r/{thread}/{request:020}").into_bytes()
}

/// The range of a thread's metrics.
pub fn metrics(thread: &str) -> (Vec<u8>, Vec<u8>) {
    (
        format!("m/r/{thread}/").into_bytes(),
        format!("m/r/{thread}0").into_bytes(),
    )
}

pub const TOTAL: &[u8] = b"m/total";

pub fn answer(event: u64) -> Vec<u8> {
    format!("hk/{event:010}").into_bytes()
}

/// The thread a key of `prefix` names: `tm/<thread>`, `t/<thread>/…`, `m/r/<thread>/…`.
pub fn thread_of(key: &[u8], prefix: &str) -> Option<String> {
    let rest = std::str::from_utf8(key).ok()?.strip_prefix(prefix)?;
    Some(rest.split('/').next()?.to_string())
}

/// The number a key ends with.
pub fn number_of(key: &[u8]) -> Option<u64> {
    std::str::from_utf8(key)
        .ok()?
        .rsplit('/')
        .next()?
        .parse()
        .ok()
}

/// The hash of a fact's content: 32 hex digits.
pub fn hash_of(content: &[u8]) -> String {
    blake3::hash(content).to_hex()[..32].to_string()
}

/// Unix time in milliseconds.
pub fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("after 1970")
        .as_millis() as u64
}
