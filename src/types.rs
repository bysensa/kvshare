//! What crosses the socket besides keys and values, and what kvshare repeats of
//! SurrealKV because SurrealKV keeps it private.

use serde::{Deserialize, Serialize};
use surrealkv::{Durability, IntoBytes, Mode, Options};

use crate::error::{Error, Result};

/// A transaction's mode, as it travels to the owner.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum WireMode {
    ReadWrite,
    ReadOnly,
    WriteOnly,
}

impl WireMode {
    pub(crate) fn of(mode: Mode) -> Result<Self> {
        match mode {
            Mode::ReadWrite => Ok(WireMode::ReadWrite),
            Mode::ReadOnly => Ok(WireMode::ReadOnly),
            Mode::WriteOnly => Ok(WireMode::WriteOnly),
            other => Err(Error::InvalidArgument(format!(
                "the transaction mode {other:?} is not known to kvshare"
            ))),
        }
    }

    pub(crate) fn mode(self) -> Mode {
        match self {
            WireMode::ReadWrite => Mode::ReadWrite,
            WireMode::ReadOnly => Mode::ReadOnly,
            WireMode::WriteOnly => Mode::WriteOnly,
        }
    }

    /// Whether the mode allows writes, as SurrealKV's own check does.
    pub(crate) fn mutable(self) -> bool {
        self != WireMode::ReadOnly
    }
}

/// When a commit returns, as it travels to the owner.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum WireDurability {
    Eventual,
    Immediate,
}

impl WireDurability {
    pub(crate) fn of(durability: Durability) -> Result<Self> {
        match durability {
            Durability::Eventual => Ok(WireDurability::Eventual),
            Durability::Immediate => Ok(WireDurability::Immediate),
            other => Err(Error::InvalidArgument(format!(
                "the durability {other:?} is not known to kvshare"
            ))),
        }
    }

    pub(crate) fn durability(self) -> Durability {
        match self {
            WireDurability::Eventual => Durability::Eventual,
            WireDurability::Immediate => Durability::Immediate,
        }
    }
}

/// Options of a read, as SurrealKV's `ReadOptions`: the bounds of an iteration, the lower
/// one included and the upper one excluded, `None` for no bound.
///
/// kvshare repeats the type because SurrealKV keeps the bounds private, and a client has
/// to send them to the owner.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct ReadOptions {
    pub(crate) lower_bound: Option<Vec<u8>>,
    pub(crate) upper_bound: Option<Vec<u8>>,
}

impl ReadOptions {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn set_iterate_lower_bound(&mut self, bound: Option<Vec<u8>>) {
        self.lower_bound = bound;
    }

    pub fn set_iterate_upper_bound(&mut self, bound: Option<Vec<u8>>) {
        self.upper_bound = bound;
    }

    /// The bounds of `range`: `..`, `a..b`, `a..=b`, `a..`, `..b` or `..=b`.
    pub fn from_range<R: KeyRange>(range: R) -> Self {
        ReadOptions {
            lower_bound: range.start_bound_bytes(),
            upper_bound: range.end_bound_bytes(),
        }
    }
}

/// A range of keys as the bounds of an iteration, as SurrealKV's `KeyRange`: the lower
/// bound included, the upper one excluded, `None` for no bound. An inclusive end becomes
/// the exclusive bound just past it, the key with a zero byte appended.
///
/// kvshare repeats the trait because SurrealKV declares it in a private module.
pub trait KeyRange {
    /// The lower bound, included; `None` if there is none.
    fn start_bound_bytes(&self) -> Option<Vec<u8>>;
    /// The upper bound, excluded; `None` if there is none.
    fn end_bound_bytes(&self) -> Option<Vec<u8>>;
}

impl KeyRange for std::ops::RangeFull {
    fn start_bound_bytes(&self) -> Option<Vec<u8>> {
        None
    }

    fn end_bound_bytes(&self) -> Option<Vec<u8>> {
        None
    }
}

impl<K: IntoBytes> KeyRange for std::ops::Range<K> {
    fn start_bound_bytes(&self) -> Option<Vec<u8>> {
        Some(self.start.as_slice().to_vec())
    }

    fn end_bound_bytes(&self) -> Option<Vec<u8>> {
        Some(self.end.as_slice().to_vec())
    }
}

impl<K: IntoBytes> KeyRange for std::ops::RangeInclusive<K> {
    fn start_bound_bytes(&self) -> Option<Vec<u8>> {
        Some(self.start().as_slice().to_vec())
    }

    fn end_bound_bytes(&self) -> Option<Vec<u8>> {
        Some(just_past(self.end().as_slice()))
    }
}

impl<K: IntoBytes> KeyRange for std::ops::RangeFrom<K> {
    fn start_bound_bytes(&self) -> Option<Vec<u8>> {
        Some(self.start.as_slice().to_vec())
    }

    fn end_bound_bytes(&self) -> Option<Vec<u8>> {
        None
    }
}

impl<K: IntoBytes> KeyRange for std::ops::RangeTo<K> {
    fn start_bound_bytes(&self) -> Option<Vec<u8>> {
        None
    }

    fn end_bound_bytes(&self) -> Option<Vec<u8>> {
        Some(self.end.as_slice().to_vec())
    }
}

impl<K: IntoBytes> KeyRange for std::ops::RangeToInclusive<K> {
    fn start_bound_bytes(&self) -> Option<Vec<u8>> {
        None
    }

    fn end_bound_bytes(&self) -> Option<Vec<u8>> {
        Some(just_past(self.end.as_slice()))
    }
}

impl<R: KeyRange> KeyRange for &R {
    fn start_bound_bytes(&self) -> Option<Vec<u8>> {
        (*self).start_bound_bytes()
    }

    fn end_bound_bytes(&self) -> Option<Vec<u8>> {
        (*self).end_bound_bytes()
    }
}

/// The smallest key greater than `key`.
pub(crate) fn just_past(key: &[u8]) -> Vec<u8> {
    let mut past = Vec::with_capacity(key.len() + 1);
    past.extend_from_slice(key);
    past.push(0);
    past
}

impl From<&ReadOptions> for surrealkv::ReadOptions {
    fn from(options: &ReadOptions) -> Self {
        let mut read = surrealkv::ReadOptions::new();
        read.set_iterate_lower_bound(options.lower_bound.clone());
        read.set_iterate_upper_bound(options.upper_bound.clone());
        read
    }
}

/// The options of a database that every process opening it must set alike, hashed: the
/// versions kept, the comparator, the value log, encryption, and how much an entry may
/// hold, `max_entry`. The other options tune the process that owns the database and may
/// differ from one process to another.
pub(crate) fn fingerprint(options: &Options, max_entry: usize) -> [u8; 32] {
    let agreed = format!(
        "versioning={} retention_ns={} comparator={} vlog={} encrypted={} cipher={:?} entry={max_entry}",
        options.enable_versioning,
        options.versioned_history_retention_ns,
        options.comparator.name(),
        options.enable_vlog,
        options.key_manager.is_some(),
        options.encryption_cipher,
    );
    *blake3::hash(agreed.as_bytes()).as_bytes()
}
