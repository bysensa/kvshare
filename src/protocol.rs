//! What the owner and its clients say to each other, and how it travels.
//!
//! A message is a frame: its length as a little-endian `u32`, then the message in
//! postcard. The owner's first frame is a [`Welcome`]: the versions of the protocol it
//! speaks, from the oldest to the newest, the canonical path of the database it owns,
//! its pid, and the fingerprint of the options that every process must set alike. A
//! client that speaks none of those versions, or one that meant another database or
//! other options, refuses before its first request. Otherwise its first frame is a
//! [`Hello`] that names the version both will speak, the newest they have in common.
//! The welcome and the hello keep their shape in every version, so that a process of
//! any version can read them.
//!
//! Then the client sends `(id, Request)` frames and the owner answers `(id, Answer)`
//! frames with the same id, in any order: requests of several tasks of one process
//! share the connection. [`Request::Begin`] and [`Request::Rollback`] have no answer.
//!
//! A transaction lives in the owner, in the table of the connection that began it, under
//! a number the client chose: when the connection ends, every transaction it left open
//! is rolled back. The owner handles a connection's requests in their order, so a
//! transaction that a client begins without waiting is open, with its snapshot, before
//! the client's next request reaches the owner.
//!
//! A client keeps the writes of its transaction until it needs the owner to have them:
//! every read and the commit carry them, and the owner applies them to the transaction
//! in their order, each by the SurrealKV method the client called, before the read or
//! the commit. A read then is SurrealKV's own read of that transaction, which sees the
//! transaction's writes.
//!
//! A cursor of a client reads in pages. Each page is a new SurrealKV iterator on the same
//! transaction, so the pages see the same snapshot and the same writes; it starts at the
//! first entry, the last, the first at or past a key, or next to an entry of an earlier
//! page, which the page names by its [`Position`].

use std::io;
use std::path::PathBuf;

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncRead, AsyncReadExt as _, AsyncWrite, AsyncWriteExt as _};

use crate::admin::{Admin, Reply};
use crate::error::KvError;
use crate::types::{WireDurability, WireMode};

/// The version of the protocol, the newest this build speaks. Bump it when a message
/// would mean something else.
pub const VERSION: u32 = 8;

/// The oldest version of the protocol this build still speaks. It stays behind
/// [`VERSION`] for as long as a build answers a process of an older one the way that
/// process expects.
pub const OLDEST: u32 = 8;

/// The version to speak with a peer that speaks `oldest` to `newest`: the newest both
/// speak, if there is one.
pub(crate) fn agreed(oldest: u32, newest: u32) -> Option<u32> {
    let spoken = newest.min(VERSION);
    (spoken >= oldest.max(OLDEST)).then_some(spoken)
}

/// How many transactions one connection may hold open at once.
pub const MAX_TRANSACTIONS: usize = 64;

/// The most a key and its value hold together, in bytes: more is refused when it is
/// written, by an owner and by a client alike, so that every entry crosses the socket in
/// one frame whichever process owns the database.
pub const MAX_ENTRY: usize = 64 << 20;

/// The largest frame either side accepts: an entry of the greatest size and room for
/// what travels with it. A longer one is a broken peer.
pub const MAX_FRAME: u32 = (MAX_ENTRY + (1 << 20)) as u32;

/// How many bytes of writes a client's transaction keeps at most before it sends them
/// ahead of its next request, so that a transaction may write more than a frame holds.
pub const WRITES_AHEAD: usize = 4 << 20;

/// How many entries a page of a client's cursor holds unless the cursor asks for another
/// number.
pub const DEFAULT_PAGE: usize = 256;

/// The keys and values a page holds at most before it stops early; a page always holds
/// one entry if there is one, however large.
pub const PAGE_BYTES: usize = 1 << 20;

/// A transaction's number in the table of its connection, chosen by the client.
pub type TxId = u64;

/// A request's number on its connection; the answer carries it back.
pub type RequestId = u64;

/// What the owner hands a connection when it begins. Its shape is the same in every
/// version of the protocol.
#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct Welcome {
    /// The oldest version of the protocol the owner speaks.
    pub(crate) oldest: u32,
    /// The newest.
    pub(crate) version: u32,
    /// The canonical path of the database the owner holds.
    pub(crate) db: PathBuf,
    pub(crate) owner_pid: u32,
    /// The fingerprint of the options every process must set alike.
    pub(crate) fingerprint: [u8; 32],
}

/// What a client answers the welcome with, before its first request: the version of
/// the protocol it will speak, one of the owner's. Its shape is the same in every
/// version.
#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct Hello {
    pub(crate) version: u32,
}

/// A write a transaction makes, one kind for each SurrealKV method, which the owner
/// calls as the client did.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum Write {
    Set {
        key: Vec<u8>,
        value: Vec<u8>,
    },
    SetAt {
        key: Vec<u8>,
        value: Vec<u8>,
        timestamp: u64,
    },
    /// `set_with_options`, with the one option a `WriteOptions` holds.
    SetWithOptions {
        key: Vec<u8>,
        value: Vec<u8>,
        timestamp: Option<u64>,
    },
    /// SurrealKV's `delete`: a hard delete, which takes the key's history with it.
    Delete {
        key: Vec<u8>,
    },
    DeleteWithOptions {
        key: Vec<u8>,
        timestamp: Option<u64>,
    },
    /// SurrealKV's `soft_delete`: a tombstone, after which the key's history stays.
    SoftDelete {
        key: Vec<u8>,
    },
    SoftDeleteWithOptions {
        key: Vec<u8>,
        timestamp: Option<u64>,
    },
    Replace {
        key: Vec<u8>,
        value: Vec<u8>,
    },
    DeleteRange {
        start: Vec<u8>,
        end: Vec<u8>,
    },
    SetSavepoint,
    RollbackToSavepoint,
}

impl Write {
    /// The bytes of the keys and the value the write carries.
    pub(crate) fn bytes(&self) -> usize {
        match self {
            Write::Set { key, value }
            | Write::SetAt { key, value, .. }
            | Write::SetWithOptions { key, value, .. }
            | Write::Replace { key, value } => key.len() + value.len(),
            Write::Delete { key }
            | Write::DeleteWithOptions { key, .. }
            | Write::SoftDelete { key }
            | Write::SoftDeleteWithOptions { key, .. } => key.len(),
            Write::DeleteRange { start, end } => start.len() + end.len(),
            Write::SetSavepoint | Write::RollbackToSavepoint => 0,
        }
    }
}

/// What a cursor reads: SurrealKV's `range_with_options` or `history_with_options`.
///
/// A history's limit is not here: the client spends it, as SurrealKV's iterator does,
/// and the owner reads every page without one.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum Source {
    /// The newest value of each key in `[lower, upper)`, `None` for no bound.
    Range {
        lower: Option<Vec<u8>>,
        upper: Option<Vec<u8>>,
    },
    /// Every version of the keys in `[start, end)`; tombstones or not, and only those
    /// written between two times (nanoseconds, both included) or all.
    History {
        start: Vec<u8>,
        end: Vec<u8>,
        tombstones: bool,
        between: Option<(u64, u64)>,
    },
}

impl Source {
    /// The same source, with a start past its end narrowed to nothing. SurrealKV 1.4
    /// panics on such a range once the transaction holds a write (it is a range of the
    /// transaction's `BTreeMap` of writes); an empty one holds the same keys, none, and
    /// SurrealKV still checks the transaction before it answers with nothing.
    pub(crate) fn narrowed(mut self) -> Self {
        match &mut self {
            Source::Range {
                lower: Some(lower),
                upper: Some(upper),
            } => {
                if *lower > *upper {
                    *upper = lower.clone();
                }
            }
            Source::History { start, end, .. } => {
                if *start > *end {
                    *end = start.clone();
                }
            }
            Source::Range { .. } => {}
        }
        self
    }
}

/// Where a page of a cursor starts, and so which way it reads.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum At {
    /// `seek_first`, then forwards.
    First,
    /// `seek_last`, then backwards.
    Last,
    /// `seek(key)`, then forwards.
    Seek(Vec<u8>),
    /// The entry after this one, then forwards.
    After(Position),
    /// The entry before this one, then backwards.
    Before(Position),
}

impl At {
    /// Whether a page that starts here reads forwards.
    pub(crate) fn forwards(&self) -> bool {
        !matches!(self, At::Last | At::Before(_))
    }
}

/// An entry of a cursor, as a later page can find it again: its key, and which of the
/// key's entries it is, counted from the newest or from the oldest. A range has one entry
/// a key; a history has a version an entry, the newest first.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct Position {
    pub(crate) key: Vec<u8>,
    pub(crate) nth: Nth,
}

/// Which of a key's entries, counted from one end of the key's entries.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum Nth {
    /// The newest is 0.
    FromNewest(u64),
    /// The oldest is 0.
    FromOldest(u64),
}

/// One entry of a page.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct Item {
    /// The key as SurrealKV encodes it: the user's key, then the sequence number and the
    /// kind of the write.
    pub(crate) key: Vec<u8>,
    /// When the version was written: SurrealKV reports it for a history and 0 for a range.
    pub(crate) timestamp: u64,
    pub(crate) nth: Nth,
    pub(crate) value: Fetched,
}

/// The value of an entry of a page.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum Fetched {
    /// What SurrealKV's `value()` answered.
    Value(Vec<u8>),
    /// The error SurrealKV's `value()` answered.
    Failed(KvError),
    /// Not read: the cursor reads keys only.
    Skipped,
}

impl Item {
    /// The user's key, without SurrealKV's trailer.
    pub(crate) fn user_key(&self) -> &[u8] {
        surrealkv::InternalKeyRef::from_encoded(&self.key).user_key()
    }

    pub(crate) fn position(&self) -> Position {
        Position {
            key: self.user_key().to_vec(),
            nth: self.nth,
        }
    }
}

/// What a client asks the owner. Every request on a transaction carries the writes the
/// client made since its last request, which the owner applies first.
#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum Request {
    /// Begin transaction `tx`. It has no answer: a failure comes back on the next request
    /// on `tx`. With `after_commits`, the owner begins it only once the commits the
    /// connection sent before it are done: the client stopped waiting for one of them.
    Begin {
        tx: TxId,
        mode: WireMode,
        durability: WireDurability,
        after_commits: bool,
    },
    /// `get`, or `get_for_update`.
    Get {
        tx: TxId,
        writes: Vec<Write>,
        key: Vec<u8>,
        for_update: bool,
    },
    GetMany {
        tx: TxId,
        writes: Vec<Write>,
        keys: Vec<Vec<u8>>,
    },
    GetAt {
        tx: TxId,
        writes: Vec<Write>,
        key: Vec<u8>,
        timestamp: u64,
    },
    GetManyAt {
        tx: TxId,
        writes: Vec<Write>,
        keys: Vec<Vec<u8>>,
        timestamp: u64,
    },
    /// A page of up to `limit` entries of `source`, from `at`; with their values, or
    /// with keys only.
    Cursor {
        tx: TxId,
        writes: Vec<Write>,
        source: Source,
        at: At,
        limit: u32,
        values: bool,
    },
    /// Commit transaction `tx`, whose commit returns as `durability` says. The
    /// transaction ends whatever the answer.
    Commit {
        tx: TxId,
        writes: Vec<Write>,
        durability: WireDurability,
    },
    /// Roll transaction `tx` back. It has no answer.
    Rollback { tx: TxId },
    /// An operation on the whole database.
    Admin(Admin),
    /// Writes of transaction `tx` sent ahead of its next request, which the owner
    /// applies in their order. It has no answer: why they could not be applied comes
    /// back on the next request on `tx`.
    Writes { tx: TxId, writes: Vec<Write> },
}

impl Request {
    /// Take the writes the request carries and leave it none: a request that is not
    /// sent gives them back to its transaction.
    pub(crate) fn take_writes(&mut self) -> Vec<Write> {
        match self {
            Request::Get { writes, .. }
            | Request::GetMany { writes, .. }
            | Request::GetAt { writes, .. }
            | Request::GetManyAt { writes, .. }
            | Request::Cursor { writes, .. }
            | Request::Commit { writes, .. }
            | Request::Writes { writes, .. } => std::mem::take(writes),
            Request::Begin { .. } | Request::Rollback { .. } | Request::Admin(_) => Vec::new(),
        }
    }
}

/// What the owner answers a request.
#[derive(Debug, Serialize, Deserialize)]
pub(crate) enum Answer {
    Got(Option<Vec<u8>>),
    GotMany(Vec<Option<Vec<u8>>>),
    /// A page of a cursor, in the order it was read, and whether more follow that way.
    Page {
        items: Vec<Item>,
        more: bool,
    },
    Committed,
    /// What an operation on the whole database answered.
    Admin(Reply),
    Failed(KvError),
}

/// Write `message` as one frame, in one write.
pub(crate) async fn write_frame<W, T>(writer: &mut W, message: &T) -> io::Result<()>
where
    W: AsyncWrite + Unpin,
    T: Serialize,
{
    writer.write_all(&encode_frame(message)?).await
}

/// Why a message did not become a frame.
#[derive(Debug)]
pub(crate) enum Unframed {
    /// It is longer than [`MAX_FRAME`].
    TooLong,
    Encoding(postcard::Error),
}

impl From<Unframed> for io::Error {
    fn from(unframed: Unframed) -> Self {
        match unframed {
            Unframed::TooLong => {
                io::Error::new(io::ErrorKind::InvalidInput, "the message is too long")
            }
            Unframed::Encoding(error) => io::Error::other(error),
        }
    }
}

/// `message` as one frame: its length, then the message.
pub(crate) fn encode_frame<T: Serialize>(message: &T) -> Result<Vec<u8>, Unframed> {
    let mut frame = postcard::to_extend(message, vec![0u8; 4]).map_err(Unframed::Encoding)?;
    let length = u32::try_from(frame.len() - 4)
        .ok()
        .filter(|length| *length <= MAX_FRAME)
        .ok_or(Unframed::TooLong)?;
    frame[..4].copy_from_slice(&length.to_le_bytes());
    Ok(frame)
}

/// Read one frame and decode its message.
pub(crate) async fn read_frame<R, T>(reader: &mut R) -> io::Result<T>
where
    R: AsyncRead + Unpin,
    T: DeserializeOwned,
{
    let length = reader.read_u32_le().await?;
    if length > MAX_FRAME {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("a frame of {length} bytes is longer than {MAX_FRAME}"),
        ));
    }
    let mut body = vec![0u8; length as usize];
    reader.read_exact(&mut body).await?;
    postcard::from_bytes(&body).map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A frame read back is the message written, and a frame that claims to be longer
    /// than the limit is refused before its body is read.
    #[tokio::test]
    async fn a_frame_carries_its_message_and_a_huge_one_is_refused() {
        let request = Request::Commit {
            tx: 7,
            durability: WireDurability::Immediate,
            writes: vec![
                Write::Set {
                    key: b"k".to_vec(),
                    value: vec![0; 300],
                },
                Write::DeleteRange {
                    start: b"a".to_vec(),
                    end: b"b".to_vec(),
                },
                Write::SetSavepoint,
            ],
        };
        let mut wire = Vec::new();
        write_frame(&mut wire, &(9u64, &request))
            .await
            .expect("a frame");
        let (id, read): (RequestId, Request) = read_frame(&mut wire.as_slice())
            .await
            .expect("the frame back");
        assert_eq!(id, 9);
        assert_eq!(read, request);

        let huge = (MAX_FRAME + 1).to_le_bytes();
        let error = read_frame::<_, Request>(&mut huge.as_slice())
            .await
            .expect_err("too long");
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    }
}
