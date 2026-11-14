//! What the owner and its clients say to each other, and how it travels.
//!
//! A message is a frame: its length as a little-endian `u32`, then the message in
//! postcard. The owner's first frame is a [`Welcome`]: the version of the protocol, the
//! canonical path of the database it owns, and its pid. A client of another version, or
//! one that meant another database, refuses before its first request.
//!
//! Then the client sends `(id, Request)` frames and the owner answers `(id, Answer)`
//! frames with the same id, in any order: requests of several tasks of one process
//! share the connection. [`Request::Rollback`] has no answer.
//!
//! A transaction lives in the owner, in the table of the connection that began it:
//! when the connection ends, every transaction it left open is rolled back.
//!
//! A client keeps the writes of its transaction until it needs the owner to have them:
//! a commit, a range read and a history read carry them, and the owner applies them to
//! the transaction in their order before anything else. A range or history read then
//! is SurrealKV's own read of that transaction, which sees the transaction's writes.
//! Such reads come in pages: each page is a new read of the same transaction.

use std::io;
use std::path::PathBuf;

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncRead, AsyncReadExt as _, AsyncWrite, AsyncWriteExt as _};

/// The version of the protocol. Bump it when a message would mean something else.
pub const VERSION: u32 = 3;

/// How many transactions one connection may hold open at once.
pub const MAX_TRANSACTIONS: usize = 64;

/// The largest frame either side accepts: a longer one is a broken peer.
pub const MAX_FRAME: u32 = 64 << 20;

/// How many entries or versions a page of a range or history read holds unless the
/// reader asks for another number.
pub const DEFAULT_PAGE: usize = 256;

/// The keys and values a page holds at most before it stops early; a page always holds
/// one entry if there is one, however large.
pub const PAGE_BYTES: usize = 1 << 20;

/// A transaction's number in the table of its connection.
pub type TxId = u64;

/// A request's number on its connection; the answer carries it back.
pub type RequestId = u64;

/// A key and its value, as a range read returns them.
pub type Entry = (Vec<u8>, Vec<u8>);

/// When a commit returns, as SurrealKV's `Durability`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Durability {
    /// The commit is in the operating system's page cache: it survives the death of
    /// the process, not a power cut.
    Eventual,
    /// The commit waits for a sync of the write-ahead log.
    Immediate,
}

impl From<Durability> for surrealkv::Durability {
    fn from(durability: Durability) -> Self {
        match durability {
            Durability::Eventual => surrealkv::Durability::Eventual,
            Durability::Immediate => surrealkv::Durability::Immediate,
        }
    }
}

/// What the owner hands a connection when it begins.
#[derive(Debug, Serialize, Deserialize)]
pub struct Welcome {
    pub version: u32,
    /// The canonical path of the database the owner holds.
    pub db: PathBuf,
    pub owner_pid: u32,
}

/// A write a transaction makes; the client keeps them until the owner needs them.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum Write {
    Set {
        key: Vec<u8>,
        value: Vec<u8>,
    },
    /// SurrealKV's `delete`: a hard delete, which takes the key's history with it.
    Delete {
        key: Vec<u8>,
    },
    /// SurrealKV's `soft_delete`: a tombstone, after which the key's history stays.
    SoftDelete {
        key: Vec<u8>,
    },
}

impl Write {
    /// The key the write is to.
    pub fn key(&self) -> &[u8] {
        match self {
            Write::Set { key, .. } | Write::Delete { key } | Write::SoftDelete { key } => key,
        }
    }
}

/// Which versions a history read returns besides the values: tombstones or not, and only
/// those written between two times (nanoseconds, both included) or all.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct HistoryFilter {
    pub tombstones: bool,
    pub between: Option<(u64, u64)>,
}

/// One version of a key in a history read.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Version {
    pub key: Vec<u8>,
    /// When it was written, in nanoseconds; `u64::MAX` for a write of the reading
    /// transaction itself, which has no time until it commits.
    pub timestamp: u64,
    /// The value, or `None` for a tombstone.
    pub value: Option<Vec<u8>>,
}

/// What a client asks the owner.
#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Request {
    /// The value of `key` in transaction `tx`; with `tx: None`, in a transaction that
    /// begins now, whose number the answer carries.
    Get { tx: Option<TxId>, key: Vec<u8> },
    /// The value of `key` as of `timestamp` (nanoseconds), in a read of its own.
    GetAt { key: Vec<u8>, timestamp: u64 },
    /// Apply `writes` to transaction `tx` (or one that begins now), then read up to
    /// `limit` entries of `[start, end)` in key order, or in reverse.
    Range {
        tx: Option<TxId>,
        writes: Vec<Write>,
        start: Vec<u8>,
        end: Vec<u8>,
        reverse: bool,
        limit: u32,
    },
    /// Apply `writes` to transaction `tx` (or one that begins now), then read up to
    /// `limit` versions of the keys of `[start, end)` that pass `filter`, past the first
    /// `skip` versions of `start`.
    History {
        tx: Option<TxId>,
        writes: Vec<Write>,
        start: Vec<u8>,
        end: Vec<u8>,
        skip: u64,
        filter: HistoryFilter,
        limit: u32,
    },
    /// Apply `writes` to transaction `tx` and commit it; with `tx: None`, in a
    /// transaction that begins now. The transaction ends whatever the answer.
    Commit {
        tx: Option<TxId>,
        durability: Durability,
        writes: Vec<Write>,
    },
    /// Roll transaction `tx` back. It has no answer.
    Rollback { tx: TxId },
}

/// What the owner answers a request.
#[derive(Debug, Serialize, Deserialize)]
pub enum Answer {
    Got {
        tx: TxId,
        value: Option<Vec<u8>>,
    },
    GotAt(Option<Vec<u8>>),
    /// A page of a range read, and whether more follow.
    Ranged {
        tx: TxId,
        entries: Vec<Entry>,
        more: bool,
    },
    /// A page of a history read, and whether more follow.
    Versions {
        tx: TxId,
        versions: Vec<Version>,
        more: bool,
    },
    Committed,
    Failed(KvError),
}

/// Why the owner refused a request.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum KvError {
    /// Another transaction committed a write to a key this one wrote.
    Conflict,
    /// The connection holds no transaction with this number.
    NoSuchTransaction(TxId),
    /// The connection already holds [`MAX_TRANSACTIONS`] open transactions.
    TooManyTransactions,
    /// SurrealKV refused the operation.
    Engine(String),
}

impl From<surrealkv::Error> for KvError {
    fn from(error: surrealkv::Error) -> Self {
        match error {
            surrealkv::Error::TransactionWriteConflict => KvError::Conflict,
            other => KvError::Engine(other.to_string()),
        }
    }
}

/// Write `message` as one frame, in one write.
pub async fn write_frame<W, T>(writer: &mut W, message: &T) -> io::Result<()>
where
    W: AsyncWrite + Unpin,
    T: Serialize,
{
    let mut frame = postcard::to_extend(message, vec![0u8; 4]).map_err(io::Error::other)?;
    let length = u32::try_from(frame.len() - 4)
        .ok()
        .filter(|length| *length <= MAX_FRAME)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "the message is too long"))?;
    frame[..4].copy_from_slice(&length.to_le_bytes());
    writer.write_all(&frame).await
}

/// Read one frame and decode its message.
pub async fn read_frame<R, T>(reader: &mut R) -> io::Result<T>
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
            tx: Some(7),
            durability: Durability::Immediate,
            writes: vec![
                Write::Set {
                    key: b"k".to_vec(),
                    value: vec![0; 300],
                },
                Write::Delete {
                    key: b"gone".to_vec(),
                },
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
