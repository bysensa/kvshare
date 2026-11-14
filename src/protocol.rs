//! What the owner and its clients say to each other, and how it travels.
//!
//! A message is a frame: its length as a little-endian `u32`, then the message in
//! postcard. The owner's first frame is a [`Welcome`]: the version of the protocol, the
//! canonical path of the database it owns, its pid, and the fingerprint of the options
//! that every process must set alike. A client of another version, or one that meant
//! another database or other options, refuses before its first request.
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
//! transaction's writes. Range and history reads come in pages: each page is a new read
//! of the same transaction.

use std::io;
use std::path::PathBuf;

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncRead, AsyncReadExt as _, AsyncWrite, AsyncWriteExt as _};

use crate::error::KvError;
use crate::types::{WireDurability, WireMode};

/// The version of the protocol. Bump it when a message would mean something else.
pub const VERSION: u32 = 4;

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

/// A transaction's number in the table of its connection, chosen by the client.
pub type TxId = u64;

/// A request's number on its connection; the answer carries it back.
pub type RequestId = u64;

/// A key and its value, as a range read returns them.
pub type Entry = (Vec<u8>, Vec<u8>);

/// What the owner hands a connection when it begins.
#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct Welcome {
    pub(crate) version: u32,
    /// The canonical path of the database the owner holds.
    pub(crate) db: PathBuf,
    pub(crate) owner_pid: u32,
    /// The fingerprint of the options every process must set alike.
    pub(crate) fingerprint: [u8; 32],
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

/// What a client asks the owner. Every request on a transaction carries the writes the
/// client made since its last request, which the owner applies first.
#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum Request {
    /// Begin transaction `tx`. It has no answer: a failure comes back on the next request
    /// on `tx`.
    Begin {
        tx: TxId,
        mode: WireMode,
        durability: WireDurability,
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
    /// Up to `limit` entries of `[start, end)` in key order, or in reverse.
    Range {
        tx: TxId,
        writes: Vec<Write>,
        start: Vec<u8>,
        end: Vec<u8>,
        reverse: bool,
        limit: u32,
    },
    /// Up to `limit` versions of the keys of `[start, end)` that pass `filter`, past the
    /// first `skip` versions of `start`.
    History {
        tx: TxId,
        writes: Vec<Write>,
        start: Vec<u8>,
        end: Vec<u8>,
        skip: u64,
        filter: HistoryFilter,
        limit: u32,
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
}

/// What the owner answers a request.
#[derive(Debug, Serialize, Deserialize)]
pub(crate) enum Answer {
    Got(Option<Vec<u8>>),
    GotMany(Vec<Option<Vec<u8>>>),
    /// A page of a range read, and whether more follow.
    Ranged {
        entries: Vec<Entry>,
        more: bool,
    },
    /// A page of a history read, and whether more follow.
    Versions {
        versions: Vec<Version>,
        more: bool,
    },
    Committed,
    Failed(KvError),
}

/// Write `message` as one frame, in one write.
pub(crate) async fn write_frame<W, T>(writer: &mut W, message: &T) -> io::Result<()>
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
