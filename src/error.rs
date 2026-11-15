//! Errors: SurrealKV's own, by the names SurrealKV gives them, and the errors of
//! reaching the owner.
//!
//! An owner translates a SurrealKV error into [`KvError`] for its client, and the client
//! turns it into an [`Error`]; the owner's own transactions turn their SurrealKV errors
//! into an [`Error`] through the same [`KvError`]. So the owner and its clients see the
//! same error for the same operation.

use std::fmt;
use std::io;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use surrealkv::WriteStallReason;

use crate::protocol::{TxId, Unframed};

/// Why an operation failed.
///
/// kvshare may add errors: a `match` on an `Error` outside this crate needs an arm for
/// the ones it does not name.
///
/// ```compile_fail
/// fn name(error: kvshare::Error) -> &'static str {
///     use kvshare::Error::*;
///     match error {
///         TransactionWriteConflict | TransactionRetry | TransactionClosed => "transaction",
///         TransactionReadOnly | TransactionWriteOnly | TransactionWithoutSavepoint => "mode",
///         EmptyKey | KeyNotFound | InvalidArgument(_) | TooLarge(_) => "argument",
///         WriteStall { .. } | Engine(_) | TooManyTransactions => "engine",
///         OwnerLost(_) | OwnerSilent { .. } | NoOwner { .. } => "owner",
///         Handshake(_) | VersionMismatch { .. } | OptionsMismatch | Io(_) => "meeting",
///     }
/// }
/// ```
#[derive(Debug)]
#[non_exhaustive]
pub enum Error {
    /// Another transaction committed a write to a key this one wrote first.
    TransactionWriteConflict,
    TransactionRetry,
    /// The transaction has committed or rolled back.
    TransactionClosed,
    /// A write, a savepoint or a commit in a read-only transaction.
    TransactionReadOnly,
    /// A read in a write-only transaction.
    TransactionWriteOnly,
    /// `rollback_to_savepoint` with no savepoint set.
    TransactionWithoutSavepoint,
    EmptyKey,
    KeyNotFound,
    InvalidArgument(String),
    /// SurrealKV held the write back: too much waits to be flushed or compacted.
    WriteStall {
        reason: WriteStallReason,
    },
    /// Any other SurrealKV error, as SurrealKV words it.
    Engine(String),
    /// A key with its value holds more than [`MAX_ENTRY`](crate::MAX_ENTRY) bytes, or a
    /// request or its answer is larger than one frame of the owner's socket. Nothing
    /// was written or sent, and the transaction works on.
    TooLarge(String),
    /// The owner already holds as many open transactions of this process as it allows.
    TooManyTransactions,
    /// The owner went away during the operation; for a commit, whether it happened is
    /// unknown. The next transaction elects a new owner.
    OwnerLost(String),
    /// The owner holds the database but did not answer in time. Its lock is held, so
    /// nobody can take its place until it exits.
    OwnerSilent {
        pid: u32,
        waited: Duration,
    },
    /// No process became the owner, nor answered as one, in time.
    NoOwner {
        waited: Duration,
    },
    /// The owner refused the connection, or answered out of turn.
    Handshake(String),
    /// The owner and this process speak no version of the protocol in common: `owner`
    /// is the owner's version nearest to `this`, the newest this process speaks.
    VersionMismatch {
        owner: u32,
        this: u32,
    },
    /// The owner opened the database with options that must agree among its processes
    /// (versions kept, comparator, value log, encryption) set otherwise.
    OptionsMismatch,
    Io(io::Error),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::TransactionWriteConflict => write!(f, "transaction write conflict"),
            Error::TransactionRetry => write!(f, "transaction must be retried"),
            Error::TransactionClosed => write!(f, "transaction is closed"),
            Error::TransactionReadOnly => write!(f, "transaction is read-only"),
            Error::TransactionWriteOnly => write!(f, "transaction is write-only"),
            Error::TransactionWithoutSavepoint => write!(f, "transaction has no savepoint"),
            Error::EmptyKey => write!(f, "empty key"),
            Error::KeyNotFound => write!(f, "key not found"),
            Error::InvalidArgument(reason) => write!(f, "invalid argument: {reason}"),
            Error::WriteStall { reason } => write!(f, "write stall: {reason:?}"),
            Error::Engine(reason) => write!(f, "SurrealKV: {reason}"),
            Error::TooLarge(what) => write!(f, "too large: {what}"),
            Error::TooManyTransactions => write!(f, "too many open transactions"),
            Error::OwnerLost(reason) => write!(f, "the owner went away: {reason}"),
            Error::OwnerSilent { pid, waited } => {
                write!(f, "the owner (pid {pid}) did not answer within {waited:?}")
            }
            Error::NoOwner { waited } => write!(f, "no owner answered within {waited:?}"),
            Error::Handshake(reason) => write!(f, "the owner refused the connection: {reason}"),
            Error::VersionMismatch { owner, this } => {
                write!(
                    f,
                    "the owner speaks protocol v{owner}, this process v{this}"
                )
            }
            Error::OptionsMismatch => write!(
                f,
                "the owner opened the database with other versioning, comparator, value log or encryption options"
            ),
            Error::Io(error) => write!(f, "{error}"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::Io(error) => Some(error),
            _ => None,
        }
    }
}

/// An error as it travels from the owner to a client.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum KvError {
    TransactionWriteConflict,
    TransactionRetry,
    TransactionClosed,
    TransactionReadOnly,
    TransactionWriteOnly,
    TransactionWithoutSavepoint,
    EmptyKey,
    KeyNotFound,
    InvalidArgument(String),
    /// A write stall, its reason as a number.
    WriteStall(u8),
    Engine(String),
    TooManyTransactions,
    /// The connection holds no transaction with this number: a client out of step with
    /// its owner.
    NoSuchTransaction(TxId),
    /// The answer is larger than one frame.
    TooLarge,
}

impl From<surrealkv::Error> for KvError {
    fn from(error: surrealkv::Error) -> Self {
        match error {
            surrealkv::Error::TransactionWriteConflict => KvError::TransactionWriteConflict,
            surrealkv::Error::TransactionRetry => KvError::TransactionRetry,
            surrealkv::Error::TransactionClosed => KvError::TransactionClosed,
            surrealkv::Error::TransactionReadOnly => KvError::TransactionReadOnly,
            surrealkv::Error::TransactionWriteOnly => KvError::TransactionWriteOnly,
            surrealkv::Error::TransactionWithoutSavepoint => KvError::TransactionWithoutSavepoint,
            surrealkv::Error::EmptyKey => KvError::EmptyKey,
            surrealkv::Error::KeyNotFound => KvError::KeyNotFound,
            surrealkv::Error::InvalidArgument(reason) => KvError::InvalidArgument(reason),
            surrealkv::Error::WriteStall {
                reason: WriteStallReason::MemtableLimit,
            } => KvError::WriteStall(0),
            surrealkv::Error::WriteStall {
                reason: WriteStallReason::L0FileLimit,
            } => KvError::WriteStall(1),
            other => KvError::Engine(other.to_string()),
        }
    }
}

impl From<KvError> for Error {
    fn from(error: KvError) -> Self {
        match error {
            KvError::TransactionWriteConflict => Error::TransactionWriteConflict,
            KvError::TransactionRetry => Error::TransactionRetry,
            KvError::TransactionClosed => Error::TransactionClosed,
            KvError::TransactionReadOnly => Error::TransactionReadOnly,
            KvError::TransactionWriteOnly => Error::TransactionWriteOnly,
            KvError::TransactionWithoutSavepoint => Error::TransactionWithoutSavepoint,
            KvError::EmptyKey => Error::EmptyKey,
            KvError::KeyNotFound => Error::KeyNotFound,
            KvError::InvalidArgument(reason) => Error::InvalidArgument(reason),
            KvError::WriteStall(0) => Error::WriteStall {
                reason: WriteStallReason::MemtableLimit,
            },
            KvError::WriteStall(1) => Error::WriteStall {
                reason: WriteStallReason::L0FileLimit,
            },
            KvError::WriteStall(other) => {
                Error::Handshake(format!("the owner sent {other} for a write stall's reason"))
            }
            KvError::Engine(reason) => Error::Engine(reason),
            KvError::TooManyTransactions => Error::TooManyTransactions,
            KvError::NoSuchTransaction(tx) => {
                Error::Handshake(format!("the owner holds no transaction {tx}"))
            }
            KvError::TooLarge => {
                Error::TooLarge("the answer does not fit one frame of the owner's socket".into())
            }
        }
    }
}

impl From<Unframed> for Error {
    fn from(unframed: Unframed) -> Self {
        match unframed {
            Unframed::TooLong => {
                Error::TooLarge("the request does not fit one frame of the owner's socket".into())
            }
            Unframed::Encoding(error) => {
                Error::InvalidArgument(format!("the request could not be encoded: {error}"))
            }
        }
    }
}

impl From<surrealkv::Error> for Error {
    fn from(error: surrealkv::Error) -> Self {
        KvError::from(error).into()
    }
}

/// The result of an operation of kvshare.
pub type Result<T> = std::result::Result<T, Error>;

#[cfg(test)]
mod tests {
    use super::*;

    /// An error of the operating system is the source of the `Io` that carries it; the
    /// other errors have none.
    #[test]
    fn an_io_error_is_the_source_of_its_error() {
        use std::error::Error as _;
        let error = Error::Io(io::Error::other("the disk"));
        assert_eq!(
            error.source().map(ToString::to_string).as_deref(),
            Some("the disk")
        );
        assert!(Error::EmptyKey.source().is_none());
    }

    /// A write stall crosses the socket as the variant and the reason SurrealKV gave.
    #[test]
    fn a_write_stall_keeps_its_reason_across_the_socket() {
        for reason in [
            WriteStallReason::MemtableLimit,
            WriteStallReason::L0FileLimit,
        ] {
            let sent = KvError::from(surrealkv::Error::WriteStall { reason });
            let wire = postcard::to_allocvec(&sent).expect("encoded");
            let received: KvError = postcard::from_bytes(&wire).expect("decoded");
            match Error::from(received) {
                Error::WriteStall { reason: got } => assert_eq!(got, reason),
                other => panic!("a write stall became {other:?}"),
            }
        }
    }
}
