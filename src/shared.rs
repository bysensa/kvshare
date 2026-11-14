//! What a process uses: a shared database, and transactions that run in this process
//! when it owns the database, or in the owner when it does not. Who owns it is decided
//! in `election`.
//!
//! A client's transaction costs as few round trips as it can. `begin` asks nothing;
//! the first read begins the transaction in the owner; writes are kept in the client
//! and travel with the commit; a rollback is sent without waiting. A transaction that
//! reads nothing therefore takes its snapshot when it commits: its writes do not
//! conflict with commits made since `begin`, which snapshot isolation allows, since
//! the transaction saw none of the data they changed.

use std::collections::HashMap;
use std::future::Future;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::time::Duration;

use surrealkv::{Mode, Transaction, Tree};

use crate::client::{Connection, Lost};
use crate::election::{Current, Role};
use crate::endpoint::Endpoint;
use crate::exec::{apply, history_page, range_page};
use crate::protocol::{
    Answer, Durability, Entry, HistoryFilter, KvError, Request, TxId, Version, Write,
};
use crate::scan::{History, Scan};

/// How a process opens a shared database.
#[derive(Debug, Clone)]
pub struct Options {
    /// The directory of the owners' sockets.
    pub run_dir: PathBuf,
    /// Keep versions for this many nanoseconds (0: for ever), or keep none.
    pub versioning: Option<u64>,
    /// How long an election may take before it gives up.
    pub elect_within: Duration,
    /// How long a request may wait for the owner's answer.
    pub call_within: Duration,
}

impl Options {
    /// Sockets in `run_dir`, every version kept, five seconds for an election and a call.
    pub fn new(run_dir: impl Into<PathBuf>) -> Self {
        Options {
            run_dir: run_dir.into(),
            versioning: Some(0),
            elect_within: Duration::from_secs(5),
            call_within: Duration::from_secs(5),
        }
    }
}

/// Why an operation on the shared database failed.
#[derive(Debug)]
pub enum Error {
    /// Another transaction committed a write to a key this one wrote.
    Conflict,
    /// The owner went away during the request; for a commit, whether it happened is
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
    /// The owner speaks another version, or holds another database.
    Handshake(String),
    /// SurrealKV refused the operation, or the owner answered out of turn.
    Engine(String),
    Io(io::Error),
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::Conflict => write!(f, "another transaction wrote the same key first"),
            Error::OwnerLost(reason) => write!(f, "the owner went away during the call: {reason}"),
            Error::OwnerSilent { pid, waited } => {
                write!(f, "the owner (pid {pid}) did not answer within {waited:?}")
            }
            Error::NoOwner { waited } => write!(f, "no owner answered within {waited:?}"),
            Error::Handshake(reason) => write!(f, "the owner refused the connection: {reason}"),
            Error::Engine(reason) => write!(f, "SurrealKV: {reason}"),
            Error::Io(error) => write!(f, "{error}"),
        }
    }
}

impl std::error::Error for Error {}

impl From<surrealkv::Error> for Error {
    fn from(error: surrealkv::Error) -> Self {
        match error {
            surrealkv::Error::TransactionWriteConflict => Error::Conflict,
            other => Error::Engine(other.to_string()),
        }
    }
}

impl From<KvError> for Error {
    fn from(error: KvError) -> Self {
        match error {
            KvError::Conflict => Error::Conflict,
            KvError::NoSuchTransaction(tx) => Error::Engine(format!("no transaction {tx}")),
            KvError::TooManyTransactions => Error::Engine("too many open transactions".into()),
            KvError::Engine(reason) => Error::Engine(reason),
        }
    }
}

/// Where an operation goes: the tree of this process, or the owner's connection.
enum Route {
    Local {
        tree: Arc<Tree>,
        generation: u64,
    },
    Remote {
        connection: Arc<Connection>,
        owner_pid: u32,
        generation: u64,
    },
}

/// One database shared with the other processes that open it.
pub struct Shared {
    pub(crate) endpoint: Endpoint,
    pub(crate) options: Options,
    pub(crate) generations: AtomicU64,
    pub(crate) current: tokio::sync::Mutex<Option<Current>>,
}

impl Shared {
    /// The database at `db`. Nothing is opened until the first transaction.
    pub fn new(db: &Path, options: Options) -> io::Result<Self> {
        Ok(Shared {
            endpoint: Endpoint::new(db, &options.run_dir)?,
            options,
            generations: AtomicU64::new(0),
            current: tokio::sync::Mutex::new(None),
        })
    }

    pub fn endpoint(&self) -> &Endpoint {
        &self.endpoint
    }

    /// The pid of the owner this process works through, or its own if it owns the
    /// database, after an election if none was held.
    pub async fn owner_pid(&self) -> Result<u32, Error> {
        Ok(match self.route().await? {
            Route::Local { .. } => std::process::id(),
            Route::Remote { owner_pid, .. } => owner_pid,
        })
    }

    /// Begin a read-write transaction whose commit returns as `durability` says. In a
    /// client it asks the owner nothing: the first read or the commit does.
    pub async fn begin(&self, durability: Durability) -> Result<Tx<'_>, Error> {
        let (generation, open) = match self.route().await? {
            Route::Local { tree, generation } => {
                let mut transaction = tree.begin()?;
                transaction.set_durability(durability.into());
                (generation, Open::Local(transaction))
            }
            Route::Remote {
                connection,
                owner_pid,
                generation,
            } => {
                let remote = Remote {
                    connection,
                    owner_pid,
                    durability,
                    tx: None,
                    pending: Pending::default(),
                    flushed: false,
                };
                (generation, Open::Remote(remote))
            }
        };
        Ok(Tx {
            shared: self,
            generation,
            open: Some(open),
        })
    }

    /// The value of `key` as of `timestamp` (nanoseconds), in a read of its own.
    pub async fn get_at(&self, key: &[u8], timestamp: u64) -> Result<Option<Vec<u8>>, Error> {
        match self.route().await? {
            Route::Local { tree, .. } => {
                let transaction = tree.begin_with_mode(Mode::ReadOnly)?;
                Ok(transaction.get_at(key, timestamp)?)
            }
            Route::Remote {
                connection,
                owner_pid,
                generation,
            } => {
                let request = Request::GetAt {
                    key: key.to_vec(),
                    timestamp,
                };
                match self
                    .call(generation, owner_pid, connection.call(request))
                    .await?
                {
                    Answer::GotAt(value) => Ok(value),
                    other => Err(out_of_turn(other)),
                }
            }
        }
    }

    /// Let the database go: an owner hands it over, a client disconnects.
    pub async fn close(self) -> Result<(), Error> {
        match self.current.into_inner() {
            Some(Current {
                role: Role::Owner(owner),
                ..
            }) => Ok(owner.close().await?),
            _ => Ok(()),
        }
    }

    /// Where an operation goes, after an election if no role is held. The role is not
    /// borrowed past the return, so a failed request can forget it.
    async fn route(&self) -> Result<Route, Error> {
        let mut current = self.current.lock().await;
        let current = self.held(&mut current).await?;
        Ok(match &current.role {
            Role::Owner(owner) => Route::Local {
                tree: Arc::clone(owner.tree()),
                generation: current.generation,
            },
            Role::Client {
                connection,
                owner_pid,
            } => Route::Remote {
                connection: Arc::clone(connection),
                owner_pid: *owner_pid,
                generation: current.generation,
            },
        })
    }

    /// Wait for the answer to a request; a request the owner never answered forgets
    /// the owner, and a refusal becomes the error it names.
    async fn call(
        &self,
        generation: u64,
        owner_pid: u32,
        call: impl Future<Output = Result<Answer, Lost>>,
    ) -> Result<Answer, Error> {
        match tokio::time::timeout(self.options.call_within, call).await {
            Err(_) => Err(Error::OwnerSilent {
                pid: owner_pid,
                waited: self.options.call_within,
            }),
            Ok(Err(Lost(reason))) => {
                self.forget(generation).await;
                Err(Error::OwnerLost(reason))
            }
            Ok(Ok(Answer::Failed(refusal))) => Err(refusal.into()),
            Ok(Ok(answer)) => Ok(answer),
        }
    }
}

/// A transaction: in this process when it owns the database, in the owner otherwise.
///
/// A transaction dropped without a commit is rolled back.
pub struct Tx<'a> {
    shared: &'a Shared,
    generation: u64,
    open: Option<Open>,
}

enum Open {
    Local(Transaction),
    Remote(Remote),
}

/// A transaction in the owner, as the client keeps it.
struct Remote {
    connection: Arc<Connection>,
    owner_pid: u32,
    durability: Durability,
    /// The owner's number of the transaction, once a read has begun it there.
    tx: Option<TxId>,
    /// The writes the owner has not had yet.
    pending: Pending,
    /// Whether writes went to the owner with a read: then the commit must reach it even
    /// when nothing is pending.
    flushed: bool,
}

/// The writes of a remote transaction that the owner has not had yet, in their order,
/// and where the last write of each key is, for the transaction's own point reads.
///
/// They go to the owner in this order, with a commit or with a range or history read,
/// and SurrealKV decides there what a write leaves of the earlier writes of its key.
#[derive(Default)]
struct Pending {
    writes: Vec<Write>,
    last: HashMap<Vec<u8>, usize>,
}

impl Pending {
    fn push(&mut self, write: Write) {
        self.last.insert(write.key().to_vec(), self.writes.len());
        self.writes.push(write);
    }

    /// What the transaction's own writes say of `key`: its value, or `Some(None)` if
    /// they deleted it; `None` if they did not touch it.
    fn read(&self, key: &[u8]) -> Option<Option<Vec<u8>>> {
        let at = *self.last.get(key)?;
        Some(match &self.writes[at] {
            Write::Set { value, .. } => Some(value.clone()),
            Write::Delete { .. } | Write::SoftDelete { .. } => None,
        })
    }

    /// The writes, in their order, leaving none.
    fn take(&mut self) -> Vec<Write> {
        self.last.clear();
        std::mem::take(&mut self.writes)
    }
}

impl<'a> Tx<'a> {
    /// Whether the transaction runs in this process, which owns the database.
    pub fn is_local(&self) -> bool {
        matches!(self.open, Some(Open::Local(_)))
    }

    /// The value of `key` as this transaction sees it, its own writes included.
    pub async fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>, Error> {
        let (shared, generation) = (self.shared, self.generation);
        match self
            .open
            .as_mut()
            .expect("a transaction is open until it ends")
        {
            Open::Local(transaction) => Ok(transaction.get(key)?),
            Open::Remote(remote) => {
                if let Some(written) = remote.pending.read(key) {
                    return Ok(written);
                }
                let request = Request::Get {
                    tx: remote.tx,
                    key: key.to_vec(),
                };
                let call = remote.connection.call(request);
                match shared.call(generation, remote.owner_pid, call).await? {
                    Answer::Got { tx, value } => {
                        remote.tx = Some(tx);
                        Ok(value)
                    }
                    other => Err(out_of_turn(other)),
                }
            }
        }
    }

    pub async fn set(&mut self, key: &[u8], value: &[u8]) -> Result<(), Error> {
        self.write(Write::Set {
            key: key.to_vec(),
            value: value.to_vec(),
        })
    }

    /// A hard delete, as SurrealKV's `delete`: the key's history goes with it.
    pub async fn delete(&mut self, key: &[u8]) -> Result<(), Error> {
        self.write(Write::Delete { key: key.to_vec() })
    }

    /// A tombstone, as SurrealKV's `soft_delete`: the key's history stays.
    pub async fn soft_delete(&mut self, key: &[u8]) -> Result<(), Error> {
        self.write(Write::SoftDelete { key: key.to_vec() })
    }

    /// The entries of `[start, end)` as this transaction sees them, its own writes
    /// included, read in pages.
    pub fn range(&mut self, start: &[u8], end: &[u8]) -> Scan<'_, 'a> {
        Scan::new(self, start, end)
    }

    /// The versions of the keys of `[start, end)`, keys in order and the versions of a
    /// key newest first, read in pages. The tree must keep versions.
    pub fn history(&mut self, start: &[u8], end: &[u8]) -> History<'_, 'a> {
        History::new(self, start, end)
    }

    /// Make `write` in this process's transaction, or keep it for the owner.
    fn write(&mut self, write: Write) -> Result<(), Error> {
        match self
            .open
            .as_mut()
            .expect("a transaction is open until it ends")
        {
            Open::Local(transaction) => Ok(apply(transaction, vec![write])?),
            Open::Remote(remote) => {
                remote.pending.push(write);
                Ok(())
            }
        }
    }

    /// A page of a range read; a remote transaction sends its pending writes with it.
    pub(crate) async fn range_page(
        &mut self,
        start: &[u8],
        end: &[u8],
        reverse: bool,
        limit: usize,
    ) -> Result<(Vec<Entry>, bool), Error> {
        let (shared, generation) = (self.shared, self.generation);
        match self
            .open
            .as_mut()
            .expect("a transaction is open until it ends")
        {
            Open::Local(transaction) => Ok(range_page(transaction, start, end, reverse, limit)?),
            Open::Remote(remote) => {
                let writes = remote.pending.take();
                remote.flushed |= !writes.is_empty();
                let request = Request::Range {
                    tx: remote.tx,
                    writes,
                    start: start.to_vec(),
                    end: end.to_vec(),
                    reverse,
                    limit: u32::try_from(limit).unwrap_or(u32::MAX),
                };
                let call = remote.connection.call(request);
                match shared.call(generation, remote.owner_pid, call).await? {
                    Answer::Ranged { tx, entries, more } => {
                        remote.tx = Some(tx);
                        Ok((entries, more))
                    }
                    other => Err(out_of_turn(other)),
                }
            }
        }
    }

    /// A page of a history read; a remote transaction sends its pending writes with it.
    pub(crate) async fn history_page(
        &mut self,
        start: &[u8],
        end: &[u8],
        skip: u64,
        filter: HistoryFilter,
        limit: usize,
    ) -> Result<(Vec<Version>, bool), Error> {
        let (shared, generation) = (self.shared, self.generation);
        match self
            .open
            .as_mut()
            .expect("a transaction is open until it ends")
        {
            Open::Local(transaction) => {
                Ok(history_page(transaction, start, end, skip, filter, limit)?)
            }
            Open::Remote(remote) => {
                let writes = remote.pending.take();
                remote.flushed |= !writes.is_empty();
                let request = Request::History {
                    tx: remote.tx,
                    writes,
                    start: start.to_vec(),
                    end: end.to_vec(),
                    skip,
                    filter,
                    limit: u32::try_from(limit).unwrap_or(u32::MAX),
                };
                let call = remote.connection.call(request);
                match shared.call(generation, remote.owner_pid, call).await? {
                    Answer::Versions { tx, versions, more } => {
                        remote.tx = Some(tx);
                        Ok((versions, more))
                    }
                    other => Err(out_of_turn(other)),
                }
            }
        }
    }

    /// Commit. [`Error::OwnerLost`] here means the outcome is unknown: retry only what
    /// is safe to write twice.
    pub async fn commit(mut self) -> Result<(), Error> {
        let mut remote = match self
            .open
            .take()
            .expect("a transaction is open until it ends")
        {
            Open::Local(mut transaction) => return Ok(transaction.commit().await?),
            Open::Remote(remote) => remote,
        };
        let writes = remote.pending.take();
        if writes.is_empty() && !remote.flushed {
            // Nothing to write: the transaction ends as a rollback does.
            remote.end().await;
            return Ok(());
        }
        let request = Request::Commit {
            tx: remote.tx,
            durability: remote.durability,
            writes,
        };
        let call = remote.connection.call(request);
        match self
            .shared
            .call(self.generation, remote.owner_pid, call)
            .await?
        {
            Answer::Committed => Ok(()),
            other => Err(out_of_turn(other)),
        }
    }

    pub async fn rollback(mut self) -> Result<(), Error> {
        match self
            .open
            .take()
            .expect("a transaction is open until it ends")
        {
            Open::Local(mut transaction) => transaction.rollback(),
            Open::Remote(remote) => remote.end().await,
        }
        Ok(())
    }
}

impl Remote {
    /// Roll the transaction back in the owner, if a read began it there, without
    /// waiting. An owner that is gone has rolled it back already.
    async fn end(self) {
        if let Some(tx) = self.tx {
            let _ = self.connection.send(Request::Rollback { tx }).await;
        }
    }
}

impl Drop for Tx<'_> {
    fn drop(&mut self) {
        // A remote transaction left open would hold its snapshot in the owner until
        // the connection ends: roll it back without waiting.
        if let Some(Open::Remote(remote)) = self.open.take()
            && remote.tx.is_some()
            && let Ok(runtime) = tokio::runtime::Handle::try_current()
        {
            runtime.spawn(remote.end());
        }
    }
}

/// The error for an answer that does not fit the request.
fn out_of_turn(answer: Answer) -> Error {
    Error::Engine(format!("the owner answered out of turn: {answer:?}"))
}
