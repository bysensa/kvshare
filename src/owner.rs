//! The owner's side: the socket, and a table of transactions for each connection.

use std::collections::HashMap;
use std::io;
use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use surrealkv::{Mode, Transaction, Tree};
use tokio::io::BufReader;
use tokio::net::{UnixListener, UnixStream};
use tokio::task::{JoinHandle, JoinSet};

use crate::endpoint::Endpoint;
use crate::exec::{apply, history_page, range_page};
use crate::protocol::{
    Answer, Durability, HistoryFilter, KvError, MAX_TRANSACTIONS, Request, RequestId, TxId,
    VERSION, Welcome, Write, read_frame, write_frame,
};

/// This process owns the database: it holds the tree and serves it on the socket.
pub struct Owner {
    tree: Arc<Tree>,
    socket: PathBuf,
    accept: JoinHandle<()>,
}

impl Owner {
    /// Serve `tree`, which this process has just opened, on the socket of `endpoint`.
    ///
    /// Only the holder of the database's lock calls this, so a socket file found at
    /// the path was left by an owner that is gone, and it is removed without asking.
    /// Must be called on a Tokio runtime.
    pub fn start(tree: Tree, endpoint: &Endpoint) -> io::Result<Owner> {
        if let Some(dir) = endpoint.socket.parent() {
            std::fs::create_dir_all(dir)?;
            std::fs::set_permissions(dir, std::fs::Permissions::from_mode(0o700))?;
        }
        match std::fs::remove_file(&endpoint.socket) {
            Ok(()) => {}
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(error),
        }
        let listener = UnixListener::bind(&endpoint.socket)?;
        let tree = Arc::new(tree);
        let accept = tokio::spawn(accept(listener, Arc::clone(&tree), endpoint.db.clone()));
        Ok(Owner {
            tree,
            socket: endpoint.socket.clone(),
            accept,
        })
    }

    /// The tree, for this process's own transactions.
    pub fn tree(&self) -> &Arc<Tree> {
        &self.tree
    }

    /// Hand the database over: stop serving, remove the socket and close the tree.
    ///
    /// The clients lose their connections, their open transactions are rolled back,
    /// and they elect the next owner once the lock is released.
    pub async fn close(self) -> Result<(), surrealkv::Error> {
        // Aborting the accept task drops its set of connections, which aborts them.
        self.accept.abort();
        let _ = self.accept.await;
        let _ = std::fs::remove_file(&self.socket);
        self.tree.close().await
    }
}

/// Take connections until the task is aborted; each connection is served by a task
/// of its own, which the set aborts when it is dropped.
async fn accept(listener: UnixListener, tree: Arc<Tree>, db: PathBuf) {
    let mut connections = JoinSet::new();
    loop {
        match listener.accept().await {
            Ok((stream, _)) => {
                connections.spawn(serve(stream, Arc::clone(&tree), db.clone()));
            }
            // Out of descriptors, or a connection reset before it was taken: wait a
            // little rather than spin on an error that repeats.
            Err(_) => tokio::time::sleep(Duration::from_millis(10)).await,
        }
        while connections.try_join_next().is_some() {}
    }
}

/// Welcome the client of `stream`, then answer its requests until it goes away.
///
/// Reads and rollbacks are answered in turn. A commit runs in a task of its own, so a
/// commit that waits for a sync does not hold up the client's other requests. When the
/// client goes away, its table is dropped, which rolls back what it left open, and the
/// commits already started run to their end.
async fn serve(stream: UnixStream, tree: Arc<Tree>, db: PathBuf) {
    let (read, write) = stream.into_split();
    let writer = Arc::new(tokio::sync::Mutex::new(write));
    let welcome = Welcome {
        version: VERSION,
        db,
        owner_pid: std::process::id(),
    };
    if write_frame(&mut *writer.lock().await, &welcome)
        .await
        .is_err()
    {
        // The client went away before it was welcomed: it asked nothing.
        return;
    }
    let mut reader = BufReader::new(read);
    let mut table = Table {
        tree,
        next: 1,
        open: HashMap::new(),
    };
    let mut commits = JoinSet::new();
    while let Ok((id, request)) = read_frame::<_, (RequestId, Request)>(&mut reader).await {
        let answer = match request {
            Request::Get { tx, key } => table.get(tx, key),
            Request::GetAt { key, timestamp } => table.get_at(key, timestamp),
            Request::Range {
                tx,
                writes,
                start,
                end,
                reverse,
                limit,
            } => table.range(tx, writes, &start, &end, reverse, limit as usize),
            Request::History {
                tx,
                writes,
                start,
                end,
                skip,
                filter,
                limit,
            } => table.history(tx, writes, &start, &end, skip, filter, limit as usize),
            Request::Rollback { tx } => {
                if let Some(mut transaction) = table.open.remove(&tx) {
                    transaction.rollback();
                }
                continue;
            }
            Request::Commit {
                tx,
                durability,
                writes,
            } => match table.prepare(tx, durability, writes) {
                Ok(mut transaction) => {
                    let writer = Arc::clone(&writer);
                    commits.spawn(async move {
                        let answer = match transaction.commit().await {
                            Ok(()) => Answer::Committed,
                            Err(error) => Answer::Failed(error.into()),
                        };
                        // A client that went away learns nothing; the commit stands.
                        let _ = write_frame(&mut *writer.lock().await, &(id, answer)).await;
                    });
                    continue;
                }
                Err(error) => Err(error),
            },
        };
        let answer = answer.unwrap_or_else(Answer::Failed);
        if write_frame(&mut *writer.lock().await, &(id, answer))
            .await
            .is_err()
        {
            break;
        }
        while commits.try_join_next().is_some() {}
    }
    drop(table);
    while commits.join_next().await.is_some() {}
}

/// The transactions one client holds open.
struct Table {
    tree: Arc<Tree>,
    next: TxId,
    open: HashMap<TxId, Transaction>,
}

impl Table {
    /// Transaction `tx`, or one that begins now, with `writes` applied to it in their
    /// order: the transaction a read of a client works on.
    fn open(
        &mut self,
        tx: Option<TxId>,
        writes: Vec<Write>,
    ) -> Result<(TxId, &mut Transaction), KvError> {
        let tx = match tx {
            Some(tx) => tx,
            None => {
                if self.open.len() >= MAX_TRANSACTIONS {
                    return Err(KvError::TooManyTransactions);
                }
                let tx = self.next;
                self.next += 1;
                self.open.insert(tx, self.tree.begin()?);
                tx
            }
        };
        let transaction = self
            .open
            .get_mut(&tx)
            .ok_or(KvError::NoSuchTransaction(tx))?;
        apply(transaction, writes)?;
        Ok((tx, transaction))
    }

    /// The value of `key` in transaction `tx`, or in a transaction that begins now.
    fn get(&mut self, tx: Option<TxId>, key: Vec<u8>) -> Result<Answer, KvError> {
        let (tx, transaction) = self.open(tx, Vec::new())?;
        Ok(Answer::Got {
            tx,
            value: transaction.get(key)?,
        })
    }

    /// A page of a range read of transaction `tx`, after its `writes`.
    fn range(
        &mut self,
        tx: Option<TxId>,
        writes: Vec<Write>,
        start: &[u8],
        end: &[u8],
        reverse: bool,
        limit: usize,
    ) -> Result<Answer, KvError> {
        let (tx, transaction) = self.open(tx, writes)?;
        let (entries, more) = range_page(transaction, start, end, reverse, limit)?;
        Ok(Answer::Ranged { tx, entries, more })
    }

    /// A page of a history read of transaction `tx`, after its `writes`.
    #[allow(
        clippy::too_many_arguments,
        reason = "the fields of one request, passed as they came"
    )]
    fn history(
        &mut self,
        tx: Option<TxId>,
        writes: Vec<Write>,
        start: &[u8],
        end: &[u8],
        skip: u64,
        filter: HistoryFilter,
        limit: usize,
    ) -> Result<Answer, KvError> {
        let (tx, transaction) = self.open(tx, writes)?;
        let (versions, more) = history_page(transaction, start, end, skip, filter, limit)?;
        Ok(Answer::Versions { tx, versions, more })
    }

    /// The value of `key` as of `timestamp`, in a read of its own.
    fn get_at(&self, key: Vec<u8>, timestamp: u64) -> Result<Answer, KvError> {
        let transaction = self.tree.begin_with_mode(Mode::ReadOnly)?;
        Ok(Answer::GotAt(transaction.get_at(key, timestamp)?))
    }

    /// Take transaction `tx` out of the table, or begin one, and apply `writes` to it:
    /// what is left of it is the commit.
    fn prepare(
        &mut self,
        tx: Option<TxId>,
        durability: Durability,
        writes: Vec<Write>,
    ) -> Result<Transaction, KvError> {
        let mut transaction = match tx {
            Some(tx) => self
                .open
                .remove(&tx)
                .ok_or(KvError::NoSuchTransaction(tx))?,
            None => self.tree.begin()?,
        };
        transaction.set_durability(durability.into());
        apply(&mut transaction, writes)?;
        Ok(transaction)
    }
}
