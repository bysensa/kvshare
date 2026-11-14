//! The owner's side: the socket, and a table of transactions for each connection.

use std::collections::HashMap;
use std::io;
use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use surrealkv::{Transaction, Tree};
use tokio::io::BufReader;
use tokio::net::{UnixListener, UnixStream};
use tokio::task::{JoinHandle, JoinSet};

use crate::admin::{Maintenance, run_blocking};
use crate::endpoint::Endpoint;
use crate::error::KvError;
use crate::exec::{apply, page};
use crate::protocol::{
    Answer, MAX_TRANSACTIONS, Request, RequestId, TxId, VERSION, Welcome, Write, read_frame,
    write_frame,
};
use crate::types::{WireDurability, WireMode};

/// This process owns the database: it holds the tree and serves it on the socket.
///
/// The tree is shared in an `Arc` and never cloned: dropping any clone of a native
/// SurrealKV `Tree` closes the database.
pub(crate) struct Owner {
    tree: Arc<Tree>,
    /// Held by every operation on the whole database while it runs; see [`Maintenance`].
    maintenance: Maintenance,
    socket: PathBuf,
    accept: JoinHandle<()>,
}

impl Owner {
    /// Serve `tree`, which this process has just opened, on the socket of `endpoint`,
    /// telling each client the `fingerprint` of the options it opened the tree with.
    ///
    /// Only the holder of the database's lock calls this, so a socket file found at
    /// the path was left by an owner that is gone, and it is removed without asking.
    /// Must be called on a Tokio runtime.
    pub(crate) fn start(
        tree: Tree,
        endpoint: &Endpoint,
        fingerprint: [u8; 32],
    ) -> io::Result<Owner> {
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
        let welcome = Arc::new(Welcome {
            version: VERSION,
            db: endpoint.db.clone(),
            owner_pid: std::process::id(),
            fingerprint,
        });
        let tree = Arc::new(tree);
        let maintenance = Maintenance::default();
        let accept = tokio::spawn(accept(
            listener,
            Arc::clone(&tree),
            Arc::clone(&maintenance),
            welcome,
        ));
        Ok(Owner {
            tree,
            maintenance,
            socket: endpoint.socket.clone(),
            accept,
        })
    }

    /// The tree, for this process's own transactions.
    pub(crate) fn tree(&self) -> &Arc<Tree> {
        &self.tree
    }

    /// What this process's own operations on the whole database hold while they run.
    pub(crate) fn maintenance(&self) -> &Maintenance {
        &self.maintenance
    }

    /// Let the database go as a crash does: stop serving at once and abandon SurrealKV,
    /// which releases the lock and flushes nothing. The socket file stays, as after a
    /// crash, and the next owner removes it.
    pub(crate) fn abandon(self) {
        self.accept.abort();
        self.tree.abandon();
    }

    /// Hand the database over: stop serving, wait for the operations on the whole
    /// database that run, remove the socket and close the tree.
    ///
    /// The clients lose their connections, their open transactions are rolled back,
    /// and they elect the next owner once the lock is released. A compaction under way
    /// is waited for to its end, see [`Maintenance`]; one asked for meanwhile finds the
    /// tree closed.
    pub(crate) async fn close(self) -> Result<(), surrealkv::Error> {
        // Aborting the accept task drops its set of connections, which aborts them.
        self.accept.abort();
        let _ = self.accept.await;
        let _quiet = self.maintenance.write().await;
        let _ = std::fs::remove_file(&self.socket);
        self.tree.close().await
    }
}

/// Take connections until the task is aborted; each connection is served by a task
/// of its own, which the set aborts when it is dropped.
async fn accept(
    listener: UnixListener,
    tree: Arc<Tree>,
    maintenance: Maintenance,
    welcome: Arc<Welcome>,
) {
    let mut connections = JoinSet::new();
    loop {
        match listener.accept().await {
            Ok((stream, _)) => {
                connections.spawn(serve(
                    stream,
                    Arc::clone(&tree),
                    Arc::clone(&maintenance),
                    Arc::clone(&welcome),
                ));
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
/// Requests are handled in their order, so a transaction a client begins without
/// waiting is open before its next request. A commit, and an operation on the whole
/// database, runs in a task of its own, so one that waits for a sync or copies files does
/// not hold up the client's other requests. A transaction begins only once the commits
/// the client sent before it are done, so that its snapshot holds them: a client that
/// stopped waiting for a commit (`OwnerSilent`) learns by reading whether it landed, and
/// does not find it absent and then present. When the client goes away, its table is
/// dropped, which rolls back what it left open, and the work already started runs to its
/// end.
async fn serve(
    stream: UnixStream,
    tree: Arc<Tree>,
    maintenance: Maintenance,
    welcome: Arc<Welcome>,
) {
    let (read, write) = stream.into_split();
    let writer = Arc::new(tokio::sync::Mutex::new(write));
    if write_frame(&mut *writer.lock().await, &*welcome)
        .await
        .is_err()
    {
        // The client went away before it was welcomed: it asked nothing.
        return;
    }
    let mut reader = BufReader::new(read);
    let mut table = Table {
        tree,
        open: HashMap::new(),
    };
    // The operations on the whole database under way, and apart from them the commits,
    // which every begin waits for.
    let mut running = JoinSet::new();
    let mut commits = JoinSet::new();
    while let Ok((id, request)) = read_frame::<_, (RequestId, Request)>(&mut reader).await {
        let answer = match request {
            Request::Begin {
                tx,
                mode,
                durability,
            } => {
                while commits.join_next().await.is_some() {}
                table.begin(tx, mode, durability);
                continue;
            }
            Request::Rollback { tx } => {
                if let Some(Slot::Open(mut transaction)) = table.open.remove(&tx) {
                    transaction.rollback();
                }
                continue;
            }
            Request::Get {
                tx,
                writes,
                key,
                for_update,
            } => table.on(tx, writes, |transaction| {
                let value = if for_update {
                    transaction.get_for_update(key)?
                } else {
                    transaction.get(key)?
                };
                Ok(Answer::Got(value))
            }),
            Request::GetMany { tx, writes, keys } => table.on(tx, writes, |transaction| {
                Ok(Answer::GotMany(transaction.get_many(&keys)?))
            }),
            Request::GetAt {
                tx,
                writes,
                key,
                timestamp,
            } => table.on(tx, writes, |transaction| {
                Ok(Answer::Got(transaction.get_at(key, timestamp)?))
            }),
            Request::GetManyAt {
                tx,
                writes,
                keys,
                timestamp,
            } => table.on(tx, writes, |transaction| {
                Ok(Answer::GotMany(transaction.get_many_at(&keys, timestamp)?))
            }),
            Request::Cursor {
                tx,
                writes,
                source,
                at,
                limit,
                values,
            } => table.on(tx, writes, |transaction| {
                let source = source.narrowed();
                let (items, more) = page(transaction, &source, at, limit as usize, values)?;
                Ok(Answer::Page { items, more })
            }),
            Request::Commit {
                tx,
                writes,
                durability,
            } => match table.prepare(tx, writes, durability) {
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
            Request::Admin(admin) => {
                let (tree, writer) = (Arc::clone(&table.tree), Arc::clone(&writer));
                let maintenance = Arc::clone(&maintenance);
                running.spawn(async move {
                    let answer = match run_blocking(tree, &maintenance, admin).await {
                        Ok(reply) => Answer::Admin(reply),
                        Err(error) => Answer::Failed(error),
                    };
                    let _ = write_frame(&mut *writer.lock().await, &(id, answer)).await;
                });
                continue;
            }
        };
        let answer = answer.unwrap_or_else(Answer::Failed);
        if write_frame(&mut *writer.lock().await, &(id, answer))
            .await
            .is_err()
        {
            break;
        }
        while running.try_join_next().is_some() {}
        while commits.try_join_next().is_some() {}
    }
    drop(table);
    while running.join_next().await.is_some() {}
    while commits.join_next().await.is_some() {}
}

/// A transaction of a client, or why it could not begin: the reason comes back on the
/// client's next request on it.
enum Slot {
    Open(Transaction),
    Failed(KvError),
}

/// The transactions one client holds open.
struct Table {
    tree: Arc<Tree>,
    open: HashMap<TxId, Slot>,
}

impl Table {
    /// Begin transaction `tx` as SurrealKV begins one with `mode`, its commit returning
    /// as `durability` says; or keep why it could not begin.
    fn begin(&mut self, tx: TxId, mode: WireMode, durability: WireDurability) {
        let slot = if self.open.len() >= MAX_TRANSACTIONS {
            Slot::Failed(KvError::TooManyTransactions)
        } else {
            match self.tree.begin_with_mode(mode.mode()) {
                Ok(mut transaction) => {
                    transaction.set_durability(durability.durability());
                    Slot::Open(transaction)
                }
                Err(error) => Slot::Failed(error.into()),
            }
        };
        self.open.insert(tx, slot);
    }

    /// Apply `writes` to transaction `tx`, then answer with `read` on it.
    fn on(
        &mut self,
        tx: TxId,
        writes: Vec<Write>,
        read: impl FnOnce(&mut Transaction) -> surrealkv::Result<Answer>,
    ) -> Result<Answer, KvError> {
        match self.open.get_mut(&tx) {
            Some(Slot::Open(transaction)) => {
                apply(transaction, writes)?;
                Ok(read(transaction)?)
            }
            Some(Slot::Failed(error)) => Err(error.clone()),
            None => Err(KvError::NoSuchTransaction(tx)),
        }
    }

    /// Take transaction `tx` out of the table and apply `writes` to it: what is left of
    /// it is the commit, which returns as `durability` says.
    fn prepare(
        &mut self,
        tx: TxId,
        writes: Vec<Write>,
        durability: WireDurability,
    ) -> Result<Transaction, KvError> {
        match self.open.remove(&tx) {
            Some(Slot::Open(mut transaction)) => {
                transaction.set_durability(durability.durability());
                apply(&mut transaction, writes)?;
                Ok(transaction)
            }
            Some(Slot::Failed(error)) => Err(error),
            None => Err(KvError::NoSuchTransaction(tx)),
        }
    }
}
