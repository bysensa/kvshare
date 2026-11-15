//! The owner's side: the socket, and a table of transactions for each connection.

use std::collections::HashMap;
use std::io;
use std::os::unix::fs::DirBuilderExt;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use surrealkv::{Transaction, Tree};
use tokio::io::{AsyncWriteExt as _, BufReader};
use tokio::net::unix::OwnedWriteHalf;
use tokio::net::{UnixListener, UnixStream};
use tokio::task::{JoinHandle, JoinSet};

use crate::admin::{Maintenance, run_blocking};
use crate::endpoint::{Endpoint, same_user};
use crate::error::KvError;
use crate::exec::{apply, page};
use crate::protocol::{
    Answer, MAX_TRANSACTIONS, Request, RequestId, TxId, Unframed, VERSION, Welcome, Write,
    encode_frame, read_frame, write_frame,
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
    /// The file in the database's directory that names the socket.
    pointer: PathBuf,
    accept: JoinHandle<()>,
}

impl Owner {
    /// Serve `tree`, which this process has just opened, on the socket of `endpoint`,
    /// to the processes of the user `uid`, telling each client the `fingerprint` of
    /// the options it opened the tree with; and leave the socket's path in the
    /// database's directory for them.
    ///
    /// Only the holder of the database's lock calls this, so a socket file found at
    /// the path was left by an owner that is gone, and it is removed without asking.
    /// Must be called on a Tokio runtime.
    pub(crate) fn start(
        tree: Tree,
        endpoint: &Endpoint,
        fingerprint: [u8; 32],
        uid: u32,
    ) -> io::Result<Owner> {
        if let Some(dir) = endpoint.socket.parent() {
            // A directory made here is for this user only. One that is there already
            // is the caller's and stays as it is: who may be served is looked at on
            // every connection.
            std::fs::DirBuilder::new()
                .recursive(true)
                .mode(0o700)
                .create(dir)?;
        }
        match std::fs::remove_file(&endpoint.socket) {
            Ok(()) => {}
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(error),
        }
        let listener = UnixListener::bind(&endpoint.socket)?;
        endpoint.publish()?;
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
            uid,
            Arc::clone(&tree),
            Arc::clone(&maintenance),
            welcome,
        ));
        Ok(Owner {
            tree,
            maintenance,
            socket: endpoint.socket.clone(),
            pointer: endpoint.pointer(),
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
    pub(crate) async fn close(mut self) -> Result<(), surrealkv::Error> {
        // Aborting the accept task drops its set of connections, which aborts them.
        self.accept.abort();
        let _ = (&mut self.accept).await;
        let _quiet = self.maintenance.write().await;
        // Removed while the lock is still held: the next owner's are its own.
        let _ = std::fs::remove_file(&self.pointer);
        let _ = std::fs::remove_file(&self.socket);
        self.tree.close().await
    }
}

/// An owner dropped without a close lets the database go too, as SurrealKV's tree does
/// when it is dropped. A `JoinHandle` that is dropped leaves its task running, and the
/// accept task holds the tree and the socket, so it is aborted here; once it and its
/// connections are gone, SurrealKV closes the tree that nothing holds any more, on the
/// runtime. The socket file stays, as after a crash, and the next owner removes it.
impl Drop for Owner {
    fn drop(&mut self) {
        self.accept.abort();
    }
}

/// Take the connections of the processes of the user `uid` until the task is aborted;
/// each connection is served by a task of its own, which the set aborts when it is
/// dropped. Another user's process is hung up on before it is told anything.
async fn accept(
    listener: UnixListener,
    uid: u32,
    tree: Arc<Tree>,
    maintenance: Maintenance,
    welcome: Arc<Welcome>,
) {
    let mut connections = JoinSet::new();
    loop {
        match listener.accept().await {
            Ok((stream, _)) => {
                if same_user(&stream, uid, "a client").is_err() {
                    continue;
                }
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
/// not hold up the client's other requests. A transaction begins at once, so the tasks
/// of one client commit together; unless its client stopped waiting for a commit that
/// is still unanswered (`OwnerSilent`), and says so. That transaction begins only once
/// the commits sent before it are done, so that its snapshot holds them: the client
/// learns by reading whether the commit landed, and does not find it absent and then
/// present. When the client goes away, its table is dropped, which rolls back what it
/// left open, and the work already started runs to its end.
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
                after_commits,
            } => {
                if after_commits {
                    while commits.join_next().await.is_some() {}
                }
                table.begin(tx, mode, durability);
                continue;
            }
            Request::Rollback { tx } => {
                if let Some(Slot::Open {
                    mut transaction, ..
                }) = table.open.remove(&tx)
                {
                    transaction.rollback();
                }
                continue;
            }
            Request::Writes { tx, writes } => {
                table.ahead(tx, writes);
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
                        let _ = reply(&writer, id, answer).await;
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
                    let _ = reply(&writer, id, answer).await;
                });
                continue;
            }
        };
        let answer = answer.unwrap_or_else(Answer::Failed);
        if reply(&writer, id, answer).await.is_err() {
            break;
        }
        while running.try_join_next().is_some() {}
        while commits.try_join_next().is_some() {}
    }
    drop(table);
    while running.join_next().await.is_some() {}
    while commits.join_next().await.is_some() {}
}

/// Write `answer` to request `id` on the client's socket. An answer that no frame holds
/// becomes the refusal that says so: the client asks for less and keeps its connection.
async fn reply(
    writer: &tokio::sync::Mutex<OwnedWriteHalf>,
    id: RequestId,
    answer: Answer,
) -> io::Result<()> {
    let frame = match encode_frame(&(id, &answer)) {
        Err(Unframed::TooLong) => encode_frame(&(id, Answer::Failed(KvError::TooLarge)))?,
        framed => framed?,
    };
    writer.lock().await.write_all(&frame).await
}

/// A transaction of a client, or why it could not begin: the reason comes back on the
/// client's next request on it.
enum Slot {
    Open {
        transaction: Transaction,
        /// Why writes sent ahead could not be applied: the client's next request on
        /// the transaction is answered with it.
        refused: Option<KvError>,
    },
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
                    Slot::Open {
                        transaction,
                        refused: None,
                    }
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
            Some(Slot::Open {
                transaction,
                refused,
            }) => {
                if let Some(error) = refused.take() {
                    return Err(error);
                }
                apply(transaction, writes)?;
                Ok(read(transaction)?)
            }
            Some(Slot::Failed(error)) => Err(error.clone()),
            None => Err(KvError::NoSuchTransaction(tx)),
        }
    }

    /// Apply `writes`, which the client sent ahead of its next request on transaction
    /// `tx`. They have no answer: why they could not be applied is kept for that
    /// request, and nothing more is applied until it has been told.
    fn ahead(&mut self, tx: TxId, writes: Vec<Write>) {
        if let Some(Slot::Open {
            transaction,
            refused,
        }) = self.open.get_mut(&tx)
            && refused.is_none()
            && let Err(error) = apply(transaction, writes)
        {
            *refused = Some(error.into());
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
            Some(Slot::Open {
                mut transaction,
                refused,
            }) => {
                if let Some(error) = refused {
                    return Err(error);
                }
                transaction.set_durability(durability.durability());
                apply(&mut transaction, writes)?;
                Ok(transaction)
            }
            Some(Slot::Failed(error)) => Err(error),
            None => Err(KvError::NoSuchTransaction(tx)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Writes a client sent ahead have no answer of their own: when one of them cannot
    /// be applied, the client's next request on the transaction is answered with the
    /// reason, once, and nothing was applied after the write that failed.
    #[tokio::test]
    async fn writes_sent_ahead_that_fail_answer_the_next_request() {
        let base = std::env::temp_dir().join(format!("kvo-ahead-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&base);
        let tree = surrealkv::TreeBuilder::new()
            .with_path(base.clone())
            .build()
            .expect("a tree");
        let mut table = Table {
            tree: Arc::new(tree),
            open: HashMap::new(),
        };
        table.begin(1, WireMode::ReadWrite, WireDurability::Eventual);
        table.ahead(
            1,
            vec![
                Write::Set {
                    key: b"a".to_vec(),
                    value: b"1".to_vec(),
                },
                // No savepoint was set: SurrealKV refuses it.
                Write::RollbackToSavepoint,
                Write::Set {
                    key: b"b".to_vec(),
                    value: b"2".to_vec(),
                },
            ],
        );
        let read = |key: &'static [u8]| {
            move |transaction: &mut Transaction| Ok(Answer::Got(transaction.get(key)?))
        };
        assert_eq!(
            table.on(1, Vec::new(), read(b"a")).err(),
            Some(KvError::TransactionWithoutSavepoint),
            "the next request learns why the writes were refused"
        );
        assert!(
            matches!(table.on(1, Vec::new(), read(b"a")), Ok(Answer::Got(Some(_)))),
            "the write before the refused one was applied"
        );
        assert!(
            matches!(table.on(1, Vec::new(), read(b"b")), Ok(Answer::Got(None))),
            "the write after it was not"
        );
        drop(table);
        let _ = std::fs::remove_dir_all(&base);
    }
}
