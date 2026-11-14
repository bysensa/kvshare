//! SurrealKV's `TreeBuilder`, `Tree` and `Transaction`, by their names, for a database
//! shared by several processes.
//!
//! A method is `async` when it may reach the owner: opening the tree, `begin`, every
//! read and `commit`. Writes, savepoints and `rollback` stay synchronous, as in
//! SurrealKV. In the owner they go straight to SurrealKV. In a client they wait in the
//! transaction until its next read or its commit, which carry them to the owner in their
//! order; the owner applies each by the SurrealKV method the client called, so SurrealKV
//! itself decides what the transaction sees. A client checks at once what SurrealKV
//! checks at once: a closed transaction, a read-only one, an empty key, a missing
//! savepoint. Any other error of a write comes with the next request that carries it.
//!
//! Cursors are opened synchronously, as SurrealKV's iterators are, and move `async`.

use std::future::Future;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use surrealkv::{
    CipherPreference, CompactRangeOptions, Comparator, CompressionType, Durability, FilterPolicy,
    HistoryOptions, IntoBytes, KeyManager, Mode, Options, VLogChecksumLevel, Value,
    WalRecoveryMode, WalRecoveryReport, WriteOptions,
};

use crate::admin::{
    Admin, BackgroundError, CheckpointMetadata, EncryptionStatus, Reply, run_blocking,
};
use crate::client::{Connection, Lost};
use crate::cursor::Cursor;
use crate::election::{Current, Role, connect};
use crate::endpoint::Endpoint;
use crate::error::{Error, Result};
use crate::exec::{open, page};
use crate::protocol::{Answer, At, Item, Request, Source, TxId, Write};
use crate::types::{KeyRange, ReadOptions, WireDurability, WireMode, fingerprint};

/// Builds a [`Tree`], as SurrealKV's `TreeBuilder` does, with the same options; and the
/// few that kvshare adds: where the owners' sockets live and how long to wait for an
/// owner.
pub struct TreeBuilder {
    options: Options,
    run_dir: PathBuf,
    elect_within: Duration,
    call_within: Duration,
}

impl Default for TreeBuilder {
    fn default() -> Self {
        Self::with_options(Options::new())
    }
}

impl TreeBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_options(options: Options) -> Self {
        TreeBuilder {
            options,
            run_dir: std::env::temp_dir().join("kvshare"),
            elect_within: Duration::from_secs(5),
            call_within: Duration::from_secs(5),
        }
    }

    pub fn with_path(mut self, path: PathBuf) -> Self {
        self.options = self.options.with_path(path);
        self
    }

    pub fn with_block_size(mut self, size: usize) -> Self {
        self.options = self.options.with_block_size(size);
        self
    }

    pub fn with_block_restart_interval(mut self, interval: usize) -> Self {
        self.options = self.options.with_block_restart_interval(interval);
        self
    }

    pub fn with_filter_policy(mut self, policy: Option<Arc<dyn FilterPolicy>>) -> Self {
        self.options = self.options.with_filter_policy(policy);
        self
    }

    pub fn with_comparator(mut self, comparator: Arc<dyn Comparator>) -> Self {
        self.options = self.options.with_comparator(comparator);
        self
    }

    pub fn without_compression(mut self) -> Self {
        self.options = self.options.without_compression();
        self
    }

    pub fn with_compression_per_level(mut self, levels: Vec<CompressionType>) -> Self {
        self.options = self.options.with_compression_per_level(levels);
        self
    }

    pub fn with_level_count(mut self, count: u8) -> Self {
        self.options = self.options.with_level_count(count);
        self
    }

    pub fn with_max_memtable_size(mut self, size: usize) -> Self {
        self.options = self.options.with_max_memtable_size(size);
        self
    }

    pub fn with_block_cache_capacity(mut self, capacity_bytes: u64) -> Self {
        self.options = self.options.with_block_cache_capacity(capacity_bytes);
        self
    }

    pub fn with_index_partition_size(mut self, size: usize) -> Self {
        self.options = self.options.with_index_partition_size(size);
        self
    }

    pub fn with_vlog_max_file_size(mut self, size: u64) -> Self {
        self.options = self.options.with_vlog_max_file_size(size);
        self
    }

    pub fn with_vlog_checksum_verification(mut self, level: VLogChecksumLevel) -> Self {
        self.options = self.options.with_vlog_checksum_verification(level);
        self
    }

    pub fn with_enable_vlog(mut self, enable: bool) -> Self {
        self.options = self.options.with_enable_vlog(enable);
        self
    }

    pub fn with_vlog_value_threshold(mut self, value: usize) -> Self {
        self.options = self.options.with_vlog_value_threshold(value);
        self
    }

    pub fn with_versioning(mut self, enable: bool, retention_ns: u64) -> Self {
        self.options = self.options.with_versioning(enable, retention_ns);
        self
    }

    pub fn with_flush_on_close(mut self, value: bool) -> Self {
        self.options = self.options.with_flush_on_close(value);
        self
    }

    pub fn with_wal_recovery_mode(mut self, mode: WalRecoveryMode) -> Self {
        self.options = self.options.with_wal_recovery_mode(mode);
        self
    }

    pub fn with_memtable_stall_threshold(mut self, value: usize) -> Self {
        self.options = self.options.with_memtable_stall_threshold(value);
        self
    }

    pub fn with_l0_stall_threshold(mut self, value: usize) -> Self {
        self.options = self.options.with_l0_stall_threshold(value);
        self
    }

    pub fn with_encryption(
        mut self,
        key_manager: Arc<dyn KeyManager>,
        cipher: impl Into<CipherPreference>,
    ) -> Self {
        self.options = self.options.with_encryption(key_manager, cipher);
        self
    }

    /// The directory of the owners' sockets; every process of one database must name
    /// the same one. By default `kvshare` in the system's temporary directory.
    pub fn with_run_dir(mut self, run_dir: PathBuf) -> Self {
        self.run_dir = run_dir;
        self
    }

    /// How long opening may wait for an owner to answer or for the database to be free.
    pub fn with_election_timeout(mut self, within: Duration) -> Self {
        self.elect_within = within;
        self
    }

    /// How long a request may wait for the owner's answer.
    pub fn with_call_timeout(mut self, within: Duration) -> Self {
        self.call_within = within;
        self
    }

    /// Open the tree: own the database if it is free, or work through its owner.
    pub async fn build(self) -> Result<Tree> {
        let endpoint = Endpoint::new(&self.options.path, &self.run_dir).map_err(Error::Io)?;
        let inner = Arc::new(Inner {
            endpoint,
            fingerprint: fingerprint(&self.options),
            options: self.options,
            elect_within: self.elect_within,
            call_within: self.call_within,
            generations: AtomicU64::new(0),
            next_tx: AtomicU64::new(1),
            closed: AtomicBool::new(false),
            current: tokio::sync::Mutex::new(None),
        });
        {
            let mut current = inner.current.lock().await;
            inner.held(&mut current).await?;
        }
        Ok(Tree { inner })
    }

    /// The same as [`TreeBuilder::build`]: opening is asynchronous either way.
    pub async fn build_async(self) -> Result<Tree> {
        self.build().await
    }

    /// The tree and the options this process opens it with.
    pub async fn build_with_options(self) -> Result<(Tree, Arc<Options>)> {
        let options = Arc::new(self.options.clone());
        Ok((self.build().await?, options))
    }

    pub async fn build_with_options_async(self) -> Result<(Tree, Arc<Options>)> {
        self.build_with_options().await
    }

    /// The pid of the process that owns this builder's database now, asked over its
    /// socket, without taking part in the election.
    ///
    /// SurrealKV's lock file is no witness: every process that tries to open the
    /// database truncates it before it tries the lock, which erases the owner's pid.
    pub async fn current_owner(&self) -> Result<u32> {
        let endpoint = Endpoint::new(&self.options.path, &self.run_dir).map_err(Error::Io)?;
        let (_, owner_pid) =
            connect(&endpoint, fingerprint(&self.options), self.call_within).await?;
        Ok(owner_pid)
    }
}

/// What every handle of one opened tree shares.
pub(crate) struct Inner {
    pub(crate) endpoint: Endpoint,
    pub(crate) options: Options,
    pub(crate) fingerprint: [u8; 32],
    pub(crate) elect_within: Duration,
    pub(crate) call_within: Duration,
    pub(crate) generations: AtomicU64,
    next_tx: AtomicU64,
    closed: AtomicBool,
    pub(crate) current: tokio::sync::Mutex<Option<Current>>,
}

/// Where an operation goes: the tree of this process, or the owner's connection.
enum Route {
    Local {
        tree: Arc<surrealkv::Tree>,
        generation: u64,
    },
    Remote {
        connection: Arc<Connection>,
        owner_pid: u32,
        generation: u64,
    },
}

impl Inner {
    /// Where an operation goes, after an election if no role is held. The role is not
    /// borrowed past the return, so a failed request can forget it.
    async fn route(&self) -> Result<Route> {
        if self.closed.load(Ordering::Acquire) {
            return Err(Error::Engine("the tree is closed".into()));
        }
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

    /// Wait for the answer to a request, at most the call timeout.
    async fn call(
        &self,
        generation: u64,
        owner_pid: u32,
        call: impl Future<Output = std::result::Result<Answer, Lost>>,
    ) -> Result<Answer> {
        self.wait(generation, owner_pid, call, Some(self.call_within))
            .await
    }

    /// Wait for the answer to a request, at most `within` if it is set; a request the
    /// owner never answered forgets the owner, and a refusal becomes the error it names.
    async fn wait(
        &self,
        generation: u64,
        owner_pid: u32,
        call: impl Future<Output = std::result::Result<Answer, Lost>>,
        within: Option<Duration>,
    ) -> Result<Answer> {
        let answered = match within {
            Some(within) => {
                tokio::time::timeout(within, call)
                    .await
                    .map_err(|_| Error::OwnerSilent {
                        pid: owner_pid,
                        waited: within,
                    })?
            }
            None => call.await,
        };
        match answered {
            Err(Lost(reason)) => {
                self.forget(generation).await;
                Err(Error::OwnerLost(reason))
            }
            Ok(Answer::Failed(refusal)) => Err(refusal.into()),
            Ok(answer) => Ok(answer),
        }
    }
}

/// A database shared with the other processes that open it, as SurrealKV's `Tree`.
#[derive(Clone)]
pub struct Tree {
    inner: Arc<Inner>,
}

impl Tree {
    /// Begin a read-write transaction.
    pub async fn begin(&self) -> Result<Transaction> {
        self.begin_with_mode(Mode::ReadWrite).await
    }

    /// Begin a transaction in `mode`. Its snapshot is taken now, as in SurrealKV: a
    /// client tells the owner without waiting for an answer, and the owner opens the
    /// transaction before it reads the client's next request.
    pub async fn begin_with_mode(&self, mode: Mode) -> Result<Transaction> {
        let wire_mode = WireMode::of(mode)?;
        let (generation, state) = match self.inner.route().await? {
            Route::Local { tree, generation } => {
                (generation, State::Local(tree.begin_with_mode(mode)?))
            }
            Route::Remote {
                connection,
                owner_pid,
                generation,
            } => {
                let id = self.inner.next_tx.fetch_add(1, Ordering::Relaxed);
                let begin = Request::Begin {
                    tx: id,
                    mode: wire_mode,
                    durability: WireDurability::Eventual,
                };
                if let Err(Lost(reason)) = connection.send(begin).await {
                    self.inner.forget(generation).await;
                    return Err(Error::OwnerLost(reason));
                }
                let remote = Remote {
                    connection,
                    owner_pid,
                    id,
                    mode: wire_mode,
                    durability: Durability::Eventual,
                    closed: false,
                    savepoints: 0,
                    pending: tokio::sync::Mutex::new(Vec::new()),
                };
                (generation, State::Remote(remote))
            }
        };
        Ok(Transaction {
            inner: Arc::clone(&self.inner),
            generation,
            state,
        })
    }

    /// Let the database go: an owner closes SurrealKV and hands the database over, a
    /// client disconnects. The tree cannot begin transactions afterwards.
    pub async fn close(&self) -> Result<()> {
        self.inner.closed.store(true, Ordering::Release);
        match self.inner.current.lock().await.take() {
            Some(Current {
                role: Role::Owner(owner),
                ..
            }) => Ok(owner.close().await?),
            _ => Ok(()),
        }
    }

    /// Whether this process owns the database, after an election if no role is held.
    pub async fn is_owner(&self) -> Result<bool> {
        Ok(matches!(self.inner.route().await?, Route::Local { .. }))
    }

    /// The pid of the owner this process works through, or its own if it owns the
    /// database, after an election if no role is held.
    pub async fn owner_pid(&self) -> Result<u32> {
        Ok(match self.inner.route().await? {
            Route::Local { .. } => std::process::id(),
            Route::Remote { owner_pid, .. } => owner_pid,
        })
    }

    /// Read in a read-only transaction: `f` gets it, and it ends after.
    pub async fn view<F>(&self, f: F) -> Result<()>
    where
        F: AsyncFnOnce(&mut Transaction) -> Result<()>,
    {
        let mut transaction = self.begin_with_mode(Mode::ReadOnly).await?;
        f(&mut transaction).await
    }

    /// Make a checkpoint of the database in `checkpoint_dir`, which must lie outside the
    /// database's directory. A relative path is this process's: it is made absolute
    /// before the owner sees it. The owner's process writes the files.
    pub async fn create_checkpoint<P: AsRef<Path>>(
        &self,
        checkpoint_dir: P,
    ) -> Result<CheckpointMetadata> {
        let dir = std::path::absolute(checkpoint_dir).map_err(Error::Io)?;
        match self.admin(Admin::CreateCheckpoint(dir)).await? {
            Reply::Checkpoint(metadata) => Ok(metadata),
            other => Err(out_of_turn(Answer::Admin(other))),
        }
    }

    /// Put the database back as the checkpoint in `checkpoint_dir` holds it, as
    /// SurrealKV does: commits wait meanwhile. A transaction open across the restore
    /// reads the restored database from then on, with its own writes, and what it
    /// commits lands on it. A relative path is this process's.
    pub async fn restore_from_checkpoint<P: AsRef<Path>>(
        &self,
        checkpoint_dir: P,
    ) -> Result<CheckpointMetadata> {
        let dir = std::path::absolute(checkpoint_dir).map_err(Error::Io)?;
        match self.admin(Admin::RestoreFromCheckpoint(dir)).await? {
            Reply::Checkpoint(metadata) => Ok(metadata),
            other => Err(out_of_turn(Answer::Admin(other))),
        }
    }

    /// Compact the keys of `[start, end)` down the tree, `None` for an open bound.
    pub async fn compact_range(
        &self,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
        options: &CompactRangeOptions,
    ) -> Result<()> {
        let admin = Admin::CompactRange {
            start: start.map(<[u8]>::to_vec),
            end: end.map(<[u8]>::to_vec),
            flush_memtable: options.flush_memtable,
            target_level: options.target_level,
            rewrite_bottommost: options.rewrite_bottommost,
        };
        done(self.admin(admin).await?)
    }

    /// Flush the buffers of the log to the operating system, and with `sync` to disk.
    pub async fn flush_wal(&self, sync: bool) -> Result<()> {
        done(self.admin(Admin::FlushWal { sync }).await?)
    }

    /// What recovering the log did when the owner opened the database.
    pub async fn recovery_report(&self) -> Result<WalRecoveryReport> {
        match self.admin(Admin::RecoveryReport).await? {
            Reply::Recovery(recovery) => Ok(recovery.into()),
            other => Err(out_of_turn(Answer::Admin(other))),
        }
    }

    /// The error of SurrealKV's background work, if there is one.
    pub async fn background_error(&self) -> Result<Option<BackgroundError>> {
        match self.admin(Admin::BackgroundError).await? {
            Reply::BackgroundError(recorded) => recorded.map(|error| error.received()).transpose(),
            other => Err(out_of_turn(Answer::Admin(other))),
        }
    }

    /// How many errors SurrealKV's background work has met.
    pub async fn background_error_count(&self) -> Result<u64> {
        match self.admin(Admin::BackgroundErrorCount).await? {
            Reply::Count(count) => Ok(count),
            other => Err(out_of_turn(Answer::Admin(other))),
        }
    }

    /// Whether SurrealKV stopped the database for an error it cannot recover from.
    pub async fn is_stopped(&self) -> Result<bool> {
        match self.admin(Admin::IsStopped).await? {
            Reply::Flag(stopped) => Ok(stopped),
            other => Err(out_of_turn(Answer::Admin(other))),
        }
    }

    /// Ask the owner's key manager again for the keys the database uses. Hidden, as
    /// SurrealKV hides it.
    #[doc(hidden)]
    pub async fn prepare_keys(&self) -> Result<()> {
        done(self.admin(Admin::PrepareKeys).await?)
    }

    /// Whether the database is encrypted, and which keys cover what, as the owner's key
    /// manager says. Hidden, as SurrealKV hides it.
    #[doc(hidden)]
    pub async fn encryption_status(&self) -> Result<EncryptionStatus> {
        match self.admin(Admin::EncryptionStatus).await? {
            Reply::Encryption(status) => status.received(),
            other => Err(out_of_turn(Answer::Admin(other))),
        }
    }

    /// End the tree as a crash does: an owner abandons SurrealKV, which releases the
    /// lock and flushes nothing, and stops serving at once; a client lets its connection
    /// go. Hidden, as SurrealKV hides it: it is for tests of what follows a crash.
    #[doc(hidden)]
    pub async fn abandon(&self) {
        self.inner.closed.store(true, Ordering::Release);
        if let Some(Current {
            role: Role::Owner(owner),
            ..
        }) = self.inner.current.lock().await.take()
        {
            owner.abandon();
        }
    }

    /// Run `admin`: in this process when it owns the database, in the owner otherwise.
    /// A client waits for an operation that takes long for as long as it takes.
    async fn admin(&self, admin: Admin) -> Result<Reply> {
        match self.inner.route().await? {
            Route::Local { tree, .. } => Ok(run_blocking(tree, admin).await?),
            Route::Remote {
                connection,
                owner_pid,
                generation,
            } => {
                let within = (!admin.takes_long()).then_some(self.inner.call_within);
                let call = connection.call(Request::Admin(admin));
                match self.inner.wait(generation, owner_pid, call, within).await? {
                    Answer::Admin(reply) => Ok(reply),
                    other => Err(out_of_turn(other)),
                }
            }
        }
    }
}

/// The end of an operation that answers nothing but that it is done.
fn done(reply: Reply) -> Result<()> {
    match reply {
        Reply::Done => Ok(()),
        other => Err(out_of_turn(Answer::Admin(other))),
    }
}

/// A transaction, as SurrealKV's `Transaction`: in this process when it owns the
/// database, in the owner otherwise.
pub struct Transaction {
    inner: Arc<Inner>,
    generation: u64,
    state: State,
}

enum State {
    Local(surrealkv::Transaction),
    Remote(Remote),
}

/// A transaction in the owner, as the client keeps it.
struct Remote {
    connection: Arc<Connection>,
    owner_pid: u32,
    id: TxId,
    mode: WireMode,
    durability: Durability,
    closed: bool,
    savepoints: u64,
    /// The writes the owner has not had yet, in their order. Every read takes them to
    /// the owner, holding the lock until its answer, so they reach the owner before any
    /// read the transaction makes after them.
    pending: tokio::sync::Mutex<Vec<Write>>,
}

impl Remote {
    /// What SurrealKV checks of a write before it takes it: the mode, then whether the
    /// transaction is closed, then the key.
    fn check(&self, key: &[u8]) -> Result<()> {
        if !self.mode.mutable() {
            return Err(Error::TransactionReadOnly);
        }
        if self.closed {
            return Err(Error::TransactionClosed);
        }
        if key.is_empty() {
            return Err(Error::EmptyKey);
        }
        Ok(())
    }

    /// What SurrealKV checks of a savepoint operation: the mode, then whether the
    /// transaction is closed.
    fn check_savepoint(&self) -> Result<()> {
        if !self.mode.mutable() {
            return Err(Error::TransactionReadOnly);
        }
        if self.closed {
            return Err(Error::TransactionClosed);
        }
        Ok(())
    }

    fn push(&mut self, write: Write) {
        self.pending.get_mut().push(write);
    }

    /// Send a read of this transaction with the writes it has kept, and wait for the
    /// answer. A closed transaction reads nothing, as in SurrealKV.
    async fn ask(
        &self,
        inner: &Inner,
        generation: u64,
        request: impl FnOnce(TxId, Vec<Write>) -> Request,
    ) -> Result<Answer> {
        if self.closed {
            return Err(Error::TransactionClosed);
        }
        // The lock is held until the answer: a read that took no writes must not
        // overtake, on the socket, the read that took them.
        let mut pending = self.pending.lock().await;
        let call = self
            .connection
            .call(request(self.id, std::mem::take(&mut *pending)));
        inner.call(generation, self.owner_pid, call).await
    }
}

impl Transaction {
    /// Whether the transaction runs in this process, which owns the database.
    pub fn is_local(&self) -> bool {
        matches!(self.state, State::Local(_))
    }

    pub fn set_durability(&mut self, durability: Durability) {
        match &mut self.state {
            State::Local(transaction) => transaction.set_durability(durability),
            State::Remote(remote) => remote.durability = durability,
        }
    }

    pub fn with_durability(mut self, durability: Durability) -> Self {
        self.set_durability(durability);
        self
    }

    pub fn set<K: IntoBytes, V: IntoBytes>(&mut self, key: K, value: V) -> Result<()> {
        match &mut self.state {
            State::Local(transaction) => Ok(transaction.set(key, value)?),
            State::Remote(remote) => {
                remote.check(key.as_slice())?;
                remote.push(Write::Set {
                    key: key.into_bytes(),
                    value: value.into_bytes(),
                });
                Ok(())
            }
        }
    }

    pub fn set_at<K: IntoBytes, V: IntoBytes>(
        &mut self,
        key: K,
        value: V,
        timestamp: u64,
    ) -> Result<()> {
        match &mut self.state {
            State::Local(transaction) => Ok(transaction.set_at(key, value, timestamp)?),
            State::Remote(remote) => {
                remote.check(key.as_slice())?;
                remote.push(Write::SetAt {
                    key: key.into_bytes(),
                    value: value.into_bytes(),
                    timestamp,
                });
                Ok(())
            }
        }
    }

    pub fn set_with_options<K: IntoBytes, V: IntoBytes>(
        &mut self,
        key: K,
        value: V,
        options: &WriteOptions,
    ) -> Result<()> {
        match &mut self.state {
            State::Local(transaction) => Ok(transaction.set_with_options(key, value, options)?),
            State::Remote(remote) => {
                remote.check(key.as_slice())?;
                remote.push(Write::SetWithOptions {
                    key: key.into_bytes(),
                    value: value.into_bytes(),
                    timestamp: options.timestamp,
                });
                Ok(())
            }
        }
    }

    /// A hard delete, as SurrealKV's: the key's history goes with it.
    pub fn delete<K: IntoBytes>(&mut self, key: K) -> Result<()> {
        match &mut self.state {
            State::Local(transaction) => Ok(transaction.delete(key)?),
            State::Remote(remote) => {
                remote.check(key.as_slice())?;
                remote.push(Write::Delete {
                    key: key.into_bytes(),
                });
                Ok(())
            }
        }
    }

    pub fn delete_with_options<K: IntoBytes>(
        &mut self,
        key: K,
        options: &WriteOptions,
    ) -> Result<()> {
        match &mut self.state {
            State::Local(transaction) => Ok(transaction.delete_with_options(key, options)?),
            State::Remote(remote) => {
                remote.check(key.as_slice())?;
                remote.push(Write::DeleteWithOptions {
                    key: key.into_bytes(),
                    timestamp: options.timestamp,
                });
                Ok(())
            }
        }
    }

    pub fn delete_range<K: IntoBytes>(&mut self, start: K, end: K) -> Result<()> {
        match &mut self.state {
            State::Local(transaction) => Ok(transaction.delete_range(start, end)?),
            State::Remote(remote) => {
                // SurrealKV writes nothing for a range that holds no key, and checks
                // nothing before it finds that out: not the mode, the end or the key.
                if start.as_slice() >= end.as_slice() {
                    return Ok(());
                }
                remote.check(start.as_slice())?;
                remote.push(Write::DeleteRange {
                    start: start.into_bytes(),
                    end: end.into_bytes(),
                });
                Ok(())
            }
        }
    }

    /// A tombstone, as SurrealKV's `soft_delete`: the key's history stays.
    pub fn soft_delete<K: IntoBytes>(&mut self, key: K) -> Result<()> {
        match &mut self.state {
            State::Local(transaction) => Ok(transaction.soft_delete(key)?),
            State::Remote(remote) => {
                remote.check(key.as_slice())?;
                remote.push(Write::SoftDelete {
                    key: key.into_bytes(),
                });
                Ok(())
            }
        }
    }

    pub fn soft_delete_with_options<K: IntoBytes>(
        &mut self,
        key: K,
        options: &WriteOptions,
    ) -> Result<()> {
        match &mut self.state {
            State::Local(transaction) => Ok(transaction.soft_delete_with_options(key, options)?),
            State::Remote(remote) => {
                remote.check(key.as_slice())?;
                remote.push(Write::SoftDeleteWithOptions {
                    key: key.into_bytes(),
                    timestamp: options.timestamp,
                });
                Ok(())
            }
        }
    }

    pub fn replace<K: IntoBytes, V: IntoBytes>(&mut self, key: K, value: V) -> Result<()> {
        match &mut self.state {
            State::Local(transaction) => Ok(transaction.replace(key, value)?),
            State::Remote(remote) => {
                remote.check(key.as_slice())?;
                remote.push(Write::Replace {
                    key: key.into_bytes(),
                    value: value.into_bytes(),
                });
                Ok(())
            }
        }
    }

    pub fn set_savepoint(&mut self) -> Result<()> {
        match &mut self.state {
            State::Local(transaction) => Ok(transaction.set_savepoint()?),
            State::Remote(remote) => {
                remote.check_savepoint()?;
                remote.savepoints += 1;
                remote.push(Write::SetSavepoint);
                Ok(())
            }
        }
    }

    pub fn rollback_to_savepoint(&mut self) -> Result<()> {
        match &mut self.state {
            State::Local(transaction) => Ok(transaction.rollback_to_savepoint()?),
            State::Remote(remote) => {
                remote.check_savepoint()?;
                if remote.savepoints == 0 {
                    return Err(Error::TransactionWithoutSavepoint);
                }
                remote.savepoints -= 1;
                remote.push(Write::RollbackToSavepoint);
                Ok(())
            }
        }
    }

    pub async fn get<K: IntoBytes>(&self, key: K) -> Result<Option<Value>> {
        match &self.state {
            State::Local(transaction) => Ok(transaction.get(key)?),
            State::Remote(remote) => {
                let key = key.into_bytes();
                let answer = remote
                    .ask(&self.inner, self.generation, |tx, writes| Request::Get {
                        tx,
                        writes,
                        key,
                        for_update: false,
                    })
                    .await?;
                got(answer)
            }
        }
    }

    /// The same as [`Transaction::get`]: reads are asynchronous either way.
    pub async fn get_async<K: IntoBytes>(&self, key: K) -> Result<Option<Value>> {
        self.get(key).await
    }

    /// SurrealKV reads a key alike whatever the read options say.
    pub async fn get_with_options<K: IntoBytes>(
        &self,
        key: K,
        options: &ReadOptions,
    ) -> Result<Option<Value>> {
        match &self.state {
            State::Local(transaction) => {
                Ok(transaction.get_with_options(key, &surrealkv::ReadOptions::from(options))?)
            }
            State::Remote(_) => self.get(key).await,
        }
    }

    pub async fn get_with<K, R, F>(&self, key: K, f: F) -> Result<R>
    where
        K: IntoBytes,
        F: FnOnce(Option<&[u8]>) -> R,
    {
        match &self.state {
            State::Local(transaction) => Ok(transaction.get_with(key, f)?),
            State::Remote(_) => Ok(f(self.get(key).await?.as_deref())),
        }
    }

    /// Read `key` and have SurrealKV check at the commit that no other transaction
    /// wrote it since this one began.
    pub async fn get_for_update<K: IntoBytes>(&mut self, key: K) -> Result<Option<Value>> {
        match &mut self.state {
            State::Local(transaction) => Ok(transaction.get_for_update(key)?),
            State::Remote(remote) => {
                let key = key.into_bytes();
                let answer = remote
                    .ask(&self.inner, self.generation, |tx, writes| Request::Get {
                        tx,
                        writes,
                        key,
                        for_update: true,
                    })
                    .await?;
                got(answer)
            }
        }
    }

    pub async fn get_at<K: IntoBytes>(&self, key: K, timestamp: u64) -> Result<Option<Value>> {
        match &self.state {
            State::Local(transaction) => Ok(transaction.get_at(key, timestamp)?),
            State::Remote(remote) => {
                let key = key.into_bytes();
                let answer = remote
                    .ask(&self.inner, self.generation, |tx, writes| Request::GetAt {
                        tx,
                        writes,
                        key,
                        timestamp,
                    })
                    .await?;
                got(answer)
            }
        }
    }

    pub async fn get_at_with<K, R, F>(&self, key: K, timestamp: u64, f: F) -> Result<R>
    where
        K: IntoBytes,
        F: FnOnce(Option<&[u8]>) -> R,
    {
        match &self.state {
            State::Local(transaction) => Ok(transaction.get_at_with(key, timestamp, f)?),
            State::Remote(_) => Ok(f(self.get_at(key, timestamp).await?.as_deref())),
        }
    }

    pub async fn get_many<K: IntoBytes>(&self, keys: &[K]) -> Result<Vec<Option<Value>>> {
        match &self.state {
            State::Local(transaction) => Ok(transaction.get_many(keys)?),
            State::Remote(remote) => {
                let keys = keys.iter().map(|key| key.as_slice().to_vec()).collect();
                let answer = remote
                    .ask(&self.inner, self.generation, |tx, writes| {
                        Request::GetMany { tx, writes, keys }
                    })
                    .await?;
                got_many(answer)
            }
        }
    }

    pub async fn get_many_with<K, F>(&self, keys: &[K], mut f: F) -> Result<()>
    where
        K: IntoBytes,
        F: FnMut(usize, Option<&[u8]>),
    {
        match &self.state {
            State::Local(transaction) => Ok(transaction.get_many_with(keys, f)?),
            State::Remote(_) => {
                for (at, value) in self.get_many(keys).await?.iter().enumerate() {
                    f(at, value.as_deref());
                }
                Ok(())
            }
        }
    }

    pub async fn get_many_at<K: IntoBytes>(
        &self,
        keys: &[K],
        timestamp: u64,
    ) -> Result<Vec<Option<Value>>> {
        match &self.state {
            State::Local(transaction) => Ok(transaction.get_many_at(keys, timestamp)?),
            State::Remote(remote) => {
                let keys = keys.iter().map(|key| key.as_slice().to_vec()).collect();
                let answer = remote
                    .ask(&self.inner, self.generation, |tx, writes| {
                        Request::GetManyAt {
                            tx,
                            writes,
                            keys,
                            timestamp,
                        }
                    })
                    .await?;
                got_many(answer)
            }
        }
    }

    pub async fn get_many_at_with<K, F>(&self, keys: &[K], timestamp: u64, mut f: F) -> Result<()>
    where
        K: IntoBytes,
        F: FnMut(usize, Option<&[u8]>),
    {
        match &self.state {
            State::Local(transaction) => Ok(transaction.get_many_at_with(keys, timestamp, f)?),
            State::Remote(_) => {
                for (at, value) in self.get_many_at(keys, timestamp).await?.iter().enumerate() {
                    f(at, value.as_deref());
                }
                Ok(())
            }
        }
    }

    /// A cursor over every key, as this transaction sees it.
    pub fn iter(&self) -> Result<Cursor<'_>> {
        self.range_with_options(&ReadOptions::default())
    }

    /// A cursor over the keys of `range`: `..`, `a..b`, `a..=b`, `a..`, `..b` or `..=b`.
    pub fn scan<R: KeyRange>(&self, range: R) -> Result<Cursor<'_>> {
        self.range_with_options(&ReadOptions::from_range(range))
    }

    /// A cursor over the keys of `[start, end)`.
    pub fn range<K: IntoBytes>(&self, start: K, end: K) -> Result<Cursor<'_>> {
        let mut options = ReadOptions::default();
        options.set_iterate_lower_bound(Some(start.into_bytes()));
        options.set_iterate_upper_bound(Some(end.into_bytes()));
        self.range_with_options(&options)
    }

    /// A cursor over the keys between the bounds of `options`: the newest value of each
    /// key as this transaction sees it, its own writes included.
    pub fn range_with_options(&self, options: &ReadOptions) -> Result<Cursor<'_>> {
        let source = Source::Range {
            lower: options.lower_bound.clone(),
            upper: options.upper_bound.clone(),
        };
        self.cursor(source, None)
    }

    /// A cursor over every version of the keys of `[start, end)`.
    pub fn history<K: IntoBytes>(&self, start: K, end: K) -> Result<Cursor<'_>> {
        self.history_with_options(start, end, &HistoryOptions::default())
    }

    /// A cursor over the versions of the keys of `[start, end)` that `options` lets
    /// through: keys in order, the versions of a key newest first. The tree must keep
    /// versions.
    pub fn history_with_options<K: IntoBytes>(
        &self,
        start: K,
        end: K,
        options: &HistoryOptions,
    ) -> Result<Cursor<'_>> {
        let source = Source::History {
            start: start.into_bytes(),
            end: end.into_bytes(),
            tombstones: options.include_tombstones,
            between: options.ts_range,
        };
        self.cursor(source, options.limit)
    }

    /// A cursor over `source`, spending `limit` if it is a history. Opening checks what
    /// SurrealKV checks when it opens an iterator, in SurrealKV's order: whether the
    /// transaction is closed, whether it may read, and, for a history, whether the tree
    /// keeps versions. In the owner SurrealKV checks it, opening an iterator it drops; a
    /// client checks it itself.
    fn cursor(&self, source: Source, limit: Option<usize>) -> Result<Cursor<'_>> {
        let source = source.narrowed();
        match &self.state {
            State::Local(transaction) => {
                open(transaction, &source)?;
            }
            State::Remote(remote) => {
                if remote.closed {
                    return Err(Error::TransactionClosed);
                }
                if remote.mode == WireMode::WriteOnly {
                    return Err(Error::TransactionWriteOnly);
                }
                if matches!(source, Source::History { .. }) && !self.inner.options.enable_versioning
                {
                    return Err(Error::InvalidArgument("Versioning not enabled".into()));
                }
            }
        }
        Ok(Cursor::new(self, source, limit))
    }

    pub async fn commit(&mut self) -> Result<()> {
        let (inner, generation) = (&self.inner, self.generation);
        match &mut self.state {
            State::Local(transaction) => Ok(transaction.commit().await?),
            State::Remote(remote) => {
                // What SurrealKV checks before it commits; the transaction is closed
                // from then on, whatever the commit's outcome.
                if remote.closed {
                    return Err(Error::TransactionClosed);
                }
                if !remote.mode.mutable() {
                    return Err(Error::TransactionReadOnly);
                }
                // Refused before the transaction closes, so that dropping it still rolls
                // it back in the owner.
                let durability = WireDurability::of(remote.durability)?;
                remote.closed = true;
                let request = Request::Commit {
                    tx: remote.id,
                    writes: std::mem::take(remote.pending.get_mut()),
                    durability,
                };
                let call = remote.connection.call(request);
                match inner.call(generation, remote.owner_pid, call).await? {
                    Answer::Committed => Ok(()),
                    other => Err(out_of_turn(other)),
                }
            }
        }
    }

    pub fn rollback(&mut self) {
        match &mut self.state {
            State::Local(transaction) => transaction.rollback(),
            State::Remote(remote) => {
                remote.savepoints = 0;
                remote.pending.get_mut().clear();
                if !std::mem::replace(&mut remote.closed, true) {
                    end_in_owner(remote);
                }
            }
        }
    }

    /// A page of a cursor over `source`: up to `limit` entries from `at`, in the order
    /// they were read, with their values or not, and whether more follow that way. In a
    /// client the owner reads it, and the first page carries the writes the transaction
    /// has kept.
    pub(crate) async fn page(
        &self,
        source: &Source,
        at: At,
        limit: usize,
        values: bool,
    ) -> Result<(Vec<Item>, bool)> {
        let remote = match &self.state {
            State::Local(transaction) => {
                return Ok(page(transaction, source, at, limit, values)?);
            }
            State::Remote(remote) => remote,
        };
        let answer = remote
            .ask(&self.inner, self.generation, |tx, writes| Request::Cursor {
                tx,
                writes,
                source: source.clone(),
                at,
                limit: u32::try_from(limit).unwrap_or(u32::MAX),
                values,
            })
            .await?;
        match answer {
            Answer::Page { items, more } => Ok((items, more)),
            other => Err(out_of_turn(other)),
        }
    }
}

impl Drop for Transaction {
    fn drop(&mut self) {
        // A remote transaction left open would hold its snapshot in the owner until the
        // connection ends: roll it back without waiting.
        if let State::Remote(remote) = &mut self.state
            && !std::mem::replace(&mut remote.closed, true)
        {
            end_in_owner(remote);
        }
    }
}

/// Roll the transaction back in the owner without waiting, if there is a runtime to
/// send it from. An owner that is gone has rolled it back already.
fn end_in_owner(remote: &Remote) {
    if let Ok(runtime) = tokio::runtime::Handle::try_current() {
        let (connection, tx) = (Arc::clone(&remote.connection), remote.id);
        runtime.spawn(async move {
            let _ = connection.send(Request::Rollback { tx }).await;
        });
    }
}

fn got(answer: Answer) -> Result<Option<Value>> {
    match answer {
        Answer::Got(value) => Ok(value),
        other => Err(out_of_turn(other)),
    }
}

fn got_many(answer: Answer) -> Result<Vec<Option<Value>>> {
    match answer {
        Answer::GotMany(values) => Ok(values),
        other => Err(out_of_turn(other)),
    }
}

/// The error for an answer that does not fit the request.
fn out_of_turn(answer: Answer) -> Error {
    Error::Handshake(format!("the owner answered out of turn: {answer:?}"))
}
