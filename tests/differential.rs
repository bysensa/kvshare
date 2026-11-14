//! The same operations on SurrealKV itself, on the owner's tree and through a client of
//! another owner: every answer, a value or an error, is the same.
//!
//! Each scenario writes down what each operation answered, and the records must match
//! line for line. Times are written down only when the scenario chose them; a commit's
//! own time differs from one database to another. The owner and the client run each
//! scenario with cursors that read pages of several sizes.

mod common;

use std::path::Path;

use common::{base, builder, close, owner_and_client};
use kvshare::{
    CompactRangeOptions, Durability, Error, HistoryOptions, Mode, ReadOptions, WriteOptions,
};
use surrealkv::LSMIterator;

/// A database under test, its directory, and for kvshare the size of its cursors' pages.
enum Db<'a> {
    Plain(&'a surrealkv::Tree, &'a Path),
    Shared(&'a kvshare::Tree, usize, &'a Path),
}

impl Db<'_> {
    async fn begin(&self, mode: Mode) -> Tx {
        match self {
            Db::Plain(tree, _) => Tx::Plain(tree.begin_with_mode(mode).expect("a transaction")),
            Db::Shared(tree, page, _) => Tx::Shared(
                tree.begin_with_mode(mode).await.expect("a transaction"),
                *page,
            ),
        }
    }

    fn path(&self) -> &Path {
        match self {
            Db::Plain(_, path) | Db::Shared(_, _, path) => path,
        }
    }

    async fn create_checkpoint(&self, dir: &Path) -> String {
        match self {
            Db::Plain(tree, _) => {
                said(plain(tree.create_checkpoint(dir)).map(|m| {
                    checkpoint(m.version, m.sequence_number, m.sstable_count, m.total_size)
                }))
            }
            Db::Shared(tree, ..) => {
                said(tree.create_checkpoint(dir).await.map(|m| {
                    checkpoint(m.version, m.sequence_number, m.sstable_count, m.total_size)
                }))
            }
        }
    }

    async fn restore_from_checkpoint(&self, dir: &Path) -> String {
        match self {
            Db::Plain(tree, _) => {
                said(plain(tree.restore_from_checkpoint(dir)).map(|m| {
                    checkpoint(m.version, m.sequence_number, m.sstable_count, m.total_size)
                }))
            }
            Db::Shared(tree, ..) => {
                said(tree.restore_from_checkpoint(dir).await.map(|m| {
                    checkpoint(m.version, m.sequence_number, m.sstable_count, m.total_size)
                }))
            }
        }
    }

    async fn compact_range(
        &self,
        start: Option<&[u8]>,
        end: Option<&[u8]>,
        options: &CompactRangeOptions,
    ) -> String {
        match self {
            Db::Plain(tree, _) => said(plain(tree.compact_range(start, end, options))),
            Db::Shared(tree, ..) => said(tree.compact_range(start, end, options).await),
        }
    }

    async fn flush_wal(&self, sync: bool) -> String {
        match self {
            Db::Plain(tree, _) => said(plain(tree.flush_wal(sync))),
            Db::Shared(tree, ..) => said(tree.flush_wal(sync).await),
        }
    }

    /// The recovery report, whether a background error is recorded, how many were, and
    /// whether the database stopped.
    async fn health(&self) -> String {
        match self {
            Db::Plain(tree, _) => format!(
                "{:?} error={:?} count={} stopped={}",
                tree.recovery_report(),
                tree.background_error().map(|error| error.reason),
                tree.background_error_count(),
                tree.is_stopped()
            ),
            Db::Shared(tree, ..) => format!(
                "{:?} error={:?} count={} stopped={}",
                tree.recovery_report().await.expect("a report"),
                tree.background_error()
                    .await
                    .expect("an answer")
                    .map(|error| error.reason),
                tree.background_error_count().await.expect("a count"),
                tree.is_stopped().await.expect("an answer")
            ),
        }
    }

    async fn prepare_keys(&self) -> String {
        match self {
            Db::Plain(tree, _) => said(plain(tree.prepare_keys())),
            Db::Shared(tree, ..) => said(tree.prepare_keys().await),
        }
    }

    /// The encryption status, as its fields print.
    async fn encryption_status(&self) -> String {
        match self {
            Db::Plain(tree, _) => match tree.encryption_status() {
                Ok(status) => format!("ok {status:?}"),
                Err(error) => format!("err {:?}", Error::from(error)),
            },
            Db::Shared(tree, ..) => match tree.encryption_status().await {
                Ok(status) => format!("ok {status:?}"),
                Err(error) => format!("err {error:?}"),
            },
        }
    }

    /// What `view` reads of `key`.
    async fn view(&self, key: &[u8]) -> String {
        let mut seen = None;
        let answer = match self {
            Db::Plain(tree, _) => plain(tree.view(|tx| {
                seen = tx.get(key)?;
                Ok(())
            })),
            Db::Shared(tree, ..) => {
                tree.view(async |tx| {
                    seen = tx.get(key).await?;
                    Ok(())
                })
                .await
            }
        };
        said(answer.map(|()| seen))
    }
}

/// A checkpoint as the record shows it: not its time, which is the clock's.
fn checkpoint(version: u32, sequence: u64, tables: usize, bytes: u64) -> String {
    format!("v{version} seq={sequence} tables={tables} bytes={bytes}")
}

/// A transaction under test.
enum Tx {
    Plain(surrealkv::Transaction),
    Shared(kvshare::Transaction, usize),
}

/// What an answer looks like in the record.
trait Shown {
    fn shown(self) -> String;
}

impl Shown for () {
    fn shown(self) -> String {
        "()".into()
    }
}

impl Shown for usize {
    fn shown(self) -> String {
        self.to_string()
    }
}

impl Shown for Option<usize> {
    fn shown(self) -> String {
        format!("{self:?}")
    }
}

impl Shown for Vec<u8> {
    fn shown(self) -> String {
        lossy(&self)
    }
}

impl Shown for Option<Vec<u8>> {
    fn shown(self) -> String {
        self.map_or("None".into(), |value| lossy(&value))
    }
}

impl Shown for Vec<Option<Vec<u8>>> {
    fn shown(self) -> String {
        let values: Vec<String> = self.into_iter().map(Shown::shown).collect();
        values.join(",")
    }
}

impl Shown for Vec<String> {
    fn shown(self) -> String {
        self.join(" ")
    }
}

impl Shown for String {
    fn shown(self) -> String {
        self
    }
}

fn lossy(bytes: &[u8]) -> String {
    String::from_utf8_lossy(bytes).into_owned()
}

/// A time as the record shows it: the scenario's own times as they are, the time of a
/// write not committed yet as `pending`, and a commit's own time as `now`.
fn stamp(timestamp: u64) -> String {
    match timestamp {
        u64::MAX => "pending".into(),
        chosen if chosen <= 10_000 => chosen.to_string(),
        _ => "now".into(),
    }
}

fn said<T: Shown>(result: Result<T, Error>) -> String {
    match result {
        Ok(value) => format!("ok {}", value.shown()),
        Err(error) => format!("err {error:?}"),
    }
}

/// SurrealKV's answer as kvshare reports it.
fn plain<T>(result: surrealkv::Result<T>) -> Result<T, Error> {
    result.map_err(Error::from)
}

/// A write: synchronous on both sides.
macro_rules! write {
    ($tx:expr, $method:ident($($arg:expr),*)) => {
        match $tx {
            Tx::Plain(t) => said(plain(t.$method($($arg),*))),
            Tx::Shared(t, _) => said(t.$method($($arg),*)),
        }
    };
}

/// A read: synchronous in SurrealKV, asynchronous in kvshare.
macro_rules! read {
    ($tx:expr, $method:ident($($arg:expr),*)) => {
        match $tx {
            Tx::Plain(t) => said(plain(t.$method($($arg),*))),
            Tx::Shared(t, _) => said(t.$method($($arg),*).await),
        }
    };
}

fn timestamp(at: Option<u64>) -> WriteOptions {
    WriteOptions::new().with_timestamp(at)
}

/// Which cursor a walk opens.
enum Open<'a> {
    Iter,
    Scan(Bounds<'a>),
    Range(&'a [u8], &'a [u8]),
    RangeWithOptions(Option<&'a [u8]>, Option<&'a [u8]>),
    History(&'a [u8], &'a [u8], HistoryOptions),
}

/// The kinds of range `scan` takes.
#[derive(Clone, Copy)]
enum Bounds<'a> {
    Full,
    HalfOpen(&'a [u8], &'a [u8]),
    Inclusive(&'a [u8], &'a [u8]),
    From(&'a [u8]),
    To(&'a [u8]),
    ToInclusive(&'a [u8]),
}

/// A move of a cursor.
#[derive(Clone, Copy, Debug)]
enum Move<'a> {
    First,
    Last,
    Seek(&'a [u8]),
    Next,
    Prev,
}

/// To the first entry and `steps` entries on, then to the last and `steps` back.
fn there_and_back(steps: usize) -> Vec<Move<'static>> {
    let mut moves = vec![Move::First];
    moves.extend(std::iter::repeat_n(Move::Next, steps));
    moves.push(Move::Last);
    moves.extend(std::iter::repeat_n(Move::Prev, steps));
    moves
}

fn open_plain<'t>(
    t: &'t surrealkv::Transaction,
    open: &Open<'_>,
) -> surrealkv::Result<Box<dyn LSMIterator + 't>> {
    Ok(match open {
        Open::Iter => Box::new(t.iter()?),
        Open::Scan(bounds) => match *bounds {
            Bounds::Full => Box::new(t.scan(..)?),
            Bounds::HalfOpen(a, z) => Box::new(t.scan(a..z)?),
            Bounds::Inclusive(a, z) => Box::new(t.scan(a..=z)?),
            Bounds::From(a) => Box::new(t.scan(a..)?),
            Bounds::To(z) => Box::new(t.scan(..z)?),
            Bounds::ToInclusive(z) => Box::new(t.scan(..=z)?),
        },
        Open::Range(a, z) => Box::new(t.range(*a, *z)?),
        Open::RangeWithOptions(lower, upper) => {
            let mut options = surrealkv::ReadOptions::new();
            options.set_iterate_lower_bound(lower.map(<[u8]>::to_vec));
            options.set_iterate_upper_bound(upper.map(<[u8]>::to_vec));
            Box::new(t.range_with_options(&options)?)
        }
        Open::History(a, z, options) => Box::new(t.history_with_options(*a, *z, options)?),
    })
}

fn open_shared<'t>(
    t: &'t kvshare::Transaction,
    open: &Open<'_>,
) -> kvshare::Result<kvshare::Cursor<'t>> {
    match open {
        Open::Iter => t.iter(),
        Open::Scan(bounds) => match *bounds {
            Bounds::Full => t.scan(..),
            Bounds::HalfOpen(a, z) => t.scan(a..z),
            Bounds::Inclusive(a, z) => t.scan(a..=z),
            Bounds::From(a) => t.scan(a..),
            Bounds::To(z) => t.scan(..z),
            Bounds::ToInclusive(z) => t.scan(..=z),
        },
        Open::Range(a, z) => t.range(*a, *z),
        Open::RangeWithOptions(lower, upper) => {
            let mut options = ReadOptions::new();
            options.set_iterate_lower_bound(lower.map(<[u8]>::to_vec));
            options.set_iterate_upper_bound(upper.map(<[u8]>::to_vec));
            t.range_with_options(&options)
        }
        Open::History(a, z, options) => t.history_with_options(*a, *z, options),
    }
}

/// What a move answered and where the cursor stands after it: the key, the time, the
/// kind of the write and the value.
fn landed(
    answer: Result<bool, Error>,
    valid: bool,
    entry: impl FnOnce() -> (String, u64, String, Result<Vec<u8>, Error>),
) -> String {
    match answer {
        Err(error) => format!("err {error:?}"),
        Ok(false) => format!("ok false valid={valid}"),
        Ok(true) => {
            let (key, at, kind, value) = entry();
            format!(
                "ok true valid={valid} {key}@{} {kind} {}",
                stamp(at),
                said(value)
            )
        }
    }
}

impl Tx {
    fn set(&mut self, key: &[u8], value: &[u8]) -> String {
        write!(self, set(key, value))
    }

    fn set_at(&mut self, key: &[u8], value: &[u8], at: u64) -> String {
        write!(self, set_at(key, value, at))
    }

    fn set_with_options(&mut self, key: &[u8], value: &[u8], at: Option<u64>) -> String {
        write!(self, set_with_options(key, value, &timestamp(at)))
    }

    fn delete(&mut self, key: &[u8]) -> String {
        write!(self, delete(key))
    }

    fn delete_with_options(&mut self, key: &[u8], at: Option<u64>) -> String {
        write!(self, delete_with_options(key, &timestamp(at)))
    }

    fn soft_delete(&mut self, key: &[u8]) -> String {
        write!(self, soft_delete(key))
    }

    fn soft_delete_with_options(&mut self, key: &[u8], at: Option<u64>) -> String {
        write!(self, soft_delete_with_options(key, &timestamp(at)))
    }

    fn replace(&mut self, key: &[u8], value: &[u8]) -> String {
        write!(self, replace(key, value))
    }

    fn delete_range(&mut self, start: &[u8], end: &[u8]) -> String {
        write!(self, delete_range(start, end))
    }

    fn set_savepoint(&mut self) -> String {
        write!(self, set_savepoint())
    }

    fn rollback_to_savepoint(&mut self) -> String {
        write!(self, rollback_to_savepoint())
    }

    fn rollback(&mut self) {
        match self {
            Tx::Plain(t) => t.rollback(),
            Tx::Shared(t, _) => t.rollback(),
        }
    }

    async fn commit(&mut self) -> String {
        match self {
            Tx::Plain(t) => said(plain(t.commit().await)),
            Tx::Shared(t, _) => said(t.commit().await),
        }
    }

    async fn get(&mut self, key: &[u8]) -> String {
        read!(self, get(key))
    }

    async fn get_async(&mut self, key: &[u8]) -> String {
        match self {
            Tx::Plain(t) => said(plain(t.get_async(key).await)),
            Tx::Shared(t, _) => said(t.get_async(key).await),
        }
    }

    fn set_durability(&mut self, durability: Durability) {
        match self {
            Tx::Plain(t) => t.set_durability(durability),
            Tx::Shared(t, _) => t.set_durability(durability),
        }
    }

    /// `with_durability`, which takes the transaction and gives it back.
    fn with_durability(self, durability: Durability) -> Self {
        match self {
            Tx::Plain(t) => Tx::Plain(t.with_durability(durability)),
            Tx::Shared(t, page) => Tx::Shared(t.with_durability(durability), page),
        }
    }

    async fn get_for_update(&mut self, key: &[u8]) -> String {
        read!(self, get_for_update(key))
    }

    async fn get_many(&mut self, keys: &[&[u8]]) -> String {
        read!(self, get_many(keys))
    }

    async fn get_at(&mut self, key: &[u8], at: u64) -> String {
        read!(self, get_at(key, at))
    }

    async fn get_many_at(&mut self, keys: &[&[u8]], at: u64) -> String {
        read!(self, get_many_at(keys, at))
    }

    async fn get_with_options(&mut self, key: &[u8]) -> String {
        let options = ReadOptions::new();
        match self {
            Tx::Plain(t) => said(plain(
                t.get_with_options(key, &surrealkv::ReadOptions::from(&options)),
            )),
            Tx::Shared(t, _) => said(t.get_with_options(key, &options).await),
        }
    }

    /// `get_with`, the closure taking the length of the value.
    async fn get_with(&mut self, key: &[u8]) -> String {
        let length = |value: Option<&[u8]>| value.map(<[u8]>::len);
        read!(self, get_with(key, length))
    }

    /// `get_at_with`, the closure taking the length of the value.
    async fn get_at_with(&mut self, key: &[u8], at: u64) -> String {
        let length = |value: Option<&[u8]>| value.map(<[u8]>::len);
        read!(self, get_at_with(key, at, length))
    }

    /// `get_many_with` and `get_many_at_with`, each value's place and length noted down.
    async fn get_many_with(&mut self, keys: &[&[u8]], at: Option<u64>) -> String {
        let mut seen = Vec::new();
        let note = |place: usize, value: Option<&[u8]>| {
            seen.push(format!("{place}:{:?}", value.map(<[u8]>::len)));
        };
        let answer = match (self, at) {
            (Tx::Plain(t), None) => plain(t.get_many_with(keys, note)),
            (Tx::Plain(t), Some(at)) => plain(t.get_many_at_with(keys, at, note)),
            (Tx::Shared(t, _), None) => t.get_many_with(keys, note).await,
            (Tx::Shared(t, _), Some(at)) => t.get_many_at_with(keys, at, note).await,
        };
        said(answer.map(|()| seen))
    }

    /// Open the cursor `open` and make `moves`, writing down what each answered.
    async fn walk(&mut self, open: &Open<'_>, moves: &[Move<'_>]) -> Vec<String> {
        self.walk_reading(open, moves, true).await
    }

    /// Open the cursor `open` and make `moves`, writing down what each answered: with
    /// the values, or, without `values`, keys only. A kvshare cursor then reads keys
    /// only, and SurrealKV's iterator is not asked for values.
    async fn walk_reading(
        &mut self,
        open: &Open<'_>,
        moves: &[Move<'_>],
        values: bool,
    ) -> Vec<String> {
        let unread = || Ok(b"(not read)".to_vec());
        match self {
            Tx::Plain(t) => {
                let mut cursor = match open_plain(t, open) {
                    Ok(cursor) => cursor,
                    Err(error) => return vec![format!("open: err {:?}", Error::from(error))],
                };
                let mut log = Vec::new();
                for step in moves {
                    let answer = match *step {
                        Move::First => cursor.seek_first(),
                        Move::Last => cursor.seek_last(),
                        Move::Seek(target) => cursor.seek(target),
                        Move::Next => cursor.next(),
                        Move::Prev => cursor.prev(),
                    };
                    let line = landed(plain(answer), cursor.valid(), || {
                        let key = cursor.key();
                        (
                            lossy(key.user_key()),
                            key.timestamp(),
                            format!("{:?}", key.kind()),
                            if values {
                                plain(cursor.value())
                            } else {
                                unread()
                            },
                        )
                    });
                    log.push(format!("{step:?}: {line}"));
                }
                log
            }
            Tx::Shared(t, page) => {
                let mut cursor = match open_shared(t, open) {
                    Ok(cursor) => cursor,
                    Err(error) => return vec![format!("open: err {error:?}")],
                };
                cursor.set_page_size(*page);
                cursor.set_keys_only(!values);
                let mut log = Vec::new();
                for step in moves {
                    let answer = match *step {
                        Move::First => cursor.seek_first().await,
                        Move::Last => cursor.seek_last().await,
                        Move::Seek(target) => cursor.seek(target).await,
                        Move::Next => cursor.next().await,
                        Move::Prev => cursor.prev().await,
                    };
                    let line = landed(answer, cursor.valid(), || {
                        let key = cursor.key();
                        let value = match cursor.value() {
                            // A cursor over keys only refuses every value, as it should.
                            Err(Error::InvalidArgument(_)) if !values => unread(),
                            other => other,
                        };
                        (
                            lossy(key.user_key()),
                            key.timestamp(),
                            format!("{:?}", key.kind()),
                            value,
                        )
                    });
                    log.push(format!("{step:?}: {line}"));
                }
                log
            }
        }
    }

    /// Every entry of `[start, end)`, from the first forwards or from the last backwards,
    /// each as `key=value`.
    async fn range(&mut self, start: &[u8], end: &[u8], reverse: bool) -> String {
        let (first, step) = if reverse {
            (Move::Last, Move::Prev)
        } else {
            (Move::First, Move::Next)
        };
        self.entries(&Open::Range(start, end), first, step).await
    }

    /// Every version of the keys of `[start, end)`, tombstones too, each as
    /// `key@time=value`.
    async fn history(&mut self, start: &[u8], end: &[u8]) -> String {
        let options = HistoryOptions::new().with_tombstones(true);
        self.entries(&Open::History(start, end, options), Move::First, Move::Next)
            .await
    }

    /// Every entry from `first` on, by `step`.
    async fn entries(&mut self, open: &Open<'_>, first: Move<'_>, step: Move<'_>) -> String {
        let mut moves = vec![first];
        moves.extend(std::iter::repeat_n(step, 64));
        let mut seen = Vec::new();
        for line in self.walk(open, &moves).await {
            if let Some(entry) = line.split(" valid=true ").nth(1) {
                seen.push(entry.to_string());
            } else if line.contains("err ") {
                return line;
            }
        }
        format!("ok {}", seen.join(" | "))
    }
}

/// Run `scenario` on SurrealKV itself, then, for each size of page, on a tree that owns
/// its database and on a client of a tree that owns another; every record must be
/// SurrealKV's.
///
/// The directories have short names: a socket path must fit in 103 bytes, and the
/// temporary directory of macOS takes half of them.
async fn compare(name: &str, pages: &[usize], scenario: impl AsyncFn(&Db<'_>) -> Vec<String>) {
    let base = base(&format!("d-{name}"));

    let path = base.join("p");
    let tree = surrealkv::TreeBuilder::new()
        .with_path(path.clone())
        .with_versioning(true, 0)
        .build()
        .expect("SurrealKV opens");
    let expected = scenario(&Db::Plain(&tree, &path)).await;
    tree.close().await.expect("SurrealKV closes");
    eprintln!("{name}, as SurrealKV answers:\n  {}", expected.join("\n  "));

    for (n, &page) in pages.iter().enumerate() {
        let alone = base.join(format!("o{n}"));
        let owner = builder(&alone).build().await.expect("the owner");
        let local = scenario(&Db::Shared(&owner, page, &alone.join("db"))).await;
        owner.close().await.expect("the owner closes");

        let shared = base.join(format!("c{n}"));
        let (owner, client) = owner_and_client(&shared).await;
        let remote = scenario(&Db::Shared(&client, page, &shared.join("db"))).await;
        close(owner, client, &shared).await;

        for (record, who) in [
            (&local, "the owner's tree"),
            (&remote, "through the socket"),
        ] {
            for (line, expected) in expected.iter().enumerate() {
                assert_eq!(
                    record.get(line),
                    Some(expected),
                    "{name}, {who}, pages of {page}, line {line}"
                );
            }
            assert_eq!(
                record.len(),
                expected.len(),
                "{name}, {who}, pages of {page}"
            );
        }
    }
    let _ = std::fs::remove_dir_all(&base);
}

/// What SurrealKV refuses at once: writes of a read-only or a closed transaction, empty
/// keys, a savepoint that is not there; and reads of a write-only or a closed one.
async fn refusals(db: &Db<'_>) -> Vec<String> {
    let mut log = Vec::new();
    let mut ro = db.begin(Mode::ReadOnly).await;
    log.push(format!("ro set: {}", ro.set(b"k", b"v")));
    log.push(format!("ro delete: {}", ro.delete(b"k")));
    log.push(format!("ro delete range: {}", ro.delete_range(b"a", b"z")));
    log.push(format!(
        "ro backwards range delete: {}",
        ro.delete_range(b"z", b"a")
    ));
    log.push(format!("ro savepoint: {}", ro.set_savepoint()));
    log.push(format!(
        "ro back to savepoint: {}",
        ro.rollback_to_savepoint()
    ));
    log.push(format!("ro get: {}", ro.get(b"k").await));
    log.push(format!("ro commit: {}", ro.commit().await));
    log.push(format!("ro get after commit: {}", ro.get(b"k").await));
    log.push(format!("ro commit again: {}", ro.commit().await));

    let mut rw = db.begin(Mode::ReadWrite).await;
    log.push(format!("empty set: {}", rw.set(b"", b"v")));
    log.push(format!("empty delete: {}", rw.delete(b"")));
    log.push(format!("empty soft delete: {}", rw.soft_delete(b"")));
    log.push(format!("empty replace: {}", rw.replace(b"", b"v")));
    log.push(format!(
        "empty range delete: {}",
        rw.delete_range(b"", b"z")
    ));
    log.push(format!(
        "empty keys range delete: {}",
        rw.delete_range(b"", b"")
    ));
    log.push(format!("no savepoint: {}", rw.rollback_to_savepoint()));
    log.push(format!("set: {}", rw.set(b"k", b"v")));
    log.push(format!("empty get: {}", rw.get(b"").await));
    log.push(format!("get after empty get: {}", rw.get(b"k").await));
    log.push(format!("commit: {}", rw.commit().await));
    log.push(format!("set after commit: {}", rw.set(b"k", b"w")));
    log.push(format!("savepoint after commit: {}", rw.set_savepoint()));
    log.push(format!(
        "no range after commit: {}",
        rw.delete_range(b"b", b"b")
    ));
    log.push(format!("get after commit: {}", rw.get(b"k").await));
    log.push(format!(
        "range after commit: {}",
        rw.range(b"a", b"z", false).await
    ));
    log.push(format!(
        "history after commit: {}",
        rw.history(b"a", b"z").await
    ));
    log.push(format!("commit again: {}", rw.commit().await));

    let mut rolled = db.begin(Mode::ReadWrite).await;
    log.push(format!("set: {}", rolled.set(b"r", b"v")));
    rolled.rollback();
    log.push(format!("set after rollback: {}", rolled.set(b"r", b"w")));
    log.push(format!("get after rollback: {}", rolled.get(b"r").await));
    log.push(format!("commit after rollback: {}", rolled.commit().await));

    let mut wo = db.begin(Mode::WriteOnly).await;
    log.push(format!("wo set: {}", wo.set(b"w", b"v")));
    log.push(format!("wo get: {}", wo.get(b"w").await));
    log.push(format!("wo get many: {}", wo.get_many(&[b"w", b"k"]).await));
    log.push(format!("wo range: {}", wo.range(b"a", b"z", false).await));
    log.push(format!("wo history: {}", wo.history(b"a", b"z").await));
    log.push(format!("wo commit: {}", wo.commit().await));

    let mut check = db.begin(Mode::ReadOnly).await;
    log.push(format!(
        "after: {}",
        check.get_many(&[b"k", b"r", b"w"]).await
    ));
    log
}

/// Savepoints: a rollback to one undoes the writes since, and they nest.
async fn savepoints(db: &Db<'_>) -> Vec<String> {
    let mut log = Vec::new();
    let mut tx = db.begin(Mode::ReadWrite).await;
    log.push(tx.set(b"a", b"1"));
    log.push(tx.set_savepoint());
    log.push(tx.set(b"a", b"2"));
    log.push(tx.set(b"b", b"1"));
    log.push(format!("before: {}", tx.get_many(&[b"a", b"b"]).await));
    log.push(tx.rollback_to_savepoint());
    log.push(format!("after one: {}", tx.get_many(&[b"a", b"b"]).await));
    log.push(tx.set_savepoint());
    log.push(tx.delete(b"a"));
    log.push(tx.set_savepoint());
    log.push(tx.set(b"c", b"1"));
    log.push(format!("inner: {}", tx.range(b"a", b"z", false).await));
    log.push(tx.rollback_to_savepoint());
    log.push(format!("back once: {}", tx.range(b"a", b"z", false).await));
    log.push(tx.rollback_to_savepoint());
    log.push(format!("back twice: {}", tx.range(b"a", b"z", false).await));
    log.push(tx.rollback_to_savepoint());
    log.push(tx.commit().await);
    let mut check = db.begin(Mode::ReadOnly).await;
    log.push(format!(
        "committed: {}",
        check.range(b"a", b"z", false).await
    ));
    log
}

/// Every kind of write, with times chosen and without, read back by time, by range and
/// by history, before and after the commit.
async fn writes(db: &Db<'_>) -> Vec<String> {
    let mut log = Vec::new();
    for (value, at) in [(&b"a100"[..], 100), (b"a200", 200)] {
        let mut tx = db.begin(Mode::ReadWrite).await;
        log.push(tx.set_at(b"a", value, at));
        log.push(tx.commit().await);
    }
    // Durability changes when a commit returns, not what it writes.
    let mut tx = db
        .begin(Mode::ReadWrite)
        .await
        .with_durability(Durability::Immediate);
    log.push(tx.set_with_options(b"b", b"b300", Some(300)));
    log.push(tx.set_with_options(b"c", b"c-now", None));
    log.push(tx.commit().await);
    let mut tx = db.begin(Mode::ReadWrite).await;
    tx.set_durability(Durability::Immediate);
    log.push(tx.set(b"d", b"d-now"));
    log.push(tx.set(b"e", b"e-now"));
    log.push(tx.commit().await);

    let mut tx = db.begin(Mode::ReadWrite).await;
    log.push(format!("a at 150: {}", tx.get_at(b"a", 150).await));
    log.push(format!("a at 250: {}", tx.get_at(b"a", 250).await));
    log.push(format!("a at 50: {}", tx.get_at(b"a", 50).await));
    log.push(format!(
        "at 250: {}",
        tx.get_many_at(&[b"a", b"b", b"x"], 250).await
    ));
    log.push(format!(
        "a at 150, length: {}",
        tx.get_at_with(b"a", 150).await
    ));
    log.push(tx.replace(b"a", b"replaced"));
    log.push(tx.soft_delete_with_options(b"b", Some(400)));
    log.push(tx.delete_with_options(b"c", Some(500)));
    log.push(tx.soft_delete(b"d"));
    log.push(tx.delete(b"e"));
    log.push(tx.set(b"f", b"f-now"));
    log.push(format!("history inside: {}", tx.history(b"a", b"z").await));
    log.push(format!(
        "range inside: {}",
        tx.range(b"a", b"z", false).await
    ));
    log.push(tx.commit().await);

    let mut tx = db.begin(Mode::ReadOnly).await;
    log.push(format!("history after: {}", tx.history(b"a", b"z").await));
    log.push(format!(
        "range after: {}",
        tx.range(b"a", b"z", false).await
    ));
    log.push(format!("b at 350: {}", tx.get_at(b"b", 350).await));
    log.push(format!("b at 450: {}", tx.get_at(b"b", 450).await));

    let mut tx = db.begin(Mode::ReadWrite).await;
    for key in [&b"r1"[..], b"r2", b"r3", b"s"] {
        log.push(tx.set(key, b"v"));
    }
    log.push(tx.commit().await);
    let mut tx = db.begin(Mode::ReadWrite).await;
    log.push(format!("range delete: {}", tx.delete_range(b"r", b"s")));
    log.push(format!(
        "backwards range delete: {}",
        tx.delete_range(b"z", b"a")
    ));
    log.push(format!("inside: {}", tx.range(b"r", b"t", false).await));
    log.push(tx.set(b"r2", b"again"));
    log.push(format!("set again: {}", tx.range(b"r", b"t", false).await));
    log.push(tx.commit().await);
    let mut tx = db.begin(Mode::ReadOnly).await;
    log.push(format!("after: {}", tx.range(b"r", b"t", false).await));
    log
}

/// Reads of every kind, of committed values and of the transaction's own writes.
async fn reads(db: &Db<'_>) -> Vec<String> {
    let mut log = Vec::new();
    let mut tx = db.begin(Mode::ReadWrite).await;
    for (key, value) in [
        ("a", "1"),
        ("b", "22"),
        ("c", "333"),
        ("d", "4"),
        ("e", "5"),
    ] {
        log.push(tx.set(key.as_bytes(), value.as_bytes()));
    }
    log.push(tx.commit().await);

    let mut tx = db.begin(Mode::ReadWrite).await;
    log.push(format!("many: {}", tx.get_many(&[b"a", b"x", b"c"]).await));
    log.push(format!("async: {}", tx.get_async(b"c").await));
    log.push(format!("with: {}", tx.get_with(b"c").await));
    log.push(format!("with, missing: {}", tx.get_with(b"x").await));
    log.push(format!(
        "many with: {}",
        tx.get_many_with(&[b"b", b"x", b"c"], None).await
    ));
    log.push(format!(
        "many at with: {}",
        tx.get_many_with(&[b"b", b"x"], Some(u64::MAX)).await
    ));
    log.push(format!("options: {}", tx.get_with_options(b"b").await));
    log.push(format!("latest: {}", tx.get_at(b"a", u64::MAX).await));
    log.push(format!(
        "many latest: {}",
        tx.get_many_at(&[b"a", b"e"], u64::MAX).await
    ));
    log.push(tx.set(b"c", b"30"));
    log.push(tx.delete(b"b"));
    log.push(tx.set(b"f", b"6"));
    log.push(tx.soft_delete(b"d"));
    log.push(format!("forward: {}", tx.range(b"a", b"z", false).await));
    log.push(format!("backward: {}", tx.range(b"a", b"z", true).await));
    log.push(format!("middle: {}", tx.range(b"b", b"e", false).await));
    log.push(format!("empty: {}", tx.range(b"x", b"y", false).await));
    log.push(format!(
        "one key wide: {}",
        tx.range(b"c", b"c", false).await
    ));
    log.push(format!(
        "own: {}",
        tx.get_many(&[b"b", b"c", b"d", b"f"]).await
    ));
    log.push(format!("own with: {}", tx.get_with(b"f").await));
    tx.rollback();
    let mut tx = db.begin(Mode::ReadOnly).await;
    log.push(format!(
        "rolled back: {}",
        tx.range(b"a", b"z", false).await
    ));
    log
}

/// Conflicts: a transaction's snapshot is taken when it begins, and a commit conflicts
/// with what was committed since on the keys it wrote or read for update.
async fn conflicts(db: &Db<'_>) -> Vec<String> {
    let mut log = Vec::new();
    let mut seed = db.begin(Mode::ReadWrite).await;
    log.push(seed.set(b"k", b"0"));
    log.push(seed.set(b"r", b"0"));
    log.push(seed.commit().await);

    // A blind write that another commit overtook.
    let mut first = db.begin(Mode::ReadWrite).await;
    let mut second = db.begin(Mode::ReadWrite).await;
    log.push(second.set(b"k", b"second"));
    log.push(format!("second: {}", second.commit().await));
    log.push(first.set(b"k", b"first"));
    log.push(format!("blind first: {}", first.commit().await));

    // The snapshot is the one of `begin`.
    let mut first = db.begin(Mode::ReadWrite).await;
    let mut second = db.begin(Mode::ReadWrite).await;
    log.push(second.set(b"s", b"new"));
    log.push(second.commit().await);
    log.push(format!("snapshot: {}", first.get(b"s").await));
    first.rollback();

    // A read for update that another commit overtook.
    let mut first = db.begin(Mode::ReadWrite).await;
    log.push(format!("for update: {}", first.get_for_update(b"r").await));
    let mut second = db.begin(Mode::ReadWrite).await;
    log.push(second.set(b"r", b"second"));
    log.push(second.commit().await);
    log.push(first.set(b"other", b"first"));
    log.push(format!("for update first: {}", first.commit().await));

    // A plain read that another commit overtook.
    let mut first = db.begin(Mode::ReadWrite).await;
    log.push(format!("plain read: {}", first.get(b"r").await));
    let mut second = db.begin(Mode::ReadWrite).await;
    log.push(second.set(b"r", b"third"));
    log.push(second.commit().await);
    log.push(first.set(b"other2", b"first"));
    log.push(format!("plain read first: {}", first.commit().await));

    // Two writers of one key: the first to commit wins.
    let mut first = db.begin(Mode::ReadWrite).await;
    let mut second = db.begin(Mode::ReadWrite).await;
    log.push(first.set(b"w", b"first"));
    log.push(format!("first: {}", first.commit().await));
    log.push(second.set(b"w", b"second"));
    log.push(format!("second: {}", second.commit().await));

    let mut check = db.begin(Mode::ReadOnly).await;
    log.push(format!(
        "after: {}",
        check
            .get_many(&[b"k", b"r", b"s", b"other", b"other2", b"w"])
            .await
    ));
    log
}

/// A walk: its name, the cursor it opens and the moves it makes.
type Walk<'a> = (&'a str, Open<'a>, Vec<Move<'a>>);

/// Write down each walk of `walks` on `tx`, its name before each line.
async fn walks(log: &mut Vec<String>, tx: &mut Tx, walks: Vec<Walk<'_>>) {
    for (name, open, moves) in walks {
        for line in tx.walk(&open, &moves).await {
            log.push(format!("{name} {line}"));
        }
    }
}

/// Cursors over ranges, with the transaction's own writes: the moves one by one, past
/// either end, back again, a change of direction, seeks inside and outside the bounds,
/// and every kind of range `scan` takes. Not a range whose start lies past its end:
/// SurrealKV panics on it (`tests/scan.rs` covers what kvshare does).
async fn moves(db: &Db<'_>) -> Vec<String> {
    use Move::{First, Last, Next, Prev, Seek};
    let mut log = Vec::new();
    let mut seed = db.begin(Mode::ReadWrite).await;
    for key in ["a", "b", "c", "d", "e", "f", "g", "h"] {
        log.push(seed.set(key.as_bytes(), key.to_uppercase().as_bytes()));
    }
    log.push(seed.commit().await);

    let mut tx = db.begin(Mode::ReadWrite).await;
    log.push(tx.set(b"c", b"C2"));
    log.push(tx.delete(b"e"));
    log.push(tx.set(b"i", b"I"));
    log.push(tx.soft_delete(b"g"));
    log.push(tx.set(b"bb", b"BB"));
    let range = || Open::Range(b"b", b"h");
    walks(
        &mut log,
        &mut tx,
        vec![
            (
                "steps",
                range(),
                vec![
                    Next, Next, Next, Prev, Prev, Prev, Next, First, Last, Next, Prev, Last, Prev,
                    Prev, Prev, Next, Next, Next, Next, Next, Next,
                ],
            ),
            ("prev first", range(), vec![Prev, Prev, Next, Next]),
            (
                "seeks",
                range(),
                vec![
                    Seek(b"d"),
                    Prev,
                    Seek(b"cc"),
                    Next,
                    Seek(b"a"),
                    Prev,
                    Seek(b"z"),
                    Prev,
                    Next,
                    Seek(b"e"),
                    Prev,
                    Next,
                    Seek(b"h"),
                    Seek(b"g"),
                ],
            ),
            ("iter", Open::Iter, there_and_back(10)),
            ("scan ..", Open::Scan(Bounds::Full), there_and_back(10)),
            (
                "scan b..d",
                Open::Scan(Bounds::HalfOpen(b"b", b"d")),
                there_and_back(4),
            ),
            (
                "scan b..=d",
                Open::Scan(Bounds::Inclusive(b"b", b"d")),
                there_and_back(5),
            ),
            (
                "scan c..",
                Open::Scan(Bounds::From(b"c")),
                there_and_back(8),
            ),
            ("scan ..c", Open::Scan(Bounds::To(b"c")), there_and_back(4)),
            (
                "scan ..=c",
                Open::Scan(Bounds::ToInclusive(b"c")),
                there_and_back(5),
            ),
            (
                "from c",
                Open::RangeWithOptions(Some(b"c"), None),
                there_and_back(8),
            ),
            (
                "to d",
                Open::RangeWithOptions(None, Some(b"d")),
                there_and_back(5),
            ),
            ("one key wide", Open::Range(b"c", b"c"), vec![First, Last]),
        ],
    )
    .await;

    let mut wo = db.begin(Mode::WriteOnly).await;
    walks(&mut log, &mut wo, vec![("wo", Open::Iter, vec![First])]).await;
    let mut done = db.begin(Mode::ReadWrite).await;
    log.push(done.commit().await);
    walks(
        &mut log,
        &mut done,
        vec![("closed", Open::Iter, vec![First])],
    )
    .await;
    log
}

/// A version a scenario writes: the key, the value or `None` for a tombstone, and the
/// time.
type Written = (&'static [u8], Option<&'static [u8]>, u64);

/// Cursors over histories with versions written at chosen times, with the
/// transaction's own writes: tombstones or not, a window of time, a limit spent across a
/// change of direction and renewed by a seek, and moves within a key's versions.
async fn history(db: &Db<'_>) -> Vec<String> {
    use Move::{First, Last, Next, Prev, Seek};
    let mut log = Vec::new();
    let versions: [Written; 11] = [
        (b"a", Some(b"a1"), 100),
        (b"a", Some(b"a2"), 200),
        (b"a", None, 300),
        (b"b", Some(b"b1"), 150),
        (b"c", Some(b"c1"), 120),
        (b"c", Some(b"c2"), 220),
        (b"c", Some(b"c3"), 320),
        (b"d", Some(b"d1"), 130),
        (b"d", Some(b"d2"), 230),
        (b"e", Some(b"e1"), 140),
        (b"f", Some(b"f1"), 160),
    ];
    for (key, value, at) in versions {
        let mut tx = db.begin(Mode::ReadWrite).await;
        log.push(match value {
            Some(value) => tx.set_at(key, value, at),
            None => tx.soft_delete_with_options(key, Some(at)),
        });
        log.push(tx.commit().await);
    }

    let mut tx = db.begin(Mode::ReadWrite).await;
    log.push(tx.set(b"c", b"c-pending"));
    log.push(tx.set_at(b"d", b"d4", 400));
    log.push(tx.soft_delete(b"e"));
    log.push(tx.delete(b"f"));
    let tombstones = || HistoryOptions::new().with_tombstones(true);
    let all = || Open::History(b"a", b"z", tombstones());
    let limited = |limit| Open::History(b"a", b"z", tombstones().with_limit(limit));
    walks(
        &mut log,
        &mut tx,
        vec![
            ("all", all(), there_and_back(16)),
            (
                "values",
                Open::History(b"a", b"z", HistoryOptions::new()),
                there_and_back(16),
            ),
            (
                "window",
                Open::History(b"a", b"z", tombstones().with_ts_range(130, 230)),
                there_and_back(10),
            ),
            (
                "within keys",
                all(),
                vec![
                    First,
                    Next,
                    Next,
                    Next,
                    Prev,
                    Prev,
                    Next,
                    Seek(b"c"),
                    Prev,
                    Prev,
                    Next,
                    Next,
                    Next,
                    Seek(b"cc"),
                    Prev,
                    Seek(b"z"),
                    Prev,
                    Last,
                    Prev,
                    Prev,
                    Prev,
                    Next,
                ],
            ),
            (
                "limit 3",
                limited(3),
                vec![
                    First,
                    Next,
                    Next,
                    Next,
                    Prev,
                    First,
                    Next,
                    Prev,
                    Prev,
                    Prev,
                    Last,
                    Prev,
                    Prev,
                    Prev,
                    Seek(b"c"),
                    Next,
                    Next,
                    Next,
                ],
            ),
            ("limit 0", limited(0), vec![First, Last, Next]),
            (
                "limit 1",
                limited(1),
                vec![Next, Next, Prev, Seek(b"b"), Prev],
            ),
            (
                "narrow",
                Open::History(b"c", b"d", tombstones()),
                there_and_back(6),
            ),
        ],
    )
    .await;

    let mut wo = db.begin(Mode::WriteOnly).await;
    walks(&mut log, &mut wo, vec![("wo", all(), vec![First])]).await;
    log
}

/// Cursors over keys only: the same keys, times and kinds of write as SurrealKV's
/// iterators find, over a range and over a history, with the transaction's own writes.
async fn keys(db: &Db<'_>) -> Vec<String> {
    use Move::{First, Last, Next, Prev, Seek};
    let mut log = Vec::new();
    let mut seed = db.begin(Mode::ReadWrite).await;
    for (key, at) in [("a", 100), ("a", 200), ("b", 150), ("c", 120), ("d", 130)] {
        log.push(seed.set_at(key.as_bytes(), key.as_bytes(), at));
    }
    log.push(seed.commit().await);
    let mut tx = db.begin(Mode::ReadWrite).await;
    log.push(tx.set(b"bb", b"pending"));
    log.push(tx.soft_delete(b"c"));
    log.push(tx.delete(b"d"));
    let tombstones = HistoryOptions::new().with_tombstones(true);
    let walks: Vec<Walk<'_>> = vec![
        ("range", Open::Range(b"a", b"z"), there_and_back(6)),
        (
            "history",
            Open::History(b"a", b"z", tombstones.clone()),
            there_and_back(8),
        ),
        (
            "limit 2",
            Open::History(b"a", b"z", tombstones.with_limit(2)),
            vec![First, Next, Next, Prev, Seek(b"b"), Next, Last, Prev],
        ),
    ];
    for (name, open, moves) in walks {
        for line in tx.walk_reading(&open, &moves, false).await {
            log.push(format!("{name} {line}"));
        }
    }
    log
}

/// Operations on the whole database, and a restore while a transaction is open: what the
/// transaction reads and commits after it, and what a new one reads.
async fn admin(db: &Db<'_>) -> Vec<String> {
    let mut log = Vec::new();
    let mut seed = db.begin(Mode::ReadWrite).await;
    log.push(seed.set(b"a", b"A"));
    log.push(seed.set(b"b", b"B"));
    log.push(seed.commit().await);

    log.push(format!("flush: {}", db.flush_wal(false).await));
    log.push(format!("flush and sync: {}", db.flush_wal(true).await));
    log.push(format!("health: {}", db.health().await));
    log.push(format!("keys: {}", db.prepare_keys().await));
    log.push(format!("encryption: {}", db.encryption_status().await));
    log.push(format!("view: {}", db.view(b"a").await));
    let all = CompactRangeOptions::default();
    log.push(format!(
        "compact all: {}",
        db.compact_range(None, None, &all).await
    ));
    log.push(format!(
        "compact backwards: {}",
        db.compact_range(Some(b"z"), Some(b"a"), &all).await
    ));
    let shallow = CompactRangeOptions {
        flush_memtable: false,
        target_level: Some(1),
        rewrite_bottommost: false,
    };
    log.push(format!(
        "compact a..b to level 1: {}",
        db.compact_range(Some(b"a"), Some(b"b"), &shallow).await
    ));

    // SurrealKV's errors name the paths, which differ from one database to another.
    let anonymous = |line: String| line.replace(&db.path().display().to_string(), "<db>");
    let saved = db.path().with_extension("ckpt");
    log.push(format!(
        "checkpoint: {}",
        db.create_checkpoint(&saved).await
    ));
    log.push(anonymous(format!(
        "checkpoint into the database: {}",
        db.create_checkpoint(db.path()).await
    )));
    log.push(anonymous(format!(
        "checkpoint inside the database: {}",
        db.create_checkpoint(&db.path().join("inside")).await
    )));

    let mut later = db.begin(Mode::ReadWrite).await;
    log.push(later.set(b"c", b"C"));
    log.push(later.delete(b"a"));
    log.push(later.commit().await);

    let mut open = db.begin(Mode::ReadWrite).await;
    log.push(format!("open reads: {}", open.get(b"c").await));
    log.push(open.set(b"t", b"T"));
    log.push(format!(
        "restore: {}",
        db.restore_from_checkpoint(&saved).await
    ));
    log.push(format!(
        "open after: {}",
        open.range(b"a", b"z", false).await
    ));
    log.push(format!("open reads a: {}", open.get(b"a").await));
    log.push(format!("open commits: {}", open.commit().await));
    let mut after = db.begin(Mode::ReadOnly).await;
    log.push(format!(
        "new after: {}",
        after.range(b"a", b"z", false).await
    ));
    log.push(anonymous(format!(
        "restore from nothing: {}",
        db.restore_from_checkpoint(&db.path().with_extension("none"))
            .await
    )));
    log
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cursors_over_keys_only_are_the_same() {
    compare("keys", &[1, 2, 256], keys).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn admin_is_the_same() {
    compare("admin", &[2], admin).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn refusals_are_the_same() {
    compare("refusals", &[2], refusals).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn savepoints_are_the_same() {
    compare("savepoints", &[2], savepoints).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn writes_are_the_same() {
    compare("writes", &[2], writes).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reads_are_the_same() {
    compare("reads", &[2], reads).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn conflicts_are_the_same() {
    compare("conflicts", &[2], conflicts).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cursor_moves_are_the_same() {
    compare("moves", &[1, 2, 256], moves).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn history_cursors_are_the_same() {
    compare("history", &[1, 2, 256], history).await;
}
