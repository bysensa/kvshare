//! The same operations on SurrealKV itself, on the owner's tree and through a client of
//! another owner: every answer, a value or an error, is the same.
//!
//! Each scenario writes down what each operation answered, and the three records must
//! match line for line. Times are written down only when the scenario chose them; a
//! commit's own time differs from one database to another.

mod common;

use common::{base, builder, close, owner_and_client};
use kvshare::{Durability, Error, Mode, ReadOptions, WriteOptions};
use surrealkv::{HistoryOptions, LSMIterator};

/// A database under test.
enum Db<'a> {
    Plain(&'a surrealkv::Tree),
    Shared(&'a kvshare::Tree),
}

impl Db<'_> {
    async fn begin(&self, mode: Mode) -> Tx {
        match self {
            Db::Plain(tree) => Tx::Plain(tree.begin_with_mode(mode).expect("a transaction")),
            Db::Shared(tree) => {
                Tx::Shared(tree.begin_with_mode(mode).await.expect("a transaction"))
            }
        }
    }
}

/// A transaction under test.
enum Tx {
    Plain(surrealkv::Transaction),
    Shared(kvshare::Transaction),
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
            Tx::Shared(t) => said(t.$method($($arg),*)),
        }
    };
}

/// A read: synchronous in SurrealKV, asynchronous in kvshare.
macro_rules! read {
    ($tx:expr, $method:ident($($arg:expr),*)) => {
        match $tx {
            Tx::Plain(t) => said(plain(t.$method($($arg),*))),
            Tx::Shared(t) => said(t.$method($($arg),*).await),
        }
    };
}

fn timestamp(at: Option<u64>) -> WriteOptions {
    WriteOptions::new().with_timestamp(at)
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
            Tx::Shared(t) => t.rollback(),
        }
    }

    async fn commit(&mut self) -> String {
        match self {
            Tx::Plain(t) => said(plain(t.commit().await)),
            Tx::Shared(t) => said(t.commit().await),
        }
    }

    async fn get(&mut self, key: &[u8]) -> String {
        read!(self, get(key))
    }

    async fn get_async(&mut self, key: &[u8]) -> String {
        match self {
            Tx::Plain(t) => said(plain(t.get_async(key).await)),
            Tx::Shared(t) => said(t.get_async(key).await),
        }
    }

    fn set_durability(&mut self, durability: Durability) {
        match self {
            Tx::Plain(t) => t.set_durability(durability),
            Tx::Shared(t) => t.set_durability(durability),
        }
    }

    /// `with_durability`, which takes the transaction and gives it back.
    fn with_durability(self, durability: Durability) -> Self {
        match self {
            Tx::Plain(t) => Tx::Plain(t.with_durability(durability)),
            Tx::Shared(t) => Tx::Shared(t.with_durability(durability)),
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
            Tx::Shared(t) => said(t.get_with_options(key, &options).await),
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
            (Tx::Shared(t), None) => t.get_many_with(keys, note).await,
            (Tx::Shared(t), Some(at)) => t.get_many_at_with(keys, at, note).await,
        };
        said(answer.map(|()| seen))
    }

    /// The entries of `[start, end)`, in key order or backwards; through kvshare, in
    /// pages of two.
    async fn range(&mut self, start: &[u8], end: &[u8], reverse: bool) -> String {
        let answer = match self {
            Tx::Plain(t) => plain(plain_range(t, start, end, reverse)),
            Tx::Shared(t) => {
                let mut scan = t.range(start, end).page(2);
                if reverse {
                    scan = scan.reverse();
                }
                let mut seen = Vec::new();
                loop {
                    match scan.next().await {
                        Ok(Some((key, value))) => {
                            seen.push(format!("{}={}", lossy(&key), lossy(&value)));
                        }
                        Ok(None) => break Ok(seen),
                        Err(error) => break Err(error),
                    }
                }
            }
        };
        said(answer)
    }

    /// Every version of the keys of `[start, end)`, tombstones too; through kvshare, in
    /// pages of two.
    async fn history(&mut self, start: &[u8], end: &[u8]) -> String {
        let answer = match self {
            Tx::Plain(t) => plain(plain_history(t, start, end)),
            Tx::Shared(t) => {
                let mut read = t.history(start, end).with_tombstones().page(2);
                let mut seen = Vec::new();
                loop {
                    match read.next().await {
                        Ok(Some(version)) => seen.push(format!(
                            "{}@{}={}",
                            lossy(&version.key),
                            stamp(version.timestamp),
                            version.value.as_deref().map_or("-".into(), lossy)
                        )),
                        Ok(None) => break Ok(seen),
                        Err(error) => break Err(error),
                    }
                }
            }
        };
        said(answer)
    }
}

fn plain_range(
    t: &surrealkv::Transaction,
    start: &[u8],
    end: &[u8],
    reverse: bool,
) -> surrealkv::Result<Vec<String>> {
    let mut cursor = t.range(start, end)?;
    let mut valid = if reverse {
        cursor.seek_last()?
    } else {
        cursor.seek_first()?
    };
    let mut seen = Vec::new();
    while valid {
        let key = lossy(cursor.key().user_key());
        seen.push(format!("{key}={}", lossy(&cursor.value()?)));
        valid = if reverse {
            cursor.prev()?
        } else {
            cursor.next()?
        };
    }
    Ok(seen)
}

fn plain_history(
    t: &surrealkv::Transaction,
    start: &[u8],
    end: &[u8],
) -> surrealkv::Result<Vec<String>> {
    let options = HistoryOptions::new().with_tombstones(true);
    let mut cursor = t.history_with_options(start, end, &options)?;
    let mut valid = cursor.seek_first()?;
    let mut seen = Vec::new();
    while valid {
        let (key, at, tombstone) = {
            let at = cursor.key();
            (lossy(at.user_key()), at.timestamp(), at.is_tombstone())
        };
        let value = if tombstone {
            "-".into()
        } else {
            lossy(&cursor.value()?)
        };
        seen.push(format!("{key}@{}={value}", stamp(at)));
        valid = cursor.next()?;
    }
    Ok(seen)
}

/// Run `scenario` on SurrealKV itself, on a tree that owns its database, and on a client
/// of a tree that owns another; the three records must be the same.
///
/// The directories have short names: a socket path must fit in 103 bytes, and the
/// temporary directory of macOS takes half of them.
async fn compare(name: &str, scenario: impl AsyncFn(&Db<'_>) -> Vec<String>) {
    let base = base(&format!("d-{name}"));

    let tree = surrealkv::TreeBuilder::new()
        .with_path(base.join("p"))
        .with_versioning(true, 0)
        .build()
        .expect("SurrealKV opens");
    let expected = scenario(&Db::Plain(&tree)).await;
    tree.close().await.expect("SurrealKV closes");
    eprintln!("{name}, as SurrealKV answers:\n  {}", expected.join("\n  "));

    let alone = base.join("o");
    let owner = builder(&alone).build().await.expect("the owner");
    let local = scenario(&Db::Shared(&owner)).await;
    owner.close().await.expect("the owner closes");

    let shared = base.join("c");
    let (owner, client) = owner_and_client(&shared).await;
    let remote = scenario(&Db::Shared(&client)).await;
    close(owner, client, &shared).await;

    for (line, expected) in expected.iter().enumerate() {
        assert_eq!(local.get(line), Some(expected), "{name}, the owner's tree");
        assert_eq!(
            remote.get(line),
            Some(expected),
            "{name}, through the socket"
        );
    }
    assert_eq!(local.len(), expected.len(), "{name}, the owner's tree");
    assert_eq!(remote.len(), expected.len(), "{name}, through the socket");
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn refusals_are_the_same() {
    compare("refusals", refusals).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn savepoints_are_the_same() {
    compare("savepoints", savepoints).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn writes_are_the_same() {
    compare("writes", writes).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reads_are_the_same() {
    compare("reads", reads).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn conflicts_are_the_same() {
    compare("conflicts", conflicts).await;
}
