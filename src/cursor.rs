//! Cursors over a transaction's keys and versions, with the moves of SurrealKV's
//! `LSMIterator`.
//!
//! A cursor is a window of entries read one page at a time; moving past either end of
//! the window reads the page beyond it. In the owner a page is read from SurrealKV in
//! this process, in a client the owner reads it. A cursor does not hold SurrealKV's
//! iterator between moves: that iterator cannot cross threads, and a cursor whose moves
//! are awaited has to.
//!
//! The cursor repeats what SurrealKV's iterators do on their own: a cursor not yet
//! positioned seeks on its first move, a cursor that is no longer valid stays so until a
//! seek, and a history's limit is spent by every entry a move lands on, kept across a
//! change of direction and renewed by a seek.
//!
//! A cursor holds its transaction borrowed, as SurrealKV's iterator does, so nothing is
//! written to the transaction while it lives: its pages see the same writes.

use std::borrow::Cow;

use surrealkv::{InternalKeyKind, Value};

use crate::api::Transaction;
use crate::error::{Error, Result};
use crate::protocol::{At, DEFAULT_PAGE, Fetched, Item, Source};

/// A key a cursor stands on, as SurrealKV's `InternalKeyRef`: the user's key, the
/// sequence number and the kind of the write, and the time the version was written at.
///
/// kvshare has its own because SurrealKV lets nobody else set the time of its
/// `InternalKeyRef`; the encoded key is read by SurrealKV's own.
#[derive(Clone, Copy)]
pub struct InternalKeyRef<'a> {
    key: surrealkv::InternalKeyRef<'a>,
    timestamp: u64,
}

impl<'a> InternalKeyRef<'a> {
    pub fn user_key(&self) -> &'a [u8] {
        self.key.user_key()
    }

    pub fn encoded(&self) -> &'a [u8] {
        self.key.encoded()
    }

    pub fn trailer(&self) -> u64 {
        self.key.trailer()
    }

    pub fn seq_num(&self) -> u64 {
        self.key.seq_num()
    }

    /// When the version was written, in nanoseconds: `u64::MAX` for a write of the
    /// transaction not committed yet, and 0 from a cursor that does not report times;
    /// only a history does.
    pub fn timestamp(&self) -> u64 {
        self.timestamp
    }

    pub fn kind(&self) -> InternalKeyKind {
        self.key.kind()
    }

    pub fn is_tombstone(&self) -> bool {
        self.key.is_tombstone()
    }

    pub fn is_hard_delete_marker(&self) -> bool {
        self.key.is_hard_delete_marker()
    }

    pub fn is_replace(&self) -> bool {
        self.key.is_replace()
    }
}

impl std::fmt::Debug for InternalKeyRef<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InternalKeyRef")
            .field("user_key", &self.user_key())
            .field("seq_num", &self.seq_num())
            .field("kind", &self.kind())
            .field("timestamp", &self.timestamp)
            .finish()
    }
}

/// A cursor over a range of a transaction's keys or over their versions, with the moves
/// of SurrealKV's `LSMIterator`. Moves are `async`: in a client they may ask the owner.
pub struct Cursor<'t> {
    transaction: &'t Transaction,
    source: Source,
    /// A history's limit, and how much of it the traversal has spent.
    limit: Option<usize>,
    spent: usize,
    page: usize,
    /// Whether pages carry values, or keys only.
    values: bool,
    /// Whether a seek has positioned the cursor yet.
    started: bool,
    /// The page read last, in key order.
    items: Vec<Item>,
    /// Where the cursor stands in `items`; `None` when it is not valid.
    at: Option<usize>,
    /// Whether entries may lie before and after the window.
    before: bool,
    after: bool,
}

impl<'t> Cursor<'t> {
    pub(crate) fn new(transaction: &'t Transaction, source: Source, limit: Option<usize>) -> Self {
        Cursor {
            transaction,
            source,
            limit,
            spent: 0,
            page: DEFAULT_PAGE,
            values: true,
            started: false,
            items: Vec::new(),
            at: None,
            before: false,
            after: false,
        }
    }

    /// How many entries the cursor reads at once (at least one); 256 unless set. A
    /// client's cursor asks the owner once a page.
    pub fn set_page_size(&mut self, entries: usize) {
        self.page = entries.max(1);
    }

    /// Read keys only, or values too (the default), from the next page the cursor reads
    /// on: set it before the first move. A cursor over keys only never looks a value up
    /// in the value log, and its `value`, `value_ref` and `value_into` answer
    /// `Error::InvalidArgument`. This stands in for SurrealKV's `value_encoded`, which
    /// lets a reader skip the value log too.
    pub fn set_keys_only(&mut self, keys_only: bool) {
        self.values = !keys_only;
    }

    /// Stand on the first entry at or past `target`, a user's key; for a history, on
    /// the key's newest version.
    pub async fn seek(&mut self, target: &[u8]) -> Result<bool> {
        self.start(At::Seek(target.to_vec())).await
    }

    pub async fn seek_first(&mut self) -> Result<bool> {
        self.start(At::First).await
    }

    pub async fn seek_last(&mut self) -> Result<bool> {
        self.start(At::Last).await
    }

    pub async fn next(&mut self) -> Result<bool> {
        if !self.started {
            return self.seek_first().await;
        }
        let Some(at) = self.at else {
            return Ok(false);
        };
        if at + 1 < self.items.len() {
            self.at = Some(at + 1);
        } else if !self.after || self.spent_all() {
            self.at = None;
        } else {
            self.load(At::After(self.items[at].position())).await?;
        }
        Ok(self.spend())
    }

    pub async fn prev(&mut self) -> Result<bool> {
        if !self.started {
            return self.seek_last().await;
        }
        let Some(at) = self.at else {
            return Ok(false);
        };
        if at > 0 {
            self.at = Some(at - 1);
        } else if !self.before || self.spent_all() {
            self.at = None;
        } else {
            self.load(At::Before(self.items[0].position())).await?;
        }
        Ok(self.spend())
    }

    pub fn valid(&self) -> bool {
        self.at.is_some()
    }

    /// The key the cursor stands on. Panics if the cursor is not valid, as SurrealKV's
    /// iterator does.
    pub fn key(&self) -> InternalKeyRef<'_> {
        let item = self.item();
        InternalKeyRef {
            key: surrealkv::InternalKeyRef::from_encoded(&item.key),
            timestamp: item.timestamp,
        }
    }

    /// The value the cursor stands on.
    pub fn value(&self) -> Result<Value> {
        Ok(self.value_ref()?.into_owned())
    }

    pub fn value_ref(&self) -> Result<Cow<'_, [u8]>> {
        match &self.item().value {
            Fetched::Value(value) => Ok(Cow::Borrowed(value)),
            Fetched::Failed(error) => Err(Error::from(error.clone())),
            Fetched::Skipped => Err(Error::InvalidArgument("the cursor reads keys only".into())),
        }
    }

    /// Copy the value the cursor stands on into `buf`, which is cleared first.
    pub fn value_into(&self, buf: &mut Vec<u8>) -> Result<()> {
        let value = self.value_ref()?;
        buf.clear();
        buf.extend_from_slice(&value);
        Ok(())
    }

    fn item(&self) -> &Item {
        let at = self
            .at
            .expect("key() or value() called on an invalid cursor");
        &self.items[at]
    }

    /// A seek: a new traversal, which renews the limit.
    async fn start(&mut self, at: At) -> Result<bool> {
        self.started = true;
        self.spent = 0;
        self.load(at).await?;
        Ok(self.spend())
    }

    /// Whether the limit leaves no entry to land on.
    fn spent_all(&self) -> bool {
        self.limit.is_some_and(|limit| self.spent >= limit)
    }

    /// Count the entry the cursor landed on against the limit, as SurrealKV does: once
    /// the limit is spent the cursor is no longer valid.
    fn spend(&mut self) -> bool {
        if self.at.is_none() {
            return false;
        }
        if self.spent_all() {
            self.at = None;
            return false;
        }
        self.spent += 1;
        true
    }

    /// Read the page from `at` and stand on its first entry in the order it was read; or
    /// on nothing if it is empty. A page holds no more entries than the limit leaves.
    async fn load(&mut self, at: At) -> Result<()> {
        let wanted = match self.limit {
            Some(limit) => self.page.min(limit.saturating_sub(self.spent).max(1)),
            None => self.page,
        };
        let forwards = at.forwards();
        let (before, after) = match &at {
            At::First => (false, true),
            At::Last => (true, false),
            _ => (true, true),
        };
        let (mut items, more) = self
            .transaction
            .page(&self.source, at, wanted, self.values)
            .await?;
        if items.is_empty() {
            self.items.clear();
            self.at = None;
            return Ok(());
        }
        if forwards {
            self.at = Some(0);
            self.before = before;
            self.after = more;
        } else {
            items.reverse();
            self.at = Some(items.len() - 1);
            self.before = more;
            self.after = after;
        }
        self.items = items;
        Ok(())
    }
}
