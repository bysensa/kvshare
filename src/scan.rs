//! Range and history reads as cursors that fetch pages, the same for a transaction of
//! the owner and for one of a client.
//!
//! A cursor holds its transaction by `&mut` until it is dropped, so no write can come
//! between two pages, as none can while a SurrealKV iterator borrows its transaction.
//! Each page is a new read of the same transaction: the same snapshot and the same writes.

use std::collections::VecDeque;

use crate::api::Transaction;
use crate::error::Result;
use crate::protocol::{DEFAULT_PAGE, Entry, HistoryFilter, Version};

/// The entries of a key range, in key order or backwards.
pub struct Scan<'t> {
    tx: &'t mut Transaction,
    /// What is left to read: `[start, end)`.
    start: Vec<u8>,
    end: Vec<u8>,
    reverse: bool,
    page: usize,
    ready: VecDeque<Entry>,
    more: bool,
}

impl<'t> Scan<'t> {
    pub(crate) fn new(tx: &'t mut Transaction, start: &[u8], end: &[u8]) -> Self {
        Scan {
            tx,
            start: start.to_vec(),
            end: end.to_vec(),
            reverse: false,
            page: DEFAULT_PAGE,
            ready: VecDeque::new(),
            more: true,
        }
    }

    /// Read from the end of the range backwards.
    pub fn reverse(mut self) -> Self {
        self.reverse = true;
        self
    }

    /// Fetch at most `entries` entries a page (at least one).
    pub fn page(mut self, entries: usize) -> Self {
        self.page = entries.max(1);
        self
    }

    /// The next entry, fetching a page when the last one is used up.
    pub async fn next(&mut self) -> Result<Option<Entry>> {
        if self.ready.is_empty() && self.more {
            let (entries, more) = self
                .tx
                .range_page(&self.start, &self.end, self.reverse, self.page)
                .await?;
            // The rest of the range lies past the last key read: after it going forward
            // (the smallest key greater than it is the key with a zero byte appended),
            // before it going backwards.
            if let Some((last, _)) = entries.last() {
                if self.reverse {
                    self.end = last.clone();
                } else {
                    self.start = last.clone();
                    self.start.push(0);
                }
            }
            self.more = more && !entries.is_empty();
            self.ready = entries.into();
        }
        Ok(self.ready.pop_front())
    }
}

/// The versions of the keys of a range: keys in order, the versions of a key newest first.
pub struct History<'t> {
    tx: &'t mut Transaction,
    /// What is left to read: `[start, end)`, past the first `skip` versions of `start`.
    start: Vec<u8>,
    end: Vec<u8>,
    skip: u64,
    filter: HistoryFilter,
    page: usize,
    ready: VecDeque<Version>,
    more: bool,
}

impl<'t> History<'t> {
    pub(crate) fn new(tx: &'t mut Transaction, start: &[u8], end: &[u8]) -> Self {
        History {
            tx,
            start: start.to_vec(),
            end: end.to_vec(),
            skip: 0,
            filter: HistoryFilter::default(),
            page: DEFAULT_PAGE,
            ready: VecDeque::new(),
            more: true,
        }
    }

    /// Return tombstones too, as versions without a value.
    pub fn with_tombstones(mut self) -> Self {
        self.filter.tombstones = true;
        self
    }

    /// Return only the versions written between `from` and `to` (nanoseconds, both
    /// included).
    pub fn between(mut self, from: u64, to: u64) -> Self {
        self.filter.between = Some((from, to));
        self
    }

    /// Fetch at most `versions` versions a page (at least one).
    pub fn page(mut self, versions: usize) -> Self {
        self.page = versions.max(1);
        self
    }

    /// The next version, fetching a page when the last one is used up.
    pub async fn next(&mut self) -> Result<Option<Version>> {
        if self.ready.is_empty() && self.more {
            let (versions, more) = self
                .tx
                .history_page(&self.start, &self.end, self.skip, self.filter, self.page)
                .await?;
            // The rest begins at the last key read, past its versions read so far: a key's
            // versions can run over several pages, and counting them is exact even when
            // two have the same time.
            if let Some(last) = versions.last() {
                let of_last = versions
                    .iter()
                    .rev()
                    .take_while(|version| version.key == last.key)
                    .count() as u64;
                self.skip = if last.key == self.start {
                    self.skip + of_last
                } else {
                    of_last
                };
                self.start = last.key.clone();
            }
            self.more = more && !versions.is_empty();
            self.ready = versions.into();
        }
        Ok(self.ready.pop_front())
    }
}
