//! What runs on a SurrealKV transaction for a request: a client's writes applied in
//! their order, and pages of range and history reads. The owner runs it for its clients,
//! and the owner's own transactions run the same code.
//!
//! A page is a new SurrealKV read of the transaction, so the pages of one scan see the
//! same snapshot and the same writes of the transaction. A page stops at its limit of
//! entries or at [`PAGE_BYTES`] of keys and values, whichever comes first, but always
//! holds one entry if there is one.

use surrealkv::{HistoryOptions, LSMIterator, Transaction};

use crate::protocol::{Entry, HistoryFilter, PAGE_BYTES, Version, Write};

/// Apply `writes` to `transaction` in their order. SurrealKV decides what a write leaves
/// of the earlier writes of the same key, as it does in one process.
pub(crate) fn apply(transaction: &mut Transaction, writes: Vec<Write>) -> surrealkv::Result<()> {
    for write in writes {
        match write {
            Write::Set { key, value } => transaction.set(key, value)?,
            Write::Delete { key } => transaction.delete(key)?,
            Write::SoftDelete { key } => transaction.soft_delete(key)?,
        }
    }
    Ok(())
}

/// Up to `limit` entries of `[start, end)` as `transaction` sees them, in key order or,
/// with `reverse`, backwards; and whether more follow.
pub(crate) fn range_page(
    transaction: &Transaction,
    start: &[u8],
    end: &[u8],
    reverse: bool,
    limit: usize,
) -> surrealkv::Result<(Vec<Entry>, bool)> {
    let mut cursor = transaction.range(start, end)?;
    let mut valid = if reverse {
        cursor.seek_last()?
    } else {
        cursor.seek_first()?
    };
    let (mut entries, mut bytes) = (Vec::new(), 0);
    while valid {
        if entries.len() >= limit.max(1) || bytes >= PAGE_BYTES {
            return Ok((entries, true));
        }
        let key = cursor.key().user_key().to_vec();
        let value = cursor.value()?;
        bytes += key.len() + value.len();
        entries.push((key, value));
        valid = if reverse {
            cursor.prev()?
        } else {
            cursor.next()?
        };
    }
    Ok((entries, false))
}

/// Up to `limit` versions of the keys of `[start, end)` that pass `filter`, the keys in
/// order and the versions of a key newest first, past the first `skip` versions of
/// `start` itself; and whether more follow.
pub(crate) fn history_page(
    transaction: &Transaction,
    start: &[u8],
    end: &[u8],
    skip: u64,
    filter: HistoryFilter,
    limit: usize,
) -> surrealkv::Result<(Vec<Version>, bool)> {
    let mut options = HistoryOptions::new().with_tombstones(filter.tombstones);
    if let Some((from, to)) = filter.between {
        options = options.with_ts_range(from, to);
    }
    let mut cursor = transaction.history_with_options(start, end, &options)?;
    let mut valid = cursor.seek_first()?;
    let mut skipped = 0;
    while valid && skipped < skip && cursor.key().user_key() == start {
        skipped += 1;
        valid = cursor.next()?;
    }
    let (mut versions, mut bytes) = (Vec::new(), 0);
    while valid {
        if versions.len() >= limit.max(1) || bytes >= PAGE_BYTES {
            return Ok((versions, true));
        }
        let (key, timestamp, tombstone) = {
            let at = cursor.key();
            (at.user_key().to_vec(), at.timestamp(), at.is_tombstone())
        };
        let value = if tombstone {
            None
        } else {
            Some(cursor.value()?)
        };
        bytes += key.len() + value.as_ref().map_or(0, Vec::len);
        versions.push(Version {
            key,
            timestamp,
            value,
        });
        valid = cursor.next()?;
    }
    Ok((versions, false))
}
