//! What runs on a SurrealKV transaction for a request: a client's writes applied in
//! their order, and the pages of a cursor. The owner runs it for its clients, and the
//! owner's own transactions run the same code: their cursors read pages here too.
//!
//! A page is read by a new SurrealKV iterator on the transaction, so the pages of one
//! cursor see the same snapshot and the same writes of the transaction. A page stops at
//! its limit of entries or at [`PAGE_BYTES`] of keys and values, whichever comes first,
//! but always holds one entry if there is one.
//!
//! A page names each entry by its [`Position`]: the key, and which of the key's entries
//! it is, counted from the end the page came from. Counting needs no extra reads: a step
//! to another key starts at 0, and a step within a key moves the count by one. A later
//! page finds the entry again by seeking its key and stepping as many times.

use surrealkv::{HistoryOptions, LSMIterator, Transaction, WriteOptions};

use crate::protocol::{Answer, At, Fetched, Item, Nth, PAGE_BYTES, Position, Source, Write};
use crate::types::just_past;

/// Apply `writes` to `transaction` in their order, each by the SurrealKV method the
/// client called. SurrealKV decides what a write leaves of the earlier writes of the
/// same key, as it does in one process.
pub(crate) fn apply(transaction: &mut Transaction, writes: Vec<Write>) -> surrealkv::Result<()> {
    for write in writes {
        match write {
            Write::Set { key, value } => transaction.set(key, value)?,
            Write::SetAt {
                key,
                value,
                timestamp,
            } => transaction.set_at(key, value, timestamp)?,
            Write::SetWithOptions {
                key,
                value,
                timestamp,
            } => transaction.set_with_options(key, value, &with_timestamp(timestamp))?,
            Write::Delete { key } => transaction.delete(key)?,
            Write::DeleteWithOptions { key, timestamp } => {
                transaction.delete_with_options(key, &with_timestamp(timestamp))?
            }
            Write::SoftDelete { key } => transaction.soft_delete(key)?,
            Write::SoftDeleteWithOptions { key, timestamp } => {
                transaction.soft_delete_with_options(key, &with_timestamp(timestamp))?
            }
            Write::Replace { key, value } => transaction.replace(key, value)?,
            Write::DeleteRange { start, end } => transaction.delete_range(start, end)?,
            Write::SetSavepoint => transaction.set_savepoint()?,
            Write::RollbackToSavepoint => transaction.rollback_to_savepoint()?,
        }
    }
    Ok(())
}

/// The values of the first of `keys`, each read by `get`: as many as hold
/// [`PAGE_BYTES`] together, and one at least. The client asks for the keys that are
/// left, so the values of many keys come in parts, as the pages of a cursor do: no
/// answer outgrows a frame, and no request keeps the owner's thread for long.
pub(crate) fn many(
    keys: &[Vec<u8>],
    mut get: impl FnMut(&[u8]) -> surrealkv::Result<Option<Vec<u8>>>,
) -> surrealkv::Result<Answer> {
    let (mut values, mut bytes) = (Vec::new(), 0);
    for key in keys {
        if bytes >= PAGE_BYTES {
            break;
        }
        let value = get(key)?;
        bytes += value.as_ref().map_or(0, Vec::len);
        values.push(value);
    }
    Ok(Answer::GotMany(values))
}

/// The `WriteOptions` a client gave, which hold only a time.
fn with_timestamp(timestamp: Option<u64>) -> WriteOptions {
    WriteOptions::new().with_timestamp(timestamp)
}

/// A SurrealKV iterator of `source` on `transaction`, with no limit: a cursor spends its
/// limit itself. Opening checks what SurrealKV checks of the transaction.
pub(crate) fn open<'t>(
    transaction: &'t Transaction,
    source: &Source,
) -> surrealkv::Result<Box<dyn LSMIterator + 't>> {
    match source {
        Source::Range { lower, upper } => {
            let mut options = surrealkv::ReadOptions::new();
            options.set_iterate_lower_bound(lower.clone());
            options.set_iterate_upper_bound(upper.clone());
            Ok(Box::new(transaction.range_with_options(&options)?))
        }
        Source::History {
            start,
            end,
            tombstones,
            between,
        } => {
            let mut options = HistoryOptions::new().with_tombstones(*tombstones);
            if let Some((from, to)) = *between {
                options = options.with_ts_range(from, to);
            }
            Ok(Box::new(transaction.history_with_options(
                start.as_slice(),
                end.as_slice(),
                &options,
            )?))
        }
    }
}

/// Up to `limit` entries of `source` on `transaction` from `at`, in the order they were
/// read; and whether more follow that way. Without `values`, no value is read, so a
/// value in the value log is not looked up.
pub(crate) fn page(
    transaction: &Transaction,
    source: &Source,
    at: At,
    limit: usize,
    values: bool,
) -> surrealkv::Result<(Vec<Item>, bool)> {
    let mut cursor = open(transaction, source)?;
    let forwards = at.forwards();
    let (mut valid, mut previous) = match at {
        At::First => (cursor.seek_first()?, None),
        At::Last => (cursor.seek_last()?, None),
        At::Seek(target) => (cursor.seek(&target)?, None),
        At::After(position) => {
            locate(&mut *cursor, &position)?;
            (cursor.next()?, Some(position))
        }
        At::Before(position) => {
            locate(&mut *cursor, &position)?;
            (cursor.prev()?, Some(position))
        }
    };
    let (mut items, mut bytes) = (Vec::new(), 0);
    while valid {
        if items.len() >= limit.max(1) || bytes >= PAGE_BYTES {
            return Ok((items, true));
        }
        let (key, timestamp) = {
            let key = cursor.key();
            (key.encoded().to_vec(), key.timestamp())
        };
        let value = if !values {
            Fetched::Skipped
        } else {
            match cursor.value() {
                Ok(value) => Fetched::Value(value),
                Err(error) => Fetched::Failed(error.into()),
            }
        };
        let mut item = Item {
            key,
            timestamp,
            nth: Nth::FromNewest(0),
            value,
        };
        item.nth = step(previous.as_ref(), item.user_key(), forwards);
        bytes += item.key.len();
        if let Fetched::Value(value) = &item.value {
            bytes += value.len();
        }
        previous = Some(item.position());
        items.push(item);
        valid = if forwards {
            cursor.next()?
        } else {
            cursor.prev()?
        };
    }
    Ok((items, false))
}

/// Which of its key's entries the entry of `key` is, reached by one step forwards or
/// backwards from `previous`, or by a seek when there is no `previous`.
fn step(previous: Option<&Position>, key: &[u8], forwards: bool) -> Nth {
    match previous {
        Some(previous) if previous.key == key => match (previous.nth, forwards) {
            (Nth::FromNewest(n), true) => Nth::FromNewest(n + 1),
            (Nth::FromNewest(n), false) => Nth::FromNewest(n.saturating_sub(1)),
            (Nth::FromOldest(n), true) => Nth::FromOldest(n.saturating_sub(1)),
            (Nth::FromOldest(n), false) => Nth::FromOldest(n + 1),
        },
        _ if forwards => Nth::FromNewest(0),
        _ => Nth::FromOldest(0),
    }
}

/// Put `cursor` on the entry at `position`. A cursor keeps its transaction borrowed, so
/// the transaction has not changed since the page that named the entry; an entry that
/// is not there is a client out of step with its owner.
fn locate(cursor: &mut dyn LSMIterator, position: &Position) -> surrealkv::Result<()> {
    let (mut valid, steps, forwards) = match position.nth {
        Nth::FromNewest(n) => (cursor.seek(&position.key)?, n, true),
        // The key's oldest entry is the one before the next key's newest, or the last of
        // all when no key follows.
        Nth::FromOldest(n) => {
            let valid = if cursor.seek(&just_past(&position.key))? {
                cursor.prev()?
            } else {
                cursor.seek_last()?
            };
            (valid, n, false)
        }
    };
    for _ in 0..steps {
        if !valid {
            break;
        }
        valid = if forwards {
            cursor.next()?
        } else {
            cursor.prev()?
        };
    }
    if valid && cursor.key().user_key() == position.key.as_slice() {
        Ok(())
    } else {
        Err(surrealkv::Error::InvalidArgument(format!(
            "a cursor's entry {position:?} is not in its transaction"
        )))
    }
}
