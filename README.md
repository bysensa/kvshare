# kvshare

One [SurrealKV](https://github.com/surrealdb/surrealkv) database shared by several
processes on one machine, behind SurrealKV's own API.

SurrealKV lets one process open a database: opening takes an exclusive `flock` on
`<db>/LOCK`. kvshare turns that lock into an election. The process that opens the
database owns it and serves it on a Unix socket, whose path it leaves in the
database's directory. A process that finds the database locked connects to that socket
and works through the owner. When the owner exits or
dies, the kernel releases its lock, its clients lose their connection, and the next
process to open the database becomes the owner.

```rust
use kvshare::TreeBuilder;

async fn example() -> kvshare::Result<()> {
    // Every process runs the same code; the first to open the database owns it.
    let tree = TreeBuilder::new()
        .with_path("/var/lib/app/db".into())
        .with_versioning(true, 0)
        .build()
        .await?;
    let mut tx = tree.begin().await?;
    tx.set(b"key", b"value")?;
    tx.commit().await?;
    let tx = tree.begin().await?;
    assert_eq!(tx.get(b"key").await?.as_deref(), Some(&b"value"[..]));
    // Cursors move as SurrealKV's iterators do, awaited.
    let mut cursor = tx.scan(b"a".as_slice()..)?;
    let mut valid = cursor.seek_first().await?;
    while valid {
        println!("{:?} = {:?}", cursor.key().user_key(), cursor.value()?);
        valid = cursor.next().await?;
    }
    drop(cursor);
    drop(tx);
    tree.close().await
}
```

## Status

| Stage | What | State |
|---|---|---|
| S0 | This repository, with the spike's code split into modules | done |
| S1 | `TreeBuilder`, `Tree`, `Transaction` with SurrealKV's names; point reads and writes, savepoints, commit and rollback | done |
| S2 | Cursors with `seek`, `next`, `prev`; history | done |
| S3 | `Tree`'s administrative operations through the owner | done |
| S4 | Robustness, a cursor over keys only, documentation | done |
| S5 | What a review found: how a tree ends, what is too large for a frame, a bounded queue, commits of one client together, where the processes meet, protocol versions, `Tree::transact`, `tracing` | done |

## Boundaries

- macOS and Linux. Not Windows, not a browser (`with_file_system`), not network file
  systems, not several machines.
- Tested on macOS only: no test has run on Linux yet.
- The owner and its clients are one user's processes. Each end of the socket refuses a
  process of another user; no test covers the refusal, which takes a second user.
- SurrealKV `=1.4.0`.

## Mapping to SurrealKV's API

The rule: kvshare keeps SurrealKV's type and method names, and a method is `async`
when it may reach the owner: opening the tree, `begin`, every read, the moves of a
cursor and `commit`. Writes, savepoints, `rollback` and opening a cursor stay
synchronous. A client keeps a transaction's writes until
its next read or its commit, which carry them to the owner in their order; the owner
applies each by the SurrealKV method the client called, so SurrealKV itself decides what
the transaction sees.

| SurrealKV | kvshare | Notes |
|---|---|---|
| `TreeBuilder::new`, `with_options`, `with_path` and the other `with_*` | the same | not `with_file_system` |
| `TreeBuilder::build`, `build_async`, `build_with_options`, `build_with_options_async` | the same, `async` | opening runs the election |
| `Tree::begin`, `begin_with_mode` | the same, `async` | the snapshot is taken at `begin`, as in SurrealKV; a client does not wait for the owner's answer |
| `Tree::begin_with_opts` | none yet | SurrealKV 1.4.0 does not export `TransactionOptions` |
| `Tree::close` | the same | an owner hands the database over, a client disconnects |
| `Transaction::set`, `set_at`, `set_with_options`, `delete`, `delete_with_options`, `soft_delete`, `soft_delete_with_options`, `replace`, `delete_range` | the same | synchronous |
| `Transaction::set_savepoint`, `rollback_to_savepoint`, `rollback`, `set_durability`, `with_durability` | the same | synchronous |
| `Transaction::get`, `get_async`, `get_with`, `get_with_options`, `get_for_update`, `get_many`, `get_many_with`, `get_at`, `get_at_with`, `get_many_at`, `get_many_at_with` | the same, `async` | a client runs the closures on the answer |
| `Transaction::commit` | the same | |
| `Transaction::iter`, `scan`, `range`, `range_with_options`, `history`, `history_with_options` | the same | they return a `Cursor` |
| `LSMIterator`: `seek`, `seek_first`, `seek_last`, `next`, `prev` | `Cursor`: the same, `async` | `seek` takes a user's key, as SurrealKV's transaction iterators do |
| `LSMIterator`: `valid`, `key`, `value`, `value_ref`, `value_into` | `Cursor`: the same | `key` returns kvshare's `InternalKeyRef`, with SurrealKV's methods |
| `LSMIterator`: `value_encoded`, `reanchor_first`, `reanchor_last` | none | a client has the values resolved, and `Cursor::set_keys_only` skips the value log as `value_encoded` lets a reader do; the re-anchors serve SurrealKV's merging iterators |
| `KeyRange`, `ReadOptions::from_range` | the same | kvshare's own: SurrealKV keeps `KeyRange` in a private module |
| `HistoryOptions`, `InternalKeyKind` | the same | SurrealKV's |
| `Tree::view` | the same, `async` | the closure is `async` too, as reads are |
| `Tree::create_checkpoint`, `restore_from_checkpoint` | the same, `async` | a relative path is the caller's, made absolute before the owner sees it; the owner's process writes the files; `CheckpointMetadata` is kvshare's, as SurrealKV keeps its own in a private module |
| `Tree::compact_range`, `flush_wal` | the same, `async` | `CompactRangeOptions` is SurrealKV's |
| `Tree::recovery_report`, `background_error`, `background_error_count`, `is_stopped` | the same, `async`, in a `Result` | the owner's; `BackgroundError` is kvshare's, as SurrealKV's cannot be built by others, and its `timestamp` is on the caller's clock |
| `Tree::prepare_keys`, `encryption_status`, `abandon` | the same, `async` | hidden, as SurrealKV hides them; the owner's key manager answers; `abandon` ends an owner as a crash does |

kvshare adds:

- `TreeBuilder::with_run_dir` (where this process puts its socket if it owns the
  database, see below), `with_election_timeout`, `with_call_timeout`;
- `Tree::transact` and the trait `Operation`: a transaction run until it is known to
  have committed once, see Errors;
- `TreeBuilder::with_max_entry_size`: the most a key and its value hold together,
  64 MiB (`DEFAULT_MAX_ENTRY`) unless set and just under 4 GiB (`LARGEST_MAX_ENTRY`) at
  most; every process of a database sets it alike;
- `TreeBuilder::current_owner`: the owner's pid, asked over its socket, without taking
  part in the election;
- `Tree::is_owner`, `Tree::owner_pid`, `Transaction::is_local`;
- `Cursor::set_page_size`: how many entries a cursor reads at once, 256 unless set;
- `Cursor::set_keys_only`: a cursor that reads keys only, so no value is looked up in
  the value log; its `value` answers `Error::InvalidArgument`.

## Cursors

A cursor is a window of entries read one page at a time. Moving past either end of the
window reads the page beyond it: in the owner from SurrealKV in the same process, in a
client by one request to the owner. A cursor keeps no SurrealKV iterator between moves,
because that iterator cannot cross threads and a cursor whose moves are awaited has to.

Each page is a new SurrealKV iterator on the same transaction, so the pages of a cursor
see one snapshot and the same writes; the cursor keeps its transaction borrowed, so no
write comes between them. A page names its entries by key and by which of the key's
versions each is, and the next page starts next to such an entry. The cursor itself
repeats what SurrealKV's iterators do: a cursor not yet positioned seeks on its first
move, an exhausted one stays so until a seek, and a history's `limit` is spent by each
entry a move lands on, kept across a change of direction and renewed by a seek.

## How the processes of one database meet

Only the database's path is shared. The owner binds a socket in its run directory,
under a name made of a hash of the database's canonical path, and writes the socket's
path into `<db>/SOCKET`. A process that finds the database locked reads the path there.
So the processes need not agree on a run directory, nor share an environment: an
application and a command started from a sandbox or from cron, with another `TMPDIR`,
find each other.

- The run directory is `kvshare` in `$XDG_RUNTIME_DIR` where that is set, or else
  `kvshare-<uid>` in the temporary directory; `with_run_dir` names another. A directory
  that is missing is made for its user only (0700); one that is there is left as it is.
  A socket's path holds 103 bytes at most, which is why the socket does not lie in the
  database's directory itself.
- Each end of the socket asks the system whose process the other end is, and refuses
  another user's: the owner hangs up before its welcome, a client answers `Handshake`.
- The owner's welcome names the versions of the protocol it speaks, from the oldest to
  the newest, and the client's first frame names the newest both speak. This build
  speaks version 8 only; a later one can go on serving the processes of an older
  version. A client that shares no version with its owner is `VersionMismatch`.

kvshare tells what it does through `tracing`, under the target `kvshare`: at `info`, a
process that comes to own the database, one that works through an owner, and an owner
that handed the database over; at `warn`, an owner that is silent or lost, no owner in
time, and a client the owner hung up on.

## Options every process must set alike

Any process may become the owner, so every process of one database must open it with
the options that change what is stored or how it is read. kvshare compares what it can:
the owner's welcome carries a fingerprint of whether versions are kept and for how
long, the comparator's name, whether the value log is on, whether there is a key
manager, the cipher, and how much an entry may hold (`with_max_entry_size`). A client
whose fingerprint differs is refused with `Error::OptionsMismatch` when it opens the
tree.

Some options are objects kvshare cannot compare, and they must be the same in every
process all the same:

- the comparator: only its name is compared, so two comparators of one name must order
  keys alike;
- the filter policy: not compared at all;
- the key manager: only whether there is one is compared; every process needs one that
  holds the database's keys, as it may become the owner.

The other options (sizes, caches, thresholds, compression) tune the process that owns
the database and may differ.

## Errors

`kvshare::Error` names SurrealKV's errors as SurrealKV does
(`TransactionWriteConflict`, `TransactionReadOnly`, `EmptyKey`, `WriteStall`, …), so the
owner and its clients see the same error for the same operation; SurrealKV's other
errors are `Engine` with SurrealKV's text. The errors of reaching the owner are
kvshare's own:

- `OwnerLost`: the owner went away during the operation. The next transaction elects a
  new owner.
- `OwnerSilent`: the owner holds the database but did not answer within the call
  timeout (5 s unless `with_call_timeout` says otherwise). A request that was sent still
  reaches the owner whole and may still be carried out; the connection stays usable. A
  client keeps at most 64 MiB of frames waiting for an owner that reads nothing: a
  request that finds no room among them within the call timeout is `OwnerSilent` too,
  and was not sent.
- `NoOwner`: nobody became the owner, nor answered as one, within the election
  timeout. Opening waits that long for an owner on its way in or out: one that has not
  bound its socket yet, has just died, is stopped and silent at its welcome, or hangs up
  before it while it closes the database.
- `TooManyTransactions`: a client holds 64 open transactions on its connection already;
  it learns this on the first request of the one too many.
- `TooLarge`: a key with its value holds more than the trees of the database allow
  (64 MiB unless `with_max_entry_size` says otherwise), or a request or an answer does
  not fit one frame. Nothing was written or sent, and the transaction works on.
- `Handshake`, `VersionMismatch`, `OptionsMismatch`: the owner and the client disagree.

`Error` is `#[non_exhaustive]`: kvshare may add errors, so a `match` on it needs an arm
for the ones it does not name.

**A commit that ends in `OwnerLost` or `OwnerSilent` may have happened or not.** The
owner may have committed it just before it died or while it did not answer, and a
commit that reached a live owner stands even if its client never hears of it. kvshare
has no way yet to ask what became of a commit, but a new transaction tells:

- after `OwnerSilent`, the owner begins the next transaction of the same tree only once
  the commit is done, so the transaction sees it if it landed, whenever the owner
  answers again;
- after `OwnerLost`, the next transaction runs on the next owner, which opens the
  database only once the old one died or let it go. Only an owner that lost the
  connection and lives on may still be finishing a commit of the old connection.

A caller that must know reads the keys back in a new transaction before it decides
(a read that is `OwnerSilent` itself is asked again), or writes so that the transaction
can be repeated safely (the same keys and values again); `tests/failover.rs` repeats
its writes that way.

`Tree::transact` is that loop. It runs an `Operation` until it is known to have
committed once: `apply` makes the reads and writes, in a new transaction for every
attempt, and `landed` tells from the database whether the commit of an earlier attempt
is there, by something `apply` wrote, the operation's name in a key, say. An attempt
is repeated after a conflict, a write stall, and an owner lost, silent or being
elected; after a commit whose answer never came, `landed` is asked first.

```rust
use std::time::Duration;

use kvshare::{Operation, Result, Transaction, Tree};

/// Add `by` to the stock, as the operation named `name`.
struct Restock {
    name: Vec<u8>,
    by: u64,
}

impl Operation for Restock {
    type Output = u64;

    async fn apply(&mut self, tx: &mut Transaction) -> Result<u64> {
        let stock = match tx.get_for_update(b"stock").await? {
            Some(bytes) => u64::from_le_bytes(bytes.try_into().unwrap()),
            None => 0,
        };
        tx.set(b"stock", &(stock + self.by).to_le_bytes())?;
        // What `landed` finds: the operation's name, with the stock it left.
        tx.set(&self.name, &(stock + self.by).to_le_bytes())?;
        Ok(stock + self.by)
    }

    async fn landed(&mut self, tx: &Transaction) -> Result<Option<u64>> {
        let left = tx.get(&self.name).await?;
        Ok(left.map(|bytes| u64::from_le_bytes(bytes.try_into().unwrap())))
    }
}

async fn restock(tree: &Tree) -> Result<u64> {
    let mut operation = Restock {
        name: b"op/7f3a".to_vec(),
        by: 5,
    };
    tree.transact(Duration::from_secs(10), &mut operation).await
}
```

## Where kvshare differs from SurrealKV

- Reads, `begin`, opening the tree, the moves of a cursor and `commit` are `async`.
- A range or a history whose start lies past its end is empty. SurrealKV 1.4.0 panics
  on one once the transaction holds a write, and a client must not bring down the
  owner's task.
- `recovery_report`, `background_error`, `background_error_count` and `is_stopped`
  answer in a `Result`: the owner may not answer.
- A client waits for a checkpoint, a restore, a compaction or an encryption status as
  long as it takes, without the call timeout: they copy, rewrite or read files.
- A key with its value holds at most what the trees of its database allow, 64 MiB
  unless `with_max_entry_size` sets another size, in an owner and in a client alike:
  what a process may write does not turn on the role it happens to hold. SurrealKV's
  own bound is 4 GiB for the batch of one commit.
- A client's transaction may write more than a frame holds: writes of more than 4 MiB
  travel to the owner ahead of the transaction's next request. The values of many keys
  (`get_many`) come from the owner in parts, as a cursor's pages do.
- A commit that could not be sent, `TooLarge` or no room in time, leaves the
  transaction open with its writes; SurrealKV closes a transaction at every commit.
- A tree whose every handle is dropped lets the database go, as SurrealKV's does, on
  the runtime; `close` waits for it and tells its error.
- A cursor reads each value of its page unless it reads keys only (`set_keys_only`).
- Finding an entry again for the next page steps through the versions of its key, so a
  history of a key with very many versions read in small pages reads them repeatedly.

`tests/differential.rs` runs the same scenarios on SurrealKV itself, on an owner's tree
and through a client, the cursors with pages of 1, 2 and 256 entries, and requires every
answer, a value or an error, to be the same.

## SurrealKV 1.4.0 gaps kvshare works around

- A database locked by another process is an `Error::Other` with a text, for which
  `is_database_locked()` is false; kvshare recognises the text.
- `LOCK` is truncated before the lock is tried, so a process that loses the election
  erases the owner's pid there; kvshare learns the owner's pid from the owner itself.
- Dropping any clone of a native `Tree` closes the database; kvshare shares the owner's
  tree in an `Arc` and never clones it.
- `range` and `history_with_options` panic on a start past the end once the transaction
  holds a write; kvshare answers with nothing.
- `InternalKeyRef::with_timestamp` is crate-private, so no one else can make a key that
  carries its time; kvshare's `InternalKeyRef` reads the key with SurrealKV's and keeps
  the time beside it.
- `CheckpointMetadata` lies in a private module, and `BackgroundError`, `EncryptionStatus`,
  `KeyUsage` and `FileTotals` are `#[non_exhaustive]`: kvshare has its own, with the
  same fields.
- `close` releases the lock while a `compact_range` started before it goes on: the
  compaction looks for the close only between its steps. A process that opens the
  database meanwhile has its files changed under it by the step, and the database may
  not open again (`LoadManifestFail`). kvshare's owner waits for every operation on the
  whole database that runs, its own or a client's, before it closes, so an owner that
  leaves during a compaction leaves only once it is done. `kv-owner` shows what
  SurrealKV alone does.

A restore with a transaction open goes as in SurrealKV, which `tests/differential.rs`
pins: the transaction reads the restored database from then on, with its own writes,
and what it commits lands on it.

## What was measured

On macOS (arm64), in release builds.

`tests/failover.rs` kills the owner with SIGKILL every 300 ms under 4 writing
processes and checks every commit a writer saw return. At S4, run for 120 s
(`KVSHARE_FAILOVER_SECONDS=120`, both tests at once), no returned commit was lost:

| Durability | Commits returned | Owners killed | Missing | First commit after a kill, median (max) |
|---|---|---|---|---|
| `Eventual` | 391 844 | 277 | 0 | 95 ms (403 ms) |
| `Immediate` | 29 961 | 337 | 0 | 46 ms (255 ms) |

`kv-latency 10000` at S2, with a load average of about 5 (medians, p99 in brackets):

| Operation | In the owner | Through the socket |
|---|---|---|
| `get` in an open transaction | 0.1 µs (0.1) | 15.1 µs (28.8) |
| `begin` + `get` + `rollback` | 0.2 µs (0.2) | 17.4 µs (33.3) |
| `begin` + `set` + `commit`, `Eventual` | 11.2 µs (16.2) | 20.0 µs (35.4) |
| `begin` + `set` + `commit`, `Immediate` | 4.0 ms (5.0) | 4.0 ms (4.9) |
| `get` + `set` + `commit`, `Eventual` | 11.1 µs (21.8) | 35.1 µs (54.1) |
| a cursor over 1000 keys, pages of 256 | 0.47 ms (0.55) | 0.83 ms (0.94) |
| a bare socket round trip, 64 B | | 14.2 µs (17.8) |

A read through the owner costs one socket round trip: `begin` and `rollback` do not
wait for the owner.

A cursor over 1000 keys whose values, 16 KiB each, lie in the value log, at S4 with a
load average of about 12, so slower than above throughout (medians, p99 in brackets):

| Cursor | In the owner | Through the socket |
|---|---|---|
| reading the values | 0.90 ms (3.8) | 28.9 ms (41.4) |
| keys only (`set_keys_only`) | 0.09 ms (0.10) | 0.21 ms (0.46) |

### What a client costs its owner, and its owner a client

Through one connection, with `Durability::Immediate` (`tests/handover.rs`): one task
makes about 130 to 210 commits a second, and eight tasks about 830, which is what
eight tasks of the owner make. A transaction begins behind the commits of its
connection only when the client stopped waiting for one of them; before, every begin
waited, and eight tasks made as many commits as one.

A client's requests are answered on the owner's async threads, so each is kept short:
a cursor's page and a part of `get_many` hold 256 entries or 1 MiB, and writes travel
in frames of 4 MiB. While a client read 5000 keys of 4 KiB with one `get_many`, again
and again, a 1 ms timer in the owner's process fired late by 1.5 ms at p99 and 3 ms at
most, against 1.5 ms with no client at all; with the whole answer in one frame it had
been 12 ms and 20 ms.

A client whose owner is stopped keeps at most 64 MiB of frames for it. Of 200 commits
of 1 MiB made meanwhile, about 64 are sent when the owner resumes and the others were
`OwnerSilent` and never sent (`tests/processes.rs`); before, all 200 waited in memory.

### What a change of owner costs

Every change of owner opens SurrealKV in the next owner, and an owner that leaves by
`close` flushes its memtable first; one that crashed leaves the log of its memtable to
replay. Both cost about 2.5 to 3.5 ms for each MiB the memtable holds. `kv-owner` on a
base of 256 MiB written as `kv-sim` writes (no history, no value log), with a load
average of about 5 (the range of three rounds):

| In the memtable | `close` | open after a `close` | open after a crash |
|---|---|---|---|
| 0 MiB | 34–462 ms | 39–50 ms | 27–40 ms |
| 8 MiB | 50–103 ms | 17–18 ms | 26–28 ms |
| 32 MiB | 81–199 ms | 23–40 ms | 55–87 ms |
| 96 MiB | 278–338 ms | 35–38 ms | 246–306 ms |

SurrealKV's memtable is 100 MB unless set, which makes a change of owner cost up to
about 0.35 s. Processes that come and go, each of which may become the owner, are better
off with a memtable of 16 to 32 MiB (`with_max_memtable_size`): the flush and the replay
then stay under about 0.1 s, for more frequent flushes.

A `close` also waits for the compactions the writes before it started in the
background, which move the values with the keys when there is no value log: that is
the `close` of the first row, 462 ms right after the base was written and 34 ms
two rounds later. It waits for a checkpoint that runs and, in kvshare, for a compaction
a client asked for (see the SurrealKV gaps above), so an owner that leaves during either
leaves once it is done. Checkpoints and compactions do not hold up commits meanwhile:
commits of 200 bytes kept a p99 under 1 ms (78 ms at most) during a checkpoint and a
compaction of the whole tree of 11.8 s.

In `kv-sim`, where a keeper checkpoints every 5 s and compacts the whole tree every
10 s, none of the 96 hooks that owned the database and closed it in 10 minutes waited
for one: their close took 52 ms at the median and 75 ms at most. A process owns the
database for a moment and is rarely asked meanwhile; still, a process that runs
maintenance does better to run it rarely and over small ranges, as a close waits for it.

### What history and the value log cost

SurrealKV 1.4.0 gives back the space of the value log a whole file at a time, and only
the files older than the oldest one any table points into; it never moves live values
out of an old file. One value that lives long keeps every file written after it.
`kv-space`: 1000 keys of 4 KiB each written 25 times (3.9 MiB live, 97 MiB written),
files of the value log of 4 MiB, after a compaction of the whole tree:

| Value log | History | A key never replaced | Space | Compaction |
|---|---|---|---|---|
| on | none or 1 s | no | 6 MiB | 126–149 ms |
| on | none or 1 s | yes | 98 MiB | 126–129 ms |
| off | none or 1 s | either | 3 MiB | 130–179 ms |
| on | for ever | either | 98 MiB | 139–143 ms |
| off | for ever | either | 98 MiB | 509–513 ms |

A history kept for ever keeps everything ever written, by design. Without the value
log, a compaction gives the space back but rewrites the large values it moves: it
costs more as more large values live (3.6 times as much with 98 MiB of them). With it,
the space of data that does not die in the order it was written is never given back.

SurrealKV describes a collection of the value log by the share of dead entries in a
file (`docs/ARCHITECTURE.md` of its crate), which 1.4.0 does not have; its README keeps
the value log for large values, so that the memtable does not hold them in memory.
The value log can be collected from outside, by writing the live values of its oldest
files again and compacting the tables: `kv-space` shows it with the key never replaced
(98 MiB down to 6 MiB).

Data that is replaced or removed in no particular order, a code index or threads
removed one by one, is better kept with no history and with the value log off, which
`kv-sim` does. Values of hundreds of KiB and more, files above all, are better kept out
of the database: an entry crosses an owner's socket in one frame and is held whole in
memory on both sides.

How much an entry may hold is the trees' to say (`with_max_entry_size`, 64 MiB unless
set). That bound is kvshare's: SurrealKV took every size tried, up to 1 GiB. What a
large entry costs is SurrealKV's, here with a memtable of 16 MiB and no value log, one
value committed alone:

| Value | Commit | The process grows by |
|---|---|---|
| 64 MiB | 125 ms | 257 MiB |
| 128 MiB | 235 ms | 512 MiB |
| 256 MiB | 0.8 s | 1.0 GiB |
| 512 MiB | 1.6 s | 1.9 GiB |
| 1 GiB | 6.5 s | 3.0 GiB |

Through an owner's socket, with trees that allow 512 MiB, a value of 300 MiB took 1.6 s
to commit and 0.7 s to read.

## Tests

```sh
cargo test
```

- `tests/differential.rs`: the same scenarios on SurrealKV, an owner and a client,
  among them every operation on the whole database, a restore with a transaction open,
  and cursors over keys only.
- `tests/handover.rs`: the election, hand-over, conflicts, a client's own writes, many
  tasks of one client and their commits together, a client that leaves, an owner that
  crashes, an owner that
  leaves while it compacts, a client with other options, the bound on open
  transactions.
- `tests/scan.rs`: cursors over ranges and histories in pages, the same in the owner
  and through the socket; a reversed range; a history of a tree without versions; a
  cursor over keys only; a cursor moved in a spawned task.
- `tests/processes.rs`: an owner stopped with SIGSTOP, whose client gets `OwnerSilent`
  and works on over the same connection once SIGCONT resumes it, a request bigger than
  the socket's buffer arriving whole; opening the database while the owner is stopped,
  which waits for it within the election timeout and is `NoOwner` past it; a commit the
  owner was silent about, which the client's next transaction sees; clients killed with
  SIGKILL while their transactions are open; a client that goes on committing while
  its owner is stopped, whose frames wait only up to their bound.
- `tests/failover.rs`: writers in several processes while the owner is killed again
  and again; `KVSHARE_FAILOVER_SECONDS` makes it run longer than 6 seconds.
- `tests/lifecycle.rs`: a tree dropped without a close, which lets the database go; a
  tree closed while its tasks begin transactions, which stays closed; a client's
  transaction dropped on a thread with no runtime, which is rolled back.
- `tests/limits.rs`: a client's commit of more than a frame holds; values of many keys
  that no frame holds, and more keys than one request asks for; an entry over the
  limit, refused alike in both roles; a request no frame holds, which keeps its
  transaction's writes; trees that allow larger entries, or smaller ones, than the
  default; a tree that allows another size than its owner, which is refused.
- `tests/meeting.rs`: trees with different run directories, which meet through the
  database's directory; a named run directory, which keeps its permissions; the
  default one, which is the user's own.
- `tests/transact.rs`: `Tree::transact` across conflicts, a commit a stopped owner did
  not answer (applied once), the owner's death, and errors it gives up on.
- `tests/events.rs`: the `tracing` events of an owner, its client and a hand-over.
- Unit tests: frames, endpoints and the path of the owner's socket, a write stall
  across the socket, a client that refuses an owner with no protocol version in common,
  another database or other options, the version a client and an owner agree on, a
  client that elects again when the owner hangs up before its welcome, the bound on the
  frames a client queues, a commit in doubt, and writes sent ahead that the owner
  could not apply.
- `tests/sim.rs`: the simulation below; its checks find every fault put into a run made
  by hand, and a run of 30 seconds finds nothing wrong.

`cargo run --release --bin kv-latency` prints the latencies. `cargo run --release --bin
kv-owner` prints what an owner's work costs in SurrealKV itself: a close, an open after
a close and after a crash by how much the memtable holds, the commits during a
checkpoint and a compaction, and a close during each. `cargo run --release --bin
kv-space` prints what history and the value log cost in space and in time.

## The simulation

`kv-sim` puts the heaviest load Milou could put on one database, from many processes at
once, and checks the database against what each process says it did:

- **hooks**, short-lived processes in bursts of 1 to 8 every 20 to 50 ms, any of which may
  become the owner for a moment; each answers an event (an event that comes again gets
  the answer it got) and moves a task along its flow;
- **agents**, which drive threads under leases with epochs, append to their journals in
  small batches and record metrics into a total all threads share;
- **readers**, which follow journals with cursors and check what every snapshot must hold;
- **an indexer** of facts kept by the hash of their content, a fifth of them of 2 to 8
  KiB and one in a hundred of 64 to 192 KiB, with counts of references;
- **a keeper**, which checkpoints, compacts and removes closed threads;
- **a conductor**, which kills a process every two seconds or so, the owner most of all,
  and stops the owner for one to three seconds every eight.

Each process journals what it began and what it saw committed, in lines that survive
`kill -9`; a commit whose answer was lost is read back before it is tried again. At the
end the database is opened with SurrealKV itself and checked: nothing acknowledged is
lost, nothing is applied twice, journals have no gaps and one writer an epoch, tasks
follow their flow, counts of references and the metrics' total add up, removed threads
are gone whole, every event has one answer, and the newest checkpoint holds together.
The report gives the hooks' times by what the owner went through (calm, killed,
stopped), every role's timings, the errors met, the processes' open files and memory,
the space on disk, and the findings. The database is opened as Milou would open it: no
history, no value log, a memtable of 16 MiB. A run
stops, and fails, when its disk has less than 2 GiB free.

```sh
cargo run --release --bin kv-sim -- run --seconds 60 --keep
KVSHARE_SIM_SECONDS=600 cargo test --release --test sim -- --nocapture
```

## License

Apache-2.0.
