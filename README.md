# kvshare

One [SurrealKV](https://github.com/surrealdb/surrealkv) database shared by several
processes on one machine, behind SurrealKV's own API.

SurrealKV lets one process open a database: opening takes an exclusive `flock` on
`<db>/LOCK`. kvshare turns that lock into an election. The process that opens the
database owns it and serves it on a Unix socket. A process that finds the database
locked connects to that socket and works through the owner. When the owner exits or
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
| S3 | `Tree`'s administrative operations through the owner | next |
| S4 | Robustness, Linux, documentation | |

## Boundaries

- macOS and Linux. Not Windows, not a browser (`with_file_system`), not network file
  systems, not several machines.
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
| `LSMIterator`: `value_encoded`, `reanchor_first`, `reanchor_last` | none | a client has the values resolved; the re-anchors serve SurrealKV's merging iterators |
| `KeyRange`, `ReadOptions::from_range` | the same | kvshare's own: SurrealKV keeps `KeyRange` in a private module |
| `HistoryOptions`, `InternalKeyKind` | the same | SurrealKV's |
| `Tree::create_checkpoint` and the other administrative operations | none yet | S3 |

kvshare adds:

- `TreeBuilder::with_run_dir` (where the owners' sockets live, by default `kvshare` in
  the temporary directory), `with_election_timeout`, `with_call_timeout`;
- `TreeBuilder::current_owner`: the owner's pid, asked over its socket, without taking
  part in the election;
- `Tree::is_owner`, `Tree::owner_pid`, `Transaction::is_local`;
- `Cursor::set_page_size`: how many entries a cursor reads at once, 256 unless set.

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

The processes of one database must agree on the options that change what is stored: the
versions kept and their retention, the comparator, the value log and encryption. A
client whose options differ from the owner's is refused with `Error::OptionsMismatch`.
The other options tune the owner and may differ.

## Errors

`kvshare::Error` names SurrealKV's errors as SurrealKV does
(`TransactionWriteConflict`, `TransactionReadOnly`, `EmptyKey`, …), so the owner and its
clients see the same error for the same operation; SurrealKV's other errors are
`Engine` with SurrealKV's text. The errors of reaching the owner are kvshare's own:

- `OwnerLost`: the owner went away during the operation. For a commit, whether it
  happened is unknown. The next transaction elects a new owner.
- `OwnerSilent`: the owner holds the database but did not answer in time.
- `NoOwner`: nobody became the owner, nor answered as one, in time.
- `TooManyTransactions`: a client holds 64 open transactions on its connection already;
  it learns this on the first request of the one too many.
- `Handshake`, `VersionMismatch`, `OptionsMismatch`: the owner and the client disagree.

## Where kvshare differs from SurrealKV

- Reads, `begin`, opening the tree, the moves of a cursor and `commit` are `async`.
- A range or a history whose start lies past its end is empty. SurrealKV 1.4.0 panics
  on one once the transaction holds a write, and a client must not bring down the
  owner's task.
- A cursor reads each value of its page, even if only keys are looked at.
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

## What was measured

On macOS (arm64), in release builds.

Killing the owner with SIGKILL about 15 times in 6 s under 4 writing processes lost no
commit a writer had seen return, with `Durability::Eventual` and `Immediate`
(`tests/failover.rs`; in the spike, the first commit after a kill came 40 to 70 ms
later, median).

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

## Tests

```sh
cargo test
```

- `tests/differential.rs`: the same scenarios on SurrealKV, an owner and a client.
- `tests/handover.rs`: the election, hand-over, conflicts, a client's own writes, many
  tasks of one client, a client that leaves, the bound on open transactions.
- `tests/scan.rs`: cursors over ranges and histories in pages, the same in the owner
  and through the socket; a reversed range; a history of a tree without versions; a
  cursor moved in a spawned task.
- `tests/failover.rs`: writers in several processes while the owner is killed again
  and again.

`cargo run --release --bin kv-latency` prints the latencies.

## License

Apache-2.0.
