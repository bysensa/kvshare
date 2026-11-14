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
| S3 | `Tree`'s administrative operations through the owner | done |
| S4 | Robustness, a cursor over keys only, documentation | done |

## Boundaries

- macOS and Linux. Not Windows, not a browser (`with_file_system`), not network file
  systems, not several machines.
- Tested on macOS only: no test has run on Linux yet.
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

- `TreeBuilder::with_run_dir` (where the owners' sockets live, by default `kvshare` in
  the temporary directory), `with_election_timeout`, `with_call_timeout`;
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

## Options every process must set alike

Any process may become the owner, so every process of one database must open it with
the options that change what is stored or how it is read. kvshare compares what it can:
the owner's welcome carries a fingerprint of whether versions are kept and for how
long, the comparator's name, whether the value log is on, whether there is a key
manager, and the cipher. A client whose fingerprint differs is refused with
`Error::OptionsMismatch` when it opens the tree.

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
  timeout (5 s unless `with_call_timeout` says otherwise). The request still reaches the
  owner whole and may still be carried out; the connection stays usable.
- `NoOwner`: nobody became the owner, nor answered as one, in time.
- `TooManyTransactions`: a client holds 64 open transactions on its connection already;
  it learns this on the first request of the one too many.
- `Handshake`, `VersionMismatch`, `OptionsMismatch`: the owner and the client disagree.

**A commit that ends in `OwnerLost` or `OwnerSilent` may have happened or not.** The
owner may have committed it just before it died or while it did not answer, and a
commit that reached a live owner stands even if its client never hears of it. kvshare
has no way yet to ask what became of a commit. A caller that must know writes so that
the transaction can be repeated safely (the same keys and values again), or reads the
keys back in a new transaction before it decides; `tests/failover.rs` repeats its
writes that way.

## Where kvshare differs from SurrealKV

- Reads, `begin`, opening the tree, the moves of a cursor and `commit` are `async`.
- A range or a history whose start lies past its end is empty. SurrealKV 1.4.0 panics
  on one once the transaction holds a write, and a client must not bring down the
  owner's task.
- `recovery_report`, `background_error`, `background_error_count` and `is_stopped`
  answer in a `Result`: the owner may not answer.
- A client waits for a checkpoint, a restore, a compaction or an encryption status as
  long as it takes, without the call timeout: they copy, rewrite or read files.
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

## Tests

```sh
cargo test
```

- `tests/differential.rs`: the same scenarios on SurrealKV, an owner and a client,
  among them every operation on the whole database, a restore with a transaction open,
  and cursors over keys only.
- `tests/handover.rs`: the election, hand-over, conflicts, a client's own writes, many
  tasks of one client, a client that leaves, an owner that crashes, a client with other
  options, the bound on open transactions.
- `tests/scan.rs`: cursors over ranges and histories in pages, the same in the owner
  and through the socket; a reversed range; a history of a tree without versions; a
  cursor over keys only; a cursor moved in a spawned task.
- `tests/processes.rs`: an owner stopped with SIGSTOP, whose client gets `OwnerSilent`
  and works on over the same connection once SIGCONT resumes it, a request bigger than
  the socket's buffer arriving whole; clients killed with SIGKILL while their
  transactions are open.
- `tests/failover.rs`: writers in several processes while the owner is killed again
  and again; `KVSHARE_FAILOVER_SECONDS` makes it run longer than 6 seconds.
- Unit tests: frames, endpoints, a write stall across the socket, and a client that
  refuses an owner of another protocol version, another database or other options.
- `tests/sim.rs`: the simulation below; its checks find every fault put into a run made
  by hand, and a run of 30 seconds finds nothing wrong.

`cargo run --release --bin kv-latency` prints the latencies.

## The simulation

`kv-sim` puts the heaviest load Milou could put on one database, from many processes at
once, and checks the database against what each process says it did:

- **hooks**, short-lived processes in bursts of 1 to 8 every 20 to 50 ms, any of which may
  become the owner for a moment; each answers an event (an event that comes again gets
  the answer it got) and moves a task along its flow;
- **agents**, which drive threads under leases with epochs, append to their journals in
  small batches and record metrics into a total all threads share;
- **readers**, which follow journals with cursors and check what every snapshot must hold;
- **an indexer** of facts kept by the hash of their content, a fifth of them large enough
  for the value log, with counts of references;
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
the space on disk, and the findings. The database keeps every version for ever, so it
only grows; a run stops, and fails, when its disk has less than 2 GiB free.

```sh
cargo run --release --bin kv-sim -- run --seconds 60 --keep
KVSHARE_SIM_SECONDS=600 cargo test --release --test sim -- --nocapture
```

## License

Apache-2.0.
