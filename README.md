# kvshare

One [SurrealKV](https://github.com/surrealdb/surrealkv) database shared by several
processes on one machine.

SurrealKV lets one process open a database: opening takes an exclusive `flock` on
`<db>/LOCK`. kvshare turns that lock into an election. The process that opens the
database owns it and serves it on a Unix socket. A process that finds the database
locked connects to that socket and works through the owner. When the owner exits or
dies, the kernel releases its lock, its clients lose their connection, and the next
process to open the database becomes the owner.

## Status

The code comes from a spike in the Milou repository and keeps the spike's names
(`Shared`, `Tx`) for now. The plan is to repeat SurrealKV's own API, stage by stage:

| Stage | What |
|---|---|
| S0 | This repository, with the spike's code split into modules |
| S1 | `TreeBuilder`, `Tree`, `Transaction` with SurrealKV's names; point reads and writes, savepoints, commit and rollback |
| S2 | Cursors with `seek`, `next`, `prev`; history |
| S3 | `Tree`'s administrative operations through the owner |
| S4 | Robustness, Linux, documentation |

## Boundaries

- macOS and Linux. Not Windows, not a browser (`with_file_system`), not network file
  systems, not several machines.
- SurrealKV `=1.4.0`.

## Mapping to SurrealKV's API

The rule: kvshare keeps SurrealKV's type and method names, and a method is `async`
when it may reach the owner. Writes stay synchronous: a client keeps them until a
commit or a read needs them, as SurrealKV keeps a transaction's writes until the commit.

| SurrealKV | kvshare | Notes |
|---|---|---|
| `TreeBuilder::build()` | `TreeBuilder::build().await` | the election (S1) |
| `Tree::begin()` | `Tree::begin().await` | S1 |
| `Transaction::get(k)` | `Transaction::get(k).await` | S1 |
| `Transaction::set(k, v)` | `Transaction::set(k, v)` | unchanged (S1) |
| `Transaction::commit().await` | `Transaction::commit().await` | unchanged (S1) |
| `Transaction::range(a, b)` and `LSMIterator` | the same, with `seek`, `next`, `prev` awaited | S2 |
| `Tree::create_checkpoint(dir)` and the other administrative operations | the same, awaited | S3 |

Stages S1 to S3 complete this table.

## What the spike measured

On macOS (arm64), in release builds:

- Killing the owner with SIGKILL about 15 times in 6 s under 4 writing processes lost no
  commit a writer had seen return, with `Durability::Eventual` and `Immediate`. The first
  commit after a kill came 40 to 70 ms later (median).
- A `get` through the owner took 12 to 25 µs, against 0.1 µs in the owner itself and 10
  to 15 µs for a bare socket round trip. A scan of 1000 keys took 1.2 ms through the
  owner, against 0.8 ms in the owner.

## SurrealKV 1.4.0 gaps kvshare works around

- A database locked by another process is an `Error::Other` with a text, for which
  `is_database_locked()` is false; kvshare recognises the text.
- `LOCK` is truncated before the lock is tried, so a process that loses the election
  erases the owner's pid there; kvshare learns the owner's pid from the owner itself.

## Tests

```sh
cargo test
```

- `tests/handover.rs`: the election, hand-over, conflicts, a client's own writes, many
  tasks of one client.
- `tests/scan.rs`: range and history reads in pages, the same in the owner and through
  the socket.
- `tests/failover.rs`: writers in several processes while the owner is killed again
  and again.

`cargo run --release --bin kv-latency` prints the latencies.

## License

Apache-2.0.
