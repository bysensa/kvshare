//! One SurrealKV database shared by several processes on one machine.
//!
//! SurrealKV lets one process open a database: opening takes an exclusive `flock` on
//! `<db>/LOCK`. This crate turns that lock into an election. The process that opens
//! the database is its owner: it serves the database on a Unix socket. A process that
//! finds the database locked connects to that socket and works through the owner.
//! When the owner exits or dies, the kernel releases its lock, its clients lose their
//! connection, and the next process to open the database becomes the owner.
//!
//! - [`endpoint`]: where the processes of one database meet.
//! - `election`: who owns the database, and the role a process holds.
//! - [`protocol`]: what the owner and its clients say to each other, in frames.
//! - [`owner`]: the owner's side: the socket and a table of transactions per connection.
//! - [`client`]: a client's side: one connection whose answers find their callers.
//! - `exec`: what runs on a SurrealKV transaction for a request, the same for the
//!   owner's clients and for the owner's own transactions.
//! - [`shared`]: what a process uses: a shared database and its transactions, local in
//!   the owner and remote in a client.
//! - [`scan`]: range and history reads of a transaction, as cursors over pages.

pub mod client;
mod election;
pub mod endpoint;
mod exec;
pub mod owner;
pub mod protocol;
pub mod scan;
pub mod shared;

pub use protocol::Version;
pub use scan::{History, Scan};
pub use shared::{Error, Options, Shared, Tx};
