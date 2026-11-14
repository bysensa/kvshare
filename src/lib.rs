//! One SurrealKV database shared by several processes on one machine, behind SurrealKV's
//! own API.
//!
//! SurrealKV lets one process open a database: opening takes an exclusive `flock` on
//! `<db>/LOCK`. This crate turns that lock into an election. The process that opens
//! the database is its owner: it serves the database on a Unix socket. A process that
//! finds the database locked connects to that socket and works through the owner.
//! When the owner exits or dies, the kernel releases its lock, its clients lose their
//! connection, and the next process to open the database becomes the owner.
//!
//! [`TreeBuilder`], [`Tree`] and [`Transaction`] carry SurrealKV's names and methods.
//! What may reach the owner is `async`: opening, `begin`, reads and `commit`.
//!
//! - `api`: the builder, the tree and the transaction a process uses.
//! - `endpoint`: where the processes of one database meet.
//! - `election`: who owns the database, and the role a process holds.
//! - `protocol`: what the owner and its clients say to each other, in frames.
//! - `owner`: the owner's side: the socket and a table of transactions per connection.
//! - `client`: a client's side: one connection whose answers find their callers.
//! - `exec`: what runs on a SurrealKV transaction for a request, the same for the
//!   owner's clients and for the owner's own transactions.
//! - `scan`: range and history reads of a transaction, as cursors over pages.

mod api;
mod client;
mod election;
mod endpoint;
mod error;
mod exec;
mod owner;
mod protocol;
mod scan;
mod types;

pub use api::{Transaction, Tree, TreeBuilder};
pub use error::{Error, Result};
pub use protocol::{HistoryFilter, Version};
pub use scan::{History, Scan};
pub use surrealkv::{
    CipherPreference, Comparator, CompressionType, Durability, FilterPolicy, IntoBytes, Key,
    KeyManager, Mode, Options, VLogChecksumLevel, Value, WalRecoveryMode, WriteOptions,
};
pub use types::ReadOptions;
