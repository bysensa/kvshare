//! What the roles share: how they open the database, how they tell errors apart, and
//! the loop every operation runs, which retries a conflict and, after a commit whose
//! answer was lost, reads before it writes again.

pub mod agent;
pub mod hook;
pub mod indexer;
pub mod keeper;
pub mod reader;

use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use kvshare::{Error, Transaction, Tree, TreeBuilder};

use crate::journal::Journal;
use crate::keys::OpId;
use crate::rng::Rng;

/// The options every process of a run opens the database with, Milou's: no history of
/// versions and no value log, so that a compaction gives back the space of whatever was
/// replaced or removed (the value log gives back the space of a value only once every
/// value written before it is gone); and a memtable of 16 MiB, not SurrealKV's 100 MB,
/// as for processes that come and go: each change of owner flushes it or replays its
/// log.
pub fn builder(run: &Path) -> TreeBuilder {
    TreeBuilder::new()
        .with_path(run.join("db"))
        .with_max_memtable_size(16 << 20)
        .with_run_dir(run.join("sock"))
        .with_call_timeout(Duration::from_secs(1))
        .with_election_timeout(Duration::from_secs(5))
}

/// Whether the conductor asked the long-lived roles to stop.
pub fn stopping(run: &Path) -> bool {
    run.join("stop").exists()
}

/// What a role has to work with.
pub struct Ctx {
    pub run: PathBuf,
    pub tree: Tree,
    pub journal: Journal,
    pub rng: Rng,
}

/// How a role treats an error.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Class {
    /// Another transaction won: run the operation again.
    Conflict,
    /// The owner was lost, silent, or being elected: run it again after a pause.
    Transport,
    /// Anything else: give the operation up.
    Other,
}

pub fn class(error: &Error) -> Class {
    match error {
        Error::TransactionWriteConflict | Error::TransactionRetry => Class::Conflict,
        Error::OwnerLost(_)
        | Error::OwnerSilent { .. }
        | Error::NoOwner { .. }
        | Error::Handshake(_)
        | Error::TooManyTransactions
        | Error::WriteStall { .. }
        | Error::Io(_) => Class::Transport,
        _ => Class::Other,
    }
}

/// The name of an error for the report's counts.
pub fn counted(error: &Error) -> &'static str {
    match error {
        Error::TransactionWriteConflict | Error::TransactionRetry => "conflict",
        Error::OwnerLost(_) => "owner_lost",
        Error::OwnerSilent { .. } => "owner_silent",
        Error::NoOwner { .. } => "no_owner",
        Error::Handshake(_) => "handshake",
        Error::TooManyTransactions => "too_many_transactions",
        Error::WriteStall { .. } => "write_stall",
        Error::Io(_) => "io",
        _ => "other",
    }
}

/// An operation a role commits.
pub trait Op {
    /// The journal's words for the operation as `apply` last prepared it: its kind and
    /// its arguments.
    fn describe(&self) -> String;

    /// Read and write in `tx` as operation `id`; `Ok(false)` to commit nothing.
    async fn apply(&mut self, id: OpId, tx: &mut Transaction) -> kvshare::Result<bool>;

    /// Whether operation `id`'s writes are in the database, read in `tx`.
    async fn landed(&self, id: OpId, tx: &Transaction) -> kvshare::Result<bool>;
}

/// How an operation ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Outcome {
    Acked,
    Skipped,
    Failed,
}

/// How long an operation may keep trying before it gives up.
const GIVE_UP: Duration = Duration::from_secs(30);

impl Ctx {
    /// Run `op` to its end: commit it, retry it after a conflict or a lost owner, and
    /// after a commit whose answer was lost, read whether it landed before trying again.
    pub async fn perform(&mut self, kind: &str, op: &mut impl Op) -> Outcome {
        let id = self.journal.next_op();
        let started = Instant::now();
        let outcome = self.attempt(id, op, started).await;
        self.journal
            .time(&format!("op.{kind}"), started.elapsed().as_micros(), "");
        outcome
    }

    async fn attempt(&mut self, id: OpId, op: &mut impl Op, started: Instant) -> Outcome {
        loop {
            if started.elapsed() > GIVE_UP {
                self.journal.fail(id, "gave up");
                return Outcome::Failed;
            }
            let mut tx = match self.tree.begin().await {
                Ok(tx) => tx,
                Err(error) => match self.pause_after(&error).await {
                    true => continue,
                    false => return self.failed(id, &error),
                },
            };
            match op.apply(id, &mut tx).await {
                Ok(true) => {}
                Ok(false) => {
                    tx.rollback();
                    self.journal.skip(id);
                    return Outcome::Skipped;
                }
                Err(error) => match self.pause_after(&error).await {
                    true => continue,
                    false => return self.failed(id, &error),
                },
            }
            self.journal.begin(id, &op.describe());
            match tx.commit().await {
                Ok(()) => {
                    self.journal.ack(id);
                    return Outcome::Acked;
                }
                Err(error) if class(&error) == Class::Transport => {
                    // The commit may have happened: find out before writing again.
                    self.journal.count("unknown_commit");
                    self.journal.count(counted(&error));
                    match self.landed(id, op, started).await {
                        Some(true) => {
                            self.journal.count("unknown_commit_landed");
                            self.journal.ack(id);
                            return Outcome::Acked;
                        }
                        Some(false) => self.journal.count("unknown_commit_absent"),
                        None => {
                            self.journal.fail(id, "its commit's outcome stays unknown");
                            return Outcome::Failed;
                        }
                    }
                }
                Err(error) => match self.pause_after(&error).await {
                    true => continue,
                    false => return self.failed(id, &error),
                },
            }
        }
    }

    /// Whether `op` landed, read in new transactions until one answers; `None` if none
    /// did before the operation's time ran out.
    async fn landed(&mut self, id: OpId, op: &impl Op, started: Instant) -> Option<bool> {
        while started.elapsed() <= GIVE_UP {
            let tx = match self.tree.begin().await {
                Ok(tx) => tx,
                Err(error) => {
                    if !self.pause_after(&error).await {
                        return None;
                    }
                    continue;
                }
            };
            match op.landed(id, &tx).await {
                Ok(landed) => return Some(landed),
                Err(error) => {
                    if !self.pause_after(&error).await {
                        return None;
                    }
                }
            }
        }
        None
    }

    /// Count `error`, and pause before the next try if it is worth one.
    async fn pause_after(&mut self, error: &Error) -> bool {
        self.journal.count(counted(error));
        let pause = match class(error) {
            Class::Conflict => self.rng.between(1, 10),
            Class::Transport => self.rng.between(20, 100),
            Class::Other => return false,
        };
        tokio::time::sleep(Duration::from_millis(pause)).await;
        true
    }

    fn failed(&mut self, id: OpId, error: &Error) -> Outcome {
        self.journal.fail(id, &error.to_string());
        Outcome::Failed
    }
}
