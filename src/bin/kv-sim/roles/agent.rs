//! An agent, as `milou acp` or the desktop client runs it: a long-lived process that
//! drives up to eight threads at once. A thread is driven under a lease: the agent takes
//! it when it is free or its holder's lease ran out, with the next epoch, and every write
//! to the thread checks the lease in the same transaction. A turn appends entries to the
//! thread's journal in small batches and records a metric for each request to the model,
//! adding it to the total all threads share. After a few turns the agent closes the
//! thread. Now and then it continues a task.

use std::path::PathBuf;
use std::time::Duration;

use kvshare::{Mode, Transaction};

use super::hook::{By, TASKS, TransitionOp};
use super::{Ctx, Op, Outcome, builder, stopping};
use crate::journal::Journal;
use crate::keys::{self, Entry, Lease, Meta, Metric, OpId, Total};
use crate::rng::Rng;

/// How many threads an agent drives at once.
const THREADS: usize = 8;

/// How long a lease lasts unless its holder writes to the thread again.
pub const LEASE_MS: u64 = 5_000;

pub async fn run(run: PathBuf, seed: u64) -> Result<(), String> {
    let journal = Journal::open(&run, "agent", 'a').map_err(|e| e.to_string())?;
    let tree = open(&run).await?;
    let mut ctx = Ctx {
        run,
        tree,
        journal,
        rng: Rng::new(seed),
    };
    // The process's number in the run stands for its pid, which a later process may get.
    let pid = ctx.journal.number();
    let mut held: Vec<Held> = Vec::new();
    while !stopping(&ctx.run) {
        if held.len() < THREADS
            && let Some(taken) = take_thread(&mut ctx, pid).await
        {
            held.push(taken);
        }
        if held.is_empty() {
            tokio::time::sleep(Duration::from_millis(10)).await;
            continue;
        }
        let at = ctx.rng.below(held.len() as u64) as usize;
        let lost = !turn(&mut ctx, pid, &held[at]).await;
        held[at].turns -= 1;
        if lost {
            held.swap_remove(at);
        } else if held[at].turns == 0 {
            let thread = held.swap_remove(at);
            let mut close = CloseOp {
                thread: thread.thread,
                pid,
                epoch: thread.epoch,
            };
            ctx.perform("close", &mut close).await;
        }
        if ctx.rng.chance(10) {
            let task = ctx.rng.below(TASKS);
            let mut transition = TransitionOp::new(task, By::Agent, ctx.rng.next_u64());
            ctx.perform("transition", &mut transition).await;
        }
    }
    let _ = ctx.tree.close().await;
    Ok(())
}

/// Open the database, waiting through elections until the conductor says stop.
pub async fn open(run: &std::path::Path) -> Result<kvshare::Tree, String> {
    loop {
        match builder(run).build().await {
            Ok(tree) => return Ok(tree),
            Err(error) if stopping(run) => return Err(error.to_string()),
            Err(_) => tokio::time::sleep(Duration::from_millis(50)).await,
        }
    }
}

/// A thread this agent drives.
struct Held {
    thread: String,
    epoch: u64,
    turns: u32,
}

/// Take a thread: a new one, or one whose holder's lease ran out.
async fn take_thread(ctx: &mut Ctx, pid: u32) -> Option<Held> {
    let thread = if ctx.rng.chance(50) {
        format!("t{:08x}", ctx.rng.next_u64() as u32)
    } else {
        orphan(ctx).await?
    };
    let mut lease = LeaseOp {
        thread: thread.clone(),
        pid,
        epoch: 0,
    };
    match ctx.perform("lease", &mut lease).await {
        Outcome::Acked => Some(Held {
            thread,
            epoch: lease.epoch,
            turns: ctx.rng.between(3, 8) as u32,
        }),
        _ => None,
    }
}

/// An open thread whose lease ran out, if one is found.
async fn orphan(ctx: &mut Ctx) -> Option<String> {
    let tx = ctx.tree.begin_with_mode(Mode::ReadOnly).await.ok()?;
    let mut cursor = tx.range(b"tm/".as_slice(), b"tm0".as_slice()).ok()?;
    let mut valid = cursor.seek_first().await.ok()?;
    let now = keys::now_ms();
    while valid {
        let thread = keys::thread_of(cursor.key().user_key(), "tm/")?;
        let meta: Meta = keys::decode(&cursor.value().ok()?).ok()?;
        if !meta.closed {
            let free = match tx.get(keys::lease(&thread)).await.ok()? {
                Some(bytes) => keys::decode::<Lease>(&bytes).is_ok_and(|l| l.until_ms < now),
                None => true,
            };
            if free {
                return Some(thread);
            }
        }
        valid = cursor.next().await.ok()?;
    }
    None
}

/// One turn on a thread: entries in batches, a metric every few. `false` if the lease
/// was lost on the way.
async fn turn(ctx: &mut Ctx, pid: u32, held: &Held) -> bool {
    let mut left = ctx.rng.between(20, 200);
    while left > 0 && !stopping(&ctx.run) {
        let batch = ctx.rng.between(1, 10).min(left);
        let bodies = (0..batch)
            .map(|_| {
                // Chunks of text, and now and then a tool call large enough for the
                // value log.
                let size = if ctx.rng.chance(5) {
                    ctx.rng.between(1_024, 4_096)
                } else {
                    ctx.rng.between(20, 200)
                };
                ctx.rng.bytes(size as usize)
            })
            .collect();
        let mut append = AppendOp {
            thread: held.thread.clone(),
            pid,
            epoch: held.epoch,
            bodies,
            first: 0,
        };
        if ctx.perform("append", &mut append).await != Outcome::Acked {
            return false;
        }
        left -= batch;
        if ctx.rng.chance(30) {
            let mut metric = MetricOp {
                thread: held.thread.clone(),
                pid,
                epoch: held.epoch,
                request: ctx.rng.next_u64() >> 1,
                tokens: ctx.rng.between(100, 20_000),
            };
            if ctx.perform("metric", &mut metric).await != Outcome::Acked {
                return false;
            }
        }
        tokio::time::sleep(Duration::from_millis(ctx.rng.between(1, 3))).await;
    }
    true
}

/// Whether `pid` holds `thread`'s lease under `epoch`, read for update so that a commit
/// conflicts with a lease taken meanwhile.
async fn holds(tx: &mut Transaction, thread: &str, pid: u32, epoch: u64) -> kvshare::Result<bool> {
    Ok(match tx.get_for_update(keys::lease(thread)).await? {
        Some(bytes) => {
            keys::decode::<Lease>(&bytes).is_ok_and(|l| l.pid == pid && l.epoch == epoch)
        }
        None => false,
    })
}

fn renewed(tx: &mut Transaction, thread: &str, pid: u32, epoch: u64) -> kvshare::Result<()> {
    let lease = Lease {
        pid,
        epoch,
        until_ms: keys::now_ms() + LEASE_MS,
    };
    tx.set(keys::lease(thread), keys::encode(&lease))
}

/// Take a thread's lease with the next epoch, if it is free and open.
struct LeaseOp {
    thread: String,
    pid: u32,
    epoch: u64,
}

impl Op for LeaseOp {
    fn describe(&self) -> String {
        format!("lease {} {}", self.thread, self.epoch)
    }

    async fn apply(&mut self, _id: OpId, tx: &mut Transaction) -> kvshare::Result<bool> {
        let current = match tx.get_for_update(keys::lease(&self.thread)).await? {
            Some(bytes) => Some(keys::decode::<Lease>(&bytes).map_err(kvshare::Error::Engine)?),
            None => None,
        };
        if current.is_some_and(|l| l.until_ms >= keys::now_ms()) {
            return Ok(false);
        }
        let meta = match tx.get_for_update(keys::meta(&self.thread)).await? {
            Some(bytes) => keys::decode::<Meta>(&bytes).map_err(kvshare::Error::Engine)?,
            None => Meta::default(),
        };
        if meta.closed {
            return Ok(false);
        }
        self.epoch = current.map_or(0, |l| l.epoch) + 1;
        renewed(tx, &self.thread, self.pid, self.epoch)?;
        tx.set(keys::meta(&self.thread), keys::encode(&meta))?;
        Ok(true)
    }

    async fn landed(&self, _id: OpId, tx: &Transaction) -> kvshare::Result<bool> {
        Ok(match tx.get(keys::lease(&self.thread)).await? {
            Some(bytes) => keys::decode::<Lease>(&bytes)
                .is_ok_and(|l| l.pid == self.pid && l.epoch == self.epoch),
            None => false,
        })
    }
}

/// Append a batch of entries to a thread's journal under the lease.
struct AppendOp {
    thread: String,
    pid: u32,
    epoch: u64,
    bodies: Vec<Vec<u8>>,
    first: u64,
}

impl Op for AppendOp {
    fn describe(&self) -> String {
        format!(
            "append {} {} {} {}",
            self.thread,
            self.bodies.len(),
            self.first,
            self.epoch
        )
    }

    async fn apply(&mut self, id: OpId, tx: &mut Transaction) -> kvshare::Result<bool> {
        if !holds(tx, &self.thread, self.pid, self.epoch).await? {
            return Ok(false);
        }
        let mut meta: Meta = match tx.get(keys::meta(&self.thread)).await? {
            Some(bytes) => keys::decode(&bytes).map_err(kvshare::Error::Engine)?,
            None => return Ok(false),
        };
        self.first = meta.head + 1;
        let batch = self.bodies.len() as u32;
        for (at, body) in self.bodies.iter().enumerate() {
            let entry = Entry {
                op: id,
                batch,
                pid: self.pid,
                epoch: self.epoch,
                body: body.clone(),
            };
            tx.set(
                keys::entry(&self.thread, self.first + at as u64),
                keys::encode(&entry),
            )?;
        }
        meta.head += u64::from(batch);
        tx.set(keys::meta(&self.thread), keys::encode(&meta))?;
        renewed(tx, &self.thread, self.pid, self.epoch)?;
        Ok(true)
    }

    async fn landed(&self, id: OpId, tx: &Transaction) -> kvshare::Result<bool> {
        Ok(match tx.get(keys::entry(&self.thread, self.first)).await? {
            Some(bytes) => keys::decode::<Entry>(&bytes).is_ok_and(|e| e.op == id),
            None => false,
        })
    }
}

/// Record a request's metric and add it to the total, under the lease.
struct MetricOp {
    thread: String,
    pid: u32,
    epoch: u64,
    request: u64,
    tokens: u64,
}

impl Op for MetricOp {
    fn describe(&self) -> String {
        format!("metric {} {} {}", self.thread, self.request, self.tokens)
    }

    async fn apply(&mut self, id: OpId, tx: &mut Transaction) -> kvshare::Result<bool> {
        if !holds(tx, &self.thread, self.pid, self.epoch).await? {
            return Ok(false);
        }
        let mut total: Total = match tx.get_for_update(keys::TOTAL).await? {
            Some(bytes) => keys::decode(&bytes).map_err(kvshare::Error::Engine)?,
            None => Total::default(),
        };
        total.requests += 1;
        total.tokens += self.tokens;
        let metric = Metric {
            op: id,
            tokens: self.tokens,
        };
        tx.set(
            keys::metric(&self.thread, self.request),
            keys::encode(&metric),
        )?;
        tx.set(keys::TOTAL, keys::encode(&total))?;
        Ok(true)
    }

    async fn landed(&self, id: OpId, tx: &Transaction) -> kvshare::Result<bool> {
        Ok(
            match tx.get(keys::metric(&self.thread, self.request)).await? {
                Some(bytes) => keys::decode::<Metric>(&bytes).is_ok_and(|m| m.op == id),
                None => false,
            },
        )
    }
}

/// Close a thread and let its lease go.
struct CloseOp {
    thread: String,
    pid: u32,
    epoch: u64,
}

impl Op for CloseOp {
    fn describe(&self) -> String {
        format!("close {}", self.thread)
    }

    async fn apply(&mut self, _id: OpId, tx: &mut Transaction) -> kvshare::Result<bool> {
        if !holds(tx, &self.thread, self.pid, self.epoch).await? {
            return Ok(false);
        }
        let mut meta: Meta = match tx.get(keys::meta(&self.thread)).await? {
            Some(bytes) => keys::decode(&bytes).map_err(kvshare::Error::Engine)?,
            None => return Ok(false),
        };
        meta.closed = true;
        tx.set(keys::meta(&self.thread), keys::encode(&meta))?;
        tx.delete(keys::lease(&self.thread))?;
        Ok(true)
    }

    async fn landed(&self, _id: OpId, tx: &Transaction) -> kvshare::Result<bool> {
        Ok(match tx.get(keys::meta(&self.thread)).await? {
            Some(bytes) => keys::decode::<Meta>(&bytes).is_ok_and(|m| m.closed),
            // A thread closed and already dropped by the keeper.
            None => true,
        })
    }
}
