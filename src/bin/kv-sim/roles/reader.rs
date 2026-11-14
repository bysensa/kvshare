//! A reader, as the desktop client and the web UI read: it follows the tails of thread
//! journals, lists threads, sums the metrics and reads tasks' histories. Each read is one
//! read-only transaction, so it sees one snapshot, and in it the reader checks what must
//! hold in any snapshot:
//! - the metrics' records sum to the total;
//! - a thread's journal runs from 1 to its head without a gap, under epochs that never
//!   go back, each epoch written by one process;
//! - a task's versions are the steps its transitions took.

use std::collections::HashMap;
use std::path::PathBuf;
use std::time::{Duration, Instant};

use kvshare::{HistoryOptions, Mode, Transaction};

use super::hook::TASKS;
use super::{Class, Ctx, class, counted, stopping};
use crate::journal::Journal;
use crate::keys::{self, Entry, Meta, Metric, Task, Total, Transition};
use crate::rng::Rng;

pub async fn run(run: PathBuf, seed: u64) -> Result<(), String> {
    let journal = Journal::open(&run, "reader", 'r').map_err(|e| e.to_string())?;
    let tree = super::agent::open(&run).await?;
    let mut ctx = Ctx {
        run,
        tree,
        journal,
        rng: Rng::new(seed),
    };
    // How far the reader has followed each thread: the last number, its epoch and pid.
    let mut followed: HashMap<String, (u64, u64, u32)> = HashMap::new();
    while !stopping(&ctx.run) {
        let started = Instant::now();
        let what = ctx.rng.below(3);
        let result = match ctx.tree.begin_with_mode(Mode::ReadOnly).await {
            Ok(tx) => match what {
                0 => metrics(&tx, &mut ctx).await,
                1 => thread(&tx, &mut ctx, &mut followed).await,
                _ => task(&tx, &mut ctx).await,
            },
            Err(error) => Err(error),
        };
        if let Err(error) = result {
            ctx.journal.count(&format!("read_{}", counted(&error)));
            if class(&error) == Class::Transport {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            continue;
        }
        let name = ["read.metrics", "read.thread", "read.task"][what as usize];
        ctx.journal.time(name, started.elapsed().as_micros(), "");
        tokio::time::sleep(Duration::from_millis(ctx.rng.between(5, 20))).await;
    }
    let _ = ctx.tree.close().await;
    Ok(())
}

/// The records of every request sum to the total.
async fn metrics(tx: &Transaction, ctx: &mut Ctx) -> kvshare::Result<()> {
    let total: Total = match tx.get(keys::TOTAL).await? {
        Some(bytes) => keys::decode(&bytes).map_err(kvshare::Error::Engine)?,
        None => Total::default(),
    };
    let mut summed = Total::default();
    let mut cursor = tx.range(b"m/r/".as_slice(), b"m/r0".as_slice())?;
    let mut valid = cursor.seek_first().await?;
    while valid {
        let metric: Metric = keys::decode(&cursor.value()?).map_err(kvshare::Error::Engine)?;
        summed.requests += 1;
        summed.tokens += metric.tokens;
        valid = cursor.next().await?;
    }
    if summed != total {
        ctx.journal.violation(&format!(
            "metrics: the total is {total:?} but the records sum to {summed:?}"
        ));
    }
    Ok(())
}

/// A thread's journal, followed from where the reader left it, or read whole.
async fn thread(
    tx: &Transaction,
    ctx: &mut Ctx,
    followed: &mut HashMap<String, (u64, u64, u32)>,
) -> kvshare::Result<()> {
    // The threads, listed by their keys only.
    let mut threads = Vec::new();
    let mut cursor = tx.range(b"tm/".as_slice(), b"tm0".as_slice())?;
    cursor.set_keys_only(true);
    let mut valid = cursor.seek_first().await?;
    while valid {
        threads.extend(keys::thread_of(cursor.key().user_key(), "tm/"));
        valid = cursor.next().await?;
    }
    drop(cursor);
    if threads.is_empty() {
        return Ok(());
    }
    let thread = threads[ctx.rng.below(threads.len() as u64) as usize].clone();
    let Some(meta) = tx.get(keys::meta(&thread)).await? else {
        return Ok(());
    };
    let meta: Meta = keys::decode(&meta).map_err(kvshare::Error::Engine)?;
    // From the start now and then, from where the reader left it otherwise.
    let (mut last, mut epoch, mut pid) = match followed.get(&thread) {
        Some(&at) if !ctx.rng.chance(30) => at,
        _ => (0, 0, 0),
    };
    if last > meta.head {
        ctx.journal.violation(&format!(
            "thread {thread}: its head went back from {last} to {}",
            meta.head
        ));
        (last, epoch, pid) = (0, 0, 0);
    }
    let (start, end) = keys::entries(&thread);
    let mut cursor = tx.range(start, end)?;
    let mut valid = cursor.seek(&keys::entry(&thread, last + 1)).await?;
    while valid {
        let seq = keys::number_of(cursor.key().user_key()).unwrap_or(0);
        let entry: Entry = keys::decode(&cursor.value()?).map_err(kvshare::Error::Engine)?;
        if seq != last + 1 {
            ctx.journal.violation(&format!(
                "thread {thread}: entry {seq} follows entry {last}"
            ));
        }
        if entry.epoch < epoch || (entry.epoch == epoch && entry.pid != pid && pid != 0) {
            ctx.journal.violation(&format!(
                "thread {thread}: entry {seq} of epoch {} by {} follows epoch {epoch} by {pid}",
                entry.epoch, entry.pid
            ));
        }
        (last, epoch, pid) = (seq, entry.epoch, entry.pid);
        valid = cursor.next().await?;
    }
    if last != meta.head {
        ctx.journal.violation(&format!(
            "thread {thread}: its head is {} but its last entry is {last}",
            meta.head
        ));
    }
    followed.insert(thread, (last, epoch, pid));
    Ok(())
}

/// A task's versions, newest first, are its transitions' steps, last first.
async fn task(tx: &Transaction, ctx: &mut Ctx) -> kvshare::Result<()> {
    let id = ctx.rng.below(TASKS);
    let key = keys::task(id);
    let mut past = key.clone();
    past.push(0);
    let mut versions = Vec::new();
    let options = HistoryOptions::new().with_limit(10_000);
    let mut cursor = tx.history_with_options(key.as_slice(), past.as_slice(), &options)?;
    let mut valid = cursor.seek_first().await?;
    while valid {
        let task: Task = keys::decode(&cursor.value()?).map_err(kvshare::Error::Engine)?;
        versions.push(task.status);
        valid = cursor.next().await?;
    }
    drop(cursor);
    let (start, end) = keys::transitions(id);
    let mut steps = Vec::new();
    let mut cursor = tx.range(start, end)?;
    let mut valid = cursor.seek_first().await?;
    while valid {
        let step: Transition = keys::decode(&cursor.value()?).map_err(kvshare::Error::Engine)?;
        steps.push(step.to);
        valid = cursor.next().await?;
    }
    steps.reverse();
    if versions != steps {
        ctx.journal.violation(&format!(
            "task {id}: its versions are {versions:?} but its transitions went {steps:?}"
        ));
    }
    Ok(())
}
