//! A keeper, as Milou's maintenance would run: checkpoints every five seconds, the two
//! newest kept, a compaction of the whole tree every ten, and every two the removal of a
//! closed thread: its journal, its record and its metrics go at once, and the total
//! loses the metrics with them. The number of the newest checkpoint that completed is
//! kept in `<run>/ckpt/last`.

use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use kvshare::{CompactRangeOptions, Mode, Transaction};

use super::{Ctx, Op, counted, stopping};
use crate::journal::Journal;
use crate::keys::{self, Meta, Metric, OpId, Total};
use crate::rng::Rng;

pub async fn run(run: PathBuf, seed: u64) -> Result<(), String> {
    let journal = Journal::open(&run, "keeper", 'k').map_err(|e| e.to_string())?;
    let tree = super::agent::open(&run).await?;
    let mut ctx = Ctx {
        run,
        tree,
        journal,
        rng: Rng::new(seed),
    };
    let mut checkpoint = last_checkpoint(&ctx.run).map_or(1, |n| n + 1);
    let (mut checkpointed, mut compacted, mut dropped) =
        (Instant::now(), Instant::now(), Instant::now());
    while !stopping(&ctx.run) {
        if checkpointed.elapsed() >= Duration::from_secs(5) {
            checkpointed = Instant::now();
            let dir = ctx.run.join("ckpt").join(checkpoint.to_string());
            match ctx.tree.create_checkpoint(&dir).await {
                Ok(_) => {
                    record_checkpoint(&ctx.run, checkpoint);
                    let old = ctx
                        .run
                        .join("ckpt")
                        .join((checkpoint.saturating_sub(2)).to_string());
                    let _ = std::fs::remove_dir_all(old);
                    checkpoint += 1;
                    ctx.journal
                        .time("keeper.checkpoint", checkpointed.elapsed().as_micros(), "");
                }
                Err(error) => ctx
                    .journal
                    .count(&format!("checkpoint_failed_{}", counted(&error))),
            }
        }
        if compacted.elapsed() >= Duration::from_secs(10) {
            compacted = Instant::now();
            match ctx
                .tree
                .compact_range(None, None, &CompactRangeOptions::default())
                .await
            {
                Ok(()) => ctx
                    .journal
                    .time("keeper.compact", compacted.elapsed().as_micros(), ""),
                Err(error) => ctx
                    .journal
                    .count(&format!("compact_failed_{}", counted(&error))),
            }
        }
        if dropped.elapsed() >= Duration::from_secs(2) {
            dropped = Instant::now();
            if let Some(thread) = closed_thread(&mut ctx).await {
                let mut drop = DropOp { thread };
                ctx.perform("drop", &mut drop).await;
            }
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let _ = ctx.tree.close().await;
    Ok(())
}

/// The number of the newest checkpoint that completed.
pub fn last_checkpoint(run: &Path) -> Option<u64> {
    std::fs::read_to_string(run.join("ckpt").join("last"))
        .ok()?
        .trim()
        .parse()
        .ok()
}

fn record_checkpoint(run: &Path, n: u64) {
    let (last, next) = (
        run.join("ckpt").join("last"),
        run.join("ckpt").join("last.new"),
    );
    if std::fs::write(&next, n.to_string()).is_ok() {
        let _ = std::fs::rename(next, last);
    }
}

/// A closed thread, if there is one.
async fn closed_thread(ctx: &mut Ctx) -> Option<String> {
    let tx = ctx.tree.begin_with_mode(Mode::ReadOnly).await.ok()?;
    let mut cursor = tx.range(b"tm/".as_slice(), b"tm0".as_slice()).ok()?;
    let mut valid = cursor.seek_first().await.ok()?;
    while valid {
        let meta: Meta = keys::decode(&cursor.value().ok()?).ok()?;
        if meta.closed {
            return keys::thread_of(cursor.key().user_key(), "tm/");
        }
        valid = cursor.next().await.ok()?;
    }
    None
}

/// Remove a closed thread: its journal, record and metrics, and its metrics from the
/// total.
struct DropOp {
    thread: String,
}

impl Op for DropOp {
    fn describe(&self) -> String {
        format!("drop {}", self.thread)
    }

    async fn apply(&mut self, _id: OpId, tx: &mut Transaction) -> kvshare::Result<bool> {
        let closed = match tx.get_for_update(keys::meta(&self.thread)).await? {
            Some(bytes) => keys::decode::<Meta>(&bytes).is_ok_and(|m| m.closed),
            None => false,
        };
        if !closed {
            return Ok(false);
        }
        let mut gone = Total::default();
        {
            let (start, end) = keys::metrics(&self.thread);
            let mut cursor = tx.range(start, end)?;
            let mut valid = cursor.seek_first().await?;
            while valid {
                let metric: Metric =
                    keys::decode(&cursor.value()?).map_err(kvshare::Error::Engine)?;
                gone.requests += 1;
                gone.tokens += metric.tokens;
                valid = cursor.next().await?;
            }
        }
        let mut total: Total = match tx.get_for_update(keys::TOTAL).await? {
            Some(bytes) => keys::decode(&bytes).map_err(kvshare::Error::Engine)?,
            None => Total::default(),
        };
        total.requests = total.requests.saturating_sub(gone.requests);
        total.tokens = total.tokens.saturating_sub(gone.tokens);
        tx.set(keys::TOTAL, keys::encode(&total))?;
        let (start, end) = keys::entries(&self.thread);
        tx.delete_range(start, end)?;
        let (start, end) = keys::metrics(&self.thread);
        tx.delete_range(start, end)?;
        tx.delete(keys::meta(&self.thread))?;
        tx.delete(keys::lease(&self.thread))?;
        Ok(true)
    }

    async fn landed(&self, _id: OpId, tx: &Transaction) -> kvshare::Result<bool> {
        Ok(tx.get(keys::meta(&self.thread)).await?.is_none())
    }
}
