//! An indexer, as Milou's code index stores facts by their content: it indexes a file
//! again, replacing the facts the file held with the facts it holds now. Facts are kept
//! by the hash of their content and shared by the files that hold them, with a count of
//! references; a fact no file refers to any more goes. One transaction changes the
//! file's facts, the counts and the facts together. A fifth of the facts are large, of 2
//! to 8 KiB, and one in a hundred very large, of 64 to 192 KiB, for the value log. After
//! each commit the indexer finds every fact of the file again with a cursor over keys
//! only.

use std::collections::BTreeSet;
use std::path::PathBuf;
use std::time::{Duration, Instant};

use kvshare::{Mode, Transaction};

use super::{Ctx, Op, Outcome, counted, stopping};
use crate::journal::Journal;
use crate::keys::{self, Fact, FileFacts, OpId};
use crate::rng::Rng;

/// The files the index holds.
const FILES: u64 = 200;

/// The facts several files share.
const SHARED: u64 = 500;

pub async fn run(run: PathBuf, seed: u64) -> Result<(), String> {
    let journal = Journal::open(&run, "indexer", 'i').map_err(|e| e.to_string())?;
    let tree = super::agent::open(&run).await?;
    let mut ctx = Ctx {
        run,
        tree,
        journal,
        rng: Rng::new(seed),
    };
    while !stopping(&ctx.run) {
        let path = format!("src/f{:03}.rs", ctx.rng.below(FILES));
        let mut facts = Vec::new();
        for _ in 0..ctx.rng.between(50, 200) {
            let body = if ctx.rng.chance(30) {
                let shared = ctx.rng.below(SHARED);
                format!("shared fact {shared}").into_bytes()
            } else {
                let size = if ctx.rng.chance(1) {
                    ctx.rng.between(64 << 10, 192 << 10)
                } else if ctx.rng.chance(20) {
                    ctx.rng.between(2 << 10, 8 << 10)
                } else {
                    ctx.rng.between(64, 512)
                };
                ctx.rng.bytes(size as usize)
            };
            facts.push((keys::hash_of(&body), body));
        }
        facts.sort_by(|a, b| a.0.cmp(&b.0));
        facts.dedup_by(|a, b| a.0 == b.0);
        let mut index = IndexOp {
            path: path.clone(),
            facts,
        };
        if ctx.perform("index", &mut index).await == Outcome::Acked {
            let started = Instant::now();
            if let Err(error) = verify(&mut ctx, &index).await {
                ctx.journal.count(&format!("verify_{}", counted(&error)));
            }
            ctx.journal
                .time("read.index", started.elapsed().as_micros(), "");
        }
        tokio::time::sleep(Duration::from_millis(ctx.rng.between(5, 50))).await;
    }
    let _ = ctx.tree.close().await;
    Ok(())
}

/// Every fact of the file just indexed is there, found by its key alone.
async fn verify(ctx: &mut Ctx, index: &IndexOp) -> kvshare::Result<()> {
    let tx = ctx.tree.begin_with_mode(Mode::ReadOnly).await?;
    let mut cursor = tx.range(b"ix/f/".as_slice(), b"ix/f0".as_slice())?;
    cursor.set_keys_only(true);
    for (hash, _) in &index.facts {
        let key = keys::fact(hash);
        let found = cursor.seek(&key).await? && cursor.key().user_key() == key.as_slice();
        if !found {
            ctx.journal.violation(&format!(
                "index: fact {hash} of {} is not there after its commit",
                index.path
            ));
        }
    }
    Ok(())
}

/// Index a file again: its facts, their reference counts, and the facts that come and go.
struct IndexOp {
    path: String,
    facts: Vec<(String, Vec<u8>)>,
}

impl Op for IndexOp {
    fn describe(&self) -> String {
        format!("index {} {}", self.path, self.facts.len())
    }

    async fn apply(&mut self, id: OpId, tx: &mut Transaction) -> kvshare::Result<bool> {
        let old: BTreeSet<String> = match tx.get(keys::file(&self.path)).await? {
            Some(bytes) => keys::decode::<FileFacts>(&bytes)
                .map_err(kvshare::Error::Engine)?
                .hashes
                .into_iter()
                .collect(),
            None => BTreeSet::new(),
        };
        let new: BTreeSet<String> = self.facts.iter().map(|(hash, _)| hash.clone()).collect();
        for hash in old.difference(&new) {
            let count = references(tx, hash).await?.saturating_sub(1);
            if count == 0 {
                tx.delete(keys::references(hash))?;
                tx.delete(keys::fact(hash))?;
            } else {
                tx.set(keys::references(hash), keys::encode(&count))?;
            }
        }
        for (hash, body) in &self.facts {
            if old.contains(hash) {
                continue;
            }
            let count = references(tx, hash).await? + 1;
            tx.set(keys::references(hash), keys::encode(&count))?;
            if count == 1 {
                let fact = Fact {
                    op: id,
                    body: body.clone(),
                };
                tx.set(keys::fact(hash), keys::encode(&fact))?;
            }
        }
        let file = FileFacts {
            op: id,
            hashes: new.into_iter().collect(),
        };
        tx.set(keys::file(&self.path), keys::encode(&file))?;
        Ok(true)
    }

    async fn landed(&self, id: OpId, tx: &Transaction) -> kvshare::Result<bool> {
        Ok(match tx.get(keys::file(&self.path)).await? {
            Some(bytes) => keys::decode::<FileFacts>(&bytes).is_ok_and(|f| f.op == id),
            None => false,
        })
    }
}

/// How many files refer to a fact, read for update.
async fn references(tx: &mut Transaction, hash: &str) -> kvshare::Result<u64> {
    Ok(match tx.get_for_update(keys::references(hash)).await? {
        Some(bytes) => keys::decode(&bytes).map_err(kvshare::Error::Engine)?,
        None => 0,
    })
}
