//! What an owner's work costs in SurrealKV itself, apart from the socket:
//!
//! - a change of owner: closing a tree, which flushes its memtable, and opening it
//!   again, after a close and after a crash, which replays the log of what the memtable
//!   held. A short-lived owner pays the close on its way out, and the process that owns
//!   the database after it pays the open;
//! - stalls: small commits one after another while the owner runs a checkpoint, then a
//!   compaction of the whole tree, for a client; and a close while a checkpoint runs;
//! - a close while a compaction runs, the next owner opening the database at once: what
//!   SurrealKV alone does, which kvshare's owner keeps from happening by waiting for its
//!   compactions before it closes.
//!
//! `kv-owner [MiB…]` writes a base of 256 MiB as `kv-sim` writes (no history, no value
//! log, a fifth of the values of 2 to 8 KiB) and closes it. Then, for each amount of
//! writes left in the memtable (0, 8, 32 and 96 MiB unless given), three times: it writes
//! the amount and closes, opens, writes it again and abandons the tree as a crash would,
//! and opens. Last, with the largest amount in the memtable, it times the commits before,
//! during and after a checkpoint and a compaction. It prints the load average of the
//! machine, which inflates the times when it is high.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use surrealkv::{CompactRangeOptions, LSMIterator, Tree, TreeBuilder};

/// What the base holds before the measurement.
const BASE: usize = 256 << 20;

/// How many times each amount is measured.
const ROUNDS: usize = 3;

/// The bytes of values one transaction writes.
const BATCH: usize = 1 << 20;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut amounts = std::env::args()
        .skip(1)
        .map(|mib| mib.parse::<usize>())
        .collect::<Result<Vec<_>, _>>()?;
    if amounts.is_empty() {
        amounts = vec![0, 8, 32, 96];
    }
    let dir = std::env::temp_dir().join(format!("kvo-owner-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let db = dir.join("db");
    let mut writer = Writer {
        next: 0,
        state: 0x9e37_79b9_7f4a_7c15,
    };

    let tree = open(&db)?;
    writer.write(&tree, BASE, true).await?;
    tree.close().await?;
    drop(tree);
    println!("base: {} MiB, load average: {}", BASE >> 20, load_average());

    println!("memtable MiB  close ms  open after close ms  open after crash ms  records replayed");
    for &mib in &amounts {
        for _ in 0..ROUNDS {
            let tree = open(&db)?;
            writer.write(&tree, mib << 20, false).await?;
            let started = Instant::now();
            tree.close().await?;
            let close = started.elapsed();
            drop(tree);

            let started = Instant::now();
            let tree = open(&db)?;
            let after_close = started.elapsed();
            writer.write(&tree, mib << 20, false).await?;
            tree.abandon();
            drop(tree);

            let started = Instant::now();
            let tree = open(&db)?;
            let after_crash = started.elapsed();
            let replayed = tree.recovery_report().records_replayed;
            tree.close().await?;
            println!(
                "{mib:>12}  {:>8}  {:>19}  {:>19}  {replayed:>16}",
                ms(close),
                ms(after_close),
                ms(after_crash)
            );
        }
    }

    let fill = amounts.iter().max().copied().unwrap_or(0) << 20;
    stalls(&db, &dir.join("checkpoint"), &mut writer, fill).await?;
    closes(&db, &dir.join("checkpoint-2"), &mut writer, fill).await?;
    println!("load average: {}", load_average());
    let _ = std::fs::remove_dir_all(&dir);
    Ok(())
}

/// Commit small writes one after another, a second before a checkpoint, during it, a
/// second between it and a compaction of the whole tree, during that, and a second
/// after; the memtable holds `fill` bytes first, which the checkpoint flushes. Print the
/// times of the commits in each part.
async fn stalls(
    db: &Path,
    checkpoint: &Path,
    writer: &mut Writer,
    fill: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    let tree = Arc::new(open(db)?);
    writer.write(&tree, fill, false).await?;
    let stop = Arc::new(AtomicBool::new(false));
    let committing = {
        let (tree, stop) = (Arc::clone(&tree), Arc::clone(&stop));
        tokio::spawn(async move {
            let mut times = Vec::new();
            let mut n = 0u64;
            while !stop.load(Ordering::Relaxed) {
                let started = Instant::now();
                let mut tx = tree.begin()?;
                tx.set(format!("s/{n:020}").into_bytes(), vec![7u8; 200])?;
                tx.commit().await?;
                times.push((started, started.elapsed()));
                n += 1;
            }
            Ok::<_, surrealkv::Error>(times)
        })
    };

    let pause = Duration::from_secs(1);
    tokio::time::sleep(pause).await;
    let during_checkpoint = {
        let (tree, to) = (Arc::clone(&tree), PathBuf::from(checkpoint));
        let started = Instant::now();
        tokio::task::spawn_blocking(move || tree.create_checkpoint(to).map(|_| ())).await??;
        (started, Instant::now())
    };
    tokio::time::sleep(pause).await;
    let during_compaction = {
        let tree = Arc::clone(&tree);
        let started = Instant::now();
        tokio::task::spawn_blocking(move || {
            tree.compact_range(None, None, &CompactRangeOptions::default())
        })
        .await??;
        (started, Instant::now())
    };
    tokio::time::sleep(pause).await;
    stop.store(true, Ordering::Relaxed);
    let times = committing.await??;

    tree.close().await?;

    println!(
        "commits of 200 bytes (memtable {} MiB first): part  took ms  commits  p50 p99 max ms",
        fill >> 20
    );
    // A commit belongs to the operation it overlapped, if any.
    let overlaps = |(from, to): (Instant, Instant), (started, took): &(Instant, Duration)| {
        *started < to && *started + *took > from
    };
    let mut parts: [(&str, Option<Duration>, Vec<Duration>); 3] = [
        ("neither", None, Vec::new()),
        (
            "checkpoint",
            Some(during_checkpoint.1 - during_checkpoint.0),
            Vec::new(),
        ),
        (
            "compaction",
            Some(during_compaction.1 - during_compaction.0),
            Vec::new(),
        ),
    ];
    for sample in &times {
        let part = if overlaps(during_checkpoint, sample) {
            1
        } else if overlaps(during_compaction, sample) {
            2
        } else {
            0
        };
        parts[part].2.push(sample.1);
    }
    for (part, took, mut samples) in parts {
        samples.sort();
        let at = |q: f64| {
            samples
                .get(((samples.len().max(1) - 1) as f64 * q) as usize)
                .copied()
                .unwrap_or_default()
        };
        println!(
            "  {part:<10}  {:>7}  {:>7}  {} {} {}",
            took.map_or("-".into(), ms),
            samples.len(),
            ms(at(0.5)),
            ms(at(0.99)),
            ms(samples.last().copied().unwrap_or_default())
        );
    }
    Ok(())
}

/// Close a tree while a checkpoint runs, and while a compaction runs, the memtable
/// holding `fill` bytes first, as a short-lived owner leaves while it makes one for a
/// client. After the compaction, the next owner opens the database at once, writes and
/// closes, and the keys are read back.
async fn closes(
    db: &Path,
    checkpoint: &Path,
    writer: &mut Writer,
    fill: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    // The close waits for the checkpoint.
    let tree = Arc::new(open(db)?);
    writer.write(&tree, fill, false).await?;
    let checkpointing = {
        let (tree, to) = (Arc::clone(&tree), PathBuf::from(checkpoint));
        tokio::task::spawn_blocking(move || {
            let started = Instant::now();
            let made = tree.create_checkpoint(to).map(|_| ());
            (made, started.elapsed())
        })
    };
    tokio::time::sleep(Duration::from_millis(20)).await;
    let started = Instant::now();
    tree.close().await?;
    let close = started.elapsed();
    let (made, took) = checkpointing.await?;
    println!(
        "close 20 ms into a checkpoint: {} ms; the checkpoint took {} ms and ended {}",
        ms(close),
        ms(took),
        made.map_or_else(|error| format!("with: {error}"), |()| "well".into())
    );

    // A compaction under way when the tree closes, as when a short-lived owner leaves
    // while it runs one for a client. The keys are counted first, so that what goes
    // missing after it is told from what went missing before.
    let tree = Arc::new(open(db)?);
    let (keys, unreadable) = count(&tree)?;
    println!(
        "keys before the compaction: {keys} of {} written, {unreadable} unreadable",
        writer.next
    );
    let compacting = {
        let tree = Arc::clone(&tree);
        tokio::task::spawn_blocking(move || {
            let started = Instant::now();
            let compacted = tree.compact_range(None, None, &CompactRangeOptions::default());
            (compacted, started.elapsed())
        })
    };
    tokio::time::sleep(Duration::from_millis(200)).await;
    let started = Instant::now();
    tree.close().await?;
    let close = started.elapsed();
    // The next owner opens the database as soon as the lock is free, writes and closes.
    let next = open(db).map(Arc::new);
    let next_opened = started.elapsed();
    if let Ok(next) = &next {
        writer.write(next, 8 << 20, false).await?;
        next.close().await?;
    }
    let (compacted, compaction) = compacting.await?;
    println!(
        "close 200 ms into a compaction: {} ms; the compaction took {} ms and ended {}",
        ms(close),
        ms(compaction),
        compacted.map_or_else(|error| format!("with: {error}"), |()| "well".into())
    );
    match &next {
        Ok(_) => println!(
            "  the next owner opened {} ms after the close began, {} the compaction ended",
            ms(next_opened),
            if Duration::from_millis(200) + next_opened < compaction {
                "before"
            } else {
                "after"
            }
        ),
        Err(error) => println!("  the next owner could not open: {error}"),
    }
    drop(next);

    // Every key written is there and reads, after both owners. SurrealKV alone does not
    // promise it: kvshare's owner waits for its compactions before it closes, for this.
    let tree = match open(db) {
        Ok(tree) => tree,
        Err(error) => {
            println!("  the database does not open after both: {error}");
            return Ok(());
        }
    };
    let (keys, unreadable) = count(&tree)?;
    tree.close().await?;
    println!(
        "  keys after both: {keys} of {} written, {unreadable} unreadable",
        writer.next
    );
    Ok(())
}

/// The keys `Writer` wrote that `tree` holds, and how many of their values do not read.
fn count(tree: &Tree) -> surrealkv::Result<(u64, u64)> {
    let tx = tree.begin()?;
    let mut options = surrealkv::ReadOptions::new();
    options.set_iterate_lower_bound(Some(b"k/".to_vec()));
    options.set_iterate_upper_bound(Some(b"k0".to_vec()));
    let mut cursor = tx.range_with_options(&options)?;
    let (mut keys, mut unreadable) = (0, 0);
    let mut valid = cursor.seek_first()?;
    while valid {
        keys += 1;
        if cursor.value().is_err() {
            unreadable += 1;
        }
        valid = cursor.next()?;
    }
    Ok((keys, unreadable))
}

/// Open the tree in `dir` as `kv-sim` opens it, with no history and no value log, but
/// with SurrealKV's memtable of 100 MB, which the measurement fills to each amount.
fn open(dir: &Path) -> surrealkv::Result<Tree> {
    TreeBuilder::new().with_path(dir.to_path_buf()).build()
}

/// Keys in the order they are written, and values of random bytes.
struct Writer {
    next: u64,
    state: u64,
}

impl Writer {
    /// Write `bytes` of values: with `large`, a fifth of them of 2 to 8 KiB; otherwise all
    /// of 100 to 900 bytes.
    async fn write(&mut self, tree: &Tree, bytes: usize, large: bool) -> surrealkv::Result<()> {
        let mut written = 0;
        while written < bytes {
            let mut tx = tree.begin()?;
            let mut batch = 0;
            while batch < BATCH && written + batch < bytes {
                let size = if large && self.below(5) == 0 {
                    2048 + self.below(6144)
                } else {
                    100 + self.below(800)
                };
                let mut value = Vec::with_capacity(size + 8);
                while value.len() < size {
                    value.extend_from_slice(&self.random().to_le_bytes());
                }
                value.truncate(size);
                tx.set(format!("k/{:020}", self.next).into_bytes(), value)?;
                self.next += 1;
                batch += size;
            }
            tx.commit().await?;
            written += batch;
        }
        Ok(())
    }

    /// SplitMix64.
    fn random(&mut self) -> u64 {
        self.state = self.state.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.state;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }

    fn below(&mut self, bound: usize) -> usize {
        (self.random() % bound as u64) as usize
    }
}

fn ms(duration: Duration) -> String {
    format!("{:.1}", duration.as_secs_f64() * 1e3)
}

fn load_average() -> String {
    let output = std::process::Command::new("sysctl")
        .args(["-n", "vm.loadavg"])
        .output();
    match output {
        Ok(output) if output.status.success() => {
            String::from_utf8_lossy(&output.stdout).trim().to_string()
        }
        _ => std::fs::read_to_string("/proc/loadavg").unwrap_or_else(|_| "unknown".into()),
    }
}
