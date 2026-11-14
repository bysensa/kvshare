//! What history and the value log cost in disk space and in time in SurrealKV itself.
//!
//! 1000 keys whose 4 KiB values go to the value log, when it is on, are each written 25
//! times, in files of the value log of 4 MiB: with no history, with a history of one
//! second (waited out before the compaction), and with one kept for ever; each once alone
//! and once with a key written first and never replaced, as a fact that lives long. The
//! tree is closed right after the writes, as an owner that leaves, then opened, compacted
//! whole, closed and opened again. `kv-space` prints what the value log and the tables
//! take then, and the time of the writes, of the close and of the compaction.

use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

use surrealkv::{CompactRangeOptions, Tree, TreeBuilder};

const KEYS: usize = 1000;
const VALUE: usize = 4096;
const ROUNDS: usize = 25;
const VLOG_FILE: u64 = 4 << 20;

/// How long the history of one second waits before the compaction.
const PAST_RETENTION: Duration = Duration::from_millis(1500);

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let base = std::env::temp_dir().join(format!("kvo-space-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&base);
    println!(
        "live values {} MiB, written {} MiB, value log files of {} MiB",
        (KEYS * VALUE) >> 20,
        (KEYS * VALUE * ROUNDS) >> 20,
        VLOG_FILE >> 20
    );
    println!(
        "value log  history   a key never replaced  value log MiB  tables MiB  value log files  \
         write ms  close ms  compact ms"
    );
    let histories: [(&str, Option<Duration>); 3] = [
        ("none", None),
        ("1 s", Some(Duration::from_secs(1))),
        ("for ever", Some(Duration::ZERO)),
    ];
    for vlog in [true, false] {
        for (name, history) in histories {
            for pinned in [false, true] {
                measure(&base, vlog, name, history, pinned, false).await?;
            }
        }
    }
    // The key never replaced written again, its value unchanged, before the compaction:
    // what a collector of the value log would do with the live values of its oldest file.
    measure(&base, true, "none", None, true, true).await?;
    let _ = std::fs::remove_dir_all(&base);
    Ok(())
}

/// Write, close, compact and reopen one tree, and print what it takes. With `rewrite`,
/// the key never replaced is written again, unchanged, before the compaction.
async fn measure(
    base: &Path,
    vlog: bool,
    name: &str,
    history: Option<Duration>,
    pinned: bool,
    rewrite: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let dir = base.join(format!("{vlog}-{name}-{pinned}-{rewrite}").replace(' ', "-"));
    let tree = open(&dir, vlog, history)?;
    let started = Instant::now();
    if pinned {
        let mut tx = tree.begin()?;
        tx.set(b"pin".to_vec(), vec![1u8; VALUE])?;
        tx.commit().await?;
    }
    for round in 0..ROUNDS {
        for chunk in (0..KEYS).collect::<Vec<_>>().chunks(100) {
            let mut tx = tree.begin()?;
            for &key in chunk {
                tx.set(
                    format!("k/{key:06}").into_bytes(),
                    vec![(round % 251) as u8; VALUE],
                )?;
            }
            tx.commit().await?;
        }
    }
    let write = started.elapsed();
    // Closed right after the writes, as an owner that leaves: it waits for the
    // background work the writes started.
    let started = Instant::now();
    tree.close().await?;
    let close = started.elapsed();
    drop(tree);

    if history.is_some_and(|retention| !retention.is_zero()) {
        tokio::time::sleep(PAST_RETENTION).await;
    }
    let tree = Arc::new(open(&dir, vlog, history)?);
    if rewrite {
        let mut tx = tree.begin()?;
        if let Some(value) = tx.get_for_update(b"pin")? {
            tx.set(b"pin".to_vec(), value)?;
        }
        tx.commit().await?;
    }
    let compacting = Arc::clone(&tree);
    let started = Instant::now();
    tokio::task::spawn_blocking(move || {
        compacting.compact_range(None, None, &CompactRangeOptions::default())
    })
    .await??;
    let compaction = started.elapsed();
    tree.close().await?;
    drop(tree);
    // A reopen cleans up what the close left.
    open(&dir, vlog, history)?.close().await?;

    let (in_vlog, files) = size(&dir.join("vlog"));
    let (tables, _) = size(&dir.join("sstables"));
    println!(
        "{:<9}  {name:<9} {:<20}  {:>13}  {:>10}  {files:>15}  {:>8}  {:>8}  {:>10}",
        if vlog { "on" } else { "off" },
        match (pinned, rewrite) {
            (false, _) => "no",
            (true, false) => "yes",
            (true, true) => "yes, written again",
        },
        in_vlog >> 20,
        tables >> 20,
        ms(write),
        ms(close),
        ms(compaction)
    );
    Ok(())
}

/// Open the tree in `dir`: with `vlog`, values of 1 KiB and more in the value log, in
/// files of `VLOG_FILE`; `history` kept for that long, for ever if zero, or none.
fn open(dir: &Path, vlog: bool, history: Option<Duration>) -> surrealkv::Result<Tree> {
    let builder = TreeBuilder::new()
        .with_path(dir.to_path_buf())
        .with_enable_vlog(vlog)
        .with_vlog_max_file_size(VLOG_FILE);
    match history {
        Some(retention) => builder.with_versioning(true, retention.as_nanos() as u64),
        None => builder.with_versioning(false, 0),
    }
    .build()
}

fn ms(duration: Duration) -> String {
    format!("{:.0}", duration.as_secs_f64() * 1e3)
}

/// The bytes of the files in `dir`, and how many there are.
fn size(dir: &Path) -> (u64, usize) {
    std::fs::read_dir(dir).map_or((0, 0), |entries| {
        entries
            .flatten()
            .filter_map(|entry| entry.metadata().ok())
            .filter(|meta| meta.is_file())
            .fold((0, 0), |(bytes, files), meta| {
                (bytes + meta.len(), files + 1)
            })
    })
}
