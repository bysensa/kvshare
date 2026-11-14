//! How much the socket costs: the same operations on a database this process owns, and
//! on a database another process owns, reached through its socket.
//!
//! `kv-latency [iterations]` prints the median and the 99th percentile of each operation
//! and the load average of the machine, which inflates the tails when it is high.

use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::{Duration, Instant};

use kvshare::{Durability, Tree, TreeBuilder};
use tokio::io::{AsyncBufReadExt, AsyncReadExt as _, AsyncWriteExt as _, BufReader};
use tokio::net::UnixStream;
use tokio::process::{Child, Command};

/// Keys written before the measurement, read back in turn.
const KEYS: usize = 1_000;

/// The size of a frame sent to `kv-worker echo`, which answers frames of this size.
const ECHO_FRAME: usize = 64;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let iterations: usize = match std::env::args().nth(1) {
        Some(n) => n.parse()?,
        None => 20_000,
    };
    let base = std::env::temp_dir().join(format!("kvo-latency-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&base);
    let run_dir = base.join("run");

    let walks = iterations / 100;
    let local = open(base.join("local"), &run_dir, false).await?;
    let mut in_process = measure(&local, iterations).await?;
    local.close().await?;
    let local = open(base.join("local-vlog"), &run_dir, true).await?;
    in_process.extend(measure_value_log(&local, walks).await?);
    local.close().await?;

    // Another process owns these databases; this one reaches them through the socket.
    let remote_db = base.join("remote");
    let mut owner = spawn_owner(&remote_db, &run_dir, false).await?;
    let remote = open(remote_db, &run_dir, false).await?;
    assert_eq!(
        remote.owner_pid().await?,
        owner.id().expect("a running owner"),
        "this process works through the child"
    );
    let mut through_socket = measure(&remote, iterations).await?;
    remote.close().await?;
    owner.start_kill()?;
    owner.wait().await?;
    let remote_db = base.join("remote-vlog");
    let mut owner = spawn_owner(&remote_db, &run_dir, true).await?;
    let remote = open(remote_db, &run_dir, true).await?;
    through_socket.extend(measure_value_log(&remote, walks).await?);
    remote.close().await?;
    owner.start_kill()?;
    owner.wait().await?;

    // The socket and the runtime alone: frames to a process that sends them back.
    let echo_socket = run_dir.join("echo.sock");
    let mut echo = Command::new(sibling("kv-worker")?)
        .arg("echo")
        .arg(&echo_socket)
        .stdout(Stdio::piped())
        .kill_on_drop(true)
        .spawn()?;
    let mut said = String::new();
    BufReader::new(echo.stdout.take().expect("a piped stdout"))
        .read_line(&mut said)
        .await?;
    assert_eq!(said.trim(), "listening", "the echo process listens");
    let mut stream = UnixStream::connect(&echo_socket).await?;
    let mut frame = [0u8; ECHO_FRAME];
    let mut raw = Vec::with_capacity(iterations);
    for _ in 0..iterations {
        let started = Instant::now();
        stream.write_all(&frame).await?;
        stream.read_exact(&mut frame).await?;
        raw.push(started.elapsed());
    }
    // The echo process ends when its one connection closes.
    drop(stream);
    echo.wait().await?;
    let _ = std::fs::remove_dir_all(&base);

    println!(
        "iterations: {iterations} (Immediate and scans: {}, value log: {walks}), load average: {}",
        iterations / 20,
        load_average()
    );
    println!(
        "{:<40} {:>12} {:>12} {:>12} {:>12} {:>8}",
        "operation", "local p50", "local p99", "socket p50", "socket p99", "x p50"
    );
    for ((name, local), (_, socket)) in in_process.iter().zip(&through_socket) {
        let (lp50, lp99) = percentiles(local);
        let (sp50, sp99) = percentiles(socket);
        println!(
            "{name:<40} {:>12} {:>12} {:>12} {:>12} {:>8.1}",
            micros(lp50),
            micros(lp99),
            micros(sp50),
            micros(sp99),
            sp50.as_secs_f64() / lp50.as_secs_f64().max(1e-12)
        );
    }
    let (rp50, rp99) = percentiles(&raw);
    println!(
        "{:<40} {:>12} {:>12} {:>12} {:>12}",
        format!("raw socket round trip, {ECHO_FRAME} B"),
        "-",
        "-",
        micros(rp50),
        micros(rp99)
    );
    Ok(())
}

/// Open the database at `db`, which elects at once; with `vlog`, large values go to the
/// value log.
async fn open(db: PathBuf, run_dir: &Path, vlog: bool) -> Result<Tree, kvshare::Error> {
    TreeBuilder::new()
        .with_path(db)
        .with_versioning(true, 0)
        .with_enable_vlog(vlog)
        .with_run_dir(run_dir.to_path_buf())
        .build()
        .await
}

/// A `kv-worker` that owns the database at `db`, once it says so.
async fn spawn_owner(db: &Path, run_dir: &Path, vlog: bool) -> std::io::Result<Child> {
    let mut command = Command::new(sibling("kv-worker")?);
    command.arg("serve").arg(db).arg(run_dir);
    if vlog {
        command.arg("vlog");
    }
    let mut owner = command.stdout(Stdio::piped()).kill_on_drop(true).spawn()?;
    let mut said = String::new();
    BufReader::new(owner.stdout.take().expect("a piped stdout"))
        .read_line(&mut said)
        .await?;
    assert_eq!(said.trim(), "owner", "the child process owns the database");
    Ok(owner)
}

/// The keys of the value-log database and the size of each value: large enough that
/// SurrealKV keeps the values in the value log.
const LOGGED_KEYS: usize = 1_000;
const LOGGED_VALUE: usize = 16 << 10;

/// Time `walks` cursors over every key of `tree`, a database that keeps large values in
/// the value log: reading the values, and reading keys only.
async fn measure_value_log(
    tree: &Tree,
    walks: usize,
) -> Result<Vec<(&'static str, Vec<Duration>)>, kvshare::Error> {
    let mut tx = tree.begin().await?;
    for n in 0..LOGGED_KEYS {
        tx.set(format!("log-{n:06}").as_bytes(), vec![5u8; LOGGED_VALUE])?;
    }
    tx.commit().await?;

    let mut rows = Vec::new();
    for (name, keys_only) in [
        ("cursor, 1000 values of 16 KiB in the log", false),
        ("the same cursor, keys only", true),
    ] {
        let mut timed = Vec::with_capacity(walks);
        for _ in 0..walks {
            let started = Instant::now();
            let tx = tree.begin().await?;
            let mut read = 0;
            {
                let mut entries = tx.range(b"log-", b"log.")?;
                entries.set_keys_only(keys_only);
                let mut valid = entries.seek_first().await?;
                while valid {
                    read += 1;
                    valid = entries.next().await?;
                }
            }
            drop(tx);
            timed.push(started.elapsed());
            assert_eq!(read, LOGGED_KEYS, "the cursor reads every key");
        }
        rows.push((name, timed));
    }
    Ok(rows)
}

/// Time each operation on `tree`, after writing the keys it reads.
async fn measure(
    tree: &Tree,
    iterations: usize,
) -> Result<Vec<(&'static str, Vec<Duration>)>, kvshare::Error> {
    let keys: Vec<Vec<u8>> = (0..KEYS)
        .map(|n| format!("key-{n:06}").into_bytes())
        .collect();
    let mut tx = tree.begin().await?;
    for key in &keys {
        tx.set(key, &[7u8; 100])?;
    }
    tx.commit().await?;

    let mut get = Vec::with_capacity(iterations);
    let tx = tree.begin().await?;
    // The first read waits for the owner to open the transaction; the rest are timed.
    tx.get(&keys[0]).await?;
    for n in 0..iterations {
        let started = Instant::now();
        tx.get(&keys[n % KEYS]).await?;
        get.push(started.elapsed());
    }
    drop(tx);

    let mut read = Vec::with_capacity(iterations);
    for n in 0..iterations {
        let started = Instant::now();
        let mut tx = tree.begin().await?;
        tx.get(&keys[n % KEYS]).await?;
        tx.rollback();
        read.push(started.elapsed());
    }

    let mut eventual = Vec::with_capacity(iterations);
    for n in 0..iterations {
        let started = Instant::now();
        let mut tx = tree.begin().await?;
        tx.set(&keys[n % KEYS], &[8u8; 100])?;
        tx.commit().await?;
        eventual.push(started.elapsed());
    }

    let mut immediate = Vec::with_capacity(iterations / 20);
    for n in 0..iterations / 20 {
        let started = Instant::now();
        let mut tx = tree.begin().await?.with_durability(Durability::Immediate);
        tx.set(&keys[n % KEYS], &[9u8; 100])?;
        tx.commit().await?;
        immediate.push(started.elapsed());
    }

    let mut update = Vec::with_capacity(iterations);
    for n in 0..iterations {
        let started = Instant::now();
        let mut tx = tree.begin().await?;
        tx.get(&keys[n % KEYS]).await?;
        tx.set(&keys[n % KEYS], &[10u8; 100])?;
        tx.commit().await?;
        update.push(started.elapsed());
    }

    // Every key, in pages of the default size: `key.` is the first key after `key-…`.
    let mut scan = Vec::with_capacity(iterations / 20);
    for _ in 0..iterations / 20 {
        let started = Instant::now();
        let tx = tree.begin().await?;
        let mut read = 0;
        {
            let mut entries = tx.range(b"key-", b"key.")?;
            let mut valid = entries.seek_first().await?;
            while valid {
                read += 1;
                valid = entries.next().await?;
            }
        }
        drop(tx);
        scan.push(started.elapsed());
        assert_eq!(read, KEYS, "the scan reads every key");
    }

    Ok(vec![
        ("get in an open transaction", get),
        ("read: begin + get + rollback", read),
        ("write: begin + set + commit, Eventual", eventual),
        ("write: begin + set + commit, Immediate", immediate),
        ("update: get + set + commit, Eventual", update),
        ("scan of 1000 keys, pages of 256", scan),
    ])
}

/// The median and the 99th percentile.
fn percentiles(samples: &[Duration]) -> (Duration, Duration) {
    let mut sorted = samples.to_vec();
    sorted.sort();
    (sorted[sorted.len() / 2], sorted[sorted.len() * 99 / 100])
}

fn micros(duration: Duration) -> String {
    format!("{:.1} µs", duration.as_secs_f64() * 1e6)
}

/// The binary `name` built beside this one.
fn sibling(name: &str) -> std::io::Result<PathBuf> {
    let me = std::env::current_exe()?;
    Ok(me.parent().unwrap_or(Path::new(".")).join(name))
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
