//! How much the socket costs: the same operations on a database this process owns, and
//! on a database another process owns, reached through its socket.
//!
//! `kv-latency [iterations]` prints the median and the 99th percentile of each operation
//! and the load average of the machine, which inflates the tails when it is high.

use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::{Duration, Instant};

use kvshare::protocol::Durability;
use kvshare::{Options, Shared};
use tokio::io::{AsyncBufReadExt, AsyncReadExt as _, AsyncWriteExt as _, BufReader};
use tokio::net::UnixStream;
use tokio::process::Command;

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

    let local = Shared::new(&base.join("local"), Options::new(&run_dir))?;
    let in_process = measure(&local, iterations).await?;
    local.close().await?;

    // Another process owns this database; this one reaches it through the socket.
    let remote_db = base.join("remote");
    let mut owner = Command::new(sibling("kv-worker")?)
        .arg("serve")
        .arg(&remote_db)
        .arg(&run_dir)
        .stdout(Stdio::piped())
        .kill_on_drop(true)
        .spawn()?;
    let mut said = String::new();
    BufReader::new(owner.stdout.take().expect("a piped stdout"))
        .read_line(&mut said)
        .await?;
    assert_eq!(said.trim(), "owner", "the child process owns the database");
    let remote = Shared::new(&remote_db, Options::new(&run_dir))?;
    assert_eq!(
        remote.owner_pid().await?,
        owner.id().expect("a running owner"),
        "this process works through the child"
    );
    let through_socket = measure(&remote, iterations).await?;
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
        "iterations: {iterations} (Immediate and scans: {}), load average: {}",
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

/// Time each operation on `shared`, after writing the keys it reads.
async fn measure(
    shared: &Shared,
    iterations: usize,
) -> Result<Vec<(&'static str, Vec<Duration>)>, kvshare::Error> {
    let keys: Vec<Vec<u8>> = (0..KEYS)
        .map(|n| format!("key-{n:06}").into_bytes())
        .collect();
    let mut tx = shared.begin(Durability::Eventual).await?;
    for key in &keys {
        tx.set(key, &[7u8; 100]).await?;
    }
    tx.commit().await?;

    let mut get = Vec::with_capacity(iterations);
    let mut tx = shared.begin(Durability::Eventual).await?;
    // The first read begins the transaction in the owner; the rest are the ones timed.
    tx.get(&keys[0]).await?;
    for n in 0..iterations {
        let started = Instant::now();
        tx.get(&keys[n % KEYS]).await?;
        get.push(started.elapsed());
    }
    tx.rollback().await?;

    let mut read = Vec::with_capacity(iterations);
    for n in 0..iterations {
        let started = Instant::now();
        let mut tx = shared.begin(Durability::Eventual).await?;
        tx.get(&keys[n % KEYS]).await?;
        tx.rollback().await?;
        read.push(started.elapsed());
    }

    let mut get_at = Vec::with_capacity(iterations);
    for n in 0..iterations {
        let started = Instant::now();
        shared.get_at(&keys[n % KEYS], u64::MAX).await?;
        get_at.push(started.elapsed());
    }

    let mut eventual = Vec::with_capacity(iterations);
    for n in 0..iterations {
        let started = Instant::now();
        let mut tx = shared.begin(Durability::Eventual).await?;
        tx.set(&keys[n % KEYS], &[8u8; 100]).await?;
        tx.commit().await?;
        eventual.push(started.elapsed());
    }

    let mut immediate = Vec::with_capacity(iterations / 20);
    for n in 0..iterations / 20 {
        let started = Instant::now();
        let mut tx = shared.begin(Durability::Immediate).await?;
        tx.set(&keys[n % KEYS], &[9u8; 100]).await?;
        tx.commit().await?;
        immediate.push(started.elapsed());
    }

    let mut update = Vec::with_capacity(iterations);
    for n in 0..iterations {
        let started = Instant::now();
        let mut tx = shared.begin(Durability::Eventual).await?;
        tx.get(&keys[n % KEYS]).await?;
        tx.set(&keys[n % KEYS], &[10u8; 100]).await?;
        tx.commit().await?;
        update.push(started.elapsed());
    }

    // Every key, in pages of the default size: `key.` is the first key after `key-…`.
    let mut scan = Vec::with_capacity(iterations / 20);
    for _ in 0..iterations / 20 {
        let started = Instant::now();
        let mut tx = shared.begin(Durability::Eventual).await?;
        let mut read = 0;
        {
            let mut entries = tx.range(b"key-", b"key.");
            while entries.next().await?.is_some() {
                read += 1;
            }
        }
        tx.rollback().await?;
        scan.push(started.elapsed());
        assert_eq!(read, KEYS, "the scan reads every key");
    }

    Ok(vec![
        ("get in an open transaction", get),
        ("read: begin + get + rollback", read),
        ("get_at (a read of its own)", get_at),
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
