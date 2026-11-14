//! A process of the tests and of the latency measurement.
//!
//! - `kv-worker write <db> <run_dir> <id> <eventual|immediate> <acks>`: commit the keys
//!   `w<id>-00000000`, `w<id>-00000001`, … until killed, one per transaction. A commit
//!   whose outcome is unknown is retried with the same key and value, which is safe to
//!   write twice. Each commit that returned is appended to `acks` as `key\tunix_nanos`.
//! - `kv-worker serve <db> <run_dir> [vlog]`: take part in the election, print `owner`
//!   or `client <pid>`, and wait until killed; with `vlog`, large values go to the value
//!   log.
//! - `kv-worker hold <db> <run_dir> <id>`: begin a transaction, write `held-<id>`, read
//!   `k` for update so that the owner has the write, print `holding`, and wait until
//!   killed with the transaction open.
//! - `kv-worker echo <socket>`: print `listening`, then send back every 64-byte frame
//!   of the one connection it takes.

use std::io::Write as _;
use std::path::PathBuf;
use std::process::ExitCode;
use std::time::{SystemTime, UNIX_EPOCH};

use kvshare::{Durability, Error, Tree, TreeBuilder};
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

#[tokio::main]
async fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let outcome = match args
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>()
        .as_slice()
    {
        ["write", db, run_dir, id, durability, acks] => {
            let durability = match *durability {
                "eventual" => Durability::Eventual,
                "immediate" => Durability::Immediate,
                other => return usage(&format!("unknown durability {other}")),
            };
            write(db.into(), run_dir.into(), id, durability, acks.into()).await
        }
        ["serve", db, run_dir] => serve(db.into(), run_dir.into(), false).await,
        ["serve", db, run_dir, "vlog"] => serve(db.into(), run_dir.into(), true).await,
        ["hold", db, run_dir, id] => hold(db.into(), run_dir.into(), id).await,
        ["echo", socket] => echo(socket.into()).await,
        _ => return usage("unknown arguments"),
    };
    match outcome {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("kv-worker {}: {error}", std::process::id());
            ExitCode::FAILURE
        }
    }
}

async fn write(
    db: PathBuf,
    run_dir: PathBuf,
    id: &str,
    durability: Durability,
    acks: PathBuf,
) -> Result<(), Box<dyn std::error::Error>> {
    let tree = open(db, run_dir, false).await?;
    // One write per line and no buffer: a line that is written survives the death of
    // this process, as a commit in the page cache does.
    let mut acks = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(acks)?;
    for n in 0u64.. {
        let key = format!("w{id}-{n:08}");
        loop {
            let attempt = async {
                let mut tx = tree.begin().await?.with_durability(durability);
                tx.set(key.as_bytes(), key.as_bytes())?;
                tx.commit().await
            };
            match attempt.await {
                Ok(()) => break,
                // The owner died or is being elected: the next attempt elects again.
                Err(
                    Error::OwnerLost(_)
                    | Error::NoOwner { .. }
                    | Error::OwnerSilent { .. }
                    | Error::Handshake(_)
                    | Error::TransactionWriteConflict,
                ) => continue,
                Err(other) => return Err(other.into()),
            }
        }
        let at = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
        acks.write_all(format!("{key}\t{at}\n").as_bytes())?;
    }
    Ok(())
}

async fn serve(
    db: PathBuf,
    run_dir: PathBuf,
    vlog: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let tree = open(db, run_dir, vlog).await?;
    let owner = tree.owner_pid().await?;
    if owner == std::process::id() {
        println!("owner");
    } else {
        println!("client {owner}");
    }
    std::io::stdout().flush()?;
    std::future::pending::<()>().await;
    Ok(())
}

async fn hold(db: PathBuf, run_dir: PathBuf, id: &str) -> Result<(), Box<dyn std::error::Error>> {
    let tree = open(db, run_dir, false).await?;
    let mut tx = tree.begin().await?;
    tx.set(format!("held-{id}").as_bytes(), b"never committed")?;
    tx.get_for_update(b"k").await?;
    println!("holding");
    std::io::stdout().flush()?;
    std::future::pending::<()>().await;
    Ok(())
}

/// Open the database at `db` with versions kept, as every process of the tests does,
/// and with `vlog` large values in the value log. Opening elects at once, so a database
/// that is free on the way in is this process's.
async fn open(db: PathBuf, run_dir: PathBuf, vlog: bool) -> Result<Tree, Error> {
    TreeBuilder::new()
        .with_path(db)
        .with_versioning(true, 0)
        .with_enable_vlog(vlog)
        .with_run_dir(run_dir)
        .build()
        .await
}

/// Send back every 64-byte frame that arrives on `socket`: what the socket and the
/// runtime cost alone, without remote calls on top.
async fn echo(socket: PathBuf) -> Result<(), Box<dyn std::error::Error>> {
    let _ = std::fs::remove_file(&socket);
    let listener = tokio::net::UnixListener::bind(&socket)?;
    println!("listening");
    std::io::stdout().flush()?;
    let (mut stream, _) = listener.accept().await?;
    let mut frame = [0u8; ECHO_FRAME];
    while stream.read_exact(&mut frame).await.is_ok() {
        stream.write_all(&frame).await?;
    }
    Ok(())
}

/// The size of a frame of `echo`; `kv-latency` sends frames of the same size.
const ECHO_FRAME: usize = 64;

fn usage(problem: &str) -> ExitCode {
    eprintln!(
        "kv-worker: {problem}\n\
         usage: kv-worker write <db> <run_dir> <id> <eventual|immediate> <acks>\n\
         \x20      kv-worker serve <db> <run_dir>\n\
         \x20      kv-worker hold <db> <run_dir> <id>\n\
         \x20      kv-worker echo <socket>"
    );
    ExitCode::from(2)
}
