//! What other processes do to a database: an owner that stops answering and resumes,
//! and clients killed in the middle of their transactions.

mod common;

use std::path::Path;
use std::process::Stdio;
use std::time::Duration;

use common::{base, builder};
use kvshare::{Error, TreeBuilder};
use tokio::io::{AsyncBufReadExt as _, BufReader};
use tokio::process::{Child, Command};

/// Start `kv-worker` with `args` and wait for its first line.
async fn worker(args: &[&str]) -> (Child, String) {
    let mut child = Command::new(env!("CARGO_BIN_EXE_kv-worker"))
        .args(args)
        .stdout(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .expect("a worker starts");
    let mut said = String::new();
    BufReader::new(child.stdout.take().expect("a piped stdout"))
        .read_line(&mut said)
        .await
        .expect("the worker's first line");
    (child, said.trim().to_string())
}

/// Send `signal` to the process `pid`.
async fn signal(signal: &str, pid: u32) {
    let status = Command::new("kill")
        .arg(format!("-{signal}"))
        .arg(pid.to_string())
        .status()
        .await
        .expect("kill runs");
    assert!(status.success(), "kill -{signal} {pid}");
}

fn path(path: &Path) -> &str {
    path.to_str().expect("a path in UTF-8")
}

/// An owner stopped with SIGSTOP leaves its client's requests unanswered: the client
/// gets `OwnerSilent` once the call timeout passes. A request bigger than the socket's
/// buffer stays whole meanwhile, so once SIGCONT resumes the owner the same connection
/// works on, and the owner has applied what the silent request carried.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_stopped_owner_is_silent_and_works_on_once_resumed() {
    let base = base("stopped");
    let (db, run) = (base.join("db"), base.join("run"));
    let (mut owner, said) = worker(&["serve", path(&db), path(&run)]).await;
    assert_eq!(said, "owner", "the worker owns the database");
    let pid = owner.id().expect("a running owner");

    let client = TreeBuilder::new()
        .with_path(db.clone())
        .with_versioning(true, 0)
        .with_run_dir(run.clone())
        .with_call_timeout(Duration::from_millis(500))
        .build()
        .await
        .expect("the client");
    let mut seed = client.begin().await.expect("a transaction");
    seed.set(b"k", b"v").expect("a write");
    seed.commit().await.expect("a commit");

    signal("STOP", pid).await;
    let mut silent = client.begin().await.expect("begin asks nothing");
    let big = vec![7u8; 4 << 20];
    silent.set(b"big", &big).expect("a write");
    match silent.get(b"k").await {
        Err(Error::OwnerSilent {
            pid: silent_pid, ..
        }) => assert_eq!(silent_pid, pid),
        other => panic!("a stopped owner should be silent, got {other:?}"),
    }
    signal("CONT", pid).await;

    let after = client.begin().await.expect("a transaction");
    assert_eq!(
        after
            .get(b"k")
            .await
            .expect("the same connection works on")
            .as_deref(),
        Some(&b"v"[..])
    );
    let carried = silent
        .get(b"big")
        .await
        .expect("the silent transaction works on");
    assert_eq!(carried.map(|value| value.len()), Some(big.len()));
    drop((silent, after));

    client.close().await.expect("the client closes");
    owner.start_kill().expect("SIGKILL to the owner");
    owner.wait().await.expect("the owner exits");
    let _ = std::fs::remove_dir_all(&base);
}

/// Clients killed with SIGKILL while their transactions are open, the owner holding
/// their writes, leave nothing behind: the owner serves on, their writes never show,
/// and the owner's own commits on the keys they read for update go through.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn clients_killed_in_their_transactions_leave_nothing_behind() {
    const ROUNDS: usize = 5;
    const HOLDERS: usize = 4;
    let base = base("killed");
    let (db, run) = (base.join("db"), base.join("run"));
    let owner = builder(&base).build().await.expect("the owner");
    assert!(owner.is_owner().await.expect("a role"));

    for round in 0..ROUNDS {
        let mut holders = Vec::new();
        for n in 0..HOLDERS {
            let id = (round * HOLDERS + n).to_string();
            let (holder, said) = worker(&["hold", path(&db), path(&run), &id]).await;
            assert_eq!(said, "holding", "the holder's transaction is open");
            holders.push(holder);
        }
        for mut holder in holders {
            holder.start_kill().expect("SIGKILL to a holder");
            holder.wait().await.expect("the holder exits");
        }
        let mut tx = owner.begin().await.expect("a transaction");
        tx.set(b"k", round.to_string().as_bytes()).expect("a write");
        tx.commit()
            .await
            .expect("the owner commits over the dead clients' reads");
    }

    let check = owner.begin().await.expect("a transaction");
    let mut held = check.range(b"held-", b"held.").expect("a cursor");
    assert!(
        !held.seek_first().await.expect("a seek"),
        "no write of a killed client shows"
    );
    drop(held);
    drop(check);

    let client = builder(&base).build().await.expect("a new client");
    let tx = client.begin().await.expect("a transaction");
    assert_eq!(
        tx.get(b"k").await.expect("the owner serves on").as_deref(),
        Some((ROUNDS - 1).to_string().as_bytes())
    );
    drop(tx);
    client.close().await.expect("the client closes");
    owner.close().await.expect("the owner closes");
    let _ = std::fs::remove_dir_all(&base);
}
