//! What other processes do to a database: an owner that stops answering and resumes,
//! one stopped while a process opens the database, a commit it was silent about, and
//! clients killed in the middle of their transactions.

mod common;

use std::path::Path;
use std::process::Stdio;
use std::time::{Duration, Instant};

use common::{base, builder};
use kvshare::{Durability, Error, TreeBuilder};
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

/// A process that opens the database while its owner is stopped (a debugger, a laptop
/// asleep, SIGSTOP) waits for the owner's welcome as long as the election timeout lets
/// it, not only the call timeout, and is the owner's client once it resumes. An owner
/// stopped for longer than that leaves it with `NoOwner`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn opening_waits_for_a_stopped_owner_as_long_as_the_election_allows() {
    let base = base("stoppedopen");
    let (db, run) = (base.join("db"), base.join("run"));
    let (mut owner, said) = worker(&["serve", path(&db), path(&run)]).await;
    assert_eq!(said, "owner", "the worker owns the database");
    let pid = owner.id().expect("a running owner");
    let open = |election: Duration| {
        TreeBuilder::new()
            .with_path(db.clone())
            .with_versioning(true, 0)
            .with_run_dir(run.clone())
            .with_call_timeout(Duration::from_millis(300))
            .with_election_timeout(election)
            .build()
    };

    signal("STOP", pid).await;
    let started = Instant::now();
    let resume = async {
        tokio::time::sleep(Duration::from_secs(1)).await;
        signal("CONT", pid).await;
    };
    let (client, ()) = tokio::join!(open(Duration::from_secs(5)), resume);
    let client = client.expect("opening waits for the stopped owner");
    assert!(
        started.elapsed() >= Duration::from_secs(1),
        "the welcome came only after SIGCONT"
    );
    assert_eq!(client.owner_pid().await.expect("the owner"), pid);
    client.close().await.expect("the client closes");

    signal("STOP", pid).await;
    match open(Duration::from_millis(800)).await {
        Err(Error::NoOwner { waited }) => assert!(waited >= Duration::from_millis(800)),
        Err(other) => panic!("an owner stopped too long should be no owner, got {other}"),
        Ok(_) => panic!("a stopped owner cannot welcome a client"),
    }
    signal("CONT", pid).await;

    owner.start_kill().expect("SIGKILL to the owner");
    owner.wait().await.expect("the owner exits");
    let _ = std::fs::remove_dir_all(&base);
}

/// An owner killed while it is stopped, with its socket's queue full of connections it
/// never took, as when hooks keep coming while it is stopped: opening the database ends
/// within the election timeout, and this process, which finds the lock free, owns the
/// database. (A simulation once saw a connect here that the system never answered; this
/// does not bring it about, and `connect` bounds the connection with the welcome.)
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn opening_ends_when_a_stopped_owner_is_killed() {
    let base = base("stoppedkill");
    let (db, run) = (base.join("db"), base.join("run"));
    let (mut owner, said) = worker(&["serve", path(&db), path(&run)]).await;
    assert_eq!(said, "owner", "the worker owns the database");
    let pid = owner.id().expect("a running owner");
    let socket = std::fs::read_dir(&run)
        .expect("the run directory")
        .flatten()
        .map(|entry| entry.path())
        .find(|path| path.extension().is_some_and(|ext| ext == "sock"))
        .expect("the owner's socket");

    signal("STOP", pid).await;
    // More connections than the queue holds (128 on macOS), none of them taken.
    let mut queued = Vec::new();
    for _ in 0..200 {
        let connecting = tokio::net::UnixStream::connect(&socket);
        if let Ok(Ok(stream)) = tokio::time::timeout(Duration::from_millis(20), connecting).await {
            queued.push(stream);
        }
    }
    let opening = tokio::spawn(
        TreeBuilder::new()
            .with_path(db.clone())
            .with_versioning(true, 0)
            .with_run_dir(run.clone())
            .with_call_timeout(Duration::from_millis(300))
            .with_election_timeout(Duration::from_secs(3))
            .build(),
    );
    tokio::time::sleep(Duration::from_millis(300)).await;
    signal("KILL", pid).await;
    owner.wait().await.expect("the owner exits");
    drop(queued);

    let tree = tokio::time::timeout(Duration::from_secs(10), opening)
        .await
        .expect("opening ends within the election timeout, not never")
        .expect("the opening task")
        .expect("the database opens once the lock is free");
    assert_eq!(
        tree.owner_pid().await.expect("the owner"),
        std::process::id(),
        "this process owns the database"
    );
    tree.close().await.expect("the tree closes");
    let _ = std::fs::remove_dir_all(&base);
}

/// A commit that came back `OwnerSilent` lands once the owner resumes, and a
/// transaction the client begins after it sees it: the owner takes a snapshot only after
/// the commits sent before it on the same connection are done. So a client unsure of a
/// commit learns its outcome by reading, and never finds it absent first and there
/// later, which would make it apply the commit a second time.
///
/// The commit syncs its log, so it takes longer than reading the requests behind it,
/// which the owner finds waiting in its socket when it resumes.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_transaction_after_a_silent_commit_sees_it() {
    const ROUNDS: usize = 8;
    let base = base("silentcommit");
    let (db, run) = (base.join("db"), base.join("run"));
    let (mut owner, said) = worker(&["serve", path(&db), path(&run)]).await;
    assert_eq!(said, "owner", "the worker owns the database");
    let pid = owner.id().expect("a running owner");
    let client = TreeBuilder::new()
        .with_path(db.clone())
        .with_versioning(true, 0)
        .with_run_dir(run.clone())
        .with_call_timeout(Duration::from_millis(300))
        .build()
        .await
        .expect("the client");

    for round in 0..ROUNDS {
        let key = format!("silent-{round}");
        signal("STOP", pid).await;
        let mut silent = client
            .begin()
            .await
            .expect("begin asks nothing")
            .with_durability(Durability::Immediate);
        silent.set(key.as_bytes(), b"landed").expect("a write");
        match silent.commit().await {
            Err(Error::OwnerSilent { .. }) => {}
            other => panic!("round {round}: a stopped owner should be silent, got {other:?}"),
        }
        // Read the key back as a client unsure of the commit does: the requests wait in
        // the owner's socket behind the commit until it resumes.
        let check = client.begin().await.expect("begin asks nothing");
        let resume = async {
            tokio::time::sleep(Duration::from_millis(50)).await;
            signal("CONT", pid).await;
        };
        let (seen, ()) = tokio::join!(check.get(key.as_bytes()), resume);
        let seen = seen.expect("the owner answers once resumed");
        let later = client
            .begin()
            .await
            .expect("a transaction")
            .get(key.as_bytes())
            .await
            .expect("a read");
        assert_eq!(
            later.as_deref(),
            Some(&b"landed"[..]),
            "round {round}: the commit landed"
        );
        assert_eq!(
            seen.as_deref(),
            Some(&b"landed"[..]),
            "round {round}: a transaction begun after the silent commit missed it"
        );
    }

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
