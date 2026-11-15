//! `Tree::transact`: a transaction repeated until it is known to have committed once,
//! across conflicts, a silent owner and a change of owner.

mod common;

use std::path::Path;
use std::process::Stdio;
use std::time::{Duration, Instant};

use common::{base, close, owner_and_client};
use kvshare::{Error, Operation, Transaction, Tree, TreeBuilder};
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

/// A client of the database in `base` that a worker serves, giving up on a request
/// after `call`.
async fn client_of(base: &Path, call: Duration) -> Tree {
    TreeBuilder::new()
        .with_path(base.join("db"))
        .with_versioning(true, 0)
        .with_run_dir(base.join("run"))
        .with_call_timeout(call)
        .build()
        .await
        .expect("the client")
}

/// Add one to the counter `n`, as the operation that leaves `mark` in the database so
/// that it can be found again.
struct AddOne {
    mark: Vec<u8>,
    applied: usize,
    found: usize,
}

impl AddOne {
    fn named(name: &str) -> Self {
        AddOne {
            mark: format!("op/{name}").into_bytes(),
            applied: 0,
            found: 0,
        }
    }
}

impl Operation for AddOne {
    type Output = ();

    async fn apply(&mut self, tx: &mut Transaction) -> kvshare::Result<()> {
        self.applied += 1;
        let now = match tx.get_for_update(b"n").await? {
            Some(bytes) => u64::from_le_bytes(bytes.try_into().expect("eight bytes")),
            None => 0,
        };
        tx.set(b"n", &(now + 1).to_le_bytes())?;
        tx.set(&self.mark, b"")?;
        Ok(())
    }

    async fn landed(&mut self, tx: &Transaction) -> kvshare::Result<Option<()>> {
        let landed = tx.get(&self.mark).await?.map(|_| ());
        self.found += usize::from(landed.is_some());
        Ok(landed)
    }
}

/// Write the key `key` without reading anything: its commit is the transaction's
/// first request, so it is sent even to an owner that answers nothing.
struct Mark {
    key: &'static [u8],
    applied: usize,
    found: usize,
}

impl Operation for Mark {
    type Output = ();

    async fn apply(&mut self, tx: &mut Transaction) -> kvshare::Result<()> {
        self.applied += 1;
        tx.set(self.key, b"")
    }

    async fn landed(&mut self, tx: &Transaction) -> kvshare::Result<Option<()>> {
        let landed = tx.get(self.key).await?.map(|_| ());
        self.found += usize::from(landed.is_some());
        Ok(landed)
    }
}

/// An operation that fails with an error repeating does not cure.
struct Refused(usize);

impl Operation for Refused {
    type Output = ();

    async fn apply(&mut self, _tx: &mut Transaction) -> kvshare::Result<()> {
        self.0 += 1;
        Err(Error::InvalidArgument("not this".into()))
    }

    async fn landed(&mut self, _tx: &Transaction) -> kvshare::Result<Option<()>> {
        Ok(None)
    }
}

async fn add_one(tree: &Tree, name: String) -> kvshare::Result<()> {
    tree.transact(Duration::from_secs(20), &mut AddOne::named(&name))
        .await
}

async fn counter(tree: &Tree) -> u64 {
    let tx = tree.begin().await.expect("a transaction");
    let bytes = tx.get(b"n").await.expect("a read").expect("a counter");
    u64::from_le_bytes(bytes.try_into().expect("eight bytes"))
}

/// Transactions that conflict are repeated until each has committed: 160 additions by
/// the tasks of an owner and of its client make 160.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn transactions_that_conflict_all_commit_once() {
    let base = base("tr1");
    let (owner, client) = owner_and_client(&base).await;
    let mut tasks = Vec::new();
    for task in 0..8 {
        let tree = if task % 2 == 0 {
            owner.clone()
        } else {
            client.clone()
        };
        tasks.push(tokio::spawn(async move {
            for n in 0..20 {
                add_one(&tree, format!("{task}-{n}"))
                    .await
                    .expect("an addition");
            }
        }));
    }
    for task in tasks {
        task.await.expect("a task ends");
    }
    assert_eq!(counter(&client).await, 160);
    close(owner, client, &base).await;
}

/// A commit that a stopped owner did not answer may have happened. `transact` asks
/// `landed` before it writes again, finds the commit there once the owner resumes, and
/// does not apply it a second time.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_commit_the_owner_was_silent_about_is_not_applied_twice() {
    let base = base("tr2");
    let (mut owner, said) =
        worker(&["serve", path(&base.join("db")), path(&base.join("run"))]).await;
    assert_eq!(said, "owner");
    let pid = owner.id().expect("a running owner");
    let client = client_of(&base, Duration::from_millis(150)).await;

    signal("STOP", pid).await;
    let resume = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(700)).await;
        signal("CONT", pid).await;
    });
    let mut once = Mark {
        key: b"op/once",
        applied: 0,
        found: 0,
    };
    client
        .transact(Duration::from_secs(20), &mut once)
        .await
        .expect("the transaction is known to have committed");
    resume.await.expect("the owner resumed");
    assert_eq!(once.applied, 1, "applied once");
    assert_eq!(once.found, 1, "and found to have landed");

    drop(client);
    owner.start_kill().expect("SIGKILL to the owner");
    owner.wait().await.expect("the owner exits");
    let _ = std::fs::remove_dir_all(&base);
}

/// Transactions go on across a change of owner: the owner is killed in the middle of
/// fifty additions, this process takes the database over, and the counter is fifty.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn transactions_go_on_with_the_next_owner() {
    let base = base("tr3");
    let (mut owner, said) =
        worker(&["serve", path(&base.join("db")), path(&base.join("run"))]).await;
    assert_eq!(said, "owner");
    let client = client_of(&base, Duration::from_secs(2)).await;
    for n in 0..50 {
        if n == 20 {
            owner.start_kill().expect("SIGKILL to the owner");
            owner.wait().await.expect("the owner exits");
        }
        add_one(&client, format!("solo-{n}"))
            .await
            .expect("an addition");
    }
    assert!(
        client.is_owner().await.expect("a role"),
        "this process took over"
    );
    assert_eq!(counter(&client).await, 50);
    client.close().await.expect("the tree closes");
    let _ = std::fs::remove_dir_all(&base);
}

/// An error that repeating does not cure ends the call at once, and an owner that
/// stays silent ends it when its time is up, with the error of the commit whose
/// outcome stays unknown.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_transaction_gives_up_on_what_repeating_does_not_cure() {
    let base = base("tr4");
    let (mut owner, said) =
        worker(&["serve", path(&base.join("db")), path(&base.join("run"))]).await;
    assert_eq!(said, "owner");
    let pid = owner.id().expect("a running owner");
    let client = client_of(&base, Duration::from_millis(100)).await;

    let mut refused = Refused(0);
    let answer = client.transact(Duration::from_secs(20), &mut refused).await;
    assert!(
        matches!(answer, Err(Error::InvalidArgument(_))),
        "{answer:?}"
    );
    assert_eq!(refused.0, 1, "asked once");

    signal("STOP", pid).await;
    let started = Instant::now();
    let mut mark = Mark {
        key: b"k",
        applied: 0,
        found: 0,
    };
    let silent = client.transact(Duration::from_millis(600), &mut mark).await;
    assert!(
        matches!(silent, Err(Error::OwnerSilent { .. })),
        "{silent:?}"
    );
    assert!(
        started.elapsed() < Duration::from_secs(3),
        "{:?}",
        started.elapsed()
    );

    signal("CONT", pid).await;
    drop(client);
    owner.start_kill().expect("SIGKILL to the owner");
    owner.wait().await.expect("the owner exits");
    let _ = std::fs::remove_dir_all(&base);
}
