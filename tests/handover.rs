//! The election, the hand-over and point reads and writes, within one process: see
//! `common` for how one process plays an owner and a client.

mod common;

use std::sync::Arc;

use common::{base, close, options, owner_and_client};
use kvshare::Shared;
use kvshare::protocol::Durability;

/// The first value to open the database owns it; the second works through it, sees its
/// writes, and takes the database over when the first lets it go.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_client_works_through_the_owner_and_takes_over_when_it_leaves() {
    let base = base("handover");
    let db = base.join("db");
    let first = Shared::new(&db, options(&base)).expect("the first value");
    let second = Shared::new(&db, options(&base)).expect("the second value");

    let mut tx = first
        .begin(Durability::Eventual)
        .await
        .expect("the first owns the database");
    assert!(tx.is_local(), "the first value opened the database itself");
    tx.set(b"k", b"from the owner").await.expect("a write");
    tx.commit().await.expect("a commit");

    let mut tx = second
        .begin(Durability::Eventual)
        .await
        .expect("the second connects");
    assert!(!tx.is_local(), "the second value works through the owner");
    assert_eq!(
        tx.get(b"k").await.expect("a read").as_deref(),
        Some(&b"from the owner"[..])
    );
    tx.set(b"k", b"from the client").await.expect("a write");
    tx.commit().await.expect("a commit through the owner");

    first
        .close()
        .await
        .expect("the owner hands the database over");

    // `begin` asks a client's owner nothing, so the first read after the owner left
    // finds the connection gone; the transaction after it elects this value, which
    // opens the database itself.
    let (tx, value) = loop {
        let mut tx = second
            .begin(Durability::Eventual)
            .await
            .expect("a transaction");
        match tx.get(b"k").await {
            Ok(value) => break (tx, value),
            Err(kvshare::Error::OwnerLost(_)) => continue,
            Err(other) => panic!("the client could not take over: {other}"),
        }
    };
    assert!(tx.is_local(), "the client became the owner");
    assert_eq!(value.as_deref(), Some(&b"from the client"[..]));
    tx.rollback().await.expect("a rollback");

    second.close().await.expect("the last owner closes");
    let _ = std::fs::remove_dir_all(&base);
}

/// A remote read-modify-write that loses to a commit of the owner on the same key is
/// refused as a conflict, as it would be in one process: the read took its snapshot.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_remote_update_that_loses_a_race_is_a_conflict() {
    let base = base("conflict");
    let (owner, client) = owner_and_client(&base).await;

    let mut remote = client
        .begin(Durability::Eventual)
        .await
        .expect("a remote transaction");
    assert_eq!(remote.get(b"k").await.expect("a read"), None);
    let mut local = owner
        .begin(Durability::Eventual)
        .await
        .expect("a local transaction");
    local.set(b"k", b"local").await.expect("a write");
    local.commit().await.expect("the first commit wins");
    remote.set(b"k", b"remote").await.expect("a write");
    match remote.commit().await {
        Err(kvshare::Error::Conflict) => {}
        other => panic!("the second commit should conflict, got {other:?}"),
    }

    close(owner, client, &base).await;
}

/// A remote transaction that reads nothing takes its snapshot when it commits, so a
/// commit made since its `begin` does not conflict with it, and its write comes last.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_remote_transaction_without_reads_takes_its_snapshot_at_commit() {
    let base = base("blind");
    let (owner, client) = owner_and_client(&base).await;

    let mut remote = client
        .begin(Durability::Eventual)
        .await
        .expect("a remote transaction");
    remote.set(b"k", b"remote").await.expect("a write");
    let mut local = owner
        .begin(Durability::Eventual)
        .await
        .expect("a local transaction");
    local.set(b"k", b"local").await.expect("a write");
    local.commit().await.expect("a commit");
    remote
        .commit()
        .await
        .expect("no conflict: the remote read nothing");

    let mut read = owner.begin(Durability::Eventual).await.expect("a read");
    assert_eq!(
        read.get(b"k").await.expect("a get").as_deref(),
        Some(&b"remote"[..])
    );
    read.rollback().await.expect("a rollback");

    close(owner, client, &base).await;
}

/// A remote transaction reads its own writes and deletes before they are sent, and the
/// owner sees them only once the commit is in.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_remote_transaction_reads_its_own_writes() {
    let base = base("own");
    let (owner, client) = owner_and_client(&base).await;
    let mut seed = owner.begin(Durability::Eventual).await.expect("a seed");
    seed.set(b"old", b"there").await.expect("a write");
    seed.commit().await.expect("a commit");

    let mut remote = client
        .begin(Durability::Eventual)
        .await
        .expect("a remote transaction");
    remote.set(b"new", b"mine").await.expect("a write");
    remote.delete(b"old").await.expect("a delete");
    assert_eq!(
        remote.get(b"new").await.expect("a get").as_deref(),
        Some(&b"mine"[..])
    );
    assert_eq!(remote.get(b"old").await.expect("a get"), None);

    let mut before = owner.begin(Durability::Eventual).await.expect("a read");
    assert_eq!(before.get(b"new").await.expect("a get"), None);
    assert!(before.get(b"old").await.expect("a get").is_some());
    before.rollback().await.expect("a rollback");

    remote.commit().await.expect("a commit");
    let mut after = owner.begin(Durability::Eventual).await.expect("a read");
    assert_eq!(
        after.get(b"new").await.expect("a get").as_deref(),
        Some(&b"mine"[..])
    );
    assert_eq!(after.get(b"old").await.expect("a get"), None);
    after.rollback().await.expect("a rollback");

    close(owner, client, &base).await;
}

/// Many tasks of one client run transactions at once over its one connection, and each
/// answer reaches its own caller.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_tasks_of_one_client_share_its_connection() {
    const TASKS: usize = 32;
    let base = base("tasks");
    let (owner, client) = owner_and_client(&base).await;
    let client = Arc::new(client);

    let mut tasks = tokio::task::JoinSet::new();
    for n in 0..TASKS {
        let client = Arc::clone(&client);
        tasks.spawn(async move {
            let key = format!("task-{n:02}").into_bytes();
            let mut tx = client.begin(Durability::Eventual).await?;
            assert_eq!(tx.get(&key).await?, None, "each task reads its own key");
            tx.set(&key, &key).await?;
            tx.commit().await
        });
    }
    while let Some(done) = tasks.join_next().await {
        done.expect("a task ends").expect("its transaction commits");
    }

    let mut read = owner.begin(Durability::Eventual).await.expect("a read");
    for n in 0..TASKS {
        let key = format!("task-{n:02}").into_bytes();
        assert_eq!(read.get(&key).await.expect("a get"), Some(key));
    }
    read.rollback().await.expect("a rollback");

    Arc::into_inner(client)
        .expect("every task let the client go")
        .close()
        .await
        .expect("the client closes");
    owner.close().await.expect("the owner closes");
    let _ = std::fs::remove_dir_all(&base);
}
