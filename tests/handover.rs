//! The election, the hand-over and point reads and writes, within one process: see
//! `common` for how one process plays an owner and a client.

mod common;

use std::sync::Arc;

use common::{base, builder, close, owner_and_client};
use kvshare::Error;

/// The first tree to open the database owns it; the second works through it, sees its
/// writes, and takes the database over when the first lets it go.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_client_works_through_the_owner_and_takes_over_when_it_leaves() {
    let base = base("handover");
    let first = builder(&base).build().await.expect("the first tree");
    let second = builder(&base).build().await.expect("the second tree");

    let mut tx = first.begin().await.expect("the first owns the database");
    assert!(tx.is_local(), "the first tree opened the database itself");
    tx.set(b"k", b"from the owner").expect("a write");
    tx.commit().await.expect("a commit");

    let mut tx = second.begin().await.expect("the second connects");
    assert!(!tx.is_local(), "the second tree works through the owner");
    assert_eq!(
        tx.get(b"k").await.expect("a read").as_deref(),
        Some(&b"from the owner"[..])
    );
    tx.set(b"k", b"from the client").expect("a write");
    tx.commit().await.expect("a commit through the owner");

    first
        .close()
        .await
        .expect("the owner hands the database over");

    // `begin` tells the owner without waiting, so the first request to find the
    // connection gone may be the read after it; the attempt after that elects this
    // tree, which opens the database itself.
    let (mut tx, value) = loop {
        let attempt = async {
            let tx = second.begin().await?;
            let value = tx.get(b"k").await?;
            Ok::<_, Error>((tx, value))
        };
        match attempt.await {
            Ok(done) => break done,
            Err(Error::OwnerLost(_)) => continue,
            Err(other) => panic!("the client could not take over: {other}"),
        }
    };
    assert!(tx.is_local(), "the client became the owner");
    assert_eq!(value.as_deref(), Some(&b"from the client"[..]));
    tx.rollback();

    second.close().await.expect("the last owner closes");
    let _ = std::fs::remove_dir_all(&base);
}

/// A remote read-modify-write that loses to a commit of the owner on the same key is
/// refused as a conflict, as it would be in one process.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_remote_update_that_loses_a_race_is_a_conflict() {
    let base = base("conflict");
    let (owner, client) = owner_and_client(&base).await;

    let mut remote = client.begin().await.expect("a remote transaction");
    assert_eq!(remote.get(b"k").await.expect("a read"), None);
    let mut local = owner.begin().await.expect("a local transaction");
    local.set(b"k", b"local").expect("a write");
    local.commit().await.expect("the first commit wins");
    remote.set(b"k", b"remote").expect("a write");
    match remote.commit().await {
        Err(Error::TransactionWriteConflict) => {}
        other => panic!("the second commit should conflict, got {other:?}"),
    }

    close(owner, client, &base).await;
}

/// A remote transaction reads its own writes and deletes before they are sent, and the
/// owner's other transactions see them only once the commit is in.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_remote_transaction_reads_its_own_writes() {
    let base = base("own");
    let (owner, client) = owner_and_client(&base).await;
    let mut seed = owner.begin().await.expect("a seed");
    seed.set(b"old", b"there").expect("a write");
    seed.commit().await.expect("a commit");

    let mut remote = client.begin().await.expect("a remote transaction");
    remote.set(b"new", b"mine").expect("a write");
    remote.delete(b"old").expect("a delete");
    assert_eq!(
        remote.get(b"new").await.expect("a get").as_deref(),
        Some(&b"mine"[..])
    );
    assert_eq!(remote.get(b"old").await.expect("a get"), None);

    let before = owner.begin().await.expect("a read");
    assert_eq!(before.get(b"new").await.expect("a get"), None);
    assert!(before.get(b"old").await.expect("a get").is_some());
    drop(before);

    remote.commit().await.expect("a commit");
    let after = owner.begin().await.expect("a read");
    assert_eq!(
        after.get(b"new").await.expect("a get").as_deref(),
        Some(&b"mine"[..])
    );
    assert_eq!(after.get(b"old").await.expect("a get"), None);
    drop(after);

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
            let mut tx = client.begin().await?;
            assert_eq!(tx.get(&key).await?, None, "each task reads its own key");
            tx.set(&key, &key)?;
            tx.commit().await
        });
    }
    while let Some(done) = tasks.join_next().await {
        done.expect("a task ends").expect("its transaction commits");
    }

    let read = owner.begin().await.expect("a read");
    for n in 0..TASKS {
        let key = format!("task-{n:02}").into_bytes();
        assert_eq!(read.get(&key).await.expect("a get"), Some(key));
    }
    drop(read);

    let client = Arc::into_inner(client).expect("every task let the client go");
    close(owner, client, &base).await;
}

/// A client that goes away leaves the owner's database open: the owner keeps committing
/// after the connection's table is gone.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_client_that_leaves_does_not_close_the_owners_database() {
    let base = base("leave");
    let (owner, client) = owner_and_client(&base).await;
    {
        // A transaction holds the connection too: it goes before the client closes.
        let mut tx = client.begin().await.expect("a remote transaction");
        tx.set(b"k", b"remote").expect("a write");
        tx.commit().await.expect("a commit through the owner");
    }
    client.close().await.expect("the client closes");

    // The connection's end reaches the owner a moment later: commit for a while after.
    for n in 0..20u32 {
        let mut tx = owner.begin().await.expect("a local transaction");
        tx.set(b"k", n.to_be_bytes().to_vec()).expect("a write");
        tx.commit()
            .await
            .unwrap_or_else(|error| panic!("commit {n} after the client left: {error}"));
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    }

    owner.close().await.expect("the owner closes");
    let _ = std::fs::remove_dir_all(&base);
}

/// An owner that ends as a crash does leaves the database to its client, which elects
/// itself and finds every commit that returned.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_client_takes_over_from_an_owner_that_crashes() {
    let base = base("abandon");
    let (owner, client) = owner_and_client(&base).await;
    {
        let mut tx = client.begin().await.expect("a remote transaction");
        tx.set(b"k", b"before the crash").expect("a write");
        tx.commit().await.expect("a commit through the owner");
    }
    owner.abandon().await;
    assert!(
        owner.begin().await.is_err(),
        "an abandoned tree begins nothing"
    );

    let (tx, value) = loop {
        let attempt = async {
            let tx = client.begin().await?;
            let value = tx.get(b"k").await?;
            Ok::<_, Error>((tx, value))
        };
        match attempt.await {
            Ok(done) => break done,
            Err(Error::OwnerLost(_)) => continue,
            Err(other) => panic!("the client could not take over: {other}"),
        }
    };
    assert!(tx.is_local(), "the client became the owner");
    assert_eq!(value.as_deref(), Some(&b"before the crash"[..]));
    drop(tx);
    close(owner, client, &base).await;
}

/// A client opens only as many transactions at once as the owner allows; one more is
/// refused on its first request, and the others still work.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_client_holds_a_bounded_number_of_transactions() {
    let base = base("bounded");
    let (owner, client) = owner_and_client(&base).await;

    let mut open = Vec::new();
    for _ in 0..64 {
        open.push(client.begin().await.expect("a transaction"));
    }
    let extra = client.begin().await.expect("begin asks nothing");
    match extra.get(b"k").await {
        Err(Error::TooManyTransactions) => {}
        other => panic!("one transaction too many should be refused, got {other:?}"),
    }
    assert_eq!(open[0].get(b"k").await.expect("a read"), None);
    drop(extra);
    drop(open);

    close(owner, client, &base).await;
}
