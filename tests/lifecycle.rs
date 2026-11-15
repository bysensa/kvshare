//! How a tree ends: dropped without a close, closed while its tasks still begin
//! transactions, and a client's transaction dropped where no runtime is.

mod common;

use std::time::Duration;

use common::{base, builder, owner_and_client};

/// An owner whose every handle is dropped lets the database go, as SurrealKV's tree
/// does when it is dropped: the next tree to open the database owns it and finds what
/// was committed.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_tree_dropped_without_close_lets_the_database_go() {
    let base = base("dropped");
    let first = builder(&base).build().await.expect("the owner");
    let mut tx = first.begin().await.expect("a transaction");
    tx.set(b"k", b"v").expect("a write");
    tx.commit().await.expect("a commit");
    drop(tx);
    drop(first);

    let second = builder(&base).build().await.expect("the next tree");
    assert!(
        second.is_owner().await.expect("a role"),
        "the dropped tree no longer owns or serves the database"
    );
    let tx = second.begin().await.expect("a transaction");
    assert_eq!(
        tx.get(b"k").await.expect("a read").as_deref(),
        Some(&b"v"[..])
    );
    drop(tx);
    second.close().await.expect("the tree closes");
    let _ = std::fs::remove_dir_all(&base);
}

/// A tree closed while its tasks begin transactions stays closed: no task that began
/// just before the close elects this process again after it. The next tree to open the
/// database owns it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_tree_closed_while_transactions_begin_stays_closed() {
    for round in 0..100 {
        let base = base(&format!("lc{round}"));
        let tree = builder(&base).build().await.expect("the owner");
        let mut tasks = Vec::new();
        for _ in 0..8 {
            let tree = tree.clone();
            tasks.push(tokio::spawn(async move {
                while let Ok(tx) = tree.begin().await {
                    drop(tx);
                    tokio::task::yield_now().await;
                }
            }));
        }
        tokio::time::sleep(Duration::from_millis(2)).await;
        tree.close().await.expect("the tree closes");
        for task in tasks {
            task.await.expect("a task ends");
        }

        let next = builder(&base).build().await.expect("the next tree");
        assert!(
            next.is_owner().await.expect("a role"),
            "round {round}: the closed tree was elected again and still serves the database"
        );
        next.close().await.expect("the next tree closes");
        let _ = std::fs::remove_dir_all(&base);
    }
}

/// A client's transaction dropped on a thread with no runtime is rolled back in the
/// owner all the same: 64 of them leave the owner's table of the connection empty.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_transaction_dropped_outside_a_runtime_is_rolled_back() {
    let base = base("offrt");
    let (owner, client) = owner_and_client(&base).await;
    for _ in 0..64 {
        let tx = client.begin().await.expect("a transaction");
        std::thread::spawn(move || drop(tx))
            .join()
            .expect("the thread ends");
    }
    let tx = client.begin().await.expect("a transaction");
    assert_eq!(
        tx.get(b"k").await.expect("the 65th transaction reads"),
        None
    );
    drop(tx);
    common::close(owner, client, &base).await;
}
