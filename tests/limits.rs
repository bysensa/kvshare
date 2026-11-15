//! What is too large for one frame of the owner's socket: a transaction's writes, the
//! values of one read, a single entry, and a request that no frame holds.

mod common;

use common::{base, close, owner_and_client};
use kvshare::{Error, MAX_ENTRY};

/// The keys `<prefix>000` to `<prefix>069`.
fn keys(prefix: char) -> Vec<Vec<u8>> {
    (0..70u32)
        .map(|n| format!("{prefix}{n:03}").into_bytes())
        .collect()
}

/// A client's transaction may write more than a frame holds: its writes travel ahead of
/// the commit in several frames, and the commit lands them all, as an owner's does.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_client_commits_more_than_a_frame_holds() {
    let base = base("lim1");
    let (owner, client) = owner_and_client(&base).await;
    let value = vec![1u8; 1 << 20];
    let mut tx = client.begin().await.expect("a transaction");
    for key in keys('a') {
        tx.set(key, &value).expect("a write");
    }
    tx.commit().await.expect("a commit of 70 MiB through the owner");

    let tx = owner.begin().await.expect("a transaction");
    let read = tx.get_many(&keys('a')).await.expect("the owner reads");
    assert_eq!(read.len(), 70);
    assert!(
        read.iter()
            .all(|found| found.as_deref() == Some(&value[..])),
        "every value landed whole"
    );
    drop(tx);
    close(owner, client, &base).await;
}

/// A client reads values that no single frame holds: the owner refuses the answer that
/// is too large, and the client asks for the halves of its keys.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_client_reads_many_values_that_no_frame_holds() {
    let base = base("lim2");
    let (owner, client) = owner_and_client(&base).await;
    let value = vec![2u8; 1 << 20];
    let mut tx = owner.begin().await.expect("a transaction");
    for key in keys('b') {
        tx.set(key, &value).expect("a write");
    }
    tx.commit().await.expect("the owner's commit");

    let tx = client.begin().await.expect("a transaction");
    let read = tx.get_many(&keys('b')).await.expect("the client reads");
    assert_eq!(read.len(), 70);
    assert!(
        read.iter()
            .all(|found| found.as_deref() == Some(&value[..])),
        "every value came, in the order of its key"
    );
    drop(tx);
    close(owner, client, &base).await;
}

/// A key with a value over the limit is refused when it is written, by an owner and by
/// a client alike, and the transaction works on.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_entry_over_the_limit_is_refused_alike_by_the_owner_and_a_client() {
    let base = base("lim3");
    let (owner, client) = owner_and_client(&base).await;
    let huge = vec![3u8; MAX_ENTRY];
    for (role, tree) in [("owner", &owner), ("client", &client)] {
        let mut tx = tree.begin().await.expect("a transaction");
        assert!(
            matches!(tx.set(b"k", &huge), Err(Error::TooLarge(_))),
            "the {role} refuses a key and a value of more than {MAX_ENTRY} bytes"
        );
        tx.set(role.as_bytes(), b"small").expect("a write that fits");
        tx.commit().await.expect("the transaction works on");
    }
    let tx = client.begin().await.expect("a transaction");
    assert_eq!(
        tx.get_many(&[&b"owner"[..], &b"client"[..]])
            .await
            .expect("a read"),
        vec![Some(b"small".to_vec()), Some(b"small".to_vec())]
    );
    assert_eq!(tx.get(b"k").await.expect("a read"), None);
    drop(tx);
    close(owner, client, &base).await;
}

/// A request that no frame holds, here a read of a key of 66 MiB, is `TooLarge` and is
/// not sent; the writes the transaction kept for it are not lost, and the owner is not
/// taken for gone.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_request_no_frame_holds_keeps_the_transactions_writes() {
    let base = base("lim4");
    let (owner, client) = owner_and_client(&base).await;
    let mut tx = client.begin().await.expect("a transaction");
    tx.set(b"k", b"v").expect("a write");
    let key = vec![4u8; 66 << 20];
    assert!(
        matches!(tx.get(&key).await, Err(Error::TooLarge(_))),
        "the read is refused before it is sent"
    );
    assert_eq!(
        tx.get(b"k").await.expect("the same transaction reads").as_deref(),
        Some(&b"v"[..]),
        "the write the refused read would have carried is still the transaction's"
    );
    tx.commit().await.expect("a commit");
    assert!(!client.is_owner().await.expect("a role"), "the owner is the same");
    close(owner, client, &base).await;
}
