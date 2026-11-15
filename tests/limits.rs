//! What is too large for one frame of the owner's socket: a transaction's writes, the
//! values of one read, a single entry, and a request that no frame holds.

mod common;

use common::{base, close, owner_and_client};
use kvshare::{DEFAULT_MAX_ENTRY, Error};

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
    tx.commit()
        .await
        .expect("a commit of 70 MiB through the owner");

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

/// A client reads values that no single frame holds: the owner answers the values of
/// many keys in parts, and the client asks for the keys that are left.
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
    let huge = vec![3u8; DEFAULT_MAX_ENTRY];
    for (role, tree) in [("owner", &owner), ("client", &client)] {
        let mut tx = tree.begin().await.expect("a transaction");
        assert!(
            matches!(tx.set(b"k", &huge), Err(Error::TooLarge(_))),
            "the {role} refuses a key and a value of more than {DEFAULT_MAX_ENTRY} bytes"
        );
        tx.set(role.as_bytes(), b"small")
            .expect("a write that fits");
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
        tx.get(b"k")
            .await
            .expect("the same transaction reads")
            .as_deref(),
        Some(&b"v"[..]),
        "the write the refused read would have carried is still the transaction's"
    );
    tx.commit().await.expect("a commit");
    assert!(
        !client.is_owner().await.expect("a role"),
        "the owner is the same"
    );
    close(owner, client, &base).await;
}

/// The values of many keys come through the owner in parts, and are the same, in the
/// same order, as the owner's own read of them: more keys than one request asks for,
/// some of them missing.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_client_reads_more_keys_than_one_request_asks_for() {
    let base = base("lim5");
    let (owner, client) = owner_and_client(&base).await;
    let keys: Vec<Vec<u8>> = (0..1000u32)
        .map(|n| format!("c{n:04}").into_bytes())
        .collect();
    let mut tx = owner.begin().await.expect("a transaction");
    for (n, key) in keys.iter().enumerate() {
        if n % 7 != 0 {
            tx.set(key, format!("value {n}").into_bytes())
                .expect("a write");
        }
    }
    tx.commit().await.expect("the owner's commit");

    let local = owner.begin().await.expect("a transaction");
    let remote = client.begin().await.expect("a transaction");
    let expected = local.get_many(&keys).await.expect("the owner reads");
    assert_eq!(expected.iter().filter(|value| value.is_none()).count(), 143);
    assert_eq!(
        remote.get_many(&keys).await.expect("the client reads"),
        expected
    );
    assert_eq!(
        remote.get_many::<Vec<u8>>(&[]).await.expect("no keys"),
        Vec::<Option<Vec<u8>>>::new()
    );
    drop((local, remote));
    close(owner, client, &base).await;
}

/// The database in `base`, by trees that let a key and its value hold `bytes` at most.
fn allowing(base: &std::path::Path, bytes: usize) -> kvshare::TreeBuilder {
    common::builder(base).with_max_entry_size(bytes)
}

/// Trees that allow larger entries than the default write and read them, through the
/// owner too: the frames of their connection grow with their entries. What is larger
/// still is refused.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn trees_that_allow_larger_entries_write_and_read_them() {
    let base = base("lim6");
    let owner = allowing(&base, 72 << 20).build().await.expect("the owner");
    let client = allowing(&base, 72 << 20).build().await.expect("the client");
    assert!(!client.is_owner().await.expect("a role"));

    let value = vec![6u8; 66 << 20];
    let mut tx = client.begin().await.expect("a transaction");
    tx.set(b"large", &value)
        .expect("an entry over the default of 64 MiB");
    tx.commit().await.expect("a commit through the owner");
    drop(value);
    for (role, tree) in [("owner", &owner), ("client", &client)] {
        let tx = tree.begin().await.expect("a transaction");
        let read = tx.get(b"large").await.expect("a read");
        assert_eq!(
            read.map(|value| value.len()),
            Some(66 << 20),
            "the {role} reads it whole"
        );
    }

    let mut tx = owner.begin().await.expect("a transaction");
    assert!(
        matches!(tx.set(b"k", vec![0u8; 73 << 20]), Err(Error::TooLarge(_))),
        "73 MiB is over what these trees allow"
    );
    drop(tx);
    close(owner, client, &base).await;
}

/// Trees that allow smaller entries refuse what the default lets through, an owner and
/// a client alike.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn trees_that_allow_smaller_entries_refuse_larger_ones() {
    let base = base("lim7");
    let owner = allowing(&base, 1 << 20).build().await.expect("the owner");
    let client = allowing(&base, 1 << 20).build().await.expect("the client");
    let value = vec![7u8; 2 << 20];
    for (role, tree) in [("owner", &owner), ("client", &client)] {
        let mut tx = tree.begin().await.expect("a transaction");
        assert!(
            matches!(tx.set(b"k", &value), Err(Error::TooLarge(_))),
            "the {role} refuses 2 MiB"
        );
        tx.set(role.as_bytes(), &value[..1 << 19])
            .expect("half a MiB fits");
        tx.commit().await.expect("a commit");
    }
    close(owner, client, &base).await;
}

/// How much an entry may hold is an option every process of a database sets alike: a
/// tree that allows another size than the owner's is refused when it opens.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn trees_that_allow_other_entry_sizes_do_not_meet() {
    let base = base("lim8");
    let owner = common::builder(&base).build().await.expect("the owner");
    let other = allowing(&base, 1 << 20).build().await;
    assert!(
        matches!(other, Err(Error::OptionsMismatch)),
        "{:?}",
        other.err()
    );
    owner.close().await.expect("the owner closes");
    let _ = std::fs::remove_dir_all(&base);
}

/// No size at all, and a size no frame can carry, are refused when the tree is opened.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_entry_size_out_of_range_is_refused() {
    let base = base("lim9");
    for bytes in [0, usize::MAX] {
        let opened = allowing(&base, bytes).build().await;
        assert!(
            matches!(opened, Err(Error::InvalidArgument(_))),
            "{bytes}: {:?}",
            opened.err()
        );
    }
    let _ = std::fs::remove_dir_all(&base);
}
