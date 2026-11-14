//! Range and history reads in the owner's process and through its socket: the same
//! answers either way, whatever the size of a page. See `common` for how one process
//! plays an owner and a client.

mod common;

use common::{base, close, owner_and_client};
use kvshare::protocol::Durability;
use kvshare::{Shared, Version};

/// The ten keys `k00`..`k09` the range tests read.
fn ten_keys() -> Vec<String> {
    (0..10).map(|n| format!("k{n:02}")).collect()
}

/// Commit `k00`..`k09` through `shared`, each with the value `v` and its number.
async fn write_ten_keys(shared: &Shared) {
    let mut tx = shared
        .begin(Durability::Eventual)
        .await
        .expect("a transaction");
    for key in ten_keys() {
        let value = key.replace('k', "v");
        tx.set(key.as_bytes(), value.as_bytes())
            .await
            .expect("a write");
    }
    tx.commit().await.expect("a commit");
}

/// Every entry of `[start, end)` that a new transaction of `shared` sees, read in pages
/// of `page` entries.
async fn range(
    shared: &Shared,
    start: &[u8],
    end: &[u8],
    reverse: bool,
    page: usize,
) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut tx = shared
        .begin(Durability::Eventual)
        .await
        .expect("a transaction");
    let mut entries = Vec::new();
    {
        let mut scan = tx.range(start, end).page(page);
        if reverse {
            scan = scan.reverse();
        }
        while let Some(entry) = scan.next().await.expect("a page") {
            entries.push(entry);
        }
    }
    tx.rollback().await.expect("a rollback");
    entries
}

/// Every version of the keys of `[start, end)` that a new transaction of `shared` sees,
/// read in pages of `page` versions, with tombstones or not, between two times or not.
async fn history(
    shared: &Shared,
    (start, end): (&[u8], &[u8]),
    tombstones: bool,
    between: Option<(u64, u64)>,
    page: usize,
) -> Vec<Version> {
    let mut tx = shared
        .begin(Durability::Eventual)
        .await
        .expect("a transaction");
    let mut versions = Vec::new();
    {
        let mut read = tx.history(start, end).page(page);
        if tombstones {
            read = read.with_tombstones();
        }
        if let Some((from, to)) = between {
            read = read.between(from, to);
        }
        while let Some(version) = read.next().await.expect("a page") {
            versions.push(version);
        }
    }
    tx.rollback().await.expect("a rollback");
    versions
}

fn keys(entries: &[(Vec<u8>, Vec<u8>)]) -> Vec<String> {
    entries
        .iter()
        .map(|(key, _)| String::from_utf8_lossy(key).into_owned())
        .collect()
}

/// Each version as `key=value`, or `key=-` for a tombstone.
fn shown(versions: &[Version]) -> Vec<String> {
    versions
        .iter()
        .map(|version| {
            let value = version
                .value
                .as_deref()
                .map_or("-".into(), String::from_utf8_lossy);
            format!("{}={value}", String::from_utf8_lossy(&version.key))
        })
        .collect()
}

/// Forwards and backwards, in pages of one, three and all entries, the owner and the
/// client read the same entries in the same order.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_range_reads_the_same_in_pages_in_the_owner_and_through_the_socket() {
    let base = base("range");
    let (owner, client) = owner_and_client(&base).await;
    write_ten_keys(&client).await;

    for reverse in [false, true] {
        let mut expected = ten_keys();
        if reverse {
            expected.reverse();
        }
        for page in [1, 3, 1000] {
            for (who, shared) in [("owner", &owner), ("client", &client)] {
                let entries = range(shared, b"k", b"l", reverse, page).await;
                assert_eq!(
                    keys(&entries),
                    expected,
                    "{who}, reverse {reverse}, page {page}"
                );
                assert!(
                    entries.iter().all(|(key, value)| key[1..] == value[1..]),
                    "{who}: each value goes with its key"
                );
            }
        }
    }
    close(owner, client, &base).await;
}

/// A client's scan sees the writes its transaction has not sent yet, which go to the
/// owner with it; the owner's other transactions see them only after the commit.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_remote_range_sees_its_own_writes_which_others_see_after_the_commit() {
    let base = base("own-writes");
    let (owner, client) = owner_and_client(&base).await;
    write_ten_keys(&owner).await;

    let mut tx = client
        .begin(Durability::Eventual)
        .await
        .expect("a transaction");
    tx.set(b"k03", b"new").await.expect("a write");
    tx.delete(b"k05").await.expect("a delete");
    tx.set(b"k10", b"v10").await.expect("a write");
    let mut seen = Vec::new();
    {
        let mut scan = tx.range(b"k", b"l").page(3);
        while let Some(entry) = scan.next().await.expect("a page") {
            seen.push(entry);
        }
    }
    let mut expected = ten_keys();
    expected.retain(|key| key != "k05");
    expected.push("k10".into());
    assert_eq!(keys(&seen), expected);
    assert_eq!(seen[3], (b"k03".to_vec(), b"new".to_vec()));

    // The writes went to the owner with the scan: point reads now ask the owner, whose
    // transaction has them.
    assert_eq!(tx.get(b"k05").await.expect("a get"), None);
    assert_eq!(
        tx.get(b"k03").await.expect("a get").as_deref(),
        Some(&b"new"[..])
    );
    let others = range(&owner, b"k", b"l", false, 1000).await;
    assert_eq!(
        keys(&others),
        ten_keys(),
        "no other transaction sees them yet"
    );

    // Nothing is pending any more, and the commit still carries what went with the scan.
    tx.commit().await.expect("a commit");
    let after = range(&owner, b"k", b"l", false, 1000).await;
    assert_eq!(after, seen);
    close(owner, client, &base).await;
}

/// The pages of one scan read one snapshot: what the owner commits into the part of the
/// range a client has not read yet stays out of the client's scan.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_paged_remote_range_keeps_its_snapshot() {
    let base = base("snapshot");
    let (owner, client) = owner_and_client(&base).await;
    write_ten_keys(&owner).await;

    let mut tx = client
        .begin(Durability::Eventual)
        .await
        .expect("a transaction");
    let mut seen = Vec::new();
    {
        let mut scan = tx.range(b"k", b"l").page(2);
        seen.push(scan.next().await.expect("a page").expect("an entry"));
        let mut later = owner
            .begin(Durability::Eventual)
            .await
            .expect("a transaction");
        later.set(b"k055", b"late").await.expect("a write");
        later.delete(b"k08").await.expect("a delete");
        later.commit().await.expect("a commit");
        while let Some(entry) = scan.next().await.expect("a page") {
            seen.push(entry);
        }
    }
    tx.rollback().await.expect("a rollback");
    assert_eq!(keys(&seen), ten_keys());
    close(owner, client, &base).await;
}

/// Versions come key by key, newest first; tombstones only on request; a time window
/// keeps what was written in it; pages of any size give the same, and the owner and the
/// client read the same versions with the same times.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_history_reads_the_same_in_pages_in_the_owner_and_through_the_socket() {
    let base = base("history");
    let (owner, client) = owner_and_client(&base).await;
    // Through the socket: `h1` three times and then a tombstone, `h2` twice.
    for (key, value) in [
        ("h1", "1"),
        ("h2", "a"),
        ("h1", "2"),
        ("h1", "3"),
        ("h2", "b"),
    ] {
        let mut tx = client
            .begin(Durability::Eventual)
            .await
            .expect("a transaction");
        tx.set(key.as_bytes(), value.as_bytes())
            .await
            .expect("a write");
        tx.commit().await.expect("a commit");
    }
    let mut tx = client
        .begin(Durability::Eventual)
        .await
        .expect("a transaction");
    tx.soft_delete(b"h1").await.expect("a soft delete");
    tx.commit().await.expect("a commit");

    let range = (&b"h"[..], &b"i"[..]);
    let all = history(&owner, range, true, None, 1000).await;
    assert_eq!(
        shown(&all),
        ["h1=-", "h1=3", "h1=2", "h1=1", "h2=b", "h2=a"]
    );
    for pair in all.windows(2).filter(|pair| pair[0].key == pair[1].key) {
        assert!(
            pair[0].timestamp > pair[1].timestamp,
            "newest first: {pair:?}"
        );
    }
    let values = history(&owner, range, false, None, 1000).await;
    assert_eq!(shown(&values), ["h1=3", "h1=2", "h1=1", "h2=b", "h2=a"]);
    // From `h2=a` to `h1=3`: they and `h1=2`, which was written between them.
    let window = Some((all[5].timestamp, all[1].timestamp));
    let between = history(&owner, range, false, window, 1000).await;
    assert_eq!(shown(&between), ["h1=3", "h1=2", "h2=a"]);

    for page in [1, 2, 4, 1000] {
        for (who, shared) in [("owner", &owner), ("client", &client)] {
            let read = history(shared, range, true, None, page).await;
            assert_eq!(read, all, "{who}, page {page}, with tombstones");
            let read = history(shared, range, false, window, page).await;
            assert_eq!(read, between, "{who}, page {page}, between");
        }
    }
    close(owner, client, &base).await;
}

/// A hard delete takes the key's history with it, as SurrealKV's `delete` does.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_hard_delete_takes_the_history_with_it() {
    let base = base("hard-delete");
    let (owner, client) = owner_and_client(&base).await;
    for value in ["1", "2"] {
        let mut tx = client
            .begin(Durability::Eventual)
            .await
            .expect("a transaction");
        tx.set(b"x", value.as_bytes()).await.expect("a write");
        tx.commit().await.expect("a commit");
    }
    let mut tx = client
        .begin(Durability::Eventual)
        .await
        .expect("a transaction");
    tx.delete(b"x").await.expect("a delete");
    tx.commit().await.expect("a commit");

    for shared in [&owner, &client] {
        assert!(
            history(shared, (b"x", b"y"), false, None, 1000)
                .await
                .is_empty()
        );
    }
    close(owner, client, &base).await;
}

/// A client's history read sees the write its transaction has not sent yet, at the time
/// SurrealKV gives a write that has no time until it commits.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_remote_history_sees_its_own_pending_write() {
    let base = base("pending-history");
    let (owner, client) = owner_and_client(&base).await;
    let mut tx = owner
        .begin(Durability::Eventual)
        .await
        .expect("a transaction");
    tx.set(b"h", b"old").await.expect("a write");
    tx.commit().await.expect("a commit");

    let mut tx = client
        .begin(Durability::Eventual)
        .await
        .expect("a transaction");
    tx.set(b"h", b"new").await.expect("a write");
    let mut versions = Vec::new();
    {
        let mut read = tx.history(b"h", b"i");
        while let Some(version) = read.next().await.expect("a page") {
            versions.push(version);
        }
    }
    assert_eq!(shown(&versions), ["h=new", "h=old"]);
    assert_eq!(versions[0].timestamp, u64::MAX);
    tx.rollback().await.expect("a rollback");

    let after = history(&owner, (b"h", b"i"), false, None, 1000).await;
    assert_eq!(shown(&after), ["h=old"], "the rolled back write is gone");
    close(owner, client, &base).await;
}
