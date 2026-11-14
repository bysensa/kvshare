//! Cursors over ranges and histories in the owner's process and through its socket: the
//! same answers either way, whatever the size of a page. See `common` for how one process
//! plays an owner and a client; `differential.rs` compares cursors with SurrealKV's own.

mod common;

use common::{base, close, owner_and_client};
use kvshare::{Cursor, HistoryOptions, Tree};

/// The ten keys `k00`..`k09` the range tests read.
fn ten_keys() -> Vec<String> {
    (0..10).map(|n| format!("k{n:02}")).collect()
}

/// Commit `k00`..`k09` through `tree`, each with the value `v` and its number.
async fn write_ten_keys(tree: &Tree) {
    let mut tx = tree.begin().await.expect("a transaction");
    for key in ten_keys() {
        let value = key.replace('k', "v");
        tx.set(key.as_bytes(), value.as_bytes()).expect("a write");
    }
    tx.commit().await.expect("a commit");
}

/// One entry of a cursor: the key, the time and the value, `None` for a tombstone.
type Seen = (String, u64, Option<String>);

/// Every entry of `cursor`, from the first forwards or from the last backwards.
async fn walk(cursor: &mut Cursor<'_>, backwards: bool) -> Vec<Seen> {
    let mut valid = if backwards {
        cursor.seek_last().await
    } else {
        cursor.seek_first().await
    }
    .expect("a seek");
    let mut seen = Vec::new();
    while valid {
        let key = cursor.key();
        let value = (!key.is_tombstone())
            .then(|| String::from_utf8_lossy(&cursor.value().expect("a value")).into_owned());
        seen.push((
            String::from_utf8_lossy(key.user_key()).into_owned(),
            key.timestamp(),
            value,
        ));
        valid = if backwards {
            cursor.prev().await
        } else {
            cursor.next().await
        }
        .expect("a move");
    }
    seen
}

/// Every entry of `[start, end)` that a new transaction of `tree` sees, read in pages of
/// `page` entries.
async fn range(tree: &Tree, start: &[u8], end: &[u8], backwards: bool, page: usize) -> Vec<Seen> {
    let tx = tree.begin().await.expect("a transaction");
    let mut cursor = tx.range(start, end).expect("a cursor");
    cursor.set_page_size(page);
    walk(&mut cursor, backwards).await
}

/// Every version of the keys of `[start, end)` that a new transaction of `tree` sees, read
/// in pages of `page` versions.
async fn history(
    tree: &Tree,
    (start, end): (&[u8], &[u8]),
    options: &HistoryOptions,
    page: usize,
) -> Vec<Seen> {
    let tx = tree.begin().await.expect("a transaction");
    let mut cursor = tx
        .history_with_options(start, end, options)
        .expect("a cursor");
    cursor.set_page_size(page);
    walk(&mut cursor, false).await
}

fn keys(seen: &[Seen]) -> Vec<String> {
    seen.iter().map(|(key, ..)| key.clone()).collect()
}

/// Each version as `key=value`, or `key=-` for a tombstone.
fn shown(seen: &[Seen]) -> Vec<String> {
    seen.iter()
        .map(|(key, _, value)| format!("{key}={}", value.as_deref().unwrap_or("-")))
        .collect()
}

/// Forwards and backwards, in pages of one, three and all entries, the owner and the
/// client read the same entries in the same order.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_range_reads_the_same_in_pages_in_the_owner_and_through_the_socket() {
    let base = base("range");
    let (owner, client) = owner_and_client(&base).await;
    write_ten_keys(&client).await;

    for backwards in [false, true] {
        let mut expected = ten_keys();
        if backwards {
            expected.reverse();
        }
        for page in [1, 3, 1000] {
            for (who, tree) in [("owner", &owner), ("client", &client)] {
                let seen = range(tree, b"k", b"l", backwards, page).await;
                assert_eq!(keys(&seen), expected, "{who}, {backwards}, page {page}");
                assert!(
                    seen.iter().all(|(key, at, value)| *at == 0
                        && value.as_deref().map(|v| &v[1..]) == Some(&key[1..])),
                    "{who}: each value goes with its key, and a range reports no time"
                );
            }
        }
    }
    close(owner, client, &base).await;
}

/// A client's cursor sees the writes its transaction has not sent yet, which go to the
/// owner with its first page; the owner's other transactions see them only after the
/// commit.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_remote_cursor_sees_its_own_writes_which_others_see_after_the_commit() {
    let base = base("own-writes");
    let (owner, client) = owner_and_client(&base).await;
    write_ten_keys(&owner).await;

    let mut tx = client.begin().await.expect("a transaction");
    tx.set(b"k03", b"new").expect("a write");
    tx.delete(b"k05").expect("a delete");
    tx.set(b"k10", b"v10").expect("a write");
    let seen = {
        let mut cursor = tx.range(b"k", b"l").expect("a cursor");
        cursor.set_page_size(3);
        walk(&mut cursor, false).await
    };
    let mut expected = ten_keys();
    expected.retain(|key| key != "k05");
    expected.push("k10".into());
    assert_eq!(keys(&seen), expected);
    assert_eq!(seen[3], ("k03".into(), 0, Some("new".into())));

    // The writes went to the owner with the first page: point reads now ask the owner,
    // whose transaction has them.
    assert_eq!(tx.get(b"k05").await.expect("a get"), None);
    let others = range(&owner, b"k", b"l", false, 1000).await;
    assert_eq!(
        keys(&others),
        ten_keys(),
        "no other transaction sees them yet"
    );

    tx.commit().await.expect("a commit");
    let after = range(&owner, b"k", b"l", false, 1000).await;
    assert_eq!(after, seen);
    close(owner, client, &base).await;
}

/// The pages of one cursor read one snapshot: what the owner commits into the part of the
/// range a client has not read yet stays out of the client's cursor.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_paged_remote_cursor_keeps_its_snapshot() {
    let base = base("snapshot");
    let (owner, client) = owner_and_client(&base).await;
    write_ten_keys(&owner).await;

    let tx = client.begin().await.expect("a transaction");
    let mut cursor = tx.range(b"k", b"l").expect("a cursor");
    cursor.set_page_size(2);
    let mut seen = Vec::new();
    let mut valid = cursor.seek_first().await.expect("a seek");
    seen.push(String::from_utf8_lossy(cursor.key().user_key()).into_owned());
    let mut later = owner.begin().await.expect("a transaction");
    later.set(b"k055", b"late").expect("a write");
    later.delete(b"k08").expect("a delete");
    later.commit().await.expect("a commit");
    while valid {
        valid = cursor.next().await.expect("a move");
        if valid {
            seen.push(String::from_utf8_lossy(cursor.key().user_key()).into_owned());
        }
    }
    assert_eq!(seen, ten_keys());
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
        let mut tx = client.begin().await.expect("a transaction");
        tx.set(key.as_bytes(), value.as_bytes()).expect("a write");
        tx.commit().await.expect("a commit");
    }
    let mut tx = client.begin().await.expect("a transaction");
    tx.soft_delete(b"h1").expect("a soft delete");
    tx.commit().await.expect("a commit");

    let range = (&b"h"[..], &b"i"[..]);
    let tombstones = HistoryOptions::new().with_tombstones(true);
    let all = history(&owner, range, &tombstones, 1000).await;
    assert_eq!(
        shown(&all),
        ["h1=-", "h1=3", "h1=2", "h1=1", "h2=b", "h2=a"]
    );
    for pair in all.windows(2).filter(|pair| pair[0].0 == pair[1].0) {
        assert!(pair[0].1 > pair[1].1, "newest first: {pair:?}");
    }
    let values = history(&owner, range, &HistoryOptions::new(), 1000).await;
    assert_eq!(shown(&values), ["h1=3", "h1=2", "h1=1", "h2=b", "h2=a"]);
    // From `h2=a` to `h1=3`: they and `h1=2`, which was written between them.
    let window = HistoryOptions::new().with_ts_range(all[5].1, all[1].1);
    let between = history(&owner, range, &window, 1000).await;
    assert_eq!(shown(&between), ["h1=3", "h1=2", "h2=a"]);

    for page in [1, 2, 4, 1000] {
        for (who, tree) in [("owner", &owner), ("client", &client)] {
            let read = history(tree, range, &tombstones, page).await;
            assert_eq!(read, all, "{who}, page {page}, with tombstones");
            let read = history(tree, range, &window, page).await;
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
        let mut tx = client.begin().await.expect("a transaction");
        tx.set(b"x", value.as_bytes()).expect("a write");
        tx.commit().await.expect("a commit");
    }
    let mut tx = client.begin().await.expect("a transaction");
    tx.delete(b"x").expect("a delete");
    tx.commit().await.expect("a commit");

    for tree in [&owner, &client] {
        let read = history(tree, (b"x", b"y"), &HistoryOptions::new(), 1000).await;
        assert!(read.is_empty());
    }
    close(owner, client, &base).await;
}

/// A client's history sees the write its transaction has not sent yet, at the time
/// SurrealKV gives a write that has no time until it commits.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_remote_history_sees_its_own_pending_write() {
    let base = base("pending-history");
    let (owner, client) = owner_and_client(&base).await;
    let mut tx = owner.begin().await.expect("a transaction");
    tx.set(b"h", b"old").expect("a write");
    tx.commit().await.expect("a commit");

    let mut tx = client.begin().await.expect("a transaction");
    tx.set(b"h", b"new").expect("a write");
    let versions = {
        let mut cursor = tx.history(b"h", b"i").expect("a cursor");
        walk(&mut cursor, false).await
    };
    assert_eq!(shown(&versions), ["h=new", "h=old"]);
    assert_eq!(versions[0].1, u64::MAX);
    tx.rollback();

    let after = history(&owner, (b"h", b"i"), &HistoryOptions::new(), 1000).await;
    assert_eq!(shown(&after), ["h=old"], "the rolled back write is gone");
    close(owner, client, &base).await;
}

/// A range whose start lies past its end holds no key, and a cursor over it, or over its
/// history, finds nothing, in the owner and through the socket, even with writes
/// pending, where SurrealKV itself panics.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_range_with_its_bounds_reversed_is_empty() {
    let base = base("reversed");
    let (owner, client) = owner_and_client(&base).await;
    write_ten_keys(&owner).await;

    for (who, tree) in [("owner", &owner), ("client", &client)] {
        let mut tx = tree.begin().await.expect("a transaction");
        tx.set(b"k05", b"pending").expect("a write");
        let mut cursor = tx.range(b"l", b"k").expect("a cursor");
        assert!(walk(&mut cursor, false).await.is_empty(), "{who}");
        assert!(walk(&mut cursor, true).await.is_empty(), "{who}");
        let mut cursor = tx.history(b"l", b"k").expect("a cursor");
        assert!(walk(&mut cursor, false).await.is_empty(), "{who}, history");
        assert_eq!(
            tx.get(b"k05")
                .await
                .expect("the transaction still works")
                .as_deref(),
            Some(&b"pending"[..]),
            "{who}"
        );
    }
    close(owner, client, &base).await;
}

/// A history of a tree that keeps no versions is refused when the cursor opens, as
/// SurrealKV refuses it, in the owner and through the socket.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_history_without_versions_is_refused_as_surrealkv_refuses_it() {
    let base = base("unversioned");
    let plain = surrealkv::TreeBuilder::new()
        .with_path(base.join("p"))
        .build()
        .expect("SurrealKV opens");
    let expected = {
        let tx = plain.begin().expect("a transaction");
        let refusal = tx.history(b"a", b"z").err().expect("refused");
        format!("{:?}", kvshare::Error::from(refusal))
    };
    plain.close().await.expect("SurrealKV closes");

    let shared = base.join("s");
    let builder = || {
        kvshare::TreeBuilder::new()
            .with_path(shared.join("db"))
            .with_run_dir(shared.join("run"))
    };
    let owner = builder().build().await.expect("the owner");
    let client = builder().build().await.expect("the client");
    for (who, tree) in [("owner", &owner), ("client", &client)] {
        let tx = tree.begin().await.expect("a transaction");
        let refusal = tx.history(b"a", b"z").err().expect("refused");
        assert_eq!(format!("{refusal:?}"), expected, "{who}");
    }
    close(owner, client, &shared).await;
    let _ = std::fs::remove_dir_all(&base);
}

/// A cursor's moves can be awaited in a spawned task: what it holds crosses threads.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_cursor_moves_in_a_spawned_task() {
    let base = base("spawned");
    let (owner, client) = owner_and_client(&base).await;
    write_ten_keys(&owner).await;

    let mut tasks = tokio::task::JoinSet::new();
    for tree in [owner.clone(), client.clone()] {
        tasks.spawn(async move {
            let tx = tree.begin().await.expect("a transaction");
            let mut cursor = tx.range(b"k", b"l").expect("a cursor");
            cursor.set_page_size(3);
            let mut count = 0;
            let mut valid = cursor.seek_first().await.expect("a seek");
            while valid {
                count += 1;
                tokio::task::yield_now().await;
                valid = cursor.next().await.expect("a move");
            }
            count
        });
    }
    while let Some(count) = tasks.join_next().await {
        assert_eq!(count.expect("a task ends"), 10);
    }
    close(owner, client, &base).await;
}
