//! The election, the hand-over and point reads and writes, within one process: see
//! `common` for how one process plays an owner and a client.

mod common;

use std::sync::Arc;
use std::time::Duration;

use common::{base, builder, close, owner_and_client};
use kvshare::{CompactRangeOptions, Error};

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

/// How many commits a second `tasks` tasks of `tree` make, each committing `each` small
/// transactions that wait for the disk.
async fn commits_a_second(tree: &kvshare::Tree, tasks: usize, each: usize, tag: &str) -> f64 {
    let started = std::time::Instant::now();
    let mut running = Vec::new();
    for task in 0..tasks {
        let (tree, tag) = (tree.clone(), tag.to_string());
        running.push(tokio::spawn(async move {
            for n in 0..each {
                let mut tx = tree
                    .begin()
                    .await
                    .expect("a transaction")
                    .with_durability(kvshare::Durability::Immediate);
                tx.set(format!("{tag}-{task}-{n}").into_bytes(), b"v")
                    .expect("a write");
                tx.commit().await.expect("a commit");
            }
        }));
    }
    for task in running {
        task.await.expect("a task ends");
    }
    (tasks * each) as f64 / started.elapsed().as_secs_f64()
}

/// The tasks of one client commit together: a transaction that begins does not wait
/// for the commits of other tasks that are still being waited for, so SurrealKV writes
/// them to the disk in one group, as it does for the tasks of the owner. Eight tasks
/// then commit well over one and a half times as much as one.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_tasks_of_one_client_commit_together() {
    let base = base("together");
    let (owner, client) = owner_and_client(&base).await;
    let one = commits_a_second(&client, 1, 50, "one").await;
    let eight = commits_a_second(&client, 8, 50, "eight").await;
    println!("commits a second through one connection: one task {one:.0}, eight tasks {eight:.0}");
    assert!(
        eight > one * 1.5,
        "eight tasks of a client commit {eight:.0} a second, one task {one:.0}"
    );
    close(owner, client, &base).await;
}

/// An owner that leaves goes on serving while it writes its memtable out, which is most
/// of the time its leaving takes: the commits its client makes meanwhile are answered,
/// and the next owner finds every one of them. Only then does it hang up. An owner that
/// hung up first left its clients, and those waiting to become clients, nothing but
/// another election.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_owner_that_leaves_serves_while_it_writes_its_memtable_out() {
    let base = base("leaving");
    let (owner, client) = owner_and_client(&base).await;
    // What makes the leaving take a while: 24 MiB that are still in the memtable.
    let value = vec![9u8; 1 << 20];
    for n in 0..24 {
        let mut tx = owner.begin().await.expect("a transaction");
        tx.set(format!("fill{n:02}").into_bytes(), &value)
            .expect("a write");
        tx.commit().await.expect("a commit");
    }

    // The client commits small transactions one after another until its owner is gone.
    let committing = tokio::spawn({
        let client = client.clone();
        async move {
            let mut answered = Vec::new();
            for n in 0u32.. {
                let Ok(mut tx) = client.begin().await else {
                    break;
                };
                if tx.is_local() || tx.set(format!("c{n:06}").into_bytes(), b"x").is_err() {
                    break;
                }
                match tx.commit().await {
                    Ok(()) => answered.push((n, std::time::Instant::now())),
                    Err(_) => break,
                }
            }
            answered
        }
    });
    tokio::time::sleep(Duration::from_millis(50)).await;
    let leaving = std::time::Instant::now();
    owner.close().await.expect("the owner leaves");
    let answered = committing.await.expect("the client's task ends");
    let meanwhile = answered.iter().filter(|(_, at)| *at > leaving).count();
    println!("commits the leaving owner answered: {meanwhile}");
    assert!(
        meanwhile >= 20,
        "the owner answered {meanwhile} commits after it began to leave"
    );

    let tx = loop {
        match client.begin().await {
            Ok(tx) if tx.is_local() => break tx,
            _ => tokio::task::yield_now().await,
        }
    };
    for (n, _) in &answered {
        assert!(
            tx.get(format!("c{n:06}").into_bytes())
                .await
                .expect("a read")
                .is_some(),
            "commit {n}, which the leaving owner answered, is lost"
        );
    }
    drop(tx);
    client.close().await.expect("the client closes");
    let _ = std::fs::remove_dir_all(&base);
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

/// An owner that leaves while it compacts the database waits for the compaction before
/// it lets the database go. SurrealKV looks for a close only between the steps of a
/// compaction and releases its lock in the middle of one: the next owner, opening the
/// database at once, would have its files changed under it by the step that goes on,
/// and the database could not be opened again. After both owners every key reads.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_owner_that_leaves_while_it_compacts_waits_for_the_compaction() {
    // About 1 MiB a batch, a fifth of it in the value log. Six tables of 8 MiB, each
    // flushed by a close, give the compaction a merge that lasts longer than the owner
    // takes to leave; nothing is left to flush, so the merge starts at once. (A close
    // during the flush that a compaction starts with is safe: the compaction looks for
    // it before its first merge.)
    const PER_BATCH: u64 = 700;
    let key = |n: u64| format!("k/{n:010}").into_bytes();
    let value = |n: u64| {
        let size = if n.is_multiple_of(5) {
            2048 + (n * 37 % 6144)
        } else {
            100 + (n * 13 % 800)
        };
        vec![(n % 251) as u8; size as usize]
    };
    let base = base("compactclose");
    let open = || builder(&base).with_enable_vlog(true).build();
    let mut written = 0;
    let mut write = async |tree: &kvshare::Tree, batches: u64| {
        for _ in 0..batches {
            let mut tx = tree.begin().await.expect("a transaction");
            for _ in 0..PER_BATCH {
                tx.set(key(written), value(written)).expect("a write");
                written += 1;
            }
            tx.commit().await.expect("a commit");
        }
    };
    for _ in 0..6 {
        let tree = open().await.expect("a tree");
        write(&tree, 8).await;
        tree.close().await.expect("the tree closes");
    }
    let owner = open().await.expect("the owner");
    let client = open().await.expect("the client");

    let compacting = {
        let owner = owner.clone();
        let options = CompactRangeOptions {
            flush_memtable: false,
            ..CompactRangeOptions::default()
        };
        tokio::spawn(async move { owner.compact_range(None, None, &options).await })
    };
    tokio::time::sleep(Duration::from_millis(20)).await;
    owner.close().await.expect("the owner closes");
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert!(
        compacting.is_finished(),
        "the owner let the database go while it compacted"
    );
    let _ = compacting.await;

    // The client takes the database over at once, writes and leaves.
    let mut tx = loop {
        match client.begin().await {
            Ok(tx) => match tx.get(b"k/").await {
                Ok(_) => break tx,
                Err(Error::OwnerLost(_)) => continue,
                Err(other) => panic!("the client could not take over: {other}"),
            },
            Err(Error::OwnerLost(_)) => continue,
            Err(other) => panic!("the client could not take over: {other}"),
        }
    };
    assert!(tx.is_local(), "the client became the owner");
    for _ in 0..PER_BATCH {
        tx.set(key(written), value(written)).expect("a write");
        written += 1;
    }
    tx.commit().await.expect("a commit");
    client.close().await.expect("the client closes");

    let reopened = open().await.expect("the database opens again");
    let tx = reopened.begin().await.expect("a transaction");
    let mut cursor = tx
        .range(b"k/".as_slice(), b"k0".as_slice())
        .expect("a cursor");
    let mut valid = cursor.seek_first().await.expect("a seek");
    let mut keys = 0;
    while valid {
        let n = keys;
        assert_eq!(cursor.key().user_key(), key(n).as_slice(), "key {n}");
        assert_eq!(cursor.value().expect("a value"), value(n), "value {n}");
        keys += 1;
        valid = cursor.next().await.expect("a move");
    }
    assert_eq!(keys, written, "every key written is there");
    drop(cursor);
    drop(tx);
    reopened.close().await.expect("the tree closes");
    let _ = std::fs::remove_dir_all(&base);
}

/// A process whose options change what is stored (here, whether versions are kept)
/// cannot work through an owner that set them otherwise.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_client_with_other_options_is_refused() {
    let base = base("options");
    let owner = builder(&base).build().await.expect("the owner");
    let other = builder(&base).with_versioning(false, 0).build().await;
    match other {
        Err(Error::OptionsMismatch) => {}
        Err(error) => panic!("other options should be refused, got {error}"),
        Ok(_) => panic!("other options should be refused"),
    }
    owner.close().await.expect("the owner closes");
    let _ = std::fs::remove_dir_all(&base);
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
