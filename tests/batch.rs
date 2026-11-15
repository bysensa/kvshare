//! `Transaction::get_many_for_update`: many keys read for update at once, which a
//! client asks its owner for in one request instead of one a key.

mod common;

use std::time::Instant;

use common::{base, close, owner_and_client};
use kvshare::{Error, Tree};

/// The keys `b0000` and on, `count` of them.
fn keys(count: u32) -> Vec<Vec<u8>> {
    (0..count)
        .map(|n| format!("b{n:04}").into_bytes())
        .collect()
}

/// Write two keys out of three, so that some are missing.
async fn seed(tree: &Tree, keys: &[Vec<u8>]) {
    let mut tx = tree.begin().await.expect("a transaction");
    for (n, key) in keys.iter().enumerate() {
        if n % 3 != 0 {
            tx.set(key, format!("v{n}").into_bytes()).expect("a write");
        }
    }
    tx.commit().await.expect("a commit");
}

/// How a transaction of `tree` ends that reads `keys` for update, at once or one by
/// one, while a transaction of `rival` writes the key at `touched` and commits first.
async fn race(
    tree: &Tree,
    rival: &Tree,
    keys: &[Vec<u8>],
    touched: usize,
    at_once: bool,
) -> String {
    let mut tx = tree.begin().await.expect("a transaction");
    if at_once {
        tx.get_many_for_update(keys).await.expect("a read");
    } else {
        for key in keys {
            tx.get_for_update(key).await.expect("a read");
        }
    }
    let mut other = rival.begin().await.expect("a rival transaction");
    other.set(&keys[touched], b"the rival's").expect("a write");
    other.commit().await.expect("the rival commits first");
    tx.set(b"mine", b"x").expect("a write");
    match tx.commit().await {
        Ok(()) => "committed".to_string(),
        Err(Error::TransactionWriteConflict) => "conflict".to_string(),
        Err(other) => format!("{other:?}"),
    }
}

/// Many keys read for update answer what `get_many` answers, in the order of the keys
/// and with `None` for a missing one, in the owner and through it; more keys than one
/// request asks for. With nobody else writing, the commit goes through.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn many_keys_read_for_update_answer_as_get_many() {
    let base = base("bat1");
    let (owner, client) = owner_and_client(&base).await;
    let keys = keys(600);
    seed(&owner, &keys).await;
    for (role, tree) in [("owner", &owner), ("client", &client)] {
        let mut tx = tree.begin().await.expect("a transaction");
        let expected = tx.get_many(&keys).await.expect("a read");
        assert_eq!(expected.iter().filter(|value| value.is_none()).count(), 200);
        let read = tx
            .get_many_for_update(&keys)
            .await
            .expect("a read for update");
        assert_eq!(read, expected, "the {role}");
        tx.set(role.as_bytes(), b"x").expect("a write");
        tx.commit()
            .await
            .expect("nobody else wrote: the commit goes through");
    }
    close(owner, client, &base).await;
}

/// A key read for update among many is checked at the commit as one read alone is: the
/// transaction loses to another that wrote the key meanwhile, wherever the key stands
/// in a list longer than one request, and a key that was missing as well. In the owner
/// and through it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_key_read_for_update_among_many_conflicts_as_one_read_alone() {
    let base = base("bat2");
    let (owner, client) = owner_and_client(&base).await;
    let keys = keys(600);
    seed(&owner, &keys).await;
    for (role, tree, rival) in [("owner", &owner, &client), ("client", &client, &owner)] {
        for touched in [0, 1, 299, 550] {
            let alone = race(tree, rival, &keys, touched, false).await;
            let at_once = race(tree, rival, &keys, touched, true).await;
            assert_eq!(
                alone, "conflict",
                "the {role}, key {touched}, read one by one"
            );
            assert_eq!(at_once, alone, "the {role}, key {touched}, read at once");
        }
    }
    close(owner, client, &base).await;
}

/// An empty key among many is refused as one read alone is, in the owner and through
/// it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_empty_key_among_many_is_refused() {
    let base = base("bat3");
    let (owner, client) = owner_and_client(&base).await;
    for (role, tree) in [("owner", &owner), ("client", &client)] {
        let mut tx = tree.begin().await.expect("a transaction");
        let read = tx.get_many_for_update(&[b"a".to_vec(), Vec::new()]).await;
        assert!(matches!(read, Err(Error::EmptyKey)), "the {role}: {read:?}");
    }
    close(owner, client, &base).await;
}

/// What it is for: a client reads 250 keys for update with one request to its owner,
/// not 250, and so several times sooner.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_client_reads_many_keys_for_update_with_one_request() {
    let base = base("bat4");
    let (owner, client) = owner_and_client(&base).await;
    let keys = keys(250);
    seed(&owner, &keys).await;
    let mut took = [0.0f64; 2];
    for (slot, at_once) in [(0, false), (1, true)] {
        let started = Instant::now();
        for _ in 0..20 {
            let mut tx = client.begin().await.expect("a transaction");
            if at_once {
                tx.get_many_for_update(&keys).await.expect("a read");
            } else {
                for key in &keys {
                    tx.get_for_update(key).await.expect("a read");
                }
            }
        }
        took[slot] = started.elapsed().as_secs_f64() * 1000.0 / 20.0;
    }
    println!(
        "250 keys read for update through the owner: one by one {:.2} ms, at once {:.2} ms",
        took[0], took[1]
    );
    assert!(
        took[1] * 3.0 < took[0],
        "one by one {:.2} ms, at once {:.2} ms",
        took[0],
        took[1]
    );
    close(owner, client, &base).await;
}
