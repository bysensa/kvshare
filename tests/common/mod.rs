//! What the tests share: one database with an owner and a client of it, in one process.
//!
//! SurrealKV refuses a second tree on a directory that a tree of the same process holds,
//! with the same "already locked" error a second process gets, so two [`Tree`] values
//! in one test play an owner and a client.

#![allow(dead_code)]

use std::path::{Path, PathBuf};
use std::time::Duration;

use kvshare::{Tree, TreeBuilder};

/// A fresh directory for the test `name`.
pub fn base(name: &str) -> PathBuf {
    let base = std::env::temp_dir().join(format!("kvo-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&base);
    base
}

/// The database in `base`, its sockets under `base`, versions kept, and a short election
/// so that a test that fails fails soon.
pub fn builder(base: &Path) -> TreeBuilder {
    TreeBuilder::new()
        .with_path(base.join("db"))
        .with_versioning(true, 0)
        .with_run_dir(base.join("run"))
        .with_election_timeout(Duration::from_secs(3))
}

/// An owner and a client of one database in `base`: opening elects at once, so the
/// first tree opened owns the database.
pub async fn owner_and_client(base: &Path) -> (Tree, Tree) {
    let owner = builder(base).build().await.expect("the owner");
    let client = builder(base).build().await.expect("the client");
    assert!(
        owner.is_owner().await.expect("a role"),
        "the first tree owns it"
    );
    assert!(
        !client.is_owner().await.expect("a role"),
        "the second is a client"
    );
    (owner, client)
}

/// Close the client, then the owner, and remove `base`.
pub async fn close(owner: Tree, client: Tree, base: &Path) {
    client.close().await.expect("the client closes");
    owner.close().await.expect("the owner closes");
    let _ = std::fs::remove_dir_all(base);
}
