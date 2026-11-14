//! What the tests share: one database with an owner and a client of it, in one process.
//!
//! SurrealKV refuses a second tree on a directory that a tree of the same process holds,
//! with the same "already locked" error a second process gets, so two [`Shared`] values
//! in one test play an owner and a client.

use std::path::{Path, PathBuf};
use std::time::Duration;

use kvshare::protocol::Durability;
use kvshare::{Options, Shared};

/// A fresh directory for the test `name`.
pub fn base(name: &str) -> PathBuf {
    let base = std::env::temp_dir().join(format!("kvo-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&base);
    base
}

/// Sockets under `base`, and a short election, so that a test that fails fails soon.
pub fn options(base: &Path) -> Options {
    let mut options = Options::new(base.join("run"));
    options.elect_within = Duration::from_secs(3);
    options
}

/// An owner and a client of one database in `base`, the owner already elected.
pub async fn owner_and_client(base: &Path) -> (Shared, Shared) {
    let db = base.join("db");
    let owner = Shared::new(&db, options(base)).expect("the owner");
    let client = Shared::new(&db, options(base)).expect("the client");
    owner
        .begin(Durability::Eventual)
        .await
        .expect("the owner opens")
        .rollback()
        .await
        .expect("a rollback");
    (owner, client)
}

/// Close the client, then the owner, and remove `base`.
pub async fn close(owner: Shared, client: Shared, base: &Path) {
    client.close().await.expect("the client closes");
    owner.close().await.expect("the owner closes");
    let _ = std::fs::remove_dir_all(base);
}
