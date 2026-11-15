//! Where the processes of one database meet: the owner leaves the path of its socket in
//! the database's directory, so no two processes have to agree on a directory for it.

mod common;

use std::os::unix::fs::{MetadataExt as _, PermissionsExt as _};
use std::path::PathBuf;

use common::{base, builder};
use kvshare::TreeBuilder;

/// Trees that name different run directories, as processes with different environments
/// do, meet all the same: the second finds the owner's socket through the database's
/// directory. When the owner leaves, the next one serves from its own directory.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn trees_with_different_run_directories_meet() {
    let base = base("meet");
    let first = builder(&base).build().await.expect("the owner");
    let mut tx = first.begin().await.expect("a transaction");
    tx.set(b"k", b"v").expect("a write");
    tx.commit().await.expect("a commit");

    let second = builder(&base)
        .with_run_dir(base.join("elsewhere"))
        .build()
        .await
        .expect("a tree with another run directory finds the owner");
    assert!(!second.is_owner().await.expect("a role"));
    let tx = second.begin().await.expect("a transaction");
    assert_eq!(tx.get(b"k").await.expect("a read").as_deref(), Some(&b"v"[..]));
    drop(tx);

    first.close().await.expect("the owner leaves");
    let tx = loop {
        // The first requests may still find the old owner gone.
        match second.begin().await {
            Ok(tx) if tx.is_local() => break tx,
            _ => tokio::task::yield_now().await,
        }
    };
    drop(tx);
    let third = builder(&base).build().await.expect("a third tree");
    assert!(
        !third.is_owner().await.expect("a role"),
        "the third finds the new owner in the other run directory"
    );
    third.close().await.expect("the third closes");
    second.close().await.expect("the second closes");
    let _ = std::fs::remove_dir_all(&base);
}

/// A run directory the caller names is the caller's: an owner leaves its permissions
/// as they are.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_named_run_directory_keeps_its_permissions() {
    let base = base("perm");
    let run = base.join("run");
    std::fs::create_dir_all(&run).expect("the run directory");
    std::fs::set_permissions(&run, std::fs::Permissions::from_mode(0o755)).expect("its mode");
    let tree = builder(&base).build().await.expect("the owner");
    let mode = std::fs::metadata(&run).expect("the run directory").mode() & 0o777;
    assert_eq!(mode, 0o755, "the run directory's mode is {mode:o}");
    tree.close().await.expect("the tree closes");
    let _ = std::fs::remove_dir_all(&base);
}

/// With no run directory named, an owner serves from a directory of its user's own:
/// made for the user only, under the user's runtime directory or named by the user's
/// id in the temporary directory.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_default_run_directory_is_the_users_own() {
    let base = base("deflt");
    let db = base.join("db");
    let tree = TreeBuilder::new()
        .with_path(db.clone())
        .build()
        .await
        .expect("the owner");
    let socket = PathBuf::from(
        String::from_utf8(std::fs::read(db.join("SOCKET")).expect("the owner's socket path"))
            .expect("a path in UTF-8"),
    );
    let run = socket.parent().expect("the socket's directory");
    let made = std::fs::metadata(run).expect("the run directory");
    let uid = std::fs::metadata(&db).expect("the database").uid();
    assert_eq!(made.uid(), uid, "the directory is the user's");
    assert_eq!(made.mode() & 0o777, 0o700, "and for the user only");
    let name = run.file_name().expect("a name").to_string_lossy();
    assert!(
        name == "kvshare" || name == format!("kvshare-{uid}"),
        "the default run directory is {}",
        run.display()
    );
    tree.close().await.expect("the tree closes");
    let _ = std::fs::remove_dir_all(&base);
}
