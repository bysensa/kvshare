//! Who owns the database: the election, and the role a process holds after it.
//!
//! The election is SurrealKV's own lock. A process opens the database: if it can, it
//! is the owner and starts serving; if the database is locked, it connects to the
//! owner's socket. A socket that is missing or refuses means the owner is starting or
//! has just died, and one that hangs up before the welcome means it is going away: the
//! process waits a little and opens again. A stopped owner is waited for: its welcome
//! may take what is left of the election timeout. A client that loses its owner forgets
//! it, and its next transaction runs the election again.

use std::hash::{BuildHasher, RandomState};
use std::io;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use surrealkv::{Options, Tree, TreeBuilder};
use tokio::net::UnixStream;

use crate::api::Inner;
use crate::client::Connection;
use crate::endpoint::Endpoint;
use crate::error::{Error, Result};
use crate::owner::Owner;
use crate::protocol::VERSION;

/// This process's part in a shared database.
pub(crate) enum Role {
    Owner(Owner),
    Client {
        connection: Arc<Connection>,
        owner_pid: u32,
    },
}

/// The role this process holds now; `generation` tells a role from the one after it.
pub(crate) struct Current {
    pub(crate) generation: u64,
    pub(crate) role: Role,
}

impl Inner {
    /// The role held now, after an election if none is.
    pub(crate) async fn held<'c>(&self, current: &'c mut Option<Current>) -> Result<&'c Current> {
        if current.is_none() {
            let role = self.elect().await?;
            *current = Some(Current {
                generation: self.generations.fetch_add(1, Ordering::Relaxed) + 1,
                role,
            });
        }
        Ok(current.as_ref().expect("a role was just elected"))
    }

    /// Drop the role of `generation`, unless a later one has replaced it.
    pub(crate) async fn forget(&self, generation: u64) {
        let mut current = self.current.lock().await;
        if current
            .as_ref()
            .is_some_and(|held| held.generation == generation)
        {
            *current = None;
        }
    }

    /// Open the database and own it, or connect to the process that does.
    async fn elect(&self) -> Result<Role> {
        let started = Instant::now();
        loop {
            match open(&self.options).await {
                Ok(tree) => {
                    let owner =
                        Owner::start(tree, &self.endpoint, self.fingerprint).map_err(Error::Io)?;
                    return Ok(Role::Owner(owner));
                }
                Err(error) if is_locked(&error) => {
                    // The welcome may take what is left of the election: a stopped owner
                    // answers once it resumes.
                    let left = self.elect_within.saturating_sub(started.elapsed());
                    match connect(&self.endpoint, self.fingerprint, left).await {
                        Ok((connection, owner_pid)) => {
                            return Ok(Role::Client {
                                connection,
                                owner_pid,
                            });
                        }
                        // The owner is on its way in or out: open again after a pause.
                        Err(Error::Io(error)) if passing(&error) => {}
                        Err(other) => return Err(other),
                    }
                }
                Err(error) => return Err(error.into()),
            }
            if started.elapsed() >= self.elect_within {
                return Err(Error::NoOwner {
                    waited: started.elapsed(),
                });
            }
            tokio::time::sleep(jitter(Duration::from_millis(5), Duration::from_millis(50))).await;
        }
    }
}

/// Connect to the owner's socket of `endpoint` and check its welcome against this
/// process: the protocol, the database and the `fingerprint` of the options that must
/// agree. The connection and the owner's pid. The connection and the welcome together
/// take at most `within`: a connect the system never answers was seen on macOS, the
/// owner gone, and it must not hold up an election for ever.
///
/// An owner that is on its way in or out, see [`passing`], is an `Error::Io` of that
/// kind; one that answers what this process cannot take is a `Handshake` or a mismatch.
pub(crate) async fn connect(
    endpoint: &Endpoint,
    fingerprint: [u8; 32],
    within: Duration,
) -> Result<(Arc<Connection>, u32)> {
    let deadline = tokio::time::Instant::now() + within;
    let stream = tokio::time::timeout_at(deadline, UnixStream::connect(&endpoint.socket))
        .await
        .map_err(|_| {
            Error::Io(io::Error::new(
                io::ErrorKind::TimedOut,
                "no connection in time",
            ))
        })?
        .map_err(Error::Io)?;
    let left = deadline.saturating_duration_since(tokio::time::Instant::now());
    let (connection, welcome) = Connection::open(stream, left).await.map_err(|error| {
        if passing(&error) {
            Error::Io(error)
        } else {
            Error::Handshake(error.to_string())
        }
    })?;
    if welcome.version != VERSION {
        return Err(Error::VersionMismatch {
            owner: welcome.version,
            this: VERSION,
        });
    }
    if welcome.db != endpoint.db {
        return Err(Error::Handshake(format!(
            "the owner holds {}, not {}",
            welcome.db.display(),
            endpoint.db.display()
        )));
    }
    if welcome.fingerprint != fingerprint {
        return Err(Error::OptionsMismatch);
    }
    Ok((Arc::new(connection), welcome.owner_pid))
}

/// Open the tree with `options`, off the async threads: opening reads and may recover
/// files.
async fn open(options: &Options) -> surrealkv::Result<Tree> {
    let options = options.clone();
    tokio::task::spawn_blocking(move || TreeBuilder::with_options(options).build())
        .await
        .expect("opening the tree does not panic")
}

/// Whether connecting to the owner failed because the owner is on its way in or out,
/// which an election waits out: it holds the lock but has not bound its socket yet, or
/// has just died and left the file (`NotFound`, `ConnectionRefused`); it is stopped and
/// silent at its welcome (`TimedOut`); it is closing the database or being killed and
/// hangs up before its welcome (`UnexpectedEof`, `ConnectionReset`, `BrokenPipe`).
fn passing(error: &io::Error) -> bool {
    matches!(
        error.kind(),
        io::ErrorKind::NotFound
            | io::ErrorKind::ConnectionRefused
            | io::ErrorKind::TimedOut
            | io::ErrorKind::UnexpectedEof
            | io::ErrorKind::ConnectionReset
            | io::ErrorKind::BrokenPipe
    )
}

/// Whether opening failed because another process holds the database.
///
/// SurrealKV 1.4 answers with `Error::Other("... already locked by another process")`
/// on the operating system's files; `is_database_locked` is true only for the
/// browser's file system. The text is the only sign there is.
fn is_locked(error: &surrealkv::Error) -> bool {
    error.is_database_locked()
        || matches!(error, surrealkv::Error::Other(message) if message.contains("already locked"))
}

/// A pause between `low` and `high`, different in every process, so that processes
/// that lost the same owner do not open the database in step.
fn jitter(low: Duration, high: Duration) -> Duration {
    let spread = (high - low).as_nanos() as u64;
    let draw = RandomState::new().hash_one(Instant::now());
    low + Duration::from_nanos(draw % spread.max(1))
}

#[cfg(test)]
mod tests {
    use tokio::net::UnixListener;

    use super::*;
    use crate::protocol::{Welcome, write_frame};

    /// A client refuses an owner that speaks another version of the protocol, or holds
    /// another database, or set the options that must agree otherwise, before its
    /// first request.
    #[tokio::test]
    async fn a_client_refuses_an_owner_it_cannot_understand() {
        let base = std::env::temp_dir().join(format!("kvo-welcome-{}", std::process::id()));
        let endpoint = Endpoint::new(&base.join("db"), &base.join("run")).expect("an endpoint");
        std::fs::create_dir_all(base.join("run")).expect("the run directory");
        let fingerprint = [7u8; 32];
        let welcomes = [
            Welcome {
                version: VERSION + 1,
                db: endpoint.db.clone(),
                owner_pid: 1,
                fingerprint,
            },
            Welcome {
                version: VERSION,
                db: base.join("another"),
                owner_pid: 1,
                fingerprint,
            },
            Welcome {
                version: VERSION,
                db: endpoint.db.clone(),
                owner_pid: 1,
                fingerprint: [8u8; 32],
            },
        ];
        for welcome in welcomes {
            let _ = std::fs::remove_file(&endpoint.socket);
            let listener = UnixListener::bind(&endpoint.socket).expect("a fake owner");
            let fake = tokio::spawn(async move {
                let (mut stream, _) = listener.accept().await.expect("the client");
                write_frame(&mut stream, &welcome).await.expect("a welcome");
                // Keep the connection open until the client has read the welcome.
                tokio::time::sleep(Duration::from_millis(200)).await;
            });
            let refusal = connect(&endpoint, fingerprint, Duration::from_secs(1))
                .await
                .err()
                .expect("refused");
            match refusal {
                Error::VersionMismatch { owner, this } => {
                    assert_eq!((owner, this), (VERSION + 1, VERSION));
                }
                Error::Handshake(reason) => assert!(reason.contains("another"), "{reason}"),
                Error::OptionsMismatch => {}
                other => panic!("an unexpected refusal: {other}"),
            }
            fake.await.expect("the fake owner ends");
        }
        let _ = std::fs::remove_dir_all(&base);
    }

    /// An owner that goes away while a process connects, as one that closes the
    /// database or is killed does, hangs up before its welcome. The process elects
    /// again instead of giving up, and owns the database once the lock is free.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn an_owner_that_hangs_up_before_its_welcome_is_elected_again() {
        let base = std::env::temp_dir().join(format!("kvo-hangup-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&base);
        let (db, run) = (base.join("db"), base.join("run"));
        std::fs::create_dir_all(&run).expect("the run directory");
        // A tree opened apart holds the lock, as an owner would.
        let held = TreeBuilder::new()
            .with_path(db.clone())
            .build()
            .expect("the lock");
        let endpoint = Endpoint::new(&db, &run).expect("an endpoint");
        let listener = UnixListener::bind(&endpoint.socket).expect("an owner's socket");
        let going = tokio::spawn(async move {
            // Hang up on every connection for a while, then let the database go.
            let until = Instant::now() + Duration::from_millis(300);
            while Instant::now() < until {
                if let Ok(Ok((stream, _))) =
                    tokio::time::timeout(Duration::from_millis(50), listener.accept()).await
                {
                    drop(stream);
                }
            }
            drop(listener);
            held.close().await.expect("the lock is let go");
        });
        let tree = crate::TreeBuilder::new()
            .with_path(db.clone())
            .with_run_dir(run.clone())
            .with_election_timeout(Duration::from_secs(5))
            .build()
            .await
            .expect("elected once the lock is free");
        assert_eq!(
            tree.owner_pid().await.expect("the owner"),
            std::process::id(),
            "this process owns the database"
        );
        going.await.expect("the hanging owner ends");
        tree.close().await.expect("the tree closes");
        let _ = std::fs::remove_dir_all(&base);
    }
}
