//! Where the processes of one database meet.
//!
//! The owner leaves the path of its socket in the database's directory, in the file
//! `SOCKET`, and a process that finds the database locked reads it there. So no two
//! processes have to agree on a directory for sockets: each one names its own, or takes
//! the default of its own environment, and only the database's path is shared.
//!
//! A socket's path is limited to 104 bytes on macOS and 108 on Linux, which a
//! database's own directory may well exceed; so the socket lies in a run directory,
//! under a name made of a hash of the database's canonical path, and `~/x/../db` and
//! `/Users/me/db` are one database with one socket.

use std::ffi::OsString;
use std::io;
use std::os::unix::ffi::{OsStrExt, OsStringExt};
use std::path::{Path, PathBuf};
use std::sync::OnceLock;

/// The longest socket path both macOS and Linux accept, without the closing NUL.
const MAX_SOCKET_PATH: usize = 103;

/// The file in a database's directory that holds the path of its owner's socket.
const POINTER: &str = "SOCKET";

/// The database, and the socket this process serves it on if it owns it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Endpoint {
    /// The canonical path of the database directory.
    pub db: PathBuf,
    /// `<run_dir>/kv-<16 hex digits of blake3(db)>.sock`.
    pub socket: PathBuf,
}

impl Endpoint {
    /// The endpoint of the database at `db`, with this process's socket in `run_dir`.
    ///
    /// The database directory is created if it is missing: a path is made canonical
    /// only once it exists.
    pub fn new(db: &Path, run_dir: &Path) -> io::Result<Self> {
        std::fs::create_dir_all(db)?;
        let db = std::fs::canonicalize(db)?;
        let hash = blake3::hash(db.as_os_str().as_bytes());
        let socket = run_dir.join(format!("kv-{}.sock", &hash.to_hex()[..16]));
        if socket.as_os_str().len() > MAX_SOCKET_PATH {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "the socket path {} is longer than {MAX_SOCKET_PATH} bytes; choose a shorter run directory",
                    socket.display()
                ),
            ));
        }
        Ok(Endpoint { db, socket })
    }

    /// The file that names the owner's socket.
    pub fn pointer(&self) -> PathBuf {
        self.db.join(POINTER)
    }

    /// Leave this process's socket path in the database's directory, for the processes
    /// that find the database locked. Only the owner calls it, once its socket is
    /// bound; the file is put in place whole, so a reader finds the old path or the
    /// new one.
    pub fn publish(&self) -> io::Result<()> {
        let pointer = self.pointer();
        let draft = pointer.with_extension(format!("{}.tmp", std::process::id()));
        std::fs::write(&draft, self.socket.as_os_str().as_bytes())?;
        std::fs::rename(&draft, &pointer)
    }

    /// The socket the owner of the database serves it on, as the owner left it. A file
    /// that is missing (`NotFound`) means no owner has got that far yet.
    pub fn owner_socket(&self) -> io::Result<PathBuf> {
        let socket = PathBuf::from(OsString::from_vec(std::fs::read(self.pointer())?));
        if !socket.is_absolute() || socket.as_os_str().len() > MAX_SOCKET_PATH {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("{} does not hold a socket's path", self.pointer().display()),
            ));
        }
        Ok(socket)
    }
}

/// The user this process runs as.
///
/// The standard library does not tell; a socket does, of its peer, and both ends of a
/// pair made here are this process. Must be called on a Tokio runtime.
pub fn uid() -> io::Result<u32> {
    static UID: OnceLock<u32> = OnceLock::new();
    if let Some(uid) = UID.get() {
        return Ok(*uid);
    }
    let (end, _other) = tokio::net::UnixStream::pair()?;
    let uid = end.peer_cred()?.uid();
    Ok(*UID.get_or_init(|| uid))
}

/// The directory of this process's sockets when it names none: `kvshare` in the user's
/// runtime directory (`$XDG_RUNTIME_DIR`) where there is one, or else `kvshare-<uid>`
/// in the temporary directory, which other users may share.
pub fn default_run_dir(uid: u32) -> PathBuf {
    match std::env::var_os("XDG_RUNTIME_DIR")
        .map(PathBuf::from)
        .filter(|dir| dir.is_absolute())
    {
        Some(runtime) => runtime.join("kvshare"),
        None => std::env::temp_dir().join(format!("kvshare-{uid}")),
    }
}

/// Refuse a socket whose other end is another user's process: `whose` says which end.
pub fn same_user(stream: &tokio::net::UnixStream, uid: u32, whose: &str) -> io::Result<()> {
    let peer = stream.peer_cred()?.uid();
    if peer != uid {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            format!("{whose} runs as user {peer}, this process as user {uid}"),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Two spellings of one directory meet at one socket; another directory does not.
    #[test]
    fn one_database_has_one_socket_whatever_the_spelling() {
        let base = std::env::temp_dir().join(format!("kvo-endpoint-{}", std::process::id()));
        let run = base.join("run");
        let plain = Endpoint::new(&base.join("db"), &run).expect("an endpoint");
        let detour = Endpoint::new(&base.join("other/../db"), &run).expect("an endpoint");
        let other = Endpoint::new(&base.join("other"), &run).expect("an endpoint");
        assert_eq!(plain, detour);
        assert_ne!(plain.socket, other.socket);
        let _ = std::fs::remove_dir_all(&base);
    }

    /// A run directory that would make the socket path too long is refused up front.
    #[test]
    fn a_socket_path_that_is_too_long_is_refused() {
        let base = std::env::temp_dir().join(format!("kvo-long-{}", std::process::id()));
        let run = base.join("r".repeat(120));
        let error = Endpoint::new(&base.join("db"), &run).expect_err("too long");
        assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
        let _ = std::fs::remove_dir_all(&base);
    }

    /// The owner's socket path is read back as it was left, by an endpoint of the same
    /// database with another run directory; before any owner left one there is none,
    /// and what is no socket's path is refused.
    #[test]
    fn the_owners_socket_is_found_through_the_database() {
        let base = std::env::temp_dir().join(format!("kvo-pointer-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&base);
        let owner = Endpoint::new(&base.join("db"), &base.join("one")).expect("an endpoint");
        let other = Endpoint::new(&base.join("db"), &base.join("two")).expect("an endpoint");
        assert_eq!(
            other.owner_socket().expect_err("no owner yet").kind(),
            io::ErrorKind::NotFound
        );
        owner.publish().expect("the path is left");
        assert_eq!(other.owner_socket().expect("the owner's socket"), owner.socket);
        assert_ne!(other.socket, owner.socket);

        std::fs::write(owner.pointer(), b"not/absolute").expect("a broken file");
        assert_eq!(
            other.owner_socket().expect_err("no path").kind(),
            io::ErrorKind::InvalidData
        );
        let _ = std::fs::remove_dir_all(&base);
    }

    /// A process is its own user, and a default run directory is the user's own.
    #[tokio::test]
    async fn the_default_run_directory_names_the_user() {
        let me = uid().expect("this process's user");
        let (end, _other) = tokio::net::UnixStream::pair().expect("a pair of sockets");
        same_user(&end, me, "the peer").expect("both ends are this process");
        let refused = same_user(&end, me.wrapping_add(1), "the peer").expect_err("another user");
        assert_eq!(refused.kind(), io::ErrorKind::PermissionDenied);
        let run = default_run_dir(me);
        assert!(
            run.ends_with("kvshare") || run.ends_with(format!("kvshare-{me}")),
            "{}",
            run.display()
        );
    }
}
