//! Where the processes of one database meet.
//!
//! Every process derives the same socket path from the database's canonical path, so
//! no process has to ask another where the owner listens: `~/x/../db` and `/Users/me/db`
//! meet at one socket. The hash keeps the path short, because a Unix socket path is
//! limited to 104 bytes on macOS and 108 on Linux.

use std::io;
use std::os::unix::ffi::OsStrExt;
use std::path::{Path, PathBuf};

/// The longest socket path both macOS and Linux accept, without the closing NUL.
const MAX_SOCKET_PATH: usize = 103;

/// The database and the socket its owner serves it on.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Endpoint {
    /// The canonical path of the database directory.
    pub db: PathBuf,
    /// `<run_dir>/kv-<16 hex digits of blake3(db)>.sock`.
    pub socket: PathBuf,
}

impl Endpoint {
    /// The endpoint of the database at `db`, with its socket in `run_dir`.
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
}
