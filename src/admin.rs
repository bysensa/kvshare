//! Operations on the whole database, `Tree`'s own: checkpoints, a restore, compaction, the
//! log, and what SurrealKV reports of itself.
//!
//! The owner runs them for itself and for its clients by the same [`run_blocking`], off
//! the async threads: most are blocking file I/O. What they answer crosses the socket as plain data:
//! the enums SurrealKV marks `#[non_exhaustive]` as numbers, and SurrealKV's structs that
//! nobody else may build as kvshare's own, with the same fields.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use surrealkv::tde::DbId;
use surrealkv::{
    BackgroundErrorReason, CipherSuite, CompactRangeOptions, ErrorSeverity, KeyState, Tree,
    WalRecoveryReport,
};

use crate::error::{Error, KvError, Result};

/// What a checkpoint holds, as SurrealKV's `CheckpointMetadata`, which SurrealKV keeps in
/// a private module.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct CheckpointMetadata {
    /// The version of the checkpoint's format.
    pub version: u32,
    /// When the checkpoint was made.
    pub timestamp: u64,
    /// The sequence number of the database at the checkpoint.
    pub sequence_number: u64,
    /// How many tables the checkpoint holds.
    pub sstable_count: usize,
    /// The checkpoint's size in bytes.
    pub total_size: u64,
}

/// An error of SurrealKV's background work, as SurrealKV's `BackgroundError`, which
/// nobody else may build.
#[derive(Debug)]
pub struct BackgroundError {
    pub error: Error,
    pub severity: ErrorSeverity,
    pub reason: BackgroundErrorReason,
    /// When the error was recorded, on this process's monotonic clock: a client learns
    /// how long ago the owner recorded it.
    pub timestamp: Instant,
}

/// How many files, and how many bytes in them, as SurrealKV's `FileTotals`.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct FileTotals {
    pub files: u64,
    pub bytes: u64,
}

/// What one key covers, as SurrealKV's `KeyUsage`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct KeyUsage {
    pub key_id: u32,
    /// What the key manager says the key may do; `None` when it was not asked.
    pub state: Option<KeyState>,
    pub manifest: FileTotals,
    pub tables: FileTotals,
    pub value_log: FileTotals,
    pub wal: FileTotals,
}

/// Whether the database is encrypted, and which keys cover what, as SurrealKV's
/// `EncryptionStatus`. Its shape is not settled in SurrealKV.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EncryptionStatus {
    pub encrypted: bool,
    pub suite: Option<CipherSuite>,
    pub db_id: Option<DbId>,
    pub active_key_id: Option<u32>,
    pub manifest_key_id: Option<u32>,
    pub manifest_key_is_active: bool,
    pub keys: Vec<KeyUsage>,
    pub unattributed: FileTotals,
    pub plaintext: FileTotals,
    pub warnings: Vec<String>,
    pub notes: Vec<String>,
}

/// An operation on the whole database.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum Admin {
    /// `create_checkpoint`, into an absolute directory.
    CreateCheckpoint(PathBuf),
    /// `restore_from_checkpoint`, from an absolute directory.
    RestoreFromCheckpoint(PathBuf),
    CompactRange {
        start: Option<Vec<u8>>,
        end: Option<Vec<u8>>,
        flush_memtable: bool,
        target_level: Option<u8>,
        rewrite_bottommost: bool,
    },
    FlushWal {
        sync: bool,
    },
    RecoveryReport,
    BackgroundError,
    BackgroundErrorCount,
    IsStopped,
    PrepareKeys,
    EncryptionStatus,
}

impl Admin {
    /// Whether the operation may take long enough that a client should not give up on
    /// the owner while it runs: it copies, restores, compacts or reads the files.
    pub(crate) fn takes_long(&self) -> bool {
        matches!(
            self,
            Admin::CreateCheckpoint(_)
                | Admin::RestoreFromCheckpoint(_)
                | Admin::CompactRange { .. }
                | Admin::EncryptionStatus
        )
    }
}

/// What an operation on the whole database answered.
#[derive(Debug, Serialize, Deserialize)]
pub(crate) enum Reply {
    Done,
    Checkpoint(CheckpointMetadata),
    Recovery(Recovery),
    BackgroundError(Option<Recorded>),
    Count(u64),
    Flag(bool),
    Encryption(Encryption),
}

/// SurrealKV's `WalRecoveryReport`, as it crosses the socket.
#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct Recovery {
    segments_replayed: u64,
    records_replayed: u64,
    records_dropped: u64,
    bytes_trimmed: u64,
    quarantined_files: Vec<PathBuf>,
}

/// A background error, as it crosses the socket: its enums as numbers, and how long ago
/// it was recorded.
#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct Recorded {
    error: KvError,
    severity: u8,
    reason: u8,
    age: Duration,
}

/// An `EncryptionStatus`, as it crosses the socket.
#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct Encryption {
    encrypted: bool,
    suite: Option<u8>,
    db_id: Option<[u8; 16]>,
    active_key_id: Option<u32>,
    manifest_key_id: Option<u32>,
    manifest_key_is_active: bool,
    keys: Vec<Usage>,
    unattributed: FileTotals,
    plaintext: FileTotals,
    warnings: Vec<String>,
    notes: Vec<String>,
}

/// A `KeyUsage`, as it crosses the socket.
#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct Usage {
    key_id: u32,
    state: Option<u8>,
    manifest: FileTotals,
    tables: FileTotals,
    value_log: FileTotals,
    wal: FileTotals,
}

/// Run `admin` on `tree` on a thread that may block, and wait for its answer.
pub(crate) async fn run_blocking(
    tree: Arc<Tree>,
    admin: Admin,
) -> std::result::Result<Reply, KvError> {
    tokio::task::spawn_blocking(move || run(&tree, admin))
        .await
        .unwrap_or_else(|_| {
            Err(KvError::Engine(
                "an operation on the database panicked".into(),
            ))
        })
}

/// Run `admin` on `tree`. Blocking.
fn run(tree: &Tree, admin: Admin) -> std::result::Result<Reply, KvError> {
    Ok(match admin {
        Admin::CreateCheckpoint(dir) => {
            let metadata = tree.create_checkpoint(dir)?;
            Reply::Checkpoint(CheckpointMetadata {
                version: metadata.version,
                timestamp: metadata.timestamp,
                sequence_number: metadata.sequence_number,
                sstable_count: metadata.sstable_count,
                total_size: metadata.total_size,
            })
        }
        Admin::RestoreFromCheckpoint(dir) => {
            let metadata = tree.restore_from_checkpoint(dir)?;
            Reply::Checkpoint(CheckpointMetadata {
                version: metadata.version,
                timestamp: metadata.timestamp,
                sequence_number: metadata.sequence_number,
                sstable_count: metadata.sstable_count,
                total_size: metadata.total_size,
            })
        }
        Admin::CompactRange {
            start,
            end,
            flush_memtable,
            target_level,
            rewrite_bottommost,
        } => {
            let options = CompactRangeOptions {
                flush_memtable,
                target_level,
                rewrite_bottommost,
            };
            tree.compact_range(start.as_deref(), end.as_deref(), &options)?;
            Reply::Done
        }
        Admin::FlushWal { sync } => {
            tree.flush_wal(sync)?;
            Reply::Done
        }
        Admin::RecoveryReport => {
            let report = tree.recovery_report();
            Reply::Recovery(Recovery {
                segments_replayed: report.segments_replayed as u64,
                records_replayed: report.records_replayed as u64,
                records_dropped: report.records_dropped as u64,
                bytes_trimmed: report.bytes_trimmed,
                quarantined_files: report.quarantined_files,
            })
        }
        Admin::BackgroundError => Reply::BackgroundError(match tree.background_error() {
            None => None,
            Some(recorded) => Some(Recorded {
                severity: severity_code(recorded.severity)?,
                reason: reason_code(recorded.reason)?,
                age: recorded.timestamp.elapsed(),
                error: recorded.error.into(),
            }),
        }),
        Admin::BackgroundErrorCount => Reply::Count(tree.background_error_count()),
        Admin::IsStopped => Reply::Flag(tree.is_stopped()),
        Admin::PrepareKeys => {
            tree.prepare_keys()?;
            Reply::Done
        }
        Admin::EncryptionStatus => {
            let status = tree.encryption_status()?;
            let mut keys = Vec::with_capacity(status.keys.len());
            for usage in status.keys {
                keys.push(Usage {
                    key_id: usage.key_id,
                    state: usage.state.map(key_state_code).transpose()?,
                    manifest: totals(usage.manifest),
                    tables: totals(usage.tables),
                    value_log: totals(usage.value_log),
                    wal: totals(usage.wal),
                });
            }
            Reply::Encryption(Encryption {
                encrypted: status.encrypted,
                suite: status.suite.map(suite_code).transpose()?,
                db_id: status.db_id.map(|id| *id.as_bytes()),
                active_key_id: status.active_key_id,
                manifest_key_id: status.manifest_key_id,
                manifest_key_is_active: status.manifest_key_is_active,
                keys,
                unattributed: totals(status.unattributed),
                plaintext: totals(status.plaintext),
                warnings: status.warnings,
                notes: status.notes,
            })
        }
    })
}

fn totals(totals: surrealkv::FileTotals) -> FileTotals {
    FileTotals {
        files: totals.files,
        bytes: totals.bytes,
    }
}

impl From<Recovery> for WalRecoveryReport {
    fn from(recovery: Recovery) -> Self {
        WalRecoveryReport {
            segments_replayed: recovery.segments_replayed as usize,
            records_replayed: recovery.records_replayed as usize,
            records_dropped: recovery.records_dropped as usize,
            bytes_trimmed: recovery.bytes_trimmed,
            quarantined_files: recovery.quarantined_files,
        }
    }
}

impl Recorded {
    /// The error as this process sees it: recorded `age` before now.
    pub(crate) fn received(self) -> Result<BackgroundError> {
        Ok(BackgroundError {
            error: self.error.into(),
            severity: severity(self.severity)?,
            reason: reason(self.reason)?,
            timestamp: Instant::now()
                .checked_sub(self.age)
                .unwrap_or_else(Instant::now),
        })
    }
}

impl Encryption {
    pub(crate) fn received(self) -> Result<EncryptionStatus> {
        let mut keys = Vec::with_capacity(self.keys.len());
        for usage in self.keys {
            keys.push(KeyUsage {
                key_id: usage.key_id,
                state: usage.state.map(key_state).transpose()?,
                manifest: usage.manifest,
                tables: usage.tables,
                value_log: usage.value_log,
                wal: usage.wal,
            });
        }
        Ok(EncryptionStatus {
            encrypted: self.encrypted,
            suite: self.suite.map(suite).transpose()?,
            db_id: self.db_id.map(DbId::from_bytes),
            active_key_id: self.active_key_id,
            manifest_key_id: self.manifest_key_id,
            manifest_key_is_active: self.manifest_key_is_active,
            keys,
            unattributed: self.unattributed,
            plaintext: self.plaintext,
            warnings: self.warnings,
            notes: self.notes,
        })
    }
}

/// The refusal for a value of SurrealKV that kvshare cannot carry: SurrealKV may add
/// variants to the enums it marks `#[non_exhaustive]`.
fn unknown(what: &str, value: impl std::fmt::Debug) -> KvError {
    KvError::Engine(format!("kvshare cannot carry the {what} {value:?}"))
}

fn not_a_code(what: &str, code: u8) -> Error {
    Error::Handshake(format!("the owner sent {code} for a {what}"))
}

fn severity_code(severity: ErrorSeverity) -> std::result::Result<u8, KvError> {
    Ok(match severity {
        ErrorSeverity::NoError => 0,
        ErrorSeverity::SoftError => 1,
        ErrorSeverity::HardError => 2,
        ErrorSeverity::FatalError => 3,
        ErrorSeverity::Unrecoverable => 4,
        other => return Err(unknown("error severity", other)),
    })
}

fn severity(code: u8) -> Result<ErrorSeverity> {
    Ok(match code {
        0 => ErrorSeverity::NoError,
        1 => ErrorSeverity::SoftError,
        2 => ErrorSeverity::HardError,
        3 => ErrorSeverity::FatalError,
        4 => ErrorSeverity::Unrecoverable,
        other => return Err(not_a_code("severity", other)),
    })
}

fn reason_code(reason: BackgroundErrorReason) -> std::result::Result<u8, KvError> {
    Ok(match reason {
        BackgroundErrorReason::MemtablaFlush => 0,
        BackgroundErrorReason::Compaction => 1,
        BackgroundErrorReason::ManifestWrite => 2,
        BackgroundErrorReason::CommitGroup => 3,
        BackgroundErrorReason::ValueLogSync => 4,
        other => return Err(unknown("background error reason", other)),
    })
}

fn reason(code: u8) -> Result<BackgroundErrorReason> {
    Ok(match code {
        0 => BackgroundErrorReason::MemtablaFlush,
        1 => BackgroundErrorReason::Compaction,
        2 => BackgroundErrorReason::ManifestWrite,
        3 => BackgroundErrorReason::CommitGroup,
        4 => BackgroundErrorReason::ValueLogSync,
        other => return Err(not_a_code("background error reason", other)),
    })
}

fn key_state_code(state: KeyState) -> std::result::Result<u8, KvError> {
    Ok(match state {
        KeyState::Active => 0,
        KeyState::DecryptOnly => 1,
        KeyState::Retired => 2,
        KeyState::Unknown => 3,
        other => return Err(unknown("key state", other)),
    })
}

fn key_state(code: u8) -> Result<KeyState> {
    Ok(match code {
        0 => KeyState::Active,
        1 => KeyState::DecryptOnly,
        2 => KeyState::Retired,
        3 => KeyState::Unknown,
        other => return Err(not_a_code("key state", other)),
    })
}

fn suite_code(suite: CipherSuite) -> std::result::Result<u8, KvError> {
    Ok(match suite {
        CipherSuite::Aes256Gcm => 1,
        CipherSuite::XChaCha20Poly1305 => 2,
        other => return Err(unknown("cipher suite", other)),
    })
}

fn suite(code: u8) -> Result<CipherSuite> {
    Ok(match code {
        1 => CipherSuite::Aes256Gcm,
        2 => CipherSuite::XChaCha20Poly1305,
        other => return Err(not_a_code("cipher suite", other)),
    })
}
