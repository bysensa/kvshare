//! Writers in several processes keep committing while the owner of the database is
//! killed with SIGKILL again and again: every commit a writer saw return is in the
//! database afterwards, and a new owner takes over each time.
//!
//! Run with `--nocapture` to see the counts and how long each hand-over took.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use surrealkv::{Mode, TreeBuilder};
use tokio::process::{Child, Command};

const WRITERS: usize = 4;
const KILLING_FOR: Duration = Duration::from_secs(6);
const BETWEEN_KILLS: Duration = Duration::from_millis(300);

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn no_returned_commit_is_lost_when_owners_are_killed_eventual() {
    kill_owners_while_writing("eventual").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn no_returned_commit_is_lost_when_owners_are_killed_immediate() {
    kill_owners_while_writing("immediate").await;
}

async fn kill_owners_while_writing(durability: &str) {
    let base = std::env::temp_dir().join(format!("kvo-{durability}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&base);
    let (db, run_dir, acks) = (base.join("db"), base.join("run"), base.join("acks"));
    std::fs::create_dir_all(&acks).expect("the directory of the acknowledgements");

    let mut writers: HashMap<u32, Child> = HashMap::new();
    let mut next_id = 0;
    for _ in 0..WRITERS {
        let child = spawn_writer(&db, &run_dir, &acks, next_id, durability);
        writers.insert(child.id().expect("a running writer"), child);
        next_id += 1;
    }

    // Kill whichever writer owns the database, start another writer in its place, and
    // wait until another process answers as the owner. The probe only asks the owner
    // over its socket: it never opens the database. Its options are the writers'.
    let probe = kvshare::TreeBuilder::new()
        .with_path(db.clone())
        .with_versioning(true, 0)
        .with_run_dir(run_dir.clone());
    let mut kills: Vec<u128> = Vec::new();
    let started = Instant::now();
    while started.elapsed() < KILLING_FOR {
        tokio::time::sleep(BETWEEN_KILLS).await;
        let Ok(pid) = probe.current_owner().await else {
            continue;
        };
        let mut owner = writers.remove(&pid).unwrap_or_else(|| {
            panic!(
                "the owner {pid} is none of the writers {:?}",
                writers.keys()
            )
        });
        let at = unix_nanos();
        owner.start_kill().expect("SIGKILL to the owner");
        owner.wait().await.expect("the owner exits");
        kills.push(at);
        let child = spawn_writer(&db, &run_dir, &acks, next_id, durability);
        writers.insert(child.id().expect("a running writer"), child);
        next_id += 1;
        let waiting = Instant::now();
        while waiting.elapsed() < Duration::from_secs(5) {
            match probe.current_owner().await {
                Ok(next) if next != pid => break,
                _ => tokio::time::sleep(Duration::from_millis(5)).await,
            }
        }
    }
    // The last owner dies with SIGKILL as well: its returned commits must survive too.
    for (_, mut writer) in writers.drain() {
        writer.start_kill().expect("SIGKILL to a writer");
        writer.wait().await.expect("the writer exits");
    }

    let acknowledged = read_acknowledgements(&acks);
    let tree = TreeBuilder::new()
        .with_path(db.clone())
        .with_versioning(true, 0)
        .build()
        .expect("the database opens once every writer is gone");
    let read = tree.begin_with_mode(Mode::ReadOnly).expect("a read");
    let missing: Vec<&String> = acknowledged
        .iter()
        .filter(|(key, _)| read.get(key.as_bytes()).expect("a get").is_none())
        .map(|(key, _)| key)
        .collect();
    drop(read);
    tree.close().await.expect("the database closes");

    let mut handovers: Vec<Duration> = kills
        .iter()
        .filter_map(|&killed| {
            let first = acknowledged
                .iter()
                .map(|(_, at)| *at)
                .filter(|&at| at > killed)
                .min()?;
            Some(Duration::from_nanos((first - killed) as u64))
        })
        .collect();
    handovers.sort();
    eprintln!(
        "{durability}: {} commits returned by {next_id} writers, {} owners killed, {} missing; \
         first commit after a kill: median {:?}, max {:?}",
        acknowledged.len(),
        kills.len(),
        missing.len(),
        handovers.get(handovers.len() / 2),
        handovers.last(),
    );

    assert!(kills.len() >= 5, "only {} owners were killed", kills.len());
    assert!(
        missing.is_empty(),
        "{} returned commits are missing, the first: {:?}",
        missing.len(),
        &missing[..missing.len().min(5)]
    );
    let _ = std::fs::remove_dir_all(&base);
}

fn spawn_writer(db: &Path, run_dir: &Path, acks: &Path, id: usize, durability: &str) -> Child {
    Command::new(env!("CARGO_BIN_EXE_kv-worker"))
        .arg("write")
        .arg(db)
        .arg(run_dir)
        .arg(id.to_string())
        .arg(durability)
        .arg(acks.join(format!("{id}.acks")))
        .stdout(Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .expect("a writer starts")
}

/// Every `key\tunix_nanos` line the writers appended. A line cut short by SIGKILL has
/// no complete timestamp and is left out: its commit was not acknowledged in full.
fn read_acknowledgements(dir: &Path) -> Vec<(String, u128)> {
    let mut acknowledged = Vec::new();
    for entry in std::fs::read_dir(dir).expect("the acknowledgements") {
        let path: PathBuf = entry.expect("an entry").path();
        let text = std::fs::read_to_string(&path).expect("a file of acknowledgements");
        for line in text.lines() {
            if let Some((key, at)) = line.split_once('\t')
                && let Ok(at) = at.parse()
            {
                acknowledged.push((key.to_string(), at));
            }
        }
    }
    acknowledged
}

fn unix_nanos() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("after 1970")
        .as_nanos()
}
