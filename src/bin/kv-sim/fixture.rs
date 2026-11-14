//! A small run made by hand, which passes the check, and the faults that can be put
//! into it, each of which one check must find: this is how the checks are checked.

use std::path::Path;

use surrealkv::{Tree, TreeBuilder};

use crate::keys::{
    self, Answer, Entry, Fact, FileFacts, Lease, Meta, Metric, OpId, Status, Task, Total,
    Transition,
};
use crate::roles::VLOG_THRESHOLD;

const THREAD: &str = "tfixture";

fn op(role: char, pid: u32, n: u64) -> OpId {
    OpId { role, pid, n }
}

fn open(path: &Path) -> Result<Tree, String> {
    TreeBuilder::new()
        .with_path(path.to_path_buf())
        .with_enable_vlog(true)
        .with_vlog_value_threshold(VLOG_THRESHOLD)
        .build()
        .map_err(|e| e.to_string())
}

/// Commit `writes` in one transaction.
async fn commit(tree: &Tree, writes: Vec<(Vec<u8>, Option<Vec<u8>>)>) -> Result<(), String> {
    let mut tx = tree.begin().map_err(|e| e.to_string())?;
    for (key, value) in writes {
        match value {
            Some(value) => tx.set(key, value),
            None => tx.delete(key),
        }
        .map_err(|e| e.to_string())?;
    }
    tx.commit().await.map_err(|e| e.to_string())
}

/// Make the run in `run`: a database, a journal, and a checkpoint of the database.
pub async fn make(run: &Path) -> Result<(), String> {
    let _ = std::fs::remove_dir_all(run);
    for sub in ["journal", "ckpt"] {
        std::fs::create_dir_all(run.join(sub)).map_err(|e| e.to_string())?;
    }
    let tree = open(&run.join("db"))?;
    let (append, metric) = (op('a', 1, 1), op('a', 1, 2));
    let (created, triaged, index, answer) =
        (op('h', 2, 1), op('h', 2, 2), op('i', 3, 1), op('h', 4, 1));
    let mut writes = vec![
        (
            keys::meta(THREAD),
            Some(keys::encode(&Meta {
                head: 3,
                closed: false,
            })),
        ),
        (
            keys::lease(THREAD),
            Some(keys::encode(&Lease {
                pid: 1,
                epoch: 1,
                until_ms: 0,
            })),
        ),
    ];
    for seq in 1..=3 {
        let entry = Entry {
            op: append,
            batch: 3,
            pid: 1,
            epoch: 1,
            body: vec![seq as u8],
        };
        writes.push((keys::entry(THREAD, seq), Some(keys::encode(&entry))));
    }
    writes.push((
        keys::metric(THREAD, 7),
        Some(keys::encode(&Metric {
            op: metric,
            tokens: 100,
        })),
    ));
    writes.push((
        keys::TOTAL.to_vec(),
        Some(keys::encode(&Total {
            requests: 1,
            tokens: 100,
        })),
    ));
    let (one, two) = (keys::hash_of(b"one"), keys::hash_of(b"two"));
    for hash in [&one, &two] {
        writes.push((
            keys::fact(hash),
            Some(keys::encode(&Fact {
                op: index,
                body: vec![1],
            })),
        ));
        writes.push((keys::references(hash), Some(keys::encode(&1u64))));
    }
    let file = FileFacts {
        op: index,
        hashes: vec![one.clone(), two.clone()],
    };
    writes.push((keys::file("src/f000.rs"), Some(keys::encode(&file))));
    writes.push((
        keys::answer(5),
        Some(keys::encode(&Answer {
            op: answer,
            value: 42,
        })),
    ));
    let step = Transition {
        from: None,
        to: Status::New,
        op: created,
    };
    writes.push((keys::transition(1, 1), Some(keys::encode(&step))));
    writes.push((
        keys::task(1),
        Some(keys::encode(&Task {
            status: Status::New,
            n: 1,
            op: created,
        })),
    ));
    commit(&tree, writes).await?;
    let step = Transition {
        from: Some(Status::New),
        to: Status::Triaged,
        op: triaged,
    };
    commit(
        &tree,
        vec![
            (keys::transition(1, 2), Some(keys::encode(&step))),
            (
                keys::task(1),
                Some(keys::encode(&Task {
                    status: Status::Triaged,
                    n: 2,
                    op: triaged,
                })),
            ),
        ],
    )
    .await?;
    tree.create_checkpoint(run.join("ckpt").join("1"))
        .map_err(|e| e.to_string())?;
    std::fs::write(run.join("ckpt").join("last"), "1").map_err(|e| e.to_string())?;
    tree.close().await.map_err(|e| e.to_string())?;

    let journal = [
        format!("begin 1 {append} append {THREAD} 3 1 1"),
        format!("ack 1 {append}"),
        format!("begin 2 {metric} metric {THREAD} 7 100"),
        format!("ack 2 {metric}"),
        format!("begin 3 {created} transition 1 - New 1"),
        format!("ack 3 {created}"),
        format!("begin 4 {triaged} transition 1 New Triaged 2"),
        format!("ack 4 {triaged}"),
        format!("begin 5 {index} index src/f000.rs 2"),
        format!("ack 5 {index}"),
        format!("begin 6 {answer} answer 5 42"),
        format!("ack 6 {answer}"),
        "seen 7 5 42".to_string(),
    ];
    write_journal(run, &journal.join("\n"))
}

fn write_journal(run: &Path, lines: &str) -> Result<(), String> {
    use std::io::Write as _;
    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(run.join("journal").join("fixture-0.log"))
        .map_err(|e| e.to_string())?;
    writeln!(file, "{lines}").map_err(|e| e.to_string())
}

/// Put `fault` into the run in `run`; a fault is named by the check that must find it:
/// `lost`, `twice`, `thread`, `task`, `index`, `metrics`, `removed`, `hook`,
/// `unknown_op`, `checkpoint`, `online`.
pub async fn corrupt(run: &Path, fault: &str) -> Result<(), String> {
    let path = if fault == "checkpoint" {
        run.join("ckpt").join("1")
    } else {
        run.join("db")
    };
    let tree = open(&path)?;
    let writes = match fault {
        // An acknowledged entry deleted, and the head with it.
        "lost" => vec![
            (keys::entry(THREAD, 3), None),
            (
                keys::meta(THREAD),
                Some(keys::encode(&Meta {
                    head: 2,
                    closed: false,
                })),
            ),
        ],
        // An operation's batch written a second time.
        "twice" => {
            let entry = Entry {
                op: op('a', 1, 1),
                batch: 3,
                pid: 1,
                epoch: 1,
                body: vec![4],
            };
            vec![
                (keys::entry(THREAD, 4), Some(keys::encode(&entry))),
                (
                    keys::meta(THREAD),
                    Some(keys::encode(&Meta {
                        head: 4,
                        closed: false,
                    })),
                ),
            ]
        }
        // An entry of an older epoch after a newer one.
        "thread" => {
            let entry = Entry {
                op: op('a', 1, 1),
                batch: 3,
                pid: 1,
                epoch: 0,
                body: vec![3],
            };
            vec![(keys::entry(THREAD, 3), Some(keys::encode(&entry)))]
        }
        // A step the flow does not allow.
        "task" => {
            let step = Transition {
                from: Some(Status::Triaged),
                to: Status::Done,
                op: op('h', 2, 3),
            };
            vec![
                (keys::transition(1, 3), Some(keys::encode(&step))),
                (
                    keys::task(1),
                    Some(keys::encode(&Task {
                        status: Status::Done,
                        n: 3,
                        op: op('h', 2, 3),
                    })),
                ),
            ]
        }
        "index" => vec![(
            keys::references(&keys::hash_of(b"one")),
            Some(keys::encode(&2u64)),
        )],
        "metrics" => vec![(
            keys::TOTAL.to_vec(),
            Some(keys::encode(&Total {
                requests: 2,
                tokens: 100,
            })),
        )],
        // A thread removed by an acknowledged drop, but a piece of it left.
        "removed" => {
            write_journal(run, "begin 8 k9.1 drop tgone\nack 8 k9.1")?;
            let entry = Entry {
                op: op('a', 1, 1),
                batch: 3,
                pid: 1,
                epoch: 1,
                body: vec![9],
            };
            vec![(keys::entry("tgone", 1), Some(keys::encode(&entry)))]
        }
        "hook" => vec![(
            keys::answer(5),
            Some(keys::encode(&Answer {
                op: op('h', 4, 1),
                value: 43,
            })),
        )],
        // A metric no journal began, the total kept right.
        "unknown_op" => vec![
            (
                keys::metric(THREAD, 8),
                Some(keys::encode(&Metric {
                    op: op('z', 9, 9),
                    tokens: 1,
                })),
            ),
            (
                keys::TOTAL.to_vec(),
                Some(keys::encode(&Total {
                    requests: 2,
                    tokens: 101,
                })),
            ),
        ],
        // The checkpoint's total, not the database's, made wrong.
        "checkpoint" => vec![(
            keys::TOTAL.to_vec(),
            Some(keys::encode(&Total {
                requests: 9,
                tokens: 9,
            })),
        )],
        "online" => {
            write_journal(run, "violation 9 metrics: the total is wrong in a snapshot")?;
            Vec::new()
        }
        other => return Err(format!("no fault is called {other}")),
    };
    if !writes.is_empty() {
        commit(&tree, writes).await?;
    }
    tree.close().await.map_err(|e| e.to_string())
}
