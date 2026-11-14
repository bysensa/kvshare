//! The simulation of Milou on kvshare (`kv-sim`): its checks find every fault put into a
//! run made by hand, and a run of the simulation finds nothing wrong.
//!
//! The run lasts 30 seconds, or `KVSHARE_SIM_SECONDS`; set, it is the long run, which also
//! fails on a calm hook's p99 over the budget. The report, with the run's seed, shows
//! with `--nocapture` or when the run fails; a failed run keeps its files.

use std::process::{Command, Output};

fn sim(args: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_kv-sim"))
        .args(args)
        .output()
        .expect("kv-sim runs")
}

fn text(output: &Output) -> String {
    format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    )
}

/// Every check finds the fault made for it, and the run made by hand passes.
#[test]
fn the_checks_find_every_fault() {
    let dir = std::env::temp_dir().join(format!("kvsim-faults-{}", std::process::id()));
    let dir = dir.to_str().expect("a path in UTF-8");
    for fault in [
        "lost",
        "twice",
        "thread",
        "task",
        "index",
        "metrics",
        "removed",
        "hook",
        "unknown_op",
        "checkpoint",
        "online",
    ] {
        assert!(
            sim(&["fixture", dir]).status.success(),
            "the fixture is made"
        );
        let clean = sim(&["check", dir]);
        assert!(
            clean.status.success(),
            "the fixture passes: {}",
            text(&clean)
        );
        let corrupt = sim(&["corrupt", dir, fault]);
        assert!(corrupt.status.success(), "{fault}: {}", text(&corrupt));
        let checked = sim(&["check", dir]);
        let report = text(&checked);
        assert!(!checked.status.success(), "{fault} is found: {report}");
        assert!(
            report.contains(&format!("[{fault}]")),
            "the check {fault} finds its fault: {report}"
        );
    }
    let _ = std::fs::remove_dir_all(dir);
}

/// A run of the simulation: hooks, agents, readers, an indexer and a keeper, the owner
/// killed and stopped again and again; nothing wrong is found.
#[test]
fn a_run_finds_nothing_wrong() {
    let long = std::env::var("KVSHARE_SIM_SECONDS").ok();
    let seconds = long.clone().unwrap_or_else(|| "30".to_string());
    let dir = std::env::temp_dir().join(format!("kvsim-{}", std::process::id()));
    let dir = dir.to_str().expect("a path in UTF-8");
    let mut args = vec!["run", "--seconds", &seconds, "--dir", dir];
    if long.is_some() {
        args.extend(["--budget", "--agents", "3"]);
    }
    let output = sim(&args);
    let report = text(&output);
    println!("{report}");
    assert!(
        output.status.success(),
        "the run found something wrong:\n{report}"
    );
}
