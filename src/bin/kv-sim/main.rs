//! A simulation of Milou on one kvshare database, to find what goes wrong under the
//! heaviest load Milou could put on it: hooks as short-lived processes that may own the
//! database for a moment, agents that stream thread journals under leases, readers that
//! check what every snapshot must hold, an indexer of large facts, a keeper that
//! checkpoints, compacts and removes, and a conductor that kills and stops processes,
//! the owner most of all, and checks the database against the roles' journals at the
//! end.
//!
//! - `kv-sim run [--seconds <n>] [--seed <n>] [--dir <path>] [--agents <n>] [--budget]`:
//!   the conductor; `--budget` makes a hook's p99 over 100 ms a failure.
//! - `kv-sim hook <run> <seed> <event>` and `kv-sim agent|reader|indexer|keeper <run>
//!   <seed>`: the roles the conductor starts.
//! - `kv-sim check <run>`: the check of a run's database and journals.
//! - `kv-sim fixture <run>` and `kv-sim corrupt <run> <fault>`: a small run made by hand,
//!   and one fault put into it, which the check must find.

mod check;
mod conductor;
mod fixture;
mod journal;
mod keys;
mod report;
mod rng;
mod roles;

use std::path::PathBuf;
use std::process::ExitCode;

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let words: Vec<&str> = args.iter().map(String::as_str).collect();
    let outcome = match words.as_slice() {
        ["run", options @ ..] => match conductor::Settings::parse(options) {
            Ok(settings) => runtime(true).block_on(conductor::run(settings)),
            Err(problem) => return usage(&problem),
        },
        ["hook", run, seed, event] => match (seed.parse(), event.parse()) {
            (Ok(seed), Ok(event)) => {
                runtime(false).block_on(roles::hook::run(PathBuf::from(run), seed, event))
            }
            _ => return usage("a hook's seed and event are numbers"),
        },
        [
            role @ ("agent" | "reader" | "indexer" | "keeper"),
            run,
            seed,
        ] => {
            let Ok(seed) = seed.parse() else {
                return usage("a role's seed is a number");
            };
            let run = PathBuf::from(run);
            runtime(true).block_on(async move {
                match *role {
                    "agent" => roles::agent::run(run, seed).await,
                    "reader" => roles::reader::run(run, seed).await,
                    "indexer" => roles::indexer::run(run, seed).await,
                    _ => roles::keeper::run(run, seed).await,
                }
            })
        }
        ["check", run] => runtime(false).block_on(async {
            let findings = check::run(&PathBuf::from(run)).await?;
            report::findings(&findings);
            if findings.is_empty() {
                Ok(())
            } else {
                Err(format!("{} findings", findings.len()))
            }
        }),
        ["fixture", run] => runtime(false).block_on(fixture::make(&PathBuf::from(run))),
        ["corrupt", run, fault] => {
            runtime(false).block_on(fixture::corrupt(&PathBuf::from(run), fault))
        }
        _ => return usage("unknown arguments"),
    };
    match outcome {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            // One write, so that the lines of processes sharing a log stay whole.
            let line = format!("kv-sim {}: {error}\n", std::process::id());
            let _ = std::io::Write::write_all(&mut std::io::stderr(), line.as_bytes());
            ExitCode::FAILURE
        }
    }
}

/// A runtime of several threads for the conductor and the long-lived roles; a runtime
/// of one thread for a hook, which starts faster.
fn runtime(threads: bool) -> tokio::runtime::Runtime {
    let mut builder = if threads {
        let mut builder = tokio::runtime::Builder::new_multi_thread();
        builder.worker_threads(2);
        builder
    } else {
        tokio::runtime::Builder::new_current_thread()
    };
    builder.enable_all().build().expect("a runtime")
}

fn usage(problem: &str) -> ExitCode {
    eprintln!(
        "kv-sim: {problem}\n\
         usage: kv-sim run [--seconds <n>] [--seed <n>] [--dir <path>] [--agents <n>] [--budget]\n\
         \x20      kv-sim hook <run> <seed> <event>\n\
         \x20      kv-sim agent|reader|indexer|keeper <run> <seed>\n\
         \x20      kv-sim check <run>\n\
         \x20      kv-sim fixture <run>\n\
         \x20      kv-sim corrupt <run> <fault>"
    );
    ExitCode::from(2)
}
