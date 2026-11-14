//! The report of a run: what each role did and how long it took, what went wrong on the
//! way (conflicts, lost and silent owners, commits of unknown outcome), the processes'
//! files and memory, what was left behind, the space on disk, and the findings. A run
//! fails on any finding, a leak of files, more than one socket left, a process that would
//! not end, a disk that ran low, and, with the budget, a hook's p99 over it.

use std::collections::HashMap;
use std::path::Path;
use std::time::Duration;

use crate::check::Finding;
use crate::conductor::{HOOK_BUDGET, LOW_DISK, Sample, When};
use crate::journal::Journals;

/// What the conductor saw of a run.
pub struct Run {
    pub seed: u64,
    pub elapsed: Duration,
    /// Each hook's time from its start to its exit, whether it ended well, and what the
    /// owner went through meanwhile.
    pub hook_times: Vec<(Duration, bool, When)>,
    pub counts: HashMap<&'static str, u64>,
    pub samples: HashMap<u32, Vec<Sample>>,
    /// The least free space on the run's disk the conductor saw, in bytes.
    pub least_free: u64,
    pub sockets: usize,
    pub unended: usize,
    /// The hooks still running long after the run's end, which the conductor killed.
    pub stuck_hooks: Vec<u32>,
    pub budget: bool,
}

/// How many open files a process may gain over the last three quarters of its life
/// before it counts as a leak.
const LEAK: u64 = 64;

/// A leak of files in a process's looks: at least eight of them, and more than `LEAK`
/// files gained from the end of the first quarter to the last.
fn leak(pid: u32, series: &[Sample]) -> Option<String> {
    if series.len() < 8 {
        return None;
    }
    let (quarter, last) = (series[series.len() / 4].1, series[series.len() - 1].1);
    (last > quarter + LEAK)
        .then(|| format!("process {pid}: open files grew from {quarter} to {last}"))
}

/// How many findings of one check the report shows.
const SHOWN: usize = 10;

/// The number of findings of each check, then the first `SHOWN` of each, so that a flood
/// of one kind hides no other.
pub fn findings(findings: &[Finding]) {
    let mut by_check: Vec<(&str, usize)> = Vec::new();
    for found in findings {
        match by_check.iter_mut().find(|(check, _)| *check == found.check) {
            Some((_, count)) => *count += 1,
            None => by_check.push((found.check, 1)),
        }
    }
    if by_check.is_empty() {
        return;
    }
    let counts: Vec<String> = by_check
        .iter()
        .map(|(check, count)| format!("{check} {count}"))
        .collect();
    println!("findings by check: {}", counts.join(", "));
    for (check, count) in by_check {
        for found in findings.iter().filter(|f| f.check == check).take(SHOWN) {
            println!("[{}] {}", found.check, found.text);
        }
        if count > SHOWN {
            println!("[{check}] … and {} more", count - SHOWN);
        }
    }
}

/// The p50, p99 and largest of `micros`.
fn spread(mut micros: Vec<u128>) -> String {
    if micros.is_empty() {
        return "-".into();
    }
    micros.sort_unstable();
    let at = |q: f64| micros[((micros.len() - 1) as f64 * q) as usize];
    format!(
        "{} {} {} ms",
        ms(at(0.5)),
        ms(at(0.99)),
        ms(*micros.last().expect("not empty"))
    )
}

fn ms(micros: u128) -> String {
    format!("{:.1}", micros as f64 / 1000.0)
}

fn mib(bytes: u64) -> u64 {
    bytes >> 20
}

/// The bytes of the files under `path`.
fn size(path: &Path) -> u64 {
    match std::fs::symlink_metadata(path) {
        Ok(meta) if meta.is_dir() => std::fs::read_dir(path).map_or(0, |entries| {
            entries.flatten().map(|entry| size(&entry.path())).sum()
        }),
        Ok(meta) => meta.len(),
        Err(_) => 0,
    }
}

/// Print the report; whether the run failed.
pub fn print(dir: &Path, run: &Run, found: &[Finding]) -> Result<bool, String> {
    let journals = Journals::read(dir).map_err(|e| e.to_string())?;
    println!(
        "kv-sim report: seed {}, {:.1} s",
        run.seed,
        run.elapsed.as_secs_f64()
    );

    let owners = journals
        .times
        .get("hook.open")
        .map_or(0, |t| t.iter().filter(|(_, e)| e == "owner").count());
    println!(
        "hooks: {} run, {} became the owner; start to exit (p50 p99 max), by what the owner went through:",
        run.hook_times.len(),
        owners
    );
    let mut p99 = 0;
    for when in [When::Calm, When::OwnerKilled, When::OwnerStopped] {
        let of: Vec<&(Duration, bool, When)> = run
            .hook_times
            .iter()
            .filter(|(_, _, w)| *w == when)
            .collect();
        let micros: Vec<u128> = of.iter().map(|(d, _, _)| d.as_micros()).collect();
        let failed = of.iter().filter(|(_, ok, _)| !ok).count();
        println!(
            "  {:<14} {:>5} hooks, {:>4} failed: {}",
            format!("{when:?}"),
            micros.len(),
            failed,
            spread(micros.clone())
        );
        if when == When::Calm {
            let mut sorted = micros;
            sorted.sort_unstable();
            p99 = sorted
                .get(((sorted.len().max(1) - 1) as f64 * 0.99) as usize)
                .copied()
                .unwrap_or(0);
        }
    }
    for (name, split) in [("hook.open", "owner"), ("hook.open", "client")] {
        if let Some(times) = journals.times.get(name) {
            let of: Vec<u128> = times
                .iter()
                .filter(|(_, e)| e == split)
                .map(|(t, _)| u128::from(*t))
                .collect();
            println!(
                "  open as {split}: {} ({} hooks)",
                spread(of.clone()),
                of.len()
            );
        }
    }

    let mut names: Vec<&String> = journals
        .times
        .keys()
        .filter(|n| *n != "hook.open")
        .collect();
    names.sort();
    println!("timings (p50 p99 max):");
    for name in names {
        let times: Vec<u128> = journals.times[name]
            .iter()
            .map(|(t, _)| u128::from(*t))
            .collect();
        println!("  {name:<20} {:>6}  {}", times.len(), spread(times.clone()));
    }

    let mut states: HashMap<(String, String), u64> = HashMap::new();
    for record in journals.ops.values() {
        *states
            .entry((
                record.kind.clone(),
                format!("{:?}", record.state).to_lowercase(),
            ))
            .or_default() += 1;
    }
    let mut states: Vec<_> = states.into_iter().collect();
    states.sort();
    println!("operations:");
    for ((kind, state), count) in states {
        println!("  {kind:<12} {state:<8} {count}");
    }

    let mut counts: Vec<(String, u64)> = journals.counts.clone().into_iter().collect();
    counts.extend(
        run.counts
            .iter()
            .map(|(k, v)| (format!("conductor.{k}"), *v)),
    );
    counts.sort();
    println!("counts:");
    for (name, count) in counts {
        println!("  {name:<32} {count}");
    }

    let mut leaks = Vec::new();
    println!("processes (open files first → last, resident MiB first → last, peak):");
    let mut pids: Vec<&u32> = run.samples.keys().collect();
    pids.sort();
    for pid in pids {
        let series = &run.samples[pid];
        let (first, last) = (series[0], series[series.len() - 1]);
        let peak = series.iter().map(|sample| sample.2).max().unwrap_or(0);
        println!(
            "  {pid:>7}: {:>3} samples, files {} → {}, memory {:.0} → {:.0}, peak {:.0}",
            series.len(),
            first.1,
            last.1,
            first.2 as f64 / 1024.0,
            last.2 as f64 / 1024.0,
            peak as f64 / 1024.0
        );
        leaks.extend(leak(*pid, series));
    }
    println!(
        "left behind: {} socket files, {} processes that would not end, {} hooks stuck",
        run.sockets,
        run.unended,
        run.stuck_hooks.len()
    );
    let mut parts: Vec<String> = std::fs::read_dir(dir.join("db"))
        .map(|entries| {
            entries
                .flatten()
                .filter(|entry| entry.path().is_dir())
                .map(|entry| {
                    format!(
                        "{} {}",
                        entry.file_name().to_string_lossy(),
                        mib(size(&entry.path()))
                    )
                })
                .collect()
        })
        .unwrap_or_default();
    parts.sort();
    println!(
        "disk (MiB, by file sizes, clones counted in full): database {} ({}), checkpoints {}, least free {}",
        mib(size(&dir.join("db"))),
        parts.join(", "),
        mib(size(&dir.join("ckpt"))),
        mib(run.least_free)
    );

    let mut failures: Vec<String> = Vec::new();
    failures.extend(leaks);
    if run.least_free < LOW_DISK {
        failures.push(format!(
            "the disk had {} MiB free; the run stopped at {:.0} s",
            mib(run.least_free),
            run.elapsed.as_secs_f64()
        ));
    }
    if run.sockets > 1 {
        failures.push(format!("{} socket files left", run.sockets));
    }
    if run.unended > 0 {
        failures.push(format!("{} processes would not end", run.unended));
    }
    if !run.stuck_hooks.is_empty() {
        failures.push(format!(
            "hooks {:?} were still running long after the run's end; what they were \
             doing is in logs/stuck-<pid>.*",
            run.stuck_hooks
        ));
    }
    let over = p99 > HOOK_BUDGET.as_micros();
    if over {
        let text = format!(
            "a calm hook's p99 is {} ms, over {} ms",
            ms(p99),
            HOOK_BUDGET.as_millis()
        );
        if run.budget {
            failures.push(text);
        } else {
            println!("over budget (not failing this run): {text}");
        }
    }
    println!("findings: {}", found.len());
    findings(found);
    for failure in &failures {
        println!("[failure] {failure}");
    }
    Ok(!found.is_empty() || !failures.is_empty())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Looks a second apart, with these numbers of open files.
    fn looks(files: impl IntoIterator<Item = u64>) -> Vec<Sample> {
        files
            .into_iter()
            .enumerate()
            .map(|(at, files)| (at as f64, files, 1024))
            .collect()
    }

    #[test]
    fn a_process_that_keeps_opening_files_leaks() {
        let series = looks((0..20).map(|at| 14 + at * 10));
        assert!(leak(7, &series).is_some_and(|text| text.contains("process 7")));
    }

    #[test]
    fn a_steady_process_or_a_short_life_does_not_leak() {
        assert_eq!(leak(7, &looks([14; 20])), None);
        // Files opened at the start are no leak: the first quarter is left out.
        assert_eq!(
            leak(7, &looks([14, 200, 210, 210, 210, 210, 210, 210, 210, 210])),
            None
        );
        assert_eq!(leak(7, &looks([14, 500, 900])), None);
    }
}
