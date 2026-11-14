//! The conductor: it starts the roles, sends hooks in bursts, kills and stops processes
//! (the owner most of all), starts again what it killed, watches the long-lived
//! processes' files and memory, and at the end stops everything, checks the database
//! against the journals and prints the report.

use std::collections::{HashMap, VecDeque};
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use tokio::process::{Child, Command};
use tokio::task::JoinSet;

use crate::check;
use crate::report::{self, Run};
use crate::rng::Rng;
use crate::roles::builder;

/// How a run goes.
pub struct Settings {
    pub seconds: u64,
    pub seed: u64,
    pub dir: PathBuf,
    pub agents: usize,
    /// Whether a hook's p99 over the budget fails the run.
    pub budget: bool,
    /// Whether to keep the run's files after a run that passed.
    pub keep: bool,
}

impl Settings {
    pub fn parse(options: &[&str]) -> Result<Settings, String> {
        let mut settings = Settings {
            seconds: 30,
            seed: Rng::new(
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map_or(1, |t| t.as_nanos() as u64),
            )
            .next_u64(),
            dir: std::env::temp_dir().join(format!("kvsim-{}", std::process::id())),
            agents: 2,
            budget: false,
            keep: false,
        };
        let mut words = options.iter();
        while let Some(word) = words.next() {
            let mut value = || {
                words
                    .next()
                    .ok_or_else(|| format!("{word} wants a value"))
                    .map(|v| v.to_string())
            };
            match *word {
                "--seconds" => settings.seconds = value()?.parse().map_err(|_| "--seconds")?,
                "--seed" => settings.seed = value()?.parse().map_err(|_| "--seed")?,
                "--dir" => settings.dir = PathBuf::from(value()?),
                "--agents" => settings.agents = value()?.parse().map_err(|_| "--agents")?,
                "--budget" => settings.budget = true,
                "--keep" => settings.keep = true,
                "--long" => settings.seconds = 600,
                other => return Err(format!("unknown option {other}")),
            }
        }
        Ok(settings)
    }
}

/// The budget of a hook from its start to its exit.
pub const HOOK_BUDGET: Duration = Duration::from_millis(100);

/// How many hooks may run at once; a burst finding more running is skipped.
const LIVE_HOOKS: usize = 64;

/// One look at a process: seconds into the run, open files, resident KiB.
pub type Sample = (f64, u64, u64);

/// The looks at each watched process, by pid.
type Samples = Arc<Mutex<HashMap<u32, Vec<Sample>>>>;

/// The free space on the run's disk under which the run stops, before the disk fills.
pub const LOW_DISK: u64 = 2 << 30;

/// A long-lived process the conductor started.
struct Live {
    role: &'static str,
    child: Child,
}

pub async fn run(settings: Settings) -> Result<(), String> {
    let dir = settings.dir.clone();
    let _ = std::fs::remove_dir_all(&dir);
    for sub in ["journal", "ckpt", "sock", "logs"] {
        std::fs::create_dir_all(dir.join(sub)).map_err(|e| e.to_string())?;
    }
    println!(
        "kv-sim: seed {} for {} s in {}",
        settings.seed,
        settings.seconds,
        dir.display()
    );
    let mut rng = Rng::new(settings.seed);
    let roles: Vec<&'static str> = std::iter::repeat_n("agent", settings.agents)
        .chain(["reader", "reader", "indexer", "keeper"])
        .collect();
    let mut live: Vec<Live> = Vec::new();
    for role in &roles {
        live.push(Live {
            role,
            child: spawn(&dir, role, &[rng.next_u64().to_string()])?,
        });
    }

    let samples: Samples = Arc::default();
    let watched: Arc<Mutex<Vec<u32>>> = Arc::default();
    let least_free = Arc::new(AtomicU64::new(u64::MAX));
    let sampler = tokio::spawn(sample(
        dir.clone(),
        Arc::clone(&watched),
        Arc::clone(&samples),
        Arc::clone(&least_free),
    ));

    let started = Instant::now();
    let deadline = started + Duration::from_secs(settings.seconds);
    let mut hooks: JoinSet<HookRun> = JoinSet::new();
    let mut hook_times: Vec<HookRun> = Vec::new();
    // When the owner was stopped and resumed, and when it was killed.
    let mut stops: Vec<(Instant, Instant)> = Vec::new();
    let mut kills: Vec<Instant> = Vec::new();
    let mut hook_pids: VecDeque<u32> = VecDeque::new();
    let mut events: VecDeque<u64> = VecDeque::new();
    let mut next_event = 0u64;
    let mut stopped: Option<(u32, Instant)> = None;
    let mut counts: HashMap<&'static str, u64> = HashMap::new();
    let (mut next_burst, mut next_kill, mut next_stop) = (
        Instant::now(),
        Instant::now() + Duration::from_millis(rng.between(1_500, 2_500)),
        Instant::now() + Duration::from_millis(rng.between(6_000, 10_000)),
    );

    while Instant::now() < deadline && least_free.load(Ordering::Relaxed) >= LOW_DISK {
        let now = Instant::now();
        // Start again what was killed or ended.
        for process in live.iter_mut() {
            if matches!(process.child.try_wait(), Ok(Some(_))) {
                *counts.entry("restarts").or_default() += 1;
                process.child = spawn(&dir, process.role, &[rng.next_u64().to_string()])?;
            }
        }
        *watched.lock().expect("no panic holding it") =
            live.iter().filter_map(|p| p.child.id()).collect();
        while let Some(done) = hooks.try_join_next() {
            if let Ok(done) = done {
                hook_times.push(done);
            }
        }
        if now >= next_burst {
            next_burst = now + Duration::from_millis(rng.between(20, 50));
            for _ in 0..rng.between(1, 8) {
                if hooks.len() >= LIVE_HOOKS {
                    *counts.entry("hooks_skipped").or_default() += 1;
                    continue;
                }
                // A tenth of the events come again, as a host repeats an event.
                let event = if !events.is_empty() && rng.chance(10) {
                    events[rng.below(events.len() as u64) as usize]
                } else {
                    next_event += 1;
                    events.push_back(next_event);
                    if events.len() > 1_000 {
                        events.pop_front();
                    }
                    next_event
                };
                let args = [rng.next_u64().to_string(), event.to_string()];
                let mut child = spawn(&dir, "hook", &args)?;
                if let Some(pid) = child.id() {
                    hook_pids.push_back(pid);
                    if hook_pids.len() > 64 {
                        hook_pids.pop_front();
                    }
                }
                let spawned = Instant::now();
                hooks.spawn(async move {
                    let status = child.wait().await;
                    HookRun {
                        spawned,
                        took: spawned.elapsed(),
                        ok: status.is_ok_and(|s| s.success()),
                    }
                });
            }
        }
        if let Some((pid, until)) = stopped
            && now >= until
        {
            signal("CONT", pid).await;
            stopped = None;
            if let Some(stop) = stops.last_mut() {
                stop.1 = Instant::now();
            }
        }
        if now >= next_stop && stopped.is_none() {
            next_stop = now + Duration::from_millis(rng.between(6_000, 10_000));
            if let Ok(pid) = builder(&dir).current_owner().await {
                signal("STOP", pid).await;
                *counts.entry("owner_stopped").or_default() += 1;
                let until = Instant::now() + Duration::from_millis(rng.between(1_000, 3_000));
                stopped = Some((pid, until));
                stops.push((Instant::now(), until));
            }
        }
        if now >= next_kill {
            next_kill = now + Duration::from_millis(rng.between(1_500, 2_500));
            // A process picked at random may be the owner: its kill is the owner's.
            let owner = builder(&dir).current_owner().await.ok();
            let victim = if rng.chance(60) {
                owner.map(|pid| ("owner", pid))
            } else {
                let mut pids: Vec<u32> = live.iter().filter_map(|p| p.child.id()).collect();
                pids.extend(hook_pids.iter().copied());
                (!pids.is_empty()).then(|| {
                    let pid = pids[rng.below(pids.len() as u64) as usize];
                    (if owner == Some(pid) { "owner" } else { "any" }, pid)
                })
            };
            if let Some((kind, pid)) = victim {
                signal("KILL", pid).await;
                if kind == "owner" {
                    kills.push(Instant::now());
                }
                *counts
                    .entry(if kind == "owner" {
                        "owner_killed"
                    } else {
                        "other_killed"
                    })
                    .or_default() += 1;
                if stopped.is_some_and(|(stopped, _)| stopped == pid) {
                    stopped = None;
                }
            }
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }

    // Stop: resume what is stopped, ask the long-lived roles to end, wait for the hooks.
    if let Some((pid, _)) = stopped {
        signal("CONT", pid).await;
        if let Some(stop) = stops.last_mut() {
            stop.1 = Instant::now();
        }
    }
    std::fs::write(dir.join("stop"), b"").map_err(|e| e.to_string())?;
    while let Some(done) = hooks.join_next().await {
        if let Ok(done) = done {
            hook_times.push(done);
        }
    }
    let mut unended = 0;
    for process in live.iter_mut() {
        match tokio::time::timeout(Duration::from_secs(40), process.child.wait()).await {
            Ok(_) => {}
            Err(_) => {
                unended += 1;
                let _ = process.child.kill().await;
            }
        }
    }
    sampler.abort();
    let elapsed = started.elapsed();

    let sockets = std::fs::read_dir(dir.join("sock")).map_or(0, |entries| entries.count());
    let findings = check::run(&dir).await?;
    // A hook's time counts against the budget unless the owner was stopped while it ran
    // or killed within the second before it ended.
    let hook_times = hook_times
        .into_iter()
        .map(|hook| {
            let ended = hook.spawned + hook.took;
            let stopped = stops
                .iter()
                .any(|(from, to)| hook.spawned < *to && ended > *from);
            let handed_over = kills
                .iter()
                .any(|kill| ended > *kill && hook.spawned < *kill + Duration::from_secs(1));
            let when = if stopped {
                When::OwnerStopped
            } else if handed_over {
                When::OwnerKilled
            } else {
                When::Calm
            };
            (hook.took, hook.ok, when)
        })
        .collect();
    let run = Run {
        seed: settings.seed,
        elapsed,
        hook_times,
        counts,
        samples: std::mem::take(&mut *samples.lock().expect("no panic holding it")),
        least_free: least_free.load(Ordering::Relaxed),
        sockets,
        unended,
        budget: settings.budget,
    };
    let failed = report::print(&dir, &run, &findings)?;
    if failed {
        Err(format!(
            "the run failed; its files are in {}",
            dir.display()
        ))
    } else {
        if !settings.keep {
            let _ = std::fs::remove_dir_all(&dir);
        }
        Ok(())
    }
}

/// A hook as the conductor saw it: when it started, how long it ran, whether it ended
/// well.
struct HookRun {
    spawned: Instant,
    took: Duration,
    ok: bool,
}

/// What the owner went through while a hook ran.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum When {
    Calm,
    OwnerKilled,
    OwnerStopped,
}

/// Start `kv-sim <role> <dir> <args…>`, its errors in `<dir>/logs`.
fn spawn(dir: &Path, role: &str, args: &[String]) -> Result<Child, String> {
    let exe = std::env::current_exe().map_err(|e| e.to_string())?;
    let log = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(dir.join("logs").join(format!("{role}.err")))
        .map_err(|e| e.to_string())?;
    Command::new(exe)
        .arg(role)
        .arg(dir)
        .args(args)
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(log)
        .kill_on_drop(true)
        .spawn()
        .map_err(|e| e.to_string())
}

async fn signal(signal: &str, pid: u32) {
    let _ = Command::new("kill")
        .arg(format!("-{signal}"))
        .arg(pid.to_string())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .await;
}

/// Every second, the free space on the run's disk, the least of which goes to
/// `least_free`, and the open files and the resident memory (KiB) of each watched process.
async fn sample(
    dir: PathBuf,
    watched: Arc<Mutex<Vec<u32>>>,
    samples: Samples,
    least_free: Arc<AtomicU64>,
) {
    let started = Instant::now();
    loop {
        tokio::time::sleep(Duration::from_secs(1)).await;
        if let Some(free) = free_bytes(&dir).await {
            least_free.fetch_min(free, Ordering::Relaxed);
        }
        let pids = watched.lock().expect("no panic holding it").clone();
        for pid in pids {
            let files = Command::new("lsof")
                .args(["-n", "-P", "-p", &pid.to_string()])
                .stderr(Stdio::null())
                .output()
                .await
                .map_or(0, |out| {
                    out.stdout.split(|b| *b == b'\n').count().saturating_sub(2) as u64
                });
            let rss = Command::new("ps")
                .args(["-o", "rss=", "-p", &pid.to_string()])
                .output()
                .await
                .ok()
                .and_then(|out| String::from_utf8_lossy(&out.stdout).trim().parse().ok())
                .unwrap_or(0);
            if files > 0 {
                samples
                    .lock()
                    .expect("no panic holding it")
                    .entry(pid)
                    .or_default()
                    .push((started.elapsed().as_secs_f64(), files, rss));
            }
        }
    }
}

/// The bytes free on the disk `dir` is on, as `df` in its portable form tells them.
async fn free_bytes(dir: &Path) -> Option<u64> {
    let out = Command::new("df").arg("-Pk").arg(dir).output().await.ok()?;
    let text = String::from_utf8_lossy(&out.stdout);
    let kib: u64 = text
        .lines()
        .nth(1)?
        .split_whitespace()
        .nth(3)?
        .parse()
        .ok()?;
    Some(kib * 1024)
}
