//! A hook, as `milou follow hook` runs for one event of a host: a process that opens the
//! database, perhaps as its owner, answers the event, moves the event's task along its
//! flow, and exits. An event answered before gets the answer it got then.

use std::path::PathBuf;
use std::time::Instant;

use kvshare::Transaction;

use super::{Ctx, Op, builder};
use crate::journal::Journal;
use crate::keys::{self, Answer, OpId, Status, Task, Transition};
use crate::rng::Rng;
use crate::trace;

/// How many tasks the events of a run share.
pub const TASKS: u64 = 64;

pub async fn run(run: PathBuf, seed: u64, event: u64) -> Result<(), String> {
    let started = Instant::now();
    let mut journal = Journal::open(&run, "hook", 'h').map_err(|e| e.to_string())?;
    let opened = builder(&run).build().await;
    // What the election gave up on and tried again: a connection the system did not
    // answer, and a welcome that did not come.
    for (words, name) in [
        ("no connection in time", "connect_unanswered"),
        ("no welcome in time", "welcome_unanswered"),
    ] {
        for _ in 0..trace::installed().map_or(0, |trace| trace.count(words)) {
            journal.count(name);
        }
    }
    let tree = match opened {
        Ok(tree) => tree,
        Err(error) => {
            journal.count("hook_open_failed");
            journal.time("hook.total", started.elapsed().as_micros(), "unopened");
            // What the hook waited for, for whoever reads the run.
            if let Some(trace) = trace::installed() {
                trace.write();
            }
            return Err(error.to_string());
        }
    };
    let opened = started.elapsed();
    let owner = tree.is_owner().await.unwrap_or(false);
    journal.time(
        "hook.open",
        opened.as_micros(),
        if owner { "owner" } else { "client" },
    );
    let mut ctx = Ctx {
        run,
        tree,
        journal,
        rng: Rng::new(seed),
    };

    let mut answer = AnswerOp {
        event,
        value: ctx.rng.next_u64(),
        found: None,
    };
    ctx.perform("answer", &mut answer).await;
    if let Some(found) = answer.found {
        ctx.journal.seen(event, found);
    }
    if ctx.rng.chance(60) {
        let mut transition = TransitionOp::new(event % TASKS, By::Hook, ctx.rng.next_u64());
        ctx.perform("transition", &mut transition).await;
    }
    // Half the hooks let the database go, and time it: an owner that leaves waits for the
    // work under way in it. The others end as a process does. (The role is the one the
    // hook opened with; one that became the owner on the way shows as a long close of a
    // client.)
    if ctx.rng.chance(50) {
        let closing = Instant::now();
        let _ = ctx.tree.close().await;
        ctx.journal.time(
            "hook.close",
            closing.elapsed().as_micros(),
            if owner { "owner" } else { "client" },
        );
    }
    ctx.journal.time(
        "hook.total",
        started.elapsed().as_micros(),
        if owner { "owner" } else { "client" },
    );
    Ok(())
}

/// Answer an event, unless it has an answer already.
struct AnswerOp {
    event: u64,
    value: u64,
    found: Option<u64>,
}

impl Op for AnswerOp {
    fn describe(&self) -> String {
        format!("answer {} {}", self.event, self.value)
    }

    async fn apply(&mut self, id: OpId, tx: &mut Transaction) -> kvshare::Result<bool> {
        if let Some(stored) = tx.get(keys::answer(self.event)).await? {
            let stored: Answer = keys::decode(&stored).map_err(kvshare::Error::Engine)?;
            self.found = Some(stored.value);
            return Ok(false);
        }
        let answer = Answer {
            op: id,
            value: self.value,
        };
        tx.set(keys::answer(self.event), keys::encode(&answer))?;
        Ok(true)
    }

    async fn landed(&self, id: OpId, tx: &Transaction) -> kvshare::Result<bool> {
        Ok(match tx.get(keys::answer(self.event)).await? {
            Some(stored) => keys::decode::<Answer>(&stored).is_ok_and(|a| a.op == id),
            None => false,
        })
    }
}

/// Who moves a task: a hook or an agent's `continue`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum By {
    Hook,
    Agent,
}

/// Move a task one step along its flow, if the one who moves it has a step to take.
pub struct TransitionOp {
    task: u64,
    by: By,
    coin: u64,
    from: Option<Status>,
    to: Status,
    n: u64,
}

impl TransitionOp {
    pub fn new(task: u64, by: By, coin: u64) -> Self {
        TransitionOp {
            task,
            by,
            coin,
            from: None,
            to: Status::New,
            n: 0,
        }
    }

    /// The step `by` takes from `from`, if any.
    fn step(&self, from: Option<Status>) -> Option<Status> {
        use Status::*;
        match (self.by, from) {
            (By::Hook, None) => Some(New),
            (By::Hook, Some(New)) => Some(Triaged),
            (By::Hook, Some(Triaged)) => Some(Working),
            (By::Hook, Some(Working)) => Some(if self.coin.is_multiple_of(2) {
                Review
            } else {
                Blocked
            }),
            (By::Agent, Some(Blocked)) => Some(Working),
            (By::Agent, Some(Review)) => Some(Done),
            _ => None,
        }
    }
}

impl Op for TransitionOp {
    fn describe(&self) -> String {
        let from = self.from.map_or("-".to_string(), |s| format!("{s:?}"));
        format!("transition {} {from} {:?} {}", self.task, self.to, self.n)
    }

    async fn apply(&mut self, id: OpId, tx: &mut Transaction) -> kvshare::Result<bool> {
        let current = match tx.get_for_update(keys::task(self.task)).await? {
            Some(bytes) => Some(keys::decode::<Task>(&bytes).map_err(kvshare::Error::Engine)?),
            None => None,
        };
        let from = current.map(|task| task.status);
        let Some(to) = self.step(from) else {
            return Ok(false);
        };
        let n = current.map_or(0, |task| task.n) + 1;
        (self.from, self.to, self.n) = (from, to, n);
        tx.set(
            keys::task(self.task),
            keys::encode(&Task {
                status: to,
                n,
                op: id,
            }),
        )?;
        tx.set(
            keys::transition(self.task, n),
            keys::encode(&Transition { from, to, op: id }),
        )?;
        Ok(true)
    }

    async fn landed(&self, id: OpId, tx: &Transaction) -> kvshare::Result<bool> {
        Ok(match tx.get(keys::transition(self.task, self.n)).await? {
            Some(bytes) => keys::decode::<Transition>(&bytes).is_ok_and(|t| t.op == id),
            None => false,
        })
    }
}
