//! What kvshare says through `tracing`: who owns the database, through whom a process
//! works, and what became of an owner.

mod common;

use std::fmt::Debug;
use std::sync::{Arc, Mutex};

use common::{base, builder};
use tracing::field::{Field, Visit};
use tracing::{Event, Metadata, Subscriber, span};

/// A subscriber that keeps kvshare's events as `LEVEL message field=value ...`.
#[derive(Clone, Default)]
struct Seen(Arc<Mutex<Vec<String>>>);

impl Seen {
    /// Whether an event at `level` whose message holds `words` was seen.
    fn has(&self, level: &str, words: &str) -> bool {
        let seen = self.0.lock().expect("no panic holding it");
        seen.iter()
            .any(|event| event.starts_with(level) && event.contains(words))
    }
}

impl Subscriber for Seen {
    fn enabled(&self, metadata: &Metadata<'_>) -> bool {
        metadata.target().starts_with("kvshare")
    }

    fn new_span(&self, _: &span::Attributes<'_>) -> span::Id {
        span::Id::from_u64(1)
    }

    fn record(&self, _: &span::Id, _: &span::Record<'_>) {}

    fn record_follows_from(&self, _: &span::Id, _: &span::Id) {}

    fn event(&self, event: &Event<'_>) {
        struct Words(String, String);
        impl Visit for Words {
            fn record_debug(&mut self, field: &Field, value: &dyn Debug) {
                if field.name() == "message" {
                    self.0 = format!("{value:?}");
                } else {
                    self.1.push_str(&format!(" {}={value:?}", field.name()));
                }
            }
        }
        let mut words = Words(String::new(), String::new());
        event.record(&mut words);
        let line = format!("{} {}{}", event.metadata().level(), words.0, words.1);
        self.0.lock().expect("no panic holding it").push(line);
    }

    fn enter(&self, _: &span::Id) {}

    fn exit(&self, _: &span::Id) {}
}

/// A process says when it comes to own the database, when it works through an owner,
/// when it hands the database over, and when it finds its owner lost. The runtime has
/// one thread, so the test's subscriber sees every task's events.
#[tokio::test]
async fn the_roles_of_a_process_are_traced() {
    let seen = Seen::default();
    let _subscribed = tracing::subscriber::set_default(seen.clone());
    let base = base("events");

    let owner = builder(&base).build().await.expect("the owner");
    assert!(seen.has("INFO", "owns the database"), "{:?}", seen.0);
    let client = builder(&base).build().await.expect("the client");
    assert!(seen.has("INFO", "works through the owner"), "{:?}", seen.0);

    owner.close().await.expect("the owner leaves");
    assert!(seen.has("INFO", "handed the database over"), "{:?}", seen.0);
    let tx = loop {
        match client.begin().await {
            Ok(tx) if tx.is_local() => break tx,
            _ => tokio::task::yield_now().await,
        }
    };
    drop(tx);
    assert!(seen.has("WARN", "the owner is lost"), "{:?}", seen.0);
    let owned = seen
        .0
        .lock()
        .expect("no panic holding it")
        .iter()
        .filter(|event| event.contains("owns the database"))
        .count();
    assert_eq!(owned, 2, "the first tree, then the second");

    client.close().await.expect("the client closes");
    let _ = std::fs::remove_dir_all(&base);
}
