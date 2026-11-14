//! The client's side: one connection to the owner. The requests of every task of the
//! process travel on it at once, and each answer finds its caller by the request's id.
//!
//! Frames are written by a task of the connection's own, in the order callers queue
//! them. A caller that stops waiting, on a timeout or because its future is dropped,
//! leaves its frame to be written whole: a frame cut short would break the connection
//! for every task of the process.

use std::collections::HashMap;
use std::io;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use tokio::io::{AsyncWriteExt as _, BufReader};
use tokio::net::UnixStream;
use tokio::net::unix::{OwnedReadHalf, OwnedWriteHalf};
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;

use crate::protocol::{Answer, Request, RequestId, Welcome, encode_frame, read_frame};

/// Why a request got no answer: the connection to the owner is gone.
#[derive(Debug)]
pub struct Lost(pub String);

/// The callers waiting for an answer, by request id.
type Callers = HashMap<RequestId, oneshot::Sender<Answer>>;

/// The callers waiting for an answer; `None` once the connection ended.
type Waiting = Mutex<Option<Callers>>;

/// A connection to the owner.
pub struct Connection {
    /// Frames to write, in their order.
    frames: mpsc::UnboundedSender<Vec<u8>>,
    waiting: Arc<Waiting>,
    next: AtomicU64,
    reader: JoinHandle<()>,
    writer: JoinHandle<()>,
}

impl Connection {
    /// Read the owner's welcome on `stream` within `within`, then read its answers and
    /// write the requests in tasks of their own. Must be called on a Tokio runtime.
    pub async fn open(stream: UnixStream, within: Duration) -> io::Result<(Connection, Welcome)> {
        let (read, write) = stream.into_split();
        let mut reader = BufReader::new(read);
        let welcome: Welcome = tokio::time::timeout(within, read_frame(&mut reader))
            .await
            .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "no welcome in time"))??;
        let waiting: Arc<Waiting> = Arc::new(Mutex::new(Some(HashMap::new())));
        let (frames, queued) = mpsc::unbounded_channel();
        let connection = Connection {
            frames,
            reader: tokio::spawn(read_answers(reader, Arc::clone(&waiting))),
            writer: tokio::spawn(write_frames(write, queued, Arc::clone(&waiting))),
            waiting,
            next: AtomicU64::new(1),
        };
        Ok((connection, welcome))
    }

    /// Send `request` and wait for its answer. Dropping the returned future stops the
    /// wait, not the request: it reaches the owner whole.
    pub async fn call(&self, request: Request) -> Result<Answer, Lost> {
        let id = self.next.fetch_add(1, Ordering::Relaxed);
        let (answer, answered) = oneshot::channel();
        match lock(&self.waiting).as_mut() {
            Some(waiting) => waiting.insert(id, answer),
            None => return Err(ended()),
        };
        if let Err(lost) = self.queue(id, &request) {
            if let Some(waiting) = lock(&self.waiting).as_mut() {
                waiting.remove(&id);
            }
            return Err(lost);
        }
        answered.await.map_err(|_| ended())
    }

    /// Send `request`, which has no answer, without waiting.
    pub async fn send(&self, request: Request) -> Result<(), Lost> {
        let id = self.next.fetch_add(1, Ordering::Relaxed);
        self.queue(id, &request)
    }

    /// Queue the frame of `request` for the writer.
    fn queue(&self, id: RequestId, request: &Request) -> Result<(), Lost> {
        let frame = encode_frame(&(id, request)).map_err(|error| Lost(error.to_string()))?;
        self.frames.send(frame).map_err(|_| ended())
    }
}

impl Drop for Connection {
    fn drop(&mut self) {
        self.reader.abort();
        self.writer.abort();
    }
}

fn lock(waiting: &Waiting) -> MutexGuard<'_, Option<Callers>> {
    waiting.lock().expect("no thread panics holding it")
}

/// Hand each answer to the caller waiting for its id until the connection ends, then
/// wake every caller still waiting with the end.
async fn read_answers(mut reader: BufReader<OwnedReadHalf>, waiting: Arc<Waiting>) {
    while let Ok((id, answer)) = read_frame::<_, (RequestId, Answer)>(&mut reader).await {
        let caller = lock(&waiting)
            .as_mut()
            .and_then(|waiting| waiting.remove(&id));
        if let Some(caller) = caller {
            let _ = caller.send(answer);
        }
    }
    // Dropping the senders ends every wait with an error.
    lock(&waiting).take();
}

/// Write each queued frame whole, in its order, until the queue closes. A write that
/// fails ends the connection: every caller still waiting learns it is lost.
async fn write_frames(
    mut writer: OwnedWriteHalf,
    mut queued: mpsc::UnboundedReceiver<Vec<u8>>,
    waiting: Arc<Waiting>,
) {
    while let Some(frame) = queued.recv().await {
        if writer.write_all(&frame).await.is_err() {
            lock(&waiting).take();
            return;
        }
    }
}

fn ended() -> Lost {
    Lost("the connection to the owner ended".into())
}
