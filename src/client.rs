//! The client's side: one connection to the owner. The requests of every task of the
//! process travel on it at once, and each answer finds its caller by the request's id.

use std::collections::HashMap;
use std::io;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::io::BufReader;
use tokio::net::UnixStream;
use tokio::net::unix::{OwnedReadHalf, OwnedWriteHalf};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

use crate::protocol::{Answer, Request, RequestId, Welcome, read_frame, write_frame};

/// Why a request got no answer: the connection to the owner is gone.
#[derive(Debug)]
pub struct Lost(pub String);

/// The callers waiting for an answer, by request id; `None` once the connection ended.
type Waiting = Mutex<Option<HashMap<RequestId, oneshot::Sender<Answer>>>>;

/// A connection to the owner.
pub struct Connection {
    writer: tokio::sync::Mutex<OwnedWriteHalf>,
    waiting: Arc<Waiting>,
    next: AtomicU64,
    reader: JoinHandle<()>,
}

impl Connection {
    /// Read the owner's welcome on `stream` within `within`, then read its answers in a
    /// task of their own. Must be called on a Tokio runtime.
    pub async fn open(stream: UnixStream, within: Duration) -> io::Result<(Connection, Welcome)> {
        let (read, write) = stream.into_split();
        let mut reader = BufReader::new(read);
        let welcome: Welcome = tokio::time::timeout(within, read_frame(&mut reader))
            .await
            .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "no welcome in time"))??;
        let waiting: Arc<Waiting> = Arc::new(Mutex::new(Some(HashMap::new())));
        let reader = tokio::spawn(read_answers(reader, Arc::clone(&waiting)));
        let connection = Connection {
            writer: tokio::sync::Mutex::new(write),
            waiting,
            next: AtomicU64::new(1),
            reader,
        };
        Ok((connection, welcome))
    }

    /// Send `request` and wait for its answer.
    pub async fn call(&self, request: Request) -> Result<Answer, Lost> {
        let id = self.next.fetch_add(1, Ordering::Relaxed);
        let (answer, answered) = oneshot::channel();
        match self
            .waiting
            .lock()
            .expect("no thread panics holding it")
            .as_mut()
        {
            Some(waiting) => waiting.insert(id, answer),
            None => return Err(ended()),
        };
        if let Err(error) = self.send_frame(id, &request).await {
            if let Some(waiting) = self
                .waiting
                .lock()
                .expect("no thread panics holding it")
                .as_mut()
            {
                waiting.remove(&id);
            }
            return Err(Lost(error.to_string()));
        }
        answered.await.map_err(|_| ended())
    }

    /// Send `request`, which has no answer, without waiting.
    pub async fn send(&self, request: Request) -> Result<(), Lost> {
        let id = self.next.fetch_add(1, Ordering::Relaxed);
        self.send_frame(id, &request)
            .await
            .map_err(|error| Lost(error.to_string()))
    }

    async fn send_frame(&self, id: RequestId, request: &Request) -> io::Result<()> {
        write_frame(&mut *self.writer.lock().await, &(id, request)).await
    }
}

impl Drop for Connection {
    fn drop(&mut self) {
        self.reader.abort();
    }
}

/// Hand each answer to the caller waiting for its id until the connection ends, then
/// wake every caller still waiting with the end.
async fn read_answers(mut reader: BufReader<OwnedReadHalf>, waiting: Arc<Waiting>) {
    while let Ok((id, answer)) = read_frame::<_, (RequestId, Answer)>(&mut reader).await {
        let caller = waiting
            .lock()
            .expect("no thread panics holding it")
            .as_mut()
            .and_then(|waiting| waiting.remove(&id));
        if let Some(caller) = caller {
            let _ = caller.send(answer);
        }
    }
    // Dropping the senders ends every wait with an error.
    waiting.lock().expect("no thread panics holding it").take();
}

fn ended() -> Lost {
    Lost("the connection to the owner ended".into())
}
