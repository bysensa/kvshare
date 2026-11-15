//! The client's side: one connection to the owner. The requests of every task of the
//! process travel on it at once, and each answer finds its caller by the request's id.
//!
//! Frames are written by a task of the connection's own, in the order callers queue
//! them. A caller that stops waiting, on a timeout or because its future is dropped,
//! leaves its frame to be written whole: a frame cut short would break the connection
//! for every task of the process.
//!
//! The frames that wait for the writer are bounded. An owner that is stopped reads
//! nothing, and a client that goes on asking would otherwise keep every request in
//! memory until the owner resumes. A frame is queued while fewer than [`MAX_QUEUED`]
//! bytes wait; one that finds no room waits for it, which its caller bounds with the
//! call timeout, and is not sent if the caller gives up first.
//!
//! A commit whose caller stopped waiting while the owner has not answered it is in
//! doubt: it may land at any time. The connection knows whether one is, so that a
//! transaction begun meanwhile can ask the owner to begin it after those commits.

use std::collections::{HashMap, HashSet};
use std::io;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use tokio::io::{AsyncWriteExt as _, BufReader};
use tokio::net::UnixStream;
use tokio::net::unix::{OwnedReadHalf, OwnedWriteHalf};
use tokio::sync::{Notify, mpsc, oneshot};
use tokio::task::JoinHandle;

use crate::protocol::{
    Answer, Hello, Request, RequestId, Unframed, Welcome, agreed, encode_frame, read_frame,
};

/// How many bytes of frames may wait for the writer before another frame has to wait
/// for room. A frame is taken while fewer wait, so at most this and one frame do.
pub const MAX_QUEUED: usize = 64 << 20;

/// Why a request got no answer: the connection to the owner is gone.
#[derive(Debug)]
pub struct Lost(pub String);

/// A request as its frame, under the id its answer will carry; not sent yet.
pub struct Frame {
    id: RequestId,
    bytes: Vec<u8>,
    /// Whether the request is a commit.
    commit: bool,
}

/// The requests the owner has not answered yet.
#[derive(Default)]
struct Unanswered {
    /// The callers waiting for an answer, by request id.
    callers: HashMap<RequestId, oneshot::Sender<Answer>>,
    /// The commits whose callers stopped waiting.
    in_doubt: HashSet<RequestId>,
}

/// The requests the owner has not answered yet; `None` once the connection ended.
type Waiting = Mutex<Option<Unanswered>>;

/// The frames queued for the writer and not written yet.
#[derive(Default)]
struct Queued {
    bytes: AtomicUsize,
    /// Told whenever the writer has written a frame, and when the connection ends.
    written: Notify,
}

/// A connection to the owner.
pub struct Connection {
    /// Frames to write, in their order.
    frames: mpsc::UnboundedSender<Vec<u8>>,
    queued: Arc<Queued>,
    waiting: Arc<Waiting>,
    next: AtomicU64,
    reader: JoinHandle<()>,
    writer: JoinHandle<()>,
}

/// The wait for the answer to a frame that was queued. Dropping it stops the wait, not
/// the request: its frame reaches the owner whole.
pub struct Sent<'c> {
    connection: &'c Connection,
    id: RequestId,
    commit: bool,
    answered: oneshot::Receiver<Answer>,
}

impl Sent<'_> {
    /// The owner's answer, or the end of the connection.
    pub async fn answer(mut self) -> Result<Answer, Lost> {
        (&mut self.answered).await.map_err(|_| ended())
    }
}

impl Drop for Sent<'_> {
    fn drop(&mut self) {
        // A caller that stopped waiting leaves no entry behind for an answer that an
        // owner stopped for good never sends. A commit still unanswered is in doubt
        // from here on, until its answer comes.
        if let Some(waiting) = lock(&self.connection.waiting).as_mut()
            && waiting.callers.remove(&self.id).is_some()
            && self.commit
        {
            waiting.in_doubt.insert(self.id);
        }
    }
}

impl Connection {
    /// Read the owner's welcome on `stream` within `within`, then read its answers and
    /// write the requests in tasks of their own. If the owner speaks a version of the
    /// protocol this process speaks, the first frame written tells it which. Must be
    /// called on a Tokio runtime.
    pub async fn open(stream: UnixStream, within: Duration) -> io::Result<(Connection, Welcome)> {
        let (read, write) = stream.into_split();
        let mut reader = BufReader::new(read);
        let welcome: Welcome = tokio::time::timeout(within, read_frame(&mut reader))
            .await
            .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "no welcome in time"))??;
        let waiting: Arc<Waiting> = Arc::new(Mutex::new(Some(Unanswered::default())));
        let queued = Arc::new(Queued::default());
        let (frames, to_write) = mpsc::unbounded_channel();
        if let Some(version) = agreed(welcome.oldest, welcome.version) {
            let hello = encode_frame(&Hello { version }).map_err(io::Error::from)?;
            queued.bytes.fetch_add(hello.len(), Ordering::AcqRel);
            let _ = frames.send(hello);
        }
        let connection = Connection {
            frames,
            reader: tokio::spawn(read_answers(
                reader,
                Arc::clone(&waiting),
                Arc::clone(&queued),
            )),
            writer: tokio::spawn(write_frames(
                write,
                to_write,
                Arc::clone(&waiting),
                Arc::clone(&queued),
            )),
            queued,
            waiting,
            next: AtomicU64::new(1),
        };
        Ok((connection, welcome))
    }

    /// The frame of `request`, under a new id. Nothing is sent: a request that no frame
    /// holds is known here, while its transaction can still take its writes back.
    pub fn frame(&self, request: &Request) -> Result<Frame, Unframed> {
        let id = self.next.fetch_add(1, Ordering::Relaxed);
        Ok(Frame {
            id,
            bytes: encode_frame(&(id, request))?,
            commit: matches!(request, Request::Commit { .. }),
        })
    }

    /// Whether a commit sent on this connection is in doubt: its caller stopped
    /// waiting, and the owner has not answered it.
    pub fn commit_in_doubt(&self) -> bool {
        lock(&self.waiting)
            .as_ref()
            .is_some_and(|waiting| !waiting.in_doubt.is_empty())
    }

    /// Queue `frame` once the frames before it leave room, and hand back the wait for
    /// its answer. A caller bounds the wait for room with its timeout: nothing is sent
    /// if the returned future is dropped before it ends.
    pub async fn queue(&self, frame: Frame) -> Result<Sent<'_>, Lost> {
        self.room(frame.bytes.len()).await?;
        // Room is taken: from here to the queue nothing waits, so a caller that gives
        // up cannot leave the room taken and the frame unsent.
        let (answer, answered) = oneshot::channel();
        match lock(&self.waiting).as_mut() {
            Some(waiting) => waiting.callers.insert(frame.id, answer),
            None => {
                self.give_back(frame.bytes.len());
                return Err(ended());
            }
        };
        let sent = Sent {
            connection: self,
            id: frame.id,
            commit: frame.commit,
            answered,
        };
        self.push(frame.bytes)?;
        Ok(sent)
    }

    /// Queue `frame`, which has no answer, once the frames before it leave room.
    pub async fn post(&self, frame: Frame) -> Result<(), Lost> {
        self.room(frame.bytes.len()).await?;
        self.push(frame.bytes)
    }

    /// Queue `request`, which has no answer, without waiting, and say whether it was
    /// queued: not when the connection ended, and not when as much as may wait is
    /// waiting, unless the request goes `whatever_waits`. It needs no runtime: a
    /// transaction dropped on any thread rolls back through it.
    pub fn tell(&self, request: &Request, whatever_waits: bool) -> bool {
        let Ok(frame) = self.frame(request) else {
            return false;
        };
        if whatever_waits {
            self.queued
                .bytes
                .fetch_add(frame.bytes.len(), Ordering::AcqRel);
        } else if !self.reserve(frame.bytes.len()) {
            return false;
        }
        self.push(frame.bytes).is_ok()
    }

    /// Wait until fewer than [`MAX_QUEUED`] bytes wait for the writer, and count
    /// `bytes` more among them.
    async fn room(&self, bytes: usize) -> Result<(), Lost> {
        loop {
            // Listening before the look, so that a frame written between the look and
            // the wait is not missed.
            let written = self.queued.written.notified();
            tokio::pin!(written);
            written.as_mut().enable();
            if lock(&self.waiting).is_none() {
                return Err(ended());
            }
            if self.reserve(bytes) {
                return Ok(());
            }
            written.await;
        }
    }

    /// Count `bytes` more among those that wait, if fewer than [`MAX_QUEUED`] do.
    fn reserve(&self, bytes: usize) -> bool {
        self.queued
            .bytes
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |waiting| {
                (waiting < MAX_QUEUED).then_some(waiting + bytes)
            })
            .is_ok()
    }

    /// Take back the room of `bytes` that will not be queued after all.
    fn give_back(&self, bytes: usize) {
        self.queued.bytes.fetch_sub(bytes, Ordering::AcqRel);
        self.queued.written.notify_waiters();
    }

    /// Hand a frame whose room is taken to the writer.
    fn push(&self, frame: Vec<u8>) -> Result<(), Lost> {
        let bytes = frame.len();
        self.frames.send(frame).map_err(|_| {
            self.give_back(bytes);
            ended()
        })
    }
}

impl Drop for Connection {
    fn drop(&mut self) {
        self.reader.abort();
        self.writer.abort();
    }
}

fn lock(waiting: &Waiting) -> MutexGuard<'_, Option<Unanswered>> {
    waiting.lock().expect("no thread panics holding it")
}

/// Hand each answer to the caller waiting for its id until the connection ends, then
/// wake every caller still waiting with the end.
async fn read_answers(
    mut reader: BufReader<OwnedReadHalf>,
    waiting: Arc<Waiting>,
    queued: Arc<Queued>,
) {
    while let Ok((id, answer)) = read_frame::<_, (RequestId, Answer)>(&mut reader).await {
        let caller = lock(&waiting).as_mut().and_then(|waiting| {
            // An answer nobody waits for settles a commit that was in doubt.
            waiting.in_doubt.remove(&id);
            waiting.callers.remove(&id)
        });
        if let Some(caller) = caller {
            let _ = caller.send(answer);
        }
    }
    // Dropping the senders ends every wait with an error; a caller waiting for room
    // learns of the end too.
    lock(&waiting).take();
    queued.written.notify_waiters();
}

/// Write each queued frame whole, in its order, until the queue closes. A write that
/// fails ends the connection: every caller still waiting learns it is lost.
async fn write_frames(
    mut writer: OwnedWriteHalf,
    mut frames: mpsc::UnboundedReceiver<Vec<u8>>,
    waiting: Arc<Waiting>,
    queued: Arc<Queued>,
) {
    while let Some(frame) = frames.recv().await {
        let written = writer.write_all(&frame).await;
        queued.bytes.fetch_sub(frame.len(), Ordering::AcqRel);
        if written.is_err() {
            lock(&waiting).take();
            queued.written.notify_waiters();
            return;
        }
        queued.written.notify_waiters();
    }
}

fn ended() -> Lost {
    Lost("the connection to the owner ended".into())
}

#[cfg(test)]
mod tests {
    use tokio::io::AsyncReadExt as _;

    use super::*;
    use crate::protocol::{MAX_FRAME, VERSION, Write, write_frame};
    use crate::types::WireDurability;

    /// A connection to a peer that welcomes it, takes its hello, and then reads only
    /// when told to.
    async fn connection() -> (Connection, UnixStream) {
        let (ours, mut theirs) = UnixStream::pair().expect("a pair of sockets");
        let welcome = Welcome {
            oldest: VERSION,
            version: VERSION,
            db: "/nowhere".into(),
            owner_pid: 1,
            fingerprint: [0; 32],
        };
        write_frame(&mut theirs, &welcome).await.expect("a welcome");
        let (connection, _) = Connection::open(ours, Duration::from_secs(1))
            .await
            .expect("a connection");
        let hello: Hello = read_frame(&mut theirs).await.expect("the client's hello");
        assert_eq!(hello, Hello { version: VERSION });
        (connection, theirs)
    }

    /// Frames for an owner that reads nothing wait only up to the bound: the next one
    /// waits for room and is not sent when its caller gives up. A rollback is queued
    /// whatever waits. Once the owner reads, there is room again.
    #[tokio::test]
    async fn frames_wait_for_room_while_the_owner_reads_nothing() {
        let (connection, mut theirs) = connection().await;
        let big = Request::Writes {
            tx: 1,
            writes: vec![Write::Set {
                key: b"k".to_vec(),
                value: vec![0; 8 << 20],
            }],
        };
        let mut queued = 0;
        loop {
            let frame = connection.frame(&big).expect("a frame");
            match tokio::time::timeout(Duration::from_millis(50), connection.post(frame)).await {
                Ok(posted) => posted.expect("the connection lives"),
                Err(_) => break,
            }
            queued += 1;
            assert!(queued < 100, "the queue takes frames without end");
        }
        assert_eq!(
            queued,
            MAX_QUEUED / (8 << 20),
            "frames of 8 MiB up to the bound"
        );
        let waiting = connection.queued.bytes.load(Ordering::Acquire);
        assert!(waiting <= MAX_QUEUED + MAX_FRAME as usize, "{waiting} wait");

        assert!(
            !connection.tell(&big, false),
            "no room without waiting either"
        );
        assert!(
            connection.tell(&Request::Rollback { tx: 1 }, true),
            "a rollback is queued whatever waits"
        );

        let reading = tokio::spawn(async move {
            let mut sink = vec![0u8; 1 << 20];
            while theirs.read(&mut sink).await.is_ok_and(|read| read > 0) {}
        });
        let frame = connection.frame(&big).expect("a frame");
        tokio::time::timeout(Duration::from_secs(10), connection.post(frame))
            .await
            .expect("room once the owner reads")
            .expect("the connection lives");
        reading.abort();
    }

    /// A caller that stops waiting for an answer leaves nothing behind in the table of
    /// callers.
    #[tokio::test]
    async fn a_caller_that_gives_up_leaves_no_entry() {
        let (connection, _theirs) = connection().await;
        let frame = connection
            .frame(&Request::Rollback { tx: 1 })
            .expect("a frame");
        let callers = || {
            lock(&connection.waiting)
                .as_ref()
                .map(|waiting| waiting.callers.len())
        };
        let sent = connection.queue(frame).await.expect("queued");
        assert_eq!(callers(), Some(1));
        drop(sent);
        assert_eq!(callers(), Some(0));
    }

    /// A commit is in doubt from the moment its caller stops waiting until the owner
    /// answers it; a commit that is waited for is not, and neither is a read that was
    /// given up on.
    #[tokio::test]
    async fn a_commit_its_caller_gave_up_on_is_in_doubt_until_answered() {
        let (connection, mut theirs) = connection().await;
        let read = Request::Get {
            tx: 1,
            writes: Vec::new(),
            key: b"k".to_vec(),
            for_update: false,
        };
        let sent = connection
            .queue(connection.frame(&read).expect("a frame"))
            .await
            .expect("queued");
        drop(sent);
        assert!(!connection.commit_in_doubt(), "a read is never in doubt");

        let commit = Request::Commit {
            tx: 1,
            writes: Vec::new(),
            durability: WireDurability::Eventual,
        };
        let sent = connection
            .queue(connection.frame(&commit).expect("a frame"))
            .await
            .expect("queued");
        assert!(!connection.commit_in_doubt(), "its caller waits for it");
        drop(sent);
        assert!(connection.commit_in_doubt(), "its caller gave up");

        // The owner reads both requests and answers the commit.
        let _: (RequestId, Request) = read_frame(&mut theirs).await.expect("the read");
        let (id, _): (RequestId, Request) = read_frame(&mut theirs).await.expect("the commit");
        write_frame(&mut theirs, &(id, Answer::Committed))
            .await
            .expect("an answer");
        for _ in 0..200 {
            if !connection.commit_in_doubt() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        assert!(!connection.commit_in_doubt(), "the answer settles it");
    }
}
