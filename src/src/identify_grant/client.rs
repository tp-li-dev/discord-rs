use super::{REPLY_LEN, RPC_TIMEOUT, Reply, UNAVAILABLE_LOG, VERSION, boot_now, nanos};
use std::{
    io,
    os::{
        fd::{FromRawFd, RawFd},
        unix::net::UnixStream as StdStream,
    },
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::UnixStream,
    sync::{Mutex, oneshot},
    time::{sleep, timeout},
};
use twilight_gateway_queue::{InMemoryQueue, Queue};

/// Apply the local Discord queue first, then acquire one botd permit.
pub struct GrantQueue {
    local: InMemoryQueue,
    channel: Arc<Mutex<Option<UnixStream>>>,
    deadline: Arc<AtomicU64>,
    shard: u32,
}

impl GrantQueue {
    /// Take ownership of the botd socket before starting any other child.
    ///
    /// # Safety
    /// `fd` must be an independently owned socket inherited from botd.
    pub unsafe fn inherited_socket(fd: RawFd) -> io::Result<UnixStream> {
        let flags = unsafe { libc::fcntl(fd, libc::F_GETFD) };
        if fd < 3 || flags < 0 || flags & libc::FD_CLOEXEC != 0 {
            return Err(io::Error::other("invalid inherited Identify socket"));
        }
        if unsafe { libc::fcntl(fd, libc::F_SETFD, flags | libc::FD_CLOEXEC) } < 0 {
            return Err(io::Error::last_os_error());
        }
        let socket = unsafe { StdStream::from_raw_fd(fd) };
        if !socket.peer_addr()?.is_unnamed() {
            return Err(io::Error::other("invalid inherited Identify socket"));
        }
        socket.set_nonblocking(true)?;
        UnixStream::from_std(socket)
    }

    pub fn new(local: InMemoryQueue, socket: UnixStream, shard: u32) -> Self {
        Self {
            local,
            channel: Arc::new(Mutex::new(Some(socket))),
            deadline: Arc::new(AtomicU64::new(0)),
            shard,
        }
    }

    /// One-use check from Twilight, immediately before WebSocket start_send.
    pub fn take_permit(&self) -> bool {
        let deadline = self.deadline.swap(0, Ordering::AcqRel);
        deadline != 0 && boot_now().is_ok_and(|now| nanos(now) < deadline)
    }
}

impl Queue for GrantQueue {
    fn enqueue(&self, shard: u32) -> oneshot::Receiver<()> {
        self.deadline.store(0, Ordering::Release);
        let (mut permit, receiver) = oneshot::channel();
        if shard != self.shard {
            // A misrouted shard stays held and cannot use this capability.
            tokio::spawn(async move { permit.closed().await });
            return receiver;
        }
        let local = self.local.enqueue(shard);
        let channel = Arc::clone(&self.channel);
        let deadline = Arc::clone(&self.deadline);
        tokio::spawn(async move {
            let local_ready = tokio::select! {
                _ = permit.closed() => return,
                result = local => result.is_ok(),
            };
            if local_ready {
                request(channel, deadline, permit).await;
            }
        });
        receiver
    }
}

async fn request(
    channel: Arc<Mutex<Option<UnixStream>>>,
    deadline: Arc<AtomicU64>,
    mut permit: oneshot::Sender<()>,
) {
    loop {
        let mut channel = tokio::select! {
            biased;
            _ = permit.closed() => return,
            channel = channel.lock() => channel,
        };
        let Some(socket) = channel.as_mut() else {
            drop(channel);
            // A new daemon has no endpoint for this process. Keep the queue
            // pending while Twilight continues driving the WebSocket.
            permit.closed().await;
            return;
        };
        // Even if the WebSocket disconnects, finish the bounded exchange
        // before another request uses this stream.
        match timeout(RPC_TIMEOUT, exchange(socket)).await {
            Ok(Ok(Reply::Permit(expires))) => {
                if !permit.is_closed() {
                    deadline.store(expires, Ordering::Release);
                    if permit.send(()).is_err() {
                        deadline.store(0, Ordering::Release);
                    }
                }
                return;
            }
            Ok(Ok(Reply::Wait(delay))) => {
                drop(channel);
                let delay = Duration::from_nanos(delay)
                    .clamp(Duration::from_millis(25), Duration::from_secs(30));
                tokio::select! {
                    _ = permit.closed() => return,
                    _ = sleep(delay) => {}
                }
            }
            _ => {
                *channel = None;
                drop(channel);
                eprintln!("{UNAVAILABLE_LOG}");
                permit.closed().await;
                return;
            }
        }
    }
}

async fn exchange(socket: &mut UnixStream) -> io::Result<Reply> {
    socket.write_all(&[VERSION]).await?;
    let mut bytes = [0; REPLY_LEN];
    socket.read_exact(&mut bytes).await?;
    Ok(Reply::decode(bytes))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn a_permit_is_single_use_and_expired_permits_are_refused_at_the_sink() {
        let (socket, _peer) = UnixStream::pair().unwrap();
        let queue = GrantQueue::new(InMemoryQueue::default(), socket, 0);
        let now = boot_now().unwrap();
        queue
            .deadline
            .store(nanos(now + Duration::from_secs(10)), Ordering::Release);
        assert!(queue.take_permit());
        assert!(!queue.take_permit());
        queue.deadline.store(nanos(now), Ordering::Release);
        assert!(!queue.take_permit());
    }
}
