//! 試験用の Fault Injector / Mock Discord。運用・遠隔管理用ではありません。
//!
//! [`FaultIo`] wraps any `Unpin` Tokio stream, including duplex, TCP, and the
//! separate read/write halves of worker pipes. It adds no forwarding task or
//! unbounded queue. Use a small `tokio::io::duplex(capacity)` to model a full
//! buffer, or [`FaultControl`] to hold/release reads and writes explicitly.
//! Latency/jitter apply once per nonempty transfer attempt, persist across
//! `Pending` and cancellation, and reset after an underlying completion.
//! Bandwidth permits an initial burst, then paces successful bytes; latency and
//! pacing deadlines overlap. Flush/shutdown add no latency or byte quota.
//! A byte cutoff drops the entire underlying connection after exactly that
//! many bytes in the selected direction. The peer may drain already buffered
//! bytes. A control disconnect takes effect when the wrapper is next polled.
//!
//! Seeds reproduce jitter for the same sequence of attempts **per direction**;
//! they do not make TCP packet sizes or concurrent task scheduling deterministic.
//! Use fixed chunks/duplex for replay. Statistics contain only counts/timings.
//!
//! Worker/IPC framing can reuse the current newline JSON reader/writer:
//! ```
//! # async fn example() -> std::io::Result<()> {
//! use src::testkit::{DirectionFaults, FaultConfig, FaultIo};
//! use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
//! let (wire, mut peer) = tokio::io::duplex(64);
//! let mut ipc = FaultIo::new(wire, FaultConfig {
//!     read: DirectionFaults { disconnect_after: Some(10), ..Default::default() },
//!     seed: 15, ..Default::default()
//! })?;
//! peer.write_all(b"{\"ack\":1}\n").await?;
//! let mut frame = String::new();
//! BufReader::new(&mut ipc).read_line(&mut frame).await?;
//! assert_eq!(frame, "{\"ack\":1}\n");
//! # Ok(()) }
//! # tokio::runtime::Runtime::new().unwrap().block_on(example()).unwrap();
//! ```
//!
//! To test REST tracking/drain, send a fixture HTTP request to the returned
//! loopback address, or pass the server side of a duplex to `mock.serve(stream)`.
//! `applied` becomes visible after the complete request and before response delay:
//! ```
//! # async fn example() -> std::io::Result<()> {
//! use src::testkit::{MockDiscord, MockReply};
//! use tokio::io::{AsyncReadExt, AsyncWriteExt};
//! let mock = MockDiscord::new([MockReply::interaction_ack().with_lost_response()]);
//! let server = mock.spawn_tcp().await?;
//! let mut client = tokio::net::TcpStream::connect(server.addr()).await?;
//! // Use fixture IDs/tokens only; never real credentials.
//! client.write_all(b"POST /api/v10/interactions/1/fixture/callback HTTP/1.1\r\nHost: mock\r\nContent-Length: 2\r\n\r\n{}").await?;
//! let mut response = Vec::new();
//! client.read_to_end(&mut response).await?;
//! assert!(response.is_empty());
//! assert_eq!(mock.stats().applied, 1); // Caller still cannot infer success from EOF.
//! server.shutdown().await?;
//! # Ok(()) }
//! # tokio::runtime::Runtime::new().unwrap().block_on(example()).unwrap();
//! ```
//!
//! Mock scope: one HTTP/1.0 or HTTP/1.1 request per connection, Content-Length
//! bodies, JSON responses, no TLS, chunked encoding, WebSocket, authentication,
//! automatic retries, Discord validation or real side effects. Scripts are
//! consumed in order of **complete request receipt**, not TCP accept order.
//! Delayed/lost 2xx responses model an applied operation whose result is unknown
//! to the caller; this does not prove the application's handling of T15 or its
//! Heartbeat/deadline guarantees. Request paths, headers and bodies are neither
//! retained nor logged. Supply synthetic fixtures only.

use std::{
    collections::VecDeque,
    future::Future,
    io,
    net::{Ipv4Addr, SocketAddr},
    num::{NonZeroU64, NonZeroUsize},
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    task::{Context, Poll, Waker},
    time::Duration,
};
use tokio::{
    io::{AsyncBufReadExt, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader, ReadBuf},
    net::TcpListener,
    sync::oneshot,
    task::{JoinHandle, JoinSet},
    time::{Instant, Sleep},
};

fn invalid(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message)
}

fn broken_pipe() -> io::Error {
    io::Error::new(io::ErrorKind::BrokenPipe, "testkit connection disconnected")
}

/// Fixed-rate pacing with a maximum initial/per-transfer burst.
#[derive(Clone, Copy, Debug)]
pub struct Bandwidth {
    pub bytes_per_second: NonZeroU64,
    pub burst_bytes: NonZeroUsize,
}

/// Faults for one direction. Zero-byte cutoffs disconnect before any transfer.
#[derive(Clone, Debug, Default)]
pub struct DirectionFaults {
    pub latency: Duration,
    /// Inclusive seeded jitter in nanoseconds; at most `u64::MAX` nanoseconds.
    pub jitter: Duration,
    pub max_chunk: Option<NonZeroUsize>,
    pub disconnect_after: Option<u64>,
    pub bandwidth: Option<Bandwidth>,
}

/// Independent read/write policies and a non-cryptographic replay seed.
#[derive(Clone, Debug, Default)]
pub struct FaultConfig {
    pub read: DirectionFaults,
    pub write: DirectionFaults,
    pub seed: u64,
}

/// Saturating counters. No transported data is exposed.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct FaultStats {
    pub read_bytes: u64,
    pub write_bytes: u64,
    pub read_delay_samples: u64,
    pub write_delay_samples: u64,
    /// Cumulative sampled jitter, including an in-flight/cancelled attempt.
    pub read_jitter_nanos: u64,
    pub write_jitter_nanos: u64,
}

#[derive(Default)]
struct Gate {
    blocked: AtomicBool,
    waker: Mutex<Option<Waker>>,
}

impl Gate {
    fn poll(&self, cx: &Context<'_>) -> bool {
        // Register before checking flags so a concurrent release cannot be lost.
        *self.waker.lock().unwrap() = Some(cx.waker().clone());
        self.blocked.load(Ordering::SeqCst)
    }

    fn wake(&self) {
        let waker = self.waker.lock().unwrap().take();
        if let Some(waker) = waker {
            waker.wake();
        }
    }

    fn set(&self, blocked: bool) {
        self.blocked.store(blocked, Ordering::SeqCst);
        if !blocked {
            self.wake();
        }
    }
}

#[derive(Default)]
struct Controls {
    read: Gate,
    write: Gate,
    disconnected: AtomicBool,
}

/// Live backpressure/disconnect control. Cloning this handle does not keep I/O alive.
#[derive(Clone)]
pub struct FaultControl(Arc<Controls>);

impl FaultControl {
    pub fn block_reads(&self) {
        self.0.read.set(true);
    }
    pub fn unblock_reads(&self) {
        self.0.read.set(false);
    }
    pub fn block_writes(&self) {
        self.0.write.set(true);
    }
    pub fn unblock_writes(&self) {
        self.0.write.set(false);
    }

    pub fn disconnect(&self) {
        self.0.disconnected.store(true, Ordering::SeqCst);
        self.0.read.wake();
        self.0.write.wake();
    }
}

struct Direction {
    faults: DirectionFaults,
    seed: u64,
    bytes: u64,
    samples: u64,
    jitter: u64,
    in_flight: bool,
    timer: Option<Pin<Box<Sleep>>>,
    next_byte: Option<Instant>,
    clock_overflow: bool,
}

fn rate_duration(bytes: usize, rate: NonZeroU64) -> Duration {
    let nanos = ((bytes as u128) * 1_000_000_000).div_ceil(rate.get() as u128);
    Duration::new(
        (nanos / 1_000_000_000) as u64,
        (nanos % 1_000_000_000) as u32,
    )
}

impl Direction {
    fn new(faults: DirectionFaults, seed: u64) -> io::Result<Self> {
        let delay = faults
            .latency
            .checked_add(faults.jitter)
            .ok_or_else(|| invalid("testkit delay overflow"))?;
        if faults.jitter.as_nanos() > u64::MAX as u128
            || Instant::now().checked_add(delay).is_none()
            || faults.bandwidth.is_some_and(|b| {
                Instant::now()
                    .checked_add(rate_duration(b.burst_bytes.get(), b.bytes_per_second))
                    .is_none()
            })
        {
            return Err(invalid("testkit delay exceeds clock range"));
        }
        Ok(Self {
            faults,
            seed,
            bytes: 0,
            samples: 0,
            jitter: 0,
            in_flight: false,
            timer: None,
            next_byte: None,
            clock_overflow: false,
        })
    }

    fn cut(&self) -> bool {
        self.faults
            .disconnect_after
            .is_some_and(|n| self.bytes >= n)
    }

    fn limit(&self, available: usize) -> usize {
        let mut size = available;
        if let Some(chunk) = self.faults.max_chunk {
            size = size.min(chunk.get());
        }
        if let Some(rate) = self.faults.bandwidth {
            size = size.min(rate.burst_bytes.get());
        }
        if let Some(cutoff) = self.faults.disconnect_after {
            size =
                size.min(usize::try_from(cutoff.saturating_sub(self.bytes)).unwrap_or(usize::MAX));
        }
        size
    }

    fn ready(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        if self.clock_overflow {
            return Poll::Ready(Err(invalid("testkit pacing clock overflow")));
        }
        if !self.in_flight {
            self.in_flight = true;
            self.samples = self.samples.saturating_add(1);
            let bound = self.faults.jitter.as_nanos() as u64;
            let jitter = if bound == 0 {
                0
            } else {
                // SplitMix64: fixed across platforms, never used for secrets.
                self.seed = self.seed.wrapping_add(0x9e3779b97f4a7c15);
                let mut n = self.seed;
                n = (n ^ (n >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
                n = (n ^ (n >> 27)).wrapping_mul(0x94d049bb133111eb);
                n ^= n >> 31;
                if bound == u64::MAX {
                    n
                } else {
                    n % (bound + 1)
                }
            };
            self.jitter = self.jitter.saturating_add(jitter);
            let now = Instant::now();
            let Some(deadline) =
                now.checked_add(self.faults.latency + Duration::from_nanos(jitter))
            else {
                self.clock_overflow = true;
                return Poll::Ready(Err(invalid("testkit delay exceeds clock range")));
            };
            let deadline = self.next_byte.map_or(deadline, |rate| rate.max(deadline));
            if deadline > now {
                self.timer = Some(Box::pin(tokio::time::sleep_until(deadline)));
            }
        }
        match &mut self.timer {
            Some(timer) => timer.as_mut().poll(cx).map(|()| Ok(())),
            None => Poll::Ready(Ok(())),
        }
    }

    fn complete(&mut self, bytes: usize) {
        self.in_flight = false;
        self.timer = None;
        self.bytes = self.bytes.saturating_add(bytes as u64);
        if bytes > 0
            && let Some(rate) = self.faults.bandwidth
        {
            self.next_byte =
                Instant::now().checked_add(rate_duration(bytes, rate.bytes_per_second));
            self.clock_overflow = self.next_byte.is_none();
        }
    }
}

/// Test-only I/O adapter. No inner stream/data is included in Debug output.
pub struct FaultIo<S> {
    inner: Option<S>,
    read: Direction,
    write: Direction,
    control: FaultControl,
    write_shutdown: bool,
}

impl<S> FaultIo<S> {
    pub fn new(inner: S, config: FaultConfig) -> io::Result<Self> {
        Ok(Self {
            inner: Some(inner),
            read: Direction::new(config.read, config.seed ^ 0x72656164)?,
            write: Direction::new(config.write, config.seed ^ 0x7772697465)?,
            control: FaultControl(Arc::new(Controls::default())),
            write_shutdown: false,
        })
    }

    pub fn control(&self) -> FaultControl {
        self.control.clone()
    }

    pub fn stats(&self) -> FaultStats {
        FaultStats {
            read_bytes: self.read.bytes,
            write_bytes: self.write.bytes,
            read_delay_samples: self.read.samples,
            write_delay_samples: self.write.samples,
            read_jitter_nanos: self.read.jitter,
            write_jitter_nanos: self.write.jitter,
        }
    }

    fn close(&mut self) {
        self.inner = None;
        self.control.disconnect();
    }

    fn is_closed(&mut self) -> bool {
        if self.control.0.disconnected.load(Ordering::SeqCst) || self.read.cut() || self.write.cut()
        {
            self.close();
        }
        self.inner.is_none()
    }
}

impl<S: AsyncRead + Unpin> AsyncRead for FaultIo<S> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        if buf.remaining() == 0 {
            return Poll::Ready(Ok(()));
        }
        let blocked = this.control.0.read.poll(cx);
        if this.is_closed() {
            return Poll::Ready(Ok(()));
        }
        if blocked {
            return Poll::Pending;
        }
        std::task::ready!(this.read.ready(cx))?;
        let limit = this.read.limit(buf.remaining());
        let mut limited = ReadBuf::new(buf.initialize_unfilled_to(limit));
        let result = Pin::new(this.inner.as_mut().unwrap()).poll_read(cx, &mut limited);
        let bytes = limited.filled().len();
        if let Poll::Ready(result) = result {
            this.read.complete(bytes);
            buf.advance(bytes);
            if this.read.cut() {
                this.close();
            }
            Poll::Ready(result)
        } else {
            Poll::Pending
        }
    }
}

impl<S: AsyncWrite + Unpin> AsyncWrite for FaultIo<S> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        if buf.is_empty() {
            return Poll::Ready(Ok(0));
        }
        let blocked = this.control.0.write.poll(cx);
        if this.is_closed() || this.write_shutdown {
            return Poll::Ready(Err(broken_pipe()));
        }
        if blocked {
            return Poll::Pending;
        }
        std::task::ready!(this.write.ready(cx))?;
        let limit = this.write.limit(buf.len());
        match Pin::new(this.inner.as_mut().unwrap()).poll_write(cx, &buf[..limit]) {
            Poll::Ready(result) => {
                this.write.complete(result.as_ref().copied().unwrap_or(0));
                if this.write.cut() {
                    this.close();
                }
                Poll::Ready(result)
            }
            Poll::Pending => Poll::Pending,
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        let blocked = this.control.0.write.poll(cx);
        if this.is_closed() {
            return Poll::Ready(Err(broken_pipe()));
        }
        if blocked {
            return Poll::Pending;
        }
        Pin::new(this.inner.as_mut().unwrap()).poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        this.control.0.write.poll(cx);
        if this.is_closed() || this.write_shutdown {
            return Poll::Ready(Ok(()));
        }
        // Cleanup must remain possible even when a test holds the write gate.
        match Pin::new(this.inner.as_mut().unwrap()).poll_shutdown(cx) {
            Poll::Ready(Ok(())) => {
                this.write_shutdown = true;
                Poll::Ready(Ok(()))
            }
            result => result,
        }
    }
}

/// Parser/listener bounds. All values except `max_body_bytes` must be nonzero.
#[derive(Clone, Debug)]
pub struct MockLimits {
    pub max_header_bytes: usize,
    pub max_body_bytes: usize,
    pub request_timeout: Duration,
    pub max_connections: usize,
}

impl Default for MockLimits {
    fn default() -> Self {
        Self {
            max_header_bytes: 8192,
            max_body_bytes: 65536,
            request_timeout: Duration::from_secs(5),
            max_connections: 64,
        }
    }
}

#[derive(Clone, Copy)]
enum Delivery {
    Full,
    Lost,
    Truncated(usize),
}

/// Scripted response; intentionally does not implement Debug (fixture data may be private).
#[derive(Clone)]
pub struct MockReply {
    status: u16,
    body: Vec<u8>,
    retry_after: Option<Duration>,
    delay: Duration,
    delivery: Delivery,
}

impl MockReply {
    /// JSON fixture, limited to 1 MiB. Status must be 200..=599 except 204/304.
    pub fn json(status: u16, body: impl AsRef<[u8]>) -> io::Result<Self> {
        let body = body.as_ref();
        if !(200..=599).contains(&status)
            || matches!(status, 204 | 304)
            || body.len() > 1024 * 1024
            || serde_json::from_slice::<serde_json::Value>(body).is_err()
        {
            return Err(invalid("invalid mock JSON response"));
        }
        Ok(Self {
            status,
            body: body.to_vec(),
            retry_after: None,
            delay: Duration::ZERO,
            delivery: Delivery::Full,
        })
    }

    pub fn interaction_ack() -> Self {
        Self {
            status: 204,
            body: Vec::new(),
            retry_after: None,
            delay: Duration::ZERO,
            delivery: Delivery::Full,
        }
    }

    /// Fractional Retry-After seconds, matching the JSON `retry_after` value.
    pub fn rate_limited(retry_after: Duration) -> Self {
        let body = serde_json::json!({ "message": "rate limited",
            "retry_after": retry_after.as_secs_f64(), "global": false })
        .to_string()
        .into_bytes();
        Self {
            status: 429,
            body,
            retry_after: Some(retry_after),
            delay: Duration::ZERO,
            delivery: Delivery::Full,
        }
    }

    pub fn server_error(status: u16) -> io::Result<Self> {
        if !(500..=599).contains(&status) {
            return Err(invalid("invalid mock server-error status"));
        }
        Self::json(status, br#"{"message":"mock server error"}"#)
    }

    pub fn with_delay(mut self, delay: Duration) -> Self {
        self.delay = delay;
        self
    }
    pub fn with_lost_response(mut self) -> Self {
        self.delivery = Delivery::Lost;
        self
    }

    /// Send at most this prefix and always fewer than the complete response bytes.
    pub fn with_truncated_response(mut self, prefix_bytes: usize) -> Self {
        self.delivery = Delivery::Truncated(prefix_bytes);
        self
    }

    fn wire(&self) -> Vec<u8> {
        let reason = match self.status {
            204 => "No Content",
            429 => "Too Many Requests",
            500..=599 => "Server Error",
            _ => "Mock",
        };
        let mut head = format!(
            "HTTP/1.1 {} {}\r\nContent-Length: {}\r\nConnection: close\r\n",
            self.status,
            reason,
            self.body.len()
        );
        if self.status != 204 {
            head.push_str("Content-Type: application/json\r\n");
        }
        if let Some(retry) = self.retry_after {
            head.push_str(&format!("Retry-After: {}\r\n", retry.as_secs_f64()));
        }
        head.push_str("\r\n");
        let mut wire = head.into_bytes();
        wire.extend_from_slice(&self.body);
        wire
    }
}

/// Saturating aggregate counts only. `applied` counts selected 2xx fixtures,
/// even if the caller times out or disconnects before receiving a response.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct MockStats {
    pub requests: u64,
    pub rest_requests: u64,
    pub interaction_requests: u64,
    pub applied: u64,
    pub replies_sent: u64,
    pub lost_responses: u64,
    pub script_exhausted: u64,
    pub rejected_requests: u64,
}

struct MockState {
    replies: VecDeque<MockReply>,
    stats: MockStats,
}

/// Shared script/observations. No real credentials are needed or retained.
#[derive(Clone)]
pub struct MockDiscord {
    state: Arc<Mutex<MockState>>,
    limits: MockLimits,
}

impl MockDiscord {
    pub fn new(replies: impl IntoIterator<Item = MockReply>) -> Self {
        Self::with_limits(replies, MockLimits::default()).expect("valid default mock limits")
    }

    pub fn with_limits(
        replies: impl IntoIterator<Item = MockReply>,
        limits: MockLimits,
    ) -> io::Result<Self> {
        if limits.max_header_bytes == 0
            || limits.max_header_bytes == usize::MAX
            || limits.max_connections == 0
            || limits.request_timeout.is_zero()
            || Instant::now().checked_add(limits.request_timeout).is_none()
        {
            return Err(invalid("invalid mock limits"));
        }
        Ok(Self {
            state: Arc::new(Mutex::new(MockState {
                replies: replies.into_iter().collect(),
                stats: MockStats::default(),
            })),
            limits,
        })
    }

    pub fn stats(&self) -> MockStats {
        self.state.lock().unwrap().stats
    }
    pub fn remaining(&self) -> usize {
        self.state.lock().unwrap().replies.len()
    }

    /// Serve exactly one request. Malformed/oversized/incomplete requests consume
    /// no script entry and return a fixed error without echoing request data.
    pub async fn serve<S: AsyncRead + AsyncWrite + Unpin>(&self, stream: S) -> io::Result<()> {
        let mut stream = BufReader::new(stream);
        let request = match tokio::time::timeout(
            self.limits.request_timeout,
            read_request(&mut stream, &self.limits),
        )
        .await
        {
            Ok(result) => result,
            Err(_) => Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "mock request timed out",
            )),
        };
        let interaction = match request {
            Ok(interaction) => interaction,
            Err(error) => {
                let mut state = self.state.lock().unwrap();
                state.stats.rejected_requests = state.stats.rejected_requests.saturating_add(1);
                return Err(error);
            }
        };
        let reply = {
            let mut state = self.state.lock().unwrap();
            state.stats.requests = state.stats.requests.saturating_add(1);
            if interaction {
                state.stats.interaction_requests =
                    state.stats.interaction_requests.saturating_add(1);
            } else {
                state.stats.rest_requests = state.stats.rest_requests.saturating_add(1);
            }
            let reply = state.replies.pop_front().unwrap_or_else(|| {
                state.stats.script_exhausted = state.stats.script_exhausted.saturating_add(1);
                MockReply::server_error(503).expect("valid fixture")
            });
            if (200..300).contains(&reply.status) {
                state.stats.applied = state.stats.applied.saturating_add(1);
            }
            if !matches!(reply.delivery, Delivery::Full) {
                state.stats.lost_responses = state.stats.lost_responses.saturating_add(1);
            }
            reply
        };
        if !reply.delay.is_zero() {
            let deadline = Instant::now()
                .checked_add(reply.delay)
                .ok_or_else(|| invalid("mock response delay exceeds clock range"))?;
            tokio::time::sleep_until(deadline).await;
        }
        let wire = reply.wire();
        let length = match reply.delivery {
            Delivery::Full => wire.len(),
            Delivery::Lost => 0,
            Delivery::Truncated(length) => length.min(wire.len() - 1),
        };
        let stream = stream.get_mut();
        stream.write_all(&wire[..length]).await?;
        stream.flush().await?;
        stream.shutdown().await?;
        if matches!(reply.delivery, Delivery::Full) {
            let mut state = self.state.lock().unwrap();
            state.stats.replies_sent = state.stats.replies_sent.saturating_add(1);
        }
        Ok(())
    }

    /// Bind only IPv4 loopback on a fresh ephemeral port. Child tasks are bounded,
    /// owned by the listener, and cancelled/joined by `shutdown`.
    pub async fn spawn_tcp(&self) -> io::Result<MockServer> {
        let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await?;
        let addr = listener.local_addr()?;
        let (stop, mut stopped) = oneshot::channel();
        let mock = self.clone();
        let task = tokio::spawn(async move {
            let mut clients = JoinSet::new();
            let result = loop {
                tokio::select! {
                    biased;
                    _ = &mut stopped => break Ok(()),
                    _ = clients.join_next(), if !clients.is_empty() => {},
                    accepted = listener.accept(), if clients.len() < mock.limits.max_connections => {
                        let (stream, _) = match accepted {
                            Ok(connection) => connection,
                            Err(error) => break Err(error),
                        };
                        let mock = mock.clone();
                        clients.spawn(async move { let _ = mock.serve(stream).await; });
                    }
                }
            };
            clients.abort_all();
            while clients.join_next().await.is_some() {}
            result
        });
        Ok(MockServer {
            addr,
            stop: Some(stop),
            task: Some(task),
        })
    }
}

/// Loopback server guard. Drop aborts; `shutdown().await` also awaits cleanup.
pub struct MockServer {
    addr: SocketAddr,
    stop: Option<oneshot::Sender<()>>,
    task: Option<JoinHandle<io::Result<()>>>,
}

impl MockServer {
    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    pub async fn shutdown(mut self) -> io::Result<()> {
        if let Some(stop) = self.stop.take() {
            let _ = stop.send(());
        }
        // Keep the handle in self while awaiting, so cancellation still aborts.
        let result = self
            .task
            .as_mut()
            .unwrap()
            .await
            .map_err(|_| io::Error::other("mock listener task failed"))?;
        self.task.take();
        result
    }
}

impl Drop for MockServer {
    fn drop(&mut self) {
        if let Some(task) = &self.task {
            task.abort();
        }
    }
}

fn bad_request() -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, "invalid mock HTTP request")
}

async fn read_request<S: AsyncRead + Unpin>(
    stream: &mut BufReader<S>,
    limits: &MockLimits,
) -> io::Result<bool> {
    let mut header = Vec::new();
    loop {
        let before = header.len();
        let remaining = limits.max_header_bytes - before;
        (&mut *stream)
            .take(remaining as u64 + 1)
            .read_until(b'\n', &mut header)
            .await?;
        if header.len() > limits.max_header_bytes
            || header.len() == before
            || !header[before..].ends_with(b"\r\n")
        {
            return Err(bad_request());
        }
        if header[before..] == *b"\r\n" {
            break;
        }
    }
    let text = std::str::from_utf8(&header).map_err(|_| bad_request())?;
    if !text.is_ascii()
        || text
            .bytes()
            .any(|b| b.is_ascii_control() && !matches!(b, b'\r' | b'\n' | b'\t'))
    {
        return Err(bad_request());
    }
    let mut lines = text.split("\r\n");
    let mut request = lines.next().ok_or_else(bad_request)?.split(' ');
    let method = request.next().ok_or_else(bad_request)?;
    let path = request.next().ok_or_else(bad_request)?;
    let version = request.next().ok_or_else(bad_request)?;
    if method.is_empty()
        || !method.bytes().all(|b| b.is_ascii_uppercase())
        || !path.starts_with('/')
        || path.bytes().any(|b| b.is_ascii_control())
        || !matches!(version, "HTTP/1.0" | "HTTP/1.1")
        || request.next().is_some()
    {
        return Err(bad_request());
    }
    let parts: Vec<_> = path
        .split('?')
        .next()
        .unwrap_or("")
        .rsplit('/')
        .take(4)
        .collect();
    let interaction = method == "POST"
        && parts.len() == 4
        && parts[0] == "callback"
        && !parts[1].is_empty()
        && !parts[2].is_empty()
        && parts[3] == "interactions";
    let mut content_length = None;
    for line in lines {
        if line.is_empty() {
            break;
        }
        let (name, value) = line.split_once(':').ok_or_else(bad_request)?;
        if value.bytes().any(|b| b.is_ascii_control() && b != b'\t')
            || name.is_empty()
            || !name
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b"!#$%&'*+-.^_`|~".contains(&b))
        {
            return Err(bad_request());
        }
        if name.eq_ignore_ascii_case("transfer-encoding") || name.eq_ignore_ascii_case("expect") {
            return Err(bad_request());
        }
        if name.eq_ignore_ascii_case("content-length") {
            let value = value.trim_matches([' ', '\t']);
            if content_length.is_some()
                || value.is_empty()
                || !value.bytes().all(|b| b.is_ascii_digit())
            {
                return Err(bad_request());
            }
            content_length = Some(value.parse::<usize>().map_err(|_| bad_request())?);
        }
    }
    let length = content_length.unwrap_or(0);
    if length > limits.max_body_bytes {
        return Err(bad_request());
    }
    // Stream/discard the body rather than retaining arbitrary fixture content.
    let received = tokio::io::copy(&mut stream.take(length as u64), &mut tokio::io::sink()).await?;
    if received != length as u64 {
        return Err(bad_request());
    }
    Ok(interaction)
}
