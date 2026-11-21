//! Text-only Create Message payloads for the Gateway and the opt-in live probe.

use std::{
    future::{Future, IntoFuture},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use tokio::time::{Instant, sleep, timeout, timeout_at};
use twilight_http::{
    Client,
    error::ErrorType,
    response::{DeserializeBodyError, Response, ResponseFuture},
};
use twilight_model::{
    channel::message::AllowedMentions,
    id::{Id, marker::ChannelMarker},
};

pub const NONCE_RETRY_FLAG: &str = "--enable-create-message-nonce-retry";
const LEGACY_DEADLINE: Duration = Duration::from_secs(10);
const RETRY_WINDOW: Duration = Duration::from_secs(15);
const RETRY_BACKOFF: Duration = Duration::from_millis(250);

#[derive(Clone)]
struct RetryDeadline<C> {
    active_deadline: Instant,
    boot_deadline_ms: u64,
    boot_ms: C,
}

impl<C: Fn() -> Option<u64>> RetryDeadline<C> {
    fn new(active_deadline: Instant, boot_ms: C) -> Option<Self> {
        let boot_deadline_ms = boot_ms()?.checked_add(RETRY_WINDOW.as_millis() as u64)?;
        Some(Self {
            active_deadline,
            boot_deadline_ms,
            boot_ms,
        })
    }

    fn is_open(&self) -> bool {
        Instant::now() < self.active_deadline
            && (self.boot_ms)().is_some_and(|now| now < self.boot_deadline_ms)
    }
}

#[cfg(target_os = "linux")]
use crate::core_status::try_boot_ms as retry_boot_ms;

#[cfg(not(target_os = "linux"))]
fn retry_boot_ms() -> Option<u64> {
    None
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TextMessageKind {
    TrackedPrefixReply,
    WorkersInfo,
}

pub fn new_nonce() -> Result<String, getrandom::Error> {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut bytes = [0_u8; 12];
    getrandom::fill(&mut bytes)?;
    let mut nonce = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        nonce.push(char::from(HEX[usize::from(byte >> 4)]));
        nonce.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    Ok(nonce)
}

pub fn create_payload(content: &str, nonce: &str) -> Result<Vec<u8>, serde_json::Error> {
    #[derive(serde::Serialize)]
    struct Payload<'a> {
        content: &'a str,
        nonce: &'a str,
        enforce_nonce: bool,
        allowed_mentions: AllowedMentions,
    }

    serde_json::to_vec(&Payload {
        content,
        nonce,
        enforce_nonce: true,
        allowed_mentions: AllowedMentions::default(),
    })
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AttemptError {
    Deadline,
    GuardUnavailable,
    RateLimited,
    Http(u16),
    Transport,
    Timeout,
    Other,
}

/// Use Twilight's shared limiter but decline its internal 429 re-send. In 0.18
/// the limiter alone cannot be relied on to honor the global Retry-After.
pub async fn single_attempt<T: Unpin>(
    request: ResponseFuture<T>,
    deadline: Instant,
) -> Result<Response<T>, AttemptError> {
    single_attempt_with_guard(request, move || Instant::now() < deadline).await
}

async fn single_attempt_with_guard<T: Unpin>(
    mut request: ResponseFuture<T>,
    can_send: impl Fn() -> bool + Send + 'static,
) -> Result<Response<T>, AttemptError> {
    let sent = Arc::new(AtomicBool::new(false));
    let gate = Arc::clone(&sent);
    if !request.set_pre_flight(move || can_send() && !gate.swap(true, Ordering::Relaxed)) {
        return Err(AttemptError::GuardUnavailable);
    }

    request.await.map_err(|error| {
        let (kind, source) = error.into_parts();
        match kind {
            // Twilight 0.18 reaches the second pre-flight only after a 429.
            ErrorType::RequestCanceled if sent.load(Ordering::Relaxed) => AttemptError::RateLimited,
            ErrorType::RequestCanceled => AttemptError::Deadline,
            ErrorType::Response { status, .. } if status.get() == 429 => AttemptError::RateLimited,
            ErrorType::Response { status, .. } => AttemptError::Http(status.get()),
            ErrorType::RequestTimedOut => AttemptError::Timeout,
            // Twilight 0.18.0 uses RequestError for both failures before response
            // headers and failures reading a non-success response body. Only the
            // former is retryable. The body error no longer carries the status.
            ErrorType::RequestError
                if source.is_some_and(|source| !source.is::<DeserializeBodyError>()) =>
            {
                AttemptError::Transport
            }
            _ => AttemptError::Other,
        }
    })
}

pub async fn send_text_message(
    http: &Client,
    channel: Id<ChannelMarker>,
    kind: TextMessageKind,
    content: &str,
    nonce_retry: bool,
) -> Option<u16> {
    send_text_message_with(
        kind,
        content,
        nonce_retry,
        async {
            let no_mentions = AllowedMentions::default();
            timeout(
                LEGACY_DEADLINE,
                http.create_message(channel)
                    .content(content)
                    .allowed_mentions(Some(&no_mentions))
                    .into_future(),
            )
            .await
            .ok()
            .and_then(Result::ok)
            .map(|response| response.status().get())
        },
        |body, deadline| {
            let request = http
                .create_message(channel)
                .payload_json(body)
                .into_future();
            async move {
                single_attempt_with_guard(request, move || deadline.is_open())
                    .await
                    .map(|response| response.status().get())
            }
        },
    )
    .await
}

async fn send_text_message_with<F: Future<Output = Result<u16, AttemptError>>>(
    kind: TextMessageKind,
    content: &str,
    nonce_retry: bool,
    legacy_send: impl Future<Output = Option<u16>>,
    nonce_send: impl FnMut(&[u8], RetryDeadline<fn() -> Option<u64>>) -> F,
) -> Option<u16> {
    if !nonce_retry || kind != TextMessageKind::TrackedPrefixReply {
        return legacy_send.await;
    }

    let nonce = new_nonce().ok()?;
    let body = create_payload(content, &nonce).ok()?;
    let deadline = RetryDeadline::new(Instant::now() + RETRY_WINDOW, retry_boot_ms as fn() -> _)?;
    send_with_nonce(&body, deadline, nonce_send).await
}

async fn send_with_nonce<
    C: Fn() -> Option<u64> + Clone,
    F: Future<Output = Result<u16, AttemptError>>,
>(
    body: &[u8],
    deadline: RetryDeadline<C>,
    mut send: impl FnMut(&[u8], RetryDeadline<C>) -> F,
) -> Option<u16> {
    // Keep the active-time timeout for in-flight I/O. The boot clock also
    // covers suspend before the retry decision and before any HTTP send.
    timeout_at(deadline.active_deadline, async {
        if !deadline.is_open() {
            return None;
        }
        match send(body, deadline.clone()).await {
            Ok(status) => Some(status),
            Err(AttemptError::Transport | AttemptError::Timeout) => {
                sleep(RETRY_BACKOFF).await;
                if !deadline.is_open() {
                    return None;
                }
                send(body, deadline).await.ok()
            }
            Err(_) => None,
        }
    })
    .await
    .ok()
    .flatten()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{Value, json};
    use std::{
        future::{poll_fn, ready},
        pin::pin,
        sync::atomic::AtomicU64,
        task::Poll,
    };
    use tokio::{
        io::{AsyncBufReadExt as _, AsyncReadExt as _, AsyncWriteExt as _, BufReader},
        net::{TcpListener, TcpStream},
    };

    fn proxy_client(listener: &TcpListener) -> Client {
        let _ = rustls::crypto::ring::default_provider().install_default();
        Client::builder()
            .proxy(listener.local_addr().unwrap().to_string(), true)
            .build()
    }

    async fn accept_post(listener: &TcpListener) -> (TcpStream, Vec<u8>) {
        let (stream, _) = listener.accept().await.unwrap();
        let mut stream = BufReader::new(stream);
        let mut line = String::new();
        stream.read_line(&mut line).await.unwrap();
        assert!(line.starts_with("POST "));
        let mut content_length = None;
        loop {
            line.clear();
            assert_ne!(stream.read_line(&mut line).await.unwrap(), 0);
            if line == "\r\n" {
                break;
            }
            if let Some((name, value)) = line.split_once(':')
                && name.eq_ignore_ascii_case("content-length")
            {
                content_length = Some(value.trim().parse().unwrap());
            }
        }
        let mut body = vec![0; content_length.unwrap()];
        stream.read_exact(&mut body).await.unwrap();
        (stream.into_inner(), body)
    }

    fn active_time_deadline(deadline: Instant) -> RetryDeadline<impl Fn() -> Option<u64> + Clone> {
        RetryDeadline::new(deadline, || Some(0)).unwrap()
    }

    #[tokio::test]
    async fn nonce_retry_resends_the_full_identical_body_after_backoff() {
        let content = "一度だけ @everyone <@123> <@&456>";
        let mut attempts = Vec::new();
        let mut started = Vec::new();
        let result = send_text_message_with(
            TextMessageKind::TrackedPrefixReply,
            content,
            true,
            async { panic!("unexpected legacy send") },
            |body, _deadline| {
                attempts.push(body.to_vec());
                started.push(Instant::now());
                ready(if attempts.len() == 1 {
                    Err(AttemptError::Transport)
                } else {
                    Ok(200)
                })
            },
        )
        .await;

        assert_eq!(result, Some(200));
        assert_eq!(attempts.len(), 2);
        assert_eq!(attempts[0], attempts[1]);
        assert!(started[1] - started[0] >= RETRY_BACKOFF);
        let body: Value = serde_json::from_slice(&attempts[0]).unwrap();
        let nonce = body["nonce"].as_str().unwrap();
        assert_eq!(nonce.len(), 24);
        assert!(nonce.bytes().all(|byte| byte.is_ascii_hexdigit()));
        assert_eq!(
            body,
            json!({
                "nonce": nonce,
                "enforce_nonce": true,
                "content": content,
                "allowed_mentions": {"parse": []}
            })
        );
    }

    #[tokio::test]
    async fn nonce_retry_disabled_or_ineligible_uses_legacy_result_once() {
        for (kind, enabled) in [
            (TextMessageKind::TrackedPrefixReply, false),
            (TextMessageKind::WorkersInfo, true),
        ] {
            for legacy_result in [None, Some(200)] {
                let mut calls = 0;
                let result = send_text_message_with(
                    kind,
                    "reply",
                    enabled,
                    async {
                        calls += 1;
                        legacy_result
                    },
                    |_, _| async { panic!("unexpected nonce send") },
                )
                .await;
                assert_eq!(calls, 1);
                assert_eq!(result, legacy_result);
            }
        }
    }

    #[tokio::test]
    async fn nonce_retry_keeps_outcome_unknown_after_one_unsuccessful_retry() {
        for second in [AttemptError::Transport, AttemptError::Http(403)] {
            let mut attempts = 0;
            let deadline = active_time_deadline(Instant::now() + RETRY_WINDOW);
            let result = send_with_nonce(b"payload", deadline, |_, _| {
                attempts += 1;
                ready(Err(if attempts == 1 {
                    AttemptError::Timeout
                } else {
                    second
                }))
            })
            .await;
            // Gateway maps None to ApiOutcome::Unknown for the existing receipt.
            assert_eq!(result, None);
            assert_eq!(attempts, 2);
        }
    }

    #[tokio::test]
    async fn nonce_retry_does_not_resend_after_a_http_error_or_429() {
        for first in [AttemptError::Http(503), AttemptError::RateLimited] {
            let mut attempts = 0;
            let deadline = active_time_deadline(Instant::now() + RETRY_WINDOW);
            let result = send_with_nonce(b"payload", deadline, |_, _| {
                attempts += 1;
                ready(Err(first))
            })
            .await;
            assert_eq!(result, None);
            assert_eq!(attempts, 1);
        }
    }

    #[tokio::test]
    async fn nonce_retry_does_not_start_after_the_window_closes() {
        let mut attempts = 0;
        let result = send_with_nonce(
            b"payload",
            active_time_deadline(Instant::now() + Duration::from_millis(10)),
            |_, _| {
                attempts += 1;
                ready(Err(AttemptError::Transport))
            },
        )
        .await;
        assert_eq!(result, None);
        assert_eq!(attempts, 1);
    }

    #[tokio::test]
    async fn nonce_retry_rechecks_boot_clock_after_backoff() {
        for boot_time_after_suspend in [Some(60_000), None] {
            let now = std::cell::Cell::new(Some(1000));
            let attempts = std::cell::Cell::new(0);
            let active_deadline = Instant::now() + RETRY_WINDOW;
            let deadline = RetryDeadline::new(active_deadline, || now.get()).unwrap();
            let mut send = pin!(send_with_nonce(b"payload", deadline, |_, _| {
                attempts.set(attempts.get() + 1);
                ready(Err(AttemptError::Transport))
            }));
            poll_fn(|cx| {
                assert!(send.as_mut().poll(cx).is_pending());
                Poll::Ready(())
            })
            .await;
            assert_eq!(attempts.get(), 1);
            now.set(boot_time_after_suspend);

            assert_eq!(send.await, None);
            assert_eq!(attempts.get(), 1);
            assert!(Instant::now() < active_deadline);
        }
    }

    #[tokio::test]
    async fn nonce_retry_does_not_resend_after_a_truncated_http_error_body() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let http = proxy_client(&listener);
        let channel = Id::new(1);
        let server = tokio::spawn(async move {
            // Exercise the unwrapped Twilight error first, then the production sender.
            for _ in 0..2 {
                let (mut stream, _) = accept_post(&listener).await;
                stream
                    .write_all(b"HTTP/1.1 503 Service Unavailable\r\nContent-Length: 32\r\nConnection: close\r\n\r\n{}")
                    .await
                    .unwrap();
                stream.shutdown().await.unwrap();
            }
            // If the production sender retries, let it succeed so None cannot
            // be explained by another transport failure or by a timeout.
            if let Ok((mut stream, _)) =
                timeout(Duration::from_secs(2), accept_post(&listener)).await
            {
                stream
                    .write_all(b"HTTP/1.1 204 No Content\r\nConnection: close\r\n\r\n")
                    .await
                    .unwrap();
                return true;
            }
            false
        });
        let body = create_payload("probe", "test-nonce").unwrap();
        let error = timeout(
            Duration::from_secs(2),
            http.create_message(channel)
                .payload_json(&body)
                .into_future(),
        )
        .await
        .unwrap()
        .unwrap_err();
        assert!(matches!(error.kind(), ErrorType::RequestError));
        assert!(error.into_source().unwrap().is::<DeserializeBodyError>());
        let result = timeout(
            Duration::from_secs(3),
            send_text_message(
                &http,
                channel,
                TextMessageKind::TrackedPrefixReply,
                "probe",
                true,
            ),
        )
        .await
        .unwrap();
        assert_eq!(result, None);
        assert!(!server.await.unwrap());
    }

    #[tokio::test]
    async fn nonce_retry_limiter_wait_cannot_send_a_late_second_post() {
        assert_limiter_blocks_late_retry(false).await;
    }

    #[tokio::test]
    async fn nonce_retry_pre_flight_rechecks_boot_clock_after_limiter_wait() {
        assert_limiter_blocks_late_retry(true).await;
    }

    async fn assert_limiter_blocks_late_retry(suspend: bool) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let http = proxy_client(&listener);
        let channel = Id::new(1);
        let body = create_payload("probe", "test-nonce").unwrap();
        let follow_up = create_payload("follow-up", "different-nonce").unwrap();
        let expected = (body.clone(), follow_up.clone());
        let server = tokio::spawn(async move {
            let (mut stream, first) = accept_post(&listener).await;
            assert_eq!(first, expected.0);
            // The POST arrived. Close without returning any response headers.
            stream.shutdown().await.unwrap();
            drop(stream);

            let (mut stream, next) = accept_post(&listener).await;
            assert_eq!(next, expected.1, "an expired retry reached the wire");
            stream
                .write_all(b"HTTP/1.1 204 No Content\r\nConnection: close\r\n\r\n")
                .await
                .unwrap();
        });

        let mut attempts = 0;
        let mut blocker = None;
        let active_deadline = Instant::now()
            + if suspend {
                RETRY_WINDOW
            } else {
                Duration::from_secs(2)
            };
        let now = Arc::new(AtomicU64::new(1000));
        let clock = Arc::clone(&now);
        let deadline =
            RetryDeadline::new(active_deadline, move || Some(clock.load(Ordering::Relaxed)))
                .unwrap();
        let send = send_with_nonce(&body, deadline, |body, deadline| {
            attempts += 1;
            let attempt = attempts;
            let request = http
                .create_message(channel)
                .payload_json(body)
                .into_future();
            if attempt == 1 {
                // Twilight queues permits on creation. Keep this request unpolled
                // so it holds the same route behind the first POST and ahead of the retry.
                blocker = Some(
                    http.create_message(channel)
                        .payload_json(body)
                        .into_future(),
                );
            }
            let release = if attempt == 2 && suspend {
                blocker.take()
            } else {
                None
            };
            let now = Arc::clone(&now);
            async move {
                let mut request =
                    pin!(single_attempt_with_guard(request, move || deadline.is_open()));
                if let Some(blocker) = release {
                    poll_fn(|cx| {
                        assert!(request.as_mut().poll(cx).is_pending());
                        Poll::Ready(())
                    })
                    .await;
                    now.store(60_000, Ordering::Relaxed);
                    drop(blocker);
                }
                let result = request.await;
                if attempt == 1 {
                    assert!(matches!(&result, Err(AttemptError::Transport)));
                } else if suspend {
                    assert!(matches!(&result, Err(AttemptError::Deadline)));
                }
                result.map(|response| response.status().get())
            }
        });
        let result = timeout(Duration::from_secs(5), send)
            .await
            .expect("nonce retry did not stop at the total deadline");
        assert_eq!(attempts, 2, "the retry must have entered the limiter queue");
        assert_eq!(result, None);
        assert_eq!(Instant::now() < active_deadline, suspend);
        drop(blocker);

        // Release the limiter and send a distinct request through the same route.
        // This proves the expired retry did not slip through when the queue drained.
        let deadline = Instant::now() + Duration::from_secs(2);
        let request = http
            .create_message(channel)
            .payload_json(&follow_up)
            .into_future();
        let result = timeout_at(deadline, single_attempt(request, deadline)).await;
        assert!(matches!(result, Ok(Ok(response)) if response.status().get() == 204));
        server.await.unwrap();
    }

    #[tokio::test]
    async fn nonce_retry_pre_flight_rejects_an_expired_attempt() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let http = proxy_client(&listener);
        let body = create_payload("probe", "test-nonce").unwrap();
        let request = http
            .create_message(Id::new(1))
            .payload_json(&body)
            .into_future();
        let result = timeout(
            Duration::from_secs(2),
            single_attempt(request, Instant::now()),
        )
        .await;
        assert!(matches!(result, Ok(Err(AttemptError::Deadline))));
        assert!(
            timeout(Duration::from_millis(50), listener.accept())
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn nonce_retry_cancels_twilight_global_429_resend() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let proxy = listener.local_addr().unwrap().to_string();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut stream = BufReader::new(stream);
            loop {
                let mut line = String::new();
                assert_ne!(stream.read_line(&mut line).await.unwrap(), 0);
                if line == "\r\n" {
                    break;
                }
            }
            stream.get_mut().write_all(
                b"HTTP/1.1 429 Too Many Requests\r\nX-RateLimit-Scope: global\r\nRetry-After: 60\r\nContent-Length: 2\r\nConnection: close\r\n\r\n{}",
            ).await.unwrap();
            // An unintended second attempt would connect again or time out
            // waiting for a response, instead of returning RateLimited.
            assert!(
                timeout(Duration::from_millis(50), listener.accept())
                    .await
                    .is_err()
            );
        });
        let _ = rustls::crypto::ring::default_provider().install_default();
        let http = Client::builder().proxy(proxy, true).build();
        let body = create_payload("probe", "test-nonce").unwrap();
        let request = http
            .create_message(Id::new(1))
            .payload_json(&body)
            .into_future();
        let deadline = Instant::now() + Duration::from_secs(2);
        let result = timeout_at(deadline, single_attempt(request, deadline)).await;
        assert!(matches!(result, Ok(Err(AttemptError::RateLimited))));
        server.await.unwrap();
    }
}
