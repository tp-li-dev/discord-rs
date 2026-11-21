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
    response::{Response, ResponseFuture},
};
use twilight_model::{
    channel::message::AllowedMentions,
    id::{Id, marker::ChannelMarker},
};

pub const NONCE_RETRY_FLAG: &str = "--enable-create-message-nonce-retry";
const LEGACY_DEADLINE: Duration = Duration::from_secs(10);
const RETRY_WINDOW: Duration = Duration::from_secs(15);
const RETRY_BACKOFF: Duration = Duration::from_millis(250);

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
    mut request: ResponseFuture<T>,
    deadline: Instant,
) -> Result<Response<T>, AttemptError> {
    let sent = Arc::new(AtomicBool::new(false));
    let gate = Arc::clone(&sent);
    if !request
        .set_pre_flight(move || Instant::now() < deadline && !gate.swap(true, Ordering::Relaxed))
    {
        return Err(AttemptError::GuardUnavailable);
    }

    request.await.map_err(|error| match error.kind() {
        // Twilight 0.18 reaches the second pre-flight only after a 429.
        ErrorType::RequestCanceled if sent.load(Ordering::Relaxed) => AttemptError::RateLimited,
        ErrorType::RequestCanceled => AttemptError::Deadline,
        ErrorType::Response { status, .. } if status.get() == 429 => AttemptError::RateLimited,
        ErrorType::Response { status, .. } => AttemptError::Http(status.get()),
        ErrorType::RequestTimedOut => AttemptError::Timeout,
        ErrorType::RequestError => AttemptError::Transport,
        _ => AttemptError::Other,
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
                single_attempt(request, deadline)
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
    nonce_send: impl FnMut(&[u8], Instant) -> F,
) -> Option<u16> {
    if !nonce_retry || kind != TextMessageKind::TrackedPrefixReply {
        return legacy_send.await;
    }

    let nonce = new_nonce().ok()?;
    let body = create_payload(content, &nonce).ok()?;
    send_with_nonce(&body, Instant::now() + RETRY_WINDOW, nonce_send).await
}

async fn send_with_nonce<F: Future<Output = Result<u16, AttemptError>>>(
    body: &[u8],
    deadline: Instant,
    mut send: impl FnMut(&[u8], Instant) -> F,
) -> Option<u16> {
    timeout_at(deadline, async {
        match send(body, deadline).await {
            Ok(status) => Some(status),
            Err(AttemptError::Transport | AttemptError::Timeout) => {
                sleep(RETRY_BACKOFF).await;
                if Instant::now() >= deadline {
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
    use std::future::ready;

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
            let result = send_with_nonce(b"payload", Instant::now() + RETRY_WINDOW, |_, _| {
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
            let result = send_with_nonce(b"payload", Instant::now() + RETRY_WINDOW, |_, _| {
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
            Instant::now() + Duration::from_millis(10),
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
    async fn nonce_retry_cancels_twilight_global_429_resend() {
        use tokio::{
            io::{AsyncBufReadExt as _, AsyncWriteExt as _, BufReader},
            net::TcpListener,
        };

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
