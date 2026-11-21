//! Text-only Create Message payloads for the Gateway and the opt-in live probe.

use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use twilight_http::{
    error::ErrorType,
    response::{Response, ResponseFuture},
};
use twilight_model::channel::message::AllowedMentions;

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
) -> Result<Response<T>, AttemptError> {
    let sent = Arc::new(AtomicBool::new(false));
    let gate = Arc::clone(&sent);
    if !request.set_pre_flight(move || !gate.swap(true, Ordering::Relaxed)) {
        return Err(AttemptError::GuardUnavailable);
    }

    request.await.map_err(|error| match error.kind() {
        // Twilight 0.18 reaches the second pre-flight only after a 429.
        ErrorType::RequestCanceled if sent.load(Ordering::Relaxed) => AttemptError::RateLimited,
        ErrorType::Response { status, .. } if status.get() == 429 => AttemptError::RateLimited,
        ErrorType::Response { status, .. } => AttemptError::Http(status.get()),
        ErrorType::RequestTimedOut => AttemptError::Timeout,
        ErrorType::RequestError => AttemptError::Transport,
        _ => AttemptError::Other,
    })
}
