//! Read-only control session over a caller-provided private transport.
//! Bootstrap, peer authentication, process wiring, authorization and audit are not
//! implemented here. This module does not expose a TCP listener or a CLI.
use crate::worker::Manager;
use serde::{Deserialize, Serialize};
use std::{io, time::Duration};
use subtle::ConstantTimeEq;
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader};

const REQUEST_LIMIT: u64 = 4096;
const RESPONSE_LIMIT: usize = 64 * 1024;
const DEADLINE: Duration = Duration::from_secs(3);

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ListRequest {
    pub secret: [u8; 32],
    pub command: String,
}
#[derive(Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct ListResponse {
    pub instance: String,
    pub unknown: Vec<(usize, u64)>,
}

/// Serve exactly one request. Caller must establish an authenticated, private
/// local channel and supply a freshly generated per-process secret out of band.
/// Rejected authentication produces no response or logs.
pub async fn serve_list<S>(stream: S, secret: &[u8; 32], manager: &Manager) -> io::Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    tokio::time::timeout(DEADLINE, serve_inner(stream, secret, manager))
        .await
        .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "control session timed out"))?
}

async fn serve_inner<S>(stream: S, secret: &[u8; 32], manager: &Manager) -> io::Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let mut stream = BufReader::new(stream);
    let mut frame = Vec::new();
    (&mut stream)
        .take(REQUEST_LIMIT + 1)
        .read_until(b'\n', &mut frame)
        .await?;
    if frame.is_empty() || frame.len() as u64 > REQUEST_LIMIT || frame.last() != Some(&b'\n') {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid control frame",
        ));
    }
    let request: ListRequest = serde_json::from_slice(&frame)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "invalid control request"))?;
    if !bool::from(request.secret.ct_eq(secret)) || request.command != "list_unknown" {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "control request denied",
        ));
    }
    let (instance, unknown) = manager.unknown_responses();
    let mut reply = serde_json::to_vec(&ListResponse { instance, unknown })?;
    reply.push(b'\n');
    if reply.len() > RESPONSE_LIMIT {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "control response exceeds limit",
        ));
    }
    stream.get_mut().write_all(&reply).await?;
    stream.get_mut().flush().await
}
