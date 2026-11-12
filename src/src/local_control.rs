//! Bounded read-only control over a bootstrap-bound local connection.
use crate::worker::Manager;
use serde::{Deserialize, Serialize};
use std::{io, sync::Arc, time::Duration};
use subtle::ConstantTimeEq;
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader};

const REQUEST_LIMIT: u64 = 4096;
const RESPONSE_LIMIT: usize = 64 * 1024;
const DEADLINE: Duration = Duration::from_secs(3);

/// Existing daemon newline-JSON envelope. The capability is delivered by an
/// anonymous bootstrap pipe, never by argv or by the manifest. Deliberately no
/// Debug implementation. The payload must reject unknown fields on decoding.
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DaemonControlEnvelope<T> {
    pub secret: [u8; 32],
    #[serde(flatten)]
    pub request: T,
}

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
    let (instance, mut unknown) = manager.unknown_responses();
    unknown.sort_unstable();
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

/// Secrets travel only through the child's anonymous stdin pipe.
#[derive(Serialize, Deserialize)]
pub struct Bootstrap {
    pub port: u16,
    pub gateway_secret: [u8; 32],
    pub manager_secret: [u8; 32],
}

pub async fn read_bootstrap() -> io::Result<Bootstrap> {
    tokio::time::timeout(DEADLINE, read_json(tokio::io::stdin(), REQUEST_LIMIT))
        .await
        .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "bootstrap timed out"))?
}

async fn read_json<R: AsyncRead + Unpin, T: serde::de::DeserializeOwned>(
    reader: R,
    limit: u64,
) -> io::Result<T> {
    let mut frame = Vec::new();
    BufReader::new(reader)
        .take(limit + 1)
        .read_until(b'\n', &mut frame)
        .await?;
    if frame.len() as u64 > limit || frame.last() != Some(&b'\n') {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid control frame",
        ));
    }
    serde_json::from_slice(&frame)
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "invalid control data"))
}

pub async fn gateway_session(bootstrap: Bootstrap, manager: Arc<Manager>) -> io::Result<()> {
    let mut stream = tokio::net::TcpStream::connect(("127.0.0.1", bootstrap.port)).await?;
    stream.write_all(&bootstrap.gateway_secret).await?;
    loop {
        // Each request proves knowledge of the other bootstrap capability.
        stream.readable().await?;
        serve_list(&mut stream, &bootstrap.manager_secret, &manager).await?;
    }
}

/// Listener lifetime and credentials are unique to one spawned Gateway.
pub struct GatewayControl {
    listener: tokio::net::TcpListener,
    bootstrap: Bootstrap,
    stream: Option<tokio::net::TcpStream>,
}
impl GatewayControl {
    pub async fn bind() -> io::Result<Self> {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let mut gateway_secret = [0; 32];
        let mut manager_secret = [0; 32];
        getrandom::fill(&mut gateway_secret).map_err(io::Error::other)?;
        getrandom::fill(&mut manager_secret).map_err(io::Error::other)?;
        Ok(Self {
            bootstrap: Bootstrap {
                port: listener.local_addr()?.port(),
                gateway_secret,
                manager_secret,
            },
            listener,
            stream: None,
        })
    }
    pub async fn bootstrap<W: AsyncWrite + Unpin>(&self, mut pipe: W) -> io::Result<()> {
        let mut data = serde_json::to_vec(&self.bootstrap)?;
        data.push(b'\n');
        pipe.write_all(&data).await?;
        pipe.shutdown().await
    }
    pub async fn list(&mut self) -> io::Result<ListResponse> {
        let result = tokio::time::timeout(DEADLINE, self.list_inner())
            .await
            .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "Gateway unavailable"));
        match result {
            Ok(Ok(reply)) => Ok(reply),
            other => {
                self.stream = None;
                Err(other
                    .err()
                    .unwrap_or_else(|| io::Error::other("Gateway unavailable")))
            }
        }
    }
    async fn list_inner(&mut self) -> io::Result<ListResponse> {
        if self.stream.is_none() {
            // Serial bounded authentication: no unbounded tasks or buffered sockets.
            loop {
                let (mut stream, _) = self.listener.accept().await?;
                let mut hello = [0; 32];
                if matches!(
                    tokio::time::timeout(Duration::from_millis(100), stream.read_exact(&mut hello))
                        .await,
                    Ok(Ok(_))
                ) && bool::from(hello.ct_eq(&self.bootstrap.gateway_secret))
                {
                    self.stream = Some(stream);
                    break;
                }
            }
        }
        let stream = self.stream.as_mut().expect("authenticated stream");
        let mut request = serde_json::to_vec(&ListRequest {
            secret: self.bootstrap.manager_secret,
            command: "list_unknown".into(),
        })?;
        request.push(b'\n');
        stream.write_all(&request).await?;
        read_json(stream, RESPONSE_LIMIT as u64).await
    }
}
