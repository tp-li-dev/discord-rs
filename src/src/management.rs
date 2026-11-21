//! One mutually authenticated request/reply on the local management channel.
use rustls::crypto::tls13::OkmBlock;
use std::{
    collections::VecDeque,
    future::{Future, poll_fn},
    io,
    pin::Pin,
    sync::Arc,
    task::Poll,
    time::Duration,
};
use subtle::{Choice, ConstantTimeEq};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    task::JoinSet,
    time::timeout,
};

mod endpoint;
#[cfg(target_os = "linux")]
pub use endpoint::connect_unix;
pub use endpoint::{Listener, Stream, connect_tcp};

const FIELD: usize = 32;
const PRE_AUTH_LIMIT: usize = 8;
const AUTHENTICATED_LIMIT: usize = 32;
const HANDSHAKE_STAGE: Duration = Duration::from_secs(1);
pub const AUTHENTICATION_FAILED: &str =
    "local control authentication failed; check the peer release and management credentials";
pub const RESPONSE_LIMIT: usize = 65535;
pub const BOTD_REQUEST_LIMIT: usize = 4095;
pub const MANAGE_REQUEST_LIMIT: usize = 4094;

#[derive(Clone, Copy)]
pub enum Protocol {
    Botd,
    Manage,
}

impl Protocol {
    fn magic(self) -> &'static [u8; 8] {
        match self {
            Self::Botd => b"DRSBOT01",
            Self::Manage => b"DRSMGT01",
        }
    }

    fn credentials(self) -> usize {
        match self {
            Self::Botd => 1,
            Self::Manage => 2,
        }
    }
}

#[derive(Clone)]
pub struct Key(OkmBlock);

impl Key {
    pub fn new(secret: &[u8; 32]) -> Self {
        Self(OkmBlock::new(secret))
    }

    /// The legacy endpoint files store 32 random bytes as lowercase hex.
    pub fn from_legacy_secret(secret: &str) -> io::Result<Self> {
        if secret.len() != 64 || !secret.bytes().all(|byte| byte.is_ascii_hexdigit()) {
            return Err(auth_error());
        }
        Ok(Self(OkmBlock::new(secret.as_bytes())))
    }
}

struct Transcript {
    protocol: Protocol,
    client: [u8; FIELD],
    server: [u8; FIELD],
}

impl Transcript {
    fn tag(&self, key: &Key, label: u8, data: &[u8]) -> [u8; FIELD] {
        let mut input = Vec::with_capacity(8 + 1 + 2 * FIELD + data.len());
        input.extend_from_slice(self.protocol.magic());
        input.push(label);
        input.extend_from_slice(&self.client);
        input.extend_from_slice(&self.server);
        input.extend_from_slice(data);
        let provider = rustls::crypto::ring::cipher_suite::TLS13_AES_128_GCM_SHA256
            .tls13()
            .expect("TLS 1.3 suite")
            .hkdf_provider;
        provider.hmac_sign(&key.0, &input).as_ref()[..FIELD]
            .try_into()
            .expect("SHA256 tag length")
    }
}

pub struct Session {
    transcript: Transcript,
    key: Key,
}

fn auth_error() -> io::Error {
    io::Error::new(io::ErrorKind::PermissionDenied, AUTHENTICATION_FAILED)
}

fn frame_error() -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        "invalid authenticated control frame",
    )
}

fn nonce() -> io::Result<[u8; FIELD]> {
    let mut bytes = [0; FIELD];
    getrandom::fill(&mut bytes).map_err(|_| io::Error::other("control entropy unavailable"))?;
    Ok(bytes)
}

async fn read_magic(stream: &mut (impl AsyncRead + Unpin), protocol: Protocol) -> io::Result<()> {
    let mut magic = [0; 8];
    stream.read_exact(&mut magic).await?;
    if &magic != protocol.magic() {
        return Err(auth_error());
    }
    Ok(())
}

/// Until the server proof is verified the client sends only magic and random bytes.
pub async fn authenticate_client<S: AsyncRead + AsyncWrite + Unpin>(
    stream: &mut S,
    protocol: Protocol,
    key: Key,
) -> io::Result<Session> {
    timeout(HANDSHAKE_STAGE * 2, async {
        let mut transcript = Transcript {
            protocol,
            client: nonce()?,
            server: [0; FIELD],
        };
        stream.write_all(protocol.magic()).await?;
        stream.write_all(&transcript.client).await?;
        read_magic(stream, protocol).await?;
        stream.read_exact(&mut transcript.server).await?;
        let expected = transcript.tag(&key, b'S', &[]);
        let mut matched = Choice::from(0);
        for _ in 0..protocol.credentials() {
            let mut proof = [0; FIELD];
            stream.read_exact(&mut proof).await?;
            matched |= expected.ct_eq(&proof);
        }
        if !bool::from(matched) {
            return Err(auth_error());
        }
        stream.write_all(&transcript.tag(&key, b'C', &[])).await?;
        Ok(Session { transcript, key })
    })
    .await
    .map_err(|_| auth_error())?
    .map_err(|_: io::Error| auth_error())
}

/// Verify a fixed-size client proof before letting the caller read a body.
/// The returned key index maps to a locally configured role.
pub async fn authenticate_server<S: AsyncRead + AsyncWrite + Unpin>(
    stream: &mut S,
    protocol: Protocol,
    keys: &[Key],
) -> io::Result<(Session, usize)> {
    if keys.len() != protocol.credentials() {
        return Err(auth_error());
    }
    let mut client = [0; FIELD];
    timeout(HANDSHAKE_STAGE, async {
        read_magic(stream, protocol).await?;
        stream.read_exact(&mut client).await?;
        Ok::<(), io::Error>(())
    })
    .await
    .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "control hello timed out"))??;
    timeout(HANDSHAKE_STAGE, async {
        let transcript = Transcript {
            protocol,
            client,
            server: nonce()?,
        };
        stream.write_all(protocol.magic()).await?;
        stream.write_all(&transcript.server).await?;
        for key in keys {
            stream.write_all(&transcript.tag(key, b'S', &[])).await?;
        }
        let mut proof = [0; FIELD];
        stream.read_exact(&mut proof).await?;
        // Always check both legacy roles. Prefer the first configured key if equal.
        let mut selected = None;
        for (index, key) in keys.iter().enumerate() {
            let valid = bool::from(transcript.tag(key, b'C', &[]).ct_eq(&proof));
            if valid && selected.is_none() {
                selected = Some(index);
            }
        }
        let index = selected.ok_or_else(auth_error)?;
        Ok((
            Session {
                transcript,
                key: keys[index].clone(),
            },
            index,
        ))
    })
    .await
    .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "control proof timed out"))?
}

impl Session {
    pub async fn write_request(
        &self,
        stream: &mut (impl AsyncWrite + Unpin),
        body: &[u8],
    ) -> io::Result<()> {
        self.write_frame(stream, b'Q', body).await
    }

    pub async fn read_request(
        &self,
        stream: &mut (impl AsyncRead + Unpin),
        limit: usize,
    ) -> io::Result<Vec<u8>> {
        self.read_frame(stream, b'Q', limit).await
    }

    pub async fn write_response(
        &self,
        stream: &mut (impl AsyncWrite + Unpin),
        body: &[u8],
    ) -> io::Result<()> {
        self.write_frame(stream, b'R', body).await
    }

    pub async fn read_response(
        &self,
        stream: &mut (impl AsyncRead + Unpin),
    ) -> io::Result<Vec<u8>> {
        self.read_frame(stream, b'R', RESPONSE_LIMIT).await
    }

    fn request_limit(&self) -> usize {
        match self.transcript.protocol {
            Protocol::Botd => BOTD_REQUEST_LIMIT,
            Protocol::Manage => MANAGE_REQUEST_LIMIT,
        }
    }

    async fn write_frame(
        &self,
        stream: &mut (impl AsyncWrite + Unpin),
        label: u8,
        body: &[u8],
    ) -> io::Result<()> {
        let limit = if label == b'Q' {
            self.request_limit()
        } else {
            RESPONSE_LIMIT
        };
        if body.is_empty() || body.len() > limit {
            return Err(frame_error());
        }
        let mut frame = Vec::with_capacity(4 + body.len() + FIELD);
        frame.extend_from_slice(&(body.len() as u32).to_be_bytes());
        frame.extend_from_slice(body);
        let tag = self.transcript.tag(&self.key, label, &frame);
        frame.extend_from_slice(&tag);
        stream.write_all(&frame).await
    }

    async fn read_frame(
        &self,
        stream: &mut (impl AsyncRead + Unpin),
        label: u8,
        limit: usize,
    ) -> io::Result<Vec<u8>> {
        let mut len = [0; 4];
        stream.read_exact(&mut len).await?;
        let size = u32::from_be_bytes(len) as usize;
        if size == 0 || size > limit {
            return Err(frame_error());
        }
        let mut frame = vec![0; 4 + size];
        frame[..4].copy_from_slice(&len);
        stream.read_exact(&mut frame[4..]).await?;
        let mut supplied = [0; FIELD];
        stream.read_exact(&mut supplied).await?;
        let expected = self.transcript.tag(&self.key, label, &frame);
        if !bool::from(expected.ct_eq(&supplied)) {
            return Err(frame_error());
        }
        Ok(frame.split_off(4))
    }
}

type Authenticated = (Stream, Session, usize);
type Handshake = Pin<Box<dyn Future<Output = io::Result<Authenticated>> + Send>>;

/// Bound pre-auth work without pausing accept behind idle readers. Pending
/// handshakes live here, so eviction closes the stream synchronously.
pub async fn serve<F>(
    listener: Listener,
    protocol: Protocol,
    keys: Vec<Key>,
    handle: impl Fn(Stream, Session, usize) -> F,
    rejected: impl Fn(),
) -> io::Result<()>
where
    F: Future<Output = ()> + Send + 'static,
{
    let keys = Arc::new(keys);
    let mut pending = VecDeque::<Handshake>::with_capacity(PRE_AUTH_LIMIT);
    let mut sessions = JoinSet::new();
    loop {
        tokio::select! {
            biased;
            _ = sessions.join_next(), if !sessions.is_empty() => {}
            result = poll_fn(|cx| {
                for (index, handshake) in pending.iter_mut().enumerate() {
                    if let Poll::Ready(result) = handshake.as_mut().poll(cx) {
                        return Poll::Ready((index, result));
                    }
                }
                Poll::Pending
            }), if !pending.is_empty() => {
                let (index, result) = result;
                drop(pending.remove(index));
                match result {
                    Ok((stream, session, key)) if sessions.len() < AUTHENTICATED_LIMIT => {
                        sessions.spawn(handle(stream, session, key));
                    }
                    Ok(_) => {}
                    Err(_) => rejected(),
                }
            }
            accepted = listener.accept() => {
                let Some(mut stream) = accepted? else { continue };
                if pending.len() == PRE_AUTH_LIMIT {
                    drop(pending.pop_front());
                }
                let keys = keys.clone();
                pending.push_back(Box::pin(async move {
                    let (session, key) = authenticate_server(&mut stream, protocol, &keys).await?;
                    Ok((stream, session, key))
                }));
            }
        }
    }
}
