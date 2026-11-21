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
const LEGACY_BOTCTL_REJECTION: &[u8] =
    b"{\"ok\":false,\"offline\":false,\"error\":\"incompatible local control protocol; upgrade botctl and botd together\"}\n";
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
        let mut hello = [0; 8 + FIELD];
        hello[..8].copy_from_slice(protocol.magic());
        hello[8..].copy_from_slice(&transcript.client);
        stream.write_all(&hello).await?;
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
        let mut magic = [0; 8];
        stream.read_exact(&mut magic).await?;
        if &magic != protocol.magic() {
            // Historical botctl expects a JSON error. Reject from the prefix
            // alone, without reading its bearer credential or parsing a body.
            if matches!(protocol, Protocol::Botd) && magic[0] == b'{' {
                let _ = stream.write_all(LEGACY_BOTCTL_REJECTION).await;
                let _ = stream.shutdown().await;
            }
            return Err(auth_error());
        }
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
        let mut challenge = Vec::with_capacity(8 + (1 + keys.len()) * FIELD);
        challenge.extend_from_slice(protocol.magic());
        challenge.extend_from_slice(&transcript.server);
        for key in keys {
            challenge.extend_from_slice(&transcript.tag(key, b'S', &[]));
        }
        stream.write_all(&challenge).await?;
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

    pub async fn read_request(&self, stream: &mut (impl AsyncRead + Unpin)) -> io::Result<Vec<u8>> {
        self.read_frame(stream, b'Q', self.request_limit()).await
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

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{DuplexStream, duplex};

    const TEST_KEY: [u8; 32] = [7; 32];

    async fn session_pair() -> (Session, Session) {
        let (mut client, mut server) = duplex(8192);
        let keys = [Key::new(&TEST_KEY)];
        let (client, server) = tokio::join!(
            authenticate_client(&mut client, Protocol::Botd, Key::new(&TEST_KEY)),
            authenticate_server(&mut server, Protocol::Botd, &keys)
        );
        (client.unwrap(), server.unwrap().0)
    }

    #[tokio::test]
    async fn request_mac_replay_and_modified_response_are_rejected() {
        let (first_client, first_server) = session_pair().await;
        let (_, second_server) = session_pair().await;
        let mut wire = Vec::new();
        first_client
            .write_request(&mut wire, br#"{"command":"status"}"#)
            .await
            .unwrap();
        let request = first_server
            .read_request(&mut wire.as_slice())
            .await
            .unwrap();
        assert_eq!(request.as_slice(), br#"{"command":"status"}"#);
        let error = second_server
            .read_request(&mut wire.as_slice())
            .await
            .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        for change_client in [false, true] {
            let original = &first_server.transcript;
            let mut transcript = Transcript {
                protocol: original.protocol,
                client: original.client,
                server: original.server,
            };
            if change_client {
                transcript.client[0] ^= 1;
            } else {
                transcript.server[0] ^= 1;
            }
            let changed = Session {
                transcript,
                key: first_server.key.clone(),
            };
            let error = changed
                .read_request(&mut wire.as_slice())
                .await
                .unwrap_err();
            assert_eq!(error.kind(), io::ErrorKind::InvalidData);
        }

        wire.clear();
        first_server
            .write_response(&mut wire, br#"{"ok":true}"#)
            .await
            .unwrap();
        assert!(
            first_client
                .read_response(&mut wire.as_slice())
                .await
                .is_ok()
        );
        wire[5] ^= 1;
        let error = first_client
            .read_response(&mut wire.as_slice())
            .await
            .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    }

    #[tokio::test]
    async fn reflected_proofs_are_rejected_in_both_directions() {
        let (mut client, mut server) = duplex(8192);
        let keys = [Key::new(&TEST_KEY)];
        let echo_server_proof = async {
            client.write_all(Protocol::Botd.magic()).await.unwrap();
            client.write_all(&[4; FIELD]).await.unwrap();
            let mut challenge = [0; 8 + 2 * FIELD];
            client.read_exact(&mut challenge).await.unwrap();
            client.write_all(&challenge[8 + FIELD..]).await.unwrap();
        };
        let ((), result) = tokio::join!(
            echo_server_proof,
            authenticate_server(&mut server, Protocol::Botd, &keys)
        );
        assert!(matches!(result, Err(error) if error.kind() == io::ErrorKind::PermissionDenied));

        let (mut client, mut server) = duplex(8192);
        let echo_client_proof = async move {
            read_magic(&mut server, Protocol::Botd).await.unwrap();
            let mut transcript = Transcript {
                protocol: Protocol::Botd,
                client: [0; FIELD],
                server: [5; FIELD],
            };
            server.read_exact(&mut transcript.client).await.unwrap();
            let client_proof = transcript.tag(&Key::new(&TEST_KEY), b'C', &[]);
            server.write_all(Protocol::Botd.magic()).await.unwrap();
            server.write_all(&transcript.server).await.unwrap();
            server.write_all(&client_proof).await.unwrap();
            let mut next = [0; 1];
            assert_eq!(server.read(&mut next).await.unwrap(), 0);
        };
        let authenticate = async move {
            let result =
                authenticate_client(&mut client, Protocol::Botd, Key::new(&TEST_KEY)).await;
            assert!(
                matches!(result, Err(error) if error.kind() == io::ErrorKind::PermissionDenied)
            );
        };
        timeout(Duration::from_secs(3), async {
            tokio::join!(echo_client_proof, authenticate);
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn partial_first_frame_does_not_extend_deadline() {
        let (mut client, mut server) = duplex(8192);
        let keys = [Key::new(&TEST_KEY)];
        let slowly = async {
            for byte in b"DRSB" {
                client.write_all(&[*byte]).await.unwrap();
                tokio::time::sleep(Duration::from_millis(170)).await;
            }
            // Keep the writer open so a truncated frame cannot explain the error.
            std::future::pending::<()>().await;
        };
        let result = timeout(Duration::from_millis(1800), async {
            tokio::select! {
                result = authenticate_server(&mut server, Protocol::Botd, &keys) => result,
                _ = slowly => unreachable!(),
            }
        })
        .await
        .expect("partial input must expire at the initial deadline");
        assert!(matches!(result, Err(error) if error.kind() == io::ErrorKind::TimedOut));
    }

    #[tokio::test]
    async fn botd_rejects_legacy_client_before_dispatch() {
        let (mut client, mut server): (DuplexStream, DuplexStream) = duplex(8192);
        let keys = [Key::new(&TEST_KEY)];
        let old_request = serde_json::to_vec(&serde_json::json!({
            "secret": TEST_KEY, "command": "status"
        }))
        .unwrap();
        client.write_all(&old_request).await.unwrap();
        let result = authenticate_server(&mut server, Protocol::Botd, &keys).await;
        assert!(matches!(result, Err(error) if error.kind() == io::ErrorKind::PermissionDenied));
        let mut reply = Vec::new();
        client.read_to_end(&mut reply).await.unwrap();
        let reply: serde_json::Value = serde_json::from_slice(&reply).unwrap();
        assert_eq!(reply["ok"], false);
        assert_eq!(
            reply["error"],
            "incompatible local control protocol; upgrade botctl and botd together"
        );
    }
}
