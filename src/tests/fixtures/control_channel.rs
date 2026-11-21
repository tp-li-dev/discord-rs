//! Test clients and stub peers for the production management protocol.
#![allow(dead_code)] // Integration crates each use a subset of this fixture.

use serde_json::Value;
use src::management::{self, Key, Protocol, Session};
use std::{io, time::Duration};
use tokio::{
    io::{AsyncRead, AsyncWrite, AsyncWriteExt},
    net::TcpStream,
    runtime::{Builder, Runtime},
    time::timeout,
};

pub async fn request(port: u16, secret: &[u8; 32], value: &Value) -> io::Result<Vec<u8>> {
    let mut stream = management::connect_tcp(port).await?;
    exchange(&mut stream, Protocol::Botd, Key::new(secret), value).await
}

pub async fn exchange(
    stream: &mut (impl AsyncRead + AsyncWrite + Unpin),
    protocol: Protocol,
    key: Key,
    value: &Value,
) -> io::Result<Vec<u8>> {
    let session = management::authenticate_client(stream, protocol, key).await?;
    session
        .write_request(stream, &serde_json::to_vec(value)?)
        .await?;
    session.read_response(stream).await
}

pub fn blocking_request(port: u16, secret: &[u8; 32], value: &Value) -> Option<Value> {
    // Some existing users call this from Drop, inside a Tokio runtime.
    std::thread::scope(|scope| {
        scope
            .spawn(|| {
                let rt = runtime();
                let body = rt.block_on(async {
                    timeout(Duration::from_millis(800), request(port, secret, value))
                        .await
                        .ok()?
                        .ok()
                })?;
                serde_json::from_slice(&body).ok()
            })
            .join()
            .expect("blocking control fixture")
    })
}

fn runtime() -> Runtime {
    Builder::new_current_thread().enable_all().build().unwrap()
}

pub async fn botd_request(
    stream: &mut (impl AsyncRead + AsyncWrite + Unpin),
    secret: &[u8; 32],
) -> (Session, Value) {
    let (session, _) = management::authenticate_server(stream, Protocol::Botd, &[Key::new(secret)])
        .await
        .expect("authenticate test client");
    let bytes = timeout(Duration::from_secs(3), session.read_request(stream))
        .await
        .expect("test request deadline")
        .expect("authenticated test request");
    (
        session,
        serde_json::from_slice(&bytes).expect("test request JSON"),
    )
}

pub struct BlockingPeer {
    runtime: Runtime,
    stream: TcpStream,
    session: Session,
}

impl BlockingPeer {
    pub fn botd(stream: std::net::TcpStream, secret: &[u8; 32]) -> io::Result<Self> {
        Self::new(stream, Protocol::Botd, &[Key::new(secret)])
    }

    pub fn manage(stream: std::net::TcpStream, admin: &str, viewer: &str) -> io::Result<Self> {
        Self::new(
            stream,
            Protocol::Manage,
            &[
                Key::from_legacy_secret(admin)?,
                Key::from_legacy_secret(viewer)?,
            ],
        )
    }

    fn new(stream: std::net::TcpStream, protocol: Protocol, keys: &[Key]) -> io::Result<Self> {
        let runtime = runtime();
        stream.set_nonblocking(true)?;
        stream.set_nodelay(true)?;
        let (stream, session) = runtime.block_on(async {
            let mut stream = TcpStream::from_std(stream)?;
            let (session, _) = management::authenticate_server(&mut stream, protocol, keys).await?;
            Ok::<_, io::Error>((stream, session))
        })?;
        Ok(Self {
            runtime,
            stream,
            session,
        })
    }

    pub fn read(&mut self) -> Value {
        let body = self.runtime.block_on(async {
            timeout(
                Duration::from_secs(3),
                self.session.read_request(&mut self.stream),
            )
            .await
            .unwrap()
            .unwrap()
        });
        let value: Value = serde_json::from_slice(&body).unwrap();
        assert!(value.get("secret").is_none());
        value
    }

    pub fn reply(&mut self, value: &Value) {
        self.reply_bytes(&serde_json::to_vec(value).unwrap());
    }

    pub fn reply_bytes(&mut self, bytes: &[u8]) {
        self.runtime.block_on(async {
            timeout(
                Duration::from_secs(3),
                self.session.write_response(&mut self.stream, bytes),
            )
            .await
            .unwrap()
            .unwrap()
        });
    }

    pub fn raw_reply(&mut self, bytes: &[u8]) {
        self.runtime.block_on(async {
            timeout(Duration::from_secs(3), self.stream.write_all(bytes))
                .await
                .unwrap()
                .unwrap()
        });
    }
}
