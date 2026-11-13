//! Real TCP/TLS boundary tests for the dedicated RPC listener. These exercise
//! authentication and bounds; multi-node quorum behavior is in raft_mtls.rs.
use openraft::{Vote, raft::VoteRequest};
use rcgen::{
    BasicConstraints, Certificate, CertificateParams, DistinguishedName, DnType,
    ExtendedKeyUsagePurpose, IsCa, KeyPair, KeyUsagePurpose,
};
use rustls::{
    ClientConfig, RootCertStore, ServerConfig,
    pki_types::{CertificateDer, PrivatePkcs8KeyDer, ServerName},
};
use src::{
    cluster::NodeId,
    raft_quorum::{
        NodeOptions, RaftNode, StorageMode,
        transport::{
            AuthenticatedChannel, AuthenticatedRpcAdapter, RpcEndpoint, RpcLimits, RpcRequest,
            RpcScope, TransportError as RpcError,
        },
    },
    redact::Secret,
    tls_transport::{
        RevocationList, TlsIdentity, TlsTransport, TlsTransportConfig, TransportError,
        TransportLimits, node_name,
        raft_rpc::{ALPN, TlsRpcTransport},
    },
};
use std::{collections::BTreeSet, net::SocketAddr, path::PathBuf, sync::Arc, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    time::{Instant, sleep, timeout},
};
use tokio_rustls::{TlsAcceptor, TlsConnector, client::TlsStream};

const WAIT: Duration = Duration::from_secs(60);

struct Ca {
    cert: Certificate,
    key: Secret<KeyPair>,
}

impl Ca {
    fn new() -> Self {
        let key = KeyPair::generate().unwrap();
        let mut params = CertificateParams::new(Vec::<String>::new()).unwrap();
        params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        params.key_usages = vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::CrlSign];
        Self {
            cert: params.self_signed(&key).unwrap(),
            key: Secret::new(key),
        }
    }

    fn root(&self) -> CertificateDer<'static> {
        self.cert.der().clone()
    }

    fn issue(&self, node: u64) -> TlsIdentity {
        let key = KeyPair::generate().unwrap();
        let mut params = CertificateParams::new(vec![node_name(NodeId(node))]).unwrap();
        params.distinguished_name = DistinguishedName::new();
        params
            .distinguished_name
            .push(DnType::CommonName, node_name(NodeId(node)));
        params.key_usages = vec![KeyUsagePurpose::DigitalSignature];
        params.extended_key_usages = vec![
            ExtendedKeyUsagePurpose::ClientAuth,
            ExtendedKeyUsagePurpose::ServerAuth,
        ];
        let cert = params
            .signed_by(&key, &self.cert, self.key.expose_secret())
            .unwrap();
        TlsIdentity {
            certificates: vec![cert.der().clone()],
            private_key: Secret::new(PrivatePkcs8KeyDer::from(key.serialize_der()).into()),
        }
    }

    fn config(&self, node: u64) -> TlsTransportConfig {
        let mut config = TlsTransportConfig::new(NodeId(node), self.issue(node), vec![self.root()]);
        config.allowed_nodes = BTreeSet::from([NodeId(1), NodeId(2), NodeId(3)]);
        config
    }

    fn client(&self, identity: Option<TlsIdentity>, alpn: &[u8]) -> ClientConfig {
        let mut roots = RootCertStore::empty();
        roots.add(self.root()).unwrap();
        let builder =
            ClientConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
                .with_protocol_versions(&[&rustls::version::TLS13])
                .unwrap()
                .with_root_certificates(roots);
        let mut config = match identity {
            Some(identity) => builder
                .with_client_auth_cert(identity.certificates, identity.private_key.into_inner())
                .unwrap(),
            None => builder.with_no_client_auth(),
        };
        config.alpn_protocols = vec![alpn.to_vec()];
        config
    }

    fn server(&self) -> ServerConfig {
        let provider = Arc::new(rustls::crypto::ring::default_provider());
        let mut roots = RootCertStore::empty();
        roots.add(self.root()).unwrap();
        let verifier = rustls::server::WebPkiClientVerifier::builder_with_provider(
            Arc::new(roots),
            provider.clone(),
        )
        .build()
        .unwrap();
        let identity = self.issue(2);
        let mut config = ServerConfig::builder_with_provider(provider)
            .with_protocol_versions(&[&rustls::version::TLS13])
            .unwrap()
            .with_client_cert_verifier(verifier)
            .with_single_cert(identity.certificates, identity.private_key.into_inner())
            .unwrap();
        config.alpn_protocols = vec![ALPN.to_vec()];
        config
    }
}

fn local() -> SocketAddr {
    "127.0.0.1:0".parse().unwrap()
}

async fn connect(
    config: ClientConfig,
    address: SocketAddr,
) -> std::io::Result<TlsStream<TcpStream>> {
    timeout(WAIT, async {
        TlsConnector::from(Arc::new(config))
            .connect(
                ServerName::try_from(node_name(NodeId(2))).unwrap(),
                TcpStream::connect(address).await?,
            )
            .await
    })
    .await
    .unwrap()
}

async fn greeted(ca: &Ca, rpc: &TlsRpcTransport) -> TlsStream<TcpStream> {
    let mut tls = connect(ca.client(Some(ca.issue(1)), ALPN), rpc.local_addr())
        .await
        .unwrap();
    let mut greeting = [0; 14];
    timeout(WAIT, tls.read_exact(&mut greeting))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(&greeting[..4], b"BRPC");
    assert_eq!(u16::from_be_bytes(greeting[4..6].try_into().unwrap()), 1);
    assert_eq!(u64::from_be_bytes(greeting[6..].try_into().unwrap()), 2);
    tls
}

async fn closed(stream: &mut (impl tokio::io::AsyncRead + Unpin)) {
    let mut byte = [0];
    let result = timeout(WAIT, stream.read(&mut byte))
        .await
        .expect("connection remained open");
    assert!(
        matches!(result, Ok(0) | Err(_)),
        "rejected stream carried application data"
    );
}

async fn diagnostic(rpc: &mut TlsRpcTransport, expected: TransportError) {
    let deadline = Instant::now() + WAIT;
    loop {
        if let Some(error) = rpc.take_error() {
            assert_eq!(error, expected);
            return;
        }
        assert!(
            Instant::now() < deadline,
            "expected bounded transport error"
        );
        sleep(Duration::from_millis(5)).await;
    }
}

async fn active(rpc: &TlsRpcTransport, count: usize) {
    let deadline = Instant::now() + WAIT;
    while rpc.stats().active_connections != count {
        assert!(
            Instant::now() < deadline,
            "connection permits did not converge"
        );
        sleep(Duration::from_millis(5)).await;
    }
}

#[tokio::test]
async fn dedicated_rpc_protocol_rejects_control_plaintext_and_missing_client_auth() {
    let ca = Ca::new();
    let mut tls = TlsTransport::bind(ca.config(2)).await.unwrap();
    assert!(matches!(
        tls.bind_raft_rpc("0.0.0.0:0".parse().unwrap()).await,
        Err(TransportError::ExternalBindDenied)
    ));
    let mut rpc = tls.bind_raft_rpc(local()).await.unwrap();
    for config in [
        ca.client(Some(ca.issue(1)), src::tls_transport::ALPN),
        ca.client(None, ALPN),
    ] {
        if let Ok(mut client) = connect(config, rpc.local_addr()).await {
            closed(&mut client).await;
        }
        diagnostic(&mut rpc, TransportError::Tls).await;
    }
    let mut plain = TcpStream::connect(rpc.local_addr()).await.unwrap();
    plain.write_all(b"not a TLS client hello\n").await.unwrap();
    // A raw TCP peer may receive a TLS alert record before EOF. Those bytes are
    // not decoded application data, unlike the TLS streams checked above.
    let mut alert = Vec::new();
    let _ = timeout(WAIT, plain.take(64).read_to_end(&mut alert))
        .await
        .expect("plaintext connection remained open");
    assert!(
        alert.len() < 64,
        "plaintext peer received an unbounded reply"
    );
    diagnostic(&mut rpc, TransportError::Tls).await;
    assert_eq!(rpc.stats().received, 0);
    assert_eq!(tls.stats().received, 0);

    // Raft clients never negotiate the legacy control protocol either.
    let mut source = TlsTransport::bind(ca.config(1)).await.unwrap();
    let mut sender = source.bind_raft_rpc(local()).await.unwrap();
    sender.set_peer(NodeId(2), tls.local_addr()).unwrap();
    assert!(sender.channel().exchange(2, vec![1], 256).await.is_err());
    assert_eq!(tls.stats().received, 0);
    sender.shutdown().await;
    source.shutdown().await;
    rpc.shutdown().await;
    tls.shutdown().await;
}

#[tokio::test]
async fn rpc_frames_and_handshakes_are_bounded_and_shutdown_disables_clones() {
    let ca = Ca::new();
    let mut config = ca.config(2);
    config.limits = TransportLimits {
        max_frame_len: 256,
        max_connections: 1,
        handshake_timeout: Duration::from_secs(2),
        read_timeout: Duration::from_millis(350),
        ..TransportLimits::default()
    };
    let mut tls = TlsTransport::bind(config).await.unwrap();
    let mut rpc = tls.bind_raft_rpc(local()).await.unwrap();
    let mut stalled = TcpStream::connect(rpc.local_addr()).await.unwrap();
    active(&rpc, 1).await;
    let mut excess = TcpStream::connect(rpc.local_addr()).await.unwrap();
    closed(&mut excess).await;
    diagnostic(&mut rpc, TransportError::ConnectionLimit).await;
    closed(&mut stalled).await;
    diagnostic(&mut rpc, TransportError::HandshakeTimeout).await;
    active(&rpc, 0).await;
    for length in [257_u32, 0] {
        let mut client = greeted(&ca, &rpc).await;
        client.write_u32(length).await.unwrap();
        closed(&mut client).await;
        diagnostic(
            &mut rpc,
            if length == 0 {
                TransportError::InvalidFrame
            } else {
                TransportError::FrameTooLarge
            },
        )
        .await;
        active(&rpc, 0).await;
    }
    let mut partial = greeted(&ca, &rpc).await;
    partial.write_all(&[0, 0]).await.unwrap();
    closed(&mut partial).await;
    diagnostic(&mut rpc, TransportError::ReadTimeout).await;
    active(&rpc, 0).await;
    let clone = rpc.channel();
    let mut idle = greeted(&ca, &rpc).await;
    rpc.shutdown().await;
    closed(&mut idle).await;
    assert_eq!(rpc.stats().active_connections, 0);
    assert_eq!(
        clone.exchange(1, vec![1], 1).await.err(),
        Some(RpcError::Unavailable)
    );
    assert_eq!(rpc.stats().received, 0);
    tls.shutdown().await;
}

struct Root(PathBuf);
impl Root {
    fn new() -> Self {
        let mut nonce = [0_u8; 16];
        getrandom::fill(&mut nonce).unwrap();
        let suffix: String = nonce.iter().map(|byte| format!("{byte:02x}")).collect();
        let path = std::env::temp_dir().join(format!("raftq-tls-boundary-{suffix}"));
        std::fs::create_dir(&path).unwrap();
        Self(path)
    }
}
impl Drop for Root {
    fn drop(&mut self) {
        if let (Ok(path), Ok(parent)) = (self.0.canonicalize(), std::env::temp_dir().canonicalize())
            && path.parent() == Some(parent.as_path())
            && path
                .file_name()
                .and_then(|n| n.to_str())
                .is_some_and(|n| n.starts_with("raftq-tls-boundary-"))
        {
            let _ = std::fs::remove_dir_all(path);
        }
    }
}

fn scope() -> RpcScope {
    RpcScope {
        cluster_id: [6; 16],
        members: BTreeSet::from([1, 2, 3]),
    }
}
fn request(term: u64) -> serde_json::Value {
    let nonce = [1_u8; 16];
    serde_json::json!({
        "version": 1, "scope": scope(), "from": 1, "to": 2, "nonce": nonce,
        "request": RpcRequest::Vote(VoteRequest::new(Vote::new(term, 1), None))
    })
}
async fn send(stream: &mut TlsStream<TcpStream>, body: &serde_json::Value) {
    let bytes = serde_json::to_vec(body).unwrap();
    stream.write_u32(bytes.len() as u32).await.unwrap();
    stream.write_all(&bytes).await.unwrap();
    stream.flush().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn authenticated_rpc_binds_sender_scope_target_and_rechecks_revocation_after_read() {
    let ca = Ca::new();
    let root = Root::new();
    let mut tls = TlsTransport::bind(ca.config(2)).await.unwrap();
    let mut rpc = tls.bind_raft_rpc(local()).await.unwrap();
    let limits = RpcLimits::default();
    let adapter = Arc::new(AuthenticatedRpcAdapter::new(rpc.channel(), scope(), limits).unwrap());
    let node = RaftNode::start(
        root.0.join("node"),
        2,
        scope().members,
        StorageMode::Create,
        NodeOptions {
            allow_test_mode: true,
            ..NodeOptions::default()
        },
        adapter,
    )
    .await
    .unwrap();
    rpc.set_endpoint(Arc::new(
        RpcEndpoint::new(2, scope(), node.raft.clone(), limits).unwrap(),
    ))
    .unwrap();
    let mut valid = greeted(&ca, &rpc).await;
    send(&mut valid, &request(5)).await;
    let length = timeout(WAIT, valid.read_u32()).await.unwrap().unwrap();
    let mut body = vec![0; length as usize];
    timeout(WAIT, valid.read_exact(&mut body))
        .await
        .unwrap()
        .unwrap();
    let response: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(response["from"], 2);
    assert_eq!(response["to"], 1);
    assert!(response["result"]["Ok"]["Vote"].is_object());
    active(&rpc, 0).await;
    let committed_vote = node.raft.metrics().borrow().current_term;
    assert_eq!(committed_vote, 5);

    for field in ["from", "to", "scope", "request"] {
        let mut forged = request(100);
        match field {
            "from" => forged["from"] = 3.into(),
            "to" => forged["to"] = 3.into(),
            "scope" => forged["scope"]["cluster_id"] = serde_json::to_value([9_u8; 16]).unwrap(),
            "request" => {
                forged["request"] = serde_json::to_value(RpcRequest::Vote(VoteRequest::new(
                    Vote::new(100, 3),
                    None,
                )))
                .unwrap()
            }
            _ => unreachable!(),
        }
        let mut client = greeted(&ca, &rpc).await;
        send(&mut client, &forged).await;
        closed(&mut client).await;
        diagnostic(
            &mut rpc,
            if field == "scope" {
                TransportError::InvalidFrame
            } else {
                TransportError::IdentityMismatch
            },
        )
        .await;
        assert_eq!(node.raft.metrics().borrow().current_term, committed_vote);
    }

    let mut pending = greeted(&ca, &rpc).await;
    tls.set_revocations(RevocationList {
        nodes: BTreeSet::from([NodeId(1)]),
        ..RevocationList::default()
    })
    .unwrap();
    send(&mut pending, &request(200)).await;
    closed(&mut pending).await;
    diagnostic(&mut rpc, TransportError::CertificateRevoked).await;
    assert_eq!(node.raft.metrics().borrow().current_term, committed_vote);
    assert_eq!(rpc.stats().received, 1);
    rpc.shutdown().await;
    node.shutdown().await.unwrap();
    tls.shutdown().await;
}

#[tokio::test]
async fn outbound_response_length_and_cancellation_release_capacity() {
    let ca = Ca::new();
    let listener = TcpListener::bind(local()).await.unwrap();
    let address = listener.local_addr().unwrap();
    let acceptor = TlsAcceptor::from(Arc::new(ca.server()));
    let server = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        let mut tls = acceptor.accept(socket).await.unwrap();
        let mut greeting = b"BRPC".to_vec();
        greeting.extend_from_slice(&1_u16.to_be_bytes());
        greeting.extend_from_slice(&2_u64.to_be_bytes());
        tls.write_all(&greeting).await.unwrap();
        let length = tls.read_u32().await.unwrap();
        let mut request = vec![0; length as usize];
        tls.read_exact(&mut request).await.unwrap();
        tls.write_u32(257).await.unwrap();
        tls.flush().await.unwrap();
        closed(&mut tls).await;
    });
    let mut source = TlsTransport::bind(ca.config(1)).await.unwrap();
    let mut rpc = source.bind_raft_rpc(local()).await.unwrap();
    rpc.set_peer(NodeId(2), address).unwrap();
    assert_eq!(
        rpc.channel().exchange(2, vec![1], 256).await.err(),
        Some(RpcError::FrameTooLarge)
    );
    timeout(WAIT, server).await.unwrap().unwrap();
    active(&rpc, 0).await;
    let listener = TcpListener::bind(local()).await.unwrap();
    rpc.set_peer(NodeId(2), listener.local_addr().unwrap())
        .unwrap();
    let channel = rpc.channel();
    let exchange = tokio::spawn(async move { channel.exchange(2, vec![1], 256).await });
    let (_socket, _) = timeout(WAIT, listener.accept()).await.unwrap().unwrap();
    active(&rpc, 1).await;
    exchange.abort();
    match exchange.await {
        Err(error) => assert!(error.is_cancelled()),
        Ok(_) => panic!("exchange completed before cancellation"),
    }
    active(&rpc, 0).await;
    rpc.shutdown().await;
    source.shutdown().await;
}

#[tokio::test]
async fn revocation_while_response_is_stalled_never_returns_an_authenticated_success() {
    let ca = Ca::new();
    let listener = TcpListener::bind(local()).await.unwrap();
    let address = listener.local_addr().unwrap();
    let acceptor = TlsAcceptor::from(Arc::new(ca.server()));
    let (processed_tx, processed_rx) = tokio::sync::oneshot::channel();
    let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        let mut tls = acceptor.accept(socket).await.unwrap();
        let mut greeting = b"BRPC".to_vec();
        greeting.extend_from_slice(&1_u16.to_be_bytes());
        greeting.extend_from_slice(&2_u64.to_be_bytes());
        tls.write_all(&greeting).await.unwrap();
        let length = tls.read_u32().await.unwrap();
        let mut request = vec![0; length as usize];
        tls.read_exact(&mut request).await.unwrap();
        assert_eq!(request, [42]);
        // The remote side has processed its input before local revocation.
        // No rollback claim is implied by rejecting the later response.
        processed_tx.send(()).unwrap();
        reply_rx.await.unwrap();
        tls.write_u32(1).await.unwrap();
        tls.write_all(&[43]).await.unwrap();
        tls.flush().await.unwrap();
        closed(&mut tls).await;
    });
    let mut source = TlsTransport::bind(ca.config(1)).await.unwrap();
    let mut rpc = source.bind_raft_rpc(local()).await.unwrap();
    rpc.set_peer(NodeId(2), address).unwrap();
    let channel = rpc.channel();
    let exchange = tokio::spawn(async move { channel.exchange(2, vec![42], 256).await });
    timeout(WAIT, processed_rx).await.unwrap().unwrap();
    source
        .set_revocations(RevocationList {
            nodes: BTreeSet::from([NodeId(2)]),
            ..RevocationList::default()
        })
        .unwrap();
    reply_tx.send(()).unwrap();
    assert_eq!(
        timeout(WAIT, exchange).await.unwrap().unwrap().err(),
        Some(RpcError::Unauthenticated)
    );
    diagnostic(&mut rpc, TransportError::CertificateRevoked).await;
    timeout(WAIT, server).await.unwrap().unwrap();
    assert_eq!(rpc.stats().delivered, 0);
    assert_eq!(rpc.stats().failed, 1);
    active(&rpc, 0).await;
    rpc.shutdown().await;
    source.shutdown().await;
}
