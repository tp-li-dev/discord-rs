//! Real loopback TCP/TLS boundary tests. These do not establish distributed
//! consensus, cross-host clock safety, production failover or Discord guarantees.
use rcgen::{
    BasicConstraints, Certificate, CertificateParams, DistinguishedName, DnType,
    ExtendedKeyUsagePurpose, GeneralSubtree, IsCa, KeyPair, KeyUsagePurpose, NameConstraints,
    date_time_ymd,
};
use rustls::{
    ClientConfig, RootCertStore, ServerConfig,
    pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer, ServerName},
};
use src::{
    cluster::{
        ControlMessage, Envelope, Heartbeat, ManualClock, NodeId, OwnershipProposal,
        OwnershipRecord, PeerTable, ProposalKind, Transport,
    },
    failover::{self, Event, Failover, State},
    ownership::{Clock, Lease, OwnershipKey},
    protocol::{BotId, OwnershipEpoch},
    redact::Secret,
    tls_transport::{TlsIdentity, TlsTransport, TlsTransportConfig, TransportError, node_name},
};
use std::{future::Future, net::SocketAddr, sync::Arc, task::Poll, time::Duration};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    time::{sleep, timeout},
};
use tokio_rustls::{TlsAcceptor, TlsConnector, client::TlsStream};

const A: NodeId = NodeId(1);
const B: NodeId = NodeId(2);
const C: NodeId = NodeId(3);
const WAIT: Duration = Duration::from_secs(5);
const ALPN: &[u8] = b"botd-cluster/1";

struct TestCa {
    certificate: Certificate,
    key: Secret<KeyPair>,
}

impl TestCa {
    fn new() -> Self {
        let key = KeyPair::generate().unwrap();
        let mut params = CertificateParams::new(Vec::<String>::new()).unwrap();
        params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        params.key_usages = vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::CrlSign];
        params.distinguished_name = DistinguishedName::new();
        params
            .distinguished_name
            .push(DnType::CommonName, "ephemeral test CA");
        Self {
            certificate: params.self_signed(&key).unwrap(),
            key: Secret::new(key),
        }
    }

    fn root(&self) -> CertificateDer<'static> {
        self.certificate.der().clone()
    }

    fn issue(&self, node: NodeId) -> TlsIdentity {
        self.issue_params(node_params(node))
    }

    fn issue_params(&self, params: CertificateParams) -> TlsIdentity {
        let key = KeyPair::generate().unwrap();
        let cert = params
            .signed_by(&key, &self.certificate, self.key.expose_secret())
            .unwrap();
        identity(cert, key)
    }

    fn config(&self, node: NodeId, members: &[NodeId]) -> TlsTransportConfig {
        let mut config = TlsTransportConfig::new(node, self.issue(node), vec![self.root()]);
        config.allowed_nodes = members.iter().copied().collect();
        config
    }
}

fn node_params(node: NodeId) -> CertificateParams {
    let mut params = CertificateParams::new(vec![node_name(node)]).unwrap();
    params.distinguished_name = DistinguishedName::new();
    params
        .distinguished_name
        .push(DnType::CommonName, node_name(node));
    params.key_usages = vec![KeyUsagePurpose::DigitalSignature];
    params.extended_key_usages = vec![
        ExtendedKeyUsagePurpose::ClientAuth,
        ExtendedKeyUsagePurpose::ServerAuth,
    ];
    params
}

fn identity(cert: Certificate, key: KeyPair) -> TlsIdentity {
    TlsIdentity {
        certificates: vec![cert.der().clone()],
        private_key: Secret::new(PrivatePkcs8KeyDer::from(key.serialize_der()).into()),
    }
}

fn client_config(ca: &TestCa, identity: Option<TlsIdentity>) -> ClientConfig {
    let mut roots = RootCertStore::empty();
    roots.add(ca.root()).unwrap();
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
    config.alpn_protocols = vec![ALPN.to_vec()];
    config
}

fn server_config(ca: &TestCa, identity: TlsIdentity) -> ServerConfig {
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let mut roots = RootCertStore::empty();
    roots.add(ca.root()).unwrap();
    let verifier = rustls::server::WebPkiClientVerifier::builder_with_provider(
        Arc::new(roots),
        provider.clone(),
    )
    .build()
    .unwrap();
    // Deliberately bypass TlsTransport's local certificate preflight so tests
    // can offer invalid peer material. TLS still authenticates client A.
    let mut config = ServerConfig::builder_with_provider(provider)
        .with_protocol_versions(&[&rustls::version::TLS13])
        .unwrap()
        .with_client_cert_verifier(verifier)
        .with_single_cert(identity.certificates, identity.private_key.into_inner())
        .unwrap();
    config.alpn_protocols = vec![ALPN.to_vec()];
    config
}

fn server_greeting() -> Vec<u8> {
    let mut greeting = b"BTLS".to_vec();
    greeting.extend_from_slice(&1_u16.to_be_bytes());
    greeting.extend_from_slice(&B.0.to_be_bytes());
    greeting.extend_from_slice(&1_u64.to_be_bytes());
    greeting
}

async fn raw_connect(
    config: ClientConfig,
    addr: SocketAddr,
) -> std::io::Result<TlsStream<TcpStream>> {
    timeout(WAIT, async move {
        let socket = TcpStream::connect(addr).await?;
        TlsConnector::from(Arc::new(config))
            .connect(ServerName::try_from(node_name(B)).unwrap(), socket)
            .await
    })
    .await
    .expect("TLS connect exceeded test deadline")
}

async fn greeting(stream: &mut TlsStream<TcpStream>) -> std::io::Result<u64> {
    let mut bytes = [0; 22];
    timeout(WAIT, stream.read_exact(&mut bytes))
        .await
        .expect("server greeting exceeded test deadline")?;
    assert_eq!(&bytes[..4], b"BTLS");
    assert_eq!(u16::from_be_bytes(bytes[4..6].try_into().unwrap()), 1);
    assert_eq!(u64::from_be_bytes(bytes[6..14].try_into().unwrap()), B.0);
    Ok(u64::from_be_bytes(bytes[14..22].try_into().unwrap()))
}

async fn raw_authenticated(ca: &TestCa, server: &TlsTransport) -> TlsStream<TcpStream> {
    let mut stream = raw_connect(client_config(ca, Some(ca.issue(A))), server.local_addr())
        .await
        .unwrap();
    assert_eq!(greeting(&mut stream).await.unwrap(), 1);
    stream
}

async fn closed(stream: &mut (impl AsyncRead + Unpin)) {
    let mut byte = [0; 1];
    let result = timeout(WAIT, stream.read(&mut byte))
        .await
        .expect("rejected connection remained open");
    assert!(
        matches!(result, Ok(0) | Err(_)),
        "rejected frame received an ACK"
    );
}

async fn diagnostic(transport: &mut TlsTransport, expected: TransportError) {
    timeout(WAIT, async {
        loop {
            if let Some(error) = transport.take_error() {
                assert_eq!(error, expected);
                break;
            }
            sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("missing rejection diagnostic");
}

async fn connections(transport: &TlsTransport, expected: usize) {
    timeout(WAIT, async {
        while transport.stats().active_connections != expected {
            sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("connection permits were not acquired/released");
}

async fn received(transport: &mut TlsTransport) -> Envelope {
    timeout(WAIT, async {
        loop {
            if let Some(envelope) = transport.receive().unwrap() {
                return envelope;
            }
            sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("expected envelope was not delivered")
}

async fn pair(ca: &TestCa) -> (TlsTransport, TlsTransport) {
    let mut a = TlsTransport::bind(ca.config(A, &[A, B])).await.unwrap();
    let mut b = TlsTransport::bind(ca.config(B, &[A, B])).await.unwrap();
    a.set_peer(B, b.local_addr()).unwrap();
    b.set_peer(A, a.local_addr()).unwrap();
    (a, b)
}

fn heartbeat(sequence: u64, sent_at: Duration) -> ControlMessage {
    ControlMessage::Heartbeat(Heartbeat {
        sequence,
        sent_at,
        ownership: Vec::new(),
    })
}

fn record(bot: u64, shard: u32) -> OwnershipRecord {
    OwnershipRecord {
        owner: A,
        lease: Lease {
            key: OwnershipKey {
                bot_id: BotId(bot),
                shard_id: shard,
            },
            epoch: OwnershipEpoch(17),
            expires_at: Duration::new(31, 500),
        },
    }
}

fn proposal(kind: ProposalKind) -> OwnershipProposal {
    OwnershipProposal {
        id: 29,
        requester: A,
        requester_incarnation: 1,
        target: B,
        target_incarnation: 1,
        key: OwnershipKey {
            bot_id: BotId(9),
            shard_id: 3,
        },
        expected_epoch: OwnershipEpoch(17),
        kind,
        attempt: 2,
        requested_at: Duration::new(4, 5),
        expires_at: Duration::new(10, 9),
        ack_deadline: Duration::new(7, 3),
    }
}

fn raw_heartbeat(from: NodeId, to: NodeId, to_incarnation: u64) -> Vec<u8> {
    let mut body = b"BCTL".to_vec();
    body.extend_from_slice(&1_u16.to_be_bytes());
    for value in [1_u64, from.0, to.0, 1, to_incarnation] {
        body.extend_from_slice(&value.to_be_bytes());
    }
    body.extend_from_slice(&[0; 12]); // envelope sent_at
    body.push(0); // Heartbeat
    body.extend_from_slice(&1_u64.to_be_bytes());
    body.extend_from_slice(&[0; 12]); // heartbeat sent_at
    body.extend_from_slice(&0_u32.to_be_bytes());
    assert_eq!(body.len(), 83);
    body
}

async fn write_body(stream: &mut TlsStream<TcpStream>, body: &[u8]) {
    timeout(WAIT, async {
        stream
            .write_all(&(body.len() as u32).to_be_bytes())
            .await
            .unwrap();
        stream.write_all(body).await.unwrap();
        stream.flush().await.unwrap();
    })
    .await
    .expect("frame write exceeded test deadline");
}

async fn bind_error(config: TlsTransportConfig) -> TransportError {
    match TlsTransport::bind(config).await {
        Err(error) => error,
        Ok(mut transport) => {
            transport.shutdown().await;
            panic!("invalid configuration was accepted")
        }
    }
}

#[tokio::test]
async fn mtls_roundtrips_every_control_variant_and_keeps_bot_shard_identity() {
    let ca = TestCa::new();
    let (mut a, mut b) = pair(&ca).await;
    let messages = [
        ControlMessage::Heartbeat(Heartbeat {
            sequence: u64::MAX,
            sent_at: Duration::new(9, 123),
            ownership: vec![record(11, 0), record(22, 0), record(11, 2)],
        }),
        ControlMessage::OwnershipProposal(proposal(ProposalKind::Acquire)),
        ControlMessage::OwnershipProposal(proposal(ProposalKind::Renew)),
        ControlMessage::OwnershipProposal(proposal(ProposalKind::Failback)),
        ControlMessage::OwnershipAck {
            proposal: proposal(ProposalKind::Acquire),
        },
        ControlMessage::OwnershipDecision {
            proposal: proposal(ProposalKind::Acquire),
            granted: true,
            current: Some(record(9, 3)),
        },
        ControlMessage::OwnershipDecision {
            proposal: proposal(ProposalKind::Acquire),
            granted: false,
            current: None,
        },
    ];
    for message in messages {
        a.send_confirmed(A, B, message.clone()).await.unwrap();
        let delivered = received(&mut b).await;
        assert_eq!((delivered.from, delivered.to), (A, B));
        assert_eq!(
            (delivered.from_incarnation, delivered.to_incarnation),
            (1, 1)
        );
        assert_eq!(delivered.message, message);
        b.send_confirmed(B, A, message.clone()).await.unwrap();
        assert_eq!(received(&mut a).await.message, message);
    }
    assert_eq!(a.stats().delivered, 7);
    assert_eq!(b.stats().received, 7);
    a.shutdown().await;
    b.shutdown().await;
}

#[tokio::test]
async fn trait_send_receive_preserves_authenticated_sender() {
    let ca = TestCa::new();
    let (mut a, mut b) = pair(&ca).await;
    let msg = heartbeat(1, Duration::ZERO);
    a.send(A, B, msg.clone()).unwrap();
    let envelope = received(&mut b).await;
    assert_eq!(envelope.from, A);
    assert_eq!(envelope.message, msg);
    assert!(a.send(C, B, msg.clone()).is_err());
    assert!(a.send(A, C, msg).is_err());
    assert!(a.set_peer(C, b.local_addr()).is_err());
    a.shutdown().await;
    b.shutdown().await;
}

#[tokio::test]
async fn cn_only_ca_signed_identities_are_accepted() {
    let ca = TestCa::new();
    let mut config_a = ca.config(A, &[A, B]);
    let mut params_a = node_params(A);
    params_a.subject_alt_names.clear();
    config_a.identity = ca.issue_params(params_a);
    let mut config_b = ca.config(B, &[A, B]);
    let mut params_b = node_params(B);
    params_b.subject_alt_names.clear();
    config_b.identity = ca.issue_params(params_b);
    let mut a = TlsTransport::bind(config_a).await.unwrap();
    let mut b = TlsTransport::bind(config_b).await.unwrap();
    a.set_peer(B, b.local_addr()).unwrap();
    b.set_peer(A, a.local_addr()).unwrap();
    a.send_confirmed(A, B, heartbeat(1, Duration::ZERO))
        .await
        .unwrap();
    assert_eq!(received(&mut b).await.from, A);
    b.send_confirmed(B, A, heartbeat(2, Duration::ZERO))
        .await
        .unwrap();
    assert_eq!(received(&mut a).await.from, B);
    a.shutdown().await;
    b.shutdown().await;
}

#[tokio::test]
async fn cn_fallback_cannot_bypass_intermediate_or_root_dns_name_constraints() {
    let ca = TestCa::new();
    for intermediate in [true, false] {
        let constrained_key = KeyPair::generate().unwrap();
        let mut params = CertificateParams::new(Vec::<String>::new()).unwrap();
        params.distinguished_name = DistinguishedName::new();
        params
            .distinguished_name
            .push(DnType::CommonName, "constrained ephemeral CA");
        params.is_ca = IsCa::Ca(BasicConstraints::Constrained(0));
        params.key_usages = vec![KeyUsagePurpose::KeyCertSign];
        params.name_constraints = Some(NameConstraints {
            permitted_subtrees: vec![GeneralSubtree::DnsName(node_name(C))],
            excluded_subtrees: Vec::new(),
        });
        let constrained_cert = if intermediate {
            params
                .signed_by(&constrained_key, &ca.certificate, ca.key.expose_secret())
                .unwrap()
        } else {
            params.self_signed(&constrained_key).unwrap()
        };
        let issuer = TestCa {
            certificate: constrained_cert,
            key: Secret::new(constrained_key),
        };
        let mut config = ca.config(B, &[A, B, C]);
        if !intermediate {
            config.trusted_cas.push(issuer.root());
        }
        let mut server = TlsTransport::bind(config).await.unwrap();
        let mut cn_only = node_params(A);
        cn_only.subject_alt_names.clear();
        let cases = [
            ("CN cannot evade DNS constraints", cn_only, false),
            ("out of range SAN", node_params(A), false),
            ("in range SAN baseline", node_params(C), true),
        ];
        for (case, params, valid) in cases {
            let mut identity = issuer.issue_params(params);
            if intermediate {
                identity.certificates.push(issuer.root());
            }
            let connection =
                raw_connect(client_config(&ca, Some(identity)), server.local_addr()).await;
            if valid {
                let mut stream = connection.unwrap();
                assert_eq!(greeting(&mut stream).await.unwrap(), 1, "{case}");
                write_body(&mut stream, &raw_heartbeat(C, B, 1)).await;
                let mut ack = [0];
                timeout(WAIT, stream.read_exact(&mut ack))
                    .await
                    .unwrap()
                    .unwrap();
                assert_eq!(ack, [1]);
                assert_eq!(received(&mut server).await.from, C);
            } else {
                if let Ok(mut stream) = connection {
                    assert!(
                        greeting(&mut stream).await.is_err(),
                        "{case}; intermediate={intermediate}"
                    );
                }
                diagnostic(&mut server, TransportError::Tls).await;
                assert!(server.receive().unwrap().is_none());
            }
        }
        server.shutdown().await;
    }
}

#[tokio::test]
async fn untrusted_expired_unauthorized_and_ambiguous_client_certificates_are_rejected() {
    let ca = TestCa::new();
    let fake_ca = TestCa::new();
    let mut server = TlsTransport::bind(ca.config(B, &[A, B])).await.unwrap();
    let mut expired = node_params(A);
    expired.not_before = date_time_ymd(2000, 1, 1);
    expired.not_after = date_time_ymd(2001, 1, 1);
    let mut future = node_params(A);
    future.not_before = date_time_ymd(2998, 1, 1);
    future.not_after = date_time_ymd(2999, 1, 1);
    let mut disagree = node_params(A);
    disagree
        .distinguished_name
        .push(DnType::CommonName, node_name(C));
    let mut two_nodes = node_params(A);
    two_nodes
        .subject_alt_names
        .push(rcgen::SanType::DnsName(node_name(C).try_into().unwrap()));
    let mut unrelated_san = node_params(A);
    unrelated_san.subject_alt_names = vec![rcgen::SanType::DnsName(
        "unrelated.example".try_into().unwrap(),
    )];
    let mut wildcard = node_params(A);
    wildcard.subject_alt_names = vec![rcgen::SanType::DnsName("*.cluster".try_into().unwrap())];
    let mut wrong_usage = node_params(A);
    wrong_usage.extended_key_usages = vec![ExtendedKeyUsagePurpose::ServerAuth];
    let self_key = KeyPair::generate().unwrap();
    let self_cert = node_params(A).self_signed(&self_key).unwrap();
    let cases = [
        ("not allowlisted", ca.issue(C)),
        ("untrusted CA", fake_ca.issue(A)),
        ("self signed leaf", identity(self_cert, self_key)),
        ("expired", ca.issue_params(expired)),
        ("not yet valid", ca.issue_params(future)),
        ("SAN CN disagreement", ca.issue_params(disagree)),
        ("ambiguous SAN", ca.issue_params(two_nodes)),
        ("SAN cannot fall back to CN", ca.issue_params(unrelated_san)),
        ("wildcard identity", ca.issue_params(wildcard)),
        ("wrong EKU", ca.issue_params(wrong_usage)),
    ];
    for (case, identity) in cases {
        if let Ok(mut stream) =
            raw_connect(client_config(&ca, Some(identity)), server.local_addr()).await
        {
            assert!(
                greeting(&mut stream).await.is_err(),
                "{case} received authenticated greeting"
            );
        }
        diagnostic(&mut server, TransportError::Tls).await;
        assert!(
            server.receive().unwrap().is_none(),
            "{case} delivered a message"
        );
    }
    assert_eq!(server.stats().received, 0);
    server.shutdown().await;
}

#[tokio::test]
async fn missing_client_certificate_and_wrong_alpn_are_rejected() {
    let ca = TestCa::new();
    let mut server = TlsTransport::bind(ca.config(B, &[A, B])).await.unwrap();
    let mut wrong_alpn = client_config(&ca, Some(ca.issue(A)));
    wrong_alpn.alpn_protocols = vec![b"another-protocol".to_vec()];
    let mut missing_alpn = client_config(&ca, Some(ca.issue(A)));
    missing_alpn.alpn_protocols.clear();
    for config in [client_config(&ca, None), wrong_alpn, missing_alpn] {
        if let Ok(mut stream) = raw_connect(config, server.local_addr()).await {
            assert!(greeting(&mut stream).await.is_err());
        }
        diagnostic(&mut server, TransportError::Tls).await;
    }
    assert!(server.receive().unwrap().is_none());
    server.shutdown().await;
}

#[tokio::test]
async fn plaintext_is_rejected_without_reaching_the_control_queue() {
    let ca = TestCa::new();
    let mut server = TlsTransport::bind(ca.config(B, &[A, B])).await.unwrap();
    let mut plain = TcpStream::connect(server.local_addr()).await.unwrap();
    plain
        .write_all(b"GET /manage HTTP/1.1\r\nHost: localhost\r\n\r\n")
        .await
        .unwrap();
    // A raw TCP peer may receive a TLS alert before EOF; that is not a control ACK.
    let mut response = Vec::new();
    let _ = timeout(WAIT, plain.take(64).read_to_end(&mut response))
        .await
        .expect("plaintext connection remained open");
    assert!(response.len() < 64);
    assert!(response.first().is_none_or(|byte| *byte == 21));
    diagnostic(&mut server, TransportError::Tls).await;
    assert!(server.receive().unwrap().is_none());
    server.shutdown().await;
}

#[tokio::test]
async fn tls12_cannot_negotiate_a_daemon_session() {
    let ca = TestCa::new();
    let mut server = TlsTransport::bind(ca.config(B, &[A, B])).await.unwrap();
    let mut roots = RootCertStore::empty();
    roots.add(ca.root()).unwrap();
    let identity = ca.issue(A);
    let mut config =
        ClientConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
            .with_protocol_versions(&[&rustls::version::TLS12])
            .unwrap()
            .with_root_certificates(roots)
            .with_client_auth_cert(identity.certificates, identity.private_key.into_inner())
            .unwrap();
    config.alpn_protocols = vec![ALPN.to_vec()];
    assert!(raw_connect(config, server.local_addr()).await.is_err());
    diagnostic(&mut server, TransportError::Tls).await;
    assert!(server.receive().unwrap().is_none());
    server.shutdown().await;
}

#[tokio::test]
async fn outgoing_connections_validate_server_chain_validity_and_usage() {
    let ca = TestCa::new();
    let fake_ca = TestCa::new();
    let mut expired = node_params(B);
    expired.not_before = date_time_ymd(2000, 1, 1);
    expired.not_after = date_time_ymd(2001, 1, 1);
    let mut future = node_params(B);
    future.not_before = date_time_ymd(2998, 1, 1);
    future.not_after = date_time_ymd(2999, 1, 1);
    let mut wrong_usage = node_params(B);
    wrong_usage.extended_key_usages = vec![ExtendedKeyUsagePurpose::ClientAuth];
    let self_key = KeyPair::generate().unwrap();
    let self_cert = node_params(B).self_signed(&self_key).unwrap();
    let cases = [
        ("valid server", ca.issue(B), true),
        ("untrusted server CA", fake_ca.issue(B), false),
        ("expired server", ca.issue_params(expired), false),
        ("not yet valid server", ca.issue_params(future), false),
        (
            "server client-only EKU",
            ca.issue_params(wrong_usage),
            false,
        ),
        (
            "self signed server leaf",
            identity(self_cert, self_key),
            false,
        ),
    ];
    for (case, identity, valid) in cases {
        let server_config = server_config(&ca, identity);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let actor = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            let Ok(mut stream) = TlsAcceptor::from(Arc::new(server_config))
                .accept(socket)
                .await
            else {
                return false;
            };
            stream.write_all(&server_greeting()).await.unwrap();
            stream.flush().await.unwrap();
            let length = stream.read_u32().await.unwrap() as usize;
            assert!(length <= 4096);
            let mut message = vec![0; length];
            stream.read_exact(&mut message).await.unwrap();
            assert_eq!(&message[..4], b"BCTL");
            stream.write_all(&[1]).await.unwrap();
            stream.flush().await.unwrap();
            true
        });
        let mut a = TlsTransport::bind(ca.config(A, &[A, B])).await.unwrap();
        a.set_peer(B, address).unwrap();
        let result = a.send_confirmed(A, B, heartbeat(1, Duration::ZERO)).await;
        assert_eq!(
            result,
            if valid {
                Ok(())
            } else {
                Err(TransportError::Tls)
            },
            "{case}"
        );
        assert_eq!(
            timeout(WAIT, actor).await.unwrap().unwrap(),
            valid,
            "{case}"
        );
        a.shutdown().await;
    }
}

#[tokio::test]
async fn expected_server_node_is_verified_before_delivery() {
    let ca = TestCa::new();
    let mut a = TlsTransport::bind(ca.config(A, &[A, B, C])).await.unwrap();
    let mut c = TlsTransport::bind(ca.config(C, &[A, B, C])).await.unwrap();
    a.set_peer(B, c.local_addr()).unwrap();
    assert_eq!(
        a.send_confirmed(A, B, heartbeat(1, Duration::ZERO)).await,
        Err(TransportError::Tls)
    );
    assert!(c.receive().unwrap().is_none());
    a.shutdown().await;
    c.shutdown().await;
}

#[tokio::test]
async fn local_identity_and_private_key_errors_do_not_expose_secrets() {
    let ca = TestCa::new();
    let mut mismatch = ca.config(A, &[A, B]);
    mismatch.identity = ca.issue(B);
    assert_eq!(bind_error(mismatch).await, TransportError::IdentityMismatch);
    let identity = ca.issue(A);
    assert_eq!(format!("{:?}", identity.private_key), "[REDACTED]");
    assert_eq!(format!("{:#?}", identity.private_key), "[REDACTED]");
    let mut malformed = ca.config(A, &[A, B]);
    let sentinel = "ephemeral-key-material-must-never-be-logged";
    malformed.identity.private_key = Secret::new(PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(
        sentinel.as_bytes().to_vec(),
    )));
    let config_debug = format!("{malformed:?}");
    assert!(!config_debug.contains(sentinel));
    assert!(config_debug.contains("[REDACTED]"));
    let error = bind_error(malformed).await;
    assert!(!format!("{error:?} {error}").contains(sentinel));
}

#[tokio::test]
async fn loopback_default_external_bind_opt_in_and_invalid_limits() {
    let ca = TestCa::new();
    assert!(ca.config(A, &[A]).bind_addr.ip().is_loopback());
    let mut external = ca.config(A, &[A]);
    external.bind_addr = "0.0.0.0:0".parse().unwrap();
    assert_eq!(
        bind_error(external).await,
        TransportError::ExternalBindDenied
    );
    let mut external = ca.config(A, &[A]);
    external.bind_addr = "0.0.0.0:0".parse().unwrap();
    external.allow_external_bind = true;
    let mut transport = TlsTransport::bind(external).await.unwrap();
    assert!(transport.local_addr().ip().is_unspecified());
    transport.shutdown().await;
    for field in 0..7 {
        let mut config = ca.config(A, &[A]);
        match field {
            0 => config.limits.max_frame_len = 0,
            1 => config.limits.max_connections = 0,
            2 => config.limits.queue_capacity = 0,
            3 => config.limits.handshake_timeout = Duration::ZERO,
            4 => config.limits.read_timeout = Duration::ZERO,
            5 => config.limits.write_timeout = Duration::ZERO,
            _ => config.incarnation = 0,
        }
        assert_eq!(bind_error(config).await, TransportError::InvalidConfig);
    }
}

#[tokio::test]
async fn oversized_zero_and_malformed_frames_are_rejected_before_delivery() {
    let ca = TestCa::new();
    let mut config = ca.config(B, &[A, B]);
    config.limits.max_frame_len = 4096;
    let mut server = TlsTransport::bind(config).await.unwrap();
    for (size, expected) in [
        (u32::MAX, TransportError::FrameTooLarge),
        (4097, TransportError::FrameTooLarge),
        (0, TransportError::InvalidFrame),
    ] {
        let mut stream = raw_authenticated(&ca, &server).await;
        stream.write_all(&size.to_be_bytes()).await.unwrap();
        stream.flush().await.unwrap();
        closed(&mut stream).await;
        diagnostic(&mut server, expected).await;
        assert!(server.receive().unwrap().is_none());
    }
    let mut invalid_version = raw_heartbeat(A, B, 1);
    invalid_version[5] = 2;
    let mut trailing = raw_heartbeat(A, B, 1);
    trailing.push(0);
    let mut invalid_duration = raw_heartbeat(A, B, 1);
    invalid_duration[54..58].copy_from_slice(&1_000_000_000_u32.to_be_bytes());
    let mut exaggerated_count = raw_heartbeat(A, B, 1);
    exaggerated_count[79..83].copy_from_slice(&u32::MAX.to_be_bytes());
    for body in [
        vec![0; 1],
        invalid_version,
        trailing,
        invalid_duration,
        exaggerated_count,
    ] {
        let mut stream = raw_authenticated(&ca, &server).await;
        write_body(&mut stream, &body).await;
        closed(&mut stream).await;
        diagnostic(&mut server, TransportError::InvalidFrame).await;
        assert!(server.receive().unwrap().is_none());
    }
    server.shutdown().await;
}

#[tokio::test]
async fn wire_sender_destination_and_receiver_incarnation_are_bound_to_tls() {
    let ca = TestCa::new();
    let mut server = TlsTransport::bind(ca.config(B, &[A, B, C])).await.unwrap();
    for body in [
        raw_heartbeat(C, B, 1),
        raw_heartbeat(A, C, 1),
        raw_heartbeat(A, B, 2),
    ] {
        let mut stream = raw_authenticated(&ca, &server).await;
        write_body(&mut stream, &body).await;
        closed(&mut stream).await;
        diagnostic(&mut server, TransportError::IdentityMismatch).await;
        assert!(server.receive().unwrap().is_none());
    }
    // A correct authenticated packet still works after rejected connections.
    let mut stream = raw_authenticated(&ca, &server).await;
    write_body(&mut stream, &raw_heartbeat(A, B, 1)).await;
    let mut ack = [0];
    timeout(WAIT, stream.read_exact(&mut ack))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(ack, [1]);
    assert_eq!(received(&mut server).await.from, A);
    server.shutdown().await;
}

#[tokio::test]
async fn handshake_slowloris_is_limited_by_one_absolute_deadline() {
    let ca = TestCa::new();
    let mut config = ca.config(B, &[A, B]);
    config.limits.handshake_timeout = Duration::from_millis(150);
    let mut server = TlsTransport::bind(config).await.unwrap();
    let mut stream = TcpStream::connect(server.local_addr()).await.unwrap();
    stream.write_all(&[22, 3, 3, 0, 100]).await.unwrap();
    let (mut reader, mut writer) = stream.into_split();
    let trickle = tokio::spawn(async move {
        for _ in 0..40 {
            if writer.write_all(&[0]).await.is_err() {
                break;
            }
            sleep(Duration::from_millis(30)).await;
        }
    });
    timeout(Duration::from_millis(900), closed(&mut reader))
        .await
        .expect("handshake deadline reset by bytes");
    diagnostic(&mut server, TransportError::HandshakeTimeout).await;
    trickle.abort();
    let _ = trickle.await;
    connections(&server, 0).await;
    assert!(server.receive().unwrap().is_none());
    server.shutdown().await;
}

#[tokio::test]
async fn authenticated_frame_slowloris_is_limited_by_one_absolute_deadline() {
    let ca = TestCa::new();
    let mut config = ca.config(B, &[A, B]);
    config.limits.read_timeout = Duration::from_millis(150);
    let mut server = TlsTransport::bind(config).await.unwrap();
    let mut stream = raw_authenticated(&ca, &server).await;
    stream.write_all(&100_u32.to_be_bytes()).await.unwrap();
    stream.flush().await.unwrap();
    let (mut reader, mut writer) = tokio::io::split(stream);
    let trickle = tokio::spawn(async move {
        for _ in 0..40 {
            if writer.write_all(&[0]).await.is_err() || writer.flush().await.is_err() {
                break;
            }
            sleep(Duration::from_millis(30)).await;
        }
    });
    timeout(Duration::from_millis(900), closed(&mut reader))
        .await
        .expect("frame deadline reset by bytes");
    diagnostic(&mut server, TransportError::ReadTimeout).await;
    trickle.abort();
    let _ = trickle.await;
    connections(&server, 0).await;
    assert!(server.receive().unwrap().is_none());
    server.shutdown().await;
}

#[tokio::test]
async fn partial_length_prefix_uses_the_same_absolute_frame_deadline() {
    let ca = TestCa::new();
    let mut config = ca.config(B, &[A, B]);
    config.limits.read_timeout = Duration::from_millis(300);
    let mut server = TlsTransport::bind(config).await.unwrap();
    let stream = raw_authenticated(&ca, &server).await;
    let (mut reader, mut writer) = tokio::io::split(stream);
    let trickle = tokio::spawn(async move {
        for byte in 100_u32.to_be_bytes() {
            if writer.write_all(&[byte]).await.is_err() || writer.flush().await.is_err() {
                return;
            }
            sleep(Duration::from_millis(140)).await;
        }
        // A per-byte prefix deadline would accept the final byte around 420ms,
        // then wait until at least 720ms for the missing body. Keep the socket
        // open so that EOF cannot accidentally make that implementation pass.
        std::future::pending::<()>().await;
    });
    timeout(Duration::from_millis(600), closed(&mut reader))
        .await
        .expect("partial length prefix extended the whole-frame deadline");
    diagnostic(&mut server, TransportError::ReadTimeout).await;
    trickle.abort();
    let _ = trickle.await;
    connections(&server, 0).await;
    assert!(server.receive().unwrap().is_none());
    server.shutdown().await;
}

#[tokio::test]
async fn outgoing_handshake_timeout_releases_its_connection_permit() {
    let ca = TestCa::new();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (accepted_tx, accepted_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = tokio::sync::oneshot::channel();
    let actor = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        accepted_tx.send(()).unwrap();
        let _ = release_rx.await;
        drop(socket);
    });
    let mut config = ca.config(A, &[A, B]);
    config.limits.handshake_timeout = Duration::from_millis(150);
    let mut a = TlsTransport::bind(config).await.unwrap();
    a.set_peer(B, address).unwrap();
    assert_eq!(
        timeout(WAIT, a.send_confirmed(A, B, heartbeat(1, Duration::ZERO)))
            .await
            .expect("outgoing handshake ignored its deadline"),
        Err(TransportError::HandshakeTimeout)
    );
    timeout(WAIT, accepted_rx).await.unwrap().unwrap();
    diagnostic(&mut a, TransportError::HandshakeTimeout).await;
    connections(&a, 0).await;
    assert_eq!(a.stats().delivered, 0);
    release_tx.send(()).unwrap();
    timeout(WAIT, actor).await.unwrap().unwrap();
    a.shutdown().await;
}

#[tokio::test]
async fn outgoing_greeting_and_ack_reads_each_have_a_finite_deadline() {
    let ca = TestCa::new();
    for withhold_ack in [false, true] {
        let server_config = server_config(&ca, ca.issue(B));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (authenticated_tx, authenticated_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = tokio::sync::oneshot::channel();
        let actor = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            let mut stream = TlsAcceptor::from(Arc::new(server_config))
                .accept(socket)
                .await
                .unwrap();
            authenticated_tx.send(()).unwrap();
            if withhold_ack {
                stream.write_all(&server_greeting()).await.unwrap();
                stream.flush().await.unwrap();
                let length = stream.read_u32().await.unwrap() as usize;
                assert!(length <= 4096);
                let mut message = vec![0; length];
                stream.read_exact(&mut message).await.unwrap();
                assert_eq!(&message[..4], b"BCTL");
            }
            let _ = release_rx.await;
            drop(stream);
        });
        let mut config = ca.config(A, &[A, B]);
        config.limits.read_timeout = Duration::from_millis(150);
        let mut a = TlsTransport::bind(config).await.unwrap();
        a.set_peer(B, address).unwrap();
        assert_eq!(
            timeout(WAIT, a.send_confirmed(A, B, heartbeat(1, Duration::ZERO)))
                .await
                .expect("outgoing read ignored its deadline"),
            Err(TransportError::ReadTimeout),
            "withhold_ack={withhold_ack}"
        );
        timeout(WAIT, authenticated_rx).await.unwrap().unwrap();
        diagnostic(&mut a, TransportError::ReadTimeout).await;
        connections(&a, 0).await;
        assert_eq!(a.stats().delivered, 0);
        release_tx.send(()).unwrap();
        timeout(WAIT, actor).await.unwrap().unwrap();
        a.shutdown().await;
    }
}

#[tokio::test]
async fn connection_limit_rejects_before_unbounded_handshake_tasks_and_recovers() {
    let ca = TestCa::new();
    let mut config = ca.config(B, &[A, B]);
    config.limits.max_connections = 2;
    let mut server = TlsTransport::bind(config).await.unwrap();
    let first = TcpStream::connect(server.local_addr()).await.unwrap();
    let second = TcpStream::connect(server.local_addr()).await.unwrap();
    connections(&server, 2).await;
    let mut excess = TcpStream::connect(server.local_addr()).await.unwrap();
    closed(&mut excess).await;
    diagnostic(&mut server, TransportError::ConnectionLimit).await;
    assert_eq!(server.stats().active_connections, 2);
    drop((first, second));
    connections(&server, 0).await;
    let mut valid = raw_authenticated(&ca, &server).await;
    write_body(&mut valid, &raw_heartbeat(A, B, 1)).await;
    assert_eq!(received(&mut server).await.from, A);
    server.shutdown().await;
}

#[tokio::test]
async fn bounded_receive_queue_does_not_ack_dropped_messages_and_recovers() {
    let ca = TestCa::new();
    let mut config = ca.config(B, &[A, B]);
    config.limits.queue_capacity = 1;
    let mut b = TlsTransport::bind(config).await.unwrap();
    let mut a = TlsTransport::bind(ca.config(A, &[A, B])).await.unwrap();
    a.set_peer(B, b.local_addr()).unwrap();
    a.send_confirmed(A, B, heartbeat(1, Duration::ZERO))
        .await
        .unwrap();
    assert!(
        a.send_confirmed(A, B, heartbeat(2, Duration::ZERO))
            .await
            .is_err()
    );
    diagnostic(&mut b, TransportError::QueueFull).await;
    assert_eq!(received(&mut b).await.message, heartbeat(1, Duration::ZERO));
    assert!(b.receive().unwrap().is_none());
    a.send_confirmed(A, B, heartbeat(3, Duration::ZERO))
        .await
        .unwrap();
    assert_eq!(received(&mut b).await.message, heartbeat(3, Duration::ZERO));
    a.shutdown().await;
    b.shutdown().await;
}

#[tokio::test]
async fn bounded_send_queue_and_outbound_frame_limit_are_immediate() {
    let ca = TestCa::new();
    let mut config = ca.config(A, &[A, B]);
    config.limits.queue_capacity = 1;
    config.limits.max_frame_len = 4096;
    let mut a = TlsTransport::bind(config).await.unwrap();
    // No await between these calls: the current-thread worker cannot drain the queue.
    a.set_peer(B, "127.0.0.1:9".parse().unwrap()).unwrap();
    a.send(A, B, heartbeat(1, Duration::ZERO)).unwrap();
    assert!(a.send(A, B, heartbeat(2, Duration::ZERO)).is_err());
    a.shutdown().await;
    let (mut a, mut b) = pair(&ca).await;
    let huge = ControlMessage::Heartbeat(Heartbeat {
        sequence: 1,
        sent_at: Duration::ZERO,
        ownership: vec![record(1, 0); 30_000],
    });
    assert_eq!(
        a.send_confirmed(A, B, huge).await,
        Err(TransportError::FrameTooLarge)
    );
    assert!(b.receive().unwrap().is_none());
    a.shutdown().await;
    b.shutdown().await;
}

#[tokio::test]
async fn shutdown_closes_listener_active_connections_and_future_sends() {
    let ca = TestCa::new();
    let (mut a, mut b) = pair(&ca).await;
    let addr = b.local_addr();
    let mut stalled = TcpStream::connect(addr).await.unwrap();
    connections(&b, 1).await;
    timeout(WAIT, b.shutdown())
        .await
        .expect("shutdown stalled on peer I/O");
    closed(&mut stalled).await;
    assert!(TcpStream::connect(addr).await.is_err());
    assert_eq!(b.stats().active_connections, 0);
    assert!(b.send(B, A, heartbeat(1, Duration::ZERO)).is_err());
    assert!(
        a.send_confirmed(A, B, heartbeat(1, Duration::ZERO))
            .await
            .is_err()
    );
    b.shutdown().await; // Idempotent resource cleanup.
    a.shutdown().await;
}

#[tokio::test]
async fn canceled_shutdown_can_be_awaited_again_and_joins_every_connection() {
    let ca = TestCa::new();
    let mut b = TlsTransport::bind(ca.config(B, &[A, B])).await.unwrap();
    let address = b.local_addr();
    let mut stalled = TcpStream::connect(address).await.unwrap();
    let mut authenticated = raw_authenticated(&ca, &b).await;
    connections(&b, 2).await;
    let mut shutting_down = Box::pin(b.shutdown());
    // Poll exactly once on the current-thread runtime. The stop request is sent,
    // but the actor cannot run or finish its child joins before this is canceled.
    std::future::poll_fn(|cx| {
        assert!(shutting_down.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    drop(shutting_down);
    assert_eq!(b.stats().active_connections, 2);
    assert_eq!(
        b.send_confirmed(B, A, heartbeat(1, Duration::ZERO)).await,
        Err(TransportError::Closed)
    );
    timeout(WAIT, b.shutdown())
        .await
        .expect("second shutdown lost the unfinished actor handle");
    assert_eq!(b.stats().active_connections, 0);
    closed(&mut stalled).await;
    closed(&mut authenticated).await;
    assert!(TcpStream::connect(address).await.is_err());
    assert!(b.receive().is_err());
}

#[tokio::test]
async fn real_tcp_heartbeat_loss_drives_suspicion_failover_decision_and_quorum_stop() {
    let ca = TestCa::new();
    let (mut a, mut b) = pair(&ca).await;
    // Like cluster_sim, peers share an injected clock; TCP I/O itself is real.
    // This deliberately makes no cross-host clock-skew or lease-safety claim.
    let clock = ManualClock::default();
    let config = failover::Config::default();
    let mut peers = PeerTable::new(&[A, B], clock.now());
    let mut failover = Failover::new(clock.clone(), config).unwrap();
    assert_eq!(
        failover.handle(Event::Quorum(true)).unwrap(),
        State::Assigned
    );
    for sequence in 1..=4 {
        clock.advance(config.heartbeat_interval).unwrap();
        a.send_confirmed(A, B, heartbeat(sequence, clock.now()))
            .await
            .unwrap();
        let packet = received(&mut b).await;
        let ControlMessage::Heartbeat(hb) = packet.message else {
            panic!("expected heartbeat");
        };
        assert!(peers.observe(
            packet.from,
            packet.from_incarnation,
            &hb,
            clock.now(),
            config.suspect_after
        ));
        assert_eq!(failover.handle(Event::Heartbeat).unwrap(), State::Assigned);
    }
    let last_seen = peers.get(A).unwrap().last_seen.unwrap();
    a.shutdown().await;
    clock
        .set(last_seen + config.suspect_after - Duration::from_nanos(1))
        .unwrap();
    assert!(peers.healthy(A, clock.now(), config.suspect_after));
    assert_eq!(failover.handle(Event::Tick).unwrap(), State::Assigned);
    clock.advance(Duration::from_nanos(1)).unwrap();
    assert!(!peers.healthy(A, clock.now(), config.suspect_after));
    assert_eq!(failover.handle(Event::Tick).unwrap(), State::Suspected);
    clock
        .set(last_seen + config.fail_after - Duration::from_nanos(1))
        .unwrap();
    assert_eq!(failover.handle(Event::Tick).unwrap(), State::Suspected);
    clock.advance(Duration::from_nanos(1)).unwrap();
    assert_eq!(
        failover.handle(Event::Tick).unwrap(),
        State::FailoverPending
    );
    // Detection never authorizes a new owner. Loss of quorum prevents promotion.
    assert_eq!(
        failover.handle(Event::Quorum(false)).unwrap(),
        State::Degraded
    );
    assert_eq!(
        failover
            .handle(Event::OwnershipApproved {
                attempt: failover.attempt()
            })
            .unwrap(),
        State::Degraded
    );
    assert!(b.receive().unwrap().is_none());
    b.shutdown().await;
}
