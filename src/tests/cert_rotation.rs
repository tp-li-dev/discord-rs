//! Certificate lifecycle tests over real loopback TCP. Keys are ephemeral and
//! remain in memory. These tests do not establish cross-host or production safety.
use rcgen::{
    BasicConstraints, Certificate, CertificateParams, DistinguishedName, DnType,
    ExtendedKeyUsagePurpose, IsCa, KeyPair, KeyUsagePurpose, date_time_ymd,
};
use rustls::{
    ClientConfig, RootCertStore, ServerConfig,
    pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer, ServerName},
};
use src::{
    cluster::{ControlMessage, Envelope, Heartbeat, NodeId, Transport},
    redact::Secret,
    tls_transport::{
        ALPN, CertificateSerial, RevocationList, TlsIdentity, TlsTransport, TlsTransportConfig,
        TransportError, certificate_serial, node_name,
    },
};
use std::{
    collections::BTreeSet,
    net::SocketAddr,
    process::Command,
    sync::Arc,
    time::{Duration, SystemTime},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::oneshot,
    time::{sleep, timeout},
};
use tokio_rustls::{TlsAcceptor, TlsConnector, client::TlsStream};

const A: NodeId = NodeId(1);
const B: NodeId = NodeId(2);
const WAIT: Duration = Duration::from_secs(60);
const OVERLAP: Duration = Duration::from_secs(3);
const KEY_SENTINEL: &str = "rotation-private-key-material-must-never-appear";

struct Ca {
    certificate: Certificate,
    key: Secret<KeyPair>,
}

impl Ca {
    fn new() -> Self {
        let key = KeyPair::generate().unwrap();
        let mut params = CertificateParams::new(Vec::<String>::new()).unwrap();
        params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        params.key_usages = vec![KeyUsagePurpose::KeyCertSign, KeyUsagePurpose::CrlSign];
        params.distinguished_name = DistinguishedName::new();
        params
            .distinguished_name
            .push(DnType::CommonName, "ephemeral rotation test CA");
        Self {
            certificate: params.self_signed(&key).unwrap(),
            key: Secret::new(key),
        }
    }

    fn root(&self) -> CertificateDer<'static> {
        self.certificate.der().clone()
    }

    fn issue(&self, node: NodeId, serial: u64) -> TlsIdentity {
        self.issue_params(params(node, serial))
    }

    fn issue_params(&self, params: CertificateParams) -> TlsIdentity {
        let key = KeyPair::generate().unwrap();
        let certificate = params
            .signed_by(&key, &self.certificate, self.key.expose_secret())
            .unwrap();
        TlsIdentity {
            certificates: vec![certificate.der().clone()],
            private_key: Secret::new(PrivatePkcs8KeyDer::from(key.serialize_der()).into()),
        }
    }

    fn config(&self, node: NodeId, serial: u64) -> TlsTransportConfig {
        let mut config = TlsTransportConfig::new(node, self.issue(node, serial), vec![self.root()]);
        config.allowed_nodes = BTreeSet::from([A, B]);
        // Retain an authenticated connection while the overlap deadline passes.
        config.limits.read_timeout = WAIT;
        config
    }

    fn client(&self, identity: TlsIdentity) -> ClientConfig {
        let mut roots = RootCertStore::empty();
        roots.add(self.root()).unwrap();
        let mut config =
            ClientConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
                .with_protocol_versions(&[&rustls::version::TLS13])
                .unwrap()
                .with_root_certificates(roots)
                .with_client_auth_cert(identity.certificates, identity.private_key.into_inner())
                .unwrap();
        config.alpn_protocols = vec![ALPN.to_vec()];
        config.resumption = rustls::client::Resumption::disabled();
        config
    }

    fn server(&self, identity: TlsIdentity) -> ServerConfig {
        let provider = Arc::new(rustls::crypto::ring::default_provider());
        let mut roots = RootCertStore::empty();
        roots.add(self.root()).unwrap();
        let verifier = rustls::server::WebPkiClientVerifier::builder_with_provider(
            Arc::new(roots),
            provider.clone(),
        )
        .build()
        .unwrap();
        // Bypass local preflight only in the raw peer fixture, so an expired
        // server certificate can be offered to the real transport verifier.
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

fn params(node: NodeId, serial: u64) -> CertificateParams {
    let mut params = CertificateParams::new(vec![node_name(node)]).unwrap();
    params.serial_number = Some(serial.into());
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

fn serial(value: u8) -> CertificateSerial {
    CertificateSerial::from_bytes(&[value]).unwrap()
}

fn heartbeat(sequence: u64) -> ControlMessage {
    ControlMessage::Heartbeat(Heartbeat {
        sequence,
        sent_at: Duration::ZERO,
        ownership: Vec::new(),
    })
}

async fn received(transport: &mut TlsTransport) -> Envelope {
    timeout(WAIT, async {
        loop {
            if let Some(message) = transport.receive().unwrap() {
                return message;
            }
            sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("expected message was not admitted")
}

async fn diagnostic(transport: &mut TlsTransport, expected: TransportError) {
    timeout(WAIT, async {
        loop {
            if let Some(error) = transport.take_error() {
                assert_eq!(error, expected);
                return;
            }
            sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("expected diagnostic was not reported");
}

fn server_greeting() -> Vec<u8> {
    let mut greeting = b"BTLS".to_vec();
    greeting.extend_from_slice(&1_u16.to_be_bytes());
    greeting.extend_from_slice(&B.0.to_be_bytes());
    greeting.extend_from_slice(&1_u64.to_be_bytes());
    greeting
}

fn assert_no_key_material(output: &str) {
    assert!(!output.contains(KEY_SENTINEL));
    assert!(!output.contains(&format!("{:?}", KEY_SENTINEL.as_bytes())));
}

async fn pair(ca: &Ca) -> (TlsTransport, TlsTransport) {
    let mut a = TlsTransport::bind(ca.config(A, 11)).await.unwrap();
    let mut b = TlsTransport::bind(ca.config(B, 21)).await.unwrap();
    a.set_peer(B, b.local_addr()).unwrap();
    b.set_peer(A, a.local_addr()).unwrap();
    (a, b)
}

async fn send(a: &mut TlsTransport, b: &mut TlsTransport, sequence: u64) {
    timeout(WAIT, a.send_confirmed(A, B, heartbeat(sequence)))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(received(b).await.message, heartbeat(sequence));
}

async fn authenticated(
    config: ClientConfig,
    address: SocketAddr,
) -> Result<TlsStream<TcpStream>, ()> {
    timeout(WAIT, async {
        let socket = TcpStream::connect(address).await.map_err(|_| ())?;
        let mut stream = TlsConnector::from(Arc::new(config))
            .connect(ServerName::try_from(node_name(B)).unwrap(), socket)
            .await
            .map_err(|_| ())?;
        let mut greeting = [0; 22];
        stream.read_exact(&mut greeting).await.map_err(|_| ())?;
        assert_eq!(&greeting[..4], b"BTLS");
        assert_eq!(u64::from_be_bytes(greeting[6..14].try_into().unwrap()), B.0);
        assert_eq!(u64::from_be_bytes(greeting[14..22].try_into().unwrap()), 1);
        Ok(stream)
    })
    .await
    .expect("authentication exceeded its test deadline")
}

async fn frame(stream: &mut TlsStream<TcpStream>) -> Result<(), ()> {
    let mut body = b"BCTL".to_vec();
    body.extend_from_slice(&1_u16.to_be_bytes());
    for value in [1_u64, A.0, B.0, 1, 1] {
        body.extend_from_slice(&value.to_be_bytes());
    }
    body.extend_from_slice(&[0; 12]);
    body.push(0);
    body.extend_from_slice(&1_u64.to_be_bytes());
    body.extend_from_slice(&[0; 12]);
    body.extend_from_slice(&0_u32.to_be_bytes());
    timeout(WAIT, async {
        stream
            .write_all(&(body.len() as u32).to_be_bytes())
            .await
            .map_err(|_| ())?;
        stream.write_all(&body).await.map_err(|_| ())?;
        stream.flush().await.map_err(|_| ())?;
        let mut ack = [0];
        stream.read_exact(&mut ack).await.map_err(|_| ())?;
        if ack == [1] { Ok(()) } else { Err(()) }
    })
    .await
    .expect("frame exchange exceeded its test deadline")
}

async fn exchange(ca: &Ca, identity: TlsIdentity, address: SocketAddr) -> Result<(), ()> {
    let mut stream = authenticated(ca.client(identity), address).await?;
    frame(&mut stream).await
}

#[test]
fn certificate_serial_matches_unsigned_serial_with_der_sign_padding() {
    let ca = Ca::new();
    let identity = ca.issue(A, 0x80);
    assert_eq!(
        certificate_serial(&identity.certificates[0]).unwrap(),
        serial(0x80)
    );
    assert!(certificate_serial(&CertificateDer::from(vec![0x30, 0])).is_err());
}

#[test]
fn serial_normalization_and_size_boundaries_are_consistent() {
    assert_eq!(
        CertificateSerial::from_bytes(&[0, 0, 0x80]).unwrap(),
        serial(0x80)
    );
    assert_eq!(
        CertificateSerial::from_bytes(&[0, 0]).unwrap().as_bytes(),
        &[0]
    );
    assert_eq!(
        CertificateSerial::from_bytes(&[0xff; 20])
            .unwrap()
            .as_bytes(),
        &[0xff; 20]
    );
    let mut sign_padded = vec![0];
    sign_padded.extend_from_slice(&[0xff; 20]);
    assert_eq!(
        CertificateSerial::from_bytes(&sign_padded).unwrap(),
        CertificateSerial::from_bytes(&[0xff; 20]).unwrap()
    );
    for invalid in [Vec::new(), vec![1; 21], vec![0; 22]] {
        assert_eq!(
            CertificateSerial::from_bytes(&invalid),
            Err(TransportError::InvalidIdentity)
        );
    }
}

#[tokio::test]
async fn node_revocation_is_live_and_replacement_can_restore_admission() {
    let ca = Ca::new();
    let (mut a, mut b) = pair(&ca).await;
    let address = b.local_addr();
    send(&mut a, &mut b, 1).await;
    b.set_revocations(RevocationList {
        nodes: BTreeSet::from([A]),
        ..Default::default()
    })
    .unwrap();
    assert!(a.send_confirmed(A, B, heartbeat(2)).await.is_err());
    assert!(b.receive().unwrap().is_none());
    b.set_revocations(RevocationList::default()).unwrap();
    send(&mut a, &mut b, 3).await;
    assert_eq!(b.local_addr(), address);
    a.shutdown().await;
    b.shutdown().await;
}

#[tokio::test]
async fn initial_serial_revocation_rejects_only_that_leaf_serial() {
    let ca = Ca::new();
    let mut config = ca.config(B, 21);
    config.revocations.serials.insert(serial(11));
    let mut b = TlsTransport::bind(config).await.unwrap();
    assert!(
        exchange(&ca, ca.issue(A, 11), b.local_addr())
            .await
            .is_err()
    );
    assert!(b.receive().unwrap().is_none());
    exchange(&ca, ca.issue(A, 12), b.local_addr())
        .await
        .unwrap();
    assert_eq!(received(&mut b).await.from, A);
    b.shutdown().await;
}

#[tokio::test]
async fn outgoing_node_and_serial_revocation_reject_server_before_delivery() {
    let ca = Ca::new();
    let (mut a, mut b) = pair(&ca).await;
    a.set_revocations(RevocationList {
        nodes: BTreeSet::from([B]),
        ..Default::default()
    })
    .unwrap();
    assert!(a.send_confirmed(A, B, heartbeat(1)).await.is_err());
    assert!(b.receive().unwrap().is_none());
    a.set_revocations(RevocationList {
        serials: BTreeSet::from([serial(21)]),
        ..Default::default()
    })
    .unwrap();
    assert!(a.send_confirmed(A, B, heartbeat(2)).await.is_err());
    assert!(b.receive().unwrap().is_none());
    b.rotate_identity(ca.issue(B, 22)).unwrap();
    send(&mut a, &mut b, 3).await;
    a.shutdown().await;
    b.shutdown().await;
}

#[tokio::test]
async fn peer_overlap_accepts_old_and_new_then_retires_old_without_rebinding() {
    let ca = Ca::new();
    let mut b = TlsTransport::bind(ca.config(B, 21)).await.unwrap();
    let address = b.local_addr();
    b.rotate_peer_certificate(A, serial(11), serial(12), OVERLAP)
        .unwrap();
    for value in [11, 12] {
        exchange(&ca, ca.issue(A, value), address).await.unwrap();
        assert_eq!(received(&mut b).await.from, A);
    }
    assert!(exchange(&ca, ca.issue(A, 13), address).await.is_err());
    assert!(b.receive().unwrap().is_none());
    sleep(OVERLAP + Duration::from_millis(50)).await;
    assert!(exchange(&ca, ca.issue(A, 11), address).await.is_err());
    assert!(b.receive().unwrap().is_none());
    exchange(&ca, ca.issue(A, 12), address).await.unwrap();
    assert_eq!(received(&mut b).await.from, A);
    assert_eq!(b.local_addr(), address);
    // The next rotation must build on the previous accepted generation.
    assert!(
        b.rotate_peer_certificate(A, serial(11), serial(13), OVERLAP)
            .is_err()
    );
    b.rotate_peer_certificate(A, serial(12), serial(13), OVERLAP)
        .unwrap();
    b.shutdown().await;
}

#[tokio::test]
async fn outgoing_overlap_retires_old_server_certificate() {
    let ca = Ca::new();
    let (mut a, mut b) = pair(&ca).await;
    a.rotate_peer_certificate(B, serial(21), serial(22), OVERLAP)
        .unwrap();
    send(&mut a, &mut b, 1).await;
    b.rotate_identity(ca.issue(B, 22)).unwrap();
    send(&mut a, &mut b, 2).await;
    b.rotate_identity(ca.issue(B, 21)).unwrap();
    send(&mut a, &mut b, 3).await;
    sleep(OVERLAP + Duration::from_millis(50)).await;
    assert!(a.send_confirmed(A, B, heartbeat(4)).await.is_err());
    assert!(b.receive().unwrap().is_none());
    b.rotate_identity(ca.issue(B, 22)).unwrap();
    send(&mut a, &mut b, 5).await;
    a.shutdown().await;
    b.shutdown().await;
}

#[tokio::test]
async fn overlapping_rotations_are_rejected_without_replacing_current_policy() {
    let ca = Ca::new();
    let mut b = TlsTransport::bind(ca.config(B, 21)).await.unwrap();
    assert!(
        b.rotate_peer_certificate(A, serial(11), serial(12), Duration::ZERO)
            .is_err()
    );
    assert!(
        b.rotate_peer_certificate(A, serial(11), serial(11), OVERLAP)
            .is_err()
    );
    b.rotate_peer_certificate(A, serial(11), serial(12), OVERLAP)
        .unwrap();
    assert_eq!(
        b.rotate_peer_certificate(A, serial(12), serial(13), OVERLAP),
        Err(TransportError::RotationInProgress)
    );
    exchange(&ca, ca.issue(A, 12), b.local_addr())
        .await
        .unwrap();
    assert_eq!(received(&mut b).await.from, A);
    assert!(
        exchange(&ca, ca.issue(A, 13), b.local_addr())
            .await
            .is_err()
    );
    assert!(b.receive().unwrap().is_none());
    b.shutdown().await;
}

#[tokio::test]
async fn revocation_after_handshake_prevents_pending_frame_admission() {
    let ca = Ca::new();
    let mut b = TlsTransport::bind(ca.config(B, 21)).await.unwrap();
    let mut stream = authenticated(ca.client(ca.issue(A, 11)), b.local_addr())
        .await
        .unwrap();
    b.set_revocations(RevocationList {
        serials: BTreeSet::from([serial(11)]),
        ..Default::default()
    })
    .unwrap();
    assert!(frame(&mut stream).await.is_err());
    assert!(b.receive().unwrap().is_none());
    exchange(&ca, ca.issue(A, 12), b.local_addr())
        .await
        .unwrap();
    assert_eq!(received(&mut b).await.from, A);
    b.shutdown().await;
}

#[tokio::test]
async fn retirement_applies_to_connections_authenticated_before_rotation() {
    let ca = Ca::new();
    let mut b = TlsTransport::bind(ca.config(B, 21)).await.unwrap();
    let mut stream = authenticated(ca.client(ca.issue(A, 11)), b.local_addr())
        .await
        .unwrap();
    b.rotate_peer_certificate(A, serial(11), serial(12), OVERLAP)
        .unwrap();
    sleep(OVERLAP + Duration::from_millis(50)).await;
    assert!(frame(&mut stream).await.is_err());
    assert!(b.receive().unwrap().is_none());
    exchange(&ca, ca.issue(A, 12), b.local_addr())
        .await
        .unwrap();
    assert_eq!(received(&mut b).await.from, A);
    b.shutdown().await;
}

#[tokio::test]
async fn identity_rotation_preserves_existing_exchange_and_changes_next_handshake() {
    let ca = Ca::new();
    let mut b = TlsTransport::bind(ca.config(B, 21)).await.unwrap();
    let address = b.local_addr();
    let mut stream = authenticated(ca.client(ca.issue(A, 11)), address)
        .await
        .unwrap();
    assert_eq!(
        certificate_serial(&stream.get_ref().1.peer_certificates().unwrap()[0]).unwrap(),
        serial(21)
    );
    b.rotate_identity(ca.issue(B, 22)).unwrap();
    frame(&mut stream).await.unwrap();
    assert_eq!(received(&mut b).await.from, A);
    let mut next = authenticated(ca.client(ca.issue(A, 11)), address)
        .await
        .unwrap();
    assert_eq!(
        certificate_serial(&next.get_ref().1.peer_certificates().unwrap()[0]).unwrap(),
        serial(22)
    );
    frame(&mut next).await.unwrap();
    assert_eq!(received(&mut b).await.from, A);
    assert_eq!(b.local_addr(), address);
    b.shutdown().await;
}

#[tokio::test]
async fn both_peers_rotate_their_client_and_server_identity_without_restart() {
    let ca = Ca::new();
    let (mut a, mut b) = pair(&ca).await;
    let addresses = (a.local_addr(), b.local_addr());
    a.rotate_peer_certificate(B, serial(21), serial(22), OVERLAP)
        .unwrap();
    b.rotate_peer_certificate(A, serial(11), serial(12), OVERLAP)
        .unwrap();
    send(&mut a, &mut b, 1).await;
    a.rotate_identity(ca.issue(A, 12)).unwrap();
    send(&mut a, &mut b, 2).await;
    b.rotate_identity(ca.issue(B, 22)).unwrap();
    send(&mut a, &mut b, 3).await;
    b.send_confirmed(B, A, heartbeat(4)).await.unwrap();
    let message = received(&mut a).await;
    assert_eq!(message.message, heartbeat(4));
    assert_eq!(message.from_incarnation, 1);
    assert_eq!((a.local_addr(), b.local_addr()), addresses);
    a.shutdown().await;
    b.shutdown().await;
}

#[tokio::test]
async fn invalid_identity_updates_are_atomic_and_do_not_expose_private_keys() {
    let ca = Ca::new();
    let other_ca = Ca::new();
    let (mut a, mut b) = pair(&ca).await;
    let mut expired = params(B, 22);
    expired.not_before = date_time_ymd(2000, 1, 1);
    expired.not_after = date_time_ymd(2001, 1, 1);
    let mut malformed = ca.issue(B, 22);
    malformed.private_key = Secret::new(PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(
        KEY_SENTINEL.as_bytes().to_vec(),
    )));
    for debug in [format!("{malformed:?}"), format!("{malformed:#?}")] {
        assert_no_key_material(&debug);
        assert!(debug.contains("[REDACTED]"));
    }
    // Both objects are valid in isolation, but this key does not belong to the
    // certificate. Configuration must fail before replacing either direction.
    let mut mismatched_key = ca.issue(B, 22);
    mismatched_key.private_key = ca.issue(B, 23).private_key;
    for (index, identity) in [
        ca.issue(A, 22),
        other_ca.issue(B, 22),
        ca.issue_params(expired),
        malformed,
        mismatched_key,
    ]
    .into_iter()
    .enumerate()
    {
        let error = b.rotate_identity(identity).unwrap_err();
        assert_no_key_material(&format!("{error:?} {error:#?} {error}"));
        assert!(std::error::Error::source(&error).is_none());
        send(&mut a, &mut b, index as u64 + 1).await;
        // The server still presents the original identity after every failure.
        let stream = authenticated(ca.client(ca.issue(A, 11)), b.local_addr())
            .await
            .unwrap();
        assert_eq!(
            certificate_serial(&stream.get_ref().1.peer_certificates().unwrap()[0]).unwrap(),
            serial(21)
        );
        drop(stream);
    }
    assert_no_key_material(&format!("{a:?} {b:#?}"));
    while let Some(error) = b.take_error() {
        assert_no_key_material(&format!("{error:?} {error}"));
    }
    a.shutdown().await;
    b.shutdown().await;
}

#[test]
fn private_key_material_is_absent_from_actual_test_process_output() {
    // Filter to the async test only; this process-capture test is never selected
    // in the child and therefore cannot recursively spawn itself.
    let output = Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "invalid_identity_updates_are_atomic_and_do_not_expose_private_keys",
            "--nocapture",
            "--test-threads=1",
        ])
        .output()
        .expect("could not execute the isolated diagnostic test");
    assert_no_key_material(&String::from_utf8_lossy(&output.stdout));
    assert_no_key_material(&String::from_utf8_lossy(&output.stderr));
    assert!(output.status.success(), "isolated diagnostic test failed");
    assert!(String::from_utf8_lossy(&output.stdout).contains("1 passed"));
}

#[tokio::test]
async fn expired_certificates_are_rejected_in_both_connection_directions() {
    let ca = Ca::new();
    let mut expired_client = params(A, 11);
    expired_client.not_before = date_time_ymd(2000, 1, 1);
    expired_client.not_after = date_time_ymd(2001, 1, 1);
    let mut b = TlsTransport::bind(ca.config(B, 21)).await.unwrap();
    assert!(
        exchange(&ca, ca.issue_params(expired_client), b.local_addr())
            .await
            .is_err()
    );
    assert!(b.receive().unwrap().is_none());
    b.shutdown().await;

    let mut expired_server = params(B, 21);
    expired_server.not_before = date_time_ymd(2000, 1, 1);
    expired_server.not_after = date_time_ymd(2001, 1, 1);
    let config = ca.server(ca.issue_params(expired_server));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let actor = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        TlsAcceptor::from(Arc::new(config))
            .accept(socket)
            .await
            .is_err()
    });
    let mut a = TlsTransport::bind(ca.config(A, 11)).await.unwrap();
    a.set_peer(B, address).unwrap();
    assert_eq!(
        a.send_confirmed(A, B, heartbeat(1)).await,
        Err(TransportError::Tls)
    );
    assert!(timeout(WAIT, actor).await.unwrap().unwrap());
    assert_eq!(a.stats().delivered, 0);
    a.shutdown().await;
}

#[tokio::test]
async fn explicit_revocation_takes_precedence_over_rotation_overlap() {
    let ca = Ca::new();
    let mut b = TlsTransport::bind(ca.config(B, 21)).await.unwrap();
    b.rotate_peer_certificate(A, serial(11), serial(12), OVERLAP)
        .unwrap();
    b.set_revocations(RevocationList {
        nodes: BTreeSet::from([A]),
        ..Default::default()
    })
    .unwrap();
    for value in [11, 12] {
        assert!(
            exchange(&ca, ca.issue(A, value), b.local_addr())
                .await
                .is_err()
        );
        assert!(b.receive().unwrap().is_none());
    }
    b.set_revocations(RevocationList {
        serials: BTreeSet::from([serial(12)]),
        ..Default::default()
    })
    .unwrap();
    assert!(
        exchange(&ca, ca.issue(A, 12), b.local_addr())
            .await
            .is_err()
    );
    assert!(b.receive().unwrap().is_none());
    b.shutdown().await;
}

#[tokio::test]
async fn outgoing_revocation_while_greeting_is_stalled_prevents_body_write() {
    let ca = Ca::new();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let config = ca.server(ca.issue(B, 21));
    let (authenticated_tx, authenticated_rx) = oneshot::channel();
    let (release_tx, release_rx) = oneshot::channel();
    let actor = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        let mut stream = TlsAcceptor::from(Arc::new(config))
            .accept(socket)
            .await
            .unwrap();
        authenticated_tx.send(()).unwrap();
        release_rx.await.unwrap();
        if stream.write_all(&server_greeting()).await.is_err() || stream.flush().await.is_err() {
            return true;
        }
        let mut byte = [0];
        matches!(stream.read(&mut byte).await, Ok(0) | Err(_))
    });
    let mut a = TlsTransport::bind(ca.config(A, 11)).await.unwrap();
    a.set_peer(B, address).unwrap();
    a.send(A, B, heartbeat(1)).unwrap();
    timeout(WAIT, authenticated_rx).await.unwrap().unwrap();
    a.set_revocations(RevocationList {
        nodes: BTreeSet::from([B]),
        ..Default::default()
    })
    .unwrap();
    release_tx.send(()).unwrap();
    diagnostic(&mut a, TransportError::CertificateRevoked).await;
    assert!(
        timeout(WAIT, actor).await.unwrap().unwrap(),
        "revoked connection transmitted a frame"
    );
    assert_eq!(a.stats().delivered, 0);
    a.shutdown().await;
}

#[tokio::test]
async fn outgoing_revocation_after_remote_receipt_does_not_report_delivery_success() {
    let ca = Ca::new();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let config = ca.server(ca.issue(B, 21));
    let (received_tx, received_rx) = oneshot::channel();
    let (release_tx, release_rx) = oneshot::channel();
    let actor = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        let mut stream = TlsAcceptor::from(Arc::new(config))
            .accept(socket)
            .await
            .unwrap();
        stream.write_all(&server_greeting()).await.unwrap();
        stream.flush().await.unwrap();
        let length = stream.read_u32().await.unwrap() as usize;
        assert!(length <= 4096);
        let mut body = vec![0; length];
        stream.read_exact(&mut body).await.unwrap();
        assert_eq!(&body[..4], b"BCTL");
        received_tx.send(()).unwrap();
        release_rx.await.unwrap();
        // The sender may already close when it observes the policy update.
        let _ = stream.write_all(&[1]).await;
        let _ = stream.flush().await;
    });
    let mut a = TlsTransport::bind(ca.config(A, 11)).await.unwrap();
    a.set_peer(B, address).unwrap();
    a.send(A, B, heartbeat(1)).unwrap();
    timeout(WAIT, received_rx).await.unwrap().unwrap();
    a.set_revocations(RevocationList {
        nodes: BTreeSet::from([B]),
        ..Default::default()
    })
    .unwrap();
    release_tx.send(()).unwrap();
    diagnostic(&mut a, TransportError::CertificateRevoked).await;
    timeout(WAIT, actor).await.unwrap().unwrap();
    // Receipt was proven above, but its late ACK cannot turn the revoked
    // exchange into success or establish that nothing was delivered.
    assert_eq!(a.stats().delivered, 0);
    assert_eq!(a.stats().failed, 1);
    a.shutdown().await;
}

#[tokio::test]
async fn oversized_revocation_updates_leave_the_previous_deny_list_in_force() {
    let ca = Ca::new();
    let (mut a, mut b) = pair(&ca).await;
    b.set_revocations(RevocationList {
        nodes: BTreeSet::from([A]),
        ..Default::default()
    })
    .unwrap();
    let too_many_nodes = RevocationList {
        nodes: (0..4097).map(NodeId).collect(),
        ..Default::default()
    };
    let too_many_serials = RevocationList {
        serials: (0_u64..65537)
            .map(|value| CertificateSerial::from_bytes(&value.to_be_bytes()).unwrap())
            .collect(),
        ..Default::default()
    };
    for invalid in [too_many_nodes, too_many_serials] {
        assert_eq!(
            b.set_revocations(invalid),
            Err(TransportError::InvalidConfig)
        );
        assert!(a.send_confirmed(A, B, heartbeat(1)).await.is_err());
        assert!(b.receive().unwrap().is_none());
    }
    b.set_revocations(RevocationList::default()).unwrap();
    send(&mut a, &mut b, 2).await;
    a.shutdown().await;
    b.shutdown().await;
}

#[tokio::test]
async fn self_revocation_blocks_bind_and_live_traffic() {
    let ca = Ca::new();
    for revoke_by_node in [false, true] {
        let mut config = ca.config(B, 21);
        if revoke_by_node {
            config.revocations.nodes.insert(B);
        } else {
            config.revocations.serials.insert(serial(21));
        }
        match TlsTransport::bind(config).await {
            Err(error) => assert_eq!(error, TransportError::CertificateRevoked),
            Ok(mut transport) => {
                transport.shutdown().await;
                panic!("self-revoked identity opened a listener");
            }
        }
    }
    let (mut a, mut b) = pair(&ca).await;
    b.set_revocations(RevocationList {
        nodes: BTreeSet::from([B]),
        ..Default::default()
    })
    .unwrap();
    assert!(a.send_confirmed(A, B, heartbeat(1)).await.is_err());
    assert_eq!(
        b.send_confirmed(B, A, heartbeat(1)).await,
        Err(TransportError::CertificateRevoked)
    );
    assert!(a.receive().unwrap().is_none());
    assert!(b.receive().unwrap().is_none());
    b.set_revocations(RevocationList::default()).unwrap();
    send(&mut a, &mut b, 2).await;
    a.shutdown().await;
    b.shutdown().await;
}

#[tokio::test]
async fn lifecycle_updates_after_shutdown_are_rejected() {
    let ca = Ca::new();
    let mut b = TlsTransport::bind(ca.config(B, 21)).await.unwrap();
    b.shutdown().await;
    assert_eq!(
        b.set_revocations(RevocationList::default()),
        Err(TransportError::Closed)
    );
    assert_eq!(
        b.rotate_peer_certificate(A, serial(11), serial(12), OVERLAP),
        Err(TransportError::Closed)
    );
    assert_eq!(
        b.rotate_identity(ca.issue(B, 22)),
        Err(TransportError::Closed)
    );
}

#[tokio::test]
async fn already_admitted_messages_are_not_withdrawn_by_revocation() {
    let ca = Ca::new();
    let (mut a, mut b) = pair(&ca).await;
    a.send_confirmed(A, B, heartbeat(1)).await.unwrap();
    b.set_revocations(RevocationList {
        nodes: BTreeSet::from([A]),
        ..Default::default()
    })
    .unwrap();
    assert_eq!(received(&mut b).await.message, heartbeat(1));
    assert!(a.send_confirmed(A, B, heartbeat(2)).await.is_err());
    assert!(b.receive().unwrap().is_none());
    a.shutdown().await;
    b.shutdown().await;
}

#[tokio::test]
async fn certificate_expiring_after_handshake_cannot_admit_a_late_frame() {
    let ca = Ca::new();
    let mut b = TlsTransport::bind(ca.config(B, 21)).await.unwrap();
    let expires_at = SystemTime::now() + Duration::from_secs(3);
    let mut short_lived = params(A, 11);
    short_lived.not_before = date_time_ymd(2000, 1, 1);
    short_lived.not_after = expires_at.into();
    let mut stream = authenticated(ca.client(ca.issue_params(short_lived)), b.local_addr())
        .await
        .unwrap();
    // rcgen encodes whole-second certificate timestamps. Wait beyond the
    // original wall-clock deadline while staying within the frame I/O timeout.
    sleep(
        expires_at
            .duration_since(SystemTime::now())
            .unwrap_or(Duration::ZERO)
            + Duration::from_secs(1),
    )
    .await;
    assert!(frame(&mut stream).await.is_err());
    diagnostic(&mut b, TransportError::Tls).await;
    assert!(b.receive().unwrap().is_none());
    b.shutdown().await;
}
