//! CA lifecycle checks use ephemeral keys and real loopback TCP only.
use rcgen::{
    BasicConstraints, Certificate, CertificateParams, DistinguishedName, DnType,
    ExtendedKeyUsagePurpose, IsCa, KeyPair, KeyUsagePurpose,
};
use rustls::{
    ClientConfig, RootCertStore,
    pki_types::{CertificateDer, PrivatePkcs8KeyDer, ServerName},
};
use src::{
    cert_policy::{PolicyStore, certificate_fingerprint},
    cluster::{ControlMessage, Heartbeat, NodeId, Transport},
    redact::Secret,
    tls_transport::{
        ALPN, CertificateSerial, TlsIdentity, TlsTransport, TlsTransportConfig, TransportError,
        node_name,
    },
};
use std::{collections::BTreeSet, net::SocketAddr, path::PathBuf, sync::Arc, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
    time::{sleep, timeout},
};
use tokio_rustls::{TlsConnector, client::TlsStream};

const A: NodeId = NodeId(1);
const B: NodeId = NodeId(2);
const C: NodeId = NodeId(3);
const WAIT: Duration = Duration::from_secs(60);
const OVERLAP: Duration = Duration::from_millis(700);

struct Ca {
    certificate: Certificate,
    key: Secret<KeyPair>,
    intermediates: Vec<CertificateDer<'static>>,
}

impl Ca {
    fn new(name: &str) -> Self {
        let key = KeyPair::generate().unwrap();
        let certificate = ca_params(name).self_signed(&key).unwrap();
        Self {
            certificate,
            key: Secret::new(key),
            intermediates: Vec::new(),
        }
    }

    fn intermediate(&self, name: &str) -> Self {
        let key = KeyPair::generate().unwrap();
        let certificate = ca_params(name)
            .signed_by(&key, &self.certificate, self.key.expose_secret())
            .unwrap();
        let mut intermediates = vec![certificate.der().clone()];
        intermediates.extend(self.intermediates.clone());
        Self {
            certificate,
            key: Secret::new(key),
            intermediates,
        }
    }

    fn root(&self) -> CertificateDer<'static> {
        self.certificate.der().clone()
    }

    fn issue(&self, node: NodeId, serial: u64) -> TlsIdentity {
        let key = KeyPair::generate().unwrap();
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
        let certificate = params
            .signed_by(&key, &self.certificate, self.key.expose_secret())
            .unwrap();
        let mut certificates = vec![certificate.der().clone()];
        certificates.extend(self.intermediates.clone());
        TlsIdentity {
            certificates,
            private_key: Secret::new(PrivatePkcs8KeyDer::from(key.serialize_der()).into()),
        }
    }

    fn config(
        &self,
        node: NodeId,
        serial: u64,
        roots: Vec<CertificateDer<'static>>,
    ) -> TlsTransportConfig {
        let mut config = TlsTransportConfig::new(node, self.issue(node, serial), roots);
        config.allowed_nodes = BTreeSet::from([A, B]);
        config.limits.read_timeout = WAIT;
        config
    }
}

fn ca_params(name: &str) -> CertificateParams {
    let mut params = CertificateParams::new(Vec::<String>::new()).unwrap();
    params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    params.key_usages = vec![KeyUsagePurpose::KeyCertSign];
    params.distinguished_name = DistinguishedName::new();
    params.distinguished_name.push(DnType::CommonName, name);
    params
}

fn heartbeat() -> ControlMessage {
    ControlMessage::Heartbeat(Heartbeat {
        sequence: 1,
        sent_at: Duration::ZERO,
        ownership: Vec::new(),
    })
}

async fn send(a: &mut TlsTransport, b: &mut TlsTransport, from: NodeId, to: NodeId) {
    timeout(WAIT, a.send_confirmed(from, to, heartbeat()))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(b.receive().unwrap().unwrap().from, from);
}

async fn authenticated(
    identity: TlsIdentity,
    roots: Vec<CertificateDer<'static>>,
    address: SocketAddr,
) -> Result<TlsStream<TcpStream>, ()> {
    let mut trust = RootCertStore::empty();
    for root in roots {
        trust.add(root).unwrap();
    }
    let mut config =
        ClientConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
            .with_protocol_versions(&[&rustls::version::TLS13])
            .unwrap()
            .with_root_certificates(trust)
            .with_client_auth_cert(identity.certificates, identity.private_key.into_inner())
            .unwrap();
    config.alpn_protocols = vec![ALPN.to_vec()];
    config.resumption = rustls::client::Resumption::disabled();
    timeout(WAIT, async {
        let socket = TcpStream::connect(address).await.map_err(|_| ())?;
        let mut stream = TlsConnector::from(Arc::new(config))
            .connect(ServerName::try_from(node_name(B)).unwrap(), socket)
            .await
            .map_err(|_| ())?;
        let mut greeting = [0; 22];
        stream.read_exact(&mut greeting).await.map_err(|_| ())?;
        assert_eq!(&greeting[..4], b"BTLS");
        Ok(stream)
    })
    .await
    .unwrap()
}

async fn frame(stream: &mut TlsStream<TcpStream>) -> Result<(), ()> {
    frame_from(stream, A).await
}

async fn frame_from(stream: &mut TlsStream<TcpStream>, from: NodeId) -> Result<(), ()> {
    let mut body = b"BCTL".to_vec();
    body.extend_from_slice(&1_u16.to_be_bytes());
    for value in [1_u64, from.0, B.0, 1, 1] {
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
    .unwrap()
}

async fn exchange(
    ca: &Ca,
    serial: u64,
    roots: Vec<CertificateDer<'static>>,
    b: &mut TlsTransport,
) -> Result<(), ()> {
    exchange_from(ca, A, serial, roots, b).await
}

async fn exchange_from(
    ca: &Ca,
    from: NodeId,
    serial: u64,
    roots: Vec<CertificateDer<'static>>,
    b: &mut TlsTransport,
) -> Result<(), ()> {
    let mut stream = authenticated(ca.issue(from, serial), roots, b.local_addr()).await?;
    frame_from(&mut stream, from).await?;
    assert_eq!(b.receive().unwrap().unwrap().from, from);
    Ok(())
}

#[tokio::test]
async fn ca_overlap_accepts_both_then_retires_old_in_both_roles() {
    let old = Ca::new("old root");
    let new = Ca::new("new root");
    let mut a = TlsTransport::bind(old.config(A, 11, vec![old.root()]))
        .await
        .unwrap();
    let mut b = TlsTransport::bind(old.config(B, 21, vec![old.root()]))
        .await
        .unwrap();
    a.set_peer(B, b.local_addr()).unwrap();
    b.set_peer(A, a.local_addr()).unwrap();
    a.rotate_certificate_authorities(vec![new.root()], OVERLAP)
        .unwrap();
    b.rotate_certificate_authorities(vec![new.root()], OVERLAP)
        .unwrap();
    let roots = vec![old.root(), new.root()];
    exchange(&old, 13, roots.clone(), &mut b).await.unwrap();
    exchange(&new, 14, roots.clone(), &mut b).await.unwrap();
    send(&mut a, &mut b, A, B).await;
    b.rotate_identity(new.issue(B, 22)).unwrap();
    sleep(OVERLAP + Duration::from_millis(150)).await;
    assert!(exchange(&old, 13, roots.clone(), &mut b).await.is_err());
    assert!(b.receive().unwrap().is_none());
    exchange(&new, 14, roots, &mut b).await.unwrap();
    assert!(a.send_confirmed(A, B, heartbeat()).await.is_err());
    a.rotate_identity(new.issue(A, 12)).unwrap();
    send(&mut a, &mut b, A, B).await;
    send(&mut b, &mut a, B, A).await;
    a.shutdown().await;
    b.shutdown().await;
}

#[tokio::test]
async fn invalid_and_concurrent_ca_updates_leave_the_current_policy() {
    let old = Ca::new("initial root");
    let new = Ca::new("replacement root");
    let third = Ca::new("third root");
    let mut b = TlsTransport::bind(old.config(B, 21, vec![old.root()]))
        .await
        .unwrap();
    for (cas, overlap) in [
        (Vec::new(), OVERLAP),
        (vec![CertificateDer::from(vec![0x30, 0])], OVERLAP),
        (vec![new.root(), new.root()], OVERLAP),
        (vec![new.root()], Duration::ZERO),
    ] {
        assert!(b.rotate_certificate_authorities(cas, overlap).is_err());
    }
    let unchanged = b.certificate_policy_snapshot(1).unwrap();
    assert_eq!(unchanged.trusted_cas, vec![old.root().to_vec()]);
    assert!(unchanged.ca_rotation.is_none());
    exchange(&old, 11, vec![old.root()], &mut b).await.unwrap();
    b.rotate_certificate_authorities(vec![new.root()], WAIT)
        .unwrap();
    assert_eq!(
        b.rotate_certificate_authorities(vec![third.root()], WAIT),
        Err(TransportError::RotationInProgress)
    );
    assert_eq!(
        b.certificate_policy_snapshot(2).unwrap().trusted_cas,
        vec![new.root().to_vec()]
    );
    exchange(&old, 11, vec![old.root()], &mut b).await.unwrap();
    b.shutdown().await;
}

#[tokio::test]
async fn omitted_root_revocation_blocks_incoming_and_outgoing_peer_chains() {
    let peer_ca = Ca::new("peer root");
    let local_ca = Ca::new("local root");
    let roots = vec![peer_ca.root(), local_ca.root()];
    assert_eq!(peer_ca.issue(A, 11).certificates.len(), 1);
    let mut a = TlsTransport::bind(peer_ca.config(A, 11, roots.clone()))
        .await
        .unwrap();
    let mut b = TlsTransport::bind(local_ca.config(B, 21, roots.clone()))
        .await
        .unwrap();
    a.set_peer(B, b.local_addr()).unwrap();
    b.set_peer(A, a.local_addr()).unwrap();
    send(&mut a, &mut b, A, B).await;
    send(&mut b, &mut a, B, A).await;
    b.set_revoked_certificate_authorities(BTreeSet::from([certificate_fingerprint(
        &peer_ca.root(),
    )]))
    .unwrap();
    assert!(a.send_confirmed(A, B, heartbeat()).await.is_err());
    assert!(b.send_confirmed(B, A, heartbeat()).await.is_err());
    assert!(a.receive().unwrap().is_none());
    assert!(b.receive().unwrap().is_none());
    exchange(&local_ca, 12, roots, &mut b).await.unwrap();
    b.set_revoked_certificate_authorities(BTreeSet::new())
        .unwrap();
    send(&mut a, &mut b, A, B).await;
    a.shutdown().await;
    b.shutdown().await;
}

#[tokio::test]
async fn intermediate_revocation_blocks_both_directions_without_revoking_root() {
    let root = Ca::new("shared root");
    let intermediate = root.intermediate("revocable intermediate");
    assert_eq!(intermediate.issue(A, 11).certificates.len(), 2);
    let mut a = TlsTransport::bind(intermediate.config(A, 11, vec![root.root()]))
        .await
        .unwrap();
    let mut b = TlsTransport::bind(root.config(B, 21, vec![root.root()]))
        .await
        .unwrap();
    a.set_peer(B, b.local_addr()).unwrap();
    b.set_peer(A, a.local_addr()).unwrap();
    send(&mut a, &mut b, A, B).await;
    send(&mut b, &mut a, B, A).await;
    b.set_revoked_certificate_authorities(BTreeSet::from([certificate_fingerprint(
        &intermediate.root(),
    )]))
    .unwrap();
    assert!(a.send_confirmed(A, B, heartbeat()).await.is_err());
    assert!(b.send_confirmed(B, A, heartbeat()).await.is_err());
    assert!(a.receive().unwrap().is_none());
    assert!(b.receive().unwrap().is_none());
    exchange(&root, 12, vec![root.root()], &mut b)
        .await
        .unwrap();
    b.set_revoked_certificate_authorities(BTreeSet::new())
        .unwrap();
    send(&mut a, &mut b, A, B).await;
    a.shutdown().await;
    b.shutdown().await;
}

#[tokio::test]
async fn ca_revocation_after_handshake_rejects_pending_frame_admission() {
    let peer_ca = Ca::new("pending peer root");
    let local_ca = Ca::new("pending local root");
    let roots = vec![peer_ca.root(), local_ca.root()];
    let mut b = TlsTransport::bind(local_ca.config(B, 21, roots.clone()))
        .await
        .unwrap();
    let mut stream = authenticated(peer_ca.issue(A, 11), roots.clone(), b.local_addr())
        .await
        .unwrap();
    b.set_revoked_certificate_authorities(BTreeSet::from([certificate_fingerprint(
        &peer_ca.root(),
    )]))
    .unwrap();
    assert!(frame(&mut stream).await.is_err());
    assert!(b.receive().unwrap().is_none());
    exchange(&local_ca, 12, roots, &mut b).await.unwrap();
    b.shutdown().await;
}

#[tokio::test]
async fn ca_retirement_rechecks_a_connection_authenticated_before_rotation() {
    let old = Ca::new("pending old root");
    let new = Ca::new("pending new root");
    let mut b = TlsTransport::bind(old.config(B, 21, vec![old.root()]))
        .await
        .unwrap();
    let mut stream = authenticated(old.issue(A, 11), vec![old.root()], b.local_addr())
        .await
        .unwrap();
    b.rotate_certificate_authorities(vec![new.root()], OVERLAP)
        .unwrap();
    b.rotate_identity(new.issue(B, 22)).unwrap();
    sleep(OVERLAP + Duration::from_millis(150)).await;
    assert!(frame(&mut stream).await.is_err());
    assert!(b.receive().unwrap().is_none());
    exchange(&new, 12, vec![new.root()], &mut b).await.unwrap();
    b.shutdown().await;
}

#[tokio::test]
async fn restored_ca_and_leaf_deadlines_are_not_extended_by_restart() {
    let old = Ca::new("restore old root");
    let new = Ca::new("restore new root");
    let mut b = TlsTransport::bind(old.config(B, 21, vec![old.root()]))
        .await
        .unwrap();
    b.rotate_peer_certificate(
        A,
        CertificateSerial::from_bytes(&[11]).unwrap(),
        CertificateSerial::from_bytes(&[12]).unwrap(),
        OVERLAP,
    )
    .unwrap();
    b.rotate_certificate_authorities(vec![new.root()], OVERLAP)
        .unwrap();
    let snapshot = b.certificate_policy_snapshot(1).unwrap();
    b.shutdown().await;
    sleep(OVERLAP + Duration::from_millis(150)).await;
    let mut restored =
        TlsTransport::bind_with_policy(new.config(B, 22, vec![new.root()]), snapshot.clone())
            .await
            .unwrap();
    let captured = restored.certificate_policy_snapshot(2).unwrap();
    assert!(
        captured.ca_rotation.unwrap().retire_at_unix_ms
            <= snapshot.ca_rotation.unwrap().retire_at_unix_ms
    );
    assert!(
        captured.peer_rotations[0].retire_at_unix_ms
            <= snapshot.peer_rotations[0].retire_at_unix_ms
    );
    // Distinguish the two restored policies: correct leaf serial under the old
    // CA and retired leaf serial under the new CA must independently fail.
    assert!(
        exchange(&old, 12, vec![new.root()], &mut restored)
            .await
            .is_err()
    );
    assert!(
        exchange(&new, 11, vec![new.root()], &mut restored)
            .await
            .is_err()
    );
    assert!(restored.receive().unwrap().is_none());
    exchange(&new, 12, vec![new.root()], &mut restored)
        .await
        .unwrap();
    restored.shutdown().await;
}

#[tokio::test]
async fn peer_proof_exhaustion_preserves_other_peer_and_local_rotation_capacity() {
    let ca = Ca::new("proof quota root");
    let mut config = ca.config(B, 21, vec![ca.root()]);
    config.allowed_nodes.insert(C);
    let mut b = TlsTransport::bind(config).await.unwrap();
    // Distinct, CA-valid leaf certificates exercise the node's count budget.
    // A chain is remembered after authentication and reused at frame admission.
    let existing = ca.issue(A, 1_000);
    let same_identity = TlsIdentity {
        certificates: existing.certificates.clone(),
        private_key: Secret::new(existing.private_key.expose_secret().clone_key()),
    };
    let mut first = authenticated(existing, vec![ca.root()], b.local_addr())
        .await
        .unwrap();
    frame(&mut first).await.unwrap();
    assert_eq!(b.receive().unwrap().unwrap().from, A);
    drop(first);
    for serial in 1_001..1_064 {
        exchange(&ca, serial, vec![ca.root()], &mut b)
            .await
            .unwrap();
    }
    assert!(exchange(&ca, 1_064, vec![ca.root()], &mut b).await.is_err());
    assert!(b.receive().unwrap().is_none());
    exchange_from(&ca, C, 2_000, vec![ca.root()], &mut b)
        .await
        .unwrap();
    // The exhausted peer's allowance is separate from this node's credentials.
    b.rotate_identity(ca.issue(B, 22)).unwrap();
    exchange_from(&ca, C, 2_000, vec![ca.root()], &mut b)
        .await
        .unwrap();
    // Existing proofs remain usable after the exhausted-node rejection.
    let mut existing = authenticated(same_identity, vec![ca.root()], b.local_addr())
        .await
        .unwrap();
    frame(&mut existing).await.unwrap();
    assert_eq!(b.receive().unwrap().unwrap().from, A);
    b.shutdown().await;
}

struct PolicyDirectory(PathBuf);

impl PolicyDirectory {
    fn new() -> Self {
        let mut suffix = [0; 16];
        getrandom::fill(&mut suffix).unwrap();
        let name: String = suffix.iter().map(|byte| format!("{byte:02x}")).collect();
        let path = std::env::temp_dir().join(format!("cachain-ca-policy-{name}"));
        std::fs::create_dir(&path).unwrap();
        Self(path)
    }

    fn policy(&self) -> PathBuf {
        self.0.join("policy.bin")
    }
}

impl Drop for PolicyDirectory {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(self.policy());
        let _ = std::fs::remove_file(self.0.join("policy.bin.lock"));
        let _ = std::fs::remove_dir(&self.0);
    }
}

#[tokio::test]
async fn persisted_policy_is_installed_and_restored_before_accepting_peers() {
    let ca = Ca::new("persisted policy root");
    let directory = PolicyDirectory::new();
    let mut config = ca.config(B, 21, vec![ca.root()]);
    config.allowed_nodes.insert(C);
    let mut b = TlsTransport::bind(config).await.unwrap();
    exchange(&ca, 11, vec![ca.root()], &mut b).await.unwrap();

    let mut snapshot = b.certificate_policy_snapshot(1).unwrap();
    snapshot.revoked_nodes.insert(A.0);
    // A synthetic authentication key remains in memory; no credential is saved
    // in the public snapshot, test directory, or diagnostic output.
    let mut store = PolicyStore::open(directory.policy(), Secret::new([0x52; 32])).unwrap();
    store.save(&snapshot).unwrap();
    let persisted = store.load(1).unwrap();
    b.install_certificate_policy(persisted.clone()).unwrap();
    assert!(exchange(&ca, 11, vec![ca.root()], &mut b).await.is_err());
    assert!(b.receive().unwrap().is_none());
    exchange_from(&ca, C, 31, vec![ca.root()], &mut b)
        .await
        .unwrap();
    assert!(b.install_certificate_policy(persisted.clone()).is_err());

    let mut invalid = persisted.clone();
    invalid.revision = 2;
    invalid.trusted_cas = vec![vec![0x30, 0]];
    assert!(b.install_certificate_policy(invalid).is_err());
    assert_eq!(
        b.certificate_policy_snapshot(2).unwrap().revoked_nodes,
        persisted.revoked_nodes
    );
    assert_eq!(store.load(1).unwrap(), persisted);
    assert!(exchange(&ca, 11, vec![ca.root()], &mut b).await.is_err());
    exchange_from(&ca, C, 31, vec![ca.root()], &mut b)
        .await
        .unwrap();
    b.shutdown().await;
    drop(store);

    let mut reopened = PolicyStore::open(directory.policy(), Secret::new([0x52; 32])).unwrap();
    let restored_snapshot = reopened.load(1).unwrap();
    let mut config = ca.config(B, 22, vec![ca.root()]);
    config.allowed_nodes.insert(C);
    let mut restored = TlsTransport::bind_with_policy(config, restored_snapshot)
        .await
        .unwrap();
    assert!(
        exchange(&ca, 11, vec![ca.root()], &mut restored)
            .await
            .is_err()
    );
    assert!(restored.receive().unwrap().is_none());
    exchange_from(&ca, C, 31, vec![ca.root()], &mut restored)
        .await
        .unwrap();

    let mut self_revoked = restored.certificate_policy_snapshot(2).unwrap();
    self_revoked.revoked_nodes.insert(B.0);
    assert_eq!(
        TlsTransport::bind_with_policy(ca.config(B, 23, vec![ca.root()]), self_revoked)
            .await
            .unwrap_err(),
        TransportError::CertificateRevoked
    );
    restored.shutdown().await;
    drop(reopened);
}
