//! Immutable lease ceilings are checked at both the authenticated RPC and durable
//! store boundaries. These fixtures use ephemeral certificates and loopback TCP.
//! The takeover clock is injected; this does not validate OS suspend or Linux.
use openraft::{Config, ServerState, SnapshotPolicy, Vote, raft::VoteRequest};
use rcgen::{
    BasicConstraints, Certificate, CertificateParams, DistinguishedName, DnType,
    ExtendedKeyUsagePurpose, IsCa, KeyPair, KeyUsagePurpose,
};
use rustls::pki_types::PrivatePkcs8KeyDer;
use src::{
    cluster::{ManualClock, NodeId},
    ownership::{Clock, OwnershipCoordinator, OwnershipError, OwnershipKey},
    protocol::{BotId, OwnershipEpoch},
    raft_quorum::{
        NodeOptions, QuorumCoordinator, QuorumError, RaftNode, StorageMode, TAKEOVER_QUARANTINE,
        transport::{
            AuthenticatedRpcAdapter, MemoryTransport, RaftRpcTransport, RpcEndpoint, RpcLimits,
            RpcRequest, RpcScope, TransportError,
        },
    },
    redact::Secret,
    tls_transport::{
        TlsIdentity, TlsTransport, TlsTransportConfig, node_name, raft_rpc::TlsRpcTransport,
    },
};
use std::{path::PathBuf, sync::Arc, time::Duration};
use tokio::time::{Instant, sleep};

const WAIT: Duration = Duration::from_secs(60);
const CEILING: Duration = Duration::from_secs(2);

struct Root(PathBuf);

impl Root {
    fn new() -> Self {
        let mut nonce = [0; 16];
        getrandom::fill(&mut nonce).unwrap();
        let suffix: String = nonce.iter().map(|byte| format!("{byte:02x}")).collect();
        let path = std::env::temp_dir().join(format!("raftq-lease-policy-{suffix}"));
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
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.starts_with("raftq-lease-policy-"))
        {
            let _ = std::fs::remove_dir_all(path);
        }
    }
}

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
        Self {
            certificate: params.self_signed(&key).unwrap(),
            key: Secret::new(key),
        }
    }

    fn config(&self, id: u64) -> TlsTransportConfig {
        let key = KeyPair::generate().unwrap();
        let mut params = CertificateParams::new(vec![node_name(NodeId(id))]).unwrap();
        params.distinguished_name = DistinguishedName::new();
        params
            .distinguished_name
            .push(DnType::CommonName, node_name(NodeId(id)));
        params.key_usages = vec![KeyUsagePurpose::DigitalSignature];
        params.extended_key_usages = vec![
            ExtendedKeyUsagePurpose::ClientAuth,
            ExtendedKeyUsagePurpose::ServerAuth,
        ];
        let certificate = params
            .signed_by(&key, &self.certificate, self.key.expose_secret())
            .unwrap();
        let mut config = TlsTransportConfig::new(
            NodeId(id),
            TlsIdentity {
                certificates: vec![certificate.der().clone()],
                private_key: Secret::new(PrivatePkcs8KeyDer::from(key.serialize_der()).into()),
            },
            vec![self.certificate.der().clone()],
        );
        config.allowed_nodes = [NodeId(1), NodeId(2), NodeId(3)].into();
        config
    }
}

fn scope() -> RpcScope {
    RpcScope {
        cluster_id: [81; 16],
        members: [1, 2, 3].into(),
    }
}

fn options(local_maximum: Duration) -> NodeOptions {
    NodeOptions {
        allow_test_mode: true,
        max_lease_ttl: local_maximum,
        // Shared CI runners run many test binaries at once; keep elections and
        // writes far above scheduler jitter so the policy under test is what fails.
        write_timeout: Duration::from_secs(10),
        raft_config: Config {
            cluster_name: "raft-lease-policy-test".into(),
            heartbeat_interval: 100,
            election_timeout_min: 1500,
            election_timeout_max: 3000,
            snapshot_policy: SnapshotPolicy::Never,
            ..Config::default()
        },
        ..NodeOptions::default()
    }
}

struct Node {
    raft: Arc<RaftNode>,
    rpc: TlsRpcTransport,
    tls: TlsTransport,
}

impl Node {
    async fn start(root: &Root, ca: &Ca, id: u64, local_maximum: Duration) -> Self {
        let tls = TlsTransport::bind(ca.config(id)).await.unwrap();
        let mut rpc = tls
            .bind_raft_rpc("127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();
        let adapter = Arc::new(
            AuthenticatedRpcAdapter::new_with_lease_ceiling(
                rpc.channel(),
                scope(),
                RpcLimits::default(),
                CEILING,
            )
            .unwrap(),
        );
        let endpoint_scope = adapter.scope().clone();
        let raft = Arc::new(
            RaftNode::start(
                root.0.join(format!("node-{id}")),
                id,
                scope().members,
                StorageMode::Create,
                options(local_maximum),
                adapter,
            )
            .await
            .unwrap(),
        );
        rpc.set_endpoint(Arc::new(
            RpcEndpoint::new(id, endpoint_scope, raft.raft.clone(), RpcLimits::default()).unwrap(),
        ))
        .unwrap();
        Self { raft, rpc, tls }
    }

    async fn stop(mut self) {
        self.rpc.shutdown().await;
        self.raft.shutdown().await.unwrap();
        self.tls.shutdown().await;
    }
}

#[test]
fn lease_policy_namespace_is_deterministic_and_preserves_exact_duration() {
    let original = scope();
    let scoped = original.with_lease_ceiling(CEILING).unwrap();
    assert_eq!(scoped, original.with_lease_ceiling(CEILING).unwrap());
    assert_eq!(scoped.members, original.members);
    assert_ne!(scoped.cluster_id, original.cluster_id);
    assert_ne!(
        scoped,
        original
            .with_lease_ceiling(CEILING - Duration::from_nanos(1))
            .unwrap()
    );
    let mut different_cluster = original.clone();
    different_cluster.cluster_id[0] ^= 1;
    assert_ne!(
        scoped,
        different_cluster.with_lease_ceiling(CEILING).unwrap()
    );
    let mut different_membership = original.clone();
    different_membership.members.insert(4);
    assert_ne!(
        scoped,
        different_membership.with_lease_ceiling(CEILING).unwrap()
    );
    for invalid in [
        Duration::ZERO,
        TAKEOVER_QUARANTINE + Duration::from_nanos(1),
    ] {
        assert_eq!(
            original.with_lease_ceiling(invalid),
            Err(TransportError::InvalidConfig)
        );
    }
}

#[tokio::test]
async fn immutable_policy_rejects_longer_leases_and_changed_store_identity() {
    let root = Root::new();
    let ca = Ca::new();
    let mut tls = TlsTransport::bind(ca.config(1)).await.unwrap();
    let mut rpc = tls
        .bind_raft_rpc("127.0.0.1:0".parse().unwrap())
        .await
        .unwrap();
    let adapter = |base, ceiling| {
        Arc::new(
            AuthenticatedRpcAdapter::new_with_lease_ceiling(
                rpc.channel(),
                base,
                RpcLimits::default(),
                ceiling,
            )
            .unwrap(),
        )
    };
    let invalid_path = root.0.join("invalid");
    assert_eq!(
        RaftNode::start(
            &invalid_path,
            1,
            scope().members,
            StorageMode::Create,
            options(CEILING + Duration::from_nanos(1)),
            adapter(scope(), CEILING),
        )
        .await
        .err(),
        Some(QuorumError::InvalidConfig)
    );
    assert!(!invalid_path.exists());

    let path = root.0.join("node");
    let node = RaftNode::start(
        &path,
        1,
        scope().members,
        StorageMode::Create,
        options(Duration::from_millis(500)),
        adapter(scope(), CEILING),
    )
    .await
    .unwrap();
    assert_eq!(node.lease_ceiling(), CEILING);
    node.shutdown().await.unwrap();
    drop(node);

    let mut other_scope = scope();
    other_scope.cluster_id[0] ^= 1;
    for (base, ceiling) in [
        (scope(), Duration::from_secs(1)),
        (scope(), Duration::from_secs(3)),
        (other_scope, CEILING),
    ] {
        assert_eq!(
            RaftNode::start(
                &path,
                1,
                scope().members,
                StorageMode::Open,
                options(Duration::from_millis(500)),
                adapter(base, ceiling),
            )
            .await
            .err(),
            Some(QuorumError::Storage)
        );
    }
    let legacy = Arc::new(
        AuthenticatedRpcAdapter::new(rpc.channel(), scope(), RpcLimits::default()).unwrap(),
    );
    assert_eq!(legacy.cluster_lease_ceiling(), None);
    assert_eq!(
        RaftNode::start(
            &path,
            1,
            scope().members,
            StorageMode::Open,
            options(Duration::from_millis(500)),
            legacy,
        )
        .await
        .err(),
        Some(QuorumError::Storage)
    );
    // Rejected reopens did not alter the original store. A different local max
    // below the fixed ceiling is permitted, while the quarantine stays fixed.
    let reopened = RaftNode::start(
        &path,
        1,
        scope().members,
        StorageMode::Open,
        options(Duration::from_millis(750)),
        adapter(scope(), CEILING),
    )
    .await
    .unwrap();
    assert_eq!(reopened.lease_ceiling(), CEILING);
    reopened.shutdown().await.unwrap();
    drop(reopened);

    let legacy = RaftNode::start(
        root.0.join("legacy"),
        1,
        scope().members,
        StorageMode::Create,
        options(Duration::from_millis(500)),
        Arc::new(MemoryTransport::default()),
    )
    .await
    .unwrap();
    assert_eq!(legacy.lease_ceiling(), TAKEOVER_QUARANTINE);
    legacy.shutdown().await.unwrap();
    drop(legacy);
    assert_eq!(
        RaftNode::start(
            root.0.join("legacy"),
            1,
            scope().members,
            StorageMode::Open,
            options(Duration::from_millis(500)),
            adapter(scope(), CEILING),
        )
        .await
        .err(),
        Some(QuorumError::Storage)
    );
    rpc.shutdown().await;
    tls.shutdown().await;
}

#[tokio::test]
async fn legacy_and_different_ceiling_rpcs_cannot_change_a_scoped_nodes_term() {
    let root = Root::new();
    let ca = Ca::new();
    let receiver = Node::start(&root, &ca, 2, CEILING).await;
    receiver.raft.raft.runtime_config().elect(false);
    let mut tls = TlsTransport::bind(ca.config(1)).await.unwrap();
    let mut rpc = tls
        .bind_raft_rpc("127.0.0.1:0".parse().unwrap())
        .await
        .unwrap();
    rpc.set_peer(NodeId(2), receiver.rpc.local_addr()).unwrap();
    let before = receiver.raft.raft.metrics().borrow().current_term;
    let legacy =
        AuthenticatedRpcAdapter::new(rpc.channel(), scope(), RpcLimits::default()).unwrap();
    let incompatible = AuthenticatedRpcAdapter::new_with_lease_ceiling(
        rpc.channel(),
        scope(),
        RpcLimits::default(),
        Duration::from_secs(1),
    )
    .unwrap();
    for adapter in [&legacy, &incompatible] {
        assert!(
            adapter
                .request(
                    1,
                    2,
                    RpcRequest::Vote(VoteRequest::new(Vote::new(before + 100, 1), None))
                )
                .await
                .is_err()
        );
        assert_eq!(receiver.raft.raft.metrics().borrow().current_term, before);
    }
    let compatible = AuthenticatedRpcAdapter::new_with_lease_ceiling(
        rpc.channel(),
        scope(),
        RpcLimits::default(),
        CEILING,
    )
    .unwrap();
    compatible
        .request(
            1,
            2,
            RpcRequest::Vote(VoteRequest::new(Vote::new(before + 1, 1), None)),
        )
        .await
        .unwrap();
    // A successful application RPC, not a TCP connection or a queue ACK.
    assert_eq!(
        receiver.raft.raft.metrics().borrow().current_term,
        before + 1
    );
    rpc.shutdown().await;
    tls.shutdown().await;
    receiver.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn takeover_waits_fleet_ceiling_even_when_candidate_has_a_shorter_local_maximum() {
    let root = Root::new();
    let ca = Ca::new();
    let mut nodes = Vec::new();
    for id in 1..=3 {
        let local_maximum = if id == 2 {
            Duration::from_millis(500)
        } else {
            CEILING
        };
        nodes.push(Node::start(&root, &ca, id, local_maximum).await);
    }
    let addresses: Vec<_> = nodes.iter().map(|node| node.rpc.local_addr()).collect();
    for (index, node) in nodes.iter_mut().enumerate() {
        for (other, address) in addresses.iter().enumerate() {
            if other != index {
                node.rpc
                    .set_peer(NodeId(other as u64 + 1), *address)
                    .unwrap();
            }
        }
    }
    nodes[0].raft.initialize().await.unwrap();
    let deadline = Instant::now() + WAIT;
    loop {
        let leader = nodes.iter().find_map(|node| {
            let metrics = node.raft.raft.metrics().borrow().clone();
            (metrics.state == ServerState::Leader).then_some(metrics.id)
        });
        if leader.is_some()
            && nodes
                .iter()
                .all(|node| node.raft.raft.metrics().borrow().current_leader == leader)
        {
            break;
        }
        assert!(Instant::now() < deadline, "Raft leader election timed out");
        sleep(Duration::from_millis(10)).await;
    }
    let clock = ManualClock::default();
    let key = OwnershipKey {
        bot_id: BotId(9),
        shard_id: 0,
    };
    let mut old = QuorumCoordinator::new(nodes[0].raft.clone(), clock.clone()).unwrap();
    let old_lease = old
        .acquire_quorum(key, OwnershipEpoch(0), CEILING)
        .await
        .unwrap();
    let mut next = QuorumCoordinator::new(nodes[1].raft.clone(), clock.clone()).unwrap();
    let epoch = next.begin_takeover(key, old_lease.epoch).await.unwrap();
    assert_eq!(next.takeover_ready_at(key), Some(clock.now() + CEILING));
    clock.advance(CEILING - Duration::from_millis(1)).unwrap();
    assert!(old.check(key, old_lease.epoch).is_ok());
    assert_eq!(
        next.finish_takeover(key, epoch, Duration::from_millis(500))
            .await,
        Err(OwnershipError::QuorumUnavailable)
    );
    clock.advance(Duration::from_millis(1)).unwrap();
    assert_eq!(
        old.check(key, old_lease.epoch),
        Err(OwnershipError::Expired)
    );
    next.finish_takeover(key, epoch, Duration::from_millis(500))
        .await
        .unwrap();
    assert!(next.check(key, epoch).is_ok());
    assert_eq!(epoch.0, old_lease.epoch.0 + 1);
    drop(old);
    drop(next);
    for node in nodes {
        node.stop().await;
    }
}
