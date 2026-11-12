//! Real OpenRaft nodes, separate loopback TCP/mTLS listeners and durable stores.
//! No MemoryTransport or delivery shortcut is used. Nodes share one OS process;
//! shutdown/reopen is not a power-loss, multi-host or OS-suspend experiment.
use openraft::{Config, ServerState, SnapshotPolicy, Vote, storage::RaftStateMachine};
use rcgen::{
    BasicConstraints, Certificate, CertificateParams, DistinguishedName, DnType,
    ExtendedKeyUsagePurpose, IsCa, KeyPair, KeyUsagePurpose,
};
use rustls::pki_types::{CertificateDer, PrivatePkcs8KeyDer};
use src::{
    cluster::{ManualClock, NodeId},
    ownership::{OwnershipCoordinator, OwnershipError, OwnershipKey},
    protocol::{BotId, OwnershipEpoch},
    raft_quorum::{
        Command, NodeOptions, Outcome, Ownership, OwnershipData, QuorumCoordinator, QuorumError,
        RaftNode, ShardKey, StorageMode,
        transport::{
            AuthenticatedRpcAdapter, RaftRpcTransport, RpcEndpoint, RpcLimits, RpcRequest,
            RpcScope, SnapshotRequest,
        },
    },
    redact::Secret,
    tls_transport::{
        CertificateSerial, RevocationList, TlsIdentity, TlsTransport, TlsTransportConfig,
        certificate_serial, node_name, raft_rpc::TlsRpcTransport,
    },
};
use std::{
    collections::{BTreeMap, BTreeSet},
    path::PathBuf,
    sync::Arc,
    time::Duration,
};
use tokio::time::{Instant, sleep, sleep_until, timeout};

const WAIT: Duration = Duration::from_secs(20);

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
            .push(DnType::CommonName, "ephemeral Raft test CA");
        Self {
            certificate: params.self_signed(&key).unwrap(),
            key: Secret::new(key),
        }
    }

    fn root(&self) -> CertificateDer<'static> {
        self.certificate.der().clone()
    }

    fn issue(&self, node: u64, serial: u64) -> TlsIdentity {
        let key = KeyPair::generate().unwrap();
        let mut params = CertificateParams::new(vec![node_name(NodeId(node))]).unwrap();
        params.serial_number = Some(serial.into());
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
            .signed_by(&key, &self.certificate, self.key.expose_secret())
            .unwrap();
        TlsIdentity {
            certificates: vec![cert.der().clone()],
            private_key: Secret::new(PrivatePkcs8KeyDer::from(key.serialize_der()).into()),
        }
    }

    fn config(&self, node: u64, identity: TlsIdentity) -> TlsTransportConfig {
        let mut config = TlsTransportConfig::new(NodeId(node), identity, vec![self.root()]);
        config.allowed_nodes = [NodeId(1), NodeId(2), NodeId(3)].into();
        config.limits.read_timeout = Duration::from_secs(5);
        config
    }
}

struct Root(PathBuf);

impl Root {
    fn new() -> Self {
        let mut nonce = [0; 16];
        getrandom::fill(&mut nonce).unwrap();
        let suffix: String = nonce.iter().map(|byte| format!("{byte:02x}")).collect();
        let path = std::env::temp_dir().join(format!("raftq-mtls-{suffix}"));
        std::fs::create_dir(&path).unwrap();
        Self(path)
    }
}

impl Drop for Root {
    fn drop(&mut self) {
        // Only the resolved, immediate temp child created by this fixture is owned.
        if let (Ok(path), Ok(parent)) = (self.0.canonicalize(), std::env::temp_dir().canonicalize())
            && path.parent() == Some(parent.as_path())
            && path
                .file_name()
                .and_then(|n| n.to_str())
                .is_some_and(|n| n.starts_with("raftq-mtls-"))
        {
            let _ = std::fs::remove_dir_all(path);
        }
    }
}

fn scope() -> RpcScope {
    RpcScope {
        cluster_id: [42; 16],
        members: [1, 2, 3].into(),
    }
}

fn options() -> NodeOptions {
    NodeOptions {
        allow_test_mode: true,
        write_timeout: Duration::from_secs(3),
        raft_config: Config {
            cluster_name: "raftq-mtls-test".into(),
            heartbeat_interval: 100,
            election_timeout_min: 700,
            election_timeout_max: 1200,
            snapshot_policy: SnapshotPolicy::Never,
            max_in_snapshot_log_to_keep: 0,
            purge_batch_size: 1,
            replication_lag_threshold: 4,
            ..Config::default()
        },
        ..NodeOptions::default()
    }
}

struct Node {
    raft: Arc<RaftNode>,
    rpc: TlsRpcTransport,
    tls: TlsTransport,
    adapter: Arc<AuthenticatedRpcAdapter>,
    serial: CertificateSerial,
}

impl Node {
    async fn stop(mut self) {
        self.rpc.shutdown().await;
        self.raft.shutdown().await.unwrap();
        self.tls.shutdown().await;
    }
}

struct Cluster {
    root: Arc<Root>,
    ca: Ca,
    nodes: BTreeMap<u64, Node>,
}

impl Cluster {
    async fn new() -> Self {
        let mut cluster = Self {
            root: Arc::new(Root::new()),
            ca: Ca::new(),
            nodes: BTreeMap::new(),
        };
        for id in 1..=3 {
            cluster.start(id, StorageMode::Create).await;
        }
        cluster.nodes[&1].raft.initialize().await.unwrap();
        cluster.leader(&[1, 2, 3]).await;
        cluster
    }

    async fn start(&mut self, id: u64, mode: StorageMode) {
        let identity = self.ca.issue(id, id);
        let serial = certificate_serial(&identity.certificates[0]).unwrap();
        let tls = TlsTransport::bind(self.ca.config(id, identity))
            .await
            .unwrap();
        let mut rpc = tls
            .bind_raft_rpc("127.0.0.1:0".parse().unwrap())
            .await
            .unwrap();
        let adapter = Arc::new(
            AuthenticatedRpcAdapter::new(rpc.channel(), scope(), RpcLimits::default()).unwrap(),
        );
        assert!(adapter.is_authenticated());
        let raft = Arc::new(
            RaftNode::start(
                self.root.0.join(format!("node-{id}")),
                id,
                scope().members,
                mode,
                options(),
                adapter.clone(),
            )
            .await
            .unwrap(),
        );
        rpc.set_endpoint(Arc::new(
            RpcEndpoint::new(id, scope(), raft.raft.clone(), RpcLimits::default()).unwrap(),
        ))
        .unwrap();
        for (peer_id, peer) in &mut self.nodes {
            peer.rpc.set_peer(NodeId(id), rpc.local_addr()).unwrap();
            rpc.set_peer(NodeId(*peer_id), peer.rpc.local_addr())
                .unwrap();
        }
        assert!(
            self.nodes
                .insert(
                    id,
                    Node {
                        raft,
                        rpc,
                        tls,
                        adapter,
                        serial
                    }
                )
                .is_none()
        );
    }

    async fn leader(&self, ids: &[u64]) -> u64 {
        let deadline = Instant::now() + WAIT;
        loop {
            for id in ids {
                let metrics = self.nodes[id].raft.raft.metrics().borrow().clone();
                if metrics.state == ServerState::Leader
                    && metrics.current_leader == Some(*id)
                    && ids.iter().all(|peer| {
                        self.nodes[peer].raft.raft.metrics().borrow().current_leader == Some(*id)
                    })
                {
                    return *id;
                }
            }
            assert!(
                Instant::now() < deadline,
                "mTLS cluster did not agree on a leader"
            );
            sleep(Duration::from_millis(20)).await;
        }
    }

    async fn data(&self, id: u64) -> OwnershipData {
        self.nodes[&id].raft.machine.data().await.unwrap()
    }

    async fn converged(&self, ids: &[u64], expected: &OwnershipData) {
        let deadline = Instant::now() + WAIT;
        loop {
            let mut matches = true;
            for id in ids {
                matches &= self.data(*id).await == *expected;
            }
            if matches {
                return;
            }
            assert!(
                Instant::now() < deadline,
                "durable state did not converge over mTLS"
            );
            sleep(Duration::from_millis(20)).await;
        }
    }

    async fn stop(&mut self, id: u64) {
        self.nodes.remove(&id).unwrap().stop().await;
    }

    async fn stop_all(&mut self) {
        for node in std::mem::take(&mut self.nodes).into_values() {
            node.stop().await;
        }
    }
}

impl Drop for Cluster {
    fn drop(&mut self) {
        let nodes = std::mem::take(&mut self.nodes);
        if !nodes.is_empty()
            && let Ok(runtime) = tokio::runtime::Handle::try_current()
        {
            let root = self.root.clone();
            runtime.spawn(async move {
                for node in nodes.into_values() {
                    node.stop().await;
                }
                drop(root);
            });
        }
    }
}

fn key(bot_id: u64, shard_id: u32) -> ShardKey {
    ShardKey { bot_id, shard_id }
}
fn acquire(key: ShardKey, expected_epoch: u64, owner: u64) -> Command {
    Command::Acquire {
        key,
        expected_epoch,
        owner,
        session: [owner as u8; 16],
    }
}
fn release(key: ShardKey, epoch: u64, owner: u64) -> Command {
    Command::Release {
        key,
        epoch,
        owner,
        session: [owner as u8; 16],
    }
}
fn applied(outcome: Outcome, epoch: u64, owner: u64, held: bool) {
    assert_eq!(
        outcome,
        Outcome::Applied(Ownership {
            epoch,
            owner,
            session: [owner as u8; 16],
            held
        })
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn election_forwarding_concurrent_epoch_cas_and_bot_shard_isolation() {
    let mut cluster = Cluster::new().await;
    let leader = cluster.leader(&[1, 2, 3]).await;
    let followers: Vec<_> = (1..=3).filter(|id| *id != leader).collect();
    let k = key(700, 0);
    // Competing writes both cross TCP from different authenticated followers.
    let (a, b) = tokio::join!(
        cluster.nodes[&followers[0]]
            .raft
            .submit(acquire(k, 0, followers[0])),
        cluster.nodes[&followers[1]]
            .raft
            .submit(acquire(k, 0, followers[1])),
    );
    let results = [a.unwrap(), b.unwrap()];
    assert_eq!(
        results
            .iter()
            .filter(|r| matches!(r, Outcome::Applied(_)))
            .count(),
        1
    );
    assert_eq!(
        results
            .iter()
            .filter(|r| **r == Outcome::Stale { current_epoch: 1 })
            .count(),
        1
    );
    let record = cluster.data(leader).await.0[&k];
    applied(
        cluster.nodes[&record.owner]
            .raft
            .submit(release(k, 1, record.owner))
            .await
            .unwrap(),
        1,
        record.owner,
        false,
    );
    applied(
        cluster.nodes[&followers[0]]
            .raft
            .submit(acquire(k, 1, followers[0]))
            .await
            .unwrap(),
        2,
        followers[0],
        true,
    );
    for other in [key(701, 0), key(700, 1)] {
        applied(
            cluster.nodes[&followers[1]]
                .raft
                .submit(acquire(other, 0, followers[1]))
                .await
                .unwrap(),
            1,
            followers[1],
            true,
        );
    }
    let data = cluster.data(leader).await;
    assert_eq!(data.0.len(), 3);
    cluster.converged(&[1, 2, 3], &data).await;
    for node in cluster.nodes.values() {
        assert!(node.rpc.stats().received > 0);
        assert!(node.rpc.stats().delivered > 0);
        assert_eq!(
            node.tls.stats().received,
            0,
            "no legacy control queue carries Raft"
        );
    }
    cluster.stop_all().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn leader_stop_reelects_but_majority_loss_rejects_writes_and_lease_renewal() {
    let mut cluster = Cluster::new().await;
    let old = cluster.leader(&[1, 2, 3]).await;
    let k = key(710, 0);
    applied(
        cluster.nodes[&old]
            .raft
            .submit(acquire(k, 0, old))
            .await
            .unwrap(),
        1,
        old,
        true,
    );
    let initial = cluster.data(old).await;
    cluster.converged(&[1, 2, 3], &initial).await;
    cluster.stop(old).await;
    let remaining: Vec<_> = cluster.nodes.keys().copied().collect();
    let leader = cluster.leader(&remaining).await;
    let other = *remaining.iter().find(|id| **id != leader).unwrap();
    let next = key(710, 1);
    applied(
        cluster.nodes[&other]
            .raft
            .submit(acquire(next, 0, other))
            .await
            .unwrap(),
        1,
        other,
        true,
    );
    let clock = ManualClock::default();
    let mut gate =
        QuorumCoordinator::new(cluster.nodes[&leader].raft.clone(), clock.clone()).unwrap();
    let lease_key = OwnershipKey {
        bot_id: BotId(710),
        shard_id: 2,
    };
    let ttl = Duration::from_secs(10);
    let lease = gate
        .acquire_quorum(lease_key, OwnershipEpoch(0), ttl)
        .await
        .unwrap();
    let durable = cluster.data(leader).await;
    cluster.converged(&remaining, &durable).await;
    cluster.stop(other).await;
    let result = timeout(
        WAIT,
        cluster.nodes[&leader]
            .raft
            .submit(acquire(key(710, 3), 0, leader)),
    )
    .await
    .unwrap();
    assert!(matches!(
        result,
        Err(QuorumError::OutcomeUnknown | QuorumError::RaftRejected)
    ));
    assert_eq!(
        gate.renew_quorum(lease_key, lease.epoch, ttl).await,
        Err(OwnershipError::QuorumUnavailable)
    );
    assert_eq!(
        cluster.data(leader).await,
        durable,
        "minority cannot apply an epoch change"
    );
    clock.advance(ttl).unwrap();
    assert_eq!(
        gate.check(lease_key, lease.epoch),
        Err(OwnershipError::Expired)
    );
    drop(gate);
    cluster.stop_all().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn full_restart_preserves_epochs_and_reservations_without_restoring_a_lease() {
    let mut cluster = Cluster::new().await;
    let leader = cluster.leader(&[1, 2, 3]).await;
    let owner = (1..=3).find(|id| *id != leader).unwrap();
    let k = key(720, 0);
    applied(
        cluster.nodes[&owner]
            .raft
            .submit(acquire(k, 0, owner))
            .await
            .unwrap(),
        1,
        owner,
        true,
    );
    applied(
        cluster.nodes[&owner]
            .raft
            .submit(release(k, 1, owner))
            .await
            .unwrap(),
        1,
        owner,
        false,
    );
    applied(
        cluster.nodes[&owner]
            .raft
            .submit(acquire(k, 1, owner))
            .await
            .unwrap(),
        2,
        owner,
        true,
    );
    let durable = cluster.data(leader).await;
    cluster.converged(&[1, 2, 3], &durable).await;
    cluster.stop_all().await;
    for id in 1..=3 {
        cluster.start(id, StorageMode::Open).await;
    }
    let leader = cluster.leader(&[1, 2, 3]).await;
    cluster.converged(&[1, 2, 3], &durable).await;
    assert_eq!(
        cluster.nodes[&owner]
            .raft
            .submit(acquire(k, 1, owner))
            .await
            .unwrap(),
        Outcome::Stale { current_epoch: 2 }
    );
    let mut gate =
        QuorumCoordinator::new(cluster.nodes[&owner].raft.clone(), ManualClock::default()).unwrap();
    assert!(gate.check(k.into(), OwnershipEpoch(2)).is_err());
    drop(gate);
    // The low-level fixture explicitly closes/releases the old reservation.
    applied(
        cluster.nodes[&owner]
            .raft
            .submit(release(k, 2, owner))
            .await
            .unwrap(),
        2,
        owner,
        false,
    );
    applied(
        cluster.nodes[&leader]
            .raft
            .submit(acquire(k, 2, leader))
            .await
            .unwrap(),
        3,
        leader,
        true,
    );
    let after = cluster.data(leader).await;
    cluster.converged(&[1, 2, 3], &after).await;
    cluster.stop_all().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn revoked_member_cannot_send_any_raft_rpc_or_obtain_a_quorum() {
    let mut cluster = Cluster::new().await;
    let leader = cluster.leader(&[1, 2, 3]).await;
    let revoked = (1..=3).find(|id| *id != leader).unwrap();
    let healthy = (1..=3).find(|id| *id != leader && *id != revoked).unwrap();
    cluster.nodes[&revoked]
        .raft
        .raft
        .runtime_config()
        .elect(false);
    for id in [leader, healthy] {
        cluster
            .nodes
            .get_mut(&id)
            .unwrap()
            .tls
            .set_revocations(RevocationList {
                nodes: BTreeSet::from([NodeId(revoked)]),
                ..RevocationList::default()
            })
            .unwrap();
    }
    let before = cluster.nodes[&leader].raft.raft.metrics().borrow().clone();
    let data = cluster.data(leader).await;
    let term = before.current_term + 100;
    let vote = Vote::new_committed(term, revoked);
    let attempts = [
        RpcRequest::Vote(openraft::raft::VoteRequest::new(
            Vote::new(term, revoked),
            None,
        )),
        RpcRequest::Append(openraft::raft::AppendEntriesRequest {
            vote,
            prev_log_id: None,
            entries: vec![],
            leader_commit: None,
        }),
        RpcRequest::Snapshot(SnapshotRequest {
            vote,
            meta: openraft::SnapshotMeta::default(),
            data: OwnershipData::default(),
        }),
        RpcRequest::Forward {
            source: revoked,
            command: acquire(key(730, 0), 0, revoked),
        },
    ];
    for request in attempts {
        assert!(
            timeout(
                WAIT,
                cluster.nodes[&revoked]
                    .adapter
                    .request(revoked, leader, request)
            )
            .await
            .unwrap()
            .is_err()
        );
    }
    assert_eq!(
        cluster.nodes[&leader]
            .raft
            .raft
            .metrics()
            .borrow()
            .current_term,
        before.current_term,
        "revoked high-term votes never reach OpenRaft"
    );
    assert_eq!(cluster.data(leader).await, data);
    assert!(
        cluster.nodes[&revoked]
            .raft
            .submit(acquire(key(730, 1), 0, revoked))
            .await
            .is_err()
    );
    assert!(cluster.nodes[&leader].rpc.stats().rejected >= 4);
    applied(
        cluster.nodes[&healthy]
            .raft
            .submit(acquire(key(730, 2), 0, healthy))
            .await
            .unwrap(),
        1,
        healthy,
        true,
    );
    let committed = cluster.data(leader).await;
    cluster.converged(&[leader, healthy], &committed).await;
    assert!(!committed.0.contains_key(&key(730, 0)));
    assert!(!committed.0.contains_key(&key(730, 1)));
    cluster.stop_all().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn certificate_rotation_updates_existing_rpc_handles_and_retires_old_serial() {
    let mut cluster = Cluster::new().await;
    let leader = cluster.leader(&[1, 2, 3]).await;
    let rotating = (1..=3).find(|id| *id != leader).unwrap();
    let replacement = cluster.ca.issue(rotating, rotating + 100);
    let new = certificate_serial(&replacement.certificates[0]).unwrap();
    let old = cluster.nodes[&rotating].serial.clone();
    let overlap = Duration::from_millis(500);
    for (id, node) in &mut cluster.nodes {
        if *id != rotating {
            node.tls
                .rotate_peer_certificate(NodeId(rotating), old.clone(), new.clone(), overlap)
                .unwrap();
        }
    }
    let expired = Instant::now() + overlap;
    cluster
        .nodes
        .get_mut(&rotating)
        .unwrap()
        .tls
        .rotate_identity(replacement)
        .unwrap();
    applied(
        cluster.nodes[&rotating]
            .raft
            .submit(acquire(key(740, 0), 0, rotating))
            .await
            .unwrap(),
        1,
        rotating,
        true,
    );
    sleep_until(expired + Duration::from_millis(20)).await;
    applied(
        cluster.nodes[&rotating]
            .raft
            .submit(acquire(key(740, 1), 0, rotating))
            .await
            .unwrap(),
        1,
        rotating,
        true,
    );
    // CA-signed material with the retired serial is still a member identity,
    // but must fail the shared live rotation policy. Keys remain only in memory.
    let mut retired_tls = TlsTransport::bind(
        cluster
            .ca
            .config(rotating, cluster.ca.issue(rotating, rotating)),
    )
    .await
    .unwrap();
    let mut retired_rpc = retired_tls
        .bind_raft_rpc("127.0.0.1:0".parse().unwrap())
        .await
        .unwrap();
    retired_rpc
        .set_peer(NodeId(leader), cluster.nodes[&leader].rpc.local_addr())
        .unwrap();
    let adapter =
        AuthenticatedRpcAdapter::new(retired_rpc.channel(), scope(), RpcLimits::default()).unwrap();
    assert!(
        adapter
            .request(
                rotating,
                leader,
                RpcRequest::Forward {
                    source: rotating,
                    command: acquire(key(740, 2), 0, rotating)
                }
            )
            .await
            .is_err()
    );
    assert!(!cluster.data(leader).await.0.contains_key(&key(740, 2)));
    retired_rpc.shutdown().await;
    retired_tls.shutdown().await;
    let durable = cluster.data(leader).await;
    cluster.converged(&[1, 2, 3], &durable).await;
    cluster.stop_all().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn snapshot_rpc_catches_up_a_stopped_peer_after_log_purge() {
    let mut cluster = Cluster::new().await;
    let leader = cluster.leader(&[1, 2, 3]).await;
    let lagging = (1..=3).find(|id| *id != leader).unwrap();
    cluster.stop(lagging).await;
    let majority: Vec<_> = cluster.nodes.keys().copied().collect();
    let k = key(750, 0);
    for epoch in 0..8 {
        applied(
            cluster.nodes[&leader]
                .raft
                .submit(acquire(k, epoch, leader))
                .await
                .unwrap(),
            epoch + 1,
            leader,
            true,
        );
        applied(
            cluster.nodes[&leader]
                .raft
                .submit(release(k, epoch + 1, leader))
                .await
                .unwrap(),
            epoch + 1,
            leader,
            false,
        );
    }
    let durable = cluster.data(leader).await;
    cluster.converged(&majority, &durable).await;
    let last = cluster.nodes[&leader]
        .raft
        .machine
        .clone()
        .applied_state()
        .await
        .unwrap()
        .0
        .unwrap();
    cluster.nodes[&leader]
        .raft
        .raft
        .trigger()
        .snapshot()
        .await
        .unwrap();
    cluster.nodes[&leader]
        .raft
        .raft
        .wait(Some(WAIT))
        .snapshot(last, "snapshot ready")
        .await
        .unwrap();
    cluster.nodes[&leader]
        .raft
        .raft
        .trigger()
        .purge_log(last.index)
        .await
        .unwrap();
    cluster.nodes[&leader]
        .raft
        .raft
        .wait(Some(WAIT))
        .purged(Some(last), "covered logs purged")
        .await
        .unwrap();
    cluster.start(lagging, StorageMode::Open).await;
    cluster.nodes[&lagging]
        .raft
        .raft
        .wait(Some(WAIT))
        .snapshot(last, "mTLS snapshot installed")
        .await
        .unwrap();
    cluster.converged(&[1, 2, 3], &durable).await;
    applied(
        cluster.nodes[&lagging]
            .raft
            .submit(acquire(k, 8, lagging))
            .await
            .unwrap(),
        9,
        lagging,
        true,
    );
    cluster.stop_all().await;
}

#[tokio::test]
async fn authenticated_transport_does_not_enable_production_or_implicit_test_mode() {
    let root = Root::new();
    let ca = Ca::new();
    let mut tls = TlsTransport::bind(ca.config(1, ca.issue(1, 1)))
        .await
        .unwrap();
    let mut rpc = tls
        .bind_raft_rpc("127.0.0.1:0".parse().unwrap())
        .await
        .unwrap();
    let adapter = Arc::new(
        AuthenticatedRpcAdapter::new(rpc.channel(), scope(), RpcLimits::default()).unwrap(),
    );
    assert!(adapter.is_authenticated());
    for (index, config, expected) in [
        (0, NodeOptions::default(), QuorumError::Disabled),
        (
            1,
            NodeOptions {
                enable_production: true,
                ..options()
            },
            QuorumError::ProductionNotReady,
        ),
    ] {
        let path = root.0.join(format!("must-not-exist-{index}"));
        let result = RaftNode::start(
            &path,
            1,
            scope().members,
            StorageMode::Create,
            config,
            adapter.clone(),
        )
        .await;
        assert!(matches!(result, Err(error) if error == expected));
        assert!(!path.exists(), "startup guard must precede storage effects");
    }
    rpc.shutdown().await;
    tls.shutdown().await;
}
