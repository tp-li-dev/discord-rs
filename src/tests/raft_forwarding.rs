//! Forwarding uses real OpenRaft tasks/files; this fixture's RPC delivery is in
//! memory. It does not establish TLS, runtime fencing or cross-host safety.
use src::raft_quorum::{
    Command, NodeOptions, Outcome, RaftNode, ShardKey, StorageMode,
    transport::{
        ForwardResponse, MemoryTransport, RaftRpcTransport, RpcRequest, RpcResponse, TransportError,
    },
};
use std::{
    collections::{BTreeMap, BTreeSet},
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};
use tokio::time::{Instant, sleep, timeout};

const WAIT: Duration = Duration::from_secs(15);
static NEXT_ROOT: AtomicU64 = AtomicU64::new(0);

struct Root(PathBuf);

impl Root {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "raftq-forwarding-{}-{}",
            std::process::id(),
            NEXT_ROOT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir(&path).expect("create fresh forwarding fixture");
        Self(path)
    }
}

impl Drop for Root {
    fn drop(&mut self) {
        // Remove only the immediate, uniquely created fixture child, after
        // checking the resolved path. On failure leave evidence in place.
        if let (Ok(path), Ok(parent)) = (self.0.canonicalize(), std::env::temp_dir().canonicalize())
            && path.parent() == Some(parent.as_path())
            && path
                .file_name()
                .and_then(|n| n.to_str())
                .is_some_and(|n| n.starts_with("raftq-forwarding-"))
        {
            let _ = std::fs::remove_dir_all(path);
        }
    }
}

struct Cluster {
    root: Arc<Root>,
    nodes: BTreeMap<u64, RaftNode>,
    transport: MemoryTransport,
}

impl Cluster {
    async fn new() -> Self {
        let mut cluster = Self {
            root: Arc::new(Root::new()),
            nodes: BTreeMap::new(),
            transport: MemoryTransport::default(),
        };
        for id in 1..=3 {
            let node = RaftNode::start(
                cluster.root.0.join(format!("node-{id}")),
                id,
                BTreeSet::from([1, 2, 3]),
                StorageMode::Create,
                NodeOptions {
                    allow_test_mode: true,
                    raft_config: openraft::Config {
                        cluster_name: "forwarding-fixture".into(),
                        heartbeat_interval: 50,
                        election_timeout_min: 300,
                        election_timeout_max: 600,
                        snapshot_policy: openraft::SnapshotPolicy::Never,
                        ..openraft::Config::default()
                    },
                    ..NodeOptions::default()
                },
                Arc::new(cluster.transport.clone()),
            )
            .await
            .unwrap();
            cluster.transport.register(id, node.raft.clone()).unwrap();
            cluster.nodes.insert(id, node);
        }
        cluster.nodes[&1].initialize().await.unwrap();
        cluster
    }

    async fn leader(&self) -> u64 {
        let deadline = Instant::now() + WAIT;
        loop {
            let leader = self.nodes.iter().find_map(|(id, node)| {
                let metrics = node.raft.metrics().borrow().clone();
                (metrics.state == openraft::ServerState::Leader).then_some(*id)
            });
            if let Some(id) = leader {
                for node in self.nodes.values() {
                    node.raft
                        .wait(Some(WAIT))
                        .current_leader(id, "all peers learn leader")
                        .await
                        .unwrap();
                }
                return id;
            }
            assert!(Instant::now() < deadline, "leader election timed out");
            sleep(Duration::from_millis(10)).await;
        }
    }

    async fn stop(mut self) {
        for id in self.nodes.keys() {
            self.transport.remove(*id);
        }
        for node in std::mem::take(&mut self.nodes).into_values() {
            node.shutdown().await.unwrap();
        }
    }
}

impl Drop for Cluster {
    fn drop(&mut self) {
        // Break transport/raft cycles even on assertion failure, and keep the
        // fixture directory alive until background shutdown has released files.
        let nodes = std::mem::take(&mut self.nodes);
        for id in nodes.keys() {
            self.transport.remove(*id);
        }
        if !nodes.is_empty()
            && let Ok(runtime) = tokio::runtime::Handle::try_current()
        {
            let root = self.root.clone();
            runtime.spawn(async move {
                for node in nodes.into_values() {
                    let _ = node.shutdown().await;
                }
                drop(root);
            });
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn follower_redirect_does_not_write_and_leader_forwarding_commits_epoch_cas() {
    let cluster = Cluster::new().await;
    let leader = cluster.leader().await;
    let followers: Vec<_> = cluster
        .nodes
        .keys()
        .copied()
        .filter(|id| *id != leader)
        .collect();
    let source = followers[0];
    let other_follower = followers[1];
    let key = ShardKey {
        bot_id: 1001,
        shard_id: 2,
    };
    let command = Command::Acquire {
        key,
        expected_epoch: 0,
        owner: source,
        session: [7; 16],
    };
    let forward = || RpcRequest::Forward {
        source,
        command: command.clone(),
    };

    let reply = timeout(
        WAIT,
        cluster.transport.request(source, other_follower, forward()),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(
        matches!(reply, RpcResponse::Forward(ForwardResponse::NotLeader { leader: Some(id) }) if id == leader)
    );
    for node in cluster.nodes.values() {
        assert!(!node.machine.data().await.unwrap().0.contains_key(&key));
    }

    let reply = timeout(WAIT, cluster.transport.request(source, leader, forward()))
        .await
        .unwrap()
        .unwrap();
    let RpcResponse::Forward(ForwardResponse::Applied(Outcome::Applied(record))) = reply else {
        panic!("forwarded write did not commit")
    };
    assert_eq!(record.epoch, 1);
    assert_eq!(record.owner, source);
    assert!(record.held);
    assert_eq!(
        cluster.nodes[&leader].machine.data().await.unwrap().0[&key],
        record
    );

    let reply = timeout(WAIT, cluster.transport.request(source, leader, forward()))
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(
        reply,
        RpcResponse::Forward(ForwardResponse::Applied(Outcome::Stale {
            current_epoch: 1
        }))
    ));
    let spoof = RpcRequest::Forward {
        source,
        command: Command::Release {
            key,
            epoch: 1,
            owner: other_follower,
            session: [7; 16],
        },
    };
    assert_eq!(
        cluster.transport.request(source, leader, spoof).await.err(),
        Some(TransportError::IdentityMismatch)
    );
    assert_eq!(
        cluster.nodes[&leader].machine.data().await.unwrap().0[&key],
        record
    );

    // Exercise the public node API too: the local follower must discover the
    // leader and forward while preserving its authenticated owner identity.
    let other_key = ShardKey {
        bot_id: key.bot_id,
        shard_id: key.shard_id + 1,
    };
    let outcome = cluster.nodes[&source]
        .submit(Command::Acquire {
            key: other_key,
            expected_epoch: 0,
            owner: source,
            session: [8; 16],
        })
        .await
        .unwrap();
    let Outcome::Applied(forwarded_record) = outcome else {
        panic!("follower node submit did not commit")
    };
    assert_eq!(forwarded_record.epoch, 1);
    assert_eq!(forwarded_record.owner, source);
    assert_eq!(
        cluster.nodes[&leader].machine.data().await.unwrap().0[&other_key],
        forwarded_record
    );

    // A Raft task failure is not a redirect and must not invite automatic retry.
    cluster.nodes[&leader].shutdown().await.unwrap();
    let reply = timeout(WAIT, cluster.transport.request(source, leader, forward()))
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(
        reply,
        RpcResponse::Forward(ForwardResponse::OutcomeUnknown)
    ));
    cluster.stop().await;
}
