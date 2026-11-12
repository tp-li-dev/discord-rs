//! Test harness, not a production coordinator. Membership always remains {1,2,3}.
use crate::{
    Outcome, OwnershipData, Raft, Transfer,
    network::{Bus, NetworkFactory},
    storage::{LogStore, StateMachine},
};
use openraft::{Config, ServerState, SnapshotPolicy};
use std::{collections::BTreeMap, sync::Arc, time::Duration};
use tokio::time::{Instant, sleep, timeout};

pub const WAIT: Duration = Duration::from_secs(8);
pub const WRITE_DEADLINE: Duration = Duration::from_millis(800);

pub struct Cluster {
    pub nodes: BTreeMap<u64, Raft>,
    pub machines: BTreeMap<u64, StateMachine>,
    pub bus: Bus,
}

#[derive(Debug, PartialEq, Eq)]
pub enum WriteFailure {
    /// No grant. The request may still commit later: timeout is NOT cancellation.
    OutcomeUnknown,
    RaftRejected(String),
    Stopped,
}

impl Cluster {
    pub async fn new() -> Self {
        let config = Arc::new(
            Config {
                cluster_name: "quorum-evaluation-only".into(),
                heartbeat_interval: 40,
                election_timeout_min: 160,
                election_timeout_max: 320,
                snapshot_policy: SnapshotPolicy::Never,
                max_in_snapshot_log_to_keep: 0,
                purge_batch_size: 1,
                replication_lag_threshold: 8,
                ..Config::default()
            }
            .validate()
            .expect("valid test config"),
        );
        let mut cluster = Self {
            nodes: BTreeMap::new(),
            machines: BTreeMap::new(),
            bus: Bus::default(),
        };
        for id in 1..=3 {
            let sm = StateMachine::default();
            let raft = Raft::new(
                id,
                config.clone(),
                NetworkFactory {
                    source: id,
                    bus: cluster.bus.clone(),
                },
                LogStore::default(),
                sm.clone(),
            )
            .await
            .expect("create in-memory raft");
            cluster.bus.register(id, raft.clone());
            cluster.nodes.insert(id, raft);
            cluster.machines.insert(id, sm);
        }
        cluster.nodes[&1]
            .initialize(BTreeMap::from([(1, ()), (2, ()), (3, ())]))
            .await
            .expect("bootstrap once");
        cluster
    }

    /// Metrics locate a candidate only; writes still require OpenRaft commit.
    pub async fn leader(&self, candidates: &[u64]) -> u64 {
        let start = Instant::now();
        loop {
            for id in candidates {
                if let Some(raft) = self.nodes.get(id) {
                    let m = raft.metrics().borrow().clone();
                    if m.state == ServerState::Leader && m.current_leader == Some(*id) {
                        return *id;
                    }
                }
            }
            assert!(
                start.elapsed() < WAIT,
                "leader not elected among {candidates:?}"
            );
            sleep(Duration::from_millis(10)).await;
        }
    }

    pub async fn write(&self, node: u64, request: Transfer) -> Result<Outcome, WriteFailure> {
        let raft = self.nodes.get(&node).ok_or(WriteFailure::Stopped)?;
        match timeout(WRITE_DEADLINE, raft.client_write(request)).await {
            Ok(Ok(response)) => Ok(response.data),
            Ok(Err(error)) => Err(WriteFailure::RaftRejected(error.to_string())),
            Err(_) => Err(WriteFailure::OutcomeUnknown),
        }
    }

    pub async fn data(&self, node: u64) -> OwnershipData {
        self.machines[&node].data().await
    }

    pub async fn wait_data(&self, nodes: &[u64], expected: &OwnershipData) {
        let start = Instant::now();
        loop {
            let mut matches = true;
            for node in nodes {
                matches &= self.data(*node).await == *expected;
            }
            if matches {
                return;
            }
            assert!(
                start.elapsed() < WAIT,
                "state machines did not converge: {nodes:?}"
            );
            sleep(Duration::from_millis(10)).await;
        }
    }

    /// Remove from the transport and shut down the real OpenRaft task, without
    /// removing its vote from cluster membership. Storage is retained for inspection.
    pub async fn stop(&mut self, node: u64) {
        self.bus.remove(node);
        if let Some(raft) = self.nodes.remove(&node) {
            raft.shutdown().await.expect("shutdown raft");
        }
    }

    pub fn isolate(&self, node: u64) {
        for other in self.nodes.keys().copied().filter(|id| *id != node) {
            self.bus.block(node, other);
            self.bus.block(other, node);
        }
    }

    pub async fn shutdown(&mut self) {
        for id in self.nodes.keys().copied().collect::<Vec<_>>() {
            self.stop(id).await;
        }
    }
}
