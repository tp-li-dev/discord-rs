//! Direct in-process RPC adapter with directed link failures. No sockets or auth.
use crate::{Raft, TypeConfig};
use openraft::error::{
    Fatal, RPCError, RaftError, RemoteError, ReplicationClosed, StreamingError, Unreachable,
};
use openraft::network::{Backoff, RPCOption, RaftNetwork, RaftNetworkFactory};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, SnapshotResponse, VoteRequest, VoteResponse,
};
use openraft::{OptionalSend, Snapshot, Vote};
use std::{
    collections::{BTreeMap, BTreeSet},
    future::Future,
    sync::{Arc, Mutex},
    time::Duration,
};

#[derive(Default)]
struct Links {
    nodes: BTreeMap<u64, Raft>,
    blocked: BTreeSet<(u64, u64)>,
    snapshots: usize,
}

#[derive(Clone, Default)]
pub struct Bus(Arc<Mutex<Links>>);

impl Bus {
    pub fn register(&self, id: u64, raft: Raft) {
        self.0.lock().unwrap().nodes.insert(id, raft);
    }
    pub fn remove(&self, id: u64) {
        self.0.lock().unwrap().nodes.remove(&id);
    }
    pub fn block(&self, from: u64, to: u64) {
        self.0.lock().unwrap().blocked.insert((from, to));
    }
    pub fn heal(&self) {
        self.0.lock().unwrap().blocked.clear();
    }
    pub fn snapshot_count(&self) -> usize {
        self.0.lock().unwrap().snapshots
    }

    fn endpoint(&self, from: u64, to: u64) -> Result<Raft, Unreachable> {
        let links = self.0.lock().unwrap();
        if links.nodes.contains_key(&from)
            && !links.blocked.contains(&(from, to))
            && let Some(raft) = links.nodes.get(&to)
        {
            return Ok(raft.clone());
        }
        Err(Unreachable::new(&std::io::Error::other(
            "in-memory link unavailable",
        )))
    }
}

pub struct NetworkFactory {
    pub source: u64,
    pub bus: Bus,
}
pub struct Connection {
    source: u64,
    target: u64,
    bus: Bus,
}

impl RaftNetworkFactory<TypeConfig> for NetworkFactory {
    type Network = Connection;
    async fn new_client(&mut self, target: u64, _: &()) -> Connection {
        Connection {
            source: self.source,
            target,
            bus: self.bus.clone(),
        }
    }
}

impl RaftNetwork<TypeConfig> for Connection {
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<TypeConfig>,
        _: RPCOption,
    ) -> Result<AppendEntriesResponse<u64>, RPCError<u64, (), RaftError<u64>>> {
        let raft = self
            .bus
            .endpoint(self.source, self.target)
            .map_err(RPCError::Unreachable)?;
        raft.append_entries(rpc)
            .await
            .map_err(|e| RPCError::RemoteError(RemoteError::new(self.target, e)))
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<u64>,
        _: RPCOption,
    ) -> Result<VoteResponse<u64>, RPCError<u64, (), RaftError<u64>>> {
        let raft = self
            .bus
            .endpoint(self.source, self.target)
            .map_err(RPCError::Unreachable)?;
        raft.vote(rpc)
            .await
            .map_err(|e| RPCError::RemoteError(RemoteError::new(self.target, e)))
    }

    async fn full_snapshot(
        &mut self,
        vote: Vote<u64>,
        snapshot: Snapshot<TypeConfig>,
        cancel: impl Future<Output = ReplicationClosed> + OptionalSend + 'static,
        _: RPCOption,
    ) -> Result<SnapshotResponse<u64>, StreamingError<TypeConfig, Fatal<u64>>> {
        let raft = self
            .bus
            .endpoint(self.source, self.target)
            .map_err(StreamingError::Unreachable)?;
        tokio::select! {
            reason = cancel => Err(StreamingError::Closed(reason)),
            result = raft.install_full_snapshot(vote, snapshot) => {
                let response = result.map_err(|e| StreamingError::RemoteError(RemoteError::new(self.target, e)))?;
                self.bus.0.lock().unwrap().snapshots += 1;
                Ok(response)
            }
        }
    }

    fn backoff(&self) -> Backoff {
        Backoff::new(std::iter::repeat(Duration::from_millis(30)))
    }
}
