//! In-memory evaluation only. No leases, process fencing, authentication or durability.
//! Elections, replication, membership and commit decisions belong entirely to OpenRaft.
pub mod cluster;
pub mod network;
pub mod storage;

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// Ownership is scoped by both bot and shard; different bots may share a shard id.
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct ShardKey {
    pub bot_id: u64,
    pub shard_id: u32,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct Ownership {
    pub ownership_epoch: u64,
    pub owner: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Transfer {
    pub key: ShardKey,
    pub expected_epoch: u64,
    pub new_owner: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum Outcome {
    Applied(Ownership),
    Stale { current_epoch: u64 },
    EpochExhausted,
    Noop,
}

/// Generic snapshots clone this data in memory; no on-disk/wire codec is claimed.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct OwnershipData(pub BTreeMap<ShardKey, Ownership>);

impl OwnershipData {
    /// Deterministic application state machine, not a consensus algorithm.
    pub fn transfer(&mut self, request: &Transfer) -> Outcome {
        let current = self.0.get(&request.key).map_or(0, |o| o.ownership_epoch);
        if current != request.expected_epoch {
            return Outcome::Stale {
                current_epoch: current,
            };
        }
        let Some(epoch) = current.checked_add(1) else {
            return Outcome::EpochExhausted;
        };
        let ownership = Ownership {
            ownership_epoch: epoch,
            owner: request.new_owner,
        };
        self.0.insert(request.key, ownership);
        Outcome::Applied(ownership)
    }
}

openraft::declare_raft_types!(
    pub TypeConfig:
        D = Transfer,
        R = Outcome,
        NodeId = u64,
        Node = (),
        SnapshotData = OwnershipData,
);

pub type Raft = openraft::Raft<TypeConfig>;
pub type Entry = openraft::Entry<TypeConfig>;
