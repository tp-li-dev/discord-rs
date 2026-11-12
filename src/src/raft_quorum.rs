//! Experimental, durable OpenRaft ownership coordinator (OpenRaft 0.9.26).
//!
//! Elections, replication, votes and commit decisions belong exclusively to
//! OpenRaft. The application state machine implements per-(bot, shard) epoch CAS.
//! This module is opt-in and is NOT wired into botd or the existing refusing
//! `ownership::QuorumCoordinator`. Production startup is still refused, even
//! with `enable_production`, until authenticated RPC and runtime fencing are
//! integrated and validated. No Bot credentials enter this API or its journal.
//!
//! # Synchronous ownership boundary
//!
//! `OwnershipCoordinator` is synchronous. Call `prepare_acquire` / `prepare_renew`
//! asynchronously, then let the existing trait consume the exact, one-use grant.
//! Alternatively use `acquire_quorum` / `renew_quorum`. Unprepared calls fail
//! closed. `check` and `watchdog` do no network or disk IO, and deadlines start
//! BEFORE the Raft request, never at response arrival. A timeout or cancellation
//! may still commit later, but cannot grant local authority by itself.
//!
//! # Conservative handoff, not automatic crash failover
//!
//! A durable reservation stays held until its originating coordinator closes its
//! local admission gate and commits `release_quorum`. Local expiry alone does
//! NOT allow another host to take over: remote monotonic timestamps and Raft's
//! leader lease are not shard lease proofs. `release` on the synchronous trait
//! closes only the local gate; follow it with `release_quorum` to free the durable
//! reservation. Cancellation can leave a reservation held; the same coordinator
//! can release it, but a new session after a crash cannot adopt it. This sacrifices
//! failover availability rather than revoking a disconnected holder by assumption.
//! There is intentionally no force-unlock or wall-clock based takeover API.
//!
//! All new work MUST use this coordinator's admission gate. Release callers must
//! also stop any delegated admission gates before releasing. Existing work and
//! external side effects need their own draining/fencing protocol. This module
//! does not stop Gateway/Core processes, guarantee exactly-once effects, or prove
//! clock safety under OS suspend. Linux, power loss, real hosts and OS suspend
//! require separate validation. Fixed membership only; no membership CLI/recovery
//! administration, client forwarding, or automatic journal compaction is provided.

pub mod storage;
pub mod transport;

use crate::ownership::{Clock, Lease, OwnershipCoordinator, OwnershipError, OwnershipKey};
use crate::protocol::{BotId, OwnershipEpoch};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    path::Path,
    sync::Arc,
    time::Duration,
};
use storage::{FileStore, StateMachine};
use transport::{NetworkFactory, RaftRpcTransport};

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct ShardKey {
    pub bot_id: u64,
    pub shard_id: u32,
}

impl From<OwnershipKey> for ShardKey {
    fn from(key: OwnershipKey) -> Self {
        Self {
            bot_id: key.bot_id.0,
            shard_id: key.shard_id,
        }
    }
}

impl From<ShardKey> for OwnershipKey {
    fn from(key: ShardKey) -> Self {
        Self {
            bot_id: BotId(key.bot_id),
            shard_id: key.shard_id,
        }
    }
}

/// A replicated reservation, NOT a locally usable lease. Sessions are random
/// public incarnation identifiers, not authentication secrets.
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct Ownership {
    pub epoch: u64,
    pub owner: u64,
    pub session: [u8; 16],
    pub held: bool,
}

/// Trusted control-plane commands; remote user authorization precedes this API.
/// A release is an assertion by the originating holder that its gates are closed.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum Command {
    Acquire {
        key: ShardKey,
        expected_epoch: u64,
        owner: u64,
        session: [u8; 16],
    },
    Renew {
        key: ShardKey,
        epoch: u64,
        owner: u64,
        session: [u8; 16],
    },
    Release {
        key: ShardKey,
        epoch: u64,
        owner: u64,
        session: [u8; 16],
    },
}

impl Command {
    fn identity(&self) -> (u64, [u8; 16]) {
        match self {
            Self::Acquire { owner, session, .. }
            | Self::Renew { owner, session, .. }
            | Self::Release { owner, session, .. } => (*owner, *session),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum Outcome {
    Applied(Ownership),
    Stale { current_epoch: u64 },
    Held,
    NotOwned,
    EpochExhausted,
    InvalidOwner,
    Noop,
}

/// Serialize as pairs: JSON object keys cannot represent the structured shard
/// key. Duplicate keys on disk/on the wire are rejected rather than overwritten.
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct OwnershipData(#[serde(with = "ownership_pairs")] pub BTreeMap<ShardKey, Ownership>);

mod ownership_pairs {
    use super::{Ownership, ShardKey};
    use serde::{Deserialize, Deserializer, Serialize, Serializer};
    use std::collections::BTreeMap;

    pub fn serialize<S: Serializer>(
        map: &BTreeMap<ShardKey, Ownership>,
        s: S,
    ) -> Result<S::Ok, S::Error> {
        map.iter().collect::<Vec<_>>().serialize(s)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(
        d: D,
    ) -> Result<BTreeMap<ShardKey, Ownership>, D::Error> {
        let pairs = Vec::<(ShardKey, Ownership)>::deserialize(d)?;
        let mut map = BTreeMap::new();
        for (key, value) in pairs {
            if value.epoch == 0
                || value.owner == 0
                || value.session == [0; 16]
                || map.insert(key, value).is_some()
            {
                return Err(serde::de::Error::custom("invalid ownership record"));
            }
        }
        Ok(map)
    }
}

impl OwnershipData {
    /// Deterministic application logic applied ONLY to committed Raft entries.
    pub fn apply(&mut self, request: &Command) -> Outcome {
        let (owner, session) = request.identity();
        if owner == 0 || session == [0; 16] {
            return Outcome::InvalidOwner;
        }
        match *request {
            Command::Acquire {
                key,
                expected_epoch,
                ..
            } => {
                let previous = self.0.get(&key);
                let current_epoch = previous.map_or(0, |o| o.epoch);
                if expected_epoch != current_epoch {
                    return Outcome::Stale { current_epoch };
                }
                if previous.is_some_and(|o| o.held) {
                    return Outcome::Held;
                }
                let Some(epoch) = current_epoch.checked_add(1) else {
                    return Outcome::EpochExhausted;
                };
                let record = Ownership {
                    epoch,
                    owner,
                    session,
                    held: true,
                };
                self.0.insert(key, record);
                Outcome::Applied(record)
            }
            Command::Renew { key, epoch, .. } | Command::Release { key, epoch, .. } => {
                let Some(record) = self.0.get_mut(&key) else {
                    return Outcome::NotOwned;
                };
                if epoch != record.epoch {
                    return Outcome::Stale {
                        current_epoch: record.epoch,
                    };
                }
                if owner != record.owner || session != record.session {
                    return Outcome::NotOwned;
                }
                if matches!(request, Command::Release { .. }) {
                    // Idempotent release of this exact reservation. Epoch is retained.
                    record.held = false;
                } else if !record.held {
                    return Outcome::NotOwned;
                }
                Outcome::Applied(*record)
            }
        }
    }
}

openraft::declare_raft_types!(
    pub TypeConfig:
        D = Command,
        R = Outcome,
        NodeId = u64,
        Node = (),
        SnapshotData = OwnershipData,
);

pub type Raft = openraft::Raft<TypeConfig>;
pub type Entry = openraft::Entry<TypeConfig>;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StorageMode {
    Create,
    Open,
}

#[derive(Clone, Debug)]
pub struct NodeOptions {
    /// Explicit, non-production use. Never inferred from absent credentials.
    pub allow_test_mode: bool,
    /// Necessary but not sufficient: production is currently refused because
    /// tls_transport and runtime fencing are not yet connected.
    pub enable_production: bool,
    pub write_timeout: Duration,
    pub max_lease_ttl: Duration,
    pub raft_config: openraft::Config,
}

impl Default for NodeOptions {
    fn default() -> Self {
        Self {
            allow_test_mode: false,
            enable_production: false,
            write_timeout: Duration::from_secs(3),
            max_lease_ttl: Duration::from_secs(30),
            raft_config: openraft::Config {
                cluster_name: "botd-raft-quorum".into(),
                snapshot_policy: openraft::SnapshotPolicy::LogsSinceLast(256),
                ..openraft::Config::default()
            },
        }
    }
}

/// Fixed diagnostics: never interpolate payloads, paths, library error text or
/// any credentials into an error returned by this boundary.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum QuorumError {
    Disabled,
    ProductionNotReady,
    InvalidConfig,
    Storage,
    RaftRejected,
    /// Submission may commit after the deadline. This is not a cancellation ACK.
    OutcomeUnknown,
}
impl std::fmt::Display for QuorumError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Raft quorum: {self:?}")
    }
}
impl std::error::Error for QuorumError {}

/// Low-level trusted OpenRaft handles are exposed for RPC dispatch/diagnostics.
/// Do not use metrics/local reads as grants or mutate membership behind this
/// fixed-membership adapter. Bootstrap is explicit and never done during reopen.
pub struct RaftNode {
    pub node_id: u64,
    pub raft: Raft,
    pub machine: StateMachine,
    members: BTreeSet<u64>,
    options: NodeOptions,
}

impl RaftNode {
    pub async fn start(
        path: impl AsRef<Path>,
        node_id: u64,
        members: BTreeSet<u64>,
        mode: StorageMode,
        options: NodeOptions,
        transport: Arc<dyn RaftRpcTransport>,
    ) -> Result<Self, QuorumError> {
        // Gate before filesystem/network effects. The flag is deliberately NOT a
        // claim that this backend is ready for deployment.
        if options.enable_production {
            return Err(QuorumError::ProductionNotReady);
        }
        if !options.allow_test_mode {
            return Err(QuorumError::Disabled);
        }
        if node_id == 0
            || members.contains(&0)
            || !members.contains(&node_id)
            || !(3..=31).contains(&members.len())
            || members.len().is_multiple_of(2)
            || options.write_timeout.is_zero()
            || options.write_timeout > Duration::from_secs(60)
            || options.max_lease_ttl.is_zero()
            || options.max_lease_ttl > Duration::from_secs(300)
        {
            return Err(QuorumError::InvalidConfig);
        }
        let config = options
            .raft_config
            .clone()
            .validate()
            .map_err(|_| QuorumError::InvalidConfig)?;
        let (logs, machine) = match mode {
            StorageMode::Create => {
                FileStore::create_in_cluster(
                    path,
                    node_id,
                    members.clone(),
                    options.raft_config.cluster_name.clone(),
                )
                .await
            }
            StorageMode::Open => {
                FileStore::open_in_cluster(
                    path,
                    node_id,
                    members.clone(),
                    options.raft_config.cluster_name.clone(),
                )
                .await
            }
        }
        .map_err(|_| QuorumError::Storage)?;
        let raft = Raft::new(
            node_id,
            Arc::new(config),
            NetworkFactory::new(node_id, transport),
            logs,
            machine.clone(),
        )
        .await
        .map_err(|_| QuorumError::Storage)?;
        Ok(Self {
            node_id,
            raft,
            machine,
            members,
            options,
        })
    }

    /// Bootstrap ONCE on one node, with all peers configured identically. A
    /// missing/corrupt existing store must never be recreated by a restart path.
    pub async fn initialize(&self) -> Result<(), QuorumError> {
        self.raft
            .initialize(
                self.members
                    .iter()
                    .map(|id| (*id, ()))
                    .collect::<BTreeMap<_, _>>(),
            )
            .await
            .map_err(|_| QuorumError::RaftRejected)
    }

    /// The selected node must be leader; no unauthenticated client-forwarding
    /// endpoint or automatic retries are created. `Applied` is a durable commit
    /// result, but callers still need a local lease before admitting work.
    pub async fn submit(&self, command: Command) -> Result<Outcome, QuorumError> {
        let (owner, session) = command.identity();
        if !self.members.contains(&owner) || session == [0; 16] {
            return Ok(Outcome::InvalidOwner);
        }
        match tokio::time::timeout(self.options.write_timeout, self.raft.client_write(command))
            .await
        {
            Ok(Ok(response)) => Ok(response.data),
            Ok(Err(_)) => Err(QuorumError::RaftRejected),
            Err(_) => Err(QuorumError::OutcomeUnknown),
        }
    }

    pub async fn shutdown(&self) -> Result<(), QuorumError> {
        self.raft
            .shutdown()
            .await
            .map_err(|_| QuorumError::RaftRejected)
    }
}

#[derive(Clone, Copy)]
enum PreparationKind {
    Acquire,
    Renew,
}

#[derive(Clone, Copy)]
struct Prepared {
    kind: PreparationKind,
    ttl: Duration,
    lease: Lease,
}

/// Non-cloneable local admission gate backed by committed Raft reservations.
/// An asynchronous prepare phase bridges the existing synchronous trait. This
/// currently uses a leader-local client; it does not transparently forward writes.
pub struct QuorumCoordinator<C: Clock> {
    node: Arc<RaftNode>,
    clock: C,
    session: [u8; 16],
    leases: HashMap<OwnershipKey, Lease>,
    prepared: HashMap<OwnershipKey, Prepared>,
    // Includes outcome-unknown requests, so an ungranted reservation can be
    // explicitly relinquished by its original session without granting a lease.
    claims: HashMap<OwnershipKey, OwnershipEpoch>,
    last_now: Duration,
    clock_failed: bool,
}

impl<C: Clock> QuorumCoordinator<C> {
    pub fn new(node: Arc<RaftNode>, clock: C) -> Result<Self, OwnershipError> {
        let mut session = [0; 16];
        getrandom::fill(&mut session).map_err(|_| OwnershipError::QuorumUnavailable)?;
        if session == [0; 16] {
            return Err(OwnershipError::QuorumUnavailable);
        }
        let last_now = clock.now();
        Ok(Self {
            node,
            clock,
            session,
            leases: HashMap::new(),
            prepared: HashMap::new(),
            claims: HashMap::new(),
            last_now,
            clock_failed: false,
        })
    }

    fn now(&mut self) -> Result<Duration, OwnershipError> {
        let now = self.clock.now();
        if self.clock_failed || now < self.last_now {
            self.clock_failed = true;
            self.leases.clear();
            self.prepared.clear();
            return Err(OwnershipError::ClockRegressed);
        }
        self.last_now = now;
        Ok(now)
    }

    fn deadline(&self, now: Duration, ttl: Duration) -> Result<Duration, OwnershipError> {
        if ttl.is_zero() || ttl > self.node.options.max_lease_ttl {
            return Err(OwnershipError::InvalidDuration);
        }
        now.checked_add(ttl).ok_or(OwnershipError::TimeOverflow)
    }

    fn applied(outcome: Outcome) -> Result<Ownership, OwnershipError> {
        match outcome {
            Outcome::Applied(record) => Ok(record),
            Outcome::Stale { .. } | Outcome::Held => Err(OwnershipError::Fenced),
            Outcome::EpochExhausted => Err(OwnershipError::EpochExhausted),
            Outcome::NotOwned | Outcome::InvalidOwner => Err(OwnershipError::NotOwned),
            Outcome::Noop => Err(OwnershipError::QuorumUnavailable),
        }
    }

    pub async fn prepare_acquire(
        &mut self,
        key: OwnershipKey,
        expected_epoch: OwnershipEpoch,
        ttl: Duration,
    ) -> Result<(), OwnershipError> {
        let now = self.now()?;
        let expires_at = self.deadline(now, ttl)?;
        if self.claims.contains_key(&key) {
            return Err(OwnershipError::Fenced);
        }
        let epoch = OwnershipEpoch(
            expected_epoch
                .0
                .checked_add(1)
                .ok_or(OwnershipError::EpochExhausted)?,
        );
        self.prepared.remove(&key);
        // Record before await: cancellation must never make an ambiguous claim
        // available for local acquisition/retry with a different identity.
        self.claims.insert(key, epoch);
        let outcome = self
            .node
            .submit(Command::Acquire {
                key: key.into(),
                expected_epoch: expected_epoch.0,
                owner: self.node.node_id,
                session: self.session,
            })
            .await
            .map_err(|_| OwnershipError::QuorumUnavailable)?;
        let record = match Self::applied(outcome) {
            Ok(record) => record,
            Err(error) => {
                self.claims.remove(&key);
                return Err(error);
            }
        };
        if !record.held
            || record.epoch != epoch.0
            || record.owner != self.node.node_id
            || record.session != self.session
        {
            return Err(OwnershipError::QuorumUnavailable);
        }
        if self.now()? >= expires_at {
            return Err(OwnershipError::Expired);
        }
        self.prepared.insert(
            key,
            Prepared {
                kind: PreparationKind::Acquire,
                ttl,
                lease: Lease {
                    key,
                    epoch,
                    expires_at,
                },
            },
        );
        Ok(())
    }

    pub async fn acquire_quorum(
        &mut self,
        key: OwnershipKey,
        expected_epoch: OwnershipEpoch,
        ttl: Duration,
    ) -> Result<Lease, OwnershipError> {
        self.prepare_acquire(key, expected_epoch, ttl).await?;
        self.acquire(key, ttl)
    }

    pub async fn prepare_renew(
        &mut self,
        key: OwnershipKey,
        epoch: OwnershipEpoch,
        ttl: Duration,
    ) -> Result<(), OwnershipError> {
        self.check(key, epoch)?;
        let old = self.leases[&key];
        let now = self.now()?;
        let expires_at = self.deadline(now, ttl)?.max(old.expires_at);
        self.prepared.remove(&key);
        let record = Self::applied(
            self.node
                .submit(Command::Renew {
                    key: key.into(),
                    epoch: epoch.0,
                    owner: self.node.node_id,
                    session: self.session,
                })
                .await
                .map_err(|_| OwnershipError::QuorumUnavailable)?,
        )?;
        // A renewal that arrives after the previous lease expired cannot revive
        // authority, even if the new proposed deadline is still in the future.
        self.check(key, epoch)?;
        if !record.held
            || record.epoch != epoch.0
            || record.owner != self.node.node_id
            || record.session != self.session
        {
            return Err(OwnershipError::QuorumUnavailable);
        }
        self.prepared.insert(
            key,
            Prepared {
                kind: PreparationKind::Renew,
                ttl,
                lease: Lease {
                    key,
                    epoch,
                    expires_at,
                },
            },
        );
        Ok(())
    }

    pub async fn renew_quorum(
        &mut self,
        key: OwnershipKey,
        epoch: OwnershipEpoch,
        ttl: Duration,
    ) -> Result<Lease, OwnershipError> {
        self.prepare_renew(key, epoch, ttl).await?;
        self.renew(key, epoch, ttl)
    }

    /// Stop local admissions FIRST, then release the exact durable reservation.
    /// On quorum loss the local gate stays closed; retry is safe. The originating
    /// session may also release an outcome-unknown or expired unconsumed grant.
    pub async fn release_quorum(
        &mut self,
        key: OwnershipKey,
        epoch: OwnershipEpoch,
    ) -> Result<(), OwnershipError> {
        self.release(key, epoch)?;
        let record = Self::applied(
            self.node
                .submit(Command::Release {
                    key: key.into(),
                    epoch: epoch.0,
                    owner: self.node.node_id,
                    session: self.session,
                })
                .await
                .map_err(|_| OwnershipError::QuorumUnavailable)?,
        )?;
        if record.held
            || record.epoch != epoch.0
            || record.owner != self.node.node_id
            || record.session != self.session
        {
            return Err(OwnershipError::QuorumUnavailable);
        }
        self.claims.remove(&key);
        Ok(())
    }
}

impl<C: Clock> OwnershipCoordinator for QuorumCoordinator<C> {
    fn acquire(&mut self, key: OwnershipKey, ttl: Duration) -> Result<Lease, OwnershipError> {
        let now = self.now()?;
        let prepared = self
            .prepared
            .remove(&key)
            .ok_or(OwnershipError::QuorumUnavailable)?;
        if !matches!(prepared.kind, PreparationKind::Acquire) || prepared.ttl != ttl {
            return Err(OwnershipError::QuorumUnavailable);
        }
        if now >= prepared.lease.expires_at {
            return Err(OwnershipError::Expired);
        }
        self.leases.insert(key, prepared.lease);
        Ok(prepared.lease)
    }

    fn renew(
        &mut self,
        key: OwnershipKey,
        epoch: OwnershipEpoch,
        ttl: Duration,
    ) -> Result<Lease, OwnershipError> {
        self.check(key, epoch)?;
        let prepared = self
            .prepared
            .remove(&key)
            .ok_or(OwnershipError::QuorumUnavailable)?;
        if !matches!(prepared.kind, PreparationKind::Renew)
            || prepared.ttl != ttl
            || prepared.lease.epoch != epoch
        {
            return Err(OwnershipError::QuorumUnavailable);
        }
        if self.now()? >= prepared.lease.expires_at {
            return Err(OwnershipError::Expired);
        }
        self.leases.insert(key, prepared.lease);
        Ok(prepared.lease)
    }

    fn release(&mut self, key: OwnershipKey, epoch: OwnershipEpoch) -> Result<(), OwnershipError> {
        let claimed = self.claims.get(&key).ok_or(OwnershipError::NotOwned)?;
        if *claimed != epoch {
            return Err(OwnershipError::Fenced);
        }
        self.prepared.remove(&key);
        self.leases.remove(&key);
        Ok(())
    }

    fn check(&mut self, key: OwnershipKey, epoch: OwnershipEpoch) -> Result<(), OwnershipError> {
        let now = self.now()?;
        let lease = self.leases.get(&key).ok_or(OwnershipError::NotOwned)?;
        if lease.epoch != epoch {
            return Err(OwnershipError::Fenced);
        }
        if now >= lease.expires_at {
            return Err(OwnershipError::Expired);
        }
        Ok(())
    }

    fn watchdog(&mut self) -> Result<Vec<OwnershipKey>, OwnershipError> {
        let now = self.now()?;
        let expired = self
            .leases
            .values()
            .filter(|l| now >= l.expires_at)
            .map(|l| l.key)
            .collect::<Vec<_>>();
        for key in &expired {
            self.leases.remove(key);
        }
        self.prepared.retain(|_, p| now < p.lease.expires_at);
        Ok(expired)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cas_handoff_session_fencing_and_overflow() {
        let key = ShardKey {
            bot_id: 7,
            shard_id: 2,
        };
        let mut data = OwnershipData::default();
        let acquire = Command::Acquire {
            key,
            expected_epoch: 0,
            owner: 1,
            session: [1; 16],
        };
        assert!(matches!(
            data.apply(&acquire),
            Outcome::Applied(Ownership { epoch: 1, .. })
        ));
        assert_eq!(data.apply(&acquire), Outcome::Stale { current_epoch: 1 });
        assert_eq!(
            data.apply(&Command::Acquire {
                key,
                expected_epoch: 1,
                owner: 2,
                session: [2; 16]
            }),
            Outcome::Held
        );
        assert_eq!(
            data.apply(&Command::Release {
                key,
                epoch: 1,
                owner: 1,
                session: [2; 16]
            }),
            Outcome::NotOwned
        );
        assert!(matches!(
            data.apply(&Command::Release {
                key,
                epoch: 1,
                owner: 1,
                session: [1; 16]
            }),
            Outcome::Applied(Ownership { held: false, .. })
        ));
        assert_eq!(
            data.apply(&Command::Renew {
                key,
                epoch: 1,
                owner: 1,
                session: [1; 16]
            }),
            Outcome::NotOwned
        );
        data.0.get_mut(&key).unwrap().epoch = u64::MAX;
        let before = data.clone();
        assert_eq!(
            data.apply(&Command::Acquire {
                key,
                expected_epoch: u64::MAX,
                owner: 2,
                session: [2; 16]
            }),
            Outcome::EpochExhausted
        );
        assert_eq!(data, before);
    }

    #[test]
    fn structured_keys_roundtrip_and_duplicate_keys_fail() {
        let mut data = OwnershipData::default();
        let key = ShardKey {
            bot_id: u64::MAX,
            shard_id: u32::MAX,
        };
        data.apply(&Command::Acquire {
            key,
            expected_epoch: 0,
            owner: 1,
            session: [1; 16],
        });
        let bytes = serde_json::to_vec(&data).unwrap();
        assert_eq!(
            serde_json::from_slice::<OwnershipData>(&bytes).unwrap(),
            data
        );
        let pair = (key, data.0[&key]);
        assert!(
            serde_json::from_slice::<OwnershipData>(
                &serde_json::to_vec(&vec![pair, pair]).unwrap()
            )
            .is_err()
        );
    }
}
