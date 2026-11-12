//! Deterministic, in-process control-plane **simulation**, not a consensus backend.
//!
//! `TestQuorumCoordinator` counts transported ACKs from a fixed membership and
//! serializes decisions through one test oracle backed by `StandaloneCoordinator`.
//! It has no election, replicated log, persistence, membership changes or Raft.
//! Production `ownership::QuorumCoordinator` remains disabled. Its replacement
//! must follow `docs/quorum-evaluation.md` (OpenRaft evaluation).
//!
//! Every simulated host has its own cached admission lease. It cannot consult the
//! oracle to revoke another host: transfers wait for the previous lease deadline.
//! All clocks share one injected time domain. This deliberately does NOT establish
//! safety under clock skew, OS suspend, process stalls, real TLS/Tailscale, Discord
//! connections, Identify limits, or external side effects. Startup models readiness
//! of a separate Gateway/Core pair per (bot, shard), without spawning processes.
//! Node restart retains only the oracle, modelling durable control-plane history;
//! real crash recovery and loss of that history remain unimplemented.

use crate::{
    failover::{self, Event, Failover, State},
    ownership::{
        Clock, Lease, OwnershipCoordinator, OwnershipError, OwnershipKey, StandaloneCoordinator,
    },
    protocol::OwnershipEpoch,
};
use std::{
    cell::Cell,
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, Mutex},
    time::Duration,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct NodeId(pub u64);

// OwnershipKey intentionally need not gain Ord in the production module.
type Key = (u64, u32);
fn index(key: OwnershipKey) -> Key {
    (key.bot_id.0, key.shard_id)
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ShardPlacement {
    pub key: OwnershipKey,
    pub primary: NodeId,
    pub failover: Vec<NodeId>,
}

#[derive(Clone, Copy, Debug)]
pub struct SimulationConfig {
    pub failover: failover::Config,
    pub lease_ttl: Duration,
    pub renew_every: Duration,
    pub proposal_timeout: Duration,
    pub startup_delay: Duration,
}
impl Default for SimulationConfig {
    fn default() -> Self {
        Self {
            failover: failover::Config::default(),
            lease_ttl: Duration::from_secs(4),
            renew_every: Duration::from_secs(1),
            proposal_timeout: Duration::from_secs(3),
            startup_delay: Duration::ZERO,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SimulationError {
    InvalidConfig,
    UnknownNode,
    InvalidNodeState,
    ClockRegressed,
    TimeOverflow,
    CounterExhausted,
    Ownership(OwnershipError),
    Policy(failover::Error),
    MultipleActive(OwnershipKey),
    Transport(crate::tls_transport::TransportError),
}
impl std::fmt::Display for SimulationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "simulation error: {self:?}")
    }
}
impl std::error::Error for SimulationError {}
impl From<OwnershipError> for SimulationError {
    fn from(e: OwnershipError) -> Self {
        Self::Ownership(e)
    }
}
impl From<failover::Error> for SimulationError {
    fn from(e: failover::Error) -> Self {
        Self::Policy(e)
    }
}
fn plus(a: Duration, b: Duration) -> Result<Duration, SimulationError> {
    a.checked_add(b).ok_or(SimulationError::TimeOverflow)
}

/// Shared, monotonic virtual clock. No wall-clock sleeps or background tasks.
#[derive(Clone, Default)]
pub struct ManualClock(Arc<Mutex<Duration>>);
impl Clock for ManualClock {
    fn now(&self) -> Duration {
        *self.0.lock().expect("simulation clock lock poisoned")
    }
}
impl ManualClock {
    pub fn set(&self, at: Duration) -> Result<(), SimulationError> {
        let mut time = self.0.lock().expect("simulation clock lock poisoned");
        if at < *time {
            return Err(SimulationError::ClockRegressed);
        }
        *time = at;
        Ok(())
    }
    pub fn advance(&self, by: Duration) -> Result<(), SimulationError> {
        self.set(plus(self.now(), by)?)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct OwnershipRecord {
    pub owner: NodeId,
    pub lease: Lease,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ProposalKind {
    Acquire,
    Renew,
    Failback,
}

/// Deadlines are fixed at request creation, never extended by ACK delivery.
/// These timestamps are valid ONLY in the simulation's shared clock domain.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct OwnershipProposal {
    pub id: u64,
    pub requester: NodeId,
    pub requester_incarnation: u64,
    pub target: NodeId,
    pub target_incarnation: u64,
    pub key: OwnershipKey,
    pub expected_epoch: OwnershipEpoch,
    pub kind: ProposalKind,
    pub attempt: u64,
    pub requested_at: Duration,
    pub expires_at: Duration,
    pub ack_deadline: Duration,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Heartbeat {
    pub sequence: u64,
    pub sent_at: Duration,
    pub ownership: Vec<OwnershipRecord>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ControlMessage {
    Heartbeat(Heartbeat),
    OwnershipProposal(OwnershipProposal),
    OwnershipAck {
        proposal: OwnershipProposal,
    },
    OwnershipDecision {
        proposal: OwnershipProposal,
        granted: bool,
        current: Option<OwnershipRecord>,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Envelope {
    pub sequence: u64,
    pub from: NodeId,
    pub to: NodeId,
    pub from_incarnation: u64,
    pub to_incarnation: u64,
    pub sent_at: Duration,
    pub deliver_at: Duration,
    pub message: ControlMessage,
}

/// A transport supplies authenticated member identities. The in-memory backend
/// trusts its test caller; it is not an implementation of remote authentication.
/// No socket listener, tokens or remote management command exists in this module.
/// The TCP implementation lives in `tls_transport`. Its synchronous `send`
/// acknowledges bounded local queue admission, not remote delivery; failures
/// after admission must be observed through that implementation's diagnostics.
pub trait Transport {
    fn send(
        &mut self,
        from: NodeId,
        to: NodeId,
        message: ControlMessage,
    ) -> Result<(), SimulationError>;
    fn receive(&mut self) -> Result<Option<Envelope>, SimulationError>;
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Link {
    pub delay: Duration,
    pub loss_per_mille: u16,
    pub enabled: bool,
}
impl Default for Link {
    fn default() -> Self {
        Self {
            delay: Duration::ZERO,
            loss_per_mille: 0,
            enabled: true,
        }
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct NetworkStats {
    pub sent: u64,
    pub delivered: u64,
    pub dropped: u64,
}

#[derive(Clone, Copy)]
struct Endpoint {
    running: bool,
    incarnation: u64,
}

/// FIFO for equal deadlines, otherwise delivery-time ordered. Loss uses a stable
/// SplitMix64 sequence (not cryptographic). Link changes affect in-flight packets
/// at delivery too; restart discards packets of previous endpoint incarnations.
/// Queue capacity bounds even arbitrarily delayed traffic; overflow drops packets.
pub struct InMemoryNetwork<C> {
    clock: C,
    endpoints: BTreeMap<NodeId, Endpoint>,
    links: BTreeMap<(NodeId, NodeId), Link>,
    queue: BTreeMap<(Duration, u64), Envelope>,
    capacity: usize,
    sequence: u64,
    random: u64,
    last_now: Duration,
    clock_failed: bool,
    stats: NetworkStats,
}
impl<C: Clock> InMemoryNetwork<C> {
    pub fn new(
        clock: C,
        members: Vec<NodeId>,
        seed: u64,
        capacity: usize,
    ) -> Result<Self, SimulationError> {
        let unique: BTreeSet<_> = members.iter().copied().collect();
        if unique.is_empty() || unique.len() != members.len() || capacity == 0 {
            return Err(SimulationError::InvalidConfig);
        }
        let last_now = clock.now();
        Ok(Self {
            clock,
            endpoints: unique
                .into_iter()
                .map(|id| {
                    (
                        id,
                        Endpoint {
                            running: true,
                            incarnation: 1,
                        },
                    )
                })
                .collect(),
            links: BTreeMap::new(),
            queue: BTreeMap::new(),
            capacity,
            sequence: 0,
            random: seed,
            last_now,
            clock_failed: false,
            stats: NetworkStats::default(),
        })
    }
    fn now(&mut self) -> Result<Duration, SimulationError> {
        let now = self.clock.now();
        if self.clock_failed || now < self.last_now {
            self.clock_failed = true;
            return Err(SimulationError::ClockRegressed);
        }
        self.last_now = now;
        Ok(now)
    }
    pub fn set_link(
        &mut self,
        from: NodeId,
        to: NodeId,
        link: Link,
    ) -> Result<(), SimulationError> {
        if !self.endpoints.contains_key(&from) || !self.endpoints.contains_key(&to) {
            return Err(SimulationError::UnknownNode);
        }
        if link.loss_per_mille > 1000 {
            return Err(SimulationError::InvalidConfig);
        }
        self.links.insert((from, to), link);
        Ok(())
    }
    pub fn heal(&mut self) {
        self.links.clear();
    }
    pub fn partition(&mut self, left: &[NodeId], right: &[NodeId]) -> Result<(), SimulationError> {
        if left
            .iter()
            .chain(right)
            .any(|id| !self.endpoints.contains_key(id))
        {
            return Err(SimulationError::UnknownNode);
        }
        if left.iter().any(|id| right.contains(id)) {
            return Err(SimulationError::InvalidConfig);
        }
        for &a in left {
            for &b in right {
                self.set_link(
                    a,
                    b,
                    Link {
                        enabled: false,
                        ..Link::default()
                    },
                )?;
                self.set_link(
                    b,
                    a,
                    Link {
                        enabled: false,
                        ..Link::default()
                    },
                )?;
            }
        }
        Ok(())
    }
    pub fn stop_node(&mut self, id: NodeId) -> Result<(), SimulationError> {
        let endpoint = self
            .endpoints
            .get_mut(&id)
            .ok_or(SimulationError::UnknownNode)?;
        if !endpoint.running {
            return Err(SimulationError::InvalidNodeState);
        }
        endpoint.running = false;
        Ok(())
    }
    pub fn recover_node(&mut self, id: NodeId) -> Result<(), SimulationError> {
        let endpoint = self
            .endpoints
            .get_mut(&id)
            .ok_or(SimulationError::UnknownNode)?;
        if endpoint.running {
            return Err(SimulationError::InvalidNodeState);
        }
        endpoint.incarnation = endpoint
            .incarnation
            .checked_add(1)
            .ok_or(SimulationError::CounterExhausted)?;
        endpoint.running = true;
        Ok(())
    }
    pub fn stats(&self) -> NetworkStats {
        self.stats
    }
    pub fn queued(&self) -> usize {
        self.queue.len()
    }
    pub fn next_delivery(&self) -> Option<Duration> {
        self.queue.first_key_value().map(|((time, _), _)| *time)
    }
    fn sample(&mut self) -> u64 {
        self.random = self.random.wrapping_add(0x9e3779b97f4a7c15);
        let mut z = self.random;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d049bb133111eb);
        z ^ (z >> 31)
    }
}
impl<C: Clock> Transport for InMemoryNetwork<C> {
    fn send(
        &mut self,
        from: NodeId,
        to: NodeId,
        message: ControlMessage,
    ) -> Result<(), SimulationError> {
        let now = self.now()?;
        let source = *self
            .endpoints
            .get(&from)
            .ok_or(SimulationError::UnknownNode)?;
        let target = *self
            .endpoints
            .get(&to)
            .ok_or(SimulationError::UnknownNode)?;
        let link = self.links.get(&(from, to)).copied().unwrap_or_default();
        let deliver_at = plus(now, link.delay)?;
        self.sequence = self
            .sequence
            .checked_add(1)
            .ok_or(SimulationError::CounterExhausted)?;
        self.stats.sent += 1;
        let lost = self.sample() % 1000 < u64::from(link.loss_per_mille);
        if !source.running
            || !target.running
            || !link.enabled
            || lost
            || self.queue.len() >= self.capacity
        {
            self.stats.dropped += 1;
            return Ok(());
        }
        self.queue.insert(
            (deliver_at, self.sequence),
            Envelope {
                sequence: self.sequence,
                from,
                to,
                from_incarnation: source.incarnation,
                to_incarnation: target.incarnation,
                sent_at: now,
                deliver_at,
                message,
            },
        );
        Ok(())
    }
    fn receive(&mut self) -> Result<Option<Envelope>, SimulationError> {
        let now = self.now()?;
        while self.next_delivery().is_some_and(|time| time <= now) {
            let (_, packet) = self.queue.pop_first().expect("due packet exists");
            let source = self.endpoints[&packet.from];
            let target = self.endpoints[&packet.to];
            if !source.running
                || !target.running
                || source.incarnation != packet.from_incarnation
                || target.incarnation != packet.to_incarnation
                || !self
                    .links
                    .get(&(packet.from, packet.to))
                    .copied()
                    .unwrap_or_default()
                    .enabled
            {
                self.stats.dropped += 1;
                continue;
            }
            self.stats.delivered += 1;
            return Ok(Some(packet));
        }
        Ok(None)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Peer {
    pub node: NodeId,
    pub incarnation: u64,
    pub last_sequence: u64,
    pub last_seen: Option<Duration>,
}
#[derive(Clone, Debug)]
pub struct PeerTable {
    peers: BTreeMap<NodeId, Peer>,
    created_at: Duration,
}
impl PeerTable {
    pub fn new(members: &[NodeId], now: Duration) -> Self {
        Self {
            peers: members
                .iter()
                .map(|&node| {
                    (
                        node,
                        Peer {
                            node,
                            incarnation: 0,
                            last_sequence: 0,
                            last_seen: None,
                        },
                    )
                })
                .collect(),
            created_at: now,
        }
    }
    pub fn get(&self, node: NodeId) -> Option<&Peer> {
        self.peers.get(&node)
    }
    pub fn iter(&self) -> impl Iterator<Item = &Peer> {
        self.peers.values()
    }
    pub fn healthy(&self, node: NodeId, now: Duration, timeout: Duration) -> bool {
        self.get(node)
            .and_then(|p| p.last_seen)
            .is_some_and(|at| now.saturating_sub(at) < timeout)
    }
    fn age(&self, node: NodeId, now: Duration) -> Duration {
        now.saturating_sub(
            self.get(node)
                .and_then(|p| p.last_seen)
                .unwrap_or(self.created_at),
        )
    }
    /// Reject stale/replayed heartbeats. A delayed packet cannot resurrect health
    /// indefinitely: it must also be younger than the suspicion threshold.
    pub fn observe(
        &mut self,
        node: NodeId,
        incarnation: u64,
        heartbeat: &Heartbeat,
        now: Duration,
        max_age: Duration,
    ) -> bool {
        let Some(peer) = self.peers.get_mut(&node) else {
            return false;
        };
        if heartbeat.sent_at > now
            || now - heartbeat.sent_at >= max_age
            || incarnation < peer.incarnation
            || (incarnation == peer.incarnation && heartbeat.sequence <= peer.last_sequence)
        {
            return false;
        }
        peer.incarnation = incarnation;
        peer.last_sequence = heartbeat.sequence;
        peer.last_seen = Some(now);
        true
    }
}

struct Pending {
    proposal: OwnershipProposal,
    acks: BTreeSet<NodeId>,
}

/// Test oracle ONLY. Membership never shrinks when a node stops. ACKs attest to
/// one exact proposal, not liveness or votes in a distributed consensus protocol.
/// Serial CAS and non-overlap come from this shared oracle, not ACK counting.
pub struct TestQuorumCoordinator<C> {
    clock: C,
    members: BTreeSet<NodeId>,
    authority: StandaloneCoordinator<C>,
    records: BTreeMap<Key, OwnershipRecord>,
    pending: BTreeMap<u64, Pending>,
    next_id: u64,
}
impl<C: Clock + Clone> TestQuorumCoordinator<C> {
    pub fn new(clock: C, members: &[NodeId]) -> Result<Self, SimulationError> {
        let unique: BTreeSet<_> = members.iter().copied().collect();
        if unique.is_empty() || unique.len() != members.len() {
            return Err(SimulationError::InvalidConfig);
        }
        Ok(Self {
            authority: StandaloneCoordinator::new(clock.clone()),
            clock,
            members: unique,
            records: BTreeMap::new(),
            pending: BTreeMap::new(),
            next_id: 0,
        })
    }
    pub fn majority(&self) -> usize {
        self.members.len() / 2 + 1
    }
    pub fn record(&self, key: OwnershipKey) -> Option<OwnershipRecord> {
        self.records.get(&index(key)).copied()
    }
    fn begin(
        &mut self,
        mut proposal: OwnershipProposal,
    ) -> Result<OwnershipProposal, SimulationError> {
        self.next_id = self
            .next_id
            .checked_add(1)
            .ok_or(SimulationError::CounterExhausted)?;
        proposal.id = self.next_id;
        self.pending.insert(
            proposal.id,
            Pending {
                proposal,
                acks: BTreeSet::new(),
            },
        );
        Ok(proposal)
    }
    fn ack(&mut self, from: NodeId, proposal: OwnershipProposal) -> bool {
        if !self.members.contains(&from) || self.clock.now() >= proposal.ack_deadline {
            return false;
        }
        let Some(pending) = self.pending.get_mut(&proposal.id) else {
            return false;
        };
        if pending.proposal != proposal {
            return false;
        }
        pending.acks.insert(from)
    }
    fn cancel_requester(&mut self, requester: NodeId) {
        self.pending
            .retain(|_, p| p.proposal.requester != requester);
    }
    fn decisions(
        &mut self,
    ) -> Result<Vec<(OwnershipProposal, bool, Option<OwnershipRecord>)>, SimulationError> {
        self.authority.watchdog()?;
        let now = self.clock.now();
        let mut decisions = Vec::new();
        let ids: Vec<_> = self.pending.keys().copied().collect();
        for id in ids {
            let pending = &self.pending[&id];
            let p = pending.proposal;
            let current = self.record(p.key);
            let expired = now >= p.ack_deadline || now >= p.expires_at;
            if !expired && pending.acks.len() < self.majority() {
                continue;
            }
            let matches = current.map_or(OwnershipEpoch(0), |r| r.lease.epoch) == p.expected_epoch;
            let invalid_renew = p.kind == ProposalKind::Renew
                && !current.is_some_and(|r| {
                    r.owner == p.requester && p.target == p.requester && now < r.lease.expires_at
                });
            if expired || !matches || invalid_renew {
                self.pending.remove(&id);
                decisions.push((p, false, current));
                continue;
            }
            // Re-read current deadline, including renewals committed since this
            // request began. Even a stopped owner is conservatively waited out.
            if p.kind != ProposalKind::Renew && current.is_some_and(|r| now < r.lease.expires_at) {
                continue;
            }
            let lease = if p.kind == ProposalKind::Renew {
                self.authority
                    .renew(p.key, p.expected_epoch, p.expires_at - now)?
            } else {
                self.authority.acquire(p.key, p.expires_at - now)?
            };
            let record = OwnershipRecord {
                owner: p.target,
                lease,
            };
            self.records.insert(index(p.key), record);
            self.pending.remove(&id);
            decisions.push((p, true, Some(record)));
        }
        Ok(decisions)
    }
    fn next_deadline(&self, now: Duration) -> Option<Duration> {
        self.pending
            .values()
            .flat_map(|p| {
                [
                    Some(p.proposal.ack_deadline),
                    self.record(p.proposal.key).map(|r| r.lease.expires_at),
                ]
            })
            .flatten()
            .filter(|&at| at > now)
            .min()
    }
}

struct Runtime {
    lease: Lease,
    ready_at: Duration,
    ready: bool,
    last_renewal: Duration,
}
struct Shard<C> {
    policy: Failover<C>,
    waiting_since: Option<Duration>,
    observed: Option<OwnershipRecord>,
    runtime: Option<Runtime>,
    pending: Option<OwnershipProposal>,
}
struct Node<C> {
    peers: PeerTable,
    shards: BTreeMap<Key, Shard<C>>,
    sequence: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TraceEvent {
    Delivered {
        from: NodeId,
        to: NodeId,
        sequence: u64,
    },
    Proposed(OwnershipProposal),
    Ack {
        proposal: u64,
        voter: NodeId,
    },
    Decision {
        proposal: u64,
        granted: bool,
        record: Option<OwnershipRecord>,
    },
    Policy {
        node: NodeId,
        key: OwnershipKey,
        state: State,
    },
    Active {
        node: NodeId,
        lease: Lease,
    },
    Expired {
        node: NodeId,
        lease: Lease,
    },
    Stopped(NodeId),
    Recovered(NodeId),
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TraceEntry {
    pub at: Duration,
    pub event: TraceEvent,
}

/// Stable member/key iteration and per-delivery invariant checks make traces
/// reproducible. Trace storage grows with the scenario; use `drain_trace` between
/// bounded runs. Admissions are checked locally, including at the exact deadline.
pub struct ClusterSimulation<C = ManualClock> {
    clock: C,
    config: SimulationConfig,
    placements: BTreeMap<Key, ShardPlacement>,
    nodes: BTreeMap<NodeId, Node<C>>,
    network: InMemoryNetwork<C>,
    coordinator: TestQuorumCoordinator<C>,
    next_tick: Duration,
    last_now: Duration,
    clock_failed: Cell<bool>,
    last_admission: Cell<Duration>,
    trace: Vec<TraceEntry>,
}

impl ClusterSimulation<ManualClock> {
    pub fn new(
        members: Vec<NodeId>,
        placements: Vec<ShardPlacement>,
        config: SimulationConfig,
        seed: u64,
    ) -> Result<Self, SimulationError> {
        Self::with_clock(members, placements, config, ManualClock::default(), seed)
    }
    /// Advance through every scheduled delivery, poll, startup and lease deadline;
    /// a large advance never skips a transient double-Active interval.
    pub fn advance(&mut self, by: Duration) -> Result<(), SimulationError> {
        let target = plus(self.now(), by)?;
        self.poll()?;
        while self.now() < target {
            let now = self.now();
            let mut next = target.min(self.next_tick);
            if let Some(at) = self.network.next_delivery() {
                next = next.min(at);
            }
            if let Some(at) = self.coordinator.next_deadline(now) {
                next = next.min(at);
            }
            for node in self.nodes.values() {
                for shard in node.shards.values() {
                    if let Some(runtime) = &shard.runtime {
                        if runtime.lease.expires_at > now {
                            next = next.min(runtime.lease.expires_at);
                        }
                        if !runtime.ready && runtime.ready_at > now {
                            next = next.min(runtime.ready_at);
                        }
                    }
                }
            }
            self.clock.set(next)?;
            self.poll()?;
        }
        Ok(())
    }
}

impl<C: Clock + Clone> ClusterSimulation<C> {
    pub fn with_clock(
        members: Vec<NodeId>,
        placements: Vec<ShardPlacement>,
        config: SimulationConfig,
        clock: C,
        seed: u64,
    ) -> Result<Self, SimulationError> {
        Failover::new(clock.clone(), config.failover)?;
        if config.lease_ttl.is_zero()
            || config.renew_every.is_zero()
            || config.renew_every >= config.lease_ttl
            || config.proposal_timeout.is_zero()
            || config.proposal_timeout > config.lease_ttl
            || placements.is_empty()
        {
            return Err(SimulationError::InvalidConfig);
        }
        let network = InMemoryNetwork::new(clock.clone(), members.clone(), seed, 4096)?;
        let coordinator = TestQuorumCoordinator::new(clock.clone(), &members)?;
        let mut plan = BTreeMap::new();
        for placement in placements {
            let mut candidates = BTreeSet::from([placement.primary]);
            if !members.contains(&placement.primary)
                || placement
                    .failover
                    .iter()
                    .any(|n| !members.contains(n) || !candidates.insert(*n))
                || plan.insert(index(placement.key), placement).is_some()
            {
                return Err(SimulationError::InvalidConfig);
            }
        }
        let now = clock.now();
        let mut simulation = Self {
            clock,
            config,
            placements: plan,
            nodes: BTreeMap::new(),
            network,
            coordinator,
            next_tick: now,
            last_now: now,
            clock_failed: Cell::new(false),
            last_admission: Cell::new(now),
            trace: Vec::new(),
        };
        for member in members {
            simulation.reset_node(member)?;
        }
        Ok(simulation)
    }
    fn reset_node(&mut self, id: NodeId) -> Result<(), SimulationError> {
        let members: Vec<_> = self.network.endpoints.keys().copied().collect();
        let mut shards = BTreeMap::new();
        for (&key, placement) in &self.placements {
            let policy = Failover::new(self.clock.clone(), self.config.failover)?;
            shards.insert(
                key,
                Shard {
                    policy,
                    waiting_since: None,
                    observed: None,
                    runtime: None,
                    pending: None,
                },
            );
            debug_assert_eq!(key, index(placement.key));
        }
        self.nodes.insert(
            id,
            Node {
                peers: PeerTable::new(&members, self.now()),
                shards,
                sequence: 0,
            },
        );
        Ok(())
    }
    pub fn now(&self) -> Duration {
        self.clock.now()
    }
    /// Inject link faults. Use the cluster's stop/recover methods for host
    /// lifecycle changes, so policies and runtime gates are reset together.
    pub fn network_mut(&mut self) -> &mut InMemoryNetwork<C> {
        &mut self.network
    }
    pub fn network(&self) -> &InMemoryNetwork<C> {
        &self.network
    }
    pub fn trace(&self) -> &[TraceEntry] {
        &self.trace
    }
    pub fn drain_trace(&mut self) -> Vec<TraceEntry> {
        std::mem::take(&mut self.trace)
    }
    pub fn peer(&self, node: NodeId, peer: NodeId) -> Option<&Peer> {
        self.nodes.get(&node)?.peers.get(peer)
    }
    pub fn ownership(&self, key: OwnershipKey) -> Option<OwnershipRecord> {
        self.coordinator.record(key)
    }
    pub fn policy_state(&self, node: NodeId, key: OwnershipKey) -> Option<State> {
        Some(
            self.nodes
                .get(&node)?
                .shards
                .get(&index(key))?
                .policy
                .state(),
        )
    }
    pub fn local_lease(&self, node: NodeId, key: OwnershipKey) -> Option<Lease> {
        Some(
            self.nodes
                .get(&node)?
                .shards
                .get(&index(key))?
                .runtime
                .as_ref()?
                .lease,
        )
    }
    pub fn check(
        &self,
        node: NodeId,
        key: OwnershipKey,
        epoch: OwnershipEpoch,
    ) -> Result<(), OwnershipError> {
        let now = self.now();
        if self.clock_failed.get() || now < self.last_now || now < self.last_admission.get() {
            self.clock_failed.set(true);
            return Err(OwnershipError::ClockRegressed);
        }
        self.last_admission.set(now);
        if !self.network.endpoints.get(&node).is_some_and(|e| e.running) {
            return Err(OwnershipError::NotOwned);
        }
        let runtime = self
            .nodes
            .get(&node)
            .and_then(|n| n.shards.get(&index(key)))
            .and_then(|s| s.runtime.as_ref())
            .ok_or(OwnershipError::NotOwned)?;
        if runtime.lease.epoch != epoch {
            return Err(OwnershipError::Fenced);
        }
        if self.now() >= runtime.lease.expires_at {
            return Err(OwnershipError::Expired);
        }
        if !runtime.ready {
            return Err(OwnershipError::NotOwned);
        }
        Ok(())
    }
    pub fn active_owners(&self, key: OwnershipKey) -> Vec<NodeId> {
        self.nodes
            .keys()
            .copied()
            .filter(|&id| {
                self.local_lease(id, key)
                    .is_some_and(|l| self.check(id, key, l.epoch).is_ok())
            })
            .collect()
    }
    pub fn assert_single_active(&self) -> Result<(), SimulationError> {
        for p in self.placements.values() {
            if self.active_owners(p.key).len() > 1 {
                return Err(SimulationError::MultipleActive(p.key));
            }
        }
        Ok(())
    }
    fn log(&mut self, event: TraceEvent) {
        self.trace.push(TraceEntry {
            at: self.now(),
            event,
        });
    }
    pub fn partition(&mut self, left: &[NodeId], right: &[NodeId]) -> Result<(), SimulationError> {
        self.network.partition(left, right)
    }
    pub fn heal(&mut self) {
        self.network.heal();
    }
    pub fn stop_node(&mut self, node: NodeId) -> Result<(), SimulationError> {
        self.network.stop_node(node)?;
        self.coordinator.cancel_requester(node);
        for shard in self
            .nodes
            .get_mut(&node)
            .expect("registered node")
            .shards
            .values_mut()
        {
            shard.runtime = None;
            shard.pending = None;
        }
        self.log(TraceEvent::Stopped(node));
        self.assert_single_active()
    }
    pub fn recover_node(&mut self, node: NodeId) -> Result<(), SimulationError> {
        self.network.recover_node(node)?;
        self.reset_node(node)?;
        self.log(TraceEvent::Recovered(node));
        self.assert_single_active()
    }
    fn policy(&mut self, node: NodeId, key: Key, event: Event) -> Result<(), SimulationError> {
        let now = self.now();
        let shard = self
            .nodes
            .get_mut(&node)
            .expect("node")
            .shards
            .get_mut(&key)
            .expect("shard");
        let before = shard.policy.state();
        let after = shard.policy.handle(event)?;
        if after != State::FailbackWaiting {
            shard.waiting_since = None;
        } else if before != State::FailbackWaiting {
            shard.waiting_since = Some(now);
        }
        if before != after {
            self.log(TraceEvent::Policy {
                node,
                key: self.placements[&key].key,
                state: after,
            });
        }
        Ok(())
    }
    fn observe(shard: &mut Shard<C>, record: OwnershipRecord) {
        if shard.observed.is_none_or(|old| {
            record.lease.epoch.0 > old.lease.epoch.0
                || (record.lease.epoch == old.lease.epoch
                    && record.owner == old.owner
                    && record.lease.expires_at > old.lease.expires_at)
        }) {
            shard.observed = Some(record);
        }
    }
    fn quorum_hint(&self, id: NodeId) -> bool {
        1 + self.nodes[&id]
            .peers
            .iter()
            .filter(|p| {
                p.node != id
                    && self.nodes[&id].peers.healthy(
                        p.node,
                        self.now(),
                        self.config.failover.fail_after,
                    )
            })
            .count()
            >= self.coordinator.majority()
    }
    fn healthy(&self, observer: NodeId, peer: NodeId) -> bool {
        observer == peer
            || self.nodes[&observer].peers.healthy(
                peer,
                self.now(),
                self.config.failover.fail_after,
            )
    }
    fn send_all(&mut self, from: NodeId, message: ControlMessage) -> Result<(), SimulationError> {
        let members: Vec<_> = self.nodes.keys().copied().collect();
        for member in members {
            self.network.send(from, member, message.clone())?;
        }
        Ok(())
    }
    fn propose(
        &mut self,
        requester: NodeId,
        key: Key,
        target: NodeId,
        kind: ProposalKind,
    ) -> Result<(), SimulationError> {
        let now = self.now();
        let node = &self.nodes[&requester];
        let shard = &node.shards[&key];
        let target_incarnation = if target == requester {
            self.network.endpoints[&requester].incarnation
        } else {
            node.peers.get(target).map_or(0, |p| p.incarnation)
        };
        let p = self.coordinator.begin(OwnershipProposal {
            id: 0,
            requester,
            requester_incarnation: self.network.endpoints[&requester].incarnation,
            target,
            target_incarnation,
            key: self.placements[&key].key,
            expected_epoch: shard.observed.map_or(OwnershipEpoch(0), |r| r.lease.epoch),
            kind,
            attempt: shard.policy.attempt(),
            requested_at: now,
            expires_at: plus(now, self.config.lease_ttl)?,
            ack_deadline: plus(now, self.config.proposal_timeout)?,
        })?;
        self.nodes
            .get_mut(&requester)
            .expect("node")
            .shards
            .get_mut(&key)
            .expect("shard")
            .pending = Some(p);
        self.log(TraceEvent::Proposed(p));
        self.send_all(requester, ControlMessage::OwnershipProposal(p))
    }
    fn expire_and_start(&mut self) -> Result<(), SimulationError> {
        let now = self.now();
        let mut ready = Vec::new();
        let mut expired = Vec::new();
        for (&node, state) in &mut self.nodes {
            for (&key, shard) in &mut state.shards {
                if let Some(runtime) = &mut shard.runtime {
                    if now >= runtime.lease.expires_at {
                        expired.push((node, runtime.lease));
                        shard.runtime = None;
                    } else if !runtime.ready && now >= runtime.ready_at {
                        ready.push((node, key, runtime.lease));
                    }
                }
            }
        }
        for (node, lease) in expired {
            self.log(TraceEvent::Expired { node, lease });
        }
        for (node, key, lease) in ready {
            if node != self.placements[&key].primary {
                let attempt = self.nodes[&node].shards[&key].policy.attempt();
                self.policy(node, key, Event::StartupComplete { attempt })?;
                if self.nodes[&node].shards[&key].policy.state() != State::TemporaryActive {
                    continue;
                }
            }
            self.nodes
                .get_mut(&node)
                .expect("node")
                .shards
                .get_mut(&key)
                .expect("shard")
                .runtime
                .as_mut()
                .expect("live startup")
                .ready = true;
            self.log(TraceEvent::Active { node, lease });
            self.assert_single_active()?;
        }
        Ok(())
    }
    // The existing policy represents a proxy lifecycle. Reconcile it from an
    // already installed lease after health recovery, before a primary heartbeat
    // could cancel a Pending attempt. This does not grant or extend a lease.
    fn reconcile_proxy(&mut self, id: NodeId, key: Key) -> Result<(), SimulationError> {
        if id == self.placements[&key].primary {
            return Ok(());
        }
        let shard = &self.nodes[&id].shards[&key];
        let Some(runtime) = &shard.runtime else {
            return Ok(());
        };
        if self.now() >= runtime.lease.expires_at {
            return Ok(());
        }
        let ready = runtime.ready;
        if matches!(shard.policy.state(), State::Assigned | State::Suspected) {
            self.policy(id, key, Event::Quorum(false))?;
            self.policy(id, key, Event::Quorum(self.quorum_hint(id)))?;
        }
        let shard = &self.nodes[&id].shards[&key];
        if shard.policy.state() == State::FailoverPending {
            let attempt = shard.policy.attempt();
            self.policy(id, key, Event::OwnershipApproved { attempt })?;
            if ready {
                self.policy(id, key, Event::StartupComplete { attempt })?;
            }
        }
        Ok(())
    }
    fn tick_node(&mut self, id: NodeId) -> Result<(), SimulationError> {
        let now = self.now();
        let node = self.nodes.get_mut(&id).expect("node");
        node.sequence = node
            .sequence
            .checked_add(1)
            .ok_or(SimulationError::CounterExhausted)?;
        let hb = Heartbeat {
            sequence: node.sequence,
            sent_at: now,
            ownership: node.shards.values().filter_map(|s| s.observed).collect(),
        };
        self.send_all(id, ControlMessage::Heartbeat(hb))?;
        let keys: Vec<_> = self.placements.keys().copied().collect();
        for key in keys {
            self.policy(id, key, Event::Quorum(self.quorum_hint(id)))?;
            self.policy(id, key, Event::Tick)?;
            self.reconcile_proxy(id, key)?;
            let shard = self
                .nodes
                .get_mut(&id)
                .expect("node")
                .shards
                .get_mut(&key)
                .expect("shard");
            if shard.pending.is_some_and(|p| now >= p.ack_deadline) {
                shard.pending = None;
            }
            if shard.pending.is_some() {
                continue;
            }
            let plan = &self.placements[&key];
            let shard = &self.nodes[&id].shards[&key];
            let observed_owner = shard.observed.map_or(plan.primary, |r| r.owner);
            let live = shard
                .runtime
                .as_ref()
                .is_some_and(|r| now < r.lease.expires_at);
            let return_ready = id != plan.primary
                && observed_owner == id
                && shard.policy.state() == State::FailbackWaiting
                && shard
                    .waiting_since
                    .is_some_and(|at| now.saturating_sub(at) >= self.config.failover.stable_for)
                && self.healthy(id, plan.primary);
            if return_ready {
                self.propose(id, key, plan.primary, ProposalKind::Failback)?;
            } else if live {
                if shard
                    .runtime
                    .as_ref()
                    .is_some_and(|r| now.saturating_sub(r.last_renewal) >= self.config.renew_every)
                {
                    self.propose(id, key, id, ProposalKind::Renew)?;
                }
            } else if observed_owner == id {
                if id != plan.primary && shard.policy.state() != State::FailoverPending {
                    self.policy(id, key, Event::Quorum(false))?;
                    self.policy(id, key, Event::Quorum(self.quorum_hint(id)))?;
                }
                if id == self.placements[&key].primary
                    || self.nodes[&id].shards[&key].policy.state() == State::FailoverPending
                {
                    self.propose(id, key, id, ProposalKind::Acquire)?;
                }
            } else if self.nodes[&id].peers.age(observed_owner, now)
                >= self.config.failover.fail_after
            {
                let candidate = plan.failover.iter().copied().find(|&n| self.healthy(id, n));
                if candidate == Some(id) && shard.policy.state() == State::FailoverPending {
                    self.propose(id, key, id, ProposalKind::Acquire)?;
                }
            }
        }
        Ok(())
    }
    fn deliver(&mut self, packet: Envelope) -> Result<(), SimulationError> {
        let now = self.now();
        let id = packet.to;
        self.log(TraceEvent::Delivered {
            from: packet.from,
            to: id,
            sequence: packet.sequence,
        });
        match packet.message {
            ControlMessage::Heartbeat(hb) => {
                let old_incarnation = self.nodes[&id]
                    .peers
                    .get(packet.from)
                    .map_or(0, |p| p.incarnation);
                if !self.nodes.get_mut(&id).expect("node").peers.observe(
                    packet.from,
                    packet.from_incarnation,
                    &hb,
                    now,
                    self.config.failover.suspect_after,
                ) {
                    return Ok(());
                }
                for record in hb.ownership {
                    if let Some(shard) = self
                        .nodes
                        .get_mut(&id)
                        .expect("node")
                        .shards
                        .get_mut(&index(record.lease.key))
                    {
                        Self::observe(shard, record);
                    }
                }
                let keys: Vec<_> = self
                    .placements
                    .iter()
                    .filter(|(key, p)| {
                        let shard = &self.nodes[&id].shards[key];
                        // Standbys detect the CURRENT owner, including a proxy.
                        // A proxy holding a lease watches its preferred primary
                        // for failback. Its own heartbeat cannot cancel a retry.
                        let watched = if shard.runtime.is_some() && id != p.primary {
                            p.primary
                        } else {
                            shard.observed.map_or(p.primary, |r| r.owner)
                        };
                        watched == packet.from && (watched != id || id == p.primary)
                    })
                    .map(|(&key, _)| key)
                    .collect();
                for key in keys {
                    if old_incarnation != 0
                        && old_incarnation != packet.from_incarnation
                        && self.nodes[&id].shards[&key].policy.state() == State::FailbackWaiting
                    {
                        self.policy(id, key, Event::Quorum(false))?;
                    }
                    self.policy(id, key, Event::Quorum(self.quorum_hint(id)))?;
                    self.reconcile_proxy(id, key)?;
                    self.policy(id, key, Event::Heartbeat)?;
                }
            }
            ControlMessage::OwnershipProposal(p) => {
                if packet.from != p.requester
                    || packet.from_incarnation != p.requester_incarnation
                    || now >= p.ack_deadline
                    || !self
                        .coordinator
                        .pending
                        .get(&p.id)
                        .is_some_and(|pending| pending.proposal == p)
                {
                    return Ok(());
                }
                self.network.send(
                    id,
                    packet.from,
                    ControlMessage::OwnershipAck { proposal: p },
                )?;
            }
            ControlMessage::OwnershipAck { proposal: p } => {
                if id == p.requester
                    && packet.to_incarnation == p.requester_incarnation
                    && self.coordinator.ack(packet.from, p)
                {
                    self.log(TraceEvent::Ack {
                        proposal: p.id,
                        voter: packet.from,
                    });
                }
            }
            ControlMessage::OwnershipDecision {
                proposal: p,
                granted,
                current,
            } => {
                if packet.from != p.requester || packet.from_incarnation != p.requester_incarnation
                {
                    return Ok(());
                }
                let key = index(p.key);
                let shard = self
                    .nodes
                    .get_mut(&id)
                    .expect("node")
                    .shards
                    .get_mut(&key)
                    .expect("configured proposal");
                if shard.pending.is_some_and(|old| old.id == p.id) {
                    shard.pending = None;
                }
                if let Some(record) = current {
                    Self::observe(shard, record);
                }
                if !granted {
                    return Ok(());
                }
                let record = current.expect("grant has record");
                if id == p.requester && p.kind == ProposalKind::Failback {
                    self.policy(id, key, Event::FailbackComplete { attempt: p.attempt })?;
                }
                let shard = self
                    .nodes
                    .get_mut(&id)
                    .expect("node")
                    .shards
                    .get_mut(&key)
                    .expect("shard");
                if id != p.target
                    || packet.to_incarnation != p.target_incarnation
                    || now >= record.lease.expires_at
                    || shard
                        .observed
                        .is_some_and(|seen| seen.lease.epoch.0 > record.lease.epoch.0)
                    || shard
                        .runtime
                        .as_ref()
                        .is_some_and(|r| r.lease.epoch.0 > record.lease.epoch.0)
                {
                    return Ok(());
                }
                if p.kind == ProposalKind::Renew {
                    if let Some(runtime) = &mut shard.runtime
                        && runtime.lease.epoch == record.lease.epoch
                        && record.lease.expires_at >= runtime.lease.expires_at
                    {
                        runtime.lease = record.lease;
                        runtime.last_renewal = p.requested_at;
                    }
                } else {
                    if id != self.placements[&key].primary
                        && (p.attempt != shard.policy.attempt()
                            || shard.policy.state() != State::FailoverPending)
                    {
                        return Ok(());
                    }
                    shard.runtime = Some(Runtime {
                        lease: record.lease,
                        ready_at: plus(now, self.config.startup_delay)?,
                        ready: false,
                        last_renewal: p.requested_at,
                    });
                    if id != self.placements[&key].primary {
                        self.policy(id, key, Event::OwnershipApproved { attempt: p.attempt })?;
                    }
                }
            }
        }
        Ok(())
    }
    fn flush_decisions(&mut self) -> Result<(), SimulationError> {
        // Cancel a superseded local attempt before the test oracle can commit
        // collected ACKs. In particular, failback stability must still hold
        // after waiting out the old lease, not just when the proposal was sent.
        let now = self.now();
        let cancelled: Vec<_> = self
            .coordinator
            .pending
            .values()
            .filter_map(|pending| {
                let p = pending.proposal;
                let shard = &self.nodes[&p.requester].shards[&index(p.key)];
                let stale = match p.kind {
                    ProposalKind::Failback => {
                        shard.policy.state() != State::FailbackWaiting
                            || shard.policy.attempt() != p.attempt
                            || !shard.waiting_since.is_some_and(|at| {
                                now.saturating_sub(at) >= self.config.failover.stable_for
                            })
                            || !self.nodes[&p.requester].peers.healthy(
                                p.target,
                                now,
                                self.config.failover.suspect_after,
                            )
                            || self.network.endpoints[&p.target].incarnation != p.target_incarnation
                    }
                    ProposalKind::Acquire if p.target != self.placements[&index(p.key)].primary => {
                        shard.policy.state() != State::FailoverPending
                            || shard.policy.attempt() != p.attempt
                    }
                    _ => false,
                };
                stale.then_some(p)
            })
            .collect();
        for proposal in cancelled {
            self.coordinator.pending.remove(&proposal.id);
            let current = self.coordinator.record(proposal.key);
            self.log(TraceEvent::Decision {
                proposal: proposal.id,
                granted: false,
                record: current,
            });
            self.send_all(
                proposal.requester,
                ControlMessage::OwnershipDecision {
                    proposal,
                    granted: false,
                    current,
                },
            )?;
        }
        for (proposal, granted, current) in self.coordinator.decisions()? {
            self.log(TraceEvent::Decision {
                proposal: proposal.id,
                granted,
                record: current,
            });
            self.send_all(
                proposal.requester,
                ControlMessage::OwnershipDecision {
                    proposal,
                    granted,
                    current,
                },
            )?;
        }
        Ok(())
    }
    /// Poll an externally injected clock. Skipped time is not reconstructed;
    /// `advance` on ManualClock should be used for exhaustive event scheduling.
    pub fn poll(&mut self) -> Result<(), SimulationError> {
        let now = self.now();
        if self.clock_failed.get() || now < self.last_now || now < self.last_admission.get() {
            self.clock_failed.set(true);
            return Err(SimulationError::ClockRegressed);
        }
        self.last_now = now;
        self.expire_and_start()?;
        self.flush_decisions()?;
        while let Some(packet) = self.network.receive()? {
            self.deliver(packet)?;
            self.expire_and_start()?;
            self.flush_decisions()?;
            self.assert_single_active()?;
        }
        if now >= self.next_tick {
            self.next_tick = plus(now, self.config.failover.heartbeat_interval)?;
            let members: Vec<_> = self.nodes.keys().copied().collect();
            for node in members {
                if self.network.endpoints[&node].running {
                    self.tick_node(node)?;
                }
                self.assert_single_active()?;
            }
            self.flush_decisions()?;
            while let Some(packet) = self.network.receive()? {
                self.deliver(packet)?;
                self.expire_and_start()?;
                self.flush_decisions()?;
                self.assert_single_active()?;
            }
        }
        self.assert_single_active()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::BotId;

    const A: NodeId = NodeId(1);
    const B: NodeId = NodeId(2);
    const C: NodeId = NodeId(3);
    fn key() -> OwnershipKey {
        OwnershipKey {
            bot_id: BotId(7),
            shard_id: 0,
        }
    }
    fn hb(sequence: u64, sent_at: Duration) -> ControlMessage {
        ControlMessage::Heartbeat(Heartbeat {
            sequence,
            sent_at,
            ownership: vec![],
        })
    }
    fn proposal(owner: NodeId, epoch: u64, kind: ProposalKind, now: u64) -> OwnershipProposal {
        OwnershipProposal {
            id: 0,
            requester: owner,
            requester_incarnation: 1,
            target: owner,
            target_incarnation: 1,
            key: key(),
            expected_epoch: OwnershipEpoch(epoch),
            kind,
            attempt: 1,
            requested_at: Duration::from_secs(now),
            expires_at: Duration::from_secs(now + 4),
            ack_deadline: Duration::from_secs(now + 3),
        }
    }
    fn quorum(clock: &ManualClock) -> TestQuorumCoordinator<ManualClock> {
        TestQuorumCoordinator::new(clock.clone(), &[A, B, C]).unwrap()
    }
    fn approve(q: &mut TestQuorumCoordinator<ManualClock>, p: OwnershipProposal) {
        assert!(q.ack(A, p));
        assert!(q.ack(B, p));
    }
    fn one_shard(config: SimulationConfig) -> ClusterSimulation {
        ClusterSimulation::new(
            vec![A, B, C],
            vec![ShardPlacement {
                key: key(),
                primary: A,
                failover: vec![B, C],
            }],
            config,
            42,
        )
        .unwrap()
    }

    #[test]
    fn transport_has_exact_delivery_boundaries_and_fifo_ties() {
        let clock = ManualClock::default();
        let mut net = InMemoryNetwork::new(clock.clone(), vec![A, B], 17, 4).unwrap();
        net.set_link(
            A,
            B,
            Link {
                delay: Duration::from_millis(100),
                ..Link::default()
            },
        )
        .unwrap();
        net.send(A, B, hb(1, clock.now())).unwrap();
        net.send(A, B, hb(2, clock.now())).unwrap();
        clock.set(Duration::from_millis(99)).unwrap();
        assert!(net.receive().unwrap().is_none());
        clock.set(Duration::from_millis(100)).unwrap();
        let first = net.receive().unwrap().unwrap();
        let second = net.receive().unwrap().unwrap();
        assert!(first.sequence < second.sequence);
        assert_eq!(first.deliver_at, clock.now());
        assert!(net.receive().unwrap().is_none());
        assert_eq!(
            net.stats(),
            NetworkStats {
                sent: 2,
                delivered: 2,
                dropped: 0
            }
        );
    }

    #[test]
    fn transport_queue_loss_and_inflight_partition_are_explicit_drops() {
        let clock = ManualClock::default();
        let mut net = InMemoryNetwork::new(clock.clone(), vec![A, B], 17, 1).unwrap();
        net.set_link(
            A,
            B,
            Link {
                delay: Duration::from_secs(1),
                ..Link::default()
            },
        )
        .unwrap();
        net.send(A, B, hb(1, clock.now())).unwrap();
        net.send(A, B, hb(2, clock.now())).unwrap(); // bounded queue
        assert_eq!(net.queued(), 1);
        net.partition(&[A], &[B]).unwrap();
        clock.advance(Duration::from_secs(1)).unwrap();
        assert!(net.receive().unwrap().is_none());
        net.heal();
        net.set_link(
            A,
            B,
            Link {
                loss_per_mille: 1000,
                ..Link::default()
            },
        )
        .unwrap();
        net.send(A, B, hb(3, clock.now())).unwrap();
        assert_eq!(net.stats().dropped, 3);
        net.heal();
        net.send(A, B, hb(4, clock.now())).unwrap();
        assert!(net.receive().unwrap().is_some());
    }

    #[test]
    fn stopped_or_restarted_endpoints_drop_old_inflight_messages() {
        for endpoint in [A, B] {
            let clock = ManualClock::default();
            let mut net = InMemoryNetwork::new(clock.clone(), vec![A, B], 17, 4).unwrap();
            net.set_link(
                A,
                B,
                Link {
                    delay: Duration::from_secs(1),
                    ..Link::default()
                },
            )
            .unwrap();
            net.send(A, B, hb(1, clock.now())).unwrap();
            net.stop_node(endpoint).unwrap();
            net.recover_node(endpoint).unwrap();
            clock.advance(Duration::from_secs(1)).unwrap();
            assert!(net.receive().unwrap().is_none());
            net.send(A, B, hb(2, clock.now())).unwrap();
            clock.advance(Duration::from_secs(1)).unwrap();
            assert!(net.receive().unwrap().is_some());
            assert_eq!(net.stats().dropped, 1);
        }
    }

    #[test]
    fn seeded_partial_loss_is_reproducible_and_not_all_or_nothing() {
        fn run(seed: u64) -> Vec<u64> {
            let clock = ManualClock::default();
            let mut net = InMemoryNetwork::new(clock.clone(), vec![A, B], seed, 200).unwrap();
            net.set_link(
                A,
                B,
                Link {
                    loss_per_mille: 500,
                    ..Link::default()
                },
            )
            .unwrap();
            for sequence in 0..100 {
                net.send(A, B, hb(sequence, clock.now())).unwrap();
            }
            let mut delivered = vec![];
            while let Some(p) = net.receive().unwrap() {
                delivered.push(p.sequence);
            }
            delivered
        }
        let first = run(99);
        assert_eq!(first, run(99));
        assert_ne!(first, run(100));
        assert!(first.len() > 20 && first.len() < 80);
    }

    #[test]
    fn malformed_network_configuration_and_overflow_are_rejected() {
        let clock = ManualClock::default();
        assert!(InMemoryNetwork::new(clock.clone(), vec![A, A], 0, 1).is_err());
        assert!(InMemoryNetwork::new(clock.clone(), vec![A], 0, 0).is_err());
        let mut net = InMemoryNetwork::new(clock.clone(), vec![A, B], 0, 4).unwrap();
        assert_eq!(
            net.set_link(A, C, Link::default()),
            Err(SimulationError::UnknownNode)
        );
        assert_eq!(
            net.set_link(
                A,
                B,
                Link {
                    loss_per_mille: 1001,
                    ..Link::default()
                }
            ),
            Err(SimulationError::InvalidConfig)
        );
        assert_eq!(
            net.partition(&[A], &[A, B]),
            Err(SimulationError::InvalidConfig)
        );
        assert_eq!(
            net.send(A, C, hb(0, clock.now())),
            Err(SimulationError::UnknownNode)
        );
        clock.set(Duration::MAX).unwrap();
        assert_eq!(
            clock.advance(Duration::from_nanos(1)),
            Err(SimulationError::TimeOverflow)
        );
        assert_eq!(
            clock.set(Duration::ZERO),
            Err(SimulationError::ClockRegressed)
        );
        net.set_link(
            A,
            B,
            Link {
                delay: Duration::from_nanos(1),
                ..Link::default()
            },
        )
        .unwrap();
        assert_eq!(
            net.send(A, B, hb(1, clock.now())),
            Err(SimulationError::TimeOverflow)
        );
        assert_eq!(net.queued(), 0);
    }

    #[test]
    fn peer_table_rejects_old_sequence_incarnation_future_and_stale_heartbeats() {
        let mut peers = PeerTable::new(&[A, B], Duration::ZERO);
        let heartbeat = Heartbeat {
            sequence: 5,
            sent_at: Duration::ZERO,
            ownership: vec![],
        };
        let max_age = Duration::from_millis(1500);
        assert!(peers.observe(A, 2, &heartbeat, Duration::ZERO, max_age));
        assert!(!peers.observe(A, 2, &heartbeat, Duration::ZERO, max_age));
        assert!(!peers.observe(
            A,
            1,
            &Heartbeat {
                sequence: 6,
                ..heartbeat.clone()
            },
            Duration::ZERO,
            max_age
        ));
        assert!(!peers.observe(C, 1, &heartbeat, Duration::ZERO, max_age));
        assert!(!peers.observe(B, 1, &heartbeat, max_age, max_age));
        assert!(!peers.observe(
            B,
            1,
            &Heartbeat {
                sent_at: max_age,
                ..heartbeat.clone()
            },
            Duration::ZERO,
            max_age
        ));
        assert!(peers.observe(
            A,
            3,
            &Heartbeat {
                sequence: 1,
                ..heartbeat
            },
            Duration::ZERO,
            max_age
        ));
        assert!(!peers.healthy(A, max_age, max_age));
    }

    #[test]
    fn acknowledgements_are_distinct_members_bound_to_the_exact_proposal() {
        let clock = ManualClock::default();
        let mut q = quorum(&clock);
        let p = q.begin(proposal(A, 0, ProposalKind::Acquire, 0)).unwrap();
        assert!(q.ack(A, p));
        assert!(!q.ack(A, p));
        assert!(!q.ack(NodeId(99), p));
        for altered in [
            OwnershipProposal { target: B, ..p },
            OwnershipProposal { id: p.id + 1, ..p },
            OwnershipProposal {
                requester_incarnation: 2,
                ..p
            },
            OwnershipProposal {
                expected_epoch: OwnershipEpoch(1),
                ..p
            },
            OwnershipProposal {
                key: OwnershipKey {
                    bot_id: BotId(8),
                    ..key()
                },
                ..p
            },
            OwnershipProposal {
                expires_at: p.expires_at + Duration::from_secs(1),
                ..p
            },
        ] {
            assert!(!q.ack(B, altered));
        }
        assert!(q.decisions().unwrap().is_empty());
        assert!(q.ack(B, p));
        let decisions = q.decisions().unwrap();
        assert_eq!(decisions.len(), 1);
        assert!(decisions[0].1);
        assert_eq!(q.record(key()).unwrap().lease.epoch, OwnershipEpoch(1));
        assert!(!q.ack(C, p)); // consumed certificate cannot be replayed
    }

    #[test]
    fn delayed_ack_does_not_restart_ttl_and_exact_deadline_is_rejected() {
        let clock = ManualClock::default();
        let mut q = quorum(&clock);
        let p = q.begin(proposal(A, 0, ProposalKind::Acquire, 0)).unwrap();
        clock.set(Duration::from_secs(2)).unwrap();
        approve(&mut q, p);
        assert!(q.decisions().unwrap()[0].1);
        assert_eq!(
            q.record(key()).unwrap().lease.expires_at,
            Duration::from_secs(4)
        );
        let renew = q.begin(proposal(A, 1, ProposalKind::Renew, 2)).unwrap();
        clock.set(renew.ack_deadline).unwrap();
        assert!(!q.ack(A, renew));
        assert!(!q.decisions().unwrap()[0].1);
        assert_eq!(
            q.record(key()).unwrap().lease.expires_at,
            Duration::from_secs(4)
        );
    }

    #[test]
    fn transfer_waits_for_renewed_deadline_and_competing_cas_has_one_winner() {
        let clock = ManualClock::default();
        let mut q = quorum(&clock);
        let first = q.begin(proposal(A, 0, ProposalKind::Acquire, 0)).unwrap();
        approve(&mut q, first);
        q.decisions().unwrap();
        clock.set(Duration::from_secs(1)).unwrap();
        let mut request = proposal(B, 1, ProposalKind::Acquire, 1);
        request.expires_at = Duration::from_secs(12);
        request.ack_deadline = Duration::from_secs(10);
        let takeover = q.begin(request).unwrap();
        approve(&mut q, takeover);
        assert!(q.decisions().unwrap().is_empty());
        let renew = q
            .begin(OwnershipProposal {
                expires_at: Duration::from_secs(8),
                ..proposal(A, 1, ProposalKind::Renew, 1)
            })
            .unwrap();
        approve(&mut q, renew);
        assert_eq!(q.decisions().unwrap().len(), 1);
        let rival = q
            .begin(OwnershipProposal {
                requester: C,
                target: C,
                ..request
            })
            .unwrap();
        approve(&mut q, rival);
        clock.set(Duration::from_secs(4)).unwrap();
        assert!(q.decisions().unwrap().is_empty()); // initial deadline is not enough
        clock.set(Duration::from_secs(8)).unwrap();
        let results = q.decisions().unwrap();
        assert_eq!(results.iter().filter(|(_, granted, _)| *granted).count(), 1);
        assert_eq!(q.record(key()).unwrap().owner, B);
        assert_eq!(q.record(key()).unwrap().lease.epoch, OwnershipEpoch(2));
        assert_eq!(
            q.authority.check(key(), OwnershipEpoch(1)),
            Err(OwnershipError::Fenced)
        );
    }

    #[test]
    fn expired_lease_cannot_be_resurrected_by_delayed_renewal() {
        let clock = ManualClock::default();
        let mut q = quorum(&clock);
        let p = q.begin(proposal(A, 0, ProposalKind::Acquire, 0)).unwrap();
        approve(&mut q, p);
        q.decisions().unwrap();
        clock.set(Duration::from_secs(3)).unwrap();
        let renew = q.begin(proposal(A, 1, ProposalKind::Renew, 3)).unwrap();
        clock.set(Duration::from_secs(4)).unwrap();
        approve(&mut q, renew);
        assert!(!q.decisions().unwrap()[0].1);
        assert_eq!(
            q.record(key()).unwrap().lease.expires_at,
            Duration::from_secs(4)
        );
    }

    #[test]
    fn pending_proxy_startup_cannot_ignore_quorum_loss() {
        let mut sim = one_shard(SimulationConfig {
            startup_delay: Duration::from_secs(2),
            ..SimulationConfig::default()
        });
        sim.advance(Duration::from_secs(3)).unwrap();
        sim.stop_node(A).unwrap();
        for _ in 0..100 {
            if sim.policy_state(B, key()) == Some(State::StandbyReady) {
                break;
            }
            sim.advance(Duration::from_millis(100)).unwrap();
        }
        assert_eq!(sim.policy_state(B, key()), Some(State::StandbyReady));
        assert!(sim.active_owners(key()).is_empty());
        // Directly supply the state machine's health input at the completion
        // boundary to isolate the rejected-StartupComplete regression.
        sim.policy(B, index(key()), Event::Quorum(false)).unwrap();
        let ready_at = sim.nodes[&B].shards[&index(key())]
            .runtime
            .as_ref()
            .unwrap()
            .ready_at;
        sim.clock.set(ready_at).unwrap();
        sim.expire_and_start().unwrap();
        assert_eq!(sim.policy_state(B, key()), Some(State::Degraded));
        assert!(sim.active_owners(key()).is_empty());
    }

    #[derive(Clone, Default)]
    struct RegressingClock(Arc<Mutex<Duration>>);
    impl Clock for RegressingClock {
        fn now(&self) -> Duration {
            *self.0.lock().unwrap()
        }
    }
    #[test]
    fn injected_clock_regression_is_sticky_even_between_admission_checks() {
        let clock = RegressingClock::default();
        let mut sim = ClusterSimulation::with_clock(
            vec![A, B, C],
            vec![ShardPlacement {
                key: key(),
                primary: A,
                failover: vec![B, C],
            }],
            SimulationConfig::default(),
            clock.clone(),
            0,
        )
        .unwrap();
        sim.poll().unwrap();
        let lease = sim.local_lease(A, key()).unwrap();
        *clock.0.lock().unwrap() = Duration::from_millis(100);
        assert_eq!(sim.check(A, key(), lease.epoch), Ok(()));
        *clock.0.lock().unwrap() = Duration::from_millis(99);
        assert_eq!(
            sim.check(A, key(), lease.epoch),
            Err(OwnershipError::ClockRegressed)
        );
        *clock.0.lock().unwrap() = Duration::from_millis(200);
        assert_eq!(
            sim.check(A, key(), lease.epoch),
            Err(OwnershipError::ClockRegressed)
        );
        assert_eq!(sim.poll(), Err(SimulationError::ClockRegressed));
        assert!(sim.active_owners(key()).is_empty());
    }
}
