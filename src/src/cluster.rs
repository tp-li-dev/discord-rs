//! Control-plane **test fixtures**, not a production consensus backend.
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
//!
//! `TestOnlyQuorumCoordinator` is the separate, explicitly gated loopback process
//! fixture: each node counts authenticated ACKs and holds exclusive voter leases.
//! It has no shared standalone oracle. Its local-clock, bounded-stop and reboot
//! quarantine assumptions are documented on the type and are not Raft guarantees.

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
    /// Experimental loopback process tests only; never a production consensus API.
    TestOnly(TestOnlyMessage),
}

/// Canonical, non-secret deployment description supplied by the runtime. Every
/// test message binds membership, placement and timing to this exact string.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TestOnlyMessage {
    pub configuration: String,
    pub payload: TestOnlyPayload,
}

/// No timestamps cross the transport: each participant uses its own monotonic
/// clock. These fields describe test claims, not remotely usable lease grants.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TestOnlyClaim {
    pub key: OwnershipKey,
    pub owner: NodeId,
    pub incarnation: u64,
    pub epoch: OwnershipEpoch,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TestOnlyRequest {
    pub id: u64,
    pub key: OwnershipKey,
    pub requester: NodeId,
    pub incarnation: u64,
    pub epoch: OwnershipEpoch,
    pub renew: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TestOnlyPayload {
    Heartbeat {
        sequence: u64,
        ownership: Vec<TestOnlyClaim>,
    },
    Request(TestOnlyRequest),
    Vote {
        request: TestOnlyRequest,
        granted: bool,
        highest_epoch: OwnershipEpoch,
    },
}

/// Test-only fixed-membership lease experiment. There is no election, log,
/// persistence, membership change, Byzantine tolerance or production consensus.
/// The caller must separately require its explicit unsafe-test startup flag,
/// loopback mTLS and mock Discord, and enforce local process-stop deadlines.
pub struct TestOnlyQuorumConfig {
    pub node: NodeId,
    pub incarnation: u64,
    pub members: Vec<NodeId>,
    pub keys: Vec<OwnershipKey>,
    pub configuration: String,
    pub vote_ttl: Duration,
    /// The requester stops before voters may approve another owner. Includes
    /// watchdog scheduling and process termination allowance in the test model.
    pub stop_margin: Duration,
}

struct TestOnlyHold {
    request: TestOnlyRequest,
    deadline: Duration,
}

struct TestOnlyPending {
    request: TestOnlyRequest,
    deadline: Duration,
    ack_deadline: Duration,
    acks: BTreeSet<NodeId>,
}

/// Counts exact, authenticated ACKs from an immutable majority. Unlike the older
/// simulation oracle this does not delegate authority to StandaloneCoordinator.
/// Each voter holds an exclusive (node, incarnation, epoch) grant for vote_ttl;
/// the requester uses a shorter deadline fixed BEFORE sending any request. ACK
/// delay never extends it. Duplicate votes count once and duplicate requests do
/// not extend a voter's hold. A fresh process cannot vote for one full vote_ttl.
///
/// These are test-fixture assumptions, NOT a general distributed safety proof:
/// clocks advance at the same rate, the host is not suspended, all participants
/// follow the protocol, and the independent process watchdog stops in the margin.
/// In particular epochs are not durable across loss of all participants.
pub struct TestOnlyQuorumCoordinator<C> {
    clock: C,
    config: TestOnlyQuorumConfig,
    members: BTreeSet<NodeId>,
    keys: BTreeSet<Key>,
    quarantine_until: Duration,
    last_now: Duration,
    clock_failed: bool,
    sequence: u64,
    highest: BTreeMap<Key, u64>,
    holds: BTreeMap<Key, TestOnlyHold>,
    pending: BTreeMap<Key, TestOnlyPending>,
    leases: BTreeMap<Key, Lease>,
}

impl<C: Clock> TestOnlyQuorumCoordinator<C> {
    pub fn new(clock: C, config: TestOnlyQuorumConfig) -> Result<Self, SimulationError> {
        let members: BTreeSet<_> = config.members.iter().copied().collect();
        let keys: BTreeSet<_> = config.keys.iter().copied().map(index).collect();
        if members.len() < 3
            || members.len() > 31
            || members.len() != config.members.len()
            || !members.contains(&config.node)
            || config.incarnation == 0
            || keys.is_empty()
            || keys.len() > 4096
            || keys.len() != config.keys.len()
            || config.configuration.is_empty()
            || config.configuration.len() > 64 * 1024
            || config.vote_ttl.is_zero()
            || config.vote_ttl > Duration::from_secs(300)
            || config.stop_margin.is_zero()
            || config.stop_margin >= config.vote_ttl
        {
            return Err(SimulationError::InvalidConfig);
        }
        let now = clock.now();
        let quarantine_until = plus(now, config.vote_ttl)?;
        Ok(Self {
            clock,
            config,
            members,
            keys,
            quarantine_until,
            last_now: now,
            clock_failed: false,
            sequence: 0,
            highest: BTreeMap::new(),
            holds: BTreeMap::new(),
            pending: BTreeMap::new(),
            leases: BTreeMap::new(),
        })
    }

    fn now(&mut self) -> Result<Duration, OwnershipError> {
        let now = self.clock.now();
        if self.clock_failed || now < self.last_now {
            self.clock_failed = true;
            self.leases.clear();
            self.pending.clear();
            return Err(OwnershipError::ClockRegressed);
        }
        self.last_now = now;
        Ok(now)
    }

    pub fn majority(&self) -> usize {
        self.members.len() / 2 + 1
    }

    pub fn local_lease(&self, key: OwnershipKey) -> Option<Lease> {
        let now = self.clock.now();
        if self.clock_failed || now < self.last_now {
            return None;
        }
        self.leases
            .get(&index(key))
            .filter(|lease| now < lease.expires_at)
            .copied()
    }

    pub fn has_pending(&self, key: OwnershipKey) -> bool {
        let now = self.clock.now();
        !self.clock_failed
            && now >= self.last_now
            && self
                .pending
                .get(&index(key))
                .is_some_and(|p| now < p.ack_deadline)
    }

    /// Revokes local admission and cancels pending ACK aggregation. Voter holds
    /// deliberately survive; neither an unauthenticated release nor a local
    /// process-stop claim may shorten another participant's exclusion deadline.
    pub fn cancel(&mut self, key: OwnershipKey) {
        self.pending.remove(&index(key));
        self.leases.remove(&index(key));
    }

    pub fn heartbeat(&self, sequence: u64) -> TestOnlyMessage {
        TestOnlyMessage {
            configuration: self.config.configuration.clone(),
            payload: TestOnlyPayload::Heartbeat {
                sequence,
                ownership: self
                    .config
                    .keys
                    .iter()
                    .filter_map(|&key| self.local_lease(key))
                    .map(|lease| TestOnlyClaim {
                        key: lease.key,
                        owner: self.config.node,
                        incarnation: self.config.incarnation,
                        epoch: lease.epoch,
                    })
                    .collect(),
            },
        }
    }

    /// Starts one exact request per key. The caller transports it to every peer;
    /// local ACK processing obeys the same quarantine/hold rules as remote ACKs.
    pub fn begin(
        &mut self,
        key: OwnershipKey,
        renew: bool,
    ) -> Result<TestOnlyMessage, SimulationError> {
        let now = self.now()?;
        let idx = index(key);
        if !self.keys.contains(&idx) || self.has_pending(key) {
            return Err(SimulationError::InvalidConfig);
        }
        let epoch = if renew {
            self.local_lease(key).ok_or(OwnershipError::Expired)?.epoch
        } else {
            // A live local grant must be renewed or cancelled explicitly.
            if self.local_lease(key).is_some() {
                return Err(OwnershipError::Fenced.into());
            }
            let next = self
                .highest
                .get(&idx)
                .copied()
                .unwrap_or(0)
                .checked_add(1)
                .ok_or(OwnershipError::EpochExhausted)?;
            self.highest.insert(idx, next);
            OwnershipEpoch(next)
        };
        self.sequence = self
            .sequence
            .checked_add(1)
            .ok_or(SimulationError::CounterExhausted)?;
        let request = TestOnlyRequest {
            id: self.sequence,
            key,
            requester: self.config.node,
            incarnation: self.config.incarnation,
            epoch,
            renew,
        };
        let deadline = plus(now, self.config.vote_ttl - self.config.stop_margin)?;
        let ack_deadline = if renew {
            deadline.min(
                self.local_lease(key)
                    .ok_or(OwnershipError::Expired)?
                    .expires_at,
            )
        } else {
            deadline
        };
        self.pending.insert(
            idx,
            TestOnlyPending {
                request,
                deadline,
                ack_deadline,
                acks: BTreeSet::new(),
            },
        );
        let vote = self.vote(request, now)?;
        self.record_vote(self.config.node, request, vote.0, vote.1, now);
        Ok(TestOnlyMessage {
            configuration: self.config.configuration.clone(),
            payload: TestOnlyPayload::Request(request),
        })
    }

    fn vote(
        &mut self,
        request: TestOnlyRequest,
        now: Duration,
    ) -> Result<(bool, OwnershipEpoch), SimulationError> {
        let idx = index(request.key);
        let highest = self.highest.get(&idx).copied().unwrap_or(0);
        let mut granted = false;
        if now >= self.quarantine_until {
            if let Some(hold) = self.holds.get_mut(&idx).filter(|hold| now < hold.deadline) {
                if hold.request == request {
                    // Retransmission is idempotent and cannot extend exclusion.
                    granted = true;
                } else if request.renew
                    && request.requester == hold.request.requester
                    && request.incarnation == hold.request.incarnation
                    && request.epoch == hold.request.epoch
                    && request.id > hold.request.id
                {
                    hold.deadline = plus(now, self.config.vote_ttl)?;
                    hold.request = request;
                    granted = true;
                }
            } else if !request.renew
                && (request.epoch.0 > highest
                    // begin reserves its own epoch before calling the voter.
                    || (request.requester == self.config.node
                        && request.incarnation == self.config.incarnation
                        && request.epoch.0 == highest
                        && self.pending.get(&idx).is_some_and(|p| p.request == request)))
            {
                self.holds.insert(
                    idx,
                    TestOnlyHold {
                        request,
                        deadline: plus(now, self.config.vote_ttl)?,
                    },
                );
                self.highest.insert(idx, request.epoch.0);
                granted = true;
            }
        }
        Ok((
            granted,
            OwnershipEpoch(self.highest.get(&idx).copied().unwrap_or(0)),
        ))
    }

    fn record_vote(
        &mut self,
        from: NodeId,
        request: TestOnlyRequest,
        granted: bool,
        highest_epoch: OwnershipEpoch,
        now: Duration,
    ) {
        let idx = index(request.key);
        let majority = self.majority();
        let Some(pending) = self.pending.get_mut(&idx) else {
            return;
        };
        if pending.request != request || now >= pending.ack_deadline {
            return;
        }
        self.highest
            .entry(idx)
            .and_modify(|epoch| *epoch = (*epoch).max(highest_epoch.0))
            .or_insert(highest_epoch.0);
        if granted {
            pending.acks.insert(from);
        }
        if pending.acks.len() >= majority {
            let lease = Lease {
                key: request.key,
                epoch: request.epoch,
                expires_at: pending.deadline,
            };
            self.pending.remove(&idx);
            self.leases.insert(idx, lease);
        }
    }

    /// `from` and `incarnation` must come from the mutually authenticated
    /// transport envelope, never from an unauthenticated request body.
    pub fn receive(
        &mut self,
        from: NodeId,
        incarnation: u64,
        message: &TestOnlyMessage,
    ) -> Result<Option<TestOnlyMessage>, SimulationError> {
        let now = self.now()?;
        if !self.members.contains(&from)
            || incarnation == 0
            || message.configuration != self.config.configuration
        {
            return Ok(None);
        }
        match message.payload {
            TestOnlyPayload::Heartbeat { .. } => Ok(None),
            TestOnlyPayload::Request(request) => {
                if request.requester != from
                    || request.incarnation != incarnation
                    || request.id == 0
                    || request.epoch.0 == 0
                    || !self.keys.contains(&index(request.key))
                {
                    return Ok(None);
                }
                let (granted, highest_epoch) = self.vote(request, now)?;
                Ok(Some(TestOnlyMessage {
                    configuration: self.config.configuration.clone(),
                    payload: TestOnlyPayload::Vote {
                        request,
                        granted,
                        highest_epoch,
                    },
                }))
            }
            TestOnlyPayload::Vote {
                request,
                granted,
                highest_epoch,
            } => {
                if request.requester == self.config.node
                    && request.incarnation == self.config.incarnation
                {
                    self.record_vote(from, request, granted, highest_epoch, now);
                }
                Ok(None)
            }
        }
    }
}

impl<C: Clock> OwnershipCoordinator for TestOnlyQuorumCoordinator<C> {
    /// Synchronous callers cannot create authority. Use begin/receive and inspect
    /// the approved local lease; this prevents accidental standalone fallback.
    fn acquire(&mut self, _: OwnershipKey, _: Duration) -> Result<Lease, OwnershipError> {
        Err(OwnershipError::QuorumUnavailable)
    }
    fn renew(
        &mut self,
        _: OwnershipKey,
        _: OwnershipEpoch,
        _: Duration,
    ) -> Result<Lease, OwnershipError> {
        Err(OwnershipError::QuorumUnavailable)
    }
    fn release(&mut self, key: OwnershipKey, epoch: OwnershipEpoch) -> Result<(), OwnershipError> {
        self.check(key, epoch)?;
        self.cancel(key);
        Ok(())
    }
    fn check(&mut self, key: OwnershipKey, epoch: OwnershipEpoch) -> Result<(), OwnershipError> {
        let now = self.now()?;
        let lease = self
            .leases
            .get(&index(key))
            .ok_or(OwnershipError::NotOwned)?;
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
        self.pending.retain(|_, p| now < p.ack_deadline);
        let expired: Vec<_> = self
            .leases
            .values()
            .filter(|lease| now >= lease.expires_at)
            .map(|lease| lease.key)
            .collect();
        for key in &expired {
            self.leases.remove(&index(*key));
        }
        Ok(expired)
    }
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
            // The deterministic oracle and the real-process lease fixture are
            // intentionally separate protocols; neither upgrades the other.
            ControlMessage::TestOnly(_) => {}
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
mod test_only_quorum_tests {
    use super::*;
    use crate::protocol::BotId;

    const KEY: OwnershipKey = OwnershipKey {
        bot_id: BotId(101),
        shard_id: 0,
    };
    const OTHER: OwnershipKey = OwnershipKey {
        bot_id: BotId(202),
        shard_id: 0,
    };
    type Coordinator = TestOnlyQuorumCoordinator<ManualClock>;

    fn config(node: u64, incarnation: u64) -> TestOnlyQuorumConfig {
        TestOnlyQuorumConfig {
            node: NodeId(node),
            incarnation,
            members: vec![NodeId(1), NodeId(2), NodeId(3)],
            keys: vec![KEY, OTHER],
            configuration: "test-only|members=1,2,3|ttl=1000|margin=200".into(),
            vote_ttl: Duration::from_millis(1000),
            stop_margin: Duration::from_millis(200),
        }
    }

    struct World {
        clock: ManualClock,
        nodes: Vec<Coordinator>,
    }
    impl World {
        fn cold() -> Self {
            let clock = ManualClock::default();
            Self {
                nodes: (1..=3)
                    .map(|id| Coordinator::new(clock.clone(), config(id, 100 + id)).unwrap())
                    .collect(),
                clock,
            }
        }
        fn new() -> Self {
            let world = Self::cold();
            world.advance(1000);
            world
        }
        fn advance(&self, millis: u64) {
            self.clock.advance(Duration::from_millis(millis)).unwrap();
        }
        fn request(
            &mut self,
            from: usize,
            to: usize,
            message: &TestOnlyMessage,
        ) -> TestOnlyMessage {
            let id = self.nodes[from].config.node;
            let incarnation = self.nodes[from].config.incarnation;
            self.nodes[to]
                .receive(id, incarnation, message)
                .unwrap()
                .unwrap()
        }
        fn ack(&mut self, from: usize, to: usize, message: &TestOnlyMessage) {
            let id = self.nodes[from].config.node;
            let incarnation = self.nodes[from].config.incarnation;
            assert!(
                self.nodes[to]
                    .receive(id, incarnation, message)
                    .unwrap()
                    .is_none()
            );
        }
        fn exchange(&mut self, from: usize, to: usize, message: &TestOnlyMessage) {
            let reply = self.request(from, to, message);
            self.ack(to, from, &reply);
        }
        fn grant(&mut self, node: usize, key: OwnershipKey) -> Lease {
            let request = self.nodes[node].begin(key, false).unwrap();
            self.exchange(node, (node + 1) % 3, &request);
            self.nodes[node].local_lease(key).unwrap()
        }
    }

    fn granted(message: &TestOnlyMessage) -> bool {
        matches!(message.payload, TestOnlyPayload::Vote { granted: true, .. })
    }

    #[test]
    fn test_only_requires_distinct_fixed_members_known_keys_and_valid_timing() {
        for change in 0..10 {
            let mut c = config(1, 101);
            match change {
                0 => c.members.pop().map(|_| ()).unwrap(),
                1 => c.members[1] = NodeId(1),
                2 => c.node = NodeId(9),
                3 => c.incarnation = 0,
                4 => c.keys.clear(),
                5 => c.keys.push(KEY),
                6 => c.configuration.clear(),
                7 => c.vote_ttl = Duration::ZERO,
                8 => c.stop_margin = Duration::ZERO,
                9 => c.stop_margin = c.vote_ttl,
                _ => unreachable!(),
            }
            assert!(
                Coordinator::new(ManualClock::default(), c).is_err(),
                "case {change}"
            );
        }
    }

    #[test]
    fn test_only_boot_quarantine_refuses_every_vote_for_full_hold_interval() {
        let mut w = World::cold();
        let request = w.nodes[0].begin(KEY, false).unwrap();
        let reply = w.request(0, 1, &request);
        assert!(!granted(&reply));
        w.ack(1, 0, &reply);
        assert!(w.nodes[0].local_lease(KEY).is_none());
        w.advance(999);
        let request = w.nodes[0].begin(KEY, false).unwrap();
        assert!(!granted(&w.request(0, 1, &request)));
        w.advance(1);
        assert!(granted(&w.request(0, 1, &request)));
        // The own vote was refused before quarantine: a single remote vote is
        // still not a quorum; no retrospective self-vote is fabricated.
        w.exchange(0, 1, &request);
        assert!(w.nodes[0].local_lease(KEY).is_none());
    }

    #[test]
    fn test_only_majority_never_shrinks_when_peers_disappear() {
        let mut w = World::new();
        assert_eq!(w.nodes[0].majority(), 2);
        w.nodes[0].begin(KEY, false).unwrap();
        assert!(w.nodes[0].local_lease(KEY).is_none());
        w.advance(800);
        assert!(w.nodes[0].local_lease(KEY).is_none());
        w.nodes[0].watchdog().unwrap();
        w.nodes[0].begin(KEY, false).unwrap();
        assert!(w.nodes[0].local_lease(KEY).is_none());
        assert_eq!(
            w.nodes[0].acquire(KEY, Duration::from_millis(800)),
            Err(OwnershipError::QuorumUnavailable)
        );
    }

    #[test]
    fn test_only_late_duplicate_and_foreign_ack_do_not_extend_deadline() {
        let mut w = World::new();
        let request = w.nodes[0].begin(KEY, false).unwrap();
        let reply = w.request(0, 1, &request);
        w.advance(700);
        w.nodes[0].receive(NodeId(99), 99, &reply).unwrap();
        assert!(w.nodes[0].local_lease(KEY).is_none());
        w.ack(1, 0, &reply);
        let lease = w.nodes[0].local_lease(KEY).unwrap();
        assert_eq!(lease.expires_at, Duration::from_millis(1800));
        w.advance(99);
        w.ack(1, 0, &reply);
        assert_eq!(w.nodes[0].local_lease(KEY), Some(lease));
        w.advance(1);
        w.ack(1, 0, &reply);
        assert!(w.nodes[0].local_lease(KEY).is_none());
        assert_eq!(w.nodes[0].watchdog().unwrap(), vec![KEY]);
        assert!(w.nodes[0].watchdog().unwrap().is_empty());
    }

    #[test]
    fn test_only_ack_at_boundary_is_rejected() {
        let mut w = World::new();
        let request = w.nodes[0].begin(KEY, false).unwrap();
        let reply = w.request(0, 1, &request);
        w.advance(800);
        w.ack(1, 0, &reply);
        assert!(w.nodes[0].local_lease(KEY).is_none());
    }

    #[test]
    fn test_only_duplicate_request_does_not_extend_voter_hold() {
        let mut w = World::new();
        let request = w.nodes[0].begin(KEY, false).unwrap();
        assert!(granted(&w.request(0, 1, &request)));
        w.advance(700);
        assert!(granted(&w.request(0, 1, &request)));
        assert_eq!(
            w.nodes[1].holds[&index(KEY)].deadline,
            Duration::from_millis(2000)
        );
        w.advance(300);
        assert!(!granted(&w.request(0, 1, &request)));
    }

    #[test]
    fn test_only_quorum_intersection_prevents_overlapping_owners_and_bot_mixups() {
        let mut w = World::new();
        let old = w.grant(0, KEY);
        let other = w.grant(2, OTHER);
        assert_eq!(other.key, OTHER);
        let attempt = w.nodes[2].begin(KEY, false).unwrap();
        w.exchange(2, 0, &attempt);
        w.exchange(2, 1, &attempt);
        assert!(w.nodes[2].local_lease(KEY).is_none());
        assert_eq!(w.nodes[0].local_lease(KEY), Some(old));
        w.advance(800);
        assert!(w.nodes[0].local_lease(KEY).is_none());
        let attempt = w.nodes[2].begin(KEY, false).unwrap();
        assert!(!granted(&w.request(2, 1, &attempt)));
        // Even after local admission expires, the full voter hold is respected.
        w.advance(200);
        w.exchange(2, 0, &attempt);
        w.exchange(2, 1, &attempt);
        assert!(w.nodes[2].local_lease(KEY).is_some());
        assert!(w.nodes[0].local_lease(KEY).is_none());
    }

    #[test]
    fn test_only_renew_requires_quorum_and_keeps_epoch() {
        let mut w = World::new();
        let first = w.grant(0, KEY);
        w.advance(300);
        let request = w.nodes[0].begin(KEY, true).unwrap();
        assert_eq!(w.nodes[0].local_lease(KEY), Some(first));
        w.exchange(0, 1, &request);
        let renewed = w.nodes[0].local_lease(KEY).unwrap();
        assert_eq!(renewed.epoch, first.epoch);
        assert_eq!(renewed.expires_at, Duration::from_millis(2100));
        assert_eq!(
            w.nodes[1].holds[&index(KEY)].deadline,
            Duration::from_millis(2300)
        );
    }

    #[test]
    fn test_only_pending_renew_cannot_resurrect_an_expired_local_lease() {
        let mut w = World::new();
        w.grant(0, KEY);
        w.advance(700);
        let request = w.nodes[0].begin(KEY, true).unwrap();
        let reply = w.request(0, 1, &request);
        w.advance(100);
        w.ack(1, 0, &reply);
        assert!(w.nodes[0].local_lease(KEY).is_none());
        assert!(!w.nodes[0].has_pending(KEY));
    }

    #[test]
    fn test_only_release_cancels_pending_without_shortening_remote_exclusion() {
        let mut w = World::new();
        let old = w.grant(0, KEY);
        w.advance(100);
        let renewal = w.nodes[0].begin(KEY, true).unwrap();
        let reply = w.request(0, 1, &renewal);
        w.nodes[0].release(KEY, old.epoch).unwrap();
        w.ack(1, 0, &reply);
        assert!(w.nodes[0].local_lease(KEY).is_none());
        let request = w.nodes[2].begin(KEY, false).unwrap();
        assert!(!granted(&w.request(2, 1, &request)));
        assert_eq!(
            w.nodes[1].holds[&index(KEY)].deadline,
            Duration::from_millis(2100)
        );
    }

    #[test]
    fn test_only_restart_loses_votes_but_waits_out_all_precrash_holds() {
        let mut w = World::new();
        w.grant(0, KEY);
        w.advance(200);
        w.nodes[1] = Coordinator::new(w.clock.clone(), config(2, 1)).unwrap();
        // A smaller random incarnation is valid but never skips quarantine.
        let request = w.nodes[2].begin(KEY, false).unwrap();
        assert!(!granted(&w.request(2, 1, &request)));
        w.advance(999);
        let request = w.nodes[2].begin(KEY, false).unwrap();
        assert!(!granted(&w.request(2, 1, &request)));
        w.advance(1);
        assert!(granted(&w.request(2, 1, &request)));
        assert!(w.nodes[0].local_lease(KEY).is_none());
    }

    #[test]
    fn test_only_unbound_config_identity_and_unknown_keys_are_ignored() {
        let mut w = World::new();
        let message = w.nodes[0].begin(KEY, false).unwrap();
        let mut invalid = message.clone();
        invalid.configuration.push('!');
        assert!(
            w.nodes[1]
                .receive(NodeId(1), 101, &invalid)
                .unwrap()
                .is_none()
        );
        assert!(
            w.nodes[1]
                .receive(NodeId(2), 101, &message)
                .unwrap()
                .is_none()
        );
        assert!(
            w.nodes[1]
                .receive(NodeId(1), 102, &message)
                .unwrap()
                .is_none()
        );
        let TestOnlyPayload::Request(mut request) = message.payload else {
            panic!()
        };
        request.key.shard_id = 99;
        invalid = TestOnlyMessage {
            configuration: message.configuration,
            payload: TestOnlyPayload::Request(request),
        };
        assert!(
            w.nodes[1]
                .receive(NodeId(1), 101, &invalid)
                .unwrap()
                .is_none()
        );
        assert!(w.nodes[1].holds.is_empty());
    }

    #[test]
    fn test_only_vote_matches_complete_request_and_distinct_voter() {
        let mut w = World::new();
        let message = w.nodes[0].begin(KEY, false).unwrap();
        let reply = w.request(0, 1, &message);
        let mut forged = reply.clone();
        let TestOnlyPayload::Vote { request, .. } = &mut forged.payload else {
            panic!()
        };
        request.epoch.0 += 1;
        w.ack(1, 0, &forged);
        assert!(w.nodes[0].local_lease(KEY).is_none());
        // Re-labelling a reply as the already-counted self identity is no extra vote.
        w.nodes[0].receive(NodeId(1), 101, &reply).unwrap();
        assert!(w.nodes[0].local_lease(KEY).is_none());
        w.ack(1, 0, &reply);
        assert!(w.nodes[0].local_lease(KEY).is_some());
    }

    #[test]
    fn test_only_independent_clock_origins_do_not_cross_as_remote_timestamps() {
        let a_clock = ManualClock::default();
        let b_clock = ManualClock::default();
        a_clock.set(Duration::from_secs(10)).unwrap();
        b_clock.set(Duration::from_secs(1_000_000)).unwrap();
        let mut a = Coordinator::new(a_clock.clone(), config(1, 101)).unwrap();
        let mut b = Coordinator::new(b_clock.clone(), config(2, 102)).unwrap();
        a_clock.advance(Duration::from_secs(1)).unwrap();
        b_clock.advance(Duration::from_secs(1)).unwrap();
        let request = a.begin(KEY, false).unwrap();
        let vote = b.receive(NodeId(1), 101, &request).unwrap().unwrap();
        a.receive(NodeId(2), 102, &vote).unwrap();
        assert_eq!(
            a.local_lease(KEY).unwrap().expires_at,
            Duration::from_millis(11_800)
        );
        assert_eq!(
            b.holds[&index(KEY)].deadline,
            Duration::from_secs(1_000_002)
        );
    }

    #[test]
    fn test_only_clock_regression_permanently_fails_closed() {
        let mut w = World::new();
        let lease = w.grant(0, KEY);
        *w.clock.0.lock().unwrap() = Duration::ZERO;
        assert!(w.nodes[0].local_lease(KEY).is_none());
        assert_eq!(
            w.nodes[0].check(KEY, lease.epoch),
            Err(OwnershipError::ClockRegressed)
        );
        w.advance(2000);
        assert_eq!(w.nodes[0].watchdog(), Err(OwnershipError::ClockRegressed));
        assert!(w.nodes[0].begin(KEY, false).is_err());
    }

    #[test]
    fn test_only_heartbeat_contains_only_live_owner_claims_and_local_fencing_works() {
        let mut w = World::new();
        let lease = w.grant(0, KEY);
        assert_eq!(
            w.nodes[0].check(KEY, OwnershipEpoch(99)),
            Err(OwnershipError::Fenced)
        );
        assert_eq!(w.nodes[0].check(KEY, lease.epoch), Ok(()));
        let TestOnlyPayload::Heartbeat {
            sequence,
            ownership,
        } = w.nodes[0].heartbeat(42).payload
        else {
            panic!()
        };
        assert_eq!(sequence, 42);
        assert_eq!(
            ownership,
            vec![TestOnlyClaim {
                key: KEY,
                owner: NodeId(1),
                incarnation: 101,
                epoch: lease.epoch
            }]
        );
        w.advance(800);
        let TestOnlyPayload::Heartbeat { ownership, .. } = w.nodes[0].heartbeat(43).payload else {
            panic!()
        };
        assert!(ownership.is_empty());
    }

    #[test]
    fn test_only_seeded_delays_duplicates_partitions_and_reboots_keep_single_lease() {
        // Exercise the actual per-node coordinator, with no shared authority.
        // Old process leases survive coordinator replacement until their own
        // deadline; this models the separate guardian's last approved lease.
        let mut grant_count = 0;
        for seed in 1..=64u64 {
            let mut w = World::new();
            w.grant(0, KEY);
            let mut random = seed;
            let mut packets: Vec<(Duration, usize, usize, u64, TestOnlyMessage)> = Vec::new();
            let mut orphans: Vec<Lease> = Vec::new();
            let mut observed_grants = BTreeSet::new();
            let mut incarnation = 1000;
            for _ in 0..512 {
                random = random.wrapping_mul(6364136223846793005).wrapping_add(1);
                w.advance((random >> 32) % 71);
                let node = (random as usize) % 3;
                let action = (random >> 8) % 31;
                if action <= 2 && !w.nodes[node].has_pending(KEY) {
                    let renew = w.nodes[node].local_lease(KEY).is_some();
                    let message = w.nodes[node].begin(KEY, renew).unwrap();
                    for peer in 0..3 {
                        if peer != node {
                            let at = w.clock.now()
                                + Duration::from_millis((random >> (16 + peer * 8)) % 450);
                            packets.push((
                                at,
                                node,
                                peer,
                                w.nodes[node].config.incarnation,
                                message.clone(),
                            ));
                        }
                    }
                } else if action == 3 {
                    if let Some(lease) = w.nodes[node].local_lease(KEY) {
                        orphans.push(lease);
                    }
                    incarnation += 1;
                    w.nodes[node] =
                        Coordinator::new(w.clock.clone(), config(node as u64 + 1, incarnation))
                            .unwrap();
                } else if action == 4 && !packets.is_empty() {
                    let packet = packets[(random as usize) % packets.len()].clone();
                    packets.push(packet); // authentic retransmission, not an extra voter
                }
                // Bounded loss/queue model: a directed partition is a dropped
                // request or reply; delayed traffic retains original incarnation.
                if packets.len() > 200 {
                    packets.drain(..packets.len() - 200);
                }
                let due = packets.iter().position(|p| p.0 <= w.clock.now());
                if let Some(index) = due {
                    let (_, from, to, from_incarnation, packet) = packets.remove(index);
                    if action != 5
                        && let Some(reply) = w.nodes[to]
                            .receive(NodeId(from as u64 + 1), from_incarnation, &packet)
                            .unwrap()
                    {
                        packets.push((
                            w.clock.now() + Duration::from_millis((random >> 40) % 450),
                            to,
                            from,
                            w.nodes[to].config.incarnation,
                            reply,
                        ));
                    }
                }
                orphans.retain(|lease| w.clock.now() < lease.expires_at);
                let live = w
                    .nodes
                    .iter()
                    .filter(|node| node.local_lease(KEY).is_some())
                    .count()
                    + orphans.len();
                assert!(
                    live <= 1,
                    "seed {seed}, time {:?}: {live} live grants",
                    w.clock.now()
                );
                for node in &w.nodes {
                    if let Some(lease) = node.local_lease(KEY) {
                        observed_grants.insert((
                            node.config.node,
                            node.config.incarnation,
                            lease.epoch.0,
                        ));
                    }
                }
                for node in &mut w.nodes {
                    node.watchdog().unwrap();
                }
            }
            grant_count += observed_grants.len();
        }
        assert!(
            grant_count > 128,
            "fault scan must exercise new grants, got {grant_count}"
        );
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
