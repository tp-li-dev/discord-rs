//! Loopback process-test adapter for the **test-only** majority ACK coordinator.
//!
//! This is not Raft, a durable consensus backend, or production cluster support.
//! It runs only with explicit opt-in and the token-free mock Discord Gateway.
//! All lease deadlines leaving this module are in this process's `Instant`
//! domain. Peer clocks and peer payload timestamps never become local deadlines.

use crate::{
    cluster::{
        ControlMessage, NodeId, TestOnlyPayload, TestOnlyQuorumConfig, TestOnlyQuorumCoordinator,
        Transport,
    },
    daemon::ShardKey,
    failover::{self, Event, Failover, State},
    ownership::{Clock, Lease, OwnershipCoordinator, OwnershipKey},
    placement::{self, ClusterMode, LocalShardIntent, ShardCount},
    protocol::BotId,
    redact::Secret,
    shard_runtime::{self, ShardRuntimeSpec},
    tls_transport::{TlsIdentity, TlsTransport, TlsTransportConfig, TransportLimits},
};
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use serde::Deserialize;
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    fmt,
    io::Read,
    net::SocketAddr,
    path::{Component, Path, PathBuf},
    time::{Duration, Instant},
};

const MAX_CONFIG: usize = 1024 * 1024;
const MAX_DER: usize = 64 * 1024;
const MAX_MEMBERS: usize = 16;
const MAX_SHARDS: usize = 256;
const MAX_RETIRED_INCARNATIONS: usize = 64;

/// Safe fixed diagnostics: TOML text, keys, certificates and credentials are not
/// copied into errors, even when a user puts a secret into the wrong field.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ClusterRuntimeError {
    ConfigRead,
    ConfigTooLarge,
    InvalidConfig,
    TestOnlyOptInRequired,
    MockDiscordRequired,
    UnsupportedMode,
    CertificateRead,
    InvalidPrivateKey,
    Transport,
    Coordinator,
    Policy,
    CounterExhausted,
    UnknownShard,
}
impl fmt::Display for ClusterRuntimeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "test cluster: {self:?}")
    }
}
impl std::error::Error for ClusterRuntimeError {}

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TestClusterPeer {
    pub id: u64,
    pub name: String,
    pub address: SocketAddr,
}

/// Explicitly non-production, immutable configuration for one botd lifetime.
/// Certificate files are DER (leaf certificate, PKCS#8/PKCS#1/SEC1 private key,
/// and one trust anchor). Relative paths resolve against the TOML directory.
#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TestClusterConfig {
    pub node_id: u64,
    pub listen: SocketAddr,
    pub cert_path: PathBuf,
    pub key_path: PathBuf,
    pub ca_path: PathBuf,
    #[serde(default = "default_lease_ms")]
    pub lease_ms: u64,
    #[serde(default = "default_stop_margin_ms")]
    pub stop_margin_ms: u64,
    pub peers: Vec<TestClusterPeer>,
}
fn default_lease_ms() -> u64 {
    4000
}
fn default_stop_margin_ms() -> u64 {
    1000
}

#[derive(Clone)]
pub struct LoadedClusterConfig {
    pub placement: placement::Config,
    pub test_cluster: Option<TestClusterConfig>,
}

fn bounded_file(path: &Path, limit: usize) -> Result<Vec<u8>, ClusterRuntimeError> {
    let file = std::fs::File::open(path).map_err(|_| ClusterRuntimeError::ConfigRead)?;
    let mut bytes = Vec::new();
    file.take(limit as u64 + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| ClusterRuntimeError::ConfigRead)?;
    if bytes.len() > limit {
        return Err(ClusterRuntimeError::ConfigTooLarge);
    }
    Ok(bytes)
}

impl LoadedClusterConfig {
    /// Read the extension separately, preserving strict unknown-field rejection
    /// in the existing standalone placement loader and in this test extension.
    pub fn load(path: &Path) -> Result<Self, ClusterRuntimeError> {
        let bytes = bounded_file(path, MAX_CONFIG)?;
        let source = std::str::from_utf8(&bytes).map_err(|_| ClusterRuntimeError::InvalidConfig)?;
        let mut value: toml::Value =
            toml::from_str(source).map_err(|_| ClusterRuntimeError::InvalidConfig)?;
        let table = value
            .as_table_mut()
            .ok_or(ClusterRuntimeError::InvalidConfig)?;
        let mut test_cluster: Option<TestClusterConfig> = table
            .remove("test_cluster")
            .map(toml::Value::try_into)
            .transpose()
            .map_err(|_| ClusterRuntimeError::InvalidConfig)?;
        let placement: placement::Config = value
            .try_into()
            .map_err(|_| ClusterRuntimeError::InvalidConfig)?;
        if let Some(config) = &mut test_cluster {
            let parent = path.parent().unwrap_or_else(|| Path::new("."));
            for file in [
                &mut config.cert_path,
                &mut config.key_path,
                &mut config.ca_path,
            ] {
                if file.as_os_str().is_empty()
                    || file.components().any(|part| part == Component::ParentDir)
                {
                    return Err(ClusterRuntimeError::InvalidConfig);
                }
                if file.is_relative() {
                    *file = parent.join(&*file);
                }
            }
        }
        let loaded = Self {
            placement,
            test_cluster,
        };
        if loaded.test_cluster.is_some() {
            loaded.validate_test_cluster()?;
        }
        Ok(loaded)
    }

    fn validate_test_cluster(&self) -> Result<(), ClusterRuntimeError> {
        let config = self
            .test_cluster
            .as_ref()
            .ok_or(ClusterRuntimeError::UnsupportedMode)?;
        if self.placement.cluster.mode != ClusterMode::Quorum
            || self.placement.node.remote_bind.is_some()
        {
            return Err(ClusterRuntimeError::UnsupportedMode);
        }
        self.placement
            .validate(&HashMap::new())
            .map_err(|_| ClusterRuntimeError::InvalidConfig)?;
        crate::botref::validate_bots(&self.placement.bots)
            .map_err(|_| ClusterRuntimeError::InvalidConfig)?;
        if config.node_id == 0
            || !loopback_endpoint(config.listen)
            || !(2..MAX_MEMBERS).contains(&config.peers.len())
            || !(500..=60_000).contains(&config.lease_ms)
            || config.stop_margin_ms < 100
            || config.stop_margin_ms >= config.lease_ms
            || self.placement.failover.heartbeat_ms > (config.lease_ms - config.stop_margin_ms) / 4
            || self.placement.failover.unhealthy_after_ms > 300_000
            || self.placement.failover.failback_stable_ms > 3_600_000
        {
            return Err(ClusterRuntimeError::InvalidConfig);
        }
        let mut ids = BTreeSet::from([config.node_id]);
        let mut names = BTreeSet::from([self.placement.node.id.as_str()]);
        let mut routes = BTreeSet::from([config.listen]);
        for peer in &config.peers {
            if peer.id == 0
                || !ids.insert(peer.id)
                || peer.name.trim().is_empty()
                || peer.name.trim() != peer.name
                || peer.name.len() > 128
                || !names.insert(peer.name.as_str())
                || !loopback_endpoint(peer.address)
                || !routes.insert(peer.address)
            {
                return Err(ClusterRuntimeError::InvalidConfig);
            }
        }
        let mut count = 0usize;
        for bot in &self.placement.bots {
            let id: u64 = bot
                .bot_id
                .parse()
                .map_err(|_| ClusterRuntimeError::InvalidConfig)?;
            if bot.bot_id != id.to_string() {
                return Err(ClusterRuntimeError::InvalidConfig);
            }
            let ShardCount::Fixed(total) = bot.total_shards else {
                return Err(ClusterRuntimeError::InvalidConfig);
            };
            // No inferred placement or unresolved auto-sharding in this fixture.
            if total as usize != bot.shard_placement.len() {
                return Err(ClusterRuntimeError::InvalidConfig);
            }
            count += bot.shard_placement.len();
            for shard in &bot.shard_placement {
                if std::iter::once(&shard.primary)
                    .chain(&shard.failover)
                    .any(|name| !names.contains(name.as_str()))
                {
                    return Err(ClusterRuntimeError::InvalidConfig);
                }
            }
        }
        if count == 0 || count > MAX_SHARDS {
            return Err(ClusterRuntimeError::InvalidConfig);
        }
        Ok(())
    }

    /// Descriptions for every shard this node may own, primary or fallback.
    /// These descriptions confer no ownership and must only be launched after
    /// `ClusterAction::Start` with a currently valid quorum-backed lease.
    pub fn local_specs(&self, root: &Path) -> Result<Vec<ShardRuntimeSpec>, ClusterRuntimeError> {
        self.validate_test_cluster()?;
        let intents = self
            .placement
            .bots
            .iter()
            .flat_map(|bot| {
                bot.shard_placement.iter().filter_map(|shard| {
                    if shard.primary != self.placement.node.id
                        && !shard.failover.contains(&self.placement.node.id)
                    {
                        return None;
                    }
                    let ShardCount::Fixed(total_shards) = bot.total_shards else {
                        return None;
                    };
                    Some(LocalShardIntent {
                        bot_id: bot.bot_id.clone(),
                        shard_id: shard.shard_id,
                        total_shards,
                        token_env: bot.token_env.clone(),
                    })
                })
            })
            .collect::<Vec<_>>();
        shard_runtime::plan(&intents, root).map_err(|_| ClusterRuntimeError::InvalidConfig)
    }

    fn members(&self) -> BTreeMap<NodeId, String> {
        let config = self.test_cluster.as_ref().expect("validated extension");
        std::iter::once((NodeId(config.node_id), self.placement.node.id.clone()))
            .chain(
                config
                    .peers
                    .iter()
                    .map(|peer| (NodeId(peer.id), peer.name.clone())),
            )
            .collect()
    }

    /// Canonical, bounded public deployment description, not a hash of secrets.
    /// Route addresses may differ because E2E proxies inject directed partitions.
    /// All voters must agree on membership, IDs, placement and safety timings.
    fn configuration(&self) -> Result<String, ClusterRuntimeError> {
        let config = self.test_cluster.as_ref().expect("validated extension");
        let members: Vec<_> = self
            .members()
            .into_iter()
            .map(|(id, name)| (id.0, name))
            .collect();
        let mut bots = self.placement.bots.clone();
        bots.sort_by(|a, b| a.bot_id.cmp(&b.bot_id));
        let bots: Vec<_> = bots
            .into_iter()
            .map(|mut bot| {
                bot.shard_placement.sort_by_key(|shard| shard.shard_id);
                json!({"bot_id": bot.bot_id, "total_shards": bot.total_shards,
                "shards": bot.shard_placement})
            })
            .collect();
        let f = &self.placement.failover;
        let value = json!({"version": 1, "cluster": self.placement.cluster.id,
            "members": members, "bots": bots, "lease_ms": config.lease_ms,
            "stop_margin_ms": config.stop_margin_ms,
            "heartbeat_ms": f.heartbeat_ms, "suspect_ms": f.suspect_after_ms,
            "fail_ms": f.unhealthy_after_ms, "stable_ms": f.failback_stable_ms});
        let encoded =
            serde_json::to_string(&value).map_err(|_| ClusterRuntimeError::InvalidConfig)?;
        if encoded.len() > 64 * 1024 {
            return Err(ClusterRuntimeError::InvalidConfig);
        }
        Ok(encoded)
    }
}

fn loopback_endpoint(address: SocketAddr) -> bool {
    address.ip().is_loopback() && address.port() != 0
}

#[derive(Clone)]
struct RuntimeClock {
    origin: Instant,
}
impl Clock for RuntimeClock {
    fn now(&self) -> Duration {
        self.origin.elapsed()
    }
}

#[derive(Clone, Debug)]
pub enum ClusterAction {
    Start {
        key: ShardKey,
        epoch: u64,
        expires_at: Instant,
    },
    Renew {
        key: ShardKey,
        epoch: u64,
        expires_at: Instant,
    },
    Stop {
        key: ShardKey,
        reason: &'static str,
    },
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum ProcessPhase {
    Idle,
    Starting,
    Active,
    Draining,
}
impl ProcessPhase {
    fn label(self) -> &'static str {
        match self {
            Self::Idle => "stopped",
            Self::Starting => "starting",
            Self::Active => "active",
            Self::Draining => "draining",
        }
    }
}

struct RuntimeShard {
    key: ShardKey,
    ownership: OwnershipKey,
    primary: NodeId,
    failover: Vec<NodeId>,
    policy: Failover<RuntimeClock>,
    phase: ProcessPhase,
    published: Option<Lease>,
    next_request: Duration,
    returning: bool,
}

struct RuntimePeer {
    name: String,
    incarnation: u64,
    sequence: u64,
    last_seen: Option<Duration>,
    retired: BTreeSet<u64>,
    exhausted: bool,
    claims: BTreeSet<(u64, u32)>,
}
impl RuntimePeer {
    /// Returns whether this valid heartbeat begins a new observed incarnation.
    /// Incarnations are random identifiers, not ordered clocks or epochs.
    fn observe(&mut self, incarnation: u64, sequence: u64, now: Duration) -> Option<bool> {
        if incarnation == 0
            || sequence == 0
            || self.exhausted
            || self.retired.contains(&incarnation)
        {
            return None;
        }
        let restarted = self.incarnation != 0 && self.incarnation != incarnation;
        if self.incarnation == incarnation && sequence <= self.sequence {
            return None;
        }
        if restarted {
            if self.retired.len() >= MAX_RETIRED_INCARNATIONS {
                self.exhausted = true;
                return None;
            }
            self.retired.insert(self.incarnation);
        }
        if self.incarnation != incarnation {
            self.claims.clear();
        }
        self.incarnation = incarnation;
        self.sequence = sequence;
        self.last_seen = Some(now);
        Some(restarted)
    }
}

/// Every botd owns one runtime, one transport, and one test ACK coordinator.
/// A process supervisor must honour every Stop and independently enforce the
/// passed deadline; keeping an Active-looking snapshot is never permission.
pub struct ClusterRuntime {
    node: NodeId,
    clock: RuntimeClock,
    transport: TlsTransport,
    coordinator: TestOnlyQuorumCoordinator<RuntimeClock>,
    configuration: String,
    peers: BTreeMap<NodeId, RuntimePeer>,
    shards: BTreeMap<ShardKey, RuntimeShard>,
    failover: failover::Config,
    next_heartbeat: Duration,
    heartbeat_sequence: u64,
    quorum: bool,
    quorum_since: Option<Duration>,
    stopped: bool,
}

impl ClusterRuntime {
    pub async fn bind(
        loaded: &LoadedClusterConfig,
        allow_test_quorum: bool,
        mock_discord: bool,
    ) -> Result<Self, ClusterRuntimeError> {
        // These guards run before certificate I/O, socket bind or process work.
        if !allow_test_quorum {
            return Err(ClusterRuntimeError::TestOnlyOptInRequired);
        }
        if !mock_discord {
            return Err(ClusterRuntimeError::MockDiscordRequired);
        }
        loaded.validate_test_cluster()?;
        let config = loaded.test_cluster.as_ref().expect("validated extension");
        let certificate = bounded_file(&config.cert_path, MAX_DER)
            .map_err(|_| ClusterRuntimeError::CertificateRead)?;
        let key = bounded_file(&config.key_path, MAX_DER)
            .map_err(|_| ClusterRuntimeError::CertificateRead)?;
        let ca = bounded_file(&config.ca_path, MAX_DER)
            .map_err(|_| ClusterRuntimeError::CertificateRead)?;
        let key =
            PrivateKeyDer::try_from(key).map_err(|_| ClusterRuntimeError::InvalidPrivateKey)?;
        let mut incarnation_bytes = [0u8; 8];
        getrandom::fill(&mut incarnation_bytes)
            .map_err(|_| ClusterRuntimeError::CounterExhausted)?;
        let incarnation = u64::from_le_bytes(incarnation_bytes).max(1);
        let node = NodeId(config.node_id);
        let mut tls = TlsTransportConfig::new(
            node,
            TlsIdentity {
                certificates: vec![CertificateDer::from(certificate)],
                private_key: Secret::new(key),
            },
            vec![CertificateDer::from(ca)],
        );
        tls.incarnation = incarnation;
        tls.bind_addr = config.listen;
        let members = loaded.members();
        tls.allowed_nodes = members.keys().copied().collect();
        tls.peers = config
            .peers
            .iter()
            .map(|peer| (NodeId(peer.id), peer.address))
            .collect();
        tls.limits = TransportLimits {
            max_frame_len: 256 * 1024,
            queue_capacity: 1024,
            max_connections: 64,
            handshake_timeout: Duration::from_millis(config.lease_ms.min(1000)),
            read_timeout: Duration::from_millis(config.lease_ms.min(1000)),
            write_timeout: Duration::from_millis(config.lease_ms.min(1000)),
        };
        let transport = TlsTransport::bind(tls)
            .await
            .map_err(|_| ClusterRuntimeError::Transport)?;
        let clock = RuntimeClock {
            origin: Instant::now(),
        };
        let f = &loaded.placement.failover;
        let failover = failover::Config {
            heartbeat_interval: Duration::from_millis(f.heartbeat_ms),
            suspect_after: Duration::from_millis(f.suspect_after_ms),
            fail_after: Duration::from_millis(f.unhealthy_after_ms),
            stable_for: Duration::from_millis(f.failback_stable_ms),
            cooldown: Duration::from_millis(f.heartbeat_ms.saturating_mul(2)),
            max_backoff: Duration::from_millis(f.unhealthy_after_ms.saturating_mul(4)),
        };
        let names: BTreeMap<_, _> = members
            .iter()
            .map(|(&id, name)| (name.as_str(), id))
            .collect();
        let mut shards = BTreeMap::new();
        for bot in &loaded.placement.bots {
            let bot_id = BotId(
                bot.bot_id
                    .parse()
                    .map_err(|_| ClusterRuntimeError::InvalidConfig)?,
            );
            for placement in &bot.shard_placement {
                let key = ShardKey {
                    bot_id: bot.bot_id.clone(),
                    shard_id: placement.shard_id,
                };
                let ownership = OwnershipKey {
                    bot_id,
                    shard_id: placement.shard_id,
                };
                shards.insert(
                    key.clone(),
                    RuntimeShard {
                        key,
                        ownership,
                        primary: names[placement.primary.as_str()],
                        failover: placement
                            .failover
                            .iter()
                            .map(|name| names[name.as_str()])
                            .collect(),
                        policy: Failover::new(clock.clone(), failover)
                            .map_err(|_| ClusterRuntimeError::Policy)?,
                        phase: ProcessPhase::Idle,
                        published: None,
                        next_request: Duration::ZERO,
                        returning: false,
                    },
                );
            }
        }
        let configuration = loaded.configuration()?;
        let coordinator = TestOnlyQuorumCoordinator::new(
            clock.clone(),
            TestOnlyQuorumConfig {
                node,
                incarnation,
                members: members.keys().copied().collect(),
                keys: shards.values().map(|shard| shard.ownership).collect(),
                configuration: configuration.clone(),
                vote_ttl: Duration::from_millis(config.lease_ms),
                stop_margin: Duration::from_millis(config.stop_margin_ms),
            },
        )
        .map_err(|_| ClusterRuntimeError::Coordinator)?;
        let peers = members
            .into_iter()
            .filter(|(id, _)| *id != node)
            .map(|(id, name)| {
                (
                    id,
                    RuntimePeer {
                        name,
                        incarnation: 0,
                        sequence: 0,
                        last_seen: None,
                        retired: BTreeSet::new(),
                        exhausted: false,
                        claims: BTreeSet::new(),
                    },
                )
            })
            .collect();
        Ok(Self {
            node,
            clock,
            transport,
            coordinator,
            configuration,
            peers,
            shards,
            failover,
            next_heartbeat: Duration::ZERO,
            heartbeat_sequence: 0,
            quorum: false,
            quorum_since: None,
            stopped: false,
        })
    }

    fn broadcast(&mut self, message: crate::cluster::TestOnlyMessage) {
        for &peer in self.peers.keys() {
            // Local queue admission is never an ACK vote. Congestion or a failed
            // connection simply causes the pending lease request to expire.
            let _ = self
                .transport
                .send(self.node, peer, ControlMessage::TestOnly(message.clone()));
        }
    }

    /// Bounded, nonblocking control work. This performs no process waits, disk
    /// I/O or per-Discord-event remote calls. Schedule more often than heartbeat.
    pub fn tick(&mut self) -> Result<Vec<ClusterAction>, ClusterRuntimeError> {
        if self.stopped {
            return Ok(Vec::new());
        }
        let now = self.clock.now();
        let mut heard = BTreeSet::new();
        let mut restarted = BTreeSet::new();
        for _ in 0..1024 {
            let Some(envelope) = self
                .transport
                .receive()
                .map_err(|_| ClusterRuntimeError::Transport)?
            else {
                break;
            };
            let ControlMessage::TestOnly(message) = envelope.message else {
                continue;
            };
            if envelope.to != self.node || message.configuration != self.configuration {
                continue;
            }
            let Some(peer) = self.peers.get_mut(&envelope.from) else {
                continue;
            };
            if envelope.from_incarnation == 0
                || peer.retired.contains(&envelope.from_incarnation)
                || peer.exhausted
            {
                continue;
            }
            if let TestOnlyPayload::Heartbeat {
                sequence,
                ownership,
            } = &message.payload
            {
                let Some(new_incarnation) = peer.observe(envelope.from_incarnation, *sequence, now)
                else {
                    continue;
                };
                if new_incarnation {
                    restarted.insert(envelope.from);
                }
                peer.claims = ownership
                    .iter()
                    .filter(|claim| {
                        claim.owner == envelope.from
                            && claim.incarnation == envelope.from_incarnation
                    })
                    .map(|claim| (claim.key.bot_id.0, claim.key.shard_id))
                    .collect();
                heard.insert(envelope.from);
            }
            if let Some(reply) = self
                .coordinator
                .receive(envelope.from, envelope.from_incarnation, &message)
                .map_err(|_| ClusterRuntimeError::Coordinator)?
            {
                let _ =
                    self.transport
                        .send(self.node, envelope.from, ControlMessage::TestOnly(reply));
            }
        }
        self.coordinator
            .watchdog()
            .map_err(|_| ClusterRuntimeError::Coordinator)?;
        let healthy: BTreeSet<_> = std::iter::once(self.node)
            .chain(self.peers.iter().filter_map(|(&id, peer)| {
                peer.last_seen
                    .filter(|seen| now.saturating_sub(*seen) < self.failover.suspect_after)
                    .map(|_| id)
            }))
            .collect();
        // For N = peers + self, strict majority is floor(N / 2) + 1.
        self.quorum = healthy.len() > self.peers.len().div_ceil(2);
        if self.quorum {
            self.quorum_since.get_or_insert(now);
        } else {
            self.quorum_since = None;
        }
        let primary_stable = self
            .quorum_since
            .is_some_and(|since| now.saturating_sub(since) >= self.failover.stable_for);
        let mut actions = Vec::new();
        let mut messages = Vec::new();
        for shard in self.shards.values_mut() {
            let local_primary = shard.primary == self.node;
            let local_fallback = shard.failover.contains(&self.node);
            let running = matches!(shard.phase, ProcessPhase::Starting | ProcessPhase::Active);
            let candidate = shard
                .failover
                .iter()
                .copied()
                .find(|node| healthy.contains(node));
            let eligible = local_primary || running || candidate == Some(self.node);
            shard
                .policy
                .handle(Event::Quorum(self.quorum))
                .map_err(|_| ClusterRuntimeError::Policy)?;
            shard
                .policy
                .handle(Event::CandidateEligible(eligible))
                .map_err(|_| ClusterRuntimeError::Policy)?;
            if restarted.contains(&shard.primary) && shard.policy.state() == State::FailbackWaiting
            {
                // A quick restart is not continuous primary stability, even if
                // it completed within one heartbeat suspicion window.
                shard
                    .policy
                    .handle(Event::Quorum(false))
                    .map_err(|_| ClusterRuntimeError::Policy)?;
                shard
                    .policy
                    .handle(Event::Quorum(self.quorum))
                    .map_err(|_| ClusterRuntimeError::Policy)?;
            }
            if running && !local_primary && self.coordinator.local_lease(shard.ownership).is_some()
            {
                // Policy can become Degraded during a brief health loss while a
                // still-valid independent lease keeps the Gateway admitted.
                // Reconcile that actual grant before a primary heartbeat can
                // turn the pending policy back into an Assigned-looking state.
                if matches!(shard.policy.state(), State::Assigned | State::Suspected) {
                    shard
                        .policy
                        .handle(Event::Quorum(false))
                        .map_err(|_| ClusterRuntimeError::Policy)?;
                    shard
                        .policy
                        .handle(Event::Quorum(self.quorum))
                        .map_err(|_| ClusterRuntimeError::Policy)?;
                }
                if shard.policy.state() == State::FailoverPending {
                    let attempt = shard.policy.attempt();
                    shard
                        .policy
                        .handle(Event::OwnershipApproved { attempt })
                        .map_err(|_| ClusterRuntimeError::Policy)?;
                    if shard.phase == ProcessPhase::Active {
                        shard
                            .policy
                            .handle(Event::StartupComplete { attempt })
                            .map_err(|_| ClusterRuntimeError::Policy)?;
                    }
                }
            }
            if local_primary || heard.contains(&shard.primary) {
                shard
                    .policy
                    .handle(Event::Heartbeat)
                    .map_err(|_| ClusterRuntimeError::Policy)?;
            }
            shard
                .policy
                .handle(Event::Tick)
                .map_err(|_| ClusterRuntimeError::Policy)?;
            let primary_age = if local_primary {
                Duration::ZERO
            } else {
                now.saturating_sub(
                    self.peers
                        .get(&shard.primary)
                        .and_then(|peer| peer.last_seen)
                        .unwrap_or(Duration::ZERO),
                )
            };
            let primary_claim = self.peers.get(&shard.primary).is_some_and(|peer| {
                healthy.contains(&shard.primary)
                    && peer
                        .claims
                        .contains(&(shard.ownership.bot_id.0, shard.ownership.shard_id))
            });
            if shard.returning && shard.phase == ProcessPhase::Idle && primary_claim {
                let attempt = shard.policy.attempt();
                shard
                    .policy
                    .handle(Event::FailbackComplete { attempt })
                    .map_err(|_| ClusterRuntimeError::Policy)?;
                shard.returning = false;
            }
            if running && !local_primary && shard.policy.failback_ready() {
                self.coordinator.cancel(shard.ownership);
                shard.published = None;
                shard.phase = ProcessPhase::Draining;
                shard.returning = true;
                actions.push(ClusterAction::Stop {
                    key: shard.key.clone(),
                    reason: "stable_primary_returned",
                });
                continue;
            }
            let lease = self.coordinator.local_lease(shard.ownership);
            if running && lease.is_none() {
                self.coordinator.cancel(shard.ownership);
                shard.published = None;
                shard.phase = ProcessPhase::Draining;
                actions.push(ClusterAction::Stop {
                    key: shard.key.clone(),
                    reason: "lease_expired",
                });
                continue;
            }
            if shard.phase == ProcessPhase::Draining {
                continue;
            }
            let desired = (local_primary && (running || primary_stable))
                || (local_fallback
                    && (running
                        || (primary_age >= self.failover.fail_after
                            && candidate == Some(self.node)
                            && matches!(
                                shard.policy.state(),
                                State::FailoverPending | State::TemporaryActive
                            ))));
            if let Some(lease) = lease {
                if !desired {
                    self.coordinator.cancel(shard.ownership);
                    continue;
                }
                if shard.phase == ProcessPhase::Idle {
                    if !self.quorum || now < shard.next_request {
                        continue;
                    }
                    if !local_primary {
                        let attempt = shard.policy.attempt();
                        shard
                            .policy
                            .handle(Event::OwnershipApproved { attempt })
                            .map_err(|_| ClusterRuntimeError::Policy)?;
                    }
                    shard.phase = ProcessPhase::Starting;
                    shard.published = Some(lease);
                    actions.push(ClusterAction::Start {
                        key: shard.key.clone(),
                        epoch: lease.epoch.0,
                        expires_at: self.clock.origin + lease.expires_at,
                    });
                } else if shard
                    .published
                    .is_none_or(|old| old.expires_at < lease.expires_at)
                {
                    shard.published = Some(lease);
                    actions.push(ClusterAction::Renew {
                        key: shard.key.clone(),
                        epoch: lease.epoch.0,
                        expires_at: self.clock.origin + lease.expires_at,
                    });
                }
            }
            if desired
                && (running || self.quorum)
                && now >= shard.next_request
                && !self.coordinator.has_pending(shard.ownership)
            {
                let renew = self.coordinator.local_lease(shard.ownership).is_some();
                messages.push(
                    self.coordinator
                        .begin(shard.ownership, renew)
                        .map_err(|_| ClusterRuntimeError::Coordinator)?,
                );
                shard.next_request = now.saturating_add(self.failover.heartbeat_interval);
            }
        }
        for message in messages {
            self.broadcast(message);
        }
        if now >= self.next_heartbeat {
            self.heartbeat_sequence = self
                .heartbeat_sequence
                .checked_add(1)
                .ok_or(ClusterRuntimeError::CounterExhausted)?;
            let mut heartbeat = self.coordinator.heartbeat(self.heartbeat_sequence);
            if let TestOnlyPayload::Heartbeat { ownership, .. } = &mut heartbeat.payload {
                ownership.retain(|claim| {
                    self.shards.values().any(|shard| {
                        shard.ownership == claim.key && shard.phase == ProcessPhase::Active
                    })
                });
            }
            self.broadcast(heartbeat);
            self.next_heartbeat = now.saturating_add(self.failover.heartbeat_interval);
        }
        Ok(actions)
    }

    /// The supervisor calls this only after observing the real Gateway's Ready,
    /// including its Worker handshake. Spawning an OS process is not readiness.
    pub fn startup_complete(&mut self, key: &ShardKey) -> Result<(), ClusterRuntimeError> {
        let shard = self
            .shards
            .get_mut(key)
            .ok_or(ClusterRuntimeError::UnknownShard)?;
        if shard.phase != ProcessPhase::Starting {
            return Ok(());
        }
        if self.coordinator.local_lease(shard.ownership).is_none() {
            // Next tick sends Stop; the independent guardian enforces the same
            // deadline even if this event arrives after botd is stalled.
            return Ok(());
        }
        shard.phase = ProcessPhase::Active;
        if shard.primary != self.node {
            let attempt = shard.policy.attempt();
            shard
                .policy
                .handle(Event::StartupComplete { attempt })
                .map_err(|_| ClusterRuntimeError::Policy)?;
        }
        Ok(())
    }

    /// Call only after the supervisor has reaped its owned child/guardian. A
    /// timeout or a successful shutdown write is not proof of process exit.
    pub fn process_exited(&mut self, key: &ShardKey) -> Result<(), ClusterRuntimeError> {
        let shard = self
            .shards
            .get_mut(key)
            .ok_or(ClusterRuntimeError::UnknownShard)?;
        if shard.phase == ProcessPhase::Starting {
            let attempt = shard.policy.attempt();
            shard
                .policy
                .handle(Event::StartupFailed { attempt })
                .map_err(|_| ClusterRuntimeError::Policy)?;
        }
        self.coordinator.cancel(shard.ownership);
        shard.phase = ProcessPhase::Idle;
        shard.published = None;
        shard.next_request = self.clock.now().saturating_add(self.failover.cooldown);
        Ok(())
    }

    /// A failed supervisor control write revokes local admission immediately.
    /// The caller still retains/reaps the child and calls `process_exited` only
    /// once exit is observed; this method is not evidence of a stopped Gateway.
    pub fn process_failed(
        &mut self,
        key: &ShardKey,
    ) -> Result<Option<ClusterAction>, ClusterRuntimeError> {
        let shard = self
            .shards
            .get_mut(key)
            .ok_or(ClusterRuntimeError::UnknownShard)?;
        self.coordinator.cancel(shard.ownership);
        shard.published = None;
        if matches!(shard.phase, ProcessPhase::Idle | ProcessPhase::Draining) {
            return Ok(None);
        }
        shard.phase = ProcessPhase::Draining;
        Ok(Some(ClusterAction::Stop {
            key: key.clone(),
            reason: "process_control_failed",
        }))
    }

    pub fn status(&self) -> Value {
        let now = self.clock.now();
        let peers: Vec<_> = self.peers.iter().map(|(id, peer)| json!({
            "node_id": id.0, "name": peer.name,
            "healthy": peer.last_seen.is_some_and(|seen| now.saturating_sub(seen) < self.failover.suspect_after),
            "last_seen_ms": peer.last_seen.map(|seen| now.saturating_sub(seen).as_millis()),
            "incarnation_limit_reached": peer.exhausted,
        })).collect();
        let shards: Vec<_> = self.shards.values().map(|shard| {
            let lease = self.coordinator.local_lease(shard.ownership);
            json!({"bot_id": shard.key.bot_id, "shard_id": shard.key.shard_id,
                "primary": shard.primary.0, "policy": format!("{:?}", shard.policy.state()),
                "process": shard.phase.label(), "epoch": lease.map(|lease| lease.epoch.0),
                "lease_remaining_ms": lease.map(|lease| lease.expires_at.saturating_sub(now).as_millis()),
                "returning": shard.returning})
        }).collect();
        let transport = self.transport.stats();
        json!({"implementation": "TestOnlyQuorumCoordinator", "production_ready": false,
            "node_id": self.node.0, "quorum": self.quorum, "peers": peers, "shards": shards,
            "primary_stable_ms": self.quorum_since.map(|since| now.saturating_sub(since).as_millis()),
            "transport": {"received": transport.received, "delivered": transport.delivered,
                "rejected": transport.rejected, "failed": transport.failed}})
    }

    pub async fn shutdown(&mut self) {
        self.stopped = true;
        self.quorum = false;
        for shard in self.shards.values() {
            self.coordinator.cancel(shard.ownership);
        }
        self.transport.shutdown().await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    const CONFIG: &str = r#"
[cluster]
id = "fixture"
mode = "quorum"
[node]
id = "a"
[failover]
heartbeat_ms = 100
suspect_after_ms = 300
unhealthy_after_ms = 600
failback_stable_ms = 1200
[test_cluster]
node_id = 1
listen = "127.0.0.1:31001"
cert_path = "cert.der"
key_path = "key.der"
ca_path = "ca.der"
lease_ms = 1500
stop_margin_ms = 500
[[test_cluster.peers]]
id = 2
name = "b"
address = "127.0.0.1:31002"
[[test_cluster.peers]]
id = 3
name = "c"
address = "127.0.0.1:31003"
[[bots]]
name = "alpha"
bot_id = "101"
application_id = "201"
token_env = "TOKEN_ALPHA"
total_shards = 3
[[bots.shard_placement]]
shard_id = 0
primary = "a"
failover = ["b", "c"]
[[bots.shard_placement]]
shard_id = 1
primary = "b"
failover = ["c", "a"]
[[bots.shard_placement]]
shard_id = 2
primary = "c"
failover = ["b"]
"#;

    struct ConfigFile(PathBuf);
    impl ConfigFile {
        fn new(contents: &str) -> Self {
            static NEXT: AtomicU64 = AtomicU64::new(1);
            let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("target/cluster-config-tests");
            std::fs::create_dir_all(&root).unwrap();
            let path = root.join(format!(
                "{}-{}.toml",
                std::process::id(),
                NEXT.fetch_add(1, Ordering::Relaxed)
            ));
            std::fs::write(&path, contents).unwrap();
            Self(path)
        }
        fn load(&self) -> Result<LoadedClusterConfig, ClusterRuntimeError> {
            LoadedClusterConfig::load(&self.0)
        }
    }
    impl Drop for ConfigFile {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self.0);
        }
    }

    fn loaded() -> LoadedClusterConfig {
        ConfigFile::new(CONFIG).load().unwrap()
    }

    #[test]
    fn config_keeps_strict_placement_and_extension_boundaries() {
        assert!(ConfigFile::new(CONFIG).load().is_ok());
        for text in [
            CONFIG.replace("node_id = 1", "node_id = 1\nunknown = 1"),
            CONFIG.replace("mode = \"quorum\"", "mode = \"quorum\"\nunknown = 1"),
            CONFIG.replace("name = \"b\"", "name = \"b\"\nunknown = 1"),
            CONFIG.replace("heartbeat_ms = 100", "heartbeat_ms = 100\nunknown = 1"),
        ] {
            assert!(matches!(
                ConfigFile::new(&text).load(),
                Err(ClusterRuntimeError::InvalidConfig)
            ));
        }
    }

    #[test]
    fn standalone_without_extension_preserves_existing_configuration() {
        let text = "[cluster]\nid=\"single\"\nmode=\"standalone\"\n[node]\nid=\"local\"\n";
        let config = ConfigFile::new(text).load().unwrap();
        assert!(config.test_cluster.is_none());
        assert_eq!(config.placement.cluster.mode, ClusterMode::Standalone);
    }

    #[test]
    fn malformed_membership_routes_and_nonloopback_are_rejected() {
        for text in [
            CONFIG.replace("node_id = 1", "node_id = 0"),
            CONFIG.replace("id = 2", "id = 1"),
            CONFIG.replace("name = \"b\"", "name = \"a\""),
            CONFIG.replace("127.0.0.1:31002", "127.0.0.1:31001"),
            CONFIG.replace("127.0.0.1:31002", "127.0.0.1:0"),
            CONFIG.replace("127.0.0.1:31001", "0.0.0.0:31001"),
            CONFIG.replace("127.0.0.1:31003", "192.0.2.1:31003"),
            CONFIG.replace("primary = \"c\"", "primary = \"absent\""),
        ] {
            assert!(ConfigFile::new(&text).load().is_err());
        }
    }

    #[test]
    fn safety_timing_and_complete_canonical_shard_identity_are_required() {
        for text in [
            CONFIG.replace("lease_ms = 1500", "lease_ms = 499"),
            CONFIG.replace("lease_ms = 1500", "lease_ms = 60001"),
            CONFIG.replace("stop_margin_ms = 500", "stop_margin_ms = 0"),
            CONFIG.replace("stop_margin_ms = 500", "stop_margin_ms = 1500"),
            CONFIG.replace("heartbeat_ms = 100", "heartbeat_ms = 251"),
            CONFIG.replace("total_shards = 3", "total_shards = 4"),
            CONFIG.replace("total_shards = 3", "total_shards = \"auto\""),
            CONFIG.replace("bot_id = \"101\"", "bot_id = \"0101\""),
            CONFIG.replace("bot_id = \"101\"", "bot_id = \"arbitrary\""),
            CONFIG.replace("failback_stable_ms = 1200", "failback_stable_ms = 0"),
        ] {
            assert!(ConfigFile::new(&text).load().is_err());
        }
    }

    #[test]
    fn default_cluster_and_failover_timings_are_compatible() {
        let text = CONFIG
            .replace("heartbeat_ms = 100\n", "")
            .replace("suspect_after_ms = 300\n", "")
            .replace("unhealthy_after_ms = 600\n", "")
            .replace("failback_stable_ms = 1200\n", "")
            .replace("lease_ms = 1500\n", "")
            .replace("stop_margin_ms = 500\n", "");
        let loaded = ConfigFile::new(&text).load().unwrap();
        assert_eq!(loaded.placement.failover, placement::Failover::default());
        let cluster = loaded.test_cluster.as_ref().unwrap();
        assert_eq!(cluster.lease_ms, 4000);
        assert_eq!(cluster.stop_margin_ms, 1000);
        assert!(loaded.validate_test_cluster().is_ok());
    }

    #[test]
    fn relative_der_paths_resolve_from_toml_and_parent_traversal_is_rejected() {
        let file = ConfigFile::new(CONFIG);
        let loaded = file.load().unwrap();
        let config = loaded.test_cluster.unwrap();
        assert_eq!(config.cert_path, file.0.parent().unwrap().join("cert.der"));
        assert_eq!(config.key_path, file.0.parent().unwrap().join("key.der"));
        assert!(
            ConfigFile::new(&CONFIG.replace("key.der", "../key.der"))
                .load()
                .is_err()
        );
        assert!(
            ConfigFile::new(&CONFIG.replace("key.der", ""))
                .load()
                .is_err()
        );
    }

    #[test]
    fn local_launch_specs_include_only_explicit_primary_and_fallback() {
        let config = loaded();
        let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("target/cluster-specs");
        let specs = config.local_specs(&root).unwrap();
        assert_eq!(specs.len(), 2);
        assert_eq!(
            specs.iter().map(|spec| spec.shard_id).collect::<Vec<_>>(),
            [0, 1]
        );
        for spec in specs {
            assert!(spec.core.environment.credential_refs.is_empty());
            assert_eq!(spec.gateway.environment.credential_refs, ["TOKEN_ALPHA"]);
            assert!(spec.ipc_path.starts_with(&root));
        }
    }

    #[test]
    fn configuration_agreement_covers_safety_but_not_local_routes_or_cert_paths() {
        let config = loaded();
        let expected = config.configuration().unwrap();
        let mut same = config.clone();
        let ext = same.test_cluster.as_mut().unwrap();
        ext.listen.set_port(32001);
        ext.peers.reverse();
        ext.peers[0].address.set_port(32003);
        ext.key_path = PathBuf::from("another-private-key.der");
        assert_eq!(same.configuration().unwrap(), expected);
        for change in 0..5 {
            let mut different = config.clone();
            match change {
                0 => different.test_cluster.as_mut().unwrap().lease_ms += 1,
                1 => different.test_cluster.as_mut().unwrap().stop_margin_ms += 1,
                2 => different.placement.failover.failback_stable_ms += 1,
                3 => different.placement.cluster.id.push('2'),
                _ => different.placement.bots[0].shard_placement[0]
                    .failover
                    .reverse(),
            }
            assert_ne!(different.configuration().unwrap(), expected);
        }
        let mut other_node = config.clone();
        other_node.placement.node.id = "b".into();
        let ext = other_node.test_cluster.as_mut().unwrap();
        ext.node_id = 2;
        ext.peers[0].id = 1;
        ext.peers[0].name = "a".into();
        assert_eq!(other_node.configuration().unwrap(), expected);
    }

    #[tokio::test]
    async fn opt_in_and_mock_guards_precede_missing_certificate_io() {
        let config = loaded();
        assert!(matches!(
            ClusterRuntime::bind(&config, false, true).await,
            Err(ClusterRuntimeError::TestOnlyOptInRequired)
        ));
        assert!(matches!(
            ClusterRuntime::bind(&config, true, false).await,
            Err(ClusterRuntimeError::MockDiscordRequired)
        ));
        assert!(matches!(
            ClusterRuntime::bind(&config, true, true).await,
            Err(ClusterRuntimeError::CertificateRead)
        ));
    }

    #[test]
    fn bounded_parser_errors_never_echo_untrusted_source_or_paths() {
        let secret = "synthetic-pasted-credential-never-echo";
        let file = ConfigFile::new(&format!("invalid = {secret}"));
        let error = match file.load() {
            Err(error) => error,
            Ok(_) => panic!("expected invalid TOML"),
        };
        assert!(!format!("{error:?} {error}").contains(secret));
        let file = ConfigFile::new(&" ".repeat(MAX_CONFIG + 1));
        assert!(matches!(
            file.load(),
            Err(ClusterRuntimeError::ConfigTooLarge)
        ));
    }

    fn peer() -> RuntimePeer {
        RuntimePeer {
            name: "peer".into(),
            incarnation: 0,
            sequence: 0,
            last_seen: None,
            retired: BTreeSet::new(),
            exhausted: false,
            claims: BTreeSet::new(),
        }
    }

    #[test]
    fn heartbeat_replay_and_old_incarnation_do_not_refresh_health() {
        let mut peer = peer();
        assert_eq!(peer.observe(9, 1, Duration::from_millis(10)), Some(false));
        assert_eq!(peer.observe(9, 1, Duration::from_millis(20)), None);
        assert_eq!(peer.last_seen, Some(Duration::from_millis(10)));
        assert_eq!(peer.observe(2, 0, Duration::from_millis(20)), None);
        assert_eq!(peer.incarnation, 9);
        assert_eq!(peer.observe(2, 1, Duration::from_millis(30)), Some(true));
        assert_eq!(peer.observe(9, 100, Duration::from_millis(40)), None);
        assert_eq!(peer.last_seen, Some(Duration::from_millis(30)));
        assert_eq!(peer.observe(2, 2, Duration::from_millis(50)), Some(false));
    }

    #[test]
    fn excessive_incarnation_churn_fails_closed_with_bounded_memory() {
        let mut peer = peer();
        for id in 1..=(MAX_RETIRED_INCARNATIONS as u64 + 1) {
            assert!(peer.observe(id, 1, Duration::from_millis(id)).is_some());
        }
        let last_seen = peer.last_seen;
        assert!(peer.observe(1000, 1, Duration::from_secs(1)).is_none());
        assert!(peer.exhausted);
        assert_eq!(peer.retired.len(), MAX_RETIRED_INCARNATIONS);
        assert_eq!(peer.last_seen, last_seen);
    }

    #[test]
    fn failback_readiness_requires_continuous_primary_health_and_exact_threshold() {
        let clock = crate::cluster::ManualClock::default();
        let config = failover::Config {
            heartbeat_interval: Duration::from_millis(100),
            suspect_after: Duration::from_millis(300),
            fail_after: Duration::from_millis(600),
            stable_for: Duration::from_millis(1200),
            cooldown: Duration::from_millis(200),
            max_backoff: Duration::from_secs(2),
        };
        let mut policy = Failover::new(clock.clone(), config).unwrap();
        policy.handle(Event::Quorum(true)).unwrap();
        clock.set(Duration::from_millis(600)).unwrap();
        policy.handle(Event::Tick).unwrap();
        let attempt = policy.attempt();
        policy.handle(Event::OwnershipApproved { attempt }).unwrap();
        policy.handle(Event::StartupComplete { attempt }).unwrap();
        clock.set(Duration::from_millis(800)).unwrap();
        policy.handle(Event::Heartbeat).unwrap();
        assert_eq!(policy.state(), State::FailbackWaiting);
        for time in (900..2000).step_by(100) {
            clock.set(Duration::from_millis(time)).unwrap();
            policy.handle(Event::Heartbeat).unwrap();
            assert!(!policy.failback_ready());
        }
        clock.set(Duration::from_millis(2000)).unwrap();
        policy.handle(Event::Heartbeat).unwrap();
        assert!(policy.failback_ready());
        clock.set(Duration::from_millis(2300)).unwrap();
        assert!(!policy.failback_ready());
        policy.handle(Event::Tick).unwrap();
        assert_eq!(policy.state(), State::TemporaryActive);
    }
}
