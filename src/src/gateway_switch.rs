//! Pure, shard-scoped Gateway rolling update protocol; no transport or consensus.
//!
//! The driver must authenticate messages, serialize this machine with admission
//! and routing, and execute effects in order. A Pause/Quiesce ACK is a barrier:
//! the peer has closed admission and will reject delayed controls through the
//! indicated epoch. Spawn/Identify always create a Standby with command execution
//! disabled. WarmReady certifies Discord READY and required initial state.
//!
//! Epochs come ONLY from OwnershipCoordinator, after the opposite admission gate
//! has stopped. The driver must check the live lease for every `admit_work` and
//! deliver `AuthorityLost` on fencing/expiry/watchdog failure. Epoch comparison
//! here is not a lease check. Across hosts, quorum approval and safe lease expiry
//! or an authenticated stop barrier are still required by ownership.rs.
//!
//! Work admitted before Pause may register further REST operations until it is
//! finished. REST results retain their original Core route across epoch changes.
//! No response payloads or credentials are stored. IDs/tombstones are retained
//! for this machine's lifetime; a production driver needs a bounded, durable
//! retention policy and must never reuse IDs while delayed messages can survive.
//! This protocol does not provide cross-session event deduplication or buffering,
//! persistence, actual process termination, or exactly-once Discord side effects.

use crate::{
    ownership::OwnershipKey,
    protocol::{GenerationId, OperationId, OwnershipEpoch, RequestId},
};
use std::{collections::HashMap, time::Duration};

/// Explicit simulation input, never fetched from or substituted for Discord.
/// One scheduler in the daemon covers initial launches AND rolling candidates
/// across all local shards. Reservations are spent before spawn and not refunded
/// on failure: an uncertain session start must not create extra quota.
#[derive(Clone, Debug, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MockIdentifyLimit {
    pub bot_id: u64,
    pub total: u64,
    pub remaining: u64,
    pub reset_after_ms: u64,
    pub max_concurrency: u32,
    pub interval_ms: u64,
}

struct MockBudget {
    limit: MockIdentifyLimit,
    reset_at: Duration,
    buckets: HashMap<u32, Duration>,
}

/// Local mock-only reservation accounting. No distributed limiter or durable
/// budget is claimed; production and quorum launchers must not use this type.
pub struct MockIdentifyScheduler {
    bots: HashMap<u64, MockBudget>,
    last_now: Duration,
}

impl MockIdentifyScheduler {
    pub fn new(limits: Vec<MockIdentifyLimit>) -> Result<Self, &'static str> {
        let mut bots = HashMap::new();
        for limit in limits {
            if limit.total == 0
                || limit.remaining > limit.total
                || limit.reset_after_ms == 0
                || limit.max_concurrency == 0
                || limit.interval_ms == 0
                || bots.contains_key(&limit.bot_id)
            {
                return Err("invalid mock Identify budget");
            }
            bots.insert(
                limit.bot_id,
                MockBudget {
                    reset_at: Duration::from_millis(limit.reset_after_ms),
                    limit,
                    buckets: HashMap::new(),
                },
            );
        }
        if bots.is_empty() {
            return Err("mock Identify budget is empty");
        }
        Ok(Self {
            bots,
            last_now: Duration::ZERO,
        })
    }

    pub fn contains_bot(&self, bot: u64) -> bool {
        self.bots.contains_key(&bot)
    }

    pub fn reserve(
        &mut self,
        bot: u64,
        shard: u32,
        now: Duration,
    ) -> Result<IdentifyDecision, &'static str> {
        if now < self.last_now {
            return Err("mock Identify clock regressed");
        }
        self.last_now = now;
        let budget = self
            .bots
            .get_mut(&bot)
            .ok_or("missing mock Identify budget")?;
        if now >= budget.reset_at {
            budget.reset_at = now
                .checked_add(Duration::from_millis(budget.limit.reset_after_ms))
                .ok_or("mock Identify clock overflow")?;
            budget.limit.remaining = budget.limit.total;
        }
        if budget.limit.remaining == 0 {
            return Ok(IdentifyDecision::Defer(IdentifyDeferral {
                reason: IdentifyBlock::BudgetExhausted,
                retry_after: budget.reset_at.saturating_sub(now),
            }));
        }
        let bucket = shard % budget.limit.max_concurrency;
        if let Some(until) = budget.buckets.get(&bucket).filter(|until| **until > now) {
            return Ok(IdentifyDecision::Defer(IdentifyDeferral {
                reason: IdentifyBlock::RateLimited,
                retry_after: *until - now,
            }));
        }
        let until = now
            .checked_add(Duration::from_millis(budget.limit.interval_ms))
            .ok_or("mock Identify clock overflow")?;
        budget.limit.remaining -= 1;
        budget.buckets.insert(bucket, until);
        Ok(IdentifyDecision::Reserved)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub enum Phase {
    Stable,
    Deferred,
    Spawning,
    Warming,
    PausingOld,
    AwaitingCandidateAuthority,
    ActivatingCandidate,
    DrainingOld,
    QuiescingCandidate,
    AwaitingOldAuthority,
    ActivatingOld,
    DrainingCandidate,
    RecoveryRequired,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Authority {
    pub generation: GenerationId,
    pub epoch: OwnershipEpoch,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IdentifyDecision {
    /// A reservation from the shared Bot-level Identify scheduler, not a quota
    /// snapshot. The driver owns reservation consumption/release and rate limits.
    Reserved,
    Defer(IdentifyDeferral),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct IdentifyDeferral {
    pub reason: IdentifyBlock,
    pub retry_after: Duration,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IdentifyBlock {
    BudgetExhausted,
    RateLimited,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Failure {
    GatewayFailed,
    ReadyNotReached,
    Cancelled,
    IdentifyLimited(IdentifyDeferral),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Diagnostic {
    IdentifyDeferred(IdentifyDeferral),
    UpdateAborted(Failure),
    AuthorityUnavailable,
    TimedOut(Phase),
    RecoveryRequired,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RestOrigin {
    pub core_generation: GenerationId,
    pub request_id: RequestId,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RestRoute {
    pub gateway_generation: GenerationId,
    pub origin: RestOrigin,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Effect {
    SpawnStandby(GenerationId),
    IdentifyStandby(GenerationId),
    PauseOld(Authority),
    RequestAuthority {
        generation: GenerationId,
        after_epoch: OwnershipEpoch,
    },
    Activate(Authority),
    /// Cancel pending spawn/identify/activate as well as closing admission.
    /// ACK must guarantee no delayed activation through this epoch can reopen it.
    Quiesce(Authority),
    /// Epoch identifies this drain attempt, including repeated drains after rollback.
    Drain(Authority),
    Stop(GenerationId),
    RouteRestResult {
        operation_id: OperationId,
        route: RestRoute,
    },
    Diagnostic(Diagnostic),
    RecoverShard,
}

/// Match timeouts to the exact wait, so an old timer cannot abort a newer phase.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Wait {
    pub phase: Phase,
    pub generation: GenerationId,
    pub epoch: OwnershipEpoch,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Event {
    Begin {
        candidate: GenerationId,
        identify: IdentifyDecision,
    },
    RetryIdentify(IdentifyDecision),
    Spawned(GenerationId),
    WarmReady(GenerationId),
    OldPaused(Authority),
    /// The driver has acquired and validated this epoch with its coordinator.
    AuthorityGranted(Authority),
    AuthorityDenied(GenerationId),
    RetryAuthority,
    ActiveAck(Authority),
    CandidateQuiesced(Authority),
    Failed {
        generation: GenerationId,
        reason: Failure,
    },
    Timeout(Wait),
    /// Local fencing/lease loss is never an instruction to restore old authority.
    AuthorityLost,
    WorkFinished {
        generation: GenerationId,
        request_id: RequestId,
    },
    RestResult {
        generation: GenerationId,
        operation_id: OperationId,
    },
    RestResultAck {
        generation: GenerationId,
        operation_id: OperationId,
    },
    /// Certifies no remaining producers or untracked work for this generation.
    DrainAck(Authority),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Error {
    WrongShard,
    InvalidPhase,
    WrongGeneration,
    StaleEpoch,
    GenerationReused,
    AdmissionClosed,
    DuplicateRequest,
    UnknownRequest,
    WorkFinished,
    DuplicateOperation,
    UnknownOperation,
    ResultNotReceived,
    UnfinishedWork,
    StaleTimeout,
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "gateway switch error: {self:?}")
    }
}
impl std::error::Error for Error {}

#[derive(Clone, Debug, Eq, PartialEq)]
struct Work {
    route: RestRoute,
    finished: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct Operation {
    route: RestRoute,
    received: bool,
    delivered: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GatewaySwitch {
    key: OwnershipKey,
    old: GenerationId,
    candidate: Option<GenerationId>,
    last_generation: GenerationId,
    epoch: OwnershipEpoch,
    active: Option<Authority>,
    phase: Phase,
    drain_acked: bool,
    restore_old: bool,
    work: HashMap<RequestId, Work>,
    operations: HashMap<OperationId, Operation>,
}

impl GatewaySwitch {
    /// Initial authority must already have been checked with the coordinator.
    pub fn new(key: OwnershipKey, initial: Authority) -> Self {
        Self {
            key,
            old: initial.generation,
            candidate: None,
            last_generation: initial.generation,
            epoch: initial.epoch,
            active: Some(initial),
            phase: Phase::Stable,
            drain_acked: false,
            restore_old: false,
            work: HashMap::new(),
            operations: HashMap::new(),
        }
    }

    pub fn key(&self) -> OwnershipKey {
        self.key
    }

    pub fn phase(&self) -> Phase {
        self.phase
    }

    /// Includes activation sent but not yet acknowledged: it may already execute.
    pub fn active(&self) -> Option<Authority> {
        self.active
    }

    pub fn epoch(&self) -> OwnershipEpoch {
        self.epoch
    }

    pub fn wait(&self) -> Option<Wait> {
        let generation = match self.phase {
            Phase::Stable | Phase::Deferred | Phase::RecoveryRequired => return None,
            Phase::PausingOld
            | Phase::AwaitingOldAuthority
            | Phase::ActivatingOld
            | Phase::DrainingOld => self.old,
            _ => self.candidate?,
        };
        Some(Wait {
            phase: self.phase,
            generation,
            epoch: self.epoch,
        })
    }

    fn scope(&self, key: OwnershipKey) -> Result<(), Error> {
        if key == self.key {
            Ok(())
        } else {
            Err(Error::WrongShard)
        }
    }

    fn candidate_is(&self, generation: GenerationId) -> Result<(), Error> {
        if self.candidate == Some(generation) {
            Ok(())
        } else {
            Err(Error::WrongGeneration)
        }
    }

    fn exact(&self, authority: Authority, generation: GenerationId) -> Result<(), Error> {
        if authority.generation != generation {
            return Err(Error::WrongGeneration);
        }
        if authority.epoch != self.epoch {
            return Err(Error::StaleEpoch);
        }
        Ok(())
    }

    fn request_authority(&self, generation: GenerationId) -> Effect {
        Effect::RequestAuthority {
            generation,
            after_epoch: self.epoch,
        }
    }

    /// Must be called under the same lock as routing and with a live lease check.
    pub fn admit_work(
        &mut self,
        key: OwnershipKey,
        authority: Authority,
        origin: RestOrigin,
    ) -> Result<(), Error> {
        self.scope(key)?;
        if self.active != Some(authority)
            || (self.phase == Phase::QuiescingCandidate && self.restore_old)
        {
            return Err(Error::AdmissionClosed);
        }
        if self.work.contains_key(&origin.request_id) {
            return Err(Error::DuplicateRequest);
        }
        self.work.insert(
            origin.request_id,
            Work {
                route: RestRoute {
                    gateway_generation: authority.generation,
                    origin,
                },
                finished: false,
            },
        );
        Ok(())
    }

    /// Existing work can emit REST even after its Gateway was paused/fenced.
    /// This does not authorize new commands or retry an unknown REST side effect.
    pub fn register_rest(
        &mut self,
        key: OwnershipKey,
        request_id: RequestId,
        operation_id: OperationId,
    ) -> Result<RestRoute, Error> {
        self.scope(key)?;
        let work = self.work.get(&request_id).ok_or(Error::UnknownRequest)?;
        if work.finished {
            return Err(Error::WorkFinished);
        }
        if self.operations.contains_key(&operation_id) {
            return Err(Error::DuplicateOperation);
        }
        let route = work.route;
        self.operations.insert(
            operation_id,
            Operation {
                route,
                received: false,
                delivered: false,
            },
        );
        Ok(route)
    }

    pub fn rest_route(&self, operation_id: OperationId) -> Option<RestRoute> {
        self.operations.get(&operation_id).map(|op| op.route)
    }

    pub fn pending_rest(&self, generation: GenerationId) -> usize {
        self.operations
            .values()
            .filter(|op| op.route.gateway_generation == generation && !op.delivered)
            .count()
    }

    fn abort(&mut self, reason: Diagnostic) -> Vec<Effect> {
        // PausingOld is uncertain: the Pause may have applied despite ACK loss.
        self.restore_old = !matches!(self.phase, Phase::Spawning | Phase::Warming);
        // Until the barrier ACK, a candidate sent Activate may still execute.
        // Never advertise the old generation as restored before that barrier.
        self.phase = Phase::QuiescingCandidate;
        vec![
            Effect::Diagnostic(reason),
            Effect::Quiesce(Authority {
                generation: self.candidate.expect("candidate during update"),
                epoch: self.epoch,
            }),
        ]
    }

    fn recover(&mut self) -> Vec<Effect> {
        self.active = None;
        self.phase = Phase::RecoveryRequired;
        let mut effects = vec![Effect::Quiesce(Authority {
            generation: self.old,
            epoch: self.epoch,
        })];
        if let Some(generation) = self.candidate {
            effects.push(Effect::Quiesce(Authority {
                generation,
                epoch: self.epoch,
            }));
        }
        effects.extend([
            Effect::Diagnostic(Diagnostic::RecoveryRequired),
            Effect::RecoverShard,
        ]);
        effects
    }

    fn retiring(&self) -> Option<GenerationId> {
        match self.phase {
            Phase::DrainingOld => Some(self.old),
            Phase::DrainingCandidate => self.candidate,
            _ => None,
        }
    }

    fn finish_if_safe(&mut self, effects: &mut Vec<Effect>) {
        let Some(retiring) = self.retiring() else {
            return;
        };
        if !self.drain_acked || self.pending_rest(retiring) != 0 {
            return;
        }
        // DrainAck was accepted only after every tracked producer finished.
        effects.push(Effect::Stop(retiring));
        if self.phase == Phase::DrainingOld {
            self.old = self.candidate.expect("candidate while draining old");
        }
        self.candidate = None;
        self.drain_acked = false;
        self.phase = Phase::Stable;
    }

    /// Rejects stale/foreign events without mutation. Effects contain metadata
    /// only. Timeouts are explicit inputs; waiting does not perform I/O or tick.
    pub fn handle(&mut self, key: OwnershipKey, event: Event) -> Result<Vec<Effect>, Error> {
        self.scope(key)?;
        let mut effects = Vec::new();
        match event {
            Event::Begin {
                candidate,
                identify,
            } => {
                if self.phase != Phase::Stable {
                    return Err(Error::InvalidPhase);
                }
                if candidate.0 <= self.last_generation.0 {
                    return Err(Error::GenerationReused);
                }
                self.candidate = Some(candidate);
                self.last_generation = candidate;
                self.identify(identify, &mut effects);
            }
            Event::RetryIdentify(identify) => {
                if self.phase != Phase::Deferred {
                    return Err(Error::InvalidPhase);
                }
                self.identify(identify, &mut effects);
            }
            Event::Spawned(generation) => {
                self.candidate_is(generation)?;
                if self.phase != Phase::Spawning {
                    return Err(Error::InvalidPhase);
                }
                self.phase = Phase::Warming;
                effects.push(Effect::IdentifyStandby(generation));
            }
            Event::WarmReady(generation) => {
                self.candidate_is(generation)?;
                if self.phase != Phase::Warming {
                    return Err(Error::InvalidPhase);
                }
                self.phase = Phase::PausingOld;
                effects.push(Effect::PauseOld(
                    self.active.expect("old active until Pause ACK"),
                ));
            }
            Event::OldPaused(authority) => {
                self.exact(authority, self.old)?;
                if self.phase != Phase::PausingOld {
                    return Err(Error::InvalidPhase);
                }
                self.active = None;
                self.phase = Phase::AwaitingCandidateAuthority;
                effects
                    .push(self.request_authority(self.candidate.expect("candidate after warmup")));
            }
            Event::AuthorityGranted(authority) => {
                let target = match self.phase {
                    Phase::AwaitingCandidateAuthority => {
                        self.candidate.expect("candidate awaiting grant")
                    }
                    Phase::AwaitingOldAuthority => self.old,
                    _ => return Err(Error::InvalidPhase),
                };
                if authority.generation != target {
                    return Err(Error::WrongGeneration);
                }
                if authority.epoch.0 <= self.epoch.0 {
                    return Err(Error::StaleEpoch);
                }
                self.epoch = authority.epoch;
                self.active = Some(authority);
                self.phase = if target == self.old {
                    Phase::ActivatingOld
                } else {
                    Phase::ActivatingCandidate
                };
                effects.push(Effect::Activate(authority));
            }
            Event::AuthorityDenied(generation) => match self.phase {
                Phase::AwaitingCandidateAuthority => {
                    self.candidate_is(generation)?;
                    effects = self.abort(Diagnostic::AuthorityUnavailable);
                }
                Phase::AwaitingOldAuthority if generation == self.old => {
                    effects.push(Effect::Diagnostic(Diagnostic::AuthorityUnavailable))
                }
                _ => return Err(Error::InvalidPhase),
            },
            Event::RetryAuthority => {
                let target = match self.phase {
                    Phase::AwaitingCandidateAuthority => {
                        self.candidate.expect("candidate awaiting grant")
                    }
                    Phase::AwaitingOldAuthority => self.old,
                    _ => return Err(Error::InvalidPhase),
                };
                effects.push(self.request_authority(target));
            }
            Event::ActiveAck(authority) => {
                let retiring = match self.phase {
                    Phase::ActivatingCandidate => {
                        self.exact(authority, self.candidate.expect("candidate activating"))?;
                        self.old
                    }
                    Phase::ActivatingOld => {
                        self.exact(authority, self.old)?;
                        self.candidate.expect("abandoned candidate")
                    }
                    _ => return Err(Error::InvalidPhase),
                };
                self.phase = if retiring == self.old {
                    Phase::DrainingOld
                } else {
                    Phase::DrainingCandidate
                };
                self.drain_acked = false;
                effects.push(Effect::Drain(Authority {
                    generation: retiring,
                    epoch: self.epoch,
                }));
            }
            Event::CandidateQuiesced(authority) => {
                self.exact(authority, self.candidate.ok_or(Error::WrongGeneration)?)?;
                if self.phase != Phase::QuiescingCandidate {
                    return Err(Error::InvalidPhase);
                }
                if !self.restore_old {
                    self.phase = Phase::DrainingCandidate;
                    self.drain_acked = false;
                    effects.push(Effect::Drain(authority));
                } else {
                    self.active = None;
                    self.phase = Phase::AwaitingOldAuthority;
                    effects.push(self.request_authority(self.old));
                }
            }
            Event::Failed { generation, reason } => {
                if self.phase == Phase::RecoveryRequired {
                    return Err(Error::InvalidPhase);
                }
                if generation == self.old {
                    effects = self.recover();
                } else {
                    self.candidate_is(generation)?;
                    if self.phase == Phase::Deferred {
                        self.candidate = None;
                        self.phase = Phase::Stable;
                        effects.push(Effect::Diagnostic(Diagnostic::UpdateAborted(reason)));
                    } else if matches!(
                        self.phase,
                        Phase::Spawning
                            | Phase::Warming
                            | Phase::PausingOld
                            | Phase::AwaitingCandidateAuthority
                            | Phase::ActivatingCandidate
                            | Phase::DrainingOld
                    ) {
                        let diagnostic = match reason {
                            Failure::IdentifyLimited(deferral) => {
                                Diagnostic::IdentifyDeferred(deferral)
                            }
                            _ => Diagnostic::UpdateAborted(reason),
                        };
                        effects = self.abort(diagnostic);
                    } else {
                        return Err(Error::InvalidPhase);
                    }
                }
            }
            Event::Timeout(wait) => {
                if self.wait() != Some(wait) {
                    return Err(Error::StaleTimeout);
                }
                if matches!(
                    self.phase,
                    Phase::Spawning
                        | Phase::Warming
                        | Phase::PausingOld
                        | Phase::AwaitingCandidateAuthority
                        | Phase::ActivatingCandidate
                ) {
                    effects = self.abort(Diagnostic::TimedOut(wait.phase));
                } else if self.phase == Phase::ActivatingOld {
                    effects = self.recover();
                } else {
                    // Neither a missing stop barrier nor an unknown REST result
                    // can be turned into a successful drain by a timer.
                    effects.push(Effect::Diagnostic(Diagnostic::TimedOut(wait.phase)));
                }
            }
            Event::AuthorityLost => effects = self.recover(),
            Event::WorkFinished {
                generation,
                request_id,
            } => {
                let work = self
                    .work
                    .get_mut(&request_id)
                    .ok_or(Error::UnknownRequest)?;
                if work.route.gateway_generation != generation {
                    return Err(Error::WrongGeneration);
                }
                if work.finished {
                    return Err(Error::WorkFinished);
                }
                work.finished = true;
            }
            Event::RestResult {
                generation,
                operation_id,
            } => {
                let operation = self
                    .operations
                    .get_mut(&operation_id)
                    .ok_or(Error::UnknownOperation)?;
                if operation.route.gateway_generation != generation {
                    return Err(Error::WrongGeneration);
                }
                operation.received = true;
                if !operation.delivered {
                    effects.push(Effect::RouteRestResult {
                        operation_id,
                        route: operation.route,
                    });
                }
            }
            Event::RestResultAck {
                generation,
                operation_id,
            } => {
                let operation = self
                    .operations
                    .get_mut(&operation_id)
                    .ok_or(Error::UnknownOperation)?;
                if operation.route.gateway_generation != generation {
                    return Err(Error::WrongGeneration);
                }
                if !operation.received {
                    return Err(Error::ResultNotReceived);
                }
                operation.delivered = true;
            }
            Event::DrainAck(authority) => {
                let generation = authority.generation;
                if self.retiring() != Some(generation) {
                    return Err(Error::InvalidPhase);
                }
                self.exact(authority, generation)?;
                if self
                    .work
                    .values()
                    .any(|work| work.route.gateway_generation == generation && !work.finished)
                {
                    return Err(Error::UnfinishedWork);
                }
                self.drain_acked = true;
            }
        }
        self.finish_if_safe(&mut effects);
        Ok(effects)
    }

    fn identify(&mut self, identify: IdentifyDecision, effects: &mut Vec<Effect>) {
        match identify {
            IdentifyDecision::Reserved => {
                self.phase = Phase::Spawning;
                effects.push(Effect::SpawnStandby(
                    self.candidate.expect("candidate requesting identify"),
                ));
            }
            IdentifyDecision::Defer(deferral) => {
                self.phase = Phase::Deferred;
                effects.push(Effect::Diagnostic(Diagnostic::IdentifyDeferred(deferral)));
            }
        }
    }
}
