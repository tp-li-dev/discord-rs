//! Local, poll-driven recovery of owned Gateway/Core OS children.
//!
//! One slot per (Bot, Shard, role), one owned handle per slot. Only a successful
//! `Child::try_wait` returning an exit permits replacing that child. EOF, failed
//! health checks, kill requests and wait errors are NOT proof of termination.
//! The caller must keep polling, also while stopping or after a clock failure.
//!
//! This is not an ownership coordinator, process adoption, an updater or a
//! Discord client. `RecoveryHost` is the trusted admission/spawn boundary. Keep
//! one engine per local owner, serialize it with management/update admission,
//! and retain existing quorum/lease/watchdog and authenticated IPC checks. An OS
//! child running here does not mean Ready or Active. Core recovery never changes
//! a Gateway slot, its connection, or its ownership epoch.
//!
//! No command, environment, credential, session, application error or output is
//! retained. Events are returned to the caller for metrics/audit sinks; they are
//! not persisted here. Restart history is in memory and must not be reset by
//! reconstructing the engine while old processes/reservations can survive.
//! Dropping this engine does not implement graceful shutdown or child adoption;
//! the host's Tokio kill-on-drop setting still applies to its child handles.

use crate::{
    protocol::BotId,
    supervisor::{CircuitState, Clock, MonotonicClock, RestartPolicy, RestartStatus},
};
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, collections::VecDeque, io, time::Duration};
use tokio::process::Child;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub enum ChildRole {
    Gateway,
    Core,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Serialize, Deserialize)]
pub struct ChildKey {
    pub bot_id: BotId,
    pub shard_id: u32,
    pub role: ChildRole,
}

#[derive(Clone, Copy, Debug)]
pub struct RecoveryConfig {
    pub restart: RestartPolicy,
    /// Continuous observed liveness AFTER authenticated Ready, before resetting
    /// consecutive failures/closing a half-open circuit. Never resets budgets.
    pub stable_for: Duration,
    /// Consecutive exits (or spawn failures) without observed stable readiness.
    pub max_startup_failures: u32,
}

impl Default for RecoveryConfig {
    fn default() -> Self {
        Self {
            restart: RestartPolicy::default(),
            stable_for: Duration::from_secs(30),
            max_startup_failures: 5,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RecoveryError {
    InvalidConfig,
    Capacity,
    DuplicateChild,
    UnknownChild,
    StaleGeneration,
    NotRunning,
    NotStopped,
    ClockFailed,
}

impl std::fmt::Display for RecoveryError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "child recovery error: {self:?}")
    }
}
impl std::error::Error for RecoveryError {}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum DegradedReason {
    RestartBudget,
    RestartWindow,
    StartupCrashLoop,
    MonitorFailed,
    ClockRegressed,
    TimeOverflow,
    GenerationExhausted,
    IdentifyUnavailable,
    LaunchRejected,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum RecoveryState {
    /// OS child only; not an assertion of Ready, Active, lease or WS health.
    Running,
    Waiting,
    Stopping,
    Stopped,
    /// Latched: neither window expiry nor circuit cooldown clears this state.
    /// A monitor/clock fault can leave a live handle, which must still be reaped.
    Degraded(DegradedReason),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum StartMode {
    Core,
    /// Resume only. Rejection MUST NOT silently fall back to an Identify.
    /// Reap this attempt and invalidate the host's resume hint before retrying.
    GatewayResume,
    GatewayIdentify,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct StartRequest {
    pub key: ChildKey,
    /// Local process generation, not an ownership epoch or a durable allocator.
    pub generation: u64,
    pub mode: StartMode,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IdentifyAdmission {
    /// An actual, consumed admission from the shared Bot budget/queue, not a
    /// session_start_limit snapshot or a predicted reset. Never refunded here.
    Granted,
    Pending,
    /// No safe shared coordinator/queue is configured, or it has failed.
    Unavailable,
}

/// All methods are nonblocking and run in the serialized local control owner.
/// This trait does not authorize remote callers. There is deliberately no
/// permissive/default implementation.
pub trait RecoveryHost {
    /// Revalidate existing ownership/lease and management admission now. This
    /// must not acquire a new epoch merely because a Core needs restarting.
    /// Existing live children still need the independent lease watchdog.
    fn authorized(&mut self, key: ChildKey) -> bool;

    /// Only a boolean hint: keep Resume secrets in the Gateway's protected path.
    fn resume_available(&mut self, key: ChildKey) -> bool;

    /// Use the SAME Bot-wide Identify coordinator as initial starts and updates.
    /// A local implementation can poll a pending IdentifyRegistry queue receiver;
    /// separate processes/hosts need coordinated allocations. REST admission for
    /// metadata refresh uses rest_budget separately, never as an Identify permit.
    fn try_acquire_identify(&mut self, key: ChildKey) -> IdentifyAdmission;

    /// Cancel any pending queue request on hold/stop/authority loss. Admissions
    /// already consumed remain spent. Must be idempotent and not block.
    fn cancel_identify(&mut self, key: ChildKey);

    /// Spawn promptly, in isolation (env_clear; Core never receives Bot tokens).
    /// Return the child even if later Hello/Ready fails: an Err MUST mean no
    /// child exists. Retain control pipes outside this engine as needed.
    ///
    /// The Gateway adapter must enforce Resume-only and coordinate the actual
    /// Identify send, including its bucket interval. A process-start grant alone
    /// cannot guarantee timing of a delayed WebSocket handshake. Revalidate at
    /// that boundary, with no unbudgeted fallback or independent fresh queue.
    fn spawn(&mut self, request: StartRequest) -> io::Result<Child>;
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum DeferralReason {
    ManagementHold,
    Ownership,
    IdentifyBudget,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum RecoveryEventKind {
    Exited { success: bool, code: Option<i32> },
    SpawnFailed,
    StartupFailed { consecutive: u32 },
    RestartScheduled { retry_at: Duration },
    CircuitOpened { until: Duration },
    CircuitHalfOpen,
    Started { mode: StartMode },
    Stable,
    Deferred { reason: DeferralReason },
    Stopped,
    Degraded { reason: DegradedReason },
}

/// Fixed enums and identifiers only; safe to map into bounded metric labels and
/// structured audit records. Do not append raw OS/application error text.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct RecoveryEvent {
    pub key: ChildKey,
    pub generation: u64,
    pub at: Duration,
    pub kind: RecoveryEventKind,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RecoverySnapshot {
    pub key: ChildKey,
    pub generation: u64,
    pub state: RecoveryState,
    pub pid: Option<u32>,
    pub restart: RestartStatus,
    pub startup_failures: u32,
    pub stable: bool,
    pub held: bool,
}

struct Entry {
    key: ChildKey,
    generation: u64,
    generation_floor: u64,
    child: Option<Child>,
    state: RecoveryState,
    restart: RestartStatus,
    recent: VecDeque<Duration>,
    attempted: bool,
    desired: bool,
    held: bool,
    ready_at: Option<Duration>,
    stable: bool,
    startup_failures: u32,
    deferred: Option<DeferralReason>,
    // Preserve an already scheduled delay through explicit stop/start too.
    not_before: Duration,
}

impl Entry {
    fn emit(&self, at: Duration, kind: RecoveryEventKind, events: &mut Vec<RecoveryEvent>) {
        events.push(RecoveryEvent {
            key: self.key,
            generation: self.generation,
            at,
            kind,
        });
    }

    fn degrade(&mut self, at: Duration, reason: DegradedReason, events: &mut Vec<RecoveryEvent>) {
        if matches!(self.state, RecoveryState::Degraded(_)) {
            return;
        }
        self.state = RecoveryState::Degraded(reason);
        self.restart.retry_at = None;
        self.ready_at = None;
        self.stable = false;
        self.restart.exhausted = matches!(
            reason,
            DegradedReason::RestartBudget | DegradedReason::RestartWindow
        );
        self.emit(at, RecoveryEventKind::Degraded { reason }, events);
    }

    fn defer(&mut self, at: Duration, reason: DeferralReason, events: &mut Vec<RecoveryEvent>) {
        if self.deferred != Some(reason) {
            self.deferred = Some(reason);
            self.emit(at, RecoveryEventKind::Deferred { reason }, events);
        }
    }

    fn budget_available(
        &mut self,
        now: Duration,
        policy: RestartPolicy,
        events: &mut Vec<RecoveryEvent>,
    ) -> bool {
        if !self.attempted {
            return true;
        }
        while self
            .recent
            .front()
            .is_some_and(|at| now.saturating_sub(*at) >= policy.window)
        {
            self.recent.pop_front();
        }
        let reason = if self.restart.restarts >= policy.max_restarts {
            Some(DegradedReason::RestartBudget)
        } else if self.recent.len() >= policy.max_restarts_in_window as usize {
            Some(DegradedReason::RestartWindow)
        } else {
            None
        };
        if let Some(reason) = reason {
            self.degrade(now, reason, events);
            return false;
        }
        true
    }

    fn failed(&mut self, now: Duration, config: RecoveryConfig, events: &mut Vec<RecoveryEvent>) {
        self.restart.failures = self.restart.failures.saturating_add(1);
        if !self.stable {
            self.startup_failures = self.startup_failures.saturating_add(1);
            self.emit(
                now,
                RecoveryEventKind::StartupFailed {
                    consecutive: self.startup_failures,
                },
                events,
            );
        }
        self.ready_at = None;
        self.stable = false;
        if !self.budget_available(now, config.restart, events) {
            return;
        }
        if self.startup_failures >= config.max_startup_failures {
            self.degrade(now, DegradedReason::StartupCrashLoop, events);
            return;
        }
        let Some(mut retry_at) = now.checked_add(config.restart.backoff(self.restart.restarts))
        else {
            self.degrade(now, DegradedReason::TimeOverflow, events);
            return;
        };
        if self.restart.failures >= config.restart.circuit_breaker_after {
            let Some(until) = now.checked_add(config.restart.circuit_open_for) else {
                self.degrade(now, DegradedReason::TimeOverflow, events);
                return;
            };
            self.restart.circuit = CircuitState::Open { until };
            retry_at = retry_at.max(until);
            self.emit(now, RecoveryEventKind::CircuitOpened { until }, events);
        }
        self.state = RecoveryState::Waiting;
        self.restart.retry_at = Some(retry_at);
        self.not_before = retry_at;
        self.deferred = None;
        self.emit(
            now,
            RecoveryEventKind::RestartScheduled { retry_at },
            events,
        );
    }
}

/// No background task is spawned. Drive `poll` from the daemon/Gateway owner's
/// existing timer. Slot records (including stopped/degraded records) are retained
/// against capacity; polling returns only the current tick's transition events.
pub struct ChildRecovery<C = MonotonicClock> {
    clock: C,
    config: RecoveryConfig,
    capacity: usize,
    entries: HashMap<ChildKey, Entry>,
    last_now: Duration,
    clock_failed: bool,
    legacy_process_exits: bool,
    wait_for_window: bool,
}

impl<C: Clock> ChildRecovery<C> {
    pub fn new(clock: C, config: RecoveryConfig, capacity: usize) -> Result<Self, RecoveryError> {
        if config.restart.validate().is_err()
            || config.stable_for.is_zero()
            || config.max_startup_failures == 0
            || capacity == 0
        {
            return Err(RecoveryError::InvalidConfig);
        }
        let last_now = clock.now();
        Ok(Self {
            clock,
            config,
            capacity,
            entries: HashMap::new(),
            last_now,
            clock_failed: false,
            legacy_process_exits: false,
            wait_for_window: false,
        })
    }

    /// Compatibility for the original process-only daemon contract: code zero
    /// is a cooperative shutdown, and its window limit postpones retries. The
    /// managed Gateway/Core adapters retain the strict service defaults.
    pub(crate) fn use_legacy_process_contract(&mut self) {
        self.legacy_process_exits = true;
        self.wait_for_window = true;
    }

    /// Stop retrying a non-retryable host setup failure, retaining an observed
    /// live/unknown handle. Only fixed reasons may leave this boundary.
    pub(crate) fn fail_closed(
        &mut self,
        key: ChildKey,
        reason: DegradedReason,
    ) -> Result<Vec<RecoveryEvent>, RecoveryError> {
        let entry = self
            .entries
            .get_mut(&key)
            .ok_or(RecoveryError::UnknownChild)?;
        let mut events = Vec::new();
        entry.degrade(self.last_now, reason, &mut events);
        Ok(events)
    }

    /// Register a stopped, reconciled slot, then let poll perform its first
    /// admitted start. There MUST be no previous live or unknown child/reservation
    /// for this key. `after_generation` comes from the owner's generation
    /// allocator. No existing slot can be replaced to erase its restart history.
    pub fn register(&mut self, key: ChildKey, after_generation: u64) -> Result<(), RecoveryError> {
        if self.clock_failed {
            return Err(RecoveryError::ClockFailed);
        }
        if self.entries.contains_key(&key) {
            return Err(RecoveryError::DuplicateChild);
        }
        if self.entries.len() >= self.capacity {
            return Err(RecoveryError::Capacity);
        }
        self.entries.insert(
            key,
            Entry {
                key,
                generation: after_generation,
                generation_floor: after_generation,
                child: None,
                state: RecoveryState::Waiting,
                restart: RestartStatus {
                    restarts: 0,
                    failures: 0,
                    retry_at: Some(self.last_now),
                    circuit: CircuitState::Closed,
                    exhausted: false,
                },
                recent: VecDeque::new(),
                attempted: false,
                desired: true,
                held: false,
                ready_at: None,
                stable: false,
                startup_failures: 0,
                deferred: None,
                not_before: Duration::ZERO,
            },
        );
        Ok(())
    }

    pub fn snapshot(&self, key: ChildKey) -> Result<RecoverySnapshot, RecoveryError> {
        let entry = self.entries.get(&key).ok_or(RecoveryError::UnknownChild)?;
        Ok(RecoverySnapshot {
            key,
            generation: entry.generation,
            state: entry.state,
            pid: entry.child.as_ref().and_then(Child::id),
            restart: entry.restart,
            startup_failures: entry.startup_failures,
            stable: entry.stable,
            held: entry.held,
        })
    }

    /// Reserve generations used by admitted update candidates, including failed
    /// candidates. Does not change the currently running generation/Ready proof.
    pub fn advance_generation_floor(
        &mut self,
        key: ChildKey,
        after_generation: u64,
    ) -> Result<(), RecoveryError> {
        let entry = self
            .entries
            .get_mut(&key)
            .ok_or(RecoveryError::UnknownChild)?;
        entry.generation_floor = entry.generation_floor.max(after_generation);
        Ok(())
    }

    /// Trusted owner access for graceful shutdown or an explicitly authorized
    /// kill. Do not move/replace this handle. A kill request alone never permits
    /// restarting; poll must subsequently collect the exit.
    pub fn child_mut(&mut self, key: ChildKey) -> Result<Option<&mut Child>, RecoveryError> {
        Ok(self
            .entries
            .get_mut(&key)
            .ok_or(RecoveryError::UnknownChild)?
            .child
            .as_mut())
    }

    /// Management/update exclusion. It preserves deadlines and all budgets.
    /// Call poll immediately after holding to cancel a pending Identify request.
    pub fn hold(&mut self, key: ChildKey, held: bool) -> Result<(), RecoveryError> {
        self.entries
            .get_mut(&key)
            .ok_or(RecoveryError::UnknownChild)?
            .held = held;
        Ok(())
    }

    /// Cancel automatic recovery BEFORE sending shutdown. Does not kill or drop
    /// a child. Stopping completes only once poll reaps it. No automatic rearm.
    pub fn stop(&mut self, key: ChildKey) -> Result<(), RecoveryError> {
        let entry = self
            .entries
            .get_mut(&key)
            .ok_or(RecoveryError::UnknownChild)?;
        entry.desired = false;
        entry.restart.retry_at = None;
        entry.ready_at = None;
        entry.stable = false;
        if !matches!(entry.state, RecoveryState::Degraded(_)) {
            entry.state = RecoveryState::Stopping;
        }
        Ok(())
    }

    /// Explicit authenticated management start AFTER a normal stop was reaped.
    /// Preserves all budgets/generations and cannot clear Degraded. Callers must
    /// not reconstruct the engine as an implicit reset of exhausted budgets.
    pub fn start_stopped(&mut self, key: ChildKey) -> Result<(), RecoveryError> {
        let now = self.clock.now();
        if self.clock_failed || now < self.last_now {
            self.clock_failed = true;
            return Err(RecoveryError::ClockFailed);
        }
        self.last_now = now;
        let entry = self
            .entries
            .get_mut(&key)
            .ok_or(RecoveryError::UnknownChild)?;
        if entry.state != RecoveryState::Stopped || entry.child.is_some() {
            return Err(RecoveryError::NotStopped);
        }
        entry.desired = true;
        entry.state = RecoveryState::Waiting;
        entry.restart.retry_at = Some(now.max(entry.not_before));
        entry.deferred = None;
        Ok(())
    }

    /// Call only after authenticated, generation-matched Hello/Ready. Repeated
    /// notifications cannot shorten the stability interval or reset budgets.
    pub fn confirm_ready(&mut self, key: ChildKey, generation: u64) -> Result<(), RecoveryError> {
        let now = self.clock.now();
        if self.clock_failed || now < self.last_now {
            // Leave last_now intact so poll emits the clock fault and keeps
            // reaping. Latch now in case the clock recovers before the next poll.
            self.clock_failed = true;
            return Err(RecoveryError::ClockFailed);
        }
        self.last_now = now;
        let entry = self
            .entries
            .get_mut(&key)
            .ok_or(RecoveryError::UnknownChild)?;
        if entry.generation != generation {
            return Err(RecoveryError::StaleGeneration);
        }
        if entry.state != RecoveryState::Running || entry.child.is_none() {
            return Err(RecoveryError::NotRunning);
        }
        entry.ready_at.get_or_insert(now);
        Ok(())
    }

    /// Commit an explicitly admitted management update after the replacement's
    /// authenticated Ready/state restore and the old generation's pause ACK.
    /// Return the old handle to its drain owner; preserve every restart budget.
    /// On rejection the caller retains the candidate handle too. This is not a
    /// crash recovery path and cannot clear a latched Degraded state.
    // Both success and rejection must return an owned Child. Boxing only the
    // error would add allocation without reducing the Child-sized return value.
    #[allow(clippy::result_large_err)]
    pub fn replace_ready(
        &mut self,
        key: ChildKey,
        expected_generation: u64,
        next_generation: u64,
        child: Child,
    ) -> Result<Child, (RecoveryError, Child)> {
        self.install_ready(key, expected_generation, next_generation, child, false)
            .map(|old| old.expect("replacement requires a running child"))
    }

    /// Install a verified management candidate after a failed automatic attempt
    /// has been reaped. The owner's old routing/journal may still refer to the
    /// earlier dead generation. No degraded budget or stop can be bypassed.
    #[allow(clippy::result_large_err)] // Same owned-handle contract as replace_ready.
    pub fn adopt_ready(
        &mut self,
        key: ChildKey,
        expected_generation: u64,
        next_generation: u64,
        child: Child,
    ) -> Result<Option<Child>, (RecoveryError, Child)> {
        self.install_ready(key, expected_generation, next_generation, child, true)
    }

    #[allow(clippy::result_large_err)] // Preserve the caller's handle on every rejection.
    fn install_ready(
        &mut self,
        key: ChildKey,
        expected_generation: u64,
        next_generation: u64,
        child: Child,
        allow_reaped: bool,
    ) -> Result<Option<Child>, (RecoveryError, Child)> {
        let now = self.clock.now();
        if self.clock_failed || now < self.last_now {
            self.clock_failed = true;
            return Err((RecoveryError::ClockFailed, child));
        }
        self.last_now = now;
        let Some(entry) = self.entries.get_mut(&key) else {
            return Err((RecoveryError::UnknownChild, child));
        };
        if entry.generation != expected_generation
            || next_generation <= expected_generation
            || next_generation < entry.generation_floor
        {
            return Err((RecoveryError::StaleGeneration, child));
        }
        let running = entry.state == RecoveryState::Running && entry.child.is_some();
        let reaped = allow_reaped
            && entry.attempted
            && entry.state == RecoveryState::Waiting
            && entry.child.is_none();
        if !entry.desired || !(running || reaped) {
            return Err((RecoveryError::NotRunning, child));
        }
        let old = entry.child.replace(child);
        entry.generation = next_generation;
        entry.generation_floor = entry.generation_floor.max(next_generation);
        entry.state = RecoveryState::Running;
        entry.restart.retry_at = None;
        entry.ready_at = Some(now);
        entry.stable = false;
        entry.deferred = None;
        Ok(old)
    }

    /// Revoke readiness evidence without treating a transport failure as exit.
    /// Neither the owned handle nor any restart budget is reset.
    pub fn invalidate_ready(
        &mut self,
        key: ChildKey,
        generation: u64,
    ) -> Result<(), RecoveryError> {
        let entry = self
            .entries
            .get_mut(&key)
            .ok_or(RecoveryError::UnknownChild)?;
        if entry.generation != generation {
            return Err(RecoveryError::StaleGeneration);
        }
        entry.ready_at = None;
        entry.stable = false;
        Ok(())
    }

    /// Nonblocking OS observation followed by at most one start attempt per slot.
    /// Every unrequested exit, including code 0, is a service failure. Explicit
    /// stop suppresses all retries. Wait errors retain the handle and latch
    /// Degraded; no liveness error is interpreted as an absent process.
    pub fn poll(&mut self, host: &mut impl RecoveryHost) -> Vec<RecoveryEvent> {
        let sampled = self.clock.now();
        self.clock_failed |= sampled < self.last_now;
        if !self.clock_failed {
            self.last_now = sampled;
        }
        let now = self.last_now;
        let mut events = Vec::new();
        for entry in self.entries.values_mut() {
            if self.clock_failed {
                entry.degrade(now, DegradedReason::ClockRegressed, &mut events);
            }
            if let Some(child) = entry.child.as_mut() {
                match child.try_wait() {
                    Ok(Some(status)) => {
                        entry.child = None;
                        entry.emit(
                            now,
                            RecoveryEventKind::Exited {
                                success: status.success(),
                                code: status.code(),
                            },
                            &mut events,
                        );
                        if self.legacy_process_exits && status.success() {
                            entry.desired = false;
                            entry.state = RecoveryState::Stopping;
                            entry.restart.retry_at = None;
                        }
                        if entry.desired && !matches!(entry.state, RecoveryState::Degraded(_)) {
                            let mut config = self.config;
                            if self.wait_for_window {
                                config.restart.max_restarts_in_window = u32::MAX;
                            }
                            entry.failed(now, config, &mut events);
                        }
                    }
                    Ok(None) => {
                        // Check liveness BEFORE time-based success. A delayed
                        // poll after an immediate crash must not close a circuit.
                        if entry.state == RecoveryState::Running
                            && !entry.stable
                            && entry
                                .ready_at
                                .is_some_and(|at| now.saturating_sub(at) >= self.config.stable_for)
                        {
                            entry.stable = true;
                            entry.startup_failures = 0;
                            entry.restart.failures = 0;
                            entry.restart.circuit = CircuitState::Closed;
                            entry.emit(now, RecoveryEventKind::Stable, &mut events);
                        }
                    }
                    Err(_) => entry.degrade(now, DegradedReason::MonitorFailed, &mut events),
                }
            }
            if !entry.desired || matches!(entry.state, RecoveryState::Degraded(_)) {
                host.cancel_identify(entry.key);
                if !entry.desired && entry.child.is_none() && entry.state == RecoveryState::Stopping
                {
                    entry.state = RecoveryState::Stopped;
                    entry.emit(now, RecoveryEventKind::Stopped, &mut events);
                }
                continue;
            }
            if entry.held {
                host.cancel_identify(entry.key);
                if entry.child.is_none() {
                    entry.defer(now, DeferralReason::ManagementHold, &mut events);
                }
                continue;
            }
            if entry.child.is_some() || entry.restart.retry_at.is_none_or(|at| now < at) {
                continue;
            }
            if self.wait_for_window && entry.attempted {
                while entry
                    .recent
                    .front()
                    .is_some_and(|at| now.saturating_sub(*at) >= self.config.restart.window)
                {
                    entry.recent.pop_front();
                }
                if entry.recent.len() >= self.config.restart.max_restarts_in_window as usize {
                    if let Some(at) = entry.recent[0].checked_add(self.config.restart.window) {
                        entry.restart.retry_at = Some(at);
                        entry.not_before = at;
                    } else {
                        entry.degrade(now, DegradedReason::TimeOverflow, &mut events);
                    }
                    continue;
                }
            }
            if !entry.budget_available(now, self.config.restart, &mut events) {
                host.cancel_identify(entry.key);
                continue;
            }
            let Some(generation) = entry.generation.max(entry.generation_floor).checked_add(1)
            else {
                entry.degrade(now, DegradedReason::GenerationExhausted, &mut events);
                host.cancel_identify(entry.key);
                continue;
            };
            if !host.authorized(entry.key) {
                host.cancel_identify(entry.key);
                entry.defer(now, DeferralReason::Ownership, &mut events);
                continue;
            }
            let mode = match entry.key.role {
                ChildRole::Core => StartMode::Core,
                ChildRole::Gateway if host.resume_available(entry.key) => {
                    host.cancel_identify(entry.key);
                    StartMode::GatewayResume
                }
                ChildRole::Gateway => match host.try_acquire_identify(entry.key) {
                    IdentifyAdmission::Granted => StartMode::GatewayIdentify,
                    IdentifyAdmission::Pending => {
                        entry.defer(now, DeferralReason::IdentifyBudget, &mut events);
                        continue;
                    }
                    IdentifyAdmission::Unavailable => {
                        entry.degrade(now, DegradedReason::IdentifyUnavailable, &mut events);
                        host.cancel_identify(entry.key);
                        continue;
                    }
                },
            };
            // Even a consumed Identify grant does not override a revoked lease.
            if !host.authorized(entry.key) {
                host.cancel_identify(entry.key);
                entry.defer(now, DeferralReason::Ownership, &mut events);
                continue;
            }
            if entry.attempted {
                entry.restart.restarts += 1;
                entry.recent.push_back(now);
            }
            entry.attempted = true;
            entry.generation = generation;
            entry.generation_floor = generation;
            entry.restart.retry_at = None;
            entry.ready_at = None;
            entry.stable = false;
            entry.deferred = None;
            if matches!(entry.restart.circuit, CircuitState::Open { .. }) {
                entry.restart.circuit = CircuitState::HalfOpen;
                entry.emit(now, RecoveryEventKind::CircuitHalfOpen, &mut events);
            }
            match host.spawn(StartRequest {
                key: entry.key,
                generation,
                mode,
            }) {
                Ok(child) => {
                    entry.child = Some(child);
                    entry.state = RecoveryState::Running;
                    entry.emit(now, RecoveryEventKind::Started { mode }, &mut events);
                }
                Err(_) => {
                    entry.emit(now, RecoveryEventKind::SpawnFailed, &mut events);
                    let mut config = self.config;
                    if self.wait_for_window {
                        config.restart.max_restarts_in_window = u32::MAX;
                    }
                    entry.failed(now, config, &mut events);
                }
            }
        }
        events
    }
}
