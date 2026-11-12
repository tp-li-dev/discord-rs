use src::{
    child_recovery::*,
    identify::IdentifyRegistry,
    protocol::BotId,
    supervisor::{CircuitState, Clock, RestartPolicy, Sleep},
};
use std::{
    collections::{HashMap, HashSet},
    io,
    num::NonZeroU64,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{process::Child, sync::oneshot};
use twilight_gateway_queue::Queue;
use twilight_model::gateway::SessionStartLimit;

#[derive(Clone, Default)]
struct ManualClock(Arc<Mutex<Duration>>);
impl ManualClock {
    fn set(&self, seconds: u64) {
        *self.0.lock().unwrap() = Duration::from_secs(seconds);
    }
}
impl Clock for ManualClock {
    fn now(&self) -> Duration {
        *self.0.lock().unwrap()
    }
    fn sleep_until(&self, _: Duration) -> Sleep<'_> {
        Box::pin(std::future::pending())
    }
}

fn key(bot: u64, shard: u32, role: ChildRole) -> ChildKey {
    ChildKey {
        bot_id: BotId(bot),
        shard_id: shard,
        role,
    }
}
fn core() -> ChildKey {
    key(1, 0, ChildRole::Core)
}
fn gateway() -> ChildKey {
    key(1, 0, ChildRole::Gateway)
}
fn config() -> RecoveryConfig {
    RecoveryConfig {
        restart: RestartPolicy {
            max_restarts: 20,
            max_restarts_in_window: 10,
            window: Duration::from_secs(10),
            initial_backoff: Duration::from_secs(1),
            max_backoff: Duration::from_secs(4),
            circuit_breaker_after: 10,
            circuit_open_for: Duration::from_secs(10),
        },
        stable_for: Duration::from_secs(5),
        max_startup_failures: 10,
    }
}

struct Host {
    starts: Vec<StartRequest>,
    identifies: Vec<ChildKey>,
    cancellations: Vec<ChildKey>,
    resumable: HashSet<ChildKey>,
    authorized: bool,
    admission: IdentifyAdmission,
    revoke_on_identify: bool,
}
impl Default for Host {
    fn default() -> Self {
        Self {
            starts: Vec::new(),
            identifies: Vec::new(),
            cancellations: Vec::new(),
            resumable: HashSet::new(),
            authorized: true,
            admission: IdentifyAdmission::Granted,
            revoke_on_identify: false,
        }
    }
}
impl RecoveryHost for Host {
    fn authorized(&mut self, _: ChildKey) -> bool {
        self.authorized
    }
    fn resume_available(&mut self, key: ChildKey) -> bool {
        self.resumable.contains(&key)
    }
    fn try_acquire_identify(&mut self, key: ChildKey) -> IdentifyAdmission {
        self.identifies.push(key);
        if self.revoke_on_identify {
            self.authorized = false;
        }
        self.admission
    }
    fn cancel_identify(&mut self, key: ChildKey) {
        self.cancellations.push(key);
    }
    fn spawn(&mut self, request: StartRequest) -> io::Result<Child> {
        self.starts.push(request);
        Err(io::Error::other("untrusted fixture diagnostic"))
    }
}

#[test]
fn invalid_configuration_duplicate_and_capacity_are_rejected() {
    let clock = ManualClock::default();
    let mut bad = config();
    bad.stable_for = Duration::ZERO;
    assert!(ChildRecovery::new(clock.clone(), bad, 1).is_err());
    bad = config();
    bad.max_startup_failures = 0;
    assert!(ChildRecovery::new(clock.clone(), bad, 1).is_err());
    bad = config();
    bad.restart.max_restarts_in_window = 0;
    assert!(ChildRecovery::new(clock.clone(), bad, 1).is_err());
    bad = config();
    bad.restart.max_backoff = Duration::ZERO;
    assert!(ChildRecovery::new(clock.clone(), bad, 1).is_err());
    assert!(ChildRecovery::new(clock.clone(), config(), 0).is_err());
    let mut engine = ChildRecovery::new(clock, config(), 1).unwrap();
    engine.register(core(), 0).unwrap();
    assert_eq!(
        engine.register(core(), 0),
        Err(RecoveryError::DuplicateChild)
    );
    assert_eq!(engine.register(gateway(), 0), Err(RecoveryError::Capacity));
}

#[test]
fn exponential_delays_are_capped_and_repeated_polling_never_spends_attempts() {
    let clock = ManualClock::default();
    let mut engine = ChildRecovery::new(clock.clone(), config(), 1).unwrap();
    let mut host = Host::default();
    engine.register(core(), 40).unwrap();
    for (now, retry, attempts) in [(0, 1, 1), (1, 3, 2), (3, 7, 3), (7, 11, 4)] {
        clock.set(now);
        engine.poll(&mut host);
        assert_eq!(host.starts.len(), attempts);
        let snapshot = engine.snapshot(core()).unwrap();
        assert_eq!(snapshot.restart.retry_at, Some(Duration::from_secs(retry)));
        assert_eq!(snapshot.generation, 40 + attempts as u64);
        assert_eq!(snapshot.restart.restarts, attempts as u32 - 1);
        for _ in 0..3 {
            assert!(engine.poll(&mut host).is_empty());
        }
        clock.set(retry - 1);
        engine.poll(&mut host);
        assert_eq!(host.starts.len(), attempts);
    }
}

#[test]
fn lifetime_budget_is_latched_degraded_including_failed_spawns() {
    let clock = ManualClock::default();
    let mut policy = config();
    policy.restart.max_restarts = 2;
    let mut engine = ChildRecovery::new(clock.clone(), policy, 1).unwrap();
    let mut host = Host::default();
    engine.register(core(), 0).unwrap();
    let mut events = Vec::new();
    for now in [0, 1, 3, 100, 200] {
        clock.set(now);
        events.extend(engine.poll(&mut host));
    }
    assert_eq!(host.starts.len(), 3);
    let snapshot = engine.snapshot(core()).unwrap();
    assert_eq!(
        snapshot.state,
        RecoveryState::Degraded(DegradedReason::RestartBudget)
    );
    assert!(snapshot.restart.exhausted);
    assert_eq!(snapshot.restart.retry_at, None);
    assert_eq!(
        events
            .iter()
            .filter(|event| matches!(event.kind, RecoveryEventKind::Degraded { .. }))
            .count(),
        1
    );
    assert!(!format!("{events:?}").contains("untrusted fixture diagnostic"));
    engine.hold(core(), true).unwrap();
    engine.hold(core(), false).unwrap();
    engine.stop(core()).unwrap();
    engine.poll(&mut host);
    assert_eq!(engine.snapshot(core()).unwrap().state, snapshot.state);
}

#[test]
fn zero_restart_budget_still_allows_the_initial_admitted_start() {
    let clock = ManualClock::default();
    let mut policy = config();
    policy.restart.max_restarts = 0;
    let mut engine = ChildRecovery::new(clock, policy, 1).unwrap();
    engine.register(core(), 0).unwrap();
    let mut host = Host::default();
    engine.poll(&mut host);
    assert_eq!(host.starts.len(), 1);
    assert_eq!(engine.snapshot(core()).unwrap().restart.restarts, 0);
    assert_eq!(
        engine.snapshot(core()).unwrap().state,
        RecoveryState::Degraded(DegradedReason::RestartBudget)
    );
}

#[test]
fn window_exhaustion_stops_instead_of_replenishing_after_cooldown() {
    let clock = ManualClock::default();
    let mut policy = config();
    policy.restart.max_restarts_in_window = 2;
    let mut engine = ChildRecovery::new(clock.clone(), policy, 1).unwrap();
    engine.register(core(), 0).unwrap();
    let mut host = Host::default();
    for now in [0, 1, 3, 100] {
        clock.set(now);
        engine.poll(&mut host);
    }
    assert_eq!(host.starts.len(), 3);
    assert_eq!(
        engine.snapshot(core()).unwrap().state,
        RecoveryState::Degraded(DegradedReason::RestartWindow)
    );
}

#[test]
fn sliding_window_excludes_attempts_at_exact_window_boundary() {
    let clock = ManualClock::default();
    let mut policy = config();
    policy.restart.max_restarts_in_window = 2;
    let mut engine = ChildRecovery::new(clock.clone(), policy, 1).unwrap();
    engine.register(core(), 0).unwrap();
    let mut host = Host::default();
    for now in [0, 1, 11] {
        clock.set(now);
        engine.poll(&mut host);
    }
    let snapshot = engine.snapshot(core()).unwrap();
    assert_eq!(snapshot.state, RecoveryState::Waiting);
    assert_eq!(snapshot.restart.restarts, 2);
    assert_eq!(snapshot.restart.retry_at, Some(Duration::from_secs(15)));
}

#[test]
fn circuit_cooldown_allows_one_half_open_attempt_without_resetting_budgets() {
    let clock = ManualClock::default();
    let mut policy = config();
    policy.restart.circuit_breaker_after = 2;
    let mut engine = ChildRecovery::new(clock.clone(), policy, 1).unwrap();
    engine.register(core(), 0).unwrap();
    let mut host = Host::default();
    engine.poll(&mut host);
    clock.set(1);
    engine.poll(&mut host);
    assert_eq!(
        engine.snapshot(core()).unwrap().restart.circuit,
        CircuitState::Open {
            until: Duration::from_secs(11)
        }
    );
    clock.set(10);
    assert!(engine.poll(&mut host).is_empty());
    assert_eq!(host.starts.len(), 2);
    clock.set(11);
    let events = engine.poll(&mut host);
    assert_eq!(host.starts.len(), 3);
    assert_eq!(
        events
            .iter()
            .filter(|event| event.kind == RecoveryEventKind::CircuitHalfOpen)
            .count(),
        1
    );
    assert_eq!(
        engine.snapshot(core()).unwrap().restart.circuit,
        CircuitState::Open {
            until: Duration::from_secs(21)
        }
    );
    assert_eq!(engine.snapshot(core()).unwrap().restart.restarts, 2);
    assert!(engine.poll(&mut host).is_empty());
}

#[test]
fn startup_failures_trip_a_distinct_crash_loop_reason() {
    let clock = ManualClock::default();
    let mut policy = config();
    policy.max_startup_failures = 3;
    let mut engine = ChildRecovery::new(clock.clone(), policy, 1).unwrap();
    engine.register(core(), 0).unwrap();
    let mut host = Host::default();
    for now in [0, 1, 3, 100] {
        clock.set(now);
        engine.poll(&mut host);
    }
    assert_eq!(
        engine.snapshot(core()).unwrap().state,
        RecoveryState::Degraded(DegradedReason::StartupCrashLoop)
    );
    assert_eq!(host.starts.len(), 3);
}

#[test]
fn resume_is_preferred_and_identify_fallback_waits_without_spending_restarts() {
    let clock = ManualClock::default();
    let mut engine = ChildRecovery::new(clock.clone(), config(), 2).unwrap();
    engine.register(gateway(), 0).unwrap();
    engine.register(core(), 0).unwrap();
    let mut host = Host {
        admission: IdentifyAdmission::Pending,
        ..Host::default()
    };
    host.resumable.insert(gateway());
    engine.poll(&mut host);
    assert!(host.identifies.is_empty());
    assert!(
        host.starts
            .iter()
            .any(|r| r.mode == StartMode::GatewayResume)
    );
    assert!(host.starts.iter().any(|r| r.mode == StartMode::Core));
    // The host invalidates its resume hint after an unsuccessful resume attempt.
    host.resumable.clear();
    engine.stop(core()).unwrap();
    clock.set(1);
    let events = engine.poll(&mut host);
    assert!(events.iter().any(|e| e.key == gateway()
        && e.kind
            == RecoveryEventKind::Deferred {
                reason: DeferralReason::IdentifyBudget
            }));
    for _ in 0..5 {
        assert!(engine.poll(&mut host).is_empty());
    }
    assert_eq!(engine.snapshot(gateway()).unwrap().restart.restarts, 0);
    host.admission = IdentifyAdmission::Granted;
    engine.poll(&mut host);
    assert_eq!(host.starts.last().unwrap().mode, StartMode::GatewayIdentify);
    assert_eq!(engine.snapshot(gateway()).unwrap().restart.restarts, 1);
}

#[test]
fn hold_authority_and_stop_cancel_pending_admission_and_never_bypass_budget() {
    let clock = ManualClock::default();
    let mut engine = ChildRecovery::new(clock, config(), 1).unwrap();
    engine.register(gateway(), 0).unwrap();
    let mut host = Host::default();
    engine.hold(gateway(), true).unwrap();
    engine.poll(&mut host);
    assert!(host.starts.is_empty() && host.identifies.is_empty());
    engine.hold(gateway(), false).unwrap();
    host.authorized = false;
    engine.poll(&mut host);
    assert!(host.starts.is_empty() && host.identifies.is_empty());
    host.authorized = true;
    host.revoke_on_identify = true;
    engine.poll(&mut host);
    assert!(host.starts.is_empty());
    assert_eq!(host.identifies.len(), 1);
    assert_eq!(engine.snapshot(gateway()).unwrap().generation, 0);
    engine.stop(gateway()).unwrap();
    host.authorized = true;
    host.revoke_on_identify = false;
    engine.poll(&mut host);
    assert_eq!(
        engine.snapshot(gateway()).unwrap().state,
        RecoveryState::Stopped
    );
    assert!(host.starts.is_empty());
    assert!(host.cancellations.contains(&gateway()));
}

#[test]
fn missing_identify_coordinator_fails_closed_only_for_that_gateway() {
    let clock = ManualClock::default();
    let mut engine = ChildRecovery::new(clock, config(), 2).unwrap();
    engine.register(gateway(), 0).unwrap();
    engine.register(core(), 0).unwrap();
    let mut host = Host {
        admission: IdentifyAdmission::Unavailable,
        ..Host::default()
    };
    engine.poll(&mut host);
    assert_eq!(
        engine.snapshot(gateway()).unwrap().state,
        RecoveryState::Degraded(DegradedReason::IdentifyUnavailable)
    );
    assert_eq!(host.starts.len(), 1);
    assert_eq!(host.starts[0].key, core());
}

#[test]
fn explicit_stop_start_preserves_backoff_circuit_and_attempt_budget() {
    let clock = ManualClock::default();
    let mut policy = config();
    policy.restart.circuit_breaker_after = 1;
    let mut engine = ChildRecovery::new(clock.clone(), policy, 1).unwrap();
    engine.register(core(), 0).unwrap();
    let mut host = Host::default();
    engine.poll(&mut host);
    engine.stop(core()).unwrap();
    engine.poll(&mut host);
    engine.start_stopped(core()).unwrap();
    engine.poll(&mut host);
    assert_eq!(host.starts.len(), 1);
    assert_eq!(
        engine.snapshot(core()).unwrap().restart.retry_at,
        Some(Duration::from_secs(10))
    );
    clock.set(10);
    engine.poll(&mut host);
    assert_eq!(host.starts.len(), 2);
    assert_eq!(engine.snapshot(core()).unwrap().restart.restarts, 1);
    assert_eq!(engine.snapshot(core()).unwrap().restart.failures, 2);
}

#[test]
fn clock_regression_is_latched_even_if_detected_by_ready_and_then_recovers() {
    let clock = ManualClock::default();
    clock.set(10);
    let mut engine = ChildRecovery::new(clock.clone(), config(), 1).unwrap();
    engine.register(core(), 0).unwrap();
    let mut host = Host::default();
    engine.poll(&mut host);
    clock.set(9);
    assert_eq!(
        engine.confirm_ready(core(), 1),
        Err(RecoveryError::ClockFailed)
    );
    clock.set(100);
    let events = engine.poll(&mut host);
    assert!(events.iter().any(|event| event.kind
        == RecoveryEventKind::Degraded {
            reason: DegradedReason::ClockRegressed
        }));
    assert_eq!(host.starts.len(), 1);
    assert_eq!(
        engine.register(gateway(), 0),
        Err(RecoveryError::ClockFailed)
    );
}

#[test]
fn overflowing_time_or_generation_never_wraps_into_permission() {
    let clock = ManualClock::default();
    let mut engine = ChildRecovery::new(clock, config(), 1).unwrap();
    engine.register(gateway(), u64::MAX).unwrap();
    let mut host = Host::default();
    engine.poll(&mut host);
    assert_eq!(
        engine.snapshot(gateway()).unwrap().state,
        RecoveryState::Degraded(DegradedReason::GenerationExhausted)
    );
    assert!(host.starts.is_empty() && host.identifies.is_empty());
    let clock = ManualClock::default();
    *clock.0.lock().unwrap() = Duration::MAX;
    let mut engine = ChildRecovery::new(clock, config(), 1).unwrap();
    engine.register(core(), 0).unwrap();
    engine.poll(&mut host);
    assert_eq!(
        engine.snapshot(core()).unwrap().state,
        RecoveryState::Degraded(DegradedReason::TimeOverflow)
    );
}

/// A local fixture adapter uses the existing registry, not a second limiter.
/// Production process/WS handoff and distributed reservations remain separate.
struct QueueHost {
    host: Host,
    registry: IdentifyRegistry,
    pending: HashMap<ChildKey, oneshot::Receiver<()>>,
}
impl RecoveryHost for QueueHost {
    fn authorized(&mut self, key: ChildKey) -> bool {
        self.host.authorized(key)
    }
    fn resume_available(&mut self, key: ChildKey) -> bool {
        self.host.resume_available(key)
    }
    fn try_acquire_identify(&mut self, key: ChildKey) -> IdentifyAdmission {
        let Some(bot) = NonZeroU64::new(key.bot_id.0) else {
            return IdentifyAdmission::Unavailable;
        };
        let Some(queue) = self.registry.get(bot) else {
            return IdentifyAdmission::Unavailable;
        };
        let receiver = self
            .pending
            .entry(key)
            .or_insert_with(|| queue.enqueue(key.shard_id));
        match receiver.try_recv() {
            Ok(()) => {
                self.pending.remove(&key);
                IdentifyAdmission::Granted
            }
            Err(oneshot::error::TryRecvError::Empty) => IdentifyAdmission::Pending,
            Err(oneshot::error::TryRecvError::Closed) => IdentifyAdmission::Unavailable,
        }
    }
    fn cancel_identify(&mut self, key: ChildKey) {
        self.pending.remove(&key);
    }
    fn spawn(&mut self, request: StartRequest) -> io::Result<Child> {
        self.host.spawn(request)
    }
}

#[tokio::test]
async fn real_identify_registry_is_shared_between_shards_and_isolated_between_bots() {
    let clock = ManualClock::default();
    let mut engine = ChildRecovery::new(clock, config(), 4).unwrap();
    let same_bot = key(1, 1, ChildRole::Gateway);
    let other_bot = key(2, 0, ChildRole::Gateway);
    for child in [gateway(), same_bot, other_bot, core()] {
        engine.register(child, 0).unwrap();
    }
    let mut registry = IdentifyRegistry::default();
    let limits = SessionStartLimit {
        total: 1,
        remaining: 1,
        reset_after: 60_000,
        max_concurrency: 2,
    };
    for bot in [1, 2] {
        registry
            .register(NonZeroU64::new(bot).unwrap(), &limits)
            .unwrap();
    }
    let mut host = QueueHost {
        host: Host::default(),
        registry,
        pending: HashMap::new(),
    };
    tokio::time::timeout(Duration::from_secs(3), async {
        while host.host.starts.len() < 3 {
            engine.poll(&mut host);
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await
    .unwrap();
    // Observe that the remaining same-Bot shard stays blocked, not merely that
    // the first callbacks happened to run before the fourth became runnable.
    tokio::time::sleep(Duration::from_millis(30)).await;
    engine.poll(&mut host);
    assert_eq!(host.host.starts.len(), 3);
    assert_eq!(
        host.host
            .starts
            .iter()
            .filter(|r| r.key.bot_id == BotId(1) && r.key.role == ChildRole::Gateway)
            .count(),
        1
    );
    assert!(host.host.starts.iter().any(|r| r.key == other_bot));
    assert!(host.host.starts.iter().any(|r| r.key == core()));
    assert_eq!(engine.snapshot(gateway()).unwrap().restart.restarts, 0);
    assert_eq!(engine.snapshot(same_bot).unwrap().restart.restarts, 0);
}
