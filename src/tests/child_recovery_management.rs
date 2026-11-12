//! Management handoff and Resume rejection contracts around real owned handles.
//! The test executable is the child fixture; no secondary build or Discord
//! connection is involved. Resume rejection is an injected host observation.
use src::{
    child_recovery::{
        ChildKey, ChildRecovery, ChildRole, DeferralReason, DegradedReason, IdentifyAdmission,
        RecoveryConfig, RecoveryError, RecoveryEvent, RecoveryEventKind, RecoveryHost,
        RecoveryState, StartMode, StartRequest,
    },
    protocol::BotId,
    supervisor::{Clock, RestartPolicy, Sleep},
};
use std::{
    future::Future,
    io::{self, BufRead, Write},
    process::Stdio,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    process::{Child, Command},
};

const WAIT: Duration = Duration::from_secs(15);

#[derive(Clone, Default)]
struct ManualClock(Arc<Mutex<Duration>>);

impl ManualClock {
    fn set(&self, now: Duration) {
        *self.0.lock().unwrap() = now;
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

async fn bounded<T>(future: impl Future<Output = T>) -> T {
    tokio::time::timeout(WAIT, future)
        .await
        .expect("recovery management fixture deadline")
}

fn config() -> RecoveryConfig {
    RecoveryConfig {
        restart: RestartPolicy {
            max_restarts: 5,
            max_restarts_in_window: 5,
            window: Duration::from_secs(60),
            initial_backoff: Duration::from_secs(1),
            max_backoff: Duration::from_secs(4),
            circuit_breaker_after: 5,
            circuit_open_for: Duration::from_secs(10),
        },
        stable_for: Duration::from_secs(30),
        max_startup_failures: 6,
    }
}

fn child_fixture() -> io::Result<Child> {
    let mut command = Command::new(std::env::current_exe()?);
    command
        .args([
            "--exact",
            "management_fixture",
            "--nocapture",
            "--test-threads=1",
        ])
        .env_clear()
        .env("RECOVERY_MANAGEMENT_FIXTURE", "1")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .kill_on_drop(true);
    #[cfg(windows)]
    command.env(
        "SystemRoot",
        std::env::var_os("SystemRoot")
            .ok_or_else(|| io::Error::other("SystemRoot required for fixture"))?,
    );
    command.spawn()
}

async fn ready(child: &mut Child) -> u32 {
    let expected = child.id().expect("owned live fixture");
    let mut output = BufReader::new(child.stdout.take().unwrap());
    bounded(async {
        loop {
            let mut line = String::new();
            assert_ne!(output.read_line(&mut line).await.unwrap(), 0);
            if let Some((_, pid)) = line.split_once("MANAGEMENT_READY ") {
                assert_eq!(pid.trim().parse::<u32>().unwrap(), expected);
                return expected;
            }
        }
    })
    .await
}

#[derive(Default)]
struct Host {
    starts: Vec<StartRequest>,
    resumable: bool,
    identify_permits: u32,
    identify_requests: u32,
    identify_consumed: u32,
}

impl RecoveryHost for Host {
    fn authorized(&mut self, _: ChildKey) -> bool {
        // One local test owner, with no distributed ownership claim.
        true
    }

    fn resume_available(&mut self, _: ChildKey) -> bool {
        self.resumable
    }

    fn try_acquire_identify(&mut self, key: ChildKey) -> IdentifyAdmission {
        assert_eq!(key.role, ChildRole::Gateway);
        self.identify_requests += 1;
        if self.identify_permits == 0 {
            IdentifyAdmission::Pending
        } else {
            self.identify_permits -= 1;
            self.identify_consumed += 1;
            IdentifyAdmission::Granted
        }
    }

    fn cancel_identify(&mut self, _: ChildKey) {}

    fn spawn(&mut self, request: StartRequest) -> io::Result<Child> {
        let child = child_fixture()?;
        self.starts.push(request);
        Ok(child)
    }
}

struct Harness {
    clock: ManualClock,
    engine: ChildRecovery<ManualClock>,
    key: ChildKey,
    host: Host,
    events: Vec<RecoveryEvent>,
}

impl Harness {
    fn new(role: ChildRole, config: RecoveryConfig) -> Self {
        let clock = ManualClock::default();
        let key = ChildKey {
            bot_id: BotId(101),
            shard_id: 0,
            role,
        };
        let mut engine = ChildRecovery::new(clock.clone(), config, 1).unwrap();
        engine.register(key, 0).unwrap();
        Self {
            clock,
            engine,
            key,
            host: Host::default(),
            events: Vec::new(),
        }
    }

    fn poll(&mut self) {
        self.events.extend(self.engine.poll(&mut self.host));
    }

    async fn ready(&mut self) -> u32 {
        let pid = ready(self.engine.child_mut(self.key).unwrap().unwrap()).await;
        let generation = self.engine.snapshot(self.key).unwrap().generation;
        self.engine.confirm_ready(self.key, generation).unwrap();
        pid
    }

    async fn start(&mut self) -> u32 {
        self.poll();
        self.ready().await
    }

    async fn until(&mut self, state: RecoveryState) {
        bounded(async {
            loop {
                self.poll();
                if self.engine.snapshot(self.key).unwrap().state == state {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await;
    }

    async fn kill(&mut self, state: RecoveryState) {
        self.engine
            .child_mut(self.key)
            .unwrap()
            .unwrap()
            .start_kill()
            .unwrap();
        self.until(state).await;
    }

    fn advance_to_retry(&mut self) {
        let retry = self
            .engine
            .snapshot(self.key)
            .unwrap()
            .restart
            .retry_at
            .unwrap();
        self.clock.set(retry);
        self.poll();
    }

    async fn cleanup(&mut self) {
        self.engine.stop(self.key).unwrap();
        if let Some(child) = self.engine.child_mut(self.key).unwrap() {
            child.start_kill().unwrap();
        }
        bounded(async {
            loop {
                self.poll();
                if self.engine.child_mut(self.key).unwrap().is_none() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await;
    }
}

impl Drop for Harness {
    fn drop(&mut self) {
        let _ = self.engine.stop(self.key);
        if let Ok(Some(child)) = self.engine.child_mut(self.key) {
            let _ = child.start_kill();
        }
        let deadline = Instant::now() + Duration::from_secs(3);
        loop {
            self.poll();
            if self
                .engine
                .child_mut(self.key)
                .is_ok_and(|child| child.is_none())
            {
                return;
            }
            if Instant::now() >= deadline {
                return;
            }
            std::thread::sleep(Duration::from_millis(5));
        }
    }
}

#[test]
fn management_fixture() {
    if std::env::var_os("RECOVERY_MANAGEMENT_FIXTURE").is_none() {
        return;
    }
    assert!(
        std::env::vars_os().all(|(name, _)| name == "RECOVERY_MANAGEMENT_FIXTURE"
            || name.to_string_lossy().eq_ignore_ascii_case("SystemRoot"))
    );
    println!("MANAGEMENT_READY {}", std::process::id());
    io::stdout().flush().unwrap();
    for line in io::stdin().lock().lines() {
        match line.unwrap().as_str() {
            "reject_resume" => std::process::exit(86),
            "ping" => {}
            _ => panic!("unknown management fixture input"),
        }
    }
}

#[tokio::test]
async fn replacement_matches_generation_returns_rejected_candidate_and_preserves_restart_budget() {
    let mut policy = config();
    policy.restart.max_restarts = 1;
    let mut harness = Harness::new(ChildRole::Core, policy);
    harness.start().await;
    harness.kill(RecoveryState::Waiting).await;
    harness.advance_to_retry();
    let active_pid = harness.ready().await;
    let before = harness.engine.snapshot(harness.key).unwrap();
    assert_eq!(before.generation, 2);
    assert_eq!(before.restart.restarts, 1);

    let mut candidate = child_fixture().unwrap();
    let candidate_pid = ready(&mut candidate).await;
    let (error, mut candidate) = harness
        .engine
        .replace_ready(harness.key, 1, 3, candidate)
        .unwrap_err();
    assert_eq!(error, RecoveryError::StaleGeneration);
    assert_eq!(candidate.id(), Some(candidate_pid));
    assert!(
        candidate.try_wait().unwrap().is_none(),
        "rejected candidate was killed or reaped"
    );
    assert_eq!(harness.engine.snapshot(harness.key).unwrap(), before);

    let (error, candidate) = harness
        .engine
        .replace_ready(harness.key, 2, 2, candidate)
        .unwrap_err();
    assert_eq!(error, RecoveryError::StaleGeneration);
    assert_eq!(candidate.id(), Some(candidate_pid));
    assert_eq!(harness.engine.snapshot(harness.key).unwrap(), before);

    let mut old = harness
        .engine
        .replace_ready(harness.key, 2, 3, candidate)
        .unwrap();
    assert_eq!(old.id(), Some(active_pid));
    assert!(
        old.try_wait().unwrap().is_none(),
        "management must retain the old drain owner"
    );
    let after = harness.engine.snapshot(harness.key).unwrap();
    assert_eq!(after.pid, Some(candidate_pid));
    assert_eq!(after.generation, 3);
    assert_eq!(after.restart, before.restart);
    assert_eq!(after.startup_failures, before.startup_failures);
    assert_eq!(
        harness.engine.confirm_ready(harness.key, 2),
        Err(RecoveryError::StaleGeneration)
    );
    old.kill().await.unwrap();
    old.wait().await.unwrap();

    harness
        .kill(RecoveryState::Degraded(DegradedReason::RestartBudget))
        .await;
    assert_eq!(
        harness.host.starts.len(),
        2,
        "management handoff reset the consumed restart budget"
    );
    harness.clock.set(Duration::from_secs(120));
    harness.poll();
    assert_eq!(harness.host.starts.len(), 2);
    harness.cleanup().await;
}

#[tokio::test]
async fn waiting_adoption_preserves_consumed_budget_and_cancels_only_the_pending_retry() {
    let mut policy = config();
    policy.restart.max_restarts = 2;
    policy.restart.circuit_breaker_after = 2;
    let mut harness = Harness::new(ChildRole::Core, policy);
    harness.start().await;
    harness.kill(RecoveryState::Waiting).await;
    harness.advance_to_retry();
    harness.ready().await;
    harness.kill(RecoveryState::Waiting).await;
    let before = harness.engine.snapshot(harness.key).unwrap();
    assert_eq!(before.generation, 2);
    assert_eq!(before.pid, None);
    assert_eq!(before.restart.restarts, 1);
    assert_eq!(before.restart.failures, 2);
    assert_eq!(before.startup_failures, 2);
    assert!(matches!(
        before.restart.circuit,
        src::supervisor::CircuitState::Open { .. }
    ));
    let previous_retry = before.restart.retry_at.unwrap();

    let mut candidate = child_fixture().unwrap();
    let candidate_pid = ready(&mut candidate).await;
    let (error, candidate) = harness
        .engine
        .adopt_ready(harness.key, 1, 3, candidate)
        .unwrap_err();
    assert_eq!(error, RecoveryError::StaleGeneration);
    assert_eq!(candidate.id(), Some(candidate_pid));
    assert_eq!(harness.engine.snapshot(harness.key).unwrap(), before);
    let (error, candidate) = harness
        .engine
        .replace_ready(harness.key, 2, 3, candidate)
        .unwrap_err();
    assert_eq!(error, RecoveryError::NotRunning);
    assert_eq!(candidate.id(), Some(candidate_pid));
    assert_eq!(harness.engine.snapshot(harness.key).unwrap(), before);

    // An explicit, Ready-checked management candidate can fill the reaped slot;
    // it cannot erase the automatic recovery history that led to Waiting.
    assert!(
        harness
            .engine
            .adopt_ready(harness.key, 2, 3, candidate)
            .unwrap()
            .is_none()
    );
    let after = harness.engine.snapshot(harness.key).unwrap();
    let mut expected_restart = before.restart;
    expected_restart.retry_at = None;
    assert_eq!(after.restart, expected_restart);
    assert_eq!(after.startup_failures, before.startup_failures);
    assert_eq!(after.state, RecoveryState::Running);
    assert_eq!(after.pid, Some(candidate_pid));
    assert_eq!(after.generation, 3);
    assert!(!after.stable);
    assert_eq!(
        harness.engine.confirm_ready(harness.key, 2),
        Err(RecoveryError::StaleGeneration)
    );

    harness.clock.set(previous_retry);
    harness.poll();
    assert_eq!(harness.host.starts.len(), 2);
    assert_eq!(harness.engine.snapshot(harness.key).unwrap(), after);
    harness.kill(RecoveryState::Waiting).await;
    harness.advance_to_retry();
    harness.ready().await;
    assert_eq!(
        harness
            .engine
            .snapshot(harness.key)
            .unwrap()
            .restart
            .restarts,
        2
    );
    assert_eq!(harness.engine.snapshot(harness.key).unwrap().generation, 4);
    harness
        .kill(RecoveryState::Degraded(DegradedReason::RestartBudget))
        .await;
    assert_eq!(harness.host.starts.len(), 3);
    harness.cleanup().await;
}

#[tokio::test]
async fn ready_adoption_cannot_clear_degraded_and_returns_the_live_candidate() {
    let mut policy = config();
    policy.restart.max_restarts = 0;
    let mut harness = Harness::new(ChildRole::Core, policy);
    harness.start().await;
    harness
        .kill(RecoveryState::Degraded(DegradedReason::RestartBudget))
        .await;
    let before = harness.engine.snapshot(harness.key).unwrap();
    let mut candidate = child_fixture().unwrap();
    let candidate_pid = ready(&mut candidate).await;
    let (error, mut candidate) = harness
        .engine
        .adopt_ready(harness.key, 1, 2, candidate)
        .unwrap_err();
    assert_eq!(error, RecoveryError::NotRunning);
    assert_eq!(candidate.id(), Some(candidate_pid));
    assert!(candidate.try_wait().unwrap().is_none());
    assert_eq!(harness.engine.snapshot(harness.key).unwrap(), before);
    harness.clock.set(Duration::from_secs(120));
    harness.poll();
    assert_eq!(harness.engine.snapshot(harness.key).unwrap(), before);
    assert_eq!(harness.host.starts.len(), 1);
    candidate.kill().await.unwrap();
    candidate.wait().await.unwrap();
    harness.cleanup().await;
}

#[tokio::test]
async fn running_adoption_returns_the_drain_owner_and_never_overrides_a_stop() {
    let mut harness = Harness::new(ChildRole::Core, config());
    let mut candidate = child_fixture().unwrap();
    let candidate_pid = ready(&mut candidate).await;
    let unstarted = harness.engine.snapshot(harness.key).unwrap();
    let (error, candidate) = harness
        .engine
        .adopt_ready(harness.key, 0, 1, candidate)
        .unwrap_err();
    assert_eq!(error, RecoveryError::NotRunning);
    assert_eq!(candidate.id(), Some(candidate_pid));
    assert_eq!(harness.engine.snapshot(harness.key).unwrap(), unstarted);
    let old_pid = harness.start().await;
    let before = harness.engine.snapshot(harness.key).unwrap();
    let mut old = harness
        .engine
        .adopt_ready(harness.key, 1, 2, candidate)
        .unwrap()
        .expect("a live old generation remains owned by its drain caller");
    assert_eq!(old.id(), Some(old_pid));
    assert!(old.try_wait().unwrap().is_none());
    let after = harness.engine.snapshot(harness.key).unwrap();
    assert_eq!(after.pid, Some(candidate_pid));
    assert_eq!(after.generation, 2);
    assert_eq!(after.restart, before.restart);
    old.kill().await.unwrap();
    old.wait().await.unwrap();

    let mut rejected = child_fixture().unwrap();
    let rejected_pid = ready(&mut rejected).await;
    harness.engine.stop(harness.key).unwrap();
    let stopping = harness.engine.snapshot(harness.key).unwrap();
    assert_eq!(stopping.state, RecoveryState::Stopping);
    let (error, rejected) = harness
        .engine
        .adopt_ready(harness.key, 2, 3, rejected)
        .unwrap_err();
    assert_eq!(error, RecoveryError::NotRunning);
    assert_eq!(rejected.id(), Some(rejected_pid));
    assert_eq!(harness.engine.snapshot(harness.key).unwrap(), stopping);
    harness.kill(RecoveryState::Stopped).await;
    let stopped = harness.engine.snapshot(harness.key).unwrap();
    let (error, mut rejected) = harness
        .engine
        .adopt_ready(harness.key, 2, 3, rejected)
        .unwrap_err();
    assert_eq!(error, RecoveryError::NotRunning);
    assert_eq!(rejected.id(), Some(rejected_pid));
    assert!(rejected.try_wait().unwrap().is_none());
    assert_eq!(harness.engine.snapshot(harness.key).unwrap(), stopped);
    assert_eq!(harness.host.starts.len(), 1);
    rejected.kill().await.unwrap();
    rejected.wait().await.unwrap();
    harness.cleanup().await;
}

#[tokio::test]
async fn admitted_failed_update_generations_are_not_reused_by_automatic_recovery() {
    let mut harness = Harness::new(ChildRole::Core, config());
    harness.start().await;
    let before = harness.engine.snapshot(harness.key).unwrap();
    // These are reservations for management candidates which failed before
    // promotion. Reserving them must neither alter nor invalidate active Ready.
    harness
        .engine
        .advance_generation_floor(harness.key, 5)
        .unwrap();
    harness
        .engine
        .advance_generation_floor(harness.key, 3)
        .unwrap();
    assert_eq!(harness.engine.snapshot(harness.key).unwrap(), before);
    harness.engine.confirm_ready(harness.key, 1).unwrap();
    let mut candidate = child_fixture().unwrap();
    let pid = ready(&mut candidate).await;
    let (error, mut candidate) = harness
        .engine
        .adopt_ready(harness.key, 1, 4, candidate)
        .unwrap_err();
    assert_eq!(error, RecoveryError::StaleGeneration);
    assert_eq!(candidate.id(), Some(pid));
    assert_eq!(harness.engine.snapshot(harness.key).unwrap(), before);
    candidate.kill().await.unwrap();
    candidate.wait().await.unwrap();
    harness.kill(RecoveryState::Waiting).await;
    harness.advance_to_retry();
    harness.ready().await;
    assert_eq!(harness.engine.snapshot(harness.key).unwrap().generation, 6);
    assert_eq!(
        harness
            .host
            .starts
            .iter()
            .map(|request| request.generation)
            .collect::<Vec<_>>(),
        vec![1, 6]
    );
    assert_eq!(harness.host.identify_requests, 0);
    harness.cleanup().await;
}

#[tokio::test]
async fn generation_floor_overflow_and_stopping_never_accept_a_management_replacement() {
    let mut harness = Harness::new(ChildRole::Core, config());
    harness.start().await;
    harness.engine.stop(harness.key).unwrap();
    let before = harness.engine.snapshot(harness.key).unwrap();
    let mut candidate = child_fixture().unwrap();
    let pid = ready(&mut candidate).await;
    let (error, mut candidate) = harness
        .engine
        .replace_ready(harness.key, 1, 2, candidate)
        .unwrap_err();
    assert_eq!(error, RecoveryError::NotRunning);
    assert_eq!(candidate.id(), Some(pid));
    assert_eq!(harness.engine.snapshot(harness.key).unwrap(), before);
    candidate.kill().await.unwrap();
    candidate.wait().await.unwrap();
    harness.kill(RecoveryState::Stopped).await;
    harness
        .engine
        .advance_generation_floor(harness.key, u64::MAX)
        .unwrap();
    harness.engine.start_stopped(harness.key).unwrap();
    harness.poll();
    assert_eq!(
        harness.engine.snapshot(harness.key).unwrap().state,
        RecoveryState::Degraded(DegradedReason::GenerationExhausted)
    );
    assert_eq!(harness.host.starts.len(), 1);
    harness.cleanup().await;
}

#[tokio::test]
async fn rejected_resume_is_reaped_before_a_new_identify_admission_can_start_a_gateway() {
    let mut harness = Harness::new(ChildRole::Gateway, config());
    harness.host.resumable = true;
    let old_pid = harness.start().await;
    assert_eq!(harness.host.starts[0].mode, StartMode::GatewayResume);
    assert_eq!(harness.host.identify_requests, 0);
    // The trusted host invalidates its private Resume hint on the injected
    // rejection. The rejected child exits; it cannot send Identify on that
    // connection. The local recovery policy must then obtain a new grant.
    harness.host.resumable = false;
    harness
        .engine
        .child_mut(harness.key)
        .unwrap()
        .unwrap()
        .stdin
        .as_mut()
        .unwrap()
        .write_all(b"reject_resume\n")
        .await
        .unwrap();
    harness.until(RecoveryState::Waiting).await;
    assert!(harness.engine.child_mut(harness.key).unwrap().is_none());
    assert!(harness.events.iter().any(|event| event.kind
        == RecoveryEventKind::Exited {
            success: false,
            code: Some(86),
        }));
    harness.advance_to_retry();
    for _ in 0..5 {
        harness.poll();
    }
    assert_eq!(harness.host.starts.len(), 1);
    assert!(harness.host.identify_requests > 0);
    assert_eq!(harness.host.identify_consumed, 0);
    assert_eq!(
        harness
            .engine
            .snapshot(harness.key)
            .unwrap()
            .restart
            .restarts,
        0
    );
    assert!(harness.events.iter().any(|event| event.kind
        == RecoveryEventKind::Deferred {
            reason: DeferralReason::IdentifyBudget,
        }));

    harness.host.identify_permits = 1;
    harness.poll();
    let next_pid = harness.ready().await;
    assert_ne!(next_pid, old_pid);
    assert_eq!(harness.host.starts.len(), 2);
    assert_eq!(harness.host.starts[1].mode, StartMode::GatewayIdentify);
    assert_eq!(harness.host.starts[1].generation, 2);
    assert_eq!(harness.host.identify_consumed, 1);
    assert_eq!(
        harness
            .engine
            .snapshot(harness.key)
            .unwrap()
            .restart
            .restarts,
        1
    );
    harness.cleanup().await;
}
