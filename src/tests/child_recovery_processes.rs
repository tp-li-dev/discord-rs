//! Real OS children, but no Discord, daemon, remote ownership or production
//! fencing. Each fixture holds one loopback port as its local Active resource.
//! Replacements must observe that resource released before they can occupy it.

use src::{
    child_recovery::{
        ChildKey, ChildRecovery, ChildRole, DegradedReason, IdentifyAdmission, RecoveryConfig,
        RecoveryError, RecoveryEvent, RecoveryEventKind, RecoveryHost, RecoveryState, StartMode,
        StartRequest,
    },
    protocol::BotId,
    supervisor::{CircuitState, Clock, RestartPolicy, Sleep},
};
use std::{
    collections::{HashMap, HashSet},
    io::{self, BufRead, Write},
    net::{Ipv4Addr, TcpListener},
    process::Stdio,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    process::{Child, ChildStdin, ChildStdout, Command},
};

const DEADLINE: Duration = Duration::from_secs(15);

#[derive(Clone, Default)]
struct ManualClock(Arc<AtomicU64>);

impl ManualClock {
    fn set(&self, time: Duration) {
        self.0
            .store(time.as_millis().try_into().unwrap(), Ordering::SeqCst);
    }
}

impl Clock for ManualClock {
    fn now(&self) -> Duration {
        Duration::from_millis(self.0.load(Ordering::SeqCst))
    }

    fn sleep_until(&self, _: Duration) -> Sleep<'_> {
        Box::pin(std::future::pending())
    }
}

fn key(bot: u64, shard_id: u32, role: ChildRole) -> ChildKey {
    ChildKey {
        bot_id: BotId(bot),
        shard_id,
        role,
    }
}

fn config() -> RecoveryConfig {
    RecoveryConfig {
        restart: RestartPolicy {
            max_restarts: 10,
            max_restarts_in_window: 10,
            window: Duration::from_secs(10),
            initial_backoff: Duration::from_millis(10),
            max_backoff: Duration::from_millis(40),
            circuit_breaker_after: 2,
            circuit_open_for: Duration::from_millis(100),
        },
        stable_for: Duration::from_millis(100),
        max_startup_failures: 6,
    }
}

#[derive(Default)]
struct FixtureHost {
    starts: Vec<StartRequest>,
    inputs: HashMap<ChildKey, ChildStdin>,
    outputs: HashMap<ChildKey, BufReader<ChildStdout>>,
    ports: HashMap<ChildKey, u16>,
    released_before_restart: usize,
}

impl RecoveryHost for FixtureHost {
    fn authorized(&mut self, _: ChildKey) -> bool {
        // This fixture has one local owner. It makes no quorum/lease claim.
        true
    }

    fn resume_available(&mut self, _: ChildKey) -> bool {
        true
    }

    fn try_acquire_identify(&mut self, _: ChildKey) -> IdentifyAdmission {
        panic!("Core and Resume-only fixtures must not consume Identify")
    }

    fn cancel_identify(&mut self, _: ChildKey) {}

    fn spawn(&mut self, request: StartRequest) -> io::Result<Child> {
        let port = self.ports.get(&request.key).copied().unwrap_or(0);
        if port != 0 {
            // A fixture never voluntarily releases this listener while alive.
            // This fails if a previous local Active still owns the resource.
            let probe = TcpListener::bind((Ipv4Addr::LOCALHOST, port))
                .expect("previous child still owns the Active port before replacement");
            drop(probe);
            self.released_before_restart += 1;
        }
        assert_eq!(
            request.mode,
            match request.key.role {
                ChildRole::Core => StartMode::Core,
                ChildRole::Gateway => StartMode::GatewayResume,
            }
        );
        let mut command = Command::new(std::env::current_exe()?);
        command
            .args([
                "--exact",
                "fixture_process",
                "--nocapture",
                "--test-threads=1",
            ])
            .env_clear()
            .env("RECOVERY_FIXTURE", "1")
            .env("RECOVERY_PORT", port.to_string())
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
        let mut child = command.spawn()?;
        self.inputs.insert(request.key, child.stdin.take().unwrap());
        self.outputs
            .insert(request.key, BufReader::new(child.stdout.take().unwrap()));
        self.starts.push(request);
        Ok(child)
    }
}

struct Harness {
    clock: ManualClock,
    recovery: ChildRecovery<ManualClock>,
    host: FixtureHost,
    keys: Vec<ChildKey>,
    events: Vec<RecoveryEvent>,
}

impl Harness {
    fn new(keys: &[ChildKey], config: RecoveryConfig) -> Self {
        let clock = ManualClock::default();
        let mut recovery = ChildRecovery::new(clock.clone(), config, keys.len()).unwrap();
        for &key in keys {
            recovery.register(key, 0).unwrap();
        }
        Self {
            clock,
            recovery,
            host: FixtureHost::default(),
            keys: keys.to_vec(),
            events: Vec::new(),
        }
    }

    fn poll(&mut self) {
        self.events.extend(self.recovery.poll(&mut self.host));
    }

    async fn line(&mut self, key: ChildKey, prefix: &str) -> String {
        tokio::time::timeout(DEADLINE, async {
            let output = self.host.outputs.get_mut(&key).unwrap();
            loop {
                let mut line = String::new();
                assert_ne!(
                    output.read_line(&mut line).await.unwrap(),
                    0,
                    "fixture exited before its expected response"
                );
                // libtest may print its test name on the same line.
                if let Some(index) = line.find(prefix) {
                    return line[index..].trim().to_owned();
                }
            }
        })
        .await
        .expect("fixture response deadline exceeded")
    }

    async fn ready(&mut self, key: ChildKey) -> u32 {
        let line = self.line(key, "RECOVERY_READY ").await;
        let fields = line.split_whitespace().collect::<Vec<_>>();
        assert_eq!(fields.len(), 3);
        let pid: u32 = fields[1].parse().unwrap();
        let port: u16 = fields[2].parse().unwrap();
        assert_eq!(self.recovery.snapshot(key).unwrap().pid, Some(pid));
        if let Some(previous) = self.host.ports.insert(key, port) {
            assert_eq!(port, previous, "replacement must use the same Active port");
        }
        assert!(TcpListener::bind((Ipv4Addr::LOCALHOST, port)).is_err());
        pid
    }

    async fn start_all(&mut self) -> HashMap<ChildKey, u32> {
        self.poll();
        let mut pids = HashMap::new();
        for key in self.keys.clone() {
            pids.insert(key, self.ready(key).await);
        }
        assert_eq!(
            pids.values().copied().collect::<HashSet<_>>().len(),
            pids.len()
        );
        pids
    }

    async fn send(&mut self, key: ChildKey, command: &[u8]) {
        tokio::time::timeout(DEADLINE, async {
            let input = self.host.inputs.get_mut(&key).unwrap();
            input.write_all(command).await.unwrap();
            input.flush().await.unwrap();
        })
        .await
        .expect("fixture input deadline exceeded");
    }

    async fn ping(&mut self, key: ChildKey, pid: u32) {
        self.send(key, b"ping\n").await;
        assert_eq!(
            self.line(key, "RECOVERY_ALIVE ").await,
            format!("RECOVERY_ALIVE {pid}")
        );
    }

    fn kill(&mut self, key: ChildKey) {
        self.recovery
            .child_mut(key)
            .unwrap()
            .unwrap()
            .start_kill()
            .unwrap();
    }

    async fn until(&mut self, key: ChildKey, state: RecoveryState) {
        tokio::time::timeout(DEADLINE, async {
            loop {
                self.poll();
                if self.recovery.snapshot(key).unwrap().state == state {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("child recovery state deadline exceeded");
    }

    fn due(&mut self, key: ChildKey) {
        self.clock.set(
            self.recovery
                .snapshot(key)
                .unwrap()
                .restart
                .retry_at
                .unwrap(),
        );
        self.poll();
    }

    fn assert_live(&mut self, key: ChildKey, pid: u32) {
        let child = self.recovery.child_mut(key).unwrap().unwrap();
        assert_eq!(child.id(), Some(pid));
        assert!(child.try_wait().unwrap().is_none());
    }

    async fn cleanup(&mut self) {
        for key in self.keys.clone() {
            self.recovery.stop(key).unwrap();
            if let Some(child) = self.recovery.child_mut(key).unwrap() {
                child.start_kill().unwrap();
            }
        }
        tokio::time::timeout(DEADLINE, async {
            loop {
                self.poll();
                if self
                    .keys
                    .iter()
                    .all(|key| self.recovery.child_mut(*key).unwrap().is_none())
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("owned fixture children were not reaped");
    }
}

impl Drop for Harness {
    fn drop(&mut self) {
        // Panic cleanup uses only handles owned by this fixture, never PID lookup
        // or an external kill command. try_wait reaps even on a current-thread
        // runtime, without needing an async task to be scheduled during unwind.
        for &key in &self.keys {
            let _ = self.recovery.stop(key);
            if let Ok(Some(child)) = self.recovery.child_mut(key) {
                let _ = child.start_kill();
            }
        }
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            self.poll();
            if self
                .keys
                .iter()
                .all(|key| self.recovery.child_mut(*key).unwrap().is_none())
            {
                return;
            }
            if Instant::now() >= deadline {
                // kill_on_drop is a final safeguard. Normal test paths require
                // verified reap in cleanup and cannot pass using this fallback.
                if !std::thread::panicking() {
                    panic!("owned fixture cleanup failed");
                }
                return;
            }
            std::thread::sleep(Duration::from_millis(5));
        }
    }
}

/// The same test executable is the fixture, so no secondary Cargo/rustc build,
/// dependency selection, script shell, or generated file is needed.
#[test]
fn fixture_process() {
    if std::env::var_os("RECOVERY_FIXTURE").is_none() {
        return;
    }
    assert!(std::env::vars_os().all(|(name, _)| {
        name == "RECOVERY_FIXTURE"
            || name == "RECOVERY_PORT"
            || name.to_string_lossy().eq_ignore_ascii_case("SystemRoot")
    }));
    let port = std::env::var("RECOVERY_PORT")
        .unwrap()
        .parse::<u16>()
        .unwrap();
    let active = TcpListener::bind((Ipv4Addr::LOCALHOST, port)).unwrap();
    println!(
        "RECOVERY_READY {} {}",
        std::process::id(),
        active.local_addr().unwrap().port()
    );
    io::stdout().flush().unwrap();
    for line in io::stdin().lock().lines() {
        match line.unwrap().as_str() {
            "ping" => {
                println!("RECOVERY_ALIVE {}", std::process::id());
                io::stdout().flush().unwrap();
            }
            "crash" => std::process::exit(86),
            "clean" => return,
            _ => panic!("unknown fixture command"),
        }
    }
}

#[tokio::test]
async fn core_and_gateway_recover_independently_without_overlapping_active_ports() {
    let gateway = key(1, 0, ChildRole::Gateway);
    let core = key(1, 0, ChildRole::Core);
    let keys = [
        gateway,
        core,
        key(1, 1, ChildRole::Gateway),
        key(1, 1, ChildRole::Core),
        key(2, 0, ChildRole::Gateway),
        key(2, 0, ChildRole::Core),
    ];
    let mut harness = Harness::new(&keys, config());
    let mut pids = harness.start_all().await;
    for target in [core, gateway] {
        let starts = harness.host.starts.len();
        harness.kill(target);
        harness.until(target, RecoveryState::Waiting).await;
        assert_eq!(
            harness.host.starts.len(),
            starts,
            "backoff must precede spawn"
        );
        assert!(harness.recovery.child_mut(target).unwrap().is_none());
        for (&key, &pid) in &pids {
            if key != target {
                harness.assert_live(key, pid);
                harness.ping(key, pid).await;
            }
        }
        harness.due(target);
        let replacement = harness.ready(target).await;
        assert_ne!(replacement, pids[&target]);
        pids.insert(target, replacement);
        let exit = harness
            .events
            .iter()
            .position(|event| {
                event.key == target
                    && event.generation == 1
                    && matches!(event.kind, RecoveryEventKind::Exited { .. })
            })
            .unwrap();
        let restart = harness
            .events
            .iter()
            .position(|event| {
                event.key == target
                    && event.generation == 2
                    && matches!(event.kind, RecoveryEventKind::Started { .. })
            })
            .unwrap();
        assert!(
            exit < restart,
            "exit must be collected before replacement starts"
        );
    }
    assert_eq!(harness.host.released_before_restart, 2);
    for key in keys {
        assert_eq!(
            harness.recovery.snapshot(key).unwrap().restart.restarts,
            u32::from(key == core || key == gateway)
        );
    }
    harness.cleanup().await;
}

#[tokio::test]
async fn live_child_and_late_poll_never_launch_a_second_generation() {
    let core = key(1, 0, ChildRole::Core);
    let mut harness = Harness::new(&[core], config());
    let pids = harness.start_all().await;
    harness.clock.set(Duration::from_secs(60));
    for _ in 0..8 {
        harness.poll();
        harness.assert_live(core, pids[&core]);
    }
    harness.ping(core, pids[&core]).await;
    assert_eq!(harness.host.starts.len(), 1);
    assert_eq!(harness.recovery.snapshot(core).unwrap().generation, 1);
    assert_eq!(
        harness.recovery.register(core, 1),
        Err(RecoveryError::DuplicateChild)
    );
    harness.cleanup().await;
}

#[tokio::test]
async fn startup_crash_loop_latches_degraded_and_stops_spawning() {
    let core = key(1, 0, ChildRole::Core);
    let mut policy = config();
    policy.max_startup_failures = 3;
    let mut harness = Harness::new(&[core], policy);
    harness.start_all().await;
    for attempt in 1..=3 {
        harness.kill(core);
        let expected = if attempt == 3 {
            RecoveryState::Degraded(DegradedReason::StartupCrashLoop)
        } else {
            RecoveryState::Waiting
        };
        harness.until(core, expected).await;
        if attempt < 3 {
            harness.due(core);
            harness.ready(core).await;
        }
    }
    let snapshot = harness.recovery.snapshot(core).unwrap();
    assert_eq!(snapshot.startup_failures, 3);
    assert_eq!(snapshot.restart.restarts, 2);
    assert_eq!(snapshot.restart.retry_at, None);
    assert_eq!(snapshot.pid, None);
    assert_eq!(
        harness.recovery.start_stopped(core),
        Err(RecoveryError::NotStopped),
        "manual start must not clear a latched Degraded state"
    );
    harness.clock.set(Duration::from_secs(60));
    for _ in 0..8 {
        harness.poll();
    }
    assert_eq!(harness.host.starts.len(), 3);
    assert_eq!(
        harness
            .events
            .iter()
            .filter(|event| matches!(
                event.kind,
                RecoveryEventKind::Degraded {
                    reason: DegradedReason::StartupCrashLoop
                }
            ))
            .count(),
        1
    );
    harness.cleanup().await;
}

#[tokio::test]
async fn explicit_stop_suppresses_recovery_after_nonzero_exit() {
    let core = key(1, 0, ChildRole::Core);
    let mut harness = Harness::new(&[core], config());
    harness.start_all().await;
    harness.recovery.stop(core).unwrap();
    harness.send(core, b"crash\n").await;
    harness.until(core, RecoveryState::Stopped).await;
    harness.clock.set(Duration::from_secs(60));
    harness.poll();
    assert_eq!(harness.host.starts.len(), 1);
    assert_eq!(harness.recovery.snapshot(core).unwrap().restart.restarts, 0);
    assert!(harness.events.iter().any(|event| matches!(
        event.kind,
        RecoveryEventKind::Exited {
            success: false,
            code: Some(86)
        }
    )));
    harness.cleanup().await;
}

#[tokio::test]
async fn half_open_closes_only_after_current_generation_ready_and_stable_liveness() {
    let core = key(1, 0, ChildRole::Core);
    let mut policy = config();
    policy.restart.circuit_breaker_after = 1;
    let mut harness = Harness::new(&[core], policy);
    harness.start_all().await;
    harness.kill(core);
    harness.until(core, RecoveryState::Waiting).await;
    assert!(matches!(
        harness.recovery.snapshot(core).unwrap().restart.circuit,
        CircuitState::Open { .. }
    ));
    harness.due(core);
    harness.ready(core).await;
    assert_eq!(
        harness.recovery.snapshot(core).unwrap().restart.circuit,
        CircuitState::HalfOpen
    );
    assert_eq!(
        harness.recovery.confirm_ready(core, 1),
        Err(RecoveryError::StaleGeneration)
    );
    harness.recovery.confirm_ready(core, 2).unwrap();
    let ready_at = harness.clock.now();
    harness
        .clock
        .set(ready_at + policy.stable_for - Duration::from_millis(1));
    harness.poll();
    assert_eq!(
        harness.recovery.snapshot(core).unwrap().restart.circuit,
        CircuitState::HalfOpen
    );
    harness.clock.set(ready_at + policy.stable_for);
    harness.poll();
    let stable = harness.recovery.snapshot(core).unwrap();
    assert!(stable.stable);
    assert_eq!(stable.restart.circuit, CircuitState::Closed);
    assert_eq!(stable.startup_failures, 0);
    assert_eq!(stable.restart.failures, 0);
    assert_eq!(
        stable.restart.restarts, 1,
        "stability must not replenish budget"
    );
    assert_eq!(
        harness.recovery.start_stopped(core),
        Err(RecoveryError::NotStopped),
        "a live child cannot be manually started again"
    );
    harness.recovery.stop(core).unwrap();
    assert!(!harness.recovery.snapshot(core).unwrap().stable);
    assert_eq!(
        harness.recovery.snapshot(core).unwrap().state,
        RecoveryState::Stopping
    );
    assert_eq!(
        harness.recovery.start_stopped(core),
        Err(RecoveryError::NotStopped),
        "a stop request does not prove that the child has exited"
    );
    harness.send(core, b"clean\n").await;
    harness.until(core, RecoveryState::Stopped).await;
    harness.recovery.start_stopped(core).unwrap();
    let admitted = harness.recovery.snapshot(core).unwrap();
    assert_eq!(admitted.generation, 2);
    assert_eq!(admitted.restart.restarts, 1);
    assert_eq!(admitted.state, RecoveryState::Waiting);
    harness.poll();
    harness.ready(core).await;
    let restarted = harness.recovery.snapshot(core).unwrap();
    assert_eq!(restarted.generation, 3);
    assert_eq!(restarted.restart.restarts, 2);
    assert!(!restarted.stable);
    harness.cleanup().await;
}

#[tokio::test]
async fn exit_before_late_poll_cannot_close_a_half_open_circuit() {
    let core = key(1, 0, ChildRole::Core);
    let mut policy = config();
    policy.restart.circuit_breaker_after = 1;
    let mut harness = Harness::new(&[core], policy);
    harness.start_all().await;
    harness.kill(core);
    harness.until(core, RecoveryState::Waiting).await;
    harness.due(core);
    harness.ready(core).await;
    harness.recovery.confirm_ready(core, 2).unwrap();
    harness.kill(core);
    tokio::time::timeout(
        DEADLINE,
        harness.recovery.child_mut(core).unwrap().unwrap().wait(),
    )
    .await
    .unwrap()
    .unwrap();
    // The OS exit is already known; a delayed policy poll must process it before
    // interpreting elapsed time as healthy readiness.
    harness.clock.set(Duration::from_secs(60));
    harness.poll();
    let snapshot = harness.recovery.snapshot(core).unwrap();
    assert_eq!(snapshot.startup_failures, 2);
    assert!(!snapshot.stable);
    assert!(matches!(
        snapshot.restart.circuit,
        CircuitState::Open { .. }
    ));
    assert!(
        !harness
            .events
            .iter()
            .any(|event| event.kind == RecoveryEventKind::Stable)
    );
    harness.cleanup().await;
}

#[tokio::test]
async fn clock_fault_preserves_and_reaps_live_handle_without_restarting() {
    let core = key(1, 0, ChildRole::Core);
    let mut harness = Harness::new(&[core], config());
    let pids = harness.start_all().await;
    harness.clock.set(Duration::from_secs(10));
    harness.poll();
    harness.clock.set(Duration::from_secs(9));
    harness.poll();
    assert_eq!(
        harness.recovery.snapshot(core).unwrap().state,
        RecoveryState::Degraded(DegradedReason::ClockRegressed)
    );
    harness.assert_live(core, pids[&core]);
    harness.kill(core);
    harness.cleanup().await;
    assert_eq!(harness.host.starts.len(), 1);
    assert!(harness.recovery.child_mut(core).unwrap().is_none());
}

#[tokio::test]
async fn unrequested_success_exit_is_recovered_as_a_service_failure() {
    let core = key(1, 0, ChildRole::Core);
    let mut harness = Harness::new(&[core], config());
    harness.start_all().await;
    harness.send(core, b"clean\n").await;
    harness.until(core, RecoveryState::Waiting).await;
    assert!(harness.events.iter().any(|event| matches!(
        event.kind,
        RecoveryEventKind::Exited {
            success: true,
            code: Some(0)
        }
    )));
    harness.due(core);
    harness.ready(core).await;
    assert_eq!(harness.host.starts.len(), 2);
    harness.cleanup().await;
}
