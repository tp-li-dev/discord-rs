//! Legacy Daemon adapter boundaries, using owned process-only Gateway fixtures.
//! No Discord, Core, remote management, or distributed ownership is simulated.
use src::{
    child_recovery::{ChildRole, DegradedReason, RecoveryEventKind},
    daemon::{Daemon, DaemonError, ProcessSnapshot, ProcessState, ShardKey, StopMode},
    placement::LocalShardIntent,
    shard_runtime::{self, ShardRuntimeSpec},
    supervisor::{Clock, RestartPolicy, Sleep},
};
use std::{
    ffi::OsString,
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

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

struct Root(PathBuf);

impl Root {
    fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let root = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("target")
            .join("daemon-recovery-wiring")
            .join(format!(
                "{}-{}",
                std::process::id(),
                NEXT.fetch_add(1, Ordering::Relaxed)
            ));
        std::fs::create_dir_all(&root).unwrap();
        Self(root)
    }

    fn ready(&self, bot: &str) -> PathBuf {
        self.0.join(format!("{bot}.ready"))
    }

    fn exit(&self, bot: &str) -> PathBuf {
        self.0.join(format!("{bot}.exit"))
    }
}

fn key(bot: &str) -> ShardKey {
    ShardKey {
        bot_id: bot.into(),
        shard_id: 0,
    }
}

fn credentials(name: &str) -> Option<OsString> {
    match name {
        "TOKEN_A" => Some("wiring-fixture-a-only".into()),
        "TOKEN_B" => Some("wiring-fixture-b-only".into()),
        _ => None,
    }
}

fn specs(root: &Root, bots: &[&str]) -> Vec<ShardRuntimeSpec> {
    let intents: Vec<_> = bots
        .iter()
        .map(|bot| LocalShardIntent {
            bot_id: (*bot).into(),
            shard_id: 0,
            total_shards: 1,
            token_env: format!("TOKEN_{bot}"),
        })
        .collect();
    let mut specs = shard_runtime::plan(&intents, &root.0).unwrap();
    for spec in &mut specs {
        spec.gateway.program = std::env::current_exe().unwrap().into_os_string();
        spec.gateway.args = [
            "--exact",
            "process_only_gateway_fixture",
            "--nocapture",
            "--test-threads=1",
        ]
        .into_iter()
        .map(OsString::from)
        .collect();
        spec.gateway.environment.variables.extend([
            ("DAEMON_RECOVERY_WIRING_FIXTURE".into(), "1".into()),
            (
                "FIXTURE_READY".into(),
                root.ready(&spec.bot_id).to_str().unwrap().into(),
            ),
            (
                "FIXTURE_EXIT".into(),
                root.exit(&spec.bot_id).to_str().unwrap().into(),
            ),
        ]);
        spec.core.program = root.0.join("core-must-not-start").into_os_string();
    }
    specs
}

fn policy() -> RestartPolicy {
    RestartPolicy {
        max_restarts: 4,
        max_restarts_in_window: 4,
        window: Duration::from_secs(30),
        initial_backoff: Duration::from_secs(1),
        max_backoff: Duration::from_secs(4),
        circuit_breaker_after: 10,
        circuit_open_for: Duration::from_secs(10),
    }
}

struct Managed(Daemon<ManualClock>);

impl Managed {
    fn new(root: &Root, bots: &[&str], clock: &ManualClock) -> Self {
        Self(Daemon::with_clock(specs(root, bots), policy(), clock.clone()).unwrap())
    }

    async fn shutdown(&mut self) {
        for snapshot in self.0.snapshots() {
            self.0.stop(&snapshot.key, StopMode::Force).unwrap();
        }
        eventually(|| {
            // A latched clock error must still permit owned handle reaping.
            let _ = self.0.poll(&credentials);
            self.0
                .snapshots()
                .iter()
                .all(|snapshot| !self.0.has_child(&snapshot.key))
        })
        .await;
    }
}

impl Drop for Managed {
    fn drop(&mut self) {
        for snapshot in self.0.snapshots() {
            let _ = self.0.stop(&snapshot.key, StopMode::Force);
        }
        let deadline = Instant::now() + Duration::from_secs(3);
        loop {
            let _ = self.0.poll(&credentials);
            if self
                .0
                .snapshots()
                .iter()
                .all(|snapshot| !self.0.has_child(&snapshot.key))
                || Instant::now() >= deadline
            {
                return;
            }
            std::thread::sleep(Duration::from_millis(5));
        }
    }
}

fn snapshot(daemon: &Daemon<ManualClock>, bot: &str) -> ProcessSnapshot {
    daemon
        .snapshots()
        .into_iter()
        .find(|snapshot| snapshot.key == key(bot))
        .unwrap()
}

async fn eventually(mut check: impl FnMut() -> bool) {
    tokio::time::timeout(Duration::from_secs(15), async {
        while !check() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("legacy recovery wiring deadline");
}

async fn ready(root: &Root, bot: &str, pid: u32) {
    eventually(|| {
        std::fs::read_to_string(root.ready(bot))
            .ok()
            .and_then(|text| text.parse::<u32>().ok())
            == Some(pid)
    })
    .await;
}

/// Same executable, isolated environment, fixed local stop marker, no compiler
/// subprocess. Its only persisted output is its own numeric PID after validation.
#[test]
fn process_only_gateway_fixture() {
    if std::env::var_os("DAEMON_RECOVERY_WIRING_FIXTURE").is_none() {
        return;
    }
    let bot = std::env::var("BOT_ID").unwrap();
    let expected = match bot.as_str() {
        "A" => ("TOKEN_A", "wiring-fixture-a-only", "TOKEN_B"),
        "B" => ("TOKEN_B", "wiring-fixture-b-only", "TOKEN_A"),
        _ => std::process::exit(91),
    };
    if std::env::var(expected.0).ok().as_deref() != Some(expected.1)
        || std::env::var_os(expected.2).is_some()
        || std::env::var_os("DISCORD_TOKEN").is_some()
        || std::env::var_os("PATH").is_some()
    {
        std::process::exit(91);
    }
    std::fs::write(
        std::env::var_os("FIXTURE_READY").unwrap(),
        std::process::id().to_string(),
    )
    .unwrap();
    let exit = PathBuf::from(std::env::var_os("FIXTURE_EXIT").unwrap());
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        if std::fs::read(&exit).is_ok_and(|bytes| bytes == b"17") {
            std::process::exit(17);
        }
        if Instant::now() >= deadline {
            std::process::exit(92);
        }
        std::thread::sleep(Duration::from_millis(5));
    }
}

#[tokio::test]
async fn unrelated_retry_failure_does_not_turn_a_successful_manual_start_into_an_error() {
    let root = Root::new();
    let clock = ManualClock::default();
    let mut daemon = Managed::new(&root, &["A", "B"], &clock);
    let missing_reads = AtomicU64::new(0);
    let source = |name: &str| {
        if name == "TOKEN_A" {
            missing_reads.fetch_add(1, Ordering::Relaxed);
            None
        } else {
            credentials(name)
        }
    };
    assert_eq!(
        daemon.0.start(&key("A"), &source),
        Err(DaemonError::MissingCredential)
    );
    assert_eq!(snapshot(&daemon.0, "A").retry_at_ms, Some(1000));
    assert_eq!(missing_reads.load(Ordering::Relaxed), 1);

    // B's explicit start also polls the already-due retry of A. A must retain
    // its failure/retry status without poisoning the result for target B.
    clock.set(1);
    assert_eq!(daemon.0.start(&key("B"), &source), Ok(()));
    assert_eq!(missing_reads.load(Ordering::Relaxed), 2);
    let failed = snapshot(&daemon.0, "A");
    assert_eq!(
        failed.state,
        ProcessState::Failed {
            error: DaemonError::MissingCredential,
            code: None
        }
    );
    assert_eq!(failed.restarts, 1);
    assert_eq!(failed.retry_at_ms, Some(3000));
    assert!(!daemon.0.has_child(&key("A")));
    let started = snapshot(&daemon.0, "B");
    assert_eq!(started.state, ProcessState::Running);
    assert_eq!(started.restarts, 0);
    ready(&root, "B", started.identity.unwrap().pid).await;
    assert!(daemon.0.has_child(&key("B")));
    daemon.shutdown().await;
}

#[tokio::test]
async fn regression_seen_by_manual_start_stays_degraded_when_the_clock_recovers_before_poll() {
    let root = Root::new();
    let clock = ManualClock::default();
    clock.set(10);
    let mut daemon = Managed::new(&root, &["A", "B"], &clock);
    daemon.0.start(&key("A"), &credentials).unwrap();
    let identity = snapshot(&daemon.0, "A").identity.unwrap();
    ready(&root, "A", identity.pid).await;
    daemon.0.take_recovery_events();

    clock.set(9);
    assert_eq!(
        daemon.0.start(&key("B"), &credentials),
        Err(DaemonError::ClockRegressed)
    );
    let immediate = snapshot(&daemon.0, "A");
    assert_eq!(
        immediate.recovery.unwrap()["state"],
        serde_json::json!({"Degraded":"ClockRegressed"})
    );
    assert!(immediate.restart_exhausted);
    assert!(!immediate.desired_running);
    let events = daemon.0.take_recovery_events();
    assert_eq!(
        events
            .iter()
            .filter(|event| event.key == key("A")
                && event.role == ChildRole::Gateway
                && event.kind
                    == RecoveryEventKind::Degraded {
                        reason: DegradedReason::ClockRegressed
                    })
            .count(),
        1
    );

    // The engine must retain the fault observed outside poll. Reading a later
    // non-regressed value must not revive recovery or silently drop telemetry.
    clock.set(11);
    assert_eq!(
        daemon.0.poll(&credentials),
        Err(DaemonError::ClockRegressed)
    );
    let after = snapshot(&daemon.0, "A");
    assert_eq!(
        after.recovery.unwrap()["state"],
        serde_json::json!({"Degraded":"ClockRegressed"})
    );
    assert_eq!(after.identity, Some(identity));
    assert!(after.restart_exhausted);
    assert!(after.retry_at_ms.is_none());
    assert!(
        daemon.0.has_child(&key("A")),
        "clock failure alone must not kill a live child"
    );
    assert!(!daemon.0.has_child(&key("B")));
    assert!(snapshot(&daemon.0, "B").identity.is_none());
    assert!(!root.ready("B").exists());
    assert!(
        !daemon
            .0
            .take_recovery_events()
            .iter()
            .any(|event| matches!(event.kind, RecoveryEventKind::Degraded { .. }))
    );
    daemon.shutdown().await;
}

#[tokio::test]
async fn guarded_legacy_gateway_requires_shared_admission_after_its_first_process_exit() {
    let root = Root::new();
    let clock = ManualClock::default();
    let mut daemon = Managed::new(&root, &["A"], &clock);
    daemon.0.require_shared_recovery_admission();
    daemon.0.start(&key("A"), &credentials).unwrap();
    let identity = snapshot(&daemon.0, "A").identity.unwrap();
    ready(&root, "A", identity.pid).await;
    let mut events = daemon.0.take_recovery_events();
    std::fs::write(root.exit("A"), b"17").unwrap();
    eventually(|| {
        daemon.0.poll(&credentials).unwrap();
        !daemon.0.has_child(&key("A"))
    })
    .await;
    let exited = snapshot(&daemon.0, "A");
    assert_eq!(
        exited.state,
        ProcessState::Failed {
            error: DaemonError::ChildExited,
            code: Some(17)
        }
    );
    assert_eq!(exited.retry_at_ms, Some(1000));

    clock.set(1);
    daemon.0.poll(&credentials).unwrap();
    events.extend(daemon.0.take_recovery_events());
    let refused = snapshot(&daemon.0, "A");
    assert_eq!(
        refused.recovery.unwrap()["state"],
        serde_json::json!({"Degraded":"IdentifyUnavailable"})
    );
    assert_eq!(refused.identity, Some(identity.clone()));
    assert_eq!(refused.restarts, 0);
    assert!(refused.restart_exhausted);
    assert!(refused.retry_at_ms.is_none());
    assert!(!refused.desired_running);
    assert!(events.iter().any(|event| event.key == key("A")
        && event.kind
            == RecoveryEventKind::Degraded {
                reason: DegradedReason::IdentifyUnavailable
            }));
    assert_eq!(
        events
            .iter()
            .filter(|event| matches!(event.kind, RecoveryEventKind::Started { .. }))
            .count(),
        1
    );

    clock.set(100);
    daemon.0.poll(&credentials).unwrap();
    assert!(!daemon.0.has_child(&key("A")));
    assert_eq!(snapshot(&daemon.0, "A").identity, Some(identity));
    assert!(
        daemon
            .0
            .take_recovery_events()
            .iter()
            .all(|event| !matches!(event.kind, RecoveryEventKind::Started { .. }))
    );
    assert_eq!(
        daemon.0.start(&key("A"), &credentials),
        Err(DaemonError::InvalidState)
    );
    daemon.shutdown().await;
}
