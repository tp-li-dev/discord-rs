use src::{
    daemon::*,
    placement::{Config, LocalShardIntent},
    shard_runtime::{self, ShardRuntimeSpec},
    supervisor::{Clock, RestartPolicy, Sleep},
};
use std::{
    collections::HashMap,
    ffi::OsString,
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};

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
        let path = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("target")
            .join("daemon-tests")
            .join(format!(
                "{}-{}",
                std::process::id(),
                NEXT.fetch_add(1, Ordering::Relaxed)
            ));
        std::fs::create_dir_all(&path).unwrap();
        Self(path)
    }
}
impl Drop for Root {
    fn drop(&mut self) {
        let base = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("target")
            .join("daemon-tests");
        if let (Ok(target), Ok(base)) = (self.0.canonicalize(), base.canonicalize())
            && target != base
            && target.starts_with(&base)
        {
            let _ = std::fs::remove_dir_all(target);
        }
    }
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
fn credentials(name: &str) -> Option<OsString> {
    match name {
        "TOKEN_A" => Some("dummy-A".into()),
        "TOKEN_B" => Some("dummy-B".into()),
        _ => None,
    }
}
fn key(bot: &str, shard_id: u32) -> ShardKey {
    ShardKey {
        bot_id: bot.into(),
        shard_id,
    }
}
fn specs(root: &Root, inputs: &[(&str, u32)]) -> Vec<ShardRuntimeSpec> {
    let intents = inputs
        .iter()
        .map(|(bot, shard)| LocalShardIntent {
            bot_id: (*bot).into(),
            shard_id: *shard,
            total_shards: 2,
            token_env: format!("TOKEN_{bot}"),
        })
        .collect::<Vec<_>>();
    let mut specs = shard_runtime::plan(&intents, &root.0).unwrap();
    for s in &mut specs {
        s.gateway.program = std::env::current_exe().unwrap().into_os_string();
        s.gateway.args = [
            "--exact",
            "fixture_process",
            "--nocapture",
            "--test-threads=1",
        ]
        .into_iter()
        .map(OsString::from)
        .collect();
        // Like a real Gateway, the fixture's command line carries its own
        // `--management-path`, which the daemon uses to tell that this child
        // is alive before it would ever discard a leftover reservation. After
        // "--" libtest treats these as (harmless) name filters.
        s.gateway.args.extend([
            OsString::from("--"),
            OsString::from("--management-path"),
            s.gateway.management_path.clone().into_os_string(),
        ]);
        let vars = &mut s.gateway.environment.variables;
        vars.insert("FIXTURE_MODE".into(), "hold".into());
        vars.insert("FIXTURE_LABEL".into(), s.bot_id.clone());
        vars.insert(
            "FIXTURE_READY".into(),
            ready(root, &s.bot_id, s.shard_id).to_str().unwrap().into(),
        );
        vars.insert(
            "FIXTURE_STOP".into(),
            exit_file(root, &s.bot_id, s.shard_id)
                .to_str()
                .unwrap()
                .into(),
        );
        // Core must not be launched by this Gateway-unit skeleton.
        s.core.program = root.0.join("core-must-not-launch").into_os_string();
    }
    specs
}
fn ready(root: &Root, bot: &str, shard: u32) -> PathBuf {
    root.0.join(format!("{bot}-{shard}.ready"))
}
fn exit_file(root: &Root, bot: &str, shard: u32) -> PathBuf {
    root.0.join(format!("{bot}-{shard}.exit"))
}

/// Also acts as a cross-platform child executable; production has no fixture mode.
/// Audit files record booleans only, never credential or environment values.
#[test]
fn fixture_process() {
    let Some(mode) = std::env::var_os("FIXTURE_MODE") else {
        return;
    };
    let label = std::env::var("FIXTURE_LABEL").unwrap();
    let expected_name = format!("TOKEN_{label}");
    let expected = format!("dummy-{label}");
    let isolated = std::env::var(&expected_name).ok().as_deref() == Some(expected.as_str())
        && ["TOKEN_A", "TOKEN_B"]
            .iter()
            .filter(|n| **n != expected_name)
            .all(|n| std::env::var_os(n).is_none())
        && std::env::var_os("DISCORD_TOKEN").is_none()
        && std::env::var_os("PATH").is_none();
    let args: Vec<_> = std::env::args().skip(1).collect();
    let args_exact = args.len() == 7
        && args[..4]
            == [
                "--exact",
                "fixture_process",
                "--nocapture",
                "--test-threads=1",
            ]
        && args[4] == "--"
        && args[5] == "--management-path";
    let ready = std::env::var_os("FIXTURE_READY").unwrap();
    std::fs::write(
        ready,
        serde_json::to_vec(&serde_json::json!({"isolated": isolated, "args_exact": args_exact}))
            .unwrap(),
    )
    .unwrap();
    if !isolated || !args_exact {
        std::process::exit(91);
    }
    if mode == "exit" {
        std::process::exit(17);
    }
    let stop = PathBuf::from(std::env::var_os("FIXTURE_STOP").unwrap());
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    loop {
        if let Ok(text) = std::fs::read_to_string(&stop) {
            std::fs::write(stop.with_extension("ack"), b"acknowledged").unwrap();
            std::process::exit(text.parse().unwrap());
        }
        if std::time::Instant::now() >= deadline {
            std::process::exit(92);
        }
        std::thread::sleep(Duration::from_millis(5));
    }
}

struct Managed(Daemon<ManualClock>);
impl Drop for Managed {
    fn drop(&mut self) {
        for snapshot in self.0.snapshots() {
            let _ = self.0.stop(&snapshot.key, StopMode::Force);
        }
        let _ = self.0.poll(&credentials);
    }
}
fn managed(specs: Vec<ShardRuntimeSpec>, p: RestartPolicy, clock: &ManualClock) -> Managed {
    Managed(Daemon::with_clock(specs, p, clock.clone()).unwrap())
}
async fn eventually(mut test: impl FnMut() -> bool) {
    tokio::time::timeout(Duration::from_secs(10), async {
        while !test() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("child did not reach expected state");
}
async fn ready_audit(path: &Path) {
    eventually(|| {
        std::fs::read(path)
            .ok()
            .and_then(|bytes| serde_json::from_slice::<serde_json::Value>(&bytes).ok())
            .is_some_and(|value| value["isolated"] == true && value["args_exact"] == true)
    })
    .await;
}
async fn terminal(d: &mut Daemon<ManualClock>) {
    eventually(|| {
        let _ = d.poll(&credentials);
        d.snapshots()
            .iter()
            .all(|s| !matches!(s.state, ProcessState::Running))
    })
    .await;
}

#[tokio::test]
async fn real_children_keep_bots_and_shards_isolated_and_stop_independently() {
    let root = Root::new();
    let clock = ManualClock::default();
    let mut d = managed(
        specs(&root, &[("A", 0), ("A", 1), ("B", 0)]),
        policy(),
        &clock,
    );
    d.0.start_all(&credentials).unwrap();
    for (bot, shard) in [("A", 0), ("A", 1), ("B", 0)] {
        ready_audit(&ready(&root, bot, shard)).await;
    }
    let snapshots = d.0.snapshots();
    assert_eq!(snapshots.len(), 3);
    let mut pids = snapshots
        .iter()
        .map(|s| s.identity.as_ref().unwrap().pid)
        .collect::<Vec<_>>();
    pids.sort();
    pids.dedup();
    assert_eq!(pids.len(), 3);
    d.0.stop(&key("A", 0), StopMode::Force).unwrap();
    eventually(|| {
        d.0.poll(&credentials).unwrap();
        matches!(d.0.snapshots()[0].state, ProcessState::Exited { .. })
    })
    .await;
    assert!(
        d.0.snapshots()[1..]
            .iter()
            .all(|s| s.state == ProcessState::Running)
    );
    clock.set(100);
    d.0.poll(&credentials).unwrap();
    assert_eq!(d.0.snapshots()[0].restarts, 0);
    for s in d.0.snapshots() {
        d.0.stop(&s.key, StopMode::Force).unwrap();
    }
    terminal(&mut d.0).await;
}

#[tokio::test]
async fn crash_restarts_after_backoff_with_new_pid_generation_and_fresh_credentials() {
    let root = Root::new();
    let clock = ManualClock::default();
    let mut d = managed(specs(&root, &[("A", 0)]), policy(), &clock);
    let reads = std::sync::atomic::AtomicUsize::new(0);
    let source = |name: &str| {
        reads.fetch_add(1, Ordering::Relaxed);
        credentials(name)
    };
    d.0.start_all(&source).unwrap();
    ready_audit(&ready(&root, "A", 0)).await;
    let first = d.0.snapshots()[0].identity.clone().unwrap();
    std::fs::write(exit_file(&root, "A", 0), "17").unwrap();
    terminal(&mut d.0).await;
    let crashed = d.0.snapshots().remove(0);
    assert!(matches!(
        crashed.state,
        ProcessState::Failed { code: Some(17), .. }
    ));
    assert_eq!(crashed.retry_at_ms, Some(1000));
    assert_eq!(reads.load(Ordering::Relaxed), 1);
    d.0.poll(&source).unwrap();
    assert_eq!(d.0.snapshots()[0].restarts, 0);
    std::fs::remove_file(exit_file(&root, "A", 0)).unwrap();
    std::fs::remove_file(ready(&root, "A", 0)).unwrap();
    clock.set(1);
    d.0.poll(&source).unwrap();
    ready_audit(&ready(&root, "A", 0)).await;
    let restarted = d.0.snapshots().remove(0);
    assert_eq!(restarted.state, ProcessState::Running);
    assert_eq!(restarted.restarts, 1);
    assert_eq!(reads.load(Ordering::Relaxed), 2);
    let next = restarted.identity.unwrap();
    assert_ne!(next.generation, first.generation);
    assert!(next.generation.sequence > first.generation.sequence);
    assert!(next.launch_observed_unix_ms >= first.launch_observed_unix_ms);
    d.0.stop(&key("A", 0), StopMode::Force).unwrap();
    terminal(&mut d.0).await;
}

#[tokio::test]
async fn normal_exit_and_requested_stop_do_not_restart() {
    for code in [0, 17] {
        let root = Root::new();
        let clock = ManualClock::default();
        let mut d = managed(specs(&root, &[("A", 0)]), policy(), &clock);
        d.0.start_all(&credentials).unwrap();
        ready_audit(&ready(&root, "A", 0)).await;
        if code != 0 {
            d.0.stop(&key("A", 0), StopMode::WaitForExit).unwrap();
        }
        assert_eq!(d.0.snapshots()[0].state, ProcessState::Running);
        std::fs::write(exit_file(&root, "A", 0), code.to_string()).unwrap();
        terminal(&mut d.0).await;
        assert_eq!(
            d.0.snapshots()[0].state,
            ProcessState::Exited { code: Some(code) }
        );
        clock.set(100);
        d.0.poll(&credentials).unwrap();
        assert_eq!(d.0.snapshots()[0].restarts, 0);
        assert!(!d.0.snapshots()[0].desired_running);
    }
}

#[tokio::test]
async fn stopping_a_pending_retry_prevents_spawn() {
    let root = Root::new();
    let clock = ManualClock::default();
    let mut plan = specs(&root, &[("A", 0)]);
    plan[0]
        .gateway
        .environment
        .variables
        .insert("FIXTURE_MODE".into(), "exit".into());
    let mut d = managed(plan, policy(), &clock);
    d.0.start_all(&credentials).unwrap();
    terminal(&mut d.0).await;
    assert_eq!(d.0.snapshots()[0].retry_at_ms, Some(1000));
    d.0.stop(&key("A", 0), StopMode::WaitForExit).unwrap();
    clock.set(100);
    d.0.poll(&credentials).unwrap();
    assert_eq!(d.0.snapshots()[0].restarts, 0);
    assert_eq!(d.0.snapshots()[0].retry_at_ms, None);
}

#[tokio::test]
async fn crash_budget_exponential_backoff_and_circuit_are_bounded() {
    let root = Root::new();
    let clock = ManualClock::default();
    let mut plan = specs(&root, &[("A", 0)]);
    plan[0]
        .gateway
        .environment
        .variables
        .insert("FIXTURE_MODE".into(), "exit".into());
    let p = RestartPolicy {
        max_restarts: 2,
        circuit_breaker_after: 2,
        ..policy()
    };
    let mut d = managed(plan, p, &clock);
    d.0.start_all(&credentials).unwrap();
    terminal(&mut d.0).await;
    assert_eq!(d.0.snapshots()[0].retry_at_ms, Some(1000));
    clock.set(1);
    d.0.poll(&credentials).unwrap();
    terminal(&mut d.0).await;
    assert_eq!(d.0.snapshots()[0].retry_at_ms, Some(11000));
    clock.set(10);
    d.0.poll(&credentials).unwrap();
    assert_eq!(d.0.snapshots()[0].restarts, 1);
    clock.set(11);
    d.0.poll(&credentials).unwrap();
    terminal(&mut d.0).await;
    let final_state = d.0.snapshots().remove(0);
    assert_eq!(final_state.restarts, 2);
    assert!(final_state.restart_exhausted);
    assert_eq!(final_state.retry_at_ms, None);
    clock.set(1000);
    d.0.poll(&credentials).unwrap();
    assert_eq!(d.0.snapshots()[0].restarts, 2);
}

#[tokio::test]
async fn sliding_window_limits_retry_storms() {
    let root = Root::new();
    let clock = ManualClock::default();
    let mut plan = specs(&root, &[("A", 0)]);
    plan[0]
        .gateway
        .environment
        .variables
        .insert("FIXTURE_MODE".into(), "exit".into());
    let p = RestartPolicy {
        max_restarts_in_window: 1,
        ..policy()
    };
    let mut d = managed(plan, p, &clock);
    d.0.start_all(&credentials).unwrap();
    terminal(&mut d.0).await;
    clock.set(1);
    d.0.poll(&credentials).unwrap();
    terminal(&mut d.0).await;
    assert_eq!(d.0.snapshots()[0].retry_at_ms, Some(3000));
    clock.set(3);
    d.0.poll(&credentials).unwrap();
    assert_eq!(d.0.snapshots()[0].retry_at_ms, Some(31000));
    assert_eq!(d.0.snapshots()[0].restarts, 1);
    clock.set(31);
    d.0.poll(&credentials).unwrap();
    terminal(&mut d.0).await;
    assert_eq!(d.0.snapshots()[0].restarts, 2);
}

#[tokio::test]
async fn missing_credentials_fail_without_logging_values_and_do_not_block_other_bots() {
    let root = Root::new();
    let clock = ManualClock::default();
    let mut d = managed(specs(&root, &[("A", 0), ("B", 0)]), policy(), &clock);
    let source = |name: &str| {
        if name == "TOKEN_A" {
            None
        } else {
            credentials(name)
        }
    };
    assert_eq!(d.0.start_all(&source), Err(DaemonError::MissingCredential));
    ready_audit(&ready(&root, "B", 0)).await;
    assert!(d.0.snapshots()[0].identity.is_none());
    assert_eq!(d.0.snapshots()[1].state, ProcessState::Running);
    assert!(!ready(&root, "A", 0).exists());
    let json = serde_json::to_string(&d.0.snapshots()).unwrap();
    assert!(!json.contains("dummy-"));
    assert!(!json.contains("TOKEN_"));
    clock.set(1);
    d.0.poll(&credentials).unwrap();
    ready_audit(&ready(&root, "A", 0)).await;
    for s in d.0.snapshots() {
        d.0.stop(&s.key, StopMode::Force).unwrap();
    }
    terminal(&mut d.0).await;
}

#[tokio::test]
async fn spawn_failure_is_reported_retried_and_does_not_leave_a_reservation() {
    let root = Root::new();
    let clock = ManualClock::default();
    let mut plan = specs(&root, &[("A", 0)]);
    let reservation = plan[0]
        .gateway
        .management_path
        .with_extension("botd-reservation");
    plan[0].gateway.program = root.0.join("missing-program").into_os_string();
    let mut d = managed(
        plan,
        RestartPolicy {
            max_restarts: 1,
            ..policy()
        },
        &clock,
    );
    assert_eq!(d.0.start_all(&credentials), Err(DaemonError::SpawnFailed));
    assert!(!reservation.exists());
    clock.set(1);
    assert_eq!(d.0.poll(&credentials), Err(DaemonError::SpawnFailed));
    assert!(d.0.snapshots()[0].restart_exhausted);
    assert!(!reservation.exists());
}

#[tokio::test]
async fn daemon_drop_keeps_child_alive_and_blocks_replacement_until_reconciliation() {
    let root = Root::new();
    let clock = ManualClock::default();
    let plan = specs(&root, &[("A", 0)]);
    let mut d = Daemon::with_clock(plan.clone(), policy(), clock.clone()).unwrap();
    d.start_all(&credentials).unwrap();
    ready_audit(&ready(&root, "A", 0)).await;
    drop(d);
    let mut replacement = managed(plan, policy(), &clock);
    assert_eq!(
        replacement.0.start_all(&credentials),
        Err(DaemonError::ReservationExists)
    );
    assert!(replacement.0.snapshots()[0].identity.is_none());
    // A living child observes a fresh instruction AFTER its owner was dropped.
    // Its successful exit is proof that Drop did not kill it. OS-neutral wait
    // evidence is an audit file written by the fixture before exiting below.
    std::fs::write(exit_file(&root, "A", 0), "0").unwrap();
    // Probe the OS-owned Child through a small external watcher would be unsafe
    // PID adoption. Instead the fixture's stop acknowledgement provides evidence.
    let ack = exit_file(&root, "A", 0).with_extension("ack");
    eventually(|| ack.exists()).await;
}

/// Linux only: the decision reads /proc. Other OSes keep refusing (see the test
/// above), which is the safe default.
#[cfg(target_os = "linux")]
#[tokio::test]
async fn leftover_reservation_without_a_live_gateway_is_removed_and_launch_proceeds() {
    let root = Root::new();
    let clock = ManualClock::default();
    let plan = specs(&root, &[("A", 0)]);
    let reservation = plan[0]
        .gateway
        .management_path
        .with_extension("botd-reservation");
    std::fs::create_dir_all(reservation.parent().unwrap()).unwrap();
    // As left behind by a killed botd: a reservation but no process at all.
    std::fs::write(&reservation, b"stale").unwrap();
    let mut d = managed(plan, policy(), &clock);
    d.0.start_all(&credentials).unwrap();
    ready_audit(&ready(&root, "A", 0)).await;
    assert_eq!(d.0.snapshots()[0].state, ProcessState::Running);
    // The new launch wrote its own reservation (not the stale content).
    assert_ne!(std::fs::read(&reservation).unwrap(), b"stale");
    for s in d.0.snapshots() {
        d.0.stop(&s.key, StopMode::Force).unwrap();
    }
    terminal(&mut d.0).await;
}

#[tokio::test]
async fn duplicate_start_unknown_shard_and_program_changes_are_rejected() {
    let root = Root::new();
    let clock = ManualClock::default();
    let mut d = managed(specs(&root, &[("A", 0)]), policy(), &clock);
    assert_eq!(
        d.0.set_gateway_program(Path::new("relative")),
        Err(DaemonError::InvalidProgram)
    );
    assert_eq!(
        d.0.start(&key("missing", 0), &credentials),
        Err(DaemonError::UnknownShard)
    );
    assert_eq!(
        d.0.stop(&key("missing", 0), StopMode::Force),
        Err(DaemonError::UnknownShard)
    );
    d.0.start_all(&credentials).unwrap();
    assert_eq!(
        d.0.start(&key("A", 0), &credentials),
        Err(DaemonError::InvalidState)
    );
    assert_eq!(
        d.0.set_gateway_program(&std::env::current_exe().unwrap()),
        Err(DaemonError::InvalidState)
    );
    d.0.stop(&key("A", 0), StopMode::Force).unwrap();
    terminal(&mut d.0).await;
}

#[tokio::test]
async fn clock_regression_disables_restarts_but_preserves_and_reaps_children() {
    let root = Root::new();
    let clock = ManualClock::default();
    clock.set(5);
    let mut d = managed(specs(&root, &[("A", 0)]), policy(), &clock);
    d.0.start_all(&credentials).unwrap();
    ready_audit(&ready(&root, "A", 0)).await;
    clock.set(4);
    assert_eq!(d.0.poll(&credentials), Err(DaemonError::ClockRegressed));
    assert_eq!(d.0.snapshots()[0].state, ProcessState::Running);
    assert!(!d.0.snapshots()[0].desired_running);
    clock.set(10);
    assert_eq!(d.0.poll(&credentials), Err(DaemonError::ClockRegressed));
    assert_eq!(
        d.0.start(&key("A", 0), &credentials),
        Err(DaemonError::ClockRegressed)
    );
    std::fs::write(exit_file(&root, "A", 0), "17").unwrap();
    terminal(&mut d.0).await;
    assert_eq!(d.0.snapshots()[0].restarts, 0);
}

#[test]
fn policies_and_untrusted_launch_specs_fail_closed() {
    let root = Root::new();
    for p in [
        RestartPolicy {
            max_restarts_in_window: 0,
            ..policy()
        },
        RestartPolicy {
            initial_backoff: Duration::ZERO,
            ..policy()
        },
        RestartPolicy {
            window: Duration::ZERO,
            ..policy()
        },
        RestartPolicy {
            max_backoff: Duration::ZERO,
            ..policy()
        },
        RestartPolicy {
            circuit_breaker_after: 0,
            ..policy()
        },
        RestartPolicy {
            circuit_open_for: Duration::ZERO,
            ..policy()
        },
    ] {
        assert!(matches!(
            Daemon::new(specs(&root, &[("A", 0)]), p),
            Err(DaemonError::InvalidPolicy)
        ));
    }
    for variant in 0..9 {
        let mut plan = specs(&root, &[("A", 0), ("B", 0)]);
        match variant {
            0 => plan[0].gateway.environment.clear_inherited = false,
            1 => plan[0]
                .core
                .environment
                .credential_refs
                .push("TOKEN_A".into()),
            2 => plan[0]
                .gateway
                .environment
                .variables
                .insert("token_b".into(), "forbidden".into())
                .map(drop)
                .unwrap_or(()),
            3 => plan[1].gateway.environment.credential_refs = vec!["token_a".into()],
            4 => plan[0]
                .gateway
                .environment
                .credential_refs
                .push("TOKEN_B".into()),
            5 => plan[1].ipc_path = plan[0].ipc_path.clone(),
            6 => plan[0].gateway.role = shard_runtime::Role::Core,
            7 => plan[0]
                .gateway
                .environment
                .variables
                .insert("bad=name".into(), "x".into())
                .map(drop)
                .unwrap_or(()),
            _ => plan[0].core.environment.clear_inherited = false,
        }
        assert!(
            matches!(Daemon::new(plan, policy()), Err(DaemonError::InvalidSpec)),
            "variant {variant}"
        );
    }
}

fn config() -> Config {
    toml::from_str(
        r#"
[cluster]
id = "local"
mode = "standalone"
[node]
id = "one"
[[bots]]
name = "first"
bot_id = "A"
application_id = "1"
token_env = "TOKEN_A"
total_shards = 2
[[bots.shard_placement]]
shard_id = 0
primary = "one"
[[bots.shard_placement]]
shard_id = 1
primary = "two"
"#,
    )
    .unwrap()
}

#[test]
fn toml_only_local_primary_is_planned_and_unsupported_modes_fail_closed() {
    let root = Root::new();
    let mut c = config();
    let daemon = Daemon::from_config(&c, &HashMap::new(), &root.0, policy()).unwrap();
    assert_eq!(daemon.snapshots().len(), 1);
    assert_eq!(daemon.snapshots()[0].key, key("A", 0));
    c.cluster.mode = src::placement::ClusterMode::Quorum;
    assert!(matches!(
        Daemon::from_config(&c, &HashMap::new(), &root.0, policy()),
        Err(DaemonError::UnsupportedDistributedMode)
    ));
    c.cluster.mode = src::placement::ClusterMode::Standalone;
    c.node.remote_bind = Some("127.0.0.1:1234".parse().unwrap());
    assert!(matches!(
        Daemon::from_config(&c, &HashMap::new(), &root.0, policy()),
        Err(DaemonError::RemoteManagementDenied)
    ));
    c.node.remote_bind = None;
    c.bots[0].total_shards = src::placement::ShardCount::Auto(src::placement::AutoShardCount::Auto);
    assert!(matches!(
        Daemon::from_config(&c, &HashMap::new(), &root.0, policy()),
        Err(DaemonError::InvalidConfig)
    ));
    assert!(Daemon::from_config(&c, &HashMap::from([("A".into(), 2)]), &root.0, policy()).is_ok());
}

#[test]
fn configuration_rejects_a_credential_alias_even_for_a_nonlocal_bot() {
    let root = Root::new();
    let mut c = config();
    let mut other = c.bots[0].clone();
    other.name = "second".into();
    other.bot_id = "B".into();
    other.application_id = "2".into();
    other.token_env = "token_a".into();
    for p in &mut other.shard_placement {
        p.primary = "elsewhere".into();
    }
    c.bots.push(other);
    assert!(matches!(
        Daemon::from_config(&c, &HashMap::new(), &root.0, policy()),
        Err(DaemonError::InvalidConfig)
    ));
    c.bots[1].token_env = "TOKEN_B".into();
    assert!(Daemon::from_config(&c, &HashMap::new(), &root.0, policy()).is_ok());
}

#[test]
fn launch_without_an_async_runtime_fails_instead_of_panicking() {
    let root = Root::new();
    let clock = ManualClock::default();
    let mut daemon = Daemon::with_clock(specs(&root, &[("A", 0)]), policy(), clock).unwrap();
    let no_reads = |_: &str| -> Option<OsString> { panic!("credentials read without runtime") };
    assert_eq!(daemon.start_all(&no_reads), Err(DaemonError::NoRuntime));
    assert!(daemon.snapshots()[0].restart_exhausted);
    assert!(daemon.snapshots()[0].identity.is_none());
}

#[tokio::test]
async fn deadline_overflow_disables_retry_and_reports_the_fault() {
    let root = Root::new();
    let clock = ManualClock::default();
    *clock.0.lock().unwrap() = Duration::MAX;
    let mut d = managed(specs(&root, &[("A", 0)]), policy(), &clock);
    let missing = |_: &str| None;
    assert_eq!(d.0.start_all(&missing), Err(DaemonError::TimeOverflow));
    let snapshot = d.0.snapshots().remove(0);
    assert_eq!(
        snapshot.state,
        ProcessState::Failed {
            error: DaemonError::TimeOverflow,
            code: None
        }
    );
    assert!(snapshot.restart_exhausted);
    assert!(!snapshot.desired_running);
    assert_eq!(snapshot.retry_at_ms, None);
    assert!(snapshot.identity.is_none());
}

#[tokio::test]
async fn stopped_children_release_reservations_and_zero_budget_disables_auto_restart() {
    let root = Root::new();
    let clock = ManualClock::default();
    let plan = specs(&root, &[("A", 0)]);
    let mut d = managed(
        plan.clone(),
        RestartPolicy {
            max_restarts: 0,
            ..policy()
        },
        &clock,
    );
    d.0.start_all(&credentials).unwrap();
    ready_audit(&ready(&root, "A", 0)).await;
    std::fs::write(exit_file(&root, "A", 0), "17").unwrap();
    terminal(&mut d.0).await;
    assert!(d.0.snapshots()[0].restart_exhausted);
    assert_eq!(d.0.snapshots()[0].retry_at_ms, None);
    let old = d.0.snapshots()[0].identity.clone().unwrap();
    std::fs::remove_file(exit_file(&root, "A", 0)).unwrap();
    std::fs::remove_file(ready(&root, "A", 0)).unwrap();
    let mut next = managed(plan, policy(), &clock);
    next.0.start_all(&credentials).unwrap();
    ready_audit(&ready(&root, "A", 0)).await;
    assert_ne!(
        next.0.snapshots()[0].identity.as_ref().unwrap().generation,
        old.generation
    );
    next.0.stop(&key("A", 0), StopMode::Force).unwrap();
    terminal(&mut next.0).await;
}

#[test]
fn orphan_evidence_requires_identity_native_start_time_and_authenticated_ipc() {
    let identity = ProcessIdentity {
        key: key("A", 0),
        pid: 123,
        generation: LaunchGeneration {
            daemon_instance: [1; 16],
            sequence: 2,
        },
        launch_observed_unix_ms: 50,
    };
    let record = OrphanRecord {
        identity: identity.clone(),
        os_start_time: Some(ProcessStartTime {
            boot_id: "boot".into(),
            ticks: 10,
        }),
    };
    let observed = OrphanObservation {
        identity,
        os_start_time: record.os_start_time.clone(),
        ipc_authenticated: true,
    };
    assert_eq!(
        reconcile_orphan(&record, &observed),
        Reconciliation::Matches
    );
    for variant in 0..6 {
        let mut altered = observed.clone();
        match variant {
            0 => altered.identity.key.bot_id = "B".into(),
            1 => altered.identity.key.shard_id = 1,
            2 => altered.identity.pid += 1,
            3 => altered.identity.generation.sequence += 1,
            4 => altered.identity.generation.daemon_instance = [2; 16],
            _ => altered.identity.launch_observed_unix_ms += 1,
        }
        assert_eq!(
            reconcile_orphan(&record, &altered),
            Reconciliation::IdentityMismatch
        );
    }
    let mut altered = observed.clone();
    altered.os_start_time = None;
    assert_eq!(
        reconcile_orphan(&record, &altered),
        Reconciliation::MissingProcessStartTime
    );
    let mut altered = observed.clone();
    altered.os_start_time.as_mut().unwrap().ticks += 1;
    assert_eq!(
        reconcile_orphan(&record, &altered),
        Reconciliation::ProcessStartTimeMismatch
    );
    let mut altered = observed.clone();
    altered.os_start_time.as_mut().unwrap().boot_id = "next-boot".into();
    assert_eq!(
        reconcile_orphan(&record, &altered),
        Reconciliation::ProcessStartTimeMismatch
    );
    let mut altered = observed;
    altered.ipc_authenticated = false;
    assert_eq!(
        reconcile_orphan(&record, &altered),
        Reconciliation::Unauthenticated
    );
}

#[tokio::test]
async fn botd_binary_bootstraps_authenticated_read_only_local_status() {
    let root = Root::new();
    let config_path = root.0.join("placement.toml");
    // No children required for this transport integration check.
    std::fs::write(
        &config_path,
        "[cluster]\nid='local'\nmode='standalone'\n[node]\nid='one'\n",
    )
    .unwrap();
    let mut child = tokio::process::Command::new(env!("CARGO_BIN_EXE_botd"))
        .arg("--file")
        .arg(config_path)
        .arg("--runtime-root")
        .arg(&root.0)
        .arg("--gateway-program")
        .arg(std::env::current_exe().unwrap())
        .arg("--bootstrap-stdin")
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let mut stdin = child.stdin.take().unwrap();
    let secret = [3u8; 32];
    let mut bootstrap = serde_json::json!({"secret": secret.to_vec()}).to_string();
    bootstrap.push('\n');
    stdin.write_all(bootstrap.as_bytes()).await.unwrap();
    let mut line = String::new();
    tokio::time::timeout(
        Duration::from_secs(10),
        BufReader::new(child.stdout.take().unwrap()).read_line(&mut line),
    )
    .await
    .unwrap()
    .unwrap();
    let announcement: serde_json::Value = serde_json::from_str(&line).unwrap();
    assert_eq!(announcement["local_only"], true);
    assert_eq!(announcement["startup_ok"], true);
    assert!(announcement.get("secret").is_none());
    let port = announcement["port"].as_u64().unwrap() as u16;
    for (supplied, command, permitted) in [
        (secret, "status", true),
        ([4; 32], "status", false),
        (secret, "stop", false),
    ] {
        let mut stream = tokio::net::TcpStream::connect(("127.0.0.1", port))
            .await
            .unwrap();
        let mut request =
            serde_json::json!({"secret": supplied.to_vec(), "command": command}).to_string();
        request.push('\n');
        stream.write_all(request.as_bytes()).await.unwrap();
        stream.shutdown().await.unwrap();
        let mut response = Vec::new();
        tokio::time::timeout(Duration::from_secs(5), stream.read_to_end(&mut response))
            .await
            .unwrap()
            .unwrap();
        if permitted {
            let response: serde_json::Value = serde_json::from_slice(&response).unwrap();
            assert_eq!(response["status"]["processes"], serde_json::json!([]));
            assert_eq!(response["status"]["monitor_ok"], true);
        } else {
            assert!(response.is_empty());
        }
    }
    child.kill().await.unwrap();
}

/// A tiny executable which consumes the actual shard_runtime argument contract.
/// Source and build output stay in the ignored test target directory.
fn compile_gateway_fixture(root: &Root) -> PathBuf {
    let source = root.0.join("gateway-fixture.rs");
    let program = root
        .0
        .join(format!("gateway-fixture{}", std::env::consts::EXE_SUFFIX));
    std::fs::write(&source, r#"
use std::{collections::HashMap, path::PathBuf, time::{Duration, Instant}};
fn main() {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    if args.len() != 14 { std::process::exit(80); }
    let args = args.chunks_exact(2).map(|v| (v[0].clone(), v[1].clone())).collect::<HashMap<_, _>>();
    let bot = &args["--bot-id"];
    let token_name = &args["--token-env"];
    let token_ok = token_name == &format!("TOKEN_{bot}")
        && std::env::var(token_name).ok().as_deref() == Some(format!("dummy-{bot}").as_str());
    let foreign_absent = ["TOKEN_A", "TOKEN_B"].iter().filter(|n| **n != token_name)
        .all(|name| std::env::var_os(name).is_none());
    let isolated = token_ok && foreign_absent && std::env::var_os("DISCORD_TOKEN").is_none()
        && std::env::var_os("PATH").is_none();
    let state = PathBuf::from(&args["--state-path"]);
    let management = PathBuf::from(&args["--management-path"]);
    let args_ok = args["--shard-id"] == "0" && args["--total-shards"] == "1"
        && state.parent() == management.parent()
        && state.parent() == PathBuf::from(&args["--ipc-path"]).parent();
    if !isolated || !args_ok { std::process::exit(81); }
    std::fs::write(state, b"isolated-and-arguments-verified").unwrap();
    let stop = management.with_extension("exit");
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        if let Ok(code) = std::fs::read_to_string(&stop) {
            std::fs::write(management.with_extension("ack"), b"acknowledged").unwrap();
            std::process::exit(code.parse().unwrap());
        }
        if Instant::now() >= deadline { std::process::exit(82); }
        std::thread::sleep(Duration::from_millis(5));
    }
}
"#).unwrap();
    let output = std::process::Command::new("rustc")
        .arg("--edition=2024")
        .arg(source)
        .arg("-o")
        .arg(&program)
        .output()
        .unwrap();
    assert!(output.status.success(), "fixture compilation failed");
    program
}

async fn live_status(port: u16, secret: [u8; 32]) -> serde_json::Value {
    let mut stream = tokio::net::TcpStream::connect(("127.0.0.1", port))
        .await
        .unwrap();
    let mut request =
        serde_json::json!({"secret": secret.to_vec(), "command": "status"}).to_string();
    request.push('\n');
    stream.write_all(request.as_bytes()).await.unwrap();
    let mut reply = Vec::new();
    tokio::time::timeout(Duration::from_secs(5), stream.read_to_end(&mut reply))
        .await
        .unwrap()
        .unwrap();
    serde_json::from_slice(&reply).unwrap()
}

#[tokio::test]
async fn botd_toml_launches_two_bots_monitors_crashes_and_children_survive_daemon_exit() {
    let root = Root::new();
    let program = compile_gateway_fixture(&root);
    let config_path = root.0.join("placement.toml");
    let mut text = String::from("[cluster]\nid='local'\nmode='standalone'\n[node]\nid='one'\n");
    for bot in ["A", "B"] {
        text.push_str(&format!("[[bots]]\nname='bot{bot}'\nbot_id='{bot}'\napplication_id='{bot}'\ntoken_env='TOKEN_{bot}'\ntotal_shards=1\n[[bots.shard_placement]]\nshard_id=0\nprimary='one'\n"));
    }
    std::fs::write(&config_path, text).unwrap();
    let c = Config::load(&config_path).unwrap();
    let plan =
        shard_runtime::plan(&c.local_primary_intents(&HashMap::new()).unwrap(), &root.0).unwrap();
    let mut child = tokio::process::Command::new(env!("CARGO_BIN_EXE_botd"))
        .arg("--file")
        .arg(&config_path)
        .arg("--runtime-root")
        .arg(&root.0)
        .arg("--gateway-program")
        .arg(&program)
        .arg("--bootstrap-stdin")
        .env("TOKEN_A", "dummy-A")
        .env("TOKEN_B", "dummy-B")
        .env("DISCORD_TOKEN", "must-be-removed")
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let secret = [9u8; 32];
    let mut bootstrap = serde_json::json!({"secret": secret.to_vec()}).to_string();
    bootstrap.push('\n');
    child
        .stdin
        .take()
        .unwrap()
        .write_all(bootstrap.as_bytes())
        .await
        .unwrap();
    let mut line = String::new();
    tokio::time::timeout(
        Duration::from_secs(10),
        BufReader::new(child.stdout.take().unwrap()).read_line(&mut line),
    )
    .await
    .unwrap()
    .unwrap();
    let announcement: serde_json::Value = serde_json::from_str(&line).unwrap();
    assert_eq!(announcement["startup_ok"], true);
    let port = announcement["port"].as_u64().unwrap() as u16;
    for s in &plan {
        eventually(|| s.gateway.state_path.exists()).await;
    }
    let first = live_status(port, secret).await;
    assert_eq!(first["status"]["processes"].as_array().unwrap().len(), 2);
    assert!(
        first["status"]["processes"]
            .as_array()
            .unwrap()
            .iter()
            .all(|s| s["state"]["state"] == "Running")
    );
    // A slow unauthenticated client cannot suspend process monitoring.
    let idle = tokio::net::TcpStream::connect(("127.0.0.1", port))
        .await
        .unwrap();
    std::fs::write(plan[0].gateway.management_path.with_extension("exit"), "17").unwrap();
    eventually(|| {
        plan[0]
            .gateway
            .management_path
            .with_extension("ack")
            .exists()
    })
    .await;
    // Retain the exit marker: retries are allowed, but they must not become an
    // unbounded immediate restart loop while this client holds the server.
    tokio::time::sleep(Duration::from_millis(250)).await;
    drop(idle);
    let failed = live_status(port, secret).await;
    assert_eq!(failed["status"]["processes"][0]["state"]["state"], "Failed");
    assert_eq!(failed["status"]["processes"][0]["state"]["code"], 17);
    assert_eq!(
        failed["status"]["processes"][1]["state"]["state"],
        "Running"
    );
    let response_text = failed.to_string();
    assert!(!response_text.contains("dummy-"));
    assert!(!response_text.contains("TOKEN_"));
    // Kill only the daemon (test cleanup authorization), then have Bot B prove
    // it is still alive by acknowledging a new independent exit instruction.
    child.kill().await.unwrap();
    std::fs::write(plan[1].gateway.management_path.with_extension("exit"), "0").unwrap();
    eventually(|| {
        plan[1]
            .gateway
            .management_path
            .with_extension("ack")
            .exists()
    })
    .await;
}
