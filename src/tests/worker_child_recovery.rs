//! Real Core children behind a Gateway-local Manager; no Discord credentials.
use src::{
    child_recovery::{
        ChildRole, DegradedReason, RecoveryConfig, RecoveryEventKind, RecoveryState, StartMode,
    },
    supervisor::RestartPolicy,
    worker::{DeploymentIdentity, Manager, WorkerIdentity},
};
use std::{path::PathBuf, process::Stdio, sync::Arc, time::Duration};
use tokio::time::{sleep, timeout};

const LIMIT: Duration = Duration::from_secs(15);

fn binary() -> PathBuf {
    env!("CARGO_BIN_EXE_src").into()
}

fn deployment(max_restarts: u32) -> DeploymentIdentity {
    DeploymentIdentity::parse(|name| {
        Ok(match name {
            "BOT_ID" => Some("901".into()),
            "SHARD_ID" => Some("1".into()),
            "TOTAL_SHARDS" => Some("2".into()),
            "GENERATION_ID" => Some("700".into()),
            _ => None,
        })
    })
    .unwrap()
    .with_recovery_config(RecoveryConfig {
        restart: RestartPolicy {
            max_restarts,
            max_restarts_in_window: 8,
            window: Duration::from_secs(60),
            initial_backoff: Duration::from_millis(20),
            max_backoff: Duration::from_millis(40),
            circuit_breaker_after: 8,
            circuit_open_for: Duration::from_millis(100),
        },
        stable_for: Duration::from_secs(30),
        max_startup_failures: 8,
    })
}

async fn manager(max_restarts: u32) -> Arc<Manager> {
    Manager::start_pool_isolated(binary(), 1, deployment(max_restarts))
        .await
        .unwrap()
}

async fn terminate_owned_core(pid: u32) {
    tokio::task::spawn_blocking(move || {
        #[cfg(windows)]
        {
            use std::os::windows::process::CommandExt;
            let output = std::process::Command::new("powershell.exe")
                .args([
                    "-NoProfile", "-NonInteractive", "-Command",
                    "$target = Get-Process -Id ([int]$env:RECOVERY_TEST_PID) -ErrorAction Stop; if (-not [StringComparer]::OrdinalIgnoreCase.Equals($target.MainModule.FileName, $env:RECOVERY_TEST_EXE)) { throw 'unexpected fixture executable' }; $target.Kill(); if (-not $target.WaitForExit(5000)) { throw 'fixture did not exit' }",
                ])
                .env("RECOVERY_TEST_PID", pid.to_string())
                .env("RECOVERY_TEST_EXE", env!("CARGO_BIN_EXE_src"))
                .stdin(Stdio::null())
                .creation_flags(0x0800_0000)
                .output()
                .expect("launch owned Core termination helper");
            assert!(output.status.success(), "owned Core termination failed");
        }
        #[cfg(unix)]
        {
            #[cfg(target_os = "linux")]
            assert_eq!(
                std::fs::read_link(format!("/proc/{pid}/exe")).unwrap(),
                std::fs::canonicalize(binary()).unwrap(),
            );
            assert!(std::process::Command::new("kill")
                .args(["-KILL", &pid.to_string()])
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status()
                .unwrap()
                .success(), "owned Core termination failed");
        }
    })
    .await
    .unwrap();
}

async fn observe(manager: &Manager, condition: impl Fn(&WorkerIdentity) -> bool) -> WorkerIdentity {
    timeout(LIMIT, async {
        loop {
            let worker = manager.worker_identities().await.remove(0);
            if condition(&worker) {
                return worker;
            }
            sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("Core recovery observation timed out")
}

#[tokio::test]
async fn idle_core_exit_recovers_without_a_request_or_gateway_epoch_change() {
    timeout(LIMIT, async {
        let manager = manager(2).await;
        let before = manager.worker_identities().await.remove(0);
        manager.take_recovery_events();
        terminate_owned_core(before.pid).await;
        // Status reads no Core IPC and does not drive recovery.
        let after = observe(&manager, |worker| {
            worker.pid != before.pid && worker.recovery.as_ref().is_some_and(|s| s.ready)
        })
        .await;
        let old_identity = before.identity.unwrap();
        let new_identity = after.identity.unwrap();
        assert_eq!(new_identity.bot_id, old_identity.bot_id);
        assert_eq!(new_identity.shard, old_identity.shard);
        assert_eq!(new_identity.ownership_epoch, old_identity.ownership_epoch);
        assert!(new_identity.generation_id.0 > old_identity.generation_id.0);
        assert!(after.credential_free);
        assert_eq!(after.recovery.unwrap().restarts, 1);
        let events = manager.take_recovery_events();
        assert!(
            events
                .iter()
                .all(|(index, e)| *index == 0 && e.key.role == ChildRole::Core)
        );
        assert!(
            events
                .iter()
                .any(|(_, e)| matches!(e.kind, RecoveryEventKind::Exited { .. }))
        );
        assert!(events.iter().any(|(_, e)| e.kind
            == RecoveryEventKind::Started {
                mode: StartMode::Core
            }));
        assert_eq!(
            manager.dispatch("!ping".into()).await.unwrap().as_deref(),
            Some("Pong! 🦀")
        );
        manager.shutdown().await.unwrap();
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn budget_exhaustion_is_latched_in_status_events_and_manual_update() {
    timeout(LIMIT, async {
        let manager = manager(1).await;
        let first = manager.worker_identities().await.remove(0);
        terminate_owned_core(first.pid).await;
        let second = observe(&manager, |worker| worker.pid != first.pid).await;
        manager.take_recovery_events();
        terminate_owned_core(second.pid).await;
        let degraded = observe(&manager, |worker| {
            worker
                .recovery
                .as_ref()
                .is_some_and(|s| s.state == RecoveryState::Degraded(DegradedReason::RestartBudget))
        })
        .await;
        let status = degraded.recovery.unwrap();
        assert_eq!(status.restarts, 1);
        assert_eq!(status.pid, None);
        assert!(!status.ready);
        assert_eq!(status.degraded_reason, Some(DegradedReason::RestartBudget));
        let events = manager.take_recovery_events();
        assert!(events.iter().any(|(_, e)| e.kind
            == RecoveryEventKind::Degraded {
                reason: DegradedReason::RestartBudget,
            }));
        assert!(manager.replace(binary(), true).await.is_err());
        sleep(Duration::from_millis(200)).await;
        let current = manager
            .worker_identities()
            .await
            .remove(0)
            .recovery
            .unwrap();
        assert_eq!(current.state, status.state);
        assert_eq!(current.generation, status.generation);
        assert!(
            !manager
                .take_recovery_events()
                .iter()
                .any(|(_, e)| matches!(e.kind, RecoveryEventKind::Started { .. }))
        );
        // A failed child is reaped but is not a successful graceful shutdown.
        assert!(manager.shutdown().await.is_err());
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn explicit_core_update_preserves_the_automatic_recovery_budget() {
    timeout(LIMIT, async {
        let manager = manager(1).await;
        let first = manager.worker_identities().await.remove(0);
        terminate_owned_core(first.pid).await;
        let recovered = observe(&manager, |worker| worker.pid != first.pid).await;
        manager.replace(binary(), true).await.unwrap();
        let updated = manager.worker_identities().await.remove(0);
        assert_ne!(updated.pid, recovered.pid);
        assert_eq!(updated.recovery.as_ref().unwrap().restarts, 1);
        terminate_owned_core(updated.pid).await;
        let degraded = observe(&manager, |worker| {
            worker
                .recovery
                .as_ref()
                .is_some_and(|s| s.state == RecoveryState::Degraded(DegradedReason::RestartBudget))
        })
        .await;
        assert_eq!(degraded.pid, updated.pid);
        assert_eq!(degraded.recovery.unwrap().restarts, 1);
        assert!(manager.shutdown().await.is_err());
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn graceful_stop_cancels_automatic_core_recovery() {
    timeout(LIMIT, async {
        let manager = manager(0).await;
        let before = manager.worker_identities().await.remove(0);
        assert_eq!(manager.shutdown().await.unwrap(), vec![before.pid]);
        sleep(Duration::from_millis(200)).await;
        let after = manager.worker_identities().await.remove(0);
        let status = after.recovery.unwrap();
        assert_eq!(after.pid, before.pid);
        assert_eq!(status.state, RecoveryState::Stopped);
        assert_eq!(status.pid, None);
        assert_eq!(status.restarts, 0);
        assert!(
            manager
                .take_recovery_events()
                .iter()
                .any(|(_, e)| e.kind == RecoveryEventKind::Stopped)
        );
        assert_eq!(manager.shutdown().await.unwrap(), vec![before.pid]);
    })
    .await
    .unwrap();
}
