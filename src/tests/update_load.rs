//! Bounded open-loop load against real botd/Gateway/Worker and mock HTTP processes.
//! All scenarios run before asserting acceptance, preserving failure evidence.
use std::{
    path::{Path, PathBuf},
    process::Command,
    sync::OnceLock,
    time::Duration,
};

fn evidence_directory() -> PathBuf {
    let artifact = std::env::current_exe().unwrap();
    let root = artifact
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .join(format!("update-load-{}", getrandom::u64().unwrap()));
    std::fs::create_dir(&root).unwrap();
    root
}

fn relay_binary() -> &'static Path {
    static RELAY: OnceLock<PathBuf> = OnceLock::new();
    RELAY.get_or_init(|| {
        let relay =
            evidence_directory().join(format!("worker-relay{}", std::env::consts::EXE_SUFFIX));
        let compiled = Command::new("rustc")
            .args(["--edition=2024", "-Dwarnings"])
            .arg(Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/update_load_worker.rs"))
            .arg("-o")
            .arg(&relay)
            .status()
            .expect("compile std-only fault fixture");
        assert!(compiled.success(), "fault fixture compilation failed");
        relay
    })
}

fn sidecar(path: &Path, suffix: &str) -> PathBuf {
    let mut value = path.as_os_str().to_owned();
    value.push(suffix);
    value.into()
}

#[tokio::test]
async fn unused_delivery_survives_restore_wait_and_routing_commit() {
    use src::worker::{ApiOutcome, DeploymentIdentity, Manager};
    use tokio::time::timeout;

    // Compile before process deadlines. No fixed sleep determines switch timing.
    let relay = tokio::task::spawn_blocking(relay_binary).await.unwrap();
    let root = evidence_directory();
    let candidate = root.join(format!("held-worker{}", std::env::consts::EXE_SUFFIX));
    std::fs::copy(relay, &candidate).unwrap();
    std::fs::write(sidecar(&candidate, ".real"), env!("CARGO_BIN_EXE_src")).unwrap();
    std::fs::write(sidecar(&candidate, ".mode"), "hold-restore").unwrap();

    timeout(Duration::from_secs(30), async {
        let manager = Manager::start_pool_isolated(
            env!("CARGO_BIN_EXE_src").into(),
            1,
            DeploymentIdentity::parse(|_| Ok(None)).unwrap(),
        )
        .await
        .unwrap();
        let old_pid = manager.worker_identities().await[0].pid;
        let mut updates = tokio::task::JoinSet::new();
        updates.spawn({
            let manager = manager.clone();
            let candidate = candidate.clone();
            async move { manager.replace(candidate, true).await }
        });
        timeout(Duration::from_secs(10), async {
            while !std::fs::read_dir(&root).unwrap().any(|entry| {
                entry
                    .unwrap()
                    .path()
                    .extension()
                    .is_some_and(|ext| ext == "restored")
            }) {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("candidate must reach the restore gate");

        // A candidate's slow Restore must not hold the routing lock or prevent
        // the Gateway reserving an Interaction before its 3-second ACK deadline.
        let reservation = timeout(Duration::from_secs(3), manager.begin_response_delivery(0)).await;
        std::fs::write(sidecar(&candidate, ".release"), []).unwrap();
        let mut response = reservation
            .expect("candidate Restore blocked admission")
            .unwrap();
        timeout(Duration::from_secs(10), async {
            while manager.worker_identities().await[0].pid == old_pid {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("candidate routing commit");
        let new_pid = manager.worker_identities().await[0].pid;
        assert_eq!(
            manager
                .dispatch_key_with_response(&mut response, 0, "!ping".into())
                .await
                .unwrap()
                .as_deref(),
            Some("Pong! 🦀")
        );
        // Only the unused reservation moves. Result delivery stays bound to the
        // actual dispatch generation, and the old reservation does not leak.
        let receipt = manager
            .finish_response(response, ApiOutcome::Confirmed { status: 200 })
            .await
            .unwrap();
        assert_eq!(receipt.pid, new_pid);
        assert_ne!(receipt.pid, old_pid);
        updates.join_next().await.unwrap().unwrap().unwrap();
        assert!(manager.unknown_responses().1.is_empty());
        manager.shutdown().await.unwrap();
    })
    .await
    .expect("delivery regression deadline");
}

#[tokio::test]
async fn reserved_delivery_survives_shutdown_admission_barrier() {
    use src::worker::{ApiOutcome, DeploymentIdentity, Manager};
    use tokio::time::timeout;

    let manager = Manager::start_pool_isolated(
        env!("CARGO_BIN_EXE_src").into(),
        1,
        DeploymentIdentity::parse(|_| Ok(None)).unwrap(),
    )
    .await
    .unwrap();
    let mut response = manager.begin_response_delivery(0).await.unwrap();
    let shutdown = manager.shutdown();
    tokio::pin!(shutdown);
    // Poll shutdown until it waits on our outstanding reservation. The new
    // admission failure below proves the barrier has actually closed.
    assert!(
        timeout(Duration::from_millis(20), &mut shutdown)
            .await
            .is_err()
    );
    assert!(manager.begin_response_delivery(0).await.is_err());
    assert_eq!(
        timeout(
            Duration::from_secs(3),
            manager.dispatch_key_with_response(&mut response, 0, "!ping".into()),
        )
        .await
        .expect("reserved dispatch must not wait behind its own drain writer")
        .unwrap()
        .as_deref(),
        Some("Pong! 🦀")
    );
    manager
        .finish_response(response, ApiOutcome::Confirmed { status: 200 })
        .await
        .unwrap();
    timeout(Duration::from_secs(5), shutdown)
        .await
        .unwrap()
        .unwrap();
}

#[test]
fn continuous_events_during_updates_and_core_crashes() {
    run_load_cases(&[
        "core_series",
        "gateway_switch",
        "crash_old_core",
        "crash_new_core",
    ]);
}

/// After handler execution and before result delivery, the outcome must be
/// explicitly reported as unknown, never silently omitted or automatically retried.
#[test]
#[ignore = "known limitation: outcome-unknown after handler execution; see architecture 結果不明"]
fn continuous_events_after_handler_outcome_unknown() {
    run_load_cases(&["crash_new_core_after_handler"]);
}

fn run_load_cases(cases: &[&str]) {
    let manifest = Path::new(env!("CARGO_MANIFEST_DIR"));
    let root = evidence_directory();
    let relay = relay_binary();
    let python = if cfg!(windows) { "python" } else { "python3" };
    let mut failed = Vec::new();
    for &case in cases {
        let status = Command::new(python)
            .arg(manifest.join("tests/fixtures/update_load.py"))
            .args(["--case", case, "--botd", env!("CARGO_BIN_EXE_botd")])
            .args(["--gateway", env!("CARGO_BIN_EXE_src"), "--relay"])
            .arg(relay)
            .arg("--output")
            .arg(root.join(case))
            .env("PYTHONDONTWRITEBYTECODE", "1")
            .status()
            .expect("Python 3 is required for the process load fixture");
        if !status.success() {
            failed.push(case);
        }
    }
    println!("load evidence: {}", root.display());
    assert!(failed.is_empty(), "acceptance failed for {failed:?}");
}
