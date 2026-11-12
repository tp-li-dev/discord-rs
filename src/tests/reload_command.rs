use src::worker::Manager;
use std::{path::PathBuf, time::Duration};

const BUILD_BUDGET: Duration = Duration::from_secs(600);
const CLEANUP_BUDGET: Duration = Duration::from_secs(30);

fn directory() -> PathBuf {
    let executable = std::env::current_exe().unwrap();
    let root = executable
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .join("reload-command-tests");
    std::fs::create_dir_all(&root).unwrap();
    loop {
        let path = root.join(format!(
            "reload-{}-{:016x}",
            std::process::id(),
            getrandom::u64().unwrap()
        ));
        match std::fs::create_dir(&path) {
            Ok(()) => return path,
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(error) => panic!("create reload scenario directory: {error}"),
        }
    }
}

#[tokio::test]
async fn reload_command_builds_and_switches_a_live_manager() {
    let directory = directory();
    let file = directory.join("worker-path.txt");
    let manager = Manager::start(PathBuf::from(env!("CARGO_BIN_EXE_src")))
        .await
        .unwrap();
    let before = manager.describe_workers().await;
    manager.dispatch("!job start".into()).await.unwrap();
    for _ in 0..4 {
        manager.tick_key(0).await.unwrap();
    }
    let watcher = tokio::spawn(manager.clone().watch(file.clone()));
    let output = tokio::time::timeout(
        // This includes a potentially cold release build under CPU load. It is
        // a watchdog, not the runtime's hot-swap latency requirement.
        BUILD_BUDGET,
        tokio::process::Command::new(env!("CARGO_BIN_EXE_reload"))
            .arg("general")
            .env("DISCORD_WORKER_FILE", &file)
            .env("CARGO_NET_OFFLINE", "true")
            .env("CARGO_TERM_COLOR", "never")
            .env_remove("DISCORD_TOKEN")
            .kill_on_drop(true)
            .output(),
    )
    .await
    .expect("reload build/switch exceeded its 600-second cold-build watchdog")
    .unwrap();
    assert!(
        output.status.success(),
        "{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let after = manager.describe_workers().await;
    let output_text = String::from_utf8_lossy(&output.stdout);
    assert!(output_text.contains("更新しました！ Workerのビルドを確認しました"));
    assert_ne!(before, after);
    assert_eq!(after.matches("[REDACTED] / PID:").count(), 2, "{after}");
    // !version is application response data, whereas diagnostic output must
    // suppress the arbitrary build label. Still verify the new build is live.
    let version = manager.dispatch("!version".into()).await.unwrap().unwrap();
    let build = version
        .strip_prefix("Workerビルド: ")
        .and_then(|value| value.split_once(" / PID: "))
        .map(|(build, _)| build)
        .unwrap();
    assert!(build.starts_with("build-"), "{version}");
    assert!(!output_text.contains(build));
    assert!(!String::from_utf8_lossy(&output.stderr).contains(build));
    assert!(!after.contains(build));
    assert_eq!(
        manager
            .dispatch("!job status".into())
            .await
            .unwrap()
            .as_deref()
            .map(|s| s.lines().next().unwrap()),
        Some("仕事実行中: 4ステップ、合計 10")
    );
    manager.tick_key(0).await.unwrap();
    assert_eq!(
        manager
            .dispatch("!job status".into())
            .await
            .unwrap()
            .as_deref()
            .map(|s| s.lines().next().unwrap()),
        Some("仕事実行中: 5ステップ、合計 15")
    );
    assert_eq!(
        manager
            .dispatch("!ping".into())
            .await
            .unwrap()
            .as_deref()
            .map(|s| s.lines().next().unwrap()),
        Some("Pong! 🦀")
    );
    let worker = PathBuf::from(std::fs::read_to_string(&file).unwrap());
    watcher.abort();
    assert!(watcher.await.unwrap_err().is_cancelled());
    manager.shutdown().await.unwrap();
    drop(manager);
    let workers = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("workers");
    assert_eq!(worker.parent(), Some(workers.as_path()));
    tokio::time::timeout(CLEANUP_BUDGET, async {
        loop {
            match std::fs::remove_file(&worker) {
                Ok(()) => break,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => break,
                Err(_) => tokio::time::sleep(Duration::from_millis(25)).await,
            }
        }
        // Only this test's exclusively created directory is removed.
        loop {
            match std::fs::remove_dir_all(&directory) {
                Ok(()) => break,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => break,
                Err(_) => tokio::time::sleep(Duration::from_millis(25)).await,
            }
        }
    })
    .await
    .expect("reload artifacts remained locked after watcher and worker shutdown");
}
