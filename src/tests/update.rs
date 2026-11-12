use src::worker::Manager;
use std::{
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

const WAIT_BUDGET: Duration = Duration::from_secs(30);

fn directory(label: &str) -> PathBuf {
    let executable = std::env::current_exe().unwrap();
    let root = executable
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .join("update-tests");
    std::fs::create_dir_all(&root).unwrap();
    loop {
        let path = root.join(format!(
            "{label}-{}-{:016x}",
            std::process::id(),
            getrandom::u64().unwrap()
        ));
        match std::fs::create_dir(&path) {
            Ok(()) => return path,
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(error) => panic!("create update scenario directory: {error}"),
        }
    }
}

async fn cleanup(directory: &Path) {
    // The directory was exclusively created by this test. Windows can keep an
    // executable locked briefly after child exit; wait for removal, not a sleep.
    tokio::time::timeout(WAIT_BUDGET, async {
        loop {
            match std::fs::remove_dir_all(directory) {
                Ok(()) => return,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => return,
                Err(_) => tokio::time::sleep(Duration::from_millis(25)).await,
            }
        }
    })
    .await
    .expect("update scenario directory remained locked after child shutdown");
}

#[tokio::test]
async fn scheduler_advances_jobs_automatically() {
    let manager = Manager::start_pool(PathBuf::from(env!("CARGO_BIN_EXE_src")), 1)
        .await
        .unwrap();
    manager.start_jobs();
    manager.dispatch("!job start".into()).await.unwrap();
    tokio::time::timeout(WAIT_BUDGET, async {
        loop {
            let status = manager
                .dispatch("!job status".into())
                .await
                .unwrap()
                .unwrap();
            let step = status
                .strip_prefix("仕事実行中: ")
                .and_then(|value| value.split_once("ステップ"))
                .map(|(step, _)| step.parse::<u64>().unwrap())
                .expect("job must remain running while the scheduler advances it");
            // Scheduling may be delayed, or may advance multiple times before
            // we run again. The contract is automatic progress, not exactly one
            // tick in a wall-clock window.
            if step >= 1 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("automatic job scheduler did not make progress");
    manager.shutdown().await.unwrap();
}

#[tokio::test]
async fn failed_candidate_records_its_error_and_keeps_serving() {
    let directory = directory("worker-failure");
    let file = directory.join("worker-path.txt");
    let missing = directory.join("does-not-exist.exe");
    std::fs::write(&file, missing.to_string_lossy().as_bytes()).unwrap();
    let manager = Manager::start_pool(PathBuf::from(env!("CARGO_BIN_EXE_src")), 1)
        .await
        .unwrap();
    let task = tokio::spawn(manager.clone().watch(file.clone()));
    let error_path = src::worker::error_file(&file);
    let value: serde_json::Value = tokio::time::timeout(WAIT_BUDGET, async {
        loop {
            // The watcher creates/truncates before writing JSON. Existence
            // alone does not mean the record is complete yet.
            if let Ok(bytes) = std::fs::read(&error_path)
                && let Ok(value) = serde_json::from_slice::<serde_json::Value>(&bytes)
            {
                break value;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .expect("candidate failure was not recorded as complete JSON");
    assert_eq!(value["path"].as_str(), missing.to_str());
    assert!(!value["error"].as_str().unwrap().is_empty());
    assert_eq!(
        manager.dispatch("!ping".into()).await.unwrap().as_deref(),
        Some("Pong! 🦀")
    );
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    manager.shutdown().await.unwrap();
    drop(manager);
    cleanup(&directory).await;
}

#[tokio::test]
async fn switches_real_workers_and_retains_old_worker_on_failure() {
    let binary = PathBuf::from(env!("CARGO_BIN_EXE_src"));
    let directory = directory("worker-switch");
    let second = directory.join(format!("worker-v2{}", std::env::consts::EXE_SUFFIX));
    std::fs::copy(&binary, &second).unwrap();
    let manager = Manager::start(binary).await.unwrap();
    assert_eq!(
        manager
            .dispatch("!ping".into())
            .await
            .unwrap()
            .as_deref()
            .map(|s| s.lines().next().unwrap()),
        Some("Pong! 🦀")
    );
    assert_eq!(
        manager
            .dispatch("!count".into())
            .await
            .unwrap()
            .as_deref()
            .map(|s| s.lines().next().unwrap()),
        Some("カウント: 1")
    );
    assert_eq!(
        manager
            .dispatch_key(1, "!count".into())
            .await
            .unwrap()
            .as_deref()
            .map(|s| s.lines().next().unwrap()),
        Some("カウント: 1")
    );
    manager.dispatch("!job start".into()).await.unwrap();
    manager.dispatch_key(1, "!job start".into()).await.unwrap();
    for _ in 0..3 {
        manager.tick_key(0).await.unwrap();
    }
    for _ in 0..7 {
        manager.tick_key(1).await.unwrap();
    }
    assert_eq!(
        manager
            .dispatch("!job status".into())
            .await
            .unwrap()
            .as_deref()
            .map(|s| s.lines().next().unwrap()),
        Some("仕事実行中: 3ステップ、合計 6")
    );
    assert_eq!(
        manager
            .dispatch_key(1, "!job status".into())
            .await
            .unwrap()
            .as_deref()
            .map(|s| s.lines().next().unwrap()),
        Some("仕事実行中: 7ステップ、合計 28")
    );

    let mut requests = Vec::new();
    for _ in 0..20 {
        let manager = Arc::clone(&manager);
        requests.push(tokio::spawn(async move {
            manager.dispatch("!count".into()).await.unwrap().unwrap()
        }));
    }
    manager.replace(second, false).await.unwrap();
    assert_eq!(
        manager
            .dispatch("!job status".into())
            .await
            .unwrap()
            .as_deref()
            .map(|s| s.lines().next().unwrap()),
        Some("仕事実行中: 3ステップ、合計 6")
    );
    assert_eq!(
        manager
            .dispatch_key(1, "!job status".into())
            .await
            .unwrap()
            .as_deref()
            .map(|s| s.lines().next().unwrap()),
        Some("仕事実行中: 7ステップ、合計 28")
    );
    for _ in 0..27 {
        manager.tick_key(0).await.unwrap();
    }
    for _ in 0..23 {
        manager.tick_key(1).await.unwrap();
    }
    manager.tick_key(0).await.unwrap();
    assert!(
        manager
            .dispatch("!job status".into())
            .await
            .unwrap()
            .unwrap()
            .starts_with("仕事実行中: 31ステップ、合計 496")
    );
    manager.dispatch("!job stop".into()).await.unwrap();
    manager.tick_key(0).await.unwrap();
    assert!(
        manager
            .dispatch("!job status".into())
            .await
            .unwrap()
            .unwrap()
            .starts_with("仕事停止中: 31ステップ、合計 496")
    );
    assert_eq!(
        manager
            .dispatch("!job status".into())
            .await
            .unwrap()
            .as_deref()
            .map(|s| s.lines().next().unwrap()),
        Some("仕事停止中: 31ステップ、合計 496")
    );
    assert_eq!(
        manager
            .dispatch_key(1, "!job status".into())
            .await
            .unwrap()
            .as_deref()
            .map(|s| s.lines().next().unwrap()),
        Some("仕事実行中: 30ステップ、合計 465")
    );
    let mut counts = Vec::new();
    for request in requests {
        counts.push(
            request
                .await
                .unwrap()
                .strip_prefix("カウント: ")
                .unwrap()
                .parse::<u64>()
                .unwrap(),
        );
    }
    counts.sort_unstable();
    assert_eq!(counts, (2..=21).collect::<Vec<_>>());
    assert_eq!(
        manager
            .dispatch("!count".into())
            .await
            .unwrap()
            .as_deref()
            .map(|s| s.lines().next().unwrap()),
        Some("カウント: 22")
    );
    assert_eq!(
        manager
            .dispatch_key(1, "!count".into())
            .await
            .unwrap()
            .as_deref()
            .map(|s| s.lines().next().unwrap()),
        Some("カウント: 2")
    );
    assert!(
        manager
            .replace(directory.join("missing.exe"), false)
            .await
            .is_err()
    );
    assert_eq!(
        manager
            .dispatch("!count".into())
            .await
            .unwrap()
            .as_deref()
            .map(|s| s.lines().next().unwrap()),
        Some("カウント: 23")
    );
    assert_eq!(
        manager
            .dispatch_key(1, "!count".into())
            .await
            .unwrap()
            .as_deref()
            .map(|s| s.lines().next().unwrap()),
        Some("カウント: 3")
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
    assert_eq!(manager.dispatch("hello".into()).await.unwrap(), None);
    manager.shutdown().await.unwrap();
    drop(manager);
    cleanup(&directory).await;
}
