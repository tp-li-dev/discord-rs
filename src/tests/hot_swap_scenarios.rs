//! Process scenarios for architecture §6 / T04–T06 / T19.
//! The relay runs the production Worker; only fault timing and build labels change.
//! These are Worker-manager tests, not proof of Shard/Discord hot swapping.
use src::worker::{DeploymentIdentity, Manager, error_file, status_file};
use std::{
    future::Future,
    path::{Path, PathBuf},
    process::{Command, Output},
    sync::{Arc, OnceLock},
    time::Duration,
};
use tokio::{task::JoinHandle, time::timeout};

const BUDGET: Duration = Duration::from_secs(30);
const PONG: &str = "Pong! 🦀";

async fn bounded<T>(future: impl Future<Output = T>) -> T {
    timeout(BUDGET, future).await.expect("scenario deadline")
}

fn sidecar(path: &Path, suffix: &str) -> PathBuf {
    let mut value = path.as_os_str().to_owned();
    value.push(suffix);
    value.into()
}

fn unique_directory(label: &str) -> PathBuf {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("target")
        .join("hot-swap-scenarios");
    std::fs::create_dir_all(&root).unwrap();
    let path = root.join(format!(
        "{label}-{}-{:016x}",
        std::process::id(),
        getrandom::u64().unwrap()
    ));
    std::fs::create_dir(&path).unwrap();
    path
}

fn relay_binary() -> &'static Path {
    static BINARY: OnceLock<PathBuf> = OnceLock::new();
    BINARY.get_or_init(|| {
        let output =
            unique_directory("relay-build").join(format!("relay{}", std::env::consts::EXE_SUFFIX));
        let result = Command::new("rustc")
            .args(["--edition=2024", "-Dwarnings"])
            .arg(Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/hot_swap_worker.rs"))
            .arg("-o")
            .arg(&output)
            .env_remove("DISCORD_TOKEN")
            .output()
            .expect("compile fault relay with rustc");
        assert!(
            result.status.success(),
            "relay compilation failed: {result:?}"
        );
        output
    })
}

struct Scenario(PathBuf);
impl Scenario {
    fn new(label: &str) -> Self {
        Self(unique_directory(label))
    }

    fn worker(&self, mode: &str) -> PathBuf {
        let path = self
            .0
            .join(format!("{mode}{}", std::env::consts::EXE_SUFFIX));
        std::fs::copy(relay_binary(), &path).unwrap();
        std::fs::write(sidecar(&path, ".real"), env!("CARGO_BIN_EXE_src")).unwrap();
        path
    }

    async fn cleanup(self) {
        // Manager's kill_on_drop is asynchronous on Windows. Yield to the reactor
        // while waiting for the exact, exclusively created test directory to unlock.
        bounded(async {
            loop {
                match std::fs::remove_dir_all(&self.0) {
                    Ok(()) => break,
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => break,
                    Err(_) => tokio::time::sleep(Duration::from_millis(25)).await,
                }
            }
        })
        .await;
    }
}

// Aborting a scenario on an assertion must not leave a watcher/dispatch alive.
struct Task<T>(JoinHandle<T>);
impl<T> Drop for Task<T> {
    fn drop(&mut self) {
        self.0.abort();
    }
}
async fn joined<T>(task: &mut Task<T>) -> T {
    bounded(&mut task.0).await.expect("scenario task panicked")
}

fn events(path: &Path) -> Vec<String> {
    match std::fs::read_to_string(sidecar(path, ".events")) {
        Ok(value) => value.lines().map(str::to_owned).collect(),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Vec::new(),
        Err(error) => panic!("read relay events: {error}"),
    }
}

async fn wait_for(mut condition: impl FnMut() -> bool) {
    bounded(async {
        while !condition() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
}

async fn wait_event(path: &Path, event: &str) {
    wait_for(|| events(path).iter().any(|value| value == event)).await;
}

fn release(path: &Path) {
    std::fs::write(sidecar(path, ".release"), b"release").unwrap();
}

async fn manager(path: PathBuf) -> Arc<Manager> {
    // No global environment mutation: scenarios run concurrently and retain
    // deployment identity checks in the real Hello exchange.
    let identity = DeploymentIdentity::parse(|_| Ok(None)).unwrap();
    bounded(Manager::start_pool_with_identity(path, 1, identity))
        .await
        .unwrap()
}

async fn count(manager: &Manager, expected: u64) {
    assert_eq!(
        bounded(manager.dispatch("!count".into())).await.unwrap(),
        Some(format!("カウント: {expected}"))
    );
}

fn read_error(file: &Path) -> Option<serde_json::Value> {
    // The production watcher writes in place: tolerate an incomplete read while
    // waiting, but validate every field after observing a complete JSON record.
    std::fs::read(error_file(file))
        .ok()
        .and_then(|bytes| serde_json::from_slice(&bytes).ok())
}

async fn wait_error(file: &Path, candidate: &Path) -> serde_json::Value {
    let mut observed = None;
    wait_for(|| {
        if let Some(value) = read_error(file)
            && value["path"].as_str() == candidate.to_str()
        {
            observed = Some(value);
            true
        } else {
            false
        }
    })
    .await;
    observed.unwrap()
}

async fn wait_success(file: &Path, candidate: &Path) {
    wait_for(|| {
        std::fs::read_to_string(status_file(file))
            .is_ok_and(|value| value == candidate.to_string_lossy())
            && !error_file(file).exists()
    })
    .await;
}

#[tokio::test]
async fn t05_candidate_crashes_before_ready_while_v1_keeps_its_process_and_state() {
    let scenario = Scenario::new("t05");
    let old = scenario.worker("v1");
    let candidate = scenario.worker("crash-before-ready");
    let healthy = scenario.worker("v2");
    let manager = manager(old).await;
    count(&manager, 1).await;
    let before = bounded(manager.describe_workers()).await;
    let file = scenario.0.join("worker-path.txt");
    std::fs::write(&file, candidate.to_str().unwrap()).unwrap();
    let mut watcher = Task(tokio::spawn(manager.clone().watch(file.clone())));

    wait_event(&candidate, "health-blocked").await;
    // Candidate exists and has received Health, but cannot emit Ready until
    // explicitly released. No scheduler sleeps establish this ordering.
    count(&manager, 2).await;
    assert_eq!(bounded(manager.describe_workers()).await, before);
    release(&candidate);
    let failure = wait_error(&file, &candidate).await;
    assert_eq!(failure["error"], "worker exited");
    assert!(!status_file(&file).exists());
    assert!(!events(&candidate).iter().any(|event| event == "ready"));
    assert!(events(&candidate).contains(&"crashed-before-ready".into()));
    assert_eq!(bounded(manager.describe_workers()).await, before);
    count(&manager, 3).await;

    // A later healthy update proves that failure reopened admission and kept
    // the current state, rather than merely leaving one successful ping.
    std::fs::write(&file, healthy.to_str().unwrap()).unwrap();
    wait_success(&file, &healthy).await;
    count(&manager, 4).await;
    assert!(
        bounded(manager.describe_workers())
            .await
            .contains("v2 / PID:")
    );
    watcher.0.abort();
    let _ = (&mut watcher.0).await;
    drop(watcher);
    drop(manager);
    scenario.cleanup().await;
}

async fn rejected_hello(mode: &str, expected_error: &str) {
    let scenario = Scenario::new(mode);
    let candidate = scenario.worker(mode);
    let healthy = scenario.worker("v2");
    let manager = manager(scenario.worker("v1")).await;
    count(&manager, 1).await;
    count(&manager, 2).await;
    let before = bounded(manager.describe_workers()).await;
    let file = scenario.0.join("worker-path.txt");
    std::fs::write(&file, candidate.to_str().unwrap()).unwrap();
    let mut watcher = Task(tokio::spawn(manager.clone().watch(file.clone())));
    let failure = wait_error(&file, &candidate).await;
    assert!(
        failure["error"].as_str().unwrap().contains(expected_error),
        "{failure}"
    );
    wait_event(&candidate, "hello-rejected").await;
    let history = events(&candidate);
    assert!(history.contains(&"ready".into()));
    assert!(history.contains(&"hello".into()));
    assert!(!history.contains(&"restore".into()));
    assert!(!history.contains(&"dispatch".into()));
    assert!(!status_file(&file).exists());
    assert_eq!(bounded(manager.describe_workers()).await, before);
    count(&manager, 3).await;

    std::fs::write(&file, healthy.to_str().unwrap()).unwrap();
    wait_success(&file, &healthy).await;
    count(&manager, 4).await;
    assert_ne!(bounded(manager.describe_workers()).await, before);
    watcher.0.abort();
    let _ = (&mut watcher.0).await;
    drop(watcher);
    drop(manager);
    scenario.cleanup().await;
}

// Existing ipc_handshake.rs tests Ready-field rejection; these reach the
// second (Hello) stage and verify watcher diagnostics and a subsequent update.
#[tokio::test]
async fn t19_unknown_hello_version_is_recorded_and_v1_state_survives() {
    rejected_hello("hello-version", "protocol").await;
}

#[tokio::test]
async fn t19_unknown_required_hello_capability_is_recorded_and_v1_state_survives() {
    rejected_hello("hello-capability", "capability").await;
}

async fn crash_child(scenario: &Scenario) -> Output {
    scenario.worker("v1");
    scenario.worker("crash-once");
    let mut command = tokio::process::Command::new(std::env::current_exe().unwrap());
    command
        .args(["--exact", "t06_process_driver", "--nocapture"])
        .env("HOT_SWAP_SCENARIO_DIR", &scenario.0)
        .env_remove("DISCORD_TOKEN")
        .kill_on_drop(true);
    let result = bounded(command.output()).await.unwrap();
    assert!(result.status.success(), "T06 child failed: {result:?}");
    result
}

#[tokio::test]
async fn t06_crash_after_commit_restarts_v2_without_replaying_the_failed_request() {
    let scenario = Scenario::new("t06-recovery");
    let result = crash_child(&scenario).await;
    let stderr = String::from_utf8(result.stderr).unwrap();
    assert!(stderr.contains("Workerを再起動しました"), "{stderr}");
    scenario.cleanup().await;
}

// Run in a subprocess so the actual Manager stderr, rather than a fixture's
// invented diagnostic, can be checked by the enclosing process scenario.
#[tokio::test]
async fn t06_process_driver() {
    let Some(directory) = std::env::var_os("HOT_SWAP_SCENARIO_DIR") else {
        return;
    };
    let directory = PathBuf::from(directory);
    let old = directory.join(format!("v1{}", std::env::consts::EXE_SUFFIX));
    let candidate = directory.join(format!("crash-once{}", std::env::consts::EXE_SUFFIX));
    let manager = manager(old.clone()).await;
    for expected in 1..=3 {
        count(&manager, expected).await;
    }
    let before = bounded(manager.describe_workers()).await;
    let file = directory.join("worker-path.txt");
    std::fs::write(&file, candidate.to_str().unwrap()).unwrap();
    let mut watcher = Task(tokio::spawn(manager.clone().watch(file.clone())));
    wait_success(&file, &candidate).await;
    let committed = bounded(manager.describe_workers()).await;
    assert_ne!(committed, before);
    assert!(committed.contains("crash-once / PID:"));
    wait_event(&candidate, "restore-completed").await;

    // The production handler increments count, then its result is lost when
    // the relay exits. This request must surface an error, never automatic replay.
    let failure = bounded(manager.dispatch("!count".into()))
        .await
        .unwrap_err();
    assert_eq!(failure.to_string(), "worker exited");
    wait_event(&candidate, "crashed-after-result").await;
    let recovered = bounded(manager.describe_workers()).await;
    assert_ne!(recovered, committed);
    assert!(recovered.contains("crash-once / PID:"));
    // Restored is flushed before the relay appends its completion marker.
    wait_for(|| {
        events(&candidate)
            .iter()
            .filter(|event| *event == "restore-completed")
            .count()
            == 2
    })
    .await;
    let history = events(&candidate);
    assert_eq!(
        history.iter().filter(|event| *event == "dispatch").count(),
        1
    );
    assert_eq!(
        history
            .iter()
            .filter(|event| *event == "restore-completed")
            .count(),
        2
    );
    // Restore the last confirmed state (3), not the unconfirmed count (4).
    count(&manager, 4).await;
    count(&manager, 5).await;
    assert_eq!(
        events(&old)
            .iter()
            .filter(|event| *event == "dispatch")
            .count(),
        3
    );
    assert_eq!(
        events(&candidate)
            .iter()
            .filter(|event| *event == "dispatch")
            .count(),
        3
    );
    watcher.0.abort();
    let _ = (&mut watcher.0).await;
    drop(watcher);
    drop(manager);
}

#[tokio::test]
#[ignore = "T06: post-commit crashes only log restart to stderr; no management failure record"]
async fn t06_post_commit_unknown_outcome_is_recorded_for_management() {
    // The management-file convention is an observability extension, not a
    // claim that §6 prescribes this persistence format or a rollback to v1.
    let scenario = Scenario::new("t06-record");
    crash_child(&scenario).await;
    let candidate = scenario
        .0
        .join(format!("crash-once{}", std::env::consts::EXE_SUFFIX));
    let failure = read_error(&scenario.0.join("worker-path.txt"));
    scenario.cleanup().await;
    let failure =
        failure.expect("post-commit crash/unknown request outcome must remain observable");
    assert_eq!(failure["path"].as_str(), candidate.to_str());
    assert!(!failure["error"].as_str().unwrap().is_empty());
}

#[tokio::test]
async fn t04_inflight_response_survives_the_current_serial_cutover() {
    let scenario = Scenario::new("t04-serial");
    let old = scenario.worker("v1");
    let candidate = scenario.worker("v2");
    let manager = manager(old.clone()).await;
    count(&manager, 1).await;
    let mut request = Task(tokio::spawn({
        let manager = manager.clone();
        async move { manager.dispatch("!slow".into()).await }
    }));
    wait_event(&old, "slow-started").await;
    let mut update = Task(tokio::spawn({
        let manager = manager.clone();
        let candidate = candidate.clone();
        async move { manager.replace(candidate, false).await }
    }));
    wait_event(&candidate, "hello").await;
    release(&old);
    assert_eq!(joined(&mut request).await.unwrap().as_deref(), Some(PONG));
    joined(&mut update).await.unwrap();
    // This passing characterization checks completion and state migration,
    // while the ignored tests below retain the stronger §6 requirements.
    assert!(
        bounded(manager.describe_workers())
            .await
            .contains("v2 / PID:")
    );
    count(&manager, 2).await;
    assert_eq!(
        events(&old)
            .iter()
            .filter(|event| *event == "dispatch")
            .count(),
        2
    );
    assert_eq!(
        events(&candidate)
            .iter()
            .filter(|event| *event == "dispatch")
            .count(),
        1
    );
    drop(request);
    drop(update);
    drop(manager);
    scenario.cleanup().await;
}

#[tokio::test]
#[ignore = "T04: the 5-second IPC deadline times out a 5-second in-flight handler"]
async fn t04_five_second_v1_response_completes_during_update() {
    // The production demo has no five-second handler. Delay its actual ping at
    // the IPC relay: this reproduces the same total-request deadline boundary,
    // without claiming application-task or Discord REST drain was exercised.
    let scenario = Scenario::new("t04-five-seconds");
    let old = scenario.worker("v1");
    let candidate = scenario.worker("v2");
    let manager = manager(old.clone()).await;
    let mut request = Task(tokio::spawn({
        let manager = manager.clone();
        async move { manager.dispatch("!slow-five".into()).await }
    }));
    wait_event(&old, "slow-started").await;
    let mut update = Task(tokio::spawn({
        let manager = manager.clone();
        let candidate = candidate.clone();
        async move { manager.replace(candidate, false).await }
    }));
    wait_event(&candidate, "hello").await;
    release(&old);
    let response = joined(&mut request).await;
    let switched = joined(&mut update).await;
    drop(request);
    drop(update);
    drop(manager);
    scenario.cleanup().await;
    assert_eq!(response.unwrap().as_deref(), Some(PONG));
    switched.unwrap();
}

#[tokio::test]
#[ignore = "T04: replace waits on the old dispatch read lock; new requests cannot reach v2 during drain"]
async fn t04_new_requests_reach_v2_before_v1_is_released() {
    let scenario = Scenario::new("t04-concurrent-drain");
    let old = scenario.worker("v1");
    let candidate = scenario.worker("v2");
    let manager = manager(old.clone()).await;
    let mut request = Task(tokio::spawn({
        let manager = manager.clone();
        async move { manager.dispatch("!slow".into()).await }
    }));
    wait_event(&old, "slow-started").await;
    let mut update = Task(tokio::spawn({
        let manager = manager.clone();
        let candidate = candidate.clone();
        async move { manager.replace(candidate, false).await }
    }));
    wait_event(&candidate, "hello").await;
    // Old completion is gate-controlled, so this is not an assertion about
    // winning a sleep race. Candidate must commit while old work is still held.
    let committed_while_old_running = timeout(Duration::from_secs(2), &mut update.0).await;
    let concurrent_commit = committed_while_old_running.is_ok();
    let routed_to_v2 = if concurrent_commit {
        let description = bounded(manager.describe_workers()).await;
        let pong = bounded(manager.dispatch("!ping".into())).await.unwrap();
        description.contains("v2 / PID:")
            && pong.as_deref() == Some(PONG)
            && events(&candidate)
                .iter()
                .filter(|event| *event == "dispatch")
                .count()
                == 1
            && events(&old)
                .iter()
                .filter(|event| *event == "dispatch")
                .count()
                == 1
    } else {
        false
    };
    release(&old);
    let response = joined(&mut request).await;
    if !concurrent_commit {
        joined(&mut update).await.unwrap();
    } else {
        committed_while_old_running.unwrap().unwrap().unwrap();
    }
    drop(request);
    drop(update);
    drop(manager);
    scenario.cleanup().await;
    assert_eq!(response.unwrap().as_deref(), Some(PONG));
    assert!(
        routed_to_v2,
        "v2 must serve new work while v1 remains in flight"
    );
}
