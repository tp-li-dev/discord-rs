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

fn application_worker_binary() -> &'static Path {
    static BINARY: OnceLock<PathBuf> = OnceLock::new();
    BINARY.get_or_init(|| {
        let output = unique_directory("application-build")
            .join(format!("application{}", std::env::consts::EXE_SUFFIX));
        let test_executable = std::env::current_exe().unwrap();
        let dependencies = test_executable.parent().unwrap();
        let mut command = Command::new("rustc");
        command
            .args(["--edition=2024", "-Dwarnings"])
            .arg(Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/hot_swap_handler.rs"))
            .arg("-L")
            .arg(dependencies);
        // `cargo check`/clippy can leave additional .rmeta files beside the
        // built libraries. Link only the actual rlibs used by executable tests.
        for name in ["src", "bot_framework", "tokio"] {
            let prefix = format!("lib{name}-");
            let libraries: Vec<_> = std::fs::read_dir(dependencies)
                .unwrap()
                .map(|entry| entry.unwrap().path())
                .filter(|path| {
                    path.file_name()
                        .and_then(|file| file.to_str())
                        .is_some_and(|file| file.starts_with(&prefix) && file.ends_with(".rlib"))
                })
                .collect();
            assert_eq!(
                libraries.len(),
                1,
                "need one compiled {name} rlib for fixture linking"
            );
            let mut external = std::ffi::OsString::from(format!("{name}="));
            external.push(&libraries[0]);
            command.arg("--extern").arg(external);
        }
        let result = command
            .arg("-o")
            .arg(&output)
            .env_remove("DISCORD_TOKEN")
            .output()
            .expect("compile production Worker with an async application handler");
        assert!(
            result.status.success(),
            "application fixture compilation failed: {result:?}"
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

async fn wait_active(manager: &Manager, build: &str) {
    // The old relay remains gated until release(), so observing the new route
    // never depends on beating an application-completion sleep. Keep this
    // watchdog below the fixture's gate timeout; it is not a latency target.
    timeout(Duration::from_secs(8), async {
        loop {
            if manager
                .describe_workers()
                .await
                .contains(&format!("{build} / PID:"))
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("new generation must become active while the old request is gated");
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
    assert_eq!(failure["phase"], "update");
    assert_eq!(failure["exit_code"], 71);
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
    assert_eq!(failure["error"], expected_error);
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
    rejected_hello("hello-version", "incompatible protocol range").await;
}

#[tokio::test]
async fn t19_unknown_required_hello_capability_is_recorded_and_v1_state_survives() {
    rejected_hello("hello-capability", "unsupported required capability").await;
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
    // Match the Gateway path: the failed delivery remains tied to the crashed
    // generation even after the Manager has installed its replacement.
    let mut response = bounded(manager.begin_response(0)).await.unwrap();
    let failure = bounded(manager.dispatch_key_with_response(&mut response, 0, "!count".into()))
        .await
        .unwrap_err();
    drop(response);
    let (_, unknown) = manager.unknown_responses();
    assert_eq!(unknown.len(), 1);
    assert_eq!(unknown[0].0, 0);
    assert_eq!(failure.to_string(), "worker exited");
    wait_event(&candidate, "crashed-after-result").await;
    let recorded = wait_error(&file, &candidate).await;
    assert_eq!(recorded["phase"], "runtime");
    assert_eq!(recorded["worker"], 0);
    assert_eq!(recorded["outcome"], "unknown");
    assert_eq!(recorded["recovery"], "restarted");
    assert_eq!(recorded["error"], "worker exited");
    assert_eq!(recorded["exit_code"], 72);
    let committed_pid: u32 = committed
        .lines()
        .nth(1)
        .unwrap()
        .rsplit(" / PID: ")
        .next()
        .unwrap()
        .parse()
        .unwrap();
    assert_eq!(recorded["pid"], committed_pid);
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
    // The watcher polls every two seconds. Successful requests and the next
    // no-op update must not erase a crash's unknown-outcome record.
    tokio::time::sleep(Duration::from_secs(3)).await;
    assert_eq!(read_error(&file), Some(recorded));
    watcher.0.abort();
    let _ = (&mut watcher.0).await;
    drop(watcher);
    drop(manager);
}

#[tokio::test]
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
    assert_eq!(failure["error"], "worker exited");
    assert_eq!(failure["phase"], "runtime");
    assert_eq!(failure["outcome"], "unknown");
    assert_eq!(failure["recovery"], "restarted");
    assert_eq!(failure["exit_code"], 72);
}

#[tokio::test]
async fn t04_inflight_response_and_checkpoint_survive_cutover() {
    let scenario = Scenario::new("t04-checkpoint");
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
    // A completed update must preserve both the old response and the last
    // confirmed application checkpoint.
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
    wait_active(&manager, "v2").await;
    assert!(!request.0.is_finished());
    assert!(!update.0.is_finished(), "old work still requires drain");
    assert_eq!(
        bounded(manager.dispatch("!ping".into()))
            .await
            .unwrap()
            .as_deref(),
        Some(PONG)
    );
    assert!(!events(&old).contains(&"slow-completed".into()));
    let released_at = std::time::Instant::now();
    release(&old);
    let response = joined(&mut request).await;
    assert!(released_at.elapsed() >= Duration::from_secs(5));
    let switched = joined(&mut update).await;
    drop(request);
    drop(update);
    drop(manager);
    scenario.cleanup().await;
    assert_eq!(response.unwrap().as_deref(), Some(PONG));
    switched.unwrap();
}

#[tokio::test]
async fn t04_new_requests_reach_v2_before_v1_is_released() {
    let scenario = Scenario::new("t04-concurrent-drain");
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
    // replace() completes only after retiring v1. Its routing commit must
    // nevertheless let v2 finish new work while v1 is explicitly held here.
    wait_active(&manager, "v2").await;
    count(&manager, 2).await;
    assert!(!request.0.is_finished(), "v1 must remain in flight");
    assert!(!update.0.is_finished(), "v1 has not drained yet");
    assert!(!events(&old).contains(&"slow-completed".into()));
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
    release(&old);
    let response = joined(&mut request).await;
    joined(&mut update).await.unwrap();
    // The late v1 response carries count=1. It must not overwrite count=2
    // already committed by v2 while the old generation was draining.
    count(&manager, 3).await;
    drop(request);
    drop(update);
    drop(manager);
    scenario.cleanup().await;
    assert_eq!(response.unwrap().as_deref(), Some(PONG));
}

#[tokio::test]
async fn t04_external_response_stays_with_v1_while_v2_accepts_new_work() {
    let scenario = Scenario::new("t04-external-response");
    let old = scenario.worker("v1");
    let candidate = scenario.worker("v2");
    let manager = manager(old.clone()).await;
    count(&manager, 1).await;
    let mut response = bounded(manager.begin_response(0)).await.unwrap();
    let mut request = Task(tokio::spawn({
        let manager = manager.clone();
        async move {
            let result = manager
                .dispatch_key_with_response(&mut response, 0, "!slow".into())
                .await;
            (result, response)
        }
    }));
    wait_event(&old, "slow-started").await;
    let mut update = Task(tokio::spawn({
        let manager = manager.clone();
        async move { manager.replace(candidate, false).await }
    }));
    wait_active(&manager, "v2").await;

    let mut next_response = bounded(manager.begin_response(0)).await.unwrap();
    assert_eq!(
        bounded(manager.dispatch_key_with_response(&mut next_response, 0, "!count".into()))
            .await
            .unwrap()
            .as_deref(),
        Some("カウント: 2")
    );
    next_response.confirm();
    assert!(!request.0.is_finished());
    assert!(!update.0.is_finished());

    release(&old);
    let (result, old_response) = joined(&mut request).await;
    assert_eq!(result.unwrap().as_deref(), Some(PONG));
    assert!(
        bounded(manager.describe_workers())
            .await
            .contains("v2 / PID:")
    );
    assert!(
        !update.0.is_finished(),
        "the old external result is still pending"
    );
    // This simulates Gateway confirmation of the response belonging to v1.
    // New admission must remain on v2 throughout this final drain stage.
    count(&manager, 3).await;
    old_response.confirm();
    joined(&mut update).await.unwrap();
    assert!(manager.unknown_responses().1.is_empty());
    count(&manager, 4).await;
    drop(request);
    drop(update);
    drop(manager);
    scenario.cleanup().await;
}

#[tokio::test]
async fn t04_unknown_old_response_blocks_followup_updates_until_reconciled() {
    let scenario = Scenario::new("t04-unknown-response");
    let old = scenario.worker("v1");
    let candidate = scenario.worker("v2");
    let manager = manager(old).await;
    let mut old_response = bounded(manager.begin_response(0)).await.unwrap();
    assert_eq!(
        bounded(manager.dispatch_key_with_response(&mut old_response, 0, "!ping".into()))
            .await
            .unwrap()
            .as_deref(),
        Some(PONG)
    );
    let mut update = Task(tokio::spawn({
        let manager = manager.clone();
        let candidate = candidate.clone();
        async move { manager.replace(candidate, false).await }
    }));
    wait_active(&manager, "v2").await;
    assert!(!update.0.is_finished());
    drop(old_response);
    assert_eq!(
        joined(&mut update).await.unwrap_err().to_string(),
        "unresolved response outcomes prevent worker update"
    );
    assert!(
        bounded(manager.describe_workers())
            .await
            .contains("v2 / PID:")
    );
    let mut new_response = bounded(manager.begin_response(0)).await.unwrap();
    assert_eq!(
        bounded(manager.dispatch_key_with_response(&mut new_response, 0, "!count".into()))
            .await
            .unwrap()
            .as_deref(),
        Some("カウント: 1")
    );
    new_response.confirm();
    let (instance, unknown) = manager.unknown_responses();
    assert_eq!(unknown.len(), 1);
    let (worker, operation) = unknown[0];
    assert_eq!(worker, 0);
    assert_eq!(
        bounded(manager.replace(candidate.clone(), true))
            .await
            .unwrap_err()
            .to_string(),
        "unresolved response outcomes prevent worker update"
    );
    assert_eq!(manager.unknown_responses().1, unknown);
    manager
        .reconcile_response(&instance, worker, operation, "fixture delivery confirmed")
        .unwrap();
    bounded(manager.replace(candidate, true)).await.unwrap();
    assert!(manager.unknown_responses().1.is_empty());
    count(&manager, 2).await;
    drop(update);
    drop(manager);
    scenario.cleanup().await;
}

#[tokio::test]
async fn t04_cancelling_retirement_keeps_old_response_and_new_routing() {
    let scenario = Scenario::new("t04-cancel-retirement");
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
    wait_active(&manager, "v2").await;
    assert!(!update.0.is_finished());
    update.0.abort();
    assert!(bounded(&mut update.0).await.unwrap_err().is_cancelled());
    count(&manager, 1).await;
    assert!(!request.0.is_finished());
    release(&old);
    assert_eq!(joined(&mut request).await.unwrap().as_deref(), Some(PONG));
    bounded(manager.replace(candidate, true)).await.unwrap();
    count(&manager, 2).await;
    drop(request);
    drop(update);
    drop(manager);
    scenario.cleanup().await;
}

#[tokio::test]
async fn t04_stateful_request_finishes_before_snapshot_handoff() {
    let scenario = Scenario::new("t04-stateful-barrier");
    let old = scenario.worker("count-gated");
    let candidate = scenario.worker("v2");
    let manager = manager(old.clone()).await;
    let mut request = Task(tokio::spawn({
        let manager = manager.clone();
        async move { manager.dispatch("!count".into()).await }
    }));
    wait_event(&old, "stateful-started").await;
    let mut update = Task(tokio::spawn({
        let manager = manager.clone();
        let candidate = candidate.clone();
        async move { manager.replace(candidate, false).await }
    }));
    wait_event(&candidate, "hello").await;
    // snapshot-v3 is shared mutable demo state. Its in-flight mutation must
    // finish before v2 can be restored; read-only concurrency cannot lose it.
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(!request.0.is_finished());
    assert!(!update.0.is_finished());
    assert!(
        bounded(manager.describe_workers())
            .await
            .contains("count-gated / PID:")
    );
    assert!(!events(&candidate).contains(&"restore".into()));
    release(&old);
    assert_eq!(
        joined(&mut request).await.unwrap().as_deref(),
        Some("カウント: 1")
    );
    joined(&mut update).await.unwrap();
    wait_active(&manager, "v2").await;
    count(&manager, 2).await;
    drop(request);
    drop(update);
    drop(manager);
    scenario.cleanup().await;
}

#[tokio::test]
async fn t04_real_five_second_handler_drains_while_v2_serves_new_requests() {
    let scenario = Scenario::new("t04-real-handler");
    let application = scenario
        .0
        .join(format!("application{}", std::env::consts::EXE_SUFFIX));
    std::fs::copy(application_worker_binary(), &application).unwrap();
    let old = scenario.worker("v1");
    let candidate = scenario.worker("v2");
    std::fs::write(sidecar(&old, ".real"), application.to_str().unwrap()).unwrap();
    let manager = manager(old.clone()).await;
    let mut request = Task(tokio::spawn({
        let manager = manager.clone();
        async move { manager.dispatch("!five_second".into()).await }
    }));
    wait_for(|| sidecar(&application, ".started").exists()).await;
    let mut update = Task(tokio::spawn({
        let manager = manager.clone();
        async move { manager.replace(candidate, false).await }
    }));
    wait_active(&manager, "v2").await;
    assert!(!request.0.is_finished());
    assert!(!update.0.is_finished());
    assert_eq!(
        bounded(manager.dispatch("!ping".into()))
            .await
            .unwrap()
            .as_deref(),
        Some(PONG)
    );
    let released_at = std::time::Instant::now();
    release(&application);
    assert_eq!(
        joined(&mut request).await.unwrap().as_deref(),
        Some("five-second handler completed")
    );
    assert!(released_at.elapsed() >= Duration::from_secs(5));
    joined(&mut update).await.unwrap();
    drop(request);
    drop(update);
    drop(manager);
    scenario.cleanup().await;
}

#[tokio::test]
async fn t06_recovery_during_candidate_readiness_rechecks_unknown_old_outcomes() {
    let scenario = Scenario::new("t06-readiness-recovery-race");
    let old = scenario.worker("crash-once");
    let candidate = scenario.worker("ready-gated");
    let manager = manager(old.clone()).await;
    // Establish a confirmed, restored checkpoint before the injected loss.
    bounded(manager.replace(old.clone(), true)).await.unwrap();
    wait_event(&old, "restore-completed").await;
    let before = bounded(manager.describe_workers()).await;
    let mut old_response = bounded(manager.begin_response(0)).await.unwrap();
    let mut update = Task(tokio::spawn({
        let manager = manager.clone();
        let candidate = candidate.clone();
        async move { manager.replace(candidate, false).await }
    }));
    // replace has passed its first old-generation check but is still waiting
    // for the candidate. Recovery may install a generation in this interval.
    wait_event(&candidate, "health-blocked").await;
    let failure =
        bounded(manager.dispatch_key_with_response(&mut old_response, 0, "!count".into()))
            .await
            .unwrap_err();
    assert_eq!(failure.to_string(), "worker exited");
    drop(old_response);
    let (instance, unknown) = manager.unknown_responses();
    assert_eq!(unknown.len(), 1);
    assert_eq!(unknown[0].0, 0);
    let recovered = bounded(manager.describe_workers()).await;
    assert_ne!(recovered, before);
    assert!(recovered.contains("crash-once / PID:"));
    assert!(!update.0.is_finished());

    release(&candidate);
    assert_eq!(
        joined(&mut update).await.unwrap_err().to_string(),
        "unresolved response outcomes prevent worker update"
    );
    assert_eq!(bounded(manager.describe_workers()).await, recovered);
    assert_eq!(manager.unknown_responses().1, unknown);
    assert!(!events(&candidate).contains(&"restore".into()));

    let (worker, operation) = unknown[0];
    manager
        .reconcile_response(&instance, worker, operation, "fixture delivery confirmed")
        .unwrap();
    bounded(manager.replace(candidate, false)).await.unwrap();
    wait_active(&manager, "ready-gated").await;
    assert!(manager.unknown_responses().1.is_empty());
    // The lost result cannot advance the confirmed checkpoint or be replayed.
    count(&manager, 1).await;
    drop(update);
    drop(manager);
    scenario.cleanup().await;
}
