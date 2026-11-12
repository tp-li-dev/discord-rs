//! Regressions for stale rlib selection and simultaneous fixture preparation.
#[path = "support/fixture_build.rs"]
mod fixture_build;

use src::worker::{DeploymentIdentity, Manager};
use std::{
    fs::{FileTimes, OpenOptions},
    io::Write,
    path::PathBuf,
    process::Stdio,
    sync::Arc,
    time::{Duration, SystemTime},
};

struct UnrelatedLibraries(Vec<PathBuf>);
impl Drop for UnrelatedLibraries {
    fn drop(&mut self) {
        for path in &self.0 {
            let _ = std::fs::remove_file(path);
        }
    }
}

struct RunDirectory(PathBuf);
impl Drop for RunDirectory {
    fn drop(&mut self) {
        // Only this exclusively created test directory is removed. The Cargo
        // artifacts are outside it and may still be in use by another test.
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

async fn verify_worker(binary: PathBuf) {
    // Validate the artifact, not just the path: it must complete the production
    // Worker handshake and execute a request with the matching dependency graph.
    tokio::time::timeout(Duration::from_secs(30), async {
        let identity = DeploymentIdentity::parse(|_| Ok(None)).unwrap();
        let manager = Manager::start_pool_with_identity(binary, 1, identity)
            .await
            .unwrap();
        assert_eq!(
            manager.dispatch("!count".into()).await.unwrap(),
            Some("カウント: 1".into())
        );
        manager.shutdown().await.unwrap();
    })
    .await
    .expect("compiled fixture request deadline");
}

#[tokio::test]
async fn fixture_preparation_subprocess() {
    let Some(directory) = std::env::var_os("FIXTURE_BUILD_CHILD_DIR") else {
        return;
    };
    let directory = PathBuf::from(directory);
    let gate = PathBuf::from(std::env::var_os("FIXTURE_BUILD_START").unwrap());
    std::fs::write(directory.join("ready"), b"ready").unwrap();
    tokio::time::timeout(Duration::from_secs(30), async {
        while !gate.is_file() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("parent must release the fixture build gate");
    let binary = fixture_build::application_worker_binary().await.to_owned();
    verify_worker(binary.clone()).await;
    std::fs::write(
        directory.join("result.json"),
        serde_json::to_vec(&binary).unwrap(),
    )
    .unwrap();
}

#[tokio::test]
async fn concurrent_fixture_preparation_ignores_newer_unrelated_rlibs() {
    let test = std::env::current_exe().unwrap();
    let dependencies = test.parent().unwrap();
    let mut unrelated = UnrelatedLibraries(Vec::new());
    for name in ["src", "bot_framework", "tokio"] {
        let path = dependencies.join(format!(
            "lib{name}-unrelated-{:016x}.rlib",
            getrandom::u64().unwrap()
        ));
        let mut file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&path)
            .unwrap();
        unrelated.0.push(path);
        file.write_all(b"deliberately not a Rust library").unwrap();
        file.set_times(
            FileTimes::new().set_modified(SystemTime::now() + Duration::from_secs(86_400)),
        )
        .unwrap();
    }

    // Independent processes have different OnceLocks and manifests, but use
    // the same Cargo artifact cache. They must never publish/overwrite the
    // same executable, deadlock on Cargo's build lock or pick unrelated rlibs.
    let run = RunDirectory(fixture_build::unique_directory("concurrent-preparation"));
    let gate = run.0.join("start");
    let mut subprocesses = tokio::task::JoinSet::new();
    let mut child_directories = Vec::new();
    for index in 0..2 {
        let directory = run.0.join(format!("child-{index}"));
        std::fs::create_dir(&directory).unwrap();
        let child = tokio::process::Command::new(&test)
            .args([
                "--exact",
                "fixture_preparation_subprocess",
                "--nocapture",
                "--test-threads=1",
            ])
            .env("FIXTURE_BUILD_CHILD_DIR", &directory)
            .env("FIXTURE_BUILD_START", &gate)
            .env_remove("DISCORD_TOKEN")
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true)
            .spawn()
            .unwrap();
        subprocesses.spawn(child.wait_with_output());
        child_directories.push(directory);
    }
    tokio::time::timeout(Duration::from_secs(30), async {
        while child_directories
            .iter()
            .any(|directory| !directory.join("ready").is_file())
        {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("fixture build subprocesses must become ready");

    let barrier = Arc::new(tokio::sync::Barrier::new(8));
    let mut preparations = tokio::task::JoinSet::new();
    for _ in 0..8 {
        let barrier = barrier.clone();
        preparations.spawn(async move {
            barrier.wait().await;
            fixture_build::application_worker_binary().await.to_owned()
        });
    }
    std::fs::write(&gate, b"start").unwrap();
    let binary = tokio::time::timeout(Duration::from_secs(300), async {
        let first = preparations.join_next().await.unwrap().unwrap();
        assert!(first.is_file(), "cache published an unfinished executable");
        while let Some(result) = preparations.join_next().await {
            assert_eq!(result.unwrap(), first, "fixture was built more than once");
        }
        while let Some(result) = subprocesses.join_next().await {
            let output = result.unwrap().unwrap();
            assert!(
                output.status.success(),
                "fixture subprocess failed: {}\n{}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
        }
        first
    })
    .await
    .expect("fixture preparation deadline");
    assert!(binary.starts_with(dependencies.parent().unwrap().parent().unwrap()));
    let mut all_binaries = std::collections::HashSet::from([binary.clone()]);
    for directory in child_directories {
        let child_binary: PathBuf =
            serde_json::from_slice(&std::fs::read(directory.join("result.json")).unwrap()).unwrap();
        assert!(child_binary.is_file());
        assert!(child_binary.starts_with(dependencies.parent().unwrap().parent().unwrap()));
        assert!(
            all_binaries.insert(child_binary),
            "independent processes must not overwrite a shared fixture executable"
        );
    }
    verify_worker(binary).await;
}
