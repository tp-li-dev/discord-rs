//! Cargo-built fixture: use its dependency graph and JSON artifact path.
use std::{
    path::{Path, PathBuf},
    process::Command,
    sync::OnceLock,
};

#[path = "fixture_manifest.rs"]
mod fixture_manifest;

pub fn artifact_root() -> PathBuf {
    std::env::current_exe()
        .expect("integration test executable")
        .parent()
        .and_then(Path::parent)
        .expect("test profile directory")
        .join("interaction-fixtures")
}

pub fn unique_directory(label: &str) -> PathBuf {
    let root = artifact_root();
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
            Err(error) => panic!("create fixture directory: {error}"),
        }
    }
}

pub async fn binary() -> &'static Path {
    tokio::task::spawn_blocking(|| {
        static BINARY: OnceLock<PathBuf> = OnceLock::new();
        BINARY.get_or_init(build).as_path()
    })
    .await
    .expect("fixture build task")
}

fn build() -> PathBuf {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let project = unique_directory("project");
    let name = format!(
        "interaction-worker-{}-{:016x}",
        std::process::id(),
        getrandom::u64().unwrap()
    );
    let manifest_path = fixture_manifest::write_fixture_project(
        &project,
        "interaction-worker-fixture",
        &name,
        "tests/fixtures/interaction_worker.rs",
    );
    let target = artifact_root()
        .parent()
        .and_then(Path::parent)
        .unwrap()
        .to_path_buf();
    let result = Command::new(env!("CARGO"))
        .current_dir(root)
        .args(["build", "--offline", "--message-format=json", "--bin"])
        .arg(&name)
        .arg("--manifest-path")
        .arg(&manifest_path)
        .arg("--target-dir")
        .arg(&target)
        .env("CARGO_NET_OFFLINE", "true")
        .env_remove("DISCORD_TOKEN")
        .output()
        .expect("build interaction worker fixture");
    assert!(
        result.status.success(),
        "fixture build failed: {}\n{}",
        String::from_utf8_lossy(&result.stdout),
        String::from_utf8_lossy(&result.stderr)
    );
    let mut executable = None;
    for line in result
        .stdout
        .split(|byte| *byte == b'\n')
        .filter(|line| !line.is_empty())
    {
        let message: serde_json::Value = serde_json::from_slice(line).unwrap();
        if message["reason"] == "compiler-artifact"
            && message["target"]["name"] == name
            && let Some(path) = message["executable"].as_str()
        {
            assert!(
                executable.replace(PathBuf::from(path)).is_none(),
                "duplicate fixture artifact"
            );
        }
    }
    let executable = executable.expect("fixture Cargo artifact");
    assert!(executable.is_file());
    executable
}
