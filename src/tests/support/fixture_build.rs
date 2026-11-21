//! Build the application fixture with Cargo's dependency graph, never by
//! guessing which of the rlibs left by check/test/rebuild belongs together.
use std::{
    path::{Path, PathBuf},
    process::Command,
    sync::OnceLock,
};

#[path = "fixture_manifest.rs"]
mod fixture_manifest;

/// Keep all generated files beside the running test's build artifacts. This
/// also works with a custom CARGO_TARGET_DIR and a target-triple subdirectory.
pub fn artifact_root() -> PathBuf {
    let executable = std::env::current_exe().expect("locate integration test executable");
    let profile = executable
        .parent()
        .and_then(Path::parent)
        .expect("integration test must live in a profile/deps directory");
    profile.join("fixture-support")
}

pub fn unique_directory(label: &str) -> PathBuf {
    let root = artifact_root();
    std::fs::create_dir_all(&root).expect("create fixture artifact root");
    loop {
        let path = root.join(format!(
            "{label}-{}-{:016x}",
            std::process::id(),
            getrandom::u64().expect("fixture directory nonce")
        ));
        match std::fs::create_dir(&path) {
            Ok(()) => return path,
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(error) => panic!("create unique fixture directory: {error}"),
        }
    }
}

/// Run both the compile and any concurrent OnceLock wait off Tokio's executor.
/// Call during setup, before a Worker or a scenario deadline is started.
pub async fn application_worker_binary() -> &'static Path {
    tokio::task::spawn_blocking(|| {
        static BINARY: OnceLock<PathBuf> = OnceLock::new();
        BINARY.get_or_init(build_application_worker).as_path()
    })
    .await
    .expect("application fixture build task panicked")
}

fn build_application_worker() -> PathBuf {
    let source_root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let project = unique_directory("application-project");
    // Unique bin names prevent independent test processes from overwriting a
    // fixture another process is executing (which also fails on Windows).
    let binary_name = format!(
        "hot-swap-handler-{}-{:016x}",
        std::process::id(),
        getrandom::u64().expect("fixture binary nonce")
    );
    let manifest_path = fixture_manifest::write_fixture_project(
        &project,
        "hot-swap-application-fixture",
        &binary_name,
        "tests/fixtures/hot_swap_handler.rs",
    );
    let target = artifact_root()
        .parent()
        .and_then(Path::parent)
        .expect("fixture build output root")
        .to_path_buf();
    let result = Command::new(env!("CARGO"))
        .current_dir(source_root)
        .args(["build", "--offline", "--message-format=json", "--bin"])
        .arg(&binary_name)
        .arg("--manifest-path")
        .arg(&manifest_path)
        .arg("--target-dir")
        .arg(&target)
        .env("CARGO_NET_OFFLINE", "true")
        .env_remove("DISCORD_TOKEN")
        .output()
        .expect("build application fixture with Cargo");
    assert!(
        result.status.success(),
        "application fixture build failed ({status}):\n{stdout}\n{stderr}",
        status = result.status,
        stdout = String::from_utf8_lossy(&result.stdout),
        stderr = String::from_utf8_lossy(&result.stderr),
    );
    let binary = executable_from_messages(&result.stdout, &binary_name)
        .expect("Cargo must identify exactly one application fixture executable");
    assert!(binary.is_file(), "Cargo fixture executable is missing");
    binary
}

fn executable_from_messages(output: &[u8], name: &str) -> Result<PathBuf, &'static str> {
    let mut executable = None;
    for line in output.split(|byte| *byte == b'\n') {
        if line.is_empty() {
            continue;
        }
        let message: serde_json::Value =
            serde_json::from_slice(line).map_err(|_| "invalid Cargo JSON output")?;
        if message["reason"] != "compiler-artifact" || message["target"]["name"] != name {
            continue;
        }
        let Some(path) = message["executable"].as_str() else {
            continue;
        };
        if executable.replace(PathBuf::from(path)).is_some() {
            return Err("duplicate fixture executable in Cargo output");
        }
    }
    executable.ok_or("fixture executable absent from Cargo output")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn artifact_selection_uses_the_requested_bin_not_file_order_or_timestamps() {
        let messages = concat!(
            "{\"reason\":\"compiler-artifact\",\"target\":{\"name\":\"fixture\"},\"executable\":null}\n",
            "{\"reason\":\"compiler-artifact\",\"target\":{\"name\":\"other\"},\"executable\":\"stale.exe\"}\n",
            "{\"reason\":\"compiler-artifact\",\"target\":{\"name\":\"fixture\"},\"executable\":\"correct.exe\"}\n",
            "{\"reason\":\"build-finished\",\"success\":true}\n",
        );
        assert_eq!(
            executable_from_messages(messages.as_bytes(), "fixture").unwrap(),
            PathBuf::from("correct.exe")
        );
        assert!(executable_from_messages(messages.as_bytes(), "missing").is_err());
        let duplicate = format!("{messages}{messages}");
        assert!(executable_from_messages(duplicate.as_bytes(), "fixture").is_err());
        assert!(executable_from_messages(b"not JSON\n", "fixture").is_err());
    }
}
