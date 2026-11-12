//! Binary-level guards run before cluster sockets, store creation or child launch.
use std::{path::PathBuf, process::Stdio, time::Duration};
use tokio::{io::AsyncWriteExt, process::Command};

fn command(root: &std::path::Path) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_botd"));
    command
        .args(["--file"])
        .arg(root.join("config.toml"))
        .arg("--runtime-root")
        .arg(root.join("runtime"))
        .arg("--gateway-program")
        .arg(PathBuf::from(env!("CARGO_BIN_EXE_src")))
        .args(["--bootstrap-stdin", "--raft-store"])
        .arg(root.join("store"))
        .args([
            "--raft-rpc-listen",
            "127.0.0.1:1201",
            "--raft-rpc-peer",
            "2=127.0.0.1:1202",
            "--raft-rpc-peer",
            "3=127.0.0.1:1203",
            "--raft-create",
            "--raft-bootstrap",
        ])
        .env_clear()
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    #[cfg(windows)]
    {
        command.env("SystemRoot", std::env::var_os("SystemRoot").unwrap());
        command.creation_flags(0x0800_0000);
    }
    command
}

fn root() -> PathBuf {
    std::env::current_dir()
        .unwrap()
        .join("target")
        .join(format!(
            "raft-botd-guard-{}-{}",
            std::process::id(),
            getrandom::u64().unwrap()
        ))
}

#[tokio::test]
async fn real_binary_rejects_implicit_raft_real_discord_and_competing_modes_before_io() {
    let root = root();
    // No files exist, so these exact diagnostics also demonstrate that the
    // parser rejects unsafe mode selection before configuration/bootstrap I/O.
    for (args, diagnostic) in [
        (
            vec!["--mock-discord", "127.0.0.1:1234"],
            "Raft settings require --allow-raft-quorum",
        ),
        (
            vec!["--allow-raft-quorum"],
            "Raft quorum requires --mock-discord; production is disabled",
        ),
        (
            vec![
                "--allow-raft-quorum",
                "--mock-discord",
                "127.0.0.1:1234",
                "--allow-test-quorum",
            ],
            "Raft quorum requires exclusive automatic process ownership",
        ),
        (
            vec![
                "--allow-raft-quorum",
                "--mock-discord",
                "127.0.0.1:1234",
                "--allow-test-gateway-switch",
            ],
            "Raft quorum requires exclusive automatic process ownership",
        ),
    ] {
        let output =
            tokio::time::timeout(Duration::from_secs(10), command(&root).args(args).output())
                .await
                .unwrap()
                .unwrap();
        assert!(!output.status.success());
        assert!(output.stdout.is_empty());
        assert_eq!(
            String::from_utf8(output.stderr).unwrap().trim(),
            format!("botd: {diagnostic}")
        );
        assert!(!root.exists());
    }
}

#[tokio::test]
async fn raft_store_creation_requires_valid_full_admin_bootstrap() {
    let root = root();
    std::fs::create_dir_all(&root).unwrap();
    std::fs::write(
        root.join("config.toml"),
        "[cluster]\nid = 'guard'\nmode = 'quorum'\n[node]\nid = 'node-a'\n",
    )
    .unwrap();
    // Cluster configuration has no TLS material; neither should be consulted
    // before the anonymous local capability and its role have been checked.
    let invalid = b"{}\n".to_vec();
    let mut viewer = serde_json::json!({
        "secret": vec![7; 32],
        "access": {"subject":"fixture-viewer", "role":"viewer"}
    })
    .to_string()
    .into_bytes();
    viewer.push(b'\n');
    for (frame, diagnostic) in [
        (invalid, "invalid anonymous control bootstrap"),
        (
            viewer,
            "Raft cluster requires a full local administrator capability",
        ),
    ] {
        let mut child = command(&root)
            .args(["--allow-raft-quorum", "--mock-discord", "127.0.0.1:1234"])
            .spawn()
            .unwrap();
        let mut input = child.stdin.take().unwrap();
        input.write_all(&frame).await.unwrap();
        drop(input);
        let output = tokio::time::timeout(Duration::from_secs(10), child.wait_with_output())
            .await
            .unwrap()
            .unwrap();
        assert!(!output.status.success());
        assert!(output.stdout.is_empty());
        assert_eq!(
            String::from_utf8(output.stderr).unwrap().trim(),
            format!("botd: {diagnostic}")
        );
        assert!(!root.join("store").exists());
        assert!(!root.join("runtime").exists());
    }
    std::fs::remove_dir_all(root).unwrap();
}
