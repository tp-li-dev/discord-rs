use std::{net::TcpListener, path::PathBuf, process::Stdio, time::Duration};
use tokio::process::Command;

async fn cli(runtime: &std::path::Path, port: u16, command: &str) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_manage"))
        .arg(command)
        .env("DISCORD_CORE_DIR", runtime)
        .env("DISCORD_CORE_PORT", port.to_string())
        .output()
        .await
        .unwrap()
}

#[tokio::test]
async fn independent_commands_manage_a_persistent_core() {
    let runtime = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("target-codex-verify")
        .join(format!("core-test-{}", std::process::id()));
    std::fs::create_dir_all(&runtime).unwrap();
    let fixture_source = runtime.join("fixture.rs");
    let fixture_binary = runtime.join(format!("fixture{}", std::env::consts::EXE_SUFFIX));
    std::fs::write(
        &fixture_source,
        "fn main() { let p = std::env::var_os(\"FIXTURE_FAILURE\").unwrap(); while !std::path::Path::new(&p).exists() { std::thread::sleep(std::time::Duration::from_millis(50)); } std::process::exit(17); }",
    )
    .unwrap();
    let status = Command::new("rustc")
        .arg(&fixture_source)
        .arg("-o")
        .arg(&fixture_binary)
        .status()
        .await
        .unwrap();
    assert!(status.success());
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);
    let mut core = Command::new(env!("CARGO_BIN_EXE_manage"))
        .arg("__core")
        .arg(&fixture_binary)
        .env("DISCORD_CORE_DIR", &runtime)
        .env("DISCORD_CORE_PORT", port.to_string())
        .env("FIXTURE_FAILURE", runtime.join("fail"))
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    for _ in 0..100 {
        if runtime.join("core.json").exists() {
            break;
        }

        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let output = cli(&runtime, port, "status").await;
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(String::from_utf8_lossy(&output.stdout).contains("Botプロセス: 稼働中"));
    assert!(cli(&runtime, port, "stop").await.status.success());
    let output = cli(&runtime, port, "status").await;
    assert!(String::from_utf8_lossy(&output.stdout).contains("Botプロセス: 停止中"));
    assert!(cli(&runtime, port, "start").await.status.success());
    assert!(cli(&runtime, port, "restart").await.status.success());
    std::fs::write(runtime.join("fail"), "fail").unwrap();
    let mut saw_failure = false;
    for _ in 0..30 {
        tokio::time::sleep(Duration::from_millis(500)).await;
        let output = cli(&runtime, port, "status").await;
        let text = String::from_utf8_lossy(&output.stdout);
        if text.contains("Botプロセス: 停止中")
            && text.contains("自動再起動: 3/3")
            && text.contains("直近のエラー: Botが終了しました")
        {
            saw_failure = true;
            break;
        }
    }
    assert!(
        saw_failure,
        "failed process was not reported after bounded restarts"
    );
    assert!(cli(&runtime, port, "shutdown").await.status.success());
    assert!(
        tokio::time::timeout(Duration::from_secs(5), core.wait())
            .await
            .unwrap()
            .unwrap()
            .success()
    );
    assert!(!runtime.join("core.json").exists());
    for _ in 0..40 {
        if std::fs::remove_dir_all(&runtime).is_ok() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("fixture cleanup failed");
}

#[tokio::test]
async fn botctl_accepts_target_syntax_and_exposes_new_commands() {
    let help = Command::new(env!("CARGO_BIN_EXE_botctl"))
        .output()
        .await
        .unwrap();
    assert!(help.status.success());
    let help_text = String::from_utf8_lossy(&help.stdout);
    assert!(help_text.contains("rollback"));
    assert!(help_text.contains("doctor"));

    let runtime = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("target-codex-verify")
        .join(format!("botctl-cli-test-{}", std::process::id()));
    std::fs::create_dir_all(&runtime).unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);

    let doctor = Command::new(env!("CARGO_BIN_EXE_botctl"))
        .arg("doctor")
        .arg("mybot")
        .env("DISCORD_CORE_DIR", &runtime)
        .env("DISCORD_CORE_PORT", port.to_string())
        .output()
        .await
        .unwrap();
    assert!(!doctor.status.success());
    assert!(!String::from_utf8_lossy(&doctor.stderr).contains("引数が多すぎます"));
    std::fs::remove_dir_all(runtime).unwrap();
}
