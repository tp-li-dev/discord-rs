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
    // Missing/wrong credentials and malformed frames yield no response.
    for frame in [
        b"{}\n".to_vec(),
        b"{\"secret\":\"wrong\",\"command\":\"list-unknown\"}\n".to_vec(),
        vec![b'x'; 5000],
        b"partial".to_vec(),
    ] {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let mut stream = tokio::net::TcpStream::connect(("127.0.0.1", port))
            .await
            .unwrap();
        stream.write_all(&frame).await.unwrap();
        stream.shutdown().await.unwrap();
        let mut response = Vec::new();
        let _ = tokio::time::timeout(Duration::from_secs(4), stream.read_to_end(&mut response))
            .await
            .unwrap();
        assert!(response.is_empty());
    }
    // An unauthenticated client that never sends a frame must not block
    // authenticated requests for the read timeout.
    let idle = tokio::net::TcpStream::connect(("127.0.0.1", port))
        .await
        .unwrap();
    let output = tokio::time::timeout(Duration::from_millis(1500), cli(&runtime, port, "status"))
        .await
        .expect("idle client blocked management");
    drop(idle);
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(String::from_utf8_lossy(&output.stdout).contains("Botプロセス: 稼働中"));
    let unavailable = cli(&runtime, port, "list-unknown").await;
    assert!(!unavailable.status.success());
    assert!(String::from_utf8_lossy(&unavailable.stderr).contains("Gateway unavailable"));
    // Exhaust the bounded unauthenticated reader pool, then verify recovery.
    let mut idle_clients = Vec::new();
    for _ in 0..40 {
        idle_clients.push(
            tokio::net::TcpStream::connect(("127.0.0.1", port))
                .await
                .unwrap(),
        );
    }
    tokio::time::sleep(Duration::from_millis(3300)).await;
    drop(idle_clients);
    assert!(cli(&runtime, port, "status").await.status.success());
    assert!(cli(&runtime, port, "stop").await.status.success());
    let output = cli(&runtime, port, "status").await;
    assert!(String::from_utf8_lossy(&output.stdout).contains("Botプロセス: 停止中"));
    assert!(cli(&runtime, port, "start").await.status.success());
    assert!(cli(&runtime, port, "restart").await.status.success());
    std::fs::write(runtime.join("fail"), "fail").unwrap();
    let idle = tokio::net::TcpStream::connect(("127.0.0.1", port))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(2500)).await;
    let output = tokio::time::timeout(Duration::from_millis(1000), cli(&runtime, port, "status"))
        .await
        .expect("idle client blocked supervision");
    let status = String::from_utf8_lossy(&output.stdout);
    assert!(
        status.contains("自動再起動: 1/3") || status.contains("自動再起動: 2/3"),
        "{status}"
    );
    drop(idle);
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
