use std::{
    path::{Path, PathBuf},
    process::Stdio,
    time::Duration,
};
use tokio::process::Command;

const DEADLINE: Duration = Duration::from_secs(30);

struct ReleaseOnDrop(PathBuf);

impl Drop for ReleaseOnDrop {
    fn drop(&mut self) {
        // The test child must also terminate if an assertion aborts the scenario.
        let _ = std::fs::write(&self.0, b"release");
    }
}

fn runtime_directory(label: &str) -> PathBuf {
    let directory = std::env::temp_dir().join(format!(
        "{label}-{}-{:016x}",
        std::process::id(),
        getrandom::u64().unwrap()
    ));
    std::fs::create_dir(&directory).unwrap();
    directory
}

async fn ready_port(runtime: &Path, child: &mut tokio::process::Child) -> u16 {
    tokio::time::timeout(DEADLINE, async {
        loop {
            assert!(
                child.try_wait().unwrap().is_none(),
                "management core exited before readiness"
            );
            let endpoint = |name| {
                std::fs::read(runtime.join(name))
                    .ok()
                    .and_then(|bytes| serde_json::from_slice::<serde_json::Value>(&bytes).ok())
                    .and_then(|value| value["port"].as_u64())
                    .and_then(|port| u16::try_from(port).ok())
                    .filter(|port| *port != 0)
            };
            if let Some(port) = endpoint("core.json")
                && endpoint("viewer.json") == Some(port)
            {
                return port;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("management endpoint readiness deadline")
}

async fn wait_status(runtime: &Path, port: u16, predicate: impl Fn(&str) -> bool) -> String {
    tokio::time::timeout(DEADLINE, async {
        loop {
            let output = cli(runtime, port, "status").await;
            let status = String::from_utf8(output.stdout).unwrap();
            if output.status.success() && predicate(&status) {
                return status;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("management status condition deadline")
}

fn bot_pid(status: &str) -> Option<u32> {
    status.lines().find_map(|line| {
        line.strip_prefix("Botプロセス: 稼働中 (PID: ")?
            .strip_suffix(')')?
            .parse()
            .ok()
    })
}

async fn remove_runtime(runtime: &Path) {
    tokio::time::timeout(DEADLINE, async {
        loop {
            match std::fs::remove_dir_all(runtime) {
                Ok(()) => return,
                Err(error) if error.kind() == std::io::ErrorKind::PermissionDenied => {
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
                Err(error) => panic!("fixture cleanup failed: {:?}", error.kind()),
            }
        }
    })
    .await
    .expect("fixture cleanup deadline");
}

async fn raw_request(port: u16, request: serde_json::Value) -> Vec<u8> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut stream = tokio::net::TcpStream::connect(("127.0.0.1", port))
        .await
        .unwrap();
    let mut frame = serde_json::to_vec(&request).unwrap();
    frame.push(b'\n');
    stream.write_all(&frame).await.unwrap();
    let mut response = Vec::new();
    tokio::time::timeout(DEADLINE, stream.read_to_end(&mut response))
        .await
        .unwrap()
        .unwrap();
    response
}

async fn cli(runtime: &std::path::Path, port: u16, command: &str) -> std::process::Output {
    tokio::time::timeout(
        DEADLINE,
        Command::new(env!("CARGO_BIN_EXE_manage"))
            .arg(command)
            .env("DISCORD_CORE_DIR", runtime)
            .env("DISCORD_CORE_PORT", port.to_string())
            .kill_on_drop(true)
            .output(),
    )
    .await
    .expect("management command deadline")
    .unwrap()
}

#[tokio::test]
async fn independent_commands_manage_a_persistent_core() {
    let runtime = runtime_directory("core-test");
    let _fixture_exit = ReleaseOnDrop(runtime.join("fixture-exit"));
    let fixture_source = runtime.join("fixture.rs");
    let fixture_binary = runtime.join(format!("fixture{}", std::env::consts::EXE_SUFFIX));
    std::fs::write(
        &fixture_source,
        "fn main() { let root = std::path::PathBuf::from(std::env::var_os(\"FIXTURE_ROOT\").unwrap()); let p = root.join(format!(\"fail-{}\", std::process::id())); while !p.exists() && !root.join(\"fixture-exit\").exists() { std::thread::sleep(std::time::Duration::from_millis(10)); } std::process::exit(17); }",
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
    let mut core = Command::new(env!("CARGO_BIN_EXE_manage"))
        .arg("__core")
        .arg(&fixture_binary)
        .env("DISCORD_CORE_DIR", &runtime)
        .env("DISCORD_CORE_PORT", "0")
        .env("FIXTURE_ROOT", &runtime)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let port = ready_port(&runtime, &mut core).await;
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
        let _ = tokio::time::timeout(DEADLINE, stream.read_to_end(&mut response))
            .await
            .unwrap();
        assert!(response.is_empty());
    }
    // An unauthenticated client that never sends a frame must not block
    // authenticated requests for the read timeout.
    // The viewer capability is a separate credential, not a client-selected role.
    let viewer: serde_json::Value =
        serde_json::from_slice(&std::fs::read(runtime.join("viewer.json")).unwrap()).unwrap();
    let admin: serde_json::Value =
        serde_json::from_slice(&std::fs::read(runtime.join("core.json")).unwrap()).unwrap();
    let mut idle = tokio::net::TcpStream::connect(("127.0.0.1", port))
        .await
        .unwrap();
    let mut byte = [0; 1];
    let response = tokio::select! {
        biased;
        closed = tokio::io::AsyncReadExt::read(&mut idle, &mut byte) => {
            panic!("idle client closed before authenticated status completed: {:?}", closed.map_err(|e| e.kind()));
        }
        response = raw_request(port, serde_json::json!({"secret":admin["secret"],"command":"status"})) => response,
    };
    let response: serde_json::Value = serde_json::from_slice(&response).unwrap();
    assert_eq!(response["ok"], true);
    assert!(
        response["message"]
            .as_str()
            .unwrap()
            .contains("Botプロセス: 稼働中")
    );
    drop(idle);
    assert_ne!(viewer["secret"], admin["secret"]);
    let viewer_request =
        |command: &str| serde_json::json!({"secret": viewer["secret"], "command": command});
    let response: serde_json::Value =
        serde_json::from_slice(&raw_request(port, viewer_request("status")).await).unwrap();
    assert_eq!(response["ok"], true);
    let response: serde_json::Value =
        serde_json::from_slice(&raw_request(port, viewer_request("stop")).await).unwrap();
    assert_eq!(response["ok"], false);
    let spoof = serde_json::json!({"secret": viewer["secret"], "command": "stop", "role": "admin"});
    let response = raw_request(port, spoof).await;
    assert!(
        response.is_empty()
            || serde_json::from_slice::<serde_json::Value>(&response).unwrap()["ok"] == false
    );
    assert!(
        raw_request(
            port,
            serde_json::json!({"secret": "invalid-credential", "command": "status"})
        )
        .await
        .is_empty()
    );
    let status = cli(&runtime, port, "status").await;
    assert!(String::from_utf8_lossy(&status.stdout).contains("Botプロセス: 稼働中"));

    let metrics = cli(&runtime, port, "metrics-json").await;
    assert!(metrics.status.success());
    let snapshot: serde_json::Value = serde_json::from_slice(&metrics.stdout).unwrap();
    assert!(snapshot.is_array());
    let metrics = cli(&runtime, port, "metrics-prometheus").await;
    assert!(metrics.status.success());
    assert!(String::from_utf8_lossy(&metrics.stdout).contains("# TYPE met_events_total counter"));

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
    // Observe a reader being rejected or timing out before releasing the pool.
    // Sleeping for 3.3 seconds assumed all readers had already been scheduled.
    let mut byte = [0; 1];
    let closed = tokio::time::timeout(
        DEADLINE,
        tokio::io::AsyncReadExt::read(idle_clients.last_mut().unwrap(), &mut byte),
    )
    .await
    .expect("exhausted reader pool did not reject or expire a connection");
    assert!(matches!(closed, Ok(0) | Err(_)));
    drop(idle_clients);
    wait_status(&runtime, port, |_| true).await;
    assert!(cli(&runtime, port, "stop").await.status.success());
    let output = cli(&runtime, port, "status").await;
    assert!(String::from_utf8_lossy(&output.stdout).contains("Botプロセス: 停止中"));
    assert!(cli(&runtime, port, "start").await.status.success());
    assert!(cli(&runtime, port, "restart").await.status.success());
    // Release one exact process at a time. Fixed sleeps cannot reliably observe
    // the transient 1/3 or 2/3 state when all replacement processes exit at once.
    let mut pid = bot_pid(&wait_status(&runtime, port, |s| bot_pid(s).is_some()).await).unwrap();
    let idle = tokio::net::TcpStream::connect(("127.0.0.1", port))
        .await
        .unwrap();
    for attempt in 1..=3 {
        std::fs::write(runtime.join(format!("fail-{pid}")), "fail").unwrap();
        let status = wait_status(&runtime, port, |status| {
            status.contains(&format!("自動再起動: {attempt}/3"))
                && bot_pid(status).is_some_and(|next| next != pid)
        })
        .await;
        pid = bot_pid(&status).unwrap();
    }
    std::fs::write(runtime.join(format!("fail-{pid}")), "fail").unwrap();
    drop(idle);
    wait_status(&runtime, port, |text| {
        text.contains("Botプロセス: 停止中")
            && text.contains("自動再起動: 3/3")
            && text.contains("直近のエラー: Botが終了しました")
    })
    .await;
    assert!(cli(&runtime, port, "shutdown").await.status.success());
    assert!(
        tokio::time::timeout(DEADLINE, core.wait())
            .await
            .unwrap()
            .unwrap()
            .success()
    );
    let audit = std::fs::read_to_string(runtime.join("audit.jsonl")).unwrap();
    let records: Vec<serde_json::Value> = audit
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    assert!(
        records
            .iter()
            .any(|record| record["result"] == "denied" && record["reason"] == "unauthenticated")
    );
    assert!(records.iter().any(|record| record["role"] == "viewer"
        && record["result"] == "denied"
        && record["reason"] == "insufficient_permission"));
    assert!(!audit.contains(viewer["secret"].as_str().unwrap()));
    assert!(!audit.contains(admin["secret"].as_str().unwrap()));
    assert!(!runtime.join("core.json").exists());
    remove_runtime(&runtime).await;
}

#[cfg(unix)]
#[tokio::test]
async fn audit_write_failure_rejects_stop_before_the_side_effect() {
    let runtime = runtime_directory("audit-failure-test");
    let _fixture_exit = ReleaseOnDrop(runtime.join("release"));
    let source = runtime.join("fixture.rs");
    let binary = runtime.join(format!("fixture{}", std::env::consts::EXE_SUFFIX));
    std::fs::write(
        &source,
        r#"fn main() {
        while !std::path::Path::new(&std::env::var_os("FIXTURE_RELEASE").unwrap()).exists() {
            std::thread::sleep(std::time::Duration::from_millis(10));
        }
        std::fs::write(std::env::var_os("FIXTURE_MARKER").unwrap(), b"finished").unwrap();
    }"#,
    )
    .unwrap();
    assert!(
        Command::new("rustc")
            .arg(&source)
            .arg("-o")
            .arg(&binary)
            .status()
            .await
            .unwrap()
            .success()
    );
    let mut core = Command::new(env!("CARGO_BIN_EXE_manage"))
        .arg("__core")
        .arg(&binary)
        .env("DISCORD_CORE_DIR", &runtime)
        .env("DISCORD_CORE_PORT", "0")
        .env("FIXTURE_MARKER", runtime.join("finished"))
        .env("FIXTURE_RELEASE", runtime.join("release"))
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let port = ready_port(&runtime, &mut core).await;
    assert!(cli(&runtime, port, "status").await.status.success());
    // A non-cooperating write changes the file size, which the audit writer
    // detects before recording the next operation and executing its closure.
    use std::io::Write;
    std::fs::OpenOptions::new()
        .append(true)
        .open(runtime.join("audit.jsonl"))
        .unwrap()
        .write_all(b"corrupt")
        .unwrap();
    assert!(!cli(&runtime, port, "stop").await.status.success());
    std::fs::write(runtime.join("release"), b"release").unwrap();
    tokio::time::timeout(DEADLINE, async {
        while !runtime.join("finished").exists() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("stop ran despite audit failure");
    core.kill().await.unwrap();
    let _ = core.wait().await;
    remove_runtime(&runtime).await;
}
