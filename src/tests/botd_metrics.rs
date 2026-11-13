//! Real botd process in the explicit loopback mock-Discord mode: daemon-owned
//! measurements (Identify admission wait, drain duration, runtime role) must
//! reach `botctl status` and the Prometheus text. Not a live-Discord claim.
use serde_json::{Value, json};
use std::{
    path::{Path, PathBuf},
    process::{Command, Stdio},
    time::Duration,
};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    process::Child,
};

const SECRET: [u8; 32] = [29; 32];
const CONFIG: &str = r#"
[cluster]
id = "metrics-tests"
mode = "standalone"
[node]
id = "one"
[[bots]]
name = "alpha"
bot_id = "ab100-alpha"
application_id = "101"
token_env = "TOKEN_ALPHA"
total_shards = 1
[[bots.shard_placement]]
shard_id = 0
primary = "one"
"#;

async fn bounded<T>(future: impl std::future::Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(20), future)
        .await
        .expect("metrics process test deadline")
}

async fn cli(port: u16, args: &[&str]) -> Value {
    let mut command = tokio::process::Command::new(env!("CARGO_BIN_EXE_botctl"));
    command
        .args(args)
        .args(["--json", "--bootstrap-stdin", "--timeout", "10s"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    let mut child = command.spawn().unwrap();
    let mut bootstrap = json!({"port": port, "secret": SECRET.to_vec()}).to_string();
    bootstrap.push('\n');
    child
        .stdin
        .take()
        .unwrap()
        .write_all(bootstrap.as_bytes())
        .await
        .unwrap();
    let output = bounded(child.wait_with_output()).await.unwrap();
    let text = String::from_utf8(output.stdout).unwrap();
    assert!(output.status.success(), "{text}");
    serde_json::from_str(&text).unwrap()
}

#[tokio::test]
async fn botd_reports_real_identify_drain_and_runtime_values() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("target")
        .join("botd-metrics-test")
        .join(std::process::id().to_string());
    std::fs::create_dir_all(&root).unwrap();
    let gateway: PathBuf = root.join(format!("gateway{}", std::env::consts::EXE_SUFFIX));
    let built = Command::new("rustc")
        .args(["--edition=2024", "-Dwarnings"])
        .arg(Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/metrics_gateway.rs"))
        .arg("-o")
        .arg(&gateway)
        .output()
        .expect("compile metrics fixture");
    assert!(built.status.success());
    let file = root.join("placement.toml");
    std::fs::write(&file, CONFIG).unwrap();
    let mut botd: Child = tokio::process::Command::new(env!("CARGO_BIN_EXE_botd"))
        .arg("--file")
        .arg(&file)
        .arg("--runtime-root")
        .arg(&root)
        .arg("--gateway-program")
        .arg(&gateway)
        .args(["--bootstrap-stdin", "--no-autostart"])
        .args(["--mock-discord", "127.0.0.1:9"])
        .env_remove("TOKEN_ALPHA")
        .env_remove("DISCORD_TOKEN")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let mut bootstrap = json!({"secret": SECRET.to_vec()}).to_string();
    bootstrap.push('\n');
    botd.stdin
        .take()
        .unwrap()
        .write_all(bootstrap.as_bytes())
        .await
        .unwrap();
    let mut announcement = String::new();
    bounded(BufReader::new(botd.stdout.take().unwrap()).read_line(&mut announcement))
        .await
        .unwrap();
    let announced: Value = serde_json::from_str(&announcement).unwrap();
    assert_eq!(announced["mock_discord"], true);
    let port = u16::try_from(announced["port"].as_u64().unwrap()).unwrap();

    let idle = cli(port, &["status"]).await;
    let shard = &idle["status"]["metrics"]["shards"][0];
    assert_eq!(shard["bot_id"], "ab100-alpha");
    assert_eq!(shard["runtime"]["gateway"]["role"], "standby");
    assert!(shard["feed"]["drain_ns"].is_null());

    assert_eq!(cli(port, &["start", "alpha"]).await["ok"], true);
    let running = cli(port, &["status"]).await;
    let shard = &running["status"]["metrics"]["shards"][0];
    assert_eq!(shard["runtime"]["gateway"]["role"], "active");
    assert!(
        shard["runtime"]["gateway"]["generation_id"]
            .as_u64()
            .unwrap()
            >= 1
    );
    assert!(shard["feed"]["identify_wait_ns"].is_u64());

    assert_eq!(cli(port, &["stop", "alpha"]).await["ok"], true);
    let stopped = cli(port, &["status"]).await;
    let status = &stopped["status"];
    let shard = &status["metrics"]["shards"][0];
    assert!(
        shard["feed"]["drain_ns"].as_u64().unwrap() >= 100_000_000,
        "{shard}"
    );
    assert_eq!(shard["runtime"]["gateway"]["role"], "standby");
    assert_eq!(status["metrics"]["feed_dropped_total"], 0);
    let text = status["metrics_prometheus"].as_str().unwrap();
    let labels = "{bot_id=\"ab100-alpha\",shard_id=\"0\",generation_id=\"0\"}";
    for family in [
        "met_feed_drain_seconds",
        "met_feed_identify_wait_seconds",
        "met_feed_dropped_total",
    ] {
        assert!(text.contains(&format!("{family}{labels} ")), "{family}");
    }
    assert!(!text.contains("TOKEN") && !text.contains("secret"));
    let _ = botd.kill().await;
}
