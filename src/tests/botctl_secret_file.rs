//! 実プロセスの botd と botctl を `--secret-file` で接続する統合試験（systemd 常駐運用の経路）。
//! Gateway は起動しない（`--no-autostart`）。Discord 接続・実 systemd は検証しない。Linux/Unix のみ。
#![cfg(unix)]

use serde_json::Value;
use std::{
    future::Future,
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
    process::{Output, Stdio},
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
};
use tokio::{
    io::{AsyncBufReadExt, BufReader},
    process::{Child, Command},
};

const CONFIG: &str = r#"
[cluster]
id = "local-tests"
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

async fn bounded<T>(future: impl Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(20), future)
        .await
        .expect("secret file integration deadline")
}

fn directory(label: &str) -> PathBuf {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("target")
        .join("botctl-secret-file-tests");
    std::fs::create_dir_all(&root).unwrap();
    let path = root.join(format!(
        "{label}-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::create_dir(&path).unwrap();
    std::fs::write(path.join("placement.toml"), CONFIG).unwrap();
    path
}

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

fn botd_command(dir: &Path, extra: &[&str]) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_botd"));
    command
        .arg("--file")
        .arg(dir.join("placement.toml"))
        .arg("--runtime-root")
        .arg(dir)
        // --no-autostart のため起動されない（絶対パスであることだけが要件）。
        .arg("--gateway-program")
        .arg(env!("CARGO_BIN_EXE_botd"))
        .arg("--no-autostart")
        .args(extra)
        .env("TOKEN_ALPHA", "dummy-alpha")
        .stdin(Stdio::null())
        .kill_on_drop(true);
    command
}

/// botctl を実行する。`--json` を付ける（エラーも標準出力の JSON になる）。
async fn botctl(args: &[&str]) -> Output {
    bounded(
        Command::new(env!("CARGO_BIN_EXE_botctl"))
            .args(args)
            .arg("--json")
            .stdin(Stdio::null())
            .kill_on_drop(true)
            .output(),
    )
    .await
    .unwrap()
}

fn json_of(output: &Output) -> Value {
    assert!(
        output.stderr.is_empty(),
        "JSON errors belong on stdout: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stdout).expect("JSON output")
}

async fn generate(path: &Path) {
    let output = botctl(&["secret", "generate", "--out", path.to_str().unwrap()]).await;
    assert!(output.status.success());
    assert_eq!(json_of(&output)["ok"], true);
    assert_eq!(
        std::fs::metadata(path).unwrap().permissions().mode() & 0o7777,
        0o600
    );
}

async fn start_daemon(dir: &Path, secret: &Path, port: u16) -> Child {
    let mut child = botd_command(
        dir,
        &[
            "--secret-file",
            secret.to_str().unwrap(),
            "--control-port",
            &port.to_string(),
        ],
    )
    .stdout(Stdio::piped())
    .stderr(Stdio::null())
    .spawn()
    .unwrap();
    let mut announcement = String::new();
    bounded(BufReader::new(child.stdout.take().unwrap()).read_line(&mut announcement))
        .await
        .unwrap();
    let value: Value = serde_json::from_str(&announcement).expect("daemon announcement");
    assert_eq!(value["local_only"], true);
    assert_eq!(value["port"], u64::from(port), "--control-port is honored");
    assert!(value.get("secret").is_none());
    child
}

fn hex_of(path: &Path) -> String {
    std::fs::read_to_string(path).unwrap().trim().to_owned()
}

#[tokio::test]
async fn botctl_reaches_a_secret_file_daemon_and_rejects_wrong_or_loose_secrets() {
    let dir = directory("live");
    let secret = dir.join("botd.secret");
    generate(&secret).await;
    let hex = hex_of(&secret);
    let port = free_port();
    let port_text = port.to_string();
    let _daemon = start_daemon(&dir, &secret, port).await;
    let secret_arg = secret.to_str().unwrap();

    // 正しい secret + port で status が通る。0400 も許容する。
    for mode in [0o600, 0o400] {
        std::fs::set_permissions(&secret, std::fs::Permissions::from_mode(mode)).unwrap();
        let output = botctl(&[
            "status",
            "--secret-file",
            secret_arg,
            "--port",
            &port_text,
            "--timeout",
            "5s",
        ])
        .await;
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stdout)
        );
        let value = json_of(&output);
        assert_eq!(value["ok"], true);
        assert!(value["bots"].is_array());
        assert!(!String::from_utf8_lossy(&output.stdout).contains(&hex));
    }
    std::fs::set_permissions(&secret, std::fs::Permissions::from_mode(0o600)).unwrap();

    // 別の secret（権限は正しい）は daemon に拒否される。
    let other = dir.join("other.secret");
    generate(&other).await;
    let output = botctl(&[
        "status",
        "--secret-file",
        other.to_str().unwrap(),
        "--port",
        &port_text,
        "--timeout",
        "5s",
    ])
    .await;
    assert!(!output.status.success());
    let value = json_of(&output);
    assert_eq!(value["ok"], false);
    assert!(!String::from_utf8_lossy(&output.stdout).contains(&hex_of(&other)));

    // 権限が緩いファイルは、接続前に botctl が拒否する（理由の種別のみ。パス・値を出さない）。
    std::fs::set_permissions(&secret, std::fs::Permissions::from_mode(0o644)).unwrap();
    let output = botctl(&[
        "status",
        "--secret-file",
        secret_arg,
        "--port",
        &port_text,
        "--timeout",
        "5s",
    ])
    .await;
    assert!(!output.status.success());
    let value = json_of(&output);
    assert_eq!(value["error"], "secret file mode must be 0600 or 0400");
    let text = String::from_utf8_lossy(&output.stdout);
    assert!(!text.contains(&hex));
    assert!(!text.contains(dir.to_str().unwrap()));

    // 引数の排他・組合せ検査。
    for args in [
        &["status", "--secret-file", secret_arg][..],
        &["status", "--port", &port_text],
        &[
            "status",
            "--secret-file",
            secret_arg,
            "--port",
            &port_text,
            "--bootstrap-stdin",
        ],
        &[
            "status",
            "--secret-file",
            "relative.secret",
            "--port",
            &port_text,
        ],
        &["secret", "generate"],
        &["secret", "generate", "--out", "relative.secret"],
    ] {
        let output = botctl(args).await;
        assert!(!output.status.success(), "{args:?}");
        assert_eq!(json_of(&output)["ok"], false, "{args:?}");
    }

    // 既存ファイルは上書きされない。
    std::fs::set_permissions(&secret, std::fs::Permissions::from_mode(0o600)).unwrap();
    let output = botctl(&["secret", "generate", "--out", secret_arg]).await;
    assert!(!output.status.success());
    assert_eq!(hex_of(&secret), hex);
}

#[tokio::test]
async fn botd_refuses_unsafe_secret_files_and_conflicting_options() {
    let dir = directory("refuse");
    let loose = dir.join("loose.secret");
    generate(&loose).await;
    let hex = hex_of(&loose);
    std::fs::set_permissions(&loose, std::fs::Permissions::from_mode(0o644)).unwrap();
    let weak = dir.join("weak.secret");
    std::fs::write(&weak, format!("{}\n", "0".repeat(64))).unwrap();
    std::fs::set_permissions(&weak, std::fs::Permissions::from_mode(0o600)).unwrap();
    let missing = dir.join("missing.secret");

    let cases: Vec<(Vec<&str>, &str)> = vec![
        (
            vec!["--secret-file", loose.to_str().unwrap()],
            "botd: secret file mode must be 0600 or 0400\n",
        ),
        (
            vec!["--secret-file", weak.to_str().unwrap()],
            "botd: secret value is too weak\n",
        ),
        (
            vec!["--secret-file", missing.to_str().unwrap()],
            "botd: secret file is unreadable\n",
        ),
        (
            vec![
                "--secret-file",
                loose.to_str().unwrap(),
                "--bootstrap-stdin",
            ],
            "botd: --bootstrap-stdin and --secret-file are mutually exclusive\n",
        ),
        (
            vec![],
            "botd: anonymous stdin bootstrap or --secret-file is required\n",
        ),
    ];
    for (extra, expected) in cases {
        let output = bounded(
            botd_command(&dir, &extra)
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .output(),
        )
        .await
        .unwrap();
        assert!(!output.status.success(), "{extra:?}");
        assert_eq!(
            String::from_utf8_lossy(&output.stderr),
            expected,
            "{extra:?}"
        );
        // 起動の告知（port）は出ない。
        assert!(output.stdout.is_empty(), "{extra:?}");
        assert!(!String::from_utf8_lossy(&output.stderr).contains(&hex));
    }
}
