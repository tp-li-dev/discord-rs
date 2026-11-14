//! Real botd process with a temporary NOTIFY_SOCKET: READY=1 and WATCHDOG=1
//! arrive. Uses explicit mock Discord and no autostart; never real systemd.
#![cfg(unix)]
use std::{
    io::{BufRead, BufReader, Write},
    os::unix::net::UnixDatagram,
    process::{Command, Stdio},
    time::Duration,
};

const CONFIG: &str = r#"
[cluster]
id = "notify-tests"
mode = "standalone"
[node]
id = "one"
[[bots]]
name = "first"
bot_id = "101"
application_id = "101"
token_env = "TOKEN_ALPHA"
total_shards = 1
[[bots.shard_placement]]
shard_id = 0
primary = "one"
"#;

#[test]
fn botd_sends_ready_then_watchdog_to_notify_socket() {
    let root = std::env::temp_dir().join(format!("botd-sdn-{}", std::process::id()));
    std::fs::create_dir_all(&root).unwrap();
    let file = root.join("placement.toml");
    std::fs::write(&file, CONFIG).unwrap();
    let socket_path = root.join("n.sock");
    let socket = UnixDatagram::bind(&socket_path).unwrap();
    socket
        .set_read_timeout(Some(Duration::from_secs(15)))
        .unwrap();
    let mock = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let mut child = Command::new(env!("CARGO_BIN_EXE_botd"))
        .arg("--file")
        .arg(&file)
        .arg("--runtime-root")
        .arg(&root)
        .arg("--gateway-program")
        .arg(env!("CARGO_BIN_EXE_src"))
        .args(["--bootstrap-stdin", "--no-autostart", "--mock-discord"])
        .arg(mock.local_addr().unwrap().to_string())
        .env("NOTIFY_SOCKET", &socket_path)
        .env("WATCHDOG_USEC", "200000")
        .env_remove("WATCHDOG_PID")
        .env("TOKEN_ALPHA", "dummy")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let mut stdin = child.stdin.take().unwrap();
        stdin
            .write_all(format!("{{\"secret\":{:?}}}\n", [5u8; 32]).as_bytes())
            .unwrap();
        let mut line = String::new();
        BufReader::new(child.stdout.take().unwrap())
            .read_line(&mut line)
            .unwrap();
        assert!(line.contains("\"local_only\":true"), "{line}");
        let mut buf = [0u8; 64];
        let len = socket.recv(&mut buf).unwrap();
        assert_eq!(&buf[..len], b"READY=1");
        let len = socket.recv(&mut buf).unwrap();
        assert_eq!(&buf[..len], b"WATCHDOG=1");
        stdin
    }));
    let _ = child.kill();
    let _ = child.wait();
    let _ = std::fs::remove_dir_all(&root);
    drop(outcome.unwrap());
}
