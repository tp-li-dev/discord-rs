use src::placement::{Config, PlacementError};
use std::{
    io::{BufRead, BufReader, Write},
    net::TcpListener,
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    time::{Duration, Instant},
};

const DEADLINE: Duration = Duration::from_secs(30);

fn directory(label: &str) -> PathBuf {
    let path = std::env::temp_dir().join(format!(
        "{label}-{}-{:016x}",
        std::process::id(),
        getrandom::u64().unwrap()
    ));
    std::fs::create_dir(&path).unwrap();
    path
}

struct CoreGuard {
    child: Child,
    fixture_exit: PathBuf,
}

impl Drop for CoreGuard {
    fn drop(&mut self) {
        // The fixture also exits if an assertion panics before graceful shutdown.
        let _ = std::fs::write(&self.fixture_exit, b"exit");
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn ready_port(directory: &Path, child: &mut Child) -> u16 {
    let deadline = Instant::now() + DEADLINE;
    loop {
        assert!(
            child.try_wait().unwrap().is_none(),
            "management core exited before readiness"
        );
        let endpoint = |name| {
            std::fs::read(directory.join(name))
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
        assert!(
            Instant::now() < deadline,
            "management endpoint readiness deadline"
        );
        std::thread::sleep(Duration::from_millis(10));
    }
}

#[test]
fn placement_debug_hides_even_short_unlabelled_text() {
    let mut config = Config::load(
        &std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("examples/placement.toml"),
    )
    .unwrap();
    config.cluster.id = "short-private-value".into();
    config.node.id = "short-private-value".into();
    config.bots[0].name = "short-private-value".into();
    config.bots[0].prefix = Some("short-private-value".into());
    config.bots[0].shard_placement[0].primary = "short-private-value".into();
    assert!(!format!("{config:#?}").contains("short-private-value"));
    let error = PlacementError("request token=fake-private-value failed".into());
    assert!(!format!("{error} {error:?}").contains("fake-private-value"));
}

#[test]
fn management_reply_errors_and_log_display_are_masked() {
    let dir = directory("redaction-test");
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    std::fs::write(
        dir.join("core.json"),
        serde_json::to_vec(&serde_json::json!({"port":port,"secret":"fake-auth-capability"}))
            .unwrap(),
    )
    .unwrap();
    let server = std::thread::spawn(move || {
        for ok in [true, false] {
            let (mut socket, _) = listener.accept().unwrap();
            socket.set_read_timeout(Some(DEADLINE)).unwrap();
            socket.set_write_timeout(Some(DEADLINE)).unwrap();
            let mut line = String::new();
            BufReader::new(socket.try_clone().unwrap())
                .read_line(&mut line)
                .unwrap();
            let request: serde_json::Value = serde_json::from_str(&line).unwrap();
            assert_eq!(request["secret"], "fake-auth-capability");
            writeln!(socket, "{}", serde_json::json!({"ok":ok,"message":"request /webhooks/123/fake-url-capability failed Authorization: Bearer fake-header-capability"})).unwrap();
        }
    });
    for success in [true, false] {
        let output = Command::new(env!("CARGO_BIN_EXE_manage"))
            .arg("status")
            .env("DISCORD_CORE_DIR", &dir)
            .env("DISCORD_CORE_PORT", port.to_string())
            .output()
            .unwrap();
        assert_eq!(output.status.success(), success);
        let text = format!(
            "{}{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(text.contains("[REDACTED]"));
        assert!(!text.contains("fake-url-capability"));
        assert!(!text.contains("fake-header-capability"));
    }
    server.join().unwrap();
    std::fs::remove_file(dir.join("core.json")).unwrap();
    let fixture_source = dir.join("fixture.rs");
    let fixture_binary = dir.join(format!("fixture{}", std::env::consts::EXE_SUFFIX));
    std::fs::write(
        &fixture_source,
        "fn main() { let exit = std::env::var_os(\"FIXTURE_EXIT\").unwrap(); while !std::path::Path::new(&exit).exists() { std::thread::sleep(std::time::Duration::from_millis(10)); } }",
    )
    .unwrap();
    assert!(
        Command::new("rustc")
            .arg(&fixture_source)
            .arg("-o")
            .arg(&fixture_binary)
            .status()
            .unwrap()
            .success()
    );
    let fixture_exit = dir.join("fixture-exit");
    let child = Command::new(env!("CARGO_BIN_EXE_manage"))
        .arg("__core")
        .arg(&fixture_binary)
        .env("DISCORD_CORE_DIR", &dir)
        .env("DISCORD_CORE_PORT", "0")
        .env("FIXTURE_EXIT", &fixture_exit)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let mut core = CoreGuard {
        child,
        fixture_exit,
    };
    let port = ready_port(&dir, &mut core.child);
    std::fs::write(dir.join("core.log"), "old token=fake-log-capability\n").unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_manage"))
        .arg("logs")
        .env("DISCORD_CORE_DIR", &dir)
        .env("DISCORD_CORE_PORT", port.to_string())
        .output()
        .unwrap();
    assert!(output.status.success());
    let text = String::from_utf8_lossy(&output.stdout);
    assert!(text.contains("old token=[REDACTED]"));
    assert!(!text.contains("fake-log-capability"));
    assert!(
        Command::new(env!("CARGO_BIN_EXE_manage"))
            .arg("shutdown")
            .env("DISCORD_CORE_DIR", &dir)
            .env("DISCORD_CORE_PORT", port.to_string())
            .status()
            .unwrap()
            .success()
    );
    assert!(core.child.wait().unwrap().success());
    let audit = std::fs::read_to_string(dir.join("audit.jsonl")).unwrap();
    let records: Vec<serde_json::Value> = audit
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    assert!(
        records.iter().any(|r| r["operation"] == "logs"
            && r["role"] == "admin"
            && r["result"] == "succeeded")
    );
    assert!(!audit.contains("fake-log-capability"));
    drop(core);
    let deadline = Instant::now() + DEADLINE;
    loop {
        match std::fs::remove_dir_all(&dir) {
            Ok(()) => break,
            Err(error) if error.kind() == std::io::ErrorKind::PermissionDenied => {
                assert!(Instant::now() < deadline, "fixture cleanup deadline");
                std::thread::sleep(Duration::from_millis(20));
            }
            Err(error) => panic!("fixture cleanup failed: {:?}", error.kind()),
        }
    }
}
#[test]
fn reload_masks_child_build_stdout_and_stderr() {
    let dir = directory("redaction-build-test");
    #[cfg(windows)]
    let script = dir.join("fake-cargo.cmd");
    #[cfg(windows)]
    std::fs::write(&script, "@echo off\r\necho token=fake-build-stdout\r\necho Authorization: Bearer fake-build-stderr 1>&2\r\nexit /b 1\r\n").unwrap();
    #[cfg(unix)]
    let script = {
        use std::os::unix::fs::PermissionsExt;
        let script = dir.join("fake-cargo");
        std::fs::write(&script, "#!/bin/sh\nprintf 'token=fake-build-stdout\\n'\nprintf 'Authorization: Bearer fake-build-stderr\\n' >&2\nexit 1\n").unwrap();
        std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o700)).unwrap();
        script
    };
    let output = Command::new(env!("CARGO_BIN_EXE_reload"))
        .arg("general")
        .env("CARGO", &script)
        .env("DISCORD_WORKER_FILE", dir.join("unused-worker-path"))
        .output()
        .unwrap();
    assert!(!output.status.success());
    let text = format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(text.contains("[REDACTED]"));
    assert!(!text.contains("fake-build-stdout"));
    assert!(!text.contains("fake-build-stderr"));
    assert!(!dir.join("unused-worker-path").exists());
    std::fs::remove_file(script).unwrap();
    std::fs::remove_dir(dir).unwrap();
}
