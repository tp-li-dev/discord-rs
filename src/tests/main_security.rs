use std::{
    io::Write,
    process::{Command, Output, Stdio},
};

fn captured(output: &Output) -> String {
    format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    )
}

fn gateway() -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_src"));
    for name in [
        "BOT_ID",
        "SHARD_ID",
        "TOTAL_SHARDS",
        "GENERATION_ID",
        "BOT_WORKER_IPC_IDENTITY",
        "DISCORD_TOKEN",
    ] {
        command.env_remove(name);
    }
    command
}

#[test]
fn missing_token_fails_without_automatic_error_debug_output() {
    let output = gateway().output().unwrap();
    assert!(!output.status.success());
    assert_eq!(
        String::from_utf8_lossy(&output.stderr).trim(),
        "実行に失敗しました: [REDACTED]"
    );
    assert!(output.stdout.is_empty());
}

#[test]
fn malformed_worker_input_cannot_leak_short_or_labelled_secrets_on_exit() {
    for payload in [
        "short-private-value",
        "token=fake-labelled-capability",
        "/webhooks/123/fake-url-capability",
    ] {
        let mut child = gateway()
            .arg("--worker")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        let frame = serde_json::json!({"type": payload});
        writeln!(child.stdin.take().unwrap(), "{frame}").unwrap();
        let output = child.wait_with_output().unwrap();
        assert!(!output.status.success());
        assert!(!captured(&output).contains(payload));
        assert_eq!(
            String::from_utf8_lossy(&output.stderr).trim(),
            "実行に失敗しました: [REDACTED]"
        );
    }
}

#[test]
fn worker_health_and_successful_exit_keep_wire_compatibility() {
    let mut child = gateway()
        .arg("--worker")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    writeln!(child.stdin.take().unwrap(), "{{\"type\":\"health\"}}").unwrap();
    let output = child.wait_with_output().unwrap();
    assert!(output.status.success());
    let response: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(response["type"], "ready");
    assert_eq!(response["protocol"], 5);
    assert!(output.stderr.is_empty());
}

#[cfg(any(unix, windows))]
#[test]
fn non_unicode_token_never_reaches_the_error_output() {
    #[cfg(windows)]
    let value = {
        use std::os::windows::ffi::OsStringExt;
        let mut units = "short-private-value".encode_utf16().collect::<Vec<_>>();
        units.push(0xd800);
        std::ffi::OsString::from_wide(&units)
    };
    #[cfg(unix)]
    let value = {
        use std::os::unix::ffi::OsStringExt;
        let mut bytes = "short-private-value".as_bytes().to_vec();
        bytes.push(0xff);
        std::ffi::OsString::from_vec(bytes)
    };
    let output = gateway().env("DISCORD_TOKEN", value).output().unwrap();
    assert!(!output.status.success());
    assert!(!captured(&output).contains("short-private-value"));
    assert_eq!(
        String::from_utf8_lossy(&output.stderr).trim(),
        "実行に失敗しました: [REDACTED]"
    );
}
