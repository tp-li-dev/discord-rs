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

fn assert_json_diagnostic(output: &Output, category: &str) {
    assert!(!output.status.success());
    assert!(output.stdout.is_empty());
    let stderr = String::from_utf8(output.stderr.clone()).unwrap();
    let location = stderr
        .trim()
        .strip_prefix(&format!(
            "実行に失敗しました: invalid JSON (category={category}, line="
        ))
        .expect("JSON errors must retain their category")
        .strip_suffix(')')
        .expect("diagnostics must end after the source location");
    let (line, column) = location.split_once(", column=").unwrap();
    assert!(line.parse::<usize>().unwrap() > 0);
    // EOF immediately after a newline legitimately reports column zero.
    column.parse::<usize>().unwrap();
}

#[test]
fn missing_token_preserves_a_safe_configuration_diagnostic() {
    let output = gateway().output().unwrap();
    assert!(!output.status.success());
    assert_eq!(
        String::from_utf8_lossy(&output.stderr).trim(),
        "実行に失敗しました: DISCORD_TOKEN is not configured [token_missing]"
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
        assert!(!captured(&output).contains(payload));
        assert_json_diagnostic(&output, "Data");
    }
}

#[test]
fn malformed_worker_json_reports_syntax_and_eof_without_echoing_input() {
    for (frame, category) in [
        ("{\"type\": short-private-value}", "Syntax"),
        ("{\"content\":\"short-private-value\",", "Eof"),
    ] {
        let mut child = gateway()
            .arg("--worker")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        writeln!(child.stdin.take().unwrap(), "{frame}").unwrap();
        let output = child.wait_with_output().unwrap();
        assert!(!captured(&output).contains("short-private-value"));
        assert_json_diagnostic(&output, category);
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
        "実行に失敗しました: DISCORD_TOKEN is not Unicode [token_encoding]"
    );
}

#[test]
fn invalid_management_bootstrap_reports_the_cause_without_payload() {
    let mut child = gateway()
        .arg("--local-control")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    writeln!(
        child.stdin.take().unwrap(),
        "short-private-value token=fake-token"
    )
    .unwrap();
    let output = child.wait_with_output().unwrap();
    assert!(!output.status.success());
    assert!(output.stdout.is_empty());
    assert_eq!(
        String::from_utf8_lossy(&output.stderr).trim(),
        "実行に失敗しました: invalid management bootstrap [bootstrap_invalid]"
    );
}
