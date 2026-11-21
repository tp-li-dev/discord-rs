#![cfg(target_os = "linux")]

use src::build_env::cargo_command;
use std::{env, ffi::OsStr, process::Command};

const PROBE: &str = "DISCORD_RS_BUILD_ENV_TEST_PROBE";

#[test]
fn cargo_child_receives_only_allowlisted_environment() {
    // Put sentinel variables in an isolated process's real environment, without
    // mutating the test runner's environment or copying local credentials.
    let status = Command::new(env::current_exe().unwrap())
        .args(["--exact", "cargo_child_probe", "--ignored", "--nocapture"])
        .env_clear()
        .envs([
            (PROBE, "1"),
            ("CARGO", "/usr/bin/env"),
            ("PATH", "/usr/bin:/bin"),
            ("Path", "/discard"),
            ("DISCORD_TOKEN", "fixture"),
            ("MUSIC_BOT_TOKEN", "fixture"),
            ("DISCORD_TEST_BOT_TOKEN", "fixture"),
            ("TOKEN_ALPHA", "fixture"),
            (src::placement::GATEWAY_TOKEN_ENV, "fixture"),
            ("LC_TOKEN_ALPHA", "fixture"),
            ("UNLISTED", "fixture"),
        ])
        .status()
        .unwrap();
    assert!(status.success());
}

#[test]
#[ignore = "run in a subprocess with a controlled environment"]
fn cargo_child_probe() {
    if env::var_os(PROBE).as_deref() != Some(OsStr::new("1")) {
        return;
    }
    assert_eq!(
        env::var_os("CARGO").as_deref(),
        Some(OsStr::new("/usr/bin/env"))
    );
    let output = cargo_command().output().unwrap();
    assert!(output.status.success());
    assert_eq!(output.stdout, b"PATH=/usr/bin:/bin\n");
}
