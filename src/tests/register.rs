use std::process::Command;

#[test]
fn dry_run_discovers_linked_command_without_token_or_network() {
    let output = Command::new(env!("CARGO_BIN_EXE_register"))
        .args(["123", "456", "ping"])
        .env_remove("DISCORD_TOKEN")
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let text = String::from_utf8_lossy(&output.stdout);
    assert!(
        text.contains("command=ping description=Check response scope=guild:456"),
        "{text}"
    );
    assert!(text.contains("Dry run"));
}

#[test]
fn rejects_unknown_command_before_network() {
    let output = Command::new(env!("CARGO_BIN_EXE_register"))
        .args(["123", "global", "missing", "--apply"])
        .env_remove("DISCORD_TOKEN")
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("unknown linked command"));
}
