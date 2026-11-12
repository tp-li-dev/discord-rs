use std::process::Command;

fn doctor(args: &[&str]) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_doctor"))
        .args(args)
        .output()
        .unwrap()
}

#[test]
fn sample_requires_resolved_auto_count_then_passes() {
    let path = concat!(env!("CARGO_MANIFEST_DIR"), "/examples/placement.toml");
    let unresolved = doctor(&[path]);
    assert!(!unresolved.status.success());
    assert!(String::from_utf8_lossy(&unresolved.stderr).contains("unresolved"));
    let valid = doctor(&[path, "--resolved-shards", "bot-two=1"]);
    assert!(
        valid.status.success(),
        "{}",
        String::from_utf8_lossy(&valid.stderr)
    );
    let output = String::from_utf8_lossy(&valid.stdout);
    assert!(output.contains("Offline check only"));
    assert!(output.contains("unverified"));
}

#[test]
fn bad_arguments_and_nonexistent_configuration_fail() {
    let path = concat!(env!("CARGO_MANIFEST_DIR"), "/examples/placement.toml");
    for args in [
        vec![],
        vec!["/nonexistent/placement.toml"],
        vec![path, "--resolved-shards", "bot-two=0"],
        vec![
            path,
            "--resolved-shards",
            "bot-two=1",
            "--resolved-shards",
            "bot-two=1",
        ],
        vec![path, "--resolved-shards", "wrong=1"],
    ] {
        assert!(!doctor(&args).status.success());
    }
}
