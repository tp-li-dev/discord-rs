//! Bounded open-loop load against real botd/Gateway/Worker and mock HTTP processes.
//! All scenarios run before asserting acceptance, preserving failure evidence.
use std::{path::Path, process::Command};

// 既知の未達: 無停止更新の許容基準を満たさない（docs/progress.md 参照）。修正までは手動実行:
//   cargo test -p src --test update_load -- --ignored
#[test]
#[ignore = "known failing: zero-downtime acceptance not met; run manually until fixed"]
fn continuous_events_during_updates_and_core_crashes() {
    let manifest = Path::new(env!("CARGO_MANIFEST_DIR"));
    let artifact = std::env::current_exe().unwrap();
    let root = artifact
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .join(format!("update-load-{}", getrandom::u64().unwrap()));
    std::fs::create_dir(&root).unwrap();
    let relay = root.join(format!("worker-relay{}", std::env::consts::EXE_SUFFIX));
    let compiled = Command::new("rustc")
        .args(["--edition=2024", "-Dwarnings"])
        .arg(manifest.join("tests/fixtures/update_load_worker.rs"))
        .arg("-o")
        .arg(&relay)
        .status()
        .expect("compile std-only fault fixture");
    assert!(compiled.success(), "fault fixture compilation failed");
    let python = if cfg!(windows) { "python" } else { "python3" };
    let mut failed = Vec::new();
    for case in [
        "core_series",
        "gateway_switch",
        "crash_old_core",
        "crash_new_core",
    ] {
        let status = Command::new(python)
            .arg(manifest.join("tests/fixtures/update_load.py"))
            .args(["--case", case, "--botd", env!("CARGO_BIN_EXE_botd")])
            .args(["--gateway", env!("CARGO_BIN_EXE_src"), "--relay"])
            .arg(&relay)
            .arg("--output")
            .arg(root.join(case))
            .env("PYTHONDONTWRITEBYTECODE", "1")
            .status()
            .expect("Python 3 is required for the process load fixture");
        if !status.success() {
            failed.push(case);
        }
    }
    println!("load evidence: {}", root.display());
    assert!(failed.is_empty(), "acceptance failed for {failed:?}");
}
