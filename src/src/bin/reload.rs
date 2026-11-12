use src::redact::{MASK, error_diagnostic};
use std::{
    env,
    error::Error,
    fs,
    path::PathBuf,
    process::{Command, ExitStatus, Stdio},
    thread,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

fn main() {
    src::redact::install_panic_hook();
    if let Err(error) = run() {
        eprintln!("reload: {}", diagnostic(error.as_ref()));
        std::process::exit(1);
    }
}

#[derive(Debug)]
struct ReloadFailure(&'static str);
impl std::fmt::Display for ReloadFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.0)
    }
}
impl Error for ReloadFailure {}
#[derive(Debug)]
struct WorkerUpdateFailure {
    phase: &'static str,
    exit_code: Option<i32>,
}
impl std::fmt::Display for WorkerUpdateFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Worker更新に失敗しました [worker_update_failed] (phase={}, exit_code={:?}): {MASK}（更新要求は残っており、コアが再試行します）",
            self.phase, self.exit_code
        )
    }
}
impl Error for WorkerUpdateFailure {}

fn state_failure(value: &serde_json::Value) -> Box<dyn Error> {
    Box::new(WorkerUpdateFailure {
        phase: match value["phase"].as_str() {
            Some("update") => "update",
            Some("runtime") => "runtime",
            _ => "unknown",
        },
        exit_code: value["exit_code"]
            .as_i64()
            .and_then(|code| i32::try_from(code).ok()),
    })
}
fn failure(message: &'static str) -> Box<dyn Error> {
    Box::new(ReloadFailure(message))
}
fn diagnostic(error: &(dyn Error + 'static)) -> String {
    if let Some(error) = error.downcast_ref::<WorkerUpdateFailure>() {
        return error.to_string();
    }
    error
        .downcast_ref::<ReloadFailure>()
        .map_or_else(|| error_diagnostic(error), |e| e.0.to_owned())
}

fn build_diagnostic(status: ExitStatus) -> String {
    format!(
        "build completed (success={}, exit_code={:?}); child output: {MASK}",
        status.success(),
        status.code()
    )
}

fn run() -> Result<(), Box<dyn Error>> {
    let args: Vec<_> = env::args_os().skip(1).collect();
    if args != [std::ffi::OsString::from("general")] {
        return Err(failure("使い方: cargo run --bin reload -- general"));
    }
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let file = env::var_os("DISCORD_WORKER_FILE")
        .map(PathBuf::from)
        .unwrap_or_else(|| root.join("worker-path.txt"));
    if !file.is_absolute() {
        return Err(failure("DISCORD_WORKER_FILE は絶対パスで指定してください"));
    }
    let target = root.join("target").join("reload-build");
    let id = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
    let build = format!("build-{id}");
    println!("Workerをビルドしています");
    let status = Command::new(env::var_os("CARGO").unwrap_or_else(|| "cargo".into()))
        .current_dir(&root)
        .env("DISCORD_BUILD_ID", &build)
        .env_remove("DISCORD_TOKEN")
        .args([
            "build",
            "--offline",
            "--release",
            "--bin",
            "src",
            "--target-dir",
        ])
        .arg(&target)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()?;
    eprintln!("{}", build_diagnostic(status));
    if !status.success() {
        return Err(failure(
            "ビルドに失敗しました。稼働中のWorkerは変更していません [build_failed]",
        ));
    }
    let workers = root.join("workers");
    fs::create_dir_all(&workers)?;
    let binary = workers.join(format!(
        "worker-{}-{id}{}",
        std::process::id(),
        env::consts::EXE_SUFFIX
    ));
    fs::copy(
        target
            .join("release")
            .join(format!("src{}", env::consts::EXE_SUFFIX)),
        &binary,
    )?;
    let mut tmp = file.as_os_str().to_owned();
    tmp.push(format!(".{}.tmp", std::process::id()));
    let tmp = PathBuf::from(tmp);
    fs::write(&tmp, binary.to_string_lossy().as_bytes())?;
    fs::rename(&tmp, &file)?;
    let mut status_path = file.as_os_str().to_owned();
    status_path.push(".status");
    let status_path = PathBuf::from(status_path);
    let mut error_path = status_path.as_os_str().to_owned();
    error_path.push(".error");
    let error_path = PathBuf::from(error_path);
    println!("Botに切り替えを依頼しました。確認中...");
    for _ in 0..100 {
        if fs::read_to_string(&status_path)
            .is_ok_and(|value| value.trim() == binary.to_string_lossy())
        {
            println!("更新しました！ Workerのビルドを確認しました");
            return Ok(());
        }
        if let Ok(content) = fs::read_to_string(&error_path)
            && let Ok(value) = serde_json::from_str::<serde_json::Value>(&content)
            && value["path"].as_str() == Some(binary.to_string_lossy().as_ref())
        {
            // Only closed stage names and a numeric exit code are diagnostic data.
            return Err(state_failure(&value));
        }
        thread::sleep(Duration::from_millis(200));
    }
    Err(failure(
        "切り替えの確認ができませんでした。Botの起動とログを確認してください（更新要求は残っています） [confirmation_timeout]",
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[ignore = "subprocess fixture"]
    fn child_output_fixture() {
        if env::var_os("RELOAD_OUTPUT_FIXTURE").is_none() {
            return;
        }
        println!("token=fake-token secret=fake-secret");
        eprintln!("private_key=fake-key Authorization: Bearer fake-auth unlabelled-value");
        std::process::exit(23);
    }

    #[test]
    #[ignore = "subprocess fixture"]
    fn protected_build_output_fixture() {
        if env::var_os("RELOAD_PARENT_FIXTURE").is_none() {
            return;
        }
        src::redact::install_panic_hook();
        let status = Command::new(env::current_exe().unwrap())
            .args([
                "--exact",
                "tests::child_output_fixture",
                "--ignored",
                "--nocapture",
            ])
            .env("RELOAD_OUTPUT_FIXTURE", "1")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .unwrap();
        eprintln!("{}", build_diagnostic(status));
        let error = std::io::Error::other(
            "token=fake-token secret=fake-secret private_key=fake-key Authorization: Bearer fake-auth unlabelled-value",
        );
        eprintln!("reload: {}", diagnostic(&error));
        let state = serde_json::json!({
            "error": "token=fake-token secret=fake-secret private_key=fake-key Authorization: Bearer fake-auth unlabelled-value",
            "phase": "runtime", "exit_code": 17,
        });
        eprintln!("reload: {}", diagnostic(state_failure(&state).as_ref()));
        std::process::exit(1);
    }

    #[test]
    fn process_output_keeps_build_exit_code_without_child_or_error_payloads() {
        let output = Command::new(env::current_exe().unwrap())
            .args([
                "--exact",
                "tests::protected_build_output_fixture",
                "--ignored",
                "--nocapture",
            ])
            .env("RELOAD_PARENT_FIXTURE", "1")
            .output()
            .unwrap();
        assert_eq!(output.status.code(), Some(1));
        let text = format!(
            "{}{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(text.contains("success=false, exit_code=Some(23)"));
        assert!(text.contains(MASK));
        assert!(text.contains("Other"));
        assert!(text.contains("phase=runtime, exit_code=Some(17)"));
        for secret in [
            "fake-token",
            "fake-secret",
            "fake-key",
            "fake-auth",
            "unlabelled-value",
        ] {
            assert!(!text.contains(secret));
        }
    }

    #[test]
    fn unknown_state_fields_cannot_become_diagnostics() {
        let state = serde_json::json!({"phase": "short-private-value", "exit_code": i64::MAX, "error": "short-private-value"});
        let diagnostic = diagnostic(state_failure(&state).as_ref());
        assert!(diagnostic.contains("phase=unknown, exit_code=None"));
        assert!(!diagnostic.contains("short-private-value"));
    }
}
