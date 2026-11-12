use src::redact::redact;
use std::{
    env,
    error::Error,
    fs,
    path::PathBuf,
    process::Command,
    thread,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

fn main() {
    if let Err(error) = run() {
        eprintln!("reload: {}", redact(&error.to_string()));
        std::process::exit(1);
    }
}

fn run() -> Result<(), Box<dyn Error>> {
    let args: Vec<_> = env::args().skip(1).collect();
    if args != ["general"] {
        return Err("使い方: cargo run --bin reload -- general".into());
    }
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let file = env::var_os("DISCORD_WORKER_FILE")
        .map(PathBuf::from)
        .unwrap_or_else(|| root.join("worker-path.txt"));
    if !file.is_absolute() {
        return Err("DISCORD_WORKER_FILE は絶対パスで指定してください".into());
    }
    let target = root.join("target").join("reload-build");
    let id = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
    let build = format!("build-{id}");
    println!("Workerをビルドしています: {}", redact(&build));
    let status = Command::new(env::var_os("CARGO").unwrap_or_else(|| "cargo".into()))
        .current_dir(&root)
        .env("DISCORD_BUILD_ID", &build)
        .env_remove("DISCORD_TOKEN")
        .args(["build", "--release", "--bin", "src", "--target-dir"])
        .arg(&target)
        .output()?;
    print!("{}", redact(&String::from_utf8_lossy(&status.stdout)));
    eprint!("{}", redact(&String::from_utf8_lossy(&status.stderr)));
    if !status.status.success() {
        return Err("ビルドに失敗しました。稼働中のWorkerは変更していません".into());
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
            println!("更新しました！ Workerのビルド: {}", redact(&build));
            return Ok(());
        }
        if let Ok(content) = fs::read_to_string(&error_path)
            && let Ok(value) = serde_json::from_str::<serde_json::Value>(&content)
            && value["path"].as_str() == Some(binary.to_string_lossy().as_ref())
        {
            let reason = redact(value["error"].as_str().unwrap_or("不明なエラー"));
            return Err(format!(
                "Worker更新に失敗しました: {reason}（更新要求は残っており、コアが再試行します）"
            )
            .into());
        }
        thread::sleep(Duration::from_millis(200));
    }
    Err("切り替えの確認ができませんでした。Botの起動とログを確認してください（更新要求は残っています）".into())
}
