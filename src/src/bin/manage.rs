use serde::{Deserialize, Serialize};
use src::local_control::GatewayControl;
use std::{
    env,
    error::Error,
    path::{Path, PathBuf},
    process::Stdio,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use subtle::ConstantTimeEq;
use tokio::{
    io::{AsyncBufRead, AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    net::{TcpListener, TcpStream},
    process::{Child, Command},
    sync::{Semaphore, mpsc},
};

type ErrorBox = Box<dyn Error + Send + Sync>;
const DEFAULT_PORT: u16 = 39483;
// Includes the newline. Reject oversized or incomplete requests without buffering them.
const MAX_REQUEST_BYTES: usize = 4095;

async fn read_frame<R: AsyncBufRead + Unpin>(
    reader: &mut R,
    limit: usize,
) -> std::io::Result<Option<Vec<u8>>> {
    let mut line = Vec::new();
    reader
        .take((limit + 1) as u64)
        .read_until(b'\n', &mut line)
        .await?;
    if line.len() > limit || line.last() != Some(&b'\n') {
        return Ok(None);
    }
    Ok(Some(line))
}

#[derive(Serialize, Deserialize)]
struct Endpoint {
    port: u16,
    secret: String,
}
#[derive(Serialize, Deserialize)]
struct Request {
    secret: String,
    command: String,
}
#[derive(Serialize, Deserialize)]
struct Reply {
    ok: bool,
    message: String,
}

fn root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}
fn runtime_dir() -> PathBuf {
    env::var_os("DISCORD_CORE_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| root().join("core-runtime"))
}
fn port() -> Result<u16, ErrorBox> {
    Ok(env::var("DISCORD_CORE_PORT")
        .ok()
        .map(|s| s.parse())
        .transpose()?
        .unwrap_or(DEFAULT_PORT))
}
fn token() -> Result<String, ErrorBox> {
    let mut random = [0u8; 32];
    getrandom::fill(&mut random)?;
    Ok(random.iter().map(|b| format!("{b:02x}")).collect())
}
async fn write_endpoint(endpoint: &Endpoint) -> Result<(), ErrorBox> {
    let dir = runtime_dir();
    tokio::fs::create_dir_all(&dir).await?;
    let temp = dir.join(format!("core-{}.tmp", std::process::id()));
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options.open(&temp)?;
    use std::io::Write;
    file.write_all(&serde_json::to_vec(endpoint)?)?;
    file.sync_all()?;
    std::fs::rename(temp, dir.join("core.json"))?;
    Ok(())
}
async fn request(command: &str) -> Result<String, ErrorBox> {
    let endpoint: Endpoint =
        serde_json::from_slice(&tokio::fs::read(runtime_dir().join("core.json")).await?)?;
    if endpoint.port != port()? {
        return Err("指定したコアのポートが一致しません".into());
    }
    let mut stream = tokio::time::timeout(
        Duration::from_secs(2),
        TcpStream::connect(("127.0.0.1", endpoint.port)),
    )
    .await??;
    let mut data = serde_json::to_vec(&Request {
        secret: endpoint.secret,
        command: command.to_owned(),
    })?;
    data.push(b'\n');
    stream.write_all(&data).await?;
    let line = tokio::time::timeout(
        Duration::from_secs(600),
        read_frame(&mut BufReader::new(stream), 64 * 1024),
    )
    .await??
    .ok_or("invalid or oversized management reply")?;
    let reply: Reply = serde_json::from_slice(&line)?;
    if !reply.ok {
        return Err(reply.message.into());
    }
    Ok(reply.message)
}
async fn build() -> Result<PathBuf, ErrorBox> {
    let target = root().join("target").join("reload-build");
    println!("Botをビルドしています...");
    let result = Command::new(env::var_os("CARGO").unwrap_or_else(|| "cargo".into()))
        .current_dir(root())
        .args(["build", "--release", "--bin", "src", "--target-dir"])
        .arg(&target)
        .env_remove("DISCORD_BUILD_ID")
        .env_remove("DISCORD_TOKEN")
        .status()
        .await?;
    if !result.success() {
        return Err("Botのビルドに失敗しました".into());
    }
    let workers = root().join("workers");
    tokio::fs::create_dir_all(&workers).await?;
    let id = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
    let path = workers.join(format!(
        "parent-{}-{id}{}",
        std::process::id(),
        env::consts::EXE_SUFFIX
    ));
    tokio::fs::copy(
        target
            .join("release")
            .join(format!("src{}", env::consts::EXE_SUFFIX)),
        &path,
    )
    .await?;
    Ok(path)
}
struct Bot {
    child: Child,
    control: GatewayControl,
}
impl std::ops::Deref for Bot {
    type Target = Child;
    fn deref(&self) -> &Child {
        &self.child
    }
}
impl std::ops::DerefMut for Bot {
    fn deref_mut(&mut self) -> &mut Child {
        &mut self.child
    }
}
async fn spawn_bot(path: &Path) -> Result<Bot, ErrorBox> {
    let control = GatewayControl::bind().await?;
    let mut child = Command::new(path)
        .arg("--local-control")
        .current_dir(root())
        .stdin(Stdio::piped())
        .kill_on_drop(true)
        .spawn()?;
    control
        .bootstrap(child.stdin.take().ok_or("missing bootstrap pipe")?)
        .await?;
    Ok(Bot { child, control })
}
async fn stop_bot(child: &mut Option<Bot>) -> Result<(), ErrorBox> {
    if let Some(mut process) = child.take() {
        if process.try_wait()?.is_none() {
            process.kill().await?;
        }
        let _ = process.wait().await;
    }
    Ok(())
}
async fn core(binary: PathBuf) -> Result<(), ErrorBox> {
    let listener = TcpListener::bind(("127.0.0.1", port()?)).await?;
    let secret = token()?;
    let endpoint = Endpoint {
        port: listener.local_addr()?.port(),
        secret: secret.clone(),
    };
    let mut bot = Some(spawn_bot(&binary).await?);
    let mut desired_running = true;
    let mut failures = 0u8;
    let mut last_failure: Option<String> = None;
    let mut started = tokio::time::Instant::now();
    let mut supervision = tokio::time::interval(Duration::from_secs(2));
    let readers = Arc::new(Semaphore::new(32));
    let (requests_tx, mut requests_rx) = mpsc::channel::<(Request, BufReader<TcpStream>)>(32);
    write_endpoint(&endpoint).await?;
    println!("コアを起動しました。BotのDiscord接続状態はログで確認してください。");
    loop {
        let (input, mut reader) = tokio::select! {
            accepted = listener.accept() => {
                let (stream, _) = accepted?;
                let Ok(permit) = readers.clone().try_acquire_owned() else { continue };
                let sender = requests_tx.clone();
                let expected_secret = secret.clone();
                tokio::spawn(async move {
                    let _permit = permit;
                    let mut reader = BufReader::new(stream);
                    let Ok(Ok(Some(line))) = tokio::time::timeout(
                        Duration::from_secs(3), read_frame(&mut reader, MAX_REQUEST_BYTES)
                    ).await else { return };
                    let Ok(input) = serde_json::from_slice::<Request>(&line) else { return };
                    if bool::from(input.secret.as_bytes().ct_eq(expected_secret.as_bytes())) {
                        let _ = sender.try_send((input, reader));
                    }
                });
                continue;
            },
            Some(request) = requests_rx.recv() => request,
            _ = supervision.tick() => {
                if let Some(process) = bot.as_mut() {
                    if let Some(status) = process.try_wait()? {
                        let message = format!("Botが終了しました: {status}");
                        eprintln!("{message}"); last_failure = Some(message); bot = None;
                    } else if started.elapsed() >= Duration::from_secs(60) { failures = 0; }
                }
                if desired_running && bot.is_none() && failures < 3 {
                    failures += 1;
                    match spawn_bot(&binary).await {
                        Ok(process) => { eprintln!("Botを自動再起動しました ({failures}/3)"); bot = Some(process); started = tokio::time::Instant::now(); }
                        Err(error) => { let message = format!("Botの自動再起動に失敗しました: {error}"); eprintln!("{message}"); last_failure = Some(message); }
                    }
                }
                continue;
            }
        };
        if let Some(process) = bot.as_mut()
            && let Some(status) = process.try_wait()?
        {
            println!("Bot終了: {status}");
            last_failure = Some(format!("Botが終了しました: {status}"));
            bot = None;
        }
        let mut exit = false;
        let action: Result<String, ErrorBox> = async {
            match input.command.as_str() {
                "list-unknown" => {
                    let bot = bot.as_mut().ok_or("Gateway unavailable")?;
                    let reply = bot.control.list().await.map_err(|_| "Gateway unavailable")?;
                    let timestamp = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
                    Ok(format!("snapshot_unix_seconds={timestamp}; volatile=true; state=Unknown\n{}", serde_json::to_string(&reply)?))
                }
                "status" => Ok(format!("{}\n直近のエラー: {}\n自動再起動: {}/3\nログ: {}", match &bot {
                    Some(process) => format!("コア: 稼働中 (PID: {})\nBotプロセス: 稼働中 (PID: {})\nDiscord接続: Botのログで確認してください", std::process::id(), process.id().unwrap_or_default()),
                    None => format!("コア: 稼働中 (PID: {})\nBotプロセス: 停止中", std::process::id()),
                }, last_failure.as_deref().unwrap_or("なし"), failures, runtime_dir().join("core.log").display())),
                "start" => {
                    if bot.is_some() { return Ok("Botは既に稼働中です".into()); }
                    let process = spawn_bot(&binary).await?;
                    let pid = process.id().unwrap_or_default(); bot = Some(process); desired_running = true; failures = 0; started = tokio::time::Instant::now();
                    Ok(format!("Botを起動しました (PID: {pid})。Discord接続はログで確認してください。"))
                }
                "stop" => { desired_running = false; stop_bot(&mut bot).await?; Ok("Botを停止しました。メモリ内の状態は失われます。コアは稼働中です。".into()) }
                "restart" => {
                    desired_running = false; stop_bot(&mut bot).await?;
                    let process = spawn_bot(&binary).await?;
                    let pid = process.id().unwrap_or_default(); bot = Some(process); desired_running = true; failures = 0; started = tokio::time::Instant::now();
                    Ok(format!("Botを再起動しました (PID: {pid})。メモリ内の状態は失われました。"))
                }
                "update" => {
                    if bot.is_none() { return Err("Botが停止しています。先に start を実行してください".into()); }
                    let output = Command::new(env::var_os("CARGO").unwrap_or_else(|| "cargo".into())).current_dir(root())
                        .args(["run", "--bin", "reload", "--", "general"]).env_remove("DISCORD_TOKEN").output().await?;
                    let stdout = String::from_utf8_lossy(&output.stdout);
                    let stderr = String::from_utf8_lossy(&output.stderr);
                    println!("{stdout}\n{stderr}");
                    if !output.status.success() { return Err(format!("更新できませんでした。コアのログを確認してください。{stderr}").into()); }
                    Ok(stdout.trim().to_owned())
                }
                "shutdown" => { desired_running = false; stop_bot(&mut bot).await?; exit = true; Ok("Botとコアを停止しました。メモリ内の状態は失われます。".into()) }
                _ => Err("不明なコマンドです".into()),
            }
        }.await;
        let reply = match action {
            Ok(message) => Reply { ok: true, message },
            Err(error) => {
                let message = format!("{} 失敗: {error}", input.command);
                eprintln!("{message}");
                last_failure = Some(message.clone());
                Reply { ok: false, message }
            }
        };
        let mut data = serde_json::to_vec(&reply)?;
        data.push(b'\n');
        let _ =
            tokio::time::timeout(Duration::from_secs(3), reader.get_mut().write_all(&data)).await;
        if exit {
            break;
        }
    }
    let _ = tokio::fs::remove_file(runtime_dir().join("core.json")).await;
    Ok(())
}
fn launch_core(path: &Path) -> Result<(), ErrorBox> {
    std::fs::create_dir_all(runtime_dir())?;
    let current = env::current_exe()?;
    let id = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
    let executable = root().join("workers").join(format!(
        "core-{}-{id}{}",
        std::process::id(),
        env::consts::EXE_SUFFIX
    ));
    std::fs::copy(current, &executable)?;
    let log = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(runtime_dir().join("core.log"))?;
    let mut command = std::process::Command::new(executable);
    command
        .arg("__core")
        .arg(path)
        .current_dir(root())
        .stdin(Stdio::null())
        .stdout(Stdio::from(log.try_clone()?))
        .stderr(Stdio::from(log));
    #[cfg(windows)]
    {
        use std::os::windows::process::CommandExt;
        command.creation_flags(0x08000000);
    }
    command.spawn()?;
    Ok(())
}
#[tokio::main]
async fn main() -> Result<(), ErrorBox> {
    let args: Vec<_> = env::args_os().skip(1).collect();
    let command = args.first().and_then(|s| s.to_str()).unwrap_or("help");
    if command == "__core" {
        return core(args.get(1).ok_or("missing executable")?.into()).await;
    }
    if args.len() > 1 {
        return Err("引数が多すぎます".into());
    }
    match command {
        "start" => {
            if request("status").await.is_ok() {
                println!("{}", request("start").await?);
                return Ok(());
            }
            if env::var_os("DISCORD_TOKEN").is_none_or(|v| v.is_empty()) {
                return Err("DISCORD_TOKEN が設定されていません".into());
            }
            let binary = build().await?;
            launch_core(&binary)?;
            for _ in 0..100 {
                tokio::time::sleep(Duration::from_millis(100)).await;
                if let Ok(status) = request("status").await {
                    if status.contains("Botプロセス: 稼働中") {
                        println!("{status}");
                        return Ok(());
                    }
                    return Err(format!("Botの起動を確認できません: {status}").into());
                }
            }
            Err("コアの起動を確認できません。src/core-runtime/core.log を確認してください".into())
        }
        "list-unknown" | "status" | "stop" | "restart" | "update" | "shutdown" => {
            match request(command).await {
                Ok(message) => {
                    println!("{message}");
                    Ok(())
                }
                Err(error) => {
                    Err(format!("コアに接続できないか、コマンドが失敗しました: {error}").into())
                }
            }
        }
        "logs" => {
            print!(
                "{}",
                tokio::fs::read_to_string(runtime_dir().join("core.log")).await?
            );
            Ok(())
        }
        _ => {
            println!(
                "使い方: cargo run --bin manage -- <start|list-unknown|status|stop|restart|update|logs|shutdown>"
            );
            Ok(())
        }
    }
}

#[cfg(test)]
mod request_tests {
    use super::*;

    #[tokio::test]
    async fn request_frame_limits_and_termination() {
        let valid = vec![b'x'; MAX_REQUEST_BYTES - 1]
            .into_iter()
            .chain(*b"\n")
            .collect::<Vec<_>>();
        assert_eq!(
            read_frame(&mut BufReader::new(valid.as_slice()), MAX_REQUEST_BYTES)
                .await
                .unwrap(),
            Some(valid)
        );
        let oversized = vec![b'x'; MAX_REQUEST_BYTES + 100];
        assert!(
            read_frame(&mut BufReader::new(oversized.as_slice()), MAX_REQUEST_BYTES)
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            read_frame(&mut BufReader::new(&b"unfinished"[..]), MAX_REQUEST_BYTES)
                .await
                .unwrap()
                .is_none()
        );
    }
}
