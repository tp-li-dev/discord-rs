use serde::{Deserialize, Serialize};
use src::local_control::GatewayControl;
use src::metrics::MetricsRegistry;
use src::rbac_audit::{
    AuditConfig, AuditedExecutor, Confirmation, Principal, Request as AuditRequest,
};
use src::redact::{MASK, Secret, error_diagnostic};
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

// Wire encoding is deliberate; Secret itself remains non-serializable.
mod secret_wire {
    use super::*;
    pub fn serialize<S: serde::Serializer>(
        value: &Secret<String>,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        value.expose_secret().serialize(serializer)
    }
    pub fn deserialize<'de, D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Secret<String>, D::Error> {
        String::deserialize(deserializer).map(Secret::new)
    }
}

#[derive(Serialize, Deserialize)]
struct Endpoint {
    port: u16,
    #[serde(with = "secret_wire")]
    secret: Secret<String>,
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Request {
    #[serde(with = "secret_wire")]
    secret: Secret<String>,
    command: String,
}
#[derive(Serialize, Deserialize)]
struct Reply {
    ok: bool,
    message: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    diagnostic: Option<Diagnostic>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    status: Option<ProcessStatus>,
}

// Only closed classifications and numbers cross the diagnostic wire boundary.
#[derive(Clone, Debug, Serialize, Deserialize)]
enum Diagnostic {
    External,
    Io {
        kind: IoCause,
        os_code: Option<i32>,
    },
    Json {
        category: JsonCategory,
        line: usize,
        column: usize,
    },
    Timeout,
    ProcessExit {
        success: bool,
        code: Option<i32>,
        #[serde(default)]
        signal: Option<i32>,
    },
    ManagementRejected,
    Audit {
        phase: AuditPhase,
        cause: AuditCause,
    },
    GatewayUnavailable,
    BotStopped,
    UnknownCommand,
    InvalidArguments,
    InvalidPort,
    PortMismatch,
    InvalidReply,
    MissingBootstrapPipe,
    TokenMissing,
    StartupUnconfirmed,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
enum AuditPhase {
    Authorization,
    BeforeExecution,
    AfterExecution,
    Operation,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
enum AuditCause {
    Unauthenticated,
    UnknownRole,
    InsufficientPermission,
    UnknownOperation,
    ConfirmationRequired,
    InvalidSubject,
    InvalidTarget,
    InvalidConfig,
    Io,
    CorruptLog,
    RecordTooLarge,
    CapacityExhausted,
    Busy,
    Poisoned,
    Clock,
    OperationFailed,
}
fn audit_storage_cause(error: src::rbac_audit::AuditError) -> AuditCause {
    use src::rbac_audit::AuditError as E;
    match error {
        E::InvalidConfig => AuditCause::InvalidConfig,
        E::Io => AuditCause::Io,
        E::CorruptLog => AuditCause::CorruptLog,
        E::RecordTooLarge => AuditCause::RecordTooLarge,
        E::CapacityExhausted => AuditCause::CapacityExhausted,
        E::Busy => AuditCause::Busy,
        E::Poisoned => AuditCause::Poisoned,
        E::Clock => AuditCause::Clock,
    }
}
fn audit_diagnostic(error: src::rbac_audit::ExecuteError) -> Diagnostic {
    use src::rbac_audit::{Denial as D, ExecuteError as E};
    let (phase, cause) = match error {
        E::Denied(reason) => (
            AuditPhase::Authorization,
            match reason {
                D::Unauthenticated => AuditCause::Unauthenticated,
                D::UnknownRole => AuditCause::UnknownRole,
                D::InsufficientPermission => AuditCause::InsufficientPermission,
                D::UnknownOperation => AuditCause::UnknownOperation,
                D::ConfirmationRequired => AuditCause::ConfirmationRequired,
                D::InvalidSubject => AuditCause::InvalidSubject,
                D::InvalidTarget => AuditCause::InvalidTarget,
            },
        ),
        E::Audit(reason) => (AuditPhase::BeforeExecution, audit_storage_cause(reason)),
        E::AuditAfterExecution(reason) => (AuditPhase::AfterExecution, audit_storage_cause(reason)),
        E::OperationFailed => (AuditPhase::Operation, AuditCause::OperationFailed),
    };
    Diagnostic::Audit { phase, cause }
}
impl std::fmt::Display for Diagnostic {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::GatewayUnavailable => write!(f, "Gateway unavailable [{self:?}]"),
            Self::ProcessExit { .. } => write!(f, "Botが終了しました [{self:?}]"),
            _ => write!(f, "management_failed cause={self:?}"),
        }
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
enum JsonCategory {
    Io,
    Syntax,
    Data,
    Eof,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
enum IoCause {
    NotFound,
    PermissionDenied,
    ConnectionRefused,
    ConnectionReset,
    ConnectionAborted,
    NotConnected,
    AddrInUse,
    AddrNotAvailable,
    BrokenPipe,
    AlreadyExists,
    WouldBlock,
    InvalidInput,
    InvalidData,
    TimedOut,
    WriteZero,
    Interrupted,
    UnexpectedEof,
    Unsupported,
    Other,
}
fn io_cause(kind: std::io::ErrorKind) -> IoCause {
    use std::io::ErrorKind as K;
    match kind {
        K::NotFound => IoCause::NotFound,
        K::PermissionDenied => IoCause::PermissionDenied,
        K::ConnectionRefused => IoCause::ConnectionRefused,
        K::ConnectionReset => IoCause::ConnectionReset,
        K::ConnectionAborted => IoCause::ConnectionAborted,
        K::NotConnected => IoCause::NotConnected,
        K::AddrInUse => IoCause::AddrInUse,
        K::AddrNotAvailable => IoCause::AddrNotAvailable,
        K::BrokenPipe => IoCause::BrokenPipe,
        K::AlreadyExists => IoCause::AlreadyExists,
        K::WouldBlock => IoCause::WouldBlock,
        K::InvalidInput => IoCause::InvalidInput,
        K::InvalidData => IoCause::InvalidData,
        K::TimedOut => IoCause::TimedOut,
        K::WriteZero => IoCause::WriteZero,
        K::Interrupted => IoCause::Interrupted,
        K::UnexpectedEof => IoCause::UnexpectedEof,
        K::Unsupported => IoCause::Unsupported,
        _ => IoCause::Other,
    }
}
#[derive(Serialize, Deserialize)]
struct ProcessStatus {
    core_pid: u32,
    bot_pid: Option<u32>,
    restarts: u8,
    last_failure: Option<Diagnostic>,
}
#[derive(Debug)]
struct ProtectedFailure(Diagnostic);
impl std::fmt::Display for ProtectedFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {MASK}", self.0)
    }
}
impl Error for ProtectedFailure {}
fn diagnostic(error: &(dyn Error + 'static)) -> Diagnostic {
    if let Some(error) = error.downcast_ref::<ProtectedFailure>() {
        return error.0.clone();
    }
    if let Some(error) = error.downcast_ref::<std::io::Error>() {
        return Diagnostic::Io {
            kind: io_cause(error.kind()),
            os_code: error.raw_os_error(),
        };
    }
    if let Some(error) = error.downcast_ref::<serde_json::Error>() {
        return Diagnostic::Json {
            category: match error.classify() {
                serde_json::error::Category::Io => JsonCategory::Io,
                serde_json::error::Category::Syntax => JsonCategory::Syntax,
                serde_json::error::Category::Data => JsonCategory::Data,
                serde_json::error::Category::Eof => JsonCategory::Eof,
            },
            line: error.line(),
            column: error.column(),
        };
    }
    if error.is::<tokio::time::error::Elapsed>() {
        return Diagnostic::Timeout;
    }
    if let Some(error) = error.downcast_ref::<src::rbac_audit::ExecuteError>() {
        return audit_diagnostic(*error);
    }
    if let Some(error) = error.downcast_ref::<src::rbac_audit::AuditError>() {
        return Diagnostic::Audit {
            phase: AuditPhase::BeforeExecution,
            cause: audit_storage_cause(*error),
        };
    }
    Diagnostic::External
}
fn protected_error(error: &(dyn Error + 'static)) -> String {
    if let Some(error) = error.downcast_ref::<ProtectedFailure>() {
        return error.to_string();
    }
    if error.is::<src::rbac_audit::AuditError>() || error.is::<src::rbac_audit::ExecuteError>() {
        return diagnostic(error).to_string();
    }
    error_diagnostic(error)
}
fn failure(reason: Diagnostic) -> ErrorBox {
    Box::new(ProtectedFailure(reason))
}
fn public_instance(value: &str) -> String {
    if value.len() == 32
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        value.to_owned()
    } else {
        MASK.into()
    }
}
fn public_bot_id(value: &str) -> String {
    match value.parse::<u64>() {
        Ok(id) if id.to_string() == value => id.to_string(),
        _ => MASK.into(),
    }
}
fn process_diagnostic(status: std::process::ExitStatus) -> Diagnostic {
    #[cfg(unix)]
    let signal = {
        use std::os::unix::process::ExitStatusExt;
        status.signal()
    };
    #[cfg(not(unix))]
    let signal = None;
    Diagnostic::ProcessExit {
        success: status.success(),
        code: status.code(),
        signal,
    }
}
fn report_output(context: &'static str, output: &std::process::Output) -> Diagnostic {
    let reason = process_diagnostic(output.status);
    eprintln!("{context} output: {MASK}; {reason:?}");
    reason
}
fn status_message(status: &ProcessStatus) -> String {
    let bot = status.bot_pid.map_or_else(
        || "Botプロセス: 停止中".to_owned(),
        |pid| format!("Botプロセス: 稼働中 (PID: {pid})"),
    );
    format!(
        "コア: 稼働中 (PID: {})\n{bot}\nDiscord接続: Botのログで確認してください\n直近のエラー: {}\n自動再起動: {}/3",
        status.core_pid,
        status
            .last_failure
            .as_ref()
            .map_or_else(|| "なし".to_owned(), ToString::to_string),
        status.restarts
    )
}

fn reply_message(command: &str, reply: &Reply) -> Result<String, ErrorBox> {
    if !reply.ok {
        return Err(Box::new(ProtectedFailure(
            reply
                .diagnostic
                .clone()
                .unwrap_or(Diagnostic::ManagementRejected),
        )));
    }
    match command {
        "status" if reply.status.is_some() => Ok(status_message(reply.status.as_ref().unwrap())),
        "logs" => Ok(format!("{MASK} [log_contents_suppressed]")),
        "metrics-json" => protected_metrics_json(&reply.message),
        "metrics-prometheus" => Ok(protect_prometheus(&reply.message)),
        "list-unknown" => {
            let (header, data) = reply
                .message
                .split_once('\n')
                .ok_or_else(|| failure(Diagnostic::InvalidReply))?;
            let timestamp = header
                .strip_prefix("snapshot_unix_seconds=")
                .and_then(|value| value.split_once(';'))
                .and_then(|(number, _)| number.parse::<u64>().ok());
            let mut snapshot: src::local_control::ListResponse = serde_json::from_str(data)?;
            snapshot.instance = public_instance(&snapshot.instance);
            let header = timestamp.map_or_else(
                || "volatile=true; state=Unknown".to_owned(),
                |timestamp| {
                    format!("snapshot_unix_seconds={timestamp}; volatile=true; state=Unknown")
                },
            );
            Ok(format!("{header}\n{}", serde_json::to_string(&snapshot)?))
        }
        _ => Ok(format!("management_ok: {MASK}")),
    }
}

fn protected_metrics_json(input: &str) -> Result<String, ErrorBox> {
    let snapshots: Vec<src::metrics::StatusSnapshot> = serde_json::from_str(input)?;
    let mut value = serde_json::to_value(snapshots)?;
    for snapshot in value.as_array_mut().unwrap() {
        let label = snapshot["labels"]["bot_id"].as_str().unwrap_or_default();
        snapshot["labels"]["bot_id"] = public_bot_id(label).into();
    }
    Ok(serde_json::to_string(&value)?)
}

fn public_metric_labels(sample: &str) -> String {
    let Some((_, labels)) = sample.split_once('{') else {
        return "redacted=\"true\"".into();
    };
    let Some(labels) = labels.strip_suffix('}') else {
        return "redacted=\"true\"".into();
    };
    let mut safe = std::collections::BTreeMap::new();
    for field in labels.split(',') {
        let Some((name, value)) = field.split_once('=') else {
            continue;
        };
        let Some(value) = value
            .strip_prefix('"')
            .and_then(|value| value.strip_suffix('"'))
        else {
            continue;
        };
        let value = match name {
            "bot_id" => Some(public_bot_id(value)),
            "shard_id" => value.parse::<u32>().ok().map(|id| id.to_string()),
            "generation_id" => value.parse::<u64>().ok().map(|id| id.to_string()),
            "component" if matches!(value, "gateway" | "core") => Some(value.into()),
            "role" if matches!(value, "active" | "standby" | "draining") => Some(value.into()),
            "le" if value == "+Inf" => Some("+Inf".into()),
            "le" => value
                .parse::<f64>()
                .ok()
                .filter(|number| number.is_finite())
                .map(|number| number.to_string()),
            _ => None,
        };
        if let Some(value) = value {
            safe.insert(name, value);
        }
    }
    if safe.is_empty() {
        return "redacted=\"true\"".into();
    }
    safe.into_iter()
        .map(|(name, value)| format!("{name}=\"{value}\""))
        .collect::<Vec<_>>()
        .join(",")
}

// Untrusted labels and comments are never echoed. Preserve known metric names
// and numeric samples; rejected lines receive one fixed marker.
fn protect_prometheus(input: &str) -> String {
    const NAMES: &[&str] = &[
        "met_events_total",
        "met_resumes_total",
        "met_heartbeat_rtt_seconds",
        "met_queue_depth",
        "met_queue_oldest_age_seconds",
        "met_processing_tasks",
        "met_ipc_latency_seconds_bucket",
        "met_ipc_latency_seconds_count",
        "met_ipc_latency_seconds_sum",
        "met_http_429_total",
        "met_http_5xx_total",
        "met_interaction_initial_response_deadline_exceeded_total",
        "met_active_changes_total",
        "met_runtime_generation",
        "met_ws_connected",
        "met_failover_elapsed_seconds",
        "met_failover_in_progress",
        "met_failback_elapsed_seconds",
        "met_failback_in_progress",
    ];
    let mut output = MetricsRegistry::default().render_prometheus();
    for line in input.lines() {
        if line.starts_with('#') || line.is_empty() {
            continue;
        }
        let Some((sample, value)) = line.rsplit_once(' ') else {
            output.push_str("# [REDACTED]\n");
            continue;
        };
        let name = sample.split('{').next().unwrap_or("");
        if NAMES.contains(&name) && value.parse::<f64>().is_ok() {
            // Reformat the number too: never echo its original spelling.
            output.push_str(&format!(
                "{name}{{{}}} {}\n",
                public_metric_labels(sample),
                value.parse::<f64>().unwrap()
            ));
        } else {
            output.push_str("# [REDACTED]\n");
        }
    }
    output
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
        .transpose()
        .map_err(|_| failure(Diagnostic::InvalidPort))?
        .unwrap_or(DEFAULT_PORT))
}
fn token() -> Result<String, ErrorBox> {
    let mut random = [0u8; 32];
    getrandom::fill(&mut random)?;
    Ok(random.iter().map(|b| format!("{b:02x}")).collect())
}
async fn write_endpoint(endpoint: &Endpoint, name: &str) -> Result<(), ErrorBox> {
    let dir = runtime_dir();
    tokio::fs::create_dir_all(&dir).await?;
    let temp = dir.join(format!("{name}-{}.tmp", std::process::id()));
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
    std::fs::rename(temp, dir.join(name))?;
    Ok(())
}
async fn request(command: &str) -> Result<String, ErrorBox> {
    let endpoint: Endpoint =
        serde_json::from_slice(&tokio::fs::read(runtime_dir().join("core.json")).await?)?;
    if endpoint.port != port()? {
        return Err(failure(Diagnostic::PortMismatch));
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
    .ok_or_else(|| failure(Diagnostic::InvalidReply))?;
    let reply: Reply = serde_json::from_slice(&line)?;
    reply_message(command, &reply)
}
async fn build() -> Result<PathBuf, ErrorBox> {
    let target = root().join("target").join("reload-build");
    println!("Botをビルドしています...");
    let result = Command::new(env::var_os("CARGO").unwrap_or_else(|| "cargo".into()))
        .current_dir(root())
        .args([
            "build",
            "--offline",
            "--release",
            "--bin",
            "src",
            "--target-dir",
        ])
        .arg(&target)
        .env_remove("DISCORD_BUILD_ID")
        .env_remove("DISCORD_TOKEN")
        .output()
        .await?;
    report_output("build", &result);
    if !result.status.success() {
        return Err(Box::new(ProtectedFailure(process_diagnostic(
            result.status,
        ))));
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
        // Arbitrary executables may print unlabeled credentials. The parent
        // diagnoses lifecycle failures from exit status, never inherited text.
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .kill_on_drop(true)
        .spawn()?;
    control
        .bootstrap(
            child
                .stdin
                .take()
                .ok_or_else(|| failure(Diagnostic::MissingBootstrapPipe))?,
        )
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
    let secret = Arc::new(Secret::new(token()?));
    let endpoint = Endpoint {
        port: listener.local_addr()?.port(),
        secret: Secret::new(secret.expose_secret().clone()),
    };
    let viewer_secret = Arc::new(Secret::new(token()?));
    let viewer_endpoint = Endpoint {
        port: endpoint.port,
        secret: Secret::new(viewer_secret.expose_secret().clone()),
    };
    tokio::fs::create_dir_all(runtime_dir()).await?;
    let mut audit =
        AuditedExecutor::open(runtime_dir().join("audit.jsonl"), AuditConfig::default())?;
    let metrics = MetricsRegistry::default();
    let mut bot = Some(spawn_bot(&binary).await?);
    let mut desired_running = true;
    let mut failures = 0u8;
    let mut last_failure: Option<Diagnostic> = None;
    let mut started = tokio::time::Instant::now();
    let mut supervision = tokio::time::interval(Duration::from_secs(2));
    let readers = Arc::new(Semaphore::new(32));
    let (requests_tx, mut requests_rx) =
        mpsc::channel::<(Option<Request>, Option<&'static str>, BufReader<TcpStream>)>(32);
    write_endpoint(&endpoint, "core.json").await?;
    write_endpoint(&viewer_endpoint, "viewer.json").await?;
    println!("コアを起動しました。BotのDiscord接続状態はログで確認してください。");
    loop {
        let (input, role, mut reader) = tokio::select! {
            accepted = listener.accept() => {
                let (stream, _) = accepted?;
                let Ok(permit) = readers.clone().try_acquire_owned() else { continue };
                let sender = requests_tx.clone();
                let expected_secret = secret.clone();
                let expected_viewer = viewer_secret.clone();
                tokio::spawn(async move {
                    let _permit = permit;
                    let mut reader = BufReader::new(stream);
                    let Ok(Ok(Some(line))) = tokio::time::timeout(
                        Duration::from_secs(3), read_frame(&mut reader, MAX_REQUEST_BYTES)
                    ).await else {
                        let _ = sender.try_send((None, None, reader));
                        return;
                    };
                    let input = serde_json::from_slice::<Request>(&line).ok();
                    let role = input.as_ref().and_then(|input| {
                        let candidate = input.secret.expose_secret().as_bytes();
                        if bool::from(candidate.ct_eq(expected_secret.expose_secret().as_bytes())) { Some("admin") }
                        else if bool::from(candidate.ct_eq(expected_viewer.expose_secret().as_bytes())) { Some("viewer") }
                        else { None }
                    });
                    let _ = sender.try_send((input, role, reader));
                });
                continue;
            },
            Some(request) = requests_rx.recv() => request,
            _ = supervision.tick() => {
                if let Some(process) = bot.as_mut() {
                    if let Some(status) = process.try_wait()? {
                        let reason = process_diagnostic(status);
                        eprintln!("Botが終了しました: {reason:?}"); last_failure = Some(reason); bot = None;
                    } else if started.elapsed() >= Duration::from_secs(60) { failures = 0; }
                }
                if desired_running && bot.is_none() && failures < 3 {
                    failures += 1;
                    match spawn_bot(&binary).await {
                        Ok(process) => { eprintln!("Botを自動再起動しました ({failures}/3)"); bot = Some(process); started = tokio::time::Instant::now(); }
                        Err(error) => { eprintln!("Botの自動再起動に失敗しました: {}", protected_error(error.as_ref())); last_failure = Some(diagnostic(error.as_ref())); }
                    }
                }
                continue;
            }
        };
        let command = input.as_ref().map(|v| v.command.as_str()).unwrap_or("");
        let operation = match command {
            "status" | "metrics-json" | "metrics-prometheus" => "status",
            "logs" => "logs",
            "list-unknown" => "inspect",
            "start" | "restart" | "update" => "update",
            "stop" => "stop",
            "shutdown" => "shutdown",
            _ => "unknown",
        };
        let audit_request = AuditRequest {
            principal: role.map_or_else(Principal::unauthenticated, |r| {
                Principal::authenticated("local-capability", r)
            }),
            operation: operation.into(),
            bot_id: Some("local-bot".into()),
            shard_id: None,
            remote: false,
            confirmation: Confirmation::None,
        };
        if role.is_none() {
            let _ = audit.execute(&audit_request, || Ok(()));
            continue;
        }
        let input = input.expect("authenticated request");
        if let Some(process) = bot.as_mut()
            && let Some(status) = process.try_wait()?
        {
            println!("Bot終了: {status}");
            last_failure = Some(process_diagnostic(status));
            bot = None;
        }
        let mut exit = false;
        let mut operation_error = None;
        let action = audit.execute_async(&audit_request, async {
            let result: Result<String, ErrorBox> = async {
            match input.command.as_str() {
                "list-unknown" => {
                    let bot = bot.as_mut().ok_or_else(|| failure(Diagnostic::GatewayUnavailable))?;
                    let mut reply = bot.control.list().await.map_err(|_| failure(Diagnostic::GatewayUnavailable))?;
                    reply.instance = public_instance(&reply.instance);
                    let timestamp = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
                    Ok(format!("snapshot_unix_seconds={timestamp}; volatile=true; state=Unknown\n{}", serde_json::to_string(&reply)?))
                }
                "logs" => {
                    let bytes = tokio::fs::metadata(runtime_dir().join("core.log")).await?.len();
                    Ok(format!("{MASK} [log_contents_suppressed] bytes={bytes}"))
                },
                "metrics-json" => protected_metrics_json(&serde_json::to_string(&metrics.snapshots())?),
                "metrics-prometheus" => Ok(protect_prometheus(&metrics.render_prometheus())),
                "status" => Ok(status_message(&ProcessStatus {
                    core_pid: std::process::id(), bot_pid: bot.as_ref().and_then(|process| process.id()),
                    restarts: failures, last_failure: last_failure.clone(),
                })),
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
                    if bot.is_none() { return Err(failure(Diagnostic::BotStopped)); }
                    let output = Command::new(env::var_os("CARGO").unwrap_or_else(|| "cargo".into())).current_dir(root())
                        .args(["run", "--offline", "--bin", "reload", "--", "general"]).env_remove("DISCORD_TOKEN").output().await?;
                    let reason = report_output("reload", &output);
                    if !output.status.success() { return Err(Box::new(ProtectedFailure(reason)) as ErrorBox); }
                    Ok("更新が完了しました".into())
                }
                "shutdown" => { desired_running = false; stop_bot(&mut bot).await?; exit = true; Ok("Botとコアを停止しました。メモリ内の状態は失われます。".into()) }
                _ => Err(failure(Diagnostic::UnknownCommand)),
            }
            }.await;
            result.map_err(|error| { operation_error = Some(diagnostic(error.as_ref())); "operation_failed".to_owned() })
        }).await;
        let reply = match action {
            Ok(message) => Reply {
                ok: true,
                message,
                diagnostic: None,
                status: (input.command == "status").then(|| ProcessStatus {
                    core_pid: std::process::id(),
                    bot_pid: bot.as_ref().and_then(|process| process.id()),
                    restarts: failures,
                    last_failure: last_failure.clone(),
                }),
            },
            Err(error) => {
                let reason = operation_error.unwrap_or_else(|| {
                    // ExecuteError has only closed enum causes and no user text.
                    eprintln!("management audit: {error}");
                    audit_diagnostic(error)
                });
                let message = ProtectedFailure(reason.clone()).to_string();
                eprintln!("{message}");
                last_failure = Some(reason.clone());
                Reply {
                    ok: false,
                    message,
                    diagnostic: Some(reason),
                    status: None,
                }
            }
        };
        let mut data = serde_json::to_vec(&reply)?;
        data.push(b'\n');
        if data.len() > 64 * 1024 {
            data = b"{\"ok\":false,\"message\":\"response exceeds limit\"}\n".to_vec();
        }
        let _ =
            tokio::time::timeout(Duration::from_secs(3), reader.get_mut().write_all(&data)).await;
        if exit {
            break;
        }
    }
    let _ = tokio::fs::remove_file(runtime_dir().join("core.json")).await;
    let _ = tokio::fs::remove_file(runtime_dir().join("viewer.json")).await;
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
fn main() {
    src::redact::install_panic_hook();
    let result = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .map_err(|error| Box::new(error) as ErrorBox)
        .and_then(|runtime| runtime.block_on(run()));
    if let Err(error) = result {
        eprintln!("manage: {}", protected_error(error.as_ref()));
        std::process::exit(1);
    }
}

async fn run() -> Result<(), ErrorBox> {
    let args: Vec<_> = env::args_os().skip(1).collect();
    let command = args.first().and_then(|s| s.to_str()).unwrap_or("help");
    if command == "__core" {
        return core(
            args.get(1)
                .ok_or_else(|| failure(Diagnostic::InvalidArguments))?
                .into(),
        )
        .await;
    }
    if args.len() > 1 {
        return Err(failure(Diagnostic::InvalidArguments));
    }
    match command {
        "start" => {
            if request("status").await.is_ok() {
                println!("{}", request("start").await?);
                return Ok(());
            }
            if env::var_os("DISCORD_TOKEN").is_none_or(|v| v.is_empty()) {
                return Err(failure(Diagnostic::TokenMissing));
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
                    return Err(Box::new(ProtectedFailure(Diagnostic::ManagementRejected)));
                }
            }
            Err(failure(Diagnostic::StartupUnconfirmed))
        }
        "list-unknown" | "status" | "metrics-json" | "metrics-prometheus" | "logs" | "stop"
        | "restart" | "update" | "shutdown" => match request(command).await {
            Ok(message) => {
                println!("{message}");
                Ok(())
            }
            Err(error) => Err(Box::new(ProtectedFailure(diagnostic(error.as_ref())))),
        },
        _ => {
            println!(
                "使い方: cargo run --bin manage -- <start|list-unknown|status|metrics-json|metrics-prometheus|stop|restart|update|logs|shutdown>"
            );
            Ok(())
        }
    }
}

#[cfg(test)]
mod request_tests {
    use super::*;

    const FIXTURE: &str = "short-token-input short-secret-input short-key-input Authorization: Bearer short-header-input";

    fn fixture_process(mode: &str) -> std::process::Command {
        let mut command = std::process::Command::new(env::current_exe().unwrap());
        command
            .args([
                "--exact",
                "request_tests::output_fixture",
                "--nocapture",
                "--test-threads=1",
            ])
            .env("MANAGE_OUTPUT_FIXTURE", mode);
        command
    }

    #[test]
    fn output_fixture() {
        let Some(mode) = env::var_os("MANAGE_OUTPUT_FIXTURE") else {
            return;
        };
        src::redact::install_panic_hook();
        match mode.to_str().unwrap() {
            "raw" => {
                println!("{FIXTURE}");
                eprintln!("{FIXTURE}");
                std::process::exit(23);
            }
            "child" => {
                let output = fixture_process("raw").output().unwrap();
                report_output("reload", &output);
                let failure = ProtectedFailure(process_diagnostic(output.status));
                eprintln!("{failure}");
                let state = ProcessStatus {
                    core_pid: 1,
                    bot_pid: None,
                    restarts: 2,
                    last_failure: Some(failure.0),
                };
                println!("{}", serde_json::to_string(&state).unwrap());
                std::process::exit(1);
            }
            "reply" => {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .unwrap();
                match runtime.block_on(request("status")) {
                    Ok(message) => println!("{message}"),
                    Err(error) => {
                        eprintln!("manage: {}", protected_error(error.as_ref()));
                        std::process::exit(1);
                    }
                }
            }
            "io" => {
                let error = std::io::Error::new(std::io::ErrorKind::PermissionDenied, FIXTURE);
                eprintln!("{}", protected_error(&error));
                println!("{}", ProtectedFailure(diagnostic(&error)));
            }
            _ => unreachable!(),
        }
    }

    fn safe_output(output: &std::process::Output) -> String {
        let text = format!(
            "{}{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        for secret in [
            "short-token-input",
            "short-secret-input",
            "short-key-input",
            "short-header-input",
        ] {
            assert!(
                !text.contains(secret),
                "process output leaked fixture credential"
            );
        }
        text
    }

    #[test]
    fn subprocess_child_output_and_saved_diagnostics_hide_all_free_text() {
        let output = fixture_process("child").output().unwrap();
        assert_eq!(output.status.code(), Some(1));
        let text = safe_output(&output);
        assert!(text.contains("ProcessExit"));
        assert!(text.contains("23"));
        assert!(text.contains(MASK));
        let text = safe_output(&fixture_process("io").output().unwrap());
        assert!(text.contains("PermissionDenied"));
    }

    #[test]
    fn public_identifiers_and_metric_values_survive_without_free_text() {
        let instance = "0123456789abcdef0123456789abcdef";
        assert_eq!(public_instance(instance), instance);
        assert_eq!(public_instance(FIXTURE), MASK);
        assert_eq!(public_bot_id("101"), "101");
        assert_eq!(public_bot_id("label-private"), MASK);
        assert_eq!(public_bot_id("00101"), MASK);
        let reply = Reply {
            ok: true,
            message: format!(
                "snapshot_unix_seconds=12; volatile=true; state=Unknown\n{}",
                serde_json::json!({"instance":instance,"unknown":[[2,17]]})
            ),
            diagnostic: None,
            status: None,
        };
        let message = reply_message("list-unknown", &reply).unwrap();
        assert!(message.contains(instance));
        assert!(message.contains("[2,17]"));
        let reply = Reply {
            message: format!(
                "untrusted-header\n{}",
                serde_json::json!({"instance":FIXTURE,"unknown":[[2,17]]})
            ),
            ..reply
        };
        assert!(
            !reply_message("list-unknown", &reply)
                .unwrap()
                .contains("short-token-input")
        );
        let text = protect_prometheus(
            "met_events_total{bot_id=\"label-private\",shard_id=\"2\",generation_id=\"7\"} 19\nmet_events_total{bot_id=\"101\",shard_id=\"3\"} 20\n",
        );
        assert!(!text.contains("label-private"));
        assert!(text.contains("bot_id=\"101\""));
        assert!(text.contains("shard_id=\"2\""));
        assert!(text.contains("generation_id=\"7\""));
        assert!(text.contains(" 19\n"));
    }

    #[test]
    fn audit_reply_preserves_closed_causes_and_execution_phase() {
        use src::rbac_audit::{AuditError as A, Denial as D, ExecuteError as E};
        for (denial, expected) in [
            (D::Unauthenticated, "Unauthenticated"),
            (D::UnknownRole, "UnknownRole"),
            (D::InsufficientPermission, "InsufficientPermission"),
            (D::UnknownOperation, "UnknownOperation"),
            (D::ConfirmationRequired, "ConfirmationRequired"),
            (D::InvalidSubject, "InvalidSubject"),
            (D::InvalidTarget, "InvalidTarget"),
        ] {
            let reply = Reply {
                ok: false,
                message: FIXTURE.into(),
                diagnostic: Some(audit_diagnostic(E::Denied(denial))),
                status: None,
            };
            let reply: Reply =
                serde_json::from_slice(&serde_json::to_vec(&reply).unwrap()).unwrap();
            let error = reply_message("status", &reply).unwrap_err();
            let text = protected_error(error.as_ref());
            assert!(text.contains("Authorization"));
            assert!(text.contains(expected));
            assert!(!text.contains(FIXTURE));
        }
        for (cause, expected) in [
            (A::InvalidConfig, "InvalidConfig"),
            (A::Io, "Io"),
            (A::CorruptLog, "CorruptLog"),
            (A::RecordTooLarge, "RecordTooLarge"),
            (A::CapacityExhausted, "CapacityExhausted"),
            (A::Busy, "Busy"),
            (A::Poisoned, "Poisoned"),
            (A::Clock, "Clock"),
        ] {
            for (error, phase) in [
                (E::Audit(cause), "BeforeExecution"),
                (E::AuditAfterExecution(cause), "AfterExecution"),
            ] {
                let text = protected_error(&error);
                assert!(text.contains(expected));
                assert!(text.contains(phase));
                assert!(!text.contains(FIXTURE));
            }
        }
        assert!(protected_error(&E::OperationFailed).contains("OperationFailed"));
        let text = protected_error(&std::io::Error::other(FIXTURE));
        assert!(!text.contains(FIXTURE));
        assert!(
            ProtectedFailure(Diagnostic::GatewayUnavailable)
                .to_string()
                .contains("Gateway unavailable")
        );
        let status = ProcessStatus {
            core_pid: 1,
            bot_pid: None,
            restarts: 3,
            last_failure: Some(Diagnostic::ProcessExit {
                success: false,
                code: Some(23),
                signal: None,
            }),
        };
        let message = status_message(&status);
        assert!(message.contains("直近のエラー: Botが終了しました"));
        assert!(message.contains("23"));
    }

    #[test]
    fn subprocess_management_reply_and_json_errors_never_echo_wire_text() {
        use std::io::{BufRead, Write};
        let directory = env::temp_dir().join(format!(
            "manage-output-{}-{}",
            std::process::id(),
            getrandom::u64().unwrap()
        ));
        std::fs::create_dir(&directory).unwrap();
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        std::fs::write(
            directory.join("core.json"),
            serde_json::to_vec(&serde_json::json!({"port":port,"secret":FIXTURE})).unwrap(),
        )
        .unwrap();
        let server = std::thread::spawn(move || {
            for ok in [true, false] {
                let (mut socket, _) = listener.accept().unwrap();
                socket
                    .set_read_timeout(Some(Duration::from_secs(10)))
                    .unwrap();
                let mut input = String::new();
                std::io::BufReader::new(socket.try_clone().unwrap())
                    .read_line(&mut input)
                    .unwrap();
                let value: serde_json::Value = serde_json::from_str(&input).unwrap();
                assert_eq!(value["secret"], FIXTURE);
                writeln!(socket, "{}", serde_json::json!({"ok":ok,"message":FIXTURE})).unwrap();
            }
        });
        for success in [true, false] {
            let output = fixture_process("reply")
                .env("DISCORD_CORE_DIR", &directory)
                .env("DISCORD_CORE_PORT", port.to_string())
                .output()
                .unwrap();
            assert_eq!(output.status.success(), success);
            let text = safe_output(&output);
            assert!(text.contains(MASK));
            assert!(text.contains(if success {
                "management_ok"
            } else {
                "ManagementRejected"
            }));
        }
        server.join().unwrap();
        std::fs::write(
            directory.join("core.json"),
            serde_json::to_vec(&serde_json::json!({"port":FIXTURE,"secret":FIXTURE})).unwrap(),
        )
        .unwrap();
        let output = fixture_process("reply")
            .env("DISCORD_CORE_DIR", &directory)
            .output()
            .unwrap();
        assert_eq!(output.status.code(), Some(1));
        assert!(safe_output(&output).contains("json_error category=Data"));
        std::fs::remove_file(directory.join("core.json")).unwrap();
        std::fs::remove_dir(directory).unwrap();
    }

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
