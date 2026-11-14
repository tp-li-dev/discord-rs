//! Local authenticated daemon control. Bootstrap capability arrives on anonymous stdin
//! (`--bootstrap-stdin`) or from a permission-checked file (`--secret-file`, for systemd).
//! The TCP listener is always loopback; it is not a remote management endpoint. Its port
//! is random unless `--control-port` is given (only with `--secret-file`).
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use src::daemon::control::{CommandRequest, ControlPlane, NOTE};
use src::daemon::update_control::{ControlAccess, UpdateHub};
use src::{cluster::NodeId, cluster_runtime::RaftRuntimeOptions, raft_quorum::StorageMode};
use src::{local_control::DaemonControlEnvelope, update_rollback::UpdateControlRequest};
use std::{
    collections::BTreeMap, ffi::OsString, io, net::SocketAddr, path::PathBuf, time::Duration,
};
use subtle::ConstantTimeEq;
use tokio::{
    io::{AsyncBufReadExt, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader},
    net::TcpListener,
    sync::{mpsc, oneshot},
    task::JoinSet,
};

const REQUEST_LIMIT: u64 = 4096;
const RESPONSE_LIMIT: usize = 64 * 1024;
const DEADLINE: Duration = Duration::from_secs(3);
const USAGE: &str = "usage: botd --file F --runtime-root ABS --gateway-program ABS (--bootstrap-stdin | --secret-file ABS [--control-port PORT]) [--no-autostart] [--mock-discord LOOPBACK_IP:PORT] [--allow-test-quorum] [--allow-raft-quorum --raft-store ABS --raft-rpc-listen LOOPBACK_IP:PORT --raft-rpc-peer ID=LOOPBACK_IP:PORT ... [--raft-create] [--raft-bootstrap]] [--allow-test-gateway-switch --mock-identify-budget F]";

struct Options {
    file: PathBuf,
    runtime_root: PathBuf,
    gateway_program: PathBuf,
    no_autostart: bool,
    mock_discord: Option<SocketAddr>,
    allow_test_quorum: bool,
    raft: Option<RaftRuntimeOptions>,
    allow_test_gateway_switch: bool,
    mock_identify_budget: Option<PathBuf>,
    mock_recovery_max_restarts: Option<u32>,
    secret_file: Option<PathBuf>,
    control_port: u16,
}

fn parse_options(args: impl IntoIterator<Item = OsString>) -> Result<Options, &'static str> {
    let mut args = args.into_iter();
    let mut file = None;
    let mut runtime_root = None;
    let mut gateway_program = None;
    let mut bootstrap = false;
    let mut no_autostart = false;
    let mut mock_discord = None;
    let mut allow_test_quorum = false;
    let mut allow_raft_quorum = false;
    let mut raft_store = None;
    let mut raft_rpc_listen = None;
    let mut raft_rpc_peers = BTreeMap::new();
    let mut raft_create = false;
    let mut raft_bootstrap = false;
    let mut allow_test_gateway_switch = false;
    let mut mock_identify_budget = None;
    let mut mock_recovery_max_restarts = None;
    let mut secret_file = None;
    let mut control_port = None;
    while let Some(arg) = args.next() {
        let target = match arg.to_str() {
            Some("--file") if file.is_none() => &mut file,
            Some("--runtime-root") if runtime_root.is_none() => &mut runtime_root,
            Some("--gateway-program") if gateway_program.is_none() => &mut gateway_program,
            Some("--raft-store") if raft_store.is_none() => &mut raft_store,
            Some("--secret-file") if secret_file.is_none() => &mut secret_file,
            Some("--control-port") if control_port.is_none() => {
                let port = args
                    .next()
                    .ok_or(USAGE)?
                    .to_str()
                    .ok_or(USAGE)?
                    .parse::<u16>()
                    .map_err(|_| "invalid --control-port")?;
                if port == 0 {
                    return Err("invalid --control-port");
                }
                control_port = Some(port);
                continue;
            }
            Some("--allow-raft-quorum") if !allow_raft_quorum => {
                allow_raft_quorum = true;
                continue;
            }
            Some("--raft-create") if !raft_create => {
                raft_create = true;
                continue;
            }
            Some("--raft-bootstrap") if !raft_bootstrap => {
                raft_bootstrap = true;
                continue;
            }
            Some("--raft-rpc-listen") if raft_rpc_listen.is_none() => {
                let value = args.next().ok_or(USAGE)?;
                raft_rpc_listen = Some(raft_endpoint(value.to_str().ok_or(USAGE)?)?);
                continue;
            }
            Some("--raft-rpc-peer") => {
                let value = args.next().ok_or(USAGE)?;
                let (id, address) = value.to_str().ok_or(USAGE)?.split_once('=').ok_or(USAGE)?;
                let node: u64 = id.parse().map_err(|_| "invalid Raft peer ID")?;
                if node == 0 || id != node.to_string() || raft_rpc_peers.len() >= 15 {
                    return Err("invalid Raft peer ID or member limit");
                }
                if raft_rpc_peers
                    .insert(NodeId(node), raft_endpoint(address)?)
                    .is_some()
                {
                    return Err("duplicate Raft peer ID");
                }
                continue;
            }
            Some("--mock-identify-budget") if mock_identify_budget.is_none() => {
                &mut mock_identify_budget
            }
            Some("--mock-recovery-max-restarts") if mock_recovery_max_restarts.is_none() => {
                mock_recovery_max_restarts = Some(
                    args.next()
                        .ok_or(USAGE)?
                        .to_str()
                        .ok_or(USAGE)?
                        .parse::<u32>()
                        .map_err(|_| USAGE)?,
                );
                continue;
            }
            Some("--allow-test-gateway-switch") if !allow_test_gateway_switch => {
                allow_test_gateway_switch = true;
                continue;
            }
            Some("--bootstrap-stdin") if !bootstrap => {
                bootstrap = true;
                continue;
            }
            Some("--no-autostart") if !no_autostart => {
                no_autostart = true;
                continue;
            }
            Some("--allow-test-quorum") if !allow_test_quorum => {
                allow_test_quorum = true;
                continue;
            }
            Some("--mock-discord") if mock_discord.is_none() => {
                let value = args.next().ok_or(USAGE)?;
                let address: SocketAddr = value
                    .to_str()
                    .ok_or(USAGE)?
                    .parse()
                    .map_err(|_| "mock Discord requires a loopback IP and nonzero port")?;
                if !address.ip().is_loopback() || address.port() == 0 {
                    return Err("mock Discord requires a loopback IP and nonzero port");
                }
                mock_discord = Some(address);
                continue;
            }
            _ => return Err(USAGE),
        };
        let value = args.next().ok_or(USAGE)?;
        if value.is_empty() || value.to_string_lossy().starts_with('-') {
            return Err(USAGE);
        }
        *target = Some(PathBuf::from(value));
    }
    let raft = if allow_raft_quorum {
        if mock_discord.is_none() {
            return Err("Raft quorum requires --mock-discord; production is disabled");
        }
        if allow_test_quorum || allow_test_gateway_switch || no_autostart {
            return Err("Raft quorum requires exclusive automatic process ownership");
        }
        let storage_path = raft_store.ok_or("Raft quorum requires --raft-store")?;
        if !storage_path.is_absolute()
            || storage_path
                .components()
                .any(|part| part == std::path::Component::ParentDir)
        {
            return Err("Raft store must be an absolute path without parent traversal");
        }
        if raft_bootstrap && !raft_create {
            return Err("Raft bootstrap requires explicit --raft-create");
        }
        let rpc_listen = raft_rpc_listen.ok_or("Raft quorum requires --raft-rpc-listen")?;
        let mut routes = std::collections::BTreeSet::from([rpc_listen]);
        if raft_rpc_peers.len() < 2 || raft_rpc_peers.values().any(|route| !routes.insert(*route)) {
            return Err("Raft quorum requires distinct routes for all peers");
        }
        Some(RaftRuntimeOptions {
            allow_raft: true,
            enable_production: false,
            storage_path,
            storage_mode: if raft_create {
                StorageMode::Create
            } else {
                StorageMode::Open
            },
            bootstrap: raft_bootstrap,
            rpc_listen,
            rpc_peers: raft_rpc_peers,
        })
    } else {
        if raft_store.is_some()
            || raft_rpc_listen.is_some()
            || !raft_rpc_peers.is_empty()
            || raft_create
            || raft_bootstrap
        {
            return Err("Raft settings require --allow-raft-quorum");
        }
        None
    };
    let options = Options {
        file: file.ok_or(USAGE)?,
        runtime_root: runtime_root.ok_or(USAGE)?,
        gateway_program: gateway_program.ok_or(USAGE)?,
        no_autostart,
        mock_discord,
        allow_test_quorum,
        raft,
        allow_test_gateway_switch,
        mock_identify_budget,
        mock_recovery_max_restarts,
        secret_file,
        control_port: control_port.unwrap_or(0),
    };
    if bootstrap && options.secret_file.is_some() {
        return Err("--bootstrap-stdin and --secret-file are mutually exclusive");
    }
    if !bootstrap && options.secret_file.is_none() {
        return Err("anonymous stdin bootstrap or --secret-file is required");
    }
    if options
        .secret_file
        .as_ref()
        .is_some_and(|path| !path.is_absolute())
    {
        return Err("secret file must be an absolute path");
    }
    if control_port.is_some() && options.secret_file.is_none() {
        return Err("--control-port requires --secret-file");
    }
    if !options.runtime_root.is_absolute() || !options.gateway_program.is_absolute() {
        return Err("runtime root and gateway program must be absolute paths");
    }
    if options.allow_test_gateway_switch {
        if options.mock_discord.is_none() {
            return Err("test Gateway switch requires --mock-discord");
        }
        if options.mock_identify_budget.is_none() {
            return Err("test Gateway switch requires --mock-identify-budget");
        }
        if options.allow_test_quorum || options.no_autostart {
            return Err("test Gateway switch requires standalone automatic startup");
        }
    } else if options.mock_identify_budget.is_some() || options.mock_recovery_max_restarts.is_some()
    {
        return Err("mock Identify budget requires --allow-test-gateway-switch");
    }
    Ok(options)
}

fn raft_endpoint(value: &str) -> Result<SocketAddr, &'static str> {
    let address: SocketAddr = value
        .parse()
        .map_err(|_| "invalid Raft loopback endpoint")?;
    if !address.ip().is_loopback() || address.port() == 0 {
        return Err("Raft RPC requires a loopback IP and nonzero port");
    }
    Ok(address)
}

// Deliberately no Debug: capabilities must never be formatted or logged.
#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Bootstrap {
    secret: [u8; 32],
    #[serde(default, skip_serializing_if = "Option::is_none")]
    access: Option<ControlAccess>,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct StatusRequest {
    secret: [u8; 32],
    command: String,
    #[serde(default)]
    bot: Option<String>,
    #[serde(default)]
    operation_id: Option<String>,
    #[serde(default)]
    force: bool,
    #[serde(default)]
    yes: bool,
}

struct Envelope {
    request: CommandRequest,
    update: Option<UpdateControlRequest>,
    access: ControlAccess,
    response: oneshot::Sender<Value>,
}

// Each alternative is strict, including the flattened update payload. No role,
// subject or scope supplied by a connected client is accepted here.
#[derive(Deserialize)]
#[serde(untagged)]
enum WireRequest {
    Update(Box<DaemonControlEnvelope<UpdateControlRequest>>),
    Command(StatusRequest),
}

async fn read_frame<R: AsyncRead + Unpin>(reader: R) -> io::Result<Vec<u8>> {
    let mut frame = Vec::new();
    BufReader::new(reader)
        .take(REQUEST_LIMIT + 1)
        .read_until(b'\n', &mut frame)
        .await?;
    if frame.is_empty() || frame.len() as u64 > REQUEST_LIMIT || frame.last() != Some(&b'\n') {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid control frame",
        ));
    }
    Ok(frame)
}

async fn read_bootstrap<R: AsyncRead + Unpin>(reader: R) -> io::Result<Bootstrap> {
    tokio::time::timeout(DEADLINE, async {
        let frame = read_frame(reader).await?;
        let bootstrap: Bootstrap = serde_json::from_slice(&frame)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "invalid control bootstrap"))?;
        if bootstrap.secret == [0; 32] || bootstrap.access.as_ref().is_some_and(|a| !a.valid()) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid control bootstrap",
            ));
        }
        Ok(bootstrap)
    })
    .await
    .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "control bootstrap timed out"))?
}

/// Gateway（`src --bot-id`、`main.rs` の `GatewayOptions::parse`）が受け付ける形と同じ規則:
/// 先頭ゼロや符号を含まない正規の 10 進 u64。別名（"001" と "1"）が同じ Bot の識別に化けない
/// よう、Gateway 側が厳密に拒否する。規則を変える場合は両方を同時に変えること。
fn is_canonical_discord_id(id: &str) -> bool {
    id.parse::<u64>()
        .ok()
        .is_some_and(|number| number.to_string() == id)
}

/// secret の取得元。`--secret-file` があれば権限検査つきでファイルから、なければ匿名 stdin から読む。
/// エラーは固定文字列のみ（secret・パス・OS のエラー文字列を含めない）。
async fn load_bootstrap(secret_file: Option<&std::path::Path>) -> Result<Bootstrap, &'static str> {
    match secret_file {
        None => read_bootstrap(tokio::io::stdin())
            .await
            .map_err(|_| "invalid anonymous control bootstrap"),
        Some(path) => {
            let path = path.to_path_buf();
            let secret =
                tokio::task::spawn_blocking(move || src::secret_file::load_secret_file(&path))
                    .await
                    .map_err(|_| "secret file task failed")?
                    .map_err(|error| error.as_str())?;
            Ok(Bootstrap {
                secret: *secret.expose_secret(),
                access: None,
            })
        }
    }
}

#[cfg(test)]
async fn serve_status<S: AsyncRead + AsyncWrite + Unpin>(
    stream: S,
    secret: &[u8; 32],
    commands: &mpsc::Sender<Envelope>,
) -> io::Result<()> {
    serve_control(stream, secret, &ControlAccess::default(), commands).await
}

async fn serve_control<S: AsyncRead + AsyncWrite + Unpin>(
    mut stream: S,
    secret: &[u8; 32],
    access: &ControlAccess,
    commands: &mpsc::Sender<Envelope>,
) -> io::Result<()> {
    tokio::time::timeout(DEADLINE, async {
        let frame = read_frame(&mut stream).await?;
        let request: WireRequest = serde_json::from_slice(&frame)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "invalid status request"))?;
        let received_secret = match &request {
            WireRequest::Update(wire) => &wire.secret,
            WireRequest::Command(wire) => &wire.secret,
        };
        if !bool::from(received_secret.ct_eq(secret)) {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "status request denied",
            ));
        }
        let (command, update) = match request {
            WireRequest::Update(wire) => (
                CommandRequest {
                    command: wire.request.command_name().into(),
                    bot: Some(wire.request.bot.clone()),
                    operation_id: Some(wire.request.operation_id.clone()),
                    force: false,
                    yes: wire.request.yes,
                },
                Some(wire.request),
            ),
            WireRequest::Command(request) => (
                CommandRequest {
                    command: request.command,
                    bot: request.bot,
                    operation_id: request.operation_id,
                    force: request.force,
                    yes: request.yes,
                },
                None,
            ),
        };
        if update.is_none() && !command.valid() {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "control request denied",
            ));
        }
        // Only the daemon owner admits/executes commands. A disconnected client
        // never cancels an already admitted operation or forces a child to exit.
        let (response, receiver) = oneshot::channel();
        let value = if commands
            .try_send(Envelope {
                request: command,
                update,
                access: access.clone(),
                response,
            })
            .is_ok()
        {
            receiver
                .await
                .map_err(|_| io::Error::other("daemon control unavailable"))?
        } else {
            json!({"ok": false, "offline": false, "error": "daemon control busy", "note": NOTE})
        };
        let mut response = encode_response(value)?;
        response.push(b'\n');
        if response.len() > RESPONSE_LIMIT {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "status response exceeds limit",
            ));
        }
        stream.write_all(&response).await?;
        stream.shutdown().await
    })
    .await
    .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "status request timed out"))?
}

// Keep current states even when bounded transition histories accumulate. The
// wire limit is a byte budget, not a count of records; report omitted history.
fn encode_response(mut value: Value) -> io::Result<Vec<u8>> {
    fn trim(value: &mut Value) -> usize {
        match value {
            Value::Object(fields) => {
                let mut removed = 0;
                for (name, value) in fields {
                    if matches!(
                        name.as_str(),
                        "transitions" | "recovery_events" | "recent_events"
                    ) {
                        if let Some(events) = value.as_array_mut() {
                            let count = events.len().div_ceil(2);
                            events.drain(..count);
                            removed += count;
                        }
                    } else {
                        removed += trim(value);
                    }
                }
                removed
            }
            Value::Array(values) => values.iter_mut().map(trim).sum(),
            _ => 0,
        }
    }
    let mut omitted = 0;
    loop {
        let bytes = serde_json::to_vec(&value)?;
        if bytes.len() < RESPONSE_LIMIT {
            return Ok(bytes);
        }
        let removed = trim(&mut value);
        if removed == 0 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "status response exceeds limit",
            ));
        }
        omitted += removed;
        value["recovery_history_omitted"] = json!(omitted);
    }
}

async fn status_server(
    listener: TcpListener,
    secret: [u8; 32],
    access: ControlAccess,
    commands: mpsc::Sender<Envelope>,
) -> io::Result<()> {
    let mut sessions = JoinSet::new();
    loop {
        tokio::select! {
            accepted = listener.accept(), if sessions.len() < 32 => {
                let (stream, peer) = accepted?;
                if !peer.ip().is_loopback() { continue; }
                let commands = commands.clone();
                let access = access.clone();
                sessions.spawn(async move { let _ = serve_control(stream, &secret, &access, &commands).await; });
            }
            _ = sessions.join_next(), if !sessions.is_empty() => {}
        }
    }
}

// Recovery telemetry never blocks process observation or a Gateway heartbeat.
// Saturation/I/O failure is latched and visible; it is not reported as durable
// audit success. Files contain fixed event enums/IDs, never errors or payloads.
struct RecoveryAudit {
    sender: mpsc::Sender<Vec<u8>>,
    failed: std::sync::Arc<std::sync::atomic::AtomicBool>,
    dropped: std::sync::atomic::AtomicU64,
    upstream_dropped: std::sync::atomic::AtomicU64,
}
impl RecoveryAudit {
    async fn open(path: PathBuf) -> Result<Self, &'static str> {
        let parent = path.parent().ok_or("invalid recovery audit location")?;
        tokio::fs::create_dir_all(parent)
            .await
            .map_err(|_| "cannot create recovery audit directory")?;
        let (file, guard) = tokio::task::spawn_blocking(move || {
            // Lock a separate handle so readers can inspect the audit and its
            // empty sidecar. The trusted directory protects the lock namespace.
            let mut lock_options = std::fs::OpenOptions::new();
            lock_options
                .create(true)
                .read(true)
                .write(true)
                .truncate(false);
            #[cfg(unix)]
            {
                use std::os::unix::fs::OpenOptionsExt;
                lock_options.mode(0o600);
            }
            #[cfg(windows)]
            {
                use std::os::windows::fs::OpenOptionsExt;
                // Windows byte-range locks reject even read-only inspection of
                // an empty file. Share reads, deny another writer or deletion
                // for the lifetime of this handle instead.
                lock_options.share_mode(0x0000_0001); // FILE_SHARE_READ
            }
            let guard = lock_options.open(path.with_extension("jsonl.lock"))?;
            #[cfg(not(windows))]
            guard
                .try_lock()
                .map_err(|_| io::Error::other("recovery audit is busy"))?;
            let mut options = std::fs::OpenOptions::new();
            options.create(true).read(true).append(true);
            #[cfg(unix)]
            {
                use std::os::unix::fs::OpenOptionsExt;
                options.mode(0o600);
            }
            let file = options.open(path)?;
            Ok::<_, io::Error>((file, guard))
        })
        .await
        .map_err(|_| "cannot initialize recovery audit")?
        .map_err(|_| "cannot open recovery audit")?;
        let mut size = file
            .metadata()
            .map_err(|_| "cannot inspect recovery audit")?
            .len();
        let mut file = tokio::fs::File::from_std(file);
        let (sender, mut receiver) = mpsc::channel::<Vec<u8>>(512);
        let failed = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let fault = failed.clone();
        tokio::spawn(async move {
            let _guard = guard;
            while let Some(bytes) = receiver.recv().await {
                size = size.saturating_add(bytes.len() as u64);
                if size > 16 * 1024 * 1024
                    || file.write_all(&bytes).await.is_err()
                    || file.sync_data().await.is_err()
                {
                    fault.store(true, std::sync::atomic::Ordering::Release);
                    break;
                }
            }
        });
        Ok(Self {
            sender,
            failed,
            dropped: std::sync::atomic::AtomicU64::new(0),
            upstream_dropped: std::sync::atomic::AtomicU64::new(0),
        })
    }
    fn note_upstream_dropped(&self, total: u64) {
        self.upstream_dropped
            .fetch_max(total, std::sync::atomic::Ordering::Relaxed);
        if total != 0 {
            self.failed
                .store(true, std::sync::atomic::Ordering::Release);
        }
    }
    fn submit(&self, event: &impl Serialize) {
        let ok = serde_json::to_vec(event)
            .ok()
            .filter(|v| v.len() < 4096)
            .is_some_and(|mut bytes| {
                bytes.push(b'\n');
                self.sender.try_send(bytes).is_ok()
            });
        if !ok {
            self.failed
                .store(true, std::sync::atomic::Ordering::Release);
            self.dropped
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
    }
    fn status(&self) -> Value {
        json!({"ok": !self.failed.load(std::sync::atomic::Ordering::Acquire),
            "dropped_events": self.dropped.load(std::sync::atomic::Ordering::Relaxed),
            "upstream_dropped_events":self.upstream_dropped.load(std::sync::atomic::Ordering::Relaxed)})
    }
}

async fn run() -> Result<(), &'static str> {
    use src::{
        daemon::{Daemon, EnvironmentCredentials},
        supervisor::RestartPolicy,
    };
    let options = parse_options(std::env::args_os().skip(1))?;
    let loaded = src::cluster_runtime::LoadedClusterConfig::load(&options.file)
        .map_err(|_| "cannot load daemon configuration")?;
    if options.raft.is_some() {
        return run_raft_cluster(options, loaded).await;
    }
    if options.allow_test_gateway_switch {
        if loaded.test_cluster.is_some()
            || loaded.placement.cluster.mode != src::placement::ClusterMode::Standalone
        {
            return Err("test Gateway switch requires standalone configuration");
        }
        return run_test_gateway_switch(options, loaded.placement).await;
    }
    if loaded.test_cluster.is_some()
        || loaded.placement.cluster.mode == src::placement::ClusterMode::Quorum
    {
        return run_test_cluster(options, loaded).await;
    }
    if options.allow_test_quorum {
        return Err("test quorum flag requires test cluster configuration");
    }
    let config = loaded.placement;
    let mut daemon = Daemon::from_config(
        &config,
        &std::collections::HashMap::new(),
        &options.runtime_root,
        RestartPolicy::default(),
    )
    .map_err(|_| "daemon configuration is invalid or unsupported")?;
    daemon
        .set_gateway_program(&options.gateway_program)
        .map_err(|_| "invalid gateway program")?;
    if let Some(address) = options.mock_discord {
        daemon
            .enable_mock_discord(address)
            .map_err(|_| "invalid mock Discord configuration")?;
    }
    let bootstrap = load_bootstrap(options.secret_file.as_deref()).await?;
    let listener = TcpListener::bind(("127.0.0.1", options.control_port))
        .await
        .map_err(|_| "cannot bind local daemon control")?;
    let port = listener
        .local_addr()
        .map_err(|_| "cannot inspect local daemon control")?
        .port();
    let audit = RecoveryAudit::open(options.runtime_root.join("recovery-audit.jsonl")).await?;
    daemon.require_shared_recovery_admission();
    let credentials = EnvironmentCredentials;
    // A Gateway rejects any --bot-id that is not a canonical decimal u64 and exits at
    // once, so such a Bot would fail on every (re)start. Say so up front instead of
    // letting the supervisor loop on a child that can never start.
    if !options.no_autostart
        && daemon
            .snapshots()
            .iter()
            .any(|s| !is_canonical_discord_id(&s.key.bot_id))
    {
        return Err(
            "bot_id must be a canonical decimal Discord ID when Gateways are launched \
             (placeholder IDs are only usable with --no-autostart)",
        );
    }
    // Partial startup failures remain visible through snapshots. Dropping this
    // daemon, including any error below, deliberately does not force-kill children.
    let startup_ok = options.no_autostart || daemon.start_all(&credentials).is_ok();
    let local = daemon.snapshots().into_iter().map(|s| s.key).collect();
    let mut updates = UpdateHub::new(
        config.clone(),
        local,
        options.runtime_root.join("update-audit.jsonl"),
    );
    let mut control = ControlPlane::new(daemon, config.clone(), startup_ok);
    let (sender, mut receiver) = mpsc::channel::<Envelope>(64);
    let mut server = tokio::spawn(status_server(
        listener,
        bootstrap.secret,
        bootstrap.access.clone().unwrap_or_default(),
        sender,
    ));
    let mut announcement = serde_json::to_vec(&json!({
        "port": port, "local_only": true, "startup_ok": startup_ok,
        "mock_discord": options.mock_discord.is_some(),
    }))
    .map_err(|_| "cannot encode daemon announcement")?;
    announcement.push(b'\n');
    let mut stdout = tokio::io::stdout();
    if stdout.write_all(&announcement).await.is_err() || stdout.flush().await.is_err() {
        server.abort();
        return Err("cannot announce local daemon control");
    }
    // systemd Type=notify: the control listener is bound and announced.
    let notifier = src::sd_notify::Notifier::from_env();
    let _ = notifier.ready();
    let watchdog_every = src::sd_notify::watchdog_interval(
        std::env::var("WATCHDOG_USEC").ok().as_deref(),
        std::env::var("WATCHDOG_PID").ok().as_deref(),
        std::process::id(),
    )
    .filter(|_| notifier.is_enabled());
    let mut last_watchdog = tokio::time::Instant::now();
    let mut interval = tokio::time::interval(Duration::from_millis(25));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let result = loop {
        tokio::select! {
            signal = tokio::signal::ctrl_c() => {
                break signal.map_err(|_| "cannot receive daemon shutdown signal");
            }
            _ = interval.tick() => {
                control.tick(&credentials).await;
                // Sent only after the supervision tick completed, so a stalled
                // loop stops pinging and systemd's WatchdogSec fires.
                if watchdog_every.is_some_and(|every| last_watchdog.elapsed() >= every) {
                    let _ = notifier.watchdog();
                    last_watchdog = tokio::time::Instant::now();
                }
                audit.note_upstream_dropped(control.recovery_events_dropped());
                for event in control.take_recovery_events() { audit.submit(&event); }
                for job in updates.drain() {
                    updates.complete(job, json!({"ok":false,"error":"update execution is unavailable in the default runtime; Core readiness/drain and Gateway authority are not connected"}));
                }
            }
            Some(envelope) = receiver.recv() => {
                if !envelope.response.is_closed() {
                    let mut response = if let Some(update) = envelope.update {
                        if control.operation_exists(&update.operation_id) {
                            json!({"ok":false,"error":"operation ID conflicts with an earlier request"})
                        } else {
                            let busy = control.bot_busy(&update.bot);
                            updates.admit(update, envelope.access, busy)
                        }
                    } else if envelope.request.command == "operation" && envelope.request.operation_id.as_deref().is_some_and(|id| updates.contains(id)) {
                        updates.query(envelope.request.operation_id.as_deref().unwrap(), &envelope.access)
                    } else if matches!(envelope.request.command.as_str(), "start" | "stop" | "restart")
                        && (envelope.request.operation_id.as_deref().is_some_and(|id| updates.contains(id)) || envelope.request.bot.as_deref().and_then(|selector| src::botref::resolve(&config.bots, selector).ok().map(|b| updates.busy(&b.bot_id))).unwrap_or(false)) {
                        json!({"ok":false,"error":"Bot or operation ID is already busy"})
                    } else { control.request_as(&envelope.request, &envelope.access) };
                    response["recovery_audit"] = audit.status();
                    let _ = envelope.response.send(response);
                }
            }
            _ = &mut server => break Err("local daemon control stopped"),
        }
    };
    let _ = notifier.stopping();
    server.abort();
    result
}

async fn run_raft_cluster(
    mut options: Options,
    config: src::cluster_runtime::LoadedClusterConfig,
) -> Result<(), &'static str> {
    // Authenticate the local owner before creating stores, binding cluster RPC,
    // or launching any child. No environment credentials enter this branch.
    let bootstrap = load_bootstrap(options.secret_file.as_deref()).await?;
    if bootstrap
        .access
        .as_ref()
        .is_some_and(|a| a.role != "admin" || a.scope.is_some())
    {
        return Err("Raft cluster requires a full local administrator capability");
    }
    let listener = TcpListener::bind(("127.0.0.1", options.control_port))
        .await
        .map_err(|_| "cannot bind local daemon control")?;
    let port = listener
        .local_addr()
        .map_err(|_| "cannot inspect local daemon control")?
        .port();
    let mut host = src::daemon::cluster_host::ClusterHost::bind(
        &config,
        options.raft.take().ok_or("Raft opt-in required")?,
        &options.runtime_root,
        &options.gateway_program,
        options.mock_discord,
    )
    .await?;
    let (sender, mut receiver) = mpsc::channel::<Envelope>(64);
    let mut server = tokio::spawn(status_server(
        listener,
        bootstrap.secret,
        bootstrap.access.unwrap_or_default(),
        sender,
    ));
    let announcement = json!({"port":port,"local_only":true,"startup_ok":true,
        "mock_discord":true,"raft_quorum":true,"test_quorum":false,"production_ready":false});
    let mut bytes =
        serde_json::to_vec(&announcement).map_err(|_| "cannot encode daemon announcement")?;
    bytes.push(b'\n');
    let mut stdout = tokio::io::stdout();
    if stdout.write_all(&bytes).await.is_err() || stdout.flush().await.is_err() {
        server.abort();
        host.shutdown().await;
        return Err("cannot announce local daemon control");
    }
    let mut interval = tokio::time::interval(Duration::from_millis(25));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let result = loop {
        tokio::select! {
            signal = tokio::signal::ctrl_c() => break signal.map_err(|_| "cannot receive daemon shutdown signal"),
            _ = interval.tick() => {
                if let Err(error) = host.tick().await { break Err(error); }
            }
            Some(envelope) = receiver.recv() => {
                let response = if envelope.update.is_none()
                    && matches!(envelope.request.command.as_str(), "ps" | "status" | "inspect" | "shards") {
                    let snapshot = host.status();
                    json!({"ok":true,"offline":false,"status":snapshot,
                        "cluster":snapshot["cluster"],"processes":snapshot["processes"],
                        "transitions":snapshot["transitions"],
                        "note":"EXPERIMENTAL OpenRaft/mTLS with loopback mock Gateway; production disabled."})
                } else {
                    json!({"ok":false,"offline":false,"error":"Raft quorum owns process lifecycle; manual mutation denied"})
                };
                let _ = envelope.response.send(response);
            }
            _ = &mut server => break Err("local daemon control stopped"),
        }
    };
    server.abort();
    host.shutdown().await;
    result
}

async fn run_test_cluster(
    options: Options,
    config: src::cluster_runtime::LoadedClusterConfig,
) -> Result<(), &'static str> {
    use src::{
        cluster_runtime::{ClusterAction, ClusterRuntime},
        daemon::test_cluster_processes::{ProcessEvent, TestClusterProcesses},
    };
    if options.no_autostart {
        return Err("test quorum does not support manual process administration");
    }
    let mut runtime = ClusterRuntime::bind(
        &config,
        options.allow_test_quorum,
        options.mock_discord.is_some(),
    )
    .await
    .map_err(|error| {
        use src::cluster_runtime::ClusterRuntimeError;
        match error {
            ClusterRuntimeError::TestOnlyOptInRequired => {
                "test quorum requires --allow-test-quorum"
            }
            ClusterRuntimeError::MockDiscordRequired => "test quorum requires --mock-discord",
            _ => "test quorum configuration or mTLS transport invalid",
        }
    })?;
    let settings = config
        .test_cluster
        .as_ref()
        .ok_or("test cluster configuration required")?;
    let specs = config
        .local_specs(&options.runtime_root)
        .map_err(|_| "test shard launch configuration invalid")?;
    let mut processes = TestClusterProcesses::new(
        specs,
        &options.gateway_program,
        options
            .mock_discord
            .ok_or("test quorum requires mock Discord")?,
        settings.node_id,
        settings.lease_ms,
        settings.stop_margin_ms,
    )?;
    let bootstrap = load_bootstrap(options.secret_file.as_deref()).await?;
    // The experimental cluster exposes an aggregate view. Restrict it to its
    // original full local administrator capability until scoped views exist.
    if bootstrap
        .access
        .as_ref()
        .is_some_and(|a| a.role != "admin" || a.scope.is_some())
    {
        return Err("test cluster requires a full local administrator capability");
    }
    let listener = TcpListener::bind(("127.0.0.1", options.control_port))
        .await
        .map_err(|_| "cannot bind local daemon control")?;
    let port = listener
        .local_addr()
        .map_err(|_| "cannot inspect local daemon control")?
        .port();
    let (sender, mut receiver) = mpsc::channel::<Envelope>(64);
    let mut server = tokio::spawn(status_server(
        listener,
        bootstrap.secret,
        bootstrap.access.clone().unwrap_or_default(),
        sender,
    ));
    let announcement = json!({"port":port,"local_only":true,"startup_ok":true,
        "mock_discord":true,"test_quorum":true,"production_ready":false});
    let mut bytes =
        serde_json::to_vec(&announcement).map_err(|_| "cannot encode daemon announcement")?;
    bytes.push(b'\n');
    let mut stdout = tokio::io::stdout();
    if stdout.write_all(&bytes).await.is_err() || stdout.flush().await.is_err() {
        server.abort();
        runtime.shutdown().await;
        return Err("cannot announce local daemon control");
    }
    let mut interval = tokio::time::interval(Duration::from_millis(25));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut monitor_ok = true;
    let result = loop {
        tokio::select! {
            signal=tokio::signal::ctrl_c()=>break signal.map_err(|_| "cannot receive daemon shutdown signal"),
            _=interval.tick()=> {
                for event in processes.poll() {
                    let result = match event {
                        ProcessEvent::Ready(key)=>runtime.startup_complete(&key),
                        ProcessEvent::Exited(key)=>runtime.process_exited(&key),
                    };
                    if result.is_err() { monitor_ok=false; }
                }
                let actions = match runtime.tick() {
                    Ok(actions)=>actions,
                    Err(_)=>break Err("test cluster runtime stopped safely"),
                };
                for action in actions {
                    let (key, starting) = match &action {
                        ClusterAction::Start{key,..}=>(key.clone(),true),
                        ClusterAction::Renew{key,..} | ClusterAction::Stop{key,..}=>(key.clone(),false),
                    };
                    if processes.apply(action).await.is_err() {
                        monitor_ok=false;
                        if starting && !processes.has_process(&key) { let _ = runtime.process_exited(&key); }
                        else if let Ok(Some(stop)) = runtime.process_failed(&key) {
                            let _ = processes.apply(stop).await;
                        }
                    }
                }
            }
            Some(envelope)=receiver.recv()=> {
                let value = if matches!(envelope.request.command.as_str(),"ps"|"status"|"inspect"|"shards") {
                    let mut snapshot=processes.status();
                    snapshot["cluster"]=runtime.status();
                    snapshot["monitor_ok"]=json!(monitor_ok);
                    json!({"ok":true,"offline":false,"status":snapshot,
                        "cluster":snapshot["cluster"],"processes":snapshot["processes"],
                        "transitions":snapshot["transitions"],
                        "note":"TEST ONLY majority ACK and loopback mock Gateway; not production consensus."})
                } else {
                    json!({"ok":false,"offline":false,"error":"test quorum owns process lifecycle; manual mutation denied"})
                };
                let _=envelope.response.send(value);
            }
            _=&mut server=>break Err("local daemon control stopped"),
        }
    };
    server.abort();
    runtime.shutdown().await;
    // Dropping parent pipes cannot extend a guardian's last lease. Shutdown and
    // botd crash therefore use the same independently supervised expiry path.
    result
}

async fn run_test_gateway_switch(
    options: Options,
    config: src::placement::Config,
) -> Result<(), &'static str> {
    // All gates above run before bootstrap, binds, credentials or child launch.
    use std::io::Read;
    let mut bytes = Vec::new();
    std::fs::File::open(
        options
            .mock_identify_budget
            .as_ref()
            .ok_or("missing mock Identify budget")?,
    )
    .map_err(|_| "cannot read mock Identify budget")?
    .take(64 * 1024 + 1)
    .read_to_end(&mut bytes)
    .map_err(|_| "cannot read mock Identify budget")?;
    if bytes.len() > 64 * 1024 {
        return Err("mock Identify budget exceeds limit");
    }
    let limits = serde_json::from_slice(&bytes).map_err(|_| "invalid mock Identify budget")?;
    let bootstrap = load_bootstrap(options.secret_file.as_deref()).await?;
    let listener = TcpListener::bind(("127.0.0.1", options.control_port))
        .await
        .map_err(|_| "cannot bind local daemon control")?;
    let port = listener
        .local_addr()
        .map_err(|_| "cannot inspect local daemon control")?
        .port();
    let core_program = std::env::current_exe().map_err(|_| "Core executable unavailable")?;
    let audit = RecoveryAudit::open(options.runtime_root.join("recovery-audit.jsonl")).await?;
    let mut recovery_config = src::child_recovery::RecoveryConfig::default();
    if let Some(max_restarts) = options.mock_recovery_max_restarts {
        recovery_config.restart.max_restarts = max_restarts;
    }
    let runtime = src::daemon::test_gateway_switch::Runtime::new_with_recovery_config(
        &config,
        &options.runtime_root,
        &options.gateway_program,
        &core_program,
        options.mock_discord.ok_or("mock Discord required")?,
        limits,
        recovery_config,
    )?;
    let local = config
        .local_primary_intents(&std::collections::HashMap::new())
        .map_err(|_| "invalid local update targets")?
        .into_iter()
        .map(|s| src::daemon::ShardKey {
            bot_id: s.bot_id,
            shard_id: s.shard_id,
        })
        .collect::<Vec<_>>();
    let mut updates = UpdateHub::new(
        config.clone(),
        local.clone(),
        options.runtime_root.join("update-audit.jsonl"),
    );
    let (sender, mut receiver) = mpsc::channel::<Envelope>(64);
    let mut server = tokio::spawn(status_server(
        listener,
        bootstrap.secret,
        bootstrap.access.clone().unwrap_or_default(),
        sender,
    ));
    let mut announcement =
        serde_json::to_vec(&json!({"port":port,"local_only":true,"startup_ok":true,
        "mock_discord":true,"test_gateway_switch":true,"production_ready":false}))
        .map_err(|_| "cannot encode daemon announcement")?;
    announcement.push(b'\n');
    let mut stdout = tokio::io::stdout();
    if stdout.write_all(&announcement).await.is_err() || stdout.flush().await.is_err() {
        server.abort();
        return Err("cannot announce local daemon control");
    }
    let mut update_tick = tokio::time::interval(Duration::from_millis(25));
    update_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let result = loop {
        tokio::select! {
            signal = tokio::signal::ctrl_c() => break signal.map_err(|_| "cannot receive daemon shutdown signal"),
            Some(envelope) = receiver.recv() => {
                let mut response = if let Some(update) = envelope.update {
                    if runtime.legacy_operation_exists(&update.operation_id) {
                        json!({"ok":false,"error":"operation ID conflicts with an earlier request"})
                    } else { updates.admit(update, envelope.access, false) }
                } else if envelope.request.command == "operation" && envelope.request.operation_id.as_deref().is_some_and(|id| updates.contains(id)) {
                    updates.query(envelope.request.operation_id.as_deref().unwrap(), &envelope.access)
                } else {
                    let selector = envelope.request.bot.as_deref();
                    let bot = selector.and_then(|s| src::botref::resolve(&config.bots,s.split('/').next().unwrap_or(s)).ok());
                    let keys: Vec<_> = local.iter().filter(|key| bot.is_none_or(|b| key.bot_id == b.bot_id)).cloned().collect();
                    let read = matches!(envelope.request.command.as_str(), "ps"|"status"|"inspect"|"shards"|"operation");
                    if !envelope.access.allows_command(read, bot.map(|b| b.bot_id.as_str()), &keys) {
                        json!({"ok":false,"error":"control permission or scope denied"})
                    } else if !read && (envelope.request.operation_id.as_deref().is_some_and(|id| updates.contains(id)) || bot.is_some_and(|b| updates.busy(&b.bot_id))) {
                        json!({"ok":false,"error":"Bot or operation ID is already busy"})
                    } else if envelope.request.command == "inspect" {
                        runtime.inspect_bot(selector.unwrap_or_default())
                    } else {
                        let mut value = runtime.request(&envelope.request);
                        // The legacy experimental runtime returns an aggregate
                        // view; never expose another Bot through a scoped selector.
                        if let Some(bot) = bot {
                            if let Some(shards) = value["shards"].as_array_mut() {
                                shards.retain(|s| s["bot_id"] == bot.bot_id);
                            }
                            if let Some(transitions) = value["transitions"].as_array_mut() {
                                transitions.retain(|s| s["bot_id"] == bot.bot_id);
                            }
                            if let Some(events) = value["recovery_events"].as_array_mut() {
                                events.retain(|e| e["key"]["bot_id"].as_u64().is_some_and(|id| id.to_string() == bot.bot_id));
                            }
                        }
                        value
                    }
                };
                response["recovery_audit"] = audit.status();
                let _ = envelope.response.send(response);
            },
            _ = update_tick.tick() => {
                audit.note_upstream_dropped(runtime.recovery_events_dropped());
                for event in runtime.take_recovery_events() { audit.submit(&event); }
                updates.poll_backend(|id| runtime.update_status(id));
                for job in updates.drain() {
                    let response = runtime.request_update(&job.request);
                    updates.complete(job,response);
                }
            },
            _ = &mut server => break Err("local daemon control stopped"),
        }
    };
    server.abort();
    result
}

#[tokio::main(worker_threads = 2)]
async fn main() {
    let args: Vec<_> = std::env::args_os().skip(1).collect();
    let result = if args.first().is_some_and(|arg| arg == "--shard-core") {
        src::worker::run_shard_core(args)
            .await
            .map_err(|_| "invalid or failed Shard Core process")
    } else if args
        == [
            OsString::from("--test-cluster-guardian"),
            OsString::from("--allow-test-quorum"),
        ]
    {
        src::daemon::test_cluster_processes::run_guardian().await
    } else {
        run().await
    };
    if let Err(error) = result {
        // Only static messages cross this boundary; no config, child output,
        // capability, credential, or OS error text is formatted.
        eprintln!("botd: {error}");
        std::process::exit(1);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{DuplexStream, duplex};

    #[test]
    fn discord_id_rule_matches_the_gateway_launch_check() {
        for good in ["1", "101", "18446744073709551615", "123456789012345678"] {
            assert!(is_canonical_discord_id(good), "{good}");
        }
        for bad in [
            "",
            "001",
            "+1",
            "-1",
            " 1",
            "1 ",
            "ab100-alpha",
            "1.0",
            "18446744073709551616",
        ] {
            assert!(!is_canonical_discord_id(bad), "{bad:?}");
        }
    }

    fn options(extra: &[&str]) -> Vec<OsString> {
        let root = std::env::current_dir().unwrap();
        let mut args = vec![
            "--file".into(),
            "config.toml".into(),
            "--runtime-root".into(),
            root.clone().into_os_string(),
            "--gateway-program".into(),
            root.join("gateway-program").into_os_string(),
            "--bootstrap-stdin".into(),
        ];
        args.extend(extra.iter().map(OsString::from));
        args
    }

    #[test]
    fn accepts_absolute_program_and_root() {
        assert!(parse_options(options(&[])).is_ok());
        assert!(!parse_options(options(&[])).unwrap().allow_test_quorum);
        assert!(parse_options(options(&[])).unwrap().raft.is_none());
        assert!(
            parse_options(options(&["--allow-test-quorum"]))
                .unwrap()
                .allow_test_quorum
        );
    }

    fn raft_options() -> Vec<OsString> {
        let mut args = options(&[
            "--allow-raft-quorum",
            "--mock-discord",
            "127.0.0.1:1234",
            "--raft-rpc-listen",
            "127.0.0.1:1201",
            "--raft-rpc-peer",
            "2=127.0.0.1:1202",
            "--raft-rpc-peer",
            "3=127.0.0.1:1203",
            "--raft-store",
        ]);
        args.push(
            std::env::current_dir()
                .unwrap()
                .join("test-raft-store")
                .into_os_string(),
        );
        args
    }

    #[test]
    fn raft_reopen_is_default_and_creation_and_bootstrap_are_explicit() {
        let raft = parse_options(raft_options()).unwrap().raft.unwrap();
        assert!(raft.allow_raft);
        assert!(!raft.enable_production);
        assert_eq!(raft.storage_mode, StorageMode::Open);
        assert!(!raft.bootstrap);
        assert_eq!(raft.rpc_peers.len(), 2);
        let mut args = raft_options();
        args.push("--raft-create".into());
        args.push("--raft-bootstrap".into());
        let raft = parse_options(args).unwrap().raft.unwrap();
        assert_eq!(raft.storage_mode, StorageMode::Create);
        assert!(raft.bootstrap);
    }

    #[test]
    fn raft_requires_mock_mode_and_rejects_competing_lifecycle_owners() {
        assert!(parse_options(options(&["--allow-raft-quorum"])).is_err());
        for extra in [
            "--allow-test-quorum",
            "--allow-test-gateway-switch",
            "--no-autostart",
            "--allow-raft-quorum",
            "--raft-bootstrap",
            "--enable-production",
        ] {
            let mut args = raft_options();
            args.push(extra.into());
            assert!(parse_options(args).is_err(), "accepted {extra}");
        }
        let mut args = raft_options();
        let index = args.iter().position(|arg| arg == "--mock-discord").unwrap();
        args.drain(index..index + 2);
        assert!(parse_options(args).is_err());
    }

    #[test]
    fn raft_settings_cannot_implicitly_enable_quorum() {
        for extra in [
            &["--raft-store", "store"][..],
            &["--raft-rpc-listen", "127.0.0.1:1201"],
            &["--raft-rpc-peer", "2=127.0.0.1:1202"],
            &["--raft-create"],
            &["--raft-bootstrap"],
        ] {
            assert!(parse_options(options(extra)).is_err());
        }
        let mut args = raft_options();
        args.retain(|arg| arg != "--allow-raft-quorum");
        assert!(parse_options(args).is_err());
    }

    #[test]
    fn raft_rejects_invalid_or_duplicate_routes_and_unsafe_store_paths() {
        for peer in [
            "2=127.0.0.1:1204",
            "4=127.0.0.1:1201",
            "4=127.0.0.1:1202",
            "0=127.0.0.1:1204",
            "04=127.0.0.1:1204",
            "4=127.0.0.1:0",
            "4=0.0.0.0:1204",
            "4=192.0.2.1:1204",
            "4=localhost:1204",
            "invalid",
        ] {
            let mut args = raft_options();
            args.extend(["--raft-rpc-peer".into(), peer.into()]);
            assert!(parse_options(args).is_err(), "accepted {peer}");
        }
        for path in [
            PathBuf::from("relative"),
            std::env::current_dir().unwrap().join("..").join("store"),
        ] {
            let mut args = raft_options();
            *args.last_mut().unwrap() = path.into_os_string();
            assert!(parse_options(args).is_err());
        }
        for address in ["127.0.0.1:0", "0.0.0.0:1234", "[::]:1234", "192.0.2.1:1234"] {
            assert!(raft_endpoint(address).is_err());
        }
        assert!(raft_endpoint("[::1]:1234").is_ok());
        for field in ["--raft-store", "--raft-rpc-listen"] {
            let mut args = raft_options();
            let index = args.iter().position(|arg| arg == field).unwrap();
            args.drain(index..index + 2);
            assert!(parse_options(args).is_err());
        }
    }

    #[test]
    fn response_budget_keeps_current_state_and_reports_omitted_history() {
        let value = json!({"ok":true,"shards":[{"phase":"Degraded","recovery":{"reason":"RestartBudget"}}],
            "transitions":(0..512).map(|index| json!({"index":index,"event":"child_recovery","padding":"x".repeat(180)})).collect::<Vec<_>>()});
        let bytes = encode_response(value).unwrap();
        assert!(bytes.len() < RESPONSE_LIMIT);
        let result: Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(result["shards"][0]["recovery"]["reason"], "RestartBudget");
        assert!(result["recovery_history_omitted"].as_u64().unwrap() > 0);
        assert_eq!(
            result["transitions"].as_array().unwrap().last().unwrap()["index"],
            511
        );
    }

    #[tokio::test]
    async fn recovery_audit_opens_locks_and_records_a_fixed_event() {
        let path = std::env::current_dir()
            .unwrap()
            .join("target")
            .join(format!(
                "recovery-audit-{}-{}.jsonl",
                std::process::id(),
                getrandom::u64().unwrap()
            ));
        let audit = RecoveryAudit::open(path.clone()).await.unwrap();
        assert!(RecoveryAudit::open(path.clone()).await.is_err());
        let lock_path = path.with_extension("jsonl.lock");
        assert!(tokio::fs::read(&lock_path).await.unwrap().is_empty());
        let event = src::child_recovery::RecoveryEvent {
            key: src::child_recovery::ChildKey {
                bot_id: src::protocol::BotId(1),
                shard_id: 0,
                role: src::child_recovery::ChildRole::Gateway,
            },
            generation: 2,
            at: Duration::from_secs(1),
            kind: src::child_recovery::RecoveryEventKind::Degraded {
                reason: src::child_recovery::DegradedReason::RestartBudget,
            },
        };
        audit.submit(&event);
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let bytes = tokio::fs::read(&path).await.unwrap();
                if let Ok(record) =
                    serde_json::from_slice::<src::child_recovery::RecoveryEvent>(&bytes)
                {
                    assert_eq!(record, event);
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        assert_eq!(audit.status()["ok"], true);
        audit.note_upstream_dropped(1);
        assert_eq!(audit.status()["ok"], false);
        drop(audit);
        tokio::time::timeout(Duration::from_secs(5), async {
            while tokio::fs::remove_file(&path).await.is_err() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            while tokio::fs::remove_file(&lock_path).await.is_err() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
    }

    #[test]
    fn mock_discord_requires_an_explicit_loopback_endpoint() {
        assert!(parse_options(options(&[])).unwrap().mock_discord.is_none());
        for address in ["127.0.0.1:1234", "[::1]:1234"] {
            let parsed = parse_options(options(&["--mock-discord", address])).unwrap();
            assert_eq!(parsed.mock_discord, Some(address.parse().unwrap()));
        }
        for extra in [
            &["--mock-discord"][..],
            &["--mock-discord", "true"],
            &["--mock-discord", "localhost:1234"],
            &["--mock-discord", "127.0.0.1:0"],
            &["--mock-discord", "0.0.0.0:1234"],
            &["--mock-discord", "192.0.2.1:1234"],
            &["--mock-discord", "[::]:1234"],
            &[
                "--mock-discord",
                "127.0.0.1:1234",
                "--mock-discord",
                "127.0.0.1:5678",
            ],
        ] {
            assert!(parse_options(options(extra)).is_err());
        }
    }

    #[test]
    fn rejects_missing_duplicate_and_unknown_options() {
        for extra in [
            &["--bootstrap-stdin"][..],
            &["--file", "other"],
            &["--bind", "0.0.0.0:1"],
            &["--allow-test-quorum", "--allow-test-quorum"],
        ] {
            assert!(parse_options(options(extra)).is_err());
        }
        let mut args = options(&[]);
        args.pop();
        assert!(parse_options(args).is_err());
        assert!(parse_options([OsString::from("--file")]).is_err());
    }

    #[test]
    fn secret_file_is_exclusive_with_stdin_bootstrap() {
        let secret = std::env::current_dir().unwrap().join("botd.secret");
        let mut base = options(&[]);
        base.pop(); // drop --bootstrap-stdin
        let with = |extra: &[&str]| {
            let mut args = base.clone();
            args.extend(extra.iter().map(OsString::from));
            args
        };
        let secret_arg = secret.to_str().unwrap();
        let parsed = parse_options(with(&["--secret-file", secret_arg])).unwrap();
        assert_eq!(parsed.secret_file.as_deref(), Some(secret.as_path()));
        assert_eq!(parsed.control_port, 0);
        let parsed = parse_options(with(&[
            "--secret-file",
            secret_arg,
            "--control-port",
            "40123",
        ]))
        .unwrap();
        assert_eq!(parsed.control_port, 40123);
        for extra in [
            &["--secret-file", secret_arg, "--bootstrap-stdin"][..],
            &["--secret-file", "relative.secret"],
            &["--secret-file", secret_arg, "--secret-file", secret_arg],
            &["--secret-file", secret_arg, "--control-port", "0"],
            &["--secret-file", secret_arg, "--control-port", "65536"],
            &["--secret-file", secret_arg, "--control-port", "x"],
            &["--control-port", "40123", "--bootstrap-stdin"],
            &["--secret-file"],
            &[],
        ] {
            assert!(parse_options(with(extra)).is_err(), "{extra:?}");
        }
    }

    #[test]
    fn recovery_budget_override_requires_explicit_mock_switch() {
        assert!(parse_options(options(&["--mock-recovery-max-restarts", "0"])).is_err());
        let base = [
            "--allow-test-gateway-switch",
            "--mock-discord",
            "127.0.0.1:1234",
            "--mock-identify-budget",
            "limits.json",
            "--mock-recovery-max-restarts",
        ];
        for value in ["-1", "invalid", "4294967296"] {
            let mut extra = base.to_vec();
            extra.push(value);
            assert!(parse_options(options(&extra)).is_err());
        }
        let mut extra = base.to_vec();
        extra.push("0");
        assert_eq!(
            parse_options(options(&extra))
                .unwrap()
                .mock_recovery_max_restarts,
            Some(0)
        );
    }

    #[test]
    fn rejects_relative_program_or_root() {
        for index in [3, 5] {
            let mut args = options(&[]);
            args[index] = "relative".into();
            assert!(parse_options(args).is_err());
        }
    }

    async fn status_session(request: &[u8], status: Value) -> (io::Result<()>, Vec<u8>) {
        let (mut client, server) = duplex(128 * 1024);
        let (sender, mut receiver) = mpsc::channel::<Envelope>(1);
        let backend = tokio::spawn(async move {
            if let Some(envelope) = receiver.recv().await {
                let _ = envelope
                    .response
                    .send(json!({"ok": true, "status": status, "note": NOTE}));
            }
        });
        let task = tokio::spawn(async move { serve_status(server, &[7; 32], &sender).await });
        client.write_all(request).await.unwrap();
        client.shutdown().await.unwrap();
        let mut response = Vec::new();
        client.read_to_end(&mut response).await.unwrap();
        let result = task.await.unwrap();
        backend.await.unwrap();
        (result, response)
    }

    fn request(secret: [u8; 32], command: &str) -> Vec<u8> {
        let mut frame = serde_json::to_vec(&StatusRequest {
            secret,
            command: command.into(),
            bot: None,
            operation_id: None,
            force: false,
            yes: false,
        })
        .unwrap();
        frame.push(b'\n');
        frame
    }

    #[tokio::test]
    async fn authenticated_status_returns_snapshot_without_secret() {
        let (result, response) =
            status_session(&request([7; 32], "status"), json!({"processes": []})).await;
        result.unwrap();
        let response: Value = serde_json::from_slice(&response).unwrap();
        assert_eq!(response["ok"], true);
        assert_eq!(response["status"]["processes"], json!([]));
        assert!(response.get("secret").is_none());
    }

    #[tokio::test]
    async fn rejects_wrong_capability_and_mutating_command_without_response() {
        for frame in [request([8; 32], "status"), request([7; 32], "stop")] {
            let (result, response) = status_session(&frame, json!([])).await;
            assert_eq!(result.unwrap_err().kind(), io::ErrorKind::PermissionDenied);
            assert!(response.is_empty());
        }
    }

    #[tokio::test]
    async fn rejects_oversized_unterminated_and_malformed_requests_without_response() {
        for frame in [
            vec![b'x'; REQUEST_LIMIT as usize + 1],
            b"{}".to_vec(),
            b"bad\n".to_vec(),
        ] {
            let (result, response) = status_session(&frame, json!([])).await;
            assert_eq!(result.unwrap_err().kind(), io::ErrorKind::InvalidData);
            assert!(response.is_empty());
        }
    }

    #[tokio::test]
    async fn rejects_unknown_request_fields_without_response() {
        let mut value = serde_json::to_value(StatusRequest {
            secret: [7; 32],
            command: "status".into(),
            bot: None,
            operation_id: None,
            force: false,
            yes: false,
        })
        .unwrap();
        value["extra"] = json!(true);
        let mut frame = serde_json::to_vec(&value).unwrap();
        frame.push(b'\n');
        let (result, response) = status_session(&frame, json!([])).await;
        assert_eq!(result.unwrap_err().kind(), io::ErrorKind::InvalidData);
        assert!(response.is_empty());
    }

    #[tokio::test]
    async fn rejects_oversized_response_before_writing() {
        let (result, response) = status_session(
            &request([7; 32], "status"),
            json!("x".repeat(RESPONSE_LIMIT)),
        )
        .await;
        assert_eq!(result.unwrap_err().kind(), io::ErrorKind::InvalidData);
        assert!(response.is_empty());
    }

    #[tokio::test]
    async fn idle_control_client_has_a_deadline() {
        let (_client, server) = duplex(128);
        let (sender, _receiver) = mpsc::channel(1);
        let result = serve_status(server, &[7; 32], &sender).await;
        assert_eq!(result.unwrap_err().kind(), io::ErrorKind::TimedOut);
    }

    #[tokio::test]
    async fn status_is_dispatched_only_after_authentication() {
        let (mut client, server) = duplex(8192);
        let (sender, mut receiver) = mpsc::channel::<Envelope>(1);
        let task = tokio::spawn(async move { serve_status(server, &[7; 32], &sender).await });
        assert!(receiver.try_recv().is_err());
        client.write_all(&request([7; 32], "status")).await.unwrap();
        let envelope = receiver.recv().await.unwrap();
        assert_eq!(envelope.request.command, "status");
        envelope.response.send(json!({"ok": true, "status": {"processes": [{"state": "Failed"}], "monitor_ok": false}})).unwrap();
        let mut response = Vec::new();
        client.read_to_end(&mut response).await.unwrap();
        task.await.unwrap().unwrap();
        let response: Value = serde_json::from_slice(&response).unwrap();
        assert_eq!(response["status"]["monitor_ok"], false);
        assert_eq!(response["status"]["processes"][0]["state"], "Failed");
    }

    async fn bootstrap_input(frame: Vec<u8>) -> io::Result<Bootstrap> {
        let (mut client, server): (DuplexStream, DuplexStream) = duplex(8192);
        client.write_all(&frame).await.unwrap();
        client.shutdown().await.unwrap();
        read_bootstrap(server).await
    }

    #[tokio::test]
    async fn bootstrap_is_bounded_and_rejects_zero_or_unknown_fields() {
        let mut valid = serde_json::to_vec(&Bootstrap {
            secret: [7; 32],
            access: None,
        })
        .unwrap();
        valid.push(b'\n');
        assert_eq!(bootstrap_input(valid).await.unwrap().secret, [7; 32]);
        for frame in [
            b"{\"secret\":[0]}\n".to_vec(),
            b"{}\n".to_vec(),
            vec![b'x'; REQUEST_LIMIT as usize + 1],
            serde_json::to_vec(&Bootstrap {
                secret: [0; 32],
                access: None,
            })
            .unwrap(),
        ] {
            assert!(bootstrap_input(frame).await.is_err());
        }
        let mut zero = serde_json::to_vec(&Bootstrap {
            secret: [0; 32],
            access: None,
        })
        .unwrap();
        zero.push(b'\n');
        assert!(bootstrap_input(zero).await.is_err());
        let mut extra = json!({"secret": vec![7; 32], "port": 1})
            .to_string()
            .into_bytes();
        extra.push(b'\n');
        assert!(bootstrap_input(extra).await.is_err());
    }

    #[tokio::test]
    async fn bootstrap_has_a_deadline() {
        let (_client, server) = duplex(128);
        let result = read_bootstrap(server).await;
        assert!(matches!(result, Err(error) if error.kind() == io::ErrorKind::TimedOut));
    }
}
