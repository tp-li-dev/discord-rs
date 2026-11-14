use serde::Deserialize;
use serde_json::{Value, json};
use src::{AppError, redact::Secret, worker};
use std::{
    collections::BTreeMap,
    env,
    error::Error,
    ffi::{OsStr, OsString},
    fmt,
    future::IntoFuture,
    net::SocketAddr,
    path::{Component, PathBuf},
    process::ExitCode,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{
    io::{AsyncBufRead, AsyncBufReadExt, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader},
    net::TcpStream,
    sync::{Semaphore, mpsc, oneshot},
    task::JoinSet,
};
use twilight_gateway::{
    ConfigBuilder, Event, EventTypeFlags, Intents, Shard, ShardId, StreamExt as _,
};
use twilight_gateway_queue::InMemoryQueue;
use twilight_http::Client;
use twilight_model::gateway::SessionStartLimit;
use twilight_model::{
    application::{
        command::CommandType,
        interaction::{
            InteractionData, InteractionType,
            application_command::{CommandDataOption, CommandOptionValue},
        },
    },
    channel::message::{AllowedMentions, MessageFlags},
    http::interaction::{InteractionResponse, InteractionResponseData, InteractionResponseType},
};

fn configured_shard(deployment: &worker::DeploymentIdentity) -> ShardId {
    ShardId::new(deployment.shard.id(), deployment.shard.total())
}

fn slash_input(
    name: &str,
    kind: CommandType,
    options: &[CommandDataOption],
) -> Option<(String, Vec<(String, bot_framework::arguments::SlashValue)>)> {
    use bot_framework::arguments::SlashValue;
    if kind != CommandType::ChatInput
        || !bot_framework::commands()
            .ok()?
            .iter()
            .any(|command| command.name == name)
    {
        return None;
    }
    let args = options
        .iter()
        .map(|option| {
            let value = match &option.value {
                CommandOptionValue::String(value) => SlashValue::String(value.clone()),
                CommandOptionValue::Integer(value) => SlashValue::Integer(*value),
                CommandOptionValue::Boolean(value) => SlashValue::Boolean(*value),
                _ => return None,
            };
            Some((option.name.clone(), value))
        })
        .collect::<Option<Vec<_>>>()?;
    Some((name.to_owned(), args))
}

const DISCORD_CONTENT_LIMIT: usize = 2000;
const EMPTY_REPLY: &str = "(応答が空でした)";
const BUSY_REPLY: &str = "現在処理できません。しばらくしてからもう一度お試しください";
const UNSUPPORTED_REPLY: &str = "このコマンドは使えません";

/// Discord rejects content that is empty/blank or longer than 2000 characters, which
/// would leave the "thinking" placeholder in place. Cut to 1997 characters plus `…`.
fn clamp_reply(content: &str) -> String {
    if content.trim().is_empty() {
        return EMPTY_REPLY.to_owned();
    }
    match content.char_indices().nth(DISCORD_CONTENT_LIMIT) {
        None => content.to_owned(),
        Some(_) => {
            let end = content
                .char_indices()
                .nth(DISCORD_CONTENT_LIMIT - 3)
                .map_or(content.len(), |(index, _)| index);
            format!("{}…", &content[..end])
        }
    }
}

/// Best-effort ephemeral reply for an interaction that will not be processed. Bounded by a
/// short timeout (Discord requires a first response within 3 seconds) and never panics.
async fn reject_interaction(
    http: &Client,
    application_id: twilight_model::id::Id<twilight_model::id::marker::ApplicationMarker>,
    interaction_id: twilight_model::id::Id<twilight_model::id::marker::InteractionMarker>,
    token: &str,
    text: &str,
) {
    let response = InteractionResponse {
        kind: InteractionResponseType::ChannelMessageWithSource,
        data: Some(InteractionResponseData {
            content: Some(text.to_owned()),
            flags: Some(MessageFlags::EPHEMERAL),
            allowed_mentions: Some(AllowedMentions::default()),
            ..Default::default()
        }),
    };
    let sent = confirmed_response(
        Duration::from_millis(1500),
        http.interaction(application_id)
            .create_response(interaction_id, token, &response)
            .into_future(),
    )
    .await;
    if !sent {
        eprintln!("Interaction の拒否応答を送れませんでした（結果不明）");
    }
}

// HTTP failures and timeouts can both have an unknown external outcome.
async fn confirmed_response<F, T, E>(deadline: std::time::Duration, request: F) -> bool
where
    F: std::future::Future<Output = Result<T, E>>,
{
    matches!(tokio::time::timeout(deadline, request).await, Ok(Ok(_)))
}

// One configured shard per executable. This queue is process-local, not distributed.
async fn single_shard_queue<F>(
    total_shards: u32,
    metadata: F,
) -> Result<InMemoryQueue, StartupFailure>
where
    F: std::future::Future<Output = Result<(u32, SessionStartLimit), ()>>,
{
    let (recommended, limits) = tokio::time::timeout(std::time::Duration::from_secs(10), metadata)
        .await
        .map_err(|_| StartupFailure::MetadataTimeout)?
        .map_err(|_| StartupFailure::MetadataUnavailable)?;
    if recommended > total_shards {
        return Err(StartupFailure::InsufficientShards);
    }
    let mut registry = src::identify::IdentifyRegistry::default();
    // A private key for the only bot in this process, not a Discord bot ID.
    registry
        .register(std::num::NonZeroU64::MIN, &limits)
        .map_err(|_| StartupFailure::InvalidIdentifyLimits)
}

// Only typed diagnostics are public. These variants carry no environment values,
// protocol input, HTTP bodies or credential-bearing paths.
#[derive(Debug)]
enum StartupFailure {
    MissingToken,
    NonUnicodeToken,
    InvalidBootstrap,
    MetadataTimeout,
    MetadataUnavailable,
    InsufficientShards,
    InvalidIdentifyLimits,
    InvalidArguments,
    InvalidMockFrame,
    MockUnavailable,
    ShutdownFailed,
}

impl fmt::Display for StartupFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::MissingToken => "DISCORD_TOKEN is not configured [token_missing]",
            Self::NonUnicodeToken => "DISCORD_TOKEN is not Unicode [token_encoding]",
            Self::InvalidBootstrap => "invalid management bootstrap [bootstrap_invalid]",
            Self::MetadataTimeout => "gateway metadata timed out [metadata_timeout]",
            Self::MetadataUnavailable => "gateway metadata unavailable [metadata_unavailable]",
            Self::InsufficientShards => {
                "configured shard count is below Discord recommendation [shards_insufficient]"
            }
            Self::InvalidIdentifyLimits => "invalid session start limits [identify_limits_invalid]",
            Self::InvalidArguments => "invalid gateway launch arguments [launch_invalid]",
            Self::InvalidMockFrame => "invalid mock input [mock_input_invalid]",
            Self::MockUnavailable => "mock transport unavailable [mock_unavailable]",
            Self::ShutdownFailed => "gateway drain did not complete [shutdown_incomplete]",
        })
    }
}

impl Error for StartupFailure {}

fn gateway_failure_code(kind: &twilight_gateway::error::ReceiveMessageErrorType) -> &'static str {
    use twilight_gateway::error::ReceiveMessageErrorType;
    match kind {
        ReceiveMessageErrorType::Deserializing { .. } => "gateway_deserialization",
        ReceiveMessageErrorType::Reconnect => "gateway_reconnect",
        ReceiveMessageErrorType::Compression => "gateway_compression",
        _ => "gateway_receive",
    }
}

// External error text may contain short unlabelled credentials that heuristic
// redaction cannot recognize. Never format unknown Display, Debug or sources.
fn failure_message(context: &'static str, error: &(dyn Error + 'static)) -> String {
    let diagnostic = if let Some(error) = error.downcast_ref::<StartupFailure>() {
        error.to_string()
    } else if let Some(error) = error.downcast_ref::<twilight_gateway::error::ReceiveMessageError>()
    {
        format!("{}: [REDACTED]", gateway_failure_code(error.kind()))
    } else {
        worker::diagnostic(error)
    };
    format!("{context}: {diagnostic}")
}

fn token_from_env(value: Result<String, env::VarError>) -> Result<Secret<String>, AppError> {
    match value {
        Ok(value) => Ok(Secret::new(value)),
        Err(env::VarError::NotPresent) => Err(StartupFailure::MissingToken.into()),
        Err(env::VarError::NotUnicode(_)) => Err(StartupFailure::NonUnicodeToken.into()),
    }
}

#[derive(Default)]
struct GatewayOptions {
    launch: BTreeMap<String, OsString>,
    mock_discord: Option<SocketAddr>,
    local_control: bool,
}

impl GatewayOptions {
    fn parse(args: impl IntoIterator<Item = OsString>) -> Result<Self, StartupFailure> {
        let mut options = Self::default();
        let mut args = args.into_iter();
        while let Some(flag) = args.next() {
            let flag = flag.to_str().ok_or(StartupFailure::InvalidArguments)?;
            match flag {
                "--local-control" if !options.local_control => options.local_control = true,
                "--mock-discord" if options.mock_discord.is_none() => {
                    let addr = args.next().ok_or(StartupFailure::InvalidArguments)?;
                    let addr = addr
                        .to_str()
                        .and_then(|v| v.parse::<SocketAddr>().ok())
                        .filter(|addr| addr.ip().is_loopback() && addr.port() != 0)
                        .ok_or(StartupFailure::InvalidArguments)?;
                    options.mock_discord = Some(addr);
                }
                "--bot-id" | "--shard-id" | "--total-shards" | "--ipc-path" | "--state-path"
                | "--management-path" | "--token-env" => {
                    let value = args.next().ok_or(StartupFailure::InvalidArguments)?;
                    if options.launch.insert(flag.to_owned(), value).is_some() {
                        return Err(StartupFailure::InvalidArguments);
                    }
                }
                _ => return Err(StartupFailure::InvalidArguments),
            }
        }
        if (!options.launch.is_empty() && options.launch.len() != 7)
            || (options.local_control
                && (!options.launch.is_empty() || options.mock_discord.is_some()))
        {
            return Err(StartupFailure::InvalidArguments);
        }
        if let Some(bot_id) = options.launch.get("--bot-id") {
            let bot_id = bot_id.to_str().ok_or(StartupFailure::InvalidArguments)?;
            // Deployment namespaces are strings while today's Core protocol is
            // u64. Reject aliases such as "001" and "1", rather than letting two
            // distinct Bot paths acquire the same handshake identity.
            if bot_id
                .parse::<u64>()
                .ok()
                .is_none_or(|id| id.to_string() != bot_id)
            {
                return Err(StartupFailure::InvalidArguments);
            }
        }
        for flag in ["--ipc-path", "--state-path", "--management-path"] {
            if let Some(value) = options.launch.get(flag) {
                let path = PathBuf::from(value);
                if !path.is_absolute() || path.components().any(|p| p == Component::ParentDir) {
                    return Err(StartupFailure::InvalidArguments);
                }
            }
        }
        if let Some(value) = options.launch.get("--token-env") {
            let value = value.to_str().ok_or(StartupFailure::InvalidArguments)?;
            let mut bytes = value.bytes();
            if !bytes
                .next()
                .is_some_and(|b| b == b'_' || b.is_ascii_alphabetic())
                || !bytes.all(|b| b == b'_' || b.is_ascii_alphanumeric())
            {
                return Err(StartupFailure::InvalidArguments);
            }
        }
        Ok(options)
    }

    fn deployment(&self) -> Result<worker::DeploymentIdentity, AppError> {
        worker::DeploymentIdentity::parse(|name| {
            let environment = match env::var(name) {
                Ok(value) => Some(value),
                Err(env::VarError::NotPresent) => None,
                Err(_) => return Err("invalid deployment environment"),
            };
            let flag = match name {
                "BOT_ID" => "--bot-id",
                "SHARD_ID" => "--shard-id",
                "TOTAL_SHARDS" => "--total-shards",
                _ => return Ok(environment),
            };
            match self.launch.get(flag) {
                Some(value) => {
                    let value = value.to_str().ok_or("invalid deployment argument")?;
                    if environment.as_deref().is_some_and(|v| v != value) {
                        return Err("conflicting deployment identity");
                    }
                    Ok(Some(value.to_owned()))
                }
                None => Ok(environment),
            }
        })
    }

    fn token_env(&self) -> &OsStr {
        self.launch
            .get("--token-env")
            .map(OsString::as_os_str)
            .unwrap_or_else(|| OsStr::new("DISCORD_TOKEN"))
    }

    fn path(&self, flag: &str) -> Option<PathBuf> {
        self.launch.get(flag).map(PathBuf::from)
    }
}

// A detached OS reader avoids Tokio's uncancellable stdin blocking task keeping
// the runtime alive after another shutdown source fires. The anonymous pipe is
// owned by the launcher; EOF/malformed input is never an instruction to stop.
fn shutdown_input(enabled: bool) -> oneshot::Receiver<()> {
    let (sender, receiver) = oneshot::channel();
    if enabled {
        std::thread::spawn(move || {
            use src::shard_runtime::{GATEWAY_CONTROL_FRAME_LIMIT, GatewayControl};
            use std::io::{BufRead, Read};
            let mut frame = Vec::new();
            let read = std::io::BufReader::new(std::io::stdin())
                .take(GATEWAY_CONTROL_FRAME_LIMIT as u64 + 1)
                .read_until(b'\n', &mut frame);
            if read.is_ok()
                && frame.len() <= GATEWAY_CONTROL_FRAME_LIMIT
                && frame.last() == Some(&b'\n')
                && matches!(
                    serde_json::from_slice(&frame),
                    Ok(GatewayControl::Shutdown {})
                )
            {
                let _ = sender.send(());
            }
        });
    }
    receiver
}

async fn shutdown_requested(receiver: oneshot::Receiver<()>) {
    let control = async {
        if receiver.await.is_err() {
            std::future::pending::<()>().await;
        }
    };
    tokio::select! {
        _ = control => {},
        signal = tokio::signal::ctrl_c() => {
            if signal.is_err() { std::future::pending::<()>().await; }
        }
    }
}

const MOCK_FRAME_LIMIT: u64 = 64 * 1024;

#[derive(Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
enum MockInput {
    Prefix {
        id: u64,
        #[serde(default)]
        event_id: Option<u64>,
        key: u64,
        content: String,
        rest_addr: SocketAddr,
    },
    Slash {
        id: u64,
        #[serde(default)]
        event_id: Option<u64>,
        key: u64,
        name: String,
        rest_addr: SocketAddr,
    },
    Status {
        id: u64,
    },
    Crash {
        id: u64,
    },
}

async fn mock_frame(
    reader: &mut (impl AsyncBufRead + Unpin),
) -> Result<Option<MockInput>, AppError> {
    // Idle collectors do not time out. Once a frame starts, it must finish within
    // five seconds, and neither the request nor any parse error is logged.
    let mut first = [0; 1];
    if reader.read(&mut first).await? == 0 {
        return Ok(None);
    }
    let mut frame = vec![first[0]];
    if first[0] != b'\n' {
        tokio::time::timeout(
            Duration::from_secs(5),
            reader.take(MOCK_FRAME_LIMIT).read_until(b'\n', &mut frame),
        )
        .await
        .map_err(|_| StartupFailure::InvalidMockFrame)??;
    }
    if frame.len() as u64 > MOCK_FRAME_LIMIT || frame.last() != Some(&b'\n') {
        return Err(StartupFailure::InvalidMockFrame.into());
    }
    serde_json::from_slice(&frame)
        .map(Some)
        .map_err(|_| StartupFailure::InvalidMockFrame.into())
}

async fn mock_write(writer: &mut (impl AsyncWrite + Unpin), value: &Value) -> Result<(), AppError> {
    let mut frame = serde_json::to_vec(value)?;
    frame.push(b'\n');
    if frame.len() as u64 > MOCK_FRAME_LIMIT {
        return Err(StartupFailure::InvalidMockFrame.into());
    }
    tokio::time::timeout(Duration::from_secs(5), async {
        writer.write_all(&frame).await?;
        writer.flush().await
    })
    .await
    .map_err(|_| StartupFailure::MockUnavailable)??;
    Ok(())
}

// This deliberately small adapter only speaks to testkit's bounded, close-after-
// response loopback HTTP server. No Discord token or Authorization header exists.
// It is never used for production HTTP, redirects, TLS, retries or rate limits.
async fn mock_http(addr: SocketAddr, method: &str, path: &str, body: Value) -> Option<u16> {
    if !addr.ip().is_loopback() || addr.port() == 0 {
        return None;
    }
    let request = async {
        let body = serde_json::to_vec(&body).ok()?;
        let mut stream = TcpStream::connect(addr).await.ok()?;
        let head = format!(
            "{method} {path} HTTP/1.1\r\nHost: {addr}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            body.len()
        );
        stream.write_all(head.as_bytes()).await.ok()?;
        stream.write_all(&body).await.ok()?;
        let mut bytes = Vec::new();
        stream
            .take(MOCK_FRAME_LIMIT + 1)
            .read_to_end(&mut bytes)
            .await
            .ok()?;
        if bytes.len() as u64 > MOCK_FRAME_LIMIT {
            return None;
        }
        let boundary = bytes.windows(4).position(|b| b == b"\r\n\r\n")?;
        let head = std::str::from_utf8(&bytes[..boundary]).ok()?;
        let mut lines = head.split("\r\n");
        let mut status = lines.next()?.split(' ');
        if status.next()? != "HTTP/1.1" {
            return None;
        }
        let status: u16 = status.next()?.parse().ok()?;
        if !(200..=599).contains(&status) {
            return None;
        }
        let mut length = None;
        for line in lines {
            let (name, value) = line.split_once(':')?;
            if name.eq_ignore_ascii_case("transfer-encoding") {
                return None;
            }
            if name.eq_ignore_ascii_case("content-length") {
                if length.is_some() {
                    return None;
                }
                length = Some(value.trim().parse::<usize>().ok()?);
            }
        }
        if length? != bytes.len() - boundary - 4 {
            return None;
        }
        Some(status)
    };
    tokio::time::timeout(Duration::from_secs(2), request)
        .await
        .ok()
        .flatten()
}

#[derive(Default)]
struct ResponseProgress {
    content: Option<String>,
    dispatched: bool,
    status: Option<u16>,
}

fn api_outcome(status: Option<u16>) -> worker::ApiOutcome {
    status.map_or(worker::ApiOutcome::Unknown, |status| {
        worker::ApiOutcome::Confirmed { status }
    })
}

async fn mock_command(
    manager: Arc<worker::Manager>,
    input: MockInput,
    accepted: mpsc::Sender<(u64, u64)>,
) -> Result<Value, AppError> {
    let (id, event_id, key, rest_addr) = match &input {
        MockInput::Prefix {
            id,
            event_id,
            key,
            rest_addr,
            ..
        }
        | MockInput::Slash {
            id,
            event_id,
            key,
            rest_addr,
            ..
        } => (*id, *event_id, *key, *rest_addr),
        _ => return Err(StartupFailure::InvalidMockFrame.into()),
    };
    if !rest_addr.ip().is_loopback() || rest_addr.port() == 0 {
        return Err(StartupFailure::InvalidMockFrame.into());
    }
    let mut guard = manager.begin_response_delivery(key).await?;
    if let Some(event_id) = event_id {
        // A forwarded frame is not yet admitted work. The local switch relay
        // waits for this reservation before pausing the old Gateway, so its
        // already accepted work can retain the originating Core through drain.
        // This is only an ingress acknowledgement, not handler/REST success.
        let _ = accepted.send((id, event_id)).await;
    }
    let (content, outcome, receipt) = match input {
        MockInput::Prefix { content, .. } => {
            match manager
                .dispatch_key_with_response(&mut guard, key, content)
                .await
            {
                Ok(Some(content)) => {
                    let status = mock_http(
                        rest_addr,
                        "POST",
                        &format!("/channels/{key}/messages"),
                        json!({"content":content}),
                    )
                    .await;
                    let outcome = match status {
                        Some(200..=299) => "completed",
                        Some(_) => "failed",
                        None => "unknown",
                    };
                    let receipt = manager.finish_response(guard, api_outcome(status)).await?;
                    (Some(content), outcome, Some(receipt))
                }
                Ok(None) => {
                    guard.confirm();
                    (None, "no_reply", None)
                }
                Err(_) => {
                    // No REST request was sent. An IPC error does not establish
                    // whether the command handler had already run in Core.
                    guard.confirm();
                    (None, "dispatch_failed", None)
                }
            }
        }
        MockInput::Slash { name, .. } => {
            let Some((name, args)) = slash_input(&name, CommandType::ChatInput, &[]) else {
                guard.confirm();
                return Ok(json!({"type":"result", "id":id, "content":null,
                    "outcome":"not_dispatched", "receipt":null, "unknown_responses":manager.unknown_responses().1.len()}));
            };
            let progress = Arc::new(Mutex::new(ResponseProgress::default()));
            let dispatch_progress = progress.clone();
            let update_progress = progress.clone();
            let dispatch_manager = manager.clone();
            let dispatch_guard = &mut guard;
            let outcome = src::interaction_flow::run(
                id,
                Duration::from_secs(2),
                Duration::from_secs(2),
                move |id| async move {
                    mock_http(
                        rest_addr,
                        "POST",
                        &format!("/interactions/{id}/mock/callback"),
                        json!({"type":5}),
                    )
                    .await
                        == Some(204)
                },
                move || async move {
                    match dispatch_manager
                        .dispatch_slash_with_response(dispatch_guard, key, name, args)
                        .await
                    {
                        Ok(Some(content)) => {
                            let mut progress = dispatch_progress.lock().unwrap();
                            progress.dispatched = true;
                            progress.content = Some(content.clone());
                            content
                        }
                        _ => "コマンドを実行できませんでした".to_owned(),
                    }
                },
                move |_, content| async move {
                    let status = mock_http(
                        rest_addr,
                        "PATCH",
                        "/webhooks/1/mock/messages/@original",
                        json!({"content":content}),
                    )
                    .await;
                    update_progress.lock().unwrap().status = status;
                    matches!(status, Some(200..=299))
                },
            )
            .await;
            let (content, dispatched, status) = {
                let progress = progress.lock().unwrap();
                (
                    progress.content.clone(),
                    progress.dispatched,
                    progress.status,
                )
            };
            let receipt = if dispatched {
                Some(manager.finish_response(guard, api_outcome(status)).await?)
            } else if outcome == src::interaction_flow::Outcome::Completed {
                guard.confirm();
                None
            } else {
                // ACK might have been applied even though its response was lost.
                drop(guard);
                None
            };
            (
                content,
                if dispatched {
                    match status {
                        Some(200..=299) => "completed",
                        Some(_) => "failed",
                        None => "unknown",
                    }
                } else if outcome == src::interaction_flow::Outcome::Completed {
                    "dispatch_failed"
                } else {
                    "unknown"
                },
                receipt,
            )
        }
        _ => unreachable!(),
    };
    Ok(
        json!({"type":"result", "id":id, "content":content, "outcome":outcome,
        "receipt":receipt, "unknown_responses":manager.unknown_responses().1.len()}),
    )
}

async fn mock_events(
    stream: &mut TcpStream,
    options: &GatewayOptions,
    manager: Arc<worker::Manager>,
    bot_id: u64,
    shard: src::protocol::Shard,
    shutdown: oneshot::Receiver<()>,
) -> Result<(), AppError> {
    let (reader, mut writer) = stream.split();
    let mut reader = BufReader::new(reader);
    let workers = manager.worker_identities().await;
    mock_write(&mut writer, &json!({"type":"ready", "mode":"mock_discord", "pid":std::process::id(),
        "bot_id":bot_id, "shard_id":shard.id(), "total_shards":shard.total(),
        "ipc_path":options.path("--ipc-path"), "state_path":options.path("--state-path"),
        "management_path":options.path("--management-path"), "workers":workers,
        "credential_present":env::var_os(options.token_env()).is_some() || env::var_os("DISCORD_TOKEN").is_some()})).await?;
    let shutdown = shutdown_requested(shutdown);
    tokio::pin!(shutdown);
    let mut inputs_open = true;
    let mut commands = JoinSet::new();
    let (accepted_tx, mut accepted_rx) = mpsc::channel(1);
    let result = loop {
        tokio::select! {
            biased;
            _ = &mut shutdown => break Ok(()),
            Some((id, event_id)) = accepted_rx.recv() => {
                if let Err(error) = mock_write(&mut writer,
                    &json!({"type":"accepted", "id":id, "event_id":event_id})).await {
                    break Err(error);
                }
            }
            joined = commands.join_next(), if !commands.is_empty() => {
                match joined {
                    Some(Ok(Ok(value))) => if let Err(error) = mock_write(&mut writer, &value).await { break Err(error); },
                    Some(Ok(Err(error))) => break Err(error),
                    _ => break Err(StartupFailure::MockUnavailable.into()),
                }
            }
            input = mock_frame(&mut reader), if inputs_open && commands.is_empty() => {
                match input {
                    Ok(Some(MockInput::Status { id })) => {
                        if let Err(error) = mock_write(&mut writer, &json!({"type":"status", "id":id,
                            "pid":std::process::id(), "workers":manager.worker_identities().await,
                            "unknown_responses":manager.unknown_responses().1.len()})).await { break Err(error); }
                    }
                    Ok(Some(MockInput::Crash { id })) => {
                        let _ = id;
                        // Fault injection is reachable only through the explicitly
                        // selected mock collector; no production listener exists.
                        std::process::exit(86);
                    }
                    Ok(Some(input)) => { commands.spawn(mock_command(manager.clone(), input, accepted_tx.clone())); }
                    Ok(None) => inputs_open = false,
                    Err(error) => break Err(error),
                }
            }
        }
    };
    // Accepted work completes before Worker stdin closes. Socket write failures
    // cannot cancel a REST operation that may already have been applied.
    let drained = tokio::time::timeout(Duration::from_secs(30), async {
        let mut failed = false;
        while let Some(joined) = commands.join_next().await {
            match joined {
                Ok(Ok(value)) => {
                    let _ = mock_write(&mut writer, &value).await;
                }
                _ => failed = true,
            }
        }
        failed
    })
    .await;
    if !matches!(drained, Ok(false)) {
        return Err(StartupFailure::ShutdownFailed.into());
    }
    result
}

fn main() -> ExitCode {
    src::redact::install_panic_hook();
    let result = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .map_err(|error| Box::new(error) as AppError)
        .and_then(|runtime| runtime.block_on(run()));
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("{}", failure_message("実行に失敗しました", error.as_ref()));
            ExitCode::FAILURE
        }
    }
}

async fn run() -> Result<(), AppError> {
    let args: Vec<_> = env::args_os().skip(1).collect();
    if args == [OsString::from("--worker")] {
        return worker::run().await;
    }
    let options = GatewayOptions::parse(args)?;
    let bootstrap = if options.local_control {
        Some(
            src::local_control::read_bootstrap()
                .await
                .map_err(|_| StartupFailure::InvalidBootstrap)?,
        )
    } else {
        None
    };
    let deployment = options.deployment()?;
    let bot_id = deployment.bot_id.0;
    let shard_identity = deployment.shard;
    let watch_file = if let Some(state) = options.path("--state-path") {
        tokio::fs::create_dir_all(&state).await?;
        state.join("worker-path.txt")
    } else {
        worker::worker_file()
    };
    let mut mock = if let Some(addr) = options.mock_discord {
        Some(
            tokio::time::timeout(Duration::from_secs(5), TcpStream::connect(addr))
                .await
                .map_err(|_| StartupFailure::MockUnavailable)?
                .map_err(|_| StartupFailure::MockUnavailable)?,
        )
    } else {
        None
    };
    // No production client, token lookup, metadata request or WebSocket exists
    // on the explicit mock branch. Environment variables never enable it.
    let live = if mock.is_none() {
        Some(live_gateway(&options, &deployment).await?)
    } else {
        None
    };
    let manager = worker::Manager::start_gateway_pool(env::current_exe()?, 2, deployment).await?;
    let shutdown = shutdown_input(!options.launch.is_empty() || options.mock_discord.is_some());
    let mut background = JoinSet::new();
    if let Some(bootstrap) = bootstrap {
        let control_manager = manager.clone();
        background.spawn(async move {
            let _ = src::local_control::gateway_session(bootstrap, control_manager).await;
        });
    }
    manager.start_jobs();
    background.spawn(manager.clone().watch(watch_file));
    let result = if let Some(stream) = &mut mock {
        mock_events(
            stream,
            &options,
            manager.clone(),
            bot_id,
            shard_identity,
            shutdown,
        )
        .await
    } else {
        let (shard, http) = live.expect("one explicit gateway mode");
        live_events(shard, http, manager.clone(), shutdown).await
    };
    // A watcher is not allowed to install another generation during shutdown.
    background.abort_all();
    while background.join_next().await.is_some() {}
    let worker_pids = manager.shutdown().await?;
    if result.is_ok()
        && let Some(stream) = &mut mock
    {
        let _ = mock_write(
            stream,
            &json!({"type":"stopped", "pid":std::process::id(),
            "worker_pids":worker_pids}),
        )
        .await;
    }
    result
}

async fn live_gateway(
    options: &GatewayOptions,
    deployment: &worker::DeploymentIdentity,
) -> Result<(Shard, Arc<Client>), AppError> {
    let _ = rustls::crypto::ring::default_provider().install_default();
    let shard_id = configured_shard(deployment);
    let token = token_from_env(env::var(options.token_env()))?;
    let http = Arc::new(Client::new(token.expose_secret().clone()));
    let queue = single_shard_queue(deployment.shard.total(), async {
        let response = http.gateway().authed().await.map_err(|_| ())?;
        let metadata = response.model().await.map_err(|_| ())?;
        Ok((metadata.shards, metadata.session_start_limit))
    })
    .await?;
    let config = ConfigBuilder::new(
        token.into_inner(),
        Intents::GUILD_MESSAGES | Intents::MESSAGE_CONTENT,
    )
    .queue(queue)
    .build();
    Ok((Shard::with_config(shard_id, config), http))
}

async fn live_events(
    mut shard: Shard,
    http: Arc<Client>,
    manager: Arc<worker::Manager>,
    shutdown: oneshot::Receiver<()>,
) -> Result<(), AppError> {
    let capacity = Arc::new(Semaphore::new(64));
    let mut requests = JoinSet::new();
    let shutdown = shutdown_requested(shutdown);
    tokio::pin!(shutdown);
    println!("Botを起動しています...");
    loop {
        let result = tokio::select! {
            biased;
            _ = &mut shutdown => break,
            _ = requests.join_next(), if !requests.is_empty() => continue,
            result = shard.next_event(EventTypeFlags::all()) => match result {
                Some(result) => result,
                None => {
                    // twilight ends the stream after a fatal close (4004 auth failed,
                    // 4010-4014 invalid shard/version/intents). It never reconnects, so say
                    // so: otherwise the process just vanishes after the start-up line.
                    eprintln!("Gateway が再接続できない切断で終了しました（直前の close code を参照: 4004=認証失敗、4014=特権 Intent が未許可）");
                    break;
                }
            },
        };
        match result {
            Ok(Event::Ready(ready)) => {
                println!("ログイン成功: {}", ready.user.id);
                let Ok(permit) = capacity.clone().try_acquire_owned() else {
                    eprintln!("Ready イベントを処理できません: 混雑中");
                    continue;
                };
                let manager = manager.clone();
                let bot_name = ready.user.name.clone();
                requests.spawn(async move {
                    let _permit = permit;
                    if let Err(error) = manager.dispatch_ready(bot_name).await {
                        eprintln!(
                            "{}",
                            failure_message("Ready イベントを処理できません", error.as_ref())
                        );
                    }
                });
            }
            Ok(Event::MessageCreate(message)) if !message.author.bot => {
                let Ok(permit) = capacity.clone().try_acquire_owned() else {
                    eprintln!("Workerが混雑しているためメッセージを処理できませんでした");
                    continue;
                };
                let manager = manager.clone();
                let http = http.clone();
                requests.spawn(async move {
                    let _permit = permit;
                    // No @everyone/@here/role/user pings from any Bot-authored message.
                    let no_mentions = AllowedMentions::default();
                    if message.content.trim() == "!workers" {
                        let content = clamp_reply(&manager.describe_workers().await);
                        if !confirmed_response(
                            std::time::Duration::from_secs(10),
                            http.create_message(message.channel_id)
                                .content(&content)
                                .allowed_mentions(Some(&no_mentions))
                                .into_future(),
                        )
                        .await
                        {
                            eprintln!("Worker情報の送信エラーまたはタイムアウト（結果不明）");
                        }
                        return;
                    }
                    let mut response_guard = match manager
                        .begin_response_delivery(message.channel_id.get())
                        .await
                    {
                        Ok(guard) => guard,
                        Err(error) => {
                            eprintln!(
                                "{}",
                                failure_message("応答追跡を開始できません", error.as_ref())
                            );
                            return;
                        }
                    };
                    match manager
                        .dispatch_key_with_response(
                            &mut response_guard,
                            message.channel_id.get(),
                            message.content.clone(),
                        )
                        .await
                    {
                        Ok(Some(content)) => {
                            let content = clamp_reply(&content);
                            let status = tokio::time::timeout(
                                Duration::from_secs(10),
                                http.create_message(message.channel_id)
                                    .content(&content)
                                    .allowed_mentions(Some(&no_mentions))
                                    .into_future(),
                            )
                            .await
                            .ok()
                            .and_then(Result::ok)
                            .map(|response| response.status().get());
                            if manager
                                .finish_response(response_guard, api_outcome(status))
                                .await
                                .is_err()
                            {
                                eprintln!("REST 結果の Worker 受領を確認できません（結果不明）");
                            }
                            if status.is_none() {
                                eprintln!("送信エラーまたはタイムアウト（結果不明）");
                            }
                        }
                        Ok(None) => response_guard.confirm(),
                        Err(error) => {
                            response_guard.confirm();
                            eprintln!("{}", failure_message("Worker error", error.as_ref()));
                        }
                    }
                });
            }
            Ok(Event::InteractionCreate(event))
                if event.kind == InteractionType::ApplicationCommand =>
            {
                let interaction = event.0;
                let Some(InteractionData::ApplicationCommand(ref data)) = interaction.data else {
                    continue;
                };
                let Some((input, args)) = slash_input(&data.name, data.kind, &data.options) else {
                    reject_interaction(
                        &http,
                        interaction.application_id,
                        interaction.id,
                        &interaction.token,
                        UNSUPPORTED_REPLY,
                    )
                    .await;
                    continue;
                };
                let Some(channel) = interaction.channel.as_ref() else {
                    reject_interaction(
                        &http,
                        interaction.application_id,
                        interaction.id,
                        &interaction.token,
                        UNSUPPORTED_REPLY,
                    )
                    .await;
                    continue;
                };
                let key = channel.id.get();
                let manager = manager.clone();
                let http = http.clone();
                let Ok(permit) = capacity.clone().try_acquire_owned() else {
                    eprintln!("Interaction を受け付けられません: 混雑中");
                    // Inline and short-timeout on purpose: no extra task under overload.
                    reject_interaction(
                        &http,
                        interaction.application_id,
                        interaction.id,
                        &interaction.token,
                        BUSY_REPLY,
                    )
                    .await;
                    continue;
                };
                requests.spawn(async move {
                    let _permit = permit;
                    // The identity never enters the Worker or the logs.
                    let identity = (
                        interaction.application_id,
                        interaction.id,
                        Arc::new(Secret::new(interaction.token)),
                    );
                    let ack_http = http.clone();
                    let update_http = http.clone();
                    // The response guard is reserved only after the deferred ACK is confirmed,
                    // so a worker swap/reload cannot eat the 3-second ACK deadline.
                    let guard_slot: Arc<Mutex<Option<worker::ResponseGuard>>> =
                        Arc::new(Mutex::new(None));
                    let dispatch_slot = guard_slot.clone();
                    let progress = Arc::new(Mutex::new(ResponseProgress::default()));
                    let dispatch_progress = progress.clone();
                    let update_progress = progress.clone();
                    let dispatch_manager = manager.clone();
                    let outcome = src::interaction_flow::run(
                        identity,
                        std::time::Duration::from_secs(2),
                        std::time::Duration::from_secs(10),
                        move |(application_id, interaction_id, token)| async move {
                            let response = InteractionResponse {
                                kind: InteractionResponseType::DeferredChannelMessageWithSource,
                                data: None,
                            };
                            ack_http
                                .interaction(application_id)
                                .create_response(interaction_id, token.expose_secret(), &response)
                                .await
                                .is_ok()
                        },
                        move || async move {
                            let mut dispatch_guard = match tokio::time::timeout(
                                Duration::from_secs(10),
                                dispatch_manager.begin_response_delivery(key),
                            )
                            .await
                            {
                                Ok(Ok(guard)) => guard,
                                Ok(Err(error)) => {
                                    eprintln!(
                                        "{}",
                                        failure_message("応答追跡を開始できません", error.as_ref())
                                    );
                                    return "コマンドを実行できませんでした".to_owned();
                                }
                                Err(_) => {
                                    eprintln!("応答追跡の開始がタイムアウトしました");
                                    return BUSY_REPLY.to_owned();
                                }
                            };
                            let message = match dispatch_manager
                                .dispatch_slash_with_response(&mut dispatch_guard, key, input, args)
                                .await
                            {
                                Ok(Some(message)) => {
                                    dispatch_progress.lock().unwrap().dispatched = true;
                                    message
                                }
                                Ok(None) => "コマンドの応答がありません".to_owned(),
                                Err(_) => "コマンドを実行できませんでした".to_owned(),
                            };
                            *dispatch_slot.lock().unwrap() = Some(dispatch_guard);
                            message
                        },
                        move |(application_id, _, token), message| async move {
                            let message = clamp_reply(&message);
                            let no_mentions = AllowedMentions::default();
                            let result = update_http
                                .interaction(application_id)
                                .update_response(token.expose_secret())
                                .content(Some(&message))
                                .allowed_mentions(Some(&no_mentions))
                                .await;
                            let status = result.ok().map(|response| response.status().get());
                            update_progress.lock().unwrap().status = status;
                            status.is_some()
                        },
                    )
                    .await;
                    let (dispatched, status) = {
                        let progress = progress.lock().unwrap();
                        (progress.dispatched, progress.status)
                    };
                    let response_guard = guard_slot.lock().unwrap().take();
                    if let Some(response_guard) = response_guard {
                        if dispatched {
                            if manager
                                .finish_response(response_guard, api_outcome(status))
                                .await
                                .is_err()
                            {
                                eprintln!("REST 結果の Worker 受領を確認できません（結果不明）");
                            }
                        } else if outcome == src::interaction_flow::Outcome::Completed {
                            response_guard.confirm();
                        }
                    }
                    if outcome != src::interaction_flow::Outcome::Completed {
                        eprintln!("Interaction の応答処理に失敗しました（結果不明）: {outcome:?}");
                    }
                });
            }
            Ok(Event::GatewayClose(frame)) => {
                // Only the numeric close code: it is not a secret and tells the operator
                // why Discord closed the connection (4004, 4014, 4000-series, ...).
                match frame {
                    Some(frame) => eprintln!("Gateway 切断: close code {}", frame.code),
                    None => eprintln!("Gateway 切断: close code なし（接続の途絶）"),
                }
            }
            Err(error) => eprintln!("{}", failure_message("Gateway error", &error)),
            _ => {}
        }
    }
    drop(shard);
    tokio::time::timeout(Duration::from_secs(30), async {
        while requests.join_next().await.is_some() {}
    })
    .await
    .map_err(|_| StartupFailure::ShutdownFailed)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn clamp_reply_limits_to_2000_characters_and_never_leaves_empty() {
        assert_eq!(clamp_reply("hello"), "hello");
        let exact = "a".repeat(2000);
        assert_eq!(clamp_reply(&exact), exact);
        let over = "a".repeat(2001);
        let clamped = clamp_reply(&over);
        assert_eq!(clamped.chars().count(), 1998);
        assert_eq!(clamped, format!("{}…", "a".repeat(1997)));
        // Multibyte characters are counted as characters and never split.
        let wide = "あ".repeat(2000);
        assert_eq!(clamp_reply(&wide), wide);
        let wide_over = "あ".repeat(2001);
        assert_eq!(clamp_reply(&wide_over), format!("{}…", "あ".repeat(1997)));
        assert_eq!(clamp_reply(""), EMPTY_REPLY);
        assert_eq!(clamp_reply(" \n\t"), EMPTY_REPLY);
    }

    #[test]
    fn external_diagnostics_child() {
        let Ok(secret) = std::env::var("REDSEC_MAIN_OUTPUT_FIXTURE") else {
            return;
        };
        src::redact::install_panic_hook();
        let io = std::io::Error::new(std::io::ErrorKind::PermissionDenied, secret.clone());
        eprintln!("{}", failure_message("Worker error", &io));
        let json = serde_json::from_value::<u64>(serde_json::json!(secret)).unwrap_err();
        eprintln!("{}", failure_message("Gateway error", &json));
        let unknown: AppError = secret.into();
        eprintln!(
            "{}",
            failure_message("実行に失敗しました", unknown.as_ref())
        );
        std::process::exit(1);
    }

    #[test]
    fn process_output_retains_causes_without_external_error_contents() {
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "tests::external_diagnostics_child", "--nocapture"])
            .env("REDSEC_MAIN_OUTPUT_FIXTURE", "short-private-value token=dummy-token secret=dummy-secret Authorization: Bearer dummy-auth private_key=dummy-key")
            .output().unwrap();
        assert_eq!(output.status.code(), Some(1));
        let text = format!(
            "{}{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        for secret in [
            "short-private-value",
            "dummy-token",
            "dummy-secret",
            "dummy-auth",
            "dummy-key",
        ] {
            assert!(!text.contains(secret));
        }
        assert!(text.contains("kind=PermissionDenied"));
        assert!(text.contains("category=Data"));
        assert!(text.contains("[REDACTED]"));
    }

    fn managed_args() -> Vec<OsString> {
        let root = env::current_dir().unwrap();
        let mut args = vec![
            "--bot-id".into(),
            "101".into(),
            "--shard-id".into(),
            "1".into(),
            "--total-shards".into(),
            "2".into(),
            "--token-env".into(),
            "TOKEN_ALPHA".into(),
        ];
        for (flag, name) in [
            ("--ipc-path", "ipc"),
            ("--state-path", "gateway-state"),
            ("--management-path", "management"),
        ] {
            args.extend([OsString::from(flag), root.join(name).into_os_string()]);
        }
        args
    }

    #[test]
    fn launch_arguments_require_explicit_mock_selection_and_complete_identity() {
        let normal = GatewayOptions::parse(managed_args()).unwrap();
        assert!(normal.mock_discord.is_none());
        assert_eq!(normal.token_env(), "TOKEN_ALPHA");
        assert!(normal.path("--state-path").unwrap().is_absolute());
        for address in ["127.0.0.1:12345", "[::1]:12345"] {
            let mut args = managed_args();
            args.extend(["--mock-discord".into(), address.into()]);
            let mock = GatewayOptions::parse(args).unwrap();
            assert_eq!(mock.mock_discord, Some(address.parse().unwrap()));
        }
        assert!(
            GatewayOptions::parse(Vec::new())
                .unwrap()
                .mock_discord
                .is_none()
        );
        let mut incomplete = managed_args();
        incomplete.truncate(incomplete.len() - 2);
        assert!(GatewayOptions::parse(incomplete).is_err());
    }

    #[test]
    fn rejects_duplicate_unknown_ambiguous_and_remote_launch_arguments() {
        for extra in [
            vec!["--bot-id", "101"],
            vec!["--worker"],
            vec!["--local-control"],
            vec!["--mock-discord"],
            vec!["--unknown", "sensitive-input"],
            vec!["--mock-discord", "0.0.0.0:12345"],
            vec!["--mock-discord", "192.0.2.1:12345"],
            vec!["--mock-discord", "127.0.0.1:0"],
            vec!["--mock-discord", "localhost:12345"],
            vec![
                "--mock-discord",
                "127.0.0.1:12345",
                "--mock-discord",
                "127.0.0.1:12345",
            ],
        ] {
            let mut args = managed_args();
            args.extend(extra.into_iter().map(OsString::from));
            assert!(GatewayOptions::parse(args).is_err());
        }
        assert!(
            GatewayOptions::parse(["--local-control", "--local-control"].map(OsString::from))
                .is_err()
        );
    }

    #[test]
    fn launch_paths_and_token_references_are_validated_without_echoing_values() {
        for (index, bad) in [
            (7, "SECRET=VALUE"),
            (9, "relative"),
            (11, "../outside"),
            (13, ""),
        ] {
            let mut args = managed_args();
            args[index] = bad.into();
            let error = GatewayOptions::parse(args).err().unwrap();
            assert_eq!(
                error.to_string(),
                "invalid gateway launch arguments [launch_invalid]"
            );
        }
        let mut args = managed_args();
        args[9] = env::current_dir()
            .unwrap()
            .join("..")
            .join("outside")
            .into_os_string();
        assert!(GatewayOptions::parse(args).is_err());
    }

    #[test]
    fn gateway_bot_ids_reject_non_numeric_and_numeric_namespace_aliases() {
        for id in [
            "001",
            "+1",
            " 1",
            "1 ",
            "18446744073709551616",
            "bot-alpha",
            "",
        ] {
            let mut args = managed_args();
            args[1] = id.into();
            assert!(GatewayOptions::parse(args).is_err());
        }
        for id in ["0", "1", "18446744073709551615"] {
            let mut args = managed_args();
            args[1] = id.into();
            assert!(GatewayOptions::parse(args).is_ok());
        }
    }

    async fn frame_from(bytes: &[u8]) -> Result<Option<MockInput>, AppError> {
        let (mut input, output) = tokio::io::duplex(128 * 1024);
        input.write_all(bytes).await.unwrap();
        input.shutdown().await.unwrap();
        mock_frame(&mut BufReader::new(output)).await
    }

    #[tokio::test]
    async fn mock_input_is_bounded_typed_and_never_echoes_input() {
        assert!(matches!(
            frame_from(b"{\"type\":\"status\",\"id\":9}\n")
                .await
                .unwrap(),
            Some(MockInput::Status { id: 9 })
        ));
        assert!(frame_from(b"").await.unwrap().is_none());
        for bytes in [
            b"{\"type\":\"status\",\"id\":9,\"unexpected\":\"sensitive-input\"}\n".to_vec(),
            b"{\"type\":\"crash\",\"id\":9,\"unexpected\":true}\n".to_vec(),
            b"{\"type\":\"sensitive-input\"}\n".to_vec(),
            b"{\"type\":\"status\",\"id\":9}".to_vec(),
            vec![b'x'; MOCK_FRAME_LIMIT as usize + 1],
            b"\n".to_vec(),
        ] {
            let error = frame_from(&bytes).await.err().unwrap();
            assert_eq!(error.to_string(), "invalid mock input [mock_input_invalid]");
        }
    }

    #[tokio::test]
    async fn mock_http_confirms_only_complete_testkit_replies_without_retry() {
        use src::testkit::{MockDiscord, MockReply};
        for (reply, expected) in [
            (MockReply::json(201, b"{}").unwrap(), Some(201)),
            (MockReply::interaction_ack(), Some(204)),
            (MockReply::rate_limited(Duration::from_secs(1)), Some(429)),
            (MockReply::server_error(503).unwrap(), Some(503)),
            (
                MockReply::json(200, b"{}").unwrap().with_lost_response(),
                None,
            ),
            (
                MockReply::json(200, b"{}")
                    .unwrap()
                    .with_truncated_response(10_000),
                None,
            ),
        ] {
            let mock = MockDiscord::new([reply]);
            let server = mock.spawn_tcp().await.unwrap();
            assert_eq!(
                mock_http(
                    server.addr(),
                    "POST",
                    "/channels/1/messages",
                    json!({"content":"Pong"})
                )
                .await,
                expected
            );
            assert_eq!(mock.stats().requests, 1);
            assert_eq!(mock.stats().script_exhausted, 0);
            server.shutdown().await.unwrap();
        }
        assert_eq!(
            mock_http(
                "192.0.2.1:12345".parse().unwrap(),
                "POST",
                "/channels/1/messages",
                json!({})
            )
            .await,
            None
        );
    }

    #[test]
    fn failure_output_never_formats_external_error_text() {
        struct UntrustedError;
        impl std::fmt::Display for UntrustedError {
            fn fmt(&self, _: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                panic!("the log boundary must not inspect external Display")
            }
        }
        impl std::fmt::Debug for UntrustedError {
            fn fmt(&self, _: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                panic!("the log boundary must not inspect external Debug")
            }
        }
        impl Error for UntrustedError {
            fn source(&self) -> Option<&(dyn Error + 'static)> {
                panic!("the log boundary must not inspect unknown sources")
            }
        }
        for context in [
            "実行に失敗しました",
            "Ready イベントを処理できません",
            "応答追跡を開始できません",
            "Worker error",
            "Gateway error",
        ] {
            assert_eq!(
                failure_message(context, &UntrustedError),
                format!("{context}: [REDACTED]")
            );
            let untrusted: AppError = "short-private-value".into();
            assert_eq!(
                failure_message(context, untrusted.as_ref()),
                format!("{context}: [REDACTED]")
            );
        }
    }

    #[test]
    fn token_environment_errors_are_fixed_and_valid_values_remain_usable() {
        let token = token_from_env(Ok("short-private-value".into())).unwrap();
        assert_eq!(token.expose_secret(), "short-private-value");
        assert_eq!(format!("{token:?}"), "[REDACTED]");
        assert_eq!(
            token_from_env(Err(env::VarError::NotPresent))
                .unwrap_err()
                .to_string(),
            "DISCORD_TOKEN is not configured [token_missing]"
        );
        let error = token_from_env(Err(env::VarError::NotUnicode("short-private-value".into())))
            .unwrap_err();
        assert_eq!(
            error.to_string(),
            "DISCORD_TOKEN is not Unicode [token_encoding]"
        );
        assert!(!format!("{error:?}").contains("short-private-value"));
    }

    #[test]
    fn gateway_diagnostics_exclude_untrusted_event_body() {
        use twilight_gateway::error::ReceiveMessageErrorType;
        let kind = ReceiveMessageErrorType::Deserializing {
            event: "short-private-value token=fake-token".into(),
        };
        assert_eq!(gateway_failure_code(&kind), "gateway_deserialization");
        assert_eq!(
            gateway_failure_code(&ReceiveMessageErrorType::Reconnect),
            "gateway_reconnect"
        );
        assert_eq!(
            gateway_failure_code(&ReceiveMessageErrorType::Compression),
            "gateway_compression"
        );
    }

    #[tokio::test]
    async fn protected_interaction_identity_reaches_ack_and_update_unchanged() {
        let token = Arc::new(Secret::new("short-private-value".to_owned()));
        let expected = token.clone();
        let result = src::interaction_flow::run(
            (1, 2, token),
            std::time::Duration::from_secs(1),
            std::time::Duration::from_secs(1),
            |(application, interaction, token)| async move {
                assert_eq!((application, interaction), (1, 2));
                assert_eq!(token.expose_secret(), "short-private-value");
                true
            },
            || async { "reply".into() },
            move |(application, interaction, token), message| async move {
                assert_eq!((application, interaction), (1, 2));
                assert!(Arc::ptr_eq(&token, &expected));
                assert_eq!(message, "reply");
                true
            },
        )
        .await;
        assert_eq!(result, src::interaction_flow::Outcome::Completed);
    }

    #[tokio::test]
    async fn response_timeout_is_not_confirmed() {
        assert!(
            !confirmed_response(
                std::time::Duration::from_millis(10),
                std::future::pending::<Result<(), ()>>()
            )
            .await
        );
        assert!(
            !confirmed_response(std::time::Duration::from_secs(1), async {
                Err::<(), ()>(())
            })
            .await
        );
        assert!(
            confirmed_response(std::time::Duration::from_secs(1), async {
                Ok::<(), ()>(())
            })
            .await
        );
    }

    #[tokio::test]
    async fn metadata_failures_and_invalid_limits_prevent_queue_construction() {
        let valid = SessionStartLimit {
            max_concurrency: 1,
            remaining: 1,
            reset_after: 10_000,
            total: 1,
        };
        assert!(matches!(
            single_shard_queue(1, async { Err(()) }).await,
            Err(StartupFailure::MetadataUnavailable)
        ));
        assert!(matches!(
            single_shard_queue(1, async { Ok((2, valid.clone())) }).await,
            Err(StartupFailure::InsufficientShards)
        ));
        let mut invalid = valid.clone();
        invalid.max_concurrency = 0;
        assert!(matches!(
            single_shard_queue(1, async { Ok((1, invalid)) }).await,
            Err(StartupFailure::InvalidIdentifyLimits)
        ));
        assert!(
            single_shard_queue(1, async { Ok((1, valid.clone())) })
                .await
                .is_ok()
        );
        assert!(
            single_shard_queue(4, async { Ok((4, valid)) })
                .await
                .is_ok()
        );
    }

    #[test]
    fn gateway_uses_configured_shard() {
        let deployment = worker::DeploymentIdentity::parse(|key| {
            Ok(match key {
                "SHARD_ID" => Some("2".into()),
                "TOTAL_SHARDS" => Some("4".into()),
                _ => None,
            })
        })
        .unwrap();
        assert_eq!(configured_shard(&deployment), ShardId::new(2, 4));
    }

    #[test]
    fn routes_typed_chat_commands_and_rejects_other_variants() {
        use bot_framework::arguments::SlashValue;
        let options = vec![
            CommandDataOption {
                name: "text".into(),
                value: CommandOptionValue::String("hi".into()),
            },
            CommandDataOption {
                name: "n".into(),
                value: CommandOptionValue::Integer(7),
            },
            CommandDataOption {
                name: "flag".into(),
                value: CommandOptionValue::Boolean(true),
            },
        ];
        assert_eq!(
            slash_input("ping", CommandType::ChatInput, &options),
            Some((
                "ping".into(),
                vec![
                    ("text".into(), SlashValue::String("hi".into())),
                    ("n".into(), SlashValue::Integer(7)),
                    ("flag".into(), SlashValue::Boolean(true))
                ]
            ))
        );
        assert_eq!(slash_input("count", CommandType::ChatInput, &[]), None);
        assert_eq!(slash_input("ping", CommandType::User, &[]), None);
        let unsupported = vec![CommandDataOption {
            name: "n".into(),
            value: CommandOptionValue::Number(1.0),
        }];
        assert_eq!(
            slash_input("ping", CommandType::ChatInput, &unsupported),
            None
        );
    }
}
