use crate::{
    AppError,
    redact::Secret,
    response_drain::{ResponseDrain, ResponseOperation},
};
use bot_framework::interactions::InteractionKind;
use serde::{Deserialize, Serialize};
use std::{
    path::{Path, PathBuf},
    process::Stdio,
    sync::{
        Arc, Mutex as StdMutex,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use tokio::{
    io::{AsyncBufRead, AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    process::{Child, ChildStdin, ChildStdout, Command},
    sync::{Mutex, Notify, OwnedRwLockReadGuard, RwLock},
    time::timeout,
};

const PROTOCOL: u32 = 5;
const CAPABILITIES: &[&str] = &["snapshot-v3"];
const EVENT_SEQ_CAPABILITY: &str = "event-seq-v1";
const EVENT_REPLAY_CAPABILITY: &str = "event-replay-v1";
const HELLO_CAPABILITY: &str = "hello-v1";
const CONCURRENT_DRAIN_CAPABILITY: &str = "concurrent-drain-v1";
const API_RESULT_CAPABILITY: &str = "api-result-v1";
const INTERACTION_CAPABILITY: &str = "interaction-state-v1";
const DRAIN_SIGNAL_CAPABILITY: &str = "drain-signal-v1";
const OPTIONAL_CAPABILITIES: &[&str] = &[
    INTERACTION_CAPABILITY,
    DRAIN_SIGNAL_CAPABILITY,
    API_RESULT_CAPABILITY,
    CONCURRENT_DRAIN_CAPABILITY,
    EVENT_SEQ_CAPABILITY,
    EVENT_REPLAY_CAPABILITY,
    "hello-v1",
    "command-dispatch-v1",
    "ready-event-v1",
    "slash-arguments-v1",
];
const IDENTITY_ENV: &str = "BOT_WORKER_IPC_IDENTITY";

/// Deployment metadata only; this is not an ownership/lease grant.
/// BOT_ID, SHARD_ID, TOTAL_SHARDS default to 0, 0, 1. GENERATION_ID,
/// when supplied, is the first child generation, incremented on every spawn.
pub struct DeploymentIdentity {
    pub bot_id: crate::protocol::BotId,
    pub shard: crate::protocol::Shard,
    generation: Option<std::sync::atomic::AtomicU64>,
    core_launch: Option<crate::shard_runtime::test_switch::CoreLaunch>,
    interaction_state: Option<InteractionStateConfig>,
}
impl DeploymentIdentity {
    pub fn from_env() -> Result<Self, AppError> {
        Self::parse(|name| match std::env::var(name) {
            Ok(value) => Ok(Some(value)),
            Err(std::env::VarError::NotPresent) => Ok(None),
            Err(_) => Err("invalid deployment environment"),
        })
    }

    /// Pure parser for environment-like inputs; errors never echo input values.
    pub fn parse(
        mut lookup: impl FnMut(&str) -> Result<Option<String>, &'static str>,
    ) -> Result<Self, AppError> {
        fn number(value: Option<String>, default: u64) -> Result<u64, AppError> {
            match value {
                None => Ok(default),
                Some(value) if !value.is_empty() && value.bytes().all(|b| b.is_ascii_digit()) => {
                    value
                        .parse()
                        .map_err(|_| failure("invalid deployment number"))
                }
                Some(_) => Err(failure("invalid deployment number")),
            }
        }
        let mut checked_lookup =
            |name| lookup(name).map_err(|_| failure("invalid deployment environment"));
        let bot_id = crate::protocol::BotId(number(checked_lookup("BOT_ID")?, 0)?);
        let id = u32::try_from(number(checked_lookup("SHARD_ID")?, 0)?)
            .map_err(|_| failure("invalid shard id"))?;
        let total = u32::try_from(number(checked_lookup("TOTAL_SHARDS")?, 1)?)
            .map_err(|_| failure("invalid shard total"))?;
        let generation = checked_lookup("GENERATION_ID")?
            .map(|value| number(Some(value), 0).map(std::sync::atomic::AtomicU64::new))
            .transpose()?;
        Ok(Self {
            bot_id,
            shard: crate::protocol::Shard::new(id, total).map_err(failure)?,
            generation,
            core_launch: None,
            interaction_state: None,
        })
    }

    /// Trusted launch configuration, retained across replacement and recovery.
    /// The independent signing key travels only on the child's anonymous pipe.
    pub fn with_interaction_state(mut self, config: InteractionStateConfig) -> Self {
        self.interaction_state = Some(config);
        self
    }

    fn next_identity(&self) -> Result<crate::protocol::Identity, AppError> {
        use std::sync::atomic::Ordering;
        let generation = match &self.generation {
            Some(next) => next
                .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| n.checked_add(1))
                .map_err(|_| failure("worker generation exhausted"))?,
            None => getrandom::u64()?,
        };
        Ok(crate::protocol::Identity {
            bot_id: self.bot_id,
            shard: self.shard,
            generation_id: crate::protocol::GenerationId(generation),
            ownership_epoch: crate::protocol::OwnershipEpoch(0),
        })
    }
}

/// Optional local application state, never a Bot token. Keep the key in the
/// caller's protected configuration across Manager restarts; it is not saved
/// in the state journal or child environment. No Debug implementation exposes it.
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InteractionStateConfig {
    path: PathBuf,
    namespace: crate::state_store::PublicLabel,
    signing_key: [u8; 32],
    supported_versions: Vec<u16>,
    schema: crate::state_store::SchemaVersion,
    max_entries: usize,
    max_bytes: usize,
    max_value_bytes: usize,
    max_journal_bytes: u64,
}
impl InteractionStateConfig {
    pub fn file(
        path: PathBuf,
        namespace: crate::state_store::PublicLabel,
        signing_key: [u8; 32],
        supported_versions: Vec<u16>,
        schema: crate::state_store::SchemaVersion,
    ) -> Self {
        Self {
            path,
            namespace,
            signing_key,
            supported_versions,
            schema,
            max_entries: 4096,
            max_bytes: 16 * 1024 * 1024,
            max_value_bytes: 64 * 1024,
            max_journal_bytes: 64 * 1024 * 1024,
        }
    }

    pub fn with_limits(
        mut self,
        limits: crate::state_store::StoreLimits,
        journal_bytes: u64,
    ) -> Self {
        self.max_entries = limits.max_entries;
        self.max_bytes = limits.max_bytes;
        self.max_value_bytes = limits.max_value_bytes;
        self.max_journal_bytes = journal_bytes;
        self
    }

    async fn adapter(
        self,
        identity: &crate::protocol::Identity,
        backend: Option<Arc<dyn crate::state_store::StateStore>>,
    ) -> Result<crate::state_store::FrameworkStateStore, AppError> {
        use crate::state_store::{
            FileStateStore, FrameworkStateStore, StoreLimits, StoreScope, SystemClock,
        };
        let store = match backend {
            Some(store) => store,
            None => Arc::new(
                FileStateStore::open(
                    &self.path,
                    StoreLimits {
                        max_entries: self.max_entries,
                        max_bytes: self.max_bytes,
                        max_value_bytes: self.max_value_bytes,
                    },
                    self.max_journal_bytes,
                    Arc::new(SystemClock),
                )
                .await?,
            ),
        };
        Ok(FrameworkStateStore::new(
            store,
            StoreScope {
                bot_id: identity.bot_id,
                shard_id: identity.shard.id(),
            },
            self.namespace,
            self.signing_key,
            self.supported_versions,
            self.schema,
        )?)
    }
}

fn local_hello(identity: crate::protocol::Identity) -> crate::protocol::Hello {
    crate::protocol::Hello {
        protocol_min: PROTOCOL as u16,
        protocol_max: PROTOCOL as u16,
        identity,
        required_capabilities: CAPABILITIES.iter().map(|s| (*s).into()).collect(),
        optional_capabilities: OPTIONAL_CAPABILITIES.iter().map(|s| (*s).into()).collect(),
    }
}

const STATE_VERSION: u32 = 3;
const MAX_FRAME_BYTES: u64 = 1024 * 1024;
const DEADLINE: Duration = Duration::from_secs(5);
const APPLICATION_DEADLINE: Duration = Duration::from_secs(30);
const DRAIN_DEADLINE: Duration = Duration::from_secs(60);

// Only code-owned diagnostics may cross a log boundary unchanged. In particular,
// a peer's Rejected reason, serde's input text and an IO error's custom payload
// are never treated as trusted merely because they implement Error.
#[derive(Debug)]
struct WorkerFailure {
    message: &'static str,
    exit_code: Option<i32>,
}
impl std::fmt::Display for WorkerFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.message)
    }
}
impl std::error::Error for WorkerFailure {}

fn failure(message: &'static str) -> AppError {
    Box::new(WorkerFailure {
        message,
        exit_code: None,
    })
}

/// Safe diagnostics without formatting an arbitrary error or traversing sources.
pub fn diagnostic(error: &(dyn std::error::Error + 'static)) -> String {
    if let Some(error) = error.downcast_ref::<WorkerFailure>() {
        match error.exit_code {
            Some(code) => format!("{} (exit_code={code})", error.message),
            None => error.message.to_owned(),
        }
    } else if let Some(error) = error.downcast_ref::<std::io::Error>() {
        format!(
            "IO failure (kind={:?}, os_code={:?})",
            error.kind(),
            error.raw_os_error()
        )
    } else if let Some(error) = error.downcast_ref::<serde_json::Error>() {
        format!(
            "invalid JSON (category={:?}, line={}, column={})",
            error.classify(),
            error.line(),
            error.column()
        )
    } else {
        crate::redact::MASK.to_owned()
    }
}

// Child diagnostics are arbitrary application/library output. Neither line
// masking nor a known-secret list can protect split or unlabelled credentials.
// The parent reports protocol/IO failures and the observed child exit code.
fn protect_child_diagnostics(command: &mut Command) {
    command.stderr(Stdio::null());
}

fn worker_description(index: usize, build: &str, pid: u32) -> String {
    format!("{}: {} / PID: {}", index + 1, Secret::new(build), pid)
}

// Frames are newline-delimited JSON. Limit reads before allocating untrusted input.
async fn read_frame<R: AsyncBufRead + Unpin>(reader: &mut R) -> Result<Option<Vec<u8>>, AppError> {
    let mut frame = Vec::new();
    reader
        .take(MAX_FRAME_BYTES + 1)
        .read_until(b'\n', &mut frame)
        .await?;
    if frame.is_empty() {
        return Ok(None);
    }
    if frame.len() as u64 > MAX_FRAME_BYTES || frame.last() != Some(&b'\n') {
        return Err(failure("invalid or oversized worker frame"));
    }
    Ok(Some(frame))
}

pub fn worker_file() -> PathBuf {
    std::env::var_os("DISCORD_WORKER_FILE")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("worker-path.txt"))
}

pub fn status_file(file: &Path) -> PathBuf {
    let mut path = file.as_os_str().to_owned();
    path.push(".status");
    PathBuf::from(path)
}

pub fn error_file(file: &Path) -> PathBuf {
    let mut path = status_file(file).into_os_string();
    path.push(".error");
    PathBuf::from(path)
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum Request {
    ConfigureInteractions {
        config: InteractionStateConfig,
    },
    /// One-way control notification, read independently of application work.
    BeginDrain,
    Interaction {
        kind: InteractionKind,
        custom_id: String,
        values: Vec<String>,
    },
    Hello {
        hello: crate::protocol::Hello,
    },
    Health,
    ResponseDispatch {
        operation_id: u64,
        request: Box<Request>,
    },
    ApiResult {
        operation_id: u64,
        outcome: ApiOutcome,
    },
    Dispatch {
        key: u64,
        content: String,
    },
    Command {
        name: String,
    },
    SlashCommand {
        name: String,
        args: Vec<(String, bot_framework::arguments::SlashValue)>,
    },
    SequencedReady {
        local_sequence: u64,
        bot_name: String,
    },
    RestoreEventStream {
        next_sequence: Option<u64>,
    },
    ReadyEvent {
        bot_name: String,
    },
    Tick,
    Snapshot,
    Restore {
        version: u32,
        state: serde_json::Value,
    },
}
impl Request {
    fn application(&self) -> bool {
        if let Self::ResponseDispatch { request, .. } = self {
            return request.application();
        }
        matches!(
            self,
            Self::Dispatch { .. }
                | Self::Interaction { .. }
                | Self::Command { .. }
                | Self::SlashCommand { .. }
                | Self::ReadyEvent { .. }
                | Self::SequencedReady { .. }
                | Self::Tick
        )
    }

    // snapshot-v3 owns only legacy count/job state. FrameworkRuntime is explicitly
    // generation-local. Ready also needs this barrier for its ACK/replay cursor.
    // concurrent-drain-v1 promises that all other requests leave snapshot-v3
    // unchanged; validate that promise on every result before accepting it.
    fn state_barrier(&self) -> bool {
        match self {
            Self::ResponseDispatch { request, .. } => request.state_barrier(),
            Self::Dispatch { content, .. } => {
                let name = content.split_whitespace().next().unwrap_or("");
                matches!(name, "!count" | "!job")
            }
            Self::Command { .. } | Self::SlashCommand { .. } | Self::Interaction { .. } => false,
            _ => true,
        }
    }
}
#[derive(Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum Response {
    InteractionsConfigured,
    ApiResultAccepted {
        operation_id: u64,
        outcome: ApiOutcome,
        identity: crate::protocol::Identity,
        pid: u32,
    },
    EventStreamRestored,
    EventAccepted {
        local_sequence: u64,
    },
    Hello {
        hello: crate::protocol::Hello,
    },
    Ready {
        protocol: u32,
        #[serde(default)]
        protocol_min: Option<u32>,
        #[serde(default)]
        protocol_max: Option<u32>,
        #[serde(default)]
        required_capabilities: Vec<String>,
        #[serde(default)]
        supported_capabilities: Vec<String>,
        #[serde(default)]
        credential_free: bool,
        build: String,
        pid: u32,
    },
    Result {
        content: Option<String>,
        state: serde_json::Value,
    },
    Snapshot {
        version: u32,
        state: serde_json::Value,
    },
    Restored,
    Rejected {
        reason: String,
    },
}

/// Sanitized transport result only: credentials and HTTP bodies never enter Core.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ApiOutcome {
    Confirmed { status: u16 },
    Unknown,
}

/// A receipt received on the same pipe that executed the response-bearing command.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ApiReceipt {
    pub operation_id: u64,
    pub worker_index: usize,
    pub pid: u32,
    pub identity: crate::protocol::Identity,
    pub outcome: ApiOutcome,
}

/// Identity is absent for legacy peers which did not complete bidirectional Hello.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WorkerIdentity {
    pub index: usize,
    pub pid: u32,
    pub identity: Option<crate::protocol::Identity>,
    pub credential_free: bool,
}

// Legacy protocol 5 workers omit the range. Partial ranges are malformed.
fn validate_hello(min: Option<u32>, max: Option<u32>, required: &[String]) -> Result<(), AppError> {
    match (min, max) {
        (None, None) => {}
        (Some(min), Some(max)) if min <= PROTOCOL && PROTOCOL <= max => {}
        _ => return Err(failure("incompatible worker protocol range")),
    }
    if required
        .iter()
        .any(|capability| !CAPABILITIES.contains(&capability.as_str()))
    {
        return Err(failure("unsupported required worker capability"));
    }
    Ok(())
}

// This transaction protects only the in-memory demo state; external effects cannot be rolled back.
fn checked_result(
    state: &mut crate::cogs::State,
    previous: crate::cogs::State,
    content: Option<String>,
) -> Result<Response, AppError> {
    let value = serde_json::to_value(&*state)?;
    let result = Response::Result {
        content,
        state: value.clone(),
    };
    let snapshot = Response::Snapshot {
        version: STATE_VERSION,
        state: value.clone(),
    };
    let restore = Request::Restore {
        version: STATE_VERSION,
        state: value,
    };
    let fits = [
        serde_json::to_vec(&result)?.len(),
        serde_json::to_vec(&snapshot)?.len(),
        serde_json::to_vec(&restore)?.len(),
    ]
    .into_iter()
    .all(|size| size < MAX_FRAME_BYTES as usize);
    if fits {
        Ok(result)
    } else {
        *state = previous;
        Ok(Response::Rejected {
            reason: "state or response exceeds worker frame limit".into(),
        })
    }
}

// A handler error is a request failure, not a Worker failure. Restore the
// in-memory demo state and avoid returning potentially sensitive error text.
fn command_result(
    state: &mut crate::cogs::State,
    previous: crate::cogs::State,
    result: Result<Option<String>, AppError>,
) -> Result<Response, AppError> {
    match result {
        Ok(content) => checked_result(state, previous, content),
        Err(_) => {
            *state = previous;
            Ok(Response::Rejected {
                reason: "command handler failed".into(),
            })
        }
    }
}

#[derive(Clone)]
struct TransferState {
    events: crate::event_seq::Sender,
    checkpoint: Option<serde_json::Value>,
}

struct Process {
    _child: Child,
    input: Arc<Mutex<Option<ChildStdin>>>,
    output: BufReader<ChildStdout>,
    broken: bool,
    transfer: Arc<StdMutex<TransferState>>,
    build: String,
    pid: u32,
    supported_capabilities: Vec<String>,
    identity: Option<crate::protocol::Identity>,
    credential_free: bool,
}

#[derive(Clone, Copy, Default)]
struct LaunchPolicy {
    isolated: bool,
    require_gateway_capabilities: bool,
}

impl Process {
    async fn start(
        path: &Path,
        deployment: &DeploymentIdentity,
        policy: LaunchPolicy,
    ) -> Result<Self, AppError> {
        let identity = deployment.next_identity()?;
        let mut hello = local_hello(identity.clone());
        if deployment.interaction_state.is_none() {
            // Existing command-only peers and relays keep their EOF drain
            // contract. Negotiate new wire controls only when configured.
            hello
                .optional_capabilities
                .retain(|cap| cap != INTERACTION_CAPABILITY && cap != DRAIN_SIGNAL_CAPABILITY);
        }
        let mut command = Command::new(path);
        if policy.isolated || deployment.interaction_state.is_some() {
            command.env_clear();
        }
        if let Some(core) = &deployment.core_launch {
            command.args(core.args_for(&identity));
        } else {
            command.arg("--worker");
        }
        protect_child_diagnostics(&mut command);
        let mut child = command
            .env(IDENTITY_ENV, serde_json::to_string(&identity)?)
            .env_remove("DISCORD_TOKEN")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .kill_on_drop(true)
            .spawn()?;
        let input = child
            .stdin
            .take()
            .ok_or_else(|| failure("missing worker stdin"))?;
        let output = BufReader::new(
            child
                .stdout
                .take()
                .ok_or_else(|| failure("missing worker stdout"))?,
        );
        let mut process = Self {
            _child: child,
            input: Arc::new(Mutex::new(Some(input))),
            output,
            broken: false,
            transfer: Arc::new(StdMutex::new(TransferState {
                events: crate::event_seq::Sender::new(64, 0, MAX_FRAME_BYTES as usize, 0)
                    .map_err(|_| failure("invalid event buffer"))?,
                checkpoint: None,
            })),
            build: String::new(),
            pid: 0,
            supported_capabilities: Vec::new(),
            identity: None,
            credential_free: false,
        };
        match process.request(&Request::Health).await? {
            Response::Ready {
                protocol: PROTOCOL,
                protocol_min,
                protocol_max,
                required_capabilities,
                supported_capabilities,
                credential_free,
                build,
                pid,
            } => {
                validate_hello(protocol_min, protocol_max, &required_capabilities)?;
                // Optional reflection by older v5 peers is not an advertisement
                // that they implement new semantics or new wire requests.
                for cap in [
                    EVENT_REPLAY_CAPABILITY,
                    CONCURRENT_DRAIN_CAPABILITY,
                    API_RESULT_CAPABILITY,
                    INTERACTION_CAPABILITY,
                    DRAIN_SIGNAL_CAPABILITY,
                ] {
                    if !supported_capabilities.iter().any(|value| value == cap) {
                        hello.optional_capabilities.retain(|value| value != cap);
                    }
                }
                if supported_capabilities
                    .iter()
                    .any(|cap| cap == HELLO_CAPABILITY)
                {
                    match process
                        .request(&Request::Hello {
                            hello: hello.clone(),
                        })
                        .await?
                    {
                        Response::Hello { hello: peer } => {
                            let (_, negotiated) = hello.negotiate(&peer).map_err(failure)?;
                            process.supported_capabilities = negotiated;
                            process.identity = Some(peer.identity);
                            process.credential_free = credential_free;
                        }
                        _ => return Err(failure("worker hello rejected")),
                    }
                } else {
                    process.supported_capabilities = supported_capabilities
                        .into_iter()
                        .filter(|cap| {
                            cap != EVENT_SEQ_CAPABILITY
                                && cap != CONCURRENT_DRAIN_CAPABILITY
                                && cap != API_RESULT_CAPABILITY
                                && cap != INTERACTION_CAPABILITY
                                && cap != DRAIN_SIGNAL_CAPABILITY
                        })
                        .collect();
                }
                process.build = build;
                process.pid = pid;
                if policy.require_gateway_capabilities
                    && [
                        API_RESULT_CAPABILITY,
                        "command-dispatch-v1",
                        "slash-arguments-v1",
                        "ready-event-v1",
                    ]
                    .iter()
                    .any(|cap| !process.supports(cap))
                {
                    return Err(failure("worker lacks required Gateway capabilities"));
                }
                if process.supports(API_RESULT_CAPABILITY) && process._child.id() != Some(pid) {
                    return Err(failure("worker API result process identity mismatch"));
                }
                if let Some(config) = &deployment.interaction_state {
                    if !process.supports(INTERACTION_CAPABILITY)
                        || !process.supports(DRAIN_SIGNAL_CAPABILITY)
                        || !process.supports(CONCURRENT_DRAIN_CAPABILITY)
                        || process.identity.is_none()
                    {
                        return Err(failure("worker lacks interaction state capabilities"));
                    }
                    match process
                        .request(&Request::ConfigureInteractions {
                            config: config.clone(),
                        })
                        .await?
                    {
                        Response::InteractionsConfigured => {}
                        _ => {
                            return Err(failure("worker interaction state configuration rejected"));
                        }
                    }
                }
                if process.supports(CONCURRENT_DRAIN_CAPABILITY) {
                    // Establish a confirmed initial snapshot, including custom
                    // initial state, before admitting any application work.
                    match process.request(&Request::Snapshot).await? {
                        Response::Snapshot {
                            version: STATE_VERSION,
                            state,
                        } => {
                            process.transfer.lock().unwrap().checkpoint = Some(state);
                        }
                        _ => return Err(failure("invalid initial worker snapshot")),
                    }
                }
                Ok(process)
            }
            _ => Err(failure("incompatible worker protocol")),
        }
    }

    async fn exited(&mut self, after_ack: bool) -> AppError {
        // EOF normally precedes exit notification very briefly. Bound the wait
        // so a peer that only closes stdout cannot stall fault reporting.
        let exit_code = timeout(Duration::from_millis(100), self._child.wait())
            .await
            .ok()
            .and_then(Result::ok)
            .and_then(|status| status.code());
        Box::new(WorkerFailure {
            message: if after_ack {
                "worker exited after ACK"
            } else {
                "worker exited"
            },
            exit_code,
        })
    }

    async fn request(&mut self, request: &Request) -> Result<Response, AppError> {
        if self.broken {
            return Err(failure("worker is unavailable"));
        }
        let sequenced;
        let request = if let Request::ReadyEvent { bot_name } = request
            && self.supports(EVENT_SEQ_CAPABILITY)
        {
            let sequence = self
                .transfer
                .lock()
                .unwrap()
                .events
                .enqueue(
                    serde_json::to_vec(request)?,
                    crate::event_seq::Priority::Required,
                )
                .map_err(|_| failure("worker event buffer full or event too large"))?;
            sequenced = Request::SequencedReady {
                local_sequence: sequence,
                bot_name: bot_name.clone(),
            };
            &sequenced
        } else {
            request
        };
        let expected_ack = match request {
            Request::SequencedReady { local_sequence, .. } => Some(*local_sequence),
            _ => None,
        };
        // Poison before awaiting: cancellation must never reuse a leftover frame.
        self.broken = true;
        let deadline = if request.application() {
            APPLICATION_DEADLINE
        } else {
            DEADLINE
        };
        let result = timeout(deadline, async {
            let mut data = serde_json::to_vec(request)?;
            data.push(b'\n');
            if data.len() > MAX_FRAME_BYTES as usize {
                return Err(failure("worker request exceeds frame limit"));
            }
            timeout(DEADLINE, async {
                let mut writer = self.input.lock().await;
                let input = writer.as_mut().ok_or_else(|| {
                    std::io::Error::new(std::io::ErrorKind::BrokenPipe, "worker input is closed")
                })?;
                input.write_all(&data).await?;
                input.flush().await
            })
            .await
            .map_err(|_| failure("worker write timed out"))??;
            let frame = if expected_ack.is_some() {
                timeout(DEADLINE, read_frame(&mut self.output))
                    .await
                    .map_err(|_| failure("worker event ACK timed out"))??
            } else {
                read_frame(&mut self.output).await?
            };
            let Some(frame) = frame else {
                return Err(self.exited(false).await);
            };
            if let Some(expected) = expected_ack {
                match serde_json::from_slice::<Response>(&frame)
                    .map_err(|_| failure("invalid worker response"))?
                {
                    Response::EventAccepted { local_sequence } if local_sequence == expected => {
                        self.transfer
                            .lock()
                            .unwrap()
                            .events
                            .ack(crate::event_seq::EventAccepted(
                                crate::event_seq::SequenceRange {
                                    first: expected,
                                    last: expected,
                                },
                            ))
                            .map_err(|_| failure("invalid worker event ACK"))?;
                    }
                    _ => return Err(failure("missing or invalid worker event ACK")),
                }
                let Some(frame) = read_frame(&mut self.output).await? else {
                    return Err(self.exited(true).await);
                };
                return serde_json::from_slice(&frame)
                    .map_err(|_| failure("invalid worker response"));
            }
            serde_json::from_slice(&frame).map_err(|_| failure("invalid worker response"))
        })
        .await
        .map_err(|_| {
            failure(match request {
                Request::Health => "worker health timed out",
                Request::Hello { .. } => "worker hello timed out",
                Request::ConfigureInteractions { .. } => {
                    "worker interaction configuration timed out"
                }
                Request::Snapshot => "worker snapshot timed out",
                _ => "worker timed out",
            })
        })
        .and_then(|result| result);
        if result.is_ok() {
            self.broken = false;
        }
        let transfer = self.transfer.lock().unwrap();
        if result.is_err() && transfer.events.retained_len() != 0 {
            eprintln!(
                "worker event ACK missing: unacked={:?}, retained_bytes={}",
                transfer.events.unacked_ranges(),
                transfer.events.retained_bytes()
            );
        }
        result
    }

    fn supports(&self, capability: &str) -> bool {
        self.supported_capabilities
            .iter()
            .any(|cap| cap == capability)
    }

    async fn restore(&mut self, version: u32, state: serde_json::Value) -> Result<(), AppError> {
        match self
            .request(&Request::Restore {
                version,
                state: state.clone(),
            })
            .await?
        {
            Response::Restored => self.transfer.lock().unwrap().checkpoint = Some(state),
            Response::Rejected { .. } => {
                return Err(failure("worker state restore rejected: [REDACTED]"));
            }
            _ => return Err(failure("worker state restore not confirmed")),
        }
        Ok(())
    }

    /// Install a stable copy while the old generation's state/ACK barrier is held.
    async fn inherit_events(
        &mut self,
        previous: &TransferState,
        capabilities: &[String],
    ) -> Result<(), AppError> {
        let compatible = self.supports(EVENT_SEQ_CAPABILITY)
            && self.supports(EVENT_REPLAY_CAPABILITY)
            && [EVENT_SEQ_CAPABILITY, EVENT_REPLAY_CAPABILITY]
                .iter()
                .all(|cap| capabilities.iter().any(|value| value == cap));
        if !compatible {
            if previous.events.retained_len() != 0 {
                return Err(failure(
                    "unacknowledged events require negotiated replay support",
                ));
            }
            return Ok(());
        }
        match self
            .request(&Request::RestoreEventStream {
                next_sequence: previous.events.receiver_checkpoint(),
            })
            .await?
        {
            Response::EventStreamRestored => {}
            _ => return Err(failure("worker event stream restore rejected")),
        }
        self.transfer.lock().unwrap().events = previous.events.clone();
        let pending: Vec<_> = previous.events.replay().cloned().collect();
        for event in pending {
            let Request::ReadyEvent { bot_name } = serde_json::from_slice(&event.payload)? else {
                return Err(failure("invalid retained worker event"));
            };
            match self
                .request(&Request::SequencedReady {
                    local_sequence: event.local_sequence,
                    bot_name,
                })
                .await?
            {
                Response::Result { state, .. } => {
                    self.transfer.lock().unwrap().checkpoint = Some(state)
                }
                Response::Rejected { .. } => return Err(failure("replayed worker event rejected")),
                _ => return Err(failure("unexpected replay response")),
            }
        }
        Ok(())
    }
}

// A generation stays alive through both IPC work and its external response.
struct Generation {
    path: PathBuf,
    process: Mutex<Process>,
    transfer: Arc<StdMutex<TransferState>>,
    build: String,
    pid: u32,
    capabilities: Vec<String>,
    identity: Option<crate::protocol::Identity>,
    credential_free: bool,
    state_barrier: Arc<RwLock<()>>,
    work: Arc<RwLock<()>>,
    responses: ResponseDrain,
    drain: tokio::sync::watch::Sender<bool>,
    drain_task: Mutex<Option<tokio::task::JoinHandle<Result<(), AppError>>>>,
}
impl Generation {
    fn new(path: PathBuf, process: Process) -> Arc<Self> {
        let (drain, mut receiver) = tokio::sync::watch::channel(false);
        let drain_task = if process.supports(DRAIN_SIGNAL_CAPABILITY) {
            let input = process.input.clone();
            Some(tokio::spawn(async move {
                if receiver.changed().await.is_err() || !*receiver.borrow() {
                    return Ok(());
                }
                timeout(DEADLINE, async {
                    let mut writer = input.lock().await;
                    let input = writer
                        .as_mut()
                        .ok_or_else(|| failure("worker input is closed"))?;
                    input.write_all(b"{\"type\":\"begin_drain\"}\n").await?;
                    input.flush().await?;
                    Ok(())
                })
                .await
                .map_err(|_| failure("worker drain notification timed out"))?
            }))
        } else {
            None
        };
        Arc::new(Self {
            path,
            transfer: process.transfer.clone(),
            build: process.build.clone(),
            pid: process.pid,
            capabilities: process.supported_capabilities.clone(),
            identity: process.identity.clone(),
            credential_free: process.credential_free,
            process: Mutex::new(process),
            state_barrier: Arc::new(RwLock::new(())),
            work: Arc::new(RwLock::new(())),
            responses: ResponseDrain::new(1024),
            drain,
            drain_task: Mutex::new(drain_task),
        })
    }
    fn start_drain(&self) {
        self.responses.start_drain();
        self.drain.send_replace(true);
    }
    async fn wait_drain_notification(&self) -> Result<(), AppError> {
        let mut task = self.drain_task.lock().await;
        if let Some(handle) = task.as_mut() {
            let result = handle
                .await
                .map_err(|_| failure("worker drain notification failed"));
            // Completion consumes a JoinHandle even on failure. Remove it
            // before propagating errors so a retry cannot poll it twice.
            // Cancellation while awaiting still leaves the handle retained.
            task.take();
            result??;
        }
        Ok(())
    }
    fn needs_barrier(&self, request: &Request) -> bool {
        request.state_barrier()
            || !self
                .capabilities
                .iter()
                .any(|cap| cap == CONCURRENT_DRAIN_CAPABILITY)
    }
}
struct Admission {
    generation: Arc<Generation>,
    _work: Option<OwnedRwLockReadGuard<()>>,
    _state: Option<OwnedRwLockReadGuard<()>>,
}
struct RegisteredGeneration {
    index: usize,
    generation: Arc<Generation>,
    retired: bool,
}
#[derive(Default)]
struct ManagementRecord {
    file: Option<PathBuf>,
    revision: u64,
}
pub struct Manager {
    deployment: DeploymentIdentity,
    launch_policy: LaunchPolicy,
    stopping: AtomicBool,
    stopped: Notify,
    workers: Vec<RwLock<Arc<Generation>>>,
    generations: StdMutex<Vec<RegisteredGeneration>>,
    update: Mutex<()>,
    replace_lock: Mutex<()>,
    management: Mutex<ManagementRecord>,
    instance_id: String,
    gateway_admission: Option<StdMutex<GatewayAdmission>>,
}

struct GatewayAdmission {
    identity: crate::protocol::Identity,
    state: crate::shard_runtime::test_switch::State,
    last_request_id: u64,
    disconnected: bool,
}

impl GatewayAdmission {
    fn apply(
        &mut self,
        control: &crate::shard_runtime::test_switch::Control,
    ) -> Result<(), AppError> {
        use crate::shard_runtime::test_switch::{Action, State};
        let incoming = &control.identity;
        if self.disconnected
            || control.request_id <= self.last_request_id
            || incoming.bot_id != self.identity.bot_id
            || incoming.shard != self.identity.shard
            || incoming.generation_id != self.identity.generation_id
        {
            return Err(failure("mock Gateway control identity rejected"));
        }
        match control.action {
            Action::Status => {}
            Action::Activate => {
                if !matches!(self.state, State::Standby | State::Paused)
                    || incoming.ownership_epoch.0 <= self.identity.ownership_epoch.0
                {
                    return Err(failure("mock Gateway activation rejected"));
                }
                self.identity.ownership_epoch = incoming.ownership_epoch;
                self.state = State::Active;
            }
            Action::Pause => {
                if incoming.ownership_epoch != self.identity.ownership_epoch
                    || self.state == State::Drained
                {
                    return Err(failure("mock Gateway pause rejected"));
                }
                self.state = State::Paused;
            }
            Action::Quiesce | Action::Drain => {
                if incoming.ownership_epoch.0 < self.identity.ownership_epoch.0
                    || self.state == State::Drained
                    || (control.action == Action::Drain && self.state == State::Active)
                {
                    return Err(failure("mock Gateway quiescence rejected"));
                }
                // Advancing the fence before ACK prevents a delayed activation
                // at this epoch from reopening the retiring process.
                self.identity.ownership_epoch = incoming.ownership_epoch;
                self.state = State::Paused;
            }
        }
        self.last_request_id = control.request_id;
        Ok(())
    }
}

/// Reserve before dispatch and keep until the external outcome is known. Use
/// dispatch_*_with_response to bind execution to this exact generation.
#[must_use]
pub struct ResponseGuard {
    generation: Arc<Generation>,
    index: usize,
    instance_id: String,
    _work: OwnedRwLockReadGuard<()>,
    dispatched: bool,
    deliver_result: bool,
    expects_result: bool,
    operation: ResponseOperation,
}
impl ResponseGuard {
    /// Confirm local bookkeeping. For result-delivery reservations this is only
    /// valid when no response was produced: use Manager::finish_response for
    /// Some(content), including failures/unknown outcomes after HTTP dispatch.
    pub fn confirm(self) {
        self.operation.confirm();
    }
}

impl Manager {
    pub async fn worker_identities(&self) -> Vec<WorkerIdentity> {
        let mut identities = Vec::with_capacity(self.workers.len());
        for (index, worker) in self.workers.iter().enumerate() {
            let generation = worker.read().await;
            identities.push(WorkerIdentity {
                index,
                pid: generation.pid,
                identity: generation.identity.clone(),
                credential_free: generation.credential_free,
            });
        }
        identities
    }
    pub async fn describe_workers(&self) -> String {
        let mut lines = vec!["稼働中のWorker:".to_string()];
        for (index, worker) in self.workers.iter().enumerate() {
            let generation = worker.read().await;
            lines.push(worker_description(index, &generation.build, generation.pid));
        }
        lines.join("\n")
    }
    pub async fn start(path: PathBuf) -> Result<Arc<Self>, AppError> {
        Self::start_pool(path, 2).await
    }
    pub async fn start_pool(path: PathBuf, count: usize) -> Result<Arc<Self>, AppError> {
        Self::start_pool_with_identity(path, count, DeploymentIdentity::from_env()?).await
    }
    pub async fn start_pool_with_identity(
        path: PathBuf,
        count: usize,
        deployment: DeploymentIdentity,
    ) -> Result<Arc<Self>, AppError> {
        Self::start_configured_pool(path, count, deployment, LaunchPolicy::default()).await
    }

    /// Managed Gateway launch: Core receives only its negotiated IPC identity.
    /// Preserve the policy through recovery and replacement, not just startup.
    pub async fn start_pool_isolated(
        path: PathBuf,
        count: usize,
        deployment: DeploymentIdentity,
    ) -> Result<Arc<Self>, AppError> {
        Self::start_configured_pool(
            path,
            count,
            deployment,
            LaunchPolicy {
                isolated: true,
                require_gateway_capabilities: false,
            },
        )
        .await
    }

    /// Gateway contract: reject startup, replacement and recovery candidates
    /// before admission unless they can execute commands and return REST results.
    /// Generic Manager constructors retain legacy v5 compatibility.
    pub async fn start_gateway_pool(
        path: PathBuf,
        count: usize,
        mut deployment: DeploymentIdentity,
    ) -> Result<Arc<Self>, AppError> {
        let launch = mock_switch_launch(&deployment)?;
        let bootstrap = if launch.is_some() {
            Some(mock_switch_bootstrap().await?)
        } else {
            None
        };
        let (path, count) = if let Some(launch) = &launch {
            deployment.core_launch = Some(launch.core.clone());
            (launch.core.program.clone(), 1)
        } else {
            (path, count)
        };
        let manager = Self::start_configured_pool(
            path,
            count,
            deployment,
            LaunchPolicy {
                isolated: true,
                require_gateway_capabilities: true,
            },
        )
        .await?;
        if let Some(launch) = launch {
            manager
                .start_mock_switch_control(launch, bootstrap.expect("explicit bootstrap"))
                .await?;
        }
        Ok(manager)
    }

    async fn start_configured_pool(
        path: PathBuf,
        count: usize,
        deployment: DeploymentIdentity,
        launch_policy: LaunchPolicy,
    ) -> Result<Arc<Self>, AppError> {
        if count == 0 {
            return Err(failure("Worker数は1以上にしてください"));
        }
        let mut workers = Vec::with_capacity(count);
        let mut generations = Vec::with_capacity(count);
        for index in 0..count {
            let process = Process::start(&path, &deployment, launch_policy).await?;
            let generation = Generation::new(path.clone(), process);
            generations.push(RegisteredGeneration {
                index,
                generation: generation.clone(),
                retired: false,
            });
            workers.push(RwLock::new(generation));
        }
        let mut instance = [0u8; 16];
        getrandom::fill(&mut instance)?;
        let instance_id = instance.iter().map(|byte| format!("{byte:02x}")).collect();
        let gateway_admission = deployment.core_launch.as_ref().map(|core| {
            StdMutex::new(GatewayAdmission {
                identity: core.identity.clone(),
                state: crate::shard_runtime::test_switch::State::Standby,
                last_request_id: 0,
                disconnected: false,
            })
        });
        Ok(Arc::new(Self {
            deployment,
            launch_policy,
            stopping: AtomicBool::new(false),
            stopped: Notify::new(),
            workers,
            instance_id,
            generations: StdMutex::new(generations),
            update: Mutex::new(()),
            replace_lock: Mutex::new(()),
            management: Mutex::new(ManagementRecord::default()),
            gateway_admission,
        }))
    }

    /// A short synchronous admission boundary, never held across IPC/await.
    /// Paused ACK is ordered after all previously admitted work, while their
    /// response guards remain valid until their external outcomes are settled.
    fn gateway_admit(
        &self,
    ) -> Result<Option<std::sync::MutexGuard<'_, GatewayAdmission>>, AppError> {
        let gate = self
            .gateway_admission
            .as_ref()
            .map(|gate| gate.lock().unwrap());
        if gate.as_ref().is_some_and(|gate| {
            gate.disconnected || gate.state != crate::shard_runtime::test_switch::State::Active
        }) {
            return Err(failure("mock Gateway is not active"));
        }
        Ok(gate)
    }

    fn gateway_active(&self) -> bool {
        self.gateway_admit().is_ok()
    }

    fn close_gateway_admission(&self) {
        if let Some(gate) = &self.gateway_admission {
            let mut gate = gate.lock().unwrap();
            gate.disconnected = true;
            if gate.state != crate::shard_runtime::test_switch::State::Drained {
                gate.state = crate::shard_runtime::test_switch::State::Paused;
            }
        }
    }

    async fn start_mock_switch_control(
        self: &Arc<Self>,
        launch: crate::shard_runtime::test_switch::Launch,
        bootstrap: crate::shard_runtime::test_switch::Bootstrap,
    ) -> Result<(), AppError> {
        use crate::shard_runtime::test_switch::Hello;
        let mut stream = timeout(DEADLINE, tokio::net::TcpStream::connect(launch.address))
            .await
            .map_err(|_| failure("mock Gateway control connection timed out"))?
            .map_err(|_| failure("mock Gateway control unavailable"))?;
        let hello = Hello {
            secret: bootstrap.secret,
            identity: launch.gateway,
            pid: std::process::id(),
            workers: self.worker_identities().await,
        };
        mock_control_write(&mut stream, &hello).await?;
        let manager = self.clone();
        tokio::spawn(async move {
            let _ = manager.mock_switch_session(&mut stream).await;
            // EOF, timeout, malformed input and failed ACK writes all close
            // admission. Already accepted work and result delivery survive.
            // Keep the socket owned until the gate is closed: observing peer
            // EOF must not race a still-open admission boundary.
            manager.close_gateway_admission();
        });
        Ok(())
    }

    async fn mock_switch_session(
        &self,
        stream: &mut tokio::net::TcpStream,
    ) -> Result<(), AppError> {
        use crate::shard_runtime::test_switch::{Action, Control, IDLE_TIMEOUT, Reply, State};
        let (reader, mut writer) = stream.split();
        let mut reader = BufReader::new(reader);
        loop {
            let control: Control = timeout(IDLE_TIMEOUT, mock_control_read(&mut reader))
                .await
                .map_err(|_| failure("mock Gateway control timed out"))??;
            {
                let mut gate = self
                    .gateway_admission
                    .as_ref()
                    .expect("mock launch gate")
                    .lock()
                    .unwrap();
                gate.apply(&control)?;
            }
            if control.action == Action::Drain {
                // No receiver timeout may cancel an in-progress drain. Its
                // own bounded shutdown waits for response tracking and reap.
                self.shutdown().await?;
                self.gateway_admission
                    .as_ref()
                    .expect("mock launch gate")
                    .lock()
                    .unwrap()
                    .state = State::Drained;
            }
            let (identity, state) = {
                let gate = self
                    .gateway_admission
                    .as_ref()
                    .expect("mock launch gate")
                    .lock()
                    .unwrap();
                (gate.identity.clone(), gate.state)
            };
            let reply = Reply {
                request_id: control.request_id,
                identity,
                state,
                workers: self.worker_identities().await,
            };
            mock_control_write(&mut writer, &reply).await?;
        }
    }

    fn install(&self, index: usize, active: &mut Arc<Generation>, next: Arc<Generation>) {
        // No await between closing admission, registration and routing commit.
        active.start_drain();
        let mut generations = self.generations.lock().unwrap();
        for entry in generations.iter_mut() {
            if Arc::ptr_eq(&entry.generation, active) {
                entry.retired = true;
            }
        }
        generations.push(RegisteredGeneration {
            index,
            generation: next.clone(),
            retired: false,
        });
        *active = next;
    }

    fn reap_retired(&self) {
        self.generations.lock().unwrap().retain(|entry| {
            if !entry.retired {
                return true;
            }
            let Ok(_work) = entry.generation.work.try_write() else {
                return true;
            };
            !entry.generation.responses.unknown_ids().is_empty()
        });
    }

    async fn retire_previous(&self, index: usize) -> Result<(), AppError> {
        let retired: Vec<_> = self
            .generations
            .lock()
            .unwrap()
            .iter()
            .filter(|entry| entry.index == index && entry.retired)
            .map(|entry| entry.generation.clone())
            .collect();
        for generation in retired {
            generation.wait_drain_notification().await?;
            // This also waits for external response guards. Cancellation leaves
            // the registered generation available for later drain/reconciliation.
            let _work = timeout(DRAIN_DEADLINE, generation.work.write())
                .await
                .map_err(|_| failure("worker drain timed out; previous generation retained"))?;
            if !generation.responses.wait_settled().await.is_empty() {
                return Err(failure(
                    "unresolved response outcomes prevent worker update",
                ));
            }
            self.generations
                .lock()
                .unwrap()
                .retain(|entry| !Arc::ptr_eq(&entry.generation, &generation));
        }
        Ok(())
    }

    pub async fn replace(&self, path: PathBuf, force: bool) -> Result<(), AppError> {
        if self.gateway_admission.is_some() {
            return Err(failure("mock Shard Core replacement is daemon controlled"));
        }
        let _replacement = self.replace_lock.lock().await;
        self.require_running()?;
        for (index, worker) in self.workers.iter().enumerate() {
            self.require_running()?;
            self.retire_previous(index).await?;
            if !force && worker.read().await.path == path {
                continue;
            }
            if self
                .unknown_responses()
                .1
                .iter()
                .any(|(slot, _)| *slot == index)
            {
                return Err(failure(
                    "unresolved response outcomes prevent worker update",
                ));
            }
            // Readiness failures must not stop old admission.
            let mut candidate = Process::start(&path, &self.deployment, self.launch_policy).await?;
            let recovery = self.update.lock().await;
            self.require_running()?;
            // Recovery can install a generation while the candidate is starting.
            // Recheck under the recovery lock before allocating another retained
            // generation or bypassing an old unknown external outcome.
            self.reap_retired();
            if self
                .unknown_responses()
                .1
                .iter()
                .any(|(slot, _)| *slot == index)
            {
                return Err(failure(
                    "unresolved response outcomes prevent worker update",
                ));
            }
            if self
                .generations
                .lock()
                .unwrap()
                .iter()
                .any(|entry| entry.index == index && entry.retired)
            {
                return Err(failure("previous worker generation is still draining"));
            }
            let previous = worker.read().await.clone();
            let state_boundary = timeout(DRAIN_DEADLINE, previous.state_barrier.write())
                .await
                .map_err(|_| failure("worker state handoff timed out"))?;
            let mut active = worker.write().await;
            if !force && active.path == path {
                continue;
            }
            if !previous.responses.unknown_ids().is_empty() {
                return Err(failure(
                    "unresolved response outcomes prevent worker update",
                ));
            }
            let transfer = if previous
                .capabilities
                .iter()
                .any(|cap| cap == CONCURRENT_DRAIN_CAPABILITY)
            {
                // snapshot-v3 mutations and event cursor changes are quiescent;
                // independent in-flight handlers may continue on the old pipe.
                previous.transfer.lock().unwrap().clone()
            } else {
                let mut process = previous.process.lock().await;
                if !process.broken {
                    match process.request(&Request::Snapshot).await? {
                        Response::Snapshot {
                            version: STATE_VERSION,
                            state,
                        } => {
                            process.transfer.lock().unwrap().checkpoint = Some(state);
                        }
                        _ => return Err(failure("invalid worker snapshot")),
                    }
                }
                process.transfer.lock().unwrap().clone()
            };
            let state = transfer
                .checkpoint
                .clone()
                .unwrap_or(serde_json::to_value(crate::cogs::State::default())?);
            candidate.restore(STATE_VERSION, state).await?;
            candidate
                .inherit_events(&transfer, &previous.capabilities)
                .await?;
            self.install(index, &mut active, Generation::new(path.clone(), candidate));
            drop(state_boundary);
            drop(active);
            drop(recovery);
            println!(
                "Worker {}/{} を切り替えました",
                index + 1,
                self.workers.len()
            );
            // Routing is already v2. Success means the old generation really
            // drained; unknown outcomes retain it and are returned as an error.
            self.retire_previous(index).await?;
        }
        Ok(())
    }
    /// Snapshot unresolved operations without payloads, credentials or message content.
    /// This API is intended for a future authenticated local management channel.
    pub fn unknown_responses(&self) -> (String, Vec<(usize, u64)>) {
        let ids = self
            .generations
            .lock()
            .unwrap()
            .iter()
            .flat_map(|entry| {
                entry
                    .generation
                    .responses
                    .unknown_ids()
                    .into_iter()
                    .map(|id| (entry.index, id))
                    .collect::<Vec<_>>()
            })
            .collect();
        (self.instance_id.clone(), ids)
    }

    /// Resolve one operation after an operator has verified the outcome externally.
    /// The caller must authenticate and audit the operator; this method exposes no IPC.
    /// A nonempty evidence reference is mandatory and is neither logged nor persisted.
    pub fn reconcile_response(
        &self,
        instance_id: &str,
        worker: usize,
        id: u64,
        evidence: &str,
    ) -> Result<(), AppError> {
        if instance_id != self.instance_id {
            return Err(failure("response instance mismatch"));
        }
        if evidence.trim().is_empty() {
            return Err(failure("reconciliation evidence is required"));
        }
        if worker >= self.workers.len() {
            return Err(failure("invalid worker index"));
        }
        if !self
            .generations
            .lock()
            .unwrap()
            .iter()
            .any(|entry| entry.index == worker && entry.generation.responses.resolve_unknown(id))
        {
            return Err(failure("unknown response does not exist"));
        }
        Ok(())
    }

    /// Acquire before dispatch, retain through the external response, and confirm
    /// only after a definite outcome. Dropping leaves an unresolved result.
    /// Use dispatch_*_with_response for generation attribution. If routing changes
    /// between this reservation and dispatch, it is rejected without sending work.
    pub async fn begin_response(&self, key: u64) -> Result<ResponseGuard, AppError> {
        self.reserve_response(key, false).await
    }

    /// Reserve a response whose sanitized REST result must return over Worker IPC.
    /// A supported peer is required before any command or external effect starts.
    pub async fn begin_response_delivery(&self, key: u64) -> Result<ResponseGuard, AppError> {
        self.reserve_response(key, true).await
    }

    async fn reserve_response(
        &self,
        key: u64,
        deliver_result: bool,
    ) -> Result<ResponseGuard, AppError> {
        self.require_running()?;
        drop(self.gateway_admit()?);
        let index = self.worker_index(key);
        let active = self.workers[index].read().await;
        let generation = active.clone();
        let work = generation.work.clone().read_owned().await;
        self.require_running()?;
        let _gate = self.gateway_admit()?;
        if deliver_result
            && !generation
                .capabilities
                .iter()
                .any(|cap| cap == API_RESULT_CAPABILITY)
        {
            return Err(failure("worker does not support API result delivery"));
        }
        let operation = generation
            .responses
            .begin()
            .map_err(|_| failure("response tracking unavailable"))?;
        Ok(ResponseGuard {
            generation,
            index,
            instance_id: self.instance_id.clone(),
            _work: work,
            dispatched: false,
            deliver_result,
            expects_result: false,
            operation,
        })
    }
    fn worker_index(&self, key: u64) -> usize {
        ((key ^ (key >> 22)) % self.workers.len() as u64) as usize
    }
    pub async fn dispatch_key_with_response(
        &self,
        response: &mut ResponseGuard,
        key: u64,
        content: String,
    ) -> Result<Option<String>, AppError> {
        self.execute_with_response(response, key, Request::Dispatch { key, content })
            .await
    }
    pub async fn dispatch_slash_with_response(
        &self,
        response: &mut ResponseGuard,
        key: u64,
        name: String,
        args: Vec<(String, bot_framework::arguments::SlashValue)>,
    ) -> Result<Option<String>, AppError> {
        let request = if args.is_empty() {
            Request::Command { name }
        } else {
            Request::SlashCommand { name, args }
        };
        self.execute_with_response(response, key, request).await
    }
    /// The caller supplies authenticated, authorized component/modal input.
    /// Tokens and initial Discord ACKs remain with the Gateway. Preserve this
    /// guard through sending the reply and finish_response on the issuing Core.
    pub async fn dispatch_interaction_with_response(
        &self,
        response: &mut ResponseGuard,
        key: u64,
        kind: InteractionKind,
        custom_id: String,
        values: Vec<String>,
    ) -> Result<Option<String>, AppError> {
        self.execute_with_response(
            response,
            key,
            Request::Interaction {
                kind,
                custom_id,
                values,
            },
        )
        .await
    }
    async fn execute_with_response(
        &self,
        response: &mut ResponseGuard,
        key: u64,
        request: Request,
    ) -> Result<Option<String>, AppError> {
        if response.instance_id != self.instance_id || response.index != self.worker_index(key) {
            return Err(failure("response generation mismatch"));
        }
        if response.dispatched {
            return Err(failure("response dispatch already used"));
        }
        let admission = self
            .admit(response.index, Some(&response.generation), &request)
            .await?;
        response.dispatched = true;
        let generation = admission.generation.clone();
        let request = if response.deliver_result {
            Request::ResponseDispatch {
                operation_id: response.operation.id(),
                request: Box::new(request),
            }
        } else {
            request
        };
        let result = Self::exchange(&generation, request).await;
        response.expects_result = matches!(&result, Ok(Some(_)));
        drop(admission);
        self.recover_if_broken(response.index, generation, &result)
            .await;
        result
    }

    /// Deliver once to the issuing generation, including after routing switched.
    /// An ACK mismatch, missing ACK or cancellation leaves an unknown operation;
    /// no current-generation fallback or HTTP retry is attempted.
    pub async fn finish_response(
        &self,
        response: ResponseGuard,
        outcome: ApiOutcome,
    ) -> Result<ApiReceipt, AppError> {
        if response.instance_id != self.instance_id
            || !response.deliver_result
            || !response.expects_result
        {
            return Err(failure(
                "response result does not match a dispatched operation",
            ));
        }
        if matches!(outcome, ApiOutcome::Confirmed { status } if !(200..=599).contains(&status)) {
            return Err(failure("invalid API result status"));
        }
        let expected_identity = response
            .generation
            .identity
            .as_ref()
            .ok_or_else(|| failure("API result delivery requires negotiated identity"))?;
        let mut process = timeout(APPLICATION_DEADLINE, response.generation.process.lock())
            .await
            .map_err(|_| failure("worker API result admission timed out"))?;
        let operation_id = response.operation.id();
        let receipt = match process
            .request(&Request::ApiResult {
                operation_id,
                outcome,
            })
            .await?
        {
            Response::ApiResultAccepted {
                operation_id: received,
                outcome: received_outcome,
                identity,
                pid,
            } if received == operation_id
                && received_outcome == outcome
                && &identity == expected_identity
                && pid == response.generation.pid =>
            {
                ApiReceipt {
                    operation_id,
                    worker_index: response.index,
                    pid,
                    identity,
                    outcome,
                }
            }
            _ => {
                process.broken = true;
                return Err(failure("worker API result acknowledgement mismatch"));
            }
        };
        drop(process);
        if matches!(outcome, ApiOutcome::Confirmed { .. }) {
            response.confirm();
        }
        Ok(receipt)
    }

    fn require_running(&self) -> Result<(), AppError> {
        if self.stopping.load(Ordering::Acquire) {
            Err(failure("worker manager is shutting down"))
        } else {
            Ok(())
        }
    }

    /// Stop admission, wait for owned work/results, close Core stdin and reap it.
    /// EOF is the existing Worker drain contract. Unknown outcomes are never
    /// reported as a successful graceful shutdown; the caller chooses escalation.
    pub async fn shutdown(&self) -> Result<Vec<u32>, AppError> {
        if let Some(gate) = &self.gateway_admission {
            let mut gate = gate.lock().unwrap();
            if gate.state != crate::shard_runtime::test_switch::State::Drained {
                gate.state = crate::shard_runtime::test_switch::State::Paused;
            }
        }
        self.stopping.store(true, Ordering::Release);
        self.stopped.notify_waiters();
        timeout(DRAIN_DEADLINE, async {
            let _replacement = self.replace_lock.lock().await;
            let _recovery = self.update.lock().await;
            let generations: Vec<_> = self
                .generations
                .lock()
                .unwrap()
                .iter()
                .map(|entry| entry.generation.clone())
                .collect();
            for generation in &generations {
                generation.start_drain();
            }
            for generation in &generations {
                generation.wait_drain_notification().await?;
            }
            let mut work = Vec::with_capacity(generations.len());
            let mut unknown = false;
            for generation in &generations {
                work.push(generation.work.clone().write_owned().await);
                unknown |= !generation.responses.wait_settled().await.is_empty();
            }
            // Close every pipe before waiting for any child to exit.
            for generation in &generations {
                generation.process.lock().await.input.lock().await.take();
            }
            let mut pids = Vec::with_capacity(generations.len());
            let mut failed = false;
            for generation in &generations {
                let mut process = generation.process.lock().await;
                failed |= !process._child.wait().await?.success();
                process.broken = true;
                pids.push(process.pid);
            }
            if unknown {
                Err(failure(
                    "unresolved response outcomes prevent graceful shutdown",
                ))
            } else if failed {
                Err(failure("worker exited unsuccessfully during shutdown"))
            } else {
                Ok(pids)
            }
        })
        .await
        .map_err(|_| failure("worker shutdown timed out"))?
    }
    pub async fn dispatch(&self, content: String) -> Result<Option<String>, AppError> {
        self.dispatch_key(0, content).await
    }
    /// Dispatch admitted input on this Manager's trusted Bot/Shard scope.
    /// The ID never selects a backend or changes the scope. Use the guarded
    /// variant when an external response must be attributed across a switch.
    pub async fn dispatch_interaction(
        &self,
        key: u64,
        kind: InteractionKind,
        custom_id: String,
        values: Vec<String>,
    ) -> Result<Option<String>, AppError> {
        self.execute(
            self.worker_index(key),
            Request::Interaction {
                kind,
                custom_id,
                values,
            },
        )
        .await
    }
    pub async fn dispatch_key(
        &self,
        key: u64,
        content: String,
    ) -> Result<Option<String>, AppError> {
        let index = ((key ^ (key >> 22)) % self.workers.len() as u64) as usize;
        self.execute(index, Request::Dispatch { key, content })
            .await
    }
    pub async fn dispatch_command(
        &self,
        key: u64,
        name: String,
    ) -> Result<Option<String>, AppError> {
        let index = ((key ^ (key >> 22)) % self.workers.len() as u64) as usize;
        self.execute(index, Request::Command { name }).await
    }
    pub async fn dispatch_slash(
        &self,
        key: u64,
        name: String,
        args: Vec<(String, bot_framework::arguments::SlashValue)>,
    ) -> Result<Option<String>, AppError> {
        if args.is_empty() {
            return self.dispatch_command(key, name).await;
        }
        let index = ((key ^ (key >> 22)) % self.workers.len() as u64) as usize;
        self.execute(index, Request::SlashCommand { name, args })
            .await
    }
    /// Deliver one Gateway Ready event to one active worker, rather than duplicating it
    /// across the temporary channel-partitioned worker pool.
    pub async fn dispatch_ready(&self, bot_name: String) -> Result<(), AppError> {
        self.execute(0, Request::ReadyEvent { bot_name })
            .await
            .map(|_| ())
    }
    pub async fn tick_key(&self, key: u64) -> Result<(), AppError> {
        let index = ((key ^ (key >> 22)) % self.workers.len() as u64) as usize;
        self.execute(index, Request::Tick).await.map(|_| ())
    }
    pub fn start_jobs(self: &Arc<Self>) {
        for index in 0..self.workers.len() {
            let manager = self.clone();
            tokio::spawn(async move {
                let mut interval = tokio::time::interval(Duration::from_secs(1));
                interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
                interval.tick().await;
                loop {
                    let stopped = manager.stopped.notified();
                    if manager.stopping.load(Ordering::Acquire) {
                        break;
                    }
                    tokio::select! {
                        _ = stopped => break,
                        _ = interval.tick() => {}
                    }
                    if !manager.gateway_active() {
                        continue;
                    }
                    if let Err(error) = manager.execute(index, Request::Tick).await {
                        if !manager.gateway_active() {
                            continue;
                        }
                        eprintln!(
                            "Worker {} の仕事を進められません: {}",
                            index + 1,
                            diagnostic(error.as_ref())
                        );
                    }
                }
            });
        }
    }
    async fn admit(
        &self,
        index: usize,
        expected: Option<&Arc<Generation>>,
        request: &Request,
    ) -> Result<Admission, AppError> {
        // In the managed Shard path a ResponseGuard is already an admission
        // permit. Pause/Drain may not revoke the dispatch reserved before it.
        let reserved = expected.is_some() && self.gateway_admission.is_some();
        timeout(APPLICATION_DEADLINE, async {
            loop {
                if !reserved {
                    self.require_running()?;
                    drop(self.gateway_admit()?);
                }
                let active = self.workers[index].read().await;
                let generation = active.clone();
                if expected.is_some_and(|expected| !Arc::ptr_eq(expected, &generation)) {
                    return Err(failure("response generation is no longer active"));
                }
                let state = if generation.needs_barrier(request) {
                    match generation.state_barrier.clone().try_read_owned() {
                        Ok(guard) => Some(guard),
                        Err(_) => {
                            // Never await a state boundary while holding routing.
                            // Retry admission on the current generation after the
                            // pending switch, rather than queueing stale work.
                            drop(active);
                            drop(generation.state_barrier.read().await);
                            continue;
                        }
                    }
                } else {
                    None
                };
                // Reuse the ResponseGuard's existing read permit. Acquiring a
                // second read after shutdown queued its write would deadlock:
                // the guard itself is what that writer is waiting to drain.
                let work = if reserved {
                    None
                } else {
                    Some(generation.work.clone().read_owned().await)
                };
                let _gate = if reserved {
                    None
                } else {
                    self.require_running()?;
                    self.gateway_admit()?
                };
                return Ok(Admission {
                    generation,
                    _work: work,
                    _state: state,
                });
            }
        })
        .await
        .map_err(|_| failure("worker admission timed out"))?
    }

    async fn execute(&self, index: usize, request: Request) -> Result<Option<String>, AppError> {
        let admission = self.admit(index, None, &request).await?;
        let generation = admission.generation.clone();
        let result = Self::exchange(&generation, request).await;
        drop(admission);
        self.recover_if_broken(index, generation, &result).await;
        result
    }

    async fn exchange(
        generation: &Generation,
        request: Request,
    ) -> Result<Option<String>, AppError> {
        // Per-generation IPC remains ordered; old and new have independent pipes.
        let mut process = timeout(APPLICATION_DEADLINE, generation.process.lock())
            .await
            .map_err(|_| failure("worker admission timed out"))?;
        let command = match &request {
            Request::ResponseDispatch { request, .. } => request.as_ref(),
            request => request,
        };
        if matches!(&request, Request::ResponseDispatch { .. })
            && !process.supports(API_RESULT_CAPABILITY)
        {
            return Err(failure("worker does not support API result delivery"));
        }
        let capability = match command {
            Request::Command { .. } => Some("command-dispatch-v1"),
            Request::SlashCommand { .. } => Some("slash-arguments-v1"),
            Request::ReadyEvent { .. } => Some("ready-event-v1"),
            Request::Interaction { .. } => Some(INTERACTION_CAPABILITY),
            _ => None,
        };
        if capability.is_some_and(|cap| !process.supports(cap)) {
            return Err(failure("worker does not support requested capability"));
        }
        let result = process.request(&request).await;
        match result {
            Ok(Response::Result { content, state }) => {
                let mut transfer = process.transfer.lock().unwrap();
                if !generation.needs_barrier(&request)
                    && transfer.checkpoint.as_ref() != Some(&state)
                {
                    drop(transfer);
                    process.broken = true;
                    return Err(failure("worker violated concurrent drain state contract"));
                }
                transfer.checkpoint = Some(state);
                Ok(content)
            }
            Ok(Response::Rejected { reason }) => Err(failure(match reason.as_str() {
                "command handler failed" => "worker rejected request: command handler failed",
                "managed command failed" => "worker rejected request: managed command failed",
                "state or response exceeds worker frame limit" => {
                    "worker rejected request: state or response exceeds worker frame limit"
                }
                _ => "worker rejected request: [REDACTED]",
            })),
            Ok(_) => {
                process.broken = true;
                Err(failure("unexpected worker response"))
            }
            Err(error) => Err(error),
        }
    }

    async fn recover_if_broken(
        &self,
        index: usize,
        generation: Arc<Generation>,
        result: &Result<Option<String>, AppError>,
    ) {
        let Err(error) = result else {
            return;
        };
        if !generation.process.lock().await.broken {
            return;
        }
        if self.gateway_admission.is_some() {
            // A Core failure invalidates this managed generation. Do not mint
            // a locally overlapping identity behind the daemon's plan.
            self.close_gateway_admission();
            return;
        }
        let _recovery = self.update.lock().await;
        if self.stopping.load(Ordering::Acquire) {
            return;
        }
        let mut active = self.workers[index].write().await;
        self.reap_retired();
        let recovery = if Arc::ptr_eq(&active, &generation) {
            let transfer = generation.transfer.lock().unwrap().clone();
            let restart = async {
                if self
                    .generations
                    .lock()
                    .unwrap()
                    .iter()
                    .any(|entry| entry.index == index && entry.retired)
                {
                    return Err(failure("previous worker generation is still draining"));
                }
                let mut candidate =
                    Process::start(&generation.path, &self.deployment, self.launch_policy).await?;
                self.require_running()?;
                if let Some(state) = transfer.checkpoint.clone() {
                    candidate.restore(STATE_VERSION, state).await?;
                }
                candidate
                    .inherit_events(&transfer, &generation.capabilities)
                    .await?;
                Ok::<_, AppError>(candidate)
            }
            .await;
            match restart {
                Ok(candidate) => {
                    self.install(
                        index,
                        &mut active,
                        Generation::new(generation.path.clone(), candidate),
                    );
                    eprintln!("Workerを再起動しました");
                    "restarted"
                }
                Err(restart_error) => {
                    eprintln!(
                        "Workerの再起動に失敗しました: {}",
                        diagnostic(restart_error.as_ref())
                    );
                    "failed"
                }
            }
        } else {
            "superseded"
        };
        drop(active);
        self.reap_retired();
        self.record_failure(
            &generation.path,
            error.as_ref(),
            "runtime",
            Some((index, generation.pid, recovery)),
        )
        .await;
    }

    async fn record_failure(
        &self,
        path: &Path,
        error: &(dyn std::error::Error + Send + Sync + 'static),
        phase: &str,
        runtime: Option<(usize, u32, &str)>,
    ) {
        let safe_error = diagnostic(error);
        eprintln!("Worker failure ({phase}): {safe_error}");
        let mut record = self.management.lock().await;
        record.revision = record.revision.wrapping_add(1);
        let Some(file) = &record.file else {
            return;
        };
        // path is an exact-match management control field, never a log message.
        let mut detail = serde_json::json!({"path": path.to_string_lossy(), "error": safe_error, "phase": phase});
        if let Some(error) = error.downcast_ref::<WorkerFailure>() {
            detail["exit_code"] = serde_json::json!(error.exit_code);
        }
        if let Some((index, pid, recovery)) = runtime {
            detail["worker"] = serde_json::json!(index);
            detail["pid"] = serde_json::json!(pid);
            detail["outcome"] = serde_json::json!("unknown");
            detail["recovery"] = serde_json::json!(recovery);
        }
        if let Err(error) = tokio::fs::write(error_file(file), detail.to_string()).await {
            eprintln!("更新エラーを記録できません: {}", diagnostic(&error));
        }
    }

    pub async fn watch(self: Arc<Self>, file: PathBuf) {
        if self.gateway_admission.is_some() {
            // This single Core's artifact and generation belong to the daemon
            // plan. Legacy per-pool file updates remain on their existing path.
            return;
        }
        self.management.lock().await.file = Some(file.clone());
        loop {
            let stopped = self.stopped.notified();
            if self.stopping.load(Ordering::Acquire) {
                break;
            }
            tokio::select! {
                _ = stopped => break,
                _ = tokio::time::sleep(Duration::from_secs(2)) => {}
            }
            match tokio::fs::read_to_string(&file).await {
                Ok(value) if !value.trim().is_empty() => {
                    let path = PathBuf::from(value.trim());
                    if !path.is_absolute() {
                        eprintln!("Workerのパスは絶対パスにしてください");
                        continue;
                    }
                    let revision = self.management.lock().await.revision;
                    let mut changed = false;
                    for worker in &self.workers {
                        changed |= worker.read().await.path != path;
                    }
                    let result = if changed {
                        self.replace(path.clone(), false).await
                    } else {
                        self.reap_retired();
                        Ok(())
                    };
                    match result {
                        Ok(()) => {
                            let record = self.management.lock().await;
                            if let Err(error) = tokio::fs::write(
                                status_file(&file),
                                path.to_string_lossy().as_bytes(),
                            )
                            .await
                            {
                                eprintln!("Workerの状態を記録できません: {}", diagnostic(&error));
                            }
                            // A no-op poll is not a successful recovery/update.
                            // Never erase a newer runtime fault racing this update.
                            if changed && record.revision == revision {
                                let _ = tokio::fs::remove_file(error_file(&file)).await;
                            }
                        }
                        Err(error) => {
                            self.record_failure(&path, error.as_ref(), "update", None)
                                .await
                        }
                    }
                }
                Ok(_) => eprintln!("Worker指定ファイルが空です"),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => eprintln!("Worker指定ファイルを読めません: {}", diagnostic(&error)),
            }
        }
    }
}

fn mock_switch_launch(
    deployment: &DeploymentIdentity,
) -> Result<Option<crate::shard_runtime::test_switch::Launch>, AppError> {
    use crate::shard_runtime::test_switch::{FRAME_LIMIT, LAUNCH_ENV, Launch};
    let Some(value) = std::env::var_os(LAUNCH_ENV) else {
        return Ok(None);
    };
    let value = value
        .to_str()
        .filter(|value| value.len() <= FRAME_LIMIT)
        .ok_or_else(|| failure("invalid mock Gateway launch"))?;
    let launch: Launch =
        serde_json::from_str(value).map_err(|_| failure("invalid mock Gateway launch"))?;
    launch
        .validate()
        .map_err(|_| failure("invalid mock Gateway launch"))?;
    let args = std::env::args_os().skip(1).collect::<Vec<_>>();
    let mock = args.windows(2).any(|pair| {
        pair[0] == "--mock-discord"
            && pair[1]
                .to_str()
                .and_then(|address| address.parse::<std::net::SocketAddr>().ok())
                .is_some_and(|address| address.ip().is_loopback() && address.port() != 0)
    });
    if !mock
        || args.iter().any(|arg| arg == "--local-control")
        || launch.gateway.bot_id != deployment.bot_id
        || launch.gateway.shard != deployment.shard
        || deployment
            .generation
            .as_ref()
            .map(|generation| generation.load(Ordering::Relaxed))
            != Some(launch.gateway.generation_id.0)
    {
        return Err(failure(
            "mock Gateway launch requires matching explicit mock mode",
        ));
    }
    Ok(Some(launch))
}

async fn mock_switch_bootstrap() -> Result<crate::shard_runtime::test_switch::Bootstrap, AppError> {
    use crate::shard_runtime::test_switch::{Bootstrap, FRAME_LIMIT};
    let (sender, receiver) = tokio::sync::oneshot::channel();
    // Read exactly the bootstrap line, without BufReader read-ahead stealing a
    // subsequent Shutdown frame from the existing Gateway main stdin reader.
    std::thread::spawn(move || {
        use std::io::Read;
        let read = || -> Result<Bootstrap, &'static str> {
            let input = std::io::stdin();
            let mut input = input.lock();
            let mut frame = Vec::new();
            loop {
                let mut byte = [0u8; 1];
                input
                    .read_exact(&mut byte)
                    .map_err(|_| "invalid mock control bootstrap")?;
                frame.push(byte[0]);
                if frame.len() > FRAME_LIMIT {
                    return Err("invalid mock control bootstrap");
                }
                if byte[0] == b'\n' {
                    break;
                }
            }
            let bootstrap: Bootstrap =
                serde_json::from_slice(&frame).map_err(|_| "invalid mock control bootstrap")?;
            if bootstrap.secret == [0; 32] {
                return Err("invalid mock control bootstrap");
            }
            Ok(bootstrap)
        };
        let _ = sender.send(read());
    });
    timeout(DEADLINE, receiver)
        .await
        .map_err(|_| failure("mock control bootstrap timed out"))?
        .map_err(|_| failure("mock control bootstrap unavailable"))?
        .map_err(failure)
}

async fn mock_control_read<T: serde::de::DeserializeOwned, R: AsyncBufRead + Unpin>(
    reader: &mut R,
) -> Result<T, AppError> {
    use crate::shard_runtime::test_switch::FRAME_LIMIT;
    let mut frame = Vec::new();
    reader
        .take(FRAME_LIMIT as u64 + 1)
        .read_until(b'\n', &mut frame)
        .await
        .map_err(|_| failure("mock Gateway control read failed"))?;
    if frame.len() > FRAME_LIMIT || frame.last() != Some(&b'\n') {
        return Err(failure("invalid mock Gateway control frame"));
    }
    serde_json::from_slice(&frame).map_err(|_| failure("invalid mock Gateway control frame"))
}

async fn mock_control_write<T: Serialize, W: tokio::io::AsyncWrite + Unpin>(
    writer: &mut W,
    value: &T,
) -> Result<(), AppError> {
    use crate::shard_runtime::test_switch::FRAME_LIMIT;
    let mut frame =
        serde_json::to_vec(value).map_err(|_| failure("mock Gateway control encoding failed"))?;
    frame.push(b'\n');
    if frame.len() > FRAME_LIMIT {
        return Err(failure("mock Gateway control frame too large"));
    }
    timeout(DEADLINE, async {
        writer.write_all(&frame).await?;
        writer.flush().await
    })
    .await
    .map_err(|_| failure("mock Gateway control write timed out"))?
    .map_err(|_| failure("mock Gateway control write failed"))
}

/// Internal `botd --shard-core ...` entry point. Its argv is generated from
/// placement and must agree exactly with the anonymous-pipe IPC identity.
/// No Discord client or token is needed by this process.
pub async fn run_shard_core(args: Vec<std::ffi::OsString>) -> Result<(), AppError> {
    use crate::shard_runtime::test_switch::CoreLaunch;
    use std::collections::BTreeMap;
    let mut args = args.into_iter();
    if args.next().as_deref() != Some(std::ffi::OsStr::new("--shard-core")) {
        return Err(failure("invalid Shard Core arguments"));
    }
    let mut options = BTreeMap::new();
    while let Some(flag) = args.next() {
        let Some(flag) = flag.to_str() else {
            return Err(failure("invalid Shard Core arguments"));
        };
        if ![
            "--bot-id",
            "--shard-id",
            "--total-shards",
            "--generation-id",
            "--ownership-epoch",
            "--ipc-path",
            "--state-path",
            "--management-path",
        ]
        .contains(&flag)
        {
            return Err(failure("invalid Shard Core arguments"));
        }
        let value = args
            .next()
            .ok_or_else(|| failure("invalid Shard Core arguments"))?;
        if options.insert(flag.to_owned(), value).is_some() {
            return Err(failure("invalid Shard Core arguments"));
        }
    }
    if options.len() != 8 || !credential_free_environment() {
        return Err(failure("invalid Shard Core launch environment"));
    }
    let identity: crate::protocol::Identity = std::env::var(IDENTITY_ENV)
        .ok()
        .and_then(|value| serde_json::from_str(&value).ok())
        .ok_or_else(|| failure("invalid Shard Core IPC identity"))?;
    for (flag, expected) in [
        ("--bot-id", identity.bot_id.0.to_string()),
        ("--shard-id", identity.shard.id().to_string()),
        ("--total-shards", identity.shard.total().to_string()),
        ("--generation-id", identity.generation_id.0.to_string()),
        ("--ownership-epoch", identity.ownership_epoch.0.to_string()),
    ] {
        if options[flag] != expected.as_str() {
            return Err(failure("Shard Core arguments do not match IPC identity"));
        }
    }
    CoreLaunch {
        program: std::env::current_exe().map_err(|_| failure("invalid Shard Core program"))?,
        identity,
        ipc_path: PathBuf::from(&options["--ipc-path"]),
        state_path: PathBuf::from(&options["--state-path"]),
        management_path: PathBuf::from(&options["--management-path"]),
    }
    .validate()
    .map_err(|_| failure("invalid Shard Core paths"))?;
    run().await
}

fn credential_free_environment() -> bool {
    std::env::vars_os().all(|(name, _)| {
        name == IDENTITY_ENV || (cfg!(windows) && name.eq_ignore_ascii_case("SystemRoot"))
    })
}

/// Worker process entry point. Call once and exit the process after completion
/// or cancellation; a blocked reader may retain stdin until process exit.
pub async fn run() -> Result<(), AppError> {
    run_with_backend(None).await
}

/// An application may supply its own async StateStore implementation instead
/// of the local file backend. Configuration and scope still come from the
/// trusted startup pipe; a backend must document its own durability/fencing.
/// This is a once-per-process entry point, with the same stdin lifetime as run.
pub async fn run_with_state_store(
    store: Arc<dyn crate::state_store::StateStore>,
) -> Result<(), AppError> {
    run_with_backend(Some(store)).await
}

// The bounded reader keeps drain/EOF observable while the serial application
// slot is awaiting a handler. It never dispatches application work itself.
struct WorkerReader {
    stop: Arc<AtomicBool>,
    thread: Option<std::thread::JoinHandle<()>>,
}
impl Drop for WorkerReader {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        if let Some(thread) = self.thread.take()
            && thread.is_finished()
        {
            let _ = thread.join();
        }
        // A blocking stdin read cannot be portably interrupted. On protocol
        // failure this process entry point returns and the OS closes the pipe.
        // Unlike Tokio's stdin blocking task, this owned OS thread cannot hold
        // runtime shutdown open while a peer is waiting for our stdout EOF.
    }
}

fn read_blocking_frame<R: std::io::BufRead>(reader: &mut R) -> Result<Option<Vec<u8>>, AppError> {
    let mut frame = Vec::new();
    let mut limited = std::io::Read::take(reader, MAX_FRAME_BYTES + 1);
    std::io::BufRead::read_until(&mut limited, b'\n', &mut frame)?;
    if frame.is_empty() {
        return Ok(None);
    }
    if frame.len() as u64 > MAX_FRAME_BYTES || frame.last() != Some(&b'\n') {
        return Err(failure("invalid or oversized worker frame"));
    }
    Ok(Some(frame))
}

async fn run_with_backend(
    backend: Option<Arc<dyn crate::state_store::StateStore>>,
) -> Result<(), AppError> {
    let mut output = tokio::io::stdout();
    let mut state = crate::cogs::State::default();
    let runtime = Arc::new(crate::cogs::FrameworkRuntime::default());
    bot_framework::commands()?;
    let identity = std::env::var(IDENTITY_ENV)
        .ok()
        .map(|value| serde_json::from_str::<crate::protocol::Identity>(&value))
        .transpose()?;
    let mut tasks = crate::supervisor::WorkerTasks::new(
        identity
            .as_ref()
            .map_or(crate::protocol::BotId(0), |id| id.bot_id),
        identity.as_ref().map_or(0, |id| id.shard.id()),
    )?;
    let mut sequenced_events = false;
    let mut replay_events = false;
    let mut api_results = false;
    let mut interaction_state = false;
    let mut adapter = None;
    let mut interaction_registry = None;
    let mut application_started = false;
    let drain_enabled = Arc::new(AtomicBool::new(false));
    let (drain, _) = tokio::sync::watch::channel(false);
    let (requests, mut input) = tokio::sync::mpsc::channel(1);
    let reader_drain = drain.clone();
    let reader_enabled = drain_enabled.clone();
    let reader_stop = Arc::new(AtomicBool::new(false));
    let stopped = reader_stop.clone();
    let reader = std::thread::Builder::new()
        .name("worker-input".into())
        .spawn(move || {
            let stdin = std::io::stdin();
            let mut reader = std::io::BufReader::new(stdin.lock());
            loop {
                if stopped.load(Ordering::Acquire) {
                    break;
                }
                let next = match read_blocking_frame(&mut reader) {
                    Ok(Some(frame)) => serde_json::from_slice::<Request>(&frame)
                        .map_err(|error| Box::new(error) as AppError),
                    Ok(None) => break,
                    Err(error) => Err(error),
                };
                match next {
                    Ok(Request::BeginDrain) if reader_enabled.load(Ordering::Acquire) => {
                        reader_drain.send_replace(true);
                    }
                    Ok(Request::BeginDrain) => {
                        let _ = requests
                            .blocking_send(Err(failure("worker drain signal was not negotiated")));
                        break;
                    }
                    request => {
                        let failed = request.is_err();
                        if requests.blocking_send(request).is_err() || failed {
                            break;
                        }
                    }
                }
            }
            // Parent loss cancels local waits even when no control message arrived.
            reader_drain.send_replace(true);
        })?;
    let _reader = WorkerReader {
        stop: reader_stop,
        thread: Some(reader),
    };
    let mut pending_results = std::collections::HashSet::new();
    let mut event_stream_initialized = false;
    let mut receiver = crate::event_seq::Receiver::new(0);
    while let Some(request) = input.recv().await {
        let request = request?;
        let (request, operation) = match request {
            Request::ResponseDispatch {
                operation_id,
                request,
            } => {
                if !api_results
                    || pending_results.len() >= 1024
                    || pending_results.contains(&operation_id)
                    || !matches!(
                        &*request,
                        Request::Dispatch { .. }
                            | Request::Command { .. }
                            | Request::SlashCommand { .. }
                            | Request::Interaction { .. }
                    )
                {
                    return Err(failure("invalid response-bearing dispatch"));
                }
                (*request, Some(operation_id))
            }
            request => (request, None),
        };
        let request = match request {
            Request::SequencedReady {
                local_sequence,
                bot_name,
            } => {
                if !sequenced_events {
                    return Err(failure("event sequencing was not negotiated"));
                }
                // Serial execution slot: admit at most one event before processing.
                receiver
                    .accept(local_sequence, true)
                    .map_err(|_| failure("worker event gap or duplicate"))?;
                event_stream_initialized = true;
                let mut ack = serde_json::to_vec(&Response::EventAccepted { local_sequence })?;
                ack.push(b'\n');
                output.write_all(&ack).await?;
                output.flush().await?;
                Request::ReadyEvent { bot_name }
            }
            request => request,
        };
        let response = match request {
            Request::BeginDrain => unreachable!("reader consumes one-way drain signals"),
            Request::ConfigureInteractions { config } => {
                if !interaction_state || adapter.is_some() || application_started || *drain.borrow()
                {
                    return Err(failure("worker interaction configuration not permitted"));
                }
                let id = identity
                    .as_ref()
                    .ok_or_else(|| failure("missing worker identity"))?;
                interaction_registry =
                    Some(Arc::new(bot_framework::interactions::Registry::new()?));
                adapter = Some(Arc::new(
                    config
                        .adapter(id, backend.clone())
                        .await
                        .map_err(|_| failure("worker interaction state unavailable"))?,
                ));
                Response::InteractionsConfigured
            }
            Request::ResponseDispatch { .. } => unreachable!(),
            Request::ApiResult {
                operation_id,
                outcome,
            } => {
                if !api_results
                    || matches!(outcome, ApiOutcome::Confirmed { status } if !(200..=599).contains(&status))
                    || !pending_results.remove(&operation_id)
                {
                    Response::Rejected {
                        reason: "unmatched API result".into(),
                    }
                } else {
                    Response::ApiResultAccepted {
                        operation_id,
                        outcome,
                        identity: identity
                            .clone()
                            .ok_or_else(|| failure("missing worker identity"))?,
                        pid: std::process::id(),
                    }
                }
            }
            Request::SequencedReady { .. } => unreachable!(),
            Request::RestoreEventStream { next_sequence } => {
                if replay_events && sequenced_events && !event_stream_initialized {
                    receiver = crate::event_seq::Receiver::from_checkpoint(next_sequence);
                    event_stream_initialized = true;
                    Response::EventStreamRestored
                } else {
                    Response::Rejected {
                        reason: "worker event stream restore not permitted".into(),
                    }
                }
            }
            Request::Hello { hello: peer } => match identity.clone() {
                Some(identity) => {
                    let hello = local_hello(identity);
                    match hello.negotiate(&peer) {
                        Ok((_, capabilities)) => {
                            // A drain signal is permanent for this generation;
                            // renegotiation must never reset its state.
                            interaction_state =
                                capabilities.iter().any(|cap| cap == INTERACTION_CAPABILITY);
                            drain_enabled.store(
                                capabilities
                                    .iter()
                                    .any(|cap| cap == DRAIN_SIGNAL_CAPABILITY),
                                Ordering::Release,
                            );
                            sequenced_events =
                                capabilities.iter().any(|cap| cap == EVENT_SEQ_CAPABILITY);
                            replay_events = capabilities
                                .iter()
                                .any(|cap| cap == EVENT_REPLAY_CAPABILITY);
                            api_results =
                                capabilities.iter().any(|cap| cap == API_RESULT_CAPABILITY);
                            Response::Hello { hello }
                        }
                        Err(reason) => Response::Rejected {
                            reason: reason.into(),
                        },
                    }
                }
                None => Response::Rejected {
                    reason: "missing worker identity".into(),
                },
            },
            Request::Health => Response::Ready {
                protocol: PROTOCOL,
                protocol_min: Some(PROTOCOL),
                protocol_max: Some(PROTOCOL),
                required_capabilities: CAPABILITIES.iter().map(|s| (*s).to_owned()).collect(),
                supported_capabilities: vec![
                    API_RESULT_CAPABILITY.into(),
                    CONCURRENT_DRAIN_CAPABILITY.into(),
                    EVENT_SEQ_CAPABILITY.into(),
                    EVENT_REPLAY_CAPABILITY.into(),
                    HELLO_CAPABILITY.into(),
                    "command-dispatch-v1".into(),
                    "ready-event-v1".into(),
                    "slash-arguments-v1".into(),
                    INTERACTION_CAPABILITY.into(),
                    DRAIN_SIGNAL_CAPABILITY.into(),
                ],
                // No values are inspected, serialized or logged. Windows may
                // supply SystemRoot even to a process launched with env_clear.
                credential_free: credential_free_environment(),
                build: crate::cogs::build_id().to_owned(),
                pid: std::process::id(),
            },
            Request::Interaction {
                kind,
                custom_id,
                values,
            } => {
                application_started = true;
                match (&adapter, &interaction_registry) {
                    (Some(adapter), Some(registry)) if interaction_state && !*drain.borrow() => {
                        let adapter = adapter.clone();
                        let registry = registry.clone();
                        let signal = drain.subscribe();
                        let snapshot = serde_json::to_value(&state)?;
                        tasks
                            .request(async move {
                                let replies = StdMutex::new(Vec::new());
                                let result = registry
                                    .dispatch_with_drain(
                                        adapter.as_ref(),
                                        kind,
                                        &custom_id,
                                        &values,
                                        &replies,
                                        signal,
                                    )
                                    .await;
                                // Safe framework errors remain distinct, especially
                                // ambiguous writes; never retry the application here.
                                let content = match result {
                                    Ok(()) => {
                                        let replies = replies.into_inner().map_err(|_| {
                                            failure("interaction reply sink failed")
                                        })?;
                                        if replies.is_empty() {
                                            None
                                        } else {
                                            Some(replies.join("\n"))
                                        }
                                    }
                                    Err(error) => Some(error.response().to_owned()),
                                };
                                let response = Response::Result {
                                    content,
                                    state: snapshot,
                                };
                                if serde_json::to_vec(&response)?.len() >= MAX_FRAME_BYTES as usize
                                {
                                    return Ok(Response::Rejected {
                                        reason: "state or response exceeds worker frame limit"
                                            .into(),
                                    });
                                }
                                Ok::<_, AppError>(response)
                            })
                            .await
                            .map_err(|_| failure("managed interaction failed"))??
                    }
                    _ => Response::Rejected {
                        reason: "interaction state unavailable or draining".into(),
                    },
                }
            }
            command @ (Request::Dispatch { .. }
            | Request::Command { .. }
            | Request::SlashCommand { .. }
            | Request::ReadyEvent { .. }
            | Request::Tick) => {
                application_started = true;
                let previous = state.clone();
                let owned = std::mem::take(&mut state);
                match tasks
                    .request(handle_command(command, owned, runtime.clone()))
                    .await
                {
                    Ok((current, response)) => {
                        state = current;
                        response?
                    }
                    Err(_) => {
                        state = previous;
                        Response::Rejected {
                            reason: "managed command failed".into(),
                        }
                    }
                }
            }
            Request::Snapshot => {
                // Manager already holds the exclusive routing lock. Close root
                // admission first, join descendants, then capture state. Reopen
                // locally so a cancelled/failed candidate leaves v1 usable.
                tasks.begin_drain();
                tasks.wait_drained().await?;
                let snapshot = serde_json::to_value(&state);
                tasks.resume()?;
                Response::Snapshot {
                    version: STATE_VERSION,
                    state: snapshot?,
                }
            }
            Request::Restore {
                version,
                state: incoming,
            } => {
                if version != STATE_VERSION {
                    Response::Rejected {
                        reason: format!("unsupported state version: {version}"),
                    }
                } else {
                    match serde_json::from_value(incoming) {
                        Ok(restored) => {
                            let previous = state.clone();
                            state = restored;
                            match checked_result(&mut state, previous, None)? {
                                Response::Rejected { reason } => Response::Rejected { reason },
                                _ => Response::Restored,
                            }
                        }
                        Err(_) => Response::Rejected {
                            reason: "invalid worker state".into(),
                        },
                    }
                }
            }
        };
        if let Some(operation_id) = operation
            && matches!(
                &response,
                Response::Result {
                    content: Some(_),
                    ..
                }
            )
        {
            pending_results.insert(operation_id);
        }
        let mut data = serde_json::to_vec(&response)?;
        data.push(b'\n');
        output.write_all(&data).await?;
        output.flush().await?;
    }
    tasks.begin_drain();
    tasks.wait_drained().await?;
    output.flush().await?;
    if pending_results.is_empty() {
        Ok(())
    } else {
        Err(failure("worker exited with unresolved API results"))
    }
}

async fn handle_command(
    request: Request,
    mut state: crate::cogs::State,
    runtime: Arc<crate::cogs::FrameworkRuntime>,
) -> (crate::cogs::State, Result<Response, AppError>) {
    let previous = state.clone();
    let result = match request {
        Request::Dispatch { key, content } => {
            crate::cogs::dispatch(key, &content, &mut state, &runtime).await
        }
        Request::Command { name } => crate::cogs::dispatch_command(&name, &runtime).await,
        Request::SlashCommand { name, args } => {
            crate::cogs::dispatch_slash(&name, args, &runtime).await
        }
        Request::ReadyEvent { bot_name } => crate::cogs::dispatch_ready(&runtime, bot_name)
            .await
            .map(|()| None),
        Request::Tick => {
            crate::cogs::tick(&mut state);
            Ok(None)
        }
        _ => unreachable!("only application requests are managed here"),
    };
    let response = command_result(&mut state, previous, result);
    (state, response)
}

#[cfg(test)]
mod frame_tests {
    use super::*;

    #[test]
    fn secret_diagnostics_child() {
        let Ok(secret) = std::env::var("REDSEC_WORKER_OUTPUT_FIXTURE") else {
            return;
        };
        // A custom child/library can write arbitrary, fragmented stderr.
        eprint!("{}", &secret[..secret.len() / 2]);
        eprintln!("{}", &secret[secret.len() / 2..]);
        println!("{}", worker_description(0, &secret, 123));
        std::process::exit(37);
    }

    #[tokio::test]
    async fn child_diagnostics_and_build_names_cannot_leak_secrets() {
        let secret = "short-private-value token=dummy-token secret=dummy-secret Authorization: Bearer dummy-auth private_key=dummy-key";
        let mut command = Command::new(std::env::current_exe().unwrap());
        command
            .args([
                "--exact",
                "worker::frame_tests::secret_diagnostics_child",
                "--nocapture",
            ])
            .env("REDSEC_WORKER_OUTPUT_FIXTURE", secret)
            .stdout(Stdio::piped());
        protect_child_diagnostics(&mut command);
        let output = command.spawn().unwrap().wait_with_output().await.unwrap();
        assert_eq!(output.status.code(), Some(37));
        assert!(output.stderr.is_empty());
        let stdout = String::from_utf8(output.stdout).unwrap();
        for value in [
            "short-private-value",
            "dummy-token",
            "dummy-secret",
            "dummy-auth",
            "dummy-key",
        ] {
            assert!(!stdout.contains(value));
        }
        assert!(stdout.contains("1: [REDACTED] / PID: 123"));
        let error = WorkerFailure {
            message: "worker exited",
            exit_code: output.status.code(),
        };
        assert_eq!(diagnostic(&error), "worker exited (exit_code=37)");
    }

    fn gateway_gate() -> GatewayAdmission {
        GatewayAdmission {
            identity: crate::protocol::Identity {
                bot_id: crate::protocol::BotId(101),
                shard: crate::protocol::Shard::new(1, 2).unwrap(),
                generation_id: crate::protocol::GenerationId(7),
                ownership_epoch: crate::protocol::OwnershipEpoch(0),
            },
            state: crate::shard_runtime::test_switch::State::Standby,
            last_request_id: 0,
            disconnected: false,
        }
    }

    #[test]
    fn managed_gateway_rejects_stale_activation_after_the_quiescence_barrier() {
        use crate::shard_runtime::test_switch::{Action, Control, State};
        let mut gate = gateway_gate();
        let mut control = Control {
            request_id: 1,
            identity: gate.identity.clone(),
            action: Action::Activate,
        };
        assert!(gate.apply(&control).is_err());
        control.identity.ownership_epoch.0 = 1;
        gate.apply(&control).unwrap();
        assert_eq!(gate.state, State::Active);
        control.request_id += 1;
        control.action = Action::Quiesce;
        control.identity.ownership_epoch.0 = 3;
        gate.apply(&control).unwrap();
        assert_eq!(gate.state, State::Paused);
        control.request_id += 1;
        control.action = Action::Activate;
        for stale_epoch in 0..=3 {
            control.identity.ownership_epoch.0 = stale_epoch;
            assert!(gate.apply(&control).is_err());
            assert_eq!(gate.state, State::Paused);
        }
        control.identity.ownership_epoch.0 = 4;
        gate.apply(&control).unwrap();
        assert_eq!(gate.state, State::Active);
        assert!(
            gate.apply(&control).is_err(),
            "duplicate request is rejected"
        );
    }

    #[test]
    fn managed_gateway_control_cannot_change_scope_or_drain_an_active_core() {
        use crate::shard_runtime::test_switch::{Action, Control, State};
        let mut gate = gateway_gate();
        let mut control = Control {
            request_id: 1,
            identity: gate.identity.clone(),
            action: Action::Activate,
        };
        control.identity.ownership_epoch.0 = 1;
        control.identity.shard = crate::protocol::Shard::new(0, 2).unwrap();
        assert!(gate.apply(&control).is_err());
        control.identity.shard = gate.identity.shard;
        gate.apply(&control).unwrap();
        control.request_id += 1;
        control.action = Action::Drain;
        assert!(gate.apply(&control).is_err());
        control.action = Action::Pause;
        gate.apply(&control).unwrap();
        assert_eq!(gate.state, State::Paused);
        gate.disconnected = true;
        control.request_id += 1;
        control.action = Action::Activate;
        control.identity.ownership_epoch.0 += 1;
        assert!(gate.apply(&control).is_err());
        assert_eq!(gate.state, State::Paused);
    }

    #[tokio::test]
    async fn managed_gateway_control_frames_are_bounded_and_strict() {
        use crate::shard_runtime::test_switch::{Action, Control, FRAME_LIMIT};
        let control = Control {
            request_id: 1,
            identity: gateway_gate().identity,
            action: Action::Status,
        };
        let mut bytes = serde_json::to_vec(&control).unwrap();
        assert!(
            mock_control_read::<Control, _>(&mut BufReader::new(bytes.as_slice()))
                .await
                .is_err()
        );
        bytes.push(b'\n');
        assert!(
            mock_control_read::<Control, _>(&mut BufReader::new(bytes.as_slice()))
                .await
                .is_ok()
        );
        let bytes = vec![b' '; FRAME_LIMIT + 1];
        assert!(
            mock_control_read::<Control, _>(&mut BufReader::new(bytes.as_slice()))
                .await
                .is_err()
        );
        let mut value = serde_json::to_value(control).unwrap();
        value["unrecognized"] = serde_json::json!(true);
        let bytes = format!("{value}\n");
        assert!(
            mock_control_read::<Control, _>(&mut BufReader::new(bytes.as_bytes()))
                .await
                .is_err()
        );
    }

    #[test]
    fn failed_command_preserves_state_and_redacts_error() {
        let mut state = crate::cogs::State::default();
        state.counts.insert(1, 4);
        let previous = state.clone();
        state.counts.insert(1, 5);
        let result = command_result(&mut state, previous, Err(failure("secret error"))).unwrap();
        assert_eq!(state.counts.get(&1), Some(&4));
        assert!(
            matches!(result, Response::Rejected { reason } if reason == "command handler failed")
        );
    }

    #[test]
    fn validates_compatibility_and_unknown_capabilities() {
        assert!(validate_hello(None, None, &[]).is_ok());
        assert!(validate_hello(Some(4), Some(5), &["snapshot-v3".into()]).is_ok());
        assert!(validate_hello(Some(6), Some(7), &[]).is_err());
        assert!(validate_hello(Some(5), None, &[]).is_err());
        assert!(validate_hello(Some(5), Some(5), &["unknown".into()]).is_err());
    }

    #[test]
    fn old_and_new_ready_messages_remain_decodable() {
        let legacy = r#"{"type":"ready","protocol":5,"build":"old","pid":1}"#;
        let Response::Ready {
            protocol_min,
            protocol_max,
            required_capabilities,
            ..
        } = serde_json::from_str(legacy).unwrap()
        else {
            panic!("expected ready")
        };
        assert!(validate_hello(protocol_min, protocol_max, &required_capabilities).is_ok());
        #[derive(Deserialize)]
        struct LegacyReady {
            protocol: u32,
            build: String,
            pid: u32,
        }
        let current = serde_json::to_value(Response::Ready {
            protocol: PROTOCOL,
            protocol_min: Some(PROTOCOL),
            protocol_max: Some(PROTOCOL),
            required_capabilities: vec!["snapshot-v3".into()],
            supported_capabilities: vec![
                "command-dispatch-v1".into(),
                "ready-event-v1".into(),
                "slash-arguments-v1".into(),
            ],
            credential_free: false,
            build: "new".into(),
            pid: 7,
        })
        .unwrap();
        let legacy: LegacyReady = serde_json::from_value(current).unwrap();
        assert_eq!(
            (legacy.protocol, legacy.build.as_str(), legacy.pid),
            (5, "new", 7)
        );
    }

    #[tokio::test]
    async fn reads_multiple_frames_and_eof() {
        let mut reader = BufReader::new(&b"{}\n{}\n"[..]);
        assert_eq!(read_frame(&mut reader).await.unwrap().unwrap(), b"{}\n");
        assert_eq!(read_frame(&mut reader).await.unwrap().unwrap(), b"{}\n");
        assert!(read_frame(&mut reader).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn rejects_oversized_and_truncated_frames() {
        let oversized = vec![b'x'; MAX_FRAME_BYTES as usize + 1];
        assert!(
            read_frame(&mut BufReader::new(&oversized[..]))
                .await
                .is_err()
        );
        assert!(read_frame(&mut BufReader::new(&b"{}"[..])).await.is_err());
        let mut boundary = vec![b'x'; MAX_FRAME_BYTES as usize - 1];
        boundary.push(b'\n');
        assert_eq!(
            read_frame(&mut BufReader::new(&boundary[..]))
                .await
                .unwrap()
                .unwrap()
                .len(),
            MAX_FRAME_BYTES as usize
        );
    }
}
