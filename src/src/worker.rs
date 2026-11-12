use crate::{
    AppError,
    redact::redact,
    response_drain::{ResponseDrain, ResponseOperation},
};
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
const OPTIONAL_CAPABILITIES: &[&str] = &[
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
        })
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
        error.message.to_owned()
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
            Self::Command { .. } | Self::SlashCommand { .. } => false,
            _ => true,
        }
    }
}
#[derive(Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum Response {
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
#[derive(Clone, Debug, Serialize)]
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
    input: Option<ChildStdin>,
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
        let mut command = Command::new(path);
        if policy.isolated {
            command.env_clear();
        }
        let mut child = command
            .arg("--worker")
            .env(IDENTITY_ENV, serde_json::to_string(&identity)?)
            .env_remove("DISCORD_TOKEN")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
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
            input: Some(input),
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
                let input = self.input.as_mut().ok_or_else(|| {
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
        .map_err(|_| failure("worker timed out"))
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
}
impl Generation {
    fn new(path: PathBuf, process: Process) -> Arc<Self> {
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
        })
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
    _work: OwnedRwLockReadGuard<()>,
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
            lines.push(format!(
                "{}: {} / PID: {}",
                index + 1,
                redact(&generation.build),
                generation.pid
            ));
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
        deployment: DeploymentIdentity,
    ) -> Result<Arc<Self>, AppError> {
        Self::start_configured_pool(
            path,
            count,
            deployment,
            LaunchPolicy {
                isolated: true,
                require_gateway_capabilities: true,
            },
        )
        .await
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
        }))
    }

    fn install(&self, index: usize, active: &mut Arc<Generation>, next: Arc<Generation>) {
        // No await between closing admission, registration and routing commit.
        active.responses.start_drain();
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
        let index = self.worker_index(key);
        let active = self.workers[index].read().await;
        let generation = active.clone();
        let work = generation.work.clone().read_owned().await;
        self.require_running()?;
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
                generation.responses.start_drain();
            }
            let mut work = Vec::with_capacity(generations.len());
            let mut unknown = false;
            for generation in &generations {
                work.push(generation.work.clone().write_owned().await);
                unknown |= !generation.responses.wait_settled().await.is_empty();
            }
            // Close every pipe before waiting for any child to exit.
            for generation in &generations {
                generation.process.lock().await.input.take();
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
                    if let Err(error) = manager.execute(index, Request::Tick).await {
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
        timeout(APPLICATION_DEADLINE, async {
            loop {
                self.require_running()?;
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
                let work = generation.work.clone().read_owned().await;
                self.require_running()?;
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

pub async fn run() -> Result<(), AppError> {
    let mut input = BufReader::new(tokio::io::stdin());
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
    let mut pending_results = std::collections::HashSet::new();
    let mut event_stream_initialized = false;
    let mut receiver = crate::event_seq::Receiver::new(0);
    while let Some(frame) = read_frame(&mut input).await? {
        let request = serde_json::from_slice(&frame)?;
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
                ],
                // No values are inspected, serialized or logged. Windows may
                // supply SystemRoot even to a process launched with env_clear.
                credential_free: std::env::vars_os().all(|(name, _)| {
                    name == IDENTITY_ENV
                        || (cfg!(windows) && name.eq_ignore_ascii_case("SystemRoot"))
                }),
                build: crate::cogs::build_id().to_owned(),
                pid: std::process::id(),
            },
            command @ (Request::Dispatch { .. }
            | Request::Command { .. }
            | Request::SlashCommand { .. }
            | Request::ReadyEvent { .. }
            | Request::Tick) => {
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
