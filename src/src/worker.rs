use crate::{
    AppError,
    redact::{Secret, redact},
    response_drain::{ResponseDrain, ResponseOperation},
};
use serde::{Deserialize, Serialize};
use std::{
    path::{Path, PathBuf},
    process::Stdio,
    sync::Arc,
    time::Duration,
};
use tokio::{
    io::{AsyncBufRead, AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    process::{Child, ChildStdin, ChildStdout, Command},
    sync::{Mutex, OwnedRwLockReadGuard, RwLock},
    time::timeout,
};

const PROTOCOL: u32 = 5;
const CAPABILITIES: &[&str] = &["snapshot-v3"];
const EVENT_SEQ_CAPABILITY: &str = "event-seq-v1";
const EVENT_REPLAY_CAPABILITY: &str = "event-replay-v1";
const HELLO_CAPABILITY: &str = "hello-v1";
const OPTIONAL_CAPABILITIES: &[&str] = &[
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
                        .map_err(|_| "invalid deployment number".into())
                }
                Some(_) => Err("invalid deployment number".into()),
            }
        }
        let bot_id = crate::protocol::BotId(number(lookup("BOT_ID")?, 0)?);
        let id = u32::try_from(number(lookup("SHARD_ID")?, 0)?).map_err(|_| "invalid shard id")?;
        let total = u32::try_from(number(lookup("TOTAL_SHARDS")?, 1)?)
            .map_err(|_| "invalid shard total")?;
        let generation = lookup("GENERATION_ID")?
            .map(|value| number(Some(value), 0).map(std::sync::atomic::AtomicU64::new))
            .transpose()?;
        Ok(Self {
            bot_id,
            shard: crate::protocol::Shard::new(id, total)?,
            generation,
        })
    }

    fn next_identity(&self) -> Result<crate::protocol::Identity, AppError> {
        use std::sync::atomic::Ordering;
        let generation = match &self.generation {
            Some(next) => next
                .try_update(Ordering::Relaxed, Ordering::Relaxed, |n| n.checked_add(1))
                .map_err(|_| "worker generation exhausted")?,
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
        return Err("invalid or oversized worker frame".into());
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
#[derive(Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum Response {
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

// Legacy protocol 5 workers omit the range. Partial ranges are malformed.
fn validate_hello(min: Option<u32>, max: Option<u32>, required: &[String]) -> Result<(), AppError> {
    match (min, max) {
        (None, None) => {}
        (Some(min), Some(max)) if min <= PROTOCOL && PROTOCOL <= max => {}
        _ => return Err("incompatible worker protocol range".into()),
    }
    if required
        .iter()
        .any(|capability| !CAPABILITIES.contains(&capability.as_str()))
    {
        return Err("unsupported required worker capability".into());
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

struct Process {
    _child: Child,
    input: ChildStdin,
    output: BufReader<ChildStdout>,
    broken: bool,
    events: crate::event_seq::Sender,
    checkpoint: Option<serde_json::Value>,
    build: String,
    pid: u32,
    supported_capabilities: Vec<String>,
}
impl Process {
    async fn start(path: &Path, deployment: &DeploymentIdentity) -> Result<Self, AppError> {
        let identity = deployment.next_identity()?;
        let mut hello = local_hello(identity.clone());
        let mut child = Command::new(path)
            .arg("--worker")
            .env(IDENTITY_ENV, serde_json::to_string(&identity)?)
            .env_remove("DISCORD_TOKEN")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .kill_on_drop(true)
            .spawn()
            .map_err(|_| "worker spawn failed")?;
        let input = child.stdin.take().ok_or("missing worker stdin")?;
        let output = BufReader::new(child.stdout.take().ok_or("missing worker stdout")?);
        let mut process = Self {
            _child: child,
            input,
            output,
            broken: false,
            events: crate::event_seq::Sender::new(64, 0, MAX_FRAME_BYTES as usize, 0)
                .map_err(|_| "invalid event buffer")?,
            checkpoint: None,
            build: String::new(),
            pid: 0,
            supported_capabilities: Vec::new(),
        };
        match process.request(&Request::Health).await? {
            Response::Ready {
                protocol: PROTOCOL,
                protocol_min,
                protocol_max,
                required_capabilities,
                supported_capabilities,
                build,
                pid,
            } => {
                validate_hello(protocol_min, protocol_max, &required_capabilities)?;
                // Old v5 peers may echo optional Hello fields without handling
                // new wire requests. Offer replay only after explicit Ready
                // advertisement, then still require bilateral Hello agreement.
                if !supported_capabilities
                    .iter()
                    .any(|cap| cap == EVENT_REPLAY_CAPABILITY)
                {
                    hello
                        .optional_capabilities
                        .retain(|cap| cap != EVENT_REPLAY_CAPABILITY);
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
                            let (_, negotiated) = hello.negotiate(&peer)?;
                            process.supported_capabilities = negotiated;
                        }
                        _ => return Err("worker hello rejected".into()),
                    }
                } else {
                    // Preserve v5's advertised application capabilities.
                    process.supported_capabilities = supported_capabilities
                        .into_iter()
                        .filter(|cap| cap != EVENT_SEQ_CAPABILITY)
                        .collect();
                }
                process.build = build;
                process.pid = pid;
                Ok(process)
            }
            _ => Err("incompatible worker protocol".into()),
        }
    }

    async fn request(&mut self, request: &Request) -> Result<Response, AppError> {
        if self.broken {
            return Err("worker is unavailable".into());
        }
        let sequenced;
        let request = if let Request::ReadyEvent { bot_name } = request
            && self
                .supported_capabilities
                .iter()
                .any(|cap| cap == EVENT_SEQ_CAPABILITY)
        {
            let sequence = self
                .events
                .enqueue(
                    serde_json::to_vec(request)?,
                    crate::event_seq::Priority::Required,
                )
                .map_err(|rejected| {
                    eprintln!("worker event buffer rejected: {:?}", rejected.reason);
                    "worker event buffer full or event too large"
                })?;
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
        // Poison before the first await. If the caller cancels an in-flight
        // exchange, leftover frames must never be read as a later response.
        self.broken = true;
        let result = timeout(DEADLINE, async {
            let mut data = serde_json::to_vec(request)?;
            data.push(b'\n');
            if data.len() > MAX_FRAME_BYTES as usize {
                return Err("worker request exceeds frame limit".into());
            }
            self.input.write_all(&data).await?;
            self.input.flush().await?;
            let frame = read_frame(&mut self.output).await?.ok_or("worker exited")?;
            if let Some(expected) = expected_ack {
                match serde_json::from_slice::<Response>(&frame)
                    .map_err(|_| "invalid worker response")?
                {
                    Response::EventAccepted { local_sequence } if local_sequence == expected => {
                        self.events
                            .ack(crate::event_seq::EventAccepted(
                                crate::event_seq::SequenceRange {
                                    first: expected,
                                    last: expected,
                                },
                            ))
                            .map_err(|_| "invalid worker event ACK")?;
                    }
                    _ => return Err("missing or invalid worker event ACK".into()),
                }
                let frame = read_frame(&mut self.output)
                    .await?
                    .ok_or("worker exited after ACK")?;
                return Ok(serde_json::from_slice(&frame).map_err(|_| "invalid worker response")?);
            }
            Ok(serde_json::from_slice(&frame).map_err(|_| "invalid worker response")?)
        })
        .await
        .map_err(|_| -> AppError { "worker timed out".into() })
        .and_then(|result| result);
        if result.is_ok() {
            self.broken = false;
        }
        if result.is_err() && self.events.retained_len() != 0 {
            eprintln!(
                "worker event ACK missing: unacked={:?}, retained_bytes={}",
                self.events.unacked_ranges(),
                self.events.retained_bytes()
            );
        }
        result
    }

    fn supports(&self, capability: &str) -> bool {
        self.supported_capabilities
            .iter()
            .any(|cap| cap == capability)
    }

    /// Candidate-only transaction: keep the previous buffer until installation.
    /// Both process locks/routing must be held by the caller throughout handoff.
    async fn inherit_events(&mut self, previous: &Self) -> Result<(), AppError> {
        let compatible = self.supports(EVENT_SEQ_CAPABILITY)
            && previous.supports(EVENT_SEQ_CAPABILITY)
            && self.supports(EVENT_REPLAY_CAPABILITY)
            && previous.supports(EVENT_REPLAY_CAPABILITY);
        if !compatible {
            if previous.events.retained_len() != 0 {
                return Err("unacknowledged events require negotiated replay support".into());
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
            _ => return Err("worker event stream restore rejected".into()),
        }
        self.events = previous.events.clone();
        // Clone at most the configured bounded buffer, without borrowing across
        // request() which consumes ACKs. Replay never assigns a new sequence.
        let pending: Vec<_> = self.events.replay().cloned().collect();
        for event in pending {
            let Request::ReadyEvent { bot_name } = serde_json::from_slice(&event.payload)? else {
                return Err("invalid retained worker event".into());
            };
            match self
                .request(&Request::SequencedReady {
                    local_sequence: event.local_sequence,
                    bot_name,
                })
                .await?
            {
                Response::Result { state, .. } => self.checkpoint = Some(state),
                Response::Rejected { .. } => return Err("replayed worker event rejected".into()),
                _ => return Err("unexpected replay response".into()),
            }
        }
        Ok(())
    }
}

// A cancelled replacement must never leave admission permanently closed.
struct ResumeResponseDrain(ResponseDrain);
impl Drop for ResumeResponseDrain {
    fn drop(&mut self) {
        self.0.resume();
    }
}

struct Active {
    path: PathBuf,
    process: Arc<Mutex<Process>>,
}
pub struct Manager {
    deployment: DeploymentIdentity,
    workers: Vec<RwLock<Active>>,
    update: Mutex<()>,
    replace_lock: Mutex<()>,
    response_gates: Vec<Arc<RwLock<()>>>,
    responses: Vec<ResponseDrain>,
    instance_id: String,
}

/// Holds a worker generation until its external response has completed.
pub struct ResponseGuard {
    _gate: OwnedRwLockReadGuard<()>,
    operation: ResponseOperation,
}
impl ResponseGuard {
    pub fn confirm(self) {
        self.operation.confirm();
    }
}

impl Manager {
    pub async fn describe_workers(&self) -> String {
        let mut lines = vec!["稼働中のWorker:".to_string()];
        for (index, worker) in self.workers.iter().enumerate() {
            let active = worker.read().await;
            let process = active.process.lock().await;
            lines.push(format!(
                "{}: {} / PID: {}",
                index + 1,
                redact(&process.build),
                process.pid
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
        if count == 0 {
            return Err("Worker数は1以上にしてください".into());
        }
        let mut workers = Vec::with_capacity(count);
        for _ in 0..count {
            let process = Process::start(&path, &deployment).await?;
            workers.push(RwLock::new(Active {
                path: path.clone(),
                process: Arc::new(Mutex::new(process)),
            }));
        }
        let mut instance = [0u8; 16];
        getrandom::fill(&mut instance)?;
        let instance_id = instance.iter().map(|byte| format!("{byte:02x}")).collect();
        Ok(Arc::new(Self {
            deployment,
            workers,
            instance_id,
            update: Mutex::new(()),
            replace_lock: Mutex::new(()),
            response_gates: (0..count).map(|_| Arc::new(RwLock::new(()))).collect(),
            responses: (0..count).map(|_| ResponseDrain::new(1024)).collect(),
        }))
    }
    pub async fn replace(&self, path: PathBuf, force: bool) -> Result<(), AppError> {
        // Serialize complete update attempts, but do not hold the recovery mutex while
        // waiting for a response gate. Each worker drains independently.
        let _replacement = self.replace_lock.lock().await;
        for (index, worker) in self.workers.iter().enumerate() {
            if !force && worker.read().await.path == path {
                continue;
            }
            let _gate = self.response_gates[index].write().await;
            let tracker = &self.responses[index];
            tracker.start_drain();
            let resume = ResumeResponseDrain(tracker.clone());
            let unknown = tracker.wait_settled().await;
            drop(resume);
            if !unknown.is_empty() {
                return Err("unresolved response outcomes prevent worker update".into());
            }
            let _guard = self.update.lock().await;
            if !force && worker.read().await.path == path {
                continue;
            }
            let mut process = Process::start(&path, &self.deployment).await?;
            // Exclusive lock drains accepted requests and queues new ones during migration.
            let mut active = worker.write().await;
            let mut previous = active.process.lock().await;
            let snapshot = if previous.broken {
                // Same recovery boundary as automatic restart: only confirmed
                // snapshots survive. Unconfirmed application effects are unknown.
                Response::Snapshot {
                    version: STATE_VERSION,
                    state: previous
                        .checkpoint
                        .clone()
                        .unwrap_or(serde_json::to_value(crate::cogs::State::default())?),
                }
            } else {
                previous.request(&Request::Snapshot).await?
            };
            let (version, state) = match snapshot {
                Response::Snapshot { version, state } => (version, state),
                _ => return Err("旧Workerから状態を取得できません".into()),
            };
            match process
                .request(&Request::Restore {
                    version,
                    state: state.clone(),
                })
                .await?
            {
                Response::Restored => {}
                Response::Rejected { reason } => {
                    return Err(
                        format!("状態の引き継ぎを拒否しました: {}", Secret::new(reason)).into(),
                    );
                }
                _ => return Err("新Workerが状態の引き継ぎを確認できません".into()),
            }
            process.checkpoint = Some(state);
            process.inherit_events(&previous).await?;
            drop(previous);
            *active = Active {
                path: path.clone(),
                process: Arc::new(Mutex::new(process)),
            };
            println!(
                "Worker {}/{} を切り替えました",
                index + 1,
                self.workers.len()
            );
        }
        Ok(())
    }
    /// Snapshot unresolved operations without payloads, credentials or message content.
    /// This API is intended for a future authenticated local management channel.
    pub fn unknown_responses(&self) -> (String, Vec<(usize, u64)>) {
        let ids = self
            .responses
            .iter()
            .enumerate()
            .flat_map(|(worker, tracker)| {
                tracker
                    .unknown_ids()
                    .into_iter()
                    .map(move |id| (worker, id))
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
            return Err("response instance mismatch".into());
        }
        if evidence.trim().is_empty() {
            return Err("reconciliation evidence is required".into());
        }
        let tracker = self.responses.get(worker).ok_or("invalid worker index")?;
        if !tracker.resolve_unknown(id) {
            return Err("unknown response does not exist".into());
        }
        Ok(())
    }

    /// Acquire before dispatch, retain through the external response, and confirm
    /// only after a definite outcome. Dropping leaves an unresolved result.
    pub async fn begin_response(&self, key: u64) -> Result<ResponseGuard, AppError> {
        let index = ((key ^ (key >> 22)) % self.workers.len() as u64) as usize;
        let gate = self.response_gates[index].clone().read_owned().await;
        let operation = self.responses[index]
            .begin()
            .map_err(|error| format!("response tracking unavailable: {error:?}"))?;
        Ok(ResponseGuard {
            _gate: gate,
            operation,
        })
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
                    interval.tick().await;
                    if let Err(error) = manager.execute(index, Request::Tick).await {
                        eprintln!(
                            "Worker {} の仕事を進められません: {}",
                            index + 1,
                            Secret::new(error)
                        );
                    }
                }
            });
        }
    }
    async fn execute(&self, index: usize, request: Request) -> Result<Option<String>, AppError> {
        let worker = &self.workers[index];
        let active = worker.read().await;
        let process = active.process.clone();
        let mut guard = process.lock().await;
        if matches!(request, Request::Command { .. })
            && !guard
                .supported_capabilities
                .iter()
                .any(|cap| cap == "command-dispatch-v1")
        {
            return Err("worker does not support command dispatch".into());
        }
        if matches!(request, Request::SlashCommand { .. })
            && !guard
                .supported_capabilities
                .iter()
                .any(|cap| cap == "slash-arguments-v1")
        {
            return Err("worker does not support slash arguments".into());
        }
        if matches!(request, Request::ReadyEvent { .. })
            && !guard
                .supported_capabilities
                .iter()
                .any(|cap| cap == "ready-event-v1")
        {
            return Err("worker does not support ready events".into());
        }
        let result = guard.request(&request).await;
        if let Ok(Response::Result { state, .. }) = &result {
            guard.checkpoint = Some(state.clone());
        }
        drop(guard);
        drop(active);
        let result = match result {
            Ok(Response::Rejected { reason }) => {
                return Err(format!("worker rejected request: {}", Secret::new(reason)).into());
            }
            Ok(Response::Result { content, .. }) => Ok(content),
            Ok(_) => Err("unexpected worker response".into()),
            Err(error) => Err(error),
        };
        if result.is_err() {
            let _guard = self.update.lock().await;
            let mut active = worker.write().await;
            if Arc::ptr_eq(&active.process, &process) {
                let path = active.path.clone();
                let previous = active.process.lock().await;
                match Process::start(&path, &self.deployment).await {
                    Ok(mut new_process) => {
                        if let Some(state) = previous.checkpoint.clone() {
                            match new_process
                                .request(&Request::Restore {
                                    version: STATE_VERSION,
                                    state: state.clone(),
                                })
                                .await
                            {
                                Ok(Response::Restored) => new_process.checkpoint = Some(state),
                                _ => {
                                    eprintln!("Workerの状態を復旧できません");
                                    return result;
                                }
                            }
                        }
                        if let Err(error) = new_process.inherit_events(&previous).await {
                            eprintln!("Workerのイベントを復旧できません: {}", Secret::new(error));
                            return result;
                        }
                        drop(previous);
                        *active = Active {
                            path,
                            process: Arc::new(Mutex::new(new_process)),
                        };
                        eprintln!("Workerを再起動しました");
                    }
                    Err(error) => eprintln!("Workerの再起動に失敗しました: {}", Secret::new(error)),
                }
            }
        }
        result
    }
    pub async fn watch(self: Arc<Self>, file: PathBuf) {
        loop {
            tokio::time::sleep(Duration::from_secs(2)).await;
            match tokio::fs::read_to_string(&file).await {
                Ok(value) if !value.trim().is_empty() => {
                    let path = PathBuf::from(value.trim());
                    if !path.is_absolute() {
                        eprintln!("Workerのパスは絶対パスにしてください");
                        continue;
                    }
                    match self.replace(path.clone(), false).await {
                        Ok(()) => {
                            if let Err(error) = tokio::fs::write(
                                status_file(&file),
                                path.to_string_lossy().as_bytes(),
                            )
                            .await
                            {
                                eprintln!("Workerの状態を記録できません: {}", Secret::new(error));
                            }
                            let _ = tokio::fs::remove_file(error_file(&file)).await;
                        }
                        Err(error) => {
                            let safe_error =
                                redact(&format!("worker update failed: {}", Secret::new(error)));
                            eprintln!("Workerの更新に失敗しました: {safe_error}");
                            // path is an exact-match control field used by reload,
                            // never a diagnostic; preserve its wire compatibility.
                            let detail = serde_json::json!({"path": path.to_string_lossy(), "error": safe_error});
                            if let Err(write_error) =
                                tokio::fs::write(error_file(&file), detail.to_string()).await
                            {
                                eprintln!(
                                    "更新エラーを記録できません: {}",
                                    Secret::new(write_error)
                                );
                            }
                        }
                    }
                }
                Ok(_) => eprintln!("Worker指定ファイルが空です"),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => eprintln!("Worker指定ファイルを読めません: {}", Secret::new(error)),
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
    let mut event_stream_initialized = false;
    let mut receiver = crate::event_seq::Receiver::new(0);
    while let Some(frame) = read_frame(&mut input).await? {
        let request = match serde_json::from_slice(&frame)? {
            Request::SequencedReady {
                local_sequence,
                bot_name,
            } => {
                if !sequenced_events {
                    return Err("event sequencing was not negotiated".into());
                }
                // Serial execution slot: admit at most one event before processing.
                receiver
                    .accept(local_sequence, true)
                    .map_err(|_| "worker event gap or duplicate")?;
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
                    EVENT_SEQ_CAPABILITY.into(),
                    EVENT_REPLAY_CAPABILITY.into(),
                    HELLO_CAPABILITY.into(),
                    "command-dispatch-v1".into(),
                    "ready-event-v1".into(),
                    "slash-arguments-v1".into(),
                ],
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
        let mut data = serde_json::to_vec(&response)?;
        data.push(b'\n');
        output.write_all(&data).await?;
        output.flush().await?;
    }
    tasks.begin_drain();
    tasks.wait_drained().await?;
    output.flush().await?;
    Ok(())
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
        let result = command_result(&mut state, previous, Err("secret error".into())).unwrap();
        assert_eq!(state.counts.get(&1), Some(&4));
        assert!(
            matches!(result, Response::Rejected { reason } if reason == "command handler failed")
        );
    }

    #[test]
    fn cancelled_response_drain_reopens_admission() {
        let tracker = ResponseDrain::new(1);
        tracker.start_drain();
        {
            let _resume = ResumeResponseDrain(tracker.clone());
            assert!(tracker.begin().is_err());
        }
        tracker.begin().unwrap().confirm();
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
