use crate::{
    AppError,
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
    checkpoint: Option<serde_json::Value>,
    build: String,
    pid: u32,
    supported_capabilities: Vec<String>,
}
impl Process {
    async fn start(path: &Path) -> Result<Self, AppError> {
        let mut child = Command::new(path)
            .arg("--worker")
            .env_remove("DISCORD_TOKEN")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .kill_on_drop(true)
            .spawn()?;
        let input = child.stdin.take().ok_or("missing worker stdin")?;
        let output = BufReader::new(child.stdout.take().ok_or("missing worker stdout")?);
        let mut process = Self {
            _child: child,
            input,
            output,
            broken: false,
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
                process.build = build;
                process.pid = pid;
                process.supported_capabilities = supported_capabilities;
                Ok(process)
            }
            _ => Err("incompatible worker protocol".into()),
        }
    }

    async fn request(&mut self, request: &Request) -> Result<Response, AppError> {
        if self.broken {
            return Err("worker is unavailable".into());
        }
        let result = timeout(DEADLINE, async {
            let mut data = serde_json::to_vec(request)?;
            data.push(b'\n');
            self.input.write_all(&data).await?;
            self.input.flush().await?;
            let frame = read_frame(&mut self.output).await?.ok_or("worker exited")?;
            Ok(serde_json::from_slice(&frame)?)
        })
        .await
        .map_err(|_| -> AppError { "worker timed out".into() })
        .and_then(|result| result);
        if result.is_err() {
            self.broken = true;
        }
        result
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
                process.build,
                process.pid
            ));
        }
        lines.join("\n")
    }
    pub async fn start(path: PathBuf) -> Result<Arc<Self>, AppError> {
        Self::start_pool(path, 2).await
    }
    pub async fn start_pool(path: PathBuf, count: usize) -> Result<Arc<Self>, AppError> {
        if count == 0 {
            return Err("Worker数は1以上にしてください".into());
        }
        let mut workers = Vec::with_capacity(count);
        for _ in 0..count {
            let process = Process::start(&path).await?;
            workers.push(RwLock::new(Active {
                path: path.clone(),
                process: Arc::new(Mutex::new(process)),
            }));
        }
        let mut instance = [0u8; 16];
        getrandom::fill(&mut instance)?;
        let instance_id = instance.iter().map(|byte| format!("{byte:02x}")).collect();
        Ok(Arc::new(Self {
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
            let mut process = Process::start(&path).await?;
            // Exclusive lock drains accepted requests and queues new ones during migration.
            let mut active = worker.write().await;
            let snapshot = active
                .process
                .lock()
                .await
                .request(&Request::Snapshot)
                .await?;
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
                    return Err(format!("状態の引き継ぎを拒否しました: {reason}").into());
                }
                _ => return Err("新Workerが状態の引き継ぎを確認できません".into()),
            }
            process.checkpoint = Some(state);
            *active = Active {
                path: path.clone(),
                process: Arc::new(Mutex::new(process)),
            };
            println!(
                "Worker {}/{} を切り替えました: {}",
                index + 1,
                self.workers.len(),
                path.display()
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
                        eprintln!("Worker {} の仕事を進められません: {error}", index + 1);
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
        let checkpoint = guard.checkpoint.clone();
        drop(guard);
        drop(active);
        let result = match result {
            Ok(Response::Rejected { reason }) => return Err(reason.into()),
            Ok(Response::Result { content, .. }) => Ok(content),
            Ok(_) => Err("unexpected worker response".into()),
            Err(error) => Err(error),
        };
        if result.is_err() {
            let _guard = self.update.lock().await;
            let active = worker.read().await;
            if Arc::ptr_eq(&active.process, &process) {
                let path = active.path.clone();
                drop(active);
                match Process::start(&path).await {
                    Ok(mut new_process) => {
                        if let Some(state) = checkpoint {
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
                        *worker.write().await = Active {
                            path,
                            process: Arc::new(Mutex::new(new_process)),
                        };
                        eprintln!("Workerを再起動しました");
                    }
                    Err(error) => eprintln!("Workerの再起動に失敗しました: {error}"),
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
                                eprintln!("Workerの状態を記録できません: {error}");
                            }
                            let _ = tokio::fs::remove_file(error_file(&file)).await;
                        }
                        Err(error) => {
                            eprintln!("Workerの更新に失敗しました: {error}");
                            let detail = serde_json::json!({"path": path.to_string_lossy(), "error": error.to_string()});
                            if let Err(write_error) =
                                tokio::fs::write(error_file(&file), detail.to_string()).await
                            {
                                eprintln!("更新エラーを記録できません: {write_error}");
                            }
                        }
                    }
                }
                Ok(_) => eprintln!("Worker指定ファイルが空です"),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => eprintln!("Worker指定ファイルを読めません: {error}"),
            }
        }
    }
}

pub async fn run() -> Result<(), AppError> {
    let mut input = BufReader::new(tokio::io::stdin());
    let mut output = tokio::io::stdout();
    let mut state = crate::cogs::State::default();
    let runtime = crate::cogs::FrameworkRuntime::default();
    bot_framework::commands()?;
    while let Some(frame) = read_frame(&mut input).await? {
        let response = match serde_json::from_slice(&frame)? {
            Request::Health => Response::Ready {
                protocol: PROTOCOL,
                protocol_min: Some(PROTOCOL),
                protocol_max: Some(PROTOCOL),
                required_capabilities: CAPABILITIES.iter().map(|s| (*s).to_owned()).collect(),
                supported_capabilities: vec![
                    "command-dispatch-v1".into(),
                    "ready-event-v1".into(),
                    "slash-arguments-v1".into(),
                ],
                build: crate::cogs::build_id().to_owned(),
                pid: std::process::id(),
            },
            Request::Dispatch { key, content } => {
                let previous = state.clone();
                let result = crate::cogs::dispatch(key, &content, &mut state, &runtime).await;
                command_result(&mut state, previous, result)?
            }
            Request::Command { name } => {
                let previous = state.clone();
                let result = crate::cogs::dispatch_command(&name, &runtime).await;
                command_result(&mut state, previous, result)?
            }
            Request::SlashCommand { name, args } => {
                let previous = state.clone();
                let result = crate::cogs::dispatch_slash(&name, args, &runtime).await;
                command_result(&mut state, previous, result)?
            }
            Request::ReadyEvent { bot_name } => {
                match crate::cogs::dispatch_ready(&runtime, bot_name).await {
                    Ok(()) => {
                        let previous = state.clone();
                        checked_result(&mut state, previous, None)?
                    }
                    Err(_) => Response::Rejected {
                        reason: "ready handler failed".into(),
                    },
                }
            }
            Request::Tick => {
                let previous = state.clone();
                crate::cogs::tick(&mut state);
                checked_result(&mut state, previous, None)?
            }
            Request::Snapshot => Response::Snapshot {
                version: STATE_VERSION,
                state: serde_json::to_value(&state)?,
            },
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
                        Err(error) => Response::Rejected {
                            reason: error.to_string(),
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
    Ok(())
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
