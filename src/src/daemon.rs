//! Local supervision of one Gateway execution unit per configured Bot/Shard.
//!
//! This launcher starts `ShardRuntimeSpec::gateway`, which owns its Worker pool.
//! Gateway Ready/lease/Identify admission are separate integration work.
//! Running means an OS child exists, not Discord readiness. Cooperative shutdown
//! uses the owned child's anonymous stdin; the artifact must implement it.
//! No distributed placement, failover or ownership grant is implemented here.
//!
//! Drive `poll` regularly. Dropping this owner never kills children. A persistent
//! create-new reservation blocks accidental replacement while an orphan may
//! survive. On Linux a leftover reservation is removed at launch only when the
//! /proc command lines show no live Gateway for that shard (executable and
//! `--management-path` match); otherwise, or when that cannot be judged (any
//! other OS), it still blocks. There is deliberately no PID-based adoption of a
//! living orphan: re-adoption is unimplemented.
//! The root and artifacts are trusted operator inputs. Filesystem aliases/ACLs,
//! power-loss durability and service-manager teardown are outside this skeleton.
//! Reservations cover one configured runtime namespace, not other roots/hosts.

use crate::{
    child_recovery::{
        ChildKey, ChildRecovery, ChildRole, DegradedReason, IdentifyAdmission, RecoveryConfig,
        RecoveryEvent, RecoveryEventKind, RecoveryHost, RecoveryState, StartRequest,
    },
    placement::{ClusterMode, Config},
    shard_runtime::{self, Role, ShardRuntimeSpec},
    supervisor::{Clock, MonotonicClock, RestartPolicy},
};
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, HashMap, HashSet, VecDeque},
    ffi::OsString,
    fmt,
    io::Write,
    net::SocketAddr,
    path::{Component, Path, PathBuf},
    process::Stdio,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::{
    io::AsyncWriteExt,
    process::{Child, Command},
};

pub mod cluster_host;

/// Size above which `gateway.log` is rotated to `gateway.log.1` at launch.
const GATEWAY_LOG_MAX_BYTES: u64 = 1024 * 1024;

/// Open (append, 0600) `<dir>/gateway.log` for a Gateway child's output.
/// Rotates once to `gateway.log.1` when over `max_bytes` (one generation kept).
/// Returns None, so the caller falls back to null, when the path is a symlink
/// or not a regular file, or on any I/O error. The check-then-open is not
/// atomic; the directory is daemon-owned state, not an attacker-writable one.
fn open_gateway_log(dir: &Path, max_bytes: u64, generation: u64) -> Option<std::fs::File> {
    let path = dir.join("gateway.log");
    match std::fs::symlink_metadata(&path) {
        Ok(meta) => {
            if !meta.file_type().is_file() {
                return None;
            }
            if meta.len() > max_bytes {
                let old = dir.join("gateway.log.1");
                if let Ok(m) = std::fs::symlink_metadata(&old)
                    && !m.file_type().is_file()
                {
                    return None;
                }
                std::fs::rename(&path, &old).ok()?;
            }
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
        Err(_) => return None,
    }
    let mut options = std::fs::OpenOptions::new();
    options.create(true).append(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options.open(&path).ok()?;
    if !file.metadata().ok()?.file_type().is_file() {
        return None;
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        file.set_permissions(std::fs::Permissions::from_mode(0o600))
            .ok()?;
    }
    writeln!(
        file,
        "--- gateway generation {generation} --- ({})",
        crate::log_time::utc_stamp()
    )
    .ok()?;
    Some(file)
}
/// Authenticated update admission and off-loop audit/artifact work.
pub mod update_control;

/// Process harness for the explicitly enabled loopback quorum experiment.
/// It never reads Bot credentials and is not a production lease watchdog.
pub mod test_cluster_processes {
    use super::{ShardKey, ShardRuntimeSpec};
    use crate::cluster_runtime::ClusterAction;
    use serde::{Deserialize, Serialize};
    use serde_json::{Value, json};
    use std::{
        collections::{BTreeMap, VecDeque},
        io::{BufRead, Read},
        net::SocketAddr,
        path::{Path, PathBuf},
        process::Stdio,
        time::{Duration, Instant, SystemTime, UNIX_EPOCH},
    };
    use tokio::{
        io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
        net::{TcpListener, TcpStream},
        process::{Child, Command},
        sync::mpsc,
        task::JoinHandle,
    };

    const PIPE_LIMIT: u64 = 64 * 1024;
    const WRITE_WAIT: Duration = Duration::from_millis(100);
    const HISTORY_LIMIT: usize = 256;

    // Deliberately no Debug: trusted paths and arbitrary child frames are not logs.
    #[derive(Deserialize, Serialize)]
    #[serde(deny_unknown_fields)]
    struct Launch {
        program: PathBuf,
        args: Vec<String>,
        key: ShardKey,
        total_shards: u32,
        node_id: u64,
        epoch: u64,
        collector: SocketAddr,
        deadline_ms: u64,
        max_lease_ms: u64,
        stop_grace_ms: u64,
    }

    #[derive(Deserialize, Serialize)]
    #[serde(tag = "command", rename_all = "snake_case", deny_unknown_fields)]
    enum Input {
        Launch(Launch),
        Renew { epoch: u64, deadline_ms: u64 },
        Stop {},
    }

    #[derive(Deserialize, Serialize)]
    #[serde(deny_unknown_fields)]
    struct Notice {
        state: String,
        gateway_pid: Option<u32>,
        reason: String,
    }

    struct Process {
        epoch: u64,
        state: &'static str,
        child: Child,
        gateway_pid: Option<u32>,
        reader: JoinHandle<()>,
    }
    impl Drop for Process {
        fn drop(&mut self) {
            self.reader.abort();
            // The guardian keeps its last bounded lease after stdin EOF.
        }
    }

    pub enum ProcessEvent {
        Ready(ShardKey),
        Exited(ShardKey),
    }

    /// One independent guardian per real Gateway, with a bounded anonymous pipe.
    /// The guardian survives botd death, then drains at its last lease deadline.
    pub struct TestClusterProcesses {
        specs: BTreeMap<ShardKey, ShardRuntimeSpec>,
        processes: BTreeMap<ShardKey, Process>,
        program: PathBuf,
        collector: SocketAddr,
        node_id: u64,
        max_lease_ms: u64,
        stop_grace_ms: u64,
        sender: mpsc::Sender<(ShardKey, u64, Notice)>,
        notices: mpsc::Receiver<(ShardKey, u64, Notice)>,
        history: VecDeque<Value>,
    }

    fn wall_ms() -> Result<u64, &'static str> {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| "test watchdog clock invalid")?
            .as_millis()
            .try_into()
            .map_err(|_| "test watchdog clock overflow")
    }

    fn wire_deadline(expires_at: Instant) -> Result<u64, &'static str> {
        let wall = wall_ms()?;
        let remaining = expires_at
            .checked_duration_since(Instant::now())
            .ok_or("test lease expired before process action")?;
        let millis: u64 = remaining
            .as_millis()
            .try_into()
            .map_err(|_| "test lease duration overflow")?;
        wall.checked_add(millis)
            .ok_or("test lease deadline overflow")
    }

    async fn write_input(child: &mut Child, input: &Input) -> Result<(), &'static str> {
        let pipe = child.stdin.as_mut().ok_or("test guardian input closed")?;
        let mut frame = serde_json::to_vec(input).map_err(|_| "test guardian input invalid")?;
        frame.push(b'\n');
        if frame.len() as u64 > PIPE_LIMIT {
            return Err("test guardian input too large");
        }
        tokio::time::timeout(WRITE_WAIT, async {
            pipe.write_all(&frame).await?;
            pipe.flush().await
        })
        .await
        .map_err(|_| "test guardian input timeout")?
        .map_err(|_| "test guardian input failed")
    }

    impl TestClusterProcesses {
        pub fn new(
            specs: Vec<ShardRuntimeSpec>,
            program: &Path,
            collector: SocketAddr,
            node_id: u64,
            max_lease_ms: u64,
            stop_margin_ms: u64,
        ) -> Result<Self, &'static str> {
            if !program.is_absolute()
                || !collector.ip().is_loopback()
                || collector.port() == 0
                || max_lease_ms == 0
                || stop_margin_ms < 100
            {
                return Err("invalid test cluster process configuration");
            }
            let (sender, notices) = mpsc::channel(128);
            Ok(Self {
                specs: specs
                    .into_iter()
                    .map(|s| {
                        (
                            ShardKey {
                                bot_id: s.bot_id.clone(),
                                shard_id: s.shard_id,
                            },
                            s,
                        )
                    })
                    .collect(),
                processes: BTreeMap::new(),
                program: program.to_owned(),
                collector,
                node_id,
                max_lease_ms,
                stop_grace_ms: (stop_margin_ms / 3).min(250),
                sender,
                notices,
                history: VecDeque::new(),
            })
        }

        fn record(&mut self, key: &ShardKey, epoch: u64, state: &str, reason: &str) {
            if self.history.len() == HISTORY_LIMIT {
                self.history.pop_front();
            }
            self.history.push_back(json!({"key":key,"bot_id":key.bot_id,
                "shard_id":key.shard_id,"epoch":epoch,"state":state,"reason":reason}));
        }

        pub fn status(&self) -> Value {
            let processes: Vec<_> = self
                .processes
                .iter()
                .map(|(key, p)| {
                    json!({
                        "key":key,"bot_id":key.bot_id,"shard_id":key.shard_id,
                        "state":p.state,"epoch":p.epoch,"guardian_pid":p.child.id(),
                        "gateway_pid":p.gateway_pid,
                    })
                })
                .collect();
            json!({"processes":processes,"transitions":self.history})
        }

        pub fn has_process(&self, key: &ShardKey) -> bool {
            self.processes.contains_key(key)
        }

        pub async fn apply(&mut self, action: ClusterAction) -> Result<(), &'static str> {
            match action {
                ClusterAction::Start {
                    key,
                    epoch,
                    expires_at,
                } => {
                    if self.processes.contains_key(&key) {
                        return Err("test guardian still stopping");
                    }
                    let spec = self
                        .specs
                        .get(&key)
                        .ok_or("test shard is not assigned here")?;
                    let launch = Launch {
                        program: self.program.clone(),
                        args: spec
                            .gateway
                            .args
                            .iter()
                            .map(|a| {
                                a.to_str()
                                    .map(str::to_owned)
                                    .ok_or("test launch arguments must be UTF-8")
                            })
                            .collect::<Result<_, _>>()?,
                        key: key.clone(),
                        total_shards: spec.total_shards,
                        node_id: self.node_id,
                        epoch,
                        collector: self.collector,
                        deadline_ms: wire_deadline(expires_at)?,
                        max_lease_ms: self.max_lease_ms,
                        stop_grace_ms: self.stop_grace_ms,
                    };
                    let executable =
                        std::env::current_exe().map_err(|_| "test guardian program unavailable")?;
                    let mut command = Command::new(executable);
                    command
                        .args(["--test-cluster-guardian", "--allow-test-quorum"])
                        .env_clear()
                        .stdin(Stdio::piped())
                        .stdout(Stdio::piped())
                        .stderr(Stdio::null())
                        .kill_on_drop(false);
                    #[cfg(windows)]
                    {
                        command.env(
                            "SystemRoot",
                            std::env::var_os("SystemRoot")
                                .ok_or("test guardian system environment unavailable")?,
                        );
                        command.creation_flags(0x0000_0200 | 0x0800_0000);
                    }
                    #[cfg(unix)]
                    command.process_group(0);
                    let mut child = command.spawn().map_err(|_| "test guardian spawn failed")?;
                    let stdout = child
                        .stdout
                        .take()
                        .ok_or("test guardian output unavailable")?;
                    let sender = self.sender.clone();
                    let reader_key = key.clone();
                    let reader = tokio::spawn(async move {
                        let mut reader = BufReader::new(stdout);
                        loop {
                            let mut frame = Vec::new();
                            let read = (&mut reader).take(4097).read_until(b'\n', &mut frame).await;
                            if !matches!(read, Ok(1..))
                                || frame.len() > 4096
                                || frame.last() != Some(&b'\n')
                            {
                                break;
                            }
                            let Ok(notice) = serde_json::from_slice::<Notice>(&frame) else {
                                break;
                            };
                            if sender
                                .send((reader_key.clone(), epoch, notice))
                                .await
                                .is_err()
                            {
                                break;
                            }
                        }
                    });
                    self.processes.insert(
                        key.clone(),
                        Process {
                            epoch,
                            state: "starting",
                            child,
                            gateway_pid: None,
                            reader,
                        },
                    );
                    self.record(&key, epoch, "starting", "quorum_lease");
                    // Even a failed write may have delivered the complete launch.
                    // Retain the guardian handle until reaped; never kill it and
                    // leave a newly spawned Gateway without its lease watchdog.
                    write_input(
                        &mut self
                            .processes
                            .get_mut(&key)
                            .expect("inserted guardian")
                            .child,
                        &Input::Launch(launch),
                    )
                    .await
                }
                ClusterAction::Renew {
                    key,
                    epoch,
                    expires_at,
                } => {
                    let process = self
                        .processes
                        .get_mut(&key)
                        .ok_or("test guardian unavailable")?;
                    if process.epoch != epoch || process.state == "draining" {
                        return Err("test guardian renewal fenced");
                    }
                    write_input(
                        &mut process.child,
                        &Input::Renew {
                            epoch,
                            deadline_ms: wire_deadline(expires_at)?,
                        },
                    )
                    .await
                }
                ClusterAction::Stop { key, reason } => {
                    if let Some(process) = self.processes.get_mut(&key) {
                        let epoch = process.epoch;
                        process.state = "draining";
                        let result = write_input(&mut process.child, &Input::Stop {}).await;
                        self.record(&key, epoch, "draining", reason);
                        result
                    } else {
                        Ok(())
                    }
                }
            }
        }

        /// Reap owned handles, never identify/kill children using a persisted PID.
        pub fn poll(&mut self) -> Vec<ProcessEvent> {
            let mut events = Vec::new();
            while let Ok((key, epoch, notice)) = self.notices.try_recv() {
                if let Some(process) = self.processes.get_mut(&key) {
                    if process.epoch != epoch {
                        continue;
                    }
                    if notice.state == "ready" && process.state == "starting" {
                        process.gateway_pid = notice.gateway_pid;
                        process.state = "active";
                        events.push(ProcessEvent::Ready(key.clone()));
                        self.record(&key, epoch, "active", "gateway_core_ready");
                    } else if notice.state == "draining" {
                        process.state = "draining";
                        self.record(&key, epoch, "draining", "guardian_stop");
                    }
                }
            }
            let ended: Vec<_> = self
                .processes
                .iter_mut()
                .filter_map(|(key, p)| {
                    matches!(p.child.try_wait(), Ok(Some(_))).then(|| (key.clone(), p.epoch))
                })
                .collect();
            for (key, epoch) in ended {
                self.processes.remove(&key);
                self.record(&key, epoch, "stopped", "guardian_reaped");
                events.retain(|e| !matches!(e,ProcessEvent::Ready(k) if k == &key));
                events.push(ProcessEvent::Exited(key));
            }
            events
        }
    }

    // Tokio's stdin adapter uses a blocking worker that can delay runtime exit.
    // A detached, bounded reader thread lets a drained guardian exit even while
    // the still-running parent retains the anonymous input pipe.
    fn guardian_inputs() -> mpsc::Receiver<Input> {
        let (sender, receiver) = mpsc::channel(8);
        std::thread::spawn(move || {
            let mut input = std::io::stdin().lock();
            loop {
                let mut frame = Vec::new();
                if !matches!(
                    (&mut input)
                        .take(PIPE_LIMIT + 1)
                        .read_until(b'\n', &mut frame),
                    Ok(1..)
                ) || frame.len() as u64 > PIPE_LIMIT
                    || frame.last() != Some(&b'\n')
                {
                    break;
                }
                let Ok(message) = serde_json::from_slice(&frame) else {
                    break;
                };
                if sender.blocking_send(message).is_err() {
                    break;
                }
            }
        });
        receiver
    }

    fn local_deadline(
        deadline_ms: u64,
        max_lease_ms: u64,
        last_wall: &mut u64,
    ) -> Result<Instant, &'static str> {
        let monotonic = Instant::now();
        let now = wall_ms()?;
        if now < *last_wall {
            return Err("test watchdog clock regressed");
        }
        *last_wall = now;
        let remaining = deadline_ms
            .checked_sub(now)
            .filter(|v| *v > 0 && *v <= max_lease_ms)
            .ok_or("test watchdog deadline invalid")?;
        monotonic
            .checked_add(Duration::from_millis(remaining))
            .ok_or("test watchdog deadline overflow")
    }

    async fn notice(state: &str, pid: Option<u32>, reason: &str) {
        let Ok(mut bytes) = serde_json::to_vec(&Notice {
            state: state.into(),
            gateway_pid: pid,
            reason: reason.into(),
        }) else {
            return;
        };
        bytes.push(b'\n');
        let _ = tokio::time::timeout(Duration::from_millis(10), async {
            let mut output = tokio::io::stdout();
            output.write_all(&bytes).await?;
            output.flush().await
        })
        .await;
    }

    async fn relay(
        listener: TcpListener,
        collector: SocketAddr,
        node_id: u64,
        epoch: u64,
        key: ShardKey,
        pid: u32,
        ready: mpsc::Sender<()>,
    ) -> Result<(), &'static str> {
        let (gateway, peer) = listener
            .accept()
            .await
            .map_err(|_| "test relay accept failed")?;
        if !peer.ip().is_loopback() {
            return Err("test relay peer rejected");
        }
        let external = TcpStream::connect(collector)
            .await
            .map_err(|_| "test relay connect failed")?;
        let (read_gateway, mut write_gateway) = gateway.into_split();
        let (mut read_external, mut write_external) = external.into_split();
        let incoming = async {
            tokio::io::copy(&mut read_external, &mut write_gateway)
                .await
                .map_err(|_| "test relay input failed")?;
            Ok::<(), &'static str>(())
        };
        let outgoing = async {
            let mut reader = BufReader::new(read_gateway);
            let mut observed_ready = false;
            loop {
                let mut frame = Vec::new();
                let read = (&mut reader)
                    .take(PIPE_LIMIT + 1)
                    .read_until(b'\n', &mut frame)
                    .await
                    .map_err(|_| "test relay output failed")?;
                if read == 0 {
                    return Ok(());
                }
                if frame.len() as u64 > PIPE_LIMIT || frame.last() != Some(&b'\n') {
                    return Err("test relay frame rejected");
                }
                let mut value: Value =
                    serde_json::from_slice(&frame).map_err(|_| "test relay frame rejected")?;
                if value["type"] == "ready" {
                    if observed_ready
                        || value["pid"].as_u64() != Some(u64::from(pid))
                        || value["bot_id"].as_u64() != key.bot_id.parse::<u64>().ok()
                        || value["shard_id"].as_u64() != Some(u64::from(key.shard_id))
                    {
                        return Err("test relay identity rejected");
                    }
                    observed_ready = true;
                    value["test_cluster_node_id"] = json!(node_id);
                    value["ownership_epoch"] = json!(epoch);
                    value["guardian_pid"] = json!(std::process::id());
                    frame = serde_json::to_vec(&value).map_err(|_| "test relay encoding failed")?;
                    frame.push(b'\n');
                    ready.send(()).await.map_err(|_| "test guardian stopped")?;
                }
                write_external
                    .write_all(&frame)
                    .await
                    .map_err(|_| "test relay output failed")?;
            }
        };
        tokio::select! { result=incoming=>result, result=outgoing=>result }
    }

    /// Internal botd mode, requiring the explicit test flag at the binary entry.
    /// Only the existing token-free mock Gateway mode is launched. Local wall
    /// time converts pipe deadlines to a monotonic timer; expiry is never renewed
    /// by EOF, malformed messages, stale epochs, or a late renewal. Clock skew,
    /// OS suspend and an unresponsive kernel are outside this loopback experiment.
    pub async fn run_guardian() -> Result<(), &'static str> {
        let mut inputs = guardian_inputs();
        let Some(Input::Launch(launch)) =
            tokio::time::timeout(Duration::from_secs(3), inputs.recv())
                .await
                .map_err(|_| "test guardian bootstrap timeout")?
        else {
            return Err("test guardian bootstrap invalid");
        };
        if !launch.program.is_absolute()
            || !launch.collector.ip().is_loopback()
            || launch.collector.port() == 0
            || launch.epoch == 0
            || launch.max_lease_ms > 300_000
            || launch.stop_grace_ms == 0
            || launch.stop_grace_ms > 250
            || launch.key.shard_id >= launch.total_shards
        {
            return Err("test guardian configuration invalid");
        }
        let mut last_wall = 0;
        let mut expires = local_deadline(launch.deadline_ms, launch.max_lease_ms, &mut last_wall)?;
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .map_err(|_| "test relay bind failed")?;
        let address = listener
            .local_addr()
            .map_err(|_| "test relay address unavailable")?;
        let mut command = Command::new(&launch.program);
        command
            .args(&launch.args)
            .arg("--mock-discord")
            .arg(address.to_string())
            .env_clear()
            .env("BOT_ID", &launch.key.bot_id)
            .env("SHARD_ID", launch.key.shard_id.to_string())
            .env("TOTAL_SHARDS", launch.total_shards.to_string())
            .env("GENERATION_ID", launch.epoch.to_string())
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .kill_on_drop(true);
        #[cfg(windows)]
        {
            command.env(
                "SystemRoot",
                std::env::var_os("SystemRoot")
                    .ok_or("test gateway system environment unavailable")?,
            );
            command.creation_flags(0x0000_0200 | 0x0800_0000);
        }
        #[cfg(unix)]
        command.process_group(0);
        // Check after all preparation, immediately before the process side effect.
        if Instant::now() >= expires {
            return Err("test guardian initial lease expired");
        }
        let mut child = command.spawn().map_err(|_| "test gateway spawn failed")?;
        let pid = child.id().ok_or("test gateway identity unavailable")?;
        let (ready_tx, mut ready_rx) = mpsc::channel(1);
        let mut relay = tokio::spawn(relay(
            listener,
            launch.collector,
            launch.node_id,
            launch.epoch,
            launch.key,
            pid,
            ready_tx,
        ));
        let mut pipe_open = true;
        let mut relay_done = false;
        let mut clock_check = tokio::time::interval(Duration::from_millis(25));
        let reason = loop {
            tokio::select! {
                biased;
                _ = tokio::time::sleep_until(expires.into()) => break "lease_expired",
                status = child.wait() => {
                    relay.abort();
                    notice("stopped",Some(pid),"gateway_exit").await;
                    return status.map(|_|()).map_err(|_| "test gateway wait failed");
                }
                _ = &mut relay => { relay_done=true; break "collector_closed"; }
                Some(()) = ready_rx.recv() => { notice("ready",Some(pid),"gateway_core_ready").await; }
                message = inputs.recv(), if pipe_open => match message {
                    Some(Input::Renew{epoch,deadline_ms}) if epoch == launch.epoch => {
                        // A queued renewal cannot revive an already expired guardian.
                        if Instant::now() >= expires { break "lease_expired"; }
                        match local_deadline(deadline_ms,launch.max_lease_ms,&mut last_wall) {
                            Ok(next) if next > expires => expires = next,
                            Ok(_) => {},
                            Err(_) => break "invalid_lease",
                        }
                    }
                    Some(Input::Stop{}) => break "handoff",
                    Some(_) => break "invalid_control",
                    None => pipe_open=false,
                },
                _=clock_check.tick() => {
                    match wall_ms() {
                        Ok(now) if now >= last_wall => last_wall=now,
                        _ => break "clock_regressed",
                    }
                }
            }
        };
        notice("draining", Some(pid), reason).await;
        let mut grace = Duration::from_millis(launch.stop_grace_ms);
        if reason == "handoff" {
            // An orderly return may use the remainder of its already-approved
            // lease to drain work. It still stops before the voter's hold can
            // expire; no peer ACK or local shutdown request extends the lease.
            grace += expires.saturating_duration_since(Instant::now());
        }
        let cooperative = async {
            if let Some(pipe) = child.stdin.as_mut() {
                pipe.write_all(crate::shard_runtime::GATEWAY_SHUTDOWN_FRAME)
                    .await?;
                pipe.flush().await?;
            }
            child.wait().await
        };
        if !matches!(tokio::time::timeout(grace, cooperative).await, Ok(Ok(_))) {
            // Test mode's authorized lease fence: the real Gateway is terminated
            // before the voter hold expires. Worker stdin closes with its parent.
            let _ = child.start_kill();
            child
                .wait()
                .await
                .map_err(|_| "test gateway fence failed")?;
        }
        if !relay_done {
            let _ = tokio::time::timeout(WRITE_WAIT, &mut relay).await;
            relay.abort();
        }
        notice("stopped", Some(pid), reason).await;
        Ok(())
    }

    #[cfg(test)]
    mod tests {
        use super::*;
        #[test]
        fn guardian_deadlines_are_bounded_and_fail_closed() {
            let now = wall_ms().unwrap();
            let mut last = now;
            assert!(local_deadline(now.saturating_sub(1), 1000, &mut last).is_err());
            assert!(local_deadline(now + 2000, 1000, &mut last).is_err());
            assert!(local_deadline(now + 500, 1000, &mut last).is_ok());
            last = now + 10_000;
            assert!(local_deadline(now + 500, 1000, &mut last).is_err());
            assert!(wire_deadline(Instant::now() - Duration::from_secs(1)).is_err());
        }
        #[test]
        fn guardian_control_rejects_untrusted_fields() {
            for input in [
                r#"{"command":"stop","secret":"synthetic"}"#,
                r#"{"command":"renew","epoch":1,"deadline_ms":1,"force":true}"#,
                r#"{"command":"activate"}"#,
            ] {
                assert!(serde_json::from_str::<Input>(input).is_err());
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct ShardKey {
    pub bot_id: String,
    pub shard_id: u32,
}

/// Local administration state. Authentication and bounded framing belong to the
/// listener; only authenticated commands may be passed to this module.
pub mod control {
    #[cfg(unix)]
    fn core_status_view(
        core: &crate::core_units::CoreStatus,
        read: crate::core_status::StatusRead,
        last: &mut Option<(u64, u64, u64)>,
    ) -> serde_json::Value {
        use crate::core_status::StatusRead;
        use serde_json::json;
        let mut view = json!({
            "running": core.running, "failures": core.failures, "status": null,
            "incarnation": core.status_id.map(|id| format!("{id:016x}")),
            "empty": core.pid.is_none(),
        });
        if last.is_some_and(|(id, _, _)| Some(id) != core.status_id) {
            *last = None;
        }
        let reason = match read {
            StatusRead::Fresh {
                record,
                status_age_s,
            } if Some(record.pid) == core.pid && Some(record.generation) == core.status_id => {
                if last
                    .is_some_and(|(_, seq, boot_ms)| record.seq < seq || record.boot_ms < boot_ms)
                {
                    view["status_reason"] = json!("regressed");
                    return view;
                }
                *last = Some((record.generation, record.seq, record.boot_ms));
                view["status"] = json!({"pending": record.pending, "oldest_age_s": record.oldest_age_s, "warned": record.warned, "threshold_s": record.threshold_s, "status_age_s": status_age_s, "suspected_stalled": status_age_s >= 30, "generation": record.generation, "seq": record.seq});
                return view;
            }
            StatusRead::Fresh { .. } => "stale_generation",
            StatusRead::Missing => "unsupported",
            StatusRead::Unreadable => "unreadable",
            StatusRead::Corrupt => "corrupt",
            StatusRead::ClockUnavailable => "clock_unavailable",
        };
        view["status_reason"] = json!(reason);
        view
    }

    use super::*;
    use crate::{botref, placement::BotPlacement};
    use serde_json::{Value, json};
    use std::time::Instant;

    #[cfg(all(test, unix))]
    #[test]
    fn status_view_classifies_read_results() {
        use crate::core_status::{StatusRead, StatusRecord};
        let core = crate::core_units::CoreStatus {
            running: true,
            failures: 3,
            pid: Some(42),
            status_path: None,
            status_id: Some(7),
        };
        let mut last = None;
        let record = StatusRecord {
            pending: 2,
            oldest_age_s: 4,
            warned: 1,
            threshold_s: 30,
            boot_ms: 1,
            pid: 42,
            generation: 7,
            seq: 9,
        };
        for (age, suspected) in [(29, false), (30, true)] {
            let view = core_status_view(
                &core,
                StatusRead::Fresh {
                    record,
                    status_age_s: age,
                },
                &mut last,
            );
            assert_eq!(view["status"]["suspected_stalled"], suspected);
            assert_eq!(view["status"]["pending"], 2);
            assert_eq!(view["status"]["generation"], 7);
            assert_eq!(view["status"]["seq"], 9);
            assert_eq!(view["failures"], 3);
        }
        for (read, reason) in [
            (StatusRead::Missing, "unsupported"),
            (StatusRead::Unreadable, "unreadable"),
            (StatusRead::Corrupt, "corrupt"),
            (StatusRead::ClockUnavailable, "clock_unavailable"),
            (
                StatusRead::Fresh {
                    record: StatusRecord { pid: 43, ..record },
                    status_age_s: 0,
                },
                "stale_generation",
            ),
        ] {
            let view = core_status_view(&core, read, &mut last);
            assert!(view["status"].is_null());
            assert_eq!(view["status_reason"], reason);
        }
    }

    #[cfg(all(test, unix))]
    #[test]
    fn status_view_checks_incarnation_and_monotonicity() {
        use crate::core_status::{StatusRead, StatusRecord};
        let mut core = crate::core_units::CoreStatus {
            running: true,
            failures: 0,
            pid: Some(42),
            status_path: None,
            status_id: Some(7),
        };
        let record = StatusRecord {
            pending: 0,
            oldest_age_s: 0,
            warned: 0,
            threshold_s: 30,
            boot_ms: 100,
            pid: 42,
            generation: 7,
            seq: 9,
        };
        let mut last = None;
        let read = |record, status_age_s| StatusRead::Fresh {
            record,
            status_age_s,
        };
        assert_eq!(
            core_status_view(&core, read(record, 0), &mut last)["status"]["seq"],
            9
        );
        assert_eq!(
            core_status_view(
                &core,
                read(
                    StatusRecord {
                        generation: 8,
                        ..record
                    },
                    0
                ),
                &mut last
            )["status_reason"],
            "stale_generation"
        );
        assert_eq!(
            core_status_view(&core, read(StatusRecord { seq: 8, ..record }, 0), &mut last)["status_reason"],
            "regressed"
        );
        assert_eq!(
            core_status_view(&core, read(record, 30), &mut last)["status"]["suspected_stalled"],
            true
        );
        assert_eq!(
            core_status_view(
                &core,
                read(
                    StatusRecord {
                        boot_ms: 99,
                        ..record
                    },
                    0
                ),
                &mut last
            )["status_reason"],
            "regressed"
        );
        core.status_id = Some(8);
        let next = StatusRecord {
            generation: 8,
            seq: 1,
            boot_ms: 1,
            ..record
        };
        assert_eq!(
            core_status_view(&core, read(next, 0), &mut last)["status"]["seq"],
            1
        );
    }

    pub const NOTE: &str = "Local process status only; Discord readiness, Core drain, ownership and leases are not verified.";
    pub const OPERATION_LIMIT: usize = 1024;
    pub const OPERATION_SHARD_LIMIT: usize = 64;
    pub const OPERATION_TIMEOUT: Duration = Duration::from_secs(30);
    pub const CORE_RECOVERY_VERSION: u32 = 2;

    pub fn core_recovery_incarnation(id: &str) -> bool {
        id.len() == 16
            && id
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
            && id != "0000000000000000"
    }

    pub fn core_recovery_failure_reason(reason: &str) -> bool {
        matches!(
            reason,
            "audit_unavailable"
                | "audit_timeout"
                | "daemon_shutdown"
                | "shard_not_running"
                | "core_out_of_range"
                | "core_status_unavailable"
                | "incarnation_required"
                | "incarnation_mismatch"
                | "last_running_core"
                | "core_not_suspected_stalled"
                | "rate_limited"
                | "separate_cores_required"
                | "termination_unconfirmed"
                | "launch_failed"
                | "launch_timeout"
        )
    }
    fn update_timeout(shards: usize) -> Duration {
        Duration::from_secs(120 + 45 * shards as u64)
    }

    #[derive(Clone, Default, Deserialize, Serialize)]
    #[serde(deny_unknown_fields)]
    pub struct CommandRequest {
        pub command: String,
        #[serde(default)]
        pub bot: Option<String>,
        #[serde(default)]
        pub operation_id: Option<String>,
        #[serde(default)]
        pub force: bool,
        #[serde(default)]
        pub yes: bool,
        /// Restrict a lifecycle command to these local Shards of the Bot (`None` = all of them).
        /// Authorization is evaluated on exactly these Shards, so a Shard-scoped administrator
        /// can act on its own Shards only. An old daemon rejects the unknown field (fail closed).
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub shards: Option<Vec<u32>>,
        /// Refuse stale client-side guild routing before admitting any mutation.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub expect_total_shards: Option<u32>,
        /// Pin a selector resolved from a prior Bot listing to its exact identity.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub expect_bot_id: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub shard: Option<u32>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub core: Option<u32>,
        /// 16 桁の非ゼロの 16 進文字列。JSON の数値精度に依存しない。
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub expect_incarnation: Option<String>,
        #[serde(default, skip_serializing_if = "std::ops::Not::not")]
        pub expect_empty: bool,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pub core_recovery_version: Option<u32>,
    }

    impl CommandRequest {
        fn shards_valid(&self) -> bool {
            match &self.shards {
                None => true,
                Some(list) => {
                    matches!(
                        self.command.as_str(),
                        "start" | "stop" | "restart" | "gateway-update"
                    ) && !list.is_empty()
                        && list.len() <= 64
                        && list.iter().collect::<std::collections::BTreeSet<_>>().len()
                            == list.len()
                }
            }
        }

        /// Sorted Shard filter, or `None` for the whole Bot.
        fn shard_filter(&self) -> Option<Vec<u32>> {
            if self.command == "core-restart" {
                return self.shard.map(|shard| vec![shard]);
            }
            self.shards.as_ref().map(|list| {
                let mut sorted = list.clone();
                sorted.sort_unstable();
                sorted
            })
        }

        fn incarnation(&self) -> Option<u64> {
            let id = self.expect_incarnation.as_deref()?;
            if id.len() != 16 || !id.bytes().all(|byte| byte.is_ascii_hexdigit()) {
                return None;
            }
            u64::from_str_radix(id, 16).ok().filter(|&id| id != 0)
        }

        pub fn valid(&self) -> bool {
            let selector = self
                .bot
                .as_ref()
                .is_some_and(|v| !v.is_empty() && v.len() <= 256);
            let id = self.operation_id.as_ref().is_some_and(|v| {
                v.len() == 32
                    && v.bytes()
                        .all(|c| c.is_ascii_digit() || (b'a'..=b'f').contains(&c))
            });
            let base = match self.command.as_str() {
                "ps" => {
                    self.bot.is_none() && self.operation_id.is_none() && !self.force && !self.yes
                }
                "status" => {
                    (self.bot.is_none() || selector)
                        && self.operation_id.is_none()
                        && !self.force
                        && !self.yes
                }
                "inspect" | "shards" => {
                    selector && self.operation_id.is_none() && !self.force && !self.yes
                }
                "core-preview" => {
                    selector
                        && self.shard.is_some()
                        && self.operation_id.is_none()
                        && !self.force
                        && !self.yes
                        && self.core.is_none()
                }
                "operation" => self.bot.is_none() && id && !self.force && !self.yes,
                "start" | "restart" | "gateway-prepare" | "gateway-update" => {
                    selector && id && !self.force && !self.yes
                }
                "restart-all" => self.bot.is_none() && id && !self.force && self.yes,
                "core-restart" => {
                    selector
                        && id
                        && !self.yes
                        && self.shard.is_some()
                        && self.core.is_some()
                        && self.expect_bot_id.is_some()
                        && self.expect_total_shards.is_some()
                        && self.core_recovery_version == Some(CORE_RECOVERY_VERSION)
                        && (self.expect_incarnation.is_none() || self.incarnation().is_some())
                        && !(self.expect_empty && self.expect_incarnation.is_some())
                }
                // Missing confirmation is diagnosed by the authenticated handler.
                "stop" => selector && id && (!self.yes || self.force),
                _ => false,
            };
            base && self.shards_valid()
                && (self.command == "core-restart" || self.core_recovery_version.is_none())
                && (matches!(self.command.as_str(), "core-restart" | "core-preview")
                    || self.shard.is_none())
                && (self.command == "core-restart"
                    || (self.core.is_none()
                        && self.expect_incarnation.is_none()
                        && !self.expect_empty))
                && self.expect_bot_id.as_ref().is_none_or(|bot_id| {
                    !bot_id.is_empty()
                        && bot_id.len() <= 64
                        && bot_id.trim() == bot_id.as_str()
                        && !bot_id.chars().any(char::is_control)
                        && matches!(
                            self.command.as_str(),
                            "start" | "stop" | "restart" | "gateway-update" | "core-restart"
                        )
                })
                && self.expect_total_shards.is_none_or(|total| {
                    total > 0
                        && matches!(
                            self.command.as_str(),
                            "start" | "stop" | "restart" | "gateway-update" | "core-restart"
                        )
                })
        }
    }

    #[derive(Clone, Copy, PartialEq, Eq, Serialize)]
    #[serde(rename_all = "lowercase")]
    pub enum OperationState {
        Running,
        Succeeded,
        Failed,
    }

    #[derive(Clone, Serialize)]
    pub struct OperationShard {
        #[serde(flatten)]
        pub process: ProcessSnapshot,
        pub error: Option<DaemonError>,
    }

    #[derive(Clone, Serialize)]
    pub struct OperationSnapshot {
        pub operation_id: String,
        pub command: String,
        pub bot_id: Option<String>,
        pub state: OperationState,
        pub elapsed_ms: u128,
        pub shards: Vec<OperationShard>,
        pub failed_shards: Vec<ShardKey>,
    }

    #[derive(Clone, Copy, PartialEq, Eq)]
    enum UpdateStage {
        /// The standby is not started yet.
        Begin,
        #[cfg(unix)]
        WaitingIdentify,
        /// Started, logging in.
        Warming,
        /// Told to take the Cores; waiting for its confirmation.
        Activating,
    }

    #[derive(PartialEq, Eq)]
    enum Phase {
        Starting,
        Stopping,
        /// Gateway update (separated layout), one Shard at a time.
        Updating,
        #[cfg(unix)]
        CoreRestart,
    }

    #[cfg(unix)]
    const CORE_RESTART_INTERVAL: Duration = Duration::from_secs(60);
    #[cfg(unix)]
    const CORE_RESTART_WINDOW: Duration = Duration::from_secs(15 * 60);
    #[cfg(unix)]
    const CORE_RESTART_LIMIT: usize = 3;
    #[cfg(unix)]
    const AUDIT_QUEUE_LIMIT: usize = 64;
    pub const AUDIT_ACK_TIMEOUT: Duration = Duration::from_secs(5);
    #[cfg(unix)]
    const AUDIT_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(2);
    #[cfg(unix)]
    const CORE_LAUNCH_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(2);
    const CORE_RESTART_NOTE: &str = "succeeded は、旧 Core があれば終了を確認し、新しい Core を起動して stdin へ IPC capability を書き込んだことを示します。IPC の再接続と処理の状態は別途確認してください。中断したイベントの処理や未受領の REST 結果は不明になり得ます。";

    #[cfg(unix)]
    #[derive(Clone, Copy, PartialEq, Eq)]
    struct CoreRestartTarget {
        core: u32,
        total_shards: u32,
        expected: Option<u64>,
        expect_empty: bool,
    }

    #[cfg(unix)]
    struct CoreRestart {
        target: CoreRestartTarget,
        incarnation: Option<u64>,
        actor: (String, String),
        reason: Option<&'static str>,
        pending: Option<PendingCoreRestart>,
        charge_clock: Option<CoreAuditClock>,
        charged: bool,
    }

    #[cfg(unix)]
    #[derive(Deserialize, PartialEq, Eq)]
    #[serde(deny_unknown_fields)]
    struct CoreAuditRow {
        #[serde(rename = "ts")]
        _ts: u64,
        event: String,
        operation_id: String,
        bot_id: Option<String>,
        shard: u32,
        core: u32,
        subject: String,
        role: String,
        force: bool,
        incarnation: Option<String>,
        total_shards: Option<u32>,
        #[serde(default)]
        expect_empty: bool,
        reason: Option<String>,
        charge_clock: Option<CoreAuditClock>,
    }

    #[cfg(unix)]
    #[derive(Deserialize, Serialize, PartialEq, Eq)]
    #[serde(deny_unknown_fields)]
    struct CoreAuditClock {
        boot_id: String,
        boot_ms: u64,
    }

    #[cfg(unix)]
    struct ArchivedCoreRestart {
        requested: CoreAuditRow,
        terminal: Option<CoreAuditRow>,
    }

    #[cfg(unix)]
    impl ArchivedCoreRestart {
        fn state(&self) -> &str {
            self.terminal
                .as_ref()
                .map_or("unknown", |row| row.event.as_str())
        }
    }

    #[cfg(unix)]
    type CoreRestartBudget = BTreeMap<(ShardKey, u32), std::collections::VecDeque<Instant>>;

    #[cfg(unix)]
    struct CoreAuditStartup {
        replay: CoreAuditReplay,
        boot_id: Option<String>,
        budget: CoreRestartBudget,
    }

    #[cfg(unix)]
    #[derive(Default)]
    struct CoreAuditReplay {
        archived: BTreeMap<String, ArchivedCoreRestart>,
        orphaned: Vec<CoreAuditRow>,
        reserved: HashSet<String>,
        invalid: bool,
    }

    #[cfg(unix)]
    impl CoreAuditRow {
        fn key(&self) -> ShardKey {
            ShardKey {
                bot_id: self.bot_id.clone().expect("admission has canonical Bot ID"),
                shard_id: self.shard,
            }
        }

        fn same_admission(&self, other: &Self) -> bool {
            self.bot_id == other.bot_id
                && self.shard == other.shard
                && self.core == other.core
                && self.subject == other.subject
                && self.role == other.role
                && self.force == other.force
                && self.incarnation == other.incarnation
                && self.total_shards == other.total_shards
                && self.expect_empty == other.expect_empty
        }

        fn valid(&self) -> bool {
            let bot_valid = self.bot_id.as_ref().is_some_and(|id| {
                id.parse::<u64>()
                    .is_ok_and(|value| value.to_string() == *id)
            });
            let actor = super::update_control::ControlAccess {
                subject: self.subject.clone(),
                role: self.role.clone(),
                scope: None,
            };
            let common = actor.valid()
                && self.total_shards.is_some_and(|total| total > 0)
                && self
                    .incarnation
                    .as_deref()
                    .is_none_or(core_recovery_incarnation)
                && !(self.expect_empty && self.incarnation.is_some());
            if !common {
                return false;
            }
            if self.event == "rejected" {
                return (self.bot_id.is_none() || bot_valid)
                    && self.charge_clock.is_none()
                    && self.reason.as_deref().is_some_and(|reason| {
                        core_recovery_failure_reason(reason)
                            || matches!(
                                reason,
                                "permission_denied"
                                    | "bot_not_found"
                                    | "bot_id_mismatch"
                                    | "total_shards_mismatch"
                                    | "operation_id_conflict"
                                    | "shard_not_local"
                                    | "busy"
                                    | "operation_history_full"
                            )
                    });
            }
            bot_valid
                && self.role == "admin"
                && self.total_shards.is_some_and(|total| self.shard < total)
                && self.core < crate::core_attach::SLOTS as u32
                && (self.expect_empty || self.incarnation.is_some())
                && self
                    .charge_clock
                    .as_ref()
                    .is_none_or(|clock| Self::valid_boot_id(&clock.boot_id))
                && match self.event.as_str() {
                    "requested" => self.reason.is_none() && self.charge_clock.is_none(),
                    "succeeded" => self.reason.is_none(),
                    "failed" => self
                        .reason
                        .as_deref()
                        .is_some_and(core_recovery_failure_reason),
                    _ => false,
                }
        }

        fn valid_boot_id(id: &str) -> bool {
            id.len() == 36
                && id.bytes().enumerate().all(|(index, byte)| {
                    if matches!(index, 8 | 13 | 18 | 23) {
                        byte == b'-'
                    } else {
                        byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)
                    }
                })
        }
    }

    #[cfg(unix)]
    struct ControlReply {
        response: tokio::sync::oneshot::Sender<Value>,
        recovery_audit: Value,
    }

    #[cfg(unix)]
    impl ControlReply {
        fn send(self, mut value: Value) {
            value["recovery_audit"] = self.recovery_audit;
            let _ = self.response.send(value);
        }

        fn respond(reply: Option<Self>, value: Value) -> Option<Value> {
            if let Some(reply) = reply {
                reply.send(value);
                None
            } else {
                Some(value)
            }
        }
    }

    #[cfg(unix)]
    struct PendingCoreRestart {
        audit: AuditResult,
        // None after cancellation. A later acknowledgement can only record an outcome.
        reply: Option<ControlReply>,
    }

    #[cfg(unix)]
    struct PendingGatewayUpdate {
        request: CommandRequest,
        bot: Option<String>,
        actor: (String, String),
        audit: AuditResult,
        reply: ControlReply,
        deadline: Instant,
    }

    struct Operation {
        id: String,
        command: String,
        bot_id: Option<String>,
        force: bool,
        /// The requested Shard filter (sorted), kept so a repeated ID can be checked for conflicts.
        shards: Option<Vec<u32>>,
        targets: Vec<ShardKey>,
        phase: Phase,
        next: usize,
        started: Instant,
        update_timeout: Duration,
        waiting: HashSet<ShardKey>,
        generations: BTreeMap<ShardKey, LaunchGeneration>,
        errors: BTreeMap<ShardKey, DaemonError>,
        finished: Option<OperationSnapshot>,
        /// `gateway-update`: where the Shard `targets[next]` is in its update.
        update_stage: UpdateStage,
        #[cfg(unix)]
        core_restart: Option<CoreRestart>,
    }

    impl Operation {
        fn new(
            request: &CommandRequest,
            bot_id: Option<String>,
            targets: Vec<ShardKey>,
            waiting: HashSet<ShardKey>,
            phase: Phase,
            now: Instant,
        ) -> Self {
            Self {
                id: request.operation_id.clone().expect("validated ID"),
                command: request.command.clone(),
                bot_id,
                force: request.force,
                shards: request.shard_filter(),
                update_timeout: update_timeout(targets.len()),
                targets,
                phase,
                next: 0,
                started: now,
                waiting,
                generations: BTreeMap::new(),
                errors: BTreeMap::new(),
                finished: None,
                update_stage: UpdateStage::Begin,
                #[cfg(unix)]
                core_restart: None,
            }
        }

        fn is_restart(&self) -> bool {
            matches!(self.command.as_str(), "restart" | "restart-all")
        }
    }

    /// Per Core slot: the last accepted status record as (incarnation id, seq, boot_ms), used to
    /// reject regressions within one incarnation (see `core_status_view`).
    #[cfg(unix)]
    type CoreStatusHistory = std::sync::Mutex<BTreeMap<(ShardKey, usize), Option<(u64, u64, u64)>>>;

    /// One owner serializes admissions; independent Bot operations advance on
    /// ticks while clients may read status. History never evicts accepted IDs:
    /// at capacity new operations fail before any side effect. It is not durable.
    pub struct ControlPlane {
        daemon: Daemon,
        config: Config,
        operations: BTreeMap<String, Operation>,
        monitor_ok: bool,
        last_advanced: String,
        /// Where `gateway-update` requests and outcomes are recorded (one JSON object per
        /// line, 0600). None leaves them unrecorded, as before.
        gateway_update_audit: Option<PathBuf>,
        /// Who asked, per operation, so the outcome can be recorded against them.
        gateway_update_actors: BTreeMap<String, (String, String)>,
        pending_audit: Vec<PendingAudit>,
        audit_retry_at: Option<Instant>,
        // Shared across records: a failed truncate must not quarantine the same tail again
        // when a different request or pending record next tries to append.
        audit_tail: std::sync::Mutex<Option<AuditTailRepair>>,
        #[cfg(all(test, unix))]
        core_recovery_audit: Option<PathBuf>,
        #[cfg(all(test, unix))]
        core_audit_tail: std::sync::Mutex<Option<AuditTailRepair>>,
        #[cfg(unix)]
        core_audit_worker: Option<tokio::sync::mpsc::Sender<CoreAuditJob>>,
        #[cfg(unix)]
        core_audit_replay:
            Option<tokio::sync::oneshot::Receiver<std::io::Result<CoreAuditStartup>>>,
        #[cfg(unix)]
        core_audit_ready: bool,
        #[cfg(unix)]
        core_audit_boot_id: Option<String>,
        #[cfg(unix)]
        core_audit_failures: std::sync::Arc<std::sync::atomic::AtomicU64>,
        #[cfg(unix)]
        core_audit_rejections: Vec<AuditResult>,
        #[cfg(unix)]
        gateway_audit_worker: Option<tokio::sync::mpsc::Sender<CoreAuditJob>>,
        #[cfg(unix)]
        gateway_audit_failures: std::sync::Arc<std::sync::atomic::AtomicU64>,
        #[cfg(unix)]
        pending_gateway_updates: Vec<PendingGatewayUpdate>,
        #[cfg(unix)]
        audit_worker_exits: Vec<tokio::sync::oneshot::Receiver<()>>,
        #[cfg(unix)]
        audit_writes: Vec<(AuditKind, AuditResult)>,
        #[cfg(unix)]
        core_audit_synced: HashSet<String>,
        #[cfg(unix)]
        archived_core_restarts: BTreeMap<String, ArchivedCoreRestart>,
        #[cfg(unix)]
        reserved_core_ids: HashSet<String>,
        #[cfg(unix)]
        core_restart_history: CoreRestartBudget,
        #[cfg(unix)]
        core_status_history: CoreStatusHistory,
    }

    const AUDIT_SCAN_BYTES: usize = 8 * 1024;
    const AUDIT_TAIL_LIMIT: u64 = 64 * 1024;
    const AUDIT_CORRUPT_LIMIT: u64 = 1024 * 1024;
    const AUDIT_RETRY_INTERVAL: Duration = Duration::from_secs(1);

    struct AuditTailRepair {
        // Source device, inode, last newline boundary, and measured end.
        source: (u64, u64, u64, u64),
        corrupt: Option<std::fs::File>,
        limit: u64,
        copied: u64,
    }

    #[derive(Clone, Copy, PartialEq, Eq)]
    enum AuditKind {
        GatewayUpdate,
        CoreRecovery,
    }

    struct PendingAudit {
        kind: AuditKind,
        bytes: Vec<u8>,
        written: bool,
        // Device, inode, and the offset of the serialized record (not the file length).
        written_file: Option<(u64, u64, u64)>,
        #[cfg(unix)]
        core_terminal_id: Option<String>,
    }

    #[cfg(unix)]
    type AuditResult = tokio::sync::oneshot::Receiver<(PendingAudit, bool)>;

    #[cfg(unix)]
    struct CoreAuditJob {
        record: PendingAudit,
        completion: Option<tokio::sync::oneshot::Sender<(PendingAudit, bool)>>,
    }

    impl PendingAudit {
        fn new(kind: AuditKind, value: Value) -> std::io::Result<Self> {
            let mut bytes = serde_json::to_vec(&value).map_err(std::io::Error::other)?;
            bytes.push(b'\n');
            Ok(Self {
                kind,
                bytes,
                written: false,
                written_file: None,
                #[cfg(unix)]
                core_terminal_id: None,
            })
        }
    }

    impl ControlPlane {
        /// Records `gateway-update` requests and outcomes in an append-only file. A request
        /// whose record cannot be written is refused before anything starts. The record holds
        /// the subject and role (already validated names), the Bot, the operation ID and a
        /// fixed outcome word - never a secret, path, argument or error text.
        pub fn set_gateway_update_audit(&mut self, path: PathBuf) {
            self.gateway_update_audit = Some(path);
        }

        #[cfg(unix)]
        pub fn start_gateway_update_audit(&mut self, path: PathBuf) -> std::io::Result<()> {
            self.gateway_audit_worker =
                Some(self.start_audit_worker(path, |_| {}, self.gateway_audit_failures.clone())?);
            Ok(())
        }

        #[cfg(unix)]
        pub fn start_core_recovery_audit(&mut self, path: PathBuf) -> std::io::Result<()> {
            self.start_core_recovery_audit_with(path, Self::read_core_recovery_audit)
        }

        #[cfg(unix)]
        fn start_core_recovery_audit_with(
            &mut self,
            path: PathBuf,
            read: impl FnOnce(&Path) -> std::io::Result<CoreAuditReplay> + Send + 'static,
        ) -> std::io::Result<()> {
            let (replay, receive) = tokio::sync::oneshot::channel();
            self.core_audit_ready = false;
            self.core_audit_worker = Some(self.start_audit_worker(
                path,
                move |path| {
                    let startup = read(path).map(|replay| {
                        let boot_id = Self::boot_id();
                        let clock = boot_id.as_deref().zip(crate::core_status::try_boot_ms());
                        let budget =
                            Self::replayed_core_restart_budget(&replay, Instant::now(), clock);
                        CoreAuditStartup {
                            replay,
                            boot_id,
                            budget,
                        }
                    });
                    let _ = replay.send(startup);
                },
                self.core_audit_failures.clone(),
            )?);
            self.core_audit_replay = Some(receive);
            Ok(())
        }

        #[cfg(unix)]
        fn start_audit_worker(
            &mut self,
            path: PathBuf,
            initialize: impl FnOnce(&Path) + Send + 'static,
            failures: std::sync::Arc<std::sync::atomic::AtomicU64>,
        ) -> std::io::Result<tokio::sync::mpsc::Sender<CoreAuditJob>> {
            let (send, mut jobs) = tokio::sync::mpsc::channel::<CoreAuditJob>(AUDIT_QUEUE_LIMIT);
            let (done, exited) = tokio::sync::oneshot::channel();
            // A filesystem call may never return. Keep it off the Tokio runtime so
            // dropping the runtime cannot join a stalled worker after the shutdown deadline.
            std::thread::Builder::new()
                .name("control-audit".into())
                .spawn(move || {
                    initialize(&path);
                    let tail = std::sync::Mutex::new(None);
                    while let Some(mut job) = jobs.blocking_recv() {
                        let ok = Self::write_audit_file(&path, &tail, &mut job.record).is_ok();
                        if let Some(completion) = job.completion {
                            let _ = completion.send((job.record, ok));
                        } else if !ok {
                            failures.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        }
                    }
                    let _ = done.send(());
                })?;
            self.audit_worker_exits.push(exited);
            Ok(send)
        }

        #[cfg(unix)]
        fn boot_id() -> Option<String> {
            let id = std::fs::read_to_string("/proc/sys/kernel/random/boot_id").ok()?;
            let id = id.trim();
            CoreAuditRow::valid_boot_id(id).then(|| id.to_owned())
        }

        #[cfg(unix)]
        fn poll_core_audit_replay(&mut self) {
            use tokio::sync::oneshot::error::TryRecvError;
            let Some(replay) = self.core_audit_replay.as_mut() else {
                return;
            };
            match replay.try_recv() {
                Err(TryRecvError::Empty) => return,
                Ok(Ok(startup)) => {
                    self.core_restart_history = startup.budget;
                    self.archived_core_restarts = startup.replay.archived;
                    self.reserved_core_ids = startup.replay.reserved;
                    self.core_audit_ready = !startup.replay.invalid;
                    self.core_audit_boot_id = startup.boot_id;
                }
                _ => {}
            }
            self.core_audit_replay = None;
            if !self.core_audit_ready {
                eprintln!("Core recovery audit history unavailable; manual Core recovery disabled");
            }
        }

        #[cfg(unix)]
        fn read_core_recovery_audit(path: &Path) -> std::io::Result<CoreAuditReplay> {
            use std::io::{BufRead, BufReader, Read};
            let file = Self::open_audit_file(path)?;
            if let Some(parent) = path
                .parent()
                .filter(|parent| !parent.as_os_str().is_empty())
            {
                std::fs::File::open(parent)?.sync_all()?;
            }
            let mut reader = BufReader::new(file);
            let mut row = Vec::new();
            let mut replay = CoreAuditReplay::default();
            let mut rows = BTreeMap::<String, (Option<CoreAuditRow>, Option<CoreAuditRow>)>::new();
            let mut invalid_ids = HashSet::new();
            loop {
                row.clear();
                if reader.by_ref().take(4097).read_until(b'\n', &mut row)? == 0 {
                    break;
                }
                if row.len() > 4096 {
                    // Keep scanning whole records to retain recognizable IDs after this row.
                    for id in Self::compact_audit_ids(&row) {
                        replay.reserved.insert(id.clone());
                        invalid_ids.insert(id);
                    }
                    if row.last() != Some(&b'\n') {
                        reader.skip_until(b'\n')?;
                    }
                    replay.invalid = true;
                    continue;
                }
                let complete = row.last() == Some(&b'\n');
                let Ok(value) = serde_json::from_slice::<Value>(&row) else {
                    // The writer repairs a torn final append before the next record.
                    for id in Self::compact_audit_ids(&row) {
                        replay.reserved.insert(id.clone());
                        if complete {
                            invalid_ids.insert(id);
                        }
                    }
                    replay.invalid |= complete;
                    continue;
                };
                let Some(id) = value["operation_id"]
                    .as_str()
                    .filter(|id| Self::valid_audit_id(id))
                    .map(str::to_owned)
                else {
                    replay.invalid |= complete;
                    continue;
                };
                replay.reserved.insert(id.clone());
                if !complete {
                    continue;
                }
                let required = ["bot_id", "incarnation", "reason", "expect_empty"];
                let record = if required.iter().all(|key| value.get(*key).is_some()) {
                    serde_json::from_slice::<CoreAuditRow>(&row).ok()
                } else {
                    None
                };
                let Some(record) = record.filter(CoreAuditRow::valid) else {
                    for id in Self::compact_audit_ids(&row) {
                        replay.reserved.insert(id.clone());
                        invalid_ids.insert(id);
                    }
                    invalid_ids.insert(id);
                    replay.invalid = true;
                    continue;
                };
                if record.event == "rejected" {
                    continue;
                }
                let (requested, terminal) = rows.entry(id.clone()).or_default();
                if record.event == "requested" && requested.is_none() && terminal.is_some() {
                    invalid_ids.insert(id);
                    replay.invalid = true;
                    continue;
                }
                let slot = if record.event == "requested" {
                    requested
                } else {
                    terminal
                };
                match slot {
                    Some(existing) if *existing != record => {
                        invalid_ids.insert(id);
                        replay.invalid = true;
                    }
                    Some(_) => {}
                    None => *slot = Some(record),
                }
            }
            for (id, (requested, terminal)) in rows {
                if invalid_ids.contains(&id) {
                    continue;
                }
                match (requested, terminal) {
                    (Some(requested), terminal) => {
                        if terminal
                            .as_ref()
                            .is_some_and(|row| !requested.same_admission(row))
                        {
                            replay.invalid = true;
                            continue;
                        }
                        replay.archived.insert(
                            id,
                            ArchivedCoreRestart {
                                requested,
                                terminal,
                            },
                        );
                    }
                    (None, Some(terminal)) => replay.orphaned.push(terminal),
                    (None, None) => {}
                }
            }
            Ok(replay)
        }

        #[cfg(unix)]
        fn valid_audit_id(id: &str) -> bool {
            id.len() == 32
                && id
                    .bytes()
                    .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        }

        #[cfg(unix)]
        fn compact_audit_ids(row: &[u8]) -> impl Iterator<Item = String> {
            // A torn append can contain the complete ID followed by an incomplete
            // field. Reserve it, but never interpret the record as an outcome.
            const KEY: &[u8] = b"\"operation_id\":\"";
            row.windows(KEY.len() + 34).filter_map(|bytes| {
                if !matches!(bytes[0], b'{' | b',')
                    || &bytes[1..=KEY.len()] != KEY
                    || bytes.last() != Some(&b'"')
                {
                    return None;
                }
                let id = std::str::from_utf8(&bytes[KEY.len() + 1..bytes.len() - 1]).ok()?;
                Self::valid_audit_id(id).then(|| id.to_owned())
            })
        }

        #[cfg(unix)]
        fn replayed_core_restart_budget(
            replay: &CoreAuditReplay,
            now: Instant,
            boot_clock: Option<(&str, u64)>,
        ) -> CoreRestartBudget {
            let rows = replay
                .archived
                .values()
                .map(|op| (&op.requested, op.terminal.as_ref()))
                .chain(replay.orphaned.iter().map(|row| (row, Some(row))));
            let mut recent: Vec<_> = rows
                .filter_map(|(row, terminal)| {
                    let age = terminal
                        .and_then(|row| row.charge_clock.as_ref())
                        .zip(boot_clock)
                        .filter(|(stamp, (boot_id, _))| stamp.boot_id == *boot_id)
                        .and_then(|(stamp, (_, ms))| ms.checked_sub(stamp.boot_ms))
                        .map_or(Duration::ZERO, Duration::from_millis);
                    (age < CORE_RESTART_WINDOW).then(|| (now.checked_sub(age).unwrap_or(now), row))
                })
                .collect();
            recent.sort_by_key(|(at, _)| *at);
            let mut budget = CoreRestartBudget::new();
            for (at, row) in recent {
                let history = budget.entry((row.key(), row.core)).or_default();
                if history.len() == CORE_RESTART_LIMIT {
                    history.pop_front();
                }
                history.push_back(at);
            }
            budget
        }

        #[cfg(unix)]
        fn archived_core_view(op: &ArchivedCoreRestart) -> Value {
            let row = &op.requested;
            let state = op.state();
            let terminal = op.terminal.as_ref();
            json!({
                "ok": state == "succeeded", "offline": false,
                "operation": {
                    "operation_id": row.operation_id, "command": "core-restart",
                    "core_recovery_version": CORE_RECOVERY_VERSION, "source": "archive",
                    "bot_id": row.bot_id, "shard": row.shard, "core": row.core,
                    "incarnation": row.incarnation, "state": state,
                    "reason": terminal.map_or(Some("terminal_audit_missing"), |row| row.reason.as_deref()),
                    "terminal_audit": if terminal.is_none() { "missing" } else { "synced" },
                },
                "note": CORE_RESTART_NOTE,
            })
        }

        fn audit_gateway_update(
            &self,
            event: &str,
            operation_id: &str,
            bot: Option<&str>,
            actor: &(String, String),
            error: Option<DaemonError>,
        ) -> std::io::Result<()> {
            let mut record = Self::audit_record(event, operation_id, bot, actor, error)?;
            self.write_audit(&mut record)
        }

        fn audit_record(
            event: &str,
            operation_id: &str,
            bot: Option<&str>,
            actor: &(String, String),
            error: Option<DaemonError>,
        ) -> std::io::Result<PendingAudit> {
            PendingAudit::new(
                AuditKind::GatewayUpdate,
                json!({
                    "at_ms": Self::audit_timestamp(), "event": event, "operation_id": operation_id,
                    "bot": bot, "subject": actor.0, "role": actor.1,
                    "error": error.map(|e| format!("{e:?}")),
                }),
            )
        }

        fn audit_timestamp() -> u128 {
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_or(0, |d| d.as_millis())
        }

        fn audit_file_identity(meta: &std::fs::Metadata) -> (u64, u64) {
            #[cfg(unix)]
            {
                use std::os::unix::fs::MetadataExt;
                (meta.dev(), meta.ino())
            }
            #[cfg(not(unix))]
            {
                let _ = meta;
                (0, 0) // Linux is the supported platform.
            }
        }

        fn open_audit_file(path: &std::path::Path) -> std::io::Result<std::fs::File> {
            #[cfg(unix)]
            {
                crate::core_units::open_append_private(path)
            }
            #[cfg(not(unix))]
            {
                std::fs::OpenOptions::new()
                    .read(true)
                    .create(true)
                    .append(true)
                    .open(path)
            }
        }

        fn audit_tail_boundary(
            file: &mut (impl std::io::Read + std::io::Seek),
            mut end: u64,
        ) -> std::io::Result<u64> {
            use std::io::SeekFrom;
            let mut buffer = [0; AUDIT_SCAN_BYTES];
            while end > 0 {
                let start = end.saturating_sub(buffer.len() as u64);
                let count = (end - start) as usize;
                file.seek(SeekFrom::Start(start))?;
                file.read_exact(&mut buffer[..count])?;
                if let Some(index) = buffer[..count].iter().rposition(|&b| b == b'\n') {
                    return Ok(start + index as u64 + 1);
                }
                end = start;
            }
            Ok(0)
        }

        fn quarantine_audit_tail(
            tail: &std::sync::Mutex<Option<AuditTailRepair>>,
            file: &mut std::fs::File,
            path: &std::path::Path,
            boundary: u64,
            end: u64,
        ) -> std::io::Result<()> {
            use std::io::{Read, Seek, SeekFrom, Write};
            let (dev, ino) = Self::audit_file_identity(&file.metadata()?);
            let source = (dev, ino, boundary, end);
            let mut saved = tail
                .lock()
                .map_err(|_| std::io::Error::other("audit repair state unavailable"))?;
            if saved.as_ref().is_none_or(|repair| repair.source != source) {
                *saved = Some(AuditTailRepair {
                    source,
                    corrupt: None,
                    limit: 0,
                    copied: 0,
                });
            }
            let repair = saved.as_mut().expect("repair initialized");
            if repair.corrupt.is_none() {
                let mut corrupt_path = path.as_os_str().to_os_string();
                corrupt_path.push(".corrupt");
                let corrupt = Self::open_audit_file(std::path::Path::new(&corrupt_path))?;
                repair.limit = (end - boundary)
                    .min(AUDIT_TAIL_LIMIT)
                    .min(AUDIT_CORRUPT_LIMIT.saturating_sub(corrupt.metadata()?.len()));
                if repair.limit < end - boundary {
                    eprintln!("監査ログの破損部分が隔離の上限を超えました（超過分は破棄）");
                }
                repair.corrupt = Some(corrupt);
            }
            let mut buffer = [0; AUDIT_SCAN_BYTES];
            while repair.copied < repair.limit {
                let count = (repair.limit - repair.copied).min(buffer.len() as u64) as usize;
                file.seek(SeekFrom::Start(boundary + repair.copied))?;
                file.read_exact(&mut buffer[..count])?;
                let mut written = 0;
                while written < count {
                    // Remember each successful short write, including when a later write
                    // fails. Retain the descriptor so a sync failure retries only the sync.
                    let n = match repair
                        .corrupt
                        .as_mut()
                        .expect("quarantine opened")
                        .write(&buffer[written..count])
                    {
                        Err(error) if error.kind() == std::io::ErrorKind::Interrupted => continue,
                        result => result?,
                    };
                    if n == 0 {
                        return Err(std::io::ErrorKind::WriteZero.into());
                    }
                    written += n;
                    repair.copied += n as u64;
                }
            }
            repair
                .corrupt
                .as_ref()
                .expect("quarantine opened")
                .sync_data()
        }

        fn audit_record_matches(
            file: &mut std::fs::File,
            record: &PendingAudit,
        ) -> std::io::Result<bool> {
            use std::io::{Read, Seek, SeekFrom};
            let Some((dev, ino, offset)) = record.written_file else {
                return Ok(false);
            };
            let meta = file.metadata()?;
            if Self::audit_file_identity(&meta) != (dev, ino)
                || offset
                    .checked_add(record.bytes.len() as u64)
                    .is_none_or(|end| end > meta.len())
            {
                return Ok(false);
            }
            file.seek(SeekFrom::Start(offset))?;
            let mut buffer = [0; AUDIT_SCAN_BYTES];
            for expected in record.bytes.chunks(buffer.len()) {
                match file.read_exact(&mut buffer[..expected.len()]) {
                    Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof => {
                        return Ok(false);
                    }
                    result => result?,
                }
                if &buffer[..expected.len()] != expected {
                    return Ok(false);
                }
            }
            Ok(true)
        }

        fn write_audit(&self, record: &mut PendingAudit) -> std::io::Result<()> {
            let (path, tail) = match record.kind {
                AuditKind::GatewayUpdate => {
                    let Some(path) = &self.gateway_update_audit else {
                        return Ok(());
                    };
                    (path, &self.audit_tail)
                }
                #[cfg(all(test, unix))]
                AuditKind::CoreRecovery => (
                    self.core_recovery_audit
                        .as_ref()
                        .ok_or_else(|| std::io::Error::other("audit unavailable"))?,
                    &self.core_audit_tail,
                ),
                #[cfg(not(all(test, unix)))]
                AuditKind::CoreRecovery => return Err(std::io::Error::other("audit unavailable")),
            };
            Self::write_audit_file(path, tail, record)
        }

        fn write_audit_file(
            path: &Path,
            tail: &std::sync::Mutex<Option<AuditTailRepair>>,
            record: &mut PendingAudit,
        ) -> std::io::Result<()> {
            use std::io::{Seek, SeekFrom, Write};
            // This verified descriptor is read/write + append. Never reopen the path for
            // reading: a FIFO replacement could block that second open before validation.
            let mut file = Self::open_audit_file(path)?;
            if record.written && !Self::audit_record_matches(&mut file, record)? {
                record.written = false;
                record.written_file = None;
            }
            if !record.written {
                let end = file.metadata()?.len();
                let boundary = Self::audit_tail_boundary(&mut file, end)?;
                if boundary < end {
                    Self::quarantine_audit_tail(tail, &mut file, path, boundary, end)?;
                    // Keep repair progress on failure, including a failure of this truncate.
                    file.set_len(boundary)?;
                }
                *tail
                    .lock()
                    .map_err(|_| std::io::Error::other("audit repair state unavailable"))? = None;
                let (dev, ino) = Self::audit_file_identity(&file.metadata()?);
                let offset = file.seek(SeekFrom::End(0))?;
                file.write_all(&record.bytes)?;
                record.written = true;
                record.written_file = Some((dev, ino, offset));
            }
            file.sync_data()
        }

        #[cfg(unix)]
        fn submit_audit(
            worker: &tokio::sync::mpsc::Sender<CoreAuditJob>,
            record: PendingAudit,
            tracked: bool,
        ) -> Result<Option<AuditResult>, PendingAudit> {
            let (completion, result) = if tracked {
                let (tx, rx) = tokio::sync::oneshot::channel();
                (Some(tx), Some(rx))
            } else {
                (None, None)
            };
            worker
                .try_send(CoreAuditJob { record, completion })
                .map_err(|error| error.into_inner().record)?;
            Ok(result)
        }

        fn write_terminal_audit(&mut self, mut record: PendingAudit) {
            #[cfg(unix)]
            if let Some(worker) = match record.kind {
                AuditKind::CoreRecovery => self.core_audit_worker.as_ref(),
                AuditKind::GatewayUpdate => self.gateway_audit_worker.as_ref(),
            } {
                let kind = record.kind;
                match Self::submit_audit(worker, record, true) {
                    Ok(Some(result)) => self.audit_writes.push((kind, result)),
                    Ok(None) => unreachable!("tracked audit write"),
                    Err(record) => self.pending_audit.push(record),
                }
                return;
            }
            if self.write_audit(&mut record).is_err() {
                self.pending_audit.push(record);
                return;
            }
            #[cfg(unix)]
            if let Some(id) = record.core_terminal_id {
                self.core_audit_synced.insert(id);
            }
        }

        fn retry_pending_audit(&mut self) {
            #[cfg(unix)]
            self.core_audit_rejections.retain_mut(|result| {
                use tokio::sync::oneshot::error::TryRecvError;
                match result.try_recv() {
                    Err(TryRecvError::Empty) => return true,
                    Ok((_, true)) => {}
                    Ok((_, false)) | Err(TryRecvError::Closed) => {
                        self.core_audit_failures
                            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    }
                }
                false
            });
            #[cfg(unix)]
            self.audit_writes.retain_mut(|(kind, result)| {
                use tokio::sync::oneshot::error::TryRecvError;
                match result.try_recv() {
                    Ok((record, false)) => {
                        self.pending_audit.push(record);
                        self.audit_retry_at = Some(Instant::now() + AUDIT_RETRY_INTERVAL);
                    }
                    Ok((record, true)) => {
                        if let Some(id) = record.core_terminal_id {
                            self.core_audit_synced.insert(id);
                        }
                    }
                    Err(TryRecvError::Empty) => return true,
                    Err(TryRecvError::Closed) => {
                        let failures = match kind {
                            AuditKind::CoreRecovery => &self.core_audit_failures,
                            AuditKind::GatewayUpdate => &self.gateway_audit_failures,
                        };
                        failures.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    }
                }
                false
            });
            if self.audit_retry_at.is_some_and(|at| Instant::now() < at) {
                return;
            }
            let pending = std::mem::take(&mut self.pending_audit);
            for record in pending {
                self.write_terminal_audit(record);
            }
            self.audit_retry_at = if self.pending_audit.is_empty() {
                None
            } else {
                Some(Instant::now() + AUDIT_RETRY_INTERVAL)
            };
        }

        pub fn recovery_events_dropped(&self) -> u64 {
            self.daemon.recovery_events_dropped
        }

        pub fn take_recovery_events(&mut self) -> Vec<DaemonRecoveryEvent> {
            self.daemon.take_recovery_events()
        }

        pub fn operation_exists(&self, id: &str) -> bool {
            self.operations.contains_key(id) || self.operation_id_reserved(id)
        }

        #[cfg(unix)]
        fn operation_id_reserved(&self, id: &str) -> bool {
            self.reserved_core_ids.contains(id)
                || self.archived_core_restarts.contains_key(id)
                || self
                    .pending_gateway_updates
                    .iter()
                    .any(|pending| pending.request.operation_id.as_deref() == Some(id))
        }

        #[cfg(not(unix))]
        fn operation_id_reserved(&self, _id: &str) -> bool {
            false
        }

        fn replay_retry_response(&self) -> Option<Value> {
            #[cfg(unix)]
            if self.core_audit_replay.is_some() {
                let mut response = Self::error("audit_replay_pending");
                response["retry_after_ms"] = json!(100);
                return Some(response);
            }
            None
        }

        pub fn dispatch_manifest_update(
            &self,
            updates: &mut super::update_control::UpdateHub,
            request: crate::update_rollback::UpdateControlRequest,
            access: super::update_control::ControlAccess,
        ) -> Value {
            if let Some(response) = self.replay_retry_response() {
                return response;
            }
            if self.operation_exists(&request.operation_id) {
                return Self::error("operation ID conflicts with an earlier request");
            }
            let busy = self.operations.values().any(|op| {
                op.finished.is_none() && op.targets.iter().any(|key| key.bot_id == request.bot)
            });
            updates.admit(request, access, busy)
        }

        pub fn request_as(
            &mut self,
            request: &CommandRequest,
            access: &super::update_control::ControlAccess,
        ) -> Value {
            if request.command == "core-restart" {
                #[cfg(unix)]
                return self
                    .request_core_restart(request, access, Instant::now(), false, None)
                    .unwrap_or_else(|| Self::error("audit_unavailable"));
                #[cfg(not(unix))]
                return Self::error("core restart requires separate Core mode on Linux");
            }
            #[cfg(unix)]
            if request.command == "operation"
                && request.valid()
                && let Some(op) = request
                    .operation_id
                    .as_ref()
                    .and_then(|id| self.archived_core_restarts.get(id))
            {
                let key = op.requested.key();
                if !access.allows_command(true, Some(&key.bot_id), std::slice::from_ref(&key)) {
                    return Self::error("control permission or scope denied");
                }
                return Self::archived_core_view(op);
            }
            if request.command == "core-preview" {
                if !request.valid() {
                    return Self::error("invalid control request");
                }
                let bot = request
                    .bot
                    .as_deref()
                    .and_then(|selector| botref::resolve(&self.config.bots, selector).ok());
                let Some(bot) = bot else {
                    return Self::error("bot_not_found");
                };
                let key = ShardKey {
                    bot_id: bot.bot_id.clone(),
                    shard_id: request.shard.expect("validated shard"),
                };
                if !access.allows_command(true, Some(&key.bot_id), std::slice::from_ref(&key)) {
                    return Self::error("control permission or scope denied");
                }
                let Some(cores) = self.cores_view(&key) else {
                    return Self::error("local separate Core shard required");
                };
                return json!({"ok": true, "offline": false, "bot_id": key.bot_id,
                    "total_shards": bot.total_shards, "shard": key.shard_id, "cores": cores});
            }
            let target = if request.command == "operation" {
                request
                    .operation_id
                    .as_ref()
                    .and_then(|id| self.operations.get(id))
                    .and_then(|op| op.bot_id.clone())
            } else {
                request
                    .bot
                    .as_deref()
                    .and_then(|selector| botref::resolve(&self.config.bots, selector).ok())
                    .map(|bot| bot.bot_id.clone())
            };
            let read = matches!(
                request.command.as_str(),
                "ps" | "status" | "inspect" | "shards" | "operation"
            );
            let keys: Vec<_> = if request.command == "operation" {
                request
                    .operation_id
                    .as_ref()
                    .and_then(|id| self.operations.get(id))
                    .map(|op| op.targets.clone())
                    .unwrap_or_default()
            } else if request.command == "shards" {
                self.config
                    .bots
                    .iter()
                    .filter(|b| target.as_ref().is_none_or(|bot| bot == &b.bot_id))
                    .flat_map(|bot| {
                        bot.shard_placement.iter().map(|s| ShardKey {
                            bot_id: bot.bot_id.clone(),
                            shard_id: s.shard_id,
                        })
                    })
                    .collect()
            } else {
                self.daemon
                    .snapshots()
                    .into_iter()
                    .filter(|s| target.as_ref().is_none_or(|bot| bot == &s.key.bot_id))
                    .map(|s| s.key)
                    .collect()
            };
            // Authorize exactly the Shards the command will touch.
            let keys: Vec<_> = match (read, request.shard_filter()) {
                (false, Some(wanted)) => keys
                    .into_iter()
                    .filter(|key| wanted.contains(&key.shard_id))
                    .collect(),
                _ => keys,
            };
            if !access.allows_command(read, target.as_deref(), &keys) {
                return Self::error("control permission or scope denied");
            }
            if request.command == "gateway-update" && request.valid() {
                if let Some(response) = self.replay_retry_response() {
                    return response;
                }
                let id = request.operation_id.clone().unwrap_or_default();
                let bot = target.as_deref();
                let actor = (access.subject.clone(), access.role.clone());
                if self
                    .audit_gateway_update("requested", &id, bot, &actor, None)
                    .is_err()
                {
                    return Self::error("audit unavailable; gateway-update refused");
                }
                let response = self.request(request);
                if response["ok"] == true {
                    self.gateway_update_actors.insert(id, actor);
                } else {
                    let _ = self.audit_gateway_update("rejected", &id, bot, &actor, None);
                }
                return response;
            }
            self.request(request)
        }

        #[cfg(unix)]
        pub fn dispatch_gateway_update(
            &mut self,
            request: CommandRequest,
            access: &super::update_control::ControlAccess,
            external_id_conflict: bool,
            response: tokio::sync::oneshot::Sender<Value>,
            recovery_audit: Value,
        ) {
            let reply = ControlReply {
                response,
                recovery_audit,
            };
            if request.command != "gateway-update" || !request.valid() {
                reply.send(Self::error("invalid control request"));
                return;
            }
            let bot = request
                .bot
                .as_deref()
                .and_then(|selector| botref::resolve(&self.config.bots, selector).ok())
                .map(|bot| bot.bot_id.clone());
            let filter = request.shard_filter();
            let keys: Vec<_> = self
                .daemon
                .entries
                .keys()
                .filter(|key| {
                    bot.as_ref().is_none_or(|bot| *bot == key.bot_id)
                        && filter
                            .as_ref()
                            .is_none_or(|wanted| wanted.contains(&key.shard_id))
                })
                .cloned()
                .collect();
            if !access.allows_command(false, bot.as_deref(), &keys) {
                reply.send(Self::error("control permission or scope denied"));
                return;
            }
            if let Some(response) = self.replay_retry_response() {
                reply.send(response);
                return;
            }
            let actor = (access.subject.clone(), access.role.clone());
            let id = request.operation_id.as_deref().expect("validated ID");
            if external_id_conflict || self.operation_id_reserved(id) {
                self.audit_gateway_rejection(id, bot.as_deref(), &actor);
                reply.send(Self::error(
                    "operation ID conflicts with an earlier request",
                ));
                return;
            }
            if bot.is_none() || (!self.operations.contains_key(id) && self.core_restart_busy(&keys))
            {
                self.audit_gateway_rejection(id, bot.as_deref(), &actor);
                let response = if bot.is_none() {
                    self.request(&request)
                } else {
                    Self::error("busy")
                };
                reply.send(response);
                return;
            }
            let Some(worker) = self
                .gateway_audit_worker
                .as_ref()
                .filter(|_| self.pending_gateway_updates.len() < AUDIT_QUEUE_LIMIT)
            else {
                reply.send(Self::error("audit unavailable; gateway-update refused"));
                return;
            };
            let Ok(record) = Self::audit_record("requested", id, bot.as_deref(), &actor, None)
            else {
                reply.send(Self::error("audit unavailable; gateway-update refused"));
                return;
            };
            let Ok(Some(audit)) = Self::submit_audit(worker, record, true) else {
                reply.send(Self::error("audit unavailable; gateway-update refused"));
                return;
            };
            self.pending_gateway_updates.push(PendingGatewayUpdate {
                request,
                bot,
                actor,
                audit,
                reply,
                deadline: Instant::now() + AUDIT_ACK_TIMEOUT,
            });
        }

        #[cfg(unix)]
        fn audit_gateway_rejection(&self, id: &str, bot: Option<&str>, actor: &(String, String)) {
            let submitted = Self::audit_record("rejected", id, bot, actor, None)
                .ok()
                .zip(self.gateway_audit_worker.as_ref())
                .is_some_and(|(record, worker)| Self::submit_audit(worker, record, false).is_ok());
            if !submitted {
                self.gateway_audit_failures
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
        }

        #[cfg(unix)]
        fn settle_gateway_audits(&mut self, now: Instant) {
            use tokio::sync::oneshot::error::TryRecvError;
            for mut pending in std::mem::take(&mut self.pending_gateway_updates) {
                let id = pending
                    .request
                    .operation_id
                    .as_deref()
                    .expect("validated ID");
                let response = match pending.audit.try_recv() {
                    Err(TryRecvError::Empty) if now < pending.deadline => {
                        self.pending_gateway_updates.push(pending);
                        continue;
                    }
                    Ok((_, true)) if now < pending.deadline => {
                        let response = self.request(&pending.request);
                        if response["ok"] == true {
                            self.gateway_update_actors
                                .insert(id.to_owned(), pending.actor);
                        } else {
                            self.audit_gateway_rejection(
                                id,
                                pending.bot.as_deref(),
                                &pending.actor,
                            );
                        }
                        response
                    }
                    _ => {
                        self.audit_gateway_rejection(id, pending.bot.as_deref(), &pending.actor);
                        Self::error("audit unavailable; gateway-update refused")
                    }
                };
                pending.reply.send(response);
            }
        }

        #[cfg(unix)]
        pub fn dispatch_core_restart(
            &mut self,
            request: &CommandRequest,
            access: &super::update_control::ControlAccess,
            external_id_conflict: bool,
            response: tokio::sync::oneshot::Sender<Value>,
            recovery_audit: Value,
        ) {
            let reply = ControlReply {
                response,
                recovery_audit,
            };
            // すぐ返せる拒否はここで応答する。追記中の要求の応答は操作が保持する。
            let _ = self.request_core_restart(
                request,
                access,
                Instant::now(),
                external_id_conflict,
                Some(reply),
            );
        }

        #[cfg(unix)]
        fn request_core_restart(
            &mut self,
            request: &CommandRequest,
            access: &super::update_control::ControlAccess,
            now: Instant,
            external_id_conflict: bool,
            mut reply: Option<ControlReply>,
        ) -> Option<Value> {
            if request.command != "core-restart" || !request.valid() {
                return ControlReply::respond(reply, Self::error("invalid control request"));
            }
            if !access.valid() {
                return ControlReply::respond(reply, Self::error("permission_denied"));
            }
            let bot = request
                .bot
                .as_deref()
                .and_then(|selector| botref::resolve(&self.config.bots, selector).ok());
            let key = ShardKey {
                bot_id: bot.map_or_else(String::new, |bot| bot.bot_id.clone()),
                shard_id: request.shard.expect("validated shard"),
            };
            let core = request.core.expect("validated core");
            let mut recovery = CoreRestart {
                target: CoreRestartTarget {
                    core,
                    total_shards: request.expect_total_shards.expect("validated total_shards"),
                    expected: request.incarnation(),
                    expect_empty: request.expect_empty,
                },
                incarnation: request.incarnation(),
                actor: (access.subject.clone(), access.role.clone()),
                reason: None,
                pending: None,
                charge_clock: None,
                charged: false,
            };
            let id = request.operation_id.as_deref().expect("validated ID");
            let force = request.force;
            let reject = |plane: &mut Self,
                          reply,
                          recovery: &CoreRestart,
                          reason,
                          retry: Option<Duration>| {
                plane.audit_core_rejection(id, &key, force, recovery, reason);
                let mut value = Self::error(reason);
                value["rejected"] = json!(true);
                if let Some(retry) = retry {
                    value["retry_after_ms"] = json!(retry.as_millis().max(1));
                }
                ControlReply::respond(reply, value)
            };
            let local_keys = self
                .daemon
                .entries
                .contains_key(&key)
                .then(|| std::slice::from_ref(&key));
            if !access.allows_command(
                false,
                bot.map(|bot| bot.bot_id.as_str()),
                local_keys.unwrap_or_default(),
            ) {
                return reject(self, reply, &recovery, "permission_denied", None);
            }
            if let Some(response) = self.replay_retry_response() {
                return ControlReply::respond(reply, response);
            }
            if external_id_conflict {
                return reject(self, reply, &recovery, "operation_id_conflict", None);
            }
            let Some(bot) = bot else {
                return reject(self, reply, &recovery, "bot_not_found", None);
            };
            if request.expect_bot_id.as_deref() != Some(key.bot_id.as_str()) {
                return reject(self, reply, &recovery, "bot_id_mismatch", None);
            }
            if bot.total_shards != crate::placement::ShardCount::Fixed(recovery.target.total_shards)
            {
                return reject(self, reply, &recovery, "total_shards_mismatch", None);
            }
            if let Some(old) = self.operations.get(id) {
                if old.bot_id.as_deref() == Some(key.bot_id.as_str())
                    && old.shards == request.shard_filter()
                    && old.force == force
                    && old.core_restart.as_ref().is_some_and(|previous| {
                        previous.target == recovery.target && previous.actor == recovery.actor
                    })
                {
                    let value = self.operation_view(old);
                    return ControlReply::respond(reply, value);
                }
                return reject(self, reply, &recovery, "operation_id_conflict", None);
            }
            if let Some(old) = self.archived_core_restarts.get(id) {
                let row = &old.requested;
                let expected = recovery.target.expected.map(|id| format!("{id:016x}"));
                if row.key() == key
                    && row.core == core
                    && row.total_shards == Some(recovery.target.total_shards)
                    && row.incarnation == expected
                    && row.expect_empty == recovery.target.expect_empty
                    && row.force == force
                    && row.subject == access.subject
                    && row.role == access.role
                {
                    return ControlReply::respond(reply, Self::archived_core_view(old));
                }
                return reject(self, reply, &recovery, "operation_id_conflict", None);
            }
            if self.operation_id_reserved(id) {
                return reject(self, reply, &recovery, "operation_id_conflict", None);
            }
            if !self.core_audit_ready {
                return reject(self, reply, &recovery, "audit_unavailable", None);
            }
            let Some(entry) = self.daemon.entries.get(&key) else {
                return reject(self, reply, &recovery, "shard_not_local", None);
            };
            if entry.candidate.is_some()
                || entry.retiring.is_some()
                || entry.update_fallback_reserved
                || entry.stop_since.is_some()
                || self
                    .operations
                    .values()
                    .any(|op| op.finished.is_none() && op.targets.contains(&key))
            {
                return reject(self, reply, &recovery, "busy", None);
            }
            if self.operations.len() >= OPERATION_LIMIT {
                return reject(self, reply, &recovery, "operation_history_full", None);
            }
            recovery.incarnation = match self.check_core_restart(&key, recovery.target, force, now)
            {
                Ok(incarnation) => incarnation,
                Err((reason, retry)) => {
                    return reject(self, reply, &recovery, reason, retry);
                }
            };
            let Ok(mut record) =
                Self::core_restart_record("requested", id, &key, force, &recovery, None)
            else {
                return reject(self, reply, &recovery, "audit_unavailable", None);
            };
            self.daemon
                .entries
                .get_mut(&key)
                .unwrap()
                .cores
                .as_mut()
                .unwrap()
                .reserve_manual_restart(core as usize);
            if let Some(worker) = self.core_audit_worker.as_ref() {
                if reply.is_none() {
                    self.release_core_restart(&key, core);
                    return reject(self, reply, &recovery, "audit_unavailable", None);
                }
                let Ok(Some(audit)) = Self::submit_audit(worker, record, true) else {
                    self.release_core_restart(&key, core);
                    return reject(self, reply, &recovery, "audit_unavailable", None);
                };
                recovery.pending = Some(PendingCoreRestart {
                    audit,
                    reply: reply.take(),
                });
            } else {
                if self.write_audit(&mut record).is_err() {
                    self.release_core_restart(&key, core);
                    if record.written {
                        self.audit_core_rejection(id, &key, force, &recovery, "audit_unavailable");
                        return ControlReply::respond(reply, Self::error("audit_unavailable"));
                    }
                    return reject(self, reply, &recovery, "audit_unavailable", None);
                }
                if let Err(reason) = self.begin_core_restart(&key, &mut recovery, force, now) {
                    self.release_core_restart(&key, core);
                    self.audit_core_rejection(id, &key, force, &recovery, reason);
                    return ControlReply::respond(reply, Self::error(reason));
                }
            }
            let mut op = Operation::new(
                request,
                Some(key.bot_id.clone()),
                vec![key],
                HashSet::new(),
                Phase::CoreRestart,
                now,
            );
            let pending = recovery.pending.is_some();
            op.core_restart = Some(recovery);
            let response = (!pending).then(|| self.operation_view(&op));
            self.operations.insert(id.to_owned(), op);
            response.and_then(|value| ControlReply::respond(reply, value))
        }

        #[cfg(unix)]
        fn begin_core_restart(
            &mut self,
            key: &ShardKey,
            recovery: &mut CoreRestart,
            force: bool,
            now: Instant,
        ) -> Result<(), &'static str> {
            let checked = self.check_core_restart(key, recovery.target, force, now);
            let core = recovery.target.core;
            let result = checked
                .map_err(|(reason, _)| reason)
                .and_then(|incarnation| {
                    if incarnation != recovery.incarnation {
                        return Err("incarnation_mismatch");
                    }
                    if !self.gateway_eligible_for_core_restart(key) {
                        return Err("shard_not_running");
                    }
                    let started = self
                        .daemon
                        .entries
                        .get_mut(key)
                        .unwrap()
                        .cores
                        .as_mut()
                        .unwrap()
                        .begin_manual_restart(core as usize, incarnation, now, force)
                        .map_err(|_| "core_status_unavailable")?;
                    started.then_some(()).ok_or("incarnation_mismatch")
                });
            // A synced request consumes one attempt. Measure it at the final check/EOF,
            // since the write-ahead acknowledgement can be delayed.
            self.charge_core_attempt(key, recovery, now);
            result
        }

        #[cfg(unix)]
        fn charge_core_attempt(
            &mut self,
            key: &ShardKey,
            recovery: &mut CoreRestart,
            now: Instant,
        ) {
            if recovery.charged {
                let history = self
                    .core_restart_history
                    .get_mut(&(key.clone(), recovery.target.core))
                    .unwrap();
                history.pop_back();
            }
            self.charge_core_restart(key, recovery.target.core, now);
            recovery.charged = true;
            recovery.charge_clock = self
                .core_audit_boot_id
                .as_ref()
                .zip(crate::core_status::try_boot_ms())
                .map(|(boot_id, boot_ms)| CoreAuditClock {
                    boot_id: boot_id.clone(),
                    boot_ms,
                });
        }

        #[cfg(unix)]
        fn gateway_eligible_for_core_restart(&mut self, key: &ShardKey) -> bool {
            let entry = &self.daemon.entries[key];
            let recovery_key = entry.recovery_key;
            entry.snapshot.desired_running
                && entry.snapshot.state == ProcessState::Running
                && entry.stop_since.is_none()
                && entry.candidate.is_none()
                && entry.retiring.is_none()
                && !entry.update_fallback_reserved
                && self
                    .daemon
                    .recovery
                    .snapshot(recovery_key)
                    .is_ok_and(|snapshot| {
                        snapshot.state == RecoveryState::Running && !snapshot.held
                    })
                && self
                    .daemon
                    .recovery
                    .child_mut(recovery_key)
                    .is_ok_and(|child| {
                        child.is_some_and(|child| matches!(child.try_wait(), Ok(None)))
                    })
        }

        #[cfg(unix)]
        fn release_core_restart(&mut self, key: &ShardKey, core: u32) {
            self.daemon
                .entries
                .get_mut(key)
                .unwrap()
                .cores
                .as_mut()
                .unwrap()
                .release_manual_restart(core as usize);
        }

        #[cfg(unix)]
        fn charge_core_restart(&mut self, key: &ShardKey, core: u32, now: Instant) {
            let history = self
                .core_restart_history
                .entry((key.clone(), core))
                .or_default();
            history.retain(|at| now.saturating_duration_since(*at) < CORE_RESTART_WINDOW);
            history.push_back(now);
        }

        #[cfg(unix)]
        fn audit_core_rejection(
            &mut self,
            id: &str,
            key: &ShardKey,
            force: bool,
            recovery: &CoreRestart,
            reason: &'static str,
        ) {
            let written =
                Self::core_restart_record("rejected", id, key, force, recovery, Some(reason))
                    .is_ok_and(|mut record| {
                        if let Some(worker) = self.core_audit_worker.as_ref() {
                            // The queue can hold 64 jobs while the worker writes one.
                            if self.core_audit_rejections.len() > AUDIT_QUEUE_LIMIT {
                                return false;
                            }
                            let Ok(Some(result)) = Self::submit_audit(worker, record, true) else {
                                return false;
                            };
                            self.core_audit_rejections.push(result);
                            true
                        } else {
                            self.write_audit(&mut record).is_ok()
                        }
                    });
            if !written {
                self.core_audit_failures
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
        }

        #[cfg(unix)]
        fn check_core_restart(
            &mut self,
            key: &ShardKey,
            target: CoreRestartTarget,
            force: bool,
            now: Instant,
        ) -> Result<Option<u64>, (&'static str, Option<Duration>)> {
            let gateway_eligible = self.gateway_eligible_for_core_restart(key);
            let entry = self.daemon.entries.get_mut(key).expect("validated shard");
            let cores = entry
                .cores
                .as_mut()
                .ok_or(("separate_cores_required", None))?;
            if !gateway_eligible || !cores.prepared() {
                return Err(("shard_not_running", None));
            }
            let index = target.core as usize;
            if index >= crate::core_attach::SLOTS {
                return Err(("core_out_of_range", None));
            }
            let core = cores
                .manual_restart_status(index)
                .map_err(|_| ("core_status_unavailable", None))?;
            if !target.expect_empty && target.expected.is_none() {
                return Err(("incarnation_required", None));
            }
            if target.expected != core.status_id || target.expect_empty != core.pid.is_none() {
                return Err(("incarnation_mismatch", None));
            }
            if !force && core.running {
                // Process availability is a separate requirement from the reason to restart.
                if !cores.peer_available(index) {
                    return Err(("last_running_core", None));
                }
                if self.core_view(key, index, &core)["status"]["suspected_stalled"] != true {
                    return Err(("core_not_suspected_stalled", None));
                }
            }
            if let Some(history) = self.core_restart_history.get(&(key.clone(), target.core)) {
                let mut in_window = history.iter().filter_map(|at| {
                    CORE_RESTART_WINDOW
                        .checked_sub(now.saturating_duration_since(*at))
                        .filter(|remaining| !remaining.is_zero())
                });
                let window = in_window
                    .next()
                    .filter(|_| in_window.count() >= CORE_RESTART_LIMIT - 1);
                let interval = history.back().and_then(|at| {
                    CORE_RESTART_INTERVAL.checked_sub(now.saturating_duration_since(*at))
                });
                if let Some(remaining) = window
                    .max(interval)
                    .filter(|remaining| !remaining.is_zero())
                {
                    return Err(("rate_limited", Some(remaining)));
                }
            }
            Ok(core.status_id)
        }

        #[cfg(unix)]
        fn core_restart_record(
            event: &'static str,
            id: &str,
            key: &ShardKey,
            force: bool,
            recovery: &CoreRestart,
            reason: Option<&'static str>,
        ) -> std::io::Result<PendingAudit> {
            let terminal = matches!(event, "succeeded" | "failed");
            let mut row = json!({
                "ts": Self::audit_timestamp(), "event": event,
                "subject": recovery.actor.0, "role": recovery.actor.1,
                "bot_id": key.bot_id.parse::<u64>().ok().map(|id| id.to_string()),
                "shard": key.shard_id, "core": recovery.target.core,
                "total_shards": recovery.target.total_shards,
                "expect_empty": recovery.target.expect_empty,
                "operation_id": id, "force": force,
                "incarnation": recovery.incarnation.map(|id| format!("{id:016x}")),
                "reason": reason,
            });
            if terminal && let Some(clock) = &recovery.charge_clock {
                row["charge_clock"] = json!(clock);
            }
            let mut record = PendingAudit::new(AuditKind::CoreRecovery, row)?;
            if terminal {
                record.core_terminal_id = Some(id.to_owned());
            }
            Ok(record)
        }

        pub fn new(daemon: Daemon, config: Config, monitor_ok: bool) -> Self {
            Self {
                daemon,
                config,
                operations: BTreeMap::new(),
                monitor_ok,
                last_advanced: String::new(),
                gateway_update_audit: None,
                gateway_update_actors: BTreeMap::new(),
                pending_audit: Vec::new(),
                audit_retry_at: None,
                audit_tail: std::sync::Mutex::new(None),
                #[cfg(all(test, unix))]
                core_recovery_audit: None,
                #[cfg(all(test, unix))]
                core_audit_tail: std::sync::Mutex::new(None),
                #[cfg(unix)]
                core_audit_worker: None,
                #[cfg(unix)]
                core_audit_replay: None,
                #[cfg(unix)]
                core_audit_ready: false,
                #[cfg(unix)]
                core_audit_boot_id: None,
                #[cfg(unix)]
                core_audit_failures: Default::default(),
                #[cfg(unix)]
                core_audit_rejections: Vec::new(),
                #[cfg(unix)]
                gateway_audit_worker: None,
                #[cfg(unix)]
                gateway_audit_failures: Default::default(),
                #[cfg(unix)]
                pending_gateway_updates: Vec::new(),
                #[cfg(unix)]
                audit_worker_exits: Vec::new(),
                #[cfg(unix)]
                audit_writes: Vec::new(),
                #[cfg(unix)]
                core_audit_synced: HashSet::new(),
                #[cfg(unix)]
                archived_core_restarts: BTreeMap::new(),
                #[cfg(unix)]
                reserved_core_ids: HashSet::new(),
                #[cfg(unix)]
                core_restart_history: BTreeMap::new(),
                #[cfg(unix)]
                core_status_history: std::sync::Mutex::new(BTreeMap::new()),
            }
        }

        fn error(error: &str) -> Value {
            json!({"ok": false, "offline": false, "error": error, "note": NOTE})
        }

        /// Process rows for status output. In the separated layout each Shard row also
        /// carries `cores` (running and failures only); embedded layout keeps the rows unchanged.
        fn process_views(&self, processes: &[ProcessSnapshot]) -> Vec<Value> {
            processes
                .iter()
                .map(|process| {
                    let mut view = json!(process);
                    if let (Some(cores), Value::Object(map)) =
                        (self.cores_view(&process.key), &mut view)
                    {
                        map.insert("cores".into(), cores);
                    }
                    view
                })
                .collect()
        }

        #[cfg(unix)]
        fn cores_view(&self, key: &ShardKey) -> Option<Value> {
            self.daemon.core_status(key).map(|cores| {
                Value::Array(
                    cores
                        .iter()
                        .enumerate()
                        .map(|(index, core)| self.core_view(key, index, core))
                        .collect(),
                )
            })
        }

        #[cfg(unix)]
        fn core_view(
            &self,
            key: &ShardKey,
            index: usize,
            core: &crate::core_units::CoreStatus,
        ) -> Value {
            let read = core
                .status_path
                .as_ref()
                .map(|path| crate::core_status::read_status(path))
                .unwrap_or(crate::core_status::StatusRead::Missing);
            let mut history = self
                .core_status_history
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            core_status_view(core, read, history.entry((key.clone(), index)).or_default())
        }

        #[cfg(not(unix))]
        fn cores_view(&self, _key: &ShardKey) -> Option<Value> {
            None
        }

        fn bot_view(&self, bot: &BotPlacement) -> Value {
            let processes: Vec<_> = self
                .daemon
                .snapshots()
                .into_iter()
                .filter(|s| s.key.bot_id == bot.bot_id)
                .collect();
            let state = if processes.is_empty() {
                "not_local"
            } else if processes.iter().any(|s| s.restart_exhausted) {
                "degraded"
            } else if processes
                .iter()
                .any(|s| matches!(s.state, ProcessState::Failed { .. }))
            {
                "failed"
            } else if processes
                .iter()
                .all(|s| matches!(s.state, ProcessState::Running))
            {
                "running"
            } else if processes
                .iter()
                .all(|s| matches!(s.state, ProcessState::Exited { .. }))
            {
                "stopped"
            } else {
                "mixed"
            };
            json!({"name": bot.name, "bot_id": bot.bot_id, "total_shards": bot.total_shards,
                "status": state, "processes": self.process_views(&processes)})
        }

        fn snapshot(&self, op: &Operation) -> OperationSnapshot {
            if let Some(snapshot) = &op.finished {
                return snapshot.clone();
            }
            let shards = self
                .daemon
                .snapshots()
                .into_iter()
                .filter(|s| op.targets.contains(&s.key))
                .map(|process| OperationShard {
                    error: op.errors.get(&process.key).copied(),
                    process,
                })
                .collect();
            OperationSnapshot {
                operation_id: op.id.clone(),
                command: op.command.clone(),
                bot_id: op.bot_id.clone(),
                state: OperationState::Running,
                elapsed_ms: op.started.elapsed().as_millis(),
                shards,
                failed_shards: op.errors.keys().cloned().collect(),
            }
        }

        fn operation_view(&self, op: &Operation) -> Value {
            let snapshot = self.snapshot(op);
            let mut value = json!({"ok": snapshot.state != OperationState::Failed, "offline": false, "operation": snapshot, "note": if op.command == "core-restart" { CORE_RESTART_NOTE } else { NOTE }});
            #[cfg(unix)]
            if op.finished.is_none() && op.update_stage == UpdateStage::WaitingIdentify {
                value["operation"]["waiting_reason"] = json!("IdentifyBudget");
            }
            #[cfg(unix)]
            if let Some(recovery) = &op.core_restart {
                if recovery
                    .pending
                    .as_ref()
                    .is_some_and(|pending| pending.reply.is_some())
                {
                    return Self::error("audit_pending");
                }
                value["operation"]["core_recovery_version"] = json!(CORE_RECOVERY_VERSION);
                value["operation"]["source"] = json!("live");
                value["operation"]["core"] = json!(recovery.target.core);
                value["operation"]["shard"] = json!(op.targets[0].shard_id);
                value["operation"]["incarnation"] =
                    json!(recovery.incarnation.map(|id| format!("{id:016x}")));
                value["operation"]["reason"] = json!(recovery.reason);
                if op.finished.is_some() {
                    value["operation"]["terminal_audit"] =
                        json!(if self.core_audit_synced.contains(&op.id) {
                            "synced"
                        } else {
                            "pending_or_failed"
                        });
                }
                return value;
            }
            value
        }

        fn finish(&mut self, op: &mut Operation) {
            let mut snapshot = self.snapshot(op);
            snapshot.state = if op.errors.is_empty() {
                OperationState::Succeeded
            } else {
                OperationState::Failed
            };
            let failed = snapshot.state == OperationState::Failed;
            let first_error = op.errors.values().next().copied();
            op.finished = Some(snapshot);
            if op.is_restart() {
                self.daemon.release_restart_identify(&op.targets);
            }
            if op.command != "core-restart" {
                for key in &op.targets {
                    if self.daemon.entries[key].journal_start_pending {
                        let _ = self.daemon.stop(key, StopMode::WaitForExit);
                    }
                    self.daemon.hold_restart(key, false);
                }
            }
            if op.command == "gateway-update" && failed {
                // A standby that was never activated must not outlive its operation.
                #[cfg(unix)]
                for key in &op.targets {
                    self.daemon.cancel_gateway_update(key);
                }
            }
            if op.command == "gateway-update"
                && let Some(actor) = self.gateway_update_actors.get(&op.id)
            {
                let event = if failed { "failed" } else { "succeeded" };
                // The request was recorded before it started; a failure to record the
                // outcome cannot undo it, so it is only reported.
                if let Ok(record) =
                    Self::audit_record(event, &op.id, op.bot_id.as_deref(), actor, first_error)
                {
                    self.write_terminal_audit(record);
                }
            }
            #[cfg(unix)]
            if op
                .core_restart
                .as_ref()
                .is_some_and(|recovery| recovery.pending.is_none())
            {
                self.record_core_terminal(op);
            }
        }

        #[cfg(unix)]
        fn record_core_terminal(&mut self, op: &Operation) {
            let recovery = op.core_restart.as_ref().expect("Core restart operation");
            let event = if op.errors.is_empty() {
                "succeeded"
            } else {
                "failed"
            };
            if let Ok(record) = Self::core_restart_record(
                event,
                &op.id,
                &op.targets[0],
                op.force,
                recovery,
                recovery.reason,
            ) {
                self.write_terminal_audit(record);
            }
        }

        /// No mutation is performed on a malformed request, unknown/ambiguous
        /// selector, conflicting ID, full journal or unconfirmed force request.
        pub fn request(&mut self, request: &CommandRequest) -> Value {
            if !request.valid() {
                return Self::error("invalid control request");
            }
            if matches!(request.command.as_str(), "core-restart" | "core-preview") {
                return Self::error("permission_denied");
            }
            let separate_update =
                request.command == "gateway-update" && self.daemon.separate_cores();
            if !separate_update
                && matches!(
                    request.command.as_str(),
                    "gateway-prepare" | "gateway-update"
                )
            {
                return Self::error("Gateway switch requires the explicit mock runtime");
            }
            if request.command == "gateway-prepare" {
                return Self::error("Gateway switch requires the explicit mock runtime");
            }
            if request.command == "operation" {
                return self
                    .operations
                    .get(request.operation_id.as_ref().expect("validated ID"))
                    .map(|op| self.operation_view(op))
                    .unwrap_or_else(|| {
                        Self::error("operation not found in this daemon incarnation")
                    });
            }
            let selected = match request.bot.as_deref() {
                Some(selector) => match botref::resolve(&self.config.bots, selector) {
                    Ok(bot) => Some(bot.clone()),
                    Err(botref::BotRefError::NotFound) => return Self::error("bot not found"),
                    Err(botref::BotRefError::Ambiguous { .. }) => {
                        return Self::error("ambiguous bot reference");
                    }
                    Err(_) => return Self::error("invalid bot configuration"),
                },
                None => None,
            };
            if let Some(expected) = request.expect_bot_id.as_ref()
                && selected.as_ref().is_none_or(|bot| &bot.bot_id != expected)
            {
                return Self::error("bot identity changed; refresh the bot list before retrying");
            }
            if let Some(expected) = request.expect_total_shards
                && selected.as_ref().is_none_or(|bot| {
                    bot.total_shards != crate::placement::ShardCount::Fixed(expected)
                })
            {
                return Self::error(
                    "bot total_shards changed; resolve guilds again before retrying",
                );
            }
            match request.command.as_str() {
                "ps" | "status" | "inspect" => {
                    let processes: Vec<_> = self
                        .daemon
                        .snapshots()
                        .into_iter()
                        .filter(|s| selected.as_ref().is_none_or(|b| b.bot_id == s.key.bot_id))
                        .collect();
                    let mut value = json!({"ok": true, "offline": false, "note": NOTE,
                        "status": {"processes": self.process_views(&processes), "monitor_ok": self.monitor_ok}});
                    for (name, kind) in [
                        ("gateway_update_audit", AuditKind::GatewayUpdate),
                        ("core_recovery_audit", AuditKind::CoreRecovery),
                    ] {
                        let pending = self
                            .pending_audit
                            .iter()
                            .filter(|record| record.kind == kind)
                            .count();
                        #[cfg(unix)]
                        {
                            let pending = pending
                                + self
                                    .audit_writes
                                    .iter()
                                    .filter(|(other, _)| *other == kind)
                                    .count();
                            let (available, failures, outstanding) = match kind {
                                AuditKind::CoreRecovery => (
                                    self.core_audit_ready
                                        && self
                                            .core_audit_worker
                                            .as_ref()
                                            .is_some_and(|worker| !worker.is_closed()),
                                    &self.core_audit_failures,
                                    self.operations
                                        .values()
                                        .filter(|op| {
                                            op.core_restart
                                                .as_ref()
                                                .is_some_and(|recovery| recovery.pending.is_some())
                                        })
                                        .count()
                                        + self.core_audit_rejections.len(),
                                ),
                                AuditKind::GatewayUpdate => (
                                    self.gateway_audit_worker
                                        .as_ref()
                                        .is_some_and(|worker| !worker.is_closed())
                                        || self.gateway_update_audit.is_some(),
                                    &self.gateway_audit_failures,
                                    self.pending_gateway_updates.len(),
                                ),
                            };
                            let pending = pending + outstanding;
                            let failures = failures.load(std::sync::atomic::Ordering::Relaxed);
                            value["status"][name] = json!({
                                "ok": available && failures == 0 && pending == 0,
                                "available": available, "pending": pending, "failed_or_dropped": failures,
                            });
                        }
                        #[cfg(not(unix))]
                        {
                            value["status"][name] = json!({"ok": pending == 0, "pending": pending});
                        }
                    }
                    // Advisory observations only; no labels beyond Bot/shard/generation.
                    value["status"]["metrics"] = self
                        .daemon
                        .metrics()
                        .status(selected.as_ref().map(|b| b.bot_id.as_str()));
                    if selected.is_none() {
                        let text = self.daemon.metrics().render_prometheus();
                        // Keep the control response bounded for very large layouts.
                        if text.len() <= 24 * 1024 {
                            value["status"]["metrics_prometheus"] = json!(text);
                        } else {
                            value["status"]["metrics_prometheus_omitted"] = json!(true);
                        }
                    }
                    if let Some(bot) = selected {
                        value["bot"] = self.bot_view(&bot);
                    } else {
                        value["bots"] = self.config.bots.iter().map(|b| self.bot_view(b)).collect();
                    }
                    value
                }
                "shards" => {
                    let bot = selected.expect("validated selector");
                    let snapshots = self.daemon.snapshots();
                    let shards: Vec<_> = bot.shard_placement.iter().map(|p| {
                        let process = snapshots.iter().find(|s| s.key.bot_id == bot.bot_id && s.key.shard_id == p.shard_id);
                        json!({"shard_id": p.shard_id, "total_shards": bot.total_shards, "primary": p.primary,
                            "local": p.primary == self.config.node.id, "process": process})
                    }).collect();
                    json!({"ok": true, "offline": false, "note": NOTE, "bot_id": bot.bot_id, "name": bot.name, "shards": shards})
                }
                _ => self.admit(request, selected.map(|bot| bot.bot_id)),
            }
        }

        fn core_restart_busy(&self, targets: &[ShardKey]) -> bool {
            self.operations.values().any(|op| {
                op.command == "core-restart"
                    && op.finished.is_none()
                    && op.targets.iter().any(|key| targets.contains(key))
            })
        }

        fn admit(&mut self, request: &CommandRequest, bot_id: Option<String>) -> Value {
            if let Some(response) = self.replay_retry_response() {
                return response;
            }
            if request.force && !request.yes {
                return Self::error("force stop requires --yes");
            }
            let id = request.operation_id.as_ref().expect("validated ID");
            if let Some(old) = self.operations.get(id) {
                if old.command != request.command
                    || old.bot_id != bot_id
                    || old.force != request.force
                    || old.shards != request.shard_filter()
                {
                    return Self::error("operation ID conflicts with an earlier request");
                }
                return self.operation_view(old);
            }
            if self.operation_id_reserved(id) {
                return Self::error("operation ID conflicts with an earlier request");
            }
            if self.operations.len() >= OPERATION_LIMIT {
                return Self::error("operation history is full");
            }
            let mut targets: Vec<_> = self
                .daemon
                .entries
                .keys()
                .filter(|key| bot_id.as_ref().is_none_or(|bot| key.bot_id == *bot))
                .cloned()
                .collect();
            if targets.is_empty() {
                return Self::error(if bot_id.is_some() {
                    "bot has no local shards"
                } else {
                    "no bot has local shards"
                });
            }
            if let Some(wanted) = request.shard_filter() {
                // A Shard that is not local is an error, never silently skipped: the operator
                // asked for a specific Shard and must learn that nothing happened to it.
                if wanted
                    .iter()
                    .any(|id| !targets.iter().any(|key| key.shard_id == *id))
                {
                    return Self::error("requested shard is not a local shard of the bot");
                }
                targets.retain(|key| wanted.contains(&key.shard_id));
            }
            if targets.len() > OPERATION_SHARD_LIMIT {
                return Self::error("operation exceeds local shard limit (64)");
            }
            if self.core_restart_busy(&targets) {
                return Self::error("busy");
            }
            let target_bots: HashSet<_> = targets.iter().map(|key| &key.bot_id).collect();
            let active: Vec<_> = self
                .operations
                .iter()
                .filter(|(_, op)| {
                    op.finished.is_none()
                        && op.command != "core-restart"
                        && op
                            .targets
                            .iter()
                            .any(|key| target_bots.contains(&key.bot_id))
                })
                .map(|(id, _)| id.clone())
                .collect();
            if !active.is_empty() && !request.force {
                return Self::error("bot already has an operation in progress");
            }
            // Superseding an operation also changes its other shards' restart holds and
            // candidate lifetimes. A force stop must authorize that entire effect.
            // Refuse before finishing anything or installing new holds.
            if active.iter().any(|id| {
                self.operations[id]
                    .targets
                    .iter()
                    .any(|key| !targets.contains(key))
            }) {
                return Self::error(if request.shards.is_some() {
                    "another operation is running on other shards of this bot"
                } else {
                    "restart-all is in progress"
                });
            }
            if matches!(request.command.as_str(), "start" | "gateway-update")
                && bot_id
                    .as_deref()
                    .is_some_and(|bot| self.daemon.gateway_update_fallback_pending(bot))
            {
                // A new operation must not hold the shard with reserved recovery.
                return Self::error("gateway fallback recovery pending; retry after recovery");
            }
            if matches!(request.command.as_str(), "restart" | "restart-all")
                && let Err(error) = self.daemon.check_restart_writer()
            {
                return Self::error(&format!("{error:?}"));
            }
            // Explicit force stop supersedes an earlier drain/restart. The old
            // operation can never resume and launch a new generation afterwards.
            for id in active {
                let mut old = self.operations.remove(&id).expect("active operation");
                for key in &old.targets {
                    old.errors
                        .insert(key.clone(), DaemonError::OperationSuperseded);
                }
                self.finish(&mut old);
                self.operations.insert(id, old);
            }
            if matches!(request.command.as_str(), "restart" | "restart-all")
                && let Err(error) = self.daemon.reserve_restart_identify(&targets)
            {
                return Self::error(&format!("{error:?}"));
            }
            let waiting = if request.command == "start" || request.command == "gateway-update" {
                HashSet::new()
            } else {
                // An abnormal exit between admission and the first tick must
                // not be mistaken for an already-stopped, successful drain.
                targets
                    .iter()
                    .filter(|key| self.daemon.has_child(key))
                    .cloned()
                    .collect()
            };
            let op = Operation::new(
                request,
                bot_id,
                targets,
                waiting,
                if request.command == "start" {
                    Phase::Starting
                } else if request.command == "gateway-update" {
                    Phase::Updating
                } else {
                    Phase::Stopping
                },
                Instant::now(),
            );
            for key in &op.targets {
                self.daemon.hold_restart(key, true);
            }
            let response = self.operation_view(&op);
            self.operations.insert(id.clone(), op);
            response
        }

        /// Opt-in daemon shutdown (`botd --stop-children-on-exit`): request a
        /// cooperative stop of every owned child, then reap until `deadline`.
        /// Never force-kills. Exited children release their reservation through
        /// the normal poll path. Returns the shards whose child is still alive.
        pub async fn shutdown_children(
            &mut self,
            credentials: &impl CredentialSource,
            deadline: Duration,
        ) -> Vec<ShardKey> {
            #[cfg(unix)]
            self.cancel_audit_admissions();
            self.retry_pending_audit();
            let keys: Vec<ShardKey> = self.daemon.snapshots().into_iter().map(|s| s.key).collect();
            #[cfg(unix)]
            self.daemon.abandon_gateway_updates();
            for key in &keys {
                // A failed request leaves the child running; it is reported below.
                let _ = self.daemon.graceful_stop(key).await;
            }
            let until = Instant::now() + deadline;
            loop {
                let _ = self.daemon.poll(credentials);
                let remaining: Vec<ShardKey> = keys
                    .iter()
                    .filter(|key| self.daemon.has_child(key))
                    .cloned()
                    .collect();
                if remaining.is_empty() || Instant::now() >= until {
                    // Cores outlive Gateways on purpose, so they are stopped last.
                    #[cfg(unix)]
                    self.daemon.stop_cores(Duration::from_secs(5)).await;
                    return remaining;
                }
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        }

        /// Observe actual exits before advancing operations. One command per
        /// Bot per tick bounds cooperative writes and keeps reads responsive.
        pub async fn tick(&mut self, credentials: &impl CredentialSource) {
            #[cfg(unix)]
            {
                self.poll_core_audit_replay();
                self.settle_gateway_audits(Instant::now());
            }
            self.retry_pending_audit();
            self.monitor_ok = self.daemon.poll(credentials).is_ok();
            let mut ids: Vec<_> = self
                .operations
                .iter()
                .filter(|(_, op)| {
                    #[cfg(unix)]
                    if op
                        .core_restart
                        .as_ref()
                        .is_some_and(|recovery| recovery.pending.is_some())
                    {
                        return true;
                    }
                    op.finished.is_none()
                })
                .map(|(id, _)| id.clone())
                .collect();
            // Limit cumulative slow pipe writes across Bots as well as the
            // individual write. Rotate the cursor so a busy Bot cannot starve
            // later IDs, status requests, supervision or daemon shutdown.
            let offset = ids.partition_point(|id| id <= &self.last_advanced);
            ids.rotate_left(offset);
            let tick_started = Instant::now();
            for id in ids.into_iter().take(16) {
                let mut op = self.operations.remove(&id).expect("operation exists");
                #[cfg(unix)]
                if !self.settle_core_restart_audit(&mut op, Instant::now()) {
                    self.last_advanced.clone_from(&id);
                    continue;
                }
                if op.finished.is_none() {
                    self.advance(&mut op, credentials).await;
                }
                self.last_advanced.clone_from(&id);
                self.operations.insert(id, op);
                if tick_started.elapsed() >= Duration::from_millis(10) {
                    break;
                }
            }
        }

        /// One step of a Gateway update: start the standby Gateway of the current Shard, wait
        /// for it to be logged in, then activate it; then the next Shard. The first failure
        /// ends the operation and leaves the running Gateways as they are.
        #[cfg(unix)]
        async fn advance_update(
            &mut self,
            op: &mut Operation,
            credentials: &impl CredentialSource,
        ) {
            use crate::daemon::UpdateProgress;
            let Some(key) = op.targets.get(op.next).cloned() else {
                self.finish(op);
                return;
            };
            // The whole operation has one deadline. A confirmation already asked for
            // (`Activating`) is allowed to conclude on its own shorter limit; nothing new
            // starts after the deadline, for this Shard or the next ones.
            if op.started.elapsed() >= op.update_timeout
                && op.update_stage != UpdateStage::Activating
            {
                self.daemon.cancel_gateway_update(&key);
                let error = if op.update_stage == UpdateStage::WaitingIdentify {
                    DaemonError::IdentifyDeferred
                } else {
                    DaemonError::StopTimedOut
                };
                op.errors.insert(key, error);
                self.finish(op);
                return;
            }
            match op.update_stage {
                UpdateStage::Begin | UpdateStage::WaitingIdentify => {
                    match self.daemon.begin_gateway_update(&key, credentials) {
                        Ok(true) => op.update_stage = UpdateStage::Warming,
                        Ok(false) => op.update_stage = UpdateStage::WaitingIdentify,
                        Err(error) => {
                            op.errors.insert(key, error);
                            self.finish(op);
                        }
                    }
                }
                UpdateStage::Warming => match self.daemon.gateway_update_progress(&key) {
                    UpdateProgress::Warming => {
                        if op.started.elapsed() >= op.update_timeout {
                            self.daemon.cancel_gateway_update(&key);
                            op.errors.insert(key, DaemonError::StopTimedOut);
                            self.finish(op);
                        }
                    }
                    UpdateProgress::Ready => {
                        match self.daemon.send_gateway_activation(&key).await {
                            Ok(()) => op.update_stage = UpdateStage::Activating,
                            Err(error) => {
                                op.errors.insert(key, error);
                                self.finish(op);
                            }
                        }
                    }
                    UpdateProgress::Failed(error) => {
                        op.errors.insert(key, error);
                        self.finish(op);
                    }
                },
                UpdateStage::Activating => match self.daemon.gateway_activation_progress(&key) {
                    UpdateProgress::Warming => {}
                    UpdateProgress::Ready => match self.daemon.commit_gateway_update(&key).await {
                        Ok(()) => {
                            self.daemon.hold_restart(&key, false);
                            op.next += 1;
                            op.update_stage = UpdateStage::Begin;
                            if op.next >= op.targets.len() {
                                self.finish(op);
                            }
                        }
                        Err(error) => {
                            self.daemon.fail_gateway_activation(&key);
                            op.errors.insert(key, error);
                            self.finish(op);
                        }
                    },
                    UpdateProgress::Failed(error) => {
                        self.daemon.fail_gateway_activation(&key);
                        op.errors.insert(key, error);
                        self.finish(op);
                    }
                },
            }
        }

        async fn advance(&mut self, op: &mut Operation, credentials: &impl CredentialSource) {
            #[cfg(unix)]
            if op.phase == Phase::CoreRestart {
                self.advance_core_restart(op, Instant::now());
                return;
            }
            #[cfg(unix)]
            if op.phase == Phase::Updating {
                self.advance_update(op, credentials).await;
                return;
            }
            if op.started.elapsed() >= OPERATION_TIMEOUT {
                for (index, key) in op.targets.iter().enumerate() {
                    if op.waiting.contains(key)
                        || !op.generations.contains_key(key) && op.phase == Phase::Starting
                        || index >= op.next
                    {
                        op.errors
                            .entry(key.clone())
                            .or_insert(DaemonError::StopTimedOut);
                    }
                }
                if op.errors.is_empty() {
                    op.errors
                        .insert(op.targets[0].clone(), DaemonError::StopTimedOut);
                }
                // Cancel retries of launches owned by this operation, never
                // kill or alter an unattempted shard on timeout.
                for key in op.generations.keys() {
                    let _ = self.daemon.stop(key, StopMode::WaitForExit);
                }
                self.finish(op);
                return;
            }
            // poll() can submit a new grant between admission and this first
            // stop. Refuse while every target is still running.
            if op.phase == Phase::Stopping
                && op.next == 0
                && op.is_restart()
                && let Err(error) = self.daemon.check_restart_writer()
            {
                op.errors.insert(op.targets[0].clone(), error);
                self.finish(op);
                return;
            }
            if op.next < op.targets.len() {
                let key = &op.targets[op.next];
                op.next += 1;
                let result = if op.phase == Phase::Starting {
                    if op.is_restart() {
                        self.daemon
                            .start_inner(key, credentials, StartKind::Restart)
                    } else {
                        self.daemon.start_inner(key, credentials, StartKind::Manual)
                    }
                } else {
                    if self.daemon.has_child(key) {
                        op.waiting.insert(key.clone());
                    }
                    if op.force {
                        self.daemon.stop(key, StopMode::Force)
                    } else {
                        self.daemon.graceful_stop(key).await
                    }
                };
                match result {
                    Err(DaemonError::IdentifyJournalPending) if op.phase == Phase::Starting => {
                        op.next -= 1;
                        return;
                    }
                    Err(error) => {
                        op.errors.insert(key.clone(), error);
                        if op.phase == Phase::Stopping && op.is_restart() {
                            self.finish(op);
                            return;
                        }
                        // A failed manual start must not secretly succeed later
                        // through an automatically scheduled restart.
                        if op.phase == Phase::Starting
                            && !matches!(
                                error,
                                DaemonError::InvalidState
                                    | DaemonError::IdentifyDeferred
                                    | DaemonError::IdentifyUnavailable
                            )
                        {
                            let _ = self.daemon.stop(key, StopMode::WaitForExit);
                        }
                    }
                    Ok(()) if op.phase == Phase::Starting => {
                        if let Some(identity) = self
                            .daemon
                            .snapshots()
                            .into_iter()
                            .find(|s| &s.key == key)
                            .and_then(|s| s.identity)
                        {
                            op.generations.insert(key.clone(), identity.generation);
                        }
                    }
                    Ok(()) => {}
                }
                // Even successful spawn needs at least one subsequent poll.
                return;
            }
            let snapshots = self.daemon.snapshots();
            if op.phase == Phase::Stopping {
                for snapshot in &snapshots {
                    if op.waiting.contains(&snapshot.key) && !self.daemon.has_child(&snapshot.key) {
                        op.waiting.remove(&snapshot.key);
                        if !op.force
                            && !matches!(snapshot.state, ProcessState::Exited { code: Some(0) })
                        {
                            op.errors
                                .entry(snapshot.key.clone())
                                .or_insert(DaemonError::ChildExited);
                        }
                    }
                }
                if !op.waiting.is_empty() {
                    return;
                }
                if op.is_restart() && op.errors.is_empty() {
                    op.phase = Phase::Starting;
                    op.next = 0;
                    return;
                }
            } else {
                for snapshot in snapshots
                    .into_iter()
                    .filter(|s| op.targets.contains(&s.key))
                {
                    if op.errors.contains_key(&snapshot.key) {
                        continue;
                    }
                    let expected = op.generations.get(&snapshot.key);
                    let running = matches!(snapshot.state, ProcessState::Running)
                        && self.daemon.has_child(&snapshot.key)
                        && snapshot.identity.as_ref().map(|i| &i.generation) == expected;
                    if !running {
                        op.errors
                            .insert(snapshot.key.clone(), DaemonError::ChildExited);
                        let _ = self.daemon.stop(&snapshot.key, StopMode::WaitForExit);
                    }
                }
            }
            self.finish(op);
        }

        #[cfg(unix)]
        fn settle_core_restart_audit(&mut self, op: &mut Operation, now: Instant) -> bool {
            use tokio::sync::oneshot::error::TryRecvError;
            let Some(pending) = op
                .core_restart
                .as_ref()
                .and_then(|recovery| recovery.pending.as_ref())
            else {
                return true;
            };
            if pending.reply.is_some() {
                if now.saturating_duration_since(op.started) >= AUDIT_ACK_TIMEOUT {
                    self.cancel_pending_core_restart(op, "audit_timeout");
                } else if !self.gateway_eligible_for_core_restart(&op.targets[0]) {
                    self.cancel_pending_core_restart(op, "shard_not_running");
                }
            }
            let recovery = op.core_restart.as_mut().unwrap();
            let mut pending = recovery.pending.take().unwrap();
            let key = &op.targets[0];
            let result = match pending.audit.try_recv() {
                Err(TryRecvError::Empty) => {
                    recovery.pending = Some(pending);
                    return true;
                }
                result => result,
            };
            let Some(reply) = pending.reply else {
                // A late acknowledgement can only make the cancellation durable.
                if result.is_ok_and(|(record, ok)| ok || record.written) {
                    self.record_core_terminal(op);
                } else {
                    self.core_audit_failures
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                }
                return true;
            };
            let confirmed_rejection = matches!(&result, Ok((record, false)) if !record.written);
            let (reason, recorded) = match result {
                Ok((record, false)) if record.written => {
                    self.charge_core_attempt(key, recovery, now);
                    ("audit_unavailable", true)
                }
                Ok((_, false)) | Err(_) => ("audit_unavailable", false),
                Ok((_, true)) => match self.begin_core_restart(key, recovery, op.force, now) {
                    Ok(()) => {
                        op.started = now;
                        reply.send(self.operation_view(op));
                        return true;
                    }
                    Err(reason) => (reason, true),
                },
            };
            self.release_core_restart(key, recovery.target.core);
            if recorded {
                recovery.reason = Some(reason);
                op.errors.insert(key.clone(), DaemonError::InvalidState);
                self.finish(op);
                reply.send(self.operation_view(op));
            } else {
                self.audit_core_rejection(&op.id, key, op.force, recovery, reason);
                let mut response = Self::error(reason);
                if confirmed_rejection {
                    response["rejected"] = json!(true);
                }
                reply.send(response);
            }
            recorded
        }

        #[cfg(unix)]
        fn cancel_pending_core_restart(&mut self, op: &mut Operation, reason: &'static str) {
            let recovery = op.core_restart.as_mut().unwrap();
            let reply = recovery.pending.as_mut().unwrap().reply.take().unwrap();
            let key = &op.targets[0];
            self.release_core_restart(key, recovery.target.core);
            recovery.reason = Some(reason);
            op.errors
                .insert(key.clone(), DaemonError::OperationSuperseded);
            self.finish(op);
            reply.send(Self::error(reason));
        }

        #[cfg(unix)]
        fn advance_core_restart(&mut self, op: &mut Operation, now: Instant) {
            use crate::core_units::ManualRestartError;
            let recovery = op.core_restart.as_mut().expect("Core restart operation");
            if recovery.pending.is_some() {
                return;
            }
            let key = &op.targets[0];
            let wanted = self.gateway_eligible_for_core_restart(key);
            let entry = self.daemon.entries.get_mut(key).expect("owned shard");
            let program = Path::new(&entry.spec.gateway.program);
            let cores = entry.cores.as_mut().expect("owned Core units");
            match cores.poll_manual_restart(recovery.target.core as usize, program, now, wanted) {
                None => return,
                Some(Ok(())) => {}
                Some(Err(error)) => {
                    let (reason, error) = match error {
                        ManualRestartError::TerminationUnconfirmed => {
                            ("termination_unconfirmed", DaemonError::MonitorFailed)
                        }
                        ManualRestartError::ShardStopped => {
                            ("shard_not_running", DaemonError::OperationSuperseded)
                        }
                        ManualRestartError::LaunchFailed => {
                            ("launch_failed", DaemonError::SpawnFailed)
                        }
                        ManualRestartError::LaunchTimeout => {
                            ("launch_timeout", DaemonError::SpawnFailed)
                        }
                        ManualRestartError::LastRunningCore => {
                            ("last_running_core", DaemonError::InvalidState)
                        }
                    };
                    recovery.reason = Some(reason);
                    op.errors.insert(key.clone(), error);
                }
            }
            // If launch/backoff took time, also space the next attempt from completion.
            self.charge_core_attempt(key, recovery, now);
            self.finish(op);
        }

        #[cfg(unix)]
        fn cancel_audit_admissions(&mut self) {
            for pending in std::mem::take(&mut self.pending_gateway_updates) {
                let id = pending
                    .request
                    .operation_id
                    .as_deref()
                    .expect("validated ID");
                self.audit_gateway_rejection(id, pending.bot.as_deref(), &pending.actor);
                pending.reply.send(Self::error("daemon_shutdown"));
            }
            let ids: Vec<_> = self
                .operations
                .values()
                .filter(|op| op.phase == Phase::CoreRestart && op.finished.is_none())
                .map(|op| op.id.clone())
                .collect();
            for id in ids {
                let mut op = self.operations.remove(&id).expect("active operation");
                let key = &op.targets[0];
                let recovery = op.core_restart.as_mut().expect("Core restart operation");
                if recovery.pending.is_some() {
                    self.cancel_pending_core_restart(&mut op, "daemon_shutdown");
                    self.operations.insert(id, op);
                    continue;
                }
                let entry = self.daemon.entries.get_mut(key).expect("owned shard");
                entry
                    .cores
                    .as_mut()
                    .expect("owned Core units")
                    .cancel_manual_restart(recovery.target.core as usize);
                recovery.reason = Some("daemon_shutdown");
                op.errors
                    .insert(key.clone(), DaemonError::OperationSuperseded);
                self.finish(&mut op);
                self.operations.insert(id, op);
            }
        }
        /// Resolve already-submitted admissions without starting new work, then close and
        /// drain both writers within a single deadline.
        #[cfg(unix)]
        pub async fn shutdown_control_audits(&mut self) -> bool {
            self.shutdown_control_audits_with_timeout(AUDIT_SHUTDOWN_TIMEOUT)
                .await
        }

        #[cfg(unix)]
        pub async fn shutdown_core_launches(&mut self) -> bool {
            self.shutdown_core_launches_with_timeout(CORE_LAUNCH_SHUTDOWN_TIMEOUT)
                .await
        }

        #[cfg(unix)]
        async fn shutdown_core_launches_with_timeout(&mut self, timeout: Duration) -> bool {
            // This is also called on the exit path that leaves healthy children alone.
            self.cancel_audit_admissions();
            tokio::time::timeout(timeout, async {
                loop {
                    let mut pending = false;
                    for entry in self.daemon.entries.values_mut() {
                        if let Some(cores) = &mut entry.cores {
                            pending |= cores.poll_cancelled_launches();
                        }
                    }
                    if !pending {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .is_ok()
        }

        #[cfg(unix)]
        async fn shutdown_control_audits_with_timeout(&mut self, timeout: Duration) -> bool {
            self.cancel_audit_admissions();
            let drained = tokio::time::timeout(timeout, async {
                loop {
                    let ids: Vec<_> = self
                        .operations
                        .values()
                        .filter(|op| {
                            op.core_restart
                                .as_ref()
                                .is_some_and(|recovery| recovery.pending.is_some())
                        })
                        .map(|op| op.id.clone())
                        .collect();
                    let any_pending = !ids.is_empty();
                    for id in ids {
                        let mut op = self.operations.remove(&id).unwrap();
                        self.settle_core_restart_audit(&mut op, Instant::now());
                        self.operations.insert(id, op);
                    }
                    self.retry_pending_audit();
                    if !any_pending
                        && self.audit_writes.is_empty()
                        && self.pending_audit.is_empty()
                        && self.core_audit_rejections.is_empty()
                    {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
                self.core_audit_worker = None;
                self.gateway_audit_worker = None;
                while let Some(exited) = self.audit_worker_exits.last_mut() {
                    let _ = exited.await;
                    self.audit_worker_exits.pop();
                }
            })
            .await
            .is_ok();
            self.core_audit_worker = None;
            self.gateway_audit_worker = None;
            drained
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        fn log_dir(tag: &str) -> PathBuf {
            let dir = std::env::temp_dir().join(format!("botd-glog-{}-{tag}", std::process::id()));
            let _ = std::fs::remove_dir_all(&dir);
            std::fs::create_dir_all(&dir).unwrap();
            dir
        }

        #[test]
        fn gateway_log_created_with_marker() {
            let dir = log_dir("create");
            drop(open_gateway_log(&dir, 1024, 7).expect("opened"));
            let text = std::fs::read_to_string(dir.join("gateway.log")).unwrap();
            let stamp = text
                .strip_prefix("--- gateway generation 7 --- (")
                .and_then(|line| line.strip_suffix("Z)\n"))
                .expect("timestamped generation marker");
            let (seconds, millis) = stamp.split_once('.').unwrap();
            assert_eq!(seconds.len(), 19);
            assert_eq!(millis.len(), 3);
            assert!(millis.bytes().all(|byte| byte.is_ascii_digit()));
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                let mode = std::fs::metadata(dir.join("gateway.log"))
                    .unwrap()
                    .permissions()
                    .mode();
                assert_eq!(mode & 0o777, 0o600);
            }
            let _ = std::fs::remove_dir_all(&dir);
        }

        #[test]
        fn gateway_log_rotates_once_when_oversized() {
            let dir = log_dir("rotate");
            std::fs::write(dir.join("gateway.log"), vec![b'x'; 100]).unwrap();
            std::fs::write(dir.join("gateway.log.1"), b"older").unwrap();
            drop(open_gateway_log(&dir, 50, 2).expect("opened"));
            assert_eq!(
                std::fs::read(dir.join("gateway.log.1")).unwrap(),
                vec![b'x'; 100]
            );
            let text = std::fs::read_to_string(dir.join("gateway.log")).unwrap();
            assert_eq!(text.lines().count(), 1);
            assert!(text.starts_with("--- gateway generation 2 --- ("));
            // Under the limit: appended, not rotated.
            drop(open_gateway_log(&dir, 1024, 3).expect("opened"));
            let text = std::fs::read_to_string(dir.join("gateway.log")).unwrap();
            assert!(text.contains("generation 2") && text.contains("generation 3"));
            let _ = std::fs::remove_dir_all(&dir);
        }

        #[cfg(unix)]
        #[test]
        fn gateway_log_symlink_falls_back_to_none() {
            let dir = log_dir("symlink");
            let target = dir.join("target");
            std::fs::write(&target, b"keep").unwrap();
            std::os::unix::fs::symlink(&target, dir.join("gateway.log")).unwrap();
            assert!(open_gateway_log(&dir, 1024, 1).is_none());
            assert_eq!(std::fs::read(&target).unwrap(), b"keep");
            let _ = std::fs::remove_dir_all(&dir);
        }

        fn plane(bot_count: usize, shard_count: u32) -> ControlPlane {
            let mut text =
                "[cluster]\nid='local'\nmode='standalone'\n[node]\nid='one'\n".to_string();
            for bot in 0..bot_count {
                text.push_str(&format!("[[bots]]\nname='bot{bot}'\nbot_id='id-{bot}'\napplication_id='{bot}'\ntoken_env='TOKEN_{bot}'\ntotal_shards={shard_count}\n"));
                for shard in 0..shard_count {
                    text.push_str(&format!(
                        "[[bots.shard_placement]]\nshard_id={shard}\nprimary='one'\n"
                    ));
                }
            }
            let config: Config = toml::from_str(&text).unwrap();
            let mut daemon = Daemon::from_config(
                &config,
                &HashMap::new(),
                &std::env::current_dir()
                    .unwrap()
                    .join("target")
                    .join("daemon-control-tests")
                    .join(format!(
                        "{}-{:016x}",
                        std::process::id(),
                        getrandom::u64().unwrap()
                    )),
                RestartPolicy::default(),
            )
            .unwrap();
            daemon
                .set_gateway_program(&std::env::current_exe().unwrap())
                .unwrap();
            ControlPlane::new(daemon, config, true)
        }

        fn request(command: &str, bot: usize, id: usize) -> CommandRequest {
            CommandRequest {
                command: command.into(),
                bot: Some(format!("bot{bot}")),
                operation_id: Some(format!("{id:032x}")),
                ..Default::default()
            }
        }

        fn query(id: usize) -> CommandRequest {
            CommandRequest {
                command: "operation".into(),
                operation_id: Some(format!("{id:032x}")),
                ..Default::default()
            }
        }

        #[cfg(unix)]
        mod core_restart {
            use super::*;
            use crate::daemon::update_control::{ControlAccess, UpdateHub};
            use std::os::unix::fs::{PermissionsExt, symlink};

            fn manifest_update(id: &str) -> crate::update_rollback::UpdateControlRequest {
                serde_json::from_value(json!({
                    "command": "update", "bot": "7", "operation_id": id, "yes": true,
                    "component": "core",
                    "manifest": {
                        "bot_id": "7", "bot_name": "private-selector", "total_shards": 2,
                        "shards": [{"shard_id": 0, "core": {"current": 1, "candidate": 2}}],
                        "artifacts": {"core": {"path": "/fixture", "sha256": "0".repeat(64)}}
                    }
                }))
                .unwrap()
            }

            struct Fixture {
                plane: ControlPlane,
                dir: PathBuf,
                status_dir: Option<PathBuf>,
                key: ShardKey,
                now: Instant,
            }

            impl Fixture {
                fn new(diagnostics: bool, exited_slot: Option<usize>) -> Self {
                    let config: Config = toml::from_str(
                        "[cluster]\nid='local'\nmode='standalone'\n[node]\nid='one'\n\
                         [[bots]]\nname='private-selector'\nbot_id='7'\napplication_id='7'\n\
                         token_env='PRIVATE_TOKEN_ENV'\ntotal_shards=2\n\
                         [[bots.shard_placement]]\nshard_id=0\nprimary='one'\n\
                         [[bots.shard_placement]]\nshard_id=1\nprimary='one'\n",
                    )
                    .unwrap();
                    let name = format!("cr-test-{:016x}", getrandom::u64().unwrap());
                    let dir = std::env::temp_dir().join(&name);
                    std::fs::create_dir(&dir).unwrap();
                    // P1 reads only tmpfs, which may be mounted noexec.
                    let status_dir = diagnostics.then(|| {
                        let shm = Path::new("/dev/shm");
                        assert!(crate::core_status::is_tmpfs(shm));
                        let status = shm.join(&name);
                        std::fs::create_dir(&status).unwrap();
                        status
                    });
                    let program = dir.join("private-program");
                    let exit = exited_slot.map_or_else(String::new, |slot| {
                        format!("case \"$2\" in */core-{slot}.sock) exit 0;; esac\n")
                    });
                    std::fs::write(
                        &program,
                        format!("#!/bin/sh\nIFS= read -r capability || exit 1\n{exit}while IFS= read -r line; do :; done\n"),
                    ).unwrap();
                    std::fs::set_permissions(&program, std::fs::Permissions::from_mode(0o700))
                        .unwrap();
                    let mut daemon = Daemon::from_config(
                        &config,
                        &HashMap::new(),
                        &dir,
                        RestartPolicy::default(),
                    )
                    .unwrap();
                    daemon.set_gateway_program(&program).unwrap();
                    daemon.enable_separate_cores().unwrap();
                    let key = ShardKey {
                        bot_id: "7".into(),
                        shard_id: 0,
                    };
                    daemon
                        .enable_mock_discord("127.0.0.1:1".parse().unwrap())
                        .unwrap();
                    let cores = daemon
                        .entries
                        .get_mut(&key)
                        .unwrap()
                        .cores
                        .as_mut()
                        .unwrap();
                    if let Some(path) = &status_dir {
                        cores.configure_status(Some(path.clone()), false, None);
                    }
                    static RUNTIME: std::sync::OnceLock<tokio::runtime::Runtime> =
                        std::sync::OnceLock::new();
                    let runtime = RUNTIME.get_or_init(|| {
                        tokio::runtime::Builder::new_multi_thread()
                            .worker_threads(1)
                            .enable_all()
                            .build()
                            .unwrap()
                    });
                    let _entered = runtime.enter();
                    daemon.start(&key, &no_credentials).unwrap();
                    let cores = daemon
                        .entries
                        .get_mut(&key)
                        .unwrap()
                        .cores
                        .as_mut()
                        .unwrap();
                    if let Some(slot) = exited_slot {
                        let deadline = Instant::now() + Duration::from_secs(2);
                        while cores.manual_restart_status(slot).unwrap().running {
                            assert!(Instant::now() < deadline, "test child did not exit");
                            std::thread::yield_now();
                        }
                    }
                    let mut plane = ControlPlane::new(daemon, config, true);
                    plane.core_recovery_audit = Some(dir.join("core-recovery-audit.jsonl"));
                    plane.core_audit_ready = true;
                    Self {
                        plane,
                        dir,
                        status_dir,
                        key,
                        now: Instant::now(),
                    }
                }

                fn request(&self, id: usize, core: u32, force: bool) -> CommandRequest {
                    CommandRequest {
                        command: "core-restart".into(),
                        bot: Some("private-selector".into()),
                        operation_id: Some(format!("{id:032x}")),
                        shard: Some(0),
                        core: Some(core),
                        expect_bot_id: Some("7".into()),
                        expect_total_shards: Some(2),
                        core_recovery_version: Some(CORE_RECOVERY_VERSION),
                        expect_incarnation: self
                            .plane
                            .daemon
                            .core_status(&self.key)
                            .unwrap()
                            .get(core as usize)
                            .and_then(|core| core.status_id)
                            .map(|id| format!("{id:016x}")),
                        expect_empty: self
                            .plane
                            .daemon
                            .core_status(&self.key)
                            .unwrap()
                            .get(core as usize)
                            .is_some_and(|core| core.pid.is_none()),
                        force,
                        ..Default::default()
                    }
                }

                fn stop_gateway(&mut self) {
                    let recovery_key = self.plane.daemon.entries[&self.key].recovery_key;
                    let Some(child) = self.plane.daemon.recovery.child_mut(recovery_key).unwrap()
                    else {
                        return;
                    };
                    if child.try_wait().unwrap().is_none() {
                        child.start_kill().unwrap();
                        let until = Instant::now() + Duration::from_secs(2);
                        while child.try_wait().unwrap().is_none() {
                            assert!(Instant::now() < until);
                            std::thread::yield_now();
                        }
                    }
                }

                fn load_replay(&mut self, replay: CoreAuditReplay, clock: Option<(&str, u64)>) {
                    self.plane.core_restart_history =
                        ControlPlane::replayed_core_restart_budget(&replay, self.now, clock);
                    self.plane.archived_core_restarts = replay.archived;
                    self.plane.reserved_core_ids = replay.reserved;
                    self.plane.core_audit_ready = !replay.invalid;
                }

                fn submit(&mut self, request: &CommandRequest, at: Duration) -> Value {
                    self.plane
                        .request_core_restart(
                            request,
                            &ControlAccess::default(),
                            self.now + at,
                            false,
                            None,
                        )
                        .unwrap()
                }

                fn complete(&mut self, id: usize, at: Duration) -> Value {
                    let id = format!("{id:032x}");
                    let mut op = self.plane.operations.remove(&id).unwrap();
                    let deadline = Instant::now() + Duration::from_secs(2);
                    while op.finished.is_none() {
                        self.plane.advance_core_restart(&mut op, self.now + at);
                        assert!(Instant::now() < deadline, "test restart did not finish");
                        std::thread::yield_now();
                    }
                    let answer = self.plane.operation_view(&op);
                    self.plane.operations.insert(id, op);
                    answer
                }
            }

            impl Drop for Fixture {
                fn drop(&mut self) {
                    self.stop_gateway();
                    for entry in self.plane.daemon.entries.values_mut() {
                        if let Some(cores) = entry.cores.as_mut() {
                            cores.kill();
                            let until = Instant::now() + Duration::from_secs(2);
                            while cores.any_alive() && Instant::now() < until {
                                std::thread::yield_now();
                            }
                        }
                    }
                    if let Some(path) = &self.status_dir {
                        let _ = std::fs::remove_dir_all(path);
                    }
                    let _ = std::fs::remove_dir_all(&self.dir);
                }
            }

            #[test]
            fn pinning_and_request_shape_are_fail_closed() {
                let mut f = Fixture::new(true, None);
                let valid = f.request(1, 0, true);
                assert!(valid.valid());
                for (field, value, reason) in [
                    ("expect_bot_id", json!("8"), "bot_id_mismatch"),
                    ("expect_total_shards", json!(3), "total_shards_mismatch"),
                    (
                        "expect_incarnation",
                        json!("ffffffffffffffff"),
                        "incarnation_mismatch",
                    ),
                    ("core", json!(2), "core_out_of_range"),
                    ("shard", json!(2), "shard_not_local"),
                ] {
                    let mut req = serde_json::to_value(&valid).unwrap();
                    req[field] = value;
                    assert_eq!(
                        f.submit(&serde_json::from_value(req).unwrap(), Duration::ZERO)["error"],
                        reason
                    );
                }
                for (field, value) in [
                    ("command", json!("restart")),
                    ("shards", json!([0])),
                    ("yes", json!(true)),
                    ("expect_incarnation", json!("0000000000000000")),
                    ("expect_incarnation", json!("bad")),
                    ("expect_bot_id", Value::Null),
                    ("expect_total_shards", Value::Null),
                    ("operation_id", json!("argument text")),
                ] {
                    let mut req = serde_json::to_value(&valid).unwrap();
                    req[field] = value;
                    assert!(
                        !serde_json::from_value::<CommandRequest>(req)
                            .unwrap()
                            .valid(),
                        "{field}"
                    );
                }
                assert!(f.plane.operations.is_empty());
                assert!(f.plane.core_restart_history.is_empty());
            }

            #[test]
            fn requires_admin_and_scope_for_exactly_one_shard() {
                let mut f = Fixture::new(true, None);
                let req = f.request(1, 0, true);
                let other = ShardKey {
                    bot_id: "7".into(),
                    shard_id: 1,
                };
                for (role, key) in [("viewer", &f.key), ("updater", &f.key), ("admin", &other)] {
                    let access = ControlAccess {
                        subject: "operator".into(),
                        role: role.into(),
                        scope: Some(vec![key.clone()]),
                    };
                    assert_eq!(
                        f.plane.request_as(&req, &access)["error"],
                        "permission_denied"
                    );
                }
                assert_eq!(f.plane.request(&req)["error"], "permission_denied");
                assert!(f.plane.operations.is_empty());
                let allowed = ControlAccess {
                    scope: Some(vec![f.key.clone()]),
                    ..Default::default()
                };
                assert_eq!(
                    f.plane.request_as(&req, &allowed)["operation"]["state"],
                    "running"
                );
                let denied = ControlAccess {
                    scope: Some(vec![other]),
                    ..Default::default()
                };
                assert_eq!(f.plane.request_as(&query(1), &denied)["ok"], false);
                assert_eq!(
                    f.plane.request_as(&query(1), &allowed)["operation"]["core"],
                    0
                );
            }

            #[test]
            fn force_is_required_for_unpinned_healthy_or_last_running_core() {
                let mut f = Fixture::new(false, None);
                let mut req = f.request(1, 0, false);
                req.expect_incarnation = None;
                assert_eq!(
                    f.submit(&req, Duration::ZERO)["error"],
                    "incarnation_required"
                );
                req.force = true;
                req.expect_incarnation = Some("0000000000000001".into());
                assert_eq!(
                    f.submit(&req, Duration::ZERO)["error"],
                    "incarnation_mismatch"
                );
                req.expect_incarnation = None;
                assert_eq!(
                    f.submit(&req, Duration::ZERO)["error"],
                    "incarnation_required"
                );
                req.expect_incarnation = f.request(1, 0, true).expect_incarnation;
                assert_eq!(
                    f.submit(&req, Duration::ZERO)["operation"]["state"],
                    "running"
                );

                let mut f = Fixture::new(true, None);
                let mut req = f.request(1, 0, false);
                assert_eq!(
                    f.submit(&req, Duration::ZERO)["error"],
                    "core_not_suspected_stalled"
                );
                req.expect_incarnation = None;
                assert_eq!(
                    f.submit(&req, Duration::ZERO)["error"],
                    "incarnation_required"
                );

                let mut f = Fixture::new(true, Some(1));
                let mut req = f.request(1, 0, false);
                let down = f.request(2, 1, false);
                assert_eq!(f.submit(&req, Duration::ZERO)["error"], "last_running_core");
                assert_eq!(
                    f.submit(&down, Duration::ZERO)["operation"]["state"],
                    "running"
                );
                assert_eq!(
                    f.complete(2, Duration::from_secs(1))["operation"]["state"],
                    "succeeded"
                );
                req.force = true;
                assert_eq!(
                    f.submit(&req, Duration::ZERO)["operation"]["state"],
                    "running"
                );
            }

            #[test]
            fn rate_limits_include_failed_attempts_and_apply_to_force() {
                let mut f = Fixture::new(true, None);
                let seconds = Duration::from_secs;
                let millis = Duration::from_millis;
                let old = f.request(1, 0, true);
                assert_eq!(f.submit(&old, seconds(0))["operation"]["state"], "running");
                assert_eq!(f.complete(1, seconds(0))["operation"]["state"], "succeeded");
                let stale = f.request(9, 0, true);
                let stale = CommandRequest {
                    expect_incarnation: old.expect_incarnation,
                    ..stale
                };
                assert_eq!(
                    f.submit(&stale, seconds(0))["error"],
                    "incarnation_mismatch"
                );
                let req = f.request(2, 0, true);
                assert_eq!(f.submit(&req, millis(59_999))["retry_after_ms"], 1);
                assert_eq!(f.submit(&req, seconds(60))["operation"]["state"], "running");
                let missing = f.dir.join("missing-program");
                f.plane
                    .daemon
                    .entries
                    .get_mut(&f.key)
                    .unwrap()
                    .spec
                    .gateway
                    .program = missing.into_os_string();
                assert_eq!(
                    f.complete(2, seconds(60))["operation"]["reason"],
                    "launch_failed"
                );
                let program = f.dir.join("private-program");
                f.plane
                    .daemon
                    .entries
                    .get_mut(&f.key)
                    .unwrap()
                    .spec
                    .gateway
                    .program = program.into_os_string();
                let req = f.request(3, 0, true);
                assert_eq!(
                    f.submit(&req, seconds(120))["operation"]["state"],
                    "running"
                );
                assert_eq!(
                    f.complete(3, seconds(120))["operation"]["state"],
                    "succeeded"
                );
                let req = f.request(4, 0, true);
                assert_eq!(f.submit(&req, seconds(180))["retry_after_ms"], 720_000);
                assert_eq!(f.submit(&req, millis(899_999))["retry_after_ms"], 1);
                let other = f.request(5, 1, true);
                assert_eq!(
                    f.submit(&other, seconds(180))["operation"]["state"],
                    "running"
                );
                assert_eq!(
                    f.complete(5, seconds(180))["operation"]["state"],
                    "succeeded"
                );
                assert_eq!(
                    f.submit(&req, seconds(900))["operation"]["state"],
                    "running"
                );
            }

            #[tokio::test]
            async fn busy_exclusion_applies_in_both_directions() {
                let mut f = Fixture::new(true, None);
                let req = f.request(1, 0, true);
                for command in ["start", "stop", "restart", "gateway-update"] {
                    let mut op = request(command, 0, 2);
                    op.bot = req.bot.clone();
                    op.shards = Some(vec![0]);
                    assert_eq!(f.plane.request(&op)["ok"], true);
                    assert_eq!(f.submit(&req, Duration::ZERO)["error"], "busy");
                    let mut op = f.plane.operations.remove(&format!("{:032x}", 2)).unwrap();
                    f.plane.finish(&mut op);
                }
                let entry = f.plane.daemon.entries.get_mut(&f.key).unwrap();
                entry.update_fallback_reserved = true;
                assert_eq!(f.submit(&req, Duration::ZERO)["error"], "busy");
                let entry = f.plane.daemon.entries.get_mut(&f.key).unwrap();
                entry.update_fallback_reserved = false;
                let spawn = || {
                    tokio::process::Command::new("/bin/sleep")
                        .arg("30")
                        .kill_on_drop(true)
                        .spawn()
                        .unwrap()
                };
                let candidate_child = spawn();
                let candidate_identity = ProcessIdentity {
                    key: f.key.clone(),
                    pid: candidate_child.id().unwrap_or(0),
                    generation: LaunchGeneration {
                        daemon_instance: [0; 16],
                        sequence: 0,
                    },
                    launch_observed_unix_ms: 0,
                };
                entry.candidate = Some(Candidate {
                    child: candidate_child,
                    identity: candidate_identity,
                    started: f.now,
                    ready_marker: PathBuf::new(),
                    active_marker: PathBuf::new(),
                    activated: None,
                });
                assert_eq!(f.submit(&req, Duration::ZERO)["error"], "busy");
                let entry = f.plane.daemon.entries.get_mut(&f.key).unwrap();
                entry.candidate = None;
                entry.retiring = Some(Retiring {
                    child: spawn(),
                    since: f.now,
                });
                assert_eq!(f.submit(&req, Duration::ZERO)["error"], "busy");
                f.plane.daemon.entries.get_mut(&f.key).unwrap().retiring = None;
                assert_eq!(
                    f.submit(&req, Duration::ZERO)["operation"]["state"],
                    "running"
                );
                let other_core = f.request(3, 1, true);
                assert_eq!(f.submit(&other_core, Duration::ZERO)["error"], "busy");
                for command in ["start", "stop", "gateway-update"] {
                    let mut op = request(command, 0, 2);
                    op.bot = req.bot.clone();
                    op.shards = Some(vec![0]);
                    if command == "stop" {
                        op.force = true;
                        op.yes = true;
                    }
                    assert_eq!(f.plane.request(&op)["error"], "busy");
                }
                let mut independent = request("start", 0, 4);
                independent.bot = req.bot.clone();
                independent.shards = Some(vec![1]);
                assert_eq!(f.plane.request(&independent)["ok"], true);
            }

            #[test]
            fn audit_failure_leaves_child_and_rate_limit_untouched() {
                let mut f = Fixture::new(true, None);
                let req = f.request(1, 0, true);
                let before = f.plane.daemon.core_status(&f.key).unwrap();
                let path = f.dir.join("core-recovery-audit.jsonl");
                let target = f.dir.join("private-target");
                std::fs::write(&target, b"keep").unwrap();
                symlink(&target, &path).unwrap();
                assert_eq!(f.submit(&req, Duration::ZERO)["error"], "audit_unavailable");
                std::fs::remove_file(&path).unwrap();
                std::fs::hard_link(&target, &path).unwrap();
                assert_eq!(f.submit(&req, Duration::ZERO)["error"], "audit_unavailable");
                assert_eq!(std::fs::read(&target).unwrap(), b"keep");
                assert_eq!(f.plane.daemon.core_status(&f.key).unwrap(), before);
                assert!(f.plane.operations.is_empty());
                assert!(f.plane.core_restart_history.is_empty());
                std::fs::remove_file(&path).unwrap();
                assert_eq!(
                    f.submit(&req, Duration::ZERO)["operation"]["state"],
                    "running"
                );
            }

            #[test]
            fn pending_audit_holds_the_shard_but_never_starts_the_restart() {
                let mut f = Fixture::new(true, None);
                let (writer, mut jobs) = tokio::sync::mpsc::channel(4);
                f.plane.core_audit_worker = Some(writer);
                let req = f.request(1, 0, true);
                let (response, mut reply) = tokio::sync::oneshot::channel();
                f.plane.dispatch_core_restart(
                    &req,
                    &ControlAccess::default(),
                    false,
                    response,
                    json!({}),
                );
                let id = req.operation_id.as_deref().unwrap();
                let before = f.plane.daemon.core_status(&f.key).unwrap();
                let mut op = f.plane.operations.remove(id).unwrap();
                assert!(f.plane.settle_core_restart_audit(&mut op, f.now));
                f.plane.advance_core_restart(&mut op, f.now);
                f.plane.operations.insert(id.to_owned(), op);
                assert!(matches!(
                    reply.try_recv(),
                    Err(tokio::sync::oneshot::error::TryRecvError::Empty)
                ));
                assert_eq!(f.plane.request(&query(1))["error"], "audit_pending");
                let second = f.request(2, 1, true);
                assert_eq!(f.submit(&second, Duration::ZERO)["error"], "busy");
                let job = jobs.try_recv().unwrap();
                let row: Value = serde_json::from_slice(&job.record.bytes).unwrap();
                assert_eq!(row["event"], "requested");
                assert!(job.completion.unwrap().send((job.record, false)).is_ok());
                let mut op = f.plane.operations.remove(id).unwrap();
                assert!(!f.plane.settle_core_restart_audit(&mut op, f.now));
                assert_eq!(reply.try_recv().unwrap()["error"], "audit_unavailable");
                assert_eq!(f.plane.daemon.core_status(&f.key).unwrap(), before);
                assert!(f.plane.core_restart_history.is_empty());
                assert!(f.plane.operations.is_empty());
                let (response, mut reply) = tokio::sync::oneshot::channel();
                f.plane.dispatch_core_restart(
                    &req,
                    &ControlAccess::default(),
                    false,
                    response,
                    json!({}),
                );
                let mut op = f.plane.operations.remove(id).unwrap();
                // Earlier rejections also carry a completion; pick this request's own row.
                let job = std::iter::from_fn(|| jobs.try_recv().ok())
                    .find(|job| {
                        job.completion.is_some()
                            && serde_json::from_slice::<Value>(&job.record.bytes)
                                .is_ok_and(|row| row["event"] == "requested")
                    })
                    .unwrap();
                assert!(job.completion.unwrap().send((job.record, true)).is_ok());
                assert!(f.plane.settle_core_restart_audit(&mut op, f.now));
                assert_eq!(reply.try_recv().unwrap()["operation"]["state"], "running");
                assert_eq!(f.plane.core_restart_history.len(), 1);
                f.plane.operations.insert(id.to_owned(), op);
            }

            #[test]
            fn recovering_gateway_is_ineligible_at_admission_and_before_eof() {
                let mut f = Fixture::new(false, None);
                let first = f.request(1, 0, true);
                let before = f.plane.daemon.core_status(&f.key).unwrap();
                f.stop_gateway();
                f.plane.daemon.poll(&no_credentials).unwrap();
                assert!(f.plane.daemon.entries[&f.key].snapshot.desired_running);
                let recovery_key = f.plane.daemon.entries[&f.key].recovery_key;
                assert_eq!(
                    f.plane
                        .daemon
                        .recovery
                        .snapshot(recovery_key)
                        .unwrap()
                        .state,
                    RecoveryState::Waiting
                );
                assert_eq!(
                    f.submit(&first, Duration::ZERO)["error"],
                    "shard_not_running"
                );
                assert!(f.plane.operations.is_empty());
                assert_eq!(f.plane.daemon.core_status(&f.key).unwrap(), before);

                let mut f = Fixture::new(false, None);
                let (queue, mut jobs) = tokio::sync::mpsc::channel(4);
                f.plane.core_audit_worker = Some(queue);
                let request = f.request(1, 0, true);
                let before = f.plane.daemon.core_status(&f.key).unwrap();
                let (response, mut reply) = tokio::sync::oneshot::channel();
                f.plane.dispatch_core_restart(
                    &request,
                    &ControlAccess::default(),
                    false,
                    response,
                    json!({}),
                );
                let mut job = jobs.try_recv().unwrap();
                f.plane.write_audit(&mut job.record).unwrap();
                assert!(job.completion.unwrap().send((job.record, true)).is_ok());
                let id = request.operation_id.as_deref().unwrap();
                let mut op = f.plane.operations.remove(id).unwrap();
                f.stop_gateway();
                let now = op.started;
                assert!(f.plane.settle_core_restart_audit(&mut op, now));
                assert_eq!(reply.try_recv().unwrap()["error"], "shard_not_running");
                assert_eq!(f.plane.operation_view(&op)["operation"]["state"], "failed");
                assert_eq!(f.plane.daemon.core_status(&f.key).unwrap(), before);
                assert!(
                    !f.plane.daemon.entries[&f.key]
                        .cores
                        .as_ref()
                        .unwrap()
                        .manual_restart_reserved()
                );
            }

            #[test]
            fn expired_audit_releases_the_shard_and_late_success_cannot_start_restart() {
                let mut f = Fixture::new(false, None);
                let (queue, mut jobs) = tokio::sync::mpsc::channel(4);
                f.plane.core_audit_worker = Some(queue);
                let request = f.request(1, 0, true);
                let (response, mut reply) = tokio::sync::oneshot::channel();
                f.plane.dispatch_core_restart(
                    &request,
                    &ControlAccess::default(),
                    false,
                    response,
                    json!({}),
                );
                let id = request.operation_id.as_deref().unwrap();
                let mut op = f.plane.operations.remove(id).unwrap();
                let before = f.plane.daemon.core_status(&f.key).unwrap();
                let due = op.started + AUDIT_ACK_TIMEOUT;
                assert!(
                    f.plane
                        .settle_core_restart_audit(&mut op, due - Duration::from_millis(1))
                );
                assert!(op.finished.is_none());
                assert!(f.plane.settle_core_restart_audit(&mut op, due));
                assert_eq!(reply.try_recv().unwrap()["error"], "audit_timeout");
                assert_eq!(f.plane.operation_view(&op)["operation"]["state"], "failed");
                assert!(
                    !f.plane.daemon.entries[&f.key]
                        .cores
                        .as_ref()
                        .unwrap()
                        .manual_restart_reserved()
                );
                f.plane.operations.insert(id.to_owned(), op);
                let stop = CommandRequest {
                    command: "stop".into(),
                    bot: request.bot.clone(),
                    operation_id: Some(format!("{:032x}", 2)),
                    shards: Some(vec![0]),
                    force: true,
                    yes: true,
                    ..Default::default()
                };
                assert_eq!(f.plane.request(&stop)["ok"], true);

                let mut job = jobs.try_recv().unwrap();
                f.plane.write_audit(&mut job.record).unwrap();
                assert!(job.completion.unwrap().send((job.record, true)).is_ok());
                let mut op = f.plane.operations.remove(id).unwrap();
                assert!(f.plane.settle_core_restart_audit(&mut op, due));
                let terminal = jobs.try_recv().unwrap();
                let row: Value = serde_json::from_slice(&terminal.record.bytes).unwrap();
                assert_eq!(row["event"], "failed");
                assert_eq!(row["reason"], "audit_timeout");
                assert_eq!(f.plane.daemon.core_status(&f.key).unwrap(), before);
            }

            #[test]
            fn delayed_audit_starts_the_interval_at_the_action_boundary() {
                let mut f = Fixture::new(false, None);
                let (queue, mut jobs) = tokio::sync::mpsc::channel(4);
                f.plane.core_audit_worker = Some(queue);
                let request = f.request(1, 0, true);
                let (response, mut reply) = tokio::sync::oneshot::channel();
                f.plane.dispatch_core_restart(
                    &request,
                    &ControlAccess::default(),
                    false,
                    response,
                    json!({}),
                );
                let id = request.operation_id.as_deref().unwrap();
                let mut op = f.plane.operations.remove(id).unwrap();
                let action_at = op.started + AUDIT_ACK_TIMEOUT - Duration::from_millis(1);
                let mut job = jobs.try_recv().unwrap();
                f.plane.write_audit(&mut job.record).unwrap();
                assert!(job.completion.unwrap().send((job.record, true)).is_ok());
                assert!(f.plane.settle_core_restart_audit(&mut op, action_at));
                assert_eq!(reply.try_recv().unwrap()["operation"]["state"], "running");
                assert_eq!(
                    f.plane.core_restart_history[&(f.key.clone(), 0)].back(),
                    Some(&action_at)
                );
                f.plane.operations.insert(id.to_owned(), op);
                assert_eq!(
                    f.complete(1, action_at.duration_since(f.now))["operation"]["state"],
                    "succeeded"
                );
                f.plane.core_audit_worker = None;
                let next = f.request(2, 0, true);
                assert_eq!(
                    f.submit(
                        &next,
                        (action_at - f.now) + CORE_RESTART_INTERVAL - Duration::from_millis(1)
                    )["retry_after_ms"],
                    1
                );
                assert_eq!(
                    f.submit(&next, (action_at - f.now) + CORE_RESTART_INTERVAL)["operation"]["state"],
                    "running"
                );
            }

            #[test]
            fn already_exited_child_obeys_backoff_and_spawn_success_precedes_readiness() {
                let mut f = Fixture::new(true, Some(0));
                let old = f.request(1, 0, true);
                assert_eq!(
                    f.submit(&old, Duration::ZERO)["operation"]["state"],
                    "running"
                );
                let id = old.operation_id.as_deref().unwrap();
                let mut op = f.plane.operations.remove(id).unwrap();
                f.plane
                    .advance_core_restart(&mut op, f.now + Duration::from_millis(999));
                assert!(op.finished.is_none());
                let slot = &f.plane.daemon.core_status(&f.key).unwrap()[0];
                assert_eq!(slot.failures, 1);
                assert!(slot.pid.is_none());
                f.plane.operations.insert(id.to_owned(), op);
                assert_eq!(
                    f.complete(1, Duration::from_secs(1))["operation"]["state"],
                    "succeeded"
                );
                // The fixture exits after bootstrap without opening a Core IPC socket.
                let cores = f
                    .plane
                    .daemon
                    .entries
                    .get_mut(&f.key)
                    .unwrap()
                    .cores
                    .as_mut()
                    .unwrap();
                let until = Instant::now() + Duration::from_secs(2);
                while cores.manual_restart_status(0).unwrap().running {
                    assert!(Instant::now() < until);
                    std::thread::yield_now();
                }
                assert_eq!(
                    f.plane.request_as(&query(1), &ControlAccess::default())["operation"]["state"],
                    "succeeded"
                );
            }

            #[test]
            fn stale_previews_are_rejected_with_or_without_p1_and_even_with_force() {
                for diagnostics in [false, true] {
                    let mut f = Fixture::new(diagnostics, None);
                    let old = f.request(1, 0, true);
                    assert!(old.expect_incarnation.is_some());
                    let mut empty = old.clone();
                    empty.operation_id = Some(format!("{:032x}", 8));
                    empty.expect_incarnation = None;
                    empty.expect_empty = true;
                    assert_eq!(
                        f.submit(&empty, Duration::ZERO)["error"],
                        "incarnation_mismatch"
                    );
                    assert_eq!(
                        f.submit(&old, Duration::ZERO)["operation"]["state"],
                        "running"
                    );
                    assert_eq!(
                        f.complete(1, Duration::ZERO)["operation"]["state"],
                        "succeeded"
                    );
                    let before = f.plane.daemon.core_status(&f.key).unwrap();
                    let stale = CommandRequest {
                        operation_id: Some(format!("{:032x}", 2)),
                        ..old
                    };
                    assert_eq!(
                        f.submit(&stale, Duration::ZERO)["error"],
                        "incarnation_mismatch"
                    );
                    assert_eq!(f.plane.daemon.core_status(&f.key).unwrap(), before);
                }
            }

            #[test]
            fn stopped_peer_is_checked_again_after_audit_even_with_stalled_target() {
                let mut f = Fixture::new(true, None);
                let statuses = f.plane.daemon.core_status(&f.key).unwrap();
                let target = &statuses[0];
                let pid = statuses[1].pid.unwrap() as libc::pid_t;
                let boot_ms = crate::core_status::try_boot_ms().unwrap();
                let mut writer = crate::core_status::StatusWriter::open_checked_with_id(
                    target.status_path.as_deref().unwrap(),
                    false,
                    target.status_id,
                )
                .unwrap();
                writer.write(&crate::core_status::StatusRecord {
                    pending: 1,
                    oldest_age_s: 30,
                    warned: 0,
                    threshold_s: 30,
                    boot_ms: boot_ms.saturating_sub(31_000),
                    pid: target.pid.unwrap(),
                    generation: target.status_id.unwrap(),
                    seq: 1,
                });
                assert_eq!(
                    f.plane.core_view(&f.key, 0, target)["status"]["suspected_stalled"],
                    true
                );
                let (queue, mut jobs) = tokio::sync::mpsc::channel(4);
                f.plane.core_audit_worker = Some(queue);
                let req = f.request(1, 0, false);
                let (response, mut reply) = tokio::sync::oneshot::channel();
                f.plane.dispatch_core_restart(
                    &req,
                    &ControlAccess::default(),
                    false,
                    response,
                    json!({}),
                );
                let job = jobs.try_recv().unwrap();
                let id = req.operation_id.as_deref().unwrap();
                let mut op = f.plane.operations.remove(id).unwrap();
                assert_eq!(unsafe { libc::kill(pid, libc::SIGSTOP) }, 0);
                let until = Instant::now() + Duration::from_secs(2);
                while f
                    .plane
                    .daemon
                    .entries
                    .get_mut(&f.key)
                    .unwrap()
                    .cores
                    .as_mut()
                    .unwrap()
                    .peer_available(0)
                {
                    assert!(Instant::now() < until);
                    std::thread::yield_now();
                }
                assert!(job.completion.unwrap().send((job.record, true)).is_ok());
                assert!(f.plane.settle_core_restart_audit(&mut op, f.now));
                let response = reply.try_recv().unwrap();
                assert_eq!(response["ok"], false);
                assert_eq!(response["operation"]["operation_id"], id);
                assert_eq!(response["operation"]["state"], "failed");
                assert_eq!(response["operation"]["reason"], "last_running_core");
                assert_eq!(f.plane.operation_view(&op)["operation"]["state"], "failed");
                assert_eq!(f.plane.daemon.core_status(&f.key).unwrap(), statuses);
                assert_eq!(unsafe { libc::kill(pid, libc::SIGCONT) }, 0);
            }

            #[test]
            fn peer_loss_during_grace_withholds_non_force_sigkill() {
                let mut f = Fixture::new(true, None);
                let status = f.plane.daemon.core_status(&f.key).unwrap();
                let target = &status[0];
                let boot_ms = crate::core_status::try_boot_ms().unwrap();
                let mut writer = crate::core_status::StatusWriter::open_checked_with_id(
                    target.status_path.as_deref().unwrap(),
                    false,
                    target.status_id,
                )
                .unwrap();
                writer.write(&crate::core_status::StatusRecord {
                    pending: 1,
                    oldest_age_s: 30,
                    warned: 0,
                    threshold_s: 30,
                    boot_ms: boot_ms.saturating_sub(31_000),
                    pid: target.pid.unwrap(),
                    generation: target.status_id.unwrap(),
                    seq: 1,
                });
                let target_pid = target.pid.unwrap() as libc::pid_t;
                let peer_pid = status[1].pid.unwrap() as libc::pid_t;
                assert_eq!(unsafe { libc::kill(target_pid, libc::SIGSTOP) }, 0);
                let until = Instant::now() + Duration::from_secs(2);
                while !std::fs::read_to_string(format!("/proc/{target_pid}/stat"))
                    .unwrap()
                    .rsplit_once(") ")
                    .is_some_and(|(_, stat)| stat.starts_with("T "))
                {
                    assert!(Instant::now() < until);
                    std::thread::yield_now();
                }
                let request = f.request(1, 0, false);
                assert_eq!(
                    f.submit(&request, Duration::ZERO)["operation"]["state"],
                    "running"
                );
                assert_eq!(unsafe { libc::kill(peer_pid, libc::SIGSTOP) }, 0);
                let until = Instant::now() + Duration::from_secs(2);
                while f
                    .plane
                    .daemon
                    .entries
                    .get_mut(&f.key)
                    .unwrap()
                    .cores
                    .as_mut()
                    .unwrap()
                    .peer_available(0)
                {
                    assert!(Instant::now() < until);
                    std::thread::yield_now();
                }
                let id = request.operation_id.as_deref().unwrap();
                let mut op = f.plane.operations.remove(id).unwrap();
                f.plane
                    .advance_core_restart(&mut op, f.now + Duration::from_millis(4_999));
                assert!(op.finished.is_none());
                f.plane
                    .advance_core_restart(&mut op, f.now + Duration::from_secs(5));
                let result = f.plane.operation_view(&op);
                assert_eq!(result["operation"]["state"], "failed");
                assert_eq!(result["operation"]["reason"], "last_running_core");
                let after = f.plane.daemon.core_status(&f.key).unwrap();
                assert_eq!(after[0].pid, target.pid);
                assert_eq!(after[0].status_id, target.status_id);
                assert!(!after[0].running);
                assert_eq!(after[1], status[1]);
                assert!(
                    std::fs::read_to_string(format!("/proc/{target_pid}/stat"))
                        .unwrap()
                        .rsplit_once(") ")
                        .is_some_and(|(_, stat)| stat.starts_with("T "))
                );
                f.plane.operations.insert(id.to_owned(), op);
                assert_eq!(unsafe { libc::kill(peer_pid, libc::SIGCONT) }, 0);
                let retry = f.request(2, 0, true);
                assert_eq!(retry.incarnation(), target.status_id);
                assert_eq!(
                    f.submit(&retry, Duration::from_secs(64))["error"],
                    "rate_limited"
                );
                let stale = CommandRequest {
                    operation_id: Some(format!("{:032x}", 3)),
                    expect_incarnation: status[1].status_id.map(|id| format!("{id:016x}")),
                    ..retry.clone()
                };
                let retry_at = Duration::from_secs(65);
                let denied = f.plane.request_core_restart(
                    &retry,
                    &ControlAccess {
                        role: "viewer".into(),
                        ..Default::default()
                    },
                    f.now + retry_at,
                    false,
                    None,
                );
                assert_eq!(denied.unwrap()["error"], "permission_denied");
                assert_eq!(f.submit(&stale, retry_at)["error"], "incarnation_mismatch");
                assert_eq!(f.submit(&retry, retry_at)["operation"]["state"], "running");
                assert_eq!(
                    f.complete(2, retry_at + Duration::from_secs(5))["operation"]["state"],
                    "succeeded"
                );
                let replaced = f.plane.daemon.core_status(&f.key).unwrap();
                assert!(replaced[0].running);
                assert_ne!(replaced[0].pid, target.pid);
                assert_ne!(replaced[0].status_id, target.status_id);
                assert!(!Path::new(&format!("/proc/{target_pid}")).exists());
                assert_eq!(replaced[1], status[1]);
            }

            #[test]
            fn dropped_reply_does_not_cancel_durable_admission_and_retry_is_actor_bound() {
                let mut f = Fixture::new(false, None);
                let (queue, mut jobs) = tokio::sync::mpsc::channel(4);
                f.plane.core_audit_worker = Some(queue);
                let req = f.request(1, 0, true);
                let (response, reply) = tokio::sync::oneshot::channel();
                f.plane.dispatch_core_restart(
                    &req,
                    &ControlAccess::default(),
                    false,
                    response,
                    json!({}),
                );
                drop(reply);
                let mut job = jobs.try_recv().unwrap();
                f.plane.write_audit(&mut job.record).unwrap();
                assert!(job.completion.unwrap().send((job.record, true)).is_ok());
                let id = req.operation_id.as_deref().unwrap();
                let mut op = f.plane.operations.remove(id).unwrap();
                assert!(f.plane.settle_core_restart_audit(&mut op, f.now));
                f.plane.operations.insert(id.to_owned(), op);
                let other = ControlAccess {
                    subject: "other-operator".into(),
                    ..Default::default()
                };
                assert_eq!(
                    f.plane.request_as(&req, &other)["error"],
                    "operation_id_conflict"
                );
                assert_eq!(
                    f.submit(&req, Duration::ZERO)["operation"]["state"],
                    "running"
                );
                assert_eq!(f.plane.core_restart_history[&(f.key.clone(), 0)].len(), 1);
                assert_eq!(
                    f.complete(1, Duration::ZERO)["operation"]["state"],
                    "succeeded"
                );
            }

            #[test]
            fn audit_replay_queries_unknown_without_repeating_and_restores_budget() {
                let mut f = Fixture::new(false, None);
                let old = f.request(1, 0, true);
                let path = f.dir.join("core-recovery-audit.jsonl");
                let (queue, mut jobs) = tokio::sync::mpsc::channel(4);
                f.plane.core_audit_worker = Some(queue);
                let (response, _reply) = tokio::sync::oneshot::channel();
                f.plane.dispatch_core_restart(
                    &old,
                    &ControlAccess::default(),
                    false,
                    response,
                    json!({}),
                );
                let mut job = jobs.try_recv().unwrap();
                f.plane.write_audit(&mut job.record).unwrap();
                // The original owner never consumes the fsync acknowledgment or closes stdin.
                let mut restarted = Fixture::new(false, None);
                restarted.load_replay(ControlPlane::read_core_recovery_audit(&path).unwrap(), None);
                let target = restarted.plane.daemon.core_status(&restarted.key).unwrap();
                assert_eq!(
                    restarted
                        .plane
                        .request_as(&query(1), &ControlAccess::default())["operation"]["state"],
                    "unknown"
                );
                let denied = ControlAccess {
                    scope: Some(vec![ShardKey {
                        bot_id: "7".into(),
                        shard_id: 1,
                    }]),
                    ..Default::default()
                };
                assert_eq!(restarted.plane.request_as(&query(1), &denied)["ok"], false);
                assert_eq!(
                    restarted.submit(&old, Duration::ZERO)["operation"]["state"],
                    "unknown"
                );
                assert_eq!(
                    restarted.plane.daemon.core_status(&restarted.key).unwrap(),
                    target
                );
                let request = restarted.request(2, 0, true);
                assert_eq!(
                    restarted.submit(&request, Duration::from_millis(59_999))["retry_after_ms"],
                    1
                );
                assert_eq!(
                    restarted.submit(&request, Duration::from_secs(60))["operation"]["state"],
                    "running"
                );
            }

            #[test]
            fn crash_after_reap_before_launch_stays_unknown() {
                let mut f = Fixture::new(false, Some(0));
                let request = f.request(1, 0, true);
                assert_eq!(
                    f.submit(&request, Duration::ZERO)["operation"]["state"],
                    "running"
                );
                assert!(f.plane.daemon.core_status(&f.key).unwrap()[0].pid.is_none());
                let path = f.dir.join("core-recovery-audit.jsonl");
                let archived = ControlPlane::read_core_recovery_audit(&path).unwrap();
                let mut restarted = Fixture::new(false, None);
                restarted.load_replay(archived, None);
                let before = restarted.plane.daemon.core_status(&restarted.key).unwrap();
                let result = restarted.submit(&request, Duration::ZERO);
                assert_eq!(result["operation"]["state"], "unknown");
                assert_eq!(
                    restarted.plane.daemon.core_status(&restarted.key).unwrap(),
                    before
                );
            }

            #[test]
            fn audit_replay_charges_each_operation_id_once_in_the_rolling_window() {
                let mut f = Fixture::new(false, None);
                for id in 1..=2 {
                    let request = f.request(id, 0, true);
                    let at = Duration::from_secs((id as u64 - 1) * 60);
                    assert_eq!(f.submit(&request, at)["operation"]["state"], "running");
                    assert_eq!(f.complete(id, at)["operation"]["state"], "succeeded");
                }
                let path = f.dir.join("core-recovery-audit.jsonl");
                let data = std::fs::read_to_string(&path).unwrap();
                use std::io::Write;
                let mut file = std::fs::OpenOptions::new()
                    .append(true)
                    .open(&path)
                    .unwrap();
                writeln!(file, "{}", data.lines().next().unwrap()).unwrap();
                file.sync_data().unwrap();
                let archived = ControlPlane::read_core_recovery_audit(&path).unwrap();
                assert!(!archived.invalid);
                let mut restarted = Fixture::new(false, None);
                restarted.load_replay(archived, None);
                assert_eq!(
                    restarted.plane.core_restart_history[&(restarted.key.clone(), 0)].len(),
                    2
                );
                let third = restarted.request(3, 0, true);
                assert_eq!(
                    restarted.submit(&third, Duration::from_secs(60))["operation"]["state"],
                    "running"
                );
                assert_eq!(
                    restarted.complete(3, Duration::from_secs(60))["operation"]["state"],
                    "succeeded"
                );
                let request = restarted.request(4, 0, true);
                assert_eq!(
                    restarted.submit(&request, Duration::from_secs(120))["error"],
                    "rate_limited"
                );
            }

            #[test]
            fn replay_ages_written_charges_only_with_a_matching_monotonic_clock() {
                let mut f = Fixture::new(false, None);
                f.plane.core_audit_boot_id = Some(ControlPlane::boot_id().unwrap());
                let req = f.request(1, 0, true);
                assert_eq!(f.submit(&req, Duration::ZERO)["ok"], true);
                assert_eq!(f.complete(1, Duration::ZERO)["ok"], true);
                let path = f.dir.join("core-recovery-audit.jsonl");
                let data = std::fs::read_to_string(&path).unwrap();
                let mut rows: Vec<Value> = data
                    .lines()
                    .map(|line| serde_json::from_str(line).unwrap())
                    .collect();
                rows[0]["ts"] = json!(1);
                rows[1]["ts"] = json!(1);
                std::fs::write(&path, format!("{}\n{}\n{}\n", rows[0], rows[1], rows[1])).unwrap();
                let replay = ControlPlane::read_core_recovery_audit(&path).unwrap();
                assert!(!replay.invalid);
                let terminal = replay
                    .archived
                    .values()
                    .next()
                    .unwrap()
                    .terminal
                    .as_ref()
                    .unwrap();
                let stamp = terminal.charge_clock.as_ref().unwrap();
                let key = (f.key.clone(), 0);
                let now = f.now + CORE_RESTART_WINDOW;
                for clock in [
                    None,
                    Some(("different boot", stamp.boot_ms + 999_999)),
                    Some((stamp.boot_id.as_str(), stamp.boot_ms.saturating_sub(1))),
                ] {
                    let budget = ControlPlane::replayed_core_restart_budget(&replay, now, clock);
                    assert_eq!(budget[&key].iter().copied().collect::<Vec<_>>(), [now]);
                }
                let clock = Some((stamp.boot_id.as_str(), stamp.boot_ms + 30_000));
                let budget = ControlPlane::replayed_core_restart_budget(&replay, now, clock);
                assert_eq!(
                    budget[&key].iter().copied().collect::<Vec<_>>(),
                    [now - Duration::from_secs(30)]
                );
                let clock = Some((stamp.boot_id.as_str(), stamp.boot_ms + 900_000));
                assert!(ControlPlane::replayed_core_restart_budget(&replay, now, clock).is_empty());
            }

            #[test]
            fn replay_reserves_orphan_and_rejected_ids_without_returning_foreign_data() {
                let mut f = Fixture::new(false, None);
                let req = f.request(1, 0, true);
                assert_eq!(f.submit(&req, Duration::ZERO)["ok"], true);
                assert_eq!(f.complete(1, Duration::ZERO)["ok"], true);
                let path = f.dir.join("core-recovery-audit.jsonl");
                let data = std::fs::read_to_string(&path).unwrap();
                let mut rows = data.lines();
                let requested: Value = serde_json::from_str(rows.next().unwrap()).unwrap();
                let terminal: Value = serde_json::from_str(rows.next().unwrap()).unwrap();
                let mut rejected = requested.clone();
                rejected["operation_id"] = json!(format!("{:032x}", 2));
                rejected["event"] = json!("rejected");
                rejected["reason"] = json!("busy");
                let torn = format!("{{\"operation_id\":\"{:032x}\",\"reason\":", 3);
                std::fs::write(&path, format!("{terminal}\n{rejected}\n{torn}")).unwrap();
                let mut restarted = Fixture::new(false, None);
                let replay = ControlPlane::read_core_recovery_audit(&path).unwrap();
                assert!(!replay.invalid);
                assert_eq!(replay.orphaned.len(), 1);
                restarted.load_replay(replay, None);
                for id in 1..=3 {
                    let request = restarted.request(id, 0, true);
                    assert!(
                        restarted
                            .plane
                            .operation_exists(request.operation_id.as_deref().unwrap())
                    );
                    assert!(
                        restarted
                            .plane
                            .request_as(&query(id), &ControlAccess::default())
                            .get("operation")
                            .is_none()
                    );
                    assert_eq!(
                        restarted.submit(&request, Duration::ZERO)["error"],
                        "operation_id_conflict"
                    );
                }

                let mut foreign_incarnation = requested.clone();
                foreign_incarnation["incarnation"] = json!("/foreign/incarnation");
                let mut foreign_reason = terminal.clone();
                foreign_reason["event"] = json!("failed");
                foreign_reason["reason"] = json!("/foreign/reason");
                let mut conflicting_admission = requested.clone();
                conflicting_admission["force"] = json!(false);
                let mut conflicting_terminal = terminal.clone();
                conflicting_terminal["event"] = json!("failed");
                conflicting_terminal["reason"] = json!("launch_failed");
                for text in [
                    format!("{foreign_incarnation}\n"),
                    format!("{requested}\n{foreign_reason}\n"),
                    format!("{requested}\n{conflicting_admission}\n"),
                    format!("{requested}\n{terminal}\n{conflicting_terminal}\n"),
                ] {
                    std::fs::write(&path, text).unwrap();
                    let replay = ControlPlane::read_core_recovery_audit(&path).unwrap();
                    assert!(replay.invalid);
                    assert!(
                        replay
                            .reserved
                            .contains(req.operation_id.as_deref().unwrap())
                    );
                    restarted.load_replay(replay, None);
                    let reply = restarted
                        .plane
                        .request_as(&query(1), &ControlAccess::default());
                    assert!(reply.get("operation").is_none());
                    assert!(!reply.to_string().contains("/foreign/"));
                    let fresh = restarted.request(3, 0, true);
                    assert_eq!(
                        restarted.submit(&fresh, Duration::ZERO)["error"],
                        "audit_unavailable"
                    );
                }
            }

            #[tokio::test]
            async fn supervision_runs_during_stalled_replay_and_worker_shutdown_is_bounded() {
                let mut f = Fixture::new(false, None);
                let path = f.dir.join("core-recovery-audit.jsonl");
                let (entered, reading) = tokio::sync::oneshot::channel();
                let (resume, release) = std::sync::mpsc::channel();
                f.plane
                    .start_core_recovery_audit_with(path, move |path| {
                        let _ = entered.send(());
                        let _ = release.recv_timeout(Duration::from_secs(5));
                        ControlPlane::read_core_recovery_audit(path)
                    })
                    .unwrap();
                tokio::time::timeout(Duration::from_secs(2), reading)
                    .await
                    .unwrap()
                    .unwrap();
                let request = f.request(1, 0, true);
                let (send, mut reply) = tokio::sync::oneshot::channel();
                f.plane.dispatch_core_restart(
                    &request,
                    &ControlAccess::default(),
                    false,
                    send,
                    json!({}),
                );
                let response = reply.try_recv().unwrap();
                assert_eq!(response["error"], "audit_replay_pending");
                assert!(response["retry_after_ms"].as_u64().unwrap() > 0);
                assert!(f.plane.operations.is_empty());
                assert!(f.plane.core_audit_rejections.is_empty());
                assert_eq!(
                    f.plane.request(&CommandRequest {
                        command: "status".into(),
                        ..Default::default()
                    })["status"]["core_recovery_audit"]["available"],
                    false
                );
                f.stop_gateway();
                tokio::time::timeout(Duration::from_secs(1), f.plane.tick(&no_credentials))
                    .await
                    .unwrap();
                assert_ne!(
                    f.plane.daemon.entries[&f.key].snapshot.state,
                    ProcessState::Running
                );
                assert!(
                    !f.plane
                        .shutdown_control_audits_with_timeout(Duration::from_millis(20))
                        .await
                );
                assert!(f.plane.core_audit_worker.is_none());
                resume.send(()).unwrap();
                tokio::time::timeout(Duration::from_secs(2), &mut f.plane.audit_worker_exits[0])
                    .await
                    .unwrap()
                    .unwrap();
            }

            #[tokio::test]
            async fn archived_id_cannot_be_acquired_before_replay_publishes_it() {
                let mut old = Fixture::new(false, None);
                let archived = old.request(1, 0, true);
                assert_eq!(old.submit(&archived, Duration::ZERO)["ok"], true);
                assert_eq!(
                    old.complete(1, Duration::ZERO)["operation"]["state"],
                    "succeeded"
                );
                let id = archived.operation_id.unwrap();
                let stop = CommandRequest {
                    command: "stop".into(),
                    bot: archived.bot,
                    operation_id: Some(id.clone()),
                    shards: Some(vec![0]),
                    ..Default::default()
                };
                let mut f = Fixture::new(false, None);
                let mut updates = UpdateHub::new(
                    f.plane.config.clone(),
                    vec![f.key.clone()],
                    f.dir.join("update-audit.jsonl"),
                );
                let (entered, reading) = tokio::sync::oneshot::channel();
                let (resume, release) = std::sync::mpsc::channel();
                f.plane
                    .start_core_recovery_audit_with(
                        old.dir.join("core-recovery-audit.jsonl"),
                        move |path| {
                            entered.send(()).unwrap();
                            release.recv_timeout(Duration::from_secs(5)).unwrap();
                            ControlPlane::read_core_recovery_audit(path)
                        },
                    )
                    .unwrap();
                tokio::time::timeout(Duration::from_secs(2), reading)
                    .await
                    .unwrap()
                    .unwrap();
                assert!(!f.plane.operation_exists(&id));
                let access = ControlAccess::default();
                let rejected = f.plane.request_as(&stop, &access);
                assert_eq!(rejected["error"], "audit_replay_pending");
                assert!(rejected["retry_after_ms"].as_u64().unwrap() > 0);
                let rejected = f.plane.dispatch_manifest_update(
                    &mut updates,
                    manifest_update(&id),
                    access.clone(),
                );
                assert_eq!(rejected["error"], "audit_replay_pending");
                assert!(!updates.contains(&id));
                assert!(f.plane.operations.is_empty());
                resume.send(()).unwrap();
                tokio::time::timeout(Duration::from_secs(2), async {
                    while f.plane.core_audit_replay.is_some() {
                        f.plane.tick(&no_credentials).await;
                        tokio::task::yield_now().await;
                    }
                })
                .await
                .unwrap();
                assert!(f.plane.operation_exists(&id));
                assert_eq!(
                    f.plane.request_as(&stop, &access)["error"],
                    "operation ID conflicts with an earlier request"
                );
                let archive = f.plane.request_as(&query(1), &access);
                assert_eq!(archive["operation"]["source"], "archive");
                assert_eq!(archive["operation"]["state"], "succeeded");
                assert!(f.plane.shutdown_control_audits().await);
            }

            #[tokio::test]
            async fn stalled_launch_releases_the_shard_and_shutdown_waits_for_child_reaping() {
                for expire in [false, true] {
                    let mut f = Fixture::new(false, None);
                    let request = f.request(1, 0, true);
                    let pid = f.plane.daemon.core_status(&f.key).unwrap()[0].pid.unwrap();
                    let late = f.dir.join("late-core");
                    std::fs::write(
                        &late,
                        "#!/bin/sh\nif IFS= read -r secret; then : > \"$2.capability\"; fi\nexec /bin/sleep 30\n",
                    ).unwrap();
                    std::fs::set_permissions(&late, std::fs::Permissions::from_mode(0o700))
                        .unwrap();
                    let entry = f.plane.daemon.entries.get_mut(&f.key).unwrap();
                    entry.spec.gateway.program = late.into_os_string();
                    let cores = entry.cores.as_mut().unwrap();
                    let marker = crate::core_attach::socket_path(cores.dir(), 0)
                        .with_extension("sock.capability");
                    let (spawned, resume) = cores.pause_next_manual_launch();
                    assert_eq!(f.submit(&request, Duration::ZERO)["ok"], true);
                    let id = request.operation_id.unwrap();
                    let mut op = f.plane.operations.remove(&id).unwrap();
                    let until = Instant::now() + Duration::from_secs(2);
                    let late_pid = loop {
                        f.plane.advance_core_restart(&mut op, f.now);
                        assert!(op.finished.is_none());
                        if let Ok(pid) = spawned.try_recv() {
                            break pid;
                        }
                        assert!(Instant::now() < until);
                        std::thread::yield_now();
                    };
                    assert!(!Path::new(&format!("/proc/{pid}")).exists());
                    assert!(Path::new(&format!("/proc/{late_pid}")).exists());
                    assert!(!marker.exists());
                    if expire {
                        let stop = CommandRequest {
                            command: "stop".into(),
                            bot: request.bot,
                            operation_id: Some(format!("{:032x}", 2)),
                            shards: Some(vec![0]),
                            force: true,
                            yes: true,
                            ..Default::default()
                        };
                        f.plane
                            .advance_core_restart(&mut op, f.now + Duration::from_millis(4_999));
                        assert!(op.finished.is_none());
                        f.plane.operations.insert(id.clone(), op);
                        assert_eq!(f.plane.request(&stop)["error"], "busy");
                        op = f.plane.operations.remove(&id).unwrap();
                        f.plane
                            .advance_core_restart(&mut op, f.now + Duration::from_secs(5));
                        assert_eq!(
                            f.plane.operation_view(&op)["operation"]["reason"],
                            "launch_timeout"
                        );
                        f.plane.operations.insert(id.clone(), op);
                        assert_eq!(f.plane.request(&stop)["ok"], true);
                        let replay = ControlPlane::read_core_recovery_audit(
                            &f.dir.join("core-recovery-audit.jsonl"),
                        )
                        .unwrap();
                        assert!(!replay.invalid);
                        assert_eq!(replay.archived[&id].state(), "failed");
                    } else {
                        f.plane.operations.insert(id.clone(), op);
                    }
                    assert!(
                        !f.plane
                            .shutdown_core_launches_with_timeout(Duration::from_millis(20))
                            .await
                    );
                    if !expire {
                        assert_eq!(
                            f.plane.request(&query(1))["operation"]["reason"],
                            "daemon_shutdown"
                        );
                    }
                    assert!(Path::new(&format!("/proc/{late_pid}")).exists());
                    resume.send(()).unwrap();
                    assert!(f.plane.shutdown_core_launches().await);
                    assert!(!Path::new(&format!("/proc/{late_pid}")).exists());
                    assert!(!marker.exists());
                }
            }

            #[tokio::test]
            async fn gateway_audit_rejections_are_canonical_and_admission_waits_for_sync() {
                let mut f = Fixture::new(false, None);
                let core = f.request(1, 0, true);
                assert_eq!(f.submit(&core, Duration::ZERO)["ok"], true);
                let (writer, mut jobs) = tokio::sync::mpsc::channel(8);
                f.plane.gateway_audit_worker = Some(writer);
                let request = CommandRequest {
                    command: "gateway-update".into(),
                    bot: core.bot,
                    operation_id: Some(format!("{:032x}", 2)),
                    shards: Some(vec![0]),
                    ..Default::default()
                };
                for (selector, expected) in [
                    (request.bot.clone(), json!("7")),
                    (Some("/secret/selector".into()), Value::Null),
                ] {
                    let (response, mut reply) = tokio::sync::oneshot::channel();
                    f.plane.dispatch_gateway_update(
                        CommandRequest {
                            bot: selector,
                            ..request.clone()
                        },
                        &ControlAccess::default(),
                        false,
                        response,
                        json!({}),
                    );
                    assert_eq!(
                        reply.try_recv().unwrap()["error"],
                        if expected.is_null() {
                            "bot not found"
                        } else {
                            "busy"
                        }
                    );
                    let job = jobs.try_recv().unwrap();
                    assert!(job.completion.is_none());
                    let row: Value = serde_json::from_slice(&job.record.bytes).unwrap();
                    assert_eq!(row["event"], "rejected");
                    assert_eq!(row["bot"], expected);
                    assert!(!row.to_string().contains("selector"));
                    assert!(jobs.try_recv().is_err());
                }
                assert_eq!(f.complete(1, Duration::ZERO)["ok"], true);
                let mut updates = UpdateHub::new(
                    f.plane.config.clone(),
                    vec![f.key.clone()],
                    f.dir.join("update-audit.jsonl"),
                );
                for synced in [false, true] {
                    let (response, mut reply) = tokio::sync::oneshot::channel();
                    f.plane.dispatch_gateway_update(
                        request.clone(),
                        &ControlAccess::default(),
                        false,
                        response,
                        json!({}),
                    );
                    let job = jobs.try_recv().unwrap();
                    let row: Value = serde_json::from_slice(&job.record.bytes).unwrap();
                    assert_eq!(row["event"], "requested");
                    assert_eq!(row["bot"], "7");
                    let id = request.operation_id.as_deref().unwrap();
                    let now =
                        f.plane.pending_gateway_updates[0].deadline - Duration::from_millis(1);
                    f.plane.settle_gateway_audits(now);
                    assert!(f.plane.operation_exists(id));
                    assert!(!f.plane.operations.contains_key(id));
                    let conflict = f.plane.dispatch_manifest_update(
                        &mut updates,
                        manifest_update(id),
                        ControlAccess::default(),
                    );
                    assert_eq!(
                        conflict["error"],
                        "operation ID conflicts with an earlier request"
                    );
                    assert!(!updates.contains(id));
                    assert!(reply.try_recv().is_err());
                    assert!(job.completion.unwrap().send((job.record, synced)).is_ok());
                    f.plane.settle_gateway_audits(now);
                    assert_eq!(reply.try_recv().unwrap()["ok"], synced);
                    assert_eq!(f.plane.operation_exists(id), synced);
                    if !synced {
                        assert!(jobs.try_recv().unwrap().completion.is_none());
                    }
                }
            }

            #[tokio::test]
            async fn core_audit_status_exposes_rejection_drops_disk_failures_and_worker_state() {
                let mut f = Fixture::new(false, None);
                let mut req = f.request(1, 0, true);
                req.core = Some(crate::core_attach::SLOTS as u32);
                let status = CommandRequest {
                    command: "status".into(),
                    ..Default::default()
                };
                let (writer, jobs) = tokio::sync::mpsc::channel(1);
                f.plane.core_audit_worker = Some(writer);
                assert_eq!(f.submit(&req, Duration::ZERO)["error"], "core_out_of_range");
                let health = f.plane.request(&status)["status"]["core_recovery_audit"].clone();
                assert_eq!(health["ok"], false);
                assert_eq!(health["pending"], 1);
                assert_eq!(health["failed_or_dropped"], 0);
                assert_eq!(f.submit(&req, Duration::ZERO)["error"], "core_out_of_range");
                let health = f.plane.request(&status)["status"]["core_recovery_audit"].clone();
                assert_eq!(health["ok"], false);
                assert_eq!(health["available"], true);
                assert_eq!(health["failed_or_dropped"], 1);
                drop(jobs);
                f.plane.retry_pending_audit();
                assert_eq!(f.submit(&req, Duration::ZERO)["error"], "core_out_of_range");
                let health = f.plane.request(&status)["status"]["core_recovery_audit"].clone();
                assert_eq!(health["available"], false);
                assert_eq!(health["pending"], 0);
                assert_eq!(health["failed_or_dropped"], 3);

                let path = f.dir.join("core-recovery-audit.jsonl");
                f.plane.start_core_recovery_audit(path.clone()).unwrap();
                let until = Instant::now() + Duration::from_secs(2);
                while !f.plane.core_audit_ready {
                    f.plane.poll_core_audit_replay();
                    assert!(Instant::now() < until);
                    tokio::task::yield_now().await;
                }
                std::fs::remove_file(&path).unwrap();
                std::fs::create_dir(&path).unwrap();
                assert_eq!(f.submit(&req, Duration::ZERO)["error"], "core_out_of_range");
                loop {
                    f.plane.retry_pending_audit();
                    let health = f.plane.request(&status)["status"]["core_recovery_audit"].clone();
                    if health["failed_or_dropped"] == 4 {
                        assert_eq!(health["ok"], false);
                        assert_eq!(health["available"], true);
                        assert_eq!(health["pending"], 0);
                        break;
                    }
                    assert!(Instant::now() < until);
                    tokio::task::yield_now().await;
                }
                assert!(f.plane.shutdown_control_audits().await);
                let mut restarted = Fixture::new(false, None);
                restarted.plane.start_core_recovery_audit(path).unwrap();
                let until = Instant::now() + Duration::from_secs(2);
                while restarted.plane.core_audit_replay.is_some() {
                    restarted.plane.poll_core_audit_replay();
                    assert!(Instant::now() < until);
                    tokio::task::yield_now().await;
                }
                let health =
                    restarted.plane.request(&status)["status"]["core_recovery_audit"].clone();
                assert_eq!(health["failed_or_dropped"], 0);
                assert_eq!(health["pending"], 0);
                assert_eq!(health["ok"], false);
                assert_eq!(health["available"], false);
                assert!(restarted.plane.shutdown_control_audits().await);
            }

            #[tokio::test]
            async fn shutdown_durably_cancels_an_admission_without_consuming_it_as_an_action() {
                let mut f = Fixture::new(false, None);
                let path = f.dir.join("core-recovery-audit.jsonl");
                f.plane.start_core_recovery_audit(path.clone()).unwrap();
                let until = Instant::now() + Duration::from_secs(2);
                while !f.plane.core_audit_ready {
                    f.plane.poll_core_audit_replay();
                    assert!(Instant::now() < until);
                    tokio::task::yield_now().await;
                }
                let req = f.request(1, 0, true);
                let before = f.plane.daemon.core_status(&f.key).unwrap();
                let (response, mut reply) = tokio::sync::oneshot::channel();
                f.plane.dispatch_core_restart(
                    &req,
                    &ControlAccess::default(),
                    false,
                    response,
                    json!({}),
                );
                let id = req.operation_id.as_deref().unwrap();
                assert!(
                    f.plane.operations[id]
                        .core_restart
                        .as_ref()
                        .unwrap()
                        .pending
                        .is_some()
                );
                assert!(f.plane.shutdown_control_audits().await);
                assert_eq!(reply.try_recv().unwrap()["error"], "daemon_shutdown");
                assert_eq!(f.plane.daemon.core_status(&f.key).unwrap(), before);
                let replay = ControlPlane::read_core_recovery_audit(&path).unwrap();
                assert!(!replay.invalid);
                assert_eq!(replay.archived[id].state(), "failed");
                assert_eq!(
                    ControlPlane::archived_core_view(&replay.archived[id])["operation"]["reason"],
                    "daemon_shutdown"
                );
            }

            #[test]
            fn preview_is_shard_scoped_and_never_audits_the_client_selector() {
                let mut f = Fixture::new(false, None);
                let own = ControlAccess {
                    scope: Some(vec![f.key.clone()]),
                    ..Default::default()
                };
                let mut preview = CommandRequest {
                    command: "core-preview".into(),
                    bot: Some("private-selector".into()),
                    shard: Some(0),
                    ..Default::default()
                };
                let view = f.plane.request_as(&preview, &own);
                assert_eq!(view["ok"], true);
                assert_eq!(view["shard"], 0);
                assert_eq!(view["total_shards"], 2);
                assert_eq!(view["cores"][0]["status_reason"], "unsupported");
                assert_eq!(view["cores"][0]["empty"], false);
                assert!(view["cores"][0]["incarnation"].is_string());
                let action = f.request(1, 0, true);
                for role in ["viewer", "updater"] {
                    let access = ControlAccess {
                        role: role.into(),
                        ..own.clone()
                    };
                    assert_eq!(f.plane.request_as(&preview, &access)["ok"], true);
                    assert_eq!(
                        f.plane.request_as(&action, &access)["error"],
                        "permission_denied"
                    );
                }
                preview.shard = Some(1);
                assert_eq!(f.plane.request_as(&preview, &own)["ok"], false);
                let mut restart = f.request(1, 0, true);
                restart.bot = Some("/client/selector-sentinel".into());
                assert_eq!(f.submit(&restart, Duration::ZERO)["error"], "bot_not_found");
                let lines =
                    std::fs::read_to_string(f.dir.join("core-recovery-audit.jsonl")).unwrap();
                assert!(!lines.contains("selector-sentinel"));
                assert!(lines.contains("permission_denied"));
                assert!(lines.contains("bot_not_found"));
            }

            #[test]
            fn audit_contains_only_validated_fields_and_retries_outcome() {
                let mut f = Fixture::new(true, None);
                let req = f.request(1, 0, true);
                let path = f.dir.join("core-recovery-audit.jsonl");
                let before = f.plane.daemon.core_status(&f.key).unwrap();
                assert_eq!(
                    f.submit(&req, Duration::ZERO)["operation"]["state"],
                    "running"
                );
                let saved = path.with_extension("saved");
                std::fs::rename(&path, &saved).unwrap();
                std::fs::create_dir(&path).unwrap();
                let value = f.complete(1, Duration::ZERO);
                assert_eq!(value["operation"]["state"], "succeeded");
                assert_eq!(value["note"], CORE_RESTART_NOTE);
                assert_eq!(value["operation"]["terminal_audit"], "pending_or_failed");
                let archived = ControlPlane::read_core_recovery_audit(&saved).unwrap();
                let id = req.operation_id.as_deref().unwrap();
                assert_eq!(archived.archived[id].state(), "unknown");
                let after = f.plane.daemon.core_status(&f.key).unwrap();
                assert_ne!(after[0].status_id, before[0].status_id);
                assert_eq!(after[1], before[1]);
                assert_eq!(f.plane.pending_audit.len(), 1);
                std::fs::remove_dir(&path).unwrap();
                std::fs::rename(saved, &path).unwrap();
                f.plane.retry_pending_audit();
                assert!(f.plane.pending_audit.is_empty());
                assert_eq!(
                    f.plane.request(&query(1))["operation"]["terminal_audit"],
                    "synced"
                );
                let archived = ControlPlane::read_core_recovery_audit(&path).unwrap();
                assert_eq!(archived.archived[id].state(), "succeeded");
                let wrong = CommandRequest {
                    expect_bot_id: Some("8".into()),
                    ..req.clone()
                };
                assert_eq!(f.submit(&wrong, Duration::ZERO)["error"], "bot_id_mismatch");
                let text = std::fs::read_to_string(&path).unwrap();
                let rows: Vec<Value> = text
                    .lines()
                    .map(|line| serde_json::from_str(line).unwrap())
                    .collect();
                assert_eq!(rows.len(), 3);
                for (row, event) in rows.iter().zip(["requested", "succeeded", "rejected"]) {
                    assert_eq!(row.as_object().unwrap().len(), 13);
                    assert!(row["ts"].as_u64().unwrap() > 0);
                    assert_eq!(row["event"], event);
                    assert_eq!(row["bot_id"], "7");
                    assert_eq!(row["subject"], "local-bootstrap");
                    assert_eq!(row["role"], "admin");
                    assert_eq!(row["shard"], 0);
                    assert_eq!(row["core"], 0);
                    assert_eq!(row["force"], true);
                    assert_eq!(row["operation_id"].as_str(), req.operation_id.as_deref());
                    assert_eq!(
                        row["incarnation"].as_str(),
                        req.expect_incarnation.as_deref()
                    );
                }
                assert_eq!(rows[2]["reason"], "bot_id_mismatch");
                let secret = std::fs::read_to_string(crate::core_attach::secret_path(
                    f.plane.daemon.entries[&f.key].cores.as_ref().unwrap().dir(),
                ))
                .unwrap();
                for forbidden in [
                    "PRIVATE_TOKEN_ENV",
                    "private-selector",
                    "private-program",
                    "core-listen",
                    secret.trim(),
                    f.dir.to_str().unwrap(),
                ] {
                    assert!(!text.contains(forbidden));
                }
                assert_eq!(
                    std::fs::metadata(path).unwrap().permissions().mode() & 0o777,
                    0o600
                );
            }
        }

        fn no_credentials(_: &str) -> Option<OsString> {
            panic!("read credentials without a launch");
        }

        #[test]
        fn filtered_force_stop_preserves_another_shards_restart() {
            let mut plane = plane(1, 2);
            let mut restart = request("restart", 0, 1);
            restart.shards = Some(vec![1]);
            assert_eq!(plane.request(&restart)["operation"]["state"], "running");
            let before = serde_json::to_value(plane.daemon.snapshots()).unwrap();
            let key = |shard_id| ShardKey {
                bot_id: "id-0".into(),
                shard_id,
            };
            assert!(plane.daemon.entries[&key(1)].restart_held);
            assert!(!plane.daemon.entries[&key(0)].restart_held);

            let mut stop = request("stop", 0, 2);
            stop.shards = Some(vec![0]);
            stop.force = true;
            stop.yes = true;
            let refused = plane.request(&stop);
            assert_eq!(refused["ok"], false);
            assert_eq!(
                refused["error"],
                "another operation is running on other shards of this bot"
            );
            assert_eq!(plane.request(&query(1))["operation"]["state"], "running");
            assert_eq!(plane.operations.len(), 1);
            assert!(plane.daemon.entries[&key(1)].restart_held);
            assert!(!plane.daemon.entries[&key(0)].restart_held);
            assert_eq!(
                serde_json::to_value(plane.daemon.snapshots()).unwrap(),
                before
            );

            // Whole-Bot force stop retains its existing supersession behavior.
            stop.shards = None;
            assert_eq!(plane.request(&stop)["ok"], true);
            assert_eq!(plane.request(&query(1))["operation"]["state"], "failed");
        }

        #[test]
        fn scoped_operation_queries_use_the_operations_targets() {
            use super::super::update_control::ControlAccess;
            let mut plane = plane(1, 2);
            let access = |shard_id| ControlAccess {
                subject: "scoped-admin".into(),
                role: "admin".into(),
                scope: Some(vec![ShardKey {
                    bot_id: "id-0".into(),
                    shard_id,
                }]),
            };
            let mut restart = request("restart", 0, 1);
            restart.shards = Some(vec![1]);
            assert_eq!(plane.request_as(&restart, &access(1))["ok"], true);
            let own = plane.request_as(&query(1), &access(1));
            assert_eq!(own["ok"], true);
            assert_eq!(own["operation"]["shards"].as_array().unwrap().len(), 1);
            assert_eq!(plane.request_as(&query(1), &access(0))["ok"], false);

            // A matching shard is insufficient if the operation also covers another shard.
            let mut whole = request("stop", 0, 2);
            whole.force = true;
            whole.yes = true;
            assert_eq!(plane.request(&whole)["ok"], true);
            assert_eq!(plane.request_as(&query(2), &access(1))["ok"], false);
            // Completed operation snapshots retain their original authorization scope.
            assert_eq!(
                plane.request_as(&query(1), &access(1))["operation"]["state"],
                "failed"
            );
        }

        #[test]
        fn shard_count_precondition_is_only_valid_for_lifecycle_mutations() {
            for command in ["start", "stop", "restart", "gateway-update"] {
                let mut command = request(command, 0, 1);
                command.expect_total_shards = Some(2);
                assert!(command.valid());
                command.expect_total_shards = Some(0);
                assert!(!command.valid());
            }
            for command in [
                "ps",
                "status",
                "inspect",
                "shards",
                "operation",
                "gateway-prepare",
            ] {
                let mut command = match command {
                    "operation" => query(1),
                    "gateway-prepare" => request(command, 0, 1),
                    _ => CommandRequest {
                        command: command.into(),
                        bot: matches!(command, "inspect" | "shards").then(|| "bot0".into()),
                        ..Default::default()
                    },
                };
                assert!(command.valid());
                command.expect_total_shards = Some(2);
                assert!(!command.valid());
            }
        }

        #[test]
        fn stale_shard_count_is_refused_before_supersession_or_restart_holds() {
            let mut plane = plane(1, 2);
            let mut restart = request("restart", 0, 1);
            restart.shards = Some(vec![1]);
            restart.expect_total_shards = Some(1);
            let refused = plane.request(&restart);
            assert_eq!(refused["ok"], false);
            assert_eq!(
                refused["error"],
                "bot total_shards changed; resolve guilds again before retrying"
            );
            assert!(plane.operations.is_empty());
            assert!(
                plane
                    .daemon
                    .entries
                    .values()
                    .all(|entry| !entry.restart_held)
            );
            restart.expect_total_shards = Some(2);
            assert_eq!(plane.request(&restart)["ok"], true);

            let mut stop = request("stop", 0, 2);
            stop.force = true;
            stop.yes = true;
            stop.expect_total_shards = Some(1);
            assert_eq!(plane.request(&stop)["ok"], false);
            assert_eq!(plane.operations.len(), 1);
            assert_eq!(plane.request(&query(1))["operation"]["state"], "running");
        }

        #[test]
        fn bot_identity_precondition_is_only_valid_for_lifecycle_mutations() {
            for command in ["start", "stop", "restart", "gateway-update"] {
                let mut command = request(command, 0, 1);
                command.expect_bot_id = Some("id-0".into());
                assert!(command.valid());
                for invalid in ["".to_owned(), " id-0".into(), "x\n".into(), "x".repeat(65)] {
                    command.expect_bot_id = Some(invalid);
                    assert!(!command.valid());
                }
            }
            for name in [
                "ps",
                "status",
                "inspect",
                "shards",
                "operation",
                "gateway-prepare",
            ] {
                let mut command = match name {
                    "operation" => query(1),
                    "gateway-prepare" => request(name, 0, 1),
                    _ => CommandRequest {
                        command: name.into(),
                        bot: matches!(name, "inspect" | "shards").then(|| "bot0".into()),
                        ..Default::default()
                    },
                };
                assert!(command.valid());
                command.expect_bot_id = Some("id-0".into());
                assert!(!command.valid());
            }
        }

        #[test]
        fn changed_bot_identity_is_refused_before_supersession_or_restart_holds() {
            let mut plane = plane(1, 2);
            // Both shards must be reservable for this identity precondition test.
            plane
                .daemon
                .set_local_identify_policy(crate::local_identify::LocalIdentifyPolicy {
                    min_interval: Duration::ZERO,
                    ..Default::default()
                });
            let before = plane.daemon.snapshots();
            let mut restart = request("restart", 0, 1);
            // The selector still resolves, but now to a different identity from the listing.
            restart.expect_bot_id = Some("removed-bot".into());
            assert_eq!(
                plane.request(&restart)["error"],
                "bot identity changed; refresh the bot list before retrying"
            );
            assert!(plane.operations.is_empty());
            assert_eq!(plane.daemon.snapshots(), before);
            assert!(
                plane
                    .daemon
                    .entries
                    .values()
                    .all(|entry| !entry.restart_held)
            );

            restart.expect_bot_id = Some("id-0".into());
            assert_eq!(plane.request(&restart)["ok"], true);
            let mut stop = request("stop", 0, 2);
            stop.force = true;
            stop.yes = true;
            stop.expect_bot_id = Some("removed-bot".into());
            assert_eq!(plane.request(&stop)["ok"], false);
            assert_eq!(plane.operations.len(), 1);
            assert_eq!(plane.request(&query(1))["operation"]["state"], "running");
        }

        #[test]
        fn matching_bot_identity_does_not_bypass_restart_identify_spacing() {
            let mut plane = plane(1, 2);
            let before = plane.daemon.snapshots();
            let mut restart = request("restart", 0, 1);
            restart.expect_bot_id = Some("id-0".into());

            let refused = plane.request(&restart);
            assert_eq!(refused["ok"], false);
            assert_eq!(refused["error"], "IdentifyDeferred");
            assert!(plane.operations.is_empty());
            assert_eq!(plane.daemon.snapshots(), before);
            assert!(
                plane
                    .daemon
                    .entries
                    .values()
                    .all(|entry| !entry.restart_held)
            );
        }

        #[test]
        fn rolling_commands_cannot_mutate_the_default_control_plane() {
            let mut plane = plane(1, 1);
            let before = plane.daemon.snapshots();
            for command in ["gateway-prepare", "gateway-update"] {
                let response = plane.request(&request(command, 0, 1));
                assert_eq!(response["ok"], false);
                assert_eq!(
                    response["error"],
                    "Gateway switch requires the explicit mock runtime"
                );
                assert!(plane.operations.is_empty());
                assert_eq!(plane.daemon.snapshots(), before);
            }
        }

        #[test]
        fn configuring_nonce_retry_replaces_the_previous_allowlist() {
            use crate::discord_message::NONCE_RETRY_FLAG;

            let mut plane = plane(2, 2);
            for enabled in ["id-0", "id-1", "id-1"] {
                plane
                    .daemon
                    .configure_create_message_nonce_retry(
                        &plane.config,
                        &HashSet::from([enabled.to_owned()]),
                    )
                    .unwrap();
                for entry in plane.daemon.entries.values() {
                    let mut command = Command::new(&entry.spec.gateway.program);
                    entry.add_gateway_args(&mut command);
                    let count = command
                        .as_std()
                        .get_args()
                        .filter(|arg| *arg == NONCE_RETRY_FLAG)
                        .count();
                    assert_eq!(count, (entry.spec.bot_id == enabled) as usize);
                }
            }
        }

        #[test]
        fn configured_environment_cannot_override_windows_system_root() {
            let plane = plane(1, 1);
            for role in [Role::Gateway, Role::Core] {
                let mut specs: Vec<_> = plane
                    .daemon
                    .entries
                    .values()
                    .map(|entry| entry.spec.clone())
                    .collect();
                let process = if role == Role::Gateway {
                    &mut specs[0].gateway
                } else {
                    &mut specs[0].core
                };
                process
                    .environment
                    .variables
                    .insert("systemroot".into(), "untrusted-os-root".into());
                assert_eq!(validate_specs(&specs), Err(DaemonError::InvalidSpec));
            }
        }

        #[tokio::test]
        async fn explicit_mock_configuration_never_reads_credentials_and_freezes_on_launch() {
            let mut plane = plane(1, 1);
            for address in ["0.0.0.0:1000", "192.0.2.1:1000", "127.0.0.1:0"] {
                assert_eq!(
                    plane.daemon.enable_mock_discord(address.parse().unwrap()),
                    Err(DaemonError::InvalidSpec)
                );
            }
            assert!(
                plane
                    .daemon
                    .entries
                    .values()
                    .all(|entry| entry.mock_discord.is_none())
            );
            let address = "127.0.0.1:1234".parse().unwrap();
            plane.daemon.enable_mock_discord(address).unwrap();
            let key = ShardKey {
                bot_id: "id-0".into(),
                shard_id: 0,
            };
            let missing_program = std::env::current_dir()
                .unwrap()
                .join("target")
                .join(format!(
                    "missing-gateway-{:016x}",
                    getrandom::u64().unwrap()
                ));
            plane.daemon.set_gateway_program(&missing_program).unwrap();
            assert_eq!(
                plane.daemon.start(&key, &no_credentials),
                Err(DaemonError::SpawnFailed)
            );
            assert_eq!(
                plane.daemon.enable_mock_discord(address),
                Err(DaemonError::InvalidState)
            );
            assert!(!plane.daemon.has_child(&key));
            assert!(plane.daemon.entries[&key].reservation.is_none());
        }

        #[test]
        fn exit_before_shutdown_child() {
            if std::env::var_os("BOTCTL_TEST_EXIT_BEFORE_SHUTDOWN").is_some() {
                std::process::exit(27);
            }
        }

        #[tokio::test]
        async fn abnormal_exit_after_admission_cannot_be_reported_as_a_successful_restart() {
            let mut plane = plane(1, 1);
            plane
                .daemon
                .set_local_identify_policy(crate::local_identify::LocalIdentifyPolicy {
                    min_interval: Duration::ZERO,
                    ..Default::default()
                });
            let key = ShardKey {
                bot_id: "id-0".into(),
                shard_id: 0,
            };
            let entry = plane.daemon.entries.get_mut(&key).unwrap();
            entry.spec.gateway.args = [
                "--exact",
                "daemon::control::tests::exit_before_shutdown_child",
            ]
            .map(OsString::from)
            .to_vec();
            entry
                .spec
                .gateway
                .environment
                .variables
                .insert("BOTCTL_TEST_EXIT_BEFORE_SHUTDOWN".into(), "1".into());
            plane
                .daemon
                .start(&key, &|_: &str| Some(OsString::from("dummy")))
                .unwrap();
            let generation = plane.daemon.snapshots()[0]
                .identity
                .as_ref()
                .unwrap()
                .generation
                .clone();
            assert_eq!(plane.request(&request("restart", 0, 1))["ok"], true);
            // Reap in the OS adapter without updating the daemon entry, placing
            // the crash deterministically between admission and the first tick.
            tokio::time::timeout(Duration::from_secs(5), async {
                let recovery_key = plane.daemon.entries[&key].recovery_key;
                while plane
                    .daemon
                    .recovery
                    .child_mut(recovery_key)
                    .unwrap()
                    .unwrap()
                    .try_wait()
                    .unwrap()
                    .is_none()
                {
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
            })
            .await
            .unwrap();
            plane.tick(&no_credentials).await;
            plane.tick(&no_credentials).await;
            let result = plane.request(&query(1));
            assert_eq!(result["operation"]["state"], "failed");
            assert_eq!(result["operation"]["shards"][0]["error"], "ChildExited");
            assert!(!plane.daemon.has_child(&key));
            assert_eq!(
                plane.daemon.snapshots()[0]
                    .identity
                    .as_ref()
                    .unwrap()
                    .generation,
                generation
            );
        }

        #[tokio::test]
        async fn history_capacity_rejects_new_actions_but_keeps_requery_and_deduplication() {
            let mut plane = plane(1, 1);
            for id in 0..OPERATION_LIMIT {
                assert_eq!(plane.request(&request("stop", 0, id))["ok"], true);
                plane.tick(&no_credentials).await;
                plane.tick(&no_credentials).await;
                assert_eq!(plane.request(&query(id))["operation"]["state"], "succeeded");
            }
            assert_eq!(
                plane.request(&request("start", 0, OPERATION_LIMIT))["error"],
                "operation history is full"
            );
            assert_eq!(plane.operations.len(), OPERATION_LIMIT);
            let original = plane.request(&query(0));
            assert_eq!(plane.request(&request("stop", 0, 0)), original);
            assert_eq!(plane.request(&request("start", 0, 0))["ok"], false);
            assert!(
                plane
                    .daemon
                    .snapshots()
                    .iter()
                    .all(|s| s.identity.is_none() && !s.desired_running)
            );
        }

        #[tokio::test]
        async fn failed_manual_launch_cancels_automatic_retry_and_terminal_results_are_frozen() {
            let mut plane = plane(1, 1);
            plane.request(&request("stop", 0, 1));
            plane.tick(&no_credentials).await;
            plane.tick(&no_credentials).await;
            let stopped = plane.request(&query(1));
            plane.request(&request("start", 0, 2));
            plane.tick(&|_: &str| None).await;
            plane.tick(&no_credentials).await;
            let failure = plane.request(&query(2));
            assert_eq!(failure["ok"], false);
            assert_eq!(
                failure["operation"]["shards"][0]["error"],
                "MissingCredential"
            );
            let state = plane.daemon.snapshots().remove(0);
            assert!(!state.desired_running);
            assert!(state.retry_at_ms.is_none());
            assert!(state.identity.is_none());
            assert_eq!(plane.request(&query(1)), stopped);
        }

        #[tokio::test]
        async fn expired_start_does_not_change_a_preexisting_retry_or_an_unattempted_shard() {
            let mut plane = plane(1, 2);
            let key = ShardKey {
                bot_id: "id-0".into(),
                shard_id: 0,
            };
            let entry = plane.daemon.entries.get_mut(&key).unwrap();
            entry.snapshot.desired_running = true;
            entry.snapshot.retry_at_ms = Some(300_000);
            plane.request(&request("start", 0, 1));
            plane.tick(&no_credentials).await;
            plane
                .operations
                .get_mut(&format!("{:032x}", 1))
                .unwrap()
                .started = Instant::now() - Duration::from_secs(31);
            plane.tick(&no_credentials).await;
            let failure = plane.request(&query(1));
            assert_eq!(failure["operation"]["state"], "failed");
            assert_eq!(failure["operation"]["shards"][0]["error"], "InvalidState");
            assert_eq!(failure["operation"]["shards"][1]["error"], "StopTimedOut");
            let existing = plane.daemon.entries.get(&key).unwrap();
            assert!(existing.snapshot.desired_running);
            assert_eq!(existing.snapshot.retry_at_ms, Some(300_000));
            assert!(!existing.restart_held);
            assert!(
                plane
                    .daemon
                    .entries
                    .values()
                    .all(|e| !e.started && !plane.daemon.has_child(&e.snapshot.key))
            );
        }

        #[tokio::test]
        async fn tick_budget_rotates_across_bots_and_terminal_history_does_not_starve_them() {
            let mut plane = plane(33, 1);
            for id in 0..33 {
                assert_eq!(plane.request(&request("stop", id, id))["ok"], true);
            }
            plane.tick(&no_credentials).await;
            assert!(plane.operations.values().filter(|op| op.next != 0).count() <= 16);
            for _ in 0..100 {
                plane.tick(&no_credentials).await;
            }
            assert!(plane.operations.values().all(|op| op.finished.is_some()));
            for id in 0..33 {
                assert_eq!(plane.request(&query(id))["operation"]["state"], "succeeded");
            }
        }

        #[test]
        fn admission_limits_and_unconfirmed_force_do_not_mutate_any_shard() {
            let mut plane = plane(1, OPERATION_SHARD_LIMIT as u32 + 1);
            assert_eq!(plane.request(&request("start", 0, 1))["ok"], false);
            let mut force = request("stop", 0, 2);
            force.force = true;
            assert_eq!(plane.request(&force)["error"], "force stop requires --yes");
            assert!(plane.operations.is_empty());
            assert!(
                plane
                    .daemon
                    .entries
                    .values()
                    .all(|e| !e.restart_held && !e.started)
            );
            let mut invalid = request("start", 0, 1);
            invalid.operation_id = Some("untrusted invalid identifier".into());
            assert!(!invalid.valid());
            assert_eq!(plane.request(&invalid)["error"], "invalid control request");
            assert!(
                serde_json::from_str::<CommandRequest>(r#"{"command":"ps","unexpected":true}"#)
                    .is_err()
            );
        }

        #[test]
        fn selected_status_and_inspect_contain_only_the_resolved_bot() {
            let mut plane = plane(2, 1);
            for command in ["status", "inspect"] {
                let request = CommandRequest {
                    command: command.into(),
                    bot: Some("bot1".into()),
                    ..Default::default()
                };
                let response = plane.request(&request);
                assert_eq!(response["bot"]["bot_id"], "id-1");
                assert_eq!(response["status"]["processes"].as_array().unwrap().len(), 1);
                assert_eq!(response["status"]["processes"][0]["key"]["bot_id"], "id-1");
            }
        }

        /// `gateway-update` replaces a running Gateway, so it is a mutation like stop: only an
        /// admin whose scope covers every shard of the Bot may ask for it. A refusal happens
        /// before any other check, so it cannot start a standby Gateway or read a credential.
        #[test]
        fn gateway_update_is_recorded_before_it_runs_and_refused_if_it_cannot_be() {
            use super::super::update_control::ControlAccess;
            let dir = log_dir("gwaudit");
            let admin = ControlAccess::default();
            let mut recorded = plane(1, 1);
            recorded.set_gateway_update_audit(dir.join("gateway-update-audit.jsonl"));
            // Not the separated layout here, so the daemon declines - but the request and its
            // refusal are on record.
            let response = recorded.request_as(&request("gateway-update", 0, 3), &admin);
            assert_eq!(response["ok"], false);
            let text = std::fs::read_to_string(dir.join("gateway-update-audit.jsonl")).unwrap();
            let lines: Vec<Value> = text
                .lines()
                .map(|l| serde_json::from_str(l).unwrap())
                .collect();
            assert_eq!(lines.len(), 2, "{text}");
            assert_eq!(lines[0]["event"], "requested");
            assert_eq!(lines[1]["event"], "rejected");
            assert_eq!(lines[0]["subject"], "local-bootstrap");
            assert_eq!(lines[0]["role"], "admin");
            assert!(!text.contains("secret") && !text.contains('/'));
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                let mode = std::fs::metadata(dir.join("gateway-update-audit.jsonl"))
                    .unwrap()
                    .permissions()
                    .mode();
                assert_eq!(mode & 0o777, 0o600);
            }
            // A record that cannot be written stops the request before anything happens.
            let mut unwritable = plane(1, 1);
            unwritable.set_gateway_update_audit(dir.clone());
            let refused = unwritable.request_as(&request("gateway-update", 0, 4), &admin);
            assert_eq!(
                refused["error"],
                "audit unavailable; gateway-update refused"
            );
            assert!(unwritable.operations.is_empty());
            let _ = std::fs::remove_dir_all(&dir);
        }

        #[test]
        fn audit_partial_tail_and_stable_retry_bytes() {
            let dir = log_dir("audit-partial-tail");
            let path = dir.join("audit.jsonl");
            std::fs::write(&path, b"{partial").unwrap();
            let mut plane = plane(1, 1);
            let actor = ("subject".to_owned(), "admin".to_owned());
            let mut record =
                ControlPlane::audit_record("succeeded", "id", Some("bot"), &actor, None).unwrap();
            let bytes = record.bytes.clone();
            plane.set_gateway_update_audit(dir.clone());
            assert!(plane.write_audit(&mut record).is_err());
            plane.pending_audit.push(record);
            assert_eq!(plane.pending_audit[0].bytes, bytes);
            plane.set_gateway_update_audit(path.clone());
            plane.retry_pending_audit();
            assert!(plane.pending_audit.is_empty());
            let data = std::fs::read(&path).unwrap();
            assert_eq!(data, bytes);
            for line in data
                .split(|&byte| byte == b'\n')
                .filter(|line| !line.is_empty())
            {
                serde_json::from_slice::<Value>(line).unwrap();
            }
            assert_eq!(
                std::fs::read(dir.join("audit.jsonl.corrupt")).unwrap(),
                b"{partial"
            );
            let _ = std::fs::remove_dir_all(dir);
        }

        #[test]
        fn update_timeout_scales_with_shards() {
            assert_eq!(update_timeout(0), Duration::from_secs(120));
            assert_eq!(update_timeout(7), Duration::from_secs(435));
        }

        #[cfg(unix)]
        #[test]
        fn audit_sync_retry_reappends_after_truncation() {
            let dir = log_dir("audit-truncated-retry");
            let path = dir.join("audit.jsonl");
            let mut plane = plane(1, 1);
            plane.set_gateway_update_audit(path.clone());
            let actor = ("subject".to_owned(), "admin".to_owned());
            let mut record =
                ControlPlane::audit_record("succeeded", "id", Some("bot"), &actor, None).unwrap();
            plane.write_audit(&mut record).unwrap();
            std::fs::write(&path, b"").unwrap();
            plane.write_audit(&mut record).unwrap();
            assert_eq!(std::fs::read(&path).unwrap(), record.bytes);
            let _ = std::fs::remove_dir_all(dir);
        }

        #[test]
        fn audit_tail_scan_reads_bounded_chunks_from_the_end() {
            struct Counted {
                cursor: std::io::Cursor<Vec<u8>>,
                read: usize,
            }
            impl std::io::Read for Counted {
                fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
                    assert!(buffer.len() <= AUDIT_SCAN_BYTES);
                    let n = std::io::Read::read(&mut self.cursor, buffer)?;
                    self.read += n;
                    Ok(n)
                }
            }
            impl std::io::Seek for Counted {
                fn seek(&mut self, at: std::io::SeekFrom) -> std::io::Result<u64> {
                    std::io::Seek::seek(&mut self.cursor, at)
                }
            }
            let mut bytes = b"{}\n".repeat(1_000_000);
            let boundary = bytes.len() as u64;
            bytes.extend_from_slice(b"{partial");
            let end = bytes.len() as u64;
            let mut reader = Counted {
                cursor: std::io::Cursor::new(bytes),
                read: 0,
            };
            assert_eq!(
                ControlPlane::audit_tail_boundary(&mut reader, end).unwrap(),
                boundary
            );
            assert!(reader.read <= AUDIT_SCAN_BYTES, "valid history was scanned");
            let mut reader = Counted {
                cursor: std::io::Cursor::new(vec![b'x'; AUDIT_SCAN_BYTES * 3 + 1]),
                read: 0,
            };
            assert_eq!(
                ControlPlane::audit_tail_boundary(&mut reader, (AUDIT_SCAN_BYTES * 3 + 1) as u64)
                    .unwrap(),
                0
            );
        }

        #[test]
        fn audit_large_tail_is_capped_but_fully_removed() {
            let dir = log_dir("audit-large-tail");
            let path = dir.join("audit.jsonl");
            let history = b"{}\n".repeat(100_000);
            let mut data = history.clone();
            data.extend(vec![b'x'; AUDIT_TAIL_LIMIT as usize * 2]);
            std::fs::write(&path, data).unwrap();
            let mut plane = plane(1, 1);
            plane.set_gateway_update_audit(path.clone());
            let mut record = ControlPlane::audit_record(
                "succeeded",
                "id",
                Some("bot"),
                &("subject".into(), "admin".into()),
                None,
            )
            .unwrap();
            plane.write_audit(&mut record).unwrap();
            let mut expected = history;
            expected.extend_from_slice(&record.bytes);
            assert_eq!(std::fs::read(path).unwrap(), expected);
            assert_eq!(
                std::fs::read(dir.join("audit.jsonl.corrupt")).unwrap(),
                vec![b'x'; AUDIT_TAIL_LIMIT as usize]
            );
            let _ = std::fs::remove_dir_all(dir);
        }

        #[test]
        fn audit_quarantine_retry_is_idempotent_and_bounded() {
            let dir = log_dir("audit-quarantine-retry");
            let path = dir.join("audit.jsonl");
            let corrupt = dir.join("audit.jsonl.corrupt");
            std::fs::write(&path, b"{}\npartial").unwrap();
            let mut plane = plane(1, 1);
            plane.set_gateway_update_audit(path.clone());
            let mut file = ControlPlane::open_audit_file(&path).unwrap();
            // Model a failed truncate after successful quarantine. Retry through another
            // descriptor and then another record, sharing the saved repair progress.
            ControlPlane::quarantine_audit_tail(&plane.audit_tail, &mut file, &path, 3, 10)
                .unwrap();
            drop(file);
            let mut file = ControlPlane::open_audit_file(&path).unwrap();
            ControlPlane::quarantine_audit_tail(&plane.audit_tail, &mut file, &path, 3, 10)
                .unwrap();
            assert_eq!(std::fs::read(&corrupt).unwrap(), b"partial");
            let mut record = ControlPlane::audit_record(
                "succeeded",
                "id",
                Some("bot"),
                &("subject".into(), "admin".into()),
                None,
            )
            .unwrap();
            plane.write_audit(&mut record).unwrap();
            assert_eq!(std::fs::read(&corrupt).unwrap(), b"partial");
            let mut expected = b"{}\n".to_vec();
            expected.extend_from_slice(&record.bytes);
            assert_eq!(std::fs::read(&path).unwrap(), expected);
            // Leave three bytes of quarantine space, then test both near-full and full.
            std::fs::write(&corrupt, vec![b'x'; AUDIT_CORRUPT_LIMIT as usize - 3]).unwrap();
            std::fs::write(&path, b"next partial").unwrap();
            record.written = false;
            plane.write_audit(&mut record).unwrap();
            assert_eq!(std::fs::read(&path).unwrap(), record.bytes);
            assert_eq!(
                std::fs::metadata(&corrupt).unwrap().len(),
                AUDIT_CORRUPT_LIMIT
            );
            let data = std::fs::read(&corrupt).unwrap();
            assert_eq!(&data[data.len() - 3..], b"nex");
            std::fs::write(&path, b"another partial").unwrap();
            record.written = false;
            plane.write_audit(&mut record).unwrap();
            assert_eq!(std::fs::read(&path).unwrap(), record.bytes);
            assert_eq!(
                std::fs::metadata(&corrupt).unwrap().len(),
                AUDIT_CORRUPT_LIMIT
            );
            drop(file);
            drop(plane);
            let _ = std::fs::remove_dir_all(dir);
        }

        #[test]
        fn audit_retries_are_paced_after_a_failure() {
            let dir = log_dir("audit-paced-retry");
            let path = dir.join("audit.jsonl");
            let mut plane = plane(1, 1);
            plane.set_gateway_update_audit(dir.clone());
            plane.pending_audit.push(
                ControlPlane::audit_record(
                    "failed",
                    "id",
                    Some("bot"),
                    &("subject".into(), "admin".into()),
                    None,
                )
                .unwrap(),
            );
            plane.retry_pending_audit();
            assert_eq!(plane.pending_audit.len(), 1);
            assert!(plane.audit_retry_at.is_some());
            plane.set_gateway_update_audit(path.clone());
            // Inject the retry deadline instead of relying on wall-clock speed or sleeping.
            plane.audit_retry_at = Some(Instant::now() + Duration::from_secs(3600));
            plane.retry_pending_audit();
            assert!(!path.exists());
            plane.audit_retry_at = Some(Instant::now());
            plane.retry_pending_audit();
            assert!(plane.pending_audit.is_empty());
            assert!(plane.audit_retry_at.is_none());
            assert!(path.exists());
            let _ = std::fs::remove_dir_all(dir);
        }

        #[test]
        fn audit_sync_retry_checks_bytes_after_regrowth_and_accepts_plain_append() {
            use std::io::Write;
            let dir = log_dir("audit-regrown-retry");
            let path = dir.join("audit.jsonl");
            let mut plane = plane(1, 1);
            plane.set_gateway_update_audit(path.clone());
            let mut record = ControlPlane::audit_record(
                "succeeded",
                "id",
                Some("bot"),
                &("subject".into(), "admin".into()),
                None,
            )
            .unwrap();
            plane.write_audit(&mut record).unwrap();
            // Same inode, regrown beyond the saved record: length is not proof of contents.
            let replacement = b"{}\n".repeat(record.bytes.len());
            std::fs::write(&path, &replacement).unwrap();
            plane.write_audit(&mut record).unwrap();
            let mut expected = replacement;
            expected.extend_from_slice(&record.bytes);
            assert_eq!(std::fs::read(&path).unwrap(), expected);
            ControlPlane::open_audit_file(&path)
                .unwrap()
                .write_all(b"{}\n")
                .unwrap();
            expected.extend_from_slice(b"{}\n");
            plane.write_audit(&mut record).unwrap();
            assert_eq!(
                std::fs::read(path).unwrap(),
                expected,
                "intact record was duplicated"
            );
            let _ = std::fs::remove_dir_all(dir);
        }

        #[test]
        fn update_admission_rejects_pending_fallback_without_holding_recovery() {
            let mut plane = plane(2, 2);
            let protected = ShardKey {
                bot_id: "id-0".into(),
                shard_id: 1,
            };
            plane
                .daemon
                .entries
                .get_mut(&protected)
                .unwrap()
                .update_fallback_reserved = true;
            // Model a finished update with shard 1 still awaiting fallback admission.
            // Call ControlPlane admission directly; no separated Core or child is needed.
            let retry = request("gateway-update", 0, 92);
            for _ in 0..2 {
                let response = plane.admit(&retry, Some("id-0".into()));
                assert_eq!(response["ok"], false);
                assert_eq!(
                    response["error"],
                    "gateway fallback recovery pending; retry after recovery"
                );
                assert!(plane.operations.is_empty());
                assert!(
                    plane
                        .daemon
                        .entries
                        .values()
                        .all(|entry| !entry.restart_held)
                );
                assert!(plane.daemon.entries[&protected].update_fallback_reserved);
            }
            // A different Bot can proceed without holding the protected Bot's shards.
            let other = plane.admit(&request("gateway-update", 1, 93), Some("id-1".into()));
            assert_eq!(other["ok"], true);
            for entry in plane.daemon.entries.values() {
                assert_eq!(entry.restart_held, entry.snapshot.key.bot_id == "id-1");
            }
            // Rejection did not consume the operation ID. Once recovery releases its
            // reservation, the same request can be admitted and acquire the normal holds.
            plane
                .daemon
                .entries
                .get_mut(&protected)
                .unwrap()
                .update_fallback_reserved = false;
            let admitted = plane.admit(&retry, Some("id-0".into()));
            assert_eq!(admitted["ok"], true);
            assert_eq!(plane.operations.len(), 2);
            assert!(
                plane
                    .daemon
                    .entries
                    .values()
                    .all(|entry| entry.restart_held)
            );
        }

        #[test]
        fn update_fallback_identify_defers_other_shards_without_spending_its_slot() {
            use crate::local_identify::LocalIdentifyPolicy;
            let mut plane = plane(2, 2);
            let daemon = &mut plane.daemon;
            let protected = ShardKey {
                bot_id: "id-0".into(),
                shard_id: 1,
            };
            let other = ShardKey {
                bot_id: "id-0".into(),
                shard_id: 0,
            };
            let independent = ShardKey {
                bot_id: "id-1".into(),
                shard_id: 0,
            };
            let owner_key = daemon.entries[&protected].recovery_key;
            let other_key = daemon.entries[&other].recovery_key;
            let independent_key = daemon.entries[&independent].recovery_key;
            daemon
                .entries
                .get_mut(&protected)
                .unwrap()
                .update_fallback_reserved = true;
            daemon.set_local_identify_policy(LocalIdentifyPolicy {
                min_interval: Duration::from_secs(30),
                window: Duration::from_secs(3600),
                max_per_window: 10,
            });
            let at = Duration::ZERO;
            for _ in 0..9 {
                daemon.local_identify.record("id-0", at);
            }
            let (_, clock) = tokio::sync::watch::channel(Duration::from_secs(60));
            let clock = MonotonicClock::with_manual_time(clock);
            let mut host = ProcessRecoveryHost {
                entries: &mut daemon.entries,
                credentials: &no_credentials,
                instance: daemon.instance,
                sequence: &mut daemon.sequence,
                manual: Some(&other),
                reserved_restart: None,
                clock: &clock,
                last_now: &mut daemon.last_now,
                clock_failed: &mut daemon.clock_failed,
                local_identify: &mut daemon.local_identify,
                errors: BTreeMap::new(),
            };
            assert_eq!(
                host.try_acquire_identify(other_key),
                IdentifyAdmission::Pending
            );
            assert_eq!(host.errors[&other], DaemonError::IdentifyDeferred);
            host.manual = None;
            assert_eq!(
                host.try_acquire_identify(other_key),
                IdentifyAdmission::Pending
            );
            assert_eq!(
                host.try_acquire_identify(independent_key),
                IdentifyAdmission::Granted
            );
            assert_eq!(
                host.try_acquire_identify(owner_key),
                IdentifyAdmission::Granted
            );
            assert!(!host.entries[&protected].update_fallback_reserved);
            assert_eq!(
                host.try_acquire_identify(other_key),
                IdentifyAdmission::Unavailable
            );
        }

        #[cfg(unix)]
        #[test]
        fn update_identify_cooldown_and_exhaustion() {
            use crate::local_identify::{
                LocalAdmission, LocalIdentifyPolicy, LocalIdentifyRegistry,
            };
            let now = Duration::ZERO;
            let mut registry = LocalIdentifyRegistry::with_policy(LocalIdentifyPolicy {
                min_interval: Duration::from_secs(30),
                window: Duration::from_secs(3600),
                max_per_window: 3,
            });
            registry.record("bot", now);
            assert!(matches!(
                registry.try_acquire_standby("bot", now),
                LocalAdmission::Pending { .. }
            ));
            registry.record("bot", now);
            assert!(matches!(
                registry.try_acquire_standby("bot", now),
                LocalAdmission::Exhausted { .. }
            ));
        }

        #[cfg(unix)]
        mod restart_identify {
            use super::*;
            use crate::local_identify::LocalIdentifyPolicy;

            fn fixture(
                bots: usize,
                shards: u32,
                max_per_window: u32,
            ) -> (ControlPlane, tokio::sync::watch::Sender<Duration>) {
                let mut plane = plane(bots, shards);
                let specs = plane
                    .daemon
                    .entries
                    .values()
                    .map(|entry| {
                        let mut spec = entry.spec.clone();
                        spec.gateway.program = "/bin/sh".into();
                        spec.gateway.args =
                            ["-c", "IFS= read -r line"].map(OsString::from).to_vec();
                        spec
                    })
                    .collect();
                let (time, clock) = tokio::sync::watch::channel(Duration::ZERO);
                plane.daemon = Daemon::with_clock(
                    specs,
                    RestartPolicy::default(),
                    MonotonicClock::with_manual_time(clock),
                )
                .unwrap();
                plane.daemon.set_local_identify_policy(LocalIdentifyPolicy {
                    max_per_window,
                    ..Default::default()
                });
                (plane, time)
            }

            fn credentials(_: &str) -> Option<OsString> {
                Some("fixture-only".into())
            }

            fn key(bot: usize, shard_id: u32) -> ShardKey {
                ShardKey {
                    bot_id: format!("id-{bot}"),
                    shard_id,
                }
            }

            fn running_pid(plane: &ControlPlane, key: &ShardKey) -> u32 {
                let entry = &plane.daemon.entries[key];
                assert!(entry.snapshot.desired_running);
                assert!(matches!(entry.snapshot.state, ProcessState::Running));
                assert!(entry.snapshot.retry_at_ms.is_none());
                let live = plane
                    .daemon
                    .recovery
                    .snapshot(entry.recovery_key)
                    .unwrap()
                    .pid
                    .unwrap();
                assert_eq!(entry.snapshot.identity.as_ref().unwrap().pid, live);
                live
            }

            async fn complete(plane: &mut ControlPlane, id: usize) -> Value {
                tokio::time::timeout(Duration::from_secs(5), async {
                    loop {
                        plane.tick(&credentials).await;
                        let result = plane.request(&query(id));
                        if result["operation"]["state"] != "running" {
                            return result;
                        }
                        tokio::time::sleep(Duration::from_millis(5)).await;
                    }
                })
                .await
                .unwrap()
            }

            async fn cleanup(plane: &mut ControlPlane) {
                assert!(
                    plane
                        .shutdown_children(&credentials, Duration::from_secs(5))
                        .await
                        .is_empty()
                );
            }

            fn restart_all(id: usize) -> CommandRequest {
                CommandRequest {
                    bot: None,
                    yes: true,
                    ..request("restart-all", 0, id)
                }
            }

            #[tokio::test]
            async fn launch_rejects_a_denied_reference_even_after_spec_validation() {
                let (mut plane, _) = fixture(1, 1, 10);
                let key = key(0, 0);
                // Emulate an invalid runtime spec reaching the launch boundary
                // after the normal planner and daemon validation have run.
                plane
                    .daemon
                    .entries
                    .get_mut(&key)
                    .unwrap()
                    .spec
                    .gateway
                    .environment
                    .credential_refs[0] = "LD_PRELOAD".into();
                let forbidden_read = |_: &str| -> Option<OsString> {
                    panic!("launch read an invalid credential reference")
                };
                assert_eq!(
                    plane.daemon.start(&key, &forbidden_read),
                    Err(DaemonError::InvalidSpec)
                );
                assert!(!plane.daemon.has_child(&key));
            }

            #[tokio::test]
            async fn recovery_pass_stall_does_not_backdate_the_next_identify_launch() {
                use std::sync::Arc;
                use std::sync::atomic::{AtomicUsize, Ordering};

                // The first recovery-pass observation happens before the stall.
                // All later observations are after it until the clock is advanced.
                struct StallClock(Arc<AtomicUsize>);
                impl Clock for StallClock {
                    fn now(&self) -> Duration {
                        let sample = self.0.fetch_add(1, Ordering::Relaxed);
                        Duration::from_secs(match sample {
                            0..=2 => 0,
                            3..100 => 30,
                            _ => 60,
                        })
                    }
                    fn sleep_until(&self, _: Duration) -> crate::supervisor::Sleep<'_> {
                        Box::pin(std::future::pending())
                    }
                }

                let (plane, _) = fixture(1, 2, 10);
                let specs = plane
                    .daemon
                    .entries
                    .values()
                    .map(|entry| entry.spec.clone())
                    .collect();
                let samples = Arc::new(AtomicUsize::new(0));
                let mut daemon = Daemon::with_clock(
                    specs,
                    RestartPolicy::default(),
                    StallClock(Arc::clone(&samples)),
                )
                .unwrap();
                for entry in daemon.entries.values_mut() {
                    daemon.recovery.register(entry.recovery_key, 0).unwrap();
                    entry.started = true;
                    entry.snapshot.desired_running = true;
                }
                let keys = [key(0, 0), key(0, 1)];
                let running = |daemon: &Daemon<StallClock>| {
                    keys.iter().filter(|key| daemon.has_child(key)).count()
                };
                daemon.poll(&credentials).unwrap();
                assert_eq!(running(&daemon), 1);
                daemon.poll(&credentials).unwrap();
                assert_eq!(
                    running(&daemon),
                    1,
                    "the clock has not moved since the first launch"
                );

                samples.store(100, Ordering::Relaxed);
                daemon.poll(&credentials).unwrap();
                assert_eq!(running(&daemon), 2);
                for key in keys {
                    daemon.stop(&key, StopMode::WaitForExit).unwrap();
                    let recovery_key = daemon.entries[&key].recovery_key;
                    let child = daemon.recovery.child_mut(recovery_key).unwrap().unwrap();
                    child.kill().await.unwrap();
                }
                daemon.poll(&credentials).unwrap();
            }

            #[tokio::test]
            async fn admission_detects_regression_from_the_recovery_observation() {
                use std::sync::atomic::{AtomicUsize, Ordering};

                struct RegressingClock(AtomicUsize);
                impl Clock for RegressingClock {
                    fn now(&self) -> Duration {
                        Duration::from_secs(match self.0.fetch_add(1, Ordering::Relaxed) {
                            0..=2 => 0, // constructor and start of the recovery pass
                            3 => 30,    // recovery has decided the child is eligible
                            _ => 29,    // admission regressed within this same pass
                        })
                    }
                    fn sleep_until(&self, _: Duration) -> crate::supervisor::Sleep<'_> {
                        Box::pin(std::future::pending())
                    }
                }

                let (plane, _) = fixture(1, 1, 10);
                let specs = plane
                    .daemon
                    .entries
                    .values()
                    .map(|entry| entry.spec.clone())
                    .collect();
                let mut daemon = Daemon::with_clock(
                    specs,
                    RestartPolicy::default(),
                    RegressingClock(AtomicUsize::new(0)),
                )
                .unwrap();
                let key = key(0, 0);
                let entry = daemon.entries.get_mut(&key).unwrap();
                daemon.recovery.register(entry.recovery_key, 0).unwrap();
                entry.started = true;
                entry.snapshot.desired_running = true;

                let no_spawn = |_: &str| -> Option<OsString> {
                    panic!("regressed admission must not launch a child")
                };
                assert_eq!(daemon.poll(&no_spawn), Err(DaemonError::ClockRegressed));
                assert!(!daemon.has_child(&key));
                assert_eq!(daemon.poll(&no_spawn), Err(DaemonError::ClockRegressed));
            }

            #[tokio::test]
            async fn lifetime_restart_limit_refuses_restart_and_restart_all_before_any_stop() {
                let (mut plane, time) = fixture(2, 1, 10);
                let first = key(0, 0);
                let limited = key(1, 0);
                plane.daemon.start(&first, &credentials).unwrap();
                plane.daemon.start(&limited, &credentials).unwrap();
                let max = RestartPolicy::default().max_restarts as usize;
                for attempt in 1..=max {
                    time.send_replace(Duration::from_secs(31 * attempt as u64));
                    assert_eq!(plane.request(&request("restart", 1, attempt))["ok"], true);
                    assert_eq!(
                        complete(&mut plane, attempt).await["operation"]["state"],
                        "succeeded"
                    );
                }
                let state = plane
                    .daemon
                    .recovery
                    .snapshot(plane.daemon.entries[&limited].recovery_key)
                    .unwrap();
                assert_eq!(state.restart.restarts as usize, max);
                let pids = [running_pid(&plane, &first), running_pid(&plane, &limited)];
                time.send_replace(Duration::from_secs(3600));

                assert_eq!(
                    plane.request(&request("restart", 1, 100))["error"],
                    "RestartBudget"
                );
                assert_eq!(plane.request(&restart_all(101))["error"], "RestartBudget");
                plane.tick(&credentials).await;
                assert_eq!(plane.operations.len(), max);
                assert!(
                    plane
                        .daemon
                        .entries
                        .values()
                        .all(|entry| !entry.restart_held)
                );
                assert_eq!(
                    [running_pid(&plane, &first), running_pid(&plane, &limited)],
                    pids
                );
                cleanup(&mut plane).await;
            }

            #[cfg(target_os = "linux")]
            #[tokio::test]
            async fn restart_leaves_other_gateway_running_during_a_grant_sync() {
                use std::io::{Read, Write};
                use tokio::sync::oneshot;

                let (mut plane, _) = fixture(2, 1, 10);
                let root = std::env::temp_dir()
                    .join(format!("igr-busy-{:016x}", getrandom::u64().unwrap()));
                let policy = LocalIdentifyPolicy {
                    min_interval: Duration::ZERO,
                    ..Default::default()
                };
                let (entered, entered_sync) = oneshot::channel();
                let (release, release_sync) = std::sync::mpsc::channel();
                let mut entered = Some(entered);
                let mut writes = 0;
                plane.daemon.local_identify.send_grants = Some(
                    crate::identify_grant::service::Service::open_with_sync_hook(
                        &root,
                        policy,
                        move || {
                            writes += 1;
                            // The first two writes are A and B's startup launches.
                            if writes == 3 {
                                entered.take().unwrap().send(()).unwrap();
                                release_sync.recv_timeout(Duration::from_secs(10)).unwrap();
                            }
                        },
                    )
                    .unwrap(),
                );
                let a = key(0, 0);
                let b = key(1, 0);
                plane.daemon.start(&a, &credentials).unwrap();
                plane.daemon.start(&b, &credentials).unwrap();
                let pid = running_pid(&plane, &b);
                let generation = plane.daemon.entries[&a]
                    .snapshot
                    .identity
                    .as_ref()
                    .unwrap()
                    .generation
                    .clone();
                let mut socket = plane
                    .daemon
                    .local_identify
                    .send_grants
                    .as_mut()
                    .unwrap()
                    .attach(
                        &mut tokio::process::Command::new("/bin/true"),
                        &a,
                        generation,
                        false,
                    )
                    .unwrap();

                // A's grant starts in poll() after B's admission, before the first stop.
                assert_eq!(plane.request(&request("restart", 1, 1))["ok"], true);
                socket.write_all(&[1]).unwrap(); // Identify grant wire v1 request.
                plane.tick(&credentials).await;
                tokio::time::timeout(Duration::from_secs(2), entered_sync)
                    .await
                    .unwrap()
                    .unwrap();
                let refused = plane.request(&query(1));
                assert_eq!(refused["operation"]["state"], "failed");
                assert!(refused.to_string().contains("IdentifyJournalBusy"));

                // The same busy state also refuses new requests at admission.
                for id in 2..5 {
                    assert_eq!(
                        plane.request(&request("restart", 1, id))["error"],
                        "IdentifyJournalBusy"
                    );
                    plane.tick(&credentials).await;
                    assert_eq!(running_pid(&plane, &b), pid);
                    assert!(!plane.daemon.entries[&b].restart_held);
                }
                assert_eq!(plane.operations.len(), 1);

                release.send(()).unwrap();
                assert!(
                    plane
                        .daemon
                        .local_identify
                        .send_grants
                        .as_mut()
                        .unwrap()
                        .wait_for_write()
                );
                socket.read_exact(&mut [0; 10]).unwrap();
                assert_eq!(plane.request(&request("restart", 1, 5))["ok"], true);
                assert_eq!(
                    complete(&mut plane, 5).await["operation"]["state"],
                    "succeeded"
                );
                assert_ne!(running_pid(&plane, &b), pid);
                cleanup(&mut plane).await;
                drop(plane);
                std::fs::remove_dir_all(root).unwrap();
            }

            #[cfg(target_os = "linux")]
            #[tokio::test]
            async fn restart_all_refuses_when_only_one_replacement_fits_in_the_journal() {
                let (mut plane, _) = fixture(2, 1, 10);
                plane.daemon.set_local_identify_policy(LocalIdentifyPolicy {
                    min_interval: Duration::ZERO,
                    ..Default::default()
                });
                let root =
                    std::env::temp_dir().join(format!("igr-r-{:016x}", getrandom::u64().unwrap()));
                plane.daemon.enable_identify_send_grants(&root).unwrap();
                let first = key(0, 0);
                let second = key(1, 0);
                plane.daemon.start(&first, &credentials).unwrap();
                plane.daemon.start(&second, &credentials).unwrap();
                let pids = [running_pid(&plane, &first), running_pid(&plane, &second)];
                let instance = plane.daemon.instance;
                plane
                    .daemon
                    .local_identify
                    .send_grants
                    .as_mut()
                    .unwrap()
                    .limit_journal_remaining_to_one_restart(&first, instance);
                let available = plane.daemon.snapshots()[0]
                    .identify_send_grant
                    .as_ref()
                    .unwrap()["journal_unreserved_bytes"]
                    .as_u64()
                    .unwrap();
                assert!(available < 8193);

                assert_eq!(
                    plane.request(&restart_all(1))["error"],
                    "IdentifyJournalCapacity"
                );
                plane.tick(&credentials).await;
                assert!(plane.operations.is_empty());
                assert!(
                    plane
                        .daemon
                        .entries
                        .values()
                        .all(|entry| !entry.restart_held)
                );
                assert_eq!(
                    [running_pid(&plane, &first), running_pid(&plane, &second)],
                    pids
                );

                // The denial rolled back both Bots' reservations. One replacement fits.
                assert_eq!(plane.request(&request("restart", 0, 2))["ok"], true);
                assert_eq!(
                    complete(&mut plane, 2).await["operation"]["state"],
                    "succeeded"
                );
                assert_ne!(running_pid(&plane, &first), pids[0]);
                assert_eq!(running_pid(&plane, &second), pids[1]);
                cleanup(&mut plane).await;
                drop(plane);
                std::fs::remove_dir_all(root).unwrap();
            }

            #[tokio::test]
            async fn manual_restart_reports_identify_exhaustion_and_leaves_no_retry() {
                let (mut plane, time) = fixture(1, 1, 1);
                let key = key(0, 0);
                plane.daemon.start(&key, &credentials).unwrap();
                let pid = running_pid(&plane, &key);
                time.send_replace(Duration::from_secs(30));

                let rejected = plane.request(&request("restart", 0, 2));
                assert_eq!(rejected["ok"], false);
                assert_eq!(rejected["error"], "IdentifyUnavailable");
                plane.tick(&credentials).await;
                assert!(plane.operations.is_empty());
                assert!(!plane.daemon.entries[&key].restart_held);
                assert_eq!(running_pid(&plane, &key), pid);
                cleanup(&mut plane).await;
            }

            #[tokio::test]
            async fn spacing_is_refused_before_stop_and_a_new_request_can_restart_at_the_boundary()
            {
                let (mut plane, time) = fixture(1, 1, 10);
                let key = key(0, 0);
                plane.daemon.start(&key, &credentials).unwrap();
                let pid = running_pid(&plane, &key);
                let before = plane.daemon.snapshots();
                let restart = |id| CommandRequest {
                    expect_bot_id: Some(key.bot_id.clone()),
                    ..request("restart", 0, id)
                };
                time.send_replace(Duration::from_millis(29_999));
                let refused = plane.request(&restart(1));
                assert_eq!(refused["ok"], false);
                assert_eq!(refused["error"], "IdentifyDeferred");
                assert_eq!(plane.daemon.snapshots(), before);
                assert!(!plane.daemon.entries[&key].restart_held);
                plane.tick(&credentials).await;
                assert_eq!(running_pid(&plane, &key), pid);
                assert!(plane.operations.is_empty());

                time.send_replace(Duration::from_secs(30));
                assert_eq!(plane.request(&restart(2))["ok"], true);
                assert_eq!(
                    complete(&mut plane, 2).await["operation"]["state"],
                    "succeeded"
                );
                assert_ne!(running_pid(&plane, &key), pid);
                assert_eq!(plane.request(&restart(3))["error"], "IdentifyDeferred");
                cleanup(&mut plane).await;
            }

            #[tokio::test]
            async fn failed_stop_releases_the_unused_slot_without_charging_it() {
                let (mut plane, time) = fixture(1, 2, 2);
                let old = key(0, 0);
                let other = key(0, 1);
                plane.daemon.start(&old, &credentials).unwrap();
                let recovery_key = plane.daemon.entries[&old].recovery_key;
                let pipe = plane
                    .daemon
                    .recovery
                    .child_mut(recovery_key)
                    .unwrap()
                    .unwrap()
                    .stdin
                    .take()
                    .unwrap();
                time.send_replace(Duration::from_secs(30));
                let mut restart = request("restart", 0, 1);
                restart.shards = Some(vec![0]);
                assert_eq!(plane.request(&restart)["ok"], true);
                let result = complete(&mut plane, 1).await;
                assert_eq!(result["operation"]["state"], "failed");
                assert_eq!(result["operation"]["shards"][0]["error"], "StopFailed");
                assert!(plane.daemon.has_child(&old));
                assert!(!plane.daemon.entries[&old].restart_held);
                plane.daemon.start(&other, &credentials).unwrap();
                running_pid(&plane, &other);
                plane
                    .daemon
                    .recovery
                    .child_mut(recovery_key)
                    .unwrap()
                    .unwrap()
                    .stdin = Some(pipe);
                // The first request failed before writing. Force is for fixture cleanup.
                plane.daemon.stop(&old, StopMode::Force).unwrap();
                cleanup(&mut plane).await;
            }

            #[tokio::test]
            async fn restart_shard_list_requires_budget_and_spacing_for_every_replacement() {
                let (mut plane, time) = fixture(1, 2, 3);
                let first = key(0, 0);
                let second = key(0, 1);
                plane.daemon.start(&first, &credentials).unwrap();
                time.send_replace(Duration::from_secs(30));
                plane.daemon.start(&second, &credentials).unwrap();
                let pids = [running_pid(&plane, &first), running_pid(&plane, &second)];

                let mut restart = request("restart", 0, 1);
                restart.shards = Some(vec![1, 0]);
                for (at, denied) in [(60, "IdentifyUnavailable"), (3630, "IdentifyDeferred")] {
                    time.send_replace(Duration::from_secs(at));
                    assert_eq!(plane.request(&restart)["error"], denied);
                    plane.tick(&credentials).await;
                    assert_eq!(
                        [running_pid(&plane, &first), running_pid(&plane, &second)],
                        pids
                    );
                    assert!(plane.operations.is_empty());
                }
                cleanup(&mut plane).await;
            }

            #[tokio::test]
            async fn restart_all_releases_earlier_bot_reservations_if_any_bot_is_denied() {
                let (mut plane, time) = fixture(2, 1, 2);
                let first = key(0, 0);
                let second = key(1, 0);
                plane.daemon.start(&first, &credentials).unwrap();
                time.send_replace(Duration::from_secs(1));
                plane.daemon.start(&second, &credentials).unwrap();
                let pids = [running_pid(&plane, &first), running_pid(&plane, &second)];

                time.send_replace(Duration::from_secs(30));
                assert_eq!(plane.request(&restart_all(1))["error"], "IdentifyDeferred");
                plane.tick(&credentials).await;
                assert_eq!(
                    [running_pid(&plane, &first), running_pid(&plane, &second)],
                    pids
                );
                assert!(plane.operations.is_empty());

                // The first Bot could reserve at t=30. Its reservation must
                // have rolled back when the second Bot was denied.
                plane
                    .daemon
                    .local_identify
                    .record("id-1", Duration::from_secs(1));
                time.send_replace(Duration::from_secs(31));
                assert_eq!(
                    plane.request(&restart_all(1))["error"],
                    "IdentifyUnavailable"
                );
                plane.tick(&credentials).await;
                assert_eq!(
                    [running_pid(&plane, &first), running_pid(&plane, &second)],
                    pids
                );

                time.send_replace(Duration::from_secs(3601));
                let access = super::super::super::update_control::ControlAccess {
                    subject: "restart-test".into(),
                    role: "admin".into(),
                    scope: Some(vec![first.clone(), second.clone()]),
                };
                assert_eq!(
                    plane.request_as(&restart_all(2), &access)["error"],
                    "control permission or scope denied"
                );
                let access = super::super::super::update_control::ControlAccess {
                    scope: None,
                    ..access
                };
                assert_eq!(plane.request_as(&restart_all(2), &access)["ok"], true);
                let done = complete(&mut plane, 2).await;
                assert_eq!(done["operation"]["state"], "succeeded");
                assert_ne!(running_pid(&plane, &first), pids[0]);
                assert_ne!(running_pid(&plane, &second), pids[1]);
                cleanup(&mut plane).await;
            }

            #[tokio::test]
            async fn pending_restart_reserves_its_bot_only_and_cannot_be_preempted_by_a_start() {
                let (mut plane, time) = fixture(2, 2, 2);
                let target = key(0, 0);
                let competitor = key(0, 1);
                let independent = key(1, 0);
                plane.daemon.start(&target, &credentials).unwrap();
                plane.daemon.start(&independent, &credentials).unwrap();
                let original = running_pid(&plane, &target);
                let other_pid = running_pid(&plane, &independent);
                time.send_replace(Duration::from_secs(30));
                let mut first = request("restart", 0, 1);
                first.shards = Some(vec![0]);
                assert_eq!(plane.request(&first)["ok"], true);

                // Issue another start while the accepted restart still owns
                // its slot and before its first stop has been sent.
                assert_eq!(
                    plane.daemon.start(&competitor, &credentials),
                    Err(DaemonError::IdentifyDeferred)
                );
                assert!(!plane.daemon.has_child(&competitor));
                assert_eq!(running_pid(&plane, &target), original);
                let mut other = request("restart", 1, 2);
                other.shards = Some(vec![0]);
                assert_eq!(plane.request(&other)["ok"], true);
                assert_eq!(
                    complete(&mut plane, 1).await["operation"]["state"],
                    "succeeded"
                );
                assert_eq!(
                    complete(&mut plane, 2).await["operation"]["state"],
                    "succeeded"
                );
                assert_ne!(running_pid(&plane, &target), original);
                assert_ne!(running_pid(&plane, &independent), other_pid);
                cleanup(&mut plane).await;
            }
        }

        #[test]
        fn gateway_update_terminal_audit_is_retried() {
            let dir = log_dir("gateway-audit-retry");
            let path = dir.join("audit.jsonl");
            let mut plane = plane(1, 1);
            plane.set_gateway_update_audit(path.clone());
            let req = request("gateway-update", 0, 91);
            let id = req.operation_id.clone().unwrap();
            let actor = ("local-bootstrap".to_owned(), "admin".to_owned());
            // Unit fixtures have no separated Core, so admit and finish directly.
            plane
                .audit_gateway_update("requested", &id, Some("id-0"), &actor, None)
                .unwrap();
            assert_eq!(plane.admit(&req, Some("id-0".into()))["ok"], true);
            plane.gateway_update_actors.insert(id.clone(), actor);
            plane.set_gateway_update_audit(dir.clone());
            let mut op = plane.operations.remove(&id).unwrap();
            plane.finish(&mut op);
            plane.operations.insert(id.clone(), op);
            let status = plane.request(&CommandRequest {
                command: "status".into(),
                ..Default::default()
            });
            assert_eq!(status["status"]["gateway_update_audit"]["pending"], 1);
            plane.set_gateway_update_audit(path.clone());
            plane.retry_pending_audit();
            let status = plane.request(&CommandRequest {
                command: "status".into(),
                ..Default::default()
            });
            assert_eq!(status["status"]["gateway_update_audit"]["pending"], 0);
            let text = std::fs::read_to_string(path).unwrap();
            let events: Vec<Value> = text
                .lines()
                .map(|line| serde_json::from_str(line).unwrap())
                .collect();
            assert_eq!(events.len(), 2);
            assert_eq!(events[1]["event"], "succeeded");
            assert_eq!(events[1]["operation_id"], id);
            let _ = std::fs::remove_dir_all(dir);
        }

        #[test]
        fn gateway_update_needs_an_admin_whose_scope_covers_the_bot() {
            use super::super::update_control::ControlAccess;
            let mut plane = plane(2, 1);
            let denied = "control permission or scope denied";
            let access = |role: &str, scope: Option<Vec<ShardKey>>| ControlAccess {
                subject: "operator".into(),
                role: role.into(),
                scope,
            };
            let own = Some(vec![ShardKey {
                bot_id: "id-0".into(),
                shard_id: 0,
            }]);
            let other = Some(vec![ShardKey {
                bot_id: "id-1".into(),
                shard_id: 0,
            }]);
            for refused in [
                access("viewer", None),
                access("updater", None),
                access("admin", other),
                access("nonsense", None),
            ] {
                let response = plane.request_as(&request("gateway-update", 0, 1), &refused);
                assert_eq!(response["error"], denied, "{response}");
            }
            // Allowed by role and scope, it reaches the daemon, which (not in the separated
            // layout here) declines for its own, different reason.
            for allowed in [access("admin", None), access("admin", own)] {
                let response = plane.request_as(&request("gateway-update", 0, 2), &allowed);
                assert_ne!(response["error"], denied, "{response}");
            }
            assert!(plane.operations.is_empty(), "nothing was admitted");
        }

        #[test]
        fn scoped_reads_and_lifecycle_do_not_expose_unpermitted_remote_placements() {
            let mut plane = plane(2, 1);
            let access = super::super::update_control::ControlAccess {
                subject: "scoped-viewer".into(),
                role: "viewer".into(),
                scope: Some(vec![ShardKey {
                    bot_id: "id-0".into(),
                    shard_id: 0,
                }]),
            };
            let mut read = CommandRequest {
                command: "status".into(),
                bot: Some("bot0".into()),
                ..Default::default()
            };
            assert_eq!(
                plane.request_as(&read, &access)["status"]["processes"]
                    .as_array()
                    .unwrap()
                    .len(),
                1
            );
            read.bot = Some("bot1".into());
            assert_eq!(plane.request_as(&read, &access)["ok"], false);
            assert_eq!(
                plane.request_as(&request("stop", 0, 1), &access)["ok"],
                false
            );
            plane.config.bots[0].shard_placement.push(
                serde_json::from_value(json!({"shard_id":1,"primary":"other","failover":[]}))
                    .unwrap(),
            );
            read.command = "shards".into();
            read.bot = Some("bot0".into());
            assert_eq!(plane.request_as(&read, &access)["ok"], false);
            assert_eq!(
                plane.request_as(
                    &read,
                    &super::super::update_control::ControlAccess::default()
                )["shards"]
                    .as_array()
                    .unwrap()
                    .len(),
                2
            );
        }
    }
}

/// Random daemon incarnation plus a checked, monotonically increasing sequence.
/// This is process identity metadata, never an ownership epoch or an IPC secret.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LaunchGeneration {
    pub daemon_instance: [u8; 16],
    pub sequence: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProcessIdentity {
    pub key: ShardKey,
    pub pid: u32,
    pub generation: LaunchGeneration,
    /// Wall-clock observation around spawn, NOT the OS process creation time.
    pub launch_observed_unix_ms: u128,
}

/// Native, boot-scoped process creation identity, obtained by a platform adapter.
/// Wall-clock spawn observations must never be substituted for these values.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProcessStartTime {
    pub boot_id: String,
    pub ticks: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OrphanRecord {
    pub identity: ProcessIdentity,
    pub os_start_time: Option<ProcessStartTime>,
}

/// Evidence must be collected from the OS AND an authenticated IPC peer, not
/// from a PID file alone. The platform/IPC adapters are not implemented here.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrphanObservation {
    pub identity: ProcessIdentity,
    pub os_start_time: Option<ProcessStartTime>,
    pub ipc_authenticated: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Reconciliation {
    Matches,
    IdentityMismatch,
    MissingProcessStartTime,
    ProcessStartTimeMismatch,
    Unauthenticated,
}

/// Matching evidence is a prerequisite only; this does not adopt, signal, or
/// authorize a process. A boolean here is not an implementation of IPC auth.
pub fn reconcile_orphan(record: &OrphanRecord, observed: &OrphanObservation) -> Reconciliation {
    if record.identity != observed.identity {
        return Reconciliation::IdentityMismatch;
    }
    let (Some(expected), Some(actual)) = (&record.os_start_time, &observed.os_start_time) else {
        return Reconciliation::MissingProcessStartTime;
    };
    if expected != actual {
        return Reconciliation::ProcessStartTimeMismatch;
    }
    if !observed.ipc_authenticated {
        return Reconciliation::Unauthenticated;
    }
    Reconciliation::Matches
}

/// Result of looking for a live Gateway of one shard in the OS process table.
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum GatewayLiveness {
    /// A process whose executable and `--management-path` match exists.
    Alive,
    /// The whole process table was read and no such process exists.
    NotAlive,
    /// The process table could not be read or judged: callers must refuse.
    Unknown,
}

/// Pure judgement over `/proc/*/cmdline` contents (NUL separated arguments).
/// True when some command line has the Gateway program as its executable (the
/// exact path, or the same file name) and `--management-path` equal to this
/// shard's management path (`--flag value` or `--flag=value`). Matching is
/// deliberately generous: a false "alive" only keeps the old refusal.
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
fn gateway_alive_in(cmdlines: &[Vec<u8>], program: &Path, management_path: &Path) -> bool {
    let program = program.to_string_lossy();
    let program = program.as_bytes();
    let program_name = program.rsplit(|b| *b == b'/').next().unwrap_or(program);
    let management = management_path.to_string_lossy();
    let management = management.as_bytes();
    const FLAG: &[u8] = b"--management-path";
    cmdlines.iter().any(|raw| {
        let mut args = raw.split(|b| *b == 0);
        let Some(argv0) = args.next() else {
            return false;
        };
        let name = argv0.rsplit(|b| *b == b'/').next().unwrap_or(argv0);
        let same_program = argv0 == program || (!program_name.is_empty() && name == program_name);
        if argv0.is_empty() || !same_program {
            return false;
        }
        let mut args = args.peekable();
        while let Some(arg) = args.next() {
            if arg == FLAG {
                if args.peek().is_some_and(|value| *value == management) {
                    return true;
                }
            } else if arg.len() > FLAG.len()
                && arg.starts_with(FLAG)
                && arg[FLAG.len()] == b'='
                && &arg[FLAG.len() + 1..] == management
            {
                return true;
            }
        }
        false
    })
}

#[cfg(target_os = "linux")]
fn proc_mount_shows_all_pids(mountinfo: &str) -> bool {
    let mut found = false;
    for line in mountinfo.lines() {
        let Some((mount, filesystem)) = line.split_once(" - ") else {
            return false;
        };
        if mount.split_ascii_whitespace().nth(4) != Some("/proc") {
            continue;
        }
        let mut fields = filesystem.split_ascii_whitespace();
        if fields.next() != Some("proc") {
            return false;
        }
        let Some(options) = fields.nth(1) else {
            return false;
        };
        if options.split(',').any(|option| {
            option.starts_with("hidepid=") && !matches!(option, "hidepid=off" | "hidepid=0")
        }) {
            return false;
        }
        found = true;
    }
    found
}

/// Reads every `/proc/<pid>/cmdline`. None when the table cannot be trusted
/// (no /proc, unreadable entry other than one that vanished meanwhile).
#[cfg(target_os = "linux")]
fn read_proc_cmdlines() -> Option<Vec<Vec<u8>>> {
    // hidepid can omit non-dumpable Gateways without returning a read error.
    let mounts = std::fs::read_to_string("/proc/self/mountinfo").ok()?;
    if !proc_mount_shows_all_pids(&mounts) {
        return None;
    }
    let mut lines = Vec::new();
    let mut seen_pid = false;
    for entry in std::fs::read_dir("/proc").ok()? {
        let entry = entry.ok()?;
        let name = entry.file_name();
        let Some(name) = name.to_str() else { continue };
        if name.is_empty() || !name.bytes().all(|b| b.is_ascii_digit()) {
            continue;
        }
        seen_pid = true;
        match std::fs::read(entry.path().join("cmdline")) {
            Ok(bytes) => lines.push(bytes),
            // The process exited between listing and reading (ENOENT or ESRCH=3).
            Err(e) if e.kind() == std::io::ErrorKind::NotFound || e.raw_os_error() == Some(3) => {}
            Err(_) => return None,
        }
    }
    seen_pid.then_some(lines)
}

#[cfg(target_os = "linux")]
fn scan_gateway_liveness(program: &Path, management_path: &Path) -> GatewayLiveness {
    match read_proc_cmdlines() {
        Some(lines) if gateway_alive_in(&lines, program, management_path) => GatewayLiveness::Alive,
        Some(_) => GatewayLiveness::NotAlive,
        None => GatewayLiveness::Unknown,
    }
}

/// Only Linux can judge; elsewhere the old "reservation blocks" rule stays.
#[cfg(not(target_os = "linux"))]
fn scan_gateway_liveness(_program: &Path, _management_path: &Path) -> GatewayLiveness {
    GatewayLiveness::Unknown
}

/// Removes a leftover launch reservation, but only when the shard's Gateway is
/// confirmed not running and the path is a regular file (not a symlink).
/// Prints one stderr line with the shard key and generation (no paths/secrets).
fn reclaim_stale_reservation(path: &Path, key: &ShardKey, liveness: GatewayLiveness) -> bool {
    if liveness != GatewayLiveness::NotAlive {
        return false;
    }
    match std::fs::symlink_metadata(path) {
        Ok(meta) if meta.file_type().is_file() => {}
        _ => return false,
    }
    let generation = std::fs::read(path)
        .ok()
        .and_then(|bytes| serde_json::from_slice::<(ShardKey, LaunchGeneration)>(&bytes).ok())
        .map(|(_, generation)| generation.sequence);
    if std::fs::remove_file(path).is_err() {
        return false;
    }
    match generation {
        Some(sequence) => eprintln!(
            "daemon: removed stale launch reservation (no live Gateway) bot_id={} shard_id={} generation={sequence}",
            key.bot_id, key.shard_id
        ),
        None => eprintln!(
            "daemon: removed stale launch reservation (no live Gateway) bot_id={} shard_id={} generation=unknown",
            key.bot_id, key.shard_id
        ),
    }
    true
}

#[cfg(test)]
mod stale_reservation_tests {
    use super::*;

    const PROGRAM: &str = "/opt/discord-rs/bin/src";
    const MANAGEMENT: &str = "/var/lib/discord-rs/runtime/bot-31/shard-0/gateway-management";

    fn cmdline(parts: &[&str]) -> Vec<u8> {
        let mut bytes = Vec::new();
        for part in parts {
            bytes.extend_from_slice(part.as_bytes());
            bytes.push(0);
        }
        bytes
    }
    fn alive(lines: &[Vec<u8>]) -> bool {
        gateway_alive_in(lines, Path::new(PROGRAM), Path::new(MANAGEMENT))
    }

    #[test]
    fn matching_program_and_management_path_is_alive() {
        let gateway = cmdline(&[
            PROGRAM,
            "--bot-id",
            "1",
            "--management-path",
            MANAGEMENT,
            "--token-env",
            "T",
        ]);
        assert!(alive(&[cmdline(&["bash"]), gateway.clone()]));
        assert!(alive(&[cmdline(&[
            PROGRAM,
            format!("--management-path={MANAGEMENT}").as_str()
        ])]));
        // Same executable name through another path still counts as alive.
        assert!(alive(&[cmdline(&[
            "/usr/local/bin/src",
            "--management-path",
            MANAGEMENT
        ])]));
    }

    #[test]
    fn other_shard_other_program_or_no_flag_is_not_alive() {
        let other_shard = cmdline(&[
            PROGRAM,
            "--management-path",
            "/var/lib/discord-rs/runtime/bot-31/shard-1/gateway-management",
        ]);
        // The Core worker of the same shard uses core-management, not this path.
        let core = cmdline(&[
            "/opt/discord-rs/bin/bot-core",
            "--management-path",
            "/var/lib/discord-rs/runtime/bot-31/shard-0/core-management",
        ]);
        let other_program = cmdline(&["/bin/tail", "--management-path", MANAGEMENT]);
        let no_value = cmdline(&[PROGRAM, "--management-path"]);
        let long = format!("{MANAGEMENT}x");
        let prefix = cmdline(&[PROGRAM, "--management-path", long.as_str()]);
        let value_without_flag = cmdline(&[PROGRAM, MANAGEMENT]);
        assert!(!alive(&[]));
        assert!(!alive(&[Vec::new(), other_shard, core, other_program]));
        assert!(!alive(&[no_value, prefix, value_without_flag]));
    }

    #[test]
    fn empty_or_unterminated_command_lines_do_not_panic() {
        assert!(!alive(&[Vec::new(), vec![0], vec![0, 0, 0]]));
        let mut unterminated = cmdline(&[PROGRAM, "--management-path"]);
        unterminated.extend_from_slice(MANAGEMENT.as_bytes());
        assert!(alive(&[unterminated]));
    }

    fn scratch(name: &str) -> PathBuf {
        let dir =
            std::env::temp_dir().join(format!("discord-rs-stale-{}-{name}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }
    fn key() -> ShardKey {
        ShardKey {
            bot_id: "31".into(),
            shard_id: 0,
        }
    }

    #[test]
    fn reservation_is_removed_only_when_confirmed_not_alive() {
        let dir = scratch("removal");
        let path = dir.join("gateway-management.botd-reservation");
        std::fs::write(&path, b"not json").unwrap();
        assert!(!reclaim_stale_reservation(
            &path,
            &key(),
            GatewayLiveness::Alive
        ));
        assert!(!reclaim_stale_reservation(
            &path,
            &key(),
            GatewayLiveness::Unknown
        ));
        assert!(path.exists());
        assert!(reclaim_stale_reservation(
            &path,
            &key(),
            GatewayLiveness::NotAlive
        ));
        assert!(!path.exists());
        // Nothing to remove: not reported as removed.
        assert!(!reclaim_stale_reservation(
            &path,
            &key(),
            GatewayLiveness::NotAlive
        ));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[cfg(unix)]
    #[test]
    fn symlink_or_directory_reservation_is_never_removed() {
        let dir = scratch("symlink");
        let target = dir.join("target");
        std::fs::write(&target, b"x").unwrap();
        let link = dir.join("link.botd-reservation");
        std::os::unix::fs::symlink(&target, &link).unwrap();
        assert!(!reclaim_stale_reservation(
            &link,
            &key(),
            GatewayLiveness::NotAlive
        ));
        assert!(link.exists() && target.exists());
        let folder = dir.join("folder.botd-reservation");
        std::fs::create_dir(&folder).unwrap();
        assert!(!reclaim_stale_reservation(
            &folder,
            &key(),
            GatewayLiveness::NotAlive
        ));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn filtered_proc_mount_cannot_prove_a_gateway_is_absent() {
        let mount = |options| format!("29 25 0:24 / /proc rw - proc proc rw,{options}");
        assert!(proc_mount_shows_all_pids(&mount("subset=pid")));
        assert!(proc_mount_shows_all_pids(&mount("hidepid=off,subset=pid")));
        assert!(!proc_mount_shows_all_pids(&mount(
            "hidepid=invisible,subset=pid"
        )));
        assert!(!proc_mount_shows_all_pids(&mount("hidepid=2")));
        assert!(!proc_mount_shows_all_pids(&mount("hidepid=noaccess")));
        assert!(!proc_mount_shows_all_pids(""));
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn proc_scan_does_not_see_this_test_binary_as_that_gateway() {
        // Reads the real /proc. This process is not a Gateway for MANAGEMENT.
        assert_eq!(
            scan_gateway_liveness(Path::new(PROGRAM), Path::new(MANAGEMENT)),
            GatewayLiveness::NotAlive
        );
    }
}

/// Fixed categories: never retain credential values, command output or OS error
/// text, which may contain deployment paths or other sensitive input.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum DaemonError {
    InvalidConfig,
    UnsupportedDistributedMode,
    RemoteManagementDenied,
    InvalidPolicy,
    InvalidSpec,
    InvalidProgram,
    UnknownBot,
    UnknownShard,
    InvalidState,
    IdentifyDeferred,
    IdentifyUnavailable,
    IdentifyJournalPending,
    IdentifyJournalBusy,
    IdentifyBootIdUnavailable,
    IdentifyJournalCapacity,
    RestartBudget,
    RestartDeferred,
    NoRuntime,
    MissingCredential,
    MissingSystemEnvironment,
    ReservationExists,
    ReservationIo,
    SpawnFailed,
    ChildExited,
    MonitorFailed,
    StopFailed,
    StopTimedOut,
    OperationSuperseded,
    ClockRegressed,
    TimeOverflow,
    GenerationExhausted,
    RandomUnavailable,
}
impl fmt::Display for DaemonError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "daemon error: {self:?}")
    }
}
impl std::error::Error for DaemonError {}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "state")]
pub enum ProcessState {
    Running,
    Exited {
        code: Option<i32>,
    },
    Failed {
        error: DaemonError,
        code: Option<i32>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProcessSnapshot {
    pub key: ShardKey,
    pub total_shards: u32,
    pub state: ProcessState,
    pub desired_running: bool,
    pub identity: Option<ProcessIdentity>,
    pub restarts: u32,
    /// Deadline in this daemon's monotonic clock domain, not a wall-clock time.
    pub retry_at_ms: Option<u128>,
    pub restart_exhausted: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub recovery: Option<serde_json::Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub identify_send_grant: Option<serde_json::Value>,
}

/// Reads one named reference immediately before spawn. Implementations must not
/// log values. The daemon does not cache credentials between attempts.
pub trait CredentialSource {
    fn read(&self, name: &str) -> Option<OsString>;
}
impl<F: Fn(&str) -> Option<OsString>> CredentialSource for F {
    fn read(&self, name: &str) -> Option<OsString> {
        self(name)
    }
}
pub struct EnvironmentCredentials;
impl CredentialSource for EnvironmentCredentials {
    fn read(&self, name: &str) -> Option<OsString> {
        std::env::var_os(name)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StopMode {
    /// Suppress retries without sending a shutdown request. Use `graceful_stop`
    /// to request cooperative exit through the owned child's anonymous stdin.
    WaitForExit,
    /// Explicit force only: kill the owned Child handle, never a saved PID.
    Force,
}

struct Entry {
    spec: ShardRuntimeSpec,
    snapshot: ProcessSnapshot,
    recovery_key: ChildKey,
    reservation: Option<PathBuf>,
    started: bool,
    graceful_stop: Option<Result<(), DaemonError>>,
    restart_held: bool,
    journal_start_pending: bool,
    nonce_retry: bool,
    /// Protect the next real Identify for this update's fallback recovery.
    update_fallback_reserved: bool,
    /// Only explicit local test configuration may select this. Never infer the
    /// mode from missing credentials, inherited environment, or child failure.
    mock_discord: Option<SocketAddr>,
    /// Metrics feed (best effort, never blocks) and the boundary timers it uses.
    feed: Option<crate::metrics_feed::FeedSender>,
    admission_since: Option<std::time::Instant>,
    stop_since: Option<std::time::Instant>,
    /// The Shard Core processes when botd runs them (separated layout); None = embedded.
    #[cfg(unix)]
    cores: Option<crate::core_units::CoreUnits>,
    /// Ownership epoch of the Gateway that is active now. A Gateway (re)started by botd
    /// attaches to the Cores with it; an update raises it.
    #[cfg(unix)]
    epoch: u64,
    /// A standby Gateway being warmed for an update.
    #[cfg(unix)]
    candidate: Option<Candidate>,
    /// The replaced Gateway, asked to stop and watched until it has.
    #[cfg(unix)]
    retiring: Option<Retiring>,
}

#[cfg(unix)]
struct Candidate {
    child: Child,
    identity: ProcessIdentity,
    started: std::time::Instant,
    ready_marker: PathBuf,
    /// Written by the standby once it has attached to every Core (it is then the Gateway
    /// the Cores serve).
    active_marker: PathBuf,
    /// Set when the activation frame was delivered.
    activated: Option<std::time::Instant>,
}

/// A marker counts only as a regular file (not a directory or a link put in its place).
#[cfg(unix)]
fn marker_present(path: &Path) -> bool {
    std::fs::symlink_metadata(path).is_ok_and(|meta| meta.file_type().is_file())
}

/// How long an activated standby may take to confirm that it holds the Cores.
#[cfg(unix)]
const ACTIVATION_CONFIRM_TIMEOUT: Duration = Duration::from_secs(20);

#[cfg(unix)]
struct Retiring {
    child: Child,
    since: std::time::Instant,
}

/// How long a standby Gateway may take to log in before the update is abandoned.
#[cfg(unix)]
const CANDIDATE_READY_TIMEOUT: Duration = Duration::from_secs(90);
/// How long a replaced Gateway may take to exit before it is killed (botd holds its handle).
#[cfg(unix)]
const RETIRING_GRACE: Duration = Duration::from_secs(30);

/// Progress of a Gateway update on one Shard.
#[cfg(unix)]
#[derive(Debug, PartialEq, Eq)]
pub enum UpdateProgress {
    Warming,
    Ready,
    Failed(DaemonError),
}

#[cfg(unix)]
fn prepare_status_dir(
    root: &Path,
    bot_id: &str,
    shard: u32,
    allow_non_tmpfs: bool,
) -> Option<PathBuf> {
    use std::os::unix::fs::{DirBuilderExt, MetadataExt};
    if !root.is_absolute()
        || root
            .components()
            .any(|c| c == std::path::Component::ParentDir)
        || !root.is_dir()
        || (!allow_non_tmpfs && !crate::core_status::is_tmpfs(root))
    {
        return None;
    }
    let bot = bot_id
        .bytes()
        .map(|b| format!("{b:02x}"))
        .collect::<String>();
    let mut dir = root.to_path_buf();
    for part in [format!("bot-{bot}"), format!("shard-{shard}")] {
        dir.push(part);
        match std::fs::DirBuilder::new().mode(0o700).create(&dir) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {}
            Err(_) => return None,
        }
        use std::os::unix::fs::OpenOptionsExt;
        let opened = std::fs::OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_NONBLOCK)
            .open(&dir)
            .ok()?;
        let meta = opened.metadata().ok()?;
        if (!allow_non_tmpfs && !crate::core_status::file_is_tmpfs(&opened))
            || !meta.file_type().is_dir()
            || meta.mode() & 0o7777 != 0o700
            || meta.uid() != unsafe { libc::geteuid() }
            || (!allow_non_tmpfs && !crate::core_status::is_tmpfs(&dir))
        {
            return None;
        }
    }
    Some(dir)
}

pub struct Daemon<C = MonotonicClock> {
    entries: BTreeMap<ShardKey, Entry>,
    recovery: ChildRecovery<RecoveryClock<C>>,
    clock: std::sync::Arc<C>,
    recovery_events: VecDeque<DaemonRecoveryEvent>,
    recovery_events_dropped: u64,
    local_identify: crate::local_identify::LocalIdentifyRegistry,
    last_now: Duration,
    clock_failed: bool,
    instance: [u8; 16],
    sequence: u64,
    metrics: crate::daemon_metrics::DaemonMetrics,
    log_checked: Option<std::time::Instant>,
}

#[derive(Clone, Copy)]
enum GatewayLaunch {
    Active,
    #[cfg(unix)]
    Standby,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum StartKind {
    Automatic,
    Manual,
    Restart,
}

impl Entry {
    fn add_gateway_args(&self, command: &mut Command) {
        command.args(&self.spec.gateway.args);
        if self.nonce_retry {
            command.arg(crate::discord_message::NONCE_RETRY_FLAG);
        }
    }

    fn acquire_identify(
        &self,
        registry: &mut crate::local_identify::LocalIdentifyRegistry,
        now: Duration,
        launch: GatewayLaunch,
        generation: LaunchGeneration,
        reserved_restart: bool,
    ) -> Result<crate::local_identify::LocalAdmission, crate::local_identify::JournalPending> {
        use crate::local_identify::LocalAdmission;
        if self.mock_discord.is_some() {
            return Ok(LocalAdmission::Granted);
        }
        let standby = match launch {
            GatewayLaunch::Active => false,
            #[cfg(unix)]
            GatewayLaunch::Standby => true,
        };
        registry.acquire_gateway(
            &self.snapshot.key,
            generation,
            now,
            standby,
            reserved_restart,
        )
    }
}

// Share the exact clock domain with the recovery engine without requiring C: Clone.
struct RecoveryClock<C>(std::sync::Arc<C>);
impl<C: Clock> Clock for RecoveryClock<C> {
    fn now(&self) -> Duration {
        self.0.now()
    }
    fn sleep_until(&self, deadline: Duration) -> crate::supervisor::Sleep<'_> {
        self.0.sleep_until(deadline)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DaemonRecoveryEvent {
    pub key: ShardKey,
    pub role: ChildRole,
    pub generation: u64,
    pub at_ms: u128,
    pub kind: RecoveryEventKind,
}

struct ProcessRecoveryHost<'a, S> {
    entries: &'a mut BTreeMap<ShardKey, Entry>,
    credentials: &'a S,
    instance: [u8; 16],
    sequence: &'a mut u64,
    manual: Option<&'a ShardKey>,
    reserved_restart: Option<&'a ShardKey>,
    clock: &'a dyn Clock,
    last_now: &'a mut Duration,
    clock_failed: &'a mut bool,
    local_identify: &'a mut crate::local_identify::LocalIdentifyRegistry,
    errors: BTreeMap<ShardKey, DaemonError>,
}
impl<S: CredentialSource> RecoveryHost for ProcessRecoveryHost<'_, S> {
    fn authorized(&mut self, key: ChildKey) -> bool {
        self.entries.values().any(|e| {
            e.recovery_key == key
                && e.snapshot.desired_running
                && (!e.restart_held || self.manual == Some(&e.snapshot.key))
        })
    }
    fn resume_available(&mut self, _: ChildKey) -> bool {
        false
    }
    fn try_acquire_identify(&mut self, key: ChildKey) -> IdentifyAdmission {
        self.try_acquire_identify_at(key, *self.last_now)
    }

    fn try_acquire_identify_at(&mut self, key: ChildKey, observed: Duration) -> IdentifyAdmission {
        use crate::local_identify::LocalAdmission;
        // A single local update owns the Bot's fallback opportunity until commit, a
        // pre-activation abort, or its recovery's Identify grant. Defer OTHER starts
        // (even manual ones) without consuming history. This deliberately serializes
        // recovery with a candidate rather than inventing a second budget counter.
        // The protected recovery still uses the normal spacing/window admission.
        let blocked = self
            .entries
            .values()
            .find(|entry| entry.recovery_key == key)
            .is_some_and(|current| {
                #[cfg(unix)]
                if current
                    .cores
                    .as_ref()
                    .is_some_and(|cores| cores.manual_restart_reserved())
                {
                    return true;
                }
                if current.mock_discord.is_some() {
                    return false;
                }
                #[cfg(unix)]
                if current.update_fallback_reserved && current.candidate.is_some() {
                    // Do not spend the fallback on the old Gateway while its candidate
                    // is still alive, even for callers outside ControlPlane's hold.
                    return true;
                }
                self.entries.values().any(|entry| {
                    entry.update_fallback_reserved
                        && entry.recovery_key.bot_id == key.bot_id
                        && entry.recovery_key != key
                })
            });
        let entry = self
            .entries
            .values_mut()
            .find(|e| e.recovery_key == key)
            .unwrap();
        // Identify wait: first time this launch asked for admission until it
        // was granted. Unavailable keeps the timer running.
        let since = *entry
            .admission_since
            .get_or_insert_with(std::time::Instant::now);
        let reserved_restart = self.reserved_restart == Some(&entry.snapshot.key);
        let decision = if blocked && !reserved_restart {
            Ok(LocalAdmission::Pending {
                retry_after: Duration::ZERO,
            })
        } else {
            let now = self.clock.now();
            if *self.clock_failed || now < observed.max(*self.last_now) {
                *self.clock_failed = true;
                return IdentifyAdmission::Pending;
            }
            *self.last_now = now;
            entry.acquire_identify(
                self.local_identify,
                now,
                GatewayLaunch::Active,
                LaunchGeneration {
                    daemon_instance: self.instance,
                    sequence: self.sequence.saturating_add(1),
                },
                reserved_restart,
            )
        };
        match decision {
            Ok(LocalAdmission::Granted) => {
                entry.update_fallback_reserved = false;
                entry.admission_since = None;
                if let Some(feed) = &entry.feed {
                    feed.observe(crate::metrics_feed::Observation::IdentifyWait(
                        since.elapsed(),
                    ));
                }
                IdentifyAdmission::Granted
            }
            denial if self.manual == Some(&entry.snapshot.key) => {
                let error = match denial {
                    Err(_) => DaemonError::IdentifyJournalPending,
                    Ok(LocalAdmission::Exhausted { .. }) => DaemonError::IdentifyUnavailable,
                    _ => DaemonError::IdentifyDeferred,
                };
                self.errors.insert(entry.snapshot.key.clone(), error);
                // The explicit start is cancelled by its caller. Keep this
                // stopped slot reusable for a later management operation.
                IdentifyAdmission::Pending
            }
            Ok(LocalAdmission::Exhausted { .. }) => IdentifyAdmission::Unavailable,
            Ok(LocalAdmission::Pending { .. }) | Err(_) => IdentifyAdmission::Pending,
        }
    }
    fn cancel_identify(&mut self, _: ChildKey) {}
    fn spawn(&mut self, request: StartRequest) -> std::io::Result<Child> {
        let entry = self
            .entries
            .values_mut()
            .find(|e| e.recovery_key == request.key)
            .unwrap();
        #[cfg(target_os = "linux")]
        let previous_sequence = *self.sequence;
        match Daemon::<MonotonicClock>::launch(
            entry,
            self.credentials,
            self.instance,
            self.sequence,
            self.local_identify,
        ) {
            Ok(child) => Ok(child),
            Err(error) => {
                #[cfg(target_os = "linux")]
                if let Some(service) = &mut self.local_identify.send_grants {
                    // This generation was charged even if validation failed before spawn.
                    let sequence = previous_sequence.saturating_add(1);
                    *self.sequence = sequence;
                    service.release_first_grant_space(
                        &entry.snapshot.key,
                        &LaunchGeneration {
                            daemon_instance: self.instance,
                            sequence,
                        },
                    );
                }
                self.errors.insert(entry.snapshot.key.clone(), error);
                Err(std::io::Error::other("child launch failed"))
            }
        }
    }
}

impl Daemon {
    /// Only explicitly local primary assignments in standalone mode may launch.
    /// Unresolved auto counts fail closed; no Discord or secret access occurs.
    pub fn from_config(
        config: &Config,
        resolved_auto: &HashMap<String, u32>,
        root: &Path,
        policy: RestartPolicy,
    ) -> Result<Self, DaemonError> {
        if config.cluster.mode != ClusterMode::Standalone {
            return Err(DaemonError::UnsupportedDistributedMode);
        }
        if config.node.remote_bind.is_some() {
            return Err(DaemonError::RemoteManagementDenied);
        }
        crate::botref::validate_bots(&config.bots).map_err(|_| DaemonError::InvalidConfig)?;
        let intents = config
            .local_primary_intents(resolved_auto)
            .map_err(|_| DaemonError::InvalidConfig)?;
        // Validate the entire deployment, including Bots with no local shard.
        // A later placement change must not silently reinterpret another Bot's
        // credential source (Windows names are case insensitive).
        let mut references = HashSet::new();
        if config
            .bots
            .iter()
            .any(|bot| !references.insert(bot.token_env.to_ascii_uppercase()))
        {
            return Err(DaemonError::InvalidConfig);
        }
        let specs = shard_runtime::plan(&intents, root).map_err(|_| DaemonError::InvalidSpec)?;
        Self::new(specs, policy)
    }

    pub fn new(specs: Vec<ShardRuntimeSpec>, policy: RestartPolicy) -> Result<Self, DaemonError> {
        Self::with_clock(specs, policy, MonotonicClock::default())
    }
}

fn env_name(name: &str) -> bool {
    let mut chars = name.chars();
    chars
        .next()
        .is_some_and(|c| c == '_' || c.is_ascii_alphabetic())
        && chars.all(|c| c == '_' || c.is_ascii_alphanumeric())
}

fn safe_path(path: &Path) -> bool {
    path.is_absolute() && !path.components().any(|c| c == Component::ParentDir)
}

fn validate_specs(specs: &[ShardRuntimeSpec]) -> Result<(), DaemonError> {
    // Reuse the planner's identity, credential separation and count validation.
    let intents = specs
        .iter()
        .map(|s| crate::placement::LocalShardIntent {
            bot_id: s.bot_id.clone(),
            shard_id: s.shard_id,
            total_shards: s.total_shards,
            token_env: s
                .gateway
                .environment
                .credential_refs
                .first()
                .cloned()
                .unwrap_or_default(),
        })
        .collect::<Vec<_>>();
    let root = std::env::current_dir().map_err(|_| DaemonError::InvalidSpec)?;
    shard_runtime::plan(&intents, &root).map_err(|_| DaemonError::InvalidSpec)?;
    let credential_names = intents
        .iter()
        .map(|i| i.token_env.to_ascii_uppercase())
        .collect::<HashSet<_>>();
    let mut paths = HashSet::new();
    for spec in specs {
        if spec.gateway.role != Role::Gateway
            || spec.core.role != Role::Core
            || spec.gateway.environment.credential_refs.len() != 1
            || !spec.core.environment.credential_refs.is_empty()
        {
            return Err(DaemonError::InvalidSpec);
        }
        for process in [&spec.gateway, &spec.core] {
            if !process.environment.clear_inherited || process.program.is_empty() {
                return Err(DaemonError::InvalidSpec);
            }
            let mut names = HashSet::new();
            for (name, value) in &process.environment.variables {
                if !env_name(name)
                    || value.contains('\0')
                    || name.eq_ignore_ascii_case("SYSTEMROOT")
                    || name.eq_ignore_ascii_case(crate::placement::GATEWAY_TOKEN_ENV)
                    || credential_names.contains(&name.to_ascii_uppercase())
                    || !names.insert(name.to_ascii_uppercase())
                {
                    return Err(DaemonError::InvalidSpec);
                }
            }
        }
        for path in [
            &spec.ipc_path,
            &spec.gateway.state_path,
            &spec.gateway.management_path,
            &spec.core.state_path,
            &spec.core.management_path,
        ] {
            if !safe_path(path) || !paths.insert(path.as_os_str().to_ascii_lowercase()) {
                return Err(DaemonError::InvalidSpec);
            }
        }
    }
    Ok(())
}

impl<C: Clock> Daemon<C> {
    pub fn with_clock(
        specs: Vec<ShardRuntimeSpec>,
        policy: RestartPolicy,
        clock: C,
    ) -> Result<Self, DaemonError> {
        if policy.max_restarts_in_window == 0
            || policy.window.is_zero()
            || policy.initial_backoff.is_zero()
            || policy.max_backoff < policy.initial_backoff
            || policy.circuit_breaker_after == 0
            || policy.circuit_open_for.is_zero()
        {
            return Err(DaemonError::InvalidPolicy);
        }
        validate_specs(&specs)?;
        let metric_keys: Vec<ShardKey> = specs
            .iter()
            .map(|spec| ShardKey {
                bot_id: spec.bot_id.clone(),
                shard_id: spec.shard_id,
            })
            .collect();
        let metrics = crate::daemon_metrics::DaemonMetrics::new(&metric_keys);
        let mut instance = [0; 16];
        getrandom::fill(&mut instance).map_err(|_| DaemonError::RandomUnavailable)?;
        let mut bot_keys = HashMap::new();
        let entries: BTreeMap<ShardKey, Entry> = specs
            .into_iter()
            .map(|spec| {
                let key = ShardKey {
                    bot_id: spec.bot_id.clone(),
                    shard_id: spec.shard_id,
                };
                let snapshot = ProcessSnapshot {
                    key: key.clone(),
                    total_shards: spec.total_shards,
                    state: ProcessState::Exited { code: None },
                    desired_running: false,
                    identity: None,
                    restarts: 0,
                    retry_at_ms: None,
                    restart_exhausted: false,
                    recovery: None,
                    identify_send_grant: None,
                };
                // Placement permits named Bot IDs too; these numeric keys are
                // local engine indexes only. Public events retain the real ID.
                let next = bot_keys.len() as u64 + 1;
                let bot_id = *bot_keys.entry(key.bot_id.clone()).or_insert(next);
                let recovery_key = ChildKey {
                    bot_id: crate::protocol::BotId(bot_id),
                    shard_id: key.shard_id,
                    role: ChildRole::Gateway,
                };
                let feed = metrics.sender(&key);
                (
                    key,
                    Entry {
                        spec,
                        snapshot,
                        recovery_key,
                        reservation: None,
                        started: false,
                        graceful_stop: None,
                        restart_held: false,
                        journal_start_pending: false,
                        nonce_retry: false,
                        update_fallback_reserved: false,
                        mock_discord: None,
                        feed,
                        admission_since: None,
                        stop_since: None,
                        #[cfg(unix)]
                        cores: None,
                        #[cfg(unix)]
                        epoch: 0,
                        #[cfg(unix)]
                        candidate: None,
                        #[cfg(unix)]
                        retiring: None,
                    },
                )
            })
            .collect();
        let last_now = clock.now();
        let clock = std::sync::Arc::new(clock);
        let mut recovery = ChildRecovery::new(
            RecoveryClock(clock.clone()),
            RecoveryConfig {
                restart: policy,
                max_startup_failures: u32::MAX,
                ..RecoveryConfig::default()
            },
            entries.len().max(1),
        )
        .map_err(|_| DaemonError::InvalidPolicy)?;
        recovery.use_legacy_process_contract();
        Ok(Self {
            entries,
            recovery,
            clock,
            recovery_events: VecDeque::new(),
            recovery_events_dropped: 0,
            local_identify: crate::local_identify::LocalIdentifyRegistry::default(),
            last_now,
            clock_failed: false,
            instance,
            sequence: 0,
            metrics,
            log_checked: None,
        })
    }

    /// Read-only advisory metrics owned by this daemon (see `daemon_metrics`).
    pub fn metrics(&self) -> &crate::daemon_metrics::DaemonMetrics {
        &self.metrics
    }

    /// Explicit trusted artifact path, before ANY launch attempt. No shell or
    /// implicit argument rewriting is used. The caller validates artifact trust.
    pub fn set_gateway_program(&mut self, program: &Path) -> Result<(), DaemonError> {
        if self.entries.values().any(|e| e.started) {
            return Err(DaemonError::InvalidState);
        }
        if !safe_path(program) {
            return Err(DaemonError::InvalidProgram);
        }
        for entry in self.entries.values_mut() {
            entry.spec.gateway.program = program.as_os_str().to_owned();
        }
        Ok(())
    }

    /// Replace the exact Bot ID allowlist before any launch attempt. All
    /// Gateway launches add the daemon-owned flag separately from custom args.
    pub fn configure_create_message_nonce_retry(
        &mut self,
        config: &Config,
        bot_ids: &HashSet<String>,
    ) -> Result<(), DaemonError> {
        if self.entries.values().any(|entry| entry.started) {
            return Err(DaemonError::InvalidState);
        }
        // Validate against the full deployment, including Bots that have no local shard.
        if bot_ids
            .iter()
            .any(|id| !config.bots.iter().any(|bot| &bot.bot_id == id))
        {
            return Err(DaemonError::UnknownBot);
        }
        for entry in self.entries.values_mut() {
            entry.nonce_retry = bot_ids.contains(&entry.spec.bot_id);
        }
        Ok(())
    }

    /// Enable send-time admission before launching any local Gateway.
    #[cfg(target_os = "linux")]
    pub fn enable_identify_send_grants(&mut self, root: &Path) -> Result<(), DaemonError> {
        if self.sequence != 0
            || self.local_identify.send_grants.is_some()
            || self
                .entries
                .values()
                .any(|entry| entry.started || entry.mock_discord.is_some())
        {
            return Err(DaemonError::InvalidState);
        }
        self.local_identify.send_grants = Some(crate::identify_grant::service::Service::open(
            root,
            self.local_identify.policy(),
        )?);
        Ok(())
    }

    /// Explicit token-free Discord simulation. Gateways connect out to the
    /// caller's loopback collector; no listener or remote management is exposed.
    /// Configuration is frozen after the first launch attempt, including failed
    /// attempts. Normal launches never fall back to this mode.
    pub fn enable_mock_discord(&mut self, address: SocketAddr) -> Result<(), DaemonError> {
        #[cfg(target_os = "linux")]
        if self.local_identify.send_grants.is_some() {
            return Err(DaemonError::InvalidState);
        }
        if self.entries.values().any(|entry| entry.started) {
            return Err(DaemonError::InvalidState);
        }
        if !address.ip().is_loopback() || address.port() == 0 {
            return Err(DaemonError::InvalidSpec);
        }
        for entry in self.entries.values_mut() {
            entry.mock_discord = Some(address);
        }
        Ok(())
    }

    /// Separated layout (opt-in): botd runs each Shard Cores and the Gateway connects to
    /// them. Before any launch. The Core directory is the Shard planned `ipc_path`.
    #[cfg(unix)]
    pub fn enable_separate_cores(&mut self) -> Result<(), DaemonError> {
        if self.entries.values().any(|entry| entry.started) {
            return Err(DaemonError::InvalidState);
        }
        for entry in self.entries.values_mut() {
            let bot_id = entry
                .spec
                .bot_id
                .parse::<u64>()
                .ok()
                .filter(|id| id.to_string() == entry.spec.bot_id)
                .ok_or(DaemonError::InvalidSpec)?;
            let shard = crate::protocol::Shard::new(entry.spec.shard_id, entry.spec.total_shards)
                .map_err(|_| DaemonError::InvalidSpec)?;
            let mut cores = crate::core_units::CoreUnits::new(
                entry.spec.ipc_path.clone(),
                crate::protocol::BotId(bot_id),
                shard,
                self.instance,
            );
            cores.set_epoch_floor(entry.epoch);
            entry.cores = Some(cores);
        }
        Ok(())
    }

    /// Configure optional, best-effort status files before starting any shard.
    #[cfg(unix)]
    pub fn configure_core_status(
        &mut self,
        root: Option<&Path>,
        allow_non_tmpfs: bool,
        interval_ms: Option<u64>,
    ) {
        for entry in self.entries.values_mut() {
            if let Some(cores) = &mut entry.cores {
                let dir = root.and_then(|root| {
                    prepare_status_dir(
                        root,
                        &entry.spec.bot_id,
                        entry.spec.shard_id,
                        allow_non_tmpfs,
                    )
                });
                cores.configure_status(dir, allow_non_tmpfs, interval_ms);
            }
        }
    }

    /// Whether this daemon runs the Cores (separated layout).
    #[cfg(unix)]
    pub fn separate_cores(&self) -> bool {
        self.entries.values().any(|entry| entry.cores.is_some())
    }

    fn gateway_update_fallback_pending(&self, bot_id: &str) -> bool {
        self.entries
            .values()
            .any(|entry| entry.snapshot.key.bot_id == bot_id && entry.update_fallback_reserved)
    }

    fn check_restart_writer(&mut self) -> Result<(), DaemonError> {
        #[cfg(target_os = "linux")]
        if let Some(service) = &mut self.local_identify.send_grants {
            return service.check_restart_writer();
        }
        Ok(())
    }

    fn reserve_restart_identify(&mut self, targets: &[ShardKey]) -> Result<(), DaemonError> {
        use crate::local_identify::LocalAdmission;
        let now = self.now()?;
        self.sequence
            .checked_add(targets.len() as u64)
            .ok_or(DaemonError::GenerationExhausted)?;
        let mut slots_by_bot = BTreeMap::new();
        for key in targets {
            let entry = &self.entries[key];
            self.recovery
                .check_replacement(entry.recovery_key, now)
                .map_err(|reason| match reason {
                    DegradedReason::RestartBudget => DaemonError::RestartBudget,
                    DegradedReason::RestartWindow => DaemonError::RestartDeferred,
                    DegradedReason::GenerationExhausted => DaemonError::GenerationExhausted,
                    _ => DaemonError::InvalidState,
                })?;
            if self.gateway_update_fallback_pending(&key.bot_id) {
                return Err(DaemonError::IdentifyDeferred);
            }
            #[cfg(unix)]
            if entry
                .cores
                .as_ref()
                .is_some_and(|cores| cores.manual_restart_reserved())
            {
                return Err(DaemonError::IdentifyDeferred);
            }
            if entry.mock_discord.is_none() {
                *slots_by_bot.entry(key.bot_id.as_str()).or_insert(0) += 1;
            }
        }
        let mut reserved = Vec::new();
        for (bot, slots) in slots_by_bot {
            let error = match self.local_identify.reserve_restart(bot, now, slots) {
                LocalAdmission::Granted => {
                    reserved.push(bot);
                    continue;
                }
                LocalAdmission::Pending { .. } => DaemonError::IdentifyDeferred,
                LocalAdmission::Exhausted { .. } => DaemonError::IdentifyUnavailable,
            };
            for bot in reserved {
                self.local_identify.release_restart(bot);
            }
            return Err(error);
        }
        #[cfg(target_os = "linux")]
        if let Some(service) = &mut self.local_identify.send_grants
            && let Err(error) = service.reserve_restart_space(targets, self.instance)
        {
            for bot in reserved {
                self.local_identify.release_restart(bot);
            }
            return Err(error);
        }
        Ok(())
    }

    fn release_restart_identify(&mut self, targets: &[ShardKey]) {
        #[cfg(target_os = "linux")]
        if let Some(service) = &mut self.local_identify.send_grants {
            service.release_restart_space(targets);
        }
        for key in targets {
            self.local_identify.release_restart(&key.bot_id);
        }
    }

    /// Starts a standby Gateway for `key` from the same program path (the operator has put
    /// the new build there). It logs in to Discord with its own session and holds events;
    /// it does not touch the Cores until `activate_gateway_update`.
    #[cfg(unix)]
    pub fn begin_gateway_update(
        &mut self,
        key: &ShardKey,
        credentials: &impl CredentialSource,
    ) -> Result<bool, DaemonError> {
        tokio::runtime::Handle::try_current().map_err(|_| DaemonError::NoRuntime)?;
        let now = self.now()?;
        // A prior failed update may still be waiting for its fallback Identify.
        let fallback_pending = self.gateway_update_fallback_pending(&key.bot_id);
        let sequence = self
            .sequence
            .checked_add(1)
            .ok_or(DaemonError::GenerationExhausted)?;
        let entry = self.entries.get_mut(key).ok_or(DaemonError::UnknownShard)?;
        let running = self
            .recovery
            .snapshot(entry.recovery_key)
            .is_ok_and(|s| s.pid.is_some());
        let Some(cores) = entry.cores.as_ref() else {
            return Err(DaemonError::InvalidState);
        };
        if cores.manual_restart_reserved()
            || !entry.started
            || !running
            || entry.candidate.is_some()
            || entry.retiring.is_some()
        {
            return Err(DaemonError::InvalidState);
        }
        if fallback_pending {
            return Ok(false);
        }
        let spec = &entry.spec.gateway;
        if !safe_path(Path::new(&spec.program)) {
            return Err(DaemonError::InvalidProgram);
        }
        use crate::local_identify::LocalAdmission;
        let generation = LaunchGeneration {
            daemon_instance: self.instance,
            sequence,
        };
        match entry.acquire_identify(
            &mut self.local_identify,
            now,
            GatewayLaunch::Standby,
            generation.clone(),
            false,
        ) {
            Ok(LocalAdmission::Granted) => {}
            Ok(LocalAdmission::Pending { .. }) | Err(_) => return Ok(false),
            Ok(LocalAdmission::Exhausted { .. }) => return Err(DaemonError::IdentifyUnavailable),
        }
        #[cfg(target_os = "linux")]
        if self.local_identify.send_grants.is_some() {
            // The launch charge owns this generation from now on, including
            // admission followed by a missing credential or a spawn error.
            self.sequence = sequence;
        }
        let [name] = spec.environment.credential_refs.as_slice() else {
            return Err(DaemonError::InvalidSpec);
        };
        if !crate::placement::valid_token_env(name) {
            return Err(DaemonError::InvalidSpec);
        }
        // The explicit loopback simulation takes no credential, exactly like a normal launch.
        let credential = if entry.mock_discord.is_none() {
            Some(
                credentials
                    .read(name)
                    .filter(|value| !value.is_empty())
                    .ok_or(DaemonError::MissingCredential)?,
            )
        } else {
            None
        };
        let marker = spec.state_path.join("standby-ready");
        let active_marker = spec.state_path.join("standby-active");
        // A leftover marker would let this update pass a step it has not reached, so a marker
        // that cannot be removed stops the update before anything starts.
        for stale in [&marker, &active_marker] {
            match std::fs::remove_file(stale) {
                Ok(()) => {}
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(_) => return Err(DaemonError::ReservationIo),
            }
        }
        // The standby writes to the same `gateway.log` as every other Gateway of this shard
        // (append mode: each write lands whole at the end, but formatted output can take several
        // writes, so during the short overlap the old and the new Gateway can interleave inside
        // a line; no line-level guarantee is made). One file means an operator reading
        // `gateway.log` always sees the current Gateway; a separate candidate file left the live
        // Gateway's output elsewhere.
        let log = spec.management_path.parent().and_then(|dir| {
            let path = dir.join("gateway.log");
            crate::core_units::open_append_private(&path).ok()
        });
        let (out, err) = match log.and_then(|file| file.try_clone().ok().map(|c| (file, c))) {
            Some((a, b)) => (Stdio::from(a), Stdio::from(b)),
            None => (Stdio::null(), Stdio::null()),
        };
        let mut command = Command::new(&spec.program);
        entry.add_gateway_args(&mut command);
        command
            .arg("--core-dir")
            .arg(cores.dir())
            .arg("--standby")
            .env_clear()
            .envs(&spec.environment.variables)
            .env("BOT_ID", &entry.spec.bot_id)
            .env("SHARD_ID", entry.spec.shard_id.to_string())
            .env("TOTAL_SHARDS", entry.spec.total_shards.to_string())
            .env("GENERATION_ID", sequence.to_string())
            .stdin(Stdio::piped())
            .stdout(out)
            .stderr(err)
            .kill_on_drop(false)
            .process_group(0);
        if let Some(address) = entry.mock_discord {
            command
                .arg("--mock-discord")
                .arg(address.to_string())
                .env_remove("DISCORD_TOKEN")
                .env_remove(crate::placement::GATEWAY_TOKEN_ENV);
        } else if let Some(credential) = credential {
            command.env(crate::placement::GATEWAY_TOKEN_ENV, credential);
        }
        #[cfg(target_os = "linux")]
        let grant_fd = self
            .local_identify
            .send_grants
            .as_mut()
            .map(|service| service.attach(&mut command, key, generation.clone(), true))
            .transpose()
            .map_err(|_| DaemonError::SpawnFailed)?;
        let launch_observed_unix_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| DaemonError::TimeOverflow)?
            .as_millis();
        let child = command.spawn().map_err(|_| DaemonError::SpawnFailed)?;
        let identity = ProcessIdentity {
            key: key.clone(),
            pid: child
                .id()
                .expect("newly spawned candidate has a process id"),
            generation,
            launch_observed_unix_ms,
        };
        #[cfg(target_os = "linux")]
        drop(grant_fd);
        if entry.mock_discord.is_none() {
            entry.update_fallback_reserved = true;
        }
        self.sequence = sequence;
        entry.candidate = Some(Candidate {
            child,
            identity,
            started: std::time::Instant::now(),
            ready_marker: marker,
            active_marker,
            activated: None,
        });
        Ok(true)
    }

    /// Where the standby Gateway is. A failure kills and forgets it.
    #[cfg(unix)]
    pub fn gateway_update_progress(&mut self, key: &ShardKey) -> UpdateProgress {
        let Some(entry) = self.entries.get_mut(key) else {
            return UpdateProgress::Failed(DaemonError::UnknownShard);
        };
        let Some(candidate) = entry.candidate.as_mut() else {
            return UpdateProgress::Failed(DaemonError::InvalidState);
        };
        let failure = match candidate.child.try_wait() {
            Ok(Some(_)) => Some(DaemonError::ChildExited),
            Err(_) => Some(DaemonError::MonitorFailed),
            Ok(None) if marker_present(&candidate.ready_marker) => return UpdateProgress::Ready,
            Ok(None) if candidate.started.elapsed() >= CANDIDATE_READY_TIMEOUT => {
                Some(DaemonError::StopTimedOut)
            }
            Ok(None) => None,
        };
        match failure {
            None => UpdateProgress::Warming,
            Some(error) => {
                if let Some(mut candidate) = entry.candidate.take() {
                    let _ = candidate.child.start_kill();
                    let _ = std::fs::remove_file(&candidate.ready_marker);
                }
                entry.update_fallback_reserved = false;
                UpdateProgress::Failed(error)
            }
        }
    }

    /// Abandons a standby Gateway that has not been activated.
    #[cfg(unix)]
    pub fn cancel_gateway_update(&mut self, key: &ShardKey) {
        #[cfg(target_os = "linux")]
        self.cancel_journal_launch(key, true);
        if let Some(entry) = self.entries.get_mut(key)
            && let Some(mut candidate) = entry.candidate.take()
        {
            if candidate.activated.is_none() {
                entry.update_fallback_reserved = false;
            }
            let _ = candidate.child.start_kill();
            let _ = std::fs::remove_file(&candidate.ready_marker);
            let _ = std::fs::remove_file(&candidate.active_marker);
        }
    }

    /// Step 1 of making the (ready) standby Gateway the active one: tell it to attach to the
    /// Cores with the next epoch. That takes the Cores from the running Gateway, so from here
    /// the old Gateway can no longer serve and the epoch is raised for good: any Gateway botd
    /// starts later attaches above it. Nothing else changes until the standby confirms
    /// (`gateway_activation_progress`) and `commit_gateway_update` hands the Shard over.
    #[cfg(unix)]
    pub async fn send_gateway_activation(&mut self, key: &ShardKey) -> Result<(), DaemonError> {
        let entry = self.entries.get_mut(key).ok_or(DaemonError::UnknownShard)?;
        let Some(candidate) = entry.candidate.as_mut() else {
            return Err(DaemonError::InvalidState);
        };
        let epoch = entry.epoch + 1;
        let frame = serde_json::to_vec(&shard_runtime::GatewayControl::Activate { epoch })
            .map_err(|_| DaemonError::StopFailed)?;
        let delivered = match candidate.child.stdin.as_mut() {
            Some(pipe) => {
                tokio::time::timeout(Duration::from_secs(2), async {
                    pipe.write_all(&frame).await?;
                    pipe.write_all(b"\n").await?;
                    pipe.flush().await
                })
                .await
            }
            None => Ok(Err(std::io::Error::other("no stdin"))),
        };
        if !matches!(delivered, Ok(Ok(()))) {
            self.cancel_gateway_update(key);
            return Err(DaemonError::StopFailed);
        }
        entry.epoch = epoch;
        if let Some(cores) = entry.cores.as_mut() {
            cores.set_epoch_floor(epoch);
        }
        let _ = std::fs::remove_file(&candidate.ready_marker);
        candidate.activated = Some(std::time::Instant::now());
        Ok(())
    }

    /// Whether the activated standby has confirmed that it holds the Cores. `Ready` means
    /// confirmed; a standby that exited or stayed silent too long is `Failed`.
    #[cfg(unix)]
    pub fn gateway_activation_progress(&mut self, key: &ShardKey) -> UpdateProgress {
        let Some(entry) = self.entries.get_mut(key) else {
            return UpdateProgress::Failed(DaemonError::UnknownShard);
        };
        let Some(candidate) = entry.candidate.as_mut() else {
            return UpdateProgress::Failed(DaemonError::InvalidState);
        };
        let Some(activated) = candidate.activated else {
            return UpdateProgress::Failed(DaemonError::InvalidState);
        };
        match candidate.child.try_wait() {
            Ok(Some(_)) => return UpdateProgress::Failed(DaemonError::ChildExited),
            Err(_) => return UpdateProgress::Failed(DaemonError::MonitorFailed),
            Ok(None) => {}
        }
        if marker_present(&candidate.active_marker) {
            UpdateProgress::Ready
        } else if activated.elapsed() >= ACTIVATION_CONFIRM_TIMEOUT {
            UpdateProgress::Failed(DaemonError::StopTimedOut)
        } else {
            UpdateProgress::Warming
        }
    }

    /// The activation did not succeed. The running Gateway has already lost its Cores, so it
    /// is killed (not asked to stop: a clean exit would be read as "stopped on purpose") and
    /// botd restarts it, now with the raised epoch. The standby, if any, is killed too.
    #[cfg(unix)]
    pub fn fail_gateway_activation(&mut self, key: &ShardKey) {
        self.cancel_gateway_update(key);
        let Some(entry) = self.entries.get_mut(key) else {
            return;
        };
        match self.recovery.child_mut(entry.recovery_key) {
            Ok(Some(old)) => {
                let _ = old.start_kill();
            }
            // The old Gateway exited while the update waited. A clean exit (code 0) is read as
            // "stopped on purpose" and recovery stands down; the update must not turn that into
            // a Shard that stays down, so the stopped slot is started again. Budgets are kept.
            Ok(None) => {
                let stopped = self
                    .recovery
                    .snapshot(entry.recovery_key)
                    .is_ok_and(|s| s.state == RecoveryState::Stopped);
                if stopped
                    && entry.started
                    && self.recovery.start_stopped(entry.recovery_key).is_ok()
                {
                    entry.snapshot.desired_running = true;
                }
            }
            Err(_) => {}
        }
    }

    /// Step 2: the standby holds the Cores. Hand the supervision of the Shard to it and ask
    /// the old Gateway to exit. The old process stays owned by botd until it has exited
    /// (killed after a grace period).
    #[cfg(unix)]
    pub async fn commit_gateway_update(&mut self, key: &ShardKey) -> Result<(), DaemonError> {
        let entry = self.entries.get_mut(key).ok_or(DaemonError::UnknownShard)?;
        let Some(candidate) = entry.candidate.take() else {
            return Err(DaemonError::InvalidState);
        };
        let expected = match self.recovery.snapshot(entry.recovery_key) {
            Ok(snapshot) => snapshot.generation,
            Err(_) => {
                let mut child = candidate.child;
                let _ = child.start_kill();
                return Err(DaemonError::InvalidState);
            }
        };
        let _ = std::fs::remove_file(&candidate.active_marker);
        match self.recovery.replace_ready(
            entry.recovery_key,
            expected,
            expected + 1,
            candidate.child,
        ) {
            Ok(mut old) => {
                #[cfg(target_os = "linux")]
                if let Some(service) = &mut self.local_identify.send_grants {
                    service.promote(key, &candidate.identity.generation);
                }
                if let Some(pipe) = old.stdin.as_mut() {
                    let _ = tokio::time::timeout(Duration::from_millis(500), async {
                        pipe.write_all(shard_runtime::GATEWAY_SHUTDOWN_FRAME)
                            .await?;
                        pipe.flush().await
                    })
                    .await;
                }
                entry.retiring = Some(Retiring {
                    child: old,
                    since: std::time::Instant::now(),
                });
                entry.snapshot.identity = Some(candidate.identity);
                entry.graceful_stop = None;
                entry.update_fallback_reserved = false;
                Ok(())
            }
            Err((_, mut child)) => {
                let _ = child.start_kill();
                Err(DaemonError::InvalidState)
            }
        }
    }

    /// For daemon shutdown: an update in flight is abandoned. A standby Gateway that has not
    /// been activated is killed, and a replaced Gateway that is still exiting is not left
    /// behind. The Gateway that is active is stopped by the normal graceful path.
    #[cfg(unix)]
    pub fn abandon_gateway_updates(&mut self) {
        #[cfg(target_os = "linux")]
        if let Some(service) = &mut self.local_identify.send_grants {
            for key in self.entries.keys() {
                if let Some(sequence) = service.cancel_launch(key, true) {
                    self.sequence = sequence;
                }
            }
        }
        for entry in self.entries.values_mut() {
            entry.update_fallback_reserved = false;
            if let Some(mut candidate) = entry.candidate.take() {
                let _ = candidate.child.start_kill();
                let _ = std::fs::remove_file(&candidate.ready_marker);
            }
            if let Some(retiring) = entry.retiring.as_mut() {
                let _ = retiring.child.start_kill();
            }
        }
    }

    /// Reaps replaced Gateways; kills one that ignored the stop request.
    fn rotate_gateway_logs(&mut self) {
        let due = self
            .log_checked
            .is_none_or(|at| at.elapsed() >= Duration::from_secs(60));
        if !due {
            return;
        }
        self.log_checked = Some(std::time::Instant::now());
        for entry in self.entries.values() {
            if let Some(dir) = entry.spec.gateway.management_path.parent() {
                // `gateway-candidate.log` is the pre-2026-11-16 name; a leftover file is still
                // kept within its size limit.
                for name in ["gateway.log", "gateway-candidate.log"] {
                    let _ = crate::core_units::rotate_in_place(&dir.join(name), 8 * 1024 * 1024);
                }
            }
        }
    }

    #[cfg(unix)]
    fn poll_retiring(&mut self) {
        for entry in self.entries.values_mut() {
            let Some(retiring) = entry.retiring.as_mut() else {
                continue;
            };
            match retiring.child.try_wait() {
                Ok(Some(_)) => entry.retiring = None,
                Ok(None) if retiring.since.elapsed() >= RETIRING_GRACE => {
                    let _ = retiring.child.start_kill();
                }
                _ => {}
            }
        }
    }

    /// Core status per Shard for operators (no paths or secrets).
    #[cfg(unix)]
    pub fn core_status(&self, key: &ShardKey) -> Option<Vec<crate::core_units::CoreStatus>> {
        self.entries.get(key)?.cores.as_ref().map(|c| c.status())
    }

    /// Stops every Shard Cores: close their stdin, wait until `deadline`, then kill the
    /// ones still alive. Run after the Gateways have been stopped.
    #[cfg(unix)]
    pub async fn stop_cores(&mut self, deadline: Duration) {
        for entry in self.entries.values_mut() {
            if let Some(cores) = entry.cores.as_mut() {
                cores.begin_stop();
            }
        }
        let until = std::time::Instant::now() + deadline;
        loop {
            let mut alive = false;
            for entry in self.entries.values_mut() {
                if let Some(cores) = entry.cores.as_mut() {
                    alive |= cores.any_alive();
                }
            }
            if !alive || std::time::Instant::now() >= until {
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        for entry in self.entries.values_mut() {
            if let Some(cores) = entry.cores.as_mut() {
                cores.kill();
            }
        }
        let reap_until = std::time::Instant::now() + deadline;
        loop {
            let mut unconfirmed = 0;
            for entry in self.entries.values_mut() {
                if let Some(cores) = entry.cores.as_mut() {
                    if cores.any_alive() {
                        unconfirmed += 1;
                    } else {
                        cores.cleanup();
                    }
                }
            }
            if unconfirmed == 0 {
                break;
            }
            if std::time::Instant::now() >= reap_until {
                eprintln!("Core termination unconfirmed for {unconfirmed} shard(s)");
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    }

    pub fn snapshots(&self) -> Vec<ProcessSnapshot> {
        self.entries
            .values()
            .map(|entry| {
                let snapshot = entry.snapshot.clone();
                #[cfg(target_os = "linux")]
                if let Some(service) = &self.local_identify.send_grants {
                    return ProcessSnapshot {
                        identify_send_grant: Some(service.status(&snapshot.key)),
                        ..snapshot
                    };
                }
                snapshot
            })
            .collect()
    }

    fn now(&mut self) -> Result<Duration, DaemonError> {
        let now = self.clock.now();
        if self.clock_failed || now < self.last_now {
            self.clock_failed = true;
            let mut events = Vec::new();
            for entry in self.entries.values_mut() {
                entry.snapshot.desired_running = false;
                entry.snapshot.retry_at_ms = None;
                if entry.started {
                    events.extend(
                        self.recovery
                            .fail_closed(entry.recovery_key, DegradedReason::ClockRegressed)
                            .unwrap_or_default(),
                    );
                    entry.snapshot.restart_exhausted = true;
                    entry.snapshot.recovery = Some(
                        serde_json::json!({"state":RecoveryState::Degraded(DegradedReason::ClockRegressed)}),
                    );
                }
            }
            self.record_recovery_events(events);
            return Err(DaemonError::ClockRegressed);
        }
        self.last_now = now;
        Ok(now)
    }

    pub fn start_all(&mut self, credentials: &impl CredentialSource) -> Result<(), DaemonError> {
        let keys = self
            .entries
            .iter()
            .filter(|(_, e)| !e.started)
            .map(|(k, _)| k.clone())
            .collect::<Vec<_>>();
        let mut error = None;
        for key in keys {
            if let Err(e) = self.start_inner(&key, credentials, StartKind::Automatic) {
                error.get_or_insert(e);
            }
        }
        error.map_or(Ok(()), Err)
    }

    /// Replace the local Identify admission limits (clears recorded history).
    /// Intended for tests; production uses the documented defaults. Set this
    /// before enabling persistent send grants, which keep their initial policy.
    pub fn set_local_identify_policy(
        &mut self,
        policy: crate::local_identify::LocalIdentifyPolicy,
    ) {
        self.local_identify.set_policy(policy);
    }

    pub fn take_recovery_events(&mut self) -> Vec<DaemonRecoveryEvent> {
        self.recovery_events.drain(..).collect()
    }

    pub fn start(
        &mut self,
        key: &ShardKey,
        credentials: &impl CredentialSource,
    ) -> Result<(), DaemonError> {
        loop {
            let result = self.start_inner(key, credentials, StartKind::Manual);
            #[cfg(target_os = "linux")]
            if result == Err(DaemonError::IdentifyJournalPending)
                && let Some(service) = &mut self.local_identify.send_grants
            {
                if !service.wait_for_write() {
                    // A different operation owns an already durable launch.
                    self.stop(key, StopMode::WaitForExit)?;
                    return Err(DaemonError::IdentifyDeferred);
                }
                continue;
            }
            return result;
        }
    }

    #[cfg(target_os = "linux")]
    fn cancel_journal_launch(&mut self, key: &ShardKey, standby: bool) {
        if let Some(service) = &mut self.local_identify.send_grants
            && let Some(sequence) = service.cancel_launch(key, standby)
        {
            // A submitted generation remains charged if the operation is cancelled.
            self.sequence = sequence;
        }
    }

    fn start_inner(
        &mut self,
        key: &ShardKey,
        credentials: &impl CredentialSource,
        kind: StartKind,
    ) -> Result<(), DaemonError> {
        self.now()?;
        if self.has_child(key) {
            return Err(DaemonError::InvalidState);
        }
        let entry = self.entries.get_mut(key).ok_or(DaemonError::UnknownShard)?;
        if !entry.journal_start_pending {
            if entry.snapshot.desired_running {
                return Err(DaemonError::InvalidState);
            }
            if entry.started {
                self.recovery
                    .start_stopped(entry.recovery_key)
                    .map_err(|_| DaemonError::InvalidState)?;
            } else {
                self.recovery
                    .register(entry.recovery_key, 0)
                    .map_err(|_| DaemonError::InvalidState)?;
            }
            entry.started = true;
            entry.snapshot.desired_running = true;
        }
        // The accepted management operation owns this one launch. Its hold is
        // restored before the next automatic poll; no budget is reset.
        self.recovery
            .hold(entry.recovery_key, false)
            .map_err(|_| DaemonError::InvalidState)?;
        let result = self.poll_recovery(credentials, Some(key), kind);
        let entry = self.entries.get_mut(key).unwrap();
        entry.journal_start_pending = result == Err(DaemonError::IdentifyJournalPending);
        let _ = self.recovery.hold(entry.recovery_key, entry.restart_held);
        if kind != StartKind::Automatic
            && matches!(
                result,
                Err(DaemonError::IdentifyDeferred | DaemonError::IdentifyUnavailable)
            )
        {
            self.stop(key, StopMode::WaitForExit)?;
        }
        result
    }

    fn launch(
        entry: &mut Entry,
        credentials: &impl CredentialSource,
        instance: [u8; 16],
        sequence: &mut u64,
        _local_identify: &mut crate::local_identify::LocalIdentifyRegistry,
    ) -> Result<Child, DaemonError> {
        tokio::runtime::Handle::try_current().map_err(|_| DaemonError::NoRuntime)?;
        let spec = &entry.spec.gateway;
        // Absolute executables prevent the parent's PATH (possibly untrusted)
        // participating in program lookup despite env_clear().
        if !safe_path(Path::new(&spec.program)) {
            return Err(DaemonError::InvalidProgram);
        }
        let [name] = spec.environment.credential_refs.as_slice() else {
            return Err(DaemonError::InvalidSpec);
        };
        if !crate::placement::valid_token_env(name) {
            return Err(DaemonError::InvalidSpec);
        }
        let credential = if entry.mock_discord.is_none() {
            Some(
                credentials
                    .read(name)
                    .filter(|value| !value.is_empty())
                    .ok_or(DaemonError::MissingCredential)?,
            )
        } else {
            None
        };
        // Windows' network stack needs SystemRoot even for a literal loopback
        // address. Inherit only this OS location, never PATH or arbitrary user
        // variables. The planner reserves its name against credential aliases,
        // and validate_specs forbids an application-supplied replacement.
        #[cfg(windows)]
        let system_root = std::env::var_os("SystemRoot")
            .filter(|value| !value.is_empty())
            .ok_or(DaemonError::MissingSystemEnvironment)?;
        *sequence = sequence
            .checked_add(1)
            .ok_or(DaemonError::GenerationExhausted)?;
        let generation = LaunchGeneration {
            daemon_instance: instance,
            sequence: *sequence,
        };
        let observed = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| DaemonError::TimeOverflow)?
            .as_millis();
        if entry.reservation.is_none() {
            let path = spec.management_path.with_extension("botd-reservation");
            std::fs::create_dir_all(path.parent().ok_or(DaemonError::InvalidSpec)?)
                .map_err(|_| DaemonError::ReservationIo)?;
            let open = || {
                std::fs::OpenOptions::new()
                    .write(true)
                    .create_new(true)
                    .open(&path)
            };
            let mut file = match open() {
                Ok(file) => file,
                Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
                    // A leftover (e.g. botd was killed). Remove it only when no
                    // Gateway of this shard is verifiably alive; otherwise the
                    // reservation keeps blocking a duplicate launch.
                    let liveness =
                        scan_gateway_liveness(Path::new(&spec.program), &spec.management_path);
                    if !reclaim_stale_reservation(&path, &entry.snapshot.key, liveness) {
                        return Err(DaemonError::ReservationExists);
                    }
                    open().map_err(|e| {
                        if e.kind() == std::io::ErrorKind::AlreadyExists {
                            DaemonError::ReservationExists
                        } else {
                            DaemonError::ReservationIo
                        }
                    })?
                }
                Err(_) => return Err(DaemonError::ReservationIo),
            };
            // No credentials, argv, environment values or paths are persisted.
            let record = serde_json::to_vec(&(entry.snapshot.key.clone(), &generation))
                .map_err(|_| DaemonError::ReservationIo)?;
            if file
                .write_all(&record)
                .and_then(|_| file.sync_all())
                .is_err()
            {
                // A partial reservation still blocks duplicate launches safely.
                return Err(DaemonError::ReservationIo);
            }
            entry.reservation = Some(path);
        }
        #[cfg(unix)]
        if let Some(cores) = entry.cores.as_mut() {
            // Cores first: the Gateway connects to them before it logs in to Discord.
            cores
                .ensure(Path::new(&spec.program))
                .map_err(|_| DaemonError::SpawnFailed)?;
        }
        let mut command = Command::new(&spec.program);
        #[cfg(unix)]
        if let Some(cores) = entry.cores.as_ref() {
            command.arg("--core-dir").arg(cores.dir());
            if entry.epoch > 0 {
                command.arg("--core-epoch").arg(entry.epoch.to_string());
            }
        }
        entry.add_gateway_args(&mut command);
        command
            .env_clear()
            .envs(&spec.environment.variables)
            .env("BOT_ID", &entry.spec.bot_id)
            .env("SHARD_ID", entry.spec.shard_id.to_string())
            .env("TOTAL_SHARDS", entry.spec.total_shards.to_string())
            .env("GENERATION_ID", sequence.to_string())
            .stdin(Stdio::piped())
            .kill_on_drop(false);
        // Failure reasons (close codes etc.) go to gateway.log beside the
        // shard's state; if it cannot be opened safely, fall back to null.
        let (out, err) = match spec
            .management_path
            .parent()
            .and_then(|dir| open_gateway_log(dir, GATEWAY_LOG_MAX_BYTES, *sequence))
            .and_then(|file| file.try_clone().ok().map(|clone| (file, clone)))
        {
            Some((a, b)) => (Stdio::from(a), Stdio::from(b)),
            None => (Stdio::null(), Stdio::null()),
        };
        command.stdout(out).stderr(err);
        #[cfg(windows)]
        command.env("SystemRoot", system_root);
        if let Some(address) = entry.mock_discord {
            command
                .arg("--mock-discord")
                .arg(address.to_string())
                .env_remove("DISCORD_TOKEN")
                .env_remove(crate::placement::GATEWAY_TOKEN_ENV);
        } else if let Some(credential) = credential {
            command.env(crate::placement::GATEWAY_TOKEN_ENV, credential);
        }
        // Terminal Ctrl-C must target the daemon's foreground group, not its
        // independently supervised Gateway units. This is not a service manager
        // or a guarantee against an OS/session/job object's teardown policy.
        #[cfg(unix)]
        command.process_group(0);
        #[cfg(windows)]
        command.creation_flags(0x0000_0200 | 0x0800_0000); // NEW_PROCESS_GROUP | NO_WINDOW
        #[cfg(target_os = "linux")]
        let grant_fd = match _local_identify
            .send_grants
            .as_mut()
            .map(|service| {
                service.attach(&mut command, &entry.snapshot.key, generation.clone(), false)
            })
            .transpose()
        {
            Ok(fd) => fd,
            Err(_) => {
                Self::release_reservation(entry)?;
                return Err(DaemonError::SpawnFailed);
            }
        };
        let child = match command.spawn() {
            Ok(child) => child,
            Err(_) => {
                Self::release_reservation(entry)?;
                return Err(DaemonError::SpawnFailed);
            }
        };
        #[cfg(target_os = "linux")]
        drop(grant_fd);
        let pid = child.id().expect("newly spawned child has a process id");
        entry.graceful_stop = None;
        entry.snapshot.identity = Some(ProcessIdentity {
            key: entry.snapshot.key.clone(),
            pid,
            generation,
            launch_observed_unix_ms: observed,
        });
        entry.snapshot.state = ProcessState::Running;
        entry.snapshot.retry_at_ms = None;
        Ok(child)
    }

    fn release_reservation(entry: &mut Entry) -> Result<(), DaemonError> {
        if let Some(path) = &entry.reservation {
            std::fs::remove_file(path).map_err(|_| DaemonError::ReservationIo)?;
            entry.reservation = None;
        }
        Ok(())
    }

    fn record_recovery_events(&mut self, events: Vec<RecoveryEvent>) {
        for event in events {
            let Some(entry) = self.entries.values().find(|e| e.recovery_key == event.key) else {
                continue;
            };
            if self.recovery_events.len() == 256 {
                self.recovery_events.pop_front();
                self.recovery_events_dropped = self.recovery_events_dropped.saturating_add(1);
            }
            self.recovery_events.push_back(DaemonRecoveryEvent {
                key: entry.snapshot.key.clone(),
                role: event.key.role,
                generation: event.generation,
                at_ms: event.at.as_millis(),
                kind: event.kind,
            });
        }
    }

    fn poll_recovery(
        &mut self,
        credentials: &impl CredentialSource,
        requested: Option<&ShardKey>,
        kind: StartKind,
    ) -> Result<(), DaemonError> {
        let mut first_error = self.now().err();
        #[cfg(unix)]
        self.poll_retiring();
        #[cfg(unix)]
        self.rotate_gateway_logs();
        #[cfg(unix)]
        for entry in self.entries.values_mut() {
            // Preserve the reservation if the OLD Gateway stops while a candidate is
            // warming. Once no candidate remains, an operator stop or a terminal recovery
            // failure must not leave the rest of the Bot waiting forever.
            if entry.candidate.is_none()
                && (!entry.snapshot.desired_running
                    || self
                        .recovery
                        .snapshot(entry.recovery_key)
                        .is_ok_and(|state| {
                            matches!(
                                state.state,
                                RecoveryState::Stopped | RecoveryState::Degraded(_)
                            )
                        }))
            {
                entry.update_fallback_reserved = false;
            }
            let program = PathBuf::from(&entry.spec.gateway.program);
            let wanted = entry.snapshot.desired_running;
            if let Some(cores) = entry.cores.as_mut() {
                cores.poll(&program, wanted);
            }
        }
        let mut host = ProcessRecoveryHost {
            entries: &mut self.entries,
            credentials,
            instance: self.instance,
            sequence: &mut self.sequence,
            manual: requested.filter(|_| kind != StartKind::Automatic),
            reserved_restart: requested.filter(|_| kind == StartKind::Restart),
            clock: &*self.clock,
            last_now: &mut self.last_now,
            clock_failed: &mut self.clock_failed,
            local_identify: &mut self.local_identify,
            errors: BTreeMap::new(),
        };
        let mut events = self.recovery.poll(&mut host);
        let errors = host.errors;
        if self.clock_failed && first_error.is_none() {
            first_error = self.now().err();
        }
        for (key, error) in errors {
            if requested.is_none_or(|requested| requested == &key) {
                first_error.get_or_insert(error);
            }
            if error == DaemonError::IdentifyJournalPending {
                continue;
            }
            let entry = self.entries.get_mut(&key).unwrap();
            entry.snapshot.state = ProcessState::Failed { error, code: None };
            if matches!(
                error,
                DaemonError::ReservationExists
                    | DaemonError::ReservationIo
                    | DaemonError::InvalidProgram
                    | DaemonError::InvalidSpec
                    | DaemonError::NoRuntime
                    | DaemonError::GenerationExhausted
                    | DaemonError::TimeOverflow
            ) {
                events.extend(
                    self.recovery
                        .fail_closed(entry.recovery_key, DegradedReason::LaunchRejected)
                        .unwrap_or_default(),
                );
            }
        }
        for event in &events {
            let entry = self
                .entries
                .values_mut()
                .find(|e| e.recovery_key == event.key)
                .unwrap();
            match event.kind {
                RecoveryEventKind::Exited { success, code } => {
                    entry.snapshot.state = if success || !entry.snapshot.desired_running {
                        ProcessState::Exited { code }
                    } else {
                        ProcessState::Failed {
                            error: DaemonError::ChildExited,
                            code,
                        }
                    };
                }
                RecoveryEventKind::Stopped => {
                    entry.snapshot.desired_running = false;
                }
                RecoveryEventKind::Degraded { reason } => {
                    entry.snapshot.desired_running = false;
                    match reason {
                        DegradedReason::TimeOverflow => {
                            entry.snapshot.state = ProcessState::Failed {
                                error: DaemonError::TimeOverflow,
                                code: None,
                            };
                            if requested.is_none_or(|key| key == &entry.snapshot.key) {
                                first_error = Some(DaemonError::TimeOverflow);
                            }
                        }
                        DegradedReason::MonitorFailed => {
                            entry.snapshot.state = ProcessState::Failed {
                                error: DaemonError::MonitorFailed,
                                code: None,
                            };
                            if requested.is_none_or(|key| key == &entry.snapshot.key) {
                                first_error.get_or_insert(DaemonError::MonitorFailed);
                            }
                        }
                        DegradedReason::IdentifyUnavailable => {
                            entry.snapshot.state = ProcessState::Failed {
                                error: DaemonError::IdentifyUnavailable,
                                code: None,
                            };
                            if requested == Some(&entry.snapshot.key) {
                                first_error.get_or_insert(DaemonError::IdentifyUnavailable);
                            }
                        }
                        _ => {}
                    }
                }
                _ => {}
            }
        }
        self.record_recovery_events(events);
        for entry in self.entries.values_mut().filter(|e| e.started) {
            let state = self
                .recovery
                .snapshot(entry.recovery_key)
                .map_err(|_| DaemonError::InvalidState)?;
            // Drain boundary: stop requested until the owned child was reaped.
            if state.pid.is_none()
                && let Some(since) = entry.stop_since.take()
                && let Some(feed) = &entry.feed
            {
                feed.observe(crate::metrics_feed::Observation::Drain(since.elapsed()));
            }
            self.metrics
                .set_process(&entry.snapshot.key, state.pid.is_some(), state.generation);
            entry.snapshot.restarts = state.restart.restarts;
            entry.snapshot.retry_at_ms = state.restart.retry_at.map(|at| at.as_millis());
            entry.snapshot.restart_exhausted = matches!(state.state, RecoveryState::Degraded(_));
            entry.snapshot.recovery = Some(serde_json::json!({
                "state":state.state,"generation":state.generation,"pid":state.pid,
                "restarts":state.restart.restarts,"startup_failures":state.startup_failures,"stable":state.stable,
                "deferred":state.deferred,
            }));
            if state.pid.is_none()
                && !entry.snapshot.desired_running
                && let Err(error) = Self::release_reservation(entry)
                && requested.is_none_or(|key| key == &entry.snapshot.key)
            {
                first_error.get_or_insert(error);
            }
        }
        first_error.map_or(Ok(()), Err)
    }

    /// Only ChildRecovery observes exits and schedules retries. OS liveness is
    /// distinct from authenticated Ready; this legacy wire cannot confirm Ready.
    pub fn poll(&mut self, credentials: &impl CredentialSource) -> Result<(), DaemonError> {
        let result = self.poll_recovery(credentials, None, StartKind::Automatic);
        #[cfg(target_os = "linux")]
        if self.local_identify.send_grants.is_some() && !self.clock_failed {
            let fallback = self
                .entries
                .values()
                .filter(|entry| entry.update_fallback_reserved)
                .map(|entry| entry.snapshot.key.bot_id.as_str())
                .collect();
            self.local_identify.poll_send_grants(&fallback);
        }
        result
    }

    /// Cancellation-safe: retain the owned Child until poll reaps its exit.
    /// Force requires the caller's explicit operator authorization. No kill or
    /// stop is issued on daemon Drop or when only a status client disconnects.
    pub fn stop(&mut self, key: &ShardKey, mode: StopMode) -> Result<(), DaemonError> {
        #[cfg(target_os = "linux")]
        self.cancel_journal_launch(key, false);
        let entry = self.entries.get_mut(key).ok_or(DaemonError::UnknownShard)?;
        entry.journal_start_pending = false;
        entry.snapshot.desired_running = false;
        entry.snapshot.retry_at_ms = None;
        entry.admission_since = None;
        if !entry.started {
            return Self::release_reservation(entry);
        }
        self.recovery
            .stop(entry.recovery_key)
            .map_err(|_| DaemonError::InvalidState)?;
        if let Some(child) = self
            .recovery
            .child_mut(entry.recovery_key)
            .map_err(|_| DaemonError::InvalidState)?
        {
            entry.stop_since.get_or_insert_with(std::time::Instant::now);
            if mode == StopMode::Force {
                child.start_kill().map_err(|_| DaemonError::StopFailed)?;
            }
        } else {
            Self::release_reservation(entry)?;
        }
        Ok(())
    }

    pub fn has_child(&self, key: &ShardKey) -> bool {
        self.entries.get(key).is_some_and(|entry| {
            self.recovery
                .snapshot(entry.recovery_key)
                .is_ok_and(|s| s.pid.is_some())
        })
    }

    fn hold_restart(&mut self, key: &ShardKey, held: bool) {
        if let Some(entry) = self.entries.get_mut(key) {
            entry.restart_held = held;
            if entry.started {
                let _ = self.recovery.hold(entry.recovery_key, held);
            }
        }
    }

    /// Request cooperative shutdown through the launch-scoped anonymous pipe.
    /// The child must drain, then exit with code zero. A successful write is NOT
    /// a successful stop: callers must poll/reap the child. EOF is not shutdown.
    /// No credentials, paths or process identifiers are sent on this channel.
    /// Cancellation or a failed/partial write never causes an automatic resend.
    pub async fn graceful_stop(&mut self, key: &ShardKey) -> Result<(), DaemonError> {
        self.stop(key, StopMode::WaitForExit)?;
        let entry = self.entries.get_mut(key).ok_or(DaemonError::UnknownShard)?;
        let Some(child) = self.recovery.child_mut(entry.recovery_key).ok().flatten() else {
            return Ok(());
        };
        if let Some(result) = entry.graceful_stop {
            return result;
        }
        entry.graceful_stop = Some(Err(DaemonError::StopFailed));
        let pipe = child.stdin.as_mut().ok_or(DaemonError::StopFailed)?;
        let result = tokio::time::timeout(Duration::from_millis(100), async {
            pipe.write_all(shard_runtime::GATEWAY_SHUTDOWN_FRAME)
                .await?;
            pipe.flush().await
        })
        .await;
        if matches!(result, Ok(Ok(()))) {
            entry.graceful_stop = Some(Ok(()));
            Ok(())
        } else {
            Err(DaemonError::StopFailed)
        }
    }
}

/// Opt-in, single-daemon process wiring for the rolling protocol. All Discord
/// traffic is the existing explicit loopback simulation; this is NOT a quorum
/// backend or permission to enable rolling on the production Gateway path.
pub mod test_gateway_switch;
