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
    writeln!(file, "--- gateway generation {generation} ---").ok()?;
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
    use super::*;
    use crate::{botref, placement::BotPlacement};
    use serde_json::{Value, json};
    use std::time::Instant;

    pub const NOTE: &str = "Local process status only; Discord readiness, Core drain, ownership and leases are not verified.";
    pub const OPERATION_LIMIT: usize = 1024;
    pub const OPERATION_SHARD_LIMIT: usize = 64;
    pub const OPERATION_TIMEOUT: Duration = Duration::from_secs(30);

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
    }

    impl CommandRequest {
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
            match self.command.as_str() {
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
                "operation" => self.bot.is_none() && id && !self.force && !self.yes,
                "start" | "restart" | "gateway-prepare" | "gateway-update" => {
                    selector && id && !self.force && !self.yes
                }
                // Missing confirmation is diagnosed by the authenticated handler.
                "stop" => selector && id && (!self.yes || self.force),
                _ => false,
            }
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
        pub bot_id: String,
        pub state: OperationState,
        pub elapsed_ms: u128,
        pub shards: Vec<OperationShard>,
        pub failed_shards: Vec<ShardKey>,
    }

    #[derive(PartialEq, Eq)]
    enum Phase {
        Starting,
        Stopping,
    }

    struct Operation {
        id: String,
        command: String,
        bot_id: String,
        force: bool,
        targets: Vec<ShardKey>,
        phase: Phase,
        next: usize,
        started: Instant,
        waiting: HashSet<ShardKey>,
        generations: BTreeMap<ShardKey, LaunchGeneration>,
        errors: BTreeMap<ShardKey, DaemonError>,
        finished: Option<OperationSnapshot>,
    }

    /// One owner serializes admissions; independent Bot operations advance on
    /// ticks while clients may read status. History never evicts accepted IDs:
    /// at capacity new operations fail before any side effect. It is not durable.
    pub struct ControlPlane {
        daemon: Daemon,
        config: Config,
        operations: BTreeMap<String, Operation>,
        monitor_ok: bool,
        last_advanced: String,
    }

    impl ControlPlane {
        pub fn recovery_events_dropped(&self) -> u64 {
            self.daemon.recovery_events_dropped
        }

        pub fn take_recovery_events(&mut self) -> Vec<DaemonRecoveryEvent> {
            self.daemon.take_recovery_events()
        }

        pub fn operation_exists(&self, id: &str) -> bool {
            self.operations.contains_key(id)
        }

        pub fn bot_busy(&self, bot: &str) -> bool {
            self.operations
                .values()
                .any(|op| op.bot_id == bot && op.finished.is_none())
        }

        pub fn request_as(
            &mut self,
            request: &CommandRequest,
            access: &super::update_control::ControlAccess,
        ) -> Value {
            let target = if request.command == "operation" {
                request
                    .operation_id
                    .as_ref()
                    .and_then(|id| self.operations.get(id))
                    .map(|op| op.bot_id.clone())
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
            let keys: Vec<_> = if request.command == "shards" {
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
            if !access.allows_command(read, target.as_deref(), &keys) {
                return Self::error("control permission or scope denied");
            }
            self.request(request)
        }

        pub fn new(daemon: Daemon, config: Config, monitor_ok: bool) -> Self {
            Self {
                daemon,
                config,
                operations: BTreeMap::new(),
                monitor_ok,
                last_advanced: String::new(),
            }
        }

        fn error(error: &str) -> Value {
            json!({"ok": false, "offline": false, "error": error, "note": NOTE})
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
                "status": state, "processes": processes})
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
            json!({"ok": snapshot.state != OperationState::Failed, "offline": false, "operation": snapshot, "note": NOTE})
        }

        fn finish(&mut self, op: &mut Operation) {
            let mut snapshot = self.snapshot(op);
            snapshot.state = if op.errors.is_empty() {
                OperationState::Succeeded
            } else {
                OperationState::Failed
            };
            op.finished = Some(snapshot);
            for key in &op.targets {
                self.daemon.hold_restart(key, false);
            }
        }

        /// No mutation is performed on a malformed request, unknown/ambiguous
        /// selector, conflicting ID, full journal or unconfirmed force request.
        pub fn request(&mut self, request: &CommandRequest) -> Value {
            if !request.valid() {
                return Self::error("invalid control request");
            }
            if matches!(
                request.command.as_str(),
                "gateway-prepare" | "gateway-update"
            ) {
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
            match request.command.as_str() {
                "ps" | "status" | "inspect" => {
                    let processes: Vec<_> = self
                        .daemon
                        .snapshots()
                        .into_iter()
                        .filter(|s| selected.as_ref().is_none_or(|b| b.bot_id == s.key.bot_id))
                        .collect();
                    let mut value = json!({"ok": true, "offline": false, "note": NOTE,
                        "status": {"processes": processes, "monitor_ok": self.monitor_ok}});
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
                _ => self.admit(request, selected.expect("validated selector").bot_id),
            }
        }

        fn admit(&mut self, request: &CommandRequest, bot_id: String) -> Value {
            if request.force && !request.yes {
                return Self::error("force stop requires --yes");
            }
            let id = request.operation_id.as_ref().expect("validated ID");
            if let Some(old) = self.operations.get(id) {
                if old.command != request.command
                    || old.bot_id != bot_id
                    || old.force != request.force
                {
                    return Self::error("operation ID conflicts with an earlier request");
                }
                return self.operation_view(old);
            }
            if self.operations.len() >= OPERATION_LIMIT {
                return Self::error("operation history is full");
            }
            let targets: Vec<_> = self
                .daemon
                .snapshots()
                .into_iter()
                .filter(|s| s.key.bot_id == bot_id)
                .map(|s| s.key)
                .collect();
            if targets.is_empty() {
                return Self::error("bot has no local shards");
            }
            if targets.len() > OPERATION_SHARD_LIMIT {
                return Self::error("operation exceeds local shard limit (64)");
            }
            let active: Vec<_> = self
                .operations
                .iter()
                .filter(|(_, op)| op.bot_id == bot_id && op.finished.is_none())
                .map(|(id, _)| id.clone())
                .collect();
            if !active.is_empty() && !request.force {
                return Self::error("bot already has an operation in progress");
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
            let waiting = if request.command == "start" {
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
            let op = Operation {
                id: id.clone(),
                command: request.command.clone(),
                bot_id,
                force: request.force,
                targets,
                phase: if request.command == "start" {
                    Phase::Starting
                } else {
                    Phase::Stopping
                },
                next: 0,
                started: Instant::now(),
                waiting,
                generations: BTreeMap::new(),
                errors: BTreeMap::new(),
                finished: None,
            };
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
            let keys: Vec<ShardKey> = self
                .daemon
                .snapshots()
                .into_iter()
                .map(|s| s.key)
                .collect();
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
                    return remaining;
                }
                tokio::time::sleep(Duration::from_millis(25)).await;
            }
        }

        /// Observe actual exits before advancing operations. One command per
        /// Bot per tick bounds cooperative writes and keeps reads responsive.
        pub async fn tick(&mut self, credentials: &impl CredentialSource) {
            self.monitor_ok = self.daemon.poll(credentials).is_ok();
            let mut ids: Vec<_> = self
                .operations
                .iter()
                .filter(|(_, op)| op.finished.is_none())
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
                self.advance(&mut op, credentials).await;
                self.last_advanced.clone_from(&id);
                self.operations.insert(id, op);
                if tick_started.elapsed() >= Duration::from_millis(10) {
                    break;
                }
            }
        }

        async fn advance(&mut self, op: &mut Operation, credentials: &impl CredentialSource) {
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
            if op.next < op.targets.len() {
                let key = &op.targets[op.next];
                op.next += 1;
                let result = if op.phase == Phase::Starting {
                    self.daemon.start(key, credentials)
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
                    Err(error) => {
                        op.errors.insert(key.clone(), error);
                        // A failed manual start must not secretly succeed later
                        // through an automatically scheduled restart.
                        if op.phase == Phase::Starting && error != DaemonError::InvalidState {
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
                if op.command == "restart" && op.errors.is_empty() {
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
            assert_eq!(text, "--- gateway generation 7 ---\n");
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
            assert_eq!(
                std::fs::read_to_string(dir.join("gateway.log")).unwrap(),
                "--- gateway generation 2 ---\n"
            );
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

        fn no_credentials(_: &str) -> Option<OsString> {
            panic!("read credentials without a launch");
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

/// Reads every `/proc/<pid>/cmdline`. None when the table cannot be trusted
/// (no /proc, unreadable entry other than one that vanished meanwhile).
#[cfg(target_os = "linux")]
fn read_proc_cmdlines() -> Option<Vec<Vec<u8>>> {
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
            Err(e)
                if e.kind() == std::io::ErrorKind::NotFound || e.raw_os_error() == Some(3) => {}
            Err(_) => return None,
        }
    }
    seen_pid.then_some(lines)
}

#[cfg(target_os = "linux")]
fn scan_gateway_liveness(program: &Path, management_path: &Path) -> GatewayLiveness {
    match read_proc_cmdlines() {
        Some(lines) if gateway_alive_in(&lines, program, management_path) => {
            GatewayLiveness::Alive
        }
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
        let dir = std::env::temp_dir().join(format!(
            "discord-rs-stale-{}-{name}",
            std::process::id()
        ));
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
        assert!(!reclaim_stale_reservation(&path, &key(), GatewayLiveness::Alive));
        assert!(!reclaim_stale_reservation(&path, &key(), GatewayLiveness::Unknown));
        assert!(path.exists());
        assert!(reclaim_stale_reservation(&path, &key(), GatewayLiveness::NotAlive));
        assert!(!path.exists());
        // Nothing to remove: not reported as removed.
        assert!(!reclaim_stale_reservation(&path, &key(), GatewayLiveness::NotAlive));
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
        assert!(!reclaim_stale_reservation(&link, &key(), GatewayLiveness::NotAlive));
        assert!(link.exists() && target.exists());
        let folder = dir.join("folder.botd-reservation");
        std::fs::create_dir(&folder).unwrap();
        assert!(!reclaim_stale_reservation(&folder, &key(), GatewayLiveness::NotAlive));
        let _ = std::fs::remove_dir_all(&dir);
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
    UnknownShard,
    InvalidState,
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
    /// Only explicit local test configuration may select this. Never infer the
    /// mode from missing credentials, inherited environment, or child failure.
    mock_discord: Option<SocketAddr>,
    /// Metrics feed (best effort, never blocks) and the boundary timers it uses.
    feed: Option<crate::metrics_feed::FeedSender>,
    admission_since: Option<std::time::Instant>,
    stop_since: Option<std::time::Instant>,
}

pub struct Daemon<C = MonotonicClock> {
    entries: BTreeMap<ShardKey, Entry>,
    recovery: ChildRecovery<RecoveryClock<C>>,
    clock: std::sync::Arc<C>,
    recovery_events: VecDeque<DaemonRecoveryEvent>,
    recovery_events_dropped: u64,
    require_shared_recovery_admission: bool,
    last_now: Duration,
    clock_failed: bool,
    instance: [u8; 16],
    sequence: u64,
    metrics: crate::daemon_metrics::DaemonMetrics,
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
    require_shared_admission: bool,
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
        // This legacy launcher has no Resume/Identify wire contract. botd gates
        // automatic real-Discord recovery until such an adapter is installed.
        // Explicit token-free simulation and trusted process-only callers keep
        // their existing lifecycle contract; neither is a Discord admission.
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
        if self.require_shared_admission
            && entry.mock_discord.is_none()
            && self.manual != Some(&entry.snapshot.key)
        {
            IdentifyAdmission::Unavailable
        } else {
            entry.admission_since = None;
            if let Some(feed) = &entry.feed {
                feed.observe(crate::metrics_feed::Observation::IdentifyWait(
                    since.elapsed(),
                ));
            }
            IdentifyAdmission::Granted
        }
    }
    fn cancel_identify(&mut self, _: ChildKey) {}
    fn spawn(&mut self, request: StartRequest) -> std::io::Result<Child> {
        let entry = self
            .entries
            .values_mut()
            .find(|e| e.recovery_key == request.key)
            .unwrap();
        match Daemon::<MonotonicClock>::launch(
            entry,
            self.credentials,
            self.instance,
            self.sequence,
        ) {
            Ok(child) => Ok(child),
            Err(error) => {
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
                        mock_discord: None,
                        feed,
                        admission_since: None,
                        stop_since: None,
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
            require_shared_recovery_admission: false,
            last_now,
            clock_failed: false,
            instance,
            sequence: 0,
            metrics,
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

    /// Explicit token-free Discord simulation. Gateways connect out to the
    /// caller's loopback collector; no listener or remote management is exposed.
    /// Configuration is frozen after the first launch attempt, including failed
    /// attempts. Normal launches never fall back to this mode.
    pub fn enable_mock_discord(&mut self, address: SocketAddr) -> Result<(), DaemonError> {
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

    pub fn snapshots(&self) -> Vec<ProcessSnapshot> {
        self.entries.values().map(|e| e.snapshot.clone()).collect()
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
            if let Err(e) = self.start(&key, credentials) {
                error.get_or_insert(e);
            }
        }
        error.map_or(Ok(()), Err)
    }

    /// The legacy artifact cannot coordinate a real Discord Identify at its
    /// send boundary. botd enables this guard; managed Gateway adapters instead
    /// use the shared Bot scheduler for initial, update and recovery admission.
    pub fn require_shared_recovery_admission(&mut self) {
        self.require_shared_recovery_admission = true;
    }

    pub fn take_recovery_events(&mut self) -> Vec<DaemonRecoveryEvent> {
        self.recovery_events.drain(..).collect()
    }

    pub fn start(
        &mut self,
        key: &ShardKey,
        credentials: &impl CredentialSource,
    ) -> Result<(), DaemonError> {
        self.now()?;
        if self.has_child(key) {
            return Err(DaemonError::InvalidState);
        }
        let entry = self.entries.get_mut(key).ok_or(DaemonError::UnknownShard)?;
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
        // The accepted management operation owns this one launch. Its hold is
        // restored before the next automatic poll; no budget is reset.
        self.recovery
            .hold(entry.recovery_key, false)
            .map_err(|_| DaemonError::InvalidState)?;
        let result = self.poll_recovery(credentials, Some(key));
        let entry = &self.entries[key];
        let _ = self.recovery.hold(entry.recovery_key, entry.restart_held);
        result
    }

    fn launch(
        entry: &mut Entry,
        credentials: &impl CredentialSource,
        instance: [u8; 16],
        sequence: &mut u64,
    ) -> Result<Child, DaemonError> {
        tokio::runtime::Handle::try_current().map_err(|_| DaemonError::NoRuntime)?;
        let spec = &entry.spec.gateway;
        // Absolute executables prevent the parent's PATH (possibly untrusted)
        // participating in program lookup despite env_clear().
        if !safe_path(Path::new(&spec.program)) {
            return Err(DaemonError::InvalidProgram);
        }
        let name = &spec.environment.credential_refs[0];
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
                    let liveness = scan_gateway_liveness(
                        Path::new(&spec.program),
                        &spec.management_path,
                    );
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
        let mut command = Command::new(&spec.program);
        command
            .args(&spec.args)
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
                .env_remove(name);
        } else if let Some(credential) = credential {
            command.env(name, credential);
        }
        // Terminal Ctrl-C must target the daemon's foreground group, not its
        // independently supervised Gateway units. This is not a service manager
        // or a guarantee against an OS/session/job object's teardown policy.
        #[cfg(unix)]
        command.process_group(0);
        #[cfg(windows)]
        command.creation_flags(0x0000_0200 | 0x0800_0000); // NEW_PROCESS_GROUP | NO_WINDOW
        let child = match command.spawn() {
            Ok(child) => child,
            Err(_) => {
                Self::release_reservation(entry)?;
                return Err(DaemonError::SpawnFailed);
            }
        };
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
        manual: Option<&ShardKey>,
    ) -> Result<(), DaemonError> {
        let mut first_error = self.now().err();
        let mut host = ProcessRecoveryHost {
            entries: &mut self.entries,
            credentials,
            instance: self.instance,
            sequence: &mut self.sequence,
            manual,
            require_shared_admission: self.require_shared_recovery_admission,
            errors: BTreeMap::new(),
        };
        let mut events = self.recovery.poll(&mut host);
        let errors = host.errors;
        for (key, error) in errors {
            if manual.is_none_or(|requested| requested == &key) {
                first_error.get_or_insert(error);
            }
            let entry = self.entries.get_mut(&key).unwrap();
            entry.snapshot.state = ProcessState::Failed { error, code: None };
            if matches!(
                error,
                DaemonError::ReservationExists
                    | DaemonError::ReservationIo
                    | DaemonError::InvalidProgram
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
                            if manual.is_none_or(|key| key == &entry.snapshot.key) {
                                first_error = Some(DaemonError::TimeOverflow);
                            }
                        }
                        DegradedReason::MonitorFailed => {
                            entry.snapshot.state = ProcessState::Failed {
                                error: DaemonError::MonitorFailed,
                                code: None,
                            };
                            if manual.is_none_or(|key| key == &entry.snapshot.key) {
                                first_error.get_or_insert(DaemonError::MonitorFailed);
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
            }));
            if state.pid.is_none()
                && !entry.snapshot.desired_running
                && let Err(error) = Self::release_reservation(entry)
                && manual.is_none_or(|key| key == &entry.snapshot.key)
            {
                first_error.get_or_insert(error);
            }
        }
        first_error.map_or(Ok(()), Err)
    }

    /// Only ChildRecovery observes exits and schedules retries. OS liveness is
    /// distinct from authenticated Ready; this legacy wire cannot confirm Ready.
    pub fn poll(&mut self, credentials: &impl CredentialSource) -> Result<(), DaemonError> {
        self.poll_recovery(credentials, None)
    }

    /// Cancellation-safe: retain the owned Child until poll reaps its exit.
    /// Force requires the caller's explicit operator authorization. No kill or
    /// stop is issued on daemon Drop or when only a status client disconnects.
    pub fn stop(&mut self, key: &ShardKey, mode: StopMode) -> Result<(), DaemonError> {
        let entry = self.entries.get_mut(key).ok_or(DaemonError::UnknownShard)?;
        entry.snapshot.desired_running = false;
        entry.snapshot.retry_at_ms = None;
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
