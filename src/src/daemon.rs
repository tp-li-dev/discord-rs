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
//! survive; stale reservations require external, authenticated reconciliation.
//! There is deliberately no automatic PID-based adoption or reservation removal.
//! The root and artifacts are trusted operator inputs. Filesystem aliases/ACLs,
//! power-loss durability and service-manager teardown are outside this skeleton.
//! Reservations cover one configured runtime namespace, not other roots/hosts.

use crate::{
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
                while plane
                    .daemon
                    .entries
                    .get_mut(&key)
                    .unwrap()
                    .child
                    .as_mut()
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
            entry.retry_at = Some(Duration::from_secs(300));
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
            assert_eq!(existing.retry_at, Some(Duration::from_secs(300)));
            assert!(!existing.restart_held);
            assert!(
                plane
                    .daemon
                    .entries
                    .values()
                    .all(|e| !e.started && e.child.is_none())
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
    child: Option<Child>,
    retry_at: Option<Duration>,
    recent: VecDeque<Duration>,
    failures: u32,
    reservation: Option<PathBuf>,
    started: bool,
    graceful_stop: Option<Result<(), DaemonError>>,
    restart_held: bool,
    /// Only explicit local test configuration may select this. Never infer the
    /// mode from missing credentials, inherited environment, or child failure.
    mock_discord: Option<SocketAddr>,
}

pub struct Daemon<C = MonotonicClock> {
    entries: BTreeMap<ShardKey, Entry>,
    policy: RestartPolicy,
    clock: C,
    last_now: Duration,
    clock_failed: bool,
    instance: [u8; 16],
    sequence: u64,
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
        let mut instance = [0; 16];
        getrandom::fill(&mut instance).map_err(|_| DaemonError::RandomUnavailable)?;
        let entries = specs
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
                };
                (
                    key,
                    Entry {
                        spec,
                        snapshot,
                        child: None,
                        retry_at: None,
                        recent: VecDeque::new(),
                        failures: 0,
                        reservation: None,
                        started: false,
                        graceful_stop: None,
                        restart_held: false,
                        mock_discord: None,
                    },
                )
            })
            .collect();
        let last_now = clock.now();
        Ok(Self {
            entries,
            policy,
            clock,
            last_now,
            clock_failed: false,
            instance,
            sequence: 0,
        })
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
            for entry in self.entries.values_mut() {
                entry.snapshot.desired_running = false;
                entry.retry_at = None;
                entry.snapshot.retry_at_ms = None;
            }
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

    pub fn start(
        &mut self,
        key: &ShardKey,
        credentials: &impl CredentialSource,
    ) -> Result<(), DaemonError> {
        let now = self.now()?;
        let entry = self.entries.get_mut(key).ok_or(DaemonError::UnknownShard)?;
        if entry.child.is_some() || entry.snapshot.desired_running {
            return Err(DaemonError::InvalidState);
        }
        entry.started = true;
        entry.snapshot.desired_running = true;
        entry.snapshot.restart_exhausted = false;
        match Self::launch(entry, credentials, self.instance, &mut self.sequence) {
            Ok(()) => Ok(()),
            Err(error) => {
                Self::failed(entry, error, None, self.policy, now)?;
                Err(error)
            }
        }
    }

    fn launch(
        entry: &mut Entry,
        credentials: &impl CredentialSource,
        instance: [u8; 16],
        sequence: &mut u64,
    ) -> Result<(), DaemonError> {
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
            let mut file = std::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&path)
                .map_err(|e| {
                    if e.kind() == std::io::ErrorKind::AlreadyExists {
                        DaemonError::ReservationExists
                    } else {
                        DaemonError::ReservationIo
                    }
                })?;
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
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .kill_on_drop(false);
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
        entry.child = Some(child);
        entry.graceful_stop = None;
        entry.snapshot.identity = Some(ProcessIdentity {
            key: entry.snapshot.key.clone(),
            pid,
            generation,
            launch_observed_unix_ms: observed,
        });
        entry.snapshot.state = ProcessState::Running;
        entry.retry_at = None;
        entry.snapshot.retry_at_ms = None;
        Ok(())
    }

    fn release_reservation(entry: &mut Entry) -> Result<(), DaemonError> {
        if let Some(path) = &entry.reservation {
            std::fs::remove_file(path).map_err(|_| DaemonError::ReservationIo)?;
            entry.reservation = None;
        }
        Ok(())
    }

    fn failed(
        entry: &mut Entry,
        error: DaemonError,
        code: Option<i32>,
        policy: RestartPolicy,
        now: Duration,
    ) -> Result<(), DaemonError> {
        entry.snapshot.state = ProcessState::Failed { error, code };
        entry.retry_at = None;
        entry.snapshot.retry_at_ms = None;
        if !entry.snapshot.desired_running {
            return Self::release_reservation(entry);
        }
        entry.failures = entry.failures.saturating_add(1);
        // Reservation conflict is never retried: another daemon/orphan may own it.
        if entry.snapshot.restarts >= policy.max_restarts
            || matches!(
                error,
                DaemonError::ReservationExists
                    | DaemonError::ReservationIo
                    | DaemonError::InvalidProgram
                    | DaemonError::NoRuntime
                    | DaemonError::GenerationExhausted
                    | DaemonError::TimeOverflow
            )
        {
            entry.snapshot.restart_exhausted = true;
            entry.snapshot.desired_running = false;
            return Self::release_reservation(entry);
        }
        let mut delay = policy.initial_backoff;
        for _ in 0..entry.snapshot.restarts {
            if delay >= policy.max_backoff {
                break;
            }
            delay = delay.saturating_mul(2).min(policy.max_backoff);
        }
        if entry.failures >= policy.circuit_breaker_after {
            delay = delay.max(policy.circuit_open_for);
        }
        let Some(retry_at) = now.checked_add(delay) else {
            entry.snapshot.state = ProcessState::Failed {
                error: DaemonError::TimeOverflow,
                code,
            };
            entry.snapshot.desired_running = false;
            entry.snapshot.restart_exhausted = true;
            Self::release_reservation(entry)?;
            return Err(DaemonError::TimeOverflow);
        };
        entry.retry_at = Some(retry_at);
        entry.snapshot.retry_at_ms = Some(retry_at.as_millis());
        Ok(())
    }

    /// Nonblocking child collection and bounded restart scheduling. A failed
    /// spawn never prevents monitoring other Bots. Credentials are read afresh.
    pub fn poll(&mut self, credentials: &impl CredentialSource) -> Result<(), DaemonError> {
        // Even a bad clock must not prevent collecting/observing existing children.
        let clock_result = self.now();
        let now = self.last_now;
        let mut first_error = clock_result.err();
        for entry in self.entries.values_mut() {
            if let Some(child) = &mut entry.child {
                match child.try_wait() {
                    Ok(Some(status)) => {
                        entry.child = None;
                        if status.success() || !entry.snapshot.desired_running {
                            entry.snapshot.state = ProcessState::Exited {
                                code: status.code(),
                            };
                            entry.snapshot.desired_running = false;
                            entry.retry_at = None;
                            entry.snapshot.retry_at_ms = None;
                            if let Err(e) = Self::release_reservation(entry) {
                                first_error.get_or_insert(e);
                            }
                        } else if let Err(e) = Self::failed(
                            entry,
                            DaemonError::ChildExited,
                            status.code(),
                            self.policy,
                            now,
                        ) {
                            first_error.get_or_insert(e);
                        }
                    }
                    Ok(None) => {}
                    Err(_) => {
                        // Retain the handle/reservation: liveness is unknown and
                        // starting another generation here would be unsafe.
                        entry.snapshot.state = ProcessState::Failed {
                            error: DaemonError::MonitorFailed,
                            code: None,
                        };
                        entry.snapshot.desired_running = false;
                        first_error.get_or_insert(DaemonError::MonitorFailed);
                    }
                }
            }
            if self.clock_failed
                || entry.restart_held
                || entry.child.is_some()
                || !entry.snapshot.desired_running
                || entry.retry_at.is_none_or(|at| now < at)
            {
                continue;
            }
            while entry
                .recent
                .front()
                .is_some_and(|at| now.saturating_sub(*at) >= self.policy.window)
            {
                entry.recent.pop_front();
            }
            if entry.recent.len() >= self.policy.max_restarts_in_window as usize {
                match entry.recent[0].checked_add(self.policy.window) {
                    Some(at) => {
                        entry.retry_at = Some(at);
                        entry.snapshot.retry_at_ms = Some(at.as_millis());
                    }
                    None => {
                        entry.snapshot.state = ProcessState::Failed {
                            error: DaemonError::TimeOverflow,
                            code: None,
                        };
                        entry.snapshot.desired_running = false;
                        entry.snapshot.restart_exhausted = true;
                        entry.retry_at = None;
                        entry.snapshot.retry_at_ms = None;
                        if let Err(e) = Self::release_reservation(entry) {
                            first_error.get_or_insert(e);
                        }
                        first_error.get_or_insert(DaemonError::TimeOverflow);
                    }
                }
                continue;
            }
            entry.snapshot.restarts += 1;
            entry.recent.push_back(now);
            if let Err(error) = Self::launch(entry, credentials, self.instance, &mut self.sequence)
            {
                first_error.get_or_insert(error);
                if let Err(e) = Self::failed(entry, error, None, self.policy, now) {
                    first_error.get_or_insert(e);
                }
            }
        }
        first_error.map_or(Ok(()), Err)
    }

    /// Cancellation-safe: retain the owned Child until poll reaps its exit.
    /// Force requires the caller's explicit operator authorization. No kill or
    /// stop is issued on daemon Drop or when only a status client disconnects.
    pub fn stop(&mut self, key: &ShardKey, mode: StopMode) -> Result<(), DaemonError> {
        let entry = self.entries.get_mut(key).ok_or(DaemonError::UnknownShard)?;
        entry.snapshot.desired_running = false;
        entry.retry_at = None;
        entry.snapshot.retry_at_ms = None;
        if let Some(child) = &mut entry.child {
            if mode == StopMode::Force {
                child.start_kill().map_err(|_| DaemonError::StopFailed)?;
            }
        } else {
            Self::release_reservation(entry)?;
        }
        Ok(())
    }

    /// A retained Child also covers an unknown liveness result from try_wait.
    /// Never authorize a replacement based on a stale snapshot or saved PID.
    pub fn has_child(&self, key: &ShardKey) -> bool {
        self.entries
            .get(key)
            .is_some_and(|entry| entry.child.is_some())
    }

    fn hold_restart(&mut self, key: &ShardKey, held: bool) {
        if let Some(entry) = self.entries.get_mut(key) {
            entry.restart_held = held;
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
        let Some(child) = entry.child.as_mut() else {
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
pub mod test_gateway_switch {
    use super::{control::CommandRequest, *};
    use crate::update_rollback::{Artifact, Component as UpdateComponent, UpdateControlRequest};
    use crate::{
        gateway_switch::{
            Authority, Effect, Event, Failure, GatewaySwitch, IdentifyBlock, IdentifyDecision,
            MockIdentifyLimit, MockIdentifyScheduler, Phase,
        },
        ownership::{OwnershipCoordinator, OwnershipKey, StandaloneCoordinator},
        protocol::{BotId, GenerationId, Identity, OwnershipEpoch, Shard},
        shard_runtime::test_switch::{self as wire, Action, CoreLaunch, State},
    };
    use serde::{Serialize, de::DeserializeOwned};
    use serde_json::{Value, json};
    use std::sync::{
        Arc, Mutex as StdMutex,
        atomic::{AtomicBool, Ordering},
    };
    use subtle::ConstantTimeEq;
    use tokio::{
        io::{AsyncBufRead, AsyncBufReadExt, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader},
        net::{TcpListener, TcpStream},
        sync::{Mutex, mpsc},
        task::JoinHandle,
    };

    const IO_TIMEOUT: Duration = Duration::from_secs(5);
    const LEASE: Duration = Duration::from_secs(120);
    type Result<T> = std::result::Result<T, &'static str>;

    async fn checked_artifact(
        check: JoinHandle<Result<PathBuf>>,
        deadline: Duration,
    ) -> Result<PathBuf> {
        // Timeout may leave read-only hashing on the blocking worker. It cannot
        // launch a child, and this actor resumes its lease/monitor loop promptly.
        tokio::time::timeout(deadline, check)
            .await
            .map_err(|_| "artifact verification timed out; candidate not launched")?
            .map_err(|_| "artifact verification task failed")?
    }

    async fn read_json<T: DeserializeOwned>(
        reader: &mut (impl AsyncBufRead + Unpin),
        limit: usize,
    ) -> Result<T> {
        let mut bytes = Vec::new();
        let count = reader
            .take(limit as u64 + 1)
            .read_until(b'\n', &mut bytes)
            .await
            .map_err(|_| "Gateway IPC read failed")?;
        if count == 0 || count > limit || bytes.last() != Some(&b'\n') {
            return Err("invalid Gateway IPC frame");
        }
        serde_json::from_slice(&bytes).map_err(|_| "invalid Gateway IPC message")
    }

    async fn write_json(
        writer: &mut (impl AsyncWrite + Unpin),
        value: &impl Serialize,
        limit: usize,
    ) -> Result<()> {
        let mut bytes = serde_json::to_vec(value).map_err(|_| "Gateway IPC encoding failed")?;
        bytes.push(b'\n');
        if bytes.len() > limit {
            return Err("Gateway IPC frame exceeds limit");
        }
        writer
            .write_all(&bytes)
            .await
            .map_err(|_| "Gateway IPC write failed")?;
        writer.flush().await.map_err(|_| "Gateway IPC flush failed")
    }

    struct Peer {
        stream: BufReader<TcpStream>,
        identity: Identity,
        serial: u64,
        view: Arc<StdMutex<Value>>,
    }

    impl Peer {
        async fn exchange(
            &mut self,
            action: Action,
            epoch: Option<OwnershipEpoch>,
        ) -> Result<State> {
            self.serial = self
                .serial
                .checked_add(1)
                .ok_or("Gateway request id exhausted")?;
            let mut identity = self.identity.clone();
            if let Some(epoch) = epoch {
                identity.ownership_epoch = epoch;
            }
            let request = wire::Control {
                request_id: self.serial,
                identity: identity.clone(),
                action,
            };
            let limit = if matches!(action, Action::Drain) {
                Duration::from_secs(65)
            } else {
                IO_TIMEOUT
            };
            let reply: wire::Reply = tokio::time::timeout(limit, async {
                write_json(self.stream.get_mut(), &request, wire::FRAME_LIMIT).await?;
                read_json(&mut self.stream, wire::FRAME_LIMIT).await
            })
            .await
            .map_err(|_| "Gateway control deadline exceeded")??;
            if reply.request_id != request.request_id || reply.identity != identity {
                return Err("Gateway control acknowledgement identity mismatch");
            }
            self.identity = identity;
            let mut view = self.view.lock().unwrap();
            view["state"] = json!(reply.state);
            view["epoch"] = json!(reply.identity.ownership_epoch.0);
            view["workers"] = json!(reply.workers);
            Ok(reply.state)
        }
    }

    struct Gateway {
        child: Child,
        peer: Arc<Mutex<Peer>>,
        gate: Arc<AtomicBool>,
        healthy: Arc<AtomicBool>,
        view: Arc<StdMutex<Value>>,
        heartbeat: JoinHandle<()>,
        relay: JoinHandle<Result<()>>,
    }

    impl Drop for Gateway {
        fn drop(&mut self) {
            self.gate.store(false, Ordering::SeqCst);
            self.heartbeat.abort();
            self.relay.abort();
        }
    }

    // Data gating complements (and does not replace) the gate inside the real
    // Gateway's Manager. A mock shadow event is explicitly suppressed, not queued
    // for later execution. Results from already accepted work flow in all states.
    async fn relay(
        mut upstream: TcpStream,
        downstream: TcpStream,
        gate: Arc<AtomicBool>,
    ) -> Result<()> {
        let (up_read, mut up_write) = upstream.split();
        let (down_read, mut down_write) = downstream.into_split();
        let mut up_read = BufReader::new(up_read);
        let mut down_read = BufReader::new(down_read);
        // read_until is cancellation-safe only when the accumulated buffer also
        // survives select! cancellation (e.g. a result arriving mid-input).
        let mut up_frame = Vec::new();
        let mut down_frame = Vec::new();
        let mut inputs_open = true;
        loop {
            tokio::select! {
                input = relay_frame(&mut up_read, &mut up_frame), if inputs_open => {
                    match input {
                        Ok(value) => {
                            let inspection = matches!(value["type"].as_str(), Some("status" | "crash"));
                            if inspection || gate.load(Ordering::SeqCst) {
                                write_json(&mut down_write, &value, 64 * 1024).await?;
                            } else {
                                write_json(&mut up_write, &json!({"type":"suppressed","id":value["id"]}), 64*1024).await?;
                            }
                        }
                        Err(_) => inputs_open = false,
                    }
                }
                result = relay_frame(&mut down_read, &mut down_frame) => {
                    let value = result?;
                    let stopped = value["type"] == "stopped";
                    write_json(&mut up_write, &value, 64 * 1024).await?;
                    if stopped { return Ok(()); }
                }
            }
        }
    }

    async fn relay_frame(
        reader: &mut (impl AsyncBufRead + Unpin),
        frame: &mut Vec<u8>,
    ) -> Result<Value> {
        const LIMIT: usize = 64 * 1024;
        reader
            .take((LIMIT + 1 - frame.len()) as u64)
            .read_until(b'\n', frame)
            .await
            .map_err(|_| "mock relay read failed")?;
        if frame.is_empty() || frame.len() > LIMIT || frame.last() != Some(&b'\n') {
            return Err("invalid mock relay frame");
        }
        let value = serde_json::from_slice(frame).map_err(|_| "invalid mock relay message")?;
        frame.clear();
        Ok(value)
    }

    fn generation_spec(original: &ShardRuntimeSpec, generation: u64) -> ShardRuntimeSpec {
        let mut spec = original.clone();
        spec.ipc_path = spec.ipc_path.join(format!("generation-{generation}"));
        for process in [&mut spec.gateway, &mut spec.core] {
            process.state_path = process.state_path.join(format!("generation-{generation}"));
            process.management_path = process
                .management_path
                .join(format!("generation-{generation}"));
            for pair in process.args.chunks_exact_mut(2) {
                match pair[0].to_str() {
                    Some("--ipc-path") => pair[1] = spec.ipc_path.as_os_str().to_owned(),
                    Some("--state-path") => pair[1] = process.state_path.as_os_str().to_owned(),
                    Some("--management-path") => {
                        pair[1] = process.management_path.as_os_str().to_owned()
                    }
                    _ => {}
                }
            }
        }
        spec
    }

    impl Gateway {
        async fn spawn(
            spec: &ShardRuntimeSpec,
            gateway_program: &Path,
            core_program: &Path,
            collector: SocketAddr,
            generation: u64,
        ) -> Result<Self> {
            let spec = generation_spec(spec, generation);
            let control = TcpListener::bind("127.0.0.1:0")
                .await
                .map_err(|_| "cannot bind Gateway control")?;
            let mock = TcpListener::bind("127.0.0.1:0")
                .await
                .map_err(|_| "cannot bind mock relay")?;
            let identity = Identity {
                bot_id: BotId(
                    spec.bot_id
                        .parse()
                        .map_err(|_| "invalid runtime Bot identity")?,
                ),
                shard: Shard::new(spec.shard_id, spec.total_shards)?,
                generation_id: GenerationId(generation),
                ownership_epoch: OwnershipEpoch(0),
            };
            let launch = wire::Launch {
                address: control
                    .local_addr()
                    .map_err(|_| "cannot inspect Gateway control")?,
                gateway: identity.clone(),
                core: CoreLaunch::from_spec(&spec, core_program, generation)
                    .map_err(|_| "invalid Core launch plan")?,
            };
            let mut secret = [0; 32];
            getrandom::fill(&mut secret).map_err(|_| "cannot create Gateway capability")?;
            let mut command = Command::new(gateway_program);
            command
                .args(&spec.gateway.args)
                .arg("--mock-discord")
                .arg(
                    mock.local_addr()
                        .map_err(|_| "cannot inspect mock relay")?
                        .to_string(),
                )
                .env_clear()
                .env("BOT_ID", &spec.bot_id)
                .env("SHARD_ID", spec.shard_id.to_string())
                .env("TOTAL_SHARDS", spec.total_shards.to_string())
                .env("GENERATION_ID", generation.to_string())
                .env(
                    wire::LAUNCH_ENV,
                    serde_json::to_string(&launch).map_err(|_| "cannot encode Gateway launch")?,
                )
                .stdin(Stdio::piped())
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .kill_on_drop(true);
            #[cfg(windows)]
            {
                command.env(
                    "SystemRoot",
                    std::env::var_os("SystemRoot")
                        .filter(|v| !v.is_empty())
                        .ok_or("missing system environment")?,
                );
                command.creation_flags(0x0000_0200 | 0x0800_0000);
            }
            #[cfg(unix)]
            command.process_group(0);
            let mut child = command.spawn().map_err(|_| "Gateway spawn failed")?;
            let pid = child.id().ok_or("Gateway process identity unavailable")?;
            let ready = tokio::time::timeout(Duration::from_secs(15), async {
                write_json(
                    child
                        .stdin
                        .as_mut()
                        .ok_or("Gateway bootstrap unavailable")?,
                    &wire::Bootstrap { secret },
                    wire::FRAME_LIMIT,
                )
                .await?;
                let (stream, _) = control
                    .accept()
                    .await
                    .map_err(|_| "Gateway control unavailable")?;
                let mut stream = BufReader::new(stream);
                let hello: wire::Hello = read_json(&mut stream, wire::FRAME_LIMIT).await?;
                if !bool::from(hello.secret.ct_eq(&secret))
                    || hello.identity != identity
                    || hello.pid != pid
                    || hello.workers.len() != 1
                    || !hello.workers[0].credential_free
                    || hello.workers[0].identity.as_ref() != Some(&launch.core.identity)
                {
                    return Err("Gateway handshake rejected");
                }
                let (downstream, _) = mock
                    .accept()
                    .await
                    .map_err(|_| "mock Gateway connection unavailable")?;
                let mut downstream = BufReader::new(downstream);
                let mut ready: Value = read_json(&mut downstream, 64 * 1024).await?;
                if ready["type"] != "ready"
                    || ready["pid"] != pid
                    || ready["bot_id"] != identity.bot_id.0
                    || ready["shard_id"] != spec.shard_id
                    || ready["total_shards"] != spec.total_shards
                    || ready["credential_present"] != false
                    || ready["workers"] != json!(hello.workers)
                {
                    return Err("mock Gateway readiness rejected");
                }
                ready["gateway_generation"] = json!(generation);
                ready["test_gateway_switch"] = json!(true);
                let mut upstream = TcpStream::connect(collector)
                    .await
                    .map_err(|_| "mock collector unavailable")?;
                write_json(&mut upstream, &ready, 64 * 1024).await?;
                let view = Arc::new(StdMutex::new(json!({"generation":generation,"pid":pid,
                    "state":"standby","epoch":0,"workers":hello.workers})));
                Ok((stream, downstream.into_inner(), upstream, view))
            })
            .await;
            let (stream, downstream, upstream, view) = match ready {
                Ok(Ok(ready)) => ready,
                other => {
                    // A missing ACK is never proof of exit. Reap the owned test
                    // child before reporting a failed candidate as absent.
                    let _ = child.kill().await;
                    let _ = child.wait().await;
                    return Err(match other {
                        Ok(Err(error)) => error,
                        _ => "Gateway readiness timed out",
                    });
                }
            };
            let peer = Arc::new(Mutex::new(Peer {
                stream,
                identity,
                serial: 0,
                view: view.clone(),
            }));
            let gate = Arc::new(AtomicBool::new(false));
            let healthy = Arc::new(AtomicBool::new(true));
            let relay = tokio::spawn(relay(upstream, downstream, gate.clone()));
            let heartbeat_peer = peer.clone();
            let heartbeat_gate = gate.clone();
            let heartbeat_healthy = healthy.clone();
            let heartbeat = tokio::spawn(async move {
                loop {
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    let mut peer = heartbeat_peer.lock().await;
                    if peer.exchange(Action::Status, None).await.is_err() {
                        heartbeat_gate.store(false, Ordering::SeqCst);
                        heartbeat_healthy.store(false, Ordering::SeqCst);
                        let _ = peer.stream.get_mut().shutdown().await;
                        break;
                    }
                }
            });
            Ok(Self {
                child,
                peer,
                gate,
                healthy,
                view,
                heartbeat,
                relay,
            })
        }

        async fn control(&mut self, action: Action, epoch: OwnershipEpoch) -> Result<()> {
            if !matches!(action, Action::Status | Action::Activate) {
                self.gate.store(false, Ordering::SeqCst);
            }
            let state = self.peer.lock().await.exchange(action, Some(epoch)).await?;
            let expected = match action {
                Action::Activate => State::Active,
                Action::Pause | Action::Quiesce => State::Paused,
                Action::Drain => State::Drained,
                Action::Status => state,
            };
            if state != expected {
                return Err("Gateway acknowledgement state mismatch");
            }
            if matches!(action, Action::Activate) {
                self.gate.store(true, Ordering::SeqCst);
            }
            Ok(())
        }

        async fn stop(&mut self) -> Result<()> {
            self.gate.store(false, Ordering::SeqCst);
            self.heartbeat.abort();
            let result = tokio::time::timeout(Duration::from_secs(15), async {
                let sent = self
                    .child
                    .stdin
                    .as_mut()
                    .ok_or("Gateway shutdown pipe unavailable")?
                    .write_all(shard_runtime::GATEWAY_SHUTDOWN_FRAME)
                    .await
                    .map_err(|_| "Gateway shutdown failed");
                let exit = self.child.wait().await.map_err(|_| "Gateway reap failed")?;
                if !exit.success() {
                    return Err("Gateway drain did not exit successfully");
                }
                sent
            })
            .await
            .map_err(|_| "Gateway stop timed out")?;
            // The collector's stopped frame is sent before closing the relay.
            let _ = tokio::time::timeout(IO_TIMEOUT, &mut self.relay).await;
            result
        }
    }

    #[derive(Default)]
    struct Shared {
        shards: BTreeMap<ShardKey, Value>,
        transitions: VecDeque<Value>,
        operations: BTreeMap<String, Value>,
        updates: BTreeMap<String, UpdateBatch>,
    }

    struct UpdateBatch {
        fingerprint: Vec<u8>,
        command: &'static str,
        bot_id: String,
        targets: Vec<(ShardKey, String, u64)>,
        started: std::time::Instant,
        finished: Option<Value>,
    }

    #[derive(Clone)]
    struct GatewayUpdate {
        current: u64,
        candidate: u64,
        artifact: Artifact,
    }

    struct Job {
        id: String,
        command: String,
        update: Option<GatewayUpdate>,
    }
    struct Actor {
        spec: ShardRuntimeSpec,
        key: ShardKey,
        ownership_key: OwnershipKey,
        gateway_program: PathBuf,
        core_program: PathBuf,
        collector: SocketAddr,
        budget: Arc<StdMutex<MockIdentifyScheduler>>,
        coordinator: Arc<StdMutex<StandaloneCoordinator>>,
        started: std::time::Instant,
        shared: Arc<StdMutex<Shared>>,
        machine: Option<GatewaySwitch>,
        gateways: BTreeMap<u64, Gateway>,
        generation: u64,
        candidate: Option<u64>,
        pending: Option<Job>,
        prepared: bool,
        stopped: bool,
        faulted: bool,
        deferral: Option<Value>,
        reservation: PathBuf,
    }

    impl Actor {
        fn publish(&self) {
            let phase = if self.stopped {
                "Stopped".to_owned()
            } else if self.faulted {
                "RecoveryRequired".to_owned()
            } else {
                self.machine
                    .as_ref()
                    .map_or_else(|| "Starting".to_owned(), |m| format!("{:?}", m.phase()))
            };
            let active = self
                .machine
                .as_ref()
                .and_then(|m| m.active())
                .map(|a| a.generation.0);
            let processes: Vec<_> = self
                .gateways
                .values()
                .map(|g| g.view.lock().unwrap().clone())
                .collect();
            self.shared.lock().unwrap().shards.insert(
                self.key.clone(),
                json!({
                    "bot_id":self.key.bot_id,"shard_id":self.key.shard_id,"phase":phase,
                    "active_generation":active,"generation_high_water":self.generation,
                    "processes":processes,"identify":self.deferral,
                }),
            );
        }

        fn record(&self, event: &str, generation: u64, epoch: u64) {
            let active_count = self
                .gateways
                .values()
                .filter(|g| g.view.lock().unwrap()["state"] == "active")
                .count();
            let pid = self
                .gateways
                .get(&generation)
                .map(|g| g.view.lock().unwrap()["pid"].clone());
            let mut shared = self.shared.lock().unwrap();
            if shared.transitions.len() == 512 {
                shared.transitions.pop_front();
            }
            shared.transitions.push_back(json!({"bot_id":self.key.bot_id,"shard_id":self.key.shard_id,
                "event":event,"generation":generation,"epoch":epoch,"pid":pid,"active_count":active_count}));
        }

        fn finish_job(&mut self, error: Option<&'static str>) {
            if let Some(job) = self.pending.take() {
                let error = if let Some(update) = &job.update {
                    if error.is_none()
                        && self
                            .machine
                            .as_ref()
                            .and_then(|m| m.active())
                            .is_some_and(|active| active.generation.0 == update.candidate)
                    {
                        self.gateway_program = update.artifact.path.clone();
                        None
                    } else {
                        error.or(Some("candidate did not become the final active generation"))
                    }
                } else {
                    error
                };
                let mut shared = self.shared.lock().unwrap();
                let Some(operation) = shared.operations.get_mut(&job.id) else {
                    return;
                };
                operation["state"] = json!(if error.is_some() {
                    "failed"
                } else {
                    "succeeded"
                });
                operation["error"] = json!(error);
            }
        }

        fn reserve(&mut self) -> Result<IdentifyDecision> {
            let decision = self.budget.lock().unwrap().reserve(
                self.ownership_key.bot_id.0,
                self.key.shard_id,
                self.started.elapsed(),
            )?;
            self.deferral = match decision {
                IdentifyDecision::Reserved => None,
                IdentifyDecision::Defer(d) => Some(json!({"reason": match d.reason {
                    IdentifyBlock::BudgetExhausted => "budget_exhausted", IdentifyBlock::RateLimited => "rate_limited",
                }, "retry_after_ms": d.retry_after.as_millis()})),
            };
            Ok(decision)
        }

        fn event(&mut self, event: Event) -> Result<Vec<Effect>> {
            self.machine
                .as_mut()
                .ok_or("Gateway not active")?
                .handle(self.ownership_key, event)
                .map_err(|_| "Gateway switch transition rejected")
        }

        async fn spawn(&mut self, generation: u64) -> Result<()> {
            // Hash away from the actor/runtime worker immediately before launch.
            // A failed or replaced artifact never reaches PauseOld/Activate.
            let program =
                if let Some(update) = self.pending.as_ref().and_then(|j| j.update.as_ref()) {
                    if update.candidate != generation {
                        return Err("candidate generation mismatch");
                    }
                    let artifact = update.artifact.clone();
                    checked_artifact(
                        tokio::task::spawn_blocking(move || {
                            crate::update_rollback::verify_artifact(&artifact, Path::new("."))
                        }),
                        IO_TIMEOUT,
                    )
                    .await?
                } else {
                    self.gateway_program.clone()
                };
            let gateway = Gateway::spawn(
                &self.spec,
                &program,
                &self.core_program,
                self.collector,
                generation,
            )
            .await?;
            self.gateways.insert(generation, gateway);
            self.record("spawned", generation, 0);
            self.publish();
            Ok(())
        }

        async fn initial(&mut self) -> Result<()> {
            if self.reserve()? != IdentifyDecision::Reserved {
                self.publish();
                return Ok(());
            }
            self.generation = self
                .generation
                .checked_add(1)
                .ok_or("Gateway generation exhausted")?;
            self.spawn(self.generation).await?;
            let lease = self
                .coordinator
                .lock()
                .unwrap()
                .acquire(self.ownership_key, LEASE)
                .map_err(|_| "local ownership unavailable")?;
            self.gateways
                .get_mut(&self.generation)
                .unwrap()
                .control(Action::Activate, lease.epoch)
                .await?;
            self.machine = Some(GatewaySwitch::new(
                self.ownership_key,
                Authority {
                    generation: GenerationId(self.generation),
                    epoch: lease.epoch,
                },
            ));
            self.record("active", self.generation, lease.epoch.0);
            self.publish();
            Ok(())
        }

        async fn effects(&mut self, effects: Vec<Effect>) -> Result<()> {
            let mut queue: VecDeque<_> = effects.into();
            while let Some(effect) = queue.pop_front() {
                let event = match effect {
                    Effect::SpawnStandby(g) => {
                        self.spawn(g.0).await?;
                        Some(Event::Spawned(g))
                    }
                    Effect::IdentifyStandby(g) => {
                        // Spawn consumed the reservation before the real process
                        // connected. Its authenticated Core Hello and the actual
                        // mock Ready were both observed; no real Discord READY.
                        self.record("warm_ready", g.0, 0);
                        if self
                            .pending
                            .as_ref()
                            .is_some_and(|j| j.command == "gateway-prepare")
                        {
                            self.prepared = true;
                            self.finish_job(None);
                            None
                        } else {
                            Some(Event::WarmReady(g))
                        }
                    }
                    Effect::PauseOld(a) => {
                        self.gateways
                            .get_mut(&a.generation.0)
                            .ok_or("old Gateway unavailable")?
                            .control(Action::Pause, a.epoch)
                            .await?;
                        self.record("paused", a.generation.0, a.epoch.0);
                        Some(Event::OldPaused(a))
                    }
                    Effect::RequestAuthority {
                        generation,
                        after_epoch,
                    } => {
                        let lease = self
                            .coordinator
                            .lock()
                            .unwrap()
                            .acquire(self.ownership_key, LEASE)
                            .map_err(|_| "local ownership unavailable")?;
                        if lease.epoch.0 <= after_epoch.0 {
                            return Err("non-increasing ownership epoch");
                        }
                        Some(Event::AuthorityGranted(Authority {
                            generation,
                            epoch: lease.epoch,
                        }))
                    }
                    Effect::Activate(a) => {
                        self.coordinator
                            .lock()
                            .unwrap()
                            .check(self.ownership_key, a.epoch)
                            .map_err(|_| "local authority expired")?;
                        self.gateways
                            .get_mut(&a.generation.0)
                            .ok_or("candidate Gateway unavailable")?
                            .control(Action::Activate, a.epoch)
                            .await?;
                        self.record("active", a.generation.0, a.epoch.0);
                        Some(Event::ActiveAck(a))
                    }
                    Effect::Drain(a) => {
                        if let Some(gateway) = self.gateways.get_mut(&a.generation.0) {
                            gateway.control(Action::Drain, a.epoch).await?;
                        }
                        self.record("drained", a.generation.0, a.epoch.0);
                        // The real Manager verifies all Core work/results. This
                        // adapter does not invent synthetic per-request ACKs.
                        Some(Event::DrainAck(a))
                    }
                    Effect::Stop(g) => {
                        if let Some(gateway) = self.gateways.get_mut(&g.0) {
                            gateway.stop().await?;
                        }
                        self.record("stopped", g.0, self.machine.as_ref().unwrap().epoch().0);
                        self.gateways.remove(&g.0);
                        self.candidate = None;
                        self.prepared = false;
                        None
                    }
                    Effect::Quiesce(a) => {
                        if let Some(gateway) = self.gateways.get_mut(&a.generation.0) {
                            gateway.control(Action::Quiesce, a.epoch).await?;
                        }
                        Some(Event::CandidateQuiesced(a))
                    }
                    Effect::Diagnostic(diagnostic) => {
                        self.record(
                            if matches!(
                                diagnostic,
                                crate::gateway_switch::Diagnostic::IdentifyDeferred(_)
                            ) {
                                "identify_deferred"
                            } else {
                                "update_aborted"
                            },
                            self.candidate.unwrap_or(0),
                            self.machine.as_ref().unwrap().epoch().0,
                        );
                        None
                    }
                    Effect::RecoverShard | Effect::RouteRestResult { .. } => {
                        return Err("Gateway recovery requires operator review");
                    }
                };
                if let Some(event) = event {
                    queue.extend(self.event(event)?);
                }
                self.publish();
            }
            if self
                .machine
                .as_ref()
                .is_some_and(|m| m.phase() == Phase::Stable)
            {
                self.finish_job(None);
            }
            Ok(())
        }

        async fn fail_closed(&mut self, error: &'static str) {
            // Failed preparation cannot have opened the new gate. A candidate
            // absent from this map has either never spawned or has been reaped.
            // Feed that barrier through the existing rollback protocol, keeping
            // the healthy old Gateway and its in-flight work intact.
            if let Some(candidate) = self.candidate
                && !self.gateways.contains_key(&candidate)
                && self
                    .machine
                    .as_ref()
                    .is_some_and(|m| matches!(m.phase(), Phase::Spawning | Phase::Warming))
            {
                self.finish_job(Some(error));
                if let Ok(effects) = self.event(Event::Failed {
                    generation: GenerationId(candidate),
                    reason: Failure::GatewayFailed,
                }) && self.effects(effects).await.is_ok()
                {
                    return;
                }
            }
            self.faulted = true;
            let epoch = self
                .machine
                .as_ref()
                .map_or(OwnershipEpoch(0), |m| m.epoch());
            if let Some(machine) = &mut self.machine {
                let _ = machine.handle(self.ownership_key, Event::AuthorityLost);
            }
            for gateway in self.gateways.values_mut() {
                gateway.gate.store(false, Ordering::SeqCst);
                let _ = gateway.control(Action::Quiesce, epoch).await;
            }
            self.record("recovery_required", self.generation, epoch.0);
            self.finish_job(Some(error));
            self.publish();
        }

        async fn job(&mut self, job: Job) -> Result<()> {
            if self.pending.is_some() && job.command != "stop" {
                let mut shared = self.shared.lock().unwrap();
                let op = shared.operations.get_mut(&job.id).unwrap();
                op["state"] = json!("failed");
                op["error"] = json!("Shard operation already running");
                return Ok(());
            }
            if let Some(update) = &job.update {
                let current = self
                    .machine
                    .as_ref()
                    .and_then(|m| m.active())
                    .map(|a| a.generation.0);
                if self.prepared
                    || self.stopped
                    || self.faulted
                    || self
                        .machine
                        .as_ref()
                        .is_none_or(|m| m.phase() != Phase::Stable)
                    || current != Some(update.current)
                    || self.generation.checked_add(1) != Some(update.candidate)
                {
                    let mut shared = self.shared.lock().unwrap();
                    let op = shared.operations.get_mut(&job.id).expect("admitted job");
                    op["state"] = json!("failed");
                    op["error"] = json!("Shard changed after update admission");
                    return Ok(());
                }
            }
            if job.command == "stop" {
                self.finish_job(Some("operation cancelled by stop"));
            }
            self.pending = Some(job);
            if self.pending.as_ref().unwrap().command == "stop" {
                let epoch = self
                    .machine
                    .as_ref()
                    .map_or(OwnershipEpoch(0), |m| m.epoch());
                let mut error = None;
                let mut reaped = Vec::new();
                for (generation, gateway) in &mut self.gateways {
                    gateway.gate.store(false, Ordering::SeqCst);
                    match gateway.child.try_wait() {
                        Ok(Some(exit)) => {
                            if !exit.success() {
                                error.get_or_insert("Gateway exited abnormally before stop");
                            }
                            reaped.push(*generation);
                            continue;
                        }
                        Err(_) => {
                            error.get_or_insert("Gateway monitor failed during stop");
                        }
                        Ok(None) => {}
                    }
                    // A broken control channel is not a reason to skip the
                    // existing owned Shutdown pipe, or the other generation.
                    if let Err(failure) = gateway.control(Action::Quiesce, epoch).await {
                        error.get_or_insert(failure);
                    } else if let Err(failure) = gateway.control(Action::Drain, epoch).await {
                        error.get_or_insert(failure);
                    }
                    if let Err(failure) = gateway.stop().await {
                        error.get_or_insert(failure);
                    }
                    if matches!(gateway.child.try_wait(), Ok(Some(_))) {
                        reaped.push(*generation);
                    }
                }
                for generation in reaped {
                    self.record("stopped", generation, epoch.0);
                    self.gateways.remove(&generation);
                }
                self.machine = None;
                self.stopped = self.gateways.is_empty();
                self.faulted = !self.stopped;
                if self.stopped && std::fs::remove_file(&self.reservation).is_err() {
                    error.get_or_insert("cannot release runtime reservation");
                }
                self.finish_job(error);
                self.publish();
                return Ok(());
            }
            if self.stopped || self.faulted || self.machine.is_none() {
                self.finish_job(Some("Shard is not ready"));
                return Ok(());
            }
            let effects =
                if self.prepared && self.pending.as_ref().unwrap().command == "gateway-update" {
                    self.prepared = false;
                    self.event(Event::WarmReady(GenerationId(
                        self.candidate.ok_or("candidate missing")?,
                    )))?
                } else {
                    if self.machine.as_ref().unwrap().phase() != Phase::Stable {
                        self.finish_job(Some("Shard switch already running"));
                        return Ok(());
                    }
                    self.generation = self
                        .generation
                        .checked_add(1)
                        .ok_or("Gateway generation exhausted")?;
                    self.candidate = Some(self.generation);
                    let identify = self.reserve()?;
                    self.event(Event::Begin {
                        candidate: GenerationId(self.generation),
                        identify,
                    })?
                };
            self.effects(effects).await
        }

        async fn tick(&mut self) -> Result<()> {
            if self.stopped || self.faulted {
                return Ok(());
            }
            if self.machine.is_none() {
                return self.initial().await;
            }
            if self.machine.as_ref().unwrap().phase() == Phase::RecoveryRequired {
                return Ok(());
            }
            let mut exited = None;
            for (generation, gateway) in &mut self.gateways {
                if gateway
                    .child
                    .try_wait()
                    .map_err(|_| "Gateway monitor failed")?
                    .is_some()
                {
                    exited = Some(*generation);
                    break;
                }
                if !gateway.healthy.load(Ordering::SeqCst) {
                    if self.candidate == Some(*generation)
                        && matches!(
                            self.machine.as_ref().unwrap().phase(),
                            Phase::Spawning | Phase::Warming
                        )
                    {
                        // EOF may precede OS exit observation. This generation
                        // has never received Activate. Reap it through the owned
                        // shutdown channel before entering the rollback barrier.
                        let _ = gateway.stop().await;
                        if matches!(gateway.child.try_wait(), Ok(Some(_))) {
                            exited = Some(*generation);
                            break;
                        }
                    }
                    return Err("Gateway or its control connection exited");
                }
            }
            if let Some(generation) = exited {
                self.record(
                    "exited",
                    generation,
                    self.machine.as_ref().unwrap().epoch().0,
                );
                self.gateways.remove(&generation);
                return Err("Gateway process exited");
            }
            if let Some(active) = self.machine.as_ref().unwrap().active() {
                self.coordinator
                    .lock()
                    .unwrap()
                    .renew(self.ownership_key, active.epoch, LEASE)
                    .map_err(|_| "local lease expired")?;
            }
            if self.machine.as_ref().unwrap().phase() == Phase::Deferred {
                let decision = self.reserve()?;
                if decision == IdentifyDecision::Reserved {
                    let effects = self.event(Event::RetryIdentify(decision))?;
                    self.effects(effects).await?;
                }
            }
            self.publish();
            Ok(())
        }

        async fn run(mut self, mut jobs: mpsc::Receiver<Job>) {
            self.publish();
            let mut interval = tokio::time::interval(Duration::from_millis(50));
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                let result = tokio::select! {
                    job = jobs.recv() => match job { Some(job) => self.job(job).await, None => break },
                    _ = interval.tick() => self.tick().await,
                };
                if let Err(error) = result {
                    self.fail_closed(error).await;
                }
            }
        }
    }

    pub struct Runtime {
        shared: Arc<StdMutex<Shared>>,
        actors: BTreeMap<ShardKey, mpsc::Sender<Job>>,
        tasks: Vec<JoinHandle<()>>,
        config: Config,
    }

    impl Drop for Runtime {
        fn drop(&mut self) {
            for task in &self.tasks {
                task.abort();
            }
        }
    }

    impl Runtime {
        /// Caller must also require an explicit CLI opt-in. Reject distributed
        /// config here as well: standalone is never a quorum fallback.
        pub fn new(
            config: &Config,
            root: &Path,
            gateway_program: &Path,
            core_program: &Path,
            collector: SocketAddr,
            limits: Vec<MockIdentifyLimit>,
        ) -> Result<Self> {
            let _ = Daemon::from_config(config, &HashMap::new(), root, RestartPolicy::default())
                .map_err(|_| "unsupported rolling configuration")?;
            if !safe_path(gateway_program)
                || !safe_path(core_program)
                || !collector.ip().is_loopback()
                || collector.port() == 0
            {
                return Err("invalid test rolling launch configuration");
            }
            let intents = config
                .local_primary_intents(&HashMap::new())
                .map_err(|_| "invalid local placement")?;
            let specs = shard_runtime::plan(&intents, root).map_err(|_| "invalid runtime plan")?;
            let budget = MockIdentifyScheduler::new(limits)?;
            for spec in &specs {
                let bot: u64 = spec
                    .bot_id
                    .parse()
                    .map_err(|_| "numeric Bot identity required")?;
                if bot.to_string() != spec.bot_id || !budget.contains_bot(bot) {
                    return Err("invalid runtime identity or missing Identify budget");
                }
            }
            let shared = Arc::new(StdMutex::new(Shared::default()));
            let budget = Arc::new(StdMutex::new(budget));
            let coordinator = Arc::new(StdMutex::new(StandaloneCoordinator::default()));
            let mut runtime = Self {
                shared: shared.clone(),
                actors: BTreeMap::new(),
                tasks: Vec::new(),
                config: config.clone(),
            };
            let started = std::time::Instant::now();
            for spec in specs {
                let key = ShardKey {
                    bot_id: spec.bot_id.clone(),
                    shard_id: spec.shard_id,
                };
                let ownership_key = OwnershipKey {
                    bot_id: BotId(spec.bot_id.parse().map_err(|_| "invalid Bot identity")?),
                    shard_id: spec.shard_id,
                };
                let reservation = spec
                    .gateway
                    .management_path
                    .with_extension("botd-reservation");
                std::fs::create_dir_all(reservation.parent().ok_or("invalid runtime path")?)
                    .map_err(|_| "cannot create runtime directory")?;
                let mut file = std::fs::OpenOptions::new()
                    .write(true)
                    .create_new(true)
                    .open(&reservation)
                    .map_err(|_| "runtime is already reserved or unavailable")?;
                file.write_all(
                    b"test-gateway-switch; explicit reconciliation required after crash\n",
                )
                .map_err(|_| "cannot reserve runtime")?;
                file.sync_all()
                    .map_err(|_| "cannot persist runtime reservation")?;
                let (sender, receiver) = mpsc::channel(4);
                let actor = Actor {
                    spec,
                    key: key.clone(),
                    ownership_key,
                    gateway_program: gateway_program.to_owned(),
                    core_program: core_program.to_owned(),
                    collector,
                    budget: budget.clone(),
                    coordinator: coordinator.clone(),
                    started,
                    shared: shared.clone(),
                    machine: None,
                    gateways: BTreeMap::new(),
                    generation: 0,
                    candidate: None,
                    pending: None,
                    prepared: false,
                    stopped: false,
                    faulted: false,
                    deferral: None,
                    reservation,
                };
                runtime.actors.insert(key, sender);
                runtime.tasks.push(tokio::spawn(actor.run(receiver)));
            }
            Ok(runtime)
        }

        /// Resolve against trusted daemon configuration for botctl's preflight.
        pub fn inspect_bot(&self, selector: &str) -> Value {
            let Ok(bot) = crate::botref::resolve(&self.config.bots, selector) else {
                return json!({"ok":false,"error":"unknown or ambiguous Bot"});
            };
            json!({"ok":true,"offline":false,"test_gateway_switch":true,"production_ready":false,
                "bot":{"bot_id":bot.bot_id,"name":bot.name,"total_shards":bot.total_shards}})
        }

        fn batch_view(shared: &mut Shared, id: &str) -> Value {
            let Some(batch) = shared.updates.get(id) else {
                return json!({"ok":false,"error":"unknown update operation"});
            };
            if let Some(finished) = &batch.finished {
                return finished.clone();
            }
            let mut shards = Vec::new();
            let mut failed = Vec::new();
            let mut generations = Vec::new();
            let mut running = false;
            for (key, child_id, expected) in &batch.targets {
                let result = &shared.operations[child_id];
                let observed = shared.shards.get(key);
                let generation = observed.and_then(|s| s["active_generation"].as_u64());
                let child_running = result["state"] == "running";
                running |= child_running;
                if result["state"] == "failed" || (!child_running && generation != Some(*expected))
                {
                    failed.push(key.clone());
                }
                shards.push(
                    json!({"key":key,"state":result["state"],"error":result["error"],
                    "gateway_generation":generation}),
                );
                if let Some(generation) = generation {
                    generations.push(json!({"shard_id":key.shard_id,"gateway":generation}));
                }
            }
            let state = if running {
                "running"
            } else if failed.is_empty() {
                "succeeded"
            } else {
                "failed"
            };
            let value = json!({"ok":state != "failed","offline":false,"test_gateway_switch":true,
                "production_ready":false,"operation":{"operation_id":id,"command":batch.command,
                "bot_id":batch.bot_id,"state":state,"elapsed_ms":batch.started.elapsed().as_millis(),
                "shards":shards,"failed_shards":failed,"final_generations":generations}});
            if !running {
                shared.updates.get_mut(id).expect("known batch").finished = Some(value.clone());
            }
            value
        }

        /// Inspect an admitted batch without replaying any mutation. Terminal
        /// observations are frozen, including generations and partial failures.
        pub fn update_status(&self, operation_id: &str) -> Value {
            Self::batch_view(&mut self.shared.lock().unwrap(), operation_id)
        }

        pub fn legacy_operation_exists(&self, operation_id: &str) -> bool {
            self.shared
                .lock()
                .unwrap()
                .operations
                .contains_key(operation_id)
        }

        /// Test-only Gateway execution adapter. Production/Core activation does
        /// not have the required control contract and must never be synthesized
        /// by restarting a Gateway. The caller owns authentication and auditing.
        /// Artifact preflight belongs on the caller's blocking worker; each actor
        /// independently rehashes again immediately before launching a candidate.
        pub fn request_update(&self, request: &UpdateControlRequest) -> Value {
            if let Err(error) = request.validate() {
                return json!({"ok":false,"error":error});
            }
            if request.component != UpdateComponent::Gateway {
                return json!({"ok":false,"error":"Core update execution is not connected"});
            }
            let Some(bot) = self.config.bots.iter().find(|b| b.bot_id == request.bot) else {
                return json!({"ok":false,"error":"unknown canonical Bot"});
            };
            if bot.name != request.manifest.bot_name
                || bot.total_shards
                    != crate::placement::ShardCount::Fixed(request.manifest.total_shards)
            {
                return json!({"ok":false,"error":"manifest does not match daemon Bot configuration"});
            }
            let artifact = request
                .manifest
                .artifacts
                .gateway
                .as_ref()
                .expect("validated artifact");
            if !artifact.path.is_absolute() || !safe_path(&artifact.path) {
                return json!({"ok":false,"error":"candidate requires an absolute executable path"});
            }
            let fingerprint = match serde_json::to_vec(request) {
                Ok(value) => value,
                Err(_) => return json!({"ok":false,"error":"invalid update payload"}),
            };
            let mut shared = self.shared.lock().unwrap();
            if let Some(old) = shared.updates.get(&request.operation_id) {
                if old.fingerprint != fingerprint {
                    return json!({"ok":false,"error":"operation ID conflicts with an earlier request"});
                }
                return Self::batch_view(&mut shared, &request.operation_id);
            }
            if shared.operations.contains_key(&request.operation_id) {
                return json!({"ok":false,"error":"operation ID conflicts with an earlier request"});
            }
            if shared.operations.len() + request.manifest.shards.len() > 256
                || shared.updates.len() >= 256
            {
                return json!({"ok":false,"error":"operation history full"});
            }
            // Acquire every channel permit before inserting a job or changing a
            // generation. A full/disconnected actor rejects the entire batch.
            let mut permits = Vec::new();
            for shard in &request.manifest.shards {
                let key = ShardKey {
                    bot_id: request.bot.clone(),
                    shard_id: shard.shard_id,
                };
                let Some(sender) = self.actors.get(&key) else {
                    return json!({"ok":false,"error":"requested Shard is not local"});
                };
                let Some(view) = shared.shards.get(&key) else {
                    return json!({"ok":false,"error":"requested Shard has no verified runtime"});
                };
                let generations = shard.gateway.expect("validated Gateway generation");
                let selector = format!("{}/{}", key.bot_id, key.shard_id);
                if view["phase"] != "Stable"
                    || view["active_generation"].as_u64() != Some(generations.current)
                    || view["generation_high_water"]
                        .as_u64()
                        .and_then(|g| g.checked_add(1))
                        != Some(generations.candidate)
                    || shared
                        .operations
                        .values()
                        .any(|op| op["state"] == "running" && op["bot"] == selector)
                    || shared.updates.values().any(|batch| {
                        batch.finished.is_none()
                            && batch.targets.iter().any(|(target, _, _)| target == &key)
                    })
                {
                    return json!({"ok":false,"error":"Shard is busy or current/candidate generation is stale"});
                }
                let Ok(permit) = sender.try_reserve() else {
                    return json!({"ok":false,"error":"Shard control busy"});
                };
                permits.push((key, generations, permit));
            }
            let mut targets = Vec::new();
            for (key, generations, permit) in permits {
                let id = format!("{}-shard-{}", request.operation_id, key.shard_id);
                shared.operations.insert(
                    id.clone(),
                    json!({"operation_id":id,"command":"gateway-update",
                    "bot":format!("{}/{}",key.bot_id,key.shard_id),"state":"running","error":null}),
                );
                targets.push((key, id.clone(), generations.candidate));
                permit.send(Job {
                    id,
                    command: "gateway-update".into(),
                    update: Some(GatewayUpdate {
                        current: generations.current,
                        candidate: generations.candidate,
                        artifact: artifact.clone(),
                    }),
                });
            }
            shared.updates.insert(
                request.operation_id.clone(),
                UpdateBatch {
                    fingerprint,
                    command: request.command_name(),
                    bot_id: request.bot.clone(),
                    targets,
                    started: std::time::Instant::now(),
                    finished: None,
                },
            );
            Self::batch_view(&mut shared, &request.operation_id)
        }

        pub fn request(&self, request: &CommandRequest) -> Value {
            let mut shared = self.shared.lock().unwrap();
            if matches!(
                request.command.as_str(),
                "ps" | "status" | "inspect" | "shards"
            ) {
                return json!({"ok":true,"test_gateway_switch":true,"production_ready":false,
                    "shards":shared.shards.values().collect::<Vec<_>>(),"transitions":shared.transitions,
                    "note":"Single-daemon mock Discord process wiring only; no distributed or production guarantee."});
            }
            if request.command == "operation" {
                return match request
                    .operation_id
                    .as_ref()
                    .and_then(|id| shared.operations.get(id))
                {
                    Some(operation) => json!({"ok":true,"operation":operation}),
                    None => json!({"ok":false,"error":"unknown operation"}),
                };
            }
            if !matches!(
                request.command.as_str(),
                "gateway-prepare" | "gateway-update" | "stop"
            ) || request.force
                || request.yes
            {
                return json!({"ok":false,"error":"operation not supported in mock rolling mode"});
            }
            let Some((bot, shard)) = request.bot.as_deref().and_then(|v| v.split_once('/')) else {
                return json!({"ok":false,"error":"test rolling selector must be bot_id/shard_id"});
            };
            let Ok(shard_id) = shard.parse::<u32>() else {
                return json!({"ok":false,"error":"invalid Shard selector"});
            };
            let key = ShardKey {
                bot_id: bot.to_owned(),
                shard_id,
            };
            let Some(sender) = self.actors.get(&key) else {
                return json!({"ok":false,"error":"unknown Shard"});
            };
            let Some(id) = &request.operation_id else {
                return json!({"ok":false,"error":"operation id required"});
            };
            if shared.updates.contains_key(id)
                || shared.updates.values().any(|batch| {
                    batch.finished.is_none()
                        && batch.targets.iter().any(|(target, _, _)| target == &key)
                })
            {
                return json!({"ok":false,"error":"Shard update in progress or operation ID conflict"});
            }
            if let Some(operation) = shared.operations.get(id) {
                return if operation["command"] == request.command
                    && operation["bot"].as_str() == request.bot.as_deref()
                {
                    json!({"ok":true,"operation":operation})
                } else {
                    json!({"ok":false,"error":"operation id conflict"})
                };
            }
            if shared.operations.len() >= 256 {
                return json!({"ok":false,"error":"operation history full"});
            }
            let operation = json!({"operation_id":id,"command":request.command,"bot":request.bot,"state":"running","error":null});
            // Insert under the same lock before waking the actor.
            shared.operations.insert(id.clone(), operation.clone());
            if sender
                .try_send(Job {
                    id: id.clone(),
                    command: request.command.clone(),
                    update: None,
                })
                .is_err()
            {
                shared.operations.remove(id);
                return json!({"ok":false,"error":"Shard control busy"});
            }
            json!({"ok":true,"operation":operation})
        }
    }

    #[cfg(test)]
    mod update_tests {
        use super::*;

        #[tokio::test]
        async fn artifact_check_deadline_returns_no_launchable_candidate() {
            let check = tokio::spawn(std::future::pending::<Result<PathBuf>>());
            assert_eq!(
                checked_artifact(check, Duration::from_millis(1)).await,
                Err("artifact verification timed out; candidate not launched")
            );
        }

        fn fixture() -> (Runtime, Vec<mpsc::Receiver<Job>>, UpdateControlRequest) {
            let config: Config = toml::from_str("[cluster]\nid='test'\nmode='standalone'\n[node]\nid='local'\n[[bots]]\nname='alpha'\nbot_id='101'\napplication_id='101'\ntoken_env='TOKEN_ALPHA'\ntotal_shards=2\n").unwrap();
            let shared = Arc::new(StdMutex::new(Shared::default()));
            let mut actors = BTreeMap::new();
            let mut receivers = Vec::new();
            for shard_id in 0..2 {
                let key = ShardKey {
                    bot_id: "101".into(),
                    shard_id,
                };
                shared.lock().unwrap().shards.insert(
                    key.clone(),
                    json!({"phase":"Stable",
                    "active_generation":1,"generation_high_water":1}),
                );
                let (sender, receiver) = mpsc::channel(1);
                actors.insert(key, sender);
                receivers.push(receiver);
            }
            let request = serde_json::from_value(json!({"command":"update","bot":"101",
                "operation_id":"0123456789abcdef0123456789abcdef","yes":true,"component":"gateway",
                "manifest":{"bot_id":"101","bot_name":"alpha","total_shards":2,
                    "shards":[{"shard_id":0,"gateway":{"current":1,"candidate":2}},
                              {"shard_id":1,"gateway":{"current":1,"candidate":2}}],
                    "artifacts":{"gateway":{"path":std::env::current_exe().unwrap(),"sha256":"a".repeat(64)}},
                    "identify":{"limits":{"max_concurrency":2,"remaining":2,"total":10,"reset_after":60000},"observed_age_ms":0}}})).unwrap();
            (
                Runtime {
                    shared,
                    actors,
                    tasks: Vec::new(),
                    config,
                },
                receivers,
                request,
            )
        }

        #[test]
        fn batch_admission_is_atomic_and_checks_live_generations() {
            let (runtime, mut receivers, mut request) = fixture();
            request.manifest.shards[1].gateway.as_mut().unwrap().current = 3;
            request.manifest.shards[1]
                .gateway
                .as_mut()
                .unwrap()
                .candidate = 4;
            assert_eq!(runtime.request_update(&request)["ok"], false);
            assert!(
                receivers
                    .iter_mut()
                    .all(|r| matches!(r.try_recv(), Err(mpsc::error::TryRecvError::Empty)))
            );
            assert!(runtime.shared.lock().unwrap().operations.is_empty());
            request.manifest.shards[1].gateway.as_mut().unwrap().current = 1;
            request.manifest.shards[1]
                .gateway
                .as_mut()
                .unwrap()
                .candidate = 2;
            let key = ShardKey {
                bot_id: "101".into(),
                shard_id: 1,
            };
            runtime.actors[&key]
                .try_send(Job {
                    id: "occupied".into(),
                    command: "gateway-update".into(),
                    update: None,
                })
                .unwrap_or_else(|_| panic!("empty queue"));
            assert_eq!(runtime.request_update(&request)["ok"], false);
            assert!(matches!(
                receivers[0].try_recv(),
                Err(mpsc::error::TryRecvError::Empty)
            ));
            assert!(runtime.shared.lock().unwrap().operations.is_empty());
        }

        #[test]
        fn update_replay_is_idempotent_conflicts_fail_and_terminal_generations_freeze() {
            let (runtime, mut receivers, mut request) = fixture();
            assert_eq!(runtime.inspect_bot("alpha")["bot"]["bot_id"], "101");
            let admitted = runtime.request_update(&request);
            assert_eq!(admitted["operation"]["state"], "running");
            let replay = runtime.request_update(&request);
            assert_eq!(
                replay["operation"]["operation_id"],
                admitted["operation"]["operation_id"]
            );
            assert_eq!(replay["operation"]["state"], "running");
            let jobs: Vec<_> = receivers
                .iter_mut()
                .map(|r| r.try_recv().unwrap())
                .collect();
            assert!(receivers.iter_mut().all(|r| r.try_recv().is_err()));
            request.manifest.artifacts.gateway.as_mut().unwrap().sha256 = "b".repeat(64);
            assert_eq!(runtime.request_update(&request)["ok"], false);
            {
                let mut shared = runtime.shared.lock().unwrap();
                for job in jobs {
                    assert_eq!(job.update.as_ref().unwrap().candidate, 2);
                    shared.operations.get_mut(&job.id).unwrap()["state"] = json!("succeeded");
                }
                for shard in shared.shards.values_mut() {
                    shard["active_generation"] = json!(2);
                }
            }
            let terminal = runtime.update_status(&request.operation_id);
            assert_eq!(terminal["operation"]["state"], "succeeded");
            assert_eq!(
                terminal["operation"]["final_generations"]
                    .as_array()
                    .unwrap()
                    .len(),
                2
            );
            assert_eq!(terminal["operation"]["final_generations"][0]["gateway"], 2);
            for shard in runtime.shared.lock().unwrap().shards.values_mut() {
                shard["active_generation"] = json!(99);
            }
            assert_eq!(runtime.update_status(&request.operation_id), terminal);
        }

        #[test]
        fn rollback_to_old_generation_is_failure_and_core_is_not_emulated_by_gateway_restart() {
            let (runtime, mut receivers, mut request) = fixture();
            request.component = UpdateComponent::Core;
            request.manifest.artifacts.core = request.manifest.artifacts.gateway.clone();
            for shard in &mut request.manifest.shards {
                shard.core = shard.gateway;
            }
            assert_eq!(
                runtime.request_update(&request)["error"],
                "Core update execution is not connected"
            );
            request.component = UpdateComponent::Gateway;
            assert_eq!(runtime.request_update(&request)["ok"], true);
            for receiver in &mut receivers {
                let job = receiver.try_recv().unwrap();
                runtime
                    .shared
                    .lock()
                    .unwrap()
                    .operations
                    .get_mut(&job.id)
                    .unwrap()["state"] = json!("succeeded");
            }
            let failed = runtime.update_status(&request.operation_id);
            assert_eq!(failed["ok"], false);
            assert_eq!(failed["operation"]["state"], "failed");
            assert_eq!(
                failed["operation"]["failed_shards"]
                    .as_array()
                    .unwrap()
                    .len(),
                2
            );
            assert_eq!(failed["operation"]["final_generations"][0]["gateway"], 1);
        }
    }
}
