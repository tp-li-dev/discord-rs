//! Mock-only daemon host for the OpenRaft cluster runtime.
//!
//! The cluster runtime is the sole owner of Gateway restart admission. This
//! adapter must not also register these children with the standalone daemon's
//! `ChildRecovery`: an exit is followed by a new grant, never an independent
//! restart under an old epoch. The existing independent guardian owns and
//! reaps the Gateway and enforces its last lease after botd dies.
//!
//! A guardian exit alone is not proof that its Gateway exited. Only its final
//! stopped notice AND a successful owned-handle wait permit releasing a Raft
//! reservation. An unconfirmed guardian failure latches this local shard closed.

use super::{ShardKey, ShardRuntimeSpec};
use crate::child_recovery::RecoveryConfig;
use crate::cluster_runtime::{
    ClusterAction, ClusterRuntime, LoadedClusterConfig, RaftRuntimeOptions,
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, VecDeque},
    net::SocketAddr,
    path::{Path, PathBuf},
    process::Stdio,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    process::{Child, Command},
    sync::mpsc,
    task::JoinHandle,
};

const PIPE_LIMIT: usize = 64 * 1024;
const NOTICE_LIMIT: usize = 4096;
const HISTORY_LIMIT: usize = 256;
const WRITE_WAIT: Duration = Duration::from_millis(100);

// Same private anonymous-pipe contract as test_cluster_processes::run_guardian.
// No Debug implementations: paths and arbitrary input are never diagnostics.
#[derive(Serialize)]
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

#[derive(Serialize)]
#[serde(tag = "command", rename_all = "snake_case")]
enum Input {
    Launch(Launch),
    Renew { epoch: u64, deadline_ms: u64 },
    Stop {},
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Notice {
    state: String,
    gateway_pid: Option<u32>,
    reason: String,
}

enum ProcessMessage {
    Notice(ShardKey, u64, Notice),
    Closed(ShardKey, u64),
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Phase {
    Starting,
    Active,
    Draining,
    Degraded,
}

impl Phase {
    fn label(self) -> &'static str {
        match self {
            Self::Starting => "starting",
            Self::Active => "active",
            Self::Draining => "draining",
            Self::Degraded => "degraded",
        }
    }
}

#[derive(Default)]
struct ExitProof {
    stopped_notice: bool,
    pipe_closed: bool,
    exit_success: Option<bool>,
}

impl ExitProof {
    fn confirmed(&self) -> bool {
        self.stopped_notice && self.pipe_closed && self.exit_success == Some(true)
    }

    fn unconfirmed_exit(&self) -> bool {
        self.pipe_closed && self.exit_success.is_some() && !self.confirmed()
    }
}

struct Process {
    epoch: u64,
    phase: Phase,
    child: Child,
    gateway_pid: Option<u32>,
    reader: Option<JoinHandle<()>>,
    proof: ExitProof,
    failed: bool,
}

/// Same limits and delay arithmetic as ChildRecovery, without assigning two
/// controllers ownership of the guardian/Gateway handles.
#[derive(Default)]
struct RecoveryBudget {
    failures: u32,
    consecutive: u32,
    last_failure_epoch: Option<u64>,
    recent: VecDeque<Instant>,
    ready_at: Option<Instant>,
    retry_at: Option<Instant>,
    exhausted: Option<&'static str>,
}

impl RecoveryBudget {
    fn end_ready(&mut self, now: Instant, stable_for: Duration) {
        if self
            .ready_at
            .take()
            .is_some_and(|ready| now.saturating_duration_since(ready) >= stable_for)
        {
            self.consecutive = 0;
        }
    }

    fn failed(&mut self, epoch: u64, now: Instant, config: RecoveryConfig) -> bool {
        if self.last_failure_epoch == Some(epoch) {
            return false;
        }
        self.last_failure_epoch = Some(epoch);
        self.end_ready(now, config.stable_for);
        self.failures = self.failures.saturating_add(1);
        self.consecutive = self.consecutive.saturating_add(1);
        while self
            .recent
            .front()
            .is_some_and(|at| now.saturating_duration_since(*at) >= config.restart.window)
        {
            self.recent.pop_front();
        }
        self.recent.push_back(now);
        self.exhausted = self.exhausted.or_else(|| {
            if self.failures > config.restart.max_restarts {
                Some("restart_budget")
            } else if self.recent.len() > config.restart.max_restarts_in_window as usize {
                Some("restart_window")
            } else if self.consecutive >= config.max_startup_failures {
                Some("startup_crash_loop")
            } else {
                None
            }
        });
        let mut delay = config.restart.backoff(self.failures.saturating_sub(1));
        if self.consecutive >= config.restart.circuit_breaker_after {
            delay = delay.max(config.restart.circuit_open_for);
        }
        self.retry_at = now.checked_add(delay);
        if self.retry_at.is_none() {
            self.exhausted = Some("recovery_clock_overflow");
        }
        true
    }
}

struct PendingStart {
    epoch: u64,
    expires_at: Instant,
    // None is a latched degraded slot, never permission to start immediately.
    not_before: Option<Instant>,
}

impl Drop for Process {
    fn drop(&mut self) {
        if let Some(reader) = &self.reader {
            reader.abort();
        }
        // Closing botd's input cannot extend the independent guardian's lease.
        // Do not kill the guardian and strand the Gateway it owns.
    }
}

/// Token-free loopback process host. Production Discord is always rejected.
pub struct ClusterHost {
    runtime: ClusterRuntime,
    specs: BTreeMap<ShardKey, ShardRuntimeSpec>,
    processes: BTreeMap<ShardKey, Process>,
    gateway_program: PathBuf,
    collector: SocketAddr,
    node_id: u64,
    max_lease_ms: u64,
    stop_grace_ms: u64,
    sender: mpsc::Sender<ProcessMessage>,
    notices: mpsc::Receiver<ProcessMessage>,
    transitions: VecDeque<Value>,
    monitor_ok: bool,
    recovery: BTreeMap<ShardKey, RecoveryBudget>,
    pending_starts: BTreeMap<ShardKey, PendingStart>,
}

fn wire_deadline(expires_at: Instant) -> Result<u64, &'static str> {
    let wall: u64 = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| "cluster guardian clock invalid")?
        .as_millis()
        .try_into()
        .map_err(|_| "cluster guardian clock overflow")?;
    let remaining: u64 = expires_at
        .checked_duration_since(Instant::now())
        .ok_or("cluster lease expired before process action")?
        .as_millis()
        .try_into()
        .map_err(|_| "cluster lease duration overflow")?;
    if remaining == 0 {
        return Err("cluster lease expired before process action");
    }
    wall.checked_add(remaining)
        .ok_or("cluster lease deadline overflow")
}

async fn write_input(child: &mut Child, input: &Input) -> Result<(), &'static str> {
    let pipe = child
        .stdin
        .as_mut()
        .ok_or("cluster guardian input closed")?;
    let mut frame = serde_json::to_vec(input).map_err(|_| "cluster guardian input invalid")?;
    frame.push(b'\n');
    if frame.len() > PIPE_LIMIT {
        return Err("cluster guardian input too large");
    }
    tokio::time::timeout(WRITE_WAIT, async {
        pipe.write_all(&frame).await?;
        pipe.flush().await
    })
    .await
    .map_err(|_| "cluster guardian input timeout")?
    .map_err(|_| "cluster guardian input failed")
}

impl ClusterHost {
    pub async fn bind(
        config: &LoadedClusterConfig,
        raft: RaftRuntimeOptions,
        runtime_root: &Path,
        gateway_program: &Path,
        mock_discord: Option<SocketAddr>,
    ) -> Result<Self, &'static str> {
        // Reject before certificate/store reads, network bind or process work.
        if !raft.allow_raft || raft.enable_production {
            return Err("Raft cluster requires explicit mock-only opt-in");
        }
        let collector = mock_discord
            .filter(|a| a.ip().is_loopback() && a.port() != 0)
            .ok_or("Raft cluster requires loopback mock Discord")?;
        if !gateway_program.is_absolute() {
            return Err("Raft cluster Gateway program must be absolute");
        }
        let settings = config
            .test_cluster
            .as_ref()
            .ok_or("Raft cluster settings required")?;
        let specs = config
            .local_specs(runtime_root)
            .map_err(|_| "Raft cluster shard configuration invalid")?;
        let runtime = ClusterRuntime::bind_raft(config, raft, true)
            .await
            .map_err(|_| "Raft cluster configuration, storage or mTLS invalid")?;
        let (sender, notices) = mpsc::channel(128);
        Ok(Self {
            runtime,
            specs: specs
                .into_iter()
                .map(|spec| {
                    (
                        ShardKey {
                            bot_id: spec.bot_id.clone(),
                            shard_id: spec.shard_id,
                        },
                        spec,
                    )
                })
                .collect(),
            processes: BTreeMap::new(),
            gateway_program: gateway_program.to_owned(),
            collector,
            node_id: settings.node_id,
            max_lease_ms: settings.lease_ms,
            stop_grace_ms: (settings.stop_margin_ms / 3).min(250),
            sender,
            notices,
            transitions: VecDeque::new(),
            monitor_ok: true,
            recovery: BTreeMap::new(),
            pending_starts: BTreeMap::new(),
        })
    }

    fn record(&mut self, key: &ShardKey, epoch: u64, state: &str, reason: &'static str) {
        if self.transitions.len() == HISTORY_LIMIT {
            self.transitions.pop_front();
        }
        self.transitions
            .push_back(json!({"key":key,"bot_id":key.bot_id,
            "shard_id":key.shard_id,"epoch":epoch,"state":state,"reason":reason}));
    }

    async fn start(
        &mut self,
        key: ShardKey,
        epoch: u64,
        expires_at: Instant,
    ) -> Result<(), &'static str> {
        if self.processes.contains_key(&key) {
            return Err("cluster guardian still owned or unconfirmed");
        }
        let spec = self
            .specs
            .get(&key)
            .ok_or("cluster shard not assigned here")?;
        let launch = Launch {
            program: self.gateway_program.clone(),
            args: spec
                .gateway
                .args
                .iter()
                .map(|a| {
                    a.to_str()
                        .map(str::to_owned)
                        .ok_or("cluster launch arguments must be UTF-8")
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
        let executable = std::env::current_exe().map_err(|_| "cluster guardian unavailable")?;
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
                    .ok_or("cluster guardian system environment unavailable")?,
            );
            command.creation_flags(0x0000_0200 | 0x0800_0000);
        }
        #[cfg(unix)]
        command.process_group(0);
        if Instant::now() >= expires_at {
            return Err("cluster lease expired before guardian spawn");
        }
        let mut child = command
            .spawn()
            .map_err(|_| "cluster guardian spawn failed")?;
        let reader = child.stdout.take().map(|stdout| {
            let sender = self.sender.clone();
            let reader_key = key.clone();
            tokio::spawn(async move {
                let mut reader = BufReader::new(stdout);
                loop {
                    let mut frame = Vec::new();
                    let result = (&mut reader)
                        .take((NOTICE_LIMIT + 1) as u64)
                        .read_until(b'\n', &mut frame)
                        .await;
                    if !matches!(result, Ok(1..))
                        || frame.len() > NOTICE_LIMIT
                        || frame.last() != Some(&b'\n')
                    {
                        break;
                    }
                    let Ok(notice) = serde_json::from_slice::<Notice>(&frame) else {
                        break;
                    };
                    if sender
                        .send(ProcessMessage::Notice(reader_key.clone(), epoch, notice))
                        .await
                        .is_err()
                    {
                        return;
                    }
                }
                let _ = sender.send(ProcessMessage::Closed(reader_key, epoch)).await;
            })
        });
        let pipe_closed = reader.is_none();
        self.processes.insert(
            key.clone(),
            Process {
                epoch,
                phase: Phase::Starting,
                child,
                gateway_pid: None,
                reader,
                proof: ExitProof {
                    pipe_closed,
                    ..ExitProof::default()
                },
                failed: false,
            },
        );
        self.record(&key, epoch, "starting", "raft_lease");
        // A timeout may have delivered Launch. Keep the handle and watchdog;
        // only an observed, confirmed exit can authorize releasing ownership.
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

    async fn stop(&mut self, key: &ShardKey, reason: &'static str) -> Result<(), &'static str> {
        if let Some(budget) = self.recovery.get_mut(key) {
            budget.end_ready(Instant::now(), RecoveryConfig::default().stable_for);
        }
        let Some(process) = self.processes.get_mut(key) else {
            return Ok(());
        };
        if process.phase == Phase::Degraded {
            return Ok(());
        }
        process.phase = Phase::Draining;
        let epoch = process.epoch;
        let result = write_input(&mut process.child, &Input::Stop {}).await;
        self.record(key, epoch, "draining", reason);
        result
    }

    async fn failed(&mut self, key: &ShardKey, epoch: u64) -> Result<(), &'static str> {
        self.monitor_ok = false;
        if let Some(process) = self.processes.get_mut(key)
            && process.epoch == epoch
        {
            process.failed = true;
        }
        if let Some(ClusterAction::Stop { reason, .. }) = self
            .runtime
            .process_failed_epoch(key, epoch)
            .map_err(|_| "cluster process failure could not revoke admission")?
        {
            let _ = self.stop(key, reason).await;
        }
        if self
            .pending_starts
            .get(key)
            .is_some_and(|start| start.epoch == epoch && start.not_before.is_some())
        {
            self.pending_starts.remove(key);
            self.runtime
                .process_exited_epoch(key, epoch)
                .map_err(|_| "cluster failed unstarted grant release failed")?;
        }
        Ok(())
    }

    fn recovery_failure(&mut self, key: &ShardKey, epoch: u64) {
        let budget = self.recovery.entry(key.clone()).or_default();
        if budget.failed(epoch, Instant::now(), RecoveryConfig::default()) {
            let reason = budget.exhausted.unwrap_or("restart_backoff");
            self.record(key, epoch, "recovery_waiting", reason);
        }
    }

    async fn poll_processes(&mut self) -> Result<(), &'static str> {
        // The pipe reader fixes epoch when spawned. Never reconstruct it from
        // the runtime's current status after receiving an asynchronous notice.
        let mut failures = Vec::new();
        let mut recovery_failures = Vec::new();
        for _ in 0..1024 {
            let Ok(message) = self.notices.try_recv() else {
                break;
            };
            match message {
                ProcessMessage::Notice(key, epoch, notice) => {
                    let Some(process) = self.processes.get_mut(&key) else {
                        continue;
                    };
                    if process.epoch != epoch {
                        continue;
                    }
                    match notice.state.as_str() {
                        "ready" if process.phase == Phase::Starting => {
                            if notice.gateway_pid.is_none_or(|pid| pid == 0) {
                                failures.push((key, epoch));
                                continue;
                            }
                            process.gateway_pid = notice.gateway_pid;
                            self.runtime
                                .startup_complete_epoch(&key, epoch)
                                .map_err(|_| "cluster readiness callback failed")?;
                            if self.runtime.check_active(&key, epoch).is_ok() {
                                process.phase = Phase::Active;
                                self.recovery.entry(key.clone()).or_default().ready_at =
                                    Some(Instant::now());
                                self.record(&key, epoch, "active", "gateway_core_ready");
                            }
                        }
                        "ready" => {} // Late Ready never revives a draining child.
                        "draining" => {
                            if let Some(budget) = self.recovery.get_mut(&key) {
                                budget.end_ready(
                                    Instant::now(),
                                    RecoveryConfig::default().stable_for,
                                );
                            }
                            if process.phase != Phase::Degraded {
                                process.phase = Phase::Draining;
                            }
                            failures.push((key, epoch));
                        }
                        "stopped" => {
                            process.proof.stopped_notice = true;
                            // The guardian waited for its owned Gateway before
                            // sending this notice, but we still reap it below.
                            if !matches!(notice.reason.as_str(), "lease_expired" | "handoff") {
                                recovery_failures.push((key.clone(), epoch));
                            }
                        }
                        _ => failures.push((key, epoch)),
                    }
                }
                ProcessMessage::Closed(key, epoch) => {
                    if let Some(process) = self.processes.get_mut(&key)
                        && process.epoch == epoch
                    {
                        process.proof.pipe_closed = true;
                        if !process.proof.stopped_notice {
                            failures.push((key, epoch));
                        }
                    }
                }
            }
        }
        let mut confirmed = Vec::new();
        let mut unknown = Vec::new();
        for (key, process) in &mut self.processes {
            match process.child.try_wait() {
                Ok(Some(status)) => {
                    process.proof.exit_success = Some(status.success());
                    if process.proof.confirmed() {
                        confirmed.push((key.clone(), process.epoch));
                    } else if process.proof.unconfirmed_exit() && process.phase != Phase::Degraded {
                        unknown.push((key.clone(), process.epoch));
                    } else if !process.failed && !process.proof.stopped_notice {
                        failures.push((key.clone(), process.epoch));
                    }
                }
                Err(_) if !process.failed => failures.push((key.clone(), process.epoch)),
                _ => {}
            }
        }
        failures.sort();
        failures.dedup();
        for (key, epoch) in recovery_failures {
            self.recovery_failure(&key, epoch);
        }
        for (key, epoch) in failures {
            if confirmed.contains(&(key.clone(), epoch)) {
                continue;
            }
            // Natural/ordered stops also revoke admission before release, but
            // are not monitor errors. Only an absent stop proof is degraded.
            let normal_stop = self
                .processes
                .get(&key)
                .is_some_and(|p| p.proof.stopped_notice || p.phase == Phase::Draining);
            let was_ok = self.monitor_ok;
            self.failed(&key, epoch).await?;
            if normal_stop {
                self.monitor_ok = was_ok;
            }
        }
        for (key, epoch) in unknown {
            self.failed(&key, epoch).await?;
            self.processes.get_mut(&key).expect("owned guardian").phase = Phase::Degraded;
            self.record(&key, epoch, "degraded", "gateway_exit_unconfirmed");
            // Keep a tombstone: no Release and no local replacement. Another
            // owner can recover only through the runtime's fenced quarantine.
        }
        for (key, epoch) in confirmed {
            self.processes.remove(&key);
            if let Some(budget) = self.recovery.get_mut(&key) {
                budget.end_ready(Instant::now(), RecoveryConfig::default().stable_for);
            }
            self.runtime
                .process_exited_epoch(&key, epoch)
                .map_err(|_| "cluster process exit callback failed")?;
            self.record(&key, epoch, "stopped", "gateway_and_guardian_reaped");
        }
        Ok(())
    }

    /// Poll bounded control events and apply grants. Network/storage work stays
    /// inside the runtime; Gateway expiry also has an independent process clock.
    pub async fn tick(&mut self) -> Result<(), &'static str> {
        self.poll_processes().await?;
        let actions = self
            .runtime
            .tick()
            .map_err(|_| "Raft cluster runtime failed closed")?;
        for action in actions {
            match action {
                ClusterAction::Start {
                    key,
                    epoch,
                    expires_at,
                } => {
                    let budget = self.recovery.entry(key.clone()).or_default();
                    let not_before = budget.retry_at.unwrap_or_else(Instant::now);
                    if budget.exhausted.is_some() {
                        self.pending_starts.insert(
                            key.clone(),
                            PendingStart {
                                epoch,
                                expires_at,
                                not_before: None,
                            },
                        );
                        self.failed(&key, epoch).await?;
                        // Keep this local recovery latch closed. Withholding
                        // the restart callback is not a claim of a live child.
                    } else {
                        self.pending_starts.insert(
                            key,
                            PendingStart {
                                epoch,
                                expires_at,
                                not_before: Some(not_before),
                            },
                        );
                    }
                }
                ClusterAction::Renew {
                    key,
                    epoch,
                    expires_at,
                } => {
                    if let Some(pending) = self.pending_starts.get_mut(&key) {
                        if pending.epoch == epoch
                            && pending.not_before.is_some()
                            && Instant::now() < pending.expires_at
                        {
                            pending.expires_at = expires_at;
                        } else {
                            self.failed(&key, epoch).await?;
                        }
                        continue;
                    }
                    let result = if let Some(process) = self.processes.get_mut(&key) {
                        if process.epoch == epoch
                            && matches!(process.phase, Phase::Starting | Phase::Active)
                        {
                            match wire_deadline(expires_at) {
                                Ok(deadline_ms) => {
                                    write_input(
                                        &mut process.child,
                                        &Input::Renew { epoch, deadline_ms },
                                    )
                                    .await
                                }
                                Err(error) => Err(error),
                            }
                        } else {
                            Err("cluster guardian renewal fenced")
                        }
                    } else {
                        Err("cluster guardian unavailable")
                    };
                    if result.is_err() {
                        self.recovery_failure(&key, epoch);
                        self.failed(&key, epoch).await?;
                    }
                }
                ClusterAction::Stop { key, reason } => {
                    if let Some(pending) = self.pending_starts.get(&key) {
                        if pending.not_before.is_some() {
                            let epoch = pending.epoch;
                            self.pending_starts.remove(&key);
                            self.runtime
                                .process_exited_epoch(&key, epoch)
                                .map_err(|_| "cluster cancelled unstarted grant release failed")?;
                        }
                        continue;
                    }
                    if self.stop(&key, reason).await.is_err() {
                        self.monitor_ok = false;
                    }
                }
            }
        }
        let ready: Vec<_> = self
            .pending_starts
            .iter()
            .filter(|(_, start)| start.not_before.is_some_and(|at| Instant::now() >= at))
            .map(|(key, start)| (key.clone(), start.epoch, start.expires_at))
            .collect();
        for (key, epoch, expires_at) in ready {
            self.pending_starts.remove(&key);
            if self.start(key.clone(), epoch, expires_at).await.is_err() {
                self.recovery_failure(&key, epoch);
                if !self.processes.contains_key(&key) {
                    // A start error without a retained handle proves that no
                    // child was created, allowing this unused grant's release.
                    self.monitor_ok = false;
                    self.runtime
                        .process_exited_epoch(&key, epoch)
                        .map_err(|_| "cluster unstarted grant release failed")?;
                } else {
                    self.failed(&key, epoch).await?;
                }
            }
        }
        Ok(())
    }

    pub fn status(&self) -> Value {
        let processes: Vec<_> = self
            .processes
            .iter()
            .map(|(key, process)| {
                json!({
                    "key":key,"bot_id":key.bot_id,"shard_id":key.shard_id,
                    "state":process.phase.label(),"epoch":process.epoch,
                    "guardian_pid":process.child.id(),"gateway_pid":process.gateway_pid,
                    "active_admission":self.runtime.check_active(key,process.epoch).is_ok(),
                    "gateway_exit_confirmed":process.proof.confirmed(),
                })
            })
            .collect();
        let recovery: Vec<_> = self.recovery.iter().map(|(key, budget)| json!({
            "key":key,"failures":budget.failures,"consecutive":budget.consecutive,
            "degraded":budget.exhausted,
            "retry_after_ms":budget.retry_at.map(|at| at.saturating_duration_since(Instant::now()).as_millis()),
            "waiting":self.pending_starts.contains_key(key),
        })).collect();
        json!({"cluster":self.runtime.status(),"processes":processes,
            "transitions":self.transitions,"monitor_ok":self.monitor_ok,
            "recovery":recovery,
            "recovery_policy":"new_raft_grant_after_confirmed_gateway_exit"})
    }

    pub async fn shutdown(&mut self) {
        // Shutdown does not prove delegated process exit and must not release
        // durable reservations. The guardians retain their last bounded lease.
        self.runtime.shutdown().await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn guardian_exit_without_gateway_wait_proof_never_releases() {
        let mut proof = ExitProof {
            pipe_closed: true,
            exit_success: Some(true),
            ..ExitProof::default()
        };
        assert!(!proof.confirmed());
        assert!(proof.unconfirmed_exit());
        proof.stopped_notice = true;
        assert!(proof.confirmed());
        proof.exit_success = Some(false);
        assert!(!proof.confirmed());
        assert!(proof.unconfirmed_exit());
    }

    #[test]
    fn stopped_notice_or_control_eof_alone_cannot_release() {
        let mut proof = ExitProof {
            stopped_notice: true,
            ..ExitProof::default()
        };
        assert!(!proof.confirmed());
        proof.pipe_closed = true;
        assert!(!proof.confirmed());
        proof.exit_success = Some(true);
        assert!(proof.confirmed());
    }

    #[test]
    fn expired_delegation_is_rejected_before_spawn_or_renew() {
        assert!(wire_deadline(Instant::now() - Duration::from_millis(1)).is_err());
        assert!(wire_deadline(Instant::now() + Duration::from_secs(1)).is_ok());
    }

    #[test]
    fn recovery_uses_shared_backoff_and_counts_a_generation_once() {
        let now = Instant::now();
        let config = RecoveryConfig::default();
        let mut budget = RecoveryBudget::default();
        assert!(budget.failed(1, now, config));
        assert_eq!(budget.retry_at, Some(now + config.restart.initial_backoff));
        assert!(!budget.failed(1, now + Duration::from_millis(1), config));
        assert_eq!(budget.failures, 1);
        assert!(budget.failed(2, now, config));
        assert_eq!(budget.retry_at, Some(now + config.restart.backoff(1)));
        assert!(budget.failed(3, now, config));
        assert_eq!(budget.retry_at, Some(now + config.restart.circuit_open_for));
        assert!(budget.failed(4, now, config));
        assert_eq!(budget.exhausted, Some("restart_window"));
    }

    #[test]
    fn recovery_lifetime_budget_survives_stability_and_clock_windows() {
        let now = Instant::now();
        let mut config = RecoveryConfig::default();
        config.restart.max_restarts = 1;
        let mut budget = RecoveryBudget::default();
        budget.failed(1, now, config);
        budget.ready_at = Some(now);
        budget.failed(2, now + config.restart.window + config.stable_for, config);
        assert_eq!(budget.consecutive, 1);
        assert_eq!(budget.exhausted, Some("restart_budget"));
        budget.ready_at = Some(now);
        budget.failed(3, now + config.restart.window * 2, config);
        assert_eq!(budget.exhausted, Some("restart_budget"));
    }

    #[test]
    fn stopped_time_does_not_count_as_stable_readiness() {
        let now = Instant::now();
        let config = RecoveryConfig::default();
        let mut budget = RecoveryBudget::default();
        budget.failed(1, now, config);
        budget.ready_at = Some(now);
        budget.end_ready(now + Duration::from_secs(1), config.stable_for);
        budget.failed(2, now + config.stable_for * 2, config);
        assert_eq!(
            budget.consecutive, 2,
            "stopped time reset consecutive failures"
        );
        assert_eq!(budget.ready_at, None);
        // A genuinely stable generation still clears consecutive failures,
        // while the lifetime counter survives the orderly handoff.
        let later = now + config.stable_for * 3;
        budget.ready_at = Some(later);
        budget.end_ready(later + config.stable_for, config.stable_for);
        budget.failed(
            3,
            later + config.stable_for + Duration::from_secs(1),
            config,
        );
        assert_eq!(budget.consecutive, 1);
        assert_eq!(budget.failures, 3);
    }
}
