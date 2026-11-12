//! Local supervision of one Gateway execution unit per configured Bot/Shard.
//!
//! This skeleton launches only `ShardRuntimeSpec::gateway`; Core lifecycle and
//! authenticated Gateway shutdown/Ready/lease/Identify admission are separate
//! integration work. Running means an OS child exists, not Discord readiness.
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
    path::{Component, Path, PathBuf},
    process::Stdio,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::process::{Child, Command};

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct ShardKey {
    pub bot_id: String,
    pub shard_id: u32,
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
    ReservationExists,
    ReservationIo,
    SpawnFailed,
    ChildExited,
    MonitorFailed,
    StopFailed,
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
    /// Suppress retries and wait for exit requested through the Gateway's own
    /// control channel. This skeleton does not implement that shutdown protocol.
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
        let credential = credentials
            .read(name)
            .filter(|v| !v.is_empty())
            .ok_or(DaemonError::MissingCredential)?;
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
            .env(name, credential)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .kill_on_drop(false);
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
                || entry.child.is_some()
                || !entry.snapshot.desired_running
                || !entry.retry_at.is_some_and(|at| now >= at)
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
}
