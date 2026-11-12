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
                "start" | "restart" => selector && id && !self.force && !self.yes,
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
                !serde_json::from_str::<CommandRequest>(r#"{"command":"ps","unexpected":true}"#)
                    .is_ok()
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
