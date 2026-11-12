//! Server-owned capability policy and volatile update journal. Audit fsync and
//! artifact hashing run outside the supervision loop. An accepted ID is retained
//! until daemon exit, including failures and audit failures; reconnects only query.
use super::{ShardKey, control::OPERATION_LIMIT};
use crate::{
    placement::Config,
    rbac_audit::{AuditConfig, AuditedExecutor, ExecuteError, Principal, Role},
    update_rollback::{self, UpdateControlRequest},
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    time::{Duration, Instant},
};
use tokio::sync::{mpsc, oneshot};

/// Only accepted from the daemon's trusted anonymous launch bootstrap, never
/// from a management command. Omitting scope grants all configured local shards.
#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ControlAccess {
    pub subject: String,
    pub role: String,
    #[serde(default)]
    pub scope: Option<Vec<ShardKey>>,
}

impl Default for ControlAccess {
    fn default() -> Self {
        Self {
            subject: "local-bootstrap".into(),
            role: "admin".into(),
            scope: None,
        }
    }
}

impl ControlAccess {
    pub fn valid(&self) -> bool {
        !self.subject.is_empty()
            && self.subject.len() <= 64
            && self
                .subject
                .bytes()
                .all(|c| c.is_ascii_alphanumeric() || matches!(c, b'-' | b'_' | b'.'))
            && Role::parse(&self.role).is_some()
            && self.scope.as_ref().is_none_or(|s| {
                s.len() <= 64
                    && s.iter().all(|k| {
                        !k.bot_id.is_empty()
                            && k.bot_id.len() <= 64
                            && !k.bot_id.chars().any(char::is_control)
                    })
            })
    }

    pub fn allows_keys(&self, keys: &[ShardKey]) -> bool {
        self.valid()
            && self
                .scope
                .as_ref()
                .is_none_or(|scope| !keys.is_empty() && keys.iter().all(|key| scope.contains(key)))
    }

    pub fn allows_command(&self, read: bool, bot: Option<&str>, keys: &[ShardKey]) -> bool {
        self.valid()
            && (read || self.role == "admin")
            && (self.scope.is_none() || bot.is_some())
            && self.allows_keys(keys)
    }
}

struct Entry {
    fingerprint: Vec<u8>,
    subject: String,
    bot: String,
    targets: Vec<ShardKey>,
    started: Instant,
    response: Value,
}

struct AuditJob {
    request: UpdateControlRequest,
    access: ControlAccess,
    allowed: bool,
    admission: Result<(), &'static str>,
}

enum Event {
    Authorized(Prepared),
    Prepared(Prepared),
    Finished {
        id: String,
        response: Option<Value>,
        result: Result<(), ExecuteError>,
    },
}

pub struct Prepared {
    pub request: UpdateControlRequest,
    pub completion: oneshot::Sender<Value>,
}

pub struct UpdateHub {
    config: Config,
    local: Vec<ShardKey>,
    entries: BTreeMap<String, Entry>,
    jobs: mpsc::Sender<AuditJob>,
    events: mpsc::Receiver<Event>,
    sender: mpsc::Sender<Event>,
    pending: BTreeMap<String, oneshot::Sender<Value>>,
}

fn failure(error: &'static str) -> Value {
    json!({"ok":false,"offline":false,"error":error})
}

fn operation(request: &UpdateControlRequest, state: &str, error: Option<&str>) -> Value {
    let keys: Vec<_> = request
        .manifest
        .shards
        .iter()
        .map(|s| ShardKey {
            bot_id: request.bot.clone(),
            shard_id: s.shard_id,
        })
        .collect();
    json!({"ok":state != "failed","offline":false,"operation":{
        "operation_id":request.operation_id,"command":request.command_name(),"bot_id":request.bot,
        "state":state,"elapsed_ms":0,"shards":keys.iter().map(|key| json!({"key":key})).collect::<Vec<_>>(),
        "failed_shards":if state == "failed" {keys} else {Vec::new()},"final_generations":[],"error":error}})
}

fn successful_completion(request: &UpdateControlRequest, value: &Value) -> bool {
    let op = &value["operation"];
    if value["ok"] != true
        || op["state"] != "succeeded"
        || op["operation_id"] != request.operation_id
        || op["command"] != request.command_name()
        || op["bot_id"] != request.bot
        || !op["failed_shards"].as_array().is_some_and(Vec::is_empty)
    {
        return false;
    }
    let Some(shards) = op["shards"].as_array() else {
        return false;
    };
    let Some(generations) = op["final_generations"].as_array() else {
        return false;
    };
    shards.len() == request.manifest.shards.len()
        && generations.len() == shards.len()
        && request.manifest.shards.iter().all(|target| {
            shards
                .iter()
                .filter(|s| {
                    s["key"]["bot_id"] == request.bot && s["key"]["shard_id"] == target.shard_id
                })
                .count()
                == 1
                && generations
                    .iter()
                    .filter(|g| {
                        g["shard_id"] == target.shard_id
                            && (request.component == update_rollback::Component::Gateway
                                || g["core"].as_u64() == target.core.map(|g| g.candidate))
                            && (request.component == update_rollback::Component::Core
                                || g["gateway"].as_u64() == target.gateway.map(|g| g.candidate))
                    })
                    .count()
                    == 1
        })
}

impl UpdateHub {
    pub fn new(config: Config, local: Vec<ShardKey>, audit_path: PathBuf) -> Self {
        let (jobs, mut receiver) = mpsc::channel::<AuditJob>(64);
        let (sender, events) = mpsc::channel(128);
        let notifications = sender.clone();
        // One exclusive audit owner; waiting here occupies a blocking worker,
        // never the async daemon owner or a Gateway heartbeat task.
        tokio::task::spawn_blocking(move || {
            let mut executor = None;
            while let Some(job) = receiver.blocking_recv() {
                let id = job.request.operation_id.clone();
                let mut response = None;
                // Do not lock/create an audit file for daemons that never receive
                // an update. Open failures remain sticky for this incarnation.
                let executor = executor.get_or_insert_with(|| {
                    audit_path
                        .parent()
                        .ok_or(crate::rbac_audit::AuditError::Io)
                        .and_then(|p| {
                            std::fs::create_dir_all(p)
                                .map_err(|_| crate::rbac_audit::AuditError::Io)
                        })
                        .and_then(|()| AuditedExecutor::open(&audit_path, AuditConfig::default()))
                });
                let result = match executor {
                    Err(error) => Err(ExecuteError::Audit(*error)),
                    Ok(executor) => executor.execute_update_scoped(&job.request,
                        Principal::authenticated(job.access.subject, job.access.role), false, job.allowed, || {
                            if let Err(error) = job.admission {
                                response = Some(operation(&job.request, "failed", Some(error)));
                                return Err("admission_rejected".into());
                            }
                            let (completion, finished) = oneshot::channel();
                            // The closure owns only the serialized, validated payload;
                            // no credential is copied into journal entries or audit.
                            let request = serde_json::from_value(serde_json::to_value(&job.request).map_err(|_| "encoding_failed")?).map_err(|_| "encoding_failed")?;
                            notifications.blocking_send(Event::Authorized(Prepared {request, completion})).map_err(|_| "daemon_stopped")?;
                            let mut value = finished.blocking_recv().map_err(|_| "execution_outcome_unknown")?;
                            let success = successful_completion(&job.request, &value);
                            if !success && value["operation"]["state"] == "succeeded" {
                                value = operation(&job.request, "failed", Some("backend returned invalid final generations or target metadata; reconcile execution"));
                            }
                            response = Some(value);
                            if success { Ok(()) } else { Err("update_failed".into()) }
                        }),
                };
                if notifications
                    .blocking_send(Event::Finished {
                        id,
                        response,
                        result,
                    })
                    .is_err()
                {
                    break;
                }
            }
        });
        Self {
            config,
            local,
            entries: BTreeMap::new(),
            jobs,
            events,
            sender,
            pending: BTreeMap::new(),
        }
    }

    pub fn contains(&self, id: &str) -> bool {
        self.entries.contains_key(id)
    }
    pub fn busy(&self, bot: &str) -> bool {
        self.entries
            .values()
            .any(|e| e.bot == bot && e.response["operation"]["state"] == "running")
    }

    pub fn query(&self, id: &str, access: &ControlAccess) -> Value {
        match self.entries.get(id) {
            Some(entry) if access.allows_keys(&entry.targets) => entry.response.clone(),
            _ => failure("operation not found or scope denied"),
        }
    }

    fn validate_target(&self, request: &UpdateControlRequest) -> Result<(), &'static str> {
        request.validate()?;
        let bot = self
            .config
            .bots
            .iter()
            .find(|b| b.bot_id == request.bot)
            .ok_or("unknown canonical Bot")?;
        if bot.name != request.manifest.bot_name
            || bot.total_shards
                != crate::placement::ShardCount::Fixed(request.manifest.total_shards)
        {
            return Err("manifest Bot metadata does not match daemon configuration");
        }
        if request.manifest.shards.iter().any(|s| {
            !self.local.contains(&ShardKey {
                bot_id: request.bot.clone(),
                shard_id: s.shard_id,
            })
        }) {
            return Err("update targets a nonlocal or unknown Shard");
        }
        Ok(())
    }

    pub fn admit(
        &mut self,
        request: UpdateControlRequest,
        access: ControlAccess,
        legacy_conflict: bool,
    ) -> Value {
        if request.operation_id.len() != 32
            || !request
                .operation_id
                .bytes()
                .all(|c| c.is_ascii_digit() || (b'a'..=b'f').contains(&c))
        {
            return failure("invalid update operation ID");
        }
        let fingerprint = match serde_json::to_vec(&request) {
            Ok(v) => v,
            Err(_) => return failure("invalid update payload"),
        };
        let targets: Vec<_> = request
            .manifest
            .shards
            .iter()
            .map(|s| ShardKey {
                bot_id: request.bot.clone(),
                shard_id: s.shard_id,
            })
            .collect();
        let allowed = access.allows_keys(&targets);
        if let Some(old) = self.entries.get(&request.operation_id) {
            return if allowed && old.subject == access.subject && old.fingerprint == fingerprint {
                old.response.clone()
            } else {
                failure("operation ID conflicts with an earlier request or scope denied")
            };
        }
        if self.entries.len() >= OPERATION_LIMIT {
            return failure("operation history is full");
        }
        let admission = self.validate_target(&request).and_then(|()| {
            if legacy_conflict || self.busy(&request.bot) {
                Err("Bot or operation ID is already busy")
            } else {
                Ok(())
            }
        });
        let response = operation(&request, "running", None);
        let entry = Entry {
            fingerprint,
            subject: access.subject.clone(),
            bot: request.bot.clone(),
            targets,
            started: Instant::now(),
            response: response.clone(),
        };
        let id = request.operation_id.clone();
        // No accepted ID may be removed/replayed, including audit failures.
        if self
            .jobs
            .try_send(AuditJob {
                request,
                access,
                allowed,
                admission,
            })
            .is_err()
        {
            return failure("update audit queue is full");
        }
        self.entries.insert(id, entry);
        response
    }

    pub fn drain(&mut self) -> Vec<Prepared> {
        let mut prepared = Vec::new();
        while let Ok(event) = self.events.try_recv() {
            match event {
                Event::Authorized(job) => {
                    let sender = self.sender.clone();
                    tokio::spawn(async move {
                        let request = job.request;
                        let checked = tokio::time::timeout(
                            Duration::from_secs(5),
                            tokio::task::spawn_blocking(move || {
                                let absolute = [
                                    (
                                        request.component != update_rollback::Component::Gateway,
                                        &request.manifest.artifacts.core,
                                    ),
                                    (
                                        request.component != update_rollback::Component::Core,
                                        &request.manifest.artifacts.gateway,
                                    ),
                                ]
                                .into_iter()
                                .filter(|(selected, _)| *selected)
                                .all(|(_, a)| a.as_ref().is_some_and(|a| a.path.is_absolute()));
                                if !absolute {
                                    return (
                                        request,
                                        Err("daemon artifacts require absolute paths"),
                                    );
                                }
                                let result = update_rollback::verify_artifacts(
                                    &request.manifest,
                                    request.component,
                                    Path::new("."),
                                );
                                (request, result)
                            }),
                        )
                        .await;
                        match checked {
                            Ok(Ok((request, Ok(())))) => {
                                let _ = sender
                                    .send(Event::Prepared(Prepared {
                                        request,
                                        completion: job.completion,
                                    }))
                                    .await;
                            }
                            Ok(Ok((request, Err(error)))) => {
                                let _ =
                                    job.completion
                                        .send(operation(&request, "failed", Some(error)));
                            }
                            _ => {
                                let _ = job.completion.send(failure(
                                    "artifact verifier failed or timed out; no candidate launched",
                                ));
                            }
                        }
                    });
                }
                Event::Prepared(job) => prepared.push(job),
                Event::Finished {
                    id,
                    response,
                    result,
                } => {
                    if let Some(entry) = self.entries.get_mut(&id) {
                        let has_final_result = response.as_ref().is_some_and(|v| {
                            v["operation"]["operation_id"] == id
                                && v["operation"]["state"] != "running"
                        });
                        let mut value = response
                            .filter(|v| v["operation"]["operation_id"] == id)
                            .unwrap_or_else(|| entry.response.clone());
                        if result.is_err() {
                            value["ok"] = json!(false);
                            value["operation"]["state"] = json!("failed");
                            if !has_final_result {
                                value["operation"]["failed_shards"] = json!(entry.targets);
                            }
                            if value["operation"]["error"].is_null() {
                                value["operation"]["error"] = json!(
                                    "update authorization, execution or audit failed; inspect audit for outcome"
                                );
                            }
                            value["error"] = value["operation"]["error"].clone();
                            if matches!(result, Err(ExecuteError::AuditAfterExecution(_))) {
                                value["operation"]["error"] = json!(
                                    "completion audit failed; execution outcome requires reconciliation"
                                );
                                value["error"] = value["operation"]["error"].clone();
                            }
                        }
                        value["operation"]["elapsed_ms"] =
                            json!(entry.started.elapsed().as_millis());
                        entry.response = value;
                    }
                }
            }
        }
        prepared
    }

    pub fn complete(&mut self, job: Prepared, response: Value) {
        if response["operation"]["state"] == "running" && response["ok"] == true {
            self.pending
                .insert(job.request.operation_id, job.completion);
        } else {
            let value = if response.get("operation").is_some() {
                response
            } else {
                operation(
                    &job.request,
                    "failed",
                    Some(
                        response["error"]
                            .as_str()
                            .unwrap_or("update backend rejected request"),
                    ),
                )
            };
            let _ = job.completion.send(value);
        }
    }

    pub fn poll_backend(&mut self, mut query: impl FnMut(&str) -> Value) {
        let done: Vec<_> = self
            .pending
            .keys()
            .filter_map(|id| {
                let value = query(id);
                (value["operation"]["state"] != "running").then(|| (id.clone(), value))
            })
            .collect();
        for (id, value) in done {
            if let Some(completion) = self.pending.remove(&id) {
                let _ = completion.send(value);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn request() -> UpdateControlRequest {
        serde_json::from_value(json!({"command":"update","bot":"101","operation_id":"0123456789abcdef0123456789abcdef",
            "yes":true,"component":"core","manifest":{"bot_id":"101","bot_name":"first","total_shards":1,
            "shards":[{"shard_id":0,"core":{"current":1,"candidate":2}}],
            "artifacts":{"core":{"path":std::env::current_exe().unwrap(),"sha256":"a".repeat(64)}}}})).unwrap()
    }

    #[test]
    fn completion_requires_actual_candidate_and_exact_affected_shards() {
        let request = request();
        let mut result = operation(&request, "succeeded", None);
        result["operation"]["final_generations"] = json!([{"shard_id":0,"core":2}]);
        assert!(successful_completion(&request, &result));
        for invalid in [
            json!([]),
            json!([{"shard_id":0,"core":1}]),
            json!([{"shard_id":1,"core":2}]),
            json!([{"shard_id":0,"core":2},{"shard_id":0,"core":2}]),
        ] {
            let mut changed = result.clone();
            changed["operation"]["final_generations"] = invalid;
            assert!(!successful_completion(&request, &changed));
        }
        result["operation"]["shards"][0]["key"]["bot_id"] = json!("202");
        assert!(!successful_completion(&request, &result));
    }

    #[tokio::test]
    async fn unavailable_audit_prevents_dispatch_and_keeps_queryable_failure_metadata() {
        let config:Config = toml::from_str("[cluster]\nid='test'\nmode='standalone'\n[node]\nid='one'\n[[bots]]\nname='first'\nbot_id='101'\napplication_id='101'\ntoken_env='TOKEN_ONE'\ntotal_shards=1\n[[bots.shard_placement]]\nshard_id=0\nprimary='one'\n").unwrap();
        let root = std::env::current_dir()
            .unwrap()
            .join("target")
            .join(format!(
                "update-audit-failure-{:016x}",
                getrandom::u64().unwrap()
            ));
        // A directory cannot be opened as the append-only audit file on either OS.
        let path = root.join("audit.jsonl");
        std::fs::create_dir_all(&path).unwrap();
        let request = request();
        let id = request.operation_id.clone();
        let access = ControlAccess::default();
        let mut hub = UpdateHub::new(
            config,
            vec![ShardKey {
                bot_id: "101".into(),
                shard_id: 0,
            }],
            path,
        );
        assert_eq!(
            hub.admit(request, access.clone(), false)["operation"]["state"],
            "running"
        );
        let result = tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                assert!(hub.drain().is_empty(), "audit failure dispatched execution");
                let result = hub.query(&id, &access);
                if result["operation"]["state"] == "failed" {
                    break result;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();
        assert_eq!(result["operation"]["operation_id"], id);
        assert_eq!(result["operation"]["shards"][0]["key"]["bot_id"], "101");
        assert_eq!(
            result["operation"]["failed_shards"]
                .as_array()
                .unwrap()
                .len(),
            1
        );
        assert_eq!(result["operation"]["final_generations"], json!([]));
        assert_eq!(hub.query(&id, &access), result);
        drop(hub);
        let resolved = std::fs::canonicalize(&root).unwrap();
        let target =
            std::fs::canonicalize(std::env::current_dir().unwrap().join("target")).unwrap();
        assert_eq!(resolved.parent(), Some(target.as_path()));
        assert!(
            resolved
                .file_name()
                .unwrap()
                .to_string_lossy()
                .starts_with("update-audit-failure-")
        );
        std::fs::remove_dir_all(resolved).unwrap();
    }
}
