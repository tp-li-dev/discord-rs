//! Production Worker processes and an on-disk StateStore, without Discord.
//! These tests do not prove multi-host durability, Discord ACK or lease fencing.
#[path = "support/interaction_fixture.rs"]
mod interaction_fixture;

use bot_framework::interactions::{
    InteractionError, InteractionKind, InteractionStateStore, Route,
};
use src::{
    protocol::BotId,
    state_store::{
        FileStateStore, FrameworkStateStore, PublicLabel, SchemaVersion, StateClock, StateId,
        StoreLimits, StoreScope, SystemClock,
    },
    worker::{ApiOutcome, DeploymentIdentity, InteractionStateConfig, Manager},
};
use std::{future::Future, path::PathBuf, sync::Arc, time::Duration};
use tokio::time::timeout;

// Synthetic fixture key, independent of Bot/Interaction credentials.
const KEY: [u8; 32] = [0x45; 32];
const JOURNAL_LIMIT: u64 = 4 * 1024 * 1024;

fn schema() -> SchemaVersion {
    SchemaVersion::new(1, 1).unwrap()
}
fn limits() -> StoreLimits {
    StoreLimits {
        max_entries: 128,
        max_bytes: 1024 * 1024,
        max_value_bytes: 16 * 1024,
    }
}
fn route(name: &str) -> Route {
    Route {
        namespace: "app".into(),
        name: name.into(),
        version: 1,
    }
}

async fn bounded<T>(future: impl Future<Output = T>) -> T {
    timeout(Duration::from_secs(55), future)
        .await
        .expect("worker interaction scenario deadline")
}

struct Scenario {
    root: PathBuf,
    worker: PathBuf,
    journal: PathBuf,
}
impl Scenario {
    async fn new(label: &str) -> Self {
        let fixture = interaction_fixture::binary().await;
        let root = interaction_fixture::unique_directory(label);
        let worker = root.join(format!("worker{}", std::env::consts::EXE_SUFFIX));
        src::testkit::copy_executable(fixture, &worker).unwrap();
        let journal = root.join("state.journal");
        Self {
            root,
            worker,
            journal,
        }
    }
    fn deployment(&self, bot: u64, shard: u32) -> DeploymentIdentity {
        DeploymentIdentity::parse(|name| {
            Ok(match name {
                "BOT_ID" => Some(bot.to_string()),
                "SHARD_ID" => Some(shard.to_string()),
                "TOTAL_SHARDS" => Some("2".into()),
                _ => None,
            })
        })
        .unwrap()
        .with_interaction_state(
            InteractionStateConfig::file(
                self.journal.clone(),
                PublicLabel::new("app").unwrap(),
                KEY,
                vec![1],
                schema(),
            )
            .with_limits(limits(), JOURNAL_LIMIT),
        )
    }
    async fn start(&self, bot: u64, shard: u32) -> Arc<Manager> {
        Manager::start_pool_with_identity(self.worker.clone(), 1, self.deployment(bot, shard))
            .await
            .unwrap()
    }
    async fn adapter(&self, bot: u64, shard: u32) -> FrameworkStateStore {
        let backend = FileStateStore::open(
            &self.journal,
            limits(),
            JOURNAL_LIMIT,
            Arc::new(SystemClock),
        )
        .await
        .unwrap();
        FrameworkStateStore::new(
            Arc::new(backend),
            StoreScope {
                bot_id: BotId(bot),
                shard_id: shard,
            },
            PublicLabel::new("app").unwrap(),
            KEY,
            vec![1],
            schema(),
        )
        .unwrap()
    }
    async fn seed(
        &self,
        bot: u64,
        shard: u32,
        kind: InteractionKind,
        name: &str,
        count: u64,
    ) -> String {
        let expires = SystemClock.now().unwrap().0 + 180_000;
        self.adapter(bot, shard)
            .await
            // Deliberately reuse the same synthetic key across Bot/Shard and
            // routes, so isolation cannot pass merely because IDs are random.
            .create_with_id(
                kind,
                &route(name),
                StateId::from_bytes([0x61; 16]),
                serde_json::to_vec(&count).unwrap(),
                expires,
            )
            .await
            .unwrap()
            .custom_id
    }
    async fn stored(&self, bot: u64, shard: u32, kind: InteractionKind, id: &str) -> u64 {
        let record = self.adapter(bot, shard).await.load(kind, id).await.unwrap();
        serde_json::from_slice(&record.data).unwrap()
    }
    async fn wait_started(&self, pid: u32) {
        let mut marker = self.worker.as_os_str().to_owned();
        marker.push(".waiting");
        bounded(async {
            loop {
                if std::fs::read_to_string(PathBuf::from(&marker))
                    .ok()
                    .as_deref()
                    == Some(&pid.to_string())
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await;
    }
    async fn cleanup(self) {
        // Only the exclusively created scenario directory is removed. Process
        // shutdown has already reaped each Worker, including on Windows.
        assert_eq!(self.worker.parent(), Some(self.root.as_path()));
        bounded(async {
            loop {
                match std::fs::remove_dir_all(&self.root) {
                    Ok(()) => break,
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => break,
                    Err(_) => tokio::time::sleep(Duration::from_millis(25)).await,
                }
            }
        })
        .await;
    }
}

async fn dispatch(
    manager: &Manager,
    kind: InteractionKind,
    id: &str,
    values: &[&str],
) -> Option<String> {
    manager
        .dispatch_interaction(
            0,
            kind,
            id.into(),
            values.iter().map(|value| (*value).into()).collect(),
        )
        .await
        .unwrap()
}

#[tokio::test]
async fn button_select_modal_survive_hot_swap_and_complete_manager_restart() {
    let scenario = Scenario::new("lifecycle").await;
    bounded(async {
        let component = InteractionKind::Component;
        let modal = InteractionKind::Modal;
        let button = scenario.seed(21, 0, component, "button", 0).await;
        let select = scenario.seed(21, 0, component, "select", 10).await;
        let open = scenario.seed(21, 0, component, "open", 20).await;
        let manager = scenario.start(21, 0).await;
        let original = manager.worker_identities().await[0].clone();
        assert_eq!(
            dispatch(&manager, component, &button, &[]).await.as_deref(),
            Some("button:1:")
        );
        assert_eq!(
            dispatch(&manager, component, &select, &["red", "blue"])
                .await
                .as_deref(),
            Some("select:11:red,blue")
        );
        let form = dispatch(&manager, component, &open, &[])
            .await
            .expect("created modal application output");
        assert_eq!(
            dispatch(&manager, modal, &form, &["before"])
                .await
                .as_deref(),
            Some("modal:21:before")
        );
        manager
            .replace(scenario.worker.clone(), true)
            .await
            .unwrap();
        let replacement = manager.worker_identities().await[0].clone();
        assert_ne!(original.pid, replacement.pid);
        assert_ne!(original.identity, replacement.identity);
        assert_eq!(
            dispatch(&manager, component, &button, &[]).await.as_deref(),
            Some("button:2:")
        );
        assert_eq!(
            dispatch(&manager, component, &select, &["green"])
                .await
                .as_deref(),
            Some("select:12:green")
        );
        assert_eq!(
            dispatch(&manager, modal, &form, &["after"])
                .await
                .as_deref(),
            Some("modal:22:after")
        );
        manager.shutdown().await.unwrap();
        drop(manager);
        // All adapters/file handles are independent; no parent MemoryStateStore
        // survives this restart to provide the state under test.
        let restarted = scenario.start(21, 0).await;
        assert_ne!(replacement.pid, restarted.worker_identities().await[0].pid);
        assert_eq!(
            dispatch(&restarted, component, &button, &[])
                .await
                .as_deref(),
            Some("button:3:")
        );
        assert_eq!(
            dispatch(&restarted, component, &select, &["black"])
                .await
                .as_deref(),
            Some("select:13:black")
        );
        assert_eq!(
            dispatch(&restarted, modal, &form, &["restart"])
                .await
                .as_deref(),
            Some("modal:23:restart")
        );
        assert_eq!(scenario.stored(21, 0, component, &button).await, 3);
        restarted.shutdown().await.unwrap();
    })
    .await;
    scenario.cleanup().await;
}

#[tokio::test]
async fn real_worker_rejects_other_bot_shard_kind_and_tampered_id() {
    let scenario = Scenario::new("isolation").await;
    bounded(async {
        let kind = InteractionKind::Component;
        let own = scenario.seed(21, 0, kind, "button", 0).await;
        let other_bot = scenario.seed(22, 0, kind, "button", 100).await;
        let other_shard = scenario.seed(21, 1, kind, "button", 200).await;
        let a = scenario.start(21, 0).await;
        let b = scenario.start(22, 0).await;
        let c = scenario.start(21, 1).await;
        for manager in [&b, &c] {
            assert_eq!(
                manager
                    .dispatch_interaction(0, kind, own.clone(), vec![])
                    .await
                    .unwrap()
                    .as_deref(),
                Some(InteractionError::InvalidId.response())
            );
        }
        assert_eq!(
            a.dispatch_interaction(0, InteractionKind::Modal, own.clone(), vec![])
                .await
                .unwrap()
                .as_deref(),
            Some(InteractionError::InvalidId.response())
        );
        let mut tampered = own.clone().into_bytes();
        // The final field is a 32-character lowercase hexadecimal MAC. Keep
        // the wire/application versions valid and change exactly one MAC nibble.
        let last = tampered.last_mut().unwrap();
        *last = if *last == b'a' { b'b' } else { b'a' };
        assert_eq!(
            a.dispatch_interaction(0, kind, String::from_utf8(tampered).unwrap(), vec![])
                .await
                .unwrap()
                .as_deref(),
            Some(InteractionError::InvalidId.response())
        );
        assert_eq!(
            dispatch(&a, kind, &own, &[]).await.as_deref(),
            Some("button:1:")
        );
        assert_eq!(
            dispatch(&b, kind, &other_bot, &[]).await.as_deref(),
            Some("button:101:")
        );
        assert_eq!(
            dispatch(&c, kind, &other_shard, &[]).await.as_deref(),
            Some("button:201:")
        );
        for manager in [a, b, c] {
            manager.shutdown().await.unwrap();
        }
    })
    .await;
    scenario.cleanup().await;
}

#[tokio::test]
async fn hot_swap_cancels_local_wait_and_keeps_old_api_receipt_on_old_generation() {
    let scenario = Scenario::new("wait-swap").await;
    bounded(async {
        let kind = InteractionKind::Component;
        let id = scenario.seed(21, 0, kind, "wait", 0).await;
        let manager = scenario.start(21, 0).await;
        let old = manager.worker_identities().await[0].clone();
        let mut response = manager.begin_response_delivery(0).await.unwrap();
        let wait = tokio::spawn({
            let manager = manager.clone();
            let id = id.clone();
            async move {
                let result = manager
                    .dispatch_interaction_with_response(&mut response, 0, kind, id, vec![])
                    .await;
                (result, response)
            }
        });
        scenario.wait_started(old.pid).await;
        let update = tokio::spawn({
            let manager = manager.clone();
            let path = scenario.worker.clone();
            async move { manager.replace(path, true).await }
        });
        let (result, response) = wait.await.unwrap();
        assert_eq!(result.unwrap().as_deref(), Some("wait-cancelled"));
        assert!(
            !update.is_finished(),
            "old response must remain tracked through drain"
        );
        let new = manager.worker_identities().await[0].clone();
        assert_ne!(old.pid, new.pid);
        assert_eq!(
            dispatch(&manager, kind, &id, &["ready"]).await.as_deref(),
            Some("wait-ready")
        );
        let receipt = manager
            .finish_response(response, ApiOutcome::Confirmed { status: 200 })
            .await
            .unwrap();
        assert_eq!(receipt.pid, old.pid);
        assert_eq!(Some(receipt.identity), old.identity);
        update.await.unwrap().unwrap();
        assert_eq!(scenario.stored(21, 0, kind, &id).await, 1);
        assert!(manager.unknown_responses().1.is_empty());
        manager.shutdown().await.unwrap();
    })
    .await;
    scenario.cleanup().await;
}

#[tokio::test]
async fn shutdown_cancels_pending_wait_and_waits_for_final_state_write() {
    let scenario = Scenario::new("wait-shutdown").await;
    bounded(async {
        let kind = InteractionKind::Component;
        let id = scenario.seed(21, 0, kind, "wait", 0).await;
        let manager = scenario.start(21, 0).await;
        let pid = manager.worker_identities().await[0].pid;
        let wait = tokio::spawn({
            let manager = manager.clone();
            let id = id.clone();
            async move { manager.dispatch_interaction(0, kind, id, vec![]).await }
        });
        scenario.wait_started(pid).await;
        let stop = tokio::spawn({
            let manager = manager.clone();
            async move { manager.shutdown().await }
        });
        assert_eq!(
            wait.await.unwrap().unwrap().as_deref(),
            Some("wait-cancelled")
        );
        assert_eq!(stop.await.unwrap().unwrap(), vec![pid]);
        assert_eq!(scenario.stored(21, 0, kind, &id).await, 1);
    })
    .await;
    scenario.cleanup().await;
}

#[tokio::test]
async fn crashed_worker_recovery_reopens_persisted_state_without_replaying_request() {
    let scenario = Scenario::new("crash-restart").await;
    bounded(async {
        let kind = InteractionKind::Component;
        let button = scenario.seed(21, 0, kind, "button", 0).await;
        let crash = scenario.seed(21, 0, kind, "crash", 0).await;
        let manager = scenario.start(21, 0).await;
        let before = manager.worker_identities().await[0].pid;
        assert_eq!(
            dispatch(&manager, kind, &button, &[]).await.as_deref(),
            Some("button:1:")
        );
        assert!(
            manager
                .dispatch_interaction(0, kind, crash, vec![])
                .await
                .is_err()
        );
        assert_ne!(before, manager.worker_identities().await[0].pid);
        assert_eq!(
            dispatch(&manager, kind, &button, &[]).await.as_deref(),
            Some("button:2:")
        );
        assert_eq!(scenario.stored(21, 0, kind, &button).await, 2);
        manager.shutdown().await.unwrap();
    })
    .await;
    scenario.cleanup().await;
}
