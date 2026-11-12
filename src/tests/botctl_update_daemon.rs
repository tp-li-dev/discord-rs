//! Real botctl and botd processes, with explicit mock Discord and no autostart.
//! Exercises authenticated admission/failure/audit, never real Discord or deploys.
use serde_json::{Value, json};
use std::{
    future::Future,
    path::{Path, PathBuf},
    process::Stdio,
    sync::OnceLock,
    time::Duration,
};
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    net::{TcpListener, TcpStream},
    process::Child,
};

const SECRET: [u8; 32] = [67; 32];
const ID: &str = "123456789abcdef0123456789abcdef0";
const SENTINEL: &str = "UPDATE_PROCESS_ENV_MUST_NOT_BE_PERSISTED";
const CONFIG: &str = r#"
[cluster]
id = "update-tests"
mode = "standalone"
[node]
id = "one"
[[bots]]
name = "first"
bot_id = "101"
application_id = "101"
token_env = "TOKEN_ALPHA"
total_shards = 2
[[bots.shard_placement]]
shard_id = 0
primary = "one"
[[bots.shard_placement]]
shard_id = 1
primary = "one"
"#;

async fn bounded<T>(future: impl Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(20), future)
        .await
        .expect("process integration deadline")
}

fn artifact() -> Value {
    static ARTIFACT: OnceLock<Value> = OnceLock::new();
    ARTIFACT
        .get_or_init(|| {
            let program = Path::new(env!("CARGO_BIN_EXE_src"));
            let bytes = std::fs::read(program).unwrap();
            let digest = rustls::crypto::ring::cipher_suite::TLS13_AES_128_GCM_SHA256
                .tls13()
                .unwrap()
                .common
                .hash_provider
                .hash(&bytes);
            let hash: String = digest.as_ref().iter().map(|b| format!("{b:02x}")).collect();
            json!({"path":program,"sha256":hash})
        })
        .clone()
}

struct Fixture {
    root: PathBuf,
    child: Child,
    port: u16,
    _mock: TcpListener,
}

impl Fixture {
    async fn new(access: Option<Value>) -> Self {
        let mut nonce = [0; 16];
        getrandom::fill(&mut nonce).unwrap();
        let root = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("target")
            .join(format!("update-daemon-{:032x}", u128::from_ne_bytes(nonce)));
        std::fs::create_dir_all(&root).unwrap();
        let file = root.join("placement.toml");
        std::fs::write(&file, CONFIG).unwrap();
        let mock = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut child = tokio::process::Command::new(env!("CARGO_BIN_EXE_botd"))
            .arg("--file")
            .arg(file)
            .arg("--runtime-root")
            .arg(&root)
            .arg("--gateway-program")
            .arg(env!("CARGO_BIN_EXE_src"))
            .args(["--bootstrap-stdin", "--no-autostart", "--mock-discord"])
            .arg(mock.local_addr().unwrap().to_string())
            .env("DISCORD_TOKEN", SENTINEL)
            .env("TOKEN_ALPHA", SENTINEL)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .kill_on_drop(true)
            .spawn()
            .unwrap();
        let mut bootstrap = json!({"secret":SECRET});
        if let Some(access) = access {
            bootstrap["access"] = access;
        }
        bounded(
            child
                .stdin
                .take()
                .unwrap()
                .write_all(format!("{bootstrap}\n").as_bytes()),
        )
        .await
        .unwrap();
        let mut line = String::new();
        bounded(BufReader::new(child.stdout.take().unwrap()).read_line(&mut line))
            .await
            .unwrap();
        assert!(!line.contains(SENTINEL));
        let announcement: Value = serde_json::from_str(&line).expect("daemon startup announcement");
        assert_eq!(announcement["local_only"], true);
        assert_eq!(announcement["startup_ok"], true);
        assert_eq!(announcement["mock_discord"], true);
        assert!(announcement.get("secret").is_none());
        Self {
            root,
            child,
            port: announcement["port"].as_u64().unwrap() as u16,
            _mock: mock,
        }
    }

    fn manifest(&self) -> Value {
        json!({"bot_id":"101","bot_name":"first","total_shards":2,
            "shards":[{"shard_id":0,"core":{"current":1,"candidate":2}}],
            "artifacts":{"core":artifact()}})
    }

    fn request(&self, command: &str) -> Value {
        json!({"secret":SECRET,"command":command,"bot":"101","operation_id":ID,
            "yes":true,"component":"core","manifest":self.manifest()})
    }

    async fn cli(&self, command: &str, update: bool) -> Value {
        let mut program = tokio::process::Command::new(env!("CARGO_BIN_EXE_botctl"));
        program.arg(command);
        if update {
            let plan = self.root.join("plan.json");
            std::fs::write(&plan, self.manifest().to_string()).unwrap();
            program
                .arg("first")
                .arg("--plan")
                .arg(plan)
                .args(["--yes", "--operation-id", ID]);
        } else if command == "operation" {
            program.arg(ID);
        }
        program
            .args(["--bootstrap-stdin", "--json", "--timeout", "5s"])
            .env("DISCORD_TOKEN", SENTINEL)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);
        let mut child = program.spawn().unwrap();
        let bootstrap = json!({"port":self.port,"secret":SECRET});
        let _ = child
            .stdin
            .take()
            .unwrap()
            .write_all(format!("{bootstrap}\n").as_bytes())
            .await;
        let output = bounded(child.wait_with_output()).await.unwrap();
        assert!(output.stderr.is_empty());
        let text = String::from_utf8(output.stdout).unwrap();
        assert!(!text.contains(SENTINEL));
        assert!(!text.contains("\"secret\""));
        let value: Value = serde_json::from_str(&text).unwrap();
        assert_eq!(output.status.success(), value["ok"] == true);
        value
    }

    async fn raw(&self, value: &Value) -> Vec<u8> {
        bounded(async {
            let mut stream = TcpStream::connect(("127.0.0.1", self.port)).await.unwrap();
            stream
                .write_all(format!("{value}\n").as_bytes())
                .await
                .unwrap();
            let mut bytes = Vec::new();
            stream.take(65537).read_to_end(&mut bytes).await.unwrap();
            assert!(bytes.len() <= 65536);
            bytes
        })
        .await
    }

    async fn json(&self, value: &Value) -> Value {
        serde_json::from_slice(&self.raw(value).await).expect("authenticated JSON response")
    }

    async fn finished(&self) -> Value {
        bounded(async {
            loop {
                let value = self
                    .json(&json!({"secret":SECRET,"command":"operation","operation_id":ID}))
                    .await;
                if value["operation"]["state"] != "running" {
                    return value;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
    }

    fn records(&self) -> Vec<Value> {
        std::fs::read_to_string(self.root.join("update-audit.jsonl"))
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect()
    }

    async fn stop(&mut self) {
        if self.child.try_wait().unwrap().is_none() {
            self.child.kill().await.unwrap();
            self.child.wait().await.unwrap();
        }
    }

    async fn cleanup(mut self) {
        self.stop().await;
        for entry in std::fs::read_dir(&self.root).unwrap() {
            let entry = entry.unwrap();
            if entry.file_type().unwrap().is_file() {
                let text = std::fs::read_to_string(entry.path()).unwrap();
                assert!(!text.contains(SENTINEL));
                assert!(!text.contains("\"secret\""));
            }
        }
        let target = std::fs::canonicalize(&self.root).unwrap();
        let parent =
            std::fs::canonicalize(Path::new(env!("CARGO_MANIFEST_DIR")).join("target")).unwrap();
        assert_eq!(target.parent(), Some(parent.as_path()));
        assert!(
            target
                .file_name()
                .unwrap()
                .to_string_lossy()
                .starts_with("update-daemon-")
        );
        std::fs::remove_dir_all(target).unwrap();
    }
}

#[tokio::test]
async fn real_cli_update_and_rollback_report_failed_execution_with_correlated_audit() {
    for command in ["update", "rollback"] {
        let mut fixture = Fixture::new(None).await;
        let value = fixture.cli(command, true).await;
        assert_eq!(value["ok"], false);
        assert_eq!(value["offline"], false);
        assert_eq!(value["operation_id"], ID);
        let operation = &value["operation"];
        assert_eq!(operation["state"], "failed");
        assert_eq!(operation["command"], command);
        assert_eq!(operation["bot_id"], "101");
        assert!(operation["elapsed_ms"].is_number());
        assert_eq!(operation["failed_shards"].as_array().unwrap().len(), 1);
        assert!(
            operation["final_generations"]
                .as_array()
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            fixture.cli("operation", false).await["operation"],
            *operation
        );
        assert_eq!(fixture.cli("status", false).await["ok"], true);
        fixture.stop().await;
        let records = fixture.records();
        assert_eq!(records.len(), 2);
        assert_eq!(records[0]["result"], "started");
        assert_eq!(records[1]["result"], "failed");
        assert_eq!(records[0]["request_id"], records[1]["request_id"]);
        for record in records {
            assert_eq!(record["operation_id"], ID);
            assert_eq!(record["operation"], command);
            assert_eq!(record["subject"], "local-bootstrap");
            assert_eq!(record["role"], "admin");
        }
        fixture.cleanup().await;
    }
}

#[tokio::test]
async fn server_roles_and_shard_scopes_deny_without_started_records() {
    for (role, scope) in [
        ("viewer", None),
        ("updater", Some(json!([{"bot_id":"101","shard_id":1}]))),
    ] {
        let mut access = json!({"subject":"test-operator","role":role});
        if let Some(scope) = scope {
            access["scope"] = scope;
        }
        let mut fixture = Fixture::new(Some(access)).await;
        let value = fixture.json(&fixture.request("update")).await;
        assert_eq!(value["operation"]["state"], "running");
        bounded(async {
            loop {
                if let Ok(metadata) =
                    tokio::fs::metadata(fixture.root.join("update-audit.jsonl")).await
                    && metadata.len() > 0
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await;
        if role == "viewer" {
            assert_eq!(fixture.finished().await["ok"], false);
        } else {
            let query = fixture
                .json(&json!({"secret":SECRET,"command":"operation","operation_id":ID}))
                .await;
            assert_eq!(query["ok"], false);
            assert!(query.get("operation").is_none());
        }
        fixture.stop().await;
        let records = fixture.records();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0]["result"], "denied");
        assert_eq!(records[0]["operation_id"], ID);
        assert_eq!(records[0]["role"], role);
        assert_eq!(records[0]["subject"], "test-operator");
        fixture.cleanup().await;
    }
}

#[tokio::test]
async fn idempotent_ids_never_reexecute_and_changed_content_conflicts() {
    let mut fixture = Fixture::new(Some(json!({"subject":"test-updater","role":"updater"}))).await;
    let request = fixture.request("update");
    let admitted = fixture.json(&request).await;
    assert_eq!(admitted["operation"]["state"], "running");
    let completed = fixture.finished().await;
    assert_eq!(completed["operation"]["state"], "failed");
    let replay = fixture.json(&request).await;
    assert_eq!(replay, completed);
    let mut changed = request.clone();
    changed["manifest"]["shards"][0]["core"]["candidate"] = json!(3);
    let conflict = fixture.json(&changed).await;
    assert_eq!(conflict["ok"], false);
    assert!(conflict["error"].as_str().unwrap().contains("conflict"));
    assert_eq!(fixture.finished().await, completed);
    let legacy = fixture
        .json(&json!({"secret":SECRET,"command":"stop","bot":"first","operation_id":ID}))
        .await;
    assert_eq!(legacy["ok"], false);
    assert_eq!(fixture.finished().await, completed);
    fixture.stop().await;
    assert_eq!(fixture.records().len(), 2);
    fixture.cleanup().await;
}

#[tokio::test]
async fn update_id_cannot_shadow_an_earlier_lifecycle_operation() {
    let mut fixture = Fixture::new(None).await;
    let stopped = fixture
        .json(&json!({"secret":SECRET,"command":"stop","bot":"first","operation_id":ID}))
        .await;
    assert_eq!(stopped["ok"], true);
    let original = fixture.finished().await;
    assert_eq!(original["operation"]["command"], "stop");
    let conflict = fixture.json(&fixture.request("update")).await;
    assert_eq!(conflict["ok"], false);
    assert!(conflict["error"].as_str().unwrap().contains("conflict"));
    assert_eq!(fixture.finished().await, original);
    fixture.stop().await;
    assert!(!fixture.root.join("update-audit.jsonl").exists());
    fixture.cleanup().await;
}

#[tokio::test]
async fn canonical_bot_metadata_is_rechecked_by_the_daemon_before_execution() {
    let mut fixture = Fixture::new(None).await;
    let mut request = fixture.request("rollback");
    request["manifest"]["bot_name"] = json!("stale-name");
    let admitted = fixture.json(&request).await;
    assert_eq!(admitted["operation"]["state"], "running");
    let completed = fixture.finished().await;
    assert_eq!(completed["ok"], false);
    assert_eq!(completed["operation"]["state"], "failed");
    assert!(
        completed["operation"]["error"]
            .as_str()
            .unwrap()
            .contains("metadata")
    );
    fixture.stop().await;
    let records = fixture.records();
    assert_eq!(records.len(), 2);
    assert_eq!(records[0]["result"], "started");
    assert_eq!(records[1]["result"], "failed");
    fixture.cleanup().await;
}

#[tokio::test]
async fn unauthenticated_or_identity_injected_wire_requests_cannot_admit_updates() {
    let mut fixture = Fixture::new(None).await;
    for field in ["role", "subject", "authenticated"] {
        let mut request = fixture.request("update");
        request[field] = json!("admin");
        assert!(fixture.raw(&request).await.is_empty());
    }
    let mut request = fixture.request("update");
    request["secret"] = json!(vec![0; 32]);
    assert!(fixture.raw(&request).await.is_empty());
    let mut request = fixture.request("update");
    request["manifest"]["secret"] = json!(SECRET);
    assert!(fixture.raw(&request).await.is_empty());
    let status = fixture.cli("status", false).await;
    assert_eq!(status["ok"], true);
    fixture.stop().await;
    assert!(!fixture.root.join("update-audit.jsonl").exists() || fixture.records().is_empty());
    fixture.cleanup().await;
}
