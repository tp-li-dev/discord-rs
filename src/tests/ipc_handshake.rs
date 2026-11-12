use src::protocol::{BotId, GenerationId, Hello, Identity, OwnershipEpoch, Shard};
use std::io::{BufRead, BufReader, Write};
use std::process::{Command, Stdio};

fn hello() -> Hello {
    Hello {
        protocol_min: 5,
        protocol_max: 5,
        identity: Identity {
            bot_id: BotId(42),
            shard: Shard::new(1, 2).unwrap(),
            generation_id: GenerationId(7),
            ownership_epoch: OwnershipEpoch(3),
        },
        required_capabilities: vec!["snapshot-v3".into()],
        optional_capabilities: vec!["hello-v1".into(), "future-optional".into()],
    }
}

fn exchange(peer: Hello) -> serde_json::Value {
    let mut child = Command::new(env!("CARGO_BIN_EXE_src"))
        .arg("--worker")
        .env_remove("DISCORD_TOKEN")
        .env(
            "BOT_WORKER_IPC_IDENTITY",
            serde_json::to_string(&hello().identity).unwrap(),
        )
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();
    let mut input = child.stdin.take().unwrap();
    writeln!(
        input,
        "{}",
        serde_json::json!({"type":"hello", "hello":peer})
    )
    .unwrap();
    input.flush().unwrap();
    let mut line = String::new();
    BufReader::new(child.stdout.take().unwrap())
        .read_line(&mut line)
        .unwrap();
    child.kill().ok();
    child.wait().unwrap();
    serde_json::from_str(&line).unwrap()
}

#[test]
fn t19_real_worker_negotiates_and_rejects_incompatible_hello() {
    let local = hello();
    let response = exchange(local.clone());
    assert_eq!(response["type"], "hello");
    let peer: Hello = serde_json::from_value(response["hello"].clone()).unwrap();
    let (version, capabilities) = local.negotiate(&peer).unwrap();
    assert_eq!(version, 5);
    assert_eq!(capabilities, vec!["hello-v1", "snapshot-v3"]);
    for field in 0..6 {
        let mut invalid = local.clone();
        match field {
            0 => invalid.identity.bot_id = BotId(99),
            1 => invalid.identity.shard = Shard::new(0, 2).unwrap(),
            2 => invalid.identity.generation_id = GenerationId(8),
            3 => invalid
                .required_capabilities
                .push("unknown-required".into()),
            4 => {
                invalid.protocol_min = 6;
                invalid.protocol_max = 7;
            }
            _ => {
                invalid.protocol_min = 6;
                invalid.protocol_max = 4;
            }
        }
        assert_eq!(exchange(invalid)["type"], "rejected");
    }
}

#[test]
fn negotiation_is_symmetric_and_selects_highest_common_version() {
    let mut a = hello();
    a.protocol_min = 3;
    a.protocol_max = 7;
    let mut b = a.clone();
    b.protocol_min = 4;
    b.protocol_max = 6;
    b.optional_capabilities = vec!["hello-v1".into()];
    assert_eq!(a.negotiate(&b), b.negotiate(&a));
    assert_eq!(a.negotiate(&b).unwrap().0, 6);
    b.required_capabilities.push("unsupported".into());
    assert!(a.negotiate(&b).is_err());
    assert!(b.negotiate(&a).is_err());
}

#[tokio::test]
async fn t19_rejected_candidates_keep_old_worker_and_legacy_v5_starts() {
    let directory = std::env::temp_dir().join(format!("ipc-handshake-{}", std::process::id()));
    std::fs::create_dir_all(&directory).unwrap();
    let fixture = directory.join(format!("fixture{}", std::env::consts::EXE_SUFFIX));
    assert!(
        Command::new("rustc")
            .arg("--edition=2024")
            .arg(
                std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                    .join("tests/fixtures/handshake_worker.rs")
            )
            .arg("-o")
            .arg(&fixture)
            .status()
            .unwrap()
            .success()
    );
    let manager = src::worker::Manager::start_pool(env!("CARGO_BIN_EXE_src").into(), 1)
        .await
        .unwrap();
    let before = manager.describe_workers().await;
    for name in ["bad-version", "bad-capability", "bad-identity"] {
        let candidate = directory.join(format!("{name}{}", std::env::consts::EXE_SUFFIX));
        std::fs::copy(&fixture, &candidate).unwrap();
        assert!(manager.replace(candidate, false).await.is_err());
        assert_eq!(manager.describe_workers().await, before);
        assert!(
            manager
                .dispatch("!ping".into())
                .await
                .unwrap()
                .unwrap()
                .starts_with("Pong!")
        );
    }
    let legacy = directory.join(format!("legacy{}", std::env::consts::EXE_SUFFIX));
    std::fs::copy(&fixture, &legacy).unwrap();
    let old = src::worker::Manager::start_pool(legacy, 1).await.unwrap();
    assert!(old.describe_workers().await.contains("fixture"));
    // kill_on_drop teardown is asynchronous on Windows; keep fixture cleanup out
    // of assertions about protocol behavior.
    drop(old);
    drop(manager);
}
