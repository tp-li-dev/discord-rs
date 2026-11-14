use serde_json::{Value, json};
use std::process::Stdio;
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    process::Command,
};

#[tokio::test]
async fn negotiated_ready_ack_precedes_result_and_sequence_is_checked() {
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        let identity = json!({"bot_id":7,"shard":{"id":0,"total":1},"generation_id":9,"ownership_epoch":0});
        let mut child = Command::new(env!("CARGO_BIN_EXE_src")).arg("--worker")
            .env("BOT_WORKER_IPC_IDENTITY",identity.to_string()).env_remove("DISCORD_TOKEN")
            .stdin(Stdio::piped()).stdout(Stdio::piped()).stderr(Stdio::null()).kill_on_drop(true).spawn().unwrap();
        let mut input = child.stdin.take().unwrap();
        let mut output = BufReader::new(child.stdout.take().unwrap());
        let hello = json!({"type":"hello","hello":{"protocol_min":5,"protocol_max":5,"identity":identity,
            "required_capabilities":["snapshot-v3"],"optional_capabilities":["hello-v1","ready-event-v1","event-seq-v1"]}});
        input.write_all(format!("{hello}\n").as_bytes()).await.unwrap();
        let mut line = String::new(); output.read_line(&mut line).await.unwrap();
        assert_eq!(serde_json::from_str::<Value>(&line).unwrap()["type"],"hello");
        for sequence in 0..2 {
            let event = json!({"type":"sequenced_ready","local_sequence":sequence,"bot_name":"fixture"});
            input.write_all(format!("{event}\n").as_bytes()).await.unwrap();
            line.clear(); output.read_line(&mut line).await.unwrap();
            assert_eq!(serde_json::from_str::<Value>(&line).unwrap(),json!({"type":"event_accepted","local_sequence":sequence}));
            line.clear(); output.read_line(&mut line).await.unwrap();
            assert_eq!(serde_json::from_str::<Value>(&line).unwrap()["type"],"result");
        }
        input.write_all(b"{\"type\":\"sequenced_ready\",\"local_sequence\":3,\"bot_name\":\"fixture\"}\n").await.unwrap();
        line.clear(); assert_eq!(output.read_line(&mut line).await.unwrap(),0);
        assert!(!child.wait().await.unwrap().success());
    }).await.unwrap();
}

#[tokio::test]
async fn parent_preserves_legacy_format_and_rejects_missing_or_wrong_ack() {
    let directory = std::env::temp_dir().join(format!("event-seq-ipc-{}", std::process::id()));
    std::fs::create_dir_all(&directory).unwrap();
    let fixture = directory.join(format!("fixture{}", std::env::consts::EXE_SUFFIX));
    assert!(
        std::process::Command::new("rustc")
            .arg("--edition=2024")
            .arg(
                std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                    .join("tests/fixtures/event_seq_worker.rs")
            )
            .arg("-o")
            .arg(&fixture)
            .status()
            .unwrap()
            .success()
    );
    for mode in ["legacy-event", "missing-ack", "wrong-ack", "timeout-ack"] {
        let path = directory.join(format!("{mode}{}", std::env::consts::EXE_SUFFIX));
        src::testkit::copy_executable(&fixture, &path).unwrap();
        let manager = src::worker::Manager::start_pool(path, 1).await.unwrap();
        let result = manager.dispatch_ready("fixture".into()).await;
        if mode == "legacy-event" {
            result.unwrap();
        } else if mode == "timeout-ack" {
            // ACK admission keeps the control deadline even though handler
            // completion now has a separate, longer application deadline.
            assert_eq!(
                result.unwrap_err().to_string(),
                "worker event ACK timed out"
            );
        } else {
            assert_eq!(
                result.unwrap_err().to_string(),
                "missing or invalid worker event ACK"
            );
        }
    }
}

#[tokio::test]
async fn oversized_ready_is_explicitly_rejected() {
    let manager = src::worker::Manager::start_pool(env!("CARGO_BIN_EXE_src").into(), 1)
        .await
        .unwrap();
    let error = manager
        .dispatch_ready("x".repeat(1_048_576))
        .await
        .unwrap_err();
    assert_eq!(
        error.to_string(),
        "worker event buffer full or event too large"
    );
}
