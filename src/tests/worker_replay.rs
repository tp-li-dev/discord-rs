use serde_json::{Value, json};
use src::worker::Manager;
use std::{path::PathBuf, process::Stdio, sync::OnceLock, time::Duration};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    process::Command,
};

fn fixture(mode: &str) -> PathBuf {
    static DIRECTORY: OnceLock<PathBuf> = OnceLock::new();
    let dir = DIRECTORY.get_or_init(|| {
        let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("target")
            .join(format!("worker-replay-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let base = dir.join(format!("fixture{}", std::env::consts::EXE_SUFFIX));
        assert!(
            std::process::Command::new("rustc")
                .arg("--edition=2024")
                .arg(
                    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                        .join("tests/fixtures/replay_worker.rs")
                )
                .arg("-o")
                .arg(&base)
                .status()
                .unwrap()
                .success()
        );
        for mode in [
            "failing",
            "no-replay",
            "acked-fail",
            "reject-state",
            "reject-command",
            "slow-ack",
        ] {
            src::testkit::copy_executable(
                &base,
                dir.join(format!("{mode}{}", std::env::consts::EXE_SUFFIX)),
            )
            .unwrap();
        }
        dir
    });
    dir.join(format!("{mode}{}", std::env::consts::EXE_SUFFIX))
}

#[tokio::test]
async fn retained_event_survives_failed_replay_and_capability_downgrade() {
    tokio::time::timeout(Duration::from_secs(20), async {
        let manager = Manager::start_pool(fixture("failing"), 1).await.unwrap();
        manager.dispatch_ready("first".into()).await.unwrap(); // sequence 0 ACKed
        assert_eq!(
            manager
                .dispatch_ready("retry".into())
                .await
                .unwrap_err()
                .to_string(),
            "missing or invalid worker event ACK"
        );
        // Automatic replay to the faulty fixture failed. Neither that attempt nor
        // a candidate lacking the negotiated capability may discard the buffer.
        let before = manager.describe_workers().await;
        assert!(
            manager
                .replace(fixture("no-replay"), true)
                .await
                .unwrap_err()
                .to_string()
                .contains("negotiated replay")
        );
        assert_eq!(before, manager.describe_workers().await);
        manager
            .replace(env!("CARGO_BIN_EXE_src").into(), true)
            .await
            .unwrap();
        // Candidate accepted replay 1, so the next event must be 2, never 0.
        manager.dispatch_ready("next".into()).await.unwrap();
        assert!(
            manager
                .dispatch_key(19, "!count".into())
                .await
                .unwrap()
                .unwrap()
                .starts_with("カウント: 5")
        );
        // A second healthy switch must preserve the advanced cursor too.
        manager
            .replace(env!("CARGO_BIN_EXE_src").into(), true)
            .await
            .unwrap();
        manager.dispatch_ready("after-switch".into()).await.unwrap();
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn old_worker_must_also_negotiate_replay() {
    let manager = Manager::start_pool(fixture("no-replay"), 1).await.unwrap();
    manager.dispatch_ready("first".into()).await.unwrap();
    assert!(manager.dispatch_ready("retry".into()).await.is_err());
    assert!(
        manager
            .replace(env!("CARGO_BIN_EXE_src").into(), true)
            .await
            .unwrap_err()
            .to_string()
            .contains("negotiated replay")
    );
}

#[tokio::test]
async fn cancelled_exchange_is_poisoned_and_retained_event_moves_to_new_worker() {
    tokio::time::timeout(Duration::from_secs(10), async {
        let path = fixture("slow-ack");
        let marker = path.with_extension("started");
        let _ = std::fs::remove_file(&marker);
        let manager = Manager::start_pool(path, 1).await.unwrap();
        manager.dispatch_ready("first".into()).await.unwrap();
        let dispatch = tokio::spawn({
            let manager = manager.clone();
            async move { manager.dispatch_ready("cancelled".into()).await }
        });
        // Wait for evidence the child has read sequence 1, then cancel while
        // its ACK is pending. Cancellation must not leave stdout reusable.
        while !marker.exists() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        dispatch.abort();
        assert!(dispatch.await.unwrap_err().is_cancelled());
        manager
            .replace(env!("CARGO_BIN_EXE_src").into(), true)
            .await
            .unwrap();
        manager.dispatch_ready("after-cancel".into()).await.unwrap();
        assert!(
            manager
                .dispatch_key(19, "!count".into())
                .await
                .unwrap()
                .unwrap()
                .starts_with("カウント: 5")
        );
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn acked_events_are_not_replayed_after_result_loss() {
    let manager = Manager::start_pool(fixture("acked-fail"), 1).await.unwrap();
    assert_eq!(
        manager
            .dispatch_ready("accepted".into())
            .await
            .unwrap_err()
            .to_string(),
        "worker exited after ACK"
    );
    // Fixture exits whenever sequence 0 is delivered: success proves no retry of
    // accepted 0 and continuing sequence 1 on the replacement connection.
    manager.dispatch_ready("next".into()).await.unwrap();
}

async fn exchange(
    input: &mut tokio::process::ChildStdin,
    output: &mut BufReader<tokio::process::ChildStdout>,
    request: Value,
) -> Value {
    input
        .write_all(format!("{request}\n").as_bytes())
        .await
        .unwrap();
    let mut line = String::new();
    assert_ne!(output.read_line(&mut line).await.unwrap(), 0);
    serde_json::from_str(&line).unwrap()
}

#[tokio::test]
async fn receiver_rejects_replayed_duplicate_and_cursor_rewind() {
    tokio::time::timeout(Duration::from_secs(10), async {
        let identity = json!({"bot_id":7,"shard":{"id":0,"total":1},"generation_id":9,"ownership_epoch":0});
        let mut child = Command::new(env!("CARGO_BIN_EXE_src")).arg("--worker").env("BOT_WORKER_IPC_IDENTITY", identity.to_string()).env_remove("DISCORD_TOKEN").stdin(Stdio::piped()).stdout(Stdio::piped()).stderr(Stdio::null()).kill_on_drop(true).spawn().unwrap();
        let mut input = child.stdin.take().unwrap();
        let mut output = BufReader::new(child.stdout.take().unwrap());
        let hello = json!({"type":"hello","hello":{"protocol_min":5,"protocol_max":5,"identity":identity,"required_capabilities":["snapshot-v3"],"optional_capabilities":["hello-v1","event-seq-v1","event-replay-v1"]}});
        assert_eq!(exchange(&mut input, &mut output, hello).await["type"], "hello");
        assert_eq!(exchange(&mut input, &mut output, json!({"type":"restore_event_stream","next_sequence":7})).await["type"], "event_stream_restored");
        assert_eq!(exchange(&mut input, &mut output, json!({"type":"restore_event_stream","next_sequence":0})).await["type"], "rejected");
        let event = json!({"type":"sequenced_ready","local_sequence":7,"bot_name":"fixture"});
        assert_eq!(exchange(&mut input, &mut output, event.clone()).await, json!({"type":"event_accepted","local_sequence":7}));
        let mut line = String::new();
        output.read_line(&mut line).await.unwrap();
        assert_eq!(serde_json::from_str::<Value>(&line).unwrap()["type"], "result");
        input.write_all(format!("{event}\n").as_bytes()).await.unwrap();
        line.clear();
        assert_eq!(output.read_line(&mut line).await.unwrap(), 0);
        assert!(!child.wait().await.unwrap().success());
    }).await.unwrap();
}

#[tokio::test]
async fn unnegotiated_cursor_restore_and_invalid_state_are_safely_rejected() {
    tokio::time::timeout(Duration::from_secs(10), async {
        let mut child = Command::new(env!("CARGO_BIN_EXE_src"))
            .arg("--worker").env_remove("DISCORD_TOKEN")
            .stdin(Stdio::piped()).stdout(Stdio::piped()).stderr(Stdio::null())
            .kill_on_drop(true).spawn().unwrap();
        let mut input = child.stdin.take().unwrap();
        let mut output = BufReader::new(child.stdout.take().unwrap());
        assert_eq!(exchange(&mut input, &mut output, json!({"type":"restore_event_stream","next_sequence":7})).await["type"], "rejected");
        let invalid = json!({"type":"restore","version":3,"state":{"counts":{"0":"tiny-secret token=fake-value"},"jobs":{}}});
        assert_eq!(exchange(&mut input, &mut output, invalid).await, json!({"type":"rejected","reason":"invalid worker state"}));
        assert_eq!(exchange(&mut input, &mut output, json!({"type":"snapshot"})).await["state"], json!({"counts":{},"jobs":{}}));
        // Snapshot drain must reopen the old process for failed/cancelled updates.
        assert_eq!(exchange(&mut input, &mut output, json!({"type":"command","name":"ping"})).await["content"], "Pong! 🦀");
        drop(input);
        assert!(child.wait().await.unwrap().success());
    }).await.unwrap();
}

#[test]
fn handoff_copy_keeps_old_payload_and_exhausted_cursor_never_wraps() {
    use src::event_seq::{EventAccepted, Priority, ReceiveError, Receiver, Sender, SequenceRange};
    let mut previous = Sender::new(2, 0, 64, u64::MAX).unwrap();
    previous
        .enqueue(b"event".to_vec(), Priority::Required)
        .ok()
        .unwrap();
    let mut candidate = previous.clone();
    candidate
        .ack(EventAccepted(SequenceRange {
            first: u64::MAX,
            last: u64::MAX,
        }))
        .unwrap();
    assert_eq!(candidate.receiver_checkpoint(), None);
    assert_eq!(previous.receiver_checkpoint(), Some(u64::MAX));
    assert_eq!(previous.replay().next().unwrap().payload, b"event");
    assert_eq!(
        Receiver::from_checkpoint(candidate.receiver_checkpoint()).accept(0, true),
        Err(ReceiveError::SequenceExhausted)
    );
    let mut receiver = Receiver::from_checkpoint(previous.receiver_checkpoint());
    receiver.accept(u64::MAX, true).unwrap();
    assert_eq!(
        receiver.accept(u64::MAX, true),
        Err(ReceiveError::SequenceExhausted)
    );
}

#[tokio::test]
async fn rejected_worker_text_is_hidden_at_manager_boundaries() {
    let manager = Manager::start_pool(fixture("reject-command"), 1)
        .await
        .unwrap();
    let error = manager
        .dispatch("!ping".into())
        .await
        .unwrap_err()
        .to_string();
    assert!(error.contains("[REDACTED]"));
    assert!(!error.contains("tiny-secret"));
    assert!(!error.contains("fake-reject"));
    let manager = Manager::start_pool(env!("CARGO_BIN_EXE_src").into(), 1)
        .await
        .unwrap();
    let error = manager
        .replace(fixture("reject-state"), true)
        .await
        .unwrap_err()
        .to_string();
    assert!(error.contains("[REDACTED]"));
    assert!(!error.contains("tiny-secret"));
    assert!(!error.contains("fake-reject"));
    assert_eq!(
        manager.dispatch("!ping".into()).await.unwrap().as_deref(),
        Some("Pong! 🦀")
    );
}

#[tokio::test]
async fn watch_persists_only_protected_update_failure_text() {
    tokio::time::timeout(Duration::from_secs(10), async {
        let path = fixture("reject-state");
        let file = path.with_extension("requested");
        let error_file = src::worker::error_file(&file);
        let _ = std::fs::remove_file(&error_file);
        std::fs::write(&file, path.to_string_lossy().as_bytes()).unwrap();
        let manager = Manager::start_pool(env!("CARGO_BIN_EXE_src").into(), 1)
            .await
            .unwrap();
        let watcher = tokio::spawn(manager.clone().watch(file.clone()));
        // Abort-on-drop also covers a failed assertion in this test.
        struct Watcher(tokio::task::JoinHandle<()>);
        impl Drop for Watcher {
            fn drop(&mut self) {
                self.0.abort();
            }
        }
        let _watcher = Watcher(watcher);
        let detail = loop {
            if let Ok(bytes) = tokio::fs::read(&error_file).await
                && let Ok(value) = serde_json::from_slice::<Value>(&bytes)
            {
                break value;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        };
        assert_eq!(detail["path"].as_str(), path.to_str()); // exact control field
        let error = detail["error"].as_str().unwrap();
        assert!(error.contains("[REDACTED]"));
        assert!(!error.contains("tiny-secret"));
        assert!(!error.contains("fake-reject"));
        assert_eq!(
            manager.dispatch("!ping".into()).await.unwrap().as_deref(),
            Some("Pong! 🦀")
        );
    })
    .await
    .unwrap();
}
