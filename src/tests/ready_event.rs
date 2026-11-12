use std::process::Stdio;
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    process::Command,
};

#[tokio::test]
async fn worker_receives_ready_event_and_executes_linked_handler() {
    let mut child = Command::new(env!("CARGO_BIN_EXE_src"))
        .arg("--worker")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let mut stdin = child.stdin.take().unwrap();
    let mut stdout = BufReader::new(child.stdout.take().unwrap());
    let mut stderr = BufReader::new(child.stderr.take().unwrap());
    stdin.write_all(b"{\"type\":\"health\"}\n").await.unwrap();
    let mut response = String::new();
    stdout.read_line(&mut response).await.unwrap();
    let hello: serde_json::Value = serde_json::from_str(&response).unwrap();
    assert!(
        hello["supported_capabilities"]
            .as_array()
            .unwrap()
            .iter()
            .any(|v| v == "ready-event-v1")
    );
    stdin
        .write_all(b"{\"type\":\"ready_event\",\"bot_name\":\"fixture\"}\n")
        .await
        .unwrap();
    response.clear();
    stdout.read_line(&mut response).await.unwrap();
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&response).unwrap()["type"],
        "result"
    );
    let mut log = String::new();
    stderr.read_line(&mut log).await.unwrap();
    assert_eq!(log.trim(), "Ready: fixture");
}

#[tokio::test]
async fn manager_delivers_event_to_a_live_worker() {
    let manager = src::worker::Manager::start_pool(env!("CARGO_BIN_EXE_src").into(), 2)
        .await
        .unwrap();
    manager.dispatch_ready("fixture".into()).await.unwrap();
    assert_eq!(
        manager.dispatch("!ping".into()).await.unwrap().as_deref(),
        Some("Pong! 🦀")
    );
}
