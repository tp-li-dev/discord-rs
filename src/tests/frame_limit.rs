use serde_json::{Value, json};
use std::process::Stdio;
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    process::Command,
};

#[tokio::test]
async fn oversized_state_change_is_rejected_without_losing_prior_state() {
    let mut child = Command::new(env!("CARGO_BIN_EXE_src"))
        .arg("--worker")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let mut input = child.stdin.take().unwrap();
    let mut output = BufReader::new(child.stdout.take().unwrap());
    let mut counts = serde_json::Map::new();
    // Build a state just below the limit, including the envelopes needed to move it.
    for key in 0..100_000_u64 {
        counts.insert(key.to_string(), json!(1));
    }
    let mut size = serde_json::to_vec(
        &json!({"type":"restore", "version":3, "state":{"counts":counts,"jobs":{}}}),
    )
    .unwrap()
    .len();
    for key in 100_000..130_000_u64 {
        counts.insert(key.to_string(), json!(1));
        size += key.to_string().len() + 5;
        if size > 1_048_500 {
            break;
        }
    }
    // Move close enough to the limit that a large reply is rejected without altering state.
    let state = json!({"counts": counts, "jobs": {}});
    async fn send(
        input: &mut tokio::process::ChildStdin,
        output: &mut BufReader<tokio::process::ChildStdout>,
        v: Value,
    ) -> Value {
        let mut data = serde_json::to_vec(&v).unwrap();
        data.push(b'\n');
        input.write_all(&data).await.unwrap();
        let mut line = String::new();
        output.read_line(&mut line).await.unwrap();
        serde_json::from_str(&line).unwrap()
    }
    let restored = send(
        &mut input,
        &mut output,
        json!({"type":"restore","version":3,"state":state}),
    )
    .await;
    assert_eq!(restored["type"], "restored");
    // A near-limit state still supports ordinary updates and snapshots.
    let before = send(&mut input, &mut output, json!({"type":"snapshot"})).await;
    assert_eq!(before["type"], "snapshot");
    let mut expected = before["state"].clone();
    let mut rejected = false;
    for key in 0..100_u64 {
        let result = send(
            &mut input,
            &mut output,
            json!({"type":"dispatch","key":18446744073709551500_u64 + key,"content":"!count"}),
        )
        .await;
        if result["type"] == "rejected" {
            rejected = true;
            break;
        }
        assert_eq!(result["type"], "result");
        expected = result["state"].clone();
    }
    assert!(rejected, "expected to reach the frame limit");
    let snapshot = send(&mut input, &mut output, json!({"type":"snapshot"})).await;
    assert_eq!(snapshot["state"], expected);
    let health = send(&mut input, &mut output, json!({"type":"health"})).await;
    assert_eq!(health["type"], "ready");
}
