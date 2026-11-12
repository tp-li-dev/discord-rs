//! Synthetic IPC peer for result ACK faults. Never records request or env values.
use std::io::{self, BufRead, Write};

fn main() {
    let mode = std::env::current_exe()
        .unwrap()
        .file_stem()
        .unwrap()
        .to_string_lossy()
        .into_owned();
    let identity = std::env::var("BOT_WORKER_IPC_IDENTITY").unwrap();
    let pid = std::process::id();
    for line in io::stdin().lock().lines() {
        let line = line.unwrap();
        let response = if line.contains("\"type\":\"health\"") {
            let supported = if mode == "unadvertised" {
                "\"hello-v1\""
            } else {
                "\"hello-v1\",\"api-result-v1\""
            };
            let ready_pid = if mode == "bad-ready-pid" {
                pid.wrapping_add(1)
            } else {
                pid
            };
            format!(
                "{{\"type\":\"ready\",\"protocol\":5,\"supported_capabilities\":[{supported}],\"build\":\"fixture\",\"pid\":{ready_pid},\"credential_free\":true}}"
            )
        } else if line.contains("\"type\":\"hello\"") {
            // Reflect all optional capabilities to exercise Ready-advertisement
            // gating. Reflection alone must never enable a new wire contract.
            line
        } else if line.contains("\"type\":\"response_dispatch\"") {
            "{\"type\":\"result\",\"content\":\"fixture reply\",\"state\":{}}".to_owned()
        } else if line.contains("\"type\":\"api_result\"") {
            if mode == "missing-ack" {
                continue;
            }
            let number = line
                .split("\"operation_id\":")
                .nth(1)
                .unwrap()
                .chars()
                .take_while(char::is_ascii_digit)
                .collect::<String>();
            let operation: u64 = number.parse().unwrap();
            let operation = if mode == "bad-operation" {
                operation.wrapping_add(1)
            } else {
                operation
            };
            let reported_identity = if mode == "bad-identity" {
                identity.replace("\"bot_id\":101", "\"bot_id\":102")
            } else {
                identity.clone()
            };
            let receipt_pid = if mode == "bad-result-pid" {
                pid.wrapping_add(1)
            } else {
                pid
            };
            let status = if mode == "bad-outcome" { 201 } else { 200 };
            format!(
                "{{\"type\":\"api_result_accepted\",\"operation_id\":{operation},\"outcome\":{{\"kind\":\"confirmed\",\"status\":{status}}},\"identity\":{reported_identity},\"pid\":{receipt_pid}}}"
            )
        } else {
            std::process::exit(90)
        };
        println!("{response}");
        io::stdout().flush().unwrap();
    }
}
