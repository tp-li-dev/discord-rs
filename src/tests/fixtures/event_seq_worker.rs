use std::io::{self, BufRead, Write};
fn main() {
    let mode = std::env::current_exe()
        .unwrap()
        .file_stem()
        .unwrap()
        .to_string_lossy()
        .into_owned();
    for line in io::stdin().lock().lines() {
        let line = line.unwrap();
        if line.contains("\"health\"") {
            let caps = if mode == "legacy-event" {
                r#"["ready-event-v1"]"#
            } else {
                r#"["hello-v1","ready-event-v1","event-seq-v1"]"#
            };
            println!(
                r#"{{"type":"ready","protocol":5,"build":"fixture","pid":1,"supported_capabilities":{caps}}}"#
            );
        } else if line.contains("\"type\":\"hello\"") {
            println!("{line}");
        } else {
            let sequenced = line.contains("\"type\":\"sequenced_ready\"");
            if mode == "legacy-event" {
                assert!(!sequenced);
                assert!(line.contains("\"ready_event\""));
            } else {
                assert!(sequenced);
            }
            if mode == "timeout-ack" {
                std::thread::sleep(std::time::Duration::from_secs(6));
            }
            if mode == "wrong-ack" {
                println!(r#"{{"type":"event_accepted","local_sequence":99}}"#);
            }
            println!(r#"{{"type":"result","content":null,"state":{{}}}}"#);
        }
        io::stdout().flush().unwrap();
    }
}
