// Fault-injection IPC fixture. All data is synthetic; rustc needs only std.
use std::io::{self, BufRead, Write};

fn sequence(line: &str) -> u64 {
    let rest = line.split("\"local_sequence\":").nth(1).unwrap();
    rest.split(|c: char| !c.is_ascii_digit())
        .next()
        .unwrap()
        .parse()
        .unwrap()
}

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
            println!(
                r#"{{"type":"ready","protocol":5,"build":"fixture","pid":1,"supported_capabilities":["hello-v1","event-seq-v1","event-replay-v1","ready-event-v1"]}}"#
            );
        } else if line.contains("\"type\":\"hello\"") {
            if mode == "no-replay" {
                println!(
                    "{}",
                    line.replace("\"event-replay-v1\",", "")
                        .replace(",\"event-replay-v1\"", "")
                );
            } else {
                println!("{line}");
            }
        } else if line.contains("\"restore_event_stream\"") {
            println!(r#"{{"type":"event_stream_restored"}}"#);
        } else if line.contains("\"sequenced_ready\"") {
            let seq = sequence(&line);
            if mode == "slow-ack" && seq == 1 {
                std::fs::write(
                    std::env::current_exe().unwrap().with_extension("started"),
                    b"started",
                )
                .unwrap();
                std::thread::sleep(std::time::Duration::from_secs(3));
            }
            if mode == "acked-fail" && seq == 0 {
                println!(r#"{{"type":"event_accepted","local_sequence":0}}"#);
                io::stdout().flush().unwrap();
                return;
            }
            if seq != 0 && mode != "acked-fail" {
                // Do not process this event. A bad ACK breaks the parent's pipe.
                println!(r#"{{"type":"event_accepted","local_sequence":999}}"#);
                io::stdout().flush().unwrap();
                return;
            }
            println!(r#"{{"type":"event_accepted","local_sequence":{seq}}}"#);
            println!(
                r#"{{"type":"result","content":null,"state":{{"counts":{{"19":4}},"jobs":{{}}}}}}"#
            );
        } else if line.contains("\"restore\"") {
            if mode == "reject-state" {
                println!(r#"{{"type":"rejected","reason":"tiny-secret token=fake-reject"}}"#);
            } else {
                println!(r#"{{"type":"restored"}}"#);
            }
        } else if line.contains("\"snapshot\"") {
            println!(r#"{{"type":"snapshot","version":3,"state":{{"counts":{{}},"jobs":{{}}}}}}"#);
        } else {
            println!(r#"{{"type":"rejected","reason":"tiny-secret token=fake-reject"}}"#);
        }
        io::stdout().flush().unwrap();
    }
}
