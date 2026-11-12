use std::io::{self, BufRead, Write};
fn main() {
    assert!(std::env::var_os("DISCORD_TOKEN").is_none());
    let identity = std::env::var("BOT_WORKER_IPC_IDENTITY").unwrap();
    assert!(identity.contains("\"bot_id\":42"));
    assert!(identity.contains("\"shard\":{\"id\":2,\"total\":4}"));
    let name = std::env::current_exe()
        .unwrap()
        .file_stem()
        .unwrap()
        .to_string_lossy()
        .into_owned();
    for line in io::stdin().lock().lines() {
        let line = line.unwrap();
        let response = if line.contains("\"type\":\"health\"") {
            r#"{"type":"ready","protocol":5,"supported_capabilities":["hello-v1"],"build":"deployment","pid":1}"#.to_owned()
        } else if line.contains("\"type\":\"hello\"") {
            assert!(line.contains(&identity));
            if name == "good" {
                assert!(
                    identity.contains("\"generation_id\":100")
                        || identity.contains("\"generation_id\":101")
                );
            }
            match name.as_str() {
                "bot" => line.replace("\"bot_id\":42", "\"bot_id\":43"),
                "shard" => line.replace("\"id\":2", "\"id\":1"),
                "total" => line.replace("\"total\":4", "\"total\":5"),
                "generation" => line.replace(
                    &identity,
                    &identity.replace("\"generation_id\":", "\"generation_id\":9"),
                ),
                _ => line,
            }
        } else if line.contains("\"type\":\"snapshot\"") {
            r#"{"type":"snapshot","version":3,"state":{}}"#.to_owned()
        } else {
            r#"{"type":"restored"}"#.to_owned()
        };
        println!("{response}");
        io::stdout().flush().unwrap();
    }
}
