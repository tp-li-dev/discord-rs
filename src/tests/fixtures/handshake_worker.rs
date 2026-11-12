use std::io::{self, BufRead, Write};
fn main() {
    let name = std::env::current_exe()
        .unwrap()
        .file_stem()
        .unwrap()
        .to_string_lossy()
        .into_owned();
    for (index, line) in io::stdin().lock().lines().enumerate() {
        line.unwrap();
        let response = if index == 0 {
            match name.as_str() {
                "bad-version" => r#"{"type":"ready","protocol":99,"build":"fixture","pid":1}"#,
                "bad-capability" => {
                    r#"{"type":"ready","protocol":5,"required_capabilities":["unknown"],"build":"fixture","pid":1}"#
                }
                "legacy" => r#"{"type":"ready","protocol":5,"build":"fixture","pid":1}"#,
                _ => {
                    r#"{"type":"ready","protocol":5,"supported_capabilities":["hello-v1"],"build":"fixture","pid":1}"#
                }
            }
        } else {
            assert_ne!(
                name, "legacy",
                "legacy worker received an unexpected request"
            );
            r#"{"type":"hello","hello":{"protocol_min":5,"protocol_max":5,"identity":{"bot_id":999,"shard":{"id":0,"total":1},"generation_id":0,"ownership_epoch":0},"required_capabilities":["snapshot-v3"],"optional_capabilities":[]}}"#
        };
        println!("{response}");
        io::stdout().flush().unwrap();
    }
}
