use serde_json::Value;
use std::{
    path::PathBuf,
    process::{Command, Output},
    sync::atomic::{AtomicUsize, Ordering},
};

struct Fixture(PathBuf);
impl Fixture {
    fn new(text: &str) -> Self {
        static NEXT: AtomicUsize = AtomicUsize::new(0);
        let path = std::env::temp_dir().join(format!(
            "botctl-{}-{}.toml",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::write(&path, text).unwrap();
        Self(path)
    }
    fn run(&self, args: &[&str]) -> Output {
        Command::new(env!("CARGO_BIN_EXE_botctl"))
            .args(args)
            .arg("--file")
            .arg(&self.0)
            .env("FIRST_TOKEN", "SENTINEL_CREDENTIAL_VALUE")
            .output()
            .unwrap()
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}
const CONFIG: &str = r#"
[cluster]
id = "c"
mode = "standalone"
[node]
id = "a"
[[bots]]
name = "音楽"
bot_id = "abc-one"
application_id = "1"
token_env = "FIRST_TOKEN"
total_shards = 2
[[bots.shard_placement]]
shard_id = 0
primary = "a"
[[bots.shard_placement]]
shard_id = 1
primary = "b"
[[bots]]
name = "second"
bot_id = "abd-two"
application_id = "2"
token_env = "SECOND_TOKEN"
total_shards = 1
"#;
fn json(output: &Output, success: bool) -> Value {
    assert_eq!(
        output.status.success(),
        success,
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(output.stderr.is_empty());
    let text = String::from_utf8_lossy(&output.stdout);
    assert!(!text.contains("SENTINEL_CREDENTIAL_VALUE"));
    let value: Value = serde_json::from_str(&text).unwrap();
    assert_eq!(value["ok"], success);
    value
}
#[test]
fn validates_and_lists_configured_bots() {
    let f = Fixture::new(CONFIG);
    assert_eq!(
        json(&f.run(&["config", "validate", "--json"]), true)["bot_count"],
        2
    );
    let value = json(&f.run(&["ps", "--json"]), true);
    assert_eq!(value["bots"][0]["name"], "音楽");
    assert_eq!(value["bots"][0]["status"], "configured");
    assert!(f.run(&["ps"]).status.success());
}
#[test]
fn resolves_selectors_and_lists_specs_without_credentials() {
    let f = Fixture::new(CONFIG);
    for selector in ["音楽", "abc-one", "abc"] {
        let value = json(&f.run(&["shards", selector, "--json"]), true);
        let shards = value["shards"].as_array().unwrap();
        assert_eq!(shards.len(), 2);
        assert_eq!(shards[1]["primary"], "b");
        assert_eq!(shards[0]["gateway"]["program"], "gateway-host");
        assert!(
            shards[0]["gateway"]["args"]
                .as_array()
                .unwrap()
                .iter()
                .all(Value::is_string)
        );
        assert_eq!(
            shards[0]["gateway"]["env_names"],
            serde_json::json!(["FIRST_TOKEN"])
        );
        assert_eq!(shards[0]["core"]["env_names"], serde_json::json!([]));
        assert!(!shards[0]["core"].to_string().contains("FIRST_TOKEN"));
        assert_eq!(shards[0]["gateway"]["clear_inherited"], true);
    }
}
#[test]
fn unknown_and_ambiguous_bots_fail_in_json_and_text() {
    let f = Fixture::new(CONFIG);
    for (query, message) in [("absent", "not found"), ("ab", "ambiguous")] {
        assert!(
            json(&f.run(&["shards", query, "--json"]), false)["error"]
                .as_str()
                .unwrap()
                .contains(message)
        );
        let output = f.run(&["shards", query]);
        assert!(!output.status.success());
        assert!(String::from_utf8_lossy(&output.stderr).contains(message));
    }
    let collision = Fixture::new(&CONFIG.replace("second", "abc"));
    assert!(
        json(&collision.run(&["shards", "abc", "--json"]), false)["error"]
            .as_str()
            .unwrap()
            .contains("ambiguous")
    );
}
#[test]
fn invalid_unresolved_and_unimplemented_commands_fail() {
    let f = Fixture::new(CONFIG);
    for command in ["start", "stop", "update", "status", "nodes", "logs"] {
        assert!(
            json(&f.run(&[command, "--json"]), false)["error"]
                .as_str()
                .unwrap()
                .contains("未実装")
        );
    }
    for args in [
        vec!["ps", "extra", "--json"],
        vec!["ps", "--json", "--json"],
        vec!["ps", "--bad", "--json"],
    ] {
        json(&f.run(&args), false);
    }
    let auto = Fixture::new(&CONFIG.replace("total_shards = 2", "total_shards = \"auto\""));
    assert!(
        json(&auto.run(&["config", "validate", "--json"]), false)["error"]
            .as_str()
            .unwrap()
            .contains("unresolved")
    );
    let bad = Fixture::new("token = 'PASTED_SECRET_SENTINEL'");
    let out = bad.run(&["ps", "--json"]);
    assert!(!String::from_utf8_lossy(&out.stdout).contains("PASTED_SECRET_SENTINEL"));
    json(&out, false);
    let shared = Fixture::new(&CONFIG.replace("SECOND_TOKEN", "first_token"));
    json(&shared.run(&["config", "validate", "--json"]), false);
    let missing = Fixture::new(CONFIG);
    std::fs::remove_file(&missing.0).unwrap();
    json(&missing.run(&["ps", "--json"]), false);
}
