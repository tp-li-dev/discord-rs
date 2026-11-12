//! Offline configuration views only. No daemon, credentials or processes are accessed.
use serde_json::{Value, json};
use src::{
    botref,
    placement::{Config, LocalShardIntent, ShardCount},
    shard_runtime::{self, ProcessSpec},
};
use std::{collections::HashMap, env, path::Path};

const NOTE: &str = "オフライン設定表示のみ。実稼働状態・認証・quorum/leaseは未検証。起動・所有権付与は行いません。";

fn process(spec: &ProcessSpec) -> Value {
    json!({"program": spec.program.to_string_lossy(), "args": spec.args.iter().map(|a| a.to_string_lossy()).collect::<Vec<_>>(),
        "clear_inherited": spec.environment.clear_inherited,
        "env_names": spec.environment.credential_refs,
        "state_path": spec.state_path, "management_path": spec.management_path})
}

fn run(args: &[String]) -> Result<Value, String> {
    let mut words = Vec::new();
    let mut file = None;
    let mut json_seen = false;
    let mut args = args.iter();
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--json" if !json_seen => json_seen = true,
            "--file" if file.is_none() => {
                let value = args
                    .next()
                    .filter(|v| !v.starts_with('-'))
                    .ok_or("--file requires a path")?;
                file = Some(value);
            }
            value if value.starts_with('-') => return Err("unknown or duplicate option".into()),
            _ => words.push(arg.as_str()),
        }
    }
    if let Some(
        "start" | "stop" | "restart" | "update" | "rollback" | "status" | "logs" | "nodes"
        | "doctor" | "daemon",
    ) = words.first().copied()
    {
        return Err("未実装: daemon 接続・実行操作には対応していません".into());
    }
    if !matches!(
        words.as_slice(),
        ["config", "validate"] | ["ps"] | ["shards", _]
    ) {
        return Err(
            "usage: botctl config validate|ps|shards <bot> --file F [--json]; その他の操作は未実装"
                .into(),
        );
    }
    let config =
        Config::load(Path::new(file.ok_or("--file is required")?)).map_err(|e| e.to_string())?;
    botref::validate_bots(&config.bots).map_err(|e| e.to_string())?;
    // Resolve before placement checks so unknown/ambiguous selectors are diagnosed consistently.
    let selected = if words[0] == "shards" {
        Some(botref::resolve(&config.bots, words[1]).map_err(|e| e.to_string())?)
    } else {
        None
    };
    config
        .validate(&HashMap::new())
        .map_err(|e| e.to_string())?;
    // Also reject case-insensitively shared credential references on Windows/Linux.
    for (i, bot) in config.bots.iter().enumerate() {
        if config.bots[..i]
            .iter()
            .any(|b| b.token_env.eq_ignore_ascii_case(&bot.token_env))
        {
            return Err("token environment reference shared across bots".into());
        }
    }
    match words[0] {
        "config" => {
            Ok(json!({"ok": true, "offline": true, "note": NOTE, "bot_count": config.bots.len()}))
        }
        "ps" => Ok(
            json!({"ok": true, "offline": true, "note": NOTE, "bots": config.bots.iter().map(|b|
            json!({"name": b.name, "bot_id": b.bot_id, "total_shards": b.total_shards, "status": "configured"})
        ).collect::<Vec<_>>()}),
        ),
        _ => {
            let bot = selected.expect("shards selector resolved");
            let ShardCount::Fixed(total) = bot.total_shards else {
                return Err("automatic shard count unresolved".into());
            };
            let intents = bot
                .shard_placement
                .iter()
                .map(|p| LocalShardIntent {
                    bot_id: bot.bot_id.clone(),
                    shard_id: p.shard_id,
                    total_shards: total,
                    token_env: bot.token_env.clone(),
                })
                .collect::<Vec<_>>();
            let root = env::current_dir()
                .map_err(|_| "cannot determine offline runtime root")?
                .join("runtime");
            let specs = shard_runtime::plan(&intents, &root).map_err(|e| e.to_string())?;
            let shards = specs.iter().map(|s| {
                let placement = bot.shard_placement.iter().find(|p| p.shard_id == s.shard_id).expect("explicit placement");
                json!({"shard_id": s.shard_id, "total_shards": s.total_shards,
                    "primary": placement.primary, "failover": placement.failover,
                    "ipc_path": s.ipc_path, "gateway": process(&s.gateway), "core": process(&s.core)})
            }).collect::<Vec<_>>();
            Ok(json!({"ok": true, "offline": true, "note": NOTE,
                "runtime_root_note": "仮のパス基点: カレントディレクトリ/runtime。各配置先での実パスは未確定。明示配置のみ表示。",
                "name": bot.name, "bot_id": bot.bot_id, "shards": shards}))
        }
    }
}

fn main() {
    let args: Vec<_> = env::args().skip(1).collect();
    let json_output = args.iter().any(|a| a == "--json");
    match run(&args) {
        Ok(value) => println!(
            "{}",
            serde_json::to_string_pretty(&value).expect("JSON value")
        ),
        Err(error) => {
            if json_output {
                println!("{}", json!({"ok": false, "offline": true, "error": error}));
            } else {
                eprintln!("botctl: {error}");
            }
            std::process::exit(1);
        }
    }
}
