//! Offline configuration checks. No process, ownership, connectivity or Discord checks.
use src::placement::Config;
use std::{collections::HashMap, env, path::Path};

fn run(args: impl IntoIterator<Item = String>) -> Result<(), String> {
    let mut args = args.into_iter();
    let path = args
        .next()
        .ok_or("usage: doctor <config.toml> [--resolved-shards BOT_ID=COUNT]...")?;
    if path.starts_with('-') {
        return Err("usage: doctor <config.toml> [--resolved-shards BOT_ID=COUNT]...".into());
    }
    let mut resolved = HashMap::new();
    while let Some(arg) = args.next() {
        if arg != "--resolved-shards" {
            return Err("unknown argument; expected --resolved-shards BOT_ID=COUNT".into());
        }
        let value = args.next().ok_or("missing BOT_ID=COUNT")?;
        let (bot, count) = value.split_once('=').ok_or("expected BOT_ID=COUNT")?;
        if bot.is_empty() || count.is_empty() {
            return Err("expected BOT_ID=COUNT".into());
        }
        let count: u32 = count.parse().map_err(|_| "invalid shard count")?;
        if count == 0 || resolved.insert(bot.to_owned(), count).is_some() {
            return Err("shard counts must be positive and supplied once per bot".into());
        }
    }
    let config = Config::load(Path::new(&path)).map_err(|e| e.to_string())?;
    // Catch a mistyped bot ID rather than silently accepting a useless override.
    for bot in resolved.keys() {
        if !config.bots.iter().any(|b| {
            b.bot_id == *bot && matches!(b.total_shards, src::placement::ShardCount::Auto(_))
        }) {
            return Err("resolved shard count does not match an auto-configured bot".into());
        }
    }
    config.validate(&resolved).map_err(|e| e.to_string())?;
    println!(
        "Configuration validation passed for {} bot(s).",
        config.bots.len()
    );
    println!(
        "Offline check only: Discord, credentials, IPC, nodes, quorum and runtime are not checked."
    );
    println!(
        "Provided shard counts are unverified; no shard was started and no ownership was granted."
    );
    Ok(())
}

fn main() {
    src::redact::install_panic_hook();
    let args = env::args_os()
        .skip(1)
        .map(|arg| {
            arg.into_string()
                .map_err(|_| "arguments are not Unicode [argument_encoding]")
        })
        .collect::<Result<Vec<_>, _>>();
    let result = args.map_err(str::to_owned).and_then(run);
    if let Err(error) = result {
        // run only returns code-owned diagnostics; paths and config text are never included.
        eprintln!("doctor: {}", src::redact::redact(&error));
        std::process::exit(1);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::process::Command;

    #[test]
    #[ignore = "subprocess fixture"]
    fn secret_output_fixture() {
        let Some(input) = env::var_os("DOCTOR_OUTPUT_FIXTURE") else {
            return;
        };
        src::redact::install_panic_hook();
        let error = run([input.into_string().unwrap()]).unwrap_err();
        eprintln!("doctor: {}", src::redact::redact(&error));
        std::process::exit(1);
    }

    #[test]
    fn missing_config_diagnostic_never_echoes_secret_path() {
        let output = Command::new(env::current_exe().unwrap())
            .args(["--exact", "tests::secret_output_fixture", "--ignored", "--nocapture"])
            .env("DOCTOR_OUTPUT_FIXTURE", "missing-fixture/token=fake-token/secret=fake-secret/key=fake-key/Authorization: Bearer fake-auth")
            .output().unwrap();
        assert_eq!(output.status.code(), Some(1));
        let text = format!(
            "{}{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(text.contains("cannot open configuration"));
        for secret in ["fake-token", "fake-secret", "fake-key", "fake-auth"] {
            assert!(!text.contains(secret));
        }
    }
}
