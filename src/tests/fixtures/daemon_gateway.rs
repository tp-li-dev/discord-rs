//! Test-only Gateway contract: private stdin requests shutdown; EOF is not shutdown.
//! Audit files contain process IDs and booleans only, never credential values.
use std::{
    collections::HashMap,
    io::{BufRead, BufReader, Read},
    path::PathBuf,
    sync::mpsc,
    time::{Duration, Instant},
};

fn main() {
    let words: Vec<_> = std::env::args().skip(1).collect();
    if words.len() != 14 {
        std::process::exit(80);
    }
    let args: HashMap<_, _> = words
        .chunks_exact(2)
        .map(|pair| (pair[0].as_str(), pair[1].as_str()))
        .collect();
    let bot = args["--bot-id"];
    let (token_name, value) = match bot {
        "ab100-alpha" => ("TOKEN_ALPHA", "dummy-alpha"),
        "ab200-beta" => ("TOKEN_BETA", "dummy-beta"),
        _ => std::process::exit(81),
    };
    let isolated = args["--token-env"] == token_name
        && std::env::var(token_name).ok().as_deref() == Some(value)
        && ["TOKEN_ALPHA", "TOKEN_BETA", "DISCORD_TOKEN", "PATH"]
            .iter()
            .filter(|name| **name != token_name)
            .all(|name| std::env::var_os(name).is_none());
    if !isolated {
        std::process::exit(82);
    }
    let shard: u32 = args["--shard-id"].parse().unwrap();
    let total: u32 = args["--total-shards"].parse().unwrap();
    let state = PathBuf::from(args["--state-path"]);
    let management = PathBuf::from(args["--management-path"]);
    let ipc = PathBuf::from(args["--ipc-path"]);
    if shard >= total || state.parent() != management.parent() || state.parent() != ipc.parent() {
        std::process::exit(83);
    }
    let pid = std::process::id();
    std::fs::write(&state, pid.to_string()).unwrap();
    std::fs::write(management.with_extension(format!("started-{pid}")), b"true").unwrap();

    let (sender, receiver) = mpsc::channel();
    std::thread::spawn(move || {
        let mut line = String::new();
        // The production side writes a single bounded JSON line. A closed stdin
        // after daemon death must not terminate this independently owned child.
        if BufReader::new(std::io::stdin())
            .take(513)
            .read_line(&mut line)
            .is_ok()
            && line.len() <= 512
        {
            let compact: String = line.chars().filter(|ch| !ch.is_whitespace()).collect();
            if compact == "{\"command\":\"shutdown\"}" {
                let _ = sender.send(());
            }
        }
    });
    let deadline = Instant::now() + Duration::from_secs(60);
    let mut shutdown_at = None;
    let mut waiting_for_release = false;
    loop {
        // Independent test teardown, also used after an assertion or daemon crash.
        if management.with_extension("fixture-exit").exists() {
            std::fs::write(management.with_extension(format!("exited-{pid}")), b"true").unwrap();
            return;
        }
        if receiver.try_recv().is_ok() {
            std::fs::write(
                management.with_extension(format!("shutdown-{pid}")),
                b"true",
            )
            .unwrap();
            let mode = std::fs::read_to_string(management.with_extension("fixture-mode"))
                .unwrap_or_default();
            match mode.trim() {
                "slow" => waiting_for_release = true,
                "ignore" => {}
                "fail-stop" => std::process::exit(23),
                _ => shutdown_at = Some(Instant::now()),
            }
        }
        if shutdown_at.is_some_and(|at| Instant::now() >= at)
            || (waiting_for_release && management.with_extension("fixture-release").exists())
        {
            std::fs::write(management.with_extension(format!("drained-{pid}")), b"true").unwrap();
            return;
        }
        if Instant::now() >= deadline {
            std::process::exit(84);
        }
        std::thread::sleep(Duration::from_millis(5));
    }
}
