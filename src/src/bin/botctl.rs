//! Authenticated local daemon client, with offline configuration/update planning.
//! Bootstrap capabilities are supplied through anonymous stdin, or (for a systemd-resident
//! botd) a permission-checked secret file via `--secret-file` with the daemon's fixed
//! `--port`. Never argv values, environment variables or an endpoint file.
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use src::daemon::control::{
    CORE_RECOVERY_VERSION, core_recovery_failure_reason, core_recovery_incarnation,
};
use src::local_control::DaemonControlEnvelope;
use src::update_rollback::{self, Action, Component, UpdateControlRequest};
use src::{botref, placement::Config};
use std::{collections::HashMap, ffi::OsString, io::IsTerminal, path::PathBuf, time::Duration};
use tokio::{
    io::{AsyncBufReadExt, AsyncRead, AsyncReadExt, AsyncWriteExt, BufReader},
    net::TcpStream,
    time::{Instant, timeout_at},
};

const REQUEST_LIMIT: usize = 4096;
const RESPONSE_LIMIT: usize = 64 * 1024;
const CORE_USAGE: &str = "usage: botctl core status BOT --shard N | botctl core restart BOT --shard N --core K [--expect-incarnation ID] [--force] [--yes]; both require (--bootstrap-stdin | --secret-file ABS --port PORT) and accept [--json] [--timeout 30s]";
const CORE_RESTART_NOTE: &str = "succeeded means the old Core, if present, was reaped and a replacement was launched and given its IPC capability. In-flight work and events are not guaranteed to be recovered.";
const USAGE: &str = "usage: botctl ps|status [BOT]|inspect BOT|shards BOT|start BOT|stop BOT|restart BOT|operation ID (--bootstrap-stdin | --secret-file ABS --port PORT) [--json] [--timeout 30s]; botctl start|stop|restart|gateway-update BOT [--shard N]... [--guild ID]...; botctl start|stop|restart|gateway-update --all (--yes | --dry-run); botctl core status|restart BOT --shard N; botctl config validate --file F [--json]; botctl secret generate --out ABS [--json]; botctl update|rollback BOT --plan manifest.json [--component core|gateway|all] [--rolling] [--dry-run | --yes (--bootstrap-stdin | --secret-file ABS --port PORT)] [--json]";
const OFFLINE_NOTE: &str =
    "オフライン設定検証のみ。daemon・実稼働状態・認証・quorum/leaseは検証していません。";

#[derive(Clone)]
struct Options {
    command: &'static str,
    bot: Option<String>,
    file: Option<PathBuf>,
    operation_id: Option<String>,
    timeout: Duration,
    force: bool,
    yes: bool,
    plan: Option<PathBuf>,
    component: Component,
    secret_file: Option<PathBuf>,
    port: Option<u16>,
    out: Option<PathBuf>,
    /// `--shard N` (repeatable): act on these local Shards of the Bot only.
    shards: Vec<u32>,
    /// `--guild ID` (repeatable): act on the Shard that serves each guild
    /// (`(guild_id >> 22) % total_shards`, resolved from the daemon's `inspect` before any mutation).
    guilds: Vec<u64>,
    /// `--all`: run the command for every Bot that has local Shards, one Bot at a time,
    /// stopping at the first failure. Needs `--yes` (or `--dry-run` to only list the targets).
    all: bool,
    all_dry_run: bool,
    /// Only populated from the `--all` snapshot, never a fuzzy identity.
    expect_bot_id: Option<String>,
    core: Option<u32>,
    expect_incarnation: Option<String>,
}

fn valid_operation_id(value: &str) -> bool {
    value.len() == 32
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

/// Default client wait, not a completion guarantee. The daemon's 120 s + 45 s per shard
/// deadline can be followed by up to 20 s of activation, plus polling/transport overhead.
/// Operators can extend this wait with --timeout.
fn default_wait_seconds(command: &str) -> u64 {
    if command == "gateway-update" { 300 } else { 30 }
}

fn parse_timeout(value: &str) -> Result<Duration, &'static str> {
    let (number, scale) = if let Some(value) = value.strip_suffix("ms") {
        (value, 0.001)
    } else {
        (value.strip_suffix('s').unwrap_or(value), 1.0)
    };
    let seconds = number.parse::<f64>().map_err(|_| "invalid --timeout")? * scale;
    if !seconds.is_finite() || seconds <= 0.0 || seconds > 3600.0 {
        return Err("--timeout must be greater than zero and at most 3600 seconds");
    }
    let duration = Duration::try_from_secs_f64(seconds).map_err(|_| "invalid --timeout")?;
    if duration.is_zero() {
        return Err("--timeout is too small");
    }
    Ok(duration)
}

fn parse_options(args: &[OsString]) -> Result<Options, &'static str> {
    let mut words = Vec::new();
    let mut file = None;
    let mut bootstrap = false;
    let mut json_seen = false;
    let mut timeout = None;
    let mut operation_id = None;
    let mut shards: Vec<u32> = Vec::new();
    let mut guilds: Vec<u64> = Vec::new();
    let mut all = false;
    let mut force = false;
    let mut yes = false;
    let mut plan = None;
    let mut component = None;
    let mut dry_run = false;
    let mut rolling = false;
    let mut secret_file = None;
    let mut port = None;
    let mut out = None;
    let mut core = None;
    let mut expect_incarnation = None;
    let mut args = args.iter();
    while let Some(arg) = args.next() {
        match arg.to_str() {
            Some("--json") if !json_seen => json_seen = true,
            Some("--bootstrap-stdin") if !bootstrap => bootstrap = true,
            Some("--force") if !force => force = true,
            Some("--yes") if !yes => yes = true,
            Some("--dry-run") if !dry_run => dry_run = true,
            Some("--rolling") if !rolling => rolling = true,
            Some(flag @ ("--secret-file" | "--out"))
                if (flag == "--secret-file" && secret_file.is_none())
                    || (flag == "--out" && out.is_none()) =>
            {
                let value = args
                    .next()
                    .filter(|v| !v.is_empty() && !v.to_string_lossy().starts_with('-'))
                    .ok_or("--secret-file and --out require a path")?;
                let value = PathBuf::from(value);
                if !value.is_absolute() {
                    return Err("--secret-file and --out require an absolute path");
                }
                if flag == "--secret-file" {
                    secret_file = Some(value);
                } else {
                    out = Some(value);
                }
            }
            Some("--port") if port.is_none() => {
                let value = args
                    .next()
                    .and_then(|v| v.to_str())
                    .and_then(|v| v.parse::<u16>().ok())
                    .filter(|p| *p != 0)
                    .ok_or("--port requires a nonzero TCP port")?;
                port = Some(value);
            }
            Some("--plan") if plan.is_none() => {
                let value = args
                    .next()
                    .filter(|v| !v.is_empty() && !v.to_string_lossy().starts_with('-'))
                    .ok_or("--plan requires a path")?;
                plan = Some(PathBuf::from(value));
            }
            Some("--component") if component.is_none() => {
                component = Some(match args.next().and_then(|v| v.to_str()) {
                    Some("core") => Component::Core,
                    Some("gateway") => Component::Gateway,
                    Some("all") => Component::All,
                    _ => return Err("--component requires core, gateway or all"),
                });
            }
            Some("--file") if file.is_none() => {
                let value = args
                    .next()
                    .filter(|value| !value.is_empty())
                    .ok_or("--file requires a path")?;
                if value.to_string_lossy().starts_with('-') {
                    return Err("--file requires a path");
                }
                file = Some(PathBuf::from(value));
            }
            Some("--shard") => {
                let value = args
                    .next()
                    .and_then(|value| value.to_str().map(str::to_owned))
                    .ok_or("--shard requires a shard number")?;
                if value.is_empty() || !value.bytes().all(|byte| byte.is_ascii_digit()) {
                    return Err("--shard requires a non-negative shard number");
                }
                let number = value
                    .parse::<u32>()
                    .map_err(|_| "--shard number is too large")?;
                if shards.contains(&number) {
                    return Err("--shard was given twice for the same shard");
                }
                shards.push(number);
            }
            Some("--all") if !all => all = true,
            Some("--core") if core.is_none() => {
                let number = args
                    .next()
                    .and_then(|value| value.to_str())
                    .filter(|value| value.bytes().all(|byte| byte.is_ascii_digit()))
                    .and_then(|value| value.parse::<u32>().ok())
                    .ok_or("--core requires a non-negative Core slot number")?;
                core = Some(number);
            }
            Some("--expect-incarnation") if expect_incarnation.is_none() => {
                let id = args
                    .next()
                    .and_then(|value| value.to_str())
                    .map(str::to_ascii_lowercase)
                    .filter(|value| core_recovery_incarnation(value))
                    .ok_or("--expect-incarnation requires a nonzero 16-digit hexadecimal ID")?;
                expect_incarnation = Some(id);
            }
            Some("--guild") => {
                let value = args
                    .next()
                    .and_then(|value| value.to_str().map(str::to_owned))
                    .ok_or("--guild requires a guild ID")?;
                if value.is_empty() || !value.bytes().all(|byte| byte.is_ascii_digit()) {
                    return Err("--guild requires a numeric guild ID");
                }
                let id = value
                    .parse::<u64>()
                    .map_err(|_| "--guild ID is too large")?;
                if id == 0 {
                    return Err("--guild ID must not be zero");
                }
                if guilds.contains(&id) {
                    return Err("--guild was given twice for the same guild");
                }
                guilds.push(id);
            }
            Some("--timeout") if timeout.is_none() => {
                timeout = Some(parse_timeout(
                    args.next()
                        .and_then(|value| value.to_str())
                        .ok_or("--timeout requires a duration")?,
                )?);
            }
            Some("--operation-id") if operation_id.is_none() => {
                let value = args
                    .next()
                    .and_then(|value| value.to_str())
                    .ok_or("--operation-id requires an ID")?;
                if !valid_operation_id(value) {
                    return Err("operation ID must contain 32 lowercase hexadecimal characters");
                }
                operation_id = Some(value.to_owned());
            }
            Some("create" | "logs" | "nodes" | "doctor" | "daemon") if words.is_empty() => {
                return Err("未実装: この daemon 操作には対応していません");
            }
            Some(value) if !value.starts_with('-') => words.push(value),
            _ => return Err("unknown, invalid or duplicate option"),
        }
    }
    let (command, bot) = match words.as_slice() {
        ["start"] if all => ("start", None),
        ["stop" | "shutdown"] if all => ("stop", None),
        ["restart" | "reboot"] if all => ("restart", None),
        ["gateway-update"] if all => ("gateway-update", None),
        ["config", "validate"] => ("config", None),
        ["secret", "generate"] => ("secret", None),
        ["ps"] => ("ps", None),
        ["status"] => ("status", None),
        ["status", bot] => ("status", Some((*bot).to_owned())),
        ["inspect", bot] => ("inspect", Some((*bot).to_owned())),
        ["shards", bot] => ("shards", Some((*bot).to_owned())),
        ["start", bot] => ("start", Some((*bot).to_owned())),
        ["stop" | "shutdown", bot] => ("stop", Some((*bot).to_owned())),
        ["restart" | "reboot", bot] => ("restart", Some((*bot).to_owned())),
        ["gateway-update", bot] => ("gateway-update", Some((*bot).to_owned())),
        ["core", "status", bot] => ("core-preview", Some((*bot).to_owned())),
        ["core", "restart", bot] => ("core-restart", Some((*bot).to_owned())),
        ["core", ..] => return Err(CORE_USAGE),
        ["update", bot] => ("update", Some((*bot).to_owned())),
        ["rollback", bot] => ("rollback", Some((*bot).to_owned())),
        ["operation" | "op", id] if operation_id.is_none() => {
            if !valid_operation_id(id) {
                return Err("operation ID must contain 32 lowercase hexadecimal characters");
            }
            operation_id = Some((*id).to_owned());
            ("operation", None)
        }
        _ => return Err(USAGE),
    };
    if bot.as_ref().is_some_and(|bot| bot.is_empty()) {
        return Err("Bot selector must not be empty");
    }
    if bootstrap && secret_file.is_some() {
        return Err("--bootstrap-stdin and --secret-file are mutually exclusive");
    }
    if secret_file.is_some() != port.is_some() {
        return Err("--secret-file and --port must be specified together");
    }
    let core_restart = command == "core-restart";
    let core_command = core_restart || command == "core-preview";
    if core_command && (shards.len() != 1 || !guilds.is_empty()) {
        return Err("core status and core restart require exactly one --shard N");
    }
    if core_restart && core.is_none() {
        return Err("core restart requires --core K");
    }
    if !core_restart && (core.is_some() || expect_incarnation.is_some()) {
        return Err("--core and --expect-incarnation require core restart");
    }
    if command == "secret" {
        if out.is_none() {
            return Err("secret generate requires --out");
        }
        if bootstrap
            || secret_file.is_some()
            || file.is_some()
            || timeout.is_some()
            || operation_id.is_some()
            || force
            || yes
            || plan.is_some()
            || component.is_some()
            || dry_run
            || rolling
            || !shards.is_empty()
            || !guilds.is_empty()
            || all
        {
            return Err("secret generate is offline and accepts only --out and --json");
        }
        return Ok(Options {
            command,
            bot,
            file,
            operation_id,
            timeout: Duration::from_secs(30),
            force,
            yes,
            plan,
            component: Component::default(),
            secret_file,
            port,
            out,
            shards: Vec::new(),
            guilds: Vec::new(),
            all: false,
            all_dry_run: false,
            expect_bot_id: None,
            core,
            expect_incarnation,
        });
    }
    if out.is_some() {
        return Err("--out is only supported for secret generate");
    }
    let auth = bootstrap || secret_file.is_some();
    if force && !matches!(command, "stop" | "core-restart") {
        return Err("--force is supported only for stop and core restart");
    }
    if !core_command
        && (!shards.is_empty() || !guilds.is_empty())
        && !matches!(command, "start" | "stop" | "restart" | "gateway-update")
    {
        return Err(
            "--shard and --guild are supported only for start, stop, restart and gateway-update",
        );
    }
    if shards.len() > 64 || guilds.len() > 64 {
        return Err("--shard and --guild may each be given at most 64 times");
    }
    if all {
        if bot.is_some() {
            return Err("--all cannot be combined with a Bot name");
        }
        if !matches!(command, "start" | "stop" | "restart" | "gateway-update") {
            return Err("--all is supported only for start, stop, restart and gateway-update");
        }
        if !shards.is_empty() || !guilds.is_empty() || operation_id.is_some() {
            return Err("--all cannot be combined with --shard, --guild or --operation-id");
        }
        if dry_run && yes {
            return Err("--dry-run and --yes are mutually exclusive");
        }
        if !dry_run && !yes {
            return Err(
                "--all changes every Bot; add --yes to confirm (or --dry-run to list the targets)",
            );
        }
    }
    if force && command == "stop" && !yes && !(all && dry_run) {
        return Err("force stop requires explicit --yes confirmation");
    }
    let update = matches!(command, "update" | "rollback");
    if yes && !force && !update && !all && !core_restart {
        return Err("--yes requires an operation with confirmation");
    }
    if operation_id.is_some()
        && !matches!(
            command,
            "start" | "stop" | "restart" | "operation" | "update" | "rollback" | "gateway-update"
        )
    {
        return Err("--operation-id requires a mutation or operation query");
    }
    if !update && (plan.is_some() || component.is_some() || (dry_run && !all) || rolling) {
        return Err("update planning options require update or rollback");
    }
    if update {
        if operation_id.is_some() && !yes {
            return Err("--operation-id requires --yes for update/rollback");
        }
        if plan.is_none() {
            return Err(
                "未実装: daemon-derived update/rollback plans are unavailable; --plan is required",
            );
        }
        if file.is_some() {
            return Err("update/rollback uses --plan, not --file");
        }
        if dry_run && yes {
            return Err("--dry-run and --yes are mutually exclusive");
        }
        if yes && !auth {
            return Err(
                "update/rollback execution requires authenticated --bootstrap-stdin or --secret-file",
            );
        }
        if !yes && auth {
            return Err(
                "update/rollback dry-run is offline; omit --bootstrap-stdin/--secret-file or specify --yes",
            );
        }
    } else if command == "config" {
        if file.is_none() {
            return Err("--file is required for config validate");
        }
        if auth || port.is_some() || timeout.is_some() {
            return Err("config validate is offline; daemon connection options are not supported");
        }
    } else {
        if file.is_some() {
            return Err("--file is only supported for config validate; live commands require botd");
        }
        if !auth {
            return Err("daemon connection requires --bootstrap-stdin or --secret-file");
        }
    }
    Ok(Options {
        command,
        bot,
        file,
        operation_id,
        // Gateway updates get a longer default, not a bound on server completion.
        // Other commands keep 30 s. An explicit --timeout always wins.
        timeout: timeout.unwrap_or_else(|| Duration::from_secs(default_wait_seconds(command))),
        force,
        yes,
        plan,
        component: component.unwrap_or_default(),
        secret_file,
        port,
        out,
        shards,
        guilds,
        all,
        all_dry_run: all && dry_run,
        expect_bot_id: None,
        core,
        expect_incarnation,
    })
}

// Deliberately not Debug: credentials must never be formatted or logged.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Bootstrap {
    port: u16,
    secret: [u8; 32],
}

#[derive(Serialize)]
struct Request<'a> {
    secret: &'a [u8; 32],
    command: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    bot: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    operation_id: Option<&'a str>,
    force: bool,
    yes: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    shards: Option<&'a [u32]>,
    #[serde(skip_serializing_if = "Option::is_none")]
    expect_total_shards: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    expect_bot_id: Option<&'a str>,
}

async fn read_frame<R: AsyncRead + Unpin>(
    reader: R,
    limit: usize,
) -> Result<Vec<u8>, &'static str> {
    let mut frame = Vec::new();
    BufReader::new(reader)
        .take(limit as u64 + 1)
        .read_until(b'\n', &mut frame)
        .await
        .map_err(|_| "cannot read local control data")?;
    if frame.len() > limit || frame.last() != Some(&b'\n') {
        return Err("invalid or oversized local control frame");
    }
    Ok(frame)
}

async fn read_bootstrap() -> Result<Bootstrap, &'static str> {
    let frame = read_frame(tokio::io::stdin(), REQUEST_LIMIT).await?;
    let bootstrap: Bootstrap =
        serde_json::from_slice(&frame).map_err(|_| "invalid anonymous control bootstrap")?;
    if bootstrap.port == 0 || bootstrap.secret == [0; 32] {
        return Err("invalid anonymous control bootstrap");
    }
    Ok(bootstrap)
}

/// `--secret-file` 経路: 権限検査つきでファイルから secret を読み、port は `--port` で受け取る。
/// エラーは理由の種別のみ（secret・パス・OS のエラー文字列を含めない）。
async fn load_bootstrap(
    secret_file: Option<&std::path::Path>,
    port: Option<u16>,
) -> Result<Bootstrap, &'static str> {
    let (Some(path), Some(port)) = (secret_file, port) else {
        return read_bootstrap().await;
    };
    let path = path.to_path_buf();
    let secret = tokio::task::spawn_blocking(move || src::secret_file::load_secret_file(&path))
        .await
        .map_err(|_| "secret file task failed")?
        .map_err(|error| error.as_str())?;
    Ok(Bootstrap {
        port,
        secret: *secret.expose_secret(),
    })
}

fn secret_generate(path: &std::path::Path) -> Value {
    match src::secret_file::generate_secret_file(path) {
        Ok(()) => json!({
            "ok": true,
            "offline": true,
            "created": true,
            "note": "secret を 16 進 1 行・モード 0600 で新規作成しました。値は表示しません。"
        }),
        Err(error) => {
            json!({"ok": false, "offline": true, "created": false, "error": error.as_str()})
        }
    }
}

fn contains_control_secret(value: &Value, secret: &[u8; 32]) -> bool {
    fn walk(value: &Value, secret: &Value, encoded: &str, hex: &str) -> bool {
        let text_matches = |text: &str| {
            let compact: String = text.chars().filter(|c| !c.is_whitespace()).collect();
            compact.contains(encoded) || compact.to_ascii_lowercase().contains(hex)
        };
        if value == secret {
            return true;
        }
        match value {
            Value::String(text) => text_matches(text),
            Value::Array(values) => values.iter().any(|value| walk(value, secret, encoded, hex)),
            Value::Object(values) => values.iter().any(|(key, value)| {
                key.eq_ignore_ascii_case("secret")
                    || text_matches(key)
                    || walk(value, secret, encoded, hex)
            }),
            _ => false,
        }
    }
    let secret_value = json!(secret);
    let encoded = secret_value.to_string();
    let hex = secret
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    walk(value, &secret_value, &encoded, &hex)
}

/// While botd replays its audit journal at startup it answers `audit_replay_pending`
/// before any action has started, so the request is safe to repeat. The wait is bounded.
async fn exchange(bootstrap: &Bootstrap, request: &impl Serialize) -> Result<Value, &'static str> {
    let started = std::time::Instant::now();
    loop {
        let value = exchange_once(bootstrap, request).await?;
        if value["ok"] != false
            || value.get("operation").is_some()
            || value["error"] != "audit_replay_pending"
            || started.elapsed() >= Duration::from_secs(10)
        {
            return Ok(value);
        }
        let wait = value["retry_after_ms"]
            .as_u64()
            .unwrap_or(100)
            .clamp(25, 500);
        tokio::time::sleep(Duration::from_millis(wait)).await;
    }
}

async fn exchange_once(
    bootstrap: &Bootstrap,
    request: &impl Serialize,
) -> Result<Value, &'static str> {
    let frame = encode_request(request)?;
    // No hostname, endpoint file or arbitrary remote address is accepted.
    let mut stream = TcpStream::connect((std::net::Ipv4Addr::LOCALHOST, bootstrap.port))
        .await
        .map_err(|_| "local daemon unavailable")?;
    stream
        .write_all(&frame)
        .await
        .map_err(|_| "cannot send daemon request; outcome may be unknown")?;
    let frame = read_frame(&mut stream, RESPONSE_LIMIT).await?;
    let value: Value = serde_json::from_slice(&frame).map_err(|_| "invalid daemon response")?;
    if !value["ok"].is_boolean() || value["offline"] == true {
        return Err("invalid daemon response");
    }
    // A wrong or malicious local listener must not make the client echo its
    // request capability through an error string, array or unknown field.
    if contains_control_secret(&value, &bootstrap.secret) {
        return Err("daemon response contains control credentials");
    }
    Ok(value)
}

fn encode_request(request: &impl Serialize) -> Result<Vec<u8>, &'static str> {
    let mut frame = serde_json::to_vec(request).map_err(|_| "cannot encode control request")?;
    frame.push(b'\n');
    if frame.len() > REQUEST_LIMIT {
        return Err("control request exceeds limit");
    }
    Ok(frame)
}

fn failure(error: impl AsRef<str>, operation_id: Option<&str>, operation: Option<&Value>) -> Value {
    let mut value = json!({"ok": false, "offline": false, "error": error.as_ref()});
    if let Some(id) = operation_id {
        value["operation_id"] = json!(id);
    }
    if let Some(operation) = operation {
        value["operation"] = operation.clone();
    }
    value
}

fn config_validate(file: &std::path::Path) -> Result<Value, String> {
    let config = Config::load(file).map_err(|error| error.to_string())?;
    botref::validate_bots(&config.bots).map_err(|error| error.to_string())?;
    config
        .validate(&HashMap::new())
        .map_err(|error| error.to_string())?;
    for (index, bot) in config.bots.iter().enumerate() {
        if config.bots[..index]
            .iter()
            .any(|other| other.token_env.eq_ignore_ascii_case(&bot.token_env))
        {
            return Err("token environment reference shared across bots".into());
        }
    }
    Ok(json!({"ok": true, "offline": true, "note": OFFLINE_NOTE, "bot_count": config.bots.len()}))
}

fn new_operation_id() -> Result<String, &'static str> {
    use std::fmt::Write;
    let mut bytes = [0; 16];
    getrandom::fill(&mut bytes).map_err(|_| "cannot generate operation ID")?;
    let mut id = String::with_capacity(32);
    for byte in bytes {
        write!(id, "{byte:02x}").expect("formatting into a String");
    }
    Ok(id)
}

fn operation_state<'a>(value: &'a Value, id: &str, command: &str) -> Result<&'a str, &'static str> {
    let operation = &value["operation"];
    if operation["operation_id"].as_str() != Some(id)
        || (command != "operation" && operation["command"].as_str() != Some(command))
    {
        return Err("daemon response operation does not match request");
    }
    if operation["command"] == "core-restart" {
        return core_recovery_state(value);
    }
    let valid_bot = if operation["command"] == "restart-all" {
        operation.get("bot_id").is_some_and(Value::is_null)
    } else {
        operation["bot_id"].is_string()
    };
    if !valid_bot
        || operation["elapsed_ms"].as_u64().is_none()
        || !operation["shards"].is_array()
        || !operation["failed_shards"].is_array()
        || !matches!(
            operation["command"].as_str(),
            Some(
                "start"
                    | "stop"
                    | "restart"
                    | "restart-all"
                    | "update"
                    | "rollback"
                    | "gateway-update"
            )
        )
    {
        return Err("daemon operation has no valid result metadata");
    }
    if operation["state"] == "succeeded"
        && !operation["failed_shards"].as_array().unwrap().is_empty()
    {
        return Err("daemon operation claims success with failed shards");
    }
    match operation["state"].as_str() {
        Some(state @ ("running" | "succeeded" | "failed")) => Ok(state),
        _ => Err("invalid daemon operation state"),
    }
}

fn core_recovery_state(value: &Value) -> Result<&str, &'static str> {
    let op = &value["operation"];
    let invalid = "invalid Core recovery result";
    if op["core_recovery_version"] != CORE_RECOVERY_VERSION
        || op["bot_id"]
            .as_str()
            .is_none_or(|id| id.parse::<u64>().ok().is_none_or(|n| n.to_string() != id))
        || op["core"].as_u64().is_none_or(|core| core >= 2)
        || !op
            .get("incarnation")
            .is_some_and(|id| id.is_null() || id.as_str().is_some_and(core_recovery_incarnation))
    {
        return Err(invalid);
    }
    let shard = op["shard"]
        .as_u64()
        .and_then(|id| u32::try_from(id).ok())
        .ok_or(invalid)?;
    let state = op["state"].as_str().ok_or(invalid)?;
    let ok = match state {
        "running" | "succeeded" => op.get("reason") == Some(&Value::Null),
        "failed" => op["reason"]
            .as_str()
            .is_some_and(core_recovery_failure_reason),
        "unknown" => op["reason"] == "terminal_audit_missing",
        _ => false,
    };
    if !ok || value["ok"].as_bool() != Some(matches!(state, "running" | "succeeded")) {
        return Err(invalid);
    }
    match op["source"].as_str() {
        Some("archive") => {
            let terminal = match state {
                "unknown" => "missing",
                "failed" | "succeeded" => "synced",
                _ => return Err(invalid),
            };
            if op["terminal_audit"] != terminal
                || ["elapsed_ms", "shards", "failed_shards"]
                    .iter()
                    .any(|key| op.get(*key).is_some())
            {
                return Err(invalid);
            }
        }
        Some("live") => {
            let terminal_valid = match state {
                "running" => op.get("terminal_audit").is_none(),
                "succeeded" | "failed" => matches!(
                    op["terminal_audit"].as_str(),
                    Some("synced" | "pending_or_failed")
                ),
                _ => false,
            };
            let key = json!({"bot_id": op["bot_id"], "shard_id": shard});
            let shards = op["shards"].as_array().ok_or(invalid)?;
            let [process] = shards.as_slice() else {
                return Err(invalid);
            };
            let snapshot: src::daemon::ProcessSnapshot =
                serde_json::from_value(process.clone()).map_err(|_| invalid)?;
            let failed = op["failed_shards"].as_array().ok_or(invalid)?;
            let failed_valid = if state == "failed" {
                failed.as_slice() == std::slice::from_ref(&key)
            } else {
                failed.is_empty()
            };
            if !terminal_valid
                || op["elapsed_ms"].as_u64().is_none()
                || !failed_valid
                || process["key"] != key
                || snapshot.total_shards <= shard
                || !process.get("error").is_some_and(|error| {
                    if state == "failed" {
                        serde_json::from_value::<src::daemon::DaemonError>(error.clone()).is_ok()
                    } else {
                        error.is_null()
                    }
                })
            {
                return Err(invalid);
            }
        }
        _ => return Err(invalid),
    }
    Ok(state)
}

// Only display fields with a known type and vocabulary. Do not relay the
// daemon's notes, process snapshots, or any other free-form fields.
#[derive(Deserialize, Serialize)]
struct CorePreview {
    bot_id: String,
    total_shards: u32,
    shard: u32,
    cores: [CoreSlot; 2],
}

#[derive(Deserialize, Serialize)]
struct CoreSlot {
    running: bool,
    empty: bool,
    incarnation: Option<String>,
    status: Option<CoreDiagnostic>,
    #[serde(skip_serializing_if = "Option::is_none")]
    status_reason: Option<CoreStatusReason>,
}

#[derive(Deserialize, Serialize)]
struct CoreDiagnostic {
    suspected_stalled: bool,
    status_age_s: u64,
    pending: Option<u64>,
    oldest_age_s: Option<u64>,
}

#[derive(Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
enum CoreStatusReason {
    Unsupported,
    Unreadable,
    Corrupt,
    StaleGeneration,
    Regressed,
    ClockUnavailable,
}

fn validate_core_preview(value: Value, shard: u32) -> Result<CorePreview, &'static str> {
    let invalid = "invalid core-preview response; a Core recovery v2 daemon is required";
    let preview: CorePreview = serde_json::from_value(value).map_err(|_| invalid)?;
    if preview.shard != shard
        || shard >= preview.total_shards
        || preview
            .bot_id
            .parse::<u64>()
            .ok()
            .is_none_or(|id| id.to_string() != preview.bot_id)
        || !preview.cores.iter().all(|slot| {
            if slot.empty {
                !slot.running && slot.incarnation.is_none()
            } else {
                slot.incarnation
                    .as_deref()
                    .is_some_and(core_recovery_incarnation)
            }
        })
    {
        return Err(invalid);
    }
    Ok(preview)
}

fn core_reply_error(value: &Value) -> Option<&str> {
    if value["ok"] != false || value.get("operation").is_some() {
        return None;
    }
    value["error"].as_str()
}

fn core_restart_rejection(value: &Value) -> Option<&str> {
    core_reply_error(value).filter(|reason| {
        core_recovery_failure_reason(reason)
            || matches!(
                *reason,
                "permission_denied"
                    | "operation_id_conflict"
                    | "bot_not_found"
                    | "bot_id_mismatch"
                    | "total_shards_mismatch"
                    | "shard_not_local"
                    | "busy"
                    | "operation_history_full"
                    | "audit_replay_pending"
                    | "daemon control busy"
                    | "invalid control request"
            )
    })
}

fn core_outcome(state: &str, error: Option<&str>, id: Option<&str>) -> Value {
    let mut value = json!({
        "ok": state == "succeeded", "offline": false, "state": state, "note": CORE_RESTART_NOTE
    });
    if let Some(error) = error {
        value["error"] = json!(error);
    }
    if let Some(id) = id {
        value["operation_id"] = json!(id);
    }
    value
}

// Call only after operation_state has validated the Core recovery reply.
fn core_operation_fields(op: &Value) -> Value {
    json!({
        "operation_id": op["operation_id"], "command": op["command"],
        "core_recovery_version": op["core_recovery_version"],
        "bot_id": op["bot_id"], "shard": op["shard"], "core": op["core"],
        "incarnation": op["incarnation"], "state": op["state"], "reason": op["reason"],
        "source": op["source"], "terminal_audit": op["terminal_audit"],
    })
}

async fn run_core(options: Options, mut deadline: Instant) -> Value {
    let restart = options.command == "core-restart";
    let preview_error = |state, error| {
        if restart {
            core_outcome(state, Some(error), None)
        } else {
            failure(error, None, None)
        }
    };
    if restart && !options.yes && !std::io::stdin().is_terminal() {
        return preview_error(
            "rejected",
            "core restart requires --yes with non-interactive stdin",
        );
    }
    let bootstrap = match timeout_at(
        deadline,
        load_bootstrap(options.secret_file.as_deref(), options.port),
    )
    .await
    {
        Ok(Ok(bootstrap)) => bootstrap,
        Ok(Err(error)) => return preview_error("rejected", error),
        Err(_) => return preview_error("timeout", "control bootstrap timed out"),
    };
    let shard = options.shards[0];
    let selector = options.bot.as_deref().expect("validated Bot selector");
    let request = json!({
        "secret": bootstrap.secret, "command": "core-preview", "bot": selector, "shard": shard,
    });
    let unsupported =
        "core-preview unavailable; verify authentication and use a Core recovery v2 daemon";
    let preview = match timeout_at(deadline, exchange(&bootstrap, &request)).await {
        Ok(Ok(value)) if value["ok"] == true => match validate_core_preview(value, shard) {
            Ok(preview) => preview,
            Err(error) => return preview_error("rejected", error),
        },
        Ok(Ok(value)) => {
            let error = match core_reply_error(&value) {
                Some(
                    error @ ("control permission or scope denied"
                    | "bot_not_found"
                    | "local separate Core shard required"
                    | "daemon control busy"),
                ) => error,
                _ => unsupported,
            };
            return preview_error("rejected", error);
        }
        Ok(Err(_)) => return preview_error("rejected", unsupported),
        Err(_) => {
            return preview_error(
                "timeout",
                "core-preview timed out; a Core recovery v2 daemon is required",
            );
        }
    };
    if !restart {
        let mut value = json!(preview);
        value["ok"] = json!(true);
        value["offline"] = json!(false);
        return value;
    }
    let core = options.core.expect("validated Core slot");
    let Some(slot) = preview.cores.get(core as usize) else {
        return core_outcome("rejected", Some("core_out_of_range"), None);
    };
    let peer = 1 - core;
    let target = json!({
        "bot_id": preview.bot_id, "total_shards": preview.total_shards,
        "shard": shard, "core": core, "slot": slot,
        "peer": {"core": peer, "slot": preview.cores[peer as usize]}, "force": options.force,
    });
    eprintln!(
        "Core restart target\n{}",
        serde_json::to_string_pretty(&target).expect("JSON value")
    );
    let mut outcome = if options
        .expect_incarnation
        .as_ref()
        .is_some_and(|id| slot.incarnation.as_ref() != Some(id))
    {
        core_outcome("rejected", Some("incarnation_mismatch"), None)
    } else {
        let prompt_started = Instant::now();
        let confirmed = options.yes || confirm_core_restart();
        deadline += prompt_started.elapsed();
        if confirmed {
            restart_core(
                &bootstrap,
                selector,
                &preview,
                core,
                options.force,
                deadline,
            )
            .await
        } else {
            core_outcome("rejected", Some("confirmation_declined"), None)
        }
    };
    outcome["target"] = target;
    outcome
}

fn confirm_core_restart() -> bool {
    eprintln!("Restart this Core? Type yes to continue.");
    let mut answer = String::new();
    std::io::stdin().read_line(&mut answer).is_ok() && answer.trim() == "yes"
}

async fn restart_core(
    bootstrap: &Bootstrap,
    selector: &str,
    preview: &CorePreview,
    core: u32,
    force: bool,
    deadline: Instant,
) -> Value {
    if Instant::now() >= deadline {
        return core_outcome("timeout", Some("operation_wait_timed_out"), None);
    }
    let id = match new_operation_id() {
        Ok(id) => id,
        Err(error) => return core_outcome("rejected", Some(error), None),
    };
    let slot = &preview.cores[core as usize];
    let mut request = json!({
        "secret": bootstrap.secret, "command": "core-restart", "bot": selector,
        "shard": preview.shard, "core": core, "expect_bot_id": preview.bot_id,
        "expect_total_shards": preview.total_shards, "operation_id": id,
        "force": force, "core_recovery_version": CORE_RECOVERY_VERSION,
    });
    if slot.empty {
        request["expect_empty"] = json!(true);
    } else {
        request["expect_incarnation"] = json!(slot.incarnation);
    }
    let query = json!({"secret": bootstrap.secret, "command": "operation", "operation_id": id});
    let mut sending = true;
    eprintln!("Core restart operation ID {id}");
    loop {
        let response = match timeout_at(
            deadline,
            exchange(bootstrap, if sending { &request } else { &query }),
        )
        .await
        {
            Ok(Ok(response)) => response,
            Ok(Err(_)) if sending => {
                // The daemon might have admitted the request before the reply was lost.
                sending = false;
                continue;
            }
            Ok(Err(_)) => {
                return core_outcome("unknown", Some("operation_query_unavailable"), Some(&id));
            }
            Err(_) => return core_outcome("timeout", Some("operation_wait_timed_out"), Some(&id)),
        };
        // A bare failure reason can follow audit admission. Query those replies
        // unless the daemon explicitly rejected the request.
        if sending
            && let Some(reason) = core_restart_rejection(&response)
            && (response["rejected"] == true || !core_recovery_failure_reason(reason))
        {
            let mut result = core_outcome("rejected", Some(reason), Some(&id));
            if let Some(wait) = response["retry_after_ms"].as_u64() {
                result["retry_after_ms"] = json!(wait);
            }
            return result;
        }
        let op = &response["operation"];
        let bound = op["bot_id"] == preview.bot_id
            && op["shard"] == preview.shard
            && op["core"] == core
            && op["incarnation"].as_str() == slot.incarnation.as_deref()
            && (op["source"] == "archive"
                || op["shards"][0]["total_shards"] == preview.total_shards);
        let state = operation_state(&response, &id, "core-restart")
            .ok()
            .filter(|_| bound);
        match state {
            Some(state @ ("succeeded" | "failed" | "unknown")) => {
                let mut result = core_outcome(state, op["reason"].as_str(), Some(&id));
                result["operation"] = core_operation_fields(op);
                return result;
            }
            Some("running") => {}
            _ if sending => {}
            _ => match core_reply_error(&response) {
                Some("audit_pending" | "audit_replay_pending" | "daemon control busy") => {}
                Some(
                    "operation not found in this daemon incarnation"
                    | "control permission or scope denied",
                ) => {
                    return core_outcome(
                        "unknown",
                        Some("operation_not_found_or_denied"),
                        Some(&id),
                    );
                }
                _ => {
                    return core_outcome("unknown", Some("invalid_operation_reply"), Some(&id));
                }
            },
        }
        sending = false;
        tokio::time::sleep_until(deadline.min(Instant::now() + Duration::from_millis(100))).await;
        if Instant::now() >= deadline {
            return core_outcome("timeout", Some("operation_wait_timed_out"), Some(&id));
        }
    }
}

fn validate_update_result(
    value: &Value,
    expected: Option<&UpdateControlRequest>,
) -> Result<(), &'static str> {
    let operation = &value["operation"];
    if !matches!(operation["command"].as_str(), Some("update" | "rollback")) {
        return Ok(());
    }
    if expected.is_some_and(|expected| operation["bot_id"].as_str() != Some(expected.bot.as_str()))
    {
        return Err("daemon update Bot does not match request");
    }
    if operation["state"] != "succeeded" {
        return Ok(());
    }
    let generations = operation["final_generations"]
        .as_array()
        .filter(|items| !items.is_empty())
        .ok_or("daemon update has no final generations")?;
    let mut seen = std::collections::HashSet::new();
    for entry in generations {
        let shard = entry["shard_id"]
            .as_u64()
            .filter(|id| *id <= u32::MAX as u64)
            .ok_or("invalid final generation Shard")? as u32;
        if !seen.insert(shard)
            || !(entry["core"].as_u64().is_some_and(|g| g > 0)
                || entry["gateway"].as_u64().is_some_and(|g| g > 0))
        {
            return Err("invalid or duplicate final generations");
        }
        if let Some(expected) = expected {
            let target = expected
                .manifest
                .shards
                .iter()
                .find(|s| s.shard_id == shard)
                .ok_or("unexpected final generation Shard")?;
            for (part, selected, generation) in [
                (
                    "core",
                    expected.component != Component::Gateway,
                    target.core,
                ),
                (
                    "gateway",
                    expected.component != Component::Core,
                    target.gateway,
                ),
            ] {
                if selected && entry[part].as_u64() != generation.map(|g| g.candidate) {
                    return Err("daemon final generation does not match candidate");
                }
            }
        }
    }
    if expected.is_some_and(|expected| generations.len() != expected.manifest.shards.len()) {
        return Err("daemon final generations omit requested Shards");
    }
    let shards = operation["shards"]
        .as_array()
        .expect("validated operation metadata");
    let reported: std::collections::HashSet<_> = shards
        .iter()
        .filter(|s| s["key"]["bot_id"] == operation["bot_id"])
        .filter_map(|s| s["key"]["shard_id"].as_u64())
        .collect();
    if shards.len() != seen.len()
        || reported.len() != seen.len()
        || !seen.iter().all(|s| reported.contains(&u64::from(*s)))
    {
        return Err("daemon affected Shards do not match request");
    }
    Ok(())
}

async fn run(options: Options) -> Value {
    if options.command == "config" {
        return config_validate(options.file.as_ref().expect("validated config path"))
            .unwrap_or_else(|error| json!({"ok": false, "offline": true, "error": error}));
    }
    if options.command == "secret" {
        return secret_generate(options.out.as_ref().expect("validated output path"));
    }
    if matches!(options.command, "update" | "rollback") {
        return run_update(options).await;
    }
    if options.all {
        return run_all(options).await;
    }
    let deadline = Instant::now() + options.timeout;
    if matches!(options.command, "core-preview" | "core-restart") {
        return run_core(options, deadline).await;
    }
    run_live(options, None, deadline, None).await
}

/// One Bot of an `--all` run.
#[derive(Clone, Debug, PartialEq, Eq)]
struct AllTarget {
    bot_id: String,
    name: String,
    selector: String,
}

/// Split the daemon's Bot list into the Bots that have local Shards (the targets, in listing
/// order) and the ones that do not (reported as skipped, never silently dropped).
fn plan_all(bots: &[Value]) -> Result<(Vec<AllTarget>, Vec<Value>), String> {
    // Resolve against the complete listing, including non-local Bots, exactly as
    // botref::resolve does: name equality OR ID prefix, with no exact-match priority.
    // Finish planning every target before sending the first mutation.
    if bots.iter().any(|bot| {
        bot["bot_id"].as_str().is_none_or(|id| id.is_empty())
            || bot["name"].as_str().is_none_or(|name| name.is_empty())
    }) {
        return Err("invalid bot list; nothing was changed".into());
    }
    let mut targets = Vec::new();
    let mut skipped = Vec::new();
    for (index, bot) in bots.iter().enumerate() {
        let bot_id = bot["bot_id"].as_str().expect("validated Bot ID");
        let name = bot["name"].as_str().expect("validated Bot name");
        if bot["status"].as_str() == Some("not_local") {
            skipped.push(json!({"bot_id": bot_id, "name": name, "reason": "not_local"}));
        } else {
            let selector = [name, bot_id]
                .into_iter()
                .find(|query| {
                    let mut matches = bots.iter().enumerate().filter(|(_, candidate)| {
                        candidate["name"].as_str() == Some(*query)
                            || candidate["bot_id"]
                                .as_str()
                                .is_some_and(|id| id.starts_with(*query))
                    });
                    matches.next().is_some_and(|(matched, _)| matched == index)
                        && matches.next().is_none()
                })
                .ok_or_else(|| {
                    format!(
                        "no unambiguous selector for Bot {name} ({bot_id}); give Bots unique names that do not collide with ID prefixes; nothing was changed"
                    )
                })?;
            targets.push(AllTarget {
                bot_id: bot_id.to_owned(),
                name: name.to_owned(),
                selector: selector.to_owned(),
            });
        }
    }
    Ok((targets, skipped))
}

/// Run `run_one` for each target in order and stop at the first failure. A Bot is finished
/// (its operation reached a terminal state) before the next one starts, so the Identify
/// budget and the per-Bot restart holds are never overlapped across Bots.
async fn drive_all<F, Fut>(targets: &[AllTarget], mut run_one: F) -> (Vec<Value>, Option<String>)
where
    F: FnMut(AllTarget) -> Fut,
    Fut: std::future::Future<Output = Value>,
{
    let mut results = Vec::new();
    let mut stopped_at = None;
    for target in targets {
        let response = run_one(target.clone()).await;
        let state = response["operation"]["state"].as_str();
        let ok = response["ok"] == true && state.is_none_or(|state| state == "succeeded");
        results.push(json!({
            "bot_id": target.bot_id,
            "name": target.name,
            "ok": ok,
            "state": state,
            "operation_id": response["operation"]["operation_id"]
                .as_str()
                .or_else(|| response["operation_id"].as_str()),
            "error": response["error"],
            "operation": response["operation"],
            "shards": response["operation"]["shards"],
            "failed_shards": response["operation"]["failed_shards"],
        }));
        if !ok {
            stopped_at = Some(target.bot_id.clone());
            break;
        }
    }
    (results, stopped_at)
}

async fn run_all(options: Options) -> Value {
    let started = Instant::now();
    let list_deadline = Instant::now() + Duration::from_secs(30);
    let bootstrap = match timeout_at(
        list_deadline,
        load_bootstrap(options.secret_file.as_deref(), options.port),
    )
    .await
    {
        Ok(Ok(bootstrap)) => bootstrap,
        Ok(Err(error)) => return failure(error, None, None),
        Err(_) => return failure("control bootstrap timed out; no request sent", None, None),
    };
    run_all_loaded(options, &bootstrap, started, list_deadline).await
}

// Keep the one-shot stdin capability alive for the complete batch; per-Bot calls
// must neither reread stdin nor silently switch credentials between targets.
async fn run_all_loaded(
    mut options: Options,
    bootstrap: &Bootstrap,
    started: Instant,
    list_deadline: Instant,
) -> Value {
    if options.command == "restart" && !options.all_dry_run {
        // The daemon reserves the complete local batch in one request. It
        // selects the live targets itself, so no listing can go stale.
        options.command = "restart-all";
        let deadline = Instant::now() + options.timeout;
        let mut result = run_live(options, None, deadline, Some(bootstrap)).await;
        result["all"] = json!(true);
        result["command"] = json!("restart");
        return result;
    }
    let listing = Request {
        secret: &bootstrap.secret,
        command: "ps",
        bot: None,
        operation_id: None,
        force: false,
        yes: false,
        shards: None,
        expect_total_shards: None,
        expect_bot_id: None,
    };
    let response = match timeout_at(list_deadline, exchange(bootstrap, &listing)).await {
        Ok(Ok(response)) => response,
        Ok(Err(error)) => return failure(error, None, None),
        Err(_) => return failure("bot listing timed out; nothing was changed", None, None),
    };
    let Some(bots) = response["bots"]
        .as_array()
        .filter(|_| response["ok"] == true)
    else {
        return failure(
            "daemon did not return a bot list; nothing was changed",
            None,
            None,
        );
    };
    let (targets, skipped) = match plan_all(bots) {
        Ok(plan) => plan,
        Err(error) => return failure(&error, None, None),
    };
    let listed: Vec<Value> = targets
        .iter()
        .map(|target| json!({"bot_id": target.bot_id, "name": target.name}))
        .collect();
    if options.all_dry_run {
        return json!({"ok": true, "offline": false, "all": true, "dry_run": true,
            "command": options.command, "targets": listed, "skipped": skipped});
    }
    if targets.is_empty() {
        let mut value = failure("no bot has local shards; nothing was changed", None, None);
        value["all"] = json!(true);
        value["skipped"] = json!(skipped);
        return value;
    }
    let (results, stopped_at) = drive_all(&targets, |target| {
        let mut one = options.clone();
        one.all = false;
        one.all_dry_run = false;
        one.bot = Some(target.selector);
        one.expect_bot_id = Some(target.bot_id);
        // `--yes` here confirmed `--all` itself; a Bot only receives it for a forced stop.
        one.yes = one.force;
        one.operation_id = None;
        let deadline = Instant::now() + one.timeout;
        async move { run_live(one, None, deadline, Some(bootstrap)).await }
    })
    .await;
    let completed = results.iter().filter(|result| result["ok"] == true).count();
    json!({"ok": stopped_at.is_none(), "offline": false, "all": true,
        "command": options.command, "total": targets.len(), "completed": completed,
        "stopped_at": stopped_at, "results": results, "skipped": skipped,
        "elapsed_ms": started.elapsed().as_millis() as u64})
}

/// The Shard that Discord routes a guild's events to.
fn shard_for_guild(guild_id: u64, total_shards: u64) -> u32 {
    // `total_shards` is at least 1 and a u32 in every validated configuration.
    ((guild_id >> 22) % total_shards.max(1)) as u32
}

async fn run_live(
    options: Options,
    update: Option<UpdateControlRequest>,
    deadline: Instant,
    bootstrap: Option<&Bootstrap>,
) -> Value {
    let mut resolved: Vec<(u64, u32)> = Vec::new();
    let mut value = run_live_inner(options, update, deadline, &mut resolved, bootstrap).await;
    if !resolved.is_empty() {
        value["resolved_guilds"] = resolved
            .iter()
            .map(|(guild, shard)| json!({"guild_id": guild.to_string(), "shard_id": shard}))
            .collect();
    }
    value
}

async fn run_live_inner(
    mut options: Options,
    update: Option<UpdateControlRequest>,
    deadline: Instant,
    resolved: &mut Vec<(u64, u32)>,
    preloaded: Option<&Bootstrap>,
) -> Value {
    let mutation = matches!(
        options.command,
        "start" | "stop" | "restart" | "restart-all" | "update" | "rollback" | "gateway-update"
    );
    if mutation && options.operation_id.is_none() {
        options.operation_id = match new_operation_id() {
            Ok(id) => Some(id),
            Err(error) => return failure(error, None, None),
        };
    }
    let id = options.operation_id.as_deref();
    let loaded;
    let bootstrap = match preloaded {
        Some(bootstrap) => bootstrap,
        None => {
            loaded = match timeout_at(
                deadline,
                load_bootstrap(options.secret_file.as_deref(), options.port),
            )
            .await
            {
                Ok(Ok(bootstrap)) => bootstrap,
                Ok(Err(error)) => return failure(error, id, None),
                Err(_) => return failure("control bootstrap timed out; no request sent", id, None),
            };
            &loaded
        }
    };
    let mut expect_total_shards = None;
    if !options.guilds.is_empty() {
        // Resolve guilds to Shards from the daemon's own total before anything is mutated.
        let inspect = Request {
            secret: &bootstrap.secret,
            command: "inspect",
            bot: options.bot.as_deref(),
            operation_id: None,
            force: false,
            yes: false,
            shards: None,
            expect_total_shards: None,
            expect_bot_id: None,
        };
        let response = match timeout_at(deadline, exchange(bootstrap, &inspect)).await {
            Ok(Ok(response)) => response,
            Ok(Err(error)) => return failure(error, id, None),
            Err(_) => {
                return failure("guild resolution timed out; no mutation sent", id, None);
            }
        };
        let Some(total) = response["bot"]["total_shards"]
            .as_u64()
            .filter(|_| response["ok"] == true)
            .and_then(|total| u32::try_from(total).ok())
            .filter(|total| *total > 0)
        else {
            return failure(
                "daemon did not report total shards; no mutation sent",
                id,
                None,
            );
        };
        expect_total_shards = Some(total);
        for guild in options.guilds.clone() {
            let shard = shard_for_guild(guild, u64::from(total));
            resolved.push((guild, shard));
            if !options.shards.contains(&shard) {
                options.shards.push(shard);
            }
        }
    }
    let id = options.operation_id.as_deref();
    if let Some(update) = &update {
        // Check the entire authenticated payload before even a read-only query.
        if let Err(error) = encode_request(&DaemonControlEnvelope {
            secret: bootstrap.secret,
            request: update,
        }) {
            return failure(error, id, None);
        }
        let inspect = Request {
            secret: &bootstrap.secret,
            command: "inspect",
            bot: options.bot.as_deref(),
            operation_id: None,
            force: false,
            yes: false,
            shards: None,
            expect_total_shards: None,
            expect_bot_id: None,
        };
        let response = match timeout_at(deadline, exchange(bootstrap, &inspect)).await {
            Ok(Ok(response)) => response,
            Ok(Err(error)) => return failure(error, id, None),
            Err(_) => {
                return failure(
                    "update target inspection timed out; no mutation sent",
                    id,
                    None,
                );
            }
        };
        if response["ok"] != true {
            return failure(
                "daemon rejected update target inspection; no mutation sent",
                id,
                None,
            );
        }
        if response["bot"]["bot_id"].as_str() != Some(update.bot.as_str())
            || response["bot"]["total_shards"].as_u64()
                != Some(u64::from(update.manifest.total_shards))
        {
            return failure(
                "daemon update target does not match manifest; no mutation sent",
                id,
                None,
            );
        }
    }
    let mut request = Request {
        secret: &bootstrap.secret,
        command: options.command,
        bot: options.bot.as_deref(),
        operation_id: id,
        force: options.force,
        yes: options.yes,
        shards: (!options.shards.is_empty()).then_some(options.shards.as_slice()),
        expect_total_shards,
        expect_bot_id: options.expect_bot_id.as_deref(),
    };
    let mut last = None;
    let mut send_update = update.is_some();
    loop {
        let exchange_result = timeout_at(deadline, async {
            if send_update {
                exchange(
                    bootstrap,
                    &DaemonControlEnvelope {
                        secret: bootstrap.secret,
                        request: update.as_ref().expect("update payload"),
                    },
                )
                .await
            } else {
                exchange(bootstrap, &request).await
            }
        })
        .await;
        let mut response = match exchange_result {
            Ok(Ok(response)) => response,
            Ok(Err(error)) => return failure(error, id, last.as_ref()),
            Err(_) => {
                return failure(
                    "daemon request timed out; outcome is unknown; query the operation ID",
                    id,
                    last.as_ref(),
                );
            }
        };
        // Errors without an operation (e.g. an ambiguous Bot selector) remain
        // errors; never infer a successful mutation from request acceptance.
        if response["ok"] == false && response.get("operation").is_none() {
            if let Some(id) = id {
                response["operation_id"] = json!(id);
            }
            return response;
        }
        if mutation || options.command == "operation" {
            let id = id.expect("validated or generated operation ID");
            response["operation_id"] = json!(id);
            let state = operation_state(&response, id, options.command);
            if state.is_ok()
                && let Err(error) = validate_update_result(&response, update.as_ref())
            {
                return failure(error, Some(id), last.as_ref());
            }
            if options.command == "operation" && response["operation"]["command"] == "core-restart"
            {
                if let Err(error) = state {
                    return failure(error, Some(id), None);
                }
                let operation = core_operation_fields(&response["operation"]);
                response = json!({
                    "ok": response["ok"], "offline": false, "operation_id": id,
                    "operation": operation, "note": CORE_RESTART_NOTE,
                });
                if operation["state"] == "running" {
                    response["ok"] = json!(false);
                    response["pending"] = json!(true);
                    response["error"] =
                        json!("operation is still running; query the operation ID again");
                }
                return response;
            }
            match state {
                Ok("succeeded") if response["ok"] == true => return response,
                Ok("failed" | "succeeded" | "unknown") => {
                    response["ok"] = json!(false);
                    return response;
                }
                Ok("running") if response["ok"] == false => return response,
                Ok("running") => {
                    last = Some(response["operation"].clone());
                    if options.command == "operation" {
                        response["ok"] = json!(false);
                        response["pending"] = json!(true);
                        response["error"] =
                            json!("operation is still running; query the operation ID again");
                        return response;
                    }
                }
                Ok(_) => unreachable!("validated operation state"),
                Err(error) => return failure(error, Some(id), last.as_ref()),
            }
            if timeout_at(deadline, tokio::time::sleep(Duration::from_millis(25)))
                .await
                .is_err()
            {
                return failure(
                    "operation wait timed out; operation may continue; query the operation ID",
                    Some(id),
                    last.as_ref(),
                );
            }
            // Only query after sending a mutation once. A timeout or lost reply
            // never causes automatic resubmission of the operation.
            request.command = "operation";
            request.bot = None;
            request.force = false;
            request.yes = false;
            request.shards = None;
            request.expect_total_shards = None;
            request.expect_bot_id = None;
            send_update = false;
        } else {
            let valid = match options.command {
                "ps" => response["bots"].is_array(),
                "status" => response["bots"].is_array() || response["bot"].is_object(),
                "inspect" => response["bot"].is_object(),
                "shards" => response["shards"].is_array(),
                _ => false,
            };
            return if valid {
                response
            } else {
                failure("invalid daemon status response", None, None)
            };
        }
    }
}

async fn run_update(mut options: Options) -> Value {
    let started = Instant::now();
    let deadline = started + options.timeout;
    let path = options
        .plan
        .as_ref()
        .expect("validated manifest path")
        .clone();
    let component = options.component;
    let selector = options.bot.clone().expect("validated Bot selector");
    let action = if options.command == "update" {
        Action::Update
    } else {
        Action::Rollback
    };
    // Hashing runs outside the async runtime so --timeout bounds even large files.
    // Cancellation may finish read-only validation in the background; it can never
    // start a candidate or send a daemon mutation.
    let validation = tokio::task::spawn_blocking(move || {
        let mut manifest = update_rollback::load_manifest(&path)?;
        update_rollback::build_plan(&manifest, &selector, action, component, started.elapsed())?;
        update_rollback::verify_artifacts(
            &manifest,
            component,
            path.parent().unwrap_or(std::path::Path::new(".")),
        )?;
        // The daemon is on this host but may have a different working directory.
        // It must still rehash the selected artifacts immediately before launch.
        for (selected, artifact) in [
            (
                component != Component::Gateway,
                &mut manifest.artifacts.core,
            ),
            (
                component != Component::Core,
                &mut manifest.artifacts.gateway,
            ),
        ] {
            if selected {
                let artifact = artifact.as_mut().ok_or("missing selected artifact")?;
                artifact.path = std::fs::canonicalize(
                    path.parent()
                        .unwrap_or(std::path::Path::new("."))
                        .join(&artifact.path),
                )
                .map_err(|_| "cannot resolve artifact path")?;
            }
        }
        let plan = update_rollback::build_plan(
            &manifest,
            &selector,
            action,
            component,
            started.elapsed(),
        )?;
        Ok::<_, &'static str>((plan, manifest))
    });
    let (plan, manifest) = match timeout_at(deadline, validation).await {
        Ok(Ok(Ok(result))) => result,
        Ok(Ok(Err(error))) => {
            return json!({"ok":false,"offline":true,"applied":false,"error":error});
        }
        Ok(Err(_)) => {
            return json!({"ok":false,"offline":true,"applied":false,"error":"update validation failed"});
        }
        Err(_) => {
            return json!({"ok":false,"offline":true,"applied":false,"error":"update validation timed out; no request sent"});
        }
    };
    let allowed = plan.executable();
    let mut value = json!({"ok": allowed, "offline": true, "dry_run": !options.yes, "applied": false, "plan": plan});
    if !allowed {
        value["error"] =
            json!("update/rollback plan is deferred or rejected; keep old generations active");
        return value;
    }
    if !options.yes {
        return value;
    }
    let id = match options
        .operation_id
        .clone()
        .map(Ok)
        .unwrap_or_else(new_operation_id)
    {
        Ok(id) => id,
        Err(error) => return failure(error, None, None),
    };
    let update = UpdateControlRequest {
        command: action,
        bot: manifest.bot_id.clone(),
        operation_id: id.clone(),
        yes: true,
        component,
        manifest,
    };
    if let Err(error) = update.validate() {
        return failure(error, Some(&id), None);
    }
    options.operation_id = Some(id);
    run_live(options, Some(update), deadline, None).await
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let args: Vec<_> = std::env::args_os().skip(1).collect();
    let json_output = args.iter().any(|arg| arg == "--json");
    let (value, core_restart) = match parse_options(&args) {
        Ok(options) => {
            let core_restart = options.command == "core-restart";
            (run(options).await, core_restart)
        }
        Err(error) => (failure(error, None, None), false),
    };
    let success = value["ok"] == true;
    let text = serde_json::to_string_pretty(&value).expect("JSON value");
    if success || json_output {
        println!("{text}");
    } else {
        eprintln!("botctl: {text}");
    }
    // A timed-out Tokio stdin read can leave a blocking reader waiting for the
    // launcher. Exiting explicitly also bounds that case on both supported OSes.
    let code = if core_restart {
        match value["state"].as_str() {
            Some("succeeded") => 0,
            Some("rejected") => 1,
            Some("failed") => 2,
            Some("timeout") => 4,
            _ => 3,
        }
    } else if success {
        0
    } else {
        1
    };
    std::process::exit(code);
}

#[cfg(test)]
mod default_wait_tests {
    use super::*;

    fn options(args: &[&str]) -> Options {
        let args: Vec<OsString> = args.iter().map(OsString::from).collect();
        parse_options(&args).expect("valid arguments")
    }

    #[test]
    fn core_commands_require_a_single_shard_and_only_restart_accepts_a_pin() {
        let restart = options(&[
            "core",
            "restart",
            "alpha",
            "--shard",
            "1",
            "--core",
            "0",
            "--expect-incarnation",
            "ABCDEF0123456789",
            "--force",
            "--bootstrap-stdin",
        ]);
        assert_eq!(restart.command, "core-restart");
        assert_eq!(restart.shards, [1]);
        assert_eq!(restart.core, Some(0));
        assert_eq!(
            restart.expect_incarnation.as_deref(),
            Some("abcdef0123456789")
        );
        assert!(restart.force);
        assert!(!restart.yes);
        for args in [
            &["core", "restart", "alpha", "--shard", "0"][..],
            &["core", "status", "alpha"],
            &["core", "status", "alpha", "--shard", "0", "--shard", "1"],
            &["core", "status", "alpha", "--shard", "0", "--core", "0"],
            &[
                "core",
                "restart",
                "alpha",
                "--shard",
                "0",
                "--core",
                "0",
                "--expect-incarnation",
                "0000000000000000",
            ],
            &["restart", "alpha", "--core", "0"],
        ] {
            let mut args: Vec<OsString> = args.iter().map(OsString::from).collect();
            args.push("--bootstrap-stdin".into());
            assert!(parse_options(&args).is_err());
        }
    }

    #[tokio::test]
    async fn core_restart_queries_after_an_ambiguous_reply_and_pins_an_empty_slot() {
        for state in ["failed", "unknown", "timeout"] {
            let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
                .await
                .unwrap();
            let bootstrap = Bootstrap {
                port: listener.local_addr().unwrap().port(),
                secret: [7; 32],
            };
            let preview = validate_core_preview(
                json!({"bot_id": "101", "total_shards": 2, "shard": 1, "cores": [
                    {"running": false, "empty": true, "incarnation": null, "status": null},
                    {"running": true, "empty": false, "incarnation": "fedcba9876543210", "status": null},
                ]}),
                1,
            )
            .unwrap();
            let server = tokio::spawn(async move {
                let (mut mutation, _) = listener.accept().await.unwrap();
                let frame = read_frame(&mut mutation, REQUEST_LIMIT).await.unwrap();
                let request: Value = serde_json::from_slice(&frame).unwrap();
                assert_eq!(request["command"], "core-restart");
                assert_eq!(request["bot"], "alpha");
                assert_eq!(request["expect_bot_id"], "101");
                assert_eq!(request["expect_total_shards"], 2);
                assert_eq!(request["shard"], 1);
                assert_eq!(request["core"], 0);
                assert_eq!(request["core_recovery_version"], CORE_RECOVERY_VERSION);
                assert_eq!(request["expect_empty"], true);
                assert!(request.get("expect_incarnation").is_none());
                let id = request["operation_id"].as_str().unwrap();
                assert!(valid_operation_id(id));
                if state == "failed" {
                    mutation
                        .write_all(b"{\"ok\":false,\"error\":\"shard_not_running\"}\n")
                        .await
                        .unwrap();
                }
                drop(mutation); // The other cases lose the reply after the request arrives.
                let (mut query, _) = listener.accept().await.unwrap();
                let frame = read_frame(&mut query, REQUEST_LIMIT).await.unwrap();
                let request: Value = serde_json::from_slice(&frame).unwrap();
                assert_eq!(request["command"], "operation");
                assert_eq!(request["operation_id"], id);
                assert!(request.get("expect_empty").is_none());
                if state == "timeout" {
                    assert_eq!(query.read(&mut [0]).await.unwrap(), 0);
                    return;
                }
                let (reason, audit) = if state == "failed" {
                    ("shard_not_running", "synced")
                } else {
                    ("terminal_audit_missing", "missing")
                };
                let reply = json!({"ok": false, "note": "/private/path", "operation": {
                    "operation_id": id, "command": "core-restart", "bot_id": "101",
                    "core_recovery_version": CORE_RECOVERY_VERSION, "source": "archive",
                    "shard": 1, "core": 0, "state": state, "terminal_audit": audit,
                    "incarnation": null, "reason": reason
                }});
                query
                    .write_all(format!("{reply}\n").as_bytes())
                    .await
                    .unwrap();
            });
            let result = restart_core(
                &bootstrap,
                "alpha",
                &preview,
                0,
                false,
                Instant::now() + Duration::from_secs(2),
            )
            .await;
            assert_eq!(result["state"], state, "{result}");
            assert_eq!(result["ok"], false);
            assert!(
                result["operation_id"]
                    .as_str()
                    .is_some_and(valid_operation_id)
            );
            assert!(!result.to_string().contains("/private/path"));
            tokio::time::timeout(Duration::from_secs(2), server)
                .await
                .unwrap()
                .unwrap();
        }
    }

    #[test]
    fn operation_query_accepts_core_recovery_results_and_the_crash_unknown_state() {
        let mut value = json!({"ok": false, "operation": {
            "operation_id": "id", "command": "core-restart", "bot_id": "7",
            "core_recovery_version": CORE_RECOVERY_VERSION, "source": "archive",
            "shard": 1, "core": 0, "state": "unknown", "terminal_audit": "missing",
            "incarnation": "fedcba9876543210", "reason": "terminal_audit_missing"
        }});
        assert_eq!(operation_state(&value, "id", "operation"), Ok("unknown"));
        assert!(operation_state(&value, "wrong-id", "operation").is_err());
        assert!(operation_state(&value, "id", "restart").is_err());
        value["operation"]["terminal_audit"] = json!("synced");
        assert!(operation_state(&value, "id", "operation").is_err());
        value["operation"]["state"] = json!("succeeded");
        value["operation"]["reason"] = Value::Null;
        value["ok"] = Value::Bool(true);
        assert_eq!(operation_state(&value, "id", "operation"), Ok("succeeded"));
        value["operation"]["shard"] = Value::Null;
        assert!(operation_state(&value, "id", "operation").is_err());
    }

    #[test]
    fn core_recovery_requires_versioned_live_or_archived_result_shapes() {
        let key = json!({"bot_id": "7", "shard_id": 1});
        let live = json!({"ok": true, "operation": {
            "operation_id": "id", "command": "core-restart", "bot_id": "7",
            "core_recovery_version": CORE_RECOVERY_VERSION, "source": "live",
            "shard": 1, "core": 0, "state": "succeeded", "terminal_audit": "synced",
            "incarnation": null, "reason": null, "elapsed_ms": 5, "failed_shards": [],
            "shards": [{
                "key": key, "total_shards": 2, "state": {"state": "Running"},
                "desired_running": true, "identity": null, "restarts": 0,
                "retry_at_ms": null, "restart_exhausted": false, "error": null
            }]
        }});
        assert_eq!(operation_state(&live, "id", "operation"), Ok("succeeded"));
        for (field, value) in [
            ("core_recovery_version", json!(1)),
            ("source", json!("archive")),
            ("failed_shards", json!({})),
            ("failed_shards", json!([key])),
            ("shards", json!([])),
            ("terminal_audit", json!("missing")),
            ("incarnation", json!("/foreign/")),
            ("reason", json!("launch_failed")),
        ] {
            let mut bad = live.clone();
            bad["operation"][field] = value;
            assert!(operation_state(&bad, "id", "operation").is_err(), "{field}");
        }
        let mut old = live.clone();
        old["operation"]
            .as_object_mut()
            .unwrap()
            .remove("core_recovery_version");
        assert!(operation_state(&old, "id", "operation").is_err());

        let mut archived = live.clone();
        for field in ["elapsed_ms", "shards", "failed_shards"] {
            archived["operation"].as_object_mut().unwrap().remove(field);
        }
        archived["operation"]["source"] = json!("archive");
        assert_eq!(
            operation_state(&archived, "id", "operation"),
            Ok("succeeded")
        );
        archived["operation"]["failed_shards"] = Value::Null;
        assert!(operation_state(&archived, "id", "operation").is_err());
    }

    #[test]
    fn shard_filter_is_parsed_validated_and_limited_to_lifecycle_commands() {
        let base = ["--secret-file", "/tmp/s", "--port", "1"];
        let with = |extra: &[&str]| -> Result<Options, &'static str> {
            let args: Vec<OsString> = [extra, &base[..]]
                .concat()
                .iter()
                .map(OsString::from)
                .collect();
            parse_options(&args)
        };
        let one = with(&["gateway-update", "bot", "--shard", "1"]).unwrap();
        assert_eq!(one.shards, vec![1]);
        let two = with(&["restart", "bot", "--shard", "2", "--shard", "0"]).unwrap();
        assert_eq!(two.shards, vec![2, 0]);
        assert!(with(&["gateway-update", "bot"]).unwrap().shards.is_empty());
        for bad in [
            vec!["gateway-update", "bot", "--shard"],
            vec!["gateway-update", "bot", "--shard", "x"],
            vec!["gateway-update", "bot", "--shard", "-1"],
            vec!["gateway-update", "bot", "--shard", "99999999999"],
            vec!["gateway-update", "bot", "--shard", "1", "--shard", "1"],
            vec!["status", "bot", "--shard", "1"],
            vec!["ps", "--shard", "1"],
        ] {
            assert!(with(&bad).is_err(), "{bad:?} must be rejected");
        }
    }

    #[test]
    fn guild_ids_resolve_to_discord_shards_and_are_validated() {
        // The two guilds used in the real two-shard check: one per Shard for 2 shards.
        assert_eq!(shard_for_guild(1_533_441_257_798_369_401, 2), 0);
        assert_eq!(shard_for_guild(1_569_994_280_854_884_364, 2), 1);
        assert_eq!(shard_for_guild(1_569_994_280_854_884_364, 4), 3);
        assert_eq!(shard_for_guild(1_569_994_280_854_884_364, 1), 0);
        let base = ["--secret-file", "/tmp/s", "--port", "1"];
        let with = |extra: &[&str]| -> Result<Options, &'static str> {
            let args: Vec<OsString> = [extra, &base[..]]
                .concat()
                .iter()
                .map(OsString::from)
                .collect();
            parse_options(&args)
        };
        let ok = with(&["gateway-update", "bot", "--guild", "1533441257798369401"]).unwrap();
        assert_eq!(ok.guilds, vec![1_533_441_257_798_369_401]);
        for bad in [
            vec!["gateway-update", "bot", "--guild"],
            vec!["gateway-update", "bot", "--guild", "abc"],
            vec!["gateway-update", "bot", "--guild", "0"],
            vec![
                "gateway-update",
                "bot",
                "--guild",
                "99999999999999999999999",
            ],
            vec!["gateway-update", "bot", "--guild", "5", "--guild", "5"],
            vec!["status", "bot", "--guild", "5"],
        ] {
            assert!(with(&bad).is_err(), "{bad:?} must be rejected");
        }
    }

    #[test]
    fn all_flag_needs_confirmation_and_excludes_per_bot_targeting() {
        let base = ["--secret-file", "/tmp/s", "--port", "1"];
        let with = |extra: &[&str]| -> Result<Options, &'static str> {
            let args: Vec<OsString> = [extra, &base[..]]
                .concat()
                .iter()
                .map(OsString::from)
                .collect();
            parse_options(&args)
        };
        let run = with(&["gateway-update", "--all", "--yes"]).unwrap();
        assert!(run.all && !run.all_dry_run && run.bot.is_none());
        assert_eq!(run.timeout, Duration::from_secs(300));
        let dry = with(&["restart", "--all", "--dry-run"]).unwrap();
        assert!(dry.all && dry.all_dry_run);
        assert!(with(&["stop", "--all", "--force", "--yes"]).unwrap().force);
        let preview = with(&["stop", "--all", "--force", "--dry-run"]).unwrap();
        assert!(preview.all_dry_run && preview.force && !preview.yes);
        for bad in [
            vec!["stop", "--all", "--force"],
            vec!["stop", "bot", "--force", "--dry-run"],
            vec!["stop", "--all", "--force", "--dry-run", "--yes"],
            vec!["restart", "--all"],
            vec!["restart", "--all", "--yes", "--dry-run"],
            vec!["restart", "bot", "--all", "--yes"],
            vec!["gateway-update", "--all", "--yes", "--shard", "0"],
            vec!["gateway-update", "--all", "--yes", "--guild", "5"],
            vec![
                "gateway-update",
                "--all",
                "--yes",
                "--operation-id",
                "00000000000000000000000000000001",
            ],
            vec!["status", "--all", "--yes"],
            vec!["restart", "--yes"],
            vec!["restart"],
        ] {
            assert!(with(&bad).is_err(), "{bad:?} must be rejected");
        }
    }

    #[test]
    fn all_targets_skip_bots_without_local_shards_and_say_so() {
        let bots = vec![
            json!({"bot_id": "1", "name": "a", "status": "running"}),
            json!({"bot_id": "2", "name": "b", "status": "not_local"}),
            json!({"bot_id": "3", "name": "c", "status": "degraded"}),
        ];
        let (targets, skipped) = plan_all(&bots).unwrap();
        assert_eq!(
            targets
                .iter()
                .map(|t| t.bot_id.as_str())
                .collect::<Vec<_>>(),
            ["1", "3"]
        );
        assert_eq!(skipped.len(), 1);
        assert_eq!(skipped[0]["bot_id"], "2");
        assert_eq!(skipped[0]["reason"], "not_local");
    }

    #[tokio::test]
    async fn all_runs_bots_in_order_and_stops_at_the_first_failure() {
        let targets: Vec<AllTarget> = ["1", "2", "3"]
            .iter()
            .map(|id| AllTarget {
                bot_id: (*id).into(),
                name: format!("bot{id}"),
                selector: format!("bot{id}"),
            })
            .collect();
        let seen = std::sync::Mutex::new(Vec::new());
        let (results, stopped) = drive_all(&targets, |target| {
            seen.lock().unwrap().push(target.bot_id.clone());
            async move {
                if target.bot_id == "2" {
                    // Real daemon failures carry their diagnosis inside the operation.
                    json!({"ok": false, "operation": {
                        "state": "failed", "operation_id": "x",
                        "shards": [{"process": {"key": {"bot_id": "2", "shard_id": 0}}, "error": null},
                            {"process": {"key": {"bot_id": "2", "shard_id": 1}}, "error": "StopTimedOut"}],
                        "failed_shards": [{"bot_id": "2", "shard_id": 1}]
                    }})
                } else {
                    json!({"ok": true, "operation": {"state": "succeeded", "operation_id": "y"}})
                }
            }
        })
        .await;
        assert_eq!(*seen.lock().unwrap(), ["1", "2"], "bot 3 must never start");
        assert_eq!(stopped.as_deref(), Some("2"));
        assert_eq!(results.len(), 2);
        assert_eq!(results[0]["ok"], true);
        assert_eq!(results[1]["ok"], false);
        assert_eq!(results[1]["operation_id"], "x");
        assert_eq!(results[1]["shards"][0]["error"], Value::Null);
        assert_eq!(results[1]["shards"][1]["error"], "StopTimedOut");
        assert_eq!(
            results[1]["failed_shards"],
            json!([{"bot_id": "2", "shard_id": 1}])
        );
        assert_eq!(results[1]["operation"]["shards"], results[1]["shards"]);

        // Admission failures have no operation snapshot; keep their original error too.
        let (results, stopped) = drive_all(&targets, |_| async {
            json!({"ok": false, "error": "boom", "operation_id": "x"})
        })
        .await;
        assert_eq!(results.len(), 1);
        assert_eq!(stopped.as_deref(), Some("1"));
        assert_eq!(results[0]["error"], "boom");

        // A response that says ok but whose operation did not succeed also stops the run.
        let (results, stopped) = drive_all(&targets, |_| async {
            json!({"ok": true, "operation": {"state": "failed", "operation_id": "z"}})
        })
        .await;
        assert_eq!(results.len(), 1);
        assert_eq!(stopped.as_deref(), Some("1"));
        // All succeed: everything ran, nothing stopped.
        let (results, stopped) = drive_all(&targets, |_| async {
            json!({"ok": true, "operation": {"state": "succeeded", "operation_id": "z"}})
        })
        .await;
        assert_eq!((results.len(), stopped), (3, None));
    }

    #[test]
    fn all_plans_unambiguous_names_before_id_prefixes() {
        let bots = vec![
            json!({"bot_id": "101", "name": "alpha"}),
            json!({"bot_id": "1010", "name": "beta"}),
        ];
        let (targets, _) = plan_all(&bots).unwrap();
        assert_eq!(targets[0].selector, "alpha");
        assert_eq!(targets[1].selector, "beta");
        assert_eq!(targets[0].bot_id, "101");
        assert_eq!(targets[1].bot_id, "1010");

        // A name may collide with another Bot's ID prefix; try the full ID next.
        let bots = vec![
            json!({"bot_id": "1010", "name": "101"}),
            json!({"bot_id": "101", "name": "beta"}),
        ];
        let (targets, _) = plan_all(&bots).unwrap();
        assert_eq!(targets[0].selector, "1010");
        assert_eq!(targets[1].selector, "beta");
    }

    #[test]
    fn all_refuses_the_entire_plan_when_a_later_target_has_no_safe_selector() {
        let bots = vec![
            json!({"bot_id": "9", "name": "safe"}),
            json!({"bot_id": "101", "name": "101"}),
            // Even skipped Bots participate in the daemon's selector resolution.
            json!({"bot_id": "1010", "name": "remote", "status": "not_local"}),
        ];
        let error = plan_all(&bots).unwrap_err();
        assert!(error.contains("Bot 101 (101)"));
        assert!(error.contains("nothing was changed"));
        assert!(plan_all(&[json!({"name": "no-id"})]).is_err());
    }

    #[tokio::test]
    async fn all_reuses_preloaded_bootstrap_for_every_bot() {
        for (command, force) in [("start", false), ("stop", true)] {
            let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
                .await
                .unwrap();
            let bootstrap = Bootstrap {
                port: listener.local_addr().unwrap().port(),
                secret: [7; 32],
            };
            let deadline = Instant::now() + Duration::from_secs(3);
            let server = tokio::spawn(async move {
                let mut ids = Vec::new();
                for index in 0..5 {
                    let (mut stream, _) = listener.accept().await.unwrap();
                    let frame = read_frame(&mut stream, REQUEST_LIMIT).await.unwrap();
                    let request: Value = serde_json::from_slice(&frame).unwrap();
                    assert_eq!(request["secret"], json!(vec![7_u8; 32]));
                    let reply = if index == 0 {
                        assert_eq!(request["command"], "ps");
                        assert!(request.get("expect_bot_id").is_none());
                        json!({"ok": true, "bots": [
                            {"bot_id": "101", "name": "one", "status": "stopped"},
                            {"bot_id": "1010", "name": "two", "status": "stopped"}
                        ]})
                    } else {
                        let first_bot = index <= 2;
                        let bot_id = if first_bot { "101" } else { "1010" };
                        let id = request["operation_id"].as_str().unwrap().to_owned();
                        assert!(valid_operation_id(&id));
                        let state = if index % 2 == 1 {
                            assert_eq!(request["command"], command);
                            assert_eq!(request["bot"], if first_bot { "one" } else { "two" });
                            assert_eq!(request["expect_bot_id"], bot_id);
                            assert_eq!(request["force"], force);
                            assert_eq!(request["yes"], force);
                            ids.push(id.clone());
                            "running"
                        } else {
                            assert_eq!(request["command"], "operation");
                            assert!(request.get("bot").is_none());
                            assert!(request.get("expect_bot_id").is_none());
                            assert_eq!(ids.last(), Some(&id));
                            "succeeded"
                        };
                        json!({"ok": true, "operation": {
                            "operation_id": id, "command": command, "bot_id": bot_id,
                            "state": state, "elapsed_ms": 0, "shards": [], "failed_shards": []
                        }})
                    };
                    let mut frame = serde_json::to_vec(&reply).unwrap();
                    frame.push(b'\n');
                    stream.write_all(&frame).await.unwrap();
                }
                assert_ne!(ids[0], ids[1]);
            });
            let mut args = vec![command, "--all", "--yes", "--bootstrap-stdin"];
            if force {
                args.push("--force");
            }
            let result = timeout_at(
                deadline,
                run_all_loaded(
                    preloaded_options(&args, bootstrap.port),
                    &bootstrap,
                    Instant::now(),
                    deadline,
                ),
            )
            .await
            .unwrap();
            assert_eq!(result["ok"], true, "{result}");
            assert_eq!(result["completed"], 2);
            timeout_at(deadline, server).await.unwrap().unwrap();
        }
    }

    fn preloaded_options(args: &[&str], port: u16) -> Options {
        let mut options = options(args);
        // An accidental reload must fail on a missing file rather than block stdin.
        // The injected Bootstrap is the only usable credential source in these tests.
        options.secret_file = Some(std::env::temp_dir().join(format!(
            "missing-botctl-bootstrap-{}",
            new_operation_id().unwrap()
        )));
        options.port = Some(port);
        options
    }

    #[tokio::test]
    async fn guild_resolution_sends_precondition_only_on_the_mutation() {
        let listener = tokio::net::TcpListener::bind(("127.0.0.1", 0))
            .await
            .unwrap();
        let bootstrap = Bootstrap {
            port: listener.local_addr().unwrap().port(),
            secret: [8; 32],
        };
        let deadline = Instant::now() + Duration::from_secs(3);
        let server = tokio::spawn(async move {
            let mut operation_id = String::new();
            for command in ["inspect", "restart", "operation"] {
                let (mut stream, _) = listener.accept().await.unwrap();
                let frame = read_frame(&mut stream, REQUEST_LIMIT).await.unwrap();
                let request: Value = serde_json::from_slice(&frame).unwrap();
                assert_eq!(request["command"], command);
                let reply = if command == "inspect" {
                    assert!(request.get("expect_total_shards").is_none());
                    json!({"ok": true, "bot": {"bot_id": "1", "total_shards": 2}})
                } else {
                    if command == "restart" {
                        assert_eq!(request["expect_total_shards"], 2);
                        assert_eq!(request["shards"], json!([1]));
                        operation_id = request["operation_id"].as_str().unwrap().to_owned();
                    } else {
                        assert!(request.get("expect_total_shards").is_none());
                        assert!(request.get("shards").is_none());
                        assert_eq!(request["operation_id"], operation_id);
                    }
                    json!({"ok": true, "operation": {
                        "operation_id": operation_id, "command": "restart", "bot_id": "1",
                        "state": if command == "restart" {"running"} else {"succeeded"},
                        "elapsed_ms": 0, "shards": [], "failed_shards": []
                    }})
                };
                let mut frame = serde_json::to_vec(&reply).unwrap();
                frame.push(b'\n');
                stream.write_all(&frame).await.unwrap();
            }
        });
        let result = timeout_at(
            deadline,
            run_live(
                preloaded_options(
                    &[
                        "restart",
                        "1",
                        "--guild",
                        "1569994280854884364",
                        "--bootstrap-stdin",
                    ],
                    bootstrap.port,
                ),
                None,
                deadline,
                Some(&bootstrap),
            ),
        )
        .await
        .unwrap();
        assert_eq!(result["ok"], true, "{result}");
        assert_eq!(result["resolved_guilds"][0]["shard_id"], 1);
        timeout_at(deadline, server).await.unwrap().unwrap();
    }

    #[test]
    fn gateway_update_default_wait_is_overridable() {
        let base = ["--secret-file", "/tmp/s", "--port", "1"];
        let update = options(&[&["gateway-update", "bot"][..], &base[..]].concat());
        assert_eq!(update.timeout, Duration::from_secs(300));
        let restart = options(&[&["restart", "bot"][..], &base[..]].concat());
        assert_eq!(restart.timeout, Duration::from_secs(30));
        let explicit = options(
            &[
                &["gateway-update", "bot", "--timeout", "45s"][..],
                &base[..],
            ]
            .concat(),
        );
        assert_eq!(explicit.timeout, Duration::from_secs(45));
    }
}
