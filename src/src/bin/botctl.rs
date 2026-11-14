//! Authenticated local daemon client, with offline configuration/update planning.
//! Bootstrap capabilities are supplied through anonymous stdin, or (for a systemd-resident
//! botd) a permission-checked secret file via `--secret-file` with the daemon's fixed
//! `--port`. Never argv values, environment variables or an endpoint file.
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use src::local_control::DaemonControlEnvelope;
use src::update_rollback::{self, Action, Component, UpdateControlRequest};
use src::{botref, placement::Config};
use std::{collections::HashMap, ffi::OsString, path::PathBuf, time::Duration};
use tokio::{
    io::{AsyncBufReadExt, AsyncRead, AsyncReadExt, AsyncWriteExt, BufReader},
    net::TcpStream,
    time::{Instant, timeout_at},
};

const REQUEST_LIMIT: usize = 4096;
const RESPONSE_LIMIT: usize = 64 * 1024;
const USAGE: &str = "usage: botctl ps|status [BOT]|inspect BOT|shards BOT|start BOT|stop BOT|restart BOT|operation ID (--bootstrap-stdin | --secret-file ABS --port PORT) [--json] [--timeout 30s]; botctl config validate --file F [--json]; botctl secret generate --out ABS [--json]; botctl update|rollback BOT --plan manifest.json [--component core|gateway|all] [--rolling] [--dry-run | --yes (--bootstrap-stdin | --secret-file ABS --port PORT)] [--json]";
const OFFLINE_NOTE: &str =
    "オフライン設定検証のみ。daemon・実稼働状態・認証・quorum/leaseは検証していません。";

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
}

fn valid_operation_id(value: &str) -> bool {
    value.len() == 32
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
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
    let mut force = false;
    let mut yes = false;
    let mut plan = None;
    let mut component = None;
    let mut dry_run = false;
    let mut rolling = false;
    let mut secret_file = None;
    let mut port = None;
    let mut out = None;
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
        });
    }
    if out.is_some() {
        return Err("--out is only supported for secret generate");
    }
    let auth = bootstrap || secret_file.is_some();
    if force && command != "stop" {
        return Err("--force is supported only for stop");
    }
    if force && !yes {
        return Err("force stop requires explicit --yes confirmation");
    }
    let update = matches!(command, "update" | "rollback");
    if yes && !force && !update {
        return Err("--yes requires --force stop");
    }
    if operation_id.is_some()
        && !matches!(
            command,
            "start" | "stop" | "restart" | "operation" | "update" | "rollback"
        )
    {
        return Err("--operation-id requires a mutation or operation query");
    }
    if !update && (plan.is_some() || component.is_some() || dry_run || rolling) {
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
        timeout: timeout.unwrap_or(Duration::from_secs(30)),
        force,
        yes,
        plan,
        component: component.unwrap_or_default(),
        secret_file,
        port,
        out,
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

async fn exchange(bootstrap: &Bootstrap, request: &impl Serialize) -> Result<Value, &'static str> {
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
    if !operation["bot_id"].is_string()
        || operation["elapsed_ms"].as_u64().is_none()
        || !operation["shards"].is_array()
        || !operation["failed_shards"].is_array()
        || !matches!(
            operation["command"].as_str(),
            Some("start" | "stop" | "restart" | "update" | "rollback")
        )
    {
        return Err("daemon operation has no valid result metadata");
    }
    if operation["state"] == "succeeded"
        && !operation["failed_shards"]
            .as_array()
            .is_some_and(Vec::is_empty)
    {
        return Err("daemon operation claims success with failed shards");
    }
    match operation["state"].as_str() {
        Some(state @ ("running" | "succeeded" | "failed")) => Ok(state),
        _ => Err("invalid daemon operation state"),
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
    let deadline = Instant::now() + options.timeout;
    run_live(options, None, deadline).await
}

async fn run_live(
    mut options: Options,
    update: Option<UpdateControlRequest>,
    deadline: Instant,
) -> Value {
    let mutation = matches!(
        options.command,
        "start" | "stop" | "restart" | "update" | "rollback"
    );
    if mutation && options.operation_id.is_none() {
        options.operation_id = match new_operation_id() {
            Ok(id) => Some(id),
            Err(error) => return failure(error, None, None),
        };
    }
    let id = options.operation_id.as_deref();
    let bootstrap = match timeout_at(
        deadline,
        load_bootstrap(options.secret_file.as_deref(), options.port),
    )
    .await
    {
        Ok(Ok(bootstrap)) => bootstrap,
        Ok(Err(error)) => return failure(error, id, None),
        Err(_) => return failure("control bootstrap timed out; no request sent", id, None),
    };
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
        };
        let response = match timeout_at(deadline, exchange(&bootstrap, &inspect)).await {
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
    };
    let mut last = None;
    let mut send_update = update.is_some();
    loop {
        let exchange_result = timeout_at(deadline, async {
            if send_update {
                exchange(
                    &bootstrap,
                    &DaemonControlEnvelope {
                        secret: bootstrap.secret,
                        request: update.as_ref().expect("update payload"),
                    },
                )
                .await
            } else {
                exchange(&bootstrap, &request).await
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
            match state {
                Ok("succeeded") if response["ok"] == true => return response,
                Ok("failed" | "succeeded") => {
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
    run_live(options, Some(update), deadline).await
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let args: Vec<_> = std::env::args_os().skip(1).collect();
    let json_output = args.iter().any(|arg| arg == "--json");
    let value = match parse_options(&args) {
        Ok(options) => run(options).await,
        Err(error) => failure(error, None, None),
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
    std::process::exit(if success { 0 } else { 1 });
}
