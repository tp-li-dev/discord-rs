//! Opt-in, read-only Discord REST smoke check. Never starts a Gateway or Core.
use src::redact::{Secret, install_panic_hook};
use std::{
    env,
    ffi::OsString,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use twilight_http::{Client, error::ErrorType, response::ResponseFuture};

const DEFAULT_TOKEN_ENV: &str = "DISCORD_TEST_BOT_TOKEN";
const DEADLINE: Duration = Duration::from_secs(10);

struct Plan {
    token_env: String,
    apply: bool,
}

fn parse(args: impl IntoIterator<Item = OsString>) -> Result<Plan, &'static str> {
    let mut args = args.into_iter();
    let mut plan = Plan {
        token_env: DEFAULT_TOKEN_ENV.into(),
        apply: false,
    };
    let mut custom_env = false;
    while let Some(arg) = args.next() {
        if arg == "--apply" && !plan.apply {
            plan.apply = true;
        } else if arg == "--token-env" && !custom_env {
            let name = args
                .next()
                .ok_or("argument_invalid")?
                .into_string()
                .map_err(|_| "argument_encoding")?;
            if name.is_empty()
                || name.len() > 128
                || !name.bytes().enumerate().all(|(i, c)| {
                    c == b'_' || c.is_ascii_alphabetic() || (i > 0 && c.is_ascii_digit())
                })
            {
                return Err("token_env_invalid");
            }
            plan.token_env = name;
            custom_env = true;
        } else {
            // Never echo arbitrary arguments, including accidental token values.
            return Err("argument_invalid");
        }
    }
    Ok(plan)
}

fn token_state(value: &Option<Secret<OsString>>) -> &'static str {
    match value {
        None => "missing",
        Some(value) if value.expose_secret().is_empty() => "empty",
        Some(_) => "present",
    }
}

fn read_token(value: Option<Secret<OsString>>) -> Result<Secret<String>, &'static str> {
    let token = value
        .ok_or("token_missing")?
        .into_inner()
        .into_string()
        .map_err(|_| "token_encoding")?;
    if token.is_empty() {
        return Err("token_empty");
    }
    // Accept raw Bot credentials only, not auth schemes, whitespace or headers.
    // Do not assume a three-part layout or print rejected values.
    if token.len() > 4096
        || !token
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"._-".contains(&b))
    {
        return Err("token_format");
    }
    Ok(Secret::new(token))
}

struct Summary {
    kind: &'static str,
    rate_limit_headers: Option<bool>,
}

impl Summary {
    fn print(&self, check: &'static str) {
        println!(
            "check={check} result={} kind={} rate_limit_headers={}",
            if self.kind == "ok" {
                "success"
            } else {
                "failure"
            },
            self.kind,
            match self.rate_limit_headers {
                Some(true) => "present",
                Some(false) => "absent",
                None => "unknown",
            }
        );
    }
}

fn has_rate_limit_header(name: &str) -> bool {
    name.get(..12)
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case("x-ratelimit-"))
        || name.eq_ignore_ascii_case("retry-after")
}

fn error_kind(kind: &ErrorType) -> &'static str {
    // Error Display/Debug and source chains may include bodies or credentials.
    match kind {
        ErrorType::Unauthorized => "authentication",
        ErrorType::Response { status, .. } => match status.get() {
            401 => "authentication",
            403 => "forbidden",
            429 => "rate_limited",
            500..=599 => "server",
            _ => "http",
        },
        ErrorType::RequestTimedOut => "timeout",
        ErrorType::RequestCanceled => "cancelled",
        ErrorType::RequestError => "transport",
        ErrorType::Parsing { .. } => "invalid_response",
        _ => "request",
    }
}

// The limiter invokes this before each attempt, including its automatic 429 retry.
fn allow_first_attempt(attempted: &AtomicBool) -> bool {
    !attempted.swap(true, Ordering::SeqCst)
}

async fn probe<T: serde::de::DeserializeOwned + Unpin>(
    mut request: ResponseFuture<T>,
    validate: fn(&T) -> bool,
) -> Summary {
    let attempted = Arc::new(AtomicBool::new(false));
    let gate = Arc::clone(&attempted);
    if !request.set_pre_flight(move || allow_first_attempt(&gate)) {
        // Fail closed if a dependency change removes the pre-flight limiter.
        return Summary {
            kind: "retry_guard_unavailable",
            rate_limit_headers: None,
        };
    }
    let mut headers = None;
    let result = tokio::time::timeout(DEADLINE, async {
        let response = request.await.map_err(|error| {
            match error.kind() {
                // Only the denied second attempt can cause cancellation here.
                ErrorType::RequestCanceled if attempted.load(Ordering::SeqCst) => "rate_limited",
                kind => error_kind(kind),
            }
        })?;
        headers = Some(
            response
                .headers()
                .any(|(name, _)| has_rate_limit_header(name)),
        );
        let model = response.model().await.map_err(|_| "invalid_response")?;
        if !validate(&model) {
            return Err("invalid_response");
        }
        Ok(())
    })
    .await;
    Summary {
        kind: match result {
            Ok(Ok(())) => "ok",
            Ok(Err(kind)) => kind,
            Err(_) => "timeout",
        },
        rate_limit_headers: headers,
    }
}

async fn apply(token: Secret<String>) -> bool {
    let _ = rustls::crypto::ring::default_provider().install_default();
    // No tracing subscriber is installed: dependency logs must not expose payloads.
    let client = Client::builder()
        .token(token.into_inner())
        .timeout(DEADLINE)
        .build();
    // The only network operations in this program. No configurable host or proxy.
    let gateway = probe(client.gateway().authed().into_future(), |info| {
        info.url.starts_with("wss://") && src::gateway_metadata::validate_metadata(info).is_ok()
    })
    .await;
    gateway.print("gateway_bot");
    if gateway.kind != "ok" {
        println!(
            "check=current_user result=skipped kind=previous_failure rate_limit_headers=unknown"
        );
        return false;
    }
    let user = probe(client.current_user().into_future(), |user| user.bot).await;
    user.print("current_user");
    user.kind == "ok"
}

fn run(args: impl IntoIterator<Item = OsString>) -> Result<bool, &'static str> {
    let plan = parse(args)?;
    let value = env::var_os(&plan.token_env).map(Secret::new);
    println!(
        "mode={} config=valid token_env={} token={} requests={}",
        if plan.apply { "apply" } else { "dry_run" },
        if plan.token_env == DEFAULT_TOKEN_ENV {
            "default"
        } else {
            "custom"
        },
        token_state(&value),
        if plan.apply { "read_only_get" } else { "none" }
    );
    if !plan.apply {
        return Ok(true);
    }
    let token = read_token(value)?;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|_| "runtime")?;
    Ok(runtime.block_on(apply(token)))
}

fn execute(args: impl IntoIterator<Item = OsString>) -> bool {
    match run(args) {
        Ok(success) => success,
        Err(kind) => {
            eprintln!("result=failure kind={kind} rate_limit_headers=unknown");
            false
        }
    }
}

fn main() {
    install_panic_hook();
    if !execute(env::args_os().skip(1)) {
        std::process::exit(1);
    }
}

#[cfg(test)]
#[path = "../../tests/discord_smoke/cli.rs"]
mod tests;
