//! Opt-in live Create Message nonce probe. Run only in an approved test channel.

use src::{
    discord_message::{AttemptError, create_payload, new_nonce, single_attempt},
    redact::install_panic_hook,
};
use std::{
    collections::BTreeSet, env, ffi::OsString, future::IntoFuture, process::ExitCode,
    time::Duration,
};
use tokio::time::{Instant, sleep, timeout_at};
use twilight_http::Client;
use twilight_model::{
    channel::Message,
    id::{
        Id,
        marker::{ChannelMarker, MessageMarker},
    },
};

const HELP: &str = "+Usage: nonce_probe --token-env NAME --channel-id ID --yes-post [--delay-seconds SECONDS] [--cleanup]

Posts the text 'discord-rs nonce probe <random hex>' with all mentions disabled.
Sends two Create Message requests with identical JSON and nonce, then a control
with identical content and a different nonce. With --delay-seconds (1-3600),
also sends the original JSON a third time after waiting that long after post 2.
--cleanup deletes the unique messages identified as belonging to this run.
The token is read from the named environment variable. Use an approved test
channel. --yes-post is required before the probe can make any requests.";
const DEADLINE: Duration = Duration::from_secs(12);

struct Plan {
    token_env: String,
    channel: Id<ChannelMarker>,
    delay: Option<Duration>,
    cleanup: bool,
}

fn parse(args: Vec<OsString>) -> Result<Plan, &'static str> {
    let mut token_env = None;
    let mut channel = None;
    let mut delay = None;
    let mut cleanup = false;
    let mut yes_post = false;
    let mut args = args.into_iter();
    while let Some(arg) = args.next() {
        match arg.to_str() {
            Some("--yes-post") if !yes_post => yes_post = true,
            Some("--cleanup") if !cleanup => cleanup = true,
            Some("--token-env") if token_env.is_none() => {
                let name = args
                    .next()
                    .and_then(|value| value.into_string().ok())
                    .ok_or("invalid_token_env_name")?;
                let mut bytes = name.bytes();
                if !bytes
                    .next()
                    .is_some_and(|b| b == b'_' || b.is_ascii_alphabetic())
                    || !bytes.all(|b| b == b'_' || b.is_ascii_alphanumeric())
                {
                    return Err("invalid_token_env_name");
                }
                token_env = Some(name);
            }
            Some("--channel-id") if channel.is_none() => {
                channel = Some(
                    args.next()
                        .and_then(|value| value.to_str()?.parse::<u64>().ok())
                        .and_then(Id::new_checked)
                        .ok_or("invalid_channel_id")?,
                );
            }
            Some("--delay-seconds") if delay.is_none() => {
                delay = Some(
                    args.next()
                        .and_then(|value| value.to_str()?.parse().ok())
                        .filter(|seconds| (1..=3600).contains(seconds))
                        .map(Duration::from_secs)
                        .ok_or("invalid_delay_seconds")?,
                );
            }
            _ => return Err("invalid_argument"),
        }
    }
    if !yes_post {
        return Err("explicit_yes_post_required");
    }
    Ok(Plan {
        token_env: token_env.ok_or("token_env_required")?,
        channel: channel.ok_or("channel_id_required")?,
        delay,
        cleanup,
    })
}

#[derive(Clone, Copy)]
enum ProbeError {
    Attempt(AttemptError),
    Message,
}

fn print_attempt_error(label: &str, error: AttemptError) {
    let (status, reason) = match error {
        AttemptError::Deadline => (None, "deadline"),
        AttemptError::Http(status) => (Some(status), "http_error"),
        AttemptError::RateLimited => (Some(429), "rate_limited"),
        AttemptError::Transport => (None, "transport"),
        AttemptError::Timeout => (None, "http_timeout"),
        AttemptError::GuardUnavailable => (None, "guard_unavailable"),
        AttemptError::Other => (None, "request_or_response_error"),
    };
    let status = status.map_or_else(|| "unknown".to_owned(), |status| status.to_string());
    println!("{label} status={status} result={reason}");
}

async fn post(
    http: &Client,
    channel: Id<ChannelMarker>,
    body: &[u8],
    content: &str,
    label: &str,
    ids: &mut BTreeSet<Id<MessageMarker>>,
) -> Result<Id<MessageMarker>, ProbeError> {
    let deadline = Instant::now() + DEADLINE;
    let request = http
        .create_message(channel)
        .payload_json(body)
        .into_future();
    let response = timeout_at(deadline, single_attempt(request, deadline))
        .await
        .unwrap_or(Err(AttemptError::Deadline))
        .inspect_err(|error| print_attempt_error(label, *error))
        .map_err(ProbeError::Attempt)?;
    let status = response.status().get();
    let message: Message = match timeout_at(deadline, response.model()).await {
        Ok(Ok(message)) if message.channel_id == channel && message.content == content => message,
        _ => {
            println!("{label} status={status} result=unexpected_message");
            return Err(ProbeError::Message);
        }
    };
    println!("{label} status={status} message_id={}", message.id);
    ids.insert(message.id);
    Ok(message.id)
}

async fn run(plan: Plan) -> Result<bool, &'static str> {
    let token = env::var(&plan.token_env).map_err(|_| "token_unavailable")?;
    if token.is_empty()
        || !token
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"._-".contains(&b))
    {
        return Err("invalid_token_format");
    }
    let _ = rustls::crypto::ring::default_provider().install_default();
    let http = Client::new(token);
    let nonce = new_nonce().map_err(|_| "nonce_unavailable")?;
    let content = format!("discord-rs nonce probe {nonce}");
    let body = create_payload(&content, &nonce).map_err(|_| "payload_unavailable")?;
    let control =
        create_payload(&content, &format!("{nonce}c")).map_err(|_| "payload_unavailable")?;
    let channel = plan.channel;
    let mut ids = BTreeSet::new();
    println!("channel_id={channel} content={content}");
    let verdict = async {
        let first_started = Instant::now();
        let first = post(&http, channel, &body, &content, "post1", &mut ids).await?;
        println!(
            "post2_start_elapsed_ms={}",
            first_started.elapsed().as_millis()
        );
        let second = post(&http, channel, &body, &content, "post2", &mut ids).await?;
        println!("second_same_message_id={}", second == first);
        if let Some(delay) = plan.delay {
            sleep(delay).await;
            println!(
                "post3_start_elapsed_ms={}",
                first_started.elapsed().as_millis()
            );
            let third = post(&http, channel, &body, &content, "post3", &mut ids).await?;
            println!("third_same_as_first={}", third == first);
            println!("third_same_as_second={}", third == second);
        }
        let previous_count = ids.len();
        post(&http, channel, &control, &content, "control", &mut ids).await?;
        let control_is_new = ids.len() > previous_count;
        println!("control_distinct_message_id={control_is_new}");
        Ok::<_, ProbeError>((second == first, control_is_new))
    }
    .await;
    let complete = match verdict {
        Ok((true, true)) => {
            println!("result=deduplicated");
            true
        }
        Ok((false, true)) => {
            println!("result=not_deduplicated");
            true
        }
        _ => {
            println!("result=inconclusive");
            false
        }
    };
    if plan.cleanup {
        if matches!(
            verdict,
            Err(ProbeError::Attempt(
                AttemptError::RateLimited | AttemptError::Deadline
            ))
        ) {
            println!("cleanup=skipped_possible_rate_limit");
            return Ok(false);
        }
        let mut cleaned = true;
        for id in ids {
            let deadline = Instant::now() + DEADLINE;
            match timeout_at(
                deadline,
                single_attempt(http.delete_message(channel, id).into_future(), deadline),
            )
            .await
            .unwrap_or(Err(AttemptError::Deadline))
            {
                Ok(response) => {
                    println!("cleanup message_id={id} status={}", response.status().get())
                }
                Err(error) => {
                    print_attempt_error(&format!("cleanup message_id={id}"), error);
                    cleaned = false;
                    if matches!(error, AttemptError::RateLimited | AttemptError::Deadline) {
                        break;
                    }
                }
            }
        }
        return Ok(complete && cleaned);
    }
    Ok(complete)
}

#[tokio::main]
async fn main() -> ExitCode {
    install_panic_hook();
    let args: Vec<_> = env::args_os().skip(1).collect();
    if args.len() == 1 && (args[0] == "--help" || args[0] == "-h") {
        println!("{HELP}");
        return ExitCode::SUCCESS;
    }
    if !cfg!(target_os = "linux") {
        eprintln!("nonce_probe: Linux required");
        return ExitCode::FAILURE;
    }
    let result = match parse(args) {
        Ok(plan) => run(plan).await,
        Err(error) => {
            eprintln!("{HELP}");
            Err(error)
        }
    };
    match result {
        Ok(true) => ExitCode::SUCCESS,
        Ok(false) => ExitCode::FAILURE,
        Err(error) => {
            eprintln!("nonce_probe: {error}");
            ExitCode::FAILURE
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn posting_requires_explicit_consent() {
        let args = ["--token-env", "NONCE_PROBE_BOT_TOKEN", "--channel-id", "1"];
        assert!(matches!(
            parse(args.into_iter().map(OsString::from).collect()),
            Err("explicit_yes_post_required")
        ));
        let plan = parse(
            args.into_iter()
                .chain(["--yes-post", "--cleanup", "--delay-seconds", "30"])
                .map(OsString::from)
                .collect(),
        )
        .unwrap();
        assert_eq!(plan.channel.get(), 1);
        assert_eq!(plan.delay, Some(Duration::from_secs(30)));
        assert!(plan.cleanup);
    }
}
