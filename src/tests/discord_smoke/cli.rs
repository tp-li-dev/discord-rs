// Included in the bin test target to avoid building every existing bin.
use super::*;
use std::process::{Command, Output};

const FIXTURE: &str = "DSMOKE_CLI_TEST_ARGS";
const CUSTOM_ENV: &str = "DSMOKE_SYNTHETIC_TOKEN";
const SYNTHETIC: &str = "opaque-synthetic-credential";

fn process(args: &[&str], variable: &str, value: Option<&str>) -> Output {
    let mut command = Command::new(env::current_exe().unwrap());
    command
        .args(["--exact", "tests::process_fixture", "--nocapture"])
        .env(FIXTURE, serde_json::to_string(args).unwrap())
        .env_remove(DEFAULT_TOKEN_ENV)
        .env_remove(CUSTOM_ENV);
    if let Some(value) = value {
        command.env(variable, value);
    }
    command.output().unwrap()
}

fn text(output: &Output) -> String {
    let text = format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    // Do not include captured output in assertion failures.
    assert!(!text.contains(SYNTHETIC), "synthetic credential leaked");
    text
}

#[test]
fn process_fixture() {
    let Some(args) = env::var_os(FIXTURE) else {
        return;
    };
    install_panic_hook();
    let args: Vec<String> = serde_json::from_str(&args.to_string_lossy()).unwrap();
    std::process::exit(if execute(args.into_iter().map(OsString::from)) {
        0
    } else {
        1
    });
}

#[test]
fn dry_run_without_token_succeeds_without_runtime_or_network() {
    let output = process(&[], DEFAULT_TOKEN_ENV, None);
    assert!(output.status.success());
    let text = text(&output);
    assert!(
        text.contains("mode=dry_run config=valid token_env=default token=missing requests=none")
    );
    assert!(!text.contains("check="));
}

#[test]
fn dry_run_only_reports_presence_even_for_invalid_credentials() {
    for value in [
        SYNTHETIC.to_owned(),
        format!("{SYNTHETIC}\r\nAuthorization: Bearer {SYNTHETIC}"),
    ] {
        let output = process(&["--token-env", CUSTOM_ENV], CUSTOM_ENV, Some(&value));
        assert!(output.status.success());
        let text = text(&output);
        assert!(text.contains("token_env=custom token=present requests=none"));
        assert!(!text.contains("Authorization"));
        assert!(!text.contains("check="));
    }
}

#[test]
fn apply_rejects_missing_or_empty_token_before_client_creation() {
    for args in [vec!["--apply"], vec!["--token-env", CUSTOM_ENV, "--apply"]] {
        for (value, expected) in [(None, "token_missing"), (Some(""), "token_empty")] {
            let variable = if args.len() == 1 {
                DEFAULT_TOKEN_ENV
            } else {
                CUSTOM_ENV
            };
            let output = process(&args, variable, value);
            assert_eq!(output.status.code(), Some(1));
            let text = text(&output);
            assert!(text.contains(expected));
            assert!(!text.contains("check="));
        }
    }
}

#[test]
fn apply_rejects_header_injection_without_echoing_secret() {
    let value = format!("{SYNTHETIC}\r\nInjected: {SYNTHETIC}");
    let output = process(&["--apply"], DEFAULT_TOKEN_ENV, Some(&value));
    assert_eq!(output.status.code(), Some(1));
    assert!(text(&output).contains("kind=token_format"));
}

#[test]
fn arguments_and_environment_names_are_never_echoed() {
    for args in [
        vec![SYNTHETIC],
        vec!["--token-env", SYNTHETIC],
        vec!["--apply", "--apply"],
        vec!["--token-env"],
        vec!["--token-env", "A", "--token-env", "B"],
    ] {
        let output = process(&args, DEFAULT_TOKEN_ENV, Some(SYNTHETIC));
        assert_eq!(output.status.code(), Some(1));
        text(&output);
    }
    // A syntactically valid name can itself be sensitive: report only custom/default.
    let output = process(
        &["--token-env", "PRIVATE_ENV_NAME"],
        "PRIVATE_ENV_NAME",
        Some(SYNTHETIC),
    );
    assert!(output.status.success());
    assert!(!text(&output).contains("PRIVATE_ENV_NAME"));
}

#[test]
fn retry_gate_permits_exactly_one_attempt() {
    let attempted = AtomicBool::new(false);
    assert!(allow_first_attempt(&attempted));
    for _ in 0..10 {
        assert!(!allow_first_attempt(&attempted));
    }
}

#[test]
fn only_rate_limit_header_names_count() {
    for name in ["X-RateLimit-Limit", "x-ratelimit-bucket", "Retry-After"] {
        assert!(has_rate_limit_header(name));
    }
    for name in [
        "authorization",
        "content-type",
        "x-ratelimit",
        "retry-after-extra",
        "é",
    ] {
        assert!(!has_rate_limit_header(name));
    }
}

#[test]
fn classifies_http_status_without_formatting_response_body() {
    use twilight_http::{api_error::ApiError, response::StatusCode};
    for (status, expected) in [
        (StatusCode::UNAUTHORIZED, "authentication"),
        (StatusCode::FORBIDDEN, "forbidden"),
        (StatusCode::TOO_MANY_REQUESTS, "rate_limited"),
        (StatusCode::NOT_FOUND, "http"),
    ] {
        let kind = ErrorType::Response {
            body: SYNTHETIC.as_bytes().to_vec(),
            error: serde_json::from_value::<ApiError>(serde_json::json!({
                "code": 0,
                "message": SYNTHETIC,
            }))
            .unwrap(),
            status,
        };
        assert_eq!(error_kind(&kind), expected);
    }
}

// Exercise the actual Twilight retry path with a token-free loopback server.
async fn mock_probe(status: u16, extra_headers: &str, body: &str) -> (Summary, usize) {
    use std::sync::atomic::AtomicUsize;
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
    };

    let _ = rustls::crypto::ring::default_provider().install_default();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let count = Arc::new(AtomicUsize::new(0));
    let observed = Arc::clone(&count);
    let response = format!(
        "HTTP/1.1 {status} Test\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n{extra_headers}\r\n{body}",
        body.len()
    );
    let server = tokio::spawn(async move {
        loop {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut received = Vec::new();
            let mut chunk = [0u8; 1024];
            while !received.windows(4).any(|bytes| bytes == b"\r\n\r\n") {
                let size = stream.read(&mut chunk).await.unwrap();
                if size == 0 {
                    return;
                }
                received.extend_from_slice(&chunk[..size]);
                assert!(received.len() <= 8192);
            }
            observed.fetch_add(1, Ordering::SeqCst);
            stream.write_all(response.as_bytes()).await.unwrap();
        }
    });
    // No credentials and no configurable proxy in the production CLI.
    let client = Client::builder().proxy(address.to_string(), true).build();
    let result = tokio::time::timeout(
        Duration::from_secs(2),
        probe(client.current_user().into_future(), |user| user.bot),
    )
    .await;
    server.abort();
    let _ = server.await;
    (
        result.expect("mock probe exceeded its deadline"),
        count.load(Ordering::SeqCst),
    )
}

#[tokio::test]
async fn actual_twilight_429_path_never_sends_second_get() {
    let (summary, requests) = mock_probe(429,
        "X-RateLimit-Scope: user\r\nX-RateLimit-Bucket: synthetic\r\nX-RateLimit-Limit: 1\r\nX-RateLimit-Remaining: 0\r\nX-RateLimit-Reset-After: 0.001\r\nRetry-After: 0.001\r\n",
        r#"{"message":"synthetic","retry_after":0.001,"global":false}"#).await;
    assert_eq!(requests, 1);
    assert_eq!(summary.kind, "rate_limited");
    assert_eq!(summary.rate_limit_headers, None);
}

#[tokio::test]
async fn successful_and_invalid_models_preserve_header_presence() {
    let user = r#"{"id":"1","username":"synthetic","discriminator":"0000","bot":true,"mfa_enabled":false}"#;
    for (headers, body, expected, present) in [
        ("X-RateLimit-Limit: 1\r\n", user, "ok", true),
        ("", user, "ok", false),
        (
            "Retry-After: 1\r\n",
            "invalid-json",
            "invalid_response",
            true,
        ),
    ] {
        let (summary, requests) = mock_probe(200, headers, body).await;
        assert_eq!(requests, 1);
        assert_eq!(summary.kind, expected);
        assert_eq!(summary.rate_limit_headers, Some(present));
    }
}
