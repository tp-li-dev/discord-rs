//! Local authenticated daemon control. Bootstrap capability arrives on anonymous stdin.
//! The TCP listener is always loopback; it is not a remote management endpoint.
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use src::daemon::control::{CommandRequest, ControlPlane, NOTE};
use std::{ffi::OsString, io, net::SocketAddr, path::PathBuf, time::Duration};
use subtle::ConstantTimeEq;
use tokio::{
    io::{AsyncBufReadExt, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader},
    net::TcpListener,
    sync::{mpsc, oneshot},
    task::JoinSet,
};

const REQUEST_LIMIT: u64 = 4096;
const RESPONSE_LIMIT: usize = 64 * 1024;
const DEADLINE: Duration = Duration::from_secs(3);
const USAGE: &str = "usage: botd --file F --runtime-root ABS --gateway-program ABS --bootstrap-stdin [--no-autostart] [--mock-discord LOOPBACK_IP:PORT]";

struct Options {
    file: PathBuf,
    runtime_root: PathBuf,
    gateway_program: PathBuf,
    no_autostart: bool,
    mock_discord: Option<SocketAddr>,
}

fn parse_options(args: impl IntoIterator<Item = OsString>) -> Result<Options, &'static str> {
    let mut args = args.into_iter();
    let mut file = None;
    let mut runtime_root = None;
    let mut gateway_program = None;
    let mut bootstrap = false;
    let mut no_autostart = false;
    let mut mock_discord = None;
    while let Some(arg) = args.next() {
        let target = match arg.to_str() {
            Some("--file") if file.is_none() => &mut file,
            Some("--runtime-root") if runtime_root.is_none() => &mut runtime_root,
            Some("--gateway-program") if gateway_program.is_none() => &mut gateway_program,
            Some("--bootstrap-stdin") if !bootstrap => {
                bootstrap = true;
                continue;
            }
            Some("--no-autostart") if !no_autostart => {
                no_autostart = true;
                continue;
            }
            Some("--mock-discord") if mock_discord.is_none() => {
                let value = args.next().ok_or(USAGE)?;
                let address: SocketAddr = value
                    .to_str()
                    .ok_or(USAGE)?
                    .parse()
                    .map_err(|_| "mock Discord requires a loopback IP and nonzero port")?;
                if !address.ip().is_loopback() || address.port() == 0 {
                    return Err("mock Discord requires a loopback IP and nonzero port");
                }
                mock_discord = Some(address);
                continue;
            }
            _ => return Err(USAGE),
        };
        let value = args.next().ok_or(USAGE)?;
        if value.is_empty() || value.to_string_lossy().starts_with('-') {
            return Err(USAGE);
        }
        *target = Some(PathBuf::from(value));
    }
    let options = Options {
        file: file.ok_or(USAGE)?,
        runtime_root: runtime_root.ok_or(USAGE)?,
        gateway_program: gateway_program.ok_or(USAGE)?,
        no_autostart,
        mock_discord,
    };
    if !bootstrap {
        return Err("anonymous stdin bootstrap is required");
    }
    if !options.runtime_root.is_absolute() || !options.gateway_program.is_absolute() {
        return Err("runtime root and gateway program must be absolute paths");
    }
    Ok(options)
}

// Deliberately no Debug: capabilities must never be formatted or logged.
#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Bootstrap {
    secret: [u8; 32],
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct StatusRequest {
    secret: [u8; 32],
    command: String,
    #[serde(default)]
    bot: Option<String>,
    #[serde(default)]
    operation_id: Option<String>,
    #[serde(default)]
    force: bool,
    #[serde(default)]
    yes: bool,
}

struct Envelope {
    request: CommandRequest,
    response: oneshot::Sender<Value>,
}

async fn read_frame<R: AsyncRead + Unpin>(reader: R) -> io::Result<Vec<u8>> {
    let mut frame = Vec::new();
    BufReader::new(reader)
        .take(REQUEST_LIMIT + 1)
        .read_until(b'\n', &mut frame)
        .await?;
    if frame.is_empty() || frame.len() as u64 > REQUEST_LIMIT || frame.last() != Some(&b'\n') {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid control frame",
        ));
    }
    Ok(frame)
}

async fn read_bootstrap<R: AsyncRead + Unpin>(reader: R) -> io::Result<Bootstrap> {
    tokio::time::timeout(DEADLINE, async {
        let frame = read_frame(reader).await?;
        let bootstrap: Bootstrap = serde_json::from_slice(&frame)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "invalid control bootstrap"))?;
        if bootstrap.secret == [0; 32] {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "invalid control bootstrap",
            ));
        }
        Ok(bootstrap)
    })
    .await
    .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "control bootstrap timed out"))?
}

async fn serve_status<S: AsyncRead + AsyncWrite + Unpin>(
    mut stream: S,
    secret: &[u8; 32],
    commands: &mpsc::Sender<Envelope>,
) -> io::Result<()> {
    tokio::time::timeout(DEADLINE, async {
        let frame = read_frame(&mut stream).await?;
        let request: StatusRequest = serde_json::from_slice(&frame)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "invalid status request"))?;
        if !bool::from(request.secret.ct_eq(secret)) {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "status request denied",
            ));
        }
        let command = CommandRequest {
            command: request.command,
            bot: request.bot,
            operation_id: request.operation_id,
            force: request.force,
            yes: request.yes,
        };
        if !command.valid() {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "control request denied",
            ));
        }
        // Only the daemon owner admits/executes commands. A disconnected client
        // never cancels an already admitted operation or forces a child to exit.
        let (response, receiver) = oneshot::channel();
        let value = if commands
            .try_send(Envelope {
                request: command,
                response,
            })
            .is_ok()
        {
            receiver
                .await
                .map_err(|_| io::Error::other("daemon control unavailable"))?
        } else {
            json!({"ok": false, "offline": false, "error": "daemon control busy", "note": NOTE})
        };
        let mut response = serde_json::to_vec(&value)?;
        response.push(b'\n');
        if response.len() > RESPONSE_LIMIT {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "status response exceeds limit",
            ));
        }
        stream.write_all(&response).await?;
        stream.shutdown().await
    })
    .await
    .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "status request timed out"))?
}

async fn status_server(
    listener: TcpListener,
    secret: [u8; 32],
    commands: mpsc::Sender<Envelope>,
) -> io::Result<()> {
    let mut sessions = JoinSet::new();
    loop {
        tokio::select! {
            accepted = listener.accept(), if sessions.len() < 32 => {
                let (stream, peer) = accepted?;
                if !peer.ip().is_loopback() { continue; }
                let commands = commands.clone();
                sessions.spawn(async move { let _ = serve_status(stream, &secret, &commands).await; });
            }
            _ = sessions.join_next(), if !sessions.is_empty() => {}
        }
    }
}

async fn run() -> Result<(), &'static str> {
    use src::{
        daemon::{Daemon, EnvironmentCredentials},
        placement::Config,
        supervisor::RestartPolicy,
    };
    let options = parse_options(std::env::args_os().skip(1))?;
    let config = Config::load(&options.file).map_err(|_| "cannot load daemon configuration")?;
    let mut daemon = Daemon::from_config(
        &config,
        &std::collections::HashMap::new(),
        &options.runtime_root,
        RestartPolicy::default(),
    )
    .map_err(|_| "daemon configuration is invalid or unsupported")?;
    daemon
        .set_gateway_program(&options.gateway_program)
        .map_err(|_| "invalid gateway program")?;
    if let Some(address) = options.mock_discord {
        daemon
            .enable_mock_discord(address)
            .map_err(|_| "invalid mock Discord configuration")?;
    }
    let bootstrap = read_bootstrap(tokio::io::stdin())
        .await
        .map_err(|_| "invalid anonymous control bootstrap")?;
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .map_err(|_| "cannot bind local daemon control")?;
    let port = listener
        .local_addr()
        .map_err(|_| "cannot inspect local daemon control")?
        .port();
    let credentials = EnvironmentCredentials;
    // Partial startup failures remain visible through snapshots. Dropping this
    // daemon, including any error below, deliberately does not force-kill children.
    let startup_ok = options.no_autostart || daemon.start_all(&credentials).is_ok();
    let mut control = ControlPlane::new(daemon, config, startup_ok);
    let (sender, mut receiver) = mpsc::channel::<Envelope>(64);
    let mut server = tokio::spawn(status_server(listener, bootstrap.secret, sender));
    let mut announcement = serde_json::to_vec(&json!({
        "port": port, "local_only": true, "startup_ok": startup_ok,
        "mock_discord": options.mock_discord.is_some(),
    }))
    .map_err(|_| "cannot encode daemon announcement")?;
    announcement.push(b'\n');
    let mut stdout = tokio::io::stdout();
    if stdout.write_all(&announcement).await.is_err() || stdout.flush().await.is_err() {
        server.abort();
        return Err("cannot announce local daemon control");
    }
    let mut interval = tokio::time::interval(Duration::from_millis(25));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let result = loop {
        tokio::select! {
            signal = tokio::signal::ctrl_c() => {
                break signal.map_err(|_| "cannot receive daemon shutdown signal");
            }
            _ = interval.tick() => {
                control.tick(&credentials).await;
            }
            Some(envelope) = receiver.recv() => {
                if !envelope.response.is_closed() {
                    let _ = envelope.response.send(control.request(&envelope.request));
                }
            }
            _ = &mut server => break Err("local daemon control stopped"),
        }
    };
    server.abort();
    result
}

#[tokio::main]
async fn main() {
    if let Err(error) = run().await {
        // Only static messages cross this boundary; no config, child output,
        // capability, credential, or OS error text is formatted.
        eprintln!("botd: {error}");
        std::process::exit(1);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{DuplexStream, duplex};

    fn options(extra: &[&str]) -> Vec<OsString> {
        let root = std::env::current_dir().unwrap();
        let mut args = vec![
            "--file".into(),
            "config.toml".into(),
            "--runtime-root".into(),
            root.clone().into_os_string(),
            "--gateway-program".into(),
            root.join("gateway-program").into_os_string(),
            "--bootstrap-stdin".into(),
        ];
        args.extend(extra.iter().map(OsString::from));
        args
    }

    #[test]
    fn accepts_absolute_program_and_root() {
        assert!(parse_options(options(&[])).is_ok());
    }

    #[test]
    fn mock_discord_requires_an_explicit_loopback_endpoint() {
        assert!(parse_options(options(&[])).unwrap().mock_discord.is_none());
        for address in ["127.0.0.1:1234", "[::1]:1234"] {
            let parsed = parse_options(options(&["--mock-discord", address])).unwrap();
            assert_eq!(parsed.mock_discord, Some(address.parse().unwrap()));
        }
        for extra in [
            &["--mock-discord"][..],
            &["--mock-discord", "true"],
            &["--mock-discord", "localhost:1234"],
            &["--mock-discord", "127.0.0.1:0"],
            &["--mock-discord", "0.0.0.0:1234"],
            &["--mock-discord", "192.0.2.1:1234"],
            &["--mock-discord", "[::]:1234"],
            &[
                "--mock-discord",
                "127.0.0.1:1234",
                "--mock-discord",
                "127.0.0.1:5678",
            ],
        ] {
            assert!(parse_options(options(extra)).is_err());
        }
    }

    #[test]
    fn rejects_missing_duplicate_and_unknown_options() {
        for extra in [
            &["--bootstrap-stdin"][..],
            &["--file", "other"],
            &["--bind", "0.0.0.0:1"],
        ] {
            assert!(parse_options(options(extra)).is_err());
        }
        let mut args = options(&[]);
        args.pop();
        assert!(parse_options(args).is_err());
        assert!(parse_options([OsString::from("--file")]).is_err());
    }

    #[test]
    fn rejects_relative_program_or_root() {
        for index in [3, 5] {
            let mut args = options(&[]);
            args[index] = "relative".into();
            assert!(parse_options(args).is_err());
        }
    }

    async fn status_session(request: &[u8], status: Value) -> (io::Result<()>, Vec<u8>) {
        let (mut client, server) = duplex(128 * 1024);
        let (sender, mut receiver) = mpsc::channel::<Envelope>(1);
        let backend = tokio::spawn(async move {
            if let Some(envelope) = receiver.recv().await {
                let _ = envelope
                    .response
                    .send(json!({"ok": true, "status": status, "note": NOTE}));
            }
        });
        let task = tokio::spawn(async move { serve_status(server, &[7; 32], &sender).await });
        client.write_all(request).await.unwrap();
        client.shutdown().await.unwrap();
        let mut response = Vec::new();
        client.read_to_end(&mut response).await.unwrap();
        let result = task.await.unwrap();
        backend.await.unwrap();
        (result, response)
    }

    fn request(secret: [u8; 32], command: &str) -> Vec<u8> {
        let mut frame = serde_json::to_vec(&StatusRequest {
            secret,
            command: command.into(),
            bot: None,
            operation_id: None,
            force: false,
            yes: false,
        })
        .unwrap();
        frame.push(b'\n');
        frame
    }

    #[tokio::test]
    async fn authenticated_status_returns_snapshot_without_secret() {
        let (result, response) =
            status_session(&request([7; 32], "status"), json!({"processes": []})).await;
        result.unwrap();
        let response: Value = serde_json::from_slice(&response).unwrap();
        assert_eq!(response["ok"], true);
        assert_eq!(response["status"]["processes"], json!([]));
        assert!(response.get("secret").is_none());
    }

    #[tokio::test]
    async fn rejects_wrong_capability_and_mutating_command_without_response() {
        for frame in [request([8; 32], "status"), request([7; 32], "stop")] {
            let (result, response) = status_session(&frame, json!([])).await;
            assert_eq!(result.unwrap_err().kind(), io::ErrorKind::PermissionDenied);
            assert!(response.is_empty());
        }
    }

    #[tokio::test]
    async fn rejects_oversized_unterminated_and_malformed_requests_without_response() {
        for frame in [
            vec![b'x'; REQUEST_LIMIT as usize + 1],
            b"{}".to_vec(),
            b"bad\n".to_vec(),
        ] {
            let (result, response) = status_session(&frame, json!([])).await;
            assert_eq!(result.unwrap_err().kind(), io::ErrorKind::InvalidData);
            assert!(response.is_empty());
        }
    }

    #[tokio::test]
    async fn rejects_unknown_request_fields_without_response() {
        let mut value = serde_json::to_value(StatusRequest {
            secret: [7; 32],
            command: "status".into(),
            bot: None,
            operation_id: None,
            force: false,
            yes: false,
        })
        .unwrap();
        value["extra"] = json!(true);
        let mut frame = serde_json::to_vec(&value).unwrap();
        frame.push(b'\n');
        let (result, response) = status_session(&frame, json!([])).await;
        assert_eq!(result.unwrap_err().kind(), io::ErrorKind::InvalidData);
        assert!(response.is_empty());
    }

    #[tokio::test]
    async fn rejects_oversized_response_before_writing() {
        let (result, response) = status_session(
            &request([7; 32], "status"),
            json!("x".repeat(RESPONSE_LIMIT)),
        )
        .await;
        assert_eq!(result.unwrap_err().kind(), io::ErrorKind::InvalidData);
        assert!(response.is_empty());
    }

    #[tokio::test]
    async fn idle_control_client_has_a_deadline() {
        let (_client, server) = duplex(128);
        let (sender, _receiver) = mpsc::channel(1);
        let result = serve_status(server, &[7; 32], &sender).await;
        assert_eq!(result.unwrap_err().kind(), io::ErrorKind::TimedOut);
    }

    #[tokio::test]
    async fn status_is_dispatched_only_after_authentication() {
        let (mut client, server) = duplex(8192);
        let (sender, mut receiver) = mpsc::channel::<Envelope>(1);
        let task = tokio::spawn(async move { serve_status(server, &[7; 32], &sender).await });
        assert!(receiver.try_recv().is_err());
        client.write_all(&request([7; 32], "status")).await.unwrap();
        let envelope = receiver.recv().await.unwrap();
        assert_eq!(envelope.request.command, "status");
        envelope.response.send(json!({"ok": true, "status": {"processes": [{"state": "Failed"}], "monitor_ok": false}})).unwrap();
        let mut response = Vec::new();
        client.read_to_end(&mut response).await.unwrap();
        task.await.unwrap().unwrap();
        let response: Value = serde_json::from_slice(&response).unwrap();
        assert_eq!(response["status"]["monitor_ok"], false);
        assert_eq!(response["status"]["processes"][0]["state"], "Failed");
    }

    async fn bootstrap_input(frame: Vec<u8>) -> io::Result<Bootstrap> {
        let (mut client, server): (DuplexStream, DuplexStream) = duplex(8192);
        client.write_all(&frame).await.unwrap();
        client.shutdown().await.unwrap();
        read_bootstrap(server).await
    }

    #[tokio::test]
    async fn bootstrap_is_bounded_and_rejects_zero_or_unknown_fields() {
        let mut valid = serde_json::to_vec(&Bootstrap { secret: [7; 32] }).unwrap();
        valid.push(b'\n');
        assert_eq!(bootstrap_input(valid).await.unwrap().secret, [7; 32]);
        for frame in [
            b"{\"secret\":[0]}\n".to_vec(),
            b"{}\n".to_vec(),
            vec![b'x'; REQUEST_LIMIT as usize + 1],
            serde_json::to_vec(&Bootstrap { secret: [0; 32] }).unwrap(),
        ] {
            assert!(bootstrap_input(frame).await.is_err());
        }
        let mut zero = serde_json::to_vec(&Bootstrap { secret: [0; 32] }).unwrap();
        zero.push(b'\n');
        assert!(bootstrap_input(zero).await.is_err());
        let mut extra = json!({"secret": vec![7; 32], "port": 1})
            .to_string()
            .into_bytes();
        extra.push(b'\n');
        assert!(bootstrap_input(extra).await.is_err());
    }

    #[tokio::test]
    async fn bootstrap_has_a_deadline() {
        let (_client, server) = duplex(128);
        let result = read_bootstrap(server).await;
        assert!(matches!(result, Err(error) if error.kind() == io::ErrorKind::TimedOut));
    }
}
