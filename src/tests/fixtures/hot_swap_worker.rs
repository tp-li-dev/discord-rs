//! A std-only fault relay around the production Worker, compiled by the tests.
//! Sidecars and events contain control metadata only, never request payloads.
use std::{
    fs::{self, OpenOptions},
    io::{self, BufRead, BufReader, Write},
    path::{Path, PathBuf},
    process::{Child, ChildStdin, ChildStdout, Command, ExitCode, Stdio},
    thread,
    time::{Duration, Instant},
};

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

fn sidecar(executable: &Path, suffix: &str) -> PathBuf {
    let mut name = executable.as_os_str().to_owned();
    name.push(suffix);
    PathBuf::from(name)
}

struct Relay {
    child: Child,
    input: Option<ChildStdin>,
    output: BufReader<ChildStdout>,
}

impl Relay {
    fn start(executable: &Path) -> Result<Self> {
        let real = fs::read_to_string(sidecar(executable, ".real"))?;
        let mut child = Command::new(real.trim())
            .arg("--worker")
            .env_remove("DISCORD_TOKEN")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()?;
        let input = child.stdin.take().ok_or("missing stdin")?;
        let output = child.stdout.take().ok_or("missing stdout")?;
        Ok(Self {
            child,
            input: Some(input),
            output: BufReader::new(output),
        })
    }

    fn exchange(&mut self, request: &str) -> Result<String> {
        let input = self.input.as_mut().ok_or("closed stdin")?;
        input.write_all(request.as_bytes())?;
        input.flush()?;
        let mut response = String::new();
        if self.output.read_line(&mut response)? == 0 {
            return Err("production worker exited".into());
        }
        Ok(response)
    }
}

impl Drop for Relay {
    fn drop(&mut self) {
        // EOF also lets the production Worker terminate if the relay is killed
        // externally before this destructor runs.
        drop(self.input.take());
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn event(executable: &Path, value: &str) -> Result<()> {
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(sidecar(executable, ".events"))?;
    writeln!(file, "{value}")?;
    file.flush()?;
    Ok(())
}

fn wait_release(executable: &Path) -> Result<()> {
    let deadline = Instant::now() + Duration::from_secs(15);
    while !sidecar(executable, ".release").exists() {
        if Instant::now() >= deadline {
            event(executable, "gate-timeout")?;
            return Err("fixture gate timed out".into());
        }
        thread::sleep(Duration::from_millis(10));
    }
    Ok(())
}

// Production serializes these trusted messages as compact JSON. This small
// scanner changes only a top-level scalar in its Ready response; it is not an
// alternative implementation of the production protocol or JSON parser.
fn replace_scalar(json: &mut String, field: &str, replacement: &str) -> Result<()> {
    let key = format!("\"{field}\":");
    let mut start = json.find(&key).ok_or("missing Ready field")? + key.len();
    let bytes = json.as_bytes();
    while bytes.get(start).is_some_and(u8::is_ascii_whitespace) {
        start += 1;
    }
    let mut end = start;
    if bytes.get(start) == Some(&b'"') {
        end += 1;
        loop {
            match bytes.get(end) {
                Some(b'\\') => end += 2,
                Some(b'"') => {
                    end += 1;
                    break;
                }
                Some(_) => end += 1,
                None => return Err("unterminated Ready field".into()),
            }
        }
    } else {
        while bytes.get(end).is_some_and(|b| *b != b',' && *b != b'}') {
            end += 1;
        }
    }
    json.replace_range(start..end, replacement);
    Ok(())
}

fn request_type(line: &str) -> Result<&'static str> {
    [
        "health",
        "hello",
        "dispatch",
        "snapshot",
        "restore",
        "restore_event_stream",
    ]
    .into_iter()
    .find(|kind| line.contains(&format!("\"type\":\"{kind}\"")))
    .ok_or_else(|| "unsupported fixture request".into())
}

fn run() -> Result<u8> {
    let executable = std::env::current_exe()?;
    let mode = executable
        .file_stem()
        .and_then(|name| name.to_str())
        .ok_or("invalid fixture name")?;
    if ![
        "v1",
        "v2",
        "count-gated",
        "ready-gated",
        "crash-before-ready",
        "hello-version",
        "hello-capability",
        "crash-once",
    ]
    .contains(&mode)
    {
        return Err("unknown fixture mode".into());
    }
    let mut relay = Relay::start(&executable)?;
    let stdin = io::stdin();
    let mut input = stdin.lock();
    let stdout = io::stdout();
    let mut output = stdout.lock();
    let mut initial = true;
    loop {
        let mut line = String::new();
        if input.read_line(&mut line)? == 0 {
            return Ok(0);
        }
        let kind = request_type(&line)?;
        event(&executable, kind)?;
        if initial && kind == "health" && ["crash-before-ready", "ready-gated"].contains(&mode) {
            event(&executable, "health-blocked")?;
            wait_release(&executable)?;
            if mode == "crash-before-ready" {
                event(&executable, "crashed-before-ready")?;
                return Ok(71);
            }
        }
        if kind == "hello" && ["hello-version", "hello-capability"].contains(&mode) {
            let identity = std::env::var("BOT_WORKER_IPC_IDENTITY")?;
            let version = if mode == "hello-version" { 99 } else { 5 };
            let required = if mode == "hello-capability" {
                "[\"snapshot-v3\",\"unknown-required\"]"
            } else {
                "[\"snapshot-v3\"]"
            };
            // The manager can kill us as soon as it reads the invalid Hello.
            // This marker records its emission attempt, before that race.
            event(&executable, "hello-rejected")?;
            writeln!(
                output,
                "{{\"type\":\"hello\",\"hello\":{{\"protocol_min\":{version},\"protocol_max\":{version},\"identity\":{identity},\"required_capabilities\":{required},\"optional_capabilities\":[\"hello-v1\"]}}}}"
            )?;
            output.flush()?;
            continue;
        }
        let slow = kind == "dispatch" && line.contains("!slow");
        if kind == "dispatch" && mode == "count-gated" && line.contains("\"content\":\"!count\"") {
            event(&executable, "stateful-started")?;
            wait_release(&executable)?;
        }
        if slow {
            event(&executable, "slow-started")?;
            wait_release(&executable)?;
            if line.contains("!slow-five") {
                thread::sleep(Duration::from_secs(5));
            }
            line = line
                .replace("!slow-five", "!ping")
                .replace("!slow", "!ping");
            event(&executable, "slow-forwarded")?;
        }
        let mut response = relay.exchange(&line)?;
        if initial && kind == "health" {
            replace_scalar(&mut response, "build", &format!("\"{mode}\""))?;
            replace_scalar(&mut response, "pid", &std::process::id().to_string())?;
        }
        if kind == "dispatch" && mode == "crash-once" {
            match OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(sidecar(&executable, ".crashed"))
            {
                Ok(_) => {
                    event(&executable, "crashed-after-result")?;
                    return Ok(72);
                }
                Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
                Err(error) => return Err(error.into()),
            }
        }
        output.write_all(response.as_bytes())?;
        output.flush()?;
        if initial && kind == "health" {
            event(&executable, "ready")?;
        }
        if kind == "restore" && response.contains("\"type\":\"restored\"") {
            event(&executable, "restore-completed")?;
        }
        if slow {
            event(&executable, "slow-completed")?;
        }
        initial = false;
    }
}

fn main() -> ExitCode {
    match run() {
        Ok(code) => ExitCode::from(code),
        Err(_) => {
            eprintln!("hot swap fixture failed");
            ExitCode::from(70)
        }
    }
}
