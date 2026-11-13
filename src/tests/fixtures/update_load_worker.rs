//! Test-only stdio proxy around the real Worker, with bounded fault gates.
//! Sidecars contain only process IDs and fixed lifecycle markers.
use std::{
    fs::{self, OpenOptions},
    io::{self, BufRead, BufReader, Read, Write},
    path::{Path, PathBuf},
    process::{Child, ChildStdin, ChildStdout, Command, ExitCode, Stdio},
    thread,
    time::{Duration, Instant},
};

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;
const FRAME_LIMIT: u64 = 1024 * 1024;

fn sidecar(executable: &Path, suffix: &str) -> PathBuf {
    let mut name = executable.as_os_str().to_owned();
    name.push(suffix);
    name.into()
}

fn frame(reader: &mut impl BufRead) -> Result<Option<String>> {
    let mut bytes = Vec::new();
    reader.take(FRAME_LIMIT + 1).read_until(b'\n', &mut bytes)?;
    if bytes.is_empty() {
        return Ok(None);
    }
    if bytes.len() as u64 > FRAME_LIMIT || bytes.last() != Some(&b'\n') {
        return Err("invalid fixture frame".into());
    }
    Ok(Some(String::from_utf8(bytes)?))
}

// The real Worker serializes trusted JSON. Inspect only fixed protocol keys;
// never persist or display a request, response, environment, or error payload.
fn scalar_range(json: &str, field: &str) -> Result<std::ops::Range<usize>> {
    let key = format!("\"{field}\":");
    let mut start = json.find(&key).ok_or("missing protocol field")? + key.len();
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
                None => return Err("unterminated protocol scalar".into()),
            }
        }
    } else {
        while bytes
            .get(end)
            .is_some_and(|byte| *byte != b',' && *byte != b'}')
        {
            end += 1;
        }
    }
    Ok(start..end)
}

fn kind(json: &str) -> Result<&str> {
    Ok(json[scalar_range(json, "type")?].trim_matches('"'))
}

struct Relay {
    child: Child,
    input: Option<ChildStdin>,
    output: BufReader<ChildStdout>,
}

impl Relay {
    fn start(executable: &Path) -> Result<Self> {
        let program = fs::read_to_string(sidecar(executable, ".real"))?;
        // The owning Manager has already cleared credentials and supplied only
        // Worker identity. Preserve that isolated launch environment unchanged.
        let mut child = Command::new(program.trim())
            .arg("--worker")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()?;
        let input = child.stdin.take().ok_or("missing Worker input")?;
        let output = BufReader::new(child.stdout.take().ok_or("missing Worker output")?);
        let relay = Self {
            child,
            input: Some(input),
            output,
        };
        fs::write(
            sidecar(executable, &format!(".{}.started", std::process::id())),
            format!(
                "{{\"relay_pid\":{},\"worker_pid\":{}}}",
                std::process::id(),
                relay.child.id()
            ),
        )?;
        Ok(relay)
    }
}

impl Drop for Relay {
    fn drop(&mut self) {
        drop(self.input.take());
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn run() -> Result<u8> {
    if std::env::args_os().skip(1).collect::<Vec<_>>() != [std::ffi::OsString::from("--worker")] {
        return Err("fixture requires Worker mode".into());
    }
    let executable = std::env::current_exe()?;
    let mode = fs::read_to_string(sidecar(&executable, ".mode"))?;
    let mode = mode.trim();
    if !["pass", "hold-restore", "crash-restore", "crash-dispatch"].contains(&mode) {
        return Err("invalid fixture mode".into());
    }
    let mut relay = Relay::start(&executable)?;
    let mut input = io::stdin().lock();
    let mut output = io::stdout().lock();
    let mut initial_health = true;
    let mut restored = false;
    while let Some(request) = frame(&mut input)? {
        let request_kind = kind(&request)?;
        let stdin = relay.input.as_mut().ok_or("closed Worker input")?;
        stdin.write_all(request.as_bytes())?;
        stdin.flush()?;
        // This negotiated control message has deliberately no response.
        if request_kind == "begin_drain" {
            continue;
        }
        let count = if request_kind == "sequenced_ready" {
            2
        } else {
            1
        };
        for _ in 0..count {
            let mut response = frame(&mut relay.output)?.ok_or("Worker output ended")?;
            if request_kind == "health" && initial_health {
                let range = scalar_range(&response, "pid")?;
                response.replace_range(range, &std::process::id().to_string());
                initial_health = false;
            } else if kind(&response)? == "api_result_accepted" {
                // Manager authenticates results against its direct child PID.
                // The proxy represents that identity; the handler is unchanged.
                let range = scalar_range(&response, "pid")?;
                response.replace_range(range, &std::process::id().to_string());
            }
            if request_kind == "restore" && kind(&response)? == "restored" {
                if mode == "crash-restore" {
                    match OpenOptions::new().write(true).create_new(true)
                        .open(sidecar(&executable, ".crash-once")) {
                        Ok(_) => {
                            fs::write(sidecar(&executable, &format!(".{}.crashed", std::process::id())), [])?;
                            return Ok(86);
                        }
                        Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
                        Err(error) => return Err(error.into()),
                    }
                }
                restored = true;
                fs::write(
                    sidecar(&executable, &format!(".{}.restored", std::process::id())),
                    [],
                )?;
                if mode == "hold-restore" {
                    let release = sidecar(&executable, ".release");
                    let deadline = Instant::now() + Duration::from_secs(10);
                    while !release.exists() {
                        if Instant::now() >= deadline {
                            return Err("fixture restore gate timed out".into());
                        }
                        thread::sleep(Duration::from_millis(5));
                    }
                }
            }
            let dispatch = ["dispatch", "slash_command", "command"]
                .into_iter()
                .any(|name| request.contains(&format!("\"type\":\"{name}\"")));
            if mode == "crash-dispatch" && restored && dispatch {
                // Both pool slots and later recovery processes use this same
                // artifact. Claim the injection atomically, once per path.
                match OpenOptions::new()
                    .write(true)
                    .create_new(true)
                    .open(sidecar(&executable, ".crash-once"))
                {
                    Ok(_) => {
                        fs::write(
                            sidecar(&executable, &format!(".{}.crashed", std::process::id())),
                            [],
                        )?;
                        return Ok(86);
                    }
                    Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
                    Err(error) => return Err(error.into()),
                }
            }
            output.write_all(response.as_bytes())?;
            output.flush()?;
        }
    }
    Ok(0)
}

fn main() -> ExitCode {
    match run() {
        Ok(code) => ExitCode::from(code),
        Err(_) => {
            eprintln!("update load fixture failed");
            ExitCode::from(70)
        }
    }
}
