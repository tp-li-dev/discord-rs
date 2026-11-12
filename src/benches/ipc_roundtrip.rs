//! Real --worker processes, newline-delimited JSON, closed-loop load.
//! Run with --self-test for benchmark checks (harness = false).
use serde_json::{Value, json};
use src::protocol::{BotId, GenerationId, Hello, Identity, OwnershipEpoch, Shard};
use std::{
    path::Path,
    process::Stdio,
    time::{Duration, Instant},
};
use tokio::{
    io::{AsyncBufRead, AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    process::{Child, ChildStdin, ChildStdout, Command},
    sync::Barrier,
    task::JoinSet,
    time::timeout,
};

type Result<T> = std::result::Result<T, &'static str>;
const LIMIT: u64 = 1024 * 1024;
const DEADLINE: Duration = Duration::from_secs(5);

fn parse_count(value: Option<&str>, default: usize) -> Result<usize> {
    match value {
        None => Ok(default),
        Some(value) if !value.is_empty() && value.bytes().all(|b| b.is_ascii_digit()) => value
            .parse::<usize>()
            .ok()
            .filter(|n| (1..=1_000_000).contains(n))
            .ok_or("counts must be in 1..=1000000"),
        Some(_) => Err("counts must be in 1..=1000000"),
    }
}

fn count(name: &str, default: usize) -> Result<usize> {
    let value = std::env::var(name).map_err(|_| "invalid benchmark count environment");
    match value {
        Ok(value) => parse_count(Some(&value), default),
        Err(_) if std::env::var_os(name).is_none() => Ok(default),
        Err(error) => Err(error),
    }
}

fn quota(total: usize, concurrency: usize, lane: usize) -> usize {
    total / concurrency + usize::from(lane < total % concurrency)
}

// Integer nearest-rank in permille, including p99.9. Empty sets have no percentile.
fn percentiles(times: &mut [u128]) -> Option<[u128; 4]> {
    if times.is_empty() {
        return None;
    }
    times.sort_unstable();
    Some([500, 950, 990, 999].map(|p| times[(times.len() * p).div_ceil(1000) - 1]))
}

async fn read_frame(reader: &mut (impl AsyncBufRead + Unpin)) -> Result<Value> {
    let mut bytes = Vec::new();
    reader
        .take(LIMIT + 1)
        .read_until(b'\n', &mut bytes)
        .await
        .map_err(|_| "IPC read failed")?;
    if bytes.is_empty() || bytes.len() as u64 > LIMIT || bytes.last() != Some(&b'\n') {
        return Err("IPC EOF, oversized or unterminated frame");
    }
    serde_json::from_slice(&bytes).map_err(|_| "invalid IPC JSON")
}

fn valid_pong(reply: &Value) -> bool {
    reply == &json!({"type":"result", "content":"Pong! 🦀", "state":{"counts":{}, "jobs":{}}})
}

struct Worker {
    child: Child,
    input: Option<ChildStdin>,
    output: BufReader<ChildStdout>,
}

impl Worker {
    async fn start() -> Result<Self> {
        let identity = Identity {
            bot_id: BotId(42),
            shard: Shard::new(0, 1).map_err(|_| "invalid synthetic shard")?,
            generation_id: GenerationId(1),
            ownership_epoch: OwnershipEpoch(0),
        };
        let hello = Hello {
            protocol_min: 5,
            protocol_max: 5,
            identity: identity.clone(),
            required_capabilities: vec!["snapshot-v3".into(), "hello-v1".into()],
            optional_capabilities: vec![],
        };
        let mut command = Command::new(Path::new(env!("CARGO_BIN_EXE_src")));
        // No inherited Bot tokens, credentials, deployment config or user logs.
        command.arg("--worker").env_clear();
        // Windows system DLL lookup can require these non-secret OS paths.
        for name in ["SystemRoot", "WINDIR"] {
            if let Some(value) = std::env::var_os(name) {
                command.env(name, value);
            }
        }
        command.env(
            "BOT_WORKER_IPC_IDENTITY",
            serde_json::to_string(&identity).map_err(|_| "identity encoding failed")?,
        );
        let mut child = command
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .kill_on_drop(true)
            .spawn()
            .map_err(|_| "worker spawn failed")?;
        let input = child.stdin.take().ok_or("missing worker stdin")?;
        let output = BufReader::new(child.stdout.take().ok_or("missing worker stdout")?);
        let mut worker = Self {
            child,
            input: Some(input),
            output,
        };
        let startup = async {
            let ready = worker.exchange(&json!({"type":"health"})).await?;
            if ready["type"] != "ready"
                || ready["protocol"] != 5
                || ready["pid"].as_u64() != worker.child.id().map(u64::from)
            {
                return Err("worker readiness mismatch");
            }
            let reply = worker
                .exchange(&json!({"type":"hello", "hello":hello}))
                .await?;
            if reply["type"] != "hello" {
                return Err("worker handshake rejected");
            }
            let peer: Hello = serde_json::from_value(reply["hello"].clone())
                .map_err(|_| "invalid worker hello")?;
            hello
                .negotiate(&peer)
                .map_err(|_| "worker negotiation failed")?;
            Ok(())
        }
        .await;
        if let Err(error) = startup {
            worker.close().await.ok();
            return Err(error);
        }
        Ok(worker)
    }

    async fn exchange(&mut self, request: &Value) -> Result<Value> {
        timeout(DEADLINE, async {
            let mut bytes = serde_json::to_vec(request).map_err(|_| "IPC encoding failed")?;
            bytes.push(b'\n');
            let input = self.input.as_mut().ok_or("worker stdin closed")?;
            input
                .write_all(&bytes)
                .await
                .map_err(|_| "IPC write failed")?;
            input.flush().await.map_err(|_| "IPC flush failed")?;
            read_frame(&mut self.output).await
        })
        .await
        .map_err(|_| "IPC timeout")?
    }

    async fn ping(&mut self) -> Result<()> {
        let reply = self
            .exchange(&json!({"type":"dispatch", "key":0, "content":"!ping"}))
            .await?;
        if valid_pong(&reply) {
            Ok(())
        } else {
            Err("worker pong mismatch")
        }
    }

    async fn close(mut self) -> Result<()> {
        drop(self.input.take()); // EOF requests normal Worker exit.
        match timeout(DEADLINE, self.child.wait()).await {
            Ok(Ok(status)) if status.success() => Ok(()),
            Ok(_) => Err("worker exit failed"),
            Err(_) => {
                self.child.kill().await.map_err(|_| "worker kill failed")?;
                Err("worker exit timeout (killed and reaped)")
            }
        }
    }
}

struct Phase {
    workers: Vec<Worker>,
    times: Vec<u128>,
    failures: usize,
    skipped: usize,
    elapsed: Duration,
}

async fn phase(workers: Vec<Worker>, total: usize) -> Result<Phase> {
    let concurrency = workers.len();
    let ready = std::sync::Arc::new(Barrier::new(concurrency + 1));
    let barrier = std::sync::Arc::new(Barrier::new(concurrency + 1));
    let done = std::sync::Arc::new(Barrier::new(concurrency + 1));
    let mut tasks = JoinSet::new();
    for (lane, mut worker) in workers.into_iter().enumerate() {
        let barrier = barrier.clone();
        let ready = ready.clone();
        let done = done.clone();
        let n = quota(total, concurrency, lane);
        tasks.spawn(async move {
            let mut times = Vec::with_capacity(n);
            let mut failures = 0;
            let mut skipped = 0;
            ready.wait().await;
            barrier.wait().await;
            for i in 0..n {
                let start = Instant::now();
                let result = worker.ping().await;
                let elapsed = start.elapsed().as_nanos();
                if result.is_err() {
                    failures += 1;
                    skipped = n - i - 1;
                    break; // Never retry or reuse a possibly desynchronized stream.
                }
                times.push(elapsed);
            }
            let finished = Instant::now();
            done.wait().await;
            (worker, times, failures, skipped, finished)
        });
    }
    let mut result = Phase {
        workers: Vec::with_capacity(concurrency),
        times: Vec::with_capacity(total),
        failures: 0,
        skipped: 0,
        elapsed: Duration::ZERO,
    };
    // All lane buffers are allocated before the common timing boundary.
    ready.wait().await;
    let start = Instant::now();
    barrier.wait().await;
    done.wait().await;
    while let Some(task) = tasks.join_next().await {
        let (worker, times, failures, skipped, finished) =
            task.map_err(|_| "benchmark task failed")?;
        result.workers.push(worker);
        result.times.extend(times);
        result.failures += failures;
        result.skipped += skipped;
        result.elapsed = result.elapsed.max(finished.duration_since(start));
    }
    if result.times.len() + result.failures + result.skipped != total {
        return Err("benchmark accounting mismatch");
    }
    Ok(result)
}

async fn close_all(workers: Vec<Worker>) -> usize {
    let mut tasks = JoinSet::new();
    for worker in workers {
        tasks.spawn(worker.close());
    }
    let mut failures = 0;
    while let Some(result) = tasks.join_next().await {
        failures += usize::from(!matches!(result, Ok(Ok(()))));
    }
    failures
}

async fn run_case(concurrency: usize, warmup: usize, samples: usize) -> Result<()> {
    let mut workers = Vec::with_capacity(concurrency);
    for _ in 0..concurrency {
        match Worker::start().await {
            Ok(worker) => workers.push(worker),
            Err(error) => {
                let teardown = close_all(workers).await;
                eprintln!(
                    "concurrency={concurrency},startup_failures=1,teardown_failures={teardown},measurement=not_run"
                );
                return Err(error);
            }
        }
    }
    let warmed = phase(workers, warmup).await?;
    if warmed.failures != 0 {
        let teardown = close_all(warmed.workers).await;
        eprintln!(
            "concurrency={concurrency},warmup_successes={},warmup_failures={},warmup_skipped={},teardown_failures={teardown},measurement=not_run",
            warmed.times.len(),
            warmed.failures,
            warmed.skipped
        );
        return Err("warmup failed");
    }
    let mut measured = phase(warmed.workers, samples).await?;
    let ranks = percentiles(&mut measured.times);
    let throughput = measured.times.len() as f64 / measured.elapsed.as_secs_f64();
    let teardown = close_all(measured.workers).await;
    let ranks = ranks
        .map(|values| values.map(|n| n.to_string()))
        .unwrap_or_else(|| std::array::from_fn(|_| "NA".to_owned()));
    println!(
        "{concurrency},{concurrency},{warmup},{samples},{},{},{},{teardown},{},{throughput:.3},{},{},{},{}",
        measured.times.len(),
        measured.failures,
        measured.skipped,
        measured.elapsed.as_nanos(),
        ranks[0],
        ranks[1],
        ranks[2],
        ranks[3]
    );
    if measured.failures != 0 || measured.skipped != 0 || teardown != 0 {
        Err("measurement or teardown failed; see counts")
    } else {
        Ok(())
    }
}

async fn self_test() -> Result<()> {
    assert_eq!(parse_count(None, 7)?, 7);
    assert_eq!(parse_count(Some("1000000"), 1)?, 1_000_000);
    for invalid in [
        "",
        "0",
        "1000001",
        "-1",
        "+1",
        " 1",
        "1.0",
        "999999999999999999999999",
    ] {
        assert!(parse_count(Some(invalid), 1).is_err());
    }
    assert_eq!(percentiles(&mut []), None);
    assert_eq!(percentiles(&mut [7]), Some([7; 4]));
    let mut times: Vec<u128> = (1..=10_000).rev().collect();
    assert_eq!(percentiles(&mut times), Some([5000, 9500, 9900, 9990]));
    for concurrency in [1, 4, 16] {
        for total in [1, 15, 16, 17, 10_001] {
            let quotas: Vec<_> = (0..concurrency)
                .map(|i| quota(total, concurrency, i))
                .collect();
            assert_eq!(quotas.iter().sum::<usize>(), total);
            assert!(quotas.iter().max().unwrap() - quotas.iter().min().unwrap() <= 1);
        }
    }
    assert!(valid_pong(
        &json!({"type":"result", "content":"Pong! 🦀", "state":{"counts":{}, "jobs":{}}})
    ));
    for reply in [
        json!(null),
        json!({"type":"rejected"}),
        json!({"type":"result", "content":"wrong"}),
    ] {
        assert!(!valid_pong(&reply));
    }
    for bytes in [b"".as_slice(), b"{}", b"invalid\n", b"{}\n"] {
        assert_eq!(
            read_frame(&mut BufReader::new(bytes)).await.is_ok(),
            bytes == b"{}\n"
        );
    }
    let mut boundary = vec![b' '; LIMIT as usize - 3];
    boundary.extend_from_slice(b"{}\n");
    assert!(
        read_frame(&mut BufReader::new(boundary.as_slice()))
            .await
            .is_ok()
    );
    boundary.insert(0, b' ');
    assert!(
        read_frame(&mut BufReader::new(boundary.as_slice()))
            .await
            .is_err()
    );
    let (reader, _writer) = tokio::io::duplex(64);
    assert!(
        timeout(
            Duration::from_millis(20),
            read_frame(&mut BufReader::new(reader))
        )
        .await
        .is_err()
    );
    let mut worker = Worker::start().await?;
    worker.ping().await?;
    worker
        .child
        .kill()
        .await
        .map_err(|_| "self-test worker kill failed")?;
    assert!(worker.ping().await.is_err());
    let mut failed = phase(vec![worker], 3).await?;
    assert_eq!(failed.failures, 1);
    assert_eq!(failed.skipped, 2);
    assert_eq!(percentiles(&mut failed.times), None);
    // A deliberately killed process must be reported as teardown failure.
    assert_eq!(close_all(failed.workers).await, 1);
    println!(
        "self-test passed: counts, quotas, percentiles, reply validation, frame bounds/EOF/JSON/timeout, real worker ping/exit, failure/skipped accounting"
    );
    Ok(())
}

fn main() -> Result<()> {
    let args: Vec<_> = std::env::args()
        .skip(1)
        .filter(|arg| arg != "--bench")
        .collect();
    if !args.is_empty() && args != ["--self-test"] {
        return Err("usage: ipc_roundtrip [--self-test]");
    }
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|_| "benchmark runtime failed")?;
    if args == ["--self-test"] {
        return runtime.block_on(self_test());
    }
    let warmup = count("IPC_ROUNDTRIP_WARMUP", 1_000)?;
    let samples = count("IPC_ROUNDTRIP_SAMPLES", 10_000)?;
    println!(
        "# real worker JSON IPC; unit=ns; percentiles=nearest-rank; closed-loop; parent_runtime=current_thread"
    );
    println!(
        "concurrency,workers,warmup,samples,successes,failures,skipped,teardown_failures,elapsed_ns,events_per_second,p50_ns,p95_ns,p99_ns,p99_9_ns"
    );
    let mut failed = false;
    for concurrency in [1, 4, 16] {
        if let Err(error) = runtime.block_on(run_case(concurrency, warmup, samples)) {
            eprintln!("concurrency={concurrency}: {error}");
            failed = true;
        }
    }
    if failed {
        Err("one or more benchmark cases failed")
    } else {
        Ok(())
    }
}
