use super::{control::CommandRequest, *};
use crate::update_rollback::{Artifact, Component as UpdateComponent, UpdateControlRequest};
use crate::{
    child_recovery::{
        ChildKey, ChildRecovery, ChildRole, IdentifyAdmission, RecoveryConfig, RecoveryEvent,
        RecoveryEventKind, RecoveryHost, RecoveryState, StartMode, StartRequest,
    },
    gateway_switch::{
        Authority, Effect, Event, Failure, GatewaySwitch, IdentifyBlock, IdentifyDecision,
        MockIdentifyLimit, MockIdentifyScheduler, Phase,
    },
    ownership::{OwnershipCoordinator, OwnershipKey, StandaloneCoordinator},
    protocol::{BotId, GenerationId, Identity, OwnershipEpoch, Shard},
    shard_runtime::test_switch::{self as wire, Action, CoreLaunch, State},
};
use serde::{Serialize, de::DeserializeOwned};
use serde_json::{Value, json};
use std::sync::{
    Arc, Mutex as StdMutex,
    atomic::{AtomicBool, Ordering},
};
use subtle::ConstantTimeEq;
use tokio::{
    io::{AsyncBufRead, AsyncBufReadExt, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader},
    net::{TcpListener, TcpStream},
    sync::{Mutex, Notify, mpsc},
    task::JoinHandle,
};

const IO_TIMEOUT: Duration = Duration::from_secs(5);
const LEASE: Duration = Duration::from_secs(120);
type Result<T> = std::result::Result<T, &'static str>;

const HANDOFF_EVENTS: usize = 1024;
const HANDOFF_BYTES: usize = 2 * 1024 * 1024;
const RECENT_EVENTS: usize = 4096;

#[derive(Default)]
struct IngressState {
    active: Option<u64>,
    holding: bool,
    pending: VecDeque<(u64, Value, usize)>,
    pending_bytes: usize,
    shadow: VecDeque<(u64, Value, usize)>,
    shadow_bytes: usize,
    recent: HashSet<u64>,
    recent_order: VecDeque<u64>,
    unaccepted: HashMap<u64, HashSet<u64>>,
}

#[derive(Default)]
struct Ingress {
    state: StdMutex<IngressState>,
    changed: Notify,
}

enum IngressRoute {
    Forward(Value),
    Held,
    Suppressed,
    Rejected,
}

impl IngressState {
    fn forget_shadow(&mut self, id: u64) {
        if let Some(index) = self.shadow.iter().position(|(event, _, _)| *event == id)
            && let Some((_, _, bytes)) = self.shadow.remove(index)
        {
            self.shadow_bytes -= bytes;
        }
    }

    fn has_capacity(&self, event: u64, bytes: usize) -> bool {
        let existing = self.shadow.iter().find(|(id, _, _)| *id == event);
        let entries = self.pending.len() + self.shadow.len() - usize::from(existing.is_some());
        let retained =
            self.pending_bytes + self.shadow_bytes - existing.map_or(0, |(_, _, bytes)| *bytes);
        entries < HANDOFF_EVENTS && bytes <= HANDOFF_BYTES.saturating_sub(retained)
    }

    fn remember(&mut self, id: u64) {
        self.forget_shadow(id);
        self.recent.insert(id);
        self.recent_order.push_back(id);
        if self.recent_order.len() > RECENT_EVENTS
            && let Some(expired) = self.recent_order.pop_front()
        {
            self.recent.remove(&expired);
        }
    }

    fn route(&mut self, generation: u64, value: Value, open: bool) -> IngressRoute {
        let Some(event) = value["event_id"].as_u64() else {
            return if open {
                IngressRoute::Forward(value)
            } else {
                IngressRoute::Suppressed
            };
        };
        if self.recent.contains(&event) {
            return IngressRoute::Suppressed;
        }
        let awaiting_acceptance = self
            .unaccepted
            .get(&generation)
            .is_some_and(|events| !events.is_empty());
        if self.holding
            || (self.active == Some(generation)
                && (!self.pending.is_empty() || awaiting_acceptance))
        {
            let bytes = serde_json::to_vec(&value).map_or(usize::MAX, |bytes| bytes.len());
            if !self.has_capacity(event, bytes) {
                return IngressRoute::Rejected;
            }
            self.remember(event);
            self.pending_bytes += bytes;
            self.pending.push_back((event, value, bytes));
            return IngressRoute::Held;
        }
        if open && self.active == Some(generation) {
            self.remember(event);
            self.unaccepted.entry(generation).or_default().insert(event);
            IngressRoute::Forward(value)
        } else {
            // Retain unclaimed shadow copies in their observed stream order.
            // A shadow may read N before Pause while Active still has N in its
            // socket. Losing that copy could put a later N+1 ahead of N during
            // the handoff. Active admission removes the cached duplicate.
            if self.active.is_some() && !self.shadow.iter().any(|(id, _, _)| *id == event) {
                let bytes = serde_json::to_vec(&value).map_or(usize::MAX, |bytes| bytes.len());
                if !self.has_capacity(event, bytes) {
                    return IngressRoute::Rejected;
                }
                self.shadow_bytes += bytes;
                self.shadow.push_back((event, value, bytes));
            }
            // Cached shadow input is still not an admission or REST effect.
            IngressRoute::Suppressed
        }
    }

    fn next(&mut self, generation: u64) -> Option<Value> {
        if self.holding
            || self.active != Some(generation)
            || self
                .unaccepted
                .get(&generation)
                .is_some_and(|events| !events.is_empty())
        {
            return None;
        }
        let (event, value, bytes) = self.pending.pop_front()?;
        self.pending_bytes -= bytes;
        self.unaccepted.entry(generation).or_default().insert(event);
        Some(value)
    }
}

impl Ingress {
    fn activate(&self, generation: u64) {
        let mut state = self.state.lock().unwrap();
        if !state.holding {
            state.active = Some(generation);
        }
    }

    fn release(&self, generation: u64) {
        let mut state = self.state.lock().unwrap();
        state.active = Some(generation);
        state.holding = false;
        drop(state);
        self.changed.notify_waiters();
    }

    fn begin_pause(&self, generation: u64) {
        let mut state = self.state.lock().unwrap();
        if state.active == Some(generation) {
            state.active = None;
            state.holding = true;
            while let Some((event, value, bytes)) = state.shadow.pop_front() {
                state.shadow_bytes -= bytes;
                if !state.recent.contains(&event) {
                    state.remember(event);
                    state.pending_bytes += bytes;
                    state.pending.push_back((event, value, bytes));
                }
            }
        }
    }

    async fn wait_accepted(&self, generation: u64) -> Result<()> {
        // The control socket and event socket are independent. Wait until the
        // Gateway has actually reserved every forwarded response, not merely
        // until the daemon has written the event into its TCP send buffer.
        tokio::time::timeout(IO_TIMEOUT, async {
            loop {
                let changed = self.changed.notified();
                tokio::pin!(changed);
                changed.as_mut().enable();
                if self
                    .state
                    .lock()
                    .unwrap()
                    .unaccepted
                    .get(&generation)
                    .is_none_or(HashSet::is_empty)
                {
                    return;
                }
                changed.await;
            }
        })
        .await
        .map_err(|_| "Gateway input admission barrier timed out")
    }

    fn accepted(&self, generation: u64, event: u64) -> Result<()> {
        let mut state = self.state.lock().unwrap();
        let Some(events) = state.unaccepted.get_mut(&generation) else {
            return Err("Gateway input admission acknowledgement mismatch");
        };
        if !events.remove(&event) {
            return Err("Gateway input admission acknowledgement mismatch");
        }
        if events.is_empty() {
            state.unaccepted.remove(&generation);
        }
        drop(state);
        self.changed.notify_waiters();
        Ok(())
    }

    fn close(&self) {
        let mut state = self.state.lock().unwrap();
        state.active = None;
        state.holding = false;
    }

    fn has_pending_input(&self) -> bool {
        let state = self.state.lock().unwrap();
        !state.pending.is_empty() || !state.shadow.is_empty() || !state.unaccepted.is_empty()
    }
}

async fn checked_artifact(
    check: JoinHandle<Result<PathBuf>>,
    deadline: Duration,
) -> Result<PathBuf> {
    // Timeout may leave read-only hashing on the blocking worker. It cannot
    // launch a child, and this actor resumes its lease/monitor loop promptly.
    tokio::time::timeout(deadline, check)
        .await
        .map_err(|_| "artifact verification timed out; candidate not launched")?
        .map_err(|_| "artifact verification task failed")?
}

async fn read_json<T: DeserializeOwned>(
    reader: &mut (impl AsyncBufRead + Unpin),
    limit: usize,
) -> Result<T> {
    let mut bytes = Vec::new();
    let count = reader
        .take(limit as u64 + 1)
        .read_until(b'\n', &mut bytes)
        .await
        .map_err(|_| "Gateway IPC read failed")?;
    if count == 0 || count > limit || bytes.last() != Some(&b'\n') {
        return Err("invalid Gateway IPC frame");
    }
    serde_json::from_slice(&bytes).map_err(|_| "invalid Gateway IPC message")
}

async fn write_json(
    writer: &mut (impl AsyncWrite + Unpin),
    value: &impl Serialize,
    limit: usize,
) -> Result<()> {
    let bytes = json_frame(value, limit)?;
    writer
        .write_all(&bytes)
        .await
        .map_err(|_| "Gateway IPC write failed")?;
    writer.flush().await.map_err(|_| "Gateway IPC flush failed")
}

fn json_frame(value: &impl Serialize, limit: usize) -> Result<Vec<u8>> {
    let mut bytes = serde_json::to_vec(value).map_err(|_| "Gateway IPC encoding failed")?;
    bytes.push(b'\n');
    if bytes.len() > limit {
        return Err("Gateway IPC frame exceeds limit");
    }
    Ok(bytes)
}

struct Peer {
    stream: BufReader<TcpStream>,
    identity: Identity,
    serial: u64,
    view: Arc<StdMutex<Value>>,
    view_open: Arc<AtomicBool>,
}

impl Peer {
    async fn exchange(&mut self, action: Action, epoch: Option<OwnershipEpoch>) -> Result<State> {
        self.serial = self
            .serial
            .checked_add(1)
            .ok_or("Gateway request id exhausted")?;
        let mut identity = self.identity.clone();
        if let Some(epoch) = epoch {
            identity.ownership_epoch = epoch;
        }
        let request = wire::Control {
            request_id: self.serial,
            identity: identity.clone(),
            action,
        };
        let limit = if matches!(action, Action::Drain) {
            Duration::from_secs(65)
        } else {
            IO_TIMEOUT
        };
        let reply: wire::Reply = tokio::time::timeout(limit, async {
            write_json(self.stream.get_mut(), &request, wire::FRAME_LIMIT).await?;
            read_json(&mut self.stream, wire::FRAME_LIMIT).await
        })
        .await
        .map_err(|_| "Gateway control deadline exceeded")??;
        if reply.request_id != request.request_id || reply.identity != identity {
            return Err("Gateway control acknowledgement identity mismatch");
        }
        self.identity = identity;
        let mut view = self.view.lock().unwrap();
        if !self.view_open.load(Ordering::SeqCst) {
            return Err("Gateway observations are closed");
        }
        view["state"] = json!(reply.state);
        view["epoch"] = json!(reply.identity.ownership_epoch.0);
        view["workers"] = json!(reply.workers);
        Ok(reply.state)
    }
}

struct Gateway {
    // Active child is owned by the Actor recovery engine; update/draining
    // generations retain their handles until the management barrier completes.
    child: Option<Child>,
    input: Option<tokio::process::ChildStdin>,
    peer: Arc<Mutex<Peer>>,
    gate: Arc<AtomicBool>,
    healthy: Arc<AtomicBool>,
    view: Arc<StdMutex<Value>>,
    view_open: Arc<AtomicBool>,
    heartbeat: JoinHandle<()>,
    relay: JoinHandle<Result<()>>,
    ingress: Arc<Ingress>,
    generation: u64,
}

impl Drop for Gateway {
    fn drop(&mut self) {
        self.gate.store(false, Ordering::SeqCst);
        self.heartbeat.abort();
        self.relay.abort();
    }
}

// Data gating complements the real Gateway's Manager admission. Only events
// carrying a semantic identity can be deduplicated and held across the gap;
// unkeyed shadow inputs retain their explicit suppression contract.
async fn relay(
    mut upstream: TcpStream,
    downstream: TcpStream,
    gate: Arc<AtomicBool>,
    ingress: Arc<Ingress>,
    generation: u64,
) -> Result<()> {
    let (up_read, mut up_write) = upstream.split();
    let (down_read, mut down_write) = downstream.into_split();
    let mut up_read = BufReader::new(up_read);
    let mut down_read = BufReader::new(down_read);
    // read_until is cancellation-safe only when the accumulated buffer also
    // survives select! cancellation (e.g. a result arriving mid-input).
    let mut up_frame = Vec::new();
    let mut down_frame = Vec::new();
    let mut inputs_open = true;
    // Keep a partially written event across select cancellation. Reading the
    // Gateway's ACK/results must continue even when its input socket is full.
    let mut outbound: Option<(Vec<u8>, usize)> = None;
    loop {
        let changed = ingress.changed.notified();
        tokio::pin!(changed);
        changed.as_mut().enable();
        let held = if outbound.is_none() && gate.load(Ordering::SeqCst) {
            ingress.state.lock().unwrap().next(generation)
        } else {
            None
        };
        if let Some(value) = held {
            outbound = Some((json_frame(&value, 64 * 1024)?, 0));
        }
        tokio::select! {
            _ = changed => {},
            written = async {
                let (bytes, offset) = outbound.as_ref().expect("pending relay write");
                down_write.write(&bytes[*offset..]).await
            }, if outbound.is_some() => {
                let count = written.map_err(|_| "Gateway IPC write failed")?;
                if count == 0 {
                    return Err("Gateway IPC write ended");
                }
                let (bytes, offset) = outbound.as_mut().expect("pending relay write");
                *offset += count;
                if *offset == bytes.len() {
                    outbound = None;
                }
            }
            input = relay_frame(&mut up_read, &mut up_frame), if inputs_open && outbound.is_none() => {
                match input {
                    Ok(value) => {
                        let inspection = matches!(value["type"].as_str(), Some("status" | "crash"));
                        if inspection {
                            outbound = Some((json_frame(&value, 64 * 1024)?, 0));
                        } else {
                            let id = value["id"].clone();
                            let route = ingress.state.lock().unwrap().route(generation, value, gate.load(Ordering::SeqCst));
                            match route {
                                IngressRoute::Forward(value) => outbound = Some((json_frame(&value, 64 * 1024)?, 0)),
                                IngressRoute::Held => ingress.changed.notify_waiters(),
                                IngressRoute::Suppressed => write_json(&mut up_write, &json!({"type":"suppressed","id":id}), 64*1024).await?,
                                IngressRoute::Rejected => write_json(&mut up_write, &json!({"type":"result","id":id,"outcome":"handoff_full"}), 64*1024).await?,
                            }
                        }
                    }
                    Err(_) => inputs_open = false,
                }
            }
            result = relay_frame(&mut down_read, &mut down_frame) => {
                let value = result?;
                if value["type"] == "accepted" {
                    ingress.accepted(generation, value["event_id"].as_u64().ok_or("invalid Gateway input admission acknowledgement")?)?;
                    continue;
                }
                let stopped = value["type"] == "stopped";
                write_json(&mut up_write, &value, 64 * 1024).await?;
                if stopped { return Ok(()); }
            }
        }
    }
}

async fn relay_frame(
    reader: &mut (impl AsyncBufRead + Unpin),
    frame: &mut Vec<u8>,
) -> Result<Value> {
    const LIMIT: usize = 64 * 1024;
    reader
        .take((LIMIT + 1 - frame.len()) as u64)
        .read_until(b'\n', frame)
        .await
        .map_err(|_| "mock relay read failed")?;
    if frame.is_empty() || frame.len() > LIMIT || frame.last() != Some(&b'\n') {
        return Err("invalid mock relay frame");
    }
    let value = serde_json::from_slice(frame).map_err(|_| "invalid mock relay message")?;
    frame.clear();
    Ok(value)
}

fn generation_spec(original: &ShardRuntimeSpec, generation: u64) -> ShardRuntimeSpec {
    let mut spec = original.clone();
    spec.ipc_path = spec.ipc_path.join(format!("generation-{generation}"));
    for process in [&mut spec.gateway, &mut spec.core] {
        process.state_path = process.state_path.join(format!("generation-{generation}"));
        process.management_path = process
            .management_path
            .join(format!("generation-{generation}"));
        for pair in process.args.chunks_exact_mut(2) {
            match pair[0].to_str() {
                Some("--ipc-path") => pair[1] = spec.ipc_path.as_os_str().to_owned(),
                Some("--state-path") => pair[1] = process.state_path.as_os_str().to_owned(),
                Some("--management-path") => {
                    pair[1] = process.management_path.as_os_str().to_owned()
                }
                _ => {}
            }
        }
    }
    spec
}

// Recovery telemetry can advance between the authenticated control Hello and
// the mock connection's Ready. Compare the stable worker identity proof only.
fn matching_workers(observed: &Value, expected: &[crate::worker::WorkerIdentity]) -> bool {
    observed.as_array().is_some_and(|workers| {
        workers.len() == expected.len()
            && workers.iter().zip(expected).all(|(worker, expected)| {
                worker["index"] == expected.index
                    && worker["pid"] == expected.pid
                    && worker["identity"] == json!(expected.identity)
                    && worker["credential_free"] == expected.credential_free
            })
    })
}

#[derive(Default)]
struct CoreObservations {
    degraded: bool,
    events: Vec<RecoveryEvent>,
    dropped: u64,
}

fn collect_core_observations(
    seen: &mut HashMap<(u64, usize), u64>,
    generation: u64,
    workers: &Value,
    observed: &mut CoreObservations,
) {
    let workers: Vec<crate::worker::WorkerIdentity> =
        serde_json::from_value(workers.clone()).unwrap_or_default();
    for worker in workers {
        if let Some(recovery) = worker.recovery {
            observed.degraded |= matches!(recovery.state, RecoveryState::Degraded(_));
            let sequence = seen.entry((generation, worker.index)).or_default();
            for event in recovery.recent_events {
                if event.sequence > *sequence {
                    observed.dropped = observed
                        .dropped
                        .saturating_add(event.sequence - *sequence - 1);
                    *sequence = event.sequence;
                    observed.events.push(event.event);
                }
            }
        }
    }
}

impl Gateway {
    fn launch(
        spec: &ShardRuntimeSpec,
        gateway_program: &Path,
        core_program: &Path,
        collector: SocketAddr,
        generation: u64,
        ingress: Arc<Ingress>,
    ) -> Result<(Child, JoinHandle<Result<Self>>)> {
        let spec = generation_spec(spec, generation);
        let control = local_listener()?;
        let mock = local_listener()?;
        let identity = Identity {
            bot_id: BotId(
                spec.bot_id
                    .parse()
                    .map_err(|_| "invalid runtime Bot identity")?,
            ),
            shard: Shard::new(spec.shard_id, spec.total_shards)?,
            generation_id: GenerationId(generation),
            ownership_epoch: OwnershipEpoch(0),
        };
        let launch = wire::Launch {
            address: control
                .local_addr()
                .map_err(|_| "cannot inspect Gateway control")?,
            gateway: identity.clone(),
            core: CoreLaunch::from_spec(&spec, core_program, generation)
                .map_err(|_| "invalid Core launch plan")?,
        };
        let mut secret = [0; 32];
        getrandom::fill(&mut secret).map_err(|_| "cannot create Gateway capability")?;
        let mut command = Command::new(gateway_program);
        command
            .args(&spec.gateway.args)
            .arg("--mock-discord")
            .arg(
                mock.local_addr()
                    .map_err(|_| "cannot inspect mock relay")?
                    .to_string(),
            )
            .env_clear()
            .env("BOT_ID", &spec.bot_id)
            .env("SHARD_ID", spec.shard_id.to_string())
            .env("TOTAL_SHARDS", spec.total_shards.to_string())
            .env("GENERATION_ID", generation.to_string())
            .env(
                wire::LAUNCH_ENV,
                serde_json::to_string(&launch).map_err(|_| "cannot encode Gateway launch")?,
            )
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .kill_on_drop(true);
        #[cfg(windows)]
        {
            command.env(
                "SystemRoot",
                std::env::var_os("SystemRoot")
                    .filter(|v| !v.is_empty())
                    .ok_or("missing system environment")?,
            );
            command.creation_flags(0x0000_0200 | 0x0800_0000);
        }
        #[cfg(unix)]
        command.process_group(0);
        let mut child = command.spawn().map_err(|_| "Gateway spawn failed")?;
        let pid = child.id().expect("spawned child has an identity");
        let mut input = child.stdin.take().expect("piped Gateway stdin");
        let ready_task = tokio::spawn(async move {
            let ready = tokio::time::timeout(Duration::from_secs(15), async {
                write_json(&mut input, &wire::Bootstrap { secret }, wire::FRAME_LIMIT).await?;
                let (stream, _) = control
                    .accept()
                    .await
                    .map_err(|_| "Gateway control unavailable")?;
                let mut stream = BufReader::new(stream);
                let hello: wire::Hello = read_json(&mut stream, wire::FRAME_LIMIT).await?;
                if !bool::from(hello.secret.ct_eq(&secret))
                    || hello.identity != identity
                    || hello.pid != pid
                    || hello.workers.len() != 1
                    || !hello.workers[0].credential_free
                    || hello.workers[0].identity.as_ref() != Some(&launch.core.identity)
                {
                    return Err("Gateway handshake rejected");
                }
                let (downstream, _) = mock
                    .accept()
                    .await
                    .map_err(|_| "mock Gateway connection unavailable")?;
                let mut downstream = BufReader::new(downstream);
                let mut ready: Value = read_json(&mut downstream, 64 * 1024).await?;
                if ready["type"] != "ready"
                    || ready["pid"] != pid
                    || ready["bot_id"] != identity.bot_id.0
                    || ready["shard_id"] != spec.shard_id
                    || ready["total_shards"] != spec.total_shards
                    || ready["credential_present"] != false
                    || !matching_workers(&ready["workers"], &hello.workers)
                {
                    return Err("mock Gateway readiness rejected");
                }
                ready["gateway_generation"] = json!(generation);
                ready["test_gateway_switch"] = json!(true);
                let mut upstream = TcpStream::connect(collector)
                    .await
                    .map_err(|_| "mock collector unavailable")?;
                write_json(&mut upstream, &ready, 64 * 1024).await?;
                let view = Arc::new(StdMutex::new(json!({"generation":generation,"pid":pid,
                "state":"standby","epoch":0,"workers":ready["workers"]})));
                Ok((stream, downstream.into_inner(), upstream, view))
            })
            .await;
            let (stream, downstream, upstream, view) = match ready {
                Ok(Ok(ready)) => ready,
                other => {
                    return Err(match other {
                        Ok(Err(error)) => error,
                        _ => "Gateway readiness timed out",
                    });
                }
            };
            let view_open = Arc::new(AtomicBool::new(true));
            let peer = Arc::new(Mutex::new(Peer {
                stream,
                identity,
                serial: 0,
                view: view.clone(),
                view_open: view_open.clone(),
            }));
            let gate = Arc::new(AtomicBool::new(false));
            let healthy = Arc::new(AtomicBool::new(true));
            let relay = tokio::spawn(relay(
                upstream,
                downstream,
                gate.clone(),
                ingress.clone(),
                generation,
            ));
            let heartbeat_peer = peer.clone();
            let heartbeat_gate = gate.clone();
            let heartbeat_healthy = healthy.clone();
            let heartbeat = tokio::spawn(async move {
                loop {
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    let mut peer = heartbeat_peer.lock().await;
                    if peer.exchange(Action::Status, None).await.is_err() {
                        heartbeat_gate.store(false, Ordering::SeqCst);
                        heartbeat_healthy.store(false, Ordering::SeqCst);
                        let _ = peer.stream.get_mut().shutdown().await;
                        break;
                    }
                }
            });
            Ok(Self {
                child: None,
                input: Some(input),
                peer,
                gate,
                healthy,
                view,
                view_open,
                heartbeat,
                relay,
                ingress,
                generation,
            })
        });
        Ok((child, ready_task))
    }

    async fn spawn(
        spec: &ShardRuntimeSpec,
        gateway_program: &Path,
        core_program: &Path,
        collector: SocketAddr,
        generation: u64,
        ingress: Arc<Ingress>,
    ) -> Result<Self> {
        let (mut child, ready) = Self::launch(
            spec,
            gateway_program,
            core_program,
            collector,
            generation,
            ingress,
        )?;
        match ready.await {
            Ok(Ok(mut gateway)) => {
                gateway.child = Some(child);
                Ok(gateway)
            }
            result => {
                let _ = child.kill().await;
                child.wait().await.map_err(|_| "candidate reap failed")?;
                Err(match result {
                    Ok(Err(error)) => error,
                    _ => "Gateway readiness task failed",
                })
            }
        }
    }

    async fn control(&mut self, action: Action, epoch: OwnershipEpoch) -> Result<()> {
        if matches!(action, Action::Pause) {
            self.ingress.begin_pause(self.generation);
        }
        if !matches!(action, Action::Status | Action::Activate) {
            self.gate.store(false, Ordering::SeqCst);
        }
        if matches!(action, Action::Pause) {
            self.ingress.wait_accepted(self.generation).await?;
        }
        let state = self.peer.lock().await.exchange(action, Some(epoch)).await?;
        let expected = match action {
            Action::Activate => State::Active,
            Action::Pause | Action::Quiesce => State::Paused,
            Action::Drain => State::Drained,
            Action::Status => state,
        };
        if state != expected {
            return Err("Gateway acknowledgement state mismatch");
        }
        if matches!(action, Action::Activate) {
            self.ingress.activate(self.generation);
            self.gate.store(true, Ordering::SeqCst);
            self.ingress.changed.notify_waiters();
        }
        Ok(())
    }

    async fn stop_with(&mut self, child: &mut Child) -> Result<()> {
        self.gate.store(false, Ordering::SeqCst);
        self.heartbeat.abort();
        let result = tokio::time::timeout(Duration::from_secs(15), async {
            let sent = self
                .input
                .as_mut()
                .ok_or("Gateway shutdown pipe unavailable")?
                .write_all(shard_runtime::GATEWAY_SHUTDOWN_FRAME)
                .await
                .map_err(|_| "Gateway shutdown failed");
            let exit = child.wait().await.map_err(|_| "Gateway reap failed")?;
            if !exit.success() {
                return Err("Gateway drain did not exit successfully");
            }
            sent
        })
        .await
        .map_err(|_| "Gateway stop timed out")?;
        // The collector's stopped frame is sent before closing the relay.
        let _ = tokio::time::timeout(IO_TIMEOUT, &mut self.relay).await;
        result
    }
    async fn stop(&mut self) -> Result<()> {
        let mut child = self.child.take().ok_or("Gateway is owned by recovery")?;
        let result = self.stop_with(&mut child).await;
        self.child = Some(child);
        result
    }
}

#[derive(Default)]
struct Shared {
    shards: BTreeMap<ShardKey, Value>,
    transitions: VecDeque<Value>,
    operations: BTreeMap<String, Value>,
    updates: BTreeMap<String, UpdateBatch>,
    recovery_events: VecDeque<RecoveryEvent>,
    recent_recovery_events: VecDeque<RecoveryEvent>,
    recovery_dropped: u64,
}

struct UpdateBatch {
    fingerprint: Vec<u8>,
    command: &'static str,
    bot_id: String,
    targets: Vec<(ShardKey, String, u64)>,
    started: std::time::Instant,
    finished: Option<Value>,
}

#[derive(Clone)]
struct GatewayUpdate {
    current: u64,
    candidate: u64,
    artifact: Artifact,
}

struct Job {
    id: String,
    command: String,
    update: Option<GatewayUpdate>,
}
fn local_listener() -> Result<TcpListener> {
    let listener =
        std::net::TcpListener::bind("127.0.0.1:0").map_err(|_| "cannot bind Gateway listener")?;
    listener
        .set_nonblocking(true)
        .map_err(|_| "cannot configure Gateway listener")?;
    TcpListener::from_std(listener).map_err(|_| "cannot configure Gateway listener")
}

struct RecoveryLaunch<'a> {
    key: ChildKey,
    allowed: bool,
    spec: &'a ShardRuntimeSpec,
    gateway_program: &'a Path,
    core_program: &'a Path,
    collector: SocketAddr,
    budget: &'a Arc<StdMutex<MockIdentifyScheduler>>,
    now: Duration,
    deferral: &'a mut Option<Value>,
    ready: &'a mut Option<(u64, JoinHandle<Result<Gateway>>)>,
    ingress: Arc<Ingress>,
}

impl RecoveryHost for RecoveryLaunch<'_> {
    fn authorized(&mut self, key: ChildKey) -> bool {
        key == self.key && self.allowed
    }
    fn resume_available(&mut self, _: ChildKey) -> bool {
        // No protected Resume session handoff exists in this mock adapter.
        // It must therefore obtain a fresh shared admission on every attempt.
        false
    }
    fn try_acquire_identify(&mut self, key: ChildKey) -> IdentifyAdmission {
        match self
            .budget
            .lock()
            .unwrap()
            .reserve(key.bot_id.0, key.shard_id, self.now)
        {
            Ok(IdentifyDecision::Reserved) => {
                *self.deferral = None;
                IdentifyAdmission::Granted
            }
            Ok(IdentifyDecision::Defer(d)) => {
                *self.deferral = Some(json!({"reason":match d.reason {
                    IdentifyBlock::BudgetExhausted => "budget_exhausted",
                    IdentifyBlock::RateLimited => "rate_limited",
                },"retry_after_ms":d.retry_after.as_millis()}));
                IdentifyAdmission::Pending
            }
            Err(_) => IdentifyAdmission::Unavailable,
        }
    }
    fn cancel_identify(&mut self, _: ChildKey) {}
    fn spawn(&mut self, request: StartRequest) -> std::io::Result<Child> {
        if !self.allowed
            || request.key != self.key
            || request.mode != StartMode::GatewayIdentify
            || self.ready.is_some()
        {
            return Err(std::io::Error::other("Gateway recovery admission denied"));
        }
        let (child, ready) = Gateway::launch(
            self.spec,
            self.gateway_program,
            self.core_program,
            self.collector,
            request.generation,
            self.ingress.clone(),
        )
        .map_err(|_| std::io::Error::other("Gateway launch failed"))?;
        *self.ready = Some((request.generation, ready));
        Ok(child)
    }
}

struct Actor {
    spec: ShardRuntimeSpec,
    key: ShardKey,
    ownership_key: OwnershipKey,
    gateway_program: PathBuf,
    core_program: PathBuf,
    collector: SocketAddr,
    budget: Arc<StdMutex<MockIdentifyScheduler>>,
    coordinator: Arc<StdMutex<StandaloneCoordinator>>,
    started: std::time::Instant,
    shared: Arc<StdMutex<Shared>>,
    machine: Option<GatewaySwitch>,
    gateways: BTreeMap<u64, Gateway>,
    generation: u64,
    candidate: Option<u64>,
    pending: Option<Job>,
    prepared: bool,
    stopped: bool,
    faulted: bool,
    deferral: Option<Value>,
    reservation: PathBuf,
    recovery: ChildRecovery,
    recovery_key: ChildKey,
    ready: Option<(u64, JoinHandle<Result<Gateway>>)>,
    core_event_sequences: StdMutex<HashMap<(u64, usize), u64>>,
    ingress: Arc<Ingress>,
}

impl Drop for Actor {
    fn drop(&mut self) {
        if let Some((_, ready)) = &self.ready {
            ready.abort();
        }
    }
}

impl Actor {
    fn relay_core_events(&self) -> bool {
        let mut observed = CoreObservations::default();
        {
            let mut seen = self.core_event_sequences.lock().unwrap();
            seen.retain(|(generation, _), _| self.gateways.contains_key(generation));
            for (generation, gateway) in &self.gateways {
                collect_core_observations(
                    &mut seen,
                    *generation,
                    &gateway.view.lock().unwrap()["workers"],
                    &mut observed,
                );
            }
        }
        if observed.dropped != 0 {
            let mut shared = self.shared.lock().unwrap();
            shared.recovery_dropped = shared.recovery_dropped.saturating_add(observed.dropped);
        }
        for event in observed.events {
            self.record_recovery(event);
        }
        observed.degraded
    }

    fn remove_gateway(&mut self, generation: u64) {
        if let Some(gateway) = self.gateways.get(&generation) {
            gateway.heartbeat.abort();
            // Serialize closure with the peer's view update. Its last received
            // worker events must reach the sink before this view is discarded;
            // an already running heartbeat cannot overwrite it after capture.
            let _view = gateway.view.lock().unwrap();
            gateway.view_open.store(false, Ordering::SeqCst);
        }
        self.relay_core_events();
        self.gateways.remove(&generation);
    }

    fn publish(&self) {
        let recovery = self
            .recovery
            .snapshot(self.recovery_key)
            .expect("registered Gateway");
        let core_degraded = self.relay_core_events();
        let phase = if core_degraded || matches!(recovery.state, RecoveryState::Degraded(_)) {
            "Degraded".to_owned()
        } else if self.stopped {
            "Stopped".to_owned()
        } else if self.faulted {
            "RecoveryRequired".to_owned()
        } else {
            self.machine
                .as_ref()
                .map_or_else(|| "Starting".to_owned(), |m| format!("{:?}", m.phase()))
        };
        let active = self
            .machine
            .as_ref()
            .and_then(|m| m.active())
            .map(|a| a.generation.0);
        let processes: Vec<_> = self
            .gateways
            .values()
            .map(|g| g.view.lock().unwrap().clone())
            .collect();
        self.shared.lock().unwrap().shards.insert(
            self.key.clone(),
            json!({
                "bot_id":self.key.bot_id,"shard_id":self.key.shard_id,"phase":phase,
                "active_generation":active,"generation_high_water":self.generation,
                "processes":processes,"identify":self.deferral,"core_degraded":core_degraded,
                "recovery": {
                    "state":match recovery.state {
                        RecoveryState::Running => "Running", RecoveryState::Waiting => "Waiting",
                        RecoveryState::Stopping => "Stopping", RecoveryState::Stopped => "Stopped",
                        RecoveryState::Degraded(_) => "Degraded",
                    },
                    "reason":match recovery.state { RecoveryState::Degraded(reason) => Some(format!("{reason:?}")), _ => None },
                    "generation":recovery.generation,"restarts":recovery.restart.restarts,
                    "startup_failures":recovery.startup_failures,"stable":recovery.stable,
                    "retry_at_ms":recovery.restart.retry_at.map(|d| d.as_millis()),
                },
            }),
        );
    }

    fn record(&self, event: &str, generation: u64, epoch: u64) {
        let active_count = self
            .gateways
            .values()
            .filter(|g| g.view.lock().unwrap()["state"] == "active")
            .count();
        let pid = self
            .gateways
            .get(&generation)
            .map(|g| g.view.lock().unwrap()["pid"].clone());
        let mut shared = self.shared.lock().unwrap();
        if shared.transitions.len() == 512 {
            shared.transitions.pop_front();
        }
        shared.transitions.push_back(json!({"bot_id":self.key.bot_id,"shard_id":self.key.shard_id,
            "event":event,"generation":generation,"epoch":epoch,"pid":pid,"active_count":active_count}));
    }

    fn record_recovery(&self, event: RecoveryEvent) {
        let active_count = self
            .gateways
            .values()
            .filter(|g| g.view.lock().unwrap()["state"] == "active")
            .count();
        let epoch = self.machine.as_ref().map_or(0, |m| m.epoch().0);
        let pid = self
            .gateways
            .get(&event.generation)
            .map(|g| g.view.lock().unwrap()["pid"].clone());
        let mut shared = self.shared.lock().unwrap();
        if shared.recovery_events.len() == 512 {
            shared.recovery_events.pop_front();
            shared.recovery_dropped = shared.recovery_dropped.saturating_add(1);
        }
        shared.recovery_events.push_back(event);
        if shared.recent_recovery_events.len() == 128 {
            shared.recent_recovery_events.pop_front();
        }
        shared.recent_recovery_events.push_back(event);
        if shared.transitions.len() == 512 {
            shared.transitions.pop_front();
        }
        shared.transitions.push_back(
            json!({"bot_id":self.key.bot_id,"shard_id":self.key.shard_id,
            "event":"child_recovery","recovery":event,"generation":event.generation,
            "epoch":epoch,"pid":pid,"active_count":active_count}),
        );
    }

    fn finish_job(&mut self, error: Option<&'static str>) {
        if let Some(job) = self.pending.take() {
            let error = if let Some(update) = &job.update {
                if error.is_none()
                    && self
                        .machine
                        .as_ref()
                        .and_then(|m| m.active())
                        .is_some_and(|active| active.generation.0 == update.candidate)
                {
                    self.gateway_program = update.artifact.path.clone();
                    None
                } else {
                    error.or(Some("candidate did not become the final active generation"))
                }
            } else {
                error
            };
            let mut shared = self.shared.lock().unwrap();
            let Some(operation) = shared.operations.get_mut(&job.id) else {
                return;
            };
            operation["state"] = json!(if error.is_some() {
                "failed"
            } else {
                "succeeded"
            });
            operation["error"] = json!(error);
        }
    }

    fn reserve(&mut self) -> Result<IdentifyDecision> {
        let decision = self.budget.lock().unwrap().reserve(
            self.ownership_key.bot_id.0,
            self.key.shard_id,
            self.started.elapsed(),
        )?;
        self.deferral = match decision {
            IdentifyDecision::Reserved => None,
            IdentifyDecision::Defer(d) => Some(json!({"reason": match d.reason {
                IdentifyBlock::BudgetExhausted => "budget_exhausted", IdentifyBlock::RateLimited => "rate_limited",
            }, "retry_after_ms": d.retry_after.as_millis()})),
        };
        Ok(decision)
    }

    fn event(&mut self, event: Event) -> Result<Vec<Effect>> {
        self.machine
            .as_mut()
            .ok_or("Gateway not active")?
            .handle(self.ownership_key, event)
            .map_err(|_| "Gateway switch transition rejected")
    }

    async fn spawn(&mut self, generation: u64) -> Result<()> {
        // Hash away from the actor/runtime worker immediately before launch.
        // A failed or replaced artifact never reaches PauseOld/Activate.
        let program = if let Some(update) = self.pending.as_ref().and_then(|j| j.update.as_ref()) {
            if update.candidate != generation {
                return Err("candidate generation mismatch");
            }
            let artifact = update.artifact.clone();
            checked_artifact(
                tokio::task::spawn_blocking(move || {
                    crate::update_rollback::verify_artifact(&artifact, Path::new("."))
                }),
                IO_TIMEOUT,
            )
            .await?
        } else {
            self.gateway_program.clone()
        };
        let gateway = Gateway::spawn(
            &self.spec,
            &program,
            &self.core_program,
            self.collector,
            generation,
            self.ingress.clone(),
        )
        .await?;
        self.gateways.insert(generation, gateway);
        self.record("spawned", generation, 0);
        self.publish();
        Ok(())
    }

    fn poll_recovery(&mut self, allow_spawn: bool) -> Result<()> {
        let allowed = allow_spawn
            && !self.stopped
            && !self.faulted
            && self.machine.is_none()
            && self.gateways.is_empty()
            && self.pending.is_none();
        let events = self.recovery.poll(&mut RecoveryLaunch {
            key: self.recovery_key,
            allowed,
            spec: &self.spec,
            gateway_program: &self.gateway_program,
            core_program: &self.core_program,
            collector: self.collector,
            budget: &self.budget,
            now: self.started.elapsed(),
            deferral: &mut self.deferral,
            ready: &mut self.ready,
            ingress: self.ingress.clone(),
        });
        let mut failure = None;
        for event in events {
            self.generation = self.generation.max(event.generation);
            if let RecoveryEventKind::Exited { .. } = event.kind {
                if let Some((_, ready)) = self.ready.take() {
                    ready.abort();
                }
                self.record(
                    "exited",
                    event.generation,
                    self.machine.as_ref().map_or(0, |m| m.epoch().0),
                );
                self.remove_gateway(event.generation);
                if !self.stopped && self.machine.is_some() {
                    if self
                        .machine
                        .as_ref()
                        .is_some_and(|m| m.phase() == Phase::Stable)
                        && self.gateways.is_empty()
                        && self.pending.is_none()
                    {
                        self.machine = None;
                        self.candidate = None;
                    } else {
                        failure = Some("Gateway exited during management operation");
                    }
                }
            }
            self.record_recovery(event);
        }
        failure.map_or(Ok(()), Err)
    }

    async fn initial(&mut self) -> Result<()> {
        if self
            .ready
            .as_ref()
            .is_none_or(|(_, ready)| !ready.is_finished())
        {
            self.publish();
            return Ok(());
        }
        let (generation, ready) = self.ready.take().expect("finished Gateway readiness");
        let snapshot = self
            .recovery
            .snapshot(self.recovery_key)
            .map_err(|_| "Gateway recovery unavailable")?;
        let result = ready.await;
        if snapshot.state != RecoveryState::Running
            || snapshot.generation != generation
            || snapshot.pid.is_none()
        {
            // A stale Ready cannot reactivate a generation already reaped/stopped.
            return Ok(());
        }
        let gateway = match result {
            Ok(Ok(gateway)) => gateway,
            _ => {
                if let Some(child) = self
                    .recovery
                    .child_mut(self.recovery_key)
                    .map_err(|_| "Gateway recovery unavailable")?
                {
                    child
                        .start_kill()
                        .map_err(|_| "failed Gateway cleanup unavailable")?;
                }
                self.record("startup_rejected", generation, 0);
                self.publish();
                return Ok(());
            }
        };
        self.recovery
            .confirm_ready(self.recovery_key, generation)
            .map_err(|_| "Gateway Ready rejected by recovery")?;
        self.gateways.insert(generation, gateway);
        self.record("spawned", generation, 0);
        let lease = self
            .coordinator
            .lock()
            .unwrap()
            .acquire(self.ownership_key, LEASE)
            .map_err(|_| "local ownership unavailable")?;
        if self
            .gateways
            .get_mut(&generation)
            .expect("ready Gateway")
            .control(Action::Activate, lease.epoch)
            .await
            .is_err()
        {
            self.remove_gateway(generation);
            if let Some(child) = self
                .recovery
                .child_mut(self.recovery_key)
                .map_err(|_| "Gateway recovery unavailable")?
            {
                child
                    .start_kill()
                    .map_err(|_| "failed Gateway cleanup unavailable")?;
            }
            self.record("startup_rejected", generation, lease.epoch.0);
            self.publish();
            return Ok(());
        }
        self.machine = Some(GatewaySwitch::new(
            self.ownership_key,
            Authority {
                generation: GenerationId(generation),
                epoch: lease.epoch,
            },
        ));
        self.record("active", generation, lease.epoch.0);
        self.publish();
        Ok(())
    }

    fn promote_active(&mut self) -> Result<()> {
        let Some(active) = self.machine.as_ref().and_then(|m| m.active()) else {
            return Ok(());
        };
        let current = self
            .recovery
            .snapshot(self.recovery_key)
            .map_err(|_| "Gateway recovery unavailable")?;
        if active.generation.0 == current.generation {
            return Ok(());
        }
        let gateway = self
            .gateways
            .get_mut(&active.generation.0)
            .ok_or("active Gateway unavailable")?;
        let child = gateway.child.take().ok_or("candidate child unavailable")?;
        match self.recovery.replace_ready(
            self.recovery_key,
            current.generation,
            active.generation.0,
            child,
        ) {
            Ok(old) => {
                if let Some(gateway) = self.gateways.get_mut(&current.generation) {
                    gateway.child = Some(old);
                }
                Ok(())
            }
            Err((_, child)) => {
                self.gateways
                    .get_mut(&active.generation.0)
                    .expect("candidate")
                    .child = Some(child);
                Err("Gateway recovery transfer rejected")
            }
        }
    }

    async fn effects(&mut self, effects: Vec<Effect>) -> Result<()> {
        let mut queue: VecDeque<_> = effects.into();
        while let Some(effect) = queue.pop_front() {
            let event = match effect {
                Effect::SpawnStandby(g) => {
                    self.spawn(g.0).await?;
                    Some(Event::Spawned(g))
                }
                Effect::IdentifyStandby(g) => {
                    // Spawn consumed the reservation before the real process
                    // connected. Its authenticated Core Hello and the actual
                    // mock Ready were both observed; no real Discord READY.
                    self.record("warm_ready", g.0, 0);
                    if self
                        .pending
                        .as_ref()
                        .is_some_and(|j| j.command == "gateway-prepare")
                    {
                        self.prepared = true;
                        self.finish_job(None);
                        None
                    } else {
                        Some(Event::WarmReady(g))
                    }
                }
                Effect::PauseOld(a) => {
                    self.gateways
                        .get_mut(&a.generation.0)
                        .ok_or("old Gateway unavailable")?
                        .control(Action::Pause, a.epoch)
                        .await?;
                    self.record("paused", a.generation.0, a.epoch.0);
                    Some(Event::OldPaused(a))
                }
                Effect::RequestAuthority {
                    generation,
                    after_epoch,
                } => {
                    let lease = self
                        .coordinator
                        .lock()
                        .unwrap()
                        .acquire(self.ownership_key, LEASE)
                        .map_err(|_| "local ownership unavailable")?;
                    if lease.epoch.0 <= after_epoch.0 {
                        return Err("non-increasing ownership epoch");
                    }
                    Some(Event::AuthorityGranted(Authority {
                        generation,
                        epoch: lease.epoch,
                    }))
                }
                Effect::Activate(a) => {
                    self.coordinator
                        .lock()
                        .unwrap()
                        .check(self.ownership_key, a.epoch)
                        .map_err(|_| "local authority expired")?;
                    self.gateways
                        .get_mut(&a.generation.0)
                        .ok_or("candidate Gateway unavailable")?
                        .control(Action::Activate, a.epoch)
                        .await?;
                    self.record("active", a.generation.0, a.epoch.0);
                    Some(Event::ActiveAck(a))
                }
                Effect::Drain(a) => {
                    if let Some(gateway) = self.gateways.get_mut(&a.generation.0) {
                        gateway.control(Action::Drain, a.epoch).await?;
                    }
                    self.record("drained", a.generation.0, a.epoch.0);
                    // The real Manager verifies all Core work/results. This
                    // adapter does not invent synthetic per-request ACKs.
                    Some(Event::DrainAck(a))
                }
                Effect::Stop(g) => {
                    if let Some(gateway) = self.gateways.get_mut(&g.0) {
                        if gateway.child.is_some() {
                            gateway.stop().await?;
                        } else if let Some(child) = self
                            .recovery
                            .child_mut(self.recovery_key)
                            .map_err(|_| "Gateway recovery unavailable")?
                        {
                            gateway.stop_with(child).await?;
                        }
                    }
                    self.record("stopped", g.0, self.machine.as_ref().unwrap().epoch().0);
                    self.remove_gateway(g.0);
                    self.candidate = None;
                    self.prepared = false;
                    None
                }
                Effect::Quiesce(a) => {
                    if let Some(gateway) = self.gateways.get_mut(&a.generation.0) {
                        gateway.control(Action::Quiesce, a.epoch).await?;
                    }
                    Some(Event::CandidateQuiesced(a))
                }
                Effect::Diagnostic(diagnostic) => {
                    self.record(
                        if matches!(
                            diagnostic,
                            crate::gateway_switch::Diagnostic::IdentifyDeferred(_)
                        ) {
                            "identify_deferred"
                        } else {
                            "update_aborted"
                        },
                        self.candidate.unwrap_or(0),
                        self.machine.as_ref().unwrap().epoch().0,
                    );
                    None
                }
                Effect::RecoverShard | Effect::RouteRestResult { .. } => {
                    return Err("Gateway recovery requires operator review");
                }
            };
            if let Some(event) = event {
                queue.extend(self.event(event)?);
            }
            self.publish();
        }
        if self
            .machine
            .as_ref()
            .is_some_and(|m| m.phase() == Phase::Stable)
        {
            self.promote_active()?;
            if let Some(active) = self.machine.as_ref().and_then(|machine| machine.active()) {
                self.ingress.release(active.generation.0);
            }
            self.finish_job(None);
            self.publish();
        }
        Ok(())
    }

    async fn fail_closed(&mut self, error: &'static str) {
        // Failed preparation cannot have opened the new gate. A candidate
        // absent from this map has either never spawned or has been reaped.
        // Feed that barrier through the existing rollback protocol, keeping
        // the healthy old Gateway and its in-flight work intact.
        if let Some(candidate) = self.candidate
            && !self.gateways.contains_key(&candidate)
            && self
                .machine
                .as_ref()
                .is_some_and(|m| matches!(m.phase(), Phase::Spawning | Phase::Warming))
        {
            self.finish_job(Some(error));
            if let Ok(effects) = self.event(Event::Failed {
                generation: GenerationId(candidate),
                reason: Failure::GatewayFailed,
            }) && self.effects(effects).await.is_ok()
            {
                return;
            }
        }
        self.faulted = true;
        self.ingress.close();
        let epoch = self
            .machine
            .as_ref()
            .map_or(OwnershipEpoch(0), |m| m.epoch());
        if let Some(machine) = &mut self.machine {
            let _ = machine.handle(self.ownership_key, Event::AuthorityLost);
        }
        for gateway in self.gateways.values_mut() {
            gateway.gate.store(false, Ordering::SeqCst);
            let _ = gateway.control(Action::Quiesce, epoch).await;
        }
        self.record("recovery_required", self.generation, epoch.0);
        self.finish_job(Some(error));
        self.publish();
    }

    async fn job(&mut self, job: Job) -> Result<()> {
        if self.pending.is_some() && job.command != "stop" {
            let mut shared = self.shared.lock().unwrap();
            let op = shared.operations.get_mut(&job.id).unwrap();
            op["state"] = json!("failed");
            op["error"] = json!("Shard operation already running");
            return Ok(());
        }
        if let Some(update) = &job.update {
            let current = self
                .machine
                .as_ref()
                .and_then(|m| m.active())
                .map(|a| a.generation.0);
            if self.prepared
                || self.stopped
                || self.faulted
                || self
                    .machine
                    .as_ref()
                    .is_none_or(|m| m.phase() != Phase::Stable)
                || current != Some(update.current)
                || self.generation.checked_add(1) != Some(update.candidate)
            {
                let mut shared = self.shared.lock().unwrap();
                let op = shared.operations.get_mut(&job.id).expect("admitted job");
                op["state"] = json!("failed");
                op["error"] = json!("Shard changed after update admission");
                return Ok(());
            }
        }
        if job.command == "stop" {
            self.finish_job(Some("operation cancelled by stop"));
        }
        self.pending = Some(job);
        if self.pending.as_ref().unwrap().command == "stop" {
            self.ingress.close();
            let epoch = self
                .machine
                .as_ref()
                .map_or(OwnershipEpoch(0), |m| m.epoch());
            self.recovery
                .stop(self.recovery_key)
                .map_err(|_| "Gateway recovery stop failed")?;
            if let Some((_, ready)) = self.ready.take() {
                ready.abort();
            }
            let mut error = None;
            let mut reaped = Vec::new();
            for (generation, gateway) in &mut self.gateways {
                gateway.gate.store(false, Ordering::SeqCst);
                let observed = if let Some(child) = gateway.child.as_mut() {
                    child.try_wait()
                } else if let Some(child) = self
                    .recovery
                    .child_mut(self.recovery_key)
                    .map_err(|_| "Gateway recovery unavailable")?
                {
                    child.try_wait()
                } else {
                    Ok(None)
                };
                match observed {
                    Ok(Some(exit)) => {
                        if !exit.success() {
                            error.get_or_insert("Gateway exited abnormally before stop");
                        }
                        reaped.push(*generation);
                        continue;
                    }
                    Err(_) => {
                        error.get_or_insert("Gateway monitor failed during stop");
                    }
                    Ok(None) => {}
                }
                if let Err(failure) = self.ingress.wait_accepted(*generation).await {
                    error.get_or_insert(failure);
                }
                if let Err(failure) = gateway.control(Action::Quiesce, epoch).await {
                    error.get_or_insert(failure);
                } else if let Err(failure) = gateway.control(Action::Drain, epoch).await {
                    error.get_or_insert(failure);
                }
                let stopped = if gateway.child.is_some() {
                    gateway.stop().await
                } else if let Some(child) = self
                    .recovery
                    .child_mut(self.recovery_key)
                    .map_err(|_| "Gateway recovery unavailable")?
                {
                    gateway.stop_with(child).await
                } else {
                    Err("Gateway child unavailable")
                };
                if let Err(failure) = stopped {
                    error.get_or_insert(failure);
                }
                let observed = if let Some(child) = gateway.child.as_mut() {
                    child.try_wait()
                } else if let Some(child) = self
                    .recovery
                    .child_mut(self.recovery_key)
                    .map_err(|_| "Gateway recovery unavailable")?
                {
                    child.try_wait()
                } else {
                    Ok(None)
                };
                if matches!(observed, Ok(Some(_))) {
                    reaped.push(*generation);
                }
            }
            // An unready managed child has no control peer yet. Stop its owned
            // handle and wait for termination before releasing the reservation.
            if self.gateways.is_empty()
                && let Some(child) = self
                    .recovery
                    .child_mut(self.recovery_key)
                    .map_err(|_| "Gateway recovery unavailable")?
            {
                let _ = child.kill().await;
                if child.wait().await.is_err() {
                    error.get_or_insert("Gateway reap failed");
                }
            }
            for generation in reaped {
                self.record("stopped", generation, epoch.0);
                self.remove_gateway(generation);
            }
            // Evaluate after the admission/result barriers, not at the instant
            // stop was requested. An ACK arriving during stop is still valid.
            if self.ingress.has_pending_input() {
                error.get_or_insert("Gateway stopped with pending input admission");
            }
            let _ = self.poll_recovery(false);
            self.machine = None;
            self.stopped = self.gateways.is_empty()
                && self
                    .recovery
                    .child_mut(self.recovery_key)
                    .is_ok_and(|child| child.is_none());
            self.faulted = !self.stopped;
            if self.stopped && std::fs::remove_file(&self.reservation).is_err() {
                error.get_or_insert("cannot release runtime reservation");
            }
            self.finish_job(error);
            self.publish();
            return Ok(());
        }
        if self.stopped || self.faulted || self.machine.is_none() {
            self.finish_job(Some("Shard is not ready"));
            return Ok(());
        }
        let effects = if self.prepared && self.pending.as_ref().unwrap().command == "gateway-update"
        {
            self.prepared = false;
            self.event(Event::WarmReady(GenerationId(
                self.candidate.ok_or("candidate missing")?,
            )))?
        } else {
            if self.machine.as_ref().unwrap().phase() != Phase::Stable {
                self.finish_job(Some("Shard switch already running"));
                return Ok(());
            }
            self.generation = self
                .generation
                .checked_add(1)
                .ok_or("Gateway generation exhausted")?;
            self.recovery
                .advance_generation_floor(self.recovery_key, self.generation)
                .map_err(|_| "Gateway generation floor unavailable")?;
            self.candidate = Some(self.generation);
            let identify = self.reserve()?;
            self.event(Event::Begin {
                candidate: GenerationId(self.generation),
                identify,
            })?
        };
        self.effects(effects).await
    }

    async fn tick(&mut self) -> Result<()> {
        // Even faulted/stopping owners must reap. Only this engine may schedule
        // a replacement for the active child; update candidates stay serialized.
        self.poll_recovery(true)?;
        if self.stopped || self.faulted {
            self.publish();
            return Ok(());
        }
        if matches!(
            self.recovery
                .snapshot(self.recovery_key)
                .map_err(|_| "Gateway recovery unavailable")?
                .state,
            RecoveryState::Degraded(_)
        ) {
            // A monitor/clock failure can retain a live child. Its admission
            // must close even though no replacement is allowed in Degraded.
            if !self.gateways.is_empty() {
                return Err("Gateway recovery degraded");
            }
            self.publish();
            return Ok(());
        }
        if self.machine.is_none() {
            return self.initial().await;
        }
        if self.machine.as_ref().unwrap().phase() == Phase::RecoveryRequired {
            self.publish();
            return Ok(());
        }
        let mut exited = None;
        for (generation, gateway) in &mut self.gateways {
            if let Some(child) = gateway.child.as_mut()
                && child
                    .try_wait()
                    .map_err(|_| "Gateway monitor failed")?
                    .is_some()
            {
                exited = Some(*generation);
                break;
            }
            if !gateway.healthy.load(Ordering::SeqCst) {
                // A dead process may surface as IPC EOF one poll before its OS
                // exit. Close admission, then request termination of our handle;
                // the recovery engine must observe actual exit before restarting.
                gateway.gate.store(false, Ordering::SeqCst);
                if gateway.child.is_none() {
                    if let Some(child) = self
                        .recovery
                        .child_mut(self.recovery_key)
                        .map_err(|_| "Gateway recovery unavailable")?
                    {
                        child.start_kill().map_err(|_| "Gateway cleanup failed")?;
                    }
                    return Ok(());
                }
                if self.candidate == Some(*generation)
                    && matches!(
                        self.machine.as_ref().unwrap().phase(),
                        Phase::Spawning | Phase::Warming
                    )
                {
                    let _ = gateway.stop().await;
                    if gateway
                        .child
                        .as_mut()
                        .is_some_and(|child| matches!(child.try_wait(), Ok(Some(_))))
                    {
                        exited = Some(*generation);
                        break;
                    }
                }
                return Err("Gateway or its control connection exited");
            }
        }
        if let Some(generation) = exited {
            self.record(
                "exited",
                generation,
                self.machine.as_ref().unwrap().epoch().0,
            );
            self.remove_gateway(generation);
            return Err("Gateway process exited");
        }
        if let Some(active) = self.machine.as_ref().unwrap().active() {
            self.coordinator
                .lock()
                .unwrap()
                .renew(self.ownership_key, active.epoch, LEASE)
                .map_err(|_| "local lease expired")?;
        }
        if self.machine.as_ref().unwrap().phase() == Phase::Deferred {
            let decision = self.reserve()?;
            if decision == IdentifyDecision::Reserved {
                let effects = self.event(Event::RetryIdentify(decision))?;
                self.effects(effects).await?;
            }
        }
        self.publish();
        Ok(())
    }

    async fn run(mut self, mut jobs: mpsc::Receiver<Job>) {
        self.publish();
        let mut interval = tokio::time::interval(Duration::from_millis(50));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            let result = tokio::select! {
                job = jobs.recv() => match job { Some(job) => self.job(job).await, None => break },
                _ = interval.tick() => self.tick().await,
            };
            if let Err(error) = result {
                self.fail_closed(error).await;
            }
        }
    }
}

pub struct Runtime {
    shared: Arc<StdMutex<Shared>>,
    actors: BTreeMap<ShardKey, mpsc::Sender<Job>>,
    tasks: Vec<JoinHandle<()>>,
    config: Config,
}

impl Drop for Runtime {
    fn drop(&mut self) {
        for task in &self.tasks {
            task.abort();
        }
    }
}

impl Runtime {
    /// Caller must also require an explicit CLI opt-in. Reject distributed
    /// config here as well: standalone is never a quorum fallback.
    pub fn new(
        config: &Config,
        root: &Path,
        gateway_program: &Path,
        core_program: &Path,
        collector: SocketAddr,
        limits: Vec<MockIdentifyLimit>,
    ) -> Result<Self> {
        Self::new_with_recovery_config(
            config,
            root,
            gateway_program,
            core_program,
            collector,
            limits,
            RecoveryConfig::default(),
        )
    }

    /// Explicit mock harness policy; never selected by an inherited environment.
    pub fn new_with_recovery_config(
        config: &Config,
        root: &Path,
        gateway_program: &Path,
        core_program: &Path,
        collector: SocketAddr,
        limits: Vec<MockIdentifyLimit>,
        recovery_config: RecoveryConfig,
    ) -> Result<Self> {
        let _ = Daemon::from_config(config, &HashMap::new(), root, RestartPolicy::default())
            .map_err(|_| "unsupported rolling configuration")?;
        if !safe_path(gateway_program)
            || !safe_path(core_program)
            || !collector.ip().is_loopback()
            || collector.port() == 0
        {
            return Err("invalid test rolling launch configuration");
        }
        let intents = config
            .local_primary_intents(&HashMap::new())
            .map_err(|_| "invalid local placement")?;
        let specs = shard_runtime::plan(&intents, root).map_err(|_| "invalid runtime plan")?;
        let budget = MockIdentifyScheduler::new(limits)?;
        for spec in &specs {
            let bot: u64 = spec
                .bot_id
                .parse()
                .map_err(|_| "numeric Bot identity required")?;
            if bot.to_string() != spec.bot_id || !budget.contains_bot(bot) {
                return Err("invalid runtime identity or missing Identify budget");
            }
        }
        let shared = Arc::new(StdMutex::new(Shared::default()));
        let budget = Arc::new(StdMutex::new(budget));
        let coordinator = Arc::new(StdMutex::new(StandaloneCoordinator::default()));
        let mut runtime = Self {
            shared: shared.clone(),
            actors: BTreeMap::new(),
            tasks: Vec::new(),
            config: config.clone(),
        };
        let started = std::time::Instant::now();
        for spec in specs {
            let key = ShardKey {
                bot_id: spec.bot_id.clone(),
                shard_id: spec.shard_id,
            };
            let ownership_key = OwnershipKey {
                bot_id: BotId(spec.bot_id.parse().map_err(|_| "invalid Bot identity")?),
                shard_id: spec.shard_id,
            };
            let reservation = spec
                .gateway
                .management_path
                .with_extension("botd-reservation");
            std::fs::create_dir_all(reservation.parent().ok_or("invalid runtime path")?)
                .map_err(|_| "cannot create runtime directory")?;
            let mut file = std::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&reservation)
                .map_err(|_| "runtime is already reserved or unavailable")?;
            file.write_all(b"test-gateway-switch; explicit reconciliation required after crash\n")
                .map_err(|_| "cannot reserve runtime")?;
            file.sync_all()
                .map_err(|_| "cannot persist runtime reservation")?;
            let (sender, receiver) = mpsc::channel(4);
            let recovery_key = ChildKey {
                bot_id: ownership_key.bot_id,
                shard_id: key.shard_id,
                role: ChildRole::Gateway,
            };
            let mut recovery = ChildRecovery::new(MonotonicClock::default(), recovery_config, 1)
                .map_err(|_| "invalid Gateway recovery policy")?;
            recovery
                .register(recovery_key, 0)
                .map_err(|_| "invalid Gateway recovery slot")?;
            let actor = Actor {
                spec,
                key: key.clone(),
                ownership_key,
                gateway_program: gateway_program.to_owned(),
                core_program: core_program.to_owned(),
                collector,
                budget: budget.clone(),
                coordinator: coordinator.clone(),
                started,
                shared: shared.clone(),
                machine: None,
                gateways: BTreeMap::new(),
                generation: 0,
                candidate: None,
                pending: None,
                prepared: false,
                stopped: false,
                faulted: false,
                deferral: None,
                reservation,
                recovery,
                recovery_key,
                ready: None,
                core_event_sequences: StdMutex::new(HashMap::new()),
                ingress: Arc::new(Ingress::default()),
            };
            runtime.actors.insert(key, sender);
            runtime.tasks.push(tokio::spawn(actor.run(receiver)));
        }
        Ok(runtime)
    }

    /// Fixed identifiers/enums only; drain into the daemon audit/metrics sink.
    pub fn take_recovery_events(&self) -> Vec<RecoveryEvent> {
        self.shared
            .lock()
            .unwrap()
            .recovery_events
            .drain(..)
            .collect()
    }

    /// Cumulative source queue losses, independent of the downstream audit sink.
    pub fn recovery_events_dropped(&self) -> u64 {
        self.shared.lock().unwrap().recovery_dropped
    }

    /// Resolve against trusted daemon configuration for botctl's preflight.
    pub fn inspect_bot(&self, selector: &str) -> Value {
        let Ok(bot) = crate::botref::resolve(&self.config.bots, selector) else {
            return json!({"ok":false,"error":"unknown or ambiguous Bot"});
        };
        json!({"ok":true,"offline":false,"test_gateway_switch":true,"production_ready":false,
            "bot":{"bot_id":bot.bot_id,"name":bot.name,"total_shards":bot.total_shards}})
    }

    fn batch_view(shared: &mut Shared, id: &str) -> Value {
        let Some(batch) = shared.updates.get(id) else {
            return json!({"ok":false,"error":"unknown update operation"});
        };
        if let Some(finished) = &batch.finished {
            return finished.clone();
        }
        let mut shards = Vec::new();
        let mut failed = Vec::new();
        let mut generations = Vec::new();
        let mut running = false;
        for (key, child_id, expected) in &batch.targets {
            let result = &shared.operations[child_id];
            let observed = shared.shards.get(key);
            let generation = observed.and_then(|s| s["active_generation"].as_u64());
            let child_running = result["state"] == "running";
            running |= child_running;
            if result["state"] == "failed" || (!child_running && generation != Some(*expected)) {
                failed.push(key.clone());
            }
            shards.push(
                json!({"key":key,"state":result["state"],"error":result["error"],
                "gateway_generation":generation}),
            );
            if let Some(generation) = generation {
                generations.push(json!({"shard_id":key.shard_id,"gateway":generation}));
            }
        }
        let state = if running {
            "running"
        } else if failed.is_empty() {
            "succeeded"
        } else {
            "failed"
        };
        let value = json!({"ok":state != "failed","offline":false,"test_gateway_switch":true,
            "production_ready":false,"operation":{"operation_id":id,"command":batch.command,
            "bot_id":batch.bot_id,"state":state,"elapsed_ms":batch.started.elapsed().as_millis(),
            "shards":shards,"failed_shards":failed,"final_generations":generations}});
        if !running {
            shared.updates.get_mut(id).expect("known batch").finished = Some(value.clone());
        }
        value
    }

    /// Inspect an admitted batch without replaying any mutation. Terminal
    /// observations are frozen, including generations and partial failures.
    pub fn update_status(&self, operation_id: &str) -> Value {
        Self::batch_view(&mut self.shared.lock().unwrap(), operation_id)
    }

    pub fn legacy_operation_exists(&self, operation_id: &str) -> bool {
        self.shared
            .lock()
            .unwrap()
            .operations
            .contains_key(operation_id)
    }

    /// Test-only Gateway execution adapter. Production/Core activation does
    /// not have the required control contract and must never be synthesized
    /// by restarting a Gateway. The caller owns authentication and auditing.
    /// Artifact preflight belongs on the caller's blocking worker; each actor
    /// independently rehashes again immediately before launching a candidate.
    pub fn request_update(&self, request: &UpdateControlRequest) -> Value {
        if let Err(error) = request.validate() {
            return json!({"ok":false,"error":error});
        }
        if request.component != UpdateComponent::Gateway {
            return json!({"ok":false,"error":"Core update execution is not connected"});
        }
        let Some(bot) = self.config.bots.iter().find(|b| b.bot_id == request.bot) else {
            return json!({"ok":false,"error":"unknown canonical Bot"});
        };
        if bot.name != request.manifest.bot_name
            || bot.total_shards
                != crate::placement::ShardCount::Fixed(request.manifest.total_shards)
        {
            return json!({"ok":false,"error":"manifest does not match daemon Bot configuration"});
        }
        let artifact = request
            .manifest
            .artifacts
            .gateway
            .as_ref()
            .expect("validated artifact");
        if !artifact.path.is_absolute() || !safe_path(&artifact.path) {
            return json!({"ok":false,"error":"candidate requires an absolute executable path"});
        }
        let fingerprint = match serde_json::to_vec(request) {
            Ok(value) => value,
            Err(_) => return json!({"ok":false,"error":"invalid update payload"}),
        };
        let mut shared = self.shared.lock().unwrap();
        if let Some(old) = shared.updates.get(&request.operation_id) {
            if old.fingerprint != fingerprint {
                return json!({"ok":false,"error":"operation ID conflicts with an earlier request"});
            }
            return Self::batch_view(&mut shared, &request.operation_id);
        }
        if shared.operations.contains_key(&request.operation_id) {
            return json!({"ok":false,"error":"operation ID conflicts with an earlier request"});
        }
        if shared.operations.len() + request.manifest.shards.len() > 256
            || shared.updates.len() >= 256
        {
            return json!({"ok":false,"error":"operation history full"});
        }
        // Acquire every channel permit before inserting a job or changing a
        // generation. A full/disconnected actor rejects the entire batch.
        let mut permits = Vec::new();
        for shard in &request.manifest.shards {
            let key = ShardKey {
                bot_id: request.bot.clone(),
                shard_id: shard.shard_id,
            };
            let Some(sender) = self.actors.get(&key) else {
                return json!({"ok":false,"error":"requested Shard is not local"});
            };
            let Some(view) = shared.shards.get(&key) else {
                return json!({"ok":false,"error":"requested Shard has no verified runtime"});
            };
            let generations = shard.gateway.expect("validated Gateway generation");
            let selector = format!("{}/{}", key.bot_id, key.shard_id);
            if view["phase"] != "Stable"
                || view["active_generation"].as_u64() != Some(generations.current)
                || view["generation_high_water"]
                    .as_u64()
                    .and_then(|g| g.checked_add(1))
                    != Some(generations.candidate)
                || shared
                    .operations
                    .values()
                    .any(|op| op["state"] == "running" && op["bot"] == selector)
                || shared.updates.values().any(|batch| {
                    batch.finished.is_none()
                        && batch.targets.iter().any(|(target, _, _)| target == &key)
                })
            {
                return json!({"ok":false,"error":"Shard is busy or current/candidate generation is stale"});
            }
            let Ok(permit) = sender.try_reserve() else {
                return json!({"ok":false,"error":"Shard control busy"});
            };
            permits.push((key, generations, permit));
        }
        let mut targets = Vec::new();
        for (key, generations, permit) in permits {
            let id = format!("{}-shard-{}", request.operation_id, key.shard_id);
            shared.operations.insert(
                id.clone(),
                json!({"operation_id":id,"command":"gateway-update",
                "bot":format!("{}/{}",key.bot_id,key.shard_id),"state":"running","error":null}),
            );
            targets.push((key, id.clone(), generations.candidate));
            permit.send(Job {
                id,
                command: "gateway-update".into(),
                update: Some(GatewayUpdate {
                    current: generations.current,
                    candidate: generations.candidate,
                    artifact: artifact.clone(),
                }),
            });
        }
        shared.updates.insert(
            request.operation_id.clone(),
            UpdateBatch {
                fingerprint,
                command: request.command_name(),
                bot_id: request.bot.clone(),
                targets,
                started: std::time::Instant::now(),
                finished: None,
            },
        );
        Self::batch_view(&mut shared, &request.operation_id)
    }

    pub fn request(&self, request: &CommandRequest) -> Value {
        let mut shared = self.shared.lock().unwrap();
        if matches!(
            request.command.as_str(),
            "ps" | "status" | "inspect" | "shards"
        ) {
            return json!({"ok":true,"test_gateway_switch":true,"production_ready":false,
                "shards":shared.shards.values().collect::<Vec<_>>(),"transitions":shared.transitions,
                "recovery_events":shared.recent_recovery_events,"recovery_events_dropped":shared.recovery_dropped,
                "note":"Single-daemon mock Discord process wiring only; no distributed or production guarantee."});
        }
        if request.command == "operation" {
            return match request
                .operation_id
                .as_ref()
                .and_then(|id| shared.operations.get(id))
            {
                Some(operation) => json!({"ok":true,"operation":operation}),
                None => json!({"ok":false,"error":"unknown operation"}),
            };
        }
        if !matches!(
            request.command.as_str(),
            "gateway-prepare" | "gateway-update" | "stop"
        ) || request.force
            || request.yes
        {
            return json!({"ok":false,"error":"operation not supported in mock rolling mode"});
        }
        let Some((bot, shard)) = request.bot.as_deref().and_then(|v| v.split_once('/')) else {
            return json!({"ok":false,"error":"test rolling selector must be bot_id/shard_id"});
        };
        let Ok(shard_id) = shard.parse::<u32>() else {
            return json!({"ok":false,"error":"invalid Shard selector"});
        };
        let key = ShardKey {
            bot_id: bot.to_owned(),
            shard_id,
        };
        let Some(sender) = self.actors.get(&key) else {
            return json!({"ok":false,"error":"unknown Shard"});
        };
        let Some(id) = &request.operation_id else {
            return json!({"ok":false,"error":"operation id required"});
        };
        if shared.updates.contains_key(id)
            || shared.updates.values().any(|batch| {
                batch.finished.is_none()
                    && batch.targets.iter().any(|(target, _, _)| target == &key)
            })
        {
            return json!({"ok":false,"error":"Shard update in progress or operation ID conflict"});
        }
        if let Some(operation) = shared.operations.get(id) {
            return if operation["command"] == request.command
                && operation["bot"].as_str() == request.bot.as_deref()
            {
                json!({"ok":true,"operation":operation})
            } else {
                json!({"ok":false,"error":"operation id conflict"})
            };
        }
        if shared.operations.len() >= 256 {
            return json!({"ok":false,"error":"operation history full"});
        }
        let operation = json!({"operation_id":id,"command":request.command,"bot":request.bot,"state":"running","error":null});
        // Insert under the same lock before waking the actor.
        shared.operations.insert(id.clone(), operation.clone());
        if sender
            .try_send(Job {
                id: id.clone(),
                command: request.command.clone(),
                update: None,
            })
            .is_err()
        {
            shared.operations.remove(id);
            return json!({"ok":false,"error":"Shard control busy"});
        }
        json!({"ok":true,"operation":operation})
    }
}

#[cfg(test)]
mod update_tests {
    use super::*;

    fn keyed_input(id: u64) -> Value {
        json!({"type":"prefix","id":id,"event_id":id,"key":id,"content":"!ping","rest_addr":"127.0.0.1:1"})
    }

    #[tokio::test]
    async fn handoff_waits_for_admission_and_deduplicates_fifo_across_repeated_switches() {
        let ingress = Ingress::default();
        ingress.activate(1);
        assert!(matches!(
            ingress.state.lock().unwrap().route(1, keyed_input(1), true),
            IngressRoute::Forward(_)
        ));
        ingress.begin_pause(1);
        assert!(
            tokio::time::timeout(Duration::from_millis(1), ingress.wait_accepted(1))
                .await
                .is_err()
        );
        ingress.accepted(1, 1).unwrap();
        ingress.wait_accepted(1).await.unwrap();
        assert!(ingress.accepted(1, 1).is_err());
        {
            let mut state = ingress.state.lock().unwrap();
            assert!(matches!(
                state.route(2, keyed_input(1), false),
                IngressRoute::Suppressed
            ));
            assert!(matches!(
                state.route(1, keyed_input(2), false),
                IngressRoute::Held
            ));
            assert!(matches!(
                state.route(2, keyed_input(2), false),
                IngressRoute::Suppressed
            ));
            assert!(matches!(
                state.route(2, keyed_input(3), false),
                IngressRoute::Held
            ));
        }
        ingress.activate(2);
        assert!(
            ingress.state.lock().unwrap().next(2).is_none(),
            "old results have not drained"
        );
        ingress.release(2);
        for id in [2, 3] {
            assert_eq!(ingress.state.lock().unwrap().next(2).unwrap()["id"], id);
            assert!(
                ingress.state.lock().unwrap().next(2).is_none(),
                "receive the preceding admission ACK before forwarding another event"
            );
            ingress.accepted(2, id).unwrap();
        }
        ingress.begin_pause(2);
        ingress.wait_accepted(2).await.unwrap();
        assert!(matches!(
            ingress
                .state
                .lock()
                .unwrap()
                .route(3, keyed_input(3), false),
            IngressRoute::Suppressed
        ));
        assert!(matches!(
            ingress
                .state
                .lock()
                .unwrap()
                .route(2, keyed_input(4), false),
            IngressRoute::Held
        ));
        ingress.release(3);
        assert_eq!(ingress.state.lock().unwrap().next(3).unwrap()["id"], 4);
        ingress.accepted(3, 4).unwrap();
        let state = ingress.state.lock().unwrap();
        assert!(state.pending.is_empty());
        assert!(state.unaccepted.is_empty());
        assert_eq!(state.pending_bytes, 0);
    }

    #[test]
    fn handoff_active_inputs_wait_in_fifo_behind_the_unaccepted_frame() {
        let ingress = Ingress::default();
        ingress.activate(1);
        assert!(matches!(
            ingress.state.lock().unwrap().route(1, keyed_input(1), true),
            IngressRoute::Forward(_)
        ));
        for id in [2, 3] {
            assert!(matches!(
                ingress
                    .state
                    .lock()
                    .unwrap()
                    .route(1, keyed_input(id), true),
                IngressRoute::Held
            ));
        }
        assert!(ingress.state.lock().unwrap().next(1).is_none());
        for id in [1, 2] {
            ingress.accepted(1, id).unwrap();
            assert_eq!(ingress.state.lock().unwrap().next(1).unwrap()["id"], id + 1);
            assert!(ingress.state.lock().unwrap().next(1).is_none());
        }
        ingress.accepted(1, 3).unwrap();
        assert!(ingress.state.lock().unwrap().next(1).is_none());
    }

    #[test]
    fn handoff_keeps_the_unclaimed_shadow_prefix_before_new_gap_input() {
        let ingress = Ingress::default();
        ingress.activate(1);
        // IDs are deliberately not ordered numerically: they identify events,
        // while the shadow's observed stream supplies the prefix order.
        assert!(matches!(
            ingress
                .state
                .lock()
                .unwrap()
                .route(2, keyed_input(900), false),
            IngressRoute::Suppressed
        ));
        assert_eq!(ingress.state.lock().unwrap().shadow.len(), 1);
        ingress.begin_pause(1);
        assert!(matches!(
            ingress
                .state
                .lock()
                .unwrap()
                .route(2, keyed_input(3), false),
            IngressRoute::Held
        ));
        // The previous Active now reads its delayed copy of the cached prefix.
        assert!(matches!(
            ingress
                .state
                .lock()
                .unwrap()
                .route(1, keyed_input(900), false),
            IngressRoute::Suppressed
        ));
        ingress.release(2);
        for id in [900, 3] {
            assert_eq!(ingress.state.lock().unwrap().next(2).unwrap()["id"], id);
            ingress.accepted(2, id).unwrap();
        }
        assert!(!ingress.has_pending_input());

        // If Active already claimed the shadow prefix, Pause must not replay it.
        assert!(matches!(
            ingress
                .state
                .lock()
                .unwrap()
                .route(3, keyed_input(4), false),
            IngressRoute::Suppressed
        ));
        assert!(matches!(
            ingress.state.lock().unwrap().route(2, keyed_input(4), true),
            IngressRoute::Forward(_)
        ));
        ingress.begin_pause(2);
        let state = ingress.state.lock().unwrap();
        assert!(state.shadow.is_empty());
        assert!(state.pending.is_empty());
        assert_eq!(state.shadow_bytes, 0);
    }

    #[test]
    fn handoff_stop_rechecks_admission_after_the_last_acknowledgement() {
        let ingress = Ingress::default();
        ingress.activate(1);
        assert!(matches!(
            ingress.state.lock().unwrap().route(1, keyed_input(1), true),
            IngressRoute::Forward(_)
        ));
        ingress.close();
        assert!(ingress.has_pending_input());
        ingress.accepted(1, 1).unwrap();
        assert!(!ingress.has_pending_input());
    }

    #[test]
    fn handoff_bounds_refuse_admission_and_unkeyed_shadow_inputs_stay_suppressed() {
        let mut state = IngressState {
            holding: true,
            ..Default::default()
        };
        assert!(matches!(
            state.route(1, json!({"type":"prefix","id":1}), false),
            IngressRoute::Suppressed
        ));
        for id in 0..HANDOFF_EVENTS as u64 {
            assert!(matches!(
                state.route(1, keyed_input(id), false),
                IngressRoute::Held
            ));
        }
        assert!(matches!(
            state.route(1, keyed_input(HANDOFF_EVENTS as u64), false),
            IngressRoute::Rejected
        ));
        assert!(!state.recent.contains(&(HANDOFF_EVENTS as u64)));
        assert_eq!(state.pending.len(), HANDOFF_EVENTS);
        let mut state = IngressState {
            holding: true,
            ..Default::default()
        };
        let mut value = keyed_input(1);
        value["content"] = json!("x".repeat(HANDOFF_BYTES));
        assert!(matches!(
            state.route(1, value, false),
            IngressRoute::Rejected
        ));
        assert!(state.pending.is_empty());
        assert!(state.recent.is_empty());
        let mut state = IngressState::default();
        for id in 0..RECENT_EVENTS as u64 + 1 {
            state.remember(id);
        }
        assert_eq!(state.recent.len(), RECENT_EVENTS);
        assert!(!state.recent.contains(&0));

        let mut state = IngressState {
            active: Some(1),
            ..Default::default()
        };
        for id in 0..HANDOFF_EVENTS as u64 {
            assert!(matches!(
                state.route(2, keyed_input(id), false),
                IngressRoute::Suppressed
            ));
        }
        assert!(matches!(
            state.route(2, keyed_input(HANDOFF_EVENTS as u64), false),
            IngressRoute::Rejected
        ));
        assert!(matches!(
            state.route(1, keyed_input(0), true),
            IngressRoute::Forward(_)
        ));
        assert_eq!(state.shadow.len(), HANDOFF_EVENTS - 1);
        assert!(matches!(
            state.route(1, keyed_input(HANDOFF_EVENTS as u64), true),
            IngressRoute::Held
        ));
        assert!(matches!(
            state.route(2, keyed_input(HANDOFF_EVENTS as u64 + 1), false),
            IngressRoute::Rejected
        ));
    }

    #[test]
    fn core_event_intake_relays_last_degraded_once_and_reports_sequence_gaps() {
        use crate::child_recovery::DegradedReason;
        use crate::worker::{WorkerIdentity, WorkerRecoveryEvent, WorkerRecoveryStatus};

        let event = |kind| RecoveryEvent {
            key: ChildKey {
                bot_id: BotId(101),
                shard_id: 0,
                role: ChildRole::Core,
            },
            generation: 2,
            at: Duration::from_secs(1),
            kind,
        };
        let started = event(RecoveryEventKind::Started {
            mode: StartMode::Core,
        });
        let degraded = event(RecoveryEventKind::Degraded {
            reason: DegradedReason::RestartBudget,
        });
        let mut worker = WorkerIdentity {
            index: 0,
            pid: 42,
            identity: None,
            credential_free: true,
            recovery: Some(WorkerRecoveryStatus {
                generation: 2,
                state: RecoveryState::Running,
                degraded_reason: None,
                pid: Some(42),
                restarts: 1,
                startup_failures: 0,
                ready: true,
                stable: false,
                recent_events: vec![WorkerRecoveryEvent {
                    sequence: 1,
                    event: started,
                }],
            }),
        };
        let mut seen = HashMap::new();
        let mut initial = CoreObservations::default();
        collect_core_observations(&mut seen, 7, &json!([worker]), &mut initial);
        assert_eq!(initial.events, [started]);
        assert_eq!(initial.dropped, 0);

        // A final heartbeat can arrive after the last published snapshot and
        // just before Gateway retirement. Intake must still relay Degraded.
        let recovery = worker.recovery.as_mut().unwrap();
        recovery.state = RecoveryState::Degraded(DegradedReason::RestartBudget);
        recovery.degraded_reason = Some(DegradedReason::RestartBudget);
        recovery.recent_events = vec![WorkerRecoveryEvent {
            sequence: 3,
            event: degraded,
        }];
        let mut retiring = CoreObservations::default();
        collect_core_observations(&mut seen, 7, &json!([worker]), &mut retiring);
        assert!(retiring.degraded);
        assert_eq!(retiring.events, [degraded]);
        assert_eq!(retiring.dropped, 1);

        let mut repeated = CoreObservations::default();
        collect_core_observations(&mut seen, 7, &json!([worker]), &mut repeated);
        assert!(repeated.degraded);
        assert!(repeated.events.is_empty());
        assert_eq!(repeated.dropped, 0);

        // A new Gateway owns a distinct Manager sequence even for the same
        // worker index. Missing history before the first observation is visible.
        let mut replacement = CoreObservations::default();
        collect_core_observations(&mut seen, 8, &json!([worker]), &mut replacement);
        assert_eq!(replacement.events, [degraded]);
        assert_eq!(replacement.dropped, 2);
    }

    #[test]
    fn readiness_compares_identity_without_freezing_recovery_telemetry() {
        let worker = crate::worker::WorkerIdentity {
            index: 0,
            pid: 42,
            identity: None,
            credential_free: true,
            recovery: None,
        };
        let mut observed = json!([worker]);
        observed[0]["recovery"] = json!({"state":"Running","stable":true});
        assert!(matching_workers(&observed, std::slice::from_ref(&worker)));
        observed[0]["pid"] = json!(43);
        assert!(!matching_workers(&observed, std::slice::from_ref(&worker)));
        observed[0]["pid"] = json!(42);
        observed[0]["credential_free"] = json!(false);
        assert!(!matching_workers(&observed, std::slice::from_ref(&worker)));
        assert!(!matching_workers(&json!([]), &[worker]));
    }

    #[tokio::test]
    async fn artifact_check_deadline_returns_no_launchable_candidate() {
        let check = tokio::spawn(std::future::pending::<Result<PathBuf>>());
        assert_eq!(
            checked_artifact(check, Duration::from_millis(1)).await,
            Err("artifact verification timed out; candidate not launched")
        );
    }

    fn fixture() -> (Runtime, Vec<mpsc::Receiver<Job>>, UpdateControlRequest) {
        let config: Config = toml::from_str("[cluster]\nid='test'\nmode='standalone'\n[node]\nid='local'\n[[bots]]\nname='alpha'\nbot_id='101'\napplication_id='101'\ntoken_env='TOKEN_ALPHA'\ntotal_shards=2\n").unwrap();
        let shared = Arc::new(StdMutex::new(Shared::default()));
        let mut actors = BTreeMap::new();
        let mut receivers = Vec::new();
        for shard_id in 0..2 {
            let key = ShardKey {
                bot_id: "101".into(),
                shard_id,
            };
            shared.lock().unwrap().shards.insert(
                key.clone(),
                json!({"phase":"Stable",
                "active_generation":1,"generation_high_water":1}),
            );
            let (sender, receiver) = mpsc::channel(1);
            actors.insert(key, sender);
            receivers.push(receiver);
        }
        let request = serde_json::from_value(json!({"command":"update","bot":"101",
            "operation_id":"0123456789abcdef0123456789abcdef","yes":true,"component":"gateway",
            "manifest":{"bot_id":"101","bot_name":"alpha","total_shards":2,
                "shards":[{"shard_id":0,"gateway":{"current":1,"candidate":2}},
                          {"shard_id":1,"gateway":{"current":1,"candidate":2}}],
                "artifacts":{"gateway":{"path":std::env::current_exe().unwrap(),"sha256":"a".repeat(64)}},
                "identify":{"limits":{"max_concurrency":2,"remaining":2,"total":10,"reset_after":60000},"observed_age_ms":0}}})).unwrap();
        (
            Runtime {
                shared,
                actors,
                tasks: Vec::new(),
                config,
            },
            receivers,
            request,
        )
    }

    #[test]
    fn batch_admission_is_atomic_and_checks_live_generations() {
        let (runtime, mut receivers, mut request) = fixture();
        request.manifest.shards[1].gateway.as_mut().unwrap().current = 3;
        request.manifest.shards[1]
            .gateway
            .as_mut()
            .unwrap()
            .candidate = 4;
        assert_eq!(runtime.request_update(&request)["ok"], false);
        assert!(
            receivers
                .iter_mut()
                .all(|r| matches!(r.try_recv(), Err(mpsc::error::TryRecvError::Empty)))
        );
        assert!(runtime.shared.lock().unwrap().operations.is_empty());
        request.manifest.shards[1].gateway.as_mut().unwrap().current = 1;
        request.manifest.shards[1]
            .gateway
            .as_mut()
            .unwrap()
            .candidate = 2;
        let key = ShardKey {
            bot_id: "101".into(),
            shard_id: 1,
        };
        runtime.actors[&key]
            .try_send(Job {
                id: "occupied".into(),
                command: "gateway-update".into(),
                update: None,
            })
            .unwrap_or_else(|_| panic!("empty queue"));
        assert_eq!(runtime.request_update(&request)["ok"], false);
        assert!(matches!(
            receivers[0].try_recv(),
            Err(mpsc::error::TryRecvError::Empty)
        ));
        assert!(runtime.shared.lock().unwrap().operations.is_empty());
    }

    #[test]
    fn update_replay_is_idempotent_conflicts_fail_and_terminal_generations_freeze() {
        let (runtime, mut receivers, mut request) = fixture();
        assert_eq!(runtime.inspect_bot("alpha")["bot"]["bot_id"], "101");
        let admitted = runtime.request_update(&request);
        assert_eq!(admitted["operation"]["state"], "running");
        let replay = runtime.request_update(&request);
        assert_eq!(
            replay["operation"]["operation_id"],
            admitted["operation"]["operation_id"]
        );
        assert_eq!(replay["operation"]["state"], "running");
        let jobs: Vec<_> = receivers
            .iter_mut()
            .map(|r| r.try_recv().unwrap())
            .collect();
        assert!(receivers.iter_mut().all(|r| r.try_recv().is_err()));
        request.manifest.artifacts.gateway.as_mut().unwrap().sha256 = "b".repeat(64);
        assert_eq!(runtime.request_update(&request)["ok"], false);
        {
            let mut shared = runtime.shared.lock().unwrap();
            for job in jobs {
                assert_eq!(job.update.as_ref().unwrap().candidate, 2);
                shared.operations.get_mut(&job.id).unwrap()["state"] = json!("succeeded");
            }
            for shard in shared.shards.values_mut() {
                shard["active_generation"] = json!(2);
            }
        }
        let terminal = runtime.update_status(&request.operation_id);
        assert_eq!(terminal["operation"]["state"], "succeeded");
        assert_eq!(
            terminal["operation"]["final_generations"]
                .as_array()
                .unwrap()
                .len(),
            2
        );
        assert_eq!(terminal["operation"]["final_generations"][0]["gateway"], 2);
        for shard in runtime.shared.lock().unwrap().shards.values_mut() {
            shard["active_generation"] = json!(99);
        }
        assert_eq!(runtime.update_status(&request.operation_id), terminal);
    }

    #[test]
    fn rollback_to_old_generation_is_failure_and_core_is_not_emulated_by_gateway_restart() {
        let (runtime, mut receivers, mut request) = fixture();
        request.component = UpdateComponent::Core;
        request.manifest.artifacts.core = request.manifest.artifacts.gateway.clone();
        for shard in &mut request.manifest.shards {
            shard.core = shard.gateway;
        }
        assert_eq!(
            runtime.request_update(&request)["error"],
            "Core update execution is not connected"
        );
        request.component = UpdateComponent::Gateway;
        assert_eq!(runtime.request_update(&request)["ok"], true);
        for receiver in &mut receivers {
            let job = receiver.try_recv().unwrap();
            runtime
                .shared
                .lock()
                .unwrap()
                .operations
                .get_mut(&job.id)
                .unwrap()["state"] = json!("succeeded");
        }
        let failed = runtime.update_status(&request.operation_id);
        assert_eq!(failed["ok"], false);
        assert_eq!(failed["operation"]["state"], "failed");
        assert_eq!(
            failed["operation"]["failed_shards"]
                .as_array()
                .unwrap()
                .len(),
            2
        );
        assert_eq!(failed["operation"]["final_generations"][0]["gateway"], 1);
    }
}
