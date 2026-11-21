//! Runs the real daemon launch/update paths with a fixture Gateway executable
//! driven by the project's pinned Twilight and production grant client.
//! The existing JSON mock collector cannot send Discord Gateway opcodes.

#[cfg(not(target_os = "linux"))]
fn main() {}

#[cfg(target_os = "linux")]
fn main() {
    linux::main();
}

#[cfg(target_os = "linux")]
mod linux {
    use futures_util::{SinkExt, StreamExt};
    use serde_json::{Value, json};
    use src::{
        daemon::{Daemon, DaemonError, LaunchGeneration, ShardKey, UpdateProgress},
        identify_grant::{GATEWAY_FD_FLAG, GrantQueue},
        local_identify::LocalIdentifyPolicy,
        placement::LocalShardIntent,
        shard_runtime,
        supervisor::RestartPolicy,
    };
    use std::{
        ffi::OsString,
        future::Future,
        os::unix::fs::DirBuilderExt,
        path::{Path, PathBuf},
        sync::{
            Arc,
            atomic::{AtomicU64, AtomicUsize, Ordering},
        },
        time::{Duration, SystemTime, UNIX_EPOCH},
    };
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::{TcpListener, TcpStream, UnixStream},
        sync::{Mutex, Notify, oneshot},
        task::JoinHandle,
        time::{interval, sleep, timeout},
    };
    use tokio_websockets::{Message, ServerBuilder, WebSocketStream};
    use twilight_gateway::{ConfigBuilder, Intents, Shard, ShardId};
    use twilight_gateway_queue::InMemoryQueue;

    const DEFER_SAMPLE: Duration = Duration::from_millis(160);
    const CASE_TIMEOUT: Duration = Duration::from_secs(10);

    pub(super) fn main() {
        let args = std::env::args().collect::<Vec<_>>();
        // The custom test harness can be launched by Daemon just like the
        // fixture in tests/daemon.rs, including with extra standby/FD arguments.
        if args.iter().any(|arg| arg == "--core-listen") {
            exit_with_parent();
            std::io::copy(&mut std::io::stdin().lock(), &mut std::io::sink()).unwrap();
            return;
        }
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .unwrap();
        if let Some(index) = args.iter().position(|arg| arg == GATEWAY_FD_FLAG) {
            exit_with_parent();
            runtime.block_on(gateway(args[index + 1].parse().unwrap()));
        } else {
            assert!(
                std::env::var_os("GRANT_FIXTURE_WS").is_none(),
                "missing inherited grant FD"
            );
            runtime.block_on(send_grants_follow_discord_opcodes());
            println!("test send_grants_follow_discord_opcodes ... ok");
            runtime.block_on(promoted_gateway_keeps_the_launch_identity_from_its_journal());
            println!("test promoted_gateway_keeps_the_launch_identity_from_its_journal ... ok");
            let barrier = Arc::new(IdentifyBarrier::default());
            tracing::subscriber::with_default(Arc::clone(&barrier), || {
                runtime.block_on(identify_expiring_after_dequeue_is_held_at_the_sink(barrier));
            });
            println!("test identify_expiring_after_dequeue_is_held_at_the_sink ... ok");
        }
    }

    fn exit_with_parent() {
        // Clean up both fixture children on failures, even after the daemon
        // itself is dropped to exercise the old Gateway's orphaned channel.
        unsafe {
            let parent = libc::getppid();
            assert!(parent > 1);
            assert_eq!(libc::prctl(libc::PR_SET_PDEATHSIG, libc::SIGKILL), 0);
            assert_eq!(libc::getppid(), parent);
        }
    }

    async fn gateway(fd: i32) {
        let _ = rustls::crypto::ring::default_provider().install_default();
        let url = std::env::var("GRANT_FIXTURE_WS").unwrap();
        let socket = unsafe { GrantQueue::inherited_socket(fd) }.unwrap();
        // The mock does not supply Discord REST metadata. Disable its
        // process-local limiter so the assertions isolate the daemon budget.
        let queue = InMemoryQueue::new(0, 0, Duration::from_secs(1), 0);
        let config = ConfigBuilder::new(String::new(), Intents::empty())
            .proxy_url(url)
            .queue(GrantQueue::new(queue, socket, 0))
            .identify_guard(GrantQueue::take_permit)
            .build();
        let mut shard = Shard::with_config(ShardId::ONE, config);
        while shard.next().await.is_some() {}
    }

    struct Root(PathBuf);

    impl Root {
        fn new() -> Self {
            let path = Path::new("/tmp").join(format!("igr-{:016x}", getrandom::u64().unwrap()));
            std::fs::DirBuilder::new()
                .mode(0o700)
                .create(&path)
                .unwrap();
            Self(path)
        }

        fn journal(&self) -> Vec<Value> {
            let raw = std::fs::read_to_string(self.0.join("identify-send-grants.jsonl")).unwrap();
            raw.split_inclusive('\n')
                .filter(|line| line.ends_with('\n'))
                .map(|line| serde_json::from_str(line).unwrap())
                .collect()
        }

        fn records(&self, kind: &str) -> usize {
            self.journal()
                .iter()
                .filter(|record| record["action"]["kind"] == kind)
                .count()
        }
    }

    impl Drop for Root {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    fn key() -> ShardKey {
        ShardKey {
            bot_id: "42".into(),
            shard_id: 0,
        }
    }

    fn unused_reference(_: &str) -> Option<OsString> {
        // A marker for Daemon's required reference lookup. The fixture child
        // never reads it and never authenticates to a real endpoint.
        Some(OsString::from("unused"))
    }

    fn daemon(root: &Root, url: &str) -> Daemon {
        let mut specs = shard_runtime::plan(
            &[LocalShardIntent {
                bot_id: key().bot_id,
                shard_id: 0,
                total_shards: 1,
                token_env: "GRANT_FIXTURE_TOKEN_UNUSED".into(),
            }],
            &root.0,
        )
        .unwrap();
        let spec = &mut specs[0].gateway;
        spec.program = std::env::current_exe().unwrap().into_os_string();
        spec.environment
            .variables
            .insert("GRANT_FIXTURE_WS".into(), url.into());
        let mut daemon = Daemon::new(specs, RestartPolicy::default()).unwrap();
        daemon.set_local_identify_policy(LocalIdentifyPolicy {
            min_interval: Duration::from_millis(250),
            window: Duration::from_secs(120),
            max_per_window: 4,
        });
        daemon.enable_identify_send_grants(&root.0).unwrap();
        daemon.enable_separate_cores().unwrap();
        daemon
    }

    struct Running {
        daemon: Arc<Mutex<Daemon>>,
        tick: JoinHandle<()>,
    }

    impl Running {
        fn start(mut daemon: Daemon) -> Self {
            daemon.start(&key(), &unused_reference).unwrap();
            let daemon = Arc::new(Mutex::new(daemon));
            let owner = Arc::clone(&daemon);
            let tick = tokio::spawn(async move {
                let mut timer = interval(Duration::from_millis(25));
                loop {
                    timer.tick().await;
                    owner.lock().await.poll(&unused_reference).unwrap();
                }
            });
            Self { daemon, tick }
        }

        async fn begin_gateway_update(&self) {
            bounded("start standby", async {
                loop {
                    if self
                        .daemon
                        .lock()
                        .await
                        .begin_gateway_update(&key(), &unused_reference)
                        .unwrap()
                    {
                        return;
                    }
                    sleep(Duration::from_millis(25)).await;
                }
            })
            .await;
        }

        async fn lose_daemon(self) {
            self.tick.abort();
            assert!(self.tick.await.unwrap_err().is_cancelled());
            drop(self.daemon);
        }
    }

    async fn bounded<T>(label: &'static str, future: impl Future<Output = T>) -> T {
        timeout(CASE_TIMEOUT, future)
            .await
            .unwrap_or_else(|_| panic!("timed out: {label}"))
    }

    #[derive(Default)]
    struct Counts {
        identify: AtomicUsize,
        resume: AtomicUsize,
    }

    struct Discord {
        listener: TcpListener,
        counts: Arc<Counts>,
    }

    impl Discord {
        async fn new() -> Self {
            Self {
                listener: TcpListener::bind("127.0.0.1:0").await.unwrap(),
                counts: Arc::new(Counts::default()),
            }
        }

        fn url(&self) -> String {
            format!("ws://{}", self.listener.local_addr().unwrap())
        }

        async fn connect(&self) -> Connection {
            let (stream, _) = bounded("accept Gateway", self.listener.accept())
                .await
                .unwrap();
            let (_, websocket) = bounded("upgrade WebSocket", ServerBuilder::new().accept(stream))
                .await
                .unwrap();
            Connection {
                websocket,
                counts: Arc::clone(&self.counts),
            }
        }

        fn frames(&self) -> (usize, usize) {
            (
                self.counts.identify.load(Ordering::Relaxed),
                self.counts.resume.load(Ordering::Relaxed),
            )
        }
    }

    struct Connection {
        websocket: WebSocketStream<TcpStream>,
        counts: Arc<Counts>,
    }

    impl Connection {
        async fn send(&mut self, data: Value) {
            bounded(
                "send mock Discord frame",
                self.websocket.send(Message::text(data.to_string())),
            )
            .await
            .unwrap();
        }

        async fn hello(&mut self) {
            self.send(json!({"op": 10, "d": {"heartbeat_interval": 60_000}}))
                .await;
        }

        async fn ready(&mut self, discord: &Discord) {
            self.send(json!({
                "op": 0, "t": "READY", "s": 1,
                "d": {"session_id": "fixture", "resume_gateway_url": discord.url()}
            }))
            .await;
        }

        async fn next_text_opcode(&mut self) -> Option<u64> {
            loop {
                let frame = self.websocket.next().await?.expect("read Gateway frame");
                if frame.is_close() {
                    return None;
                }
                let Some(text) = frame.as_text() else {
                    continue;
                };
                // Inspect only the opcode. Never include the frame in an assertion or log.
                let value: Value = serde_json::from_str(text).expect("Gateway JSON");
                let opcode = value["op"].as_u64().expect("Gateway opcode");
                match opcode {
                    2 => {
                        self.counts.identify.fetch_add(1, Ordering::Relaxed);
                    }
                    6 => {
                        self.counts.resume.fetch_add(1, Ordering::Relaxed);
                    }
                    1 => self.send(json!({"op": 11})).await,
                    _ => {}
                }
                return Some(opcode);
            }
        }

        async fn next_auth(&mut self) -> Option<u64> {
            loop {
                match self.next_text_opcode().await {
                    Some(1) => {}
                    other => return other,
                }
            }
        }

        async fn expect_auth(&mut self, opcode: u64) {
            assert_eq!(
                bounded("receive authentication opcode", self.next_auth()).await,
                Some(opcode)
            );
        }

        async fn held(&mut self) {
            assert!(
                timeout(DEFER_SAMPLE, self.next_auth()).await.is_err(),
                "premature authentication opcode or close"
            );
        }

        async fn reconnect(mut self, discord: &Discord, event: Value) -> Self {
            self.send(event).await;
            assert_eq!(
                bounded("old connection sends Close", self.next_auth()).await,
                None
            );
            drop(self);
            let mut next = discord.connect().await;
            next.hello().await;
            next
        }

        async fn heartbeat(&mut self) {
            self.send(json!({"op": 1, "d": null})).await;
            assert_eq!(
                bounded("heartbeat while Identify is held", self.next_text_opcode()).await,
                Some(1)
            );
        }
    }

    fn boot_ns() -> u64 {
        let mut stamp = libc::timespec {
            tv_sec: 0,
            tv_nsec: 0,
        };
        assert_eq!(
            unsafe { libc::clock_gettime(libc::CLOCK_BOOTTIME, &mut stamp) },
            0
        );
        stamp.tv_sec as u64 * 1_000_000_000 + stamp.tv_nsec as u64
    }

    #[derive(Default)]
    struct IdentifyBarrier {
        first_deadline: AtomicU64,
        expired: Notify,
    }

    struct IdentifyEvent(bool);

    impl tracing::field::Visit for IdentifyEvent {
        fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
            if field.name() == "message" && format!("{value:?}") == "sending identify" {
                self.0 = true;
            }
        }
    }

    impl tracing::Subscriber for IdentifyBarrier {
        fn enabled(&self, metadata: &tracing::Metadata<'_>) -> bool {
            metadata.target() == "twilight_gateway::shard" && metadata.is_event()
        }

        fn event(&self, event: &tracing::Event<'_>) {
            if !self.enabled(event.metadata()) {
                return;
            }
            let mut identify = IdentifyEvent(false);
            event.record(&mut identify);
            if !identify.0 {
                return;
            }
            let deadline = self.first_deadline.swap(0, Ordering::AcqRel);
            if deadline != 0 {
                let now = boot_ns();
                assert!(
                    now < deadline,
                    "permit must still be valid after the queue releases it"
                );
                // Twilight has received the oneshot and is constructing the
                // outbound Identify. Do not return to poll_send until it expires.
                while boot_ns() < deadline {
                    std::thread::sleep(Duration::from_nanos(deadline.saturating_sub(boot_ns())));
                }
                self.expired.notify_one();
            }
        }

        fn new_span(&self, _: &tracing::span::Attributes<'_>) -> tracing::span::Id {
            tracing::span::Id::from_u64(1)
        }
        fn record(&self, _: &tracing::span::Id, _: &tracing::span::Record<'_>) {}
        fn record_follows_from(&self, _: &tracing::span::Id, _: &tracing::span::Id) {}
        fn enter(&self, _: &tracing::span::Id) {}
        fn exit(&self, _: &tracing::span::Id) {}
    }

    async fn issue_test_permit(socket: &mut UnixStream, deadline: u64) {
        let mut reply = [0_u8; 10];
        reply[..2].copy_from_slice(&[1, 1]);
        reply[2..].copy_from_slice(&deadline.to_le_bytes());
        socket.write_all(&reply).await.unwrap();
    }

    async fn identify_expiring_after_dequeue_is_held_at_the_sink(barrier: Arc<IdentifyBarrier>) {
        let _ = rustls::crypto::ring::default_provider().install_default();
        let discord = Discord::new().await;
        let (socket, mut broker) = UnixStream::pair().unwrap();
        let (release, released) = oneshot::channel();
        let (requested, replacement_requested) = oneshot::channel();
        let grant_worker = tokio::spawn({
            let barrier = Arc::clone(&barrier);
            async move {
                assert_eq!(broker.read_u8().await.unwrap(), 1);
                let deadline = boot_ns() + 2_000_000_000;
                barrier.first_deadline.store(deadline, Ordering::Release);
                issue_test_permit(&mut broker, deadline).await;
                if broker.read_u8().await.ok() == Some(1) {
                    requested.send(()).unwrap();
                    released.await.unwrap();
                    issue_test_permit(&mut broker, boot_ns() + 2_000_000_000).await;
                }
            }
        });
        let queue = InMemoryQueue::new(0, 0, Duration::from_secs(1), 0);
        let config = ConfigBuilder::new(String::new(), Intents::empty())
            .proxy_url(discord.url())
            .queue(GrantQueue::new(queue, socket, 0))
            .identify_guard(GrantQueue::take_permit)
            .build();
        let mut shard = Shard::with_config(ShardId::ONE, config);
        tokio::select! {
            _ = async { while shard.next().await.is_some() {} } => panic!("fixture shard stopped"),
            () = async {
                let mut connection = discord.connect().await;
                connection.hello().await;
                bounded("Identify left the queue and its permit expired", barrier.expired.notified()).await;
                connection.held().await;
                assert_eq!(discord.frames().0, 0);
                bounded("replacement permit requested", replacement_requested).await.unwrap();
                release.send(()).unwrap();
                connection.expect_auth(2).await;
                connection.held().await;
                assert_eq!(discord.frames().0, 1);
            } => {}
        }
        grant_worker.await.unwrap();
    }

    async fn promoted_gateway_keeps_the_launch_identity_from_its_journal() {
        let root = Root::new();
        let discord = Discord::new().await;
        let running = Running::start(daemon(&root, &discord.url()));
        let mut active = discord.connect().await;
        active.hello().await;
        active.expect_auth(2).await;
        active.ready(&discord).await;
        let old = running.daemon.lock().await.snapshots()[0]
            .identity
            .clone()
            .unwrap();

        let unix_ms = || {
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis()
        };
        let before_spawn = unix_ms();
        running.begin_gateway_update().await;
        let after_spawn = unix_ms();
        let mut standby = discord.connect().await;
        standby.hello().await;
        standby.expect_auth(2).await;
        standby.ready(&discord).await;

        let state = root.0.join("bot-3432/shard-0/gateway-state");
        std::fs::create_dir_all(&state).unwrap();
        // Drive the real daemon transition using the fixture's Core attach
        // acknowledgements. This fixture exercises opcodes, not Core authority.
        {
            let mut daemon = running.daemon.lock().await;
            std::fs::write(state.join("standby-ready"), []).unwrap();
            assert!(matches!(
                daemon.gateway_update_progress(&key()),
                UpdateProgress::Ready
            ));
            daemon.send_gateway_activation(&key()).await.unwrap();
            std::fs::write(state.join("standby-active"), []).unwrap();
            assert!(matches!(
                daemon.gateway_activation_progress(&key()),
                UpdateProgress::Ready
            ));
            daemon.commit_gateway_update(&key()).await.unwrap();
            let snapshot = &daemon.snapshots()[0];
            let active = snapshot.identity.as_ref().unwrap();
            let journal = root.journal();
            let mut launches = journal
                .iter()
                .filter(|record| record["action"]["kind"] == "launch");
            assert!(launches.next().is_some());
            let launch = launches.next().unwrap();
            let expected: LaunchGeneration =
                serde_json::from_value(launch["action"]["gateway"]["generation"].clone()).unwrap();
            let grant = journal.last().unwrap();
            assert_eq!(grant["action"]["kind"], "grant");
            assert_eq!(
                grant["action"]["gateway"]["generation"],
                launch["action"]["gateway"]["generation"]
            );
            assert_ne!(active.pid, old.pid);
            assert_eq!(active.generation, expected);
            assert!(active.launch_observed_unix_ms > old.launch_observed_unix_ms);
            assert!((before_spawn..=after_spawn).contains(&active.launch_observed_unix_ms));
            let grant_status = snapshot.identify_send_grant.as_ref().unwrap();
            assert_eq!(grant_status["active"], "granted");
            assert_eq!(grant_status["standby"], Value::Null);
        }
        running.lose_daemon().await;
    }

    async fn send_grants_follow_discord_opcodes() {
        let root = Root::new();
        let discord = Discord::new().await;
        let invalid = || json!({"op": 9, "d": false});
        let resume = || json!({"op": 7, "d": null});
        let running = Running::start(daemon(&root, &discord.url()));

        let mut active = discord.connect().await;
        active.held().await;
        assert_eq!(root.records("launch"), 1);
        assert_eq!(root.records("grant"), 0, "delayed Hello has no send grant");
        active.hello().await;
        active.expect_auth(2).await;
        active.ready(&discord).await;

        let mut active = active.reconnect(&discord, resume()).await;
        active.expect_auth(6).await;
        assert_eq!(root.records("grant"), 1);
        let mut active = active.reconnect(&discord, invalid()).await;
        active.held().await;
        active.expect_auth(2).await;
        active.ready(&discord).await;

        running.begin_gateway_update().await;
        let mut standby = discord.connect().await;
        // Both live generations now need Identify. Only the standby may use
        // its launch charge while botd protects the fourth slot for fallback.
        let mut active = active.reconnect(&discord, invalid()).await;
        standby.hello().await;
        standby.expect_auth(2).await;
        bounded("active denied while standby is admitted", async {
            loop {
                let status = running.daemon.lock().await.snapshots()[0]
                    .identify_send_grant
                    .clone()
                    .unwrap();
                if status["active"] == "exhausted" && status["standby"] == "granted" {
                    break;
                }
                sleep(Duration::from_millis(25)).await;
            }
        })
        .await;
        active.held().await;
        assert_eq!(discord.frames(), (3, 1));
        assert_eq!(root.records("launch"), 2);
        assert_eq!(root.records("grant"), 3);
        running.daemon.lock().await.cancel_gateway_update(&key());
        drop(standby);
        // Reconnect to abandon the wait response obtained during the update.
        let mut active = active.reconnect(&discord, invalid()).await;
        active.expect_auth(2).await;
        active.ready(&discord).await;
        assert!(matches!(
            running
                .daemon
                .lock()
                .await
                .begin_gateway_update(&key(), &unused_reference),
            Err(DaemonError::IdentifyUnavailable)
        ));
        assert!(
            timeout(DEFER_SAMPLE, discord.listener.accept())
                .await
                .is_err()
        );
        assert_eq!(
            root.records("grant"),
            4,
            "budget exhaustion creates no grant"
        );

        running.lose_daemon().await;
        let mut active = active.reconnect(&discord, resume()).await;
        active.expect_auth(6).await;
        let mut active = active.reconnect(&discord, invalid()).await;
        active.held().await;
        active.heartbeat().await;

        let mut restarted = daemon(&root, &discord.url());
        assert!(matches!(
            restarted.start(&key(), &unused_reference),
            Err(DaemonError::IdentifyUnavailable)
        ));
        assert!(
            timeout(DEFER_SAMPLE, discord.listener.accept())
                .await
                .is_err()
        );
        assert_eq!(discord.frames(), (4, 2));
        assert_eq!(root.records("grant"), 4);
        assert_eq!(root.records("launch"), 2);
        assert_eq!(
            root.journal()
                .iter()
                .filter(|record| record["action"]["converts_launch"] == true)
                .count(),
            2
        );
    }
}
