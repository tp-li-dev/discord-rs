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
        daemon::{Daemon, DaemonError, ShardKey},
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
            atomic::{AtomicUsize, Ordering},
        },
        time::Duration,
    };
    use tokio::{
        net::{TcpListener, TcpStream},
        sync::Mutex,
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
                token_env: "GRANT_FIXTURE_UNUSED".into(),
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
