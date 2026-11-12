//! Direct real-Gateway control IPC, bypassing the daemon's collector relay.
//! Only loopback mock Discord is used. Inactive collector application input is
//! rejected by the real Manager; the existing mock main then exits with failure.
use serde::{Serialize, de::DeserializeOwned};
use serde_json::{Value, json};
use src::{
    placement::LocalShardIntent,
    shard_runtime::{
        GATEWAY_SHUTDOWN_FRAME, plan,
        test_switch::{
            Action, Bootstrap, Control, CoreLaunch, Hello, LAUNCH_ENV, Launch, Reply, State,
        },
    },
    testkit::{MockDiscord, MockReply},
};
use std::{
    path::PathBuf,
    process::Stdio,
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
};
use tokio::{
    io::{AsyncBufRead, AsyncBufReadExt, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader},
    net::{
        TcpListener,
        tcp::{OwnedReadHalf, OwnedWriteHalf},
    },
    process::{Child, ChildStdin, Command},
    time::timeout,
};

async fn write<T: Serialize, W: AsyncWrite + Unpin>(writer: &mut W, value: &T) {
    let mut bytes = serde_json::to_vec(value).unwrap();
    bytes.push(b'\n');
    timeout(Duration::from_secs(5), writer.write_all(&bytes))
        .await
        .unwrap()
        .unwrap();
}

async fn read<T: DeserializeOwned, R: AsyncBufRead + Unpin>(reader: &mut R) -> T {
    let mut bytes = Vec::new();
    timeout(
        Duration::from_secs(15),
        reader.take(64 * 1024 + 1).read_until(b'\n', &mut bytes),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(bytes.len() <= 64 * 1024 && bytes.last() == Some(&b'\n'));
    serde_json::from_slice(&bytes).unwrap()
}

struct Gateway {
    child: Child,
    input: ChildStdin,
    collector_in: OwnedWriteHalf,
    collector_out: BufReader<OwnedReadHalf>,
    control_in: OwnedWriteHalf,
    control_out: BufReader<OwnedReadHalf>,
    identity: src::protocol::Identity,
    request_id: u64,
}

impl Gateway {
    async fn start() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("target")
            .join("gateway-admission-process")
            .join(format!(
                "{}-{}",
                std::process::id(),
                NEXT.fetch_add(1, Ordering::Relaxed)
            ));
        let mut specs = plan(
            &[LocalShardIntent {
                bot_id: "101".into(),
                shard_id: 1,
                total_shards: 2,
                token_env: "MOCK_UNUSED_TOKEN".into(),
            }],
            &root,
        )
        .unwrap();
        let spec = specs.pop().unwrap();
        let collector = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let controller = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let core =
            CoreLaunch::from_spec(&spec, &PathBuf::from(env!("CARGO_BIN_EXE_botd")), 1).unwrap();
        let identity = core.identity.clone();
        let launch = Launch {
            address: controller.local_addr().unwrap(),
            gateway: identity.clone(),
            core,
        };
        let mut command = Command::new(env!("CARGO_BIN_EXE_src"));
        command
            .args(&spec.gateway.args)
            .arg("--mock-discord")
            .arg(collector.local_addr().unwrap().to_string())
            .env_clear()
            .env("BOT_ID", "101")
            .env("SHARD_ID", "1")
            .env("TOTAL_SHARDS", "2")
            .env("GENERATION_ID", "1")
            .env(LAUNCH_ENV, serde_json::to_string(&launch).unwrap())
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .kill_on_drop(true);
        #[cfg(windows)]
        command.env("SystemRoot", std::env::var_os("SystemRoot").unwrap());
        let mut child = command.spawn().unwrap();
        let mut input = child.stdin.take().unwrap();
        let mut secret = [0u8; 32];
        getrandom::fill(&mut secret).unwrap();
        write(&mut input, &Bootstrap { secret }).await;
        let (collector, _) = collector.accept().await.unwrap();
        let (controller, _) = controller.accept().await.unwrap();
        let (control_out, control_in) = controller.into_split();
        let mut control_out = BufReader::new(control_out);
        let hello: Hello = read(&mut control_out).await;
        assert!(hello.secret == secret);
        assert_eq!(hello.pid, child.id().unwrap());
        assert_eq!(hello.identity, identity);
        assert_eq!(hello.workers.len(), 1);
        assert!(hello.workers[0].credential_free);
        assert_eq!(hello.workers[0].identity.as_ref(), Some(&identity));
        let (collector_out, collector_in) = collector.into_split();
        let mut collector_out = BufReader::new(collector_out);
        let ready: Value = read(&mut collector_out).await;
        assert_eq!(ready["type"], "ready");
        assert_eq!(ready["workers"].as_array().unwrap().len(), 1);
        Self {
            child,
            input,
            collector_in,
            collector_out,
            control_in,
            control_out,
            identity,
            request_id: 0,
        }
    }

    async fn send_control(&mut self, action: Action, epoch: u64) {
        self.request_id += 1;
        let mut identity = self.identity.clone();
        identity.ownership_epoch.0 = epoch;
        write(
            &mut self.control_in,
            &Control {
                request_id: self.request_id,
                identity,
                action,
            },
        )
        .await;
    }

    async fn control(&mut self, action: Action, epoch: u64) -> Reply {
        self.send_control(action, epoch).await;
        let reply: Reply = read(&mut self.control_out).await;
        assert_eq!(reply.request_id, self.request_id);
        assert_eq!(reply.identity.bot_id, self.identity.bot_id);
        assert_eq!(reply.identity.shard, self.identity.shard);
        assert_eq!(reply.identity.generation_id, self.identity.generation_id);
        reply
    }

    async fn rejected_input(&mut self) {
        let rest = MockDiscord::new([MockReply::json(200, b"{}").unwrap()]);
        let server = rest.spawn_tcp().await.unwrap();
        write(
            &mut self.collector_in,
            &json!({"type":"prefix","id":1,"key":8,
            "content":"!ping","rest_addr":server.addr()}),
        )
        .await;
        assert!(
            !timeout(Duration::from_secs(5), self.child.wait())
                .await
                .unwrap()
                .unwrap()
                .success()
        );
        let mut stderr = Vec::new();
        self.child
            .stderr
            .take()
            .unwrap()
            .take(64 * 1024)
            .read_to_end(&mut stderr)
            .await
            .unwrap();
        assert!(String::from_utf8_lossy(&stderr).contains("mock Gateway is not active"));
        assert_eq!(rest.stats().requests, 0);
        server.shutdown().await.unwrap();
    }

    async fn stop(&mut self) {
        self.input.write_all(GATEWAY_SHUTDOWN_FRAME).await.unwrap();
        assert!(
            timeout(Duration::from_secs(5), self.child.wait())
                .await
                .unwrap()
                .unwrap()
                .success()
        );
    }
}

#[tokio::test]
async fn direct_standby_and_paused_gate_reject_before_core_dispatch_or_rest() {
    timeout(Duration::from_secs(25), async {
        for paused in [false, true] {
            let mut gateway = Gateway::start().await;
            assert_eq!(
                gateway.control(Action::Status, 0).await.state,
                State::Standby
            );
            if paused {
                assert_eq!(
                    gateway.control(Action::Activate, 1).await.state,
                    State::Active
                );
                assert_eq!(gateway.control(Action::Pause, 1).await.state, State::Paused);
            }
            gateway.rejected_input().await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn pre_pause_interaction_reservation_finishes_during_core_drain() {
    timeout(Duration::from_secs(25), async {
        let mut gateway = Gateway::start().await;
        gateway.control(Action::Activate, 1).await;
        let rest = MockDiscord::new([
            MockReply::interaction_ack().with_delay(Duration::from_secs(1)),
            MockReply::json(200, b"{}").unwrap(),
        ]);
        let server = rest.spawn_tcp().await.unwrap();
        write(
            &mut gateway.collector_in,
            &json!({"type":"slash","id":9,"key":8,
            "name":"ping","rest_addr":server.addr()}),
        )
        .await;
        timeout(Duration::from_secs(5), async {
            while rest.stats().interaction_requests == 0 {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();
        assert_eq!(gateway.control(Action::Pause, 1).await.state, State::Paused);
        gateway.send_control(Action::Drain, 2).await;
        let result: Value = read(&mut gateway.collector_out).await;
        assert_eq!(result["type"], "result");
        assert_eq!(result["content"], "Pong! 🦀");
        assert_eq!(result["outcome"], "completed");
        assert_eq!(
            result["receipt"]["identity"],
            serde_json::to_value(&gateway.identity).unwrap()
        );
        let drained: Reply = read(&mut gateway.control_out).await;
        assert_eq!(drained.state, State::Drained);
        assert_eq!(drained.identity.ownership_epoch.0, 2);
        assert_eq!(rest.stats().requests, 2);
        gateway.stop().await;
        server.shutdown().await.unwrap();
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn eof_stale_activate_and_idle_watchdog_close_real_gateway_admission() {
    timeout(Duration::from_secs(35), async {
        for fault in ["eof", "stale", "idle"] {
            let mut gateway = Gateway::start().await;
            gateway.control(Action::Activate, 1).await;
            match fault {
                "eof" => gateway.control_in.shutdown().await.unwrap(),
                "stale" => {
                    assert_eq!(
                        gateway.control(Action::Quiesce, 3).await.state,
                        State::Paused
                    );
                    gateway.send_control(Action::Activate, 3).await;
                }
                "idle" => {}
                _ => unreachable!(),
            }
            // Peer EOF is emitted only after the actual Manager admission gate
            // has closed. The idle case exercises the real ten-second timer.
            let mut byte = [0u8; 1];
            assert_eq!(
                timeout(Duration::from_secs(15), gateway.control_out.read(&mut byte))
                    .await
                    .unwrap()
                    .unwrap(),
                0
            );
            gateway.rejected_input().await;
        }
    })
    .await
    .unwrap();
}
