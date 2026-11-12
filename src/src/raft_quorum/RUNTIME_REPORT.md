# Raft による自動 failover / failback の runtime 接続

2026-11-12。仕様は `docs/architecture.md` §5.3–5.4。担当範囲の実装レポートであり、本番デプロイ手順ではない。`docs/progress.md`、`docs/handoff.md`、`daemon.rs`、`bin/botd.rs`、`tls_transport.rs` は変更していない。

## 実装

- `cluster_runtime::ClusterRuntime::bind_raft` は、既存の配置・mTLS health transport・failover policy・`ClusterAction` に、実 OpenRaft の所有権 CAS と専用 mTLS RPC を接続する。既存 `bind` の試験用 ACK 経路とは明示的に選択を分け、Raft から ACK 経路へフォールバックしない。
- `RaftRuntimeOptions::allow_raft` は既定 `false`。模擬 Discord も必須で、`enable_production = true` は拒否する。これらの guard は証明書読み取り・bind・store 作成より先に実行する。listen / peer は loopback の非ゼロ port に限定し、認証なしの管理 API は追加しない。
- Shard ごとに独立した coordinator/session と最大1件の非同期 Raft 要求を持つ。同期 `tick` と `check_active` は Raft・ネットワーク・ディスクを待たない。通常の admission は epoch とローカル期限を照合する。Core に Bot token を渡す経路は追加していない。
- 所有者変更は expected epoch の CAS 成功応答が必須。ローカル状態機械の read は CAS の候補値にだけ使用する。heartbeat の多数判定、leader hint、TLS 配送 ACK は所有権の証拠にしない。status の `quorum_source` は health による到達可能性の推定であることを示す。
- 保持中予約の crash takeover は、新 epoch の fence commit → 成功応答後から fleet lease ceiling 全期間の隔離 → 新たな quorum commit → lease 付与の順。全参加者の上限は mTLS RPC scope と永続 store identity に固定する。古い scope、異なる上限、異なる配置での RPC / store reopen を拒否する。既存の上限指定なし API は300秒の待機を維持する。稼働中クラスタの上限短縮・設定移行は実装していない。
- runtime の lease は要求開始から `lease_ms - stop_margin_ms`。隔離期間は `lease_ms` で、停止余裕を残す。遅い応答で期限を後ろへ動かさず、旧 lease が失効してから届いた更新 receipt で admission を復活させない。結果不明の取得は grant にせず、別 session の新 CAS と全隔離を必要とする。
- failback は元 primary の連続した heartbeat と安定期間を確認して Stop を発行する。Stop の発行や IPC 書込み成功だけでは Raft 予約を解放せず、同じ epoch の `process_exited_epoch` で delegated process の停止・回収を確認した後に Release を実行する。確認できない crash からの回復は全隔離付き takeover を使う。
- `startup_complete_epoch` / `process_failed_epoch` / `process_exited_epoch` は旧 epoch の通知を無視する。Raft 経路では旧来の epoch なし callback を `EpochRequired` で拒否する。開始時の epoch を Starting / Active / Draining を通じて保持し、古い通知による新世代の解放を防ぐ。
- store の `Create` / `Open` と bootstrap を呼出元が明示する。再起動時の自動再作成・再 bootstrap は行わない。bootstrap は新規 store の最小 node ID のみ許可する。shutdown は delegated process の停止証明ではないため、保持中予約を勝手に解放しない。

## 使い方と未適用の daemon / botd 差分案

`LoadedClusterConfig` の `[test_cluster]` は引き続き共通の control TLS 設定・lease 設定として使用する。Raft 専用入力は別の `RaftRuntimeOptions` で渡す。実行可能な使用例は `tests/fixtures/raft_runtime_support.rs::Cluster::start` / `options`。現状の botd CLI は新 API を呼ばないため、CLI の本番経路は未接続である。

別担当が統合する際は、少なくとも次の変更が必要。以下は差分案であり、対象ファイルへ適用していない。

1. `bin/botd.rs::Options` と parser に、既定 false の `--allow-raft-quorum`、専用 RPC listen / 全 peer の route、node ごとの絶対 store path、明示的な create / bootstrap の入力を追加する。未指定は `StorageMode::Open` / bootstrap false。旧 `--allow-test-quorum`、Gateway switch モードとの同時指定を拒否し、模擬 Discord 必須の guard と匿名 stdin bootstrap 認証を維持する。通常の `cluster.mode = "quorum"` だけで起動してはならない。
2. 明示的に Raft を選択した分岐で、既存 `ClusterRuntime::bind` の代わりに次を呼ぶ。store の消失を見て `Create` へ切り替える処理は入れない。

```rust
let mut runtime = ClusterRuntime::bind_raft(
    &config,
    RaftRuntimeOptions {
        allow_raft: options.allow_raft_quorum,
        enable_production: false,
        storage_path: options.raft_store,
        storage_mode: options.raft_storage_mode,
        bootstrap: options.raft_bootstrap,
        rpc_listen: options.raft_rpc_listen,
        rpc_peers: options.raft_rpc_peers,
    },
    options.mock_discord.is_some(),
).await?;
```

この例の `Options` の追加フィールド・parser は未実装。配置の lease / stop margin は既存 guardian に同じ値を渡す。実 Discord 用 supervisor へ切り替える根拠にはしない。

3. `daemon.rs::test_cluster_processes` は現在、通知を作る箇所で epoch を保持している。`ProcessEvent` にその値を残す。

```diff
 pub enum ProcessEvent {
-    Ready(ShardKey),
-    Exited(ShardKey),
+    Ready(ShardKey, u64),
+    Exited(ShardKey, u64),
 }
-events.push(ProcessEvent::Ready(key.clone()));
+events.push(ProcessEvent::Ready(key.clone(), epoch));
-events.retain(|e| !matches!(e, ProcessEvent::Ready(k) if k == &key));
+events.retain(|e| !matches!(e, ProcessEvent::Ready(k, _) if k == &key));
-events.push(ProcessEvent::Exited(key));
+events.push(ProcessEvent::Exited(key, epoch));
```

4. `bin/botd.rs` の event 消費・失敗 callback も同じ epoch を渡す。epoch を通知受信時の status から再取得してはならない。

```diff
-ProcessEvent::Ready(key) => runtime.startup_complete(&key),
-ProcessEvent::Exited(key) => runtime.process_exited(&key),
+ProcessEvent::Ready(key, epoch) => runtime.startup_complete_epoch(&key, epoch),
+ProcessEvent::Exited(key, epoch) => runtime.process_exited_epoch(&key, epoch),
```

`ClusterAction::Start` / `Renew` の apply 失敗時も action の epoch を保持し、`process_failed_epoch` を使う。Start が失敗して子を一切起動していないことを確認できた場合に限り、同 epoch の `process_exited_epoch` を呼ぶ。Stop 書込み失敗を Exited に変換しない。既存の `ClusterAction::Stop` は runtime 内で既に grant を取り消しているので、停止・回収監視を継続する。

5. 起動通知・status の文言を Raft 用に分岐し、`production_ready = false` を維持する。現行の「majority ACK」という固定表示を Raft に使わず、health と Raft grant、プロセス存在と admission を区別する。変更後は botd → guardian → Gateway → Core の模擬プロセス E2E を別途実行する。

## 検証

環境: Windows、Rust/Cargo 1.90.0。cargo は全て `--offline -p src`、`CARGO_NET_OFFLINE=true`、`CARGO_PROFILE_DEV_DEBUG=0`、`CARGO_INCREMENTAL=0`、`CARGO_BUILD_JOBS=2`。試験は `--test-threads=1` を使用し、workspace 全体試験・依存追加・commit は行わない。

| 対象 / コマンド（`src/` から実行） | 結果 |
|---|---|
| `cargo test --offline -p src --lib cluster_runtime::tests -- --test-threads=1` | 13件成功 |
| `cargo test --offline -p src --test raft_cluster_runtime --test raft_lease_policy -- --test-threads=1 --nocapture` | 新規9件成功（runtime 5件、lease policy 4件） |
| `cargo test --offline -p src --test raft_quorum --test raft_quorum_handoff --test raft_forwarding --test failover --test failover_ownership --test ownership -- --test-threads=1` | 45件成功 |
| `cargo test --offline -p src --test raft_cluster_runtime --test e2e_cluster_processes -- --test-threads=1 --nocapture` | 9件成功（最終差分の runtime 5件、既存 ACK 経路の実プロセス4件） |
| `cargo clippy --offline -p src --lib --test raft_cluster_runtime --test raft_lease_policy --test raft_quorum --test raft_quorum_handoff --test raft_forwarding -- -D warnings` | 成功 |
| 変更した Rust 6ファイルの `rustfmt --check --edition 2024 --config skip_children=true` | 成功 |
| `git diff --check` | 成功 |

重複実行を除く対象は71試験。workspace 全体を試した件数ではない。新規 runtime 試験では全 Start / Renew と独立監視時に模擬 admission の重複を検査し、この条件で二重 Active 違反は0件だった。状態遷移の検知待機と権限取得待機を区別して確認する。

初回 runtime 試験は3件成功・1件失敗。他 Bot が Raft leader 停止後も同じ epoch で連続稼働するという過剰な期待だった。leader 再選出中に安全停止して同じ primary 3 で epoch 1 → 2 に復帰することを後続実行で観測した。試験を、配置の維持・epoch の単調増加・旧 epoch の拒否・単一 Active の検査へ修正した。無停止を実装できたとは扱わない。追加した polling 停止試験は、保留要求の存在と失効後の復活拒否を観測するが、停止時点の特定 receipt が成功 commit だったことまでは観測していない。

clippy 初回は追加コードの `nonminimal_bool` と `clone_on_copy` を検出し、同値の式と Copy 値の利用へ修正後に成功した。環境由来の `could not canonicalize path` と Git の LF/CRLF 通知は残るが、上記コマンドは成功した。

## 未実装・未検証・可用性の制約

- 新規 E2E は1つの Windows プロセス内で3つの runtime / Raft node を起動し、各 node が独立した実 TCP/mTLS listener と永続 store を持つ。通信分断は TLS を終端しない proxy で注入する。node shutdown/reopen、模擬 Gateway の独立した lease gate、模擬 Discord HTTP が検証対象。実 botd / guardian / Gateway / Core の新経路のプロセス停止試験ではない。
- 初期値は3秒検知・30秒安定待機を維持する。新試験は heartbeat 100ms、疑い300ms、障害600ms、安定1200ms、lease ceiling 2000ms、停止余裕500msへ短縮する。3秒/30秒の実時間性能や復旧時間上限は測っていない。
- heartbeat が届いても Raft RPC の過半数が失われれば lease は更新できず安全停止する。primary の health だけは健全な分断では代理の昇格も待機する場合があり、可用性を保証しない。leader 再選出時は別 Shard の lease も失効し得る。同じ epoch の連続稼働や無停止は保証しない。
- 実ホスト間、実ネットワーク・Tailscale、Linux、OS suspend / VM pause、時計速度差、停止不能な guardian、実 Discord、外部副作用の exactly-once は未検証。application polling の停止は OS suspend の再現ではない。
- 本番 CLI / supervisor への配線、分散 Identify/REST 予算、membership 変更、scope / lease 上限の移行、証明書 policy の配布・永続化、journal compaction、電源断の耐久性はこの作業の完成範囲に含まない。

## 変更ファイル

- `src/src/cluster_runtime.rs`
- `src/src/raft_quorum.rs`
- `src/src/raft_quorum/transport.rs`
- `src/tests/raft_cluster_runtime.rs`（新規）
- `src/tests/fixtures/raft_runtime_support.rs`（新規）
- `src/tests/raft_lease_policy.rs`（新規）
- `src/src/raft_quorum/RUNTIME_REPORT.md`（本書）
