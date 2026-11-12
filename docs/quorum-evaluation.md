# Quorum 実装の評価スパイク

2026-11-12。仕様 [architecture.md](architecture.md) §5.3 / §17-5 に対する独立評価。
**推奨は openraft を合意エンジン候補として条件付きで継続採用。本番の QuorumCoordinator 有効化は保留**する。
3ノードのメモリ内試験で合意・epoch 更新・過半数喪失中に更新成功を返さないことは確認したが、永続化、認証通信、実行権限 lease/watchdog/fencing は未検証である。

## 成果物と責任範囲

[`experiments/quorum-spike`](../experiments/quorum-spike/) は独立 Cargo プロジェクトで、メインの `src/` workspace に組み込まない。
既存の `ownership.rs`、failover、Gateway/Core、Bot token、管理経路を変更しない。
進捗は PM 統合用に本書へ記録し、指示に従って `docs/progress.md` は変更していない。

| ファイル | 実装 |
|---|---|
| `src/lib.rs` | `(bot_id, shard_id)` ごとの所有者・`ownership_epoch`、expected epoch による CAS、型設定 |
| `src/storage.rs` | メモリ内 log/vote/commit 位置、commit 済み log の状態機械適用、membership と snapshot |
| `src/network.rs` | OpenRaft の RPC を別ノードの OpenRaft API へ渡すアダプタ、方向別 RPC 遮断、snapshot 転送件数 |
| `src/cluster.rs` | 固定3投票ノードの起動・初期化、期限付き write、ノード停止、分断・修復、収束待ち |
| `src/main.rs` | 選出→epoch 更新→follower 停止後の更新→過半数喪失時の拒否を実行するサンプル |
| `tests/quorum.rs` | 所有権更新・競合・選出・停止・分断・snapshot 回復の8試験 |
| `tests/storage.rs` | OpenRaft 付属 `testing::Suite` によるストレージ契約試験 |
| `Cargo.toml` / `Cargo.lock` / `.gitignore` | バージョンと依存解決の固定、生成物の除外 |

選挙、投票、term、複製、membership、commit 判定はすべて **openraft** に任せる。アプリ側は決定的な所有権状態機械とテスト用 transport/storage を実装しただけで、独自 Raft は実装していない。

直接依存は準備済みの `openraft`、`tokio`、`serde` の3種類のまま。`openraft = "=0.9.26"` と lockfile を固定し、追加依存は導入していない。
`serde`, `storage-v2`, `generic-snapshot-data` feature を使用する。後者2つは同じ openraft の API を選ぶ設定である。
追加の serializer を導入せず、generic snapshot として所有権 map の複製を直接渡す。snapshot のメタデータには last-applied と membership が付随する。ファイル形式・通信形式は定義していない。

## 確認した挙動

`Transfer { key, expected_epoch, new_owner }` を `Raft::client_write` へ渡す。
OpenRaft が commit した log のみ `RaftStateMachine::apply` で適用し、expected epoch が現在値と一致した場合だけ `checked_add(1)` で進める。初回の現在値は0である。
古い expected epoch と上限到達は、既存の epoch/owner を変更せずアプリ結果として拒否する。`Stale` は Raft commit の失敗とは区別する。

| 要求 / 追加試験 | 試験と観察 |
|---|---|
| (1) 選出と epoch 複製 | 自動選出後、同Botの別Shard・別Botの同Shard番号を更新し、全3状態機械の一致と独立した epoch を確認 |
| (2) 1台停止して過半数残存 | follower の OpenRaft を shutdown し transport から除外しても、残り2台で epoch 更新成功。membership は3票のまま |
| (2) リーダー停止 | 全台への初期適用確認後、リーダーを shutdown。残り2台で自動再選出し、epoch をリセットせず更新成功 |
| (3) 過半数喪失 | さらにもう1台停止。残存1台への2回の write が失敗/期限切れとなり、待機後も状態機械の epoch/owner は不変 |
| (4) 旧リーダーを少数側へ隔離 | 分断直後に旧リーダーへ write。多数側2台は新リーダーを選出し epoch を更新する一方、少数側は成功を返さず状態不変。修復後は全台が多数側の状態に収束し次の更新も成功 |
| 全3台の孤立 | どのノードへの write も成功せず、各状態機械は分断前の状態を維持 |
| CAS 競合 | 同じ expected epoch の2要求を並列送信。Applied 1件・Stale 1件、epoch 増加1回、全台収束 |
| overflow | `u64::MAX` からの更新拒否と owner/epoch 維持 |
| snapshot 追従 | follower への全 inbound RPC を遮断し12更新。snapshot 作成・log purge 後に修復。snapshot RPC 実行件数が正で全台収束、続く13回目の更新も成功 |
| ストレージ契約 | 付属 Suite が log 範囲取得、vote、truncate/purge、membership、applied、snapshot 交換等を検査して成功 |

snapshot 専用試験では、遅延 follower の自動選挙だけを無効化し、遅い CI で選挙が snapshot 試験を妨げないようにした。他の障害・選出試験は自動選挙を有効にしている。
選挙 timeout は160–320 ms、heartbeat は40 ms、write の呼び出し期限は800 ms、選出・収束待ちは8秒。試験用設定であり、仕様の3秒障害検知・実復旧時間を測った値ではない。

### fail closed の意味と境界

このスパイクでは **成功した `Applied` 応答以外から新規権限を付与しない**。候補探索の metrics/current leader と、試験用の状態機械 read は権限の根拠にしない。
本番で整合 read を追加する場合は、OpenRaft の線形化確認と apply 追従を経由する必要がある。

`client_write` を timeout しても、既に投入した要求は取り消されない。`OutcomeUnknown` は結果不明であり、通信回復後に commit する可能性がある。
試験で確認したのは「過半数が失われている観察区間に成功応答も epoch 適用もない」ことで、期限切れが将来の未適用を保証するわけではない。再試行・照合・request ID による冪等化は本番設計が必要である。

commit 済み epoch の存在だけでは、旧 Gateway の Discord 接続や副作用を停止できない。
**OpenRaft 内部の leader lease は Shard 実行権限の lease ではない**。既存の `OwnershipCoordinator` への接続、ローカル期限、更新プロトコル、停止 watchdog、受付ゲート、REST 世代照合は今回実装していない。
二重Active防止全体、外部副作用 exactly-once、無停止は未検証・未保証である。

## 実測・検証結果

環境は Windows NT 10.0.26200.0 / x86_64-pc-windows-msvc、Rust 1.99.0、Cargo 1.99.0、AMD64 Family 25 Model 97 Stepping 2、論理 CPU 12。
CPU 製品名・RAM の CIM 照会は実行環境で拒否されたため記載しない。Linux は実行していない。
依存ソースは取得済みで、Cargo はすべて `--offline` で実行した。ダウンロード時間は含まない。

依存数は direct 3、Windows target の normal 依存（root を除く名前・版の一意数）78、normal + build 依存79。
全 target を含む `cargo metadata` の解決 package 数は root 込み132、すなわち依存131。lockfile の数は Windows で実際にビルドする依存数とは異なる。
Tokio/serde と共有される依存を含む独立プロジェクト全体の値であり、メイン workspace への差分依存数ではない。

初回の空 target からの `cargo test --offline --no-run` は Cargo 報告41.71秒（n=1）。その後テストを追加した再ビルドは別扱いとする。
最終版の `cargo test --offline --locked` はアプリ8件＋上流ストレージ Suite 1件、計9件が成功。アプリ試験実行2.03秒、Suite 8.11秒。これはテスト所要時間であり所有権更新 latency のベンチマークではない。
`cargo fmt --all --check`、`cargo clippy --offline --all-targets -- -D warnings` と実行サンプルも成功。
ルートパスの canonicalize に関するツールチェーン警告は出るが、コマンドは成功している。

| 追加測定 | 母数 / 失敗 | 実測 |
|---|---|---|
| ソース変更なしの `cargo build --offline --locked` | n=5、失敗0 | wall time 0.2943292 / 0.2808296 / 0.2940484 / 0.2955809 / 0.2752816秒、p95=p99=0.2955809秒 |
| 修正後の `cargo test --offline --locked --test quorum --quiet` | 20回×8件=160実行、失敗0 | 1回あたりwall time 最小2.4047164秒、最大3.5335684秒、p95=3.2612279秒、p99=3.5335684秒 |

wall time は PowerShell の `Diagnostics.Stopwatch` で Cargo プロセス起動から終了まで計測した。percentile は nearest-rank。n=5 の p95/p99 はともに最大値で、分布を精密に推定する母数ではない。空 target の初回 build は n=1 のため percentile を出さない。release build、RSS、実ネットワークでの性能は未測定。

初回反復では6回目に snapshot 待機の試験競合で1件失敗した。非同期 metrics の last-applied が12の時点で値を読み、その後作成された13の snapshot に対して12との一致を待っていた。
snapshot trigger 前に状態機械の `applied_state` から確定位置を取得するよう修正し、上記の全9試験と20回の反復を再実行して成功した。合意コードの変更ではなく試験の待機基準の修正である。
途中でツールの書込みが空き領域不足エラーとなり、当スパイクだけの生成物を `cargo clean --offline -p quorum-spike` で除去して継続した。ソースは除去していない。このため初回build値と最終再ビルド値を混同しない。

ビルド時間・反復実行の追加測定は下記コマンドで再現できる。コールド測定は既存 target の削除を行わず、新規 target ディレクトリで行う。

```powershell
cd experiments/quorum-spike
cargo fmt --all --check
cargo clippy --offline --locked --all-targets -- -D warnings
cargo test --offline --locked
cargo run --offline --locked
# 初回測定に用いた空targetからのtest build。以下のtarget名は再利用しないこと。
cargo test --offline --locked --no-run --target-dir target/test-build-fresh
# キャッシュありbuildのwall time（先に通常buildを完了させる）
1..5 | ForEach-Object {
    $timer = [Diagnostics.Stopwatch]::StartNew()
    cargo build --offline --locked
    if ($LASTEXITCODE -ne 0) { throw "build failed" }
    $timer.Stop()
    $timer.Elapsed.TotalSeconds
}
# 通常のdev buildを空targetで追加評価する場合（今回の実測値には含めない）
cargo build --offline --locked --target-dir target/build-measure-20261112
cargo tree --offline --depth 1
cargo tree --offline --target x86_64-pc-windows-msvc --edges normal,build --prefix none
1..20 | ForEach-Object {
    cargo test --offline --locked --test quorum --quiet
    if ($LASTEXITCODE -ne 0) { throw "quorum test failed" }
}
```

Linux では同じ manifest に対して `cargo fmt` / `clippy` / `test` / `run` を実行できる構成だが、通過は未検証。
Cargo の `[workspace]` はこのプロジェクト内で閉じている。

## API 安定性・保守リスク

取得済み0.9.26の `README.md` は、1.0以前の API 非互換変更と、`Change:` / `DataChange:` を含む変更履歴確認の必要性を明記する。
0.9系は bug fix 中心の系列である。今回使った実コードでは `Raft::new` に network factory / log storage / state machine を渡し、各 storage trait、snapshot、RPC error 変換の実装が必要だった。
`storage-v2` は未指定だと sealed trait により直接実装できず、`generic-snapshot-data` の有無で snapshot RPC の契約も変わる。単に型名を差し替えるだけの更新は想定しない。

[公式0.9.26リリース](https://github.com/databendlabs/openraft/releases/tag/v0.9.26) は、0.9.25から native API/ストレージ形式の変更なしと説明している。
一方、[0.9.25リリース](https://github.com/databendlabs/openraft/releases/tag/v0.9.25) は commit 安全性・状態不一致を含む修正を説明しており、成熟ライブラリ採用でも更新追跡と回帰試験が必要と判断する。
最新版全般の安全性を証明したものではなく、今回取得済み0.9.26を固定して評価した。

採用時はこのアダプタ境界を維持し、lockfile・feature・Rust版・永続形式を管理する。
更新ごとに上流差分、storage 契約 Suite、今回の障害試験、永続化の crash/recovery、異版ノード混在・rollback 可否を再確認する。上流の性能値を本プロジェクトの測定値として転記しない。

## 本番用永続ストレージの要件

メモリ内版の `save_vote` と log flush callback は **メモリ反映だけ**を確認する。
これは実プロセス再起動・電源断に耐える storage 契約を満たさず、空ストアで同一ノードとして再参加させる運用を許可しない。
本番 backend は以下を実装・障害試験する必要がある。根拠は評価版の `openraft/src/storage/v2.rs`、`storage/mod.rs` の契約と [上流ストレージ API](https://docs.rs/openraft/0.9.25/openraft/storage/trait.RaftLogStorage.html)。

- vote / term と log の write IO 順序を直列化し、vote は永続化後に返し、log callback は実際の flush 後に成功させる。ディスク障害は成功に変換しない。
- log の連続性を守り、conflict truncate と prefix purge を正しく処理する。snapshot が保護する範囲だけを削除する。
- 業務状態の owner/epoch、last-applied、membership を整合させる。apply 毎の永続 state machine、または永続 snapshot＋保持 log の replay の方式を明示する。
- commit 位置の保存・回復規則を決め、成功応答済み epoch が再起動で巻き戻らないことを検証する。`save_committed` だけで業務状態の永続化を代替できるとはしない。
- snapshot 本体・メタデータを一体として復元し、途中書込み・atomic 入替・チェックサム・schema 版・破損検出・容量上限を定義する。旧 snapshot の削除と log purge の順序を試験する。
- 同一 node ID の二重起動を防ぎ、ストレージの喪失、交換、クラスタ再構築を通常の再起動と区別する。再起動後の epoch 再利用は禁止する。

backend の選定・実装・fsync 耐久性・復旧は **未検証**。追加依存禁止の今回のスパイクでは SQLite 等を導入していない。

## Windows / Linux と残課題

| 項目 | 今回の証拠 / 未検証 |
|---|---|
| Windows | MSVC の実ビルド・Tokio複数thread試験・サンプル成功。Windows全般の本番保証ではない |
| Linux | OS依存 API を直接使わない構成だが、コンパイル・実行・CI・負荷試験は未検証 |
| 永続化 | Windows の共有/rename制約、FlushFileBuffers相当、Linux の fsync/ディレクトリ同期、電源断・破損・ディスク満杯は未検証 |
| 時計・停止 | OS suspend、VM pause、時計差、scheduler停止、lease期限とwatchdog実行遅延、停止しない旧プロセスは未検証 |
| ネットワーク | 同一プロセスの直接RPCのみ。複数ホスト、実socket、片方向packet喪失、遅延、重複、途中切断、帯域制限、mTLS/RBAC/ノードidentity照合は未検証 |
| 運用 | membership追加削除、joint consensus運用、witness設計、全台再起動、異版混在、バックアップ復元は未検証 |
| メモリ・性能 | log/map/snapshotは容量上限なし。production throughput、更新latency p95/p99、RSS、Shard数拡大、負荷時heartbeatは未測定 |
| 統合 | Gateway/Core の権限lease・watchdog・停止・REST fencing、旧Discord接続が生存する分断、複数Bot実配置、T13/T14全体は未検証 |

次の採用判断には、永続 backend の crash/recovery 試験、認証済み transport、lease/watchdog/fencing の統合、Linux/Windows・複数プロセス/ホスト障害試験を条件とする。
それまではメイン QuorumCoordinator の起動拒否を解除する根拠にしない。

## 2026-11-12 追記：複数 botd の試験専用 ACK / lease 接続

`src/src/cluster_runtime.rs` と `cluster::TestOnlyQuorumCoordinator` を、loopback 上の複数 botd / 実 Gateway・Worker プロセス試験用に接続した。これは上記 OpenRaft 評価スパイクとも、既存シミュレーションの `TestQuorumCoordinator` とも別の試験用アダプタである。**OpenRaft、選挙、複製ログ、永続合意、動的 membership は実装していない。本番利用不可**で、既存 `ownership::QuorumCoordinator` の起動拒否は維持する。

`botd --allow-test-quorum --mock-discord <loopback collector>` の両方を明示しない限り、この実行経路は証明書読込・bind・子プロセス起動前に拒否する。通常の `[cluster].mode = "quorum"` だけでは起動しない。listen と全 peer は loopback の非ゼロ port に限定し、通常 Gateway へのフォールバックは行わない。

配置 TOML は既存 `[cluster]` / `[node]` / `[failover]` / `[[bots]]` に次の拡張を追加する。`[cluster].mode = "quorum"`、`[node].id = "node-a"` とし、Bot ID は正規の十進 u64、Shard 数は固定値、全 Shard の明示配置を必要とする。`[node].remote_bind` は許可しない。

```toml
[test_cluster]
node_id = 1
listen = "127.0.0.1:18431"
cert_path = "node-a-cert.der"
key_path = "node-a-key.der"
ca_path = "test-ca.der"
lease_ms = 1500
stop_margin_ms = 500

[[test_cluster.peers]]
id = 2
name = "node-b"
address = "127.0.0.1:18432"

[[test_cluster.peers]]
id = 3
name = "node-c"
address = "127.0.0.1:18433"
```

証明書は `node-<node_id>.cluster` identity を持つ DER leaf、秘密鍵は DER、CA は DER の1 trust anchor。相対パスは配置 TOML の親ディレクトリ基準で解決する。TOML 本体、鍵、証明書、ライブラリの任意エラー本文を診断へコピーしない。E2E は実行ごとに試験用 CA / 鍵を生成し、実運用の認証情報を使わない。秘密鍵のローテーション・失効対応や Windows ACL の包括検証を追加したわけではない。

固定 membership の各投票者は `(bot, shard)` の同時所有者を1組 `(node, incarnation, epoch)` に制限し、受信時から `lease_ms` の間、別所有者への ACK を拒否する。候補は同一要求の過半数 ACK を受け取った場合だけ、**要求開始時刻 + lease_ms − stop_margin_ms** までのローカル lease を得る。遅延 ACK で deadline を後ろへずらさず、重複 ACK を票数に足さず、期限切れ・旧要求・別設定の ACK を採用しない。再起動した投票者はメモリ状態を失うため、最初の `lease_ms` は投票しない。ローカル cancel / process-exit は、他候補を早く許可する根拠にならず、既存の投票保持期限を短縮しない。

メッセージには cluster ID、全 member ID/name、全 Bot/Shard 配置、heartbeat/failover/lease 設定の正規化済み記述を含め、設定の異なる相手の票・heartbeat を採用しない。試験用 TCP proxy の経路と各ノード固有の証明書パスはこの比較から除く。mTLS の TLS ACK は受信キューへの投入確認にすぎず、所有権 ACK と区別する。peer の経過時間を lease deadline に変換せず、各ノードの `Instant` を使う。incarnation が変わった場合は以前の incarnation の heartbeat を再採用せず、過剰な再起動履歴は有限上限で fail closed とする。

`Failover` が元ノードの heartbeat 途絶、代理候補順位、quorum、復帰安定待機を判定する。代理は安定待機が成立した時点で `Draining` に入り、lease の更新をやめて停止する。元ノードの取得は旧投票保持期限と新たな過半数 ACK を待つため、この cold 切替には停止期間があり得る。元ノード側も起動または quorum 回復から `failback_stable_ms` の連続した過半数ヘルスを確認する。**試験モードの初回 primary 起動にも同じ安定待機が適用される**。代理の lease 途絶だけで元ノードが早期に取得することを避けるための保守的な条件である。実 Gateway と Worker の Ready は guardian が観測し、単なる spawn 成功を `TemporaryActive` 完了と扱わない。

実プロセスの期限は botd とは別の guardian が監視する。停止余裕 `stop_margin_ms` は、候補の実行期限と投票者の排他保持期限の間に確保する。**任意の OS suspend、VM pause、時計ジャンプ、scheduler 長期停止、停止不能な Gateway、複数ホストの時計・遅延条件に対する証明ではない**。特に「過半数 ACK だけで本番の分散合意が完成した」「任意障害でも二重副作用がない」「exactly once」と解釈してはいけない。epoch の全台再起動を跨ぐ単調性や耐久性も保証しない。

この追記の runtime 単体検証は Windows / Rust 1.99.0 で `cargo test --offline --lib cluster_runtime::tests` の12件、既存 failover / ownership 接続試験は `cargo test --offline --test failover --test failover_ownership` の19件が成功した。`cargo clippy --offline --lib -- -D warnings` も成功した。3プロセス E2E の最終結果は PM が統合する。Linux、実 Discord、実ホスト間障害、本番コンセンサスの保証は追加しない。

省略時の試験用 lease 設定は `lease_ms = 4000`、`stop_margin_ms = 1000`。既存 `[failover]` の既定 heartbeat 500 ms と組み合わせて安全余裕の設定検査を通る値とし、両方の timing 指定を省略した場合の回帰試験を追加した。上の 1500 / 500 ms は短縮 E2E 用の明示設定例である。
