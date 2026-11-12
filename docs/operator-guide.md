# 運用ガイド（ローカル試作）

`botctl` / `botd` は、認証付きのローカル接続で複数BotのShard別Gateway子プロセスを管理します。既存のDiscord実行系と `manage` は単一Bot・単一Shard用であり、このdaemonとの接続は未実装です。manageの表示にある「コア」はローカル管理プロセスを指し、仕様のShard単位Coreとは異なります。以下はリポジトリルートで `cd src` した後のコマンドです。[制約一覧](known-limitations.md) も確認してください。

## 複数Botのローカル制御：botctl / botd

### 設定と対象範囲

`botd` は `cluster.mode = "standalone"`、`node.remote_bind` 未指定、固定Shard数の設定だけを受け付けます。設定の `node.id` と一致するprimaryのShardを起動・操作します。他ホストに配置したShardの状態は取得せず、配置情報とローカル観測の有無を区別して返します。未解決の `total_shards = "auto"`、quorum構成、遠隔bind指定は起動エラーです。

例えば、次の配置を `local-bots.toml` に保存します。秘密値は書かず、Botごとに異なるtoken環境変数名を指定します。

```toml
[cluster]
id = "local"
mode = "standalone"

[node]
id = "local-node"

[[bots]]
name = "music"
bot_id = "music-001"
application_id = "123456789012345678"
token_env = "MUSIC_BOT_TOKEN"
total_shards = 1

[[bots.shard_placement]]
shard_id = 0
primary = "local-node"

[[bots]]
name = "support"
bot_id = "support-001"
application_id = "234567890123456789"
token_env = "SUPPORT_BOT_TOKEN"
total_shards = 1

[[bots.shard_placement]]
shard_id = 0
primary = "local-node"
```

```sh
cargo build --offline --bin botd --bin botctl
cargo run --offline --bin botctl -- config validate --file local-bots.toml --json
```

`config validate` はオフライン検査で、daemonへ接続しません。tokenの実値、バイナリの動作、Discord接続や権限の確認は行いません。`ps` / `status` / `inspect` / `shards` はdaemonへの接続が必須です。`--file` は `config validate` 専用で、状態表示・操作では拒否します。daemon不在時に設定ファイルを実稼働状態として表示することはありません。

### 認証情報の受渡しとdaemon起動

信頼するランチャーから、次の引数で `botd` を起動します。`ABS_RUNTIME` と `ABS_GATEWAY` は管理者が管理する絶対パスへ置き換えます。

```text
botd --file local-bots.toml --runtime-root ABS_RUNTIME --gateway-program ABS_GATEWAY --bootstrap-stdin
```

ランチャーはdaemon起動ごとに暗号学的乱数の32byte secretを生成し、次の受渡しを行います。**secretを端末へ入力・表示したり、argv・環境変数・一時ファイル・ログへ記録したりしないでください。** 下表はJSONの構造を示しており、secretの固定値を用いた起動例ではありません。

| 経路 | 1行JSONの内容 |
|---|---|
| ランチャー → `botd` の匿名stdinパイプ | `secret`: 32個のbyte値の配列 |
| `botd` → ランチャーのstdout | `port`、`local_only`、`startup_ok`。secretは含まない |
| ランチャー → 各 `botctl --bootstrap-stdin` の匿名stdinパイプ | `port`: 公告されたポート、`secret`: 同じdaemon用の配列 |

各JSONはUTF-8、末尾改行付きです。`botctl` は `127.0.0.1` の公告ポートへ接続し、各要求を認証します。接続情報はそのdaemonの存続期間だけ使用してください。自動検出用endpointファイル、一般用途の常駐ランチャー、OSサービス登録は提供していません。仕様のUnix socket / Named Pipeへの移行とOSユーザー権限の検証、遠隔相互認証・RBACも未実装です。TCPを転送・公開して遠隔管理には使用しないでください。

既定ではdaemon起動時にローカルShardを起動します。`--no-autostart` を追加すると、起動を `botctl start` まで保留できます。`startup_ok` は初回起動要求の結果であり、以後の実状態は `status` で確認します。子の環境は初期化し、そのBot用に指定したtoken参照だけを渡します。Coreを起動したり、Coreへtokenを渡したりはしません。

### 状態表示と操作

以下のコマンドは、上記ランチャーが各プロセスのstdinへ接続情報を渡す前提です。Bot名・完全ID・一意なID接頭辞をdaemon側の `botref` で解決します。名前と別BotのID接頭辞の衝突を含め、複数一致はエラーです。

```text
botctl ps --bootstrap-stdin --json
botctl status music --bootstrap-stdin --json
botctl inspect music-001 --bootstrap-stdin --json
botctl shards support --bootstrap-stdin --json
botctl start music --bootstrap-stdin --timeout 30s --json
botctl stop music --bootstrap-stdin --timeout 30s --json
botctl restart support --bootstrap-stdin --timeout 30s --json
```

| コマンド | 挙動 |
|---|---|
| `ps` / `status` | daemonが管理する全Botのプロセス状態を表示。`status BOT` は対象を絞る |
| `inspect BOT` | 対象Botの詳細とShard別のプロセス観測を表示 |
| `shards BOT` | Shard配置、ローカル観測の有無、該当プロセスの状態を表示 |
| `start BOT` | 対象BotのローカルShardを起動。成功は子プロセスの存在を観測したことを意味する |
| `stop BOT` / `shutdown BOT` | 子へGraceful stopを要求し、実際の正常終了を待つ |
| `restart BOT` / `reboot BOT` | 旧Gateway子の終了確認後に起動。Gateway接続維持更新ではない |
| `stop BOT --force --yes` | 対象の所有済み子ハンドルを強制終了。`--yes` なしは拒否 |
| `operation ID` / `op ID` | 同じdaemonで受け付けた操作の状態と結果を1回再照会 |

通常stopは子の匿名stdinパイプへ `{"command":"shutdown"}` と改行を送り、子が処理を終えて終了コード0で終了するのを確認します。対応するGateway成果物は、新規受付停止・必要なdrain・正常終了を実装し、daemon消失によるstdinのEOFを即時終了命令として扱わない必要があります。既存の `main.rs` / `worker.rs` にはこの起動引数・終了契約を接続していません。子が契約に応答しない場合、停止の成功を返さず、自動でforceへ切り替えません。

既に停止したBotへのstopは成功します。起動中、または自動再試行待ちのBotへの新しいstartはエラーです。restartは対象の旧子がすべて正常終了したことを確認してから新世代を起動します。同じBotの処理中操作を `stop --force --yes` で置き換える場合、元の操作は `OperationSuperseded` として失敗し、force stopを優先します。

`Running` やstart成功はDiscord READY、Heartbeat、Coreのdrain完了、quorum承認、leaseの有効性を証明しません。現在の制御対象は対応するGateway子プロセスです。実Discord・Coreライフサイクル・分散Identify・failoverへの統合は別途必要です。

### 操作ID、timeout、失敗の確認

起動・停止・再起動には、CLIが送信前に生成した操作IDを付けます。`--operation-id ID` で32桁の小文字16進数を指定することもできます。結果にはID、操作対象、状態、所要時間、Shard別の結果・世代、失敗Shardを含みます。`--json` は成功・失敗とも機械可読なJSONをstdoutへ出し、失敗や完了未確認は非ゼロ終了コードで返します。`--json` なしの成功表示は整形JSON、失敗表示はstderrです。

`--timeout` はbootstrap読取・接続・送受信・操作完了待ちを含むCLI全体の待機上限です。既定30秒、0より大きく3600秒以下を指定できます。単位なしと `s` は秒、`ms` はミリ秒で、小数も指定できます。待機timeoutや通信断でCLIが終了しても、受理済みのdaemon操作を取り消しません。新しいIDで同じ操作を送る前に、表示されたIDで結果を確認してください。

```text
botctl operation 0123456789abcdef0123456789abcdef --bootstrap-stdin --json
```

操作状態は `running` / `succeeded` / `failed` です。`running` は完了を意味しません。`operation` の照会時に処理中なら `pending: true`、`ok: false` と非ゼロ終了コードを返します。daemonの操作期限は30秒で、Graceful停止が間に合わなければ失敗を記録し、子を強制終了しません。状態照会を続け、必要なら対象と影響を確認して明示的にforce stopを実行します。一部Shardだけ成功する場合もあり、操作失敗が全Shardの停止や変更の取消を意味するわけではありません。全体の終了コードに加えShard別結果と現在の `status` を確認してください。

操作記録はdaemonのメモリ内に最大1024件保持し、満杯なら新規操作を拒否します。1操作の対象はローカル64Shardまでで、これを超えるBotへの起動・停止・再起動は処理前に拒否します。同じID・同じ操作は記録済み結果を返し、異なる対象・操作へのID再利用は拒否します。記録はdaemon再起動で失われ、永続監査や再起動を跨ぐ重複排除にはなりません。再照会で見つからないことだけを未実行の証拠にしないでください。

daemon終了時に子を無条件でkillしません。子が残る可能性があるため起動予約を保持し、再起動後の重複起動を拒否します。PID・起動世代・OS開始時刻・認証済みIPCによる孤児の照合と再採用は未実装です。予約を削除して新しい子を起動する前に外部で実態の確認が必要です。ランタイムrootの別名・シンボリックリンク・Windows ACL、OSやサービス管理側による子の終了、停電時の永続性は保証していません。

`botctl update` / `rollback` は未実装エラーです。Core/Gateway更新・rollbackを成功したようには表示しません。`create`、`logs`、`nodes`、`doctor` などのdaemon機能も未提供で、既存の `manage` / `reload` / `doctor` は以下の別経路を利用します。

## オフライン配置検査：doctor

```sh
cargo run --offline --bin doctor -- examples/placement.toml --resolved-shards bot-two=1
```

`total_shards = "auto"` のBotごとに `--resolved-shards BOT_ID=COUNT` を指定します。これは手入力でありDiscord推奨値の取得ではありません。未指定・重複・範囲外の解決値などは拒否します。TOMLは最大1 MiB、未知フィールドを拒否し、配置の重複・範囲などを検査します。

doctorは設定の構文・整合性だけを検査します。token環境変数の実値、権限、IPC、Discord、Identify残量、Node接続、状態ストア、quorum、プロセスの稼働状態は診断しません。設定を実Daemonへ反映したりプロセスを起動したりする機能もありません。

## Discordコマンド登録：register

最初に計画を表示します。次のIDは例示で、dry-runにはtoken不要です。

```sh
cargo run --offline --bin register -- 123456789012345678 global ping
cargo run --offline --bin register -- 123456789012345678 234567890123456789 greet
```

形式は `register <application-id> <global|guild-id> <command-name> [--apply]`。IDは非ゼロ整数、コマンドはリンク済み定義から選びます。`greet` では `name`（必須String）と `count`（省略可能Integer）のoption schemaも表示します。

実際の登録は、対象のapplication ID・スコープ・名前を確認し、`DISCORD_TOKEN` を環境変数で渡したうえで末尾に `--apply` を付けます。例えば上のguild指定コマンドに `--apply` を付けると、そのguildの同名1件を作成または上書きします。他コマンドの一括置換・削除はしません。起動時の自動登録や差分同期はありません。

登録HTTPは10秒で打ち切ります。失敗・timeoutはDiscordで適用済みの可能性があり、自動再送しません。Discord側の状態を照合してから再実行を判断してください。`--offline` はCargoの依存取得を禁止する指定で、`--apply` のDiscord通信を禁止するものではありません。実Discordへの登録は本作業では実施していません。

## ローカル起動・状態・停止：manage

秘密をソース・ログ・コマンド履歴に記録しない方法で `DISCORD_TOKEN` を設定します。prefix処理には Message Content Intent が必要です。実行バイナリはGateway情報を取得し、推奨Shard数が1以外の場合は起動を拒否します。取得失敗・timeoutでも起動しません。

内部のCargo呼び出しもofflineにする場合、管理プロセス起動前に環境を設定します。

```powershell
# PowerShell
$env:CARGO_NET_OFFLINE = 'true'
```

```sh
# Linux shell
export CARGO_NET_OFFLINE=true
```

```sh
cargo run --offline --bin manage -- start
cargo run --offline --bin manage -- status
cargo run --offline --bin manage -- logs
cargo run --offline --bin manage -- list-unknown
cargo run --offline --bin manage -- stop
cargo run --offline --bin manage -- start
cargo run --offline --bin manage -- restart
cargo run --offline --bin manage -- shutdown
```

| 操作 | 現在の挙動 |
|---|---|
| `start` | 管理プロセス不在ならreleaseビルド後、管理プロセスとBot親を起動。既存管理プロセスがあればBotの起動を要求 |
| `status` | 管理/Botプロセスの生存、直近エラー、再起動回数、ログパスを表示。Discord接続成功は判定しない |
| `logs` | ログファイル全体を読み取り表示。追尾・ページ化はない |
| `list-unknown` | 認証付きのローカル制御経路で外部応答の結果不明一覧を照会。timestampとvolatile表示付き。解除操作はない |
| `stop` | Bot親を強制終了し、管理プロセスを残す。メモリ状態を失う |
| `restart` | 現在のBot親バイナリを再起動。再ビルドはせず、メモリ状態を失う |
| `shutdown` | Bot親と管理プロセスを終了。メモリ状態を失う |

既定の管理TCPは `127.0.0.1:39483`。`DISCORD_CORE_DIR` で管理ディレクトリ、`DISCORD_CORE_PORT` でポートを変更できます。起動時と操作時で揃えてください。既定ログは `src/core-runtime/core.log`、endpointは `src/core-runtime/core.json`（リポジトリルート基準）です。endpointは管理認証情報を含むため内容を共有しないでください。

管理操作とビルドは直列です。更新ビルド中はstatusやBot親の異常終了監視も待機する場合があります。Bot親が異常終了すると管理プロセスは最大3回再起動を試み、60秒の安定稼働後に回数をリセットします。OSサービス登録やホスト障害からの復旧機能ではありません。

## Worker更新：update / reload

```sh
cargo run --offline --bin manage -- update
# 同じWorker更新を、管理CLIを経由せず依頼する場合
cargo run --offline --bin reload -- general
```

`general` だけを受け付け、実際は全Workerを1個ずつ更新します。Rustコードの動的ロードやGateway更新ではありません。親/Gateway/HTTP部分の変更は親の再起動が必要です。

reloadはreleaseビルド → `src/workers/` への成果物コピー → `src/worker-path.txt` に更新先を書き込み → 完了確認を行います。確認待ちは約20秒で、timeoutや失敗後も更新要求は残り、親が再試行し得ます。直ちに再実行せず、ログと `!workers` / `!version` で実状態を確認してください。ビルドIDはコードの差分を証明しません。

対象Workerの受付を止め、処理と外部応答確認を待ち、snapshotを新Workerへ移して切り替えます。別Workerは処理を継続できます。途中失敗時は切替済みWorkerが新版、未切替Workerが旧版のまま残り得ます。全体を旧版へ戻す原子的rollbackやCLIはありません。結果不明の外部応答があるWorkerは旧版を維持します。`list-unknown` で照会できますが、照合・解除CLIや永続監査は未実装です。再起動で追跡が消えても外部操作の成否が判明したことにはなりません。

引き継ぎ対象は `!count` のカウンターと `!job` の計算状態です。`!job start` はチャンネルごとに1個の計算を開始し、Workerの約1秒周期で進みます。`!job status`、更新、再度statusで明示的checkpointの継続を確認できます。任意のFutureや外部APIの実行途中を移動しません。StateMap/Cogの状態はsnapshot対象外です。

`DISCORD_WORKER_FILE` を使う場合は親とreloadに同じ絶対パスを渡します。更新指定ファイルと `workers/` は任意バイナリ実行につながるため、信頼する管理者だけに書込権限を限定してください。Windowsのendpoint保護は親ディレクトリのACLに依存します。Windows ACL、成果物の署名検証、全ログ経路の秘密マスクは検証・実装が残っています。
