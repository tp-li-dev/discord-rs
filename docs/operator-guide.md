# 運用ガイド（ローカル試作）

> [!NOTE]
> 2026-11-13の決定により、**対応OSはLinux(Ubuntu)のみであり、Windowsはサポート外**（開発機での動作は保証しない。Windows固有コードは残るが保証・CI対象外）です。実Discordでの検証はユーザーが実施する範囲です。

`botctl` / `botd` は、認証付きのローカル接続で複数BotのShard別Gateway子プロセスを管理します。実 `src` Gatewayの起動引数・Workerプール・停止契約へ接続し、Discordに接続しない明示的な模擬モードを追加しました。実Discordでの複数Shard運用や分散制御の検証は完了していません（実Discord検証はユーザーが実施する範囲です）。`manage` は別のローカル単一Bot管理経路で、その表示の「コア」は管理プロセスを指します。以下はリポジトリルートで `cd src` した後のコマンドです。

実装契約と検証範囲は [Shard実行設計](shard-runtime-design.md)、残る制約は [制約一覧](known-limitations.md) を参照してください。

## 複数Botのローカル制御：botctl / botd

### 設定と対象範囲

通常の `botd` は `cluster.mode = "standalone"`、`node.remote_bind` 未指定、固定Shard数の設定を受け付けます。設定の `node.id` と一致するprimaryのShardを起動・操作します。他ホストに配置したShardの状態は取得せず、配置情報とローカル観測の有無を区別して返します。未解決の `total_shards = "auto"`、遠隔bind指定は起動エラーです。quorum構成は明示的な試験オプションと模擬Discordを必要とし、本番分散合意ではありません（[試験経路](shard-runtime-design.md) 参照）。

**実 `src` Gatewayには数値Bot IDが必要です。** 配置・daemonは文字列IDを許容しますが、実Worker IPCの `BotId(u64)` に合わせ、`bot_id = "101"` のようなu64の正規の10進数文字列を指定してください。UUID・任意文字列は配置検査に成功しても実Gatewayの起動で失敗します。`"001"` や `"+1"` も、別の文字列IDを同じIPC identityへ混同しないため拒否します。暗黙のハッシュ変換は行いません。Botの表示名には従来どおり名前を使用できます。

例えば、次の配置を `local-bots.toml` に保存します。秘密値は書かず、Botごとに異なるtoken環境変数名を指定します。

```toml
[cluster]
id = "local"
mode = "standalone"

[node]
id = "local-node"

[[bots]]
name = "music"
bot_id = "101"
application_id = "123456789012345678"
token_env = "MUSIC_BOT_TOKEN"
total_shards = 1

[[bots.shard_placement]]
shard_id = 0
primary = "local-node"

[[bots]]
name = "support"
bot_id = "202"
application_id = "234567890123456789"
token_env = "SUPPORT_BOT_TOKEN"
total_shards = 1

[[bots.shard_placement]]
shard_id = 0
primary = "local-node"
```

```sh
cargo build --offline --bin src --bin botd --bin botctl
cargo run --offline --bin botctl -- config validate --file local-bots.toml --json
```

`config validate` はオフライン検査で、daemonへ接続しません。tokenの実値、バイナリの動作、Discord接続や権限の確認は行いません。`ps` / `status` / `inspect` / `shards` はdaemonへの接続が必須です。`--file` は `config validate` 専用で、状態表示・操作では拒否します。daemon不在時に設定ファイルを実稼働状態として表示することはありません。

### 認証情報の受渡しとdaemon起動

信頼するランチャーから、次の引数で `botd` を起動します。`ABS_RUNTIME` と `ABS_GATEWAY` は管理者が管理する絶対パスへ置き換えます。

```text
botd --file local-bots.toml --runtime-root ABS_RUNTIME --gateway-program ABS_GATEWAY --bootstrap-stdin
```

このリポジトリのGatewayを使う場合、`ABS_GATEWAY` はビルドした `target/debug/src`（Linux）の絶対パスです（Windowsはサポート外であり、開発機での動作は保証しません。Windows固有コードは残りますが保証・CI対象外です）。Gatewayは1プロセスで1Shardを担当し、既定2 WorkerをそのShard内で起動します。`--gateway-program` の任意成果物が同じ契約を実装していることまでdaemonが保証するわけではありません。

ランチャーはdaemon起動ごとに暗号学的乱数の32byte secretを生成し、次の受渡しを行います。**secretを端末へ入力・表示したり、argv・環境変数・一時ファイル・ログへ記録したりしないでください。** 下表はJSONの構造を示しており、secretの固定値を用いた起動例ではありません。

| 経路 | 1行JSONの内容 |
|---|---|
| ランチャー → `botd` の匿名stdinパイプ | `secret`: 32個のbyte値の配列 |
| `botd` → ランチャーのstdout | `port`、`local_only`、`startup_ok`、`mock_discord`。secretは含まない |
| ランチャー → 各 `botctl --bootstrap-stdin` の匿名stdinパイプ | `port`: 公告されたポート、`secret`: 同じdaemon用の配列 |

各JSONはUTF-8、末尾改行付きです。`botctl` は `127.0.0.1` の公告ポートへ接続し、各要求を認証します。接続情報はそのdaemonの存続期間だけ使用してください。自動検出用endpointファイル、一般用途の常駐ランチャー、OSサービス登録は提供していません。ローカルbootstrapのアクセス設定にはroleとShard scopeがあります。仕様のUnix socket / Named Pipeへの移行、OSユーザー権限の検証、遠隔管理は未接続です。別途mTLS transportの試験実装がありますが、本daemonの遠隔管理を提供しません。TCPを転送・公開して遠隔管理には使用しないでください。

#### secretファイル（systemd常駐・再接続用）

常駐させる `botd` には、匿名stdinの代わりに secret ファイルで管理 secret を渡せます（`--bootstrap-stdin` とは排他。どちらも無ければ起動エラー）。

```text
botctl secret generate --out ABS_SECRET
botd --file local-bots.toml --runtime-root ABS_RUNTIME --gateway-program ABS_GATEWAY --secret-file ABS_SECRET --control-port PORT
botctl status --secret-file ABS_SECRET --port PORT --json
```

- ファイルは 32 byte を 64 文字の 16 進で書いた 1 行（前後の空白・改行は可、生バイナリは不可）。`botctl secret generate --out` が `/dev/urandom` 由来の値をモード 0600 で新規作成します（既存ファイルは上書きしません。値は表示しません）。
- 読込前に、絶対パス・通常ファイル（最終要素がシンボリックリンクでない、追加のハードリンクがない）・モード 0600 または 0400・所有者が実効ユーザー（root 所有は 0400 かつ環境変数 `CREDENTIALS_DIRECTORY` 配下のときだけ）を検査します。違反、長さ不正、16 進以外、全ゼロなど極端に弱い値は、理由の種別だけを出して拒否します（secret・パスは出しません）。Linux/Unix のみで、他の OS では拒否します。
- `--control-port` を指定すると管理用 loopback ポートを固定でき（既定はランダム）、`botctl` は同じ `--port` で接続します。`--secret-file` と `--port` は必ず併用し、`--bootstrap-stdin` とは併用できません。この経路の capability は従来の secret のみの起動と同じローカル管理者権限です（role・scope の指定はできません）。
- ローテーションは新しい secret を作成して置換し、botd を再起動します（稼働中の再読込はありません）。
- 手順の詳細は [linux-deployment.md](linux-deployment.md)。

既定ではdaemon起動時にローカルShardを起動します。`--no-autostart` を追加すると、起動を `botctl start` まで保留できます。`startup_ok` は初回起動要求の結果であり、以後の実状態は `status` で確認します。Gatewayの環境は初期化し、通常モードではそのBot用のtokenとBot/Shard/起動世代だけを注入します。`PATH` や他Botの環境は継承しません（過去のWindows開発機用コードには `SystemRoot` を追加継承する処理が存在しますが、Windowsはサポート外です）。identity用4変数はtoken参照名に使えません。

Gatewayが起動するWorkerはさらに `env_clear` するため、任意名のBot tokenもCoreへ渡しません。状態・Worker更新監視ファイル・計画上のIPC/管理パスはBot/Shard別です。業務状態はメモリ内であり、状態パスの割当が永続化やGateway再起動時の復元を意味するわけではありません。

### Discordに接続しない模擬実行

実GatewayとWorkerの経路をtokenなしで検証するには、試験ランナーがloopbackでcollectorを待ち受け、botdに次の追加オプションを渡します。

```text
botd --file local-bots.toml --runtime-root ABS_RUNTIME --gateway-program ABS_GATEWAY --bootstrap-stdin --mock-discord 127.0.0.1:PORT
```

`PORT` は試験ランナーが確保した非ゼロポートへ置き換えます。模擬モードは既定無効で、token未設定や接続失敗時に暗黙で有効になりません。IPリテラルのloopbackだけを受け付け、ホスト名・外部IP・wildcard・ポート0は拒否します。選択は最初の起動試行前に確定します。botdの管理認証は通常どおり必要です。

このモードのdaemonはBot token参照先を読み取らず、Gatewayへtokenを渡しません。Gatewayは指定collectorへ外向きに接続し、実Discordのメタデータ・WebSocket・RESTを呼びません。Gateway側に新たな管理listenerは作りません。collectorは試験専用の入力・観測先で、botd管理secretを受け取りません。単なる起動オプションだけで手動対話が始まる機能ではありません。

collectorからの正規化されたprefix/Slash相当JSONは実Workerの同じハイブリッドハンドラーへ渡ります。`testkit.rs` の模擬REST/Interactionへの応答後、`api-result-v1` により要求元Workerへ結果を送り、操作ID・世代を照合したACKを確認します。試験用の状態照会・crash注入もこの明示モードに限定します。模擬JSONは64 KiB上限、読取開始後5秒以内に終端が必要です。模擬RESTの接続先もloopbackに限定します。実DiscordのWebSocket入力解析やHTTP/TLSはこのモードでは検証しません。

Workerへ戻すのはHTTPステータスまたは結果不明で、HTTP本文・Interaction tokenは渡しません。これはWorkerの未完了操作表で照合するIPC受領確認です。Frameworkの `ctx.reply()` は応答バッファへの追加のままで、ハンドラーのreply FutureがHTTP結果を返すAPIには未接続です。

再現用の統合試験は次のコマンドです。Bot AのShard 0/1とBot BのShard 0を別Gatewayプロセスで起動し、実Worker、模擬HTTP、再起動・停止までを扱います。最終実行結果は [検証記録](shard-runtime-design.md#検証記録と未検証範囲) を確認してください。

```sh
cargo test --offline --test e2e_shard_runtime
```

この試験は実Discordの登録・接続・REST、Bot全体のIdentify/REST予算、quorum/lease、Gateway新旧WebSocketのActive切替を検証するものではありません。別の `e2e_gateway_process_switch` は明示試験モードで実Gatewayプロセスの新旧切替を模擬Discord上で扱いますが、実WebSocket切替や本番安全性の証明ではありません。`--offline` だけでは実行プログラムのDiscord通信は無効にならず、模擬モードの明示が必要です。

### 状態表示と操作

以下のコマンドは、上記ランチャーが各プロセスのstdinへ接続情報を渡す前提です（systemd 常駐の botd へは各コマンドに `--bootstrap-stdin` の代わりに `--secret-file ABS_SECRET --port PORT` を付けます）。Bot名・完全ID・一意なID接頭辞をdaemon側の `botref` で解決します。名前と別BotのID接頭辞の衝突を含め、複数一致はエラーです。

```text
botctl ps --bootstrap-stdin --json
botctl status music --bootstrap-stdin --json
botctl inspect 101 --bootstrap-stdin --json
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

通常stopは子の匿名stdinパイプへ `{"command":"shutdown"}` と改行を送り、子が処理を終えて終了コード0で終了するのを確認します。実Gatewayの契約は512 byte上限で、未知command・追加field・過大/未終端フレームを拒否します。daemon消失によるstdinのEOFや不正入力を即時終了命令として扱いません。

実Gatewayは新規入力を止め、受け付け済み処理とREST結果確認を待ち、Workerの周期処理・新規受付を止めて各Workerのstdinを閉じます。全Workerの終了コードと子ハンドルの回収を確認してからGatewayが正常終了します。結果不明がある場合も子の終了・回収を待ちますが、外部操作の成否は未確定のままで、Gatewayは非ゼロ終了しstopを失敗とします。期限超過・Worker異常終了もstop成功とは表示せず、daemonは自動でforceへ切り替えません。この契約は任意アプリの未管理タスクや外部副作用の完了まで保証するものではありません。

既に停止したBotへのstopは成功します。起動中、または自動再試行待ちのBotへの新しいstartはエラーです。restartは対象の旧子がすべて正常終了したことを確認してから新世代を起動します。同じBotの処理中操作を `stop --force --yes` で置き換える場合、元の操作は `OperationSuperseded` として失敗し、force stopを優先します。

`Running` やstart成功はDiscord READY、Heartbeat、Coreのdrain完了、quorum承認、leaseの有効性を証明しません。daemonはGateway子を管理し、GatewayがWorkerのライフサイクルを所有します。実Discord検証、Shard Core全資源のライフサイクル、分散Identify・failoverへの統合は別途必要です。

### 操作ID、timeout、失敗の確認

起動・停止・再起動には、CLIが送信前に生成した操作IDを付けます。`--operation-id ID` で32桁の小文字16進数を指定することもできます。結果にはID、操作対象、状態、所要時間、Shard別の結果・世代、失敗Shardを含みます。`--json` は成功・失敗とも機械可読なJSONをstdoutへ出し、失敗や完了未確認は非ゼロ終了コードで返します。`--json` なしの成功表示は整形JSON、失敗表示はstderrです。

`--timeout` はbootstrap読取・接続・送受信・操作完了待ちを含むCLI全体の待機上限です。既定30秒、0より大きく3600秒以下を指定できます。単位なしと `s` は秒、`ms` はミリ秒で、小数も指定できます。待機timeoutや通信断でCLIが終了しても、受理済みのdaemon操作を取り消しません。新しいIDで同じ操作を送る前に、表示されたIDで結果を確認してください。

```text
botctl operation 0123456789abcdef0123456789abcdef --bootstrap-stdin --json
```

操作状態は `running` / `succeeded` / `failed` です。`running` は完了を意味しません。`operation` の照会時に処理中なら `pending: true`、`ok: false` と非ゼロ終了コードを返します。daemonの操作期限は30秒で、Graceful停止が間に合わなければ失敗を記録し、子を強制終了しません。状態照会を続け、必要なら対象と影響を確認して明示的にforce stopを実行します。一部Shardだけ成功する場合もあり、操作失敗が全Shardの停止や変更の取消を意味するわけではありません。全体の終了コードに加えShard別結果と現在の `status` を確認してください。

実Gatewayの受理済み処理待ちは30秒、Worker管理側の停止待ちは60秒で、daemon操作の30秒とは別の期限です。daemonがtimeoutを返しても後から子が終了する場合があります。失敗した操作の結果だけでなく、現在のプロセス状態を再確認してください。

更新監視の取消中などに大きな未読IPC応答がWorker stdoutを塞ぐ停止競合は未検証で、shutdownがtimeoutする可能性があります。正常stop・遅延REST試験で確認した子回収を、この条件での正常終了や孤児不在の保証とは扱わないでください。

操作記録はdaemonのメモリ内に最大1024件保持し、満杯なら新規操作を拒否します。1操作の対象はローカル64Shardまでで、これを超えるBotへの起動・停止・再起動は処理前に拒否します。同じID・同じ操作は記録済み結果を返し、異なる対象・操作へのID再利用は拒否します。記録はdaemon再起動で失われ、永続監査や再起動を跨ぐ重複排除にはなりません。再照会で見つからないことだけを未実行の証拠にしないでください。

daemon終了時、既定では子を無条件でkillしません。子が残る可能性があるため起動予約を保持します。任意の `botd --stop-children-on-exit`（既定オフ。標準の単独daemonのみ）を付けると、SIGINT／SIGTERM（Linux）を受けた botd が起動した全Shardの子へgracefulな停止を要求し、最大30秒待って停止した子の予約も解放して終了します。期限内に止まらない子は強制終了せず、標準エラーに出して残します（フラグ無しではSIGTERMを処理しません）。再起動時に予約が残っていた場合、Linuxでは `/proc/*/cmdline` を調べ、そのShardのGateway（実行ファイルと `--management-path` が一致）が生きていないと確認できたときだけ予約を削除して起動し、標準エラーにShardキーと世代を1行出します。生きている、または `/proc` を読めず判定できない場合（Linux以外を含む）は従来どおり拒否します。PID・起動世代・OS開始時刻・認証済みIPCによる孤児の照合と再採用（生きている子を新しいbotdが引き継ぐこと）は未実装のままで、生きている孤児は運用者が外部で実態を確認して整理します。同一runtime rootに複数のbotdを同時に動かす構成は非対応です。これらの動作はローカル未ビルドで、CIの検証待ちです。ランタイムrootの別名・シンボリックリンク、OSやサービス管理側による子の終了、停電時の永続性は保証していません。Windows ACL はサポート外（Windows非対応）です。

`botctl update` / `rollback` は計画と成果物の検証後、明示的な要求を認証付きdaemonへ送れます。通常runtimeは実行を利用不可として失敗させます。試験用Gateway切替runtimeには実行経路がありますが、本番更新・rollbackの完成を意味しません。`create`、`logs`、`nodes`、`doctor` などのdaemon機能も未提供で、既存の `manage` / `reload` / `doctor` は以下の別経路を利用します。

## 更新・rollbackのオフライン計画：botctl

`update` / `rollback` はJSON manifestを必須とし、既定でdry-runです。`--component` の既定は `core`、選択肢は `core` / `gateway` / `all` です。Gatewayを含む計画にはIdentify制限の観測値も必要です。manifest形式の例と制約は [update_rollback.rs](../src/src/update_rollback.rs) を参照してください。パスはmanifestのディレクトリから解決し、対象成果物のSHA-256を照合します。秘密情報をmanifestに含めないでください。

```sh
cargo run --offline --bin botctl -- update BOT --plan manifest.json --component core --dry-run --json
cargo run --offline --bin botctl -- rollback BOT --plan manifest.json --component core --dry-run --json
```

rollbackでも候補のプロセス世代は現在より大きくします。計画中のIdentify情報は入力された観測値に基づく推定であり、残量予約、最新状態、quorum・lease、所有権、準備完了の証明ではありません。ハッシュ一致も署名、実行可能性、将来の起動時点のファイル一致を保証しません。

`--yes --bootstrap-stdin` は検証後にdaemonへ要求を送ります。daemonはbootstrap由来のrole・scopeで認可し、監査と成果物再検証を行います。通常runtimeは実行を拒否し、操作IDで結果を照会できます。試験用Gateway切替runtimeの経路は模擬Discord専用です。操作履歴は揮発性で、計画や受付の成功は実更新・rollbackの成功ではありません。

## 試験実装と運用上の境界

- `fs_perms` はLinuxで開いたファイル（0600）・ディレクトリ（0700）のmodeを設定・検証します。Windows用パスAPIコードは残りますが、Windowsはサポート外（Windows ACL、ハンドルAPI、ディレクトリ保護は未解決課題ではなく『サポート外』）です。TOCTOU等の制約があり、既存の秘密ファイルは自動移行されません。詳細は [fs_perms.rs](../src/src/fs_perms.rs) を参照してください。
- `tls_transport` の証明書更新と [cert_policy.rs](../src/src/cert_policy.rs) の認証付き永続policyはライブラリ機能です。CA更新を含む安全な配布・有効化・失効の運用は統合されておらず、再起動時の復元や信頼経路も利用側の責務です。
- OpenRaftの `raft_quorum`、mTLS Raft RPC、`cluster_runtime::bind_raft` は接続された試験経路です。botdの本番backend起動は拒否され、模擬Discord等の明示的条件が必要です。永続合意の存在を本番failoverや包括的fencingの保証としないでください。
- Gatewayのプロセス切替経路は `--allow-test-gateway-switch`、`--mock-discord`、`--mock-identify-budget` を必要とするstandalone自動起動の試験専用です。実Discord・本番Gateway更新には使えません。試験では新旧プロセスと模擬応答を観測しますが、実ネットワークや障害全般の無停止・無欠落を保証しません。
- frameworkの `FrameworkStateStore` はBot/Shardとnamespaceに束縛した状態アダプターです。ファイル永続backendもありますが、実Worker経路への接続やホスト障害を跨ぐ保証を意味しません。

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

秘密をソース・ログ・コマンド履歴に記録しない方法で `DISCORD_TOKEN` を設定します。prefix処理には Message Content Intent が必要です。実行バイナリはGateway情報を取得し、設定総Shard数がDiscord推奨値未満の場合は起動を拒否します。manageの既定は1Shardです。取得失敗・timeoutでも起動しません。IdentifyキューはGatewayプロセス内だけで、別プロセス・ホスト・再起動を跨ぐBot単位の予算共有は未接続です。

内部のCargo呼び出しもofflineにする場合、管理プロセス起動前に環境を設定します。

```sh
# Linux shell（サポート対象）
export CARGO_NET_OFFLINE=true
```

（過去のWindows開発機での実行には PowerShell で `$env:CARGO_NET_OFFLINE = 'true'` を使用した記録がありますが、Windowsはサポート外です。）

```sh
cargo run --offline --bin manage -- start
cargo run --offline --bin manage -- status
cargo run --offline --bin manage -- logs
cargo run --offline --bin manage -- list-unknown
cargo run --offline --bin manage -- metrics-json
cargo run --offline --bin manage -- metrics-prometheus
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
| `metrics-json` / `metrics-prometheus` | 管理プロセス内のregistryをJSON / Prometheusテキストで表示。現状Shardの計測値が自動収集されるわけではなく、HTTP公開もない |
| `stop` | Bot親を強制終了し、管理プロセスを残す。メモリ状態を失う |
| `restart` | 現在のBot親バイナリを再起動。再ビルドはせず、メモリ状態を失う |
| `shutdown` | Bot親と管理プロセスを終了。メモリ状態を失う |

既定の管理TCPは `127.0.0.1:39483`。`DISCORD_CORE_DIR` で管理ディレクトリ、`DISCORD_CORE_PORT` でポートを変更できます。起動時と操作時で揃えてください。既定ログは `src/core-runtime/core.log`、endpointは `src/core-runtime/core.json` と読み取り権限用 `viewer.json`（リポジトリルート基準）です。両方とも管理認証情報を含むため内容を共有しないでください。管理経路はadmin/viewerを区別し `audit.jsonl` に監査します。管理ディレクトリのアクセス権保護が必要です。

管理操作とビルドは直列です。更新ビルド中はstatusやBot親の異常終了監視も待機する場合があります。Bot親が異常終了すると管理プロセスは最大3回再起動を試み、60秒の安定稼働後に回数をリセットします。OSサービス登録やホスト障害からの復旧機能ではありません。

## Worker更新：update / reload

```sh
cargo run --offline --bin manage -- update
# 同じWorker更新を、管理CLIを経由せず依頼する場合
cargo run --offline --bin reload -- general
```

`general` だけを受け付け、実際は全Workerを1個ずつ更新します。Rustコードの動的ロードやGateway更新ではありません。親/Gateway/HTTP部分の変更は親の再起動が必要です。

reloadはreleaseビルド → `src/workers/` への成果物コピー → `src/worker-path.txt` に更新先を書き込み → 完了確認を行います。確認待ちは約20秒で、timeoutや失敗後も更新要求は残り、親が再試行し得ます。直ちに再実行せず、ログと `!workers` / `!version` で実状態を確認してください。ビルドIDはコードの差分を証明しません。

上記は従来のmanage/reload経路です。botd配下のGatewayはShard別のWorker更新監視ファイルを使用するため、共有の既定ファイルを書き換えて全Shardを更新できるとは扱わないでください。botctlの通常runtimeでは更新実行を拒否します。

対応capabilityを交渉できた場合、snapshotを引き継いだ新Workerへの配送を開始し、旧Workerの受付済み処理と外部応答の確認を並行して待ちます。旧版が対応しない場合は直列切替です。別Workerは処理を継続できます。途中失敗時は切替済みWorkerが新版、未切替Workerが旧版のまま残り得ます。全体を旧版へ戻す原子的rollbackやCLIはありません。切替前に結果不明があれば更新を拒否し、切替後に不明になった場合は新版をActiveのまま旧世代の記録を保持します。`list-unknown` で照会できますが、照合・解除CLIや永続的な結果不明操作の記録は未実装です。再起動で追跡が消えても外部操作の成否が判明したことにはなりません。

実Gatewayで使う成果物にはWorkerのReady/Helloで `api-result-v1`、`command-dispatch-v1`、`slash-arguments-v1`、`ready-event-v1` を確認できることが必要です。初回起動・更新・復旧で不足する候補は採用前に拒否し、更新時の旧Workerの状態・受付を維持します。汎用Manager APIの旧v5互換は残りますが、それだけを実Gateway用成果物の適合性確認とは扱わないでください。

引き継ぎ対象は `!count` のカウンターと `!job` の計算状態です。`!job start` はチャンネルごとに1個の計算を開始し、Workerの約1秒周期で進みます。`!job status`、更新、再度statusで明示的checkpointの継続を確認できます。任意のFutureや外部APIの実行途中を移動しません。StateMap/Cogの状態はsnapshot対象外です。

`DISCORD_WORKER_FILE` を使う場合は親とreloadに同じ絶対パスを渡します。更新指定ファイルと `workers/` は任意バイナリ実行につながるため、信頼する管理者だけに書込権限を限定してください。Windows ACL はサポート外（Windows非対応）です。成果物の署名検証、全ログ経路の秘密マスクは検証・実装が残っています。

## 復旧と検証の境界

`child_recovery` はdaemonのGateway監視とWorker側の復旧経路に接続されています。再試行・予算・degraded状態の観測は、孤児の再採用やホスト障害からの安全な分散復旧を意味しません。`metrics_feed` はライブラリとテストがありますがdaemonに接続されていません。

`discord_smoke` は明示実行の読み取り専用REST確認補助CLI（`src/src/bin/discord_smoke.rs`、`src/tests/discord_smoke/cli.rs`）で、GatewayやCoreを起動しません。実Discordでの検証はユーザーが実施する範囲です。性能測定の条件とLinux共有runner（Actions `ubuntu-latest`、AMD EPYC 7763 4vCPU、IPC往復 p99 = 115.5/258.1/973.6 µs、failover検知 Suspected p50 1503.6 ms / FailoverPending p50 3003.1 ms、共有ランナーで本番保証ではない、往復値で片道p99の直接測定ではない）の記録は [benchmarks.md](benchmarks.md) を参照してください。手動ワークフローとして `.github/workflows/stress.yml` と `bench.yml` が用意されていますが、更新負荷中の無停止条件は未達で `src/tests/update_load.rs` は `#[ignore]` です。
