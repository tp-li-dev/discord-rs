# 運用ガイド（ローカル試作）

現在は単一Bot・単一Shardの実行系です。manageの表示にある「コア」はローカル管理プロセスを指し、仕様のShard単位Coreとは異なります。`botctl` / `botd`、Bot名・ID・短縮IDによる選択、分散管理は提供していません。以下はすべてリポジトリルートで `cd src` した後のコマンドです。[制約一覧](known-limitations.md) も確認してください。

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
