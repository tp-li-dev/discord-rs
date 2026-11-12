# Shard 単位のローカル実行と模擬 Discord 試験

`botd` が配置計画に従って Shard ごとに実 `src` Gateway バイナリを起動し、各 Gateway が実 Worker プールを所有する経路を接続した。平常時は1 Shardにつき1 Gatewayプロセスで、既定の2 WorkerはそのShard内の処理分担であり、別Shardではない。実Discordを使用しない明示的な模擬モードも同じバイナリに用意する。

この文書は2026-11-12の実起動接続の変更を記述する。[architecture.md](architecture.md) が仕様の基準である。[progress.md](progress.md) と [known-limitations.md](known-limitations.md) には、変更前の「実起動未接続」「fixtureのみ」に相当する記述が残る。両ファイルはPM統合対象として今回変更していない。以下の実装契約と検証結果を区別し、§16.2全体の完了を意味しない。

## 起動経路と対象範囲

1. `botd` は設定を検証し、`cluster.mode = "standalone"`、遠隔bindなし、固定Shard数、当該ノードがprimaryのShardだけを対象にする。未解決の `auto`、quorum構成、遠隔bindは拒否する。
2. `shard_runtime::plan` がBot/Shard別の引数・環境方針・パスを生成する。daemonは指定された絶対パスのGateway成果物をシェルを介さずに起動する。
3. GatewayがBot/Shard/総Shard数を検証し、同じ実行ファイルの `--worker` を子として起動する。双方向Helloでidentityとcapabilityを照合し、Workerの準備を確認する。
4. daemonはGatewayの所有済み子ハンドルを監視し、異常終了時は既存の回数・時間窓・backoff制限内でそのShardだけを再起動する。他Bot/Shardを一緒に再起動しない。
5. 通常stopは匿名stdinのShutdown契約を使い、GatewayがWorkerを停止・回収して正常終了したことをdaemonが確認する。

`Running` と `start` 成功はOS子プロセスの存在の観測である。Discord READY、Heartbeat、所有権、leaseの健全性を表さない。Gateway READYのdaemon向け通知、複数ホスト間の配置・failover、分散Identify予算、quorum/lease/fencingは未接続であり、本経路がそれらを代行することはない。

## 起動仕様・identity・秘密の分離

`shard_runtime::plan(&[LocalShardIntent], &Path)` は純粋な計画生成で、環境変数の参照、ファイル操作、プロセス起動、listener作成を行わない。出力順はBot ID、Shard IDの順。各 `ShardRuntimeSpec` はGateway/Coreのプログラム名、個別の `OsString` 引数、状態・管理パス、当該ShardのIPCパスを持つ。

Gatewayの引数は `--bot-id`、`--shard-id`、`--total-shards`、`--ipc-path`、`--state-path`、`--management-path`、`--token-env`。`botd --gateway-program` が計画上の `gateway-host` を実成果物へ置き換える。Core欄の `bot-core` は予約された起動仕様であり、daemonがそれを別途起動するわけではない。実CoreはGateway所有のWorkerプールである。

daemonは毎回 `env_clear` し、通常モードでは当該Botの `credential_refs` で指定したtokenだけを読み取り・注入する。さらに `BOT_ID` / `SHARD_ID` / `TOTAL_SHARDS` / `GENERATION_ID` を計画と起動世代から設定する。WindowsではloopbackのIPリテラルへの接続にも必要だったOS変数 `SystemRoot` だけを追加継承し、未設定・空なら固定診断で起動を拒否する。`PATH` や他の環境は継承しない。引数と環境のidentityが矛盾する実Gateway起動は拒否する。Workerは隔離起動で再び `env_clear` し、必要なIPC identityだけを渡す。`DISCORD_TOKEN` に加え任意の `token_env` 名の値もCoreへ継承しない。秘密値は起動仕様・予約記録・試験の結果へ書き出さない。

Windowsの環境変数名の大小文字非区別も考慮し、異なるBotで同じtoken参照名を使う計画、identity用4環境変数と `SystemRoot` をtoken参照名に使う計画は両OSで拒否する。起動仕様の任意変数による `SystemRoot` の上書きも拒否する。異なる環境変数名に同じtoken値を登録した場合の同一性検査は行わない。起動世代はdaemon内の単調な番号とdaemon incarnationを持つが、IPCの数値世代やownership epochを再起動を跨いで永続化した保証ではない。

**Bot IDの互換制約**: 配置設定・計画・daemonは従来どおり文字列IDを扱うが、実 `src` / WorkerのIPCは `BotId(u64)` である。このバイナリを起動する設定では `bot_id = "101"` のようなu64の正規の10進数文字列が必要。UUID・任意文字列の計画生成に成功しても実Gatewayは起動に失敗する。`"1"` と `"001"` が同じIPC identityになることを避けるため、先頭ゼロ付き数値や `+` 付き表記も拒否する（`"0"` 自体は数値表現として有効）。文字列をハッシュ等で数値へ暗黙変換しない。文字列IDのIPC対応は別途必要である。

## パスと状態の分離

パスはホストOSの絶対rootへ `Path::join` し、次の名前空間を割り当てる。

```text
root/bot-<Bot IDのUTF-8小文字16進表現>/shard-<番号>/
  ipc
  gateway-state
  core-state
  gateway-management
  core-management
```

Bot IDは1〜64 UTF-8 byte、前後空白・制御文字は禁止。CON/NUL等の予約名、区切り、ドライブ表現、末尾ピリオドを生のパス成分にせず、大小文字の異なるIDも異なる16進表現にする。rootの親参照、Shard範囲外・重複、同一Bot内の総数/変数名の不一致、パスの大小文字非区別での衝突を拒否し、入力をエラー本文へ転記しない。

実GatewayのWorker更新監視ファイルはShard別の `gateway-state/worker-path.txt` へ置く。`ipc` / `core-state` / 管理パスの計画上の割当は、Unix socket・Named Pipe・永続StateStoreの実装を意味しない。Gateway–Worker間の実IPCは世代ごとの匿名stdin/stdoutパイプである。カウンター・ジョブ・Framework StateMap等の業務状態はメモリ内で、Gateway再起動による復元は保証しない。

rootと実行成果物は信頼する配備入力である。ACL、symlink/junction等の別名、OS/transportの長さ制限、署名検証、停電時の永続性は本経路で保証しない。

## 明示的な模擬 Discord モード

`botd --mock-discord 127.0.0.1:PORT` を明示した場合だけ有効になる。`PORT` は試験側が所有するcollectorの非ゼロポートで、IPリテラルのloopbackアドレスだけを許可する。ホスト名、外部IP、wildcard bind相当、ポート0は拒否する。環境変数の継承、tokenの未設定、Discord接続失敗から暗黙に有効化しない。選択は最初の起動試行前に確定し、daemonの起動通知にも `mock_discord` を含める。

模擬モードではdaemonはtoken参照先を読み取らず、Gatewayへtokenを渡さない。Gatewayは実Discordのメタデータ・WebSocket・RESTへ接続せず、指定collectorへ外向きのローカルTCP接続を1本張る。Gateway側に模擬管理listenerを公開しない。collectorは試験ランナーが所有する専用の入力・観測経路であり、認証付きbotd管理APIの代替ではない。botdの管理secretも共有しない。

collectorとの改行JSONは上限64 KiB、読み始めたフレームの完了期限5秒。起動通知にはGateway PID、Bot/Shard/総数、計画パス、Hello確認済みWorker identity、credential有無の真偽値を含める。credentialの実値は含めない。模擬入力は正規化されたprefix/slash JSON、状態照会、明示的なGateway crash注入で、模擬REST接続先もloopbackに限定する。Discord WebSocketイベントの実デシリアライズやHTTP/TLSを再現するモードではない。

`!ping` 相当と `/ping` 相当の入力は実Workerの既存ハイブリッドハンドラーを使う。Slashでは模擬Interactionのdefer確認後にdispatchする。応答は `testkit.rs` の模擬REST/Interactionサーバーへ送信し、`api-result-v1` capabilityの結果通知を発行元Workerへ返し、操作IDと世代が一致するACKを確認する。Worker応答に対するHTTPステータスを完全受信した場合は429/5xxを含め `Confirmed { status }` として返す。確認済みHTTP失敗を成功とはせず、応答喪失・timeout・結果ACK不明による `Unknown` と区別する。非冪等操作を無条件再送しない。

`ApiResult` が渡すのはHTTPステータスまたは `Unknown` で、HTTP本文やInteraction tokenは渡さない。Workerの未完了操作表で帰属を照合してACKするIPC受領契約である。Frameworkの `ctx.reply()` は従来どおり応答バッファへの追加であり、HTTP結果をハンドラー内のreply Futureの戻り値へ返す高水準APIには未接続である。

実Gateway専用の `Manager::start_gateway_pool` は、WorkerのReady/Helloで `api-result-v1`、`command-dispatch-v1`、`slash-arguments-v1`、`ready-event-v1` を確認してから受付を開始する。初回起動・更新・復旧の候補が不足する場合は採用前に拒否し、更新中の旧Workerの状態・受付を変更しない。汎用の `start_pool_with_identity` / `start_pool_isolated` は従来の旧v5互換を維持する。汎用Managerで起動できることだけを、実Gateway用成果物の適合性と同一視しない。

この経路はT01と、T08が要求する結果帰属の一部の検証に用いる。Gatewayの新旧WebSocket並行接続・Active切替そのものを実装・検証したことにはならない。模擬HTTPの成功は実Discordの権限・Rate Limit・3秒期限遵守の証明でもない。

## Graceful stop と孤児の扱い

daemonから起動したGatewayの制御入力は、所有済み匿名stdinパイプの `{"command":"shutdown"}` と末尾改行。上限512 byte、未知commandや追加fieldを拒否する。EOF、壊れたJSON、過大・未終端フレームは終了命令ではない。daemonの消失や管理クライアントの切断だけでGatewayを即時終了しない。

正常なShutdownでは新規入力を止め、受け付け済みコマンドと外部応答確認を待つ。Worker管理側の新規受付・周期タスクを停止し、処理と結果追跡を整合させ、Worker stdinを閉じる。WorkerはEOFに従って終了し、Gatewayは所有する子ハンドルをwait/reapする。全Workerの正常終了後にGatewayが終了コード0を返し、daemonがGatewayの終了を回収した時点でstop成功となる。パイプへの書込成功はstop完了ではない。

結果不明が残る場合もWorkerのstdinを閉じて全子の終了・回収を待つが、成功したdrainとは扱わずGatewayは非ゼロ終了、daemonのstopも失敗とする。結果不明の追跡は非永続であり、子の終了が外部操作の成否を確定するわけではない。

Gatewayの受理済み処理の待機は30秒、Worker管理側の停止待機は60秒、daemonのstop操作期限は30秒で、層ごとの期限は異なる。daemonのtimeout後も子が処理・停止中の場合がある。daemonは自動forceへ切り替えず、操作結果と現在の実状態の再確認が必要となる。任意のアプリタスク・未管理Future・外部副作用まで回収できる包括的保証はない。

更新監視の取消中などに大きな未読IPC応答がWorker stdoutパイプを塞ぐ停止競合は未検証で、shutdownがtimeoutする可能性がある。正常stop・遅延REST中のE2Eで観測した子回収を、この条件での正常終了や孤児不在へ一般化しない。

daemon自体の終了時はGatewayを一括killせず、重複起動防止の予約を残す。daemon再起動後のPID・起動世代・OS開始時刻・認証済みIPCによる再照合・再採用は未実装である。Gateway crash後のWorker EOF終了と、明示stopによる回収はE2Eの対象だが、OS suspend、ホスト消失、サービス/ジョブオブジェクトによるteardown、強制終了一般で孤児が出ない保証はしない。

## 検証記録と未検証範囲

2026-11-12、Windows / Rust 1.99.0の最終コードで、子Cargoにも `CARGO_NET_OFFLINE=true` を適用した `cargo test --offline --workspace` が成功した。574件成功・失敗0件で、既存の手動ベンチマーク1件のみignore。新規の実Gateway E2E 5件、Worker結果通知・停止・互換性の13件、Gateway単体14件を含む。`cargo fmt --all --check`、`cargo clippy --offline --workspace --all-targets -- -D warnings`、`git diff --check` も成功した。

[e2e_shard_runtime.rs](../src/tests/e2e_shard_runtime.rs) の構成はBot AのShard 0/1、Bot BのShard 0で、実 `botd` → 実 `src` → 実Workerを使用する。3 Gatewayと6 WorkerのPID・Hello identity、両入力からのREST結果帰属、状態分離、対象Gatewayの再起動、遅延REST中のstopと子プロセス終了を確認した。これは記載した障害条件とローカル試験構成での観測であり、一般的な孤児不在や本番動作の保証ではない。

障害試験には結果不明・Interaction ACK喪失・HTTP 429/503、応答なし・ハンドラーエラー、過大/不正入力による対象Shardだけの再起動、collector EOFとdaemon死亡でのGateway存続、模擬モードの明示性を含む。[e2e_shard_worker.rs](../src/tests/e2e_shard_worker.rs) は結果通知の帰属・ACK検査、停止、必要capabilityのない候補の採用前拒否と旧Workerの継続を検証した。

既存の [shard_runtime.rs](../src/tests/shard_runtime.rs) は純粋な計画の順序・入力拒否・パス分離を、[shard_plan.rs](../src/tests/shard_plan.rs) は模擬メタデータと単一プロセス内Identify bucketを検証する。これらを実Discord、複数ホスト、lease、分断耐性の証拠としない。

実Discordへの接続・登録・HTTP、Linux/Ubuntu CI、Windows ACL、分散Identify/REST予算、quorum/lease/fencing、Gateway並行更新、再起動を跨ぐ状態・結果不明の永続化は未検証または未接続。T01〜T20、§16.2、無停止、完全無欠落、exactly-onceの達成は宣言しない。
