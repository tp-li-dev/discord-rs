# 既知の制約・未実装・未保証事項

## 現行実装の補足

> [!NOTE]
> 2026-11-13の決定により、**対応OSはLinux(Ubuntu)のみであり、Windowsはサポート外**（開発機での動作は保証しない。Windows固有コードは残るが保証・CI対象外）です。Windowsでの検証結果はすべて『過去の参考記録』と位置づけられ、Windows ACL未実装などの未対応事項は未解決課題ではなく『サポート外』として扱われます。

以下の過去の記述に優先する。`cluster_runtime::bind_raft` は OpenRaft と mTLS Raft RPC を接続する試験経路だが、模擬Discordを必要とし本番起動は拒否される。`fs_perms` は Linux において開いたハンドルの Unix mode をファイル0600・ディレクトリ0700に設定・検証する。Windows 用パス API コードは残るが、Windows はサポート外（Windows ACL、ハンドル API、ディレクトリ保護は未解決課題ではなく『サポート外』）である。信頼されたパスを前提とし TOCTOU を完全には防がない。

`FrameworkStateStore` と `StateStoreAdapter`、ファイル永続 StateStore、`subscription`、`durable_jobs` がある。ローカルの部品や試験は複数ホストの耐久性や実Discord副作用の exactly-once を保証しない。`child_recovery` は daemon と Worker の復旧経路に接続されるが、任意の孤児再採用や全障害条件を保証しない。`metrics_feed` は存在するが daemon には未接続。

証明書の leaf 切替・失効、永続ポリシーと CA チェーンを扱う部品はあるが、配布・本番運用への自動統合は保証されない。`update_rollback` と daemon 更新制御は実装され、capability による role / shard scope と監査を用いるが、実行できる更新には下記の制約がある。

`src/tests/update_load.rs` の `continuous_events_during_updates_and_core_crashes` は受入基準未達の既知失敗として `#[ignore]`。実Discord smoke CLI（`src/src/bin/discord_smoke.rs`、`src/tests/discord_smoke/cli.rs`）の実接続はユーザーが実施する検証範囲。`stress.yml` と `bench.yml` は手動ワークフローである。[progress.md](progress.md) は過去の Linux CI 成功を記録するが、Windows はサポート外となったため Windows Actions 再確認は対象外。この文書更新では CI やテストを実行していない。

2026-11-12時点の実装と [progress.md](progress.md) に基づきます。[architecture.md](architecture.md) §16.2 の全条件は未達です。個々の条件と試験の根拠は [conformance.md](conformance.md) を参照してください。模擬試験や独立した部品の成功は、実Discord・実ネットワークでの保証ではありません。実Discordでの検証はユーザーが実施する範囲です。

## 実行構成・配置・分散制御

- `botd` は設定された Bot/Shard ごとに Gateway 子プロセスを起動・監視し、各 Gateway は Worker プールを所有します。明示的な模擬Discordモードでは複数Bot・Shardの別プロセス起動と隔離を試験しています。実Discordでの複数Shard稼働は未検証です。プロセスが存在することは Discord Ready、所有権、lease の健全性を意味しません。
- `botctl` はローカル daemon の ps/status/inspect/shards/start/stop/restart/operation とオフライン設定検証に対応します。update/rollback は既定で成果物をハッシュ確認するオフライン計画、`--yes`（`--bootstrap-stdin` または `--secret-file`）で認証付き要求と操作IDによる再照会を行います。systemd 常駐の botd へは、管理 secret をファイル（`botd --secret-file ABS [--control-port PORT]`、`botctl --secret-file ABS --port PORT`、`botctl secret generate --out ABS` で作成）で渡して接続できます。ファイルは 64 文字の 16 進 1 行で、通常ファイル（シンボリックリンク・追加のハードリンク不可）・モード 0600/0400・所有者が実効ユーザー（root 所有は 0400 かつ `CREDENTIALS_DIRECTORY` 配下のみ）を検査し、満たさなければ拒否します。この検査と systemd 実機での常駐接続は Linux の実機では未検証（CI の試験は模擬環境）で、途中ディレクトリのシンボリックリンク・POSIX ACL・ファイルシステムの強制は検査せず、secret のローテーションには botd の再起動が必要です。読込バッファの消去は最善努力でメモリ全体からの消去は保証しません。実行成功に対応するのは明示的な模擬Discord Gateway-switchモードの Gateway 更新のみです。通常モードと Core/all は必要な制御契約が未接続のため拒否します。logs、nodes、doctor 等の daemon 操作は未実装です。操作履歴は揮発性で、daemon再起動後の子プロセス自動引継ぎや残存予約の自動解除はありません。
- 配置検証・Shard起動仕様・Identify予算・更新ゲートはありますが、複数ホストへの実配置、設定世代同期、プロセス間・ホスト間でのIdentify予算共有は未完成です。実Discordの推奨Shard数や制限への追従を包括的に検証していません。
- ownership、lease、watchdog、failover/failback には状態機械や模擬時計・模擬ネットワークの試験があります。本番のプロセス起動・Discord送信・quorumとの統合は未完成です。botd 既定経路は systemd `Type=notify` 向けに `READY=1`（制御経路 listen 開始後）・`WATCHDOG=1`（`WATCHDOG_USEC` の半分間隔、監視ループの tick 完了後のみ）・`STOPPING=1` を送りますが（`src/sd_notify.rs`）、テスト用クラスタ/Raft/Gateway 切替モードは未対応で、実 systemd での検証は未実施です。3秒検知・30秒安定確認は設定上の目標で、実復旧時間の保証ではありません。実験的 Raft ストアには epoch 永続化がありますが、本番統合や OS suspend 時の時計安全性は未検証です。
- `cluster.rs` の過半数ACKは試験用シミュレーションであり本番合意ではありません。メインworkspaceの `raft_quorum` は OpenRaft 0.9.26 を用いる実験的な固定メンバー構成で、永続予約・epoch CAS とローカル admission gate を備えます。試験用cluster runtimeとmTLS Raft RPCに接続されますが、本番起動は拒否されます。本番fencingは未検証です。保持予約は明示的な解放が必要で、クラッシュ後の自動引継ぎや強制解除はありません。独立スパイクの評価は [quorum-evaluation.md](quorum-evaluation.md) 参照。実分断・quorum喪失下での二重Active防止を本番環境で保証できません。
- Gateway並行切替には状態機械に加え、明示的な試験フラグと模擬Discordを使う botd → Gateway → Core のローカルプロセス切替経路があります。実Discord上の新旧WebSocket接続・Shadow・Active移譲・旧REST結果保持、本番 lease との統合は未検証です。この試験経路は production_ready ではありません。

## Worker更新・配送・外部副作用

- `manage` の更新は Gateway が所有する Worker プロセスの更新です。Gateway 自体の更新や、daemonからのShard単位ローリング更新ではありません。対応 capability を交渉した Worker は旧世代の処理・応答を保持しつつ新世代へ配送します。旧版との互換経路は直列切替です。任意のアプリ状態や Future は移行せず、snapshot 対象も限定されます。複数Workerの途中失敗では新旧混在があり、全体の自動rollbackはありません。
- 結果不明の外部操作は自動再送しません。旧世代を保持して更新を失敗または未完了とする場合があり、結果不明の追跡は親の再起動で失われます。照合APIと読み取り用管理操作はありますが、永続監査や解除CLIはありません。reload の確認timeout後に処理が進む可能性もあります。
- IPC の連番・ACK・再配送は対応 capability を交渉した限定経路です。全Dispatchの永続配送、セッション横断の欠落検出・重複排除はありません。過負荷ではメッセージ破棄やInteractionのACK失敗が起こり得ます。
- 実DiscordでのInteraction期限、429/5xx、接続断・取消競合、旧Coreへの実REST結果帰属は未検証です。外部副作用にはアプリ側の冪等性と照合が必要です。exactly-once、完全無欠落、グローバル順序、障害を含む無停止は保証しません。

## API・状態・IPC

- フリー関数の型付きprefix/Slashとoption schemaはWorker経路に接続されています。Cogの型付きメソッドやライフサイクルはframework試験段階で、実Workerの全ライフサイクルには未接続です。引数型・引用・権限・クールダウン等には制限があります。
- Button/Select/Modalの名前空間付きルーティング、重複検出、waitのキャンセル規約はframework側にあります。`FrameworkStateStore` は framework の状態操作を `StateStore` に接続しますが、Workerのコンポーネントとローカルファイルを使う試験がありますが、実Discord配送統合や複数ホストでの耐久性は未保証です。汎用イベントやContextの全設計APIも未完成です。
- StateStoreはTTL・容量・CAS・スキーマ等を持つ非同期APIとメモリ・ファイル永続化実装があります。StateMap、View/Modal状態、チェックポイント等がプロセスやホスト障害を越えて保持される保証はありません。
- TaskManager/Supervisorは子タスク追跡、取消、再起動予算、drain等の基盤があり、Workerの一部に接続されています。Cog、IPC、全資源を含む完全なdrainや障害復旧は未検証です。
- 実Worker IPCはstdin/stdoutの改行JSON、フレーム上限1 MiBです。MessagePack、Unix socket/Named Pipe、制御・データ経路分離、snapshot分割は採用していません。Helloのidentityとcapability交渉はありますが、型の存在だけで全経路の認証・fencingを保証しません。[ipc-protocol.md](ipc-protocol.md) 参照。

## 管理・セキュリティ・観測性

- daemon 更新の capability は匿名stdinの信頼済み起動設定で Principal・role・任意の Shard scope に結び付けます。従来の `secret` のみの起動設定はローカル管理者権限を持ちます。接続クライアントからの role/subject/scope は受理しません。範囲限定capabilityの集約閲覧は制限されます。部分Shardだけを許可した場合、Bot全体の inspect が拒否されCLI事前確認を通せない場合があり、範囲内の型付き直接要求とは区別してください。
- 更新監査は最初の更新要求時に runtime-root の `update-audit.jsonl` を開き、開始と完了（または認可拒否）を記録します。開始監査失敗時は実行せず、完了監査失敗時も成功を返しません。操作IDに対応する内容と終端結果は daemon 生存中だけ保持します。異なる内容での再利用や既存の起動停止操作IDとの衝突は拒否しますが、daemon再起動後の永続的な冪等性や実行結果の自動照合はありません。secret、要求本文、成果物パスは監査へ保存しません。
- 監査とハッシュ処理は daemon の監視ループ外で実行します。監査の単一writerが更新完了まで順に処理するため、Identify予算待ちの操作は他Botの更新・拒否監査を待たせます。statusと監視は継続しますが、更新取消は未実装です。模擬Gateway actor内の再ハッシュ待ちは5秒で打ち切り、遅れて完了しても起動しません。ただし同Shardのactor進行はハッシュ・Ready・drainの各待機中に保留されます。別タスクの制御heartbeatや他Shardの進行と区別してください。
- 成果物のハッシュ照合は起動用ファイルハンドルの固定ではありません。照合から起動までのファイル差替えを完全には防がないため、成果物と親ディレクトリは信頼できる管理者だけが変更できる前提です。Windows ACL はサポート外（Windows非対応）。署名・Linux本番更新・実Discord（ユーザー検証範囲）・永続Identify予算・分散quorum/leaseによる更新は未検証です。
- `botd` と `manage` はローカルloopbackの認証付き管理経路です。RBAC・監査・metricsの部品と管理経路の試験があり、manage はローカル監査ファイルを使用します。遠隔管理サービス、全経路の永続監査、サービス登録や本番向け認証情報運用は未完成です。mTLS Transportは試験用daemon間通信とRaft RPCに接続されていますが、本番quorumは起動できません。証明書失効・leaf切替、永続ポリシーとCAチェーンの部品はありますが、本番運用への自動統合は未保証です。既に受理されたメッセージは撤回できません。
- Workerとビルド子へのBot token継承を防ぐ処理、秘密マスク、フレーム上限等の試験があります。全ログ・子プログラム・ファイル権限を通した秘密保護は包括的に検証されていません。`fs_perms` は開いたハンドルの Unix mode をファイル0600・ディレクトリ0700に設定・確認しますが、所有者、ACL、親ディレクトリ等を検証しません。Windowsのファイル用パスAPIコードは残りますが、Windowsはサポート外（Windows ACL、ハンドルAPI、ディレクトリ保護は未解決課題ではなくサポート外）です。成果物の署名・検証も未完成です。[security.md](security.md) 参照。
- Prometheus形式metricsとmetrics_feedはありますが、feedはdaemonに未接続です。分散trace、完全な健全性診断、全経路の観測性は未完成です。statusはDiscord接続や所有権の証明ではありません。registerは1件upsertとdry-runで、自動差分同期はなく、HTTP失敗やtimeoutは結果不明になり得ます。

## 検証の範囲

過去の参考記録: 今回の更新制御は Windows / Rust 1.90.0 で関連235件（対象別の再実行重複を除く）が成功しました（Windowsはサポート外であり、開発機での動作は保証しない。Linuxが対象）。対象は lib、botd、botctl_update、botctl_update_control、botctl_update_daemon、botctl_daemon、update_control_rbac、update_rollback、e2e_gateway_process_switch、gateway_admission_process、e2e_shard_runtime、e2e_shard_worker です。`cargo --offline fmt --all --check` と `cargo clippy --offline --lib --bin botd --bin botctl --tests -- -D warnings` も成功しました。実プロセス試験は模擬Discordのみで、更新中のstatus応答・別Bot継続を確認したものであり性能ベンチマークではありません。workspace全体、Linux、実Discord（ユーザー検証範囲）、本番更新は今回未検証です。

[progress.md](progress.md) に記録されている Windows / Rust 1.99.0 での過去のworkspace検証は『過去の参考記録』です。統合直後にT04が一度失敗し、再実行で成功、fixture の成果物選択も修正されたと記録されています。これらは追加後の現コードの再検証結果ではなく、安定性は引き続き要確認です。現統合版の本番Linux、実Discord（ユーザー検証範囲）、複数ホスト・実ネットワーク、OS suspendや本番合意の検証は確認されていません。

[benchmarks.md](benchmarks.md) はWindows開発機の過去の参考記録とユーザー提供のLinux共有CI記録（Actions ubuntu-latest、IPC往復 p99 = 115.5/258.1/973.6 µs、failover検知 Suspected p50 1503.6 ms / FailoverPending p50 3003.1 ms、共有ランナーで本番保証ではない、往復値で片道p99の直接測定ではない）を含みます。実Discord検証はユーザーが実施する範囲です。Linux本番性能、負荷時Heartbeat、RSS/Shard、1/4/16/64 Shard、更新・Failover遅延は未検証です。個々の測定を仕様全体の性能目標達成の証拠にはしません。

## 実プロセス統合試験の不安定さ（解決済み、2026-11-14）
- 実プロセス（botd、Gateway、Worker、模擬 Discord）を複数起動する統合試験は、GitHub Actions（Ubuntu、4 vCPU）で確率的に失敗していた（`e2e_gateway_process_switch` ほか。20 回の反復で 7 回失敗）。
- 原因：mock の Identify 予約器（`MockIdentifyScheduler::reserve`）が、各 Shard が『ロック取得前に採った時刻』を受け取り、他 Shard の予約より前の時刻だと『時刻の逆行』としてエラーを返していた。同じ Bot の複数 Shard が同時に起動すると、負けた Shard が `IdentifyUnavailable` として永久に `Degraded` になり、Gateway が接続してこなかった。
- 修正：時刻を単調に丸める（`max(now, last_now)`）。再現の単体試験を追加（`tests/gateway_switch.rs`）。修正後、Ubuntu で 20 回の反復がすべて成功した。
- 注意：この競合は mock の予約器の問題だが、実運用の Identify 調整（共有予算）を実装する際も、時刻のサンプリングと直列化の順序に同じ落とし穴がある。判定の時刻は、直列化の内側で採ること。
- **未解決（低頻度）**：`raft_botd_processes` の `raft_botd_death_persistent_rejoin_and_automatic_failback` が、強制終了した botd の再起動（rejoin）で、`Raft cluster configuration, storage or mTLS invalid` により起動に失敗したことがある（Ubuntu の CI で 40 回の反復中 1 回。その後の 20 回は全件成功）。当時は失敗の種別が 1 つの文言に丸められていたため原因は不明。診断のため、種別ごとの固定文言（秘密なし）を出すよう変更済み。次に再現したら、その文言で原因（InvalidConfig、Transport、Coordinator など）を特定する。この経路は試験用（模擬 Discord 限定、本番では起動拒否）。

## 実 Discord 経路の事前レビュー（2026-11-14、静的レビューのみ。実接続は未実施）
実接続（`--mock-discord` なし）のコードを、実行せずに読んで確認した結果。アカウント停止・BAN・token 漏洩につながる致命的な問題は見つからなかった（token は環境変数のみから読み、`Secret` で伏せ、Core/Worker には渡らない。認証失敗で反復ログインしない。429 は twilight-http が遵守、5xx で非冪等 POST を再送しない。`register` は既定 dry-run で、1 コマンドの upsert のみ）。以下は、実機で確認すべき点と、未対応の項目。

- **対応済み**：致命的な切断（4004、4010〜4014）で理由を出さず終了していた → close code（数値のみ）と、再接続しない旨を標準エラーに出すようにした（`main.rs`。終了コードは変えない＝botd の再起動ループを避けるため）。初回は Gateway を単体で起動して出力を見る手順を `real-discord-verification.md` に追加（H1・H2 対策）。
- **高（手順で回避。コードは未修正）**
  - H1（実装済み、CI 検証待ち）：botd は Gateway 子の stdout/stderr を Shard 状態ディレクトリ直下の `gateway.log`（0600。シンボリックリンクなら null へフォールバック。起動時に 1 MiB 超で `gateway.log.1` へ 1 世代ローテーション。世代ごとに区切り行）へ追記する。journald には出ない。ローカル未ビルドで、実環境での確認は未実施。
  - H3：Gateway は `GUILD_MESSAGES | MESSAGE_CONTENT` を固定で要求する。MESSAGE_CONTENT は特権 Intent で、Developer Portal で有効化しないと 4014 で拒否される想定（実挙動は未検証）。Slash だけなら Intent 0 でも動作するはずだが、Intent を設定可能にする変更は未実施。
- **中（未対応）**
  - M1（**実装済み・CI 検証待ち**）：Interaction は先に Defer の ACK を送り、ACK 確認後に `begin_response_delivery` を行う順序へ変更（`main.rs`。`interaction_flow::run` の状態機械と `Outcome` は無変更）。begin は 10 秒の期限付きで、失敗・期限切れ時は固定文言で `update_response` する。ACK 失敗時は begin しないため Worker への副作用もない。ローカル未ビルド。
  - M2（**実装済み・CI 検証待ち**）：同時 64 件超、未リンク／非 ChatInput のコマンド、channel なしは、ephemeral の短い応答（interaction callback、1.5 秒の期限、失敗しても panic しない）を返す。過負荷時も新しいタスクは増やさず、イベントループ内で期限付きに送る（最悪でも 1 件 1.5 秒の背圧）。**未対応のオプション型（`slash_input` の `_`）は未区別**で、「このコマンドは使えません」の応答になる。ローカル未ビルド。
  - M3（**実装済み・CI 検証待ち**）：純関数 `clamp_reply`（2000 文字超は 1997 文字＋`…`、空白のみ・空は固定文言）を prefix 応答・`!workers`・`update_response` の全送信で通す。単体試験あり（境界 2000/2001、マルチバイト、空）。Discord の実際の文字数判定との差は未検証。ローカル未ビルド。
  - M4：401 とネットワーク失敗が区別されず、無効 token でも supervisor の再起動（有界、最大 8 回、バックオフ 1〜30 秒）のたびに `/gateway/bot` を叩き直す。401 は再起動対象外にする専用の終了コードが望ましい。
  - M5（**実装済み・CI 検証待ち**）：Bot 発の全送信（prefix の `create_message`、`!workers`、`update_response`、ephemeral 応答）に `AllowedMentions::default()` を明示（メンション解析なし）。follow-up は現状使っていない。実 Discord での挙動は未検証。ローカル未ビルド。
  - M6：token の環境変数名が不一致（Gateway と `register` は `DISCORD_TOKEN`、`discord_smoke` の既定は `DISCORD_TEST_BOT_TOKEN`）。手順書に明記した。名前の統一は未実施。
- **低**：再接続バックオフにジッターなし（上限約 255 秒、回数の上限なし）。Identify 枠が尽きていると起動後に最大 `reset_after`（最長 24 時間）黙って待つ。単体起動は SIGTERM 未処理。DM の prefix コマンドは動かない（DIRECT_MESSAGES Intent なし）。`register` の説明文が固定。Gateway 子の環境に token があり、`/proc/PID/environ` から同一ユーザーまたは root が読める。HTTP プロキシ非対応（twilight 側）。
- **未実装（再掲）**：プロセス再起動をまたぐ Resume（session はメモリのみ。再起動は毎回新規 Identify）、複数プロセス・複数ホスト間の Identify 調整（プロセス内のみ）。
