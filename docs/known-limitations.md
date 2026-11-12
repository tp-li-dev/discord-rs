# 既知の制約・未実装・未保証事項

2026-11-12時点の実装と [progress.md](progress.md) に基づきます。[architecture.md](architecture.md) §16.2 の全条件は未達です。個々の条件と試験の根拠は [conformance.md](conformance.md) を参照してください。模擬試験や独立した部品の成功は、実Discord・実ネットワークでの保証ではありません。

## 実行構成・配置・分散制御

- `botd` は設定された Bot/Shard ごとに Gateway 子プロセスを起動・監視し、各 Gateway は Worker プールを所有します。明示的な模擬Discordモードでは複数Bot・Shardの別プロセス起動と隔離を試験しています。実Discordでの複数Shard稼働は未検証です。プロセスが存在することは Discord Ready、所有権、lease の健全性を意味しません。
- `botctl` はローカル daemon の ps/status/inspect/shards/start/stop/restart/operation とオフライン設定検証に対応します。update/rollback は既定で成果物をハッシュ確認するオフライン計画、`--yes --bootstrap-stdin` で認証付き要求と操作IDによる再照会を行います。実行成功に対応するのは明示的な模擬Discord Gateway-switchモードの Gateway 更新のみです。通常モードと Core/all は必要な制御契約が未接続のため拒否します。logs、nodes、doctor 等の daemon 操作は未実装です。操作履歴は揮発性で、daemon再起動後の子プロセス自動引継ぎや残存予約の自動解除はありません。
- 配置検証・Shard起動仕様・Identify予算・更新ゲートはありますが、複数ホストへの実配置、設定世代同期、プロセス間・ホスト間でのIdentify予算共有は未完成です。実Discordの推奨Shard数や制限への追従を包括的に検証していません。
- ownership、lease、watchdog、failover/failback は状態機械や模擬時計・模擬ネットワークで試験されています。本番のプロセス起動・Discord送信・quorumとの統合は未完成です。3秒検知・30秒安定確認は設定上の目標で、実復旧時間の保証ではありません。epoch の永続化や OS suspend 時の時計安全性も未検証です。
- `cluster.rs` の過半数ACKは試験用シミュレーションであり本番合意ではありません。openraftは独立スパイクで評価した段階でメインworkspaceには未統合です（[quorum-evaluation.md](quorum-evaluation.md)）。実分断・quorum喪失下での二重Active防止を本番環境で保証できません。
- Gateway並行切替は状態機械と結合シミュレーションで検証していますが、実Discord上の新旧WebSocket接続・Shadow・Active移譲・旧REST結果保持は未実装／未検証です。

## Worker更新・配送・外部副作用

- `manage` の更新は Gateway が所有する Worker プロセスの更新です。Gateway 自体の更新や、daemonからのShard単位ローリング更新ではありません。対応 capability を交渉した Worker は旧世代の処理・応答を保持しつつ新世代へ配送します。旧版との互換経路は直列切替です。任意のアプリ状態や Future は移行せず、snapshot 対象も限定されます。複数Workerの途中失敗では新旧混在があり、全体の自動rollbackはありません。
- 結果不明の外部操作は自動再送しません。旧世代を保持して更新を失敗または未完了とする場合があり、結果不明の追跡は親の再起動で失われます。照合APIと読み取り用管理操作はありますが、永続監査や解除CLIはありません。reload の確認timeout後に処理が進む可能性もあります。
- IPC の連番・ACK・再配送は対応 capability を交渉した限定経路です。全Dispatchの永続配送、セッション横断の欠落検出・重複排除はありません。過負荷ではメッセージ破棄やInteractionのACK失敗が起こり得ます。
- 実DiscordでのInteraction期限、429/5xx、接続断・取消競合、旧Coreへの実REST結果帰属は未検証です。外部副作用にはアプリ側の冪等性と照合が必要です。exactly-once、完全無欠落、グローバル順序、障害を含む無停止は保証しません。

## API・状態・IPC

- フリー関数の型付きprefix/Slashとoption schemaはWorker経路に接続されています。Cogの型付きメソッドやライフサイクルはframework試験段階で、実Workerの全ライフサイクルには未接続です。引数型・引用・権限・クールダウン等には制限があります。
- Button/Select/Modalの名前空間付きルーティング、重複検出、waitのキャンセル規約はframework側にあります。実Workerへのコンポーネント接続、永続Subscription配送、実StateStoreアダプタは未実装です。汎用イベントやContextの全設計APIも未完成です。
- StateStoreはTTL・容量・CAS・スキーマ等を持つ非同期APIとメモリ実装です。永続バックエンドはありません。StateMap、View/Modal状態、チェックポイント等がプロセスやホスト障害を越えて保持される保証はありません。
- TaskManager/Supervisorは子タスク追跡、取消、再起動予算、drain等の基盤があり、Workerの一部に接続されています。Cog、IPC、全資源を含む完全なdrainや障害復旧は未検証です。
- 実Worker IPCはstdin/stdoutの改行JSON、フレーム上限1 MiBです。MessagePack、Unix socket/Named Pipe、制御・データ経路分離、snapshot分割は採用していません。Helloのidentityとcapability交渉はありますが、型の存在だけで全経路の認証・fencingを保証しません。[ipc-protocol.md](ipc-protocol.md) 参照。

## 管理・セキュリティ・観測性

- daemon 更新の capability は匿名stdinの信頼済み起動設定で Principal・role・任意の Shard scope に結び付けます。従来の `secret` のみの起動設定はローカル管理者権限を持ちます。接続クライアントからの role/subject/scope は受理しません。範囲限定capabilityの集約閲覧は制限されます。部分Shardだけを許可した場合、Bot全体の inspect が拒否されCLI事前確認を通せない場合があり、範囲内の型付き直接要求とは区別してください。
- 更新監査は最初の更新要求時に runtime-root の `update-audit.jsonl` を開き、開始と完了（または認可拒否）を記録します。開始監査失敗時は実行せず、完了監査失敗時も成功を返しません。操作IDに対応する内容と終端結果は daemon 生存中だけ保持します。異なる内容での再利用や既存の起動停止操作IDとの衝突は拒否しますが、daemon再起動後の永続的な冪等性や実行結果の自動照合はありません。secret、要求本文、成果物パスは監査へ保存しません。
- 監査とハッシュ処理は daemon の監視ループ外で実行します。監査の単一writerが更新完了まで順に処理するため、Identify予算待ちの操作は他Botの更新・拒否監査を待たせます。statusと監視は継続しますが、更新取消は未実装です。模擬Gateway actor内の再ハッシュ待ちは5秒で打ち切り、遅れて完了しても起動しません。ただし同Shardのactor進行はハッシュ・Ready・drainの各待機中に保留されます。別タスクの制御heartbeatや他Shardの進行と区別してください。
- 成果物のハッシュ照合は起動用ファイルハンドルの固定ではありません。照合から起動までのファイル差替えを完全には防がないため、成果物と親ディレクトリは信頼できる管理者だけが変更できる前提です。Windows ACL・署名・Linux本番更新・実Discord・永続Identify予算・分散quorum/leaseによる更新は未検証です。

- `botd` と `manage` はローカルloopbackの認証付き管理経路です。RBAC・監査・metricsの部品と管理経路の試験がありますが、manage はローカル監査ファイルを使用しますが、遠隔管理サービス、全経路の永続監査、サービス登録や本番向け認証情報運用は未完成です。mTLS Transportは別途実装・loopback試験されていますが daemon間通信や本番quorumへ未接続です。証明書失効・ローテーションも未実装です。
- Workerとビルド子へのBot token継承を防ぐ処理、秘密マスク、フレーム上限等の試験があります。全ログ・子プログラム・ファイル権限を通した秘密保護は包括的に検証されていません。Windows ACL、成果物の署名・検証は未完成です。[security.md](security.md) 参照。
- Prometheus形式metricsはありますが、分散trace、完全な健全性診断、全経路の観測性は未完成です。statusはDiscord接続や所有権の証明ではありません。registerは1件upsertとdry-runで、自動差分同期はなく、HTTP失敗やtimeoutは結果不明になり得ます。

## 検証の範囲

今回の更新制御は Windows / Rust 1.90.0 で関連235件（対象別の再実行重複を除く）が成功しました。対象は lib、botd、botctl_update、botctl_update_control、botctl_update_daemon、botctl_daemon、update_control_rbac、update_rollback、e2e_gateway_process_switch、gateway_admission_process、e2e_shard_runtime、e2e_shard_worker です。`cargo --offline fmt --all --check` と `cargo clippy --offline --lib --bin botd --bin botctl --tests -- -D warnings` も成功しました。実プロセス試験は模擬Discordのみで、更新中のstatus応答・別Bot継続を確認したものであり性能ベンチマークではありません。workspace全体、Linux、実Discord、本番更新は今回未検証です。

[progress.md](progress.md) にWindows / Rust 1.99.0でのworkspace検証を記録しています。直近の統合直後にT04が一度失敗し、再実行では成功したため安定性は要確認です。今回のworkspace試験はT04 fixtureが `tokio::time` 無効の成果物を参照してコンパイルに失敗し、clippyを並行しない再実行でも同じ失敗を再現しました。現統合版のLinux、GitHub Actions、Windows ACL、実Discord、複数ホスト・実ネットワーク、OS suspendや本番合意の検証は確認されていません。

[benchmarks.md](benchmarks.md) はWindows開発機での測定です。Linux本番性能、負荷時Heartbeat、RSS/Shard、1/4/16/64 Shard、更新・Failover遅延は未検証です。個々の測定を仕様全体の性能目標達成の証拠にはしません。
