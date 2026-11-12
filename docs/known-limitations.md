# 既知の制約・未実装・未保証事項

2026-11-12時点の実装と [progress.md](progress.md) に基づきます。[architecture.md](architecture.md) §16.2 の全条件は未達です。個々の条件と試験の根拠は [conformance.md](conformance.md) を参照してください。模擬試験や独立した部品の成功は、実Discord・実ネットワークでの保証ではありません。

## 実行構成・配置・分散制御

- `botd` は設定された Bot/Shard ごとに Gateway 子プロセスを起動・監視し、各 Gateway は Worker プールを所有します。明示的な模擬Discordモードでは複数Bot・Shardの別プロセス起動と隔離を試験しています。実Discordでの複数Shard稼働は未検証です。プロセスが存在することは Discord Ready、所有権、lease の健全性を意味しません。
- `botctl` はローカル daemon の ps/status/inspect/shards/start/stop/restart/operation とオフライン設定検証に対応します。update/rollback は manifest に基づくオフライン計画と成果物検証に対応しますが、実行要求は拒否され daemon に送信されません。logs、nodes、doctor 等の daemon 操作は未実装です。操作履歴は揮発性で、daemon再起動後の子プロセス自動引継ぎや残存予約の自動解除はありません。
- 配置検証・Shard起動仕様・Identify予算・更新ゲートはありますが、複数ホストへの実配置、設定世代同期、プロセス間・ホスト間でのIdentify予算共有は未完成です。実Discordの推奨Shard数や制限への追従を包括的に検証していません。
- ownership、lease、watchdog、failover/failback には状態機械や模擬時計・模擬ネットワークの試験があります。本番のプロセス起動・Discord送信・quorumとの統合は未完成です。3秒検知・30秒安定確認は設定上の目標で、実復旧時間の保証ではありません。実験的 Raft ストアには epoch 永続化がありますが、本番統合や OS suspend 時の時計安全性は未検証です。
- `cluster.rs` の過半数ACKは試験用シミュレーションであり本番合意ではありません。メインworkspaceの `raft_quorum` は OpenRaft 0.9.26 を用いる実験的な固定メンバー構成で、永続予約・epoch CAS とローカル admission gate を備えます。botd、本番 fencing、認証済み Raft RPC には未接続で、本番起動は拒否されます。保持予約は明示的な解放が必要で、クラッシュ後の自動引継ぎや強制解除はありません。独立スパイクの評価は [quorum-evaluation.md](quorum-evaluation.md) 参照。実分断・quorum喪失下での二重Active防止を本番環境で保証できません。
- Gateway並行切替には状態機械に加え、明示的な試験フラグと模擬Discordを使う botd → Gateway → Core のローカルプロセス切替経路があります。実Discord上の新旧WebSocket接続・Shadow・Active移譲・旧REST結果保持、本番 lease との統合は未検証です。この試験経路は production_ready ではありません。

## Worker更新・配送・外部副作用

- `manage` の更新は Gateway が所有する Worker プロセスの更新です。Gateway 自体の更新や、daemonからのShard単位ローリング更新ではありません。対応 capability を交渉した Worker は旧世代の処理・応答を保持しつつ新世代へ配送します。旧版との互換経路は直列切替です。任意のアプリ状態や Future は移行せず、snapshot 対象も限定されます。複数Workerの途中失敗では新旧混在があり、全体の自動rollbackはありません。
- 結果不明の外部操作は自動再送しません。旧世代を保持して更新を失敗または未完了とする場合があり、結果不明の追跡は親の再起動で失われます。照合APIと読み取り用管理操作はありますが、永続監査や解除CLIはありません。reload の確認timeout後に処理が進む可能性もあります。
- IPC の連番・ACK・再配送は対応 capability を交渉した限定経路です。全Dispatchの永続配送、セッション横断の欠落検出・重複排除はありません。過負荷ではメッセージ破棄やInteractionのACK失敗が起こり得ます。
- 実DiscordでのInteraction期限、429/5xx、接続断・取消競合、旧Coreへの実REST結果帰属は未検証です。外部副作用にはアプリ側の冪等性と照合が必要です。exactly-once、完全無欠落、グローバル順序、障害を含む無停止は保証しません。

## API・状態・IPC

- フリー関数の型付きprefix/Slashとoption schemaはWorker経路に接続されています。Cogの型付きメソッドやライフサイクルはframework試験段階で、実Workerの全ライフサイクルには未接続です。引数型・引用・権限・クールダウン等には制限があります。
- Button/Select/Modalの名前空間付きルーティング、重複検出、waitのキャンセル規約はframework側にあります。`FrameworkStateStore` は framework の状態操作を `StateStore` に接続しますが、実Workerへのコンポーネント接続と永続Subscription配送は未実装です。汎用イベントやContextの全設計APIも未完成です。
- StateStoreはTTL・容量・CAS・スキーマ等を持つ非同期APIとメモリ実装です。永続バックエンドはありません。StateMap、View/Modal状態、チェックポイント等がプロセスやホスト障害を越えて保持される保証はありません。
- TaskManager/Supervisorは子タスク追跡、取消、再起動予算、drain等の基盤があり、Workerの一部に接続されています。Cog、IPC、全資源を含む完全なdrainや障害復旧は未検証です。
- 実Worker IPCはstdin/stdoutの改行JSON、フレーム上限1 MiBです。MessagePack、Unix socket/Named Pipe、制御・データ経路分離、snapshot分割は採用していません。Helloのidentityとcapability交渉はありますが、型の存在だけで全経路の認証・fencingを保証しません。[ipc-protocol.md](ipc-protocol.md) 参照。

## 管理・セキュリティ・観測性

- `botd` と `manage` はローカルloopbackの認証付き管理経路です。RBAC・監査・metricsの部品と管理経路の試験があり、manage はローカル監査ファイルを使用します。遠隔管理サービス、全経路の永続監査、サービス登録や本番向け認証情報運用は未完成です。mTLS Transportは別途実装・loopback試験されていますが daemon間通信や本番quorumへ未接続です。証明書失効リストと leaf 証明書の重複期間付きローテーションは信頼されたプロセス内 API として存在しますが、配布・永続化・再起動後のポリシー復元・CA更新・遠隔管理は未実装です。既に受理されたメッセージは撤回できません。
- Workerとビルド子へのBot token継承を防ぐ処理、秘密マスク、フレーム上限等の試験があります。全ログ・子プログラム・ファイル権限を通した秘密保護は包括的に検証されていません。`fs_perms` は開いたハンドルの Unix mode をファイル0600・ディレクトリ0700に設定・確認しますが、所有者、ACL、親ディレクトリ等を検証しません。Windows では Unsupported で、ACL は未対応です。成果物の署名・検証も未完成です。[security.md](security.md) 参照。
- Prometheus形式metricsはありますが、分散trace、完全な健全性診断、全経路の観測性は未完成です。statusはDiscord接続や所有権の証明ではありません。registerは1件upsertとdry-runで、自動差分同期はなく、HTTP失敗やtimeoutは結果不明になり得ます。

## 検証の範囲

[progress.md](progress.md) にWindows / Rust 1.99.0での過去のworkspace検証を記録しています。統合直後にT04が一度失敗し、再実行で成功、fixture の成果物選択も修正されたと記録されています。これらは追加後の現コードの再検証結果ではなく、安定性は引き続き要確認です。現統合版のLinux、GitHub Actions、Windows ACL、実Discord、複数ホスト・実ネットワーク、OS suspendや本番合意の検証は確認されていません。

[benchmarks.md](benchmarks.md) はWindows開発機での測定です。Linux本番性能、負荷時Heartbeat、RSS/Shard、1/4/16/64 Shard、更新・Failover遅延は未検証です。個々の測定を仕様全体の性能目標達成の証拠にはしません。
