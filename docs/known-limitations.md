# 既知の制約・未実装・未保証事項

2026-11-12の実コード・テストと [progress.md](progress.md) に基づきます。[architecture.md](architecture.md) §16.2の完了条件は未達です。型・状態機械・起動仕様のテスト成功を、実プロセスや分散環境での保証と混同しないでください。

## 実行構成・配置・分散制御

| 項目 | 現在の実装と残る制約 |
|---|---|
| Shard単位 Gateway/Core | 実行系は単一Bot・単一Shardの親Gateway/RESTと既定2 Worker。Workerはチャンネル分担でありShardではない。`shard_runtime.rs` はShard別起動仕様・パス分離・Gatewayだけへのtoken割当を生成するが、実起動に未接続 |
| 複数Bot daemon / botctl | 未実装。Bot名・ID・短縮ID選択、複数Botの隔離・監視・同時更新は未検証。manageはローカル単一Bot用 |
| 配置・auto shard | TOML検査・起動計画・メタデータ解決の基盤あり。doctorのauto解決は手入力。実配置・移動・設定世代同期には未接続 |
| Identify制御 | 起動前のDiscord情報取得とプロセス内キューを接続。実バイナリは推奨Shard数が1以外なら拒否。再起動・別プロセス・別ホスト間の開始予算共有はない |
| quorum / lease / fencing | StandaloneCoordinatorはメモリ内epoch、期限・watchdogを単体試験。実プロセスの受付・停止・RESTには未接続。再起動を跨ぐepoch永続化なし。OS suspend時の時計安全性も保証しない。QuorumCoordinatorは権限付与を拒否するだけで、成熟した合意ライブラリの統合は未実装。独自コンセンサスは実装していない |
| Failover / Failback | 注入Clockによる状態機械あり。ネットワーク、実プロセス、所有権には未接続。3秒検知・30秒安定確認は設定/目標であり実復旧時間の保証ではない。分断・quorum喪失・旧Discord接続生存時の二重Active防止は統合未検証 |
| Gateway並行更新 | 新旧WebSocket、Standby/Shadow、Active移譲、旧世代REST帰属、失敗時復帰は未実装。Gateway変更は親の再起動が必要 |

## Worker更新・配送・外部副作用

- Worker単位の受付ゲート・応答待ち・snapshot引き継ぎはあるが、仕様のShard Core世代交代や旧新並行drainを完成したものではない。別Workerは動作できるが、対象Workerの新規処理は待つ。複数Workerの途中失敗は新旧混在となり、全体rollbackはない。[hot_swap_scenarios.rs](../src/tests/hot_swap_scenarios.rs) のT04再現試験は、旧dispatchのread lockが切替を待たせることと、IPC期限5秒が5秒応答待ちをtimeoutさせることを確認する `#[ignore]` 試験。実Workerへのrelayで遅延を注入しており、本物の5秒アプリハンドラー、Shard単位の並行drain、旧CoreへのREST結果帰属は未検証。
- 外部応答timeout・失敗・取消は結果不明になり得る。該当Workerの更新を拒否する追跡と読み取り専用 `manage list-unknown` はある。照合根拠を指定するManager APIはあるが、解除CLI・根拠の検証・永続監査・ページ化は未実装。追跡は再起動で消える。
- メッセージ送信/編集に10秒、Interaction初回ACKに2秒のtimeoutを置くが、実Discordの3秒期限遵守、429/5xx、再接続・遅延・取消競合は未検証。ACK結果不明ならコマンドを実行・再送しない。飽和時にはInteractionをACKできずDiscord側で失敗表示となり得る。
- Gatewayの同時処理上限は64。Worker要求はWorkerごとに直列、待機・処理には期限がある。過負荷のメッセージは破棄され得る。永続スプール、全DispatchのACK/再配送・欠落検出、セッション横断の重複排除は未実装。限定的なat-least-onceも全経路の保証ではない。
- **exactly-once、完全無欠落、グローバル順序、障害を含む無停止は保証しない。** HTTP成功後の応答喪失は、外部副作用を取り消せない。任意の通知・DB更新にはアプリ側の冪等性と照合が必要。
- reloadの確認timeout後も要求が残り、後から更新され得る。Workerクラッシュ時は最後に親が受け取ったsnapshotから復旧を試みるだけで、未確認処理の結果は不明。親/ホスト停止でメモリ状態を失う。上記シナリオ試験で、Ready前クラッシュ時の旧PID・状態維持（T05）、正常Ready後の未知Helloバージョン範囲／必須capability拒否と旧版維持・後続正常更新（T19）、切替後の応答喪失時に失敗要求を再送せず新版を最後の確認済みsnapshotから再起動しstderrに記録すること（T06）をWindowsで確認。旧版への切戻しではない。切替後クラッシュの管理用 `.status.error` 記録はなく、この観測性不足の再現試験も `#[ignore]` で残す（当該ファイル形式は§6の明示要件ではない）。Linux・実Discord・外部副作用・自発クラッシュの常時監視は未検証。

## API・状態・IPC

- フリー関数の型付きprefix/Slash、option schema生成は実Worker経路まで接続。Cogの型付きメソッド・Ready・start/drain/stopはframework試験段階で実Workerライフサイクルには未接続。
- 引数はString/i64/boolと末尾Option。prefixの引用・escapeは非対応。選択肢、範囲、ローカライズ、権限チェック、クールダウン、help、Context Menu、UserId等のDiscord型変換は未実装。
- イベントはReadyのみ、実配送はWorker index 0のみ。MessageCreate等の汎用イベントAPI、Button/View/Modal、永続Subscription、wait_forは未実装。
- Contextは応答バッファ・型付き状態・引数抽出の最小API。設計上のsend/defer/author/guild/cache/tasks APIは未実装。reply成功はREST成功ではない。
- StateMapはWorker内一時状態。更新・再起動でリセットされる。count/jobだけが明示snapshot対象。StateStore、DB永続化、schema移行、永続View、任意Future移行、汎用durable jobは未実装。
- DrainTrackerやprepare/abort/commitの基盤はあるが、汎用Task Manager/Supervisor、子タスク追跡、Cogを含む全資源のdrain完了には統合していない。
- 実Worker IPCはstdin/stdoutの改行JSON、上限1 MiB。MessagePack/length-prefix、Unix socket/Named Pipe、制御/データ分離は未採用。大きな状態は拒否され、snapshot分割・永続化は未実装。
- 双方向Hello・バージョン/capability・identity照合はあるが、Bot/Shard/世代identityはdemo固定値で実配置に未接続。旧v5互換は新しい必須機能の保証ではない。プロトコル型の存在だけで認証・fencing・REST要求/結果ルーティングを完成扱いしない。詳細は [ipc-protocol.md](ipc-protocol.md)。

## 管理・セキュリティ・観測性

- 管理TCPはloopbackと共有secretによるローカル操作。遠隔管理、Daemon間TLS相互認証、RBAC、サービス登録、Daemon再起動後の子プロセス再照合は未実装。未認証の遠隔公開を想定しない。
- 認証済み管理操作とビルドは直列で、更新中にstatusや監視が遅延し得る。statusはDiscord接続・Heartbeat・所有権の健全性を証明しない。doctorも構文・配置検査だけ。
- Workerとビルド子からDISCORD_TOKENを除外するが、秘密マスクのユーティリティは既存全ログ経路に未適用。ログ漏洩がないという包括保証はできない。endpoint、更新指定ファイル、バイナリの権限管理が必要。Windows ACL未検証。署名/チェックサムによる成果物許可、全操作の永続監査は未実装。[security.md](security.md) 参照。
- Bot全体・複数ホストのREST予算調整、包括的権限検証、Allowed Mentionsの安全な既定値、Shard cacheの容量/TTL/整合性、分散trace伝搬・metrics・詳細診断は未完成。
- registerは1件upsertとdry-runのみ。自動差分同期なし。同名同スコープを上書きし、HTTP失敗/timeoutでは結果不明になり得る。自動再送はしない。

## 検証の範囲

Windows / Rust 1.99.0 のworkspace fmt/clippy/test成功はprogressに記録されています。過去のLinux試験記録もありますが、現統合版のLinux再検証、Windows/Ubuntu CI実行結果、Windows ACL、実Discord登録・接続・REST、複数ホスト・分断・強制終了・OS suspendを含む障害検証は未確認です。Windowsでのローカルテスト成功をWindows本番運用の包括保証とはしません。

[benchmarks.md](benchmarks.md) にWindows開発機でのJSON/MessagePack codec単体測定があります。Linux本番性能、実IPCのp95/p99、負荷時Heartbeat、RSS/Shard、1/4/16/64 Shard、更新・Failover遅延は未検証です。codec測定を仕様のp99片道1 ms目標の達成証拠にはしません。T01–T20と§16.2全体の完了を宣言できる段階ではありません。

### 本文書更新時の確認結果

2026-11-12、Windows / Rust 1.99.0で `cargo test --offline --workspace`（compile-fail・実Worker・管理・更新試験を含む）、fmt check、workspace全targetのclippy（警告禁止）が成功しました。掲載RustサンプルのコンパイルとCogサンプル1テスト、doctorの例、registerのgreet dry-run、文書内リンク・差分の検査も成功しました。実Discord操作や新たな性能測定は行っていません。この結果は上記の未実装機能・障害条件の保証を追加しません。
