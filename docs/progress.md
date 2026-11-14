# 開発計画・引き継ぎ

## 基準と現状
2026-11-11、`tp-li-dev/discord-rs` の main（0843664）を取得。仕様は architecture.md。既存実装は単一 Shard の Gateway、2 Worker の stdin/stdout JSON IPC、管理プロセスと更新 CLI。Worker は token を継承しない。更新は Worker ごとに順次行われ、途中失敗時に新旧が混在する。状態は親プロセスのメモリのみ。仕様の shard 単位 Gateway/Core、複数 Bot、分散制御、属性マクロ、ハイブリッドコマンドは未実装。

## 段階計画
1. 既存動作のテストと IPC・管理経路の境界を整える。フレーム上限は暫定 1 MiB、既存の大きい状態が超過すると拒否されるため、状態の分割や永続化を含めて再設計する。
2. 仕様 §16 の順序で開発者 API・マクロ、Shard 単位の Gateway/Core とバージョン付き IPC、drain と応答確認を構築し、既存機能を移行する。
3. 複数 Bot の Daemon/CLI と認証済み管理を構築する。既存ローカル管理の Windows ACL、要求制限、ログとトークンの取り扱いを検証する。
4. 成熟した consensus 実装を評価し quorum と lease/fencing を実装、分断時は安全に停止する。独自 Raft は実装しない。
5. 仕様 T01–T20、Linux/Windows、実 Discord・複数ホスト障害注入・性能を検証し、環境、失敗数、p95/p99 と未達を記録する。

## 設計判断・未解決
Gateway 並行接続による欠落ゼロや exactly-once は未保証。静的登録方式、IPC codec、consensus ライブラリ、lease の時間安全性は仕様 §17 に従い検証して決定する。既存 Worker は Shard ではなく、仕様の Core の代替として完成扱いしない。実 Discord、Windows、複数ホストの試験環境は未確認。本番デプロイ、破壊的操作、秘密情報の変更には事前確認を要する。

## 今回の変更と検証
Worker の双方向受信に 1 MiB、管理要求に 4095 byte（改行込み）の上限を設け、未終端を拒否する。境界・超過・未終端の単体テストを追加。Linux / Rust 1.99.1 で単体テスト3件、管理統合テスト1件が通過。`cargo fmt` と `cargo clippy --all-targets -- -D warnings` 通過。`cargo test` は単体3件・統合5件すべて通過（更新コマンドの release ビルドを含む）。Windows、Discord、性能、分断試験は未実施。管理クライアント側の応答読み取り制限、IPC のサイズ制限と大規模状態移行の設計は残る。

## 追加計画：依存関係を考慮した並行開発
開発者 API の最小ハイブリッドコマンドと、IPC handshake・drain の基礎を独立して並行実装する。まず模擬入力でコンパイル・ルーティングを検証し、次に実 Gateway/Core の IPC・REST 応答へ接続する。その後に daemon と複数 Bot、Core の世代切替、Gateway 並行接続、quorum/lease と障害試験を順に進める。各段階は小規模な動作テストを通して前進し、実 Discord・Windows・複数ノードでのみ確認できる保証は未検証として残す。開発中の細かい改善は最終監査で調整するが、認証、秘密分離、分断時の安全性は最初から維持する。

## 並行開発の結果
- `framework` と `framework-macros` に属性マクロ・inventory 登録・重複検出・共通ディスパッチを試作。模擬入力で検証済み。Discord、IPC、型付き引数、Cog、イベントには未接続。
- `DrainTracker` は受付停止と子処理を含む完了待ちを提供。既存 Worker・REST 応答確認・IPC 排出には未接続。
- Worker Ready に互換性のある範囲と必須 capability の検査を追加。旧 v5 は許容。旧親は新必須 capability を検証しないため新たな必須機能では protocol を上げる。双方向交渉と Bot/Shard/世代照合は未実装。
- reload ビルド子プロセスから DISCORD_TOKEN を除外。Linux/Windows CI を追加したが Actions 実行結果は未確認。
- Linux / Rust 1.99.1 で `cargo fmt --all --check`、`cargo clippy --workspace --all-targets -- -D warnings`、`cargo test --workspace` 成功。次は試作 API を既存 Worker の prefix 経路へ接続し、slash と REST 応答を同じ Gateway/Core 経路に接続する。compile-fail テストと Discord 実接続、Windows、ベンチマークは未実施。

## 継続開発：prefix 統合と安全なサイズ拒否
- `!ping` を属性マクロで登録し、既存 Worker の IPC 経由で実行。count/job は既存経路を維持。実 Worker のテストを追加。Slash、型付き引数、実 Discord は未接続。
- マクロの誤った関数定義について compile-fail 4例を追加。Gateway/Core の識別子・制御/データ・結果不明の型を追加したが、通信・認証・fencing には未接続。
- レビューでサイズ上限超過による状態巻き戻りを発見。現行ローカル状態を変更前に保持し、フレーム上限超過時は拒否して Worker と既存状態を維持する。実プロセス試験で確認。外部副作用には適用不可。状態コピーと複数回シリアライズのコストは未測定で、分割・永続化・ACK を後続で設計する。
- 管理クライアントの応答にも 64 KiB の上限を追加。管理ループの直列処理による遅延は残る。
- Linux Rust 1.99.1: `cargo fmt --all`、`cargo clippy --workspace --all-targets -- -D warnings`、`cargo test --workspace` 成功。初回テストはサンドボックスの TCP bind 制限で失敗、ネットワーク権限付きで再実行し通過。Windows/Discord/性能は未検証。
- 次は Interaction の受付と初回応答期限・応答帰属を設計し、実 Gateway/Core 経路へ接続する。型だけで認証やリースを実装したと見なさない。

## 2026-11-12：Interaction と配置の基礎
- 引数なしの登録済み ChatInput を Gateway で受け、defer 後、専用 IPC コマンドとして Worker に転送し元 Interaction を編集する試作を追加。旧 prefix と同名でも専用経路で実行し、旧 Worker が capability を広告しなければ送信せず拒否する。spawn 前に同時実行上限を適用し、初回 ACK は2秒、編集は10秒で打ち切る。ACK の結果不明時に処理を実行・再送しない。飽和時は未応答で Discord が失敗表示する。登録は Discord の既存コマンドの上書きを避けるため自動実行しない。実 Discord・HTTP・3秒期限・応答確認は未検証。
- ReadyEvent の属性登録と順序付きディスパッチを試作。Gateway/IPC は未接続。Bot/Shard 配置の重複・範囲・token 環境変数名の検証を追加。実配置・所有権には未接続。
- DrainTracker の受付競合と取消試験を追加。現在の REST 送信は Worker Manager の外にあるため、応答確認前に drain 完了と判定しないよう実切替への接続は保留。切替失敗時の受付再開も要設計。
- Linux Rust 1.99.1 で fmt、workspace clippy（警告禁止）、workspace 全テスト成功。Windows CI は未実行。次は Interaction HTTP の模擬統合テストとコマンド登録の非破壊な運用設計、実経路の REST 応答追跡と drain 統合を進める。

## 継続：応答試験・管理・設定
- Interaction の ACK 成功/失敗/期限と元 Interaction への応答帰属を模擬 Future でテスト。実 HTTP は未検証。Ready イベントを Gateway から Worker IPC に配送（旧 Worker の capability を確認、pool index 0 のみ）。
- 配置 TOML の読込・サイズ制限・構文検査、オフライン doctor を追加。`cargo run --bin doctor -- examples/placement.toml --resolved-shards bot-two=1`。Daemon と Discord は診断しない。
- 未認証の管理接続を有限の並行読取へ分離し、アイドル接続中の監視・status 継続を実プロセスで検証。認証済みの操作と更新ビルドは依然直列。
- 応答 drain 基盤は取消・timeout の結果不明を追跡するが実切替未接続。REST 送信と世代追跡の原子性、失敗時の受付再開、結果不明の照合を設計中。
- Linux fmt/clippy/workspace test は上記の統合時点で成功。その後追加した doctor と型付き StateMap は個別テスト成功、全体再検証が必要。Windows・実 Discord・性能は未検証。

## 継続：状態・応答追跡
- 型付き StateMap を Worker の command/Ready Context に接続。Worker ごとに独立し、更新・再起動で初期化される。永続状態ではない。
- 単一 Slash コマンド登録 CLI は既定 dry-run。`cargo run --bin register -- <application-id> <global|guild-id> ping` で計画を表示し、`--apply` は同名同スコープ1件を上書きする。実登録未実施。
- コマンドの通常エラーが Worker を落とす回帰を修正。現行メモリ状態の復元は外部副作用を取り消さない。
- Worker 更新は進行中の外部応答を待ち、結果不明があれば旧 Worker を維持。実プロセステスト2件通過。通常メッセージ送信にも10秒期限を追加。結果不明の照合・解除と永続化は未実装、解消まで更新を拒否し、再起動で追跡は消える。現状は全 Worker の応答受付が更新中に待機し、新旧並行 drain ではない。
- Linux workspace test/fmt/clippy は応答追跡と型付き状態の統合時点で成功。後続の HTTP timeout 等の変更について再検証する。Windows、実 Discord、複数ホスト、性能は未検証。

## 継続：Worker 単位の受付と Cog
- 更新ゲートを Worker ごとに分離し、別 Worker の処理継続と部分更新を統合試験で確認。途中の結果不明は旧 Worker を維持し、既に切り替えた Worker は新世代のまま。更新 Future の取消時に受付を再開する修正を追加。
- Cog の明示登録・型付き共有・ライフサイクル基礎と、`#[cog] impl` の引数なし command 自動登録を追加。型付き引数、イベントメソッド、Worker ライフサイクル統合は未実装。
- 不明な応答の一覧と、インスタンス ID・Worker 番号・操作 ID・照合根拠を要する明示解除の Manager API を追加。認証済み管理 IPC/CLI、監査の永続化は未接続。再起動で追跡は消える。
- 設定からローカル primary の起動意図を列挙する機能を追加。実プロセス起動、Identify と quorum/lease は未実装。
- 変更前後の Linux workspace fmt/clippy/test は通過。最後に追加した受付再開と照合 API は個別検証済み、全体再検証が必要。

## 継続：レビューと Identify
- 応答結果不明の解除 ID が Worker 間で衝突して誤解除される欠陥を修正し、プロセス内一意 ID と誤 Worker 指定の回帰試験を追加。認証済み管理経路、永続監査、根拠検証は未実装。Ready handler のエラー本文はログへ出さない。
- Cog の停止失敗・状態差替・名前衝突をテスト。prepare/abort/commit の独立した失敗時復旧契約を試作したが Cog や Worker 切替には未接続。
- Gateway メタデータ取得・検証と auto Shard 数解決、単一プロセスで Bot 単位に Twilight Identify キューを共有する基盤を追加。Gateway に未接続。別プロセスや再起動間の制限共有はなく、本番複数ノードの許可機構ではない。配置エラーから未信頼の ID を除き、ログ漏洩を防止。
- Linux fmt/clippy/workspace test は上記の Identify と配置ログ修正以前の状態で通過。これら追加後は個別テスト通過、全体再検証が必要。

## 継続：Gateway 初期化と引数基盤
- 単一 Shard Gateway 起動前に Discord Gateway 情報を取得・検証し、単一プロセス内の Identify キューへ接続。取得失敗は固定エラーで起動停止し、推奨 Shard 数が複数の場合は警告する。実 Discord と再起動間・複数プロセス間の Identify 窓は未検証・未調整。起動時に metadata 取得が必要となる。
- Prefix/Slash 相当の入力から String/i64/bool と末尾 Option を抽出し、フリー関数属性マクロに接続。実 Gateway/Worker、Cog メソッド、Discord 登録 schema は未接続で、型付きハイブリッド完成ではない。
- 認証済みローカル応答照会・解除の設計案を docs/local-response-management.md に追加。実装および永続監査は未完。
- Linux Rust 1.99.1: `cargo fmt --all`、workspace clippy（警告禁止）、workspace test が通過。実 Discord、Windows、複数ホスト・性能試験は未実施。

## main 統合時点
- 型付き Prefix / Slash の実 Worker IPC、Discord option schema 生成、登録要求への schema 接続を追加。実 Discord への登録・接続は未検証。
- 認証付き読み取り専用ローカル制御プロトコルを匿名ストリームで検証。実プロセス配線・bootstrap・監査・CLI は未実装。
- 配置・Identify キューのオフライン結合試験と Shard 実行設計書を追加。複数 Shard の実起動、分散 Identify・quorum/lease は未実装。
- 今後の優先順は実 IPC 管理と応答監査、Shard 単位の Gateway/Core 分離、Identify 制御と所有権、実 Discord・Windows・障害・性能検証。未検証部分を完成扱いしない。

## 2026-11-12：PM 主導の並列開発 第1弾（Codex CLI）
- 4作業を worktree 分離で Codex に実装させ、PM が検証・統合した。Codex のサンドボックスはネットワークと .git 書込みが使えず、cargo 検証とコミットは PM が代行。
- `ownership.rs`：OwnershipCoordinator trait、StandaloneCoordinator、ownership_epoch の単調増加と古い epoch の拒否、注入 Clock による lease/watchdog（失効後 fail closed）、QuorumCoordinator は起動拒否のみ（独自合意は実装しない）。再起動を跨ぐ epoch 永続化と実プロセス停止への接続は未実装。
- `shard_runtime.rs`：配置計画から Shard 単位の Gateway/Core 起動仕様（token は Gateway のみ、IPC/状態/管理パスを Bot/Shard 単位で分離）を生成。実プロセス起動には未接続。
- 認証付きローカル管理：`manage list-unknown`（読み取り専用）を匿名パイプ bootstrap で配線。解除操作・永続監査・ページ化は未実装、Windows ACL は未検証。
- framework Cog：Ready イベント自動登録、型付き引数、既定 drain フック。実 Worker/Gateway 接続は未実施。
- main 統合後、Linux ではなく Windows 上の Rust 1.99.0 で `cargo fmt --check`、workspace clippy（警告禁止）、workspace test が成功。Linux、Windows CI、実 Discord、複数ホスト、性能は未検証。

## 2026-11-12：並列開発 第2弾（Codex CLI）
- `protocol.rs`/`worker.rs`：双方向 Hello・バージョン範囲交渉・Bot/Shard/世代 identity 照合・必須/任意 capability 交渉を追加（T19 相当の統合試験、旧 v5 互換維持）。identity は demo 固定値で、実配置とは未接続。docs/ipc-protocol.md を追加。
- `failover.rs`：§5.4 の状態機械（注入 Clock、500ms/1.5s/3s/30s 設定可、Quorum 不可は Degraded、指数バックオフ）を単体試験10件で検証。ネットワーク・実プロセス・Ownership との接続は未実装。
- `redact.rs`/docs/security.md：秘密マスク、`Secret<T>`、現状の漏洩リスク調査。既存ログ経路への適用は未実施（修正案のみ）。
- `benches/ipc_codec.rs`/docs/benchmarks.md：JSON と MessagePack の codec を Windows 開発機で計測。Linux 本番性能の根拠ではなく、採用判断は保留。
- 統合時に、管理試験が操作IDの固定値（0）に依存し並列試験で不安定になり得る問題を、worker 番号の確認に修正。
- main 統合後、fmt、clippy（警告禁止）、workspace test が Windows / Rust 1.99.0 で成功。Linux、Windows CI、実 Discord、複数ホストは未検証。

## 2026-11-12：並列開発 第3弾（Codex CLI）
- docs：README、developer-guide、operator-guide、known-limitations を実コード基準で整備（掲載サンプルのコンパイル・実行、doctor、register dry-run を確認）。
- `botref.rs`：Bot の名前/完全ID/一意なID接頭辞の解決、名前のスラッグ安全性、名前とIDの曖昧衝突の拒否、token env・状態パス重複検査（純関数。CLI/daemon 未接続）。
- `event_seq.rs`：local_sequence、有限バッファ（件数・バイト上限、優先度別満杯拒否）、ACK 範囲、未 ACK 再配送、受信側の欠落/重複検出。受付 ACK と処理完了を型で区別。実 IPC・切替には未接続。
- Worker identity：`BOT_ID`/`SHARD_ID`/`TOTAL_SHARDS`/`GENERATION_ID` を handshake と Gateway に接続（未指定は従来の 0/0/1）。不一致拒否を統合試験。世代番号の永続化は未実装。
- main 統合後、fmt・clippy（警告禁止）・workspace test が Windows / Rust 1.99.0 で成功。Linux、CI、実 Discord、複数ホストは未検証。
- 次候補：event_seq を実 IPC に接続、failover/ownership の接続、daemon/botctl の骨格、Core 世代切替の統合、secret マスクの既存ログ経路への適用、Linux CI の実行確認。

## 2026-11-12：並列開発 第4弾（Codex CLI）
- event_seq を Worker IPC に接続：`event-seq-v1` capability 交渉時のみ Ready に連番、Worker が受付 ACK を返す。旧 Worker は従来形式。ACK 欠落/不正/timeout を可視化。切替時の未 ACK 再配送は未接続。
- `botctl`（オフライン骨格）：config validate / shards / ps と --json、token は env 名のみ表示。daemon 操作は『未実装』で異常終了。
- failover と ownership を接続：承認を OwnershipCoordinator 経由で取得し epoch を保持、fencing、lease 失効で Active 停止、T11〜T14 相当を模擬 Clock/Fake で試験。実プロセス・実ネットワーク・成熟した合意バックエンドは未接続。
- 秘密マスクを bin/*.rs、placement.rs 等に適用し回帰試験を追加。worker.rs/main.rs 側の未対応箇所は docs/security.md に行・修正案を記録（PM 後続対応）。
- 統合時に clippy（items_after_test_module）を機械修正。main で fmt・clippy・workspace test（157件）が Windows / Rust 1.99.0 で成功。
- 運用メモ：並列 worktree ごとの target が累計約70GBとなりディスク満杯が発生。完了済み worktree の target と古い検証用 target を削除して復旧。今後は統合後に都度削除する。

## 2026-11-12：並列開発 第5弾（Codex CLI、reasoning high）
- `docs/quorum-evaluation.md`/`experiments/quorum-spike`：openraft 0.9.26 を独立スパイクで評価（メモリ内3ノード、リーダー選出・1ノード停止継続・過半数喪失で書込み失敗・分断で少数側が epoch を進められないことを確認、初回ビルド約42秒）。結論は『条件付き継続、本番採用は保留』。Linux、永続化、認証付き通信、lease/watchdog/fencing 統合は未検証。メイン workspace には未組込み。
- `supervisor.rs`：TaskManager/Supervisor（所有者・親子追跡、取消、タイムアウト、再起動予算・バックオフ・サーキットブレーカー、段階 drain）。注入時計で30件。実 Worker/Cog/IPC には未接続。
- `state_store.rs`：非同期 StateStore（TTL・容量・スキーマ互換・CAS）、メモリ実装、MAC 検証付き 100 文字制限 custom_id、View/Modal 状態、drain 対応 wait。非永続。外部 DB・Subscription 配送は未実装。
- `testkit.rs`：障害注入（遅延・切断・途中切断・満杯・帯域・seed）と模擬 REST/Interaction（429/5xx/結果不明）、自己試験31件。実 Worker での T15 統合は未実施。
- `framework/examples`：ping・hybrid・cog_counter・cog_moderation の実行可能サンプルと統合試験17件、CI に examples ビルドを追加（GitHub Actions 実行は未確認）。ban は dry-run、on_message 相当は未対応と明記。
- `benches/ipc_roundtrip.rs`/docs/benchmarks.md：実 Worker IPC 往復 各10,000件・失敗0、p99 は並列度1/4/16 で 126.3/389.5/588.3 µs（Windows 開発機、1ms 目標との比較は Linux で要再測定）。
- main 統合後、fmt・clippy（警告禁止）・workspace test（269件）が Windows / Rust 1.99.0 で成功。

## 2026-11-12：並列開発 第6〜7弾（Codex CLI：Sol/Terra/Astra）
- 第6弾：Core ホットスワップ試験(T04/T05/T06/T19 実 Worker)、REST/Identify 予算(`rest_budget.rs`/`update_gate.rs`)、RBAC・監査(`rbac_audit.rs`)、worker 統合（秘密保護・TaskManager 接続・event_seq 再配送）、`daemon.rs`/`botd`、`gateway_switch.rs`（307状態・34,549遷移で単一 Active 検証）、`metrics.rs`（Prometheus 出力）。
- 統合時に、秘密マスクがエラー診断まで潰して試験が5件失敗。第7弾で、秘密のみマスクし診断（種別・原因・終了コード）を保持する設計に修正。さらに T04（5秒処理中の in-flight 応答、並行 drain）と T06（切替直後クラッシュの管理記録）の本体不備を修正し、#[ignore] を解除。
- 第7弾：`cluster.rs`（Transport/Peer 表/決定的模擬ネットワーク、試験専用の過半数 ACK、T10〜T14、1,555 イベント列と 128 seed×96 障害操作で二重 Active 違反なし。本番合意ではない）、botctl↔botd 接続（ps/status/inspect/shards/start/stop/restart、force は --yes 必須、update/rollback は未実装）、管理経路への RBAC・監査・metrics 適用（T20）、Gateway ローリング結合シミュレーション（T07〜T09）。
- 統合後の試験不安定要因を修正：fixture の rlib 選択が古い rlib で失敗（最新を選ぶよう修正）、log_redaction を RBAC 前提へ更新。
- 現状：main で fmt・clippy（警告禁止）・workspace test が Windows / Rust 1.99.0 で成功（545件、無視1件の手動ベンチ、2回連続）。未検証: Linux、GitHub Actions、実 Discord、複数ホスト・実ネットワーク、Windows ACL、本番合意（openraft は評価のみ）。
- 運用：Codex は Sol(high)/Terra(high)/Astra(max) を難易度で使い分け。

## 2026-11-12：並列開発 第8弾（Astra max / Terra high）
- botd E2E：明示的な模擬 Discord モード（本番既定は無効、token 不要）を追加。botd が Bot A の Shard 0/1 と Bot B の Shard 0 を別プロセスで起動し、Gateway ごとに Worker(Core) が起動、`/ping`・`!ping` 相当が同一ハンドラーを通り REST 結果が要求元 Worker に戻ること、Bot 間の token/状態/IPC 分離、一方の Gateway クラッシュ時に他 Shard 継続と botd 再起動、Shutdown 契約による graceful 停止と孤児ゼロを統合試験（18件）で確認。実 Discord は未接続。
- `tls_transport.rs`：Tokio TCP + rustls の mTLS Transport（証明書によるノード ID 照合、平文・未許可ノード・偽 CA・巨大フレーム・slowloris の拒否、loopback 既定、秘密鍵保護）。実 TCP 上で heartbeat 途絶による failover 判定を再現。証明書失効・ローテーション、本体配線、本番 quorum/lease 統合は未実装。
- framework コンポーネント経路：Button/Select/Modal を `custom_id` で名前空間付きハンドラーへルーティングする属性マクロ（重複検出）、2つの独立 Registry が同一ストアで処理する模擬 T16、`wait_for` の drain 時キャンセル型と永続 Subscription 型の区別。実 StateStore アダプタ・実 Worker 接続・Subscription 配送は未実装。
- 統合後 workspace test は 605 件成功。ただし統合直後の初回実行で hot_swap_scenarios の T04 が1度失敗し、再実行（単体3回・全体1回）では成功。再ビルド直後の負荷に依存するフレークの可能性があり、安定性要再確認。

## 2026-11-12：並列開発 第9弾（最終段）
- 試験安定化：T04 のフレーク原因は、fixture が rlib を更新日時で個別選択し、異なる feature 構成の Tokio を混在させること（`there is no reactor running`）と特定。Cargo の依存解決/JSON 成果物取得へ移行し、条件待ち・ポート競合排除などを実施。空の target で `cargo test --workspace` を10回連続成功（CPU 負荷6プロセス併走9回を含む）。当初の一過性失敗との原因同一性は元ログ未入手のため未確定。
- 複数 botd 分散 E2E：mTLS で接続した3 botd が、試験用過半数 ACK・lease watchdog・failover/failback で T10〜T14（閾値短縮）を実 Gateway プロセス（模擬 Discord）上で検証し、二重 Active 違反なし。本番利用不可（明示フラグと模擬 Discord を必須とする guard）。実 openraft・永続化・実ホスト間・OS suspend は未検証。
- ベンチ追加（Windows 開発機の参考値）：failover 検知（loopback mTLS、50回）Suspected p50/p95 1,504.6/1,513.3 ms、FailoverPending 3,008.7/3,014.7 ms、正常時の誤判定 0/100 観測。event_seq・TaskManager・metrics・StateStore も計測。3秒は検知目標で Discord 再接続を含まない。
- docs：`docs/conformance.md`（§16.2 と T01〜T20 の適合状況表）、README と各ガイド、known-limitations を最新化。
- main 統合後の workspace test は 647 件成功、失敗 0、無視 1（手動ベンチ）を2回連続確認。fmt・clippy（警告禁止）成功。いずれも Windows / Rust 1.99.0。

## 残課題（次段階）
Linux と GitHub Actions での実行確認、実 Discord での接続・登録・Interaction 応答・Identify 検証、実 openraft 組込みと永続ストレージ、Daemon↔Gateway/Core の実本番配線（Worker を Core 代替から Shard 単位 Core へ）、StateStore と framework コンポーネント経路の Worker 接続、update/rollback、証明書失効・ローテーション、Windows ACL、性能の Linux 再測定。

## 引き継ぎ
別環境への移行用に、方針・PM/Codex 並列の進め方・運用上の注意・残課題を [handoff.md](handoff.md) にまとめた（2026-11-12）。

## 2026-11-12：別環境（Rust 1.90.0 / Windows）での再検証と互換修正
- 新環境のツールチェインは Rust 1.90.0。`atomic_try_update`（`AtomicU64::try_update`）が未安定でビルド不可だったため、安定版の `fetch_update` に置換（`framework/examples/counter/mod.rs`、`src/worker.rs`）。
- clippy `nonminimal_bool` 3件（`src/daemon.rs` 2、`src/main.rs` 1）を同値の書き換えで修正。fmt・clippy（警告禁止）は成功。
- `cargo test --workspace --no-fail-fast`：642 成功、5 失敗、1 無視。
  - `compile_fail::command_diagnostics`：trybuild の期待出力（警告文言）が Rust 1.99 前提で、1.90 の出力と不一致。実装の不具合ではなく、スナップショットのツールチェイン依存。1.99 では成功の記録があるため期待値は変更していない。
  - `botctl_daemon` の4件：全体並列実行時のみ失敗。単独実行（同ファイル、2回連続）では 11 件すべて成功。負荷依存のフレークと推定、原因は未特定（要調査）。
- 未検証は従来どおり（Linux、GitHub Actions、実 Discord、複数ホスト）。

## 2026-11-12：並列開発 第10弾（Codex 0.167.2：Astra max / Sol / Terra）
- 運用：Codex CLI を 0.147.0→0.167.2 に更新（6.1 系・astra が旧版で拒否されたため）。`gpt-6.1-*` と `gpt-6-astra` が利用可能。openraft 0.9.26 を PM が `cargo add`（features: serde, storage-v2, generic-snapshot-data）で追加。
- flake：botctl_daemon のタイムアウト試験の固定 1.5 秒競合を条件待ち（試験側が許可するまで Gateway が終了しない）に変更。負荷下で複数回成功。元の4件失敗が同一原因だったかは未特定。
- `fs_perms.rs`：Unix は開いたハンドルで mode 0600/0700 を検査・設定。Windows ACL は未実装（`Unsupported`）。既存の秘密ファイル経路には未接続。
- `tls_transport.rs`：ノードID/シリアル失効、重複期間付きローテーション、原子的交換（試験23件）。CA 更新・CA/中間失効、ポリシー永続化・配布は未実装。
- framework↔StateStore：InteractionContext から MAC 検証付き状態の作成/取得/TTL/CAS/削除、Bot/Shard/名前空間分離、wait と Subscription の区別（試験18件、`framework/STATE_ADAPTER.md`）。trybuild の警告文言依存を除去。Worker 実接続は未実施。
- `update_rollback.rs`/botctl：`update|rollback --plan` は dry-run 既定（SHA-256 検証、Identify予算考慮の Shard 別計画、rollback 計画）。実更新は daemon 未接続で `--yes` でも拒否。
- Gateway/Core 配線：模擬 Discord 限定で認証付き IPC による Gateway 切替（Pause→Activate）、Bot 単位 Identify 予算共有、placement からの1 Shard 1 Core 起動（Core に token 非配布）を実プロセス試験で確認。本番既定は無効。切替後の自動 rollback、子再採用、無欠落は未保証。
- `raft_quorum`：openraft による epoch CAS、永続ストレージ（sync_all、破損検出、ロック）、Ownership trait 接続、メモリ transport（試験35件）。本番起動は明示フラグでも拒否。mTLS 接続・自動引継ぎ・leader 転送は未実装。
- main 統合後（Windows / Rust 1.90.0）：fmt・clippy（警告禁止）成功、`cargo test --workspace --no-fail-fast` 756 成功・失敗0・無視2。Linux、GitHub Actions、実 Discord、複数ホストは未検証。
- 第2波を起動：raftnet（Raft を mTLS 上へ）、wkstate（StateStore の Worker 接続）、updctl（update/rollback の制御経路）、cidocs（CI・Linux 手順・文書）。

## 2026-11-12：並列開発 第11〜13弾（Codex：Astra max / Sol / Terra）と現状
- 統合済み：Raft(mTLS) transport・leader 転送・fencing 付き引継ぎ・自動 failover/failback runtime（`raft_quorum*`、`cluster_runtime.rs`）、証明書の失効/ローテーション/CA 管理/方針永続化（`tls_transport.rs`、`cert_policy.rs`）、永続 Subscription/ジョブ（`subscription.rs`、`durable_jobs.rs`）、framework↔StateStore↔Worker 接続とファイル永続 StateStore、daemon 側 update/rollback（認可・監査・同一操作ID）、子プロセス自動復旧（`child_recovery.rs` を daemon/worker に接続）、ログ経路の秘密保護、メトリクスのフィード API（daemon 未接続）、ファイル権限（`fs_perms.rs`：Unix mode、Windows は icacls /save の SDDL 解析によるパス指定 API）、実 Discord 検証の準備（`discord_smoke`、docs/real-discord-verification.md）。
- CI：GitHub Actions を並列ジョブ化（fmt、clippy、test、examples+doctor、stable 互換）。Linux は全ジョブ成功。Windows は Raft snapshot 試験と ACL 試験がランナー固有でタイミング/環境依存し、修正を統合済み（Actions での再確認は未了）。
- Linux ベンチ（Actions ubuntu-latest、AMD EPYC 7763 4vCPU、共有ランナー）：IPC 往復 各10,000件失敗0、p99 は並列度1/4/16 で 115.5/258.1/973.6 µs、failover 検知 Suspected p50 1503.6 ms / FailoverPending p50 3003.1 ms。往復値であり仕様の片道 p99<1ms の直接測定ではない。本番性能の保証ではない。
- **未達（正直な記録）**：無停止更新の負荷試験（`tests/update_load.rs`、現在 `#[ignore]`、手動実行）は許容基準（欠落0・ACK3秒以内等）を満たさない。連続更新で欠落1、Gateway 切替で欠落4・ACK未着2・2回目の切替失敗、新 Worker クラッシュで ACK 最大4.6秒。修正は実行中（zdtfix）。
- 実行中：zdtfix（上記の修正）、raftwire（Raft runtime の botd 配線。既定無効・模擬 Discord のみ）。完了後は `ci-verify` ブランチで Actions 検証を先行し、main へは確認後に反映する。
- 範囲：実 Discord での検証（接続、Slash 登録、Interaction 3秒、Identify/Resume）はユーザーが実施する。手順は docs/real-discord-verification.md。それより前の手順（模擬環境での実装・試験と CI）を完成の範囲とする。
- 未検証のまま：実 Discord、複数ホストの実障害、OS suspend、本番での Raft 有効化（起動は拒否のまま）、Windows ACL のディレクトリ対応、メトリクスの daemon 接続、永続 Subscription/ジョブの Worker 接続。

## 2026-11-12 終了時点（Codex クオータ切れで中断）
- Codex の利用枠が尽きた（次回利用可能: 2026-11-18 18:17 頃）。以降の Codex 方針: Luna max の高速モードを主、Terra は medium、Astra は設計・最終チェックのみ（コスパ重視）。
- 中断した作業は WIP としてブランチに保全済み（origin に push 済み、いずれも未検証・main 未統合）：
  - `codex/zdtfix`：無停止更新の修正。4ケース中3つ（連続更新、Gateway 切替、旧 Worker クラッシュ）は欠落0・ACK 47ms 以内。残る『ハンドラー実行後・結果返却前のクラッシュ』は仕様上の限界（結果不明）。次は `tests/update_load.rs` を、(1) 厳格な通常テスト、(2) 結果不明の #[ignore] テスト、に分割してから main へ統合する（分割しないと CI が失敗する）。
  - `codex/raftwire`：Raft runtime の botd 配線（既定無効・模擬 Discord のみ）と 3 プロセス E2E。ほぼ実装済みで未検証。
  - `codex/finaldocs`：docs の最終整備（benchmarks, conformance, developer-guide, known-limitations, operator-guide, security を途中まで更新）。事実照合が未了。
- 要調査：Actions の手動 stress（suite=acl、repeat=5）で Ubuntu iteration 1 と Windows iteration 4 が失敗。Windows ACL の CI 差の修正が完全には効いていない可能性がある。ログ確認から始める。main の Windows テストの結果は未確認のまま。
- 再開手順：`gh run list` で Actions を確認 → 上記3ブランチを `ci-verify` 等で Actions 検証 → 問題なければ main へ統合。ローカルの重い全体試験は避け、Actions（ci.yml、stress.yml、bench.yml）で行う。

## 2026-11-13：Windows サポート外、CI の並列化、Codex クオータ切れ後の統合
- 決定：対応 OS は Linux（Ubuntu）のみ。Windows はサポート外（Windows 固有コードは残すが保証・CI 対象外）。AGENTS.md と docs/architecture.md を更新。
- CI：`ci.yml` を Ubuntu のみの並列構成に再編（fmt、clippy、test の 5 シャード、examples+doctor、集約の ci-ok）。`.github/scripts/test-shard.sh` が実測時間で試験を分割（新規テストは rest に自動で入る）。dev プロファイルの debug 情報を縮小。stable は週次/手動の `stable.yml` へ、`stress.yml` は Ubuntu のみ（`switch` スイートを追加）。結果：全ジョブ成功、最長経路 約 280 秒（以前は直列で約 11 分）。
- 試験の安定化：Raft の lease 試験とプロセス/ネットワーク試験の待ち時間上限を CI 向けに緩和。実プロセス統合試験（process シャード）は、バイナリごとに直列実行にして安定。並列実行で不安定になる根本原因は未特定（known-limitations.md に記録）。
- 統合：zdtfix（無停止更新：連続更新・Gateway 切替・旧 Worker クラッシュは基準を満たす。『ハンドラー実行後・結果返却前のクラッシュ』は仕様上の限界＝結果不明として `#[ignore]` の別試験に分離）、raftwire（Raft runtime を botd に配線、既定無効・模擬 Discord 限定）、メトリクスの daemon 接続（Identify 待ち・drain 時間・世代・欠落数のみ。failover/IPC往復/REST予算の送信元は未接続）、docs の最終整備。
- ツール：Codex は利用枠切れ（2026-11-18 18:17 頃まで）。Antigravity CLI（agy、ファイル編集のみ）と Claude Code のサブエージェント（モデル指定なし）で代替。Haiku は環境設定のため使えなかった（要 Claude Code 再起動後の再確認）。
- 未検証・未完了：実 Discord（ユーザー実施）、複数ホストの実障害、OS suspend、本番での Raft 有効化（起動拒否のまま）、メトリクスの残りの送信元の接続、永続 Subscription/ジョブの Worker 接続、Core 単独更新・更新失敗時の自動 rollback、process シャードの並列不安定の根本原因。

## 2026-11-14：不安定な統合試験の根本原因の解決と、Linux 常駐運用の整備
- **根本原因の解決（本番相当の競合）**：実プロセス統合試験が CI（Ubuntu、4 vCPU）で約 35% 失敗していた（20 回中 7 回）。診断（時間切れ時に botd の状態を出す）で、同じ Bot の複数 Shard が同時に起動すると、片方が `Degraded: IdentifyUnavailable` になり永久に停止することが判明。原因は `MockIdentifyScheduler::reserve` が、ロック取得前に採られた時刻を『逆行』としてエラーにしていたこと。時刻を単調に丸めて修正（`src/src/gateway_switch.rs`）、単体試験を追加。修正後 20/20 成功。実運用の共有 Identify 調整でも、時刻のサンプリングと直列化の順序に同じ落とし穴がある。
- 別の不安定さ（ETXTBSY）：試験が実行ファイルを `fs::copy` した直後に起動すると、同一プロセス内の他スレッドの fork が書き込み fd を保持している間に `Text file busy` で失敗し得る。`testkit::copy_executable`（ハードリンク、不可ならコピー）に置き換え（5 か所）。
- CI：`process` シャードを、直列・再実行の暫定対応から、並列・再実行なしに戻した（根拠：反復 stress で 20/20 と 20/20）。stress に `switch`/`process` スイートを追加。
- Linux 常駐運用：`deploy/systemd/botd.service`、`deploy/botd.env.example`、`docs/linux-deployment.md` を追加。botd に systemd `sd_notify`（READY/WATCHDOG/STOPPING、既定経路のみ）を実装（実 systemd では未検証のため unit では無効）。botd と botctl に `--secret-file`（権限 0600/0400、所有者、リンク拒否の検査）と `botctl secret generate` を追加し、systemd 常駐の botd を botctl で操作できるようにした（以前は secret を渡す手段がなく操作不能だった）。`--control-port` を追加。いずれも Ubuntu の CI（試験 5 シャード、clippy、fmt）で成功。実 Ubuntu/systemd の実機は未検証。
- 運用：Haiku は Claude Code の再起動後に使えるようになった（軽い読み取り作業向け。余計なツールを呼ぶことがあるので範囲を絞る）。サブエージェントのローカル cargo は PC 負荷のため禁止し、検証は Actions で行う。
- 未検証・未完了：実 Discord（ユーザー実施）、実 systemd/Ubuntu、複数ホストの実障害、本番での Raft 有効化（起動拒否のまま）、メトリクスの残りの送信元（failover、IPC 往復、REST 予算）、永続 Subscription/ジョブの Worker 接続、Core 単独更新・更新失敗時の自動 rollback、`raft_botd_processes` の rejoin の稀な失敗（原因未特定、診断を強化済み）。

## 2026-11-14（続き）：実 systemd での検証
- `.github/workflows/systemd-smoke.yml`（`deploy/**` や botd/botctl の変更で自動実行、手動実行も可）で、GitHub のランナー（Ubuntu）の実 systemd 上に botd を導入して検証。確認できたこと：unit の構文（`systemd-analyze verify`）、`Type=notify` で `READY=1` が届き起動が完了すること、`WatchdogSec=6s` の周期を超えて再起動 0 で動き続けること、`--secret-file` + botctl の `status` が通り別 secret は拒否されること、`systemctl stop` で正常終了（Result=success）すること、Gateway と Core ワーカー 2 つが systemd 配下で起動し稼働すること（模擬 Discord、外部への接続なし）。
- unit の硬化を追加し（`SystemCallFilter=@system-service`、`~@privileged @resources`、`ProtectProc=invisible`、`ProcSubset=pid`、`RemoveIPC`、`ProtectClock`、`ProtectKernelLogs`、`ProtectHostname`、`RestrictRealtime`、`SystemCallArchitectures=native`）、硬化なしのベースラインと並べて、どちらも Gateway と Core が起動・稼働することを確認。
- 発見（運用上の落とし穴）：同梱の Gateway（`src`）は `--bot-id` が正規の 10 進 u64 でないと `launch_invalid` で即終了し、botd は子が起動のたびに落ちて再起動を繰り返す。botd は事前に検出しない（`--gateway-program` は差し替え可能で、試験用フィクスチャは文字列 ID を使うため。一律の事前検証は試験を壊すので取り消した）。docs/linux-deployment.md に症状と確認方法を明記。子の標準エラーは botd が破棄するため journald に出ない（既知の制約）。
- smoke の初期版は `--no-autostart` で子を起動しておらず、Gateway の失敗を見逃していた。判定を『Running・再起動 0・Core ワーカー 2』に直して、子の起動まで検証する形にした。
- 未検証：実 Discord、実機の systemd（ランナー外）、systemd 配下で Type=notify を既定にすること（unit では無効のまま。有効化は上記 smoke の drop-in で確認済みだが、出荷する unit の既定は Type=simple）。

## 2026-11-14（続き）：実 Discord での初回検証（ユーザー実施、成功）
- 環境：Oracle Cloud の Ubuntu、ユーザー自身のテスト用 Bot とテストギルド。手順は docs/real-discord-verification.md の「最初の 30 分」。
- 結果：(1) `botctl config validate` が成功。(2) `discord_smoke --apply`（読み取り専用）で `gateway_bot`・`current_user` がともに成功し、レート制限ヘッダも正常。(3) `register`（ギルド限定、dry-run→`--apply`）で `ping` を登録。(4) Gateway を単体起動して `ログイン成功`、`/ping` と `!ping` の両方で `Pong! 🦀` が返った（Message Content Intent を有効化済み）。(5) botd 経由で起動し、`botctl status` で `Running`・再起動 0、`botctl stop` が 50ms で成功（`Exited` code 0、`restarts` 0、`startup_failures` 0）、Gateway 子プロセスが残らないことを確認。
- 実機で分かったこと：開発 PC のフォルダをコピーすると、`.gitignore` 対象の `worker-path.txt`（Windows のパス）と `workers/`（Windows の実行ファイル）が入り、Linux で `Workerのパスは絶対パスにしてください` が 2 秒ごとに出る（無害）。サーバには `git clone` で取得するか、これらを除く。`bots.toml` の `bot_id` は、botd 内の識別番号（数字）で、Discord の Bot の ID とは別（実際の ID は token から決まる）。
- 検証していないこと：応答時間の計測値、長時間稼働、Resume、複数 Shard、レート制限が実際に出る場面、systemd 常駐での実 Discord 接続。


## 2026-11-14（続き）：実機検証を受けた改善（CI 検証済み、実 Discord では未確認）
- botd：Gateway 子の stdout/stderr を、Shard ごとの状態ディレクトリの `gateway.log`（0600、1 MiB 超で `gateway.log.1` へ 1 世代ローテーション、リンクや通常ファイル以外は従来どおり null）に残す。botd 経由でも、接続失敗の理由（close code など）が見える。
- Gateway の応答経路（`main.rs` の live 経路）：全送信で `allowed_mentions` を無効化（`@everyone` の防止）、返信を 2000 文字にクランプ（`clamp_reply`、空は固定文言）、同時 64 件超・未リンクのコマンド・channel なしの場合に ephemeral の短い応答を返す、Interaction の ACK（defer）を先に送ってから応答の配信開始（`begin_response_delivery`）を待つ順序に変更（M1）。
- 検証：CI（Ubuntu、clippy・試験 5 シャード・fmt）は成功。ただし統合試験は live 経路（`main.rs`）を通らないため、**実 Discord での再確認が必要**（特に M1 の順序変更）。再確認の手順は docs/real-discord-verification.md の 3.5・4・5。

## 2026-11-14（続き）：改善版の実機再確認（ユーザー実施、成功）
- 同じサーバ（Oracle Cloud の Ubuntu）、同じテスト用 Bot で、ソースのアーカイブ（`git archive`、約 1MB）を展開してビルド。(1) Gateway を単体起動して `ログイン成功`、`/ping` と `!ping` が返る、`Workerのパス…` の警告は出ない（Windows 由来のファイルを含まないため）。(2) botd 経由で起動し、`state: Running`、`restarts: 0`、`startup_failures: 0`。`gateway.log`（0600、`--- gateway generation 1 ---` と `ログイン成功` を含む）が Shard ディレクトリにできた。`/ping` と `!ping` が返った。
- 実機で出た問題と原因：(a) `bots.toml` を移動したため `cannot load daemon configuration`（ファイルのパス違い）。(b) botd を起動した端末に、`token_env` の名前（`DISCORD_TEST_BOT_TOKEN`）で token を設定していなかったため `MissingCredential`・`state: Failed`・`restart_exhausted: true`（`gateway.log` は子の起動前の失敗のため作られない）。手順書の例の名前が実際の `token_env` と食い違っていたことが混乱の元だったので、手順書に明記した。
- botd の停止順序：botd だけを `Ctrl+C` で止めると、Gateway とワーカーが残り（既知の設計）、起動予約ファイルも残る。`botctl stop` を先に実行する順序を手順書に明記し、残った場合の手順（kill と予約ファイルの削除）を追記した。
- 検証していないこと：応答時間の計測値、長時間稼働、Resume、複数 Shard、systemd 常駐での実 Discord 接続、レート制限が実際に出る場面。
- 改善候補（未着手）：botd が終了時に、自分が起動した子を、確認つきで停止する（または `botctl shutdown` のような一括停止）。現状は、手動運用で停止順序を守る必要がある。


## ロードマップ：Gateway の無停止更新（ローリング）の本番化（未着手、2026-11-14 に合意）
- 現状：模擬 Discord の試験モードのみ（状態機械、旧 Gateway の drain、単一 Active、共有 Identify 予算を実プロセス試験で確認）。通常の daemon では実更新を安全に拒否する。Core（Worker）のホットスワップは標準経路で使える。
- 本番化に必要：(1) 旧 Gateway から新 Gateway への Discord セッション（session_id、シーケンス番号、resume_gateway_url）の保護された引き継ぎと、新 Gateway の Resume（新規 Identify を消費しない）。(2) 複数プロセス間の Identify 調整（現状はプロセス内のみ）。(3) 実 Discord での検証（Resume の成否、切替中のイベント欠落、Interaction の 3 秒）。
- 進め方：設計は Astra（max）、実装は Luna（max、高速）、検証は stress ワークフローと実機。Codex の利用再開（2026-11-18 18:17 頃）後に着手する。

## 2026-11-14（続き）：手元の WSL2（Ubuntu 24.04、systemd 動作）での実 systemd 確認
- `deploy/dev/wsl-systemd-smoke.sh` を追加（Actions の `systemd-smoke.yml` と同じ内容を手元で繰り返せる）。WSL に build-essential・pkg-config・jq と Rust 1.90.0 を導入（ユーザー許可のうえ、`wsl -u root` で apt。Rust は一般ユーザー）。release ビルドは `-j 8` で 1 分 20 秒。
- 結果（main 69f7f74、模擬 Discord）：unit の起動（Type=notify、Watchdog）、Core ワーカーの `kill -9`（補充、Gateway は不変）、Gateway の `kill -9`（再起動）、botd の `kill -9`（systemd が再起動、`NRestarts=1`、予約が整理され Gateway が復帰）、`systemctl stop`（`Result=success`、残りプロセス 0）がすべて成功。後片付け（専用ユーザー・/etc・/opt・/var/lib・unit・プロセス）が残らないことを確認。

