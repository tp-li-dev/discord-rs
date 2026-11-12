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
