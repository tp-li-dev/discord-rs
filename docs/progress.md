# 開発計画・引き継ぎ

- B3 観測：Core の管理対象ハンドラーを待機中に未完了時間と件数を確認し、30 秒以上の要求を標準エラーに警告する。同一要求の再警告は 5 分以上空ける。期限や取消は追加していない。単体テストを追加したが、指定により cargo は未実行。Tokio の `test-util` feature が現在の manifest にないため、許可された変更範囲内では `time::pause` を使用できず、注入した時刻によるテストとしている。

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


## 2026-11-14 終了時点の状態と、次回の再開手順
- **main（検証済み）**: cd4bfb7。CI（Ubuntu）全ジョブ成功、実 systemd smoke（自動復旧を含む）成功、実 Discord の基本動作（ログイン、`/ping`、`!ping`、`!count`、ワーカーの `kill -9` 復旧、`botctl stop`）は、ユーザーのサーバと WSL で確認済み。
- **実機（WSL2 Ubuntu 24.04）**: botd は停止済み（`systemctl stop botd`、`Result=success`、残りプロセス 0）。unit、`/etc/discord-rs/{botd.env,botd.secret,bots.toml}`、`/opt/discord-rs/bin`、専用ユーザー `discord-rs` は残してある（token は `/etc/discord-rs/botd.env`、0640。値は誰にも見せていない）。再開は `sudo systemctl start botd`。不要になれば `userdel discord-rs; rm -rf /etc/discord-rs /opt/discord-rs /var/lib/discord-rs /etc/systemd/system/botd.service`。
- **実機で見つかった未解決（修正中）**: (1) 実 Discord では Gateway の `kill -9` から自動復旧しない（`try_acquire_identify` が実 Discord の自動復旧を意図的に拒否）。(2) Gateway/botd の再起動で `!count` が 0 に戻る（Core が Gateway の子で、状態が Gateway のメモリ上。設計上の Gateway/Core 分離が通常経路では未実装）。
- **未マージの作業**:
  - ブランチ `wip/identify-merge`（70ee9f5）: (1) の修正（ローカルの Identify 許可: 最短 30 秒間隔・1 時間 10 回。`src/src/local_identify.rs`、`daemon.rs`）。`ci-verify` で CI と systemd smoke を実行中。緑なら main に反映し、WSL で再度 Gateway の `kill -9` を試験する（Identify を 1 回消費）。
  - サブエージェントの作業ツリー `.claude/worktrees/agent-accc796218e0e2c02`（未コミットの可能性）: Core の確認済みスナップショット（count/job）の永続化（`src/src/core_state.rs`、`worker.rs`）。
  - サブエージェントの作業ツリー `.claude/worktrees/agent-a8a6c25f16f001e16`: Gateway と Core の分離の設計書（`docs/gateway-core-separation-design.md`）。
  - 上の 2 つは、完了時にコミットされる想定。作業ツリーに未コミットの変更が残っていれば、内容を確認してから取り込む。
- **次にやること（優先順）**: (a) `wip/identify-merge` の CI 結果を確認して main に反映 → WSL で Gateway の `kill -9` を再試験。(b) 永続化のブランチを取り込み、WSL でビルド・対象試験 → CI → 実機で `!count` が再起動後も続くか確認。(c) 設計書を読み、Gateway と Core の分離を段階実装（実装の主体は Claude とサブエージェント。Codex は Plus で 5 時間制限のため補助のみ）。(d) Gateway の無停止更新（セッション引き継ぎと Resume）は分離の後。

### 追記（終了直前）
- 永続化のサブエージェントが完了: ブランチ `worktree-agent-accc796218e0e2c02`（ac1588b、`src/src/core_state.rs` ほか。保存は約 0.5 秒ごとに変化を検知し最短 2 秒間隔、停止時に必ず保存、起動時に復元。イベントのジャーナル・カーソルは保存せず、`kill -9` では最大約 2.5 秒分が失われる）。
- Identify 許可と永続化を合わせた版を `wip/combined`（origin に push 済み）で CI 実行中。**この版は main に未反映。CI が全ジョブ緑になったら main に反映し、WSL で Gateway の `kill -9` と、再起動後の `!count` 継続を実機で確認する。**
- 設計書のサブエージェント（`docs/gateway-core-separation-design.md`）は作業中。完了していれば `worktree-agent-a8a6c25f16f001e16` に出る。

### 訂正（終了直後）
- Gateway と Core の分離の設計書は、サブエージェントが停止され、**未作成**（成果物なし）。次回、あらためて作成する（読み取り調査 → `docs/gateway-core-separation-design.md`）。上の『作業中』の記述は、この訂正を正とする。

## 2026-11-15：実 Discord・実 systemd での常駐・自動復旧・Core の無停止更新の実機確認（成功）
- 環境：WSL2 の Ubuntu 24.04（systemd）、ユーザーのテスト用 Bot、main 1ad8daa（Identify のローカル許可と Core 状態の永続化を含む）。unit は出荷版（`--stop-children-on-exit`）、token は `/etc/discord-rs/botd.env`（0640）。
- 結果（すべて実 Discord）：(1) 常駐の起動：`ログイン成功`、`/ping`・`!ping`・`!count` が返る。(2) Gateway を `kill -9`：**2 秒で自動復旧**（`restarts: 1`、`--- gateway generation 2 ---` で再ログイン）。前日は `Failed`・`restart_exhausted: true` で復旧しなかった。(3) 復旧後の `!count`：3 → **4**（Core の確認済みスナップショットの永続化が Gateway の再起動をまたいで働いた）。(4) botd を `kill -9`：**6 秒で systemd が再起動**（`NRestarts=1`）、`daemon: removed stale launch reservation (no live Gateway)` で古い予約が整理され、Gateway が復帰。`!count` は **5**。(5) Core（Worker）の無停止更新：`worker-path.txt` に新しい実行ファイルのパスを書くと、**Gateway の PID は不変のまま** 2 秒で Worker が 2 つとも入れ替わり（`Worker 1/2 を切り替えました`、`Worker 2/2 を切り替えました`）、`!count` は **6**。(6) `systemctl stop`：0 秒で `Result=success`、子も含め残りプロセス 0、起動予約 0、`core-state.json` は残る。
- 実機で分かった WSL の注意：WSL2 はアイドルで VM ごと停止する（約 25 秒）。**開いた端末（セッション）がないと botd が道連れで止まる**。実験中は WSL の端末を開いたままにする（または `.wslconfig` の `vmIdleTimeout`）。バックグラウンドの `wsl.exe -- sleep` では維持できなかった。
- 未検証：WSL の再起動後の botd の自動起動（`systemctl enable` していない）、長時間稼働、Resume、複数 Shard、レート制限の実機、Gateway の無停止更新（ローリング）、Gateway と Core の分離（通常経路は未実装。ロードマップ）。
- 修正済みの課題：実 Discord の Gateway 自動復旧（ローカルの Identify 許可：30 秒間隔・1 時間 10 回）、再起動をまたぐ `!count` の維持（確認済みスナップショットの永続化。イベントのジャーナルとカーソルは保存せず、`kill -9` では最大約 2.5 秒分が失われる）。

## 2026-11-15：Gateway と Core の分離（設計書の取り込みと方針決定）
- 設計書 `docs/gateway-core-separation-design.md` を取り込み。要点：試験用モードでも Core は Gateway の子（匿名パイプ）で、分離には IPC の媒体の改修（Unix socket、Core 側の受け入れ、接続ごとの状態初期化）が要る。生きている Core に再接続する Gateway は `core-state.json` で状態を巻き戻してはならない（鮮度判定が必須）。分離は単独で障害耐性を大きく上げず、価値は Gateway の無停止更新（新旧 Gateway が同じ Core を共有）の前提になる点。
- 方針（ユーザー決定）：Gateway の無停止更新を目標に分離を進める。第 1 段階は opt-in の `botd --core-mode separate`（Linux、Unix socket、既定は従来のまま、Core 先・Gateway 後）。判断点は設計書の推奨を採用（旧経路は検証完了まで維持、Core は N プロセス `ChildKey.slot`、ホットスワップは後回し、botd 死亡時は Core も失う、IPC 認証は既存方針、Core の成果物は `src --worker` と同一）。

## 2026-11-15：分離の 1-B（Core のソケット受け入れと接続型 attach）
- 1-B1：`--core-listen`（Unix ソケット、0700 ディレクトリ・0600 ソケット・SO_PEERCRED・stdin の 64 桁 16 進秘密を定数時間比較）。1-B2a：`Process::attach`。1-B2b-i：`Manager::attach_gateway_pool`（子を起動せず、Core の復旧・永続状態の Restore もしない。shutdown は Core を止めない）。いずれも CI 緑（process シャードの `core_attach`）。
- 1-B3：Core のアプリケーション状態を `CoreLife` に分け、`serve_connection` が接続ごとの交渉状態（capabilities・保留中の API 結果・イベントカーソル・drain）を作り直す。接続が失敗しても状態は Core に戻る。`core_server::serve` に `Lifetime::{OneConnection, UntilStopped}`。`run`（`--core-listen`）は従来どおり `OneConnection`。`UntilStopped` を botd が使う配線は未実装。
- 検証：WSL で `core_attach` 9 件成功（持続 Core が 3 接続を処理し不正秘密の接続で止まらない／`OneConnection` は 1 接続で終了）。CI は 70ab7ec で `a_stale_socket_is_replaced…` が 1 回 Connection refused で失敗したが、同じテストを WSL で 40 回回しても再現せず、テストのみ変更（Core の stderr を CI ログに出す）の再実行は成功した。**原因未特定の不安定要因として残す**（再発時は Core の stderr で確認）。最後の試験追加コミット（722eda0）の CI は未確認。
- 状態の保持そのもの（再接続後に `!count` が残る等）は構造上のもので、試験では未確認。
- 次：1-B2b-ii（切断検知と上限付き再接続）、1-B2c（`core-state.json` の鮮度判定）、1-C（botd の `--core-mode separate`）。
- 1-B2b-ii：attach した Manager は、接続が壊れたスロット（`process.broken`）を上限付き指数バックオフ（100 ms〜5 s）で再接続する（`poll_core_reconnect`）。子の起動はしない。再接続した Core へは `core-state.json` を Restore しない（Core 自身の状態を正とする）。`--core-listen <path> --core-persist` で接続をまたいで動く Core を起動できる（試験・1-C 用）。試験：Manager を作り直しても `!count` が続く、落ちて起動し直された Core へ再接続して処理が続く。CI 緑（fmt 修正後、3m48s）。
- 制約：切断の検知は次の要求の失敗時（待機中の切断は能動的には検知しない）。再接続後のイベントカーソル／未確認イベントの再送、`core-state.json` の鮮度判定（1-B2c）は未実装。前回 CI の `a_stale_socket…` 失敗は再発していない。
- 1-B2c：attach した Manager は、起動中の Core の状態を `core-state.json` で巻き戻さない。`prepare` が採用する Core 自身のスナップショットを正とし、永続化された状態（起動時）／最後に確認した状態（再接続時）は、Core の確認済みスナップショットが空の初期状態と一致する場合（= botd が起動し直した空の Core）にだけ Restore する（`Process::core_is_fresh`）。attach した Manager も状態を永続化する。試験：古いファイルが生きた Core に入らない、起動し直された Core に最後の状態が入る。
- 不安定要因の原因（解消）：`a_stale_socket_is_replaced…` の失敗は製品の欠陥ではなく試験側の競合だった。試験が listen 中の古いソケットを作ると、別テストが同時に子を fork した間、その子が fd を引き継いで古いリスナーが生き残り、準備確認の接続が古いソケットに成功して、後で RST になる（WSL で並列 8×40 回の実行で再現、修正後 0/320）。古いソケットはデータグラムの bind で作る方式に変更。
- 1-C（途中・未検証・未 push）：`core_attach.rs`（Core ディレクトリ・secret・identity の共有規約）、`core_units.rs`（botd による Core の起動・バックオフ再起動・stdin を閉じて停止、Core は stdin の EOF で終了）、Gateway の `--core-dir`（attach、ホットスワップ監視は無効）を追加。cargo check のみ通過。**未配線**：Daemon への組み込み（launch で ensure、poll で再起動、shutdown_children で Gateway の後に停止）、botd `--core-mode`、Process::attach の接続リトライ（Core 起動待ち）、E2E 試験、CI。
- 1-C（配線完了・CI 確認中）：Daemon に `enable_separate_cores`／起動時に Core → Gateway の順（`--core-dir` を Gateway に付与）／poll で Core の再起動（1 s〜30 s のバックオフ）／`shutdown_children` の最後に `stop_cores`、botd に `--core-mode embedded|separate`（separate は `--stop-children-on-exit` 必須）。`Process::attach` は起動中の Core を最大 3 s 待つ。実プロセス試験 `botd_separate_cores`（WSL で成功）：Core は Gateway の子でなく、Gateway クラッシュ後も同じ Core の PID に再接続、Core 1 本の `kill -9` で botd がその Core だけ再起動し Gateway の PID は不変、botd の SIGTERM で Gateway・Core が全停止しソケット／secret が残らない。未実施：Core 2 本同時の kill、切断中の入力拒否の確認、実 Discord・systemd での確認、`botctl status` への Core 表示、`core-state` が Gateway 再起動後も Core 側に残ることの E2E（`core_attach` の単体では確認済み）。
- 1-D（status・smoke・docs）：status の各 Shard 行に、separate のときだけ `cores`（running/failures）を追加。systemd smoke に separate の手順を追加し、Actions の実 systemd で成功（Core 2 本の親が botd、Gateway を `kill -9` しても Core の PID 不変、`systemctl stop` で Gateway・Core・sock・secret が残らない）。docs（known-limitations / shard-runtime-design / operator-guide / ipc-protocol）を更新。
- 不安定要因（原因未特定）：同じコミットの Rust CI で `update_load` の `crash_old_core`（embedded の Core クラッシュ負荷試験）が 1 回失敗し、再実行で成功した。今回の変更は embedded の経路に触れていないが、再発するまで原因は不明。
- 分離の第 1 段階（1-A〜1-D）は実装・CI 検証済み。実機（WSL・実 Discord）での確認項目は設計書 §5.4 のとおり未実施。

## 2026-11-15：分離（`botd --core-mode separate`）の実機確認（WSL2 の実 systemd、実 Discord、成功）
- 環境：WSL2 Ubuntu 24.04（systemd）、テスト用 Bot、Shard 1、main 0703cf1 のリリースビルド。drop-in `zz-separate.conf` で `--core-mode separate` を有効化（`docs/real-separate-verification.md`）。
- 結果（すべて実 Discord。`!count` は 10 から開始）：(1) 起動：ログイン成功。Core 2 本の親は botd、Gateway も botd の子（`--core-dir` で attach）。ipc のソケット・secret は 0600、ディレクトリは 0700、`botctl status` に `cores` が出る。(2) Gateway `kill -9`：**1.3 秒で新 Gateway が起動**し再ログイン、**Core の PID は不変**、`!count` は 10→**11**（巻き戻りなし）。(3) Core 1 本 `kill -9`：botd が **1.1 秒でその Core だけ再起動**、**Gateway の PID は不変**、`!count` は **12**。(4) Core 2 本とも `kill -9`：両方再起動、Gateway は不変（`BrokenPipe` を検知して記録）、直後の最初の `!count` が **13** で成功（無反応なし）。(5) `systemctl stop`：0.07 秒、`Result=success`、残りプロセス 0、ipc の sock・secret なし、起動予約なし、`core-state.json` は残る。(6) botd `kill -9`：**5.2 秒で systemd が再起動**（`NRestarts=1`）、Gateway・Core とも入れ替わり孤児なし、`!count` は **14**。
- 分かったこと：期待どおりに動いた。Gateway の再起動時間は、embedded の約 2 秒（前回の記録）より短かった（Core の再起動を含まないため）。(4) で `!count` が失われなかったのは、Gateway が保持する確認済みスナップショットが空の新 Core に戻されたため。
- 未確認のまま：長時間稼働、複数 Shard、Resume、レート制限、OS 再起動後の自動起動、Core のホットスワップ（separate では未対応）、(4) の切断中に入力が来た場合の拒否の数（今回は入力なし）。

## 2026-11-15：Gateway の無停止更新の設計案
- `docs/gateway-rolling-update-design.md` を追加（設計案、未実装）。第一候補は Resume ではなく「二重セッション＋待機側のバッファ」（新 Gateway が別セッションで Identify→Standby→Pause/epoch 切替→Active。session_id の引き渡しが不要）。必要な変更：Core の同時 2 接続と epoch による fencing、接続時 identity、候補 Gateway の `--standby`、botd の本番経路。実 Discord での V1（同一 Shard の 2 セッション）が方式を決めるため、実装前に先に確認する案。
- V1（実 Discord、WSL、2026-11-15）：同じ Bot・同じ Shard（1/1）に、botd 配下の Gateway A と、別の状態ディレクトリで手動起動した Gateway B を同時にログインさせ、`!count` を 1 回送ったところ、**2 件の返信**（A の 15、B の 1）が返った。結論：Discord は同じ Shard の 2 つの Identify セッションを同時に有効にし、**両方にイベントが届く**（二重セッション方式の前提は成立）。A の接続は B のログイン後も切れなかった（ログに再接続なし）。B は試験後に停止・削除済み。未確認：2 セッションが長時間共存できるか、Identify の消費数（`session_start_limit` の残りは未記録）、切替中のイベント欠落・重複（V3）、Interaction の 3 秒（V4）。
- R-1（Gateway 無停止更新の第 1 段、Core のみ）：持続 Core は、1 つの接続を提供中でも次の接続を受け付けて認証し、attach 行に付けた epoch（`<64 桁 hex>[ <epoch>]`、無指定は 0）で順序づける。**高い epoch は提供中の接続を止め（要求と要求の間でだけ止まる。途中では止めない）、次に提供される。同じ epoch は順番を待つ。低い epoch は即座に拒否。** acceptor は 1 つの future を保持し、認証の途中のクライアントを失わない（最初の実装は select で future を落として負荷時に接続を失い、30×6 回の反復で見つけて修正）。`Fence`/`next_request` を worker.rs に追加。試験 `connections_are_ordered_by_ownership_epoch` ほか（core_attach 15 件、WSL で 30×6 回反復 0 失敗、clippy 緑）。**未実装**：Gateway 側が epoch を送る配線（今は Process::attach は epoch 0 固定）と Hello の identity による検証（R-2）、待機側の読み取り専用化（R-3）。
- R-2：Gateway が ownership epoch を attach 行（` <epoch>`、0 のときは付けない）と Hello の identity で提示する。Core は、epoch 付きで受けた接続の identity をその epoch に揃え、Hello の完全一致で検証する。Gateway の新オプション `--core-epoch N`（`--core-dir` と併用のみ、0 は不可）、`core_attach::load` が epoch を受ける。試験 `a_gateway_with_a_higher_epoch_takes_over_and_the_old_one_cannot_return`：epoch 5 の Manager が epoch 3 の Manager を引き継ぎ、`!count` が続き、古い方は要求が通らず再接続でも取り戻せない。botd からの epoch の採番（`--core-epoch` を渡す）は R-4。
- R-3a（重複排除、方式 (b)）：`Request::Dispatch` に任意の `source_id`（Discord のメッセージ ID。`serde(default, skip_serializing_if)` で後方互換）を追加。Core は直近 4096 件の ID の窓（`SeenIds`、Core の寿命で保持し接続をまたぐ。永続化しない）を持ち、既出の ID は処理せず「内容なし・状態不変」の結果を返す。Gateway のメッセージ経路は `message.id` を渡す（`Manager::dispatch_key_with_response_id`）。試験：同じ接続での再送、別 ID／ID なしは処理、引き継いだ別の Gateway（高い epoch）が同じ ID を配送しても処理しない、重複は状態を進めない。**未実装**：スラッシュコマンド／Interaction／コンポーネントの ID による排除（現状は重複し得る。Interaction の ID を載せる変更が別途要る）、Core 再起動をまたぐ重複の検知、`--standby` と `Activate`（R-3b）。
- `register --check`（スラッシュコマンドの変更検知）：Discord が持つコマンドを 1 回読み取り（書き込みなし）、リンク済みの定義（説明と引数の名前・型・必須）と比べて `status=up-to-date|changed|missing` を出す（要登録のとき終了コード 2）。`--apply` も先に同じ比較をし、最新なら何も送らない（コマンド作成の厳しいレート制限を無駄に使わない）。読み取りに失敗した `--apply` は従来どおり登録に進み、`--check` は失敗にする。比較は単体試験（Discord が足すフィールドを無視、説明・引数・必須の変更を検知）。実 Discord で `--check` を読み取り専用で実行し、グローバルの `ping` が `missing`（ギルド単位で登録しているため）と返ることを確認。未実装：全コマンドの一括同期、起動時の自動チェック。
- R-3b（待機 Gateway）：`--standby`（`--core-dir` 必須、`--core-epoch`・mock・local-control と併用不可）。Discord に自分のセッションでログインしてイベントを最大 512 件保留（超えたら中止）し、Core には接続しない。stdin の `{"command":"activate","epoch":N}`（`GatewayControl::Activate`）で `epoch N` の attach → 保留分を先に処理 → 通常動作。stdin の制御入力を連続読みに変更（`control_input`）。
- **実機（WSL、実 Discord、手動起動の待機 B と botd 配下の A）**：待機中に `!count` を 1 回 → A だけが返信（17）。B を `activate epoch 1` → 「保留 24 件」を処理して有効化。A のログは `worker is unavailable`（Core の接続を B に取られ、epoch 0 では再接続できない）。直後の `!count` は **B だけが 1 件**返信（18）。ユーザー報告は「18 が 1 件だけ」で、有効化直後の 17 の再返信が無いことは、個別の言及がないが報告の内容と矛盾しない（重複排除が効いた可能性が高い。保留分に 17 のメッセージが含まれていたかのログ上の裏付けは取っていない）。試験後、B を停止し botd を再起動して元に戻した。
- 未確認：重複排除が実際に働いた回数の計測、切替の所要時間、更新中に連続して送った場合の欠落、Interaction（スラッシュ）の重複。**botd が更新を指揮する経路（R-4）は未実装**で、今回の切替は手動。
- R-4（botd が Gateway の無停止更新を指揮）：`botctl gateway-update <bot>`（`--core-mode separate` のときだけ有効。embedded／通常の daemon は従来どおり安全に拒否）。新しいバイナリを Gateway のプログラムのパスに置いてから実行する。Shard ごとに順に：待機 Gateway（`--standby`）を同じパスから起動 → ログイン完了（`standby-ready` マーカー）を最大 90 秒待つ → `activate`（epoch+1）を送る → `ChildRecovery::replace_ready` で監督を新 Gateway に移す → 旧 Gateway に shutdown を送り、30 秒で終了しなければ kill（botd がハンドルを保持）。最初の失敗で中止し、稼働中の Gateway には触れない。更新後に botd が Gateway を再起動するときは `--core-epoch <現在の epoch>` を渡す（低い epoch では Core に入れない）。
- **実機（WSL、実 Discord、separate）**：(1) 更新（epoch 1）：operation `succeeded`、Core の PID 不変、旧 Gateway は終了、`!count` は 17→18→19 と **1 件ずつ**（保留 2 件）。(2) 更新後の Gateway を `kill -9`：botd が `--core-epoch 1` で再起動し attach 成功、`!count` 20。(3) 2 回目の更新（epoch 2、約 1.4 秒）：成功、`!count` 21 が 1 件。途中、手動試験の別ディレクトリで保存した 18 が botd 再起動で 17 に戻って見えたが、更新とは無関係（試験の後始末による）。
- 未確認：更新中に連続して送った場合の欠落・重複の実測（V3）、Interaction（スラッシュ）の重複排除と 3 秒（V4）、`session_start_limit` の消費、複数 Shard、長時間、候補のログイン失敗時の実機（単体でも未試験）、更新と並行した Core の再起動。`botctl gateway-update` は botd の通常の認証を通るが、`update` 用の RBAC／監査経路は通っていない。自動試験は未整備（`--standby` が実 Discord 経路のみで、模擬 Discord では動かないため。実機のみで確認）。
- V3（更新中の連続送信、実 Discord、WSL）：ユーザーが約 1 秒間隔で `!count` を送り続ける間に `botctl gateway-update` を実行（21:15:00.6〜21:15:01.9、約 1.3 秒、成功）。報告は「22 から 33 まで」（12 件）で、ユーザーの判断は問題なし。**欠落・重複の報告なし**。ただし、更新の窓が約 1.3 秒と短く、約 1 秒間隔の送信では窓に重なったメッセージは 1〜2 件程度と見込まれる。窓での重複排除が実際に働いたか（Core が捨てた件数）は、計測していない。より厳密には、短い間隔（連投）での再試験と、Core 側の重複排除の件数カウンタが要る。
- Interaction（スラッシュ）の重複排除：**コード変更は不要と判断**。`interaction_flow::run` は「ACK が確認できてから実行」で、Discord は Interaction への最初の応答を 1 回だけ受け付ける（2 回目は「既に応答済み」で失敗する、という Discord の仕様に基づく理解）。更新中に旧・新の Gateway が同じ Interaction を受けても、先に ACK した方だけが実行し、後の方は `AckFailed` で実行しない。3 秒を過ぎた古い Interaction を新 Gateway が保留から再生しても ACK が失敗して実行されない。**この挙動は実 Discord では未確認**（2 つの Gateway が同時に同じスラッシュコマンドを受けた場合の実測が要る）。メッセージ（`!` コマンド）は ACK の仕組みがないので、Core の ID 窓で排除している（実機で確認済み）。コンポーネント／モーダルの Interaction も同じ ACK 規則だが、同様に未確認。
- OS 再起動後の自動起動（WSL2 の実 systemd、実 Discord、separate）：`systemctl enable botd` のあと `wsl --shutdown`。再起動で botd が自動で起動し（Core 2 本と Gateway が揃い、ログイン成功）、`!count` は 33 → **34**（保存済みスナップショットから復元）。注意：WSL は端末が開いていないとアイドル（約 25 秒）で VM ごと停止し、その間は botd も止まる（今回も一度停止し、次の起動で自動復帰して状態は失われなかった）。本物の Linux サーバでは起きない。WSL で長時間動かすなら端末を開いておくか `.wslconfig` の `vmIdleTimeout`（未変更）を使う。
- 待機 Gateway の失敗時（実機、WSL、実 Discord、separate）：`/opt/discord-rs/bin/src` を一時的に差し替えて 2 通りを再現（試験後に元へ戻し、`cmp` で一致を確認）。(1) 候補がすぐ終了：操作は `failed`（`ChildExited`）。(2) 候補がログインせず固まる：約 90 秒で `failed`（`StopTimedOut`）、候補は kill。どちらも稼働中の Gateway（PID）と Core（PID）は不変、待機プロセスは残らず、直後の `!count` は 34→**35**（1 件）。未確認：Discord の認証失敗（4004 等）で候補が終了する経路（(1) と同じ「候補の終了」として扱われるはずだが、実際の close code では未試験）、候補が有効化の途中で死んだ場合（`activate` 送信後の失敗は、epoch が上がるので旧 Gateway は Core に戻れない。botd が再起動する Gateway は新しい epoch で attach する）。
- 更新中の連投の再試験（実機、WSL、実 Discord、separate）：Core に重複破棄の件数を出力（`core-N.log` に「重複したメッセージを処理せず破棄しました（累計 N 件）」。ID や内容は出さない）。開始時のカウンターは 35。ユーザーが連投する間に `gateway-update`（21:32:13.6〜14.7、約 1.1 秒、成功、保留 2 件）。結果：最終カウントは **50**（35 から 15 件が処理された）。貼り付けられた返信は 39〜50 で、**飛びも重複もなし**。ただし 46 が 47・48 の後に届いた（返信の順序が入れ替わった。同時に処理した返信が前後しただけで、数は重複していない）。貼り付けの冒頭は 36〜38 の返信が含まれず、送信回数もユーザーの数え直しがないため、15 件すべての送受信の対応は確認していない。**Core の重複破棄の記録は 0 件**：今回も、旧と新が同じメッセージを両方受けて新が再生する、という状況が起きなかった（保留 2 件は Ready など別種のイベントと見られる）。つまり重複排除は、実 Discord では「働く場面」自体をまだ観測できていない（単体試験では働くことを確認済み）。窓の中に旧・新の両方が受けるメッセージを意図的に入れるには、候補を READY にしたまま有効化を遅らせる試験が要る。
- **重複排除が実 Discord で働くことを確認**（実機、WSL、separate、手動の待機 Gateway B と botd 配下の A）：B をログイン済みで待機させ、その間にユーザーが `!count` を 5 回送信（A が 56〜60 を返信）。B を `activate epoch 2` → **保留 15 件**を処理し、**Core が重複を 5 件破棄**（`累計 1〜5 件`、送った 5 件と一致）。ユーザー報告：直後の `!count` は **61 が 1 件だけ**（有効化直後の二重返信の有無は個別の言及なし。ただし破棄の件数は、再生された 5 件が処理されなかったことと整合する）。試験後、B を停止し botd を再起動して元に戻した（B の別ディレクトリの保存値は消えるため、botd 再起動後の数は 60 から再開する）。
- 試験手順の注意（私の誤り）：手動の有効化の epoch は、**現在稼働中の Gateway の epoch より大きく**しなければならない（同じ epoch は順番待ちで、Health がタイムアウトして B が終了する）。`botctl gateway-update` は botd が epoch を管理するので、この問題は起きない。
- 更新の自動試験：`--standby` を模擬 Discord の経路でも動かせるようにした（collector に接続し、`standby-ready` を作って待ち、`activate` で Core に attach してから ready を送る）。`begin_gateway_update` も模擬構成で使える（資格情報を読まず `--mock-discord` を渡す）。実プロセス試験 `gateway_update_replaces_the_gateway_and_keeps_the_cores`：`gateway-update` で新 Gateway が起動して有効化され、**Core の PID は不変**、旧 Gateway は終了、操作は `succeeded`。新 Gateway をクラッシュさせると botd が再起動し、**上がった epoch で同じ Core に attach** する。WSL で 4 並列×15 回で 0 失敗。実 Discord の経路（`run_standby`）とは別実装なので、そちらの保留・再生は引き続き実機でのみ確認している。

## 2026-11-15：本番サーバー（Oracle、aarch64、Ubuntu）での放置試験を開始
- 環境：`oracle-cloud-core`（aarch64）。サーバーでビルド（`cargo build --release --locked -j 2`、約 2 分）し、`/opt/discord-rs/bin` に配置。`botd --core-mode separate --stop-children-on-exit`（drop-in `zz-separate.conf`）、Shard 1、テスト用 Bot（`bots.toml` の `cluster.id = "oracle-soak"`）。token は `read -rs` で `/etc/discord-rs/botd.env`（0640）に保存し、画面・履歴には出していない。開発機（WSL）の botd は、同じ Bot の二重返信を避けるため `disable --now` で停止。
- 開始：2026-11-15 13:10:12 UTC（22:10 JST）。最初のスナップショット（`soak-report`）：botd 7.7MB・Core 各 6.1MB・Gateway 10.2MB（RSS）、fd 11〜16、再起動 0、Core 2 本 `running`・`failures 0`、ipc に sock 2 + secret 1、予約 1。ユーザー確認：`!ping` と動作を確認（`!count` は 1 から）。journald の警告 1 件は内容未確認。
- 次：数時間後と翌日に `sudo soak-report | tee -a ~/soak.log` を取り、RSS・fd・ログの大きさ・再起動・切断が単調に増えていないかを比べる（`docs/soak-test.md`）。

## 2026-11-16 夜間の自律作業（ユーザー就寝中、PC は 02:08 に自動シャットダウン）
- 23:38 時点：main = eb6e806（CI 全ジョブ緑、systemd smoke も緑）。追加：更新中の Core 落下の自動試験、複数 Shard（2 Shard）の分離・更新の自動試験（Codex `gpt-6.1-terra` が執筆、WSL で実行して合格）、`gateway-update` の role／scope 認可の単体試験、docs（Haiku 執筆、事実を確認して訂正）、待機 Gateway は制御 pipe が閉じたら終了、botd 停止時は更新途中の待機 Gateway を kill。
- Codex のモデル名：`Terra` は不可、正式名は `gpt-6.1-terra`／`gpt-6.1-sol`（`~/.codex/models_cache.json`）。疎通確認済み。
- 進行中：Codex（`gpt-6.1-sol`）による今夜の差分の敵対的レビュー（読み取り専用）。
- 23:43 `gateway-update` の監査：`runtime-root/gateway-update-audit.jsonl`（0600、追記、`requested`→`succeeded`／`failed`／`rejected`、subject・role・Bot・操作 ID・固定エラー種別のみ）。要求の記録に失敗したら開始前に拒否。単体試験（記録・拒否・権限・秘密を含まない）と、実プロセス試験（更新後に `requested`,`succeeded` が残る）。`update-audit.jsonl` のような検証付き監査・ローテーションは未実装。
- 23:57 Codex（`gpt-6.1-sol`）の敵対的レビュー（9 件、コードで確認した指摘）への対応：**修正済み**：(1) Core が一度提供した最高 epoch を、接続が切れた後も下限として保持（低い epoch は再び入れない。試験あり）。(3) 状態ファイルに書き手の epoch を記録し、低い epoch が高い epoch のファイルを上書きしない／一時ファイル名を pid 付きにし、死んだ書き手の一時ファイルを掃除（試験あり）。(4) 失敗・強制停止で終わる `gateway-update` は、未有効化の待機 Gateway を kill。(7) 待機 Gateway の保留イベントの再生は、同時実行の枠が空くまで待つ（枠が満杯でも捨てない）。(8) 待機 Gateway のログイン（Identify）を `local_identify` に記録。**既知の限界として文書化（`known-limitations.md`）**：(2) 確認付き有効化、(5) fence 時の進行中応答の ApiResult、(6) 応答書き込みの期限、(9) ログのローテーション。
- 00:05 Sol 指摘(2)「確認付き有効化」を実装：`activate` を送ったら、候補の `standby-active`（全 Core に attach 済み）を最大 20 秒待ち、確認できてから監督を移して旧 Gateway を止める（`send_gateway_activation`／`gateway_activation_progress`／`commit_gateway_update`）。確認できなければ操作は `failed`、候補を kill し、旧 Gateway も kill して botd が上がった epoch で再起動（旧は既に Core から外れており戻せないため）。正常系は既存の実プロセス試験（2 本）と複数 Shard 試験で通る。失敗系（確認が来ない）の自動試験は未整備。
- 00:06 Sol 指摘(9)のログ上限を実装：稼働中も定期（約 60 秒）に大きさを見て、超えたら `.1` に写して空にする（`core_units::rotate_in_place`、試験あり）。
- 00:08 確認が来ない場合の自動試験を追加（スクリプトの待機 Gateway がログインだけして確認しない）：操作は `failed`（確認の期限 20 秒）、旧 Gateway は終了、botd が上がった epoch で Gateway を戻し、同じ Core に接続、監査に `requested`／`failed`。WSL で合格。
- 00:22 Sol 再レビュー（4 件）への対応：(P1) 有効化の確認待ちの間に旧 Gateway が code 0 で終了して復旧が「意図した停止」と読んで止まる場合に備え、失敗時に停止済みの枠を再び起動（予算は保持。自動試験なし、コード上の対応）。(P2) ログの回転は、コピー中に書かれた行を失わないよう、測った長さのままのときだけ空にする（そうでなければ次回へ）。(P3) 検査したファイルと同じ記述子（dev／ino 一致）で copy と truncate を行う。(P4) `standby-ready`／`standby-active` を消せなければ更新を開始せず、マーカーは通常ファイルだけを有効とする。operator-guide／ipc-protocol を最新の手順に更新（Haiku、事実を確認）。
- 00:31 P1 の自動試験を追加（`a_gateway_that_exits_cleanly_during_an_update_is_brought_back_when_it_fails`）：確認待ちの間に旧 Gateway が SIGINT で code 0 の終了をしても、更新が失敗したあとに Gateway が戻る。修正を一時的に外すと試験は失敗する（60 秒で期限切れ）ことを確認し、元に戻した。
- 00:41 CI 注記：`raft_quorum_handoff::restart_keeps_fence_epoch_but_requires_a_new_full_quarantine` が 1 回 `Err(Storage)` で失敗し、失敗ジョブの再実行で成功した（今夜の変更と無関係の Raft 試験。以前から不安定要因として記録している領域。原因は未調査）。
- 00:48 Sol 指摘(6)を実装：ソケットの相手（分離した Core に接続した Gateway）への応答の書き込みに 10 秒の期限（`write_frame`）。読まなくなった旧 Gateway が、新しい epoch の引き継ぎを無期限に止めない（自動試験 `a_peer_that_stops_reading_cannot_hold_up_a_takeover`）。Raft 試験の不安定要因：Codex（`gpt-6.1-terra`）の読み取り調査では、shutdown 後に旧ノードのストア参照が非同期タスクに残り、再オープン時のロック取得に失敗する競合が最有力（確信度は中、再現は未）。試験側で `Storage` エラーだけを最大 5 秒再試行するようにした（他のエラーと、ロックが解けない場合は従来どおり失敗）。
- 00:55 夜間作業の終了：main = f7efb84（CI 全ジョブ緑）。WSL の botd は停止のまま（二重返信を避けるため）、Oracle サーバーの放置試験（22:10 JST 開始）には触れていない。実行中のエージェントなし。次回の再開は `docs/handoff.md` §0 と `docs/soak-test.md` §5。
- 01:08 明日の一括確認スクリプト `deploy/dev/update-check.sh`（Codex `gpt-6.1-terra` が執筆、私が修正）：更新前後の PID、Core の不変、Gateway の入れ替わり、旧 Gateway の消滅、IPC の残骸、マーカー、監査（requested→succeeded）、journald の警告を確認し、PASS／FAIL の表を出す。修正点：更新後の Gateway は `--standby` を引数に残すため、`--standby` を役割にせず「`--core-dir` を持つプロセスがちょうど 1 つ」で判定。**サーバーでは未実行**（`bash -n` のみ）。
- 01:43 Astra Max（`gpt-6-astra`、max、読み取り専用）の監査 12 件：P1×3 のうち 2 件と P2 の 4 件を修正（保存 epoch を botd の起動時刻と合成して世代をまたいで単調に／永続 Core は serve 終了で明示 exit／書き込み期限を 3 秒に／ログ回転が止まらない／操作全体の期限）。残りは文書化。詳細と明日の確認 5 項目：`docs/audit-2026-11-16.md`、原文：`docs/audit-astra-max-raw.md`。
- 02:06 Astra 再監査（7 件）への対応：保存の印を「起動のたびにファイルの値より必ず大きい印」に変更（モード切替・時計の逆戻り・botd 再起動・20 ビットの切り詰めの全部を解消、自動試験あり）／ログ回転のコピーを終端オフセットまでに制限／`update-check.sh` が退役を待つ／文書の限定。残り：Core 側の要求の期限（止まったハンドラ）、`soak-diff` の回転対応。
- 02:25 Astra 再監査 第 3 回（3 件）：保存の印を同一プロセス内で単調増加に（別プロセス間の衝突は残る限界）／退役中の旧 Gateway は停止要求の時点で再接続を止める／再接続の前に壊れたセッションの書き込み側を閉じる。判定は「実行可能な指摘が残る」で、対応後の再審査は未実施。
- 02:36 Astra 再監査 第 4 回（3 件）への対応：進行中の再接続が復元・取り付けの前に停止要求を確認して中止する／保存の印の上限を `u64::MAX - 1` に。未対応：バックログ再生中の停止要求の反映、すでに送った復元の取り消し（Core 側の epoch による fencing）。**夜間の監査はここで打ち切り（第 4 回への対応後の再監査は未実施）。判定は「実行可能な指摘が残る」。**

## 2026-11-16 昼：Astra の残りの指摘への対応（Codex `gpt-6.1-terra` が執筆、私が統合）
- C：保存の印を、プロセス間の排他ロック下で耐久的に採番（サイドファイル `core-state.writer`、桁あふれは検査、失敗時は保存を無効化して警告 1 回）。B：永続 Core が `--epoch-floor N` を受けて起動（下限を持った状態から）、バックログ再生中もシャードを poll し停止要求を観測（最大 512 件）。B3（Core の要求の期限）は、`WorkerTasks::request` の取り消しが安全でないため追加せず、理由をコードに記載。A：更新の前に Identify の履歴で 2 回分の余裕を確認、終端の監査を保留リストに保持して再試行しステータスに `gateway_update_audit` を追加、監査・候補ログを安全に開く（シンボリックリンク拒否、所有者・リンク数・0600 を記述子で確認）、再起動する Core に `--epoch-floor` を渡す。
- 運用メモ：`src.zip`（配布用）が誤ってコミットされていたので追跡から外し、.gitignore に追加した。
- 11:30 Astra 再監査 第5回（7 件、すべて P2/P3）に対応（Codex `gpt-6.1-terra` D/E/F が執筆、私が統合・修正）：#1 Identify のクールダウン中は更新を待機し、枯渇のときだけ失敗／#4 監査の再試行は同じバイト列を使い、部分行を改行で区切ってから追記／#3 aarch64 で誤っていた O_NOFOLLOW のハードコードをやめ、path の事前確認と開いた記述子の (dev, ino)・所有者・リンク数の照合に変更／#5 保存の印のロックに 5 秒の期限／#7 警告の文字化けを修正（Codex の書き込みで日本語が `?` になる事故を発見して私が直した）／#2 再生中に完了済みタスクを回収／#6 再生の上限超過でも通常の排出を通ってから終了。
- 注意：Codex（Windows の書き込み）が日本語を `?` に化かす事故が 2 回あった。今後、Codex の成果物は `grep -rn four question marks src tests` で必ず確認する。
- 11:58 Astra 第6回（9 件）への対応。**Codex E/F（Terra）の報告のうち、main.rs・core_units.rs の変更が実際には書き込まれていなかった**（Astra が差分で発見）。再依頼して差分で確認：#2 再生中の完了タスク回収／#6 再生の超過時も共通の排出を通る／#3 aarch64 の O_NOFOLLOW をやめ、事前の symlink_metadata と開いた記述子の (dev, ino)・所有者・リンク数の照合に（read+append ヘルパーの open は FIFO に塞がれない。後続の read-only open までは保証しない）／#5 ロックを try_lock の 5 秒再試行に／R6-1 更新の期限を Shard 数に応じて（120 秒 + 45 秒 × Shard 数）／R6-2 更新済みの Shard の再起動抑止を直ちに解除／R6-8 監査の不完全な末尾は `.corrupt` に退避して切り詰める／R6-9 同期のみの再試行はファイルの同一性と長さを確認。教訓：Codex の報告は差分で必ず確認する。
- 第7回の5件を実装：同じ Bot の他 Shard の起動を候補／失敗後の復旧許可まで待たせて Identify 枠を保護。監査の読込は検証済み記述子を再利用し、末尾検索は 8 KiB 単位、退避は 64 KiB／`.corrupt` 全体は 1 MiB まで。退避の進捗を保持して重複コピーを避け、保留監査の再試行失敗後は 1 秒空ける。同期だけの再試行でも保存位置のバイト列を照合。チェックポイントは read-only と開いた記述子の検査を併用し、open 前の FIFO 差替え競合は未解消として明記した。単体試験を追加したが、依頼に従ってビルド・cargo・試験は実行していない。差分と UTF-8 の読戻しのみ確認し、WSL での検証は統合担当へ引き継ぐ。コミット・push はしていない。
- 第8回の指摘に対応：同じ Bot に復旧用 Identify の予約が残る間は、新規 `gateway-update` を操作登録・再起動抑止より前に拒否する。復旧の予約を維持し、復旧待ちと再起動抑止の循環を避ける。ControlPlane の回帰試験に、反復拒否で抑止が漏れないこと、別 Bot の独立性、予約解除後の再受付を追加した。差分と UTF-8 の読戻しを確認。依頼に従い cargo・ビルド・試験は未実行、WSL での検証は統合担当へ引き継ぐ。コミット・push はしていない。

## 2026-11-16 昼：Astra 第7〜9回の対応と次の作業案

- Astra 第7回の 5 件（ed8f21d）、第8回の 1 件（2746d26）を修正。第9回の再監査は「NO FURTHER ACTIONABLE ISSUES」。main の試験用コードは 2746d26（以降はドキュメントのみ）。CI（Rust CI・systemd smoke）は green。`botctl_update_control` の stdin 書き込み競合による失敗が 1 回あり、再実行で通過（原因は未調査の既存の不安定さ）。
- `work/next`（main には未反映）：2 シャードの更新中に Core が落ちるテスト（471606b、CI green、WSL で 3 回成功）、今後の作業案と質問 `docs/next-steps.md`（Astra の設計レビュー込み）。
- 未検証：新版の Oracle 実機、複数シャード、B3、`!workers` の重複排除。
## 2026-11-16：Oracle（aarch64）放置試験の結果（約 9 時間 50 分）

**出典：ユーザーの報告（`soak-report` の要約）。私は生のログを見ていない。** 対象は 2026-11-15 の開始時の版（`src.zip` は main ba7b5f7 相当）で、2746d26 の新版ではない。

- 稼働：2026-11-15 13:10:12 UTC 開始、最終スナップショット 23:00 UTC で約 9 時間 50 分。botd・Core 2 本・Gateway は PID 不変、`--core-mode separate`、Shard 1。プロセス再起動 0、`worker_unavailable` 0、REST unknown 0、journal 警告 0、`botctl` は Running・failures 0・restarts 0。
- RSS（KiB）：5 時間 58 分／8 時間 44 分／9 時間 50 分で、botd 7,976／7,988／7,996、Core 0 は 6,168 のまま、Core 1 は 6,164 のまま、Gateway 11,268／11,384／11,384。fd は全スナップショットで不変（botd 16、Core 各 11、Gateway 12）。観測期間では、リークを示す単調増加は見られなかった。
- Gateway：generations 1、login 1、closes は 4 → 6 → 7（5 時間 58 分／8 時間 44 分／9 時間 50 分）。
- タイマー：`soak-3h`（19:08:20 UTC）と `soak-morning`（23:00:00 UTC）が実行され、`~/soak.log` に記録（11-15 夜に「出ていない」としていた件は、報告では解消）。

**この結果から言えること（限定）**：単一 Shard・separate・約 10 時間の範囲で、メモリ・fd・再起動・警告の悪化は観測されなかった。

**この結果では確認できていないこと（成功扱いにしない）**
- **closes の原因は未確認。** 報告には「未使用時に切断されるのは仕様」とあるが、これは私の側では検証していない。`closes` が何を数えるか（切断か再接続か）、close code、再接続の成否は、`/var/lib/discord-rs/runtime/bot-313031/shard-0/gateway.log` を見ないと分からない（未回収）。Discord が通常の運用で空の接続を切るという前提も未確認。closes が増えるたびに Identify（1 時間 10 回の上限）を消費していないかも、このログからは分からない。
- 2 シャード、Core／Gateway の障害注入、`gateway-update` の実機での長時間・繰り返しは、この試験の対象外。
- **新版（2746d26）は未試験。** この結果を新版の合格として使わない。
- `!count` が試験中ずっと続いていたか（返信の欠落・数の飛び）の記録は、報告に含まれない。
- `systemctl show discord-rs` が inactive/dead だったのは、ユニット名の違いの可能性（報告）。正しいユニット名（`botd`）で `NRestarts` を確認した記録はスナップショット側にあるが、私は未確認。

**残る実機確認**
1. 新版の入れ替え（§5）と `update-check.sh`（1 シャード）。
2. 新版での再度の放置（6 時間以上）。
3. `gateway.log` の close code 確認（切断の原因、再接続、Identify 消費）。
4. 複数シャード（両方のシャード ID に当たる通信が必要）。
5. Interaction（スラッシュ）の重複。

**次に優先する作業（`docs/next-steps.md` の見直しを反映）**：(a) 実機で新版の入れ替えと close code の確認、(b) CI の不安定なテストの原因調査、(c) `!workers` の重複排除と B3 の観測、(d) Core の無停止差し替え。(a) の結果で順序を変える。

### 追記（2026-11-16、サーバーの実出力を確認）

- **タイマー（解決）**：journal に `soak-3h`（19:08:20 UTC）と `soak-morning`（23:00:00 UTC）の起動・正常終了があり、`soak.log` に 19:08・21:54（手動）・23:00 の 3 件がある。11-15 夜に「16:14 などの定時が出ていない」としたのは、設定（開始 3 時間後と 23:00）と合わない私の読み違い。未解決の項目ではない。
- **ユニット名**：`soak-report` の既定は `UNIT=botd`（`deploy/dev/soak-report.sh`）。`systemctl show discord-rs` が inactive/dead だったのは、存在しない名前を見たため。実際のサービスは `botd`（`NRestarts=0`、スナップショットで確認）。
- **closes の意味**：`closes` は `gateway.log` 内の「Gateway 切断」を含む行の数（同スクリプト）。`gateway.log` は 9 時間 50 分で 324 バイト（5:58 で 228、8:44 で 292 バイト）しかなく、1 行は小さい。close code と再接続の結果は、この小さなファイルの中身で確認できる見込みだが、まだ貼られていない（今回の grep は存在しない `/var/log/discord-rs/` を見たため空）。**切断の原因は未確認のまま。**
- 稼働中のプロセスは `ps` で PID 4147850／55／56／57、経過時間は 11-15 開始から継続（botd の引数に `--core-mode separate`）。

### 追記（2026-11-16、`gateway.log` の中身）

- 全文：`generation 1`、`ログイン成功` 1 回、その後 `Gateway 切断: close code 4000` が **10 回**（サーバーでの 2026-11-16 の確認時点。23:00 UTC の記録では 7 回で、その後に 3 回増えている）。他の致命的な close code（4004、4010〜4014 など）はない。
- **解釈（Discord の一般的な仕様に基づく推測で、このログだけでは確認できない）**：4000 は「不明なエラー」で、Discord は再接続を許す（再開可能な）コードとされる。ログイン成功が 1 回のままなので、Gateway プロセスは終了せず、twilight が同じセッションで再接続（Resume）している可能性が高い。ただし **再接続の成功、Resume か新規 Identify か、切断中に落としたイベントの有無は、現在のログに出ないため未確認。**
- 頻度：約 10 時間（開始から確認時点まで）に 10 回、およそ 1 時間に 1 回前後。Discord 側の都合による切断として珍しくはないが、原因はこのログからは分からない。切断のたびに新規 Identify を消費している証拠はない（`login=1`）が、これも直接の確認ではない。
- **ログの不足（改善案、未実装）**：切断行に時刻がなく、再接続の成功・Resume の有無・切断から復帰までの時間が出ない。次の版で、切断と再接続の各行に時刻と結果（resumed／identify）を出すと、実機で原因を切り分けられる。

## 2026-11-16 午後：新版（main 2746d26 のコード）の WSL 実機試験（実 Discord、separate、1 Shard）

環境：WSL2 Ubuntu 24.04、release ビルド（`cargo build --release --locked -j 4`）、`botd --core-mode separate --stop-children-on-exit`、テスト用 Bot（Oracle と同じ token のため、Oracle の botd は事前に `systemctl stop` して停止）。ユーザーが `!count` を送って数と返信件数を確認。**実機の確認はこの 1 回の範囲で、再現性・長時間・複数 Shard は未確認。**

| 項目 | 結果 |
|---|---|
| 起動 | ログイン成功、`!ping`・`!count`（61）が返る。Core 2 本、Gateway 1 本、親は botd |
| `gateway-update`（1 回）`update-check.sh` | 全 PASS：Core PID 不変、Gateway 961→1105、旧 Gateway 消滅、IPC・マーカー・監査（requested→succeeded、約 1 秒）。`!count` 61→65 が連続 |
| Gateway `kill -9` | 約 1.5 秒で再起動、Core 不変。**ログインの成功時間は未計測**（PID の出現のみ） |
| Core 1 本 `kill -9`／2 本 `kill -9` | botd が再起動（約 6 秒／約 9〜11 秒）、Gateway 不変。`!count` 65→70 が連続（巻き戻りなし） |
| botd `kill -9`（#6） | systemd が約 5 秒で再起動、Core・Gateway は新しい PID に入れ替わり、古い予約を整理。`!count` 70→73 が連続 |
| `gateway-update` ×3（`--repeat 3`）| 全 PASS（各回 Core 不変、Gateway 1 本、RSS 10.7〜11.1 MB で増加なし）。`!count` 73→76 が連続 |
| 更新中に Core 0 を `kill -9` | 更新は成功（約 1.5 秒）、Core 0 は botd が 1 本だけ再起動、Gateway 1 本。`!count` 76→79 が連続 |
| `systemctl stop`（#5） | 0 秒、`Result=success`、残りプロセス 0、ipc に sock・secret なし、`core-state.json` は残る |

- journal の警告 4 件は、私が行った botd の `kill -9` に対する systemd のメッセージ（WSL の cgroup の `Failed to kill control group … Invalid argument`）で、更新中の警告ではない。
- 途中、WSL の botd が 15:54:11 に正常停止していた（原因は未確認。WSL の呼び出し終了の影響の可能性があるが推測）。端末を開いたままにしてからは再発していない。
- 未確認：更新中の「重複の抑止」（今回の試験では重複返信は観察されなかったが、観察は `!count` の返信件数のみ）、複数 Shard、長時間、Interaction の重複、`!workers` の重複。
- 試験後、WSL の botd は停止・無効のまま（Oracle で再開するため）。

次：Oracle で新版を入れ替え（`docs/soak-test.md` §5）、新版の放置（6 時間以上）。

## 2026-11-16 17:05 JST（08:05 UTC）：Oracle で新版（main 162112b）の放置を開始

- 入れ替えは Git で行った（初回：`git clone https://github.com/tp-li-dev/discord-rs`、旧 zip の `src/target` を再利用、`cargo build --release --locked -j 2`、旧バイナリは `/opt/discord-rs/bin/*.prev` に退避）。以降の更新は `git pull`。
- 起動：08:05:43 UTC。再起動 0、journal 警告 0。RSS：botd 7.5 MB、Core 各 6.1 MB、Gateway 10.2 MB、fd 16／11／11／13。
- `update-check.sh test`（1 回）：全 PASS（Core 不変、Gateway 143125→143979、旧 Gateway 消滅、IPC・マーカー・監査 requested→succeeded）。ユーザーの `!count` の確認は未記入。
- 追加の変更点（162112b）：`gateway.log` の切断・再開・再接続要求の行に UTC 時刻を付けた（`Gateway 切断: close code 4000 (2026-…Z)`）。放置中にユーザーがコマンドを送った時刻を控え、切断時刻と比べる（「操作の直後に切れる」仮説の確認）。`Resume 成功` の行が出れば、同じセッションで復帰している。
- サーバーに「System restart required」（カーネル更新）が出ている。再起動は放置試験の後に行い、そのときに OS 再起動後の自動起動（実機）も確認できる。
- 未確認：新版の長時間（6 時間以上）、切断の原因、複数 Shard、`!workers` の重複。
- CI：`update_load::crash_old_core` が不安定（162112b は新しい実行で成功、再実行で 3 回失敗。380ca6f は別の既知の不安定な `botctl_update_control` で失敗）。`src/src` の変更とは無関係（出力文言を読むコードはない）。原因は未確定。

## 2026-11-16：負荷試験失敗時の診断

- fixture で結果受信と release の時刻を記録し、acceptance 失敗時に 250 ms ごとの送信・結果・ACK、障害時刻、Worker/Gateway の stderr 末尾を上限付きで表示する。relay の実 Worker stderr も証拠に保存する。
- 原因は引き続き未確定。cargo と CI 上の実プロセス検証は未実施で、integrator が実行する。

### 2026-11-16 夕：`update_load::crash_old_core` の失敗の証拠（診断出力 50dceed の初回採取、CI）

- 結果と ACK は旧 Worker を `kill` した時刻（開始から 1933 ms）の直後にゼロになり、更新の完了（5030 ms）まで約 3 秒ゼロのまま。最初の欠落は ID 78、`errors` は空、`operation_failures` は 0。
- `gateway.log`：`--- gateway generation 1 ---` の直後に `実行に失敗しました: unresolved response outcomes prevent graceful shutdown`、続いて `generation 2`。**Gateway が終了して再起動しており、その間のイベントが欠落**した、という流れ。
- 解釈（証拠はこの 1 回の採取のみ。確定ではない）：旧 Worker を落とした時点で処理中だった応答が「結果不明」で残り、正常な終了ができないために Gateway が落ちた。結果不明を自動再実行しない既存の仕様（`docs/architecture.md` の「結果不明」）と整合する。`kill` の瞬間に処理中のイベントがあるかどうかのタイミングで再現が決まるため、WSL では再現しにくく、CI では出やすいと考えられる。
- 未判断：Gateway が「結果不明の応答が残る」ことで落ちるのが、製品として許容される動作か（別ケース `crash_new_core_after_handler` は同じ制限として `#[ignore]` 済み）。許容なら、この試験の期待値（欠落 0）か、`kill` のタイミング（処理中のイベントがない時点で落とす）を直す。許容しないなら、Gateway が結果不明を抱えたまま継続する設計が要る。

### 2026-11-16：第11回の提案を実装（実行確認は統合担当へ）

- `crash_old_core` は、送信の停止を同期して確認し、送信済み全イベントの結果と Core の受領確認を待ってから候補を起動する。Restore 待機中に旧 Worker を落とし、候補を解放して更新完了を待ってから送信を再開する。欠落ゼロは維持し、Gateway の PID・世代 1・再起動 0 を前後で確認する。停止時間を送信の追い付きバーストに変えない。
- 別ケース `crash_old_core_busy` を追加。模擬 HTTP の slot 0 向け応答を、ハンドラ処理後かつ Core への結果通知前に止め、旧 Worker を落とす。重複副作用ゼロ、不明結果の明示、Gateway の継続、切替後 20 件以上の通信成功、未解決結果による正常終了拒否を検査する。障害中の普遍的な欠落ゼロは要求せず、未解決の旧世代を抱えるためプール全体の更新成功とも扱わない。Python 側は 25 秒で打ち切る。総時間 30 秒未満と安定性は未計測のため、Rust の独立テストは `#[ignore]` として WSL／CI での確認を待つ。
- mock の prefix／slash は受領確認失敗をイベント単位の `unknown` として返し、未解決の記録を残して受信を続ける。`Manager::shutdown` の保護と実 Discord の経路は変更しない。生のエラーや秘密情報は結果へ出さない。
- cargo・ビルド・実プロセス試験は実行していない。差分、Python の構文、UTF-8 と文字化け候補を静的に確認する。コミット・push はしていない。

## 2026-11-16 夜：2 シャードの実機試験（WSL、実 Discord、別の Bot、162112b のバイナリ）

環境：WSL2、`botd --core-mode separate`、`bots-multi.toml`（`total_shards = 2`、シャード 0・1 とも同一ノード）、runtime-root は旧 Bot と別の `/var/lib/discord-rs/runtime-multi`。Bot は旧テスト Bot とは別（別 token。Oracle の放置とは衝突しない）。ユーザーが、シャード 0（サーバー 1533441257798369401）と シャード 1（1569994280854884364）の両方でコマンドを送って確認（シャード = `(ギルド ID >> 22) % 2`）。**1 回の実施で、長時間・繰り返しは未確認。**

| 項目 | 結果 |
|---|---|
| 起動 | 両シャードでログイン成功。botd 1、Core 4（シャードごとに 2）、Gateway 2。`!ping`・`!count`・`!workers` が両サーバーで返信。カウントはシャードごとに別 |
| `gateway-update`（2 シャード） | 成功（約 31 秒。シャード間の Identify 間隔 30 秒のため）。両 Gateway が入れ替わり、Core 4 本は不変、余分なプロセスなし、監査 requested→succeeded。`!count` はユーザー確認で継続 |
| Gateway（シャード 0）`kill -9` | 約 10 秒で復旧。シャード 1 と全 Core は不変 |
| Core 1 本（シャード 1）`kill -9` | その Core だけ再起動。Gateway 2 本とシャード 0 は不変 |
| Core 2 本（シャード 0）`kill -9` | 両方再起動。シャード 1 と botd は不変 |

- 発見：`botctl gateway-update` の待ちが 30 秒で、2 シャードの更新（約 31 秒）では `operation wait timed out; operation may continue` と `ok:false`、終了コード 1 を返した。操作は継続して `succeeded`。操作 ID で照会すれば成功と分かる。**改善案（未実装）**：待ち時間をシャード数に応じて延ばす（例：30 秒 ×シャード数）。
- 未確認：更新中の Core 障害を 2 シャードで、botd の `kill -9`、`systemctl stop`、3 シャード以上、長時間。`update-check.sh` は 1 シャード前提で、2 シャードでは使っていない（手動で同等の確認をした）。

### 2026-11-16 夜：`crash_old_core` の修正の検証（Astra が実装、私が検証）

- Astra の実行は途中で `code-mode host closed its stdout` のエラーで止まった（最終報告なし）。ディスクの差分を私が確認して引き継いだ（`update_load.py`・`update_load.rs`・`main.rs` のモック経路・docs）。
- 検証（WSL）：clippy 警告なし、`update_load` 3 件成功。`crash_old_core` は静止状態（送信を止め、全結果と受領を待ってから旧 Worker を落とす）に変更。新ケース `crash_old_core_busy`（処理中に落とす）は 3 回成功（各約 5.5 秒、欠落 0、重複 0、`failed_results` 1＝結果不明を明示、Gateway は継続）。`#[ignore]` を外し、CI での安定を観察する。不安定なら `#[ignore]` に戻す。
- モックの経路は、ACK の失敗を 1 件ごとの「結果不明」にして取り込みループを止めない。`Manager::shutdown` の安全装置は変えていない。本番（live）経路と separate モードは変更なし。
- 未確認：CI での連続した成功。

### 追記（2026-11-16 夜）：2 シャードの残りの項目（WSL、実 Discord、別の Bot）

- 更新中に Core を落とす：`gateway-update` の約 12 秒後にシャード 1 の Core を `kill -9`。更新は成功（約 31 秒、監査 requested→succeeded）、両 Gateway が入れ替わり、落とした Core だけが再起動、ほかの Core は不変、余分なプロセスなし、警告なし。
- botd `kill -9`：systemd が約 5 秒で再起動。Core 4 本・Gateway 2 本が入れ替わり、両シャードの古い予約を整理。**再起動後の `!count` の継続は、Discord では確認していない**（続けて停止したため。プロセス面のみ確認）。
- `systemctl stop`：0 秒、`Result=success`、残りプロセス 0、ipc に sock・secret なし、`core-state.json`（0600）は両シャードで残る。
- 試験後、WSL の botd は停止（複数シャード用の設定のまま。`/etc/systemd/system/botd.service.d/zz-separate.conf` が `bots-multi.toml` を指す。旧 1 シャード用は `/root/zz-separate.conf.single-bak`）。

### 追記（2026-11-16 夜）：`botctl gateway-update` の既定の待ち時間と、WSL 固有の既存の失敗

- `botctl gateway-update` の既定の `--timeout` を 30 秒から 300 秒に変更（他のコマンドは 30 秒のまま、`--timeout` の明示が優先）。300 秒は待ち時間の既定値で、4 シャードの完了保証ではない。サーバー側の期限（120 秒 + 45 秒 × シャード数）を過ぎても有効化の確認に最大 20 秒を使い得るほか、接続・ポーリングの時間も必要になる。長い更新には `--timeout` を明示して延長する。2 シャードの実機で、約 31 秒の更新が 30 秒の待ちで `operation wait timed out`、終了コード 1 になっていた件の対応。単体テスト追加（`default_wait_tests`）。
- WSL（この開発機）で `botctl_update_control` の `mock_daemon_binds_server_roles_and_audits_update_and_rollback` が `tests/botctl_update_control.rs:425`（`AuditedExecutor::open(..).unwrap()`）で失敗する。**今回の変更を外した状態でも 3/3 で失敗**するため、変更とは無関係で、この WSL 環境でだけ起きる（CI では成功）。原因は未調査。

### 2026-11-16 夜：`--shard N`（対象シャードの指定）の実装と検証

- 実装：`botctl start|stop|restart|gateway-update BOT --shard N [--shard M ...]`。daemon の `CommandRequest` に任意の `shards`（空・重複・65 個以上・読み取りコマンド・update/rollback への指定は不正な要求として応答なしで拒否）。**認可（role・scope）は対象のシャードだけで評価**する（シャード限定の管理者は自分のシャードだけ操作できる）。存在しないシャードの指定は黙って飛ばさずエラー。同じ操作 ID で絞り込みが違う場合は衝突として拒否。古い daemon は未知のフィールド（`deny_unknown_fields`）で拒否するため、シャード指定が無視されて全シャードが更新される事故は起きない（安全側）。監査の記録には、まだシャード指定が入らない（未実装）。
- 試験：botctl の引数の単体テスト、lib 216 件、2 シャードの統合テスト（`--shard 1` だけの更新でシャード 0 の Gateway と全 Core が不変、シャード 9 の拒否、無効な形は応答なし、同じ ID・別の絞り込みの衝突、同じ ID・同じ絞り込みの再実行）。WSL の実機（2 シャード、実 Discord）：`--shard 1` だけの更新が約 2 秒で成功し、シャード 1 の Gateway だけが入れ替わり、`--shard 9` は拒否。`!count` の継続はユーザー確認待ち。
- 未実装：`--guild ID`（シャード番号への換算）、`all`（全 Bot の順次実行）、全体の進行の表示、監査へのシャード指定の記録。

### 追記（2026-11-16 夜）：`--guild ID`（ギルドからシャードへの換算）

- 実装：`botctl start|stop|restart|gateway-update BOT --guild ID [--guild ...]`。`botctl` が daemon の `inspect` から `total_shards` を読み、`(guild_id >> 22) % total_shards` でシャード番号に換算してから、`--shard` と同じ経路（`shards` の指定）で要求する。換算の結果は応答の `resolved_guilds`（`guild_id` と `shard_id`）に出る。`--shard` と併用すると和集合。換算に失敗（`inspect` 失敗や `total_shards` なし）したら、変更を送らずに失敗する。daemon の変更はない。
- 試験：単体テスト（換算の値、不正な指定の拒否）、clippy 警告なし。WSL の実機（2 シャード、実 Discord）：`--guild 1533441257798369401` はシャード 0 だけを更新、`--guild 1569994280854884364 --shard 0` は両シャードを更新。`!count` の継続はユーザー確認待ち。
- WSL 固有の既存の失敗：`botctl_update_daemon` の 4 件が、今回の変更を入れる前の main（7a5bdcb）でも WSL で失敗する（CI では成功）。`botctl_update_control` の 1 件と同種（監査ファイルを開く処理）と見られるが、原因は未調査。
- 未実装：`all`（全 Bot の順次実行）、全体の進行の表示、監査へのシャード指定の記録。

### 追記（2026-11-16 夜）：`--all`（全 Bot の順次実行）

- 実装：`botctl start|stop|restart|gateway-update --all (--yes | --dry-run)`（`--all` は旗で、Bot 名とは別。名前が `all` の Bot と衝突しない）。`--all` は影響が大きいので `--yes`（確認）が必須で、`--dry-run` は対象の Bot 一覧だけを出して何も変更しない。`--shard`・`--guild`・`--operation-id` とは併用不可。daemon の `ps` から Bot の一覧を得て、ローカルにシャードがない Bot（`not_local`）は飛ばし、`skipped` に理由つきで出す。Bot を **1 つずつ順に**（前の Bot の操作が終わってから次へ）実行し、**最初の失敗で止める**（`stopped_at`）。Bot ごとの操作 ID と状態を `results` に出す。`--yes` は `--all` の確認にのみ使い、各 Bot へは強制停止のときだけ転送する。認可は各 Bot の要求ごとに daemon が評価する。ただし範囲限定の principal には対象なしの `ps` 自体が拒否されるため、範囲限定の `--all` は変更を送る前に失敗する。許可された Bot だけの一覧へ自動で絞る機能はない。
- 試験：botctl の単体テスト（引数の検証、対象の選別、順序と最初の失敗での停止、失敗した操作状態の扱い）。WSL の実機（2 シャード、実 Discord）：`--yes` なしは拒否、`--dry-run` は対象を表示、`--all --yes` は 34 秒で成功。複数 Bot の実機での順次実行は、Bot が 1 つの環境のため未確認（順序と停止は単体テストのみ）。

### 2026-11-16：第12回監査の8件に対応（WSL での検証待ち）

- シャードを指定した強制停止は、選択外のシャードを含む進行中の操作があれば、取り消しや restart hold の変更より前に拒否する。Bot 全体の強制停止は従来どおり。別シャードの再起動が取り消されず、hold も漏れないことを確認する単体テストを追加。
- `operation` の認可は保存された操作の対象シャードで判定する。自分のシャードの操作を照会でき、範囲外や一部しか許可されていない操作は照会できないことを単体テストへ追加。
- `--all` は bootstrap を一度だけ読み、各 Bot の処理へ渡す。結果には操作スナップショットと `shards`／`failed_shards` を残す。2 Bot の模擬制御サーバーを使うテストを追加し、再読込防止、操作 ID の分離、強制停止だけへの `yes` 転送を検査する。部分失敗の診断保持も単体テストへ追加。
- `--guild` で取得したシャード総数を `expect_total_shards` として送る。daemon は現在値と違えば操作を開始せず拒否する。読み取り命令や総数 0 への指定は不正。古い daemon は未知のフィールドとして拒否する。模擬 rolling 専用経路も、この前提条件を無視せず未対応として拒否する。妥当性・不一致時の無変更を単体テストへ追加し、2 シャードの統合テストにも wire 経由の不一致拒否と一致時の更新を追加。
- 範囲限定の `--all` が一覧取得の時点で拒否されることを上記と known-limitations に明記。`stop --all --force --dry-run` は `--yes` なしで許可するが、実行時の確認と `--dry-run`／`--yes` の排他は維持する。
- 300 秒は既定の待ち時間であり、4 シャードの完了保証ではない。最大 20 秒の有効化確認とポーリング等の余裕を考慮し、必要に応じて `--timeout` で延長する。
- 今回はコードと追加テストの静的確認のみ。cargo・ビルド・テスト・実 Discord 接続は実行せず、コミット・push もしない。実行結果は統合担当の WSL 検証後に追記する。

### 2026-11-16：第13回監査の2件に対応（WSL での検証待ち）

- `--all` の対象参照は `ps` の全一覧で検査し、名前、完全 ID の順で対象の Bot だけに解決できるものを選ぶ。非ローカル Bot との衝突も検査し、選べない対象があれば最初の変更前に Bot 名と ID を示して拒否する。`botref::resolve` の曖昧性の規則は変更していない。
- 各 Bot の変更要求に `expect_bot_id` を付け、daemon は解決後の ID が違えば操作の開始・取り消し・restart hold の変更前に拒否する。照会にはこの前提条件を付けない。ID の接頭辞衝突、名前から完全 ID への代替、計画全体の拒否、ID 不一致時の無変更と既存操作の維持、wire での前提条件の転送を検査するテストを追加・拡張した。
- 実験的な mock rolling 経路は `shards` と `expect_bot_id` も未対応として拒否する。既存の `expect_total_shards` と合わせ、botd の入口と Runtime の両方で拒否し、ジョブや操作が作られないことを検査する単体テストを追加した。
- 今回は静的確認のみ。cargo・ビルド・テスト・ネットワーク接続は実行せず、コミット・push もしない。実行結果は統合担当の WSL 検証後に追記する。

## 2026-11-16 夜（約 20:40 JST、概算）：Oracle を最新版（main e61d8d4）に更新し、放置をやり直す

- 手順：`git pull`（162112b→e61d8d4）、`cargo build --release --locked -j 2`、旧バイナリを `/opt/discord-rs/bin/*.prev` に退避、`botd` `botctl` `src` と `soak-report` を入れ替え、`systemctl restart botd`（daemon が変わったため botd の再起動を伴う）。
- 結果（ユーザー報告の出力）：botd は active、journal 警告 0、`restarts` 0、Core 2 本 `running`。`botctl gateway-update --all --dry-run` が `targets: [test]`、`skipped: []` で成功（新しい操作が本番側で使える）。ユーザーは「動作した」と報告。
- 更新前の記録：旧版（162112b、約 3.5 時間）の `gateway.log` は、新版で時刻が付いた行が `ログイン成功 … (2026-11-16T08:05:44Z)` のみで、その間の新しい close code 4000 は表示された範囲になかった。`soak-report` の `closes=11`、`login=3` と、画面の行（切断 10、ログイン 2）が合わない（数える対象が `gateway.log` 以外のログにも及ぶ可能性。原因は未確認）。旧版（17 時間 42 分）の切断は約 1.8 時間に 1 回（10 回）。**切断の頻度が新版で変わったとは言えない**（観測が短い）。
- 未確認：新版（e61d8d4）の長時間（6 時間以上）、`--all --yes` と `--shard`／`--guild` の本番側での実行、Oracle の OS 再起動後の自動起動、切断の原因（時刻付きログで操作の直後に偏るかの確認）。

### 追記（2026-11-16 夜）：Oracle での `--all --yes`、更新後のログの置き場所、cron の疑い

- Oracle（本番側）で `botctl gateway-update --all --yes` が成功（約 1.3 秒、`ok: true`、`completed: 1`、Bot `test` の 1 シャード）。結果の `results[*].operation` に操作 ID・シャードごとの状態・`failed_shards` が入る。
- **更新後の Gateway のログは `gateway-candidate.log` に出る**（`gateway.log` は更新前の Gateway のログのまま止まる）。`update-check.sh` の更新（08:06 UTC）以降の切断は `gateway-candidate.log` にあり、`soak-report` の `closes`（`gateway*.log` を合算）が 11 になり、`gateway.log` の表示（10）と食い違った理由。切断を調べるときは `gateway*.log` の両方を見る。更新を繰り返すと、`gateway-candidate.log` に複数世代のログが追記され得る（未確認）。**改善候補（未実装）**：更新の確定時にログの名前を整理する、または `soak-report`／運用ガイドに置き場所を明記する。
- cron（`/etc/cron.d/soak-hourly`、毎時 0 分）：ファイルと cron は有効だが、`soak.log` に 09:00〜11:00 UTC の定時の記録がない（手動の 08:05 と 11:41 のみ）。作成が 11:00 UTC 以降だった可能性もあり、未確認（ユーザーに権限と journal の確認を依頼）。

### 追記（2026-11-16 夜）：新版の切断の中身（Oracle、`gateway-candidate.log`、時刻つき）

- 08:06:19Z（update-check の更新）以降の候補ログ：`09:53:28Z Gateway 再接続の要求を受信` → `09:53:28Z Gateway 切断: close code 4000` → `09:53:29Z Gateway 再開（Resume 成功）`。**Discord の再接続の要求（Reconnect）に続く 4000 の切断で、約 1 秒後に Resume で復帰し、新規の Identify は使っていない**（ログインの行が増えていない）。通常の動作と見られる。旧版の 10 回の 4000 も同じ性質か（旧版には時刻・Resume の行がなく確認できない）。
- 約 3.5 時間（08:06Z〜11:45Z）で再接続の要求は 1 回。頻度の傾向や、ユーザーの操作との関係（「操作の直後に切れる」仮説）は、この 1 回では判断できない。
- 候補ログに 11:46:55Z（`--all` の更新）の世代が追記されていることを確認（更新ごとに追記される）。botd の再起動後の Gateway（11:45:34Z）は `gateway.log` に出る。
- cron の毎時の記録（09:00〜11:00 UTC）が `soak.log` にない件は、ファイル（0644、root、08:07 作成）に問題はなく、原因は未確認（調査中）。

### 追記（2026-11-16 夜）：cron の記録の場所の取り違え（解決）

- cron（`/etc/cron.d/soak-hourly`）は動いていた（journal に 09:00・10:00・11:00 UTC の `soak-report` の実行）。毎時の記録は `/home/ubuntu/soak.log` に溜まっていた。ユーザーが `sudo su` の root で `~/soak.log` を読んだため `/root/soak.log`（手動の記録だけ）を見ていた。私の案内が `~` を使っていたのが原因。手順書（`docs/soak-test.md`）と `deploy/dev/soak-diff.sh` の既定を、絶対パス `/home/ubuntu/soak.log` に直した。

### 追記（2026-11-16 夜）：Oracle 放置（162112b、約 3 時間）の毎時の記録

`/home/ubuntu/soak.log` の 09:00・10:00・11:00 UTC（botd 起動 08:05:43 UTC からの 54 分／1 時間 54 分／2 時間 54 分）。出典：ユーザーが貼った記録。
- RSS（KiB）：botd 8,176→8,220→8,228、Core 6,248→6,252→6,252／6,232 のまま、Gateway 10,796→11,136→11,208。fd は botd 16・Core 各 11・Gateway 12 で不変。再起動 0、警告 0、`worker_unavailable` 0。Gateway は最初の 1 時間で約 400 KiB 増え、その後は小さい増加（傾向は 3 点のみで判断できない）。
- `closes` は 10→11（09:53Z の Reconnect 要求＋Resume 成功の 1 件）。
- 11:45Z に botd を最新版（e61d8d4）へ入れ替えたため、この記録は旧世代（162112b）の分。最新版の放置は 11:46:55Z（`--all` の更新）から。

### 2026-11-16 夜：更新後のログを 1 つの `gateway.log` に（work/next）

- 変更：待機（候補）Gateway の出力先を `gateway-candidate.log` から、そのシャードの `gateway.log`（追記）に変更。旧 Gateway と新 Gateway は更新の重なりの短い間だけ、同じファイルに追記する（追記モードなので 1 回の書き込みは壊れないが、書式付きの出力は複数回の書き込みになり得るため、行の途中で混ざることはあり得る。行単位の保証はしない。時刻が入る）。これで運用者が `gateway.log` を読めば、常に現在の Gateway の出力が見える。旧版が作った `gateway-candidate.log` は、残っていてもローテーション（8 MiB）の対象とし、`soak-report` も合算して読む。
- 試験：統合テスト `gateway_update_replaces_the_gateway_and_keeps_the_cores` に「更新後に `gateway-candidate.log` が作られず、`gateway.log` が 1 つだけ」の確認を追加。**変更を外すとこのテストが失敗することを確認**（`a separate candidate log was created`）。WSL で clippy 警告なし、統合テスト 2 回成功、lib 223 件成功。実 Discord・Oracle での確認は未実施（次回の更新で確認）。
- 運用者ガイド（`docs/operator-guide.md`）に、`--shard`／`--guild`／`--all`、既定のタイムアウト、障害時の確認先、バイナリの復旧を追加（Terra が作成、私が内容をコードと照合）。

### 2026-11-16 夜：B3 の観測、update-check.sh の複数シャード対応、WSL 固有のテスト失敗の解消（work/next）

- **B3 の観測（第一段階）**：Core が、30 秒以上完了しない要求を、標準エラーに 1 行（`要求が 30 秒以上完了していません（経過 N 秒、未完了 M 件）`）で出す（同じ要求の再警告は 5 分以上空ける）。**要求は打ち切らず、ドロップも再試行もしない**（ハンドラが状態を持つため打ち切りは危険、という既知の理由）。内容・ID・秘密は出さない。IPC は変えていない。試験：判定（時刻を注入）の単体テスト 2 件と、実時間の短い値で本物の `select!` を通すテスト 1 件（遅い要求は 1 回だけ警告され結果を返す、速い要求は警告されない、完了後は保留 0）。実際の stuck の再現と実機での確認は未実施。
- **`deploy/dev/update-check.sh`**：N シャードに対応（`botctl inspect` で shard 数を取り、Core が 2×N・Gateway が N あることを確認、更新後は全 Core 不変・全 Gateway 入れ替え、シャードごとの IPC）、`--shard N`（対象の Gateway だけが変わり他は不変）、`ROOT=...` で runtime のルートを上書き可。WSL（実 Discord、2 シャード）で、全更新と `--shard 1` の両方が全 PASS。Core を落とした直後・存在しないシャード・`--repeat 0` は FAIL／拒否されることを確認。Ubuntu の Oracle での実行は未確認。
- **WSL 固有の失敗の原因と解消**：`botctl_update_control`（1 件）と `botctl_update_daemon`（4 件）が WSL でだけ失敗していたのは、テストが一時ディレクトリを `CARGO_MANIFEST_DIR/target`（WSL では NTFS の `/mnt/d` 上で、Unix の権限が効かず 0600 の検査が `Io` エラー）に作っていたため。cargo の `CARGO_TARGET_TMPDIR`（`CARGO_TARGET_DIR` の下）を使うように変更し、WSL でも 9 件と 6 件が全て成功。本番のコードの問題ではない。

### 2026-11-16 夜：Astra 第 15 回（6 件）への対応と、Haiku による文書の矛盾の洗い出し

- B3 の警告の出力は、標準エラーが書けなくてもパニックしない最善努力の書き込み（`writeln!` の結果を捨てる）に変更（満杯のログファイルで観測の future が巻き戻り、要求を放棄する事故の防止）。観測のテストは、要求を「警告が出るまで保留」にして、固定の 300 ms への依存をなくした（5 回連続成功）。
- `update-check.sh`：空・数字でない・重複の `--shard` を終了コード 2 で拒否（以前は空だと全シャードの更新になった）。ローカルのシャード（`inspect` の processes）を基準に検証し、他のホストが持つシャードに引きずられない。WSL（実サービス）で、拒否の 3 種、範囲外（FAIL）、`--shard 0`（全 PASS）を確認。
- 文書：「行単位で混ざる」の保証を削除（書式付きの出力は複数回の書き込みになり得る）。「2 シャードは実 Discord で未検証」と読める記述を、記録（WSL の実 Discord で 1 回確認、長時間・繰り返し・3 シャード以上は未検証）に合わせて訂正（known-limitations、operator-guide、handoff、real-separate-verification、next-steps、soak-test）。`~/soak.log`・相対パスの記述を `/home/ubuntu/soak.log` に。Haiku の洗い出し 10 件のうち、9 件は正しく反映し、1 件（`--all --yes` の Oracle での成功を WSL と取り違えたもの）は事実と合わないため反映しなかった。
- 並行の作業（別 worktree、未統合）：`work/flake`（botctl 系テストの stdin の BrokenPipe の許容。WSL で 12 回成功。CI の確率的な失敗への効果は未確認）、`work/workers`（`!workers` を返す前に Core 接続を確認し、fence 済みの旧 Gateway は返信しない。Core が 2 秒で応答しないときは従来どおり返信。実 Discord での確認は未実施）。
### 2026-11-16 夜：`!workers` を、fence 済みの旧 Gateway が返信しないようにする小さい代案（work/next）

- `!workers` に返信する前に、各 Core 接続へ `Health` を送り、`Ready` なら返信、エラーなら返信しない（fence・切断・通常の障害を区別しない）。ロックの取得に 2 秒の期限があり、取れなければ従来どおり返信する。取得後の交換はキャンセルしない（Astra 第 17 回の指摘 1：期限内の途中キャンセルは接続を壊す）。停止中は返信しない、embedded は従来どおり。
- **保証ではない**：fence 前の二重返信、確認と HTTP 送信の間の競合、Core の通常の障害中の無返信、タイムアウト時の返信は残る。exactly-once ではない。単体テストと `core_attach` の統合テストのみで、実 Discord での更新中の確認は未実施。
- 経緯：この節と `docs/design-workers-dedupe.md` の追記は、Codex（Terra）が日本語を `?` に化かしていたため、私が書き直した（第 17 回の指摘 2。`grep four question marks` の検査範囲に別 worktree の文書が入っていなかった見落とし）。

### 2026-11-16 22 時：main = 6b7c559（Astra 第 18 回で指摘なし、CI green）

- 統合：更新後のログを `gateway.log` に統一／B3 の観測（30 秒以上の要求を警告。打ち切りなし）／`update-check.sh` の複数シャード・`--shard`・ローカルシャード対応／WSL 固有のテスト失敗の解消（`CARGO_TARGET_TMPDIR`）／botctl 系テストの stdin の BrokenPipe の許容／`!workers` を、fence 済みの旧 Gateway が返信しないようにする小さい代案（ロック取得のみ 2 秒の期限、交換は最後まで）／運用者ガイドと各文書の訂正。
- 監査：第 15 回 6 件、第 16 回 2 件、第 17 回 2 件を修正し、第 18 回で指摘なし（静的な監査。実 Discord の更新中の `!workers` の確認は未実施）。
- 検証：clippy 警告なし、lib 227 件、separate 5 件、2 シャード 3 件、`core_attach` 25 件、botctl 系 15 件（WSL、すべて成功）、Rust CI・systemd smoke green。
- **Oracle は e61d8d4 のまま**（放置は 11:46:55Z＝20:46 JST から）。main の新しい変更（ログ統一、B3、`!workers`、スクリプト）は Oracle に入っていない。放置の結果を見てから更新する。
- 未実施：新版（e61d8d4 以降）の 6 時間以上の放置結果、Oracle の OS 再起動後の自動起動、`!workers` の更新中の実機確認、Interaction の重複、複数 Bot の実機。

### 2026-11-16 夜（最後）：設計判断のフォルダ `docs/design-decisions/` を作成

- 構成：`README.md`（索引）、`decisions-needed.md`（ユーザーの判断が必要なもの D1〜D8 と私の推奨）、`unverified-items.md`（未確認・不明のもの、確認方法、担当）、テーマ別の設計・対応案 `01`〜`08`（止まったハンドラ、コピーで作業してコミットする案、Core の無停止差し替え、複数ホストの準備、Identify と Resume、botd の死亡と再採用、状態の保存、結果不明の外部操作）。
- 「確認済み」（コード・実機・テストの記録に出典あり）と、「私の記憶・一般知識で公式の仕様や実装で未確認」（Discord の Identify の制限、discord.py の再試行、nonce と enforce_nonce）を分けて書いた。すべて未実装・未検証の案であり、実装済みのもの（B3 の観測など）は、そう明記した。

## 2026-11-17：Oracle 放置（最新版 e61d8d4、11-16 11:46:55Z 開始）の切断の記録

出典：ユーザーが貼った `gateway.log` と `gateway-candidate.log`（時刻つき）。**メモリ・fd の傾向は、`soak-diff` を更新後に絞って再実行した結果を待っている**（下記の取り違え）。

- cron の毎時の記録は、2026-11-16T22:00Z から 2026-11-17T09:00Z まで途切れず 12 回以上溜まっている（`/home/ubuntu/soak.log`）。放置は約 22 時間（11-16 20:46 JST 開始、11-17 18:36 JST 時点）。
- **切断と復帰（約 21 時間 50 分、ログインは 11:46:55Z の 1 回のみ）：**
  - Discord の再接続の要求（Reconnect）→ close code 4000 が **11 回**（12:08、12:46、13:44、16:08、21:25、23:26、00:02、01:37、04:38、07:06、09:33）。すべて Resume 成功（0〜1 秒）。
  - 切断の行なしで Resume 成功のみが 1 回（16:43:32Z）。原因は不明（ライブラリが静かに再接続した可能性。未確認）。
  - close code **1006**（異常終了＝接続の途絶）が 1 回（17:55:24Z）。同じ秒に Resume 成功。
  - **新規の Identify は 0 回。** 合計 13 回で、約 1.7 時間に 1 回。間隔は 35 分〜3.5 時間でバラバラ（周期的でない）。旧版（17.7 時間で 10 回）とほぼ同じ頻度。
- 切断の時刻を JST に直すと、夜中や朝（06:25、08:26、09:02、10:37 など）にも起きており、**ユーザーの操作と無関係の可能性が高い**（ユーザーの操作の時刻との突き合わせは未了）。
- **`soak-diff.sh` の取り違え：** ユーザーが実行したのは、Oracle の古い checkout（e61d8d4）の `soak-diff.sh` で、既定の `$HOME/soak.log`（root では `/root/soak.log`）を読み、手動の 5 件（11-15 13:10 〜 11-16 11:41）だけを比べた（今回の放置の毎時の記録は含まれず、更新をまたぐ `restarts`・`generations`・`closes` の WATCH はすべて見かけの差）。対策：`soak-diff.sh` の既定を絶対パスにし（先に対応済み）、更新後だけを比べる `SINCE` 引数を追加（a2434af）。

### 追記（2026-11-17）：Oracle 放置（最新版 e61d8d4、約 21 時間）の `soak-diff`（更新後のみ、22 件）

出典：ユーザーが貼った `soak-diff.sh /home/ubuntu/soak.log 2026-11-16T12:00` の出力。
- **botd と Core**：RSS は botd 7,872→7,960 kB（+88、+4 kB/h）、Core 6,380→6,404／6,356→6,368 kB（約 +1 kB/h）。fd は 16／11／11 で不変。PID は不変、`NRestarts` 0、`worker_unavailable` 0、journal の警告 0。**メモリ・fd・再起動の面では、増加の傾向なし。**
- **Gateway の行は出力になかった**（`soak-report` がコマンドを 90 文字で切るため、更新後の Gateway では `--core-dir` が切れて Gateway と判定されなかった）。**Gateway のメモリ・fd は、この結果に含まれない。** 以前の個別の記録では、Gateway の RSS は 10.2〜11.4 MB（fd 12〜13）で、傾向は小さかった（旧世代の 3 点のみ）。
- `files: WATCH`（`gateway-candidate.log` が 573→2,452 バイト、+1.9 KB）と `closes: WATCH`（11→22）は過剰な警告だった。前者は切断のたびに数行増えるだけの小さい増加、後者は Discord の Reconnect 要求で、増えた切断はすべて Resume で復帰（直前の記録）。
- ツール側の修正（main 7d7f844）：Gateway を `--shard-id` でも判定／小さいログの増加（100 KiB 未満）は警告しない／`soak-report` に `resumed=`（Resume 成功の件数）を追加し、`soak-diff` は「増えた切断がすべて Resume で復帰」なら `closes` を OK にする（`resumed` を含む記録が 2 件以上たまってから有効。古い形式の記録では従来の規則）。合成のログ 4 種で、OK／WATCH／従来の規則／大きな増加の判定を確認。Oracle への反映と再実行は、ユーザーの出力待ち。
- **「シングルの完成条件」の 1（最新版の放置 6 時間以上、再起動 0、メモリと fd に増加なし）：** botd と Core については満たしている。Gateway は、再実行の結果で確認する。

## 2026-11-17 P1（停滞の診断）の実装を、ブランチ work/p1 で完成（main へは統合前。Astra の再監査の結果次第）

詳細な仕様・保証の範囲・未対応は `docs/known-limitations.md` の「P1 停滞診断」に書いた。ここは、作業の記録。

- **経緯：** Astra と Sol の設計レビューと、私の実測（ディスク上の状態ファイルは、アイドルの Core 300 秒で書き込み 84 回・ディスクへ 98,304 バイト）により、DDR-0007 で tmpfs 方式に決まった。Terra が段階 1（`core_status` モジュール）、2・2b（Core の書き込み、tmpfs、単調時計、世代、連番）、3（botd の読み取り、`status` への表示）を実装。大きな依頼は「未完了」と報告されたので、小さい段階に分けた。
- **Astra 第 19 回の監査（7 件）と対応：** 書き込み側の記述子への `fstatfs`、読み取り側の tmpfs・記述子の確認、時計の失敗と未来の時刻、世代・連番の検証（botd が Core の起動ごとの識別子を決める、古いファイルの削除、後退の検出）、tmpfs の swap の保証の限定、試験専用の旗を debug のみに、`O_NOFOLLOW` を `libc` の定数に（aarch64 で誤りだった）。
- **Astra 第 20 回の監査（6 件）と対応：** (1) **診断をオプトインに**（`--core-status-dir` の明示指定のみ。`/run/discord-rs` があるだけでは有効にしない。botd だけを先に更新すると、古い Core が新しい引数を拒否して起動・復旧が壊れるため）、(5) 乱数の取得失敗は、診断を無効にして Core を通常どおり起動（診断が復旧を止めない）、(4) ユニットテストが debug 専用の関数に依存していた点を修正（`cargo test --release --lib` が通ることを確認）、(6) 文書を現状に合わせて書き直した。**対応しない（既知の限界として文書化）：** (2) 作成時の、親の記述子を固定した相対作成、(3) 拒否したファイルの確認と削除の隙（どちらも、サービスのユーザー自身による意図的な差し替えが必要）。
- **検証（私が実行）：** clippy 警告なし。lib 236、botd 22、`botd_separate_cores` 8、2 シャード 3、`core_attach` 28 が成功（debug、WSL）。リリースビルドに警告なし、リリースの `src`・`botd` が試験専用の旗を拒否することを確認。
- **実機に近い確認（WSL、リリースビルド、実 Discord の別 Bot、2 シャード、4 つの Core）：** 全 Core が `pending=0`・`suspected_stalled=False`・`seq` が進む。Core 1 本への `SIGSTOP` の 40 秒後に、**その Core だけ** `suspected_stalled=True`、`SIGCONT` で回復。Core の `SIGKILL` の後、botd の再起動で**世代が変わり**、`stale_generation`・`regressed` は出ない。ディスクへの書き出し `write_bytes` は 0。残りのプロセス 0。
- **未確認：** Oracle・実サーバーの systemd（`RuntimeDirectory`）、実際の長時間運用、本物の「固まったハンドラ」での検知（`SIGSTOP` による Core 全体の停止で代用した）、Astra の第 21 回の再監査。
- **私の誤り（記録）：** 試験スクリプトで `/tmp` の権限を `0755` に変え、WSL の一般ユーザーがコンパイルできなくなった（`1777` に復旧。本番に影響なし）。`cargo` を root で実行して失敗した（一般ユーザーで再実行）。
- **気づき（推測、未確認）：** botd をセッションの終了で強制終了した試験（systemd の外）で、Gateway が 2 つ取り残された。systemd の制御グループ下では起きにくいと考えているが、確認していない（`06-botd-supervision.md` の論点に近い）。

## 2026-11-17 Oracle 放置の最終数値（e61d8d4 版、25.0 時間）
- 実測（`soak-diff`、2026-11-16T12:00Z〜11-17T13:00Z、スナップショット 26 件）：botd RSS +4 kB/h、Core 2 つ +1 / +0 kB/h、**Gateway RSS 10256→10952 kB（+696 kB、+28 kB/h）**、fd はすべて増減 0、NRestarts 0、journal の警告 0、世代 3 のまま、worker_unavailable 0。判定は RSS・fd・再起動・ファイル・世代・警告が OK。
- `closes` は 11→24（判定 WATCH）。増分 13 件は、先の分析（Resume のみ、Identify 0、平均約 1.7 時間間隔）と件数が一致する。ただしこの版のログには `resumed=` がなく、今回の集計では Resume か Identify かを**確認していない**（先の分析は個別ログの読みによる）。
- Gateway RSS は +28 kB/h と微増。25 時間で +0.7 MB であり、リークの断定も否定もできない（要・さらに長い観測）。
- **未確認のまま：** Oracle は最新 main（ログ統一・B3・`!workers`・P1）を受け取っていない。OS 再起動後の自動起動は未検証。

## 2026-11-17 P3（手動復旧）の設計案を作成
- `docs/design-decisions/p3-manual-core-recovery-design.md`（**設計案のみ。未承認・未実装**）。`botctl core restart`（仮名）：対象を incarnation id で固定、`admin`＋shard の scope、シャード単位の排他（更新中は拒否）、スロットごとの回数制限（60 秒・15 分に 3 回、初期値案）、「最後の稼働中の Core」の確認、要求を先に監査に記録、botd 自身の `Child` のハンドルで終了（外部の PID 指定の `kill` はしない）。
- 保証しないこと（文書に明記）：強制終了した Core の処理中イベントは完了しない、未受領の REST は結果不明になり得る、再起動後の重複排除の記憶は空。
- **未確認：** 停滞した Core が stdin の EOF に反応するか（猶予の長さ）、既存の `operation` 照会に載せられるか。実装・試験・実機検証は未着手。
- 決めてほしい項目は設計書の 9 章（D-P3-0〜5）。

## 2026-11-17 Oracle を新版へ入れ替え（ユーザーが実施。新版の放置の起点）
- 入れ替え：Oracle で `31ee55e`（コードは 76e72ce 以降の main。ログ統一・B3・`!workers`・P1 を含む）を `--release --locked -j 2` でビルドし、`botd`・`botctl`・`src` を `/opt/discord-rs/bin/` に配置、`systemctl restart botd`。旧バイナリは `*.prev` に退避済み（戻し方は `docs/soak-test.md` §5.1）。**botd の再起動時刻 2026-11-17T13:50:42Z（新版の放置の起点。`soak-diff.sh` の SINCE は `2026-11-17T13:51`）。**
- 再起動 15 秒後（ユーザーの出力）：`active`、NRestarts 0、botd＋Core 2＋Gateway 1 が起動、Core の failures 0、journal の警告 0。
- P1（`--core-status-dir`）は**有効にしていない**（オプトイン）。
- **未確認：** Discord での `!ping`・`!count`・`!workers` の応答（ユーザーの確認待ち）、新版の 6 時間以上の放置、OS 再起動後の自動起動。

## 2026-11-17 Oracle 新版の放置 8 時間の結果（ユーザーの出力。起点 13:51Z、最終 22:00Z）
- 実測（`soak-diff`、スナップショット 9 件、8.0 時間）：botd RSS +12 kB/h、Core 2 つ +0 / +1 kB/h、**Gateway RSS 11296→11428 kB（+16 kB/h）**、fd は全プロセスで増減 0、NRestarts 0、世代 4 のまま、worker_unavailable 0、journal の警告 0。判定はすべて OK（`closes` も OK）。
- `closes` 25→27（+2）、`resumed` 18→22（+4）。**両者の増分が一致しない理由は未確認**（集計の対象が異なる可能性。ログの個別確認はしていない）。Identify が増えたかは、この数値からは断定しない。
- 「新版の放置 6 時間以上、異常なし」の条件は満たした（1 Shard・1 Bot・Oracle のみ）。`!ping`・`!count`・`!workers` の Discord での応答は、ユーザーの確認結果がまだ記録されていない。
- **未確認のまま：** OS 再起動後の自動起動（Oracle は「System restart required」。カーネルの更新待ち）、`!workers` の更新中の実機確認、Interaction の重複、複数 Bot・3 Shard 以上の実機、P1 の Oracle での有効化。

- **2026-11-18（ユーザーの報告）：** Oracle 新版での Discord のコマンドは動作した（ユーザーが「コマンドは行けた」と報告。どのコマンドか個別には記録されていない。`!workers` の内容と、更新中の挙動は別途未確認）。

## 2026-11-18 P3 stage A の独立監査への対応
- `work/p3-sol` の `378720d` と `c1a3dd0` を対象に 10 件を再確認。P1 と独立した世代 ID、空スロットの明示、監査待ちからの予約、EOF 前の peer 再確認、バックオフ、停止中の置換防止を実装した。
- 手動の置換起動を監督ループ外に移動。未回収の子を保持し、終了が不明なら `termination_unconfirmed` を返す。監査ログから最短間隔・回数と操作照会を復元し、完了記録が欠けた要求は `unknown` として再実行を防ぐ。
- スコープ付きプレビューと監査状態を追加し、直接子の保証、起動成功の意味、ログ・時計・I/O の前提を文書化した。
- 回帰テストを追加。Windows ネイティブの `cargo check --locked --offline --tests` は対象外 OS の既存の条件付きコンパイルエラーで停止。Ubuntu/WSL のテスト、実際の botd 強制終了をまたぐ再起動、systemd と実 Discord の検証は保留。

## 2026-11-20 P3 stage A の第2回独立監査への対応

- Gateway の稼働を受け付け時と EOF 前に確認する。監査応答が 5 秒で届かなければ予約を解除し、応答が後で届いても復旧を開始しない。非 force の強制終了前に peer を再確認する。置換を取り消した後に起動した直接子は capability を渡さず終了・回収する。
- 起動中の監査再生と Gateway 更新監査を監督ループ外で行う。P3 の新規操作は再生完了まで拒否する。通常停止では同期待ちの取消結果を記録し、制御監査の停止を最大 2 秒待つ。監査の可用性と記録失敗・欠落数を status に追加した。
- 回数制限の記録に起動 ID と単調時刻を追加した。履歴の矛盾と不正な完全行は新規操作を停止し、判別できた ID は予約する。操作要求と結果は Core 復旧プロトコル v2 で検証する。
- 新規回帰テストを追加。Windows 上で rustfmt、差分・UTF-8 検査を実施。Ubuntu／WSL の cargo ビルド・テスト、実 botd のプロセス喪失、systemd と実 Discord の復旧は未検証。P3 の終了管理範囲は直接子。専用 CLI の実装時期と復旧専用 role は PM 確認待ち。

## 2026-11-20 P3 stage A の第3回独立監査への対応

- 監査再生中は操作 ID 付きの変更を再試行可能な応答で保留し、Gateway 更新は同期前から ID を予約する。Core の ID 競合は認可経路で拒否記録を作り、書き込みの確認まで監査の未処理件数に含める。
- peer 喪失で SIGKILL を見送った直接子への再試行を可能にした。置換起動の 5 秒期限でシャードの予約を解き、通常停止では取り消した起動の回収を最大 2 秒待つ。期限後や強制終了時の残存リスクは既知の制約に記載した。
- 乱数の取得に失敗した場合も daemon の ID と起動連番から代替の世代を割り当てる。読み取りの 3 秒期限を維持し、認証された監査待ちの応答期限を 8 秒にした。
- 認証済み制御経路、実子プロセス、監査の応答待ちと再生を対象とした回帰テストを追加・強化した。Windows 上の rustfmt、差分、UTF-8 を確認。Ubuntu／WSL の cargo ビルド・テスト、実際の botd 強制終了、systemd、実 Discord は未検証。

## 2026-11-20 P3 stage B の運用 CLI と結合テスト

- `botctl core status` で 1 シャードをプレビューし、`botctl core restart` で確認した世代または空スロットを固定する。非対話入力では `--yes` を必須にした。操作 ID と v2 を付け、応答切断後も同じ ID を照会する。Stage A が監査同期前後で同じ理由を返す場合も、照会できた終端結果を優先する。
- 実 botctl と botd、模擬 Discord を使う結合テストを追加した。tmpfs 上の診断を有効にし、SIGSTOP された Core の置換と peer の PID、診断の正常化、監査、古い世代の拒否、権限、回数制限、Gateway 更新時の競合、正常 Core の `--force` を検査する。非対話確認の拒否と旧 daemon の切断も対象にした。
- Windows 上で変更した Rust ファイルの rustfmt、差分、UTF-8 を検査。Ubuntu／WSL の cargo ビルド・テスト、対話端末での確認、実際の botd 強制終了をまたぐ照会、systemd、実 Discord での処理再開は未検証。

### P3 stage B の第4回レビュー対応

- botd は受理前の拒否を明示し、監査処理後に確定した失敗を操作結果付きで返す。旧 v2 応答に対して照会で受理の有無を確認できない場合は CLI が `unknown` を返す。送信直前に操作 ID を標準エラーへ出し、手動の操作照会にも Core の出力制限を適用した。監査再生での再試行は操作情報を含まない `ok: false` 応答に限定した。引数エラーの JSON 形式は運用手順に明記した。
- 停滞試験では疑似端末の確認入力を保留し、Core の置換後に同じプレビューを送信して世代不一致の監査と置換 PID を検査する。この試験は CI で `/dev/shm` の tmpfs を必須とする。診断を利用しない試験には通常の一時領域を使い、機密値の検査は現在の試験用 secret の 16 進表現とバイト配列を対象とした。
- Windows 上で変更した Rust ファイルの rustfmt、差分、UTF-8 を検査。Ubuntu／WSL の Cargo ビルド・テスト、実際の botd 再起動をまたぐ照会、systemd、実 Discord は未検証。

## 2026-11-20 P3（Core の手動復旧）を統合（経過のまとめ）
- **統合したもの：** `botctl core status` / `botctl core restart`（仮名）と、botd 側の `core-preview` / `core-restart`。対象は incarnation id で固定、`admin` ＋ shard の scope、シャード単位の排他、スロットごとの回数制限（間隔 60 秒・15 分に 3 回）、「最後の稼働中の Core」の確認、要求を先に `core-recovery-audit.jsonl` に記録、botd 自身の `Child` で終了（猶予 5 秒の後に強制終了）。DDR-0008 の方針どおり。
- **進め方：** Sol（`gpt-6.2-sol`）が実装し、Astra（`gpt-6.2-astra`）が 4 回監査（第 1 回 10 件、第 2 回 10 件、第 3 回 8 件、第 4 回 8 件）。指摘は実コードで確認して修正し、私が WSL で fmt・clippy・試験を検証した。第 4 回に High はなかった。
- **検証：** WSL で fmt・clippy 問題なし、lib 272、bin 14 と 24、`botd_separate_cores` 16、`shards` 3、`core_attach` 28、`botctl_update_*` 9+6、`tests/botctl` 22。CI は `4f25f23` で Rust CI・systemd smoke とも成功。
- **観測した不安定さ（原因は未調査）：** `a_core_crash_during_a_gateway_update_leaves_a_working_system` が、全体を回した 1 回だけ時間切れで落ちた（その後の全体 4 回と単独 5 回は通過）。約 1/8 の頻度の可能性。
- **未検証：** 実 Discord での復旧、botd が実際に落ちる場合、systemd 下の挙動、対話式の確認の実機操作、OS 再起動をまたぐ回数制限の復元の実機。「復旧できる」とは、実機で停滞した Core を復旧して `!count` が続くことを確認するまで書かない。
- **決定済みの制約（#9）：** P3 が管理するのは Core の直接の子だけ。Core が起こした補助プロセスは管理しない（`docs/known-limitations.md`）。基盤が補助プロセスの起動 API を提供する案（`PDEATHSIG` ＋同一プロセスグループ。botd を子孫の回収役にする案、cgroup の案）は、将来の拡張として未設計。PID の自己申告での登録は、PID の再利用と、Core が任意の PID を指定できる危険のため採らない。
- **Oracle：** 新版（31ee55e のコード）で放置中（起点 2026-11-17T13:51Z、8 時間で異常なし）。P3 は Oracle にまだ入れていない。
- **Codex の運用：** 6.2 系（`gpt-6.2-sol`、`gpt-6.2-astra`）を使用。6.2 に Terra はないため 6.1 のまま。Sol の実装は Terra より完成度が高かった。Codex のサンドボックスからは WSL が使えないため、ビルドと試験は私が実行する。

## 2026-11-21 Oracle：OS 再起動後の自動起動を確認、新版の放置 86 時間（ユーザーの出力）
- **OS の再起動（ユーザーの出力）：** `uptime -s` は 2026-11-17 22:23:45（カーネル 7.0.0-1013 → 7.0.0-1016）。botd は 22:23:52 に起動（OS 起動の 7 秒後。systemd の unit は enabled）。再起動の直前（22:10）のログイン画面に「System restart required」が出ており、時刻の並びから、ユーザーが更新のために再起動した可能性が高い（**ユーザーに確認していない**）。
- **自動起動：** 再起動後、botd・Core 2・Gateway 1 が自動で立ち上がり、以後 78 件のスナップショットが連続した。ユーザーの報告では Bot は動作している（どのコマンドかは未記録）。これで「OS 再起動後の自動起動」は、**実機（Oracle、1 Shard・1 Bot）で一度確認できた**。再起動中に実行中だった処理の扱いや、複数 Bot では未確認。
- **放置の数値（起点 2026-11-17T13:51Z、86.0 時間、87 件）：** botd RSS +3 kB/h、Core −1 kB/h ×2、Gateway +1 kB/h、fd 増減 0、worker_unavailable 0、journal の警告 0。`closes` 25→56、`resumed` 18→61、`generations` 4→5（再起動で Gateway が立ち直った分と考えられる）。**注意：** この集計は再起動をまたぐので、RSS の増加率は成長の指標として使えない（プロセスが入れ替わっている）。再起動後だけの数値は `soak-diff.sh ... 2026-11-17T22:24` で取り直す。`closes` と `resumed` の増分の不一致は、引き続き未調査。
- 新版（P3 なし、31ee55e のコード）の放置は、再起動後の約 78 時間、異常の報告なし。P3 は Oracle にまだ入れていない。

## 2026-11-21 Oracle：再起動後だけの放置 77 時間（ユーザーの出力。2026-11-17T23:00Z〜11-21T04:00Z、78 件）
- **全プロセスが同一**（PID が変わっていない）：botd RSS 7372→8008 kB（+8 kB/h）、Core +0 kB/h ×2、**Gateway RSS 10292→11392 kB（+1100 kB、+14 kB/h）**、fd は全プロセスで増減 0、NRestarts 0、世代 5 のまま、worker_unavailable 0、journal の警告 0。判定はすべて OK。
- Gateway RSS は、これまでの 3 回の観測（+28 kB/h／25 時間、+16 kB/h／8 時間、今回 +14 kB/h／77 時間）で、いずれも微増。**増加率は観測が長いほど小さい**（傾向。有限の観測からの推測で、頭打ちとは断定しない）。別のプロセスの終端値が 11.4 MB 付近でそろっている点（今回 11392、前回 11428）も、上限がある可能性を示すが、リークの否定にはならない。さらに長い観測で、頭打ちか直線的な増加かを見る。
- `closes` 27→56（+29、約 0.38 回/時）、`resumed` 22→61（+39）。**`resumed` の増分が `closes` より大きい理由は未調査。** Identify が増えたか（`generations` は 5 のまま）は、世代の数からは増えていない。
- 単一ホスト完成の条件（D2）の状況：新版の放置 6 時間以上（達成：77 時間）、OS 再起動後の自動起動（達成）、B3・`!workers`・ログ統一の Oracle への導入（達成、31ee55e のコード）。**残り：** `!workers` の更新中の実機確認、Interaction の重複の確認、P3 の Oracle への導入と実機確認、複数 Bot・3 Shard 以上の実機。

## 2026-11-21 並行開発の統合（Identify・nonce・小項目）と実機プローブ
- **nonce の実機プローブ（ユーザーの許可を得て実施）：** 試験専用チャンネルで、同じ author・同じ nonce・`enforce_nonce=true` の Create Message を、0.4 秒後と 30.9 秒後に再送した。どちらも **同じメッセージ ID** が返り、別の nonce では別の ID が返った。作成した 2 件は削除した（HTTP 204）。API が返した ID による判定で、チャンネルの見える投稿数は目視していない。**確認できた保持期間は 30 秒まで**（Discord の記述は「過去数分」）。token は WSL の `/etc/discord-rs/botd.env` から環境変数に読み込み、画面にもログにも出していない。
- **統合したもの（すべて CI で Rust CI・systemd smoke が成功）：** `work/small`（Gateway ログのミリ秒表記、文書の訂正、`soak-diff` の集計の明確化、試験のプロセス照合の修正）、`work/nonce` と `work/nonce2`（nonce 再試行を **既定オフ**、`--enable-create-message-nonce-retry-for BOT_ID` の Bot ごとの許可リスト、受信済みエラーの本文が切れた場合は再試行しない）、`work/id`（すべての Gateway 起動を Bot ごとの同じ予算に統一、`restart` は停止の前に枠を予約し、枠がなければ何も止めず拒否、複数 Shard は全部通るか全部止めるか、`restart --all` は 1 つの操作 ID。botctl と botd のバージョン一致が必要）、`work/grant`（送信時の Identify 許可。`--identify-send-grants` で有効、**既定は無効**）。
- **PM の判断（ユーザーに確認済みの方針の範囲）：** 送信時の許可のために、Twilight の `twilight-gateway` 0.18.0 を `src/vendor/` に取り込んで改造した（送信の直前に 2 秒の期限と 1 回限りを確認）。ユーザーに「1（採用）かなあ」と相談し、PM に任せるとの方針で採用した。**代償：** Twilight の更新（脆弱性の修正を含む）を手で取り込む必要がある。歯止め：変更を最小に保つ、`vendor/twilight-gateway/UPSTREAM.md` に元の版と差分を記録、取り込んだコピーが「crates.io の 0.18.0 ＋ 私たちの差分」であることを確認する検査を追加する（**検査は未実装**）。
- **Sol(6.2) との設計議論（`review-2026-11-21-sol-discussion.md`）：** Identify の送信時の調整を完成の条件にする、nonce の Bot ごとの許可リスト、補助プロセスは `PR_SET_PDEATHSIG` を採らず cgroup 案、完成の定義に基準を足す。
- **WSL の検証での既知の失敗（CI では通る）：** `tests/secret_file`（3 件）、`tests/botctl_secret_file`（2 件）、`e2e_gateway_process_switch` の 1 件。ソースが `/mnt/d`（DrvFs）にあり、権限（0600）を守れないことと、ソケットのパスが 100 バイトを超えることによる環境の問題。`e2e_gateway_process_switch` の 1 件の原因は個別には確認していない。これまでの WSL 検証は `cargo test --workspace` が最初の失敗で止まっていたため、`--no-fail-fast` に改めた。
- **未検証：** 送信時の許可の実 Discord での動作、Oracle への導入（送信時の許可、Identify の統一、P3、nonce はいずれも Oracle に未導入）、nonce 再試行の Ubuntu での故障注入（最初の POST が成立し応答が失われ、再送が同じ ID を返し、見える投稿が 1 件）、Twilight の取り込みの検査。

## 2026-11-21 訂正：Oracle の放置試験における Identify と切断の判定

- 稼働中の Gateway は無効なセッションを受けた後、同じプロセス世代で再 Identify できる。上記の Oracle 観測で世代数が変わらなかったことから、追加の Identify がなかったとは判断できない。該当する観測区間の Identify 回数は不明。
- 上記 77 時間の「すべて OK」は当時の出力。現在の `soak-diff.sh` は切断ログの増加を WATCH と判定するため、`closes` 27→56 は **WATCH** になる。
- 当時の `worker_unavailable` は `unavailable` を含む全行の集計だった。現行のスナップショットでは Worker の診断と Identify 送信許可チャネルの失敗をそれぞれ数える。

## 2026-11-21 静的セキュリティスキャンの結果を受領
- ユーザーが提供した Codex Security のスキャン結果（リビジョン `5b60670` 対象、6 件：高 1、中 4、低 1）を、`docs/security-scan-2026-11-21.md` に要約した。**最優先は高の #2（管理用の秘密を、サーバーの確認前に、固定ポートへ送る）。** `work/ctl`（相互の challenge-response、認証前の枠の分離）と `work/buildenv`（旧式ビルドの環境の遮断）を Sol に依頼した。#4（結果不明の解消操作）と #5（REST 予算の統合）は、管理経路の変更が入ってから着手する。#3（実験用の Raft）は既知の制約。
- 同日の Astra の大規模監査（12 件）の指摘のうち、token がログに出得る問題（環境変数名 `LD_PRELOAD` など）は `work/fixa` で修正済み（main に統合）。確認の監査を実行中。

## 2026-11-21 監査・スキャンの指摘の修正を統合（main = 4079e95）
- **統合（すべて Rust CI と systemd smoke が成功）：** `work/vendor`（Twilight の取り込みの一致検査と、fixture への patch の引き継ぎ）、`work/fixa`・`work/fixb`（大規模監査 12 件）、`work/buildenv`（スキャン #6）、`work/ctl`（スキャン #2・#1：相互認証、認証前の枠の分離、Unix ソケットの選択肢）、`work/fixc`（確認監査 10 件：token の保存、systemd の boot ID、再起動の事前確認、ジャーナルの整合性、ダンプ禁止ほか）。
- **Astra の確認監査（`aece6e2` 対象）：** 12 件のうち 11 件が修正済みと確認、1 件（token の保存経路）が未修正で、新しい指摘 10 件。10 件は `work/fixc` で対応。**この `fixc` の修正に対する再確認の監査は、まだ行っていない。**
- **統合で見つかった問題と対応：** (1) `Candidate` の構造変更に既存の P3 試験が追従していなかった（試験を修正）。(2) `work/ctl` が古い形式で botd と話す fixture（`update_load`、`e2e_cluster_processes`）を壊していた（共通の認証クライアントに移行）。(3) `work/ctl` の `fork` と `flock` の競合（ジャーナルのロックを明示的に解放）。(4) `work/fixc` が、既存の試験 `restart_all_releases_earlier_bot_reservations...` の期待値を、実行せずに書き換えていた（元に戻した）。(5) `work/fixc` のダンプ禁止で、ダンプ不可のプロセスの `/proc/PID/exe` が読めなくなり、試験の確認が失敗した（試験は `cmdline` で確認。本番のコードは `exe` を読んでいない）。
- **未検証：** Oracle への導入（P3、nonce、Identify の統一、送信時の許可、管理経路の相互認証、systemd unit の変更はいずれも未導入）。実 Discord での送信時の許可の動作。Ubuntu の本番相当の環境での、管理経路の Unix ソケットの運用。ダンプ禁止下での実クラッシュの挙動。
- **運用上の注意（Oracle に入れるとき）：** `botctl` と `botd` は同じリリースを一緒に入れ替える（新しい botd は古い形式の要求を拒否する）。以前の経路（固定ポートへの秘密の送信）に露出した可能性のある管理用の秘密の回転を検討する。`/etc/discord-rs/botd.env` の項目名が新しい規則（大文字、`TOKEN` を含む）に合うことを確認する（不適合だと unit の起動前の検査で失敗する）。
- **スキャンの残り：** #4（結果不明の解消操作）と #5（REST 予算の統合）は未着手。#3（実験用 Raft）は既知の制約。
