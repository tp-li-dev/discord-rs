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
