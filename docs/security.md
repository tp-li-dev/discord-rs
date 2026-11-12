# 秘密情報とログの安全性

2026-11-12 調査・一部適用。基準は architecture.md §4.3 / §13.1。管理・登録・更新・診断 CLI と配置型の出力にマスクを接続した。全ログ経路への適用は未完了。progress.md は PM が統合するため変更していない。

## 追加 API と適用範囲

- `src/src/redact.rs` の `Secret<T>` は内部値を非公開にし、`Debug` / `Display`（整形オプションを含む）は値の型や内容によらず `[REDACTED]` だけを出力する。`new`、明示的な `expose_secret` / `into_inner` を提供する。Serialize、Deref は実装しない。メモリ消去や秘密ストアの機能はない。
- 純関数 `redact(&str) -> String` と `Redacted(&str)` の Display / Debug は、Bot / Bearer、Authorization / Proxy-Authorization、token / bot_token / interaction_token、custom_id、secret / gateway_secret / manager_secret の代入・JSON 風表記を ASCII 大文字小文字を区別せずマスクする。
- `/interactions/<数字ID>/<token>/callback`、`/webhooks/<数字ID>/<token>/...` は token パス要素をマスクし、経路を残す。クエリの `token=` も対象。パス要素中の `%2F` 等はデコードせず要素全体を隠す。
- ラベル・区切り・値の間や Bot / Bearer の後の改行、引用符内の複数行・エスケープ・閉じていない引用符を扱う。Authorization の非引用値は行末または引用符等まで隠す。引用された値は引用符ごと隠す。
- ラベルのない一般的な Discord token 形状（英数字・`_`・`-` の3要素、長さは20以上 / 6以上 / 20以上）と `mfa.` から始まる24文字以上の形状を補助的に検出する。実際の認証情報の妥当性を判定する機能ではない。

利用例（すべて架空値）：`format!("{}", Secret::new("example"))`、`format!("{}", Redacted(message))`。既知の秘密は生成・読込時に Secret で包み、ログには固定のエラー分類と安全な識別子だけを出す方針を優先する。

## 現状の脅威と対策状況 / 未対応

ソースを `token|Authorization|println!|eprintln!|tracing::` 等で検索し、関連箇所を読んだ。環境に rg がないため PowerShell の再帰列挙と Select-String を使用。以下の表は適用前の調査結果であり、今回の適用後の状態は後段「既存経路への適用」に記載する。実際の漏洩を確認したという意味ではない。実秘密は収集・表示していない。

| 箇所 | 現状と脅威 | 修正案 / 残課題 |
|---|---|---|
| `src/src/main.rs` | DISCORD_TOKEN を String として読み Twilight HTTP / Gateway に渡す。Interaction token は応答用クロージャに保持。token の直接出力は検索範囲で見つからないが、`Gateway error: {error}` / `Worker error: {error}` と Ready エラー本文、関数の `?` 伝播に保護境界がない。 | 固定分類へ置換し、必要な本文だけ Redacted を通す。既知秘密は Secret を使用。外部ライブラリのエラー表示仕様に安全性を依存させない。 |
| `src/src/main.rs`, `interaction_flow.rs` | Interaction ACK / 更新の失敗本文は固定文や結果分類に抑制され、通常メッセージ送信失敗も本文を出さない。 | 対応済み箇所の方針を維持。HTTP URL を含むエラーの将来的なログ追加時に callback / webhook token を保護する。実 HTTP エラーでの統合確認は未実施。 |
| `src/src/cogs/mod.rs`, `src/framework/src/lib.rs` | EventContext::log_info の文字列を logger がそのまま eprintln する。ハンドラーが token やユーザー入力を含む任意文字列を出せる。 | logger の出口で Redacted を適用。秘密を含むイベント本文の記録自体を避ける。 |
| `src/framework/src/arguments.rs`, `src/src/cogs/mod.rs` | ArgumentError::Unexpected は入力値を Display に含む。prefix の余剰引数は cogs 側で抑制するが、エラー型自体は他の呼出元から表示可能。 | エラーに入力本文を含めず種類・位置のみ持たせる。ラベルのない短い秘密は文字列マスクで識別できない。 |
| `src/src/worker.rs` | 更新・再起動・状態復旧等のエラー本文、Worker の拒否 reason、更新先 path を表示・エラーへ組込む経路がある。未知応答一覧は本文でなく識別子のみ。 | エラー本文・外部 reason・path の出口を保護し、固定分類を採用。パスに含まれる任意秘密やログ改行の扱いも別途検討。 |
| `src/src/bin/manage.rs` | reload の stdout / stderr をそのまま表示し、管理の message / 起動エラーをログや last_failure に保存・応答する。Endpoint / Request の secret は String だが Debug derive はない。 | 子プロセス出力と管理応答の入口・表示・保存に保護を適用。secret の型を Secret にし、専用 wire 型で意図的にシリアライズする。シリアライズ内容自体はログに出さない。 |
| `src/src/bin/register.rs`, `doctor.rs`, `reload.rs` | main の Result / `?` 経由で外部エラーが表示されうる。register の schema 表示、reload の build 名表示にも汎用マスクはない。 | CLI 最上位で固定分類・マスク済み説明へ変換。利用者が秘密を schema や build 名に混入させない設計・検証。 |
| `src/src/placement.rs`, `gateway_metadata.rs` | 配置検証は不正 ID を本文へ含めず、メタデータの取得失敗は main 側で固定エラーに変換。配置には token 値でなく環境変数名を持つ。 | 既存対策あり。設定構造体の Debug 等を将来ログへ出す場合にも未信頼文字列の混入に注意。 |
| `src/src/shard_runtime.rs`, `worker.rs`, `bin/manage.rs`, `bin/reload.rs` | Shard 起動仕様は token 環境参照を Gateway 側へ限定。Worker やビルドでは DISCORD_TOKEN を env_remove する箇所がある。 | 実プロセス分離への配線と全環境変数の継承許可リストを別途検証。別名の Bot token 環境変数まで除去される保証はない。今回 Core へ token を渡す変更はない。 |
| `src/src/local_control.rs` | ListRequest / Bootstrap は Debug を持たず、認証失敗・不正 JSON は固定エラー。認証秘密は定数時間比較し匿名パイプ bootstrap を使用。 | 秘密配列を JSON や Debug 可能な汎用コンテナに変換した後のログは別途禁止。今回の文字列マスクはバイト配列形式の秘密全体を保証しない。 |
| 管理経路全体 | ローカル制御の認証は存在するが、遠隔 mTLS / RBAC、永続監査、Windows ACL はこのマスク機能では実装・検証していない。 | §4.3 の認証・権限・監査要件を独立して実装・確認する。マスクを認証の代替にしない。 |

## 制約と検証

ヒューリスティックはすべての秘密を識別できない。未知のラベル、短い無ラベル Interaction token、分割された token、JSON / URL エンコードされたキーやスラッシュ、複数ログレコード間の分割は保証しない。非引用の値は空白・URL 区切り等で終了するため、その後の断片は検出対象外となりうる。非標準の折り返し Authorization（認証値途中の改行）も保証しない。必要なら構造化データ段階でフィールドを丸ごと隠す。誤検出により通常文章を隠すこともある。

入力文字列や一時コピーはメモリに残る。改行によるログ偽装の防止、ログ容量制限、クラッシュダンプ・外部ライブラリ独自ログ・Gateway 子プロセスの直接出力の保護は未対応。純関数を呼ばない出力先には効果がない。ビルドの stdout/stderr は終了までメモリに収集してからマスク・表示するため、表示が遅れ、出力量に比例してメモリを使う。

## 既存経路への適用（2026-11-12）

- `bin/manage.rs`: Endpoint / Request の認証秘密を `Secret<String>` に保持し、専用 serde adapter だけが wire 用に明示的に取り出す。JSON の形式と定数時間比較は維持。管理応答受信、応答作成、操作失敗・再起動失敗の保存前、reload 子出力、ビルド stdout/stderr、既存ログの表示、および最上位エラー表示で `redact` を適用した。既存ログファイル自体は書き換えない。
- `bin/register.rs`: token を `Secret` で保持し Twilight への引渡時だけ取り出す。非 Unicode 環境変数エラーは値を含めず固定分類にする。schema と最上位エラー表示をマスクし、Plan の Debug はコマンド名全体を隠す。
- `bin/reload.rs`: ビルド stdout/stderr、build 名、状態ファイルの失敗 reason、最上位エラーをマスク。`bin/doctor.rs` の最上位エラーにも適用。
- `placement.rs`: 設定・配置・起動 intent の自由文字列を Debug 時に `Secret` で丸ごと隠す。短い無ラベル文字列も対象。PlacementError の Display / Debug は `redact` を通す。Serialize の wire 内容や実配置の値は変更しない。
- `local_control.rs` の認証・JSON 失敗、`gateway_metadata.rs` の HTTP 失敗、`identify.rs` は既に固定エラーで本文を出していない。`interaction_flow.rs`、`response_drain.rs`、`drain.rs`、`switch.rs` は秘密を含むログ経路がなく、Debug は分類・数値・状態のみ。これらは変更していない。

### 未対応：PM 向け worker.rs / main.rs の具体的修正候補

行番号は今回の調査時点。これらのファイルは別作業の担当範囲として編集していない。

| 箇所 | 修正案 |
|---|---|
| `main.rs:126` | Ready の `user.name` を直接表示せず、安全な数値 ID または固定文へ置換。必要な本文には `Redacted` を適用。 |
| `main.rs:136,192,273` | Ready / Worker / Gateway の外部エラー本文を固定分類へ置換するか、整形後の文字列に `redact` を適用する。 |
| `main.rs:167,222` | 応答追跡エラーの型・伝播元を確認し、秘密を含みうる本文は固定分類へ置換。 |
| `main.rs:83,99` の最上位 `Result` と token 読込 | `?` の自動 Debug 出力を避ける main/run 境界を設け、マスク済み Display のみ出力。既知 token は `Secret` に保持し、非 Unicode 値を含む VarError は固定分類にする。 |
| `worker.rs:494,662` | 子から受け取った Rejected reason をエラーへ直接取り込まず、固定分類またはマスク済み文字列へ変換。 |
| `worker.rs:503–507` | 更新先 path の直接表示を避け、世代 ID 等を表示。任意の短い秘密がパスに含まれる場合は `redact` の検出に依存せず全体を隠す。 |
| `worker.rs:619,696,720,725,730,737` | 外部エラーと write_error のログ境界へ `redact`、可能なら固定分類を適用。 |
| `worker.rs:725–730` | 更新エラーの状態ファイル書込前にも保護を適用し、永続保存・他プロセスへの返却で生のエラーが残らないようにする。 |
| `worker.rs:837` | `error.to_string()` を Rejected reason に入れる前に分類・マスクし、IPC 受信側だけの保護に依存しない。 |

### 今回の回帰試験

`src/tests/log_redaction.rs` は架空値のみを使用する。配置 Debug と PlacementError の整形、実 manage CLI による認証 wire 互換・成功/失敗応答・既存ログ表示、実 reload CLI と失敗する偽ビルド子の stdout/stderr を検証する。実 Discord や本番管理プロセスは起動しない。管理サーバーの last_failure 保存、更新成功、実ビルドおよび実 HTTP エラーの統合動作は未検証。

Windows で `src/` から `cargo test --offline --lib --bins --test log_redaction --test redact --target-dir target/log-redaction` が50件成功（新規3件を含む）。同じ対象の `cargo clippy --offline --lib --bins --test log_redaction --test redact --target-dir target/log-redaction -- -D warnings`、変更 Rust ファイルの `rustfmt --check --edition 2024`、`git diff --check` が成功。Linux と workspace 他パッケージ全体の再試験は今回行っていない。初回は register の文書コメント位置でコンパイルが失敗したが修正後に再実行して成功した。専用 target directory を使用し、既存の実行中 exe との競合を避けた。

`src/tests/redact.rs` は架空値のみで大小文字、認証方式、ラベル、URL、複数行、UTF-8、複数秘密、重複マッチ、閉じていない引用符、非秘密文、および Secret の整形を検証する。

Windows での検証結果：

- 着手時の `cargo test --offline` はビルド・既存テストとも成功（workspace 全パッケージ指定ではない）。
- `cargo test --offline --test redact` は既存テストと同じ出力先を並行使用したため、実行中 exe の削除が Windows のアクセス拒否で失敗。別出力先による `cargo test --offline --test redact --target-dir target-redact` は5件すべて成功。検証後の出力物は既存の無視対象 `target/redact-verification` に移動した。
- `cargo clippy --offline --lib --test redact -- -D warnings`、新規 Rust ファイルの `rustfmt --check --edition 2024`、`git diff --check` は成功。
- Linux、実 Discord、実運用ログ収集、性能・ファジングは未検証。
