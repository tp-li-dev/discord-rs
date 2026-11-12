# 秘密情報とログの安全性

2026-11-12 調査。基準は architecture.md §4.3 / §13.1。今回の変更はマスク用 API とテストのみで、既存のログ出力先への接続は未対応。progress.md は PM が統合するため変更していない。

## 追加 API と適用範囲

- `src/src/redact.rs` の `Secret<T>` は内部値を非公開にし、`Debug` / `Display`（整形オプションを含む）は値の型や内容によらず `[REDACTED]` だけを出力する。`new`、明示的な `expose_secret` / `into_inner` を提供する。Serialize、Deref は実装しない。メモリ消去や秘密ストアの機能はない。
- 純関数 `redact(&str) -> String` と `Redacted(&str)` の Display / Debug は、Bot / Bearer、Authorization / Proxy-Authorization、token / bot_token / interaction_token、custom_id、secret / gateway_secret / manager_secret の代入・JSON 風表記を ASCII 大文字小文字を区別せずマスクする。
- `/interactions/<数字ID>/<token>/callback`、`/webhooks/<数字ID>/<token>/...` は token パス要素をマスクし、経路を残す。クエリの `token=` も対象。パス要素中の `%2F` 等はデコードせず要素全体を隠す。
- ラベル・区切り・値の間や Bot / Bearer の後の改行、引用符内の複数行・エスケープ・閉じていない引用符を扱う。Authorization の非引用値は行末または引用符等まで隠す。引用された値は引用符ごと隠す。
- ラベルのない一般的な Discord token 形状（英数字・`_`・`-` の3要素、長さは20以上 / 6以上 / 20以上）と `mfa.` から始まる24文字以上の形状を補助的に検出する。実際の認証情報の妥当性を判定する機能ではない。

利用例（すべて架空値）：`format!("{}", Secret::new("example"))`、`format!("{}", Redacted(message))`。既知の秘密は生成・読込時に Secret で包み、ログには固定のエラー分類と安全な識別子だけを出す方針を優先する。

## 現状の脅威と対策状況 / 未対応

ソースを `token|Authorization|println!|eprintln!|tracing::` 等で検索し、関連箇所を読んだ。環境に rg がないため PowerShell の再帰列挙と Select-String を使用。以下は漏洩しうる経路と既存対策の調査結果であり、実際の漏洩を確認したという意味ではない。実秘密は収集・表示していない。

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

入力文字列や一時コピーはメモリに残る。改行によるログ偽装の防止、ログ容量制限、クラッシュダンプ・外部ライブラリ独自ログ・子プロセスの直接出力の保護、実ログ出力先への接続は未対応。純関数を呼ばない出力先には効果がない。

`src/tests/redact.rs` は架空値のみで大小文字、認証方式、ラベル、URL、複数行、UTF-8、複数秘密、重複マッチ、閉じていない引用符、非秘密文、および Secret の整形を検証する。

Windows での検証結果：

- 着手時の `cargo test --offline` はビルド・既存テストとも成功（workspace 全パッケージ指定ではない）。
- `cargo test --offline --test redact` は既存テストと同じ出力先を並行使用したため、実行中 exe の削除が Windows のアクセス拒否で失敗。別出力先による `cargo test --offline --test redact --target-dir target-redact` は5件すべて成功。検証後の出力物は既存の無視対象 `target/redact-verification` に移動した。
- `cargo clippy --offline --lib --test redact -- -D warnings`、新規 Rust ファイルの `rustfmt --check --edition 2024`、`git diff --check` は成功。
- Linux、実 Discord、実運用ログ収集、性能・ファジングは未検証。
