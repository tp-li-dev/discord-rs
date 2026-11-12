# 秘密情報とログの安全性

2026-11-12 調査・一部適用。基準は architecture.md §4.3 / §13.1。管理・登録・更新・診断 CLI、配置型に加え、worker.rs / main.rs の後述する出力境界に保護を接続した。全ログ経路への適用は未完了。progress.md は PM が統合するため変更していない。

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

### worker.rs / main.rs の候補への対応

行番号は適用前の調査時点。以下の候補は今回対応した。短い無ラベルの秘密を含み得る外部エラーは、文字列検出に頼らず `Secret` で本文全体を隠す。

| 調査時の箇所 | 実装した対策 |
|---|---|
| `main.rs:126` | Ready の直接ログは数値 user.id のみ。イベントの bot_name 自体は機能データとして維持する。 |
| `main.rs:136,192,273` | Ready / Worker / Gateway のエラーは固定コンテキストと `Secret`。外部エラーの Display / Debug を呼ばない。 |
| `main.rs:167,222` | 応答追跡エラーにも同じ出力境界を適用。Interaction outcome は閉じた列挙型の分類のみ。 |
| `main.rs:83,99` | main/run と ExitCode を分離し自動 Debug 出力を排除。Bot token は `Secret<String>`、Interaction token は `Arc<Secret<String>>` に保持し HTTP 呼出時だけ明示参照する。欠落・非 Unicode 環境変数は固定分類。非 Unicode 引数も args_os で扱う。 |
| `worker.rs:494,662` | 子の Rejected reason は `Secret` で全体を隠してから Manager のエラーへ変換。復元拒否も同様。 |
| `worker.rs:503–507` | 更新成功ログから更新先パス全体を除き、Worker 番号と pool サイズのみ表示。spawn 失敗も固定分類。 |
| `worker.rs:619,696,720,725,730,737` | job・再起動・復旧・ファイル操作失敗の外部本文を `Secret` で隠す。表示用 build は `redact` を通す。 |
| `worker.rs:725–730` | 更新失敗の診断は `Secret` と `redact` を通し、同じ安全な文字列をログと状態ファイルの error フィールドへ保存する。 |
| `worker.rs:837` | 不正な状態の Rejected reason は固定の `invalid worker state`。親の不正 Response デコードエラーも固定分類で、serde の未信頼入力本文を伝播しない。 |

更新状態ファイルの成功パスと失敗 JSON の `path` は、reload が要求と応答を完全一致で照合するための制御データであり、引き続き正確なパスを保持する。診断ログには出力しない。パスを含む wire データ全体のマスクや状態ファイルのアクセス権変更は行っていない。この照合フィールドへ秘密を埋め込まない運用とファイル権限制限が必要であり、任意のファイル名の秘密まで保存されないという保証ではない。

担当外の `cogs/mod.rs` / framework の任意 logger は未変更で、Ready の bot_name を表示し得る。アプリの panic hook、子プロセス自身の stderr、Twilight 等の独自ログもこの境界では保護しない。TaskManager がパニックを固定分類へ変換しても、標準 panic hook の出力保護を意味しない。

`src/tests/main_security.rs` は実バイナリの欠落・非 Unicode token、不正 Worker JSON（短い無ラベル秘密を含む）、正常 health wire を確認する。main の単体試験は、外部 Display / Debug を実行しないこと、token 読込分類、Interaction identity の HTTP 相当クロージャへの引渡しを確認する。`src/tests/worker_replay.rs` は未信頼のコマンド・restore 拒否理由の保護と、実 watch が書く失敗状態の診断文字列の保護、旧 Worker 継続を確認する。すべて架空値を使用する。実 Discord・実 HTTP の例外ログと Linux は未検証。

今回の統合後、Windows / Rust 1.99.0 で `cargo test --offline --workspace` は293件（新規24件、doc-testを含む）成功。`cargo clippy --offline --workspace --all-targets -- -D warnings`、`cargo fmt --all --check`、`git diff --check` も成功した。release ビルドを伴う既存 reload / 管理 / 更新試験を含む。子 cargo も `CARGO_NET_OFFLINE=true` で実行した。これらは任意 logger・panic hook・外部ライブラリ出力の秘密保護を保証する結果ではない。

### 今回の回帰試験

`src/tests/log_redaction.rs` は架空値のみを使用する。配置 Debug と PlacementError の整形、実 manage CLI による認証 wire 互換・成功/失敗応答・既存ログ表示、実 reload CLI と失敗する偽ビルド子の stdout/stderr を検証する。実 Discord や本番管理プロセスは起動しない。管理サーバーの last_failure 保存、更新成功、実ビルドおよび実 HTTP エラーの統合動作は未検証。

Windows で `src/` から `cargo test --offline --lib --bins --test log_redaction --test redact --target-dir target/log-redaction` が50件成功（新規3件を含む）。同じ対象の `cargo clippy --offline --lib --bins --test log_redaction --test redact --target-dir target/log-redaction -- -D warnings`、変更 Rust ファイルの `rustfmt --check --edition 2024`、`git diff --check` が成功。Linux と workspace 他パッケージ全体の再試験は今回行っていない。初回は register の文書コメント位置でコンパイルが失敗したが修正後に再実行して成功した。専用 target directory を使用し、既存の実行中 exe との競合を避けた。

`src/tests/redact.rs` は架空値のみで大小文字、認証方式、ラベル、URL、複数行、UTF-8、複数秘密、重複マッチ、閉じていない引用符、非秘密文、および Secret の整形を検証する。

Windows での検証結果：

- 着手時の `cargo test --offline` はビルド・既存テストとも成功（workspace 全パッケージ指定ではない）。
- `cargo test --offline --test redact` は既存テストと同じ出力先を並行使用したため、実行中 exe の削除が Windows のアクセス拒否で失敗。別出力先による `cargo test --offline --test redact --target-dir target-redact` は5件すべて成功。検証後の出力物は既存の無視対象 `target/redact-verification` に移動した。
- `cargo clippy --offline --lib --test redact -- -D warnings`、新規 Rust ファイルの `rustfmt --check --edition 2024`、`git diff --check` は成功。
- Linux、実 Discord、実運用ログ収集、性能・ファジングは未検証。

## RBAC と追記専用監査 API（2026-11-12）

`src/src/rbac_audit.rs` に管理操作を認可・監査してから実行する `AuditedExecutor` を追加した。上記の過去調査にある「RBAC・永続監査未実装」は既存管理経路についての記述であり、この API はローカル `manage` のハンドラーに接続した。`botctl` や遠隔 Daemon のハンドラーには未接続であり、実 CLI から遠隔通信を通した T20 全体の完了は主張しない。認証通信、quorum、lease、fencing の代替ではなく、それらを迂回する実装も追加していない。

### 許可表と確認

| 操作 | viewer | updater | admin | 追加確認 |
|---|---|---|---|---|
| status / inspect / logs | 許可 | 許可 | 許可 | なし |
| update / rollback | 拒否 | 許可 | 許可 | なし |
| stop / shutdown | 拒否 | 拒否 | 許可 | なし（§4.1 の graceful stop とその別名） |
| force / ownership-switch | 拒否 | 拒否 | 許可 | 明示オプションと肯定確認の両方が必須 |

ロール・操作名は表の小文字の完全一致のみ受理する。未認証、不明ロール、不明操作、権限不足、空の主体、不正な対象は拒否する。ローカル要求にも認証・認可が必要。`remote` は監査属性であり認証の代わりにならない。`status` / `logs` は全体参照として Bot 未指定を許す。他の操作には Bot が必要で、Shard 指定時には必ず Bot も要求する。Bot の実在・正規 ID の解決、主体の対象 Bot/Shard へのアクセス範囲は呼出側で検証する。

危険操作の `Confirmation::Interactive` は `explicit` と `accepted`、非 TTY の `NonInteractive` は `explicit` と `yes` の両方を要求する。確認で不足権限を補うことはできない。管理アダプターは確認を今回の操作と対象に結び付け、非 TTY に対話確認を流用しないこと。

`Principal::authenticated(subject, role)` は認証成功を表す**信頼済みアダプター専用**の入口であり、認証処理自体は行わない。主体は検証済みのローカル OS / mTLS 等の identity、ロールはサーバー側の対応表から取得する。クライアント送信の role や authenticated フラグをここへ直結しない。Principal / Request に Deserialize・Debug は実装していない。未認証は `Principal::unauthenticated()` として同じ実行 API へ渡す。

### 監査と失敗時の動作

`AuditedExecutor::open(path, AuditConfig)` と `execute(&request, closure)` を使用する（コンパイル検査する使用例はモジュールの rustdoc）。closure は要求の操作・対象と一致する実処理を一度だけ行い、`Result<T, String>` を返す。同期ファイル I/O を行うため、Gateway の Heartbeat・イベント処理から分離した管理用タスク等で実行する。

- JSON Lines に `timestamp_unix_ms`（UTC Unix ミリ秒）、`request_id`、`subject`、`role`、`operation`、`bot_id`、`shard_id`、`remote`、`result`、`reason` を保存する。未認証主体は `anonymous`。時刻は OS の壁時計で、時計補正を跨ぐ単調性は保証しない。要求 ID は既存ログから再開し、開始と終了は同じ ID を使う。
- 拒否は `denied` と固定の拒否理由を記録する。許可時は `started` を `write_all` / `sync_all` してから closure を呼ぶ。終了時は `succeeded` / `failed`、panic 時は本文を保存せず `panicked` を追記し panic を再送出する。アプリの panic hook の出力は別の保護境界であり、この API はマスクしない。
- 主体・ロール・操作・Bot・結果理由の文字列を既存 `redact.rs` に通してから serde JSON へ変換する。CR/LF・引用符等は JSON エスケープされ、入力で別の JSONL レコードを注入できない。失敗理由はマスク後に UTF-8 境界で最大512バイトまで切り詰め、印を付ける。レコード上限を超える結果理由は固定の `omitted` に置き換える。マスクのヒューリスティックの限界は上記の制約が引き続き適用される。資格情報自体を要求・理由へ入れないこと。
- 操作前の監査書き込み失敗では closure を呼ばず `ExecuteError::Audit` を返す。書き込み・同期・ローテーション失敗後の writer は利用不能にし、後続操作も拒否する。結果レコードの最大サイズ分の容量を事前予約するため、予測可能な容量不足でも操作前に拒否する。
- 操作が既に行われた後の媒体障害・外部変更による結果記録失敗は `AuditAfterExecution` として区別する。外部副作用の取消や「操作されなかった」ことは保証できない。クラッシュ等で終了記録のない `started` も結果不明として照合し、自動再試行しない。監査と外部操作を単一トランザクションにする保証はない。

### ファイル権限・容量・ローテーション

新規ファイルは Unix で作成モード0600を指定し、既存ファイルに group/other 権限があれば拒否する。新規作成後はファイルと Unix の親ディレクトリを同期する。Windows は親ディレクトリの ACL を継承するだけで、ACL の設定・検証は行っていない。Windows ACL、電源断を含む永続化保証は未検証であり、運用側で専用ディレクトリへのアクセスを制限する必要がある。

パスはクライアント指定を使用せず、事前作成した private な安定したディレクトリ内の固定パスを使う。通常ファイル以外・symlink、Unix の複数 hardlink を拒否するが、標準ライブラリによる検査と open の間の競合や親ディレクトリの差替えを完全には防がない。信頼できないユーザーに親ディレクトリの変更権限を与えないこと。ベースファイルの OS 排他ロックを writer 生存中ずっと保持し、同じログへの協調する複数 writer を拒否する。Unix ロックは advisory であり、OS 管理者・同一ユーザーによる改竄への耐性や署名付き監査は提供しない。

既定値は1ファイル8 MiB、1レコード16 KiB（どちらも末尾 LF 込み）、追加ファイル16個。満杯時は `audit.jsonl.1`、`.2` の順に `create_new` で新規ファイルへ切り替え、過去ファイルを削除・上書き・truncate しない。最大ファイル数は `1 + max_archives`。容量枠を使い切れば操作を拒否するため、運用時には残量監視と停止中の保管・別ログ系列への切替手順が必要。結果ログの保守的な容量予約により、空きが残っていても実行を拒否する場合がある。

再開時は既存 JSONL をレコード上限付きで読み、不正 JSON・未終端・サイズ超過・連番ファイルの欠番・要求 ID の逆行を拒否し、自動修復や履歴削除はしない。通常ログの読み取り・ローテーション設定の変更は writer 停止中に行う。監査不能な過大メタデータは操作を実行せずエラーで返し、その要求を監査できたとは扱わない。

### 今回の検証範囲

Windows / Rust 1.99.0 でロール×全9操作、未認証遠隔要求、確認の全組合せ、成功・失敗・panic、秘密マスク、改行注入、サイズ上限、容量予約、履歴を維持するローテーション・再開・排他・破損拒否を試験した。実際の読み取り専用 OS ハンドルへの書き込み失敗で操作が走らないことと、操作後の外部 truncate を結果不明として返して後続操作を拒否することも試験した。この API 単体試験では実管理通信への接続、Linux実行、Unix0600試験の実行、Windows ACL、実媒体障害・強制終了・電源断・負荷時性能は未検証。`docs/progress.md` は依頼どおり変更せず、PM に最終報告で引き継ぐ。

`src/` から `cargo test --offline --lib --test rbac_audit --test redact` が73件、`cargo test --offline --doc` が3件成功した（新規 RBAC/監査の実行試験33件と使用例1件を含む）。Unix限定2件はWindowsでは実行されない。`cargo clippy --offline --lib --test rbac_audit -- -D warnings`、`rustfmt --check --edition 2024 src/rbac_audit.rs tests/rbac_audit.rs src/lib.rs`、`git diff --check` も成功。workspace 全パッケージ・全統合テストの再実行は今回の検証範囲に含めていない。


## ローカル管理経路への適用

`manage` は loopback の管理接続で資格情報を照合し、`core.json` の資格情報に admin、別の `viewer.json` の資格情報に viewer をサーバー側で結び付ける。クライアントが要求でロールを選ぶことはできない。各ファイルには同じ形式の接続先とそれぞれ別の秘密が入るため、必要な権限を持つユーザーにだけ該当ファイルへのアクセスを許可する。Windows でこれらのファイルや監査ディレクトリの ACL の設定・検証は実装されておらず、ACL は未検証。資格情報を読める同一ユーザーのプロセスからの利用を区別する仕組みではない。

認証・認可の拒否と管理操作を `runtime/audit.jsonl` に記録する。操作前の監査書き込み・同期に失敗した場合は操作を実行しない。操作後の結果記録が失敗した場合は副作用を巻き戻せず、結果不明として扱う。監査の容量、排他、改竄耐性の制約は前節に従う。受信できなかったフレームなど、要求として識別できない接続のすべてを監査できる保証はない。接続枠または処理待ちキューの飽和時も拒否を記録できない場合がある。未認証の遠隔管理を許可せず、遠隔 mTLS や遠隔管理の RBAC は未実装。

`metrics-json` と `metrics-prometheus` は管理プロセス内のメトリクススナップショットを返す。現在の registry は Gateway / Worker の観測に接続されていないため、表示値を実環境の状態や健全性の証明に使えない。Windows ACL、Linux 実機、実 Discord や遠隔管理を通じた動作は未検証。
