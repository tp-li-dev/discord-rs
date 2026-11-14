# 秘密情報とログの安全性

## 現行実装の補足

> [!NOTE]
> 2026-11-13の決定により、**対応OSはLinux(Ubuntu)のみであり、Windowsはサポート外**（開発機での動作は保証しない。Windows固有コードは残るが保証・CI対象外）です。後述のWindows開発機での検証結果はすべて『過去の参考記録』と位置づけられ、Windows ACL未実装などの未対応事項は未解決課題ではなく『サポート外』として扱われます。実Discordでの検証はユーザーが実施する範囲です。

`fs_perms` は Linux において開いたハンドルの Unix mode をファイル0600・ディレクトリ0700に設定・検証する。Windows 用パス API コードは残るが、Windows はサポート外（Windows ACL、ハンドル API、ディレクトリ保護は未解決課題ではなく『サポート外』）である。信頼された親パス等を前提とし TOCTOU を完全には防がない。既存の全資格情報経路に自動適用されない。

証明書の leaf 切替・失効、永続ポリシー、CA チェーンを扱う部品がある。`cluster_runtime::bind_raft` は mTLS Raft RPC に接続する試験経路だが、本番 Raft 起動は拒否される。証明書配布や本番運用への自動統合は未保証。過去の節は当時の個別経路の記録である。

`botd` の更新操作はサーバー側 capability を principal・role・shard scope に結び付け、監査と操作 ID を使う。遠隔管理全体や再起動を跨ぐ冪等性は未完成。

2026-11-12 調査・適用。基準は architecture.md §4.3 / §13.1。今回の担当範囲の現状は次節、従来の調査と検証履歴は後段に記載する。担当外の logger 等が残るため、リポジトリ全体の秘密保護完了は主張しない。progress.md / handoff.md は今回の指示に従い変更していない。

## 残課題6：今回の出力境界の修正

対象は `redact.rs`、`main.rs`、`bin/doctor.rs`、`bin/register.rs`、`bin/reload.rs`、`bin/manage.rs` と `worker.rs` のログ・エラー出力箇所。daemon.rs、botd.rs、botctl.rs、cogs、framework は変更していない。以下が後段の過去の「未対応」記述より優先する現状である。

| 経路 | 今回の対策と保持する診断 |
|---|---|
| 外部 Error / Debug | 未知の Display / Debug / source を呼ばない。`error_diagnostic` は I/O の kind・OSコード、JSON の category・行列位置を保持する。CLI 固有の固定原因は専用エラー型で保持する。 |
| panic | 対象5実行入口で runtime の生成・引数処理より前に `install_panic_hook()` を設定。payload・スレッド名・ソースパス・backtrace を表示せず、固定 `panic [panic]: [REDACTED]` を出す。以前の hook は呼ばない。unwind/abort と終了コードは変更しない。 |
| main / Worker | Gateway のエラー種別、Worker の固定原因、I/O/JSON分類を維持。Worker の任意 build 名は表示時に全体を `Secret` で隠す。子の終了コードをテキスト診断にも残す。 |
| Worker / Gateway 子プロセス | Worker の stderr と manage が起動する Bot の stdout/stderr を null に接続。断片化された秘密・無ラベル秘密を含む任意子出力をログへ継承・保存しない。親の protocol/I/O/終了コード診断は維持する。 |
| ビルド / reload 子出力 | 原文を表示しない。reload のビルド出力は null に廃棄。manage の収集済み子出力は本文を表示・応答・保存せず、成否と終了コードのみ報告する。子 Cargo にも `--offline` を指定。コンパイラーの自由文による詳細診断は表示できなくなる。 |
| reload 状態エラー | 自由文 reason は全体を隠し、`worker_update_failed`、許可した phase (update/runtime)、i32 の exit_code を表示する。要求との一致照合用 path は表示しない。 |
| manage 応答 / last_failure | 自由文 message を診断として信用せず、閉じた診断列挙型と数値の optional wire field を使用。未知・旧 peer の自由文は全体抑制。JSON の不正値も本文を表示しない。status の PID・再起動回数・失敗分類を保持する。 |
| manage 既存ログ | `logs` は本文を全体抑制する。短い無ラベル秘密を検出できないため、従来のマスク済み本文表示から動作が変わる。既存ログファイルの内容は書き換えない。 |
| manage 観測値 / 監査 | list-unknown は32桁小文字hexの公開 instance と数値を保持し、他の自由文を隠す。metrics は正規の数値 Bot ID、Shard/世代・既知 role/component・観測数値のみ保持し、自由ラベルを隠す。認可拒否7分類・監査失敗8分類と実行前/後の区別を閉じた wire 診断で保持する。 |
| doctor / register | 非Unicode引数・環境変数の値は表示しない。doctor の配置検査は既存の固定原因のみ表示。register の preview は入力から構築されないコード内の登録済み静的 schema の表示を維持し、補助的に redact を適用する。Plan Debug は name を全体抑制。 |
| 鍵 / 構造化フィールド | private_key / signing_key / api_key / password 等のラベル、配列・オブジェクトの秘密値、PEM秘密鍵（未終端も含む）、Authorization の空白継続行を追加保護。文字列マスク単独で任意秘密を識別する保証ではない。 |

Worker の stdout は JSON IPC 専用で、通常の content/state や設定を運ぶ。要求された機能データを消すとプロトコルが壊れるため、診断出力とは区別し、wire 自体の丸ごとマスクは行わない。更新状態ファイルの照合用 path も同様であり、ここに秘密を埋め込まない運用とアクセス制限が必要。任意のアプリ機能データまで秘密を含まないという保証ではない。

担当外に残る箇所：`cogs/mod.rs` の任意 logger と framework の自由文エラー、daemon.rs / botd.rs / botctl.rs の全出力は今回変更していない。特に `--worker` を直接起動した場合、Ready の bot_name は cogs の logger から stderr に出得る。親 Manager から起動した子では stderr を遮断するが、直接起動の出口保護には cogs の logger を固定分類または構造化出力へ変更する別作業が必要。外部ライブラリがプロセス内で独自に書く出力、hook の再置換、ネイティブ abort、クラッシュダンプ・メモリ消去・OS権限は本対策の保証外。

新規試験は担当 Rust ファイル内に置き、自己再起動する子プロセスへ架空の token / secret / key / Authorization と短い無ラベル値を渡し、stdout/stderr と終了コードを検査する。panic の payload・スレッド名、子出力、外部 I/O/JSON エラー、管理応答・保存用失敗分類を対象とする。重い workspace 全試験、release ビルド、実Discordは実行しない。

### 過去の参考記録: Windows / Rust 1.90.0 での検証結果

以下は過去のWindows開発機での検証結果である（Windowsはサポート外であり、開発機での動作は保証しない。Linuxが対象）。`src/` から Cargo はすべて `--offline -p src --jobs 2` で実行した。対象を絞った結果は重複再実行を除き62件成功・1件失敗。子プロセス専用fixtureの4件は通常実行ではignoreされ、親の回帰試験から明示実行される。

| 対象 | 結果 |
|---|---|
| `cargo test … --lib redact::tests` | 4件成功。生の無ラベルpanic payload、秘密入りスレッド名、backtrace有効時も検査。 |
| `cargo test … --lib worker::frame_tests` | 10件成功。初回は試験側の `Command::output()` がstderr設定を上書きして1件失敗したが、本番同様 `spawn().wait_with_output()` に修正後成功。 |
| `cargo test … --bin src --bin doctor --bin register --bin reload` | 22件成功、子fixture4件ignore。 |
| `cargo test … --bin manage` | 最終版6件成功。管理応答・子出力・保存診断・監査原因・公開識別子/観測値を確認。 |
| `cargo test … --test doctor` | 2件成功。 |
| `cargo test … --test main_security --test register --test redact` | 13件成功。実CLIの起動失敗・不正Worker JSON・正常health wireを含む。 |
| `cargo test … --test log_redaction` | 2件成功・1件失敗。`management_reply_errors_and_log_display_are_masked` の165行目は旧仕様 `old token=[REDACTED]` の本文表示を期待する。今回は本文全抑制へ変更したため不一致。漏洩を検出した失敗ではない。担当外の試験ファイルは変更していない。PM側で `[log_contents_suppressed]` と秘密不在の期待へ更新が必要。 |
| `cargo test … --test worker_replay rejected_worker_text_is_hidden_at_manager_boundaries` と `… watch_persists_only_protected_update_failure_text` | 各1件成功。拒否理由の保護、実watchの失敗状態保存、旧Worker継続を確認。 |
| `cargo test … --test manage independent_commands_manage_a_persistent_core` | 1件成功。認証・権限拒否、status/metrics、再起動と終了診断の既存動作を確認。 |

`cargo clippy --offline -p src --lib --bin src --bin doctor --bin register --bin reload --bin manage --test main_security --test doctor --test register --test redact --test log_redaction --jobs 2 -- -D warnings`、変更Rustファイルの `rustfmt --check --edition 2024`、`git diff --check` は成功。依存追加・commitは行っていない。Linux実行、実Discord/HTTPエラー、本番ログ収集、担当外全経路、全体負荷試験は未検証。

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

ヒューリスティックはすべての秘密を識別できない。未知のラベル、短い無ラベル Interaction token、分割された token、JSON / URL エンコードされたキーやスラッシュ、複数ログレコード間の分割は保証しない。非引用の値は空白・URL 区切り等で終了するため、その後の断片は検出対象外となりうる。Authorization は改行後に SP/TAB がある継続行を隠すが、任意の改行・分割を復元して検出する保証ではない。必要なら構造化データ段階でフィールドを丸ごと隠す。誤検出により通常文章を隠すこともある。

入力文字列や一時コピーはメモリに残る。改行によるログ偽装の一般的防止、ログ容量制限、クラッシュダンプ・外部ライブラリ独自ログの包括的保護は未対応。純関数を呼ばない出力先には効果がない。今回、対象の親から起動する Worker / Bot 子の出力は前節のとおり遮断した。manage の build/reload 出力は終了までメモリに収集するため出力量に比例してメモリを使うが、原文を表示・ログ保存しない。reload 自身のビルド子出力は収集せず廃棄する。

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

担当外の `cogs/mod.rs` / framework の任意 logger は未変更で、直接 Worker を起動すれば Ready の bot_name を表示し得る。今回、対象実行入口の panic hook と親から起動する子の出力を保護した（前節参照）。Twilight 等のプロセス内独自ログ全体を保護するグローバルなログフィルターは実装していない。TaskManager のパニック分類だけに秘密保護を依存しない。

`src/tests/main_security.rs` は実バイナリの欠落・非 Unicode token、不正 Worker JSON（短い無ラベル秘密を含む）、正常 health wire を確認する。main の単体試験は、外部 Display / Debug を実行しないこと、token 読込分類、Interaction identity の HTTP 相当クロージャへの引渡しを確認する。`src/tests/worker_replay.rs` は未信頼のコマンド・restore 拒否理由の保護と、実 watch が書く失敗状態の診断文字列の保護、旧 Worker 継続を確認する。すべて架空値を使用する。実 Discord・実 HTTP の例外ログと Linux は未検証。

過去の参考記録: 当時の統合後、Windows / Rust 1.99.0 で `cargo test --offline --workspace` は293件（新規24件、doc-testを含む）成功（Windowsはサポート外であり、開発機での動作は保証しない。Linuxが対象）。`cargo clippy --offline --workspace --all-targets -- -D warnings`、`cargo fmt --all --check`、`git diff --check` も成功した。release ビルドを伴う既存 reload / 管理 / 更新試験を含む。子 cargo も `CARGO_NET_OFFLINE=true` で実行した。これらは任意 logger・panic hook・外部ライブラリ出力の秘密保護を保証する結果ではない。

### 今回の回帰試験

`src/tests/log_redaction.rs` は架空値のみを使用する。配置 Debug と PlacementError の整形、実 manage CLI による認証 wire 互換・成功/失敗応答・既存ログ表示、実 reload CLI と失敗する偽ビルド子の stdout/stderr を検証する。実 Discord や本番管理プロセスは起動しない。管理サーバーの last_failure 保存、更新成功、実ビルドおよび実 HTTP エラーの統合動作は未検証。

過去の参考記録: 当時 Windows で `src/` から `cargo test --offline --lib --bins --test log_redaction --test redact --target-dir target/log-redaction` が50件成功（新規3件を含む）。同じ対象の `cargo clippy --offline --lib --bins --test log_redaction --test redact --target-dir target/log-redaction -- -D warnings`、変更 Rust ファイルの `rustfmt --check --edition 2024`、`git diff --check` が成功。Linux と workspace 他パッケージ全体の再試験は今回行っていない。初回は register の文書コメント位置でコンパイルが失敗したが修正後に再実行して成功した。専用 target directory を使用し、既存の実行中 exe との競合を避けた。

`src/tests/redact.rs` は架空値のみで大小文字、認証方式、ラベル、URL、複数行、UTF-8、複数秘密、重複マッチ、閉じていない引用符、非秘密文、および Secret の整形を検証する。

過去の参考記録（当時のWindowsでの検証結果、Windowsはサポート外）：

- 着手時の `cargo test --offline` はビルド・既存テストとも成功（workspace 全パッケージ指定ではない）。
- `cargo test --offline --test redact` は既存テストと同じ出力先を並行使用したため、実行中 exe の削除が Windows のアクセス拒否で失敗。別出力先による `cargo test --offline --test redact --target-dir target-redact` は5件すべて成功。検証後の出力物は既存の無視対象 `target/redact-verification` に移動した。
- `cargo clippy --offline --lib --test redact -- -D warnings`、新規 Rust ファイルの `rustfmt --check --edition 2024`、`git diff --check` は成功。
- Linux、実 Discord（ユーザー検証範囲）、実運用ログ収集、性能・ファジングは未検証。

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

新規ファイルは Unix で作成モード0600を指定し、既存ファイルに group/other 権限があれば拒否する。新規作成後はファイルと Unix の親ディレクトリを同期する。Windows はサポート外（Windows ACL の設定・検証は未解決課題ではなく『サポート外』）。電源断を含む永続化保証は未検証であり、運用側で専用ディレクトリへのアクセスを制限する必要がある。

パスはクライアント指定を使用せず、事前作成した private な安定したディレクトリ内の固定パスを使う。通常ファイル以外・symlink、Unix の複数 hardlink を拒否するが、標準ライブラリによる検査と open の間の競合や親ディレクトリの差替えを完全には防がない。信頼できないユーザーに親ディレクトリの変更権限を与えないこと。ベースファイルの OS 排他ロックを writer 生存中ずっと保持し、同じログへの協調する複数 writer を拒否する。Unix ロックは advisory であり、OS 管理者・同一ユーザーによる改竄への耐性や署名付き監査は提供しない。

既定値は1ファイル8 MiB、1レコード16 KiB（どちらも末尾 LF 込み）、追加ファイル16個。満杯時は `audit.jsonl.1`、`.2` の順に `create_new` で新規ファイルへ切り替え、過去ファイルを削除・上書き・truncate しない。最大ファイル数は `1 + max_archives`。容量枠を使い切れば操作を拒否するため、運用時には残量監視と停止中の保管・別ログ系列への切替手順が必要。結果ログの保守的な容量予約により、空きが残っていても実行を拒否する場合がある。

再開時は既存 JSONL をレコード上限付きで読み、不正 JSON・未終端・サイズ超過・連番ファイルの欠番・要求 ID の逆行を拒否し、自動修復や履歴削除はしない。通常ログの読み取り・ローテーション設定の変更は writer 停止中に行う。監査不能な過大メタデータは操作を実行せずエラーで返し、その要求を監査できたとは扱わない。

### 過去の参考記録: Windows / Rust 1.99.0 での検証範囲

過去のWindows開発機（Windows / Rust 1.99.0、サポート外）でロール×全9操作、未認証遠隔要求、確認の全組合せ、成功・失敗・panic、秘密マスク、改行注入、サイズ上限、容量予約、履歴を維持するローテーション・再開・排他・破損拒否を試験した。実際の読み取り専用 OS ハンドルへの書き込み失敗で操作が走らないことと、操作後の外部 truncate を結果不明として返して後続操作を拒否することも試験した。この API 単体試験では実管理通信への接続、Linux実行、Unix0600試験の実行、実媒体障害・強制終了・電源断・負荷時性能は未検証（Windows ACL はサポート外）。`docs/progress.md` は依頼どおり変更せず、PM に最終報告で引き継ぐ。

`src/` から `cargo test --offline --lib --test rbac_audit --test redact` が73件、`cargo test --offline --doc` が3件成功した（新規 RBAC/監査の実行試験33件と使用例1件を含む）。Unix限定2件はWindowsでは実行されない。`cargo clippy --offline --lib --test rbac_audit -- -D warnings`、`rustfmt --check --edition 2024 src/rbac_audit.rs tests/rbac_audit.rs src/lib.rs`、`git diff --check` も成功。workspace 全パッケージ・全統合テストの再実行は今回の検証範囲に含めていない。


## ローカル管理経路への適用

`manage` は loopback の管理接続で資格情報を照合し、`core.json` の資格情報に admin、別の `viewer.json` の資格情報に viewer をサーバー側で結び付ける。クライアントが要求でロールを選ぶことはできない。各ファイルには同じ形式の接続先とそれぞれ別の秘密が入るため、必要な権限を持つユーザーにだけ該当ファイルへのアクセスを許可する。Windows ACL はサポート外（Windows非対応）。資格情報を読める同一ユーザーのプロセスからの利用を区別する仕組みではない。

認証・認可の拒否と管理操作を `runtime/audit.jsonl` に記録する。操作前の監査書き込み・同期に失敗した場合は操作を実行しない。操作後の結果記録が失敗した場合は副作用を巻き戻せず、結果不明として扱う。監査の容量、排他、改竄耐性の制約は前節に従う。受信できなかったフレームなど、要求として識別できない接続のすべてを監査できる保証はない。接続枠または処理待ちキューの飽和時も拒否を記録できない場合がある。未認証の遠隔管理を許可せず、遠隔管理ハンドラーへの mTLS・RBAC の接続は未実装。試験用 daemon 間通信と Raft RPC は mTLS Transport を使う。

`metrics-json` と `metrics-prometheus` は管理プロセス内のメトリクススナップショットを返す。現在の registry は Gateway / Worker の観測に接続されていないため、表示値を実環境の状態や健全性の証明に使えない。Windows ACL はサポート外。Linux 実機、実 Discord（ユーザー検証範囲）や遠隔管理を通じた動作は未検証。

## botd / botctl のローカル管理境界

`botd` は loopback のみで受け付け、匿名 stdin（`--bootstrap-stdin`）または権限検査済みの secret ファイル（`--secret-file`。通常ファイル・モード 0600/0400・所有者検査。[linux-deployment.md](linux-deployment.md)）で渡された非ゼロの32バイト共有資格情報を要求と定数時間比較してから操作を管理キューに渡す。`botctl` は bootstrap 情報を読みこの資格情報を送信する。これは `manage` の viewer / admin と別の経路で、更新capabilityにはrole・shard scopeと監査が接続されるが、全操作で利用者ごとの役割分離・永続監査を提供しない。資格情報や bootstrap ファイルを閲覧可能な主体から保護する必要がある。`--force` の確認は権限認証の代替ではない。遠隔管理や mTLS とこのローカル listener は接続されていない。`src/src/bin/botd.rs` の `authenticated_status_returns_snapshot_without_secret`、`status_is_dispatched_only_after_authentication` と `src/tests/botctl_daemon.rs` がローカル経路の一部を検証する。Windows ACL はサポート外、複数ホストでの利用は未検証。

## Daemon 間 mTLS TCP Transport（2026-11-12）

`src/src/tls_transport.rs` の `TlsTransport` が既存 `cluster::Transport` を実装する。Tokio TCP と rustls / tokio-rustls を使い、TLS 1.3・相互証明書認証・ALPN `botd-cluster/1` を必須にする。追加依存はない。`main.rs` / `worker.rs` / `daemon.rs`、本番の合意・lease・RBAC・監査ハンドラーには接続していない。Transport の認証試験は §4.3 / §13.1 / T20 の一部であり、遠隔 CLI 操作と永続監査を含む T20 全体の完了ではない。

### 証明書とノードの対応

- 管理者が明示する CA のみを信頼する。OS の公開 CA ストアや接続元 IP を認証根拠にしない。rustls / webpki で証明書チェーン、署名、期限、用途（client/server EKU）と TLS の秘密鍵保有証明を検証する。未許可ノード、未信頼 CA・自己署名、期限切れ・有効期間前、用途不一致、証明書無し、平文を拒否する。
- `NodeId(u64)` は証明書の唯一の DNS SAN `node-<10進整数>.cluster` から取り出す（例: `NodeId(7)` は `node-7.cluster`）。先頭ゼロ、符号、ワイルドカード、末尾ドット、大文字への置換、複数 SAN、DNS 以外の SAN を受理しない。SAN がない場合だけ、UTF8String / PrintableString の唯一の CN に同形式を許す。SAN と CN が両方あれば完全一致が必要で、曖昧な名前や CN への暗黙の逃げ道を認めない。
- CN 単独の場合は、提示チェーンが leaf 1枚だけで、設定したすべての trust anchor に NameConstraints がない場合に限定する。DNS の名前制約は CN に適用されないため、中間 CA や制約付き root を使う場合は SAN を必須にする。SAN の名前制約は webpki が検査する。SAN/CN 抽出用の小さな DER リーダーは境界・長さ・タグを検査するが、X.509 の署名・期限検証を代替しない。
- 発信時はルート表の期待ノード ID とサーバー証明書を照合し、受信時はクライアント証明書とフレームの送信元を照合する。宛先 ID と接続開始時の受信側 incarnation も一致させる。別の許可ノードへのなりすましを防ぐ。payload 内の提案や ACK を実行する権限・現在の epoch の妥当性は、上位の合意・fencing 層で別途確認する。
- TLS 再開と 0-RTT は無効。接続ごとに証明書を再検証する。失効リストとleaf証明書の重複期間付きローテーションは後に実装された。OCSPと証明書配布・本番運用への自動統合は未完成。[rustls のチェーン検証 API](https://docs.rs/rustls/0.23.46/rustls/client/fn.verify_server_cert_signed_by_trust_anchor.html) は失効検査を含まない。設定した CA の秘密鍵と発行権限は運用側で管理する。

### 起動・制限・配送契約

`TlsTransportConfig::new(node_id, identity, trusted_cas)` は `127.0.0.1:0`、外部 bind 禁止、許可ノードは自分のみで作成する。`TlsIdentity` に leaf から順の DER 証明書チェーンと `Secret<PrivateKeyDer>` を渡し、`allowed_nodes` に自分と通信先を設定する。`TlsTransport::bind(config).await` 後、`local_addr()` を使って `set_peer(node_id, SocketAddr)` で許可済みノードの経路を登録できる。経路登録で許可ノードを増やすことはできない。起動時に自身の証明書・鍵・ノード ID・信頼・期限を検査する。秘密鍵は Debug に表示せず、エラーは固定の分類だけを持つ。`Secret` 自体にメモリ消去や OS の鍵保管・ACL 管理はない。

外部アドレスや wildcard bind は `allow_external_bind = true` を明示した場合のみ受理し、固定文の警告を stderr に出す。IPv4 / IPv6 とも loopback 判定を適用する。外部公開の到達制御・ファイアウォール・Tailscale の設定は別途必要で、私設網だけを認証とは扱わない。

| 制限 | 既定値と動作 |
|---|---|
| フレーム | u32 big-endian 長 + version 1 の固定フィールド binary。本文1 MiB、設定上限8 MiB。サイズ確認後に確保。Heartbeat の配列件数も残バイト数で制限し、余剰バイト・未知version/tag・不正Durationを拒否 |
| 接続 | 入出力合計32（設定1〜1024）。TCP accept直後・TLS処理前に共有枠を確保し、超過接続を閉じる。完了・失敗・取消で枠を返す |
| キュー | 送信・受信・診断それぞれ128件（設定1〜65536）。満杯時は明示拒否、診断キューのみ分類を破棄し拒否件数を維持 |
| 時間 | 接続確立＋TLS handshake、フレーム読取、各書込に既定2秒（設定は0より大きく300秒以下）。読取はlength prefixと本文を含む絶対期限で、1バイトずつ送っても延長しない |
| 終了 | `shutdown().await` は受付を停止し接続タスクを取消・終了待機する。未配信キューを破棄。Dropでもタスク取消を要求するが、終了待機にはshutdownを使用 |

MessagePack は現 workspace では dev-dependency のみで、依存追加は禁止のため、再帰構造のない専用 binary codec を採用した。Heartbeat / OwnershipProposal / OwnershipAck / OwnershipDecision のみを運ぶ。独自の合意アルゴリズムではない。1接続1メッセージで、受信キューに登録した後に1バイト ACK を返す。恒常接続・自動再送・永続スプール・処理完了 ACK・全接続間の順序保証・アプリケーションの重複排除はない。

同期の `Transport::send` 成功は**ローカル送信キューへの登録**を意味する。後続の通信失敗は `take_error()` と `stats()` で確認する。`send_confirmed(...).await` は相手のメモリ内キューへの登録 ACK を待つが、操作完了や永続 commit を意味しない。書込後の timeout・ACK喪失・Future取消では配送済みか不明になり得る。無条件に再試行しない。`stats().rejected` は ACK 送信失敗を含む受信側接続エラーで、既に登録済みの `received` と重なる場合があり、未配信件数ではない。許可ノードによる占有・連続接続の速度制限、公平なノード別帯域配分、OS のaccept backlog / メモリ全体の予算は実装していない。

`incarnation` は再起動ごとに呼出側が非ゼロの新しい値を割り当てる契約で、永続採番は未実装。Transport の時刻は各プロセスの経過時間で、payload の heartbeat / lease / 提案期限は元の時計領域を保持する。既存 PeerTable / cluster_sim は共有 Clock を前提にするため、実ホスト間の時計比較や安全な lease へそのまま転用しない。mTLS や heartbeat 喪失だけで quorum 承認・昇格権限を作らない。

### 過去の参考記録: Windows / Rust 1.99.0 での検証範囲

テスト内だけで rcgen により CA・ノード証明書を生成し、秘密鍵をファイルやログへ保存しない。当時のWindows開発機（Windows / Rust 1.99.0、サポート外）の実 TCP loopback で4種類のメッセージ往復、SAN / CN、偽 CA・未許可・不一致・期限・EKU・NameConstraints・平文・TLSバージョン・ALPN の拒否、巨大/不正フレーム、slowloris、接続・キュー上限と回復、shutdown を検証した。送信側の handshake / greeting / ACK の無応答、length prefix の小分け送信、shutdown 取消後の再待機も試験した。PeerTable / Failover には共通 ManualClock と実 TCP で届いた heartbeat を供給し、途絶の Suspected → FailoverPending と quorum 不足での Degraded を検証した。分散 consensus や実 Shard 起動を検証した試験ではない。

`src/` から次の確認に成功した。新規は実 TCP 統合25件と DER / codec / 名前の単体3件で、関連既存試験を含む145件と doctest 4件は失敗・ignoreとも0件。workspace 全体の実行試験は再実行していない。

```text
cargo test --offline --lib --test tls_transport --test cluster_sim --test failover --test failover_ownership
cargo test --offline --doc
cargo clippy --offline --workspace --all-targets -- -D warnings
cargo fmt --all --check
git diff --check
```

Linux 実行、IPv6実通信、複数ホスト/Tailscale、実 Discord（ユーザー検証範囲）、OS suspend・時計ずれ、本番 quorum/lease、負荷性能、当時の証明書失効・ローテーション、永続監査との接続は未検証または未実装だった。新機能として `fs_perms`、`cert_rotation`/`cert_policy`/CA、`raft_quorum`（mTLS Raft）、`cluster_runtime` の Raft 経路、`child_recovery`、`update_rollback` と daemon の update 制御、`subscription`/`durable_jobs`、`FrameworkStateStore` とファイル永続 `StateStore`、`metrics_feed`、`discord_smoke`、手動ワークフロー `stress.yml`/`bench.yml` が追加されているが、実Discord検証はユーザーが実施する範囲である。既存制約文書・進捗の統合更新は PM に引き継ぎ、`docs/progress.md` は変更しない。
