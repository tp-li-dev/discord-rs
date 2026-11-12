# 実 Discord 検証の準備と手動手順

この文書は検証計画であり、実 Discord での成功記録ではない。仕様は
[architecture.md](architecture.md)、現状の制約は [known-limitations.md](known-limitations.md)
を基準とする。今回の実装では実 Discord 接続・登録・Interaction・Identify・Resume を実行していない。

## 前提と承認

- 専用のテスト Bot・Application・テストギルド・テストチャンネルを使用する。
  対象の所有者、使用者、権限、実行時間帯、上限回数、中止責任者を事前に決める。
- **本番ギルドで実行する場合は、読み取り確認を含めユーザーの事前承認を必須とする。**
  テストでの承認を本番へ流用しない。登録・応答送信・接続切断等は各段階の範囲を承認する。
- Bot token は秘密注入機構から実行プロセスの環境変数へ設定済みとする。
  コマンド行、設定ファイル、シェル履歴、記録、スクリーンショットに値を載せない。
  ビルドは秘密注入前に行う。環境変数一覧や HTTP 詳細ログを収集しない。
- token を持つのはこの疎通確認プロセスまたは専用 Gateway のみ。Core/Worker へ渡さない。
  管理ポートを公開せず、認証付きローカル管理を使用する。
- 全操作は単一実行者で直列に行う。同一 Bot を使う他のプロセス・ホストが停止済みか確認する。
  quorum/lease/fencing を回避して本番の分散起動を試さない。

## 読み取り専用 CLI

追加 bin は `discord_smoke`。使用する Rust 依存は既存のものだけである。
`--token-env NAME` は環境変数の**名前**を受け取り、値そのものを受け取るオプションはない。
既定名は `DISCORD_TEST_BOT_TOKEN`。名前は `[A-Za-z_][A-Za-z0-9_]*`、128文字以内。
名前自体も出力せず、既定またはカスタムの別だけを表示する。

```text
cd src
cargo build --offline -p src --bin discord_smoke -j 1
```

Windows PowerShell（ビルド後、必要なら秘密を環境へ注入してから）:

```powershell
.\target\debug\discord_smoke.exe
.\target\debug\discord_smoke.exe --token-env DISCORD_TEST_BOT_TOKEN
# 明示的な読み取り確認。実行前に対象と承認範囲を確認する。
.\target\debug\discord_smoke.exe --token-env DISCORD_TEST_BOT_TOKEN --apply
```

Linux:

```bash
./target/debug/discord_smoke
./target/debug/discord_smoke --token-env DISCORD_TEST_BOT_TOKEN
# 明示的な読み取り確認。実行前に対象と承認範囲を確認する。
./target/debug/discord_smoke --token-env DISCORD_TEST_BOT_TOKEN --apply
```

`CARGO_TARGET_DIR` を利用している場合はその出力先の実行ファイルを使う。

### 手順と期待結果

1. `--apply` なしで起動する。設定の構文と環境変数の有無（missing/empty/present）だけを確認する。
   token 未設定でも終了コード0。HTTP client・非同期 runtime を作らず、ネットワークへ何も送らない。
   token の有効性・テスト Bot の所属・権限の証明ではない。
2. 専用テスト Bot の raw token（`Bot ` 等の接頭辞なし）を安全に注入する。
   `--apply` は token の欠落・空・非Unicode・空白や改行等の形式不正を固定種別で拒否する。
   形式検査は資格情報の有効性を保証しない。
3. `--apply` で `https://discord.com/api/v10/gateway/bot` の GET を1回行う。
   URLとメタデータの形式検査が成功した場合だけ、`/users/@me` の GET を1回行う。
   後者は Bot ユーザーであることも確認する。URL・ユーザー名・ID・レスポンス本文は表示しない。
   これらは [Gateway情報](https://docs.discord.com/developers/events/gateway#get-gateway-bot) と
   [現在のユーザー情報](https://docs.discord.com/developers/resources/user#get-current-user)
   の読み取り API である。メッセージ送信、コマンド登録、状態変更、WebSocket 接続は行わない。
4. 両方成功で終了コード0。設定・実行・HTTP・復号等の失敗は1。
   最初の確認が失敗した場合、次の GET は skipped。依存ライブラリのエラー本文は出力しない。

出力例（値は固定の分類のみ）:

```text
mode=dry_run config=valid token_env=default token=missing requests=none
check=gateway_bot result=success kind=ok rate_limit_headers=present
check=current_user result=success kind=ok rate_limit_headers=absent
result=failure kind=token_missing rate_limit_headers=unknown
```

各例は独立した行の見本であり、同一実行の記録ではない。
`kind` は authentication / forbidden / rate_limited / server / http /
timeout / transport / invalid_response / request 等。タイムアウトは疎通成功と扱わない。

`rate_limit_headers` は `X-RateLimit-*` または `Retry-After` の存在だけを示す。
値は出さない。HTTP成功後のモデル解析失敗でも、取得済みのヘッダ有無は保持する。
**Twilight 0.18 は HTTP失敗時のヘッダを呼び出し元へ返さないため、失敗時は unknown となる。**
unknown は absent を意味しない。これはレート制限対応全体の検証ではない。
[Discordのレート制限資料](https://docs.discord.com/developers/topics/rate-limits) を実行前に確認する。

各チェックは待機と本文解析を含め10秒で打ち切る。最長は概ね20秒＋ローカル初期化時間。
Twilight の送信前ゲートは各 GET の最初の試行だけを許し、429後の再送を拒否する。
429後の limiter 待機が先に期限へ達すると timeout になり得る。
ゲートを設定できなければ送信せず retry_guard_unavailable とする。自動再実行しない。
raw HTTP 429のヘッダ収集・実ネットワークでの再試行抑止は今回未検証。

## 以下は別途行う手動検証（今回実行しない）

以下の操作を `discord_smoke --apply` で行うことはできない。
専用 Gateway/Worker の実経路と、秘密を記録しない計測手段をレビューしてから行う。
必要な計測が未接続なら「未実装のため保留」と記録し、模擬結果を実測に置き換えない。

### Slash 登録

1. Application ID とテスト guild ID、リンク済み `ping` の schema を確認する。
   既存コマンド一覧を認証済みの読み取りで確認し、同名を使っていない専用 Application を選ぶ。
   同名が存在したらこの手順では変更せず中止する。
2. 秘密を注入する前に `register` を `cargo build --offline -p src --bin register -j 1` でビルドする。
   `<register実行ファイル> <application-id> <test-guild-id> ping` で計画を確認する。
   この既存 CLI は token 環境変数名が `DISCORD_TOKEN` 固定であり、smoke の名前設定は継承しない。
3. 対象・schema・復旧方法をレビューし承認された場合だけ、同じ引数に `--apply` を付け1回登録する。
   global scope、全件一括置換、起動時の自動登録を使わない。
4. 認証済み読み取りとクライアント表示で、該当 guild の1件だけが期待する schema で利用可能か確認する。

期待結果: dry-run は無変更。apply 後にテストコマンド1件だけが追加される。
[登録APIは同名を上書きする](https://docs.discord.com/developers/interactions/application-commands#create-guild-application-command)
ため、終了コードだけで他のコマンドが無変更と判断しない。
HTTP timeout/失敗は結果不明とし、一覧照合前に再送しない。

ロールバック: 作成した1件の command ID と schema を照合し、事前承認した認証付き削除手段でその1件だけ削除し、一覧を再確認する。
既存 `register` に削除機能はないため、削除手段の準備ができるまで apply しない。
誤上書き時は停止し、保存済みの非秘密 schema と権限設定を確認して承認の上で復元する。

### Interaction の3秒応答

1. 上記のテストコマンドと、単一 Bot/Shard の実 Gateway→Core→Gateway→REST 経路を用意する。
   応答は専用チャンネルまたは対応済みの ephemeral に限定し、追加ジョブ・メンションを無効にする。
2. 秘密を含まないローカル相関番号で、Interaction受信、Core配送、初回応答/Defer の送信開始と
   HTTP確認、後続処理終了の単調時計を測る。生 Interaction・callback URL・token・本文を保存しない。
3. 通常処理1回、承認済みの5秒処理1回を直列で呼ぶ。長い処理では初回 Defer の確認後にだけ処理を続ける。
   Discord上の操作から応答表示までの観測も併記する。受信後だけの時計は配送前遅延を測れない。

期待結果: [初回応答期限](https://docs.discord.com/developers/interactions/receiving-and-responding#followup-messages)
を守り、初回応答または Defer が3秒以内に成功し、二重ACKがない。
HTTP確認の計測だけで Discord受理時刻の厳密な証明とはしない。
期限超過、ACK結果不明、二重応答、非承認チャンネルへの送信が1回でもあれば中止。
ロールバック: 追加呼出しを止め、試験プロセスを graceful 停止する。
テスト応答の削除が必要なら承認済みの対象だけ処理する。送信済み内容はプロセス停止で取り消せない。

### Identify 予算

1. 別途レビュー済みの読み取り計測で `/gateway/bot` の session_start_limit
   （total / remaining / reset_after / max_concurrency）を取得し、日時と数値だけを記録する。
   smoke はこれらの値を出力しない。値を取得する安全な計測器がなければ保留する。
2. 同じ Bot を使う全プロセスを確認し、今回の新規 Identify 予算を最大1回、残す安全余裕を事前に決める。
   remaining が必要回数＋余裕に満たなければ起動せず、reset_after 後に改めて読み取る。
3. 専用 Gateway 1接続だけを開始し、送信 opcode と時刻の分類、READY受信、Heartbeat ACK を観測する。
   Bot全体の開始調整を確認し、max_concurrency に応じる bucket と開始間隔を守る。
   複数ホスト間の予算共有は未完成なので、本手順で並列起動しない。
4. 事後に同じ読み取りを1回行い、残量変化と実送信数を照合する。

期待結果: 許可した1回の Identify で READY に到達し、予算の想定外消費がない。
上限到達を意図的に起こさない。残量なしの拒否・複数Shardの窓競合は模擬試験で行う。
[Gateway仕様](https://docs.discord.com/developers/events/gateway#identifying) の最新条件を実行前に確認する。
ロールバック: 起動・自動再接続を停止する。消費した Identify 予算は巻き戻せないため、回復まで待つ。

### Resume

1. Identify 試験の同じセッションを使用する。Gateway だけが session_id、resume_gateway_url、最終 sequence を
   メモリに保持し、Core や記録へ値を渡さない。保存有無・opcode・経過時間だけ観測する。
2. 他のアプリに影響しない専用接続だけを1回切断する。OS全体の firewall/ネットワーク変更を使わない。
   close 1000/1001 はセッションを無効にするため Resume成功の試験に使わない。
3. 同じ version/encoding と resume_gateway_url で再接続し、Resume(opcode 6)、RESUMED、Heartbeat ACK を確認する。
   再接続上限1回と観測期限30秒を設定し、自動 Identify への無承認フォールバックを禁止する。
4. 必要なら事前承認した予算内でのみ新規 Identify を別ケースとして扱う。
   INVALID_SESSION や再認証が発生したケースを Resume成功に数えない。

期待結果: [Resume手順](https://docs.discord.com/developers/events/gateway#resuming) に沿って
RESUMED に至り、追加 Identify がなく、受信再開を観測できる。
全イベントの欠落ゼロ・Exactly Once はこの1回の試験で保証しない。
ロールバック: 障害注入を解除し、追加再接続を止め、専用 Gateway を停止する。
安定セッションへの復旧が必要なら予算と承認を確認した別計画で行う。

## 共通の中止基準と復旧

- 対象 Bot/guild/scope が想定外、承認範囲不明、Coreへの秘密継承、秘密らしい出力を検出したら即停止する。
- 401/403/429、5xx、timeout、TLS/DNS異常、READY/RESUMED未達、Heartbeat ACK喪失で次段階へ進まない。
  429はヘッダ有無だけから待機時間を推測せず、担当者が安全な経路で確認する。自動再送しない。
- 二重Active、想定外の再Identify、未承認の状態変更、他Bot/利用者への影響を検知したら停止する。
- 読み取り smoke は停止・終了だけでよく、Discord上の書込みを戻す必要はない。
  秘密注入を解除する。秘密漏洩の疑いがあれば記録を共有せず隔離し、値を転載しない。
  token の失効・再発行はユーザー確認後に行う。
- 状態変更を伴う試験は各節のロールバックと事後照合を行う。復旧不能・結果不明はそのまま記録し、成功としない。

## 記録テンプレート（秘密・生レスポンス禁止）

```text
実行日時 / 担当者:
承認参照 / 対象区分（テスト・本番） / 許可操作 / 最大回数:
コード revision / OS / rustc / Twilight version:
Bot・guild・channel の非秘密管理別名 / Shard / 世代:
事前条件 / 既存コマンド衝突なし / 他プロセス確認:
ケース（dry-run・GET・Slash・Interaction・Identify・Resume）:
手順（token値・秘密URLなし） / 実施回数:
期待結果 / 実測結果 / 成功・失敗・結果不明・保留:
GET分類 / rate_limit_headers（present・absent・unknown） / 終了コード:
Interaction 受信→初回送信開始ms / HTTP確認ms / UI観測 / 二重ACK数:
Identify 事前・事後予算 / 安全余裕 / 送信数 / READY / Heartbeat ACK:
Resume 回数 / 再接続ms / RESUMED / 追加Identify数:
中止理由 / 影響 / ロールバック実施と照合 / 未解決事項:
記録の秘密混入チェック済み:
```

## ローカルの対象検証

`src/` で実行する。新規試験は bin の単体試験に取り込み、他の bin 全体のビルドを避ける。
実 token・外部ネットワークを使わず、子プロセスだけに合成の秘密文字列を設定して出力を確認する。
token なしの loopback 模擬 HTTP で Twilight の429経路が GET を再送しないことと、
成功応答・不正本文でのヘッダ有無の保持を確認する。これは実 Discord の挙動の保証ではない。

```text
cargo test --offline -p src --bin discord_smoke -j 1
cargo clippy --offline -p src --bin discord_smoke -j 1 -- -D warnings
rustfmt --edition 2024 --check src/bin/discord_smoke.rs tests/discord_smoke/cli.rs
```

Windows/Linux 共通の標準ライブラリと既存 Tokio/Twilight を使用するが、ローカル試験の成功を
Linux実行・実Discord疎通・障害復旧・本番安全性の証拠とはしない。

2026-11-12 の確認結果: Windows / Rust 1.90.0 で上記の対象試験11件が成功
（うち1件は子プロセス用 fixture の通常時 no-op）。対象 bin の clippy（`-D warnings`）、
追加 Rust ファイルの rustfmt check、追加3ファイルの空白検査も成功。
依存追加・commit・実 Discord へのリクエスト・登録は行っていない。
Linux、実HTTP失敗時のヘッダ取得、実 Discord の3秒応答・Identify・Resume、
分散予算・quorum/lease 統合は未検証。Slash削除の自動化とその他の手動試験の自動実行は本 bin の対象外。
