# 実 Discord 検証の準備と手動手順

この文書は検証計画であり、実 Discord での成功記録ではない。仕様は
[architecture.md](architecture.md)、現状の制約は [known-limitations.md](known-limitations.md)
を基準とする。今回の実装では実 Discord 接続・登録・Interaction・Identify・Resume を実行していない。

## 最初の 30 分（初回実行の 1 ページ手順）

> [!WARNING]
> **実 Discord への接続は、このリポジトリではまだ一度も検証されていない。** この節は初回実行の
> 手順書であり、各ステップの「期待される出力」は実装・既存文書からの想定で、実測の記録ではない。
> 実行した結果は末尾の記録テンプレートに自分で記入する。結果が想定と違ったら成功扱いにせず止める。
> 対応 OS は Linux（Ubuntu）のみ。以下は Ubuntu 上で、ビルド済みの成果物が `src/target/release/` にある状態で
> `cd src` した後のコマンドである。ビルドは token を設定する前に行う
> （`cargo build --release --locked --bin src --bin botd --bin botctl --bin register --bin discord_smoke`）。

### 0. 前提と注意

- **テスト用 Bot とテスト用ギルドだけ**を使う。本番ギルド・本番 Bot では実行しない（本番で行う場合はユーザーの事前承認が必須。
  テストでの承認は流用できない）。同じ Bot token を使う他のプロセス・ホストが動いていないことを確認する。
- **token はシェル履歴・チャット・リポジトリ・ログ・スクリーンショットに残さない。** コマンドラインに値を書かない。
  対話シェルでは `read -s` で環境変数に入れる（入力は画面に出ず、履歴にも残らない）。

  ```bash
  read -rs DISCORD_TEST_BOT_TOKEN   # token を貼り付けて Enter（画面には何も出ない）
  export DISCORD_TEST_BOT_TOKEN
  ```

  raw token のみ（`Bot ` の接頭辞・引用符・改行を含めない）。シェルを閉じれば消える。
  systemd で動かす場合は `/etc/discord-rs/botd.env`（root:root 0600 または root:discord-rs 0640、`KEY=VALUE` 形式・`export` なし）に
  `sudo -e` で書く（[linux-deployment.md](linux-deployment.md)）。手動起動で env ファイルを使う場合も 0600 にする。
- **Developer Portal の設定**（テスト用 Application のみ）:
  1. Application を作成し、Bot タブで Bot を作る。「Reset Token」で token を再生成し、表示された値をその場で上記の
     `read -s` に貼る（再表示できない。漏れた可能性がある token は使わず再生成する）。
  2. 特権 Gateway Intents（Bot タブ）で **Message Content Intent を有効にする。**
     - 必要な機能: `!ping` などの prefix コマンド（メッセージ本文を読むため）。
     - 必要でない機能: Slash コマンド（`/ping`）。Interaction で届くので本文を要しない。
     - **ただし同梱 Gateway は Intent を固定で `GUILD_MESSAGES | MESSAGE_CONTENT` として要求する（`src/src/main.rs`）。**
       Slash のみで使う場合でも、ポータルで無効のままだと Discord が接続を拒否すると想定される（Gateway close 4014。
       コードに専用処理はなく、実際の見え方は未検証）。その場合 botd の `restarts` / `startup_failures` が増える（ステップ 4）。
     - Presence / Server Members Intent は使わないので有効にしない。
  3. ギルドへの招待（OAuth2 の URL Generator）: scope は `bot` と `applications.commands`。Bot 権限は最小限
     （テストチャンネルでの View Channel / Send Messages / Read Message History 程度）。招待先はテストギルドだけ。
  4. 控える値（非秘密）: Application ID、テストギルド ID（Discord の開発者モードで取得）、Bot のユーザー ID
     （`bots.toml` の `bot_id` は u64 の 10 進文字列。UUID や任意文字列は実 Gateway の起動で失敗する）。

### 1. 設定の検証（接続なし）

`bots.toml` は 1 Bot・1 Shard の最小構成にする（[operator-guide.md](operator-guide.md) の例を参照。秘密値は書かない）。
`token_env` は上で使う環境変数名（この手順では `DISCORD_TEST_BOT_TOKEN`）に合わせる。

```bash
./target/release/botctl config validate --file /abs/path/bots.toml --json
./target/release/discord_smoke            # --apply なし = dry-run。ネットワーク接続なし
```

### 2. 認証だけの疎通（読み取り専用）

```bash
./target/release/discord_smoke --apply    # GET /gateway/bot と GET /users/@me の 2 件のみ。変更・WebSocket 接続なし
```

`--token-env NAME` で環境変数の名前を変えられる（値は渡せない）。失敗しても自動再実行はしない。人間も再実行しない（下表）。

### 3. Slash コマンド登録

`register` の token 環境変数名は **`DISCORD_TOKEN` 固定**（`discord_smoke` の `--token-env` は効かない）。引数は
`<application-id> <global|guild-id> <command-name> [--apply]`。**テストギルドの ID を指定するギルド限定登録を使い、`global` は使わない**
（グローバルは反映に時間がかかり、取消しも難しい）。コマンド名は `ping`（リンク済みコマンド。他に `greet`、`state_counter` 等がある）。

```bash
export DISCORD_TOKEN="$DISCORD_TEST_BOT_TOKEN"   # 履歴に残るのは変数名だけで、値は残らない
./target/release/register <application-id> <guild-id> ping            # dry-run: 送信なし
./target/release/register <application-id> <guild-id> ping --apply    # 1 件だけ upsert（同名は上書き）
unset DISCORD_TOKEN
```

### 3.5 【初回は推奨】Gateway を単体で起動して、出力を直接見る

事前の静的レビュー（2026-11-14）で分かった、初回接続の落とし穴への対策。**botd 経由だと Gateway の標準出力・標準エラーが
破棄される**（`daemon.rs` で `Stdio::null()`）ため、接続の失敗理由（認証失敗、特権 Intent 未許可など）が一切見えない。
初回は、botd を使わず、Gateway（`src`）を単体で起動して、端末で出力を確認する。

```bash
# token は read -s で入力する（シェル履歴に残さない）。Gateway と register の既定名は DISCORD_TOKEN。
read -rs DISCORD_TOKEN && export DISCORD_TOKEN
./target/release/src      # Ctrl+C で停止
```

期待される出力: `Botを起動しています...` のあと `ログイン成功: <Bot の ID>`。
失敗時に出る行（close code は秘密ではない数値）:

| 出力 | 意味 | 対処 |
|---|---|---|
| `Gateway 切断: close code 4004` ＋ `Gateway が再接続できない切断で終了しました` | 認証失敗（token 誤り） | **ここで止める**。token を再生成し、リトライしない |
| `Gateway 切断: close code 4014` ＋ 同上 | 特権 Intent が未許可（MESSAGE_CONTENT） | Developer Portal で Message Content Intent を有効化して再実行。Identify を 1 回消費している |
| `Gateway 切断: close code 4010〜4013` ＋ 同上 | シャード/バージョン/Intent の指定不正 | 止めて報告する |
| 何も出ずに終了 | 想定外 | 止めて報告する |

注意:
- **Gateway は `MESSAGE_CONTENT` を含む Intent を固定で要求する**（`GUILD_MESSAGES | MESSAGE_CONTENT`）。Slash コマンドだけを使う場合でも、
  Developer Portal で **Message Content Intent を有効にしないと接続が拒否される（4014 の想定。実挙動は未検証）**。
- **token の環境変数名が違うツールがある**: Gateway と `register` は `DISCORD_TOKEN`、`discord_smoke` の既定は `DISCORD_TEST_BOT_TOKEN`
  （`--token-env` で変更可）。smoke だけ通って Gateway が `token_missing` になる場合は、名前の不一致を疑う。
- 単体起動は `SIGTERM` を処理しない（`SIGINT`＝Ctrl+C のみ）。systemd で単体起動する構成にはしない（常駐は botd 経由）。
- 失敗後に自動で再試行はしない（致命的な切断では、プロセスは終了する）。再実行する前に、原因を取り除くこと。
- ここで `ログイン成功` まで確認できたら、Ctrl+C で止めてから、ステップ 4（botd 経由）に進む。Identify は、この単体起動で 1 回、botd の起動でもう 1 回消費する。

### 4. botd の起動（1 Shard）

> **botd が読む token の環境変数名は、`bots.toml` の `token_env` と同じ名前でなければならない**（この手順書の例では `DISCORD_TEST_BOT_TOKEN`）。
> **botd を起動する端末（systemd なら EnvironmentFile）で、その名前に token を設定してから起動する**。別の端末で設定した変数は、botd には渡らない。
> 設定が無いと、子の起動前に失敗する: `botctl status --json` の `error` が `MissingCredential`、`state` が `Failed`、`restart_exhausted` が true、`gateway.log` はできない。
> 名前は `grep token_env <bots.toml>` で確認できる。値は表示せず、設定の有無だけ確認する: `[ -n "${DISCORD_TEST_BOT_TOKEN:-}" ] && echo 設定済み`。
> 起動に成功すると、Shard ごとの状態ディレクトリ（例 `<runtime-root>/bot-313031/shard-0/`）に `gateway.log`（0600）ができ、`--- gateway generation 1 ---` と `ログイン成功: …` が入る（2026-11-14 の実機で確認）。

管理用の secret ファイル（token ではない）を作る。絶対パス・0600・botd を動かすユーザー所有で、既存ファイルは上書きされない。

```bash
./target/release/botctl secret generate --out "$HOME/botd.secret"
mkdir -p "$HOME/botd-runtime"
./target/release/botd --file /abs/path/bots.toml --runtime-root "$HOME/botd-runtime" \
  --gateway-program "$(pwd)/target/release/src" --secret-file "$HOME/botd.secret" --control-port 47321
```

（フォアグラウンドで動くので、以降は別の端末で実行する。`bots.toml` は絶対パスで書く。systemd で動かす場合は
[linux-deployment.md](linux-deployment.md) の unit を使い、`sudo systemctl start botd.service`、ログは `journalctl -u botd.service`。）
**Identify はこの起動で 1 回だけ**行われる想定。起動し直すと消費が増えるので、確認なしに再起動しない。

```bash
./target/release/botctl status --secret-file "$HOME/botd.secret" --port 47321 --json
```

見る項目: 各プロセスの `state`（`Running` か）、`restarts`（0 のまま）、`restart_exhausted`（false）、`recovery` 内の `startup_failures`。
`Running` は子プロセスの存在であり、Discord の READY を証明しない（READY は Discord 上の応答で確認する）。
30 秒ほどおいて数回 status を見て、`restarts` が増えていないことを確認する。
Gateway 子のログは botd が破棄するため journald にも出ない。出るのは botd 自身の出力だけ。

### 5. Discord 上での確認

テストギルドのテストチャンネルで `/ping` と `!ping` を実行する。どちらも `Pong!` 系の応答が **3 秒以内**に返ること
（Slash は 3 秒以内に初回応答しないと Discord 側で失敗表示になる）。`!ping` は Message Content Intent が必要。
Slash だけ成功し prefix だけ失敗する場合は、Intent の設定とチャンネル権限を疑う。

### 6. 停止

> **手動で起動した botd は、必ず `botctl stop` を先に実行してから `Ctrl+C` で止める**。botd は終了時に子プロセス（Gateway と Core ワーカー）を kill しない設計（子が残る可能性があるため、起動予約を保持して重複起動を防ぐ）。
> 先に botd だけを止めると、Gateway とワーカーが残って Bot がオンラインのままになり、予約ファイル（`*.botd-reservation`）も残って、次の botd の起動を妨げる。
> 残った場合: `pgrep -af "target/release/(botd|src)"` で PID を確認 → Gateway 本体の PID を `kill`（ワーカーは子なので一緒に終わる。残れば `kill -9`）→ プロセスがないことを確認してから `find <runtime-root> -name "*.botd-reservation" -delete`。
> systemd で動かす場合は `systemctl stop`（`KillMode=mixed`）が残りも含めて止める。

```bash
./target/release/botctl stop <bot-name> --secret-file "$HOME/botd.secret" --port 47321 --json
```

その後 botd を SIGINT（フォアグラウンドなら Ctrl-C）で停止する。systemd なら `sudo systemctl stop botd.service`
（unit は `KillSignal=SIGINT`）。`ps -C botd,src` で `botd` / `src` の孤児プロセスが残っていないことを確認する。

### 各ステップの表

| ステップ | 期待される出力（想定） | 失敗時に最初に見る場所 | ここで止める基準 |
|---|---|---|---|
| 1 設定検証 | `config validate` が成功（終了コード 0）。`discord_smoke` は `mode=dry_run config=valid ... requests=none` | `bots.toml` の `bot_id`（u64 の 10 進）・`token_env` の綴り | 検証エラー。直してやり直す（接続はしていない） |
| 2 認証疎通 | `check=gateway_bot result=success kind=ok`、`check=current_user result=success kind=ok`、終了コード 0 | `kind` の値: `token_missing` / `token_format`（入力ミス）、`authentication`（401 = token 誤り） | **`authentication`（401）・`forbidden`（403）・`rate_limited`（429）は即停止。リトライしない。** 原因を確認し、ユーザー確認の上でのみ再実行 |
| 3 登録 | dry-run は `Dry run: no request sent.`。apply は `Command registered.` | Application ID・guild ID・コマンド名。`register:` で始まる固定文言（タイムアウトは「結果不明」） | `unknown linked command`、HTTP 失敗・タイムアウト（登録の有無が不明）。**再送せず**、Discord クライアントのコマンド一覧で確認してから判断 |
| 4 起動 | botd の標準出力に 1 行 JSON（`port` / `local_only` / `startup_ok`）。`status` が `Running`、`restarts` が 0 | `botctl status --json`、systemd なら `journalctl -u botd.service -n 100` | `restarts` / `startup_failures` の増加、`state` が `Failed`、`restart_exhausted` が true。botd を停止して原因確認（`bot_id` が数値か、Intent、token） |
| 5 応答確認 | `/ping` と `!ping` に 3 秒以内に応答 | `status`、Portal の Intent、Bot のチャンネル権限 | 応答なし、二重応答、想定外チャンネルへの送信。停止する |
| 6 停止 | `botctl stop` が成功、botd 終了、`ps -C botd,src` が空 | `botctl status`、`ps` | 子（`src`）が残る。ユーザー確認の上で整理する（予約の削除は事前確認が必要） |

### 全体の中止基準

次のいずれかが起きたら、その場で止めて先へ進まない。

- 401 / 403 / 429 が 1 回でも出た（429 を自動再送しない。待機時間を推測しない）。
- Identify が想定（1 回）より多く消費された疑い（再接続が繰り返される、`restarts` の増加）。
- 無限再起動（`restarts` / `startup_failures` の増加、`state` が `Failed` のまま）。
- 秘密らしい文字列がログ・端末・記録に出た。
- 対象の Bot・ギルドが想定外、本番に影響した疑い、二重応答、応答が 3 秒を超える。

### 中止した場合のロールバック

1. botd を停止する（上の「停止」）。孤児 `src` が残れば消えるまで確認する。
2. 環境変数を消す（`unset DISCORD_TEST_BOT_TOKEN DISCORD_TOKEN`）。env ファイルを使った場合は削除するか値を空にする。
   token の漏洩・誤出力が疑われる場合は、**Developer Portal の「Reset Token」で再生成**する（旧 token は失効）。
   出力を共有せず隔離し、値を転載しない。
3. 登録した Slash コマンドの削除: 既存 `register` に削除機能はない。ギルド限定で 1 件だけ登録したので、
   テスト Application を破棄するか、Discord クライアントでコマンドが残っていないか確認する。
   Discord API の `DELETE` を手動で行う場合は、ユーザー確認の上、token をコマンドラインに載せない方法で行う。
   この削除手順は本リポジトリで検証していない。
4. 消費した Identify 予算は巻き戻せない。回復まで待つ。

### 記録テンプレート（最小）

秘密・生レスポンスの値は書かない。詳細版は本書末尾の「記録テンプレート」を使う。

```text
日時 / 担当者:
コミット SHA（git rev-parse --short HEAD）:
対象（テスト Bot・テストギルド）の非秘密な別名:
ステップ1: 結果（成功/失敗/未実施）・気付き:
ステップ2: 結果 / kind / 終了コード:
ステップ3: dry-run 結果 / apply 結果（登録を Discord で確認したか）:
ステップ4: state / restarts / startup_failures / Identify は 1 回か:
ステップ5: /ping 応答時間（目視）/ !ping 応答 / 二重応答の有無:
ステップ6: 停止結果 / 孤児プロセス:
中止した場合: 理由 / ロールバック内容:
秘密混入チェック済み:
```

以降の節は詳細版で、上の手順の根拠と個別の検証項目を含む。

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

### 補足：初回実行で分かった注意（2026-11-14 の実機検証より）
- **サーバにはリポジトリを `git clone` で置く**（開発 PC のフォルダをコピーしない）。コピーすると、Git 管理外の `src/worker-path.txt` と `src/workers/`（Windows のパスと実行ファイル）が入り、Linux では `Workerのパスは絶対パスにしてください` が 2 秒ごとに出る。無害だが、出たら `worker-path.txt*` と `workers/` を削除する。
- **`bots.toml` の `bot_id` は botd 内の識別番号（先頭ゼロのない数字）** で、Discord の Bot の ID とは別。実際の Discord 側の ID は token から決まり、Gateway 起動時の `ログイン成功: <ID>` に出る。
- **token をコマンドに書かない**: `read -rs 変数名` で入力する。誤って貼り付けた場合は、シェルの履歴を消し、Developer Portal で token を再生成する。
- `botctl stop <name>` は、その Bot の Shard を止める操作で、botd 本体は動き続ける。botd を止めるのは、起動した端末での `Ctrl+C`（systemd なら `systemctl stop`）。

