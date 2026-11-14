# 本番相当の実機試験：常時稼働・自動復旧・Core の無停止更新

systemd で常駐させた botd を、実 Discord で動かして、次の 3 つを確認する手順書。**この手順は、実 Discord と実サーバでは未実施**（systemd と自動復旧は、GitHub Actions の実 systemd 上で、模擬 Discord を使って確認済み。`.github/workflows/systemd-smoke.yml`）。

前提：[real-discord-verification.md](real-discord-verification.md) の「最初の 30 分」が、すでに成功していること（認証、Slash 登録、Gateway 単体、botd 手動起動）。テスト用 Bot とテスト用ギルドだけを使う。

## 確認できること／できないこと

| 項目 | 実 Discord で試せるか | 備考 |
|---|---|---|
| systemd での常駐、OS 再起動後の自動起動 | 試せる | |
| Core（Worker）の自動復旧 | 試せる | Worker を `kill -9` しても、Gateway は切れない |
| Gateway の自動復旧 | 試せる | 再接続のたびに Identify を 1 回消費する。1 日の上限（`session_start_limit`）に注意し、繰り返さない |
| botd 異常終了からの自動復旧 | 試せる | systemd が再起動し、古い起動予約を整理する |
| **Core（Worker）の無停止更新** | 試せる | 稼働中の Gateway を切らずに Worker を入れ替える。`!count` の状態が引き継がれる |
| **Gateway の無停止更新（ローリング）** | **試せない（未実装）** | 模擬 Discord の試験モードのみ。通常の daemon は安全に拒否する。ロードマップは `progress.md` |
| 複数ホストの failover | 試せない（未検証） | 本番の Raft は起動拒否のまま |

## 0. 準備（サーバ、root で作業）

```bash
# 専用ユーザーとディレクトリ（docs/linux-deployment.md の手順）
useradd --system --no-create-home --shell /usr/sbin/nologin discord-rs
install -d -m 0755 /opt/discord-rs/bin
install -m 0755 target/release/botd target/release/botctl target/release/src /opt/discord-rs/bin/
install -d -m 0750 -o root -g discord-rs /etc/discord-rs
```
`/etc/discord-rs/bots.toml` を作る（`bot_id` は先頭ゼロのない数字、`token_env` は Bot ごとに別名）。

```bash
install -m 0640 -o root -g discord-rs /path/to/bots.toml /etc/discord-rs/bots.toml
/opt/discord-rs/bin/botctl config validate --file /etc/discord-rs/bots.toml --json
```

secret（管理用）と token（Bot の認証）：

```bash
# 管理用 secret：サービスユーザーが所有し、0600
/opt/discord-rs/bin/botctl secret generate --out /etc/discord-rs/botd.secret --json
chown discord-rs:discord-rs /etc/discord-rs/botd.secret && chmod 0600 /etc/discord-rs/botd.secret

# token：bots.toml の token_env と同じ名前で、環境変数ファイルに書く（値をチャットや履歴に残さない）
install -m 0640 -o root -g discord-rs /dev/null /etc/discord-rs/botd.env
nano /etc/discord-rs/botd.env        # 例: DISCORD_TEST_BOT_TOKEN=（token をここに貼る。KEY=VALUE、export は付けない）
```
**`botd.env` の変数名は、`grep token_env /etc/discord-rs/bots.toml` の名前と一致させる。** 違うと `MissingCredential` で起動前に失敗する（`state: Failed`、`gateway.log` はできない）。

unit を入れて起動：

```bash
install -m 0644 deploy/systemd/botd.service /etc/systemd/system/botd.service
systemctl daemon-reload
systemctl enable --now botd
systemctl status botd --no-pager
```

状態確認（以降の `botctl` は、secret を読めるサービスユーザーで実行する）：

```bash
alias bctl='sudo -u discord-rs /opt/discord-rs/bin/botctl'
bctl status --secret-file /etc/discord-rs/botd.secret --port 47321 --json | grep -E '"(state|restarts|startup_failures|restart_exhausted|error)"'
find /var/lib/discord-rs -name gateway.log -exec tail -5 {} \;
```
期待：`state: Running`、`restarts: 0`、`startup_failures: 0`、`gateway.log` に `ログイン成功: …`。Discord で `/ping` と `!ping` が返る。

## 試験 1：常駐と OS 再起動後の自動起動
1. 上の準備のあと、`/ping` が返ることを確認。
2. `reboot` でサーバを再起動し、ログインし直して `systemctl status botd`、`bctl status …`、Discord の `/ping`。
- **期待**：再起動後、手動操作なしで `Running`、`/ping` が返る。
- **止める基準**：`MissingCredential`、`restart_exhausted: true`、起動のたびに失敗する場合。`journalctl -u botd`、`gateway.log` を確認。

## 試験 2：Core（Worker）の自動復旧
Worker は Gateway の子プロセス（`src --worker`）。
```bash
pgrep -af "src --worker"         # 2 つ出る
sudo kill -9 <ワーカーの PID を 1 つ>
sleep 3
pgrep -af "src --worker"         # 新しい PID の Worker が補充される
```
- **期待**：Gateway は切れない（Discord 上で Bot がオンラインのまま）。`/ping` が引き続き返る。
- **止める基準**：Bot がオフラインになる、`/ping` が返らなくなる。

## 試験 3：Gateway の自動復旧（Identify を 1 回消費する）
```bash
GW=$(pgrep -f 'src.* --bot-id' | head -1); echo "$GW"
sudo kill -9 "$GW"
# 数秒〜数十秒待つ
bctl status --secret-file /etc/discord-rs/botd.secret --port 47321 --json | grep -E '"(state|restarts|startup_failures)"'
find /var/lib/discord-rs -name gateway.log -exec tail -8 {} \;
```
- **期待**：`restarts` が 1 に増え、`state: Running` に戻る。`gateway.log` に `--- gateway generation 2 ---` と `ログイン成功`。数秒〜数十秒で `/ping` が返る。
- **止める基準**：`restarts` が増え続ける（無限再起動）、`startup_failures` が増える、401/4004/4014 が `gateway.log` に出る。**繰り返し試さない**（Identify の消費と、短時間の再接続の繰り返しを避ける）。

## 試験 4：botd 異常終了からの自動復旧
```bash
MAIN=$(systemctl show botd -p MainPID --value); echo "$MAIN"
sudo kill -9 "$MAIN"
# 5〜20 秒待つ（RestartSec=5s）
systemctl status botd --no-pager | head -8
bctl status --secret-file /etc/discord-rs/botd.secret --port 47321 --json | grep -E '"(state|restarts|startup_failures)"'
```
- **期待**：systemd が botd を再起動し（`NRestarts` が 1）、古い起動予約が整理されて、Gateway が起動し直す。`/ping` が返る。
- **止める基準**：`ReservationExists` で起動に失敗し続ける場合（`journalctl -u botd` に出る）。その場合は、`pgrep -af "bin/(botd|src)"` で、残った子がいないことを確認し、`find /var/lib/discord-rs -name '*.botd-reservation' -delete` で手動復旧して、報告する。

## 試験 5：停止（子も含めて止まる）
```bash
sudo systemctl stop botd
pgrep -af "bin/(botd|src)" || echo "残りなし"
```
- **期待**：`--stop-children-on-exit` により、Gateway と Worker も一緒に停止し、Bot がオフラインになる。プロセスが残らない。
- **止める基準**：プロセスが残る、停止に 60 秒以上かかる。

## 試験 6：Core（Worker）の無停止更新（Gateway を切らずに Worker を入れ替える）
稼働中の Gateway は、Shard ごとの `worker-path.txt`（状態ディレクトリ配下）を 2 秒ごとに監視し、中身の絶対パスが変わると、Worker を 1 個ずつ入れ替える。`!count` のカウンターなどが、新しい Worker に引き継がれる。

1. 準備：現在の Worker 実行ファイルを、別パスにコピーして、「更新後の成果物」の代わりにする（同じコードでも、パスが違えば入れ替えが起きる）。実際の更新では、新しいビルドの `src` を、新しいパスに置く。
   ```bash
   install -m 0755 /opt/discord-rs/bin/src /opt/discord-rs/bin/src-v2
   ```
2. Discord で `!count` を数回実行し、カウントが増えることを確認する（例：1、2、3）。
3. 更新を依頼（Shard の状態ディレクトリの `worker-path.txt` に、新しい実行ファイルの絶対パスを書く）。
   ```bash
   STATE=$(dirname "$(find /var/lib/discord-rs -name gateway.log | head -1)")/gateway-state
   echo /opt/discord-rs/bin/src-v2 | sudo -u discord-rs tee "$STATE/worker-path.txt"
   ```
4. 数秒後、`pgrep -af "src.*--worker"` で、Worker の PID が入れ替わったことを確認。`$STATE/worker-path.txt.status` に、更新後のパスが書かれる（失敗時は `.error`）。
5. 更新中も含めて、Discord で `!count` と `/ping` を、続けて実行する。
- **期待**：Bot がオフラインにならない。`!count` が途切れずに続く（例：…、4、5）。更新の前後で `/ping` が返る。
- **止める基準**：`.error` ファイルができる、Bot がオフラインになる、カウンターが 0 に戻る、応答が 3 秒を超える。
- **注意**：この試験は、Worker（Core）の更新だけ。Gateway プロセス自体の更新（Discord の接続の入れ替え）は、含まれない（上の表）。更新の成果物は、信頼できる管理者だけが書き込める場所に置く（任意のバイナリ実行につながる）。

## 記録テンプレート
```
日時 / コミット SHA：
試験 1（OS 再起動後の自動起動）：OK / NG  メモ：
試験 2（Worker の自動復旧）：OK / NG  メモ：
試験 3（Gateway の自動復旧）：OK / NG  restarts=  復旧までの秒数：
試験 4（botd 異常終了）：OK / NG  NRestarts=  メモ：
試験 5（停止）：OK / NG  残りプロセス：
試験 6（Core の無停止更新）：OK / NG  !count の前後：  .error の有無：
気付き：
```
token、secret、`botd.env` の中身は、記録に書かない。

## 後片付け
```bash
sudo systemctl stop botd            # 常駐をやめる場合。続けるなら不要
sudo systemctl disable botd         # OS 起動時の自動起動もやめる場合
rm -f /opt/discord-rs/bin/src-v2    # 試験 6 で作ったコピー
```
token を、試験用に使い回した場合は、Developer Portal で再生成する。
