# 実機試験：Gateway と Core の分離（`botd --core-mode separate`）

実 Discord・実 systemd（WSL2 の Ubuntu でよい）で、分離レイアウトを確認する手順書。**この手順は実機では未実施**（実プロセス試験 `botd_separate_cores` と、Actions の実 systemd 上の smoke は、模擬 Discord で成功済み）。設計は [gateway-core-separation-design.md](gateway-core-separation-design.md) の §5.4。

前提：[real-operations-verification.md](real-operations-verification.md) の §0（ユーザー、ディレクトリ、`bots.toml`、secret、`botd.env`、unit の導入）が済み、embedded で `!ping`・`!count` が動くこと。Bot はテスト用、Shard は 1 つだけ。token は `deploy/dev/set-token.sh` で設定し、チャットやコマンドに貼らない。

> **WSL2 の注意**：開いた端末がないとアイドルで VM ごと止まる。試験中は WSL の端末を開いたままにする。

## 1. separate に切り替える（drop-in）

```bash
sudo systemctl edit botd      # 開いたエディタに、次の 3 行以上を書く（既存の ExecStart を置き換える）
```
```ini
[Service]
ExecStart=
ExecStart=/opt/discord-rs/bin/botd --file /etc/discord-rs/bots.toml --runtime-root /var/lib/discord-rs/runtime --gateway-program /opt/discord-rs/bin/src --secret-file /etc/discord-rs/botd.secret --token-stdin --control-port 47321 --stop-children-on-exit --core-mode separate
```
```bash
sudo systemctl restart botd
sudo systemctl status botd --no-pager
```
`--core-mode separate` は `--stop-children-on-exit` が必須。戻すときは `sudo systemctl revert botd`。

## 2. 以降で使う確認コマンド

```bash
# Gateway・Core の PID と親（Core は botd の子、Gateway も botd の子で、Core の子ではない）
MAIN=$(systemctl show botd -p MainPID --value)
ps -u discord-rs -o pid,ppid,etime,cmd | grep -E "src( |$)|core-listen" | grep -v grep
# 状態（separate のときだけ各 Shard 行に cores が付く）
sudo -u discord-rs /opt/discord-rs/bin/botctl status --secret-file /etc/discord-rs/botd.secret --port 47321 --json
# ipc ディレクトリ（ソケット・secret・core ログ。bot-<botid の 16 進>/shard-0/ipc）
sudo ls -la /var/lib/discord-rs/runtime/bot-*/shard-0/ipc
```
Discord 側では、そのつど `!ping` と `!count` を送って返答と数を見る。

## 3. 試験項目と期待

| # | 操作 | 期待（これが外れたら記録して止める） |
|---|---|---|
| 1 | `systemctl restart botd` 後の起動 | ログイン成功。`!ping`・`!count` が返る。Core が 2 本、親が botd。ipc に `core-0.sock`・`core-1.sock`・`core.secret`・`core-*.log`（`sock`/`secret` は 0600、ディレクトリは 0700） |
| 2 | Gateway を `sudo kill -9 <Gateway PID>` | 数秒で Gateway が復旧（Identify はローカル許可）。**Core の PID が不変**。`!count` が**続く**（巻き戻らない） |
| 3 | Core を 1 本 `sudo kill -9` | botd がその Core だけを約 1〜2 秒後に再起動。**Gateway の PID が不変**。`!count` が続く（Gateway が持つ確認済みスナップショットが空の新 Core に入る。確認済み以降の最大約 2.5 秒分は失われ得る） |
| 4 | Core を 2 本とも `kill -9` | Gateway は不変。両方再起動し再接続。切断中の入力は拒否・破棄され得る（記録する） |
| 5 | `systemctl stop botd` | `Result=success`。Gateway・Core とも残らない（`ps` で 0）。ipc に `*.sock`・`core.secret` が残らない。`core-state.json` は残る |
| 6 | `sudo kill -9 <botd の PID>` | systemd が botd を再起動（約 6 秒）。**Core も入れ替わる**（期待どおり。Core の `!count` は Gateway の永続スナップショットから戻る）。古い起動予約が整理される |
| 7 | 数値を記録 | Gateway の再起動時間（#2。embedded と比べる）、Core の再起動時間（#3）、#4 の拒否された入力の数 |

## 4. まだ確認できないこと（記録しておく）

- 長時間稼働（Oracle の放置は別記録）、3 Shard 以上、複数 Shard の繰り返し、Resume（再起動をまたぐもの）、レート制限。（2 Shard は WSL の実 Discord で 1 回確認済み。`docs/progress.md`）
- Core のホットスワップ（`worker-path.txt`）は separate では動かない（未対応）。
- Gateway の無停止更新（旧新 Gateway の同時接続）は未実装。
- botd の `kill -9` 後に systemd 外で Core が孤児になる場合の扱い：Core は stdin の EOF で終了するので残らない設計だが、実機では未確認。

## 5. 結果の記録

試験後、`docs/progress.md` に日付・環境（WSL か実サーバ）・各項目の成功／失敗・数値を書く。失敗したら `journalctl -u botd`、`ipc/core-*.log`、Gateway の `gateway.log` を見る（token や secret を貼らない）。
