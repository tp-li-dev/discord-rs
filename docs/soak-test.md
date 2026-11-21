# 本番サーバーでの放置試験（長時間稼働）

目的：数時間〜1 日、実サーバーで botd を動かしたままにして、**増え続ける値**（メモリ、fd、ログ、再起動、Identify の消費）がないかを見る。これは本番相当の配置の最初の試験で、**実 Discord に接続する**ので、テスト用 Bot とテスト用ギルドだけを使う。手順は [real-operations-verification.md](real-operations-verification.md) の §0 と [linux-deployment.md](linux-deployment.md)、分離は [real-separate-verification.md](real-separate-verification.md)。

## 0. 先に確認すること（サーバーで）

```bash
uname -m; lsb_release -d; getconf GNU_LIBC_VERSION
```
- **x86_64 で Ubuntu 24.04（glibc 2.39 以上）**：開発機（WSL）でビルドした `target/release/{botd,botctl,src}` をそのまま使える。
- **aarch64（Oracle の Ampere など）や別の Ubuntu**：サーバーでビルドする（`git clone` → `cargo build --release --locked --bin botd --bin botctl --bin src`。Rust 1.90 と `build-essential pkg-config` が要る）。バイナリのコピーは使えない。

## 1. 配置（docs の §0 のとおり）

専用ユーザー `discord-rs`、`/opt/discord-rs/bin/{botd,botctl,src}`、`/etc/discord-rs/{bots.toml,botd.secret,botd.env}`、`deploy/systemd/botd.service`。token は `botd.env` にだけ置く（チャットやコマンドに貼らない）。

分離で放置する場合は drop-in（実機試験と同じ）：

```bash
sudo systemctl edit botd
```
```ini
[Service]
ExecStart=
ExecStart=/opt/discord-rs/bin/botd --file /etc/discord-rs/bots.toml --runtime-root /var/lib/discord-rs/runtime --gateway-program /opt/discord-rs/bin/src --secret-file /etc/discord-rs/botd.secret --control-port 47321 --stop-children-on-exit --core-mode separate
```
```bash
sudo systemctl enable --now botd
```

## 2. 放置中の記録

`deploy/dev/soak-report.sh` を数時間おきに実行して追記する（読み取り専用。token・secret・パスは出さない）。

```bash
sudo install -m 0755 deploy/dev/soak-report.sh /usr/local/bin/soak-report
sudo soak-report | tee -a /home/ubuntu/soak.log          # 開始時
sudo soak-report | tee -a /home/ubuntu/soak.log          # 数時間後、翌日
```
cron で自動化する場合（1 時間おき。`/etc/cron.d/soak-hourly` に、root で）：`0 * * * * root /usr/local/bin/soak-report >> /home/ubuntu/soak.log 2>&1`。**`sudo su` で root になった状態の `~` は `/root` なので、記録の場所は必ず `/home/ubuntu/soak.log` と絶対パスで指定する**（`~/soak.log` と書くと、cron の記録と別のファイルになる。2026-11-16 に実際にそうなった）。

## 3. 何を見るか（合格の目安。数字は目安で、保証ではない）

| 項目 | 見方 | 異常の目安 |
|---|---|---|
| 再起動 | `NRestarts`、`generations`（gateway.log）、`restarts` | 放置中に増え続ける（理由のない再起動） |
| メモリ | 各プロセスの `rss_kB` | 時間とともに単調に増える（数 MB/時間以上） |
| fd | 各プロセスの `fds` | 増え続ける（リーク） |
| ログ | `files` の大きさ | `gateway.log`／`core-*.log` が急に大きくなる |
| 残骸 | sockets/secret・reservations の数 | 稼働中は socket 2 + secret 1、予約 1。それ以外が増える |
| 接続 | `closes`、`resumed`、`worker_unavailable` | `closes` が増えたときはログで原因と復帰結果を確認する |
| Core | `cores` の `running`／`failures` | `failures` が増える |
| 応答 | 時々 `!count` を送る | 返信がない／数が飛ぶ |

`soak-report.sh` は、Shard ごとの `gateway.log` と残っている旧 `gateway-candidate.log` から、`Gateway 切断` を含む行を `closes`、`Resume 成功` を含む行を `resumed` として別々に数えます。前者は Twilight の `GatewayClose` イベント、後者は Discord の `RESUMED` イベントの記録です。Twilight 0.18.0 には、WebSocket の読み取りが EOF で終わると Close イベントを返さずに再接続へ進む経路があります。その経路でも保持したセッションで Resume できるため、集計値から個々の切断と復帰は対応づけられません。集計は時刻の書式に依存せず、回転済みの `.log.1` は件数に含めません。

## 4. 終わり方

`sudo systemctl stop botd`（`Result=success`、残りプロセス 0、ipc に sock・secret なし）。結果は `docs/progress.md` に、日付・環境（サーバーの機種）・上の表の数値で書く。**確認できないこと**（今回の範囲外）：複数 Shard、複数ホスト、Resume、レート制限の高負荷。

## 5. 放置のあとに：新しいビルドへの入れ替え（Gateway の無停止更新の実機試験を兼ねる）

放置の記録を取り終えたら、新しいソースを入れ替える。**Core と botd は新しいバイナリで入れ替える必要があり（botd の再起動を含む）、その後の入れ替えからは Gateway だけを `gateway-update` で無停止に更新できる。**

```bash
# 1. 新しい src.zip を展開してビルド（前回と同じ。.gitignore された target は残してよい）
cd /home/ubuntu/discord-rs/src && cargo build --release --locked -j 2 --bin botd --bin botctl --bin src
# 2. 最初の入れ替え（この回は botd と Core も新しくなるので、Bot は数秒止まる）
sudo install -m 0755 target/release/botd target/release/botctl target/release/src /opt/discord-rs/bin/
sudo systemctl restart botd
# 3. 次回以降（Gateway だけの変更）：バイナリを置いて、更新を指示する。Core は止まらない
sudo install -m 0755 target/release/src /opt/discord-rs/bin/src.new && sudo mv /opt/discord-rs/bin/src.new /opt/discord-rs/bin/src
sudo -u discord-rs /opt/discord-rs/bin/botctl gateway-update test --secret-file /etc/discord-rs/botd.secret --port 47321 --json
# 状態: botctl operation <操作ID>（state が succeeded）。記録: /var/lib/discord-rs/runtime/gateway-update-audit.jsonl
```
一括確認：`sudo bash deploy/dev/update-check.sh test [--repeat N] [--shard N]` は、対象 Bot の shard 数を `botctl inspect` から取得し、更新の前後の PID、Core の不変、旧 Gateway の消滅、各 shard の IPC、マーカー、監査ログ、journald の警告を確認して PASS／FAIL の表を出す（終了コードは全 PASS のときだけ 0）。`--shard N` を省略すると全 shard を更新し、指定するとその shard だけを更新して他の Gateway の PID が変わらないことも確認する。`--repeat N` は更新を N 回繰り返し、2 回以上なら各 Gateway と Core の RSS を初回更新後の値と比較する（20% 以内）。Ubuntu の `python3` が必要。実行の記録：Oracle（実サーバー）で 1 シャードの更新に成功（全 PASS）。WSL（実 Discord、2 シャード）で、全更新と `--shard 1` が全 PASS、Core を落とした直後・存在しないシャード・`--repeat 0` が FAIL／拒否されることを確認（`docs/progress.md`）。表示が期待と違えば、スクリプト側の不具合も疑う。
更新の前後で `sudo soak-report` を取り、Core の PID が変わらないこと、`!count`（または `/ping`）が続くことを確認する。`mv` で置き換えるのは、実行中のファイルに上書きしない（`Text file busy` を避ける）ため。

### 5.1 うまくいかないとき（戻し方）

- 入れ替え前に `/opt/discord-rs/bin/` の旧バイナリを `*.prev` に退避しておく：`for f in botd botctl src; do sudo cp -p /opt/discord-rs/bin/$f /opt/discord-rs/bin/$f.prev; done`。
- 戻す：`.prev` を `mv` で元の名前に戻し、`sudo systemctl restart botd`（Bot は数秒止まる）。
- `gateway-update` が `failed` になった場合は、旧 Gateway が再起動されて Bot は応答し続けるはず。`sudo -u discord-rs /opt/discord-rs/bin/botctl operation <操作ID> ...` と `gateway-update-audit.jsonl` の最後の行、`journalctl -u botd -n 100` を控える。
- 更新中に同じ `!workers` が二重に返る可能性がある（既知の制限。`docs/known-limitations.md`）。`!count` / `!ping` の重複は Core 側で抑止される想定。

## 6. 実施記録

- **2026-11-15 13:10 UTC 〜 約 9 時間 50 分（Oracle aarch64、1 Shard、separate、旧版）**：再起動 0、worker_unavailable 0、REST unknown 0、journal 警告 0、RSS・fd に増加傾向なし（数値と限定は `docs/progress.md` の 2026-11-16 の節）。ユーザーの報告による。**`closes`（4 → 7）の原因は `gateway.log` の close code が未回収のため未確認。** 新版（2746d26）の放置は未実施。
- 次回の記録欄：新版の入れ替え日時／`update-check.sh` の結果／再放置の時間と数値／`closes` の内訳。
- **2026-11-16（WSL、実 Discord、新版）**：更新 ×4（うち 1 回は Core を落としながら）、Gateway・Core・botd の `kill -9`、`systemctl stop` を実施し、`!count` 61→79 が連続。詳細は `docs/progress.md`。Oracle での新版の入れ替えと再放置は未実施。
- **2026-11-17（Oracle、e61d8d4 版、25.0 時間）**：RSS・fd・再起動・世代・警告は OK（Gateway RSS +28 kB/h、fd 増減 0、NRestarts 0）。`closes` 11→24（WATCH、13 件。Resume か否かはこの集計では未確認）。詳細は `docs/progress.md`。
- **2026-11-17（Oracle、新版 31ee55e、8.0 時間、起点 13:51Z）**：RSS・fd・再起動・世代・警告・`closes` すべて OK（Gateway RSS +16 kB/h、fd 増減 0、NRestarts 0）。`closes` +2 と `resumed` +4 の不一致は未確認。詳細は `docs/progress.md`。
- **2026-11-21（Oracle、新版、起点 2026-11-17T13:51Z、86 時間）**：OS 再起動（2026-11-17 22:23:45）をはさんで、botd の自動起動を確認（22:23:52 に起動、以後 78 スナップショットが連続）。RSS・fd・worker_unavailable・journal の警告に異常なし。再起動後だけの数値は未取得。詳細は `docs/progress.md`。
- **2026-11-21（Oracle、新版、再起動後 77 時間）**：全プロセスが同一のまま。Gateway RSS +14 kB/h（+1.1 MB）、他は微増または 0、fd 増減 0、worker_unavailable 0、journal の警告 0。判定はすべて OK。Gateway RSS が頭打ちかは、さらに長い観測が必要。詳細は `docs/progress.md`。
