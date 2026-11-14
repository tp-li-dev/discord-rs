# Linux（Ubuntu）での botd 常駐運用手順

> [!WARNING]
> この文書と `deploy/` の成果物は、**実機の systemd / Ubuntu では未検証**です（作成時にはファイル内容と
> ソースとの照合だけを行いました。`systemctl` の起動・停止・再起動、hardening 指定の互換性、journald 出力は
> 実行していません）。導入時は「[導入後の確認](#導入後の確認)」で自分の環境の挙動を確かめてください。
> 本番デプロイ前にはユーザー（運用責任者）の承認を得てください。対応 OS は Linux（Ubuntu）のみです。

実装契約と制約の基準は [architecture.md](architecture.md)、[運用ガイド](operator-guide.md)、
[security.md](security.md)、[known-limitations.md](known-limitations.md) です。

## 先に知っておくべき制約

1. **botctl で操作できます（secret ファイル経由）。** unit は `botd --secret-file /etc/discord-rs/botd.secret
   --control-port 47321` で起動し、`botctl` は同じファイルと `--port 47321` で loopback へ接続します
   （「[管理 secret](#管理-secret)」）。secret ファイルの権限検査・systemd 実機での常駐接続は実機では未検証です。
   自動検出用 endpoint ファイルはなく、ポートは固定値を `--port` で指定します。
2. **systemd Watchdog / `Type=notify` は未対応。** botd は `sd_notify` を実装していません
   （src 内に該当コードなし）。unit では `Type=simple` とし、`Type=notify` と `WatchdogSec=` はコメントのまま
   無効です。botd 側の対応後に有効化してください。生死監視は `Restart=on-failure`（プロセス終了の検知）のみです。
3. **Gateway 子プロセスのログは journald に出ません。** botd は Gateway 子の stdout/stderr を `null` へ
   向けています（`src/src/daemon.rs`）。journald に出るのは botd 自身の標準出力（起動時の 1 行 JSON）と
   標準エラーのみです。
4. **本番の Raft 有効化は拒否のままです。** `--allow-raft-quorum` は `--mock-discord` を必須とし、本番 backend
   起動は拒否されます。本手順では使いません。`--mock-discord`、`--allow-test-quorum`、
   `--allow-test-gateway-switch` 等の試験用オプションも unit には入れていません。
5. **複数ホストの実障害（ホスト停止・網分断・failover）は未検証です。** botd は設定の `node.id` と
   一致する primary の Shard だけを起動し、他ホストの Shard は操作・取得しません。
6. **更新・rollback は実行できません。** 通常 runtime は更新実行を拒否します（後述）。

## 前提

- OS: Ubuntu（LTS を推奨）。`systemd` と `journald` を使用します。本リポジトリでの Linux 検証は
  GitHub Actions の `ubuntu-latest` 上で行っており、特定の Ubuntu バージョンでの動作確認記録はありません。
- Rust ツールチェイン: **1.90.0**（`.github/workflows/ci.yml` と同じ。`rustup toolchain install 1.90.0`）。
  ビルドは別マシンまたは専用ビルド用ユーザーで行い、本番ホストに toolchain を置かない運用も可能です。
- Bot token を取得済みで、`bots.toml` に使う Bot ID（u64 の 10 進文字列）・application ID を把握していること。

## ビルド

リポジトリルートから `src/` へ移動して実行します。`src/Cargo.toml` の `[[bin]]` に `botctl`、`botd` があり、
Gateway はパッケージ名と同じ `src`（`src/src/main.rs`）です。

```sh
cd src
cargo build --release --locked --bin src --bin botd --bin botctl
ls target/release/src target/release/botd target/release/botctl
```

成果物名は `src`（Gateway）、`botd`、`botctl` です（`--bin` の指定は `Cargo.toml` と
`src/src/bin/` の実在で確認済み。ビルド自体はこの文書の作成時には実行していません）。

## ユーザーとディレクトリ

```sh
# 専用ユーザー（ログイン不可・ホーム無し）
sudo useradd --system --user-group --no-create-home --shell /usr/sbin/nologin discord-rs

# 実行ファイル
sudo install -d -o root -g root -m 0755 /opt/discord-rs/bin
sudo install -o root -g root -m 0755 src/target/release/botd   /opt/discord-rs/bin/botd
sudo install -o root -g root -m 0755 src/target/release/botctl /opt/discord-rs/bin/botctl
sudo install -o root -g root -m 0755 src/target/release/src    /opt/discord-rs/bin/src

# 設定（秘密値を含めない配置設定）
sudo install -d -o root -g discord-rs -m 0750 /etc/discord-rs
sudo install -o root -g discord-rs -m 0640 src/examples/placement.toml /etc/discord-rs/bots.toml   # 編集して使う
```

権限の方針:

| 対象 | 所有者:グループ | mode | 理由 |
|---|---|---|---|
| `/opt/discord-rs/bin/*` | root:root | 0755 | 実行ファイルを `discord-rs` ユーザーが書換えられないようにする（任意バイナリ実行の回避） |
| `/etc/discord-rs/` | root:discord-rs | 0750 | |
| `/etc/discord-rs/bots.toml` | root:discord-rs | 0640 | 秘密値は書かない |
| `/etc/discord-rs/botd.env` | root:discord-rs | 0640 または root:root 0600 | token を含む。systemd が root で読むため 0600 でも読める |
| `/etc/discord-rs/botd.secret` | discord-rs:discord-rs | 0600（0400 も可） | 管理用 secret。botd/botctl が所有者・モードを検査する（下記「管理 secret」） |
| `/var/lib/discord-rs`（状態） | discord-rs:discord-rs | 0700 | unit の `StateDirectory=` が作成・維持する |

`/opt` 配下に実行ファイルを置く場合、Gateway（`src`）は botd の `--gateway-program` に絶対パスで渡します
（相対パスは起動エラー）。

### 設定ファイル

`/etc/discord-rs/bots.toml` の書式は [運用ガイド](operator-guide.md) の例と
`src/examples/placement.toml` を参照してください。`node.id` に一致する primary の Shard だけがそのホストで
起動します。`bot_id` は u64 の正規 10 進文字列、`token_env` は Bot ごとに別名にします。
編集後にオフライン検査します（daemon へ接続せず、token や Discord 接続は確認しません）。

```sh
/opt/discord-rs/bin/botctl config validate --file /etc/discord-rs/bots.toml --json
```

### token の置き場所

```sh
sudo install -o root -g discord-rs -m 0640 deploy/botd.env.example /etc/discord-rs/botd.env
sudo -e /etc/discord-rs/botd.env     # 変数名は bots.toml の token_env に合わせ、値を入れる
```

値をコマンドラインに書いたり、`echo >>` で履歴に残したりしないでください。リポジトリ内のファイルに token を
書かないでください。botd は `token_env` の環境変数から token を読み、そのBot用 Gateway にだけ渡します。Core
（Worker）へ token は渡りません。

### 管理 secret

`botctl` が接続するための管理 secret（32 byte。64 文字の 16 進 1 行）をファイルで渡します。この unit は
`ExecStart` で `--secret-file /etc/discord-rs/botd.secret --control-port 47321` を指定します（`LoadCredential=` は使わない
方式に統一。コードは root 所有 0400 の `$CREDENTIALS_DIRECTORY` 配下も受理しますが、その構成は本手順の対象外で未検証です）。

作成と配置（初回のみ。secret の値は表示されません）:

```sh
sudo /opt/discord-rs/bin/botctl secret generate --out /etc/discord-rs/botd.secret   # 0600 で新規作成。既存ファイルは上書きしない
sudo chown discord-rs:discord-rs /etc/discord-rs/botd.secret                       # 所有者 = botd を動かすユーザー
sudo ls -l /etc/discord-rs/botd.secret                                              # -rw------- discord-rs discord-rs を確認
```

botd は次をすべて満たさないと起動を拒否します（理由の種別だけを journald に出し、パスや値は出しません）:
絶対パスの通常ファイル（シンボリックリンク・追加のハードリンクは不可）、モードが 0600 または 0400、所有者が botd の実効
ユーザー（`discord-rs`）、64 文字の 16 進、全ゼロなど極端に弱い値でないこと。`botctl` も同じ検査をします。

- **botctl の実行**: secret ファイルは `discord-rs` 所有の 0600 なので、そのユーザーで実行します。
  `sudo -u discord-rs /opt/discord-rs/bin/botctl status --secret-file /etc/discord-rs/botd.secret --port 47321 --json`
  （`--port` は unit の `--control-port` と一致させる）。管理 listener は 127.0.0.1 のみで、外部へ公開・転送しない
  でください。ポートが他の用途と衝突する場合は unit と `--port` の両方を変更します。
- **secret の保護**: ファイルをバックアップする場合は暗号化した保管庫へ。チャット・チケット・シェル履歴に貼らない。
  この secret を知る主体は botd をローカルで完全に操作できます（role・scope は付与されません）。
- **ローテーション**: 稼働中の再読込はありません。新しいファイルを作って置き換え、botd を再起動します。
  ```sh
  sudo /opt/discord-rs/bin/botctl secret generate --out /etc/discord-rs/botd.secret.new
  sudo chown discord-rs:discord-rs /etc/discord-rs/botd.secret.new
  sudo mv /etc/discord-rs/botd.secret.new /etc/discord-rs/botd.secret
  sudo systemctl restart botd.service      # 再起動で Gateway が一時的に切れる（無停止ではない）
  ```
  古い secret は再起動後に無効になります。旧ファイルのコピーが残っていないか確認してください。
- 検査しないもの: 途中ディレクトリのシンボリックリンク、POSIX ACL、ファイルシステム側の強制。`/etc/discord-rs` は
  root 所有の 0750 のままにしてください。

## unit の設置と systemd 操作

```sh
sudo install -o root -g root -m 0644 deploy/systemd/botd.service /etc/systemd/system/botd.service
sudo systemd-analyze verify /etc/systemd/system/botd.service   # 構文確認（ExecStart 実体が必要）
sudo systemctl daemon-reload
sudo systemctl enable --now botd.service
```

```sh
systemctl status botd.service
sudo systemctl restart botd.service
sudo systemctl stop botd.service
sudo systemctl disable botd.service
```

unit の要点（`deploy/systemd/botd.service` 内のコメントも参照）:

- 起動引数は `--file`、`--runtime-root`、`--gateway-program`、`--secret-file`、`--control-port`（`src/src/bin/botd.rs` の USAGE）。
  `--runtime-root` は `/var/lib/discord-rs/runtime`（`StateDirectory=` で作成）。
- `User=discord-rs`、`NoNewPrivileges=yes`、`ProtectSystem=strict`、`ProtectHome=yes`、`PrivateTmp=yes`、
  `ReadWritePaths=/var/lib/discord-rs`、`EnvironmentFile=/etc/discord-rs/botd.env`、
  `Restart=on-failure`、`RestartSec=5s`、`LimitNOFILE=65536`、ログは journald。
- **`KillSignal=SIGINT`**: botd が停止要求として扱うのは SIGINT のみで、systemd 既定の SIGTERM では停止処理に入りません。
  `KillMode=mixed` で主プロセスにだけ SIGINT を送り、`TimeoutStopSec=60` 後に残りを SIGKILL します。
  botd は終了時に子を無条件で kill せず、起動予約を保持します（運用ガイド「daemon終了時」）。停止後に Gateway 子が
  cgroup の SIGKILL で終了する場合、Gateway の graceful stop（受付済み処理とREST結果確認の完了）は保証されません。
  この停止シーケンスの実機検証はしていません。
- `Restart=on-failure` による再起動後、前回の子の孤児再採用は未実装です。再起動で重複起動が拒否される場合があります
  （「障害時の確認項目」参照）。

### 導入後の確認

```sh
systemctl is-active botd.service
journalctl -u botd.service -n 50 --no-pager
sudo -u discord-rs ls -ld /var/lib/discord-rs /var/lib/discord-rs/runtime
sudo ls -l /etc/discord-rs/botd.env
ps -o pid,user,cmd -C botd,src
```

確認項目: botd が `discord-rs` ユーザーで動作していること、状態ディレクトリが 0700 であること、journald に起動直後の
1 行 JSON（`port`、`local_only`、`startup_ok`）が出ること（secret は含まれない）、Bot ごとに Gateway プロセス（`src`）が
起動していること。**Discord への接続成功（READY）はここでは判定できません**（`Running` は READY/Heartbeat/lease を
証明しません。運用ガイド参照）。

## botctl での確認

`botctl` の `ps`/`status`/`inspect`/`shards`/`start`/`stop`/`restart`/`operation` は、「[管理 secret](#管理-secret)」の
secret ファイルと固定ポートで systemd 配下の botd に接続して実行できます（`discord-rs` ユーザーで実行）。

```sh
B="sudo -u discord-rs /opt/discord-rs/bin/botctl"
S="--secret-file /etc/discord-rs/botd.secret --port 47321 --json"
$B ps $S
$B status MUSIC_BOT $S
$B restart MUSIC_BOT $S --timeout 30s
```

`status` の `Running` は子プロセスの存在観測であり、Discord への接続（READY）を証明しません（運用ガイド）。
オフライン検査は secret 不要です。

```sh
/opt/discord-rs/bin/botctl config validate --file /etc/discord-rs/bots.toml --json
cd src && cargo run --offline --bin doctor -- examples/placement.toml   # 配置の構文・整合性のみ
```

`botctl logs` / `nodes` / `doctor` は未提供です（運用ガイド）。Bot の稼働確認は、`ps`/`journalctl` と Discord 側の
実際の応答（検証用ギルド）で行ってください。

## ログ

```sh
journalctl -u botd.service -f                       # 追尾
journalctl -u botd.service --since "1 hour ago" --no-pager
journalctl -u botd.service -p err                   # エラー以上
journalctl -u botd.service -b                       # 今回の起動以降
journalctl --disk-usage                             # 容量確認
```

- journald に出るのは botd のログのみです。Gateway 子のログは現状どこにも保存されません（制約 3）。
- Bot token・Interaction token・IPC 秘密情報はログに出さない設計ですが、「全ログ経路の秘密マスク」は未検証です
  （運用ガイド）。ログを共有する前に値が含まれないか確認してください。
- 管理ディレクトリの `recovery-audit.jsonl` と `update-audit.jsonl` は `--runtime-root` 配下に作られます。

## 更新手順

**バイナリの更新**（実機での手順。通常運用の更新は「停止して入替え」のみ）:

1. 新しいバイナリを別マシンで `cargo build --release --locked` し、検証・ハッシュを控える。
2. `sudo systemctl stop botd.service`（停止時の Gateway の扱いは上記の未検証点を確認）。
3. `/opt/discord-rs/bin/` の実行ファイルを `install` で入替え（旧版を `*.bak` 等で保持）。
4. `sudo systemctl start botd.service` し、「導入後の確認」を再実行。

この手順は Discord との接続が一時的に切れ、メモリ上の状態を失います。無停止更新ではありません。

**`botctl update` / `rollback`**（`src/src/bin/botctl.rs`、`src/src/update_rollback.rs` の実際の仕様）:

- `--plan manifest.json` が必須です（daemon 由来の計画は未実装）。`--component core|gateway|all`（既定 `core`）、
  `--rolling` を指定できます。manifest は 64 KiB 以下の JSON で、パスは manifest のあるディレクトリから解決し、
  対象成果物の SHA-256 を照合します。秘密情報を含めないでください。
- **既定は dry-run** で、`--dry-run` と同じ扱いです。dry-run はオフラインで、daemon に接続せず、`--bootstrap-stdin`
  や `--secret-file` を付けるとエラーです。
- `--yes` と認証（`--bootstrap-stdin` または `--secret-file ... --port ...`）を付けたときだけ検証後に認証付きで daemon へ
  要求を送ります（`--dry-run` と同時指定は不可、`--operation-id` は `--yes` が必要）。**通常 runtime（本番の botd）は
  更新実行を拒否します**（「update execution is unavailable in the default runtime」）。実行経路は模擬 Discord の
  試験専用 runtime のみです。したがって本番で `--yes` を付けても更新は実行されません。
  dry-run の出力を更新の成否とみなさないでください。

```sh
/opt/discord-rs/bin/botctl update music --plan manifest.json --component core --dry-run --json
```

## バックアップ

- 設定: `/etc/discord-rs/bots.toml`（秘密値なし）と `deploy/` の unit をバージョン管理または別保管する。
- token: `/etc/discord-rs/botd.env` は暗号化した保管庫など安全な場所へ。平文のバックアップを共有ストレージに置かない。
  復元後に 0640/0600 の権限を再確認する。Bot token の再発行（Discord 開発者ポータル）で復旧する手順も用意しておく。
- 状態: `/var/lib/discord-rs`（監査ログ `recovery-audit.jsonl`、`update-audit.jsonl` 等）。取得は botd を停止して行う
  のが安全です。業務状態は原則メモリ内で、状態パスの存在は永続化や再起動時の復元を意味しません（運用ガイド）。バックアップも
  0700 のまま保管する。復元手順の実機検証はしていません。

## 障害時の確認項目

| 症状 | 確認 |
|---|---|
| 起動しない | `systemctl status botd` と `journalctl -u botd -n 100`。`botd: ...` のエラーメッセージを確認（USAGE 不一致、絶対パス要件、`bots.toml` の検査エラー等） |
| 直後に失敗して再起動を繰り返す | `StartLimitBurst` 超過で停止する。原因を直し `sudo systemctl reset-failed botd` の後に起動 |
| env ファイル関連 | `/etc/discord-rs/botd.env` の存在・権限・変数名が `token_env` と一致するか。値が空でないか |
| `Permission denied` | `/var/lib/discord-rs` の所有者 `discord-rs`・0700、`/opt` の実行権、`ProtectSystem=strict` で書込が必要な別パスがないか |
| Gateway 子（`src`）が起動しない | `--gateway-program` が絶対パスの実行ファイルか。`ps -C src`。Gateway のログは保存されない（制約 3） |
| 再起動後に Bot が起動しない／重複起動拒否 | botd は終了時に子を kill せず起動予約を保持する。孤児の照合・再採用は未実装。`ps -C src` で孤児を確認し、外部で実態確認のうえ運用者が整理する（運用ガイド「daemon終了時」）。予約を削除する場合は事前確認が必要 |
| Discord に接続できない | ネットワーク/DNS、token 失効、Intent 設定、Identify 制限。READY は botd では観測できないため、Discord 側の状態で確認する |
| ファイル数不足 | `systemctl show botd -p LimitNOFILE`、`ls /proc/PID/fd | wc -l` |

## 実 Discord での検証

実 Discord に接続する検証は [real-discord-verification.md](real-discord-verification.md) の手順・承認条件に従い、
ユーザーが実施する範囲です。本番ギルドでの実行には事前承認が必須です。この文書の手順で動作確認した記録はありません。

## できないこと・未検証事項

- 「botctl で操作できる」ことの実機検証: secret ファイル（`--secret-file`）の権限検査と systemd 配下の botd への
  `botctl` 接続は、実機の Ubuntu / systemd では未検証です（CI は模擬 Discord・`--no-autostart` の botd で確認）。
  稼働中の secret 再読込、役割・scope 付きの capability 指定、`LoadCredential=` 構成も対象外・未検証です。
- systemd `Type=notify` / `WatchdogSec`（botd が `sd_notify` 未対応）。対応後に unit のコメントを外して有効化する。
- Gateway 子プロセスのログ収集（journald に出ない）。
- 本番での Raft quorum 有効化（拒否のまま）、分散 Identify・failover の本番統合。
- 複数ホストでの実障害（ホスト停止・網分断・failover）。
- 無停止更新、`botctl update`/`rollback` の本番実行、原子的 rollback。
- 孤児プロセスの再採用、停電時の永続性、`SIGINT` → `KillMode=mixed` 停止の実機での挙動。
- `deploy/systemd/botd.service` の hardening 指定が各 Ubuntu バージョンで起動を妨げないこと。
