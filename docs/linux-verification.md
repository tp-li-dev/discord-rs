# Linux 手動検証手順

この手順は実行計画であり、Linux や GitHub Actions の実行結果ではありません。仕様は [architecture.md](architecture.md)、現在の制約は [known-limitations.md](known-limitations.md)、測定方法の詳細は [benchmarks.md](benchmarks.md) を参照してください。実 Discord 接続や本番環境へのデプロイは含みません。

## 準備

- 専用の Linux 検証環境で、対象コミット、未コミット差分、CPU・メモリ・OS・カーネル、空き容量を記録します。テストはローカルの一時ファイル、loopback 通信、子プロセスを使用します。実行中の本番サービスやデータを対象にしないでください。
- CI と同じ Rust 1.90.0 と rustfmt / clippy、および `src/Cargo.lock` に対応する依存を事前に用意します。以下はすべてオフラインで実行し、依存が不足した場合は取得せず不足内容を記録します。`cargo --offline` は Cargo のネットワークアクセスを抑止するもので、プログラム自身の通信を禁止するものではありません。
- CI の依存キャッシュが空の場合も offline では自動取得できません。対象 OS・lockfile の依存を事前に供給する仕組みが別途必要であり、キャッシュが存在するだけで未取得分まで揃っているとは限りません。GitHub Actions での動作は実行して確認してください。
- シェルの作業ディレクトリをリポジトリルートから `src/` に移します。別 checkout の `target` を共有しません。Bot token や管理 secret は不要です。環境変数や設定ファイル全体を出力せず、ログを保存する場合も秘密情報がないことを確認します。

```sh
git rev-parse HEAD
git status --short
uname -a
rustc --version --verbose
cargo --version
df -h .
cd src
export CARGO_NET_OFFLINE=true
```

## 機能検証

各コマンドの終了コードと失敗・ignore 件数を記録します。失敗しても成功と扱わず、原因調査後に再実行した範囲を区別します。テストと clippy は成果物の競合を避け、順番に実行します。

```sh
cargo --offline fmt --all --check
cargo --offline clippy --workspace --all-targets -- -D warnings
cargo --offline build --workspace --examples
cargo --offline build --release --bin src --target-dir target/reload-build
cargo --offline test --workspace
cargo --offline run --bin doctor -- examples/placement.toml --resolved-shards bot-two=1
```

期待結果は各コマンドの終了コード 0、fmt と clippy の違反なし、テストの失敗 0、examples のビルド成功、doctor が配置検査を正常終了することです。テストの ignore は実行済みとせず件数を記録します。release ビルドは更新統合テストの制限時間内に依存を再ビルドしないための準備です。doctor はオフラインの設定検査であり、Discord 接続や稼働状態を確認しません。Linux 固有の権限試験を含め実行対象と結果をログで確認し、Windows や実ホスト障害の証拠に流用しないでください。

## ベンチ再測定

先に機能検証とビルドを終え、他の高負荷作業を避けて実行します。条件と負荷を記録し、同一環境・同一設定で複数回実施して各回の値を残します。各試行の成功・失敗・未実行件数も記録し、失敗を除外した数値だけで成功を主張しません。以下は `src/` から実行します。

```sh
cargo --offline bench --bench ipc_roundtrip -- --self-test
export IPC_BENCH_WARMUP=1000 IPC_BENCH_SAMPLES=10000
export IPC_ROUNDTRIP_WARMUP=1000 IPC_ROUNDTRIP_SAMPLES=10000
cargo --offline bench --bench ipc_codec
cargo --offline bench --bench ipc_roundtrip
cargo --offline bench --bench primitives
cargo --offline bench --bench failover_detect
```

自己検査は終了コード 0 を期待します。各ベンチの終了コード、試行数、出力されたサイズ・遅延分位値・スループット・失敗数を記録します。`ipc_roundtrip` は並列度 1/4/16 ごとに別々の Worker を使う closed-loop の往復測定で、成功・失敗・未実行の合計が設定標本数と一致し、失敗と終了失敗が 0 であることを確認します。`failover_detect` は既定で時間を要します。途中失敗や中断も記録します。

codec 単体や Worker の stdin/stdout JSON **往復**は、仕様 §14 の Linux 小型 IPC **片道 p99 < 1 ms** を直接測るものではありません。往復値を半分にして判定せず、実 Gateway/Core の経路、Unix socket、負荷、障害時の計測を別途設計します。failover ベンチは模擬状態機械と loopback mTLS を使い、実ホスト障害・quorum・Discord 再接続を含む復旧時間ではありません。詳細な測定範囲と既存 Windows 値は [benchmarks.md](benchmarks.md) を参照してください。

## 記録テンプレート

秘密情報、環境変数の値、実イベント本文は記録しません。結果は対象コミットごとに保管し、未実施項目は未実施と記載します。

```text
実施日時 / 実施者:
commit / 未コミット差分の有無:
OS・kernel / CPU・論理CPU数 / メモリ / 空き容量:
rustc -Vv / cargo version / toolchain:
プロファイル・環境設定・負荷・CPU affinity:
依存の準備状態 / offline 実行の成否:

fmt / clippy / examples / release 準備 / workspace test / doctor:
  各コマンドの終了コード、成功・失敗・ignore、失敗箇所と再実行範囲:
  Linux 固有テストの実行・結果:
GitHub Actions: 未実行または run URL、commit、各 OS の結果:

ベンチ各回: 実行時刻、設定、終了コード、成功/失敗/未実行、全出力の要約
  codec: サイズ、各操作の p50/p95/p99:
  roundtrip: 並列度ごとの p50/p95/p99/p99.9、events/s、終了失敗:
  primitives: 各処理の分位値と失敗:
  failover_detect: 試行数、検知分位値、失敗と条件:
未実施・未検証・制約・次の作業:
```
