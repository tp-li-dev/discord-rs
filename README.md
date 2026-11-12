# Rust Discord Bot Framework（試作）

Rust / Tokio / Twilight による Discord Bot と、属性マクロを使う実験的な framework です。Cargo workspace は **`src/`** にあります。[architecture.md](docs/architecture.md) は目標仕様であり、実装完了を意味しません。

`botd` はローカル設定に基づき複数 Bot の Shard ごとに Gateway 子プロセスを起動し、各 Gateway が既定2個の Worker を管理します。Worker はチャンネルIDで振り分けられ、Shardそのものではありません。別経路の `manage` は単一 Bot を管理します。分散所有権と実 Gateway の並行更新は未完成です。

## 現在の機能

- `#[hybrid_command]` のリンク済み定義を収集し、prefix と ChatInput の共通ハンドラーを実行。`String` / `i64` / `bool` と末尾の `Option<T>` に対応。
- 実行系に `ping`、`greet`、`state_counter` と Ready イベントを接続。Cog のコマンド・Ready・状態共有・ライフサイクルは framework のテストで検証する段階で、実Workerには未接続。
- `doctor` によるオフライン配置検査、`register` による1コマンドの登録計画表示と明示適用。
- `manage` による起動・状態・ログ・Worker更新・停止、結果不明の外部応答の読み取り専用照会。
- `botd` / `botctl` による認証付きローカル複数Bot管理と明示的な模擬Discordモード。`botctl update` / `rollback` は未実装。
- `manage` のローカル権限判定・監査とJSON / Prometheus形式のmetrics。コンポーネントルーティング、揮発性StateStore、Supervisor、mTLS通信、クラスタシミュレーションの基盤もありますが、実Workerや本番分散制御との接続は限定的です。
- Workerごとの順次更新と、明示したカウンター・計算ジョブ状態の引き継ぎ。外部応答が結果不明なら該当Workerの更新を拒否。

実Worker IPC・模擬入力の試験と、実Discordでの動作は区別してください。実Discord接続・登録・応答、障害下の無停止・無欠落・exactly-onceは未検証または非保証です。詳細は [既知の制約](docs/known-limitations.md) にあります。

## 手元での確認

リポジトリルートから実行します。取得済み依存を使います。

```sh
cd src
cargo test --offline --workspace
cargo fmt --all --check
cargo clippy --offline --workspace --all-targets -- -D warnings
cargo run --offline --bin doctor -- examples/placement.toml --resolved-shards bot-two=1
cargo run --offline --bin register -- 123456789012345678 global greet
```

最後は仮の application ID に対する dry-run で、Discordには送信しません。Rust と各OSのネイティブビルド環境が必要です。

## 起動と更新

`src/` で、秘密をログやソースに残さない方法で `DISCORD_TOKEN` を環境変数に設定します。prefixには Message Content Intent と対象チャンネルの閲覧・送信権限が必要です。起動時に Gateway 情報を取得し、設定総Shard数が推奨値未満なら起動を拒否します。`manage` の既定は1です。

```sh
cargo run --offline --bin manage -- start
cargo run --offline --bin manage -- status
cargo run --offline --bin manage -- update
cargo run --offline --bin manage -- logs
cargo run --offline --bin manage -- metrics-json
cargo run --offline --bin manage -- list-unknown
```

`start` は管理プロセスをバックグラウンド起動します。内部のビルド・更新にもofflineを適用する場合は、起動前から `CARGO_NET_OFFLINE=true` を環境に設定してください。`status` はプロセス生存表示で、Discord接続成功の証明ではありません。停止・再起動はメモリ状態を失います。

`!ping`、`!greet Alice 3`、`!count`、`!job start` / `!job status` / `!job stop`、`!version`、`!workers` が実装されています。slashは自動登録されません。手順と更新失敗時の扱いは [運用ガイド](docs/operator-guide.md)、APIとローカルサンプルは [開発者ガイド](docs/developer-guide.md) を参照してください。

設計・検証履歴は [progress.md](docs/progress.md)、IPCは [ipc-protocol.md](docs/ipc-protocol.md)、秘密と権限は [security.md](docs/security.md)、限定的なWindows codec測定は [benchmarks.md](docs/benchmarks.md) にあります。
