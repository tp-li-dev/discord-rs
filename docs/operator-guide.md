# 運用ガイド（試作段階）

現在の管理機能はローカルの単一 Bot・単一 Shard を対象にした試作です。複数 Bot の配置、リモート管理、quorum/lease、フェイルオーバー、Gateway の並行更新は未実装です。実 Discord、Windows、複数ノードでの検証結果は [progress.md](progress.md) を確認してください。本番運用の可用性や無損失は保証していません。

## 配置のオフライン検証

`src` ディレクトリから次を実行できます。

```sh
cargo run --bin doctor -- examples/placement.toml --resolved-shards bot-two=1
```

`total_shards = "auto"` の Bot ごとに `--resolved-shards BOT_ID=COUNT` を指定します。この値は手入力であり、Discord の推奨値として検証されません。未指定、重複、範囲外などは失敗します。設定ファイルは最大 1 MiB で未知フィールドを拒否します。doctor は構文と配置の整合性のみを調べ、認証情報、Discord、IPC、ノード接続、quorum、稼働状態を確認せず、プロセスも起動しません。配置ファイルは現行 Daemon の設定として読み込まれません。

## Discord コマンド登録

登録は自動実行されません。`src` ディレクトリで、まず application ID と `global` または guild ID、リンク済みコマンド名を指定し、内容を確認します。次は dry run であり、ネットワーク要求を送信しません。

```sh
cargo run --bin register -- 123456789012345678 global ping
```

実際に反映する場合は末尾に `--apply` を付け、`DISCORD_TOKEN` を環境変数で渡します。対象スコープに同名コマンドがある場合は上書きされます。これは Discord 側の変更です。実行前に対象アプリ・スコープ・名前を確認してください。他のコマンドの一括置換は行いません。実 Discord での登録は未検証です。

## 既存管理と状態

既存の `manage` の起動・更新・停止方法と Worker の制約は [README](../README.md) を参照してください。`status` のプロセス生存表示は Discord の接続成功を意味しません。Worker 更新は順次行われ、途中で失敗すると新旧が混在し得ます。timeout した要求や外部送信の結果は不明になり得ます。メモリ内の明示的 snapshot は親プロセス停止で失われ、`StateMap` は Worker ごとに存在し、更新や再起動で失われます。

`core-runtime/core.json` はローカル管理の認証情報を含みます。管理ディレクトリ、Worker バイナリ、更新指定ファイルのアクセスを信頼できる管理者に限定し、トークンを設定ファイルやログへ記載しないでください。Windows の権限保護は親ディレクトリの ACL に依存します。現在の管理処理は認証済み操作とビルドを直列で処理し、更新中は他の管理操作や監視が待機することがあります。
