# twilight-gateway の上流とローカル差分

crates.io の `twilight-gateway 0.18.0` を採用しています。配布物の `.cargo_vcs_info.json` が示す上流コミットは `twilight-rs/twilight@f3bf23257ffb799c28c7a885a2dc4acc2e449e83` です。
`src/Cargo.toml` の `[patch.crates-io]` がこのディレクトリを参照します。パス依存の `src/Cargo.lock` には配布物のチェックサムがないため、[crates.io のレジストリインデックス](https://index.crates.io/tw/il/twilight-gateway) の該当版の `cksum` をここに記録します。

Crate SHA-256: d8c9e5765f6cd5abb3cc936bd6bcf92f69414e2fe247764e150982664441cd50

`src/config.rs` と `src/shard.rs` の変更は WebSocket の Identify 送信直前に任意のガードを呼び出し、送信許可を取得します。配布物に含まれない `LICENSE.md` を同梱し、配布用の `.cargo_vcs_info.json`、`Cargo.lock`、`Cargo.toml.orig` は省いています。

## 更新手順（Linux）

1. リポジトリルートで `bash deploy/dev/check-vendor.sh --offline /path/to/old.crate` を実行し、現在の差分が記録と一致することを確認します。旧版の配布物を一時ディレクトリに展開し、`git diff --no-index` で旧版と現在の `src/config.rs`、`src/shard.rs` を比較してパッチを控えます。
2. `bash deploy/dev/check-upstream-updates.sh` で候補を調べ、上流の変更履歴・セキュリティ修正・関連 Twilight クレートとの互換性を確認します。`https://static.crates.io/crates/twilight-gateway/twilight-gateway-<version>.crate` から新版を取得し、`sha256sum` の値をレジストリインデックスの該当版の `cksum` と照合してから、別の空の一時ディレクトリに展開します。
3. 新版のソースを取り込み、旧版にのみ存在したファイルを確認して整理します。新しい `.cargo_vcs_info.json` から版・コミットを記録し、ライセンスと配布用ファイルの扱いを見直します。Identify パッチを移植し、Hello からの新規 Identify と Resume の分岐、Identify キュー、WebSocket の `start_send` に対するガードの位置を確認します。
4. 必要に応じて `src/Cargo.toml` の依存バージョンを合わせ、`src` で `cargo update -p twilight-gateway --precise <version>` を実行します。`src/Cargo.lock` が新版のパス依存を指すことを確認し、上の `Crate SHA-256:` 行を照合済みの値で更新します。
5. `PATCHED_FILES` をいったん空にして `bash deploy/dev/check-vendor.sh --offline /path/to/new.crate` を実行し、一覧の `A`（追加）、`D`（省略）、`M`（変更）をレビューします。承認した `A` と `M` は `src/vendor/twilight-gateway` を作業ディレクトリにして `sha256sum <相対パス>` の出力を記録します。`D` は `-`、半角スペース 2 個、相対パスの順で各行に記録します。比較対象から除く管理ファイルは、ルート直下の `UPSTREAM.md` と `PATCHED_FILES` です。
6. 同じオフライン検査を再実行し、一覧と差分量を確認します。`bash deploy/dev/check-vendor.test.sh` と `cargo test --manifest-path src/Cargo.toml --locked --test identify_send_grants` を実行し、CI も確認します。アーカイブのパスを省略した場合は `${CARGO_HOME:-$HOME/.cargo}/registry/cache/*` のキャッシュを使います。キャッシュ取得手順は CI と同じく `src` で `cargo fetch --locked` を実行した後、ワークスペース外で `cargo info --registry crates-io twilight-gateway@<version>` を実行します。
