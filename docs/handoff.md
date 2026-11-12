# 引き継ぎ（別環境への移行用）

作成: 2026-11-12。この文書は、これまでの PM 主導・並列開発の方針、進め方、現状、残課題、運用上の注意をまとめたもの。仕様の基準は [architecture.md](architecture.md)、進捗の詳細は [progress.md](progress.md)、適合状況は [conformance.md](conformance.md)、保証できない点は [known-limitations.md](known-limitations.md)。

## 1. 現状の要約
- main に全作業が統合済み。Windows / Rust 1.99.0 で `cargo fmt --all --check`、`cargo clippy --workspace --all-targets -- -D warnings`、`cargo test --workspace` が成功（647件、失敗0、無視1＝手動ベンチ、2回連続）。
- 実装済み（模擬環境で検証）: 属性マクロ・Cog・型付き引数・コンポーネント経路(framework)、Worker IPC（handshake、event_seq、drain、ホットスワップ T04〜T06/T19）、Supervisor、StateStore、metrics、RBAC・監査、REST/Identify 予算、Gateway 並行切替の状態機械、failover 状態機械と ownership(epoch/lease/fencing)、cluster シミュレーション、mTLS Transport、botd/botctl、複数 botd の分散 E2E（試験用過半数 ACK）、testkit、examples、ベンチ。
- **未検証**: 実 Discord、Linux、GitHub Actions の実行結果、複数ホストの実障害、Windows ACL、本番の合意(openraft は評価止まり)。完成扱いしない。
- cargo プロジェクトは **`src/`**（workspace: `framework`, `framework-macros`）。コマンドは `src/` で実行する。

## 2. 方針（AGENTS.md の要点）
- `docs/architecture.md` が仕様の基準。既存コードを調査してから変更する。
- Gateway/Core の Shard 単位分離、複数 Bot、認証、quorum と lease による安全性を維持。**独自コンセンサスは実装しない**。
- 秘密情報を記録しない。Core に Bot token を渡さない。未認証の遠隔管理を許可しない。
- 機能ごとにテストし、**未検証の保証を完成と主張しない**。Windows/Linux と障害条件を考慮。
- 進捗・判断・未解決事項・検証結果は `docs/progress.md` に記録。**本番デプロイ・破壊的操作・秘密情報の変更は事前確認**。

## 3. 進め方（PM + Codex CLI 並列）
ユーザー指示: 「Claude が PM。コードは基本的に Claude が直接書かず、Codex CLI を使って並列に進める。承認を求めず自律的に」。本番デプロイ・破壊的操作・秘密情報の変更のみ事前確認する。

1. 作業を**ファイル所有範囲が重ならない**単位に分割（共有してよいのは `src/src/lib.rs` の `pub mod` 行のみ）。
2. 作業ごとに git worktree + ブランチ `codex/<name>` を作る（短い ASCII パス推奨。Windows のパス長制限で長い scratchpad 配下は削除に失敗した）。
   ```bash
   git worktree add -b codex/<name> C:/Users/<user>/cw/<name> main
   ```
3. 指示文は**引用符付き heredoc**（`<<'EOF'`）で書く（バッククォートがシェルに展開され指示が壊れた事故あり）。共通ルール（AGENTS.md/architecture.md/progress.md を読む、担当範囲外を触らない、progress.md は編集しない、`--offline` で cargo、commit しない、独立部分はサブエージェントで並列可、最後に日本語レポート）を毎回付ける。
4. 起動（バックグラウンド、worktree 単位）:
   ```bash
   CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
   codex exec -C <worktree> -m <model> -s workspace-write \
     -c model_reasoning_effort=<level> -o <out.txt> - < prompt.txt > log.txt 2>&1 &
   ```
5. Codex の **サンドボックスにはネットワークと `.git` 書込みがない**。cargo の完全検証とコミットは PM（Claude）が行う。依存追加が必要なら PM が事前に `cargo add` + `cargo fetch` する（openraft / rmp-serde / tokio-rustls / rcgen はこの方法で追加済み）。
6. 完了後: 各 worktree の `src/target` を削除 → `cargo fmt --all` → コミット（`git -c user.name=... -c user.email=...` で一時 ID を使用）→ main へ `--no-ff` マージ（`lib.rs` の mod 行の競合は両方残して解決）→ `src/` で fmt/clippy/test を実行 → progress.md を更新 → worktree 削除。失敗が出たら修正専用の worktree を切って Codex に直させる。
7. `rm -rf` に変数展開を使う場合は `"${VAR:?}"` 形式にする（安全チェックで拒否される）。

### モデル使い分け（ユーザー指定）
- `gpt-6.1-terra`: 高速、機械的・軽めの作業。
- `gpt-6-astra`: **`max` のときのみ**使用（最難関の設計・統合）。今日限りコスト高 OK とのことだった。
- `gpt-6.1-sol`（既定）: 通常の実装（high）。
- 各 Codex 内のサブエージェント（`multi_agent` 機能、既定で有効）を並列利用してよい。同一ファイルを複数が編集しない。

### 運用上の落とし穴
- **ディスク満杯**: worktree ごとの `target` が各 3〜5GB になる。並列は 4〜6 本程度、統合後は都度 `src/target` を削除。`CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0` で縮小。
- cargo の共有 `CARGO_TARGET_DIR` を異なる worktree で使うと、同名 crate の古い成果物を再利用して誤動作することがあった。
- テストのフィクスチャは過去に rlib 選択でフレークした（修正済み）。新しい fixture は Cargo の依存解決/JSON 成果物取得を使う。
- 統合の副作用（例: 秘密マスクがエラー診断を潰して試験失敗）は、各ブランチ単体で緑でも merge 後に出る。**必ず main で全試験を通す**。

## 4. 残課題（優先順）
1. **Linux と GitHub Actions の実行確認**（`.github/workflows/ci.yml` は Windows/Ubuntu、examples ビルド含む。未実行）。性能ベンチの Linux 再測定（IPC 往復 p99 < 1ms 目標との比較）。
2. **実 Discord での検証**: Gateway 接続、Slash 登録（`register` は既定 dry-run、`--apply` は要確認）、Interaction 3 秒応答、Identify 予算。要 Bot token（秘密情報は環境変数、リポジトリに置かない）。
3. **Daemon↔Gateway↔Core の本番配線**: 現状の Worker は Core の代替であり、Shard 単位 Core として完成扱いしない。Gateway 並行更新（`gateway_switch.rs`）を実 Gateway/WebSocket へ接続。
4. **実合意の導入**: `docs/quorum-evaluation.md` の openraft 評価に基づく QuorumCoordinator、永続ストレージ、lease/watchdog/fencing の統合。現状の過半数 ACK は試験専用で、本番では明示フラグなしに起動しない。
5. StateStore と framework コンポーネント経路(Button/Modal)の Worker 接続、永続 Subscription 配送、`wait_for`、永続ジョブ。
6. `update`/`rollback` の botctl 実装、証明書失効・ローテーション、Windows ACL、遠隔管理 RBAC の経路統合、メトリクスの実測値接続、ログ経路の網羅的秘密保護。
7. 未解決: 一過性だった T04 の初回失敗と修正後の原因同一性は未確定。

## 5. 環境構築・検証コマンド
```bash
cd src
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
cargo build --workspace --examples
cargo run --bin doctor -- examples/placement.toml --resolved-shards bot-two=1
```
`experiments/quorum-spike` は独立した Cargo プロジェクト（メイン workspace 外、`cd experiments/quorum-spike && cargo test`）。

## 6. 文書の所在
| 文書 | 内容 |
|---|---|
| [architecture.md](architecture.md) | 仕様（基準） |
| [progress.md](progress.md) | 日付付きの進捗・判断・検証結果 |
| [conformance.md](conformance.md) | §16.2 完了条件と T01〜T20 の適合状況 |
| [known-limitations.md](known-limitations.md) | 未実装・保証できない点 |
| [ipc-protocol.md](ipc-protocol.md) / [security.md](security.md) / [benchmarks.md](benchmarks.md) | IPC、セキュリティ、実測 |
| [quorum-evaluation.md](quorum-evaluation.md) / [shard-runtime-design.md](shard-runtime-design.md) / [local-response-management.md](local-response-management.md) | 設計・評価 |
| [developer-guide.md](developer-guide.md) / [operator-guide.md](operator-guide.md) | 利用ガイド |
