# 引き継ぎ（別環境への移行用）

作成: 2026-11-12（2026-11-13 更新）。この文書は、これまでの PM 主導・並列開発の方針、進め方、現状、残課題、運用上の注意をまとめたもの。仕様の基準は [architecture.md](architecture.md)、進捗の詳細は [progress.md](progress.md)、適合状況は [conformance.md](conformance.md)、保証できない点は [known-limitations.md](known-limitations.md)。

## 0. 最新の状態（2026-11-16 夜の更新。詳細は progress.md の末尾）
- **Gateway と Core の分離**（`botd --core-mode separate`、Linux、opt-in）と **Gateway の無停止更新**（`botctl gateway-update <bot>`）が入った。Core は botd の子で Gateway と独立に動き、Gateway の更新・クラッシュで Core の PID と `!count` の状態は変わらない。実機（WSL2、実 Discord）で、起動・各種クラッシュ復旧・更新 2 回・更新中の連投・重複排除・OS 再起動後の自動起動・待機 Gateway の失敗を確認済み。
- **Oracle サーバー（aarch64）で放置試験中**（2026-11-15 22:10 JST 開始、separate、テスト用 Bot）。`/home/ubuntu/soak.log` に記録（`soak-report`。root の `~` は `/root` なので絶対パスで指定する）、`deploy/dev/soak-diff.sh` で最初と最後を比較。次は `docs/soak-test.md` の §5（新ビルドへの入れ替え）。
- **未検証・未実装**：長時間稼働の結果、複数 Shard の長時間・繰り返し・3 Shard 以上の実機（2 Shard は WSL の実 Discord で 1 回確認済み）、Interaction（スラッシュ）の重複排除（ACK の規則による想定）、Core のホットスワップ（separate では未対応）、孤児 Core の再採用、`gateway-update` の検証付き監査（現状は追記ファイル）。Sol レビューの限界は `known-limitations.md`。
- **ツール**：Codex は ChatGPT アカウントで `gpt-6.1-terra`／`gpt-6.1-sol`（`Terra` 等の別名は不可）。Haiku は docs・補助スクリプト、Codex は試験の執筆とレビュー、Claude が設計・統合・検証。CI の確認は ci-verify ブランチに push して Actions で行う。

## 1. 現状の要約
- **対応OSと成否の基準（2026-11-13決定）**：対応OSは **Linux（Ubuntu）のみ**、**Windows はサポート外**（開発機での動作は保証しない。Windows固有コードは残るが保証・CI対象外）。**試験の成否の基準は Ubuntu の GitHub Actions**（`ci.yml`、`stress.yml`、`bench.yml`）とする。
- **CI / ベンチマーク状況**：GitHub Actions（Ubuntu のみ）の並列ジョブ（fmt、clippy、test の 5 シャード＝reload/cluster/daemon/process/rest、examples+doctor、集約の ci-ok）は、2026-11-13 に全ジョブ成功を確認（最長経路は reload シャードの約 280 秒）。stable 互換は週次/手動の別ワークフロー（stable.yml）。Linux 上での IPC / failover ベンチマークも測定済み（AMD EPYC 7763 4vCPU、IPC 往復 p99 は並列度 1/4/16 で 115.5/258.1/973.6 µs、failover 検知 Suspected p50 1503.6ms / FailoverPending p50 3003.1ms。本番性能の保証ではない）。
- **main 統合済み（第13弾まで）**：
  - 属性マクロ・Cog・型付き引数・コンポーネント経路（framework）
  - Worker IPC（handshake、event_seq、drain、ホットスワップ T04〜T06/T19）
  - Supervisor、StateStore（メモリ・ファイル永続）、metrics（フィード API）
  - RBAC・監査、REST/Identify 予算、Gateway 並行切替
  - cluster シミュレーション、mTLS Transport、証明書管理（失効・ローテーション・CA管理・方針永続化）
  - Raft(mTLS) transport / runtime（openraft、leader 転送、fencing 付き引継ぎ、自動 failover/failback）
  - 永続 Subscription / ジョブ、daemon 側 update/rollback（認可・監査・同一操作ID）
  - 子プロセス自動復旧（child_recovery）、ログ経路の秘密保護
  - botd/botctl、複数 botd 分散 E2E（模擬 Discord、試験用過半数 ACK / Raft）、testkit、examples
  - 実 Discord 検証準備ツール（`discord_smoke`、`docs/real-discord-verification.md`）
- **未検証・未完了**：実 Discord での動作（ユーザー実施予定）、複数ホストの実障害、OS suspend、本番での Raft 有効化（起動拒否ガード継続）、メトリクスの daemon 接続、永続 Subscription/ジョブの Worker 接続、Core 単独更新・更新失敗時の自動 rollback、無停止更新の負荷試験分割（`codex/zdtfix`）、Raft runtime の botd 配線（`codex/raftwire`）、docs 最終整備（`codex/finaldocs`）。完成扱いしない。
- cargo プロジェクトは **`src/`**（workspace: `framework`, `framework-macros`）。コマンドは `src/` で実行する。

## 2. 方針（AGENTS.md の要点）
- `docs/architecture.md` が仕様の基準。既存コードを調査してから変更する。
- Gateway/Core の Shard 単位分離、複数 Bot、認証、quorum と lease による安全性を維持。**独自コンセンサスは実装しない**。
- 秘密情報を記録しない。Core に Bot token を渡さない。未認証の遠隔管理を許可しない。
- 機能ごとにテストし、**未検証の保証を完成と主張しない**。**対応OSは Linux（Ubuntu）のみとし、Windows はサポート外**（開発機での動作は保証しない）。障害条件を考慮。
- 進捗・判断・未解決事項・検証結果は `docs/progress.md` に記録。**本番デプロイ・破壊的操作・秘密情報の変更は事前確認**。

## 3. 進め方（PM + Codex / Antigravity CLI 並列）
ユーザー指示: 「Claude が PM。コードは基本的に Claude が直接書かず、CLI ツールを使って並列に進める。承認を求めず自律的に」。本番デプロイ・破壊的操作・秘密情報の変更のみ事前確認する。

### ツール併用・モデル方針
- **Codex CLI の位置づけ（2026-11-14 更新）**: ユーザーの ChatGPT プランが Pro から Plus に下がり、5 時間の利用制限もあるため、Codex は主戦力にしない。軽い作業の補助に限る（使う場合は Terra=medium、設計精査のみ Astra）。
- **実装の主体**: Claude 本体＋Claude のサブエージェント（モデル指定なし。Haiku は Claude Code の再起動後に使える）＋ agy。大きな作業（Gateway と Core の分離など）も Claude が段階に分けて進める。
- **Antigravity CLI（agy）の併用**: Codex が使えない間および併用時は Antigravity CLI を使用する。
  - コマンド例: `agy -p "<prompt>" --model gemini-3.8-flash-high --mode accept-edits`
  - 利用可能モデル: `gemini-3.8-flash-high`（主）, `gemini-3.8-flash-medium`, `gemini-3.8-flash-low`, `claude-opus-4-6-thinking`, `claude-sonnet-4-6`。
- **検証方針（重要）**: **重い全体試験や反復検証はローカルで回さず GitHub Actions（ci.yml、stress.yml、bench.yml）で行う**。手元のローカルマシンで重い全体検証を回さない。

### 作業・worktree 運用ルール
1. 作業を**ファイル所有範囲が重ならない**単位に分割（共有してよいのは `src/src/lib.rs` の `pub mod` 行のみ）。
2. 作業ごとに git worktree + ブランチ `codex/<name>` を作る。パスは必ず **`C:/Users/<user>/cw/<name>`** のように短い ASCII パスを使用する。
   ```bash
   git worktree add -b codex/<name> C:/Users/<user>/cw/<name> main
   ```
3. 指示文は**引用符付き heredoc**（`<<'EOF'`）で書く（バッククォートがシェルに展開され指示が壊れる事故防止）。共通ルール（AGENTS.md/architecture.md/progress.md を読む、担当範囲外を触らない、progress.md は編集しない、`--offline` で cargo、commit しない、独立部分はサブエージェントで並列可、最後に日本語レポート）を毎回付ける。
4. 起動（バックグラウンド、worktree 単位）:
   - Codex:
     ```bash
     CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0 \
     codex exec -C <worktree> -m <model> -s workspace-write \
       -c model_reasoning_effort=<level> -o <out.txt> - < prompt.txt > log.txt 2>&1 &
     ```
   - Antigravity:
     ```bash
     agy -p "<prompt>" --model gemini-3.8-flash-high --mode accept-edits
     ```
5. サンドボックスにはネットワークと `.git` 書込みがないため、cargo の完全検証とコミットは PM が行う。依存追加が必要なら PM が事前に `cargo add` + `cargo fetch` する。
6. **統合とクリーンアップ**:
   - 作業完了後、**各 worktree の `src/target` を必ず削除**する（ディスク圧迫防止）。
   - `cargo fmt --all` → コミット（一時 ID）→ main へ `--no-ff` マージ → `src/` で fmt/clippy（警告禁止）確認。
   - **重い全体試験はローカルで回さず GitHub Actions（`gh workflow run ci.yml` 等）へプッシュして Actions で検証**する。
   - `docs/progress.md` を更新し、作業完了した worktree を削除する。
7. `rm -rf` に変数展開を使う場合は `"${VAR:?}"` 形式にする（安全チェックで拒否される）。

### 運用上の落とし穴
- **ディスク満杯**: worktree ごとの `target` が各 3〜5GB になる。並列作業時は統合後に都度 **各 worktree の `src/target` を削除**。`CARGO_PROFILE_DEV_DEBUG=0 CARGO_INCREMENTAL=0` で縮小。
- **統合の副作用**: 秘密マスクがエラー診断を潰す等、各ブランチ単体で緑でも merge 後に出る問題がある。必ず統合後に Actions で全体試験を通す。
- **過去の運用メモ（Windows 由来・参考情報、現在は Windows サポート外）**:
  - *パス長制限*: Windows のパス長制限で長い scratchpad 配下は削除に失敗した（`C:/Users/<user>/cw/<name>` の短いパスで回避していた）。
  - *ファイル権限・ACL の CI 環境差*: `icacls` / SDDL や Unix mode との挙動差で Windows 側の CI がタイミング/環境依存した。
  - *共有 CARGO_TARGET_DIR*: 異なる worktree で共有すると同名 crate の古い成果物を再利用して誤動作することがあった。
  - *テストフィクスチャ*: 過去に rlib 選択でフレークしたが、Cargo の依存解決/JSON 成果物取得へ移行して修正済み。

## 4. 残課題

### 完了済み
1. **Raft mTLS**: openraft による epoch CAS、永続ストレージ、Raft(mTLS) transport、leader 転送、fencing 付き引継ぎ、自動 failover/failback runtime（`raft_quorum*`, `cluster_runtime.rs`）。
2. **証明書管理**: ノード ID/シリアル失効、重複期間付きローテーション、CA 管理、ポリシー永続化・原子的交換（`tls_transport.rs`, `cert_policy.rs`）。
3. **永続 Subscription / ジョブ**: `subscription.rs`、`durable_jobs.rs` の実装と状態保持。
4. **update / rollback**: daemon 側 update/rollback の認可、監査、同一操作 ID 処理、計画生成（`update_rollback.rs`）。
5. **子プロセス自動復旧**: daemon / worker における子プロセス自動復旧（`child_recovery.rs`）。
6. **Linux CI / ベンチ**: GitHub Actions の並列ジョブ化（Linux ジョブ全成功）、Ubuntu ランナー上での IPC 往復・failover 検知ベンチマーク測定。
7. **ログ秘密保護**: ログ経路・エラー診断における `Secret<T>` による秘密マスクと診断情報保持。
8. **実 Discord 検証ツール**: 実環境確認用ツール `discord_smoke` および検証手順書 `docs/real-discord-verification.md` の整備。

### 未完了・保留事項（WIP ブランチは origin に push 済み）
1. **無停止更新の試験分割（WIP: `codex/zdtfix`）**:
   - `tests/update_load.rs` の負荷試験において、連続更新・Gateway 切替・旧 Worker クラッシュは欠落0・ACK 47ms 以内を達成済みだが、「ハンドラー実行後・結果返却前のクラッシュ」は仕様上の限界（結果不明）となる。
   - `tests/update_load.rs` を、(1) 厳格な通常テスト、(2) 結果不明の `#[ignore]` テスト、に分割して CI が通るようにする（未統合のままだと CI が失敗するため）。origin に push 済み、未検証。
2. **Raft botd 配線（WIP: `codex/raftwire`）**:
   - Raft runtime の botd への配線（既定無効・模擬 Discord のみ）と 3 プロセス E2E。ほぼ実装済みで未検証。origin に push 済み。
3. **docs 最終整備（WIP: `codex/finaldocs`）**:
   - `benchmarks.md`, `conformance.md`, `developer-guide.md`, `known-limitations.md`, `operator-guide.md`, `security.md` の事実照合と最終更新。origin に push 済み。
4. **メトリクスの daemon 接続**:
   - フィード API は実装済みだが、daemon 本体への接続が未完了。
5. **永続 Subscription / ジョブの Worker 接続**:
   - モジュールは実装・単体検証済みだが、Worker 実経路への接続が未完了。
6. **Core 単独更新・更新失敗時の自動 rollback**:
   - 切替後の自動 rollback、子プロセスの再採用などの自動化。
7. **実 Discord での検証（ユーザーが実施）**:
   - Gateway 接続、Slash 登録、Interaction 3 秒応答、Identify / Resume 窓の実機確認。手順は `docs/real-discord-verification.md` に記載。ユーザー自身が実施する。

## 5. 環境構築・検証コマンド
対応OSは Linux（Ubuntu）のみです（Windows はサポート外）。ローカルでの重い検証は行わず、GitHub Actions を活用します。

```bash
cd src
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo run --bin doctor -- examples/placement.toml --resolved-shards bot-two=1
```

- 重い全体試験や反復検証は GitHub Actions で実行:
  ```bash
  # GitHub CLI による Actions の確認・実行
  gh workflow run ci.yml
  gh run list
  gh run watch
  ```
- `experiments/quorum-spike` は独立した Cargo プロジェクト（メイン workspace 外、`cd experiments/quorum-spike && cargo test`）。

## 6. 文書の所在
| 文書 | 内容 |
|---|---|
| [architecture.md](architecture.md) | 仕様（基準） |
| [progress.md](progress.md) | 日付付きの進捗・判断・検証結果 |
| [conformance.md](conformance.md) | §16.2 完了条件と T01〜T20 の適合状況 |
| [known-limitations.md](known-limitations.md) | 未実装・保証できない点 |
| [ipc-protocol.md](ipc-protocol.md) / [security.md](security.md) / [benchmarks.md](benchmarks.md) | IPC、セキュリティ、実測（Linux ベンチおよび Windows 過去参考記録） |
| [quorum-evaluation.md](quorum-evaluation.md) / [shard-runtime-design.md](shard-runtime-design.md) / [local-response-management.md](local-response-management.md) | 設計・評価 |
| [developer-guide.md](developer-guide.md) / [operator-guide.md](operator-guide.md) | 利用ガイド |
| [real-discord-verification.md](real-discord-verification.md) | 実 Discord 検証手順書（ユーザー実施用） |

### 検証環境（2026-11-14 追加）
- **WSL2 の Ubuntu 24.04（systemd 動作）**: Rust 1.90.0 導入済み。ソースは `git archive` で WSL 内（`~/discord-rs`）に展開してビルドする（`/mnt/d` 上のビルドは遅い）。ビルドは `-j 6`〜`8` 程度で短時間に。重い全体試験の繰り返しは GitHub Actions に任せる。
- 実 systemd の確認: `deploy/dev/wsl-systemd-smoke.sh`（模擬 Discord、終了時に後片付け）。実 Discord を使う本番相当の試験は `docs/real-operations-verification.md`。token は `deploy/dev/set-token.sh` で `/etc/discord-rs/botd.env`（root:root 0600）に保存し、チャットやコマンドに書かない。
- 実 Discord の結果（2026-11-14）: 常駐・`/ping`・`!ping`・`!count`・ワーカーの `kill -9` からの復旧は成功。Gateway の `kill -9` は、実 Discord では自動復旧しない（Identify の許可が未接続）→ 修正中。Gateway/botd の再起動で `!count` が 0 に戻る（Core が Gateway の子で、状態が Gateway のメモリ上にあるため。設計の Gateway/Core 分離が通常経路では未実装）→ 設計書を作成中（`docs/gateway-core-separation-design.md`）。

