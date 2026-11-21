# 設計判断と未解決事項（design-decisions）

2026-11-16 の作業の最後に、「いまの設計の限界」「私（Claude）が不明・未確認と言ったもの」「判断が必要なもの」を、後から読み返せるように分けてまとめたフォルダです。**ここに書いた案は、すべて未実装・未検証です**（実装済みのものは、そう明記しています）。

## まず読むもの

| ファイル | 内容 |
|---|---|
| [decisions-needed.md](decisions-needed.md) | **ユーザーの判断が必要なもの**の一覧と、私の推奨 |
| [unverified-items.md](unverified-items.md) | 「未確認」「不明」のもの、確認の方法、誰が行うか |
| [discussion-records/](discussion-records/) | **ChatGPT との議論で合意した方針（DDR-0001〜0005）**。実装済みの意味ではない | 最新：[DDR-0006](discussion-records/0006-proposals-accepted-2026-11-17.md)（提案の了承）、[DDR-0007](discussion-records/0007-p1-tmpfs-and-cutover-order.md)（**P1 は tmpfs 方式、Core 切り替えの順序と失敗時の中止、作業の優先順位**） |
| [reconciliation-with-ddr.md](reconciliation-with-ddr.md) | DDR と、この `design-decisions/` の資料・現在のコードとの**食い違いの一覧**と、次に決めること |
| [proposals-2026-11-17.md](proposals-2026-11-17.md) | **DDR で残った 3 つの判断への Claude の提案**（基盤の状態の引き継ぎ、止まったハンドラの段階的な対処、03 の書き換え骨子）。**2026-11-17 にユーザーが了承**（[DDR-0006](discussion-records/0006-proposals-accepted-2026-11-17.md)）。未実装 |
| [p1-stalled-diagnostics-design.md](p1-stalled-diagnostics-design.md) | **P1（停滞の診断を daemon から照会できる）の設計案**。Core が状態を小さなファイルに書き、botd が読む方式（botd が Core の IPC に接続すると Gateway を締め出す危険があるため）。方向は了承済み、具体的な方式はユーザーの確認前。未実装 |
| [p3-manual-core-recovery-design.md](p3-manual-core-recovery-design.md) | **P3（認証・監査つきの Core 手動復旧）の設計案**。incarnation 固定・排他・回数制限・退役世代の上限・監査。**未承認・未実装**。決めてほしい項目あり（9 章） |
| [review-2026-11-21-sol-discussion.md](review-2026-11-21-sol-discussion.md) | **Sol(6.2)との設計議論の記録**（nonce、Identify の送信時の調整、補助プロセス、完成の定義）。助言であり、ユーザーの決定ではない |
| [review-2026-11-17-astra-sol.md](review-2026-11-17-astra-sol.md) | **Astra と Sol の設計レビュー（ウェブ検索つき、独立）**。2 人が一致した指摘：P1 は tmpfs と単調時計に、差し替えは受け付けの壁と最終の重複排除の引き継ぎに、nonce は操作ごとに固定して本文全体の JSON で、Identify は Bot ごとに調整。私の I/O の実測も記載 |

## テーマ別の設計・対応案

| ファイル | テーマ | 状態 |
|---|---|---|
| [01-stuck-handler.md](01-stuck-handler.md) | 止まったハンドラ（B3）。起きるか、強制停止できるか | 観測は実装済み、打ち切りは未決定 |
| [02-copy-on-commit-state.md](02-copy-on-commit-state.md) | 状態を「借りる」のをやめ、コピーで作業して最後にコミットする案（ユーザー発案の「管理元を移す原子的な仕組み」の具体化） | 設計案のみ |
| [03-core-hot-swap.md](03-core-hot-swap.md) | Core の無停止差し替え（新 Core が新しい通信を受け、旧 Core は終わり次第停止） | 設計案のみ（組み込みモードに実装あり） |
| [04-multi-host-preparation.md](04-multi-host-preparation.md) | 複数ホスト。シングルを完成させてから。シングル開発中に守るルール | 設計方針のみ |
| [05-identify-and-resume.md](05-identify-and-resume.md) | Identify と Resume。再起動をまたぐ Resume が要るか | 後回しを推奨 |
| [06-botd-supervision.md](06-botd-supervision.md) | botd が死んだときの動きと、子プロセスの引き継ぎ（再採用） | 後回しを推奨 |
| [07-state-persistence.md](07-state-persistence.md) | 状態の保存（最大約 2.5 秒の窓）。何を保存しているか | 現状の説明と選択肢 |
| [08-unknown-outcome-operations.md](08-unknown-outcome-operations.md) | 結果不明の外部操作（HTTP の結果が分からないとき）。discord.py との比較、nonce による安全な再試行 | 調査が必要 |

## 読み方の約束

- **確認済み**：コードを読む、または実機・テストで確認したもの。出典（`docs/progress.md` など）を付けています。
- **推測・記憶**：私の一般知識や記憶によるもの（Discord の仕様、discord.py の挙動など）。**公式の仕様や実装で未確認**と明記しています。
- 優先度や推奨は、「1 台のサーバーで運用する」前提での私の評価です。

## 関連する既存の文書

- [../known-limitations.md](../known-limitations.md)：既知の制限の全体
- [../progress.md](../progress.md)：進捗と検証の記録（2026-11-16 の節が最新）
- [../next-steps.md](../next-steps.md)：今後の作業案
- [../operator-guide.md](../operator-guide.md)：運用者向けの手順
- [../architecture.md](../architecture.md)：仕様の基準
