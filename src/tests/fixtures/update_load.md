# 無停止更新の負荷試験と受付境界

仕様の基準は `docs/architecture.md` §6–8。実 Discord へは接続せず、
実 botd / Gateway / Worker と loopback の模擬 HTTP を使う。
`docs/progress.md` と `docs/handoff.md` は本作業では編集しない。

## 判定を変更しない項目

送信した全入力を母集団に、正常応答欠落・重複・逆転、ACK 未着・重複、
3000 ms を超えた ACK、孤児、観測した二重 Active をそれぞれ失敗とする。
ACK 時間には入力キューでの待機も含む。エラー返信を正常応答に数えず、
受付拒否も送信母集団から除外しない。

## 本体の修正

- Worker の候補 Restore 中は配送先の write lock を保持しない。
  状態引継ぎの barrier と更新の排他は維持し、旧世代の応答予約と独立処理を継続する。
- `begin_response_delivery` で予約後、IPC への送信前に更新された要求は、
  新世代へ予約し直して配送する。既存の `begin_response` の stale 拒否契約は維持する。
- 改行まで送信できていない IPC 要求だけを未送信として分類する。
  確認済みの復旧世代がある場合に限って再配送し、全フレーム送信後の結果喪失は再実行しない。
- Gateway 切替の模擬入力に任意の意味的 `event_id` を付ける。
  これは prefix のメッセージ ID / Slash の Interaction ID 相当であり、
  Worker の分配キーや他 Bot/Shard の選択・認可には使わない。
  同じ Shard の入力を世代間で重複排除し、切替中は有界キューへ保留する。
- Gateway が実際に応答枠を予約した後の内部 `accepted` を待って Pause する。
  TCP に転送したことを受付完了とみなさない。旧応答の drain・終了後に保留分を配送する。
- 作業中に追加された Gateway 側の変更（未受付 Shadow 入力の有界保持、
  stop 時の最終受付 ACK 後の残件確認）は保全し、今回の実プロセス試験に含めた。
- Worker プールでも予約済み応答を admission permit として扱う。停止開始後の
  再受付拒否や、自分の予約を待つ drain writer の後ろへの read lock 再取得を避ける。
- `continuous_events_during_updates_and_core_crashes` の `#[ignore]` を除去する。
  新 Worker の結果不明ケースも欠落 0 の判定対象に残し、未達は通常の試験で失敗する。
  未処理の Shadow コピーも保持し、Active が同じ入力を受けたら除外する。
  保留と Shadow の合計は 1024 件 / 2 MiB、最近の ID は 4096 件まで。
  未受付の転送は1件までとし、部分書込中も受付 ACK・結果を読み続ける。

## 受付拒否と未保証事項

意味的 ID がある今回の模擬経路では、Pause 直前の未受付入力を保留・配送して扱う。
「受付拒否は欠落ではない」という受入基準への変更はしていない。
無キー入力、有限保持容量を超える入力、長い drain での ACK 期限、
独立した Discord セッション間の任意の配送差に対する全順序は保証しない。
受付拒否を正常完了と扱うには PM の基準判断が必要である。

`crash_new_core` は、新 Worker がハンドラーを実行して Result を生成した後、
relay が親へ Result を返す前に終了する。この時点でアプリの副作用の有無は確定できない。
既存の T06 と同様に、結果不明のハンドラーを自動再実行しない。
この条件でも正常応答欠落 0 を実現するには、再実行安全性の明示的な契約、
または障害を越えて照合できる結果・副作用の管理が別途必要になる。
エラー返信への置換や、この入力を試験母集団から外す変更は行わない。

## 再現コマンド

`src/` で実行する。新規依存は不要。Python 3 は既存 fixture の実行に必要。

```powershell
cargo test --offline -p src --test update_load -- --test-threads=1 --nocapture
cargo clippy --offline -p src -j 2 -- -D warnings
```

証拠は Cargo target 配下の `update-load-*` にある `result.json` / `events.json`。
秘密値や raw IPC 本文は保存せず、許可した ID・時刻・結果分類・実行成果物ハッシュを保持する。
二重 Active の確認は配送先と ACK 済み遷移の観測であり、任意瞬間の包括的な証明ではない。

## 検証結果

2026-11-12、Windows / Rust 1.90.0 / Python 3.11.9、40 Hz。
上記の対象試験を一度だけ実行し、2 passed / 1 failed / 0 ignored。
試験実行は 35.03 秒（コンパイル時間を除く）。ローカル反復は行っていない。
`cargo clippy --offline -p src -j 2 -- -D warnings` は成功。
`git diff --check` も成功。テスト実行中と実行後で対象ソースの SHA-256 が一致した。

| ケース | 送信数 | 正常応答欠落 | ACK 最大 ms | 結果 |
|---|---:|---:|---:|---|
| core_series（3 回更新） | 288 | 0 | 16 | 成功 |
| gateway_switch（2 回切替） | 177 | 0 | 47 | 成功 |
| crash_old_core | 100 | 0 | 16 | 成功 |
| crash_new_core | 100 | 1 | 1344 | 未達 |

全ケースで重複・逆転・ACK 未着・ACK 重複・3000 ms 超過・孤児・観測した二重 Active は 0。
Gateway の 2 回目を含む旧 Gateway の drain / 終了を確認した。
`crash_new_core` は `missing_effects=1, failed_effects=1, failed_results=1`。
エラー応答は正常応答に数えていない。結果不明のハンドラーを自動再実行しない仕様上、
この欠落は未解決であり、全許容基準達成や無停止更新の完成とは報告しない。

`reserved_delivery_survives_shutdown_admission_barrier` と
`unused_delivery_survives_restore_wait_and_routing_commit` はともに成功。

証拠: `src/target/debug/update-load-9092135366187372998/` 内の各ケースの
`result.json` / `events.json`。このディレクトリは Git 管理対象ではない。

GitHub Actions の Windows / Ubuntu で反復すべき試験:

- `continuous_events_during_updates_and_core_crashes`（内部の上記 4 ケースすべて）
- `unused_delivery_survives_restore_wait_and_routing_commit`
- `reserved_delivery_survives_shutdown_admission_barrier`
- `daemon::test_gateway_switch::update_tests::handoff_` で始まる受付境界・順序・容量試験
- `test_update_load_metrics.py` の計測判定試験

最後の 2 項目は今回ローカルでは実行していない。Actions 自体も今回未実行。
全体試験、Linux、実 Discord、本番更新、複数ホストは今回の検証対象外。
