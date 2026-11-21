# 05. Identify と Resume

## 用語

- **Identify**：Discord に接続するときの「ログイン」。Bot の token を渡して、新しい接続（セッション）を始める。Discord は回数を制限している。
- **Resume**：前の接続の続き。セッションの番号を伝え、切れていた間のイベントを受け取る。回数の制限は（一般には）ない。

## いまの状態（確認済み）

- `1006` は Close フレームを受信せずに接続が終わったことを表す WebSocket の予約値で、Close フレームには載らない。Discord の `4000` は原因不明の切断で再接続可、`4007`・`4009` では新規セッションが必要、`4004` では再接続不可。Gateway の op 7 Reconnect は再接続と Resume を求めるイベントで、Close フレームとは別に届く。`1006` や `4000` だけでは op 7 の受信や Resume の成否は分からない。[RFC 6455 §7.1.5・§7.4.1](https://www.rfc-editor.org/rfc/rfc6455.html#section-7.1.5)、[Discord Gateway Close Event Codes](https://docs.discord.com/developers/topics/opcodes-and-status-codes#gateway-close-event-codes)、[Discord Gateway Resuming](https://docs.discord.com/developers/events/gateway#resuming)。
- Oracle の 2026-11-16 夜の 1 件では、`Gateway 再接続の要求を受信` → `close code 4000` → `Gateway 再開（Resume 成功）` の順に記録された。記録上の時刻差は 1 秒（秒単位のログ）。出典は `docs/progress.md`。
- **プロセスを再起動したときの Resume は、できない。** セッション情報がメモリだけにあり、再起動のたびに新しい Identify を使う。
- **Identify を使うのは、Gateway が新しく起動するとき**：プロセスの再起動、`gateway-update`（シャードごとに 1 回）、クラッシュからの復旧。
- **自分たちで課している制限：同じ Bot で、直近の起動から 30 秒以上空ける、1 時間に 10 回まで。** 超えると `IdentifyUnavailable` で `Degraded`（出典：`docs/known-limitations.md`）。初回起動と手動の起動は、常に許可して記録する。botd の再起動で履歴は消える。

## どれくらい頻繁に起きるか

- 通常の運用では、ほとんど起きない。Oracle の最初の約 18 時間は、Identify は起動時の 1 回だけだった。
- 頻繁に更新する、またはクラッシュを繰り返すときに、制限に当たり得る。

## Discord 側の制限（**私の記憶。公式で未確認**）

「1 日 1000 回程度」「シャードごとに数秒に 1 回程度」。実際の値は、公式ドキュメント（Gateway の Session Start Limit）で確認する（[unverified-items.md](unverified-items.md)）。

## 再起動をまたぐ Resume を作る場合の論点

- セッション情報（セッション ID と最後の番号）を、安全に保存する（秘匿の扱い）。
- 古くなったセッションの扱い（Discord が無効と返したら、Identify に切り替える）。
- `gateway-update` では、旧 Gateway と新 Gateway が同じセッションを同時には使えないので、更新の Identify を節約する効果はない。節約できるのは、**プロセスの再起動**のときだけ。

## 評価・推奨

- **この規模（数シャード）では、影響が小さい。後回しを推奨。** 大きな Bot で再起動が頻繁になったときに、再検討する。

## 決めること

[decisions-needed.md](decisions-needed.md) の D6。
