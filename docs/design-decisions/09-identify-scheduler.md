# 09. Identify スケジューラー

- 対象: [DDR-0007](discussion-records/0007-p1-tmpfs-and-cutover-order.md) の優先順位 4
- 仕様の基準: [architecture.md](../architecture.md) §5・§7、調査の起点: [Astra / Sol レビュー](review-2026-11-17-astra-sol.md#t3-identify-と-resume05-identify-and-resumemd)

## Discord 公式仕様（2026-11-21 確認）

- `max_concurrency` は 5 秒間に許される Identify 数。シャードのキーは `shard_id % max_concurrency` で、同じキーは 5 秒に 1 回、異なるキーは並行に開始できる。Discord はシャードをバケット順に開始する例も示している。 [Max Concurrency](https://docs.discord.com/developers/events/gateway#max-concurrency)
- 通常は Bot の全シャード合計で 24 時間に 1,000 回。Resume は対象外。上限に達すると全セッションが切断され、Bot の認証情報がリセットされる。大規模 Bot には別の上限がある。 [Identifying](https://docs.discord.com/developers/events/gateway#identifying)・[Sharding for Large Bots](https://docs.discord.com/developers/events/gateway#sharding-for-large-bots)
- `Get Gateway Bot` は `session_start_limit` の `total`、`remaining`、`reset_after`（ミリ秒）、`max_concurrency` を返す。実装で Discord 側の残量を扱う際は応答を基にする。 [Get Gateway Bot / Session Start Limit](https://docs.discord.com/developers/events/gateway#session-start-limit-object)

## 変更前の経路

行番号は調査した `5b60670` 時点。ここでの予算は単一 botd のメモリ上にあり、Bot ID ごとに許可間隔 30 秒、直近 1 時間に最大 10 回を設定している（`src/src/local_identify.rs:16–27,125–156`）。

| 起動の契機 | 呼び出しと許可の扱い |
| --- | --- |
| botd の自動起動 | `botd.rs:807–811` が制限を有効にして `start_all` を呼ぶ。`daemon.rs:8699–8713` は各 Shard を明示的な `start` として扱い、`daemon.rs:7822–7832` で制限を判定せず記録する。 |
| 管理操作の start / restart | `daemon.rs:3787–3809` で `start`、restart は停止を待って同じ開始段階へ進む（`daemon.rs:3828–3848`）。`daemon.rs:8735–8767` は起動対象を `manual` として渡し、更新の保護中を除き制限を判定せず記録する。 |
| gateway-update の待機 Gateway | `daemon.rs:3697–3703` → `begin_gateway_update`。履歴の複製で待機用と復旧用の 2 枠を見積もり（`daemon.rs:8192–8213,8258–8264`）、別の起動処理で spawn 後に記録して復旧の機会を保護する（`daemon.rs:8334–8342`）。 |
| 有効化後に失敗した更新の復旧 | `daemon.rs:3739–3748,8470–8493`。候補を取り消し、旧 Gateway を終了、または停止済みの回復スロットを再開する。次の回復 poll が同じ Bot の復旧枠を保護し、対象 Shard は通常の間隔・残量を判定して再起動する（`daemon.rs:7772–7846,8971–9004`）。 |
| 自動復旧 | `daemon.rs:9004,9122–9124` → `child_recovery.rs:766–792` → `ProcessRecoveryHost::try_acquire_identify`（`daemon.rs:7763–7846`）。制限を有効にした botd は許可と同時に記録する。通常起動・復旧の spawn は `daemon.rs:7849–7868,8770–8918`。 |

表中でファイル名だけを示した Rust ソースは `src/src/` 配下（`botd.rs` は `src/src/bin/`）。明示的な模擬 Discord の別ランタイムは `src/src/daemon/test_gateway_switch.rs:849–872,1112,1691,1792` の模擬予算を持つ。実 Gateway の各プロセスは `Get Gateway Bot` の応答から個別の Twilight キューを作る（`src/src/main.rs:135–155,1201–1222`）。

起動処理は同じ botd に直列化されていても、初回・手動が制限を通らないため、復旧と更新を合わせた試行数・間隔を守れない。待機起動は事前判定と記録が別経路。さらに、接続後にセッションが無効になった Gateway は独自に再 Identify でき、botd は初回を含む実際の送信時刻・回数を観測しない。READY の件数から失敗した試行は分からない。

## 選択肢と推奨

**A. botd 内の起動許可を一元化する。** 単一 botd が Bot ID ごとの履歴を持つ。初回・手動・待機・更新失敗時の復旧・自動復旧が、同一の判定と記録を通る。更新の 2 枠条件も同じ履歴で判定し、消費は今回の起動分のみ。起動許可は実送信より前なので、起動遅延で送信間隔が縮む場合や、起動済み Gateway の再 Identify には対応できない。

**B. A に加えて Gateway が実送信前に許可を得る。** DDR-0007 の送信調整にはこちらが必要。既存の匿名 stdin パイプは botd → Gateway の `Shutdown` / `Activate` 専用（`src/src/shard_runtime.rs:14–24`、`src/src/main.rs:396–435`）。Gateway → botd の Identify 要求と応答の経路は存在しない。Gateway ↔ Core の認証付き IPC は別の相手であり、Core を認証情報や許可の中継に使わない。

B の追加には、起動時に継承する双方向のローカル IPC、またはプロセス認証付きソケットが必要。要求を Bot・Shard・世代に結び付け、1 回限りの短命の許可、切断時の取り消し、サイズと待ち行列の上限、旧版 Gateway との互換性を設計する。Twilight の Identify キューを通る初回・再 Identify を捕捉できる範囲と、許可から実送信までの遅延を試験で確認する。送信許可待ちが Heartbeat や Resume を詰まらせないようにする。起動時の予約を初回送信の許可へ引き継ぎ、同一試行の二重消費を避ける。複数ホストへ広げる場合は既存の認証・quorum・lease に従う調整が別途必要。

**推奨は段階的に A → B。** 今回は A の起動許可の一本化を実装対象とする。B の IPC、送信許可の有効期限、botd が停止したときの既存 Gateway の扱いは下記の判断を待つ。

## 起動許可と優先順位

- 実装案では既存のローカル値（30 秒、1 時間 10 回）を全実 Gateway の起動に適用する。これらは Discord の値とは別の運用上の上限。botd の単一所有者が許可の判定から記録まで同期的に行う。起動失敗や READY 未達でも使用済みの許可は戻さない方針とする。模擬 Discord の予算は試験用の既存経路を維持する。
- 更新は現行の復旧機会の保護を維持する。待機起動には待機用と復旧用の枠が必要で、待機が起動したあとは同じ Bot の別 Shard が次の枠を使うのを抑止する。有効化後に失敗した場合は対象 Shard の復旧が保護された機会を使う。間隔・回数上限は復旧にも適用する。
- 通常の監視 tick は自動復旧を処理してから管理操作を進める。使用済みの枠の横取りや一般の優先キューは追加しない。B の優先順位案は、停止中の稼働 Shard の再 Identify・復旧（更新後の復旧を含む）、手動 start / restart、稼働中 Shard の更新用待機の順。初回自動起動をどの順位に置くか、同順位の公平性や更新の長期待ちをどう扱うかは未決定。

| 要求 | 間隔待ち・復旧枠の保護中 | 回数上限 |
| --- | --- | --- |
| 初回自動起動・自動復旧・更新失敗からの復旧 | `Deferred: IdentifyBudget` を監査に出し、監視 tick で再試行する。待機中は子を起動せず、再起動回数を消費しない。 | `Degraded: IdentifyUnavailable`。初回の同期的な拒否は起動結果にも反映する。 |
| 手動 start / restart | 今回の既定案は明示的に `IdentifyDeferred` を返す。再試行は新たな操作とする。restart の起動段階で拒否された Shard は停止したままになる。 | `IdentifyUnavailable` で拒否する。 |
| gateway-update の待機起動 | 既存の更新期限まで `waiting_reason: IdentifyBudget` を表示して待つ。期限切れは `IdentifyDeferred`。 | `IdentifyUnavailable` で失敗し、旧 Gateway を維持する。 |

同じ Bot の更新後の復旧機会が保護されている間、管理操作による追加の再起動禁止が対象 Shard の復旧を妨げないよう、start / restart / gateway-update は受付時点で明示的に拒否する。

## 障害と隔離

- Bot ID が異なる履歴は別管理。待機用の保護も同じ Bot にだけ及ぶ。Shard ごとの所有権・排他と管理操作の認証は既存の経路に従う。
- botd が再起動すると履歴と保護は失われる。生存プロセスの二重起動を防ぐ既存の予約と、過去の Identify 予算を復元する仕組みは別。再起動を繰り返した場合の間隔・残量の保証は未実装。
- 経過時間には botd の単調時計を使うため壁時計の修正で履歴は失効しない。注入した時計が逆行した場合は既存の `ClockRegressed` で新規起動を停止する。単調時計が進んだ分は経過として扱う。B では Discord の `reset_after` を受信時の単調時計に対応付け、情報が古いときや更新できないときに新規許可を出す条件を決める。
- B の障害時の案は、許可応答を失った試行を消費済みとし、IPC 復帰までは新しい Identify を保留して理由を表示する。既存 WebSocket と Resume は許可待ちの対象外。監査に出すのは Bot・Shard・世代、起動理由、許可結果、送信・Resume の試行と結果までとし、認証情報やセッション ID を記録しない。

## 試験計画

- 注入時計で間隔の直前と境界、時間枠の失効、2 枠の判定、逆行、拒否・延期で履歴が増えないことを確認する。
- ローカルの子プロセスを使い、初回の同時起動、管理 start / restart、待機 Gateway、更新失敗後の復旧、自動復旧が同じ制限を受けることを確認する。同じ Bot の複数 Shard の同時要求で 1 件のみ起動できることと、別 Bot の起動が進むことを確認する。
- B の受入試験では初回と無効セッション後の実送信、Resume による非消費、IPC 切断・重複・期限切れ、旧版との混在を模擬 Discord で再現する。許可または READY の観測だけでは送信を証明できないため、模擬サーバーで opcode 2 の受信を確認する。実 Discord の検証は別途行う。

## ユーザー判断が必要な点

1. B に進む場合、IPC 形式と旧版 Gateway の移行方法。botd 不在時に既存 Gateway の再 Identify を保留する案の可否と、待ちの期限。
2. Discord の残量取得・更新頻度、ローカルの保守的な上限を残すか、必要な復旧予算を何枠にするか。botd 再起動で履歴を失った際の再照会・待機・永続化の方針。
3. 再 Identify、初回起動、手動操作、待機更新の正式な優先順位と公平性。手動 restart の停止前に予算を予約するか、管理操作を待機可能にするか。
4. 複数 botd・複数ホストでの予算の調整範囲と、既存の quorum・lease への組み込み時期。
