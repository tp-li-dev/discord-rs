# 09. Identify スケジューラー

- 対象: [DDR-0007](discussion-records/0007-p1-tmpfs-and-cutover-order.md) の優先順位 4
- 仕様の基準: [architecture.md](../architecture.md) §5・§7、調査の起点: [Astra / Sol レビュー](review-2026-11-17-astra-sol.md#t3-identify-と-resume05-identify-and-resumemd)

この文書は起動許可を一元化した段階（案 A）の設計を記録する。現在は `--identify-send-grants` OFF（既定）でこの方式を使う。ON で使う送信許可（案 B）の実装は [設計判断 11](11-identify-send-grant.md) を参照。

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

## 検討時の選択肢

**A. botd 内の起動許可を一元化する。** 単一 botd が Bot ID ごとの履歴を持つ。初回・手動・待機・更新失敗時の復旧・自動復旧が、同一の判定と記録を通る。更新の 2 枠条件も同じ履歴で判定し、消費は今回の起動分のみ。起動許可は実送信より前なので、起動遅延で送信間隔が縮む場合や、起動済み Gateway の再 Identify には対応できない。

**B. A に加えて Gateway が実送信前に許可を得る。** DDR-0007 の送信調整にはこちらが必要。検討時点の匿名 stdin パイプは botd → Gateway の `Shutdown` / `Activate` 専用（`src/src/shard_runtime.rs:14–24`、`src/src/main.rs:396–435`）で、Identify 要求と応答には別経路が必要だった。Gateway ↔ Core の認証付き IPC は別の相手であり、Core を認証情報や許可の中継に使わない。

B の追加には、起動時に継承する双方向のローカル IPC、またはプロセス認証付きソケットが必要。要求を Bot・Shard・世代に結び付け、1 回限りの短命の許可、切断時の取り消し、サイズと待ち行列の上限、旧版 Gateway との互換性を設計する。Twilight の Identify キューを通る初回・再 Identify を捕捉できる範囲と、許可から実送信までの遅延を試験で確認する。送信許可待ちが Heartbeat や Resume を詰まらせないようにする。起動時の予約を初回送信の許可へ引き継ぎ、同一試行の二重消費を避ける。複数ホストへ広げる場合は既存の認証・quorum・lease に従う調整が別途必要。

**採用した順序は A → B。** 先に A で起動許可を一本化し、その後に B をオプションで追加した。

## 起動許可と優先順位

- 実装案では既存のローカル値（30 秒、1 時間 10 回）を全実 Gateway の起動に適用する。これらは Discord の値とは別の運用上の上限。botd の単一所有者が許可の判定から記録まで同期的に行う。起動失敗や READY 未達でも使用済みの許可は戻さない方針とする。模擬 Discord の予算は試験用の既存経路を維持する。
- 更新は現行の復旧機会の保護を維持する。待機起動には待機用と復旧用の枠が必要で、待機が起動したあとは同じ Bot の別 Shard が次の枠を使うのを抑止する。有効化後に失敗した場合は対象 Shard の復旧が保護された機会を使う。間隔・回数上限は復旧にも適用する。
- 通常の監視 tick は自動復旧を処理してから管理操作を進める。使用済みの枠の横取りや一般の優先キューは追加しない。B の優先順位案は、停止中の稼働 Shard の再 Identify・復旧（更新後の復旧を含む）、手動 start / restart、稼働中 Shard の更新用待機の順。初回自動起動をどの順位に置くか、同順位の公平性や更新の長期待ちをどう扱うかは未決定。

| 要求 | 間隔待ち・復旧枠の保護中 | 回数上限 |
| --- | --- | --- |
| 初回自動起動・自動復旧・更新失敗からの復旧 | `Deferred: IdentifyBudget` を監査に出し、監視 tick で再試行する。待機中は子を起動せず、再起動回数を消費しない。 | `Degraded: IdentifyUnavailable`。初回の同期的な拒否は起動結果にも反映する。 |
| 手動 start / restart | `IdentifyDeferred` を返す。restart は停止前に拒否する。間隔が間もなく満了する場合も待機せず、再試行は新たな操作とする。 | `IdentifyUnavailable` で拒否する。restart は停止前に拒否する。 |
| gateway-update の待機起動 | 既存の更新期限まで `waiting_reason: IdentifyBudget` を表示して待つ。期限切れは `IdentifyDeferred`。 | `IdentifyUnavailable` で失敗し、旧 Gateway を維持する。 |

同じ Bot の更新後の復旧機会が保護されている間、管理操作による追加の再起動禁止が対象 Shard の復旧を妨げないよう、start / restart / gateway-update は受付時点で明示的に拒否する。

## 手動 restart の停止前予約

- 認証・対象確認・排他チェックの後、botd は新 Gateway の枠を Bot 単位で予約する。予約が全件成功するまで停止要求も自動復旧の保留も行わない。受付から起動まで、同じ Bot の通常起動・自動復旧・待機起動は予約を横取りできない。別 Bot の予算は独立している。
- 予約した枠は停止中も保持し、`ChildRecovery` が代替 Gateway の起動を許可する時点で履歴に記録する。その時刻から次の許可間隔を数える。停止失敗・タイムアウト・操作の取り消しで終了した場合は、未使用の枠を解放する。起動を試みた枠は、spawn が失敗しても消費済みとする。
- Bot 全体の restart、`--shard` リスト、`--guild` から解決した Shard リストも、対象全件の予約を一括で行う。既定の 30 秒間隔では同じ Bot の実 Gateway を複数同時に予約できない。回数枠が足りていても `IdentifyDeferred` を返すので、運用者は間隔を空けて 1 Shard ずつ操作する。枠の不足が判定された場合は `IdentifyUnavailable` を返す。明示的な模擬 Discord の起動は従来どおり実 Gateway の予算を使わない。
- `botctl restart --all --yes` は認証済みの一件の操作として送信し、botd が全ローカル Shard を対象にする。Bot をまたいで検査し、後続の Bot が拒否された場合は先行 Bot の仮予約も返す。既存の操作上限である 64 Shard を適用する。実行結果には `operation.command` の `restart-all`、1 件の操作 ID と対象 Shard の結果が入る。`--dry-run` で対象 Bot と非ローカルの Bot を確認できる。実行には対応する botctl と botd が必要。
- 予約を拒否したときは受付応答の `error` に固定値 `IdentifyDeferred` または `IdentifyUnavailable` を返し、その操作を登録しない。開始済みの操作は従来の操作 ID で照会でき、停止や起動の失敗は対象 Shard に固定のエラー名を返す。診断には秘密やファイルパスを含めない。一括性は Identify の受付に適用される。受付後に実際の停止や起動が失敗した場合は、すでに発行した停止を取り消せる保証はない。

## 停止・削除と Identify の順序の確認

- 通常の明示的な start は稼働中の Gateway を拒否し、停止済みの Shard を対象とする。残存する起動用ファイルの再取得や削除は、許可後の launch で生存確認を経て行う。自動復旧による許可待ちは既にプロセスが失われた Shard に対するもので、別 Shard の健全な Gateway を停止しない。
- 通常の gateway-update は待機 Gateway の許可を得てから待機用マーカーを消去して spawn し、準備完了・昇格確認の後に旧 Gateway を停止する。昇格を送信すると epoch の下限が上がり、候補へ Core の所有権が移っている可能性がある。この段階で失敗した場合、旧 Gateway が安全にサービスを再開できる保証はない。ここでは既存どおり失敗した候補と旧 Gateway を終了し、事前に保護した復旧枠を間隔が満了した時点で使う。`standby_and_failed_activation_recovery_share_the_launch_budget` が待機・拒否での PID 維持、許可前のマーカー維持、昇格失敗時の停止・間隔待ち・境界での新 PID を確認する。
- 明示的な模擬 rolling ランタイムでは候補を先に許可する。制御接続を失って受付を閉じた Gateway の終了処理も障害復旧に当たり、終了を観測した後の再起動には模擬の共有予算が適用される。`gateway_os_death_recovers_then_waits_when_the_shared_bot_identify_budget_is_spent` が OS 終了観測後に残量不足で起動せず、他の Shard と Bot が応答を続けることを確認する。

## 障害と隔離

- Bot ID が異なる履歴は別管理。待機用の保護も同じ Bot にだけ及ぶ。Shard ごとの所有権・排他と管理操作の認証は既存の経路に従う。
- OFF（案 A）では botd 再起動でメモリ上の履歴と復旧枠の保護が失われる。送信許可チャネルは作らず、生き残った Gateway は従来の Twilight キューで共有予算を使わずに再 Identify できる。生存プロセスの二重起動は既存の起動予約で防ぐ。
- 起動調整の経過時間には botd の単調時計を使うため壁時計の修正で履歴は失効しない。注入した時計が逆行した場合は既存の `ClockRegressed` で新規起動を停止する。
- ON（案 B）では稼働中の Gateway の再 Identify も共有予算を消費する。同一 OS 起動内はジャーナルを再生し、boot ID が変わった場合は検出から 1 時間と許可の有効期間（2 秒）分を保留する。botd 再起動前のチャネルは復元せず、旧 Gateway の新規 Identify は保留される。Heartbeat と Resume は許可対象外。後継 Gateway は新しいチャネルを受け取る。

## 試験計画

- 注入時計で間隔の直前と境界、時間枠の失効、2 枠の判定、逆行、拒否・延期で履歴が増えないことを確認する。
- ローカルの子プロセスを使い、初回の同時起動、管理 start / restart、待機 Gateway、更新失敗後の復旧、自動復旧が同じ制限を受けることを確認する。同じ Bot の複数 Shard の同時要求で 1 件のみ起動できることと、別 Bot の起動が進むことを確認する。
- 注入時計と実際の子プロセスで、回数枠・間隔による restart の拒否で PID が変わらないこと、停止要求失敗で未使用枠を返すこと、Shard リストと `--all` の予約を一括で拒否すること、保留中に同じ Bot の別途起動が枠を使えず別 Bot の restart は進むことを確認する。CLI と botd の結合試験では `--all` が一件の照会可能な操作になることを確認する。
- B の受入試験では初回と無効セッション後の実送信、Resume による非消費、IPC 切断・重複・期限切れ、旧版との混在を模擬 Discord で再現する。許可または READY の観測だけでは送信を証明できないため、模擬サーバーで opcode 2 の受信を確認する。実 Discord の検証は別途行う。

## ユーザー判断が必要な点

1. Discord の残量の継続取得と更新頻度、ローカル上限と復旧予算の見直し。
2. 再 Identify、初回起動、手動操作、待機更新の間の優先順位と公平性の見直し。
3. 複数 botd・複数ホストでの予算の調整範囲と、既存の quorum・lease への組み込み時期。
