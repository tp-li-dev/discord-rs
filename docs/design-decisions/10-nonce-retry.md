# 10. 結果不明の Create Message の nonce 再試行

[DDR-0007 の優先順位 5](discussion-records/0007-p1-tmpfs-and-cutover-order.md)、[結果不明の外部操作](08-unknown-outcome-operations.md)、[レビュー T2](review-2026-11-17-astra-sol.md) に対応する。実機の 2 回投稿と対照試験で重複抑止を確認し、運用者が承認するまで、準備した再試行は無効にする。

## 現在の結果不明と受領

以下の行番号は変更前のコミット `5b60670` を参照する。

- `src/src/main.rs:1201–1221` の `live_gateway` が Bot token を読み、シャードと Twilight HTTP client を構築する。Core/Worker 分岐は `src/src/main.rs:964–970` で先に分かれる。子 Worker の起動は環境を消去し（`src/src/worker.rs:800–805`）、分離 Core の起動仕様には資格情報参照がない（`src/src/shard_runtime.rs:180–188`、`src/src/daemon.rs:7945–7952`）。
- 通常のテキスト返信は `src/src/main.rs:1354–1397` で応答追跡を予約して Core に処理を依頼し、Gateway が `create_message` を 10 秒で打ち切る。成功応答のステータスを保持し、Twilight エラーとタイムアウトは `None` に畳む。`api_outcome`（`src/src/main.rs:690–694`）が `None` を `worker::ApiOutcome::Unknown` に変換する。これが稼働経路の結果不明で、共通プロトコルの名称は `ApiOutcome::UnknownOutcome`（`src/src/protocol.rs:179–196`）。`rest_unknown` という識別子は変更前の `src/src` に見当たらない。
- `!workers` と Interaction の拒否応答は `confirmed_response` で成功の有無を判定する（`src/src/main.rs:98–135,1337–1352`）。Slash の初期 ACK と応答編集は別経路（`src/src/main.rs:1475–1555`）。いずれも Gateway 側のアプリケーション再試行はなく、Twilight の内部 429 処理は別に存在する。
- `begin_response_delivery` の追跡は dispatch 後に Core 世代を固定する（`src/src/worker.rs:2647–2652`）。`finish_response` はその世代の IPC 接続に `ApiResult` を送り、操作 ID、結果、世代の識別情報、PID が一致する受領を照合する。結果不明は受領があっても追跡を解消しない（`src/src/worker.rs:2836–2896`、`src/src/response_drain.rs:159–175`）。
- `manage list-unknown` は認証済みローカル管理経由で Gateway の揮発性スナップショットを得る（`src/src/bin/manage.rs:370–387,759–764`、`src/src/local_control.rs:65–74`）。一覧は Worker インデックスと操作 ID を持つ。`reconcile_response` は外部照合の証跡がある場合の内部 API で、解消用の管理 IPC は公開していない（`src/src/worker.rs:2588–2636`）。

## Discord と Twilight の仕様

- Discord の `POST /channels/{channel.id}/messages` は最大 25 文字の整数または文字列の `nonce` を受け取る。`enforce_nonce: true` と nonce を指定すると、過去数分に作られた同一作者・同一 nonce のメッセージが返り、新規作成は行われないと公式資料にある。正確な保持時間、チャンネル横断の範囲、削除・編集後、実環境での動作は未確認。 [Discord Create Message](https://docs.discord.com/developers/resources/message#create-message)
- Twilight HTTP 0.18.0 の `CreateMessage::nonce(u64)` には `enforce_nonce` の設定がない。`payload_json(&[u8])` は他のビルダー項目（添付ファイル以外）を置き換える。添付のない場合は HTTP の JSON 本文を直接置き換える。 [Twilight CreateMessage](https://docs.rs/twilight-http/0.18.0/twilight_http/request/channel/message/struct.CreateMessage.html#method.payload_json)
- 本文全体を一度シリアライズする。現在のテキスト送信では `content`、文字列の `nonce`、`enforce_nonce: true`、`allowed_mentions` が必要。既存の `AllowedMentions::default()`（`{"parse":[]}`）を入れてメンション抑止を維持する。`allowed_mentions` を省略した通常のメッセージでは Discord がメンションを解析する。埋め込み、参照、添付、フラグ等を将来対象にするなら、その操作の全フィールドと送信形式を別途検証する。 [Discord Allowed Mentions](https://docs.discord.com/developers/resources/message#allowed-mentions-object)

## 適用範囲と期限

- 対象は、同一 Bot・同一チャンネルへの、発行元 Core の受領を追跡する通常のテキスト返信の Create Message。初回から `enforce_nonce: true` を送る。既定は無効で、この場合は従来のビルダーと 10 秒の待機を使う。Ubuntu の Gateway 起動オプション `--enable-create-message-nonce-retry` でのみ有効化できる。botd に指定すると botd が管理する全 Bot の Gateway の起動（待機 Gateway を含む）に適用される。検証中はこのオプションを指定しない。
- 論理操作ごとに OS 乱数 96 ビットを一度採り、24 桁の小文字 16 進文字列にする。同じ Bot の別チャンネル、シャード、Gateway 世代、再起動をまたいで衝突しにくくする。イベント ID やプロセス内の採番を nonce に使わない。乱数取得や本文構築に失敗したら再試行付きの送信を中止し、通常の結果不明の経路に載せる。
- nonce を含む本文のバイト列を、同じ Gateway の同じ応答追跡のメモリに期限まで保持する。両試行で同じ Bot、チャンネル、本文を使う。プロセスをまたぐリプレイを行わないため、ディスクや IPC に nonce・本文を保存しない。Gateway が失われた場合も、別の Gateway はこの操作を再開しない。
- Twilight が明示的に返した通信エラーまたは HTTP タイムアウトに限り、250 ms 待って最大 1 回再試行する。初回のレート制限待ちから数えて合計 15 秒で打ち切る。アプリケーション側の期限切れは、レート制限待ちか送信後かを区別できないため、その場で結果不明とする。HTTP エラー、ローカルのリクエスト構築エラー、応答解析エラーには再試行を追加しない。2 回目の 2xx が確認できた場合だけ最初の不明を解消する。2 回目の拒否やエラーでは初回の結果が確定しないため、不明のまま扱う。
- Discord の 429 では `Retry-After` または JSON の `retry_after`（秒）の経過前に再送しない。Twilight 0.18.0 は 429 を内部で再送し、global scope ではパーサーが待ち時間を返さない。対象リクエストでは共有 Twilight client のレート制限許可を使い、送信前フックで 1 HTTP 送信までに制限する。429 を受けた操作に新しい HTTP 試行を追加しないため、待ち時間を推測して再送することもない。既存 client の Bot 全体・複数 Gateway 間のレート制限はこの変更の範囲外であり、運用で有効にする前に確認する。 [Discord Rate Limits](https://docs.discord.com/developers/topics/rate-limits#exceeding-a-rate-limit)、[Twilight ResponseFuture の実装](https://docs.rs/twilight-http/0.18.0/src/twilight_http/response/future.rs.html)

## 世代と手動照合

再試行中も最初の `ResponseGuard` と操作 ID を保持する。最終結果の `ApiResult` を発行元の Core 世代に一度だけ送り、その受領を待つ。再試行中に切り替わった Core へ結果を付け替えない。Core の受領欠落、Gateway の停止、または最終結果不明では、照合できる範囲で従来の `UnknownOutcome` として扱う。Core の受領欠落を理由に Discord へ再送しない。Cog に新しい同期エラーやメッセージ ID を返す API は追加しない。

`!workers`、Webhook、Interaction の初期応答・編集・追加応答、メッセージの編集・削除、リアクション、ファイル付き送信は対象外。上書きや後続操作との順序も含め、安全性が実証された操作だけを将来個別に追加する。運用者は `list-unknown` の取得時刻・操作 ID と送信先の履歴などを照合し、必要な対応を判断する。結果不明のままの再送、プロセス再起動後の再送、無期限の重複排除や exactly-once は提供しない。

有効化の判定には [実機プローブ](10-nonce-probe-runbook.md) の同一 ID、異なる nonce の対照の別 ID、実際の投稿数、2 回の間隔を使う。即時の 2 回投稿に加え、同じ条件で 15 秒以上経過した遅延試行も同一 ID を返すことを確認する。試験結果と Linux での検証をレビューしてから、対象 Gateway に限り明示的に設定する。
