# 10. 結果不明の Create Message の nonce 再試行

[DDR-0007 の優先順位 5](discussion-records/0007-p1-tmpfs-and-cutover-order.md)、[結果不明の外部操作](08-unknown-outcome-operations.md)、[レビュー T2](review-2026-11-17-astra-sol.md) に対応する。既定は無効。本番での有効化には Ubuntu での障害注入の証跡と運用者の承認が必要。

## 現在の結果不明と受領

以下の行番号は変更前のコミット `5b60670` を参照する。

- `src/src/main.rs:1201–1221` の `live_gateway` が Bot token を読み、シャードと Twilight HTTP client を構築する。Core/Worker 分岐は `src/src/main.rs:964–970` で先に分かれる。子 Worker の起動は環境を消去し（`src/src/worker.rs:800–805`）、分離 Core の起動仕様には資格情報参照がない（`src/src/shard_runtime.rs:180–188`、`src/src/daemon.rs:7945–7952`）。
- 通常のテキスト返信は `src/src/main.rs:1354–1397` で応答追跡を予約して Core に処理を依頼し、Gateway が `create_message` を 10 秒で打ち切る。成功応答のステータスを保持し、Twilight エラーとタイムアウトは `None` に畳む。`api_outcome`（`src/src/main.rs:690–694`）が `None` を `worker::ApiOutcome::Unknown` に変換する。これが稼働経路の結果不明で、共通プロトコルの名称は `ApiOutcome::UnknownOutcome`（`src/src/protocol.rs:179–196`）。`rest_unknown` という識別子は変更前の `src/src` に見当たらない。
- `!workers` と Interaction の拒否応答は `confirmed_response` で成功の有無を判定する（`src/src/main.rs:98–135,1337–1352`）。Slash の初期 ACK と応答編集は別経路（`src/src/main.rs:1475–1555`）。いずれも Gateway 側のアプリケーション再試行はなく、Twilight の内部 429 処理は別に存在する。
- `begin_response_delivery` の追跡は dispatch 後に Core 世代を固定する（`src/src/worker.rs:2647–2652`）。`finish_response` はその世代の IPC 接続に `ApiResult` を送り、操作 ID、結果、世代の識別情報、PID が一致する受領を照合する。結果不明は受領があっても追跡を解消しない（`src/src/worker.rs:2836–2896`、`src/src/response_drain.rs:159–175`）。
- `manage list-unknown` は認証済みローカル管理経由で Gateway の揮発性スナップショットを得る（`src/src/bin/manage.rs:370–387,759–764`、`src/src/local_control.rs:65–74`）。一覧は Worker インデックスと操作 ID を持つ。`reconcile_response` は外部照合の証跡がある場合の内部 API で、解消用の管理 IPC は公開していない（`src/src/worker.rs:2588–2636`）。

## Discord と Twilight の仕様

- Discord の `POST /channels/{channel.id}/messages` は最大 25 文字の整数または文字列の `nonce` を受け取る。`enforce_nonce: true` と nonce を指定すると、過去数分に作られた同一作者・同一 nonce のメッセージが返り、新規作成は行われないと公式資料にある。正確な保持時間、チャンネル横断の範囲、削除・編集後の挙動は未確認。 [Discord Create Message](https://docs.discord.com/developers/resources/message#create-message)
- Twilight HTTP 0.18.0 の `CreateMessage::nonce(u64)` には `enforce_nonce` の設定がない。`payload_json(&[u8])` は他のビルダー項目（添付ファイル以外）を置き換える。添付のない場合は HTTP の JSON 本文を直接置き換える。 [Twilight CreateMessage](https://docs.rs/twilight-http/0.18.0/twilight_http/request/channel/message/struct.CreateMessage.html#method.payload_json)
- 本文全体を一度シリアライズする。現在のテキスト送信では `content`、文字列の `nonce`、`enforce_nonce: true`、`allowed_mentions` が必要。既存の `AllowedMentions::default()`（`{"parse":[]}`）を入れてメンション抑止を維持する。`allowed_mentions` を省略した通常のメッセージでは Discord がメンションを解析する。埋め込み、参照、添付、フラグ等を将来対象にするなら、その操作の全フィールドと送信形式を別途検証する。 [Discord Allowed Mentions](https://docs.discord.com/developers/resources/message#allowed-mentions-object)

## 適用範囲と起動設定

- 対象は、同一 Bot・同一チャンネルへの、発行元 Core の受領を追跡するプレフィックスコマンドのプレーンテキスト返信。通常の Create Message として初回から `enforce_nonce: true` を送る。無効時は従来のビルダーと 10 秒の待機を使う。
- Ubuntu の live Discord 用 `botd` は、`--enable-create-message-nonce-retry-for BOT_ID` を Bot ごとに指定できる。起動時に `--file` の構成全体と完全な `bot_id` を照合する。未登録 ID が一つでもあれば起動エラーになり、省略時は全 Bot が無効になる。対象 Bot のローカルシャードの Gateway 起動仕様に `--enable-create-message-nonce-retry` を一度設定し、通常起動・障害復旧・待機 Gateway 起動で共有する。既存の起動時オプションでデーモン単位の試験運用を制御できるように、繰り返し指定できる CLI を採用した。Bot の配置設定に新しい項目を追加せずに運用対象を変更でき、設定変更の反映には `botd` の再起動と対象 Gateway の再起動が必要になる。

| 対象外 | 理由 |
| --- | --- |
| 添付ファイル | ファイル付き送信は multipart を使う。添付メタデータとファイル本体の再送時の一致を別途検証する必要がある。 |
| コンポーネント | 現行の JSON はコンポーネントと関連するフラグを持たない。表示や操作の意味を保つには、フィールド全体と再送時の挙動を検証する必要がある。 |
| `message_reference` 付きの返信 | 参照先と返信メンションの設定を固定する必要がある。参照先の削除や取得不能が試行間に起きた場合の挙動も未検証。 |
| スレッド作成 | チャンネル上のテキスト作成と異なる API 操作で、スレッド自体の重複抑止を確認していない。 |
| Interaction のコールバック・編集・追加応答 | コールバックと追加応答は別 API で、Create Message の nonce による重複抑止は適用範囲外。初期 ACK には固有の期限があり、編集の再送は後から行われた編集を上書きし得る。 |
| `!workers`、Webhook、メッセージの編集・削除、リアクション | `!workers` は発行元 Core の受領追跡を使わない。ほかは別 API 操作であり、操作の順序や結果の照合に個別の検証が必要。 |

## 再試行条件と期限

- 論理操作ごとに OS 乱数 96 ビットを一度採り、24 桁の小文字 16 進文字列にする。同じ Bot の別チャンネル、シャード、Gateway 世代、再起動をまたいで衝突しにくくする。イベント ID やプロセス内の採番を nonce に使わない。乱数取得や本文構築に失敗したら再試行付きの送信を中止し、通常の結果不明の経路に載せる。
- nonce を含む本文のバイト列を、同じ Gateway の同じ応答追跡のメモリに期限まで保持する。両試行で同じ Bot、チャンネル、本文を使う。プロセスをまたぐリプレイを行わないため、ディスクや IPC に nonce・本文を保存しない。Gateway が失われた場合も、別の Gateway はこの操作を再開しない。
- 再試行するのは、初回の HTTP 応答ヘッダーを得る前に Twilight 0.18.0 が返した通信エラー（`RequestError` のうち通信経路由来のもの）または Twilight の HTTP 応答待ちタイムアウト（`RequestTimedOut`）。いずれも初回の結果は不明として扱い、エラーから 250 ms 待って最大 1 回、同じ本文で再試行する。初回の limiter 待ちを始める前に設定した 15 秒の単一の期限に、両試行の limiter 待ち、HTTP 応答待ち、バックオフが含まれる。期限到達後は limiter が許可を返しても送信前フックが送信を拒否する。アプリケーション側の期限切れは結果不明で終了する。
- 受信済みの HTTP エラー応答、ローカルのリクエスト構築エラー、応答解析エラーで再試行は行わない。Twilight 0.18.0 は 429 以外の非 2xx 応答の本文が途中で切れたときも `RequestError` を返す。この場合は原因の `DeserializeBodyError` で本文の読取失敗だと判別できる。元の HTTP ステータス番号は呼び出し側に渡らないため、結果は `UnknownOutcome` に載せる。2 回目の 2xx を確認できれば結果不明を解消する。2 回目の拒否やエラーでも初回の結果は不明のままになる。 [Twilight ResponseFuture の実装](https://docs.rs/twilight-http/0.18.0/src/twilight_http/response/future.rs.html)
- Discord の 429 が観測された場合は、この操作を再試行しない。Twilight 0.18.0 自体は 429 で再送するため、共有 limiter を使いながら、送信前フックで各試行につき HTTP 送信を 1 回に制限する。Bot 全体・複数 Gateway 間のレート制限の挙動は本番で有効にする前に別途確認する。 [Discord Rate Limits](https://docs.discord.com/developers/topics/rate-limits#exceeding-a-rate-limit)

## 世代と手動照合

再試行中も最初の `ResponseGuard` と操作 ID を保持する。最終結果の `ApiResult` を発行元の Core 世代に一度だけ送り、その受領を待つ。再試行中に切り替わった Core へ結果を付け替えない。Core の受領欠落、Gateway の停止、または最終結果不明では、照合できる範囲で従来の `UnknownOutcome` として扱う。Core の受領欠落を理由に Discord へ再送しない。Cog に新しい同期エラーやメッセージ ID を返す API は追加しない。

運用者は `list-unknown` の取得時刻・操作 ID と送信先の履歴などを照合し、必要な対応を判断する。自動再試行は同じ Gateway 内の 15 秒間に限られる。無期限の重複排除や exactly-once の保証はない。

## 本番で有効化する条件

2026-11-21 の実機プローブでは、同一作者・同一 nonce・`enforce_nonce: true` で初回から約 0.4 秒後と約 30.9 秒後に送ったときに同じメッセージ ID が返り、異なる nonce の対照には別 ID が返った。遅延の観測は約 30 秒の 1 条件までで、Discord の正確な保持時間と再現性は未確認。 [実機プローブの手順](10-nonce-probe-runbook.md)

本番でいずれかの Bot を有効にする前に、Ubuntu 上の Gateway の実送信経路で、承認済みの試験用 Bot とチャンネルを使って障害を注入する。初回 POST を Discord が確定した後で応答を Gateway に渡さず、Gateway が同じ nonce と本文で 15 秒の期限内に一度再試行することを確認する。初回と再試行のメッセージ ID が一致し、チャンネルの履歴に対象メッセージが 1 通だけ見えることを、試行間隔とともに記録する。応答が両方届くプローブとローカルの模擬 HTTP テストは、この障害条件の代わりにならない。証跡をレビューし、運用者の承認を得た Bot ID だけを許可リストに追加する。
