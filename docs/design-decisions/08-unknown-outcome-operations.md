# 08. 結果不明の外部操作

## 何が問題か（例）

Bot が「メッセージを送る」という通信（HTTP）を Discord に出し、通信の途中で切れたとします。**Discord が受け取ったのか、受け取っていないのか、分かりません。**

- もう一度送ると、受け取っていたなら**二重投稿**になる。
- 送らないと、受け取っていなかったなら**メッセージが届かない**。

これは「欠陥」ではなく、**どちらに寄せるかの方針**です。

## 私たちの現状（確認済み。出典：`docs/known-limitations.md`、`docs/architecture.md`）

- **結果が不明なときは、自動で再送しない。** 「結果不明」として記録し、運用者が確認する。二重投稿を避ける側に寄せている。
- 理由：複数のプロセス（旧 Gateway と新 Gateway、Core の再起動など）が関わる構成では、勝手に再送すると二重になりやすい。
- exactly-once（ちょうど 1 回）は保証しない。重複を減らし、欠落を明示する設計。

## discord.py v2.6.4 の場合（ソースで確認）

- `Messageable.send` は nonce の指定がなければ 64 ビットの乱数を 1 回生成する。HTTP 本文には nonce を文字列で入れ、`enforce_nonce=true` を付ける。同じ呼び出しの HTTP 再試行では、この本文の nonce を再利用する。[Messageable.send](https://github.com/Rapptz/discord.py/blob/v2.6.4/discord/abc.py#L1554-L1575)、[handle_message_parameters](https://github.com/Rapptz/discord.py/blob/v2.6.4/discord/http.py#L194-L196)、[HTTPClient.request](https://github.com/Rapptz/discord.py/blob/v2.6.4/discord/http.py#L589-L723)。
- `HTTPClient.request` は初回を含め最大 5 試行。再試行の条件を満たす 429 は `retry_after` を待ち、500・502・504・524 と一部の接続リセット（`errno` 54 または 10054）はバックオフする。他の 5xx や試行を使い切ったときは例外になる。[HTTPClient.request](https://github.com/Rapptz/discord.py/blob/v2.6.4/discord/http.py#L589-L723)。

## 改善の候補：nonce による期間限定の重複抑止

Create Message では、`nonce` は整数または文字列で最大 25 文字。`enforce_nonce=true` と nonce を指定すると、過去数分の同じ投稿者・同じ nonce のメッセージがあればそのメッセージを返す。保持期間の正確な長さ、チャンネルをまたぐ範囲、削除・編集後の挙動は未確認。[Discord Create Message](https://docs.discord.com/developers/resources/message#create-message)。

## 調査の結果（2026-11-16 夜、twilight-http 0.18 のソースで確認）

- `CreateMessage::nonce(u64)` はある（`twilight-http-0.18.0/src/request/channel/message/create_message.rs`、280 行付近）。**`enforce_nonce` を指定するメソッドはない**（`nonce` の記述のみ）。
- **`payload_json(&[u8])` で、リクエスト本文を JSON で直接渡せる**（「呼ぶと、`attachments` 以外の他のフィールドは無視される」）。本文に `"enforce_nonce": true` と `"nonce"` を含めれば、ライブラリが未対応でも送れる見込み。
- 現在、通常の返信と `!workers` は `http.create_message(...)` で送っている（`src/src/main.rs` の 1298・1335 行付近）。
- **未確認（実機）**：同じ nonce で再送したときの重複抑止。実際に投稿する試験には投稿先と許可が要る。
- 確認の手順（案）：試験用の小さなプログラムで、(1) 同じ nonce・同じ本文を 2 回送り、返ったメッセージ ID が同じか、チャンネルに 1 通だけ残るかを見る。(2) 1 分・5 分と時間を空けて再度送り、重複を除く時間の目安を見る。

## 提案する方針

- 原則：**自動で再試行しない**（現状維持）。
- 例外：**重複を防げると確認できた操作だけ**（検証した重複抑止期間内のメッセージ作成、冪等な操作）を再試行する。
- どの操作を再試行してよいかは、**操作の種類ごとの一覧**として、文書とコードで持つ。

## 未確認（実機）

- 429・5xx の実際の挙動（意図的に起こしていない）。
- Interaction（スラッシュ）の応答の期限（3 秒）と、結果不明の関係。

## 決めること

[decisions-needed.md](decisions-needed.md) の D3（調査を進めてよいか）。
