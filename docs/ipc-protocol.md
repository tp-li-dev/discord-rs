# 現行 Worker IPC（v5 / Hello 拡張）

## 実装範囲とフレーム

`worker.rs` の親 Manager と `--worker` 子プロセス間で、stdin/stdout の改行区切り UTF-8 JSON を使用する。各 JSON オブジェクトの `type` は snake_case。stdout はプロトコル専用。1 フレームは末尾 LF を含め最大 **1 MiB (1,048,576 bytes)**、LF のない EOF と超過フレームは拒否する。受信は上限 + 1 byte までに制限する。親の要求/応答は 5 秒 timeout。一般の親送信には共通サイズ事前検査がなく、子側受信で拒否する。状態・結果・restore は生成時にもサイズ検査する。

architecture §8 の u32 big-endian length prefix + MessagePack / 初期 8 MiB は設計案であり、現行 Worker の実フレームではない。`protocol.rs` の Envelope/Control/Data も現行 Worker の wire envelope ではない。JSON の独立したネスト深度・イベントサイズ・キュー長制御はここでは追加していない。

## 起動と互換性

1. 親は `{"type":"health"}` を送る。
2. 子は `{"type":"ready","protocol":5,"protocol_min":5,"protocol_max":5,"required_capabilities":["snapshot-v3"],"supported_capabilities":["hello-v1","command-dispatch-v1","ready-event-v1","slash-arguments-v1"],"build":"...","pid":123}` を返す。
3. 親は protocol=5、範囲と必須 capability を検査する。min/max 両方省略の旧 v5 Ready は受理し、片方のみは拒否する。
4. `hello-v1` 広告時だけ、親は `{"type":"hello","hello":{...}}` を送り、子も同じ形の Hello を返す。広告のない旧 Worker には Hello を送らず、旧 capability 動作を維持する。

Hello 本体の実例:

```json
{"protocol_min":5,"protocol_max":5,"identity":{"bot_id":0,"shard":{"id":0,"total":1},"generation_id":123,"ownership_epoch":0},"required_capabilities":["snapshot-v3"],"optional_capabilities":["hello-v1","command-dispatch-v1","ready-event-v1","slash-arguments-v1"]}
```

双方で範囲の妥当性・共通範囲、Bot/Shard（total を含む）/generation_id/ownership_epoch の一致、相互の必須 capability の対応を検査する。共通範囲の最大バージョンを選び、必須・任意の広告の共通集合をソート・重複排除して使用する。未知の必須は拒否、未知の任意は無視する。現行 Worker の実装範囲は v5 のみ。子の拒否は `{"type":"rejected","reason":"..."}`。親は不一致・拒否・不正応答・timeout で候補を破棄し、既存の差替え処理は旧 Worker を保持する。

親が生成した接続 identity を子専用環境変数 `BOT_WORKER_IPC_IDENTITY` に渡し、子はこれを期待値として照合する。各 spawn の generation_id は乱数。現行 demo Manager の API は配置 identity を受け取らないため Bot=0、Shard=0/1、epoch=0 固定であり、実 Bot/Shard 配置への接続は未実装。これは認証や lease の証明ではなく、遠隔接続にも使用しない。旧親との互換のため子は Hello を必須化せず、Health と既存要求を受け付ける。identity 未設定時の Hello は拒否する。

## 現行要求と応答

| 要求 type / フィールド | 応答 |
|---|---|
| health | ready（上記） |
| hello / hello | hello / hello、または rejected / reason |
| dispatch / key:u64, content:string | result / content:string または null, state:JSON |
| command / name:string | result |
| slash_command / name:string, args:名前と SlashValue のペア配列 | result |
| ready_event / bot_name:string | result |
| tick | result |
| snapshot | snapshot / version:u32, state:JSON |
| restore / version:u32, state:JSON | restored、または rejected / reason |

アプリ処理失敗も rejected を返す。SlashValue は framework の serde 型に従う。snapshot の状態形式は現在 version=3。独立した ACK、request_id、timestamp、drain/switch 制御メッセージ、MessagePack 分割転送はこの Worker wire にはない。プロセスの起動世代とアプリ状態 version は別の概念。
