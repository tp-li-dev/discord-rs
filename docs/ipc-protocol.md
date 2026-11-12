# 現行 Worker IPC（v5 / Hello 拡張）

## 実装範囲とフレーム

`worker.rs` の親 Manager と `--worker` 子プロセス間で、stdin/stdout の改行区切り UTF-8 JSON を使用する。各 JSON オブジェクトの `type` は snake_case。stdout はプロトコル専用。1 フレームは末尾 LF を含め最大 **1 MiB (1,048,576 bytes)**、LF のない EOF と超過フレームは拒否する。受信は上限 + 1 byte までに制限する。親も書込前にフレームサイズを検査し、状態・結果・restore は生成時にもサイズ検査する。通信開始前に接続を再利用不可とし、応答一式の正常受信後だけ解除する。待機取消時に残った ACK / Result を次の要求へ誤対応させない。

Health / Hello / Snapshot / Restore 等の制御交換は5秒、アプリ要求の交換全体は30秒を期限とする。アプリ要求にも書込5秒、Ready の受付 ACK 待ち5秒を適用し、5秒処理するハンドラーの結果待ちと区別する。配送先・状態境界への受付待ちと世代ごとの IPC ロック待ちはそれぞれ別枠で30秒。これらは待機時間を含むエンドツーエンド30秒や Discord の応答期限を保証するものではない。

architecture §8 の u32 big-endian length prefix + MessagePack / 初期 8 MiB は設計案であり、現行 Worker の実フレームではない。`protocol.rs` の Envelope/Control/Data も現行 Worker の wire envelope ではない。JSON の独立したネスト深度制限はない。Ready の保持容量・受付制限は末尾の event-seq-v1 を参照。

## 起動と互換性

1. 親は `{"type":"health"}` を送る。
2. 子は `{"type":"ready","protocol":5,"protocol_min":5,"protocol_max":5,"required_capabilities":["snapshot-v3"],"supported_capabilities":["hello-v1","command-dispatch-v1","ready-event-v1","slash-arguments-v1"],"build":"...","pid":123}` を返す。
3. 親は protocol=5、範囲と必須 capability を検査する。min/max 両方省略の旧 v5 Ready は受理し、片方のみは拒否する。
4. `hello-v1` 広告時だけ、親は `{"type":"hello","hello":{...}}` を送り、子も同じ形の Hello を返す。広告のない旧 Worker には Hello を送らず、旧 capability 動作を維持する。
5. 新 Worker は `concurrent-drain-v1` も任意 capability として Ready に広告する。親は明示広告と双方向 Hello の合意を確認し、交渉済みなら初期 Snapshot を取得してからアプリ要求を許可する。上記 JSON は既存フィールドの例であり、全 capability の列挙ではない。

Hello 本体の実例:

```json
{"protocol_min":5,"protocol_max":5,"identity":{"bot_id":0,"shard":{"id":0,"total":1},"generation_id":123,"ownership_epoch":0},"required_capabilities":["snapshot-v3"],"optional_capabilities":["hello-v1","command-dispatch-v1","ready-event-v1","slash-arguments-v1"]}
```

双方で範囲の妥当性・共通範囲、Bot/Shard（total を含む）/generation_id/ownership_epoch の一致、相互の必須 capability の対応を検査する。共通範囲の最大バージョンを選び、必須・任意の広告の共通集合をソート・重複排除して使用する。未知の必須は拒否、未知の任意は無視する。現行 Worker の実装範囲は v5 のみ。子の拒否は `{"type":"rejected","reason":"..."}`。親は不一致・拒否・不正応答・timeout で候補を破棄し、既存の差替え処理は旧 Worker を保持する。

親が生成した接続 identity を子専用環境変数 `BOT_WORKER_IPC_IDENTITY` に渡し、子はこれを期待値として照合する。`BOT_ID` / `SHARD_ID` / `TOTAL_SHARDS` を Manager と Gateway に反映し、未指定は Bot=0、Shard=0/1。`GENERATION_ID` 指定時は spawn ごとに増加、未指定は乱数。epoch=0 のままで、配置計画・ownership / lease による起動権限とは未接続。これは認証や lease の証明ではなく、遠隔接続にも使用しない。旧親との互換のため子は Hello を必須化せず、Health と既存要求を受け付ける。identity 未設定時の Hello は拒否する。

## 現行要求と応答

| 要求 type / フィールド | 応答 |
|---|---|
| health | ready（上記） |
| hello / hello | hello / hello、または rejected / reason |
| dispatch / key:u64, content:string | result / content:string または null, state:JSON |
| command / name:string | result |
| slash_command / name:string, args:名前と SlashValue のペア配列 | result |
| ready_event / bot_name:string | result |
| sequenced_ready / local_sequence:u64, bot_name:string | event_accepted / local_sequence、続いて result または rejected |
| restore_event_stream / next_sequence:u64 または null | event_stream_restored、または rejected（下記の capability・初期化条件あり） |
| tick | result |
| snapshot | snapshot / version:u32, state:JSON |
| restore / version:u32, state:JSON | restored、または rejected / reason |

アプリ処理失敗も rejected を返す。SlashValue は framework の serde 型に従う。snapshot の状態形式は現在 version=3。request_id、timestamp、drain/switch 制御メッセージ、MessagePack 分割転送はこの Worker wire にはない。プロセスの起動世代とアプリ状態 version は別の概念。

## 外部応答結果の帰属と環境

`api-result-v1` は Ready の明示広告と Hello の双方での交渉を要する。`response_dispatch` は `operation_id` と dispatch / command / slash_command を送り、応答内容のある結果に対して Worker が未決操作を登録する。Gateway は外部応答後、同じ世代へ `api_result` を送り、Worker は operation_id、outcome、identity、pid を含む `api_result_accepted` を返す。親は期待値を照合する。未決枠は最大1024。これは Discord 側の副作用の exactly-once や結果不明の解消を保証しない。`src/src/worker.rs`、`src/tests/worker_response_generation.rs` と模擬 Discord を使う `src/tests/e2e_shard_worker.rs` を参照。実 Discord は未検証。

Ready の `credential_free` は Worker 自身による環境変数名の検査結果であり、秘密の不在を証明する認証ではない。isolated 起動では環境を消去し identity を渡す（Windows の `SystemRoot` を除く）。通常の Worker 起動でも `DISCORD_TOKEN` を除外するが、全起動方式で別名の秘密を除外する保証はない。Gateway は Worker の親であり、Worker 更新は Gateway 自体の更新ではない。Gateway の終了は Worker にも影響するため、Gateway 再起動を跨ぐ接続維持は主張しない。

## TaskManager と世代ごとの drain

Worker の dispatch / command / slash_command / Ready / tick は `WorkerTasks` adapter から `TaskManager` に登録する。1要求の親とハンドラーを実行する子は両方 `RequestScoped` で、Bot / Shard / ローカル単調 request_id / parent TaskId を持つ。本文や token は所有者メタデータへ入れない。子が完了・失敗するまで親を保持し、両 join の回収後に結果を IPC へ返す。終了記録は回収し、登録数は128（親子で64要求）に制限する。実 Worker の要求処理は引き続き直列。

Manager は現在の配送先と、既存処理・外部応答を保持する世代を分離する。候補の起動・Ready / Hello 検査中は旧 Worker を使い続ける。候補の状態復元・イベント引継ぎが完了した時点で新規配送先を切り替え、旧世代の新規応答受付を閉じる。受付済み要求は元のプロセスと IPC を保持し、新世代とは別の IPC で完了する。`replace` の成功は旧世代の処理と外部応答の drain 完了まで待つため、新規要求が新版へ届き始める時点より遅い。

旧世代の処理・応答待ちは60秒を期限とし、満期や結果不明を成功扱いしない。切替後に満期・結果不明・呼出元取消が発生しても、旧世代の登録を残す。既に配送先が新版であれば旧版へ自動で戻さず、後続の更新で残った世代を drain・照合する。各 Worker 枠の登録は Active と保持中の旧世代までとし、未完了の旧世代がある間の追加更新・自動再起動を抑止する。複数 Worker の途中失敗では新旧が混在し、プール全体の rollback は行わない。

`concurrent-drain-v1` の契約は現在の `snapshot-v3` に限定する。snapshot 対象は legacy count / job であり、prefix の `!count` / `!job` 系、Tick、Ready は状態・イベントカーソルの切替境界を保持する。これらの処理が終わるまで状態引継ぎを待つ。他の prefix、command、slash_command は snapshot を変更しない契約で、旧世代の処理中でも新世代へ配送を切り替えられる。FrameworkRuntime / StateMap は従来どおり世代内だけの状態で、引継ぎ対象ではない。独立処理の Result が確認済み snapshot と異なる状態を返した場合は契約違反として接続を破損扱いにし、その状態を採用しない。旧世代の遅い結果で新版の checkpoint を上書きしない。

この契約を未交渉の旧 v5 Worker は全要求の完了を待ってから Snapshot を要求する直列切替となる。未知の任意 capability をそのまま反射する旧 Worker に並行 drain の契約を仮定しない。実際の Snapshot 処理は TaskManager の新規親タスク受付を閉じ、子を含む全 join を待って状態を取得する。候補失敗・取消後も旧 Worker を再利用できるよう、Snapshot 内で drain が終わるとローカル受付を再開する。EOF 時もタスクを drain して最後の flush を行う。

外部応答を伴う呼出しは `begin_response(key)` で `ResponseGuard` を予約し、`dispatch_key_with_response` / `dispatch_slash_with_response` へ同じ guard を渡す。guard は Worker 枠と世代を保持し、実行先の世代と外部応答追跡を一致させる。予約後、実行前に配送先が変わった場合は stale として拒否し、勝手に新版へ再送しない。別 Manager・別 Worker 枠・同じ guard の再実行も拒否する。外部応答が確定した場合だけ `confirm` し、未確認の Drop は結果不明として旧世代も含め照会・照合対象に残る。旧来の guard を渡さない dispatch API と組み合わせても、応答先世代の一致は保証しない。

新しい drain wire 要求は追加しない。ローカル request_id も現行 wire のフィールドではない。TaskManager の取消要求とタスク終了は別であり、adapter の待機取消でも join を保持する。任意の Cog / Context::spawn への公開接続、並列コマンド処理、durable job / 全資源の drain、外部副作用の確定をこの接続だけで保証しない。

## event-seq-v1 と event-replay-v1（Ready 配送）

双方の Hello で `event-seq-v1` を交渉した接続だけが
`{"type":"sequenced_ready","local_sequence":0,"bot_name":"..."}` を使う。
Health の広告だけでは有効にしない。未交渉の旧 Worker へは `ready_event` を送り、ACK なしの1結果を受け取る。新 Worker は任意 capability として `event-seq-v1` と `event-replay-v1` を広告する。親は replay を Ready で明示広告した相手にだけ Hello でも提案し、双方の Hello での交渉をさらに必要とする。未知の optional を反射する旧 v5 相手へ新しい wire 要求を送らないための保守的な互換制約。Hello の実例にある他の capability は省略例であり、必須化していない。

新しいストリームの連番は0から開始し、Discord WS の sequence と区別する。子の `event_seq::Receiver` が順序を検査し、直列の実行枠（容量1）へ受け付け、ハンドラー開始前に `{"type":"event_accepted","local_sequence":0}` を flush する。後続 result / rejected が処理結果であり、受付 ACK は処理完了・永続化の証明ではない。未交渉の sequenced_ready、欠落、重複は ACK せず接続を終了する。

親の Sender は64イベント / payload 合計1 MiBを上限に保持する。一致する単一イベントの ACK で解放する。満杯・不正 ACK・ACK 欠落・5秒 timeout を分類し、ログには未 ACK 範囲と byte 数だけを出す。直列要求では通常1イベント以下が未 ACK となる。process ロックを待つ呼出元タスクや OS pipe の容量はこの上限に含まれない。ACK 後は handler 失敗・結果喪失でも再配送しない。

旧・新 Worker の両接続で **event-seq-v1 と event-replay-v1 が交渉済みの場合だけ**、明示更新・自動再起動で Sender と連番を引き継ぐ。

1. 同一 Worker の routing と状態・ACK の切替境界を保持する。並行 drain 対応世代は確認済み checkpoint と Sender の一貫したコピーを使い、未対応世代は既存処理後に Snapshot を要求する。候補へ状態を restore し、破損接続の復旧では最後に確認できた checkpoint（なければ初期状態）だけを使う。
2. 候補へ `{"type":"restore_event_stream","next_sequence":N}` を送り、`{"type":"event_stream_restored"}` を確認する。N は Sender の連続 ACK prefix の直後で、初期値0。u64 を使い切った場合の null は終了済みカーソルを意味し、0に戻さない。
3. 候補は受信カーソルを最初のイベントより前に一度だけ設定できる。未交渉・再設定は rejected。引継ぎ済み範囲の再送や受付後の重複は Receiver が拒否する。
4. Sender を候補へ複製し、未 ACK payload だけを元の local_sequence で順番に配送する。再 enqueue や番号の振り直しはしない。ACK と処理結果を確認してから候補を設置する。
5. 候補の restore / replay が失敗・取消されたら旧 Sender を残す。必要な capability が片側にない状態で未 ACK を抱える場合も更新を拒否して保持する。未 ACK がない旧 Worker との更新は従来互換を維持し、新しい接続の連番を開始できる。

復旧中の再配送が成功しても、元の要求の失敗は呼出元へそのまま返す。自動的な業務コマンドの再試行は行わない。ACK 喪失前に旧 Worker が実行した処理の副作用は、新 Worker 側で再び起こり得る。受信側の重複拒否は引継ぎ済みカーソルと同じ生存ストリーム内に限定され、ACK 未確認の旧実行の exactly-once を保証しない。確認済み ACK 後に結果が喪失した処理も再実行せず、アプリ状態は最後に確認できた snapshot までの復旧となる。親停止でバッファを失い、Ready 以外の dispatch / command / REST は対象外。全経路の at-least-once、無欠落、グローバル順序、永続的重複排除は保証しない。

検証は `event_seq_ipc.rs` / `worker_replay.rs` の実プロセスと故障注入 fixture、`worker_tasks.rs` の制御可能な Future を使用する。ACK / result 順序、欠落・重複拒否、ACK 喪失後の切替、候補 replay 失敗時の保持、両側 capability 不足拒否、連続更新のカーソル、結果喪失後の非再配送、通信待機取消からの切替、親子追跡・drain・取消・パニック・容量回収を確認する。Linux、実 Discord、プロセス suspend、親/ホスト強制停止、外部副作用の統合検証は未実施。

## 診断と切替後の障害記録

Worker の診断はコードで定義した固定原因、IO の種別・OSコード、JSON の分類・行列だけを公開し、任意の Error 本文や source をそのまま整形しない。Worker の Rejected は既知の固定理由だけを採用し、未知の理由本文は `[REDACTED]` に置き換える。秘密らしい文字列を検索して消す処理だけに依存せず、通常の `worker exited` やプロトコル・capability 不一致を識別できるようにする。

`watch` は管理パスを登録し、更新失敗と実行中の接続障害を `.status.error` の JSON に書く。共通フィールドは要求成果物を照合する `path`、保護済み診断の `error`、`phase`。runtime 障害は Worker 番号・PID・`outcome: "unknown"`・復旧結果を付け、取得できた終了コードも記録する。`path` は既存 reload の完全一致用制御フィールドであり、ログ本文として表示しない。記録は障害処理中に行い、次の監視周期まで延期しない。

クラッシュ時は最後の確認済み checkpoint から新版の再起動を試みる。失敗した業務要求は再実行せず、旧版への自動切戻しも行わない。既に切替済みの旧世代の障害によって現在の Active を置換しない。同じ成果物を指す監視周期では記録を消さず、別成果物への成功した更新でも更新中に発生した新しい障害記録は消さない。管理ファイルは直近の診断用であり、永続的な操作監査・全事故履歴・結果不明操作の永続ストアではない。ファイル書込失敗は stderr に診断するが、保存を保証しない。自発クラッシュの常時監視はなく、IPC 要求で検出する。

`hot_swap_scenarios.rs` の T04 / T05 / T06 / T19 は実 Worker に対する relay の遅延・切断・互換性故障を使う。T04 は relay の5秒遅延に加え、`hot_swap_handler.rs` に登録した実際の5秒 async ハンドラーを production `worker::run` / WorkerTasks で動かし、旧ハンドラーの完了前に新 Worker が応答することを検証する。旧外部応答の確認待ち、更新待機の取消、旧結果不明の照合、状態を変える count の切替境界も試験する。Shard Core 全資源の drain、実 Discord REST 結果の旧 Core 帰属は未検証。最終的な実行結果は統合検証の記録を参照する。

並行 drain 追加前の統合検証記録：2026-11-12、Windows / Rust 1.99.0 で `cargo test --offline --workspace` は293件成功（既存269件と新規24件、doc-testを含む）。既存の compile-fail・管理・release reload・更新・旧 v5 互換試験を含む。`CARGO_NET_OFFLINE=true` で子 cargo もオフライン化した。workspace 全 target の clippy（`--offline` / 警告禁止）、fmt check、差分の空白検査も成功。初回の全体試験で検出した旧 v5 の optional 反射との互換問題は、Ready 明示広告の条件を追加し、全体再実行で確認した。この記録は今回の並行 drain 修正後の検証結果ではない。

2026-11-12、今回の最終コードを Windows / Rust 1.99.0 で検証。`CARGO_NET_OFFLINE=true` を設定し、`cargo fmt --all`、`cargo clippy --offline --workspace --all-targets -- -D warnings`、`cargo test --offline --workspace` の組を3回連続で成功させた。各回479件成功・失敗0件。既存の手動ベンチマーク1件はignoreのまま、T04 / T06 のignoreは全て解除し、ホットスワップ15件を各回実行した。候補起動中に旧Workerがクラッシュ・復旧する競合も、切替直前の結果不明再検査・復旧済みPID維持・照合後更新を検証。秘密を含む未知ErrorのDisplay / Debug / source非参照、IO種別とJSON分類の保持、guardの世代不一致・再使用・切替後未実行予約の拒否も確認した。実Discord・Linux・複数ホストの保証は追加しない。
