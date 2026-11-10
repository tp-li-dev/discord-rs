# Discord Rust Bot

## 起動

以前のBotを手動で起動している場合は、最初に一度停止してください。コアは自分が起動したBotだけを管理します。PowerShellで `src` フォルダを開き、トークンを環境変数に設定して起動します。

```powershell
cd "D:\data-backup\code\コード保存\discord-rs\src"
$env:DISCORD_TOKEN = 'Botのトークン'
cargo run --bin botctl -- start
```

コマンド実行後は端末に戻り、コアとBotは裏で動き続けます。以後は別の端末からでも同じフォルダで次を実行できます。

```powershell
cargo run --bin botctl -- status
cargo run --bin botctl -- doctor
cargo run --bin botctl -- update
cargo run --bin botctl -- logs
cargo run --bin botctl -- stop
cargo run --bin botctl -- start
cargo run --bin botctl -- restart
cargo run --bin botctl -- rollback
cargo run --bin botctl -- shutdown
```

`stop` はBotだけを止め、コアは維持します。`shutdown` は両方を終了します。`stop`・`restart`・`shutdown` は親を停止するため、メモリ内のジョブや状態は消えます。`restart` は現在の親バイナリを再起動し、コードの再ビルドはしません。`status` はプロセスの生存確認と直近の異常終了を表示しますが、Discordへの接続成功を保証しません。`doctor` は `status` に加えてランタイムディレクトリ、Worker指定ファイル、コアポートを表示します。`rollback` は現時点では未実装です。ログは `src/core-runtime/core.log` に記録されます。Botが異常終了した場合、コアは最大3回自動再起動を試し、60秒の安定稼働後に回数をリセットします。

現在のコアは管理要求を直列で処理するため、`update` のビルド中は `status` などの他の管理要求とBotの異常終了監視が待機します。Botの通常処理自体は別プロセスで続きます。

## 更新

Botを起動したまま、例えば `src/cogs/general.rs` の `!ping` の応答を変更し、`cargo run --bin botctl -- update` を実行します。Workerの更新コマンドを直接使う場合は次を実行します。

```powershell
cd "D:\data-backup\code\コード保存\discord-rs\src"
cargo run --bin reload -- general
```

ビルド、Workerの起動確認、切り替えを行います。「更新しました！」と表示されたらDiscordで `!ping` を試してください。初回ビルドには時間がかかります。`general` は現在利用できる唯一の指定先で、実際には全Workerを順番に更新します。

現在は2つのWorkerを起動し、チャンネルIDから担当を固定します。1つずつ、受付済みの処理を完了させ、そのWorkerへの新しい処理を待たせて旧Workerの状態を新Workerへ引き継ぎます。他のWorkerはその間も動作します。`!count` でチャンネルごとのカウンターを増やし、更新後にも値が続くことを試せます。

### 実行中の仕事を引き継ぐ試験

Discordで `!job start` を送ると、1から順番に数を1ステップずつ足す仕事を開始します。`!job status` は進捗と担当WorkerのビルドID・PIDを表示します。実行中に別のPowerShellから更新コマンドを実行すると、仕事はステップの区切りで新Workerに引き継がれ、続きから進みます。`!job stop` で停止できます。

仕事はチャンネルごとに1つで、各Workerのタイマーが約1秒ごとに1ステップ進めます。更新中は該当Workerのステップが一時的に待機し、他のWorkerは進みます。停止後は `!job start` で新しい仕事を開始できます。

`!workers` は全WorkerのビルドIDとPID、`!version` はそのチャンネルを担当するWorkerのビルドIDとPIDを表示します。初期ビルドは通常 `dev`、更新時は毎回別の `build-...` になります。ビルドIDはビルドの識別子であり、コードに差分があったことや異なる機能を保証しません。PIDはOSのプロセス番号です。

ビルド、起動確認、状態の引き継ぎが失敗した場合、失敗したWorkerは切り替えません。途中まで更新済みのWorkerはそのままで、親は残りの更新を再試行します。その間は新旧Workerが混在するため、バージョン間の互換性が必要です。確認がタイムアウトした場合、更新要求は残っており、後から反映される可能性があります。Bot側のログを確認してください。

## 仕組みと制限

GatewayとDiscord HTTP通信は親プロセス、コマンド処理はWorkerが担当します。親プロセス自体やGateway部分の変更には再起動が必要です。コマンドはWorker単位の切り替えであり、Rustのファイルを直接動的ロードする仕組みではありません。

Workerごとの要求は直列で、親全体の処理待ちは最大64件です。上限を超えたメッセージは破棄され、要求は5秒でタイムアウトします。引き継ぐのは明示的に定義した状態（現在はカウンターと計算ジョブの途中経過）だけで、任意のメモリや実行中の処理を自動移行するものではありません。状態形式を変更する場合は互換性や変換処理の設計が必要です。

Worker障害時は最後に応答を受け取った状態から再起動を試みます。この状態は親のメモリにしかなく、親の停止では失われます。失敗・タイムアウトした要求の結果は不明で、自動再送しません。現在再開できるのは上記の計算ジョブのみで、任意の関数の実行途中や外部API呼び出しを移動するものではありません。仕事に外部への副作用を追加する場合は、重複を防ぐ設計が別途必要です。永続キュー、Discord上の更新コマンドは未実装です。Discordやプロセスの障害まで含む無停止・無損失は保証しません。Worker数の変更時は担当先が変わるため、状態を分配し直す仕組みも必要です。

更新用ファイル `worker-path.txt` と `workers` フォルダは、信頼できる管理者のみが書き込める場所に置いてください。これらへの書き込みは任意のプログラム実行につながります。`core-runtime/core.json` はローカル管理用の認証情報を含みます。Windowsでは親フォルダのアクセス権を継承するため、他のユーザーが読み書きできないようにしてください。`DISCORD_WORKER_FILE` を指定する場合は、コアと更新コマンドの両方に同じ絶対パスを設定してください。コアはローカルのTCPポート39483を使い、別ポートが必要なら `DISCORD_CORE_PORT` を各コマンドで統一してください。
