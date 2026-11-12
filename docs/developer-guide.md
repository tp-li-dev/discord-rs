# 開発者ガイド（試作段階）

仕様の目標と現在の実装は異なります。基準は [architecture.md](architecture.md)、検証済み範囲と残作業は [progress.md](progress.md) を参照してください。現在は単一 Shard の Gateway と複数 Worker です。

## コマンド

`src/framework` の実験的 API では、リンクされた属性付き関数を `inventory` で収集します。既存の実例は `src/src/cogs/general.rs`、ルーティングのテストは `src/framework/tests/hybrid.rs` です。

```rust
use bot_framework::prelude::*;

#[hybrid_command(name = "ping", description = "Check response")]
async fn ping(ctx: Context<'_>) -> BotResult<()> {
    ctx.reply("Pong!").await
}
```

モジュールは実際にバイナリにリンクされる必要があります。`commands()` は名前順に収集し、重複名をエラーにします。名前を省略すると関数名、description を省略すると空文字列になります。Discord への登録には空でない 100 文字以内の説明が必要です。関数は安全な非ジェネリック `async fn` で、引数は `Context` 一つのみです。型付き引数、Cog マクロ、汎用引数変換は未実装です。

現在の Bot は `!ping` のような引数なし prefix と、登録済み・オプションなしの ChatInput Interaction を同じ Worker ハンドラーに送ります。通常の prefix 経路では先に既存コマンドが照合されます。Slash の登録は自動ではありません。登録方法と上書きの注意は [operator-guide.md](operator-guide.md) を参照してください。実 Discord での接続・登録・応答は未検証です。

## イベントと状態

`#[event]` は現在 `EventContext` と `ReadyEvent` を受ける関数だけに対応します。リンクされた Ready ハンドラーは名前順に呼ばれ、最初のエラーで処理を止めます。Gateway の Ready は Worker pool の index 0 に配送されます。全 Worker での実行や他のイベントは保証しません。

`StateMap` は具体型ごとにサービスを格納し、`Context::state::<T>()` または `EventContext::state::<T>()` で参照します。未登録型はエラーです。状態は Worker プロセス内にのみ存在し、Worker の入れ替えや再起動で初期化され、snapshot に含まれません。例えば `!state_counter` と `!count` は異なり、後者の明示的なカウンター状態は Worker の snapshot 対象です。永続化や任意の Future の移動は実装されていません。

## 確認

リポジトリの `src` ディレクトリで `cargo test --workspace` を実行します。属性やローカル IPC のテスト成功は、実 Discord、Windows、複数ホスト、性能や切替時の無損失を証明しません。IPC と REST 結果の確定、障害時の副作用や重複排除の設計は引き続き必要です。
