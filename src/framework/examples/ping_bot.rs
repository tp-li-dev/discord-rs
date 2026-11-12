//! Cog なしの最小ハイブリッドコマンド。src/ から実行:
//! `cargo run --offline -p bot-framework --example ping_bot`
//!
//! Discord 接続・コマンド登録・token は不要。ここでは prefix/slash 相当の
//! 模擬入力を同じハンドラーへ渡す。reply はローカルバッファへの追加であり、
//! Discord REST の送信成功を意味しない。
use bot_framework::{arguments::Arguments, dispatch, prelude::*};
use std::sync::Mutex;

#[hybrid_command(name = "ping", description = "Bot の応答を確認")]
async fn ping(ctx: Context<'_>) -> BotResult<()> {
    ctx.reply("Pong!").await
}

pub async fn demo() -> BotResult<Vec<String>> {
    let replies = Mutex::new(Vec::new());
    // Gateway アダプターが !ping から名前と引数を切り出した後の入力。
    let prefix = Mutex::new(Arguments::prefix("")?);
    dispatch("ping", Context::new(&replies).with_arguments(&prefix)).await?;
    // /ping の名前と型付き option が既に抽出された入力。
    let slash = Mutex::new(Arguments::slash([])?);
    dispatch("ping", Context::new(&replies).with_arguments(&slash)).await?;
    replies
        .into_inner()
        .map_err(|_| "reply sink poisoned".into())
}

#[cfg(not(test))]
#[tokio::main(flavor = "current_thread")]
async fn main() -> BotResult<()> {
    for reply in demo().await? {
        println!("{reply}");
    }
    Ok(())
}
