//! 型付きハイブリッドコマンドと on_message 相当のホスト側処理。
//! src/ から `cargo run --offline -p bot-framework --example hybrid_bot`。
//!
//! 未対応: #[event] による MessageCreate 登録、Gateway からのメッセージ配送、
//! author/guild/権限、REST 送信。on_message は通常のホスト関数であり、
//! framework のイベント API を装うものではない。実 prefix 利用には
//! Message Content Intent が必要。引用・escape は現在の引数 API では非対応。
use bot_framework::{
    arguments::{Arguments, SlashValue},
    dispatch,
    prelude::*,
};
use std::sync::Mutex;

#[hybrid_command(name = "greet", description = "型付き引数で挨拶")]
async fn greet(
    ctx: Context<'_>,
    name: String,
    enthusiastic: bool,
    salutation: Option<String>,
) -> BotResult<()> {
    let salutation = salutation.as_deref().unwrap_or("Hello");
    let punctuation = if enthusiastic { "!" } else { "." };
    ctx.reply(format!("{salutation}, {name}{punctuation}"))
        .await
}

/// 模擬メッセージに対する処理。実アダプターが bot 判定を提供する必要がある。
/// bot を無視し、通常メッセージへの反応とコマンド配送を両立する。
pub async fn on_message(
    content: &str,
    author_is_bot: bool,
    replies: &Mutex<Vec<String>>,
) -> BotResult<bool> {
    if author_is_bot {
        return Ok(false);
    }
    if content == "hello" {
        Context::new(replies)
            .reply("Hello from on_message!")
            .await?;
        return Ok(true);
    }
    let Some(command) = content.strip_prefix('!') else {
        return Ok(false);
    };
    let split = command.find(char::is_whitespace).unwrap_or(command.len());
    let (name, input) = command.split_at(split);
    if name.is_empty() {
        return Ok(false);
    }
    let arguments = Mutex::new(Arguments::prefix(input)?);
    dispatch(name, Context::new(replies).with_arguments(&arguments)).await
}

/// Discord の Interaction から名前と option を抽出した後の模擬入力。
/// Discord 登録や Interaction の初回応答期限はこのローカル例では検証しない。
pub async fn on_slash(
    name: &str,
    options: impl IntoIterator<Item = (String, SlashValue)>,
    replies: &Mutex<Vec<String>>,
) -> BotResult<bool> {
    let arguments = Mutex::new(Arguments::slash(options)?);
    dispatch(name, Context::new(replies).with_arguments(&arguments)).await
}

pub async fn demo() -> BotResult<Vec<String>> {
    let replies = Mutex::new(Vec::new());
    on_message("hello", false, &replies).await?;
    on_message("!greet Ada true", false, &replies).await?;
    on_slash(
        "greet",
        [
            ("name".into(), SlashValue::String("Ada".into())),
            ("enthusiastic".into(), SlashValue::Boolean(true)),
        ],
        &replies,
    )
    .await?;
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
