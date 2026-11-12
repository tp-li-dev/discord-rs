//! Cog とサービスの分割、型付き ban 引数を試すオフライン例。
//! `cd src && cargo run --offline -p bot-framework --example cog_moderation`
//!
//! Discord には接続せず、BAN の計画だけをメモリへ記録する dry-run。
//! UserId 引数、guild/author、権限検証、Gateway 経由の BAN REST は未対応。
//! Context::reply の成功は応答バッファへの追加であり、REST 成功ではない。
//! このサンプルを実際の管理コマンドとして公開しないこと。

#[path = "moderation/cog.rs"]
pub mod moderation;

use bot_framework::{
    arguments::{Arguments, SlashValue},
    dispatch,
    prelude::*,
};
use moderation::{Moderation, RecordingModeration};
use std::sync::{Arc, Mutex};

/// 実際のマクロ登録・Cog 注入・共通ディスパッチを模擬入力で呼ぶ。
pub async fn demo() -> BotResult<Vec<String>> {
    let service = Arc::new(RecordingModeration::default());
    let mut states = StateMap::new();
    let mut cogs = CogSet::new();
    cogs.add(&mut states, Moderation::new(service.clone()))
        .await?;
    let replies = Mutex::new(Vec::new());

    // prefix は空白区切りのみ。引用・escape・複数語の理由は未対応。
    let prefix = Mutex::new(Arguments::prefix("123456789012345678 spam")?);
    dispatch(
        "ban",
        Context::with_state(&replies, &states).with_arguments(&prefix),
    )
    .await?;
    // Slash の文字列 option は複数語をそのまま渡せる。
    let slash = Mutex::new(Arguments::slash([
        (
            "user".into(),
            SlashValue::String("123456789012345678".into()),
        ),
        (
            "reason".into(),
            SlashValue::String("repeated spam messages".into()),
        ),
    ])?);
    dispatch(
        "ban",
        Context::with_state(&replies, &states).with_arguments(&slash),
    )
    .await?;
    assert_eq!(service.requests()?.len(), 2);

    // ここでは入力が有限で全 handler を await 済み。実ホストは受付停止と
    // 実行中 handler/task の終了待ちを済ませてから remove する必要がある。
    cogs.remove::<Moderation>(&mut states).await?;
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
