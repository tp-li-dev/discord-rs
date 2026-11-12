//! アプリのサービスと Cog。framework 本体に独自の Discord API を追加しない。

use bot_framework::prelude::*;
use std::{
    num::NonZeroU64,
    sync::{Arc, Mutex},
};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BanRequest {
    pub user_id: NonZeroU64,
    pub reason: Option<String>,
}

/// ここで扱うのは BAN 計画のみ。認可や実 REST 処理を代替しない。
pub trait ModerationService: Send + Sync {
    fn plan_ban(&self, request: BanRequest) -> BotResult<()>;
}

/// プロセス内の記録先。再起動・Worker 更新で消え、永続監査にはならない。
#[derive(Default)]
pub struct RecordingModeration {
    requests: Mutex<Vec<BanRequest>>,
}

impl RecordingModeration {
    pub fn requests(&self) -> BotResult<Vec<BanRequest>> {
        Ok(self
            .requests
            .lock()
            .map_err(|_| "plan sink poisoned")?
            .clone())
    }
}

impl ModerationService for RecordingModeration {
    fn plan_ban(&self, request: BanRequest) -> BotResult<()> {
        self.requests
            .lock()
            .map_err(|_| "plan sink poisoned")?
            .push(request);
        Ok(())
    }
}

pub struct Moderation {
    service: Arc<dyn ModerationService>,
}

impl Cog for Moderation {}

#[cog]
impl Moderation {
    pub fn new(service: Arc<dyn ModerationService>) -> Self {
        Self { service }
    }

    #[hybrid_command(name = "ban", description = "BAN 計画を記録する dry-run")]
    async fn ban(&self, ctx: Context<'_>, user: String, reason: Option<String>) -> BotResult<()> {
        // 現行マクロに UserId 変換はない。i64 では u64 全域を表現できない
        // ため String を受け取り、符号・非 ASCII 数字・0・overflow を拒否。
        if user.is_empty() || !user.bytes().all(|byte| byte.is_ascii_digit()) {
            return Err("user must be a nonzero decimal u64".into());
        }
        let user_id = user
            .parse::<NonZeroU64>()
            .map_err(|_| "user must be a nonzero decimal u64")?;
        self.service.plan_ban(BanRequest { user_id, reason })?;
        // 入力された理由を返信やログへ複製しない。
        ctx.reply(format!(
            "dry-run: BAN plan recorded for {user_id}; no Discord request sent"
        ))
        .await
    }
}
