mod general;

use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    sync::{Mutex, atomic::AtomicUsize},
};

/// Framework services live for one Worker process. They are not in the snapshot.
pub struct FrameworkRuntime {
    states: bot_framework::StateMap,
}
impl Default for FrameworkRuntime {
    fn default() -> Self {
        let mut states = bot_framework::StateMap::new();
        states.insert(FrameworkCounter(AtomicUsize::new(0)));
        Self { states }
    }
}
pub struct FrameworkCounter(pub AtomicUsize);

pub async fn dispatch_ready(
    runtime: &FrameworkRuntime,
    bot_name: String,
) -> Result<(), crate::AppError> {
    let logger = |message: &str| eprintln!("{message}");
    bot_framework::dispatch_ready(
        bot_framework::EventContext::with_state(&logger, &runtime.states),
        bot_framework::ReadyEvent { bot_name },
    )
    .await
}

#[derive(Clone, Default, Serialize, Deserialize)]
pub struct State {
    pub counts: BTreeMap<u64, u64>,
    #[serde(default)]
    pub jobs: BTreeMap<u64, Job>,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct Job {
    pub step: u64,
    pub total: u64,
    pub running: bool,
}

pub fn tick(state: &mut State) {
    for job in state.jobs.values_mut() {
        if job.running {
            job.step = job.step.saturating_add(1);
            job.total = job.total.saturating_add(job.step);
        }
    }
}

pub async fn dispatch(
    key: u64,
    content: &str,
    state: &mut State,
    runtime: &FrameworkRuntime,
) -> Result<Option<String>, crate::AppError> {
    let content = content.trim();
    if let Some(reply) = general::dispatch(key, content, state) {
        return Ok(Some(reply));
    }
    let Some(input) = content.strip_prefix('!') else {
        return Ok(None);
    };
    let end = input.find(char::is_whitespace).unwrap_or(input.len());
    let name = &input[..end];
    if name.is_empty()
        || !bot_framework::commands()?
            .iter()
            .any(|command| command.name == name)
    {
        return Ok(None);
    }
    let arguments = Mutex::new(bot_framework::arguments::Arguments::prefix(&input[end..])?);
    let replies = Mutex::new(Vec::new());
    let result = bot_framework::dispatch(
        name,
        bot_framework::Context::with_state(&replies, &runtime.states).with_arguments(&arguments),
    )
    .await;
    // Preserve the existing treatment of surplus prefix text as an unrecognized command.
    if let Err(error) = result {
        if matches!(
            error.downcast_ref::<bot_framework::arguments::ArgumentError>(),
            Some(bot_framework::arguments::ArgumentError::Unexpected(_))
        ) {
            return Ok(None);
        }
        return Err(error);
    }
    let replies = replies.into_inner().map_err(|_| "reply sink poisoned")?;
    Ok((!replies.is_empty()).then(|| replies.join("\n")))
}

pub async fn dispatch_command(
    name: &str,
    runtime: &FrameworkRuntime,
) -> Result<Option<String>, crate::AppError> {
    let replies = Mutex::new(Vec::new());
    if !bot_framework::dispatch(
        name,
        bot_framework::Context::with_state(&replies, &runtime.states),
    )
    .await?
    {
        return Ok(None);
    }
    let replies = replies.into_inner().map_err(|_| "reply sink poisoned")?;
    Ok((!replies.is_empty()).then(|| replies.join("\n")))
}

pub async fn dispatch_slash(
    name: &str,
    args: Vec<(String, bot_framework::arguments::SlashValue)>,
    runtime: &FrameworkRuntime,
) -> Result<Option<String>, crate::AppError> {
    let arguments = Mutex::new(bot_framework::arguments::Arguments::slash(args)?);
    let replies = Mutex::new(Vec::new());
    if !bot_framework::dispatch(
        name,
        bot_framework::Context::with_state(&replies, &runtime.states).with_arguments(&arguments),
    )
    .await?
    {
        return Ok(None);
    }
    let replies = replies.into_inner().map_err(|_| "reply sink poisoned")?;
    Ok((!replies.is_empty()).then(|| replies.join("\n")))
}

pub fn build_id() -> &'static str {
    general::BUILD_ID
}

#[cfg(test)]
mod tests {
    use super::*;
    use bot_framework::prelude::*;

    #[hybrid_command(name = "count")]
    async fn collision(ctx: Context<'_>) -> BotResult<()> {
        ctx.reply("hybrid count").await
    }

    #[tokio::test]
    async fn explicit_command_bypasses_legacy_prefix_collision() {
        let mut state = State::default();
        let runtime = FrameworkRuntime::default();
        assert_eq!(
            dispatch(1, "!count", &mut state, &runtime)
                .await
                .unwrap()
                .as_deref(),
            Some("カウント: 1")
        );
        assert_eq!(
            dispatch_command("count", &runtime)
                .await
                .unwrap()
                .as_deref(),
            Some("hybrid count")
        );
        assert_eq!(state.counts[&1], 1);
    }
}
