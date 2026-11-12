//! Link real interaction handlers into the production Worker process.
//! The sole marker contains only the process ID, never interaction/state data.
use bot_framework::{
    BotResult, component,
    interactions::{InteractionContext, InteractionKind, WaitCancelled},
    modal,
};
use std::{future::pending, process::ExitCode};

async fn advance(ctx: InteractionContext<'_>, name: &str) -> BotResult<()> {
    let mut saved = ctx.load_state::<u64>().await?;
    saved.data += 1;
    ctx.save_state(&saved.data, saved.revision, saved.expires_at_unix_ms)
        .await?;
    ctx.reply(format!("{name}:{}:{}", saved.data, ctx.values().join(",")))
        .await
}

#[component(namespace = "app", name = "button", version = 1)]
async fn button(ctx: InteractionContext<'_>) -> BotResult<()> {
    advance(ctx, "button").await
}

#[component(namespace = "app", name = "select", version = 1)]
async fn select(ctx: InteractionContext<'_>) -> BotResult<()> {
    advance(ctx, "select").await
}

#[modal(namespace = "app", name = "form", version = 1)]
async fn form(ctx: InteractionContext<'_>) -> BotResult<()> {
    advance(ctx, "modal").await
}

#[component(namespace = "app", name = "open", version = 1)]
async fn open(ctx: InteractionContext<'_>) -> BotResult<()> {
    let saved = ctx.load_state::<u64>().await?;
    let modal = ctx
        .create_state(
            InteractionKind::Modal,
            "form",
            1,
            &saved.data,
            saved.expires_at_unix_ms,
        )
        .await?;
    // This is application output for a following modal, never a diagnostic.
    ctx.reply(modal.custom_id).await
}

#[component(namespace = "app", name = "wait", version = 1)]
async fn wait(ctx: InteractionContext<'_>) -> BotResult<()> {
    if ctx.values().first().is_some_and(|value| value == "ready") {
        ctx.wait_for(std::future::ready(()))
            .await
            .map_err(|_| "unexpected wait cancellation")?;
        return ctx.reply("wait-ready").await;
    }
    let mut marker = std::env::current_exe()?.into_os_string();
    marker.push(".waiting");
    std::fs::write(
        std::path::PathBuf::from(marker),
        std::process::id().to_string(),
    )?;
    match ctx.wait_for(pending::<()>()).await {
        Err(WaitCancelled::Draining) => {
            // Cancellation must return to the handler, allowing its final CAS
            // and reply to finish on this generation before drain completes.
            let mut saved = ctx.load_state::<u64>().await?;
            saved.data += 1;
            ctx.save_state(&saved.data, saved.revision, saved.expires_at_unix_ms)
                .await?;
            ctx.reply("wait-cancelled").await
        }
        Err(_) => Err("unexpected wait signal closure".into()),
        Ok(()) => unreachable!("pending future never completes"),
    }
}

#[component(namespace = "app", name = "crash", version = 1)]
async fn crash(_ctx: InteractionContext<'_>) -> BotResult<()> {
    std::process::exit(44)
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> ExitCode {
    match src::worker::run().await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!(
                "interaction fixture worker failed: {}",
                src::worker::diagnostic(error.as_ref())
            );
            ExitCode::FAILURE
        }
    }
}
