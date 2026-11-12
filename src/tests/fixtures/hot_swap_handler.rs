//! A real async application handler linked to the production Worker runtime.
//! Only test-control marker names are written; payloads and credentials are not.
use bot_framework::prelude::*;
use std::{path::PathBuf, process::ExitCode, time::Duration};

fn marker(suffix: &str) -> std::io::Result<PathBuf> {
    let mut path = std::env::current_exe()?.into_os_string();
    path.push(suffix);
    Ok(path.into())
}

#[hybrid_command(name = "five_second", description = "Fixture async handler")]
async fn five_second(ctx: Context<'_>) -> BotResult<()> {
    std::fs::write(marker(".started")?, b"started")?;
    let gate = marker(".release")?;
    tokio::time::timeout(Duration::from_secs(15), async {
        while !gate.exists() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await?;
    tokio::time::sleep(Duration::from_secs(5)).await;
    ctx.reply("five-second handler completed").await?;
    Ok(())
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> ExitCode {
    match src::worker::run().await {
        Ok(()) => ExitCode::SUCCESS,
        Err(_) => {
            eprintln!("fixture application worker failed");
            ExitCode::FAILURE
        }
    }
}
