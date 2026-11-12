//! Explicit, one-command registration. The default is a dry run.
use bot_framework::arguments::{ArgumentKind, ArgumentSchema};
use std::{env, num::NonZeroU64};
use twilight_http::Client;
use twilight_model::application::command::{CommandOption, CommandOptionType};
use twilight_model::id::{
    Id,
    marker::{ApplicationMarker, GuildMarker},
};

#[derive(Debug, PartialEq)]
struct Plan {
    app: NonZeroU64,
    guild: Option<NonZeroU64>,
    name: String,
    apply: bool,
}

fn parse_id(value: &str) -> Result<NonZeroU64, &'static str> {
    value
        .parse::<NonZeroU64>()
        .map_err(|_| "ID must be a nonzero integer")
}
fn parse(args: &[String]) -> Result<Plan, &'static str> {
    if !(args.len() == 3 || (args.len() == 4 && args[3] == "--apply")) {
        return Err("usage: register <application-id> <global|guild-id> <command-name> [--apply]");
    }
    Ok(Plan {
        app: parse_id(&args[0])?,
        guild: if args[1] == "global" {
            None
        } else {
            Some(parse_id(&args[1])?)
        },
        name: args[2].clone(),
        apply: args.len() == 4,
    })
}

// A timeout can occur after Discord accepted the request: never retry automatically.
async fn await_registration<F, E>(
    request: F,
    deadline: std::time::Duration,
) -> Result<(), &'static str>
where
    F: std::future::Future<Output = Result<(), E>>,
{
    match tokio::time::timeout(deadline, request).await {
        Ok(Ok(())) => Ok(()),
        Ok(Err(_)) => Err("Discord command registration failed; outcome may be unknown"),
        Err(_) => Err("Discord command registration timed out; outcome unknown"),
    }
}

fn discord_options(args: &[ArgumentSchema]) -> Vec<CommandOption> {
    args.iter()
        .map(|arg| CommandOption {
            name: arg.name.to_owned(),
            description: format!("Argument {}", arg.name),
            kind: match arg.kind {
                ArgumentKind::String => CommandOptionType::String,
                ArgumentKind::Integer => CommandOptionType::Integer,
                ArgumentKind::Boolean => CommandOptionType::Boolean,
            },
            required: Some(arg.required),
            autocomplete: None,
            channel_types: None,
            choices: None,
            description_localizations: None,
            max_length: None,
            max_value: None,
            min_length: None,
            min_value: None,
            name_localizations: None,
            options: None,
            file_types: None,
        })
        .collect()
}

#[tokio::main]
async fn main() -> Result<(), src::AppError> {
    let plan = parse(&env::args().skip(1).collect::<Vec<_>>())?;
    let commands = src::registered_commands()?;
    let command = commands
        .iter()
        .find(|cmd| cmd.name == plan.name)
        .ok_or("unknown linked command")?;
    if command.description.is_empty() || command.description.chars().count() > 100 {
        return Err("command description must contain 1–100 characters".into());
    }
    println!(
        "command={} description={} scope={}",
        command.name,
        command.description,
        plan.guild
            .map_or_else(|| "global".to_owned(), |guild| format!("guild:{guild}"))
    );
    let options = discord_options(command.arguments);
    println!("options={}", serde_json::to_string(&options)?);
    println!(
        "Only this command name is upserted; an existing command with the same name and scope is overwritten."
    );
    if !plan.apply {
        println!("Dry run: no request sent. Pass --apply to register.");
        return Ok(());
    }
    let token = env::var("DISCORD_TOKEN")?;
    let _ = rustls::crypto::ring::default_provider().install_default();
    let client = Client::new(token);
    let application = Id::<ApplicationMarker>::new(plan.app.get());
    let commands = client.interaction(application);
    let request = async {
        match plan.guild {
            Some(guild) => commands
                .create_guild_command(Id::<GuildMarker>::new(guild.get()))
                .chat_input(command.name, command.description)
                .command_options(&options)
                .await
                .map(|_| ()),
            None => commands
                .create_global_command()
                .chat_input(command.name, command.description)
                .command_options(&options)
                .await
                .map(|_| ()),
        }
    };
    // Do not print HTTP errors: they may contain request details.
    await_registration(request, std::time::Duration::from_secs(10)).await?;
    println!("Command registered.");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    fn strings(values: &[&str]) -> Vec<String> {
        values.iter().map(|s| s.to_string()).collect()
    }
    #[tokio::test]
    async fn registration_timeout_is_reported_as_unknown_without_error_details() {
        let result = await_registration(
            std::future::pending::<Result<(), &'static str>>(),
            std::time::Duration::from_millis(10),
        )
        .await;
        assert_eq!(
            result,
            Err("Discord command registration timed out; outcome unknown")
        );
        let result = await_registration(
            async { Err::<(), _>("secret") },
            std::time::Duration::from_secs(1),
        )
        .await;
        assert!(!result.unwrap_err().contains("secret"));
    }

    #[test]
    fn validates_scope_ids_and_explicit_apply() {
        let plan = parse(&strings(&["123", "global", "ping"])).unwrap();
        assert!(!plan.apply && plan.guild.is_none());
        assert!(
            parse(&strings(&["123", "456", "ping", "--apply"]))
                .unwrap()
                .apply
        );
        for args in [
            &["0", "global", "ping"][..],
            &["1", "0", "ping"],
            &["1", "global", "ping", "--unexpected"],
            &["1", "global"],
        ] {
            assert!(parse(&strings(args)).is_err());
        }
    }
}
