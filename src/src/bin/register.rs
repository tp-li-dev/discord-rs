//! Explicit, one-command registration. The default is a dry run.
use bot_framework::arguments::{ArgumentKind, ArgumentSchema};
use src::redact::{Secret, error_diagnostic, redact};
use std::{env, num::NonZeroU64};
use twilight_http::Client;
use twilight_model::application::command::{CommandOption, CommandOptionType};
use twilight_model::id::{
    Id,
    marker::{ApplicationMarker, GuildMarker},
};

#[derive(PartialEq)]
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

#[derive(Debug)]
struct RegistrationFailure(&'static str);
impl std::fmt::Display for RegistrationFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.0)
    }
}
impl std::error::Error for RegistrationFailure {}
fn failure(message: &'static str) -> src::AppError {
    Box::new(RegistrationFailure(message))
}
fn diagnostic(error: &(dyn std::error::Error + 'static)) -> String {
    error
        .downcast_ref::<RegistrationFailure>()
        .map_or_else(|| error_diagnostic(error), |e| e.0.to_owned())
}

fn main() {
    src::redact::install_panic_hook();
    let result = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|error| Box::new(error) as src::AppError)
        .and_then(|runtime| runtime.block_on(run()));
    if let Err(error) = result {
        eprintln!("register: {}", diagnostic(error.as_ref()));
        std::process::exit(1);
    }
}

async fn run() -> Result<(), src::AppError> {
    let args = env::args_os()
        .skip(1)
        .map(|arg| {
            arg.into_string()
                .map_err(|_| failure("arguments are not Unicode [argument_encoding]"))
        })
        .collect::<Result<Vec<_>, _>>()?;
    let plan = parse(&args).map_err(failure)?;
    let commands = src::registered_commands()
        .map_err(|_| failure("linked command registry is invalid [registry_invalid]"))?;
    let command = commands
        .iter()
        .find(|cmd| cmd.name == plan.name)
        .ok_or_else(|| failure("unknown linked command"))?;
    if command.description.is_empty() || command.description.chars().count() > 100 {
        return Err(failure("command description must contain 1–100 characters"));
    }
    println!(
        "command={} description={} scope={}",
        redact(command.name),
        redact(command.description),
        plan.guild
            .map_or_else(|| "global".to_owned(), |guild| format!("guild:{guild}"))
    );
    let options = discord_options(command.arguments);
    println!("options={}", redact(&serde_json::to_string(&options)?));
    println!(
        "Only this command name is upserted; an existing command with the same name and scope is overwritten."
    );
    if !plan.apply {
        println!("Dry run: no request sent. Pass --apply to register.");
        return Ok(());
    }
    let token = Secret::new(env::var("DISCORD_TOKEN").map_err(|error| {
        failure(match error {
            env::VarError::NotPresent => "DISCORD_TOKEN is not configured [token_missing]",
            env::VarError::NotUnicode(_) => "DISCORD_TOKEN is not Unicode [token_encoding]",
        })
    })?);
    let _ = rustls::crypto::ring::default_provider().install_default();
    let client = Client::new(token.into_inner());
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
    await_registration(request, std::time::Duration::from_secs(10))
        .await
        .map_err(failure)?;
    println!("Command registered.");
    Ok(())
}

impl std::fmt::Debug for Plan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Plan")
            .field("app", &self.app)
            .field("guild", &self.guild)
            .field("name", &Secret::new(&self.name))
            .field("apply", &self.apply)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn strings(values: &[&str]) -> Vec<String> {
        values.iter().map(|s| s.to_string()).collect()
    }
    #[test]
    #[ignore = "subprocess fixture"]
    fn secret_error_fixture() {
        let Some(input) = env::var_os("REGISTER_ERROR_FIXTURE") else {
            return;
        };
        src::redact::install_panic_hook();
        let error = std::io::Error::other(input.to_string_lossy().to_string());
        eprintln!("register: {}", diagnostic(&error));
        let plan = parse(&strings(&["123", "global", &input.to_string_lossy()])).unwrap();
        eprintln!("{plan:?}");
        std::process::exit(1);
    }

    #[test]
    fn process_error_and_plan_debug_keep_classification_without_payloads() {
        let output = std::process::Command::new(env::current_exe().unwrap())
            .args(["--exact", "tests::secret_error_fixture", "--ignored", "--nocapture"])
            .env("REGISTER_ERROR_FIXTURE", "token=fake-token secret=fake-secret private_key=fake-key Authorization: Bearer fake-auth unlabelled-value")
            .output().unwrap();
        assert_eq!(output.status.code(), Some(1));
        let text = format!(
            "{}{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(text.contains("Other"));
        assert!(text.contains("[REDACTED]"));
        for secret in [
            "fake-token",
            "fake-secret",
            "fake-key",
            "fake-auth",
            "unlabelled-value",
        ] {
            assert!(!text.contains(secret));
        }
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
