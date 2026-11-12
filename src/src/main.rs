use src::{AppError, redact::Secret, worker};
use std::{env, ffi::OsStr, future::IntoFuture, process::ExitCode, sync::Arc};
use tokio::sync::Semaphore;
use twilight_gateway::{
    ConfigBuilder, Event, EventTypeFlags, Intents, Shard, ShardId, StreamExt as _,
};
use twilight_gateway_queue::InMemoryQueue;
use twilight_http::Client;
use twilight_model::gateway::SessionStartLimit;
use twilight_model::{
    application::{
        command::CommandType,
        interaction::{
            InteractionData, InteractionType,
            application_command::{CommandDataOption, CommandOptionValue},
        },
    },
    http::interaction::{InteractionResponse, InteractionResponseType},
};

fn configured_shard(deployment: &worker::DeploymentIdentity) -> ShardId {
    ShardId::new(deployment.shard.id(), deployment.shard.total())
}

fn slash_input(
    name: &str,
    kind: CommandType,
    options: &[CommandDataOption],
) -> Option<(String, Vec<(String, bot_framework::arguments::SlashValue)>)> {
    use bot_framework::arguments::SlashValue;
    if kind != CommandType::ChatInput
        || !bot_framework::commands()
            .ok()?
            .iter()
            .any(|command| command.name == name)
    {
        return None;
    }
    let args = options
        .iter()
        .map(|option| {
            let value = match &option.value {
                CommandOptionValue::String(value) => SlashValue::String(value.clone()),
                CommandOptionValue::Integer(value) => SlashValue::Integer(*value),
                CommandOptionValue::Boolean(value) => SlashValue::Boolean(*value),
                _ => return None,
            };
            Some((option.name.clone(), value))
        })
        .collect::<Option<Vec<_>>>()?;
    Some((name.to_owned(), args))
}

// HTTP failures and timeouts can both have an unknown external outcome.
async fn confirmed_response<F, T, E>(deadline: std::time::Duration, request: F) -> bool
where
    F: std::future::Future<Output = Result<T, E>>,
{
    matches!(tokio::time::timeout(deadline, request).await, Ok(Ok(_)))
}

// One configured shard per executable. This queue is process-local, not distributed.
async fn single_shard_queue<F>(
    total_shards: u32,
    metadata: F,
) -> Result<InMemoryQueue, &'static str>
where
    F: std::future::Future<Output = Result<(u32, SessionStartLimit), ()>>,
{
    let (recommended, limits) = tokio::time::timeout(std::time::Duration::from_secs(10), metadata)
        .await
        .map_err(|_| "gateway metadata timed out")?
        .map_err(|_| "gateway metadata unavailable")?;
    if recommended > total_shards {
        return Err("configured shard count is below Discord recommendation");
    }
    let mut registry = src::identify::IdentifyRegistry::default();
    // A private key for the only bot in this process, not a Discord bot ID.
    registry.register(std::num::NonZeroU64::MIN, &limits)
}

// External error text may contain short unlabelled credentials that heuristic
// redaction cannot recognize. Never format that text at this output boundary.
fn failure_message<T>(context: &'static str, error: T) -> String {
    format!("{context}: {}", Secret::new(error))
}

fn token_from_env(value: Result<String, env::VarError>) -> Result<Secret<String>, AppError> {
    match value {
        Ok(value) => Ok(Secret::new(value)),
        Err(env::VarError::NotPresent) => Err("DISCORD_TOKEN is not configured".into()),
        Err(env::VarError::NotUnicode(_)) => Err("DISCORD_TOKEN is not Unicode".into()),
    }
}

#[tokio::main]
async fn main() -> ExitCode {
    match run().await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("{}", failure_message("実行に失敗しました", error));
            ExitCode::FAILURE
        }
    }
}

async fn run() -> Result<(), AppError> {
    if env::args_os().nth(1).as_deref() == Some(OsStr::new("--worker")) {
        return worker::run().await;
    }
    let bootstrap = if env::args_os().nth(1).as_deref() == Some(OsStr::new("--local-control")) {
        Some(
            src::local_control::read_bootstrap()
                .await
                .map_err(|_| "invalid management bootstrap")?,
        )
    } else {
        None
    };
    let _ = rustls::crypto::ring::default_provider().install_default();
    let deployment = worker::DeploymentIdentity::from_env()?;
    let shard_id = configured_shard(&deployment);
    let token = token_from_env(env::var("DISCORD_TOKEN"))?;
    let http = Arc::new(Client::new(token.expose_secret().clone()));
    let queue = single_shard_queue(deployment.shard.total(), async {
        let response = http.gateway().authed().await.map_err(|_| ())?;
        let metadata = response.model().await.map_err(|_| ())?;
        Ok((metadata.shards, metadata.session_start_limit))
    })
    .await?;
    let config = ConfigBuilder::new(
        token.into_inner(),
        Intents::GUILD_MESSAGES | Intents::MESSAGE_CONTENT,
    )
    .queue(queue)
    .build();
    let mut shard = Shard::with_config(shard_id, config);
    let manager =
        worker::Manager::start_pool_with_identity(env::current_exe()?, 2, deployment).await?;
    if let Some(bootstrap) = bootstrap {
        let control_manager = manager.clone();
        tokio::spawn(async move {
            let _ = src::local_control::gateway_session(bootstrap, control_manager).await;
        });
    }
    manager.start_jobs();
    tokio::spawn(manager.clone().watch(worker::worker_file()));
    let capacity = Arc::new(Semaphore::new(64));
    println!("Botを起動しています...");
    while let Some(result) = shard.next_event(EventTypeFlags::all()).await {
        match result {
            Ok(Event::Ready(ready)) => {
                println!("ログイン成功: {}", ready.user.id);
                let Ok(permit) = capacity.clone().try_acquire_owned() else {
                    eprintln!("Ready イベントを処理できません: 混雑中");
                    continue;
                };
                let manager = manager.clone();
                let bot_name = ready.user.name.clone();
                tokio::spawn(async move {
                    let _permit = permit;
                    if let Err(error) = manager.dispatch_ready(bot_name).await {
                        eprintln!(
                            "{}",
                            failure_message("Ready イベントを処理できません", error)
                        );
                    }
                });
            }
            Ok(Event::MessageCreate(message)) if !message.author.bot => {
                let Ok(permit) = capacity.clone().try_acquire_owned() else {
                    eprintln!("Workerが混雑しているためメッセージを処理できませんでした");
                    continue;
                };
                let manager = manager.clone();
                let http = http.clone();
                tokio::spawn(async move {
                    let _permit = permit;
                    if message.content.trim() == "!workers" {
                        let content = manager.describe_workers().await;
                        if !confirmed_response(
                            std::time::Duration::from_secs(10),
                            http.create_message(message.channel_id)
                                .content(&content)
                                .into_future(),
                        )
                        .await
                        {
                            eprintln!("Worker情報の送信エラーまたはタイムアウト（結果不明）");
                        }
                        return;
                    }
                    let response_guard =
                        match manager.begin_response(message.channel_id.get()).await {
                            Ok(guard) => guard,
                            Err(error) => {
                                eprintln!("{}", failure_message("応答追跡を開始できません", error));
                                return;
                            }
                        };
                    match manager
                        .dispatch_key(message.channel_id.get(), message.content.clone())
                        .await
                    {
                        Ok(Some(content)) => {
                            let sent = confirmed_response(
                                std::time::Duration::from_secs(10),
                                http.create_message(message.channel_id)
                                    .content(&content)
                                    .into_future(),
                            )
                            .await;
                            if sent {
                                response_guard.confirm();
                            } else {
                                eprintln!("送信エラーまたはタイムアウト（結果不明）");
                            }
                        }
                        Ok(None) => response_guard.confirm(),
                        Err(error) => {
                            response_guard.confirm();
                            eprintln!("{}", failure_message("Worker error", error));
                        }
                    }
                });
            }
            Ok(Event::InteractionCreate(event))
                if event.kind == InteractionType::ApplicationCommand =>
            {
                let interaction = event.0;
                let Some(InteractionData::ApplicationCommand(ref data)) = interaction.data else {
                    continue;
                };
                let Some((input, args)) = slash_input(&data.name, data.kind, &data.options) else {
                    continue;
                };
                let Some(channel) = interaction.channel.as_ref() else {
                    continue;
                };
                let key = channel.id.get();
                let manager = manager.clone();
                let http = http.clone();
                let Ok(permit) = capacity.clone().try_acquire_owned() else {
                    eprintln!("Interaction を受け付けられません: 混雑中");
                    continue;
                };
                tokio::spawn(async move {
                    let _permit = permit;
                    let response_guard = match manager.begin_response(key).await {
                        Ok(guard) => guard,
                        Err(error) => {
                            eprintln!("{}", failure_message("応答追跡を開始できません", error));
                            return;
                        }
                    };
                    // The identity never enters the Worker or the logs.
                    let identity = (
                        interaction.application_id,
                        interaction.id,
                        Arc::new(Secret::new(interaction.token)),
                    );
                    let ack_http = http.clone();
                    let update_http = http.clone();
                    let outcome = src::interaction_flow::run(
                        identity,
                        std::time::Duration::from_secs(2),
                        std::time::Duration::from_secs(10),
                        move |(application_id, interaction_id, token)| async move {
                            let response = InteractionResponse {
                                kind: InteractionResponseType::DeferredChannelMessageWithSource,
                                data: None,
                            };
                            ack_http
                                .interaction(application_id)
                                .create_response(interaction_id, token.expose_secret(), &response)
                                .await
                                .is_ok()
                        },
                        move || async move {
                            match manager.dispatch_slash(key, input, args).await {
                                Ok(Some(message)) => message,
                                Ok(None) => "コマンドの応答がありません".to_owned(),
                                Err(_) => "コマンドを実行できませんでした".to_owned(),
                            }
                        },
                        move |(application_id, _, token), message| async move {
                            update_http
                                .interaction(application_id)
                                .update_response(token.expose_secret())
                                .content(Some(&message))
                                .await
                                .is_ok()
                        },
                    )
                    .await;
                    if outcome == src::interaction_flow::Outcome::Completed {
                        response_guard.confirm();
                    } else {
                        eprintln!("Interaction の応答処理に失敗しました（結果不明）: {outcome:?}");
                    }
                });
            }
            Err(error) => eprintln!("{}", failure_message("Gateway error", error)),
            _ => {}
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn failure_output_never_formats_external_error_text() {
        struct UntrustedError;
        impl std::fmt::Display for UntrustedError {
            fn fmt(&self, _: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                panic!("the log boundary must not inspect external Display")
            }
        }
        impl std::fmt::Debug for UntrustedError {
            fn fmt(&self, _: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                panic!("the log boundary must not inspect external Debug")
            }
        }
        for context in [
            "実行に失敗しました",
            "Ready イベントを処理できません",
            "応答追跡を開始できません",
            "Worker error",
            "Gateway error",
        ] {
            assert_eq!(
                failure_message(context, UntrustedError),
                format!("{context}: [REDACTED]")
            );
            assert!(
                !failure_message(context, "short-private-value").contains("short-private-value")
            );
        }
    }

    #[test]
    fn token_environment_errors_are_fixed_and_valid_values_remain_usable() {
        let token = token_from_env(Ok("short-private-value".into())).unwrap();
        assert_eq!(token.expose_secret(), "short-private-value");
        assert_eq!(format!("{token:?}"), "[REDACTED]");
        assert_eq!(
            token_from_env(Err(env::VarError::NotPresent))
                .unwrap_err()
                .to_string(),
            "DISCORD_TOKEN is not configured"
        );
        let error = token_from_env(Err(env::VarError::NotUnicode("short-private-value".into())))
            .unwrap_err();
        assert_eq!(error.to_string(), "DISCORD_TOKEN is not Unicode");
        assert!(!format!("{error:?}").contains("short-private-value"));
    }

    #[tokio::test]
    async fn protected_interaction_identity_reaches_ack_and_update_unchanged() {
        let token = Arc::new(Secret::new("short-private-value".to_owned()));
        let expected = token.clone();
        let result = src::interaction_flow::run(
            (1, 2, token),
            std::time::Duration::from_secs(1),
            std::time::Duration::from_secs(1),
            |(application, interaction, token)| async move {
                assert_eq!((application, interaction), (1, 2));
                assert_eq!(token.expose_secret(), "short-private-value");
                true
            },
            || async { "reply".into() },
            move |(application, interaction, token), message| async move {
                assert_eq!((application, interaction), (1, 2));
                assert!(Arc::ptr_eq(&token, &expected));
                assert_eq!(message, "reply");
                true
            },
        )
        .await;
        assert_eq!(result, src::interaction_flow::Outcome::Completed);
    }

    #[tokio::test]
    async fn response_timeout_is_not_confirmed() {
        assert!(
            !confirmed_response(
                std::time::Duration::from_millis(10),
                std::future::pending::<Result<(), ()>>()
            )
            .await
        );
        assert!(
            !confirmed_response(std::time::Duration::from_secs(1), async {
                Err::<(), ()>(())
            })
            .await
        );
        assert!(
            confirmed_response(std::time::Duration::from_secs(1), async {
                Ok::<(), ()>(())
            })
            .await
        );
    }

    #[tokio::test]
    async fn metadata_failures_and_invalid_limits_prevent_queue_construction() {
        let valid = SessionStartLimit {
            max_concurrency: 1,
            remaining: 1,
            reset_after: 10_000,
            total: 1,
        };
        assert!(single_shard_queue(1, async { Err(()) }).await.is_err());
        assert!(
            single_shard_queue(1, async { Ok((2, valid.clone())) })
                .await
                .is_err()
        );
        let mut invalid = valid.clone();
        invalid.max_concurrency = 0;
        assert!(
            single_shard_queue(1, async { Ok((1, invalid)) })
                .await
                .is_err()
        );
        assert!(
            single_shard_queue(1, async { Ok((1, valid.clone())) })
                .await
                .is_ok()
        );
        assert!(
            single_shard_queue(4, async { Ok((4, valid)) })
                .await
                .is_ok()
        );
    }

    #[test]
    fn gateway_uses_configured_shard() {
        let deployment = worker::DeploymentIdentity::parse(|key| {
            Ok(match key {
                "SHARD_ID" => Some("2".into()),
                "TOTAL_SHARDS" => Some("4".into()),
                _ => None,
            })
        })
        .unwrap();
        assert_eq!(configured_shard(&deployment), ShardId::new(2, 4));
    }

    #[test]
    fn routes_typed_chat_commands_and_rejects_other_variants() {
        use bot_framework::arguments::SlashValue;
        let options = vec![
            CommandDataOption {
                name: "text".into(),
                value: CommandOptionValue::String("hi".into()),
            },
            CommandDataOption {
                name: "n".into(),
                value: CommandOptionValue::Integer(7),
            },
            CommandDataOption {
                name: "flag".into(),
                value: CommandOptionValue::Boolean(true),
            },
        ];
        assert_eq!(
            slash_input("ping", CommandType::ChatInput, &options),
            Some((
                "ping".into(),
                vec![
                    ("text".into(), SlashValue::String("hi".into())),
                    ("n".into(), SlashValue::Integer(7)),
                    ("flag".into(), SlashValue::Boolean(true))
                ]
            ))
        );
        assert_eq!(slash_input("count", CommandType::ChatInput, &[]), None);
        assert_eq!(slash_input("ping", CommandType::User, &[]), None);
        let unsupported = vec![CommandDataOption {
            name: "n".into(),
            value: CommandOptionValue::Number(1.0),
        }];
        assert_eq!(
            slash_input("ping", CommandType::ChatInput, &unsupported),
            None
        );
    }
}
