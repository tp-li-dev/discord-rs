use src::{AppError, worker};
use std::{env, future::IntoFuture, sync::Arc};
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

// Current executable runs one bot and one shard only. This queue is process-local.
async fn single_shard_queue<F>(metadata: F) -> Result<InMemoryQueue, &'static str>
where
    F: std::future::Future<Output = Result<(u32, SessionStartLimit), ()>>,
{
    let (recommended, limits) = tokio::time::timeout(std::time::Duration::from_secs(10), metadata)
        .await
        .map_err(|_| "gateway metadata timed out")?
        .map_err(|_| "gateway metadata unavailable")?;
    if recommended != 1 {
        return Err(
            "this executable supports only one shard; Discord recommends a different count",
        );
    }
    let mut registry = src::identify::IdentifyRegistry::default();
    // A private key for the only bot in this process, not a Discord bot ID.
    registry.register(std::num::NonZeroU64::MIN, &limits)
}

#[tokio::main]
async fn main() -> Result<(), AppError> {
    if env::args().nth(1).as_deref() == Some("--worker") {
        return worker::run().await;
    }
    let _ = rustls::crypto::ring::default_provider().install_default();
    let token = env::var("DISCORD_TOKEN")?;
    let http = Arc::new(Client::new(token.clone()));
    let queue = single_shard_queue(async {
        let response = http.gateway().authed().await.map_err(|_| ())?;
        let metadata = response.model().await.map_err(|_| ())?;
        Ok((metadata.shards, metadata.session_start_limit))
    })
    .await?;
    let config = ConfigBuilder::new(token, Intents::GUILD_MESSAGES | Intents::MESSAGE_CONTENT)
        .queue(queue)
        .build();
    let mut shard = Shard::with_config(ShardId::ONE, config);
    let manager = worker::Manager::start(env::current_exe()?).await?;
    manager.start_jobs();
    tokio::spawn(manager.clone().watch(worker::worker_file()));
    let capacity = Arc::new(Semaphore::new(64));
    println!("Botを起動しています...");
    while let Some(result) = shard.next_event(EventTypeFlags::all()).await {
        match result {
            Ok(Event::Ready(ready)) => {
                println!("ログイン成功: {}", ready.user.name);
                let Ok(permit) = capacity.clone().try_acquire_owned() else {
                    eprintln!("Ready イベントを処理できません: 混雑中");
                    continue;
                };
                let manager = manager.clone();
                let bot_name = ready.user.name.clone();
                tokio::spawn(async move {
                    let _permit = permit;
                    if let Err(error) = manager.dispatch_ready(bot_name).await {
                        eprintln!("Ready イベントを処理できません: {error}");
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
                                eprintln!("応答追跡を開始できません: {error}");
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
                            eprintln!("Worker error: {error}");
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
                            eprintln!("応答追跡を開始できません: {error}");
                            return;
                        }
                    };
                    // The identity never enters the Worker or the logs.
                    let identity = (
                        interaction.application_id,
                        interaction.id,
                        interaction.token,
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
                                .create_response(interaction_id, &token, &response)
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
                                .update_response(&token)
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
            Err(error) => eprintln!("Gateway error: {error}"),
            _ => {}
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
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
        assert!(single_shard_queue(async { Err(()) }).await.is_err());
        assert!(
            single_shard_queue(async { Ok((2, valid.clone())) })
                .await
                .is_err()
        );
        let mut invalid = valid.clone();
        invalid.max_concurrency = 0;
        assert!(
            single_shard_queue(async { Ok((1, invalid)) })
                .await
                .is_err()
        );
        assert!(single_shard_queue(async { Ok((1, valid)) }).await.is_ok());
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
