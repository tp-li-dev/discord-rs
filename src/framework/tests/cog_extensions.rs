use bot_framework::{
    CommandFuture,
    arguments::{ArgumentKind, Arguments, SlashValue},
    dispatch, dispatch_ready,
    prelude::*,
};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, Ordering},
};
struct Service(Mutex<Vec<String>>);
impl Cog for Service {}
#[cog]
impl Service {
    #[hybrid_command(name = "typed_cog", description = "Typed Cog")]
    async fn command(
        &self,
        ctx: Context<'_>,
        text: String,
        number: i64,
        flag: bool,
        suffix: Option<String>,
    ) -> BotResult<()> {
        self.0.lock().unwrap().push(text.clone());
        ctx.reply(format!(
            "{text}:{number}:{flag}:{}",
            suffix.unwrap_or_default()
        ))
        .await
    }
    #[event]
    async fn ready(&self, ctx: EventContext<'_>, event: ReadyEvent) -> BotResult<()> {
        if event.bot_name == "fail" {
            return Err("event failed".into());
        }
        self.0.lock().unwrap().push(event.bot_name);
        ctx.log_info("ready");
        Ok(())
    }
}
#[tokio::test]
async fn typed_commands_and_events_share_the_injected_instance() {
    let mut states = StateMap::new();
    let mut cogs = CogSet::new();
    let replies = Mutex::new(Vec::new());
    let logs = Mutex::new(Vec::new());
    let logger = |message: &str| logs.lock().unwrap().push(message.to_owned());
    assert!(
        dispatch_ready(
            EventContext::with_state(&logger, &states),
            ReadyEvent {
                bot_name: "missing".into()
            }
        )
        .await
        .is_err()
    );
    let cog = cogs
        .add(&mut states, Service(Mutex::new(Vec::new())))
        .await
        .unwrap();
    let command = bot_framework::commands()
        .unwrap()
        .into_iter()
        .find(|c| c.name == "typed_cog")
        .unwrap();
    assert_eq!(
        command
            .arguments
            .iter()
            .map(|a| (a.name, a.kind, a.required))
            .collect::<Vec<_>>(),
        [
            ("text", ArgumentKind::String, true),
            ("number", ArgumentKind::Integer, true),
            ("flag", ArgumentKind::Boolean, true),
            ("suffix", ArgumentKind::String, false)
        ]
    );
    let prefix = Mutex::new(Arguments::prefix("hello -2 true end").unwrap());
    dispatch(
        "typed_cog",
        Context::with_state(&replies, &states).with_arguments(&prefix),
    )
    .await
    .unwrap();
    let slash = Mutex::new(
        Arguments::slash([
            ("text".into(), SlashValue::String("world".into())),
            ("number".into(), SlashValue::Integer(3)),
            ("flag".into(), SlashValue::Boolean(false)),
        ])
        .unwrap(),
    );
    dispatch(
        "typed_cog",
        Context::with_state(&replies, &states).with_arguments(&slash),
    )
    .await
    .unwrap();
    for input in [
        "hello nope true",
        "hello 1 nope",
        "hello 1",
        "hello 1 true end extra",
    ] {
        let args = Mutex::new(Arguments::prefix(input).unwrap());
        assert!(
            dispatch(
                "typed_cog",
                Context::with_state(&replies, &states).with_arguments(&args)
            )
            .await
            .is_err()
        );
    }
    dispatch_ready(
        EventContext::with_state(&logger, &states),
        ReadyEvent {
            bot_name: "bot".into(),
        },
    )
    .await
    .unwrap();
    assert_eq!(
        *replies.lock().unwrap(),
        ["hello:-2:true:end", "world:3:false:"]
    );
    assert_eq!(*cog.0.lock().unwrap(), ["hello", "world", "bot"]);
    assert_eq!(*logs.lock().unwrap(), ["ready"]);
    assert_eq!(
        dispatch_ready(
            EventContext::with_state(&logger, &states),
            ReadyEvent {
                bot_name: "fail".into()
            }
        )
        .await
        .unwrap_err()
        .to_string(),
        "event failed"
    );
    cogs.remove::<Service>(&mut states).await.unwrap();
    assert!(
        dispatch_ready(
            EventContext::with_state(&logger, &states),
            ReadyEvent {
                bot_name: "removed".into()
            }
        )
        .await
        .is_err()
    );
}
struct Hooks {
    fail: AtomicBool,
    calls: Mutex<Vec<&'static str>>,
}
impl Cog for Hooks {
    fn drain(&self) -> CommandFuture<'_> {
        Box::pin(async {
            self.calls.lock().unwrap().push("drain");
            if self.fail.swap(false, Ordering::SeqCst) {
                Err("drain failed".into())
            } else {
                Ok(())
            }
        })
    }
    fn stop(&self) -> CommandFuture<'_> {
        Box::pin(async {
            self.calls.lock().unwrap().push("stop");
            Ok(())
        })
    }
}
#[tokio::test]
async fn drain_failure_preserves_registration_and_skips_stop() {
    let mut cogs = CogSet::new();
    let mut states = StateMap::new();
    let cog = cogs
        .add(
            &mut states,
            Hooks {
                fail: AtomicBool::new(true),
                calls: Mutex::new(Vec::new()),
            },
        )
        .await
        .unwrap();
    assert!(cogs.remove::<Hooks>(&mut states).await.is_err());
    assert!(Arc::ptr_eq(&cog, states.get::<Arc<Hooks>>().unwrap()));
    assert_eq!(*cog.calls.lock().unwrap(), ["drain"]);
    cogs.remove::<Hooks>(&mut states).await.unwrap();
    assert_eq!(*cog.calls.lock().unwrap(), ["drain", "drain", "stop"]);
    assert!(!states.contains::<Arc<Hooks>>());
}

struct PendingDrain;
impl Cog for PendingDrain {
    fn drain(&self) -> CommandFuture<'_> {
        Box::pin(std::future::pending())
    }
}
#[tokio::test]
async fn cancelled_drain_preserves_registration() {
    let mut cogs = CogSet::new();
    let mut states = StateMap::new();
    let cog = cogs.add(&mut states, PendingDrain).await.unwrap();
    {
        let mut removal = Box::pin(cogs.remove::<PendingDrain>(&mut states));
        let mut context = std::task::Context::from_waker(std::task::Waker::noop());
        assert!(std::future::Future::poll(removal.as_mut(), &mut context).is_pending());
    }
    assert!(Arc::ptr_eq(
        &cog,
        states.get::<Arc<PendingDrain>>().unwrap()
    ));
}
