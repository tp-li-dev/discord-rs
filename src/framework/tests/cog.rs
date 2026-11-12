use bot_framework::{CommandFuture, dispatch, prelude::*};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};

struct CounterCog {
    count: AtomicUsize,
    started: Arc<AtomicBool>,
    stopped: Arc<AtomicBool>,
}
impl Cog for CounterCog {
    fn start(&self) -> CommandFuture<'_> {
        Box::pin(async {
            self.started.store(true, Ordering::SeqCst);
            Ok(())
        })
    }
    fn stop(&self) -> CommandFuture<'_> {
        Box::pin(async {
            self.stopped.store(true, Ordering::SeqCst);
            Ok(())
        })
    }
}
#[hybrid_command(name = "cog_counter")]
async fn cog_counter(ctx: Context<'_>) -> BotResult<()> {
    let cog = ctx.state::<Arc<CounterCog>>()?;
    let count = cog.count.fetch_add(1, Ordering::SeqCst) + 1;
    ctx.reply(count.to_string()).await
}
struct Broken;
impl Cog for Broken {
    fn start(&self) -> CommandFuture<'_> {
        Box::pin(async { Err("start failed".into()) })
    }
}
#[tokio::test]
async fn cog_lifecycle_and_shared_handler_state() {
    let started = Arc::new(AtomicBool::new(false));
    let stopped = Arc::new(AtomicBool::new(false));
    let mut cogs = CogSet::new();
    let mut states = StateMap::new();
    let counter = cogs
        .add(
            &mut states,
            CounterCog {
                count: AtomicUsize::new(0),
                started: started.clone(),
                stopped: stopped.clone(),
            },
        )
        .await
        .unwrap();
    assert!(started.load(Ordering::SeqCst));
    assert!(
        cogs.add(
            &mut states,
            CounterCog {
                count: AtomicUsize::new(0),
                started,
                stopped: stopped.clone()
            }
        )
        .await
        .is_err()
    );
    let replies = Mutex::new(Vec::new());
    assert!(
        dispatch("cog_counter", Context::with_state(&replies, &states))
            .await
            .unwrap()
    );
    assert_eq!(counter.count.load(Ordering::SeqCst), 1);
    cogs.remove::<CounterCog>(&mut states).await.unwrap();
    assert!(stopped.load(Ordering::SeqCst));
    assert!(states.get::<Arc<CounterCog>>().is_err());
    assert!(cogs.add(&mut states, Broken).await.is_err());
    assert!(states.get::<Arc<Broken>>().is_err());
}

struct RetryStop(AtomicUsize);
impl Cog for RetryStop {
    fn stop(&self) -> CommandFuture<'_> {
        Box::pin(async {
            if self.0.fetch_add(1, Ordering::SeqCst) == 0 {
                Err("stop failed".into())
            } else {
                Ok(())
            }
        })
    }
}
#[tokio::test]
async fn failed_stop_preserves_cog_for_retry() {
    let mut cogs = CogSet::new();
    let mut states = StateMap::new();
    let instance = cogs
        .add(&mut states, RetryStop(AtomicUsize::new(0)))
        .await
        .unwrap();
    assert_eq!(
        cogs.remove::<RetryStop>(&mut states)
            .await
            .unwrap_err()
            .to_string(),
        "stop failed"
    );
    assert!(Arc::ptr_eq(
        &instance,
        states.get::<Arc<RetryStop>>().unwrap()
    ));
    cogs.remove::<RetryStop>(&mut states).await.unwrap();
    assert!(states.get::<Arc<RetryStop>>().is_err());
    assert_eq!(instance.0.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn replaced_cog_state_is_not_removed_by_mistake() {
    let mut cogs = CogSet::new();
    let mut states = StateMap::new();
    let original = cogs
        .add(&mut states, RetryStop(AtomicUsize::new(0)))
        .await
        .unwrap();
    let replacement = Arc::new(RetryStop(AtomicUsize::new(0)));
    states.insert(replacement.clone());
    assert!(
        cogs.remove::<RetryStop>(&mut states)
            .await
            .unwrap_err()
            .to_string()
            .contains("cog state replaced")
    );
    assert!(Arc::ptr_eq(
        &replacement,
        states.get::<Arc<RetryStop>>().unwrap()
    ));
    assert_eq!(original.0.load(Ordering::SeqCst), 0);
}
