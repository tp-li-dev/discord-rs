use bot_framework::{dispatch, prelude::*};
use std::sync::{
    Mutex,
    atomic::{AtomicUsize, Ordering},
};

struct Counter(AtomicUsize);
impl Cog for Counter {}
#[cog]
impl Counter {
    #[hybrid_command(description = "Increment counter")]
    async fn add(&self, ctx: Context<'_>) -> BotResult<()> {
        ctx.reply((self.0.fetch_add(1, Ordering::SeqCst) + 1).to_string())
            .await
    }
}

#[tokio::test]
async fn collected_method_uses_registered_cog_instance() {
    let mut states = StateMap::new();
    let mut cogs = CogSet::new();
    let replies = Mutex::new(Vec::new());
    let error = dispatch("add", Context::with_state(&replies, &states))
        .await
        .unwrap_err();
    assert!(error.to_string().contains("Counter"));
    let cog = cogs
        .add(&mut states, Counter(AtomicUsize::new(0)))
        .await
        .unwrap();
    assert!(
        dispatch("add", Context::with_state(&replies, &states))
            .await
            .unwrap()
    );
    assert!(
        dispatch("add", Context::with_state(&replies, &states))
            .await
            .unwrap()
    );
    assert_eq!(cog.0.load(Ordering::SeqCst), 2);
    assert_eq!(*replies.lock().unwrap(), ["1", "2"]);
    assert_eq!(
        bot_framework::commands().unwrap()[0].description,
        "Increment counter"
    );
}
