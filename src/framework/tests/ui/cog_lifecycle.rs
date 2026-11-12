use bot_framework::{Cog, CommandFuture};
struct Service;
impl Cog for Service {
    fn drain(&mut self) -> CommandFuture<'_> { Box::pin(async { Ok(()) }) }
}
fn main() {}
