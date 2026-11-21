This is the crates.io source of twilight-gateway 0.18.0 from
twilight-rs/twilight@f3bf23257ffb799c28c7a885a2dc4acc2e449e83.

Local edits in `src/config.rs` and `src/shard.rs` add an optional Identify guard
at the WebSocket sink. When upgrading Twilight, recheck Hello/Resume routing,
the Identify queue, and the position of the guard relative to `start_send`.
