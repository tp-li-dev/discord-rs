# 開発者ガイド（現在のAPI）

[architecture.md](architecture.md) は目標仕様です。ここでは `src/framework`、`src/framework-macros` と実Workerのコード・テストに存在するAPIを説明します。実Discordでの検証はユーザーが実施する範囲です。

> [!NOTE]
> 2026-11-13の決定により、**対応OSはLinux(Ubuntu)のみであり、Windowsはサポート外**（開発機での動作は保証しない。Windows固有コードは残るが保証・CI対象外）です。後述のWindows開発機での検証結果はすべて『過去の参考記録』と位置づけられます。未達事項や保証限界は [known-limitations.md](known-limitations.md) を参照してください。

## Cogなしのコマンド

実装例は `src/src/cogs/general.rs`、Workerへの接続は `src/src/cogs/mod.rs` です。

```rust
use bot_framework::prelude::*;

#[hybrid_command(name = "ping", description = "Check response")]
async fn ping(ctx: Context<'_>) -> BotResult<()> {
    ctx.reply("Pong!").await
}

#[hybrid_command(name = "greet", description = "Greet a user")]
async fn greet(ctx: Context<'_>, name: String, count: Option<i64>) -> BotResult<()> {
    ctx.reply(format!("Hello, {name}! ({})", count.unwrap_or(1))).await
}
```

既存の `ping` / `greet` と置き換える例です。同名で追加すると重複エラーになります。別ファイルに置くなら `mod` 宣言などで実際にリンクしてください。`inventory` は未リンクのファイルを発見しません。`commands()` は名前順に返し、重複名を拒否します。名前省略時は関数名、description省略時は空文字列です。Discord登録には1〜100文字のdescriptionが必要です。

フリー関数は安全な非ジェネリック `async fn` とし、先頭は `Context<'_>`、戻り値は `BotResult<()>` にします。型付き引数は `String` / `i64` / `bool` とその `Option<T>` に対応し、省略可能な引数は必須引数より後に置きます。prefixは空白区切りのみで、引用符・バックスラッシュを拒否します。boolは `true` / `false`、型不一致・必須欠落・余分な入力はエラーです。実prefixアダプターでは余分な引数を未認識コマンドとして無応答にする扱いがあります。

`!greet Alice 3` と、登録後の `/greet name:Alice count:3` は同じハンドラーに到達する実装です。`typed_prefix.rs` / `typed_slash.rs` は実Worker IPCで検証します。登録スキーマは属性引数から生成されますが、実Discordへの登録・応答は別途確認が必要です。

`Context::reply` 自体は文字列を応答用バッファへ追加します。実アダプターが複数のreplyを改行結合し、親側でREST送信します。replyの成功はDiscordの送信確認ではありません。`ctx.send` / `defer` / `guild_id` / `author` / `tasks` / `cache` は現在の公開APIにありません。

## Cog・イベント・状態をローカルで試す

次は `framework/tests/cog_extensions.rs` と `state.rs` のAPIを使う、Discord接続不要のサンプルです。`src/framework/tests/guide_example.rs` など独立したテストターゲットとして保存すると、`src/` から `cargo test --offline -p bot-framework --test guide_example` で実行できます。既存WorkerにCogを追加する起動APIはまだありません。

```rust
use bot_framework::{arguments::Arguments, dispatch, dispatch_ready, prelude::*};
use std::sync::{Mutex, atomic::{AtomicUsize, Ordering}};

struct Counter(AtomicUsize);
struct Greeting;
impl Cog for Greeting {}

#[cog]
impl Greeting {
    #[hybrid_command(name = "guide_greet", description = "Local Cog example")]
    async fn greet(&self, ctx: Context<'_>, name: String) -> BotResult<()> {
        let n = ctx.state::<Counter>()?.0.fetch_add(1, Ordering::Relaxed) + 1;
        ctx.reply(format!("Hello, {name}! ({n})")).await
    }

    #[event]
    async fn ready(&self, ctx: EventContext<'_>, event: ReadyEvent) -> BotResult<()> {
        ctx.state::<Counter>()?.0.fetch_add(1, Ordering::Relaxed);
        ctx.log_info(&event.bot_name);
        Ok(())
    }
}

#[tokio::test]
async fn cog_and_state() -> BotResult<()> {
    let mut states = StateMap::new();
    states.insert(Counter(AtomicUsize::new(0)));
    let mut cogs = CogSet::new();
    cogs.add(&mut states, Greeting).await?;
    let logger = |_: &str| {};
    dispatch_ready(
        EventContext::with_state(&logger, &states),
        ReadyEvent { bot_name: "local".into() },
    ).await?;
    let replies = Mutex::new(Vec::new());
    let args = Mutex::new(Arguments::prefix("Alice")?);
    assert!(dispatch(
        "guide_greet",
        Context::with_state(&replies, &states).with_arguments(&args),
    ).await?);
    assert_eq!(*replies.lock().unwrap(), ["Hello, Alice! (2)"]);
    cogs.remove::<Greeting>(&mut states).await?;
    Ok(())
}
```

`CogSet::add` は `start` 成功後に `Arc<Cog型>` をStateMapへ登録します。`#[cog] impl` 内のコマンドとReadyメソッドは同じインスタンスを参照します。インスタンス未登録や型の二重登録はエラーです。`remove` は `drain` → `stop` → 登録削除の順で、失敗・取消時は登録を残します。フックの既定実装は成功するだけです。ホスト側で新規受付停止と処理中ハンドラーの終了待ちが必要で、フック単体にWorker drainや安全なホットスワップの保証はありません。再試行可能なフックは冪等にしてください。

フリー関数のイベントは次の形です（実例 `general.rs`）。

```rust
use bot_framework::prelude::*;

#[event]
async fn on_ready(ctx: EventContext<'_>, event: ReadyEvent) -> BotResult<()> {
    ctx.log_info(&event.bot_name);
    Ok(())
}
```

現在のイベントはReadyだけです。リンク済みハンドラーを名前順に呼び、最初のエラーで停止します。実GatewayからのReadyはWorker poolのindex 0だけに配送されます。全Workerへの通知やMessageCreateイベントのユーザー定義ハンドラーは提供しません。

StateMapは具体型ごとに `Send + Sync + 'static` の値を保持し、`Context::state::<T>()` / `EventContext::state::<T>()` で取得します。同型のinsertは置換、未登録型の取得はエラーです。共有可変値にはAtomicやMutexなどを使い、Mutexガードをawait越しに保持しないでください。

実Workerでは `FrameworkRuntime::default` が `FrameworkCounter` を注入します。`!state_counter` はWorkerごとの一時状態で、Worker更新・再起動でリセットされます。`!count` と計算ジョブは別の明示的snapshotで、親メモリから引き継ぎます。StateMapやCogの状態が自動的に保存・移行されることはありません。

## コンポーネント・状態・タスク基盤

`framework/src/interactions.rs` のRegistryと `#[component]` / `#[modal]` はButton・Select・Modalの名前空間付きルートに対応し、`wait_for` とSubscriptionの型もあります。通信非依存のAPIで、Discordや実Workerへの配送は未接続です。ルートやcustom IDだけでユーザー認可・再送防止はできません。

`src/state_store.rs` はTTL・容量制限・CAS・schema検査を伴う非同期StateStoreを提供します。メモリ状態はプロセス停止で失われます。`FrameworkStateStore` と `StateStoreAdapter` により framework の状態操作を StateStore へ接続可能であり、ファイル永続 StateStore backend も実装されていますが、実Discord配送やWorkerへの自動統合を意味しません。`subscription` や `durable_jobs.rs` も耐久 backend を要求するライブラリ基盤です。`src/supervisor.rs` のTaskManager / Supervisorは追跡・取消・drain・再起動方針の基盤で、Workerの要求タスクに一部接続されています。任意のCog・外部副作用を含む全資源のdrainを保証しません。

ファイル権限保護として `fs_perms` があり、Linux では開いたハンドルの mode をファイル0600・ディレクトリ0700に設定・検証します（Windows用コードは残りますがWindowsはサポート外です）。`src/tls_transport.rs` は相互TLSと証明書更新（`cert_rotation`）、`cert_policy.rs` は認証付きpolicy保存とCAチェーン管理を提供します。`raft_quorum` のOpenRaft、mTLS Raft RPC、`cluster_runtime::bind_raft` は試験経路として接続されていますが、本番backendのbotd起動は拒否されます。`child_recovery` はdaemonのGateway監視とWorker復旧に接続され、`update_rollback` と daemon の更新制御はサーバー側 capability 認可と監査を備えますが通常モードでの実行は制限されます。`metrics_feed` は観測用 feed ですが daemon には未接続です。いずれも本番の分散安全性を証明しません。

Discord不要のサンプルは `src/` から次で実行できます。

```sh
cargo run --offline -p bot-framework --example ping_bot
cargo run --offline -p bot-framework --example hybrid_bot
```

## 確認するテスト

すべて `src/` で実行します。

```sh
cargo test --offline -p bot-framework
cargo test --offline --test typed_prefix --test typed_slash --test state_framework --test ready_event
cargo test --offline --workspace
```

frameworkの `hybrid` / `typed_macro` / `cog_extensions` / `state` / `events` とcompile-failがAPI・診断の根拠です。実Worker試験はDiscord HTTPの成功、Interaction期限、OS障害・分断時の安全性を証明しません。

## 過去の検証記録（過去の参考記録: Windows）

> [!NOTE]
> 対応OSはLinux(Ubuntu)のみであり、**Windowsはサポート外**（開発機での動作は保証しない。Windows固有コードは残るが保証・CI対象外、2026-11-13決定）です。以下の2026-11-12時点でのWindows / Rust 1.99.0での成功記録は『過去の参考記録』と位置づけられます。

2026-11-12、Windows / Rust 1.99.0で `cargo test --offline --workspace`、`cargo fmt --all --check`、`cargo clippy --offline --workspace --all-targets -- -D warnings` が成功しました。掲載した3つのRustブロックを一時ディレクトリへ抽出し、既存workspaceのビルド成果物に対してrustcでコンパイルしました。Cog・Ready・状態共有サンプルは1テスト成功、フリー関数のコマンド・イベント例もコンパイル成功です。リポジトリのコードは変更していません。実Discord・Linux・複数ホストの再検証は行っていません。

## CIと追加の検証対象

`.github/workflows/ci.yml` はpushとpull requestで実行されます（Linux/Ubuntu CIが対象であり、Windows CIはサポート外）。`stress.yml` は手動実行でOSごと1〜20回、suite（update_load, raft, recovery, acl, all）を選択します。`bench.yml` も手動のLinux測定です。workflowの存在は今回の実行成功を意味しません。更新負荷試験の無停止条件は既知の未達で `src/tests/update_load.rs` は `#[ignore]` です。

ユーザー提供のLinux CIベンチ記録（GitHub Actions `ubuntu-latest`、AMD EPYC 7763 4vCPU）では、IPC往復 p99 が並列度 1 / 4 / 16 で 115.5 / 258.1 / 973.6 µs、failover 検知 p50 は Suspected 1503.6 ms、FailoverPending 3003.1 ms です。これらは共有ランナーでの観測であり本番性能の保証ではなく、往復値は片道p99の直接測定ではありません。測定条件と結果は [benchmarks.md](benchmarks.md) を参照してください。

`discord_smoke` は読み取り専用REST補助CLI（`src/src/bin/discord_smoke.rs`、`src/tests/discord_smoke/cli.rs`）で、GatewayやCoreを起動しません。実Discordでの検証はユーザーが実施する範囲です。`FrameworkStateStore` のアダプター、ファイル永続StateStore、subscriptionとdurable jobsはライブラリAPIとして利用条件を確認してください。実Workerの自動永続化や外部副作用のexactly-onceを保証しません。
