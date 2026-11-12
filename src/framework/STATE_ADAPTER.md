# コンポーネント状態アダプタ

`bot_framework::interactions::InteractionStateStore` が framework と状態保存の接続点です。
`src::state_store::FrameworkStateStore` が既存の `StateStore` と `CustomIdCodec` を使って実装します。
framework から実行基盤クレートへの依存は追加していません。

## 初期化と配送

ホストは信頼済み設定から Bot ID・Shard ID・アプリ名前空間を決め、次の順序で構築します。

1. `Arc<dyn StateStore>` と読み書き用 `SchemaVersion` を用意する。
2. `FrameworkStateStore::new(store, scope, namespace, signing_key, supported_versions, schema)` を作る。
3. `Registry::new()` でリンク済み `#[component]` / `#[modal]` ハンドラーを収集する。
4. 認証・受付制御済みの入力に対して `registry.dispatch(&adapter, kind, custom_id, values, replies)` を呼ぶ。

Button と Select は `InteractionKind::Component`、Modal は `InteractionKind::Modal` です。
Select の選択値や Modal の値は既存の `ctx.values()` で受け取ります。
値の信頼性やユーザー権限はホスト・アプリ側で確認します。
この API は値の文字列配列を運ぶだけで、Discord のペイロードを解析しません。

署名鍵は Bot token とは独立した鍵です。ログ、状態ペイロード、custom_id には格納しません。
Bot token を Core に渡す必要はありません。新 Core から復元するには、同じ状態バックエンド、
Bot/Shard/名前空間、互換スキーマ、署名鍵が必要です。鍵ローテーションの重複期間は未実装です。

## ハンドラーでの状態操作

`ctx.state::<T>()` は配送開始時に検証した JSON スナップショットです。
`ctx.load_state::<T>().await` は再度 MAC と保存状態を検証し、次をまとめて返します。

- `data`: JSON から復元した値
- `revision`: その読取りに対応する CAS 用 revision
- `expires_at_unix_ms`: 有効期限
- `route`: 検証済みの名前空間・機能名・バージョン

```rust
use bot_framework::{BotResult, component, interactions::InteractionContext};
use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize)]
struct Counter {
    count: u64,
}

#[component(namespace = "app", name = "counter", version = 1)]
async fn counter(ctx: InteractionContext<'_>) -> BotResult<()> {
    let mut saved = ctx.load_state::<Counter>().await?;
    saved.data.count += 1;
    ctx.save_state(&saved.data, saved.revision, saved.expires_at_unix_ms).await?;
    ctx.reply("更新しました").await
}
```

`save_state` は現在の custom_id の状態を CAS で更新します。更新対象の route と保存済み
envelope を照合したうえで、その revision を原子的な条件にします。競合時は `Conflict` です。
状態を再取得して判断し直し、外部副作用を無条件に再実行しないでください。
成功時はその書込みが確定した revision を返し、保存後の別読取りで revision を推定しません。
`ctx.state()` の元スナップショットは更新されません。必要なら `load_state` を再実行します。

期限は Unix epoch からのミリ秒で、相対時間ではありません。`now >= expires_at` で期限切れです。
`save_state` の期限引数で TTL を変更できますが、すでに期限切れの状態は CAS で復活しません。
共有バックエンドで使う時計の同期・巻戻り対策はバックエンドの契約に従います。

`ctx.create_state(kind, name, version, &value, expires_at_unix_ms).await` は現在の名前空間内に
別の状態を作り、新しいランダム ID を署名して返します。Button から後続 Modal の状態を作る
場合にも利用できます。`CreatedState.custom_id` を送信するコンポーネント等に設定するのはホスト側です。
初回の状態はホストから trait の `create`、または `create_with_id` で作成できます。
`create_with_id` は事前生成した `StateId` を使うための API です。実運用では毎回新しい ID を
用意し、期限切れ・削除済みの ID を再利用しないでください。

`ctx.delete_state(revision).await` は明示的な条件付き削除です。
CAS・条件付き削除で状態が消失または期限切れの場合も `Conflict`、読取りの場合は
`ExpiredOrMissing` です。MAC はリプレイ対策やユーザー認可そのものではありません。

## 拒否と互換性

アダプタは custom_id の MAC、100文字上限、対応バージョンを検査します。
MAC は Bot・Shard・Component/Modal の区分にも結び付いており、同じ鍵でも他のスコープでは
利用できません。さらに、正しい MAC を持つ別名前空間の ID も拒否します。
ストアからは期限・スキーマ・保存済み route の一致を確認してからデータを公開します。
custom_id に期限そのものは含めず、保存状態の期限を各操作で確認します。

エラーは入力を含まない固定文言です。`UnknownOutcome` は書込みが成功した可能性を残すため、
`StoreUnavailable` と区別します。タイムアウトや Future の取消時も外部書込みを取り消せたとは
限りません。バックエンドの契約に従って状態を照合してください。
ハンドラーが `?` で返した `UnknownOutcome` は `Registry::dispatch` でも保持します。
自動採番する `create` / `create_state` の失敗時には生成した ID を返さないため、結果不明の
新規状態が期限まで残る可能性があります。事前に ID を把握して照合する必要があるホストは
`create_with_id` を使用してください。自動的な照合・再送・孤立レコードの探索は実装していません。
状態値、署名鍵、custom_id を診断用出力に含めないでください。

既存 `InteractionStateStore::restore` と `RestoredState` の構造は維持します。
追加メソッドの既定値は `StateOperationsUnsupported` で、復元のみを実装した既存ストアは
引き続きルーティングに使えます。MAC を検証しないテスト用ストアに本番の安全性はありません。
属性マクロと既存 examples の呼出し方も変更していません。

## wait と Subscription

`interactions::wait_for` は世代内の Future を待ちます。drain 開始・通知元閉鎖でキャンセルし、
結果とキャンセルが同時に準備できている場合はキャンセルを優先します。
ホストは世代単位の watch channel を用意し、受付停止後に true を通知して戻さないでください。
旧世代の wait を新 Core へ移したり、キャンセル時に保存済み Subscription を削除したりしません。
別に spawn したタスクはホストの Task Manager で追跡する必要があります。

`interactions::Subscription` は従来からの記述用の型で、保存や配送を実装しません。
`src::state_store::Subscription` は保存・復元・明示取消を持つ別の型です。
その `durable_wait` は永続性を要求し、`MemoryStateStore` では `DurabilityRequired` を返します。
コンポーネント状態アダプタは Subscription を作成・配送する API を持ちません。
どちらも実行中 Future の自動移行や exactly-once 実行を意味しません。

## Worker 担当への接続条件と未検証範囲

Worker/daemon の実装は今回変更していません。接続担当は次を実装する必要があります。

- 認証済み IPC identity から Bot/Shard ごとのアダプタを選び、custom_id から scope を選ばない。
- Gateway 側で初回 ACK/defer と Interaction token を管理し、Component/Modal の入力と値を配送する。
- Active 世代への受付、drain 開始通知、処理中ハンドラー・状態書込み・応答結果の追跡を接続する。
- 現在の `Mutex<Vec<String>>` の返信を Gateway の応答操作に接続し、元世代への結果帰属を維持する。
- ユーザー認可、所有権/lease による副作用制御、永続バックエンド、Subscription の配送・復元を実装する。

Windows/Linux 固有の IPC やファイル操作はこのアダプタに追加していません。
メモリストアを共有する独立 Registry の試験は、プロセス・ホストを跨ぐ永続性の証拠ではありません。
実 Worker、実 Discord、外部永続ストア、Linux 上での実行は今回の接続実装では検証していません。
仕様・進捗の共有文書は担当範囲外のため変更していません。

## 今回の検証（2026-11-12）

Windows / Rust 1.90.0、Cargo プロジェクトの `src/` ディレクトリで実行しました。

- `cargo test --offline --workspace --no-fail-fast`: 665件成功、失敗0件、既存の手動ベンチ1件を無視。
- `cargo --offline fmt --all --check`: 成功。
- `cargo clippy --offline --workspace --all-targets -- -D warnings`: 成功。
- `cargo build --offline --workspace --examples`: 成功。
- `git diff --check`: 成功。

追加試験は18件です。実 StateStore アダプタ11件、framework wait と Subscription の境界5件、
復元専用ストアの互換性・Registry 経由の結果不明伝播2件を確認しました。
MAC 改ざん・別名前空間・Bot/Shard/種別の拒否、期限境界、CAS競合、検証読取り後の別更新・
期限切れ、schema/envelope の不整合、容量制限、エラー/Debug の秘匿を含みます。
状態アダプタ試験はメモリバックエンドと決定的な競合注入を使い、外部ストアの原子性や永続性を
検証したものではありません。

既存の compile-fail fixture に残っていた不要な import と、その警告の期待値を除きました。
これにより今回確認した Rust 1.90 / 1.99 間の警告文言差に試験結果が依存しなくなりました。
対象としているマクロのエラー診断5件は維持しています。Rust 1.99 での再実行はしていません。
