# コンポーネント状態アダプタ

`bot_framework::interactions::InteractionStateStore` が framework と状態保存の接続点です。
`src::state_store::FrameworkStateStore` が既存の `StateStore` と `CustomIdCodec` を使って実装します。
framework から実行基盤クレートへの依存は追加していません。

## 初期化と配送

ホストは信頼済み設定から Bot ID・Shard ID・アプリ名前空間を決め、次の順序で構築します。

1. `Arc<dyn StateStore>` と読み書き用 `SchemaVersion` を用意する。
2. `FrameworkStateStore::new(store, scope, namespace, signing_key, supported_versions, schema)` を作る。
3. `Registry::new()` でリンク済み `#[component]` / `#[modal]` ハンドラーを収集する。
4. 認証・受付制御済みの入力に対して `registry.dispatch_with_drain(&adapter, kind, custom_id, values, replies, drain)` を呼ぶ。

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

Worker 経路のハンドラーでは `ctx.wait_for(future).await` がその世代の通知を使います。
キャンセルされるのは渡した Future で、ハンドラー全体ではありません。ハンドラーは取消理由を確認し、
必要な後処理や応答を終えてください。状態書込みを一括で abort して結果不明を隠す処理はしません。
`dispatch_with_drain` は通知が true または閉鎖済みの場合、状態読取り前と読取り後に受付を拒否します。
既存 `dispatch` は互換性のため残しますが、世代通知を持たないため、その Context の `wait_for` は
`WaitCancelled::SignalClosed` を返します。単独の `interactions::wait_for` も引き続き利用できます。

`interactions::Subscription` は従来からの記述用の型で、保存や配送を実装しません。
`src::state_store::Subscription` は保存・復元・明示取消を持つ別の型です。
その `durable_wait` は永続性を要求し、`MemoryStateStore` では `DurabilityRequired` を返します。
コンポーネント状態アダプタは Subscription を作成・配送する API を持ちません。
どちらも実行中 Future の自動移行や exactly-once 実行を意味しません。

## Worker 実プロセスとの接続

`src::worker::InteractionStateConfig::file` で状態ファイル、名前空間、独立した署名鍵、
対応ルートバージョン、スキーマを用意し、必要なら `with_limits` で容量上限を指定します。
`DeploymentIdentity::with_interaction_state` に設定して起動すると、親は Ready と双方向 Hello の
capability/identity 検査後、専用の起動設定要求を匿名 stdin パイプへ送ります。
設定時は `interaction-state-v1`・`drain-signal-v1`・`concurrent-drain-v1` の交渉を必須とし、
未対応の候補は採用しません。設定を持たない通常コマンドの旧 Worker 互換経路は維持します。
鍵を環境変数、コマンド引数、状態ジャーナル、ログへ記録する必要はありません。
これは秘密の新規生成・保管や鍵ローテーションを自動管理する API ではありません。

子の `worker::run` は信頼済み IPC identity の Bot/Shard から `FrameworkStateStore` を構築します。
custom_id、ユーザー入力や保存済みデータから Bot/Shard を選びません。
署名ドメインには Bot/Shard/Component/Modal が含まれ、世代番号は含まれません。
同じ鍵・スコープ・名前空間・互換スキーマ・共有バックエンドを持つ次世代が、更新前の ID を復元します。
`worker::run_with_state_store(Arc<dyn StateStore>)` はアプリが独自バックエンドを注入する接続点です。
framework 自体は実行基盤クレートへ依存せず、既存の `InteractionStateStore` trait を使います。

親の `Manager::dispatch_interaction` は Active 世代へ Component/Modal と custom_id・値を配送し、
子が Registry を通じてリンク済みハンドラーを実行します。Button と Select は同じ Component 区分で、
Select/Modal の値は `ctx.values()` に届きます。ハンドラーと状態書込みは管理対象の要求タスク内で完了させます。
外部応答を伴う場合は同じ `ResponseGuard` を `dispatch_interaction_with_response` に渡し、
既存の `finish_response` により送信結果を元世代へ返します。外部応答が不明なら既存の結果不明追跡に残し、
新しい世代へ無条件再送しません。文字列返信の収集だけで Discord への送信成功を意味しません。
検証拒否・期限切れ・結果不明は `InteractionError::response()` の固定文言で返信します。
親で受付済みでも、直列実行待ちや状態の検証中に drain が始まった Interaction は、
ハンドラーを開始せず拒否する場合があります。既に開始したハンドラーは後処理まで追跡します。

`drain-signal-v1` の明示広告・Hello 交渉が成立した子には、一方向の `BeginDrain` 通知を送ります。
親が旧世代への受付を閉じた後に送信し、子の専用 reader がハンドラー待機中も受信して世代 watch を true にします。
以後その世代の通知を false に戻しません。新世代は独立した通知を作ります。
Snapshot は候補失敗時に旧世代を再利用するための操作であり、それだけでは恒久 drain 通知を発しません。
EOF でも世代内 wait は取り消します。取消は保存状態や Subscription を削除せず、Future の別プロセス移行もしません。
reader は有界キューを持つ専用 OS スレッドで、同期入力が Tokio runtime のエラー終了を妨げないようにします。
`run` / `run_with_state_store` はプロセス entrypoint として一度だけ呼び、終了・取消後はプロセスを終了してください。
ブロック中の stdin 読取りは OS 終了まで残る場合があり、同じプロセスで stdin を再利用する API ではありません。

簡易 `FileStateStore` は同一ホスト上のローカルファイルを新旧 Worker で共有するための実装です。
OS ファイルロックと revision 条件によりプロセス間の更新競合を扱い、I/O は Tokio の非同期実行スレッドから分離します。
任意の既存アプリ状態・StateMap や実行中 Future が自動的に永続化されるわけではありません。
ファイル実装の容量・クラッシュ復旧・保管場所の条件は `FileStateStore` の API 文書に従います。
全スナップショットを追記し、操作ごとに全履歴を再読込みする簡易実装です。compaction はありません。
ジャーナル上限では既存状態を保全して書込みを拒否します。OS ファイルロック API に Rust 1.89 以上が必要です。
ネットワークファイルシステムや複数ホストでの強整合ストア、電源断への完全な耐性は保証しません。

実 Discord の Gateway 側 Component/Modal デコード、初回 ACK/defer、Interaction token 管理と
外部 HTTP 送信への接続はこの担当範囲外です。Daemon からの設定配布、ユーザー認可、quorum/lease の本番統合、
Windows ACL、Linux 実機、複数ホスト・外部 DB での動作は別途検証が必要です。
ローカル Worker 試験の成功を、それらの完成の証拠にはしません。仕様・進捗の共有文書は変更していません。

## 永続 Subscription 配送とジョブの設計メモ（未実装）

以下は [local-response-management.md](../../docs/local-response-management.md) と整合させた接続条件です。
現行の Subscription 保存 API や `FileStateStore` だけでは配送・再開・永続ジョブは実装されません。

- Subscription は Bot/Shard/名前空間、ルート・スキーマ、イベントフィルター、有効期限、再開ハンドラー、
  チェックポイント、revision を持つ保存レコードにします。実行中 Future、Discord token、任意コードや署名鍵を保存しません。
  世代の wait 取消と購読の明示取消を分離し、世代交代で勝手に購読を削除しません。
- 配送担当は既存の quorum と lease/ownership epoch に従って選びます。レコードの claim は revision を条件に CAS し、
  所有者・epoch・期限を保存します。副作用を許可する実行経路でも現在の権限を確認します。
  期限切れ claim の再取得だけで旧実行の停止や二重副作用の防止ができるとは扱わず、独自コンセンサスも追加しません。
- 認証済みイベントの受付記録、アプリの処理結果／次チェックポイント、外部操作の結果を別の段階として記録します。
  event key がある場合に限り、その範囲で重複を判定します。外部送信前に操作 ID と元世代を記録し、
  送信結果を受けてからチェックポイントを進める順序とクラッシュ時の各境界を検証する必要があります。
- 外部送信の timeout・接続断・プロセス停止は `Unknown` として永続ジャーナルに残す設計にします。
  ローカル CAS の成功や claim の期限切れを根拠に非冪等操作を自動再送しません。
  照会不能を空一覧に変換せず、照合／解除は認証・認可済み管理操作とし、対象・本人・根拠・時刻・結果を監査します。
  解除前の監査記録に失敗した場合は解除せず、token・本文・状態値を監査に自動付与しません。
- 永続ジョブは入力の安全な参照、チェックポイント、再開ハンドラー、スキーマ互換条件、期限、試行履歴と
  外部操作 ID を保存します。新 Active は互換性と権限を検証して保存点から再開します。
  キャンセル・競合・結果不明時の進行条件を明示し、メモリ上の Future の移行や exactly-once は約束しません。
- 配送キュー、claim、チェックポイント、結果不明一覧に容量・期限・読書き時間の上限を設けます。
  外部 DB の耐久性・条件付き更新・時計、Windows ACL／Linux 所有権、部分書込み・ストレージ障害・
  世代切替・lease 喪失・再起動・外部応答不明を試験してから永続配送の保証を宣言します。

## 前回アダプタ単体の検証（2026-11-12）

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

## 世代通知 API の検証（2026-11-12）

Windows / Rust 1.90.0 で次を実行しました。実 Worker の統合試験結果は接続担当のレポートを参照してください。

- `cargo test --offline -p bot-framework --test interaction_generation --test interactions --test interaction_state_api`: 9件成功。
  新規5件はハンドラーの wait 取消と Future の Drop・後処理、読取り前／読取り後の受付拒否、
  既存 dispatch の互換性、InteractionKind の wire 名を確認します。
- `cargo clippy --offline -p bot-framework --tests -- -D warnings`: 成功。
- 担当 framework ファイルの `rustfmt --edition 2024`、`git diff --check`: 成功。

## Worker 接続の最終検証（2026-11-12）

Windows / Rust 1.90.0、Cargo プロジェクトの `src/` で実行しました。
Cargo はすべて offline とし、子ビルドにも `CARGO_NET_OFFLINE=true` を指定しました。

- `cargo test --offline --workspace --no-fail-fast`: 775件成功、失敗0件、既存の無視2件。
  無視は手動ベンチと別試験から明示起動するクラッシュ用子プロセス補助です。
- `cargo clippy --offline --workspace --all-targets -- -D warnings`: 成功。
- `cargo --offline fmt --all --check`: 成功。
- `cargo build --offline --workspace --examples`: 成功。
- `git diff --check`: 成功。

新規試験は19件です。

- `framework/tests/interaction_generation.rs`: 5件。世代通知による wait の取消・Future の破棄・
  ハンドラー後処理、状態復元前後の drain 拒否、旧 dispatch の互換性を確認しました。
- `tests/file_state_store.rs`: 9件。独立したハンドルと別プロセスの CAS、再オープン、削除後の revision、
  TTL・schema・時計・容量、途中末尾と破損、Future 取消後に完全な書込みが残り得る契約を確認しました。
- `tests/worker_interactions.rs`: 5件。実 Worker の匿名パイプで Button/Select/Modal の値と CAS、
  Button から作成した Modal の復元、ホットスワップ・Manager の完全停止再起動・クラッシュ復旧を確認しました。
  同一ファイル・署名鍵・状態キーを使った Bot/Shard の分離、種別違いと MAC 改ざんの拒否、
  drain 時の wait 取消後の状態書込み、新世代の独立した通知、旧世代への API 結果帰属も確認しました。

実 Worker fixture は `tests/fixtures/interaction_worker.rs`、Cargo による依存解決と成果物の取得は
`tests/support/interaction_fixture.rs` です。古い rlib を手動選択していません。
実装変更は `src/worker.rs`、`src/state_store.rs`、`framework/src/interactions.rs`、本文書です。
`daemon.rs`・共有 docs・依存定義・lockfile は変更せず、commit もしていません。

開発中に既存試験で検出した、stdin 先読みによる不正入力時の終了待ちと JSON 診断分類の消失を修正し、
該当する既存試験と新 Worker 試験52件の再実行後、上記 workspace 全体を再実行しました。
この検証はローカルのプロセス再起動・世代重複に限定されます。
実 Discord の入力解析・初回 ACK/defer・HTTP、Linux、Windows ACL、電源断・ストレージ障害、
複数ホストや外部 DB、quorum/lease の本番統合、永続 Subscription 配送とジョブ実行は未実装または未検証です。
