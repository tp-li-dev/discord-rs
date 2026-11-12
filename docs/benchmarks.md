# IPC codec ベンチマーク

## 対象と再現手順

仕様 §14.1–14.2 の JSON / MessagePack 比較のうち、codec 単体の encode/decode と符号化サイズを測定する。`src/benches/ipc_codec.rs` は `src/src/protocol.rs` の公開型 `Envelope` / `Message::Data` / `Data::DiscordEvent` を直接使用する。プロトコル型の複製や追加依存はない。

リポジトリルートから PowerShell で実行する。

```powershell
Set-Location src
cargo --offline build
cargo --offline test --workspace
cargo --offline test --lib protocol::tests
cargo --offline clippy --bench ipc_codec -- -D warnings
rustfmt --edition 2024 --check benches/ipc_codec.rs
$env:IPC_BENCH_WARMUP = '1000'
$env:IPC_BENCH_SAMPLES = '10000'
cargo --offline bench --bench ipc_codec
```

環境変数を省略しても同じ母数。各値は 1..=1,000,000 の整数で指定する。出力は各ケース・codec・操作のサイズと p50/p95/p99（ns）。Linux でも `src/` から同じ Cargo コマンドを使用できるが、本記録では未実行。

入力は小64 byte、中4,096 byte、大65,536 byte の `Vec<u8>`。内容は `i % 256` の反復で、固定の合成 ID・時刻・event_key を持つ。実イベントや秘密情報は使わない。同じ入力を繰り返すため、実 Discord トラフィックの分布を表すものではない。

JSON は `serde_json::to_vec/from_slice`、MessagePack は `rmp_serde::to_vec_named/from_slice`。既存の内部タグ付き enum を含めて名前付き map で符号化する。既存 `Vec<u8>` は整数列として扱われ、MessagePack bin 専用表現には変更しない。符号化サイズは Envelope 全体の byte 数で、フレームヘッダや改行を含まない。

各ケースで両 codec の decode 結果と元 Envelope の完全一致を事前検査する。各操作で warmup 1,000 回後、独立した `Instant` 計測を10,000回行う（12組、合計120,000計測、warmup 12,000回）。生成・事前符号化・比較・ソート・表示は計測外。encode の出力確保、decode の所有値確保は計測内、返却値の破棄は計測外。`black_box` で入力・出力を使用する。昇順ソートした nearest-rank `ceil(N*p/100)-1` の添字を用いる。中ケースだけ codec 順序を逆転するが、無作為化や複数独立試行は行わない。

## 実測環境

- 実行日: 2026-11-12
- OS: Windows、`Microsoft Windows NT 10.0.26200.0`（.NET の OSVersion）
- CPU: AMD Ryzen 5 7600X 6-Core Processor（レジストリで確認）
- rustc: `1.99.0 (b940084d7 2026-09-28)`、host `x86_64-pc-windows-msvc`、LLVM `23.1.1`
- profile: Cargo 標準 bench profile `[optimized]`（release 相当、profile 上書きなし）
- serde_json 1.0.154、rmp-serde 1.3.1。既存 lockfile・取得済み依存を offline 使用。
- 単一スレッド、CPU affinity・電源設定・バックグラウンド負荷の固定なし。タイマのオーバーヘッドは差し引かない。

## 実測結果

以下はビルド・テスト終了後の1試行。時間はすべて ns、サイズは Envelope 全体の byte 数。

| ペイロード | codec | サイズ | 操作 | p50 | p95 | p99 |
|---|---|---:|---|---:|---:|---:|
| 小64 B | JSON | 495 | encode | 500 | 800 | 900 |
| 小64 B | JSON | 495 | decode | 2000 | 3300 | 3600 |
| 小64 B | MessagePack | 304 | encode | 600 | 800 | 1000 |
| 小64 B | MessagePack | 304 | decode | 1600 | 1700 | 2300 |
| 中4096 B | JSON | 14937 | encode | 14800 | 15700 | 18700 |
| 中4096 B | JSON | 14937 | decode | 69400 | 93100 | 105300 |
| 中4096 B | MessagePack | 6384 | encode | 9700 | 14400 | 17100 |
| 中4096 B | MessagePack | 6384 | decode | 75600 | 118800 | 137500 |
| 大65536 B | JSON | 234297 | encode | 234900 | 309800 | 328700 |
| 大65536 B | JSON | 234297 | decode | 1777400 | 2567400 | 4100600 |
| 大65536 B | MessagePack | 98546 | encode | 221900 | 294100 | 485700 |
| 大65536 B | MessagePack | 98546 | decode | 1516900 | 1940600 | 2031300 |

この入力では MessagePack の符号化サイズが全ケースで小さい。時間の優劣はサイズ・操作・percentile によって異なり、codec 採用判断は保留する。

## 制約と判断

**Windows 開発機での参考値であり Linux 本番性能の根拠ではない。** codec 採用判断は保留する。

実 IPC の片道・往復遅延、フレーミング、Named Pipe / Unix socket、認証、quorum / lease、バッチ、並列負荷、コピー回数、CPU%、RSS、Tokio の遅延、世代切替・障害条件を測定していない。仕様の Linux 小型 IPC p99 1 ms 目標の合否は判定できない。サイズ・内容や整数列の分布が異なれば結果も変わる。小さい操作ではタイマ分解能・測定コストの影響が大きい。単発試行の裾の遅延には OS スケジューリング等が含まれ、信頼区間や再現性の保証はない。Linux の実トランスポートと代表的トラフィックで追加評価が必要。

## 検証結果

- `cargo --offline build`: 成功。
- `cargo --offline test --lib protocol::tests`: 2件成功。
- `cargo --offline clippy --bench ipc_codec -- -D warnings`: 成功。
- `rustfmt --edition 2024 --check benches/ipc_codec.rs`、`git diff --check`: 成功。
- `cargo --offline test --workspace`: 既存 `tests/local_control.rs:28` の `authenticated_private_session_lists_only_unknown_ids` が失敗（期待 `[(0, 0)]`、実際 `[(0, 1)]`）。このテストバイナリは5件成功・1件失敗で終了し、後続の全テスト完走は未確認。担当範囲外のため修正していない。protocol を含むライブラリ35件は成功。
- 初回の `cargo --offline test` はベンチファイル追加中に manifest を参照し対象未検出で終了したため、ファイル作成完了後に上記 workspace テストを再実行した。
- ベンチ実行時、全3サイズ・両 codec の往復一致を確認。初回は他のコンパイル・テストと重複したため記録値に使わず、それらの終了後に同じコマンドで再測定した値を掲載する。
- `docs/progress.md` は担当指示に従い編集せず、コミットも行っていない。
