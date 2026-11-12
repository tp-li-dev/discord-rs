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

## 実 Worker JSON IPC 往復ベンチ（追加）

`src/benches/ipc_roundtrip.rs` は Cargo がビルドする実アプリケーションバイナリを `--worker` で子プロセス起動し、親ベンチから stdin/stdout の改行 JSON で `!ping` を実行する。外部ベンチクレートは使用せず、`std::time::Instant` で計測する。既存の Worker 実装・コマンドハンドラーは変更しない。親側はベンチ専用の送受信ループであり、製品の `Manager` によるロック・応答追跡・再起動処理は含まない。

### 往復の条件と再現コマンド

`src/` で実行する。`CARGO_BIN_EXE_src` により同じ bench profile の Worker を選ぶため、手動パス指定や古い debug バイナリは使用しない。Cargo 標準 bench profile（optimized、release 相当）を使う。

```powershell
Set-Location src
cargo --offline clippy --bench ipc_roundtrip -- -D warnings
rustfmt --edition 2024 --check benches/ipc_roundtrip.rs
cargo --offline bench --bench ipc_roundtrip -- --self-test
$env:IPC_ROUNDTRIP_WARMUP = '1000'
$env:IPC_ROUNDTRIP_SAMPLES = '10000'
cargo --offline bench --bench ipc_roundtrip
```

Linux でも `src/` から同じ Cargo コマンドを使える。環境変数の設定だけ `export IPC_ROUNDTRIP_WARMUP=1000 IPC_ROUNDTRIP_SAMPLES=10000` に置き換える。設定は省略時も上記と同じで、各値は 1..=1,000,000 の10進整数。低母数での smoke test は両変数を `1` または `17` にする。小さい母数の p99.9 は最大値に近く、裾の評価には使えない。

- 並列度は必ず **1 / 4 / 16** の順。並列度 C ごとに新しい実 Worker を C プロセス起動し、各 Worker に最大1件の未完了要求を持つ closed-loop 負荷。単一 Worker への C 件の同時要求や、C Shard の実行を意味しない。
- 各ケースの warmup は合計 W 件、その完了後に合計 N 件を計測する。各 lane に商・余りで均等配分するので、C 倍の母数にはならない。N<C のとき実際に要求する lane は N 個。既定値では合計 warmup 3,000 件、計測30,000件。
- 子プロセスの起動、health と双方向 Hello（version/capability/固定合成 identity の検査）、warmup、ソート、表示、終了待ちは計測外。warmup と計測はそれぞれ barrier で開始を合わせる。親 Tokio runtime は current-thread、Worker の runtime は実アプリケーションの既定値。
- 往復遅延には親での要求 JSON 生成・符号化、書込み・flush、OS パイプ、Worker の読取り・JSON decode、実 prefix `!ping` ハンドラー、空状態 snapshot を含む JSON 応答の生成・書込み、親での読取り・decode・期待応答の完全一致確認を含む。親でのベクトルへの標本格納は往復遅延の計測外。キューを積み続ける open-loop 負荷ではなく、coordinated omission の補正も行わない。
- 各成功要求の ns を昇順に並べ、nearest-rank `ceil(successes*p/1000)-1`（p=500/950/990/999）で p50/p95/p99/p99.9 を得る。失敗や未実行を遅延標本に混ぜず、成功ゼロなら `NA`。events/s は成功数を共通 barrier 解放直前から最後の lane 完了までの実経過秒で割る。barrier 解放・親スケジューリング・標本格納のコストはスループットに含むが、結果回収・ソート・終了待ちは含まない。
- 各 JSON 応答は改行を含め1 MiB 以下、各送受信は5秒で打ち切る。EOF、不正 JSON、過大・未終端フレーム、timeout、期待応答の不一致は失敗。失敗 lane は再送せず停止し、その lane の残りを `skipped` として計上する。`successes + failures + skipped = N` を検査する。起動失敗・warmup 失敗は計測未実施として別出力し、成功値を捏造しない。
- `teardown_failures` は終了異常または5秒の終了期限超過。通常は stdin を閉じて EOF による終了を待ち、期限超過時は kill/reap する。失敗のあるケースがあれば全体を非ゼロ終了する。他の並列度のケースも実行して結果を残す。
- 子の環境は消去し、Windows の非秘密 OS パス `SystemRoot` / `WINDIR` と固定合成 identity だけを渡す。実 Bot token、配置情報、実イベントは使用しない。Worker の stderr は破棄し、エラー出力にもフレーム本文・環境値・資格情報を含めない。

**Windows 開発機の参考値**として記録する。**1ms 未満目標との比較は Linux で要再測定**。仕様 §14 の目標は Linux の小型 IPC の p99 **片道**であり、本ベンチの往復値を半分にして片道を推定したり、Windows 値だけで合格と判定したりしない。Named Pipe / Unix socket、MessagePack、実 Discord / REST、Heartbeat、Shard 分離、更新・障害時性能、CPU/RSS、反復試行・信頼区間は本ベンチの評価外。

### 往復ベンチの実測環境

- 実行日: 2026-11-12。
- OS: `Microsoft Windows NT 10.0.26200.0`（.NET OSVersion）。
- CPU: AMD Ryzen 5 7600X 6-Core Processor、論理 CPU 数12。
- rustc `1.99.0 (b940084d7 2026-09-28)`、host `x86_64-pc-windows-msvc`、LLVM `23.1.1`。cargo `1.99.0 (5f94df478 2026-08-27)`。
- 親・子とも Cargo 標準 bench profile `[optimized]`、既存 lockfile の取得済み依存を `--offline` 使用。Tokio 1.54.1、serde_json 1.0.154。追加依存なし。
- CPU affinity、優先度、電源設定、バックグラウンド負荷は固定していない。タイマ・検査コストは差し引かない。

### 往復ベンチの検証記録

- `cargo --offline clippy --bench ipc_roundtrip -- -D warnings`、`cargo fmt --all --check`、`git diff --check`: 成功。
- 初回の `cargo --offline bench --bench ipc_roundtrip -- --self-test`: 成功。設定値の境界、母数の均等配分、p50/p95/p99/p99.9、空標本、誤応答、1 MiB 境界・超過・EOF・未終端・不正 JSON・無通信 timeout、実 Worker ping・強制終了・失敗/未実行/終了失敗の集計を検査した。
- その後、全 lane 完了後に結果回収を開始する barrier を追加。最終版の clippy と fmt は成功したが、自己検査の再ビルドは `no space on device`（ディスク容量不足）で失敗した。
- `cargo --offline test --workspace`: compile-fail、ライブラリ35件、実 Worker/handshake/フレーム上限/認証付き管理などは通過。その後 `tests/reload_command.rs` の更新試験が別 target への release ビルド中に容量不足（os error 112）で失敗し、後続テストを含む全体完走は未確認。テスト対象コードの不具合が原因と判定したものではない。
- この時点では最終版の記録用10,000件実測、低母数での実プロセス smoke、Linux 実測は未完了。容量確保後に再実行する必要があり、未実測の性能値は掲載しない。
- `docs/progress.md` は PM 統合用のため編集せず、コミットも行っていない。

### 容量確保後の最終検証と往復実測結果

上記の容量不足後、生成済み `src/target/debug` のビルド成果物を NTFS 圧縮して容量を確保した。ベンチと Worker が使う `target/release` は圧縮対象に含めていない。最終版の `cargo --offline bench --bench ipc_roundtrip -- --self-test` は成功し、`CARGO_NET_OFFLINE=true` を設定した `cargo --offline test --workspace` も更新試験・後続テスト・doc-test を含めて全体が成功した。前段の容量不足による失敗記録はこの再実行で解消している。最終版の clippy（警告禁止）、workspace fmt check、差分検査も成功。

実プロセスの smoke は W=N=1 と W=N=17 の2設定で実行し、それぞれ並列度1/4/16すべてが成功。要求失敗・未実行・終了失敗はいずれも0件。`IPC_ROUNDTRIP_SAMPLES=0` の実起動は期待どおり非ゼロ終了で拒否された。これら smoke は性能評価値としては採用しない。

以下は、圧縮・ビルド・テストの終了後、上記の再現コマンドで **W=1,000、N=10,000（各並列度の合計）** を指定した最終版の1試行。health/Hello に成功した Worker のみで測定を開始し、全3ケースで起動失敗・warmup 失敗・未実行・終了失敗は0件だった。時間は ns、events/s は成功した往復数/秒。

| 並列度 / Worker数 | 成功標本数 | 失敗件数 | p50 (ns) | p95 (ns) | p99 (ns) | p99.9 (ns) | events/s |
|---:|---:|---:|---:|---:|---:|---:|---:|
| 1 / 1 | 10,000 | 0 | 40,500 | 73,600 | 126,300 | 449,800 | 20,925.585 |
| 4 / 4 | 10,000 | 0 | 91,100 | 226,500 | 389,500 | 1,193,500 | 35,697.533 |
| 16 / 16 | 10,000 | 0 | 260,900 | 455,200 | 588,300 | 1,047,100 | 58,028.336 |

スループットの分母となる実経過時間は並列度1で477,883,900 ns、4で280,131,400 ns、16で172,329,600 ns。合計30,000件の計測すべてで期待した `Pong! 🦀` と空の counts/jobs 状態に完全一致した。並列度4/16の往復p99.9はこの試行で1 msを超えた。N=10,000のp99.9は上位約10標本の影響が大きく、複数試行による再現性や信頼区間は検証していない。

**Windows 開発機の参考値。1ms 未満目標との比較は Linux で要再測定。** Linux の片道p99目標、実 Gateway/Core・Manager 経路、実 Discord、障害・更新中の性能を達成したという保証は追加しない。

### 2026-11-12: プリミティブと failover 検知の追加測定

Windows 開発機（Windows NT 10.0.26200、AMD64 Family 25 Model 97、論理 CPU 12、Rust 1.99.0）。`src/` で `cargo bench --offline --bench primitives` と `cargo bench --offline --bench failover_detect` を実行。これは **Windows 開発機の参考値で Linux 本番の根拠ではない**。3秒は検知目標であり Discord 再接続を含まない。

プリミティブは current-thread Tokio、warmup 1,000 回、各 10,000 標本、失敗 0 件。各標本を `Instant` で計測し nearest-rank を使用、計測オーバーヘッドを差し引いていない。

| 処理 | p50 ns | p95 ns | p99 ns | 最大 ns |
|---|---:|---:|---:|---:|
| event_seq enqueue/replay/accept/ack、64 bytes | 100 | 100 | 200 | 500 |
| Counter inc | 0 | 100 | 100 | 3,600 |
| Histogram observe | 0 | 100 | 100 | 2,600 |
| MemoryStateStore put、64 bytes | 500 | 600 | 3,400 | 34,600 |
| MemoryStateStore get、64 bytes | 300 | 400 | 400 | 11,200 |
| TaskManager 登録・開始から完了確認 | 2,600 | 2,900 | 3,200 | 106,500 |

`event_seq` の直列サイクル数を各標本の計測時間の合計で割った値は 11,114,816 cycles/s。通信・IPC 往復のスループットではない。task の忘却は計測区間外。状態ストアは単一キー上書きで揮発性メモリのみ。0 ns は時計の分解能によるものでゼロコストを意味しない。各結果は単一実行であり、実負荷や競合下の性能、再現性を保証しない。

failover は loopback mTLS 2 ノード、current-thread Tokio、既定 heartbeat 500 ms / suspect 1.5 s / fail 3 s。各試行で 2 回送受信し、最後の受信を状態機械へ入力した直後から停止、5 ms 間隔の sleep と Tick で検知を観測した。50 回の逐次試行、成功 50、失敗 0。nearest-rank の p50/p95 と最大値を示す。測定はポーリング遅延を含む。

| 状態 | p50 ms | p95 ms | 最大 ms |
|---|---:|---:|---:|
| Suspected | 1,504.630 | 1,513.338 | 1,514.375 |
| FailoverPending | 3,008.664 | 3,014.664 | 3,015.272 |

正常 heartbeat 到着時に直前の Tick で判定した誤判定は 100 観測中 0 件。正常時の連続監視や長時間・負荷・遅延注入は行っておらず、実運用の誤判定率ではない。試験用の承認・起動完了イベントを与え TemporaryActive とした後、cooldown 経過後の heartbeat で FailbackWaiting に入り、1,518.890 ms 後に heartbeat 欠落を Tick で検出して TemporaryActive に戻った。古い FailbackComplete は無視された（1 試行）。30 秒の安定期間を通した正常フェイルバックは測定していない。

TLS は heartbeat 配送に使用し、状態機械への入力はベンチが手動で接続している。実 daemon、複数ホスト、quorum/lease 承認、実プロセス起動、Discord 再接続や復旧完了は測定していない。初期の証明書 fixture による起動失敗 2 回を修正後に上記を取得した。
