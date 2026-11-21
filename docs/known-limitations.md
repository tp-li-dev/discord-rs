# 既知の制約・未実装・未保証事項

- Core の長時間実行中のハンドラーは 5 秒間隔で確認し、30 秒以上の要求を警告します。同じ要求の再警告は 5 分以上空けます。これは観測のみで、実行期限の設定や要求の取消・再試行は行いません。

## 現行実装の補足

> [!NOTE]
> 2026-11-13の決定により、**対応OSはLinux(Ubuntu)のみであり、Windowsはサポート外**（開発機での動作は保証しない。Windows固有コードは残るが保証・CI対象外）です。Windowsでの検証結果はすべて『過去の参考記録』と位置づけられ、Windows ACL未実装などの未対応事項は未解決課題ではなく『サポート外』として扱われます。

以下の過去の記述に優先する。`cluster_runtime::bind_raft` は OpenRaft と mTLS Raft RPC を接続する試験経路だが、模擬Discordを必要とし本番起動は拒否される。`fs_perms` は Linux において開いたハンドルの Unix mode をファイル0600・ディレクトリ0700に設定・検証する。Windows 用パス API コードは残るが、Windows はサポート外（Windows ACL、ハンドル API、ディレクトリ保護は未解決課題ではなく『サポート外』）である。信頼されたパスを前提とし TOCTOU を完全には防がない。

`FrameworkStateStore` と `StateStoreAdapter`、ファイル永続 StateStore、`subscription`、`durable_jobs` がある。ローカルの部品や試験は複数ホストの耐久性や実Discord副作用の exactly-once を保証しない。`child_recovery` は daemon と Worker の復旧経路に接続されるが、任意の孤児再採用や全障害条件を保証しない。`metrics_feed` は存在するが daemon には未接続。

証明書の leaf 切替・失効、永続ポリシーと CA チェーンを扱う部品はあるが、配布・本番運用への自動統合は保証されない。`update_rollback` と daemon 更新制御は実装され、capability による role / shard scope と監査を用いるが、実行できる更新には下記の制約がある。

`src/tests/update_load.rs` の `continuous_events_during_updates_and_core_crashes` は有効。`crash_old_core` は送信を一時停止し、それ以前の結果と Core 受領確認が揃ってから旧 Worker を落とす静止状態の障害試験に変更した。欠落ゼロの基準は維持するが、処理中の強制終了について欠落ゼロを保証する試験ではない。処理中の障害は別ケース `crash_old_core_busy`（`old_core_receipt_failure_keeps_gateway_running`）に分け、WSL／CI で安定性と総実行時間 30 秒未満を確認するまで `#[ignore]` とする。既存の `continuous_events_after_handler_outcome_unknown` も引き続き `#[ignore]`。今回の変更は未実行。実Discord smoke CLI（`src/src/bin/discord_smoke.rs`、`src/tests/discord_smoke/cli.rs`）の実接続はユーザーが実施する検証範囲。`stress.yml` と `bench.yml` は手動ワークフローである。[progress.md](progress.md) は過去の Linux CI 成功を記録するが、Windows はサポート外となったため Windows Actions 再確認は対象外。

2026-11-12時点の実装と [progress.md](progress.md) に基づきます。[architecture.md](architecture.md) §16.2 の全条件は未達です。個々の条件と試験の根拠は [conformance.md](conformance.md) を参照してください。模擬試験や独立した部品の成功は、実Discord・実ネットワークでの保証ではありません。実Discordでの検証はユーザーが実施する範囲です。

## 実行構成・配置・分散制御

- 第12回監査への対応（未ビルド・未試験）：`--shard` の強制停止で選択外の操作を取り消さず、操作の照会は保存された対象シャードの scope で認可する。`--guild` は取得した `total_shards` を前提条件として送り、不一致なら変更前に拒否する。ただし同じ総数の別構成を識別する設定世代の固定ではない。
- `--all` は最初の `ps` の一覧を使い、Bot ごとの操作完了を待って最初の失敗で止まる。範囲限定の principal は対象を指定しない `ps` 自体を許可されないため、`--all` は変更前に失敗する（見える Bot だけに絞った一覧は未実装）。bootstrap は一度だけ読み、部分失敗の操作スナップショットと `shards`／`failed_shards` を保持する。`stop --all --force --dry-run` は確認なしで一覧を表示できるが、実際の強制停止には `--yes` が必要。
- 第13回監査への対応（未ビルド・未試験）：`--all` は全 Bot の一覧（非ローカルも含む）に対し、名前、完全 ID の順で一意に解決できる参照を選ぶ。選べない Bot があれば最初の変更前に拒否するため、ID 接頭辞とも衝突しない名前へ変更する必要がある。各変更に `expect_bot_id` を送り、daemon は参照先の ID が変わっていれば開始前に拒否する。古い daemon は未知のフィールドとして拒否する。ID が同じままの設定変更を固定する機能ではなく、途中失敗時に完了済み Bot を巻き戻すこともない。実験的な `--allow-test-gateway-switch` は `shards`・`expect_total_shards`・`expect_bot_id` を未対応として拒否する。
- `gateway-update` のクライアント待ち時間は既定 300 秒で、4 シャードの完了を保証しない。サーバー側の期限後にも最大 20 秒の有効化確認と接続・ポーリングの余裕が必要になり得る。長い更新は `--timeout` で延長する。クライアントの timeout は daemon の操作を取り消さないため、操作 ID で結果を照会する。

- `botd` は設定された Bot/Shard ごとに Gateway 子プロセスを起動・監視し、各 Gateway は Worker プールを所有します。明示的な模擬Discordモードでは複数Bot・Shardの別プロセス起動と隔離を試験しています。実Discordでの複数Shard稼働は、WSLで2 Shardを1回確認しただけで（更新・障害・`--shard`・`--guild`・`--all`。`docs/progress.md` 2026-11-16 夜）、長時間・繰り返し・3 Shard以上は未検証です。プロセスが存在することは Discord Ready、所有権、lease の健全性を意味しません。
- `botctl` はローカル daemon の ps/status/inspect/shards/start/stop/restart/operation とオフライン設定検証に対応します。update/rollback は既定で成果物をハッシュ確認するオフライン計画、`--yes`（`--bootstrap-stdin` または `--secret-file`）で認証付き要求と操作IDによる再照会を行います。systemd 常駐の botd へは、管理 secret をファイル（`botd --secret-file ABS [--control-port PORT]`、`botctl --secret-file ABS --port PORT`、`botctl secret generate --out ABS` で作成）で渡して接続できます。ファイルは 64 文字の 16 進 1 行で、通常ファイル（シンボリックリンク・追加のハードリンク不可）・モード 0600/0400・所有者が実効ユーザー（root 所有は 0400 かつ `CREDENTIALS_DIRECTORY` 配下のみ）を検査し、満たさなければ拒否します。この検査と systemd 実機での常駐接続は Linux の実機では未検証（CI の試験は模擬環境）で、途中ディレクトリのシンボリックリンク・POSIX ACL・ファイルシステムの強制は検査せず、secret のローテーションには botd の再起動が必要です。読込バッファの消去は最善努力でメモリ全体からの消去は保証しません。実行成功に対応するのは明示的な模擬Discord Gateway-switchモードの Gateway 更新のみです。通常モードと Core/all は必要な制御契約が未接続のため拒否します。logs、nodes、doctor 等の daemon 操作は未実装です。操作履歴は揮発性で、daemon再起動後の子プロセスの自動引継ぎ（孤児の再採用）は未実装です。残存予約は、Linux で `/proc` の走査によりそのShardのGatewayが生きていないと確認できた場合だけ自動削除し、それ以外（生きている・判定不能・Linux以外）は従来どおり拒否します。`botd --stop-children-on-exit`（任意）で SIGINT／SIGTERM 時に子をgracefulに停止します（既定オフ。期限30秒、強制終了はしない）。これらはローカル未ビルドでCI検証待ちです。
- 配置検証・Shard起動仕様・Identify予算・更新ゲートはありますが、複数ホストへの実配置、設定世代同期、プロセス間・ホスト間でのIdentify予算共有は未完成です。実Discordの推奨Shard数や制限への追従を包括的に検証していません。
- ownership、lease、watchdog、failover/failback には状態機械や模擬時計・模擬ネットワークの試験があります。本番のプロセス起動・Discord送信・quorumとの統合は未完成です。botd 既定経路は systemd `Type=notify` 向けに `READY=1`（制御経路 listen 開始後）・`WATCHDOG=1`（`WATCHDOG_USEC` の半分間隔、監視ループの tick 完了後のみ）・`STOPPING=1` を送りますが（`src/sd_notify.rs`）、テスト用クラスタ/Raft/Gateway 切替モードは未対応で、実 systemd での検証は未実施です。3秒検知・30秒安定確認は設定上の目標で、実復旧時間の保証ではありません。実験的 Raft ストアには epoch 永続化がありますが、本番統合や OS suspend 時の時計安全性は未検証です。
- `cluster.rs` の過半数ACKは試験用シミュレーションであり本番合意ではありません。メインworkspaceの `raft_quorum` は OpenRaft 0.9.26 を用いる実験的な固定メンバー構成で、永続予約・epoch CAS とローカル admission gate を備えます。試験用cluster runtimeとmTLS Raft RPCに接続されますが、本番起動は拒否されます。本番fencingは未検証です。保持予約は明示的な解放が必要で、クラッシュ後の自動引継ぎや強制解除はありません。独立スパイクの評価は [quorum-evaluation.md](quorum-evaluation.md) 参照。実分断・quorum喪失下での二重Active防止を本番環境で保証できません。
- Gateway並行切替には状態機械に加え、明示的な試験フラグと模擬Discordを使う botd → Gateway → Core のローカルプロセス切替経路があります。実Discord上の新旧WebSocket接続・Shadow・Active移譲・旧REST結果保持、本番 lease との統合は未検証です。この試験経路は production_ready ではありません。
- `botd --core-mode separate`（opt-in。既定は `embedded`）は Linux 限定で、`--stop-children-on-exit` を必須とし、試験用の quorum・raft・Gateway 切替の指定とは併用できません。検証は WSL の実プロセス試験（`botd_separate_cores`、模擬 Discord）のみで、実 Discord での 1 Shard と 2 Shard の確認（WSL。2 Shard は 1 回のみ）、Oracle での systemd 常駐の放置（約 18 時間）は記録がありますが、長時間・繰り返し・3 Shard 以上は未検証です。
- separate では Core の無停止差し替え（ホットスワップ）は未対応です。Gateway の `--core-dir` では Worker 更新の監視を行いません。
- separate の `botctl gateway-update` による Gateway の無停止差し替えは、WSL と実 Discord で 1 回の更新（成功、カウンタ継続、重複 5 件の破棄）を確認しました。複数 Shard の本番運用、長時間稼働（soak）、Interaction の重複は未検証です。activate 送信後に失敗した場合の Core と旧 Gateway の状態も未検証です。
- `gateway-update` の既知の限界（2026-11-16 夜の Codex（`gpt-6.1-sol`）レビューによる。コードで確認した指摘で、実機での再現はしていない）：
  - 待機 Gateway の起動から確定／有効化前の中止まで、同じ Bot の他 Shard の自動起動・復旧を延期して復旧用の枠を保護する。保護中に届いた同じ Bot の start / restart / gateway-update は管理操作の受付時点で拒否する。有効化後に失敗した場合も対象 Shard の復旧許可まで保護し、通常の間隔・回数制限を適用する。停止や復旧の最終失敗でも保護を解除する。別 Bot は独立した予算で起動できる。
  - 第7回の監査ファイル修復（未試験）：検証済みの read+append 記述子で末尾から 8 KiB ずつ改行を探す。不完全な末尾は最大 64 KiB だけ `.corrupt` に退避し、元ファイルは最後の改行まで切り詰める。`.corrupt` は 1 MiB まで追記し、上限による退避省略は固定文言で警告する（既存の上限超過ファイルは縮めない）。コピー済みの位置と記述子は botd のメモリに保持し、同期・切り詰め失敗で同じ断片を繰り返し追記しない。保留終端監査の再試行が失敗した後は、次の再試行まで 1 秒以上空ける（初回の再試行は直ちに行い得る）。同期だけの再試行も保存位置のバイト列を確認する。修復状態は再起動で失われ、複数の書き手や外部ローテーションとの排他はない。同期 I/O 自体の待ち時間は依然として制限していない。
  - チェックポイント読込の残る競合：`core-state.json` は 0400 のファイルも読めるよう read-only で開く。事前の `symlink_metadata` と、開いた記述子の通常ファイル種別・サイズ・`(dev, ino)` を検査する。ただし事前確認と open の間に、状態ディレクトリへの書込権限を持つローカル主体が FIFO に差し替えると、open 自体が待ち続け得る。後段の記述子検査や採番ロックの 5 秒期限では防げない。この狭い競合は未解消。監査ファイルは検証済みの read+append 記述子を再利用し、読込のための別 open を廃止した。
  - 有効化の確認（対応済み）：botd は `activate` を送ったあと、候補が全 Core に attach したことの確認（`standby-active`、最大 20 秒）を待ってから、監督を移して旧 Gateway に停止を要求する。確認できなければ操作は `failed`。このとき旧 Gateway は既に Core から外れているため戻せず、kill して botd が上がった epoch で復旧する。復旧の起動は次の Identify 許可を待つ。READY のマーカー（`standby-ready`）は Discord 切断後も残る。確認が来ない場合の自動試験あり（`an_update_whose_standby_never_confirms_fails_and_the_gateway_recovers`：操作は `failed`、旧 Gateway は終了、同じ Core に Gateway が戻り、監査に `requested`／`failed` が残る）。
  - 進行中の応答：Core が接続を切り替える（fence する）とき、旧 Gateway が HTTP 送信中の応答の `ApiResult` は Core に届かないことがある（結果不明の扱い）。更新前に新規受付を止めて決着させる処理は未実装。
  - 再接続の止め方の限界：退役中の Gateway は、停止要求を受けた時点（イベントループの終了後）で再接続を止める。バックログの再生が枠待ちの間は停止要求が反映されない。すでに送った復元は取り消せない（Core 側の epoch による fencing が要る）。未対応（再監査 第 4 回の指摘 1・3）。
  - 2026-11-16 の Astra Max 監査で見つかり、未対応のもの（詳細は [audit-2026-11-16.md](audit-2026-11-16.md)）：保留イベントの再生中はシャードを poll しないため、保留が 64 件を超え処理が遅いとハートビートが止まり得る（仮説、実機で確認する）／`gateway-update` の繰り返しで再起動の予算（1 時間に 10 回）を使い切ると、失敗時に Shard が止まり得る／監査の同期 I/O とログのコピーが botd の監視ループ内で走る／終端の監査が書けなくても成功を返す／候補のログと監査ファイルを開くときにシンボリックリンクと既存の権限を検査しない／`!workers` は重複排除の対象外。
  - 応答の書き込み期限（対応済み）：ソケットの相手（分離した Core に接続した Gateway）への応答の書き込みは 3 秒で打ち切る（置換 Gateway の最初の Health の 5 秒より短くするため）。相手が応答を読まなくなった場合に限り、新しい epoch の引き継ぎは最大約 3 秒の遅れで済む。Core の要求処理そのものが止まっている場合（外部の依存先に待たされるハンドラなど）は、Core 側に要求の期限がないため引き継ぎも復旧もできない（再監査の指摘 4、未対応）（接続は切れ、Core の状態は保持される。自動試験あり）。標準入出力の Core（embedded）には期限を付けていない。
  - ログ（対応済み）：`gateway.log`・`gateway-candidate.log` は 8 MiB、`core-*.log` は 4 MiB を超えると、稼働中でも約 60 秒以内に `.1` へ写して空にする（書き手は追記のまま続く。`.1` は 1 世代のみ。`.1` の位置がシンボリックリンク等なら何もしない）。
  - 状態ファイルの競合は一部対策済み：書き手が起動するたびに、ファイルの値より必ず大きい「印」（起動時刻のナノ秒と、ファイルの値+1 の大きい方）を取って `core-state.json` に記録し、小さい印の書き手は上書きしない。後から起動した Gateway が先に起動していた Gateway に、通常は勝つ（同じナノ秒の起動と、ファイルの印が将来の値のまま時計が戻った場合は、別プロセス間で衝突し得る。同一プロセス内では単調増加。再監査の指摘 1、残る限界。Astra Max 再監査で、最初の案（epoch／botd の起動時刻）がモードの切り替え・時計の逆戻りで保存を止める点を指摘され、この方式に変えた）。確認と rename は別プロセス間で不可分ではない（ロックではない）。永続化を Core 側の単一の持ち主にする設計は未実装。
- `gateway-update` は、他の変更系コマンドと同じ role／scope の認可（`admin` かつ操作対象の全 Shard が scope 内。指定がなければ Bot のローカル全 Shard）を通ります。実行の要求と結果は `runtime-root/gateway-update-audit.jsonl`（0600、追記のみ、1 行 1 JSON：時刻・event（requested／rejected／succeeded／failed）・操作 ID・Bot・subject・role・固定のエラー種別。secret・パス・引数は含まない）に残り、記録できない要求は開始前に拒否されます。ただし完了の記録は fsync 済みでも、`update-audit.jsonl` のような単一ライターの検証付き監査（改ざん検知・世代付きローテーション）ではなく、ローテーションもありません。単一の bootstrap secret の既定 role は `admin` なので、secret を持つ者は実行できます。
- 重複排除は Core のメモリ上で直近 4096 件の Discord message id を見るだけです。Core の再起動をまたぐ重複は検出しません。スラッシュコマンドは Discord の単一 ACK 規則（処理より前に ACK を送る）で重複を防ぐ想定ですが、実 Discord での効果は未検証です。
- separate で botd が死亡すると Core も失われます（Core は stdin の EOF で終了）。botd の SIGKILL 時の後始末は未検証で、試験は SIGTERM による停止のみです。
- separate の Core 切断は、要求の失敗で検知する記録です。待機中の切断の検知、再接続後のイベント連番・未確認イベントの扱いは未検証です。再接続は 100 ms から最大 5 秒の指数バックオフです。
- botd が起動し直した空の Core へは、Gateway の確認済みスナップショット（`core-state.json`）を復元します（模擬の試験のみ）。確認済み以降の変化は失われ得ます。実 Discord での `!count` 継続は未確認です。

## Worker更新・配送・外部副作用

- 第11回への対応（未ビルド・未試験）：模擬 Discord の各イベントで `ApiResult` の受領確認が失敗した場合は、`outcome=unknown`・`receipt=null` を返し、受信ループを終了させない。Manager に残る未解決の記録は消さず、正常終了を拒否する保護も維持する。実 Discord の経路は変更していない。busy 障害試験は HTTP 応答をゲートで止めて旧 Worker を落とし、不明結果、重複送信なし、Gateway の PID／世代継続、その後の通信、正常終了の拒否を検査する。未解決の旧世代が残るため、対象スロットの切替後もプール全体の更新完了は要求しない。Python fixture は 25 秒の watchdog を持つが、Rust 側の fixture 準備を含む総時間は未計測。

- `manage` の更新は Gateway が所有する Worker プロセスの更新です。Gateway 自体の更新や、daemonからのShard単位ローリング更新ではありません。対応 capability を交渉した Worker は旧世代の処理・応答を保持しつつ新世代へ配送します。旧版との互換経路は直列切替です。任意のアプリ状態や Future は移行せず、snapshot 対象も限定されます。複数Workerの途中失敗では新旧混在があり、全体の自動rollbackはありません。
- 結果不明の外部操作は自動再送しません。旧世代を保持して更新を失敗または未完了とする場合があり、結果不明の追跡は親の再起動で失われます。照合APIと読み取り用管理操作はありますが、永続監査や解除CLIはありません。reload の確認timeout後に処理が進む可能性もあります。
- IPC の連番・ACK・再配送は対応 capability を交渉した限定経路です。全Dispatchの永続配送、セッション横断の欠落検出・重複排除はありません。過負荷ではメッセージ破棄やInteractionのACK失敗が起こり得ます。
- 実DiscordでのInteraction期限、429/5xx、接続断・取消競合、旧Coreへの実REST結果帰属は未検証です。外部副作用にはアプリ側の冪等性と照合が必要です。exactly-once、完全無欠落、グローバル順序、障害を含む無停止は保証しません。

## API・状態・IPC

- フリー関数の型付きprefix/Slashとoption schemaはWorker経路に接続されています。Cogの型付きメソッドやライフサイクルはframework試験段階で、実Workerの全ライフサイクルには未接続です。引数型・引用・権限・クールダウン等には制限があります。
- Button/Select/Modalの名前空間付きルーティング、重複検出、waitのキャンセル規約はframework側にあります。`FrameworkStateStore` は framework の状態操作を `StateStore` に接続しますが、Workerのコンポーネントとローカルファイルを使う試験がありますが、実Discord配送統合や複数ホストでの耐久性は未保証です。汎用イベントやContextの全設計APIも未完成です。
- StateStoreはTTL・容量・CAS・スキーマ等を持つ非同期APIとメモリ・ファイル永続化実装があります。StateMap、View/Modal状態、チェックポイント等がプロセスやホスト障害を越えて保持される保証はありません。
- Gateway が `--state-path` 配下の `core-state.json` に確認済みスナップショット（snapshot-v3 の `!count`／`!job` のみ。framework の StateStore とは別）を保存し、Gateway／botd の再起動後に復元します。保存は変化時のみ・最短2秒間隔・原子的書き込み・0600・1 MiB 上限で、停止時は最終状態を書きます。`kill -9` 等では直近およそ2.5秒分の変化が失われ、イベントジャーナルとアプリのStateMap/Cog状態は保存されません。保存失敗や破損・不一致ファイルは警告または無視（空の状態で起動）で、`--state-path` なしでは何もしません。ローカル未ビルドでCI検証待ち、実Discordでの確認も未実施です。詳細は [operator-guide.md](operator-guide.md)。
- TaskManager/Supervisorは子タスク追跡、取消、再起動予算、drain等の基盤があり、Workerの一部に接続されています。Cog、IPC、全資源を含む完全なdrainや障害復旧は未検証です。
- 実Worker IPCはstdin/stdoutの改行JSON、フレーム上限1 MiBです。既定経路の実Worker IPCは上記のstdin/stdoutです。opt-in の `--core-mode separate` だけが Unix socket 接続型（能力 secret と SO_PEERCRED 付き）を使います。MessagePack、Named Pipe、制御・データ経路分離、snapshot分割は採用していません。Helloのidentityとcapability交渉はありますが、型の存在だけで全経路の認証・fencingを保証しません。[ipc-protocol.md](ipc-protocol.md) 参照。

## 管理・セキュリティ・観測性

- daemon 更新の capability は匿名stdinの信頼済み起動設定で Principal・role・任意の Shard scope に結び付けます。従来の `secret` のみの起動設定はローカル管理者権限を持ちます。接続クライアントからの role/subject/scope は受理しません。範囲限定capabilityの集約閲覧は制限されます。部分Shardだけを許可した場合、Bot全体の inspect が拒否されCLI事前確認を通せない場合があり、範囲内の型付き直接要求とは区別してください。
- 更新監査は最初の更新要求時に runtime-root の `update-audit.jsonl` を開き、開始と完了（または認可拒否）を記録します。開始監査失敗時は実行せず、完了監査失敗時も成功を返しません。操作IDに対応する内容と終端結果は daemon 生存中だけ保持します。異なる内容での再利用や既存の起動停止操作IDとの衝突は拒否しますが、daemon再起動後の永続的な冪等性や実行結果の自動照合はありません。secret、要求本文、成果物パスは監査へ保存しません。
- 監査とハッシュ処理は daemon の監視ループ外で実行します。監査の単一writerが更新完了まで順に処理するため、Identify予算待ちの操作は他Botの更新・拒否監査を待たせます。statusと監視は継続しますが、更新取消は未実装です。模擬Gateway actor内の再ハッシュ待ちは5秒で打ち切り、遅れて完了しても起動しません。ただし同Shardのactor進行はハッシュ・Ready・drainの各待機中に保留されます。別タスクの制御heartbeatや他Shardの進行と区別してください。
- 成果物のハッシュ照合は起動用ファイルハンドルの固定ではありません。照合から起動までのファイル差替えを完全には防がないため、成果物と親ディレクトリは信頼できる管理者だけが変更できる前提です。Windows ACL はサポート外（Windows非対応）。署名・Linux本番更新・実Discord（ユーザー検証範囲）・永続Identify予算・分散quorum/leaseによる更新は未検証です。
- `botd` と `manage` はローカルloopbackの認証付き管理経路です。RBAC・監査・metricsの部品と管理経路の試験があり、manage はローカル監査ファイルを使用します。遠隔管理サービス、全経路の永続監査、サービス登録や本番向け認証情報運用は未完成です。mTLS Transportは試験用daemon間通信とRaft RPCに接続されていますが、本番quorumは起動できません。証明書失効・leaf切替、永続ポリシーとCAチェーンの部品はありますが、本番運用への自動統合は未保証です。既に受理されたメッセージは撤回できません。
- Workerとビルド子へのBot token継承を防ぐ処理、秘密マスク、フレーム上限等の試験があります。全ログ・子プログラム・ファイル権限を通した秘密保護は包括的に検証されていません。`fs_perms` は開いたハンドルの Unix mode をファイル0600・ディレクトリ0700に設定・確認しますが、所有者、ACL、親ディレクトリ等を検証しません。Windowsのファイル用パスAPIコードは残りますが、Windowsはサポート外（Windows ACL、ハンドルAPI、ディレクトリ保護は未解決課題ではなくサポート外）です。成果物の署名・検証も未完成です。[security.md](security.md) 参照。
- Prometheus形式metricsとmetrics_feedはありますが、feedはdaemonに未接続です。分散trace、完全な健全性診断、全経路の観測性は未完成です。statusはDiscord接続や所有権の証明ではありません。registerは1件upsertとdry-runで、自動差分同期はなく、HTTP失敗やtimeoutは結果不明になり得ます。

## 検証の範囲

過去の参考記録: 今回の更新制御は Windows / Rust 1.90.0 で関連235件（対象別の再実行重複を除く）が成功しました（Windowsはサポート外であり、開発機での動作は保証しない。Linuxが対象）。対象は lib、botd、botctl_update、botctl_update_control、botctl_update_daemon、botctl_daemon、update_control_rbac、update_rollback、e2e_gateway_process_switch、gateway_admission_process、e2e_shard_runtime、e2e_shard_worker です。`cargo --offline fmt --all --check` と `cargo clippy --offline --lib --bin botd --bin botctl --tests -- -D warnings` も成功しました。実プロセス試験は模擬Discordのみで、更新中のstatus応答・別Bot継続を確認したものであり性能ベンチマークではありません。workspace全体、Linux、実Discord（ユーザー検証範囲）、本番更新は今回未検証です。

[progress.md](progress.md) に記録されている Windows / Rust 1.99.0 での過去のworkspace検証は『過去の参考記録』です。統合直後にT04が一度失敗し、再実行で成功、fixture の成果物選択も修正されたと記録されています。これらは追加後の現コードの再検証結果ではなく、安定性は引き続き要確認です。現統合版の本番Linux、実Discord（ユーザー検証範囲）、複数ホスト・実ネットワーク、OS suspendや本番合意の検証は確認されていません。

[benchmarks.md](benchmarks.md) はWindows開発機の過去の参考記録とユーザー提供のLinux共有CI記録（Actions ubuntu-latest、IPC往復 p99 = 115.5/258.1/973.6 µs、failover検知 Suspected p50 1503.6 ms / FailoverPending p50 3003.1 ms、共有ランナーで本番保証ではない、往復値で片道p99の直接測定ではない）を含みます。実Discord検証はユーザーが実施する範囲です。Linux本番性能、負荷時Heartbeat、RSS/Shard、1/4/16/64 Shard、更新・Failover遅延は未検証です。個々の測定を仕様全体の性能目標達成の証拠にはしません。

## 実プロセス統合試験の不安定さ（解決済み、2026-11-14）
- 実プロセス（botd、Gateway、Worker、模擬 Discord）を複数起動する統合試験は、GitHub Actions（Ubuntu、4 vCPU）で確率的に失敗していた（`e2e_gateway_process_switch` ほか。20 回の反復で 7 回失敗）。
- 原因：mock の Identify 予約器（`MockIdentifyScheduler::reserve`）が、各 Shard が『ロック取得前に採った時刻』を受け取り、他 Shard の予約より前の時刻だと『時刻の逆行』としてエラーを返していた。同じ Bot の複数 Shard が同時に起動すると、負けた Shard が `IdentifyUnavailable` として永久に `Degraded` になり、Gateway が接続してこなかった。
- 修正：時刻を単調に丸める（`max(now, last_now)`）。再現の単体試験を追加（`tests/gateway_switch.rs`）。修正後、Ubuntu で 20 回の反復がすべて成功した。
- 注意：この競合は mock の予約器の問題だが、実運用の Identify 調整（共有予算）を実装する際も、時刻のサンプリングと直列化の順序に同じ落とし穴がある。判定の時刻は、直列化の内側で採ること。
- **未解決（低頻度）**：`raft_botd_processes` の `raft_botd_death_persistent_rejoin_and_automatic_failback` が、強制終了した botd の再起動（rejoin）で、`Raft cluster configuration, storage or mTLS invalid` により起動に失敗したことがある（Ubuntu の CI で 40 回の反復中 1 回。その後の 20 回は全件成功）。当時は失敗の種別が 1 つの文言に丸められていたため原因は不明。診断のため、種別ごとの固定文言（秘密なし）を出すよう変更済み。次に再現したら、その文言で原因（InvalidConfig、Transport、Coordinator など）を特定する。この経路は試験用（模擬 Discord 限定、本番では起動拒否）。

## 実 Discord 経路の事前レビュー（2026-11-14、静的レビューのみ。実接続は未実施）
実接続（`--mock-discord` なし）のコードを、実行せずに読んで確認した結果。アカウント停止・BAN・token 漏洩につながる致命的な問題は見つからなかった（token は環境変数のみから読み、`Secret` で伏せ、Core/Worker には渡らない。認証失敗で反復ログインしない。429 は twilight-http が遵守、5xx で非冪等 POST を再送しない。`register` は既定 dry-run で、1 コマンドの upsert のみ）。以下は、実機で確認すべき点と、未対応の項目。

- **対応済み**：致命的な切断（4004、4010〜4014）で理由を出さず終了していた → close code（数値のみ）と、再接続しない旨を標準エラーに出すようにした（`main.rs`。終了コードは変えない＝botd の再起動ループを避けるため）。初回は Gateway を単体で起動して出力を見る手順を `real-discord-verification.md` に追加（H1・H2 対策）。
- **高（手順で回避。コードは未修正）**
  - H1（実装済み、CI 検証待ち）：botd は Gateway 子の stdout/stderr を Shard 状態ディレクトリ直下の `gateway.log`（0600。シンボリックリンクなら null へフォールバック。起動時に 1 MiB 超で `gateway.log.1` へ 1 世代ローテーション。世代ごとに区切り行）へ追記する。journald には出ない。ローカル未ビルドで、実環境での確認は未実施。
  - H3：Gateway は `GUILD_MESSAGES | MESSAGE_CONTENT` を固定で要求する。MESSAGE_CONTENT は特権 Intent で、Developer Portal で有効化しないと 4014 で拒否される想定（実挙動は未検証）。Slash だけなら Intent 0 でも動作するはずだが、Intent を設定可能にする変更は未実施。
- **中（未対応）**
  - M1（**実装済み・CI 検証待ち**）：Interaction は先に Defer の ACK を送り、ACK 確認後に `begin_response_delivery` を行う順序へ変更（`main.rs`。`interaction_flow::run` の状態機械と `Outcome` は無変更）。begin は 10 秒の期限付きで、失敗・期限切れ時は固定文言で `update_response` する。ACK 失敗時は begin しないため Worker への副作用もない。ローカル未ビルド。
  - M2（**実装済み・CI 検証待ち**）：同時 64 件超、未リンク／非 ChatInput のコマンド、channel なしは、ephemeral の短い応答（interaction callback、1.5 秒の期限、失敗しても panic しない）を返す。過負荷時も新しいタスクは増やさず、イベントループ内で期限付きに送る（最悪でも 1 件 1.5 秒の背圧）。**未対応のオプション型（`slash_input` の `_`）は未区別**で、「このコマンドは使えません」の応答になる。ローカル未ビルド。
  - M3（**実装済み・CI 検証待ち**）：純関数 `clamp_reply`（2000 文字超は 1997 文字＋`…`、空白のみ・空は固定文言）を prefix 応答・`!workers`・`update_response` の全送信で通す。単体試験あり（境界 2000/2001、マルチバイト、空）。Discord の実際の文字数判定との差は未検証。ローカル未ビルド。
  - M4：401 とネットワーク失敗が区別されず、無効 token でも supervisor の再起動（有界、最大 8 回、バックオフ 1〜30 秒）のたびに `/gateway/bot` を叩き直す。401 は再起動対象外にする専用の終了コードが望ましい。
  - M5（**実装済み・CI 検証待ち**）：Bot 発の全送信（prefix の `create_message`、`!workers`、`update_response`、ephemeral 応答）に `AllowedMentions::default()` を明示（メンション解析なし）。follow-up は現状使っていない。実 Discord での挙動は未検証。ローカル未ビルド。
  - M6：token の環境変数名が不一致（Gateway と `register` は `DISCORD_TOKEN`、`discord_smoke` の既定は `DISCORD_TEST_BOT_TOKEN`）。手順書に明記した。名前の統一は未実施。
- **低**：再接続バックオフにジッターなし（上限約 255 秒、回数の上限なし）。Identify 枠が尽きていると起動後に最大 `reset_after`（最長 24 時間）黙って待つ。単体起動は SIGTERM 未処理。DM の prefix コマンドは動かない（DIRECT_MESSAGES Intent なし）。`register` の説明文が固定。Gateway 子の環境に token があり、`/proc/PID/environ` から同一ユーザーまたは root が読める。HTTP プロキシ非対応（twilight 側）。
- **実 Gateway の起動許可**：単一 botd 内で、初回・手動・更新の待機起動・自動復旧が同じ Bot 単位の上限（許可間隔 30 秒、直近 1 時間で最大 10 回）を共有する。起動後の再 Identify や複数 botd・複数ホストの実送信は対象外。botd を再起動すると許可履歴は消える。 [設計と未決定事項](design-decisions/09-identify-scheduler.md)
- **未実装（再掲）**：プロセス再起動をまたぐ Resume（session はメモリのみ。再起動は毎回新規 Identify）、複数プロセス・複数ホスト間の Identify 調整（プロセス内のみ）。

## P3 の Core 手動再起動

- 手動再起動が終了を確認するのは botd が直接所有する Core のみです。Core の子孫やプロセスグループ全体の終了には別途、世代単位の管理が必要です。終了を観測できなければ `termination_unconfirmed` を返し、同じ botd が終了を確認するまでそのスロットの子を保持します。
- `succeeded` はプロセスの起動と IPC capability の書き込みまでです。ソケット待受、認証付き再接続、アプリケーションの処理再開は別の確認が必要です。Core 再起動による受信済みイベントの再配送には保証がありません。未受領の REST 結果は不明になり得ます。Core の `SeenIds` は空になり、対象は source ID を持つ `Dispatch` です。もう一方の Core も Gateway の配送許可枠を共有しています。
- 回数制限と操作 ID の再構成は同じ runtime root の監査ログを保持する前提です。ログの全行を起動時に監督ループ外で読みます。自動ローテーションはありません。別の OS 起動や終端記録の欠落などで経過時間が不明な試行は、再生時点から最長 15 分間、回数制限に含まれます。矛盾または不正な完全行を検出した場合は、P3 の新規操作を停止します。プレリリースの開発ビルドで書かれた監査ジャーナルはサポート対象外で、その履歴では P3 を無効のままにします。
- 置換の起動は 5 秒で打ち切り、シャードの操作予約を解除します。遅れて起動した直接子は capability を渡さず終了・回収します。同じ Core スロットは起動結果と直接子の回収を確認するまで再利用できません。通常の Gateway 起動に伴う Core 起動、Core の自動再起動、ログローテーション、P1 と peer の状態観測には同期 I/O が残ります。
- 通常停止では取り消した置換の結果と直接子の回収を最大 2 秒待ちます。起動スレッドが子を作成した後、ハンドルを botd に引き渡す前に停止待ちが切れるか botd が強制終了すると、stdin の EOF を無視する直接子が残り得ます。引き渡し後も、強制終了を要求した子の回収を期限内に確認できない場合があります。起動スレッドの終了でも発火する Linux の親死亡シグナルは、この起動経路に設定していません。
- 制御監査の停止待ちは最大 2 秒です。ストレージが応答しない場合や botd が強制終了した場合は、終端記録が未確認のままになり、要求のみが残る操作の照会は `unknown` になります。拒否の記録失敗やキューのあふれは `status` の `failed_or_dropped` に反映されます。
- 制御は既存の認証付きループバック TCP です。認証失敗は接続処理で拒否されます。`botctl core restart` は Core 復旧プロトコル v2 を使い、対象シャードの `admin` が必要です。復旧専用 role は未実装です。
- CLI の待機期限が切れても送信済みの操作は続く場合があります。表示された ID で照会できる範囲は、botd が保持する操作結果と復元できた監査履歴です。CLI と結合テストの Ubuntu／WSL での実行、対話端末での確認、実際の botd 強制終了をまたぐ操作照会、systemd、実 Discord での処理再開は未検証です。

## P1 停滞診断（Core の状態ファイルと、botd からの照会）

**状態：** 実装済み（ブランチ work/p1）。**実機（実 Discord の別 Bot、WSL、2 シャード）で動作を確認した。Oracle・実サーバーの systemd では未確認。** 設計は [DDR-0007](design-decisions/discussion-records/0007-p1-tmpfs-and-cutover-order.md)、[p1-stalled-diagnostics-design.md](design-decisions/p1-stalled-diagnostics-design.md)。

**何をするか：** 分離モードで、Core が要求の要約（未完了の数、最も古い経過秒、警告の件数）を、**tmpfs 上の固定長 64 バイトの状態ファイル**に、**同じ位置へ上書き**で書く（アイドルなら 10 秒おきの生存の印、要求があれば 5 秒おき）。botd がそれを読み、`botctl status` の各 Core に `status`（`pending`、`oldest_age_s`、`status_age_s`、`seq`、`generation`、`suspected_stalled`）を足す。**停滞の「疑い」**（記録が 30 秒以上更新されない）であり、原因や完全な固まりの証明ではない。表示できないときは、`status` が `null` で、理由（`unsupported`＝ファイルなし、`unreadable`、`corrupt`、`stale_generation`、`regressed`、`clock_unavailable`）を `status_reason` に出す。内容に、要求の内容・ID・ユーザー情報・秘密は含まない。botd は Core のソケットに**接続しない**（高い epoch の接続が Gateway を締め出すため）。

**有効にする方法（オプトイン）：** `botd --core-status-dir <絶対パス>`（tmpfs のディレクトリ。本番は、systemd の `RuntimeDirectory=discord-rs` が作る `/run/discord-rs`）を**明示的に指定したときだけ**有効になる。**`/run/discord-rs` があるだけでは、自動で有効にしない。** 理由：botd だけを先に更新すると、古い Core が新しい引数を拒否し、Core の起動・復旧が壊れるため（Astra 第 20 回の指摘 1）。**診断を使う運用では、Core のバイナリ（`src`）を先に更新してから、`--core-status-dir` を指定する。** tmpfs でないディレクトリを指定すると、**何も書かず**、機能は無効になる（ディスクには書かない）。

**確認済み（実測）：**
- ディスクへの書き出し：アイドルの Core で 300 秒、**`write_bytes` が 0**（WSL、ext4、デバッグビルド）。旧方式（ディスク上のファイル）は 98,304 バイト、状態ファイルなしの制御実験は 0。tmpfs でない場所を指定すると、ファイルは作られず、書き込みも 0。
- 実機に近い確認（リリースビルド、2 シャード、4 つの Core）：アイドルで全 Core が `pending=0`・`suspected_stalled=False`、`seq` が進む。Core 1 本に `SIGSTOP` を送ると、40 秒後に**その Core だけ** `suspected_stalled=True`、`SIGCONT` で回復。Core を `SIGKILL` して botd が再起動すると、**世代が変わり**、`stale_generation`・`regressed` は出ない。botd は応答し続けた。
- テスト：lib 236 件、`botd` 22 件、`botd_separate_cores` 8 件（SIGSTOP の検知、世代の確認を含む）、`core_attach` 28 件、2 シャード 3 件、clippy 警告なし。リリースビルドに警告なし、`cargo test --release --lib` は通る。リリースの `src`・`botd` は、試験専用の旗（`--core-status-allow-non-tmpfs`、`--core-status-interval-ms`）を**拒否**する。

**保証の範囲（正確に）：**
- 「ファイルのデータを、ディスク上のファイルシステムに書かない」。**tmpfs のページは、メモリ逼迫時に swap され得る**（カーネルの `noswap` は強制していない）ので、**通常の運用で書き出しがゼロ**であり、絶対のゼロではない。
- 読み取りは、tmpfs 上の 64 バイトの同期読み取り（親・記述子の tmpfs 確認、`O_NOFOLLOW|O_NONBLOCK`、通常ファイル・サイズ・所有者・リンク数・dev/ino の確認）。**サービスのユーザー自身が、状態ディレクトリを、到達できない NFS 等への symlink やマウントに差し替えた場合、botd の制御経路が止まり得る**（理論上。最初のパスの `statfs` がブロックし得る）。有界のバックグラウンド読み取り（結果のキャッシュつき）は**未実装**。
- 64 バイトの上書きは、並行する読み取りに対して、原子的な 1 つのスナップショットになる保証はない。CRC は破損を確率的に検出するだけで、原子性も鮮度も証明しない。読み取りは CRC の不一致で 1 回だけ再試行し、それでも合わなければ `corrupt`（一時的）として扱う。
- 時刻は単調時計（`CLOCK_BOOTTIME`）。時計の取得失敗は「取得不能」として扱い、鮮度 0 にしない。1 秒を超える未来の時刻の記録は拒否する。世代は botd が Core の起動ごとに決める非ゼロの乱数で、botd は世代と PID を照合し、連番・時刻の後退（`regressed`）を検出する（同じ連番の繰り返し＝固まった Core は、後退ではなく、経過時間で停滞の疑いにする）。Core の（再）起動の前に、そのスロットの古い状態ファイルを削除する。

**既知の限界・未対応（Astra 第 20 回の指摘のうち、対応しないもの）：**
- ファイル・ディレクトリの**作成は、親の記述子を固定してからの相対作成**になっていない。親が `is_tmpfs` の確認の後に差し替えられると、**ディスク上に inode やディレクトリができ得る**。作成した**ファイル**は、直後の `fstatfs` で tmpfs でないと分かれば拒否して削除を試みるが、**削除は最善努力で、失敗しても無視する**（残り得る）。作成した**ディレクトリ**は、拒否するだけで、**削除しない**（残る）。ファイルシステムやジャーナルへの書き込みは取り消せない。この競合は、**サービスのユーザー自身による、状態ディレクトリの意図的な差し替えが必要**で、「信頼できる非公開のパス、単一の botd」という前提の範囲外。親の記述子を固定した `openat2` による作成は、将来の課題。
- 拒否したファイルの**削除は、確認と削除が別の操作**（置き換えられたパスを消し得る）。同じく、サービスのユーザー自身による差し替えが必要。通常の起動（単一の botd が、Core の起動の前に削除する）では、直列化されている。
- **統合テストは、debug ビルド（`cargo test`）を前提**とする（試験専用の旗が、リリースで拒否されるため）。`cargo test --release` の統合テストは、サポートしない。
- Oracle・実サーバーの systemd（`RuntimeDirectory`）での確認は未実施。

