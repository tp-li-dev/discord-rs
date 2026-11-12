# Shard 実行に向けた設計メモ

仕様の運用単位は Shard ごとの Gateway と Core であり、複数 Shard を単一プロセスに収容することを必須にしない。単一プロセスによる複数 Gateway の検証は共有キューやイベント配送の試験には使えるが、障害隔離や独立した更新単位の完成を意味しない。

## 準備から起動までの候補

1. 設定全体を検証し、Bot ごとの Discord メタデータを対象 Bot の認証情報で取得する。`auto` を解決し、明示されたローカル primary のみを計画する。手入力の Shard 数は Discord の観測値とは区別する。
2. Bot ごとに Identify 制限の観測値から共有キューを作る。全 Shard は同一 Bot の同一キューを使い、重複作成で予算をリセットしない。観測値は予約ではなく、別プロセスや別ホストも消費するため、分散構成では共有制御が別途必要。
3. Shard ごとに識別子と総数を固定した Gateway 実行単位、状態分離された Core と更新経路を用意する。各 Gateway に対応する token のみを渡し、Core と他 Bot の環境には渡さない。IPC・管理ポート・状態ファイルを Bot/Shard 単位に分離する。
4. 所有権の承認と有効な lease が必要な構成では、それらを検証するまで処理を開始しない。未実装の場合は起動を拒否する。旧所有者が期限切れで停止することも試験する。
5. Gateway の Identify は Bot 単位の制限に従い、失敗・停止・再試行の結果を観測する。Core、Gateway、応答 drain の失敗時は他 Shard の状態に混入させない。

現状の Gateway は `ShardId::ONE` 固定で、Worker 更新ファイル、管理、状態は Shard 単位に分離されていない。Identify queue は単一プロセス内の試作で、クォーラム、lease、分散予算管理、再起動後の安全性を実装していない。したがって配置計画を実際の複数 Shard 起動には接続しない。

## 純粋な起動仕様の生成

`shard_runtime::plan(&[LocalShardIntent], &Path)` は、配置計画のローカル primary 出力から Bot/Shard ごとの `ShardRuntimeSpec` を生成する。出力順は Bot ID、Shard ID の順で固定する。各仕様は Gateway と Core のプログラム名、個別の `OsString` 引数、環境ポリシー、状態・管理パス、および当該ペアだけで共有する IPC パスを持つ。ファイル操作・環境変数の参照・プロセス起動・管理 listener の作成は行わない。`gateway-host` / `bot-core` と引数は将来のランチャー向け契約であり、既存 main/worker の CLI には未接続。

環境は両ロールとも継承禁止（`clear_inherited = true`）、通常変数は空。Gateway のみ当該 Bot の `token_env` を `credential_refs` と `--token-env` に持ち、Core はどちらも持たない。値は保持・取得しない。将来のランチャーは必ず `env_clear` 相当を適用し、指定された名前の秘密だけを注入する。Windows の環境変数名の大小文字非区別を考慮し、異なる Bot が同一名（大小文字違いを含む）を参照する計画は拒否する。同じ変数値を異なる名前で登録した場合の検知は本関数の範囲外。

パスはホスト OS の絶対 root へ `Path::join` し、`root/bot-<UTF-8 の小文字16進表現>/shard-<番号>/` 配下に `ipc`、`gateway-state`、`core-state`、`gateway-management`、`core-management` を割り当てる。Bot ID は1〜64 UTF-8 byte、前後空白・制御文字は禁止。予約名 CON/NUL/COM1、スラッシュ・逆スラッシュ・ドライブ表現・末尾ピリオドも生のパス成分にしない。大小文字の異なる ID も異なる16進表現になり、Windows の大小文字非区別でも衝突しない。パス全体を ASCII 小文字化した重複検査も行う。root の親参照、Shard 範囲外・重複、同一 Bot 内の総数/変数名の不一致は固定エラーで拒否し、入力をエラー本文へ転記しない。

root は信頼された配備設定であり、その存在・ACL・symlink/junction・OS/transport の長さ制限は検査しない。IPC と管理パスは論理的な場所であり、Unix socket / Windows named pipe の名前への変換や認証は今後の transport 実装が担う。世代並行更新の名前割当、実行成果物の検証、Identify 共有、quorum/lease/fencing、管理認証は未接続。この仕様を起動許可として使用してはならない。

### 今回の検証（2026-11-12）

- `src/tests/shard_runtime.rs` に配置出力との接続、引数と環境・パスの分離、順序、範囲・重複・不整合・変数名共有の拒否、予約名・区切り・大小文字・日本語 ID の試験を追加。
- Windows で `cargo fmt --all` 成功。workspace clippy はオンライン取得が TLS `SEC_E_NO_CREDENTIALS` で失敗し、`--offline` でも `inventory` キャッシュ不足で実行不能。`cargo test --workspace --offline` も同じ依存不足で停止。
- 代替として実装ファイルと配置入力型のスタブを使った一時 rustc ハーネス（`--test -D warnings`）で、配置読込に依存しない3試験が Windows 上で通過。Cargo 統合試験・workspace clippy/test 成功を意味しない。Linux 実行は未検証。

## 既存の配置・Identify 試験

`src/tests/shard_plan.rs` は模擬メタデータと配置の連結、および同一 Bot の Shard が同じ Identify bucket を共有し、異なる Bot は別キューを使えることをオフラインで調べる。実 Gateway、Discord のレート制限、複数プロセス、lease、分断耐性の試験ではない。次段階ではモック Gateway を使って各実行単位の引数・状態・停止の分離を検証し、その後に安全な権限制御と実 Discord の試験を行う。
