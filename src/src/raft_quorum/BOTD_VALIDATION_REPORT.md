# botd Raft 配線の継続検証（2026-11-12）

対象 HEAD: `bf1ca423ad22091855ce1eeb9e46a06d062b29ff`。この記録は今回の担当が直接実行した検証に限定する。並行処理の実行結果は合算しない。

## 変更と確認

- WIP の `--allow-raft-quorum` → `ClusterHost` → `ClusterRuntime::bind_raft` の配線を確認。既定無効、loopback 模擬 Discord 必須、本番有効化拒否、旧 ACK 経路へのフォールバック禁止を維持。
- `RecoveryBudget` の clippy 警告 `unnecessary_lazy_evaluations` を同値の `Option::or` で修正。
- binary guard に `--no-autostart` との併用拒否と、Shard scope 付き admin の起動拒否を追加。証明書や store 作成より前の拒否を検査する。
- `raft_quorum.rs` の module コメントを、現在の模擬 botd 配線に合わせて訂正。
- 作業中に別処理が E2E の操作IDを32桁hexへ修正し、後に任意追加の guardian 強制終了試験と専用 helper を削除した。それらの変更は上書きしていない。

## ローカル検証

Windows / Rust・Cargo 1.90.0。`src/` で `CARGO_NET_OFFLINE=true`、`CARGO_PROFILE_DEV_DEBUG=0`、`CARGO_INCREMENTAL=0`、`CARGO_BUILD_JOBS=2` を設定した。

```text
cargo test --offline -p src --test raft_botd_guards --test raft_botd_processes -- --test-threads=1 --nocapture
```

上記コマンドは1回のみ実行。guard 2件成功、プロセス対象5件中4件成功・1件失敗、合計6件成功・1件失敗。全体の終了コードは1。

- `raft_botd_death_persistent_rejoin_and_automatic_failback`: 成功。3実 botd、実 mTLS、同じ永続 store への復帰を使用。短縮設定で引継ぎ4243ms、返却4043msを観測。性能保証ではない。
- `raft_botd_partitions_stop_expired_gateways_without_double_active`: 成功。health/Raft の分断、全ノード孤立、health を残す Raft RPC 単独分断を検証。
- `raft_gateway_crash_recovers_only_after_exit_and_new_epoch`: 成功。
- `support::process_support::process_probe_detects_the_current_real_process`: 成功。
- `raft_guardian_death_latches_unconfirmed_exit_without_releasing`: 失敗。guardian 強制終了後の collector read が `invalid or failed collector frame` となり、終了済みプロセスを含む cleanup でも失敗した。接続リセットの OS エラー詳細は採取していない。この試験は並行処理によって削除されたため、guardian 異常死の実プロセス保証は残課題。削除後の E2E は再実行していない。

```text
cargo clippy --offline -p src --lib --bin botd --test raft_botd_guards --test raft_botd_processes -- -D warnings
```

初回は上記の警告1件で失敗。修正後の確認は成功（終了コード0）。`git diff --check` も成功。検証終了後のプロセス照会では `botd` / `src` プロセスは見つからなかった。

## Actions で反復する対象

Windows / Ubuntu の両方で、少なくとも以下を直列実行で反復する。今回 Actions の起動・変更・結果確認は行っていない。

- `raft_botd_death_persistent_rejoin_and_automatic_failback`
- `raft_botd_partitions_stop_expired_gateways_without_double_active`
- `raft_gateway_crash_recovers_only_after_exit_and_new_epoch`
- `raft_botd_guards` の2試験

関連回帰は `raft_cluster_runtime`、`raft_lease_policy`、`raft_quorum_handoff`。guardian 強制終了の E2E は、異常切断の観測と冪等な後片付けを整えてから復元・検証する必要がある。

実 Discord、本番、Linux、実ホスト間、OS suspend、時計速度差、分散 Identify/REST 予算は今回未検証。短縮 lease/安定待機による単発成功から、本番の無停止・安全性・復旧時間上限を主張しない。

新規依存・commit・実 Discord 接続・全体試験・ローカル反復は行っていない。`docs/progress.md` と `docs/handoff.md` は編集していない。
