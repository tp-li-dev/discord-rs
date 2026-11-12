# 仕様適合表

[architecture.md](architecture.md) §16.2・§16.3 と現行コードの対応（2026-11-12時点の静的照合）。試験名は根拠であり、本表の作成時に実行したという意味ではない。過去の実行結果は [progress.md](progress.md)、残課題は [known-limitations.md](known-limitations.md) を参照。

**完了**は条件全体の実装と必要な検証を確認した状態、**部分**は実装または試験があるが条件全体の達成を確認できない状態、**未実装**は対象機能がない状態。シミュレーションやモックの結果は実Discord・複数ホストでの保証に代用しない。

過去の統合版では Windows/Rust 1.99.0 上で workspace test 647件成功・失敗0・無視1（手動ベンチ）を2回連続で確認したと記録されている（[progress.md](progress.md)）。その後の追加を含む現コードや Linux の検証結果を意味しない。T04 fixture の成果物選択は修正されているが、安定性は引き続き要確認。

## §16.2 完了条件

| 条件 | 状況 | 根拠となるファイル・試験 | 未検証範囲・残作業 |
|---|---|---|---|
| fmt check | 部分 | `.github/workflows/ci.yml` の fmt check | 過去のLinux CI成功はprogressに記録。今回未実行 |
| clippy 全target・警告禁止 | 部分 | `src/` で `cargo clippy --offline --workspace --all-targets -- -D warnings` が過去にWindowsで成功 | 今回未実行。修正後のWindows Actions再確認は未了 |
| workspace test・マクロ成功/失敗 | 部分 | `src/framework/tests/compile_fail.rs` `command_diagnostics`、`src/framework/tests/cog_extensions.rs` `typed_commands_and_events_share_the_injected_instance` | 過去のWindows統合版で完走（647件）。今回未実行。未試験の障害条件 |
| Windows/Ubuntu CI | 部分 | `.github/workflows/ci.yml`、progressのローカルWindows記録 | 過去のLinux CI成功はprogressに記録。Windowsの修正後Actions再確認は未了 |
| CogなしハイブリッドPing | 部分 | `src/framework/examples/hybrid_bot.rs`、`src/tests/e2e_shard_runtime.rs` `botd_real_gateways_route_hybrid_commands_isolate_bots_recover_and_drain` | 実Discord接続・応答 |
| Cogの状態共有・イベント・コマンド | 部分 | `src/framework/tests/cog_extensions.rs` `typed_commands_and_events_share_the_injected_instance`、`src/framework/tests/example_cog_counter.rs` `ready_event_uses_the_cog_and_does_not_reset_counter` | 実WorkerのCogライフサイクル接続・実Discord |
| CLI複数Bot選択と管理 | 部分 | `src/tests/botctl_daemon.rs` `multiple_bots_use_real_states_and_graceful_lifecycle_by_name_id_and_prefix`、`src/tests/update_rollback.rs` | 認証付きdaemon更新制御あり。実Discord、遠隔管理、全障害条件 |
| 複数Shard配置・監視 | 部分 | `src/tests/e2e_shard_runtime.rs` `botd_real_gateways_route_hybrid_commands_isolate_bots_recover_and_drain`、`src/tests/cluster_sim.rs` `t10_three_nodes_match_configured_placement_for_multiple_bots` | 別ホスト実配置と本番監視 |
| Core更新と接続維持・旧応答 | 部分 | `src/tests/hot_swap_scenarios.rs` `t04_real_five_second_handler_drains_while_v2_serves_new_requests`、`t04_external_response_stays_with_v1_while_v2_accepts_new_work` | 実Discord WebSocket/REST、Shard Core全資源のdrain |
| Gateway並行更新 | 部分 | `src/tests/gateway_switch.rs` `t07_standby_never_admits_commands_and_order_is_strict`、`src/tests/e2e_gateway_process_switch.rs` | プロセス切替は明示的な試験モードと模擬Discordに限定。実Discordと本番lease |
| 更新失敗時の維持・切戻し | 部分 | `src/tests/hot_swap_scenarios.rs` `t05_candidate_crashes_before_ready_while_v1_keeps_its_process_and_state`、`src/tests/update_rollback.rs` | daemon更新制御あり。全Shard一括rollback、実Discord外部副作用 |
| 分散Daemon failover/failback・分断 | 部分 | `src/tests/cluster_sim.rs` `t11_server_stop_is_detected_at_three_seconds_before_proxy_activation`、`t13_partition_preserves_old_local_admission_only_until_exact_lease_expiry` | 本番合意・lease統合、実複数ホスト・旧Discord接続 |
| Identify・Interaction・Rate Limit | 部分 | `src/tests/gateway_switch.rs` `t09_budget_and_rate_limit_defer_without_spawning_or_pausing_old`、`src/tests/testkit_selftest.rs` `mock_scripts_rest_success_rate_limit_server_error_and_interaction_ack` | 実Discordの予算、3秒制限、複数プロセス間調整 |
| token・認可・過大IPCの試験 | 部分 | `src/tests/main_security.rs` `missing_token_preserves_a_safe_configuration_diagnostic`、`src/tests/rbac_audit.rs` `t20_every_unauthenticated_remote_operation_is_denied_and_audited`、`src/src/worker.rs` `rejects_oversized_and_truncated_frames` | 全ログ経路、Windows ACL、実遠隔管理系の統合 |
| 再現可能なベンチマーク | 部分 | [benchmarks.md](benchmarks.md)、`src/benches/ipc_roundtrip.rs` | 共有runnerの結果は本番保証ではない。RSS/Shard・比較対象・負荷時目標の一式 |
| 指定日本語文書 | 部分 | `README.md`、`docs/developer-guide.md`、`docs/operator-guide.md`、`docs/ipc-protocol.md`、`docs/security.md`、`docs/benchmarks.md` | 実装変更に伴う継続的な照合 |
| 未実装・保証限界の記録 | 部分 | [known-limitations.md](known-limitations.md)、本表 | 最新コードとの継続的な照合 |

## 実装部品と境界（静的照合）

| 部品 | 実在する試験・実装 | 境界 |
|---|---|---|
| ファイル権限 | `src/tests/fs_perms_path_api.rs` `windows_removes_explicit_allow_deny_and_inherited_aces`、`windows_rechecks_permissions_on_every_open` | WindowsのファイルパスAPIはACLを扱う。ハンドル単独・ディレクトリは未対応。親やリンクの制約あり |
| 証明書・CA | `src/tests/cert_rotation.rs` `peer_overlap_accepts_old_and_new_then_retires_old_without_rebinding`、`src/tests/ca_policy.rs` `ca_overlap_accepts_both_then_retires_old_in_both_roles`、`src/tests/cert_policy.rs` `authenticated_roundtrip_replacement_and_reopen_preserve_deadlines` | ポリシー永続化・CA更新部品あり。配布、運用時の鍵管理と本番統合は別途必要 |
| Raft・mTLS | `src/tests/raft_mtls.rs` `election_forwarding_concurrent_epoch_cas_and_bot_shard_isolation`、`src/tests/raft_quorum_storage.rs` `vote_log_commit_applied_membership_and_snapshot_survive_reopen`、`src/tests/raft_cluster_runtime.rs` `raft_partition_and_total_quorum_loss_never_allow_two_mock_actives` | cluster runtimeのRaft経路は明示的な試験・模擬Discord用。本番起動は拒否。実複数ホストのfencingは未検証 |
| 子復旧 | `src/tests/botd_child_recovery.rs` `core_os_death_recovers_without_gateway_epoch_connection_or_identify_change`、`src/tests/worker_child_recovery.rs` `idle_core_exit_recovers_without_a_request_or_gateway_epoch_change` | daemonとWorker管理に接続。実Discord・分散Identifyや全OS障害の保証ではない |
| 更新・切戻し | `src/tests/botctl_update_control.rs` `update_and_rollback_use_authenticated_contract_and_poll_without_replay`、`src/tests/update_control_rbac.rs` `update_and_rollback_enforce_server_principals_and_record_denials`、`src/tests/update_rollback.rs` `failure_after_commit_requires_safe_retained_previous_process` | daemon更新制御と計画部品あり。実Discordと全障害条件は未検証 |
| 永続状態と配送 | `src/tests/file_state_store.rs` `reopen_preserves_data_revision_and_deletion`、`src/tests/worker_interactions.rs` `button_select_modal_survive_hot_swap_and_complete_manager_restart`、`src/tests/subscription.rs` `registration_and_checkpoint_survive_reopen_with_bot_and_shard_isolation`、`src/tests/durable_jobs.rs` `checkpoint_and_expired_retry_safe_claim_resume_after_restart_and_backoff` | ファイルStateStore・アダプタ・永続キュー部品あり。任意の外部副作用のexactly-onceや本番耐障害性は保証しない |
| メトリクス | `src/tests/metrics_feed.rs` `measured_waits_and_failover_are_exported_without_extra_labels` | feed部品はあるがdaemonには接続されていない |
| Discord smoke | `src/src/bin/discord_smoke.rs`、`src/tests/discord_smoke/cli.rs` `dry_run_without_token_succeeds_without_runtime_or_network` | 読み取り専用CLI。実Discordでの検証は利用者が実施する |
| 負荷・性能 | `.github/workflows/stress.yml`、`.github/workflows/bench.yml`、`src/tests/update_load.rs` `continuous_events_during_updates_and_core_crashes` | 手動workflowが存在。更新負荷の無停止受入条件は既知の未達で試験は `#[ignore]`。本表作成時の実行結果ではない |

利用者提供のLinuxベンチ記録では Actions `ubuntu-latest`（AMD EPYC 7763、4 vCPU）のIPC往復p99は並列1/4/16で115.5/258.1/973.6 µs、Suspected p50は1503.6 ms、FailoverPending p50は3003.1 ms。共有runnerでの観測であり本番保証ではなく、往復値は片道p99の直接計測ではない。詳細は [benchmarks.md](benchmarks.md)。

## §16.3 最低テストシナリオ

| ID・条件 | 状況 | 根拠となるファイル・試験名 | 未検証範囲 |
|---|---|---|---|
| T01 `/ping` / `!ping` | 部分 | `src/tests/e2e_shard_runtime.rs` `botd_real_gateways_route_hybrid_commands_isolate_bots_recover_and_drain` | 模擬Discord。実Discordでの登録・配送 |
| T02 自動収集・重複 | 部分 | `src/framework/tests/duplicate.rs` `duplicate_names_are_rejected`、`src/framework/tests/cog_collision.rs` `duplicate_cog_command_names_are_rejected_before_any_dispatch` | 全構成・プラットフォームでの収集 |
| T03 Cogあり/なし・状態共有 | 部分 | `src/framework/tests/cog_extensions.rs` `typed_commands_and_events_share_the_injected_instance`、`src/framework/tests/example_cog_counter.rs` `prefix_slash_and_plain_command_share_one_service` | 実WorkerへのCog接続 |
| T04 5秒処理中のCore切替 | 部分 | `src/tests/hot_swap_scenarios.rs` `t04_real_five_second_handler_drains_while_v2_serves_new_requests`、`t04_external_response_stays_with_v1_while_v2_accepts_new_work` | 実Discord接続・外部副作用、全資源 |
| T05 Ready前クラッシュ | 部分 | `src/tests/hot_swap_scenarios.rs` `t05_candidate_crashes_before_ready_while_v1_keeps_its_process_and_state` | 実Discord・全障害タイミング |
| T06 切替直後クラッシュ | 部分 | `src/tests/hot_swap_scenarios.rs` `t06_crash_after_commit_restarts_v2_without_replaying_the_failed_request`、`t06_post_commit_unknown_outcome_is_recorded_for_management` | 実外部副作用・永続事故記録 |
| T07 Gateway並行Standby | 部分 | `src/tests/gateway_switch.rs` `t07_standby_never_admits_commands_and_order_is_strict`、`src/tests/e2e_gateway_process_switch.rs` `prepared_real_gateway_is_standby_then_switch_preserves_old_rest_origin` | 模擬Discord。実WebSocket並行起動 |
| T08 Gateway Active切替 | 部分 | `src/tests/gateway_switch.rs` `t08_old_work_can_issue_rest_after_pause_and_routes_survive_switch`、`src/tests/e2e_gateway_process_switch.rs` | 模擬Discord。実Discord RESTへの接続 |
| T09 Identify・Rate Limit延期 | 部分 | `src/tests/gateway_switch.rs` `t09_budget_and_rate_limit_defer_without_spawning_or_pausing_old`、`src/tests/gateway_rolling_sim.rs` `identify_deferral_reset_and_shared_rest_429` | 実Discordと本番Gateway切替 |
| T10 3ホストへの配置 | 部分 | `src/tests/e2e_cluster_processes.rs` `t10_t11_t12_placement_daemon_death_and_stable_failback` | loopback試験。実ホスト配置 |
| T11 Server停止と引継ぎ | 部分 | `src/tests/e2e_cluster_processes.rs` `t10_t11_t12_placement_daemon_death_and_stable_failback` | 実ホスト・本番quorum/leaseと実Discord復旧 |
| T12 安定後の自動返却 | 部分 | `src/tests/e2e_cluster_processes.rs` `t10_t11_t12_placement_daemon_death_and_stable_failback` | 実ホスト・本番所有権統合 |
| T13 分断と旧Discord接続 | 部分 | `src/tests/e2e_cluster_processes.rs` `t13_t14_mtls_partition_expires_old_lease_and_majority_loss_blocks_promotion` | loopback試験・模擬Discord。実ホスト間分断とDiscord接続 |
| T14 quorum喪失 | 部分 | `src/tests/cluster_sim.rs` `t14_minority_never_promotes_or_renews_and_recovers_after_quorum_returns`、`src/tests/raft_quorum.rs` `quorum_loss_cannot_renew_or_grant_and_clock_regression_is_sticky` | 試験用Raft runtime経路あり。本番起動拒否、本番fencing・実分断 |
| T15 IPC遅延・切断・満杯 | 部分 | `src/tests/testkit_selftest.rs` `mock_scripts_rest_success_rate_limit_server_error_and_interaction_ack`、`src/tests/event_seq.rs` `ordered_admission_gap_duplicate_and_queue_backpressure` | 実Workerの障害注入とHeartbeatへの影響 |
| T16 View/Modalと更新 | 部分 | `src/framework/tests/interactions.rs` `routes_across_independent_registries_and_errors`、`src/tests/framework_state_adapter.rs` | `src/tests/worker_interactions.rs` `button_select_modal_survive_hot_swap_and_complete_manager_restart` にファイル永続状態とWorker切替の試験あり。実Discord・本番永続性 |
| T17 wait_forと更新 | 部分 | `src/framework/tests/interactions.rs` `wait_cancelled_on_drain_or_closed_signal`、`src/tests/state_store.rs` `wait_for_completes_or_cancels_for_drain_and_closed_signal` | `src/tests/subscription.rs` `delivery_repeats_only_after_lease_boundary_and_rejects_stale_ack` に永続配送部品の試験あり。実Discordとの統合 |
| T18 複数Bot同時更新 | 部分 | `src/tests/e2e_shard_runtime.rs` `botd_real_gateways_route_hybrid_commands_isolate_bots_recover_and_drain`、`src/tests/shard_runtime.rs` `multiple_bots_and_shards_keep_credentials_and_paths_separate` | 同時更新自体、実Discordと権限分離の統合 |
| T19 未知IPCバージョン/Capability | 部分 | `src/tests/ipc_handshake.rs` `t19_real_worker_negotiates_and_rejects_incompatible_hello`、`src/tests/hot_swap_scenarios.rs` `t19_unknown_required_hello_capability_is_recorded_and_v1_state_survives` | 全将来バージョン・実Discord負荷下 |
| T20 未認証遠隔操作 | 部分 | `src/tests/rbac_audit.rs` `t20_every_unauthenticated_remote_operation_is_denied_and_audited`、`src/tests/tls_transport.rs` `untrusted_expired_unauthorized_and_ambiguous_client_certificates_are_rejected`、`src/tests/cert_rotation.rs` | 実遠隔管理経路・永続監査。現CLI管理はローカル限定 |

