# 仕様適合表

[architecture.md](architecture.md) §16.2・§16.3 と現行コードの対応（2026-11-12時点の静的照合）。試験名は根拠であり、本表の作成時に実行したという意味ではない。過去の実行結果は [progress.md](progress.md)、残課題は [known-limitations.md](known-limitations.md) を参照。

**完了**は条件全体の実装と必要な検証を確認した状態、**部分**は実装または試験があるが条件全体の達成を確認できない状態、**未実装**は対象機能がない状態。シミュレーションやモックの結果は実Discord・複数ホストでの保証に代用しない。

過去の統合版では Windows/Rust 1.99.0 上で workspace test 647件成功・失敗0・無視1（手動ベンチ）を2回連続で確認したと記録されている（[progress.md](progress.md)）。その後の追加を含む現コードや Linux の検証結果を意味しない。T04 fixture の成果物選択は修正されているが、安定性は引き続き要確認。

## §16.2 完了条件

| 条件 | 状況 | 根拠となるファイル・試験 | 未検証範囲・残作業 |
|---|---|---|---|
| fmt check | 部分 | `src/` で `cargo --offline fmt --all --check` が現 checkout の Windows / Rust 1.90.0 で成功 | Linuxでの実行は未確認 |
| clippy 全target・警告禁止 | 部分 | `src/` で `cargo clippy --offline --workspace --all-targets -- -D warnings` が過去にWindowsで成功 | 現コードとLinuxでの再実行は未確認 |
| workspace test・マクロ成功/失敗 | 部分 | `src/framework/tests/compile_fail.rs` `command_diagnostics`、`src/framework/tests/cog_extensions.rs` `typed_commands_and_events_share_the_injected_instance` | 過去のWindows統合版で完走（647件）。現コード、Linuxと未試験の障害条件 |
| Windows/Ubuntu CI | 部分 | `.github/workflows/ci.yml`、progressのローカルWindows記録 | 現統合版のActions実行結果とLinux |
| CogなしハイブリッドPing | 部分 | `src/framework/examples/hybrid_bot.rs`、`src/tests/e2e_shard_runtime.rs` `botd_real_gateways_route_hybrid_commands_isolate_bots_recover_and_drain` | 実Discord接続・応答 |
| Cogの状態共有・イベント・コマンド | 部分 | `src/framework/tests/cog_extensions.rs` `typed_commands_and_events_share_the_injected_instance`、`src/framework/tests/example_cog_counter.rs` `ready_event_uses_the_cog_and_does_not_reset_counter` | 実WorkerのCogライフサイクル接続・実Discord |
| CLI複数Bot選択と管理 | 部分 | `src/tests/botctl_daemon.rs` `multiple_bots_use_real_states_and_graceful_lifecycle_by_name_id_and_prefix`、`src/tests/update_rollback.rs` | update/rollbackはオフライン計画のみ。daemon実行、遠隔管理 |
| 複数Shard配置・監視 | 部分 | `src/tests/e2e_shard_runtime.rs` `botd_real_gateways_route_hybrid_commands_isolate_bots_recover_and_drain`、`src/tests/cluster_sim.rs` `t10_three_nodes_match_configured_placement_for_multiple_bots` | 別ホスト実配置と本番監視 |
| Core更新と接続維持・旧応答 | 部分 | `src/tests/hot_swap_scenarios.rs` `t04_real_five_second_handler_drains_while_v2_serves_new_requests`、`t04_external_response_stays_with_v1_while_v2_accepts_new_work` | 実Discord WebSocket/REST、Shard Core全資源のdrain |
| Gateway並行更新 | 部分 | `src/tests/gateway_switch.rs` `t07_standby_never_admits_commands_and_order_is_strict`、`src/tests/e2e_gateway_process_switch.rs` | プロセス切替は明示的な試験モードと模擬Discordに限定。実Discordと本番lease |
| 更新失敗時の維持・切戻し | 部分 | `src/tests/hot_swap_scenarios.rs` `t05_candidate_crashes_before_ready_while_v1_keeps_its_process_and_state`、`src/tests/update_rollback.rs` | オフライン計画のみ。daemon実行、全Shard一括rollback、実Discord外部副作用 |
| 分散Daemon failover/failback・分断 | 部分 | `src/tests/cluster_sim.rs` `t11_server_stop_is_detected_at_three_seconds_before_proxy_activation`、`t13_partition_preserves_old_local_admission_only_until_exact_lease_expiry` | 本番合意・lease統合、実複数ホスト・旧Discord接続 |
| Identify・Interaction・Rate Limit | 部分 | `src/tests/gateway_switch.rs` `t09_budget_and_rate_limit_defer_without_spawning_or_pausing_old`、`src/tests/testkit_selftest.rs` `mock_scripts_rest_success_rate_limit_server_error_and_interaction_ack` | 実Discordの予算、3秒制限、複数プロセス間調整 |
| token・認可・過大IPCの試験 | 部分 | `src/tests/main_security.rs` `missing_token_preserves_a_safe_configuration_diagnostic`、`src/tests/rbac_audit.rs` `t20_every_unauthenticated_remote_operation_is_denied_and_audited`、`src/src/worker.rs` `rejects_oversized_and_truncated_frames` | 全ログ経路、Windows ACL、実遠隔管理系の統合 |
| 再現可能なベンチマーク | 部分 | [benchmarks.md](benchmarks.md)、`src/benches/ipc_roundtrip.rs` | Linux、RSS/Shard・比較対象・負荷時目標の一式 |
| 指定日本語文書 | 部分 | `README.md`、`docs/developer-guide.md`、`docs/operator-guide.md`、`docs/ipc-protocol.md`、`docs/security.md`、`docs/benchmarks.md` | 実装変更に伴う継続的な照合 |
| 未実装・保証限界の記録 | 部分 | [known-limitations.md](known-limitations.md)、本表 | 最新コードとの継続的な照合 |

## 追加された部品の範囲（静的照合）

| 部品 | 根拠 | 残る境界 |
|---|---|---|
| ファイル権限 | `src/src/fs_perms.rs`、`src/tests/fs_perms.rs` | Unix の開いたハンドルの mode のみ。所有者・ACL・親を確認せず、Windows は Unsupported |
| 証明書更新・失効 | `src/src/tls_transport.rs`、`src/tests/cert_rotation.rs` | 信頼済みプロセス内 API と loopback 試験コード。配布・永続化・CA更新・本番連携は未完成 |
| OpenRaft | `src/src/raft_quorum.rs`、`src/tests/raft_quorum.rs`、`src/tests/raft_quorum_storage.rs` | 実験的で botd 未接続。本番起動拒否、クラッシュ後の予約自動解放なし |
| 更新・切戻し | `src/src/update_rollback.rs`、`src/tests/update_rollback.rs` | オフライン計画・成果物検証。daemon 実行は拒否 |
| Gateway プロセス切替 | `src/tests/e2e_gateway_process_switch.rs` | 明示的な試験モード・模擬Discord。実Discord と本番quorum/lease は未検証 |
| framework 状態アダプタ | `src/src/state_store.rs`、`src/tests/framework_state_adapter.rs` | 共有メモリに対する試験コード。永続バックエンドや実Worker統合は未完成 |

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
| T10 3ホストへの配置 | 部分 | `src/tests/cluster_sim.rs` `t10_three_nodes_match_configured_placement_for_multiple_bots` | 決定的シミュレーション。実ホスト配置 |
| T11 Server停止と引継ぎ | 部分 | `src/tests/cluster_sim.rs` `t11_server_stop_is_detected_at_three_seconds_before_proxy_activation` | 実ホスト・本番quorum/leaseと実Discord復旧 |
| T12 安定後の自動返却 | 部分 | `src/tests/cluster_sim.rs` `t12_recovered_primary_waits_thirty_stable_seconds_then_receives_new_epoch`、`t12_flapping_primary_restarts_stability_and_does_not_bounce_ownership` | 実ホスト・本番所有権統合 |
| T13 分断と旧Discord接続 | 部分 | `src/tests/cluster_sim.rs` `t13_partition_preserves_old_local_admission_only_until_exact_lease_expiry`、`src/tests/failover_ownership.rs` `t13_partition_keeps_old_discord_alive_until_lease_expires_without_double_active` | 模擬時計・所有権。実分断とDiscord接続 |
| T14 quorum喪失 | 部分 | `src/tests/cluster_sim.rs` `t14_minority_never_promotes_or_renews_and_recovers_after_quorum_returns`、`src/tests/raft_quorum.rs` `quorum_loss_cannot_renew_or_grant_and_clock_regression_is_sticky` | 実験的OpenRaft部品はbotd未接続。本番fencing・実分断 |
| T15 IPC遅延・切断・満杯 | 部分 | `src/tests/testkit_selftest.rs` `mock_scripts_rest_success_rate_limit_server_error_and_interaction_ack`、`src/tests/event_seq.rs` `ordered_admission_gap_duplicate_and_queue_backpressure` | 実Workerの障害注入とHeartbeatへの影響 |
| T16 View/Modalと更新 | 部分 | `src/framework/tests/interactions.rs` `routes_across_independent_registries_and_errors`、`src/tests/framework_state_adapter.rs` | 共有メモリストアと模擬ルーティング。永続DB・実Worker切替 |
| T17 wait_forと更新 | 部分 | `src/framework/tests/interactions.rs` `wait_cancelled_on_drain_or_closed_signal`、`src/tests/state_store.rs` `wait_for_completes_or_cancels_for_drain_and_closed_signal` | 実Worker drain接続・永続Subscription配送 |
| T18 複数Bot同時更新 | 部分 | `src/tests/e2e_shard_runtime.rs` `botd_real_gateways_route_hybrid_commands_isolate_bots_recover_and_drain`、`src/tests/shard_runtime.rs` `multiple_bots_and_shards_keep_credentials_and_paths_separate` | 同時更新自体、実Discordと権限分離の統合 |
| T19 未知IPCバージョン/Capability | 部分 | `src/tests/ipc_handshake.rs` `t19_real_worker_negotiates_and_rejects_incompatible_hello`、`src/tests/hot_swap_scenarios.rs` `t19_unknown_required_hello_capability_is_recorded_and_v1_state_survives` | 全将来バージョン・実Discord負荷下 |
| T20 未認証遠隔操作 | 部分 | `src/tests/rbac_audit.rs` `t20_every_unauthenticated_remote_operation_is_denied_and_audited`、`src/tests/tls_transport.rs` `untrusted_expired_unauthorized_and_ambiguous_client_certificates_are_rejected`、`src/tests/cert_rotation.rs` | 実遠隔管理経路・永続監査。現CLI管理はローカル限定 |

