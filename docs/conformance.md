# 仕様適合表

[architecture.md](architecture.md) §16.2・§16.3 と現行コードの対応（2026-11-12時点の静的照合）。試験名は根拠であり、本表の作成時に実行したという意味ではない。過去の実行結果は [progress.md](progress.md)、残課題は [known-limitations.md](known-limitations.md) を参照。

**完了**は条件全体の実装と必要な検証を確認した状態、**部分**は実装または試験があるが条件全体の達成を確認できない状態、**未実装**は対象機能がない状態。シミュレーションやモックの結果は実Discord・複数ホストでの保証に代用しない。

workspace testは統合後のmainでWindows/Rust 1.99.0上に647件成功・失敗0・無視1（手動ベンチ）を2回連続で確認した（[progress.md](progress.md)）。T04 fixtureの不安定要因（feature構成の異なるrlib混在）は修正済み。この確認はWindowsのみで、Linuxとの区別を保つ。

## §16.2 完了条件

| 条件 | 状況 | 根拠となるファイル・試験 | 未検証範囲・残作業 |
|---|---|---|---|
| fmt check | 完了 | `src/` で `cargo fmt --all --check` がWindowsで成功 | 他環境での実行は未確認 |
| clippy 全target・警告禁止 | 完了 | `src/` で `cargo clippy --offline --workspace --all-targets -- -D warnings` がWindowsで成功 | Linuxでの実行は未確認 |
| workspace test・マクロ成功/失敗 | 部分 | `src/framework/tests/compile_fail.rs` `command_diagnostics`、`src/framework/tests/cog_extensions.rs` `typed_commands_and_events_share_the_injected_instance` | Windowsでは完走（647件）。Linuxと未試験の障害条件 |
| Windows/Ubuntu CI | 部分 | `.github/workflows/ci.yml`、progressのローカルWindows記録 | 現統合版のActions実行結果とLinux |
| CogなしハイブリッドPing | 部分 | `src/framework/examples/hybrid_bot.rs`、`src/tests/e2e_shard_runtime.rs` `botd_real_gateways_route_hybrid_commands_isolate_bots_recover_and_drain` | 実Discord接続・応答 |
| Cogの状態共有・イベント・コマンド | 部分 | `src/framework/tests/cog_extensions.rs` `typed_commands_and_events_share_the_injected_instance`、`src/framework/tests/example_cog_counter.rs` `ready_event_uses_the_cog_and_does_not_reset_counter` | 実WorkerのCogライフサイクル接続・実Discord |
| CLI複数Bot選択と管理 | 部分 | `src/tests/botctl_daemon.rs` `multiple_bots_use_real_states_and_graceful_lifecycle_by_name_id_and_prefix` | update/rollback等の未実装操作、遠隔管理 |
| 複数Shard配置・監視 | 部分 | `src/tests/e2e_shard_runtime.rs` `botd_real_gateways_route_hybrid_commands_isolate_bots_recover_and_drain`、`src/tests/cluster_sim.rs` `t10_three_nodes_match_configured_placement_for_multiple_bots` | 別ホスト実配置と本番監視 |
| Core更新と接続維持・旧応答 | 部分 | `src/tests/hot_swap_scenarios.rs` `t04_real_five_second_handler_drains_while_v2_serves_new_requests`、`t04_external_response_stays_with_v1_while_v2_accepts_new_work` | 実Discord WebSocket/REST、Shard Core全資源のdrain |
| Gateway並行更新 | 部分 | `src/tests/gateway_switch.rs` `t07_standby_never_admits_commands_and_order_is_strict`、`t08_old_work_can_issue_rest_after_pause_and_routes_survive_switch` | 実Gateway/WebSocketへの統合 |
| 更新失敗時の維持・切戻し | 部分 | `src/tests/hot_swap_scenarios.rs` `t05_candidate_crashes_before_ready_while_v1_keeps_its_process_and_state`、`t06_crash_after_commit_restarts_v2_without_replaying_the_failed_request` | 全Shard一括rollback、実Discord外部副作用 |
| 分散Daemon failover/failback・分断 | 部分 | `src/tests/cluster_sim.rs` `t11_server_stop_is_detected_at_three_seconds_before_proxy_activation`、`t13_partition_preserves_old_local_admission_only_until_exact_lease_expiry` | 本番合意・lease統合、実複数ホスト・旧Discord接続 |
| Identify・Interaction・Rate Limit | 部分 | `src/tests/gateway_switch.rs` `t09_budget_and_rate_limit_defer_without_spawning_or_pausing_old`、`src/tests/testkit_selftest.rs` `mock_scripts_rest_success_rate_limit_server_error_and_interaction_ack` | 実Discordの予算、3秒制限、複数プロセス間調整 |
| token・認可・過大IPCの試験 | 部分 | `src/tests/main_security.rs` `missing_token_preserves_a_safe_configuration_diagnostic`、`src/tests/rbac_audit.rs` `t20_every_unauthenticated_remote_operation_is_denied_and_audited`、`src/src/worker.rs` `rejects_oversized_and_truncated_frames` | 全ログ経路、Windows ACL、実遠隔管理系の統合 |
| 再現可能なベンチマーク | 部分 | [benchmarks.md](benchmarks.md)、`src/benches/ipc_roundtrip.rs` | Linux、RSS/Shard・比較対象・負荷時目標の一式 |
| 指定日本語文書 | 部分 | `README.md`、`docs/developer-guide.md`、`docs/operator-guide.md`、`docs/ipc-protocol.md`、`docs/security.md`、`docs/benchmarks.md` | 実装変更に伴う継続的な照合 |
| 未実装・保証限界の記録 | 部分 | [known-limitations.md](known-limitations.md)、本表 | 最新コードとの継続的な照合 |

## §16.3 最低テストシナリオ

| ID・条件 | 状況 | 根拠となるファイル・試験名 | 未検証範囲 |
|---|---|---|---|
| T01 `/ping` / `!ping` | 部分 | `src/tests/e2e_shard_runtime.rs` `botd_real_gateways_route_hybrid_commands_isolate_bots_recover_and_drain` | 模擬Discord。実Discordでの登録・配送 |
| T02 自動収集・重複 | 部分 | `src/framework/tests/duplicate.rs` `duplicate_names_are_rejected`、`src/framework/tests/cog_collision.rs` `duplicate_cog_command_names_are_rejected_before_any_dispatch` | 全構成・プラットフォームでの収集 |
| T03 Cogあり/なし・状態共有 | 部分 | `src/framework/tests/cog_extensions.rs` `typed_commands_and_events_share_the_injected_instance`、`src/framework/tests/example_cog_counter.rs` `prefix_slash_and_plain_command_share_one_service` | 実WorkerへのCog接続 |
| T04 5秒処理中のCore切替 | 部分 | `src/tests/hot_swap_scenarios.rs` `t04_real_five_second_handler_drains_while_v2_serves_new_requests`、`t04_external_response_stays_with_v1_while_v2_accepts_new_work` | 実Discord接続・外部副作用、全資源 |
| T05 Ready前クラッシュ | 部分 | `src/tests/hot_swap_scenarios.rs` `t05_candidate_crashes_before_ready_while_v1_keeps_its_process_and_state` | 実Discord・全障害タイミング |
| T06 切替直後クラッシュ | 部分 | `src/tests/hot_swap_scenarios.rs` `t06_crash_after_commit_restarts_v2_without_replaying_the_failed_request`、`t06_post_commit_unknown_outcome_is_recorded_for_management` | 実外部副作用・永続事故記録 |
| T07 Gateway並行Standby | 部分 | `src/tests/gateway_switch.rs` `t07_standby_never_admits_commands_and_order_is_strict` | 状態機械。実WebSocket並行起動 |
| T08 Gateway Active切替 | 部分 | `src/tests/gateway_switch.rs` `t08_old_work_can_issue_rest_after_pause_and_routes_survive_switch` | 実GatewayとDiscord RESTへの接続 |
| T09 Identify・Rate Limit延期 | 部分 | `src/tests/gateway_switch.rs` `t09_budget_and_rate_limit_defer_without_spawning_or_pausing_old`、`src/tests/gateway_rolling_sim.rs` `identify_deferral_reset_and_shared_rest_429` | 実Discordと本番Gateway切替 |
| T10 3ホストへの配置 | 部分 | `src/tests/cluster_sim.rs` `t10_three_nodes_match_configured_placement_for_multiple_bots` | 決定的シミュレーション。実ホスト配置 |
| T11 Server停止と引継ぎ | 部分 | `src/tests/cluster_sim.rs` `t11_server_stop_is_detected_at_three_seconds_before_proxy_activation` | 実ホスト・本番quorum/leaseと実Discord復旧 |
| T12 安定後の自動返却 | 部分 | `src/tests/cluster_sim.rs` `t12_recovered_primary_waits_thirty_stable_seconds_then_receives_new_epoch`、`t12_flapping_primary_restarts_stability_and_does_not_bounce_ownership` | 実ホスト・本番所有権統合 |
| T13 分断と旧Discord接続 | 部分 | `src/tests/cluster_sim.rs` `t13_partition_preserves_old_local_admission_only_until_exact_lease_expiry`、`src/tests/failover_ownership.rs` `t13_partition_keeps_old_discord_alive_until_lease_expires_without_double_active` | 模擬時計・所有権。実分断とDiscord接続 |
| T14 quorum喪失 | 部分 | `src/tests/cluster_sim.rs` `t14_minority_never_promotes_or_renews_and_recovers_after_quorum_returns`、`src/tests/ownership.rs` `quorum_startup_refuses_without_backend` | 本番合意ライブラリ統合・実分断 |
| T15 IPC遅延・切断・満杯 | 部分 | `src/tests/testkit_selftest.rs` `mock_scripts_rest_success_rate_limit_server_error_and_interaction_ack`、`src/tests/event_seq.rs` `ordered_admission_gap_duplicate_and_queue_backpressure` | 実Workerの障害注入とHeartbeatへの影響 |
| T16 View/Modalと更新 | 部分 | `src/framework/tests/interactions.rs` `routes_across_independent_registries_and_errors`、`src/tests/state_store.rs` `view_and_modal_states_restore_in_a_fresh_reader_until_expiry` | メモリストアと模擬ルーティング。永続DB・実Worker切替 |
| T17 wait_forと更新 | 部分 | `src/framework/tests/interactions.rs` `wait_cancelled_on_drain_or_closed_signal`、`src/tests/state_store.rs` `wait_for_completes_or_cancels_for_drain_and_closed_signal` | 実Worker drain接続・永続Subscription配送 |
| T18 複数Bot同時更新 | 部分 | `src/tests/e2e_shard_runtime.rs` `botd_real_gateways_route_hybrid_commands_isolate_bots_recover_and_drain`、`src/tests/shard_runtime.rs` `multiple_bots_and_shards_keep_credentials_and_paths_separate` | 同時更新自体、実Discordと権限分離の統合 |
| T19 未知IPCバージョン/Capability | 部分 | `src/tests/ipc_handshake.rs` `t19_real_worker_negotiates_and_rejects_incompatible_hello`、`src/tests/hot_swap_scenarios.rs` `t19_unknown_required_hello_capability_is_recorded_and_v1_state_survives` | 全将来バージョン・実Discord負荷下 |
| T20 未認証遠隔操作 | 部分 | `src/tests/rbac_audit.rs` `t20_every_unauthenticated_remote_operation_is_denied_and_audited`、`src/tests/tls_transport.rs` `untrusted_expired_unauthorized_and_ambiguous_client_certificates_are_rejected` | 実遠隔管理経路・永続監査。現CLI管理はローカル限定 |

