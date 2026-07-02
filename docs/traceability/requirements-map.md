# Requirements Traceability Map

| Requirement source | VPS/runtime fact | Implementation module | Tests |
|---|---|---|---|
| 常驻 daemon + 多速率 loop | 旧系统是 3 分钟 timer oneshot；新 VPS service 已切到 `qbt-orchestrator-daemon.service` | `qbt_orchestrator.service.DaemonRuntime`, `LoopTask`, `qbt_orchestrator.daemon.SafetyMonitor` | `test_daemon_runtime_runs_safety_ticks_and_persists_disk_state`, `test_daemon_runtime_runs_design_multirate_loops_and_records_events`, `test_daemon_safety_loop_only_uses_allowed_operations_and_pauses_below_floor` |
| qBT sync/maindata delta cache | qBT v5.1.4 支持 maindata | `qbt_orchestrator.qbt_sync` | `test_qbt_sync_full_update_rebuilds_and_unhealthy_preserves_cache` |
| 2GiB emergency disk floor | VPS 62G 小盘，现网约 6.4G free | `policies.disk` | `test_disk_pressure_and_emergency_action_do_not_need_db` |
| Active/Soak/Dead/Carousel + seq_dl policy | 现网有 stopped/forcedMeta/missingFiles 混合队列 | `policies.health`, `policies.download_mode` | `test_health_state_machine_and_seq_dl_policy` |
| SQLite v2 additive migration | legacy `state.sqlite` 无 v2 表 | `qbt_orchestrator.db` | `test_sqlite_migration_db_actor_readonly_and_job_recovery` |
| UploadWorker + rclone verify gate | root remote `gcrypt:` | `qbt_orchestrator.upload` | `test_rclone_upload_worker_verify_failure_does_not_cleanup_and_success_full_allows_delete` |
| Media pipeline + Emby precise refresh | Emby container path `/media/gcrypt` | `qbt_orchestrator.media` | `test_media_pipeline_groups_multi_cd_passthrough_and_emby_precise_refresh` |
| scraper 不得直接写远端 | backfill 当前可直接上传 sidecar，v2 要收口 | `integrations.gdrive_backfill` | `test_sidecar_scraper_guard_blocks_remote_writes` |
| Telegram auth/approval | 设计要求 viewer/operator/admin；VPS 未配置 token 时不启动外联 | `telegram_control`, `integrations.telegram`, `service.TelegramSupervisor`, `runtime.CommandProcessor` | `test_telegram_auth_approval_and_duplicate_click_idempotency`, `test_build_telegram_supervisor_from_env_requires_token_and_parses_roles`, `test_daemon_runtime_processes_queued_bot_commands_after_safety_tick` |
| CLI dry-run/status/trace/migrate | VPS 无 pytest/sqlite3 CLI，需 python3 可执行 | `qbt_orchestrator.cli` | `test_cli_status_trace_migrate_and_events_json` |
| redaction | 不允许 token/magnet/rclone config 泄漏 | `observability.redact` | `test_redaction_masks_tokens_magnets_and_rclone_config_paths` |
