# Daemon upgrade implementation baseline

此清单从 `2026-07-02-qbt-orchestrator-daemon-upgrade-design.md` 抽取，作为仓库实施顺序和验收门禁。

## 一次性上线基线

必须同时具备：

1. 常驻 daemon + 多速率 loop。
2. qBT `sync/maindata` delta cache，`full_update=true` 必须 clear/rebuild，并有 sync health gate。
3. 2GB emergency disk floor，DB 慢/锁表也不能阻塞暂停下载快路径。
4. Active / Soak / Dead / Carousel 状态机。
5. Soak/Dead/Carousel 禁用 seq_dl；Active 仅自动启用 seq_dl。
6. piece boundary overhead reservation。
7. Batch pipeline prefetch。
8. SQLite `torrent_jobs` + `asyncio.Queue` 唤醒。
9. `aiosqlite` + DbActor 单写队列 + readonly 连接池 + WAL。
10. rclone copy + `lsjson` size 严格校验，verify 失败不得清理本地。
11. I/O Governor + upload backpressure/graceful degradation。
12. State reconciliation 与 action_log 幂等恢复。
13. sync-health-gated orphan candidate + two-confirmation guarded quarantine。
14. ContentGate -> MediaGrouping -> UploadWorker -> MediaPipelineService -> SidecarVerified 或 PassthroughAllowed -> Emby `POST /Library/Media/Updated`。
15. Telegram supervised polling、查询、审批、通知、perf/trace。
16. SQLite structured observability：`events_v2` / `decision_log` / `metrics_snapshots`。

## 建议实施切片

### Phase 0：仓库与测试基线

- 保留 `legacy/live_20260702/` 快照。
- 建立 fake qBT / fake rclone / fake filesystem / fake Telegram 基础设施。
- 新代码必须测试先行；legacy smoke tests 保持通过。

### Phase 1：纯计算 policy 与 models

- `models.py`、`policies/disk.py`、`policies/health.py`、`policies/download_mode.py`。
- 覆盖 EMA、状态机、disk pressure、seq_dl desired-state。

### Phase 2：qBT sync 与 executor 幂等包装

- `qbt_client.py`、`qbt_sync.py`、`executor.py`。
- fake qBT 覆盖 maindata delta、full_update clear/rebuild、异常不覆盖旧缓存、toggle read-check-toggle-verify。

### Phase 3：SQLite v2 与 DbActor

- migrations、`state_store.py`、`db_actor.py`、`readonly_store.py`。
- WAL、busy_timeout、单写队列、readonly query、retention、trace indexes。

### Phase 4：daemon 多速率 loop 与安全快路径

- `daemon.py`、workers/safety_monitor、download_scheduler、maintenance。
- 验证 2 秒 loop 禁止 heavy API/os.walk/rclone，`free < 2GB` 不等待 DB。

### Phase 5：batch pipeline + upload worker + IoGovernor

- batching、reservations、torrent_jobs、UploadWorker、rclone verify、cleanup gate。
- 验证 copy 成功 verify 失败不 deleteFiles，中间 batch 不整 torrent delete/readd。

### Phase 6：media pipeline / scraper / Emby

- ContentGate、MediaGrouping、filename normalizer、sidecar staging、sidecar upload job、Emby path mapper/refresh debounce。
- 验证 unrecognized passthrough、multi-CD single group、Emby precise path。

### Phase 7：Telegram/CLI/observability

- commands/approvals/notifications/trace、Telegram polling watchdog、CLI JSON 查询。
- 验证权限、审批、降噪、redaction、trace correlation。

### Phase 8：迁移与灰度部署

- 生成 systemd daemon service/timer disable plan、state DB backup/migration、dry-run/once/reconcile。
- 不直接替换旧 orchestrator；先并行 dry-run，只读对比 planner/action，再切换。

## 上线硬门禁

- 配置 JSON 可解析，阈值单位明确。
- qBT API endpoint 与 qBT v5.1.4 兼容。
- `sync/maindata` delta 与 full_update clear/rebuild 测试通过。
- sync unhealthy/suspect 时暂停 Orphan Janitor、cleanup/quarantine、新大 batch。
- readonly 查询不进 DbActor 写队列。
- SQLite 5 天 retention、batch delete、WAL checkpoint、journal_size_limit 生效。
- `free < 2GB` 快路径可在 DB 不可写时暂停所有 qBT 下载。
- Telegram/rclone/qBT token、完整 magnet 不进入日志。
- Emby 只允许 `POST /Library/Media/Updated`，path 必须是服务端视角单组目录。
