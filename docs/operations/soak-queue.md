# Soak Queue 运维说明

`soak_resident` 是独立于 full active slots 的常驻探测队列。它启动低速/未知速度任务，但只按短期 exposure 预留空间，默认单任务 128-512MiB，总 exposure 上限 4GiB。

磁盘阈值以 `/etc/qbt-orchestrator/config.json` 的 `disk` 段为权威来源。对应环境变量若存在只能与配置一致，否则 daemon 拒绝启动，防止静默漂移。当前目标地板为 3GiB、drain 退出水位为 5GiB、全停保护线为 2GiB。预算仍按 `free_bytes - disk_floor - active/batch reservations` 计算。

`soak_hot` 是 resident soak 在 EMA 速度超过 `QBT_ORCH_SOAK_HOT_BPS` 并持续 `QBT_ORCH_SOAK_HOT_CONFIRM_SEC` 后的提权状态。预算不足时，它会尝试暂停非 hold、非 seed-long、非临近完成的 active 任务，释放 `active_download` reservation。

当磁盘接近地板（默认 `QBT_ORCH_DISK_FLOOR_GB + QBT_ORCH_SOAK_LOW_CAPACITY_THROTTLE_MARGIN_GB`）且 soak 速度突增时，daemon 对对应 hash 调用 qBT `setDownloadLimit`，默认限速 `QBT_ORCH_SOAK_LOW_CAPACITY_LIMIT_BPS=262144`，优先限速而不是因为速度突增停止 soak。容量恢复后会对上次低容量限速的 resident 解除限速（limit=0）。

recovery/drain mode：低于 3GiB 后不再接纳新的增长写入；优先等待上传/清理释放空间。容量检测同时检查 availability、完整种子和最近进度，缺块且长期无进度的近完成任务不会永久占用 finish-resident。

常用检查：

```bash
python3 -m qbt_orchestrator.cli status queue --config /etc/qbt-orchestrator/config.json --json
sqlite3 /var/lib/qbt-orchestrator/state.sqlite "select desired_state,count(*),sum(reserved_bytes) from scheduler_allocations group by desired_state;"
sqlite3 /var/lib/qbt-orchestrator/state.sqlite "select kind,state,count(*),sum(bytes) from resource_reservations group by kind,state;"
sqlite3 /var/lib/qbt-orchestrator/state.sqlite "select hash,state,ema_dlspeed_bps,exposure_bytes,reason from soak_state order by updated_at desc limit 20;"
```

正常磁盘 OK 且候选存在时，qBT active-like 数量可以超过 `QBT_ORCH_ACTIVE_SLOTS=5`，但应小于 `QBT_ORCH_SOAK_MAX_QBT_ACTIVE_DOWNLOADS=16`，且 `soak_probe` 预留不超过 `QBT_ORCH_SOAK_MAX_EXPOSURE_GB`。

容量阈值写入正式 JSON 配置，不再通过环境变量覆盖。可选的自动回收开关：

```env
QBT_ORCH_RECOVERY_MODE=1
QBT_ORCH_RECOVERY_ACTIVE_SLOTS=4
QBT_ORCH_RECOVERY_MAX_REMAINING_GB=1.5
QBT_ORCH_RECOVERY_MARGIN_MB=256
QBT_ORCH_FINISH_RESIDENT_MAX_STALL_SEC=1800
QBT_ORCH_CAPACITY_VIABILITY_STALE_SEC=1800
QBT_ORCH_CAPACITY_RECLAIM=1
QBT_ORCH_CAPACITY_RECLAIM_DRY_RUN=1
QBT_ORCH_CAPACITY_RECLAIM_MIN_DEAD_SEC=21600
QBT_ORCH_CAPACITY_RECLAIM_MIN_BYTES_MB=64
QBT_ORCH_CAPACITY_RECLAIM_MAX_PER_TICK=1
QBT_ORCH_CAPACITY_RECLAIM_INTERVAL_SEC=300
QBT_ORCH_CAPACITY_RECLAIM_TG_CHAT_IDS=
QBT_ORCH_SOAK_MIN_FREE_GB=0
QBT_ORCH_SOAK_LOW_CAPACITY_THROTTLE_MARGIN_GB=1
QBT_ORCH_SOAK_LOW_CAPACITY_LIMIT_BPS=262144
QBT_ORCH_SOAK_THROTTLE_TRIGGER_BPS=1048576
```

回滚开关：

```env
QBT_ORCH_SOAK_ENABLED=0
```

修改后重启 `qbt-orchestrator-daemon.service`。Safety Loop 的 `<2GiB` 紧急暂停不依赖 Soak Queue，关闭 soak 不会影响 emergency pause。回收 live 模式不会调用 qBT `torrents/delete`；它仅停止已验证的 dead torrent、删除受管 incomplete payload 并触发 recheck，保留 torrent 记录。

真实回收会将 hash、种子名、完整磁力链接、路径、释放字节数和 recheck
结果持久化到 `capacity_reclaims`，并在同一个 SQLite 事务中写入
`bot_notifications`。Telegram 发送失败沿用持久队列重试。专用 chat id 为空时，
依次回退到 `QBT_ORCH_TG_ALERT_CHAT_IDS` 和 `QBT_ORCH_TG_ADMINS`；DRY_RUN
不会创建正式回收记录或通知。
