# Soak Queue 运维说明

`soak_resident` 是独立于 full active slots 的常驻探测队列。它启动低速/未知速度任务，但只按短期 exposure 预留空间，默认单任务 128-512MiB，总 exposure 上限 4GiB。

当前策略更激进：`QBT_ORCH_DISK_FLOOR_GB=3` 是统一的安全地板。只要 `free_bytes - disk_floor - active/batch reservations` 仍有预算，soak 会常驻运行；历史 `QBT_ORCH_SOAK_MIN_FREE_GB` 只保留为兼容字段，不再作为 6-8GiB 的硬停止阈值。

`soak_hot` 是 resident soak 在 EMA 速度超过 `QBT_ORCH_SOAK_HOT_BPS` 并持续 `QBT_ORCH_SOAK_HOT_CONFIRM_SEC` 后的提权状态。预算不足时，它会尝试暂停非 hold、非 seed-long、非临近完成的 active 任务，释放 `active_download` reservation。

当磁盘接近地板（默认 `QBT_ORCH_DISK_FLOOR_GB + QBT_ORCH_SOAK_LOW_CAPACITY_THROTTLE_MARGIN_GB`）且 soak 速度突增时，daemon 对对应 hash 调用 qBT `setDownloadLimit`，默认限速 `QBT_ORCH_SOAK_LOW_CAPACITY_LIMIT_BPS=262144`，优先限速而不是因为速度突增停止 soak。容量恢复后会对上次低容量限速的 resident 解除限速（limit=0）。

常用检查：

```bash
python3 -m qbt_orchestrator.cli status queue --config /etc/qbt-orchestrator/config.json --json
sqlite3 /var/lib/qbt-orchestrator/state.sqlite "select desired_state,count(*),sum(reserved_bytes) from scheduler_allocations group by desired_state;"
sqlite3 /var/lib/qbt-orchestrator/state.sqlite "select kind,state,count(*),sum(bytes) from resource_reservations group by kind,state;"
sqlite3 /var/lib/qbt-orchestrator/state.sqlite "select hash,state,ema_dlspeed_bps,exposure_bytes,reason from soak_state order by updated_at desc limit 20;"
```

正常磁盘 OK 且候选存在时，qBT active-like 数量可以超过 `QBT_ORCH_ACTIVE_SLOTS=5`，但应小于 `QBT_ORCH_SOAK_MAX_QBT_ACTIVE_DOWNLOADS=16`，且 `soak_probe` 预留不超过 `QBT_ORCH_SOAK_MAX_EXPOSURE_GB`。

关键环境变量：

```env
QBT_ORCH_DISK_FLOOR_GB=3
QBT_ORCH_SOAK_MIN_FREE_GB=0
QBT_ORCH_SOAK_LOW_CAPACITY_THROTTLE_MARGIN_GB=1
QBT_ORCH_SOAK_LOW_CAPACITY_LIMIT_BPS=262144
QBT_ORCH_SOAK_THROTTLE_TRIGGER_BPS=1048576
```

回滚开关：

```env
QBT_ORCH_SOAK_ENABLED=0
```

修改后重启 `qbt-orchestrator-daemon.service`。Safety Loop 的 `<2GiB` 紧急暂停不依赖 Soak Queue，关闭 soak 不会影响 emergency pause。
