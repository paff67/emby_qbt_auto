# Soak Queue 运维说明

`soak_resident` 是独立于 full active slots 的常驻探测队列。它启动低速/未知速度任务，但只按短期 exposure 预留空间，默认单任务 128-512MiB，总 exposure 上限 4GiB。

`soak_hot` 是 resident soak 在 EMA 速度超过 `QBT_ORCH_SOAK_HOT_BPS` 并持续 `QBT_ORCH_SOAK_HOT_CONFIRM_SEC` 后的提权状态。预算不足时，它会尝试暂停非 hold、非 seed-long、非临近完成的 active 任务，释放 `active_download` reservation。

常用检查：

```bash
python3 -m qbt_orchestrator.cli status queue --config /etc/qbt-orchestrator/config.json --json
sqlite3 /var/lib/qbt-orchestrator/state.sqlite "select desired_state,count(*),sum(reserved_bytes) from scheduler_allocations group by desired_state;"
sqlite3 /var/lib/qbt-orchestrator/state.sqlite "select kind,state,count(*),sum(bytes) from resource_reservations group by kind,state;"
sqlite3 /var/lib/qbt-orchestrator/state.sqlite "select hash,state,ema_dlspeed_bps,exposure_bytes,reason from soak_state order by updated_at desc limit 20;"
```

正常磁盘 OK 且候选存在时，qBT active-like 数量可以超过 `QBT_ORCH_ACTIVE_SLOTS=5`，但应小于 `QBT_ORCH_SOAK_MAX_QBT_ACTIVE_DOWNLOADS=16`，且 `soak_probe` 预留不超过 `QBT_ORCH_SOAK_MAX_EXPOSURE_GB`。

回滚开关：

```env
QBT_ORCH_SOAK_ENABLED=0
```

修改后重启 `qbt-orchestrator-daemon.service`。Safety Loop 的 `<2GiB` 紧急暂停不依赖 Soak Queue，关闭 soak 不会影响 emergency pause。
