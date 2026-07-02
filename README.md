# emby_qbt_auto

qBittorrent 编排器、rclone 上传、gdrive-backfill 刮削、Emby 精准刷新自动化的重构仓库。

## 当前状态

- `legacy/live_20260702/` 保存 2026-07-02 从 US1 `/opt/qbt-orchestrator` 复制的非敏感现行代码快照。
- `tests/legacy_live/` 是同一批 legacy 测试的本地可运行副本，用于重构前回归。
- 生产配置、SQLite 状态库、rclone 配置、日志、cookie/token 不进入仓库。

## 本地验证

```powershell
python -m pytest -q
```

当前基线：11 passed, 1 warning。

## 设计来源

完整设计方案见本机：
`C:/Users/paff/Documents/tem/docs/superpowers/specs/2026-07-02-qbt-orchestrator-daemon-upgrade-design.md`
