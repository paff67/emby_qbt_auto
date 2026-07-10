# emby_qbt_auto

qBittorrent 编排器、rclone 上传、gdrive-backfill 刮削、Emby 精准刷新自动化的重构仓库。

## 当前状态

- `src/qbt_orchestrator/` 是 daemon v2 的模块化实现。
- `qbt_orchestrator/` 是 src-layout 的运行入口兼容包装，支持在 release 目录直接执行 `python -m qbt_orchestrator.cli`。
- `tests/` 保留 daemon v2 当前实现所需的 fake qBT/rclone/filesystem/Telegram/Emby/gdrive-backfill 测试与核心回归测试。
- 旧 live 快照/legacy smoke tests 已从 GitHub 工作树移除；如需追溯可查看 Git 历史，不再作为当前发布资产携带。
- 生产配置、SQLite 状态库、rclone 配置、日志、cookie/token 不进入仓库。

## 本地验证

```powershell
python -m pytest -q
python tests/test_new_system_behaviors.py
```

## CLI dry-run

```powershell
$env:PYTHONPATH='src'
python -m qbt_orchestrator.cli migrate --dry-run --state-db .tmp-state.sqlite
python -m qbt_orchestrator.cli once --dry-run --state-db .tmp-state.sqlite
python -m qbt_orchestrator.cli status --json --state-db .tmp-state.sqlite
```

## 部署资产

- `deploy/systemd/qbt-orchestrator-daemon.service`
- `deploy/systemd/qbt-orchestrator-daemon.env.example`
- `deploy/scripts/backup-live.sh`
- `deploy/scripts/install-release.sh`
- `deploy/scripts/run-dry-run.sh`
- `deploy/scripts/rollback.sh`

生产部署必须先 backup + migrate dry-run + once dry-run + daemon dry-run，再切换旧 timer。

## 设计来源

完整设计方案见本机：
`C:/Users/paff/Documents/tem/docs/superpowers/specs/2026-07-02-qbt-orchestrator-daemon-upgrade-design.md`

VPS 真实情况记录：

- `docs/prep/vps-baseline-20260702.md`
- `docs/prep/root-verification-20260702.md`
- `docs/traceability/requirements-map.md`
