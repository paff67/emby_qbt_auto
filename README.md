# emby_qbt_auto

qBittorrent 编排器、rclone 上传、gdrive-backfill 刮削、Emby 精准刷新自动化的重构仓库。

## 当前状态

- `src/qbt_orchestrator/` 是 daemon v2 的模块化实现。
- `qbt_orchestrator/` 是 src-layout 的运行入口兼容包装，支持在 release 目录直接执行 `python -m qbt_orchestrator.cli`。
- `tests/` 保留 daemon v2 当前实现所需的 fake qBT/rclone/filesystem/Telegram/Emby/gdrive-backfill 测试与核心回归测试。
- 旧 live 快照/legacy smoke tests 已从 GitHub 工作树移除；如需追溯可查看 Git 历史，不再作为当前发布资产携带。
- 生产配置、SQLite 状态库、rclone 配置、日志、cookie/token 不进入仓库。

## 规范媒体闭环

正式媒体路径固定为 `gcrypt:/<番号>/<番号> <JavDB 原始标题>.<ext>`；顶层目录保持纯番号。NFO 的 `<title>` 使用相同显示名，`<originaltitle>` 保留来源标题，`<id>` 与 `<sorttitle>` 保持纯番号。

全量上传校验后不会立即删除本地数据，而是依次经过：

1. `media_pipeline` 解析番号与受信标题并生成侧车；
2. `media_promotions` 以 `rclone moveto` 晋升到规范路径，拒绝覆盖且失败自动反向移动；
3. 所有视频晋升和 NFO/图片上传都验证成功后，才排队精准 Emby 刷新；
4. 只有 `canonical_remote_verified=true` 的清理任务可调用 qBT `deleteFiles=true`。

清理策略的 `hold`、`seed-long`、未规范验证、晋升冲突是硬阻断；其余按“磁盘压力 / ratio / 做种时间 / share limit / 最大保留时间”任一满足即释放，避免 ratio 与时间的 AND 死锁。远端迁移和回滚流程见 `docs/operations/canonical-media-migration.md`。

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

