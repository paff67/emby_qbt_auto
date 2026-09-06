# 规范媒体迁移与正式上线

## 不变量

- 目录：`gcrypt:/<番号>/`
- 视频与 NFO：`<番号> <来源标题>.<ext>` / `<番号> <来源标题>.nfo`
- NFO：`title=<番号> <来源标题>`、`originaltitle=<来源标题>`、`id/sorttitle=<番号>`
- 任何目标对象已存在时拒绝覆盖；移动后必须按 size/可用 hash 验证。
- qBT 本地文件只允许在规范视频、重写 NFO 和最终 manifest 都验证后，由 `FullTorrentCleanupRunner` 调用 `deleteFiles=true`。

## 备份

以下命令在 VPS root shell 执行；时间戳目录是唯一回滚依据：

```bash
ts=$(date +%Y%m%d-%H%M%S)
backup=/opt/qbt-orchestrator/backups/canonical-$ts
install -d -m 0700 "$backup"
python3 - <<'PY' "$backup/state.sqlite" "/var/lib/qbt-orchestrator/state.sqlite"
import sqlite3, sys
dst, src = sys.argv[1:]
with sqlite3.connect(src) as s, sqlite3.connect(dst) as d:
    s.backup(d)
PY
cp -a /etc/qbt-orchestrator/daemon.env "$backup/daemon.env"
readlink -f /opt/qbt-orchestrator/current > "$backup/release.txt"
```

不重启 qBittorrent，不移动 `/data/downloads`。服务切换只重启 `qbt-orchestrator-daemon.service`，预期中断一个 safety tick（通常小于 2 秒）。

## 生成只读计划

先从只读 Javinizer 数据库导出 `titles.json`；每项至少包含 `title` 与 `confidence`。然后仅列举远端：

```bash
report=/opt/qbt-orchestrator/reports/canonical-$ts
python3 /opt/qbt/gdrive-backfill/bin/repair_emby_layout.py \
  plan --titles "$report/titles.json" --output-dir "$report"
jq . "$report/summary.json"
jq 'group_by(.reason)|map({reason:.[0].reason,count:length})' "$report/review.json"
```

人工检查 `plan.json`、`actions.csv`、`review.json` 和冻结的 `inventory.json`。`missing_title`、`low_confidence`、`target_conflict` 保持原位，不自动修复。

## Canary、验证与回滚

先停止 daemon，避免它与历史迁移同时操作同一远端对象；qBT 下载/做种容器保持运行：

```bash
systemctl stop qbt-orchestrator-daemon.service
python3 /opt/qbt/gdrive-backfill/bin/repair_emby_layout.py \
  apply --plan "$report/plan.json" --titles "$report/titles.json" \
  --journal "$report/migration.jsonl" --report-dir "$report" \
  --batch-size 2 --state-db /var/lib/qbt-orchestrator/state.sqlite
python3 /opt/qbt/gdrive-backfill/bin/repair_emby_layout.py \
  audit --plan "$report/plan.json"
```

确认 canary：源路径消失、目标 size/hash 正确、NFO 四个 identity 字段正确、Emby 精准目录刷新成功、SQLite 只变更匹配 hash 的 upload/cleanup job。若失败：

```bash
python3 /opt/qbt/gdrive-backfill/bin/repair_emby_layout.py \
  rollback --journal "$report/migration.jsonl" \
  --nfo-journal "$report/nfo-rewrite.jsonl"
cp -a "$backup/daemon.env" /etc/qbt-orchestrator/daemon.env
old=$(cat "$backup/release.txt")
ln -sfn "$old" /opt/qbt-orchestrator/current
systemctl restart qbt-orchestrator-daemon.service
```

回滚按 journal 逆序移动；NFO 仅在备份存在时恢复。SQLite 如需回滚，必须先停 daemon，再用备份替换，禁止运行中直接覆盖 WAL 数据库。

## 全量分批与 live 配置

Canary 通过后重复 `apply --batch-size <N>`；已验证对象再次计划为空，命令幂等。迁移完成并审计 `pending=0/conflict=0` 后，将正式值写入 `/etc/qbt-orchestrator/daemon.env`：

```text
QBT_ORCH_DRY_RUN=0
QBT_ORCH_MEDIA_PIPELINE_DRY_RUN=0
QBT_ORCH_MEDIA_PROMOTION_DRY_RUN=0
QBT_ORCH_EMBY_REFRESH_DRY_RUN=0
QBT_ORCH_FULL_CLEANUP=1
QBT_ORCH_FULL_CLEANUP_DRY_RUN=0
QBT_ORCH_CLEANUP_PRESSURE_FREE_GB=5
QBT_ORCH_CLEANUP_MIN_SEED_SEC=900
QBT_ORCH_CLEANUP_MIN_RATIO=1.0
QBT_ORCH_CLEANUP_MAX_RETENTION_SEC=7200
```

```bash
systemctl restart qbt-orchestrator-daemon.service
journalctl -u qbt-orchestrator-daemon.service --since '5 minutes ago' --no-pager
python3 -m qbt_orchestrator.cli status queue --state-db /var/lib/qbt-orchestrator/state.sqlite --json
```

至少观察一小时：safety tick 持续、promotion 无 failed/conflict、Emby refresh 无 blocked、cleanup 只有 `canonical_remote_verified=true`，且 qBT 删除成功后对应 parent upload 才进入 `done`。
