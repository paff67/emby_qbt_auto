# Root read-only verification（2026-07-02 18:36 CST）

本记录来自 `ssh -o BatchMode=yes root@ssh.paff-67.top` 只读采集。未修改 VPS 文件、未重启服务、未输出 rclone config/token/API key/cookie。

## Root 登录验证

```text
host: racknerd-5431ac0
whoami: root
uptime: 1 day 16h
```

## qBT runtime

```text
qBT version: v5.1.4
maindata rid: 1
full_update: True
torrent_count: 169
categories: auto=92, <none>=77
states: stoppedDL=92, missingFiles=67, forcedMetaDL=9, stoppedUP=1
managed_auto_or_tag: 92
total_size_gb: 927.7
remaining_gb: 905.0
```

qBT preference 摘要：

```json
{
  "preallocate_all": false,
  "incomplete_files_ext": false,
  "save_path": "/downloads/active",
  "temp_path_enabled": true,
  "temp_path": "/downloads/incomplete",
  "max_active_downloads": 6,
  "max_active_torrents": 20,
  "max_connec": 500,
  "max_connec_per_torrent": 100,
  "dl_limit": 0,
  "up_limit": 0,
  "queueing_enabled": true
}
```

设计影响：新 preferences guard 必须至少强制 `preallocate_all=false`；是否启用 `incomplete_files_ext=true` 需要灰度验证，因为现网当前为 `false`。

## qBT orchestrator config shape

当前 `/etc/qbt-orchestrator/config.json` 已读取并脱敏。关键点：

- `mode=live`
- qBT container：`qbittorrent`
- qBT API：container-local `http://127.0.0.1:8080`
- managed category/tag：`auto`
- hold/seed/no-batch tags：`hold` / `seed-long` / `no-batch`
- qBT save/temp：`/downloads/active` / `/downloads/incomplete`
- state DB：`/var/lib/qbt-orchestrator/state.sqlite`
- work dir：`/var/lib/qbt-orchestrator/work`
- log：`/var/log/qbt-orchestrator/orchestrator.log`
- rclone：root config `/root/.config/rclone/rclone.conf`，remote `gcrypt:`，`transfers=4`，`checkers=8`
- scheduler：size-aware enabled，`stable_slots=4`，`probe_slots=1`，`overflow_slots=1`，`max_active_downloads=6`
- disk：target/emergency/pause-new 均 `3 GiB`，pause-all `2 GiB`，critical `1 GiB`，max batch `12 GiB`
- slow policy enabled：min speed `262144 B/s`，cooldown `2h`，long cooldown `12h`
- dedupe：backfill DB `/opt/qbt/gdrive-backfill/state/backfill.sqlite`，normalizer `/opt/qbt/gdrive-backfill/bin/jav_name_normalize.py`，metadata timeout `900s`

## state.sqlite schema/counts

DB path：`/var/lib/qbt-orchestrator/state.sqlite`，size `1855488` bytes。

现有表与计数：

```text
checked_add_events=2323
checked_add_kv=1
checked_add_requests=114
events=11618
qbt_add_webui_jobs=5
remote_media_index=107
sqlite_sequence=3
torrent_state=136
```

现有 schema 是 legacy v1 风格：

- `torrent_state` 单表保存 batch/health/cooldown/slot 字段；尚无 v2 的 `torrent_health`、`torrent_batches`、`torrent_jobs`、`events_v2`、`decision_log`、`metrics_snapshots`。
- `events` 只有 `ts/hash/level/message`，不满足结构化 trace/decision/metrics 需求。
- `remote_media_index` 已有 `normalized_id` index，可作为 dedupe 迁移输入。

迁移要求：必须保留旧列与旧表，新增 v2 表；迁移前备份 `state.sqlite*`。

## gdrive-backfill / scraper runtime

配置摘要（敏感值未输出）：

- `RCLONE_BIN=/usr/bin/rclone`
- `RCLONE_CONFIG=/root/.config/rclone/rclone.conf`
- `WORK_ROOT=/opt/qbt/gdrive-backfill/work`
- `STATE_DB=/opt/qbt/gdrive-backfill/state/backfill.sqlite`
- `MAX_ITEMS_PER_RUN=30`
- `RCLONE_TRANSFERS=4`
- `RCLONE_CHECKERS=8`
- `MAX_SCRAPER_PARALLEL=1`
- `NORMALIZE_ENABLED=1`
- `NORMALIZE_RENAME_REMOTE=0`
- `NORMALIZE_MIN_CONFIDENCE=0.70`
- `SCRAPER_SCRIPT=/opt/qbt/gdrive-backfill/bin/javinizer_scrape_one.sh`
- `JAVINIZER_IMAGE=ghcr.io/javinizer/javinizer-go:latest`
- `JAVINIZER_SCRAPERS=javdb`
- `DOWNLOAD_JAVDB_PREVIEWS=1`
- `GENERATE_CONTACT_SHEET=1`
- roots：`gcrypt:`

最近 summary：

```text
运行时间: 2026-07-02T18:17:57+08:00
扫描文件数: 4562
发现视频数: 321
本次处理数: 0
跳过已有数: 107
清洗失败数: 209
失败数: 0
耗时: 56.0s
```

backfill DB：`/opt/qbt/gdrive-backfill/state/backfill.sqlite`，size `303104` bytes。

```text
items=442
sqlite_sequence=1
```

设计影响：现有 backfill 是“定时全量扫描 + 直接上传 sidecar”的模式；新方案需改为由 qBT daemon 在 UploadVerified 后按 media group 定向调用 scraper，sidecar 上传必须走 UploadWorker/IoGovernor。

## rclone / remote capacity

Root remotes：`paff-vps-back:`、`gdrive:`、`gcrypt:`。

`rclone about gcrypt:`：

```text
total=5497558138880
used=894382617002
free=4603010736503
trashed=1204549024
other=164785375
```

## Emby

Public info：

```text
ServerName=d54da3b8ce3f
Version=4.9.5.0
Id=0eb424df32814af8b5e507b857a8b739
```

Emby mount 关系此前已确认：`/mnt/gcrypt -> /media/gcrypt`。新 Emby path mapper 应将 `gcrypt:` 远端路径映射到 Emby 容器内可见路径；现有设计草案里的 `/mnt/gdrive/Media/` 示例不适配当前 runtime，后续实现应以实际 mount `/media/gcrypt` 为准。

## 现有 VPS 测试

在 VPS 上执行：

```bash
cd /opt/qbt-orchestrator
for f in tests/test_*.py; do python3 "$f"; done
```

结果：

```text
tests/test_add_checked_policy.py: ok
tests/test_batch_safety.py: ok
tests/test_junk_rules.py: ok
tests/test_observe_policy.py: ok
```

说明：生产机无 `pytest`，但现有测试脚本的 direct-run 入口可通过。

## 迁移/部署注意事项

- 当前 `state.sqlite` 无 v2 表，需 schema migration。
- 当前 backfill DB 只有 `items` 表，不能直接满足 media pipeline DAG，需要新表维护 `media_groups`、`media_pipeline_runs`、`sidecar_manifests` 等。
- 当前 root rclone config 只应在 VPS root 环境使用，不进入仓库。
- 新 daemon 灰度时应先 dry-run/readonly compare，不直接替换 timer oneshot。
