# Capacity-Reclaimed Archive Tags

个人部署运维手册：自动容量回收成功后，为仍存在于 qBittorrent 的种子添加归档标签，并在标签确认前用 `tag_pending` 非阻断重试。

## 标签语义

固定归档标签（无环境变量覆盖）：

- `capacity-reclaimed`
- `hold`

行为约定：

- 使用 qBT `/api/v2/torrents/addTags`，幂等追加，不覆盖 category，不删除已有标签，不删除 torrent。
- `hold` 复用现有管理边界：带 `hold` 的种子已被 alerts / capacity / carousel / file_batch / junk_janitor / cleanup 等路径排除。
- 人工恢复下载时，先在 qBT 中移除 `capacity-reclaimed` 与 `hold`，确认磁盘空间足够后再 resume。本版本不提供自动解锁按钮。

## 状态含义

| 状态 | 含义 |
|------|------|
| `tag_pending` | 本地 payload 已删除、recheck 已提交，但两个归档标签尚未同时回读确认。保留 mutation lease，仅锁该 hash；不阻塞其他任务。 |
| `reclaimed` + `recheck_error IS NULL` | 标签已确认（`capacity-reclaimed` 与 `hold` 同时存在），lease 已释放。 |
| `reclaimed` + `recheck_error='torrent_absent_after_reclaim'` | 本地空间已回收，但 qBT 中已找不到该种子，无法打标签；lease 已释放。 |

`tag_pending` 属于：

- Python reclaim fence（`CAPACITY_RECLAIM_LOCKED_STATES`）
- 非阻断对账（`CAPACITY_NONBLOCKING_RECONCILE_STATES`）

不属于启动阻断集（`CAPACITY_BLOCKING_RECOVERY_STATES`）。

## qBT WebUI 筛选

1. 打开 qBittorrent WebUI → Torrents。
2. 使用 tag 过滤器选择 `capacity-reclaimed`。
3. 也可同时查看 `hold`，确认归档种子不会被 soak / planner 重新拉起。

## 手动重新下载

1. 在 qBT 中选中目标种子。
2. 移除标签 `capacity-reclaimed` 与 `hold`。
3. 确认 `/data/downloads`（或当前 managed root）有足够空间。
4. Resume / Start 该种子。
5. 不要在仍有 `tag_pending` 行时强行并发写入同一 hash 的 job/reservation；先让 daemon 收敛或手工收敛 DB 状态。

## 查询 pending

```sql
select id, hash, name, recheck_error, updated_at
from capacity_reclaims
where state='tag_pending'
order by updated_at;
```

## 查询“已回收但无法归档标签”的终态

```sql
select id, hash, name, recheck_error, reclaimed_at
from capacity_reclaims
where state='reclaimed'
  and recheck_error='torrent_absent_after_reclaim';
```

## 回滚限制

回滚到不认识 `tag_pending` 的旧版本前，必须确认：

```sql
select count(*) from capacity_reclaims where state='tag_pending';
```

结果必须为 `0`。若大于 0：

1. 保持新版本运行，等待非阻断对账重试完成；或
2. 停 daemon，备份 DB，手工给仍存在的 torrent 添加两个标签，将对应行收敛为 `reclaimed` 后再回滚。

## Telegram 通知要点

- 成功：info，包含种子名、hash、释放空间、两个标签、磁力链接。
- pending：warning，小时桶 dedupe；持续失败同一小时最多一条。
- torrent absent：warning，说明本地已回收但 qBT 无种子可打标签，并附带磁力链接。
