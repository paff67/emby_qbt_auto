# US1 qBT/Emby/rclone 重构前只读基线（2026-07-02）

## 连接与权限

- SSH alias：`paff-vps`
- SSH HostName：`ssh.paff-67.top`
- 登录用户：`paffops`
- 主机名：`racknerd-5431ac0`
- `paffops` 在 `docker` 和 `sudo` 组，但 `sudo -n true` 失败：当前无免密 sudo。
- 本轮未修改 VPS 状态；未读取 root-only 的 `/etc/qbt-orchestrator/config.json`、`/var/lib/qbt-orchestrator/state.sqlite`、`/opt/qbt/gdrive-backfill/*` 详细内容、root rclone 配置。

## 现行 qBT 栈

- qBT container：`qbittorrent`，image `lscr.io/linuxserver/qbittorrent:latest`，运行约 40h。
- qBT version：`v5.1.4`。
- Compose：`/opt/qbt/docker-compose.yml`。
- Mounts：`/opt/qbt/config -> /config`，`/data/downloads -> /downloads`。
- Ports：`8081:8080`，`6881/tcp+udp`。
- Host direct Web API：HTTP 401（认证保护）；container-local API 可用。
- qBT prefs 摘要：
  - `preallocate_all=False`
  - `incomplete_files_ext=False`（设计草案建议后续 guard 为 true，需要兼容评估）
  - `save_path=/downloads/active`
  - `temp_path_enabled=True`
  - `temp_path=/downloads/incomplete`
  - `max_active_downloads=6`
  - `max_active_torrents=20`

## qBT 当前 torrent 摘要

基于 `GET /api/v2/sync/maindata?rid=0`：

- `full_update=True`，`torrent_count=169`
- categories：`auto=92`，`<none>=77`
- states：`stoppedDL=91`，`missingFiles=67`，`forcedMetaDL=9`，`stalledDL=1`，`stoppedUP=1`
- managed auto/category-or-tag：`92`
- total size：约 `927.7 GiB`
- remaining：约 `905.0 GiB`

## 磁盘

- `/dev/vda2`：62G total / 53G used / 6.4G available / 90%
- `/data/downloads`：约 23G
- `/data/downloads/incomplete`：约 11G
- 当前距离设计要求的 2GB emergency floor 不远，planner 已在日志中以约 2.9G budget 保守运行。

## 现行 orchestrator

- Unit：`qbt-orchestrator.timer` 每 3 分钟触发 `qbt-orchestrator.service`。
- Service：`Type=oneshot`，`User=root`，`ExecStart=/usr/bin/flock -n /run/qbt-orchestrator.lock /usr/bin/python3 /opt/qbt-orchestrator/orchestrator.py --config /etc/qbt-orchestrator/config.json`。
- 最新运行状态：service 正常退出，timer active。
- 日志近期重复：`size-aware plan selected=2/77, stable=2/4, probe=0/1, overflow=0/1, cooling=4, budget=2.9G, remaining=2.0G`，`active_downloads=2/2, free=6.4G`。
- 现行代码指纹：
  - `/opt/qbt-orchestrator/orchestrator.py`：68668 bytes，sha256 prefix `99fa6a045a2e7d05`
  - `/opt/qbt-orchestrator/qbt_add_checked.py`：44616 bytes，sha256 prefix `7d248016c8ec667e`
  - `/opt/qbt-orchestrator/qbt_add_webui.py`：35759 bytes，sha256 prefix `b0346b279e80da98`
  - `/opt/qbt-orchestrator/junk_rules.py`：2395 bytes，sha256 prefix `a656cb6b480ccc39`

## 现行代码边界（静态）

- 单文件 `orchestrator.py`，类 `Orchestrator`，约 94 个函数/方法。
- `run()` 每次全量 `torrents/info`，再对 managed torrent 读取 `torrents/files`。
- 当前已具备：size-aware planner、batch、slow cooldown、rclone copy/size check、qBT-aware deleteFiles 完整清理、observe metadata 逻辑、部分测试。
- 当前缺口与设计目标一致：无常驻 daemon、多速率 loop、`sync/maindata` cache、DbActor、异步 upload worker、SQLite v2 observability、Telegram 控制面、media pipeline/Emby 刷新闭环。

## Emby / rclone mount

- Emby container：`emby`，image `lscr.io/linuxserver/emby:latest`，version `4.9.5.0`。
- Mounts：`/mnt/gcrypt -> /media/gcrypt`，`/opt/emby/config -> /config`，`/opt/emby/transcode -> /transcode`。
- Public info endpoint `http://127.0.0.1:8096/System/Info/Public` 可用。
- `rclone-gcrypt-emby.service` active，root 运行，只读挂载 `gcrypt:` 到 `/mnt/gcrypt`。
- Unit 关键参数：`--read-only`，`--vfs-cache-mode full`，`--vfs-cache-max-size 12G`，`--vfs-cache-min-free-space 2G`，`--tpslimit 8`。
- `paffops` 的 rclone 无配置；真实 `gcrypt:` remote 在 `/root/.config/rclone/rclone.conf`，本轮未读取。

## gdrive-backfill / scraper

- `gdrive-backfill-flaresolverr` container 正常运行。
- `/opt/qbt/gdrive-backfill` 目录对 `paffops` 无权限；未读取 backfill env、state、日志。
- 设计要求后续把定向 `scrape-one` 纳入 UploadWorker + IoGovernor，禁止 scraper 直接写远端。

## 部署前阻塞

后续真正替换 `/opt/qbt-orchestrator`、写 `/etc/systemd/system/*`、迁移 `/var/lib/qbt-orchestrator/state.sqlite`、读取 backfill/root rclone 配置都需要 root。当前 Codex 无免密 sudo，必须由用户在交互 SSH 中执行 sudo 命令，或调整免密 sudo/临时授权后再让 Codex 执行。
