# qBT Host HTTP API 模式

默认 qBT 客户端仍使用 `QBT_ORCH_QBT_API_MODE=docker`，即 `docker exec qbittorrent curl ...`。这条路径稳定但会把高频 API 调用成本转嫁到 `dockerd/containerd`。

`QBT_ORCH_QBT_API_MODE=host` 会让 daemon 直接访问 host 端口：

```env
QBT_ORCH_QBT_API_MODE=host
QBT_ORCH_QBT_API_BASE=http://127.0.0.1:8081
QBT_ORCH_QBT_USERNAME=<qbt-webui-user>
QBT_ORCH_QBT_PASSWORD=<qbt-webui-password>
```

Host 模式行为：

- `POST /api/v2/auth/login` 获取 `SID` cookie；
- 后续 `GET/POST` 复用 cookie；
- 收到 `401/403` 时清 cookie 并重登一次；
- qBT 表单写 API 继续使用 `application/x-www-form-urlencoded`；
- `QBT_ORCH_QBT_API_TIMEOUT_SEC` 和 `QBT_ORCH_QBT_API_MAX_RPS` 同时适用于 docker/host 两种模式。

上线前检查：

```bash
curl -i --connect-timeout 2 --max-time 5 http://127.0.0.1:8081/api/v2/app/version
```

如果返回 `401 Unauthorized`，必须在 `/etc/qbt-orchestrator/daemon.env` 配置 qBT WebUI 用户名/密码，或在 qBT WebUI 中显式信任 host->container 的调用源。

回滚：

```env
QBT_ORCH_QBT_API_MODE=docker
```

改完后重启 `qbt-orchestrator-daemon.service`。Host 模式只降低 Docker runtime/API 调用 CPU；不会降低 qBT 下载本身的写盘、网络和内存占用。
