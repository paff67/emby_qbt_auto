# qBT localhost-only bridge API mode

This deployment avoids per-request `docker exec` while still using qBT's
container-local localhost trust boundary.

## Architecture

```text
qbt-orchestrator daemon
  -> http://127.0.0.1:18081
  -> Docker-published localhost-only port on qbittorrent container
  -> qbt-host-bridge sidecar sharing qbittorrent network namespace
  -> http://127.0.0.1:8080 inside qBT netns
  -> qBittorrent WebAPI
```

The public WebUI/API on `8081` remains unchanged and authenticated.  The bridge
port is published only on host loopback:

```yaml
ports:
  - 8081:8080
  - 6881:6881
  - 6881:6881/udp
  - "127.0.0.1:18081:18081"
```

The sidecar uses:

```yaml
qbt-host-bridge:
  image: alpine/socat:latest
  container_name: qbt-host-bridge
  network_mode: "service:qbittorrent"
  command:
    - "-d"
    - "-d"
    - "TCP-LISTEN:18081,fork,reuseaddr,bind=0.0.0.0"
    - "TCP:127.0.0.1:8080"
```

Because the sidecar connects to qBT via `127.0.0.1:8080` inside the shared qBT
network namespace, qBT treats requests as container-local.  The orchestrator
therefore uses `QBT_ORCH_QBT_API_MODE=host-proxy`, which disables qBT SID login
and prevents systemd EnvironmentFile password parsing issues from affecting the
API path.

## Install

```bash
cd /opt/emby_qbt_auto/current
deploy/scripts/install-qbt-host-bridge.sh
```

The script backs up:

- `/opt/qbt/docker-compose.yml`
- `/etc/qbt-orchestrator/daemon.env`

Then it updates `/etc/qbt-orchestrator/daemon.env`:

```env
QBT_ORCH_QBT_API_MODE=host-proxy
QBT_ORCH_QBT_API_BASE=http://127.0.0.1:18081
QBT_ORCH_QBT_HTTP_HOST_HEADER=127.0.0.1:8080
```

## Verify

```bash
curl -fsS -H 'Host: 127.0.0.1:8080' http://127.0.0.1:18081/api/v2/app/version
cd /opt/emby_qbt_auto/current
python3 -m qbt_orchestrator.cli qbt-api-check --config /etc/qbt-orchestrator/config.json --json
systemctl show qbt-orchestrator-daemon.service -p ActiveState -p SubState -p NRestarts -p ExecMainPID --no-pager
```

Expected `qbt-api-check` fields:

```json
{
  "mode": "host-proxy",
  "client": "QbtHttpClient",
  "api_base": "http://127.0.0.1:18081",
  "auth_mode": "none",
  "auth_enabled": false,
  "default_headers": {"Host": "127.0.0.1:8080"},
  "version": "v5.1.4"
}
```

## Rollback

Restore the backed-up compose/env files, then:

```bash
cd /opt/qbt
docker compose up -d qbittorrent
docker rm -f qbt-host-bridge 2>/dev/null || true
systemctl restart qbt-orchestrator-daemon.service
```

Or keep the bridge installed and only switch the daemon back:

```bash
python3 - <<'PY'
from pathlib import Path
p = Path('/etc/qbt-orchestrator/daemon.env')
lines = [line for line in p.read_text().splitlines() if not line.strip().startswith(('QBT_ORCH_QBT_API_MODE=', 'QBT_ORCH_QBT_API_BASE='))]
lines.append('QBT_ORCH_QBT_API_MODE=docker')
p.write_text('\n'.join(lines) + '\n')
PY
systemctl restart qbt-orchestrator-daemon.service
```
