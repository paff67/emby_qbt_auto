#!/usr/bin/env bash
set -euo pipefail

COMPOSE_FILE="${COMPOSE_FILE:-/opt/qbt/docker-compose.yml}"
ENV_FILE="${ENV_FILE:-/etc/qbt-orchestrator/daemon.env}"
BACKUP_ROOT="${BACKUP_ROOT:-/var/lib/qbt-orchestrator/recovery}"
BRIDGE_PORT="${QBT_ORCH_QBT_BRIDGE_PORT:-18081}"
BRIDGE_IMAGE="${QBT_ORCH_QBT_BRIDGE_IMAGE:-alpine/socat:latest}"

mkdir -p "$BACKUP_ROOT"
ts="$(date +%Y%m%d-%H%M%S)"
compose_backup="$BACKUP_ROOT/docker-compose-qbt-host-bridge-$ts.yml"
env_backup="$BACKUP_ROOT/daemon-env-qbt-host-bridge-$ts.env"
cp -a "$COMPOSE_FILE" "$compose_backup"
cp -a "$ENV_FILE" "$env_backup"

python3 - "$COMPOSE_FILE" "$BRIDGE_PORT" "$BRIDGE_IMAGE" <<'PY'
from pathlib import Path
import sys

path = Path(sys.argv[1])
port = sys.argv[2]
image = sys.argv[3]
text = path.read_text()
lines = text.splitlines()

if "qbt-host-bridge:" not in text:
    # Add the localhost-only published bridge port to the qbittorrent service.
    out: list[str] = []
    in_qbt = False
    in_ports = False
    inserted_port = False
    for line in lines:
        stripped = line.strip()
        if line.startswith("  qbittorrent:"):
            in_qbt = True
            in_ports = False
        elif line.startswith("  ") and not line.startswith("    ") and stripped.endswith(":") and not line.startswith("  qbittorrent:"):
            if in_qbt and in_ports and not inserted_port:
                out.append(f'      - "127.0.0.1:{port}:{port}"')
                inserted_port = True
            in_qbt = False
            in_ports = False
        if in_qbt and stripped == "ports:":
            in_ports = True
        elif in_qbt and in_ports and stripped and not stripped.startswith("-") and not line.startswith("      "):
            if not inserted_port:
                out.append(f'      - "127.0.0.1:{port}:{port}"')
                inserted_port = True
            in_ports = False
        out.append(line)
    if in_qbt and in_ports and not inserted_port:
        out.append(f'      - "127.0.0.1:{port}:{port}"')
    text = "\n".join(out) + "\n"

    bridge = f'''
  qbt-host-bridge:
    image: {image}
    container_name: qbt-host-bridge
    network_mode: "service:qbittorrent"
    depends_on:
      - qbittorrent
    command:
      - "-d"
      - "-d"
      - "TCP-LISTEN:{port},fork,reuseaddr,bind=0.0.0.0"
      - "TCP:127.0.0.1:8080"
    restart: unless-stopped
'''
    text = text.rstrip() + "\n" + bridge
else:
    if f"127.0.0.1:{port}:{port}" not in text:
        text = text.replace("      - 6881:6881/udp", f"      - 6881:6881/udp\n      - \"127.0.0.1:{port}:{port}\"")

path.write_text(text)
PY

python3 - "$ENV_FILE" "$BRIDGE_PORT" <<'PY'
from pathlib import Path
import sys

path = Path(sys.argv[1])
port = sys.argv[2]
lines = path.read_text().splitlines()
remove_prefixes = ("QBT_ORCH_QBT_API_MODE=", "QBT_ORCH_QBT_API_BASE=")
out = [line for line in lines if not line.strip().startswith(remove_prefixes)]
out.append("QBT_ORCH_QBT_API_MODE=host-proxy")
out.append(f"QBT_ORCH_QBT_API_BASE=http://127.0.0.1:{port}")
path.write_text("\n".join(out) + "\n")
PY

cd "$(dirname "$COMPOSE_FILE")"
docker compose up -d qbittorrent qbt-host-bridge
sleep 5
curl -fsS "http://127.0.0.1:${BRIDGE_PORT}/api/v2/app/version"
systemctl restart qbt-orchestrator-daemon.service
sleep 5
systemctl show qbt-orchestrator-daemon.service -p ActiveState -p SubState -p NRestarts -p ExecMainPID --no-pager
echo
echo "compose_backup=$compose_backup"
echo "env_backup=$env_backup"
