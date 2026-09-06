#!/usr/bin/env bash
set -euo pipefail
env_file=${QBT_ORCH_ENV_FILE:-/etc/qbt-orchestrator/daemon.env}
rollback_release=${1:-}
stamp=$(date -u +%Y%m%dT%H%M%SZ)

systemctl stop qbt-orchestrator-daemon.service || true
if [[ -f "$env_file" ]]; then
  cp -a "$env_file" "${env_file}.pre-v3-rollback.${stamp}"
  python3 - "$env_file" <<'PY'
from pathlib import Path
import sys

path = Path(sys.argv[1])
updates = {
    "QBT_ORCH_SCHEDULER_ENGINE": "legacy",
    "QBT_ORCH_BACKGROUND_PERIODIC_WORKERS": "0",
    "QBT_ORCH_BATCH_PIPELINE": "0",
    "QBT_ORCH_SOAK_ENABLED": "0",
    "QBT_ORCH_FULL_CLEANUP": "0",
    "QBT_ORCH_FULL_CLEANUP_DRY_RUN": "1",
    "QBT_ORCH_FILE_BATCH_DRY_RUN": "1",
    "QBT_ORCH_UPLOAD_DRY_RUN": "1",
}
lines = path.read_text(encoding="utf-8").splitlines()
seen = set()
out = []
for line in lines:
    key = line.split("=", 1)[0].strip() if "=" in line and not line.lstrip().startswith("#") else ""
    if key in updates:
        out.append(f"{key}={updates[key]}")
        seen.add(key)
    else:
        out.append(line)
for key, value in updates.items():
    if key not in seen:
        out.append(f"{key}={value}")
path.write_text("\n".join(out) + "\n", encoding="utf-8")
PY
fi
if [[ -n "$rollback_release" ]]; then
  test -d "$rollback_release"
  ln -sfn "$rollback_release" /opt/emby_qbt_auto/current
fi
# Additive SQLite columns/tables are deliberately retained. Never delete or
# downgrade /var/lib/qbt-orchestrator/state.sqlite during rollback.
systemctl enable --now qbt-orchestrator.timer
systemctl status qbt-orchestrator.timer --no-pager -l
