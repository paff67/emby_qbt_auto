#!/usr/bin/env bash
set -euo pipefail
ts=$(date +%Y%m%d-%H%M%S)
backup=/var/lib/qbt-orchestrator/recovery/pre-daemon-v2-$ts
install -d "$backup"
cp -a /opt/qbt-orchestrator "$backup/opt-qbt-orchestrator"
cp -a /etc/qbt-orchestrator "$backup/etc-qbt-orchestrator"
cp -a /var/lib/qbt-orchestrator/state.sqlite* "$backup/" 2>/dev/null || true
echo "$backup"
