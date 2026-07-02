#!/usr/bin/env bash
set -euo pipefail
cd /opt/emby_qbt_auto/current
python3 -m qbt_orchestrator.cli migrate --dry-run --config /etc/qbt-orchestrator/config.json
python3 -m qbt_orchestrator.cli once --dry-run --config /etc/qbt-orchestrator/config.json
python3 -m qbt_orchestrator.cli daemon --dry-run --config /etc/qbt-orchestrator/config.json
