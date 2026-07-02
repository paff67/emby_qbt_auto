#!/usr/bin/env bash
set -euo pipefail
release_dir=${1:?release dir required}
install -d /opt/emby_qbt_auto/releases
ln -sfn "$release_dir" /opt/emby_qbt_auto/current
python3 -m qbt_orchestrator.cli migrate --dry-run --config /etc/qbt-orchestrator/config.json
python3 -m qbt_orchestrator.cli once --dry-run --config /etc/qbt-orchestrator/config.json
