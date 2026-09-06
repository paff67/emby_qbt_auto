#!/usr/bin/env bash
set -euo pipefail
release_dir=${1:-/opt/emby_qbt_auto/current}
config=${QBT_ORCH_CONFIG:-/etc/qbt-orchestrator/config.json}
state_db=$(mktemp /tmp/qbt-orchestrator-v3.XXXXXX.sqlite)
trace_file=$(mktemp /tmp/qbt-orchestrator-v3-trace.XXXXXX.json)
trap 'rm -f "$state_db" "$state_db-wal" "$state_db-shm" "$trace_file"' EXIT

cd "$release_dir"
python3 -m pytest -q
python3 -m qbt_orchestrator.cli migrate --state-db "$state_db" --config "$config"
python3 -m qbt_orchestrator.cli migrate --apply --state-db "$state_db" --config "$config"
python3 -m qbt_orchestrator.cli once --dry-run --state-db "$state_db" --config "$config"
python3 -m qbt_orchestrator.cli daemon --dry-run --max-safety-ticks 3 --safety-interval 0 --state-db "$state_db" --config "$config"
python3 -m qbt_orchestrator.cli status --json --state-db "$state_db" --config "$config"
python3 -m qbt_orchestrator.cli trace --json --state-db "$state_db" --config "$config" > "$trace_file"
if grep -E 'torrents/delete|deleteFiles|os\.remove|unlink|rmtree' "$trace_file"; then
  echo "dry-run gate failed: destructive action found" >&2
  exit 1
fi
