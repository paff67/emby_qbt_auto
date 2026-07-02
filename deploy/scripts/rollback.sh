#!/usr/bin/env bash
set -euo pipefail
systemctl stop qbt-orchestrator-daemon.service || true
systemctl enable --now qbt-orchestrator.timer
systemctl status qbt-orchestrator.timer --no-pager -l
