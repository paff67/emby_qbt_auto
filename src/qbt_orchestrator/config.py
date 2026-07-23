from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Mapping

from .models import AppConfig, DiskConfig, EmbyConfig, QbtConfig, QbtPreferencesConfig, RcloneConfig


def _section(data: Mapping[str, Any], name: str) -> Mapping[str, Any]:
    value = data.get(name, {})
    return value if isinstance(value, Mapping) else {}


def load_config(path: str | Path) -> AppConfig:
    with open(path, "r", encoding="utf-8") as f:
        return load_config_from_dict(json.load(f))


def load_config_from_dict(data: Mapping[str, Any]) -> AppConfig:
    qbt_raw = _section(data, "qbt")
    paths = _section(data, "paths")
    rclone_raw = _section(data, "rclone")
    emby_raw = _section(data, "emby")
    qbt_prefs_raw = _section(data, "qbt_preferences")
    disk_raw = _section(data, "disk_pressure") or _section(data, "disk")
    warnings = ("incomplete_files_ext is observed false on VPS; v2 records drift but does not force change by default",)
    qbt = QbtConfig(
        container=str(qbt_raw.get("container", "qbittorrent")),
        api_base=str(qbt_raw.get("api_base", "http://127.0.0.1:8080")),
        category_auto=str(qbt_raw.get("category_auto", "auto")),
        tag_auto=str(qbt_raw.get("tag_auto", "auto")),
        tag_hold=str(qbt_raw.get("tag_hold", "hold")),
        tag_seed_long=str(qbt_raw.get("tag_seed_long", "seed-long")),
        tag_no_batch=str(qbt_raw.get("tag_no_batch", "no-batch")),
        save_path=str(qbt_raw.get("save_path", "/downloads/active")),
        temp_path=str(qbt_raw.get("temp_path", "/downloads/incomplete")),
    )
    rclone = RcloneConfig(
        config=str(rclone_raw.get("config", "/root/.config/rclone/rclone.conf")),
        remote=str(rclone_raw.get("remote", "gcrypt:")),
        transfers=int(rclone_raw.get("transfers", 1)),
        checkers=int(rclone_raw.get("checkers", 2)),
    )
    emby = EmbyConfig(
        enabled=bool(emby_raw.get("enabled", True)),
        container_media_prefix=str(emby_raw.get("container_media_prefix", emby_raw.get("emby_prefix", "/media/gcrypt"))).rstrip("/"),
        debounce_sec=int(emby_raw.get("debounce_sec", 300)),
        max_debounce_wait_sec=int(emby_raw.get("max_debounce_wait_sec", 900)),
    )
    ok_free_bytes = int(float(disk_raw.get("ok_free_gb", 5)) * 1024**3)
    guard_free_bytes = int(float(disk_raw.get("guard_free_gb", disk_raw.get("target_min_free_gb", 3))) * 1024**3)
    disk = DiskConfig(
        ok_free_bytes=ok_free_bytes,
        watch_free_bytes=int(float(disk_raw.get("watch_free_gb", 4)) * 1024**3),
        guard_free_bytes=guard_free_bytes,
        critical_free_bytes=int(float(disk_raw.get("critical_free_gb", 2)) * 1024**3),
        emergency_free_bytes=int(float(disk_raw.get("pause_all_downloads_free_below_gb", 2)) * 1024**3),
        drain_enter_bytes=int(float(disk_raw.get("pause_new_free_below_gb", guard_free_bytes / 1024**3)) * 1024**3),
        drain_exit_bytes=int(float(disk_raw.get("drain_exit_gb", ok_free_bytes / 1024**3)) * 1024**3),
        explore_enter_bytes=int(float(disk_raw.get("explore_free_gb", 8)) * 1024**3),
    )
    incomplete_desired = qbt_prefs_raw.get("incomplete_files_ext_desired", qbt_prefs_raw.get("incomplete_files_ext", None))
    qbt_preferences = QbtPreferencesConfig(
        preallocate_all=bool(qbt_prefs_raw.get("preallocate_all", False)),
        incomplete_files_ext_desired=None if incomplete_desired is None else bool(incomplete_desired),
    )
    return AppConfig(qbt=qbt, disk=disk, emby=emby, rclone=rclone, qbt_preferences=qbt_preferences, state_db=str(paths.get("state_db", "/var/lib/qbt-orchestrator/state.sqlite")), dry_run=bool(data.get("dry_run", True)), runtime_warnings=warnings)
