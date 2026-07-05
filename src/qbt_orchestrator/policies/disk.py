from __future__ import annotations
from typing import Iterable, Mapping
from ..models import DiskPressureState, DiskSample, QbtAction
GIB = 1024 ** 3

def classify_disk(free_bytes: int, emergency_free_bytes: int = 2 * GIB) -> DiskSample:
    if free_bytes < int(emergency_free_bytes): state = DiskPressureState.EMERGENCY
    elif free_bytes < 3 * GIB: state = DiskPressureState.CRITICAL
    elif free_bytes < 4 * GIB: state = DiskPressureState.GUARD
    elif free_bytes < 5 * GIB: state = DiskPressureState.WATCH
    else: state = DiskPressureState.OK
    return DiskSample(free_bytes, state)

def _is_managed(torrent: Mapping[str, object]) -> bool:
    tags = str(torrent.get("tags") or "")
    return torrent.get("category") == "auto" or "auto" in [t.strip() for t in tags.split(",")]

def emergency_pause_action(torrents: Iterable[Mapping[str, object]]) -> QbtAction | None:
    stopped = {"pausedup", "pauseddl", "stoppedup", "stoppeddl"}
    hashes = [str(t["hash"]) for t in torrents if t.get("hash") and _is_managed(t) and str(t.get("state", "")).lower() not in stopped]
    if not hashes: return None
    return QbtAction("/api/v2/torrents/stop", {"hashes": "|".join(hashes)}, "disk_emergency")
