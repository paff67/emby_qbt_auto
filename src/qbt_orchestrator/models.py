from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, Optional

GIB = 1024 ** 3
MIB = 1024 ** 2

class LifecycleState(str, Enum):
    QUEUED = "queued"
    METADATA_PROBE = "metadata_probe"
    ACTIVE = "active"
    SOAK = "soak"
    DEAD = "dead"
    CAROUSEL_PROBE = "carousel_probe"
    SEEDING = "seeding"
    UPLOADING = "uploading"
    ARCHIVED = "archived"
    HOLD = "hold"
    ERROR = "error"

class DiskPressureState(str, Enum):
    OK = "ok"
    WATCH = "watch"
    GUARD = "guard"
    CRITICAL = "critical"
    EMERGENCY = "emergency"

class JobState(str, Enum):
    QUEUED = "queued"
    RUNNING = "running"
    VERIFY_PENDING = "verify_pending"
    CLEANUP_READY = "cleanup_ready"
    DONE = "done"
    FAILED = "failed"
    RETRY_WAIT = "retry_wait"
    CANCELLING = "cancelling"
    CANCELLED = "cancelled"

@dataclass(frozen=True)
class QbtConfig:
    container: str = "qbittorrent"
    api_base: str = "http://127.0.0.1:8080"
    category_auto: str = "auto"
    tag_auto: str = "auto"
    tag_hold: str = "hold"
    tag_seed_long: str = "seed-long"
    tag_no_batch: str = "no-batch"
    save_path: str = "/downloads/active"
    temp_path: str = "/downloads/incomplete"

@dataclass(frozen=True)
class DiskConfig:
    ok_free_bytes: int = 5 * GIB
    watch_free_bytes: int = 4 * GIB
    guard_free_bytes: int = 3 * GIB
    critical_free_bytes: int = 2 * GIB
    emergency_free_bytes: int = 2 * GIB
    drain_enter_bytes: int = 3 * GIB
    drain_exit_bytes: int = 5 * GIB
    explore_enter_bytes: int = 8 * GIB

@dataclass(frozen=True)
class EmbyConfig:
    enabled: bool = True
    container_media_prefix: str = "/media/gcrypt"
    debounce_sec: int = 300
    max_debounce_wait_sec: int = 900
    refresh_endpoint: str = "/Library/Media/Updated"

@dataclass(frozen=True)
class RcloneConfig:
    config: str = "/root/.config/rclone/rclone.conf"
    remote: str = "gcrypt:"
    transfers: int = 1
    checkers: int = 2

@dataclass(frozen=True)
class QbtPreferencesConfig:
    preallocate_all: bool = False
    incomplete_files_ext_desired: Optional[bool] = None

@dataclass(frozen=True)
class AppConfig:
    qbt: QbtConfig = field(default_factory=QbtConfig)
    disk: DiskConfig = field(default_factory=DiskConfig)
    emby: EmbyConfig = field(default_factory=EmbyConfig)
    rclone: RcloneConfig = field(default_factory=RcloneConfig)
    qbt_preferences: QbtPreferencesConfig = field(default_factory=QbtPreferencesConfig)
    state_db: str = "/var/lib/qbt-orchestrator/state.sqlite"
    dry_run: bool = True
    runtime_warnings: tuple[str, ...] = ()

@dataclass(frozen=True)
class TorrentSnapshot:
    hash: str
    name: str = ""
    magnet_uri: str = ""
    category: str = ""
    tags: str = ""
    state: str = ""
    amount_left: int = 0
    size: int = 0
    progress: float = 0.0
    content_path: str = ""
    save_path: str = ""
    num_seeds: int = 0
    num_peers: int = 0
    dlspeed_bps: int = 0
    upspeed_bps: int = 0
    completed_bytes: int = 0
    ratio: float = 0.0
    seeding_time: int = 0
    completion_on: int = 0
    share_limit_reached: bool = False
    has_metadata: bool | None = None
    availability: float | None = None
    last_activity: int = 0
    seen_complete: int = 0

    @classmethod
    def from_qbt(cls, payload: Dict[str, Any]) -> "TorrentSnapshot":
        has_metadata = payload.get("has_metadata")
        return cls(
            hash=payload.get("hash", ""),
            name=payload.get("name", ""),
            magnet_uri=str(payload.get("magnet_uri") or ""),
            category=payload.get("category", "") or "",
            tags=payload.get("tags", "") or "",
            state=payload.get("state", "") or "",
            amount_left=int(payload.get("amount_left") or 0),
            size=int(payload.get("size") or payload.get("total_size") or 0),
            progress=float(payload.get("progress") or 0),
            content_path=str(payload.get("content_path") or ""),
            save_path=str(payload.get("save_path") or ""),
            num_seeds=max(
                int(payload.get("num_seeds") or 0),
                int(payload.get("num_complete") or 0),
            ),
            num_peers=max(
                int(payload.get("num_peers") or 0),
                int(payload.get("num_incomplete") or 0),
            ),
            dlspeed_bps=int(payload.get("dlspeed_bps") or payload.get("dlspeed") or 0),
            upspeed_bps=int(payload.get("upspeed_bps") or payload.get("upspeed") or 0),
            completed_bytes=int(payload.get("completed_bytes") or payload.get("completed") or payload.get("downloaded") or 0),
            ratio=float(payload.get("ratio") or 0.0),
            seeding_time=int(payload.get("seeding_time") or 0),
            completion_on=int(payload.get("completion_on") or 0),
            share_limit_reached=bool(payload.get("share_limit_reached") or False),
            has_metadata=None if has_metadata is None else bool(has_metadata),
            availability=(
                None
                if payload.get("availability") is None
                else float(payload.get("availability"))
            ),
            last_activity=int(payload.get("last_activity") or 0),
            seen_complete=int(payload.get("seen_complete") or 0),
        )

@dataclass(frozen=True)
class QbtAction:
    path: str
    payload: Dict[str, Any]
    reason: str = ""

@dataclass(frozen=True)
class DiskSample:
    free_bytes: int
    state: DiskPressureState

@dataclass(frozen=True)
class BatchReservation:
    payload_bytes: int
    piece_spill_overhead_bytes: int
    filesystem_slack_bytes: int
    reserved_bytes: int
    payload_efficiency: float

@dataclass(frozen=True)
class CleanupDecision:
    allow_delete_files: bool
    state: str
    reason: str

@dataclass
class ActionLogEntry:
    path: str
    payload: Dict[str, Any]
    status: str
    dry_run: bool
    error: str | None = None
