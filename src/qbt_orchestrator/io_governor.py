from __future__ import annotations

import json
import sqlite3
import time
from dataclasses import dataclass
from enum import IntEnum
from pathlib import Path
from typing import Any, Callable

from .db import readonly_connect, write_transaction
from .observability import redact


class JobPriority(IntEnum):
    EMERGENCY_CONTROL = 0
    FULL_TORRENT_RELEASE_UPLOAD = 10
    PREEMPTION_RELEASE_UPLOAD = 15
    BATCH_DELIVERY_UPLOAD = 50
    SIDECAR_UPLOAD = 70
    MEDIA_PIPELINE = 80


def _connect(path: str | Path) -> sqlite3.Connection:
    return readonly_connect(path)


@dataclass(frozen=True)
class RcloneLimits:
    transfers: int
    checkers: int
    bwlimit: str | None
    state: str = "ok"


@dataclass(frozen=True)
class UploadBackpressureDecision:
    allow_new_upload_jobs: bool
    reason: str
    pending_bytes: int
    oldest_pending_sec: int
    pending_jobs: int
    candidate_bytes: int = 0
    projected_bytes: int = 0


class IoGovernor:
    """Small single-disk VPS I/O governor for rclone command shaping.

    The design keeps expensive I/O checks out of the 2s safety loop.  This
    object only consumes lightweight providers supplied by the caller and
    returns command-line limits for rclone.
    """

    def __init__(
        self,
        iowait_provider: Callable[[], float] | None = None,
        free_bytes_provider: Callable[[], int] | None = None,
        iowait_warn_percent: float = 20.0,
        iowait_critical_percent: float = 35.0,
        enabled: bool = False,
    ):
        self.iowait_provider = iowait_provider or (lambda: 0.0)
        self.free_bytes_provider = free_bytes_provider or (lambda: 1 << 60)
        self.iowait_warn_percent = float(iowait_warn_percent)
        self.iowait_critical_percent = float(iowait_critical_percent)
        self.enabled = bool(enabled)
        self._last_snapshot: dict[str, Any] = {}

    def rclone_limits(self, default_transfers: int = 4, default_checkers: int = 8) -> RcloneLimits:
        iowait = float(self.iowait_provider())
        free_bytes = int(self.free_bytes_provider())
        gib = 1024**3
        state = "ok"
        transfers = int(default_transfers)
        checkers = int(default_checkers)
        bwlimit: str | None = None
        if not self.enabled:
            self._last_snapshot = {
                "component": "io_governor",
                "state": "disabled",
                "iowait_percent": iowait,
                "free_bytes": free_bytes,
                "transfers": transfers,
                "checkers": checkers,
                "bwlimit": None,
            }
            return RcloneLimits(transfers=transfers, checkers=checkers, bwlimit=None, state="disabled")
        if iowait >= self.iowait_critical_percent or free_bytes < 3 * gib:
            state = "critical"
            transfers = 1
            checkers = min(checkers, 2)
            bwlimit = "2M"
        elif iowait >= self.iowait_warn_percent or free_bytes < 4 * gib:
            state = "watch"
            transfers = min(transfers, 2)
            checkers = min(checkers, 4)
            bwlimit = "8M"
        self._last_snapshot = {
            "component": "io_governor",
            "state": state,
            "iowait_percent": iowait,
            "free_bytes": free_bytes,
            "transfers": transfers,
            "checkers": checkers,
            "bwlimit": bwlimit,
        }
        return RcloneLimits(transfers=transfers, checkers=checkers, bwlimit=bwlimit, state=state)

    def last_snapshot(self) -> dict[str, Any]:
        return dict(self._last_snapshot)


class UploadBackpressurePolicy:
    """Gate new upload/file-batch work when upload cleanup is falling behind."""

    def __init__(
        self,
        max_backlog_bytes: int = 20 * 1024**3,
        max_oldest_pending_sec: int = 3600,
        now: Callable[[], int] | None = None,
    ):
        self.max_backlog_bytes = int(max_backlog_bytes)
        self.max_oldest_pending_sec = int(max_oldest_pending_sec)
        self.now = now or (lambda: int(time.time()))

    def evaluate(
        self,
        state_db: str | Path,
        candidate_bytes: int = 0,
        *,
        disk_releasing: bool = False,
    ) -> UploadBackpressureDecision:
        now = int(self.now())
        con = _connect(state_db)
        rows = [
            dict(r)
            for r in con.execute(
                "select id,payload_json,created_at,updated_at from torrent_jobs "
                "where job_type in ('upload','sidecar_upload') "
                "and state in ('queued','running','verify_pending','retry_wait')"
            )
        ]
        con.close()
        pending_bytes = 0
        oldest = 0
        for row in rows:
            try:
                payload = json.loads(row.get("payload_json") or "{}")
            except json.JSONDecodeError:
                payload = {}
            pending_bytes += int(payload.get("size") or 0)
            created_at = int(row.get("created_at") or row.get("updated_at") or now)
            oldest = max(oldest, max(0, now - created_at))
        projected_bytes = pending_bytes + int(candidate_bytes)
        reason = "ok"
        allow = True
        if projected_bytes > self.max_backlog_bytes:
            allow = False
            reason = "upload_backlog_over_limit"
        elif oldest > self.max_oldest_pending_sec:
            allow = False
            reason = "oldest_upload_pending_over_limit"
        if disk_releasing and not allow:
            allow = True
            reason = "disk_releasing_bypass"
        return UploadBackpressureDecision(
            allow_new_upload_jobs=allow,
            reason=reason,
            pending_bytes=pending_bytes,
            oldest_pending_sec=oldest,
            pending_jobs=len(rows),
            candidate_bytes=int(candidate_bytes),
            projected_bytes=projected_bytes,
        )

    def record(self, state_db: str | Path, decision: UploadBackpressureDecision, torrent_hash: str | None = None) -> None:
        now = int(self.now())
        data = {
            "allow_new_upload_jobs": decision.allow_new_upload_jobs,
            "reason": decision.reason,
            "pending_bytes": decision.pending_bytes,
            "oldest_pending_sec": decision.oldest_pending_sec,
            "pending_jobs": decision.pending_jobs,
            "candidate_bytes": decision.candidate_bytes,
            "projected_bytes": decision.projected_bytes,
        }
        def txn(con: sqlite3.Connection) -> None:
            con.execute(
                "insert into metrics_snapshots(ts,component,metrics_json) values(?,?,?)",
                (now, "upload_backpressure", json.dumps(redact(data), ensure_ascii=False)),
            )
            if not decision.allow_new_upload_jobs:
                con.execute(
                    "insert into events_v2(ts,level,component,event_type,hash,message,data_json) values(?,?,?,?,?,?,?)",
                    (
                        now,
                        "warning",
                        "upload_backpressure",
                        "new_upload_blocked",
                        torrent_hash,
                        f"new upload blocked: {decision.reason}",
                        json.dumps(redact(data), ensure_ascii=False),
                    ),
                )

        write_transaction(state_db, txn)
