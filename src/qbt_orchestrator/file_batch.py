from __future__ import annotations

import json
import re
import sqlite3
import time
from dataclasses import dataclass
from pathlib import PurePosixPath
from typing import Any, Mapping

from .runtime import ObservabilityStore, TorrentJobRepository


@dataclass(frozen=True)
class FileBatchResult:
    scanned: int
    eligible: int
    enqueued: int
    skipped_existing: int
    dry_run: int = 0


def _connect(path) -> sqlite3.Connection:
    con = sqlite3.connect(path)
    con.row_factory = sqlite3.Row
    return con


def _safe_name(value: str, maxlen: int = 96) -> str:
    value = re.sub(r'[\\/:*?"<>|\x00-\x1f]+', "_", value).strip().strip(".") or "torrent"
    return value[:maxlen]


def _tags(torrent: Mapping[str, Any]) -> set[str]:
    return {p.strip() for p in str(torrent.get("tags") or "").split(",") if p.strip()}


def _is_managed(torrent: Mapping[str, Any]) -> bool:
    tags = _tags(torrent)
    return (str(torrent.get("category") or "") == "auto" or "auto" in tags) and "hold" not in tags


def _is_completed(torrent: Mapping[str, Any]) -> bool:
    return int(torrent.get("size") or 0) > 0 and (int(torrent.get("amount_left") or 0) == 0 or float(torrent.get("progress") or 0) >= 1.0)


class FileBatchService:
    """Level-2 file/batch loop using qBT sync snapshots only.

    This first production-safe slice does not call torrents/files, os.walk, or
    rclone.  It detects completed managed full-torrent payloads and creates a
    durable upload job for the event-driven UploadWorker.
    """

    def __init__(self, state_db, dry_run: bool = True, host_downloads: str = "/data/downloads", container_downloads: str = "/downloads", remote: str = "gcrypt:"):
        self.state_db = state_db
        self.dry_run = dry_run
        self.host_downloads = host_downloads.rstrip("/")
        self.container_downloads = container_downloads.rstrip("/")
        self.remote = remote.rstrip("/")
        self.jobs = TorrentJobRepository(state_db)
        self.obs = ObservabilityStore(state_db)

    def sync_completed(self, snapshots: Mapping[str, Mapping[str, Any]]) -> FileBatchResult:
        scanned = len(snapshots)
        eligible = 0
        enqueued = 0
        skipped_existing = 0
        for h, raw in snapshots.items():
            torrent = dict(raw)
            torrent.setdefault("hash", h)
            if not (_is_managed(torrent) and _is_completed(torrent)):
                continue
            eligible += 1
            torrent_hash = str(torrent.get("hash") or h)
            payload = self._payload_for(torrent_hash, torrent)
            if self._existing_upload_job(torrent_hash):
                skipped_existing += 1
                continue
            if self.dry_run:
                self.obs.action(torrent_hash, None, "enqueue_upload", "torrent_jobs/upload", payload, "dry_run", True)
                self.obs.event("info", "file_batch", "upload_queue_dry_run", f"would enqueue upload for {torrent_hash[:8]}", payload, hash=torrent_hash)
                continue
            job_id = self.jobs.enqueue(torrent_hash, None, "upload", payload, priority=50)
            enqueued += 1
            self.obs.event("info", "file_batch", "upload_queued", f"upload job {job_id} queued", {"job_id": job_id, **payload}, hash=torrent_hash, job_id=job_id)
        return FileBatchResult(scanned, eligible, enqueued, skipped_existing, dry_run=eligible - enqueued - skipped_existing if self.dry_run else 0)

    def _existing_upload_job(self, torrent_hash: str) -> bool:
        con = _connect(self.state_db)
        row = con.execute("select id from torrent_jobs where hash=? and job_type='upload' and state not in ('cancelled','failed') limit 1", (torrent_hash,)).fetchone()
        con.close()
        return row is not None

    def _payload_for(self, torrent_hash: str, torrent: Mapping[str, Any]) -> dict[str, Any]:
        name = str(torrent.get("name") or torrent_hash)
        local = self._local_path(torrent)
        remote_dir = f"{self.remote}/{_safe_name(name)}-{torrent_hash[:12]}"
        return {
            "local": local,
            "remote": remote_dir,
            "size": int(torrent.get("size") or 0),
            "full_torrent": True,
            "source": "file_batch_completed_full_torrent",
        }

    def _local_path(self, torrent: Mapping[str, Any]) -> str:
        raw = str(torrent.get("content_path") or "")
        if not raw:
            save_path = str(torrent.get("save_path") or f"{self.container_downloads}/active")
            raw = str(PurePosixPath(save_path) / str(torrent.get("name") or torrent.get("hash") or "torrent"))
        if raw == self.container_downloads:
            return self.host_downloads
        if raw.startswith(self.container_downloads + "/"):
            return self.host_downloads + raw[len(self.container_downloads):]
        return raw
