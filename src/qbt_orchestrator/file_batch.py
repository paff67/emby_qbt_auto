from __future__ import annotations

import json
import re
import sqlite3
import time
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Any, Mapping

from .db import readonly_connect, write_transaction
from .models import BatchReservation
from .observability import redact
from .policies.batching import compute_batch_reservation
from .runtime import ObservabilityStore, TorrentJobRepository


@dataclass(frozen=True)
class FileBatchResult:
    scanned: int
    eligible: int
    enqueued: int
    skipped_existing: int
    dry_run: int = 0
    batches_created: int = 0
    batches_blocked: int = 0


def _connect(path) -> sqlite3.Connection:
    return readonly_connect(path)


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


def _is_stopped(torrent: Mapping[str, Any]) -> bool:
    return str(torrent.get("state") or "").lower() in {"pauseddl", "pausedup", "stoppeddl", "stoppedup", "paused", "stopped"}


MEDIA_EXTENSIONS = {".mp4", ".mkv", ".avi", ".mov", ".wmv", ".m4v", ".ts", ".webm", ".flv", ".mpg", ".mpeg", ".iso"}
JUNK_BATCH_NAME = re.compile(r"(?i)(最新地址|最\s*新\s*位\s*址|收藏不迷路|官方指定|博彩|赌场|直播|telegram|996gg\.cc|489155|x\s*u\s*u|uu美少女|社\s*區|社\s*区|福利|体育|电竞|楼风)")


class FileBatchService:
    """Level-2 file/batch loop using qBT sync snapshots only.

    This first production-safe slice does not call torrents/files, os.walk, or
    rclone.  It detects completed managed full-torrent payloads and creates a
    durable upload job for the event-driven UploadWorker.
    """

    def __init__(
        self,
        state_db,
        dry_run: bool = True,
        host_downloads: str = "/data/downloads",
        container_downloads: str = "/downloads",
        remote: str = "gcrypt:",
        backpressure_policy=None,
        qbt=None,
        executor=None,
        batch_pipeline_enabled: bool = True,
        disk_floor_bytes: int = 2 * 1024**3,
        filesystem_slack_bytes: int = 128 * 1024**2,
        max_batch_bytes: int = 12 * 1024**3,
        max_inflight_batches_per_torrent: int = 2,
        min_payload_efficiency: float = 0.65,
        reservation_ttl_sec: int = 3600,
        batch_live_verify: bool = False,
        batch_allow_hashes: set[str] | None = None,
        batch_allow_tag: str | None = None,
        batch_max_new_per_tick: int | None = None,
        batch_max_live_batch_bytes: int | None = None,
        now=None,
    ):
        self.state_db = state_db
        self.dry_run = dry_run
        self.host_downloads = host_downloads.rstrip("/")
        self.container_downloads = container_downloads.rstrip("/")
        self.remote = remote.rstrip("/")
        self.backpressure_policy = backpressure_policy
        self.qbt = qbt
        self.executor = executor
        self.batch_pipeline_enabled = bool(batch_pipeline_enabled)
        self.disk_floor_bytes = int(disk_floor_bytes)
        self.filesystem_slack_bytes = int(filesystem_slack_bytes)
        self.max_batch_bytes = int(max_batch_bytes)
        self.max_inflight_batches_per_torrent = int(max_inflight_batches_per_torrent)
        self.min_payload_efficiency = float(min_payload_efficiency)
        self.reservation_ttl_sec = int(reservation_ttl_sec)
        self.batch_live_verify = bool(batch_live_verify)
        self.batch_allow_hashes = {str(item).strip().lower() for item in (batch_allow_hashes or set()) if str(item).strip()}
        self.batch_allow_tag = str(batch_allow_tag or "").strip()
        if batch_max_new_per_tick is None:
            batch_max_new_per_tick = 1 if self.batch_live_verify else 1_000_000
        self.batch_max_new_per_tick = max(0, int(batch_max_new_per_tick))
        self.batch_max_live_batch_bytes = None if batch_max_live_batch_bytes in (None, 0) else max(0, int(batch_max_live_batch_bytes))
        self._new_batches_created_this_tick = 0
        self.now = now or (lambda: int(time.time()))
        self.jobs = TorrentJobRepository(state_db)
        self.obs = ObservabilityStore(state_db)

    def sync_completed(self, snapshots: Mapping[str, Mapping[str, Any]], free_bytes: int | None = None, sync_healthy: bool = True) -> FileBatchResult:
        self._new_batches_created_this_tick = 0
        scanned = len(snapshots)
        eligible = 0
        enqueued = 0
        skipped_existing = 0
        batches_created = 0
        batches_blocked = 0
        for h, raw in snapshots.items():
            torrent = dict(raw)
            torrent.setdefault("hash", h)
            if _is_managed(torrent) and not _is_completed(torrent):
                batch_result = self._maybe_create_pipeline_batch(torrent, free_bytes=free_bytes, sync_healthy=sync_healthy)
                batches_created += int(batch_result.get("created") or 0)
                batches_blocked += int(batch_result.get("blocked") or 0)
                enqueued += int(batch_result.get("upload_queued") or 0)
            if not (_is_managed(torrent) and _is_completed(torrent)):
                continue
            eligible += 1
            torrent_hash = str(torrent.get("hash") or h)
            payload = self._payload_for(torrent_hash, torrent)
            if self._existing_upload_job(torrent_hash):
                skipped_existing += 1
                continue
            if self.backpressure_policy is not None:
                decision = self.backpressure_policy.evaluate(self.state_db, candidate_bytes=int(payload.get("size") or 0))
                if not decision.allow_new_upload_jobs:
                    self.backpressure_policy.record(self.state_db, decision, torrent_hash=torrent_hash)
                    continue
                self.backpressure_policy.record(self.state_db, decision, torrent_hash=torrent_hash)
            if self.dry_run:
                self.obs.action(torrent_hash, None, "enqueue_upload", "torrent_jobs/upload", payload, "dry_run", True)
                self.obs.event("info", "file_batch", "upload_queue_dry_run", f"would enqueue upload for {torrent_hash[:8]}", payload, hash=torrent_hash)
                continue
            job_id = self.jobs.enqueue(torrent_hash, None, "upload", payload, priority=50)
            enqueued += 1
            self.obs.event("info", "file_batch", "upload_queued", f"upload job {job_id} queued", {"job_id": job_id, **payload}, hash=torrent_hash, job_id=job_id)
        return FileBatchResult(
            scanned,
            eligible,
            enqueued,
            skipped_existing,
            dry_run=eligible - enqueued - skipped_existing if self.dry_run else 0,
            batches_created=batches_created,
            batches_blocked=batches_blocked,
        )

    def _maybe_create_pipeline_batch(self, torrent: Mapping[str, Any], *, free_bytes: int | None, sync_healthy: bool) -> dict[str, int]:
        h = str(torrent.get("hash") or "")
        tags = _tags(torrent)
        if not self.batch_pipeline_enabled or not h or "no-batch" in tags or self.qbt is None or free_bytes is None:
            return {"created": 0, "blocked": 0}
        if not sync_healthy:
            self._decision(h, "prefetch_blocked", "sync_unhealthy", {"free_bytes": free_bytes})
            return {"created": 0, "blocked": 1}
        live_allowed = self._live_verify_canary_allowed(h, tags)
        if not live_allowed and self._inflight_batch_count(h) == 0:
            self._decision(
                h,
                "prefetch_blocked",
                "live_verify_no_canary_match",
                {"batch_live_verify": True, "allow_hashes": sorted(self.batch_allow_hashes), "allow_tag": self.batch_allow_tag},
            )
            return {"created": 0, "blocked": 1}
        tick_cap_reached = self._new_batches_created_this_tick >= self.batch_max_new_per_tick
        if tick_cap_reached and self._inflight_batch_count(h) == 0:
            self._decision(
                h,
                "prefetch_blocked",
                "live_verify_new_batch_tick_cap",
                {"batch_live_verify": self.batch_live_verify, "max_new_per_tick": self.batch_max_new_per_tick},
            )
            return {"created": 0, "blocked": 1}
        try:
            files = self.qbt.torrent_files(h)
        except Exception as exc:
            self.obs.event("error", "file_batch", "batch_file_probe_failed", str(redact(str(exc))), {"hash": h}, hash=h)
            return {"created": 0, "blocked": 1}
        queued = self._queue_downloaded_pipeline_batches(torrent, files)
        if tick_cap_reached:
            self._decision(
                h,
                "prefetch_blocked",
                "live_verify_new_batch_tick_cap",
                {"batch_live_verify": self.batch_live_verify, "max_new_per_tick": self.batch_max_new_per_tick, "upload_queued": queued},
            )
            return {"created": 0, "blocked": 1, "upload_queued": queued}
        if not live_allowed:
            self._decision(
                h,
                "prefetch_blocked",
                "live_verify_no_canary_match",
                {"batch_live_verify": True, "allow_hashes": sorted(self.batch_allow_hashes), "allow_tag": self.batch_allow_tag, "upload_queued": queued},
            )
            return {"created": 0, "blocked": 1, "upload_queued": queued}
        if self._inflight_batch_count(h) >= self.max_inflight_batches_per_torrent:
            self._decision(h, "prefetch_blocked", "inflight_batch_cap", {"limit": self.max_inflight_batches_per_torrent})
            return {"created": 0, "blocked": 1, "upload_queued": queued}
        try:
            piece_size = self._piece_size(h, torrent)
        except Exception as exc:
            self.obs.event("error", "file_batch", "batch_file_probe_failed", str(redact(str(exc))), {"hash": h}, hash=h)
            return {"created": 0, "blocked": 1, "upload_queued": queued}
        selected, reservation = self._select_batch_files(files, piece_size, free_bytes)
        if not selected or reservation is None:
            reason_code = "live_verify_batch_size_cap" if self._first_pending_file_exceeds_live_cap(files, piece_size, free_bytes) else "batch_budget_insufficient"
            self._decision(
                h,
                "prefetch_blocked",
                reason_code,
                {
                    "free_bytes": int(free_bytes),
                    "safe_budget": self._safe_batch_budget(free_bytes),
                    "piece_size": piece_size,
                    "batch_max_live_batch_bytes": self.batch_max_live_batch_bytes,
                },
            )
            return {"created": 0, "blocked": 1, "upload_queued": queued}
        if self._live_batch_size_cap_exceeded(reservation.reserved_bytes):
            self._decision(
                h,
                "prefetch_blocked",
                "live_verify_batch_size_cap",
                {
                    "reserved_bytes": reservation.reserved_bytes,
                    "batch_max_live_batch_bytes": self.batch_max_live_batch_bytes,
                },
            )
            return {"created": 0, "blocked": 1, "upload_queued": queued}
        if reservation.payload_efficiency < self.min_payload_efficiency:
            self._decision(
                h,
                "prefetch_blocked",
                "payload_efficiency_too_low",
                {"payload_efficiency": reservation.payload_efficiency, "min_payload_efficiency": self.min_payload_efficiency},
            )
            return {"created": 0, "blocked": 1, "upload_queued": queued}
        indices = [int(f["index"]) for f in selected]
        if self.backpressure_policy is not None:
            decision = self.backpressure_policy.evaluate(self.state_db, candidate_bytes=int(reservation.reserved_bytes))
            self.backpressure_policy.record(self.state_db, decision, torrent_hash=h)
            if not decision.allow_new_upload_jobs:
                self._decision(h, "prefetch_blocked", decision.reason, {"reserved_bytes": reservation.reserved_bytes})
                return {"created": 0, "blocked": 1, "upload_queued": queued}
        payload = {
            "hash": h,
            "indices": indices,
            "total_bytes": reservation.payload_bytes,
            "reserved_bytes": reservation.reserved_bytes,
            "piece_size": piece_size,
            "piece_spill_overhead_bytes": reservation.piece_spill_overhead_bytes,
            "filesystem_slack_bytes": reservation.filesystem_slack_bytes,
            "payload_efficiency": reservation.payload_efficiency,
        }
        if self.dry_run:
            self.obs.action(h, None, "batch_pipeline", "torrent_batches", payload, "dry_run", True)
            self._decision(h, "prefetch_allowed", "dry_run", payload)
            return {"created": 0, "blocked": 0, "upload_queued": queued}
        batch_id = self._create_batch_and_reservation(h, indices, reservation, piece_size)
        try:
            self._apply_batch_to_qbt(h, indices, [int(f.get("index")) for f in files], should_start=_is_stopped(torrent))
        except Exception as exc:
            self._mark_batch_failed(batch_id, str(exc))
            return {"created": 0, "blocked": 1, "upload_queued": queued}
        self._mark_batch_applied(batch_id)
        self._decision(h, "prefetch_allowed", "batch_pipeline_reserved", {"batch_id": batch_id, **payload})
        self.obs.event("info", "file_batch", "batch_applied", f"batch {batch_id} applied", {"batch_id": batch_id, **payload}, hash=h)
        self._new_batches_created_this_tick += 1
        return {"created": 1, "blocked": 0, "upload_queued": queued}

    def _live_verify_canary_allowed(self, h: str, tags: set[str]) -> bool:
        if not self.batch_live_verify:
            return True
        if self.batch_allow_hashes and h.lower() in self.batch_allow_hashes:
            return True
        if self.batch_allow_tag and self.batch_allow_tag in tags:
            return True
        return False

    def _queue_downloaded_pipeline_batches(self, torrent: Mapping[str, Any], files: list[Mapping[str, Any]]) -> int:
        h = str(torrent.get("hash") or "")
        if not h:
            return 0
        by_index = {int(row.get("index") or 0): row for row in files}
        con = _connect(self.state_db)
        try:
            rows = [
                dict(r)
                for r in con.execute(
                    "select * from torrent_batches where hash=? and state in ('downloading','applied_to_qbt','downloaded') order by batch_no,id",
                    (h,),
                )
            ]
        finally:
            con.close()
        queued = 0
        for row in rows:
            batch_id = int(row["id"])
            if self._existing_batch_upload_job(batch_id):
                continue
            indices = [int(x) for x in json.loads(row["indices_json"] or "[]")]
            selected = [by_index[i] for i in indices if i in by_index]
            if len(selected) != len(indices) or not selected:
                continue
            if any(float(item.get("progress") or 0) < 1.0 for item in selected):
                continue
            payload = self._payload_for_batch(batch_id, torrent, selected)
            if payload is None:
                continue
            if self.dry_run:
                self.obs.action(h, None, "enqueue_batch_upload", "torrent_jobs/upload", payload, "dry_run", True)
                continue
            try:
                self._qbt_post("/api/v2/torrents/filePrio", {"hash": h, "id": "|".join(str(i) for i in indices), "priority": "0"}, h)
            except Exception as exc:
                self.obs.event("error", "file_batch", "batch_pause_files_failed", str(redact(str(exc))), {"batch_id": batch_id}, hash=h)
                continue
            job_id = self.jobs.enqueue(h, batch_id, "upload", payload, priority=40)
            self._mark_batch_upload_queued(batch_id, job_id, int(payload["size"]))
            queued += 1
            self.obs.event("info", "file_batch", "batch_upload_queued", f"batch {batch_id} upload job {job_id} queued", {"batch_id": batch_id, "job_id": job_id, **payload}, hash=h, job_id=job_id)
        return queued

    def _payload_for_batch(self, batch_id: int, torrent: Mapping[str, Any], selected: list[Mapping[str, Any]]) -> dict[str, Any] | None:
        h = str(torrent.get("hash") or "")
        name = str(torrent.get("name") or h or "torrent")
        local_root = self._local_path(torrent)
        root_path = Path(local_root)
        remote_dir = f"{self.remote}/{_safe_name(name)}-{h[:12]}"
        files: list[dict[str, Any]] = []
        media_files: list[dict[str, Any]] = []
        total = 0
        for row in sorted(selected, key=lambda x: int(x.get("index") or 0)):
            rel = self._file_relative_path(row)
            size = int(row.get("size") or 0)
            if not rel or size <= 0:
                continue
            local_path = root_path / rel
            if not local_path.exists():
                self.obs.event("warning", "file_batch", "batch_local_file_missing", f"missing {rel}", {"batch_id": batch_id, "local_path": str(local_path)}, hash=h)
                return None
            remote_path = f"{remote_dir.rstrip('/')}/{rel.replace('\\', '/')}"
            item = {"relative_path": rel.replace("\\", "/"), "local_path": str(local_path), "remote_path": remote_path, "size": size}
            files.append(item)
            total += size
            if local_path.suffix.lower() in MEDIA_EXTENSIONS:
                media_files.append({"remote_path": remote_path, "size": size})
        if not files:
            return None
        return {
            "local": local_root,
            "remote": remote_dir,
            "size": total,
            "full_torrent": False,
            "source": "file_batch_pipeline_batch",
            "batch_id": int(batch_id),
            "copy_mode": "copy_files",
            "files": files,
            "media_files": media_files,
            "upload_manifest_id": f"batch-{batch_id}",
        }

    @staticmethod
    def _file_relative_path(row: Mapping[str, Any]) -> str:
        raw = str(row.get("name") or row.get("path") or row.get("relative_path") or "")
        return raw.replace("\\", "/").lstrip("/")

    def _existing_batch_upload_job(self, batch_id: int) -> bool:
        con = _connect(self.state_db)
        try:
            row = con.execute(
                "select id from torrent_jobs where batch_id=? and job_type='upload' and state not in ('cancelled','failed') limit 1",
                (int(batch_id),),
            ).fetchone()
            return row is not None
        finally:
            con.close()

    def _mark_batch_upload_queued(self, batch_id: int, job_id: int, downloaded_bytes: int) -> None:
        now = int(self.now())
        def txn(con: sqlite3.Connection) -> None:
            row = con.execute("select hash from torrent_batches where id=?", (int(batch_id),)).fetchone()
            h = str(row["hash"]) if row else None
            con.execute(
                "update torrent_batches set state='upload_queued', downloaded_bytes=?, downloaded_at=coalesce(downloaded_at,?), "
                "upload_job_id=?, local_pinned_bytes=?, upload_queued_at=?, updated_at=? where id=?",
                (int(downloaded_bytes), now, int(job_id), int(downloaded_bytes), now, now, int(batch_id)),
            )
            con.execute(
                "update resource_reservations set state='released', released_at=?, reason='batch_downloaded_upload_queued' "
                "where batch_id=? and kind='batch' and state='active'",
                (now, int(batch_id)),
            )
            existing = con.execute(
                "select id from resource_reservations where batch_id=? and kind='cleanup_pending' order by id limit 1",
                (int(batch_id),),
            ).fetchone()
            if existing:
                con.execute(
                    "update resource_reservations set hash=?, bytes=?, state='active', released_at=null, reason='batch_upload_queued' where id=?",
                    (h, int(downloaded_bytes), int(existing["id"])),
                )
            else:
                con.execute(
                    "insert into resource_reservations(hash,batch_id,kind,bytes,state,created_at,expires_at,reason) values(?,?,?,?,?,?,?,?)",
                    (h, int(batch_id), "cleanup_pending", int(downloaded_bytes), "active", now, None, "batch_upload_queued"),
                )
        write_transaction(self.state_db, txn)

    def _piece_size(self, h: str, torrent: Mapping[str, Any]) -> int:
        raw = torrent.get("piece_size")
        if raw:
            return max(1, int(raw))
        if hasattr(self.qbt, "torrent_properties"):
            props = self.qbt.torrent_properties(h)
            return max(1, int(props.get("piece_size") or props.get("pieceSize") or 16 * 1024**2))
        return 16 * 1024**2

    def _select_batch_files(self, files: list[Mapping[str, Any]], piece_size: int, free_bytes: int):
        budget = self._safe_batch_budget(free_bytes)
        selected: list[Mapping[str, Any]] = []
        for row in sorted(files, key=lambda f: int(f.get("index") or 0)):
            if float(row.get("progress") or 0) >= 1.0:
                continue
            size = int(row.get("size") or 0)
            if size <= 0:
                continue
            if not self._is_selectable_batch_media(row):
                continue
            candidate = [*selected, row]
            reservation = self._batch_reservation(candidate, piece_size)
            if sum(int(item.get("size") or 0) for item in candidate) > self.max_batch_bytes:
                break
            if self._live_batch_size_cap_exceeded(reservation.reserved_bytes):
                break
            if reservation.reserved_bytes > budget:
                break
            selected = candidate
        if not selected:
            return [], None
        return selected, self._batch_reservation(selected, piece_size)

    def _batch_reservation(self, selected: list[Mapping[str, Any]], piece_size: int) -> BatchReservation:
        reservation_rows = []
        for row in selected:
            remaining = self._remaining_file_bytes(row)
            if remaining <= 0:
                continue
            reservation_rows.append({"size": remaining})
        reservation = compute_batch_reservation(reservation_rows, piece_size=piece_size, filesystem_slack=self.filesystem_slack_bytes)
        payload_bytes = sum(int(row.get("size") or 0) for row in selected)
        return BatchReservation(
            payload_bytes=payload_bytes,
            piece_spill_overhead_bytes=reservation.piece_spill_overhead_bytes,
            filesystem_slack_bytes=reservation.filesystem_slack_bytes,
            reserved_bytes=reservation.reserved_bytes,
            payload_efficiency=reservation.payload_efficiency,
        )

    @staticmethod
    def _remaining_file_bytes(row: Mapping[str, Any]) -> int:
        size = int(row.get("size") or 0)
        if size <= 0:
            return 0
        progress = max(0.0, min(1.0, float(row.get("progress") or 0.0)))
        if progress >= 1.0:
            return 0
        return max(1, int(round(size * (1.0 - progress))))

    @staticmethod
    def _is_selectable_batch_media(row: Mapping[str, Any]) -> bool:
        path = PurePosixPath(str(row.get("name") or row.get("path") or row.get("relative_path") or ""))
        if path.suffix.lower() not in MEDIA_EXTENSIONS:
            return False
        if JUNK_BATCH_NAME.search(path.name):
            return False
        if int(row.get("size") or 0) < 50 * 1024**2:
            return False
        return True

    def _live_batch_size_cap_exceeded(self, reserved_bytes: int) -> bool:
        return self.batch_live_verify and self.batch_max_live_batch_bytes is not None and int(reserved_bytes) > int(self.batch_max_live_batch_bytes)

    def _first_pending_file_exceeds_live_cap(self, files: list[Mapping[str, Any]], piece_size: int, free_bytes: int) -> bool:
        if not (self.batch_live_verify and self.batch_max_live_batch_bytes is not None):
            return False
        budget = self._safe_batch_budget(free_bytes)
        for row in sorted(files, key=lambda f: int(f.get("index") or 0)):
            if float(row.get("progress") or 0) >= 1.0:
                continue
            size = int(row.get("size") or 0)
            if size <= 0:
                continue
            if not self._is_selectable_batch_media(row):
                continue
            reservation = self._batch_reservation([row], piece_size)
            return reservation.reserved_bytes > int(self.batch_max_live_batch_bytes) and reservation.reserved_bytes <= budget
        return False

    def _safe_batch_budget(self, free_bytes: int) -> int:
        return max(0, int(free_bytes) - self.disk_floor_bytes - self._active_reservation_bytes())

    def _active_reservation_bytes(self) -> int:
        con = _connect(self.state_db)
        try:
            row = con.execute(
                "select coalesce(sum(bytes),0) from resource_reservations where state='active' and (expires_at is null or expires_at>?)",
                (int(self.now()),),
            ).fetchone()
            return int(row[0] if row else 0)
        finally:
            con.close()

    def _inflight_batch_count(self, h: str) -> int:
        states = ("reserved", "applied_to_qbt", "downloading", "downloaded", "upload_queued", "uploading", "verify_pending", "verified_local_pinned", "cleanup_deferred")
        placeholders = ",".join("?" for _ in states)
        con = _connect(self.state_db)
        try:
            row = con.execute(f"select count(*) from torrent_batches where hash=? and state in ({placeholders})", (h, *states)).fetchone()
            return int(row[0] if row else 0)
        finally:
            con.close()

    def _next_batch_no(self, con: sqlite3.Connection, h: str) -> int:
        row = con.execute("select coalesce(max(batch_no),0)+1 from torrent_batches where hash=?", (h,)).fetchone()
        return int(row[0] if row else 1)

    def _selected_extents(self, indices: list[int]) -> int:
        if not indices:
            return 0
        extents = 1
        for prev, cur in zip(indices, indices[1:]):
            if cur != prev + 1:
                extents += 1
        return extents

    def _create_batch_and_reservation(self, h: str, indices: list[int], reservation, piece_size: int) -> int:
        now = int(self.now())
        def txn(con: sqlite3.Connection) -> int:
            batch_no = self._next_batch_no(con, h)
            cur = con.execute(
                "insert into torrent_batches(hash,batch_no,state,mode,indices_json,total_bytes,reserved_bytes,piece_size,selected_extents,piece_spill_overhead_bytes,payload_efficiency,priority_applied,created_at,updated_at) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    h,
                    batch_no,
                    "reserved",
                    "pipeline",
                    json.dumps(indices),
                    int(reservation.payload_bytes),
                    int(reservation.reserved_bytes),
                    int(piece_size),
                    self._selected_extents(indices),
                    int(reservation.piece_spill_overhead_bytes),
                    float(reservation.payload_efficiency),
                    0,
                    now,
                    now,
                ),
            )
            batch_id = int(cur.lastrowid)
            con.execute(
                "insert into resource_reservations(hash,batch_id,kind,bytes,state,created_at,expires_at,reason) values(?,?,?,?,?,?,?,?)",
                (h, batch_id, "batch", int(reservation.reserved_bytes), "active", now, now + self.reservation_ttl_sec, "batch_pipeline_reserved"),
            )
            return batch_id

        return int(write_transaction(self.state_db, txn))

    def _mark_batch_applied(self, batch_id: int) -> None:
        now = int(self.now())
        write_transaction(
            self.state_db,
            lambda con: con.execute(
                "update torrent_batches set state='downloading', priority_applied=1, updated_at=? where id=?",
                (now, int(batch_id)),
            ),
        )

    def _mark_batch_failed(self, batch_id: int, error: str) -> None:
        now = int(self.now())
        def txn(con: sqlite3.Connection) -> None:
            con.execute("update torrent_batches set state='failed', updated_at=? where id=?", (now, int(batch_id)))
            con.execute(
                "update resource_reservations set state='released', released_at=?, reason='qbt_apply_failed' where batch_id=? and state='active'",
                (now, int(batch_id)),
            )
            con.execute(
                "insert into action_log(ts,job_id,action_type,path,payload_json,status,dry_run,error) values(?,?,?,?,?,?,?,?)",
                (now, int(batch_id), "batch_pipeline", "torrent_batches", "{}", "failed", 0, redact(error)),
            )
        write_transaction(self.state_db, txn)

    def _apply_batch_to_qbt(self, h: str, selected_indices: list[int], all_indices: list[int], *, should_start: bool) -> None:
        unselected = [i for i in all_indices if i not in set(selected_indices)]
        if selected_indices:
            self._qbt_post("/api/v2/torrents/filePrio", {"hash": h, "id": "|".join(str(i) for i in selected_indices), "priority": "1"}, h)
        if unselected:
            self._qbt_post("/api/v2/torrents/filePrio", {"hash": h, "id": "|".join(str(i) for i in unselected), "priority": "0"}, h)
        if should_start:
            self._qbt_post("/api/v2/torrents/start", {"hashes": h}, h)

    def _qbt_post(self, path: str, payload: dict[str, Any], h: str | None) -> None:
        if self.executor is not None and hasattr(self.executor, "qbt_post"):
            self.executor.qbt_post(path, payload)
        elif hasattr(self.qbt, "qbt_post"):
            self.qbt.qbt_post(path, payload)
        elif hasattr(self.qbt, "post"):
            self.qbt.post(path, payload)
        else:
            raise RuntimeError("qbt object does not support post")
        self.obs.action(h, None, "batch_qbt_post", path, payload, "succeeded", False)

    def _decision(self, h: str, decision: str, reason_code: str, data: dict[str, Any]) -> None:
        write_transaction(
            self.state_db,
            lambda con: con.execute(
                "insert into decision_log(ts,component,hash,decision,reason_code,data_json) values(?,?,?,?,?,?)",
                (int(self.now()), "file_batch", h, decision, reason_code, json.dumps(redact(data), ensure_ascii=False)),
            ),
        )

    def _existing_upload_job(self, torrent_hash: str) -> bool:
        con = _connect(self.state_db)
        row = con.execute("select id from torrent_jobs where hash=? and job_type='upload' and state not in ('cancelled','failed') limit 1", (torrent_hash,)).fetchone()
        con.close()
        return row is not None

    def _payload_for(self, torrent_hash: str, torrent: Mapping[str, Any]) -> dict[str, Any]:
        name = str(torrent.get("name") or torrent_hash)
        local = self._local_path(torrent)
        remote_dir = f"{self.remote}/{_safe_name(name)}-{torrent_hash[:12]}"
        payload = {
            "local": local,
            "remote": remote_dir,
            "size": int(torrent.get("size") or 0),
            "full_torrent": True,
            "source": "file_batch_completed_full_torrent",
        }
        manifest = self._manifest_for(local, remote_dir)
        if manifest:
            files, media_files, total_size, remote, copy_mode = manifest
            payload.update({"files": files, "media_files": media_files, "size": total_size, "remote": remote, "copy_mode": copy_mode})
        return payload

    def _local_path(self, torrent: Mapping[str, Any]) -> str:
        raw = str(torrent.get("content_path") or "")
        if not raw:
            save_path = str(torrent.get("save_path") or f"{self.container_downloads}/active")
            raw = str(PurePosixPath(save_path) / str(torrent.get("name") or torrent.get("hash") or "torrent"))
        if raw == self.container_downloads:
            return self.host_downloads
        if raw.startswith(self.container_downloads + "/"):
            return self._host_path_for_container_suffix(raw[len(self.container_downloads):])
        return raw

    def _host_path_for_container_suffix(self, suffix: str) -> str:
        rel = str(suffix or "").replace("\\", "/").lstrip("/")
        if not rel:
            return self.host_downloads
        if "\\" in self.host_downloads or Path(self.host_downloads).drive:
            return str(Path(self.host_downloads).joinpath(*PurePosixPath(rel).parts))
        return f"{self.host_downloads.rstrip('/')}/{rel}"

    def _manifest_for(self, local: str, remote_dir: str) -> tuple[list[dict[str, Any]], list[dict[str, Any]], int, str, str] | None:
        path = Path(local)
        if not path.exists():
            return None
        rows: list[tuple[Path, str]] = []
        if path.is_file():
            rows.append((path, path.name))
        elif path.is_dir():
            for child in sorted((p for p in path.rglob("*") if p.is_file()), key=lambda p: p.relative_to(path).as_posix()):
                rows.append((child, child.relative_to(path).as_posix()))
        else:
            return None
        files: list[dict[str, Any]] = []
        media_files: list[dict[str, Any]] = []
        total = 0
        for file_path, rel in rows:
            rel_posix = rel.replace("\\", "/")
            size = int(file_path.stat().st_size)
            total += size
            remote_path = f"{remote_dir.rstrip('/')}/{rel_posix}"
            item = {
                "relative_path": rel_posix,
                "local_path": str(file_path),
                "remote_path": remote_path,
                "size": size,
            }
            files.append(item)
            if file_path.suffix.lower() in MEDIA_EXTENSIONS:
                media_files.append({"remote_path": remote_path, "size": size})
        if not files:
            return None
        if path.is_file():
            return files, media_files, total, files[0]["remote_path"], "copyto"
        return files, media_files, total, remote_dir, "copy"
