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
DEFAULT_DOWNLOAD_EXTENSIONS = {".txt"}
JUNK_BATCH_NAME = re.compile(r"(?i)(最新地址|最\s*新\s*位\s*址|收藏不迷路|官方指定|博彩|赌场|直播|telegram|996gg\.cc|x\s*u\s*u|uu美少女|社\s*區|社\s*区|福利|体育|电竞|楼风)")
PIPELINE_DOWNLOAD_STATES = ("reserved", "applied_to_qbt", "downloading")


def active_pipeline_batch_hashes(state_db: str | Path, now: int | None = None) -> set[str]:
    """Hashes with live pipeline batch reservations that planner must not stop.

    Batch reservations are explicit disk-budget claims for a selected subset of
    qBT files.  While such a reservation is active, planner should not pause the
    torrent as an unselected low-speed download, otherwise qBT live state and
    the durable reservation diverge.
    """
    now = int(time.time() if now is None else now)
    placeholders = ",".join("?" for _ in PIPELINE_DOWNLOAD_STATES)
    con = _connect(state_db)
    try:
        rows = con.execute(
            f"select distinct tb.hash from torrent_batches tb "
            f"join resource_reservations rr on rr.batch_id=tb.id and rr.kind='batch' "
            f"where tb.state in ({placeholders}) and rr.state='active' "
            f"and (rr.expires_at is null or rr.expires_at>?)",
            (*PIPELINE_DOWNLOAD_STATES, now),
        ).fetchall()
        return {str(r["hash"]) for r in rows if str(r["hash"] or "")}
    finally:
        con.close()


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
        disk_floor_bytes: int = 3 * 1024**3,
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
            batch_max_new_per_tick = 1 if self.batch_live_verify and (self.batch_allow_hashes or self.batch_allow_tag) else 1_000_000
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
            if payload is None:
                continue
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
        if self._reconcile_pipeline_batch_state(torrent, files):
            return {"created": 0, "blocked": 1, "upload_queued": queued}
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
        already_selected_indices = self._inflight_batch_indices(h)
        selected, reservation = self._select_batch_files(files, piece_size, free_bytes, already_selected_indices=already_selected_indices)
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
            self._apply_batch_to_qbt(h, indices, files, should_start=_is_stopped(torrent), protected_indices=already_selected_indices)
        except Exception as exc:
            self._mark_batch_failed(batch_id, str(exc))
            return {"created": 0, "blocked": 1, "upload_queued": queued}
        self._mark_batch_applied(batch_id)
        self._decision(h, "prefetch_allowed", "batch_pipeline_reserved", {"batch_id": batch_id, **payload})
        self.obs.event("info", "file_batch", "batch_applied", f"batch {batch_id} applied", {"batch_id": batch_id, **payload}, hash=h)
        self._new_batches_created_this_tick += 1
        return {"created": 1, "blocked": 0, "upload_queued": queued}

    def _reconcile_pipeline_batch_state(self, torrent: Mapping[str, Any], files: list[Mapping[str, Any]]) -> bool:
        h = str(torrent.get("hash") or "")
        if not h:
            return False
        rows = self._pipeline_batch_rows(h)
        if not rows:
            return False
        if not _is_stopped(torrent):
            return False
        allocation = self._scheduler_allocation(h)
        desired = str((allocation or {}).get("desired_state") or "")
        if desired in {"soak", "soak_cooldown", "dead", "carousel_probe"}:
            batch_ids = [int(row["id"]) for row in rows]
            self._pause_pipeline_batches(batch_ids, h, "batch_reconcile_planner_stopped", "planner_stopped_batch")
            return True

        # The reservation is still valid and planner has not placed the torrent
        # into a cooldown/dead state.  Re-start the qBT torrent instead of
        # letting a durable batch reservation silently drift away from qBT state.
        active_rows = [row for row in rows if int(row.get("has_active_reservation") or 0)]
        if not active_rows:
            return False
        try:
            self._qbt_post("/api/v2/torrents/start", {"hashes": h}, h)
        except Exception as exc:
            self.obs.event("error", "file_batch", "batch_reconcile_start_failed", str(redact(str(exc))), {"hash": h}, hash=h)
            return True
        self._decision(h, "batch_reconciled", "restarted_reserved_batch", {"batch_ids": [int(row["id"]) for row in active_rows]})
        self.obs.event("info", "file_batch", "batch_restarted", f"restarted reserved batch for {h[:8]}", {"hash": h, "batch_ids": [int(row["id"]) for row in active_rows]}, hash=h)
        return True

    def _pipeline_batch_rows(self, h: str) -> list[dict[str, Any]]:
        placeholders = ",".join("?" for _ in PIPELINE_DOWNLOAD_STATES)
        con = _connect(self.state_db)
        try:
            return [
                dict(r)
                for r in con.execute(
                    f"select tb.*, exists("
                    f"  select 1 from resource_reservations rr "
                    f"  where rr.batch_id=tb.id and rr.kind='batch' and rr.state='active' "
                    f"  and (rr.expires_at is null or rr.expires_at>?)"
                    f") as has_active_reservation "
                    f"from torrent_batches tb "
                    f"where tb.hash=? and tb.state in ({placeholders}) order by tb.id",
                    (int(self.now()), h, *PIPELINE_DOWNLOAD_STATES),
                ).fetchall()
            ]
        finally:
            con.close()

    def _scheduler_allocation(self, h: str) -> dict[str, Any] | None:
        con = _connect(self.state_db)
        try:
            row = con.execute("select * from scheduler_allocations where hash=?", (h,)).fetchone()
            return dict(row) if row else None
        finally:
            con.close()

    def _pause_pipeline_batches(self, batch_ids: list[int], h: str, reservation_reason: str, decision_reason: str) -> None:
        if not batch_ids:
            return
        now = int(self.now())
        placeholders = ",".join("?" for _ in batch_ids)

        def txn(con: sqlite3.Connection) -> None:
            con.execute(
                f"update torrent_batches set state='paused_by_planner', updated_at=? where id in ({placeholders})",
                (now, *batch_ids),
            )
            con.execute(
                f"update resource_reservations set state='released', released_at=?, reason=? "
                f"where kind='batch' and state='active' and batch_id in ({placeholders})",
                (now, reservation_reason, *batch_ids),
            )
            con.execute(
                "insert into decision_log(ts,component,hash,decision,reason_code,data_json) values(?,?,?,?,?,?)",
                (
                    now,
                    "file_batch",
                    h,
                    "batch_reconciled",
                    decision_reason,
                    json.dumps(redact({"batch_ids": batch_ids, "released_reservation": True}), ensure_ascii=False),
                ),
            )

        write_transaction(self.state_db, txn)
        self.obs.event(
            "warning",
            "file_batch",
            "batch_reconciled",
            f"released stale batch reservation for {h[:8]}",
            {"batch_ids": batch_ids, "reason": reservation_reason},
            hash=h,
        )

    def _live_verify_canary_allowed(self, h: str, tags: set[str]) -> bool:
        if not self.batch_live_verify:
            return True
        if not self.batch_allow_hashes and not self.batch_allow_tag:
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
            resolved = self._resolve_selected_local_file(root_path, row)
            if resolved is None:
                local_path = root_path.joinpath(*PurePosixPath(rel).parts)
                self.obs.event("warning", "file_batch", "batch_local_file_missing", f"missing {rel}", {"batch_id": batch_id, "local_path": str(local_path)}, hash=h)
                return None
            local_path, rel_posix = resolved
            remote_path = f"{remote_dir.rstrip('/')}/{rel_posix}"
            item = {"relative_path": rel_posix, "local_path": str(local_path), "remote_path": remote_path, "size": size}
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

    def _select_batch_files(self, files: list[Mapping[str, Any]], piece_size: int, free_bytes: int, already_selected_indices: set[int] | None = None):
        budget = self._safe_batch_budget(free_bytes)
        already_selected_indices = already_selected_indices or set()
        candidates: list[Mapping[str, Any]] = []
        for row in sorted(files, key=lambda f: int(f.get("index") or 0)):
            if int(row.get("index") or 0) in already_selected_indices:
                continue
            if float(row.get("progress") or 0) >= 1.0:
                continue
            size = int(row.get("size") or 0)
            if size <= 0:
                continue
            if not self._is_selectable_batch_media(row):
                continue
            candidates.append(row)
        selected = self._best_batch_combination(candidates, piece_size, budget)
        if not selected:
            return [], None
        return selected, self._batch_reservation(selected, piece_size)

    def _best_batch_combination(self, candidates: list[Mapping[str, Any]], piece_size: int, budget: int) -> list[Mapping[str, Any]]:
        if budget <= 0 or not candidates:
            return []
        if len(candidates) <= 18:
            return self._best_batch_combination_exact(candidates, piece_size, budget)
        return self._best_batch_combination_dp(candidates, piece_size, budget)

    def _best_batch_combination_exact(self, candidates: list[Mapping[str, Any]], piece_size: int, budget: int) -> list[Mapping[str, Any]]:
        best: list[Mapping[str, Any]] = []
        best_score: tuple[float, ...] | None = None
        for mask in range(1, 1 << len(candidates)):
            selected = [candidates[i] for i in range(len(candidates)) if mask & (1 << i)]
            score = self._batch_score_if_valid(selected, piece_size, budget)
            if score is None:
                continue
            if best_score is None or score > best_score:
                best = selected
                best_score = score
        return sorted(best, key=lambda f: int(f.get("index") or 0))

    def _best_batch_combination_dp(self, candidates: list[Mapping[str, Any]], piece_size: int, budget: int) -> list[Mapping[str, Any]]:
        scale = max(1, min(64 * 1024**2, max(1024**2, int(piece_size))))
        capacity_units = max(0, int(budget // scale))
        if capacity_units <= 0:
            return []
        ranked = sorted(candidates, key=lambda row: self._single_candidate_rank(row, piece_size), reverse=True)[:48]
        states: dict[int, tuple[tuple[float, ...], list[Mapping[str, Any]]]] = {0: ((0.0,), [])}
        for row in ranked:
            cost_units = max(1, (self._remaining_file_bytes(row) + scale - 1) // scale)
            updates: dict[int, tuple[tuple[float, ...], list[Mapping[str, Any]]]] = {}
            for used_units, (_score, selected) in list(states.items()):
                new_units = used_units + cost_units
                if new_units > capacity_units:
                    continue
                candidate_selected = [*selected, row]
                score = self._batch_score_if_valid(candidate_selected, piece_size, budget)
                if score is None:
                    continue
                current = states.get(new_units) or updates.get(new_units)
                if current is None or score > current[0]:
                    updates[new_units] = (score, candidate_selected)
            states.update(updates)
        best = max((value for units, value in states.items() if units > 0), key=lambda value: value[0], default=None)
        if best is None:
            return []
        return sorted(best[1], key=lambda f: int(f.get("index") or 0))

    def _batch_score_if_valid(self, selected: list[Mapping[str, Any]], piece_size: int, budget: int) -> tuple[float, ...] | None:
        if not selected:
            return None
        payload = sum(int(row.get("size") or 0) for row in selected)
        if payload <= 0 or payload > self.max_batch_bytes:
            return None
        reservation = self._batch_reservation(selected, piece_size)
        if reservation.reserved_bytes > budget:
            return None
        if self._live_batch_size_cap_exceeded(reservation.reserved_bytes):
            return None
        completion_bonus = sum(
            int(row.get("size") or 0) * max(0.0, min(1.0, float(row.get("progress") or 0.0)))
            for row in selected
        )
        indices = [int(row.get("index") or 0) for row in selected]
        return (
            float(payload),
            float(reservation.payload_efficiency),
            float(completion_bonus),
            -float(reservation.piece_spill_overhead_bytes),
            -float(len(selected)),
            -float(sum(indices)),
        )

    def _single_candidate_rank(self, row: Mapping[str, Any], piece_size: int) -> tuple[float, ...]:
        reservation = self._batch_reservation([row], piece_size)
        size = int(row.get("size") or 0)
        progress = max(0.0, min(1.0, float(row.get("progress") or 0.0)))
        return (
            float(size),
            float(reservation.payload_efficiency),
            float(size) * progress,
            -float(reservation.reserved_bytes),
            -float(int(row.get("index") or 0)),
        )

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

    @staticmethod
    def _keeps_default_download_priority(row: Mapping[str, Any]) -> bool:
        path = PurePosixPath(str(row.get("name") or row.get("path") or row.get("relative_path") or ""))
        return path.suffix.lower() in DEFAULT_DOWNLOAD_EXTENSIONS and not JUNK_BATCH_NAME.search(path.name)

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
            rows = con.execute(
                "select id,hash,kind,bytes from resource_reservations where state='active' and (expires_at is null or expires_at>?)",
                (int(self.now()),),
            ).fetchall()
            grouped: dict[str, dict[str, int]] = {}
            for row in rows:
                key = str(row["hash"] or f"{row['kind']}:{row['id']}")
                bucket = grouped.setdefault(key, {"active_download": 0, "batch": 0, "other": 0})
                kind = str(row["kind"] or "")
                if kind == "active_download":
                    bucket["active_download"] += int(row["bytes"] or 0)
                elif kind == "batch":
                    bucket["batch"] += int(row["bytes"] or 0)
                else:
                    bucket["other"] += int(row["bytes"] or 0)
            return sum(max(bucket["active_download"], bucket["batch"]) + bucket["other"] for bucket in grouped.values())
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

    def _inflight_batch_indices(self, h: str) -> set[int]:
        states = ("reserved", "applied_to_qbt", "downloading", "downloaded", "upload_queued", "uploading", "verify_pending", "verified_local_pinned", "cleanup_deferred")
        placeholders = ",".join("?" for _ in states)
        con = _connect(self.state_db)
        try:
            rows = con.execute(f"select indices_json from torrent_batches where hash=? and state in ({placeholders})", (h, *states)).fetchall()
        finally:
            con.close()
        out: set[int] = set()
        for row in rows:
            try:
                values = json.loads(row["indices_json"] or "[]")
            except (TypeError, json.JSONDecodeError):
                continue
            for value in values:
                try:
                    out.add(int(value))
                except (TypeError, ValueError):
                    continue
        return out

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

    def _apply_batch_to_qbt(self, h: str, selected_indices: list[int], all_files: list[Mapping[str, Any]], *, should_start: bool, protected_indices: set[int] | None = None) -> None:
        protected_indices = protected_indices or set()
        default_download_indices = {int(row.get("index") or 0) for row in all_files if self._keeps_default_download_priority(row)}
        all_indices = [int(f.get("index")) for f in all_files]
        unselected = [i for i in all_indices if i not in set(selected_indices) and i not in protected_indices and i not in default_download_indices]
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

    def _payload_for(self, torrent_hash: str, torrent: Mapping[str, Any]) -> dict[str, Any] | None:
        name = str(torrent.get("name") or torrent_hash)
        local = self._local_path(torrent)
        remote_dir = f"{self.remote}/{_safe_name(name)}-{torrent_hash[:12]}"
        qbt_selected_files = self._selected_qbt_files_for_completed_payload(torrent_hash)
        payload = {
            "local": local,
            "remote": remote_dir,
            "size": int(torrent.get("size") or 0),
            "full_torrent": True,
            "source": "file_batch_completed_full_torrent",
        }
        manifest = self._manifest_for(local, remote_dir, selected_qbt_files=qbt_selected_files)
        if manifest is None and qbt_selected_files is not None:
            self.obs.event(
                "warning",
                "file_batch",
                "completed_manifest_empty_after_qbt_filter",
                f"no qBT-selected local files for {torrent_hash[:8]}",
                {"local": local, "selected_qbt_files": len(qbt_selected_files)},
                hash=torrent_hash,
            )
            return None
        if manifest:
            files, media_files, total_size, remote, copy_mode = manifest
            payload.update({"files": files, "media_files": media_files, "size": total_size, "remote": remote, "copy_mode": copy_mode})
        return payload

    def _selected_qbt_files_for_completed_payload(self, torrent_hash: str) -> list[Mapping[str, Any]] | None:
        if self.qbt is None or not hasattr(self.qbt, "torrent_files"):
            return None
        try:
            rows = [dict(row) for row in self.qbt.torrent_files(torrent_hash)]
        except Exception as exc:
            self.obs.event("warning", "file_batch", "completed_file_probe_failed", str(redact(str(exc))), {"hash": torrent_hash}, hash=torrent_hash)
            return None
        return [row for row in rows if int(row.get("priority", 1) or 0) != 0]

    def _local_path(self, torrent: Mapping[str, Any]) -> str:
        raw = str(torrent.get("content_path") or "")
        if not raw:
            save_path = str(torrent.get("save_path") or f"{self.container_downloads}/active")
            raw = str(PurePosixPath(save_path) / str(torrent.get("name") or torrent.get("hash") or "torrent"))
        if raw == self.container_downloads:
            local = self.host_downloads
        elif raw.startswith(self.container_downloads + "/"):
            local = self._host_path_for_container_suffix(raw[len(self.container_downloads):])
        else:
            local = raw

        # qBT v5 reports ``content_path`` as the concrete file path for
        # single-file torrents.  Upload manifests, however, are rooted at the
        # torrent folder so qBT file rows like ``DASS-592/movie.mp4`` can be
        # normalized to ``movie.mp4`` and copied as a normal manifest.
        path = Path(local)
        if path.is_file():
            return str(path.parent)
        return local

    def _host_path_for_container_suffix(self, suffix: str) -> str:
        rel = str(suffix or "").replace("\\", "/").lstrip("/")
        if not rel:
            return self.host_downloads
        if "\\" in self.host_downloads or Path(self.host_downloads).drive:
            return str(Path(self.host_downloads).joinpath(*PurePosixPath(rel).parts))
        return f"{self.host_downloads.rstrip('/')}/{rel}"

    def _manifest_for(self, local: str, remote_dir: str, selected_qbt_files: list[Mapping[str, Any]] | None = None) -> tuple[list[dict[str, Any]], list[dict[str, Any]], int, str, str] | None:
        path = Path(local)
        if not path.exists():
            return None
        allowed_relatives = self._allowed_manifest_relatives(path, selected_qbt_files)
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
            if allowed_relatives is not None and rel_posix not in allowed_relatives:
                continue
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
        copy_mode = "copy_files" if selected_qbt_files is not None else "copy"
        return files, media_files, total, remote_dir, copy_mode

    @staticmethod
    def _allowed_manifest_relatives(root: Path, selected_qbt_files: list[Mapping[str, Any]] | None) -> set[str] | None:
        if selected_qbt_files is None:
            return None
        allowed: set[str] = set()
        root_name = root.name
        for row in selected_qbt_files:
            rel = FileBatchService._file_relative_path(row)
            if not rel:
                continue
            rel_path = FileBatchService._safe_qbt_relative_path(rel)
            if rel_path is None:
                continue
            allowed.add(rel_path.as_posix())
            parts = rel_path.parts
            if len(parts) > 1 and parts[0] == root_name:
                stripped = FileBatchService._safe_qbt_relative_path(PurePosixPath(*parts[1:]).as_posix())
                if stripped is not None:
                    allowed.add(stripped.as_posix())
        return allowed

    def _resolve_selected_local_file(self, root: Path, row: Mapping[str, Any]) -> tuple[Path, str] | None:
        rel = self._file_relative_path(row)
        rel_path = self._safe_qbt_relative_path(rel)
        if rel_path is None:
            return None
        candidate_relatives = [rel_path]
        parts = rel_path.parts
        if len(parts) > 1 and parts[0] == root.name:
            stripped = self._safe_qbt_relative_path(PurePosixPath(*parts[1:]).as_posix())
            if stripped is not None:
                candidate_relatives.append(stripped)
        if root.is_file() and parts and root.name == parts[-1]:
            return root, root.name
        root_resolved = root.resolve(strict=False)
        seen: set[str] = set()
        for candidate_rel in candidate_relatives:
            rel_posix = candidate_rel.as_posix()
            if rel_posix in seen:
                continue
            seen.add(rel_posix)
            candidate = root.joinpath(*candidate_rel.parts)
            if not candidate.exists() or not candidate.is_file():
                continue
            try:
                candidate.resolve(strict=False).relative_to(root_resolved)
            except ValueError:
                continue
            return candidate, rel_posix
        return None

    @staticmethod
    def _safe_qbt_relative_path(rel: str) -> PurePosixPath | None:
        rel = str(rel or "").replace("\\", "/").lstrip("/")
        if not rel:
            return None
        path = PurePosixPath(rel)
        if any(part in {"", ".", ".."} for part in path.parts):
            return None
        return path
