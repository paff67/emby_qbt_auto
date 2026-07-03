from __future__ import annotations

import json
import re
import sqlite3
import time
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Any, Mapping

from .db import write_transaction
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


def _is_stopped(torrent: Mapping[str, Any]) -> bool:
    return str(torrent.get("state") or "").lower() in {"pauseddl", "pausedup", "stoppeddl", "stoppedup", "paused", "stopped"}


MEDIA_EXTENSIONS = {".mp4", ".mkv", ".avi", ".mov", ".wmv", ".m4v", ".ts"}


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
        self.now = now or (lambda: int(time.time()))
        self.jobs = TorrentJobRepository(state_db)
        self.obs = ObservabilityStore(state_db)

    def sync_completed(self, snapshots: Mapping[str, Mapping[str, Any]], free_bytes: int | None = None, sync_healthy: bool = True) -> FileBatchResult:
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
        if self._inflight_batch_count(h) >= self.max_inflight_batches_per_torrent:
            self._decision(h, "prefetch_blocked", "inflight_batch_cap", {"limit": self.max_inflight_batches_per_torrent})
            return {"created": 0, "blocked": 1}
        try:
            files = self.qbt.torrent_files(h)
            piece_size = self._piece_size(h, torrent)
        except Exception as exc:
            self.obs.event("error", "file_batch", "batch_file_probe_failed", str(redact(str(exc))), {"hash": h}, hash=h)
            return {"created": 0, "blocked": 1}
        selected, reservation = self._select_batch_files(files, piece_size, free_bytes)
        if not selected or reservation is None:
            self._decision(
                h,
                "prefetch_blocked",
                "batch_budget_insufficient",
                {"free_bytes": int(free_bytes), "safe_budget": self._safe_batch_budget(free_bytes), "piece_size": piece_size},
            )
            return {"created": 0, "blocked": 1}
        if reservation.payload_efficiency < self.min_payload_efficiency:
            self._decision(
                h,
                "prefetch_blocked",
                "payload_efficiency_too_low",
                {"payload_efficiency": reservation.payload_efficiency, "min_payload_efficiency": self.min_payload_efficiency},
            )
            return {"created": 0, "blocked": 1}
        indices = [int(f["index"]) for f in selected]
        if self.backpressure_policy is not None:
            decision = self.backpressure_policy.evaluate(self.state_db, candidate_bytes=int(reservation.reserved_bytes))
            self.backpressure_policy.record(self.state_db, decision, torrent_hash=h)
            if not decision.allow_new_upload_jobs:
                self._decision(h, "prefetch_blocked", decision.reason, {"reserved_bytes": reservation.reserved_bytes})
                return {"created": 0, "blocked": 1}
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
            return {"created": 0, "blocked": 0}
        batch_id = self._create_batch_and_reservation(h, indices, reservation, piece_size)
        try:
            self._apply_batch_to_qbt(h, indices, [int(f.get("index")) for f in files], should_start=_is_stopped(torrent))
        except Exception as exc:
            self._mark_batch_failed(batch_id, str(exc))
            return {"created": 0, "blocked": 1}
        self._mark_batch_applied(batch_id)
        self._decision(h, "prefetch_allowed", "batch_pipeline_reserved", {"batch_id": batch_id, **payload})
        self.obs.event("info", "file_batch", "batch_applied", f"batch {batch_id} applied", {"batch_id": batch_id, **payload}, hash=h)
        return {"created": 1, "blocked": 0}

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
            candidate = [*selected, row]
            reservation = compute_batch_reservation(candidate, piece_size=piece_size, filesystem_slack=self.filesystem_slack_bytes)
            if reservation.payload_bytes > self.max_batch_bytes:
                break
            if reservation.reserved_bytes > budget:
                break
            selected = candidate
        if not selected:
            return [], None
        return selected, compute_batch_reservation(selected, piece_size=piece_size, filesystem_slack=self.filesystem_slack_bytes)

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
            return self.host_downloads + raw[len(self.container_downloads):]
        return raw

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
