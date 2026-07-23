from __future__ import annotations

import inspect
import json
import math
import os
import stat as statmod
import time
from dataclasses import dataclass, field
from pathlib import Path, PurePosixPath
from typing import Any, Callable, Mapping
from urllib.parse import quote

from .capacity_assessment import CapacityAssessment, TorrentCapacityEvidence
from .db import readonly_connect, write_transaction
from .observability import redact


STOPPED_DOWNLOAD_STATES = frozenset({"stoppedDL", "pausedDL"})


def _is_stopped_download_state(value: Any) -> bool:
    return str(value or "") in STOPPED_DOWNLOAD_STATES


PROTECTED_TAGS = frozenset({"hold", "seed-long"})
OPEN_JOB_STATES = (
    "queued",
    "running",
    "verify_pending",
    "retry_wait",
    "promotion_wait",
    "cleanup_wait",
)
RECLAIM_RELEASE_STATES = frozenset({"released", "cancelled"})
RECLAIM_LOCKED_STATES = frozenset(
    {
        "stopping",
        "deleting",
        "quarantined",
        "deleted",
        "recheck_pending",
        "partial_or_unknown",
        "aborted_paused",
        "stop_unknown",
        "reclaimed",
        "failed",
    }
)
MAGNET_PREFIX = "mag" + "net:?"
PROGRESS_EPSILON = 1e-9
CONTENT_SIZE_FIELDS = ("size", "total_size", "wanted_size")
QUARANTINE_DIRNAME = ".qbt-orchestrator-reclaim"
FILE_ATTRIBUTE_REPARSE_POINT = getattr(
    statmod, "FILE_ATTRIBUTE_REPARSE_POINT", 0x400
)


@dataclass(frozen=True)
class FilesystemIdentity:
    dev: int
    ino: int
    mode: int


class UnsafeFilesystemObject(RuntimeError):
    pass


class FilesystemIdentityChanged(RuntimeError):
    pass


def _nonnegative_integer(value: Any) -> int | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(number) or number < 0 or not number.is_integer():
        return None
    return int(number)


def _filesystem_id(value: Any) -> int | None:
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value if value >= 0 else None
    text = str(value).strip()
    if not text.isdigit():
        return None
    return int(text)


def _completed_bytes(item: Mapping[str, Any]) -> int | None:
    for field in ("completed_bytes", "completed", "downloaded"):
        if field in item and item[field] is not None:
            return _nonnegative_integer(item[field])
    return None


def capacity_reclaim_locked_hashes(state_db: str | Path) -> set[str]:
    """Return hashes held by a durable reclaim lease.

    Unknown states deliberately remain locked.  Only an explicit future human
    confirmation transition to ``released`` or ``cancelled`` lifts the lease.
    """

    con = readonly_connect(state_db)
    try:
        rows = con.execute(
            "select distinct hash from capacity_reclaims "
            "where state not in ('released','cancelled')"
        ).fetchall()
        return {str(row["hash"]) for row in rows if str(row["hash"] or "")}
    finally:
        con.close()


class CapacityReclaimAuditStore:
    """Persist every live reclaim and atomically queue its Telegram notices."""

    def __init__(
        self,
        state_db: str | Path,
        *,
        notification_chat_ids: list[str] | tuple[str, ...] | None = None,
        now: Callable[[], int] | None = None,
    ):
        self.state_db = Path(state_db)
        self.notification_chat_ids = tuple(
            dict.fromkeys(
                str(chat_id).strip()
                for chat_id in (notification_chat_ids or [])
                if str(chat_id).strip()
            )
        )
        self.now = now or (lambda: int(__import__("time").time()))

    @staticmethod
    def _identity(candidate: Mapping[str, Any]) -> dict[str, Any]:
        torrent_hash = str(candidate.get("hash") or "").strip()
        name = " ".join(str(candidate.get("name") or torrent_hash).split())[:512]
        magnet_uri = str(candidate.get("magnet_uri") or "").strip()
        if not magnet_uri.startswith(MAGNET_PREFIX):
            magnet_uri = (
                MAGNET_PREFIX
                + "xt=urn:btih:"
                + quote(torrent_hash, safe="")
                + "&dn="
                + quote(name, safe="")
            )
        reclaimable_since = int(candidate.get("reclaimable_since") or 0)
        dead_since = candidate.get("dead_since")
        assessment_json = str(candidate.get("assessment_json") or "{}")
        return {
            "reclaim_key": f"{torrent_hash}:{reclaimable_since}",
            "hash": torrent_hash,
            "name": name,
            "magnet_uri": magnet_uri,
            "host_path": str(candidate.get("host_path") or ""),
            "content_path": str(candidate.get("content_path") or ""),
            "allocated_bytes": max(0, int(candidate.get("allocated_bytes") or 0)),
            "completed_bytes": max(0, int(candidate.get("completed_bytes") or 0)),
            "progress": float(candidate.get("progress") or 0.0),
            "dead_since": None if dead_since is None else int(dead_since),
            "reclaimable_since": reclaimable_since,
            "capacity_generation": int(candidate.get("capacity_generation") or 0),
            "capacity_reason": str(candidate.get("capacity_reason") or "")[:256],
            "assessment_json": assessment_json,
            "quarantine_path": str(candidate.get("quarantine_path") or "").strip()
            or None,
            "filesystem_dev": _filesystem_id(candidate.get("filesystem_dev")),
            "filesystem_ino": _filesystem_id(candidate.get("filesystem_ino")),
        }

    @staticmethod
    def _safe_error(error: str | None) -> str | None:
        if error is None:
            return None
        return str(redact(str(error)))[:2000]

    @staticmethod
    def _notification_ids(row: Mapping[str, Any]) -> list[int]:
        try:
            values = json.loads(str(row["notification_ids_json"] or "[]"))
        except (TypeError, ValueError, json.JSONDecodeError):
            values = []
        return [int(value) for value in values if str(value).isdigit()]

    def _enqueue_notification(
        self,
        con,
        row: Mapping[str, Any],
        *,
        kind: str,
        level: str,
        message: str,
        payload: Mapping[str, Any],
        now: int,
    ) -> list[int]:
        notification_ids = self._notification_ids(row)
        for chat_id in self.notification_chat_ids:
            dedupe_key = f"capacity-reclaim:{int(row['id'])}:{kind}:{chat_id}"
            con.execute(
                "insert or ignore into bot_notifications(dedupe_key,chat_id,level,topic,message,"
                "payload_json,state,attempts,created_at,updated_at) "
                "values(?,?,?,?,?,?,'queued',0,?,?)",
                (
                    dedupe_key,
                    chat_id,
                    level,
                    "capacity_reclaim",
                    message,
                    json.dumps(dict(payload), ensure_ascii=False, sort_keys=True),
                    now,
                    now,
                ),
            )
            notice = con.execute(
                "select id from bot_notifications where dedupe_key=?", (dedupe_key,)
            ).fetchone()
            assert notice is not None
            notice_id = int(notice["id"])
            if notice_id not in notification_ids:
                notification_ids.append(notice_id)
        con.execute(
            "update capacity_reclaims set notification_ids_json=?,updated_at=? where id=?",
            (json.dumps(notification_ids), now, int(row["id"])),
        )
        return notification_ids

    @staticmethod
    def _reservation_rejected(
        generation: int,
        reason: str,
    ) -> dict[str, Any]:
        return {
            "reclaim_id": None,
            "state": None,
            "capacity_generation": int(generation),
            "reserved": False,
            "reason": str(reason),
        }

    def _eligibility_reason(
        self,
        con,
        candidate: Mapping[str, Any],
    ) -> str | None:
        torrent_hash = str(candidate.get("hash") or "").strip()
        generation = int(candidate.get("capacity_generation") or 0)
        assessment = con.execute(
            "select current_generation from capacity_assessment_state where id=1"
        ).fetchone()
        if assessment is None or int(assessment["current_generation"]) != generation:
            return "stale_assessment"
        health = con.execute(
            "select capacity_generation,capacity_viable,reclaimable_since,no_progress_since "
            "from torrent_health where hash=?",
            (torrent_hash,),
        ).fetchone()
        if health is None:
            return "health_missing"
        if int(health["capacity_generation"] or 0) != generation:
            return "stale_health_generation"
        if health["capacity_viable"] is None or int(health["capacity_viable"]) != 0:
            return "capacity_viable"
        reclaimable_since = candidate.get("reclaimable_since")
        if (
            reclaimable_since is None
            or health["reclaimable_since"] is None
            or int(health["reclaimable_since"]) != int(reclaimable_since)
        ):
            return "reclaimable_changed"
        no_progress_since = candidate.get("no_progress_since")
        if (
            no_progress_since is None
            or health["no_progress_since"] is None
            or int(health["no_progress_since"]) != int(no_progress_since)
        ):
            return "progress_evidence_changed"
        placeholders = ",".join("?" for _ in OPEN_JOB_STATES)
        if con.execute(
            f"select 1 from torrent_jobs where hash=? and state in ({placeholders}) limit 1",
            (torrent_hash, *OPEN_JOB_STATES),
        ).fetchone() is not None:
            return "open_job"
        now = int(self.now())
        if con.execute(
            "select 1 from resource_reservations where hash=? and state='active' "
            "and (expires_at is null or expires_at>?) limit 1",
            (torrent_hash, now),
        ).fetchone() is not None:
            return "active_reservation"
        if con.execute(
            "select 1 from soak_state where hash=? and cooldown_until is not null "
            "and cooldown_until>? limit 1",
            (torrent_hash, now),
        ).fetchone() is not None:
            return "active_cooldown"
        return None

    def reserve(self, candidate: Mapping[str, Any]) -> dict[str, Any]:
        identity = self._identity(candidate)
        now = int(self.now())

        def txn(con) -> dict[str, Any]:
            if not con.in_transaction:
                con.execute("begin immediate")
            existing = con.execute(
                "select id,state,capacity_generation from capacity_reclaims where reclaim_key=?",
                (identity["reclaim_key"],),
            ).fetchone()
            if existing is not None:
                return {
                    "reclaim_id": int(existing["id"]),
                    "state": str(existing["state"]),
                    "capacity_generation": int(existing["capacity_generation"] or 0),
                    "reserved": False,
                    "reason": "reclaim_already_recorded",
                }
            locked = con.execute(
                "select 1 from capacity_reclaims where hash=? "
                "and state not in ('released','cancelled') limit 1",
                (identity["hash"],),
            ).fetchone()
            if locked is not None:
                return self._reservation_rejected(
                    identity["capacity_generation"], "capacity_reclaim_locked"
                )
            reason = self._eligibility_reason(con, candidate)
            if reason is not None:
                return self._reservation_rejected(
                    identity["capacity_generation"], reason
                )
            con.execute(
                "insert into capacity_reclaims(reclaim_key,hash,name,magnet_uri,host_path,content_path,"
                "allocated_bytes,completed_bytes,progress,dead_since,reclaimable_since,capacity_generation,"
                "capacity_reason,assessment_json,state,recheck_state,created_at,updated_at) "
                "values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,'stopping','not_requested',?,?)",
                (
                    identity["reclaim_key"],
                    identity["hash"],
                    identity["name"],
                    identity["magnet_uri"],
                    identity["host_path"],
                    identity["content_path"],
                    identity["allocated_bytes"],
                    identity["completed_bytes"],
                    identity["progress"],
                    identity["dead_since"],
                    identity["reclaimable_since"],
                    identity["capacity_generation"],
                    identity["capacity_reason"],
                    identity["assessment_json"],
                    now,
                    now,
                ),
            )
            row = con.execute(
                "select id,state,capacity_generation from capacity_reclaims where reclaim_key=?",
                (identity["reclaim_key"],),
            ).fetchone()
            assert row is not None
            return {
                "reclaim_id": int(row["id"]),
                "state": str(row["state"]),
                "capacity_generation": int(row["capacity_generation"] or 0),
                "reserved": True,
                "reason": None,
            }

        return dict(write_transaction(self.state_db, txn))

    def _mark_warning_state(
        self,
        reclaim_id: int,
        generation: int,
        *,
        target_state: str,
        allowed_states: tuple[str, ...],
        reason: str,
        error: str | None = None,
    ) -> bool:
        now = int(self.now())
        safe_error = self._safe_error(error)

        def txn(con) -> bool:
            row = con.execute(
                "select * from capacity_reclaims where id=? and capacity_generation=?",
                (int(reclaim_id), int(generation)),
            ).fetchone()
            if row is None:
                return False
            current_state = str(row["state"])
            if current_state not in {*allowed_states, target_state}:
                return False
            if current_state != target_state:
                placeholders = ",".join("?" for _ in allowed_states)
                changed = con.execute(
                    "update capacity_reclaims set state=?,recheck_state='not_requested',"
                    f"recheck_error=?,updated_at=? where id=? and capacity_generation=? and state in ({placeholders})",
                    (
                        target_state,
                        safe_error or str(reason)[:2000],
                        now,
                        int(reclaim_id),
                        int(generation),
                        *allowed_states,
                    ),
                )
                if int(changed.rowcount or 0) != 1:
                    return False
                row = con.execute(
                    "select * from capacity_reclaims where id=?", (int(reclaim_id),)
                ).fetchone()
                assert row is not None
            if target_state == "stop_unknown":
                status_text = "停止结果未知，未执行删除，需人工确认任务状态。"
            else:
                status_text = "任务已保持暂停，需人工确认继续或保留暂停。"
            message = (
                f"qBT 容量回收已中止；{status_text}\n"
                f"种子名：{row['name']}\nHash：{row['hash']}\n原因：{reason}"
            )
            payload = {
                "hash": str(row["hash"]),
                "name": str(row["name"]),
                "reason": str(reason),
                "error": safe_error,
                "reclaim_id": int(row["id"]),
                "requires_confirmation": True,
                "allowed_actions": ["resume", "keep_paused"],
            }
            self._enqueue_notification(
                con,
                row,
                kind=target_state,
                level="warning",
                message=message,
                payload=payload,
                now=now,
            )
            return True

        return bool(write_transaction(self.state_db, txn))

    def mark_aborted_paused(
        self,
        reclaim_id: int,
        generation: int,
        reason: str,
        error: str | None = None,
    ) -> bool:
        return self._mark_warning_state(
            reclaim_id,
            generation,
            target_state="aborted_paused",
            allowed_states=("stopping",),
            reason=reason,
            error=error,
        )

    def mark_stop_unknown(
        self,
        reclaim_id: int,
        generation: int,
        reason: str,
        error: str | None = None,
    ) -> bool:
        return self._mark_warning_state(
            reclaim_id,
            generation,
            target_state="stop_unknown",
            allowed_states=("stopping",),
            reason=reason,
            error=error,
        )

    def mark_restored_aborted_paused(
        self,
        reclaim_id: int,
        generation: int,
        reason: str,
        error: str | None = None,
    ) -> bool:
        return self._mark_warning_state(
            reclaim_id,
            generation,
            target_state="aborted_paused",
            allowed_states=("deleting", "quarantined"),
            reason=reason,
            error=error,
        )

    def mark_deleting(
        self,
        reclaim_id: int,
        candidate: Mapping[str, Any],
    ) -> bool:
        identity = self._identity(candidate)
        now = int(self.now())
        generation = int(identity["capacity_generation"])

        def txn(con) -> bool:
            if not con.in_transaction:
                con.execute("begin immediate")
            row = con.execute(
                "select id from capacity_reclaims where id=? and reclaim_key=? and hash=? "
                "and capacity_generation=? and state='stopping'",
                (
                    int(reclaim_id),
                    identity["reclaim_key"],
                    identity["hash"],
                    generation,
                ),
            ).fetchone()
            if row is None or self._eligibility_reason(con, candidate) is not None:
                return False
            if identity["filesystem_dev"] is None or identity["filesystem_ino"] is None:
                return False
            changed = con.execute(
                "update capacity_reclaims set state='deleting',recheck_state='pending',"
                "recheck_error=null,filesystem_dev=?,filesystem_ino=?,quarantine_path=null,"
                "updated_at=? where id=? and capacity_generation=? "
                "and reclaim_key=? and hash=? and state='stopping'",
                (
                    int(identity["filesystem_dev"]),
                    int(identity["filesystem_ino"]),
                    now,
                    int(reclaim_id),
                    generation,
                    identity["reclaim_key"],
                    identity["hash"],
                ),
            )
            return int(changed.rowcount or 0) == 1

        return bool(write_transaction(self.state_db, txn))

    def mark_quarantined(
        self,
        reclaim_id: int,
        generation: int,
        quarantine_path: str | Path,
        filesystem_dev: int,
        filesystem_ino: int,
    ) -> bool:
        now = int(self.now())
        changed = write_transaction(
            self.state_db,
            lambda con: con.execute(
                "update capacity_reclaims set state='quarantined',quarantine_path=?,"
                "updated_at=? where id=? and capacity_generation=? and state='deleting' "
                "and filesystem_dev=? and filesystem_ino=?",
                (
                    str(quarantine_path),
                    now,
                    int(reclaim_id),
                    int(generation),
                    int(filesystem_dev),
                    int(filesystem_ino),
                ),
            ).rowcount,
        )
        return int(changed or 0) == 1

    def mark_deleted(self, reclaim_id: int, generation: int) -> bool:
        now = int(self.now())
        changed = write_transaction(
            self.state_db,
            lambda con: con.execute(
                "update capacity_reclaims set state='deleted',recheck_state='pending',"
                "recheck_error=null,updated_at=? where id=? and capacity_generation=? "
                "and state in ('deleting','quarantined')",
                (now, int(reclaim_id), int(generation)),
            ).rowcount,
        )
        return int(changed or 0) == 1

    def mark_partial_or_unknown(
        self,
        reclaim_id: int,
        generation: int,
        reason: str,
        error: str | None = None,
    ) -> bool:
        return self._mark_warning_state(
            reclaim_id,
            generation,
            target_state="partial_or_unknown",
            allowed_states=("deleting", "quarantined", "deleted", "recheck_pending"),
            reason=reason,
            error=error,
        )

    def recovery_rows(self) -> list[dict[str, Any]]:
        con = readonly_connect(self.state_db)
        try:
            rows = con.execute(
                "select * from capacity_reclaims where "
                "state in ('stopping','deleting','quarantined','deleted','recheck_pending','partial_or_unknown') "
                "order by capacity_generation,id",
            ).fetchall()
            return [dict(row) for row in rows]
        finally:
            con.close()

    def mark_recheck_pending(
        self,
        reclaim_id: int,
        generation: int,
        error: str,
    ) -> dict[str, Any]:
        now = int(self.now())
        safe_error = self._safe_error(error)

        def txn(con) -> dict[str, Any]:
            row = con.execute(
                "select * from capacity_reclaims where id=? and capacity_generation=?",
                (int(reclaim_id), int(generation)),
            ).fetchone()
            if row is None or str(row["state"]) not in {"deleted", "recheck_pending"}:
                raise RuntimeError("capacity reclaim recheck state changed")
            con.execute(
                "update capacity_reclaims set state='recheck_pending',recheck_state='failed',"
                "recheck_error=?,updated_at=? where id=? and capacity_generation=? "
                "and state in ('deleted','recheck_pending')",
                (safe_error, now, int(reclaim_id), int(generation)),
            )
            row = con.execute(
                "select * from capacity_reclaims where id=?", (int(reclaim_id),)
            ).fetchone()
            assert row is not None
            notification_ids = self._enqueue_notification(
                con,
                row,
                kind="recheck_pending",
                level="warning",
                message=(
                    "qBT 容量回收已删除文件，但重新校验请求失败，将在后续运行重试。\n"
                    f"种子名：{row['name']}\nHash：{row['hash']}"
                ),
                payload={
                    "hash": str(row["hash"]),
                    "name": str(row["name"]),
                    "reason": "recheck_failed",
                    "error": safe_error,
                    "reclaim_id": int(row["id"]),
                },
                now=now,
            )
            return {
                "reclaim_id": int(row["id"]),
                "notification_ids": notification_ids,
                "state": "recheck_pending",
                "recheck_state": "failed",
            }

        return dict(write_transaction(self.state_db, txn))

    def complete(
        self,
        reclaim_id: int,
        candidate: Mapping[str, Any],
        *,
        recheck_error: str | None,
    ) -> dict[str, Any]:
        generation = int(candidate.get("capacity_generation") or 0)
        if recheck_error is not None:
            return self.mark_recheck_pending(
                reclaim_id, generation, recheck_error
            )
        identity = self._identity(candidate)
        now = int(self.now())
        full_magnet = identity["magnet_uri"]
        prefix = (
            "qBT 编排器自动容量回收完成\n"
            f"种子名：{identity['name']}\n"
            f"Hash：{identity['hash']}\n"
            f"释放空间：{identity['allocated_bytes'] / 1024**3:.2f} GiB\n"
            "状态：已请求重新校验\n"
            "磁力链接：\n"
        )
        push_magnet = full_magnet
        if len(prefix) + len(push_magnet) > 4000:
            push_magnet = (
                MAGNET_PREFIX
                + "xt=urn:btih:"
                + quote(identity["hash"], safe="")
                + "&dn="
                + quote(identity["name"], safe="")
            )
        message = prefix + push_magnet
        payload = {
            **identity,
            "reclaim_id": int(reclaim_id),
            "recheck_state": "requested",
            "recheck_error": None,
        }
        payload_json = json.dumps(payload, ensure_ascii=False, sort_keys=True)

        def txn(con) -> dict[str, Any]:
            changed = con.execute(
                "update capacity_reclaims set state='reclaimed',recheck_state='requested',"
                "recheck_error=null,reclaimed_at=?,updated_at=? where id=? "
                "and capacity_generation=? and state in ('deleted','recheck_pending')",
                (now, now, int(reclaim_id), generation),
            )
            if int(changed.rowcount or 0) != 1:
                raise RuntimeError("capacity reclaim completion state changed")
            row = con.execute(
                "select * from capacity_reclaims where id=?", (int(reclaim_id),)
            ).fetchone()
            assert row is not None
            notification_ids = self._enqueue_notification(
                con,
                row,
                kind="reclaimed",
                level="info",
                message=message,
                payload=json.loads(payload_json),
                now=now,
            )
            return {
                "reclaim_id": int(reclaim_id),
                "notification_ids": notification_ids,
                "state": "reclaimed",
                "recheck_state": "requested",
            }

        return dict(write_transaction(self.state_db, txn))


@dataclass(frozen=True)
class CapacityReclaimResult:
    dry_run: bool
    planned: int = 0
    reclaimed: int = 0
    planned_bytes: int = 0
    reclaimed_bytes: int = 0
    candidates: list[dict[str, Any]] = field(default_factory=list)
    rejection_counts: dict[str, int] = field(default_factory=dict)
    errors: list[str] = field(default_factory=list)
    assessment_generation: int = 0

    def as_dict(self) -> dict[str, Any]:
        return {
            "dry_run": bool(self.dry_run),
            "planned": int(self.planned),
            "reclaimed": int(self.reclaimed),
            "planned_bytes": int(self.planned_bytes),
            "reclaimed_bytes": int(self.reclaimed_bytes),
            "candidates": [dict(item) for item in self.candidates],
            "rejection_counts": dict(self.rejection_counts),
            "errors": list(self.errors),
            "assessment_generation": int(self.assessment_generation),
        }


class DeadPartialReclaimer:
    """Reclaim payload bytes from persistently dead torrents without deleting torrents.

    The torrent remains registered in qBittorrent.  Live mode stops it, removes
    only its validated content path, and requests a recheck so it can be retried
    later from zero if availability returns.
    """

    def __init__(
        self,
        state_db: str | Path,
        executor,
        *,
        host_downloads: str | Path,
        container_downloads: str,
        managed_root: str | Path,
        dry_run: bool = True,
        min_reclaimable_age_sec: int = 3_600,
        min_dead_age_sec: int | None = None,
        min_reclaim_bytes: int = 64 * 1024**2,
        max_per_tick: int = 1,
        notification_chat_ids: list[str] | tuple[str, ...] | None = None,
        now: Callable[[], int] | None = None,
        sleep: Callable[[float], None] | None = None,
        monotonic: Callable[[], float] | None = None,
        stop_timeout_sec: float = 5.0,
        stop_poll_interval_sec: float = 0.1,
        stop_max_polls: int = 8,
        inventory_timeout_sec: float = 5.0,
    ):
        self.state_db = Path(state_db)
        self.executor = executor
        self.host_downloads = Path(host_downloads).resolve()
        self.container_downloads = PurePosixPath(str(container_downloads))
        self.managed_root = Path(managed_root).resolve()
        self.dry_run = bool(dry_run)
        if min_dead_age_sec is not None:
            if (
                int(min_reclaimable_age_sec) != 3_600
                and int(min_reclaimable_age_sec) != int(min_dead_age_sec)
            ):
                raise ValueError("conflicting reclaimable age settings")
            min_reclaimable_age_sec = int(min_dead_age_sec)
        self.min_reclaimable_age_sec = max(0, int(min_reclaimable_age_sec))
        self.min_reclaim_bytes = max(0, int(min_reclaim_bytes))
        self.max_per_tick = max(0, int(max_per_tick))
        self.now = now or (lambda: int(__import__("time").time()))
        self.sleep = sleep or time.sleep
        self.monotonic = monotonic or time.monotonic
        self.stop_timeout_sec = max(0.0, float(stop_timeout_sec))
        self.stop_poll_interval_sec = max(0.001, float(stop_poll_interval_sec))
        self.stop_max_polls = max(1, int(stop_max_polls))
        self.inventory_timeout_sec = max(0.0, float(inventory_timeout_sec))
        self.audit = CapacityReclaimAuditStore(
            self.state_db,
            notification_chat_ids=notification_chat_ids,
            now=self.now,
        )
        if not self.managed_root.is_relative_to(self.host_downloads):
            raise ValueError("capacity reclaim managed_root must be inside host_downloads")
        self.quarantine_root = self.managed_root / QUARANTINE_DIRNAME

    def run(
        self,
        snapshots: Mapping[str, Mapping[str, Any]],
        *,
        assessment: CapacityAssessment | None = None,
        capacity_state: str,
        free_bytes: int,
        target_free_bytes: int,
    ) -> CapacityReclaimResult:
        generation = 0 if assessment is None else int(assessment.generation)
        recovery_errors = [] if self.dry_run else self._reconcile_reclaims()
        if generation <= 0:
            return CapacityReclaimResult(
                dry_run=self.dry_run,
                assessment_generation=generation,
                rejection_counts={"uncommitted_assessment": 1},
                errors=recovery_errors,
            )
        assert assessment is not None
        if self._current_assessment_generation() != generation:
            return CapacityReclaimResult(
                dry_run=self.dry_run,
                assessment_generation=generation,
                rejection_counts={"stale_assessment": 1},
                errors=recovery_errors,
            )
        if recovery_errors:
            return CapacityReclaimResult(
                dry_run=self.dry_run,
                assessment_generation=generation,
                rejection_counts={"reconciliation_failed": 1},
                errors=recovery_errors,
            )
        if (
            str(capacity_state) != "capacity_deadlock"
            or int(free_bytes) >= int(target_free_bytes)
            or self.max_per_tick <= 0
        ):
            return CapacityReclaimResult(
                dry_run=self.dry_run,
                assessment_generation=generation,
                errors=recovery_errors,
            )

        (
            eligible_rows,
            open_jobs,
            active_claims,
            active_cooldowns,
        ) = self._eligibility_state()
        all_paths = self._snapshot_paths(snapshots)
        snapshots_by_hash = {
            str(raw.get("hash") or fallback_hash): dict(raw)
            for fallback_hash, raw in snapshots.items()
        }
        rejection_counts: dict[str, int] = {}

        def reject(reason: str) -> None:
            rejection_counts[reason] = rejection_counts.get(reason, 0) + 1

        candidates: list[dict[str, Any]] = []
        now = int(self.now())
        for assessment_hash, evidence in assessment.torrents.items():
            torrent_hash = str(evidence.hash or assessment_hash)
            torrent = snapshots_by_hash.get(torrent_hash)
            if torrent is None:
                reject("snapshot_missing")
                continue
            if not evidence.managed:
                reject("not_managed")
                continue
            if not evidence.incomplete:
                reject("not_incomplete")
                continue
            try:
                assessed_availability = (
                    None
                    if evidence.availability is None
                    else float(evidence.availability)
                )
            except (TypeError, ValueError):
                assessed_availability = None
            if (
                assessed_availability is None
                or not math.isfinite(assessed_availability)
                or assessed_availability < 0
            ):
                reject("availability_unknown")
                continue
            if (
                assessed_availability >= 1.0
                or int(evidence.complete_sources) > 0
            ):
                reject("complete_source")
                continue
            if evidence.viable:
                reject("capacity_viable")
                continue
            row = eligible_rows.get(torrent_hash)
            if row is None:
                reject("health_missing")
                continue
            if int(row.get("capacity_generation") or 0) != generation:
                reject("stale_health_generation")
                continue
            if (
                row.get("capacity_viable") is None
                or int(row["capacity_viable"]) != 0
            ):
                reject("capacity_viable")
                continue
            if row.get("no_progress_since") is None or evidence.no_progress_since is None:
                reject("no_progress_unconfirmed")
                continue
            if int(row["no_progress_since"]) != int(evidence.no_progress_since):
                reject("progress_evidence_changed")
                continue
            reclaimable_since = row.get("reclaimable_since")
            if reclaimable_since is None:
                reject("reclaimable_unconfirmed")
                continue
            if now - int(reclaimable_since) < self.min_reclaimable_age_sec:
                reject("reclaimable_age")
                continue
            tags = {
                item.strip()
                for item in str(torrent.get("tags") or "").split(",")
                if item.strip()
            }
            if tags & PROTECTED_TAGS:
                reject("protected_tag")
                continue
            if torrent_hash in open_jobs:
                reject("open_job")
                continue
            if torrent_hash in active_claims:
                reject("active_reservation")
                continue
            if torrent_hash in active_cooldowns:
                reject("active_cooldown")
                continue
            host_path = self._host_path(torrent.get("content_path"))
            if host_path is None:
                reject("unsafe_path")
                continue
            if self._overlaps_other(torrent_hash, host_path, all_paths):
                reject("path_overlap")
                continue
            if not host_path.exists():
                reject("path_missing")
                continue
            try:
                allocated = self._allocated_bytes(host_path)
            except OSError:
                reject("path_inspection_failed")
                continue
            if allocated < self.min_reclaim_bytes:
                reject("below_min_reclaim")
                continue
            amount_left = _nonnegative_integer(torrent.get("amount_left"))
            completed_bytes = _completed_bytes(torrent)
            try:
                progress = float(torrent.get("progress") or 0.0)
            except (TypeError, ValueError):
                progress = float("nan")
            if (
                amount_left is None
                or amount_left <= 0
                or completed_bytes is None
                or not math.isfinite(progress)
                or progress < 0
            ):
                reject("progress_evidence_unknown")
                continue
            size_baseline: dict[str, int | None] = {}
            invalid_size = False
            for field in CONTENT_SIZE_FIELDS:
                if field not in torrent or torrent[field] is None:
                    size_baseline[field] = None
                    continue
                value = _nonnegative_integer(torrent[field])
                if value is None:
                    invalid_size = True
                    break
                size_baseline[field] = value
            if invalid_size:
                reject("content_selection_unknown")
                continue
            candidates.append(
                {
                    "hash": torrent_hash,
                    "name": str(torrent.get("name") or ""),
                    "magnet_uri": str(torrent.get("magnet_uri") or ""),
                    "host_path": str(host_path),
                    "content_path": str(torrent.get("content_path") or ""),
                    "allocated_bytes": int(allocated),
                    "completed_bytes": completed_bytes,
                    "progress": progress,
                    "amount_left": amount_left,
                    **size_baseline,
                    "no_progress_since": int(evidence.no_progress_since),
                    "reclaimable_since": int(reclaimable_since),
                    "capacity_generation": generation,
                    "capacity_reason": str(row.get("capacity_reason") or ""),
                    "assessment_json": self._assessment_evidence_json(
                        assessment, evidence
                    ),
                }
            )

        candidates.sort(
            key=lambda item: (
                -int(item["allocated_bytes"]),
                float(item["progress"]),
                str(item["hash"]),
            )
        )
        needed = max(0, int(target_free_bytes) - int(free_bytes))
        selected: list[dict[str, Any]] = []
        planned_bytes = 0
        for candidate in candidates:
            if len(selected) >= self.max_per_tick or planned_bytes >= needed:
                break
            selected.append(candidate)
            planned_bytes += int(candidate["allocated_bytes"])

        if self.dry_run:
            return CapacityReclaimResult(
                dry_run=True,
                planned=len(selected),
                planned_bytes=planned_bytes,
                candidates=selected,
                rejection_counts=rejection_counts,
                assessment_generation=generation,
            )

        reclaimed = 0
        reclaimed_bytes = 0
        errors: list[str] = list(recovery_errors)
        completed: list[dict[str, Any]] = []
        for candidate in selected:
            torrent_hash = str(candidate["hash"])
            reason = self._revalidate_candidate(candidate, assessment)
            if reason is not None:
                reject(reason)
                continue
            try:
                reservation = self.audit.reserve(candidate)
            except Exception as exc:
                reject("reservation_failed")
                errors.append(f"{torrent_hash}: failed to reserve reclaim: {exc}")
                continue
            if not reservation["reserved"]:
                reject(str(reservation.get("reason") or "reservation_failed"))
                continue
            reclaim_id = int(reservation["reclaim_id"])

            def abort_paused(reason: str, error: str | None = None) -> None:
                reject(reason)
                try:
                    if not self.audit.mark_aborted_paused(
                        int(reclaim_id), generation, reason, error
                    ):
                        errors.append(
                            f"{torrent_hash}: failed to fence paused abort state"
                        )
                except Exception as exc:
                    errors.append(
                        f"{torrent_hash}: failed to persist paused abort: {exc}"
                    )

            try:
                self.executor.qbt_post(
                    "/api/v2/torrents/stop", {"hashes": torrent_hash}
                )
            except Exception as exc:
                reject("stop_failed")
                errors.append(f"{torrent_hash}: {exc}")
                try:
                    self.audit.mark_stop_unknown(
                        int(reclaim_id), generation, "stop_failed", str(exc)
                    )
                except Exception as audit_exc:
                    errors.append(
                        f"{torrent_hash}: failed to persist unknown stop: {audit_exc}"
                    )
                continue
            current, stop_reason = self._wait_until_stopped(torrent_hash)
            if stop_reason is not None:
                abort_paused(stop_reason)
                continue
            assert current is not None
            reason = self._live_torrent_rejection(
                current,
                candidate=candidate,
                assessment=assessment,
            )
            if reason is not None:
                abort_paused(reason)
                continue
            host_path = self._host_path(current.get("content_path"))
            if host_path is None:
                abort_paused("unsafe_path")
                continue
            if host_path != Path(str(candidate["host_path"])):
                abort_paused("path_changed")
                continue
            (
                fresh_paths,
                inventory_candidate,
                inventory_reason,
            ) = self._fresh_path_inventory(torrent_hash)
            if inventory_reason is not None:
                abort_paused(inventory_reason)
                continue
            assert inventory_candidate is not None
            reason = self._live_torrent_rejection(
                inventory_candidate,
                candidate=candidate,
                assessment=assessment,
            )
            if reason is not None:
                abort_paused(reason)
                continue
            if not _is_stopped_download_state(inventory_candidate.get("state")):
                abort_paused("torrent_not_stopped")
                continue
            inventory_host_path = fresh_paths.get(torrent_hash.strip().lower())
            if inventory_host_path is None:
                abort_paused("path_inventory_failed")
                continue
            if inventory_host_path != host_path:
                abort_paused("path_changed")
                continue
            if self._overlaps_other(torrent_hash, host_path, fresh_paths):
                abort_paused("path_overlap")
                continue
            if not host_path.exists():
                abort_paused("path_missing")
                continue
            try:
                allocated = self._allocated_bytes(host_path)
            except OSError as exc:
                abort_paused("path_inspection_failed", str(exc))
                continue
            if allocated < self.min_reclaim_bytes:
                abort_paused("below_min_reclaim")
                continue
            candidate = {**candidate, "allocated_bytes": int(allocated)}
            reason = self._candidate_fence_reason(candidate, assessment)
            if reason is not None:
                abort_paused(reason)
                continue
            try:
                filesystem_identity = self._capture_payload_identity(host_path)
                self._ensure_quarantine_root()
                quarantine_path = self._quarantine_destination(int(reclaim_id))
                if self._path_exists_no_follow(quarantine_path):
                    raise UnsafeFilesystemObject(
                        "quarantine destination already exists"
                    )
            except (OSError, UnsafeFilesystemObject, FilesystemIdentityChanged) as exc:
                abort_paused("unsafe_filesystem_object", str(exc))
                continue
            candidate = {
                **candidate,
                "filesystem_dev": int(filesystem_identity.dev),
                "filesystem_ino": int(filesystem_identity.ino),
                "filesystem_mode": int(filesystem_identity.mode),
                "quarantine_path": str(quarantine_path),
            }
            try:
                deleting = self.audit.mark_deleting(int(reclaim_id), candidate)
            except Exception as exc:
                errors.append(
                    f"{torrent_hash}: failed to persist deleting state: {exc}"
                )
                abort_paused("reclaim_state_changed", str(exc))
                continue
            if not deleting:
                abort_paused("reclaim_state_changed")
                continue

            def mark_partial(reason: str, error: Exception | str) -> None:
                try:
                    if not self.audit.mark_partial_or_unknown(
                        int(reclaim_id), generation, reason, str(error)
                    ):
                        errors.append(
                            f"{torrent_hash}: failed to fence partial quarantine state"
                        )
                except Exception as audit_exc:
                    errors.append(
                        f"{torrent_hash}: failed to persist partial deletion: {audit_exc}"
                    )

            try:
                self._rename_to_quarantine(
                    host_path,
                    quarantine_path,
                    filesystem_identity,
                )
            except Exception as exc:
                mark_partial("quarantine_identity_changed", exc)
                errors.append(f"{torrent_hash}: {exc}")
                continue
            try:
                if not self.audit.mark_quarantined(
                    int(reclaim_id),
                    generation,
                    quarantine_path,
                    filesystem_identity.dev,
                    filesystem_identity.ino,
                ):
                    raise RuntimeError("capacity reclaim quarantine state changed")
            except Exception as exc:
                restored = self._restore_from_quarantine(quarantine_path, host_path)
                mark_partial("quarantine_state_persist_failed", exc)
                errors.append(
                    f"{torrent_hash}: failed to persist quarantined state: {exc}; "
                    f"restored={restored}"
                )
                continue
            try:
                self._delete_quarantine_path(quarantine_path, filesystem_identity)
            except Exception as exc:
                mark_partial("quarantine_delete_partial_or_unknown", exc)
                errors.append(f"{torrent_hash}: {exc}")
                continue
            try:
                if not self.audit.mark_deleted(int(reclaim_id), generation):
                    raise RuntimeError("capacity reclaim deleted state changed")
            except Exception as exc:
                errors.append(f"{torrent_hash}: failed to persist deleted state: {exc}")
                continue
            reclaimed_bytes += int(candidate["allocated_bytes"])
            recheck_error: str | None = None
            try:
                self.executor.qbt_post(
                    "/api/v2/torrents/recheck", {"hashes": torrent_hash}
                )
            except Exception as exc:
                recheck_error = str(exc)
                errors.append(f"{torrent_hash}: {exc}")
            try:
                audit = self.audit.complete(
                    int(reclaim_id), candidate, recheck_error=recheck_error
                )
                completed.append({**candidate, **audit})
                if recheck_error is None:
                    reclaimed += 1
            except Exception as exc:
                errors.append(f"{torrent_hash}: failed to persist completed reclaim: {exc}")
                completed.append({**candidate, "state": "deleted"})
        return CapacityReclaimResult(
            dry_run=False,
            planned=len(selected),
            reclaimed=reclaimed,
            planned_bytes=planned_bytes,
            reclaimed_bytes=reclaimed_bytes,
            candidates=completed,
            rejection_counts=rejection_counts,
            errors=errors,
            assessment_generation=generation,
        )

    def _recovery_path(self, row: Mapping[str, Any]) -> Path | None:
        mapped_path = self._host_path(row.get("content_path"))
        if mapped_path is None:
            return None
        try:
            stored_path = Path(str(row.get("host_path") or "")).resolve()
        except OSError:
            return None
        if stored_path != mapped_path:
            return None
        return mapped_path

    def _reconcile_reclaims(self) -> list[str]:
        errors: list[str] = []
        try:
            current_generation = self._current_assessment_generation()
            recovery_rows = self.audit.recovery_rows()
        except Exception as exc:
            return [f"capacity reclaim recovery scan failed: {exc}"]
        for row in recovery_rows:
            reclaim_id = int(row["id"])
            torrent_hash = str(row["hash"])
            state = str(row["state"])
            row_generation = int(row.get("capacity_generation") or 0)
            same_generation = (
                current_generation > 0 and row_generation == current_generation
            )
            if state == "stopping":
                try:
                    current = self._qbt_call_with_timeout(
                        "torrent_info",
                        torrent_hash,
                        timeout=self.inventory_timeout_sec,
                    )
                    if _is_stopped_download_state(current.get("state")):
                        changed = self.audit.mark_aborted_paused(
                            reclaim_id,
                            row_generation,
                            "restart_after_stopping",
                        )
                    else:
                        changed = self.audit.mark_stop_unknown(
                            reclaim_id,
                            row_generation,
                            "restart_stop_state_unknown",
                        )
                    if not changed:
                        errors.append(
                            f"{torrent_hash}: stopping reconciliation fence changed"
                        )
                except Exception as exc:
                    try:
                        changed = self.audit.mark_stop_unknown(
                            reclaim_id,
                            row_generation,
                            "restart_stop_state_unknown",
                            str(exc),
                        )
                        if not changed:
                            errors.append(
                                f"{torrent_hash}: stopping reconciliation fence changed"
                            )
                    except Exception as audit_exc:
                        errors.append(
                            f"{torrent_hash}: failed to reconcile stopping state: {audit_exc}"
                        )
                continue

            host_path = self._recovery_path(row)

            def recovery_partial(reason: str, error: Exception | str | None = None) -> None:
                try:
                    changed = self.audit.mark_partial_or_unknown(
                        reclaim_id,
                        row_generation,
                        reason,
                        None if error is None else str(error),
                    )
                    if not changed:
                        errors.append(
                            f"{torrent_hash}: reclaim recovery fence changed"
                        )
                except Exception as audit_exc:
                    errors.append(
                        f"{torrent_hash}: failed to persist recovery warning: {audit_exc}"
                    )

            if state in {"deleting", "quarantined"}:
                try:
                    self._ensure_quarantine_root()
                    quarantine_path = self._quarantine_destination(reclaim_id)
                    if not self._quarantine_record_matches(row, quarantine_path):
                        raise UnsafeFilesystemObject(
                            "recorded quarantine path is not controlled"
                        )
                    if host_path is None:
                        raise UnsafeFilesystemObject("recorded original path is unsafe")
                    original_exists = self._path_exists_no_follow(host_path)
                    quarantine_exists = self._path_exists_no_follow(quarantine_path)
                    if original_exists and quarantine_exists:
                        raise UnsafeFilesystemObject(
                            "original and quarantine paths both exist"
                        )
                    if original_exists:
                        raise UnsafeFilesystemObject(
                            "original path exists without quarantine payload"
                        )
                    if not quarantine_exists:
                        if not same_generation:
                            raise UnsafeFilesystemObject(
                                "prior-generation payload is missing from both paths"
                            )
                        if not self.audit.mark_deleted(reclaim_id, row_generation):
                            errors.append(
                                f"{torrent_hash}: deleting reconciliation fence changed"
                            )
                        continue
                    expected_dev = _filesystem_id(row.get("filesystem_dev"))
                    expected_ino = _filesystem_id(row.get("filesystem_ino"))
                    if expected_dev is None or expected_ino is None:
                        raise UnsafeFilesystemObject(
                            "quarantine filesystem identity is missing"
                        )
                    metadata = self._validate_payload_node(
                        quarantine_path,
                        expected_dev,
                    )
                    identity = FilesystemIdentity(
                        int(metadata.st_dev),
                        int(metadata.st_ino),
                        int(metadata.st_mode),
                    )
                    if int(identity.ino) != int(expected_ino):
                        raise FilesystemIdentityChanged(
                            "quarantine filesystem identity changed"
                        )
                    if not same_generation:
                        if not self._restore_from_quarantine(
                            quarantine_path, host_path
                        ):
                            raise RuntimeError(
                                "prior-generation quarantine restore failed"
                            )
                        self._validate_payload_node(
                            host_path,
                            expected_dev,
                            expected_identity=identity,
                        )
                        if not self.audit.mark_restored_aborted_paused(
                            reclaim_id,
                            row_generation,
                            "prior_generation_quarantine_restored",
                        ):
                            raise RuntimeError(
                                "restored reclaim recovery state changed"
                            )
                        continue
                    if state == "deleting" and not self.audit.mark_quarantined(
                        reclaim_id,
                        row_generation,
                        quarantine_path,
                        identity.dev,
                        identity.ino,
                    ):
                        raise RuntimeError(
                            "quarantine recovery state changed before deletion"
                        )
                    self._delete_quarantine_path(quarantine_path, identity)
                    if not self.audit.mark_deleted(reclaim_id, row_generation):
                        errors.append(
                            f"{torrent_hash}: quarantined reconciliation fence changed"
                        )
                except Exception as exc:
                    recovery_partial("quarantine_recovery_unsafe", exc)
                continue

            if state in {"deleted", "recheck_pending"}:
                quarantine_inconsistent = False
                try:
                    self._ensure_quarantine_root()
                    quarantine_path = self._quarantine_destination(reclaim_id)
                    quarantine_inconsistent = (
                        not self._quarantine_record_matches(row, quarantine_path)
                        or self._path_exists_no_follow(quarantine_path)
                    )
                except (OSError, UnsafeFilesystemObject):
                    quarantine_inconsistent = True
                if (
                    host_path is None
                    or self._path_exists_no_follow(host_path)
                    or quarantine_inconsistent
                ):
                    try:
                        changed = self.audit.mark_partial_or_unknown(
                            reclaim_id,
                            row_generation,
                            "deleted_path_present_or_unsafe",
                        )
                        if not changed:
                            errors.append(
                                f"{torrent_hash}: deleted reconciliation fence changed"
                            )
                    except Exception as exc:
                        errors.append(
                            f"{torrent_hash}: failed to fence inconsistent deleted path: {exc}"
                        )
                    continue
                recheck_error: str | None = None
                try:
                    self.executor.qbt_post(
                        "/api/v2/torrents/recheck", {"hashes": torrent_hash}
                    )
                except Exception as exc:
                    recheck_error = str(exc)
                    errors.append(f"{torrent_hash}: {exc}")
                try:
                    self.audit.complete(
                        reclaim_id,
                        row,
                        recheck_error=recheck_error,
                    )
                except Exception as exc:
                    errors.append(
                        f"{torrent_hash}: failed to reconcile recheck state: {exc}"
                    )
                continue

            if state == "partial_or_unknown":
                try:
                    changed = self.audit.mark_partial_or_unknown(
                        reclaim_id,
                        row_generation,
                        "partial_or_unknown_requires_confirmation",
                    )
                    if not changed:
                        errors.append(
                            f"{torrent_hash}: partial reconciliation fence changed"
                        )
                except Exception as exc:
                    errors.append(
                        f"{torrent_hash}: failed to ensure partial deletion warning: {exc}"
                    )
        return errors

    def _eligibility_state(
        self,
    ) -> tuple[dict[str, dict[str, Any]], set[str], set[str], set[str]]:
        con = readonly_connect(self.state_db)
        try:
            rows = con.execute(
                "select hash,reclaimable_since,no_progress_since,capacity_viable,"
                "capacity_reason,capacity_generation from torrent_health"
            ).fetchall()
            placeholders = ",".join("?" for _ in OPEN_JOB_STATES)
            jobs = con.execute(
                f"select distinct hash from torrent_jobs where state in ({placeholders})",
                OPEN_JOB_STATES,
            ).fetchall()
            claims = con.execute(
                "select distinct hash from resource_reservations where state='active' "
                "and (expires_at is null or expires_at>?)",
                (int(self.now()),),
            ).fetchall()
            cooldowns = con.execute(
                "select hash from soak_state where cooldown_until is not null "
                "and cooldown_until>?",
                (int(self.now()),),
            ).fetchall()
        finally:
            con.close()
        return (
            {str(row["hash"]): dict(row) for row in rows},
            {str(row["hash"]) for row in jobs if row["hash"]},
            {str(row["hash"]) for row in claims if row["hash"]},
            {str(row["hash"]) for row in cooldowns if row["hash"]},
        )

    def _current_assessment_generation(self) -> int:
        con = readonly_connect(self.state_db)
        try:
            row = con.execute(
                "select current_generation from capacity_assessment_state where id=1"
            ).fetchone()
        finally:
            con.close()
        return int(row["current_generation"]) if row is not None else 0

    @staticmethod
    def _assessment_evidence_json(
        assessment: CapacityAssessment,
        evidence: TorrentCapacityEvidence,
    ) -> str:
        compact = redact(
            {
                "generation": int(assessment.generation),
                "observed_at": int(assessment.observed_at),
                "torrent": {
                    "hash": str(evidence.hash),
                    "managed": bool(evidence.managed),
                    "incomplete": bool(evidence.incomplete),
                    "availability": evidence.availability,
                    "complete_sources": int(evidence.complete_sources),
                    "no_progress_since": evidence.no_progress_since,
                    "viable": bool(evidence.viable),
                    "viability_reason": str(evidence.viability_reason),
                },
            }
        )
        return json.dumps(
            compact,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )

    def _has_open_job_or_reservation_or_cooldown(
        self, torrent_hash: str
    ) -> bool:
        con = readonly_connect(self.state_db)
        try:
            placeholders = ",".join("?" for _ in OPEN_JOB_STATES)
            job = con.execute(
                f"select 1 from torrent_jobs where hash=? "
                f"and state in ({placeholders}) limit 1",
                (str(torrent_hash), *OPEN_JOB_STATES),
            ).fetchone()
            if job is not None:
                return True
            now = int(self.now())
            claim = con.execute(
                "select 1 from resource_reservations where hash=? and state='active' "
                "and (expires_at is null or expires_at>?) limit 1",
                (str(torrent_hash), now),
            ).fetchone()
            if claim is not None:
                return True
            cooldown = con.execute(
                "select 1 from soak_state where hash=? and cooldown_until is not null "
                "and cooldown_until>? limit 1",
                (str(torrent_hash), now),
            ).fetchone()
            return cooldown is not None
        finally:
            con.close()

    def _candidate_fence_reason(
        self,
        candidate: Mapping[str, Any],
        assessment: CapacityAssessment,
    ) -> str | None:
        if self._current_assessment_generation() != int(assessment.generation):
            return "stale_assessment"
        torrent_hash = str(candidate["hash"])
        con = readonly_connect(self.state_db)
        try:
            row = con.execute(
                "select reclaimable_since,no_progress_since,capacity_viable,capacity_generation "
                "from torrent_health where hash=?",
                (torrent_hash,),
            ).fetchone()
        finally:
            con.close()
        if row is None:
            return "eligibility_changed"
        if (
            row["no_progress_since"] is None
            or int(row["no_progress_since"])
            != int(candidate.get("no_progress_since") or 0)
        ):
            return "progress_evidence_changed"
        if (
            int(row["capacity_generation"] or 0) != int(assessment.generation)
            or row["capacity_viable"] is None
            or int(row["capacity_viable"]) != 0
            or row["reclaimable_since"] is None
            or int(row["reclaimable_since"])
            != int(candidate.get("reclaimable_since") or 0)
        ):
            return "eligibility_changed"
        if self._has_open_job_or_reservation_or_cooldown(torrent_hash):
            return "active_protection"
        return None

    @staticmethod
    def _assessment_torrent_evidence(
        assessment: CapacityAssessment,
        torrent_hash: str,
    ) -> TorrentCapacityEvidence | None:
        expected_hash = str(torrent_hash).strip().lower()
        for assessment_hash, evidence in assessment.torrents.items():
            evidence_hash = str(evidence.hash or assessment_hash).strip().lower()
            if evidence_hash == expected_hash:
                return evidence
        return None

    def _live_torrent_rejection(
        self,
        current: Mapping[str, Any],
        *,
        candidate: Mapping[str, Any],
        assessment: CapacityAssessment,
    ) -> str | None:
        expected_hash = str(candidate["hash"]).strip().lower()
        current_hash = str(current.get("hash") or "").strip().lower()
        if not current_hash:
            return "torrent_identity_unknown"
        if current_hash != expected_hash:
            return "torrent_identity_changed"
        evidence = self._assessment_torrent_evidence(assessment, expected_hash)
        if evidence is None:
            return "stale_assessment"
        tags = {
            item.strip()
            for item in str(current.get("tags") or "").split(",")
            if item.strip()
        }
        if tags & PROTECTED_TAGS:
            return "protected_tag"
        if str(current.get("category") or "") != "auto" and "auto" not in tags:
            return "not_managed"
        amount_left = _nonnegative_integer(current.get("amount_left"))
        baseline_amount_left = _nonnegative_integer(candidate.get("amount_left"))
        if amount_left is None or baseline_amount_left is None:
            return "progress_evidence_unknown"
        if amount_left != baseline_amount_left:
            return "content_selection_changed"
        for field in CONTENT_SIZE_FIELDS:
            baseline_size = candidate.get(field)
            if baseline_size is None:
                continue
            current_size = _nonnegative_integer(current.get(field))
            if current_size is None or current_size != int(baseline_size):
                return "content_selection_changed"
        raw_availability = current.get("availability")
        try:
            availability = (
                None if raw_availability is None else float(raw_availability)
            )
        except (TypeError, ValueError):
            availability = None
        if (
            availability is None
            or not math.isfinite(availability)
            or availability < 0
        ):
            return "availability_unknown"
        complete_sources = max(
            int(current.get("num_seeds") or 0),
            int(current.get("num_complete") or 0),
        )
        if availability >= 1.0 or complete_sources > 0:
            return "complete_source"
        raw_progress = current.get("progress")
        try:
            current_progress = (
                None if raw_progress is None else float(raw_progress)
            )
            baseline_progress = float(candidate.get("progress"))
        except (TypeError, ValueError):
            return "progress_evidence_unknown"
        if (
            current_progress is None
            or not math.isfinite(current_progress)
            or not math.isfinite(baseline_progress)
            or current_progress < 0
            or baseline_progress < 0
        ):
            return "progress_evidence_unknown"
        if abs(current_progress - baseline_progress) > PROGRESS_EPSILON:
            return "progress_evidence_changed"
        for field in ("dlspeed", "dlspeed_bps"):
            raw_speed = current.get(field)
            if raw_speed is None:
                continue
            try:
                speed = float(raw_speed)
            except (TypeError, ValueError):
                return "progress_evidence_unknown"
            if not math.isfinite(speed):
                return "progress_evidence_unknown"
            if speed > 0:
                return "progress_resumed"
        completed_bytes = _completed_bytes(current)
        baseline_completed = _nonnegative_integer(candidate.get("completed_bytes"))
        if completed_bytes is None or baseline_completed is None:
            return "progress_evidence_unknown"
        if completed_bytes != baseline_completed:
            return "progress_evidence_changed"
        return None

    def _revalidate_candidate(
        self,
        candidate: Mapping[str, Any],
        assessment: CapacityAssessment,
    ) -> str | None:
        if self._current_assessment_generation() != int(assessment.generation):
            return "stale_assessment"
        try:
            current = self._qbt_call_with_timeout(
                "torrent_info",
                str(candidate["hash"]),
                timeout=self.inventory_timeout_sec,
            )
        except Exception:
            return "revalidation_failed"
        reason = self._live_torrent_rejection(
            current,
            candidate=candidate,
            assessment=assessment,
        )
        if reason is not None:
            return reason
        return self._candidate_fence_reason(candidate, assessment)

    def _wait_until_stopped(
        self,
        torrent_hash: str,
    ) -> tuple[dict[str, Any] | None, str | None]:
        deadline = float(self.monotonic()) + self.stop_timeout_sec
        for attempt in range(self.stop_max_polls):
            remaining = deadline - float(self.monotonic())
            if remaining <= 0:
                break
            try:
                current = dict(
                    self._qbt_call_with_timeout(
                        "torrent_info", torrent_hash, timeout=remaining
                    )
                )
            except TimeoutError:
                return None, "stop_timeout"
            except Exception:
                return None, "stop_confirmation_failed"
            if float(self.monotonic()) >= deadline:
                return None, "stop_timeout"
            if _is_stopped_download_state(current.get("state")):
                return current, None
            remaining = deadline - float(self.monotonic())
            if attempt + 1 < self.stop_max_polls and remaining > 0:
                self.sleep(min(self.stop_poll_interval_sec, remaining))
        return None, "stop_timeout"

    def _qbt_call_with_timeout(
        self,
        method_name: str,
        *args: Any,
        timeout: float,
    ) -> Any:
        method = getattr(self.executor.qbt, method_name)
        try:
            parameters = inspect.signature(method).parameters.values()
        except (TypeError, ValueError):
            parameters = ()
        accepts_timeout = any(
            parameter.name == "timeout"
            or parameter.kind is inspect.Parameter.VAR_KEYWORD
            for parameter in parameters
        )
        if accepts_timeout:
            return method(*args, timeout=timeout)
        return method(*args)

    def _fresh_path_inventory(
        self,
        torrent_hash: str,
    ) -> tuple[dict[str, Path], dict[str, Any] | None, str | None]:
        try:
            payload = self._qbt_call_with_timeout(
                "get_maindata", 0, timeout=self.inventory_timeout_sec
            )
        except Exception:
            return {}, None, "path_inventory_failed"
        if not isinstance(payload, Mapping) or payload.get("full_update") is not True:
            return {}, None, "path_inventory_failed"
        torrents = payload.get("torrents")
        if not isinstance(torrents, Mapping):
            return {}, None, "path_inventory_failed"

        expected_hash = str(torrent_hash).strip().lower()
        paths: dict[str, Path] = {}
        identities: set[str] = set()
        candidate_row: dict[str, Any] | None = None
        for fallback_hash, raw in torrents.items():
            if not isinstance(raw, Mapping):
                return {}, None, "path_inventory_failed"
            item = dict(raw)
            fallback_identity = str(fallback_hash or "").strip().lower()
            row_identity = str(item.get("hash") or "").strip().lower()
            if row_identity and fallback_identity and row_identity != fallback_identity:
                return {}, None, "path_inventory_failed"
            identity = row_identity or fallback_identity
            if not identity or identity in identities:
                return {}, None, "path_inventory_failed"
            identities.add(identity)
            raw_path = str(item.get("content_path") or "").strip()
            if not raw_path:
                return {}, None, "path_inventory_failed"
            path = self._host_path(raw_path)
            if identity == expected_hash:
                if path is None:
                    return {}, None, "path_inventory_failed"
                item["hash"] = identity
                candidate_row = item
            if path is not None:
                paths[identity] = path
        if candidate_row is None:
            return {}, None, "path_inventory_failed"
        return paths, candidate_row, None

    def _snapshot_paths(
        self, snapshots: Mapping[str, Mapping[str, Any]]
    ) -> dict[str, Path]:
        result: dict[str, Path] = {}
        for fallback_hash, raw in snapshots.items():
            torrent_hash = str(raw.get("hash") or fallback_hash)
            path = self._host_path(raw.get("content_path"))
            if path is not None:
                result[torrent_hash] = path
        return result

    @staticmethod
    def _lstat(path: Path):
        return Path(path).lstat()

    @staticmethod
    def _is_reparse_point(path: Path, metadata: Any) -> bool:
        if statmod.S_ISLNK(int(metadata.st_mode)):
            return True
        if int(getattr(metadata, "st_file_attributes", 0) or 0) & int(
            FILE_ATTRIBUTE_REPARSE_POINT
        ):
            return True
        is_junction = getattr(Path(path), "is_junction", None)
        if callable(is_junction):
            try:
                return bool(is_junction())
            except OSError:
                return True
        return False

    @staticmethod
    def _same_identity(metadata: Any, identity: FilesystemIdentity) -> bool:
        return (
            int(metadata.st_dev) == int(identity.dev)
            and int(metadata.st_ino) == int(identity.ino)
            and statmod.S_IFMT(int(metadata.st_mode))
            == statmod.S_IFMT(int(identity.mode))
        )

    def _path_exists_no_follow(self, path: Path) -> bool:
        try:
            self._lstat(path)
        except FileNotFoundError:
            return False
        return True

    @staticmethod
    def _lexical_path(path: str | Path) -> str:
        return os.path.normcase(os.path.abspath(os.fspath(path)))

    def _ensure_quarantine_root(self) -> Path:
        managed_metadata = self._lstat(self.managed_root)
        if (
            not statmod.S_ISDIR(int(managed_metadata.st_mode))
            or self._is_reparse_point(self.managed_root, managed_metadata)
        ):
            raise UnsafeFilesystemObject("managed root is not a plain directory")
        self.quarantine_root.mkdir(mode=0o700, exist_ok=True)
        quarantine_metadata = self._lstat(self.quarantine_root)
        if (
            not statmod.S_ISDIR(int(quarantine_metadata.st_mode))
            or self._is_reparse_point(self.quarantine_root, quarantine_metadata)
            or int(quarantine_metadata.st_dev) != int(managed_metadata.st_dev)
            or os.path.ismount(self.quarantine_root)
        ):
            raise UnsafeFilesystemObject("quarantine root is unsafe")
        try:
            resolved = self.quarantine_root.resolve(strict=True)
        except OSError as exc:
            raise UnsafeFilesystemObject("quarantine root cannot be resolved") from exc
        if resolved != self.quarantine_root:
            raise UnsafeFilesystemObject("quarantine root escaped managed root")
        if os.name != "nt":
            os.chmod(self.quarantine_root, 0o700)
        return self.quarantine_root

    def _quarantine_destination(self, reclaim_id: int) -> Path:
        return self.quarantine_root / f"reclaim-{int(reclaim_id)}"

    def _is_controlled_quarantine_destination(self, path: Path) -> bool:
        suffix = path.name.removeprefix("reclaim-")
        return (
            path.parent == self.quarantine_root
            and path.name.startswith("reclaim-")
            and suffix.isdigit()
            and int(suffix) > 0
        )

    def _quarantine_record_matches(
        self,
        row: Mapping[str, Any],
        destination: Path,
    ) -> bool:
        if not self._is_controlled_quarantine_destination(destination):
            return False
        if destination.name != f"reclaim-{int(row['id'])}":
            return False
        recorded = str(row.get("quarantine_path") or "").strip()
        if not recorded:
            return str(row.get("state") or "") != "quarantined"
        return self._lexical_path(recorded) == self._lexical_path(destination)

    def _validate_payload_node(
        self,
        path: Path,
        expected_dev: int,
        *,
        expected_identity: FilesystemIdentity | None = None,
    ) -> Any:
        metadata = self._lstat(path)
        if self._is_reparse_point(path, metadata):
            raise UnsafeFilesystemObject(f"reparse point rejected: {path}")
        if int(metadata.st_dev) != int(expected_dev):
            raise UnsafeFilesystemObject(f"cross-device payload rejected: {path}")
        if os.path.ismount(path):
            raise UnsafeFilesystemObject(f"mount point rejected: {path}")
        if expected_identity is not None and not self._same_identity(
            metadata, expected_identity
        ):
            raise FilesystemIdentityChanged(f"filesystem identity changed: {path}")
        mode = int(metadata.st_mode)
        if not (statmod.S_ISREG(mode) or statmod.S_ISDIR(mode)):
            raise UnsafeFilesystemObject(f"special filesystem object rejected: {path}")
        if statmod.S_ISDIR(mode):
            try:
                with os.scandir(path) as entries:
                    children = [path / entry.name for entry in entries]
            except OSError as exc:
                raise UnsafeFilesystemObject(f"payload tree cannot be scanned: {path}") from exc
            for child in children:
                self._validate_payload_node(child, expected_dev)
            current = self._lstat(path)
            if not self._same_identity(
                current,
                FilesystemIdentity(
                    int(metadata.st_dev), int(metadata.st_ino), int(metadata.st_mode)
                ),
            ):
                raise FilesystemIdentityChanged(f"directory changed during scan: {path}")
        return metadata

    def _capture_payload_identity(self, path: Path) -> FilesystemIdentity:
        managed_metadata = self._lstat(self.managed_root)
        metadata = self._validate_payload_node(path, int(managed_metadata.st_dev))
        return FilesystemIdentity(
            int(metadata.st_dev), int(metadata.st_ino), int(metadata.st_mode)
        )

    def _rename_to_quarantine(
        self,
        source: Path,
        destination: Path,
        identity: FilesystemIdentity,
    ) -> None:
        self._ensure_quarantine_root()
        if not self._is_controlled_quarantine_destination(destination):
            raise UnsafeFilesystemObject("invalid quarantine destination")
        if self._path_exists_no_follow(destination):
            raise UnsafeFilesystemObject("quarantine destination already exists")
        os.rename(source, destination)
        try:
            moved = self._lstat(destination)
            if (
                not self._same_identity(moved, identity)
                or self._is_reparse_point(destination, moved)
            ):
                raise FilesystemIdentityChanged("moved payload identity changed")
        except Exception:
            self._restore_from_quarantine(destination, source)
            raise

    def _restore_from_quarantine(self, destination: Path, source: Path) -> bool:
        if not self._is_controlled_quarantine_destination(destination):
            return False
        try:
            if self._path_exists_no_follow(source) or not self._path_exists_no_follow(
                destination
            ):
                return False
            os.rename(destination, source)
            return self._path_exists_no_follow(source) and not self._path_exists_no_follow(
                destination
            )
        except OSError:
            return False

    def _remove_node_no_follow(
        self,
        path: Path,
        expected_dev: int,
        *,
        expected_identity: FilesystemIdentity | None = None,
    ) -> None:
        metadata = self._validate_payload_node(
            path,
            expected_dev,
            expected_identity=expected_identity,
        )
        identity = FilesystemIdentity(
            int(metadata.st_dev), int(metadata.st_ino), int(metadata.st_mode)
        )
        if statmod.S_ISDIR(int(metadata.st_mode)):
            with os.scandir(path) as entries:
                children = [path / entry.name for entry in entries]
            for child in children:
                self._remove_node_no_follow(child, expected_dev)
            current = self._lstat(path)
            if not self._same_identity(current, identity):
                raise FilesystemIdentityChanged(
                    f"directory changed before removal: {path}"
                )
            os.rmdir(path)
        else:
            current = self._lstat(path)
            if not self._same_identity(current, identity):
                raise FilesystemIdentityChanged(f"file changed before removal: {path}")
            os.unlink(path)
        if self._path_exists_no_follow(path):
            raise UnsafeFilesystemObject(f"filesystem object survived removal: {path}")

    def _delete_quarantine_path(
        self,
        destination: Path,
        identity: FilesystemIdentity,
    ) -> None:
        self._ensure_quarantine_root()
        if not self._is_controlled_quarantine_destination(destination):
            raise UnsafeFilesystemObject("refusing non-quarantine deletion")
        self._remove_node_no_follow(
            destination,
            identity.dev,
            expected_identity=identity,
        )

    def _host_path(self, raw_path: Any) -> Path | None:
        text = str(raw_path or "").strip()
        if not text:
            return None
        container_path = PurePosixPath(text)
        try:
            relative = container_path.relative_to(self.container_downloads)
        except ValueError:
            return None
        unresolved = self.host_downloads.joinpath(*relative.parts)
        if self._has_symlink_component(unresolved):
            return None
        resolved = unresolved.resolve()
        if resolved == self.managed_root or not resolved.is_relative_to(
            self.managed_root
        ):
            return None
        if resolved == self.quarantine_root or resolved.is_relative_to(
            self.quarantine_root
        ):
            return None
        return resolved

    def _has_symlink_component(self, path: Path) -> bool:
        current = path
        while current != self.host_downloads and current.is_relative_to(
            self.host_downloads
        ):
            try:
                metadata = self._lstat(current)
            except FileNotFoundError:
                pass
            else:
                if self._is_reparse_point(current, metadata):
                    return True
            current = current.parent
        return False

    @staticmethod
    def _overlaps_other(
        torrent_hash: str, candidate: Path, all_paths: Mapping[str, Path]
    ) -> bool:
        for other_hash, other in all_paths.items():
            if str(other_hash) == str(torrent_hash):
                continue
            if other == candidate or other.is_relative_to(candidate) or candidate.is_relative_to(other):
                return True
        return False

    @classmethod
    def _allocated_bytes(cls, path: Path) -> int:
        def allocated(item: Path) -> int:
            stat = item.lstat()
            blocks = getattr(stat, "st_blocks", None)
            return int(blocks) * 512 if blocks is not None else int(stat.st_size)

        if path.is_file():
            return allocated(path)
        total = allocated(path)
        for root, dirs, files in os.walk(path, followlinks=False):
            base = Path(root)
            total += sum(allocated(base / name) for name in dirs)
            total += sum(allocated(base / name) for name in files)
        return total
