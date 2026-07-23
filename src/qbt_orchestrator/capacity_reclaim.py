from __future__ import annotations

import json
import math
import os
import shutil
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
MAGNET_PREFIX = "mag" + "net:?"
PROGRESS_EPSILON = 1e-9


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
        }

    def begin(self, candidate: Mapping[str, Any]) -> int:
        identity = self._identity(candidate)
        now = int(self.now())

        def txn(con) -> int:
            con.execute(
                "insert into capacity_reclaims(reclaim_key,hash,name,magnet_uri,host_path,content_path,"
                "allocated_bytes,completed_bytes,progress,dead_since,reclaimable_since,capacity_generation,"
                "capacity_reason,assessment_json,state,recheck_state,created_at,updated_at) "
                "values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,'deleting','pending',?,?) "
                "on conflict(reclaim_key) do update set name=excluded.name,magnet_uri=excluded.magnet_uri,"
                "host_path=excluded.host_path,content_path=excluded.content_path,allocated_bytes=excluded.allocated_bytes,"
                "completed_bytes=excluded.completed_bytes,progress=excluded.progress,dead_since=excluded.dead_since,"
                "reclaimable_since=excluded.reclaimable_since,capacity_generation=excluded.capacity_generation,"
                "capacity_reason=excluded.capacity_reason,assessment_json=excluded.assessment_json,state='deleting',"
                "recheck_state='pending',recheck_error=null,updated_at=excluded.updated_at",
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
                "select id from capacity_reclaims where reclaim_key=?",
                (identity["reclaim_key"],),
            ).fetchone()
            assert row is not None
            return int(row["id"])

        return int(write_transaction(self.state_db, txn))

    def mark_failed(self, reclaim_id: int, error: str) -> None:
        now = int(self.now())
        safe_error = str(redact(error))[:2000]
        write_transaction(
            self.state_db,
            lambda con: con.execute(
                "update capacity_reclaims set state='failed',recheck_state='not_requested',"
                "recheck_error=?,updated_at=? where id=?",
                (safe_error, now, int(reclaim_id)),
            ),
        )

    def complete(
        self,
        reclaim_id: int,
        candidate: Mapping[str, Any],
        *,
        recheck_error: str | None,
    ) -> dict[str, Any]:
        identity = self._identity(candidate)
        now = int(self.now())
        recheck_state = "failed" if recheck_error else "requested"
        safe_recheck_error = None if recheck_error is None else str(redact(recheck_error))[:2000]
        status_text = "重新校验失败" if recheck_error else "已请求重新校验"
        full_magnet = identity["magnet_uri"]
        prefix = (
            "qBT 编排器自动容量回收完成\n"
            f"种子名：{identity['name']}\n"
            f"Hash：{identity['hash']}\n"
            f"释放空间：{identity['allocated_bytes'] / 1024**3:.2f} GiB\n"
            f"状态：{status_text}\n"
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
            "recheck_state": recheck_state,
            "recheck_error": safe_recheck_error,
        }
        payload_json = json.dumps(payload, ensure_ascii=False, sort_keys=True)

        def txn(con) -> dict[str, Any]:
            con.execute(
                "update capacity_reclaims set state='reclaimed',recheck_state=?,recheck_error=?,"
                "reclaimed_at=?,updated_at=? where id=?",
                (recheck_state, safe_recheck_error, now, now, int(reclaim_id)),
            )
            notification_ids: list[int] = []
            for chat_id in self.notification_chat_ids:
                dedupe_key = f"capacity-reclaim:{reclaim_id}:{chat_id}"
                con.execute(
                    "insert or ignore into bot_notifications(dedupe_key,chat_id,level,topic,message,"
                    "payload_json,state,attempts,created_at,updated_at) values(?,?,?,?,?,?,'queued',0,?,?)",
                    (
                        dedupe_key,
                        chat_id,
                        "warning" if recheck_error else "info",
                        "capacity_reclaim",
                        message,
                        payload_json,
                        now,
                        now,
                    ),
                )
                row = con.execute(
                    "select id from bot_notifications where dedupe_key=?", (dedupe_key,)
                ).fetchone()
                assert row is not None
                notification_ids.append(int(row["id"]))
            con.execute(
                "update capacity_reclaims set notification_ids_json=?,updated_at=? where id=?",
                (json.dumps(notification_ids), now, int(reclaim_id)),
            )
            return {
                "reclaim_id": int(reclaim_id),
                "notification_ids": notification_ids,
                "recheck_state": recheck_state,
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
        stop_timeout_sec: float = 5.0,
        stop_poll_interval_sec: float = 0.1,
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
        self.stop_timeout_sec = max(0.0, float(stop_timeout_sec))
        self.stop_poll_interval_sec = max(0.001, float(stop_poll_interval_sec))
        self.audit = CapacityReclaimAuditStore(
            self.state_db,
            notification_chat_ids=notification_chat_ids,
            now=self.now,
        )
        if not self.managed_root.is_relative_to(self.host_downloads):
            raise ValueError("capacity reclaim managed_root must be inside host_downloads")

    def run(
        self,
        snapshots: Mapping[str, Mapping[str, Any]],
        *,
        assessment: CapacityAssessment,
        capacity_state: str,
        free_bytes: int,
        target_free_bytes: int,
    ) -> CapacityReclaimResult:
        generation = int(assessment.generation)
        if generation <= 0:
            return CapacityReclaimResult(
                dry_run=self.dry_run,
                assessment_generation=generation,
                rejection_counts={"uncommitted_assessment": 1},
            )
        if self._current_assessment_generation() != generation:
            return CapacityReclaimResult(
                dry_run=self.dry_run,
                assessment_generation=generation,
                rejection_counts={"stale_assessment": 1},
            )
        if (
            str(capacity_state) != "capacity_deadlock"
            or int(free_bytes) >= int(target_free_bytes)
            or self.max_per_tick <= 0
        ):
            return CapacityReclaimResult(
                dry_run=self.dry_run,
                assessment_generation=generation,
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
            candidates.append(
                {
                    "hash": torrent_hash,
                    "name": str(torrent.get("name") or ""),
                    "magnet_uri": str(torrent.get("magnet_uri") or ""),
                    "host_path": str(host_path),
                    "content_path": str(torrent.get("content_path") or ""),
                    "allocated_bytes": int(allocated),
                    "completed_bytes": max(
                        0,
                        int(
                            torrent.get("completed_bytes")
                            or torrent.get("completed")
                            or torrent.get("downloaded")
                            or 0
                        ),
                    ),
                    "progress": float(torrent.get("progress") or 0.0),
                    "amount_left": max(0, int(evidence.amount_left)),
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
        errors: list[str] = []
        completed: list[dict[str, Any]] = []
        for candidate in selected:
            torrent_hash = str(candidate["hash"])
            reclaim_id: int | None = None
            reason = self._revalidate_candidate(candidate, assessment)
            if reason is not None:
                reject(reason)
                continue
            try:
                self.executor.qbt_post(
                    "/api/v2/torrents/stop", {"hashes": torrent_hash}
                )
            except Exception as exc:
                reject("stop_failed")
                errors.append(f"{torrent_hash}: {exc}")
                continue
            current, stop_reason = self._wait_until_stopped(torrent_hash)
            if stop_reason is not None:
                reject(stop_reason)
                continue
            assert current is not None
            reason = self._live_torrent_rejection(
                current,
                candidate=candidate,
                assessment=assessment,
            )
            if reason is not None:
                reject(reason)
                continue
            host_path = self._host_path(current.get("content_path"))
            if host_path is None:
                reject("unsafe_path")
                continue
            if host_path != Path(str(candidate["host_path"])):
                reject("path_changed")
                continue
            (
                fresh_paths,
                inventory_candidate,
                inventory_reason,
            ) = self._fresh_path_inventory(torrent_hash)
            if inventory_reason is not None:
                reject(inventory_reason)
                continue
            assert inventory_candidate is not None
            reason = self._live_torrent_rejection(
                inventory_candidate,
                candidate=candidate,
                assessment=assessment,
            )
            if reason is not None:
                reject(reason)
                continue
            if not _is_stopped_download_state(inventory_candidate.get("state")):
                reject("torrent_not_stopped")
                continue
            inventory_host_path = fresh_paths.get(torrent_hash.strip().lower())
            if inventory_host_path is None:
                reject("path_inventory_failed")
                continue
            if inventory_host_path != host_path:
                reject("path_changed")
                continue
            if self._overlaps_other(torrent_hash, host_path, fresh_paths):
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
            candidate = {**candidate, "allocated_bytes": int(allocated)}
            reason = self._candidate_fence_reason(candidate, assessment)
            if reason is not None:
                reject(reason)
                continue
            try:
                reclaim_id = self.audit.begin(candidate)
                self._delete_path(host_path)
                reclaimed += 1
                reclaimed_bytes += int(candidate["allocated_bytes"])
            except Exception as exc:
                if reclaim_id is not None:
                    try:
                        self.audit.mark_failed(reclaim_id, str(exc))
                    except Exception as audit_exc:
                        errors.append(f"{torrent_hash}: failed to persist reclaim failure: {audit_exc}")
                errors.append(f"{torrent_hash}: {exc}")
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
                audit = self.audit.complete(
                    int(reclaim_id), candidate, recheck_error=recheck_error
                )
                completed.append({**candidate, **audit})
            except Exception as exc:
                errors.append(f"{torrent_hash}: failed to persist completed reclaim: {exc}")
                completed.append(candidate)
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
        raw_amount_left = current.get("amount_left")
        try:
            amount_left = (
                None if raw_amount_left is None else float(raw_amount_left)
            )
        except (TypeError, ValueError):
            amount_left = None
        if (
            amount_left is None
            or not math.isfinite(amount_left)
            or amount_left < 0
        ):
            return "progress_evidence_unknown"
        if amount_left <= 0:
            return "torrent_completed"
        if amount_left < int(candidate.get("amount_left") or 0):
            return "progress_resumed"
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
        if current_progress > baseline_progress + PROGRESS_EPSILON:
            return "progress_resumed"
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
        raw_completed = (
            current.get("completed_bytes")
            or current.get("completed")
            or current.get("downloaded")
            or 0
        )
        try:
            completed_bytes = float(raw_completed)
        except (TypeError, ValueError):
            return "progress_evidence_unknown"
        if not math.isfinite(completed_bytes) or completed_bytes < 0:
            return "progress_evidence_unknown"
        if completed_bytes > int(evidence.completed_bytes):
            return "progress_resumed"
        return None

    def _revalidate_candidate(
        self,
        candidate: Mapping[str, Any],
        assessment: CapacityAssessment,
    ) -> str | None:
        if self._current_assessment_generation() != int(assessment.generation):
            return "stale_assessment"
        try:
            current = self.executor.qbt.torrent_info(str(candidate["hash"]))
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
        attempts = max(
            1,
            int(math.ceil(self.stop_timeout_sec / self.stop_poll_interval_sec)) + 1,
        )
        for attempt in range(attempts):
            try:
                current = dict(self.executor.qbt.torrent_info(torrent_hash))
            except Exception:
                return None, "stop_confirmation_failed"
            if _is_stopped_download_state(current.get("state")):
                return current, None
            if attempt + 1 < attempts:
                self.sleep(self.stop_poll_interval_sec)
        return None, "stop_timeout"

    def _fresh_path_inventory(
        self,
        torrent_hash: str,
    ) -> tuple[dict[str, Path], dict[str, Any] | None, str | None]:
        try:
            payload = self.executor.qbt.get_maindata(0)
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
        return resolved

    def _has_symlink_component(self, path: Path) -> bool:
        current = path
        while current != self.host_downloads and current.is_relative_to(
            self.host_downloads
        ):
            if current.exists() and current.is_symlink():
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

    @staticmethod
    def _delete_path(path: Path) -> None:
        if path.is_dir():
            shutil.rmtree(path)
        else:
            path.unlink()
