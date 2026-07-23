from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any, Mapping

from .observability import redact
from .runtime import BotNotificationRepository


STOPPED_STATES = {"pauseddl", "pausedup", "stoppeddl", "stoppedup", "paused", "stopped"}
GIB = 1024**3
MIB = 1024**2


@dataclass(frozen=True)
class SchedulerAlertConfig:
    enabled: bool = False
    chat_ids: list[str] = field(default_factory=list)
    interval_sec: int = 1800
    disk_alert_margin_bytes: int = 512 * MIB
    capacity_deadlock_enabled: bool = True


def _tags(torrent: Mapping[str, Any]) -> set[str]:
    return {p.strip() for p in str(torrent.get("tags") or "").split(",") if p.strip()}


def _is_managed(torrent: Mapping[str, Any]) -> bool:
    tags = _tags(torrent)
    return (str(torrent.get("category") or "") == "auto" or "auto" in tags) and "hold" not in tags


def _is_running_download(torrent: Mapping[str, Any]) -> bool:
    return (
        str(torrent.get("state") or "").lower() not in STOPPED_STATES
        and int(torrent.get("amount_left") or 0) > 0
        and float(torrent.get("progress") or 0) < 1.0
    )


def _fmt_gib(value: int) -> str:
    return f"{int(value) / GIB:.2f}GiB"


class SchedulerAlertService:
    """Queue proactive Telegram alerts for scheduler and disk-pressure anomalies."""

    def __init__(self, repo: BotNotificationRepository, config: SchedulerAlertConfig | None = None, now=None):
        self.repo = repo
        self.config = config or SchedulerAlertConfig()
        self.now = now or (lambda: int(time.time()))

    def evaluate_and_enqueue(
        self,
        *,
        snapshots: Mapping[str, Mapping[str, Any]],
        free_bytes: int,
        disk_floor_bytes: int,
        recovery_enter_bytes: int,
        emergency_floor_bytes: int,
        planner_result,
        sync_healthy: bool,
    ) -> list[int]:
        if not self.config.enabled or not self.config.chat_ids:
            return []
        now = int(self.now())
        bucket = now // max(1, int(self.config.interval_sec))
        enqueued: list[int] = []
        managed_incomplete = [dict(t, hash=str(t.get("hash") or h)) for h, t in snapshots.items() if _is_managed(t) and int(t.get("amount_left") or 0) > 0]
        running = [t for t in managed_incomplete if _is_running_download(t)]
        selected = list(getattr(planner_result, "selected_hashes", []) or [])
        if sync_healthy and managed_incomplete and not running and not selected:
            enqueued.extend(
                self._broadcast(
                    topic="scheduler_all_stopped",
                    level="warning",
                    message=(
                        "qBT Orchestrator: all managed downloads are stopped; "
                        f"managed={len(managed_incomplete)} free={_fmt_gib(int(free_bytes))} "
                        f"mode={getattr(planner_result, 'mode', 'unknown')} budget={_fmt_gib(int(getattr(planner_result, 'budget_bytes', 0) or 0))}"
                    ),
                    payload={"managed_incomplete": len(managed_incomplete), "free_bytes": int(free_bytes), "mode": getattr(planner_result, "mode", "unknown")},
                    dedupe_topic="all_stopped",
                    bucket=bucket,
                )
            )

        disk_margin = max(0, int(self.config.disk_alert_margin_bytes))
        level: str | None = None
        state = "normal"
        threshold = int(disk_floor_bytes)
        if int(free_bytes) < int(emergency_floor_bytes) + disk_margin:
            level = "critical" if int(free_bytes) < int(emergency_floor_bytes) else "warning"
            state = "emergency_near"
            threshold = int(emergency_floor_bytes)
        elif int(free_bytes) < int(recovery_enter_bytes):
            level = "warning"
            state = "recovery"
            threshold = int(recovery_enter_bytes)
        elif int(free_bytes) <= int(disk_floor_bytes) + disk_margin:
            level = "warning"
            state = "floor_near"
            threshold = int(disk_floor_bytes)
        if level is not None:
            enqueued.extend(
                self._broadcast(
                    topic="disk_threshold",
                    level=level,
                    message=(
                        "qBT Orchestrator disk threshold: "
                        f"state={state} free={_fmt_gib(int(free_bytes))} "
                        f"threshold={_fmt_gib(threshold)} emergency={_fmt_gib(int(emergency_floor_bytes))}"
                    ),
                    payload={"state": state, "free_bytes": int(free_bytes), "threshold_bytes": threshold, "emergency_floor_bytes": int(emergency_floor_bytes)},
                    dedupe_topic=f"disk:{state}",
                    bucket=bucket,
                )
            )
        return enqueued

    def enqueue_capacity_deadlock(
        self,
        transition,
        *,
        required_minimum_growth_bytes: int,
        top_manual_candidates: list[Mapping[str, Any]],
    ) -> list[int]:
        """Queue one manual-intervention notice for a new deadlock episode."""

        if (
            not self.config.enabled
            or not self.config.capacity_deadlock_enabled
            or not self.config.chat_ids
            or not bool(getattr(transition, "transitioned", False))
            or str(getattr(transition, "state", "")) != "capacity_deadlock"
        ):
            return []

        details = dict(getattr(transition, "details", {}) or {})
        managed = max(0, int(details.get("managed_incomplete") or 0))
        feasible = max(0, int(details.get("feasible_full_finish") or 0))
        releasing = max(0, int(details.get("disk_releasing_jobs") or 0))
        minimum = max(0, int(required_minimum_growth_bytes))
        candidates = [
            {
                "hash": str(candidate.get("hash") or ""),
                "required_growth_bytes": max(0, int(candidate.get("required_growth_bytes") or 0)),
            }
            for candidate in top_manual_candidates[:3]
        ]
        payload = {
            "managed_incomplete": managed,
            "feasible_full_finish": feasible,
            "disk_releasing_jobs": releasing,
            "required_minimum_growth_bytes": minimum,
            "top_manual_candidates": candidates,
            "scheduler_mode": str(getattr(transition, "scheduler_mode", "drain")),
            "entered_at": int(getattr(transition, "entered_at", 0) or 0),
        }
        candidate_text = ",".join(
            f"{candidate['hash'][:12]}:{_fmt_gib(candidate['required_growth_bytes'])}"
            for candidate in candidates
        ) or "none"
        message = (
            "qBT Orchestrator capacity deadlock: manual intervention required; "
            f"managed={managed} feasible={feasible} releasing={releasing} "
            f"minimum_growth={_fmt_gib(minimum)} candidates={candidate_text}"
        )
        ids: list[int] = []
        for chat_id in self.config.chat_ids:
            ids.append(
                self.repo.enqueue(
                    chat_id=chat_id,
                    topic="capacity_deadlock",
                    message=message,
                    level="critical",
                    payload=payload,
                    dedupe_key=(
                        f"scheduler-alert:capacity_deadlock:{chat_id}:"
                        f"{int(getattr(transition, 'entered_at', 0) or 0)}"
                    ),
                )
            )
        return ids

    def _broadcast(self, *, topic: str, level: str, message: str, payload: dict[str, Any], dedupe_topic: str, bucket: int) -> list[int]:
        ids: list[int] = []
        for chat_id in self.config.chat_ids:
            dedupe_key = f"scheduler-alert:{dedupe_topic}:{chat_id}:{bucket}"
            ids.append(
                self.repo.enqueue(
                    chat_id=chat_id,
                    topic=topic,
                    message=str(redact(message)),
                    level=level,
                    payload=payload,
                    dedupe_key=dedupe_key,
                )
            )
        return ids
