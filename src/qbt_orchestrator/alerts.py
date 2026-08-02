from __future__ import annotations

import hashlib
import time
from dataclasses import dataclass, field
from typing import Any, Mapping

from .observability import redact
from .runtime import BotNotificationRepository


GIB = 1024**3
MIB = 1024**2


@dataclass(frozen=True)
class SchedulerAlertConfig:
    enabled: bool = False
    chat_ids: list[str] = field(default_factory=list)
    interval_sec: int = 1800
    disk_alert_margin_bytes: int = 512 * MIB
    capacity_deadlock_enabled: bool = True


@dataclass(frozen=True)
class CapacityReclaimAlertContext:
    evaluation_status: str
    dry_run: bool
    planned: int
    reclaimed: int
    errors_count: int
    errors_summary: tuple[str, ...]
    rejection_counts: Mapping[str, int]
    rejection_fingerprint: str
    assessment_generation: int
    capacity_pressure_remaining: bool
    post_reclaim_free_bytes: int | None


def _fmt_gib(value: int) -> str:
    return f"{int(value) / GIB:.2f}GiB"


def _normalize_rejection_fingerprint(value: str) -> str:
    parts = sorted(
        part.strip()
        for part in str(value or "").split("|")
        if part.strip()
    )
    return "|".join(parts)


class SchedulerAlertService:
    """Queue proactive Telegram alerts for scheduler and disk-pressure anomalies."""

    def __init__(
        self,
        repo: BotNotificationRepository,
        config: SchedulerAlertConfig | None = None,
        now=None,
        warning_service=None,
    ):
        self.repo = repo
        self.config = config or SchedulerAlertConfig()
        self.now = now or (lambda: int(time.time()))
        self.warning_service = warning_service

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
        # scheduler_all_stopped is intentionally not projected to Telegram or
        # WarningInbox; the home panel surfaces it as natural-language condition.

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
        reclaim_context: CapacityReclaimAlertContext | None = None,
        mature_reclaim_candidates: int = 0,
        rejection_fingerprint: str = "",
    ) -> list[int]:
        """Record one WarningInbox row per capacity-deadlock episode fingerprint."""

        del required_minimum_growth_bytes, top_manual_candidates
        del mature_reclaim_candidates, rejection_fingerprint

        if (
            not self.config.enabled
            or not self.config.capacity_deadlock_enabled
            or self.warning_service is None
            or str(getattr(transition, "state", "")) != "capacity_deadlock"
        ):
            return []

        if (
            reclaim_context is None
            or str(reclaim_context.evaluation_status) != "live_evaluated"
            or bool(reclaim_context.dry_run)
            or not bool(reclaim_context.capacity_pressure_remaining)
        ):
            return []

        reclaimed = max(0, int(reclaim_context.reclaimed))
        message_state = "reclaiming" if reclaimed > 0 else "manual"
        rejection_counts = dict(
            sorted(
                (
                    str(reason).strip(),
                    max(0, int(count or 0)),
                )
                for reason, count in dict(reclaim_context.rejection_counts).items()
                if str(reason).strip()
            )[:32]
        )
        fingerprint = _normalize_rejection_fingerprint(
            "|".join(
                f"{reason}:{count}" for reason, count in rejection_counts.items()
            )
            or reclaim_context.rejection_fingerprint
        )
        message = (
            "可用空间不足，已暂停启动新任务；系统正在安全释放无效文件。"
            if message_state == "reclaiming"
            else "可用空间不足，当前没有能够安全回收的任务，需要人工处理。"
        )
        entered_at = int(getattr(transition, "entered_at", 0) or 0)
        dedupe_state = (
            f"{fingerprint}|message_state:{message_state}|"
            "evaluation_status:live_evaluated"
        )
        fingerprint_digest = hashlib.sha256(
            dedupe_state.encode("utf-8")
        ).hexdigest()[:16]
        warning_key = (
            f"capacity:no_safe_reclaim:{entered_at}:{fingerprint_digest}"
        )
        try:
            row = self.warning_service.report_once(
                warning_key=warning_key[:200],
                severity="critical",
                topic="capacity_deadlock",
                safe_message=message,
            )
        except Exception:
            return []
        if row is None:
            return []
        return [int(row["id"])]

    def _broadcast(self, *, topic: str, level: str, message: str, payload: dict[str, Any], dedupe_topic: str, bucket: int) -> list[int]:
        safe_message = str(redact(message))
        if self.warning_service is not None and level in {"warning", "error", "critical"}:
            if topic == "disk_threshold":
                state = str(payload.get("state") or dedupe_topic)
                warning_key = (
                    "capacity:no_safe_reclaim"
                    if state in {"emergency_near", "emergency"}
                    else f"capacity:disk:{state}"
                )
            elif topic == "capacity_deadlock":
                warning_key = "capacity:no_safe_reclaim"
            else:
                warning_key = f"daemon_task:alert:{dedupe_topic}"
            try:
                row = self.warning_service.report(
                    warning_key=str(warning_key)[:200],
                    severity=level if level in {"info", "warning", "error", "critical"} else "warning",
                    topic=topic,
                    safe_message=safe_message,
                    related_hash=str(payload.get("hash") or "") or None,
                )
                return [int(row["id"])]
            except Exception:
                pass
        ids: list[int] = []
        for chat_id in self.config.chat_ids:
            dedupe_key = f"scheduler-alert:{dedupe_topic}:{chat_id}:{bucket}"
            ids.append(
                self.repo.enqueue(
                    chat_id=chat_id,
                    topic=topic,
                    message=safe_message,
                    level=level,
                    payload=payload,
                    dedupe_key=dedupe_key,
                )
            )
        return ids
