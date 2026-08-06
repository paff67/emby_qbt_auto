from __future__ import annotations

import sqlite3
import time
from pathlib import Path
from typing import Any, Callable, Mapping

from .db import readonly_connect
from .observability import redact

# metadata_unavailable is projected at the actual probe exhaustion transition by
# MetadataProbeCoordinator. Re-projecting it here would duplicate that warning.
_FAILURE_STATES = frozenset({"failed", "invalid"})
_RESOLVED_STATES = frozenset(
    {
        "ready",
        "enrolling",
        "enrolled",
        "enrolled_hold",
        "cancelled",
        "prechecking",
        "needs_confirmation",
        "duplicate_local",
        "duplicate_remote",
    }
)
_WARNING_KEY_PREFIX = "checked_add:batch_failed:"
_DEFAULT_LIMIT = 100


def first_nonempty(*values: Any) -> str:
    for value in values:
        text = str(value or "").strip()
        if text:
            return text
    return ""


def item_label(item: Mapping[str, Any]) -> str:
    media = first_nonempty(item.get("normalized_media_id"), item.get("display_name"))
    if media:
        return media[:160]
    source_index = int(item.get("source_index") or 0)
    identity = str(item.get("canonical_identity") or "").strip()
    short = identity[:12] if identity else "unknown"
    return f"第 {source_index + 1} 条 · {short}"


def safe_failure_reason(item: Mapping[str, Any]) -> str:
    reason = first_nonempty(
        item.get("last_error"),
        item.get("decision_reason"),
        item.get("state"),
        "未知原因",
    )
    return str(redact(reason))[:240]


def failure_stage_label(state: str) -> str:
    mapping = {
        "failed": "处理失败",
        "invalid": "链接无效",
        "metadata_unavailable": "元数据获取失败",
    }
    return mapping.get(str(state or ""), str(state or "未知阶段"))


class BatchFailureWarningProjector:
    """Project terminal failed add-items into WarningInbox without Telegram sends."""

    def __init__(
        self,
        state_db: str | Path,
        warning_service,
        *,
        now: Callable[[], int] | None = None,
        limit: int = _DEFAULT_LIMIT,
    ) -> None:
        self.state_db = Path(state_db)
        self.warning_service = warning_service
        self.now = now or (lambda: int(time.time()))
        if isinstance(limit, bool) or not isinstance(limit, int) or limit <= 0:
            raise ValueError("limit")
        self.limit = int(limit)

    def tick(self) -> dict[str, Any]:
        result = {
            "projected": 0,
            "resolved": 0,
            "errors": 0,
            "scanned": 0,
        }
        failures = self._list_failure_items()
        result["scanned"] = len(failures)
        for item in failures:
            try:
                self._project_one(item)
                result["projected"] += 1
            except (ValueError, RuntimeError, sqlite3.Error, TypeError):
                result["errors"] += 1
        for item_id in self._item_ids_needing_resolve():
            try:
                if self.warning_service.inbox.resolve_by_item(
                    int(item_id),
                    admin_id="batch_failure_projector",
                    states=_RESOLVED_STATES,
                ):
                    result["resolved"] += 1
            except (ValueError, RuntimeError, sqlite3.Error, TypeError):
                result["errors"] += 1
        return result

    def _project_one(self, item: Mapping[str, Any]) -> None:
        item_id = int(item["id"])
        batch_id = int(item["batch_id"])
        state = str(item.get("state") or "")
        label = item_label(item)
        reason = safe_failure_reason(item)
        severity = "error" if state == "failed" else "warning"
        stage = failure_stage_label(state)
        self.warning_service.report(
            warning_key=f"{_WARNING_KEY_PREFIX}{batch_id}:{item_id}",
            severity=severity,
            topic="checked_add_failure",
            safe_message=(
                f"批次 #{batch_id} 处理失败：{label}；"
                f"阶段：{stage}；原因：{reason}"
            ),
            related_batch_id=batch_id,
            related_item_id=item_id,
            occurrence_fingerprint=str(item["occurrence_fingerprint"]),
        )

    def _list_failure_items(self) -> list[dict[str, Any]]:
        placeholders = ",".join("?" for _ in sorted(_FAILURE_STATES))
        con = readonly_connect(self.state_db)
        try:
            rows = con.execute(
                "with candidates as ("
                "select i.id,i.batch_id,i.state,i.source_index,i.normalized_media_id,"
                "i.display_name,i.canonical_identity,i.last_error,i.decision_reason,"
                "i.updated_at,i.attempts,i.metadata_probe_attempt,i.approval_generation,"
                "coalesce("
                "  'event:' || (select max(e.id) from bot_add_events e "
                "               where e.item_id=i.id and e.to_state=i.state),"
                "  'row:' || i.state || ':' || i.updated_at || ':' || i.attempts || ':' || "
                "  i.metadata_probe_attempt || ':' || i.approval_generation"
                ") as occurrence_fingerprint "
                f"from bot_add_items i where i.state in ({placeholders})"
                ") "
                "select c.* from candidates c "
                "left join bot_warning_inbox w on w.warning_key=("
                "  'checked_add:batch_failed:' || c.batch_id || ':' || c.id"
                ") "
                "where w.id is null or coalesce(w.occurrence_fingerprint,'') "
                "<> c.occurrence_fingerprint "
                "order by c.updated_at,c.id limit ?",
                (*sorted(_FAILURE_STATES), self.limit),
            )
            return [dict(row) for row in rows]
        finally:
            con.close()

    def _item_ids_needing_resolve(self) -> list[int]:
        con = readonly_connect(self.state_db)
        try:
            rows = con.execute(
                "select distinct w.related_item_id as item_id "
                "from bot_warning_inbox w "
                "join bot_add_items i on i.id=w.related_item_id "
                "where w.resolved=0 "
                "and (w.warning_key like 'checked_add:batch_failed:%' "
                "     or w.warning_key like 'checked_add:metadata_unavailable:%') "
                f"and i.state in ({','.join('?' for _ in sorted(_RESOLVED_STATES))}) "
                "order by w.related_item_id limit ?",
                (*sorted(_RESOLVED_STATES), self.limit),
            )
            return [int(row["item_id"]) for row in rows if row["item_id"] is not None]
        finally:
            con.close()
