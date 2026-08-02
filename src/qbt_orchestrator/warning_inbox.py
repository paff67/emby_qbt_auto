from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any, Callable, Mapping

from .db import readonly_connect, write_transaction
from .observability import redact


_SEVERITY_RANK = {"info": 1, "warning": 2, "error": 3, "critical": 4}
_MAX_EXPORT_ROWS = 1000
_MAX_EXPORT_BYTES = 512_000


def _now_default() -> int:
    return int(time.time())


def _row_dict(row: Mapping[str, Any] | None) -> dict[str, Any] | None:
    if row is None:
        return None
    return {str(k): row[k] for k in row.keys()}


class WarningInboxRepository:
    def __init__(self, state_db: str | Path, *, now: Callable[[], int] | None = None):
        self.state_db = Path(state_db)
        self.now = now or _now_default

    def upsert(
        self,
        *,
        warning_key: str,
        severity: str,
        topic: str,
        safe_message: str,
        related_hash: str | None = None,
        related_job_id: int | None = None,
        related_batch_id: int | None = None,
        related_item_id: int | None = None,
    ) -> dict[str, Any]:
        key = str(warning_key or "").strip()
        if not key:
            raise ValueError("warning_key")
        if severity not in _SEVERITY_RANK:
            raise ValueError("severity")
        topic_text = str(topic or "").strip() or "general"
        message = str(redact(str(safe_message)))[:2000]
        now = int(self.now())

        def txn(con) -> dict[str, Any]:
            existing = con.execute(
                "select * from bot_warning_inbox where warning_key=?", (key,)
            ).fetchone()
            if existing is None:
                cur = con.execute(
                    "insert into bot_warning_inbox("
                    "warning_key,severity,topic,safe_message,related_hash,related_job_id,"
                    "related_batch_id,related_item_id,occurrence_count,first_occurred_at,"
                    "last_occurred_at,updated_at,resolved) values(?,?,?,?,?,?,?,?,?,?,?,?,0)",
                    (
                        key,
                        severity,
                        topic_text,
                        message,
                        related_hash,
                        related_job_id,
                        related_batch_id,
                        related_item_id,
                        1,
                        now,
                        now,
                        now,
                    ),
                )
                warning_id = int(cur.lastrowid)
            else:
                resolved = int(existing["resolved"] or 0)
                if resolved == 0:
                    current_rank = _SEVERITY_RANK.get(str(existing["severity"]), 0)
                    new_rank = _SEVERITY_RANK[severity]
                    next_severity = (
                        severity
                        if new_rank >= current_rank
                        else str(existing["severity"])
                    )
                    con.execute(
                        "update bot_warning_inbox set severity=?,topic=?,safe_message=?,"
                        "related_hash=coalesce(?,related_hash),"
                        "related_job_id=coalesce(?,related_job_id),"
                        "related_batch_id=coalesce(?,related_batch_id),"
                        "related_item_id=coalesce(?,related_item_id),"
                        "occurrence_count=occurrence_count+1,last_occurred_at=?,updated_at=? "
                        "where id=?",
                        (
                            next_severity,
                            topic_text,
                            message,
                            related_hash,
                            related_job_id,
                            related_batch_id,
                            related_item_id,
                            now,
                            now,
                            int(existing["id"]),
                        ),
                    )
                else:
                    con.execute(
                        "update bot_warning_inbox set severity=?,topic=?,safe_message=?,"
                        "related_hash=coalesce(?,related_hash),"
                        "related_job_id=coalesce(?,related_job_id),"
                        "related_batch_id=coalesce(?,related_batch_id),"
                        "related_item_id=coalesce(?,related_item_id),"
                        "occurrence_count=occurrence_count+1,last_occurred_at=?,updated_at=?,"
                        "resolved=0,resolved_at=null,resolved_by=null where id=?",
                        (
                            severity,
                            topic_text,
                            message,
                            related_hash,
                            related_job_id,
                            related_batch_id,
                            related_item_id,
                            now,
                            now,
                            int(existing["id"]),
                        ),
                    )
                warning_id = int(existing["id"])
            return _row_dict(
                con.execute(
                    "select * from bot_warning_inbox where id=?", (warning_id,)
                ).fetchone()
            ) or {}

        return write_transaction(self.state_db, txn)

    def unread_count(self) -> int:
        con = readonly_connect(self.state_db)
        try:
            row = con.execute(
                "select count(*) as c from bot_warning_inbox where resolved=0"
            ).fetchone()
            return int(row["c"] if row and "c" in row.keys() else row[0])
        finally:
            con.close()

    def get(self, warning_id: int) -> dict[str, Any] | None:
        con = readonly_connect(self.state_db)
        try:
            row = con.execute(
                "select * from bot_warning_inbox where id=?",
                (int(warning_id),),
            ).fetchone()
            return _row_dict(row)
        finally:
            con.close()

    def list_recent(self, *, limit: int = 8, offset: int = 0) -> list[dict[str, Any]]:
        return self._list(resolved=None, limit=limit, offset=offset)

    def list_unread(self, *, limit: int = 8, offset: int = 0) -> list[dict[str, Any]]:
        return self._list(resolved=0, limit=limit, offset=offset)

    def _list(
        self, *, resolved: int | None, limit: int, offset: int
    ) -> list[dict[str, Any]]:
        if limit < 1 or limit > 100:
            raise ValueError("limit")
        if offset < 0:
            raise ValueError("offset")
        con = readonly_connect(self.state_db)
        try:
            if resolved is None:
                rows = con.execute(
                    "select * from bot_warning_inbox "
                    "order by last_occurred_at desc, id desc limit ? offset ?",
                    (int(limit), int(offset)),
                ).fetchall()
            else:
                rows = con.execute(
                    "select * from bot_warning_inbox where resolved=? "
                    "order by case severity "
                    "when 'critical' then 4 when 'error' then 3 when 'warning' then 2 else 1 end desc,"
                    "last_occurred_at desc, id desc limit ? offset ?",
                    (int(resolved), int(limit), int(offset)),
                ).fetchall()
            return [_row_dict(row) or {} for row in rows]
        finally:
            con.close()

    def mark_read(
        self, warning_id: int, *, expected_occurrence: int, admin_id: str
    ) -> bool:
        now = int(self.now())

        def txn(con) -> bool:
            cur = con.execute(
                "update bot_warning_inbox set resolved=1,resolved_at=?,resolved_by=?,updated_at=? "
                "where id=? and occurrence_count=? and resolved=0",
                (
                    now,
                    str(admin_id)[:128],
                    now,
                    int(warning_id),
                    int(expected_occurrence),
                ),
            )
            return int(cur.rowcount) == 1

        return bool(write_transaction(self.state_db, txn))

    def mark_all_read(self, *, last_occurred_cutoff: int, admin_id: str) -> int:
        now = int(self.now())

        def txn(con) -> int:
            cur = con.execute(
                "update bot_warning_inbox set resolved=1,resolved_at=?,resolved_by=?,updated_at=? "
                "where resolved=0 and last_occurred_at<=?",
                (now, str(admin_id)[:128], now, int(last_occurred_cutoff)),
            )
            return int(cur.rowcount)

        return int(write_transaction(self.state_db, txn))

    def copy_summary(self, warning_id: int) -> str:
        con = readonly_connect(self.state_db)
        try:
            row = con.execute(
                "select severity,topic,safe_message,occurrence_count,last_occurred_at "
                "from bot_warning_inbox where id=?",
                (int(warning_id),),
            ).fetchone()
            if row is None:
                return ""
            text = (
                f"[{row['severity']}] {row['topic']} x{row['occurrence_count']}: "
                f"{row['safe_message']}"
            )
            text = str(redact(text))
            return text[:256]
        finally:
            con.close()

    def export_text(
        self,
        warning_id: int | None = None,
        *,
        max_rows: int = _MAX_EXPORT_ROWS,
        max_bytes: int = _MAX_EXPORT_BYTES,
    ) -> bytes:
        max_rows = min(max(1, int(max_rows)), _MAX_EXPORT_ROWS)
        max_bytes = min(max(1, int(max_bytes)), _MAX_EXPORT_BYTES)
        con = readonly_connect(self.state_db)
        try:
            lines: list[str] = []
            if warning_id is not None:
                warning = con.execute(
                    "select * from bot_warning_inbox where id=?", (int(warning_id),)
                ).fetchone()
                if warning is None:
                    return b""
                lines.append(
                    str(
                        redact(
                            f"warning id={warning['id']} key={warning['warning_key']} "
                            f"severity={warning['severity']} topic={warning['topic']} "
                            f"count={warning['occurrence_count']} "
                            f"message={warning['safe_message']}"
                        )
                    )
                )
                related = self._collect_related_events(con, warning, limit=max_rows)
                for event in related:
                    lines.append(
                        str(
                            redact(
                                f"{event['timestamp']} [{event['source']}] "
                                f"{event.get('level') or '-'} "
                                f"{event.get('component') or '-'} "
                                f"{event['event_type']} {event['message']}"
                            )
                        )
                    )
            else:
                warnings = con.execute(
                    "select * from bot_warning_inbox order by last_occurred_at desc,id desc "
                    "limit ?",
                    (max_rows,),
                ).fetchall()
                for warning in warnings:
                    lines.append(
                        str(
                            redact(
                                f"{warning['last_occurred_at']} [{warning['severity']}] "
                                f"{warning['topic']} {warning['safe_message']}"
                            )
                        )
                    )
            truncated = False
            body_lines: list[str] = []
            size = 0
            for line in lines[:max_rows]:
                encoded = (line + "\n").encode("utf-8")
                if size + len(encoded) > max_bytes:
                    truncated = True
                    break
                body_lines.append(line)
                size += len(encoded)
            if truncated or len(lines) > len(body_lines):
                body_lines.append("--- 后续日志因导出限制已省略 ---")
            return ("\n".join(body_lines) + ("\n" if body_lines else "")).encode("utf-8")
        finally:
            con.close()

    @staticmethod
    def _collect_related_events(
        con, warning, *, limit: int
    ) -> list[dict[str, Any]]:
        events: list[dict[str, Any]] = []
        related_hash = warning["related_hash"]
        related_job_id = warning["related_job_id"]
        related_batch_id = warning["related_batch_id"]
        related_item_id = warning["related_item_id"]
        if related_hash:
            for row in con.execute(
                "select id,ts,level,component,event_type,message from events_v2 "
                "where hash=? order by ts asc,id asc limit ?",
                (str(related_hash), limit),
            ):
                events.append(
                    {
                        "source": "events_v2",
                        "source_id": int(row["id"]),
                        "timestamp": int(row["ts"]),
                        "level": str(row["level"] or ""),
                        "component": str(row["component"] or ""),
                        "event_type": str(row["event_type"] or ""),
                        "message": str(row["message"] or ""),
                    }
                )
        if related_job_id is not None:
            for row in con.execute(
                "select id,ts,level,component,event_type,message from events_v2 "
                "where job_id=? order by ts asc,id asc limit ?",
                (int(related_job_id), limit),
            ):
                events.append(
                    {
                        "source": "events_v2",
                        "source_id": int(row["id"]),
                        "timestamp": int(row["ts"]),
                        "level": str(row["level"] or ""),
                        "component": str(row["component"] or ""),
                        "event_type": str(row["event_type"] or ""),
                        "message": str(row["message"] or ""),
                    }
                )
        if related_batch_id is not None:
            for row in con.execute(
                "select id,created_at,event_type,from_state,to_state,reason_code,"
                "safe_evidence_json from bot_add_events where batch_id=? "
                "order by created_at asc,id asc limit ?",
                (int(related_batch_id), limit),
            ):
                events.append(
                    {
                        "source": "bot_add_events",
                        "source_id": int(row["id"]),
                        "timestamp": int(row["created_at"]),
                        "level": "info",
                        "component": "bot_add",
                        "event_type": str(row["event_type"] or ""),
                        "message": (
                            f"{row['from_state']}->{row['to_state']} "
                            f"{row['reason_code']} {row['safe_evidence_json'] or ''}"
                        ).strip(),
                    }
                )
        if related_item_id is not None:
            for row in con.execute(
                "select id,created_at,event_type,from_state,to_state,reason_code,"
                "safe_evidence_json from bot_add_events where item_id=? "
                "order by created_at asc,id asc limit ?",
                (int(related_item_id), limit),
            ):
                events.append(
                    {
                        "source": "bot_add_events",
                        "source_id": int(row["id"]),
                        "timestamp": int(row["created_at"]),
                        "level": "info",
                        "component": "bot_add",
                        "event_type": str(row["event_type"] or ""),
                        "message": (
                            f"{row['from_state']}->{row['to_state']} "
                            f"{row['reason_code']} {row['safe_evidence_json'] or ''}"
                        ).strip(),
                    }
                )
        deduped: dict[tuple[str, int], dict[str, Any]] = {}
        for event in events:
            deduped[(str(event["source"]), int(event["source_id"]))] = event
        ordered = sorted(
            deduped.values(),
            key=lambda item: (
                int(item["timestamp"]),
                str(item["source"]),
                int(item["source_id"]),
            ),
        )
        return ordered[:limit]


class WarningService:
    """Commit inbox rows first, then project generic Telegram notifications."""

    def __init__(
        self,
        state_db: str | Path,
        *,
        admin_chat_id: str | int | None,
        notifications=None,
        inbox: WarningInboxRepository | None = None,
        now: Callable[[], int] | None = None,
    ):
        self.state_db = Path(state_db)
        self.now = now or _now_default
        self.admin_chat_id = None if admin_chat_id in (None, "") else str(admin_chat_id)
        self.inbox = inbox or WarningInboxRepository(self.state_db, now=self.now)
        if notifications is None:
            from .runtime import BotNotificationRepository

            notifications = BotNotificationRepository(self.state_db, now=self.now)
        self.notifications = notifications

    def report(
        self,
        *,
        warning_key: str,
        severity: str,
        topic: str,
        safe_message: str,
        related_hash: str | None = None,
        related_job_id: int | None = None,
        related_batch_id: int | None = None,
        related_item_id: int | None = None,
        projection_payload: Mapping[str, Any] | None = None,
    ) -> dict[str, Any]:
        row = self.inbox.upsert(
            warning_key=warning_key,
            severity=severity,
            topic=topic,
            safe_message=safe_message,
            related_hash=related_hash,
            related_job_id=related_job_id,
            related_batch_id=related_batch_id,
            related_item_id=related_item_id,
        )
        self._project(row, projection_payload=projection_payload)
        return row

    def reconcile_projections(self, *, limit: int = 50) -> int:
        if self.admin_chat_id is None:
            return 0
        con = readonly_connect(self.state_db)
        try:
            missing = con.execute(
                "select w.id,w.occurrence_count,w.severity,w.topic,w.safe_message "
                "from bot_warning_inbox w "
                "left join bot_notifications n "
                "on n.dedupe_key=('warn:' || w.id || ':' || w.occurrence_count) "
                "where w.resolved=0 and n.id is null "
                "order by w.last_occurred_at desc,w.id desc limit ?",
                (max(1, min(100, int(limit))),),
            ).fetchall()
        finally:
            con.close()
        projected = 0
        for row in missing:
            # Reconciliation projects a generic warning without stale action buttons.
            self._project(_row_dict(row) or {})
            projected += 1
        return projected

    def _project(
        self,
        row: Mapping[str, Any],
        *,
        projection_payload: Mapping[str, Any] | None = None,
    ) -> None:
        if self.admin_chat_id is None:
            return
        warning_id = int(row["id"])
        occurrence = int(row["occurrence_count"])
        dedupe = f"warn:{warning_id}:{occurrence}"
        message = (
            f"系统警告 [{row.get('severity')}] {row.get('topic')}: "
            f"{row.get('safe_message')}"
        )
        payload: dict[str, Any] = {
            "warning_id": warning_id,
            "occurrence_count": occurrence,
        }
        if projection_payload:
            for key, value in dict(projection_payload).items():
                if key in {"warning_id", "occurrence_count"}:
                    continue
                payload[key] = value
            payload["warning_id"] = warning_id
        try:
            self.notifications.enqueue_with_status(
                chat_id=self.admin_chat_id,
                topic=str(row.get("topic") or "warning"),
                message=str(redact(message)),
                level=str(row.get("severity") or "warning"),
                payload=payload,
                dedupe_key=dedupe,
            )
        except Exception:
            # Durable inbox row remains; reconcile_projections retries later.
            return
