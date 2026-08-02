from __future__ import annotations

import json
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable

from .db import readonly_connect, write_transaction
from .observability import redact

_SHANGHAI = timezone(timedelta(hours=8))
_IMMEDIATE_STATES = frozenset(
    {"received", "resolving", "prechecking", "ready", "enrolling"}
)
_DELAYED_STATES = frozenset(
    {
        "waiting_probe_slot",
        "metadata_wait",
        "metadata_retry_wait",
        "needs_confirmation",
    }
)
_TERMINAL_STATES = frozenset(
    {
        "invalid",
        "duplicate_local",
        "metadata_unavailable",
        "duplicate_remote",
        "enrolled",
        "enrolled_hold",
        "failed",
        "cancelled",
    }
)


def _fmt_shanghai(ts: int) -> str:
    return datetime.fromtimestamp(int(ts), tz=_SHANGHAI).strftime("%Y-%m-%d %H:%M")


class BatchSummaryProjector:
    """Project initial/final Telegram batch summaries into bot_notifications."""

    def __init__(
        self,
        state_db: str | Path,
        *,
        now: Callable[[], int] | None = None,
        initial_delay_sec: int = 120,
        max_per_tick: int = 10,
    ):
        self.state_db = Path(state_db)
        self.now = now or (lambda: int(time.time()))
        self.initial_delay_sec = max(0, int(initial_delay_sec))
        self.max_per_tick = max(1, min(50, int(max_per_tick)))

    def tick(self) -> dict[str, int]:
        now = int(self.now())
        initial_ids = self._candidate_initial_ids(now)
        final_ids = self._candidate_final_ids()
        projected_initial = 0
        projected_final = 0
        for batch_id in initial_ids:
            if self._project_initial(batch_id, now):
                projected_initial += 1
        for batch_id in final_ids:
            if self._project_final(batch_id, now):
                projected_final += 1
        return {
            "initial": projected_initial,
            "final": projected_final,
            "candidates": len(initial_ids) + len(final_ids),
        }

    def _candidate_initial_ids(self, now: int) -> list[int]:
        submitted_before = now - self.initial_delay_sec
        con = readonly_connect(self.state_db)
        try:
            rows = con.execute(
                "select id from bot_add_batches "
                "where initial_summary_sent_at is null "
                "and state in ('processing','awaiting_confirmation') "
                "and submitted_at is not null and submitted_at<=? "
                "order by submitted_at,id limit ?",
                (submitted_before, self.max_per_tick),
            ).fetchall()
            return [int(row["id"]) for row in rows]
        finally:
            con.close()

    def _candidate_final_ids(self) -> list[int]:
        con = readonly_connect(self.state_db)
        try:
            rows = con.execute(
                "select id from bot_add_batches "
                "where final_summary_sent_at is null and state='complete' "
                "order by completed_at,id limit ?",
                (self.max_per_tick,),
            ).fetchall()
            return [int(row["id"]) for row in rows]
        finally:
            con.close()

    def _project_initial(self, batch_id: int, now: int) -> bool:
        def txn(con) -> bool:
            batch = con.execute(
                "select * from bot_add_batches where id=?", (batch_id,)
            ).fetchone()
            if batch is None or batch["initial_summary_sent_at"] is not None:
                return False
            if str(batch["state"]) not in {"processing", "awaiting_confirmation"}:
                return False
            submitted_at = batch["submitted_at"]
            if submitted_at is None or int(submitted_at) > now - self.initial_delay_sec:
                return False
            states = [
                str(row["state"])
                for row in con.execute(
                    "select state from bot_add_items where batch_id=?", (batch_id,)
                )
            ]
            if any(state in _IMMEDIATE_STATES for state in states):
                return False
            if not any(state in _DELAYED_STATES for state in states):
                return False
            counts = self._count_states(states)
            message = (
                f"批次 #{batch_id} 已完成第一阶段检查\n"
                f"共接收：{int(batch['received_count'] or 0)} 条\n"
                f"已加入下载：{int(batch['enrolled_count'] or 0)} 条\n"
                f"重复：{int(batch['duplicate_count'] or 0)} 条\n"
                f"历史删除拦截：{int(batch['blocked_history_count'] or 0)} 条\n"
                f"等待确认：{counts['needs_confirmation']} 条\n"
                f"等待元数据：{counts['metadata_wait']} 条\n"
                "剩余条目将在后台继续处理。"
            )
            payload = {
                "batch_id": batch_id,
                "summary": "initial",
                "reply_markup": {
                    "inline_keyboard": [
                        [
                            {
                                "text": "查看批次详情",
                                "callback_data": f"n:b:{batch_id}:0",
                            },
                            {"text": "返回首页", "callback_data": "n:h"},
                        ]
                    ]
                },
            }
            dedupe = f"tg:add-batch:{batch_id}:initial"
            self._enqueue_notification(
                con,
                chat_id=str(batch["chat_id"]),
                topic="add_batch_summary",
                message=message,
                payload=payload,
                dedupe_key=dedupe,
                now=now,
            )
            con.execute(
                "update bot_add_batches set initial_summary_sent_at=?,updated_at=? "
                "where id=? and initial_summary_sent_at is null",
                (now, now, batch_id),
            )
            return True

        return bool(write_transaction(self.state_db, txn))

    def _project_final(self, batch_id: int, now: int) -> bool:
        def txn(con) -> bool:
            batch = con.execute(
                "select * from bot_add_batches where id=?", (batch_id,)
            ).fetchone()
            if batch is None or batch["final_summary_sent_at"] is not None:
                return False
            if str(batch["state"]) != "complete":
                return False
            states = [
                str(row["state"])
                for row in con.execute(
                    "select state from bot_add_items where batch_id=?", (batch_id,)
                )
            ]
            if states and any(state not in _TERMINAL_STATES for state in states):
                return False
            completed_at = int(batch["completed_at"] or now)
            failed_like = sum(
                1 for state in states if state in {"failed", "invalid", "metadata_unavailable"}
            )
            message = (
                f"批次 #{batch_id} 已处理完成\n"
                f"共接收：{int(batch['received_count'] or 0)} 条\n"
                f"已加入下载：{int(batch['enrolled_count'] or 0)} 条\n"
                f"本地/远端重复：{int(batch['duplicate_count'] or 0)} 条\n"
                f"历史删除拦截：{int(batch['blocked_history_count'] or 0)} 条\n"
                f"失败或无效：{failed_like} 条\n"
                f"完成时间：{_fmt_shanghai(completed_at)}"
            )
            payload = {
                "batch_id": batch_id,
                "summary": "final",
                "reply_markup": {
                    "inline_keyboard": [
                        [
                            {
                                "text": "查看批次详情",
                                "callback_data": f"n:b:{batch_id}:0",
                            },
                            {"text": "返回首页", "callback_data": "n:h"},
                        ]
                    ]
                },
            }
            dedupe = f"tg:add-batch:{batch_id}:final"
            self._enqueue_notification(
                con,
                chat_id=str(batch["chat_id"]),
                topic="add_batch_summary",
                message=message,
                payload=payload,
                dedupe_key=dedupe,
                now=now,
            )
            con.execute(
                "update bot_add_batches set final_summary_sent_at=?,updated_at=? "
                "where id=? and final_summary_sent_at is null",
                (now, now, batch_id),
            )
            return True

        return bool(write_transaction(self.state_db, txn))

    @staticmethod
    def _count_states(states: list[str]) -> dict[str, int]:
        return {
            "needs_confirmation": sum(1 for state in states if state == "needs_confirmation"),
            "metadata_wait": sum(
                1
                for state in states
                if state
                in {"waiting_probe_slot", "metadata_wait", "metadata_retry_wait"}
            ),
        }

    @staticmethod
    def _enqueue_notification(
        con,
        *,
        chat_id: str,
        topic: str,
        message: str,
        payload: dict[str, Any],
        dedupe_key: str,
        now: int,
    ) -> None:
        safe_message = str(redact(message))
        safe_payload = json.dumps(redact(payload), ensure_ascii=False, sort_keys=True)
        con.execute(
            "insert or ignore into bot_notifications("
            "dedupe_key,chat_id,level,topic,message,payload_json,state,attempts,"
            "created_at,updated_at) values(?,?,?,?,?,?,?,?,?,?)",
            (
                dedupe_key,
                chat_id,
                "info",
                topic,
                safe_message,
                safe_payload,
                "queued",
                0,
                now,
                now,
            ),
        )
