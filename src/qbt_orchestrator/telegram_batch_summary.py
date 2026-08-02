from __future__ import annotations

import json
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable

from .db import readonly_connect, write_transaction
from .observability import redact

_SHANGHAI = timezone(timedelta(hours=8))


def _fmt_shanghai(ts: int) -> str:
    return datetime.fromtimestamp(int(ts), tz=_SHANGHAI).strftime("%Y-%m-%d %H:%M")


class BatchSummaryProjector:
    """Project final Telegram batch summaries into bot_notifications."""

    def __init__(
        self,
        state_db: str | Path,
        *,
        now: Callable[[], int] | None = None,
        max_per_tick: int = 10,
    ):
        self.state_db = Path(state_db)
        self.now = now or (lambda: int(time.time()))
        self.max_per_tick = max(1, min(50, int(max_per_tick)))

    def tick(self) -> dict[str, int]:
        now = int(self.now())
        final_ids = self._candidate_final_ids()
        projected_final = 0
        for batch_id in final_ids:
            if self._project_final(batch_id, now):
                projected_final += 1
        return {
            "initial": 0,
            "final": projected_final,
            "candidates": len(final_ids),
        }

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

    def _project_final(self, batch_id: int, now: int) -> bool:
        def txn(con) -> bool:
            batch = con.execute(
                "select * from bot_add_batches where id=?", (batch_id,)
            ).fetchone()
            if batch is None or batch["final_summary_sent_at"] is not None:
                return False
            if str(batch["state"]) != "complete":
                return False
            # Trust persisted batch.state='complete'; item states are only for counts.
            states = [
                str(row["state"])
                for row in con.execute(
                    "select state from bot_add_items where batch_id=?", (batch_id,)
                )
            ]
            completed_at = int(batch["completed_at"] or now)
            failed_like = sum(
                1
                for state in states
                if state in {"failed", "invalid", "metadata_unavailable"}
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
                                "text": "在控制台查看",
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
