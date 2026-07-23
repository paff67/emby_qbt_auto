from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Mapping

from .db import readonly_connect, write_transaction
from .observability import redact


INTENT_UPSERT_SQL = (
    "insert into scheduler_intents(component,hash,intent,priority,expires_at,data_json) "
    "values(?,?,?,?,?,?) "
    "on conflict(component,hash) do update set "
    "intent=excluded.intent,priority=excluded.priority,expires_at=excluded.expires_at,data_json=excluded.data_json"
)


@dataclass(frozen=True)
class SchedulerIntent:
    """A short-lived scheduling request emitted by a feature component.

    Intents describe desired work only.  They never grant capacity and never
    apply qBittorrent actions; the central planner remains the sole allocation
    owner and resolves all active intents into one generation.
    """

    component: str
    hash: str
    intent: str
    priority: int
    expires_at: int | None
    data: Mapping[str, Any] = field(default_factory=dict)


class SchedulerIntentRepository:
    """Durable TTL repository shared by intent producers and the planner."""

    def __init__(self, state_db: str | Path):
        self.state_db = Path(state_db)

    @staticmethod
    def upsert_in_transaction(con: sqlite3.Connection, intent: SchedulerIntent) -> None:
        con.execute(INTENT_UPSERT_SQL, SchedulerIntentRepository._params(intent))

    def upsert(self, intent: SchedulerIntent) -> None:
        write_transaction(
            self.state_db,
            lambda con: self.upsert_in_transaction(con, intent),
        )

    @staticmethod
    def delete_in_transaction(con: sqlite3.Connection, component: str, hash: str) -> None:
        con.execute(
            "delete from scheduler_intents where component=? and hash=?",
            (str(component), str(hash)),
        )

    def delete(self, component: str, hash: str) -> None:
        write_transaction(
            self.state_db,
            lambda con: self.delete_in_transaction(con, component, hash),
        )

    def active(self, now: int) -> list[SchedulerIntent]:
        con = readonly_connect(self.state_db)
        try:
            rows = con.execute(
                "select component,hash,intent,priority,expires_at,data_json "
                "from scheduler_intents where expires_at is null or expires_at>? "
                "order by priority desc,component,hash",
                (int(now),),
            ).fetchall()
            return [self._from_row(row) for row in rows]
        finally:
            con.close()

    @staticmethod
    def _params(intent: SchedulerIntent) -> tuple[Any, ...]:
        return (
            str(intent.component),
            str(intent.hash),
            str(intent.intent),
            int(intent.priority),
            None if intent.expires_at is None else int(intent.expires_at),
            json.dumps(redact(dict(intent.data)), ensure_ascii=False, sort_keys=True),
        )

    @staticmethod
    def _from_row(row: sqlite3.Row) -> SchedulerIntent:
        try:
            data = json.loads(str(row["data_json"] or "{}"))
        except (TypeError, ValueError, json.JSONDecodeError):
            data = {}
        if not isinstance(data, dict):
            data = {}
        return SchedulerIntent(
            component=str(row["component"]),
            hash=str(row["hash"]),
            intent=str(row["intent"]),
            priority=int(row["priority"]),
            expires_at=None if row["expires_at"] is None else int(row["expires_at"]),
            data=data,
        )
