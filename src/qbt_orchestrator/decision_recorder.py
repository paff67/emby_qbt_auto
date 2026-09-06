from __future__ import annotations

import hashlib
import json
import sqlite3
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Mapping

from .db import write_transaction
from .observability import redact


VOLATILE_DECISION_FIELDS: frozenset[str] = frozenset({"progress", "free_bytes", "budget_bytes"})


def _redacted_payload(data: Mapping[str, Any]) -> dict[str, Any]:
    return dict(redact({str(key): value for key, value in data.items()}))


def _stable_payload(data: Mapping[str, Any], ignored: frozenset[str]) -> dict[str, Any]:
    """Return redacted, deterministically ordered decision data.

    Fast-changing measurements are useful in aggregate metrics but must not turn
    an unchanged scheduler state into a new audit row every tick.
    """

    return {key: value for key, value in _redacted_payload(data).items() if key not in ignored}


def stable_fingerprint(
    data: Mapping[str, Any],
    ignored: frozenset[str] = VOLATILE_DECISION_FIELDS,
) -> str:
    payload = _stable_payload(data, ignored)
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


@dataclass(frozen=True)
class DecisionEntry:
    """One desired audit state, optionally carrying its own virtual timestamp."""

    component: str
    hash: str | None
    decision: str
    reason_code: str
    data: Mapping[str, Any] = field(default_factory=dict)
    ts: int | None = None


class DecisionRecorder:
    """Atomically persist only scheduler decision transitions.

    `record_many_in_transaction()` lets high-volume planners reuse their existing
    atomic commit instead of opening one SQLite transaction per torrent.
    """

    def __init__(self, state_db: str | Path, now: Callable[[], int] | None = None):
        self.state_db = Path(state_db)
        self.now = now or (lambda: int(time.time()))

    def record(
        self,
        component: str,
        hash: str | None,
        decision: str,
        reason_code: str,
        data: Mapping[str, Any] | None = None,
    ) -> bool:
        return self.record_many(
            [DecisionEntry(component, hash, decision, reason_code, data or {})]
        )[0]

    def record_many(self, entries: list[DecisionEntry], ts: int | None = None) -> list[bool]:
        if not entries:
            return []
        return list(
            write_transaction(
                self.state_db,
                lambda con: self.record_many_in_transaction(con, entries, ts=ts),
            )
        )

    def record_many_in_transaction(
        self,
        con: sqlite3.Connection,
        entries: list[DecisionEntry],
        ts: int | None = None,
    ) -> list[bool]:
        fallback_ts = int(self.now()) if ts is None else int(ts)
        return [self._record_entry(con, entry, fallback_ts) for entry in entries]

    @staticmethod
    def _record_entry(con: sqlite3.Connection, entry: DecisionEntry, fallback_ts: int) -> bool:
        component = str(entry.component)
        torrent_hash = str(entry.hash or "")
        decision = str(entry.decision)
        reason_code = str(entry.reason_code)
        event_ts = int(entry.ts if entry.ts is not None else fallback_ts)
        # Persist the complete redacted context; only the comparison fingerprint
        # excludes volatile measurements.
        payload = _redacted_payload(entry.data)
        fingerprint = stable_fingerprint(entry.data)

        previous = con.execute(
            "select decision,reason_code,data_fingerprint from decision_state where component=? and hash=?",
            (component, torrent_hash),
        ).fetchone()
        if previous is not None and tuple(previous) == (decision, reason_code, fingerprint):
            return False

        con.execute(
            "insert into decision_state(component,hash,decision,reason_code,data_fingerprint,updated_at) "
            "values(?,?,?,?,?,?) "
            "on conflict(component,hash) do update set decision=excluded.decision,reason_code=excluded.reason_code,"
            "data_fingerprint=excluded.data_fingerprint,updated_at=excluded.updated_at",
            (component, torrent_hash, decision, reason_code, fingerprint, event_ts),
        )
        con.execute(
            "insert into decision_log(ts,component,hash,decision,reason_code,data_json) values(?,?,?,?,?,?)",
            (
                event_ts,
                component,
                torrent_hash,
                decision,
                reason_code,
                json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=False),
            ),
        )
        return True
