from __future__ import annotations

import json
import sqlite3
import time
from pathlib import Path
from typing import Any, Callable

from .db import write_transaction
from .observability import redact


def _connect(path: str | Path) -> sqlite3.Connection:
    con = sqlite3.connect(path)
    con.row_factory = sqlite3.Row
    return con


class QbtPreferencesGuard:
    """Level-3 qBT preferences drift detector and optional reconciler.

    The VPS baseline observed `incomplete_files_ext=false`.  Per the design
    plan, v2 records that drift by default and only forces a value when the
    desired value is explicitly configured.  `preallocate_all=false` is safe to
    reconcile because preallocation can rapidly consume the small US1 disk.
    """

    def __init__(
        self,
        state_db: str | Path,
        qbt,
        desired_preallocate_all: bool = False,
        desired_incomplete_files_ext: bool | None = None,
        dry_run: bool = True,
        now: Callable[[], int] | None = None,
    ):
        self.state_db = Path(state_db)
        self.qbt = qbt
        self.desired_preallocate_all = bool(desired_preallocate_all)
        self.desired_incomplete_files_ext = desired_incomplete_files_ext
        self.dry_run = bool(dry_run)
        self.now = now or (lambda: int(time.time()))

    def reconcile(self) -> dict[str, Any]:
        actual = self.qbt.get_preferences()
        drift: dict[str, dict[str, Any]] = {}
        to_set: dict[str, Any] = {}
        if actual.get("preallocate_all") is not None and bool(actual.get("preallocate_all")) != self.desired_preallocate_all:
            drift["preallocate_all"] = {"actual": bool(actual.get("preallocate_all")), "desired": self.desired_preallocate_all}
            to_set["preallocate_all"] = self.desired_preallocate_all
        if actual.get("incomplete_files_ext") is not None:
            actual_incomplete = bool(actual.get("incomplete_files_ext"))
            if self.desired_incomplete_files_ext is None:
                if actual_incomplete is False:
                    drift["incomplete_files_ext"] = {"actual": actual_incomplete, "desired": None}
            elif actual_incomplete != bool(self.desired_incomplete_files_ext):
                drift["incomplete_files_ext"] = {"actual": actual_incomplete, "desired": bool(self.desired_incomplete_files_ext)}
                to_set["incomplete_files_ext"] = bool(self.desired_incomplete_files_ext)

        result: dict[str, Any] = {"drift": drift, "would_set": dict(to_set), "applied": {}}
        self._record_drift(drift, to_set)
        if not to_set:
            return result
        if self.dry_run:
            self._record_action(to_set, "dry_run", dry_run=True)
            return result
        try:
            self.qbt.set_preferences(to_set)
        except Exception as exc:
            self._record_action(to_set, "failed", dry_run=False, error=str(exc))
            raise
        self._record_action(to_set, "succeeded", dry_run=False)
        result["applied"] = dict(to_set)
        return result

    def _record_drift(self, drift: dict[str, Any], to_set: dict[str, Any]) -> None:
        if not drift:
            return
        now = int(self.now())
        data = {"drift": drift, "would_set": to_set, "dry_run": self.dry_run}
        write_transaction(
            self.state_db,
            lambda con: con.execute(
                "insert into events_v2(ts,level,component,event_type,message,data_json) values(?,?,?,?,?,?)",
                (now, "warning", "qbt_preferences", "preferences_drift", "qBT preferences drift detected", json.dumps(redact(data), ensure_ascii=False)),
            ),
        )

    def _record_action(self, to_set: dict[str, Any], status: str, dry_run: bool, error: str | None = None) -> None:
        now = int(self.now())
        write_transaction(
            self.state_db,
            lambda con: con.execute(
                "insert into action_log(ts,action_type,path,payload_json,status,dry_run,error) values(?,?,?,?,?,?,?)",
                (
                    now,
                    "qbt_preferences",
                    "/api/v2/app/setPreferences",
                    json.dumps(redact({"set": to_set}), ensure_ascii=False),
                    status,
                    1 if dry_run else 0,
                    str(redact(error)) if error else None,
                ),
            ),
        )
