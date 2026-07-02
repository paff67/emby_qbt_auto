from __future__ import annotations

import sqlite3
import time
from pathlib import Path
from typing import Callable


RETENTION_TABLES = ("events_v2", "action_log", "decision_log", "metrics_snapshots")


class SQLiteMaintenanceService:
    """Level-3 SQLite maintenance: retention + WAL checkpoint.

    This service is deliberately small and synchronous because it runs only in
    the 5-minute maintenance loop, never in the 2s safety path.  Deletes are
    performed in bounded batches so a production DB cannot be locked by a large
    retention cleanup.
    """

    def __init__(
        self,
        state_db: str | Path,
        now: Callable[[], int] | None = None,
        retention_days: int = 5,
        retention_delete_batch_size: int = 1000,
        journal_size_limit_bytes: int = 64 * 1024 * 1024,
        preferences_guard=None,
    ):
        self.state_db = Path(state_db)
        self.now = now or (lambda: int(time.time()))
        self.retention_days = int(retention_days)
        self.retention_delete_batch_size = max(1, int(retention_delete_batch_size))
        self.journal_size_limit_bytes = int(journal_size_limit_bytes)
        self.preferences_guard = preferences_guard

    def run_once(self) -> dict:
        cutoff = int(self.now()) - self.retention_days * 86400
        con = sqlite3.connect(self.state_db)
        con.row_factory = sqlite3.Row
        con.execute("pragma busy_timeout=5000")
        con.execute(f"pragma journal_size_limit={self.journal_size_limit_bytes}")
        deleted: dict[str, int] = {}
        try:
            for table in RETENTION_TABLES:
                deleted[table] = self._delete_old_rows(con, table, cutoff)
            con.commit()
            checkpoint = tuple(con.execute("pragma wal_checkpoint(passive)").fetchone())
            journal_size_limit = int(con.execute("pragma journal_size_limit").fetchone()[0])
        finally:
            con.close()
        result = {
            "retention_cutoff": cutoff,
            "retention_deleted": deleted,
            "retention_days": self.retention_days,
            "retention_delete_batch_size": self.retention_delete_batch_size,
            "wal_checkpoint": checkpoint,
            "journal_size_limit_bytes": journal_size_limit,
        }
        if self.preferences_guard is not None:
            result["qbt_preferences"] = self.preferences_guard.reconcile()
        return result

    def _delete_old_rows(self, con: sqlite3.Connection, table: str, cutoff: int) -> int:
        total = 0
        while True:
            cur = con.execute(
                f"delete from {table} where id in (select id from {table} where ts < ? order by id limit ?)",
                (cutoff, self.retention_delete_batch_size),
            )
            total += int(cur.rowcount or 0)
            if int(cur.rowcount or 0) < self.retention_delete_batch_size:
                return total
